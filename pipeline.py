# pipeline.py
import os
import io
import json
import gzip
import logging
from datetime import date, timedelta

import boto3
from google.cloud import bigquery
from google.oauth2 import service_account
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# ===== Runtime configuration via ENV =====
# BigQuery project (required)
BIGQUERY_PROJECT = os.getenv("BIGQUERY_PROJECT", "").strip()
if not BIGQUERY_PROJECT:
    logging.warning("ENV BIGQUERY_PROJECT is not set. You must set it in Vercel.")

# Optional: context keywords .txt bucket (per-client plain text list)
S3_CONTEXT_BUCKET = os.getenv("S3_CONTEXT_BUCKET", "ems-codex-standard-test").strip()

# Required: negatives gz JSON bucket (where negatives.json.gz lives)
S3_NEGATIVES_BUCKET = os.getenv("S3_NEGATIVES_BUCKET", "ems-codex-versioned").strip()
OUTPUT_TABLE_NAME = os.getenv("OUTPUT_TABLE_NAME", "").strip()

# Service account: prefer JSON/B64 via env for Vercel; optionally support path for local
SERVICE_ACCOUNT_JSON = os.getenv("GCP_SERVICE_ACCOUNT_JSON", "").strip()
SERVICE_ACCOUNT_JSON_B64 = os.getenv("GCP_SERVICE_ACCOUNT_JSON_B64", "").strip()
SERVICE_ACCOUNT_JSON_PATH = os.getenv("SERVICE_ACCOUNT_JSON_PATH", "").strip()

# ========================================

def _load_service_account_info():
    # Prefer explicit base64 env
    if SERVICE_ACCOUNT_JSON_B64:
        import base64
        dec = base64.b64decode(SERVICE_ACCOUNT_JSON_B64).decode("utf-8")
        return json.loads(dec)
    # Raw JSON env
    if SERVICE_ACCOUNT_JSON:
        # allow either JSON or base64 accidentally pasted here
        if SERVICE_ACCOUNT_JSON.lstrip().startswith("{"):
            return json.loads(SERVICE_ACCOUNT_JSON)
        else:
            import base64
            dec = base64.b64decode(SERVICE_ACCOUNT_JSON).decode("utf-8")
            return json.loads(dec)
    # Path (OK for local runs; not recommended on Vercel)
    if SERVICE_ACCOUNT_JSON_PATH:
        if not os.path.exists(SERVICE_ACCOUNT_JSON_PATH):
            raise FileNotFoundError(f"Service account JSON not found: {SERVICE_ACCOUNT_JSON_PATH}")
        from google.oauth2 import service_account as sa
        creds = sa.Credentials.from_service_account_file(SERVICE_ACCOUNT_JSON_PATH)
        return json.loads(creds.to_json())
    raise RuntimeError("Missing GCP creds: set GCP_SERVICE_ACCOUNT_JSON_B64 or GCP_SERVICE_ACCOUNT_JSON (or SERVICE_ACCOUNT_JSON_PATH for local)")

def get_bq_client(project: str) -> bigquery.Client:
    info = _load_service_account_info()
    creds = service_account.Credentials.from_service_account_info(info)
    return bigquery.Client(project=project, credentials=creds)

def fetch_queries(bq: bigquery.Client, project: str, dataset: str):
    sql = f"SELECT DISTINCT query FROM `{project}.{dataset}.google_search_console_web_url_query`"
    try:
        rows = bq.query(sql).result()
        queries = [r["query"] for r in rows if r["query"]]
        logging.info(f"[{dataset}] Fetched {len(queries)} queries")
        return queries
    except Exception as e:
        logging.error(f"[{dataset}] BigQuery fetch failed: {e}")
        return []

def load_json_to_bq(bq: bigquery.Client, rows: list, project: str, dataset: str, table: str):
    table_id = f"{project}.{dataset}.{table}"
    try:
        bq.get_table(table_id)
        logging.info(f"[{dataset}] Table exists: {table_id}; overwriting")
    except Exception:
        logging.info(f"[{dataset}] Table missing: {table_id}; will be created on load")

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition="WRITE_TRUNCATE",
        autodetect=True,
    )
    ndjson = ("\n".join(json.dumps(r, separators=(",", ":")) for r in rows)).encode("utf-8")
    job = bq.load_table_from_file(file_obj=io.BytesIO(ndjson), destination=table_id, job_config=job_config)
    job.result()
    logging.info(f"[{dataset}] Uploaded {len(rows)} rows to {table_id}")

# ---------- S3 helpers ----------
def _s3():
    # Uses AWS creds from env or attached role. Set AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY / AWS_DEFAULT_REGION in Vercel.
    return boto3.client("s3")

def s3_load_context_keywords(bucket: str, dataset: str):
    """Optional plain-text context file at s3://{bucket}/brand-sentiment/{dataset}_keywords.txt"""
    key = f"brand-sentiment/{dataset}_keywords.txt"
    try:
        obj = _s3().get_object(Bucket=bucket, Key=key)
        lines = obj["Body"].read().decode("utf-8").splitlines()
        kws = [ln.strip().lower() for ln in lines if ln.strip()]
        logging.info(f"[{dataset}] Context keywords: {len(kws)} from s3://{bucket}/{key}")
        return kws
    except Exception as e:
        logging.info(f"[{dataset}] No context keywords at s3://{bucket}/{key} ({e})")
        return []

def s3_load_negative_keywords(dataset: str):
    """
    REQUIRED negatives file (if absent, we proceed without forcing negatives):
      s3://{S3_NEGATIVES_BUCKET}/sentiment/development/{dataset}/negatives.json.gz

    JSON may be:
      - list of strings
      - or {"keywords": [...]}
    """
    bucket = S3_NEGATIVES_BUCKET
    key = f"sentiment/development/{dataset}/negatives.json.gz"
    try:
        obj = _s3().get_object(Bucket=bucket, Key=key)
        raw = obj["Body"].read()
        data = json.loads(gzip.decompress(raw).decode("utf-8"))
        if isinstance(data, dict):
            data = data.get("keywords", [])
        if not isinstance(data, list):
            logging.warning(f"[{dataset}] negatives.json.gz not a list/dict['keywords']; ignoring.")
            return []
        negs = [str(x).strip().lower() for x in data if str(x).strip()]
        logging.info(f"[{dataset}] Negatives: {len(negs)} from s3://{bucket}/{key}")
        return negs
    except Exception as e:
        logging.info(f"[{dataset}] No negatives at s3://{bucket}/{key} ({e}); proceeding without negatives.")
        return []

# ---------- Sentiment ----------
EXCLUSION_BASE = ['beach','restaurant','hotel','museum','park','bitter end','kia ora','lonely planet','yacht']
DEFAULT_DESTINATIONS = [
    'botswana','kenya','mozambique','rwanda','south africa','tanzania','zambia','zanzibar',
    'australia','new zealand','cambodia','hong kong','indonesia','laos','malaysia',
    'philippines','singapore','thailand','vietnam','anguilla','antigua and barbuda',
    'barbados','bermuda','british virgin islands','grenada','jamaica','sint eustatius',
    'st barths','st kitts & nevis','st vincent & the grenadines','turks & caicos',
    'maldives','mauritius','réunion','seychelles','sri lanka','greece','ibiza','italy',
    'quintana roo','yucatán','oaxaca','mexico city','jalisco','baja california sur',
    'los cabos','veracruz','abu dhabi','ajman','dubai','oman','ras al khaimah','canada',
    'usa','cook islands','fiji','tahiti','bora bora'
]

def last_monday_str() -> str:
    today = date.today()
    monday = today if today.weekday() == 0 else today - timedelta(days=today.weekday())
    return monday.strftime("%Y-%m-%d")

def analyze_sentiment(queries, destinations, exclusions, ctx_keywords, negative_keywords):
    sia = SentimentIntensityAnalyzer()
    excl = set(x.lower() for x in (exclusions + ctx_keywords))
    dests = set(destinations)
    negs = set(negative_keywords or [])

    def score(q: str) -> float:
        t = q.lower()
        # special case from your earlier logic
        if 'st lucia' in t and not any(kw in t for kw in negs):
            return 0.0
        if any(ex in t for ex in excl):
            return 0.0
        if negs and any(kw in t for kw in negs):
            return -1.0
        s = sia.polarity_scores(q)['compound']
        return s if abs(s) > 0.3 or any(d in t for d in dests) else 0.0

    monday = last_monday_str()
    out = []
    for q in queries:
        s = score(q)
        cat = "positive" if s > 0.3 else "negative" if s < -0.3 else "neutral"
        out.append({"query": q, "Sentiment_Score": float(s), "Sentiment_Category": cat, "MONDAY": monday})
    return out

# ---------- Orchestration ----------
def run_one(dataset: str):
    if not BIGQUERY_PROJECT:
        raise RuntimeError("BIGQUERY_PROJECT env is required.")
    bq = get_bq_client(BIGQUERY_PROJECT)

    queries = fetch_queries(bq, BIGQUERY_PROJECT, dataset)
    if not queries:
        logging.warning(f"[{dataset}] No queries returned.")
        return {"dataset": dataset, "rows": 0, "note": "no queries"}

    ctx = s3_load_context_keywords(S3_CONTEXT_BUCKET, dataset)
    negs = s3_load_negative_keywords(dataset)  # no defaults; if missing, proceed with empty

    rows = analyze_sentiment(queries, DEFAULT_DESTINATIONS, EXCLUSION_BASE, ctx, negs)
    load_json_to_bq(bq, rows, BIGQUERY_PROJECT, dataset, OUTPUT_TABLE_NAME)
    return {"dataset": dataset, "rows": len(rows), "ok": True}

def run_for_datasets(datasets):
    results = []
    for ds in datasets:
        logging.info(f"=== Processing dataset: {ds} ===")
        results.append(run_one(ds))
    return results
