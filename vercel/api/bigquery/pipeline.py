# vercel/api/rbiqquery/pipeline.py
import io
import json
import gzip
import logging
import uuid
from datetime import date, timedelta, datetime, timezone
import os

import boto3
from google.cloud import bigquery
from google.oauth2 import service_account
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# ========= Runtime configuration from ENV =========
BIGQUERY_PROJECT        = os.getenv("BIGQUERY_PROJECT", "").strip()
OUTPUT_TABLE            = os.getenv("OUTPUT_TABLE", "").strip()

# S3 buckets
S3_CONTEXT_BUCKET       = os.getenv("S3_CONTEXT_BUCKET", "ems-codex-standard-test").strip()
S3_NEGATIVES_BUCKET     = os.getenv("S3_NEGATIVES_BUCKET", "ems-codex-versioned").strip()

# GCP service account: JSON (raw) or base64
GCP_SA_JSON             = (os.getenv("GCP_SERVICE_ACCOUNT_JSON") or "").strip()
GCP_SA_JSON_B64         = (os.getenv("GCP_SERVICE_ACCOUNT_JSON_B64") or "").strip()

# AWS creds (standard names; Vercel → Project Settings → Environment Variables)
# AWS_ACCESS_KEY_ID
# AWS_SECRET_ACCESS_KEY
# AWS_DEFAULT_REGION
# (or use an attached IAM role if running outside Vercel)
# ==================================================

# ------------- GCP auth helpers -------------
def _load_service_account_info():
    if GCP_SA_JSON_B64:
        import base64
        return json.loads(base64.b64decode(GCP_SA_JSON_B64).decode("utf-8"))
    if GCP_SA_JSON:
        # accept raw JSON or base64 stuffed into the same var
        if GCP_SA_JSON.lstrip().startswith("{"):
            return json.loads(GCP_SA_JSON)
        import base64
        return json.loads(base64.b64decode(GCP_SA_JSON).decode("utf-8"))
    raise RuntimeError("Missing GCP creds. Set GCP_SERVICE_ACCOUNT_JSON_B64 or GCP_SERVICE_ACCOUNT_JSON.")

def get_bq_client(project: str) -> bigquery.Client:
    info = _load_service_account_info()
    creds = service_account.Credentials.from_service_account_info(info)
    return bigquery.Client(project=project, credentials=creds)

# ------------- BigQuery IO -------------
def fetch_queries(bq: bigquery.Client, project: str, dataset: str):
    sql = f"SELECT DISTINCT query FROM `{project}.{dataset}.google_search_console_web_url_query`"
    try:
        rows = bq.query(sql).result()
        out = [r["query"] for r in rows if r["query"]]
        logging.info(f"[{dataset}] Fetched {len(out)} queries")
        return out
    except Exception as e:
        logging.error(f"[{dataset}] BigQuery fetch failed: {e}")
        return []

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS `{table_id}` (
  query STRING,
  Sentiment_Score FLOAT64,
  Sentiment_Category STRING,
  MONDAY DATE,
  inserted_at TIMESTAMP,
  updated_at TIMESTAMP
)
"""

def ensure_table_schema(bq: bigquery.Client, table_id: str):
    # Create if missing
    bq.query(CREATE_TABLE_SQL.format(table_id=table_id)).result()
    # Ensure audit cols exist (no-op if present)
    bq.query(f"ALTER TABLE `{table_id}` ADD COLUMN IF NOT EXISTS inserted_at TIMESTAMP").result()
    bq.query(f"ALTER TABLE `{table_id}` ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP").result()

def upsert_rows_to_bq(bq: bigquery.Client, rows: list, project: str, dataset: str, table: str):
    table_id = f"{project}.{dataset}.{table}"
    staging_table = f"_staging_{table}_{uuid.uuid4().hex[:8]}"
    staging_table_id = f"{project}.{dataset}.{staging_table}"

    ensure_table_schema(bq, table_id)

    # exact format: 2025-09-05 08:15:42
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    staged_rows = [{
        "query": r["query"],
        "Sentiment_Score": r["Sentiment_Score"],
        "Sentiment_Category": r["Sentiment_Category"],
        "MONDAY": r["MONDAY"],      # YYYY-MM-DD string; cast to DATE in MERGE
        "inserted_at": now_str,
        "updated_at": now_str
    } for r in rows]

    # load to staging
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition="WRITE_TRUNCATE",
        autodetect=True,
    )
    ndjson = ("\n".join(json.dumps(x, separators=(",", ":")) for x in staged_rows)).encode("utf-8")
    bq.load_table_from_file(io.BytesIO(ndjson), staging_table_id, job_config=job_config).result()

    # Only bump updated_at when sentiment fields changed.
    merge_sql = f"""
    MERGE `{table_id}` T
    USING `{staging_table_id}` S
    ON T.query = S.query
    WHEN MATCHED AND (
         SAFE_CAST(T.Sentiment_Score AS FLOAT64) != SAFE_CAST(S.Sentiment_Score AS FLOAT64)
      OR T.Sentiment_Category != S.Sentiment_Category
    )
    THEN UPDATE SET
      T.Sentiment_Score    = S.Sentiment_Score,
      T.Sentiment_Category = S.Sentiment_Category,
      T.updated_at         = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN
    INSERT (query, Sentiment_Score, Sentiment_Category, MONDAY, inserted_at, updated_at)
    VALUES (
      S.query,
      S.Sentiment_Score,
      S.Sentiment_Category,
      DATE(S.MONDAY),
      CURRENT_TIMESTAMP(),
      CURRENT_TIMESTAMP()
    );
    """
    bq.query(merge_sql).result()

    # drop staging
    try:
        bq.delete_table(staging_table_id, not_found_ok=True)
    except Exception:
        pass

    logging.info(f"[{dataset}] Upserted {len(rows)} rows into {table_id}")

# ------------- S3 helpers -------------
def _s3():
    # Uses AWS_* envs or role if present
    return boto3.client("s3")

def s3_load_context_keywords(bucket: str, dataset: str):
    """
    Optional plain-text file:
      s3://{bucket}/brand-sentiment/{dataset}_keywords.txt
    """
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
    REQUIRED negatives source (gz JSON only):
      s3://{S3_NEGATIVES_BUCKET}/sentiment/development/{dataset}/negatives.json.gz

    JSON may be a list of strings or {"keywords":[...]}.
    """
    bucket = S3_NEGATIVES_BUCKET
    key = f"sentiment/development/{dataset}/negatives.json.gz"
    try:
        obj = _s3().get_object(Bucket=bucket, Key=key)
        raw = obj["Body"].read()
        data = json.loads(gzip.decompress(raw).decode("utf-8"))
        if isinstance(data, dict):
            data = data.get("keywords", [])
        negs = [str(x).strip().lower() for x in data if str(x).strip()]
        logging.info(f"[{dataset}] Negatives: {len(negs)} from s3://{bucket}/{key}")
        return negs
    except Exception as e:
        logging.info(f"[{dataset}] Failed to load negatives at s3://{bucket}/{key} ({e}); using empty list.")
        return []

# ------------- Sentiment -------------
EXCLUSION_BASE = [
    'beach','restaurant','hotel','museum','park','bitter end','kia ora','lonely planet','yacht'
]
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
    sia  = SentimentIntensityAnalyzer()
    excl = set(x.lower() for x in (exclusions + ctx_keywords))
    dest = set(destinations)
    negs = set(negative_keywords or [])

    def score(q: str) -> float:
        t = q.lower()
        # your special case stays
        if 'st lucia' in t and not any(kw in t for kw in negs):
            return 0.0
        if any(ex in t for ex in excl):
            return 0.0
        if negs and any(kw in t for kw in negs):
            return -1.0
        s = sia.polarity_scores(q)['compound']
        return s if abs(s) > 0.3 or any(d in t for d in dest) else 0.0

    monday = last_monday_str()
    out = []
    for q in queries:
        s = score(q)
        cat = "positive" if s > 0.3 else "negative" if s < -0.3 else "neutral"
        out.append({
            "query": q,
            "Sentiment_Score": float(s),
            "Sentiment_Category": cat,
            "MONDAY": monday
        })
    return out

# ------------- Orchestration -------------
def run_one(dataset: str):
    if not BIGQUERY_PROJECT:
        raise RuntimeError("BIGQUERY_PROJECT env is required.")
    bq = get_bq_client(BIGQUERY_PROJECT)

    queries = fetch_queries(bq, BIGQUERY_PROJECT, dataset)
    if not queries:
        msg = f"[{dataset}] No queries returned."
        logging.warning(msg)
        return {"dataset": dataset, "rows": 0, "note": msg}

    ctx  = s3_load_context_keywords(S3_CONTEXT_BUCKET, dataset)  # optional
    negs = s3_load_negative_keywords(dataset)                    # required source; empty list if missing

    rows = analyze_sentiment(queries, DEFAULT_DESTINATIONS, EXCLUSION_BASE, ctx, negs)
    upsert_rows_to_bq(bq, rows, BIGQUERY_PROJECT, dataset, OUTPUT_TABLE)
    return {"dataset": dataset, "rows": len(rows), "ok": True}


def run_for_datasets(datasets):
    results = []
    for ds in datasets:
        logging.info(f"=== Processing dataset: {ds} ===")
        results.append(run_one(ds))
    return results
