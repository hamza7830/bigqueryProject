# pipeline.py
import os
import io
import json
import gzip
import logging
from datetime import date, timedelta, datetime
import uuid

import boto3
from google.cloud import bigquery
from google.oauth2 import service_account
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# ===== Runtime configuration via ENV =====
BIGQUERY_PROJECT = os.getenv("BIGQUERY_PROJECT", "").strip()
if not BIGQUERY_PROJECT:
    logging.warning("ENV BIGQUERY_PROJECT is not set. You must set it in Vercel.")

S3_CONTEXT_BUCKET = os.getenv("S3_CONTEXT_BUCKET", "ems-codex-standard-test").strip()
S3_NEGATIVES_BUCKET = os.getenv("S3_NEGATIVES_BUCKET", "ems-codex-versioned").strip()

SERVICE_ACCOUNT_JSON = os.getenv("GCP_SERVICE_ACCOUNT_JSON", "").strip()
SERVICE_ACCOUNT_JSON_B64 = os.getenv("GCP_SERVICE_ACCOUNT_JSON_B64", "").strip()
SERVICE_ACCOUNT_JSON_PATH = os.getenv("SERVICE_ACCOUNT_JSON_PATH", "").strip()
# ========================================


def _load_service_account_info():
    if SERVICE_ACCOUNT_JSON_B64:
        import base64
        dec = base64.b64decode(SERVICE_ACCOUNT_JSON_B64).decode("utf-8")
        return json.loads(dec)
    if SERVICE_ACCOUNT_JSON:
        if SERVICE_ACCOUNT_JSON.lstrip().startswith("{"):
            return json.loads(SERVICE_ACCOUNT_JSON)
        else:
            import base64
            dec = base64.b64decode(SERVICE_ACCOUNT_JSON).decode("utf-8")
            return json.loads(dec)
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


# ---------- schema management (robust) ----------
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
    # Always ensure table exists with full schema
    logging.info(f"Ensuring table exists: {table_id}")
    bq.query(CREATE_TABLE_SQL.format(table_id=table_id)).result()

    # Blindly ensure the two audit columns exist (no-op if already present)
    logging.info("Ensuring inserted_at/updated_at columns exist")
    bq.query(f"ALTER TABLE `{table_id}` ADD COLUMN IF NOT EXISTS inserted_at TIMESTAMP").result()
    bq.query(f"ALTER TABLE `{table_id}` ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP").result()


# ---------- Upsert with inserted_at / updated_at ----------
def upsert_rows_to_bq(bq: bigquery.Client, rows: list, project: str, dataset: str, table: str):
    table_id = f"{project}.{dataset}.{table}"
    staging_table = f"_staging_{table}_{uuid.uuid4().hex[:8]}"
    staging_table_id = f"{project}.{dataset}.{staging_table}"

    # Ensure target table exists and has audit columns
    ensure_table_schema(bq, table_id)

    # Prepare staging payload (timestamps included for visibility; truth is set by MERGE)
    now_str = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    staged_rows = []
    for r in rows:
        staged_rows.append({
            "query": r["query"],
            "Sentiment_Score": r["Sentiment_Score"],
            "Sentiment_Category": r["Sentiment_Category"],
            "MONDAY": r["MONDAY"],  # string YYYY-MM-DD; cast in MERGE
            "inserted_at": now_str,
            "updated_at": now_str
        })

    # Load to ephemeral staging
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition="WRITE_TRUNCATE",
        autodetect=True,
    )
    ndjson = ("\n".join(json.dumps(x, separators=(",", ":")) for x in staged_rows)).encode("utf-8")
    load_job = bq.load_table_from_file(
        file_obj=io.BytesIO(ndjson),
        destination=staging_table_id,
        job_config=job_config
    )
    load_job.result()

    # Only update updated_at when sentiment fields changed.
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
      DATE(S.MONDAY),              -- cast to DATE (staging is string)
      CURRENT_TIMESTAMP(),
      CURRENT_TIMESTAMP()
    );
    """
    bq.query(merge_sql).result()

    # Drop staging
    try:
        bq.delete_table(staging_table_id, not_found_ok=True)
    except Exception:
        pass

    logging.info(f"[{dataset}] Upserted {len(rows)} rows into {table_id}")


# ---------- S3 helpers ----------
def _s3():
    return boto3.client("s3")


def s3_load_context_keywords(bucket: str, dataset: str):
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
        out.append({
            "query": q,
            "Sentiment_Score": float(s),
            "Sentiment_Category": cat,
            "MONDAY": monday
        })
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
    negs = s3_load_negative_keywords(dataset)

    rows = analyze_sentiment(queries, DEFAULT_DESTINATIONS, EXCLUSION_BASE, ctx, negs)
    upsert_rows_to_bq(bq, rows, BIGQUERY_PROJECT, dataset, "test_table")
    return {"dataset": dataset, "rows": len(rows), "ok": True}


def run_for_datasets(datasets):
    results = []
    for ds in datasets:
        logging.info(f"=== Processing dataset: {ds} ===")
        results.append(run_one(ds))
    return results
