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
S3_NEGATIVES_BUCKET     = os.getenv("S3_NEGATIVES_BUCKET", "ems-codex-versioned").strip()
GCP_SA_JSON             = (os.getenv("GCP_SERVICE_ACCOUNT_JSON") or "").strip()
GCP_SA_JSON_B64         = (os.getenv("GCP_SERVICE_ACCOUNT_JSON_B64") or "").strip()
# ==================================================

# ------------- GCP auth helpers -------------
def _load_service_account_info():
    if GCP_SA_JSON_B64:
        import base64
        return json.loads(base64.b64decode(GCP_SA_JSON_B64).decode("utf-8"))
    if GCP_SA_JSON:
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
    sql = f"""
    SELECT DISTINCT query
    FROM `{project}.{dataset}.google_search_console_web_url_query`
    WHERE DATE(date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
    """
    try:
        rows = bq.query(sql).result()
        out = [r["query"] for r in rows if r["query"]]
        logging.info(f"[{dataset}] Fetched {len(out)} queries from last 7 days")
        return out
    except Exception as e:
        logging.error(f"[{dataset}] BigQuery fetch failed: {e}")
        return []

# Schema without Sentiment_Category
CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS `{table_id}` (
  MONDAY DATE,
  query STRING,
  Sentiment_Score FLOAT64,
  inserted_at TIMESTAMP,
  updated_at TIMESTAMP
)
"""

def ensure_table_schema(bq: bigquery.Client, table_id: str):
    bq.query(CREATE_TABLE_SQL.format(table_id=table_id)).result()
    bq.query(f"ALTER TABLE `{table_id}` ADD COLUMN IF NOT EXISTS inserted_at TIMESTAMP").result()
    bq.query(f"ALTER TABLE `{table_id}` ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP").result()

def upsert_rows_to_bq(bq: bigquery.Client, rows: list, project: str, dataset: str, table: str):
    table_id = f"{project}.{dataset}.{table}"
    staging_table = f"_staging_{table}_{uuid.uuid4().hex[:8]}"
    staging_table_id = f"{project}.{dataset}.{staging_table}"

    ensure_table_schema(bq, table_id)

    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    staged_rows = [{
        "query": r["query"],
        "Sentiment_Score": r["Sentiment_Score"],
        "MONDAY": r["MONDAY"],
        "inserted_at": now_str,
        "updated_at": now_str
    } for r in rows]

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition="WRITE_TRUNCATE",
        autodetect=True,
    )
    ndjson = ("\n".join(json.dumps(x, separators=(",", ":")) for x in staged_rows)).encode("utf-8")
    bq.load_table_from_file(io.BytesIO(ndjson), staging_table_id, job_config=job_config).result()

    merge_sql = f"""
    MERGE `{table_id}` T
    USING `{staging_table_id}` S
    ON T.query = S.query
    WHEN MATCHED AND (
         SAFE_CAST(T.Sentiment_Score AS FLOAT64) != SAFE_CAST(S.Sentiment_Score AS FLOAT64)
    )
    THEN UPDATE SET
      T.Sentiment_Score = S.Sentiment_Score,
      T.updated_at      = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN
    INSERT (MONDAY, query, Sentiment_Score, inserted_at, updated_at)
    VALUES (
      DATE(S.MONDAY),
      S.query,
      S.Sentiment_Score,
      CURRENT_TIMESTAMP(),
      CURRENT_TIMESTAMP()
    );
    """
    bq.query(merge_sql).result()

    try:
        bq.delete_table(staging_table_id, not_found_ok=True)
    except Exception:
        pass

    logging.info(f" Upserted {len(rows)} rows into {table_id}")

# ------------- S3 helpers -------------
def _s3():
    return boto3.client("s3")

def s3_load_negative_keywords(dataset: str):
    bucket = S3_NEGATIVES_BUCKET
    key = f"sentiment/development/{dataset}/negatives.json.gz"
    try:
        obj = _s3().get_object(Bucket=bucket, Key=key)
        raw = obj["Body"].read()
        data = json.loads(gzip.decompress(raw).decode("utf-8"))
        if isinstance(data, dict):
            data = data.get("keywords", [])
        return [str(x).strip().lower() for x in data if str(x).strip()]
    except Exception:
        return []

# ------------- Sentiment -------------
EXCLUSION_BASE = ['beach','restaurant','hotel','museum','park','bitter end','kia ora','lonely planet','yacht']
DEFAULT_DESTINATIONS = ['botswana','kenya','mozambique','rwanda','south africa','tanzania','zambia','zanzibar',
    'australia','new zealand','cambodia','hong kong','indonesia','laos','malaysia','philippines','singapore',
    'thailand','vietnam','anguilla','antigua and barbuda','barbados','bermuda','british virgin islands','grenada',
    'jamaica','sint eustatius','st barths','st kitts & nevis','st vincent & the grenadines','turks & caicos',
    'maldives','mauritius','réunion','seychelles','sri lanka','greece','ibiza','italy','quintana roo','yucatán',
    'oaxaca','mexico city','jalisco','baja california sur','los cabos','veracruz','abu dhabi','ajman','dubai',
    'oman','ras al khaimah','canada','usa','cook islands','fiji','tahiti','bora bora'
]

def last_monday_str() -> str:
    today = date.today()
    monday = today if today.weekday() == 0 else today - timedelta(days=today.weekday())
    return monday.strftime("%Y-%m-%d")

def analyze_sentiment(queries, destinations, exclusions, negative_keywords):
    sia  = SentimentIntensityAnalyzer()
    excl = set(x.lower() for x in exclusions)
    dest = set(destinations)
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
        return s if abs(s) > 0.3 or any(d in t for d in dest) else 0.0

    monday = last_monday_str()
    return [{
        "query": q,
        "Sentiment_Score": float(score(q)),
        "MONDAY": monday
    } for q in queries]

# ------------- Orchestration -------------
def run_one(dataset: str):
    if not BIGQUERY_PROJECT:
        raise RuntimeError("BIGQUERY_PROJECT env is required.")
    bq = get_bq_client(BIGQUERY_PROJECT)

    queries = fetch_queries(bq, BIGQUERY_PROJECT, dataset)
    if not queries:
        return {"dataset": dataset, "rows": 0, "note": "no queries"}

    negs = s3_load_negative_keywords(dataset)
    rows = analyze_sentiment(queries, DEFAULT_DESTINATIONS, EXCLUSION_BASE, negs)
    upsert_rows_to_bq(bq, rows, BIGQUERY_PROJECT, dataset, OUTPUT_TABLE)
    return {"dataset": dataset, "rows": len(rows), "ok": True}

def run_for_datasets(datasets):
    results = []
    for ds in datasets:
        logging.info(f"=== Processing dataset: {ds} ===")
        results.append(run_one(ds))
    return results

