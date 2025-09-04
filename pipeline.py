# pipeline.py
import os, json, base64, logging, io
from datetime import date, timedelta

import boto3
from google.cloud import bigquery
from google.oauth2 import service_account
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

NEGATIVE_KEYWORDS = ["bad","worst","poor","disappointed","complaint","negative","disaster","horrible"]
EXCLUSION_BASE   = ['beach','restaurant','hotel','museum','park','bitter end','kia ora','lonely planet','yacht']
DEFAULT_DESTINATIONS = [
    'botswana','kenya','mozambique','rwanda','south africa','tanzania','zambia','zanzibar',
    'australia','new zealand','cambodia','hong kong','indonesia','laos','malaysia','philippines',
    'singapore','thailand','vietnam','anguilla','antigua and barbuda','barbados','bermuda',
    'british virgin islands','grenada','jamaica','sint eustatius','st barths','st kitts & nevis',
    'st vincent & the grenadines','turks & caicos','maldives','mauritius','réunion','seychelles',
    'sri lanka','greece','ibiza','italy','quintana roo','yucatán','oaxaca','mexico city','jalisco',
    'baja california sur','los cabos','veracruz','abu dhabi','ajman','dubai','oman','ras al khaimah',
    'canada','usa','cook islands','fiji','tahiti','bora bora'
]

def _load_service_account_info():
    """Accepts either plain JSON or base64 in GCP_SERVICE_ACCOUNT_JSON or GCP_SERVICE_ACCOUNT_JSON_B64."""
    raw = (os.getenv("GCP_SERVICE_ACCOUNT_JSON") or "").strip()
    b64 = (os.getenv("GCP_SERVICE_ACCOUNT_JSON_B64") or "").strip()

    if b64:
        try:
            decoded = base64.b64decode(b64).decode("utf-8").strip()
            return json.loads(decoded)
        except Exception as e:
            raise RuntimeError(f"GCP_SERVICE_ACCOUNT_JSON_B64 invalid: {e}")

    if raw:
        try:
            # raw JSON?
            if raw.lstrip().startswith("{"):
                return json.loads(raw)
            # base64 pasted into the RAW var?
            decoded = base64.b64decode(raw).decode("utf-8").strip()
            return json.loads(decoded)
        except Exception as e:
            raise RuntimeError(f"GCP_SERVICE_ACCOUNT_JSON invalid (neither JSON nor base64 JSON): {e}")

    raise RuntimeError("Missing service account: set GCP_SERVICE_ACCOUNT_JSON (JSON or base64) or GCP_SERVICE_ACCOUNT_JSON_B64")

def get_bq_client(project: str) -> bigquery.Client:
    info = _load_service_account_info()
    creds = service_account.Credentials.from_service_account_info(info)
    return bigquery.Client(project=project, credentials=creds)

def s3_load_keywords(bucket: str, client_dataset: str):
    """Optional context file: s3://{bucket}/brand-sentiment/{client_dataset}_keywords.txt"""
    key = f"brand-sentiment/{client_dataset}_keywords.txt"
    aws_access, aws_secret = os.getenv("AWS_ACCESS_KEY_ID"), os.getenv("AWS_SECRET_ACCESS_KEY")
    aws_region = os.getenv("AWS_DEFAULT_REGION", "us-east-1")

    if aws_access and aws_secret:
        s3 = boto3.client("s3", aws_access_key_id=aws_access, aws_secret_access_key=aws_secret, region_name=aws_region)
        logging.info("Using explicit AWS credentials for S3")
    else:
        s3 = boto3.client("s3")
        logging.info("Using default AWS credentials / IAM for S3")

    try:
        obj = s3.get_object(Bucket=bucket, Key=key)
        lines = obj["Body"].read().decode("utf-8").splitlines()
        kws = [ln.strip().lower() for ln in lines if ln.strip()]
        logging.info(f"[{client_dataset}] Loaded {len(kws)} context keywords from s3://{bucket}/{key}")
        return kws
    except Exception:
        logging.info(f"[{client_dataset}] No context keyword file at s3://{bucket}/{key}; continuing without it.")
        return []

def last_monday_str() -> str:
    today = date.today()
    monday = today if today.weekday() == 0 else today - timedelta(days=today.weekday())
    return monday.strftime("%Y-%m-%d")

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

def analyze_sentiment(queries, destinations, exclusions, ctx_keywords):
    sia = SentimentIntensityAnalyzer()
    all_ex = set(x.lower() for x in (exclusions + ctx_keywords))
    dests = set(destinations)

    def score(q: str) -> float:
        t = q.lower()
        if 'st lucia' in t and not any(kw in t for kw in NEGATIVE_KEYWORDS):
            return 0.0
        if any(ex in t for ex in all_ex):
            return 0.0
        if any(kw in t for kw in NEGATIVE_KEYWORDS):
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

def run_client(conf: dict):
    dataset = conf["WEBSITE_BIGQUERY_ID"]
    project = conf["BIGQUERY_PROJECT"]
    bucket  = conf["S3_BUCKET"]

    bq = get_bq_client(project)
    queries = fetch_queries(bq, project, dataset)
    if not queries:
        msg = f"[{dataset}] No queries to process"
        logging.warning(msg)
        return {"dataset": dataset, "rows": 0, "note": msg}

    ctx = s3_load_keywords(bucket, dataset)
    rows = analyze_sentiment(queries, DEFAULT_DESTINATIONS, EXCLUSION_BASE, ctx)
    load_json_to_bq(bq, rows, project, dataset, "test_table")
    return {"dataset": dataset, "rows": len(rows), "ok": True}

def load_clients():
    raw = (os.getenv("CLIENTS_JSON") or "").strip()
    if raw:
        try:
            data = json.loads(raw)
        except Exception as e:
            # allow base64 here too if someone pasted it
            try:
                dec = base64.b64decode(raw).decode("utf-8")
                data = json.loads(dec)
            except Exception as e2:
                raise RuntimeError(f"CLIENTS_JSON invalid (neither JSON nor base64 JSON): {e} / {e2}")

        if not isinstance(data, list):
            raise RuntimeError("CLIENTS_JSON must be a JSON array of client objects")

        for i, c in enumerate(data):
            for k in ("WEBSITE_BIGQUERY_ID", "BIGQUERY_PROJECT", "S3_BUCKET"):
                if k not in c or not c[k]:
                    raise RuntimeError(f"CLIENTS_JSON[{i}] missing '{k}'")
        return data

    # fallback single client
    return [{
        "WEBSITE_BIGQUERY_ID": "turquoiseholidays_co_uk",
        "BIGQUERY_PROJECT": "ems-codex-test",
        "S3_BUCKET": "ems-codex-standard-test"
    }]

def run_for_all_clients():
    results = []
    for c in load_clients():
        logging.info(f"=== Processing: {c['WEBSITE_BIGQUERY_ID']} ===")
        results.append(run_client(c))
    return results
