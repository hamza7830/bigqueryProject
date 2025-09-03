
# pipeline.py
import os
import json
import logging
from datetime import date, timedelta

import pandas as pd
import boto3

from google.cloud import bigquery
from google.oauth2 import service_account

import nltk
from nltk.sentiment import SentimentIntensityAnalyzer
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.feature_extraction.text import ENGLISH_STOP_WORDS

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# ---------- NLTK (serverless-safe) ----------
def ensure_vader_downloaded():
    # Cache VADER in /tmp to avoid repeated downloads
    nltk_data_dir = "/tmp/nltk_data"
    os.makedirs(nltk_data_dir, exist_ok=True)
    if nltk_data_dir not in nltk.data.path:
        nltk.data.path.insert(0, nltk_data_dir)
    try:
        nltk.data.find("sentiment/vader_lexicon.zip")
    except LookupError:
        nltk.download("vader_lexicon", download_dir=nltk_data_dir)

# ---------- Defaults ----------
NEGATIVE_KEYWORDS = ["bad", "worst", "poor", "disappointed", "complaint", "negative", "disaster", "horrible"]

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

EXCLUSION_BASE = ['beach','restaurant','hotel','museum','park','bitter end','kia ora','lonely planet','yacht']

# ---------- Helpers ----------
def get_last_monday_str() -> str:
    today = date.today()
    monday = today if today.weekday() == 0 else today - timedelta(days=today.weekday())
    return monday.strftime("%Y-%m-%d")

def get_gcp_credentials():
    sa_json = os.getenv("GCP_SERVICE_ACCOUNT_JSON")
    if not sa_json:
        raise RuntimeError("GCP_SERVICE_ACCOUNT_JSON env var not set")
    info = json.loads(sa_json)
    return service_account.Credentials.from_service_account_info(info)

def get_bq_client(project_id: str):
    creds = get_gcp_credentials()
    return bigquery.Client(project=project_id, credentials=creds)

def load_context_keywords_from_s3(bucket: str, client_dataset: str):
    """
    Looks for s3://{bucket}/brand-sentiment/{client_dataset}_keywords.txt
    If missing, returns [] and continues.
    """
    key = f"brand-sentiment/{client_dataset}_keywords.txt"

    # Prefer explicit credentials via env; otherwise use default chain (incl. IAM)
    aws_access = os.getenv("AWS_ACCESS_KEY_ID")
    aws_secret = os.getenv("AWS_SECRET_ACCESS_KEY")
    aws_region = os.getenv("AWS_DEFAULT_REGION", "us-east-1")

    if aws_access and aws_secret:
        s3 = boto3.client("s3",
                          aws_access_key_id=aws_access,
                          aws_secret_access_key=aws_secret,
                          region_name=aws_region)
        logging.info("Using explicit AWS credentials for S3")
    else:
        s3 = boto3.client("s3")
        logging.info("Using default AWS credentials / IAM role for S3")

    try:
        obj = s3.get_object(Bucket=bucket, Key=key)
        content = obj["Body"].read().decode("utf-8").splitlines()
        kws = [line.strip().lower() for line in content if line.strip()]
        logging.info(f"[{client_dataset}] Loaded {len(kws)} context keywords from s3://{bucket}/{key}")
        return kws
    except Exception as e:
        logging.info(f"[{client_dataset}] No context file found at s3://{bucket}/{key}; continuing without it.")
        return []

def fetch_queries(bq: bigquery.Client, project: str, dataset: str) -> pd.DataFrame:
    sql = f"""
        SELECT DISTINCT query
        FROM `{project}.{dataset}.google_search_console_web_url_query`
    """
    try:
        df = bq.query(sql).to_dataframe()  # will use BigQuery Storage API if package is installed
        logging.info(f"[{dataset}] Fetched {len(df)} queries")
        return df
    except Exception as e:
        logging.error(f"[{dataset}] BigQuery fetch failed: {e}")
        return pd.DataFrame()

def analyze_sentiment(df: pd.DataFrame, destinations, exclusions, context_keywords):
    ensure_vader_downloaded()
    sia = SentimentIntensityAnalyzer()
    all_exclusions = exclusions + context_keywords

    def score(text: str) -> float:
        t = text.lower()
        if 'st lucia' in t and not any(kw in t for kw in NEGATIVE_KEYWORDS):
            return 0.0
        if any(ex in t for ex in all_exclusions):
            return 0.0
        if any(kw in t for kw in NEGATIVE_KEYWORDS):
            return -1.0
        s = sia.polarity_scores(text)['compound']
        return s if abs(s) > 0.3 or any(dest in t for dest in destinations) else 0.0

    df['Sentiment_Score'] = df['query'].apply(score)
    df['Sentiment_Category'] = df['Sentiment_Score'].apply(
        lambda s: 'positive' if s > 0.3 else 'negative' if s < -0.3 else 'neutral'
    )
    return df

def ensure_table_then_load(bq: bigquery.Client, df: pd.DataFrame, project: str, dataset: str, table: str):
    table_id = f"{project}.{dataset}.{table}"

    # Check/Log existence (load job will create if missing using inferred schema)
    try:
        bq.get_table(table_id)
        logging.info(f"[{dataset}] Table {table_id} exists; overwriting (WRITE_TRUNCATE)")
    except Exception:
        logging.info(f"[{dataset}] Table {table_id} does not exist; will be created during load")

    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    job = bq.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()
    logging.info(f"[{dataset}] Uploaded {len(df)} rows to {table_id}")

def run_for_client(client_conf: dict) -> dict:
    """
    client_conf:
      {
        "WEBSITE_BIGQUERY_ID": "...",   # dataset name
        "BIGQUERY_PROJECT": "...",      # project id
        "S3_BUCKET": "..."              # bucket for optional context
      }
    """
    dataset = client_conf["WEBSITE_BIGQUERY_ID"]
    project = client_conf["BIGQUERY_PROJECT"]
    bucket = client_conf["S3_BUCKET"]

    bq = get_bq_client(project)
    df = fetch_queries(bq, project, dataset)
    if df.empty:
        msg = f"[{dataset}] No queries to process"
        logging.warning(msg)
        return {"dataset": dataset, "rows": 0, "note": msg}

    context_keywords = load_context_keywords_from_s3(bucket, dataset)
    df = analyze_sentiment(df, DEFAULT_DESTINATIONS, EXCLUSION_BASE, context_keywords)
    df["MONDAY"] = get_last_monday_str()

    ensure_table_then_load(bq, df, project, dataset, "test_table")
    return {"dataset": dataset, "rows": int(len(df)), "ok": True}

def load_clients_from_env() -> list[dict]:
    """
    Reads CLIENTS_JSON if provided; else returns a default single client.
    Example CLIENTS_JSON:
    [
      {"WEBSITE_BIGQUERY_ID":"turquoiseholidays_co_uk","BIGQUERY_PROJECT":"ems-codex-test","S3_BUCKET":"ems-codex-standard-test"},
      {"WEBSITE_BIGQUERY_ID":"another_client","BIGQUERY_PROJECT":"ems-codex-test","S3_BUCKET":"ems-codex-standard-test"}
    ]
    """
    raw = os.getenv("CLIENTS_JSON", "").strip()
    if raw:
        try:
            data = json.loads(raw)
            assert isinstance(data, list), "CLIENTS_JSON must be a JSON array"
            return data
        except Exception as e:
            raise RuntimeError(f"Invalid CLIENTS_JSON: {e}")

    # Fallback (your current single client)
    return [{
        "WEBSITE_BIGQUERY_ID": "turquoiseholidays_co_uk",
        "BIGQUERY_PROJECT": "ems-codex-test",
        "S3_BUCKET": "ems-codex-standard-test"
    }]

def run_for_all_clients():
    clients = load_clients_from_env()
    results = []
    for c in clients:
        logging.info(f"=== Processing client: {c['WEBSITE_BIGQUERY_ID']} ===")
        results.append(run_for_client(c))
    return results
