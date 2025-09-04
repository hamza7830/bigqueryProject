# api/diag.py
import json, os, logging
from google.cloud import bigquery
from google.oauth2 import service_account
import boto3

def handler(request):
    log = {}
    try:
        # 1) Env vars present?
        sa_raw = os.getenv("GCP_SERVICE_ACCOUNT_JSON")
        log["has_GCP_SERVICE_ACCOUNT_JSON"] = bool(sa_raw)
        if not sa_raw:
            return _resp(500, {"ok": False, "why": "Missing GCP_SERVICE_ACCOUNT_JSON", "log": log})

        # 2) Parse creds and run a trivial BigQuery query (SELECT 1)
        creds = service_account.Credentials.from_service_account_info(json.loads(sa_raw))
        project = json.loads(sa_raw).get("project_id")
        client = bigquery.Client(project=project, credentials=creds)
        val = list(client.query("SELECT 1 AS x").result())[0]["x"]
        log["bq_select_1"] = val

        # 3) Optional: check AWS identity (only if creds supplied)
        aws_id = None
        try:
            sts = boto3.client("sts")
            aws_id = sts.get_caller_identity().get("Account")
        except Exception as e:
            aws_id = f"not available ({e.__class__.__name__})"
        log["aws_account"] = aws_id

        return _resp(200, {"ok": True, "log": log})
    except Exception as e:
        logging.exception("diag failed")
        return _resp(500, {"ok": False, "error": str(e), "log": log})

def _resp(code, body):
    return {
        "statusCode": code,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(body)
    }
