# api/run.py
import json
import os
import logging
from http import HTTPStatus
from ..pipeline import run_for_all_clients

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def handler(request):
    try:
        result = run_for_all_clients()
        return {
            "statusCode": HTTPStatus.OK,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({"ok": True, "result": result})
        }
    except Exception as e:
        logging.exception("Pipeline failed")
        return {
            "statusCode": HTTPStatus.INTERNAL_SERVER_ERROR,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({"ok": False, "error": str(e)})
        }

# Vercel Python runtime expects a default export named 'handler'
