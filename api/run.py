# api/run.py
import json
from pipeline import run_for_all_clients  # adjust import if needed

def handler(request):
  try:
    out = run_for_all_clients()
    return {
      "statusCode": 200,
      "headers": {"Content-Type": "application/json"},
      "body": json.dumps({"ok": True, "result": out})
    }
  except Exception as e:
    return {
      "statusCode": 500,
      "headers": {"Content-Type": "application/json"},
      "body": json.dumps({"ok": False, "error": str(e)})
    }
