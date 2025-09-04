# api/diag_creds.py
from http.server import BaseHTTPRequestHandler
import os, json, base64

class handler(BaseHTTPRequestHandler):
    def do_GET(self):
        status = 200
        resp = {}
        try:
            raw = (os.getenv("GCP_SERVICE_ACCOUNT_JSON") or "").strip()
            b64 = (os.getenv("GCP_SERVICE_ACCOUNT_JSON_B64") or "").strip()
            cj  = (os.getenv("CLIENTS_JSON") or "").strip()

            resp["has_GCP_SERVICE_ACCOUNT_JSON"] = bool(raw)
            resp["has_GCP_SERVICE_ACCOUNT_JSON_B64"] = bool(b64)
            resp["has_CLIENTS_JSON"] = bool(cj)

            resp["GCP_SERVICE_ACCOUNT_JSON_status"] = _status(raw)
            resp["GCP_SERVICE_ACCOUNT_JSON_B64_status"] = _status(b64)
            resp["CLIENTS_JSON_status"] = _status(cj, allow_base64=True)

        except Exception as e:
            status = 500
            resp = {"error": str(e)}

        body = json.dumps(resp).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

def _status(s, allow_base64=False):
    if not s:
        return "empty"
    t = s.lstrip()
    if t.startswith("{") or t.startswith("["):
        return "json_ok" if _is_json(t) else "json_bad"
    if allow_base64:
        try:
            dec = base64.b64decode(s).decode("utf-8")
            return "base64->json_ok" if _is_json(dec) else "base64->json_bad"
        except Exception:
            return "neither_json_nor_base64"
    return "string_present"
def _is_json(s):
    try:
        json.loads(s); return True
    except Exception:
        return False
