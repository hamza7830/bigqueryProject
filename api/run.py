# api/run.py
from http.server import BaseHTTPRequestHandler
import json
from pipeline import run_for_all_clients

class handler(BaseHTTPRequestHandler):
    def do_GET(self):
        status = 200
        try:
            result = run_for_all_clients()
            payload = {"ok": True, "result": result}
        except Exception as e:
            status = 500
            payload = {"ok": False, "error": str(e)}

        body = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)
