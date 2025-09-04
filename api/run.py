# api/run.py
from http.server import BaseHTTPRequestHandler
import json
from pipeline import run_for_all_clients

class handler(BaseHTTPRequestHandler):
    def do_GET(self):
        try:
            result = run_for_all_clients()
            body = json.dumps({"ok": True, "result": result}).encode("utf-8")
            self.send_response(200)
        except Exception as e:
            body = json.dumps({"ok": False, "error": str(e)}).encode("utf-8")
            self.send_response(500)

        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)
