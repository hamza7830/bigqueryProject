# api/run.py
import json
from http.server import BaseHTTPRequestHandler
from pipeline import run_for_all_clients

class handler(BaseHTTPRequestHandler):
    def do_GET(self):
        try:
            result = run_for_all_clients()
            body = json.dumps({"ok": True, "result": result})
            self.send_response(200)
        except Exception as e:
            body = json.dumps({"ok": False, "error": str(e)})
            self.send_response(500)

        self.send_header('Content-Type', 'application/json')
        self.end_headers()
        self.wfile.write(body.encode('utf-8'))
