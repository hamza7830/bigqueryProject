import json
import os
import sys
from http.server import BaseHTTPRequestHandler

# from faker import Faker

# fake = Faker()

class handler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Content-Type', 'application/json')
        self.end_headers()

        response_data = {
            "message": "this is a test response",
            "method": "GET",
            "timestamp": str(os.environ.get('VERCEL_REQUEST_TIME', 'not available'))
        }

        self.wfile.write(json.dumps(response_data, indent=2).encode())
