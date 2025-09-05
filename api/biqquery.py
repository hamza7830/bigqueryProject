from http.server import BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs
import json

from pipeline import run_for_datasets

class handler(BaseHTTPRequestHandler):
    def _json(self, code, payload):
        body = json.dumps(payload).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_POST(self):
        try:
            length = int(self.headers.get("Content-Length", 0))
            body = self.rfile.read(length).decode("utf-8") if length else "{}"
            data = json.loads(body) if body else {}
            datasets = data.get("CLIENT_DATASETS")
            if not datasets:
                raise ValueError("POST body must include CLIENT_DATASETS: [\"dataset_a\", \"dataset_b\", ...]")
            if not isinstance(datasets, list) or not all(isinstance(x, str) for x in datasets):
                raise ValueError("CLIENT_DATASETS must be a JSON array of strings")
            result = run_for_datasets(datasets)
            self._json(200, {"ok": True, "result": result})
        except Exception as e:
            self._json(500, {"ok": False, "error": str(e)})

    def do_GET(self):
        try:
            # 3 ways to supply datasets on GET:
            # 1) ?CLIENT_DATASETS=[...] (JSON)
            # 2) ?CLIENT_DATASETS=ds1,ds2 (comma-separated)
            # 3) ?WEBSITE_BIGQUERY_ID=single_dataset
            qs = parse_qs(urlparse(self.path).query)
            raw = (qs.get("CLIENT_DATASETS", [None])[0] or "").strip()
            one = (qs.get("WEBSITE_BIGQUERY_ID", [None])[0] or "").strip()

            datasets = []
            if raw:
                try:
                    parsed = json.loads(raw)
                    if isinstance(parsed, list):
                        datasets = [str(x) for x in parsed]
                except Exception:
                    datasets = [s for s in raw.split(",") if s.strip()]
            elif one:
                datasets = [one]

            if not datasets:
                raise ValueError("Provide datasets via CLIENT_DATASETS (JSON or comma list) or WEBSITE_BIGQUERY_ID.")

            result = run_for_datasets(datasets)
            self._json(200, {"ok": True, "result": result})
        except Exception as e:
            self._json(500, {"ok": False, "error": str(e)})
