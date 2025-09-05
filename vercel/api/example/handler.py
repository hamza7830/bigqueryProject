from http.server import BaseHTTPRequestHandler
import json
import os
import sys

# Add project root to Python path (need to go up 4 levels from vercel/api/example/)
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..', '..'))
sys.path.insert(0, project_root)

# Now you can import from _app / _helper
from _app.local.environment import load_env, is_running_locally
from _helper.one_password import get_json_note_sync

def check_dependencies():
    """Check if required dependencies are available"""
    debug("=== Checking Dependencies ===")

    try:
        import onepassword
        debug("✅ onepassword-sdk is available")
        debug(f"onepassword version: {onepassword.__version__ if hasattr(onepassword, '__version__') else 'unknown'}")
    except ImportError as e:
        debug(f"❌ onepassword-sdk import failed: {e}")

    try:
        import dotenv
        debug("✅ python-dotenv is available")
    except ImportError as e:
        debug(f"❌ python-dotenv import failed: {e}")

    debug("=== End Dependencies Check ===")


def debug(msg):
    """Log debug messages to stderr"""
    sys.stderr.write(msg + "\n")
    sys.stderr.flush()

def debug_environment():
    """Debug all environment variables"""
    debug("=== Environment Variables ===")
    for key, value in os.environ.items():
        debug(f"{key}: {value}")
    debug("=== End Environment Variables ===")

def explore_filesystem():
    """Explore the filesystem to see what's available"""
    current_dir = os.path.dirname(os.path.abspath(__file__))
    debug(f"Current handler location: {current_dir}")

    # Go up levels and check what's there
    for i in range(5):
        test_path = os.path.join(current_dir, *(['..'] * i))
        abs_path = os.path.abspath(test_path)
        debug(f"Level {i} ({abs_path}): {os.listdir(abs_path) if os.path.exists(abs_path) else 'Not found'}")

        # Check specifically for _app folder
        app_path = os.path.join(abs_path, '_app')
        if os.path.exists(app_path):
            debug(f"Found _app at level {i}: {os.listdir(app_path)}")
            local_path = os.path.join(app_path, 'local')
            if os.path.exists(local_path):
                debug(f"Found _app/local: {os.listdir(local_path)}")

class handler(BaseHTTPRequestHandler):
    def do_GET(self):
        # explore_filesystem();


        debug("Hello World handler started - GET")

        # Only load env if running locally
        if is_running_locally():
            load_env()
        else:
            debug("Running in cloud environment - skipping local env loading")

        # debug_environment()

        check_dependencies()

        get_json_note_sync("eftj3nyjzwx6xf4mpjdep4jmpu", "bnl2n2hc6kldzgnusvrae3lbcy")

        self.send_response(200)
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Content-Type', 'application/json')
        self.end_headers()

        response_data = {
            "message": "Hello, World!",
            "method": "GET",
            "environment": {
                "EMS_ENVIRONMENT": os.environ.get('EMS_ENVIRONMENT', 'not set'),
                "VERCEL_ENV": os.environ.get('VERCEL_ENV', 'not set')
            },
            "timestamp": str(os.environ.get('VERCEL_REQUEST_TIME', 'not available'))
        }

        debug(f"Returning response: {response_data}")
        self.wfile.write(json.dumps(response_data, indent=2).encode())

    def do_POST(self):
        debug("Hello World handler started - POST")
        self.send_response(200)
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Content-Type', 'application/json')
        self.end_headers()

        response_data = {
            "message": "Hello, World!",
            "method": "POST",
            "environment": {
                "EMS_ENVIRONMENT": os.environ.get('EMS_ENVIRONMENT', 'not set'),
                "VERCEL_ENV": os.environ.get('VERCEL_ENV', 'not set')
            },
            "timestamp": str(os.environ.get('VERCEL_REQUEST_TIME', 'not available'))
        }

        debug(f"Returning response: {response_data}")
        self.wfile.write(json.dumps(response_data, indent=2).encode())

    def do_OPTIONS(self):
        debug("Handling OPTIONS preflight")
        self.send_response(204)
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')
        self.end_headers()
