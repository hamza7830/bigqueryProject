from http.server import BaseHTTPRequestHandler, HTTPServer
import json
import os
import sys
from faker import Faker

# Add current directory to Python path for Docker
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, current_dir)


# Now you can import from _app / _helper
from _app.local.environment import load_env, is_running_locally
from _helper.one_password import get_json_note_sync


def debug(msg):
    """Log debug messages to stderr"""
    sys.stderr.write(msg + "\n")
    sys.stderr.flush()

# Now try to import
try:
    from _app.local.environment import load_env, is_running_locally, load_env_for_local_docker
    debug("✅ Successfully imported from _app")
except ImportError as e:
    debug(f"❌ Failed to import from _app: {e}")
    # Create dummy functions to avoid errors
    def load_env():
        pass
    def is_running_locally():
        return False
    def load_env_for_local_docker():
        pass

try:
    from _helper.one_password import get_json_note_sync
    debug("✅ Successfully imported from _helper")
except ImportError as e:
    debug(f"❌ Failed to import from _helper: {e}")


def check_dependencies():
    """Check if required dependencies are available"""
    debug("=== Checking Dependencies ===")

    try:
        import onepassword
        debug("✅ onepassword-sdk is available")
        debug(f"onepassword version: {onepassword.__version__ if hasattr(onepassword, '__version__') else 'unknown'}")
    except ImportError as e:
        debug(f"❌ onepassword-sdk import failed: {e}")

    debug("=== End Dependencies Check ===")

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

def handler():
    """Run the handler logic once and exit"""
    debug("Hello World handler started")

    fake = Faker()
    debug('faker test')
    debug(fake.text())

    # Your existing logic here...
    check_dependencies()

    try:
        debug("About to call get_json_note_sync...")
        result = get_json_note_sync("eftj3nyjzwx6xf4mpjdep4jmpu", "bnl2n2hc6kldzgnusvrae3lbcy")
        debug(f"get_json_note_sync returned: {result}")
    except Exception as e:
        debug(f"get_json_note_sync failed: {e}")

    print("Task completed successfully!")

if __name__ == '__main__':
    handler()

