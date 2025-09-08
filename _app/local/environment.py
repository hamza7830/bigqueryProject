import os
import subprocess

def is_running_in_docker():
    """Check if running in Docker container"""
    return os.path.exists('/.dockerenv') or os.environ.get('DOCKER_ENV')



def is_running_locally():
    """Check if the application is running locally (not in Vercel or AWS Fargate)"""
    # Check for Vercel environment variables
    if os.environ.get('VERCEL') or os.environ.get('VERCEL_ENV'):
        return False

    # Check for AWS Fargate environment variables
    if os.environ.get('AWS_EXECUTION_ENV') == 'AWS_ECS_FARGATE':
        return False

    # Additional AWS ECS/Fargate indicators
    if os.environ.get('ECS_CONTAINER_METADATA_URI') or os.environ.get('ECS_CONTAINER_METADATA_URI_V4'):
        return False

    # Check for Docker environment
    if os.path.exists('/.dockerenv') or os.environ.get('DOCKER_ENV'):
        return False

    return True

def load_env():
    """Load environment variables based on git branch using python-dotenv"""
    # Only run if we're in a local environment
    if not is_running_locally():
        print("Skipping env loading - running in cloud environment")
        return

    try:
        # Only import dotenv if running locally
        from dotenv import load_dotenv


        # Get current git branch
        result = subprocess.run(['git', 'branch', '--show-current'],
                              capture_output=True, text=True)
        branch_name = result.stdout.strip()

        # Load base .env file
        load_dotenv('.env')

    except ImportError:
        print("python-dotenv not available - skipping .env file loading")

    except Exception as e:
        print(f"Error loading environment: {e}")


# Don't call this at module import - let the handler decide when to call it
# load_env()


def load_env_for_local_docker():
    """Load environment variables for local Docker development"""
    # Only load if we're in Docker AND have LOCAL_DOCKER_DEV environment variable set
    if not is_running_in_docker() or not os.environ.get('LOCAL_DOCKER_DEV'):
        print("Skipping Docker env loading - not in local Docker development mode")
        return

    try:
        # Only import dotenv if available
        from dotenv import load_dotenv

        # Check if .env file exists and load it
        if os.path.exists('.env'):
            load_dotenv('.env')
            print("Loaded .env file for local Docker development")
        else:
            print("No .env file found for local Docker development")

    except ImportError:
        print("python-dotenv not available - skipping .env file loading")

    except Exception as e:
        print(f"Error loading environment for Docker: {e}")
