#!/bin/bash

# Default values
FUNCTION_NAME="example"

# Get the directory where this script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Calculate project root relative to script location (up two levels from aws-python/example/)
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

echo "Script location: $SCRIPT_DIR"
echo "Project root: $PROJECT_ROOT"
echo "Current directory: $(pwd)"
echo "Files in current directory:"
ls -la

# Change to project root
cd "$PROJECT_ROOT"

echo "Building AWS Lambda function..."

# Fixed the build-arg syntax
docker build -f aws-python.Dockerfile --build-arg FUNCTION_NAME=$FUNCTION_NAME --build-arg LOCAL_DEV=true -t aws-python-example-local .

echo "Running with environment variables..."

# Run with .env file
docker run -p 2000:8000 --env-file .env aws-python-example-local &

# Store the container PID
CONTAINER_PID=$!

echo "Testing endpoint..."
sleep 3


# Stop the container
kill $CONTAINER_PID 2>/dev/null || true
