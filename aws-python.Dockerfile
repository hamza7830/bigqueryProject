FROM python:3.12-slim

# Build arguments
ARG FUNCTION_NAME=example
ARG FUNCTION_PATH=aws-python/${FUNCTION_NAME}
ARG LOCAL_DEV=false

# Set the working directory
WORKDIR /app

# Set environment variable to identify Docker environment
ENV DOCKER_ENV=1

# Conditionally set LOCAL_DOCKER_DEV for local development
RUN if [ "$LOCAL_DEV" = "true" ]; then echo "LOCAL_DOCKER_DEV=1" >> /etc/environment; fi
ENV LOCAL_DOCKER_DEV=${LOCAL_DEV}

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements.txt files
COPY requirements.txt ./
COPY ${FUNCTION_PATH}/requirements.txt ./requirements-function.txt

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt
RUN pip install --no-cache-dir -r requirements-function.txt

# Copy shared directories
COPY _app ./_app
COPY _helper ./_helper

# Copy function code
COPY ${FUNCTION_PATH}/ ./

# Run the script
CMD ["python", "handler.py"]



