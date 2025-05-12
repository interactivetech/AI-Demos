# Use python:3.10-slim as base image for amd64
FROM --platform=linux/amd64 python:3.10-slim

# Avoid interactive prompts & ensure pip is up to date
ENV DEBIAN_FRONTEND=noninteractive

# Install OS dependencies (needed for TensorFlow and some Python packages)
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        build-essential \
        libffi-dev \
        libssl-dev \
        wget \
        curl \
        git \
        && rm -rf /var/lib/apt/lists/*

# Upgrade pip and install Python packages
RUN pip install --upgrade pip && \
    pip install minio tensorflow==2.19.0

# Default command
CMD ["python3"]