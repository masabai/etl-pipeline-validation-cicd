FROM mcr.microsoft.com/devcontainers/python:3.10

# Install OS dependencies if needed
RUN apt-get update && apt-get install -y \
    git \
    wget \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /workspace
