FROM python:3.12-slim

WORKDIR /app

# Install system dependencies for SSH tunnel (used when enable_proxy=true)
RUN apt-get update && apt-get install -y --no-install-recommends \
    openssh-client \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies first (layer cache)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy all source code (entrypoint.py + pipeline/ + orchestrator/ packages)
COPY . .

# Create SSH directory with correct permissions for optional key mount
RUN mkdir -p /root/.ssh && chmod 700 /root/.ssh

# Non-root user would break SSH key permissions; run as root inside container.
# Kestra does not use --privileged, so this is the expected pattern.

CMD ["python", "entrypoint.py"]
