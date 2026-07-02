FROM python:3.12-slim

WORKDIR /app

# Install system dependencies for SSH tunnel (used when enable_proxy=true)
RUN apt-get update && apt-get install -y --no-install-recommends \
    openssh-client \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies first (layer cache). Pinned via requirements.lock
# (regenerate with `make lock` after any requirements.txt change) so the image
# that gets built is reproducible, not whatever the range resolves to today.
#
# requirements.lock is one file shared with local dev on purpose (a prod/dev
# split was tried and reverted before — see git history) so it includes test
# tooling; strip it back out here so the shipped image doesn't carry mypy/
# pytest/flake8 into production.
COPY requirements.lock .
RUN pip install --no-cache-dir -r requirements.lock \
    && pip uninstall -y mypy flake8 pytest pytest-asyncio pytest-cov pytest-socket \
        aioresponses autoflake types-requests pip-tools

# Copy all source code (entrypoint.py + pipeline/ + orchestrator/ packages)
COPY . .

# Create SSH directory with correct permissions for optional key mount
RUN mkdir -p /root/.ssh && chmod 700 /root/.ssh

# Non-root user would break SSH key permissions; run as root inside container.
# Kestra does not use --privileged, so this is the expected pattern.

CMD ["python", "entrypoint.py"]
