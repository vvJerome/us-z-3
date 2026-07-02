.PHONY: setup check test typecheck lock

# Use the local venv when present (local dev, after `make setup`); otherwise
# fall back to whatever's on PATH (CI, which installs deps into the runner's
# system Python directly rather than provisioning a venv).
PYTHON := $(shell test -x .venv/bin/python && echo .venv/bin/python || echo python3)
MYPY := $(shell test -x .venv/bin/mypy && echo .venv/bin/mypy || echo mypy)

setup:
	python3 -m venv .venv
	.venv/bin/pip install -r requirements.lock

check: test typecheck

test:
	$(PYTHON) -m pytest tests/ -q

typecheck:
	$(MYPY) pipeline/

# Regenerate requirements.lock from requirements.txt. Run after any change to
# requirements.txt and commit the result — a stale lock is worse than none.
lock:
	$(PYTHON) -m piptools compile requirements.txt --output-file=requirements.lock --resolver=backtracking
