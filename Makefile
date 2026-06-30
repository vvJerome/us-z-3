.PHONY: setup check test typecheck

setup:
	python3 -m venv .venv
	.venv/bin/pip install -r requirements.txt

check: test typecheck

test:
	.venv/bin/python -m pytest tests/ -q

typecheck:
	.venv/bin/mypy pipeline/
