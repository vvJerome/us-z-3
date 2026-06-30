.PHONY: check test typecheck

check: test typecheck

test:
	.venv/bin/python -m pytest tests/ -q

typecheck:
	.venv/bin/mypy pipeline/
