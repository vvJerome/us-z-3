# Python Rules

## Version and runtime

- Python 3.10+ minimum. Use `match/case`, `X | Y` union types, built-in generics (`list[str]`, `dict[str, int]`).
- Do not use `typing.Dict`, `typing.List`, `typing.Optional`, `typing.Tuple` — deprecated since 3.9.
- `from __future__ import annotations` at the top of every module (enables deferred evaluation for forward references).

## Async

- All I/O is async. No `requests` in async context (ms_verify.py is the only exception — it uses `asyncio.to_thread`).
- Use `asyncio.get_running_loop()`, never `asyncio.get_event_loop()`.
- Prefer `asyncio.gather(*tasks)` for concurrent work over sequential awaits.
- Never `await` inside a `with` block that holds a lock unless the lock itself is async (`asyncio.Lock`).

## Imports

- Standard library → third-party → local. One blank line between groups.
- No wildcard imports (`from x import *`).
- No unused imports. If a re-export is intentional, add `# noqa: F401`.

## Logging

- Use `logger = logging.getLogger("pipeline.modulename")` — never `print()` in library code.
- No f-strings in log calls: `logger.info("x=%s", x)` not `logger.info(f"x={x}")` — deferred formatting is cheaper and safer.
- Structured extra fields: `logger.info("msg", extra={"stage": "dns", "outcome": "hit"})`.

## Type hints

- All function signatures have parameter and return type annotations.
- Use `X | None` not `Optional[X]`.
- Use `str | None` default parameters, not sentinel objects.

## Error handling

- Catch specific exceptions, not bare `except Exception` unless re-raising.
- `PipelineHaltError` must always be re-raised — never swallowed.
- Transient errors go to `logger.warning`; fatal errors to `logger.error` before raising.

## Comments

- No comments that describe WHAT the code does — only WHY (non-obvious constraint, workaround, invariant).
- No multi-line docstrings on internal helpers — one line max.
