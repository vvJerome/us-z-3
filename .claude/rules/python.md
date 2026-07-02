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
- **Sanctioned exception:** at the outer edge of a call to an external backend (Racknerd, bbops, Zuhal, Serper, Cherry, MS probe), `except Exception` swallowed into an `error`/`status="error"` result is the intended pattern — see `dispatch_probes.safe_racknerd`/`safe_bbops`/`zuhal_probe`. The caller's reconciliation logic (`reconcile()`) is written to treat "backend errored" uniformly regardless of *why* it errored, so narrowing the except clause there would just re-implement what `reconcile()` already does. This does not license a broad `except Exception` anywhere else. Backends that *can* raise `PipelineHaltError` (Serper, Zuhal — bad API key / no credits) re-raise it before the catch-all; Racknerd and bbops have no such failure mode and don't need the explicit re-raise, but if a backend gains one, add it.
- Every `except Exception` in `pipeline/` (44 as of this writing) has been individually audited and falls into one of four buckets: (1) the backend-boundary case above, (2) explicit re-raise after cleanup or for a specific exception type first (`db/records.py`'s rollback-then-raise, `fleet/manager.py`'s `CancelledError` re-raise before the catch-all), (3) best-effort cleanup/diagnostic logging that must never crash the caller (closing a socket, draining stderr, a backup snapshot), or (4) daemon/supervisor-loop resilience where one bad cycle must not kill the loop (`fleet/control.py`, `ops/passoff_watcher.py`). None were a lazy catch-all hiding a real bug. Any *new* `except Exception` should fit one of these four, named as a comment if the reason isn't obvious from the surrounding code.

## Comments

- No comments that describe WHAT the code does — only WHY (non-obvious constraint, workaround, invariant).
- No multi-line docstrings on internal helpers — one line max.

## Modularization

- **Hard limit: no file may exceed 600 LOC.** The `post-edit-lint.sh` hook flags any file over it.
- Split by **responsibility**, never by "head/tail". When a module grows past the limit, break it into a package whose modules each own one concern, and re-export the public surface from `__init__.py` so call sites stay unchanged. Reference splits: `pipeline/db/` (schema / records / zuhal_queue / meta / patterns / enrichment / bbops_jobs) and the dispatcher (`dispatcher` + `reconcile` + `dispatch_probes` + `dispatch_verdicts`).
- A single function should not carry the whole flow — extract cohesive helpers (pure logic to its own module, side-effecting steps to named functions) rather than letting one method sprawl.
- Prefer many small, single-purpose files over one large one. Shared physics/protocol constants live in `constants.py`; operator-tunable values in `config.py` — never inline a value that is duplicated across files.
