# Monitoring module

`src/firebird/lib/monitor.py` queries Firebird `MON$` tables through `Monitor`. It keeps
an internal read-only cursor on a separate transaction manager, then presents results as
linked `*Info` objects. `monitor.db`, `attachments`, `transactions`, `statements`, and other
properties load lazily. The module also imports schema object types for links back to
database metadata.

The first query creates the monitoring snapshot. `clear()` resets the main cached
collections and commits an active internal transaction; `take_snapshot()` calls `clear()`
and starts a fresh snapshot. At present, `clear()` does not reset the compiled-statements
cache, so account for that existing behavior when working on cache invalidation.
Use the `Monitor` context manager or `close()` to release its cursor. When adding a new
`MON$` collection, include it in initialization, cache invalidation, and lifecycle paths.
Check server and ODS support before reading columns or tables introduced in later Firebird
versions; `MON$COMPILED_STATEMENTS`, for example, is tested only when available.

The integration tests are in `tests/test_monitor.py`; the user reference is
`docs/docs/ref-monitor.md`.
