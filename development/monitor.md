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
Properties backed by optional monitoring columns return `None` when the column is absent.
In particular, `StatementInfo.compiled_statement` must avoid querying
`MON$COMPILED_STATEMENTS` on Firebird 3 and 4. The same applies to
`CallStackInfo.compiled_statement` when its reference column is absent or null.
Missing or null wire compression and encryption
values must not be converted to `False`; embedded attachments have no
wire transport.

`tests/test_monitor_coverage.py` compares every live `MON$` table and column with an
explicit mapping to a `Monitor` collection, info class, and public accessor. It also checks
the selected columns and retained row attributes, including the joined statistics tables.
Run it against the Firebird 3, 4, and 5 embedded backends; `local` and `localem` follow the
version detected from their engine. When Firebird adds a monitoring column, add its public
accessor and extend this mapping. A public method such as `is_gc_allowed()` counts as an
accessor; access through `_attributes` alone does not.

The integration tests are in `tests/test_monitor.py`; the user reference is
`docs/docs/ref-monitor.md`.
