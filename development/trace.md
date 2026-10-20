# Trace parser

`src/firebird/lib/trace.py` parses textual Firebird trace and audit output into
`TraceEvent` subclasses and associated `TraceInfo` records. `TraceParser.parse()` splits
an iterable into event blocks. `push()` accepts individual lines and emits a completed
block when the next header arrives or on `STOP`. Associated info records precede the
event in the emitted sequence.

The parser tracks previously seen attachments, transactions, services, SQL statements,
and parameter sets, assigning stable internal IDs as it processes a stream. The event
dispatch map in `TraceParser.__init__` connects `Event` values to parser methods. When
adding a trace event, update recognition, dispatch, the relevant event/info data class,
and any identity-cache cleanup together. Check both normal and malformed blocks, as well
as `parse()` and `push()` behavior for streaming changes.

Static parser and malformed-input tests remain in `tests/test_trace.py`.
`tests/test_live_trace.py` starts user trace sessions through firebird-driver services
against the engine selected by the test backend. Its fixture reads queued output before
stopping a session through a separate service connection, then drains final records.
The direct `readline_timed()` service call avoids treating an idle live stream as EOF.
Never enable `log_service_query` for the trace reader: reading the feed would create
more query events. Statement-finish tests use `time_threshold = 0` and fully fetch
their cursors. They exercise plain and parameterized SQL with plan output absent,
legacy, and explained, each with detailed performance output off and on. Assertions
compare `parse()` and `push()` and use semantic event data instead of volatile trace
IDs, timestamps, or timings. The performance table columns are located from their
header because Firebird versions vary the spacing. Public usage is in
`docs/docs/ref-trace.md`.

Additional live cases assert rollback, commit/rollback retaining, statement prepare
and free, and trigger/procedure/function start and finish events. The executable
objects are created in the copied test database. Firebird 5 prefixes trigger names
with `Trigger ` and ends their event descriptor with `):`; the parser accepts both
this and the earlier format. A service sweep must emit start and finish; progress
events are parsed and checked when present, but the small test database does not
reliably emit them.
