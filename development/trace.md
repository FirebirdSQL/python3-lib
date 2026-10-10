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

Tests are in `tests/test_trace.py`; public usage is in `docs/docs/ref-trace.md`.
