# Trace and audit output

`firebird.lib.trace.TraceParser` parses Firebird trace text into `TraceInfo` and
`TraceEvent` objects. `parse(lines)` yields complete objects from an iterable.
Information records associated with an event precede that event in the output.

`push(line)` supports incremental input and returns a list of completed objects or
`None`. Finish with `push(STOP)` (`STOP` comes from `firebird.base.types`) to flush
the final event. `parse_event(block)` handles one event block; afterwards call
`retrieve_info()` to get associated `TraceInfo` records. That retrieval drains the
info buffer, so consume it before parsing another block.

The parser assigns stable internal identities to repeated attachments, transactions,
services, statements, and parameter sets within a stream. Keep a parser instance for
one logical stream when those relationships matter. Input format can vary by Firebird
version; verify event classes and fields against the installed package.
