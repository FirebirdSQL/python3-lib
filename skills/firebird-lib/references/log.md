# Server log

`firebird.lib.log.LogParser` turns multiline `firebird.log` text into `LogMessage`
records. `parse(lines)` yields complete records from an iterable; `parse_entry(lines)`
handles one complete entry. For streaming input, `push(line)` returns a record when
the next dated header completes the previous entry. Finish with `push(STOP)` to flush
the last entry (`STOP` comes from `firebird.base.types`).

`LogMessage` provides origin, timestamp, severity level, facility, code, message,
and parsed parameters. Classification uses the known message catalog in
`firebird.lib.logmsgs`. An unrecognized message body can still produce a log record;
do not assume every record has a known severity or facility. Treat the original
message as useful evidence when classification is unavailable.
