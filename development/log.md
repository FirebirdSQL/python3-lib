# Server log parser and message catalog

`src/firebird/lib/log.py` groups multiline `firebird.log` entries. `LogParser.push()`
buffers lines until a new dated header or `STOP`; `parse()` yields complete `LogMessage`
records and flushes the final entry. `parse_entry()` validates the header and asks
`logmsgs.identify_msg()` to classify the body. Unknown message bodies remain available
as records with unknown severity and facility.

`src/firebird/lib/logmsgs.py` contains `MsgDesc` definitions, severity and facility enums,
and the matcher. A descriptor's `msg_format` mixes literal parts, typed placeholders,
and an optional `OPTIONAL` suffix. Its index groups candidates by the first fixed word,
with a separate group for messages beginning with a variable. When adding a descriptor,
check whether its leading text and placeholders can distinguish it from existing entries,
and verify both complete and optional forms.

`tests/test_log.py` covers log parsing and known-message matching. User references are
`docs/docs/ref-log.md` and `docs/docs/ref-logmsgs.md`.
