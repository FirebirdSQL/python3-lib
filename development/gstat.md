# gstat parser

`src/firebird/lib/gstat.py` maps Firebird `gstat` text into `StatDatabase`, `StatTable`,
and `StatIndex`. Label/specification tables near the top of the module define conversions
for header, variable, table, and index fields. The `StatDatabase.push()` state machine
recognizes output sections; `parse(lines)` feeds it an iterable and finalizes with `STOP`.

The final `STOP` converts fill distributions, freezes table/index lists, and resets parser
state for another stream. A format change may require updates in both a specification
table and the section parser. Keep malformed-input errors tied to the appropriate line
and preserve partial/optional statistics sections.

`tests/test_gstat.py` covers whole-input and incremental parsing with the checked-in
`tests/gstat30-*.out` samples, plus malformed-input cases. Public usage is in
`docs/docs/ref-gstat.md`.
