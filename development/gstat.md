# gstat parser

`src/firebird/lib/gstat.py` maps Firebird `gstat` text into `StatDatabase`, `StatTable`,
and `StatIndex`. Label/specification tables near the top of the module define conversions
for header, variable, table, and index fields. The `StatDatabase.push()` state machine
recognizes output sections; `parse(lines)` feeds it an iterable and finalizes with `STOP`.

The final `STOP` converts fill distributions, freezes table/index lists, and resets parser
state for another stream. A format change may require updates in both a specification
table and the section parser. Keep malformed-input errors tied to the appropriate line
and preserve partial/optional statistics sections.

`tests/test_gstat.py` requests live gstat reports through the driver Services API for each
supported report mode, then checks both whole-input and incremental parsing of the same
lines. These live cases currently run only on Firebird 3; other engine versions skip them
until their output is supported. Synthetic malformed-input cases run on every backend.
The encryption report can include generator-page totals; keep those in
`StatDatabase.encrypted_generator_pages`. Public usage is in `docs/docs/ref-gstat.md`.
