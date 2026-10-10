# Architecture

`src/firebird/lib/__init__.py` does not provide the package's API. Import the relevant
`firebird.lib.<module>` directly. `schema` and `monitor` use live `firebird-driver`
connections; the parsers work on supplied text and share types from `firebird-base`.

| Module | Responsibility | Main relationship |
| --- | --- | --- |
| [schema](schema.md) | Read `RDB$` metadata and generate object DDL | Uses a driver connection and `DataList` |
| [monitor](monitor.md) | Read `MON$` snapshots as linked info objects | Uses a driver connection and schema object types |
| [gstat](gstat.md) | Parse `gstat` output into database, table, and index statistics | Uses `DataList` and `STOP` |
| [log](log.md) | Split and parse `firebird.log` entries | Matches text through `logmsgs` |
| [logmsgs](log.md) | Define known server messages and extract their parameters | Used by `log` |
| [trace](trace.md) | Parse trace and audit text into events and info records | Uses `STOP` and parser-owned identity maps |

The two database modules keep a dedicated read-only, read-committed cursor/transaction
on the supplied connection. They cache fetched data and must release their cursors when
closed. Avoid using a caller's active transaction as an implicit replacement for these
internal cursors. Their compatibility branches depend on Firebird's ODS and server version;
check the matching tests when adding a version-specific column or object.

`gstat`, `log`, and `trace` support streaming input. Their `push()` paths retain parser
state across lines, and the `STOP` sentinel ends a stream. Changes to a text format should
cover both whole-input and incremental parsing where both APIs apply. Preserve the structured
output classes and the ordering of emitted trace information and events.
