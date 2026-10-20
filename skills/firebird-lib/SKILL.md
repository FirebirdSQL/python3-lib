---
name: firebird-lib
description: Use firebird-lib in Python applications to inspect Firebird schema and monitoring data or parse gstat, server log, and trace output. Use for firebird.lib APIs, not ordinary SQL or connection setup handled by firebird-driver.
---

# Use firebird-lib

`firebird-lib` extends `firebird-driver` with metadata, monitoring, and text parsers.
Import from `firebird.lib.<module>`; the `firebird.lib` package root does not reexport
the module APIs. The library does not replace `firebird-driver` connections or its
Services API.

Read only the reference for the task. For work spanning modules, read the relevant
references together.

| Task | Reference |
| --- | --- |
| Inspect database objects, dependencies, privileges, or generate DDL | [Schema](references/schema.md) |
| Inspect attachments, transactions, statements, or I/O snapshots | [Monitoring](references/monitor.md) |
| Parse database statistics produced by gstat or the Services API | [gstat](references/gstat.md) |
| Parse `firebird.log` entries or identify server messages | [Server log](references/log.md) |
| Parse trace and audit events | [Trace](references/trace.md) |

Check the target project's installed `firebird-lib` version before relying on an exact
symbol or signature. Check the connected Firebird engine version for metadata that
varies by server release. The package supports Firebird 3 and later, but a feature
present in one engine release may be absent or null in another. For exact API details,
consult the installed package's public docstrings or the [published documentation](https://firebird-lib.rtfd.io/).
