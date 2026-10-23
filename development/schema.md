# Schema module

`src/firebird/lib/schema.py` reads Firebird `RDB$` system tables and exposes them through
`Schema` and `SchemaItem` subclasses. Callers can bind a standalone `Schema` to a driver
connection or access the driver-managed `Connection.schema`. The embedded instance is
owned by the connection; its public `bind()` and `close()` operations reject direct use.

Metadata collections load on first access and remain cached. `clear()` drops cached
collections; `reload()` can target selected `Category` values. `Schema.bind()` prepares
the internal read-only cursor, reads ODS-dependent metadata, and initializes code maps.
When adding a metadata category, account for its cache initialization, clear/reload path,
query, collection type, and version availability together.

Object classes provide related-object links, DDL generation through `get_sql_for()`, and
visitor dispatch. `Schema.get_metadata_ddl()` orders a full metadata script by `Section`.
Changes to SQL generation should preserve identifier quoting options, object dependencies,
and the distinctions between user and system objects. Version-specific SQL must be tested
against the corresponding fixture database.

`Role.system_privileges` decodes the `RDB$SYSTEM_PRIVILEGES` bitset using the
`RDB$TYPES` code-to-name mapping. The bitset exists from Firebird 4 onward; a Firebird 3
role has no such column and returns an empty set.

From Firebird 4, `RDB$SQL_SECURITY` is a nullable Boolean on the database, relations,
triggers, procedures, functions, and packages. The public `sql_security` properties
translate `TRUE` to `DEFINER`, `FALSE` to `INVOKER`, and `NULL` or a missing Firebird 3
column to `None`. `None` means no explicit clause in generated DDL, preserving inheritance
from the table or database default. Explicit object clauses are generated for table,
trigger, standalone PSQL procedure/function, and package header actions; package bodies
and views have no SQL SECURITY clause. `Section.DATABASE_SECURITY` precedes object creation
in `SCRIPT_DEFAULT_ORDER` and emits `ALTER DATABASE SET DEFAULT SQL SECURITY ...` only for
an explicit database setting.

The integration tests are in `tests/test_schema.py`; public usage and API details are in
`docs/docs/ref-schema.md` and `docs/docs/usage-guide/schema.md`.

## System metadata gap report

Run `tools/check_schema.py` from the repository root in a Hatch environment for the
Firebird engine being checked:

```console
hatch run tools3:python tools/check_schema.py
hatch run tools4:python tools/check_schema.py
hatch run tools5:python tools/check_schema.py
```

The `tools3`, `tools4`, and `tools5` environments are defined in `pyproject.toml` and
install the corresponding embedded provider. Without a registered provider, the script
uses the local Firebird installation. It creates and removes a temporary database and prints
a Markdown report with the actual engine version. Every `RDB$` table except those in
`EXCLUDE_TABLES` is listed; tables queried by `schema.py` also list columns not consumed
by code and columns whose source table is uncertain. `EXCLUDE_COLUMNS` removes known
false positives from those lists and counts. The analysis is static and reports candidates
for review, not a demand for a public property for every engine field.

The report separately checks every `RDB$TYPES` row against dictionaries populated by
`Schema.bind()`. It reports unmapped groups and rows whose code/name pair is absent or
overwritten in a dictionary. The internal `_map_to_type_` dictionary counts as a mapping.
