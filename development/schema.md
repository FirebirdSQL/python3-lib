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

The integration tests are in `tests/test_schema.py`; public usage and API details are in
`docs/docs/ref-schema.md` and `docs/docs/usage-guide/schema.md`.
