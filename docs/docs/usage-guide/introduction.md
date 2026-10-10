# Usage Guide


The `firebird-lib` package provides extensions to the [firebird-driver](https://pypi.org/project/firebird-driver/) - an official
Python driver for the open source relational database [Firebird](http://www.firebirdsql.org) ®. While the
[firebird-driver](https://pypi.org/project/firebird-driver/) package provides the Python DB API 2.0 compliant interface to the
Firebird RDBMS, the `firebird-lib` contains higher level modules to work with various
information and data structures provided by Firebird.

This package provides next modules:

- `schema` - for work with Firebird [database schema](schema.md#working-with-database-schema)
    stored in system tables.
- `monitor` - for work with Firebird [monitoring tables](monitoring.md#working-with-monitoring-tables).
- `gstat` - for processing output from [gstat Firebird utility](gstat.md#processing-gstat-output).
- `log` - for processing Firebird [server log](firebird-log.md#processing-firebrid-log).
- `trace` - Processing output from Firebird server
    [trace & audit sessions](trace.md#processing-firebrid-trace).
