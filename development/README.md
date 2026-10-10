# Development guide

All paths and commands are relative to the repository root. `firebird-lib` adds optional
schema, monitoring, and text parsing facilities to `firebird-driver`. The importable code
is in `src/firebird/lib/`; tests are in `tests/`. This directory holds maintainer guidance.
The Zensical sources in `docs/docs/` are the published user documentation.

## Project layout

| Path | Purpose |
| --- | --- |
| `src/firebird/lib/` | Library modules; `__about__.py` supplies Hatch's package version |
| `tests/test_*.py` | Tests corresponding to library modules |
| `tests/conftest.py` | Shared Firebird server configuration and database fixtures |
| `tests/fbtest*.fdb`, `tests/gstat*.out` | Versioned database and parser fixtures |
| `docs/docs/` | Zensical user guide and API reference |
| `pyproject.toml` | Build, dependencies, Hatch environments, and Ruff settings |

The package requires Python `>=3.11` and depends on `firebird-base~=2.0` and
`firebird-driver~=2.0`. Hatch tests are configured for Python 3.11–3.14. Refer to
`pyproject.toml` for the current authoritative settings.

## Where to read next

- [Architecture](architecture.md): module relationships and state boundaries.
- [Testing and docs](testing.md): server setup, focused tests, lint, and Zensical builds.
- [Schema](schema.md): `RDB$` metadata, lazy collections, and DDL generation.
- [Monitoring](monitor.md): `MON$` snapshots and linked information objects.
- [gstat](gstat.md): database statistics parser and output fixtures.
- [Server log](log.md): `firebird.log` parser and `logmsgs` message definitions.
- [Trace](trace.md): trace event parser and its identity caches.

When changing behavior, update the matching test and, when users can observe the change,
the relevant Zensical page. Keep this directory focused on implementation and maintenance;
the public API description belongs in `docs/docs/`.
