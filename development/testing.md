# Testing and documentation workflow

Run commands from the repository root. `tests/conftest.py` initializes the Firebird client
and connects to the server during pytest configuration. Therefore even `test_gstat.py`,
`test_log.py`, or `test_trace.py` needs a working Firebird client and server in this suite.
The fixture copies a version-matched `tests/fbtest30.fdb`, `fbtest40.fdb`, or `fbtest50.fdb`
into a temporary directory and points the registered `pytest` database at that copy.
Supported fixture branches cover Firebird 3, 4, and 5.

By default the fixture uses a local server and `SYSDBA` / `masterkey`. For another setup,
pass `--host`, `--port`, `--client-lib`, `--server`, or `--driver-config` to pytest as needed.
The optional `tests/firebird-driver.conf` file is read when present; an explicit
`--driver-config` path takes precedence. Keep connection credentials in local configuration,
not in committed documentation or test data.

## Commands

```console
hatch run hatch-test.py3.11:pytest tests/test_gstat.py --host=localhost
hatch test
hatch run ruff check src/firebird/lib
git diff --check
hatch run doc:build
```

The first command runs one test file in an installed Hatch Python 3.11 test environment;
choose the Python version available locally. `hatch test` uses the configured 3.11–3.14
matrix and passes `--host=localhost`. Run focused tests first, then the applicable broader
suite when a server and the required Python environments are available. Ruff configuration
and exclusions are in `pyproject.toml`.

`hatch run doc:build` invokes Zensical using `docs/zensical.toml`. The generated site is
in `docs/site/`; `hatch run doc:docset` packages it for Dash or Zeal. Read the Docs uses
`.readthedocs.yml`. Keep user-facing examples and API pages in `docs/docs/`, and describe
implementation decisions in `development/`.
