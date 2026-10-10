# Agent guidance

This repository contains the `firebird-lib` Python package. Work from the repository root
when using paths or commands in the development documentation.

Start with [development/README.md](development/README.md) for the project map and workflow.
Read the relevant module guide in `development/` before changing implementation or tests.
Keep architecture and maintenance details there; `docs/` contains published user documentation.

## Working rules

- Preserve public behavior and Firebird version compatibility unless the task calls for a change.
  Check the corresponding `tests/test_<module>.py` and user documentation when changing an API.
- Diagnose failures against the actual source and test environment before editing. The shared
  pytest setup connects to a Firebird server even for parser-only tests; see
  [development/testing.md](development/testing.md) before running them.
- Add focused regression coverage for behavior changes. Run the focused tests, Ruff for changed
  Python modules, and `git diff --check`. Report server or environment limits separately.
- Follow `pyproject.toml` for supported Python versions, dependencies, Hatch settings, and Ruff
  rules. Preserve existing license headers and local code style.
- Update the relevant `development/` guide when a module's maintenance contract changes.

## References

- [Architecture and module relationships](development/architecture.md)
- [Testing and documentation workflow](development/testing.md)
- [Schema](development/schema.md) · [Monitoring](development/monitor.md)
- [gstat](development/gstat.md) · [Server log and message catalog](development/log.md) ·
  [Trace](development/trace.md)
