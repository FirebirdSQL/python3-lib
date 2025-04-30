# Change Log
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/)
and this project adheres to [Semantic Versioning](http://semver.org/).

## [2.0.0] - 2025-04-30

### Changed

* Test changed from `unittest` to `pytest`, more tests for greater coverage.
* Minimal Python version raised to 3.11
* Improved documentation
* Parameter `any_` in `firebird.lib.schema.FunctionArgument.is_by_descriptor` was replaced
  by `any_desc` keyword-only argument.
* Parameter `without_optional` in `firebrid.lib.logmsg.MsgDesc` was changed to keyword-only.
* `firebird.lib.schema.CharacterSet.default_colate` was renamed to `default_colation`.

### Added

* Added `firebird.lib.schema.Privilege.is_usage` method.

## [1.5.1] - 2025-04-25

### Fixed

- Bug in `schema_get_all_indices` with ODS 13.0

### Changed

- Dependencies fixed to `firebird-base~=1.8` and `firebird-driver~=1.10`

## [1.5.0] - 2023-10-03

### Changed

- Build system changed from setuptools to hatch
- Package version is now defined in `firebird.lib.__about__.py` (`__version__`)

## [1.4.0] - 2023-06-30

### Added

- Initial support for Firebird 5 (new items in `schema` and `monitor` modules).
- Finally, Firebird 4 support was added (new items in `schema` and `monitor` modules).

### Changed

- Potentially breaking changes:

  - Enum `firebird.lib.monitor.ShutdownMode` was removed and replaced with
    `firebird.driver.types.ShutdownMode`. They are basically the same, but differ in value
    name ONLINE->NORMAL.
  - `firebird.lib.schema.ObjectType` value `PACKAGE` was renamed to `PACKAGE_HEADER`

- Updated dependencies: firebird-driver>=1.9.0 and firebird-base>=1.6.1
- Note: The list of reserved words (used internally to correctly quote identifiers) is
  not hardcoded in Firebird 5, but is instead read from `RDB$KEYWORDS` table.

## [1.3.0] - 2023-03-03

### Changed

- Move away from `setup.cfg` to `pyproject.toml`, changed source three layout.

## [1.2.2] - 2022-10-14

### Changed

- Further code optimizations.
- Addressing issues reported by pylint.
- Improved documentation.

## [1.2.1] - 2022-10-03

### Fixed

- schema: Fixed problems with system PSQL functions and system packages.
- Tests now properly work on Firebird 4.0

### Added

- Documentation is now also provided as Dash / Zeal docset, downloadable from releases at github.

### Changed

- Code optionizations.

## [1.2.0] - 2021-10-13

### Fixed

- schema: Fix index type in `Constraint` and `Table` CREATE SQL.
- trace: Fixed several unregistered bugs in parser.

### Added

- schema: `insert` SQL for `Table`.
- trace: `TransactionInfo.initial_id`.
- trace: Items `EventCommitRetaining.new_transaction_id` and `EventRollbackRetaining.new_transaction_id`.
- trace: Events `EventFunctionStart` and `EventFunctionFinish`.
- trace: Item `EventSweepFinish.access`.

### Changed

- schema: `Sequence` ALTER SQL uses RESTART instead START keyword.
- trace: `EventServiceQuery.parameters` was replaced by `EventServiceQuery.sent` and
         `EventServiceQuery.received`.

### Removed

- trace: `EventFreeStatement.transaction_id` and `EventCloseCursor.transaction_id` were removed.

## [1.0.1] - 2021-03-04

### Added

- trace: New `has_statement_free` parsing option indicating that parsed trace contains
  `FREE_STATEMENT` events.

### Changed

- Build scheme changed to `PEP 517`.
- Various changes to documentation and type hint adjustments.
- trace: Adjustments to seen items cache management.


## [1.0.0] - 2020-10-13

Initial release.
