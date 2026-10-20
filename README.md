# firebird-lib

## The Python library for Firebird

[![PyPI - Version](https://img.shields.io/pypi/v/firebird-lib.svg)](https://pypi.org/project/firebird-lib)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/firebird-lib.svg)](https://pypi.org/project/firebird-lib)
[![Hatch project](https://img.shields.io/badge/%F0%9F%A5%9A-Hatch-4051b5.svg)](https://github.com/pypa/hatch)
[![PyPI - Downloads](https://img.shields.io/pypi/dm/firebird-lib)](https://pypi.org/project/firebird-lib)
[![Libraries.io SourceRank](https://img.shields.io/librariesio/sourcerank/pypi/firebird-lib)](https://libraries.io/pypi/firebird-lib)
[![Ask DeepWiki](https://deepwiki.com/badge.svg)](https://deepwiki.com/FirebirdSQL/python3-lib)

This package provides optional extensions for [firebird-driver](https://pypi.org/project/firebird-driver/).

Currently it contains modules for:

* Work with Firebird database schema.
* Work with Firebird monitoring tables.
* Processing output from gstat Firebird utility.
* Processing Firebird server log.
* Processing output from Firebird server trace & audit sessions.

-----

**Table of Contents**

- [Installation](#installation)
- [Agent skill](#agent-skill)
- [License](#license)
- [Documentation](#documentation)
- [Development](#development)

## Installation

Requires: Firebird 3+

```console
pip install firebird-lib
```

## Agent skill

The [firebird-lib skill](skills/firebird-lib/SKILL.md) helps coding agents use this
package in other projects. Copy the entire `skills/firebird-lib` directory, including
its `references`, into the user skill directory for your agent:

| Agent | User skill directory |
| --- | --- |
| Codex | `~/.codex/skills/` |
| Claude Code | `~/.claude/skills/` |
| Gemini CLI | `~/.agents/skills/` |

For example, from a checkout of this repository, run
`cp -R skills/firebird-lib ~/.codex/skills/` to make the skill available to Codex
across projects. Use the corresponding directory for another agent.

## License

`firebird-lib` is distributed under the terms of the [MIT](https://spdx.org/licenses/MIT.html) license.

## Documentation

The documentation for this package is available at [https://firebird-lib.rtfd.io](https://firebird-lib.rtfd.io)

## Development

See the [development guide](development/README.md) for the project layout, test setup,
and module-specific maintenance notes.

