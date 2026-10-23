# The Python library for Firebird

The `firebird-lib` package provides extensions to [firebird-driver](https://pypi.org/project/firebird-driver/) - an official Python driver for the open source relational database [Firebird](http://www.firebirdsql.org) ®.

This package provides modules for:

- Work with Firebird database schema.
- Work with Firebird monitoring tables.
- Processing output from gstat Firebird utility.
- Processing Firebird server log.
- Processing output from Firebird server trace & audit sessions.

Start with the [Usage Guide](usage-guide/introduction.md).

## Agent skill

The [firebird-lib skill](https://github.com/FirebirdSQL/python3-lib/tree/master/skills/firebird-lib)
helps coding agents use this package. Download `firebird-lib-<version>-skill.zip`
from the matching [GitHub release](https://github.com/FirebirdSQL/python3-lib/releases)
when the release includes the skill archive. Extract it into your agent's user
skill directory. The ZIP contains the `firebird-lib` directory, including
`SKILL.md` and its `references`. For Codex, run
`unzip firebird-lib-<version>-skill.zip -d ~/.codex/skills/`.

!!! note
    Driver development is sponsored by [IBPhoenix](http://www.ibphoenix.com).

    [![PyPI - Version](https://img.shields.io/pypi/v/firebird-lib.svg)](https://pypi.org/project/firebird-lib)
    [![PyPI - Python Version](https://img.shields.io/pypi/pyversions/firebird-lib.svg)](https://pypi.org/project/firebird-lib)
    [![Hatch project](https://img.shields.io/badge/%F0%9F%A5%9A-Hatch-4051b5.svg)](https://github.com/pypa/hatch)
    [![PyPI - Downloads](https://img.shields.io/pypi/dm/firebird-lib)](https://pypi.org/project/firebird-lib)
    [![Libraries.io SourceRank](https://img.shields.io/librariesio/sourcerank/pypi/firebird-lib)](https://libraries.io/pypi/firebird-lib)
    [![Ask DeepWiki](https://deepwiki.com/badge.svg)](https://deepwiki.com/FirebirdSQL/python3-lib)

    [Source repository](https://github.com/FirebirdSQL/python3-lib)

!!! tip
    You can download docset for [Dash](https://kapeli.com/dash) (MacOS) or [Zeal](https://zealdocs.org/) (Windows / Linux) documentation
    readers from [releases](https://github.com/FirebirdSQL/python3-lib/releases) at github.
