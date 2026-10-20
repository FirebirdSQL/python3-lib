# SPDX-FileCopyrightText: 2020-present The Firebird Projects <www.firebirdsql.org>
#
# SPDX-License-Identifier: MIT
#
# PROGRAM/MODULE: firebird-lib
# FILE:           tests/test_gstat.py
# DESCRIPTION:    Tests for firebird.lib.gstat module
# CREATED:        25.4.2025
#
# The contents of this file are subject to the MIT License
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
#
# Copyright (c) 2020 Firebird Project (www.firebirdsql.org)
# All Rights Reserved.
#
# Contributor(s): Pavel Císař (original code)

"""Tests for the firebird.lib.gstat parser."""

import datetime
from collections.abc import Mapping, MutableSequence, Sized

import pytest

from firebird.base.types import STOP, Error
from firebird.driver import SrvStatFlag
from firebird.lib.gstat import DbAttribute, Encryption, FillDistribution, StatDatabase


def get_object_data(obj, skip=()):
    """Return public fields and properties for comparing both parser entry points."""
    def value_of(value):
        return len(value) if isinstance(value, Sized) and isinstance(value, (MutableSequence, Mapping)) else value

    data = {}
    for name in vars(obj):
        if not name.startswith('_') and name not in skip:
            data[name] = value_of(getattr(obj, name))
    for name in dir(obj):
        if name not in skip and isinstance(getattr(type(obj), name, None), property):
            data[name] = value_of(getattr(obj, name))
    return data


@pytest.mark.parametrize('case', [
    pytest.param(('h', SrvStatFlag.HDR_PAGES, (False, False, False, False, False)), id='header'),
    pytest.param(('a', SrvStatFlag.DEFAULT, (True, True, False, False, False)), id='all'),
    pytest.param(('d', SrvStatFlag.DATA_PAGES, (True, False, False, False, False)), id='data'),
    pytest.param(('e', SrvStatFlag.ENCRYPTION, (False, False, False, True, False)), id='encryption'),
    pytest.param(('f', SrvStatFlag.DEFAULT | SrvStatFlag.SYS_RELATIONS | SrvStatFlag.RECORD_VERSIONS,
                  (True, True, True, False, True)), id='full'),
    pytest.param(('i', SrvStatFlag.IDX_PAGES, (False, True, False, False, False)), id='indices'),
    pytest.param(('r', SrvStatFlag.DEFAULT | SrvStatFlag.RECORD_VERSIONS,
                  (True, True, True, False, False)), id='records'),
    pytest.param(('s', SrvStatFlag.DEFAULT | SrvStatFlag.SYS_RELATIONS,
                  (True, True, False, False, True)), id='system'),
])
def test_live_gstat_fb3(case, fb_vars, db_file, server_connection):
    """Parse the same complete service report through parse() and push()."""
    mode, flags, features = case
    version = fb_vars['version']
    if version.major != 3:
        pytest.skip(f'Live gstat assertions are supported on Firebird 3; found {version}')

    server_connection.database.get_statistics(database=db_file, flags=flags)
    lines = server_connection.readlines()
    assert lines
    assert any('Gstat completion time' in line for line in lines)

    parsed = StatDatabase()
    parsed.parse(lines)
    pushed = StatDatabase()
    for line in lines:
        pushed.push(line)
    pushed.push(STOP)

    assert get_object_data(parsed) == get_object_data(pushed)
    assert [get_object_data(table) for table in parsed.tables] == [
        get_object_data(table) for table in pushed.tables
    ]
    assert [get_object_data(index, skip=('table',)) for index in parsed.indices] == [
        get_object_data(index, skip=('table',)) for index in pushed.indices
    ]
    assert [index.table.name for index in parsed.indices] == [
        index.table.name for index in pushed.indices
    ]

    assert parsed.filename == str(db_file)
    assert parsed.gstat_version == 3
    assert parsed.ods_version == '12.0'
    assert parsed.page_size == 8192
    assert parsed.database_dialect == 3
    assert DbAttribute.WRITE in parsed.attributes
    assert isinstance(parsed.executed, datetime.datetime)
    assert isinstance(parsed.completed, datetime.datetime)
    assert parsed.executed <= parsed.completed
    assert (parsed.has_table_stats(), parsed.has_index_stats(), parsed.has_row_stats(),
            parsed.has_encryption_stats(), parsed.has_system()) == features

    if mode == 'h':
        assert not parsed.tables
        assert not parsed.indices
    if mode in ('a', 'd', 'f', 'r', 's'):
        country = parsed.tables.get('COUNTRY')
        assert country is not None
        assert country.table_id == 128
        assert country.data_pages >= 1
        assert isinstance(country.distribution, FillDistribution)
    if mode == 'i':
        country = parsed.tables.get('COUNTRY')
        assert country is not None
        assert country.primary_pointer_page is None
        assert country.distribution is None
    if mode in ('a', 'f', 'i', 'r', 's'):
        primary = parsed.indices.get('RDB$PRIMARY1')
        assert primary is not None
        assert primary.table.name == 'COUNTRY'
        assert primary.depth >= 1
        assert isinstance(primary.distribution, FillDistribution)
    if mode in ('r', 'f'):
        ar = parsed.tables.get('AR')
        assert ar is not None
        assert ar.avg_version_length is not None
        assert ar.total_records >= 0
    if mode in ('f', 's'):
        assert parsed.tables.get('RDB$DATABASE') is not None
        assert parsed.tables.get('RDB$RELATIONS') is not None
    if mode == 'e':
        for value in (parsed.encrypted_data_pages, parsed.encrypted_index_pages,
                      parsed.encrypted_blob_pages, parsed.encrypted_generator_pages):
            assert isinstance(value, Encryption)
            assert value.pages == value.encrypted + value.unencrypted
        assert parsed.encrypted_generator_pages.pages >= 1


def test_parse_generator_page_encryption():
    """Generator-page totals are retained when present in an encryption report."""
    db = StatDatabase()
    db.parse(['Generator pages: total 2, encrypted 1, non-crypted 1'])
    assert db.encrypted_generator_pages == Encryption(pages=2, encrypted=1, unencrypted=1)


# --- Check for edge cases and wrong input ---

def test_17_parse_malformed_header_line():
    db = StatDatabase()
    lines = [
        "Database header page information:",
        "Flags ThisIsNotANumber", # Malformed Flags value
    ]
    with pytest.raises(ValueError, match=r"Unknown information \(line 2\)|invalid literal for int"):
        db.parse(lines) # Or use push

def test_18_push_unrecognized_data_in_step0():
    db = StatDatabase()
    with pytest.raises(Error, match=r"Unrecognized data \(line 1\)"):
        db.push("Some unexpected line before any section")
        db.push(STOP) # Need STOP to finalize if push doesn't raise immediately

def test_19_parse_malformed_table_header():
    db = StatDatabase()
    lines = [
         "Analyzing database pages ...",
         "MYTABLE (BadID)", # Malformed table ID
     ]
    with pytest.raises(ValueError, match=r"invalid literal for int|could not split"):
        db.parse(lines)

def test_20_parse_malformed_encryption_line():
    db = StatDatabase()
    lines = [
         "Data pages: total 100, encrypted lots, non-crypted 50" # Malformed encrypted value
     ]
    with pytest.raises(Error, match=r"Malformed encryption information"):
        db.parse(lines)

def test_21_parse_unknown_table_stat():
    db = StatDatabase()
    lines = [
         "Analyzing database pages ...",
         'MYTABLE (128)',
         '    Primary pointer page: 10, Unknown Stat: Yes', # Unknown item
     ]
    with pytest.raises(Error, match=r"Unknown information \(line 3\)"):
        db.parse(lines)

def test_22_parse_unsupported_gstat_version():
    db = StatDatabase()
    lines = [
         "Database header page information:",
         "Checksum     12345", # Indicator of old version
     ]
    with pytest.raises(Error, match="Output from gstat older than Firebird 3 is not supported"):
        db.parse(lines)

def test_23_parse_empty_input():
    db = StatDatabase()
    db.parse([])
    # Assert initial state - no data, counts are 0
    assert db.filename is None
    assert len(db.tables) == 0
    assert len(db.indices) == 0
    assert db.gstat_version is None

def test_24_push_stop_immediately():
    db = StatDatabase()
    db.push(STOP)
    # Assert initial state
    assert db.filename is None
    assert len(db.tables) == 0
    assert len(db.indices) == 0

def test_25_parse_bad_file_spec():
    db = StatDatabase()
    lines = [
        "Database file sequence:",
        "File /path/db.fdb has an unexpected format",
    ]
    with pytest.raises(Error, match="Bad file specification"):
        db.parse(lines)

def test_26_parse_bad_date_in_header():
    db = StatDatabase()
    lines = [
         "Database header page information:",
         "Creation date:    Not A Date String",
     ]
    with pytest.raises(ValueError): # Catches strptime error
        db.parse(lines)

def test_27_parse_bad_float_in_table():
    db = StatDatabase()
    lines = [
        "Analyzing database pages ...",
        'MYTABLE (128)',
        '    Average record length: abc',
    ]
    with pytest.raises(Error, match="Unknown information"): # Catches float() error
        db.parse(lines)

def test_28_parse_bad_fill_range():
    db = StatDatabase()
    lines = [
        "Analyzing database pages ...",
        'MYTABLE (128)',
        '    Fill distribution:',
        '    10 - 30% = 5', # Invalid range
    ]
    with pytest.raises(ValueError): # Catches items_fill.index() error
        db.parse(lines)

def test_29_parse_unknown_db_attribute():
    db = StatDatabase()
    lines = [
        "Database header page information:",
        "Attributes:       force write, unknown attribute",
    ]
    with pytest.raises(ValueError, match="is not a valid DbAttribute"):
        db.parse(lines)

def test_30_push_unexpected_state_transition():
    db = StatDatabase()
    db.push("Database header page information:") # Enter step 1
    # Now push a line only valid in step 4
    # Expect it to raise "Unknown information" as it won't match items_hdr
    with pytest.raises(Error, match=r"Unknown information \(line 2\)"):
        db.push('MYTABLE (128)')
    db.push(STOP)
