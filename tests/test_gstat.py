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

"""firebird-lib - Tests for firebird.lib.gstat module
"""

import pytest
import os
import datetime
from collections.abc import Sized, MutableSequence, Mapping
from re import finditer
from pathlib import Path
from firebird.lib.gstat import *

# --- Helper Functions ---

def linesplit_iter(string):
    return (m.group(2) for m in finditer('((.*)\n|(.+)$)', string))

def iter_obj_properties(obj):
    for varname in dir(obj):
        if hasattr(type(obj), varname) and isinstance(getattr(type(obj), varname), property):
            yield varname

def iter_obj_variables(obj):
    for varname in vars(obj):
        value = getattr(obj, varname)
        if not callable(value) and not varname.startswith('_'):
            yield varname

def get_object_data(obj, skip=[]):
    """Extracts attribute and property data from an object into a dictionary."""
    data = {}
    def add(item):
        if item not in skip:
            value = getattr(obj, item)
            # Store length for sized collections/mappings instead of the full object
            if isinstance(value, Sized) and isinstance(value, (MutableSequence, Mapping)):
                value = len(value)
            data[item] = value

    for item in iter_obj_variables(obj):
        add(item)
    for item in iter_obj_properties(obj):
        add(item)
    return data

def _parse_file(filename: Path):
    """Helper to parse a gstat output file."""
    db = StatDatabase()
    try:
        with filename.open('r', encoding='utf-8') as f: # Specify encoding
            db.parse(f)
    except FileNotFoundError:
        pytest.fail(f"Test data file not found: {filename}")
    except Exception as e:
        pytest.fail(f"Error parsing file {filename}: {e}")
    return db

def _push_file(filename: Path):
    """Helper to parse a gstat output file line by line using push."""
    db = StatDatabase()
    try:
        with filename.open('r', encoding='utf-8') as f: # Specify encoding
            for line in f:
                db.push(line)
        db.push(STOP) # Signal end of input
    except FileNotFoundError:
        pytest.fail(f"Test data file not found: {filename}")
    except Exception as e:
        pytest.fail(f"Error pushing file {filename}: {e}")
    return db

# --- Test Cases ---

def test_01_parse30_h(data_path):
    """Tests parsing header-only output (gstat -h) for FB 3.0."""
    filepath: Path = data_path / 'gstat30-h.out'
    db = _parse_file(filepath)

    expected_data = {'attributes': 1, 'backup_diff_file': None,
                     'backup_guid': '{F978F787-7023-4C4A-F79D-8D86645B0487}',
                     'completed': datetime.datetime(2018, 4, 4, 15, 41, 34),
                     'continuation_file': None, 'continuation_files': 0,
                     'creation_date': datetime.datetime(2015, 11, 27, 11, 19, 39),
                     'database_dialect': 3, 'encrypted_blob_pages': None,
                     'encrypted_data_pages': None, 'encrypted_index_pages': None,
                     'executed': datetime.datetime(2018, 4, 4, 15, 41, 34),
                     'filename': '/home/fdb/test/FBTEST30.FDB', 'flags': 0,
                     'generation': 2176, 'gstat_version': 3,
                     'implementation': 'HW=AMD/Intel/x64 little-endian OS=Linux CC=gcc',
                     'indices': 0, 'last_logical_page': None, 'next_attachment_id': 1199,
                     'next_header_page': 0,
                     'next_transaction': 2141, 'oat': 2140, 'ods_version': '12.0', 'oit': 179,
                     'ost': 2140, 'page_buffers': 0,
                     'page_size': 8192, 'replay_logging_file': None, 'root_filename': None,
                     'sequence_number': 0, 'shadow_count': 0,
                     'sweep_interval': None, 'system_change_number': 24, 'tables': 0}

    assert isinstance(db, StatDatabase)
    assert get_object_data(db) == expected_data # pytest provides good diffs

    assert not db.has_table_stats()
    assert not db.has_index_stats()
    assert not db.has_row_stats()
    assert not db.has_encryption_stats()
    assert not db.has_system()

def test_02_parse30_a(data_path):
    """Tests parsing full stats output (gstat -a) for FB 3.0."""
    filepath = data_path / 'gstat30-a.out'
    db = _parse_file(filepath)

    # Expected Database Header Data
    expected_db_data = {'attributes': 1, 'backup_diff_file': None, 'backup_guid': '{F978F787-7023-4C4A-F79D-8D86645B0487}',
                         'completed': datetime.datetime(2018, 4, 4, 15, 42),
                         'continuation_file': None, 'continuation_files': 0, 'creation_date': datetime.datetime(2015, 11, 27, 11, 19, 39),
                         'database_dialect': 3, 'encrypted_blob_pages': None, 'encrypted_data_pages': None, 'encrypted_index_pages': None,
                         'executed': datetime.datetime(2018, 4, 4, 15, 42), 'filename': '/home/fdb/test/FBTEST30.FDB', 'flags': 0,
                         'generation': 2176, 'gstat_version': 3, 'implementation': 'HW=AMD/Intel/x64 little-endian OS=Linux CC=gcc',
                         'indices': 39, 'last_logical_page': None, 'next_attachment_id': 1199, 'next_header_page': 0,
                         'next_transaction': 2141, 'oat': 2140, 'ods_version': '12.0', 'oit': 179, 'ost': 2140, 'page_buffers': 0,
                         'page_size': 8192, 'replay_logging_file': None, 'root_filename': None, 'sequence_number': 0, 'shadow_count': 0,
                         'sweep_interval': None, 'system_change_number': 24, 'tables': 16}
    assert get_object_data(db) == expected_db_data

    # Check flags
    assert db.has_table_stats()
    assert db.has_index_stats()
    assert not db.has_row_stats()
    assert not db.has_encryption_stats()
    assert not db.has_system()

    # Expected Table Data (verify first few tables for brevity)
    expected_tables_data = [
        {'avg_fill': 86, 'avg_fragment_length': None, 'avg_record_length': None, 'avg_unpacked_length': None,
         'avg_version_length': None, 'blob_pages': None, 'blobs': None, 'blobs_total_length': None, 'compression_ratio': None,
         'data_page_slots': 3, 'data_pages': 3, 'distribution': FillDistribution(d20=0, d40=0, d60=0, d80=1, d100=2),
         'empty_pages': 0, 'full_pages': 1, 'index_root_page': 299, 'indices': 0, 'level_0': None, 'level_1': None, 'level_2': None,
         'max_fragments': None, 'max_versions': None, 'name': 'AR', 'pointer_pages': 1, 'primary_pages': 1,
         'primary_pointer_page': 297, 'secondary_pages': 2, 'swept_pages': 0, 'table_id': 140, 'total_formats': None,
         'total_fragments': None, 'total_records': None, 'total_versions': None, 'used_formats': None},
        {'avg_fill': 8, 'avg_fragment_length': None, 'avg_record_length': None, 'avg_unpacked_length': None,
         'avg_version_length': None, 'blob_pages': None, 'blobs': None, 'blobs_total_length': None, 'compression_ratio': None,
         'data_page_slots': 1, 'data_pages': 1, 'distribution': FillDistribution(d20=1, d40=0, d60=0, d80=0, d100=0),
         'empty_pages': 0, 'full_pages': 0, 'index_root_page': 183, 'indices': 1, 'level_0': None, 'level_1': None, 'level_2': None,
         'max_fragments': None, 'max_versions': None, 'name': 'COUNTRY', 'pointer_pages': 1, 'primary_pages': 1,
         'primary_pointer_page': 182, 'secondary_pages': 0, 'swept_pages': 0, 'table_id': 128, 'total_formats': None,
         'total_fragments': None, 'total_records': None, 'total_versions': None, 'used_formats': None},
        # ... Add more table data checks if needed ...
    ]
    assert len(db.tables) == 16 # Check count first
    for i, expected_table in enumerate(expected_tables_data):
        assert get_object_data(db.tables[i]) == expected_table, f"Table data mismatch at index {i}"

    # Expected Index Data (verify first few indices)
    expected_indices_data = [
        {'avg_data_length': 6.44, 'avg_key_length': 8.63, 'avg_node_length': 10.44, 'avg_prefix_length': 0.44,
         'clustering_factor': 1.0, 'compression_ratio': 0.8, 'depth': 1,
         'distribution': FillDistribution(d20=1, d40=0, d60=0, d80=0, d100=0), 'index_id': 0, 'leaf_buckets': 1, 'max_dup': 0,
         'name': 'RDB$PRIMARY1', 'nodes': 16, 'ratio': 0.06, 'root_page': 186, 'total_dup': 0},
        {'avg_data_length': 15.87, 'avg_key_length': 18.27, 'avg_node_length': 19.87, 'avg_prefix_length': 0.6,
         'clustering_factor': 1.0, 'compression_ratio': 0.9, 'depth': 1,
         'distribution': FillDistribution(d20=1, d40=0, d60=0, d80=0, d100=0), 'index_id': 2, 'leaf_buckets': 1, 'max_dup': 0,
         'name': 'CUSTNAMEX', 'nodes': 15, 'ratio': 0.07, 'root_page': 276, 'total_dup': 0},
         # ... Add more index data checks if needed ...
    ]
    assert len(db.indices) == 39 # Check count first
    # Check association and data for the first few indices
    assert db.indices[0].table.name == 'COUNTRY'
    assert get_object_data(db.indices[0], skip=['table']) == expected_indices_data[0]
    assert db.indices[1].table.name == 'CUSTOMER'
    assert get_object_data(db.indices[1], skip=['table']) == expected_indices_data[1]
    # Add more specific index checks as required

def test_03_parse30_d(data_path):
    """Tests parsing data page stats (gstat -d) for FB 3.0."""
    filepath = data_path / 'gstat30-d.out'
    db = _parse_file(filepath)

    assert db.has_table_stats()
    assert not db.has_index_stats()
    assert not db.has_row_stats()
    assert not db.has_encryption_stats()
    assert not db.has_system()

    # Verify table count and maybe sample data for one table
    assert len(db.tables) == 16
    expected_ar_table = {
        'avg_fill': 86, 'avg_fragment_length': None, 'avg_record_length': None, 'avg_unpacked_length': None,
        'avg_version_length': None, 'blob_pages': None, 'blobs': None, 'blobs_total_length': None, 'compression_ratio': None,
        'data_page_slots': 3, 'data_pages': 3, 'distribution': FillDistribution(d20=0, d40=0, d60=0, d80=1, d100=2),
        'empty_pages': 0, 'full_pages': 1, 'index_root_page': 299, 'indices': 0, 'level_0': None, 'level_1': None, 'level_2': None,
        'max_fragments': None, 'max_versions': None, 'name': 'AR', 'pointer_pages': 1, 'primary_pages': 1,
        'primary_pointer_page': 297, 'secondary_pages': 2, 'swept_pages': 0, 'table_id': 140, 'total_formats': None,
        'total_fragments': None, 'total_records': None, 'total_versions': None, 'used_formats': None
    }
    assert get_object_data(db.tables[0]) == expected_ar_table # Assuming AR is the first table
    assert len(db.indices) == 0 # No index stats expected with -d

def test_04_parse30_e(data_path):
    """Tests parsing encryption stats (gstat -e) for FB 3.0."""
    filepath = data_path / 'gstat30-e.out'
    db = _parse_file(filepath)

    expected_data = {'attributes': 1, 'backup_diff_file': None, 'backup_guid': '{F978F787-7023-4C4A-F79D-8D86645B0487}',
                     'completed': datetime.datetime(2018, 4, 4, 15, 45, 6),
                     'continuation_file': None, 'continuation_files': 0, 'creation_date': datetime.datetime(2015, 11, 27, 11, 19, 39),
                     'database_dialect': 3,
                     # Compare Encryption objects directly or their attributes
                     'encrypted_blob_pages': Encryption(pages=11, encrypted=0, unencrypted=11),
                     'encrypted_data_pages': Encryption(pages=121, encrypted=0, unencrypted=121),
                     'encrypted_index_pages': Encryption(pages=96, encrypted=0, unencrypted=96),
                     'executed': datetime.datetime(2018, 4, 4, 15, 45, 6), 'filename': '/home/fdb/test/FBTEST30.FDB', 'flags': 0,
                     'generation': 2181, 'gstat_version': 3, 'implementation': 'HW=AMD/Intel/x64 little-endian OS=Linux CC=gcc',
                     'indices': 0, 'last_logical_page': None, 'next_attachment_id': 1214,
                     'next_header_page': 0, 'next_transaction': 2146, 'oat': 2146, 'ods_version': '12.0', 'oit': 179, 'ost': 2146,
                     'page_buffers': 0, 'page_size': 8192, 'replay_logging_file': None, 'root_filename': None, 'sequence_number': 0,
                     'shadow_count': 0, 'sweep_interval': None, 'system_change_number': 24, 'tables': 0}

    assert isinstance(db, StatDatabase)
    # Need custom comparison or extract data for Encryption objects if direct compare fails
    # For now, assume __eq__ is implemented or compare extracted data
    assert get_object_data(db) == expected_data

    assert not db.has_table_stats()
    assert not db.has_index_stats()
    assert not db.has_row_stats()
    assert db.has_encryption_stats()
    assert not db.has_system()
    # Explicit check of encryption values
    assert db.encrypted_blob_pages == Encryption(pages=11, encrypted=0, unencrypted=11)
    assert db.encrypted_data_pages == Encryption(pages=121, encrypted=0, unencrypted=121)
    assert db.encrypted_index_pages == Encryption(pages=96, encrypted=0, unencrypted=96)


def test_05_parse30_f(data_path):
    """Tests parsing full stats including system tables (gstat -f) for FB 3.0."""
    filepath = data_path / 'gstat30-f.out'
    db = _parse_file(filepath)

    assert db.has_table_stats()
    assert db.has_index_stats()
    assert db.has_row_stats()
    assert not db.has_encryption_stats()
    assert db.has_system() # System tables included

def test_06_parse30_i(data_path):
    """Tests parsing index stats (gstat -i) for FB 3.0."""
    filepath = data_path / 'gstat30-i.out'
    db = _parse_file(filepath)

    assert not db.has_table_stats() # Only index stats expected
    assert db.has_index_stats()
    assert not db.has_row_stats()
    assert not db.has_encryption_stats()
    assert not db.has_system() # -i doesn't imply -s

    # Verify counts and sample data
    assert len(db.tables) == 16 # Tables are listed but contain minimal info
    assert len(db.indices) == 39

    # Check a sample table structure from -i output
    expected_country_table = {
        'avg_fill': None, 'avg_fragment_length': None, 'avg_record_length': None, 'avg_unpacked_length': None,
        'avg_version_length': None, 'blob_pages': None, 'blobs': None, 'blobs_total_length': None, 'compression_ratio': None,
        'data_page_slots': None, 'data_pages': None, 'distribution': None, 'empty_pages': None, 'full_pages': None,
        'index_root_page': None, 'indices': 1, 'level_0': None, 'level_1': None, 'level_2': None, 'max_fragments': None,
        'max_versions': None, 'name': 'COUNTRY', 'pointer_pages': None, 'primary_pages': None, 'primary_pointer_page': None,
        'secondary_pages': None, 'swept_pages': None, 'table_id': 128, 'total_formats': None, 'total_fragments': None,
        'total_records': None, 'total_versions': None, 'used_formats': None
    }
    # Find the COUNTRY table (order might vary)
    country_table = next((t for t in db.tables if t.name == 'COUNTRY'), None)
    assert country_table is not None
    assert get_object_data(country_table) == expected_country_table

    # Check a sample index structure
    expected_primary1_index = {
        'avg_data_length': 6.44, 'avg_key_length': 8.63, 'avg_node_length': 10.44, 'avg_prefix_length': 0.44,
        'clustering_factor': 1.0, 'compression_ratio': 0.8, 'depth': 1,
        'distribution': FillDistribution(d20=1, d40=0, d60=0, d80=0, d100=0), 'index_id': 0, 'leaf_buckets': 1, 'max_dup': 0,
        'name': 'RDB$PRIMARY1', 'nodes': 16, 'ratio': 0.06, 'root_page': 186, 'total_dup': 0
    }
    # Find the RDB$PRIMARY1 index
    primary1_index = next((idx for idx in db.indices if idx.name == 'RDB$PRIMARY1'), None)
    assert primary1_index is not None
    assert primary1_index.table.name == 'COUNTRY' # Check association
    assert get_object_data(primary1_index, skip=['table']) == expected_primary1_index

def test_07_parse30_r(data_path):
    """Tests parsing record version stats (gstat -r) for FB 3.0."""
    filepath = data_path / 'gstat30-r.out'
    db = _parse_file(filepath)

    assert db.has_table_stats()
    assert db.has_index_stats() # -r includes index stats
    assert db.has_row_stats()   # -r specifically includes row stats
    assert not db.has_encryption_stats()
    assert not db.has_system()

    # Verify counts
    assert len(db.tables) == 16
    assert len(db.indices) == 39

    # Check sample table with row stats
    expected_ar_table = {
        'avg_fill': 86, 'avg_fragment_length': 0.0, 'avg_record_length': 2.79, 'avg_unpacked_length': 120.0,
        'avg_version_length': 16.61, 'blob_pages': 0, 'blobs': 125, 'blobs_total_length': 11237, 'compression_ratio': 42.99,
        'data_page_slots': 3, 'data_pages': 3, 'distribution': FillDistribution(d20=0, d40=0, d60=0, d80=1, d100=2),
        'empty_pages': 0, 'full_pages': 1, 'index_root_page': 299, 'indices': 0, 'level_0': 125, 'level_1': 0, 'level_2': 0,
        'max_fragments': 0, 'max_versions': 1, 'name': 'AR', 'pointer_pages': 1, 'primary_pages': 1, 'primary_pointer_page': 297,
        'secondary_pages': 2, 'swept_pages': 0, 'table_id': 140, 'total_formats': 1, 'total_fragments': 0, 'total_records': 120,
        'total_versions': 105, 'used_formats': 1
    }
    ar_table = next((t for t in db.tables if t.name == 'AR'), None)
    assert ar_table is not None
    assert get_object_data(ar_table) == expected_ar_table

def test_08_parse30_s(data_path):
    """Tests parsing system table stats (gstat -s) for FB 3.0."""
    filepath = data_path / 'gstat30-s.out'
    db = _parse_file(filepath)

    assert db.has_table_stats()
    assert db.has_index_stats()
    assert not db.has_row_stats()
    assert not db.has_encryption_stats()
    assert db.has_system() # System table stats are included

    # Check that some known system tables and indices are present
    system_tables_present = {t.name for t in db.tables if t.name.startswith('RDB$')}
    assert 'RDB$DATABASE' in system_tables_present
    assert 'RDB$RELATIONS' in system_tables_present
    assert 'RDB$INDICES' in system_tables_present

    system_indices_present = {i.name for i in db.indices if i.name.startswith('RDB$')}
    assert 'RDB$PRIMARY1' in system_indices_present # Index on RDB$CHARACTER_SETS
    assert 'RDB$INDEX_0' in system_indices_present # Index on RDB$PAGES
    assert 'RDB$INDEX_15' in system_indices_present # Index on RDB$RELATION_FIELDS

# --- Tests using push() method ---

def test_09_push30_h(data_path):
    """Tests parsing header-only output (gstat -h) via push() for FB 3.0."""
    filepath = data_path / 'gstat30-h.out'
    db = _push_file(filepath) # Use the push helper

    expected_data = {'attributes': 1, 'backup_diff_file': None,
                     'backup_guid': '{F978F787-7023-4C4A-F79D-8D86645B0487}',
                     'completed': datetime.datetime(2018, 4, 4, 15, 41, 34),
                     'continuation_file': None, 'continuation_files': 0,
                     'creation_date': datetime.datetime(2015, 11, 27, 11, 19, 39),
                     'database_dialect': 3, 'encrypted_blob_pages': None,
                     'encrypted_data_pages': None, 'encrypted_index_pages': None,
                     'executed': datetime.datetime(2018, 4, 4, 15, 41, 34),
                     'filename': '/home/fdb/test/FBTEST30.FDB', 'flags': 0,
                     'generation': 2176, 'gstat_version': 3,
                     'implementation': 'HW=AMD/Intel/x64 little-endian OS=Linux CC=gcc',
                     'indices': 0, 'last_logical_page': None, 'next_attachment_id': 1199,
                     'next_header_page': 0,
                     'next_transaction': 2141, 'oat': 2140, 'ods_version': '12.0', 'oit': 179,
                     'ost': 2140, 'page_buffers': 0,
                     'page_size': 8192, 'replay_logging_file': None, 'root_filename': None,
                     'sequence_number': 0, 'shadow_count': 0,
                     'sweep_interval': None, 'system_change_number': 24, 'tables': 0}

    assert isinstance(db, StatDatabase)
    assert get_object_data(db) == expected_data

    assert not db.has_table_stats()
    assert not db.has_index_stats()
    assert not db.has_row_stats()
    assert not db.has_encryption_stats()
    assert not db.has_system()

def test_10_push30_a(data_path):
    """Tests parsing full stats (gstat -a) via push() for FB 3.0."""
    filepath = data_path / 'gstat30-a.out'
    db = _push_file(filepath)

    # Reuse assertions from test_02_parse30_a as the result should be identical
    expected_db_data = {'attributes': 1, 'backup_diff_file': None, 'backup_guid': '{F978F787-7023-4C4A-F79D-8D86645B0487}',
                         'completed': datetime.datetime(2018, 4, 4, 15, 42),
                         'continuation_file': None, 'continuation_files': 0, 'creation_date': datetime.datetime(2015, 11, 27, 11, 19, 39),
                         'database_dialect': 3, 'encrypted_blob_pages': None, 'encrypted_data_pages': None, 'encrypted_index_pages': None,
                         'executed': datetime.datetime(2018, 4, 4, 15, 42), 'filename': '/home/fdb/test/FBTEST30.FDB', 'flags': 0,
                         'generation': 2176, 'gstat_version': 3, 'implementation': 'HW=AMD/Intel/x64 little-endian OS=Linux CC=gcc',
                         'indices': 39, 'last_logical_page': None, 'next_attachment_id': 1199, 'next_header_page': 0,
                         'next_transaction': 2141, 'oat': 2140, 'ods_version': '12.0', 'oit': 179, 'ost': 2140, 'page_buffers': 0,
                         'page_size': 8192, 'replay_logging_file': None, 'root_filename': None, 'sequence_number': 0, 'shadow_count': 0,
                         'sweep_interval': None, 'system_change_number': 24, 'tables': 16}
    assert get_object_data(db) == expected_db_data
    assert db.has_table_stats()
    assert db.has_index_stats()
    assert not db.has_row_stats()
    assert not db.has_encryption_stats()
    assert not db.has_system()
    assert len(db.tables) == 16
    assert len(db.indices) == 39

def test_11_push30_d(data_path):
    """Tests parsing data page stats (gstat -d) via push() for FB 3.0."""
    filepath = data_path / 'gstat30-d.out'
    db = _push_file(filepath)
    # Re-use assertions from test_03_parse30_d
    assert db.has_table_stats()
    assert not db.has_index_stats()
    assert not db.has_row_stats()
    assert not db.has_encryption_stats()
    assert not db.has_system()
    assert len(db.tables) == 16
    assert len(db.indices) == 0

def test_12_push30_e(data_path):
    """Tests parsing encryption stats (gstat -e) via push() for FB 3.0."""
    filepath = data_path / 'gstat30-e.out'
    db = _push_file(filepath)
    # Re-use assertions from test_04_parse30_e
    assert isinstance(db, StatDatabase)
    assert not db.has_table_stats()
    assert not db.has_index_stats()
    assert not db.has_row_stats()
    assert db.has_encryption_stats()
    assert not db.has_system()
    assert db.encrypted_blob_pages == Encryption(pages=11, encrypted=0, unencrypted=11)
    assert db.encrypted_data_pages == Encryption(pages=121, encrypted=0, unencrypted=121)
    assert db.encrypted_index_pages == Encryption(pages=96, encrypted=0, unencrypted=96)


def test_13_push30_f(data_path):
    """Tests parsing full stats including system tables (gstat -f) via push() for FB 3.0."""
    filepath = data_path / 'gstat30-f.out'
    db = _push_file(filepath)
    # Re-use assertions from test_05_parse30_f
    assert db.has_table_stats()
    assert db.has_index_stats()
    assert db.has_row_stats()
    assert not db.has_encryption_stats()
    assert db.has_system()

def test_14_push30_i(data_path):
    """Tests parsing index stats (gstat -i) via push() for FB 3.0."""
    filepath = data_path / 'gstat30-i.out'
    db = _push_file(filepath)
    # Re-use assertions from test_06_parse30_i
    assert not db.has_table_stats()
    assert db.has_index_stats()
    assert not db.has_row_stats()
    assert not db.has_encryption_stats()
    assert not db.has_system()
    assert len(db.tables) == 16
    assert len(db.indices) == 39

def test_15_push30_r(data_path):
    """Tests parsing record version stats (gstat -r) via push() for FB 3.0."""
    filepath = data_path / 'gstat30-r.out'
    db = _push_file(filepath)
    # Re-use assertions from test_07_parse30_r
    assert db.has_table_stats()
    assert db.has_index_stats()
    assert db.has_row_stats()
    assert not db.has_encryption_stats()
    assert not db.has_system()
    assert len(db.tables) == 16
    assert len(db.indices) == 39

def test_16_push30_s(data_path):
    """Tests parsing system table stats (gstat -s) via push() for FB 3.0."""
    filepath = data_path / 'gstat30-s.out'
    db = _push_file(filepath)
    # Re-use assertions from test_08_parse30_s
    assert db.has_table_stats()
    assert db.has_index_stats()
    assert not db.has_row_stats()
    assert not db.has_encryption_stats()
    assert db.has_system()
    system_tables_present = {t.name for t in db.tables if t.name.startswith('RDB$')}
    assert 'RDB$DATABASE' in system_tables_present
    system_indices_present = {i.name for i in db.indices if i.name.startswith('RDB$')}
    assert 'RDB$PRIMARY1' in system_indices_present

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
