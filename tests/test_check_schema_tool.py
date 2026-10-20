# SPDX-FileCopyrightText: 2026-present The Firebird Projects <www.firebirdsql.org>
#
# SPDX-License-Identifier: MIT
#
# PROGRAM/MODULE: firebird-lib
# FILE:           tests/test_check_schema_tool.py
# DESCRIPTION:    Regression checks for the schema coverage report

"""Small source and catalog cases for the standalone schema report."""

from pathlib import Path
from runpy import run_path
from types import SimpleNamespace

tool = run_path(str(Path(__file__).resolve().parents[1] / 'tools/check_schema.py'))
analyze_source = tool['analyze_source']
column_gaps = tool['column_gaps']
render_report = tool['render_report']
unmapped_type_rows = tool['unmapped_type_rows']


def test_column_gap_semantics():
    source = '''
class Table:
    """RDB$DOC_ONLY is mentioned here but not consumed."""
    def load(self):
        return self.schema._select("select * from RDB$RELATIONS where RDB$SYSTEM_FLAG = 0")
    def name(self):
        return self._attributes['RDB$RELATION_NAME']
'''
    catalog = {'RDB$RELATIONS': {'RDB$RELATION_NAME', 'RDB$SYSTEM_FLAG', 'RDB$DOC_ONLY'}}
    tables, unused, uncertain = column_gaps(catalog, analyze_source(source))
    assert tables == {'RDB$RELATIONS'}
    assert unused['RDB$RELATIONS'] == {'RDB$DOC_ONLY'}
    assert not uncertain['RDB$RELATIONS']


def test_shared_column_is_uncertain():
    source = """
class Constraint:
    def name(self):
        return self._attributes['RDB$CONSTRAINT_NAME']
    def load(self):
        return self._select("select * from RDB$RELATION_CONSTRAINTS c "
                            "cross join RDB$REF_CONSTRAINTS r")
"""
    column = 'RDB$CONSTRAINT_NAME'
    catalog = {'RDB$RELATION_CONSTRAINTS': {column}, 'RDB$REF_CONSTRAINTS': {column}}
    tables, unused, uncertain = column_gaps(catalog, analyze_source(source))
    assert tables == set(catalog)
    assert not unused['RDB$RELATION_CONSTRAINTS']
    assert not unused['RDB$REF_CONSTRAINTS']
    assert column in uncertain['RDB$REF_CONSTRAINTS']
    assert column in uncertain['RDB$RELATION_CONSTRAINTS']


def test_direct_cursor_projection_and_join_alias():
    source = """
class Schema:
    def dimensions(self):
        return self._ic.execute('select RDB$LOWER_BOUND, RDB$UPPER_BOUND '
                                'from RDB$FIELD_DIMENSIONS where RDB$FIELD_NAME = ?')
class ViewColumn:
    def base(self):
        return self._attributes['BASE_RELATION']
    def load(self):
        return self.schema._select('select v.RDB$RELATION_NAME as BASE_RELATION '
                                   'from RDB$VIEW_RELATIONS v')
"""
    catalog = {'RDB$FIELD_DIMENSIONS': {'RDB$LOWER_BOUND', 'RDB$UPPER_BOUND', 'RDB$FIELD_NAME'},
               'RDB$VIEW_RELATIONS': {'RDB$RELATION_NAME'}}
    tables, unused, uncertain = column_gaps(catalog, analyze_source(source))
    assert tables == set(catalog)
    assert all(not columns for columns in unused.values())
    assert all(not columns for columns in uncertain.values())


def test_type_rows_are_checked_individually():
    source = "self.identity_type = enum_dict('RDB$IDENTITY_TYPE')"
    inventory = analyze_source(source)
    schema = SimpleNamespace(identity_type={0: 'BY DEFAULT', 1: 'ALWAYS'})
    rows = [('RDB$IDENTITY_TYPE', 0, 'BY DEFAULT   '),
            ('RDB$IDENTITY_TYPE', 1, 'OTHER'),
            ('RDB$IDENTITY_TYPE', 2, 'NEW'),
            ('MON$STATE', 0, 'IDLE')]
    gaps = unmapped_type_rows(rows, inventory.type_maps, schema)
    assert gaps == [('RDB$IDENTITY_TYPE', 1, 'OTHER', "identity_type[1] = 'ALWAYS'"),
                    ('RDB$IDENTITY_TYPE', 2, 'NEW', 'identity_type[2] = None'),
                    ('MON$STATE', 0, 'IDLE', 'No Schema dictionary')]


def test_report_excludes_tables_and_columns():
    report = render_report('5.0', 'embedded',
                           catalog={'RDB$FORMATS': {'RDB$FORMAT'},
                                    'RDB$TRIGGERS': {'RDB$TRIGGER_BLR', 'RDB$DEBUG_INFO',
                                                     'RDB$TRIGGER_NAME', 'RDB$VALID_BLR'},
                                    'RDB$TYPES': {'RDB$DESCRIPTION'}},
                           tables={'RDB$FORMATS', 'RDB$TRIGGERS'},
                           unused={'RDB$FORMATS': {'RDB$FORMAT'},
                                   'RDB$TRIGGERS': {'RDB$TRIGGER_BLR', 'RDB$TRIGGER_NAME'}},
                           uncertain={'RDB$FORMATS': set(),
                                      'RDB$TRIGGERS': {'RDB$DEBUG_INFO', 'RDB$VALID_BLR'}},
                           type_maps={}, type_gaps=[])
    assert 'RDB$FORMATS' not in report
    assert 'RDB$TRIGGER_BLR' not in report
    assert 'RDB$DEBUG_INFO' not in report
    assert '| `RDB$TRIGGERS` | used | 1 | 1 |' in report
    assert '**Unused columns:** `RDB$TRIGGER_NAME`' in report
    assert '**Uncertain attribution:** `RDB$VALID_BLR`' in report
    assert '| `RDB$TYPES` | unused | — | — |' in report
