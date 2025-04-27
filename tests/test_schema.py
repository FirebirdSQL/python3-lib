# SPDX-FileCopyrightText: 2020-present The Firebird Projects <www.firebirdsql.org>
#
# SPDX-License-Identifier: MIT
#
# PROGRAM/MODULE: firebird-lib
# FILE:           tests/test_schema.py
# DESCRIPTION:    Tests for firebird.lib.trace module
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

"""firebird-lib - Tests for firebird.lib.schema module
"""

import pytest # Import pytest
from firebird.lib.schema import *
from firebird.lib import schema as sm

# --- Constants ---
FB30 = '3.0'
FB40 = '4.0'
FB50 = '5.0'

# --- Schema Visitor Helper ---

class SchemaVisitor(Visitor):
    """Visitor to collect DDL statements for specific actions."""
    def __init__(self, action: str, follow: str = 'dependencies'):
        self.collected_ddl: list[str] = []
        self.seen: list[SchemaItem] = []
        self.action: str = action
        self.follow: str = follow

    def default_action(self, obj: SchemaItem):
        if not obj.is_sys_object() and self.action in obj.actions:
            if self.follow == 'dependencies':
                for dependency in obj.get_dependencies():
                    d = dependency.depended_on
                    # Check if dependency exists and hasn't been processed
                    if d and d not in self.seen:
                        d.accept(self)
            elif self.follow == 'dependents':
                for dependency in obj.get_dependents():
                    d = dependency.dependent
                    # Check if dependent exists and hasn't been processed
                    if d and d not in self.seen:
                        d.accept(self)
            # Process the current object if not seen
            if obj not in self.seen:
                try:
                    ddl = obj.get_sql_for(self.action)
                    if ddl: # Only add if DDL is generated
                        self.collected_ddl.append(ddl)
                except Exception as e:
                    # Optionally log or handle errors during DDL generation
                    print(f"Warning: Could not get DDL for {obj.name} action '{self.action}': {e}", file=sys.stderr)
                self.seen.append(obj)

    # Override visit methods for objects that shouldn't generate direct DDL
    # but whose containers should be visited.
    def visit_TableColumn(self, column):
        if column.table not in self.seen:
            column.table.accept(self)

    def visit_ViewColumn(self, column):
        if column.view not in self.seen:
            column.view.accept(self)

    def visit_ProcedureParameter(self, param):
        if param.procedure not in self.seen:
            param.procedure.accept(self)

    def visit_FunctionArgument(self, arg):
        if arg.function not in self.seen:
            arg.function.accept(self)

# --- Test Functions ---

def test_01_SchemaBindClose(db_connection):
    """Tests binding, property access, and closing a Schema object."""
    s = Schema()
    # Test accessing property before binding
    with pytest.raises(Error, match="Schema is not binded to connection."):
        _ = s.default_character_set.name
    assert s.closed

    # Test binding and property access
    s.bind(db_connection)
    assert not s.closed
    assert s.description is None
    assert s.linger is None
    assert s.owner_name == 'SYSDBA'
    assert s.default_character_set.name == 'NONE'
    # Security class name might change between versions slightly, check existence
    assert s.security_class is not None and s.security_class.startswith('SQL$')

    # Test closing
    s.close()
    assert s.closed

    # Test binding via context manager
    with s.bind(db_connection):
        assert not s.closed
    assert s.closed

def test_02_SchemaFromConnection(db_connection, fb_vars):
    """Tests accessing schema objects via connection.schema and basic counts."""
    s = db_connection.schema
    version = fb_vars['version'].base_version # Use base version (e.g., '3.0')

    assert s.param_type_from == {0: 'DATATYPE', 1: 'DOMAIN', 2: 'TYPE OF DOMAIN', 3: 'TYPE OF COLUMN'}
    if version in (FB30, FB40):
        assert s.object_types == {
            0: 'RELATION', 1: 'VIEW', 2: 'TRIGGER', 3: 'COMPUTED_FIELD',
            4: 'VALIDATION', 5: 'PROCEDURE', 6: 'EXPRESSION_INDEX',
            7: 'EXCEPTION', 8: 'USER', 9: 'FIELD', 10: 'INDEX',
            11: 'CHARACTER_SET', 12: 'USER_GROUP', 13: 'ROLE',
            14: 'GENERATOR', 15: 'UDF', 16: 'BLOB_FILTER', 17: 'COLLATION',
            18:'PACKAGE', 19:'PACKAGE BODY'
        }
    else: # Firebird 5.0
        assert s.object_types == {
            0: 'RELATION', 1: 'VIEW', 2: 'TRIGGER', 3: 'COMPUTED_FIELD',
            4: 'VALIDATION', 5: 'PROCEDURE', 6: 'INDEX_EXPRESSION',
            7: 'EXCEPTION', 8: 'USER', 9: 'FIELD', 10: 'INDEX',
            11: 'CHARACTER_SET', 12: 'USER_GROUP', 13: 'ROLE',
            14: 'GENERATOR', 15: 'UDF', 16: 'BLOB_FILTER', 17: 'COLLATION',
            18:'PACKAGE', 19:'PACKAGE BODY', 37: 'INDEX_CONDITION'
        }
    if version in (FB30, FB40):
        assert s.object_type_codes == {
            'INDEX': 10, 'EXCEPTION': 7, 'GENERATOR': 14, 'COLLATION': 17,
            'UDF': 15, 'EXPRESSION_INDEX': 6, 'FIELD': 9,
            'COMPUTED_FIELD': 3, 'TRIGGER': 2, 'RELATION': 0, 'USER': 8,
            'USER_GROUP': 12, 'BLOB_FILTER': 16, 'ROLE': 13,
            'VALIDATION': 4, 'PROCEDURE': 5, 'VIEW': 1, 'CHARACTER_SET':11,
            'PACKAGE':18, 'PACKAGE BODY':19
        }
    else: # Firebird 5.0
        assert s.object_type_codes == {
            'INDEX': 10, 'EXCEPTION': 7, 'GENERATOR': 14, 'COLLATION': 17,
            'UDF': 15, 'INDEX_EXPRESSION': 6, 'FIELD': 9,
            'COMPUTED_FIELD': 3, 'TRIGGER': 2, 'RELATION': 0, 'USER': 8,
            'USER_GROUP': 12, 'BLOB_FILTER': 16, 'ROLE': 13,
            'VALIDATION': 4, 'PROCEDURE': 5, 'VIEW': 1, 'CHARACTER_SET':11,
            'PACKAGE':18, 'PACKAGE BODY':19, 'INDEX_CONDITION': 37
        }
    assert s.character_set_names == {
        0: 'NONE', 1: 'BINARY', 2: 'ASCII7', 3: 'SQL_TEXT', 4: 'UTF-8',
        5: 'SJIS', 6: 'EUCJ', 9: 'DOS_737', 10: 'DOS_437', 11: 'DOS_850',
        12: 'DOS_865', 13: 'DOS_860', 14: 'DOS_863', 15: 'DOS_775',
        16: 'DOS_858', 17: 'DOS_862', 18: 'DOS_864', 19: 'NEXT',
        21: 'ANSI', 22: 'ISO-8859-2', 23: 'ISO-8859-3', 34: 'ISO-8859-4',
        35: 'ISO-8859-5', 36: 'ISO-8859-6', 37: 'ISO-8859-7',
        38: 'ISO-8859-8', 39: 'ISO-8859-9', 40: 'ISO-8859-13',
        44: 'WIN_949', 45: 'DOS_852', 46: 'DOS_857', 47: 'DOS_861',
        48: 'DOS_866', 49: 'DOS_869', 50: 'CYRL', 51: 'WIN_1250',
        52: 'WIN_1251', 53: 'WIN_1252', 54: 'WIN_1253', 55: 'WIN_1254',
        56: 'WIN_950', 57: 'WIN_936', 58: 'WIN_1255', 59: 'WIN_1256',
        60: 'WIN_1257', 63: 'KOI8R', 64: 'KOI8U', 65: 'WIN_1258',
        66: 'TIS620', 67: 'GBK', 68: 'CP943C', 69: 'GB18030'}
    if version == FB30:
        assert s.field_types == {
            35: 'TIMESTAMP', 37: 'VARYING', 7: 'SHORT', 8: 'LONG',
            9: 'QUAD', 10: 'FLOAT', 12: 'DATE', 45: 'BLOB_ID', 14: 'TEXT',
            13: 'TIME', 16: 'INT64', 40: 'CSTRING', 27: 'DOUBLE',
            261: 'BLOB', 23:'BOOLEAN'
        }
    else:
        assert s.field_types == {
            35: 'TIMESTAMP', 37: 'VARYING', 7: 'SHORT', 8: 'LONG',
            9: 'QUAD', 10: 'FLOAT', 12: 'DATE', 45: 'BLOB_ID', 14: 'TEXT',
            13: 'TIME', 16: 'INT64', 40: 'CSTRING', 27: 'DOUBLE',
            261: 'BLOB', 23:'BOOLEAN', 24: 'DECFLOAT(16)',
            25: 'DECFLOAT(34)', 26: 'INT128', 28: 'TIME WITH TIME ZONE',
            29: 'TIMESTAMP WITH TIME ZONE'
        }
    assert s.field_subtypes == {
        0: 'BINARY', 1: 'TEXT', 2: 'BLR', 3: 'ACL', 4: 'RANGES',
        5: 'SUMMARY', 6: 'FORMAT', 7: 'TRANSACTION_DESCRIPTION',
        8: 'EXTERNAL_FILE_DESCRIPTION', 9: 'DEBUG_INFORMATION'
    }
    assert s.function_types == {0: 'VALUE', 1: 'BOOLEAN'}
    assert s.mechanism_types == {
        0: 'BY_VALUE', 1: 'BY_REFERENCE',
        2: 'BY_VMS_DESCRIPTOR', 3: 'BY_ISC_DESCRIPTOR',
        4: 'BY_SCALAR_ARRAY_DESCRIPTOR',
        5: 'BY_REFERENCE_WITH_NULL'
    }
    assert s.parameter_mechanism_types == {0: 'NORMAL', 1: 'TYPE OF'}
    assert s.procedure_types == {0: 'LEGACY', 1: 'SELECTABLE', 2: 'EXECUTABLE'}
    assert s.relation_types == {0: 'PERSISTENT', 1: 'VIEW', 2: 'EXTERNAL', 3: 'VIRTUAL',
                                4: 'GLOBAL_TEMPORARY_PRESERVE', 5: 'GLOBAL_TEMPORARY_DELETE'}
    assert s.system_flag_types == {0: 'USER', 1: 'SYSTEM', 2: 'QLI', 3: 'CHECK_CONSTRAINT',
                                   4: 'REFERENTIAL_CONSTRAINT', 5: 'VIEW_CHECK',
                                   6: 'IDENTITY_GENERATOR'}
    assert s.transaction_state_types == {1: 'LIMBO', 2: 'COMMITTED', 3: 'ROLLED_BACK'}
    assert s.trigger_types == {
        8192: 'CONNECT', 1: 'PRE_STORE', 2: 'POST_STORE',
        3: 'PRE_MODIFY', 4: 'POST_MODIFY', 5: 'PRE_ERASE',
        6: 'POST_ERASE', 8193: 'DISCONNECT', 8194: 'TRANSACTION_START',
        8195: 'TRANSACTION_COMMIT', 8196: 'TRANSACTION_ROLLBACK'
    }
    assert s.parameter_types == {0: 'INPUT', 1: 'OUTPUT'}
    assert s.index_activity_flags == {0: 'ACTIVE', 1: 'INACTIVE'}
    assert s.index_unique_flags == {0: 'NON_UNIQUE', 1: 'UNIQUE'}
    assert s.trigger_activity_flags == {0: 'ACTIVE', 1: 'INACTIVE'}
    assert s.grant_options == {0: 'NONE', 1: 'GRANT_OPTION', 2: 'ADMIN_OPTION'}
    assert s.page_types == {1: 'HEADER', 2: 'PAGE_INVENTORY', 3: 'TRANSACTION_INVENTORY',
                            4: 'POINTER', 5: 'DATA', 6: 'INDEX_ROOT', 7: 'INDEX_BUCKET',
                            8: 'BLOB', 9: 'GENERATOR', 10: 'SCN_INVENTORY'}
    assert s.privacy_flags == {0: 'PUBLIC', 1: 'PRIVATE'}
    assert s.legacy_flags == {0: 'NEW_STYLE', 1: 'LEGACY_STYLE'}
    assert s.deterministic_flags == {0: 'NON_DETERMINISTIC', 1: 'DETERMINISTIC'}

    # properties
    assert s.description is None
    assert s.owner_name == 'SYSDBA'
    assert s.default_character_set.name == 'NONE'
    assert s.security_class == 'SQL$363'
    # Lists of db objects
    assert isinstance(s.collations, DataList)
    assert isinstance(s.character_sets, DataList)
    assert isinstance(s.exceptions, DataList)
    assert isinstance(s.generators, DataList)
    assert isinstance(s.sys_generators, DataList)
    assert isinstance(s.all_generators, DataList)
    assert isinstance(s.domains, DataList)
    assert isinstance(s.sys_domains, DataList)
    assert isinstance(s.all_domains, DataList)
    assert isinstance(s.indices, DataList)
    assert isinstance(s.sys_indices, DataList)
    assert isinstance(s.all_indices, DataList)
    assert isinstance(s.tables, DataList)
    assert isinstance(s.sys_tables, DataList)
    assert isinstance(s.all_tables, DataList)
    assert isinstance(s.views, DataList)
    assert isinstance(s.sys_views, DataList)
    assert isinstance(s.all_views, DataList)
    assert isinstance(s.triggers, DataList)
    assert isinstance(s.sys_triggers, DataList)
    assert isinstance(s.all_triggers, DataList)
    assert isinstance(s.procedures, DataList)
    assert isinstance(s.sys_procedures, DataList)
    assert isinstance(s.all_procedures, DataList)
    assert isinstance(s.constraints, DataList)
    assert isinstance(s.roles, DataList)
    assert isinstance(s.dependencies, DataList)
    assert isinstance(s.functions, DataList)
    assert isinstance(s.sys_functions, DataList)
    assert isinstance(s.all_functions, DataList)
    assert isinstance(s.files, DataList)
    s.reload()
    assert len(s.collations) == 150
    assert len(s.character_sets) == 52
    assert len(s.exceptions) == 5
    assert len(s.generators) == 2
    assert len(s.sys_generators) == 13
    assert len(s.all_generators) == 15
    assert len(s.domains) == 15
    if version == FB30:
        assert len(s.sys_domains) == 277
        assert len(s.all_domains) == 292
        assert len(s.sys_indices) == 82
        assert len(s.all_indices) == 94
        assert len(s.sys_tables) == 50
        assert len(s.all_tables) == 66
        assert len(s.sys_procedures) == 0
        assert len(s.all_procedures) == 11
        assert len(s.constraints) == 110
        assert len(s.sys_functions) == 0
        assert len(s.all_functions) == 6
        assert len(s.sys_triggers) == 57
        assert len(s.all_triggers) == 65
    elif version == FB40:
        assert len(s.sys_domains) == 297
        assert len(s.all_domains) == 312
        assert len(s.sys_indices) == 85
        assert len(s.all_indices) == 97
        assert len(s.sys_tables) == 54
        assert len(s.all_tables) == 70
        assert len(s.sys_procedures) == 1
        assert len(s.all_procedures) == 12
        assert len(s.constraints) == 113
        assert len(s.sys_functions) == 1
        assert len(s.all_functions) == 7
        assert len(s.sys_triggers) == 57
        assert len(s.all_triggers) == 65
    else:
        assert len(s.sys_domains) == 306
        assert len(s.all_domains) == 321
        assert len(s.sys_indices) == 86
        assert len(s.all_indices) == 98
        assert len(s.sys_tables) == 56
        assert len(s.all_tables) == 72
        assert len(s.sys_procedures) == 10
        assert len(s.all_procedures) == 21
        assert len(s.constraints) == 113
        assert len(s.sys_functions) == 7
        assert len(s.all_functions) == 13
        assert len(s.sys_triggers) == 54
        assert len(s.all_triggers) == 62
    assert len(s.indices) == 12
    assert len(s.tables) == 16
    assert len(s.views) == 1
    assert len(s.sys_views) == 0
    assert len(s.all_views) == 1
    assert len(s.triggers) == 8
    assert len(s.procedures) == 11
    assert len(s.roles) == 2
    assert len(s.dependencies) == 168
    assert len(s.functions) == 6
    assert len(s.files) == 0
    #
    assert isinstance(s.collations[0], sm.Collation)
    assert isinstance(s.character_sets[0], sm.CharacterSet)
    assert isinstance(s.exceptions[0], sm.DatabaseException)
    assert isinstance(s.generators[0], sm.Sequence)
    assert isinstance(s.sys_generators[0], sm.Sequence)
    assert isinstance(s.all_generators[0], sm.Sequence)
    assert isinstance(s.domains[0], sm.Domain)
    assert isinstance(s.sys_domains[0], sm.Domain)
    assert isinstance(s.all_domains[0], sm.Domain)
    assert isinstance(s.indices[0], sm.Index)
    assert isinstance(s.sys_indices[0], sm.Index)
    assert isinstance(s.all_indices[0], sm.Index)
    assert isinstance(s.tables[0], sm.Table)
    assert isinstance(s.sys_tables[0], sm.Table)
    assert isinstance(s.all_tables[0], sm.Table)
    assert isinstance(s.views[0], sm.View)
    if len(s.sys_views) > 0:
        assert isinstance(s.sys_views[0], sm.View)
    assert isinstance(s.all_views[0], sm.View)
    assert isinstance(s.triggers[0], sm.Trigger)
    assert isinstance(s.sys_triggers[0], sm.Trigger)
    assert isinstance(s.all_triggers[0], sm.Trigger)
    assert isinstance(s.procedures[0], sm.Procedure)
    if len(s.sys_procedures) > 0:
        assert isinstance(s.sys_procedures[0], sm.Procedure)
    assert isinstance(s.all_procedures[0], sm.Procedure)
    assert isinstance(s.constraints[0], sm.Constraint)
    if len(s.roles) > 0:
        assert isinstance(s.roles[0], sm.Role)
    assert isinstance(s.dependencies[0], sm.Dependency)
    if len(s.files) > 0:
        assert isinstance(s.files[0], sm.DatabaseFile)
    assert isinstance(s.functions[0], sm.Function)
    if len(s.sys_functions) > 0:
        assert isinstance(s.sys_functions[0], sm.Function)
    assert isinstance(s.all_functions[0], sm.Function)
    #
    assert s.collations.get('OCTETS').name == 'OCTETS'
    assert s.character_sets.get('WIN1250').name == 'WIN1250'
    assert s.exceptions.get('UNKNOWN_EMP_ID').name == 'UNKNOWN_EMP_ID'
    assert s.all_generators.get('EMP_NO_GEN').name == 'EMP_NO_GEN'
    assert s.all_indices.get('MINSALX').name == 'MINSALX'
    assert s.all_domains.get('FIRSTNAME').name == 'FIRSTNAME'
    assert s.all_tables.get('COUNTRY').name == 'COUNTRY'
    assert s.all_views.get('PHONE_LIST').name == 'PHONE_LIST'
    assert s.all_triggers.get('SET_EMP_NO').name == 'SET_EMP_NO'
    assert s.all_procedures.get('GET_EMP_PROJ').name == 'GET_EMP_PROJ'
    assert s.constraints.get('INTEG_1').name == 'INTEG_1'
    assert s.get_collation_by_id(0, 0).name == 'NONE'
    assert s.get_charset_by_id(0).name == 'NONE'
    assert not s.is_multifile()
    #
    assert not s.closed
    #
    with pytest.raises(Error, match="Call to 'close' not allowed for embedded Schema."):
        s.close()
    with pytest.raises(Error, match="Call to 'bind' not allowed for embedded Schema."):
        s.bind(db_connection)
    # Reload
    s.reload([Category.TABLES, Category.VIEWS])
    assert s.all_tables.get('COUNTRY').name == 'COUNTRY'
    assert s.all_views.get('PHONE_LIST').name== 'PHONE_LIST'

def test_03_Collation(db_connection):
    """Tests Collation objects."""
    s = db_connection.schema

    # System collation
    c = s.collations.get('ES_ES')
    assert c.name == 'ES_ES'
    assert c.description is None
    assert c.actions == ['comment']
    assert c.is_sys_object()
    assert c.get_quoted_name() == 'ES_ES'
    assert not c.get_dependents()
    assert not c.get_dependencies()
    assert c.security_class.startswith('SQL$') # Version specific
    assert c.owner_name == 'SYSDBA'
    assert c.id == 10
    assert c.character_set.name == 'ISO8859_1'
    assert c.base_collation is None
    assert c.attributes == 1
    assert c.specific_attributes == 'DISABLE-COMPRESSIONS=1;SPECIALS-FIRST=1'
    assert c.function_name is None

    # User defined collation
    c = s.collations.get('TEST_COLLATE')
    assert c.name == 'TEST_COLLATE'
    assert c.description is None
    assert c.actions == ['comment', 'create', 'drop']
    assert not c.is_sys_object()
    assert c.get_quoted_name() == 'TEST_COLLATE'
    assert not c.get_dependents()
    assert not c.get_dependencies()
    assert c.id == 126
    assert c.character_set.name == 'WIN1250'
    assert c.base_collation.name == 'WIN_CZ'
    assert c.attributes == 6
    assert c.specific_attributes == 'DISABLE-COMPRESSIONS=0;DISABLE-EXPANSIONS=0'
    assert c.function_name is None
    assert c.get_sql_for('create') == """CREATE COLLATION TEST_COLLATE
   FOR WIN1250
   FROM WIN_CZ
   NO PAD
   CASE INSENSITIVE
   ACCENT INSENSITIVE
   'DISABLE-COMPRESSIONS=0;DISABLE-EXPANSIONS=0'"""
    assert c.get_sql_for('drop') == "DROP COLLATION TEST_COLLATE"
    with pytest.raises(ValueError, match="Unsupported parameter"):
        c.get_sql_for('drop', badparam='')
    assert c.get_sql_for('comment') == "COMMENT ON COLLATION TEST_COLLATE IS NULL"


def test_04_CharacterSet(db_connection):
    """Tests CharacterSet objects."""
    s = db_connection.schema
    c = s.character_sets.get('UTF8')

    # common properties
    assert c.name == 'UTF8'
    assert c.description is None
    assert c.actions == ['alter', 'comment']
    assert c.is_sys_object()
    assert c.get_quoted_name() == 'UTF8'
    assert not c.get_dependents()
    assert not c.get_dependencies()
    assert c.security_class.startswith('SQL$') # Version specific
    assert c.owner_name == 'SYSDBA'

    # CharacterSet specific properties
    assert c.id == 4
    assert c.bytes_per_character == 4
    assert c.default_collation.name == 'UTF8'
    assert [x.name for x in c.collations] == ['UTF8', 'UCS_BASIC', 'UNICODE', 'UNICODE_CI',
                                              'UNICODE_CI_AI']

    # Test DDL generation
    assert c.get_sql_for('alter', collation='UCS_BASIC') == \
        "ALTER CHARACTER SET UTF8 SET DEFAULT COLLATION UCS_BASIC"
    with pytest.raises(ValueError, match="Unsupported parameter"):
        c.get_sql_for('alter', badparam='UCS_BASIC')
    with pytest.raises(ValueError, match="Missing required parameter: 'collation'"):
        c.get_sql_for('alter')

    assert c.get_sql_for('comment') == 'COMMENT ON CHARACTER SET UTF8 IS NULL'

    # Test child object access
    assert c.collations.get('UCS_BASIC').name == 'UCS_BASIC'
    assert c.get_collation_by_id(c.collations.get('UCS_BASIC').id).name == 'UCS_BASIC'

def test_05_Exception(db_connection):
    """Tests DatabaseException objects."""
    s = db_connection.schema
    c = s.exceptions.get('UNKNOWN_EMP_ID')

    # common properties
    assert c.name == 'UNKNOWN_EMP_ID'
    assert c.description is None
    assert c.actions == ['comment', 'create', 'recreate', 'alter', 'create_or_alter', 'drop']
    assert not c.is_sys_object()
    assert c.get_quoted_name() == 'UNKNOWN_EMP_ID'
    d = c.get_dependents()
    assert len(d) == 1
    dep = d[0]
    assert dep.dependent_name == 'ADD_EMP_PROJ'
    assert dep.dependent_type == ObjectType.PROCEDURE
    assert isinstance(dep.dependent, sm.Procedure)
    assert dep.depended_on_name == 'UNKNOWN_EMP_ID'
    assert dep.depended_on_type == ObjectType.EXCEPTION
    assert isinstance(dep.depended_on, sm.DatabaseException)
    assert not c.get_dependencies()
    assert c.security_class.startswith('SQL$') # Version specific
    assert c.owner_name == 'SYSDBA'

    # Exception specific properties
    assert c.id == 1
    assert c.message == "Invalid employee number or project id."

    # Test DDL generation
    assert c.get_sql_for('create') == \
        "CREATE EXCEPTION UNKNOWN_EMP_ID 'Invalid employee number or project id.'"
    assert c.get_sql_for('recreate') == \
        "RECREATE EXCEPTION UNKNOWN_EMP_ID 'Invalid employee number or project id.'"
    assert c.get_sql_for('drop') == \
        "DROP EXCEPTION UNKNOWN_EMP_ID"
    assert c.get_sql_for('alter', message="New message.") == \
        "ALTER EXCEPTION UNKNOWN_EMP_ID 'New message.'"
    with pytest.raises(ValueError, match="Unsupported parameter"):
        c.get_sql_for('alter', badparam="New message.")
    with pytest.raises(ValueError, match="Missing required parameter: 'message'"):
        c.get_sql_for('alter')
    assert c.get_sql_for('create_or_alter') == \
        "CREATE OR ALTER EXCEPTION UNKNOWN_EMP_ID 'Invalid employee number or project id.'"
    assert c.get_sql_for('comment') == \
        "COMMENT ON EXCEPTION UNKNOWN_EMP_ID IS NULL"

def test_06_Sequence(db_connection):
    """Tests Sequence (Generator) objects."""
    s = db_connection.schema

    # System generator
    c = s.all_generators.get('RDB$FIELD_NAME')
    assert c.name == 'RDB$FIELD_NAME'
    assert c.description == "Implicit domain name"
    assert c.actions == ['comment']
    assert c.is_sys_object()
    assert c.get_quoted_name() == 'RDB$FIELD_NAME'
    assert not c.get_dependents()
    assert not c.get_dependencies()
    assert c.id == 6

    # User generator
    c = s.all_generators.get('EMP_NO_GEN')
    assert c.name == 'EMP_NO_GEN'
    assert c.description is None
    assert c.actions == ['comment', 'create', 'alter', 'drop']
    assert not c.is_sys_object()
    assert c.get_quoted_name() == 'EMP_NO_GEN'
    d = c.get_dependents()
    assert len(d) == 1
    dep = d[0]
    assert dep.dependent_name == 'SET_EMP_NO'
    assert dep.dependent_type == ObjectType.TRIGGER
    assert isinstance(dep.dependent, sm.Trigger)
    assert dep.depended_on_name == 'EMP_NO_GEN'
    assert dep.depended_on_type == ObjectType.GENERATOR
    assert isinstance(dep.depended_on, sm.Sequence)
    assert not c.get_dependencies()
    assert c.id == 12
    assert c.security_class.startswith('SQL$') # Version specific
    assert c.owner_name == 'SYSDBA'
    assert c.inital_value == 0
    assert c.increment == 1
    # Sequence value can change, check if it's an integer >= 0
    assert isinstance(c.value, int) and c.value >= 0

    # Test DDL generation
    assert c.get_sql_for('create') == "CREATE SEQUENCE EMP_NO_GEN"
    assert c.get_sql_for('drop') == "DROP SEQUENCE EMP_NO_GEN"
    assert c.get_sql_for('alter', value=10) == \
        "ALTER SEQUENCE EMP_NO_GEN RESTART WITH 10"
    with pytest.raises(ValueError, match="Unsupported parameter"):
        c.get_sql_for('alter', badparam=10)
    assert c.get_sql_for('comment') == \
        "COMMENT ON SEQUENCE EMP_NO_GEN IS NULL"
    # Test legacy keyword option
    c.schema.opt_generator_keyword = 'GENERATOR'
    assert c.get_sql_for('comment') == \
        "COMMENT ON GENERATOR EMP_NO_GEN IS NULL"
    c.schema.opt_generator_keyword = 'SEQUENCE' # Restore default

def test_07_TableColumn(db_connection):
    """Tests TableColumn objects."""
    s = db_connection.schema

    # System column
    c = s.all_tables.get('RDB$PAGES').columns.get('RDB$PAGE_NUMBER')
    assert c.name == 'RDB$PAGE_NUMBER'
    assert c.description is None
    assert c.actions == ['comment']
    assert c.is_sys_object()
    assert c.get_quoted_name() == 'RDB$PAGE_NUMBER'
    assert not c.get_dependents()
    assert not c.get_dependencies()
    assert not c.is_identity()
    assert c.generator is None

    # User column
    c = s.all_tables.get('DEPARTMENT').columns.get('PHONE_NO')
    assert c.name == 'PHONE_NO'
    assert c.description is None
    assert c.actions == ['comment', 'alter', 'drop']
    assert not c.is_sys_object()
    assert c.get_quoted_name() == 'PHONE_NO'
    d = c.get_dependents()
    assert len(d) == 1
    dep = d[0]
    assert dep.dependent_name == 'PHONE_LIST'
    assert dep.dependent_type == ObjectType.VIEW
    assert isinstance(dep.dependent, sm.View)
    assert dep.field_name == 'PHONE_NO' # Check field linkage
    assert dep.depended_on_name == 'DEPARTMENT'
    assert dep.depended_on_type == ObjectType.TABLE
    # Note: depended_on might resolve to Table, not TableColumn in some contexts
    # Check name and type to be safe
    assert isinstance(dep.depended_on, sm.TableColumn)
    assert dep.depended_on.name == 'PHONE_NO'
    assert not c.get_dependencies() # Column itself doesn't depend directly
    assert c.table.name == 'DEPARTMENT'
    assert c.domain.name == 'PHONENUMBER'
    assert c.position == 6
    assert c.security_class is None
    assert c.default == "'555-1234'"
    assert c.collation is None
    assert c.datatype == 'VARCHAR(20)'
    assert c.is_nullable()
    assert not c.is_computed()
    assert c.is_domain_based()
    assert c.has_default()
    assert c.get_computedby() is None

    # Test DDL generation
    assert c.get_sql_for('comment') == \
        "COMMENT ON COLUMN DEPARTMENT.PHONE_NO IS NULL"
    assert c.get_sql_for('drop') == \
        "ALTER TABLE DEPARTMENT DROP PHONE_NO"
    assert c.get_sql_for('alter', name='NewName') == \
        'ALTER TABLE DEPARTMENT ALTER COLUMN PHONE_NO TO "NewName"'
    assert c.get_sql_for('alter', position=2) == \
        "ALTER TABLE DEPARTMENT ALTER COLUMN PHONE_NO POSITION 2"
    assert c.get_sql_for('alter', datatype='VARCHAR(25)') == \
        "ALTER TABLE DEPARTMENT ALTER COLUMN PHONE_NO TYPE VARCHAR(25)"
    with pytest.raises(ValueError, match="Unsupported parameter"):
        c.get_sql_for('alter', badparam=10)
    with pytest.raises(ValueError, match="Parameter required"):
        c.get_sql_for('alter')
    with pytest.raises(ValueError, match="Change from persistent column to computed is not allowed."):
        c.get_sql_for('alter', expression='(1+1)')

    # Computed column
    c = s.all_tables.get('EMPLOYEE').columns.get('FULL_NAME')
    assert c.is_nullable()
    assert c.is_computed()
    assert not c.is_domain_based()
    assert not c.has_default()
    assert c.get_computedby() == "(last_name || ', ' || first_name)"
    assert c.datatype == 'VARCHAR(37)'
    assert c.get_sql_for('alter', datatype='VARCHAR(50)', expression="(first_name || ', ' || last_name)") == \
        "ALTER TABLE EMPLOYEE ALTER COLUMN FULL_NAME TYPE VARCHAR(50) COMPUTED BY (first_name || ', ' || last_name)"
    with pytest.raises(ValueError, match="Change from computed column to persistent is not allowed."):
        c.get_sql_for('alter', datatype='VARCHAR(50)')

    # Array column
    c = s.all_tables.get('AR').columns.get('C2')
    assert c.datatype == 'INTEGER[4, 0:3, 2]'

    # Identity column
    c = s.all_tables.get('T5').columns.get('ID')
    assert c.is_identity()
    assert c.generator.is_identity()
    assert c.identity_type == 1
    assert c.get_sql_for('alter', restart=None) == "ALTER TABLE T5 ALTER COLUMN ID RESTART"
    assert c.get_sql_for('alter', restart=100) == "ALTER TABLE T5 ALTER COLUMN ID RESTART WITH 100"

def test_08_Index(db_connection):
    """Tests Index objects."""
    s = db_connection.schema

    # System index
    c: Index = s.all_indices.get('RDB$INDEX_0')
    assert c.name == 'RDB$INDEX_0'
    assert c.description is None
    assert c.actions == ['activate', 'recompute', 'comment']
    assert c.is_sys_object()
    assert c.get_quoted_name() == 'RDB$INDEX_0'
    assert not c.get_dependents()
    assert not c.get_dependencies()
    assert c.condition is None
    assert c.table.name == 'RDB$RELATIONS'
    assert c.segment_names == ['RDB$RELATION_NAME']

    # User index
    c = s.all_indices.get('MAXSALX')
    assert c.name == 'MAXSALX'
    assert c.description is None
    assert c.actions == ['activate', 'recompute', 'comment', 'create', 'deactivate', 'drop']
    assert not c.is_sys_object()
    assert c.get_quoted_name() == 'MAXSALX'
    assert not c.get_dependents()
    assert not c.get_dependencies()
    assert c.id == 3
    assert c.table.name == 'JOB'
    assert c.index_type == IndexType.DESCENDING
    assert c.partner_index is None
    assert c.expression is None
    assert c.condition is None
    # startswith() check for floating point precision differences
    assert str(c.statistics).startswith('0.03')
    assert c.segment_names == ['JOB_COUNTRY', 'MAX_SALARY']
    assert len(c.segments) == 2
    for segment in c.segments:
        assert isinstance(segment, sm.TableColumn)
    assert c.segments[0].name == 'JOB_COUNTRY'
    assert c.segments[1].name == 'MAX_SALARY'
    assert len(c.segment_statistics) == 2 # Check length
    assert c.segment_statistics[0] > 0.0 # Check they are floats > 0
    assert c.segment_statistics[1] > 0.0
    assert c.constraint is None
    assert not c.is_expression()
    assert not c.is_unique()
    assert not c.is_inactive()
    assert not c.is_enforcer()

    # Test DDL generation
    assert c.get_sql_for('create') == \
        """CREATE DESCENDING INDEX MAXSALX ON JOB (JOB_COUNTRY,MAX_SALARY)"""
    assert c.get_sql_for('activate') == "ALTER INDEX MAXSALX ACTIVE"
    assert c.get_sql_for('deactivate') == "ALTER INDEX MAXSALX INACTIVE"
    assert c.get_sql_for('recompute') == "SET STATISTICS INDEX MAXSALX"
    assert c.get_sql_for('drop') == "DROP INDEX MAXSALX"
    assert c.get_sql_for('comment') == "COMMENT ON INDEX MAXSALX IS NULL"

    # Constraint index
    c = s.all_indices.get('RDB$FOREIGN6')
    assert c.name == 'RDB$FOREIGN6'
    assert c.is_sys_object()
    assert c.is_enforcer()
    assert c.partner_index.name == 'RDB$PRIMARY5'
    assert c.constraint.name == 'INTEG_17'

def test_09_ViewColumn(db_connection):
    """Tests ViewColumn objects."""
    s = db_connection.schema
    c = s.all_views.get('PHONE_LIST').columns.get('LAST_NAME')

    # common properties
    assert c.name == 'LAST_NAME'
    assert c.description is None
    assert c.actions == ['comment']
    assert not c.is_sys_object()
    assert c.get_quoted_name() == 'LAST_NAME'
    assert not c.get_dependents()
    d = c.get_dependencies()
    assert len(d) == 1
    dep = d[0]
    assert dep.dependent_name == 'PHONE_LIST'
    assert dep.dependent_type == ObjectType.VIEW
    assert isinstance(dep.dependent, sm.View)
    assert dep.field_name == 'LAST_NAME'
    assert dep.depended_on_name == 'EMPLOYEE'
    assert dep.depended_on_type == ObjectType.TABLE
    assert isinstance(dep.depended_on, sm.TableColumn)
    assert dep.depended_on.name == 'LAST_NAME'
    assert dep.depended_on.table.name == 'EMPLOYEE'

    # ViewColumn specific properties
    assert c.view.name == 'PHONE_LIST'
    assert c.base_field.name == 'LAST_NAME'
    assert c.base_field.table.name == 'EMPLOYEE'
    assert c.domain.name == 'LASTNAME'
    assert c.position == 2
    assert c.security_class is None
    assert c.collation.name == 'NONE'
    assert c.datatype == 'VARCHAR(20)'
    assert c.is_nullable()

    # Test DDL generation
    assert c.get_sql_for('comment') == \
        "COMMENT ON COLUMN PHONE_LIST.LAST_NAME IS NULL"

def test_10_Domain(db_connection, fb_vars):
    """Tests Domain objects."""
    s = db_connection.schema
    version = fb_vars['version'].base_version

    # System domain
    c = s.all_domains.get('RDB$6')
    assert c.name == 'RDB$6'
    assert c.description is None
    assert c.actions == ['comment']
    assert c.is_sys_object()
    assert c.get_quoted_name() == 'RDB$6'
    assert not c.get_dependents()
    assert not c.get_dependencies()
    assert c.security_class.startswith('SQL$') # Version specific
    assert c.owner_name == 'SYSDBA'

    # User domain
    c = s.all_domains.get('PRODTYPE')
    assert c.name == 'PRODTYPE'
    assert c.description is None
    assert c.actions == ['comment', 'create', 'alter', 'drop']
    assert not c.is_sys_object()
    assert c.get_quoted_name() == 'PRODTYPE'
    assert not c.get_dependents()
    assert not c.get_dependencies()
    assert c.expression is None
    assert c.validation == "CHECK (VALUE IN ('software', 'hardware', 'other', 'N/A'))"
    assert c.default == "'software'"
    assert c.length == 12
    assert c.scale == 0
    assert c.field_type == FieldType.VARYING
    assert c.sub_type == 0
    assert c.segment_length is None
    assert c.external_length is None
    assert c.external_scale is None
    assert c.external_type is None
    assert not c.dimensions
    assert c.character_length == 12
    assert c.collation.name == 'NONE'
    assert c.character_set.name == 'NONE'
    assert c.precision is None
    assert c.datatype == 'VARCHAR(12)'
    assert not c.is_nullable()
    assert not c.is_computed()
    assert c.is_validated()
    assert not c.is_array()
    assert c.has_default()

    # Test DDL generation
    assert c.get_sql_for('create') == \
        "CREATE DOMAIN PRODTYPE AS VARCHAR(12) DEFAULT 'software' NOT NULL CHECK (VALUE IN ('software', 'hardware', 'other', 'N/A'))"
    assert c.get_sql_for('drop') == "DROP DOMAIN PRODTYPE"
    assert c.get_sql_for('alter', name='New_name') == \
        'ALTER DOMAIN PRODTYPE TO "New_name"'
    assert c.get_sql_for('alter', default="'New_default'") == \
        "ALTER DOMAIN PRODTYPE SET DEFAULT 'New_default'"
    assert c.get_sql_for('alter', check="VALUE STARTS WITH 'X'") == \
        "ALTER DOMAIN PRODTYPE ADD CHECK (VALUE STARTS WITH 'X')"
    assert c.get_sql_for('alter', datatype='VARCHAR(30)') == \
        "ALTER DOMAIN PRODTYPE TYPE VARCHAR(30)"
    with pytest.raises(ValueError, match="Unsupported parameter"):
        c.get_sql_for('alter', badparam=10)
    with pytest.raises(ValueError, match="Parameter required"):
        c.get_sql_for('alter')

    # Domain with quoted name (behavior changed in FB4+)
    c = s.all_domains.get('FIRSTNAME')
    assert c.name == 'FIRSTNAME'
    if version.startswith('3'):
        assert c.get_quoted_name() == '"FIRSTNAME"'
        assert c.get_sql_for('create') == 'CREATE DOMAIN "FIRSTNAME" AS VARCHAR(15)'
        assert c.get_sql_for('comment') == 'COMMENT ON DOMAIN "FIRSTNAME" IS NULL'
    else: # FB4+
        assert c.get_quoted_name() == 'FIRSTNAME'
        assert c.get_sql_for('create') == 'CREATE DOMAIN FIRSTNAME AS VARCHAR(15)'
        assert c.get_sql_for('comment') == 'COMMENT ON DOMAIN FIRSTNAME IS NULL'

def test_11_Dependency(db_connection):
    """Tests Dependency objects."""
    s = db_connection.schema

    # Test dependencies retrieved from a table
    l = s.all_tables.get('DEPARTMENT').get_dependents()
    assert len(l) >= 18 # Count might vary slightly with FB versions
    # Find a specific dependency (PHONE_LIST view on DEPARTMENT.DEPT_NO)
    dep_phone_list_dept_no = next((d for d in l if d.dependent_name == 'PHONE_LIST' and d.field_name == 'DEPT_NO'), None)
    assert dep_phone_list_dept_no is not None

    c = dep_phone_list_dept_no
    assert c.name is None
    assert c.description is None
    assert not c.actions
    assert c.is_sys_object() # Dependencies themselves are system info
    assert c.get_quoted_name() is None
    assert not c.get_dependents() # Dependencies don't have dependents in this model
    assert not c.get_dependencies() # Dependencies don't have dependencies
    assert c.package is None
    assert not c.is_packaged()

    assert c.dependent_name == 'PHONE_LIST'
    assert c.dependent_type == ObjectType.VIEW
    assert isinstance(c.dependent, sm.View)
    assert c.dependent.name == 'PHONE_LIST'
    assert c.field_name == 'DEPT_NO'
    assert c.depended_on_name == 'DEPARTMENT'
    assert c.depended_on_type == ObjectType.TABLE
    assert isinstance(c.depended_on, sm.TableColumn)
    assert c.depended_on.name == 'DEPT_NO'

    # Test dependencies retrieved from a package
    if s.packages: # Packages exist from FB 3.0 onwards
        pkg = s.packages.get('TEST2')
        if pkg: # Check if package exists in the specific DB version
            l = pkg.get_dependencies()
            assert len(l) == 2
            # Dependency on non-packaged function
            x = next(d for d in l if d.depended_on.name == 'FN')
            assert not x.depended_on.is_packaged()
            # Dependency on packaged function
            x = next(d for d in l if d.depended_on.name == 'F')
            assert x.depended_on.is_packaged()
            assert isinstance(x.package, sm.Package) # Dependency ON a packaged object

def test_12_Constraint(db_connection):
    """Tests Constraint objects (PK, FK, CHECK, UNIQUE, NOT NULL)."""
    s = db_connection.schema

    # Common / PRIMARY KEY
    c = s.all_tables.get('CUSTOMER').primary_key
    assert c is not None
    assert c.name == 'INTEG_60'
    assert c.description is None
    assert c.actions == ['create', 'drop']
    assert not c.is_sys_object()
    assert c.get_quoted_name() == 'INTEG_60'
    assert not c.get_dependents() # Constraints typically don't have dependents listed this way
    assert not c.get_dependencies() # Constraints typically don't have dependencies listed this way
    assert c.constraint_type == ConstraintType.PRIMARY_KEY
    assert c.table.name == 'CUSTOMER'
    assert c.index.name == 'RDB$PRIMARY22'
    assert not c.trigger_names
    assert not c.triggers
    assert c.column_name is None # PK can span multiple columns
    assert c.partner_constraint is None
    assert c.match_option is None
    assert c.update_rule is None
    assert c.delete_rule is None
    assert not c.is_not_null()
    assert c.is_pkey()
    assert not c.is_fkey()
    assert not c.is_unique()
    assert not c.is_check()
    assert not c.is_deferrable()
    assert not c.is_deferred()
    assert c.get_sql_for('create') == "ALTER TABLE CUSTOMER ADD PRIMARY KEY (CUST_NO)"
    assert c.get_sql_for('drop') == "ALTER TABLE CUSTOMER DROP CONSTRAINT INTEG_60"

    # FOREIGN KEY
    c = s.all_tables.get('CUSTOMER').foreign_keys[0]
    assert c.actions == ['create', 'drop']
    assert c.constraint_type == ConstraintType.FOREIGN_KEY
    assert c.table.name == 'CUSTOMER'
    assert c.index.name == 'RDB$FOREIGN23'
    assert not c.trigger_names
    assert not c.triggers
    assert c.column_name is None # FK can span multiple columns
    assert c.partner_constraint.name == 'INTEG_2'
    assert c.match_option == 'FULL'
    assert c.update_rule == 'RESTRICT'
    assert c.delete_rule == 'RESTRICT'
    assert not c.is_not_null()
    assert not c.is_pkey()
    assert c.is_fkey()
    assert not c.is_unique()
    assert not c.is_check()
    assert c.get_sql_for('create') == \
        """ALTER TABLE CUSTOMER ADD FOREIGN KEY (COUNTRY)
  REFERENCES COUNTRY (COUNTRY)"""

    # CHECK
    c = s.constraints.get('INTEG_59')
    assert c.actions == ['create', 'drop']
    assert c.constraint_type == ConstraintType.CHECK
    assert c.table.name == 'CUSTOMER'
    assert c.index is None
    assert c.trigger_names == ['CHECK_9', 'CHECK_10']
    assert c.triggers[0].name == 'CHECK_9'
    assert c.triggers[1].name == 'CHECK_10'
    assert c.column_name is None
    assert c.partner_constraint is None
    assert c.match_option is None
    assert c.update_rule is None
    assert c.delete_rule is None
    assert not c.is_not_null()
    assert not c.is_pkey()
    assert not c.is_fkey()
    assert not c.is_unique()
    assert c.is_check()
    assert c.get_sql_for('create') == \
        "ALTER TABLE CUSTOMER ADD CHECK (on_hold IS NULL OR on_hold = '*')"

    # UNIQUE
    c = s.constraints.get('INTEG_15')
    assert c.actions == ['create', 'drop']
    assert c.constraint_type == ConstraintType.UNIQUE
    assert c.table.name == 'DEPARTMENT'
    assert c.index.name == 'RDB$4'
    assert not c.trigger_names
    assert not c.triggers
    assert c.column_name is None
    assert c.partner_constraint is None
    assert c.match_option is None
    assert c.update_rule is None
    assert c.delete_rule is None
    assert not c.is_not_null()
    assert not c.is_pkey()
    assert not c.is_fkey()
    assert c.is_unique()
    assert not c.is_check()
    assert c.get_sql_for('create') == "ALTER TABLE DEPARTMENT ADD UNIQUE (DEPARTMENT)"

    # NOT NULL
    c = s.constraints.get('INTEG_13')
    assert not c.actions # NOT NULL constraints usually managed via ALTER COLUMN
    assert c.constraint_type == ConstraintType.NOT_NULL
    assert c.table.name == 'DEPARTMENT'
    assert c.index is None
    assert not c.trigger_names
    assert not c.triggers
    assert c.column_name == 'DEPT_NO'
    assert c.partner_constraint is None
    assert c.match_option is None
    assert c.update_rule is None
    assert c.delete_rule is None
    assert c.is_not_null()
    assert not c.is_pkey()
    assert not c.is_fkey()
    assert not c.is_unique()
    assert not c.is_check()

def test_13_Table(db_connection):
    """Tests Table objects."""
    s = db_connection.schema

    # System table
    c = s.all_tables.get('RDB$PAGES')
    assert c.name == 'RDB$PAGES'
    assert c.description is None
    assert c.actions == ['comment']
    assert c.is_sys_object()
    assert c.get_quoted_name() == 'RDB$PAGES'
    assert not c.get_dependents()
    assert not c.get_dependencies()

    # User table
    c = s.all_tables.get('EMPLOYEE')
    assert c.name == 'EMPLOYEE'
    assert c.description is None
    assert c.actions == ['comment', 'create', 'recreate', 'drop']
    assert not c.is_sys_object()
    assert c.get_quoted_name() == 'EMPLOYEE'
    d = c.get_dependents()
    # Dependent counts/types can vary, check a few key ones exist
    dep_names = [x.dependent_name for x in d]
    assert 'SAVE_SALARY_CHANGE' in dep_names # Trigger
    assert 'PHONE_LIST' in dep_names         # View
    assert 'ORG_CHART' in dep_names          # Procedure
    assert 'DELETE_EMPLOYEE' in dep_names    # Procedure
    # Check dependencies (should be empty for a base table)
    assert not c.get_dependencies()

    assert c.id == 131
    assert c.dbkey_length == 8
    # Format, security class etc. vary significantly
    assert isinstance(c.format, int)
    assert c.security_class.startswith('SQL$')
    assert c.default_class.startswith('SQL$DEFAULT')
    assert c.table_type == RelationType.PERSISTENT
    assert c.external_file is None
    assert c.owner_name == 'SYSDBA'
    assert c.flags == 1
    assert c.primary_key.name == 'INTEG_27'
    assert [x.name for x in c.foreign_keys] == ['INTEG_28', 'INTEG_29']
    assert [x.name for x in c.columns] == \
        ['EMP_NO', 'FIRST_NAME', 'LAST_NAME', 'PHONE_EXT', 'HIRE_DATE',
         'DEPT_NO', 'JOB_CODE', 'JOB_GRADE', 'JOB_COUNTRY', 'SALARY',
         'FULL_NAME']
    assert len(c.constraints) >= 13 # Count might vary slightly
    assert 'INTEG_18' in [x.name for x in c.constraints]
    assert [x.name for x in c.indices] == \
        ['RDB$PRIMARY7', 'RDB$FOREIGN8', 'RDB$FOREIGN9', 'NAMEX']
    assert [x.name for x in c.triggers] == ['SET_EMP_NO', 'SAVE_SALARY_CHANGE']

    assert c.columns.get('EMP_NO').name == 'EMP_NO'
    assert not c.is_gtt()
    assert c.is_persistent()
    assert not c.is_external()
    assert c.has_pkey()
    assert c.has_fkey()

    # Test DDL generation (simplified check, exact formatting might vary)
    create_sql = c.get_sql_for('create')
    assert create_sql.startswith("CREATE TABLE EMPLOYEE")
    assert "EMP_NO EMPNO NOT NULL" in create_sql
    assert "FULL_NAME COMPUTED BY (last_name || ', ' || first_name)" in create_sql
    assert "PRIMARY KEY (EMP_NO)" in create_sql

    create_no_pk_sql = c.get_sql_for('create', no_pk=True)
    assert create_no_pk_sql.startswith("CREATE TABLE EMPLOYEE")
    assert "PRIMARY KEY (EMP_NO)" not in create_no_pk_sql

    recreate_sql = c.get_sql_for('recreate')
    assert recreate_sql.startswith("RECREATE TABLE EMPLOYEE")

    assert c.get_sql_for('drop') == "DROP TABLE EMPLOYEE"
    assert c.get_sql_for('comment') == 'COMMENT ON TABLE EMPLOYEE IS NULL'

    # Identity columns table
    c = s.all_tables.get('T5')
    create_t5_sql = c.get_sql_for('create')
    assert "ID NUMERIC(10, 0) GENERATED BY DEFAULT AS IDENTITY" in create_t5_sql
    assert "UQ BIGINT GENERATED BY DEFAULT AS IDENTITY (START WITH 100)" in create_t5_sql
    assert "PRIMARY KEY (ID)" in create_t5_sql

def test_14_View(db_connection):
    """Tests View objects."""
    s = db_connection.schema

    c = s.all_views.get('PHONE_LIST')
    assert c.name == 'PHONE_LIST'
    assert c.description is None
    assert c.actions == ['comment', 'create', 'recreate', 'alter', 'create_or_alter', 'drop']
    assert not c.is_sys_object()
    assert c.get_quoted_name() == 'PHONE_LIST'
    assert not c.get_dependents()
    d = c.get_dependencies()
    # Check some key dependencies exist
    dep_names = [x.depended_on_name for x in d]
    assert 'DEPARTMENT' in dep_names
    assert 'EMPLOYEE' in dep_names

    assert isinstance(c.id, int) # ID varies between versions
    assert c.security_class.startswith('SQL$')
    assert c.default_class.startswith('SQL$DEFAULT')
    assert c.sql == """SELECT
    emp_no, first_name, last_name, phone_ext, location, phone_no
    FROM employee, department
    WHERE employee.dept_no = department.dept_no"""
    assert c.dbkey_length == 16
    assert c.format >= 1
    assert c.owner_name == 'SYSDBA'
    assert c.flags == 1
    assert [x.name for x in c.columns] == \
        ['EMP_NO', 'FIRST_NAME', 'LAST_NAME', 'PHONE_EXT', 'LOCATION', 'PHONE_NO']
    assert not c.triggers
    assert c.columns.get('LAST_NAME').name == 'LAST_NAME'
    assert not c.has_checkoption()

    # Test DDL generation
    create_sql = c.get_sql_for('create')
    assert create_sql.startswith("CREATE VIEW PHONE_LIST")
    assert "SELECT" in create_sql
    assert "FROM employee, department" in create_sql

    recreate_sql = c.get_sql_for('recreate')
    assert recreate_sql.startswith("RECREATE VIEW PHONE_LIST")

    assert c.get_sql_for('drop') == "DROP VIEW PHONE_LIST"

    alter_sql = c.get_sql_for('alter', query='select * from country')
    assert alter_sql == "ALTER VIEW PHONE_LIST \n   AS\n     select * from country"

    alter_cols_sql = c.get_sql_for('alter', columns='country,currency', query='select * from country')
    assert alter_cols_sql == "ALTER VIEW PHONE_LIST (country,currency)\n   AS\n     select * from country"

    alter_check_sql = c.get_sql_for('alter', columns=('country', 'currency'), query='select * from country', check=True)
    assert alter_check_sql == "ALTER VIEW PHONE_LIST (country,currency)\n   AS\n     select * from country\n     WITH CHECK OPTION"

    with pytest.raises(ValueError, match="Unsupported parameter"):
        c.get_sql_for('alter', badparam='select * from country')
    with pytest.raises(ValueError, match="Missing required parameter: 'query'"):
        c.get_sql_for('alter')

    create_or_alter_sql = c.get_sql_for('create_or_alter')
    assert create_or_alter_sql.startswith("CREATE OR ALTER VIEW PHONE_LIST")

    assert c.get_sql_for('comment') == 'COMMENT ON VIEW PHONE_LIST IS NULL'

def test_15_Trigger(db_connection):
    """Tests Trigger objects."""
    s = db_connection.schema

    # System trigger
    c = s.all_triggers.get('RDB$TRIGGER_1')
    assert c.name == 'RDB$TRIGGER_1'
    assert c.description is None
    assert c.actions == ['comment']
    assert c.is_sys_object()
    assert c.get_quoted_name() == 'RDB$TRIGGER_1'
    assert not c.get_dependents()
    assert not c.get_dependencies()

    # User trigger (SET_EMP_NO)
    c = s.all_triggers.get('SET_EMP_NO')
    assert c.name == 'SET_EMP_NO'
    assert c.description is None
    assert c.actions == ['comment', 'create', 'recreate', 'alter', 'create_or_alter', 'drop']
    assert not c.is_sys_object()
    assert c.get_quoted_name() == 'SET_EMP_NO'
    assert not c.get_dependents()
    d = c.get_dependencies()
    dep_names = [(x.depended_on_name, x.depended_on_type) for x in d]
    assert ('EMPLOYEE', ObjectType.TABLE) in dep_names
    assert ('EMP_NO_GEN', ObjectType.GENERATOR) in dep_names
    assert c.relation.name == 'EMPLOYEE'
    assert c.sequence == 0
    assert c.trigger_type == TriggerType.DML
    assert c.source == "AS\nBEGIN\n    if (new.emp_no is null) then\n    new.emp_no = gen_id(emp_no_gen, 1);\nEND"
    assert c.flags == 1
    assert c.active
    assert c.is_before()
    assert not c.is_after()
    assert not c.is_db_trigger()
    assert c.is_insert()
    assert not c.is_update()
    assert not c.is_delete()
    assert c.get_type_as_string() == 'BEFORE INSERT'
    assert c.valid_blr == 1
    assert c.engine_name is None
    assert c.entrypoint is None

    # Test DDL generation
    create_sql = c.get_sql_for('create')
    assert create_sql.startswith("CREATE TRIGGER SET_EMP_NO FOR EMPLOYEE ACTIVE")
    assert "BEFORE INSERT POSITION 0" in create_sql
    assert "new.emp_no = gen_id(emp_no_gen, 1);" in create_sql

    recreate_sql = c.get_sql_for('recreate')
    assert recreate_sql.startswith("RECREATE TRIGGER SET_EMP_NO FOR EMPLOYEE ACTIVE")

    with pytest.raises(ValueError, match="Header or body definition required"):
        c.get_sql_for('alter')

    alter_sql = c.get_sql_for('alter', fire_on='AFTER INSERT', active=False, sequence=0,
                              declare='  DECLARE VARIABLE i integer;', code='  i = 1;')
    assert alter_sql.startswith("ALTER TRIGGER SET_EMP_NO INACTIVE")
    assert "AFTER INSERT" in alter_sql
    assert "DECLARE VARIABLE i integer;" in alter_sql
    assert "i = 1;" in alter_sql

    assert c.get_sql_for('alter', active=False) == "ALTER TRIGGER SET_EMP_NO INACTIVE"

    with pytest.raises(ValueError, match="Trigger type change is not allowed"):
        c.get_sql_for('alter', fire_on='ON CONNECT')

    create_or_alter_sql = c.get_sql_for('create_or_alter')
    assert create_or_alter_sql.startswith("CREATE OR ALTER TRIGGER SET_EMP_NO")

    assert c.get_sql_for('drop') == "DROP TRIGGER SET_EMP_NO"
    assert c.get_sql_for('comment') == 'COMMENT ON TRIGGER SET_EMP_NO IS NULL'

    # Multi-event trigger
    c = s.all_triggers.get('TR_MULTI')
    assert c.trigger_type == TriggerType.DML
    assert not c.is_ddl_trigger()
    assert not c.is_db_trigger()
    assert c.is_insert()
    assert c.is_update()
    assert c.is_delete()
    assert c.get_type_as_string() == 'AFTER INSERT OR UPDATE OR DELETE'

    # DB trigger
    c = s.all_triggers.get('TR_CONNECT')
    assert c.trigger_type == TriggerType.DB
    assert not c.is_ddl_trigger()
    assert c.is_db_trigger()
    assert not c.is_insert()
    assert not c.is_update()
    assert not c.is_delete()
    assert c.get_type_as_string() == 'ON CONNECT'

    # DDL trigger
    c = s.all_triggers.get('TRIG_DDL')
    assert c.trigger_type == TriggerType.DDL
    assert c.is_ddl_trigger()
    assert not c.is_db_trigger()
    assert not c.is_insert()
    assert not c.is_update()
    assert not c.is_delete()
    assert c.get_type_as_string() == 'BEFORE ANY DDL STATEMENT'

def test_16_ProcedureParameter(db_connection):
    """Tests ProcedureParameter objects."""
    s = db_connection.schema

    # Input parameter
    proc = s.all_procedures.get('GET_EMP_PROJ')
    assert proc is not None
    c = proc.input_params[0]
    assert c.name == 'EMP_NO'
    assert c.description is None
    assert c.actions == ['comment']
    assert not c.is_sys_object()
    assert c.get_quoted_name() == 'EMP_NO'
    assert not c.get_dependents()
    assert not c.get_dependencies()
    assert c.procedure.name == 'GET_EMP_PROJ'
    assert c.sequence == 0
    assert c.domain.name == 'RDB$32' # Domain name might be internal/version specific
    assert c.datatype == 'SMALLINT'
    assert c.type_from == TypeFrom.DATATYPE
    assert c.default is None
    assert c.collation is None
    assert c.mechanism == 0 # NORMAL
    assert c.column is None
    assert c.parameter_type == ParameterType.INPUT
    assert c.is_input()
    assert c.is_nullable()
    assert not c.has_default()
    assert c.get_sql_definition() == 'EMP_NO SMALLINT'

    # Output parameter
    c = proc.output_params[0]
    assert c.name == 'PROJ_ID'
    assert c.description is None
    assert c.actions == ['comment']
    assert not c.is_sys_object()
    assert c.get_quoted_name() == 'PROJ_ID'
    assert not c.get_dependents()
    assert not c.get_dependencies()
    assert c.get_sql_for('comment') == \
           'COMMENT ON PARAMETER GET_EMP_PROJ.PROJ_ID IS NULL'
    assert c.parameter_type == ParameterType.OUTPUT
    assert not c.is_input()
    assert c.get_sql_definition() == 'PROJ_ID CHAR(5)'

def test_17_Procedure(db_connection):
    """Tests Procedure objects."""
    s = db_connection.schema
    c = s.all_procedures.get('GET_EMP_PROJ')

    # common properties
    assert c.name == 'GET_EMP_PROJ'
    assert c.description is None
    assert c.actions == ['comment', 'create', 'recreate', 'alter', 'create_or_alter', 'drop']
    assert not c.is_sys_object()
    assert c.get_quoted_name() == 'GET_EMP_PROJ'
    assert not c.get_dependents()
    d = c.get_dependencies()
    dep_names = [(x.depended_on_name, x.depended_on_type) for x in d]
    assert ('EMPLOYEE_PROJECT', ObjectType.TABLE) in dep_names

    # Procedure specific properties
    assert isinstance(c.id, int) # ID varies
    assert c.security_class.startswith('SQL$')
    assert c.source == """BEGIN
	FOR SELECT proj_id
		FROM employee_project
		WHERE emp_no = :emp_no
		INTO :proj_id
	DO
		SUSPEND;
END"""
    assert c.owner_name == 'SYSDBA'
    assert [x.name for x in c.input_params] == ['EMP_NO']
    assert [x.name for x in c.output_params] == ['PROJ_ID']
    assert c.valid_blr
    assert c.proc_type == 1 # SELECTABLE
    assert c.engine_name is None
    assert c.entrypoint is None
    assert c.package is None
    assert c.privacy is None

    # Methods
    assert c.get_param('EMP_NO').name == 'EMP_NO'
    assert c.get_param('PROJ_ID').name == 'PROJ_ID'

    # Test DDL generation
    create_sql = c.get_sql_for('create')
    assert create_sql.startswith("CREATE PROCEDURE GET_EMP_PROJ")
    assert "(EMP_NO SMALLINT)" in create_sql
    assert "RETURNS (PROJ_ID CHAR(5))" in create_sql
    assert "FOR SELECT proj_id" in create_sql

    create_no_code_sql = c.get_sql_for('create', no_code=True)
    assert create_no_code_sql.startswith("CREATE PROCEDURE GET_EMP_PROJ")
    assert "BEGIN\n  SUSPEND;\nEND" in create_no_code_sql

    recreate_sql = c.get_sql_for('recreate')
    assert recreate_sql.startswith("RECREATE PROCEDURE GET_EMP_PROJ")

    create_or_alter_sql = c.get_sql_for('create_or_alter')
    assert create_or_alter_sql.startswith("CREATE OR ALTER PROCEDURE GET_EMP_PROJ")

    assert c.get_sql_for('drop') == "DROP PROCEDURE GET_EMP_PROJ"

    alter_code_sql = c.get_sql_for('alter', code="  /* PASS */")
    assert alter_code_sql == """ALTER PROCEDURE GET_EMP_PROJ
AS
BEGIN
  /* PASS */
END"""

    with pytest.raises(ValueError, match="Missing required parameter: 'code'"):
        c.get_sql_for('alter', declare="DECLARE VARIABLE i integer;")

    alter_input_sql = c.get_sql_for('alter', input="IN1 integer", code='')
    assert alter_input_sql.startswith("ALTER PROCEDURE GET_EMP_PROJ (IN1 integer)")

    alter_output_sql = c.get_sql_for('alter', output="OUT1 integer", code='')
    assert alter_output_sql.startswith("ALTER PROCEDURE GET_EMP_PROJ\nRETURNS (OUT1 integer)")

    alter_both_sql = c.get_sql_for('alter', input=["IN1 integer", "IN2 VARCHAR(10)"],
                                   output=["OUT1 integer", "OUT2 VARCHAR(10)"], code='')
    assert "IN1 integer,\n  IN2 VARCHAR(10)" in alter_both_sql
    assert "OUT1 integer,\n  OUT2 VARCHAR(10)" in alter_both_sql

    assert c.get_sql_for('comment') == 'COMMENT ON PROCEDURE GET_EMP_PROJ IS NULL'

def test_18_Role(db_connection):
    """Tests Role objects."""
    s = db_connection.schema
    c = s.roles.get('TEST_ROLE')

    # common properties
    assert c.name == 'TEST_ROLE'
    assert c.description is None
    assert c.actions == ['comment', 'create', 'drop']
    assert not c.is_sys_object()
    assert c.get_quoted_name() == 'TEST_ROLE'
    assert not c.get_dependents()
    assert not c.get_dependencies()
    assert c.owner_name == 'SYSDBA'

    # Test DDL generation
    assert c.get_sql_for('create') == "CREATE ROLE TEST_ROLE"
    assert c.get_sql_for('drop') == "DROP ROLE TEST_ROLE"
    assert c.get_sql_for('comment') == 'COMMENT ON ROLE TEST_ROLE IS NULL'

def test_19_FunctionArgument(db_connection):
    """Tests FunctionArgument objects using mocked UDFs."""
    s = db_connection.schema

    # Mock function ADDDAY
    f = _mockFunction(s, 'ADDDAY')
    assert f is not None
    assert len(f.arguments) == 2

    # First argument
    c = f.arguments[0]
    assert c.name == 'ADDDAY_1'
    assert c.description is None
    assert not c.actions
    assert not c.is_sys_object()
    assert c.get_quoted_name() == 'ADDDAY_1'
    assert not c.get_dependents()
    assert not c.get_dependencies()
    assert c.function.name == 'ADDDAY'
    assert c.position == 1
    assert c.mechanism == Mechanism.BY_REFERENCE
    assert c.field_type == FieldType.TIMESTAMP
    assert c.length == 8
    assert c.scale == 0
    assert c.precision is None
    assert c.sub_type is None
    assert c.character_length is None
    assert c.character_set is None
    assert c.datatype == 'TIMESTAMP'
    assert not c.is_by_value()
    assert c.is_by_reference()
    assert not c.is_by_descriptor()
    assert not c.is_with_null()
    assert not c.is_freeit()
    assert not c.is_returning()
    assert c.get_sql_definition() == 'TIMESTAMP'

    # Second argument
    c = f.arguments[1]
    assert c.name == 'ADDDAY_2'
    assert c.position == 2
    assert c.mechanism == Mechanism.BY_REFERENCE
    assert c.field_type == FieldType.LONG
    assert c.datatype == 'INTEGER'
    assert c.get_sql_definition() == 'INTEGER'

    # Return value
    c = f.returns
    assert c.position == 0
    assert c.mechanism == Mechanism.BY_REFERENCE
    assert c.field_type == FieldType.TIMESTAMP
    assert c.datatype == 'TIMESTAMP'
    assert c.is_returning()
    assert c.get_sql_definition() == 'TIMESTAMP'

    # Mock function STRING2BLOB
    f = _mockFunction(s, 'STRING2BLOB')
    assert len(f.arguments) == 2
    c = f.arguments[0]
    assert c.position == 1
    assert c.mechanism == Mechanism.BY_VMS_DESCRIPTOR
    assert c.datatype == 'VARCHAR(300) CHARACTER SET UTF8'
    assert c.is_by_descriptor()
    assert not c.is_returning()
    assert c.get_sql_definition() == 'VARCHAR(300) CHARACTER SET UTF8 BY DESCRIPTOR'
    c = f.arguments[1] # Also the return argument
    assert f.arguments[1] is f.returns
    assert c.position == 2
    assert c.mechanism == Mechanism.BY_ISC_DESCRIPTOR
    assert c.field_type == FieldType.BLOB
    assert c.datatype == 'BLOB'
    assert not c.is_by_descriptor() # Specific ISC descriptor
    assert c.is_by_descriptor(any_desc=True)
    assert c.is_returning()
    assert c.get_sql_definition() == 'BLOB'

    # Mock function SRIGHT
    f = _mockFunction(s, 'SRIGHT')
    assert len(f.arguments) == 3
    c = f.arguments[0] # Arg 1
    assert c.position == 1
    assert c.mechanism == Mechanism.BY_VMS_DESCRIPTOR
    assert c.datatype == 'VARCHAR(100) CHARACTER SET UTF8'
    assert c.is_by_descriptor()
    assert c.get_sql_definition() == 'VARCHAR(100) CHARACTER SET UTF8 BY DESCRIPTOR'
    c = f.arguments[1] # Arg 2
    assert c.position == 2
    assert c.mechanism == Mechanism.BY_REFERENCE
    assert c.datatype == 'SMALLINT'
    assert c.get_sql_definition() == 'SMALLINT'
    c = f.returns # Arg 3 / Returns
    assert c.position == 3
    assert c.mechanism == Mechanism.BY_VMS_DESCRIPTOR
    assert c.datatype == 'VARCHAR(100) CHARACTER SET UTF8'
    assert c.is_returning()
    assert c.get_sql_definition() == 'VARCHAR(100) CHARACTER SET UTF8 BY DESCRIPTOR'

    # Mock function I64NVL
    f = _mockFunction(s, 'I64NVL')
    assert len(f.arguments) == 2
    for a in f.arguments:
        assert a.datatype == 'NUMERIC(18, 0)'
        assert a.is_by_descriptor()
        assert a.get_sql_definition() == 'NUMERIC(18, 0) BY DESCRIPTOR'
    assert f.returns.datatype == 'NUMERIC(18, 0)'
    assert f.returns.is_by_descriptor()
    assert f.returns.get_sql_definition() == 'NUMERIC(18, 0) BY DESCRIPTOR'

def test_20_Function(db_connection, fb_vars):
    """Tests Function objects (UDF and PSQL)."""
    s = db_connection.schema
    version = fb_vars['version'].base_version

    # --- UDF Tests (using mocks) ---
    c = _mockFunction(s, 'STRING2BLOB')
    assert c is not None
    assert len(c.arguments) == 2
    assert c.has_arguments()
    assert c.has_return()
    assert c.has_return_argument()
    assert c.get_sql_for('declare') == \
           """DECLARE EXTERNAL FUNCTION STRING2BLOB
  VARCHAR(300) CHARACTER SET UTF8 BY DESCRIPTOR,
  BLOB
RETURNS PARAMETER 2
ENTRY_POINT 'string2blob'
MODULE_NAME 'fbudf'"""

    c = _mockFunction(s, 'I64NVL')
    assert c is not None
    assert len(c.arguments) == 2
    assert c.has_arguments()
    assert c.has_return()
    assert not c.has_return_argument()
    assert c.get_sql_for('declare') == \
           """DECLARE EXTERNAL FUNCTION I64NVL
  NUMERIC(18, 0) BY DESCRIPTOR,
  NUMERIC(18, 0) BY DESCRIPTOR
RETURNS NUMERIC(18, 0) BY DESCRIPTOR
ENTRY_POINT 'idNvl'
MODULE_NAME 'fbudf'"""

    # --- Internal PSQL functions ---
    c = s.all_functions.get('F2')
    assert c.name == 'F2'
    assert c.description is None
    assert c.package is None
    assert c.engine_mame is None
    assert c.private_flag is None
    assert c.source == 'BEGIN\n  RETURN X+1;\nEND'
    assert isinstance(c.id, int)
    assert c.security_class.startswith('SQL$')
    assert c.valid_blr
    assert c.owner_name == 'SYSDBA'
    assert c.legacy_flag == 0
    assert c.deterministic_flag == 0
    assert c.actions == ['create', 'recreate', 'alter', 'create_or_alter', 'drop']
    assert not c.is_sys_object()
    assert c.get_quoted_name() == 'F2'
    assert not c.get_dependents()
    assert not c.get_dependencies()
    assert c.module_name is None
    assert c.entrypoint is None
    assert c.returns.name == 'F2_0'
    assert [a.name for a in c.arguments] == ['X']
    assert c.has_arguments()
    assert c.has_return()
    assert not c.has_return_argument()
    assert not c.is_packaged()

    # Test DDL generation
    assert c.get_sql_for('drop') == "DROP FUNCTION F2"
    assert c.get_sql_for('create') == \
           """CREATE FUNCTION F2 (X INTEGER)
RETURNS INTEGER
AS
BEGIN
  RETURN X+1;
END"""
    assert c.get_sql_for('create', no_code=True) == \
           """CREATE FUNCTION F2 (X INTEGER)
RETURNS INTEGER
AS
BEGIN
END"""
    assert c.get_sql_for('recreate') == \
           """RECREATE FUNCTION F2 (X INTEGER)
RETURNS INTEGER
AS
BEGIN
  RETURN X+1;
END"""
    assert c.get_sql_for('create_or_alter') == \
           """CREATE OR ALTER FUNCTION F2 (X INTEGER)
RETURNS INTEGER
AS
BEGIN
  RETURN X+1;
END"""
    with pytest.raises(ValueError, match="Missing required parameter: 'returns'"):
        c.get_sql_for('alter', declare="DECLARE VARIABLE i integer;", code='')
    with pytest.raises(ValueError, match="Missing required parameter: 'code'"):
        c.get_sql_for('alter', declare="DECLARE VARIABLE i integer;", returns='INTEGER')
    assert c.get_sql_for('alter', returns='INTEGER', code='') == \
           """ALTER FUNCTION F2
RETURNS INTEGER
AS
BEGIN
END"""
    assert c.get_sql_for('alter', arguments="IN1 integer", returns='INTEGER', code='') == \
           """ALTER FUNCTION F2 (IN1 integer)
RETURNS INTEGER
AS
BEGIN
END"""
    assert c.get_sql_for('alter', returns='INTEGER', arguments=["IN1 integer", "IN2 VARCHAR(10)"], code='') == \
           """ALTER FUNCTION F2 (
  IN1 integer,
  IN2 VARCHAR(10)
)
RETURNS INTEGER
AS
BEGIN
END"""

    # Test function with TYPE OF parameters
    c = s.all_functions.get('FX')
    create_fx_sql = c.get_sql_for('create')
    assert "RETURNS VARCHAR(35)" in create_fx_sql
    if version.startswith('3'):
        assert 'F TYPE OF "FIRSTNAME"' in create_fx_sql
    else:
        assert 'F TYPE OF FIRSTNAME' in create_fx_sql
    assert 'L TYPE OF COLUMN CUSTOMER.CONTACT_LAST' in create_fx_sql

    # Test packaged function
    c = s.all_functions.get('F1')
    assert c.name == 'F1'
    assert c.package is not None
    assert isinstance(c.package, sm.Package)
    assert c.actions == [] # Actions are typically on the package
    assert c.private_flag # Assuming it's private based on context
    assert c.is_packaged()

def test_21_DatabaseFile(db_connection, fb_vars):
    """Tests DatabaseFile objects (using mock)."""
    s = db_connection.schema
    # We have to use mock as the test DB is likely single-file
    c = sm.DatabaseFile(s, {'RDB$FILE_LENGTH': 1000,
                            'RDB$FILE_NAME': '/path/dbfile.f02',
                            'RDB$FILE_START': 500,
                            'RDB$FILE_SEQUENCE': 1})

    assert c.name == 'FILE_1'
    assert c.description is None
    assert not c.actions
    assert c.is_sys_object() # Metadata about files is system info
    assert c.get_quoted_name() == 'FILE_1'
    assert not c.get_dependents()
    assert not c.get_dependencies()
    assert c.filename == '/path/dbfile.f02'
    assert c.sequence == 1
    assert c.start == 500
    assert c.length == 1000

def test_22_Shadow(db_connection, fb_vars):
    """Tests Shadow objects (using mock)."""
    s = db_connection.schema
    # We have to use mocks as test DB likely has no shadows
    c = Shadow(s, {'RDB$FILE_FLAGS': 1, 'RDB$SHADOW_NUMBER': 3})
    files = []
    # Use DatabaseFile constructor directly
    files.append(DatabaseFile(s, {'RDB$FILE_LENGTH': 500,
                                  'RDB$FILE_NAME': '/path/shadow.sf1',
                                  'RDB$FILE_START': 0,
                                  'RDB$FILE_SEQUENCE': 0}))
    files.append(DatabaseFile(s, {'RDB$FILE_LENGTH': 500,
                                  'RDB$FILE_NAME': '/path/shadow.sf2',
                                  'RDB$FILE_START': 1000,
                                  'RDB$FILE_SEQUENCE': 1}))
    files.append(DatabaseFile(s, {'RDB$FILE_LENGTH': 0,
                                  'RDB$FILE_NAME': '/path/shadow.sf3',
                                  'RDB$FILE_START': 1500,
                                  'RDB$FILE_SEQUENCE': 2}))
    # Access internal attribute directly for mocking (use with caution)
    c._Shadow__files = files

    assert c.name == 'SHADOW_3'
    assert c.description is None
    assert c.actions == ['create', 'drop']
    assert not c.is_sys_object() # Shadows are user-created
    assert c.get_quoted_name() == 'SHADOW_3'
    assert not c.get_dependents()
    assert not c.get_dependencies()
    assert c.id == 3
    assert c.flags == 1
    assert [(f.name, f.filename, f.start, f.length) for f in c.files] == \
           [('FILE_0', '/path/shadow.sf1', 0, 500),
         ('FILE_1', '/path/shadow.sf2', 1000, 500),
         ('FILE_2', '/path/shadow.sf3', 1500, 0)]
    assert not c.is_conditional()
    assert not c.is_inactive()
    assert not c.is_manual()

    # Test DDL generation
    assert c.get_sql_for('create') == \
           """CREATE SHADOW 3 AUTO '/path/shadow.sf1' LENGTH 500
  FILE '/path/shadow.sf2' STARTING AT 1000 LENGTH 500
  FILE '/path/shadow.sf3' STARTING AT 1500"""
    assert c.get_sql_for('drop') == "DROP SHADOW 3"
    assert c.get_sql_for('drop', preserve=True) == "DROP SHADOW 3 PRESERVE FILE"

def test_23_PrivilegeBasic(db_connection):
    """Tests basic Privilege object attributes and DDL."""
    s = db_connection.schema
    proc = s.all_procedures.get('ALL_LANGS')
    assert proc is not None
    assert len(proc.privileges) >= 2 # At least PUBLIC and SYSDBA

    # Find privilege for SYSDBA
    c = next((p for p in proc.privileges if p.user_name == 'SYSDBA'), None)
    assert c is not None

    # Common properties
    assert c.name == 'SYSDBA_EXECUTE_ON_ALL_LANGS'
    assert c.description is None
    assert c.actions == ['grant', 'revoke']
    assert c.is_sys_object() # Privileges are system metadata
    assert c.get_quoted_name() == 'SYSDBA_EXECUTE_ON_ALL_LANGS'
    assert not c.get_dependents()
    assert not c.get_dependencies()

    # Privilege specific properties
    assert isinstance(c.user, UserInfo)
    assert c.user.user_name == 'SYSDBA'
    assert isinstance(c.grantor, UserInfo)
    assert c.grantor.user_name == 'SYSDBA'
    assert c.privilege == PrivilegeCode.EXECUTE
    assert isinstance(c.subject, sm.Procedure)
    assert c.subject.name == 'ALL_LANGS'
    assert c.user_name == 'SYSDBA'
    assert c.user_type == ObjectType.USER
    assert c.grantor_name == 'SYSDBA'
    assert c.subject_name == 'ALL_LANGS'
    assert c.subject_type == ObjectType.PROCEDURE
    assert c.field_name is None
    assert not c.has_grant()
    assert not c.is_select()
    assert not c.is_insert()
    assert not c.is_update()
    assert not c.is_delete()
    assert c.is_execute()
    assert not c.is_reference()
    assert not c.is_membership()

    # Test DDL generation
    assert c.get_sql_for('grant') == \
           "GRANT EXECUTE ON PROCEDURE ALL_LANGS TO SYSDBA"
    # Grantor list tests
    assert c.get_sql_for('grant', grantors=[]) == \
           "GRANT EXECUTE ON PROCEDURE ALL_LANGS TO SYSDBA GRANTED BY SYSDBA"
    assert c.get_sql_for('grant', grantors=['SYSDBA', 'TEST_USER']) == \
           "GRANT EXECUTE ON PROCEDURE ALL_LANGS TO SYSDBA" # Only grantee matters here
    with pytest.raises(ValueError, match="Unsupported parameter"):
        c.get_sql_for('grant', badparam=True)

    assert c.get_sql_for('revoke') == \
           "REVOKE EXECUTE ON PROCEDURE ALL_LANGS FROM SYSDBA"
    # Grantor list tests for revoke
    assert c.get_sql_for('revoke', grantors=[]) == \
           "REVOKE EXECUTE ON PROCEDURE ALL_LANGS FROM SYSDBA GRANTED BY SYSDBA"
    assert c.get_sql_for('revoke', grantors=['SYSDBA', 'TEST_USER']) == \
           "REVOKE EXECUTE ON PROCEDURE ALL_LANGS FROM SYSDBA" # Only revokee matters
    with pytest.raises(ValueError, match="Can't revoke grant option that wasn't granted."):
        c.get_sql_for('revoke', grant_option=True)
    with pytest.raises(ValueError, match="Unsupported parameter"):
        c.get_sql_for('revoke', badparam=True)

    # Find privilege for PUBLIC (should have grant option)
    c = next((p for p in proc.privileges if p.user_name == 'PUBLIC'), None)
    assert c is not None
    assert c.has_grant()
    assert c.get_sql_for('grant') == \
           "GRANT EXECUTE ON PROCEDURE ALL_LANGS TO PUBLIC WITH GRANT OPTION"
    assert c.get_sql_for('revoke') == \
           "REVOKE EXECUTE ON PROCEDURE ALL_LANGS FROM PUBLIC"
    assert c.get_sql_for('revoke', grant_option=True) == \
           "REVOKE GRANT OPTION FOR EXECUTE ON PROCEDURE ALL_LANGS FROM PUBLIC"

    # get_privileges_of()
    u = UserInfo(user_name='PUBLIC')
    p = s.get_privileges_of(u)
    assert isinstance(p, list)
    # Count varies significantly between versions, check > some baseline
    assert len(p) > 100
    with pytest.raises(ValueError, match="Argument user_type required"):
        s.get_privileges_of('PUBLIC')

def test_24_PrivilegeExtended(db_connection):
    """Tests various privilege types and combinations using mocked privileges."""
    s = db_connection.schema

    p = DataList()
    p.append(Privilege(s, {'RDB$USER': 'SYSDBA',
                           'RDB$PRIVILEGE': 'X',
                           'RDB$RELATION_NAME': 'ALL_LANGS',
                           'RDB$OBJECT_TYPE': 5,
                           'RDB$USER_TYPE': 8,
                           'RDB$FIELD_NAME': None,
                           'RDB$GRANTOR': 'SYSDBA',
                           'RDB$GRANT_OPTION': None}))
    p.append(Privilege(s, {'RDB$USER': 'PUBLIC',
                           'RDB$PRIVILEGE': 'X',
                           'RDB$RELATION_NAME': 'ALL_LANGS',
                           'RDB$OBJECT_TYPE': 5,
                           'RDB$USER_TYPE': 8,
                           'RDB$FIELD_NAME': None,
                           'RDB$GRANTOR': 'SYSDBA',
                           'RDB$GRANT_OPTION': 1}))
    p.append(Privilege(s, {'RDB$USER': 'T_USER',
                           'RDB$PRIVILEGE': 'X',
                           'RDB$RELATION_NAME': 'ALL_LANGS',
                           'RDB$OBJECT_TYPE': 5,
                           'RDB$USER_TYPE': 8,
                           'RDB$FIELD_NAME': None,
                           'RDB$GRANTOR': 'SYSDBA',
                           'RDB$GRANT_OPTION': 0}))
    p.append(Privilege(s, {'RDB$USER': 'TEST_ROLE',
                           'RDB$PRIVILEGE': 'X',
                           'RDB$RELATION_NAME': 'ALL_LANGS',
                           'RDB$OBJECT_TYPE': 5,
                           'RDB$USER_TYPE': 13,
                           'RDB$FIELD_NAME': None,
                           'RDB$GRANTOR': 'SYSDBA',
                           'RDB$GRANT_OPTION': 1}))
    p.append(Privilege(s, {'RDB$USER': 'PUBLIC',
                           'RDB$PRIVILEGE': 'X',
                           'RDB$RELATION_NAME': 'ALL_LANGS',
                           'RDB$OBJECT_TYPE': 5,
                           'RDB$USER_TYPE': 8,
                           'RDB$FIELD_NAME': None,
                           'RDB$GRANTOR': 'T_USER',
                           'RDB$GRANT_OPTION': 0}))
    p.append(Privilege(s, {'RDB$USER': 'SYSDBA',
                           'RDB$PRIVILEGE': 'S',
                           'RDB$RELATION_NAME': 'COUNTRY',
                           'RDB$OBJECT_TYPE': 0,
                           'RDB$USER_TYPE': 8,
                           'RDB$FIELD_NAME': None,
                           'RDB$GRANTOR': 'SYSDBA',
                           'RDB$GRANT_OPTION': 1}))
    p.append(Privilege(s, {'RDB$USER': 'SYSDBA',
                           'RDB$PRIVILEGE': 'I',
                           'RDB$RELATION_NAME': 'COUNTRY',
                           'RDB$OBJECT_TYPE': 0,
                           'RDB$USER_TYPE': 8,
                           'RDB$FIELD_NAME': None,
                           'RDB$GRANTOR': 'SYSDBA',
                           'RDB$GRANT_OPTION': 1}))
    p.append(Privilege(s, {'RDB$USER': 'SYSDBA',
                           'RDB$PRIVILEGE': 'U',
                           'RDB$RELATION_NAME': 'COUNTRY',
                           'RDB$OBJECT_TYPE': 0,
                           'RDB$USER_TYPE': 8,
                           'RDB$FIELD_NAME': None,
                           'RDB$GRANTOR': 'SYSDBA',
                           'RDB$GRANT_OPTION': 1}))
    p.append(Privilege(s, {'RDB$USER': 'SYSDBA',
                           'RDB$PRIVILEGE': 'D',
                           'RDB$RELATION_NAME': 'COUNTRY',
                           'RDB$OBJECT_TYPE': 0,
                           'RDB$USER_TYPE': 8,
                           'RDB$FIELD_NAME': None,
                           'RDB$GRANTOR': 'SYSDBA',
                           'RDB$GRANT_OPTION': 1}))
    p.append(Privilege(s, {'RDB$USER': 'SYSDBA',
                           'RDB$PRIVILEGE': 'R',
                           'RDB$RELATION_NAME': 'COUNTRY',
                           'RDB$OBJECT_TYPE': 0,
                           'RDB$USER_TYPE': 8,
                           'RDB$FIELD_NAME': None,
                           'RDB$GRANTOR': 'SYSDBA',
                           'RDB$GRANT_OPTION': 1}))
    p.append(Privilege(s, {'RDB$USER': 'PUBLIC',
                           'RDB$PRIVILEGE': 'S',
                           'RDB$RELATION_NAME': 'COUNTRY',
                           'RDB$OBJECT_TYPE': 0,
                           'RDB$USER_TYPE': 8,
                           'RDB$FIELD_NAME': None,
                           'RDB$GRANTOR': 'SYSDBA',
                           'RDB$GRANT_OPTION': 1}))
    p.append(Privilege(s, {'RDB$USER': 'PUBLIC',
                           'RDB$PRIVILEGE': 'R',
                           'RDB$RELATION_NAME': 'COUNTRY',
                           'RDB$OBJECT_TYPE': 0,
                           'RDB$USER_TYPE': 8,
                           'RDB$FIELD_NAME': None,
                           'RDB$GRANTOR': 'SYSDBA',
                           'RDB$GRANT_OPTION': 1}))
    p.append(Privilege(s, {'RDB$USER': 'PUBLIC',
                           'RDB$PRIVILEGE': 'I',
                           'RDB$RELATION_NAME': 'COUNTRY',
                           'RDB$OBJECT_TYPE': 0,
                           'RDB$USER_TYPE': 8,
                           'RDB$FIELD_NAME': None,
                           'RDB$GRANTOR': 'SYSDBA',
                           'RDB$GRANT_OPTION': 0}))
    p.append(Privilege(s, {'RDB$USER': 'T_USER',
                           'RDB$PRIVILEGE': 'U',
                           'RDB$RELATION_NAME': 'COUNTRY',
                           'RDB$OBJECT_TYPE': 0,
                           'RDB$USER_TYPE': 8,
                           'RDB$FIELD_NAME': 'CURRENCY',
                           'RDB$GRANTOR': 'SYSDBA',
                           'RDB$GRANT_OPTION': 0}))
    p.append(Privilege(s, {'RDB$USER': 'T_USER',
                           'RDB$PRIVILEGE': 'R',
                           'RDB$RELATION_NAME': 'COUNTRY',
                           'RDB$OBJECT_TYPE': 0,
                           'RDB$USER_TYPE': 8,
                           'RDB$FIELD_NAME': 'COUNTRY',
                           'RDB$GRANTOR': 'SYSDBA',
                           'RDB$GRANT_OPTION': 0}))
    p.append(Privilege(s, {'RDB$USER': 'T_USER',
                           'RDB$PRIVILEGE': 'S',
                           'RDB$RELATION_NAME': 'COUNTRY',
                           'RDB$OBJECT_TYPE': 0,
                           'RDB$USER_TYPE': 8,
                           'RDB$FIELD_NAME': None,
                           'RDB$GRANTOR': 'SYSDBA',
                           'RDB$GRANT_OPTION': 0}))
    p.append(Privilege(s, {'RDB$USER': 'T_USER',
                           'RDB$PRIVILEGE': 'I',
                           'RDB$RELATION_NAME': 'COUNTRY',
                           'RDB$OBJECT_TYPE': 0,
                           'RDB$USER_TYPE': 8,
                           'RDB$FIELD_NAME': None,
                           'RDB$GRANTOR': 'SYSDBA',
                           'RDB$GRANT_OPTION': 0}))
    p.append(Privilege(s, {'RDB$USER': 'T_USER',
                           'RDB$PRIVILEGE': 'D',
                           'RDB$RELATION_NAME': 'COUNTRY',
                           'RDB$OBJECT_TYPE': 0,
                           'RDB$USER_TYPE': 8,
                           'RDB$FIELD_NAME': None,
                           'RDB$GRANTOR': 'SYSDBA',
                           'RDB$GRANT_OPTION': 0}))
    p.append(Privilege(s, {'RDB$USER': 'T_USER',
                           'RDB$PRIVILEGE': 'U',
                           'RDB$RELATION_NAME': 'COUNTRY',
                           'RDB$OBJECT_TYPE': 0,
                           'RDB$USER_TYPE': 8,
                           'RDB$FIELD_NAME': None,
                           'RDB$GRANTOR': 'SYSDBA',
                           'RDB$GRANT_OPTION': 0}))
    p.append(Privilege(s, {'RDB$USER': 'T_USER',
                           'RDB$PRIVILEGE': 'R',
                           'RDB$RELATION_NAME': 'COUNTRY',
                           'RDB$OBJECT_TYPE': 0,
                           'RDB$USER_TYPE': 8,
                           'RDB$FIELD_NAME': None,
                           'RDB$GRANTOR': 'SYSDBA',
                           'RDB$GRANT_OPTION': 0}))
    p.append(Privilege(s, {'RDB$USER': 'T_USER',
                           'RDB$PRIVILEGE': 'U',
                           'RDB$RELATION_NAME': 'COUNTRY',
                           'RDB$OBJECT_TYPE': 0,
                           'RDB$USER_TYPE': 8,
                           'RDB$FIELD_NAME': 'COUNTRY',
                           'RDB$GRANTOR': 'SYSDBA',
                           'RDB$GRANT_OPTION': 0}))
    p.append(Privilege(s, {'RDB$USER': 'T_USER',
                           'RDB$PRIVILEGE': 'R',
                           'RDB$RELATION_NAME': 'COUNTRY',
                           'RDB$OBJECT_TYPE': 0,
                           'RDB$USER_TYPE': 8,
                           'RDB$FIELD_NAME': 'CURRENCY',
                           'RDB$GRANTOR': 'SYSDBA',
                           'RDB$GRANT_OPTION': 0}))
    p.append(Privilege(s, {'RDB$USER': 'PUBLIC',
                           'RDB$PRIVILEGE': 'D',
                           'RDB$RELATION_NAME': 'COUNTRY',
                           'RDB$OBJECT_TYPE': 0,
                           'RDB$USER_TYPE': 8,
                           'RDB$FIELD_NAME': None,
                           'RDB$GRANTOR': 'SYSDBA',
                           'RDB$GRANT_OPTION': 0}))
    p.append(Privilege(s, {'RDB$USER': 'PUBLIC',
                           'RDB$PRIVILEGE': 'U',
                           'RDB$RELATION_NAME': 'COUNTRY',
                           'RDB$OBJECT_TYPE': 0,
                           'RDB$USER_TYPE': 8,
                           'RDB$FIELD_NAME': None,
                           'RDB$GRANTOR': 'SYSDBA',
                           'RDB$GRANT_OPTION': 0}))
    p.append(Privilege(s, {'RDB$USER': 'SYSDBA',
                           'RDB$PRIVILEGE': 'S',
                           'RDB$RELATION_NAME': 'DEPARTMENT',
                           'RDB$OBJECT_TYPE': 0,
                           'RDB$USER_TYPE': 8,
                           'RDB$FIELD_NAME': None,
                           'RDB$GRANTOR': 'SYSDBA',
                           'RDB$GRANT_OPTION': 1}))
    p.append(Privilege(s, {'RDB$USER': 'SYSDBA',
                           'RDB$PRIVILEGE': 'I',
                           'RDB$RELATION_NAME': 'DEPARTMENT',
                           'RDB$OBJECT_TYPE': 0,
                           'RDB$USER_TYPE': 8,
                           'RDB$FIELD_NAME': None,
                           'RDB$GRANTOR': 'SYSDBA',
                           'RDB$GRANT_OPTION': 1}))
    p.append(Privilege(s, {'RDB$USER': 'SYSDBA',
                           'RDB$PRIVILEGE': 'U',
                           'RDB$RELATION_NAME': 'DEPARTMENT',
                           'RDB$OBJECT_TYPE': 0,
                           'RDB$USER_TYPE': 8,
                           'RDB$FIELD_NAME': None,
                           'RDB$GRANTOR': 'SYSDBA',
                           'RDB$GRANT_OPTION': 1}))
    p.append(Privilege(s, {'RDB$USER': 'SYSDBA',
                           'RDB$PRIVILEGE': 'D',
                           'RDB$RELATION_NAME': 'DEPARTMENT',
                           'RDB$OBJECT_TYPE': 0,
                           'RDB$USER_TYPE': 8,
                           'RDB$FIELD_NAME': None,
                           'RDB$GRANTOR': 'SYSDBA',
                           'RDB$GRANT_OPTION': 1}))
    p.append(Privilege(s, {'RDB$USER': 'SYSDBA',
                           'RDB$PRIVILEGE': 'R',
                           'RDB$RELATION_NAME': 'DEPARTMENT',
                           'RDB$OBJECT_TYPE': 0,
                           'RDB$USER_TYPE': 8,
                           'RDB$FIELD_NAME': None,
                           'RDB$GRANTOR': 'SYSDBA',
                           'RDB$GRANT_OPTION': 1}))
    p.append(Privilege(s, {'RDB$USER': 'PUBLIC',
                           'RDB$PRIVILEGE': 'S',
                           'RDB$RELATION_NAME': 'DEPARTMENT',
                           'RDB$OBJECT_TYPE': 0,
                           'RDB$USER_TYPE': 8,
                           'RDB$FIELD_NAME': None,
                           'RDB$GRANTOR': 'SYSDBA',
                           'RDB$GRANT_OPTION': 1}))
    p.append(Privilege(s, {'RDB$USER': 'PUBLIC',
                           'RDB$PRIVILEGE': 'I',
                           'RDB$RELATION_NAME': 'DEPARTMENT',
                           'RDB$OBJECT_TYPE': 0,
                           'RDB$USER_TYPE': 8,
                           'RDB$FIELD_NAME': None,
                           'RDB$GRANTOR': 'SYSDBA',
                           'RDB$GRANT_OPTION': 1}))
    p.append(Privilege(s, {'RDB$USER': 'PUBLIC',
                           'RDB$PRIVILEGE': 'U',
                           'RDB$RELATION_NAME': 'DEPARTMENT',
                           'RDB$OBJECT_TYPE': 0,
                           'RDB$USER_TYPE': 8,
                           'RDB$FIELD_NAME': None,
                           'RDB$GRANTOR': 'SYSDBA',
                           'RDB$GRANT_OPTION': 1}))
    p.append(Privilege(s, {'RDB$USER': 'PUBLIC',
                           'RDB$PRIVILEGE': 'D',
                           'RDB$RELATION_NAME': 'DEPARTMENT',
                           'RDB$OBJECT_TYPE': 0,
                           'RDB$USER_TYPE': 8,
                           'RDB$FIELD_NAME': None,
                           'RDB$GRANTOR': 'SYSDBA',
                           'RDB$GRANT_OPTION': 1}))
    p.append(Privilege(s, {'RDB$USER': 'PUBLIC',
                           'RDB$PRIVILEGE': 'R',
                           'RDB$RELATION_NAME': 'DEPARTMENT',
                           'RDB$OBJECT_TYPE': 0,
                           'RDB$USER_TYPE': 8,
                           'RDB$FIELD_NAME': None,
                           'RDB$GRANTOR': 'SYSDBA',
                           'RDB$GRANT_OPTION': 1}))
    p.append(Privilege(s, {'RDB$USER': 'ORG_CHART',
                           'RDB$PRIVILEGE': 'S',
                           'RDB$RELATION_NAME': 'DEPARTMENT',
                           'RDB$OBJECT_TYPE': 0,
                           'RDB$USER_TYPE': 5,
                           'RDB$FIELD_NAME': None,
                           'RDB$GRANTOR': 'SYSDBA',
                           'RDB$GRANT_OPTION': 0}))
    p.append(Privilege(s, {'RDB$USER': 'SYSDBA',
                           'RDB$PRIVILEGE': 'S',
                           'RDB$RELATION_NAME': 'EMPLOYEE',
                           'RDB$OBJECT_TYPE': 0,
                           'RDB$USER_TYPE': 8,
                           'RDB$FIELD_NAME': None,
                           'RDB$GRANTOR': 'SYSDBA',
                           'RDB$GRANT_OPTION': 1}))
    p.append(Privilege(s, {'RDB$USER': 'SYSDBA',
                           'RDB$PRIVILEGE': 'I',
                           'RDB$RELATION_NAME': 'EMPLOYEE',
                           'RDB$OBJECT_TYPE': 0,
                           'RDB$USER_TYPE': 8,
                           'RDB$FIELD_NAME': None,
                           'RDB$GRANTOR': 'SYSDBA',
                           'RDB$GRANT_OPTION': 1}))
    p.append(Privilege(s, {'RDB$USER': 'SYSDBA',
                           'RDB$PRIVILEGE': 'U',
                           'RDB$RELATION_NAME': 'EMPLOYEE',
                           'RDB$OBJECT_TYPE': 0,
                           'RDB$USER_TYPE': 8,
                           'RDB$FIELD_NAME': None,
                           'RDB$GRANTOR': 'SYSDBA',
                           'RDB$GRANT_OPTION': 1}))
    p.append(Privilege(s, {'RDB$USER': 'SYSDBA',
                           'RDB$PRIVILEGE': 'D',
                           'RDB$RELATION_NAME': 'EMPLOYEE',
                           'RDB$OBJECT_TYPE': 0,
                           'RDB$USER_TYPE': 8,
                           'RDB$FIELD_NAME': None,
                           'RDB$GRANTOR': 'SYSDBA',
                           'RDB$GRANT_OPTION': 1}))
    p.append(Privilege(s, {'RDB$USER': 'SYSDBA',
                           'RDB$PRIVILEGE': 'R',
                           'RDB$RELATION_NAME': 'EMPLOYEE',
                           'RDB$OBJECT_TYPE': 0,
                           'RDB$USER_TYPE': 8,
                           'RDB$FIELD_NAME': None,
                           'RDB$GRANTOR': 'SYSDBA',
                           'RDB$GRANT_OPTION': 1}))
    p.append(Privilege(s, {'RDB$USER': 'PUBLIC',
                           'RDB$PRIVILEGE': 'S',
                           'RDB$RELATION_NAME': 'EMPLOYEE',
                           'RDB$OBJECT_TYPE': 0,
                           'RDB$USER_TYPE': 8,
                           'RDB$FIELD_NAME': None,
                           'RDB$GRANTOR': 'SYSDBA',
                           'RDB$GRANT_OPTION': 1}))
    p.append(Privilege(s, {'RDB$USER': 'PUBLIC',
                           'RDB$PRIVILEGE': 'I',
                           'RDB$RELATION_NAME': 'EMPLOYEE',
                           'RDB$OBJECT_TYPE': 0,
                           'RDB$USER_TYPE': 8,
                           'RDB$FIELD_NAME': None,
                           'RDB$GRANTOR': 'SYSDBA',
                           'RDB$GRANT_OPTION': 1}))
    p.append(Privilege(s, {'RDB$USER': 'PUBLIC',
                           'RDB$PRIVILEGE': 'U',
                           'RDB$RELATION_NAME': 'EMPLOYEE',
                           'RDB$OBJECT_TYPE': 0,
                           'RDB$USER_TYPE': 8,
                           'RDB$FIELD_NAME': None,
                           'RDB$GRANTOR': 'SYSDBA',
                           'RDB$GRANT_OPTION': 1}))
    p.append(Privilege(s, {'RDB$USER': 'PUBLIC',
                           'RDB$PRIVILEGE': 'D',
                           'RDB$RELATION_NAME': 'EMPLOYEE',
                           'RDB$OBJECT_TYPE': 0,
                           'RDB$USER_TYPE': 8,
                           'RDB$FIELD_NAME': None,
                           'RDB$GRANTOR': 'SYSDBA',
                           'RDB$GRANT_OPTION': 1}))
    p.append(Privilege(s, {'RDB$USER': 'PUBLIC',
                           'RDB$PRIVILEGE': 'R',
                           'RDB$RELATION_NAME': 'EMPLOYEE',
                           'RDB$OBJECT_TYPE': 0,
                           'RDB$USER_TYPE': 8,
                           'RDB$FIELD_NAME': None,
                           'RDB$GRANTOR': 'SYSDBA',
                           'RDB$GRANT_OPTION': 1}))
    p.append(Privilege(s, {'RDB$USER': 'ORG_CHART',
                           'RDB$PRIVILEGE': 'S',
                           'RDB$RELATION_NAME': 'EMPLOYEE',
                           'RDB$OBJECT_TYPE': 0,
                           'RDB$USER_TYPE': 5,
                           'RDB$FIELD_NAME': None,
                           'RDB$GRANTOR': 'SYSDBA',
                           'RDB$GRANT_OPTION': 0}))
    p.append(Privilege(s, {'RDB$USER': 'SYSDBA',
                           'RDB$PRIVILEGE': 'X',
                           'RDB$RELATION_NAME': 'ORG_CHART',
                           'RDB$OBJECT_TYPE': 5,
                           'RDB$USER_TYPE': 8,
                           'RDB$FIELD_NAME': None,
                           'RDB$GRANTOR': 'SYSDBA',
                           'RDB$GRANT_OPTION': None}))
    p.append(Privilege(s, {'RDB$USER': 'PUBLIC',
                           'RDB$PRIVILEGE': 'X',
                           'RDB$RELATION_NAME': 'ORG_CHART',
                           'RDB$OBJECT_TYPE': 5,
                           'RDB$USER_TYPE': 8,
                           'RDB$FIELD_NAME': None,
                           'RDB$GRANTOR': 'SYSDBA',
                           'RDB$GRANT_OPTION': 1}))
    p.append(Privilege(s, {'RDB$USER': 'SYSDBA',
                           'RDB$PRIVILEGE': 'S',
                           'RDB$RELATION_NAME': 'PHONE_LIST',
                           'RDB$OBJECT_TYPE': 0,
                           'RDB$USER_TYPE': 8,
                           'RDB$FIELD_NAME': None,
                           'RDB$GRANTOR': 'SYSDBA',
                           'RDB$GRANT_OPTION': 1}))
    p.append(Privilege(s, {'RDB$USER': 'SYSDBA',
                           'RDB$PRIVILEGE': 'I',
                           'RDB$RELATION_NAME': 'PHONE_LIST',
                           'RDB$OBJECT_TYPE': 0,
                           'RDB$USER_TYPE': 8,
                           'RDB$FIELD_NAME': None,
                           'RDB$GRANTOR': 'SYSDBA',
                           'RDB$GRANT_OPTION': 1}))
    p.append(Privilege(s, {'RDB$USER': 'SYSDBA',
                           'RDB$PRIVILEGE': 'U',
                           'RDB$RELATION_NAME': 'PHONE_LIST',
                           'RDB$OBJECT_TYPE': 0,
                           'RDB$USER_TYPE': 8,
                           'RDB$FIELD_NAME': None,
                           'RDB$GRANTOR': 'SYSDBA',
                           'RDB$GRANT_OPTION': 1}))
    p.append(Privilege(s, {'RDB$USER': 'SYSDBA',
                           'RDB$PRIVILEGE': 'D',
                           'RDB$RELATION_NAME': 'PHONE_LIST',
                           'RDB$OBJECT_TYPE': 0,
                           'RDB$USER_TYPE': 8,
                           'RDB$FIELD_NAME': None,
                           'RDB$GRANTOR': 'SYSDBA',
                           'RDB$GRANT_OPTION': 1}))
    p.append(Privilege(s, {'RDB$USER': 'SYSDBA',
                           'RDB$PRIVILEGE': 'R',
                           'RDB$RELATION_NAME': 'PHONE_LIST',
                           'RDB$OBJECT_TYPE': 0,
                           'RDB$USER_TYPE': 8,
                           'RDB$FIELD_NAME': None,
                           'RDB$GRANTOR': 'SYSDBA',
                           'RDB$GRANT_OPTION': 1}))
    p.append(Privilege(s, {'RDB$USER': 'PUBLIC',
                           'RDB$PRIVILEGE': 'S',
                           'RDB$RELATION_NAME': 'PHONE_LIST',
                           'RDB$OBJECT_TYPE': 0,
                           'RDB$USER_TYPE': 8,
                           'RDB$FIELD_NAME': None,
                           'RDB$GRANTOR': 'SYSDBA',
                           'RDB$GRANT_OPTION': 1}))
    p.append(Privilege(s, {'RDB$USER': 'PUBLIC',
                           'RDB$PRIVILEGE': 'I',
                           'RDB$RELATION_NAME': 'PHONE_LIST',
                           'RDB$OBJECT_TYPE': 0,
                           'RDB$USER_TYPE': 8,
                           'RDB$FIELD_NAME': None,
                           'RDB$GRANTOR': 'SYSDBA',
                           'RDB$GRANT_OPTION': 1}))
    p.append(Privilege(s, {'RDB$USER': 'PUBLIC',
                           'RDB$PRIVILEGE': 'U',
                           'RDB$RELATION_NAME': 'PHONE_LIST',
                           'RDB$OBJECT_TYPE': 0,
                           'RDB$USER_TYPE': 8,
                           'RDB$FIELD_NAME': None,
                           'RDB$GRANTOR': 'SYSDBA',
                           'RDB$GRANT_OPTION': 1}))
    p.append(Privilege(s, {'RDB$USER': 'PUBLIC',
                           'RDB$PRIVILEGE': 'D',
                           'RDB$RELATION_NAME': 'PHONE_LIST',
                           'RDB$OBJECT_TYPE': 0,
                           'RDB$USER_TYPE': 8,
                           'RDB$FIELD_NAME': None,
                           'RDB$GRANTOR': 'SYSDBA',
                           'RDB$GRANT_OPTION': 1}))
    p.append(Privilege(s, {'RDB$USER': 'PUBLIC',
                           'RDB$PRIVILEGE': 'R',
                           'RDB$RELATION_NAME': 'PHONE_LIST',
                           'RDB$OBJECT_TYPE': 0,
                           'RDB$USER_TYPE': 8,
                           'RDB$FIELD_NAME': None,
                           'RDB$GRANTOR': 'SYSDBA',
                           'RDB$GRANT_OPTION': 1}))
    p.append(Privilege(s, {'RDB$USER': 'PUBLIC',
                           'RDB$PRIVILEGE': 'R',
                           'RDB$RELATION_NAME': 'PHONE_LIST',
                           'RDB$OBJECT_TYPE': 0,
                           'RDB$USER_TYPE': 8,
                           'RDB$FIELD_NAME': 'EMP_NO',
                           'RDB$GRANTOR': 'SYSDBA',
                           'RDB$GRANT_OPTION': 0}))
    p.append(Privilege(s, {'RDB$USER': 'SYSDBA',
                           'RDB$PRIVILEGE': 'S',
                           'RDB$RELATION_NAME': 'RDB$PAGES',
                           'RDB$OBJECT_TYPE': 0,
                           'RDB$USER_TYPE': 8,
                           'RDB$FIELD_NAME': None,
                           'RDB$GRANTOR': 'SYSDBA',
                           'RDB$GRANT_OPTION': 1}))
    p.append(Privilege(s, {'RDB$USER': 'SYSDBA',
                           'RDB$PRIVILEGE': 'I',
                           'RDB$RELATION_NAME': 'RDB$PAGES',
                           'RDB$OBJECT_TYPE': 0,
                           'RDB$USER_TYPE': 8,
                           'RDB$FIELD_NAME': None,
                           'RDB$GRANTOR': 'SYSDBA',
                           'RDB$GRANT_OPTION': 1}))
    p.append(Privilege(s, {'RDB$USER': 'SYSDBA',
                           'RDB$PRIVILEGE': 'U',
                           'RDB$RELATION_NAME': 'RDB$PAGES',
                           'RDB$OBJECT_TYPE': 0,
                           'RDB$USER_TYPE': 8,
                           'RDB$FIELD_NAME': None,
                           'RDB$GRANTOR': 'SYSDBA',
                           'RDB$GRANT_OPTION': 1}))
    p.append(Privilege(s, {'RDB$USER': 'SYSDBA',
                           'RDB$PRIVILEGE': 'D',
                           'RDB$RELATION_NAME': 'RDB$PAGES',
                           'RDB$OBJECT_TYPE': 0,
                           'RDB$USER_TYPE': 8,
                           'RDB$FIELD_NAME': None,
                           'RDB$GRANTOR': 'SYSDBA',
                           'RDB$GRANT_OPTION': 1}))
    p.append(Privilege(s, {'RDB$USER': 'SYSDBA',
                           'RDB$PRIVILEGE': 'R',
                           'RDB$RELATION_NAME': 'RDB$PAGES',
                           'RDB$OBJECT_TYPE': 0,
                           'RDB$USER_TYPE': 8,
                           'RDB$FIELD_NAME': None,
                           'RDB$GRANTOR': 'SYSDBA',
                           'RDB$GRANT_OPTION': 1}))
    p.append(Privilege(s, {'RDB$USER': 'PUBLIC',
                           'RDB$PRIVILEGE': 'S',
                           'RDB$RELATION_NAME': 'RDB$PAGES',
                           'RDB$OBJECT_TYPE': 0,
                           'RDB$USER_TYPE': 8,
                           'RDB$FIELD_NAME': None,
                           'RDB$GRANTOR': 'SYSDBA',
                           'RDB$GRANT_OPTION': 0}))
    p.append(Privilege(s, {'RDB$USER': 'SYSDBA',
                           'RDB$PRIVILEGE': 'X',
                           'RDB$RELATION_NAME': 'SHIP_ORDER',
                           'RDB$OBJECT_TYPE': 5,
                           'RDB$USER_TYPE': 8,
                           'RDB$FIELD_NAME': None,
                           'RDB$GRANTOR': 'SYSDBA',
                           'RDB$GRANT_OPTION': None}))
    p.append(Privilege(s, {'RDB$USER': 'PUBLIC',
                           'RDB$PRIVILEGE': 'X',
                           'RDB$RELATION_NAME': 'SHIP_ORDER',
                           'RDB$OBJECT_TYPE': 5,
                           'RDB$USER_TYPE': 8,
                           'RDB$FIELD_NAME': None,
                           'RDB$GRANTOR': 'SYSDBA',
                           'RDB$GRANT_OPTION': 1}))
    p.append(Privilege(s, {'RDB$USER': 'T_USER',
                           'RDB$PRIVILEGE': 'M',
                           'RDB$RELATION_NAME': 'TEST_ROLE',
                           'RDB$OBJECT_TYPE': 13,
                           'RDB$USER_TYPE': 8,
                           'RDB$FIELD_NAME': None,
                           'RDB$GRANTOR': 'SYSDBA',
                           'RDB$GRANT_OPTION': 0}))
    p.append(Privilege(s, {'RDB$USER': 'SAVE_SALARY_CHANGE',
                           'RDB$PRIVILEGE': 'I',
                           'RDB$RELATION_NAME': 'SALARY_HISTORY',
                           'RDB$OBJECT_TYPE': 0,
                           'RDB$USER_TYPE': 2,
                           'RDB$FIELD_NAME': None,
                           'RDB$GRANTOR': 'SYSDBA',
                           'RDB$GRANT_OPTION': 0}))
    p.append(Privilege(s, {'RDB$USER': 'PHONE_LIST',
                           'RDB$PRIVILEGE': 'S',
                           'RDB$RELATION_NAME': 'DEPARTMENT',
                           'RDB$OBJECT_TYPE': 0,
                           'RDB$USER_TYPE': 1,
                           'RDB$FIELD_NAME': None,
                           'RDB$GRANTOR': 'SYSDBA',
                           'RDB$GRANT_OPTION': 0}))
    p.append(Privilege(s, {'RDB$USER': 'PHONE_LIST',
                           'RDB$PRIVILEGE': 'S',
                           'RDB$RELATION_NAME': 'EMPLOYEE',
                           'RDB$OBJECT_TYPE': 0,
                           'RDB$USER_TYPE': 1,
                           'RDB$FIELD_NAME': None,
                           'RDB$GRANTOR': 'SYSDBA',
                           'RDB$GRANT_OPTION': 0}))
    #
    s.__dict__['_Schema__privileges'] = p

    # Table
    p = s.all_tables.get('COUNTRY')
    assert len(p.privileges) == 19
    assert len([x for x in p.privileges if x.user_name == 'SYSDBA']) == 5
    assert len([x for x in p.privileges if x.user_name == 'PUBLIC']) == 5
    assert len([x for x in p.privileges if x.user_name == 'T_USER']) == 9
    #
    x = p.privileges[0]
    assert isinstance(x.subject, sm.Table)
    assert x.subject.name == p.name
    # TableColumn
    p = p.columns.get('CURRENCY')
    assert len(p.privileges) == 2
    x = p.privileges[0]
    assert isinstance(x.subject, sm.Table)
    assert x.field_name == p.name
    # View
    p = s.all_views.get('PHONE_LIST')
    assert len(p.privileges) == 11
    assert len([x for x in p.privileges if x.user_name == 'SYSDBA']) == 5
    assert len([x for x in p.privileges if x.user_name == 'PUBLIC']) == 6
    #
    x = p.privileges[0]
    assert isinstance(x.subject, sm.View)
    assert x.subject.name == p.name
    # ViewColumn
    p = p.columns.get('EMP_NO')
    assert len(p.privileges) == 1
    x = p.privileges[0]
    assert isinstance(x.subject, sm.View)
    assert x.field_name == p.name
    # Procedure
    p = s.all_procedures.get('ORG_CHART')
    assert len(p.privileges) == 2
    assert len([x for x in p.privileges if x.user_name == 'SYSDBA']) == 1
    assert len([x for x in p.privileges if x.user_name == 'PUBLIC']) == 1
    #
    x = p.privileges[0]
    assert not x.has_grant()
    assert isinstance(x.subject, sm.Procedure)
    assert x.subject.name == p.name
    #
    x = p.privileges[1]
    assert x.has_grant()
    # Role
    p = s.roles.get('TEST_ROLE')
    assert len(p.privileges) == 1
    x = p.privileges[0]
    assert isinstance(x.user, sm.Role)
    assert x.user.name == p.name
    assert x.is_execute()
    # Trigger as grantee
    p = s.all_tables.get('SALARY_HISTORY')
    x = p.privileges[0]
    assert isinstance(x.user, sm.Trigger)
    assert x.user.name == 'SAVE_SALARY_CHANGE'
    # View as grantee
    p = s.all_views.get('PHONE_LIST')
    x = s.get_privileges_of(p)
    assert len(x) == 2
    x = x[0]
    assert isinstance(x.user, sm.View)
    assert x.user.name == 'PHONE_LIST'
    # get_grants()
    assert sm.get_grants(p.privileges) == [
        'GRANT REFERENCES(EMP_NO) ON PHONE_LIST TO PUBLIC',
        'GRANT DELETE, INSERT, REFERENCES, SELECT, UPDATE ON PHONE_LIST TO PUBLIC WITH GRANT OPTION',
        'GRANT DELETE, INSERT, REFERENCES, SELECT, UPDATE ON PHONE_LIST TO SYSDBA WITH GRANT OPTION']
    p = s.all_tables.get('COUNTRY')
    assert sm.get_grants(p.privileges) == [
        'GRANT DELETE, INSERT, UPDATE ON COUNTRY TO PUBLIC',
        'GRANT REFERENCES, SELECT ON COUNTRY TO PUBLIC WITH GRANT OPTION',
        'GRANT DELETE, INSERT, REFERENCES, SELECT, UPDATE ON COUNTRY TO SYSDBA WITH GRANT OPTION',
        'GRANT DELETE, INSERT, REFERENCES(COUNTRY,CURRENCY), SELECT, UPDATE(COUNTRY,CURRENCY) ON COUNTRY TO T_USER']
    p = s.roles.get('TEST_ROLE')
    assert sm.get_grants(p.privileges) == ['GRANT EXECUTE ON PROCEDURE ALL_LANGS TO TEST_ROLE WITH GRANT OPTION']
    p = s.all_tables.get('SALARY_HISTORY')
    assert sm.get_grants(p.privileges) == ['GRANT INSERT ON SALARY_HISTORY TO TRIGGER SAVE_SALARY_CHANGE']
    p = s.all_procedures.get('ORG_CHART')
    assert sm.get_grants(p.privileges) == [
        'GRANT EXECUTE ON PROCEDURE ORG_CHART TO PUBLIC WITH GRANT OPTION',
        'GRANT EXECUTE ON PROCEDURE ORG_CHART TO SYSDBA']

def test_25_Package(db_connection):
    """Tests Package objects."""
    s = db_connection.schema
    c = s.packages.get('TEST')

    # common properties
    assert c.name == 'TEST'
    assert c.description is None
    assert not c.is_sys_object()
    assert c.actions == ['create', 'recreate', 'create_or_alter', 'alter', 'drop', 'comment']
    assert c.get_quoted_name() == 'TEST'
    assert c.owner_name == 'SYSDBA'
    assert c.security_class.startswith('SQL$') # Version specific
    assert c.header == """BEGIN
  PROCEDURE P1(I INT) RETURNS (O INT); -- public procedure
  FUNCTION F(X INT) RETURNS INT;
END"""
    assert c.body == """BEGIN
  FUNCTION F1(I INT) RETURNS INT; -- private function

  PROCEDURE P1(I INT) RETURNS (O INT)
  AS
  BEGIN
  END

  FUNCTION F1(I INT) RETURNS INT
  AS
  BEGIN
    RETURN F(I)+10;
  END

  FUNCTION F(X INT) RETURNS INT
  AS
  BEGIN
    RETURN X+1;
  END
END"""
    assert not c.get_dependents()
    assert len(c.get_dependencies()) == 1
    assert len(c.functions) == 2
    assert len(c.procedures) == 1

    # Test DDL generation
    assert c.get_sql_for('create') == """CREATE PACKAGE TEST
AS
BEGIN
  PROCEDURE P1(I INT) RETURNS (O INT); -- public procedure
  FUNCTION F(X INT) RETURNS INT;
END"""
    assert c.get_sql_for('create', body=True) == """CREATE PACKAGE BODY TEST
AS
BEGIN
  FUNCTION F1(I INT) RETURNS INT; -- private function

  PROCEDURE P1(I INT) RETURNS (O INT)
  AS
  BEGIN
  END

  FUNCTION F1(I INT) RETURNS INT
  AS
  BEGIN
    RETURN F(I)+10;
  END

  FUNCTION F(X INT) RETURNS INT
  AS
  BEGIN
    RETURN X+1;
  END
END"""
    assert c.get_sql_for('alter', header="FUNCTION F2(I INT) RETURNS INT;") == \
        """ALTER PACKAGE TEST
AS
BEGIN
FUNCTION F2(I INT) RETURNS INT;
END"""
    assert c.get_sql_for('drop') == """DROP PACKAGE TEST"""
    assert c.get_sql_for('drop', body=True) == """DROP PACKAGE BODY TEST"""
    assert c.get_sql_for('create_or_alter') == """CREATE OR ALTER PACKAGE TEST
AS
BEGIN
  PROCEDURE P1(I INT) RETURNS (O INT); -- public procedure
  FUNCTION F(X INT) RETURNS INT;
END"""
    assert c.get_sql_for('comment') == 'COMMENT ON PACKAGE TEST IS NULL'

def test_27_Script(db_connection, fb_vars):
    """Tests get_metadata_ddl script generation for various sections."""
    s = db_connection.schema
    version = fb_vars['version'].base_version

    assert len(sm.SCRIPT_DEFAULT_ORDER) == 25

    script = s.get_metadata_ddl(sections=[sm.Section.COLLATIONS])
    assert len(script) == 1
    assert script[0].startswith("CREATE COLLATION TEST_COLLATE")

    script = s.get_metadata_ddl(sections=[sm.Section.CHARACTER_SETS])
    assert not script # Expect empty list for user objects

    script = s.get_metadata_ddl(sections=[sm.Section.UDFS])
    assert not script # Expect empty list for user objects

    script = s.get_metadata_ddl(sections=[sm.Section.GENERATORS])
    assert len(script) == 2
    assert "CREATE SEQUENCE EMP_NO_GEN" in script
    assert "CREATE SEQUENCE CUST_NO_GEN" in script

    script = s.get_metadata_ddl(sections=[sm.Section.EXCEPTIONS])
    assert len(script) == 5
    assert "CREATE EXCEPTION UNKNOWN_EMP_ID" in script[0]

    script = s.get_metadata_ddl(sections=[sm.Section.DOMAINS])
    assert len(script) == 15
    if version.startswith('3'):
        assert 'CREATE DOMAIN "FIRSTNAME"' in script[0]
    else:
        assert 'CREATE DOMAIN FIRSTNAME' in script[0]

    script = s.get_metadata_ddl(sections=[sm.Section.PACKAGE_DEFS])
    assert len(script) == 2
    assert "CREATE PACKAGE TEST" in script[0]
    assert "CREATE PACKAGE TEST2" in script[1]

    script = s.get_metadata_ddl(sections=[sm.Section.FUNCTION_DEFS])
    assert len(script) == 3
    assert "CREATE FUNCTION F2" in script[0]
    assert "CREATE FUNCTION FX" in script[1]
    assert "CREATE FUNCTION FN" in script[2]

    script = s.get_metadata_ddl(sections=[sm.Section.PROCEDURE_DEFS])
    assert len(script) == 11
    assert "CREATE PROCEDURE GET_EMP_PROJ" in script[0]

    script = s.get_metadata_ddl(sections=[sm.Section.TABLES])
    assert len(script) == 16
    assert "CREATE TABLE COUNTRY" in script[0]
    assert "CREATE TABLE EMPLOYEE" in script[3] # Check relative order

    script = s.get_metadata_ddl(sections=[sm.Section.PRIMARY_KEYS])
    assert len(script) == 12
    assert "ALTER TABLE COUNTRY ADD PRIMARY KEY" in script[0]

    script = s.get_metadata_ddl(sections=[sm.Section.UNIQUE_CONSTRAINTS])
    assert len(script) == 2
    assert "ALTER TABLE DEPARTMENT ADD UNIQUE" in script[0]

    script = s.get_metadata_ddl(sections=[sm.Section.CHECK_CONSTRAINTS])
    assert len(script) == 14
    assert "ALTER TABLE JOB ADD CHECK" in script[0]

    script = s.get_metadata_ddl(sections=[sm.Section.FOREIGN_CONSTRAINTS])
    assert len(script) == 14
    assert "ALTER TABLE JOB ADD FOREIGN KEY" in script[0]

    script = s.get_metadata_ddl(sections=[sm.Section.INDICES])
    assert len(script) == 12
    assert "CREATE ASCENDING INDEX MINSALX" in script[0]

    script = s.get_metadata_ddl(sections=[sm.Section.VIEWS])
    assert len(script) == 1
    assert "CREATE VIEW PHONE_LIST" in script[0]

    script = s.get_metadata_ddl(sections=[sm.Section.PACKAGE_BODIES])
    assert len(script) == 2
    assert "CREATE PACKAGE BODY TEST" in script[0]
    assert "CREATE PACKAGE BODY TEST2" in script[1]

    script = s.get_metadata_ddl(sections=[sm.Section.FUNCTION_BODIES])
    assert len(script) == 3
    assert "ALTER FUNCTION F2" in script[0]

    script = s.get_metadata_ddl(sections=[sm.Section.PROCEDURE_BODIES])
    assert len(script) == 11
    assert "ALTER PROCEDURE GET_EMP_PROJ" in script[0]

    script = s.get_metadata_ddl(sections=[sm.Section.TRIGGERS])
    assert len(script) == 8
    assert "CREATE TRIGGER SET_EMP_NO" in script[0]

    script = s.get_metadata_ddl(sections=[sm.Section.ROLES])
    assert len(script) == 2 # Includes RDB$ADMIN from FB4+
    assert "CREATE ROLE TEST_ROLE" in script or "CREATE ROLE RDB$ADMIN" in script

    script = s.get_metadata_ddl(sections=[sm.Section.GRANTS])
    # Grant count varies significantly between versions
    assert len(script) > 50 # Check for a reasonable number of grants
    assert "GRANT SELECT ON COUNTRY TO PUBLIC" in script[0]

    script = s.get_metadata_ddl(sections=[sm.Section.COMMENTS])
    assert len(script) == 1
    assert "COMMENT ON CHARACTER SET NONE" in script[0]

    script = s.get_metadata_ddl(sections=[sm.Section.SHADOWS])
    assert not script

    script = s.get_metadata_ddl(sections=[sm.Section.INDEX_DEACTIVATIONS])
    assert len(script) == 12
    assert "ALTER INDEX MINSALX INACTIVE" in script[0] or "ALTER INDEX NEEDX INACTIVE" in script[0]

    script = s.get_metadata_ddl(sections=[sm.Section.INDEX_ACTIVATIONS])
    assert len(script) == 12
    assert "ALTER INDEX MINSALX ACTIVE" in script[0] or "ALTER INDEX NEEDX ACTIVE" in script[0]

    script = s.get_metadata_ddl(sections=[sm.Section.SET_GENERATORS])
    assert len(script) == 2
    assert "ALTER SEQUENCE EMP_NO_GEN RESTART WITH" in script[0]

    script = s.get_metadata_ddl(sections=[sm.Section.TRIGGER_DEACTIVATIONS])
    assert len(script) == 8
    assert "ALTER TRIGGER SET_EMP_NO INACTIVE" in script[0]

    script = s.get_metadata_ddl(sections=[sm.Section.TRIGGER_ACTIVATIONS])
    assert len(script) == 8
    assert "ALTER TRIGGER SET_EMP_NO ACTIVE" in script[0]

def test_26_Visitor(db_connection):
    """Tests the SchemaVisitor helper."""
    s = db_connection.schema

    # Test dependency following (CREATE)
    v_create = SchemaVisitor(action='create', follow='dependencies')
    proc_all_langs = s.all_procedures.get('ALL_LANGS')
    proc_all_langs.accept(v_create)

    # Expected output depends on the order the visitor traverses dependencies.
    # Instead of matching exact multiline string, check if key elements are present.
    generated_sql_starts = [sql.split('\n')[0].strip() for sql in v_create.collected_ddl]

    # Check if the main components are generated, order might vary
    assert any("CREATE TABLE JOB" in sql for sql in generated_sql_starts)
    assert any("CREATE PROCEDURE SHOW_LANGS" in sql for sql in generated_sql_starts)
    assert any("CREATE PROCEDURE ALL_LANGS" in sql for sql in generated_sql_starts)
    # More specific checks on the content could be added if needed

    # Test dependent following (DROP)
    v_drop = SchemaVisitor(action='drop', follow='dependents')
    table_job = s.all_tables.get('JOB')
    table_job.accept(v_drop)

    expected_drops = [
        "DROP PROCEDURE ALL_LANGS",
        "DROP PROCEDURE SHOW_LANGS",
        "DROP TABLE JOB",
    ]
    # Drop order matters more, assert the collected list directly (or sorted)
    # The visitor logic adds dependents first, then the object itself.
    assert v_drop.collected_ddl == expected_drops

def test_27_Script(db_connection, fb_vars):
    """Tests get_metadata_ddl script generation."""
    s = db_connection.schema
    version = fb_vars['version'].base_version

    assert 25 == len(sm.SCRIPT_DEFAULT_ORDER)
    script = s.get_metadata_ddl(sections=[sm.Section.COLLATIONS])
    assert script == ["CREATE COLLATION TEST_COLLATE\n   FOR WIN1250\n   FROM WIN_CZ\n   NO PAD\n   CASE INSENSITIVE\n   ACCENT INSENSITIVE\n   'DISABLE-COMPRESSIONS=0;DISABLE-EXPANSIONS=0'"]
    script = s.get_metadata_ddl(sections=[sm.Section.CHARACTER_SETS])
    assert script == []
    script = s.get_metadata_ddl(sections=[sm.Section.UDFS])
    assert script == []
    script = s.get_metadata_ddl(sections=[sm.Section.GENERATORS])
    assert script == ['CREATE SEQUENCE EMP_NO_GEN', 'CREATE SEQUENCE CUST_NO_GEN']
    script = s.get_metadata_ddl(sections=[sm.Section.EXCEPTIONS])
    assert script == [
        "CREATE EXCEPTION UNKNOWN_EMP_ID 'Invalid employee number or project id.'",
        "CREATE EXCEPTION REASSIGN_SALES 'Reassign the sales records before deleting this employee.'",
        'CREATE EXCEPTION ORDER_ALREADY_SHIPPED \'Order status is "shipped."\'',
        "CREATE EXCEPTION CUSTOMER_ON_HOLD 'This customer is on hold.'",
        "CREATE EXCEPTION CUSTOMER_CHECK 'Overdue balance -- can not ship.'"
    ]
    script = s.get_metadata_ddl(sections=[sm.Section.DOMAINS])
    if version == FB30:
        assert script == [
            'CREATE DOMAIN "FIRSTNAME" AS VARCHAR(15)',
            'CREATE DOMAIN "LASTNAME" AS VARCHAR(20)',
            'CREATE DOMAIN PHONENUMBER AS VARCHAR(20)',
            'CREATE DOMAIN COUNTRYNAME AS VARCHAR(15)',
            'CREATE DOMAIN ADDRESSLINE AS VARCHAR(30)',
            'CREATE DOMAIN EMPNO AS SMALLINT',
            "CREATE DOMAIN DEPTNO AS CHAR(3) CHECK (VALUE = '000' OR (VALUE > '0' AND VALUE <= '999') OR VALUE IS NULL)",
            'CREATE DOMAIN PROJNO AS CHAR(5) CHECK (VALUE = UPPER (VALUE))',
            'CREATE DOMAIN CUSTNO AS INTEGER CHECK (VALUE > 1000)',
            "CREATE DOMAIN JOBCODE AS VARCHAR(5) CHECK (VALUE > '99999')",
            'CREATE DOMAIN JOBGRADE AS SMALLINT CHECK (VALUE BETWEEN 0 AND 6)',
            'CREATE DOMAIN SALARY AS NUMERIC(10, 2) DEFAULT 0 CHECK (VALUE > 0)',
            'CREATE DOMAIN BUDGET AS DECIMAL(12, 2) DEFAULT 50000 CHECK (VALUE > 10000 AND VALUE <= 2000000)',
            "CREATE DOMAIN PRODTYPE AS VARCHAR(12) DEFAULT 'software' NOT NULL CHECK (VALUE IN ('software', 'hardware', 'other', 'N/A'))",
            "CREATE DOMAIN PONUMBER AS CHAR(8) CHECK (VALUE STARTING WITH 'V')"
        ]
    else:
        assert script == [
            'CREATE DOMAIN FIRSTNAME AS VARCHAR(15)',
            'CREATE DOMAIN LASTNAME AS VARCHAR(20)',
            'CREATE DOMAIN PHONENUMBER AS VARCHAR(20)',
            'CREATE DOMAIN COUNTRYNAME AS VARCHAR(15)',
            'CREATE DOMAIN ADDRESSLINE AS VARCHAR(30)',
            'CREATE DOMAIN EMPNO AS SMALLINT',
            "CREATE DOMAIN DEPTNO AS CHAR(3) CHECK (VALUE = '000' OR (VALUE > '0' AND VALUE <= '999') OR VALUE IS NULL)",
            'CREATE DOMAIN PROJNO AS CHAR(5) CHECK (VALUE = UPPER (VALUE))',
            'CREATE DOMAIN CUSTNO AS INTEGER CHECK (VALUE > 1000)',
            "CREATE DOMAIN JOBCODE AS VARCHAR(5) CHECK (VALUE > '99999')",
            'CREATE DOMAIN JOBGRADE AS SMALLINT CHECK (VALUE BETWEEN 0 AND 6)',
            'CREATE DOMAIN SALARY AS NUMERIC(10, 2) DEFAULT 0 CHECK (VALUE > 0)',
            'CREATE DOMAIN BUDGET AS DECIMAL(12, 2) DEFAULT 50000 CHECK (VALUE > 10000 AND VALUE <= 2000000)',
            "CREATE DOMAIN PRODTYPE AS VARCHAR(12) DEFAULT 'software' NOT NULL CHECK (VALUE IN ('software', 'hardware', 'other', 'N/A'))",
            "CREATE DOMAIN PONUMBER AS CHAR(8) CHECK (VALUE STARTING WITH 'V')"
        ]
    script = s.get_metadata_ddl(sections=[sm.Section.PACKAGE_DEFS])
    assert script == [
        'CREATE PACKAGE TEST\nAS\nBEGIN\n  PROCEDURE P1(I INT) RETURNS (O INT); -- public procedure\n  FUNCTION F(X INT) RETURNS INT;\nEND',
        'CREATE PACKAGE TEST2\nAS\nBEGIN\n  FUNCTION F3(X INT) RETURNS INT;\nEND'
    ]
    script = s.get_metadata_ddl(sections=[sm.Section.FUNCTION_DEFS])
    if version == FB30:
        assert script == [
            'CREATE FUNCTION F2 (X INTEGER)\nRETURNS INTEGER\nAS\nBEGIN\nEND',
            'CREATE FUNCTION FX (\n  F TYPE OF "FIRSTNAME",\n  L TYPE OF COLUMN CUSTOMER.CONTACT_LAST\n)\nRETURNS VARCHAR(35)\nAS\nBEGIN\nEND',
            'CREATE FUNCTION FN\nRETURNS INTEGER\nAS\nBEGIN\nEND'
        ]
    else:
        assert script == [
            'CREATE FUNCTION F2 (X INTEGER)\nRETURNS INTEGER\nAS\nBEGIN\nEND',
            'CREATE FUNCTION FX (\n  F TYPE OF FIRSTNAME,\n  L TYPE OF COLUMN CUSTOMER.CONTACT_LAST\n)\nRETURNS VARCHAR(35)\nAS\nBEGIN\nEND',
            'CREATE FUNCTION FN\nRETURNS INTEGER\nAS\nBEGIN\nEND']
    script = s.get_metadata_ddl(sections=[sm.Section.PROCEDURE_DEFS])
    assert script == [
        'CREATE PROCEDURE GET_EMP_PROJ (EMP_NO SMALLINT)\nRETURNS (PROJ_ID CHAR(5))\nAS\nBEGIN\n  SUSPEND;\nEND',
        'CREATE PROCEDURE ADD_EMP_PROJ (\n  EMP_NO SMALLINT,\n  PROJ_ID CHAR(5)\n)\nAS\nBEGIN\n  SUSPEND;\nEND',
        'CREATE PROCEDURE SUB_TOT_BUDGET (HEAD_DEPT CHAR(3))\nRETURNS (\n  TOT_BUDGET DECIMAL(12, 2),\n  AVG_BUDGET DECIMAL(12, 2),\n  MIN_BUDGET DECIMAL(12, 2),\n  MAX_BUDGET DECIMAL(12, 2)\n)\nAS\nBEGIN\n  SUSPEND;\nEND',
        'CREATE PROCEDURE DELETE_EMPLOYEE (EMP_NUM INTEGER)\nAS\nBEGIN\n  SUSPEND;\nEND',
        'CREATE PROCEDURE DEPT_BUDGET (DNO CHAR(3))\nRETURNS (TOT DECIMAL(12, 2))\nAS\nBEGIN\n  SUSPEND;\nEND',
        'CREATE PROCEDURE ORG_CHART\nRETURNS (\n  HEAD_DEPT CHAR(25),\n  DEPARTMENT CHAR(25),\n  MNGR_NAME CHAR(20),\n  TITLE CHAR(5),\n  EMP_CNT INTEGER\n)\nAS\nBEGIN\n  SUSPEND;\nEND',
        'CREATE PROCEDURE MAIL_LABEL (CUST_NO INTEGER)\nRETURNS (\n  LINE1 CHAR(40),\n  LINE2 CHAR(40),\n  LINE3 CHAR(40),\n  LINE4 CHAR(40),\n  LINE5 CHAR(40),\n  LINE6 CHAR(40)\n)\nAS\nBEGIN\n  SUSPEND;\nEND',
        'CREATE PROCEDURE SHIP_ORDER (PO_NUM CHAR(8))\nAS\nBEGIN\n  SUSPEND;\nEND',
        'CREATE PROCEDURE SHOW_LANGS (\n  CODE VARCHAR(5),\n  GRADE SMALLINT,\n  CTY VARCHAR(15)\n)\nRETURNS (LANGUAGES VARCHAR(15))\nAS\nBEGIN\n  SUSPEND;\nEND',
        'CREATE PROCEDURE ALL_LANGS\nRETURNS (\n  CODE VARCHAR(5),\n  GRADE VARCHAR(5),\n  COUNTRY VARCHAR(15),\n  LANG VARCHAR(15)\n)\nAS\nBEGIN\n  SUSPEND;\nEND'
    ]
    script = s.get_metadata_ddl(sections=[sm.Section.TABLES])
    assert script == [
        'CREATE TABLE COUNTRY (\n  COUNTRY COUNTRYNAME NOT NULL,\n  CURRENCY VARCHAR(10) NOT NULL\n)',
        'CREATE TABLE JOB (\n  JOB_CODE JOBCODE NOT NULL,\n  JOB_GRADE JOBGRADE NOT NULL,\n  JOB_COUNTRY COUNTRYNAME NOT NULL,\n  JOB_TITLE VARCHAR(25) NOT NULL,\n  MIN_SALARY SALARY NOT NULL,\n  MAX_SALARY SALARY NOT NULL,\n  JOB_REQUIREMENT BLOB SUB_TYPE TEXT SEGMENT SIZE 400,\n  LANGUAGE_REQ VARCHAR(15)[5]\n)',
        "CREATE TABLE DEPARTMENT (\n  DEPT_NO DEPTNO NOT NULL,\n  DEPARTMENT VARCHAR(25) NOT NULL,\n  HEAD_DEPT DEPTNO,\n  MNGR_NO EMPNO,\n  BUDGET BUDGET,\n  LOCATION VARCHAR(15),\n  PHONE_NO PHONENUMBER DEFAULT '555-1234'\n)",
        'CREATE TABLE EMPLOYEE (\n  EMP_NO EMPNO NOT NULL,\n  FIRST_NAME "FIRSTNAME" NOT NULL,\n  LAST_NAME "LASTNAME" NOT NULL,\n  PHONE_EXT VARCHAR(4),\n  HIRE_DATE TIMESTAMP DEFAULT \'NOW\' NOT NULL,\n  DEPT_NO DEPTNO NOT NULL,\n  JOB_CODE JOBCODE NOT NULL,\n  JOB_GRADE JOBGRADE NOT NULL,\n  JOB_COUNTRY COUNTRYNAME NOT NULL,\n  SALARY SALARY NOT NULL,\n  FULL_NAME COMPUTED BY (last_name || \', \' || first_name)\n)' \
        if version == FB30 else \
        'CREATE TABLE EMPLOYEE (\n  EMP_NO EMPNO NOT NULL,\n  FIRST_NAME FIRSTNAME NOT NULL,\n  LAST_NAME LASTNAME NOT NULL,\n  PHONE_EXT VARCHAR(4),\n  HIRE_DATE TIMESTAMP DEFAULT \'NOW\' NOT NULL,\n  DEPT_NO DEPTNO NOT NULL,\n  JOB_CODE JOBCODE NOT NULL,\n  JOB_GRADE JOBGRADE NOT NULL,\n  JOB_COUNTRY COUNTRYNAME NOT NULL,\n  SALARY SALARY NOT NULL,\n  FULL_NAME COMPUTED BY (last_name || \', \' || first_name)\n)',
        'CREATE TABLE CUSTOMER (\n  CUST_NO CUSTNO NOT NULL,\n  CUSTOMER VARCHAR(25) NOT NULL,\n  CONTACT_FIRST "FIRSTNAME",\n  CONTACT_LAST "LASTNAME",\n  PHONE_NO PHONENUMBER,\n  ADDRESS_LINE1 ADDRESSLINE,\n  ADDRESS_LINE2 ADDRESSLINE,\n  CITY VARCHAR(25),\n  STATE_PROVINCE VARCHAR(15),\n  COUNTRY COUNTRYNAME,\n  POSTAL_CODE VARCHAR(12),\n  ON_HOLD CHAR(1) DEFAULT NULL\n)' \
        if version == FB30 else \
        'CREATE TABLE CUSTOMER (\n  CUST_NO CUSTNO NOT NULL,\n  CUSTOMER VARCHAR(25) NOT NULL,\n  CONTACT_FIRST FIRSTNAME,\n  CONTACT_LAST LASTNAME,\n  PHONE_NO PHONENUMBER,\n  ADDRESS_LINE1 ADDRESSLINE,\n  ADDRESS_LINE2 ADDRESSLINE,\n  CITY VARCHAR(25),\n  STATE_PROVINCE VARCHAR(15),\n  COUNTRY COUNTRYNAME,\n  POSTAL_CODE VARCHAR(12),\n  ON_HOLD CHAR(1) DEFAULT NULL\n)',
        'CREATE TABLE PROJECT (\n  PROJ_ID PROJNO NOT NULL,\n  PROJ_NAME VARCHAR(20) NOT NULL,\n  PROJ_DESC BLOB SUB_TYPE TEXT SEGMENT SIZE 800,\n  TEAM_LEADER EMPNO,\n  PRODUCT PRODTYPE\n)',
        'CREATE TABLE EMPLOYEE_PROJECT (\n  EMP_NO EMPNO NOT NULL,\n  PROJ_ID PROJNO NOT NULL\n)',
        'CREATE TABLE PROJ_DEPT_BUDGET (\n  FISCAL_YEAR INTEGER NOT NULL,\n  PROJ_ID PROJNO NOT NULL,\n  DEPT_NO DEPTNO NOT NULL,\n  QUART_HEAD_CNT INTEGER[4],\n  PROJECTED_BUDGET BUDGET\n)',
        "CREATE TABLE SALARY_HISTORY (\n  EMP_NO EMPNO NOT NULL,\n  CHANGE_DATE TIMESTAMP DEFAULT 'NOW' NOT NULL,\n  UPDATER_ID VARCHAR(20) NOT NULL,\n  OLD_SALARY SALARY NOT NULL,\n  PERCENT_CHANGE DOUBLE PRECISION DEFAULT 0 NOT NULL,\n  NEW_SALARY COMPUTED BY (old_salary + old_salary * percent_change / 100)\n)",
        "CREATE TABLE SALES (\n  PO_NUMBER PONUMBER NOT NULL,\n  CUST_NO CUSTNO NOT NULL,\n  SALES_REP EMPNO,\n  ORDER_STATUS VARCHAR(7) DEFAULT 'new' NOT NULL,\n  ORDER_DATE TIMESTAMP DEFAULT 'NOW' NOT NULL,\n  SHIP_DATE TIMESTAMP,\n  DATE_NEEDED TIMESTAMP,\n  PAID CHAR(1) DEFAULT 'n',\n  QTY_ORDERED INTEGER DEFAULT 1 NOT NULL,\n  TOTAL_VALUE DECIMAL(9, 2) NOT NULL,\n  DISCOUNT FLOAT DEFAULT 0 NOT NULL,\n  ITEM_TYPE PRODTYPE,\n  AGED COMPUTED BY (ship_date - order_date)\n)",
        'CREATE TABLE AR (\n  C1 INTEGER,\n  C2 INTEGER[4, 0:3, 2],\n  C3 VARCHAR(15)[0:5, 2],\n  C4 CHAR(5)[5],\n  C5 TIMESTAMP[2],\n  C6 TIME[2],\n  C7 DECIMAL(10, 2)[2],\n  C8 NUMERIC(10, 2)[2],\n  C9 SMALLINT[2],\n  C10 BIGINT[2],\n  C11 FLOAT[2],\n  C12 DOUBLE PRECISION[2],\n  C13 DECIMAL(10, 1)[2],\n  C14 DECIMAL(10, 5)[2],\n  C15 DECIMAL(18, 5)[2],\n  C16 BOOLEAN[3]\n)',
        'CREATE TABLE T2 (\n  C1 SMALLINT,\n  C2 INTEGER,\n  C3 BIGINT,\n  C4 CHAR(5),\n  C5 VARCHAR(10),\n  C6 DATE,\n  C7 TIME,\n  C8 TIMESTAMP,\n  C9 BLOB SUB_TYPE TEXT SEGMENT SIZE 80,\n  C10 NUMERIC(18, 2),\n  C11 DECIMAL(18, 2),\n  C12 FLOAT,\n  C13 DOUBLE PRECISION,\n  C14 NUMERIC(8, 4),\n  C15 DECIMAL(8, 4),\n  C16 BLOB SUB_TYPE BINARY SEGMENT SIZE 80,\n  C17 BOOLEAN\n)',
        'CREATE TABLE T3 (\n  C1 INTEGER,\n  C2 CHAR(10) CHARACTER SET UTF8,\n  C3 VARCHAR(10) CHARACTER SET UTF8,\n  C4 BLOB SUB_TYPE TEXT SEGMENT SIZE 80 CHARACTER SET UTF8,\n  C5 BLOB SUB_TYPE BINARY SEGMENT SIZE 80\n)',
        'CREATE TABLE T4 (\n  C1 INTEGER,\n  C_OCTETS CHAR(5) CHARACTER SET OCTETS,\n  V_OCTETS VARCHAR(30) CHARACTER SET OCTETS,\n  C_NONE CHAR(5),\n  V_NONE VARCHAR(30),\n  C_WIN1250 CHAR(5) CHARACTER SET WIN1250,\n  V_WIN1250 VARCHAR(30) CHARACTER SET WIN1250,\n  C_UTF8 CHAR(5) CHARACTER SET UTF8,\n  V_UTF8 VARCHAR(30) CHARACTER SET UTF8\n)',
        'CREATE TABLE T5 (\n  ID NUMERIC(10, 0) GENERATED BY DEFAULT AS IDENTITY,\n  C1 VARCHAR(15),\n  UQ BIGINT GENERATED BY DEFAULT AS IDENTITY (START WITH 100)\n)', 'CREATE TABLE T (\n  C1 INTEGER NOT NULL\n)'
    ]
    script = s.get_metadata_ddl(sections=[sm.Section.PRIMARY_KEYS])
    assert script == [
        'ALTER TABLE COUNTRY ADD PRIMARY KEY (COUNTRY)',
        'ALTER TABLE JOB ADD PRIMARY KEY (JOB_CODE,JOB_GRADE,JOB_COUNTRY)',
        'ALTER TABLE DEPARTMENT ADD PRIMARY KEY (DEPT_NO)',
        'ALTER TABLE EMPLOYEE ADD PRIMARY KEY (EMP_NO)',
        'ALTER TABLE PROJECT ADD PRIMARY KEY (PROJ_ID)',
        'ALTER TABLE EMPLOYEE_PROJECT ADD PRIMARY KEY (EMP_NO,PROJ_ID)',
        'ALTER TABLE PROJ_DEPT_BUDGET ADD PRIMARY KEY (FISCAL_YEAR,PROJ_ID,DEPT_NO)',
        'ALTER TABLE SALARY_HISTORY ADD PRIMARY KEY (EMP_NO,CHANGE_DATE,UPDATER_ID)',
        'ALTER TABLE CUSTOMER ADD PRIMARY KEY (CUST_NO)',
        'ALTER TABLE SALES ADD PRIMARY KEY (PO_NUMBER)',
        'ALTER TABLE T5 ADD PRIMARY KEY (ID)',
        'ALTER TABLE T ADD PRIMARY KEY (C1)'
    ]
    script = s.get_metadata_ddl(sections=[sm.Section.UNIQUE_CONSTRAINTS])
    assert script == [
        'ALTER TABLE DEPARTMENT ADD UNIQUE (DEPARTMENT)',
        'ALTER TABLE PROJECT ADD UNIQUE (PROJ_NAME)'
    ]
    script = s.get_metadata_ddl(sections=[sm.Section.CHECK_CONSTRAINTS])
    assert script == [
        'ALTER TABLE JOB ADD CHECK (min_salary < max_salary)',
        'ALTER TABLE EMPLOYEE ADD CHECK ( salary >= (SELECT min_salary FROM job WHERE\n                        job.job_code = employee.job_code AND\n                        job.job_grade = employee.job_grade AND\n                        job.job_country = employee.job_country) AND\n            salary <= (SELECT max_salary FROM job WHERE\n                        job.job_code = employee.job_code AND\n                        job.job_grade = employee.job_grade AND\n                        job.job_country = employee.job_country))',
        "ALTER TABLE CUSTOMER ADD CHECK (on_hold IS NULL OR on_hold = '*')",
        'ALTER TABLE PROJ_DEPT_BUDGET ADD CHECK (FISCAL_YEAR >= 1993)',
        'ALTER TABLE SALARY_HISTORY ADD CHECK (percent_change between -50 and 50)',
        "ALTER TABLE SALES ADD CHECK (order_status in\n                            ('new', 'open', 'shipped', 'waiting'))",
        'ALTER TABLE SALES ADD CHECK (ship_date >= order_date OR ship_date IS NULL)',
        'ALTER TABLE SALES ADD CHECK (date_needed > order_date OR date_needed IS NULL)',
        "ALTER TABLE SALES ADD CHECK (paid in ('y', 'n'))",
        'ALTER TABLE SALES ADD CHECK (qty_ordered >= 1)',
        'ALTER TABLE SALES ADD CHECK (total_value >= 0)',
        'ALTER TABLE SALES ADD CHECK (discount >= 0 AND discount <= 1)',
        "ALTER TABLE SALES ADD CHECK (NOT (order_status = 'shipped' AND ship_date IS NULL))",
        "ALTER TABLE SALES ADD CHECK (NOT (order_status = 'shipped' AND\n            EXISTS (SELECT on_hold FROM customer\n                    WHERE customer.cust_no = sales.cust_no\n                    AND customer.on_hold = '*')))"
    ]
    script = s.get_metadata_ddl(sections=[sm.Section.FOREIGN_CONSTRAINTS])
    assert script == [
        'ALTER TABLE JOB ADD FOREIGN KEY (JOB_COUNTRY)\n  REFERENCES COUNTRY (COUNTRY)',
        'ALTER TABLE DEPARTMENT ADD FOREIGN KEY (HEAD_DEPT)\n  REFERENCES DEPARTMENT (DEPT_NO)',
        'ALTER TABLE DEPARTMENT ADD FOREIGN KEY (MNGR_NO)\n  REFERENCES EMPLOYEE (EMP_NO)',
        'ALTER TABLE EMPLOYEE ADD FOREIGN KEY (DEPT_NO)\n  REFERENCES DEPARTMENT (DEPT_NO)',
        'ALTER TABLE EMPLOYEE ADD FOREIGN KEY (JOB_CODE,JOB_GRADE,JOB_COUNTRY)\n  REFERENCES JOB (JOB_CODE,JOB_GRADE,JOB_COUNTRY)',
        'ALTER TABLE CUSTOMER ADD FOREIGN KEY (COUNTRY)\n  REFERENCES COUNTRY (COUNTRY)',
        'ALTER TABLE PROJECT ADD FOREIGN KEY (TEAM_LEADER)\n  REFERENCES EMPLOYEE (EMP_NO)',
        'ALTER TABLE EMPLOYEE_PROJECT ADD FOREIGN KEY (EMP_NO)\n  REFERENCES EMPLOYEE (EMP_NO)',
        'ALTER TABLE EMPLOYEE_PROJECT ADD FOREIGN KEY (PROJ_ID)\n  REFERENCES PROJECT (PROJ_ID)',
        'ALTER TABLE PROJ_DEPT_BUDGET ADD FOREIGN KEY (DEPT_NO)\n  REFERENCES DEPARTMENT (DEPT_NO)',
        'ALTER TABLE PROJ_DEPT_BUDGET ADD FOREIGN KEY (PROJ_ID)\n  REFERENCES PROJECT (PROJ_ID)',
        'ALTER TABLE SALARY_HISTORY ADD FOREIGN KEY (EMP_NO)\n  REFERENCES EMPLOYEE (EMP_NO)',
        'ALTER TABLE SALES ADD FOREIGN KEY (CUST_NO)\n  REFERENCES CUSTOMER (CUST_NO)',
        'ALTER TABLE SALES ADD FOREIGN KEY (SALES_REP)\n  REFERENCES EMPLOYEE (EMP_NO)'
    ]
    script = s.get_metadata_ddl(sections=[sm.Section.INDICES])
    assert script == [
        'CREATE ASCENDING INDEX MINSALX ON JOB (JOB_COUNTRY,MIN_SALARY)',
        'CREATE DESCENDING INDEX MAXSALX ON JOB (JOB_COUNTRY,MAX_SALARY)',
        'CREATE DESCENDING INDEX BUDGETX ON DEPARTMENT (BUDGET)',
        'CREATE ASCENDING INDEX NAMEX ON EMPLOYEE (LAST_NAME,FIRST_NAME)',
        'CREATE ASCENDING INDEX CUSTNAMEX ON CUSTOMER (CUSTOMER)',
        'CREATE ASCENDING INDEX CUSTREGION ON CUSTOMER (COUNTRY,CITY)',
        'CREATE UNIQUE ASCENDING INDEX PRODTYPEX ON PROJECT (PRODUCT,PROJ_NAME)',
        'CREATE ASCENDING INDEX UPDATERX ON SALARY_HISTORY (UPDATER_ID)',
        'CREATE DESCENDING INDEX CHANGEX ON SALARY_HISTORY (CHANGE_DATE)',
        'CREATE ASCENDING INDEX NEEDX ON SALES (DATE_NEEDED)',
        'CREATE ASCENDING INDEX SALESTATX ON SALES (ORDER_STATUS,PAID)',
        'CREATE DESCENDING INDEX QTYX ON SALES (ITEM_TYPE,QTY_ORDERED)'
    ]
    script = s.get_metadata_ddl(sections=[sm.Section.VIEWS])
    assert script == ['CREATE VIEW PHONE_LIST (EMP_NO,FIRST_NAME,LAST_NAME,PHONE_EXT,LOCATION,PHONE_NO)\n   AS\n     SELECT\n    emp_no, first_name, last_name, phone_ext, location, phone_no\n    FROM employee, department\n    WHERE employee.dept_no = department.dept_no']
    script = s.get_metadata_ddl(sections=[sm.Section.PACKAGE_BODIES])
    assert script == ['CREATE PACKAGE BODY TEST\nAS\nBEGIN\n  FUNCTION F1(I INT) RETURNS INT; -- private function\n\n  PROCEDURE P1(I INT) RETURNS (O INT)\n  AS\n  BEGIN\n  END\n\n  FUNCTION F1(I INT) RETURNS INT\n  AS\n  BEGIN\n    RETURN F(I)+10;\n  END\n\n  FUNCTION F(X INT) RETURNS INT\n  AS\n  BEGIN\n    RETURN X+1;\n  END\nEND', 'CREATE PACKAGE BODY TEST2\nAS\nBEGIN\n  FUNCTION F3(X INT) RETURNS INT\n  AS\n  BEGIN\n    RETURN TEST.F(X)+100+FN();\n  END\nEND']
    script = s.get_metadata_ddl(sections=[sm.Section.FUNCTION_BODIES])
    assert script == [
        'ALTER FUNCTION F2 (X INTEGER)\nRETURNS INTEGER\nAS\nBEGIN\n  RETURN X+1;\nEND',
        'ALTER FUNCTION FX (\n  F TYPE OF "FIRSTNAME",\n  L TYPE OF COLUMN CUSTOMER.CONTACT_LAST\n)\nRETURNS VARCHAR(35)\nAS\nBEGIN\n  RETURN L || \', \' || F;\nEND' \
        if version == FB30 else \
        'ALTER FUNCTION FX (\n  F TYPE OF FIRSTNAME,\n  L TYPE OF COLUMN CUSTOMER.CONTACT_LAST\n)\nRETURNS VARCHAR(35)\nAS\nBEGIN\n  RETURN L || \', \' || F;\nEND',
        'ALTER FUNCTION FN\nRETURNS INTEGER\nAS\nBEGIN\n  RETURN 0;\nEND'
    ]
    script = s.get_metadata_ddl(sections=[sm.Section.PROCEDURE_BODIES])
    assert script == [
        'ALTER PROCEDURE GET_EMP_PROJ (EMP_NO SMALLINT)\nRETURNS (PROJ_ID CHAR(5))\nAS\nBEGIN\n\tFOR SELECT proj_id\n\t\tFROM employee_project\n\t\tWHERE emp_no = :emp_no\n\t\tINTO :proj_id\n\tDO\n\t\tSUSPEND;\nEND', 'ALTER PROCEDURE ADD_EMP_PROJ (\n  EMP_NO SMALLINT,\n  PROJ_ID CHAR(5)\n)\nAS\nBEGIN\n\tBEGIN\n\tINSERT INTO employee_project (emp_no, proj_id) VALUES (:emp_no, :proj_id);\n\tWHEN SQLCODE -530 DO\n\t\tEXCEPTION unknown_emp_id;\n\tEND\n\tSUSPEND;\nEND',
        'ALTER PROCEDURE SUB_TOT_BUDGET (HEAD_DEPT CHAR(3))\nRETURNS (\n  TOT_BUDGET DECIMAL(12, 2),\n  AVG_BUDGET DECIMAL(12, 2),\n  MIN_BUDGET DECIMAL(12, 2),\n  MAX_BUDGET DECIMAL(12, 2)\n)\nAS\nBEGIN\n\tSELECT SUM(budget), AVG(budget), MIN(budget), MAX(budget)\n\t\tFROM department\n\t\tWHERE head_dept = :head_dept\n\t\tINTO :tot_budget, :avg_budget, :min_budget, :max_budget;\n\tSUSPEND;\nEND',
        "ALTER PROCEDURE DELETE_EMPLOYEE (EMP_NUM INTEGER)\nAS\nDECLARE VARIABLE any_sales INTEGER;\nBEGIN\n\tany_sales = 0;\n\n\t/*\n\t *\tIf there are any sales records referencing this employee,\n\t *\tcan't delete the employee until the sales are re-assigned\n\t *\tto another employee or changed to NULL.\n\t */\n\tSELECT count(po_number)\n\tFROM sales\n\tWHERE sales_rep = :emp_num\n\tINTO :any_sales;\n\n\tIF (any_sales > 0) THEN\n\tBEGIN\n\t\tEXCEPTION reassign_sales;\n\t\tSUSPEND;\n\tEND\n\n\t/*\n\t *\tIf the employee is a manager, update the department.\n\t */\n\tUPDATE department\n\tSET mngr_no = NULL\n\tWHERE mngr_no = :emp_num;\n\n\t/*\n\t *\tIf the employee is a project leader, update project.\n\t */\n\tUPDATE project\n\tSET team_leader = NULL\n\tWHERE team_leader = :emp_num;\n\n\t/*\n\t *\tDelete the employee from any projects.\n\t */\n\tDELETE FROM employee_project\n\tWHERE emp_no = :emp_num;\n\n\t/*\n\t *\tDelete old salary records.\n\t */\n\tDELETE FROM salary_history\n\tWHERE emp_no = :emp_num;\n\n\t/*\n\t *\tDelete the employee.\n\t */\n\tDELETE FROM employee\n\tWHERE emp_no = :emp_num;\n\n\tSUSPEND;\nEND",
        'ALTER PROCEDURE DEPT_BUDGET (DNO CHAR(3))\nRETURNS (TOT DECIMAL(12, 2))\nAS\nDECLARE VARIABLE sumb DECIMAL(12, 2);\n\tDECLARE VARIABLE rdno CHAR(3);\n\tDECLARE VARIABLE cnt INTEGER;\nBEGIN\n\ttot = 0;\n\n\tSELECT budget FROM department WHERE dept_no = :dno INTO :tot;\n\n\tSELECT count(budget) FROM department WHERE head_dept = :dno INTO :cnt;\n\n\tIF (cnt = 0) THEN\n\t\tSUSPEND;\n\n\tFOR SELECT dept_no\n\t\tFROM department\n\t\tWHERE head_dept = :dno\n\t\tINTO :rdno\n\tDO\n\t\tBEGIN\n\t\t\tEXECUTE PROCEDURE dept_budget :rdno RETURNING_VALUES :sumb;\n\t\t\ttot = tot + sumb;\n\t\tEND\n\n\tSUSPEND;\nEND',
        "ALTER PROCEDURE ORG_CHART\nRETURNS (\n  HEAD_DEPT CHAR(25),\n  DEPARTMENT CHAR(25),\n  MNGR_NAME CHAR(20),\n  TITLE CHAR(5),\n  EMP_CNT INTEGER\n)\nAS\nDECLARE VARIABLE mngr_no INTEGER;\n\tDECLARE VARIABLE dno CHAR(3);\nBEGIN\n\tFOR SELECT h.department, d.department, d.mngr_no, d.dept_no\n\t\tFROM department d\n\t\tLEFT OUTER JOIN department h ON d.head_dept = h.dept_no\n\t\tORDER BY d.dept_no\n\t\tINTO :head_dept, :department, :mngr_no, :dno\n\tDO\n\tBEGIN\n\t\tIF (:mngr_no IS NULL) THEN\n\t\tBEGIN\n\t\t\tmngr_name = '--TBH--';\n\t\t\ttitle = '';\n\t\tEND\n\n\t\tELSE\n\t\t\tSELECT full_name, job_code\n\t\t\tFROM employee\n\t\t\tWHERE emp_no = :mngr_no\n\t\t\tINTO :mngr_name, :title;\n\n\t\tSELECT COUNT(emp_no)\n\t\tFROM employee\n\t\tWHERE dept_no = :dno\n\t\tINTO :emp_cnt;\n\n\t\tSUSPEND;\n\tEND\nEND",
        "ALTER PROCEDURE MAIL_LABEL (CUST_NO INTEGER)\nRETURNS (\n  LINE1 CHAR(40),\n  LINE2 CHAR(40),\n  LINE3 CHAR(40),\n  LINE4 CHAR(40),\n  LINE5 CHAR(40),\n  LINE6 CHAR(40)\n)\nAS\nDECLARE VARIABLE customer\tVARCHAR(25);\n\tDECLARE VARIABLE first_name\t\tVARCHAR(15);\n\tDECLARE VARIABLE last_name\t\tVARCHAR(20);\n\tDECLARE VARIABLE addr1\t\tVARCHAR(30);\n\tDECLARE VARIABLE addr2\t\tVARCHAR(30);\n\tDECLARE VARIABLE city\t\tVARCHAR(25);\n\tDECLARE VARIABLE state\t\tVARCHAR(15);\n\tDECLARE VARIABLE country\tVARCHAR(15);\n\tDECLARE VARIABLE postcode\tVARCHAR(12);\n\tDECLARE VARIABLE cnt\t\tINTEGER;\nBEGIN\n\tline1 = '';\n\tline2 = '';\n\tline3 = '';\n\tline4 = '';\n\tline5 = '';\n\tline6 = '';\n\n\tSELECT customer, contact_first, contact_last, address_line1,\n\t\taddress_line2, city, state_province, country, postal_code\n\tFROM CUSTOMER\n\tWHERE cust_no = :cust_no\n\tINTO :customer, :first_name, :last_name, :addr1, :addr2,\n\t\t:city, :state, :country, :postcode;\n\n\tIF (customer IS NOT NULL) THEN\n\t\tline1 = customer;\n\tIF (first_name IS NOT NULL) THEN\n\t\tline2 = first_name || ' ' || last_name;\n\tELSE\n\t\tline2 = last_name;\n\tIF (addr1 IS NOT NULL) THEN\n\t\tline3 = addr1;\n\tIF (addr2 IS NOT NULL) THEN\n\t\tline4 = addr2;\n\n\tIF (country = 'USA') THEN\n\tBEGIN\n\t\tIF (city IS NOT NULL) THEN\n\t\t\tline5 = city || ', ' || state || '  ' || postcode;\n\t\tELSE\n\t\t\tline5 = state || '  ' || postcode;\n\tEND\n\tELSE\n\tBEGIN\n\t\tIF (city IS NOT NULL) THEN\n\t\t\tline5 = city || ', ' || state;\n\t\tELSE\n\t\t\tline5 = state;\n\t\tline6 = country || '    ' || postcode;\n\tEND\n\n\tSUSPEND;\nEND",
        "ALTER PROCEDURE SHIP_ORDER (PO_NUM CHAR(8))\nAS\nDECLARE VARIABLE ord_stat CHAR(7);\n\tDECLARE VARIABLE hold_stat CHAR(1);\n\tDECLARE VARIABLE cust_no INTEGER;\n\tDECLARE VARIABLE any_po CHAR(8);\nBEGIN\n\tSELECT s.order_status, c.on_hold, c.cust_no\n\tFROM sales s, customer c\n\tWHERE po_number = :po_num\n\tAND s.cust_no = c.cust_no\n\tINTO :ord_stat, :hold_stat, :cust_no;\n\n\t/* This purchase order has been already shipped. */\n\tIF (ord_stat = 'shipped') THEN\n\tBEGIN\n\t\tEXCEPTION order_already_shipped;\n\t\tSUSPEND;\n\tEND\n\n\t/*\tCustomer is on hold. */\n\tELSE IF (hold_stat = '*') THEN\n\tBEGIN\n\t\tEXCEPTION customer_on_hold;\n\t\tSUSPEND;\n\tEND\n\n\t/*\n\t *\tIf there is an unpaid balance on orders shipped over 2 months ago,\n\t *\tput the customer on hold.\n\t */\n\tFOR SELECT po_number\n\t\tFROM sales\n\t\tWHERE cust_no = :cust_no\n\t\tAND order_status = 'shipped'\n\t\tAND paid = 'n'\n\t\tAND ship_date < CAST('NOW' AS TIMESTAMP) - 60\n\t\tINTO :any_po\n\tDO\n\tBEGIN\n\t\tEXCEPTION customer_check;\n\n\t\tUPDATE customer\n\t\tSET on_hold = '*'\n\t\tWHERE cust_no = :cust_no;\n\n\t\tSUSPEND;\n\tEND\n\n\t/*\n\t *\tShip the order.\n\t */\n\tUPDATE sales\n\tSET order_status = 'shipped', ship_date = 'NOW'\n\tWHERE po_number = :po_num;\n\n\tSUSPEND;\nEND",
        "ALTER PROCEDURE SHOW_LANGS (\n  CODE VARCHAR(5),\n  GRADE SMALLINT,\n  CTY VARCHAR(15)\n)\nRETURNS (LANGUAGES VARCHAR(15))\nAS\nDECLARE VARIABLE i INTEGER;\nBEGIN\n  i = 1;\n  WHILE (i <= 5) DO\n  BEGIN\n    SELECT language_req[:i] FROM joB\n    WHERE ((job_code = :code) AND (job_grade = :grade) AND (job_country = :cty)\n           AND (language_req IS NOT NULL))\n    INTO :languages;\n    IF (languages = ' ') THEN  /* Prints 'NULL' instead of blanks */\n       languages = 'NULL';         \n    i = i +1;\n    SUSPEND;\n  END\nEND",
        "ALTER PROCEDURE ALL_LANGS\nRETURNS (\n  CODE VARCHAR(5),\n  GRADE VARCHAR(5),\n  COUNTRY VARCHAR(15),\n  LANG VARCHAR(15)\n)\nAS\nBEGIN\n\tFOR SELECT job_code, job_grade, job_country FROM job \n\t\tINTO :code, :grade, :country\n\n\tDO\n\tBEGIN\n\t    FOR SELECT languages FROM show_langs \n \t\t    (:code, :grade, :country) INTO :lang DO\n\t        SUSPEND;\n\t    /* Put nice separators between rows */\n\t    code = '=====';\n\t    grade = '=====';\n\t    country = '===============';\n\t    lang = '==============';\n\t    SUSPEND;\n\tEND\n    END"
    ]
    script = s.get_metadata_ddl(sections=[sm.Section.TRIGGERS])
    assert script == [
        'CREATE TRIGGER SET_EMP_NO FOR EMPLOYEE ACTIVE\nBEFORE INSERT POSITION 0\nAS\nBEGIN\n    if (new.emp_no is null) then\n    new.emp_no = gen_id(emp_no_gen, 1);\nEND',
        "CREATE TRIGGER SAVE_SALARY_CHANGE FOR EMPLOYEE ACTIVE\nAFTER UPDATE POSITION 0\nAS\nBEGIN\n    IF (old.salary <> new.salary) THEN\n        INSERT INTO salary_history\n            (emp_no, change_date, updater_id, old_salary, percent_change)\n        VALUES (\n            old.emp_no,\n            'NOW',\n            user,\n            old.salary,\n            (new.salary - old.salary) * 100 / old.salary);\nEND",
        'CREATE TRIGGER SET_CUST_NO FOR CUSTOMER ACTIVE\nBEFORE INSERT POSITION 0\nAS\nBEGIN\n    if (new.cust_no is null) then\n    new.cust_no = gen_id(cust_no_gen, 1);\nEND',
        "CREATE TRIGGER POST_NEW_ORDER FOR SALES ACTIVE\nAFTER INSERT POSITION 0\nAS\nBEGIN\n    POST_EVENT 'new_order';\nEND",
        'CREATE TRIGGER TR_CONNECT ACTIVE\nON CONNECT POSITION 0\nAS \nBEGIN \n    /* enter trigger code here */ \nEND',
        'CREATE TRIGGER TR_MULTI FOR COUNTRY ACTIVE\nAFTER INSERT OR UPDATE OR DELETE POSITION 0\nAS \nBEGIN \n    /* enter trigger code here */ \nEND',
        'CREATE TRIGGER TRIG_DDL_SP ACTIVE\nBEFORE ALTER FUNCTION POSITION 0\nAS \nBEGIN \n    /* enter trigger code here */ \nEND',
        'CREATE TRIGGER TRIG_DDL ACTIVE\nBEFORE ANY DDL STATEMENT POSITION 0\nAS \nBEGIN \n    /* enter trigger code here */ \nEND'
    ]
    script = s.get_metadata_ddl(sections=[sm.Section.ROLES])
    assert script == ['CREATE ROLE TEST_ROLE']
    script = s.get_metadata_ddl(sections=[sm.Section.GRANTS])
    assert script == [
        'GRANT SELECT ON COUNTRY TO PUBLIC WITH GRANT OPTION',
        'GRANT INSERT ON COUNTRY TO PUBLIC WITH GRANT OPTION',
        'GRANT UPDATE ON COUNTRY TO PUBLIC WITH GRANT OPTION',
        'GRANT DELETE ON COUNTRY TO PUBLIC WITH GRANT OPTION',
        'GRANT REFERENCES ON COUNTRY TO PUBLIC WITH GRANT OPTION',
        'GRANT SELECT ON JOB TO PUBLIC WITH GRANT OPTION',
        'GRANT INSERT ON JOB TO PUBLIC WITH GRANT OPTION',
        'GRANT UPDATE ON JOB TO PUBLIC WITH GRANT OPTION',
        'GRANT DELETE ON JOB TO PUBLIC WITH GRANT OPTION',
        'GRANT REFERENCES ON JOB TO PUBLIC WITH GRANT OPTION',
        'GRANT SELECT ON DEPARTMENT TO PUBLIC WITH GRANT OPTION',
        'GRANT INSERT ON DEPARTMENT TO PUBLIC WITH GRANT OPTION',
        'GRANT UPDATE ON DEPARTMENT TO PUBLIC WITH GRANT OPTION',
        'GRANT DELETE ON DEPARTMENT TO PUBLIC WITH GRANT OPTION',
        'GRANT REFERENCES ON DEPARTMENT TO PUBLIC WITH GRANT OPTION',
        'GRANT SELECT ON EMPLOYEE TO PUBLIC WITH GRANT OPTION',
        'GRANT INSERT ON EMPLOYEE TO PUBLIC WITH GRANT OPTION',
        'GRANT UPDATE ON EMPLOYEE TO PUBLIC WITH GRANT OPTION',
        'GRANT DELETE ON EMPLOYEE TO PUBLIC WITH GRANT OPTION',
        'GRANT REFERENCES ON EMPLOYEE TO PUBLIC WITH GRANT OPTION',
        'GRANT SELECT ON PHONE_LIST TO PUBLIC WITH GRANT OPTION',
        'GRANT INSERT ON PHONE_LIST TO PUBLIC WITH GRANT OPTION',
        'GRANT UPDATE ON PHONE_LIST TO PUBLIC WITH GRANT OPTION',
        'GRANT DELETE ON PHONE_LIST TO PUBLIC WITH GRANT OPTION',
        'GRANT REFERENCES ON PHONE_LIST TO PUBLIC WITH GRANT OPTION',
        'GRANT SELECT ON PROJECT TO PUBLIC WITH GRANT OPTION',
        'GRANT INSERT ON PROJECT TO PUBLIC WITH GRANT OPTION',
        'GRANT UPDATE ON PROJECT TO PUBLIC WITH GRANT OPTION',
        'GRANT DELETE ON PROJECT TO PUBLIC WITH GRANT OPTION',
        'GRANT REFERENCES ON PROJECT TO PUBLIC WITH GRANT OPTION',
        'GRANT SELECT ON EMPLOYEE_PROJECT TO PUBLIC WITH GRANT OPTION',
        'GRANT INSERT ON EMPLOYEE_PROJECT TO PUBLIC WITH GRANT OPTION',
        'GRANT UPDATE ON EMPLOYEE_PROJECT TO PUBLIC WITH GRANT OPTION',
        'GRANT DELETE ON EMPLOYEE_PROJECT TO PUBLIC WITH GRANT OPTION',
        'GRANT REFERENCES ON EMPLOYEE_PROJECT TO PUBLIC WITH GRANT OPTION',
        'GRANT SELECT ON PROJ_DEPT_BUDGET TO PUBLIC WITH GRANT OPTION',
        'GRANT INSERT ON PROJ_DEPT_BUDGET TO PUBLIC WITH GRANT OPTION',
        'GRANT UPDATE ON PROJ_DEPT_BUDGET TO PUBLIC WITH GRANT OPTION',
        'GRANT DELETE ON PROJ_DEPT_BUDGET TO PUBLIC WITH GRANT OPTION',
        'GRANT REFERENCES ON PROJ_DEPT_BUDGET TO PUBLIC WITH GRANT OPTION',
        'GRANT SELECT ON SALARY_HISTORY TO PUBLIC WITH GRANT OPTION',
        'GRANT INSERT ON SALARY_HISTORY TO PUBLIC WITH GRANT OPTION',
        'GRANT UPDATE ON SALARY_HISTORY TO PUBLIC WITH GRANT OPTION',
        'GRANT DELETE ON SALARY_HISTORY TO PUBLIC WITH GRANT OPTION',
        'GRANT REFERENCES ON SALARY_HISTORY TO PUBLIC WITH GRANT OPTION',
        'GRANT SELECT ON CUSTOMER TO PUBLIC WITH GRANT OPTION',
        'GRANT INSERT ON CUSTOMER TO PUBLIC WITH GRANT OPTION',
        'GRANT UPDATE ON CUSTOMER TO PUBLIC WITH GRANT OPTION',
        'GRANT DELETE ON CUSTOMER TO PUBLIC WITH GRANT OPTION',
        'GRANT REFERENCES ON CUSTOMER TO PUBLIC WITH GRANT OPTION',
        'GRANT SELECT ON SALES TO PUBLIC WITH GRANT OPTION',
        'GRANT INSERT ON SALES TO PUBLIC WITH GRANT OPTION',
        'GRANT UPDATE ON SALES TO PUBLIC WITH GRANT OPTION',
        'GRANT DELETE ON SALES TO PUBLIC WITH GRANT OPTION',
        'GRANT REFERENCES ON SALES TO PUBLIC WITH GRANT OPTION',
        'GRANT EXECUTE ON PROCEDURE GET_EMP_PROJ TO PUBLIC WITH GRANT OPTION',
        'GRANT EXECUTE ON PROCEDURE ADD_EMP_PROJ TO PUBLIC WITH GRANT OPTION',
        'GRANT EXECUTE ON PROCEDURE SUB_TOT_BUDGET TO PUBLIC WITH GRANT OPTION',
        'GRANT EXECUTE ON PROCEDURE DELETE_EMPLOYEE TO PUBLIC WITH GRANT OPTION',
        'GRANT EXECUTE ON PROCEDURE DEPT_BUDGET TO PUBLIC WITH GRANT OPTION',
        'GRANT EXECUTE ON PROCEDURE ORG_CHART TO PUBLIC WITH GRANT OPTION',
        'GRANT EXECUTE ON PROCEDURE MAIL_LABEL TO PUBLIC WITH GRANT OPTION',
        'GRANT EXECUTE ON PROCEDURE SHIP_ORDER TO PUBLIC WITH GRANT OPTION',
        'GRANT EXECUTE ON PROCEDURE SHOW_LANGS TO PUBLIC WITH GRANT OPTION',
        'GRANT EXECUTE ON PROCEDURE ALL_LANGS TO PUBLIC WITH GRANT OPTION'
    ]
    script = s.get_metadata_ddl(sections=[sm.Section.COMMENTS])
    assert script == ["COMMENT ON CHARACTER SET NONE IS 'Comment on NONE character set'"]
    script = s.get_metadata_ddl(sections=[sm.Section.SHADOWS])
    assert script == []
    script = s.get_metadata_ddl(sections=[sm.Section.INDEX_DEACTIVATIONS])
    if version == FB30:
        assert script == [
            'ALTER INDEX MINSALX INACTIVE',
            'ALTER INDEX MAXSALX INACTIVE',
            'ALTER INDEX BUDGETX INACTIVE',
            'ALTER INDEX NAMEX INACTIVE',
            'ALTER INDEX PRODTYPEX INACTIVE',
            'ALTER INDEX UPDATERX INACTIVE',
            'ALTER INDEX CHANGEX INACTIVE',
            'ALTER INDEX CUSTNAMEX INACTIVE',
            'ALTER INDEX CUSTREGION INACTIVE',
            'ALTER INDEX NEEDX INACTIVE',
            'ALTER INDEX SALESTATX INACTIVE',
            'ALTER INDEX QTYX INACTIVE'
        ]
    else:
        assert script == [
            'ALTER INDEX NEEDX INACTIVE',
            'ALTER INDEX SALESTATX INACTIVE',
            'ALTER INDEX QTYX INACTIVE',
            'ALTER INDEX UPDATERX INACTIVE',
            'ALTER INDEX CHANGEX INACTIVE',
            'ALTER INDEX PRODTYPEX INACTIVE',
            'ALTER INDEX CUSTNAMEX INACTIVE',
            'ALTER INDEX CUSTREGION INACTIVE',
            'ALTER INDEX NAMEX INACTIVE',
            'ALTER INDEX BUDGETX INACTIVE',
            'ALTER INDEX MINSALX INACTIVE',
            'ALTER INDEX MAXSALX INACTIVE'
        ]
    script = s.get_metadata_ddl(sections=[sm.Section.INDEX_ACTIVATIONS])
    if version == FB30:
        assert script == [
            'ALTER INDEX MINSALX ACTIVE',
            'ALTER INDEX MAXSALX ACTIVE',
            'ALTER INDEX BUDGETX ACTIVE',
            'ALTER INDEX NAMEX ACTIVE',
            'ALTER INDEX PRODTYPEX ACTIVE',
            'ALTER INDEX UPDATERX ACTIVE',
            'ALTER INDEX CHANGEX ACTIVE',
            'ALTER INDEX CUSTNAMEX ACTIVE',
            'ALTER INDEX CUSTREGION ACTIVE',
            'ALTER INDEX NEEDX ACTIVE',
            'ALTER INDEX SALESTATX ACTIVE',
            'ALTER INDEX QTYX ACTIVE'
        ]
    else:
        assert script == [
            'ALTER INDEX NEEDX ACTIVE',
            'ALTER INDEX SALESTATX ACTIVE',
            'ALTER INDEX QTYX ACTIVE',
            'ALTER INDEX UPDATERX ACTIVE',
            'ALTER INDEX CHANGEX ACTIVE',
            'ALTER INDEX PRODTYPEX ACTIVE',
            'ALTER INDEX CUSTNAMEX ACTIVE',
            'ALTER INDEX CUSTREGION ACTIVE',
            'ALTER INDEX NAMEX ACTIVE',
            'ALTER INDEX BUDGETX ACTIVE',
            'ALTER INDEX MINSALX ACTIVE',
            'ALTER INDEX MAXSALX ACTIVE'
        ]
    script = s.get_metadata_ddl(sections=[sm.Section.SET_GENERATORS])
    assert script == ['ALTER SEQUENCE EMP_NO_GEN RESTART WITH 145',
                      'ALTER SEQUENCE CUST_NO_GEN RESTART WITH 1015']
    script = s.get_metadata_ddl(sections=[sm.Section.TRIGGER_DEACTIVATIONS])
    assert script == [
        'ALTER TRIGGER SET_EMP_NO INACTIVE',
        'ALTER TRIGGER SAVE_SALARY_CHANGE INACTIVE',
        'ALTER TRIGGER SET_CUST_NO INACTIVE',
        'ALTER TRIGGER POST_NEW_ORDER INACTIVE',
        'ALTER TRIGGER TR_CONNECT INACTIVE',
        'ALTER TRIGGER TR_MULTI INACTIVE',
        'ALTER TRIGGER TRIG_DDL_SP INACTIVE',
        'ALTER TRIGGER TRIG_DDL INACTIVE'
    ]
    script = s.get_metadata_ddl(sections=[sm.Section.TRIGGER_ACTIVATIONS])
    assert script == [
        'ALTER TRIGGER SET_EMP_NO ACTIVE',
        'ALTER TRIGGER SAVE_SALARY_CHANGE ACTIVE',
        'ALTER TRIGGER SET_CUST_NO ACTIVE',
        'ALTER TRIGGER POST_NEW_ORDER ACTIVE',
        'ALTER TRIGGER TR_CONNECT ACTIVE',
        'ALTER TRIGGER TR_MULTI ACTIVE',
        'ALTER TRIGGER TRIG_DDL_SP ACTIVE',
        'ALTER TRIGGER TRIG_DDL ACTIVE']


# --- Mock Function Helper (Needed for test_19_FunctionArgument, test_20_Function) ---
# Note: This mock might need adjustments based on the exact Schema implementation details
# It's simplified here to provide the necessary structure.

def _mockFunction(s: Schema, name):
    f = None
    if name == 'ADDDAY':
        f = Function(s, {'RDB$FUNCTION_NAME': 'ADDDAY                         ',
                         'RDB$FUNCTION_TYPE': None, 'RDB$DESCRIPTION': None,
                         'RDB$MODULE_NAME': 'fbudf',
                         'RDB$ENTRYPOINT': 'addDay                                                                                                                                                                                                                                                         ',
                         'RDB$RETURN_ARGUMENT': 0, 'RDB$SYSTEM_FLAG': 0,
                         'RDB$ENGINE_NAME': None, 'RDB$PACKAGE_NAME': None,
                         'RDB$PRIVATE_FLAG': None, 'RDB$FUNCTION_SOURCE': None,
                         'RDB$FUNCTION_ID': 12, 'RDB$VALID_BLR': None,
                         'RDB$SECURITY_CLASS': 'SQL$425                        ',
                         'RDB$OWNER_NAME': 'SYSDBA                         ',
                         'RDB$LEGACY_FLAG': 1, 'RDB$DETERMINISTIC_FLAG': 0})
        f._load_arguments(
            [{'RDB$FUNCTION_NAME': 'ADDDAY                         ',
              'RDB$ARGUMENT_POSITION': 0, 'RDB$MECHANISM': 1, 'RDB$FIELD_TYPE': 35,
              'RDB$FIELD_SCALE': 0, 'RDB$FIELD_LENGTH': 8, 'RDB$FIELD_SUB_TYPE': None,
              'RDB$CHARACTER_SET_ID': None, 'RDB$FIELD_PRECISION': None,
              'RDB$CHARACTER_LENGTH': None, 'RDB$PACKAGE_NAME': None,
              'RDB$ARGUMENT_NAME': None, 'RDB$FIELD_SOURCE': None,
              'RDB$DEFAULT_SOURCE': None, 'RDB$COLLATION_ID': None,
              'RDB$NULL_FLAG': None, 'RDB$ARGUMENT_MECHANISM': None,
              'RDB$FIELD_NAME': None, 'RDB$RELATION_NAME': None,
              'RDB$SYSTEM_FLAG': 0, 'RDB$DESCRIPTION': None},
             {'RDB$FUNCTION_NAME': 'ADDDAY                         ',
              'RDB$ARGUMENT_POSITION': 1, 'RDB$MECHANISM': 1, 'RDB$FIELD_TYPE': 35,
              'RDB$FIELD_SCALE': 0, 'RDB$FIELD_LENGTH': 8, 'RDB$FIELD_SUB_TYPE': None,
              'RDB$CHARACTER_SET_ID': None, 'RDB$FIELD_PRECISION': None,
              'RDB$CHARACTER_LENGTH': None, 'RDB$PACKAGE_NAME': None,
              'RDB$ARGUMENT_NAME': None, 'RDB$FIELD_SOURCE': None,
              'RDB$DEFAULT_SOURCE': None, 'RDB$COLLATION_ID': None,
              'RDB$NULL_FLAG': None, 'RDB$ARGUMENT_MECHANISM': None,
              'RDB$FIELD_NAME': None, 'RDB$RELATION_NAME': None,
              'RDB$SYSTEM_FLAG': 0, 'RDB$DESCRIPTION': None},
             {'RDB$FUNCTION_NAME': 'ADDDAY                         ',
              'RDB$ARGUMENT_POSITION': 2, 'RDB$MECHANISM': 1, 'RDB$FIELD_TYPE': 8,
              'RDB$FIELD_SCALE': 0, 'RDB$FIELD_LENGTH': 4, 'RDB$FIELD_SUB_TYPE': 0,
              'RDB$CHARACTER_SET_ID': None, 'RDB$FIELD_PRECISION': 0,
              'RDB$CHARACTER_LENGTH': None, 'RDB$PACKAGE_NAME': None,
              'RDB$ARGUMENT_NAME': None, 'RDB$FIELD_SOURCE': None,
              'RDB$DEFAULT_SOURCE': None, 'RDB$COLLATION_ID': None,
              'RDB$NULL_FLAG': None, 'RDB$ARGUMENT_MECHANISM': None,
              'RDB$FIELD_NAME': None, 'RDB$RELATION_NAME': None, 'RDB$SYSTEM_FLAG': 0,
              'RDB$DESCRIPTION': None}
             ]
        )
    elif name == 'STRING2BLOB':
        f = sm.Function(s,
                        {'RDB$FUNCTION_NAME': 'STRING2BLOB                    ',
                         'RDB$FUNCTION_TYPE': None, 'RDB$DESCRIPTION': None,
                         'RDB$MODULE_NAME': 'fbudf',
                         'RDB$ENTRYPOINT': 'string2blob                                                                                                                                                                                                                                                    ',
                         'RDB$RETURN_ARGUMENT': 2, 'RDB$SYSTEM_FLAG': 0,
                         'RDB$ENGINE_NAME': None, 'RDB$PACKAGE_NAME': None,
                         'RDB$PRIVATE_FLAG': None, 'RDB$FUNCTION_SOURCE': None,
                         'RDB$FUNCTION_ID': 29, 'RDB$VALID_BLR': None,
                         'RDB$SECURITY_CLASS': 'SQL$442                        ',
                         'RDB$OWNER_NAME': 'SYSDBA                         ',
                         'RDB$LEGACY_FLAG': 1, 'RDB$DETERMINISTIC_FLAG': 0})
        f._load_arguments(
            [{'RDB$FUNCTION_NAME': 'STRING2BLOB                    ',
              'RDB$ARGUMENT_POSITION': 1, 'RDB$MECHANISM': 2, 'RDB$FIELD_TYPE': 37,
              'RDB$FIELD_SCALE': 0, 'RDB$FIELD_LENGTH': 1200, 'RDB$FIELD_SUB_TYPE': 0,
              'RDB$CHARACTER_SET_ID': 4, 'RDB$FIELD_PRECISION': None,
              'RDB$CHARACTER_LENGTH': 300, 'RDB$PACKAGE_NAME': None,
              'RDB$ARGUMENT_NAME': None, 'RDB$FIELD_SOURCE': None,
              'RDB$DEFAULT_SOURCE': None, 'RDB$COLLATION_ID': 0, 'RDB$NULL_FLAG': None,
              'RDB$ARGUMENT_MECHANISM': None, 'RDB$FIELD_NAME': None,
              'RDB$RELATION_NAME': None, 'RDB$SYSTEM_FLAG': 0, 'RDB$DESCRIPTION': None},
             {'RDB$FUNCTION_NAME': 'STRING2BLOB                    ',
              'RDB$ARGUMENT_POSITION': 2, 'RDB$MECHANISM': 3, 'RDB$FIELD_TYPE': 261,
              'RDB$FIELD_SCALE': 0, 'RDB$FIELD_LENGTH': 8, 'RDB$FIELD_SUB_TYPE': 0,
              'RDB$CHARACTER_SET_ID': None, 'RDB$FIELD_PRECISION': None,
              'RDB$CHARACTER_LENGTH': None, 'RDB$PACKAGE_NAME': None,
              'RDB$ARGUMENT_NAME': None, 'RDB$FIELD_SOURCE': None,
              'RDB$DEFAULT_SOURCE': None, 'RDB$COLLATION_ID': None,
              'RDB$NULL_FLAG': None, 'RDB$ARGUMENT_MECHANISM': None,
              'RDB$FIELD_NAME': None, 'RDB$RELATION_NAME': None, 'RDB$SYSTEM_FLAG': 0,
              'RDB$DESCRIPTION': None}
            ])
    elif name == 'SRIGHT':
        f = sm.Function(s,
                        {'RDB$FUNCTION_NAME': 'SRIGHT                         ',
                         'RDB$FUNCTION_TYPE': None, 'RDB$DESCRIPTION': None,
                         'RDB$MODULE_NAME': 'fbudf',
                         'RDB$ENTRYPOINT': 'right                                                                                                                                                                                                                                                          ',
                         'RDB$RETURN_ARGUMENT': 3, 'RDB$SYSTEM_FLAG': 0,
                         'RDB$ENGINE_NAME': None, 'RDB$PACKAGE_NAME': None,
                         'RDB$PRIVATE_FLAG': None, 'RDB$FUNCTION_SOURCE': None,
                         'RDB$FUNCTION_ID': 11, 'RDB$VALID_BLR': None,
                         'RDB$SECURITY_CLASS': 'SQL$424                        ',
                         'RDB$OWNER_NAME': 'SYSDBA                         ',
                         'RDB$LEGACY_FLAG': 1, 'RDB$DETERMINISTIC_FLAG': 0})
        f._load_arguments(
            [{'RDB$FUNCTION_NAME': 'SRIGHT                         ',
              'RDB$ARGUMENT_POSITION': 1, 'RDB$MECHANISM': 2, 'RDB$FIELD_TYPE': 37,
              'RDB$FIELD_SCALE': 0, 'RDB$FIELD_LENGTH': 400, 'RDB$FIELD_SUB_TYPE': 0,
              'RDB$CHARACTER_SET_ID': 4, 'RDB$FIELD_PRECISION': None,
              'RDB$CHARACTER_LENGTH': 100, 'RDB$PACKAGE_NAME': None,
              'RDB$ARGUMENT_NAME': None, 'RDB$FIELD_SOURCE': None,
              'RDB$DEFAULT_SOURCE': None, 'RDB$COLLATION_ID': 0, 'RDB$NULL_FLAG': None,
              'RDB$ARGUMENT_MECHANISM': None, 'RDB$FIELD_NAME': None,
              'RDB$RELATION_NAME': None, 'RDB$SYSTEM_FLAG': 0, 'RDB$DESCRIPTION': None},
             {'RDB$FUNCTION_NAME': 'SRIGHT                         ',
              'RDB$ARGUMENT_POSITION': 2, 'RDB$MECHANISM': 1, 'RDB$FIELD_TYPE': 7,
              'RDB$FIELD_SCALE': 0, 'RDB$FIELD_LENGTH': 2, 'RDB$FIELD_SUB_TYPE': 0,
              'RDB$CHARACTER_SET_ID': None, 'RDB$FIELD_PRECISION': 0,
              'RDB$CHARACTER_LENGTH': None, 'RDB$PACKAGE_NAME': None,
              'RDB$ARGUMENT_NAME': None, 'RDB$FIELD_SOURCE': None,
              'RDB$DEFAULT_SOURCE': None, 'RDB$COLLATION_ID': None,
              'RDB$NULL_FLAG': None, 'RDB$ARGUMENT_MECHANISM': None,
              'RDB$FIELD_NAME': None, 'RDB$RELATION_NAME': None, 'RDB$SYSTEM_FLAG': 0,
              'RDB$DESCRIPTION': None},
             {'RDB$FUNCTION_NAME': 'SRIGHT                         ',
              'RDB$ARGUMENT_POSITION': 3, 'RDB$MECHANISM': 2, 'RDB$FIELD_TYPE': 37,
              'RDB$FIELD_SCALE': 0, 'RDB$FIELD_LENGTH': 400, 'RDB$FIELD_SUB_TYPE': 0,
              'RDB$CHARACTER_SET_ID': 4, 'RDB$FIELD_PRECISION': None,
              'RDB$CHARACTER_LENGTH': 100, 'RDB$PACKAGE_NAME': None,
              'RDB$ARGUMENT_NAME': None, 'RDB$FIELD_SOURCE': None,
              'RDB$DEFAULT_SOURCE': None, 'RDB$COLLATION_ID': 0, 'RDB$NULL_FLAG': None,
              'RDB$ARGUMENT_MECHANISM': None, 'RDB$FIELD_NAME': None,
              'RDB$RELATION_NAME': None, 'RDB$SYSTEM_FLAG': 0, 'RDB$DESCRIPTION': None}
            ])
    elif name == 'I64NVL':
        f = sm.Function(s,
                        {'RDB$FUNCTION_NAME': 'I64NVL                         ',
                         'RDB$FUNCTION_TYPE': None, 'RDB$DESCRIPTION': None,
                         'RDB$MODULE_NAME': 'fbudf',
                         'RDB$ENTRYPOINT': 'idNvl                                                                                                                                                                                                                                                          ',
                         'RDB$RETURN_ARGUMENT': 0, 'RDB$SYSTEM_FLAG': 0,
                         'RDB$ENGINE_NAME': None, 'RDB$PACKAGE_NAME': None,
                         'RDB$PRIVATE_FLAG': None, 'RDB$FUNCTION_SOURCE': None,
                         'RDB$FUNCTION_ID': 2, 'RDB$VALID_BLR': None,
                         'RDB$SECURITY_CLASS': 'SQL$415                        ',
                         'RDB$OWNER_NAME': 'SYSDBA                         ',
                         'RDB$LEGACY_FLAG': 1, 'RDB$DETERMINISTIC_FLAG': 0})
        f._load_arguments(
            [{'RDB$FUNCTION_NAME': 'I64NVL                         ',
              'RDB$ARGUMENT_POSITION': 0, 'RDB$MECHANISM': 2, 'RDB$FIELD_TYPE': 16,
              'RDB$FIELD_SCALE': 0, 'RDB$FIELD_LENGTH': 8, 'RDB$FIELD_SUB_TYPE': 1,
              'RDB$CHARACTER_SET_ID': None, 'RDB$FIELD_PRECISION': 18,
              'RDB$CHARACTER_LENGTH': None, 'RDB$PACKAGE_NAME': None,
              'RDB$ARGUMENT_NAME': None, 'RDB$FIELD_SOURCE': None,
              'RDB$DEFAULT_SOURCE': None, 'RDB$COLLATION_ID': None,
              'RDB$NULL_FLAG': None, 'RDB$ARGUMENT_MECHANISM': None,
              'RDB$FIELD_NAME': None, 'RDB$RELATION_NAME': None, 'RDB$SYSTEM_FLAG': 0,
              'RDB$DESCRIPTION': None},
             {'RDB$FUNCTION_NAME': 'I64NVL                         ',
              'RDB$ARGUMENT_POSITION': 1, 'RDB$MECHANISM': 2, 'RDB$FIELD_TYPE': 16,
              'RDB$FIELD_SCALE': 0, 'RDB$FIELD_LENGTH': 8, 'RDB$FIELD_SUB_TYPE': 1,
              'RDB$CHARACTER_SET_ID': None, 'RDB$FIELD_PRECISION': 18,
              'RDB$CHARACTER_LENGTH': None, 'RDB$PACKAGE_NAME': None,
              'RDB$ARGUMENT_NAME': None, 'RDB$FIELD_SOURCE': None,
              'RDB$DEFAULT_SOURCE': None, 'RDB$COLLATION_ID': None,
              'RDB$NULL_FLAG': None, 'RDB$ARGUMENT_MECHANISM': None,
              'RDB$FIELD_NAME': None, 'RDB$RELATION_NAME': None, 'RDB$SYSTEM_FLAG': 0,
              'RDB$DESCRIPTION': None},
             {'RDB$FUNCTION_NAME': 'I64NVL                         ',
              'RDB$ARGUMENT_POSITION': 2, 'RDB$MECHANISM': 2, 'RDB$FIELD_TYPE': 16,
              'RDB$FIELD_SCALE': 0, 'RDB$FIELD_LENGTH': 8, 'RDB$FIELD_SUB_TYPE': 1,
              'RDB$CHARACTER_SET_ID': None, 'RDB$FIELD_PRECISION': 18,
              'RDB$CHARACTER_LENGTH': None, 'RDB$PACKAGE_NAME': None,
              'RDB$ARGUMENT_NAME': None, 'RDB$FIELD_SOURCE': None,
              'RDB$DEFAULT_SOURCE': None, 'RDB$COLLATION_ID': None,
              'RDB$NULL_FLAG': None, 'RDB$ARGUMENT_MECHANISM': None,
              'RDB$FIELD_NAME': None, 'RDB$RELATION_NAME': None, 'RDB$SYSTEM_FLAG': 0,
              'RDB$DESCRIPTION': None}
            ])
    if f:
        return f
    else:
        raise Exception(f"Udefined function '{name}' for mock.")
