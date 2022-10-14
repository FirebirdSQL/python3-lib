#coding:utf-8
#
#   PROGRAM/MODULE: firebird-lib
#   FILE:           test_schema.py
#   DESCRIPTION:    Unit tests for firebird.lib.schema
#   CREATED:        21.9.2020
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
#                 ______________________________________

import unittest
import sys, os
from re import finditer
from firebird.driver import driver_config, connect, connect_server
from firebird.lib.schema import *
from firebird.lib import schema as sm
from io import StringIO

FB30 = '3.0'
FB40 = '4.0'
FB50 = '5.0'

if driver_config.get_server('local') is None:
    # Register Firebird server
    srv_cfg = """[local]
    host = localhost
    user = SYSDBA
    password = masterkey
    """
    driver_config.register_server('local', srv_cfg)

# Register database
if driver_config.get_database('fbtest') is None:
    db_cfg = """[fbtest]
    server = local
    database = fbtest3.fdb
    protocol = inet
    charset = utf8
    """
    driver_config.register_database('fbtest', db_cfg)

def linesplit_iter(string):
    return (m.group(2) for m in finditer('((.*)\n|(.+)$)', string))


class SchemaVisitor(Visitor):
    def __init__(self, test, action, follow='dependencies'):
        self.test = test
        self.seen = []
        self.action = action
        self.follow = follow
    def default_action(self, obj):
        if not obj.is_sys_object() and self.action in obj.actions:
            if self.follow == 'dependencies':
                for dependency in obj.get_dependencies():
                    d = dependency.depended_on
                    if d and d not in self.seen:
                        d.accept(self)
            elif self.follow == 'dependents':
                for dependency in obj.get_dependents():
                    d = dependency.dependent
                    if d and d not in self.seen:
                        d.accept(self)
            if obj not in self.seen:
                self.test.printout(obj.get_sql_for(self.action))
                self.seen.append(obj)
    def visit_TableColumn(self, column):
        column.table.accept(self)
    def visit_ViewColumn(self, column):
        column.view.accept(self)
    def visit_ProcedureParameter(self, param):
        param.procedure.accept(self)
    def visit_FunctionArgument(self, arg):
        arg.function.accept(self)

class TestBase(unittest.TestCase):
    def __init__(self, methodName='runTest'):
        super(TestBase, self).__init__(methodName)
        self.output = StringIO()
        self.FBTEST_DB = 'fbtest'
    def setUp(self):
        with connect_server('local') as svc:
            self.version = svc.info.version
        if self.version.startswith('3.0'):
            self.FBTEST_DB = 'fbtest30.fdb'
            self.version = FB30
        elif self.version.startswith('4.0'):
            self.FBTEST_DB = 'fbtest40.fdb'
            self.version = FB40
        elif self.version.startswith('5.0'):
            self.FBTEST_DB = 'fbtest50.fdb'
            self.version = FB50
        else:
            raise Exception("Unsupported Firebird version (%s)" % self.version)
        #
        self.cwd = os.getcwd()
        self.dbpath = self.cwd if os.path.split(self.cwd)[1] == 'test' \
            else os.path.join(self.cwd, 'test')
        self.dbfile = os.path.join(self.dbpath, self.FBTEST_DB)
        driver_config.get_database('fbtest').database.value = self.dbfile
    def clear_output(self):
        self.output.close()
        self.output = StringIO()
    def show_output(self):
        sys.stdout.write(self.output.getvalue())
        sys.stdout.flush()
    def printout(self, text='', newline=True, no_rstrip=False):
        if no_rstrip:
            self.output.write(text)
        else:
            self.output.write(text.rstrip())
        if newline:
            self.output.write('\n')
        self.output.flush()
    def printData(self, cur, print_header=True):
        """Print data from open cursor to stdout."""
        if print_header:
            # Print a header.
            line = []
            for fieldDesc in cur.description:
                line.append(fieldDesc[DESCRIPTION_NAME].ljust(fieldDesc[DESCRIPTION_DISPLAY_SIZE]))
            self.printout(' '.join(line))
            line = []
            for fieldDesc in cur.description:
                line.append("-" * max((len(fieldDesc[DESCRIPTION_NAME]), fieldDesc[DESCRIPTION_DISPLAY_SIZE])))
            self.printout(' '.join(line))
        # For each row, print the value of each field left-justified within
        # the maximum possible width of that field.
        fieldIndices = range(len(cur.description))
        for row in cur:
            line = []
            for fieldIndex in fieldIndices:
                fieldValue = str(row[fieldIndex])
                fieldMaxWidth = max((len(cur.description[fieldIndex][DESCRIPTION_NAME]), cur.description[fieldIndex][DESCRIPTION_DISPLAY_SIZE]))
                line.append(fieldValue.ljust(fieldMaxWidth))
            self.printout(' '.join(line))

class TestSchema(TestBase):
    def setUp(self):
        super().setUp()
        self.con = connect('fbtest')
    def tearDown(self):
        self.con.close()
    def test_01_SchemaBindClose(self):
        s = Schema()
        with self.assertRaises(Error) as cm:
            self.assertEqual(s.default_character_set.name, 'NONE')
        self.assertTupleEqual(cm.exception.args,
                              ("Schema is not binded to connection.",))
        self.assertTrue(s.closed)
        s.bind(self.con)
        # properties
        self.assertIsNone(s.description)
        self.assertIsNone(s.linger)
        self.assertEqual(s.owner_name, 'SYSDBA')
        self.assertEqual(s.default_character_set.name, 'NONE')
        self.assertEqual(s.security_class, 'SQL$363')
        self.assertFalse(s.closed)
        #
        s.close()
        self.assertTrue(s.closed)
        #
        with s.bind(self.con):
            self.assertFalse(s.closed)
        self.assertTrue(s.closed)

    def test_02_SchemaFromConnection(self):
        s = self.con.schema
        self.assertDictEqual(s.param_type_from,
                             {0: 'DATATYPE', 1: 'DOMAIN', 2: 'TYPE OF DOMAIN', 3: 'TYPE OF COLUMN'})
        self.assertDictEqual(s.object_types,
                             {0: 'RELATION', 1: 'VIEW', 2: 'TRIGGER', 3: 'COMPUTED_FIELD',
                              4: 'VALIDATION', 5: 'PROCEDURE', 6: 'EXPRESSION_INDEX',
                              7: 'EXCEPTION', 8: 'USER', 9: 'FIELD', 10: 'INDEX',
                              11: 'CHARACTER_SET', 12: 'USER_GROUP', 13: 'ROLE',
                              14: 'GENERATOR', 15: 'UDF', 16: 'BLOB_FILTER', 17: 'COLLATION',
                              18:'PACKAGE', 19:'PACKAGE BODY'})
        self.assertDictEqual(s.object_type_codes,
                             {'INDEX': 10, 'EXCEPTION': 7, 'GENERATOR': 14, 'COLLATION': 17,
                              'UDF': 15, 'EXPRESSION_INDEX': 6, 'FIELD': 9,
                              'COMPUTED_FIELD': 3, 'TRIGGER': 2, 'RELATION': 0, 'USER': 8,
                              'USER_GROUP': 12, 'BLOB_FILTER': 16, 'ROLE': 13,
                              'VALIDATION': 4, 'PROCEDURE': 5, 'VIEW': 1, 'CHARACTER_SET':11,
                              'PACKAGE':18, 'PACKAGE BODY':19})
        self.assertDictEqual(s.character_set_names,
                             {0: 'NONE', 1: 'BINARY', 2: 'ASCII7', 3: 'SQL_TEXT', 4: 'UTF-8',
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
                              66: 'TIS620', 67: 'GBK', 68: 'CP943C', 69: 'GB18030'})
        if self.version == FB30:
            self.assertDictEqual(s.field_types,
                                 {35: 'TIMESTAMP', 37: 'VARYING', 7: 'SHORT', 8: 'LONG',
                                  9: 'QUAD', 10: 'FLOAT', 12: 'DATE', 45: 'BLOB_ID', 14: 'TEXT',
                                  13: 'TIME', 16: 'INT64', 40: 'CSTRING', 27: 'DOUBLE',
                                  261: 'BLOB', 23:'BOOLEAN'})
        else:
            self.assertDictEqual(s.field_types,
                                 {35: 'TIMESTAMP', 37: 'VARYING', 7: 'SHORT', 8: 'LONG',
                                  9: 'QUAD', 10: 'FLOAT', 12: 'DATE', 45: 'BLOB_ID', 14: 'TEXT',
                                  13: 'TIME', 16: 'INT64', 40: 'CSTRING', 27: 'DOUBLE',
                                  261: 'BLOB', 23:'BOOLEAN', 24: 'DECFLOAT(16)',
                                  25: 'DECFLOAT(34)', 26: 'INT128', 28: 'TIME WITH TIME ZONE',
                                  29: 'TIMESTAMP WITH TIME ZONE'})
        self.assertDictEqual(s.field_subtypes,
                             {0: 'BINARY', 1: 'TEXT', 2: 'BLR', 3: 'ACL', 4: 'RANGES',
                              5: 'SUMMARY', 6: 'FORMAT', 7: 'TRANSACTION_DESCRIPTION',
                              8: 'EXTERNAL_FILE_DESCRIPTION', 9: 'DEBUG_INFORMATION'})
        self.assertDictEqual(s.function_types, {0: 'VALUE', 1: 'BOOLEAN'})
        self.assertDictEqual(s.mechanism_types,
                             {0: 'BY_VALUE', 1: 'BY_REFERENCE',
                              2: 'BY_VMS_DESCRIPTOR', 3: 'BY_ISC_DESCRIPTOR',
                              4: 'BY_SCALAR_ARRAY_DESCRIPTOR',
                              5: 'BY_REFERENCE_WITH_NULL'})
        self.assertDictEqual(s.parameter_mechanism_types,
                             {0: 'NORMAL', 1: 'TYPE OF'})
        self.assertDictEqual(s.procedure_types,
                             {0: 'LEGACY', 1: 'SELECTABLE', 2: 'EXECUTABLE'})
        self.assertDictEqual(s.relation_types,
                             {0: 'PERSISTENT', 1: 'VIEW', 2: 'EXTERNAL', 3: 'VIRTUAL',
                              4: 'GLOBAL_TEMPORARY_PRESERVE', 5: 'GLOBAL_TEMPORARY_DELETE'})
        self.assertDictEqual(s.system_flag_types,
                             {0: 'USER', 1: 'SYSTEM', 2: 'QLI', 3: 'CHECK_CONSTRAINT',
                              4: 'REFERENTIAL_CONSTRAINT', 5: 'VIEW_CHECK', 6: 'IDENTITY_GENERATOR'})
        self.assertDictEqual(s.transaction_state_types,
                             {1: 'LIMBO', 2: 'COMMITTED', 3: 'ROLLED_BACK'})
        self.assertDictEqual(s.trigger_types,
                             {8192: 'CONNECT', 1: 'PRE_STORE', 2: 'POST_STORE',
                              3: 'PRE_MODIFY', 4: 'POST_MODIFY', 5: 'PRE_ERASE',
                              6: 'POST_ERASE', 8193: 'DISCONNECT', 8194: 'TRANSACTION_START',
                              8195: 'TRANSACTION_COMMIT', 8196: 'TRANSACTION_ROLLBACK'})
        self.assertDictEqual(s.parameter_types,
                             {0: 'INPUT', 1: 'OUTPUT'})
        self.assertDictEqual(s.index_activity_flags,
                             {0: 'ACTIVE', 1: 'INACTIVE'})
        self.assertDictEqual(s.index_unique_flags,
                             {0: 'NON_UNIQUE', 1: 'UNIQUE'})
        self.assertDictEqual(s.trigger_activity_flags,
                             {0: 'ACTIVE', 1: 'INACTIVE'})
        self.assertDictEqual(s.grant_options,
                             {0: 'NONE', 1: 'GRANT_OPTION', 2: 'ADMIN_OPTION'})
        self.assertDictEqual(s.page_types,
                             {1: 'HEADER', 2: 'PAGE_INVENTORY', 3: 'TRANSACTION_INVENTORY',
                              4: 'POINTER', 5: 'DATA', 6: 'INDEX_ROOT', 7: 'INDEX_BUCKET',
                              8: 'BLOB', 9: 'GENERATOR', 10: 'SCN_INVENTORY'})
        self.assertDictEqual(s.privacy_flags,
                             {0: 'PUBLIC', 1: 'PRIVATE'})
        self.assertDictEqual(s.legacy_flags,
                             {0: 'NEW_STYLE', 1: 'LEGACY_STYLE'})
        self.assertDictEqual(s.deterministic_flags,
                             {0: 'NON_DETERMINISTIC', 1: 'DETERMINISTIC'})

        # properties
        self.assertIsNone(s.description)
        self.assertEqual(s.owner_name, 'SYSDBA')
        self.assertEqual(s.default_character_set.name, 'NONE')
        self.assertEqual(s.security_class, 'SQL$363')
        # Lists of db objects
        self.assertIsInstance(s.collations, DataList)
        self.assertIsInstance(s.character_sets, DataList)
        self.assertIsInstance(s.exceptions, DataList)
        self.assertIsInstance(s.generators, DataList)
        self.assertIsInstance(s.sys_generators, DataList)
        self.assertIsInstance(s.all_generators, DataList)
        self.assertIsInstance(s.domains, DataList)
        self.assertIsInstance(s.sys_domains, DataList)
        self.assertIsInstance(s.all_domains, DataList)
        self.assertIsInstance(s.indices, DataList)
        self.assertIsInstance(s.sys_indices, DataList)
        self.assertIsInstance(s.all_indices, DataList)
        self.assertIsInstance(s.tables, DataList)
        self.assertIsInstance(s.sys_tables, DataList)
        self.assertIsInstance(s.all_tables, DataList)
        self.assertIsInstance(s.views, DataList)
        self.assertIsInstance(s.sys_views, DataList)
        self.assertIsInstance(s.all_views, DataList)
        self.assertIsInstance(s.triggers, DataList)
        self.assertIsInstance(s.sys_triggers, DataList)
        self.assertIsInstance(s.all_triggers, DataList)
        self.assertIsInstance(s.procedures, DataList)
        self.assertIsInstance(s.sys_procedures, DataList)
        self.assertIsInstance(s.all_procedures, DataList)
        self.assertIsInstance(s.constraints, DataList)
        self.assertIsInstance(s.roles, DataList)
        self.assertIsInstance(s.dependencies, DataList)
        self.assertIsInstance(s.functions, DataList)
        self.assertIsInstance(s.sys_functions, DataList)
        self.assertIsInstance(s.all_functions, DataList)
        self.assertIsInstance(s.files, DataList)
        s.reload()
        self.assertEqual(len(s.collations), 150)
        self.assertEqual(len(s.character_sets), 52)
        self.assertEqual(len(s.exceptions), 5)
        self.assertEqual(len(s.generators), 2)
        self.assertEqual(len(s.sys_generators), 13)
        self.assertEqual(len(s.all_generators), 15)
        self.assertEqual(len(s.domains), 15)
        if self.version == FB30:
            self.assertEqual(len(s.sys_domains), 277)
            self.assertEqual(len(s.all_domains), 292)
            self.assertEqual(len(s.sys_indices), 82)
            self.assertEqual(len(s.all_indices), 94)
            self.assertEqual(len(s.sys_tables), 50)
            self.assertEqual(len(s.all_tables), 66)
            self.assertEqual(len(s.sys_procedures), 0)
            self.assertEqual(len(s.all_procedures), 11)
            self.assertEqual(len(s.constraints), 110)
            self.assertEqual(len(s.sys_functions), 0)
            self.assertEqual(len(s.all_functions), 6)
        else:
            self.assertEqual(len(s.sys_domains), 297)
            self.assertEqual(len(s.all_domains), 312)
            self.assertEqual(len(s.sys_indices), 85)
            self.assertEqual(len(s.all_indices), 97)
            self.assertEqual(len(s.sys_tables), 54)
            self.assertEqual(len(s.all_tables), 70)
            self.assertEqual(len(s.sys_procedures), 1)
            self.assertEqual(len(s.all_procedures), 12)
            self.assertEqual(len(s.constraints), 113)
            self.assertEqual(len(s.sys_functions), 1)
            self.assertEqual(len(s.all_functions), 7)
        self.assertEqual(len(s.indices), 12)
        self.assertEqual(len(s.tables), 16)
        self.assertEqual(len(s.views), 1)
        self.assertEqual(len(s.sys_views), 0)
        self.assertEqual(len(s.all_views), 1)
        self.assertEqual(len(s.triggers), 8)
        self.assertEqual(len(s.sys_triggers), 57)
        self.assertEqual(len(s.all_triggers), 65)
        self.assertEqual(len(s.procedures), 11)
        self.assertEqual(len(s.roles), 2)
        self.assertEqual(len(s.dependencies), 168)
        self.assertEqual(len(s.functions), 6)
        self.assertEqual(len(s.files), 0)
        #
        self.assertIsInstance(s.collations[0], sm.Collation)
        self.assertIsInstance(s.character_sets[0], sm.CharacterSet)
        self.assertIsInstance(s.exceptions[0], sm.DatabaseException)
        self.assertIsInstance(s.generators[0], sm.Sequence)
        self.assertIsInstance(s.sys_generators[0], sm.Sequence)
        self.assertIsInstance(s.all_generators[0], sm.Sequence)
        self.assertIsInstance(s.domains[0], sm.Domain)
        self.assertIsInstance(s.sys_domains[0], sm.Domain)
        self.assertIsInstance(s.all_domains[0], sm.Domain)
        self.assertIsInstance(s.indices[0], sm.Index)
        self.assertIsInstance(s.sys_indices[0], sm.Index)
        self.assertIsInstance(s.all_indices[0], sm.Index)
        self.assertIsInstance(s.tables[0], sm.Table)
        self.assertIsInstance(s.sys_tables[0], sm.Table)
        self.assertIsInstance(s.all_tables[0], sm.Table)
        self.assertIsInstance(s.views[0], sm.View)
        if len(s.sys_views) > 0:
            self.assertIsInstance(s.sys_views[0], sm.View)
        self.assertIsInstance(s.all_views[0], sm.View)
        self.assertIsInstance(s.triggers[0], sm.Trigger)
        self.assertIsInstance(s.sys_triggers[0], sm.Trigger)
        self.assertIsInstance(s.all_triggers[0], sm.Trigger)
        self.assertIsInstance(s.procedures[0], sm.Procedure)
        if len(s.sys_procedures) > 0:
            self.assertIsInstance(s.sys_procedures[0], sm.Procedure)
        self.assertIsInstance(s.all_procedures[0], sm.Procedure)
        self.assertIsInstance(s.constraints[0], sm.Constraint)
        if len(s.roles) > 0:
            self.assertIsInstance(s.roles[0], sm.Role)
        self.assertIsInstance(s.dependencies[0], sm.Dependency)
        if len(s.files) > 0:
            self.assertIsInstance(s.files[0], sm.DatabaseFile)
        self.assertIsInstance(s.functions[0], sm.Function)
        if len(s.sys_functions) > 0:
            self.assertIsInstance(s.sys_functions[0], sm.Function)
        self.assertIsInstance(s.all_functions[0], sm.Function)
        #
        self.assertEqual(s.collations.get('OCTETS').name, 'OCTETS')
        self.assertEqual(s.character_sets.get('WIN1250').name, 'WIN1250')
        self.assertEqual(s.exceptions.get('UNKNOWN_EMP_ID').name, 'UNKNOWN_EMP_ID')
        self.assertEqual(s.all_generators.get('EMP_NO_GEN').name, 'EMP_NO_GEN')
        self.assertEqual(s.all_indices.get('MINSALX').name, 'MINSALX')
        self.assertEqual(s.all_domains.get('FIRSTNAME').name, 'FIRSTNAME')
        self.assertEqual(s.all_tables.get('COUNTRY').name, 'COUNTRY')
        self.assertEqual(s.all_views.get('PHONE_LIST').name, 'PHONE_LIST')
        self.assertEqual(s.all_triggers.get('SET_EMP_NO').name, 'SET_EMP_NO')
        self.assertEqual(s.all_procedures.get('GET_EMP_PROJ').name, 'GET_EMP_PROJ')
        self.assertEqual(s.constraints.get('INTEG_1').name, 'INTEG_1')
        #self.assertEqual(s.get_role('X').name,'X')
        self.assertEqual(s.get_collation_by_id(0, 0).name, 'NONE')
        self.assertEqual(s.get_charset_by_id(0).name, 'NONE')
        self.assertFalse(s.is_multifile())
        #
        self.assertFalse(s.closed)
        #
        with self.assertRaises(Error) as cm:
            s.close()
        self.assertTupleEqual(cm.exception.args,
                              ("Call to 'close' not allowed for embedded Schema.",))
        with self.assertRaises(Error) as cm:
            s.bind(self.con)
        self.assertTupleEqual(cm.exception.args,
                              ("Call to 'bind' not allowed for embedded Schema.",))
        # Reload
        s.reload([Category.TABLES, Category.VIEWS])
        self.assertEqual(s.all_tables.get('COUNTRY').name, 'COUNTRY')
        self.assertEqual(s.all_views.get('PHONE_LIST').name, 'PHONE_LIST')
    def test_03_Collation(self):
        s = Schema()
        s.bind(self.con)
        # System collation
        c = s.collations.get('ES_ES')
        # common properties
        self.assertEqual(c.name, 'ES_ES')
        self.assertIsNone(c.description)
        self.assertListEqual(c.actions, ['comment'])
        self.assertTrue(c.is_sys_object())
        self.assertEqual(c.get_quoted_name(), 'ES_ES')
        self.assertListEqual(c.get_dependents(), [])
        self.assertListEqual(c.get_dependencies(), [])
        if self.version == FB30:
            self.assertEqual(c.security_class, 'SQL$263')
        else:
            self.assertEqual(c.security_class, 'SQL$283')
        self.assertEqual(c.owner_name, 'SYSDBA')
        #
        self.assertEqual(c.id, 10)
        self.assertEqual(c.character_set.name, 'ISO8859_1')
        self.assertIsNone(c.base_collation)
        self.assertEqual(c.attributes, 1)
        self.assertEqual(c.specific_attributes,
                         'DISABLE-COMPRESSIONS=1;SPECIALS-FIRST=1')
        self.assertIsNone(c.function_name)
        # User defined collation
        # create collation TEST_COLLATE
        # for win1250
        # from WIN_CZ no pad case insensitive accent insensitive
        # 'DISABLE-COMPRESSIONS=0;DISABLE-EXPANSIONS=0'
        c = s.collations.get('TEST_COLLATE')
        # common properties
        self.assertEqual(c.name, 'TEST_COLLATE')
        self.assertIsNone(c.description)
        self.assertListEqual(c.actions, ['comment', 'create', 'drop'])
        self.assertFalse(c.is_sys_object())
        self.assertEqual(c.get_quoted_name(), 'TEST_COLLATE')
        self.assertListEqual(c.get_dependents(), [])
        self.assertListEqual(c.get_dependencies(), [])
        #
        self.assertEqual(c.id, 126)
        self.assertEqual(c.character_set.name, 'WIN1250')
        self.assertEqual(c.base_collation.name, 'WIN_CZ')
        self.assertEqual(c.attributes, 6)
        self.assertEqual(c.specific_attributes,
                         'DISABLE-COMPRESSIONS=0;DISABLE-EXPANSIONS=0')
        self.assertIsNone(c.function_name)
        self.assertEqual(c.get_sql_for('create'),
                         """CREATE COLLATION TEST_COLLATE
   FOR WIN1250
   FROM WIN_CZ
   NO PAD
   CASE INSENSITIVE
   ACCENT INSENSITIVE
   'DISABLE-COMPRESSIONS=0;DISABLE-EXPANSIONS=0'""")
        self.assertEqual(c.get_sql_for('drop'), "DROP COLLATION TEST_COLLATE")
        with self.assertRaises(ValueError) as cm:
            c.get_sql_for('drop', badparam='')
        self.assertTupleEqual(cm.exception.args,
                              ("Unsupported parameter(s) 'badparam'",))
        self.assertEqual(c.get_sql_for('comment'),
                         "COMMENT ON COLLATION TEST_COLLATE IS NULL")

    def test_04_CharacterSet(self):
        s = Schema()
        s.bind(self.con)
        c = s.character_sets.get('UTF8')
        # common properties
        self.assertEqual(c.name, 'UTF8')
        self.assertIsNone(c.description)
        self.assertListEqual(c.actions, ['alter', 'comment'])
        self.assertTrue(c.is_sys_object())
        self.assertEqual(c.get_quoted_name(), 'UTF8')
        self.assertListEqual(c.get_dependents(), [])
        self.assertListEqual(c.get_dependencies(), [])
        if self.version == FB30:
            self.assertEqual(c.security_class, 'SQL$166')
        else:
            self.assertEqual(c.security_class, 'SQL$186')
        self.assertEqual(c.owner_name, 'SYSDBA')
        #
        self.assertEqual(c.id, 4)
        self.assertEqual(c.bytes_per_character, 4)
        self.assertEqual(c.default_collate.name, 'UTF8')
        self.assertListEqual([x.name for x in c.collations],
                             ['UTF8', 'UCS_BASIC', 'UNICODE', 'UNICODE_CI', 'UNICODE_CI_AI'])
        #
        self.assertEqual(c.get_sql_for('alter', collation='UCS_BASIC'),
                         "ALTER CHARACTER SET UTF8 SET DEFAULT COLLATION UCS_BASIC")
        with self.assertRaises(ValueError) as cm:
            c.get_sql_for('alter', badparam='UCS_BASIC')
        self.assertTupleEqual(cm.exception.args,
                              ("Unsupported parameter(s) 'badparam'",))
        with self.assertRaises(ValueError) as cm:
            c.get_sql_for('alter')
        self.assertTupleEqual(cm.exception.args,
                              ("Missing required parameter: 'collation'.",))
        #
        self.assertEqual(c.get_sql_for('comment'),
                         'COMMENT ON CHARACTER SET UTF8 IS NULL')
        #
        self.assertEqual(c.collations.get('UCS_BASIC').name, 'UCS_BASIC')
        self.assertEqual(c.get_collation_by_id(c.collations.get('UCS_BASIC').id).name,
                         'UCS_BASIC')
    def test_05_Exception(self):
        s = Schema()
        s.bind(self.con)
        c = s.exceptions.get('UNKNOWN_EMP_ID')
        # common properties
        self.assertEqual(c.name, 'UNKNOWN_EMP_ID')
        self.assertIsNone(c.description)
        self.assertListEqual(c.actions,
                             ['comment', 'create', 'recreate', 'alter', 'create_or_alter', 'drop'])
        self.assertFalse(c.is_sys_object())
        self.assertEqual(c.get_quoted_name(), 'UNKNOWN_EMP_ID')
        d = c.get_dependents()
        self.assertEqual(len(d), 1)
        d = d[0]
        self.assertEqual(d.dependent_name, 'ADD_EMP_PROJ')
        self.assertEqual(d.dependent_type, 5)
        self.assertIsInstance(d.dependent, sm.Procedure)
        self.assertEqual(d.depended_on_name, 'UNKNOWN_EMP_ID')
        self.assertEqual(d.depended_on_type, 7)
        self.assertIsInstance(d.depended_on, sm.DatabaseException)
        self.assertListEqual(c.get_dependencies(), [])
        if self.version == FB30:
            self.assertEqual(c.security_class, 'SQL$476')
        else:
            self.assertEqual(c.security_class, 'SQL$604')
        self.assertEqual(c.owner_name, 'SYSDBA')
        #
        self.assertEqual(c.id, 1)
        self.assertEqual(c.message, "Invalid employee number or project id.")
        #
        self.assertEqual(c.get_sql_for('create'),
                         "CREATE EXCEPTION UNKNOWN_EMP_ID 'Invalid employee number or project id.'")
        self.assertEqual(c.get_sql_for('recreate'),
                         "RECREATE EXCEPTION UNKNOWN_EMP_ID 'Invalid employee number or project id.'")
        self.assertEqual(c.get_sql_for('drop'),
                         "DROP EXCEPTION UNKNOWN_EMP_ID")
        self.assertEqual(c.get_sql_for('alter', message="New message."),
                         "ALTER EXCEPTION UNKNOWN_EMP_ID 'New message.'")
        with self.assertRaises(ValueError) as cm:
            c.get_sql_for('alter', badparam="New message.")
        self.assertTupleEqual(cm.exception.args,
                              ("Unsupported parameter(s) 'badparam'",))
        with self.assertRaises(ValueError) as cm:
            c.get_sql_for('alter')
        self.assertTupleEqual(cm.exception.args,
                              ("Missing required parameter: 'message'.",))
        self.assertEqual(c.get_sql_for('create_or_alter'),
                         "CREATE OR ALTER EXCEPTION UNKNOWN_EMP_ID 'Invalid employee number or project id.'")
        self.assertEqual(c.get_sql_for('comment'),
                         "COMMENT ON EXCEPTION UNKNOWN_EMP_ID IS NULL")
    def test_06_Sequence(self):
        s = Schema()
        s.bind(self.con)
        # System generator
        c = s.all_generators.get('RDB$FIELD_NAME')
        # common properties
        self.assertEqual(c.name, 'RDB$FIELD_NAME')
        self.assertEqual(c.description, "Implicit domain name")
        self.assertListEqual(c.actions, ['comment'])
        self.assertTrue(c.is_sys_object())
        self.assertEqual(c.get_quoted_name(), 'RDB$FIELD_NAME')
        self.assertListEqual(c.get_dependents(), [])
        self.assertListEqual(c.get_dependencies(), [])
        #
        self.assertEqual(c.id, 6)
        # User generator
        c = s.all_generators.get('EMP_NO_GEN')
        # common properties
        self.assertEqual(c.name, 'EMP_NO_GEN')
        self.assertIsNone(c.description)
        self.assertListEqual(c.actions, ['comment', 'create',
                                         'alter', 'drop'])
        self.assertFalse(c.is_sys_object())
        self.assertEqual(c.get_quoted_name(), 'EMP_NO_GEN')
        d = c.get_dependents()
        self.assertEqual(len(d), 1)
        d = d[0]
        self.assertEqual(d.dependent_name, 'SET_EMP_NO')
        self.assertEqual(d.dependent_type, 2)
        self.assertIsInstance(d.dependent, sm.Trigger)
        self.assertEqual(d.depended_on_name, 'EMP_NO_GEN')
        self.assertEqual(d.depended_on_type, 14)
        self.assertIsInstance(d.depended_on, sm.Sequence)
        self.assertListEqual(c.get_dependencies(), [])
        #
        self.assertEqual(c.id, 12)
        if self.version == FB30:
            self.assertEqual(c.security_class, 'SQL$429')
        else:
            self.assertEqual(c.security_class, 'SQL$600')
        self.assertEqual(c.owner_name, 'SYSDBA')
        self.assertEqual(c.inital_value, 0)
        self.assertEqual(c.increment, 1)
        self.assertEqual(c.value, 145)
        #
        self.assertEqual(c.get_sql_for('create'), "CREATE SEQUENCE EMP_NO_GEN")
        self.assertEqual(c.get_sql_for('drop'), "DROP SEQUENCE EMP_NO_GEN")
        self.assertEqual(c.get_sql_for('alter', value=10),
                         "ALTER SEQUENCE EMP_NO_GEN RESTART WITH 10")
        with self.assertRaises(ValueError) as cm:
            c.get_sql_for('alter', badparam=10)
        self.assertTupleEqual(cm.exception.args,
                              ("Unsupported parameter(s) 'badparam'",))
        self.assertEqual(c.get_sql_for('comment'),
                         "COMMENT ON SEQUENCE EMP_NO_GEN IS NULL")
        c.schema.opt_generator_keyword = 'GENERATOR'
        self.assertEqual(c.get_sql_for('comment'),
                         "COMMENT ON GENERATOR EMP_NO_GEN IS NULL")
    def test_07_TableColumn(self):
        s = Schema()
        s.bind(self.con)
        # System column
        c = s.all_tables.get('RDB$PAGES').columns.get('RDB$PAGE_NUMBER')
        # common properties
        self.assertEqual(c.name, 'RDB$PAGE_NUMBER')
        self.assertIsNone(c.description)
        self.assertListEqual(c.actions, ['comment'])
        self.assertTrue(c.is_sys_object())
        self.assertEqual(c.get_quoted_name(), 'RDB$PAGE_NUMBER')
        self.assertListEqual(c.get_dependents(), [])
        self.assertListEqual(c.get_dependencies(), [])
        self.assertFalse(c.is_identity())
        self.assertIsNone(c.generator)
        # User column
        c = s.all_tables.get('DEPARTMENT').columns.get('PHONE_NO')
        # common properties
        self.assertEqual(c.name, 'PHONE_NO')
        self.assertIsNone(c.description)
        self.assertListEqual(c.actions, ['comment', 'alter', 'drop'])
        self.assertFalse(c.is_sys_object())
        self.assertEqual(c.get_quoted_name(), 'PHONE_NO')
        d = c.get_dependents()
        self.assertEqual(len(d), 1)
        d = d[0]
        self.assertEqual(d.dependent_name, 'PHONE_LIST')
        self.assertEqual(d.dependent_type, 1)
        self.assertIsInstance(d.dependent, sm.View)
        self.assertEqual(d.depended_on_name, 'DEPARTMENT')
        self.assertEqual(d.depended_on_type, 0)
        self.assertIsInstance(d.depended_on, sm.TableColumn)
        self.assertListEqual(c.get_dependencies(), [])
        #
        self.assertEqual(c.table.name, 'DEPARTMENT')
        self.assertEqual(c.domain.name, 'PHONENUMBER')
        self.assertEqual(c.position, 6)
        self.assertIsNone(c.security_class)
        self.assertEqual(c.default, "'555-1234'")
        self.assertIsNone(c.collation)
        self.assertEqual(c.datatype, 'VARCHAR(20)')
        #
        self.assertTrue(c.is_nullable())
        self.assertFalse(c.is_computed())
        self.assertTrue(c.is_domain_based())
        self.assertTrue(c.has_default())
        self.assertIsNone(c.get_computedby())
        #
        self.assertEqual(c.get_sql_for('comment'),
                         "COMMENT ON COLUMN DEPARTMENT.PHONE_NO IS NULL")
        self.assertEqual(c.get_sql_for('drop'),
                         "ALTER TABLE DEPARTMENT DROP PHONE_NO")
        self.assertEqual(c.get_sql_for('alter', name='NewName'),
                         'ALTER TABLE DEPARTMENT ALTER COLUMN PHONE_NO TO "NewName"')
        self.assertEqual(c.get_sql_for('alter', position=2),
                         "ALTER TABLE DEPARTMENT ALTER COLUMN PHONE_NO POSITION 2")
        self.assertEqual(c.get_sql_for('alter', datatype='VARCHAR(25)'),
                         "ALTER TABLE DEPARTMENT ALTER COLUMN PHONE_NO TYPE VARCHAR(25)")
        with self.assertRaises(ValueError) as cm:
            c.get_sql_for('alter', badparam=10)
        self.assertTupleEqual(cm.exception.args, ("Unsupported parameter(s) 'badparam'",))
        with self.assertRaises(ValueError) as cm:
            c.get_sql_for('alter')
        self.assertTupleEqual(cm.exception.args, ("Parameter required.",))
        with self.assertRaises(ValueError) as cm:
            c.get_sql_for('alter', expression='(1+1)')
        self.assertTupleEqual(cm.exception.args,
                              ("Change from persistent column to computed is not allowed.",))
        # Computed column
        c = s.all_tables.get('EMPLOYEE').columns.get('FULL_NAME')
        self.assertTrue(c.is_nullable())
        self.assertTrue(c.is_computed())
        self.assertFalse(c.is_domain_based())
        self.assertFalse(c.has_default())
        self.assertEqual(c.get_computedby(), "(last_name || ', ' || first_name)")
        self.assertEqual(c.datatype, 'VARCHAR(37)')
        #
        self.assertEqual(c.get_sql_for('alter', datatype='VARCHAR(50)',
                                       expression="(first_name || ', ' || last_name)"),
                         "ALTER TABLE EMPLOYEE ALTER COLUMN FULL_NAME TYPE VARCHAR(50) " \
                         "COMPUTED BY (first_name || ', ' || last_name)")

        with self.assertRaises(ValueError) as cm:
            c.get_sql_for('alter', datatype='VARCHAR(50)')
        self.assertTupleEqual(cm.exception.args,
                              ("Change from computed column to persistent is not allowed.",))
        # Array column
        c = s.all_tables.get('AR').columns.get('C2')
        self.assertEqual(c.datatype, 'INTEGER[4, 0:3, 2]')
        # Identity column
        c = s.all_tables.get('T5').columns.get('ID')
        self.assertTrue(c.is_identity())
        self.assertTrue(c.generator.is_identity())
        self.assertEqual(c.identity_type, 1)
        #
        self.assertEqual(c.get_sql_for('alter', restart=None),
                         "ALTER TABLE T5 ALTER COLUMN ID RESTART")
        self.assertEqual(c.get_sql_for('alter', restart=100),
                         "ALTER TABLE T5 ALTER COLUMN ID RESTART WITH 100")
    def test_08_Index(self):
        s = Schema()
        s.bind(self.con)
        # System index
        c = s.all_indices.get('RDB$INDEX_0')
        # common properties
        self.assertEqual(c.name, 'RDB$INDEX_0')
        self.assertIsNone(c.description)
        self.assertListEqual(c.actions, ['activate', 'recompute', 'comment'])
        self.assertTrue(c.is_sys_object())
        self.assertEqual(c.get_quoted_name(), 'RDB$INDEX_0')
        self.assertListEqual(c.get_dependents(), [])
        self.assertListEqual(c.get_dependencies(), [])
        #
        self.assertEqual(c.table.name, 'RDB$RELATIONS')
        self.assertListEqual(c.segment_names, ['RDB$RELATION_NAME'])
        # user index
        c = s.all_indices.get('MAXSALX')
        # common properties
        self.assertEqual(c.name, 'MAXSALX')
        self.assertIsNone(c.description)
        self.assertListEqual(c.actions, ['activate', 'recompute', 'comment', 'create', 'deactivate', 'drop'])
        self.assertFalse(c.is_sys_object())
        self.assertEqual(c.get_quoted_name(), 'MAXSALX')
        self.assertListEqual(c.get_dependents(), [])
        self.assertListEqual(c.get_dependencies(), [])
        #
        self.assertEqual(c.id, 3)
        self.assertEqual(c.table.name, 'JOB')
        self.assertEqual(c.index_type, IndexType.DESCENDING)
        self.assertIsNone(c.partner_index)
        self.assertIsNone(c.expression)
        # startswith() is necessary, because Python 3 returns more precise value.
        self.assertTrue(str(c.statistics).startswith('0.0384615398943'))
        self.assertListEqual(c.segment_names, ['JOB_COUNTRY', 'MAX_SALARY'])
        self.assertEqual(len(c.segments), 2)
        for segment in c.segments:
            self.assertIsInstance(segment, sm.TableColumn)
        self.assertEqual(c.segments[0].name, 'JOB_COUNTRY')
        self.assertEqual(c.segments[1].name, 'MAX_SALARY')

        self.assertListEqual(c.segment_statistics,
                             [0.1428571492433548, 0.03846153989434242])
        self.assertIsNone(c.constraint)
        #
        self.assertFalse(c.is_expression())
        self.assertFalse(c.is_unique())
        self.assertFalse(c.is_inactive())
        self.assertFalse(c.is_enforcer())
        #
        self.assertEqual(c.get_sql_for('create'),
                         """CREATE DESCENDING INDEX MAXSALX ON JOB (JOB_COUNTRY,MAX_SALARY)""")
        self.assertEqual(c.get_sql_for('activate'), "ALTER INDEX MAXSALX ACTIVE")
        self.assertEqual(c.get_sql_for('deactivate'), "ALTER INDEX MAXSALX INACTIVE")
        self.assertEqual(c.get_sql_for('recompute'), "SET STATISTICS INDEX MAXSALX")
        self.assertEqual(c.get_sql_for('drop'), "DROP INDEX MAXSALX")
        self.assertEqual(c.get_sql_for('comment'),
                         "COMMENT ON INDEX MAXSALX IS NULL")
        # Constraint index
        c = s.all_indices.get('RDB$FOREIGN6')
        # common properties
        self.assertEqual(c.name, 'RDB$FOREIGN6')
        self.assertTrue(c.is_sys_object())
        self.assertTrue(c.is_enforcer())
        self.assertEqual(c.partner_index.name, 'RDB$PRIMARY5')
        self.assertEqual(c.constraint.name, 'INTEG_17')
    def test_09_ViewColumn(self):
        s = Schema()
        s.bind(self.con)
        c = s.all_views.get('PHONE_LIST').columns.get('LAST_NAME')
        # common properties
        self.assertEqual(c.name, 'LAST_NAME')
        self.assertIsNone(c.description)
        self.assertListEqual(c.actions, ['comment'])
        self.assertFalse(c.is_sys_object())
        self.assertEqual(c.get_quoted_name(), 'LAST_NAME')
        self.assertListEqual(c.get_dependents(), [])
        d = c.get_dependencies()
        self.assertEqual(len(d), 1)
        d = d[0]
        self.assertEqual(d.dependent_name, 'PHONE_LIST')
        self.assertEqual(d.dependent_type, 1)
        self.assertIsInstance(d.dependent, sm.View)
        self.assertEqual(d.field_name, 'LAST_NAME')
        self.assertEqual(d.depended_on_name, 'EMPLOYEE')
        self.assertEqual(d.depended_on_type, 0)
        self.assertIsInstance(d.depended_on, sm.TableColumn)
        self.assertEqual(d.depended_on.name, 'LAST_NAME')
        self.assertEqual(d.depended_on.table.name, 'EMPLOYEE')
        #
        self.assertEqual(c.view.name, 'PHONE_LIST')
        self.assertEqual(c.base_field.name, 'LAST_NAME')
        self.assertEqual(c.base_field.table.name, 'EMPLOYEE')
        self.assertEqual(c.domain.name, 'LASTNAME')
        self.assertEqual(c.position, 2)
        self.assertIsNone(c.security_class)
        self.assertEqual(c.collation.name, 'NONE')
        self.assertEqual(c.datatype, 'VARCHAR(20)')
        #
        self.assertTrue(c.is_nullable())
        #
        self.assertEqual(c.get_sql_for('comment'),
                         "COMMENT ON COLUMN PHONE_LIST.LAST_NAME IS NULL")
    def test_10_Domain(self):
        s = Schema()
        s.bind(self.con)
        # System domain
        c = s.all_domains.get('RDB$6')
        # common properties
        self.assertEqual(c.name, 'RDB$6')
        self.assertIsNone(c.description)
        self.assertListEqual(c.actions, ['comment'])
        self.assertTrue(c.is_sys_object())
        self.assertEqual(c.get_quoted_name(), 'RDB$6')
        self.assertListEqual(c.get_dependents(), [])
        self.assertListEqual(c.get_dependencies(), [])
        if self.version == FB30:
            self.assertEqual(c.security_class, 'SQL$439')
        else:
            self.assertEqual(c.security_class, 'SQL$460')
        self.assertEqual(c.owner_name, 'SYSDBA')
        # User domain
        c = s.all_domains.get('PRODTYPE')
        # common properties
        self.assertEqual(c.name, 'PRODTYPE')
        self.assertIsNone(c.description)
        self.assertListEqual(c.actions, ['comment', 'create',
                                         'alter', 'drop'])
        self.assertFalse(c.is_sys_object())
        self.assertEqual(c.get_quoted_name(), 'PRODTYPE')
        self.assertListEqual(c.get_dependents(), [])
        self.assertListEqual(c.get_dependencies(), [])
        #
        self.assertIsNone(c.expression)
        self.assertEqual(c.validation,
                         "CHECK (VALUE IN ('software', 'hardware', 'other', 'N/A'))")
        self.assertEqual(c.default, "'software'")
        self.assertEqual(c.length, 12)
        self.assertEqual(c.scale, 0)
        self.assertEqual(c.field_type, 37)
        self.assertEqual(c.sub_type, 0)
        self.assertIsNone(c.segment_length)
        self.assertIsNone(c.external_length)
        self.assertIsNone(c.external_scale)
        self.assertIsNone(c.external_type)
        self.assertListEqual(c.dimensions, [])
        self.assertEqual(c.character_length, 12)
        self.assertEqual(c.collation.name, 'NONE')
        self.assertEqual(c.character_set.name, 'NONE')
        self.assertIsNone(c.precision)
        self.assertEqual(c.datatype, 'VARCHAR(12)')
        #
        self.assertFalse(c.is_nullable())
        self.assertFalse(c.is_computed())
        self.assertTrue(c.is_validated())
        self.assertFalse(c.is_array())
        self.assertTrue(c.has_default())
        #
        self.assertEqual(c.get_sql_for('create'),
                         "CREATE DOMAIN PRODTYPE AS VARCHAR(12) DEFAULT 'software' " \
                         "NOT NULL CHECK (VALUE IN ('software', 'hardware', 'other', 'N/A'))")
        self.assertEqual(c.get_sql_for('drop'), "DROP DOMAIN PRODTYPE")
        self.assertEqual(c.get_sql_for('alter', name='New_name'),
                         'ALTER DOMAIN PRODTYPE TO "New_name"')
        self.assertEqual(c.get_sql_for('alter', default="'New_default'"),
                         "ALTER DOMAIN PRODTYPE SET DEFAULT 'New_default'")
        self.assertEqual(c.get_sql_for('alter', check="VALUE STARTS WITH 'X'"),
                         "ALTER DOMAIN PRODTYPE ADD CHECK (VALUE STARTS WITH 'X')")
        self.assertEqual(c.get_sql_for('alter', datatype='VARCHAR(30)'),
                         "ALTER DOMAIN PRODTYPE TYPE VARCHAR(30)")
        with self.assertRaises(ValueError) as cm:
            c.get_sql_for('alter', badparam=10)
        self.assertTupleEqual(cm.exception.args,
                              ("Unsupported parameter(s) 'badparam'",))
        with self.assertRaises(ValueError) as cm:
            c.get_sql_for('alter')
        self.assertTupleEqual(cm.exception.args, ("Parameter required.",))
        # Domain with quoted name
        c = s.all_domains.get('FIRSTNAME')
        self.assertEqual(c.name, 'FIRSTNAME')
        if self.version == FB30:
            self.assertEqual(c.get_quoted_name(), '"FIRSTNAME"')
        else:
            self.assertEqual(c.get_quoted_name(), 'FIRSTNAME')
        if self.version == FB30:
            self.assertEqual(c.get_sql_for('create'),
                             'CREATE DOMAIN "FIRSTNAME" AS VARCHAR(15)')
            self.assertEqual(c.get_sql_for('comment'),
                             'COMMENT ON DOMAIN "FIRSTNAME" IS NULL')
        else:
            self.assertEqual(c.get_sql_for('create'),
                             'CREATE DOMAIN FIRSTNAME AS VARCHAR(15)')
            self.assertEqual(c.get_sql_for('comment'),
                             'COMMENT ON DOMAIN FIRSTNAME IS NULL')
    def test_11_Dependency(self):
        s = Schema()
        s.bind(self.con)
        l = s.all_tables.get('DEPARTMENT').get_dependents()
        self.assertEqual(len(l), 18)
        c = l[3]
        # common properties
        self.assertIsNone(c.name)
        self.assertIsNone(c.description)
        self.assertListEqual(c.actions, [])
        self.assertTrue(c.is_sys_object())
        self.assertIsNone(c.get_quoted_name())
        self.assertListEqual(c.get_dependents(), [])
        self.assertListEqual(c.get_dependencies(), [])
        self.assertIsNone(c.package)
        self.assertFalse(c.is_packaged())
        #
        self.assertEqual(c.dependent_name, 'PHONE_LIST')
        self.assertEqual(c.dependent_type, 1)
        self.assertIsInstance(c.dependent, sm.View)
        self.assertEqual(c.dependent.name, 'PHONE_LIST')
        self.assertEqual(c.field_name, 'DEPT_NO')
        self.assertEqual(c.depended_on_name, 'DEPARTMENT')
        self.assertEqual(c.depended_on_type, 0)
        self.assertIsInstance(c.depended_on, sm.TableColumn)
        self.assertEqual(c.depended_on.name, 'DEPT_NO')
        #
        self.assertListEqual(c.get_dependents(), [])
        l = s.packages.get('TEST2').get_dependencies()
        self.assertEqual(len(l), 2)
        x = l[0]
        self.assertEqual(x.depended_on.name, 'FN')
        self.assertFalse(x.depended_on.is_packaged())
        x = l[1]
        self.assertEqual(x.depended_on.name, 'F')
        self.assertTrue(x.depended_on.is_packaged())
        self.assertIsInstance(x.package, sm.Package)
    def test_12_Constraint(self):
        s = Schema()
        s.bind(self.con)
        # Common / PRIMARY KEY
        c = s.all_tables.get('CUSTOMER').primary_key
        # common properties
        self.assertEqual(c.name, 'INTEG_60')
        self.assertIsNone(c.description)
        self.assertListEqual(c.actions, ['create', 'drop'])
        self.assertFalse(c.is_sys_object())
        self.assertEqual(c.get_quoted_name(), 'INTEG_60')
        self.assertListEqual(c.get_dependents(), [])
        self.assertListEqual(c.get_dependencies(), [])
        #
        self.assertEqual(c.constraint_type, ConstraintType.PRIMARY_KEY)
        self.assertEqual(c.table.name, 'CUSTOMER')
        self.assertEqual(c.index.name, 'RDB$PRIMARY22')
        self.assertListEqual(c.trigger_names, [])
        self.assertListEqual(c.triggers, [])
        self.assertIsNone(c.column_name)
        self.assertIsNone(c.partner_constraint)
        self.assertIsNone(c.match_option)
        self.assertIsNone(c.update_rule)
        self.assertIsNone(c.delete_rule)
        #
        self.assertFalse(c.is_not_null())
        self.assertTrue(c.is_pkey())
        self.assertFalse(c.is_fkey())
        self.assertFalse(c.is_unique())
        self.assertFalse(c.is_check())
        self.assertFalse(c.is_deferrable())
        self.assertFalse(c.is_deferred())
        #
        self.assertEqual(c.get_sql_for('create'),
                         "ALTER TABLE CUSTOMER ADD PRIMARY KEY (CUST_NO)")
        self.assertEqual(c.get_sql_for('drop'),
                         "ALTER TABLE CUSTOMER DROP CONSTRAINT INTEG_60")
        # FOREIGN KEY
        c = s.all_tables.get('CUSTOMER').foreign_keys[0]
        #
        self.assertListEqual(c.actions, ['create', 'drop'])
        self.assertEqual(c.constraint_type, ConstraintType.FOREIGN_KEY)
        self.assertEqual(c.table.name, 'CUSTOMER')
        self.assertEqual(c.index.name, 'RDB$FOREIGN23')
        self.assertListEqual(c.trigger_names, [])
        self.assertListEqual(c.triggers, [])
        self.assertIsNone(c.column_name)
        self.assertEqual(c.partner_constraint.name, 'INTEG_2')
        self.assertEqual(c.match_option, 'FULL')
        self.assertEqual(c.update_rule, 'RESTRICT')
        self.assertEqual(c.delete_rule, 'RESTRICT')
        #
        self.assertFalse(c.is_not_null())
        self.assertFalse(c.is_pkey())
        self.assertTrue(c.is_fkey())
        self.assertFalse(c.is_unique())
        self.assertFalse(c.is_check())
        #
        self.assertEqual(c.get_sql_for('create'),
                         """ALTER TABLE CUSTOMER ADD FOREIGN KEY (COUNTRY)
  REFERENCES COUNTRY (COUNTRY)""")
        # CHECK
        c = s.constraints.get('INTEG_59')
        #
        self.assertListEqual(c.actions, ['create', 'drop'])
        self.assertEqual(c.constraint_type, ConstraintType.CHECK)
        self.assertEqual(c.table.name, 'CUSTOMER')
        self.assertIsNone(c.index)
        self.assertListEqual(c.trigger_names, ['CHECK_9', 'CHECK_10'])
        self.assertEqual(c.triggers[0].name, 'CHECK_9')
        self.assertEqual(c.triggers[1].name, 'CHECK_10')
        self.assertIsNone(c.column_name)
        self.assertIsNone(c.partner_constraint)
        self.assertIsNone(c.match_option)
        self.assertIsNone(c.update_rule)
        self.assertIsNone(c.delete_rule)
        #
        self.assertFalse(c.is_not_null())
        self.assertFalse(c.is_pkey())
        self.assertFalse(c.is_fkey())
        self.assertFalse(c.is_unique())
        self.assertTrue(c.is_check())
        #
        self.assertEqual(c.get_sql_for('create'),
                         "ALTER TABLE CUSTOMER ADD CHECK (on_hold IS NULL OR on_hold = '*')")
        # UNIQUE
        c = s.constraints.get('INTEG_15')
        #
        self.assertListEqual(c.actions, ['create', 'drop'])
        self.assertEqual(c.constraint_type, ConstraintType.UNIQUE)
        self.assertEqual(c.table.name, 'DEPARTMENT')
        self.assertEqual(c.index.name, 'RDB$4')
        self.assertListEqual(c.trigger_names, [])
        self.assertListEqual(c.triggers, [])
        self.assertIsNone(c.column_name)
        self.assertIsNone(c.partner_constraint)
        self.assertIsNone(c.match_option)
        self.assertIsNone(c.update_rule)
        self.assertIsNone(c.delete_rule)
        #
        self.assertFalse(c.is_not_null())
        self.assertFalse(c.is_pkey())
        self.assertFalse(c.is_fkey())
        self.assertTrue(c.is_unique())
        self.assertFalse(c.is_check())
        #
        self.assertEqual(c.get_sql_for('create'),
                         "ALTER TABLE DEPARTMENT ADD UNIQUE (DEPARTMENT)")
        # NOT NULL
        c = s.constraints.get('INTEG_13')
        #
        self.assertListEqual(c.actions, [])
        self.assertEqual(c.constraint_type, ConstraintType.NOT_NULL)
        self.assertEqual(c.table.name, 'DEPARTMENT')
        self.assertIsNone(c.index)
        self.assertListEqual(c.trigger_names, [])
        self.assertListEqual(c.triggers, [])
        self.assertEqual(c.column_name, 'DEPT_NO')
        self.assertIsNone(c.partner_constraint)
        self.assertIsNone(c.match_option)
        self.assertIsNone(c.update_rule)
        self.assertIsNone(c.delete_rule)
        #
        self.assertTrue(c.is_not_null())
        self.assertFalse(c.is_pkey())
        self.assertFalse(c.is_fkey())
        self.assertFalse(c.is_unique())
        self.assertFalse(c.is_check())
    def test_13_Table(self):
        s = Schema()
        s.bind(self.con)
        # System table
        c = s.all_tables.get('RDB$PAGES')
        # common properties
        self.assertEqual(c.name, 'RDB$PAGES')
        self.assertIsNone(c.description)
        self.assertListEqual(c.actions, ['comment'])
        self.assertTrue(c.is_sys_object())
        self.assertEqual(c.get_quoted_name(), 'RDB$PAGES')
        self.assertListEqual(c.get_dependents(), [])
        self.assertListEqual(c.get_dependencies(), [])
        # User table
        c = s.all_tables.get('EMPLOYEE')
        # common properties
        self.assertEqual(c.name, 'EMPLOYEE')
        self.assertIsNone(c.description)
        self.assertListEqual(c.actions, ['comment', 'create',
                                         'recreate', 'drop'])
        self.assertFalse(c.is_sys_object())
        self.assertEqual(c.get_quoted_name(), 'EMPLOYEE')
        d = c.get_dependents()
        if self.version == FB30:
            self.assertListEqual([(x.dependent_name, x.dependent_type) for x in d],
                                 [('SAVE_SALARY_CHANGE', 2), ('SAVE_SALARY_CHANGE', 2), ('CHECK_3', 2),
                                  ('CHECK_3', 2), ('CHECK_3', 2), ('CHECK_3', 2), ('CHECK_4', 2),
                                  ('CHECK_4', 2), ('CHECK_4', 2), ('CHECK_4', 2), ('PHONE_LIST', 1),
                                  ('PHONE_LIST', 1), ('PHONE_LIST', 1), ('PHONE_LIST', 1), ('PHONE_LIST', 1),
                                  ('PHONE_LIST', 1), ('DELETE_EMPLOYEE', 5), ('DELETE_EMPLOYEE', 5),
                                  ('ORG_CHART', 5), ('ORG_CHART', 5), ('ORG_CHART', 5), ('ORG_CHART', 5),
                                  ('ORG_CHART', 5), ('RDB$9', 3), ('RDB$9', 3), ('SET_EMP_NO', 2)])
        else:
            self.assertListEqual([(x.dependent_name, x.dependent_type) for x in d],
                                 [('CHECK_3', ObjectType.TRIGGER),
                                  ('CHECK_3', ObjectType.TRIGGER),
                                  ('CHECK_3', ObjectType.TRIGGER),
                                  ('CHECK_3', ObjectType.TRIGGER),
                                  ('CHECK_4', ObjectType.TRIGGER),
                                  ('CHECK_4', ObjectType.TRIGGER),
                                  ('CHECK_4', ObjectType.TRIGGER),
                                  ('CHECK_4', ObjectType.TRIGGER),
                                  ('SET_EMP_NO', ObjectType.TRIGGER),
                                  ('SAVE_SALARY_CHANGE', ObjectType.TRIGGER),
                                  ('SAVE_SALARY_CHANGE', ObjectType.TRIGGER),
                                  ('RDB$9', ObjectType.DOMAIN),
                                  ('RDB$9', ObjectType.DOMAIN),
                                  ('PHONE_LIST', ObjectType.VIEW),
                                  ('PHONE_LIST', ObjectType.VIEW),
                                  ('PHONE_LIST', ObjectType.VIEW),
                                  ('PHONE_LIST', ObjectType.VIEW),
                                  ('PHONE_LIST', ObjectType.VIEW),
                                  ('PHONE_LIST', ObjectType.VIEW),
                                  ('ORG_CHART', ObjectType.PROCEDURE),
                                  ('ORG_CHART', ObjectType.PROCEDURE),
                                  ('ORG_CHART', ObjectType.PROCEDURE),
                                  ('ORG_CHART', ObjectType.PROCEDURE),
                                  ('ORG_CHART', ObjectType.PROCEDURE),
                                  ('DELETE_EMPLOYEE', ObjectType.PROCEDURE),
                                  ('DELETE_EMPLOYEE', ObjectType.PROCEDURE)])
        self.assertListEqual(c.get_dependencies(), [])
        #
        self.assertEqual(c.id, 131)
        self.assertEqual(c.dbkey_length, 8)
        if self.version == FB30:
            self.assertEqual(c.format, 1)
            self.assertEqual(c.security_class, 'SQL$440')
            self.assertEqual(c.default_class, 'SQL$DEFAULT54')
        else:
            self.assertEqual(c.format, 2)
            self.assertEqual(c.security_class, 'SQL$586')
            self.assertEqual(c.default_class, 'SQL$DEFAULT58')
        self.assertEqual(c.table_type, RelationType.PERSISTENT)
        self.assertIsNone(c.external_file)
        self.assertEqual(c.owner_name, 'SYSDBA')
        self.assertEqual(c.flags, 1)
        self.assertEqual(c.primary_key.name, 'INTEG_27')
        self.assertListEqual([x.name for x in c.foreign_keys],
                             ['INTEG_28', 'INTEG_29'])
        self.assertListEqual([x.name for x in c.columns],
                             ['EMP_NO', 'FIRST_NAME', 'LAST_NAME', 'PHONE_EXT',
                              'HIRE_DATE', 'DEPT_NO', 'JOB_CODE', 'JOB_GRADE',
                              'JOB_COUNTRY', 'SALARY', 'FULL_NAME'])
        self.assertListEqual([x.name for x in c.constraints],
                             ['INTEG_18', 'INTEG_19', 'INTEG_20', 'INTEG_21',
                              'INTEG_22', 'INTEG_23', 'INTEG_24', 'INTEG_25',
                              'INTEG_26', 'INTEG_27', 'INTEG_28', 'INTEG_29',
                              'INTEG_30'])
        self.assertListEqual([x.name for x in c.indices],
                             ['RDB$PRIMARY7', 'RDB$FOREIGN8', 'RDB$FOREIGN9', 'NAMEX'])
        self.assertListEqual([x.name for x in c.triggers],
                             ['SET_EMP_NO', 'SAVE_SALARY_CHANGE'])
        #
        self.assertEqual(c.columns.get('EMP_NO').name, 'EMP_NO')
        self.assertFalse(c.is_gtt())
        self.assertTrue(c.is_persistent())
        self.assertFalse(c.is_external())
        self.assertTrue(c.has_pkey())
        self.assertTrue(c.has_fkey())
        #
        if self.version == FB30:
            self.assertEqual(c.get_sql_for('create'), """CREATE TABLE EMPLOYEE (
  EMP_NO EMPNO NOT NULL,
  FIRST_NAME "FIRSTNAME" NOT NULL,
  LAST_NAME "LASTNAME" NOT NULL,
  PHONE_EXT VARCHAR(4),
  HIRE_DATE TIMESTAMP DEFAULT 'NOW' NOT NULL,
  DEPT_NO DEPTNO NOT NULL,
  JOB_CODE JOBCODE NOT NULL,
  JOB_GRADE JOBGRADE NOT NULL,
  JOB_COUNTRY COUNTRYNAME NOT NULL,
  SALARY SALARY NOT NULL,
  FULL_NAME COMPUTED BY (last_name || ', ' || first_name),
  PRIMARY KEY (EMP_NO)
)""")
            self.assertEqual(c.get_sql_for('create', no_pk=True), """CREATE TABLE EMPLOYEE (
  EMP_NO EMPNO NOT NULL,
  FIRST_NAME "FIRSTNAME" NOT NULL,
  LAST_NAME "LASTNAME" NOT NULL,
  PHONE_EXT VARCHAR(4),
  HIRE_DATE TIMESTAMP DEFAULT 'NOW' NOT NULL,
  DEPT_NO DEPTNO NOT NULL,
  JOB_CODE JOBCODE NOT NULL,
  JOB_GRADE JOBGRADE NOT NULL,
  JOB_COUNTRY COUNTRYNAME NOT NULL,
  SALARY SALARY NOT NULL,
  FULL_NAME COMPUTED BY (last_name || ', ' || first_name)
)""")
            self.assertEqual(c.get_sql_for('recreate'), """RECREATE TABLE EMPLOYEE (
  EMP_NO EMPNO NOT NULL,
  FIRST_NAME "FIRSTNAME" NOT NULL,
  LAST_NAME "LASTNAME" NOT NULL,
  PHONE_EXT VARCHAR(4),
  HIRE_DATE TIMESTAMP DEFAULT 'NOW' NOT NULL,
  DEPT_NO DEPTNO NOT NULL,
  JOB_CODE JOBCODE NOT NULL,
  JOB_GRADE JOBGRADE NOT NULL,
  JOB_COUNTRY COUNTRYNAME NOT NULL,
  SALARY SALARY NOT NULL,
  FULL_NAME COMPUTED BY (last_name || ', ' || first_name),
  PRIMARY KEY (EMP_NO)
)""")
        else:
            self.assertEqual(c.get_sql_for('create'), """CREATE TABLE EMPLOYEE (
  EMP_NO EMPNO NOT NULL,
  FIRST_NAME FIRSTNAME NOT NULL,
  LAST_NAME LASTNAME NOT NULL,
  PHONE_EXT VARCHAR(4),
  HIRE_DATE TIMESTAMP DEFAULT 'NOW' NOT NULL,
  DEPT_NO DEPTNO NOT NULL,
  JOB_CODE JOBCODE NOT NULL,
  JOB_GRADE JOBGRADE NOT NULL,
  JOB_COUNTRY COUNTRYNAME NOT NULL,
  SALARY SALARY NOT NULL,
  FULL_NAME COMPUTED BY (last_name || ', ' || first_name),
  PRIMARY KEY (EMP_NO)
)""")
            self.assertEqual(c.get_sql_for('create', no_pk=True), """CREATE TABLE EMPLOYEE (
  EMP_NO EMPNO NOT NULL,
  FIRST_NAME FIRSTNAME NOT NULL,
  LAST_NAME LASTNAME NOT NULL,
  PHONE_EXT VARCHAR(4),
  HIRE_DATE TIMESTAMP DEFAULT 'NOW' NOT NULL,
  DEPT_NO DEPTNO NOT NULL,
  JOB_CODE JOBCODE NOT NULL,
  JOB_GRADE JOBGRADE NOT NULL,
  JOB_COUNTRY COUNTRYNAME NOT NULL,
  SALARY SALARY NOT NULL,
  FULL_NAME COMPUTED BY (last_name || ', ' || first_name)
)""")
            self.assertEqual(c.get_sql_for('recreate'), """RECREATE TABLE EMPLOYEE (
  EMP_NO EMPNO NOT NULL,
  FIRST_NAME FIRSTNAME NOT NULL,
  LAST_NAME LASTNAME NOT NULL,
  PHONE_EXT VARCHAR(4),
  HIRE_DATE TIMESTAMP DEFAULT 'NOW' NOT NULL,
  DEPT_NO DEPTNO NOT NULL,
  JOB_CODE JOBCODE NOT NULL,
  JOB_GRADE JOBGRADE NOT NULL,
  JOB_COUNTRY COUNTRYNAME NOT NULL,
  SALARY SALARY NOT NULL,
  FULL_NAME COMPUTED BY (last_name || ', ' || first_name),
  PRIMARY KEY (EMP_NO)
)""")
        self.assertEqual(c.get_sql_for('drop'), "DROP TABLE EMPLOYEE")
        self.assertEqual(c.get_sql_for('comment'),
                         'COMMENT ON TABLE EMPLOYEE IS NULL')
        # Identity colums
        c = s.all_tables.get('T5')
        self.assertEqual(c.get_sql_for('create'), """CREATE TABLE T5 (
  ID NUMERIC(10, 0) GENERATED BY DEFAULT AS IDENTITY,
  C1 VARCHAR(15),
  UQ BIGINT GENERATED BY DEFAULT AS IDENTITY (START WITH 100),
  PRIMARY KEY (ID)
)""")

    def test_14_View(self):
        s = Schema()
        s.bind(self.con)
        # User view
        c = s.all_views.get('PHONE_LIST')
        # common properties
        self.assertEqual(c.name, 'PHONE_LIST')
        self.assertIsNone(c.description)
        self.assertListEqual(c.actions, ['comment', 'create',
                                         'recreate', 'alter',
                                         'create_or_alter', 'drop'])
        self.assertFalse(c.is_sys_object())
        self.assertEqual(c.get_quoted_name(), 'PHONE_LIST')
        self.assertListEqual(c.get_dependents(), [])
        d = c.get_dependencies()
        if self.version == FB30:
            self.assertListEqual([(x.depended_on_name, x.field_name, x.depended_on_type) for x in d],
                                 [('DEPARTMENT', 'DEPT_NO', 0), ('EMPLOYEE', 'DEPT_NO', 0),
                                  ('DEPARTMENT', None, 0), ('EMPLOYEE', None, 0), ('EMPLOYEE', 'EMP_NO', 0),
                                  ('EMPLOYEE', 'FIRST_NAME', 0), ('EMPLOYEE', 'LAST_NAME', 0),
                                  ('EMPLOYEE', 'PHONE_EXT', 0), ('DEPARTMENT', 'LOCATION', 0),
                                  ('DEPARTMENT', 'PHONE_NO', 0)])
            self.assertEqual(c.id, 132)
            self.assertEqual(c.security_class, 'SQL$444')
            self.assertEqual(c.default_class, 'SQL$DEFAULT55')
        else:
            self.assertListEqual([(x.depended_on_name, x.field_name, x.depended_on_type) for x in d],
                                 [('DEPARTMENT', 'DEPT_NO', 0), ('EMPLOYEE', 'DEPT_NO', 0),
                                  ('DEPARTMENT', None, 0), ('EMPLOYEE', None, 0),
                                  ('EMPLOYEE', 'EMP_NO', 0), ('EMPLOYEE', 'LAST_NAME', 0),
                                  ('EMPLOYEE', 'PHONE_EXT', 0), ('DEPARTMENT', 'PHONE_NO', 0),
                                  ('EMPLOYEE', 'FIRST_NAME', 0), ('DEPARTMENT', 'LOCATION', 0)])
            self.assertEqual(c.id, 144)
            self.assertEqual(c.security_class, 'SQL$587')
            self.assertEqual(c.default_class, 'SQL$DEFAULT71')
        #
        self.assertEqual(c.sql, """SELECT
    emp_no, first_name, last_name, phone_ext, location, phone_no
    FROM employee, department
    WHERE employee.dept_no = department.dept_no""")
        self.assertEqual(c.dbkey_length, 16)
        self.assertEqual(c.format, 1)
        self.assertEqual(c.owner_name, 'SYSDBA')
        self.assertEqual(c.flags, 1)
        self.assertListEqual([x.name for x in c.columns], ['EMP_NO', 'FIRST_NAME',
                                                           'LAST_NAME', 'PHONE_EXT',
                                                           'LOCATION', 'PHONE_NO'])
        self.assertListEqual(c.triggers, [])
        #
        self.assertEqual(c.columns.get('LAST_NAME').name, 'LAST_NAME')
        self.assertFalse(c.has_checkoption())
        #
        self.assertEqual(c.get_sql_for('create'),
                         """CREATE VIEW PHONE_LIST (EMP_NO,FIRST_NAME,LAST_NAME,PHONE_EXT,LOCATION,PHONE_NO)
   AS
     SELECT
    emp_no, first_name, last_name, phone_ext, location, phone_no
    FROM employee, department
    WHERE employee.dept_no = department.dept_no""")
        self.assertEqual(c.get_sql_for('recreate'),
                         """RECREATE VIEW PHONE_LIST (EMP_NO,FIRST_NAME,LAST_NAME,PHONE_EXT,LOCATION,PHONE_NO)
   AS
     SELECT
    emp_no, first_name, last_name, phone_ext, location, phone_no
    FROM employee, department
    WHERE employee.dept_no = department.dept_no""")
        self.assertEqual(c.get_sql_for('drop'), "DROP VIEW PHONE_LIST")
        self.assertEqual(c.get_sql_for('alter', query='select * from country'),
                         "ALTER VIEW PHONE_LIST \n   AS\n     select * from country")
        self.assertEqual(c.get_sql_for('alter', columns='country,currency',
                                       query='select * from country'),
                         "ALTER VIEW PHONE_LIST (country,currency)\n   AS\n     select * from country")
        self.assertEqual(c.get_sql_for('alter', columns='country,currency',
                                       query='select * from country', check=True),
                         "ALTER VIEW PHONE_LIST (country,currency)\n   AS\n     select * from country\n     WITH CHECK OPTION")
        self.assertEqual(c.get_sql_for('alter', columns=('country', 'currency'),
                                       query='select * from country', check=True),
                         "ALTER VIEW PHONE_LIST (country,currency)\n   AS\n     select * from country\n     WITH CHECK OPTION")
        with self.assertRaises(ValueError) as cm:
            c.get_sql_for('alter', badparam='select * from country')
        self.assertTupleEqual(cm.exception.args,
                              ("Unsupported parameter(s) 'badparam'",))
        with self.assertRaises(ValueError) as cm:
            c.get_sql_for('alter')
        self.assertTupleEqual(cm.exception.args, ("Missing required parameter: 'query'.",))
        self.assertEqual(c.get_sql_for('create_or_alter'),
                         """CREATE OR ALTER VIEW PHONE_LIST (EMP_NO,FIRST_NAME,LAST_NAME,PHONE_EXT,LOCATION,PHONE_NO)
   AS
     SELECT
    emp_no, first_name, last_name, phone_ext, location, phone_no
    FROM employee, department
    WHERE employee.dept_no = department.dept_no""")
        self.assertEqual(c.get_sql_for('comment'),
                         'COMMENT ON VIEW PHONE_LIST IS NULL')

    def test_15_Trigger(self):
        s = Schema()
        s.bind(self.con)
        # System trigger
        c = s.all_triggers.get('RDB$TRIGGER_1')
        # common properties
        self.assertEqual(c.name, 'RDB$TRIGGER_1')
        self.assertIsNone(c.description)
        self.assertListEqual(c.actions, ['comment'])
        self.assertTrue(c.is_sys_object())
        self.assertEqual(c.get_quoted_name(), 'RDB$TRIGGER_1')
        self.assertListEqual(c.get_dependents(), [])
        self.assertListEqual(c.get_dependencies(), [])
        # User trigger
        c = s.all_triggers.get('SET_EMP_NO')
        # common properties
        self.assertEqual(c.name, 'SET_EMP_NO')
        self.assertIsNone(c.description)
        self.assertListEqual(c.actions,
                             ['comment', 'create', 'recreate', 'alter', 'create_or_alter', 'drop'])
        self.assertFalse(c.is_sys_object())
        self.assertEqual(c.get_quoted_name(), 'SET_EMP_NO')
        self.assertListEqual(c.get_dependents(), [])
        d = c.get_dependencies()
        self.assertListEqual([(x.depended_on_name, x.field_name, x.depended_on_type) for x in d],
                             [('EMPLOYEE', 'EMP_NO', 0), ('EMP_NO_GEN', None, 14)])
        #
        self.assertEqual(c.relation.name, 'EMPLOYEE')
        self.assertEqual(c.sequence, 0)
        self.assertEqual(c.trigger_type, TriggerType.DML)
        self.assertEqual(c.source,
                         "AS\nBEGIN\n    if (new.emp_no is null) then\n    new.emp_no = gen_id(emp_no_gen, 1);\nEND")
        self.assertEqual(c.flags, 1)
        #
        self.assertTrue(c.active)
        self.assertTrue(c.is_before())
        self.assertFalse(c.is_after())
        self.assertFalse(c.is_db_trigger())
        self.assertTrue(c.is_insert())
        self.assertFalse(c.is_update())
        self.assertFalse(c.is_delete())
        self.assertEqual(c.get_type_as_string(), 'BEFORE INSERT')
        #
        self.assertEqual(c.valid_blr, 1)
        self.assertIsNone(c.engine_name)
        self.assertIsNone(c.entrypoint)
        #
        self.assertEqual(c.get_sql_for('create'),
                         """CREATE TRIGGER SET_EMP_NO FOR EMPLOYEE ACTIVE
BEFORE INSERT POSITION 0
AS
BEGIN
    if (new.emp_no is null) then
    new.emp_no = gen_id(emp_no_gen, 1);
END""")
        self.assertEqual(c.get_sql_for('recreate'),
                         """RECREATE TRIGGER SET_EMP_NO FOR EMPLOYEE ACTIVE
BEFORE INSERT POSITION 0
AS
BEGIN
    if (new.emp_no is null) then
    new.emp_no = gen_id(emp_no_gen, 1);
END""")
        with self.assertRaises(ValueError) as cm:
            c.get_sql_for('alter')
        self.assertTupleEqual(cm.exception.args,
                              ("Header or body definition required.",))
        with self.assertRaises(ValueError) as cm:
            c.get_sql_for('alter', declare="DECLARE VARIABLE i integer;")
        self.assertTupleEqual(cm.exception.args,
                              ("Header or body definition required.",))
        self.assertEqual(c.get_sql_for('alter', fire_on='AFTER INSERT',
                                       active=False, sequence=0,
                                       declare='  DECLARE VARIABLE i integer;\n  DECLARE VARIABLE x integer;',
                                       code='  i = 1;\n  x = 2;'),
                         """ALTER TRIGGER SET_EMP_NO INACTIVE
  AFTER INSERT
  POSITION 0
AS
  DECLARE VARIABLE i integer;
  DECLARE VARIABLE x integer;
BEGIN
  i = 1;
  x = 2;
END""")
        self.assertEqual(c.get_sql_for('alter',
                                       declare=['DECLARE VARIABLE i integer;',
                                                'DECLARE VARIABLE x integer;'],
                                       code=['i = 1;', 'x = 2;']),
                         """ALTER TRIGGER SET_EMP_NO
AS
  DECLARE VARIABLE i integer;
  DECLARE VARIABLE x integer;
BEGIN
  i = 1;
  x = 2;
END""")
        self.assertEqual(c.get_sql_for('alter', active=False),
                         "ALTER TRIGGER SET_EMP_NO INACTIVE")
        self.assertEqual(c.get_sql_for('alter', sequence=10,
                                       code=('i = 1;', 'x = 2;')),
                         """ALTER TRIGGER SET_EMP_NO
  POSITION 10
AS
BEGIN
  i = 1;
  x = 2;
END""")
        with self.assertRaises(ValueError) as cm:
            c.get_sql_for('alter', fire_on='ON CONNECT')
        self.assertTupleEqual(cm.exception.args,
                              ("Trigger type change is not allowed.",))
        self.assertEqual(c.get_sql_for('create_or_alter'),
                         """CREATE OR ALTER TRIGGER SET_EMP_NO FOR EMPLOYEE ACTIVE
BEFORE INSERT POSITION 0
AS
BEGIN
    if (new.emp_no is null) then
    new.emp_no = gen_id(emp_no_gen, 1);
END""")
        self.assertEqual(c.get_sql_for('drop'), "DROP TRIGGER SET_EMP_NO")
        self.assertEqual(c.get_sql_for('comment'),
                         'COMMENT ON TRIGGER SET_EMP_NO IS NULL')
        # Multi-trigger
        c = s.all_triggers.get('TR_MULTI')
        #
        self.assertEqual(c.trigger_type, TriggerType.DML)
        self.assertFalse(c.is_ddl_trigger())
        self.assertFalse(c.is_db_trigger())
        self.assertTrue(c.is_insert())
        self.assertTrue(c.is_update())
        self.assertTrue(c.is_delete())
        self.assertEqual(c.get_type_as_string(),
                         'AFTER INSERT OR UPDATE OR DELETE')
        # DB trigger
        c = s.all_triggers.get('TR_CONNECT')
        #
        self.assertEqual(c.trigger_type, TriggerType.DB)
        self.assertFalse(c.is_ddl_trigger())
        self.assertTrue(c.is_db_trigger())
        self.assertFalse(c.is_insert())
        self.assertFalse(c.is_update())
        self.assertFalse(c.is_delete())
        self.assertEqual(c.get_type_as_string(), 'ON CONNECT')
        # DDL trigger
        c = s.all_triggers.get('TRIG_DDL')
        #
        self.assertEqual(c.trigger_type, TriggerType.DDL)
        self.assertTrue(c.is_ddl_trigger())
        self.assertFalse(c.is_db_trigger())
        self.assertFalse(c.is_insert())
        self.assertFalse(c.is_update())
        self.assertFalse(c.is_delete())
        self.assertEqual(c.get_type_as_string(), 'BEFORE ANY DDL STATEMENT')

    def test_16_ProcedureParameter(self):
        s = Schema()
        s.bind(self.con)
        # Input parameter
        c = s.all_procedures.get('GET_EMP_PROJ').input_params[0]
        # common properties
        self.assertEqual(c.name, 'EMP_NO')
        self.assertIsNone(c.description)
        self.assertListEqual(c.actions, ['comment'])
        self.assertFalse(c.is_sys_object())
        self.assertEqual(c.get_quoted_name(), 'EMP_NO')
        self.assertListEqual(c.get_dependents(), [])
        self.assertListEqual(c.get_dependencies(), [])
        #
        self.assertEqual(c.procedure.name, 'GET_EMP_PROJ')
        self.assertEqual(c.sequence, 0)
        self.assertEqual(c.domain.name, 'RDB$32')
        self.assertEqual(c.datatype, 'SMALLINT')
        self.assertEqual(c.type_from, TypeFrom.DATATYPE)
        self.assertIsNone(c.default)
        self.assertIsNone(c.collation)
        self.assertEqual(c.mechanism, 0)
        self.assertIsNone(c.column)
        self.assertEqual(c.parameter_type, ParameterType.INPUT)
        #
        self.assertTrue(c.is_input())
        self.assertTrue(c.is_nullable())
        self.assertFalse(c.has_default())
        self.assertEqual(c.get_sql_definition(), 'EMP_NO SMALLINT')
        # Output parameter
        c = s.all_procedures.get('GET_EMP_PROJ').output_params[0]
        # common properties
        self.assertEqual(c.name, 'PROJ_ID')
        self.assertIsNone(c.description)
        self.assertListEqual(c.actions, ['comment'])
        self.assertFalse(c.is_sys_object())
        self.assertEqual(c.get_quoted_name(), 'PROJ_ID')
        self.assertListEqual(c.get_dependents(), [])
        self.assertListEqual(c.get_dependencies(), [])
        #
        self.assertEqual(c.get_sql_for('comment'),
                         'COMMENT ON PARAMETER GET_EMP_PROJ.PROJ_ID IS NULL')
        #
        self.assertEqual(c.parameter_type, ParameterType.OUTPUT)
        self.assertFalse(c.is_input())
        self.assertEqual(c.get_sql_definition(), 'PROJ_ID CHAR(5)')
    def test_17_Procedure(self):
        s = Schema()
        s.bind(self.con)
        c = s.all_procedures.get('GET_EMP_PROJ')
        # common properties
        self.assertEqual(c.name, 'GET_EMP_PROJ')
        self.assertIsNone(c.description)
        self.assertListEqual(c.actions, ['comment', 'create',
                                         'recreate', 'alter',
                                         'create_or_alter', 'drop'])
        self.assertFalse(c.is_sys_object())
        self.assertEqual(c.get_quoted_name(), 'GET_EMP_PROJ')
        self.assertListEqual(c.get_dependents(), [])
        d = c.get_dependencies()
        self.assertListEqual([(x.depended_on_name, x.field_name, x.depended_on_type) for x in d],
                             [('EMPLOYEE_PROJECT', 'PROJ_ID', 0), ('EMPLOYEE_PROJECT', 'EMP_NO', 0),
                              ('EMPLOYEE_PROJECT', None, 0)])
        #
        if self.version == FB30:
            self.assertEqual(c.id, 1)
            self.assertEqual(c.security_class, 'SQL$473')
        else:
            self.assertEqual(c.id, 2)
            self.assertEqual(c.security_class, 'SQL$612')
        self.assertEqual(c.source, """BEGIN
	FOR SELECT proj_id
		FROM employee_project
		WHERE emp_no = :emp_no
		INTO :proj_id
	DO
		SUSPEND;
END""")
        self.assertEqual(c.owner_name, 'SYSDBA')
        self.assertListEqual([x.name for x in c.input_params], ['EMP_NO'])
        self.assertListEqual([x.name for x in c.output_params], ['PROJ_ID'])
        self.assertTrue(c.valid_blr)
        self.assertEqual(c.proc_type, 1)
        self.assertIsNone(c.engine_name)
        self.assertIsNone(c.entrypoint)
        self.assertIsNone(c.package)
        self.assertIsNone(c.privacy)
        #
        self.assertEqual(c.get_param('EMP_NO').name, 'EMP_NO')
        self.assertEqual(c.get_param('PROJ_ID').name, 'PROJ_ID')
        #
        self.assertEqual(c.get_sql_for('create'),
                         """CREATE PROCEDURE GET_EMP_PROJ (EMP_NO SMALLINT)
RETURNS (PROJ_ID CHAR(5))
AS
BEGIN
	FOR SELECT proj_id
		FROM employee_project
		WHERE emp_no = :emp_no
		INTO :proj_id
	DO
		SUSPEND;
END""")
        if self.version == FB30:
            self.assertEqual(c.get_sql_for('create', no_code=True),
                             """CREATE PROCEDURE GET_EMP_PROJ (EMP_NO SMALLINT)
RETURNS (PROJ_ID CHAR(5))
AS
BEGIN
  SUSPEND;
END""")
        else:
            self.assertEqual(c.get_sql_for('create', no_code=True),
                             """CREATE PROCEDURE GET_EMP_PROJ (EMP_NO SMALLINT)
RETURNS (PROJ_ID CHAR(5))
AS
BEGIN
  SUSPEND;
END""")
        self.assertEqual(c.get_sql_for('recreate'),
                         """RECREATE PROCEDURE GET_EMP_PROJ (EMP_NO SMALLINT)
RETURNS (PROJ_ID CHAR(5))
AS
BEGIN
	FOR SELECT proj_id
		FROM employee_project
		WHERE emp_no = :emp_no
		INTO :proj_id
	DO
		SUSPEND;
END""")
        if self.version == FB30:
            self.assertEqual(c.get_sql_for('recreate', no_code=True),
                             """RECREATE PROCEDURE GET_EMP_PROJ (EMP_NO SMALLINT)
RETURNS (PROJ_ID CHAR(5))
AS
BEGIN
  SUSPEND;
END""")
        else:
            self.assertEqual(c.get_sql_for('recreate', no_code=True),
                             """RECREATE PROCEDURE GET_EMP_PROJ (EMP_NO SMALLINT)
RETURNS (PROJ_ID CHAR(5))
AS
BEGIN
  SUSPEND;
END""")

        self.assertEqual(c.get_sql_for('create_or_alter'),
                         """CREATE OR ALTER PROCEDURE GET_EMP_PROJ (EMP_NO SMALLINT)
RETURNS (PROJ_ID CHAR(5))
AS
BEGIN
	FOR SELECT proj_id
		FROM employee_project
		WHERE emp_no = :emp_no
		INTO :proj_id
	DO
		SUSPEND;
END""")
        if self.version == FB30:
            self.assertEqual(c.get_sql_for('create_or_alter', no_code=True),
                             """CREATE OR ALTER PROCEDURE GET_EMP_PROJ (EMP_NO SMALLINT)
RETURNS (PROJ_ID CHAR(5))
AS
BEGIN
  SUSPEND;
END""")
        else:
            self.assertEqual(c.get_sql_for('create_or_alter', no_code=True),
                             """CREATE OR ALTER PROCEDURE GET_EMP_PROJ (EMP_NO SMALLINT)
RETURNS (PROJ_ID CHAR(5))
AS
BEGIN
  SUSPEND;
END""")
        self.assertEqual(c.get_sql_for('drop'), "DROP PROCEDURE GET_EMP_PROJ")
        self.assertEqual(c.get_sql_for('alter', code="  /* PASS */"),
                         """ALTER PROCEDURE GET_EMP_PROJ
AS
BEGIN
  /* PASS */
END""")
        with self.assertRaises(ValueError) as cm:
            c.get_sql_for('alter', declare="DECLARE VARIABLE i integer;")
            self.assertTupleEqual(cm.exception.args,
                                  ("Missing required parameter: 'code'.",))
        self.assertEqual(c.get_sql_for('alter', code=''),
                         """ALTER PROCEDURE GET_EMP_PROJ
AS
BEGIN
END""")
        self.assertEqual(c.get_sql_for('alter', input="IN1 integer", code=''),
                         """ALTER PROCEDURE GET_EMP_PROJ (IN1 integer)
AS
BEGIN
END""")
        self.assertEqual(c.get_sql_for('alter', output="OUT1 integer", code=''),
                         """ALTER PROCEDURE GET_EMP_PROJ
RETURNS (OUT1 integer)
AS
BEGIN
END""")
        self.assertEqual(c.get_sql_for('alter', input="IN1 integer",
                                       output="OUT1 integer", code=''),
                         """ALTER PROCEDURE GET_EMP_PROJ (IN1 integer)
RETURNS (OUT1 integer)
AS
BEGIN
END""")
        self.assertEqual(c.get_sql_for('alter',
                                       input=["IN1 integer", "IN2 VARCHAR(10)"],
                                       code=''),
                         """ALTER PROCEDURE GET_EMP_PROJ (
  IN1 integer,
  IN2 VARCHAR(10)
)
AS
BEGIN
END""")
        self.assertEqual(c.get_sql_for('alter',
                                       output=["OUT1 integer", "OUT2 VARCHAR(10)"],
                                       code=''),
                         """ALTER PROCEDURE GET_EMP_PROJ
RETURNS (
  OUT1 integer,
  OUT2 VARCHAR(10)
)
AS
BEGIN
END""")
        self.assertEqual(c.get_sql_for('alter',
                                       input=["IN1 integer", "IN2 VARCHAR(10)"],
                                       output=["OUT1 integer", "OUT2 VARCHAR(10)"],
                                       code=''),
                         """ALTER PROCEDURE GET_EMP_PROJ (
  IN1 integer,
  IN2 VARCHAR(10)
)
RETURNS (
  OUT1 integer,
  OUT2 VARCHAR(10)
)
AS
BEGIN
END""")
        self.assertEqual(c.get_sql_for('alter', code="  -- line 1;\n  -- line 2;"),
                         """ALTER PROCEDURE GET_EMP_PROJ
AS
BEGIN
  -- line 1;
  -- line 2;
END""")
        self.assertEqual(c.get_sql_for('alter', code=["-- line 1;", "-- line 2;"]),
                         """ALTER PROCEDURE GET_EMP_PROJ
AS
BEGIN
  -- line 1;
  -- line 2;
END""")
        self.assertEqual(c.get_sql_for('alter', code="  /* PASS */",
                                       declare="  -- line 1;\n  -- line 2;"),
                         """ALTER PROCEDURE GET_EMP_PROJ
AS
  -- line 1;
  -- line 2;
BEGIN
  /* PASS */
END""")
        self.assertEqual(c.get_sql_for('alter', code="  /* PASS */",
                                       declare=["-- line 1;", "-- line 2;"]),
                         """ALTER PROCEDURE GET_EMP_PROJ
AS
  -- line 1;
  -- line 2;
BEGIN
  /* PASS */
END""")
        self.assertEqual(c.get_sql_for('comment'),
                         'COMMENT ON PROCEDURE GET_EMP_PROJ IS NULL')
    def test_18_Role(self):
        s = Schema()
        s.bind(self.con)
        c = s.roles.get('TEST_ROLE')
        # common properties
        self.assertEqual(c.name, 'TEST_ROLE')
        self.assertIsNone(c.description)
        self.assertListEqual(c.actions, ['comment', 'create', 'drop'])
        self.assertFalse(c.is_sys_object())
        self.assertEqual(c.get_quoted_name(), 'TEST_ROLE')
        self.assertListEqual(c.get_dependents(), [])
        self.assertListEqual(c.get_dependencies(), [])
        #
        self.assertEqual(c.owner_name, 'SYSDBA')
        #
        self.assertEqual(c.get_sql_for('create'), "CREATE ROLE TEST_ROLE")
        self.assertEqual(c.get_sql_for('drop'), "DROP ROLE TEST_ROLE")
        self.assertEqual(c.get_sql_for('comment'),
                         'COMMENT ON ROLE TEST_ROLE IS NULL')
    def _mockFunction(self, s: Schema, name):
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
    def test_19_FunctionArgument(self):
        s = Schema()
        s.bind(self.con)
        f = self._mockFunction(s, 'ADDDAY')
        c = f.arguments[0] # First argument
        self.assertEqual(len(f.arguments), 2)
        # common properties
        self.assertEqual(c.name, 'ADDDAY_1')
        self.assertIsNone(c.description)
        self.assertListEqual(c.actions, [])
        self.assertFalse(c.is_sys_object())
        self.assertEqual(c.get_quoted_name(), 'ADDDAY_1')
        self.assertListEqual(c.get_dependents(), [])
        self.assertListEqual(c.get_dependencies(), [])
        #
        self.assertEqual(c.function.name, 'ADDDAY')
        self.assertEqual(c.position, 1)
        self.assertEqual(c.mechanism, Mechanism.BY_REFERENCE)
        self.assertEqual(c.field_type, FieldType.TIMESTAMP)
        self.assertEqual(c.length, 8)
        self.assertEqual(c.scale, 0)
        self.assertIsNone(c.precision)
        self.assertIsNone(c.sub_type)
        self.assertIsNone(c.character_length)
        self.assertIsNone(c.character_set)
        self.assertEqual(c.datatype, 'TIMESTAMP')
        #
        self.assertFalse(c.is_by_value())
        self.assertTrue(c.is_by_reference())
        self.assertFalse(c.is_by_descriptor())
        self.assertFalse(c.is_with_null())
        self.assertFalse(c.is_freeit())
        self.assertFalse(c.is_returning())
        self.assertEqual(c.get_sql_definition(), 'TIMESTAMP')
        #
        c = f.arguments[1] # Second argument
        self.assertEqual(len(f.arguments), 2)
        # common properties
        self.assertEqual(c.name, 'ADDDAY_2')
        self.assertIsNone(c.description)
        self.assertListEqual(c.actions, [])
        self.assertFalse(c.is_sys_object())
        self.assertEqual(c.get_quoted_name(), 'ADDDAY_2')
        self.assertListEqual(c.get_dependents(), [])
        self.assertListEqual(c.get_dependencies(), [])
        #
        self.assertEqual(c.function.name, 'ADDDAY')
        self.assertEqual(c.position, 2)
        self.assertEqual(c.mechanism, Mechanism.BY_REFERENCE)
        self.assertEqual(c.field_type, FieldType.LONG)
        self.assertEqual(c.length, 4)
        self.assertEqual(c.scale, 0)
        self.assertEqual(c.precision, 0)
        self.assertEqual(c.sub_type, 0)
        self.assertIsNone(c.character_length)
        self.assertIsNone(c.character_set)
        self.assertEqual(c.datatype, 'INTEGER')
        #
        self.assertFalse(c.is_by_value())
        self.assertTrue(c.is_by_reference())
        self.assertFalse(c.is_by_descriptor())
        self.assertFalse(c.is_with_null())
        self.assertFalse(c.is_freeit())
        self.assertFalse(c.is_returning())
        self.assertEqual(c.get_sql_definition(), 'INTEGER')
        #
        c = f.returns
        #
        self.assertEqual(c.position, 0)
        self.assertEqual(c.mechanism, Mechanism.BY_REFERENCE)
        self.assertEqual(c.field_type, FieldType.TIMESTAMP)
        self.assertEqual(c.length, 8)
        self.assertEqual(c.scale, 0)
        self.assertIsNone(c.precision)
        self.assertIsNone(c.sub_type)
        self.assertIsNone(c.character_length)
        self.assertIsNone(c.character_set)
        self.assertEqual(c.datatype, 'TIMESTAMP')
        #
        self.assertFalse(c.is_by_value())
        self.assertTrue(c.is_by_reference())
        self.assertFalse(c.is_by_descriptor())
        self.assertFalse(c.is_with_null())
        self.assertFalse(c.is_freeit())
        self.assertTrue(c.is_returning())
        self.assertEqual(c.get_sql_definition(), 'TIMESTAMP')
        #
        f = self._mockFunction(s, 'STRING2BLOB')
        self.assertEqual(len(f.arguments), 2)
        c = f.arguments[0]
        self.assertEqual(c.function.name, 'STRING2BLOB')
        self.assertEqual(c.position, 1)
        self.assertEqual(c.mechanism, Mechanism.BY_VMS_DESCRIPTOR)
        self.assertEqual(c.field_type, FieldType.VARYING)
        self.assertEqual(c.length, 1200)
        self.assertEqual(c.scale, 0)
        self.assertIsNone(c.precision)
        self.assertEqual(c.sub_type, 0)
        self.assertEqual(c.character_length, 300)
        self.assertEqual(c.character_set.name, 'UTF8')
        self.assertEqual(c.datatype, 'VARCHAR(300) CHARACTER SET UTF8')
        #
        self.assertFalse(c.is_by_value())
        self.assertFalse(c.is_by_reference())
        self.assertTrue(c.is_by_descriptor())
        self.assertFalse(c.is_with_null())
        self.assertFalse(c.is_freeit())
        self.assertFalse(c.is_returning())
        self.assertEqual(c.get_sql_definition(), 'VARCHAR(300) CHARACTER SET UTF8 BY DESCRIPTOR')
        #
        c = f.arguments[1]
        self.assertIs(f.arguments[1], f.returns)
        self.assertEqual(c.function.name, 'STRING2BLOB')
        self.assertEqual(c.position, 2)
        self.assertEqual(c.mechanism, Mechanism.BY_ISC_DESCRIPTOR)
        self.assertEqual(c.field_type, FieldType.BLOB)
        self.assertEqual(c.length, 8)
        self.assertEqual(c.scale, 0)
        self.assertIsNone(c.precision)
        self.assertEqual(c.sub_type, 0)
        self.assertIsNone(c.character_length)
        self.assertIsNone(c.character_set)
        self.assertEqual(c.datatype, 'BLOB')
        #
        self.assertFalse(c.is_by_value())
        self.assertFalse(c.is_by_reference())
        self.assertFalse(c.is_by_descriptor())
        self.assertTrue(c.is_by_descriptor(any_=True))
        self.assertFalse(c.is_with_null())
        self.assertFalse(c.is_freeit())
        self.assertTrue(c.is_returning())
        self.assertEqual(c.get_sql_definition(), 'BLOB')
        #
        f = self._mockFunction(s, 'SRIGHT')
        self.assertEqual(len(f.arguments), 3)
        c = f.arguments[0] # First argument
        self.assertEqual(c.function.name, 'SRIGHT')
        self.assertEqual(c.position, 1)
        self.assertEqual(c.mechanism, Mechanism.BY_VMS_DESCRIPTOR)
        self.assertEqual(c.field_type, FieldType.VARYING)
        self.assertEqual(c.length, 400)
        self.assertEqual(c.scale, 0)
        self.assertIsNone(c.precision)
        self.assertEqual(c.sub_type, 0)
        self.assertEqual(c.character_length, 100)
        self.assertEqual(c.character_set.name, 'UTF8')
        self.assertEqual(c.datatype, 'VARCHAR(100) CHARACTER SET UTF8')
        #
        self.assertFalse(c.is_by_value())
        self.assertFalse(c.is_by_reference())
        self.assertTrue(c.is_by_descriptor())
        self.assertFalse(c.is_with_null())
        self.assertFalse(c.is_freeit())
        self.assertFalse(c.is_returning())
        self.assertEqual(c.get_sql_definition(), 'VARCHAR(100) CHARACTER SET UTF8 BY DESCRIPTOR')
        #
        c = f.arguments[1] # Second argument
        self.assertEqual(c.function.name, 'SRIGHT')
        self.assertEqual(c.position, 2)
        self.assertEqual(c.mechanism, Mechanism.BY_REFERENCE)
        self.assertEqual(c.field_type, FieldType.SHORT)
        self.assertEqual(c.length, 2)
        self.assertEqual(c.scale, 0)
        self.assertEqual(c.precision, 0)
        self.assertEqual(c.sub_type, 0)
        self.assertIsNone(c.character_length)
        self.assertIsNone(c.character_set)
        self.assertEqual(c.datatype, 'SMALLINT')
        #
        self.assertFalse(c.is_by_value())
        self.assertTrue(c.is_by_reference())
        self.assertFalse(c.is_by_descriptor())
        self.assertFalse(c.is_with_null())
        self.assertFalse(c.is_freeit())
        self.assertFalse(c.is_returning())
        self.assertEqual(c.get_sql_definition(), 'SMALLINT')
        #
        c = f.returns
        self.assertEqual(c.function.name, 'SRIGHT')
        self.assertEqual(c.position, 3)
        self.assertEqual(c.mechanism, Mechanism.BY_VMS_DESCRIPTOR)
        self.assertEqual(c.field_type, FieldType.VARYING)
        self.assertEqual(c.length, 400)
        self.assertEqual(c.scale, 0)
        self.assertIsNone(c.precision)
        self.assertEqual(c.sub_type, 0)
        self.assertEqual(c.character_length, 100)
        self.assertEqual(c.character_set.name, 'UTF8')
        self.assertEqual(c.datatype, 'VARCHAR(100) CHARACTER SET UTF8')
        #
        self.assertFalse(c.is_by_value())
        self.assertFalse(c.is_by_reference())
        self.assertTrue(c.is_by_descriptor())
        self.assertTrue(c.is_by_descriptor(any_=True))
        self.assertFalse(c.is_with_null())
        self.assertFalse(c.is_freeit())
        self.assertTrue(c.is_returning())
        self.assertEqual(c.get_sql_definition(), 'VARCHAR(100) CHARACTER SET UTF8 BY DESCRIPTOR')
        #
        f = self._mockFunction(s, 'I64NVL')
        self.assertEqual(len(f.arguments), 2)
        for a in f.arguments:
            self.assertEqual(a.datatype, 'NUMERIC(18, 0)')
            self.assertTrue(a.is_by_descriptor())
            self.assertEqual(a.get_sql_definition(),
                             'NUMERIC(18, 0) BY DESCRIPTOR')
        self.assertEqual(f.returns.datatype, 'NUMERIC(18, 0)')
        self.assertTrue(f.returns.is_by_descriptor())
        self.assertEqual(f.returns.get_sql_definition(),
                         'NUMERIC(18, 0) BY DESCRIPTOR')
    def test_20_Function(self):
        s = Schema()
        s.bind(self.con)
        #c = self._mockFunction(s, 'ADDDAY')
        #self.assertEqual(len(c.arguments), 1)
        ## common properties
        #self.assertEqual(c.name, 'ADDDAY')
        #self.assertIsNone(c.description)
        #self.assertIsNone(c.package)
        #self.assertIsNone(c.engine_mame)
        #self.assertIsNone(c.private_flag)
        #self.assertIsNone(c.source)
        #self.assertIsNone(c.id)
        #self.assertIsNone(c.valid_blr)
        #self.assertIsNone(c.security_class)
        #self.assertIsNone(c.owner_name)
        #self.assertIsNone(c.legacy_flag)
        #self.assertIsNone(c.deterministic_flag)
        #self.assertListEqual(c.actions, ['comment', 'declare', 'drop'])
        #self.assertFalse(c.is_sys_object())
        #self.assertEqual(c.get_quoted_name(), 'ADDDAY')
        #self.assertListEqual(c.get_dependents(), [])
        #self.assertListEqual(c.get_dependencies(), [])
        #self.assertFalse(c.ispackaged())
        ##
        #self.assertEqual(c.module_name, 'ib_udf')
        #self.assertEqual(c.entrypoint, 'IB_UDF_strlen')
        #self.assertEqual(c.returns.name, 'STRLEN_0')
        #self.assertListEqual([a.name for a in c.arguments], ['ADDDAY_1', 'ADDDAY_2'])
        ##
        #self.assertTrue(c.has_arguments())
        #self.assertTrue(c.has_return())
        #self.assertFalse(c.has_return_argument())
        ##
        #self.assertEqual(c.get_sql_for('drop'), "DROP EXTERNAL FUNCTION ADDDAY")
        #with self.assertRaises(ValueError) as cm:
            #c.get_sql_for('drop', badparam='')
        #self.assertTupleEqual(cm.exception.args,
                              #("Unsupported parameter(s) 'badparam'",))
        #self.assertEqual(c.get_sql_for('declare'),
                         #"""DECLARE EXTERNAL FUNCTION ADDDAY
  #CSTRING(32767)
#RETURNS INTEGER BY VALUE
#ENTRY_POINT 'IB_UDF_strlen'
#MODULE_NAME 'ib_udf'""")
        #with self.assertRaises(ValueError) as cm:
            #c.get_sql_for('declare', badparam='')
        #self.assertTupleEqual(cm.exception.args,
                              #("Unsupported parameter(s) 'badparam'",))
        #self.assertEqual(c.get_sql_for('comment'),
                         #'COMMENT ON EXTERNAL FUNCTION ADDDAY IS NULL')
        #
        c = self._mockFunction(s, 'STRING2BLOB')
        self.assertEqual(len(c.arguments), 2)
        #
        self.assertTrue(c.has_arguments())
        self.assertTrue(c.has_return())
        self.assertTrue(c.has_return_argument())
        #
        self.assertEqual(c.get_sql_for('declare'),
                         """DECLARE EXTERNAL FUNCTION STRING2BLOB
  VARCHAR(300) CHARACTER SET UTF8 BY DESCRIPTOR,
  BLOB
RETURNS PARAMETER 2
ENTRY_POINT 'string2blob'
MODULE_NAME 'fbudf'""")
        #
        #c = self._mockFunction(s, 'LTRIM')
        #self.assertEqual(len(c.arguments), 1)
        ##
        #self.assertTrue(c.has_arguments())
        #self.assertTrue(c.has_return())
        #self.assertFalse(c.has_return_argument())
        ##
        #self.assertEqual(c.get_sql_for('declare'),
                         #"""DECLARE EXTERNAL FUNCTION LTRIM
  #CSTRING(255)
#RETURNS CSTRING(255) FREE_IT
#ENTRY_POINT 'IB_UDF_ltrim'
#MODULE_NAME 'ib_udf'""")
        #
        c = self._mockFunction(s, 'I64NVL')
        self.assertEqual(len(c.arguments), 2)
        #
        self.assertTrue(c.has_arguments())
        self.assertTrue(c.has_return())
        self.assertFalse(c.has_return_argument())
        #
        self.assertEqual(c.get_sql_for('declare'),
                         """DECLARE EXTERNAL FUNCTION I64NVL
  NUMERIC(18, 0) BY DESCRIPTOR,
  NUMERIC(18, 0) BY DESCRIPTOR
RETURNS NUMERIC(18, 0) BY DESCRIPTOR
ENTRY_POINT 'idNvl'
MODULE_NAME 'fbudf'""")
        #
        # Internal PSQL functions (Firebird 3.0)
        c = s.all_functions.get('F2')
        # common properties
        self.assertEqual(c.name, 'F2')
        self.assertIsNone(c.description)
        self.assertIsNone(c.package)
        self.assertIsNone(c.engine_mame)
        self.assertIsNone(c.private_flag)
        self.assertEqual(c.source, 'BEGIN\n  RETURN X+1;\nEND')
        if self.version == FB30:
            self.assertEqual(c.id, 3)
            self.assertEqual(c.security_class, 'SQL$588')
        else:
            self.assertEqual(c.id, 4)
            self.assertEqual(c.security_class, 'SQL$609')
        self.assertTrue(c.valid_blr)
        self.assertEqual(c.owner_name, 'SYSDBA')
        self.assertEqual(c.legacy_flag, 0)
        self.assertEqual(c.deterministic_flag, 0)
        #
        self.assertListEqual(c.actions, ['create', 'recreate', 'alter', 'create_or_alter', 'drop'])
        self.assertFalse(c.is_sys_object())
        self.assertEqual(c.get_quoted_name(), 'F2')
        self.assertListEqual(c.get_dependents(), [])
        self.assertListEqual(c.get_dependencies(), [])
        #
        self.assertIsNone(c.module_name)
        self.assertIsNone(c.entrypoint)
        self.assertEqual(c.returns.name, 'F2_0')
        self.assertListEqual([a.name for a in c.arguments], ['X'])
        #
        self.assertTrue(c.has_arguments())
        self.assertTrue(c.has_return())
        self.assertFalse(c.has_return_argument())
        self.assertFalse(c.is_packaged())
        #
        self.assertEqual(c.get_sql_for('drop'), "DROP FUNCTION F2")
        self.assertEqual(c.get_sql_for('create'),
                             """CREATE FUNCTION F2 (X INTEGER)
RETURNS INTEGER
AS
BEGIN
  RETURN X+1;
END""")
        self.assertEqual(c.get_sql_for('create', no_code=True),
                             """CREATE FUNCTION F2 (X INTEGER)
RETURNS INTEGER
AS
BEGIN
END""")
        self.assertEqual(c.get_sql_for('recreate'),
                             """RECREATE FUNCTION F2 (X INTEGER)
RETURNS INTEGER
AS
BEGIN
  RETURN X+1;
END""")

        self.assertEqual(c.get_sql_for('create_or_alter'),
                             """CREATE OR ALTER FUNCTION F2 (X INTEGER)
RETURNS INTEGER
AS
BEGIN
  RETURN X+1;
END""")
        with self.assertRaises(ValueError) as cm:
            c.get_sql_for('alter', declare="DECLARE VARIABLE i integer;", code='')
        self.assertTupleEqual(cm.exception.args,
                              ("Missing required parameter: 'returns'",))
        with self.assertRaises(ValueError) as cm:
            c.get_sql_for('alter', declare="DECLARE VARIABLE i integer;", returns='INTEGER')
        self.assertTupleEqual(cm.exception.args,
                              ("Missing required parameter: 'code'",))
        self.assertEqual(c.get_sql_for('alter', returns='INTEGER', code=''),
                             """ALTER FUNCTION F2
RETURNS INTEGER
AS
BEGIN
END""")
        self.assertEqual(c.get_sql_for('alter', arguments="IN1 integer", returns='INTEGER',
                                       code=''),
                             """ALTER FUNCTION F2 (IN1 integer)
RETURNS INTEGER
AS
BEGIN
END""")
        self.assertEqual(c.get_sql_for('alter', returns='INTEGER',
                                       arguments=["IN1 integer", "IN2 VARCHAR(10)"],
                                       code=''),
                             """ALTER FUNCTION F2 (
  IN1 integer,
  IN2 VARCHAR(10)
)
RETURNS INTEGER
AS
BEGIN
END""")
        #
        c = s.all_functions.get('FX')
        if self.version == FB30:
            self.assertEqual(c.get_sql_for('create'),"""CREATE FUNCTION FX (
  F TYPE OF "FIRSTNAME",
  L TYPE OF COLUMN CUSTOMER.CONTACT_LAST
)
RETURNS VARCHAR(35)
AS
BEGIN
  RETURN L || \', \' || F;
END""")
        else:
            self.assertEqual(c.get_sql_for('create'),"""CREATE FUNCTION FX (
  F TYPE OF FIRSTNAME,
  L TYPE OF COLUMN CUSTOMER.CONTACT_LAST
)
RETURNS VARCHAR(35)
AS
BEGIN
  RETURN L || \', \' || F;
END""")
                             #"""CREATE FUNCTION FX (
  #L TYPE OF COLUMN CUSTOMER.CONTACT_LAST
#)
#RETURNS VARCHAR(35)
#AS
#BEGIN
  #RETURN L || ', ' || F;
#END""")
        #
        c = s.all_functions.get('F1')
        self.assertEqual(c.name, 'F1')
        self.assertIsNotNone(c.package)
        self.assertIsInstance(c.package, sm.Package)
        self.assertListEqual(c.actions, [])
        self.assertTrue(c.private_flag)
        self.assertTrue(c.is_packaged())

    def test_21_DatabaseFile(self):
        s = Schema()
        s.bind(self.con)
        # We have to use mock
        c = sm.DatabaseFile(s, {'RDB$FILE_LENGTH': 1000,
                                'RDB$FILE_NAME': '/path/dbfile.f02',
                                'RDB$FILE_START': 500,
                                'RDB$FILE_SEQUENCE': 1})
        # common properties
        self.assertEqual(c.name, 'FILE_1')
        self.assertIsNone(c.description)
        self.assertListEqual(c.actions, [])
        self.assertTrue(c.is_sys_object())
        self.assertEqual(c.get_quoted_name(), 'FILE_1')
        self.assertListEqual(c.get_dependents(), [])
        self.assertListEqual(c.get_dependencies(), [])
        #
        self.assertEqual(c.filename, '/path/dbfile.f02')
        self.assertEqual(c.sequence, 1)
        self.assertEqual(c.start, 500)
        self.assertEqual(c.length, 1000)
        #
    def test_22_Shadow(self):
        s = Schema()
        s.bind(self.con)
        # We have to use mocks
        c = Shadow(s, {'RDB$FILE_FLAGS': 1, 'RDB$SHADOW_NUMBER': 3})
        files = []
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
        c.__dict__['_Shadow__files'] = files
        # common properties
        self.assertEqual(c.name, 'SHADOW_3')
        self.assertIsNone(c.description)
        self.assertListEqual(c.actions, ['create', 'drop'])
        self.assertFalse(c.is_sys_object())
        self.assertEqual(c.get_quoted_name(), 'SHADOW_3')
        self.assertListEqual(c.get_dependents(), [])
        self.assertListEqual(c.get_dependencies(), [])
        #
        self.assertEqual(c.id, 3)
        self.assertEqual(c.flags, 1)
        self.assertListEqual([(f.name, f.filename, f.start, f.length) for f in c.files],
                             [('FILE_0', '/path/shadow.sf1', 0, 500),
                              ('FILE_1', '/path/shadow.sf2', 1000, 500),
                              ('FILE_2', '/path/shadow.sf3', 1500, 0)])
        #
        self.assertFalse(c.is_conditional())
        self.assertFalse(c.is_inactive())
        self.assertFalse(c.is_manual())
        #
        self.assertEqual(c.get_sql_for('create'),
                         """CREATE SHADOW 3 AUTO '/path/shadow.sf1' LENGTH 500
  FILE '/path/shadow.sf2' STARTING AT 1000 LENGTH 500
  FILE '/path/shadow.sf3' STARTING AT 1500""")
        self.assertEqual(c.get_sql_for('drop'), "DROP SHADOW 3")
        self.assertEqual(c.get_sql_for('drop', preserve=True), "DROP SHADOW 3 PRESERVE FILE")
    def test_23_PrivilegeBasic(self):
        s = Schema()
        s.bind(self.con)
        p = s.all_procedures.get('ALL_LANGS')
        #
        self.assertIsInstance(p.privileges, list)
        self.assertEqual(len(p.privileges), 2)
        c = p.privileges[0]
        # common properties
        self.assertIsNone(c.name)
        self.assertIsNone(c.description)
        self.assertListEqual(c.actions, ['grant', 'revoke'])
        self.assertTrue(c.is_sys_object())
        self.assertIsNone(c.get_quoted_name())
        self.assertListEqual(c.get_dependents(), [])
        self.assertListEqual(c.get_dependencies(), [])
        #
        self.assertIsInstance(c.user, UserInfo)
        self.assertIn(c.user.user_name, ['SYSDBA', 'PUBLIC'])
        self.assertIsInstance(c.grantor, UserInfo)
        self.assertEqual(c.grantor.user_name, 'SYSDBA')
        self.assertEqual(c.privilege, PrivilegeCode.EXECUTE)
        self.assertIsInstance(c.subject, sm.Procedure)
        self.assertEqual(c.subject.name, 'ALL_LANGS')
        self.assertIn(c.user_name, ['SYSDBA', 'PUBLIC'])
        self.assertEqual(c.user_type, s.object_type_codes['USER'])
        self.assertEqual(c.grantor_name, 'SYSDBA')
        self.assertEqual(c.subject_name, 'ALL_LANGS')
        self.assertEqual(c.subject_type, s.object_type_codes['PROCEDURE'])
        self.assertIsNone(c.field_name)
        #
        self.assertFalse(c.has_grant())
        self.assertFalse(c.is_select())
        self.assertFalse(c.is_insert())
        self.assertFalse(c.is_update())
        self.assertFalse(c.is_delete())
        self.assertTrue(c.is_execute())
        self.assertFalse(c.is_reference())
        self.assertFalse(c.is_membership())
        #
        self.assertEqual(c.get_sql_for('grant'),
                         "GRANT EXECUTE ON PROCEDURE ALL_LANGS TO SYSDBA")
        self.assertEqual(c.get_sql_for('grant', grantors=[]),
                         "GRANT EXECUTE ON PROCEDURE ALL_LANGS TO SYSDBA GRANTED BY SYSDBA")
        self.assertEqual(c.get_sql_for('grant', grantors=['SYSDBA', 'TEST_USER']),
                         "GRANT EXECUTE ON PROCEDURE ALL_LANGS TO SYSDBA")
        with self.assertRaises(ValueError) as cm:
            c.get_sql_for('grant', badparam=True)
        self.assertTupleEqual(cm.exception.args,
                              ("Unsupported parameter(s) 'badparam'",))
        self.assertEqual(c.get_sql_for('revoke'),
                         "REVOKE EXECUTE ON PROCEDURE ALL_LANGS FROM SYSDBA")
        self.assertEqual(c.get_sql_for('revoke', grantors=[]),
                         "REVOKE EXECUTE ON PROCEDURE ALL_LANGS FROM SYSDBA GRANTED BY SYSDBA")
        self.assertEqual(c.get_sql_for('revoke', grantors=['SYSDBA', 'TEST_USER']),
                         "REVOKE EXECUTE ON PROCEDURE ALL_LANGS FROM SYSDBA")
        with self.assertRaises(ValueError) as cm:
            c.get_sql_for('revoke', grant_option=True)
        self.assertTupleEqual(cm.exception.args,
                              ("Can't revoke grant option that wasn't granted.",))
        with self.assertRaises(ValueError) as cm:
            c.get_sql_for('revoke', badparam=True)
        self.assertTupleEqual(cm.exception.args,
                              ("Unsupported parameter(s) 'badparam'",))
        c = p.privileges[1]
        self.assertEqual(c.get_sql_for('grant'),
                         "GRANT EXECUTE ON PROCEDURE ALL_LANGS TO PUBLIC WITH GRANT OPTION")
        self.assertEqual(c.get_sql_for('revoke'),
                         "REVOKE EXECUTE ON PROCEDURE ALL_LANGS FROM PUBLIC")
        self.assertEqual(c.get_sql_for('revoke', grant_option=True),
                         "REVOKE GRANT OPTION FOR EXECUTE ON PROCEDURE ALL_LANGS FROM PUBLIC")
        # get_privileges_of()
        u = UserInfo(user_name='PUBLIC')
        p = s.get_privileges_of(u)
        if self.version == FB30:
            self.assertEqual(len(p), 115)
        else:
            self.assertEqual(len(p), 119)
        with self.assertRaises(ValueError) as cm:
            p = s.get_privileges_of('PUBLIC')
        self.assertTupleEqual(cm.exception.args,
                              ("Argument user_type required",))
        #
    def test_24_PrivilegeExtended(self):
        s = Schema()
        s.bind(self.con)
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
        self.assertEqual(len(p.privileges), 19)
        self.assertEqual(len([x for x in p.privileges if x.user_name == 'SYSDBA']), 5)
        self.assertEqual(len([x for x in p.privileges if x.user_name == 'PUBLIC']), 5)
        self.assertEqual(len([x for x in p.privileges if x.user_name == 'T_USER']), 9)
        #
        x = p.privileges[0]
        self.assertIsInstance(x.subject, sm.Table)
        self.assertEqual(x.subject.name, p.name)
        # TableColumn
        p = p.columns.get('CURRENCY')
        self.assertEqual(len(p.privileges), 2)
        x = p.privileges[0]
        self.assertIsInstance(x.subject, sm.Table)
        self.assertEqual(x.field_name, p.name)
        # View
        p = s.all_views.get('PHONE_LIST')
        self.assertEqual(len(p.privileges), 11)
        self.assertEqual(len([x for x in p.privileges if x.user_name == 'SYSDBA']), 5)
        self.assertEqual(len([x for x in p.privileges if x.user_name == 'PUBLIC']), 6)
        #
        x = p.privileges[0]
        self.assertIsInstance(x.subject, sm.View)
        self.assertEqual(x.subject.name, p.name)
        # ViewColumn
        p = p.columns.get('EMP_NO')
        self.assertEqual(len(p.privileges), 1)
        x = p.privileges[0]
        self.assertIsInstance(x.subject, sm.View)
        self.assertEqual(x.field_name, p.name)
        # Procedure
        p = s.all_procedures.get('ORG_CHART')
        self.assertEqual(len(p.privileges), 2)
        self.assertEqual(len([x for x in p.privileges if x.user_name == 'SYSDBA']), 1)
        self.assertEqual(len([x for x in p.privileges if x.user_name == 'PUBLIC']), 1)
        #
        x = p.privileges[0]
        self.assertFalse(x.has_grant())
        self.assertIsInstance(x.subject, sm.Procedure)
        self.assertEqual(x.subject.name, p.name)
        #
        x = p.privileges[1]
        self.assertTrue(x.has_grant())
        # Role
        p = s.roles.get('TEST_ROLE')
        self.assertEqual(len(p.privileges), 1)
        x = p.privileges[0]
        self.assertIsInstance(x.user, sm.Role)
        self.assertEqual(x.user.name, p.name)
        self.assertTrue(x.is_execute())
        # Trigger as grantee
        p = s.all_tables.get('SALARY_HISTORY')
        x = p.privileges[0]
        self.assertIsInstance(x.user, sm.Trigger)
        self.assertEqual(x.user.name, 'SAVE_SALARY_CHANGE')
        # View as grantee
        p = s.all_views.get('PHONE_LIST')
        x = s.get_privileges_of(p)
        self.assertEqual(len(x), 2)
        x = x[0]
        self.assertIsInstance(x.user, sm.View)
        self.assertEqual(x.user.name, 'PHONE_LIST')
        # get_grants()
        self.assertListEqual(sm.get_grants(p.privileges),
                             ['GRANT REFERENCES(EMP_NO) ON PHONE_LIST TO PUBLIC',
                              'GRANT DELETE, INSERT, REFERENCES, SELECT, UPDATE ON PHONE_LIST TO PUBLIC WITH GRANT OPTION',
                              'GRANT DELETE, INSERT, REFERENCES, SELECT, UPDATE ON PHONE_LIST TO SYSDBA WITH GRANT OPTION'])
        p = s.all_tables.get('COUNTRY')
        self.assertListEqual(sm.get_grants(p.privileges),
                             ['GRANT DELETE, INSERT, UPDATE ON COUNTRY TO PUBLIC',
                              'GRANT REFERENCES, SELECT ON COUNTRY TO PUBLIC WITH GRANT OPTION',
                              'GRANT DELETE, INSERT, REFERENCES, SELECT, UPDATE ON COUNTRY TO SYSDBA WITH GRANT OPTION',
                              'GRANT DELETE, INSERT, REFERENCES(COUNTRY,CURRENCY), SELECT, UPDATE(COUNTRY,CURRENCY) ON COUNTRY TO T_USER'])
        p = s.roles.get('TEST_ROLE')
        self.assertListEqual(sm.get_grants(p.privileges), ['GRANT EXECUTE ON PROCEDURE ALL_LANGS TO TEST_ROLE WITH GRANT OPTION'])
        p = s.all_tables.get('SALARY_HISTORY')
        self.assertListEqual(sm.get_grants(p.privileges),
                             ['GRANT INSERT ON SALARY_HISTORY TO TRIGGER SAVE_SALARY_CHANGE'])
        p = s.all_procedures.get('ORG_CHART')
        self.assertListEqual(sm.get_grants(p.privileges),
                             ['GRANT EXECUTE ON PROCEDURE ORG_CHART TO PUBLIC WITH GRANT OPTION',
                              'GRANT EXECUTE ON PROCEDURE ORG_CHART TO SYSDBA'])
        #
    def test_25_Package(self):
        s = Schema()
        s.bind(self.con)
        c = s.packages.get('TEST')
        # common properties
        self.assertEqual(c.name, 'TEST')
        self.assertIsNone(c.description)
        self.assertFalse(c.is_sys_object())
        self.assertListEqual(c.actions,
                             ['create', 'recreate', 'create_or_alter', 'alter', 'drop', 'comment'])
        self.assertEqual(c.get_quoted_name(), 'TEST')
        self.assertEqual(c.owner_name, 'SYSDBA')
        if self.version == FB30:
            self.assertEqual(c.security_class, 'SQL$575')
        else:
            self.assertEqual(c.security_class, 'SQL$622')
        self.assertEqual(c.header, """BEGIN
  PROCEDURE P1(I INT) RETURNS (O INT); -- public procedure
  FUNCTION F(X INT) RETURNS INT;
END""")
        self.assertEqual(c.body, """BEGIN
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
END""")
        self.assertListEqual(c.get_dependents(), [])
        self.assertEqual(len(c.get_dependencies()), 1)
        self.assertEqual(len(c.functions), 2)
        self.assertEqual(len(c.procedures), 1)
        #
        self.assertEqual(c.get_sql_for('create'), """CREATE PACKAGE TEST
AS
BEGIN
  PROCEDURE P1(I INT) RETURNS (O INT); -- public procedure
  FUNCTION F(X INT) RETURNS INT;
END""")
        self.assertEqual(c.get_sql_for('create', body=True), """CREATE PACKAGE BODY TEST
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
END""")
        self.assertEqual(c.get_sql_for('alter', header="FUNCTION F2(I INT) RETURNS INT;"),
                         """ALTER PACKAGE TEST
AS
BEGIN
FUNCTION F2(I INT) RETURNS INT;
END""")
        self.assertEqual(c.get_sql_for('drop'), """DROP PACKAGE TEST""")
        self.assertEqual(c.get_sql_for('drop', body=True), """DROP PACKAGE BODY TEST""")
        self.assertEqual(c.get_sql_for('create_or_alter'), """CREATE OR ALTER PACKAGE TEST
AS
BEGIN
  PROCEDURE P1(I INT) RETURNS (O INT); -- public procedure
  FUNCTION F(X INT) RETURNS INT;
END""")
        #
        self.assertEqual(c.get_sql_for('comment'),
                         'COMMENT ON PACKAGE TEST IS NULL')
    def test_26_Visitor(self):
        v = SchemaVisitor(self, 'create', follow='dependencies')
        s = Schema()
        s.bind(self.con)
        c = s.all_procedures.get('ALL_LANGS')
        c.accept(v)
        self.maxDiff = None
        output = "CREATE TABLE JOB (\n  JOB_CODE JOBCODE NOT NULL,\n" \
            "  JOB_GRADE JOBGRADE NOT NULL,\n" \
            "  JOB_COUNTRY COUNTRYNAME NOT NULL,\n" \
            "  JOB_TITLE VARCHAR(25) NOT NULL,\n" \
            "  MIN_SALARY SALARY NOT NULL,\n" \
            "  MAX_SALARY SALARY NOT NULL,\n" \
            "  JOB_REQUIREMENT BLOB SUB_TYPE TEXT SEGMENT SIZE 400,\n" \
            "  LANGUAGE_REQ VARCHAR(15)[5],\n" \
            "  PRIMARY KEY (JOB_CODE,JOB_GRADE,JOB_COUNTRY)\n" \
            ")\n" \
            "CREATE PROCEDURE SHOW_LANGS (\n" \
            "  CODE VARCHAR(5),\n" \
            "  GRADE SMALLINT,\n" \
            "  CTY VARCHAR(15)\n" \
            ")\n" \
            "RETURNS (LANGUAGES VARCHAR(15))\n" \
            "AS\n" \
            "DECLARE VARIABLE i INTEGER;\n" \
            "BEGIN\n" \
            "  i = 1;\n" \
            "  WHILE (i <= 5) DO\n" \
            "  BEGIN\n" \
            "    SELECT language_req[:i] FROM joB\n" \
            "    WHERE ((job_code = :code) AND (job_grade = :grade) AND (job_country = :cty)\n" \
            "           AND (language_req IS NOT NULL))\n" \
            "    INTO :languages;\n" \
            "    IF (languages = ' ') THEN  /* Prints 'NULL' instead of blanks */\n" \
            "       languages = 'NULL';         \n" \
            "    i = i +1;\n" \
            "    SUSPEND;\n" \
            "  END\nEND\nCREATE PROCEDURE ALL_LANGS\n" \
            "RETURNS (\n" \
            "  CODE VARCHAR(5),\n" \
            "  GRADE VARCHAR(5),\n" \
            "  COUNTRY VARCHAR(15),\n" \
            "  LANG VARCHAR(15)\n" \
            ")\n" \
            "AS\n" \
            "BEGIN\n" \
            "\tFOR SELECT job_code, job_grade, job_country FROM job \n" \
            "\t\tINTO :code, :grade, :country\n" \
            "\n" \
            "\tDO\n" \
            "\tBEGIN\n" \
            "\t    FOR SELECT languages FROM show_langs \n" \
            " \t\t    (:code, :grade, :country) INTO :lang DO\n" \
            "\t        SUSPEND;\n" \
            "\t    /* Put nice separators between rows */\n" \
            "\t    code = '=====';\n" \
            "\t    grade = '=====';\n" \
            "\t    country = '===============';\n" \
            "\t    lang = '==============';\n" \
            "\t    SUSPEND;\n" \
            "\tEND\n" \
            "    END\n"
        self.assertMultiLineEqual(self.output.getvalue(), output)

        v = SchemaVisitor(self, 'drop', follow='dependents')
        c = s.all_tables.get('JOB')
        self.clear_output()
        c.accept(v)
        self.assertEqual(self.output.getvalue(), """DROP PROCEDURE ALL_LANGS
DROP PROCEDURE SHOW_LANGS
DROP TABLE JOB
""")

    def test_27_Script(self):
        self.maxDiff = None
        self.assertEqual(25, len(sm.SCRIPT_DEFAULT_ORDER))
        s = Schema()
        s.bind(self.con)
        script = s.get_metadata_ddl(sections=[sm.Section.COLLATIONS])
        self.assertListEqual(script, ["""CREATE COLLATION TEST_COLLATE
   FOR WIN1250
   FROM WIN_CZ
   NO PAD
   CASE INSENSITIVE
   ACCENT INSENSITIVE
   'DISABLE-COMPRESSIONS=0;DISABLE-EXPANSIONS=0'"""])
        script = s.get_metadata_ddl(sections=[sm.Section.CHARACTER_SETS])
        self.assertListEqual(script, [])
        script = s.get_metadata_ddl(sections=[sm.Section.UDFS])
        self.assertListEqual(script, [])
        script = s.get_metadata_ddl(sections=[sm.Section.GENERATORS])
        self.assertListEqual(script, ['CREATE SEQUENCE EMP_NO_GEN',
                                      'CREATE SEQUENCE CUST_NO_GEN'])
        script = s.get_metadata_ddl(sections=[sm.Section.EXCEPTIONS])
        self.assertListEqual(script, ["CREATE EXCEPTION UNKNOWN_EMP_ID 'Invalid employee number or project id.'",
                                      "CREATE EXCEPTION REASSIGN_SALES 'Reassign the sales records before deleting this employee.'",
                                      'CREATE EXCEPTION ORDER_ALREADY_SHIPPED \'Order status is "shipped."\'',
                                      "CREATE EXCEPTION CUSTOMER_ON_HOLD 'This customer is on hold.'",
                                      "CREATE EXCEPTION CUSTOMER_CHECK 'Overdue balance -- can not ship.'"])
        script = s.get_metadata_ddl(sections=[sm.Section.DOMAINS])
        if self.version == FB30:
            self.assertListEqual(script, ['CREATE DOMAIN "FIRSTNAME" AS VARCHAR(15)',
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
                                          "CREATE DOMAIN PONUMBER AS CHAR(8) CHECK (VALUE STARTING WITH 'V')"])
        else:
            self.assertListEqual(script, ['CREATE DOMAIN FIRSTNAME AS VARCHAR(15)',
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
                                          "CREATE DOMAIN PONUMBER AS CHAR(8) CHECK (VALUE STARTING WITH 'V')"])
        script = s.get_metadata_ddl(sections=[sm.Section.PACKAGE_DEFS])
        self.assertListEqual(script, ['CREATE PACKAGE TEST\nAS\nBEGIN\n  PROCEDURE P1(I INT) RETURNS (O INT); -- public procedure\n  FUNCTION F(X INT) RETURNS INT;\nEND',
                                      'CREATE PACKAGE TEST2\nAS\nBEGIN\n  FUNCTION F3(X INT) RETURNS INT;\nEND'])
        script = s.get_metadata_ddl(sections=[sm.Section.FUNCTION_DEFS])
        if self.version == FB30:
            self.assertListEqual(script, ['CREATE FUNCTION F2 (X INTEGER)\nRETURNS INTEGER\nAS\nBEGIN\nEND',
                                          'CREATE FUNCTION FX (\n  F TYPE OF "FIRSTNAME",\n  L TYPE OF COLUMN CUSTOMER.CONTACT_LAST\n)\nRETURNS VARCHAR(35)\nAS\nBEGIN\nEND',
                                          'CREATE FUNCTION FN\nRETURNS INTEGER\nAS\nBEGIN\nEND'])
        else:
            self.assertListEqual(script, ['CREATE FUNCTION F2 (X INTEGER)\nRETURNS INTEGER\nAS\nBEGIN\nEND',
                                          'CREATE FUNCTION FX (\n  F TYPE OF FIRSTNAME,\n  L TYPE OF COLUMN CUSTOMER.CONTACT_LAST\n)\nRETURNS VARCHAR(35)\nAS\nBEGIN\nEND',
                                          'CREATE FUNCTION FN\nRETURNS INTEGER\nAS\nBEGIN\nEND'])
        script = s.get_metadata_ddl(sections=[sm.Section.PROCEDURE_DEFS])
        self.assertListEqual(script, ['CREATE PROCEDURE GET_EMP_PROJ (EMP_NO SMALLINT)\nRETURNS (PROJ_ID CHAR(5))\nAS\nBEGIN\n  SUSPEND;\nEND',
                                      'CREATE PROCEDURE ADD_EMP_PROJ (\n  EMP_NO SMALLINT,\n  PROJ_ID CHAR(5)\n)\nAS\nBEGIN\n  SUSPEND;\nEND',
                                      'CREATE PROCEDURE SUB_TOT_BUDGET (HEAD_DEPT CHAR(3))\nRETURNS (\n  TOT_BUDGET DECIMAL(12, 2),\n  AVG_BUDGET DECIMAL(12, 2),\n  MIN_BUDGET DECIMAL(12, 2),\n  MAX_BUDGET DECIMAL(12, 2)\n)\nAS\nBEGIN\n  SUSPEND;\nEND',
                                      'CREATE PROCEDURE DELETE_EMPLOYEE (EMP_NUM INTEGER)\nAS\nBEGIN\n  SUSPEND;\nEND',
                                      'CREATE PROCEDURE DEPT_BUDGET (DNO CHAR(3))\nRETURNS (TOT DECIMAL(12, 2))\nAS\nBEGIN\n  SUSPEND;\nEND',
                                      'CREATE PROCEDURE ORG_CHART\nRETURNS (\n  HEAD_DEPT CHAR(25),\n  DEPARTMENT CHAR(25),\n  MNGR_NAME CHAR(20),\n  TITLE CHAR(5),\n  EMP_CNT INTEGER\n)\nAS\nBEGIN\n  SUSPEND;\nEND',
                                      'CREATE PROCEDURE MAIL_LABEL (CUST_NO INTEGER)\nRETURNS (\n  LINE1 CHAR(40),\n  LINE2 CHAR(40),\n  LINE3 CHAR(40),\n  LINE4 CHAR(40),\n  LINE5 CHAR(40),\n  LINE6 CHAR(40)\n)\nAS\nBEGIN\n  SUSPEND;\nEND',
                                      'CREATE PROCEDURE SHIP_ORDER (PO_NUM CHAR(8))\nAS\nBEGIN\n  SUSPEND;\nEND',
                                      'CREATE PROCEDURE SHOW_LANGS (\n  CODE VARCHAR(5),\n  GRADE SMALLINT,\n  CTY VARCHAR(15)\n)\nRETURNS (LANGUAGES VARCHAR(15))\nAS\nBEGIN\n  SUSPEND;\nEND',
                                      'CREATE PROCEDURE ALL_LANGS\nRETURNS (\n  CODE VARCHAR(5),\n  GRADE VARCHAR(5),\n  COUNTRY VARCHAR(15),\n  LANG VARCHAR(15)\n)\nAS\nBEGIN\n  SUSPEND;\nEND'])
        script = s.get_metadata_ddl(sections=[sm.Section.TABLES])
        self.assertListEqual(script, ['CREATE TABLE COUNTRY (\n  COUNTRY COUNTRYNAME NOT NULL,\n  CURRENCY VARCHAR(10) NOT NULL\n)',
                                      'CREATE TABLE JOB (\n  JOB_CODE JOBCODE NOT NULL,\n  JOB_GRADE JOBGRADE NOT NULL,\n  JOB_COUNTRY COUNTRYNAME NOT NULL,\n  JOB_TITLE VARCHAR(25) NOT NULL,\n  MIN_SALARY SALARY NOT NULL,\n  MAX_SALARY SALARY NOT NULL,\n  JOB_REQUIREMENT BLOB SUB_TYPE TEXT SEGMENT SIZE 400,\n  LANGUAGE_REQ VARCHAR(15)[5]\n)',
                                      "CREATE TABLE DEPARTMENT (\n  DEPT_NO DEPTNO NOT NULL,\n  DEPARTMENT VARCHAR(25) NOT NULL,\n  HEAD_DEPT DEPTNO,\n  MNGR_NO EMPNO,\n  BUDGET BUDGET,\n  LOCATION VARCHAR(15),\n  PHONE_NO PHONENUMBER DEFAULT '555-1234'\n)",
                                      'CREATE TABLE EMPLOYEE (\n  EMP_NO EMPNO NOT NULL,\n  FIRST_NAME "FIRSTNAME" NOT NULL,\n  LAST_NAME "LASTNAME" NOT NULL,\n  PHONE_EXT VARCHAR(4),\n  HIRE_DATE TIMESTAMP DEFAULT \'NOW\' NOT NULL,\n  DEPT_NO DEPTNO NOT NULL,\n  JOB_CODE JOBCODE NOT NULL,\n  JOB_GRADE JOBGRADE NOT NULL,\n  JOB_COUNTRY COUNTRYNAME NOT NULL,\n  SALARY SALARY NOT NULL,\n  FULL_NAME COMPUTED BY (last_name || \', \' || first_name)\n)' \
                                      if self.version == FB30 else \
                                      'CREATE TABLE EMPLOYEE (\n  EMP_NO EMPNO NOT NULL,\n  FIRST_NAME FIRSTNAME NOT NULL,\n  LAST_NAME LASTNAME NOT NULL,\n  PHONE_EXT VARCHAR(4),\n  HIRE_DATE TIMESTAMP DEFAULT \'NOW\' NOT NULL,\n  DEPT_NO DEPTNO NOT NULL,\n  JOB_CODE JOBCODE NOT NULL,\n  JOB_GRADE JOBGRADE NOT NULL,\n  JOB_COUNTRY COUNTRYNAME NOT NULL,\n  SALARY SALARY NOT NULL,\n  FULL_NAME COMPUTED BY (last_name || \', \' || first_name)\n)',
                                      'CREATE TABLE CUSTOMER (\n  CUST_NO CUSTNO NOT NULL,\n  CUSTOMER VARCHAR(25) NOT NULL,\n  CONTACT_FIRST "FIRSTNAME",\n  CONTACT_LAST "LASTNAME",\n  PHONE_NO PHONENUMBER,\n  ADDRESS_LINE1 ADDRESSLINE,\n  ADDRESS_LINE2 ADDRESSLINE,\n  CITY VARCHAR(25),\n  STATE_PROVINCE VARCHAR(15),\n  COUNTRY COUNTRYNAME,\n  POSTAL_CODE VARCHAR(12),\n  ON_HOLD CHAR(1) DEFAULT NULL\n)' \
                                      if self.version == FB30 else \
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
                                      'CREATE TABLE T5 (\n  ID NUMERIC(10, 0) GENERATED BY DEFAULT AS IDENTITY,\n  C1 VARCHAR(15),\n  UQ BIGINT GENERATED BY DEFAULT AS IDENTITY (START WITH 100)\n)', 'CREATE TABLE T (\n  C1 INTEGER NOT NULL\n)'])
        script = s.get_metadata_ddl(sections=[sm.Section.PRIMARY_KEYS])
        self.assertListEqual(script, ['ALTER TABLE COUNTRY ADD PRIMARY KEY (COUNTRY)',
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
                                      'ALTER TABLE T ADD PRIMARY KEY (C1)'],)
        script = s.get_metadata_ddl(sections=[sm.Section.UNIQUE_CONSTRAINTS])
        self.assertListEqual(script, ['ALTER TABLE DEPARTMENT ADD UNIQUE (DEPARTMENT)',
                                      'ALTER TABLE PROJECT ADD UNIQUE (PROJ_NAME)'])
        script = s.get_metadata_ddl(sections=[sm.Section.CHECK_CONSTRAINTS])
        self.assertListEqual(script, ['ALTER TABLE JOB ADD CHECK (min_salary < max_salary)',
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
                                      "ALTER TABLE SALES ADD CHECK (NOT (order_status = 'shipped' AND\n            EXISTS (SELECT on_hold FROM customer\n                    WHERE customer.cust_no = sales.cust_no\n                    AND customer.on_hold = '*')))"])
        script = s.get_metadata_ddl(sections=[sm.Section.FOREIGN_CONSTRAINTS])
        self.assertListEqual(script, ['ALTER TABLE JOB ADD FOREIGN KEY (JOB_COUNTRY)\n  REFERENCES COUNTRY (COUNTRY)',
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
                                      'ALTER TABLE SALES ADD FOREIGN KEY (SALES_REP)\n  REFERENCES EMPLOYEE (EMP_NO)'])
        script = s.get_metadata_ddl(sections=[sm.Section.INDICES])
        self.assertListEqual(script, ['CREATE ASCENDING INDEX MINSALX ON JOB (JOB_COUNTRY,MIN_SALARY)',
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
                                      'CREATE DESCENDING INDEX QTYX ON SALES (ITEM_TYPE,QTY_ORDERED)'])
        script = s.get_metadata_ddl(sections=[sm.Section.VIEWS])
        self.assertListEqual(script, ['CREATE VIEW PHONE_LIST (EMP_NO,FIRST_NAME,LAST_NAME,PHONE_EXT,LOCATION,PHONE_NO)\n   AS\n     SELECT\n    emp_no, first_name, last_name, phone_ext, location, phone_no\n    FROM employee, department\n    WHERE employee.dept_no = department.dept_no'])
        script = s.get_metadata_ddl(sections=[sm.Section.PACKAGE_BODIES])
        self.assertListEqual(script, ['CREATE PACKAGE BODY TEST\nAS\nBEGIN\n  FUNCTION F1(I INT) RETURNS INT; -- private function\n\n  PROCEDURE P1(I INT) RETURNS (O INT)\n  AS\n  BEGIN\n  END\n\n  FUNCTION F1(I INT) RETURNS INT\n  AS\n  BEGIN\n    RETURN F(I)+10;\n  END\n\n  FUNCTION F(X INT) RETURNS INT\n  AS\n  BEGIN\n    RETURN X+1;\n  END\nEND', 'CREATE PACKAGE BODY TEST2\nAS\nBEGIN\n  FUNCTION F3(X INT) RETURNS INT\n  AS\n  BEGIN\n    RETURN TEST.F(X)+100+FN();\n  END\nEND'])
        script = s.get_metadata_ddl(sections=[sm.Section.FUNCTION_BODIES])
        self.assertListEqual(script, ['ALTER FUNCTION F2 (X INTEGER)\nRETURNS INTEGER\nAS\nBEGIN\n  RETURN X+1;\nEND',
                                      'ALTER FUNCTION FX (\n  F TYPE OF "FIRSTNAME",\n  L TYPE OF COLUMN CUSTOMER.CONTACT_LAST\n)\nRETURNS VARCHAR(35)\nAS\nBEGIN\n  RETURN L || \', \' || F;\nEND' \
                                      if self.version == FB30 else \
                                      'ALTER FUNCTION FX (\n  F TYPE OF FIRSTNAME,\n  L TYPE OF COLUMN CUSTOMER.CONTACT_LAST\n)\nRETURNS VARCHAR(35)\nAS\nBEGIN\n  RETURN L || \', \' || F;\nEND',
                                      'ALTER FUNCTION FN\nRETURNS INTEGER\nAS\nBEGIN\n  RETURN 0;\nEND'])
        script = s.get_metadata_ddl(sections=[sm.Section.PROCEDURE_BODIES])
        self.assertListEqual(script, ['ALTER PROCEDURE GET_EMP_PROJ (EMP_NO SMALLINT)\nRETURNS (PROJ_ID CHAR(5))\nAS\nBEGIN\n\tFOR SELECT proj_id\n\t\tFROM employee_project\n\t\tWHERE emp_no = :emp_no\n\t\tINTO :proj_id\n\tDO\n\t\tSUSPEND;\nEND', 'ALTER PROCEDURE ADD_EMP_PROJ (\n  EMP_NO SMALLINT,\n  PROJ_ID CHAR(5)\n)\nAS\nBEGIN\n\tBEGIN\n\tINSERT INTO employee_project (emp_no, proj_id) VALUES (:emp_no, :proj_id);\n\tWHEN SQLCODE -530 DO\n\t\tEXCEPTION unknown_emp_id;\n\tEND\n\tSUSPEND;\nEND',
                                      'ALTER PROCEDURE SUB_TOT_BUDGET (HEAD_DEPT CHAR(3))\nRETURNS (\n  TOT_BUDGET DECIMAL(12, 2),\n  AVG_BUDGET DECIMAL(12, 2),\n  MIN_BUDGET DECIMAL(12, 2),\n  MAX_BUDGET DECIMAL(12, 2)\n)\nAS\nBEGIN\n\tSELECT SUM(budget), AVG(budget), MIN(budget), MAX(budget)\n\t\tFROM department\n\t\tWHERE head_dept = :head_dept\n\t\tINTO :tot_budget, :avg_budget, :min_budget, :max_budget;\n\tSUSPEND;\nEND',
                                      "ALTER PROCEDURE DELETE_EMPLOYEE (EMP_NUM INTEGER)\nAS\nDECLARE VARIABLE any_sales INTEGER;\nBEGIN\n\tany_sales = 0;\n\n\t/*\n\t *\tIf there are any sales records referencing this employee,\n\t *\tcan't delete the employee until the sales are re-assigned\n\t *\tto another employee or changed to NULL.\n\t */\n\tSELECT count(po_number)\n\tFROM sales\n\tWHERE sales_rep = :emp_num\n\tINTO :any_sales;\n\n\tIF (any_sales > 0) THEN\n\tBEGIN\n\t\tEXCEPTION reassign_sales;\n\t\tSUSPEND;\n\tEND\n\n\t/*\n\t *\tIf the employee is a manager, update the department.\n\t */\n\tUPDATE department\n\tSET mngr_no = NULL\n\tWHERE mngr_no = :emp_num;\n\n\t/*\n\t *\tIf the employee is a project leader, update project.\n\t */\n\tUPDATE project\n\tSET team_leader = NULL\n\tWHERE team_leader = :emp_num;\n\n\t/*\n\t *\tDelete the employee from any projects.\n\t */\n\tDELETE FROM employee_project\n\tWHERE emp_no = :emp_num;\n\n\t/*\n\t *\tDelete old salary records.\n\t */\n\tDELETE FROM salary_history\n\tWHERE emp_no = :emp_num;\n\n\t/*\n\t *\tDelete the employee.\n\t */\n\tDELETE FROM employee\n\tWHERE emp_no = :emp_num;\n\n\tSUSPEND;\nEND",
                                      'ALTER PROCEDURE DEPT_BUDGET (DNO CHAR(3))\nRETURNS (TOT DECIMAL(12, 2))\nAS\nDECLARE VARIABLE sumb DECIMAL(12, 2);\n\tDECLARE VARIABLE rdno CHAR(3);\n\tDECLARE VARIABLE cnt INTEGER;\nBEGIN\n\ttot = 0;\n\n\tSELECT budget FROM department WHERE dept_no = :dno INTO :tot;\n\n\tSELECT count(budget) FROM department WHERE head_dept = :dno INTO :cnt;\n\n\tIF (cnt = 0) THEN\n\t\tSUSPEND;\n\n\tFOR SELECT dept_no\n\t\tFROM department\n\t\tWHERE head_dept = :dno\n\t\tINTO :rdno\n\tDO\n\t\tBEGIN\n\t\t\tEXECUTE PROCEDURE dept_budget :rdno RETURNING_VALUES :sumb;\n\t\t\ttot = tot + sumb;\n\t\tEND\n\n\tSUSPEND;\nEND',
                                      "ALTER PROCEDURE ORG_CHART\nRETURNS (\n  HEAD_DEPT CHAR(25),\n  DEPARTMENT CHAR(25),\n  MNGR_NAME CHAR(20),\n  TITLE CHAR(5),\n  EMP_CNT INTEGER\n)\nAS\nDECLARE VARIABLE mngr_no INTEGER;\n\tDECLARE VARIABLE dno CHAR(3);\nBEGIN\n\tFOR SELECT h.department, d.department, d.mngr_no, d.dept_no\n\t\tFROM department d\n\t\tLEFT OUTER JOIN department h ON d.head_dept = h.dept_no\n\t\tORDER BY d.dept_no\n\t\tINTO :head_dept, :department, :mngr_no, :dno\n\tDO\n\tBEGIN\n\t\tIF (:mngr_no IS NULL) THEN\n\t\tBEGIN\n\t\t\tmngr_name = '--TBH--';\n\t\t\ttitle = '';\n\t\tEND\n\n\t\tELSE\n\t\t\tSELECT full_name, job_code\n\t\t\tFROM employee\n\t\t\tWHERE emp_no = :mngr_no\n\t\t\tINTO :mngr_name, :title;\n\n\t\tSELECT COUNT(emp_no)\n\t\tFROM employee\n\t\tWHERE dept_no = :dno\n\t\tINTO :emp_cnt;\n\n\t\tSUSPEND;\n\tEND\nEND",
                                      "ALTER PROCEDURE MAIL_LABEL (CUST_NO INTEGER)\nRETURNS (\n  LINE1 CHAR(40),\n  LINE2 CHAR(40),\n  LINE3 CHAR(40),\n  LINE4 CHAR(40),\n  LINE5 CHAR(40),\n  LINE6 CHAR(40)\n)\nAS\nDECLARE VARIABLE customer\tVARCHAR(25);\n\tDECLARE VARIABLE first_name\t\tVARCHAR(15);\n\tDECLARE VARIABLE last_name\t\tVARCHAR(20);\n\tDECLARE VARIABLE addr1\t\tVARCHAR(30);\n\tDECLARE VARIABLE addr2\t\tVARCHAR(30);\n\tDECLARE VARIABLE city\t\tVARCHAR(25);\n\tDECLARE VARIABLE state\t\tVARCHAR(15);\n\tDECLARE VARIABLE country\tVARCHAR(15);\n\tDECLARE VARIABLE postcode\tVARCHAR(12);\n\tDECLARE VARIABLE cnt\t\tINTEGER;\nBEGIN\n\tline1 = '';\n\tline2 = '';\n\tline3 = '';\n\tline4 = '';\n\tline5 = '';\n\tline6 = '';\n\n\tSELECT customer, contact_first, contact_last, address_line1,\n\t\taddress_line2, city, state_province, country, postal_code\n\tFROM CUSTOMER\n\tWHERE cust_no = :cust_no\n\tINTO :customer, :first_name, :last_name, :addr1, :addr2,\n\t\t:city, :state, :country, :postcode;\n\n\tIF (customer IS NOT NULL) THEN\n\t\tline1 = customer;\n\tIF (first_name IS NOT NULL) THEN\n\t\tline2 = first_name || ' ' || last_name;\n\tELSE\n\t\tline2 = last_name;\n\tIF (addr1 IS NOT NULL) THEN\n\t\tline3 = addr1;\n\tIF (addr2 IS NOT NULL) THEN\n\t\tline4 = addr2;\n\n\tIF (country = 'USA') THEN\n\tBEGIN\n\t\tIF (city IS NOT NULL) THEN\n\t\t\tline5 = city || ', ' || state || '  ' || postcode;\n\t\tELSE\n\t\t\tline5 = state || '  ' || postcode;\n\tEND\n\tELSE\n\tBEGIN\n\t\tIF (city IS NOT NULL) THEN\n\t\t\tline5 = city || ', ' || state;\n\t\tELSE\n\t\t\tline5 = state;\n\t\tline6 = country || '    ' || postcode;\n\tEND\n\n\tSUSPEND;\nEND",
                                      "ALTER PROCEDURE SHIP_ORDER (PO_NUM CHAR(8))\nAS\nDECLARE VARIABLE ord_stat CHAR(7);\n\tDECLARE VARIABLE hold_stat CHAR(1);\n\tDECLARE VARIABLE cust_no INTEGER;\n\tDECLARE VARIABLE any_po CHAR(8);\nBEGIN\n\tSELECT s.order_status, c.on_hold, c.cust_no\n\tFROM sales s, customer c\n\tWHERE po_number = :po_num\n\tAND s.cust_no = c.cust_no\n\tINTO :ord_stat, :hold_stat, :cust_no;\n\n\t/* This purchase order has been already shipped. */\n\tIF (ord_stat = 'shipped') THEN\n\tBEGIN\n\t\tEXCEPTION order_already_shipped;\n\t\tSUSPEND;\n\tEND\n\n\t/*\tCustomer is on hold. */\n\tELSE IF (hold_stat = '*') THEN\n\tBEGIN\n\t\tEXCEPTION customer_on_hold;\n\t\tSUSPEND;\n\tEND\n\n\t/*\n\t *\tIf there is an unpaid balance on orders shipped over 2 months ago,\n\t *\tput the customer on hold.\n\t */\n\tFOR SELECT po_number\n\t\tFROM sales\n\t\tWHERE cust_no = :cust_no\n\t\tAND order_status = 'shipped'\n\t\tAND paid = 'n'\n\t\tAND ship_date < CAST('NOW' AS TIMESTAMP) - 60\n\t\tINTO :any_po\n\tDO\n\tBEGIN\n\t\tEXCEPTION customer_check;\n\n\t\tUPDATE customer\n\t\tSET on_hold = '*'\n\t\tWHERE cust_no = :cust_no;\n\n\t\tSUSPEND;\n\tEND\n\n\t/*\n\t *\tShip the order.\n\t */\n\tUPDATE sales\n\tSET order_status = 'shipped', ship_date = 'NOW'\n\tWHERE po_number = :po_num;\n\n\tSUSPEND;\nEND",
                                      "ALTER PROCEDURE SHOW_LANGS (\n  CODE VARCHAR(5),\n  GRADE SMALLINT,\n  CTY VARCHAR(15)\n)\nRETURNS (LANGUAGES VARCHAR(15))\nAS\nDECLARE VARIABLE i INTEGER;\nBEGIN\n  i = 1;\n  WHILE (i <= 5) DO\n  BEGIN\n    SELECT language_req[:i] FROM joB\n    WHERE ((job_code = :code) AND (job_grade = :grade) AND (job_country = :cty)\n           AND (language_req IS NOT NULL))\n    INTO :languages;\n    IF (languages = ' ') THEN  /* Prints 'NULL' instead of blanks */\n       languages = 'NULL';         \n    i = i +1;\n    SUSPEND;\n  END\nEND",
                                      "ALTER PROCEDURE ALL_LANGS\nRETURNS (\n  CODE VARCHAR(5),\n  GRADE VARCHAR(5),\n  COUNTRY VARCHAR(15),\n  LANG VARCHAR(15)\n)\nAS\nBEGIN\n\tFOR SELECT job_code, job_grade, job_country FROM job \n\t\tINTO :code, :grade, :country\n\n\tDO\n\tBEGIN\n\t    FOR SELECT languages FROM show_langs \n \t\t    (:code, :grade, :country) INTO :lang DO\n\t        SUSPEND;\n\t    /* Put nice separators between rows */\n\t    code = '=====';\n\t    grade = '=====';\n\t    country = '===============';\n\t    lang = '==============';\n\t    SUSPEND;\n\tEND\n    END"])
        script = s.get_metadata_ddl(sections=[sm.Section.TRIGGERS])
        self.assertListEqual(script, ['CREATE TRIGGER SET_EMP_NO FOR EMPLOYEE ACTIVE\nBEFORE INSERT POSITION 0\nAS\nBEGIN\n    if (new.emp_no is null) then\n    new.emp_no = gen_id(emp_no_gen, 1);\nEND',
                                      "CREATE TRIGGER SAVE_SALARY_CHANGE FOR EMPLOYEE ACTIVE\nAFTER UPDATE POSITION 0\nAS\nBEGIN\n    IF (old.salary <> new.salary) THEN\n        INSERT INTO salary_history\n            (emp_no, change_date, updater_id, old_salary, percent_change)\n        VALUES (\n            old.emp_no,\n            'NOW',\n            user,\n            old.salary,\n            (new.salary - old.salary) * 100 / old.salary);\nEND",
                                      'CREATE TRIGGER SET_CUST_NO FOR CUSTOMER ACTIVE\nBEFORE INSERT POSITION 0\nAS\nBEGIN\n    if (new.cust_no is null) then\n    new.cust_no = gen_id(cust_no_gen, 1);\nEND',
                                      "CREATE TRIGGER POST_NEW_ORDER FOR SALES ACTIVE\nAFTER INSERT POSITION 0\nAS\nBEGIN\n    POST_EVENT 'new_order';\nEND",
                                      'CREATE TRIGGER TR_CONNECT ACTIVE\nON CONNECT POSITION 0\nAS \nBEGIN \n    /* enter trigger code here */ \nEND',
                                      'CREATE TRIGGER TR_MULTI FOR COUNTRY ACTIVE\nAFTER INSERT OR UPDATE OR DELETE POSITION 0\nAS \nBEGIN \n    /* enter trigger code here */ \nEND',
                                      'CREATE TRIGGER TRIG_DDL_SP ACTIVE\nBEFORE ALTER FUNCTION POSITION 0\nAS \nBEGIN \n    /* enter trigger code here */ \nEND',
                                      'CREATE TRIGGER TRIG_DDL ACTIVE\nBEFORE ANY DDL STATEMENT POSITION 0\nAS \nBEGIN \n    /* enter trigger code here */ \nEND'])
        script = s.get_metadata_ddl(sections=[sm.Section.ROLES])
        self.assertListEqual(script, ['CREATE ROLE TEST_ROLE'])
        script = s.get_metadata_ddl(sections=[sm.Section.GRANTS])
        self.assertListEqual(script, ['GRANT SELECT ON COUNTRY TO PUBLIC WITH GRANT OPTION',
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
                                      'GRANT EXECUTE ON PROCEDURE ALL_LANGS TO PUBLIC WITH GRANT OPTION'])
        script = s.get_metadata_ddl(sections=[sm.Section.COMMENTS])
        self.assertListEqual(script, ["COMMENT ON CHARACTER SET NONE IS 'Comment on NONE character set'"])
        script = s.get_metadata_ddl(sections=[sm.Section.SHADOWS])
        self.assertListEqual(script, [])
        script = s.get_metadata_ddl(sections=[sm.Section.INDEX_DEACTIVATIONS])
        if self.version == FB30:
            self.assertListEqual(script, ['ALTER INDEX MINSALX INACTIVE',
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
                                          'ALTER INDEX QTYX INACTIVE'])
        else:
            self.assertListEqual(script, ['ALTER INDEX NEEDX INACTIVE',
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
                                          'ALTER INDEX MAXSALX INACTIVE'])
        script = s.get_metadata_ddl(sections=[sm.Section.INDEX_ACTIVATIONS])
        if self.version == FB30:
            self.assertListEqual(script, ['ALTER INDEX MINSALX ACTIVE',
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
                                          'ALTER INDEX QTYX ACTIVE'])
        else:
            self.assertListEqual(script, ['ALTER INDEX NEEDX ACTIVE',
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
                                          'ALTER INDEX MAXSALX ACTIVE'])
        script = s.get_metadata_ddl(sections=[sm.Section.SET_GENERATORS])
        self.assertListEqual(script, ['ALTER SEQUENCE EMP_NO_GEN RESTART WITH 145',
                                      'ALTER SEQUENCE CUST_NO_GEN RESTART WITH 1015'])
        script = s.get_metadata_ddl(sections=[sm.Section.TRIGGER_DEACTIVATIONS])
        self.assertListEqual(script, ['ALTER TRIGGER SET_EMP_NO INACTIVE',
                                      'ALTER TRIGGER SAVE_SALARY_CHANGE INACTIVE',
                                      'ALTER TRIGGER SET_CUST_NO INACTIVE',
                                      'ALTER TRIGGER POST_NEW_ORDER INACTIVE',
                                      'ALTER TRIGGER TR_CONNECT INACTIVE',
                                      'ALTER TRIGGER TR_MULTI INACTIVE',
                                      'ALTER TRIGGER TRIG_DDL_SP INACTIVE',
                                      'ALTER TRIGGER TRIG_DDL INACTIVE'])
        script = s.get_metadata_ddl(sections=[sm.Section.TRIGGER_ACTIVATIONS])
        self.assertListEqual(script, ['ALTER TRIGGER SET_EMP_NO ACTIVE',
                                      'ALTER TRIGGER SAVE_SALARY_CHANGE ACTIVE',
                                      'ALTER TRIGGER SET_CUST_NO ACTIVE',
                                      'ALTER TRIGGER POST_NEW_ORDER ACTIVE',
                                      'ALTER TRIGGER TR_CONNECT ACTIVE',
                                      'ALTER TRIGGER TR_MULTI ACTIVE',
                                      'ALTER TRIGGER TRIG_DDL_SP ACTIVE',
                                      'ALTER TRIGGER TRIG_DDL ACTIVE'])

if __name__ == '__main__':
    unittest.main()

