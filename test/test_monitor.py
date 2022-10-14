#coding:utf-8
#
# PROGRAM/MODULE: firebird-lib
# FILE:           test_schema.py
# DESCRIPTION:    Unit tests for firebird.lib.schema
# CREATED:        21.9.2020
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
import datetime
from contextlib import closing
from re import finditer
from pprint import pprint
from firebird.base.collections import DataList
from firebird.driver import *
from firebird.lib.monitor import *
from firebird.lib.schema import CharacterSet
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

if driver_config.get_database('fbtest') is None:
    # Register database
    db_cfg = """[fbtest]
    server = local
    database = fbtest3.fdb
    protocol = inet
    charset = utf8
    """
    driver_config.register_database('fbtest', db_cfg)

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

class TestMonitor(TestBase):
    def setUp(self):
        super().setUp()
        self.con = connect('fbtest')
        self.con._logging_id_ = 'fbtest'
    def tearDown(self):
        self.con.close()
    def test_01_close(self):
        s = Monitor(self.con)
        self.assertFalse(s.closed)
        s.close()
        self.assertTrue(s.closed)
        #
        with Monitor(self.con) as m:
            self.assertFalse(m.closed)
        self.assertTrue(m.closed)
    def test_02_monitor(self):
        #
        with Monitor(self.con) as m:
            sql = "select RDB$SET_CONTEXT('USER_SESSION','TESTVAR','TEST_VALUE') from rdb$database"
            with self.con.cursor() as c:
                c.execute(sql)
                c.fetchone()
                #
                self.assertIsNotNone(m.db)
                self.assertIsInstance(m.db, DatabaseInfo)
                self.assertGreater(len(m.attachments), 0)
                self.assertIsInstance(m.attachments[0], AttachmentInfo)
                self.assertGreater(len(m.transactions), 0)
                self.assertIsInstance(m.transactions[0], TransactionInfo)
                self.assertGreater(len(m.statements), 0)
                self.assertIsInstance(m.statements[0], StatementInfo)
                self.assertEqual(len(m.callstack), 0)
                self.assertGreater(len(m.iostats), 0)
                self.assertIsInstance(m.iostats[0], IOStatsInfo)
                self.assertGreater(len(m.variables), 0)
                self.assertIsInstance(m.variables[0], ContextVariableInfo)
                #
                att_id = m._con.info.id
                self.assertEqual(m.attachments.get(att_id).id, att_id)
                tra_id = m._con.main_transaction.info.id
                self.assertEqual(m.transactions.get(tra_id).id, tra_id)
                stmt_id = None
                for stmt in m.statements:
                    if stmt.sql == sql:
                        stmt_id = stmt.id
                self.assertEqual(m.statements.get(stmt_id).id, stmt_id)
                # m.get_call()
                self.assertIsInstance(m.this_attachment, AttachmentInfo)
                self.assertEqual(m.this_attachment.id,
                                 self.con.info.id)
                self.assertFalse(m.closed)
    def test_03_DatabaseInfo(self):
        with Monitor(self.con) as m:
            self.assertEqual(m.db.name.upper(), self.dbfile.upper())
            self.assertEqual(m.db.page_size, 8192)
            if self.version == FB30:
                self.assertEqual(m.db.ods, 12.0)
            else:
                self.assertEqual(m.db.ods, 13.0)
            self.assertIsInstance(m.db.oit, int)
            self.assertIsInstance(m.db.oat, int)
            self.assertIsInstance(m.db.ost, int)
            self.assertIsInstance(m.db.next_transaction, int)
            self.assertIsInstance(m.db.cache_size, int)
            self.assertEqual(m.db.sql_dialect, 3)
            self.assertIs(m.db.shutdown_mode, ShutdownMode.ONLINE)
            self.assertEqual(m.db.sweep_interval, 20000)
            self.assertFalse(m.db.read_only)
            self.assertTrue(m.db.forced_writes)
            self.assertTrue(m.db.reserve_space)
            self.assertIsInstance(m.db.created, datetime.datetime)
            self.assertIsInstance(m.db.pages, int)
            self.assertIs(m.db.backup_state, BackupState.NORMAL)
            self.assertEqual(m.db.crypt_page, 0)
            self.assertEqual(m.db.owner, 'SYSDBA')
            self.assertIs(m.db.security, Security.DEFAULT)
            self.assertIs(m.db.iostats.group, Group.DATABASE)
            self.assertEqual(m.db.iostats.stat_id, m.db.stat_id)
            # TableStats
            for table_name, stats in m.db.tablestats.items():
                self.assertIsNotNone(self.con.schema.all_tables.get(table_name))
                self.assertIsInstance(stats, TableStatsInfo)
                self.assertEqual(stats.stat_id, m.db.stat_id)
                self.assertEqual(stats.owner, m.db)
    def test_04_AttachmentInfo(self):
        with Monitor(self.con) as m:
            sql = "select RDB$SET_CONTEXT('USER_SESSION','TESTVAR','TEST_VALUE') from rdb$database"
            with self.con.cursor() as c:
                c.execute(sql)
                c.fetchone()
                #
                s = m.this_attachment
                #
                self.assertEqual(s.id, self.con.info.id)
                self.assertIsInstance(s.server_pid, int)
                self.assertIsInstance(s.state, State)
                self.assertEqual(s.name.upper(), self.dbfile.upper())
                self.assertEqual(s.user, 'SYSDBA')
                self.assertEqual(s.role, 'NONE')
                self.assertIn(s.remote_protocol, ['XNET', 'TCPv4', 'TCPv6'])
                self.assertIsInstance(s.remote_address, str)
                self.assertIsInstance(s.remote_pid, int)
                self.assertIsInstance(s.remote_process, str)
                self.assertIsInstance(s.character_set, CharacterSet)
                self.assertEqual(s.character_set.name, 'UTF8')
                self.assertIsInstance(s.timestamp, datetime.datetime)
                self.assertIsInstance(s.transactions, list)
                self.assertIn(s.auth_method, ['Srp', 'Srp256', 'Win_Sspi', 'Legacy_Auth'])
                self.assertIsInstance(s.client_version, str)
                if self.version == FB30:
                    self.assertEqual(s.remote_version, 'P15')
                else:
                    self.assertEqual(s.remote_version, 'P17')
                self.assertIsInstance(s.remote_os_user, str)
                self.assertIsInstance(s.remote_host, str)
                self.assertFalse(s.system)
                for x in s.transactions:
                    self.assertIsInstance(x, TransactionInfo)
                self.assertIsInstance(s.statements, list)
                for x in s.statements:
                    self.assertIsInstance(x, StatementInfo)
                self.assertIsInstance(s.variables, list)
                self.assertGreater(len(s.variables), 0)
                for x in s.variables:
                    self.assertIsInstance(x, ContextVariableInfo)
                self.assertIs(s.iostats.group, Group.ATTACHMENT)
                self.assertEqual(s.iostats.stat_id, s.stat_id)
                self.assertGreater(len(m.db.tablestats), 0)
                #
                self.assertTrue(s.is_active())
                self.assertFalse(s.is_idle())
                self.assertFalse(s.is_internal())
                self.assertTrue(s.is_gc_allowed())
                # TableStats
                for table_name, stats in s.tablestats.items():
                    self.assertIsNotNone(self.con.schema.all_tables.get(table_name))
                    self.assertIsInstance(stats, TableStatsInfo)
                    self.assertEqual(stats.stat_id, s.stat_id)
                    self.assertEqual(stats.owner, s)
                # terminate
                with connect('fbtest'):
                    cnt = len(m.attachments)
                    m.take_snapshot()
                    self.assertEqual(len(m.attachments), cnt + 1)
                    att = m.attachments.find(lambda i: i.id != m.this_attachment.id and not i.is_internal())
                    self.assertIsNot(att, m.this_attachment)
                    att_id = att.id
                    att.terminate()
                    m.take_snapshot()
                    self.assertEqual(len(m.attachments), cnt)
                    self.assertIsNone(m.attachments.get(att_id))
                    # Current attachment
                    with self.assertRaises(Error) as cm:
                        m.this_attachment.terminate()
                    self.assertTupleEqual(cm.exception.args,
                                          ("Can't terminate current session.",))
    def test_05_TransactionInfo(self):
        c = self.con.cursor()
        sql = "select RDB$SET_CONTEXT('USER_TRANSACTION','TESTVAR','TEST_VALUE') from rdb$database"
        c.execute(sql)
        c.fetchone()
        #
        with Monitor(self.con) as m:
            m.take_snapshot()
            s = m.this_attachment.transactions[0]
            #
            self.assertEqual(s.id, m._ic.transaction.info.id)
            self.assertIs(s.attachment, m.this_attachment)
            self.assertIsInstance(s.state, State)
            self.assertIsInstance(s.timestamp, datetime.datetime)
            self.assertIsInstance(s.top, int)
            self.assertIsInstance(s.oldest, int)
            self.assertIsInstance(s.oldest_active, int)
            self.assertIs(s.isolation_mode, IsolationMode.READ_COMMITTED_RV)
            self.assertEqual(s.lock_timeout, -1)
            self.assertIsInstance(s.statements, list)
            for x in s.statements:
                self.assertIsInstance(x, StatementInfo)
            self.assertIsInstance(s.variables, list)
            self.assertIs(s.iostats.group, Group.TRANSACTION)
            self.assertEqual(s.iostats.stat_id, s.stat_id)
            self.assertGreater(len(m.db.tablestats), 0)
            #
            self.assertTrue(s.is_active())
            self.assertFalse(s.is_idle())
            self.assertTrue(s.is_readonly())
            self.assertFalse(s.is_autocommit())
            self.assertTrue(s.is_autoundo())
            #
            s = m.transactions.get(c.transaction.info.id)
            self.assertIsInstance(s.variables, list)
            self.assertGreater(len(s.variables), 0)
            for x in s.variables:
                self.assertIsInstance(x, ContextVariableInfo)
            # TableStats
            for table_name, stats in s.tablestats.items():
                self.assertIsNotNone(self.con.schema.all_tables.get(table_name))
                self.assertIsInstance(stats, TableStatsInfo)
                self.assertEqual(stats.stat_id, s.stat_id)
                self.assertEqual(stats.owner, s)
        c.close()
    def test_06_StatementInfo(self):
        with Monitor(self.con) as m:
            m.take_snapshot()
            s = m.this_attachment.statements[0]
            #
            self.assertIsInstance(s.id, int)
            self.assertIs(s.attachment, m.this_attachment)
            self.assertEqual(s.transaction.id, m.transactions[0].id)
            self.assertIsInstance(s.state, State)
            self.assertIsInstance(s.timestamp, datetime.datetime)
            self.assertEqual(s.sql, "select * from mon$attachments")
            self.assertEqual(s.plan, 'Select Expression\n    -> Table "MON$ATTACHMENTS" Full Scan')
            # We have to use mocks for callstack
            stack = DataList()
            stack.append(CallStackInfo(m,
                                       {'MON$CALL_ID':1, 'MON$STATEMENT_ID':s.id-1, 'MON$CALLER_ID':None,
                                        'MON$OBJECT_NAME':'TRIGGER_1', 'MON$OBJECT_TYPE':2, 'MON$TIMESTAMP':datetime.datetime.now(),
                                        'MON$SOURCE_LINE':1, 'MON$SOURCE_COLUMN':1, 'MON$STAT_ID':s.stat_id+100}))
            stack.append(CallStackInfo(m,
                                       {'MON$CALL_ID':2, 'MON$STATEMENT_ID':s.id, 'MON$CALLER_ID':None,
                                        'MON$OBJECT_NAME':'TRIGGER_2', 'MON$OBJECT_TYPE':2, 'MON$TIMESTAMP':datetime.datetime.now(),
                                        'MON$SOURCE_LINE':1, 'MON$SOURCE_COLUMN':1, 'MON$STAT_ID':s.stat_id+101}))
            stack.append(CallStackInfo(m,
                                       {'MON$CALL_ID':3, 'MON$STATEMENT_ID':s.id, 'MON$CALLER_ID':2,
                                        'MON$OBJECT_NAME':'PROC_1', 'MON$OBJECT_TYPE':5, 'MON$TIMESTAMP':datetime.datetime.now(),
                                        'MON$SOURCE_LINE':2, 'MON$SOURCE_COLUMN':2, 'MON$STAT_ID':s.stat_id+102}))
            stack.append(CallStackInfo(m,
                                       {'MON$CALL_ID':4, 'MON$STATEMENT_ID':s.id, 'MON$CALLER_ID':3,
                                        'MON$OBJECT_NAME':'PROC_2', 'MON$OBJECT_TYPE':5, 'MON$TIMESTAMP':datetime.datetime.now(),
                                        'MON$SOURCE_LINE':3, 'MON$SOURCE_COLUMN':3, 'MON$STAT_ID':s.stat_id+103}))
            stack.append(CallStackInfo(m,
                                                   {'MON$CALL_ID':5, 'MON$STATEMENT_ID':s.id+1, 'MON$CALLER_ID':None,
                                                    'MON$OBJECT_NAME':'PROC_3', 'MON$OBJECT_TYPE':5, 'MON$TIMESTAMP':datetime.datetime.now(),
                                                    'MON$SOURCE_LINE':1, 'MON$SOURCE_COLUMN':1, 'MON$STAT_ID':s.stat_id+104}))
            m.__dict__['_Monitor__callstack'] = stack
            #
            self.assertListEqual(s.callstack, [stack[1], stack[2], stack[3]])
            self.assertIs(s.iostats.group, Group.STATEMENT)
            self.assertEqual(s.iostats.stat_id, s.stat_id)
            self.assertGreater(len(m.db.tablestats), 0)
            #
            self.assertTrue(s.is_active())
            self.assertFalse(s.is_idle())
            # TableStats
            for table_name, stats in s.tablestats.items():
                self.assertIsNotNone(self.con.schema.all_tables.get(table_name))
                self.assertIsInstance(stats, TableStatsInfo)
                self.assertEqual(stats.stat_id, s.stat_id)
                self.assertEqual(stats.owner, s)
    def test_07_CallStackInfo(self):
        with Monitor(self.con) as m:
            m.take_snapshot()
            stmt = m.this_attachment.statements[0]
            # We have to use mocks for callstack
            stack = DataList(key_expr='item.id')
            stack.append(CallStackInfo(m,
                                       {'MON$CALL_ID':1, 'MON$STATEMENT_ID':stmt.id-1, 'MON$CALLER_ID':None,
                                        'MON$OBJECT_NAME':'POST_NEW_ORDER', 'MON$OBJECT_TYPE':2, 'MON$TIMESTAMP':datetime.datetime.now(),
                                        'MON$SOURCE_LINE':1, 'MON$SOURCE_COLUMN':1, 'MON$STAT_ID':stmt.stat_id+100}))
            stack.append(CallStackInfo(m,
                                       {'MON$CALL_ID':2, 'MON$STATEMENT_ID':stmt.id, 'MON$CALLER_ID':None,
                                        'MON$OBJECT_NAME':'POST_NEW_ORDER', 'MON$OBJECT_TYPE':2, 'MON$TIMESTAMP':datetime.datetime.now(),
                                        'MON$SOURCE_LINE':1, 'MON$SOURCE_COLUMN':1, 'MON$STAT_ID':stmt.stat_id+101}))
            stack.append(CallStackInfo(m,
                                       {'MON$CALL_ID':3, 'MON$STATEMENT_ID':stmt.id, 'MON$CALLER_ID':2,
                                        'MON$OBJECT_NAME':'SHIP_ORDER', 'MON$OBJECT_TYPE':5, 'MON$TIMESTAMP':datetime.datetime.now(),
                                        'MON$SOURCE_LINE':2, 'MON$SOURCE_COLUMN':2, 'MON$STAT_ID':stmt.stat_id+102}))
            stack.append(CallStackInfo(m,
                                       {'MON$CALL_ID':4, 'MON$STATEMENT_ID':stmt.id, 'MON$CALLER_ID':3,
                                        'MON$OBJECT_NAME':'SUB_TOT_BUDGET', 'MON$OBJECT_TYPE':5, 'MON$TIMESTAMP':datetime.datetime.now(),
                                        'MON$SOURCE_LINE':3, 'MON$SOURCE_COLUMN':3, 'MON$STAT_ID':stmt.stat_id+103}))
            stack.append(CallStackInfo(m,
                                       {'MON$CALL_ID':5, 'MON$STATEMENT_ID':stmt.id+1, 'MON$CALLER_ID':None,
                                        'MON$OBJECT_NAME':'SUB_TOT_BUDGET', 'MON$OBJECT_TYPE':5, 'MON$TIMESTAMP':datetime.datetime.now(),
                                        'MON$SOURCE_LINE':1, 'MON$SOURCE_COLUMN':1, 'MON$STAT_ID':stmt.stat_id+104}))
            m.__dict__['_Monitor__callstack'] = stack
            data = m.iostats[0]._attributes
            data['MON$STAT_ID'] = stmt.stat_id+101
            data['MON$STAT_GROUP'] = Group.CALL.value
            m.__dict__['_Monitor__iostats'] = DataList(m.iostats, IOStatsInfo,
                                                       'item.stat_id')
            m.__dict__['_Monitor__iostats'].append(IOStatsInfo(m, data))
            m.__dict__['_Monitor__iostats'].freeze()
            #
            s = m.callstack.get(2)
            #
            self.assertEqual(s.id, 2)
            self.assertIs(s.statement, m.statements.get(stmt.id))
            self.assertIsNone(s.caller)
            self.assertIsInstance(s.dbobject, Trigger)
            self.assertEqual(s.dbobject.name, 'POST_NEW_ORDER')
            self.assertEqual(s.object_type, 2) # trigger
            self.assertEqual(s.object_name, 'POST_NEW_ORDER')
            self.assertIsInstance(s.timestamp, datetime.datetime)
            self.assertEqual(s.line, 1)
            self.assertEqual(s.column, 1)
            self.assertIs(s.iostats.group, Group.CALL)
            self.assertEqual(s.iostats.stat_id, s.stat_id)
            self.assertEqual(s.iostats.owner, s)
            self.assertIsNone(s.package_name)
            #
            x = m.callstack.get(3)
            self.assertIs(x.caller, s)
            self.assertIsInstance(x.dbobject, Procedure)
            self.assertEqual(x.dbobject.name, 'SHIP_ORDER')
            self.assertEqual(x.object_type, 5) # procedure
            self.assertEqual(x.object_name, 'SHIP_ORDER')
    def test_08_IOStatsInfo(self):
        with Monitor(self.con) as m:
            m.take_snapshot()
            #
            for io in m.iostats:
                self.assertIs(io, io.owner.iostats)
            #
            s = m.iostats[0]
            self.assertIsInstance(s.owner, DatabaseInfo)
            self.assertIs(s.group, Group.DATABASE)
            self.assertIsInstance(s.reads, int)
            self.assertIsInstance(s.writes, int)
            self.assertIsInstance(s.fetches, int)
            self.assertIsInstance(s.marks, int)
            self.assertIsInstance(s.seq_reads, int)
            self.assertIsInstance(s.idx_reads, int)
            self.assertIsInstance(s.inserts, int)
            self.assertIsInstance(s.updates, int)
            self.assertIsInstance(s.deletes, int)
            self.assertIsInstance(s.backouts, int)
            self.assertIsInstance(s.purges, int)
            self.assertIsInstance(s.expunges, int)
            self.assertIsInstance(s.locks, int)
            self.assertIsInstance(s.waits, int)
            self.assertIsInstance(s.conflits, int)
            self.assertIsInstance(s.backversion_reads, int)
            self.assertIsInstance(s.fragment_reads, int)
            self.assertIsInstance(s.repeated_reads, int)
            self.assertIsInstance(s.memory_used, int)
            self.assertIsInstance(s.memory_allocated, int)
            self.assertIsInstance(s.max_memory_used, int)
            self.assertIsInstance(s.max_memory_allocated, int)
    def test_09_ContextVariableInfo(self):
        c = self.con.cursor()
        sql = "select RDB$SET_CONTEXT('USER_SESSION','SVAR','TEST_VALUE') from rdb$database"
        c.execute(sql)
        c.fetchone()
        c2 = self.con.cursor()
        sql = "select RDB$SET_CONTEXT('USER_TRANSACTION','TVAR','TEST_VALUE') from rdb$database"
        c2.execute(sql)
        c2.fetchone()
        #
        with Monitor(self.con) as m:
            m.take_snapshot()
            #
            self.assertEqual(len(m.variables), 2)
            #
            s = m.variables[0]
            self.assertIs(s.attachment, m.this_attachment)
            self.assertIsNone(s.transaction)
            self.assertEqual(s.name, 'SVAR')
            self.assertEqual(s.value, 'TEST_VALUE')
            self.assertTrue(s.is_attachment_var())
            self.assertFalse(s.is_transaction_var())
            #
            s = m.variables[1]
            self.assertIsNone(s.attachment)
            self.assertIs(s.transaction,
                          m.transactions.get(c.transaction.info.id))
            self.assertEqual(s.name, 'TVAR')
            self.assertEqual(s.value, 'TEST_VALUE')
            self.assertFalse(s.is_attachment_var())
            self.assertTrue(s.is_transaction_var())
        c.close()
        c2.close()

if __name__ == '__main__':
    unittest.main()

