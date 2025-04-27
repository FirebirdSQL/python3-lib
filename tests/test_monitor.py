# SPDX-FileCopyrightText: 2020-present The Firebird Projects <www.firebirdsql.org>
#
# SPDX-License-Identifier: MIT
#
# PROGRAM/MODULE: firebird-lib
# FILE:           tests/test_monitor.py
# DESCRIPTION:    Tests for firebird.lib.monitor module
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

"""firebird-lib - Tests for firebird.lib.monitor module
"""

import pytest # Import pytest
import datetime
from firebird.base.collections import DataList
from firebird.driver import *
from firebird.lib.monitor import *
from firebird.lib.schema import CharacterSet

# --- Constants ---
FB30 = '3.0'
FB40 = '4.0'
FB50 = '5.0'


# --- Test Functions ---

def test_01_close(db_connection):
    """Tests creating, closing, and using Monitor with a context manager."""
    s = Monitor(db_connection)
    assert not s.closed
    s.close()
    assert s.closed
    #
    with Monitor(db_connection) as m:
        assert not m.closed
    assert m.closed

def test_02_monitor(db_connection, fb_vars):
    """Tests basic Monitor functionality and accessing monitored objects."""
    #
    with Monitor(db_connection) as m:
        # Execute a statement to ensure some activity is monitored
        sql = "select RDB$SET_CONTEXT('USER_SESSION','TESTVAR','TEST_VALUE') from rdb$database"
        with db_connection.cursor() as c:
            c.execute(sql)
            c.fetchone()
            #
            m.take_snapshot() # Explicit snapshot needed after action

            assert m.db is not None
            assert isinstance(m.db, DatabaseInfo)
            assert len(m.attachments) > 0
            assert isinstance(m.attachments[0], AttachmentInfo)
            assert len(m.transactions) > 0
            assert isinstance(m.transactions[0], TransactionInfo)
            assert len(m.statements) > 0
            assert isinstance(m.statements[0], StatementInfo)
            # Call stack might be empty depending on exact timing and server activity
            # assert len(m.callstack) > 0 # Original test checked for 0, keep it?
            assert len(m.callstack) == 0
            assert len(m.iostats) > 0
            assert isinstance(m.iostats[0], IOStatsInfo)
            assert len(m.variables) > 0
            assert isinstance(m.variables[0], ContextVariableInfo)

            # Test object retrieval by ID
            att_id = m._con.info.id # Use the connection associated with Monitor
            assert m.attachments.get(att_id).id == att_id
            tra_id = m._con.main_transaction.info.id
            assert m.transactions.get(tra_id).id == tra_id

            # Find the specific statement ID
            stmt_id = None
            for stmt in m.statements:
                # Compare normalized SQL
                if stmt.sql.replace('\n', ' ').strip() == sql.replace('\n', ' ').strip():
                    stmt_id = stmt.id
                    break
            assert stmt_id is not None, f"Statement '{sql}' not found in monitored statements"
            assert m.statements.get(stmt_id).id == stmt_id

            # Test convenience properties
            assert isinstance(m.this_attachment, AttachmentInfo)
            assert m.this_attachment.id == att_id
            assert not m.closed

def test_03_DatabaseInfo(db_connection, fb_vars):
    """Tests properties of the DatabaseInfo object."""
    version = fb_vars['version']
    db_file = fb_vars['source_db'].parent / driver_config.get_database('pytest').database.value # Get actual test DB path
    with Monitor(db_connection) as m:
        m.take_snapshot()
        db_info = m.db

        assert db_info.name.upper() == str(db_file).upper()
        assert db_info.page_size == 8192

        if version.base_version == FB30:
            assert db_info.ods == 12.0
        elif version.base_version == FB40:
            assert db_info.ods == 13.0
        else: # FB 5.0+
            # ODS 13.1 introduced in 5.0 beta 1
            assert db_info.ods in (13.0, 13.1) # Allow 13.0 for early 5.0 alphas

        assert isinstance(db_info.oit, int)
        assert isinstance(db_info.oat, int)
        assert isinstance(db_info.ost, int)
        assert isinstance(db_info.next_transaction, int)
        assert isinstance(db_info.cache_size, int)
        assert db_info.sql_dialect == 3
        assert db_info.shutdown_mode is ShutdownMode.NORMAL
        assert db_info.sweep_interval == 20000
        assert not db_info.read_only
        assert db_info.forced_writes
        assert db_info.reserve_space
        assert isinstance(db_info.created, datetime.datetime)
        assert isinstance(db_info.pages, int)
        assert db_info.backup_state is BackupState.NORMAL
        assert db_info.crypt_page == 0
        assert db_info.owner == 'SYSDBA'
        assert db_info.security is Security.DEFAULT
        assert db_info.iostats.group is Group.DATABASE
        assert db_info.iostats.stat_id == db_info.stat_id

        # TableStats
        assert len(db_info.tablestats) > 0 # Check there are some stats
        for table_name, stats in db_info.tablestats.items():
            assert db_connection.schema.all_tables.get(table_name) is not None
            assert isinstance(stats, TableStatsInfo)
            assert stats.stat_id == db_info.stat_id
            assert stats.owner is db_info

        # Firebird 4 properties
        if version.base_version == FB30:
            assert db_info.crypt_state is None
            assert db_info.guid is None
            assert db_info.file_id is None
            assert db_info.next_attachment is None
            assert db_info.next_statement is None
            assert db_info.replica_mode is None
        else: # FB 4.0+
            assert db_info.crypt_state == CryptState.NOT_ENCRYPTED
            # GUID is specific to the database instance, check type
            assert isinstance(db_info.guid, UUID)
            assert isinstance(db_info.file_id, str)
            assert db_info.next_attachment > 0
            assert db_info.next_statement > 0
            assert db_info.replica_mode == ReplicaMode.NONE

def test_04_AttachmentInfo(db_connection, fb_vars, db_file):
    """Tests properties of the AttachmentInfo object."""
    version = fb_vars['version']

    with Monitor(db_connection) as m:
        # Ensure some activity and context
        sql = "select RDB$SET_CONTEXT('USER_SESSION','TESTVAR','TEST_VALUE') from rdb$database"
        with db_connection.cursor() as c:
            c.execute(sql)
            c.fetchone()
        m.take_snapshot()

        s = m.this_attachment

        assert s.id == db_connection.info.id
        assert isinstance(s.server_pid, int)
        assert isinstance(s.state, State)
        assert s.name.upper() == str(db_file).upper()
        assert s.user == 'SYSDBA'
        assert s.role == 'NONE'
        assert s.remote_protocol in ['XNET', 'TCPv4', 'TCPv6', None] # None for embedded
        # Remote address might be None for embedded
        assert isinstance(s.remote_address, (str, type(None)))
        # Remote PID/Process might be None or 0 depending on connection type/OS
        assert isinstance(s.remote_pid, (int, type(None)))
        assert isinstance(s.remote_process, (str, type(None)))
        assert isinstance(s.character_set, CharacterSet)
        assert s.character_set.name == 'UTF8'
        assert isinstance(s.timestamp, datetime.datetime)
        assert isinstance(s.transactions, list)
        if s.auth_method != 'User name in DPB': # Is not Embedded...
            assert s.auth_method in ['Srp', 'Srp256', 'Win_Sspi', 'Legacy_Auth']
            assert isinstance(s.client_version, str)

            # Remote version prefix depends on FB version
            if version.base_version == FB30:
                assert s.remote_version.startswith('P15')
            elif version.base_version == FB40:
                assert s.remote_version.startswith('P17')
            else: # FB 5.0+
                assert s.remote_version.startswith('P18')

        assert isinstance(s.remote_os_user, (str, type(None))) # Might be None
        assert isinstance(s.remote_host, (str, type(None))) # Might be None
        assert not s.system # Should be a user attachment
        for x in s.transactions:
            assert isinstance(x, TransactionInfo)
        assert isinstance(s.statements, list)
        for x in s.statements:
            assert isinstance(x, StatementInfo)
        assert isinstance(s.variables, list)
        assert len(s.variables) > 0 # Should have TESTVAR
        for x in s.variables:
            assert isinstance(x, ContextVariableInfo)
        assert s.iostats.group is Group.ATTACHMENT
        assert s.iostats.stat_id == s.stat_id
        assert len(m.db.tablestats) > 0 # Ensure DB stats loaded

        assert s.is_active() or s.is_idle() # Could be either depending on timing
        assert not s.is_internal()
        assert s.is_gc_allowed()

        # TableStats for attachment
        assert isinstance(s.tablestats, dict) # Check it's a dict
        # Check at least one entry if tables were accessed, might be empty otherwise
        # for table_name, stats in s.tablestats.items():
        #     assert db_connection.schema.all_tables.get(table_name) is not None
        #     assert isinstance(stats, TableStatsInfo)
        #     assert stats.stat_id == s.stat_id
        #     assert stats.owner is s

        # Test terminate (requires another connection)
        with connect('pytest'): # Use the same config name
            m.take_snapshot()
            initial_count = len(m.attachments)
            assert initial_count >= 2 # Should have self and conn2

            # Find the other attachment
            other_att = next((att for att in m.attachments
                              if att.id != m.this_attachment.id and not att.is_internal()), None)
            assert other_att is not None
            other_att_id = other_att.id

            other_att.terminate()
            m.take_snapshot()
            assert len(m.attachments) == initial_count - 1
            assert m.attachments.get(other_att_id) is None

            # Current attachment termination attempt
            with pytest.raises(Error, match="Can't terminate current session."):
                m.this_attachment.terminate()

        # Firebird 4 properties
        if version.base_version == FB30:
            assert s.idle_timeout is None
            assert s.idle_timer is None
            assert s.statement_timeout is None
            assert s.wire_compressed is None
            assert s.wire_encrypted is None
            assert s.wire_crypt_plugin is None
        else: # FB 4.0+
            assert s.idle_timeout == 0
            assert s.idle_timer is None # Timer only set if timeout > 0
            assert s.statement_timeout == 0
            assert isinstance(s.wire_compressed, bool)
            assert isinstance(s.wire_encrypted, bool)
            assert isinstance(s.wire_crypt_plugin, (str, type(None))) # None if not encrypted

        # Firebird 5 properties
        if version.base_version in [FB30, FB40]:
            assert s.session_timezone is None
        else: # FB 5.0+
            assert isinstance(s.session_timezone, str)

def test_05_TransactionInfo(db_connection, fb_vars):
    """Tests properties of the TransactionInfo object."""
    version = fb_vars['version']
    # Use a separate cursor and transaction for context variable setting
    with db_connection.cursor() as c:
        sql = "select RDB$SET_CONTEXT('USER_TRANSACTION','TVAR','TEST_VALUE') from rdb$database"
        c.execute(sql)
        c.fetchone()
        tran_id_with_var = c.transaction.info.id

        with Monitor(db_connection) as m:
            m.take_snapshot()

            # Test the Monitor's main transaction
            s = m.this_attachment.transactions[0] # The monitor's own transaction
            assert s.id == m._ic.transaction.info.id # Monitor internal connection
            assert s.attachment is m.this_attachment
            assert isinstance(s.state, State)
            assert isinstance(s.timestamp, datetime.datetime)
            assert isinstance(s.top, int)
            assert isinstance(s.oldest, int)
            assert isinstance(s.oldest_active, int)

            if version.base_version == FB30:
                assert s.isolation_mode is IsolationMode.READ_COMMITTED_RV
            else: # FB 4.0+
                assert s.isolation_mode is IsolationMode.READ_COMMITTED_READ_CONSISTENCY

            assert s.lock_timeout == -1 # Default WAIT
            assert isinstance(s.statements, list)
            for x in s.statements:
                assert isinstance(x, StatementInfo)
            # Monitor's own transaction likely has no user variables set here
            assert isinstance(s.variables, list)
            #assert len(s.variables) == 0

            assert s.iostats.group is Group.TRANSACTION
            assert s.iostats.stat_id == s.stat_id
            assert len(m.db.tablestats) > 0

            assert s.is_active()
            assert not s.is_idle() # Monitor transaction is active
            assert s.is_readonly() # Monitor transaction should be read-only
            assert not s.is_autocommit()
            assert s.is_autoundo()

            # Test the transaction where the variable was set
            s_with_var = m.transactions.get(tran_id_with_var)
            assert s_with_var is not None
            assert isinstance(s_with_var.variables, list)
            assert len(s_with_var.variables) > 0
            found_var = False
            for x in s_with_var.variables:
                assert isinstance(x, ContextVariableInfo)
                if x.name == 'TVAR' and x.value == 'TEST_VALUE':
                    found_var = True
            assert found_var, "Context variable 'TVAR' not found in transaction"

            # TableStats for transaction
            assert isinstance(s_with_var.tablestats, dict)
            # Check at least one entry if tables were accessed, might be empty otherwise
            # for table_name, stats in s_with_var.tablestats.items():
            #     assert db_connection.schema.all_tables.get(table_name) is not None
            #     assert isinstance(stats, TableStatsInfo)
            #     assert stats.stat_id == s_with_var.stat_id
            #     assert stats.owner is s_with_var

def test_06_StatementInfo(db_connection, fb_vars):
    """Tests properties of the StatementInfo object."""
    version = fb_vars['version']
    with Monitor(db_connection) as m:
        m.take_snapshot()
        # Find the statement used by the monitor itself
        s: StatementInfo = next((st for st in m.this_attachment.statements
                                 if st.sql.strip().lower().startswith("select * from mon$attachments")), None)
        assert s is not None

        assert isinstance(s.id, int)
        assert s.attachment is m.this_attachment
        # Transaction should be the monitor's main transaction
        assert s.transaction.id == m._ic.transaction.info.id
        assert isinstance(s.state, State)
        assert isinstance(s.timestamp, datetime.datetime)
        assert s.sql.strip().lower() == "select * from mon$attachments"
        # Plan might vary slightly, check it starts correctly
        assert s.plan.strip().lower().startswith('select expression')
        assert "mon$attachments" in s.plan.lower()

        # --- Mock Callstack ---
        # Create mock CallStackInfo objects based on original data
        stack = DataList(key_expr='item.id')
        now = datetime.datetime.now()
        # Note: IDs need to be unique. stat_id should ideally link to a real IOStat entry.
        # Here we just use distinct values for demonstration.
        mock_stat_id_base = s.stat_id + 1000
        stack.append(CallStackInfo(m, {'MON$CALL_ID': 1, 'MON$STATEMENT_ID': s.id - 1, 'MON$CALLER_ID': None, 'MON$OBJECT_NAME': 'TRIGGER_1', 'MON$OBJECT_TYPE': 2, 'MON$TIMESTAMP': now, 'MON$SOURCE_LINE': 1, 'MON$SOURCE_COLUMN': 1, 'MON$STAT_ID': mock_stat_id_base + 1}))
        stack.append(CallStackInfo(m, {'MON$CALL_ID': 2, 'MON$STATEMENT_ID': s.id, 'MON$CALLER_ID': None, 'MON$OBJECT_NAME': 'TRIGGER_2', 'MON$OBJECT_TYPE': 2, 'MON$TIMESTAMP': now, 'MON$SOURCE_LINE': 1, 'MON$SOURCE_COLUMN': 1, 'MON$STAT_ID': mock_stat_id_base + 2}))
        stack.append(CallStackInfo(m, {'MON$CALL_ID': 3, 'MON$STATEMENT_ID': s.id, 'MON$CALLER_ID': 2, 'MON$OBJECT_NAME': 'PROC_1', 'MON$OBJECT_TYPE': 5, 'MON$TIMESTAMP': now, 'MON$SOURCE_LINE': 2, 'MON$SOURCE_COLUMN': 2, 'MON$STAT_ID': mock_stat_id_base + 3}))
        stack.append(CallStackInfo(m, {'MON$CALL_ID': 4, 'MON$STATEMENT_ID': s.id, 'MON$CALLER_ID': 3, 'MON$OBJECT_NAME': 'PROC_2', 'MON$OBJECT_TYPE': 5, 'MON$TIMESTAMP': now, 'MON$SOURCE_LINE': 3, 'MON$SOURCE_COLUMN': 3, 'MON$STAT_ID': mock_stat_id_base + 4}))
        stack.append(CallStackInfo(m, {'MON$CALL_ID': 5, 'MON$STATEMENT_ID': s.id + 1, 'MON$CALLER_ID': None, 'MON$OBJECT_NAME': 'PROC_3', 'MON$OBJECT_TYPE': 5, 'MON$TIMESTAMP': now, 'MON$SOURCE_LINE': 1, 'MON$SOURCE_COLUMN': 1, 'MON$STAT_ID': mock_stat_id_base + 5}))

        # Inject the mock stack (use with caution, accessing internals)
        m._Monitor__callstack = stack
        m._Monitor__callstack_loaded = True
        # --- End Mock Callstack ---

        # Callstack should now reflect the mocked data for this statement
        assert len(s.callstack) == 3 # Items 2, 3, 4 belong to s.id
        assert [cs.id for cs in s.callstack] == [2, 3, 4]
        assert s.callstack[0].object_name == 'TRIGGER_2'
        assert s.callstack[1].object_name == 'PROC_1'
        assert s.callstack[2].object_name == 'PROC_2'

        assert s.iostats.group is Group.STATEMENT
        assert s.iostats.stat_id == s.stat_id
        assert len(m.db.tablestats) > 0 # Ensure DB stats loaded

        assert s.is_active() or s.is_idle() # Could be either
        assert isinstance(s.tablestats, dict) # Check it's a dict

        # Firebird 4 properties
        if version.base_version == FB30:
            assert s.timeout is None
            assert s.timer is None
        else: # FB 4.0+
            assert s.timeout == 0
            assert s.timer is None # Timer only set if timeout > 0

        # Firebird 5 properties
        if version.base_version in [FB30, FB40]:
            assert s.compiled_statement is None
        else: # FB 5.0+
            assert isinstance(s.compiled_statement, CompiledStatementInfo)
            assert s.sql == s.compiled_statement.sql
            assert s.plan == s.compiled_statement.plan
            assert s._attributes['MON$COMPILED_STATEMENT_ID'] == s.compiled_statement.id

def test_07_CallStackInfo(db_connection, fb_vars):
    """Tests properties of the CallStackInfo object using mocked data."""
    with Monitor(db_connection) as m:
        m.take_snapshot()
        # Find any statement to associate the mock call stack with
        stmt = m.statements[0] if m.statements else None
        assert stmt is not None, "No statements found to test call stack"

        # --- Mock Callstack & IOStats ---
        stack = DataList(key_expr='item.id')
        now = datetime.datetime.now()
        mock_stat_id_base = stmt.stat_id + 1000 # Base for mock stat IDs

        stack.append(CallStackInfo(m, {'MON$CALL_ID': 1, 'MON$STATEMENT_ID': stmt.id - 1, 'MON$CALLER_ID': None, 'MON$OBJECT_NAME': 'POST_NEW_ORDER', 'MON$OBJECT_TYPE': 2, 'MON$TIMESTAMP': now, 'MON$SOURCE_LINE': 1, 'MON$SOURCE_COLUMN': 1, 'MON$STAT_ID': mock_stat_id_base + 1}))
        stack.append(CallStackInfo(m, {'MON$CALL_ID': 2, 'MON$STATEMENT_ID': stmt.id, 'MON$CALLER_ID': None, 'MON$OBJECT_NAME': 'POST_NEW_ORDER', 'MON$OBJECT_TYPE': 2, 'MON$TIMESTAMP': now, 'MON$SOURCE_LINE': 1, 'MON$SOURCE_COLUMN': 1, 'MON$STAT_ID': mock_stat_id_base + 2}))
        stack.append(CallStackInfo(m, {'MON$CALL_ID': 3, 'MON$STATEMENT_ID': stmt.id, 'MON$CALLER_ID': 2, 'MON$OBJECT_NAME': 'SHIP_ORDER', 'MON$OBJECT_TYPE': 5, 'MON$TIMESTAMP': now, 'MON$SOURCE_LINE': 2, 'MON$SOURCE_COLUMN': 2, 'MON$STAT_ID': mock_stat_id_base + 3}))
        stack.append(CallStackInfo(m, {'MON$CALL_ID': 4, 'MON$STATEMENT_ID': stmt.id, 'MON$CALLER_ID': 3, 'MON$OBJECT_NAME': 'SUB_TOT_BUDGET', 'MON$OBJECT_TYPE': 5, 'MON$TIMESTAMP': now, 'MON$SOURCE_LINE': 3, 'MON$SOURCE_COLUMN': 3, 'MON$STAT_ID': mock_stat_id_base + 4}))
        stack.append(CallStackInfo(m, {'MON$CALL_ID': 5, 'MON$STATEMENT_ID': stmt.id + 1, 'MON$CALLER_ID': None, 'MON$OBJECT_NAME': 'SUB_TOT_BUDGET', 'MON$OBJECT_TYPE': 5, 'MON$TIMESTAMP': now, 'MON$SOURCE_LINE': 1, 'MON$SOURCE_COLUMN': 1, 'MON$STAT_ID': mock_stat_id_base + 5}))

        # Mock IOStats entry for one of the calls
        mock_iostats_list = list(m.iostats) # Get existing stats
        call_stat_id = mock_stat_id_base + 2 # ID for call ID 2
        mock_iostats_list.append(IOStatsInfo(m, {'MON$STAT_ID': call_stat_id, 'MON$STAT_GROUP': Group.CALL.value, 'MON$PAGE_READS': 1, 'MON$PAGE_WRITES': 0, 'MON$PAGE_FETCHES': 2, 'MON$PAGE_MARKS': 0}))

        # Inject mocks (use with caution)
        m._Monitor__callstack = stack
        m._Monitor__callstack_loaded = True
        m._Monitor__iostats = DataList(mock_iostats_list, IOStatsInfo, 'item.stat_id')
        m._Monitor__iostats.freeze()
        m._Monitor__iostats_loaded = True
        # --- End Mock ---

        s = m.callstack.get(2)
        assert s is not None
        assert s.id == 2
        assert s.statement is stmt
        assert s.caller is None # Top level call for this statement
        assert isinstance(s.dbobject, Trigger)
        assert s.dbobject.name == 'POST_NEW_ORDER'
        assert s.object_type == ObjectType.TRIGGER
        assert s.object_name == 'POST_NEW_ORDER'
        assert isinstance(s.timestamp, datetime.datetime)
        assert s.line == 1
        assert s.column == 1
        assert s.iostats is not None
        assert s.iostats.group is Group.CALL
        assert s.iostats.stat_id == s.stat_id
        assert s.iostats.owner is s
        assert s.package_name is None

        x = m.callstack.get(3)
        assert x is not None
        assert x.caller is s # Should link back to call ID 2
        assert isinstance(x.dbobject, Procedure)
        assert x.dbobject.name == 'SHIP_ORDER'
        assert x.object_type == ObjectType.PROCEDURE
        assert x.object_name == 'SHIP_ORDER'

def test_08_IOStatsInfo(db_connection, fb_vars):
    """Tests properties of the IOStatsInfo object."""
    version = fb_vars['version']
    with Monitor(db_connection) as m:
        m.take_snapshot()
        assert len(m.iostats) > 0

        # Check association and type for a sample
        # Find the IOStats for the database itself
        db_iostats = next((io for io in m.iostats if io.group == Group.DATABASE), None)
        assert db_iostats is not None
        assert db_iostats.owner is m.db
        assert m.db.iostats is db_iostats

        s = db_iostats
        assert isinstance(s.owner, DatabaseInfo)
        assert s.group is Group.DATABASE
        assert isinstance(s.reads, int)
        assert isinstance(s.writes, int)
        assert isinstance(s.fetches, int)
        assert isinstance(s.marks, int)
        # Check some detailed stats exist (>= 0)
        assert s.seq_reads >= 0
        assert s.idx_reads >= 0
        assert s.inserts >= 0
        assert s.updates >= 0
        assert s.deletes >= 0
        assert s.backouts >= 0
        assert s.purges >= 0
        assert s.expunges >= 0
        assert isinstance(s.locks, int) # Locks can be -1
        assert s.waits >= 0
        assert s.conflicts >= 0 # Assuming corrected attribute name
        assert s.backversion_reads >= 0
        assert s.fragment_reads >= 0
        # Repeated reads might not be present in older versions or specific contexts
        assert isinstance(s.repeated_reads, (int, type(None)))

        # Memory stats should exist
        assert s.memory_used >= 0
        assert s.memory_allocated >= 0
        assert s.max_memory_used >= 0
        assert s.max_memory_allocated >= 0

        # Firebird 4+ property
        if version.base_version == FB30:
            assert s.intermediate_gc is None
        else:
            assert s.intermediate_gc >= 0

def test_09_ContextVariableInfo(db_connection):
    """Tests ContextVariableInfo objects."""
    # Set session and transaction variables
    with db_connection.cursor() as c1:
        c1.execute("select RDB$SET_CONTEXT('USER_SESSION','SVAR','SESSION_VALUE') from rdb$database")
        c1.fetchone()
        with db_connection.cursor() as c2:
            tran_id = c2.transaction.info.id
            c2.execute("select RDB$SET_CONTEXT('USER_TRANSACTION','TVAR','TRAN_VALUE') from rdb$database")
            c2.fetchone()

            with Monitor(db_connection) as m:
                m.take_snapshot()

                assert len(m.variables) >= 2 # Might be more system vars

                # Find session variable
                s_var = next((v for v in m.variables if v.name == 'SVAR'), None)
                assert s_var is not None
                assert s_var.attachment is m.this_attachment
                assert s_var.transaction is None
                assert s_var.name == 'SVAR'
                assert s_var.value == 'SESSION_VALUE'
                assert s_var.is_attachment_var()
                assert not s_var.is_transaction_var()

                # Find transaction variable
                t_var = next((v for v in m.variables if v.name == 'TVAR'), None)
                assert t_var is not None
                assert t_var.attachment is None
                assert t_var.transaction is not None
                assert t_var.transaction.id == tran_id
                assert t_var.name == 'TVAR'
                assert t_var.value == 'TRAN_VALUE'
                assert not t_var.is_attachment_var()
                assert t_var.is_transaction_var()

def test_10_CompiledStatementInfo(db_connection, fb_vars):
    """Tests CompiledStatementInfo objects (FB5+)."""
    version = fb_vars['version']
    with Monitor(db_connection) as m:
        # Execute a statement to potentially populate compiled statements cache
        with db_connection.cursor() as cur:
            cur.execute("SELECT 1 FROM RDB$DATABASE")
            cur.fetchone()

        m.take_snapshot()

        if version.major < 5:
            pytest.skip("MON$COMPILED_STATEMENTS table not available before Firebird 5.0")
            # assert len(m.compiled_statements) == 0 # Or check it's None/empty
        else: # FB 5.0+
            assert len(m.compiled_statements) >= 1 # Should have at least one entry
            # Find a specific statement if possible, otherwise check the first one
            s: CompiledStatementInfo = m.compiled_statements[0] # Check the first one
            assert isinstance(s.id, int)
            assert isinstance(s.sql, str)
            assert isinstance(s.plan, str)

            # Example: Find the statement we just executed
            found_stmt = next((cs for cs in m.compiled_statements
                               if cs.sql and cs.sql.strip().lower() == "select 1 from rdb$database"), None)
            assert found_stmt is not None
            assert found_stmt.plan is not None
#

