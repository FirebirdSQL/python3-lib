#coding:utf-8
#
# PROGRAM/MODULE: firebird-lib
# FILE:           firebird/lib/monitor.py
# DESCRIPTION:    Module for work with Firebird monitoring tables
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
# pylint: disable=C0302, W0212, R0902, R0912,R0913, R0914, R0915, R0904, R0903, C0103, C0301

"""firebird.lib.monitor - Module for work with Firebird monitoring tables


"""

from __future__ import annotations
from typing import Dict, List, Any, Union
import datetime
import weakref
from enum import Enum, IntEnum
from firebird.base.collections import DataList
from firebird.driver import tpb, Connection, Cursor, Statement, Isolation, Error, TraAccessMode
from firebird.lib.schema import ObjectType, CharacterSet, Procedure, Trigger, Function

FLAG_NOT_SET = 0
FLAG_SET = 1

# Enums
class ShutdownMode(IntEnum):
    """Shutdown mode.
    """
    ONLINE = 0
    MULTI = 1
    SINGLE = 2
    FULL = 3

class BackupState(IntEnum):
    """Physical backup state.
    """
    NORMAL = 0
    STALLED = 1
    MERGE = 2

class State(IntEnum):
    """Object state.
    """
    IDLE = 0
    ACTIVE = 1

class IsolationMode(IntEnum):
    """Transaction solation mode.
    """
    CONSISTENCY = 0
    CONCURRENCY = 1
    READ_COMMITTED_RV = 2
    READ_COMMITTED_NO_RV = 3

class Group(IntEnum):
    """Statistics group.
    """
    DATABASE = 0
    ATTACHMENT = 1
    TRANSACTION = 2
    STATEMENT = 3
    CALL = 4

class Security(Enum):
    """Security database.
    """
    DEFAULT = 'Default'
    SELF = 'Self'
    OTHER = 'Other'

# Classes
class Monitor:
    """Class for access to Firebird monitoring tables.
    """
    def __init__(self, connection: Connection):
        """
        Arguments:
            connection: Connection that should be used to access monitoring tables.
        """
        self._con: Connection = connection
        self._ic: Cursor = self._con.transaction_manager(tpb(Isolation.READ_COMMITTED_RECORD_VERSION,
                                                             access_mode=TraAccessMode.READ)).cursor()
        self._ic._logging_id_ = 'monitor.internal_cursor'
        self.__internal: bool = False # pylint: disable=W0238
        self._con_id: int = connection.info.id
        #
        self.__database = None
        self.__attachments = None
        self.__transactions = None
        self.__statements = None
        self.__callstack = None
        self.__iostats = None
        self.__variables = None
        self.__tablestats = None
    def __del__(self):
        if not self.closed:
            self.close()
    def __enter__(self) -> Monitor:
        return self
    def __exit__(self, exc_type, exc_value, traceback) -> None:
        self.close()
    def _select_row(self, cmd: Union[Statement, str], params: List=None) -> Dict[str, Any]:
        self._ic.execute(cmd, params)
        row = self._ic.fetchone()
        return {self._ic.description[i][0]: row[i] for i in range(len(row))}
    def _select(self, cmd: str, params: List=None) -> Dict[str, Any]:
        self._ic.execute(cmd, params)
        desc = self._ic.description
        return ({desc[i][0]: row[i] for i in range(len(row))} for row in self._ic)
    def _set_internal(self, value: bool) -> None:
        self.__internal = value # pylint: disable=W0238
    def clear(self):
        """Clear all data fetched from monitoring tables.

        Note:
            A snapshot is created the first time any of the monitoring information
            is being accessed.
        """
        if self._ic.transaction.is_active():
            self._ic.transaction.commit()
        self.__database = None
        self.__attachments = None
        self.__transactions = None
        self.__statements = None
        self.__callstack = None
        self.__iostats = None
        self.__variables = None
        self.__tablestats = None
    def close(self) -> None:
        """Sever link to `~firebird.driver.Connection`.
        """
        if self._ic.transaction.is_active():
            self._ic.transaction.commit()
        self._ic.close()
        self._con = None
        self._ic = None
    def take_snapshot(self) -> None:
        """Takes fresh snapshot of the monitoring information.
        """
        self.clear()
        self._ic.transaction.begin()
    @property
    def closed(self) -> bool:
        """True if link to `~firebird.driver.core.Connection` is closed.
        """
        return self._con is None
    @property
    def db(self) -> DatabaseInfo:
        """`.DatabaseInfo` object for attached database.
        """
        if self.__database is None:
            self.__database = DatabaseInfo(self, self._select_row('select * from mon$database'))
        return self.__database
    @property
    def attachments(self) -> DataList[AttachmentInfo]:
        """List of all attachments.
        """
        if self.__attachments is None:
            self.__attachments = DataList((AttachmentInfo(self, row) for row
                                           in self._select('select * from mon$attachments')),
                                          AttachmentInfo, 'item.id', frozen=True)
        return self.__attachments
    @property
    def this_attachment(self) -> AttachmentInfo:
        """`.AttachmentInfo` object for current connection.
        """
        return self.attachments.get(self._con_id)
    @property
    def transactions(self) -> DataList[TransactionInfo]:
        """List of all transactions.
        """
        if self.__transactions is None:
            self.__transactions = DataList((TransactionInfo(self, row) for row
                                            in self._select('select * from mon$transactions')),
                                           TransactionInfo, 'item.id', frozen=True)
        return self.__transactions
    @property
    def statements(self) -> DataList[StatementInfo]:
        """List of all statements.
        """
        if self.__statements is None:
            self.__statements = DataList((StatementInfo(self, row) for row
                                          in self._select('select * from mon$statements')),
                                         StatementInfo, 'item.id', frozen=True)
        return self.__statements
    @property
    def callstack(self) -> DataList[CallStackInfo]:
        """List with complete call stack.
        """
        if self.__callstack is None:
            self.__callstack = DataList((CallStackInfo(self, row) for row
                                         in self._select('select * from mon$call_stack')),
                                        CallStackInfo, 'item.id', frozen=True)
        return self.__callstack
    @property
    def iostats(self) -> DataList[IOStatsInfo]:
        """List of all I/O statistics.
        """
        if self.__iostats is None:
            cmd = """SELECT r.MON$STAT_ID, r.MON$STAT_GROUP,
r.MON$RECORD_SEQ_READS, r.MON$RECORD_IDX_READS, r.MON$RECORD_INSERTS,
r.MON$RECORD_UPDATES, r.MON$RECORD_DELETES, r.MON$RECORD_BACKOUTS,
r.MON$RECORD_PURGES, r.MON$RECORD_EXPUNGES, r.MON$RECORD_LOCKS, r.MON$RECORD_WAITS,
r.MON$RECORD_CONFLICTS, r.MON$BACKVERSION_READS, r.MON$FRAGMENT_READS, r.MON$RECORD_RPT_READS,
io.MON$PAGE_FETCHES, io.MON$PAGE_MARKS, io.MON$PAGE_READS, io.MON$PAGE_WRITES,
m.MON$MEMORY_ALLOCATED, m.MON$MEMORY_USED, m.MON$MAX_MEMORY_ALLOCATED, m.MON$MAX_MEMORY_USED
FROM MON$RECORD_STATS r join MON$IO_STATS io
  on r.MON$STAT_ID = io.MON$STAT_ID and r.MON$STAT_GROUP = io.MON$STAT_GROUP
  join MON$MEMORY_USAGE m
  on r.MON$STAT_ID = m.MON$STAT_ID and r.MON$STAT_GROUP = m.MON$STAT_GROUP"""
            self.__iostats = DataList((IOStatsInfo(self, row) for row
                                       in self._select(cmd)),
                                      IOStatsInfo, 'item.stat_id', frozen=True)
        return self.__iostats
    @property
    def variables(self) -> DataList[ContextVariableInfo]:
        """List of all context variables.
        """
        if self.__variables is None:
            self.__variables = DataList((ContextVariableInfo(self, row) for row
                                         in self._select('select * from mon$context_variables')),
                                        ContextVariableInfo, 'item.stat_id', frozen=True)
        return self.__variables
    @property
    def tablestats(self) -> DataList[TableStatsInfo]:
        """List of all table record I/O statistics.
        """
        if self.__tablestats is None:
            cmd = """SELECT ts.MON$STAT_ID, ts.MON$STAT_GROUP, ts.MON$TABLE_NAME,
ts.MON$RECORD_STAT_ID, r.MON$RECORD_SEQ_READS, r.MON$RECORD_IDX_READS, r.MON$RECORD_INSERTS,
r.MON$RECORD_UPDATES, r.MON$RECORD_DELETES, r.MON$RECORD_BACKOUTS,
r.MON$RECORD_PURGES, r.MON$RECORD_EXPUNGES, r.MON$RECORD_LOCKS, r.MON$RECORD_WAITS,
r.MON$RECORD_CONFLICTS, r.MON$BACKVERSION_READS, r.MON$FRAGMENT_READS, r.MON$RECORD_RPT_READS
FROM MON$TABLE_STATS ts join MON$RECORD_STATS r
  on ts.MON$RECORD_STAT_ID = r.MON$STAT_ID"""
            self.__tablestats = DataList((TableStatsInfo(self, row) for row
                                          in self._select(cmd)),
                                         TableStatsInfo, 'item.stat_id', frozen=True)
        return self.__tablestats

class InfoItem:
    """Base class for all database monitoring objects.
    """
    def __init__(self, monitor: Monitor, attributes: Dict[str, Any]):
        #: Weak reference to parent `.Monitor` instance.
        self.monitor: Monitor = monitor if isinstance(monitor, weakref.ProxyType) else weakref.proxy(monitor)
        self._attributes: Dict[str, Any] = attributes
    def _strip_attribute(self, attr: str) -> None:
        if self._attributes.get(attr):
            self._attributes[attr] = self._attributes[attr].strip()
    @property
    def stat_id(self) -> Group:
        """Internal ID.
        """
        return self._attributes.get('MON$STAT_ID')

class DatabaseInfo(InfoItem):
    """Information about attached database.
    """
    def __init__(self, monitor: Monitor, attributes: Dict[str, Any]):
        super().__init__(monitor, attributes)
        self._strip_attribute('MON$DATABASE_NAME')
        self._strip_attribute('MON$OWNER')
        self._strip_attribute('MON$SEC_DATABASE')
    @property
    def name(self) -> str:
        """Database filename or alias.
        """
        return self._attributes['MON$DATABASE_NAME']
    @property
    def page_size(self) -> int:
        """Size of database page in bytes.
        """
        return self._attributes['MON$PAGE_SIZE']
    @property
    def ods(self) -> float:
        """On-Disk Structure (ODS) version number.
        """
        return float(f"{self._attributes['MON$ODS_MAJOR']}.{self._attributes['MON$ODS_MINOR']}")
    @property
    def oit(self) -> int:
        """Transaction ID of the oldest [interesting] transaction.
        """
        return self._attributes['MON$OLDEST_TRANSACTION']
    @property
    def oat(self) -> int:
        """Transaction ID of the oldest active transaction.
        """
        return self._attributes['MON$OLDEST_ACTIVE']
    @property
    def ost(self) -> int:
        """Transaction ID of the Oldest Snapshot, i.e., the number of the OAT when
        the last garbage collection was done.
        """
        return self._attributes['MON$OLDEST_SNAPSHOT']
    @property
    def next_transaction(self) -> int:
        """Transaction ID of the next transaction that will be started.
        """
        return self._attributes['MON$NEXT_TRANSACTION']
    @property
    def cache_size(self) -> int:
        """Number of pages allocated in the page cache.
        """
        return self._attributes['MON$PAGE_BUFFERS']
    @property
    def sql_dialect(self) -> int:
        """SQL dialect of the database.
        """
        return self._attributes['MON$SQL_DIALECT']
    @property
    def shutdown_mode(self) -> ShutdownMode:
        """Current shutdown mode.
        """
        return ShutdownMode(self._attributes['MON$SHUTDOWN_MODE'])
    @property
    def sweep_interval(self) -> int:
        """The sweep interval configured in the database header. Value 0 indicates that
        sweeping is disabled.
        """
        return self._attributes['MON$SWEEP_INTERVAL']
    @property
    def read_only(self) -> bool:
        """True if database is Read Only.
        """
        return bool(self._attributes['MON$READ_ONLY'])
    @property
    def forced_writes(self) -> bool:
        """True if database uses synchronous writes.
        """
        return bool(self._attributes['MON$FORCED_WRITES'])
    @property
    def reserve_space(self) -> bool:
        """True if database reserves space on data pages.
        """
        return bool(self._attributes['MON$RESERVE_SPACE'])
    @property
    def created(self) -> datetime.datetime:
        """Creation date and time, i.e., when the database was created or last restored.
        """
        return self._attributes['MON$CREATION_DATE']
    @property
    def pages(self) -> int:
        """Number of pages allocated on disk.
        """
        return self._attributes['MON$PAGES']
    @property
    def backup_state(self) -> BackupState:
        """Current state of database with respect to nbackup physical backup.
        """
        return BackupState(self._attributes['MON$BACKUP_STATE'])
    @property
    def iostats(self) -> IOStatsInfo:
        """`.IOStatsInfo` for this object.
        """
        return self.monitor.iostats.find(lambda io: (io.stat_id == self.stat_id)
                                         and (io.group is Group.DATABASE))
    @property
    def crypt_page(self) -> int:
        """Number of page being encrypted.
        """
        return self._attributes.get('MON$CRYPT_PAGE')
    @property
    def owner(self) -> str:
        """User name of database owner.
        """
        return self._attributes.get('MON$OWNER')
    @property
    def security(self) -> Security:
        """Type of security database (Default, Self or Other).
        """
        return Security(self._attributes.get('MON$SEC_DATABASE'))
    @property
    def tablestats(self) -> Dict[str, TableStatsInfo]:
        """Dictionary of `.TableStatsInfo` instances for this object.
        """
        return {io.table_name: io for io in self.monitor.tablestats
                if (io.stat_id == self.stat_id) and (io.group is Group.DATABASE)}

class AttachmentInfo(InfoItem):
    """Information about attachment (connection) to database.
    """
    def __init__(self, monitor: Monitor, attributes: Dict[str, Any]):
        super().__init__(monitor, attributes)
        self._strip_attribute('MON$ATTACHMENT_NAME')
        self._strip_attribute('MON$USER')
        self._strip_attribute('MON$ROLE')
        self._strip_attribute('MON$REMOTE_PROTOCOL')
        self._strip_attribute('MON$REMOTE_ADDRESS')
        self._strip_attribute('MON$REMOTE_PROCESS')
        self._strip_attribute('MON$CLIENT_VERSION')
        self._strip_attribute('MON$REMOTE_VERSION')
        self._strip_attribute('MON$REMOTE_HOST')
        self._strip_attribute('MON$REMOTE_OS_USER')
        self._strip_attribute('MON$AUTH_METHOD')
    def is_active(self) -> bool:
        """Returns True if attachment is active.
        """
        return self.state is State.ACTIVE
    def is_idle(self) -> bool:
        """Returns True if attachment is idle.
        """
        return self.state is State.IDLE
    def is_gc_allowed(self) -> bool:
        """Returns True if Garbage Collection is enabled for this attachment.
        """
        return bool(self._attributes['MON$GARBAGE_COLLECTION'])
    def is_internal(self) -> bool:
        """Returns True if attachment is internal system attachment.
        """
        return bool(self._attributes.get('MON$SYSTEM_FLAG'))
    def terminate(self) -> None:
        """Terminates client session associated with this attachment.

        Raises:
            firebird.base.types.Error: If attachement is current session.
        """
        if self is self.monitor.this_attachment:
            raise Error("Can't terminate current session.")
        self.monitor._ic.execute('delete from mon$attachments where mon$attachment_id = ?',
                                 (self.id,))
    @property
    def id(self) -> int:
        """Attachment ID.
        """
        return self._attributes['MON$ATTACHMENT_ID']
    @property
    def server_pid(self) -> int:
        """Server process ID.
        """
        return self._attributes['MON$SERVER_PID']
    @property
    def state(self) -> State:
        """Attachment state (idle/active).
        """
        return State(self._attributes['MON$STATE'])
    @property
    def name(self) -> str:
        """Database filename or alias.
        """
        return self._attributes['MON$ATTACHMENT_NAME']
    @property
    def user(self) -> str:
        """User name.
        """
        return self._attributes['MON$USER']
    @property
    def role(self) -> str:
        """Role name.
        """
        return self._attributes['MON$ROLE']
    @property
    def remote_protocol(self) -> str:
        """Remote protocol name.
        """
        return self._attributes['MON$REMOTE_PROTOCOL']
    @property
    def remote_address(self) -> str:
        """Remote address.
        """
        return self._attributes['MON$REMOTE_ADDRESS']
    @property
    def remote_pid(self) -> int:
        """Remote client process ID.
        """
        return self._attributes['MON$REMOTE_PID']
    @property
    def remote_process(self) -> str:
        """Remote client process pathname.
        """
        return self._attributes['MON$REMOTE_PROCESS']
    @property
    def character_set(self) -> CharacterSet:
        """Character set name for this attachment.
        """
        return self.monitor._con.schema.get_charset_by_id(self._attributes['MON$CHARACTER_SET_ID'])
    @property
    def timestamp(self) -> datetime.datetime:
        """Attachment date/time.
        """
        return self._attributes['MON$TIMESTAMP']
    @property
    def transactions(self) -> DataList[TransactionInfo]:
        """List of transactions associated with attachment.
        """
        return self.monitor.transactions.extract(lambda s: s._attributes['MON$ATTACHMENT_ID'] == self.id,
                                                 copy=True)
    @property
    def statements(self) -> DataList[StatementInfo]:
        """List of statements associated with attachment.
        """
        return self.monitor.statements.extract(lambda s: s._attributes['MON$ATTACHMENT_ID'] == self.id,
                                               copy=True)
    @property
    def variables(self) -> DataList[ContextVariableInfo]:
        """List of variables associated with attachment.
        """
        return self.monitor.variables.extract(lambda s: s._attributes['MON$ATTACHMENT_ID'] == self.id,
                                              copy=True)
    @property
    def iostats(self) -> IOStatsInfo:
        """`.IOStatsInfo` for this object.
        """
        return self.monitor.iostats.find(lambda io: (io.stat_id == self.stat_id)
                                         and (io.group is Group.ATTACHMENT))
    @property
    def auth_method(self) -> str:
        """Authentication method.
        """
        return self._attributes.get('MON$AUTH_METHOD')
    @property
    def client_version(self) -> str:
        """Client library version.
        """
        return self._attributes.get('MON$CLIENT_VERSION')
    @property
    def remote_version(self) -> str:
        """Remote protocol version.
        """
        return self._attributes.get('MON$REMOTE_VERSION')
    @property
    def remote_os_user(self) -> str:
        """OS user name of client process.
        """
        return self._attributes.get('MON$REMOTE_OS_USER')
    @property
    def remote_host(self) -> str:
        """Name of remote host.
        """
        return self._attributes.get('MON$REMOTE_HOST')
    @property
    def system(self) -> bool:
        """True for system attachments.
        """
        return bool(self._attributes.get('MON$SYSTEM_FLAG'))
    @property
    def tablestats(self) -> Dict[str, TableStatsInfo]:
        """Dictionary of `.TableStatsInfo` instances for this object.
        """
        return {io.table_name: io for io in self.monitor.tablestats
                if (io.stat_id == self.stat_id) and (io.group is Group.ATTACHMENT)}

class TransactionInfo(InfoItem):
    """Information about transaction.
    """
    def is_active(self) -> bool:
        """Returns True if transaction is active.
        """
        return self.state is State.ACTIVE
    def is_idle(self) -> bool:
        """Returns True if transaction is idle.
        """
        return self.state is State.IDLE
    def is_readonly(self) -> bool:
        """Returns True if transaction is Read Only.
        """
        return self._attributes['MON$READ_ONLY'] == FLAG_SET
    def is_autocommit(self) -> bool:
        """Returns True for autocommited transaction.
        """
        return self._attributes['MON$AUTO_COMMIT'] == FLAG_SET
    def is_autoundo(self) -> bool:
        """Returns True for transaction with automatic undo.
        """
        return self._attributes['MON$AUTO_UNDO'] == FLAG_SET
    @property
    def id(self) -> int:
        """Transaction ID.
        """
        return self._attributes['MON$TRANSACTION_ID']
    @property
    def attachment(self) -> AttachmentInfo:
        """`.AttachmentInfo` instance to which this transaction belongs.
        """
        return self.monitor.attachments.get(self._attributes['MON$ATTACHMENT_ID'])
    @property
    def state(self) -> State:
        """Transaction state (idle/active).
        """
        return State(self._attributes['MON$STATE'])
    @property
    def timestamp(self) -> datetime.datetime:
        """Transaction start datetime.
        """
        return self._attributes['MON$TIMESTAMP']
    @property
    def top(self) -> int:
        """Top transaction.
        """
        return self._attributes['MON$TOP_TRANSACTION']
    @property
    def oldest(self) -> int:
        """Oldest transaction (local OIT).
        """
        return self._attributes['MON$OLDEST_TRANSACTION']
    @property
    def oldest_active(self) -> int:
        """Oldest active transaction (local OAT).
        """
        return self._attributes['MON$OLDEST_ACTIVE']
    @property
    def isolation_mode(self) -> IsolationMode:
        """Transaction isolation mode code.
        """
        return IsolationMode(self._attributes['MON$ISOLATION_MODE'])
    @property
    def lock_timeout(self) -> int:
        """Lock timeout.
        """
        return self._attributes['MON$LOCK_TIMEOUT']
    @property
    def statements(self) -> DataList[StatementInfo]:
        """List of statements associated with transaction.
        """
        return self.monitor.statements.extract(lambda s: s._attributes['MON$TRANSACTION_ID'] == self.id,
                                               copy=True)
    @property
    def variables(self) -> DataList[ContextVariableInfo]:
        """List of variables associated with transaction.
        """
        return self.monitor.variables.extract(lambda s: s._attributes['MON$TRANSACTION_ID'] == self.id,
                                              copy=True)
    @property
    def iostats(self) -> IOStatsInfo:
        """`.IOStatsInfo` for this object.
        """
        return self.monitor.iostats.find(lambda io: (io.stat_id == self.stat_id)
                                         and (io.group is Group.TRANSACTION))
    @property
    def tablestats(self) -> Dict[str, TableStatsInfo]:
        """Dictionary of `.TableStatsInfo` instances for this object.
        """
        return {io.table_name: io for io in self.monitor.tablestats
                if (io.stat_id == self.stat_id) and (io.group is Group.TRANSACTION)}

class StatementInfo(InfoItem):
    """Information about executed SQL statement.
    """
    def __init__(self, monitor: Monitor, attributes: Dict[str, Any]):
        super().__init__(monitor, attributes)
        self._strip_attribute('MON$SQL_TEXT')
        self._strip_attribute('MON$EXPLAINED_PLAN')
    def is_active(self) -> bool:
        """Returns True if statement is active.
        """
        return self.state is State.ACTIVE
    def is_idle(self) -> bool:
        """Returns True if statement is idle.
        """
        return self.state is State.IDLE
    def terminate(self) -> None:
        """Terminates execution of statement.

        Raises:
            Error: If this attachement is current session.
        """
        self.monitor._ic.execute('delete from mon$statements where mon$statement_id = ?',
                                 (self.id,))
    @property
    def id(self) -> int:
        """Statement ID.
        """
        return self._attributes['MON$STATEMENT_ID']
    @property
    def attachment(self) -> AttachmentInfo:
        """`.AttachmentInfo` instance to which this statement belongs.
        """
        return self.monitor.attachments.get(self._attributes['MON$ATTACHMENT_ID'])
    @property
    def transaction(self) -> TransactionInfo:
        """`.TransactionInfo` instance to which this statement belongs or None.
        """
        return self.monitor.transactions.get(self._attributes['MON$TRANSACTION_ID'])
    @property
    def state(self) -> State:
        """Statement state (idle/active).
        """
        return State(self._attributes['MON$STATE'])
    @property
    def timestamp(self) -> datetime.datetime:
        """Statement start datetime.
        """
        return self._attributes['MON$TIMESTAMP']
    @property
    def sql(self) -> str:
        """Statement SQL text, if appropriate.
        """
        return self._attributes['MON$SQL_TEXT']
    @property
    def plan(self) -> str:
        """Explained execution plan.
        """
        return self._attributes.get('MON$EXPLAINED_PLAN')
    @property
    def callstack(self) -> DataList[CallStackInfo]:
        """List with call stack for statement.
        """
        callstack = self.monitor.callstack.extract(lambda x: ((x._attributes['MON$STATEMENT_ID'] == self.id) and
                                                             (x._attributes['MON$CALLER_ID'] is None)), copy=True)
        if len(callstack) > 0:
            item = callstack[0]
            while item is not None:
                caller_id = item.id
                item = None
                for x in self.monitor.callstack:
                    if x._attributes['MON$CALLER_ID'] == caller_id:
                        callstack.append(x)
                        item = x
                        break
        return callstack
    @property
    def iostats(self) -> IOStatsInfo:
        """`.IOStatsInfo` for this object.
        """
        return self.monitor.iostats.find(lambda io: (io.stat_id == self.stat_id)
                                         and (io.group is Group.STATEMENT))
    @property
    def tablestats(self) -> Dict[str, TableStatsInfo]:
        """Dictionary of `.TableStatsInfo` instances for this object.
        """
        return {io.table_name: io for io in self.monitor.tablestats
                if (io.stat_id == self.stat_id) and (io.group is Group.STATEMENT)}

class CallStackInfo(InfoItem):
    """Information about PSQL call (stack frame).
    """
    def __init__(self, monitor: Monitor, attributes: Dict[str, Any]):
        super().__init__(monitor, attributes)
        self._strip_attribute('MON$OBJECT_NAME')
        self._strip_attribute('MON$PACKAGE_NAME')
    @property
    def id(self) -> int:
        """Call ID.
        """
        return self._attributes['MON$CALL_ID']
    @property
    def statement(self) -> StatementInfo:
        """Top-level `.StatementInfo` instance to which this call stack entry belongs.
        """
        return self.monitor.statements.get(self._attributes['MON$STATEMENT_ID'])
    @property
    def caller(self) -> CallStackInfo:
        """Call stack entry (`.CallStackInfo`) of the caller.
        """
        return self.monitor.callstack.get(self._attributes['MON$CALLER_ID'])
    @property
    def dbobject(self) -> Union[Procedure, Trigger, Function]:
        """Database object.
        """
        obj_type = self.object_type
        if obj_type == ObjectType.PROCEDURE:
            return self.monitor._con.schema.procedures.get(self.object_name)
        if obj_type == ObjectType.TRIGGER:
            return self.monitor._con.schema.triggers.get(self.object_name)
        if obj_type == ObjectType.UDF:
            return self.monitor._con.schema.functions.get(self.object_name)
        raise Error(f"Unrecognized object type '{obj_type}'")
    @property
    def object_type(self) -> ObjectType:
        """PSQL object type.
        """
        return ObjectType(self._attributes['MON$OBJECT_TYPE'])
    @property
    def object_name(self) -> str:
        """PSQL object name.
        """
        return self._attributes['MON$OBJECT_NAME']
    @property
    def timestamp(self) -> datetime.datetime:
        """Request start datetime.
        """
        return self._attributes['MON$TIMESTAMP']
    @property
    def line(self) -> int:
        """SQL source line number.
        """
        return self._attributes['MON$SOURCE_LINE']
    @property
    def column(self) -> int:
        """SQL source column number.
        """
        return self._attributes['MON$SOURCE_COLUMN']
    @property
    def package_name(self) -> str:
        """Package name.
        """
        return None if (name := self._attributes.get('MON$PACKAGE_NAME')) \
               else self.monitor._con.schema.packages.get(name)
    @property
    def iostats(self) -> IOStatsInfo:
        """`.IOStatsInfo` for this object.
        """
        return self.monitor.iostats.find(lambda io: (io.stat_id == self.stat_id)
                                         and (io.group is Group.CALL))

class IOStatsInfo(InfoItem):
    """Information about page and row level I/O operations, and about memory consumption.
    """
    @property
    def owner(self) -> Union[DatabaseInfo, AttachmentInfo, TransactionInfo,
                             StatementInfo, CallStackInfo]:
        """Object that owns this IOStats instance.
        """
        obj_type = self.group
        if obj_type is Group.DATABASE:
            return self.monitor.db
        if obj_type is Group.ATTACHMENT:
            return self.monitor.attachments.find(lambda x: x.stat_id == self.stat_id)
        if obj_type is Group.TRANSACTION:
            return self.monitor.transactions.find(lambda x: x.stat_id == self.stat_id)
        if obj_type is Group.STATEMENT:
            return self.monitor.statements.find(lambda x: x.stat_id == self.stat_id)
        if obj_type is Group.CALL:
            return self.monitor.callstack.find(lambda x: x.stat_id == self.stat_id)
        raise Error(f"Unrecognized stat group '{obj_type}'")
    @property
    def group(self) -> Group:
        """Object group code.
        """
        return Group(self._attributes['MON$STAT_GROUP'])
    @property
    def reads(self) -> int:
        """Number of page reads.
        """
        return self._attributes['MON$PAGE_READS']
    @property
    def writes(self) -> int:
        """Number of page writes.
        """
        return self._attributes['MON$PAGE_WRITES']
    @property
    def fetches(self) -> int:
        """Number of page fetches.
        """
        return self._attributes['MON$PAGE_FETCHES']
    @property
    def marks(self) -> int:
        """Number of pages with changes pending.
        """
        return self._attributes['MON$PAGE_MARKS']
    @property
    def seq_reads(self) -> int:
        """Number of records read sequentially.
        """
        return self._attributes['MON$RECORD_SEQ_READS']
    @property
    def idx_reads(self) -> int:
        """Number of records read via an index.
        """
        return self._attributes['MON$RECORD_IDX_READS']
    @property
    def inserts(self) -> int:
        """Number of inserted records.
        """
        return self._attributes['MON$RECORD_INSERTS']
    @property
    def updates(self) -> int:
        """Number of updated records.
        """
        return self._attributes['MON$RECORD_UPDATES']
    @property
    def deletes(self) -> int:
        """Number of deleted records.
        """
        return self._attributes['MON$RECORD_DELETES']
    @property
    def backouts(self) -> int:
        """Number of records where a new primary record version or a change to an existing
        primary record version is backed out due to rollback or savepoint undo.
        """
        return self._attributes['MON$RECORD_BACKOUTS']
    @property
    def purges(self) -> int:
        """Number of records where record version chain is being purged of versions no
        longer needed by OAT or younger transactions.
        """
        return self._attributes['MON$RECORD_PURGES']
    @property
    def expunges(self) -> int:
        """Number of records where record version chain is being deleted due to deletions
        by transactions older than OAT.
        """
        return self._attributes['MON$RECORD_EXPUNGES']
    @property
    def memory_used(self) -> int:
        """Number of bytes currently in use.
        """
        return self._attributes.get('MON$MEMORY_USED')
    @property
    def memory_allocated(self) -> int:
        """Number of bytes currently allocated at the OS level.
        """
        return self._attributes.get('MON$MEMORY_ALLOCATED')
    @property
    def max_memory_used(self) -> int:
        """Maximum number of bytes used by this object.
        """
        return self._attributes.get('MON$MAX_MEMORY_USED')
    @property
    def max_memory_allocated(self) -> int:
        """Maximum number of bytes allocated from the operating system by this object.
        """
        return self._attributes.get('MON$MAX_MEMORY_ALLOCATED')
    @property
    def locks(self) -> int:
        """Number of record locks.
        """
        return self._attributes.get('MON$RECORD_LOCKS')
    @property
    def waits(self) -> int:
        """Number of record waits.
        """
        return self._attributes.get('MON$RECORD_WAITS')
    @property
    def conflits(self) -> int:
        """Number of record conflits.
        """
        return self._attributes.get('MON$RECORD_CONFLICTS')
    @property
    def backversion_reads(self) -> int:
        """Number of record backversion reads.
        """
        return self._attributes.get('MON$BACKVERSION_READS')
    @property
    def fragment_reads(self) -> int:
        """Number of record fragment reads.
        """
        return self._attributes.get('MON$FRAGMENT_READS')
    @property
    def repeated_reads(self) -> int:
        """Number of repeated record reads.
        """
        return self._attributes.get('MON$RECORD_RPT_READS')

class TableStatsInfo(InfoItem):
    """Information about row level I/O operations on single table.
    """
    def __init__(self, monitor: Monitor, attributes: Dict[str, Any]):
        super().__init__(monitor, attributes)
        self._strip_attribute('MON$TABLE_NAME')
    @property
    def owner(self) -> Union[DatabaseInfo, AttachmentInfo, TransactionInfo,
                             StatementInfo, CallStackInfo]:
        """Object that owns this TableStatsInfo instance.
        """
        obj_type = self.group
        if obj_type is Group.DATABASE:
            return self.monitor.db
        if obj_type is Group.ATTACHMENT:
            return self.monitor.attachments.find(lambda x: x.stat_id == self.stat_id)
        if obj_type is Group.TRANSACTION:
            return self.monitor.transactions.find(lambda x: x.stat_id == self.stat_id)
        if obj_type is Group.STATEMENT:
            return self.monitor.statements.find(lambda x: x.stat_id == self.stat_id)
        if obj_type is Group.CALL:
            return self.monitor.callstack.find(lambda x: x.stat_id == self.stat_id)
        raise Error(f"Unrecognized stat group '{obj_type}'")
    @property
    def row_stat_id(self) -> int:
        """Internal ID.
        """
        return self._attributes['MON$RECORD_STAT_ID']
    @property
    def table_name(self) -> str:
        """Table name.
        """
        return self._attributes['MON$TABLE_NAME']
    @property
    def group(self) -> Group:
        """Object group code.
        """
        return Group(self._attributes['MON$STAT_GROUP'])
    @property
    def seq_reads(self) -> int:
        """Number of records read sequentially.
        """
        return self._attributes['MON$RECORD_SEQ_READS']
    @property
    def idx_reads(self) -> int:
        """Number of records read via an index.
        """
        return self._attributes['MON$RECORD_IDX_READS']
    @property
    def inserts(self) -> int:
        """Number of inserted records.
        """
        return self._attributes['MON$RECORD_INSERTS']
    @property
    def updates(self) -> int:
        """Number of updated records.
        """
        return self._attributes['MON$RECORD_UPDATES']
    @property
    def deletes(self) -> int:
        """Number of deleted records.
        """
        return self._attributes['MON$RECORD_DELETES']
    @property
    def backouts(self) -> int:
        """Number of records where a new primary record version or a change to an existing
        primary record version is backed out due to rollback or savepoint undo.
        """
        return self._attributes['MON$RECORD_BACKOUTS']
    @property
    def purges(self) -> int:
        """Number of records where record version chain is being purged of versions no
        longer needed by OAT or younger transactions.
        """
        return self._attributes['MON$RECORD_PURGES']
    @property
    def expunges(self) -> int:
        """Number of records where record version chain is being deleted due to deletions
        by transactions older than OAT.
        """
        return self._attributes['MON$RECORD_EXPUNGES']
    @property
    def locks(self) -> int:
        """Number of record locks.
        """
        return self._attributes['MON$RECORD_LOCKS']
    @property
    def waits(self) -> int:
        """Number of record waits.
        """
        return self._attributes['MON$RECORD_WAITS']
    @property
    def conflits(self) -> int:
        """Number of record conflits.
        """
        return self._attributes['MON$RECORD_CONFLICTS']
    @property
    def backversion_reads(self) -> int:
        """Number of record backversion reads.
        """
        return self._attributes['MON$BACKVERSION_READS']
    @property
    def fragment_reads(self) -> int:
        """Number of record fragment reads.
        """
        return self._attributes['MON$FRAGMENT_READS']
    @property
    def repeated_reads(self) -> int:
        """Number of repeated record reads.
        """
        return self._attributes['MON$RECORD_RPT_READS']

class ContextVariableInfo(InfoItem):
    """Information about context variable.
    """
    def __init__(self, monitor: Monitor, attributes: Dict[str, Any]):
        super().__init__(monitor, attributes)
        self._strip_attribute('MON$VARIABLE_NAME')
        self._strip_attribute('MON$VARIABLE_VALUE')
    def is_attachment_var(self) -> bool:
        """Returns True if variable is associated to attachment context.
        """
        return self._attributes['MON$ATTACHMENT_ID'] is not None
    def is_transaction_var(self) -> bool:
        """Returns True if variable is associated to transaction context.
        """
        return self._attributes['MON$TRANSACTION_ID'] is not None
    @property
    def attachment(self) -> AttachmentInfo:
        """`.AttachmentInfo` instance to which this context variable belongs or None.
        """
        return self.monitor.attachments.get(self._attributes['MON$ATTACHMENT_ID'])
    @property
    def transaction(self) -> TransactionInfo:
        """`.TransactionInfo` instance to which this context variable belongs or None.
        """
        return self.monitor.transactions.get(self._attributes['MON$TRANSACTION_ID'])
    @property
    def name(self) -> str:
        """Context variable name.
        """
        return self._attributes['MON$VARIABLE_NAME']
    @property
    def value(self) -> str:
        """Value of context variable.
        """
        return self._attributes['MON$VARIABLE_VALUE']
