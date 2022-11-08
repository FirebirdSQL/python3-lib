#coding:utf-8
#
# PROGRAM/MODULE: firebird-lib
# FILE:           firebird/lib/trace.py
# DESCRIPTION:    Module for parsing Firebird trace & audit protocol
# CREATED:        7.10.2020
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
# pylint: disable=C0302, W0212, R0902, R0912,R0913, R0914, R0915, R0904, R0903, C0301, W0703

"""firebird.lib.trace - Module for parsing Firebird trace & audit protocol


"""

from __future__ import annotations
from typing import List, Tuple, Dict, Set, Iterable, Any, Optional, Union
from sys import intern
import datetime
import decimal
import collections
from enum import Enum, IntEnum, auto
from dataclasses import dataclass
from firebird.base.types import Error, STOP, Sentinel

class Status(Enum):
    """Trace event status codes.
    """
    OK = ' '
    FAILED = 'F'
    UNAUTHORIZED = 'U'
    UNKNOWN = '?'

class Event(IntEnum):
    """Trace event codes.
    """
    UNKNOWN = auto()
    TRACE_INIT = auto()
    TRACE_SUSPENDED = auto()
    TRACE_FINI = auto()
    CREATE_DATABASE = auto()
    DROP_DATABASE = auto()
    ATTACH_DATABASE = auto()
    DETACH_DATABASE = auto()
    START_TRANSACTION = auto()
    COMMIT_TRANSACTION = auto()
    ROLLBACK_TRANSACTION = auto()
    COMMIT_RETAINING = auto()
    ROLLBACK_RETAINING = auto()
    PREPARE_STATEMENT = auto()
    EXECUTE_STATEMENT_START = auto()
    EXECUTE_STATEMENT_FINISH = auto()
    FREE_STATEMENT = auto()
    CLOSE_CURSOR = auto()
    EXECUTE_TRIGGER_START = auto()
    EXECUTE_TRIGGER_FINISH = auto()
    EXECUTE_FUNCTION_START = auto()
    EXECUTE_FUNCTION_FINISH = auto()
    EXECUTE_PROCEDURE_START = auto()
    EXECUTE_PROCEDURE_FINISH = auto()
    START_SERVICE = auto()
    ATTACH_SERVICE = auto()
    DETACH_SERVICE = auto()
    QUERY_SERVICE = auto()
    SET_CONTEXT = auto()
    ERROR = auto()
    WARNING = auto()
    SWEEP_START = auto()
    SWEEP_PROGRESS = auto()
    SWEEP_FINISH = auto()
    SWEEP_FAILED = auto()
    COMPILE_BLR = auto()
    EXECUTE_BLR = auto()
    EXECUTE_DYN = auto()

class TraceInfo:
    """Base class for trace info blocks.
    """

class TraceEvent:
    """Base class for trace events.
    """

@dataclass(frozen=True)
class AttachmentInfo(TraceInfo):
    """Information about database attachment.
    """
    #: Attachamnet ID
    attachment_id: int
    #: Database name/file
    database: str
    #: Database character set
    charset: str
    #: Network protocol
    protocol: str
    #: Network address
    address: str
    #: User name
    user: str
    #: Role name
    role: str
    #: Remote process
    remote_process: str
    #: Remote process ID
    remote_pid: int

@dataclass(frozen=True)
class TransactionInfo(TraceInfo):
    """Information about transaction.
    """
    #: Attachamnet ID
    attachment_id: int
    #: Transaction ID
    transaction_id: int
    #: Initial transaction ID (for retained ones)
    initial_id: int
    #: List of transaction options
    options: List[str]

@dataclass(frozen=True)
class ServiceInfo(TraceInfo):
    """Information about service attachment.
    """
    #: Service ID
    service_id: int
    #: User name
    user: str
    #: Network protocol
    protocol: str
    #: Network address
    address: str
    #: Remote process
    remote_process: str
    #: Remote process ID
    remote_pid: int

@dataclass(frozen=True)
class SQLInfo(TraceInfo):
    """Information about SQL statement.
    """
    #: SQL ID
    sql_id: int
    #: SQL command
    sql: str
    #: Execution plan
    plan: str

@dataclass(frozen=True)
class ParamSet(TraceInfo):
    """Information about set of parameters.
    """
    #: Parameter set ID
    par_id: int
    #: List of parameters (name, value pairs)
    params: List[Tuple[str, Any]]

@dataclass(frozen=True)
class AccessStats(TraceInfo):
    """Table access statistics.
    """
    #: Table name
    table: str
    #: Number of rows accessed sequentially
    natural: int
    #: Number of rows accessed via index
    index: int
    #: Number of updated rows
    update: int
    #: Number of inserted rows
    insert: int
    #: Number of deleted rows
    delete: int
    #: Number of rows where a new primary record version or a change to an existing
    #: primary record version is backed out due to rollback or savepoint undo
    backout: int
    #: Number of rows where record version chain is being purged of versions no longer
    #: needed by OAT or younger transactions
    purge: int
    #: Number of rows where record version chain is being deleted due to deletions
    #: by transactions older than OAT
    expunge: int

#
@dataclass(frozen=True)
class EventTraceInit(TraceEvent):
    "Trace session initialized trace event"
    #: Trace event ID
    event_id: int
    #: Timestamp when the event occurred
    timestamp: datetime.datetime
    #: Trace session name
    session_name: str

@dataclass(frozen=True)
class EventTraceSuspend(TraceEvent):
    "Trace session suspended trace event"
    #: Trace event ID
    event_id: int
    #: Timestamp when the event occurred
    timestamp: datetime.datetime
    #: Trace session name
    session_name: str

@dataclass(frozen=True)
class EventTraceFinish(TraceEvent):
    "Trace session finished trace event"
    #: Trace event ID
    event_id: int
    #: Timestamp when the event occurred
    timestamp: datetime.datetime
    #: Trace session name
    session_name: str

#
@dataclass(frozen=True)
class EventCreate(TraceEvent):
    "Create database trace event"
    #: Trace event ID
    event_id: int
    #: Timestamp when the event occurred
    timestamp: datetime.datetime
    #: Event status
    status: Status
    #: Database attachent ID
    attachment_id: int
    #: Database name/file
    database: str
    #: Connection character set
    charset: str
    #: Netwrok protocol
    protocol: str
    #: Netwrok address
    address: str
    #: User name
    user: str
    #: Role name
    role: str
    #: Remote process
    remote_process: str
    #: Remote process
    remote_pid: int

@dataclass(frozen=True)
class EventDrop(TraceEvent):
    "Drop database trace event"
    #: Trace event ID
    event_id: int
    #: Timestamp when the event occurred
    timestamp: datetime.datetime
    #: Event status
    status: Status
    #: Database attachent ID
    attachment_id: int
    #: Database name/file
    database: str
    #: Connection character set
    charset: str
    #: Netwrok protocol
    protocol: str
    #: Netwrok address
    address: str
    #: User name
    user: str
    #: Role name
    role: str
    #: Remote process
    remote_process: str
    #: Remote process
    remote_pid: int

@dataclass(frozen=True)
class EventAttach(TraceEvent):
    "Database attach trace event"
    #: Trace event ID
    event_id: int
    #: Timestamp when the event occurred
    timestamp: datetime.datetime
    #: Event status
    status: Status
    #: Database attachent ID
    attachment_id: int
    #: Database name/file
    database: str
    #: Connection character set
    charset: str
    #: Netwrok protocol
    protocol: str
    #: Netwrok address
    address: str
    #: User name
    user: str
    #: Role name
    role: str
    #: Remote process
    remote_process: str
    #: Remote process
    remote_pid: int

@dataclass(frozen=True)
class EventDetach(TraceEvent):
    "Database detach trace event"
    #: Trace event ID
    event_id: int
    #: Timestamp when the event occurred
    timestamp: datetime.datetime
    #: Event status
    status: Status
    #: Database attachent ID
    attachment_id: int
    #: Database name/file
    database: str
    #: Connection character set
    charset: str
    #: Netwrok protocol
    protocol: str
    #: Netwrok address
    address: str
    #: User name
    user: str
    #: Role name
    role: str
    #: Remote process
    remote_process: str
    #: Remote process
    remote_pid: int

#
@dataclass(frozen=True)
class EventTransactionStart(TraceEvent):
    "Transaction start trace event"
    #: Trace event ID
    event_id: int
    #: Timestamp when the event occurred
    timestamp: datetime.datetime
    #: Event status
    status: Status
    #: Database attachent ID
    attachment_id: int
    #: Transaction ID
    transaction_id: int
    #: List of transaction options
    options: List[str]

@dataclass(frozen=True)
class EventCommit(TraceEvent):
    "Commit trace event"
    #: Trace event ID
    event_id: int
    #: Timestamp when the event occurred
    timestamp: datetime.datetime
    #: Event status
    status: Status
    #: Database attachent ID
    attachment_id: int
    #: Transaction ID
    transaction_id: int
    #: List of transaction options
    options: List[str]
    #: Execution time in ms
    run_time: int
    #: Number of page reads
    reads: int
    #: Number of page writes
    writes: int
    #: Number of page fetches
    fetches: int
    #: Number of pages with changes pending
    marks: int

@dataclass(frozen=True)
class EventRollback(TraceEvent):
    "Rollback trace event"
    #: Trace event ID
    event_id: int
    #: Timestamp when the event occurred
    timestamp: datetime.datetime
    #: Event status
    status: Status
    #: Database attachent ID
    attachment_id: int
    #: Transaction ID
    transaction_id: int
    #: List of transaction options
    options: List[str]
    #: Execution time in ms
    run_time: int
    #: Number of page reads
    reads: int
    #: Number of page writes
    writes: int
    #: Number of page fetches
    fetches: int
    #: Number of pages with changes pending
    marks: int

@dataclass(frozen=True)
class EventCommitRetaining(TraceEvent):
    "Commit retaining trace event"
    #: Trace event ID
    event_id: int
    #: Timestamp when the event occurred
    timestamp: datetime.datetime
    #: Event status
    status: Status
    #: Database attachent ID
    attachment_id: int
    #: Transaction ID
    transaction_id: int
    #: List of transaction options
    options: List[str]
    #: New transaction number
    new_transaction_id: int
    #: Execution time in ms
    run_time: int
    #: Number of page reads
    reads: int
    #: Number of page writes
    writes: int
    #: Number of page fetches
    fetches: int
    #: Number of pages with changes pending
    marks: int

@dataclass(frozen=True)
class EventRollbackRetaining(TraceEvent):
    "Rollback retaining trace event"
    #: Trace event ID
    event_id: int
    #: Timestamp when the event occurred
    timestamp: datetime.datetime
    #: Event status
    status: Status
    #: Database attachent ID
    attachment_id: int
    #: Transaction ID
    transaction_id: int
    #: List of transaction options
    options: List[str]
    #: New transaction number
    new_transaction_id: int
    #: Execution time in ms
    run_time: int
    #: Number of page reads
    reads: int
    #: Number of page writes
    writes: int
    #: Number of page fetches
    fetches: int
    #: Number of pages with changes pending
    marks: int

#
@dataclass(frozen=True)
class EventPrepareStatement(TraceEvent):
    "Prepare statement trace event"
    #: Trace event ID
    event_id: int
    #: Timestamp when the event occurred
    timestamp: datetime.datetime
    #: Event status
    status: Status
    #: Database attachent ID
    attachment_id: int
    #: Transaction ID
    transaction_id: int
    #: Statement ID
    statement_id: int
    #: SQL ID (SQLInfo)
    sql_id: int
    #: Statement prepare time in ms
    prepare_time: int

@dataclass(frozen=True)
class EventStatementStart(TraceEvent):
    "Statement start trace event"
    #: Trace event ID
    event_id: int
    #: Timestamp when the event occurred
    timestamp: datetime.datetime
    #: Event status
    status: Status
    #: Database attachent ID
    attachment_id: int
    #: Transaction ID
    transaction_id: int
    #: Statement ID
    statement_id: int
    #: SQL ID (SQLInfo)
    sql_id: int
    #: Param set ID (ParamSet)
    param_id: int

@dataclass(frozen=True)
class EventStatementFinish(TraceEvent):
    "Statement finish trace event"
    #: Trace event ID
    event_id: int
    #: Timestamp when the event occurred
    timestamp: datetime.datetime
    #: Event status
    status: Status
    #: Database attachent ID
    attachment_id: int
    #: Transaction ID
    transaction_id: int
    #: Statement ID
    statement_id: int
    #: SQL ID (SQLInfo)
    sql_id: int
    #: Param set ID (ParamSet)
    param_id: int
    #: Number of affected rows
    records: int
    #: Execution time in ms
    run_time: int
    #: Number of page reads
    reads: int
    #: Number of page writes
    writes: int
    #: Number of page fetches
    fetches: int
    #: Number of pages with changes pending
    marks: int
    #: List with table access statistics
    access: List[AccessStats]

@dataclass(frozen=True)
class EventFreeStatement(TraceEvent):
    "Free statement trace event"
    #: Trace event ID
    event_id: int
    #: Timestamp when the event occurred
    timestamp: datetime.datetime
    #: Database attachent ID
    attachment_id: int
    ##: Transaction ID
    #transaction_id: int
    #: Statement ID
    statement_id: int
    #: SQL ID (SQLInfo)
    sql_id: int

@dataclass(frozen=True)
class EventCloseCursor(TraceEvent):
    "Close cursor trace event"
    #: Trace event ID
    event_id: int
    #: Timestamp when the event occurred
    timestamp: datetime.datetime
    #: Database attachent ID
    attachment_id: int
    ##: Transaction ID
    #transaction_id: int
    #: Statement ID
    statement_id: int
    #: SQL ID (SQLInfo)
    sql_id: int

#
@dataclass(frozen=True)
class EventTriggerStart(TraceEvent):
    "Trigger start trace event"
    #: Trace event ID
    event_id: int
    #: Timestamp when the event occurred
    timestamp: datetime.datetime
    #: Event status
    status: Status
    #: Database attachent ID
    attachment_id: int
    #: Transaction ID
    transaction_id: int
    #: trigger name
    trigger: str
    #: Table name
    table: str
    #: Trigger event
    event: str

@dataclass(frozen=True)
class EventTriggerFinish(TraceEvent):
    "Trigger finish trace event"
    #: Trace event ID
    event_id: int
    #: Timestamp when the event occurred
    timestamp: datetime.datetime
    #: Event status
    status: Status
    #: Database attachent ID
    attachment_id: int
    #: Transaction ID
    transaction_id: int
    #: trigger name
    trigger: str
    #: Table name
    table: str
    #: Trigger event
    event: str
    #: Execution time in ms
    run_time: int
    #: Number of page reads
    reads: int
    #: Number of page writes
    writes: int
    #: Number of page fetches
    fetches: int
    #: Number of pages with changes pending
    marks: int
    #: List with table access statistics
    access: List[AccessStats]

#
@dataclass(frozen=True)
class EventProcedureStart(TraceEvent):
    "Procedure start trace event"
    #: Trace event ID
    event_id: int
    #: Timestamp when the event occurred
    timestamp: datetime.datetime
    #: Event status
    status: Status
    #: Database attachent ID
    attachment_id: int
    #: Transaction ID
    transaction_id: int
    #: procedure name
    procedure: str
    #: Param set ID (ParamSet)
    param_id: int

@dataclass(frozen=True)
class EventProcedureFinish(TraceEvent):
    "Procedure finish trace event"
    #: Trace event ID
    event_id: int
    #: Timestamp when the event occurred
    timestamp: datetime.datetime
    #: Event status
    status: Status
    #: Database attachent ID
    attachment_id: int
    #: Transaction ID
    transaction_id: int
    #: procedure name
    procedure: str
    #: Param set ID (ParamSet)
    param_id: int
    #: Number of affected rows
    records: int
    #: Execution time in ms
    run_time: int
    #: Number of page reads
    reads: int
    #: Number of page writes
    writes: int
    #: Number of page fetches
    fetches: int
    #: Number of pages with changes pending
    marks: int
    #: List with table access statistics
    access: List[AccessStats]
#
@dataclass(frozen=True)
class EventFunctionStart(TraceEvent):
    "Function start trace event"
    #: Trace event ID
    event_id: int
    #: Timestamp when the event occurred
    timestamp: datetime.datetime
    #: Event status
    status: Status
    #: Database attachent ID
    attachment_id: int
    #: Transaction ID
    transaction_id: int
    #: procedure name
    function: str
    #: Param set ID (ParamSet)
    param_id: int

@dataclass(frozen=True)
class EventFunctionFinish(TraceEvent):
    "Function finish trace event"
    #: Trace event ID
    event_id: int
    #: Timestamp when the event occurred
    timestamp: datetime.datetime
    #: Event status
    status: Status
    #: Database attachent ID
    attachment_id: int
    #: Transaction ID
    transaction_id: int
    #: procedure name
    function: str
    #: Param set ID (ParamSet)
    param_id: int
    #: Return value
    returns: Tuple[str, Any]
    #: Execution time in ms
    run_time: int
    #: Number of page reads
    reads: int
    #: Number of page writes
    writes: int
    #: Number of page fetches
    fetches: int
    #: Number of pages with changes pending
    marks: int
    #: List with table access statistics
    access: List[AccessStats]
#
@dataclass(frozen=True)
class EventServiceAttach(TraceEvent):
    "Service attach trace event"
    #: Trace event ID
    event_id: int
    #: Timestamp when the event occurred
    timestamp: datetime.datetime
    #: Event status
    status: Status
    #: Service ID
    service_id: int

@dataclass(frozen=True)
class EventServiceDetach(TraceEvent):
    "Service detach trace event"
    #: Trace event ID
    event_id: int
    #: Timestamp when the event occurred
    timestamp: datetime.datetime
    #: Event status
    status: Status
    #: Service ID
    service_id: int

@dataclass(frozen=True)
class EventServiceStart(TraceEvent):
    "Service start trace event"
    #: Trace event ID
    event_id: int
    #: Timestamp when the event occurred
    timestamp: datetime.datetime
    #: Event status
    status: Status
    #: Service ID
    service_id: int
    #: Action performed by service
    action: str
    #: List of action parameters
    parameters: List[str]

@dataclass(frozen=True)
class EventServiceQuery(TraceEvent):
    "Service query trace event"
    #: Trace event ID
    event_id: int
    #: Timestamp when the event occurred
    timestamp: datetime.datetime
    #: Event status
    status: Status
    #: Service ID
    service_id: int
    #: Action performed by service
    action: str
    #: List of sent items
    sent: List[str]
    #: List of received items
    received: List[str]

#
@dataclass(frozen=True)
class EventSetContext(TraceEvent):
    "Set context variable trace event"
    #: Trace event ID
    event_id: int
    #: Timestamp when the event occurred
    timestamp: datetime.datetime
    #: Database attachent ID
    attachment_id: int
    #: Transaction ID
    transaction_id: int
    #: Context name
    context: str
    #: Key
    key: str
    #: Value
    value: str

#
@dataclass(frozen=True)
class EventError(TraceEvent):
    "Error trace event"
    #: Trace event ID
    event_id: int
    #: Timestamp when the event occurred
    timestamp: datetime.datetime
    #: Database attachent ID
    attachment_id: int
    #: Place where error occured
    place: str
    #: Error details
    details: List[str]

@dataclass(frozen=True)
class EventWarning(TraceEvent):
    "Warning trace event"
    #: Trace event ID
    event_id: int
    #: Timestamp when the event occurred
    timestamp: datetime.datetime
    #: Database attachent ID
    attachment_id: int
    #: Place where warning occured
    place: str
    #: Warning details
    details: List[str]

@dataclass(frozen=True)
class EventServiceError(TraceEvent):
    "Service error trace event"
    #: Trace event ID
    event_id: int
    #: Timestamp when the event occurred
    timestamp: datetime.datetime
    #: Service ID
    service_id: int
    #: Place where error occured
    place: str
    #: Error details
    details: List[str]

@dataclass(frozen=True)
class EventServiceWarning(TraceEvent):
    "Service warning trace event"
    #: Trace event ID
    event_id: int
    #: Timestamp when the event occurred
    timestamp: datetime.datetime
    #: Service ID
    service_id: int
    #: Place where warning occured
    place: str
    #: Warning details
    details: List[str]

#
@dataclass(frozen=True)
class EventSweepStart(TraceEvent):
    "Sweep start trace event"
    #: Trace event ID
    event_id: int
    #: Timestamp when the event occurred
    timestamp: datetime.datetime
    #: Database attachent ID
    attachment_id: int
    #: Transaction ID of the oldest [interesting] transaction
    oit: int
    #: Transaction ID of the oldest active transaction
    oat: int
    #: Transaction ID of the Oldest Snapshot
    ost: int
    #: Transaction ID of the next transaction that will be started
    next: int

@dataclass(frozen=True)
class EventSweepProgress(TraceEvent):
    "Sweep progress trace event"
    #: Trace event ID
    event_id: int
    #: Timestamp when the event occurred
    timestamp: datetime.datetime
    #: Database attachent ID
    attachment_id: int
    #: Execution time in ms
    run_time: int
    #: Number of page reads
    reads: int
    #: Number of page writes
    writes: int
    #: Number of page fetches
    fetches: int
    #: Number of pages with changes pending
    marks: int
    #: List with table access statistics
    access: List[AccessStats]

@dataclass(frozen=True)
class EventSweepFinish(TraceEvent):
    "Sweep finished trace event"
    #: Trace event ID
    event_id: int
    #: Timestamp when the event occurred
    timestamp: datetime.datetime
    #: Database attachent ID
    attachment_id: int
    #: Transaction ID of the oldest [interesting] transaction
    oit: int
    #: Transaction ID of the oldest active transaction
    oat: int
    #: Transaction ID of the Oldest Snapshot
    ost: int
    #: Transaction ID of the next transaction that will be started
    next: int
    #: Execution time in ms
    run_time: int
    #: Number of page reads
    reads: int
    #: Number of page writes
    writes: int
    #: Number of page fetches
    fetches: int
    #: Number of pages with changes pending
    marks: int
    #: List with table access statistics
    access: List[AccessStats]

@dataclass(frozen=True)
class EventSweepFailed(TraceEvent):
    "Sweep failed trace event"
    #: Trace event ID
    event_id: int
    #: Timestamp when the event occurred
    timestamp: datetime.datetime
    #: Database attachent ID
    attachment_id: int

#
@dataclass(frozen=True)
class EventBLRCompile(TraceEvent):
    "BLR compile trace event"
    #: Trace event ID
    event_id: int
    #: Timestamp when the event occurred
    timestamp: datetime.datetime
    #: Event status
    status: Status
    #: Database attachent ID
    attachment_id: int
    #: Statement ID
    statement_id: int
    #: BLR content
    content: str
    #: Prepare time in ms
    prepare_time: int

@dataclass(frozen=True)
class EventBLRExecute(TraceEvent):
    "BLR execution trace event"
    #: Trace event ID
    event_id: int
    #: Timestamp when the event occurred
    timestamp: datetime.datetime
    #: Event status
    status: Status
    #: Database attachent ID
    attachment_id: int
    #: Transaction ID
    transaction_id: int
    #: Statement ID
    statement_id: int
    #: BLR content
    content: str
    #: Execution time in ms
    run_time: int
    #: Number of page reads
    reads: int
    #: Number of page writes
    writes: int
    #: Number of page fetches
    fetches: int
    #: Number of pages with changes pending
    marks: int
    #: List with table access statistics
    access: List[AccessStats]

@dataclass(frozen=True)
class EventDYNExecute(TraceEvent):
    "DYN execution trace event"
    #: Trace event ID
    event_id: int
    #: Timestamp when the event occurred
    timestamp: datetime.datetime
    #: Event status
    status: Status
    #: Database attachent ID
    attachment_id: int
    #: Transaction ID
    transaction_id: int
    #: DYN content
    content: str
    #: Execution time in ms
    run_time: int
#
@dataclass(frozen=True)
class EventUnknown(TraceEvent):
    "Uknown trace event"
    #: Trace event ID
    event_id: int
    #: Timestamp when the event occurred
    timestamp: datetime.datetime
    #: Event data
    data: str

def safe_int(str_value: str, base: int=10):
    """Always returns integer value from string/None argument. Returns 0 if argument is None.
    """
    if str_value:
        return int(str_value, base)
    return 0

class TraceParser:
    """Parser for standard textual trace log. Produces dataclasses describing individual
    trace log entries/events.
    """
    def __init__(self):
        #: Set of attachment ids that were already processed
        self.seen_attachments: Set[int] = set()
        #: Set of transaction ids that were already processed
        self.seen_transactions: Set[int] = set()
        #: Set of service ids that were already processed
        self.seen_services: set[int] = set()
        #: Dictionary that maps (sql_cmd, plan) keys to internal ids
        self.sqlinfo_map: Dict[Tuple[str, str], int]= {}
        #: Dictionary that maps parameters (statement or procedure) keys to internal ids
        self.param_map = {}
        #: Sequence id that would be assigned to next parsed event (starts with 1)
        self.next_event_id: int = 1
        #: Sequence id that would be assigned to next parsed unique SQL command (starts with 1)
        self.next_sql_id: int = 1
        #: Sequence id that would be assigned to next parsed unique parameter (starts with 1)
        self.next_param_id: int = 1
        #: Parsing option indicating that parsed trace contains `FREE_STATEMENT` events.
        #: This has impact on cleanup of `SQLInfo` ID cache. When True, the SQLInfo is
        #: discarded when its FREE_STATEMENT event is processed. When False, the SQLInfo
        #: is discarded when its EXECUTE_STATEMENT_FINISH is processed.
        self.has_statement_free: bool = True
        #
        self.__infos: collections.deque = collections.deque()
        self.__pushed: List[str] = []
        self.__current_block: collections.deque = collections.deque()
        self.__last_timestamp: datetime.datetime = None
        self.__event_values: Dict[str, Any] = {}
        self.__parse_map = {Event.TRACE_INIT: self.__parser_trace_init,
                            Event.TRACE_FINI: self.__parser_trace_finish,
                            Event.START_TRANSACTION: self.__parser_start_transaction,
                            Event.COMMIT_TRANSACTION: self.__parser_commit_transaction,
                            Event.ROLLBACK_TRANSACTION: self.__parser_rollback_transaction,
                            Event.COMMIT_RETAINING: self.__parser_commit_retaining,
                            Event.ROLLBACK_RETAINING: self.__parser_rollback_retaining,
                            Event.PREPARE_STATEMENT: self.__parser_prepare_statement,
                            Event.EXECUTE_STATEMENT_START: self.__parser_execute_statement_start,
                            Event.EXECUTE_STATEMENT_FINISH: self.__parser_execute_statement_finish,
                            Event.FREE_STATEMENT: self.__parser_free_statement,
                            Event.CLOSE_CURSOR: self.__parser_close_cursor,
                            Event.EXECUTE_TRIGGER_START: self.__parser_trigger_start,
                            Event.EXECUTE_TRIGGER_FINISH: self.__parser_trigger_finish,
                            Event.EXECUTE_FUNCTION_START: self.__parser_func_start,
                            Event.EXECUTE_FUNCTION_FINISH: self.__parser_func_finish,
                            Event.EXECUTE_PROCEDURE_START: self.__parser_procedure_start,
                            Event.EXECUTE_PROCEDURE_FINISH: self.__parser_procedure_finish,
                            Event.CREATE_DATABASE: self.__parser_create_db,
                            Event.DROP_DATABASE: self.__parser_drop_db,
                            Event.ATTACH_DATABASE: self.__parser_attach,
                            Event.DETACH_DATABASE: self.__parser_detach,
                            Event.START_SERVICE: self.__parser_service_start,
                            Event.ATTACH_SERVICE: self.__parser_service_attach,
                            Event.DETACH_SERVICE: self.__parser_service_detach,
                            Event.QUERY_SERVICE: self.__parser_service_query,
                            Event.SET_CONTEXT: self.__parser_set_context,
                            Event.ERROR: self.__parser_error,
                            Event.WARNING: self.__parser_warning,
                            Event.SWEEP_START: self.__parser_sweep_start,
                            Event.SWEEP_PROGRESS: self.__parser_sweep_progress,
                            Event.SWEEP_FINISH: self.__parser_sweep_finish,
                            Event.SWEEP_FAILED: self.__parser_sweep_failed,
                            Event.COMPILE_BLR: self.__parser_blr_compile,
                            Event.EXECUTE_BLR: self.__parser_blr_execute,
                            Event.EXECUTE_DYN: self.__parser_dyn_execute,
                            Event.UNKNOWN: self.__parser_unknown}
    def _is_entry_header(self, line: str) -> bool:
        items = line.split()
        try:
            datetime.datetime.strptime(items[0], '%Y-%m-%dT%H:%M:%S.%f')
            return True
        except Exception:
            return False
    def _is_session_suspended(self, line: str) -> bool:
        return line.rfind('is suspended as its log is full ---') >= 0
    def _is_plan_separator(self, line: str) -> bool:
        return line == '^' * 79
    def _is_perf_start(self, line: str) -> bool:
        result = line.endswith(' records fetched')
        if result:
            result = line[:-len(' records fetched')].isdigit()
        return result
    def _is_blr_perf_start(self, line: str) -> bool:
        parts = line.split()
        return 'ms' in parts or 'fetch(es)' in parts or 'mark(s)' in parts or 'read(s)' in parts or 'write(s)' in parts
    def _is_param_start(self, line: str) -> bool:
        return line.startswith('param0 = ')
    def _iter_trace_blocks(self, ilines):
        lines = []
        for line in ilines:
            line = line.strip()
            if line:
                if not lines:
                    if self._is_entry_header(line):
                        lines.append(line)
                else:
                    if self._is_entry_header(line) or self._is_session_suspended(line):
                        yield lines
                        lines = [line]
                    else:
                        lines.append(line)
        if lines:
            yield lines
    def _identify_event(self, line: str) -> Event:
        items = line.split()
        if (len(items) == 3) or (items[2] in ('ERROR', 'WARNING')):
            return Event.__members__.get(items[2], Event.UNKNOWN)
        if items[2] == 'UNAUTHORIZED':
            return Event.__members__.get(items[3], Event.UNKNOWN)
        if items[2] == 'FAILED':
            return Event.__members__.get(items[3], Event.UNKNOWN)
        if items[2] == 'Unknown':
            return Event.UNKNOWN
        raise Error(f'Unrecognized event header: "{line}"')
    def _parse_attachment_info(self, values: Dict[str, Any], check: bool=True) -> None:
        database, _, attachment = self.__current_block.popleft().partition(' (')
        values['database'] = intern(database)
        attachment_id, user_role, charset, protocol_address = attachment.strip('()').split(',')
        _, attachment_id = attachment_id.split('_')
        values['attachment_id'] = int(attachment_id)
        values['charset'] = intern(charset.strip())
        protocol_address = protocol_address.strip()
        if protocol_address == '<internal>':
            protocol = address = protocol_address
        else:
            protocol, address = protocol_address.split(':', 1)
        values['protocol'] = intern(protocol)
        values['address'] = intern(address)
        if ':' in user_role:
            user, role = user_role.strip().split(':')
        else:
            user = user_role.strip()
            role = 'NONE'
        values['user'] = intern(user)
        values['role'] = intern(role)
        if protocol_address == '<internal>':
            values['remote_process'] = None
            values['remote_pid'] = None
        elif len(self.__current_block) > 0 and not (self.__current_block[0].startswith('(TRA') or
                                                    ' ms,' in self.__current_block[0] or
                                                    'Transaction counters:' in self.__current_block[0]):
            # This could be actually part of error message or separator line, not remote process spec
            values['remote_process'] = None
            values['remote_pid'] = None
            remote_process_id = self.__current_block[0]
            if not remote_process_id.startswith('---'):
                remote_process, remote_pid = remote_process_id.rsplit(':', 1)
                if remote_pid.isdigit():
                    # it looks like we have genuine remote process info
                    values['remote_process'] = intern(remote_process)
                    values['remote_pid'] = int(remote_pid)
                    self.__current_block.popleft()
        else:
            values['remote_process'] = None
            values['remote_pid'] = None
        #
        if check and values['attachment_id'] not in self.seen_attachments:
            self.__infos.append(AttachmentInfo(**values))
        self.seen_attachments.add(values['attachment_id'])
    def _parse_transaction_info(self, values: Dict[str, Any], check: bool=True) -> None:
        # Transaction parameters
        items = self.__current_block.popleft().strip('\t ()').split(',')
        if len(items) == 2:
            transaction_id, transaction_options = items
            initial_id = None
        else:
            transaction_id, initial_id, transaction_options = items
            initial_id = int(initial_id[6:])
        _, transaction_id = transaction_id.split('_')
        values['transaction_id'] = int(transaction_id)
        values['options'] = [intern(x.strip()) for x in transaction_options.split('|')]
        values['initial_id'] = initial_id
        if check and values['transaction_id'] not in self.seen_transactions:
            self.__infos.append(TransactionInfo(**values))
        del values['initial_id']
        self.seen_transactions.add(values['transaction_id'])
    def _parse_transaction_performance(self) -> None:
        self.__event_values['run_time'] = None
        self.__event_values['reads'] = None
        self.__event_values['writes'] = None
        self.__event_values['fetches'] = None
        self.__event_values['marks'] = None
        if self.__current_block:
            for value in self.__current_block.popleft().split(','):
                value, val_type = value.split()
                if 'ms' in val_type:
                    self.__event_values['run_time'] = int(value)
                elif 'read' in val_type:
                    self.__event_values['reads'] = int(value)
                elif 'write' in val_type:
                    self.__event_values['writes'] = int(value)
                elif 'fetch' in val_type:
                    self.__event_values['fetches'] = int(value)
                elif 'mark' in val_type:
                    self.__event_values['marks'] = int(value)
                else:
                    raise Error(f"Unhandled performance parameter {val_type}")
    def _parse_attachment_and_transaction(self) -> None:
        # Attachment
        att_values = {}
        self._parse_attachment_info(att_values)
        # Transaction
        tr_values = {}
        tr_values['attachment_id'] = att_values['attachment_id']
        self._parse_transaction_info(tr_values)
        self.__event_values['attachment_id'] = tr_values['attachment_id']
        self.__event_values['transaction_id'] = tr_values['transaction_id']
    def _parse_statement_id(self) -> None:
        self.__event_values['plan'] = None
        self.__event_values['sql'] = None
        line = self.__current_block.popleft()
        if line.startswith('Statement'):
            stmt_id = line.split()[1]
            self.__event_values['statement_id'] = int(stmt_id[:-1])
            if self.__event_values['status'] == Status.FAILED:
                return
            line = self.__current_block.popleft()
        else:
            self.__event_values['statement_id'] = 0
        if line != '-'*79:
            raise Error("Separator '-'*79 line expected")
    def _parse_blr_statement_id(self) -> None:
        line = self.__current_block[0].strip()
        if line.startswith('Statement ') and line[-1] == ':':
            _, stmt_id = self.__current_block.popleft().split()
            self.__event_values['statement_id'] = int(stmt_id[:-1])
        else:
            self.__event_values['statement_id'] = None
    def _parse_blrdyn_content(self) -> None:
        if self.__current_block[0] == '-' * 79:
            self.__current_block.popleft()
            content = []
            line = self.__current_block.popleft()
            while line and not self._is_blr_perf_start(line):
                content.append(line)
                line = self.__current_block.popleft() if self.__current_block else None
            if line:
                self.__current_block.appendleft(line)
            self.__event_values['content'] = '\n'.join(content)
        else:
            self.__event_values['content'] = None
    def _parse_prepare_time(self) -> None:
        if self.__current_block and self.__current_block[-1].endswith(' ms'):
            time, _ = self.__current_block.pop().split()
            self.__event_values['prepare_time'] = int(time)
        else:
            self.__event_values['prepare_time'] = None
    def _parse_sql_statement(self) -> None:
        if self.__current_block:
            sql = []
            line = self.__current_block.popleft()
            while line and not (self._is_plan_separator(line)
                                or self._is_perf_start(line)
                                or self._is_param_start(line)):
                sql.append(line)
                line = self.__current_block.popleft() if self.__current_block else None
            if line:
                self.__current_block.appendleft(line)
            self.__event_values['sql'] = intern('\n'.join(sql))
    def _parse_plan(self) -> None:
        if self.__current_block:
            line = self.__current_block.popleft()
            if self._is_perf_start(line) or self._is_param_start(line):
                self.__current_block.appendleft(line)
                return
            if not self._is_plan_separator(line):
                raise Error("Separator '^'*79 line expected")
            plan = []
            line = self.__current_block.popleft()
            while line and not (self._is_perf_start(line) or self._is_param_start(line)):
                plan.append(line)
                line = self.__current_block.popleft() if self.__current_block else None
            if line:
                self.__current_block.appendleft(line)
            self.__event_values['plan'] = intern('\n'.join(plan))
    def _parse_value_spec(self, param_def: str) -> Tuple[str, Any]:
        param_type, param_value = param_def.split(',', 1)
        param_type = intern(param_type)
        param_value = param_value.strip(' "')
        if param_value == '<NULL>':
            param_value = None
        elif param_type in ('smallint', 'integer', 'bigint'):
            param_value = int(param_value)
        elif param_type == 'timestamp':
            param_value = datetime.datetime.strptime(param_value, '%Y-%m-%dT%H:%M:%S.%f')
        elif param_type == 'date':
            param_value = datetime.datetime.strptime(param_value, '%Y-%m-%d')
        elif param_type == 'time':
            param_value = datetime.datetime.strptime(param_value, '%H:%M:%S.%f')
        elif param_type in ('float', 'double precision'):
            param_value = decimal.Decimal(param_value)
        return (param_type, param_value,)
    def _parse_parameters_block(self) -> List[Tuple[str, Any]]:
        parameters = []
        while self.__current_block and self.__current_block[0].startswith('param'):
            line = self.__current_block.popleft()
            _, param_def = line.split(' = ')
            parameters.append(self._parse_value_spec(param_def))
        return parameters
    def _parse_parameters(self, for_procedure: bool=False) -> None:
        parameters = self._parse_parameters_block()
        while self.__current_block and self.__current_block[0].endswith('more arguments skipped...'):
            self.__current_block.popleft()
        #
        param_id = None
        if parameters:
            key = tuple(parameters)
            if key in self.param_map:
                param_id = self.param_map[key]
            else:
                param_id = self.next_param_id
                self.next_param_id += 1
                self.param_map[key] = param_id
                self.__infos.append(ParamSet(**{'par_id': param_id, 'params': parameters}))
        #
        self.__event_values['param_id'] = param_id
    def _parse_performance(self) -> None:
        self.__event_values['run_time'] = None
        self.__event_values['reads'] = None
        self.__event_values['writes'] = None
        self.__event_values['fetches'] = None
        self.__event_values['marks'] = None
        self.__event_values['access'] = None
        if not self.__current_block:
            return
        if 'records fetched' in self.__current_block[0]:
            line = self.__current_block.popleft()
            self.__event_values['records'] = int(line.split()[0])
        else:
            self.__event_values['records'] = None
        values = self.__current_block.popleft().split(',')
        while values:
            next_value = values.pop()
            value, val_type = next_value.split()
            if 'ms' in val_type:
                self.__event_values['run_time'] = int(value)
            elif 'read' in val_type:
                self.__event_values['reads'] = int(value)
            elif 'write' in val_type:
                self.__event_values['writes'] = int(value)
            elif 'fetch' in val_type:
                self.__event_values['fetches'] = int(value)
            elif 'mark' in val_type:
                self.__event_values['marks'] = int(value)
            else:
                raise Error(f"Unhandled performance parameter {val_type}")
        if self.__current_block:
            self.__event_values['access'] = []
            if self.__current_block.popleft() != "Table                             Natural     Index    Update    Insert    Delete   Backout     Purge   Expunge":
                raise Error("Performance table header expected")
            if self.__current_block.popleft() != "*"*111:
                raise Error("Performance table header separator expected")
            while self.__current_block:
                entry = self.__current_block.popleft()
                self.__event_values['access'].append(AccessStats(intern(entry[:32].strip()),
                                                                 safe_int(entry[32:41].strip()),
                                                                 safe_int(entry[41:51].strip()),
                                                                 safe_int(entry[51:61].strip()),
                                                                 safe_int(entry[61:71].strip()),
                                                                 safe_int(entry[71:81].strip()),
                                                                 safe_int(entry[81:91].strip()),
                                                                 safe_int(entry[91:101].strip()),
                                                                 safe_int(entry[101:111].strip())))
    def _parse_sql_info(self) -> None:
        plan = self.__event_values['plan']
        sql = self.__event_values['sql']
        key = (sql, plan)
        #
        if key in self.sqlinfo_map:
            sql_id = self.sqlinfo_map[key]
        else:
            sql_id = self.next_sql_id
            self.next_sql_id += 1
            self.sqlinfo_map[key] = sql_id
            self.__infos.append(SQLInfo(**{'sql_id': sql_id, 'sql': sql, 'plan': plan,}))
        self.__event_values['sql_id'] = sql_id
    def _parse_trigger(self) -> None:
        trigger, event = self.__current_block.popleft().split('(')
        if ' FOR ' in trigger:
            trigger, table = trigger.split(' FOR ')
            self.__event_values['trigger'] = intern(trigger)
            self.__event_values['table'] = intern(table.strip())
        else:
            self.__event_values['trigger'] = intern(trigger.strip())
            self.__event_values['table'] = None
        self.__event_values['event'] = intern(event.strip('()'))
    def _parse_service(self) -> None:
        svc_id = ''
        line = self.__current_block.popleft()
        #if 'service_mgr' not in line:
            #raise Error("Service connection description expected.")
        _, _, spec = line.partition(' (')
        items = spec.strip('()').split(',')
        if len(items) == 4:
            svc_id, user, protocol_address, remote_process_id = items
        else:
            svc_id, user, protocol_address = items
            remote_process_id = None
        _, svc_id = svc_id.split(' ')
        svc_id = int(svc_id if svc_id.startswith('0x') else f'0x{svc_id}', 0)
        if svc_id not in self.seen_services:
            svc_values = {}
            svc_values['service_id'] = svc_id
            svc_values['user'] = intern(user.strip())
            protocol_address = protocol_address.strip()
            if protocol_address == 'internal':
                protocol = address = protocol_address
            else:
                protocol, address = protocol_address.split(':', 1)
            svc_values['protocol'] = intern(protocol)
            svc_values['address'] = intern(address)
            if remote_process_id is not None:
                remote_process_id = remote_process_id.strip()
                remote_process, remote_pid = remote_process_id.rsplit(':', 1)
                svc_values['remote_process'] = intern(remote_process)
                svc_values['remote_pid'] = int(remote_pid)
            else:
                svc_values['remote_process'] = None
                svc_values['remote_pid'] = None
            self.__infos.append(ServiceInfo(**svc_values))
            self.seen_services.add(svc_id)
        self.__event_values['service_id'] = svc_id
    def _parse_sweep_attachment(self) -> None:
        att_values = {}
        self._parse_attachment_info(att_values)
        self.__event_values['attachment_id'] = att_values['attachment_id']
    def _parse_sweep_tr_counters(self) -> None:
        line = self.__current_block.popleft()
        if not line:
            line = self.__current_block.popleft()
        if 'Transaction counters:' not in line:
            raise Error("Transaction counters expected")
        while len(self.__current_block) > 0:
            line = self.__current_block.popleft()
            if 'Oldest interesting' in line:
                self.__event_values['oit'] = int(line.rsplit(' ', 1)[1])
            elif 'Oldest active' in line:
                self.__event_values['oat'] = int(line.rsplit(' ', 1)[1])
            elif 'Oldest snapshot' in line:
                self.__event_values['ost'] = int(line.rsplit(' ', 1)[1])
            elif 'Next transaction' in line:
                self.__event_values['next'] = int(line.rsplit(' ', 1)[1])
            elif 'ms' in line and len(self.__current_block) >= 0:
                # Put back performance counters
                self.__current_block.appendleft(line)
                break
    def __parse_trace_header(self) -> None:
        line = self.__current_block.popleft()
        items = line.split()
        self.__last_timestamp = datetime.datetime.strptime(items[0], '%Y-%m-%dT%H:%M:%S.%f')
        if (len(items) == 3) or (items[2] in ('ERROR', 'WARNING')):
            self.__event_values['status'] = Status.OK
        else:
            if items[2] == 'UNAUTHORIZED':
                self.__event_values['status'] = Status.UNAUTHORIZED
            elif items[2] == 'FAILED':
                self.__event_values['status'] = Status.FAILED
            elif items[2] == 'Unknown':
                self.__event_values['status'] = Status.UNKNOWN
            else:
                raise Error(f'Unrecognized event header: "{line}"')
        #
        self.__event_values['event_id'] = self.next_event_id
        self.next_event_id += 1
        self.__event_values['timestamp'] = self.__last_timestamp
    def __parser_trace_suspend(self) -> EventTraceSuspend:
        # Session was suspended because log was full, so we will create fake event to note that
        line = self.__current_block.popleft()
        self.__event_values['timestamp'] = self.__last_timestamp
        self.__event_values['event_id'] = self.next_event_id
        self.next_event_id += 1
        session_name = line[4:line.find(' is suspended')]
        self.__event_values['session_name'] = intern(session_name.replace(' ', '_').upper())
        return EventTraceSuspend(**self.__event_values)
    def __parser_trace_init(self) -> EventTraceInit:
        self.__parse_trace_header()
        del self.__event_values['status']
        self.__event_values['session_name'] = intern(self.__current_block.popleft())
        return EventTraceInit(**self.__event_values)
    def __parser_trace_finish(self) -> EventTraceFinish:
        self.__parse_trace_header()
        del self.__event_values['status']
        self.__event_values['session_name'] = intern(self.__current_block.popleft())
        return EventTraceFinish(**self.__event_values)
    def __parser_start_transaction(self) -> EventTransactionStart:
        self.__parse_trace_header()
        # Attachment
        values = {}
        self._parse_attachment_info(values)
        self.__event_values['attachment_id'] = values['attachment_id']
        # Transaction parameters
        self._parse_transaction_info(self.__event_values, check=False)
        return EventTransactionStart(**self.__event_values)
    def __parser_commit_transaction(self) -> EventCommit:
        self.__parse_trace_header()
        # Attachment
        values = {}
        self._parse_attachment_info(values)
        self.__event_values['attachment_id'] = values['attachment_id']
        # Transaction parameters
        self._parse_transaction_info(self.__event_values, check=False)
        self._parse_transaction_performance()
        self.seen_transactions.remove(self.__event_values['transaction_id'])
        return EventCommit(**self.__event_values)
    def __parser_rollback_transaction(self) -> EventRollback:
        self.__parse_trace_header()
        # Attachment
        values = {}
        self._parse_attachment_info(values)
        self.__event_values['attachment_id'] = values['attachment_id']
        # Transaction parameters
        self._parse_transaction_info(self.__event_values, check=False)
        self._parse_transaction_performance()
        self.seen_transactions.remove(self.__event_values['transaction_id'])
        return EventRollback(**self.__event_values)
    def __parser_commit_retaining(self) -> EventCommitRetaining:
        self.__parse_trace_header()
        # Attachment
        values = {}
        self._parse_attachment_info(values)
        self.__event_values['attachment_id'] = values['attachment_id']
        # Transaction parameters
        self._parse_transaction_info(self.__event_values, check=False)
        if self.__current_block and self.__current_block[0].startswith('New number'):
            self.__event_values['new_transaction_id'] = int(self.__current_block.popleft().strip()[11:])
        else:
            self.__event_values['new_transaction_id'] = None
        self._parse_transaction_performance()
        return EventCommitRetaining(**self.__event_values)
    def __parser_rollback_retaining(self) -> EventRollbackRetaining:
        self.__parse_trace_header()
        # Attachment
        values = {}
        self._parse_attachment_info(values)
        self.__event_values['attachment_id'] = values['attachment_id']
        # Transaction parameters
        self._parse_transaction_info(self.__event_values, check=False)
        if self.__current_block and self.__current_block[0].startswith('New number'):
            self.__event_values['new_transaction_id'] = int(self.__current_block.popleft().strip()[11:])
        else:
            self.__event_values['new_transaction_id'] = None
        self._parse_transaction_performance()
        return EventRollbackRetaining(**self.__event_values)
    def __parser_prepare_statement(self) -> EventPrepareStatement:
        self.__parse_trace_header()
        self._parse_attachment_and_transaction()
        self._parse_statement_id()
        self._parse_prepare_time()
        self._parse_sql_statement()
        self._parse_plan()
        self._parse_sql_info()
        #
        del self.__event_values['plan']
        del self.__event_values['sql']
        return EventPrepareStatement(**self.__event_values)
    def __parser_execute_statement_start(self) -> EventStatementStart:
        self.__parse_trace_header()
        self._parse_attachment_and_transaction()
        self._parse_statement_id()
        self._parse_sql_statement()
        self._parse_plan()
        self._parse_parameters()
        self._parse_sql_info()
        #
        del self.__event_values['plan']
        del self.__event_values['sql']
        return EventStatementStart(**self.__event_values)
    def __parser_execute_statement_finish(self) -> EventStatementFinish:
        self.__parse_trace_header()
        self._parse_attachment_and_transaction()
        self._parse_statement_id()
        self._parse_sql_statement()
        self._parse_plan()
        self._parse_parameters()
        self.__event_values['records'] = None
        self._parse_performance()
        self._parse_sql_info()
        #
        if not self.has_statement_free:
            del self.sqlinfo_map[self.__event_values['sql'], self.__event_values['plan']]
        del self.__event_values['plan']
        del self.__event_values['sql']
        return EventStatementFinish(**self.__event_values)
    def __parser_free_statement(self) -> EventFreeStatement:
        self.__parse_trace_header()
        att_values = {}
        self._parse_attachment_info(att_values)
        self.__event_values['attachment_id'] = att_values['attachment_id']
        #self._parse_attachment_and_transaction()
        self._parse_statement_id()
        self._parse_sql_statement()
        self._parse_plan()
        self._parse_sql_info()
        del self.__event_values['status']
        #
        del self.sqlinfo_map[self.__event_values['sql'], self.__event_values['plan']]
        del self.__event_values['plan']
        del self.__event_values['sql']
        return EventFreeStatement(**self.__event_values)
    def __parser_close_cursor(self) -> EventCloseCursor:
        self.__parse_trace_header()
        att_values = {}
        self._parse_attachment_info(att_values)
        self.__event_values['attachment_id'] = att_values['attachment_id']
        #self._parse_attachment_and_transaction()
        self._parse_statement_id()
        self._parse_sql_statement()
        self._parse_plan()
        self._parse_sql_info()
        del self.__event_values['status']
        #
        del self.__event_values['plan']
        del self.__event_values['sql']
        return EventCloseCursor(**self.__event_values)
    def __parser_trigger_start(self) -> EventTriggerStart:
        self.__parse_trace_header()
        self._parse_attachment_and_transaction()
        self._parse_trigger()
        return EventTriggerStart(**self.__event_values)
    def __parser_trigger_finish(self) -> EventTriggerFinish:
        self.__parse_trace_header()
        self._parse_attachment_and_transaction()
        self._parse_trigger()
        self._parse_performance()
        if 'records' in self.__event_values:
            del self.__event_values['records']
        return EventTriggerFinish(**self.__event_values)
    def __parser_procedure_start(self) -> EventProcedureStart:
        self.__parse_trace_header()
        self._parse_attachment_and_transaction()
        _, name = self.__current_block.popleft().split()
        self.__event_values['procedure'] = intern(name[:-1])
        self._parse_parameters(for_procedure=True)
        return EventProcedureStart(**self.__event_values)
    def __parser_procedure_finish(self) -> EventProcedureFinish:
        self.__parse_trace_header()
        self._parse_attachment_and_transaction()
        _, name = self.__current_block.popleft().split()
        self.__event_values['procedure'] = intern(name[:-1])
        self._parse_parameters(for_procedure=True)
        self._parse_performance()
        return EventProcedureFinish(**self.__event_values)
    def __parser_func_start(self) -> EventProcedureStart:
        self.__parse_trace_header()
        self._parse_attachment_and_transaction()
        _, name = self.__current_block.popleft().split()
        self.__event_values['function'] = intern(name[:-1])
        self._parse_parameters(for_procedure=True)
        return EventFunctionStart(**self.__event_values)
    def __parser_func_finish(self) -> EventProcedureFinish:
        self.__parse_trace_header()
        self._parse_attachment_and_transaction()
        _, name = self.__current_block.popleft().split()
        self.__event_values['function'] = intern(name[:-1])
        self._parse_parameters(for_procedure=True)
        self.__current_block.popleft() # returns:
        self.__event_values['returns'] = self._parse_parameters_block()[0]
        self._parse_performance()
        if 'records' in self.__event_values:
            del self.__event_values['records']
        return EventFunctionFinish(**self.__event_values)
    def __parser_create_db(self) -> EventCreate:
        self.__parse_trace_header()
        # Attachment parameters
        self._parse_attachment_info(self.__event_values, check=False)
        return EventCreate(**self.__event_values)
    def __parser_drop_db(self) -> EventDrop:
        self.__parse_trace_header()
        # Attachment parameters
        self._parse_attachment_info(self.__event_values, check=False)
        return EventDrop(**self.__event_values)
    def __parser_attach(self) -> EventAttach:
        self.__parse_trace_header()
        # Attachment parameters
        self._parse_attachment_info(self.__event_values, check=False)
        return EventAttach(**self.__event_values)
    def __parser_detach(self) -> EventDetach:
        self.__parse_trace_header()
        # Attachment parameters
        self._parse_attachment_info(self.__event_values, check=False)
        self.seen_attachments.remove(self.__event_values['attachment_id'])
        return EventDetach(**self.__event_values)
    def __parser_service_start(self) -> EventServiceStart:
        self.__parse_trace_header()
        self._parse_service()
        # service parameters
        action = self.__current_block.popleft().strip('"')
        self.__event_values['action'] = intern(action)
        parameters = []
        while len(self.__current_block) > 0:
            parameters.append(self.__current_block.popleft())
        self.__event_values['parameters'] = parameters
        #
        return EventServiceStart(**self.__event_values)
    def __parser_service_attach(self) -> EventServiceAttach:
        self.__parse_trace_header()
        self._parse_service()
        return EventServiceAttach(**self.__event_values)
    def __parser_service_detach(self) -> EventServiceDetach:
        self.__parse_trace_header()
        self._parse_service()
        self.seen_services.remove(self.__event_values['service_id'])
        return EventServiceDetach(**self.__event_values)
    def __parser_service_query(self) -> EventServiceQuery:
        self.__parse_trace_header()
        self._parse_service()
        # service parameters
        line = self.__current_block.popleft().strip()
        if line[0] == line[-1] == '"':
            action = line.strip('"')
            self.__event_values['action'] = intern(action)
            if len(self.__current_block) > 0:
                line = self.__current_block.popleft().strip()
        else:
            self.__event_values['action'] = None
        sent = []
        received = []
        while len(self.__current_block) > 0:
            #line = self.__current_block.popleft().strip()
            if line.startswith('Send portion of the query:'):
                while not line.startswith('Receive portion of the query:'):
                    line = self.__current_block.popleft().strip()
                    sent.append(line)
                    if len(self.__current_block) == 0:
                        break
            if line.startswith('Receive portion of the query:'):
                while len(self.__current_block) > 0:
                    received.append(self.__current_block.popleft().strip())
        self.__event_values['sent'] = sent
        self.__event_values['received'] = received
        #
        return EventServiceQuery(**self.__event_values)
    def __parser_set_context(self) -> EventSetContext:
        self.__parse_trace_header()
        self._parse_attachment_and_transaction()
        line = self.__current_block.popleft()
        context, line = line.split(']', 1)
        key, value = line.split('=', 1)
        self.__event_values['context'] = intern(context[1:])
        self.__event_values['key'] = intern(key.strip())
        self.__event_values['value'] = value.strip(' "')
        del self.__event_values['status']
        return EventSetContext(**self.__event_values)
    def __parser_error(self) -> Union[EventServiceError, EventError]:
        self.__event_values['place'] = self.__current_block[0].split(' AT ')[1]
        self.__parse_trace_header()
        att_values = {}
        if 'service_mgr' in self.__current_block[0]:
            event_class = EventServiceError
            self._parse_service()
        else:
            event_class = EventError
            self._parse_attachment_info(att_values)
            self.__event_values['attachment_id'] = att_values['attachment_id']
        details = []
        while len(self.__current_block) > 0:
            details.append(self.__current_block.popleft())
        self.__event_values['details'] = details
        del self.__event_values['status']
        return event_class(**self.__event_values)
    def __parser_warning(self) -> Union[EventServiceWarning, EventWarning]:
        self.__event_values['place'] = self.__current_block[0].split(' AT ')[1]
        self.__parse_trace_header()
        att_values = {}
        if 'service_mgr' in self.__current_block[0]:
            event_class = EventServiceWarning
            self._parse_service()
        else:
            event_class = EventWarning
            self._parse_attachment_info(att_values)
            self.__event_values['attachment_id'] = att_values['attachment_id']
        details = []
        while len(self.__current_block) > 0:
            details.append(self.__current_block.popleft())
        self.__event_values['details'] = details
        del self.__event_values['status']
        return event_class(**self.__event_values)
    def __parser_sweep_start(self) -> EventSweepStart:
        self.__parse_trace_header()
        self._parse_sweep_attachment()
        self._parse_sweep_tr_counters()
        del self.__event_values['status']
        return EventSweepStart(**self.__event_values)
    def __parser_sweep_progress(self) -> EventSweepProgress:
        self.__parse_trace_header()
        self._parse_sweep_attachment()
        self._parse_performance()
        del self.__event_values['status']
        del self.__event_values['records']
        return EventSweepProgress(**self.__event_values)
    def __parser_sweep_finish(self) -> EventSweepFinish:
        self.__parse_trace_header()
        self._parse_sweep_attachment()
        self._parse_sweep_tr_counters()
        self._parse_performance()
        del self.__event_values['status']
        del self.__event_values['records']
        return EventSweepFinish(**self.__event_values)
    def __parser_sweep_failed(self) -> EventSweepFailed:
        self.__parse_trace_header()
        self._parse_sweep_attachment()
        del self.__event_values['status']
        return EventSweepFailed(**self.__event_values)
    def __parser_blr_compile(self) -> EventBLRCompile:
        self.__parse_trace_header()
        # Attachment
        values = {}
        self._parse_attachment_info(values)
        self.__event_values['attachment_id'] = values['attachment_id']
        # BLR
        self._parse_blr_statement_id()
        self._parse_blrdyn_content()
        self._parse_prepare_time()
        return EventBLRCompile(**self.__event_values)
    def __parser_blr_execute(self) -> EventBLRExecute:
        self.__parse_trace_header()
        self._parse_attachment_and_transaction()
        # BLR
        self._parse_blr_statement_id()
        self._parse_blrdyn_content()
        self._parse_performance()
        del self.__event_values['records']
        return EventBLRExecute(**self.__event_values)
    def __parser_dyn_execute(self) -> EventDYNExecute:
        self.__parse_trace_header()
        self._parse_attachment_and_transaction()
        # DYN
        self._parse_blrdyn_content()
        value, _ = self.__current_block.popleft().split()
        self.__event_values['run_time'] = int(value)
        return EventDYNExecute(**self.__event_values)
    def __parser_unknown(self) -> EventUnknown:
        items = self.__current_block[0].split()
        self.__parse_trace_header()
        self.__current_block.appendleft(' '.join(items[2:]))
        del self.__event_values['status']
        self.__event_values['data'] = '\n'.join(self.__current_block)
        return EventUnknown(**self.__event_values)
    def retrieve_info(self) -> List[TraceInfo]:
        """Returns list of `.TraceInfo` instances produced by last `.parse_event()` call.

        The list could be empty.

        Important:

           The internal buffer for info instances is cleared after call to this method,
           so all produced info instances are returned only once.
        """
        result = self.__infos.copy()
        self.__infos.clear()
        return result
    def parse_event(self, trace_block: List[str]) -> TraceEvent:
        """Parse single trace event.

        Arguments:
            trace_block: List with trace entry lines for single trace event.
        """
        self.__current_block.clear()
        self.__current_block.extend(trace_block)
        self.__event_values.clear()
        if self._is_session_suspended(self.__current_block[0]):
            return self.__parser_trace_suspend()
        return self.__parse_map[self._identify_event(self.__current_block[0])]()
    def parse(self, lines: Iterable):
        """Parse output from Firebird trace session.

        Arguments:
            lines: Iterable that return lines produced by Firebird trace session.

        Yields:
            `.TraceEvent` and `.TraceInfo` dataclasses describing individual trace log
            entries/events.

        Raises:
            firebird.base.types.Error: When any problem is found in input stream.
        """
        for rec in (self.parse_event(x) for x in self._iter_trace_blocks(lines)):
            while len(self.__infos) > 0:
                yield self.__infos.popleft()
            yield rec
    def push(self, line: Union[str, Sentinel]) -> Optional[List[Union[TraceEvent, TraceInfo]]]:
        """Push parser.

        Arguments:
            line: Single trace output line, or `~firebird.base.types.STOP` sentinel.

        Returns:
            None, or list with parsed elements (single event preceded by any info blocks
            related to it).

        Raises:
            Error: When pushed line is not recognized as part of trace event.
        """
        if line is STOP:
            if self.__pushed:
                event = self.parse_event(self.__pushed)
                self.__pushed.clear()
                result = self.__infos.copy()
                result.append(event)
                return result
        else:
            line = line.strip()
            if line:
                if not self.__pushed:
                    if self._is_entry_header(line):
                        self.__pushed.append(line)
                    else:
                        raise Error(f'Unrecognized trace line "{line}"')
                else:
                    if self._is_entry_header(line) or self._is_session_suspended(line):
                        event = self.parse_event(self.__pushed)
                        result = self.__infos.copy()
                        self.__infos.clear()
                        result.append(event)
                        self.__pushed = [line]
                        return result
                    self.__pushed.append(line)
        return None
