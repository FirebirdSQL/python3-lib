#coding:utf-8
#
# PROGRAM/MODULE: firebird-lib
# FILE:           test_trace.py
# DESCRIPTION:    Unit tests for firebird.lib.trace
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

"""firebird-lib - Unit tests for firebird.lib.trace


"""

import unittest
import sys, os
from collections.abc import Sized, MutableSequence, Mapping
from re import finditer
from io import StringIO
from firebird.driver import *
from firebird.lib.trace import *

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

def iter_obj_properties(obj):
    """Iterator function.

    Args:
        obj (class): Class object.

    Yields:
        `name', 'property` pairs for all properties in class.
"""
    for varname in dir(obj):
        if hasattr(type(obj), varname) and isinstance(getattr(type(obj), varname), property):
            yield varname

def iter_obj_variables(obj):
    """Iterator function.

    Args:
        obj (class): Class object.

    Yields:
        Names of all non-callable attributes in class.
"""
    for varname in vars(obj):
        value = getattr(obj, varname)
        if not callable(value) and not varname.startswith('_'):
            yield varname

def get_object_data(obj, skip=[]):
    def add(item):
        if item not in skip:
            value = getattr(obj, item)
            if isinstance(value, Sized) and isinstance(value, (MutableSequence, Mapping)):
                value = len(value)
            data[item] = value

    data = {}
    for item in iter_obj_variables(obj):
        add(item)
    for item in iter_obj_properties(obj):
        add(item)
    return data

class TestBase(unittest.TestCase):
    def __init__(self, methodName='runTest'):
        super(TestBase, self).__init__(methodName)
        self.output = StringIO()
        self.FBTEST_DB = 'fbtest'
        self.maxDiff = None
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

class TestTraceParse(TestBase):
    def setUp(self):
        super().setUp()
        self.dbfile = os.path.join(self.dbpath, self.FBTEST_DB)
    def test_00_linesplit_iter(self):
        trace_lines = """2014-05-23T11:00:28.5840 (3720:0000000000EFD9E8) ATTACH_DATABASE
        /home/employee.fdb (ATT_8, SYSDBA:NONE, ISO88591, TCPv4:192.168.1.5)
        /opt/firebird/bin/isql:8723

"""
        for line in linesplit_iter(trace_lines):
            self.output.write(line + '\n')
        self.assertEqual(self.output.getvalue(), trace_lines)
    def _check_events(self, trace_lines, output):
        self.output = StringIO()
        parser = TraceParser()
        for obj in parser.parse(linesplit_iter(trace_lines)):
            self.printout(str(obj))
        self.assertEqual(self.output.getvalue(), output, "PARSE: Parsed events do not match expected ones")
        self._push_check_events(trace_lines, output)
        self.output.close()
    def _push_check_events(self, trace_lines, output):
        self.output = StringIO()
        parser = TraceParser()
        for line in linesplit_iter(trace_lines):
            if events:= parser.push(line):
                for event in events:
                    self.printout(str(event))
        if events:= parser.push(STOP):
            for event in events:
                self.printout(str(event))
        self.assertEqual(self.output.getvalue(), output, "PUSH: Parsed events do not match expected ones")
        self.output.close()
    def test_01_trace_init(self):
        trace_lines = """2014-05-23T11:00:28.5840 (3720:0000000000EFD9E8) TRACE_INIT
        SESSION_1

"""
        output = "EventTraceInit(event_id=1, timestamp=datetime.datetime(2014, 5, 23, 11, 0, 28, 584000), session_name='SESSION_1')\n"
        self._check_events(trace_lines, output)
    def test_02_trace_suspend(self):
        trace_lines = """2014-05-23T11:00:28.5840 (3720:0000000000EFD9E8) TRACE_INIT
        SESSION_1

--- Session 1 is suspended as its log is full ---
2014-05-23T12:01:01.1420 (3720:0000000000EFD9E8) TRACE_INIT
	SESSION_1

"""
        output = """EventTraceInit(event_id=1, timestamp=datetime.datetime(2014, 5, 23, 11, 0, 28, 584000), session_name='SESSION_1')
EventTraceSuspend(event_id=2, timestamp=datetime.datetime(2014, 5, 23, 11, 0, 28, 584000), session_name='SESSION_1')
EventTraceInit(event_id=3, timestamp=datetime.datetime(2014, 5, 23, 12, 1, 1, 142000), session_name='SESSION_1')
"""
        self._check_events(trace_lines, output)
    def test_03_trace_finish(self):
        trace_lines = """2014-05-23T11:00:28.5840 (3720:0000000000EFD9E8) TRACE_INIT
        SESSION_1

2014-05-23T11:01:24.8080 (3720:0000000000EFD9E8) TRACE_FINI
	SESSION_1

"""
        output = """EventTraceInit(event_id=1, timestamp=datetime.datetime(2014, 5, 23, 11, 0, 28, 584000), session_name='SESSION_1')
EventTraceFinish(event_id=2, timestamp=datetime.datetime(2014, 5, 23, 11, 1, 24, 808000), session_name='SESSION_1')
"""
        self._check_events(trace_lines, output)
    def test_04_create_database(self):
        trace_lines = """2018-03-29T14:20:55.1180 (6290:0x7f9bb00bb978) CREATE_DATABASE
	/home/employee.fdb (ATT_8, SYSDBA:NONE, ISO88591, TCPv4:192.168.1.5)
	/opt/firebird/bin/isql:8723

"""
        output = """EventCreate(event_id=1, timestamp=datetime.datetime(2018, 3, 29, 14, 20, 55, 118000), status=<Status.OK: ' '>, attachment_id=8, database='/home/employee.fdb', charset='ISO88591', protocol='TCPv4', address='192.168.1.5', user='SYSDBA', role='NONE', remote_process='/opt/firebird/bin/isql', remote_pid=8723)
"""
        self._check_events(trace_lines, output)
    def test_05_drop_database(self):
        trace_lines = """2018-03-29T14:20:55.1180 (6290:0x7f9bb00bb978) DROP_DATABASE
	/home/employee.fdb (ATT_8, SYSDBA:NONE, ISO88591, TCPv4:192.168.1.5)
	/opt/firebird/bin/isql:8723

"""
        output = """EventDrop(event_id=1, timestamp=datetime.datetime(2018, 3, 29, 14, 20, 55, 118000), status=<Status.OK: ' '>, attachment_id=8, database='/home/employee.fdb', charset='ISO88591', protocol='TCPv4', address='192.168.1.5', user='SYSDBA', role='NONE', remote_process='/opt/firebird/bin/isql', remote_pid=8723)
"""
        self._check_events(trace_lines, output)
    def test_06_attach(self):
        trace_lines = """2014-05-23T11:00:28.5840 (3720:0000000000EFD9E8) ATTACH_DATABASE
	/home/employee.fdb (ATT_8, SYSDBA:NONE, ISO88591, TCPv4:192.168.1.5)
	/opt/firebird/bin/isql:8723
"""
        output = """EventAttach(event_id=1, timestamp=datetime.datetime(2014, 5, 23, 11, 0, 28, 584000), status=<Status.OK: ' '>, attachment_id=8, database='/home/employee.fdb', charset='ISO88591', protocol='TCPv4', address='192.168.1.5', user='SYSDBA', role='NONE', remote_process='/opt/firebird/bin/isql', remote_pid=8723)
"""
        self._check_events(trace_lines, output)
    def test_07_attach_failed(self):
        trace_lines = """2014-05-23T11:00:28.5840 (3720:0000000000EFD9E8) FAILED ATTACH_DATABASE
	/home/employee.fdb (ATT_8, SYSDBA:NONE, ISO88591, TCPv4:192.168.1.5)
	/opt/firebird/bin/isql:8723

"""
        output = """EventAttach(event_id=1, timestamp=datetime.datetime(2014, 5, 23, 11, 0, 28, 584000), status=<Status.FAILED: 'F'>, attachment_id=8, database='/home/employee.fdb', charset='ISO88591', protocol='TCPv4', address='192.168.1.5', user='SYSDBA', role='NONE', remote_process='/opt/firebird/bin/isql', remote_pid=8723)
"""
        self._check_events(trace_lines, output)
    def test_08_unauthorized_attach(self):
        trace_lines = """2014-09-24T14:46:15.0350 (2453:0x7fed02a04910) UNAUTHORIZED ATTACH_DATABASE
	/home/employee.fdb (ATT_0, sysdba, NONE, TCPv4:127.0.0.1)
	/opt/firebird/bin/isql:8723

"""
        output = """EventAttach(event_id=1, timestamp=datetime.datetime(2014, 9, 24, 14, 46, 15, 35000), status=<Status.UNAUTHORIZED: 'U'>, attachment_id=0, database='/home/employee.fdb', charset='NONE', protocol='TCPv4', address='127.0.0.1', user='sysdba', role='NONE', remote_process='/opt/firebird/bin/isql', remote_pid=8723)
"""
        self._check_events(trace_lines, output)
    def test_09_detach(self):
        trace_lines = """2014-05-23T11:00:28.5840 (3720:0000000000EFD9E8) ATTACH_DATABASE
	/home/employee.fdb (ATT_8, SYSDBA:NONE, ISO88591, TCPv4:192.168.1.5)
	/opt/firebird/bin/isql:8723

2014-05-23T11:01:24.8080 (3720:0000000000EFD9E8) DETACH_DATABASE
	/home/employee.fdb (ATT_8, SYSDBA:NONE, ISO88591, TCPv4:192.168.1.5)
	/opt/firebird/bin/isql:8723

"""
        output = """EventAttach(event_id=1, timestamp=datetime.datetime(2014, 5, 23, 11, 0, 28, 584000), status=<Status.OK: ' '>, attachment_id=8, database='/home/employee.fdb', charset='ISO88591', protocol='TCPv4', address='192.168.1.5', user='SYSDBA', role='NONE', remote_process='/opt/firebird/bin/isql', remote_pid=8723)
EventDetach(event_id=2, timestamp=datetime.datetime(2014, 5, 23, 11, 1, 24, 808000), status=<Status.OK: ' '>, attachment_id=8, database='/home/employee.fdb', charset='ISO88591', protocol='TCPv4', address='192.168.1.5', user='SYSDBA', role='NONE', remote_process='/opt/firebird/bin/isql', remote_pid=8723)
"""
        self._check_events(trace_lines, output)
    def test_10_detach_without_attach(self):
        trace_lines = """2014-05-23T11:01:24.8080 (3720:0000000000EFD9E8) DETACH_DATABASE
	/home/employee.fdb (ATT_8, SYSDBA:NONE, ISO88591, TCPv4:192.168.1.5)
	/opt/firebird/bin/isql:8723

"""
        output = """EventDetach(event_id=1, timestamp=datetime.datetime(2014, 5, 23, 11, 1, 24, 808000), status=<Status.OK: ' '>, attachment_id=8, database='/home/employee.fdb', charset='ISO88591', protocol='TCPv4', address='192.168.1.5', user='SYSDBA', role='NONE', remote_process='/opt/firebird/bin/isql', remote_pid=8723)
"""
        self._check_events(trace_lines, output)
    def test_11_start_transaction(self):
        trace_lines = """2014-05-23T11:00:28.5840 (3720:0000000000EFD9E8) ATTACH_DATABASE
	/home/employee.fdb (ATT_8, SYSDBA:NONE, ISO88591, TCPv4:192.168.1.5)
	/opt/firebird/bin/isql:8723

2014-05-23T11:00:28.6160 (3720:0000000000EFD9E8) START_TRANSACTION
	/home/employee.fdb (ATT_8, SYSDBA:NONE, ISO88591, TCPv4:192.168.1.5)
	/opt/firebird/bin/isql:8723
		(TRA_1568, READ_COMMITTED | REC_VERSION | WAIT | READ_WRITE)

"""
        output = """EventAttach(event_id=1, timestamp=datetime.datetime(2014, 5, 23, 11, 0, 28, 584000), status=<Status.OK: ' '>, attachment_id=8, database='/home/employee.fdb', charset='ISO88591', protocol='TCPv4', address='192.168.1.5', user='SYSDBA', role='NONE', remote_process='/opt/firebird/bin/isql', remote_pid=8723)
EventTransactionStart(event_id=2, timestamp=datetime.datetime(2014, 5, 23, 11, 0, 28, 616000), status=<Status.OK: ' '>, attachment_id=8, transaction_id=1568, options=['READ_COMMITTED', 'REC_VERSION', 'WAIT', 'READ_WRITE'])
"""
        self._check_events(trace_lines, output)
    def test_12_start_transaction_without_attachment(self):
        trace_lines = """2014-05-23T11:00:28.6160 (3720:0000000000EFD9E8) START_TRANSACTION
	/home/employee.fdb (ATT_8, SYSDBA:NONE, ISO88591, TCPv4:192.168.1.5)
	/opt/firebird/bin/isql:8723
		(TRA_1568, READ_COMMITTED | REC_VERSION | WAIT | READ_WRITE)

"""
        output = """AttachmentInfo(attachment_id=8, database='/home/employee.fdb', charset='ISO88591', protocol='TCPv4', address='192.168.1.5', user='SYSDBA', role='NONE', remote_process='/opt/firebird/bin/isql', remote_pid=8723)
EventTransactionStart(event_id=1, timestamp=datetime.datetime(2014, 5, 23, 11, 0, 28, 616000), status=<Status.OK: ' '>, attachment_id=8, transaction_id=1568, options=['READ_COMMITTED', 'REC_VERSION', 'WAIT', 'READ_WRITE'])
"""
        self._check_events(trace_lines, output)
    def test_13_commit(self):
        trace_lines = """2014-05-23T11:00:28.5840 (3720:0000000000EFD9E8) ATTACH_DATABASE
	/home/employee.fdb (ATT_8, SYSDBA:NONE, ISO88591, TCPv4:192.168.1.5)
	/opt/firebird/bin/isql:8723

2014-05-23T11:00:28.6160 (3720:0000000000EFD9E8) START_TRANSACTION
	/home/employee.fdb (ATT_8, SYSDBA:NONE, ISO88591, TCPv4:192.168.1.5)
	/opt/firebird/bin/isql:8723
		(TRA_1568, READ_COMMITTED | REC_VERSION | WAIT | READ_WRITE)

2014-05-23T11:00:29.9570 (3720:0000000000EFD9E8) COMMIT_TRANSACTION
	/home/employee.fdb (ATT_8, SYSDBA:NONE, ISO88591, TCPv4:192.168.1.5)
	/opt/firebird/bin/isql:8723
		(TRA_1568, READ_COMMITTED | REC_VERSION | WAIT | READ_WRITE)
      0 ms, 1 read(s), 1 write(s), 1 fetch(es), 1 mark(s)

"""
        output = """EventAttach(event_id=1, timestamp=datetime.datetime(2014, 5, 23, 11, 0, 28, 584000), status=<Status.OK: ' '>, attachment_id=8, database='/home/employee.fdb', charset='ISO88591', protocol='TCPv4', address='192.168.1.5', user='SYSDBA', role='NONE', remote_process='/opt/firebird/bin/isql', remote_pid=8723)
EventTransactionStart(event_id=2, timestamp=datetime.datetime(2014, 5, 23, 11, 0, 28, 616000), status=<Status.OK: ' '>, attachment_id=8, transaction_id=1568, options=['READ_COMMITTED', 'REC_VERSION', 'WAIT', 'READ_WRITE'])
EventCommit(event_id=3, timestamp=datetime.datetime(2014, 5, 23, 11, 0, 29, 957000), status=<Status.OK: ' '>, attachment_id=8, transaction_id=1568, options=['READ_COMMITTED', 'REC_VERSION', 'WAIT', 'READ_WRITE'], run_time=0, reads=1, writes=1, fetches=1, marks=1)
"""
        self._check_events(trace_lines, output)
    def test_14_commit_no_performance(self):
        trace_lines = """2014-05-23T11:00:28.5840 (3720:0000000000EFD9E8) ATTACH_DATABASE
	/home/employee.fdb (ATT_8, SYSDBA:NONE, ISO88591, TCPv4:192.168.1.5)
	/opt/firebird/bin/isql:8723

2014-05-23T11:00:28.6160 (3720:0000000000EFD9E8) START_TRANSACTION
	/home/employee.fdb (ATT_8, SYSDBA:NONE, ISO88591, TCPv4:192.168.1.5)
	/opt/firebird/bin/isql:8723
		(TRA_1568, READ_COMMITTED | REC_VERSION | WAIT | READ_WRITE)

2014-05-23T11:00:29.9570 (3720:0000000000EFD9E8) COMMIT_TRANSACTION
	/home/employee.fdb (ATT_8, SYSDBA:NONE, ISO88591, TCPv4:192.168.1.5)
	/opt/firebird/bin/isql:8723
		(TRA_1568, READ_COMMITTED | REC_VERSION | WAIT | READ_WRITE)

"""
        output = """EventAttach(event_id=1, timestamp=datetime.datetime(2014, 5, 23, 11, 0, 28, 584000), status=<Status.OK: ' '>, attachment_id=8, database='/home/employee.fdb', charset='ISO88591', protocol='TCPv4', address='192.168.1.5', user='SYSDBA', role='NONE', remote_process='/opt/firebird/bin/isql', remote_pid=8723)
EventTransactionStart(event_id=2, timestamp=datetime.datetime(2014, 5, 23, 11, 0, 28, 616000), status=<Status.OK: ' '>, attachment_id=8, transaction_id=1568, options=['READ_COMMITTED', 'REC_VERSION', 'WAIT', 'READ_WRITE'])
EventCommit(event_id=3, timestamp=datetime.datetime(2014, 5, 23, 11, 0, 29, 957000), status=<Status.OK: ' '>, attachment_id=8, transaction_id=1568, options=['READ_COMMITTED', 'REC_VERSION', 'WAIT', 'READ_WRITE'], run_time=None, reads=None, writes=None, fetches=None, marks=None)
"""
        self._check_events(trace_lines, output)
    def test_15_commit_without_attachment_and_start(self):
        trace_lines = """2014-05-23T11:00:29.9570 (3720:0000000000EFD9E8) COMMIT_TRANSACTION
	/home/employee.fdb (ATT_8, SYSDBA:NONE, ISO88591, TCPv4:192.168.1.5)
	/opt/firebird/bin/isql:8723
		(TRA_1568, READ_COMMITTED | REC_VERSION | WAIT | READ_WRITE)
      0 ms, 1 read(s), 1 write(s), 1 fetch(es), 1 mark(s)

"""
        output = """AttachmentInfo(attachment_id=8, database='/home/employee.fdb', charset='ISO88591', protocol='TCPv4', address='192.168.1.5', user='SYSDBA', role='NONE', remote_process='/opt/firebird/bin/isql', remote_pid=8723)
EventCommit(event_id=1, timestamp=datetime.datetime(2014, 5, 23, 11, 0, 29, 957000), status=<Status.OK: ' '>, attachment_id=8, transaction_id=1568, options=['READ_COMMITTED', 'REC_VERSION', 'WAIT', 'READ_WRITE'], run_time=0, reads=1, writes=1, fetches=1, marks=1)
"""
        self._check_events(trace_lines, output)
    def test_16_rollback(self):
        trace_lines = """2014-05-23T11:00:28.5840 (3720:0000000000EFD9E8) ATTACH_DATABASE
	/home/employee.fdb (ATT_8, SYSDBA:NONE, ISO88591, TCPv4:192.168.1.5)
	/opt/firebird/bin/isql:8723

2014-05-23T11:00:28.6160 (3720:0000000000EFD9E8) START_TRANSACTION
	/home/employee.fdb (ATT_8, SYSDBA:NONE, ISO88591, TCPv4:192.168.1.5)
	/opt/firebird/bin/isql:8723
		(TRA_1568, READ_COMMITTED | REC_VERSION | WAIT | READ_WRITE)

2014-05-23T11:00:29.9570 (3720:0000000000EFD9E8) ROLLBACK_TRANSACTION
	/home/employee.fdb (ATT_8, SYSDBA:NONE, ISO88591, TCPv4:192.168.1.5)
	/opt/firebird/bin/isql:8723
		(TRA_1568, READ_COMMITTED | REC_VERSION | WAIT | READ_WRITE)
0 ms

"""
        output = """EventAttach(event_id=1, timestamp=datetime.datetime(2014, 5, 23, 11, 0, 28, 584000), status=<Status.OK: ' '>, attachment_id=8, database='/home/employee.fdb', charset='ISO88591', protocol='TCPv4', address='192.168.1.5', user='SYSDBA', role='NONE', remote_process='/opt/firebird/bin/isql', remote_pid=8723)
EventTransactionStart(event_id=2, timestamp=datetime.datetime(2014, 5, 23, 11, 0, 28, 616000), status=<Status.OK: ' '>, attachment_id=8, transaction_id=1568, options=['READ_COMMITTED', 'REC_VERSION', 'WAIT', 'READ_WRITE'])
EventRollback(event_id=3, timestamp=datetime.datetime(2014, 5, 23, 11, 0, 29, 957000), status=<Status.OK: ' '>, attachment_id=8, transaction_id=1568, options=['READ_COMMITTED', 'REC_VERSION', 'WAIT', 'READ_WRITE'], run_time=0, reads=None, writes=None, fetches=None, marks=None)
"""
        self._check_events(trace_lines, output)
    def test_17_rollback_no_performance(self):
        trace_lines = """2014-05-23T11:00:28.5840 (3720:0000000000EFD9E8) ATTACH_DATABASE
	/home/employee.fdb (ATT_8, SYSDBA:NONE, ISO88591, TCPv4:192.168.1.5)
	/opt/firebird/bin/isql:8723

2014-05-23T11:00:28.6160 (3720:0000000000EFD9E8) START_TRANSACTION
	/home/employee.fdb (ATT_8, SYSDBA:NONE, ISO88591, TCPv4:192.168.1.5)
	/opt/firebird/bin/isql:8723
		(TRA_1568, READ_COMMITTED | REC_VERSION | WAIT | READ_WRITE)

2014-05-23T11:00:29.9570 (3720:0000000000EFD9E8) ROLLBACK_TRANSACTION
	/home/employee.fdb (ATT_8, SYSDBA:NONE, ISO88591, TCPv4:192.168.1.5)
	/opt/firebird/bin/isql:8723
		(TRA_1568, READ_COMMITTED | REC_VERSION | WAIT | READ_WRITE)

"""
        output = """EventAttach(event_id=1, timestamp=datetime.datetime(2014, 5, 23, 11, 0, 28, 584000), status=<Status.OK: ' '>, attachment_id=8, database='/home/employee.fdb', charset='ISO88591', protocol='TCPv4', address='192.168.1.5', user='SYSDBA', role='NONE', remote_process='/opt/firebird/bin/isql', remote_pid=8723)
EventTransactionStart(event_id=2, timestamp=datetime.datetime(2014, 5, 23, 11, 0, 28, 616000), status=<Status.OK: ' '>, attachment_id=8, transaction_id=1568, options=['READ_COMMITTED', 'REC_VERSION', 'WAIT', 'READ_WRITE'])
EventRollback(event_id=3, timestamp=datetime.datetime(2014, 5, 23, 11, 0, 29, 957000), status=<Status.OK: ' '>, attachment_id=8, transaction_id=1568, options=['READ_COMMITTED', 'REC_VERSION', 'WAIT', 'READ_WRITE'], run_time=None, reads=None, writes=None, fetches=None, marks=None)
"""
        self._check_events(trace_lines, output)
    def test_18_rollback_attachment_and_start(self):
        trace_lines = """2014-05-23T11:00:29.9570 (3720:0000000000EFD9E8) ROLLBACK_TRANSACTION
	/home/employee.fdb (ATT_8, SYSDBA:NONE, ISO88591, TCPv4:192.168.1.5)
	/opt/firebird/bin/isql:8723
		(TRA_1568, READ_COMMITTED | REC_VERSION | WAIT | READ_WRITE)
0 ms

"""
        output = """AttachmentInfo(attachment_id=8, database='/home/employee.fdb', charset='ISO88591', protocol='TCPv4', address='192.168.1.5', user='SYSDBA', role='NONE', remote_process='/opt/firebird/bin/isql', remote_pid=8723)
EventRollback(event_id=1, timestamp=datetime.datetime(2014, 5, 23, 11, 0, 29, 957000), status=<Status.OK: ' '>, attachment_id=8, transaction_id=1568, options=['READ_COMMITTED', 'REC_VERSION', 'WAIT', 'READ_WRITE'], run_time=0, reads=None, writes=None, fetches=None, marks=None)
"""
        self._check_events(trace_lines, output)
    def test_19_commit_retaining(self):
        trace_lines = """2014-05-23T11:00:28.5840 (3720:0000000000EFD9E8) ATTACH_DATABASE
	/home/employee.fdb (ATT_8, SYSDBA:NONE, ISO88591, TCPv4:192.168.1.5)
	/opt/firebird/bin/isql:8723

2014-05-23T11:00:28.6160 (3720:0000000000EFD9E8) START_TRANSACTION
	/home/employee.fdb (ATT_8, SYSDBA:NONE, ISO88591, TCPv4:192.168.1.5)
	/opt/firebird/bin/isql:8723
		(TRA_1568, READ_COMMITTED | REC_VERSION | WAIT | READ_WRITE)

2014-05-23T11:00:29.9570 (3720:0000000000EFD9E8) COMMIT_RETAINING
	/home/employee.fdb (ATT_8, SYSDBA:NONE, ISO88591, TCPv4:192.168.1.5)
	/opt/firebird/bin/isql:8723
		(TRA_1568, READ_COMMITTED | REC_VERSION | WAIT | READ_WRITE)
      0 ms, 1 read(s), 1 write(s), 1 fetch(es), 1 mark(s)

"""
        output = """EventAttach(event_id=1, timestamp=datetime.datetime(2014, 5, 23, 11, 0, 28, 584000), status=<Status.OK: ' '>, attachment_id=8, database='/home/employee.fdb', charset='ISO88591', protocol='TCPv4', address='192.168.1.5', user='SYSDBA', role='NONE', remote_process='/opt/firebird/bin/isql', remote_pid=8723)
EventTransactionStart(event_id=2, timestamp=datetime.datetime(2014, 5, 23, 11, 0, 28, 616000), status=<Status.OK: ' '>, attachment_id=8, transaction_id=1568, options=['READ_COMMITTED', 'REC_VERSION', 'WAIT', 'READ_WRITE'])
EventCommitRetaining(event_id=3, timestamp=datetime.datetime(2014, 5, 23, 11, 0, 29, 957000), status=<Status.OK: ' '>, attachment_id=8, transaction_id=1568, options=['READ_COMMITTED', 'REC_VERSION', 'WAIT', 'READ_WRITE'], new_transaction_id=None, run_time=0, reads=1, writes=1, fetches=1, marks=1)
"""
        self._check_events(trace_lines, output)
    def test_20_commit_retaining_no_performance(self):
        trace_lines = """2014-05-23T11:00:28.5840 (3720:0000000000EFD9E8) ATTACH_DATABASE
	/home/employee.fdb (ATT_8, SYSDBA:NONE, ISO88591, TCPv4:192.168.1.5)
	/opt/firebird/bin/isql:8723

2014-05-23T11:00:28.6160 (3720:0000000000EFD9E8) START_TRANSACTION
	/home/employee.fdb (ATT_8, SYSDBA:NONE, ISO88591, TCPv4:192.168.1.5)
	/opt/firebird/bin/isql:8723
		(TRA_1568, READ_COMMITTED | REC_VERSION | WAIT | READ_WRITE)

2014-05-23T11:00:29.9570 (3720:0000000000EFD9E8) COMMIT_RETAINING
	/home/employee.fdb (ATT_8, SYSDBA:NONE, ISO88591, TCPv4:192.168.1.5)
	/opt/firebird/bin/isql:8723
		(TRA_1568, READ_COMMITTED | REC_VERSION | WAIT | READ_WRITE)

"""
        output = """EventAttach(event_id=1, timestamp=datetime.datetime(2014, 5, 23, 11, 0, 28, 584000), status=<Status.OK: ' '>, attachment_id=8, database='/home/employee.fdb', charset='ISO88591', protocol='TCPv4', address='192.168.1.5', user='SYSDBA', role='NONE', remote_process='/opt/firebird/bin/isql', remote_pid=8723)
EventTransactionStart(event_id=2, timestamp=datetime.datetime(2014, 5, 23, 11, 0, 28, 616000), status=<Status.OK: ' '>, attachment_id=8, transaction_id=1568, options=['READ_COMMITTED', 'REC_VERSION', 'WAIT', 'READ_WRITE'])
EventCommitRetaining(event_id=3, timestamp=datetime.datetime(2014, 5, 23, 11, 0, 29, 957000), status=<Status.OK: ' '>, attachment_id=8, transaction_id=1568, options=['READ_COMMITTED', 'REC_VERSION', 'WAIT', 'READ_WRITE'], new_transaction_id=None, run_time=None, reads=None, writes=None, fetches=None, marks=None)
"""
        self._check_events(trace_lines, output)
    def test_21_commit_retaining_without_attachment_and_start(self):
        trace_lines = """2014-05-23T11:00:29.9570 (3720:0000000000EFD9E8) COMMIT_RETAINING
	/home/employee.fdb (ATT_8, SYSDBA:NONE, ISO88591, TCPv4:192.168.1.5)
	/opt/firebird/bin/isql:8723
		(TRA_1568, READ_COMMITTED | REC_VERSION | WAIT | READ_WRITE)
      0 ms, 1 read(s), 1 write(s), 1 fetch(es), 1 mark(s)

"""
        output = """AttachmentInfo(attachment_id=8, database='/home/employee.fdb', charset='ISO88591', protocol='TCPv4', address='192.168.1.5', user='SYSDBA', role='NONE', remote_process='/opt/firebird/bin/isql', remote_pid=8723)
EventCommitRetaining(event_id=1, timestamp=datetime.datetime(2014, 5, 23, 11, 0, 29, 957000), status=<Status.OK: ' '>, attachment_id=8, transaction_id=1568, options=['READ_COMMITTED', 'REC_VERSION', 'WAIT', 'READ_WRITE'], new_transaction_id=None, run_time=0, reads=1, writes=1, fetches=1, marks=1)
"""
        self._check_events(trace_lines, output)
    def test_22_rollback_retaining(self):
        trace_lines = """2014-05-23T11:00:28.5840 (3720:0000000000EFD9E8) ATTACH_DATABASE
	/home/employee.fdb (ATT_8, SYSDBA:NONE, ISO88591, TCPv4:192.168.1.5)
	/opt/firebird/bin/isql:8723

2014-05-23T11:00:28.6160 (3720:0000000000EFD9E8) START_TRANSACTION
	/home/employee.fdb (ATT_8, SYSDBA:NONE, ISO88591, TCPv4:192.168.1.5)
	/opt/firebird/bin/isql:8723
		(TRA_1568, READ_COMMITTED | REC_VERSION | WAIT | READ_WRITE)

2014-05-23T11:00:29.9570 (3720:0000000000EFD9E8) ROLLBACK_RETAINING
	/home/employee.fdb (ATT_8, SYSDBA:NONE, ISO88591, TCPv4:192.168.1.5)
	/opt/firebird/bin/isql:8723
		(TRA_1568, READ_COMMITTED | REC_VERSION | WAIT | READ_WRITE)
0 ms

"""
        output = """EventAttach(event_id=1, timestamp=datetime.datetime(2014, 5, 23, 11, 0, 28, 584000), status=<Status.OK: ' '>, attachment_id=8, database='/home/employee.fdb', charset='ISO88591', protocol='TCPv4', address='192.168.1.5', user='SYSDBA', role='NONE', remote_process='/opt/firebird/bin/isql', remote_pid=8723)
EventTransactionStart(event_id=2, timestamp=datetime.datetime(2014, 5, 23, 11, 0, 28, 616000), status=<Status.OK: ' '>, attachment_id=8, transaction_id=1568, options=['READ_COMMITTED', 'REC_VERSION', 'WAIT', 'READ_WRITE'])
EventRollbackRetaining(event_id=3, timestamp=datetime.datetime(2014, 5, 23, 11, 0, 29, 957000), status=<Status.OK: ' '>, attachment_id=8, transaction_id=1568, options=['READ_COMMITTED', 'REC_VERSION', 'WAIT', 'READ_WRITE'], new_transaction_id=None, run_time=0, reads=None, writes=None, fetches=None, marks=None)
"""
        self._check_events(trace_lines, output)
    def test_23_rollback_retaining_no_performance(self):
        trace_lines = """2014-05-23T11:00:28.5840 (3720:0000000000EFD9E8) ATTACH_DATABASE
	/home/employee.fdb (ATT_8, SYSDBA:NONE, ISO88591, TCPv4:192.168.1.5)
	/opt/firebird/bin/isql:8723

2014-05-23T11:00:28.6160 (3720:0000000000EFD9E8) START_TRANSACTION
	/home/employee.fdb (ATT_8, SYSDBA:NONE, ISO88591, TCPv4:192.168.1.5)
	/opt/firebird/bin/isql:8723
		(TRA_1568, READ_COMMITTED | REC_VERSION | WAIT | READ_WRITE)

2014-05-23T11:00:29.9570 (3720:0000000000EFD9E8) ROLLBACK_RETAINING
	/home/employee.fdb (ATT_8, SYSDBA:NONE, ISO88591, TCPv4:192.168.1.5)
	/opt/firebird/bin/isql:8723
		(TRA_1568, READ_COMMITTED | REC_VERSION | WAIT | READ_WRITE)

"""
        output = """EventAttach(event_id=1, timestamp=datetime.datetime(2014, 5, 23, 11, 0, 28, 584000), status=<Status.OK: ' '>, attachment_id=8, database='/home/employee.fdb', charset='ISO88591', protocol='TCPv4', address='192.168.1.5', user='SYSDBA', role='NONE', remote_process='/opt/firebird/bin/isql', remote_pid=8723)
EventTransactionStart(event_id=2, timestamp=datetime.datetime(2014, 5, 23, 11, 0, 28, 616000), status=<Status.OK: ' '>, attachment_id=8, transaction_id=1568, options=['READ_COMMITTED', 'REC_VERSION', 'WAIT', 'READ_WRITE'])
EventRollbackRetaining(event_id=3, timestamp=datetime.datetime(2014, 5, 23, 11, 0, 29, 957000), status=<Status.OK: ' '>, attachment_id=8, transaction_id=1568, options=['READ_COMMITTED', 'REC_VERSION', 'WAIT', 'READ_WRITE'], new_transaction_id=None, run_time=None, reads=None, writes=None, fetches=None, marks=None)
"""
        self._check_events(trace_lines, output)
    def test_24_rollback_retaining_without_attachment_and_start(self):
        trace_lines = """2014-05-23T11:00:29.9570 (3720:0000000000EFD9E8) ROLLBACK_RETAINING
	/home/employee.fdb (ATT_8, SYSDBA:NONE, ISO88591, TCPv4:192.168.1.5)
	/opt/firebird/bin/isql:8723
		(TRA_1568, READ_COMMITTED | REC_VERSION | WAIT | READ_WRITE)
0 ms

"""
        output = """AttachmentInfo(attachment_id=8, database='/home/employee.fdb', charset='ISO88591', protocol='TCPv4', address='192.168.1.5', user='SYSDBA', role='NONE', remote_process='/opt/firebird/bin/isql', remote_pid=8723)
EventRollbackRetaining(event_id=1, timestamp=datetime.datetime(2014, 5, 23, 11, 0, 29, 957000), status=<Status.OK: ' '>, attachment_id=8, transaction_id=1568, options=['READ_COMMITTED', 'REC_VERSION', 'WAIT', 'READ_WRITE'], new_transaction_id=None, run_time=0, reads=None, writes=None, fetches=None, marks=None)
"""
        self._check_events(trace_lines, output)
    def test_25_prepare_statement(self):
        trace_lines = """2014-05-23T11:00:28.5840 (3720:0000000000EFD9E8) ATTACH_DATABASE
	/home/employee.fdb (ATT_8, SYSDBA:NONE, ISO88591, TCPv4:192.168.1.5)
	/opt/firebird/bin/isql:8723

2014-05-23T11:00:28.6160 (3720:0000000000EFD9E8) START_TRANSACTION
	/home/employee.fdb (ATT_8, SYSDBA:NONE, ISO88591, TCPv4:192.168.1.5)
	/opt/firebird/bin/isql:8723
		(TRA_1570, READ_COMMITTED | REC_VERSION | WAIT | READ_WRITE)

2014-05-23T11:00:45.5260 (3720:0000000000EFD9E8) PREPARE_STATEMENT
	/home/employee.fdb (ATT_8, SYSDBA:NONE, ISO88591, TCPv4:192.168.1.5)
	/opt/firebird/bin/isql:8723
		(TRA_1570, READ_COMMITTED | REC_VERSION | WAIT | READ_WRITE)

Statement 181:
-------------------------------------------------------------------------------
SELECT GEN_ID(GEN_NUM, 1) FROM RDB$DATABASE
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
PLAN (RDB$DATABASE NATURAL)
     13 ms

"""
        output = """EventAttach(event_id=1, timestamp=datetime.datetime(2014, 5, 23, 11, 0, 28, 584000), status=<Status.OK: ' '>, attachment_id=8, database='/home/employee.fdb', charset='ISO88591', protocol='TCPv4', address='192.168.1.5', user='SYSDBA', role='NONE', remote_process='/opt/firebird/bin/isql', remote_pid=8723)
EventTransactionStart(event_id=2, timestamp=datetime.datetime(2014, 5, 23, 11, 0, 28, 616000), status=<Status.OK: ' '>, attachment_id=8, transaction_id=1570, options=['READ_COMMITTED', 'REC_VERSION', 'WAIT', 'READ_WRITE'])
SQLInfo(sql_id=1, sql='SELECT GEN_ID(GEN_NUM, 1) FROM RDB$DATABASE', plan='PLAN (RDB$DATABASE NATURAL)')
EventPrepareStatement(event_id=3, timestamp=datetime.datetime(2014, 5, 23, 11, 0, 45, 526000), status=<Status.OK: ' '>, attachment_id=8, transaction_id=1570, statement_id=181, sql_id=1, prepare_time=13)
"""
        self._check_events(trace_lines, output)
    def test_26_prepare_statement_no_plan(self):
        trace_lines = """2014-05-23T11:00:28.5840 (3720:0000000000EFD9E8) ATTACH_DATABASE
	/home/employee.fdb (ATT_8, SYSDBA:NONE, ISO88591, TCPv4:192.168.1.5)
	/opt/firebird/bin/isql:8723

2014-05-23T11:00:28.6160 (3720:0000000000EFD9E8) START_TRANSACTION
	/home/employee.fdb (ATT_8, SYSDBA:NONE, ISO88591, TCPv4:192.168.1.5)
	/opt/firebird/bin/isql:8723
		(TRA_1570, READ_COMMITTED | REC_VERSION | WAIT | READ_WRITE)

2014-05-23T11:00:45.5260 (3720:0000000000EFD9E8) PREPARE_STATEMENT
	/home/employee.fdb (ATT_8, SYSDBA:NONE, ISO88591, TCPv4:192.168.1.5)
	/opt/firebird/bin/isql:8723
		(TRA_1570, READ_COMMITTED | REC_VERSION | WAIT | READ_WRITE)

Statement 181:
-------------------------------------------------------------------------------
SELECT GEN_ID(GEN_NUM, 1) FROM RDB$DATABASE
     13 ms

"""
        output = """EventAttach(event_id=1, timestamp=datetime.datetime(2014, 5, 23, 11, 0, 28, 584000), status=<Status.OK: ' '>, attachment_id=8, database='/home/employee.fdb', charset='ISO88591', protocol='TCPv4', address='192.168.1.5', user='SYSDBA', role='NONE', remote_process='/opt/firebird/bin/isql', remote_pid=8723)
EventTransactionStart(event_id=2, timestamp=datetime.datetime(2014, 5, 23, 11, 0, 28, 616000), status=<Status.OK: ' '>, attachment_id=8, transaction_id=1570, options=['READ_COMMITTED', 'REC_VERSION', 'WAIT', 'READ_WRITE'])
SQLInfo(sql_id=1, sql='SELECT GEN_ID(GEN_NUM, 1) FROM RDB$DATABASE', plan=None)
EventPrepareStatement(event_id=3, timestamp=datetime.datetime(2014, 5, 23, 11, 0, 45, 526000), status=<Status.OK: ' '>, attachment_id=8, transaction_id=1570, statement_id=181, sql_id=1, prepare_time=13)
"""
        self._check_events(trace_lines, output)
    def test_27_prepare_statement_no_attachment(self):
        trace_lines = """2014-05-23T11:00:28.6160 (3720:0000000000EFD9E8) START_TRANSACTION
	/home/employee.fdb (ATT_8, SYSDBA:NONE, ISO88591, TCPv4:192.168.1.5)
	/opt/firebird/bin/isql:8723
		(TRA_1570, READ_COMMITTED | REC_VERSION | WAIT | READ_WRITE)

2014-05-23T11:00:45.5260 (3720:0000000000EFD9E8) PREPARE_STATEMENT
	/home/employee.fdb (ATT_8, SYSDBA:NONE, ISO88591, TCPv4:192.168.1.5)
	/opt/firebird/bin/isql:8723
		(TRA_1570, READ_COMMITTED | REC_VERSION | WAIT | READ_WRITE)

Statement 181:
-------------------------------------------------------------------------------
SELECT GEN_ID(GEN_NUM, 1) FROM RDB$DATABASE
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
PLAN (RDB$DATABASE NATURAL)
     13 ms

"""
        output = """AttachmentInfo(attachment_id=8, database='/home/employee.fdb', charset='ISO88591', protocol='TCPv4', address='192.168.1.5', user='SYSDBA', role='NONE', remote_process='/opt/firebird/bin/isql', remote_pid=8723)
EventTransactionStart(event_id=1, timestamp=datetime.datetime(2014, 5, 23, 11, 0, 28, 616000), status=<Status.OK: ' '>, attachment_id=8, transaction_id=1570, options=['READ_COMMITTED', 'REC_VERSION', 'WAIT', 'READ_WRITE'])
SQLInfo(sql_id=1, sql='SELECT GEN_ID(GEN_NUM, 1) FROM RDB$DATABASE', plan='PLAN (RDB$DATABASE NATURAL)')
EventPrepareStatement(event_id=2, timestamp=datetime.datetime(2014, 5, 23, 11, 0, 45, 526000), status=<Status.OK: ' '>, attachment_id=8, transaction_id=1570, statement_id=181, sql_id=1, prepare_time=13)
"""
        self._check_events(trace_lines, output)
    def test_28_prepare_statement_no_transaction(self):
        trace_lines = """2014-05-23T11:00:28.5840 (3720:0000000000EFD9E8) ATTACH_DATABASE
	/home/employee.fdb (ATT_8, SYSDBA:NONE, ISO88591, TCPv4:192.168.1.5)
	/opt/firebird/bin/isql:8723

2014-05-23T11:00:45.5260 (3720:0000000000EFD9E8) PREPARE_STATEMENT
	/home/employee.fdb (ATT_8, SYSDBA:NONE, ISO88591, TCPv4:192.168.1.5)
	/opt/firebird/bin/isql:8723
		(TRA_1570, READ_COMMITTED | REC_VERSION | WAIT | READ_WRITE)

Statement 181:
-------------------------------------------------------------------------------
SELECT GEN_ID(GEN_NUM, 1) FROM RDB$DATABASE
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
PLAN (RDB$DATABASE NATURAL)
     13 ms

"""
        output = """EventAttach(event_id=1, timestamp=datetime.datetime(2014, 5, 23, 11, 0, 28, 584000), status=<Status.OK: ' '>, attachment_id=8, database='/home/employee.fdb', charset='ISO88591', protocol='TCPv4', address='192.168.1.5', user='SYSDBA', role='NONE', remote_process='/opt/firebird/bin/isql', remote_pid=8723)
TransactionInfo(attachment_id=8, transaction_id=1570, initial_id=None, options=['READ_COMMITTED', 'REC_VERSION', 'WAIT', 'READ_WRITE'])
SQLInfo(sql_id=1, sql='SELECT GEN_ID(GEN_NUM, 1) FROM RDB$DATABASE', plan='PLAN (RDB$DATABASE NATURAL)')
EventPrepareStatement(event_id=2, timestamp=datetime.datetime(2014, 5, 23, 11, 0, 45, 526000), status=<Status.OK: ' '>, attachment_id=8, transaction_id=1570, statement_id=181, sql_id=1, prepare_time=13)
"""
        self._check_events(trace_lines, output)
    def test_29_prepare_statement_no_attachment_no_transaction(self):
        trace_lines = """2014-05-23T11:00:45.5260 (3720:0000000000EFD9E8) PREPARE_STATEMENT
	/home/employee.fdb (ATT_8, SYSDBA:NONE, ISO88591, TCPv4:192.168.1.5)
	/opt/firebird/bin/isql:8723
		(TRA_1570, READ_COMMITTED | REC_VERSION | WAIT | READ_WRITE)

Statement 181:
-------------------------------------------------------------------------------
SELECT GEN_ID(GEN_NUM, 1) FROM RDB$DATABASE
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
PLAN (RDB$DATABASE NATURAL)
     13 ms

"""
        output = """AttachmentInfo(attachment_id=8, database='/home/employee.fdb', charset='ISO88591', protocol='TCPv4', address='192.168.1.5', user='SYSDBA', role='NONE', remote_process='/opt/firebird/bin/isql', remote_pid=8723)
TransactionInfo(attachment_id=8, transaction_id=1570, initial_id=None, options=['READ_COMMITTED', 'REC_VERSION', 'WAIT', 'READ_WRITE'])
SQLInfo(sql_id=1, sql='SELECT GEN_ID(GEN_NUM, 1) FROM RDB$DATABASE', plan='PLAN (RDB$DATABASE NATURAL)')
EventPrepareStatement(event_id=1, timestamp=datetime.datetime(2014, 5, 23, 11, 0, 45, 526000), status=<Status.OK: ' '>, attachment_id=8, transaction_id=1570, statement_id=181, sql_id=1, prepare_time=13)
"""
        self._check_events(trace_lines, output)
    def test_30_statement_start(self):
        trace_lines = """2014-05-23T11:00:28.5840 (3720:0000000000EFD9E8) ATTACH_DATABASE
	/home/employee.fdb (ATT_8, SYSDBA:NONE, ISO88591, TCPv4:192.168.1.5)
	/opt/firebird/bin/isql:8723

2014-05-23T11:00:28.6160 (3720:0000000000EFD9E8) START_TRANSACTION
	/home/employee.fdb (ATT_8, SYSDBA:NONE, ISO88591, TCPv4:192.168.1.5)
	/opt/firebird/bin/isql:8723
		(TRA_1570, READ_COMMITTED | REC_VERSION | WAIT | READ_WRITE)

2014-05-23T11:00:45.5260 (3720:0000000000EFD9E8) EXECUTE_STATEMENT_START
	/home/employee.fdb (ATT_8, SYSDBA:NONE, ISO88591, TCPv4:192.168.1.5)
	/opt/firebird/bin/isql:8723
		(TRA_1570, READ_COMMITTED | REC_VERSION | WAIT | READ_WRITE)

Statement 166353:
-------------------------------------------------------------------------------
UPDATE TABLE_A SET VAL_1=?, VAL_2=?, VAL_3=?, VAL_4=? WHERE ID_EX=?

^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
PLAN (TABLE_A INDEX (TABLE_A_PK))

param0 = timestamp, "2017-11-09T11:23:52.1570"
param1 = integer, "100012829"
param2 = integer, "<NULL>"
param3 = varchar(20), "2810090906551"
param4 = integer, "4199300"
"""
        output = """EventAttach(event_id=1, timestamp=datetime.datetime(2014, 5, 23, 11, 0, 28, 584000), status=<Status.OK: ' '>, attachment_id=8, database='/home/employee.fdb', charset='ISO88591', protocol='TCPv4', address='192.168.1.5', user='SYSDBA', role='NONE', remote_process='/opt/firebird/bin/isql', remote_pid=8723)
EventTransactionStart(event_id=2, timestamp=datetime.datetime(2014, 5, 23, 11, 0, 28, 616000), status=<Status.OK: ' '>, attachment_id=8, transaction_id=1570, options=['READ_COMMITTED', 'REC_VERSION', 'WAIT', 'READ_WRITE'])
ParamSet(par_id=1, params=[('timestamp', datetime.datetime(2017, 11, 9, 11, 23, 52, 157000)), ('integer', 100012829), ('integer', None), ('varchar(20)', '2810090906551'), ('integer', 4199300)])
SQLInfo(sql_id=1, sql='UPDATE TABLE_A SET VAL_1=?, VAL_2=?, VAL_3=?, VAL_4=? WHERE ID_EX=?', plan='PLAN (TABLE_A INDEX (TABLE_A_PK))')
EventStatementStart(event_id=3, timestamp=datetime.datetime(2014, 5, 23, 11, 0, 45, 526000), status=<Status.OK: ' '>, attachment_id=8, transaction_id=1570, statement_id=166353, sql_id=1, param_id=1)
"""
        self._check_events(trace_lines, output)
    def test_31_statement_start_no_plan(self):
        trace_lines = """2014-05-23T11:00:28.5840 (3720:0000000000EFD9E8) ATTACH_DATABASE
	/home/employee.fdb (ATT_8, SYSDBA:NONE, ISO88591, TCPv4:192.168.1.5)
	/opt/firebird/bin/isql:8723

2014-05-23T11:00:28.6160 (3720:0000000000EFD9E8) START_TRANSACTION
	/home/employee.fdb (ATT_8, SYSDBA:NONE, ISO88591, TCPv4:192.168.1.5)
	/opt/firebird/bin/isql:8723
		(TRA_1570, READ_COMMITTED | REC_VERSION | WAIT | READ_WRITE)

2014-05-23T11:00:45.5260 (3720:0000000000EFD9E8) EXECUTE_STATEMENT_START
	/home/employee.fdb (ATT_8, SYSDBA:NONE, ISO88591, TCPv4:192.168.1.5)
	/opt/firebird/bin/isql:8723
		(TRA_1570, READ_COMMITTED | REC_VERSION | WAIT | READ_WRITE)

Statement 166353:
-------------------------------------------------------------------------------
UPDATE TABLE_A SET VAL_1=?, VAL_2=?, VAL_3=?, VAL_4=? WHERE ID_EX=?
param0 = timestamp, "2017-11-09T11:23:52.1570"
param1 = integer, "100012829"
param2 = integer, "<NULL>"
param3 = varchar(20), "2810090906551"
param4 = integer, "4199300"
"""
        output = """EventAttach(event_id=1, timestamp=datetime.datetime(2014, 5, 23, 11, 0, 28, 584000), status=<Status.OK: ' '>, attachment_id=8, database='/home/employee.fdb', charset='ISO88591', protocol='TCPv4', address='192.168.1.5', user='SYSDBA', role='NONE', remote_process='/opt/firebird/bin/isql', remote_pid=8723)
EventTransactionStart(event_id=2, timestamp=datetime.datetime(2014, 5, 23, 11, 0, 28, 616000), status=<Status.OK: ' '>, attachment_id=8, transaction_id=1570, options=['READ_COMMITTED', 'REC_VERSION', 'WAIT', 'READ_WRITE'])
ParamSet(par_id=1, params=[('timestamp', datetime.datetime(2017, 11, 9, 11, 23, 52, 157000)), ('integer', 100012829), ('integer', None), ('varchar(20)', '2810090906551'), ('integer', 4199300)])
SQLInfo(sql_id=1, sql='UPDATE TABLE_A SET VAL_1=?, VAL_2=?, VAL_3=?, VAL_4=? WHERE ID_EX=?', plan=None)
EventStatementStart(event_id=3, timestamp=datetime.datetime(2014, 5, 23, 11, 0, 45, 526000), status=<Status.OK: ' '>, attachment_id=8, transaction_id=1570, statement_id=166353, sql_id=1, param_id=1)
"""
        self._check_events(trace_lines, output)
    def test_32_statement_start_no_attachment(self):
        trace_lines = """2014-05-23T11:00:28.6160 (3720:0000000000EFD9E8) START_TRANSACTION
	/home/employee.fdb (ATT_8, SYSDBA:NONE, ISO88591, TCPv4:192.168.1.5)
	/opt/firebird/bin/isql:8723
		(TRA_1570, READ_COMMITTED | REC_VERSION | WAIT | READ_WRITE)

2014-05-23T11:00:45.5260 (3720:0000000000EFD9E8) EXECUTE_STATEMENT_START
	/home/employee.fdb (ATT_8, SYSDBA:NONE, ISO88591, TCPv4:192.168.1.5)
	/opt/firebird/bin/isql:8723
		(TRA_1570, READ_COMMITTED | REC_VERSION | WAIT | READ_WRITE)

Statement 166353:
-------------------------------------------------------------------------------
UPDATE TABLE_A SET VAL_1=?, VAL_2=?, VAL_3=?, VAL_4=? WHERE ID_EX=?

^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
PLAN (TABLE_A INDEX (TABLE_A_PK))

param0 = timestamp, "2017-11-09T11:23:52.1570"
param1 = integer, "100012829"
param2 = integer, "<NULL>"
param3 = varchar(20), "2810090906551"
param4 = integer, "4199300"
"""
        output = """AttachmentInfo(attachment_id=8, database='/home/employee.fdb', charset='ISO88591', protocol='TCPv4', address='192.168.1.5', user='SYSDBA', role='NONE', remote_process='/opt/firebird/bin/isql', remote_pid=8723)
EventTransactionStart(event_id=1, timestamp=datetime.datetime(2014, 5, 23, 11, 0, 28, 616000), status=<Status.OK: ' '>, attachment_id=8, transaction_id=1570, options=['READ_COMMITTED', 'REC_VERSION', 'WAIT', 'READ_WRITE'])
ParamSet(par_id=1, params=[('timestamp', datetime.datetime(2017, 11, 9, 11, 23, 52, 157000)), ('integer', 100012829), ('integer', None), ('varchar(20)', '2810090906551'), ('integer', 4199300)])
SQLInfo(sql_id=1, sql='UPDATE TABLE_A SET VAL_1=?, VAL_2=?, VAL_3=?, VAL_4=? WHERE ID_EX=?', plan='PLAN (TABLE_A INDEX (TABLE_A_PK))')
EventStatementStart(event_id=2, timestamp=datetime.datetime(2014, 5, 23, 11, 0, 45, 526000), status=<Status.OK: ' '>, attachment_id=8, transaction_id=1570, statement_id=166353, sql_id=1, param_id=1)
"""
        self._check_events(trace_lines, output)
    def test_33_statement_start_no_transaction(self):
        trace_lines = """2014-05-23T11:00:28.5840 (3720:0000000000EFD9E8) ATTACH_DATABASE
	/home/employee.fdb (ATT_8, SYSDBA:NONE, ISO88591, TCPv4:192.168.1.5)
	/opt/firebird/bin/isql:8723

2014-05-23T11:00:45.5260 (3720:0000000000EFD9E8) EXECUTE_STATEMENT_START
	/home/employee.fdb (ATT_8, SYSDBA:NONE, ISO88591, TCPv4:192.168.1.5)
	/opt/firebird/bin/isql:8723
		(TRA_1570, READ_COMMITTED | REC_VERSION | WAIT | READ_WRITE)

Statement 166353:
-------------------------------------------------------------------------------
UPDATE TABLE_A SET VAL_1=?, VAL_2=?, VAL_3=?, VAL_4=? WHERE ID_EX=?

^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
PLAN (TABLE_A INDEX (TABLE_A_PK))

param0 = timestamp, "2017-11-09T11:23:52.1570"
param1 = integer, "100012829"
param2 = integer, "<NULL>"
param3 = varchar(20), "2810090906551"
param4 = integer, "4199300"
"""
        output = """EventAttach(event_id=1, timestamp=datetime.datetime(2014, 5, 23, 11, 0, 28, 584000), status=<Status.OK: ' '>, attachment_id=8, database='/home/employee.fdb', charset='ISO88591', protocol='TCPv4', address='192.168.1.5', user='SYSDBA', role='NONE', remote_process='/opt/firebird/bin/isql', remote_pid=8723)
TransactionInfo(attachment_id=8, transaction_id=1570, initial_id=None, options=['READ_COMMITTED', 'REC_VERSION', 'WAIT', 'READ_WRITE'])
ParamSet(par_id=1, params=[('timestamp', datetime.datetime(2017, 11, 9, 11, 23, 52, 157000)), ('integer', 100012829), ('integer', None), ('varchar(20)', '2810090906551'), ('integer', 4199300)])
SQLInfo(sql_id=1, sql='UPDATE TABLE_A SET VAL_1=?, VAL_2=?, VAL_3=?, VAL_4=? WHERE ID_EX=?', plan='PLAN (TABLE_A INDEX (TABLE_A_PK))')
EventStatementStart(event_id=2, timestamp=datetime.datetime(2014, 5, 23, 11, 0, 45, 526000), status=<Status.OK: ' '>, attachment_id=8, transaction_id=1570, statement_id=166353, sql_id=1, param_id=1)
"""
        self._check_events(trace_lines, output)
    def test_34_statement_start_no_attachment_no_transaction(self):
        trace_lines = """2014-05-23T11:00:45.5260 (3720:0000000000EFD9E8) EXECUTE_STATEMENT_START
	/home/employee.fdb (ATT_8, SYSDBA:NONE, ISO88591, TCPv4:192.168.1.5)
	/opt/firebird/bin/isql:8723
		(TRA_1570, READ_COMMITTED | REC_VERSION | WAIT | READ_WRITE)

Statement 166353:
-------------------------------------------------------------------------------
UPDATE TABLE_A SET VAL_1=?, VAL_2=?, VAL_3=?, VAL_4=? WHERE ID_EX=?

^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
PLAN (TABLE_A INDEX (TABLE_A_PK))

param0 = timestamp, "2017-11-09T11:23:52.1570"
param1 = integer, "100012829"
param2 = integer, "<NULL>"
param3 = varchar(20), "2810090906551"
param4 = integer, "4199300"
"""
        output = """AttachmentInfo(attachment_id=8, database='/home/employee.fdb', charset='ISO88591', protocol='TCPv4', address='192.168.1.5', user='SYSDBA', role='NONE', remote_process='/opt/firebird/bin/isql', remote_pid=8723)
TransactionInfo(attachment_id=8, transaction_id=1570, initial_id=None, options=['READ_COMMITTED', 'REC_VERSION', 'WAIT', 'READ_WRITE'])
ParamSet(par_id=1, params=[('timestamp', datetime.datetime(2017, 11, 9, 11, 23, 52, 157000)), ('integer', 100012829), ('integer', None), ('varchar(20)', '2810090906551'), ('integer', 4199300)])
SQLInfo(sql_id=1, sql='UPDATE TABLE_A SET VAL_1=?, VAL_2=?, VAL_3=?, VAL_4=? WHERE ID_EX=?', plan='PLAN (TABLE_A INDEX (TABLE_A_PK))')
EventStatementStart(event_id=1, timestamp=datetime.datetime(2014, 5, 23, 11, 0, 45, 526000), status=<Status.OK: ' '>, attachment_id=8, transaction_id=1570, statement_id=166353, sql_id=1, param_id=1)
"""
        self._check_events(trace_lines, output)
    def test_35_statement_finish(self):
        trace_lines = """2014-05-23T11:00:28.5840 (3720:0000000000EFD9E8) ATTACH_DATABASE
	/home/employee.fdb (ATT_8, SYSDBA:NONE, ISO88591, TCPv4:192.168.1.5)
	/opt/firebird/bin/isql:8723

2014-05-23T11:00:28.6160 (3720:0000000000EFD9E8) START_TRANSACTION
	/home/employee.fdb (ATT_8, SYSDBA:NONE, ISO88591, TCPv4:192.168.1.5)
	/opt/firebird/bin/isql:8723
		(TRA_1570, READ_COMMITTED | REC_VERSION | WAIT | READ_WRITE)

2014-05-23T11:00:45.5420 (3720:0000000000EFD9E8) EXECUTE_STATEMENT_FINISH
	/home/employee.fdb (ATT_8, SYSDBA:NONE, ISO88591, TCPv4:192.168.1.5)
	/opt/firebird/bin/isql:8723
		(TRA_1570, READ_COMMITTED | REC_VERSION | WAIT | READ_WRITE)

Statement 181:
-------------------------------------------------------------------------------
SELECT GEN_ID(GEN_NUM, 1) NUMS FROM RDB$DATABASE
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
PLAN (RDB$DATABASE NATURAL)
1 records fetched
      0 ms, 2 read(s), 14 fetch(es), 1 mark(s)

Table                             Natural     Index    Update    Insert    Delete   Backout     Purge   Expunge
***************************************************************************************************************
RDB$DATABASE                            1
RDB$CHARACTER_SETS                                1
RDB$COLLATIONS                                    1
"""
        output = """EventAttach(event_id=1, timestamp=datetime.datetime(2014, 5, 23, 11, 0, 28, 584000), status=<Status.OK: ' '>, attachment_id=8, database='/home/employee.fdb', charset='ISO88591', protocol='TCPv4', address='192.168.1.5', user='SYSDBA', role='NONE', remote_process='/opt/firebird/bin/isql', remote_pid=8723)
EventTransactionStart(event_id=2, timestamp=datetime.datetime(2014, 5, 23, 11, 0, 28, 616000), status=<Status.OK: ' '>, attachment_id=8, transaction_id=1570, options=['READ_COMMITTED', 'REC_VERSION', 'WAIT', 'READ_WRITE'])
SQLInfo(sql_id=1, sql='SELECT GEN_ID(GEN_NUM, 1) NUMS FROM RDB$DATABASE', plan='PLAN (RDB$DATABASE NATURAL)')
EventStatementFinish(event_id=3, timestamp=datetime.datetime(2014, 5, 23, 11, 0, 45, 542000), status=<Status.OK: ' '>, attachment_id=8, transaction_id=1570, statement_id=181, sql_id=1, param_id=None, records=1, run_time=0, reads=2, writes=None, fetches=14, marks=1, access=[AccessStats(table='RDB$DATABASE', natural=1, index=0, update=0, insert=0, delete=0, backout=0, purge=0, expunge=0), AccessStats(table='RDB$CHARACTER_SETS', natural=0, index=1, update=0, insert=0, delete=0, backout=0, purge=0, expunge=0), AccessStats(table='RDB$COLLATIONS', natural=0, index=1, update=0, insert=0, delete=0, backout=0, purge=0, expunge=0)])
"""
        self._check_events(trace_lines, output)
    def test_36_statement_finish_no_plan(self):
        trace_lines = """2014-05-23T11:00:28.5840 (3720:0000000000EFD9E8) ATTACH_DATABASE
	/home/employee.fdb (ATT_8, SYSDBA:NONE, ISO88591, TCPv4:192.168.1.5)
	/opt/firebird/bin/isql:8723

2014-05-23T11:00:28.6160 (3720:0000000000EFD9E8) START_TRANSACTION
	/home/employee.fdb (ATT_8, SYSDBA:NONE, ISO88591, TCPv4:192.168.1.5)
	/opt/firebird/bin/isql:8723
		(TRA_1570, READ_COMMITTED | REC_VERSION | WAIT | READ_WRITE)

2014-05-23T11:00:45.5420 (3720:0000000000EFD9E8) EXECUTE_STATEMENT_FINISH
	/home/employee.fdb (ATT_8, EUROFLOW:NONE, ISO88591, TCPv4:192.168.1.5)
	/opt/firebird/bin/isql:8723
		(TRA_1570, READ_COMMITTED | REC_VERSION | WAIT | READ_WRITE)

Statement 181:
-------------------------------------------------------------------------------
SELECT GEN_ID(GEN_NUM, 1) NUMS FROM RDB$DATABASE
1 records fetched
      0 ms, 2 read(s), 14 fetch(es), 1 mark(s)

Table                             Natural     Index    Update    Insert    Delete   Backout     Purge   Expunge
***************************************************************************************************************
RDB$DATABASE                            1
RDB$CHARACTER_SETS                                1
RDB$COLLATIONS                                    1
"""
        output = """EventAttach(event_id=1, timestamp=datetime.datetime(2014, 5, 23, 11, 0, 28, 584000), status=<Status.OK: ' '>, attachment_id=8, database='/home/employee.fdb', charset='ISO88591', protocol='TCPv4', address='192.168.1.5', user='SYSDBA', role='NONE', remote_process='/opt/firebird/bin/isql', remote_pid=8723)
EventTransactionStart(event_id=2, timestamp=datetime.datetime(2014, 5, 23, 11, 0, 28, 616000), status=<Status.OK: ' '>, attachment_id=8, transaction_id=1570, options=['READ_COMMITTED', 'REC_VERSION', 'WAIT', 'READ_WRITE'])
SQLInfo(sql_id=1, sql='SELECT GEN_ID(GEN_NUM, 1) NUMS FROM RDB$DATABASE', plan=None)
EventStatementFinish(event_id=3, timestamp=datetime.datetime(2014, 5, 23, 11, 0, 45, 542000), status=<Status.OK: ' '>, attachment_id=8, transaction_id=1570, statement_id=181, sql_id=1, param_id=None, records=1, run_time=0, reads=2, writes=None, fetches=14, marks=1, access=[AccessStats(table='RDB$DATABASE', natural=1, index=0, update=0, insert=0, delete=0, backout=0, purge=0, expunge=0), AccessStats(table='RDB$CHARACTER_SETS', natural=0, index=1, update=0, insert=0, delete=0, backout=0, purge=0, expunge=0), AccessStats(table='RDB$COLLATIONS', natural=0, index=1, update=0, insert=0, delete=0, backout=0, purge=0, expunge=0)])
"""
        self._check_events(trace_lines, output)
    def test_37_statement_finish_no_attachment(self):
        trace_lines = """2014-05-23T11:00:28.6160 (3720:0000000000EFD9E8) START_TRANSACTION
	/home/employee.fdb (ATT_8, SYSDBA:NONE, ISO88591, TCPv4:192.168.1.5)
	/opt/firebird/bin/isql:8723
		(TRA_1570, READ_COMMITTED | REC_VERSION | WAIT | READ_WRITE)

2014-05-23T11:00:45.5420 (3720:0000000000EFD9E8) EXECUTE_STATEMENT_FINISH
	/home/employee.fdb (ATT_8, SYSDBA:NONE, ISO88591, TCPv4:192.168.1.5)
	/opt/firebird/bin/isql:8723
		(TRA_1570, READ_COMMITTED | REC_VERSION | WAIT | READ_WRITE)

Statement 181:
-------------------------------------------------------------------------------
SELECT GEN_ID(GEN_NUM, 1) NUMS FROM RDB$DATABASE
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
PLAN (RDB$DATABASE NATURAL)
1 records fetched
      0 ms, 2 read(s), 14 fetch(es), 1 mark(s)

Table                             Natural     Index    Update    Insert    Delete   Backout     Purge   Expunge
***************************************************************************************************************
RDB$DATABASE                            1
RDB$CHARACTER_SETS                                1
RDB$COLLATIONS                                    1
"""
        output = """AttachmentInfo(attachment_id=8, database='/home/employee.fdb', charset='ISO88591', protocol='TCPv4', address='192.168.1.5', user='SYSDBA', role='NONE', remote_process='/opt/firebird/bin/isql', remote_pid=8723)
EventTransactionStart(event_id=1, timestamp=datetime.datetime(2014, 5, 23, 11, 0, 28, 616000), status=<Status.OK: ' '>, attachment_id=8, transaction_id=1570, options=['READ_COMMITTED', 'REC_VERSION', 'WAIT', 'READ_WRITE'])
SQLInfo(sql_id=1, sql='SELECT GEN_ID(GEN_NUM, 1) NUMS FROM RDB$DATABASE', plan='PLAN (RDB$DATABASE NATURAL)')
EventStatementFinish(event_id=2, timestamp=datetime.datetime(2014, 5, 23, 11, 0, 45, 542000), status=<Status.OK: ' '>, attachment_id=8, transaction_id=1570, statement_id=181, sql_id=1, param_id=None, records=1, run_time=0, reads=2, writes=None, fetches=14, marks=1, access=[AccessStats(table='RDB$DATABASE', natural=1, index=0, update=0, insert=0, delete=0, backout=0, purge=0, expunge=0), AccessStats(table='RDB$CHARACTER_SETS', natural=0, index=1, update=0, insert=0, delete=0, backout=0, purge=0, expunge=0), AccessStats(table='RDB$COLLATIONS', natural=0, index=1, update=0, insert=0, delete=0, backout=0, purge=0, expunge=0)])
"""
        self._check_events(trace_lines, output)
    def test_38_statement_finish_no_transaction(self):
        trace_lines = """2014-05-23T11:00:28.5840 (3720:0000000000EFD9E8) ATTACH_DATABASE
	/home/employee.fdb (ATT_8, SYSDBA:NONE, ISO88591, TCPv4:192.168.1.5)
	/opt/firebird/bin/isql:8723

2014-05-23T11:00:45.5420 (3720:0000000000EFD9E8) EXECUTE_STATEMENT_FINISH
	/home/employee.fdb (ATT_8, SYSDBA:NONE, ISO88591, TCPv4:192.168.1.5)
	/opt/firebird/bin/isql:8723
		(TRA_1570, READ_COMMITTED | REC_VERSION | WAIT | READ_WRITE)

Statement 181:
-------------------------------------------------------------------------------
SELECT GEN_ID(GEN_NUM, 1) NUMS FROM RDB$DATABASE
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
PLAN (RDB$DATABASE NATURAL)
1 records fetched
      0 ms, 2 read(s), 14 fetch(es), 1 mark(s)

Table                             Natural     Index    Update    Insert    Delete   Backout     Purge   Expunge
***************************************************************************************************************
RDB$DATABASE                            1
RDB$CHARACTER_SETS                                1
RDB$COLLATIONS                                    1
"""
        output = """EventAttach(event_id=1, timestamp=datetime.datetime(2014, 5, 23, 11, 0, 28, 584000), status=<Status.OK: ' '>, attachment_id=8, database='/home/employee.fdb', charset='ISO88591', protocol='TCPv4', address='192.168.1.5', user='SYSDBA', role='NONE', remote_process='/opt/firebird/bin/isql', remote_pid=8723)
TransactionInfo(attachment_id=8, transaction_id=1570, initial_id=None, options=['READ_COMMITTED', 'REC_VERSION', 'WAIT', 'READ_WRITE'])
SQLInfo(sql_id=1, sql='SELECT GEN_ID(GEN_NUM, 1) NUMS FROM RDB$DATABASE', plan='PLAN (RDB$DATABASE NATURAL)')
EventStatementFinish(event_id=2, timestamp=datetime.datetime(2014, 5, 23, 11, 0, 45, 542000), status=<Status.OK: ' '>, attachment_id=8, transaction_id=1570, statement_id=181, sql_id=1, param_id=None, records=1, run_time=0, reads=2, writes=None, fetches=14, marks=1, access=[AccessStats(table='RDB$DATABASE', natural=1, index=0, update=0, insert=0, delete=0, backout=0, purge=0, expunge=0), AccessStats(table='RDB$CHARACTER_SETS', natural=0, index=1, update=0, insert=0, delete=0, backout=0, purge=0, expunge=0), AccessStats(table='RDB$COLLATIONS', natural=0, index=1, update=0, insert=0, delete=0, backout=0, purge=0, expunge=0)])
"""
        self._check_events(trace_lines, output)
    def test_39_statement_finish_no_attachment_no_transaction(self):
        trace_lines = """2014-05-23T11:00:45.5420 (3720:0000000000EFD9E8) EXECUTE_STATEMENT_FINISH
	/home/employee.fdb (ATT_8, SYSDBA:NONE, ISO88591, TCPv4:192.168.1.5)
	/opt/firebird/bin/isql:8723
		(TRA_1570, READ_COMMITTED | REC_VERSION | WAIT | READ_WRITE)

Statement 181:
-------------------------------------------------------------------------------
SELECT GEN_ID(GEN_NUM, 1) NUMS FROM RDB$DATABASE
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
PLAN (RDB$DATABASE NATURAL)
1 records fetched
      0 ms, 2 read(s), 14 fetch(es), 1 mark(s)

Table                             Natural     Index    Update    Insert    Delete   Backout     Purge   Expunge
***************************************************************************************************************
RDB$DATABASE                            1
RDB$CHARACTER_SETS                                1
RDB$COLLATIONS                                    1
"""
        output = """AttachmentInfo(attachment_id=8, database='/home/employee.fdb', charset='ISO88591', protocol='TCPv4', address='192.168.1.5', user='SYSDBA', role='NONE', remote_process='/opt/firebird/bin/isql', remote_pid=8723)
TransactionInfo(attachment_id=8, transaction_id=1570, initial_id=None, options=['READ_COMMITTED', 'REC_VERSION', 'WAIT', 'READ_WRITE'])
SQLInfo(sql_id=1, sql='SELECT GEN_ID(GEN_NUM, 1) NUMS FROM RDB$DATABASE', plan='PLAN (RDB$DATABASE NATURAL)')
EventStatementFinish(event_id=1, timestamp=datetime.datetime(2014, 5, 23, 11, 0, 45, 542000), status=<Status.OK: ' '>, attachment_id=8, transaction_id=1570, statement_id=181, sql_id=1, param_id=None, records=1, run_time=0, reads=2, writes=None, fetches=14, marks=1, access=[AccessStats(table='RDB$DATABASE', natural=1, index=0, update=0, insert=0, delete=0, backout=0, purge=0, expunge=0), AccessStats(table='RDB$CHARACTER_SETS', natural=0, index=1, update=0, insert=0, delete=0, backout=0, purge=0, expunge=0), AccessStats(table='RDB$COLLATIONS', natural=0, index=1, update=0, insert=0, delete=0, backout=0, purge=0, expunge=0)])
"""
        self._check_events(trace_lines, output)
    def test_40_statement_finish_no_performance(self):
        trace_lines = """2014-05-23T11:00:28.5840 (3720:0000000000EFD9E8) ATTACH_DATABASE
	/home/employee.fdb (ATT_8, SYSDBA:NONE, ISO88591, TCPv4:192.168.1.5)
	/opt/firebird/bin/isql:8723

2014-05-23T11:00:28.6160 (3720:0000000000EFD9E8) START_TRANSACTION
	/home/employee.fdb (ATT_8, SYSDBA:NONE, ISO88591, TCPv4:192.168.1.5)
	/opt/firebird/bin/isql:8723
		(TRA_1570, READ_COMMITTED | REC_VERSION | WAIT | READ_WRITE)

2014-05-23T11:00:45.5420 (3720:0000000000EFD9E8) EXECUTE_STATEMENT_FINISH
	/home/employee.fdb (ATT_8, SYSDBA:NONE, ISO88591, TCPv4:192.168.1.5)
	/opt/firebird/bin/isql:8723
		(TRA_1570, READ_COMMITTED | REC_VERSION | WAIT | READ_WRITE)

Statement 181:
-------------------------------------------------------------------------------
SELECT GEN_ID(GEN_NUM, 1) NUMS FROM RDB$DATABASE
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
PLAN (RDB$DATABASE NATURAL)
"""
        output = """EventAttach(event_id=1, timestamp=datetime.datetime(2014, 5, 23, 11, 0, 28, 584000), status=<Status.OK: ' '>, attachment_id=8, database='/home/employee.fdb', charset='ISO88591', protocol='TCPv4', address='192.168.1.5', user='SYSDBA', role='NONE', remote_process='/opt/firebird/bin/isql', remote_pid=8723)
EventTransactionStart(event_id=2, timestamp=datetime.datetime(2014, 5, 23, 11, 0, 28, 616000), status=<Status.OK: ' '>, attachment_id=8, transaction_id=1570, options=['READ_COMMITTED', 'REC_VERSION', 'WAIT', 'READ_WRITE'])
SQLInfo(sql_id=1, sql='SELECT GEN_ID(GEN_NUM, 1) NUMS FROM RDB$DATABASE', plan='PLAN (RDB$DATABASE NATURAL)')
EventStatementFinish(event_id=3, timestamp=datetime.datetime(2014, 5, 23, 11, 0, 45, 542000), status=<Status.OK: ' '>, attachment_id=8, transaction_id=1570, statement_id=181, sql_id=1, param_id=None, records=None, run_time=None, reads=None, writes=None, fetches=None, marks=None, access=None)
"""
        self._check_events(trace_lines, output)
    def test_41_statement_free(self):
        trace_lines = """2014-05-23T11:00:28.5840 (3720:0000000000EFD9E8) ATTACH_DATABASE
	/home/employee.fdb (ATT_8, SYSDBA:NONE, ISO88591, TCPv4:192.168.1.5)
	/opt/firebird/bin/isql:8723

2014-05-23T11:00:28.6160 (3720:0000000000EFD9E8) START_TRANSACTION
	/home/employee.fdb (ATT_8, SYSDBA:NONE, ISO88591, TCPv4:192.168.1.5)
	/opt/firebird/bin/isql:8723
		(TRA_1570, READ_COMMITTED | REC_VERSION | WAIT | READ_WRITE)

2014-05-23T11:00:45.5260 (3720:0000000000EFD9E8) FREE_STATEMENT
	/home/employee.fdb (ATT_8, SYSDBA:NONE, ISO88591, TCPv4:192.168.1.5)
	/opt/firebird/bin/isql:8723

-------------------------------------------------------------------------------
UPDATE TABLE_A SET VAL_1=?, VAL_2=?, VAL_3=?, VAL_4=? WHERE ID_EX=?
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
PLAN (TABLE_A INDEX (TABLE_A_PK))
"""
        output = """EventAttach(event_id=1, timestamp=datetime.datetime(2014, 5, 23, 11, 0, 28, 584000), status=<Status.OK: ' '>, attachment_id=8, database='/home/employee.fdb', charset='ISO88591', protocol='TCPv4', address='192.168.1.5', user='SYSDBA', role='NONE', remote_process='/opt/firebird/bin/isql', remote_pid=8723)
EventTransactionStart(event_id=2, timestamp=datetime.datetime(2014, 5, 23, 11, 0, 28, 616000), status=<Status.OK: ' '>, attachment_id=8, transaction_id=1570, options=['READ_COMMITTED', 'REC_VERSION', 'WAIT', 'READ_WRITE'])
SQLInfo(sql_id=1, sql='UPDATE TABLE_A SET VAL_1=?, VAL_2=?, VAL_3=?, VAL_4=? WHERE ID_EX=?', plan='PLAN (TABLE_A INDEX (TABLE_A_PK))')
EventFreeStatement(event_id=3, timestamp=datetime.datetime(2014, 5, 23, 11, 0, 45, 526000), attachment_id=8, statement_id=0, sql_id=1)
"""
        self._check_events(trace_lines, output)
    def test_42_close_cursor(self):
        trace_lines = """2014-05-23T11:00:28.5840 (3720:0000000000EFD9E8) ATTACH_DATABASE
	/home/employee.fdb (ATT_8, SYSDBA:NONE, ISO88591, TCPv4:192.168.1.5)
	/opt/firebird/bin/isql:8723

2014-05-23T11:00:28.6160 (3720:0000000000EFD9E8) START_TRANSACTION
	/home/employee.fdb (ATT_8, SYSDBA:NONE, ISO88591, TCPv4:192.168.1.5)
	/opt/firebird/bin/isql:8723
		(TRA_1570, READ_COMMITTED | REC_VERSION | WAIT | READ_WRITE)

2014-05-23T11:00:45.5260 (3720:0000000000EFD9E8) CLOSE_CURSOR
	/home/employee.fdb (ATT_8, SYSDBA:NONE, ISO88591, TCPv4:192.168.1.5)
	/opt/firebird/bin/isql:8723

-------------------------------------------------------------------------------
UPDATE TABLE_A SET VAL_1=?, VAL_2=?, VAL_3=?, VAL_4=? WHERE ID_EX=?
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
PLAN (TABLE_A INDEX (TABLE_A_PK))
"""
        output = """EventAttach(event_id=1, timestamp=datetime.datetime(2014, 5, 23, 11, 0, 28, 584000), status=<Status.OK: ' '>, attachment_id=8, database='/home/employee.fdb', charset='ISO88591', protocol='TCPv4', address='192.168.1.5', user='SYSDBA', role='NONE', remote_process='/opt/firebird/bin/isql', remote_pid=8723)
EventTransactionStart(event_id=2, timestamp=datetime.datetime(2014, 5, 23, 11, 0, 28, 616000), status=<Status.OK: ' '>, attachment_id=8, transaction_id=1570, options=['READ_COMMITTED', 'REC_VERSION', 'WAIT', 'READ_WRITE'])
SQLInfo(sql_id=1, sql='UPDATE TABLE_A SET VAL_1=?, VAL_2=?, VAL_3=?, VAL_4=? WHERE ID_EX=?', plan='PLAN (TABLE_A INDEX (TABLE_A_PK))')
EventCloseCursor(event_id=3, timestamp=datetime.datetime(2014, 5, 23, 11, 0, 45, 526000), attachment_id=8, statement_id=0, sql_id=1)
"""
        self._check_events(trace_lines, output)
    def test_43_trigger_start(self):
        trace_lines = """2014-05-23T11:00:28.5840 (3720:0000000000EFD9E8) ATTACH_DATABASE
	/home/employee.fdb (ATT_8, SYSDBA:NONE, ISO88591, TCPv4:192.168.1.5)
	/opt/firebird/bin/isql:8723

2014-05-23T11:00:28.6160 (3720:0000000000EFD9E8) START_TRANSACTION
	/home/employee.fdb (ATT_8, SYSDBA:NONE, ISO88591, TCPv4:192.168.1.5)
	/opt/firebird/bin/isql:8723
		(TRA_1570, READ_COMMITTED | REC_VERSION | WAIT | READ_WRITE)

2014-05-23T11:00:45.5260 (3720:0000000000EFD9E8) EXECUTE_TRIGGER_START
	/home/employee.fdb (ATT_8, SYSDBA:NONE, ISO88591, TCPv4:192.168.1.5)
	/opt/firebird/bin/isql:8723
		(TRA_1570, READ_COMMITTED | REC_VERSION | WAIT | READ_WRITE)
        BI_TABLE_A FOR TABLE_A (BEFORE INSERT)
"""
        output = """EventAttach(event_id=1, timestamp=datetime.datetime(2014, 5, 23, 11, 0, 28, 584000), status=<Status.OK: ' '>, attachment_id=8, database='/home/employee.fdb', charset='ISO88591', protocol='TCPv4', address='192.168.1.5', user='SYSDBA', role='NONE', remote_process='/opt/firebird/bin/isql', remote_pid=8723)
EventTransactionStart(event_id=2, timestamp=datetime.datetime(2014, 5, 23, 11, 0, 28, 616000), status=<Status.OK: ' '>, attachment_id=8, transaction_id=1570, options=['READ_COMMITTED', 'REC_VERSION', 'WAIT', 'READ_WRITE'])
EventTriggerStart(event_id=3, timestamp=datetime.datetime(2014, 5, 23, 11, 0, 45, 526000), status=<Status.OK: ' '>, attachment_id=8, transaction_id=1570, trigger='BI_TABLE_A', table='TABLE_A', event='BEFORE INSERT')
"""
        self._check_events(trace_lines, output)
    def test_44_trigger_finish(self):
        trace_lines = """2014-05-23T11:00:28.5840 (3720:0000000000EFD9E8) ATTACH_DATABASE
	/home/employee.fdb (ATT_8, SYSDBA:NONE, ISO88591, TCPv4:192.168.1.5)
	/opt/firebird/bin/isql:8723

2014-05-23T11:00:28.6160 (3720:0000000000EFD9E8) START_TRANSACTION
	/home/employee.fdb (ATT_8, SYSDBA:NONE, ISO88591, TCPv4:192.168.1.5)
	/opt/firebird/bin/isql:8723
		(TRA_1570, READ_COMMITTED | REC_VERSION | WAIT | READ_WRITE)

2014-05-23T11:00:45.5260 (3720:0000000000EFD9E8) EXECUTE_TRIGGER_FINISH
	/home/employee.fdb (ATT_8, SYSDBA:NONE, ISO88591, TCPv4:192.168.1.5)
	/opt/firebird/bin/isql:8723
		(TRA_1570, READ_COMMITTED | REC_VERSION | WAIT | READ_WRITE)
	AIU_TABLE_A FOR TABLE_A (AFTER INSERT)
   1118 ms, 681 read(s), 80 write(s), 1426 fetch(es), 80 mark(s)

Table                             Natural     Index    Update    Insert    Delete   Backout     Purge   Expunge
***************************************************************************************************************
RDB$DATABASE                            1
RDB$INDICES                                     107
RDB$RELATIONS                                    10
RDB$FORMATS                                       6
RDB$RELATION_CONSTRAINTS                         20
TABLE_A                                                              1
TABLE_B                                           2
TABLE_C                                           1
TABLE_D                                                              1
TABLE_E                                           3
TABLE_F                                          25
"""
        output = """EventAttach(event_id=1, timestamp=datetime.datetime(2014, 5, 23, 11, 0, 28, 584000), status=<Status.OK: ' '>, attachment_id=8, database='/home/employee.fdb', charset='ISO88591', protocol='TCPv4', address='192.168.1.5', user='SYSDBA', role='NONE', remote_process='/opt/firebird/bin/isql', remote_pid=8723)
EventTransactionStart(event_id=2, timestamp=datetime.datetime(2014, 5, 23, 11, 0, 28, 616000), status=<Status.OK: ' '>, attachment_id=8, transaction_id=1570, options=['READ_COMMITTED', 'REC_VERSION', 'WAIT', 'READ_WRITE'])
EventTriggerFinish(event_id=3, timestamp=datetime.datetime(2014, 5, 23, 11, 0, 45, 526000), status=<Status.OK: ' '>, attachment_id=8, transaction_id=1570, trigger='AIU_TABLE_A', table='TABLE_A', event='AFTER INSERT', run_time=1118, reads=681, writes=80, fetches=1426, marks=80, access=[AccessStats(table='RDB$DATABASE', natural=1, index=0, update=0, insert=0, delete=0, backout=0, purge=0, expunge=0), AccessStats(table='RDB$INDICES', natural=0, index=107, update=0, insert=0, delete=0, backout=0, purge=0, expunge=0), AccessStats(table='RDB$RELATIONS', natural=0, index=10, update=0, insert=0, delete=0, backout=0, purge=0, expunge=0), AccessStats(table='RDB$FORMATS', natural=0, index=6, update=0, insert=0, delete=0, backout=0, purge=0, expunge=0), AccessStats(table='RDB$RELATION_CONSTRAINTS', natural=0, index=20, update=0, insert=0, delete=0, backout=0, purge=0, expunge=0), AccessStats(table='TABLE_A', natural=0, index=0, update=0, insert=1, delete=0, backout=0, purge=0, expunge=0), AccessStats(table='TABLE_B', natural=0, index=2, update=0, insert=0, delete=0, backout=0, purge=0, expunge=0), AccessStats(table='TABLE_C', natural=0, index=1, update=0, insert=0, delete=0, backout=0, purge=0, expunge=0), AccessStats(table='TABLE_D', natural=0, index=0, update=0, insert=1, delete=0, backout=0, purge=0, expunge=0), AccessStats(table='TABLE_E', natural=0, index=3, update=0, insert=0, delete=0, backout=0, purge=0, expunge=0), AccessStats(table='TABLE_F', natural=0, index=25, update=0, insert=0, delete=0, backout=0, purge=0, expunge=0)])
"""
        self._check_events(trace_lines, output)
    def test_45_procedure_start(self):
        trace_lines = """2014-05-23T11:00:28.5840 (3720:0000000000EFD9E8) ATTACH_DATABASE
	/home/employee.fdb (ATT_8, SYSDBA:NONE, ISO88591, TCPv4:192.168.1.5)
	/opt/firebird/bin/isql:8723

2014-05-23T11:00:28.6160 (3720:0000000000EFD9E8) START_TRANSACTION
	/home/employee.fdb (ATT_8, SYSDBA:NONE, ISO88591, TCPv4:192.168.1.5)
	/opt/firebird/bin/isql:8723
		(TRA_1570, READ_COMMITTED | REC_VERSION | WAIT | READ_WRITE)

2014-05-23T11:00:45.5260 (3720:0000000000EFD9E8) EXECUTE_PROCEDURE_START
	/home/employee.fdb (ATT_8, SYSDBA:NONE, ISO88591, TCPv4:192.168.1.5)
	/opt/firebird/bin/isql:8723
		(TRA_1570, READ_COMMITTED | REC_VERSION | WAIT | READ_WRITE)

Procedure PROC_A:
param0 = varchar(50), "758749"
param1 = varchar(10), "XXX"
"""
        output = """EventAttach(event_id=1, timestamp=datetime.datetime(2014, 5, 23, 11, 0, 28, 584000), status=<Status.OK: ' '>, attachment_id=8, database='/home/employee.fdb', charset='ISO88591', protocol='TCPv4', address='192.168.1.5', user='SYSDBA', role='NONE', remote_process='/opt/firebird/bin/isql', remote_pid=8723)
EventTransactionStart(event_id=2, timestamp=datetime.datetime(2014, 5, 23, 11, 0, 28, 616000), status=<Status.OK: ' '>, attachment_id=8, transaction_id=1570, options=['READ_COMMITTED', 'REC_VERSION', 'WAIT', 'READ_WRITE'])
ParamSet(par_id=1, params=[('varchar(50)', '758749'), ('varchar(10)', 'XXX')])
EventProcedureStart(event_id=3, timestamp=datetime.datetime(2014, 5, 23, 11, 0, 45, 526000), status=<Status.OK: ' '>, attachment_id=8, transaction_id=1570, procedure='PROC_A', param_id=1)
"""
        self._check_events(trace_lines, output)
    def test_46_procedure_finish(self):
        trace_lines = """2014-05-23T11:00:28.5840 (3720:0000000000EFD9E8) ATTACH_DATABASE
	/home/employee.fdb (ATT_8, SYSDBA:NONE, ISO88591, TCPv4:192.168.1.5)
	/opt/firebird/bin/isql:8723

2014-05-23T11:00:28.6160 (3720:0000000000EFD9E8) START_TRANSACTION
	/home/employee.fdb (ATT_8, SYSDBA:NONE, ISO88591, TCPv4:192.168.1.5)
	/opt/firebird/bin/isql:8723
		(TRA_1570, READ_COMMITTED | REC_VERSION | WAIT | READ_WRITE)

2014-05-23T11:00:45.5260 (3720:0000000000EFD9E8) EXECUTE_PROCEDURE_FINISH
	/home/employee.fdb (ATT_8, SYSDBA:NONE, ISO88591, TCPv4:192.168.1.5)
	/opt/firebird/bin/isql:8723
		(TRA_1570, READ_COMMITTED | REC_VERSION | WAIT | READ_WRITE)

Procedure PROC_A:
param0 = varchar(10), "XXX"
param1 = double precision, "313204"
param2 = double precision, "1"
param3 = varchar(20), "50031"
param4 = varchar(20), "GGG(1.25)"
param5 = varchar(10), "PP100X120"
param6 = varchar(20), "<NULL>"
param7 = double precision, "3.33333333333333"
param8 = double precision, "45"
param9 = integer, "3"
param10 = integer, "<NULL>"
param11 = double precision, "1"
param12 = integer, "0"

      0 ms, 14 read(s), 14 fetch(es)

Table                             Natural     Index    Update    Insert    Delete   Backout     Purge   Expunge
***************************************************************************************************************
TABLE_A                                           1
TABLE_B                                           1
"""
        output = """EventAttach(event_id=1, timestamp=datetime.datetime(2014, 5, 23, 11, 0, 28, 584000), status=<Status.OK: ' '>, attachment_id=8, database='/home/employee.fdb', charset='ISO88591', protocol='TCPv4', address='192.168.1.5', user='SYSDBA', role='NONE', remote_process='/opt/firebird/bin/isql', remote_pid=8723)
EventTransactionStart(event_id=2, timestamp=datetime.datetime(2014, 5, 23, 11, 0, 28, 616000), status=<Status.OK: ' '>, attachment_id=8, transaction_id=1570, options=['READ_COMMITTED', 'REC_VERSION', 'WAIT', 'READ_WRITE'])
ParamSet(par_id=1, params=[('varchar(10)', 'XXX'), ('double precision', Decimal('313204')), ('double precision', Decimal('1')), ('varchar(20)', '50031'), ('varchar(20)', 'GGG(1.25)'), ('varchar(10)', 'PP100X120'), ('varchar(20)', None), ('double precision', Decimal('3.33333333333333')), ('double precision', Decimal('45')), ('integer', 3), ('integer', None), ('double precision', Decimal('1')), ('integer', 0)])
EventProcedureFinish(event_id=3, timestamp=datetime.datetime(2014, 5, 23, 11, 0, 45, 526000), status=<Status.OK: ' '>, attachment_id=8, transaction_id=1570, procedure='PROC_A', param_id=1, records=None, run_time=0, reads=14, writes=None, fetches=14, marks=None, access=[AccessStats(table='TABLE_A', natural=0, index=1, update=0, insert=0, delete=0, backout=0, purge=0, expunge=0), AccessStats(table='TABLE_B', natural=0, index=1, update=0, insert=0, delete=0, backout=0, purge=0, expunge=0)])
"""
        self._check_events(trace_lines, output)
    def test_47_service_attach(self):
        trace_lines = """2017-11-13T11:49:51.3110 (2500:0000000026C3C858) ATTACH_SERVICE
	service_mgr, (Service 0000000019993DC0, SYSDBA, TCPv4:127.0.0.1, /job/fbtrace:385)
"""
        output = """ServiceInfo(service_id=429473216, user='SYSDBA', protocol='TCPv4', address='127.0.0.1', remote_process='/job/fbtrace', remote_pid=385)
EventServiceAttach(event_id=1, timestamp=datetime.datetime(2017, 11, 13, 11, 49, 51, 311000), status=<Status.OK: ' '>, service_id=429473216)
"""
        self._check_events(trace_lines, output)
    def test_48_service_detach(self):
        trace_lines = """2017-11-13T22:50:09.3790 (2500:0000000026C39D70) DETACH_SERVICE
	service_mgr, (Service 0000000028290058, SYSDBA, TCPv4:127.0.0.1, /job/fbtrace:385)
"""
        output = """ServiceInfo(service_id=673775704, user='SYSDBA', protocol='TCPv4', address='127.0.0.1', remote_process='/job/fbtrace', remote_pid=385)
EventServiceDetach(event_id=1, timestamp=datetime.datetime(2017, 11, 13, 22, 50, 9, 379000), status=<Status.OK: ' '>, service_id=673775704)
"""
        self._check_events(trace_lines, output)
    def test_49_service_start(self):
        trace_lines = """2017-11-13T11:49:07.7860 (2500:0000000001A4DB68) START_SERVICE
	service_mgr, (Service 000000001F6F1CF8, SYSDBA, TCPv4:127.0.0.1, /job/fbtrace:385)
	"Start Trace Session"
	-TRUSTED_SVC SYSDBA -START -CONFIG <database %[\\/]TEST.FDB>
enabled true
log_connections true
log_transactions true
log_statement_prepare false
log_statement_free false
log_statement_start false
log_statement_finish false
print_plan false
print_perf false
time_threshold 1000
max_sql_length 300
max_arg_length 80
max_arg_count 30
log_procedure_start false
log_procedure_finish false
log_trigger_start false
log_trigger_finish false
log_context false
log_errors false
log_sweep false
log_blr_requests false
print_blr false
max_blr_length 500
log_dyn_requests false
print_dyn false
max_dyn_length 500
log_warnings false
log_initfini false
</database>

<services>
enabled true
log_services true
log_errors false
log_warnings false
log_initfini false
</services>
"""
        output = """ServiceInfo(service_id=527375608, user='SYSDBA', protocol='TCPv4', address='127.0.0.1', remote_process='/job/fbtrace', remote_pid=385)
EventServiceStart(event_id=1, timestamp=datetime.datetime(2017, 11, 13, 11, 49, 7, 786000), status=<Status.OK: ' '>, service_id=527375608, action='Start Trace Session', parameters=['-TRUSTED_SVC SYSDBA -START -CONFIG <database %[\\\\/]TEST.FDB>', 'enabled true', 'log_connections true', 'log_transactions true', 'log_statement_prepare false', 'log_statement_free false', 'log_statement_start false', 'log_statement_finish false', 'print_plan false', 'print_perf false', 'time_threshold 1000', 'max_sql_length 300', 'max_arg_length 80', 'max_arg_count 30', 'log_procedure_start false', 'log_procedure_finish false', 'log_trigger_start false', 'log_trigger_finish false', 'log_context false', 'log_errors false', 'log_sweep false', 'log_blr_requests false', 'print_blr false', 'max_blr_length 500', 'log_dyn_requests false', 'print_dyn false', 'max_dyn_length 500', 'log_warnings false', 'log_initfini false', '</database>', '<services>', 'enabled true', 'log_services true', 'log_errors false', 'log_warnings false', 'log_initfini false', '</services>'])
"""
        self._check_events(trace_lines, output)
    def test_50_service_query(self):
        trace_lines = """2018-03-29T14:02:10.9180 (5924:0x7feab93f4978) QUERY_SERVICE
	service_mgr, (Service 0x7feabd3da548, SYSDBA, TCPv4:127.0.0.1, /job/fbtrace:385)
	"Start Trace Session"
	 Receive portion of the query:
		 retrieve 1 line of service output per call

2018-04-03T12:41:01.7970 (5831:0x7f748c054978) QUERY_SERVICE
	service_mgr, (Service 0x7f748f839540, SYSDBA, TCPv4:127.0.0.1, /job/fbtrace:4631)
	 Receive portion of the query:
		 retrieve the version of the server engine

2018-04-03T12:41:30.7840 (5831:0x7f748c054978) QUERY_SERVICE
	service_mgr, (Service 0x7f748f839540, SYSDBA, TCPv4:127.0.0.1, /job/fbtrace:4631)
	 Receive portion of the query:
		 retrieve the implementation of the Firebird server

2018-04-03T12:56:27.5590 (5831:0x7f748c054978) QUERY_SERVICE
	service_mgr, (Service 0x7f748f839540, SYSDBA, TCPv4:127.0.0.1, /job/fbtrace:4631)
	"Repair Database"
"""
        output = """ServiceInfo(service_id=140646174008648, user='SYSDBA', protocol='TCPv4', address='127.0.0.1', remote_process='/job/fbtrace', remote_pid=385)
EventServiceQuery(event_id=1, timestamp=datetime.datetime(2018, 3, 29, 14, 2, 10, 918000), status=<Status.OK: ' '>, service_id=140646174008648, action='Start Trace Session', sent=[], received=['retrieve 1 line of service output per call'])
ServiceInfo(service_id=140138600699200, user='SYSDBA', protocol='TCPv4', address='127.0.0.1', remote_process='/job/fbtrace', remote_pid=4631)
EventServiceQuery(event_id=2, timestamp=datetime.datetime(2018, 4, 3, 12, 41, 1, 797000), status=<Status.OK: ' '>, service_id=140138600699200, action=None, sent=[], received=['retrieve the version of the server engine'])
EventServiceQuery(event_id=3, timestamp=datetime.datetime(2018, 4, 3, 12, 41, 30, 784000), status=<Status.OK: ' '>, service_id=140138600699200, action=None, sent=[], received=['retrieve the implementation of the Firebird server'])
EventServiceQuery(event_id=4, timestamp=datetime.datetime(2018, 4, 3, 12, 56, 27, 559000), status=<Status.OK: ' '>, service_id=140138600699200, action='Repair Database', sent=[], received=[])
"""
        if sys.version_info.major == 2 and sys.version_info.minor == 7 and sys.version_info.micro > 13:
            output = """ServiceInfo(service_id=140646174008648, user='SYSDBA', protocol='TCPv4', address='127.0.0.1', remote_process='/job/fbtrace', remote_pid=385)
EventServiceQuery(event_id=1, timestamp=datetime.datetime(2018, 3, 29, 14, 2, 10, 918000), status=<Status.OK: ' '>, service_id=140646174008648, action='Start Trace Session', sent=[], received=['retrieve 1 line of service output per call'])
ServiceInfo(service_id=140138600699200, user='SYSDBA', protocol='TCPv4', address='127.0.0.1', remote_process='/job/fbtrace', remote_pid=4631)
EventServiceQuery(event_id=2, timestamp=datetime.datetime(2018, 4, 3, 12, 41, 1, 797000), status=<Status.OK: ' '>, service_id=140138600699200, action=None, sent=[], received=['retrieve the version of the server engine'])
EventServiceQuery(event_id=3, timestamp=datetime.datetime(2018, 4, 3, 12, 41, 30, 784000), status=<Status.OK: ' '>, service_id=140138600699200, action=None, sent=[], received=['retrieve the implementation of the Firebird server'])
EventServiceQuery(event_id=4, timestamp=datetime.datetime(2018, 4, 3, 12, 56, 27, 559000), status=<Status.OK: ' '>, service_id=140138600699200, action='Repair Database', sent=[], received=[])
"""
        self._check_events(trace_lines, output)
    def test_51_set_context(self):
        trace_lines = """2014-05-23T11:00:28.5840 (3720:0000000000EFD9E8) ATTACH_DATABASE
	/home/employee.fdb (ATT_8, SYSDBA:NONE, ISO88591, TCPv4:192.168.1.5)
	/opt/firebird/bin/isql:8723

2014-05-23T11:00:28.6160 (3720:0000000000EFD9E8) START_TRANSACTION
	/home/employee.fdb (ATT_8, SYSDBA:NONE, ISO88591, TCPv4:192.168.1.5)
	/opt/firebird/bin/isql:8723
		(TRA_1570, READ_COMMITTED | REC_VERSION | WAIT | READ_WRITE)

2017-11-09T11:21:59.0270 (2500:0000000001A45B00) SET_CONTEXT
	/home/employee.fdb (ATT_8, SYSDBA:NONE, ISO88591, TCPv4:192.168.1.5)
	/opt/firebird/bin/isql:8723
		(TRA_1570, READ_COMMITTED | REC_VERSION | WAIT | READ_WRITE)
[USER_TRANSACTION] TRANSACTION_TIMESTAMP = "2017-11-09 11:21:59.0270"

2017-11-09T11:21:59.0300 (2500:0000000001A45B00) SET_CONTEXT
	/home/employee.fdb (ATT_8, SYSDBA:NONE, ISO88591, TCPv4:192.168.1.5)
	/opt/firebird/bin/isql:8723
		(TRA_1570, READ_COMMITTED | REC_VERSION | WAIT | READ_WRITE)
[USER_SESSION] MY_KEY = "1"
"""
        output = """EventAttach(event_id=1, timestamp=datetime.datetime(2014, 5, 23, 11, 0, 28, 584000), status=<Status.OK: ' '>, attachment_id=8, database='/home/employee.fdb', charset='ISO88591', protocol='TCPv4', address='192.168.1.5', user='SYSDBA', role='NONE', remote_process='/opt/firebird/bin/isql', remote_pid=8723)
EventTransactionStart(event_id=2, timestamp=datetime.datetime(2014, 5, 23, 11, 0, 28, 616000), status=<Status.OK: ' '>, attachment_id=8, transaction_id=1570, options=['READ_COMMITTED', 'REC_VERSION', 'WAIT', 'READ_WRITE'])
EventSetContext(event_id=3, timestamp=datetime.datetime(2017, 11, 9, 11, 21, 59, 27000), attachment_id=8, transaction_id=1570, context='USER_TRANSACTION', key='TRANSACTION_TIMESTAMP', value='2017-11-09 11:21:59.0270')
EventSetContext(event_id=4, timestamp=datetime.datetime(2017, 11, 9, 11, 21, 59, 30000), attachment_id=8, transaction_id=1570, context='USER_SESSION', key='MY_KEY', value='1')
"""
        self._check_events(trace_lines, output)
    def test_52_error(self):
        trace_lines = """2018-03-22T10:06:59.5090 (4992:0x7f92a22a4978) ERROR AT jrd8_attach_database
	/home/test.fdb (ATT_0, sysdba, NONE, TCPv4:127.0.0.1)
	/usr/bin/flamerobin:4985
335544344 : I/O error during "open" operation for file "/home/test.fdb"
335544734 : Error while trying to open file
        2 : No such file or directory

2018-03-22T11:00:59.5090 (2500:0000000022415DB8) ERROR AT jrd8_fetch
	/home/test.fdb (ATT_519417, SYSDBA:NONE, WIN1250, TCPv4:172.19.54.61)
	/usr/bin/flamerobin:4985
335544364 : request synchronization error

2018-04-03T12:49:28.5080 (5831:0x7f748c054978) ERROR AT jrd8_service_query
	service_mgr, (Service 0x7f748f839540, SYSDBA, TCPv4:127.0.0.1, /job/fbtrace:4631)
335544344 : I/O error during "open" operation for file "bug.fdb"
335544734 : Error while trying to open file
        2 : No such file or directory
"""
        output = """AttachmentInfo(attachment_id=0, database='/home/test.fdb', charset='NONE', protocol='TCPv4', address='127.0.0.1', user='sysdba', role='NONE', remote_process='/usr/bin/flamerobin', remote_pid=4985)
EventError(event_id=1, timestamp=datetime.datetime(2018, 3, 22, 10, 6, 59, 509000), attachment_id=0, place='jrd8_attach_database', details=['335544344 : I/O error during "open" operation for file "/home/test.fdb"', '335544734 : Error while trying to open file', '2 : No such file or directory'])
AttachmentInfo(attachment_id=519417, database='/home/test.fdb', charset='WIN1250', protocol='TCPv4', address='172.19.54.61', user='SYSDBA', role='NONE', remote_process='/usr/bin/flamerobin', remote_pid=4985)
EventError(event_id=2, timestamp=datetime.datetime(2018, 3, 22, 11, 0, 59, 509000), attachment_id=519417, place='jrd8_fetch', details=['335544364 : request synchronization error'])
ServiceInfo(service_id=140138600699200, user='SYSDBA', protocol='TCPv4', address='127.0.0.1', remote_process='/job/fbtrace', remote_pid=4631)
EventServiceError(event_id=3, timestamp=datetime.datetime(2018, 4, 3, 12, 49, 28, 508000), service_id=140138600699200, place='jrd8_service_query', details=['335544344 : I/O error during "open" operation for file "bug.fdb"', '335544734 : Error while trying to open file', '2 : No such file or directory'])
"""
        if sys.version_info.major == 2 and sys.version_info.minor == 7 and sys.version_info.micro > 13:
            output = """AttachmentInfo(attachment_id=0, database='/home/test.fdb', charset='NONE', protocol='TCPv4', address='127.0.0.1', user='sysdba', role='NONE', remote_process='/usr/bin/flamerobin', remote_pid=4985)
EventError(event_id=1, timestamp=datetime.datetime(2018, 3, 22, 10, 6, 59, 509000), attachment_id=0, place='jrd8_attach_database', details=['335544344 : I/O error during "open" operation for file "/home/test.fdb"', '335544734 : Error while trying to open file', '2 : No such file or directory'])
AttachmentInfo(attachment_id=519417, database='/home/test.fdb', charset='WIN1250', protocol='TCPv4', address='172.19.54.61', user='SYSDBA', role='NONE', remote_process='/usr/bin/flamerobin', remote_pid=4985)
EventError(event_id=2, timestamp=datetime.datetime(2018, 3, 22, 11, 0, 59, 509000), attachment_id=519417, place='jrd8_fetch', details=['335544364 : request synchronization error'])
ServiceInfo(service_id=140138600699200, user='SYSDBA', protocol='TCPv4', address='127.0.0.1', remote_process='/job/fbtrace', remote_pid=4631)
EventServiceError(event_id=3, timestamp=datetime.datetime(2018, 4, 3, 12, 49, 28, 508000), service_id=140138600699200, place='jrd8_service_query', details=['335544344 : I/O error during "open" operation for file "bug.fdb"', '335544734 : Error while trying to open file', '2 : No such file or directory'])
"""
        self._check_events(trace_lines, output)
    def test_53_warning(self):
        trace_lines = """2018-03-22T10:06:59.5090 (4992:0x7f92a22a4978) WARNING AT jrd8_attach_database
	/home/test.fdb (ATT_0, sysdba, NONE, TCPv4:127.0.0.1)
	/usr/bin/flamerobin:4985
Some reason for the warning.

2018-04-03T12:49:28.5080 (5831:0x7f748c054978) WARNING AT jrd8_service_query
	service_mgr, (Service 0x7f748f839540, SYSDBA, TCPv4:127.0.0.1, /job/fbtrace:4631)
Some reason for the warning.
"""
        output = """AttachmentInfo(attachment_id=0, database='/home/test.fdb', charset='NONE', protocol='TCPv4', address='127.0.0.1', user='sysdba', role='NONE', remote_process='/usr/bin/flamerobin', remote_pid=4985)
EventWarning(event_id=1, timestamp=datetime.datetime(2018, 3, 22, 10, 6, 59, 509000), attachment_id=0, place='jrd8_attach_database', details=['Some reason for the warning.'])
ServiceInfo(service_id=140138600699200, user='SYSDBA', protocol='TCPv4', address='127.0.0.1', remote_process='/job/fbtrace', remote_pid=4631)
EventServiceWarning(event_id=2, timestamp=datetime.datetime(2018, 4, 3, 12, 49, 28, 508000), service_id=140138600699200, place='jrd8_service_query', details=['Some reason for the warning.'])
"""
        if sys.version_info.major == 2 and sys.version_info.minor == 7 and sys.version_info.micro > 13:
            output = """AttachmentInfo(attachment_id=0, database='/home/test.fdb', charset='NONE', protocol='TCPv4', address='127.0.0.1', user='sysdba', role='NONE', remote_process='/usr/bin/flamerobin', remote_pid=4985)
EventWarning(event_id=1, timestamp=datetime.datetime(2018, 3, 22, 10, 6, 59, 509000), attachment_id=0, place='jrd8_attach_database', details=['Some reason for the warning.'])
ServiceInfo(service_id=140138600699200, user='SYSDBA', protocol='TCPv4', address='127.0.0.1', remote_process='/job/fbtrace', remote_pid=4631)
EventServiceWarning(event_id=2, timestamp=datetime.datetime(2018, 4, 3, 12, 49, 28, 508000), service_id=140138600699200, place='jrd8_service_query', details=['Some reason for the warning.'])
"""
        self._check_events(trace_lines, output)
    def test_54_sweep_start(self):
        trace_lines = """2018-03-22T17:33:56.9690 (12351:0x7f0174bdd978) SWEEP_START
	/opt/firebird/examples/empbuild/employee.fdb (ATT_8, SYSDBA:NONE, NONE, TCPv4:127.0.0.1)

Transaction counters:
	Oldest interesting        155
	Oldest active             156
	Oldest snapshot           156
	Next transaction          156

2018-03-22T18:33:56.9690 (12351:0x7f0174bdd978) SWEEP_START
	/opt/firebird/examples/empbuild/employee.fdb (ATT_9, SYSDBA:NONE, NONE, TCPv4:127.0.0.1)
        /opt/firebird/bin/isql:8723

Transaction counters:
	Oldest interesting        155
	Oldest active             156
	Oldest snapshot           156
	Next transaction          156
"""
        output = """AttachmentInfo(attachment_id=8, database='/opt/firebird/examples/empbuild/employee.fdb', charset='NONE', protocol='TCPv4', address='127.0.0.1', user='SYSDBA', role='NONE', remote_process=None, remote_pid=None)
EventSweepStart(event_id=1, timestamp=datetime.datetime(2018, 3, 22, 17, 33, 56, 969000), attachment_id=8, oit=155, oat=156, ost=156, next=156)
AttachmentInfo(attachment_id=9, database='/opt/firebird/examples/empbuild/employee.fdb', charset='NONE', protocol='TCPv4', address='127.0.0.1', user='SYSDBA', role='NONE', remote_process='/opt/firebird/bin/isql', remote_pid=8723)
EventSweepStart(event_id=2, timestamp=datetime.datetime(2018, 3, 22, 18, 33, 56, 969000), attachment_id=9, oit=155, oat=156, ost=156, next=156)
"""
        self._check_events(trace_lines, output)
    def test_55_sweep_progress(self):
        trace_lines = """2018-03-22T17:33:56.9820 (12351:0x7f0174bdd978) SWEEP_PROGRESS
	/opt/firebird/examples/empbuild/employee.fdb (ATT_8, SYSDBA:NONE, NONE, <internal>)
      0 ms, 5 fetch(es)

2018-03-22T17:33:56.9830 (12351:0x7f0174bdd978) SWEEP_PROGRESS
	/opt/firebird/examples/empbuild/employee.fdb (ATT_8, SYSDBA:NONE, NONE, <internal>)
      0 ms, 6 read(s), 409 fetch(es)

2018-03-22T17:33:56.9920 (12351:0x7f0174bdd978) SWEEP_PROGRESS
	/opt/firebird/examples/empbuild/employee.fdb (ATT_8, SYSDBA:NONE, NONE, <internal>)
      9 ms, 5 read(s), 345 fetch(es), 39 mark(s)

2018-03-22T17:33:56.9930 (12351:0x7f0174bdd978) SWEEP_PROGRESS
	/opt/firebird/examples/empbuild/employee.fdb (ATT_8, SYSDBA:NONE, NONE, <internal>)
      0 ms, 4 read(s), 251 fetch(es), 24 mark(s)

2018-03-22T17:33:57.0000 (12351:0x7f0174bdd978) SWEEP_PROGRESS
	/opt/firebird/examples/empbuild/employee.fdb (ATT_8, SYSDBA:NONE, NONE, <internal>)
      7 ms, 14 read(s), 877 fetch(es), 4 mark(s)

2018-03-22T17:33:57.0000 (12351:0x7f0174bdd978) SWEEP_PROGRESS
	/opt/firebird/examples/empbuild/employee.fdb (ATT_8, SYSDBA:NONE, NONE, <internal>)
      0 ms, 2 read(s), 115 fetch(es)

2018-03-22T17:33:57.0000 (12351:0x7f0174bdd978) SWEEP_PROGRESS
	/opt/firebird/examples/empbuild/employee.fdb (ATT_8, SYSDBA:NONE, NONE, <internal>)
      0 ms, 2 read(s), 7 fetch(es)

2018-03-22T17:33:57.0020 (12351:0x7f0174bdd978) SWEEP_PROGRESS
	/opt/firebird/examples/empbuild/employee.fdb (ATT_8, SYSDBA:NONE, NONE, <internal>)
      1 ms, 2 read(s), 25 fetch(es)

2018-03-22T17:33:57.0070 (12351:0x7f0174bdd978) SWEEP_PROGRESS
	/opt/firebird/examples/empbuild/employee.fdb (ATT_8, SYSDBA:NONE, NONE, <internal>)
      5 ms, 4 read(s), 1 write(s), 339 fetch(es), 97 mark(s)

2018-03-22T17:33:57.0090 (12351:0x7f0174bdd978) SWEEP_PROGRESS
	/opt/firebird/examples/empbuild/employee.fdb (ATT_8, SYSDBA:NONE, NONE, <internal>)
      2 ms, 6 read(s), 1 write(s), 467 fetch(es)

2018-03-22T17:33:57.0100 (12351:0x7f0174bdd978) SWEEP_PROGRESS
	/opt/firebird/examples/empbuild/employee.fdb (ATT_8, SYSDBA:NONE, NONE, <internal>)
      0 ms, 2 read(s), 149 fetch(es)

2018-03-22T17:33:57.0930 (12351:0x7f0174bdd978) SWEEP_PROGRESS
	/opt/firebird/examples/empbuild/employee.fdb (ATT_8, SYSDBA:NONE, NONE, <internal>)
     83 ms, 11 read(s), 8 write(s), 2307 fetch(es), 657 mark(s)

2018-03-22T17:33:57.1010 (12351:0x7f0174bdd978) SWEEP_PROGRESS
	/opt/firebird/examples/empbuild/employee.fdb (ATT_8, SYSDBA:NONE, NONE, <internal>)
      7 ms, 2 read(s), 1 write(s), 7 fetch(es)

2018-03-22T17:33:57.1010 (12351:0x7f0174bdd978) SWEEP_PROGRESS
	/opt/firebird/examples/empbuild/employee.fdb (ATT_8, SYSDBA:NONE, NONE, <internal>)
      0 ms, 2 read(s), 17 fetch(es)

2018-03-22T17:33:57.1010 (12351:0x7f0174bdd978) SWEEP_PROGRESS
	/opt/firebird/examples/empbuild/employee.fdb (ATT_8, SYSDBA:NONE, NONE, <internal>)
      0 ms, 2 read(s), 75 fetch(es)

2018-03-22T17:33:57.1120 (12351:0x7f0174bdd978) SWEEP_PROGRESS
	/opt/firebird/examples/empbuild/employee.fdb (ATT_8, SYSDBA:NONE, NONE, <internal>)
     10 ms, 5 read(s), 305 fetch(es)

2018-03-22T17:33:57.1120 (12351:0x7f0174bdd978) SWEEP_PROGRESS
	/opt/firebird/examples/empbuild/employee.fdb (ATT_8, SYSDBA:NONE, NONE, <internal>)
      0 ms, 2 read(s), 25 fetch(es)

2018-03-22T17:33:57.1120 (12351:0x7f0174bdd978) SWEEP_PROGRESS
	/opt/firebird/examples/empbuild/employee.fdb (ATT_8, SYSDBA:NONE, NONE, <internal>)
      0 ms, 2 read(s), 7 fetch(es)

2018-03-22T17:33:57.1120 (12351:0x7f0174bdd978) SWEEP_PROGRESS
	/opt/firebird/examples/empbuild/employee.fdb (ATT_8, SYSDBA:NONE, NONE, <internal>)
      0 ms, 1 read(s), 165 fetch(es)

2018-03-22T17:33:57.1120 (12351:0x7f0174bdd978) SWEEP_PROGRESS
	/opt/firebird/examples/empbuild/employee.fdb (ATT_8, SYSDBA:NONE, NONE, <internal>)
      0 ms, 2 read(s), 31 fetch(es)

2018-03-22T17:33:57.1120 (12351:0x7f0174bdd978) SWEEP_PROGRESS
	/opt/firebird/examples/empbuild/employee.fdb (ATT_8, SYSDBA:NONE, NONE, <internal>)
      0 ms, 1 read(s), 141 fetch(es)

2018-03-22T17:33:57.1120 (12351:0x7f0174bdd978) SWEEP_PROGRESS
	/opt/firebird/examples/empbuild/employee.fdb (ATT_8, SYSDBA:NONE, NONE, <internal>)
      0 ms, 5 read(s), 29 fetch(es)

2018-03-22T17:33:57.1120 (12351:0x7f0174bdd978) SWEEP_PROGRESS
	/opt/firebird/examples/empbuild/employee.fdb (ATT_8, SYSDBA:NONE, NONE, <internal>)
      0 ms, 2 read(s), 69 fetch(es)

2018-03-22T17:33:57.1120 (12351:0x7f0174bdd978) SWEEP_PROGRESS
	/opt/firebird/examples/empbuild/employee.fdb (ATT_8, SYSDBA:NONE, NONE, <internal>)
      0 ms, 107 fetch(es)

2018-03-22T17:33:57.1120 (12351:0x7f0174bdd978) SWEEP_PROGRESS
	/opt/firebird/examples/empbuild/employee.fdb (ATT_8, SYSDBA:NONE, NONE, <internal>)
      0 ms, 2 read(s), 303 fetch(es)

2018-03-22T17:33:57.1120 (12351:0x7f0174bdd978) SWEEP_PROGRESS
	/opt/firebird/examples/empbuild/employee.fdb (ATT_8, SYSDBA:NONE, NONE, <internal>)
      0 ms, 2 read(s), 13 fetch(es)

2018-03-22T17:33:57.1120 (12351:0x7f0174bdd978) SWEEP_PROGRESS
	/opt/firebird/examples/empbuild/employee.fdb (ATT_8, SYSDBA:NONE, NONE, <internal>)
      0 ms, 5 fetch(es)

2018-03-22T17:33:57.1130 (12351:0x7f0174bdd978) SWEEP_PROGRESS
	/opt/firebird/examples/empbuild/employee.fdb (ATT_8, SYSDBA:NONE, NONE, <internal>)
      0 ms, 2 read(s), 31 fetch(es)

2018-03-22T17:33:57.1130 (12351:0x7f0174bdd978) SWEEP_PROGRESS
	/opt/firebird/examples/empbuild/employee.fdb (ATT_8, SYSDBA:NONE, NONE, <internal>)
      0 ms, 6 read(s), 285 fetch(es), 60 mark(s)

2018-03-22T17:33:57.1350 (12351:0x7f0174bdd978) SWEEP_PROGRESS
	/opt/firebird/examples/empbuild/employee.fdb (ATT_8, SYSDBA:NONE, NONE, <internal>)
      8 ms, 2 read(s), 1 write(s), 45 fetch(es)

2018-03-22T17:33:57.1350 (12351:0x7f0174bdd978) SWEEP_PROGRESS
	/opt/firebird/examples/empbuild/employee.fdb (ATT_8, SYSDBA:NONE, NONE, <internal>)
      0 ms, 3 read(s), 89 fetch(es)

2018-03-22T17:33:57.1350 (12351:0x7f0174bdd978) SWEEP_PROGRESS
	/opt/firebird/examples/empbuild/employee.fdb (ATT_8, SYSDBA:NONE, NONE, <internal>)
      0 ms, 3 read(s), 61 fetch(es), 12 mark(s)

2018-03-22T17:33:57.1420 (12351:0x7f0174bdd978) SWEEP_PROGRESS
	/opt/firebird/examples/empbuild/employee.fdb (ATT_8, SYSDBA:NONE, NONE, <internal>)
      7 ms, 2 read(s), 1 write(s), 59 fetch(es)

2018-03-22T17:33:57.1480 (12351:0x7f0174bdd978) SWEEP_PROGRESS
	/opt/firebird/examples/empbuild/employee.fdb (ATT_8, SYSDBA:NONE, NONE, <internal>)
      5 ms, 3 read(s), 1 write(s), 206 fetch(es), 48 mark(s)

2018-03-22T17:33:57.1510 (12351:0x7f0174bdd978) SWEEP_PROGRESS
	/opt/firebird/examples/empbuild/employee.fdb (ATT_8, SYSDBA:NONE, NONE, <internal>)
      2 ms, 2 read(s), 1 write(s), 101 fetch(es)

2018-03-22T17:33:57.1510 (12351:0x7f0174bdd978) SWEEP_PROGRESS
	/opt/firebird/examples/empbuild/employee.fdb (ATT_8, SYSDBA:NONE, NONE, <internal>)
      0 ms, 2 read(s), 33 fetch(es)

2018-03-22T17:33:57.1510 (12351:0x7f0174bdd978) SWEEP_PROGRESS
	/opt/firebird/examples/empbuild/employee.fdb (ATT_8, SYSDBA:NONE, NONE, <internal>)
      0 ms, 2 read(s), 69 fetch(es)
"""
        output = """AttachmentInfo(attachment_id=8, database='/opt/firebird/examples/empbuild/employee.fdb', charset='NONE', protocol='<internal>', address='<internal>', user='SYSDBA', role='NONE', remote_process=None, remote_pid=None)
EventSweepProgress(event_id=1, timestamp=datetime.datetime(2018, 3, 22, 17, 33, 56, 982000), attachment_id=8, run_time=0, reads=None, writes=None, fetches=5, marks=None, access=None)
EventSweepProgress(event_id=2, timestamp=datetime.datetime(2018, 3, 22, 17, 33, 56, 983000), attachment_id=8, run_time=0, reads=6, writes=None, fetches=409, marks=None, access=None)
EventSweepProgress(event_id=3, timestamp=datetime.datetime(2018, 3, 22, 17, 33, 56, 992000), attachment_id=8, run_time=9, reads=5, writes=None, fetches=345, marks=39, access=None)
EventSweepProgress(event_id=4, timestamp=datetime.datetime(2018, 3, 22, 17, 33, 56, 993000), attachment_id=8, run_time=0, reads=4, writes=None, fetches=251, marks=24, access=None)
EventSweepProgress(event_id=5, timestamp=datetime.datetime(2018, 3, 22, 17, 33, 57), attachment_id=8, run_time=7, reads=14, writes=None, fetches=877, marks=4, access=None)
EventSweepProgress(event_id=6, timestamp=datetime.datetime(2018, 3, 22, 17, 33, 57), attachment_id=8, run_time=0, reads=2, writes=None, fetches=115, marks=None, access=None)
EventSweepProgress(event_id=7, timestamp=datetime.datetime(2018, 3, 22, 17, 33, 57), attachment_id=8, run_time=0, reads=2, writes=None, fetches=7, marks=None, access=None)
EventSweepProgress(event_id=8, timestamp=datetime.datetime(2018, 3, 22, 17, 33, 57, 2000), attachment_id=8, run_time=1, reads=2, writes=None, fetches=25, marks=None, access=None)
EventSweepProgress(event_id=9, timestamp=datetime.datetime(2018, 3, 22, 17, 33, 57, 7000), attachment_id=8, run_time=5, reads=4, writes=1, fetches=339, marks=97, access=None)
EventSweepProgress(event_id=10, timestamp=datetime.datetime(2018, 3, 22, 17, 33, 57, 9000), attachment_id=8, run_time=2, reads=6, writes=1, fetches=467, marks=None, access=None)
EventSweepProgress(event_id=11, timestamp=datetime.datetime(2018, 3, 22, 17, 33, 57, 10000), attachment_id=8, run_time=0, reads=2, writes=None, fetches=149, marks=None, access=None)
EventSweepProgress(event_id=12, timestamp=datetime.datetime(2018, 3, 22, 17, 33, 57, 93000), attachment_id=8, run_time=83, reads=11, writes=8, fetches=2307, marks=657, access=None)
EventSweepProgress(event_id=13, timestamp=datetime.datetime(2018, 3, 22, 17, 33, 57, 101000), attachment_id=8, run_time=7, reads=2, writes=1, fetches=7, marks=None, access=None)
EventSweepProgress(event_id=14, timestamp=datetime.datetime(2018, 3, 22, 17, 33, 57, 101000), attachment_id=8, run_time=0, reads=2, writes=None, fetches=17, marks=None, access=None)
EventSweepProgress(event_id=15, timestamp=datetime.datetime(2018, 3, 22, 17, 33, 57, 101000), attachment_id=8, run_time=0, reads=2, writes=None, fetches=75, marks=None, access=None)
EventSweepProgress(event_id=16, timestamp=datetime.datetime(2018, 3, 22, 17, 33, 57, 112000), attachment_id=8, run_time=10, reads=5, writes=None, fetches=305, marks=None, access=None)
EventSweepProgress(event_id=17, timestamp=datetime.datetime(2018, 3, 22, 17, 33, 57, 112000), attachment_id=8, run_time=0, reads=2, writes=None, fetches=25, marks=None, access=None)
EventSweepProgress(event_id=18, timestamp=datetime.datetime(2018, 3, 22, 17, 33, 57, 112000), attachment_id=8, run_time=0, reads=2, writes=None, fetches=7, marks=None, access=None)
EventSweepProgress(event_id=19, timestamp=datetime.datetime(2018, 3, 22, 17, 33, 57, 112000), attachment_id=8, run_time=0, reads=1, writes=None, fetches=165, marks=None, access=None)
EventSweepProgress(event_id=20, timestamp=datetime.datetime(2018, 3, 22, 17, 33, 57, 112000), attachment_id=8, run_time=0, reads=2, writes=None, fetches=31, marks=None, access=None)
EventSweepProgress(event_id=21, timestamp=datetime.datetime(2018, 3, 22, 17, 33, 57, 112000), attachment_id=8, run_time=0, reads=1, writes=None, fetches=141, marks=None, access=None)
EventSweepProgress(event_id=22, timestamp=datetime.datetime(2018, 3, 22, 17, 33, 57, 112000), attachment_id=8, run_time=0, reads=5, writes=None, fetches=29, marks=None, access=None)
EventSweepProgress(event_id=23, timestamp=datetime.datetime(2018, 3, 22, 17, 33, 57, 112000), attachment_id=8, run_time=0, reads=2, writes=None, fetches=69, marks=None, access=None)
EventSweepProgress(event_id=24, timestamp=datetime.datetime(2018, 3, 22, 17, 33, 57, 112000), attachment_id=8, run_time=0, reads=None, writes=None, fetches=107, marks=None, access=None)
EventSweepProgress(event_id=25, timestamp=datetime.datetime(2018, 3, 22, 17, 33, 57, 112000), attachment_id=8, run_time=0, reads=2, writes=None, fetches=303, marks=None, access=None)
EventSweepProgress(event_id=26, timestamp=datetime.datetime(2018, 3, 22, 17, 33, 57, 112000), attachment_id=8, run_time=0, reads=2, writes=None, fetches=13, marks=None, access=None)
EventSweepProgress(event_id=27, timestamp=datetime.datetime(2018, 3, 22, 17, 33, 57, 112000), attachment_id=8, run_time=0, reads=None, writes=None, fetches=5, marks=None, access=None)
EventSweepProgress(event_id=28, timestamp=datetime.datetime(2018, 3, 22, 17, 33, 57, 113000), attachment_id=8, run_time=0, reads=2, writes=None, fetches=31, marks=None, access=None)
EventSweepProgress(event_id=29, timestamp=datetime.datetime(2018, 3, 22, 17, 33, 57, 113000), attachment_id=8, run_time=0, reads=6, writes=None, fetches=285, marks=60, access=None)
EventSweepProgress(event_id=30, timestamp=datetime.datetime(2018, 3, 22, 17, 33, 57, 135000), attachment_id=8, run_time=8, reads=2, writes=1, fetches=45, marks=None, access=None)
EventSweepProgress(event_id=31, timestamp=datetime.datetime(2018, 3, 22, 17, 33, 57, 135000), attachment_id=8, run_time=0, reads=3, writes=None, fetches=89, marks=None, access=None)
EventSweepProgress(event_id=32, timestamp=datetime.datetime(2018, 3, 22, 17, 33, 57, 135000), attachment_id=8, run_time=0, reads=3, writes=None, fetches=61, marks=12, access=None)
EventSweepProgress(event_id=33, timestamp=datetime.datetime(2018, 3, 22, 17, 33, 57, 142000), attachment_id=8, run_time=7, reads=2, writes=1, fetches=59, marks=None, access=None)
EventSweepProgress(event_id=34, timestamp=datetime.datetime(2018, 3, 22, 17, 33, 57, 148000), attachment_id=8, run_time=5, reads=3, writes=1, fetches=206, marks=48, access=None)
EventSweepProgress(event_id=35, timestamp=datetime.datetime(2018, 3, 22, 17, 33, 57, 151000), attachment_id=8, run_time=2, reads=2, writes=1, fetches=101, marks=None, access=None)
EventSweepProgress(event_id=36, timestamp=datetime.datetime(2018, 3, 22, 17, 33, 57, 151000), attachment_id=8, run_time=0, reads=2, writes=None, fetches=33, marks=None, access=None)
EventSweepProgress(event_id=37, timestamp=datetime.datetime(2018, 3, 22, 17, 33, 57, 151000), attachment_id=8, run_time=0, reads=2, writes=None, fetches=69, marks=None, access=None)
"""
        self._check_events(trace_lines, output)
    def test_56_sweep_progress_performance(self):
        trace_lines = """2018-03-29T15:23:01.3050 (7035:0x7fde644e4978) SWEEP_PROGRESS
	/opt/firebird/examples/empbuild/employee.fdb (ATT_24, SYSDBA:NONE, NONE, <internal>)
      2 ms, 1 read(s), 11 fetch(es), 2 mark(s)

Table                             Natural     Index    Update    Insert    Delete   Backout     Purge   Expunge
***************************************************************************************************************
RDB$DATABASE                            1                                                           1

2018-03-29T15:23:01.3130 (7035:0x7fde644e4978) SWEEP_PROGRESS
	/opt/firebird/examples/empbuild/employee.fdb (ATT_24, SYSDBA:NONE, NONE, <internal>)
      7 ms, 8 read(s), 436 fetch(es), 9 mark(s)

Table                             Natural     Index    Update    Insert    Delete   Backout     Purge   Expunge
***************************************************************************************************************
RDB$FIELDS                            199                                                                     3

2018-03-29T15:23:01.3150 (7035:0x7fde644e4978) SWEEP_PROGRESS
	/opt/firebird/examples/empbuild/employee.fdb (ATT_24, SYSDBA:NONE, NONE, <internal>)
      1 ms, 4 read(s), 229 fetch(es)

Table                             Natural     Index    Update    Insert    Delete   Backout     Purge   Expunge
***************************************************************************************************************
RDB$INDEX_SEGMENTS                    111

2018-03-29T15:23:01.3150 (7035:0x7fde644e4978) SWEEP_PROGRESS
	/opt/firebird/examples/empbuild/employee.fdb (ATT_24, SYSDBA:NONE, NONE, <internal>)
      0 ms, 3 read(s), 179 fetch(es)

Table                             Natural     Index    Update    Insert    Delete   Backout     Purge   Expunge
***************************************************************************************************************
RDB$INDICES                            87

2018-03-29T15:23:01.3370 (7035:0x7fde644e4978) SWEEP_PROGRESS
	/opt/firebird/examples/empbuild/employee.fdb (ATT_24, SYSDBA:NONE, NONE, <internal>)
     21 ms, 18 read(s), 1 write(s), 927 fetch(es), 21 mark(s)

Table                             Natural     Index    Update    Insert    Delete   Backout     Purge   Expunge
***************************************************************************************************************
RDB$RELATION_FIELDS                   420                                                                     4

2018-03-29T15:23:01.3440 (7035:0x7fde644e4978) SWEEP_PROGRESS
	/opt/firebird/examples/empbuild/employee.fdb (ATT_24, SYSDBA:NONE, NONE, <internal>)
      7 ms, 2 read(s), 1 write(s), 143 fetch(es), 10 mark(s)

Table                             Natural     Index    Update    Insert    Delete   Backout     Purge   Expunge
***************************************************************************************************************
RDB$RELATIONS                          53                                                                     2

2018-03-29T15:23:01.3610 (7035:0x7fde644e4978) SWEEP_PROGRESS
	/opt/firebird/examples/empbuild/employee.fdb (ATT_24, SYSDBA:NONE, NONE, <internal>)
     17 ms, 2 read(s), 1 write(s), 7 fetch(es)

Table                             Natural     Index    Update    Insert    Delete   Backout     Purge   Expunge
***************************************************************************************************************
RDB$VIEW_RELATIONS                      2

2018-03-29T15:23:01.3610 (7035:0x7fde644e4978) SWEEP_PROGRESS
	/opt/firebird/examples/empbuild/employee.fdb (ATT_24, SYSDBA:NONE, NONE, <internal>)
      0 ms, 2 read(s), 25 fetch(es)

Table                             Natural     Index    Update    Insert    Delete   Backout     Purge   Expunge
***************************************************************************************************************
RDB$FORMATS                            11

2018-03-29T15:23:01.3860 (7035:0x7fde644e4978) SWEEP_PROGRESS
	/opt/firebird/examples/empbuild/employee.fdb (ATT_24, SYSDBA:NONE, NONE, <internal>)
     24 ms, 5 read(s), 1 write(s), 94 fetch(es), 4 mark(s)

Table                             Natural     Index    Update    Insert    Delete   Backout     Purge   Expunge
***************************************************************************************************************
RDB$SECURITY_CLASSES                   39                                                                     1

2018-03-29T15:23:01.3940 (7035:0x7fde644e4978) SWEEP_PROGRESS
	/opt/firebird/examples/empbuild/employee.fdb (ATT_24, SYSDBA:NONE, NONE, <internal>)
      7 ms, 6 read(s), 467 fetch(es)

Table                             Natural     Index    Update    Insert    Delete   Backout     Purge   Expunge
***************************************************************************************************************
RDB$TYPES                             228

2018-03-29T15:23:01.3960 (7035:0x7fde644e4978) SWEEP_PROGRESS
	/opt/firebird/examples/empbuild/employee.fdb (ATT_24, SYSDBA:NONE, NONE, <internal>)
      1 ms, 2 read(s), 149 fetch(es)

Table                             Natural     Index    Update    Insert    Delete   Backout     Purge   Expunge
***************************************************************************************************************
RDB$TRIGGERS                           67

2018-03-29T15:23:01.3980 (7035:0x7fde644e4978) SWEEP_PROGRESS
	/opt/firebird/examples/empbuild/employee.fdb (ATT_24, SYSDBA:NONE, NONE, <internal>)
      1 ms, 8 read(s), 341 fetch(es)

Table                             Natural     Index    Update    Insert    Delete   Backout     Purge   Expunge
***************************************************************************************************************
RDB$DEPENDENCIES                      163

2018-03-29T15:23:01.3980 (7035:0x7fde644e4978) SWEEP_PROGRESS
	/opt/firebird/examples/empbuild/employee.fdb (ATT_24, SYSDBA:NONE, NONE, <internal>)
      0 ms, 2 read(s), 7 fetch(es)

Table                             Natural     Index    Update    Insert    Delete   Backout     Purge   Expunge
***************************************************************************************************************
RDB$FUNCTIONS                           2

2018-03-29T15:23:01.3980 (7035:0x7fde644e4978) SWEEP_PROGRESS
	/opt/firebird/examples/empbuild/employee.fdb (ATT_24, SYSDBA:NONE, NONE, <internal>)
      0 ms, 2 read(s), 17 fetch(es)

Table                             Natural     Index    Update    Insert    Delete   Backout     Purge   Expunge
***************************************************************************************************************
RDB$FUNCTION_ARGUMENTS                  7

2018-03-29T15:23:01.3980 (7035:0x7fde644e4978) SWEEP_PROGRESS
	/opt/firebird/examples/empbuild/employee.fdb (ATT_24, SYSDBA:NONE, NONE, <internal>)
      0 ms, 2 read(s), 75 fetch(es)

Table                             Natural     Index    Update    Insert    Delete   Backout     Purge   Expunge
***************************************************************************************************************
RDB$TRIGGER_MESSAGES                   36

2018-03-29T15:23:01.3990 (7035:0x7fde644e4978) SWEEP_PROGRESS
	/opt/firebird/examples/empbuild/employee.fdb (ATT_24, SYSDBA:NONE, NONE, <internal>)
      1 ms, 5 read(s), 305 fetch(es)

Table                             Natural     Index    Update    Insert    Delete   Backout     Purge   Expunge
***************************************************************************************************************
RDB$USER_PRIVILEGES                   148

2018-03-29T15:23:01.4230 (7035:0x7fde644e4978) SWEEP_PROGRESS
	/opt/firebird/examples/empbuild/employee.fdb (ATT_24, SYSDBA:NONE, NONE, <internal>)
      0 ms, 2 read(s), 25 fetch(es)

Table                             Natural     Index    Update    Insert    Delete   Backout     Purge   Expunge
***************************************************************************************************************
RDB$GENERATORS                         11

2018-03-29T15:23:01.4230 (7035:0x7fde644e4978) SWEEP_PROGRESS
	/opt/firebird/examples/empbuild/employee.fdb (ATT_24, SYSDBA:NONE, NONE, <internal>)
      0 ms, 2 read(s), 7 fetch(es)

Table                             Natural     Index    Update    Insert    Delete   Backout     Purge   Expunge
***************************************************************************************************************
RDB$FIELD_DIMENSIONS                    2

2018-03-29T15:23:01.4230 (7035:0x7fde644e4978) SWEEP_PROGRESS
	/opt/firebird/examples/empbuild/employee.fdb (ATT_24, SYSDBA:NONE, NONE, <internal>)
      0 ms, 1 read(s), 165 fetch(es)

Table                             Natural     Index    Update    Insert    Delete   Backout     Purge   Expunge
***************************************************************************************************************
RDB$RELATION_CONSTRAINTS               80

2018-03-29T15:23:01.4230 (7035:0x7fde644e4978) SWEEP_PROGRESS
	/opt/firebird/examples/empbuild/employee.fdb (ATT_24, SYSDBA:NONE, NONE, <internal>)
      0 ms, 2 read(s), 31 fetch(es)

Table                             Natural     Index    Update    Insert    Delete   Backout     Purge   Expunge
***************************************************************************************************************
RDB$REF_CONSTRAINTS                    14

2018-03-29T15:23:01.4290 (7035:0x7fde644e4978) SWEEP_PROGRESS
	/opt/firebird/examples/empbuild/employee.fdb (ATT_24, SYSDBA:NONE, NONE, <internal>)
      5 ms, 1 read(s), 141 fetch(es)

Table                             Natural     Index    Update    Insert    Delete   Backout     Purge   Expunge
***************************************************************************************************************
RDB$CHECK_CONSTRAINTS                  68

2018-03-29T15:23:01.4300 (7035:0x7fde644e4978) SWEEP_PROGRESS
	/opt/firebird/examples/empbuild/employee.fdb (ATT_24, SYSDBA:NONE, NONE, <internal>)
      0 ms, 5 read(s), 29 fetch(es)

Table                             Natural     Index    Update    Insert    Delete   Backout     Purge   Expunge
***************************************************************************************************************
RDB$PROCEDURES                         10

2018-03-29T15:23:01.4300 (7035:0x7fde644e4978) SWEEP_PROGRESS
	/opt/firebird/examples/empbuild/employee.fdb (ATT_24, SYSDBA:NONE, NONE, <internal>)
      0 ms, 2 read(s), 69 fetch(es)

Table                             Natural     Index    Update    Insert    Delete   Backout     Purge   Expunge
***************************************************************************************************************
RDB$PROCEDURE_PARAMETERS               33

2018-03-29T15:23:01.4300 (7035:0x7fde644e4978) SWEEP_PROGRESS
	/opt/firebird/examples/empbuild/employee.fdb (ATT_24, SYSDBA:NONE, NONE, <internal>)
      0 ms, 107 fetch(es)

Table                             Natural     Index    Update    Insert    Delete   Backout     Purge   Expunge
***************************************************************************************************************
RDB$CHARACTER_SETS                     52

2018-03-29T15:23:01.4300 (7035:0x7fde644e4978) SWEEP_PROGRESS
	/opt/firebird/examples/empbuild/employee.fdb (ATT_24, SYSDBA:NONE, NONE, <internal>)
      0 ms, 2 read(s), 303 fetch(es)

Table                             Natural     Index    Update    Insert    Delete   Backout     Purge   Expunge
***************************************************************************************************************
RDB$COLLATIONS                        148

2018-03-29T15:23:01.4310 (7035:0x7fde644e4978) SWEEP_PROGRESS
	/opt/firebird/examples/empbuild/employee.fdb (ATT_24, SYSDBA:NONE, NONE, <internal>)
      0 ms, 2 read(s), 13 fetch(es)

Table                             Natural     Index    Update    Insert    Delete   Backout     Purge   Expunge
***************************************************************************************************************
RDB$EXCEPTIONS                          5

2018-03-29T15:23:01.4310 (7035:0x7fde644e4978) SWEEP_PROGRESS
	/opt/firebird/examples/empbuild/employee.fdb (ATT_24, SYSDBA:NONE, NONE, <internal>)
      0 ms, 5 fetch(es)

Table                             Natural     Index    Update    Insert    Delete   Backout     Purge   Expunge
***************************************************************************************************************
RDB$ROLES                               1

2018-03-29T15:23:01.4310 (7035:0x7fde644e4978) SWEEP_PROGRESS
	/opt/firebird/examples/empbuild/employee.fdb (ATT_24, SYSDBA:NONE, NONE, <internal>)
      0 ms, 2 read(s), 31 fetch(es)

Table                             Natural     Index    Update    Insert    Delete   Backout     Purge   Expunge
***************************************************************************************************************
COUNTRY                                14

2018-03-29T15:23:01.4310 (7035:0x7fde644e4978) SWEEP_PROGRESS
	/opt/firebird/examples/empbuild/employee.fdb (ATT_24, SYSDBA:NONE, NONE, <internal>)
      0 ms, 4 read(s), 69 fetch(es)

Table                             Natural     Index    Update    Insert    Delete   Backout     Purge   Expunge
***************************************************************************************************************
JOB                                    31

2018-03-29T15:23:01.4310 (7035:0x7fde644e4978) SWEEP_PROGRESS
	/opt/firebird/examples/empbuild/employee.fdb (ATT_24, SYSDBA:NONE, NONE, <internal>)
      0 ms, 2 read(s), 45 fetch(es)

Table                             Natural     Index    Update    Insert    Delete   Backout     Purge   Expunge
***************************************************************************************************************
DEPARTMENT                             21

2018-03-29T15:23:01.4310 (7035:0x7fde644e4978) SWEEP_PROGRESS
	/opt/firebird/examples/empbuild/employee.fdb (ATT_24, SYSDBA:NONE, NONE, <internal>)
      0 ms, 3 read(s), 89 fetch(es)

Table                             Natural     Index    Update    Insert    Delete   Backout     Purge   Expunge
***************************************************************************************************************
EMPLOYEE                               42

2018-03-29T15:23:01.4310 (7035:0x7fde644e4978) SWEEP_PROGRESS
	/opt/firebird/examples/empbuild/employee.fdb (ATT_24, SYSDBA:NONE, NONE, <internal>)
      0 ms, 2 read(s), 15 fetch(es)

Table                             Natural     Index    Update    Insert    Delete   Backout     Purge   Expunge
***************************************************************************************************************
PROJECT                                 6

2018-03-29T15:23:01.4310 (7035:0x7fde644e4978) SWEEP_PROGRESS
	/opt/firebird/examples/empbuild/employee.fdb (ATT_24, SYSDBA:NONE, NONE, <internal>)
      0 ms, 2 read(s), 59 fetch(es)

Table                             Natural     Index    Update    Insert    Delete   Backout     Purge   Expunge
***************************************************************************************************************
EMPLOYEE_PROJECT                       28

2018-03-29T15:23:01.4320 (7035:0x7fde644e4978) SWEEP_PROGRESS
	/opt/firebird/examples/empbuild/employee.fdb (ATT_24, SYSDBA:NONE, NONE, <internal>)
      0 ms, 2 read(s), 51 fetch(es)

Table                             Natural     Index    Update    Insert    Delete   Backout     Purge   Expunge
***************************************************************************************************************
PROJ_DEPT_BUDGET                       24

2018-03-29T15:23:01.4320 (7035:0x7fde644e4978) SWEEP_PROGRESS
	/opt/firebird/examples/empbuild/employee.fdb (ATT_24, SYSDBA:NONE, NONE, <internal>)
      0 ms, 2 read(s), 101 fetch(es)

Table                             Natural     Index    Update    Insert    Delete   Backout     Purge   Expunge
***************************************************************************************************************
SALARY_HISTORY                         49

2018-03-29T15:23:01.4320 (7035:0x7fde644e4978) SWEEP_PROGRESS
	/opt/firebird/examples/empbuild/employee.fdb (ATT_24, SYSDBA:NONE, NONE, <internal>)
      0 ms, 2 read(s), 33 fetch(es)

Table                             Natural     Index    Update    Insert    Delete   Backout     Purge   Expunge
***************************************************************************************************************
CUSTOMER                               15

2018-03-29T15:23:01.4320 (7035:0x7fde644e4978) SWEEP_PROGRESS
	/opt/firebird/examples/empbuild/employee.fdb (ATT_24, SYSDBA:NONE, NONE, <internal>)
      0 ms, 2 read(s), 69 fetch(es)

Table                             Natural     Index    Update    Insert    Delete   Backout     Purge   Expunge
***************************************************************************************************************
SALES                                  33
"""
        output = """AttachmentInfo(attachment_id=24, database='/opt/firebird/examples/empbuild/employee.fdb', charset='NONE', protocol='<internal>', address='<internal>', user='SYSDBA', role='NONE', remote_process=None, remote_pid=None)
EventSweepProgress(event_id=1, timestamp=datetime.datetime(2018, 3, 29, 15, 23, 1, 305000), attachment_id=24, run_time=2, reads=1, writes=None, fetches=11, marks=2, access=[AccessStats(table='RDB$DATABASE', natural=1, index=0, update=0, insert=0, delete=0, backout=0, purge=1, expunge=0)])
EventSweepProgress(event_id=2, timestamp=datetime.datetime(2018, 3, 29, 15, 23, 1, 313000), attachment_id=24, run_time=7, reads=8, writes=None, fetches=436, marks=9, access=[AccessStats(table='RDB$FIELDS', natural=199, index=0, update=0, insert=0, delete=0, backout=0, purge=0, expunge=3)])
EventSweepProgress(event_id=3, timestamp=datetime.datetime(2018, 3, 29, 15, 23, 1, 315000), attachment_id=24, run_time=1, reads=4, writes=None, fetches=229, marks=None, access=[AccessStats(table='RDB$INDEX_SEGMENTS', natural=111, index=0, update=0, insert=0, delete=0, backout=0, purge=0, expunge=0)])
EventSweepProgress(event_id=4, timestamp=datetime.datetime(2018, 3, 29, 15, 23, 1, 315000), attachment_id=24, run_time=0, reads=3, writes=None, fetches=179, marks=None, access=[AccessStats(table='RDB$INDICES', natural=87, index=0, update=0, insert=0, delete=0, backout=0, purge=0, expunge=0)])
EventSweepProgress(event_id=5, timestamp=datetime.datetime(2018, 3, 29, 15, 23, 1, 337000), attachment_id=24, run_time=21, reads=18, writes=1, fetches=927, marks=21, access=[AccessStats(table='RDB$RELATION_FIELDS', natural=420, index=0, update=0, insert=0, delete=0, backout=0, purge=0, expunge=4)])
EventSweepProgress(event_id=6, timestamp=datetime.datetime(2018, 3, 29, 15, 23, 1, 344000), attachment_id=24, run_time=7, reads=2, writes=1, fetches=143, marks=10, access=[AccessStats(table='RDB$RELATIONS', natural=53, index=0, update=0, insert=0, delete=0, backout=0, purge=0, expunge=2)])
EventSweepProgress(event_id=7, timestamp=datetime.datetime(2018, 3, 29, 15, 23, 1, 361000), attachment_id=24, run_time=17, reads=2, writes=1, fetches=7, marks=None, access=[AccessStats(table='RDB$VIEW_RELATIONS', natural=2, index=0, update=0, insert=0, delete=0, backout=0, purge=0, expunge=0)])
EventSweepProgress(event_id=8, timestamp=datetime.datetime(2018, 3, 29, 15, 23, 1, 361000), attachment_id=24, run_time=0, reads=2, writes=None, fetches=25, marks=None, access=[AccessStats(table='RDB$FORMATS', natural=11, index=0, update=0, insert=0, delete=0, backout=0, purge=0, expunge=0)])
EventSweepProgress(event_id=9, timestamp=datetime.datetime(2018, 3, 29, 15, 23, 1, 386000), attachment_id=24, run_time=24, reads=5, writes=1, fetches=94, marks=4, access=[AccessStats(table='RDB$SECURITY_CLASSES', natural=39, index=0, update=0, insert=0, delete=0, backout=0, purge=0, expunge=1)])
EventSweepProgress(event_id=10, timestamp=datetime.datetime(2018, 3, 29, 15, 23, 1, 394000), attachment_id=24, run_time=7, reads=6, writes=None, fetches=467, marks=None, access=[AccessStats(table='RDB$TYPES', natural=228, index=0, update=0, insert=0, delete=0, backout=0, purge=0, expunge=0)])
EventSweepProgress(event_id=11, timestamp=datetime.datetime(2018, 3, 29, 15, 23, 1, 396000), attachment_id=24, run_time=1, reads=2, writes=None, fetches=149, marks=None, access=[AccessStats(table='RDB$TRIGGERS', natural=67, index=0, update=0, insert=0, delete=0, backout=0, purge=0, expunge=0)])
EventSweepProgress(event_id=12, timestamp=datetime.datetime(2018, 3, 29, 15, 23, 1, 398000), attachment_id=24, run_time=1, reads=8, writes=None, fetches=341, marks=None, access=[AccessStats(table='RDB$DEPENDENCIES', natural=163, index=0, update=0, insert=0, delete=0, backout=0, purge=0, expunge=0)])
EventSweepProgress(event_id=13, timestamp=datetime.datetime(2018, 3, 29, 15, 23, 1, 398000), attachment_id=24, run_time=0, reads=2, writes=None, fetches=7, marks=None, access=[AccessStats(table='RDB$FUNCTIONS', natural=2, index=0, update=0, insert=0, delete=0, backout=0, purge=0, expunge=0)])
EventSweepProgress(event_id=14, timestamp=datetime.datetime(2018, 3, 29, 15, 23, 1, 398000), attachment_id=24, run_time=0, reads=2, writes=None, fetches=17, marks=None, access=[AccessStats(table='RDB$FUNCTION_ARGUMENTS', natural=7, index=0, update=0, insert=0, delete=0, backout=0, purge=0, expunge=0)])
EventSweepProgress(event_id=15, timestamp=datetime.datetime(2018, 3, 29, 15, 23, 1, 398000), attachment_id=24, run_time=0, reads=2, writes=None, fetches=75, marks=None, access=[AccessStats(table='RDB$TRIGGER_MESSAGES', natural=36, index=0, update=0, insert=0, delete=0, backout=0, purge=0, expunge=0)])
EventSweepProgress(event_id=16, timestamp=datetime.datetime(2018, 3, 29, 15, 23, 1, 399000), attachment_id=24, run_time=1, reads=5, writes=None, fetches=305, marks=None, access=[AccessStats(table='RDB$USER_PRIVILEGES', natural=148, index=0, update=0, insert=0, delete=0, backout=0, purge=0, expunge=0)])
EventSweepProgress(event_id=17, timestamp=datetime.datetime(2018, 3, 29, 15, 23, 1, 423000), attachment_id=24, run_time=0, reads=2, writes=None, fetches=25, marks=None, access=[AccessStats(table='RDB$GENERATORS', natural=11, index=0, update=0, insert=0, delete=0, backout=0, purge=0, expunge=0)])
EventSweepProgress(event_id=18, timestamp=datetime.datetime(2018, 3, 29, 15, 23, 1, 423000), attachment_id=24, run_time=0, reads=2, writes=None, fetches=7, marks=None, access=[AccessStats(table='RDB$FIELD_DIMENSIONS', natural=2, index=0, update=0, insert=0, delete=0, backout=0, purge=0, expunge=0)])
EventSweepProgress(event_id=19, timestamp=datetime.datetime(2018, 3, 29, 15, 23, 1, 423000), attachment_id=24, run_time=0, reads=1, writes=None, fetches=165, marks=None, access=[AccessStats(table='RDB$RELATION_CONSTRAINTS', natural=80, index=0, update=0, insert=0, delete=0, backout=0, purge=0, expunge=0)])
EventSweepProgress(event_id=20, timestamp=datetime.datetime(2018, 3, 29, 15, 23, 1, 423000), attachment_id=24, run_time=0, reads=2, writes=None, fetches=31, marks=None, access=[AccessStats(table='RDB$REF_CONSTRAINTS', natural=14, index=0, update=0, insert=0, delete=0, backout=0, purge=0, expunge=0)])
EventSweepProgress(event_id=21, timestamp=datetime.datetime(2018, 3, 29, 15, 23, 1, 429000), attachment_id=24, run_time=5, reads=1, writes=None, fetches=141, marks=None, access=[AccessStats(table='RDB$CHECK_CONSTRAINTS', natural=68, index=0, update=0, insert=0, delete=0, backout=0, purge=0, expunge=0)])
EventSweepProgress(event_id=22, timestamp=datetime.datetime(2018, 3, 29, 15, 23, 1, 430000), attachment_id=24, run_time=0, reads=5, writes=None, fetches=29, marks=None, access=[AccessStats(table='RDB$PROCEDURES', natural=10, index=0, update=0, insert=0, delete=0, backout=0, purge=0, expunge=0)])
EventSweepProgress(event_id=23, timestamp=datetime.datetime(2018, 3, 29, 15, 23, 1, 430000), attachment_id=24, run_time=0, reads=2, writes=None, fetches=69, marks=None, access=[AccessStats(table='RDB$PROCEDURE_PARAMETERS', natural=33, index=0, update=0, insert=0, delete=0, backout=0, purge=0, expunge=0)])
EventSweepProgress(event_id=24, timestamp=datetime.datetime(2018, 3, 29, 15, 23, 1, 430000), attachment_id=24, run_time=0, reads=None, writes=None, fetches=107, marks=None, access=[AccessStats(table='RDB$CHARACTER_SETS', natural=52, index=0, update=0, insert=0, delete=0, backout=0, purge=0, expunge=0)])
EventSweepProgress(event_id=25, timestamp=datetime.datetime(2018, 3, 29, 15, 23, 1, 430000), attachment_id=24, run_time=0, reads=2, writes=None, fetches=303, marks=None, access=[AccessStats(table='RDB$COLLATIONS', natural=148, index=0, update=0, insert=0, delete=0, backout=0, purge=0, expunge=0)])
EventSweepProgress(event_id=26, timestamp=datetime.datetime(2018, 3, 29, 15, 23, 1, 431000), attachment_id=24, run_time=0, reads=2, writes=None, fetches=13, marks=None, access=[AccessStats(table='RDB$EXCEPTIONS', natural=5, index=0, update=0, insert=0, delete=0, backout=0, purge=0, expunge=0)])
EventSweepProgress(event_id=27, timestamp=datetime.datetime(2018, 3, 29, 15, 23, 1, 431000), attachment_id=24, run_time=0, reads=None, writes=None, fetches=5, marks=None, access=[AccessStats(table='RDB$ROLES', natural=1, index=0, update=0, insert=0, delete=0, backout=0, purge=0, expunge=0)])
EventSweepProgress(event_id=28, timestamp=datetime.datetime(2018, 3, 29, 15, 23, 1, 431000), attachment_id=24, run_time=0, reads=2, writes=None, fetches=31, marks=None, access=[AccessStats(table='COUNTRY', natural=14, index=0, update=0, insert=0, delete=0, backout=0, purge=0, expunge=0)])
EventSweepProgress(event_id=29, timestamp=datetime.datetime(2018, 3, 29, 15, 23, 1, 431000), attachment_id=24, run_time=0, reads=4, writes=None, fetches=69, marks=None, access=[AccessStats(table='JOB', natural=31, index=0, update=0, insert=0, delete=0, backout=0, purge=0, expunge=0)])
EventSweepProgress(event_id=30, timestamp=datetime.datetime(2018, 3, 29, 15, 23, 1, 431000), attachment_id=24, run_time=0, reads=2, writes=None, fetches=45, marks=None, access=[AccessStats(table='DEPARTMENT', natural=21, index=0, update=0, insert=0, delete=0, backout=0, purge=0, expunge=0)])
EventSweepProgress(event_id=31, timestamp=datetime.datetime(2018, 3, 29, 15, 23, 1, 431000), attachment_id=24, run_time=0, reads=3, writes=None, fetches=89, marks=None, access=[AccessStats(table='EMPLOYEE', natural=42, index=0, update=0, insert=0, delete=0, backout=0, purge=0, expunge=0)])
EventSweepProgress(event_id=32, timestamp=datetime.datetime(2018, 3, 29, 15, 23, 1, 431000), attachment_id=24, run_time=0, reads=2, writes=None, fetches=15, marks=None, access=[AccessStats(table='PROJECT', natural=6, index=0, update=0, insert=0, delete=0, backout=0, purge=0, expunge=0)])
EventSweepProgress(event_id=33, timestamp=datetime.datetime(2018, 3, 29, 15, 23, 1, 431000), attachment_id=24, run_time=0, reads=2, writes=None, fetches=59, marks=None, access=[AccessStats(table='EMPLOYEE_PROJECT', natural=28, index=0, update=0, insert=0, delete=0, backout=0, purge=0, expunge=0)])
EventSweepProgress(event_id=34, timestamp=datetime.datetime(2018, 3, 29, 15, 23, 1, 432000), attachment_id=24, run_time=0, reads=2, writes=None, fetches=51, marks=None, access=[AccessStats(table='PROJ_DEPT_BUDGET', natural=24, index=0, update=0, insert=0, delete=0, backout=0, purge=0, expunge=0)])
EventSweepProgress(event_id=35, timestamp=datetime.datetime(2018, 3, 29, 15, 23, 1, 432000), attachment_id=24, run_time=0, reads=2, writes=None, fetches=101, marks=None, access=[AccessStats(table='SALARY_HISTORY', natural=49, index=0, update=0, insert=0, delete=0, backout=0, purge=0, expunge=0)])
EventSweepProgress(event_id=36, timestamp=datetime.datetime(2018, 3, 29, 15, 23, 1, 432000), attachment_id=24, run_time=0, reads=2, writes=None, fetches=33, marks=None, access=[AccessStats(table='CUSTOMER', natural=15, index=0, update=0, insert=0, delete=0, backout=0, purge=0, expunge=0)])
EventSweepProgress(event_id=37, timestamp=datetime.datetime(2018, 3, 29, 15, 23, 1, 432000), attachment_id=24, run_time=0, reads=2, writes=None, fetches=69, marks=None, access=[AccessStats(table='SALES', natural=33, index=0, update=0, insert=0, delete=0, backout=0, purge=0, expunge=0)])
"""
        self._check_events(trace_lines, output)
    def test_57_sweep_finish(self):
        trace_lines = """2018-03-22T17:33:57.2270 (12351:0x7f0174bdd978) SWEEP_FINISH
	/opt/firebird/examples/empbuild/employee.fdb (ATT_8, SYSDBA:NONE, NONE, <internal>)

Transaction counters:
	Oldest interesting        156
	Oldest active             156
	Oldest snapshot           156
	Next transaction          157
    257 ms, 177 read(s), 30 write(s), 8279 fetch(es), 945 mark(s)

"""
        output = """AttachmentInfo(attachment_id=8, database='/opt/firebird/examples/empbuild/employee.fdb', charset='NONE', protocol='<internal>', address='<internal>', user='SYSDBA', role='NONE', remote_process=None, remote_pid=None)
EventSweepFinish(event_id=1, timestamp=datetime.datetime(2018, 3, 22, 17, 33, 57, 227000), attachment_id=8, oit=156, oat=156, ost=156, next=157, run_time=257, reads=177, writes=30, fetches=8279, marks=945, access=None)
"""
        self._check_events(trace_lines, output)
    def test_58_sweep_finish(self):
        trace_lines = """2018-03-22T17:33:57.2270 (12351:0x7f0174bdd978) SWEEP_FAILED
	/opt/firebird/examples/empbuild/employee.fdb (ATT_8, SYSDBA:NONE, NONE, <internal>)
"""
        output = """AttachmentInfo(attachment_id=8, database='/opt/firebird/examples/empbuild/employee.fdb', charset='NONE', protocol='<internal>', address='<internal>', user='SYSDBA', role='NONE', remote_process=None, remote_pid=None)
EventSweepFailed(event_id=1, timestamp=datetime.datetime(2018, 3, 22, 17, 33, 57, 227000), attachment_id=8)
"""
        self._check_events(trace_lines, output)
    def test_59_blr_compile(self):
        trace_lines = """2018-04-03T17:00:43.4270 (9772:0x7f2c5004b978) COMPILE_BLR
	/home/data/db/employee.fdb (ATT_5, SYSDBA:NONE, NONE, TCPv4:127.0.0.1)
	/bin/python:9737
-------------------------------------------------------------------------------
   0 blr_version5,
   1 blr_begin,
   2    blr_message, 0, 4,0,
   6       blr_varying2, 0,0, 15,0,
  11       blr_varying2, 0,0, 10,0,
  16       blr_short, 0,
  18       blr_short, 0,
  20    blr_loop,
  21       blr_receive, 0,
  23          blr_store,
  24             blr_relation, 7, 'C','O','U','N','T','R','Y', 0,
  34             blr_begin,
  35                blr_assignment,
  36                   blr_parameter2, 0, 0,0, 2,0,
  42                   blr_field, 0, 7, 'C','O','U','N','T','R','Y',
  52                blr_assignment,
  53                   blr_parameter2, 0, 1,0, 3,0,
  59                   blr_field, 0, 8, 'C','U','R','R','E','N','C','Y',
  70                blr_end,
  71    blr_end,
  72 blr_eoc

      0 ms

2018-04-03T17:00:43.4270 (9772:0x7f2c5004b978) COMPILE_BLR
	/home/data/db/employee.fdb (ATT_5, SYSDBA:NONE, NONE, TCPv4:127.0.0.1)
	/bin/python:9737
-------------------------------------------------------------------------------
   0 blr_version5,
   1 blr_begin,
   2    blr_message, 0, 4,0,
   6       blr_varying2, 0,0, 15,0,
  11       blr_varying2, 0,0, 10,0,
  16       blr_short, 0
...
      0 ms

2018-04-03T17:00:43.4270 (9772:0x7f2c5004b978) COMPILE_BLR
	/home/data/db/employee.fdb (ATT_5, SYSDBA:NONE, NONE, TCPv4:127.0.0.1)
	/bin/python:9737

Statement 22:
      0 ms
"""
        output = """AttachmentInfo(attachment_id=5, database='/home/data/db/employee.fdb', charset='NONE', protocol='TCPv4', address='127.0.0.1', user='SYSDBA', role='NONE', remote_process='/bin/python', remote_pid=9737)
EventBLRCompile(event_id=1, timestamp=datetime.datetime(2018, 4, 3, 17, 0, 43, 427000), status=<Status.OK: ' '>, attachment_id=5, statement_id=None, content="0 blr_version5,\\n1 blr_begin,\\n2    blr_message, 0, 4,0,\\n6       blr_varying2, 0,0, 15,0,\\n11       blr_varying2, 0,0, 10,0,\\n16       blr_short, 0,\\n18       blr_short, 0,\\n20    blr_loop,\\n21       blr_receive, 0,\\n23          blr_store,\\n24             blr_relation, 7, 'C','O','U','N','T','R','Y', 0,\\n34             blr_begin,\\n35                blr_assignment,\\n36                   blr_parameter2, 0, 0,0, 2,0,\\n42                   blr_field, 0, 7, 'C','O','U','N','T','R','Y',\\n52                blr_assignment,\\n53                   blr_parameter2, 0, 1,0, 3,0,\\n59                   blr_field, 0, 8, 'C','U','R','R','E','N','C','Y',\\n70                blr_end,\\n71    blr_end,\\n72 blr_eoc", prepare_time=0)
EventBLRCompile(event_id=2, timestamp=datetime.datetime(2018, 4, 3, 17, 0, 43, 427000), status=<Status.OK: ' '>, attachment_id=5, statement_id=None, content='0 blr_version5,\\n1 blr_begin,\\n2    blr_message, 0, 4,0,\\n6       blr_varying2, 0,0, 15,0,\\n11       blr_varying2, 0,0, 10,0,\\n16       blr_short, 0\\n...', prepare_time=0)
EventBLRCompile(event_id=3, timestamp=datetime.datetime(2018, 4, 3, 17, 0, 43, 427000), status=<Status.OK: ' '>, attachment_id=5, statement_id=22, content=None, prepare_time=0)
"""
        self._check_events(trace_lines, output)
    def test_60_blr_execute(self):
        trace_lines = """2018-04-03T17:00:43.4280 (9772:0x7f2c5004b978) EXECUTE_BLR
	/home/data/db/employee.fdb (ATT_5, SYSDBA:NONE, NONE, TCPv4:127.0.0.1)
	/home/job/python/envs/pyfirebird/bin/python:9737
		(TRA_9, CONCURRENCY | NOWAIT | READ_WRITE)
-------------------------------------------------------------------------------
   0 blr_version5,
   1 blr_begin,
   2    blr_message, 0, 4,0,
   6       blr_varying2, 0,0, 15,0,
  11       blr_varying2, 0,0, 10,0,
  16       blr_short, 0,
  18       blr_short, 0,
  20    blr_loop,
  21       blr_receive, 0,
  23          blr_store,
  24             blr_relation, 7, 'C','O','U','N','T','R','Y', 0,
  34             blr_begin,
  35                blr_assignment,
  36                   blr_parameter2, 0, 0,0, 2,0,
  42                   blr_field, 0, 7, 'C','O','U','N','T','R','Y',
  52                blr_assignment,
  53                   blr_parameter2, 0, 1,0, 3,0,
  59                   blr_field, 0, 8, 'C','U','R','R','E','N','C','Y',
  70                blr_end,
  71    blr_end,
  72 blr_eoc

      0 ms, 3 read(s), 7 fetch(es), 5 mark(s)

Table                             Natural     Index    Update    Insert    Delete   Backout     Purge   Expunge
***************************************************************************************************************
COUNTRY                                                               1

2018-04-03T17:00:43.4280 (9772:0x7f2c5004b978) EXECUTE_BLR
	/home/data/db/employee.fdb (ATT_5, SYSDBA:NONE, NONE, TCPv4:127.0.0.1)
	/home/job/python/envs/pyfirebird/bin/python:9737
		(TRA_9, CONCURRENCY | NOWAIT | READ_WRITE)
-------------------------------------------------------------------------------
   0 blr_version5,
   1 blr_begin,
   2    blr_message, 0, 4,0,
   6       blr_varying2, 0,0, 15,0,
  11       blr_varying2, 0,0, 10,0,
  16       blr_short, 0,
  18       blr_short, 0...
      0 ms, 3 read(s), 7 fetch(es), 5 mark(s)

Table                             Natural     Index    Update    Insert    Delete   Backout     Purge   Expunge
***************************************************************************************************************
COUNTRY                                                               1

2018-04-03T17:00:43.4280 (9772:0x7f2c5004b978) EXECUTE_BLR
	/home/data/db/employee.fdb (ATT_5, SYSDBA:NONE, NONE, TCPv4:127.0.0.1)
	/home/job/python/envs/pyfirebird/bin/python:9737
		(TRA_9, CONCURRENCY | NOWAIT | READ_WRITE)
Statement 22:
      0 ms, 3 read(s), 7 fetch(es), 5 mark(s)
"""
        output = """AttachmentInfo(attachment_id=5, database='/home/data/db/employee.fdb', charset='NONE', protocol='TCPv4', address='127.0.0.1', user='SYSDBA', role='NONE', remote_process='/home/job/python/envs/pyfirebird/bin/python', remote_pid=9737)
TransactionInfo(attachment_id=5, transaction_id=9, initial_id=None, options=['CONCURRENCY', 'NOWAIT', 'READ_WRITE'])
EventBLRExecute(event_id=1, timestamp=datetime.datetime(2018, 4, 3, 17, 0, 43, 428000), status=<Status.OK: ' '>, attachment_id=5, transaction_id=9, statement_id=None, content="0 blr_version5,\\n1 blr_begin,\\n2    blr_message, 0, 4,0,\\n6       blr_varying2, 0,0, 15,0,\\n11       blr_varying2, 0,0, 10,0,\\n16       blr_short, 0,\\n18       blr_short, 0,\\n20    blr_loop,\\n21       blr_receive, 0,\\n23          blr_store,\\n24             blr_relation, 7, 'C','O','U','N','T','R','Y', 0,\\n34             blr_begin,\\n35                blr_assignment,\\n36                   blr_parameter2, 0, 0,0, 2,0,\\n42                   blr_field, 0, 7, 'C','O','U','N','T','R','Y',\\n52                blr_assignment,\\n53                   blr_parameter2, 0, 1,0, 3,0,\\n59                   blr_field, 0, 8, 'C','U','R','R','E','N','C','Y',\\n70                blr_end,\\n71    blr_end,\\n72 blr_eoc", run_time=0, reads=3, writes=None, fetches=7, marks=5, access=[AccessStats(table='COUNTRY', natural=0, index=0, update=0, insert=1, delete=0, backout=0, purge=0, expunge=0)])
EventBLRExecute(event_id=2, timestamp=datetime.datetime(2018, 4, 3, 17, 0, 43, 428000), status=<Status.OK: ' '>, attachment_id=5, transaction_id=9, statement_id=None, content='0 blr_version5,\\n1 blr_begin,\\n2    blr_message, 0, 4,0,\\n6       blr_varying2, 0,0, 15,0,\\n11       blr_varying2, 0,0, 10,0,\\n16       blr_short, 0,\\n18       blr_short, 0...', run_time=0, reads=3, writes=None, fetches=7, marks=5, access=[AccessStats(table='COUNTRY', natural=0, index=0, update=0, insert=1, delete=0, backout=0, purge=0, expunge=0)])
EventBLRExecute(event_id=3, timestamp=datetime.datetime(2018, 4, 3, 17, 0, 43, 428000), status=<Status.OK: ' '>, attachment_id=5, transaction_id=9, statement_id=22, content=None, run_time=0, reads=3, writes=None, fetches=7, marks=5, access=None)
"""
        self._check_events(trace_lines, output)
    def test_61_dyn_execute(self):
        trace_lines = """2018-04-03T17:42:53.5590 (10474:0x7f0d8b4f0978) EXECUTE_DYN
	/opt/firebird/examples/empbuild/employee.fdb (ATT_40, SYSDBA:NONE, NONE, <internal>)
		(TRA_221, CONCURRENCY | WAIT | READ_WRITE)
-------------------------------------------------------------------------------
   0 gds__dyn_version_1,
   1    gds__dyn_delete_rel, 1,0, 'T',
   5       gds__dyn_end,
   0 gds__dyn_eoc
     20 ms
2018-04-03T17:43:21.3650 (10474:0x7f0d8b4f0978) EXECUTE_DYN
	/opt/firebird/examples/empbuild/employee.fdb (ATT_40, SYSDBA:NONE, NONE, <internal>)
		(TRA_222, CONCURRENCY | WAIT | READ_WRITE)
-------------------------------------------------------------------------------
   0 gds__dyn_version_1,
   1    gds__dyn_begin,
   2       gds__dyn_def_local_fld, 31,0, 'C','O','U','N','T','R','Y',32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,
  36          gds__dyn_fld_source, 31,0, 'C','O','U','N','T','R','Y','N','A','M','E',32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,
  70          gds__dyn_rel_name, 1,0, 'T',
  74          gds__dyn_fld_position, 2,0, 0,0,
  79          gds__dyn_update_flag, 2,0, 1,0,
  84          gds__dyn_system_flag, 2,0, 0,0,
  89          gds__dyn_end,
  90       gds__dyn_def_sql_fld, 31,0, 'C','U','R','R','E','N','C','Y',32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,
 124          gds__dyn_fld_type, 2,0, 37,0,
 129          gds__dyn_fld_length, 2,0, 10,0,
 134          gds__dyn_fld_scale, 2,0, 0,0,
 139          gds__dyn_rel_name, 1,0, 'T',
 143          gds__dyn_fld_position, 2,0, 1,0,
 148          gds__dyn_update_flag, 2,0, 1,0,
 153          gds__dyn_system_flag, 2,0, 0,0,
 158          gds__dyn_end,
 159       gds__dyn_end,
   0 gds__dyn_eoc
      0 ms
2018-03-29T13:28:45.8910 (5265:0x7f71ed580978) EXECUTE_DYN
	/opt/firebird/examples/empbuild/employee.fdb (ATT_20, SYSDBA:NONE, NONE, <internal>)
		(TRA_189, CONCURRENCY | WAIT | READ_WRITE)
     26 ms
"""
        output = """AttachmentInfo(attachment_id=40, database='/opt/firebird/examples/empbuild/employee.fdb', charset='NONE', protocol='<internal>', address='<internal>', user='SYSDBA', role='NONE', remote_process=None, remote_pid=None)
TransactionInfo(attachment_id=40, transaction_id=221, initial_id=None, options=['CONCURRENCY', 'WAIT', 'READ_WRITE'])
EventDYNExecute(event_id=1, timestamp=datetime.datetime(2018, 4, 3, 17, 42, 53, 559000), status=<Status.OK: ' '>, attachment_id=40, transaction_id=221, content="0 gds__dyn_version_1,\\n1    gds__dyn_delete_rel, 1,0, 'T',\\n5       gds__dyn_end,\\n0 gds__dyn_eoc", run_time=20)
TransactionInfo(attachment_id=40, transaction_id=222, initial_id=None, options=['CONCURRENCY', 'WAIT', 'READ_WRITE'])
EventDYNExecute(event_id=2, timestamp=datetime.datetime(2018, 4, 3, 17, 43, 21, 365000), status=<Status.OK: ' '>, attachment_id=40, transaction_id=222, content="0 gds__dyn_version_1,\\n1    gds__dyn_begin,\\n2       gds__dyn_def_local_fld, 31,0, 'C','O','U','N','T','R','Y',32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,\\n36          gds__dyn_fld_source, 31,0, 'C','O','U','N','T','R','Y','N','A','M','E',32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,\\n70          gds__dyn_rel_name, 1,0, 'T',\\n74          gds__dyn_fld_position, 2,0, 0,0,\\n79          gds__dyn_update_flag, 2,0, 1,0,\\n84          gds__dyn_system_flag, 2,0, 0,0,\\n89          gds__dyn_end,\\n90       gds__dyn_def_sql_fld, 31,0, 'C','U','R','R','E','N','C','Y',32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,\\n124          gds__dyn_fld_type, 2,0, 37,0,\\n129          gds__dyn_fld_length, 2,0, 10,0,\\n134          gds__dyn_fld_scale, 2,0, 0,0,\\n139          gds__dyn_rel_name, 1,0, 'T',\\n143          gds__dyn_fld_position, 2,0, 1,0,\\n148          gds__dyn_update_flag, 2,0, 1,0,\\n153          gds__dyn_system_flag, 2,0, 0,0,\\n158          gds__dyn_end,\\n159       gds__dyn_end,\\n0 gds__dyn_eoc", run_time=0)
AttachmentInfo(attachment_id=20, database='/opt/firebird/examples/empbuild/employee.fdb', charset='NONE', protocol='<internal>', address='<internal>', user='SYSDBA', role='NONE', remote_process=None, remote_pid=None)
TransactionInfo(attachment_id=20, transaction_id=189, initial_id=None, options=['CONCURRENCY', 'WAIT', 'READ_WRITE'])
EventDYNExecute(event_id=3, timestamp=datetime.datetime(2018, 3, 29, 13, 28, 45, 891000), status=<Status.OK: ' '>, attachment_id=20, transaction_id=189, content=None, run_time=26)
"""
        self._check_events(trace_lines, output)
    def test_62_unknown(self):
        # It could be an event unknown to trace plugin (case 1), or completelly new event unknown to trace parser (case 2)
        trace_lines = """2014-05-23T11:00:28.5840 (3720:0000000000EFD9E8) Unknown event in ATTACH_DATABASE
	/home/employee.fdb (ATT_8, SYSDBA:NONE, ISO88591, TCPv4:192.168.1.5)
	/opt/firebird/bin/isql:8723

2018-03-22T10:06:59.5090 (4992:0x7f92a22a4978) EVENT_FROM_THE_FUTURE
This event may contain
various information
which could span
multiple lines.

Yes, it could be very long!
"""
        output = """EventUnknown(event_id=1, timestamp=datetime.datetime(2014, 5, 23, 11, 0, 28, 584000), data='Unknown event in ATTACH_DATABASE\\n/home/employee.fdb (ATT_8, SYSDBA:NONE, ISO88591, TCPv4:192.168.1.5)\\n/opt/firebird/bin/isql:8723')
EventUnknown(event_id=2, timestamp=datetime.datetime(2018, 3, 22, 10, 6, 59, 509000), data='EVENT_FROM_THE_FUTURE\\nThis event may contain\\nvarious information\\nwhich could span\\nmultiple lines.\\nYes, it could be very long!')
"""
        self._check_events(trace_lines, output)
