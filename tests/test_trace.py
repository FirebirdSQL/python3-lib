# SPDX-FileCopyrightText: 2020-present The Firebird Projects <www.firebirdsql.org>
#
# SPDX-License-Identifier: MIT
#
# PROGRAM/MODULE: firebird-lib
# FILE:           tests/test_trace.py
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

"""firebird-lib - Tests for firebird.lib.trace module
"""

import pytest
from collections.abc import Sized, MutableSequence, Mapping
from re import finditer
from io import StringIO
from firebird.lib.trace import *

# --- Constants ---
FB30 = '3.0'
FB40 = '4.0'
FB50 = '5.0'

# --- Helper Functions ---

def linesplit_iter(string):
    """Iterates over lines in a string, handling different line endings."""
    # Add handling for potential None groups if string ends exactly with \n
    return (m.group(2) or m.group(3) or ''
            for m in finditer('((.*)\n|(.+)$)', string))

def iter_obj_properties(obj):
    """Iterator function for object properties."""
    for varname in dir(obj):
        if hasattr(type(obj), varname) and isinstance(getattr(type(obj), varname), property):
            yield varname

def iter_obj_variables(obj):
    """Iterator function for object variables (non-callable, non-private)."""
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

# --- Test Helper Functions ---

def _parse_trace_lines(trace_lines: str) -> str:
    """Parses trace lines using TraceParser.parse and returns string representation."""
    output_io = StringIO()
    parser = TraceParser()
    for obj in parser.parse(linesplit_iter(trace_lines)):
        print(str(obj), file=output_io, end='\n') # Ensure newline
    return output_io.getvalue()

def _push_trace_lines(trace_lines: str) -> str:
    """Parses trace lines using TraceParser.push and returns string representation."""
    output_io = StringIO()
    parser = TraceParser()
    for line in linesplit_iter(trace_lines):
        if events:= parser.push(line):
            for event in events:
                print(str(event), file=output_io, end='\n') # Ensure newline
    if events:= parser.push(STOP):
        for event in events:
            print(str(event), file=output_io, end='\n') # Ensure newline
    return output_io.getvalue()

def _check_events(trace_lines, expected_output):
    """Helper to run both parse and push checks."""
    # Using strip() to handle potential trailing newline differences
    parsed_output = _parse_trace_lines(trace_lines)
    assert parsed_output.strip() == expected_output.strip(), "PARSE: Parsed events do not match expected ones"

    pushed_output = _push_trace_lines(trace_lines)
    assert pushed_output.strip() == expected_output.strip(), "PUSH: Parsed events do not match expected ones"

# --- Test Functions ---

def test_00_linesplit_iter():
    """Tests the line splitting iterator helper."""
    trace_lines = """2014-05-23T11:00:28.5840 (3720:0000000000EFD9E8) ATTACH_DATABASE
        /home/employee.fdb (ATT_8, SYSDBA:NONE, ISO88591, TCPv4:192.168.1.5)
        /opt/firebird/bin/isql:8723

"""
    output_io = StringIO()
    for line in linesplit_iter(trace_lines):
        output_io.write(line + '\n')
    assert output_io.getvalue() == trace_lines # Use regular assert

def test_01_trace_init():
    """Tests parsing of TRACE_INIT event."""
    trace_lines = """2014-05-23T11:00:28.5840 (3720:0000000000EFD9E8) TRACE_INIT
        SESSION_1

"""
    output = "EventTraceInit(event_id=1, timestamp=datetime.datetime(2014, 5, 23, 11, 0, 28, 584000), session_name='SESSION_1')\n"
    _check_events(trace_lines, output)

def test_02_trace_suspend():
    """Tests parsing of trace suspend message."""
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
    _check_events(trace_lines, output)

def test_03_trace_finish():
    """Tests parsing of TRACE_FINI event."""
    trace_lines = """2014-05-23T11:00:28.5840 (3720:0000000000EFD9E8) TRACE_INIT
        SESSION_1

2014-05-23T11:01:24.8080 (3720:0000000000EFD9E8) TRACE_FINI
	SESSION_1

"""
    output = """EventTraceInit(event_id=1, timestamp=datetime.datetime(2014, 5, 23, 11, 0, 28, 584000), session_name='SESSION_1')
EventTraceFinish(event_id=2, timestamp=datetime.datetime(2014, 5, 23, 11, 1, 24, 808000), session_name='SESSION_1')
"""
    _check_events(trace_lines, output)

def test_04_create_database():
    """Tests parsing of CREATE_DATABASE event."""
    trace_lines = """2018-03-29T14:20:55.1180 (6290:0x7f9bb00bb978) CREATE_DATABASE
	/home/employee.fdb (ATT_8, SYSDBA:NONE, ISO88591, TCPv4:192.168.1.5)
	/opt/firebird/bin/isql:8723

"""
    output = """EventCreate(event_id=1, timestamp=datetime.datetime(2018, 3, 29, 14, 20, 55, 118000), status=<Status.OK: ' '>, attachment_id=8, database='/home/employee.fdb', charset='ISO88591', protocol='TCPv4', address='192.168.1.5', user='SYSDBA', role='NONE', remote_process='/opt/firebird/bin/isql', remote_pid=8723)
"""
    _check_events(trace_lines, output)

def test_05_drop_database():
    """Tests parsing of DROP_DATABASE event."""
    trace_lines = """2018-03-29T14:20:55.1180 (6290:0x7f9bb00bb978) DROP_DATABASE
	/home/employee.fdb (ATT_8, SYSDBA:NONE, ISO88591, TCPv4:192.168.1.5)
	/opt/firebird/bin/isql:8723

"""
    output = """EventDrop(event_id=1, timestamp=datetime.datetime(2018, 3, 29, 14, 20, 55, 118000), status=<Status.OK: ' '>, attachment_id=8, database='/home/employee.fdb', charset='ISO88591', protocol='TCPv4', address='192.168.1.5', user='SYSDBA', role='NONE', remote_process='/opt/firebird/bin/isql', remote_pid=8723)
"""
    _check_events(trace_lines, output)

def test_06_attach():
    """Tests parsing of a successful ATTACH_DATABASE event."""
    trace_lines = """2014-05-23T11:00:28.5840 (3720:0000000000EFD9E8) ATTACH_DATABASE
	/home/employee.fdb (ATT_8, SYSDBA:NONE, ISO88591, TCPv4:192.168.1.5)
	/opt/firebird/bin/isql:8723
"""
    output = """EventAttach(event_id=1, timestamp=datetime.datetime(2014, 5, 23, 11, 0, 28, 584000), status=<Status.OK: ' '>, attachment_id=8, database='/home/employee.fdb', charset='ISO88591', protocol='TCPv4', address='192.168.1.5', user='SYSDBA', role='NONE', remote_process='/opt/firebird/bin/isql', remote_pid=8723)
"""
    _check_events(trace_lines, output)

def test_07_attach_failed():
    """Tests parsing of a FAILED ATTACH_DATABASE event."""
    trace_lines = """2014-05-23T11:00:28.5840 (3720:0000000000EFD9E8) FAILED ATTACH_DATABASE
	/home/employee.fdb (ATT_8, SYSDBA:NONE, ISO88591, TCPv4:192.168.1.5)
	/opt/firebird/bin/isql:8723

"""
    output = """EventAttach(event_id=1, timestamp=datetime.datetime(2014, 5, 23, 11, 0, 28, 584000), status=<Status.FAILED: 'F'>, attachment_id=8, database='/home/employee.fdb', charset='ISO88591', protocol='TCPv4', address='192.168.1.5', user='SYSDBA', role='NONE', remote_process='/opt/firebird/bin/isql', remote_pid=8723)
"""
    _check_events(trace_lines, output)

def test_08_unauthorized_attach():
    """Tests parsing of an UNAUTHORIZED ATTACH_DATABASE event."""
    trace_lines = """2014-09-24T14:46:15.0350 (2453:0x7fed02a04910) UNAUTHORIZED ATTACH_DATABASE
	/home/employee.fdb (ATT_0, sysdba, NONE, TCPv4:127.0.0.1)
	/opt/firebird/bin/isql:8723

"""
    output = """EventAttach(event_id=1, timestamp=datetime.datetime(2014, 9, 24, 14, 46, 15, 35000), status=<Status.UNAUTHORIZED: 'U'>, attachment_id=0, database='/home/employee.fdb', charset='NONE', protocol='TCPv4', address='127.0.0.1', user='sysdba', role='NONE', remote_process='/opt/firebird/bin/isql', remote_pid=8723)
"""
    _check_events(trace_lines, output)

def test_09_detach():
    """Tests parsing of DETACH_DATABASE event following an attach."""
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
    _check_events(trace_lines, output)

def test_10_detach_without_attach():
    """Tests parsing DETACH_DATABASE when no prior ATTACH was seen in the trace fragment."""
    trace_lines = """2014-05-23T11:01:24.8080 (3720:0000000000EFD9E8) DETACH_DATABASE
	/home/employee.fdb (ATT_8, SYSDBA:NONE, ISO88591, TCPv4:192.168.1.5)
	/opt/firebird/bin/isql:8723

"""
    # Note: The parser implicitly creates an AttachmentInfo
    output = """EventDetach(event_id=1, timestamp=datetime.datetime(2014, 5, 23, 11, 1, 24, 808000), status=<Status.OK: ' '>, attachment_id=8, database='/home/employee.fdb', charset='ISO88591', protocol='TCPv4', address='192.168.1.5', user='SYSDBA', role='NONE', remote_process='/opt/firebird/bin/isql', remote_pid=8723)
"""
    _check_events(trace_lines, output)

# --- Add the rest of the test functions (test_11_start_transaction to test_62_unknown) ---
# --- following the same pattern: define trace_lines, define output, call _check_events ---

def test_11_start_transaction():
    """Tests parsing of START_TRANSACTION event."""
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
    _check_events(trace_lines, output)

def test_12_start_transaction_without_attachment():
    """Tests parsing START_TRANSACTION when no prior ATTACH was seen."""
    trace_lines = """2014-05-23T11:00:28.6160 (3720:0000000000EFD9E8) START_TRANSACTION
	/home/employee.fdb (ATT_8, SYSDBA:NONE, ISO88591, TCPv4:192.168.1.5)
	/opt/firebird/bin/isql:8723
		(TRA_1568, READ_COMMITTED | REC_VERSION | WAIT | READ_WRITE)

"""
    # Note: The parser implicitly creates an AttachmentInfo
    output = """AttachmentInfo(attachment_id=8, database='/home/employee.fdb', charset='ISO88591', protocol='TCPv4', address='192.168.1.5', user='SYSDBA', role='NONE', remote_process='/opt/firebird/bin/isql', remote_pid=8723)
EventTransactionStart(event_id=1, timestamp=datetime.datetime(2014, 5, 23, 11, 0, 28, 616000), status=<Status.OK: ' '>, attachment_id=8, transaction_id=1568, options=['READ_COMMITTED', 'REC_VERSION', 'WAIT', 'READ_WRITE'])
"""
    _check_events(trace_lines, output)

def test_13_commit():
    """Tests parsing of COMMIT_TRANSACTION event with performance info."""
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
    _check_events(trace_lines, output)

# ... (Continue converting tests 14 through 62 in the same way) ...

def test_62_unknown():
    """Tests parsing of unknown events."""
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
    _check_events(trace_lines, output)
