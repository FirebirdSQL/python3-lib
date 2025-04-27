# SPDX-FileCopyrightText: 2020-present The Firebird Projects <www.firebirdsql.org>
#
# SPDX-License-Identifier: MIT
#
# PROGRAM/MODULE: firebird-lib
# FILE:           tests/test_log.py
# DESCRIPTION:    Tests for firebird.lib.log module
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

"""firebird-lib - Tests for firebird.lib.log module
"""

import pytest
from collections.abc import Sized, MutableSequence, Mapping
from re import finditer
from io import StringIO
from firebird.lib.log import *

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

def _parse_log_lines(log_lines: str) -> str:
    """Parses log lines using LogParser.parse and returns string representation."""
    output_io = StringIO()
    parser = LogParser()
    # Handle potential StopIteration if log_lines is empty
    try:
        for obj in parser.parse(linesplit_iter(log_lines)):
            print(str(obj), file=output_io, end='\n') # Ensure newline
    except StopIteration:
        pass # No events parsed from empty input
    return output_io.getvalue()

def _push_log_lines(log_lines: str) -> str:
    """Parses log lines using LogParser.push and returns string representation."""
    output_io = StringIO()
    parser = LogParser()
    for line in linesplit_iter(log_lines):
        events = parser.push(line)
        if events: # push might return a list or a single event
            if not isinstance(events, list):
                events = [events]
            for event in events:
                print(str(event), file=output_io, end='\n') # Ensure newline
    # Process any remaining buffered lines
    final_events = parser.push(STOP)
    if final_events:
        if not isinstance(final_events, list):
            final_events = [final_events]
        for event in final_events:
            print(str(event), file=output_io, end='\n') # Ensure newline
    return output_io.getvalue()

# --- Test Functions ---

def test_01_win_fb2_with_unknown():
    """Tests parsing a log fragment from Windows FB 2.x with unknown host messages."""
    data = """

SRVDB1  Tue Apr 04 21:25:40 2017
        INET/inet_error: read errno = 10054


SRVDB1  Tue Apr 04 21:25:41 2017
        Unable to complete network request to host "SRVDB1".
        Error reading data from the connection.


SRVDB1  Tue Apr 04 21:25:42 2017
        INET/inet_error: read errno = 10054


SRVDB1  Tue Apr 04 21:25:43 2017
        Unable to complete network request to host "SRVDB1".
        Error reading data from the connection.


SRVDB1  Tue Apr 04 21:28:48 2017
        INET/inet_error: read errno = 10054


SRVDB1  Tue Apr 04 21:28:50 2017
        Unable to complete network request to host "SRVDB1".
        Error reading data from the connection.


SRVDB1  Tue Apr 04 21:28:51 2017
        Sweep is started by SYSDBA
        Database "Mydatabase"
        OIT 551120654, OAT 551120655, OST 551120655, Next 551121770


SRVDB1  Tue Apr 04 21:28:52 2017
        INET/inet_error: read errno = 10054


SRVDB1  Tue Apr 04 21:28:53 2017
        Unable to complete network request to host "SRVDB1".
        Error reading data from the connection.


SRVDB1  Tue Apr 04 21:28:54 2017
        Sweep is finished
        Database "Mydatabase"
        OIT 551234848, OAT 551234849, OST 551234849, Next 551235006


SRVDB1  Tue Apr 04 21:28:55 2017
        Sweep is started by SWEEPER
        Database "Mydatabase"
        OIT 551243753, OAT 551846279, OST 551846279, Next 551846385


SRVDB1  Tue Apr 04 21:28:56 2017
        INET/inet_error: read errno = 10054


SRVDB1  Tue Apr 04 21:28:57 2017
        Sweep is finished
        Database "Mydatabase"
        OIT 551846278, OAT 551976724, OST 551976724, Next 551976730


SRVDB1  Tue Apr 04 21:28:58 2017
        Unable to complete network request to host "(unknown)".
        Error reading data from the connection.


SRVDB1  Thu Apr 06 12:52:56 2017
        Shutting down the server with 1 active connection(s) to 1 database(s), 0 active service(s)


"""
    expected_output = """LogMessage(origin='SRVDB1', timestamp=datetime.datetime(2017, 4, 4, 21, 25, 40), level=<Severity.ERROR: 3>, code=177, facility=<Facility.NET: 10>, message='INET/inet_error: {error} errno = {err_code}', params={'error': 'read', 'err_code': 10054})
LogMessage(origin='SRVDB1', timestamp=datetime.datetime(2017, 4, 4, 21, 25, 41), level=<Severity.UNKNOWN: 0>, code=0, facility=<Facility.UNKNOWN: 0>, message='Unable to complete network request to host "SRVDB1".\\nError reading data from the connection.', params={})
LogMessage(origin='SRVDB1', timestamp=datetime.datetime(2017, 4, 4, 21, 25, 42), level=<Severity.ERROR: 3>, code=177, facility=<Facility.NET: 10>, message='INET/inet_error: {error} errno = {err_code}', params={'error': 'read', 'err_code': 10054})
LogMessage(origin='SRVDB1', timestamp=datetime.datetime(2017, 4, 4, 21, 25, 43), level=<Severity.UNKNOWN: 0>, code=0, facility=<Facility.UNKNOWN: 0>, message='Unable to complete network request to host "SRVDB1".\\nError reading data from the connection.', params={})
LogMessage(origin='SRVDB1', timestamp=datetime.datetime(2017, 4, 4, 21, 28, 48), level=<Severity.ERROR: 3>, code=177, facility=<Facility.NET: 10>, message='INET/inet_error: {error} errno = {err_code}', params={'error': 'read', 'err_code': 10054})
LogMessage(origin='SRVDB1', timestamp=datetime.datetime(2017, 4, 4, 21, 28, 50), level=<Severity.UNKNOWN: 0>, code=0, facility=<Facility.UNKNOWN: 0>, message='Unable to complete network request to host "SRVDB1".\\nError reading data from the connection.', params={})
LogMessage(origin='SRVDB1', timestamp=datetime.datetime(2017, 4, 4, 21, 28, 51), level=<Severity.INFO: 1>, code=126, facility=<Facility.SWEEP: 7>, message='Sweep is started by {user}\\nDatabase "{database}"\\nOIT {oit}, OAT {oat}, OST {ost}, Next {next}', params={'user': 'SYSDBA', 'database': 'Mydatabase', 'oit': 551120654, 'oat': 551120655, 'ost': 551120655, 'next': 551121770})
LogMessage(origin='SRVDB1', timestamp=datetime.datetime(2017, 4, 4, 21, 28, 52), level=<Severity.ERROR: 3>, code=177, facility=<Facility.NET: 10>, message='INET/inet_error: {error} errno = {err_code}', params={'error': 'read', 'err_code': 10054})
LogMessage(origin='SRVDB1', timestamp=datetime.datetime(2017, 4, 4, 21, 28, 53), level=<Severity.UNKNOWN: 0>, code=0, facility=<Facility.UNKNOWN: 0>, message='Unable to complete network request to host "SRVDB1".\\nError reading data from the connection.', params={})
LogMessage(origin='SRVDB1', timestamp=datetime.datetime(2017, 4, 4, 21, 28, 54), level=<Severity.INFO: 1>, code=127, facility=<Facility.SWEEP: 7>, message='Sweep is finished\\nDatabase "{database}"\\nOIT {oit}, OAT {oat}, OST {ost}, Next {next}', params={'database': 'Mydatabase', 'oit': 551234848, 'oat': 551234849, 'ost': 551234849, 'next': 551235006})
LogMessage(origin='SRVDB1', timestamp=datetime.datetime(2017, 4, 4, 21, 28, 55), level=<Severity.INFO: 1>, code=126, facility=<Facility.SWEEP: 7>, message='Sweep is started by {user}\\nDatabase "{database}"\\nOIT {oit}, OAT {oat}, OST {ost}, Next {next}', params={'user': 'SWEEPER', 'database': 'Mydatabase', 'oit': 551243753, 'oat': 551846279, 'ost': 551846279, 'next': 551846385})
LogMessage(origin='SRVDB1', timestamp=datetime.datetime(2017, 4, 4, 21, 28, 56), level=<Severity.ERROR: 3>, code=177, facility=<Facility.NET: 10>, message='INET/inet_error: {error} errno = {err_code}', params={'error': 'read', 'err_code': 10054})
LogMessage(origin='SRVDB1', timestamp=datetime.datetime(2017, 4, 4, 21, 28, 57), level=<Severity.INFO: 1>, code=127, facility=<Facility.SWEEP: 7>, message='Sweep is finished\\nDatabase "{database}"\\nOIT {oit}, OAT {oat}, OST {ost}, Next {next}', params={'database': 'Mydatabase', 'oit': 551846278, 'oat': 551976724, 'ost': 551976724, 'next': 551976730})
LogMessage(origin='SRVDB1', timestamp=datetime.datetime(2017, 4, 4, 21, 28, 58), level=<Severity.UNKNOWN: 0>, code=0, facility=<Facility.UNKNOWN: 0>, message='Unable to complete network request to host "(unknown)".\\nError reading data from the connection.', params={})
LogMessage(origin='SRVDB1', timestamp=datetime.datetime(2017, 4, 6, 12, 52, 56), level=<Severity.WARNING: 2>, code=73, facility=<Facility.SYSTEM: 1>, message='Shutting down the server with {con_count} active connection(s) to {db_count} database(s), {svc_count} active service(s)', params={'con_count': 1, 'db_count': 1, 'svc_count': 0})
"""
    # Using strip() to handle potential trailing newline differences
    parsed_output = _parse_log_lines(data)
    assert parsed_output.strip() == expected_output.strip(), "PARSE: Parsed events do not match expected ones"

    pushed_output = _push_log_lines(data)
    assert pushed_output.strip() == expected_output.strip(), "PUSH: Parsed events do not match expected ones"

def test_02_lin_fb3():
    """Tests parsing a log fragment from Linux FB 3.x with guardian messages."""
    data = """
MyServer (Client)	Fri Apr  6 16:35:46 2018
	INET/inet_error: connect errno = 111


MyServer (Client)	Fri Apr  6 16:51:31 2018
	/opt/firebird/bin/fbguard: guardian starting /opt/firebird/bin/fbserver



MyServer (Server)	Fri Apr  6 16:55:23 2018
	activating shadow file /home/db/test_employee.fdb


MyServer (Server)	Fri Apr  6 16:55:31 2018
	Sweep is started by SYSDBA
	Database "/home/db/test_employee.fdb"
	OIT 1, OAT 0, OST 0, Next 1


MyServer (Server)	Fri Apr  6 16:55:31 2018
	Sweep is finished
	Database "/home/db/test_employee.fdb"
	OIT 1, OAT 0, OST 0, Next 2


MyServer (Client)	Fri Apr  6 20:18:52 2018
	/opt/firebird/bin/fbguard: /opt/firebird/bin/fbserver normal shutdown.



MyServer (Client)	Mon Apr  9 08:28:29 2018
	/opt/firebird/bin/fbguard: guardian starting /opt/firebird/bin/fbserver



MyServer (Server)	Tue Apr 17 15:01:27 2018
	INET/inet_error: invalid socket in packet_receive errno = 22


MyServer (Client)	Tue Apr 17 19:42:55 2018
	/opt/firebird/bin/fbguard: /opt/firebird/bin/fbserver normal shutdown.



"""
    expected_output = """LogMessage(origin='MyServer (Client)', timestamp=datetime.datetime(2018, 4, 6, 16, 35, 46), level=<Severity.ERROR: 3>, code=177, facility=<Facility.NET: 10>, message='INET/inet_error: {error} errno = {err_code}', params={'error': 'connect', 'err_code': 111})
LogMessage(origin='MyServer (Client)', timestamp=datetime.datetime(2018, 4, 6, 16, 51, 31), level=<Severity.INFO: 1>, code=151, facility=<Facility.GUARDIAN: 9>, message='{prog_name}: guardian starting {value}', params={'prog_name': '/opt/firebird/bin/fbguard', 'value': '/opt/firebird/bin/fbserver'})
LogMessage(origin='MyServer (Server)', timestamp=datetime.datetime(2018, 4, 6, 16, 55, 23), level=<Severity.INFO: 1>, code=124, facility=<Facility.SYSTEM: 1>, message='activating shadow file {shadow}', params={'shadow': '/home/db/test_employee.fdb'})
LogMessage(origin='MyServer (Server)', timestamp=datetime.datetime(2018, 4, 6, 16, 55, 31), level=<Severity.INFO: 1>, code=126, facility=<Facility.SWEEP: 7>, message='Sweep is started by {user}\\nDatabase "{database}"\\nOIT {oit}, OAT {oat}, OST {ost}, Next {next}', params={'user': 'SYSDBA', 'database': '/home/db/test_employee.fdb', 'oit': 1, 'oat': 0, 'ost': 0, 'next': 1})
LogMessage(origin='MyServer (Server)', timestamp=datetime.datetime(2018, 4, 6, 16, 55, 31), level=<Severity.INFO: 1>, code=127, facility=<Facility.SWEEP: 7>, message='Sweep is finished\\nDatabase "{database}"\\nOIT {oit}, OAT {oat}, OST {ost}, Next {next}', params={'database': '/home/db/test_employee.fdb', 'oit': 1, 'oat': 0, 'ost': 0, 'next': 2})
LogMessage(origin='MyServer (Client)', timestamp=datetime.datetime(2018, 4, 6, 20, 18, 52), level=<Severity.INFO: 1>, code=162, facility=<Facility.GUARDIAN: 9>, message='{prog_name}: {process_name} normal shutdown.', params={'prog_name': '/opt/firebird/bin/fbguard', 'process_name': '/opt/firebird/bin/fbserver'})
LogMessage(origin='MyServer (Client)', timestamp=datetime.datetime(2018, 4, 9, 8, 28, 29), level=<Severity.INFO: 1>, code=151, facility=<Facility.GUARDIAN: 9>, message='{prog_name}: guardian starting {value}', params={'prog_name': '/opt/firebird/bin/fbguard', 'value': '/opt/firebird/bin/fbserver'})
LogMessage(origin='MyServer (Server)', timestamp=datetime.datetime(2018, 4, 17, 15, 1, 27), level=<Severity.ERROR: 3>, code=177, facility=<Facility.NET: 10>, message='INET/inet_error: {error} errno = {err_code}', params={'error': 'invalid socket in packet_receive', 'err_code': 22})
LogMessage(origin='MyServer (Client)', timestamp=datetime.datetime(2018, 4, 17, 19, 42, 55), level=<Severity.INFO: 1>, code=162, facility=<Facility.GUARDIAN: 9>, message='{prog_name}: {process_name} normal shutdown.', params={'prog_name': '/opt/firebird/bin/fbguard', 'process_name': '/opt/firebird/bin/fbserver'})
"""
    parsed_output = _parse_log_lines(data)
    assert parsed_output.strip() == expected_output.strip(), "PARSE: Parsed events do not match expected ones"

    pushed_output = _push_log_lines(data)
    assert pushed_output.strip() == expected_output.strip(), "PUSH: Parsed events do not match expected ones"

def test_03_lin_fb3_validation_auth_etc():
    """Tests parsing a log fragment from Linux FB 3.x with various messages."""
    data = """
ultron	Sun Oct 28 15:25:54 2018
	/opt/firebird/bin/fbguard: guardian starting /opt/firebird/bin/firebird



ultron	Sun Oct 28 15:29:42 2018
	/opt/firebird/bin/fbguard: /opt/firebird/bin/firebird terminated



ultron	Wed Oct 31 13:47:44 2018
	REMOTE INTERFACE/gds__detach: Unsuccesful detach from database.
	Uncommitted work may have been lost.
	Error writing data to the connection.


ultron	Wed Nov 14 03:32:44 2018
	INET/inet_error: read errno = 104, client host = Terminal, address = 192.168.1.243/55120, user = frodo



ultron	Fri Dec  7 09:53:53 2018
	Authentication error
	No matching plugins on server



ultron	Sun Jun  9 17:26:09 2019
	INET/INET_connect: getaddrinfo(ocalhost,gds_db) failed: Neznámé jméno nebo služba



ultron	Thu Jun 13 07:32:51 2019
	Database: /usr/local/data/mydb.FDB
	Validation started


ultron	Thu Jun 13 07:36:41 2019
	Database: /usr/local/data/mydb.FDB
	Warning: Page 3867207 is an orphan


ultron	Thu Jun 13 07:36:41 2019
	Database: /usr/local/data/mydb.FDB
	Validation finished: 0 errors, 663 warnings, 663 fixed


"""
    expected_output = """LogMessage(origin='ultron', timestamp=datetime.datetime(2018, 10, 28, 15, 25, 54), level=<Severity.INFO: 1>, code=151, facility=<Facility.GUARDIAN: 9>, message='{prog_name}: guardian starting {value}', params={'prog_name': '/opt/firebird/bin/fbguard', 'value': '/opt/firebird/bin/firebird'})
LogMessage(origin='ultron', timestamp=datetime.datetime(2018, 10, 28, 15, 29, 42), level=<Severity.INFO: 1>, code=157, facility=<Facility.GUARDIAN: 9>, message='{prog_name}: {process_name} terminated', params={'prog_name': '/opt/firebird/bin/fbguard', 'process_name': '/opt/firebird/bin/firebird'})
LogMessage(origin='ultron', timestamp=datetime.datetime(2018, 10, 31, 13, 47, 44), level=<Severity.ERROR: 3>, code=284, facility=<Facility.SYSTEM: 1>, message='REMOTE INTERFACE/gds__detach: Unsuccesful detach from database.\\nUncommitted work may have been lost.\\n{err_msg}', params={'err_msg': 'Error writing data to the connection.'})
LogMessage(origin='ultron', timestamp=datetime.datetime(2018, 11, 14, 3, 32, 44), level=<Severity.ERROR: 3>, code=177, facility=<Facility.NET: 10>, message='INET/inet_error: {error} errno = {err_code}, {parameters}', params={'error': 'read', 'err_code': 104, 'parameters': 'client host = Terminal, address = 192.168.1.243/55120, user = frodo'})
LogMessage(origin='ultron', timestamp=datetime.datetime(2018, 12, 7, 9, 53, 53), level=<Severity.ERROR: 3>, code=185, facility=<Facility.AUTH: 11>, message='Authentication error\\nNo matching plugins on server', params={})
LogMessage(origin='ultron', timestamp=datetime.datetime(2019, 6, 9, 17, 26, 9), level=<Severity.ERROR: 3>, code=163, facility=<Facility.NET: 10>, message='INET/INET_connect: getaddrinfo({host},{protocol}) failed: {error}', params={'host': 'ocalhost', 'protocol': 'gds_db', 'error': 'Neznámé jméno nebo služba'})
LogMessage(origin='ultron', timestamp=datetime.datetime(2019, 6, 13, 7, 32, 51), level=<Severity.INFO: 1>, code=75, facility=<Facility.VALIDATION: 6>, message='Database: {database}\\nValidation started', params={'database': '/usr/local/data/mydb.FDB'})
LogMessage(origin='ultron', timestamp=datetime.datetime(2019, 6, 13, 7, 36, 41), level=<Severity.WARNING: 2>, code=81, facility=<Facility.VALIDATION: 6>, message='Database: {database}\\nWarning: Page {page_num} is an orphan', params={'database': '/usr/local/data/mydb.FDB', 'page_num': 3867207})
LogMessage(origin='ultron', timestamp=datetime.datetime(2019, 6, 13, 7, 36, 41), level=<Severity.INFO: 1>, code=76, facility=<Facility.VALIDATION: 6>, message='Database: {database}\\nValidation finished: {errors} errors, {warnings} warnings, {fixed} fixed', params={'database': '/usr/local/data/mydb.FDB', 'errors': 0, 'warnings': 663, 'fixed': 663})
"""
    parsed_output = _parse_log_lines(data)
    assert parsed_output.strip() == expected_output.strip(), "PARSE: Parsed events do not match expected ones"

    pushed_output = _push_log_lines(data)
    assert pushed_output.strip() == expected_output.strip(), "PUSH: Parsed events do not match expected ones"

def test_04_parse_entry_malformed_header():
    parser = LogParser()
    malformed_entry = ["MyOrigin Invalid Date String 2023", "  Continuation line"]
    with pytest.raises(Error, match="Malformed log entry"):
        parser.parse_entry(malformed_entry)

def test_05_push_malformed_header_error():
    parser = LogParser()
    lines = ["MyOrigin Invalid Date String 2023"]
    # Need to ensure this specific line bypasses push's own date check if possible,
    # or structure the test data carefully. This might be tricky as push
    # might treat it as a continuation. A direct parse_entry test is safer.
    # If push treats it as continuation, test the result after STOP.
    parser.push(lines[0])
    with pytest.raises(Error, match="Malformed log entry"):
        parser.push(STOP) # Error likely occurs here when parse_entry is called

def test_06_parse_empty_input():
    parser = LogParser()
    lines = []
    with pytest.raises(Error, match="Malformed log entry"):
        list(parser.parse(lines))

def test_07_push_state_transitions():
    parser = LogParser()
    # 1. Push header - should return None
    line1 = "Origin1 Tue Apr 04 21:25:40 2017"
    assert parser.push(line1) is None
    assert len(parser._LogParser__buffer) == 1 # Access internal for verification

    # 2. Push continuation - should return None
    line2 = "  Continuation line 1"
    assert parser.push(line2) is None
    assert len(parser._LogParser__buffer) == 2

    # 3. Push blank line (often ignored or handled by iterators, but test push)
    line3 = "   "
    assert parser.push(line3) is None # Blank lines are appended if buffer not empty
    assert len(parser._LogParser__buffer) == 3
    assert parser._LogParser__buffer[-1] == "" # After strip

    # 4. Push new header - should return the *first* parsed message
    line4 = "Origin2 Wed Apr 05 10:00:00 2017"
    msg1 = parser.push(line4)
    assert isinstance(msg1, LogMessage)
    assert msg1.origin == "Origin1"
    assert "Continuation line 1" in msg1.message
    assert len(parser._LogParser__buffer) == 1 # Buffer now holds line4
    assert parser._LogParser__buffer[0] == line4

    # 5. Push continuation - should return None
    line2 = "  Continuation line 2"
    assert parser.push(line2) is None
    assert len(parser._LogParser__buffer) == 2

    # 6. Push blank line (often ignored or handled by iterators, but test push)
    line3 = "   "
    assert parser.push(line3) is None # Blank lines are appended if buffer not empty
    assert len(parser._LogParser__buffer) == 3
    assert parser._LogParser__buffer[-1] == "" # After strip

    # 7. Push STOP - should return the second parsed message
    msg2 = parser.push(STOP)
    assert isinstance(msg2, LogMessage)
    assert msg2.origin == "Origin2"
    assert "Continuation line 2" in msg2.message
    assert len(parser._LogParser__buffer) == 0 # Buffer cleared

def test_08_log_message_frozen():
    ts = datetime.now()
    msg = LogMessage("origin", ts, Severity.INFO, 1, Facility.SYSTEM, "Test", {})
    with pytest.raises(AttributeError):
        msg.origin = "new_origin"
