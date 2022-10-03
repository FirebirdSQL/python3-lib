#coding:utf-8
#
# PROGRAM/MODULE: firebird-lib
# FILE:           test_log.py
# DESCRIPTION:    Unit tests for firebird.lib.log
# CREATED:        8.10.2020
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

"""firebird-lib - Unit tests for firebird.lib.log


"""

import unittest
import sys, os
from collections.abc import Sized, MutableSequence, Mapping
from re import finditer
from io import StringIO
from firebird.driver import *
from firebird.lib.log import *

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

class TestLogParser(TestBase):
    def setUp(self):
        super().setUp()
        self.dbfile = os.path.join(self.dbpath, self.FBTEST_DB)
        self.maxDiff = None
    def _check_events(self, log_lines, output):
        self.output = StringIO()
        parser = LogParser()
        for obj in parser.parse(linesplit_iter(log_lines)):
            self.printout(str(obj))
        self.assertEqual(self.output.getvalue(), output, "PARSE: Parsed events do not match expected ones")
        self._push_check_events(log_lines, output)
        self.output.close()
    def _push_check_events(self, log_lines, output):
        self.output = StringIO()
        parser = LogParser()
        for line in linesplit_iter(log_lines):
            if event := parser.push(line):
                self.printout(str(event))
        if event := parser.push(STOP):
            self.printout(str(event))
        self.assertEqual(self.output.getvalue(), output, "PUSH: Parsed events do not match expected ones")
        self.output.close()
    def test_01_win_fb2_with_unknown(self):
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
        output = """LogMessage(origin='SRVDB1', timestamp=datetime.datetime(2017, 4, 4, 21, 25, 40), level=<Severity.ERROR: 3>, code=177, facility=<Facility.NET: 10>, message='INET/inet_error: {error} errno = {err_code}', params={'error': 'read', 'err_code': 10054})
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
        self._check_events(data, output)
    def test_02_lin_fb3(self):
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
        output = """LogMessage(origin='MyServer (Client)', timestamp=datetime.datetime(2018, 4, 6, 16, 35, 46), level=<Severity.ERROR: 3>, code=177, facility=<Facility.NET: 10>, message='INET/inet_error: {error} errno = {err_code}', params={'error': 'connect', 'err_code': 111})
LogMessage(origin='MyServer (Client)', timestamp=datetime.datetime(2018, 4, 6, 16, 51, 31), level=<Severity.INFO: 1>, code=151, facility=<Facility.GUARDIAN: 9>, message='{prog_name}: guardian starting {value}', params={'prog_name': '/opt/firebird/bin/fbguard', 'value': '/opt/firebird/bin/fbserver'})
LogMessage(origin='MyServer (Server)', timestamp=datetime.datetime(2018, 4, 6, 16, 55, 23), level=<Severity.INFO: 1>, code=124, facility=<Facility.SYSTEM: 1>, message='activating shadow file {shadow}', params={'shadow': '/home/db/test_employee.fdb'})
LogMessage(origin='MyServer (Server)', timestamp=datetime.datetime(2018, 4, 6, 16, 55, 31), level=<Severity.INFO: 1>, code=126, facility=<Facility.SWEEP: 7>, message='Sweep is started by {user}\\nDatabase "{database}"\\nOIT {oit}, OAT {oat}, OST {ost}, Next {next}', params={'user': 'SYSDBA', 'database': '/home/db/test_employee.fdb', 'oit': 1, 'oat': 0, 'ost': 0, 'next': 1})
LogMessage(origin='MyServer (Server)', timestamp=datetime.datetime(2018, 4, 6, 16, 55, 31), level=<Severity.INFO: 1>, code=127, facility=<Facility.SWEEP: 7>, message='Sweep is finished\\nDatabase "{database}"\\nOIT {oit}, OAT {oat}, OST {ost}, Next {next}', params={'database': '/home/db/test_employee.fdb', 'oit': 1, 'oat': 0, 'ost': 0, 'next': 2})
LogMessage(origin='MyServer (Client)', timestamp=datetime.datetime(2018, 4, 6, 20, 18, 52), level=<Severity.INFO: 1>, code=162, facility=<Facility.GUARDIAN: 9>, message='{prog_name}: {process_name} normal shutdown.', params={'prog_name': '/opt/firebird/bin/fbguard', 'process_name': '/opt/firebird/bin/fbserver'})
LogMessage(origin='MyServer (Client)', timestamp=datetime.datetime(2018, 4, 9, 8, 28, 29), level=<Severity.INFO: 1>, code=151, facility=<Facility.GUARDIAN: 9>, message='{prog_name}: guardian starting {value}', params={'prog_name': '/opt/firebird/bin/fbguard', 'value': '/opt/firebird/bin/fbserver'})
LogMessage(origin='MyServer (Server)', timestamp=datetime.datetime(2018, 4, 17, 15, 1, 27), level=<Severity.ERROR: 3>, code=177, facility=<Facility.NET: 10>, message='INET/inet_error: {error} errno = {err_code}', params={'error': 'invalid socket in packet_receive', 'err_code': 22})
LogMessage(origin='MyServer (Client)', timestamp=datetime.datetime(2018, 4, 17, 19, 42, 55), level=<Severity.INFO: 1>, code=162, facility=<Facility.GUARDIAN: 9>, message='{prog_name}: {process_name} normal shutdown.', params={'prog_name': '/opt/firebird/bin/fbguard', 'process_name': '/opt/firebird/bin/fbserver'})
"""
        self._check_events(data, output)
    def test_03_lin_fb3(self):
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
        output = """LogMessage(origin='ultron', timestamp=datetime.datetime(2018, 10, 28, 15, 25, 54), level=<Severity.INFO: 1>, code=151, facility=<Facility.GUARDIAN: 9>, message='{prog_name}: guardian starting {value}', params={'prog_name': '/opt/firebird/bin/fbguard', 'value': '/opt/firebird/bin/firebird'})
LogMessage(origin='ultron', timestamp=datetime.datetime(2018, 10, 28, 15, 29, 42), level=<Severity.INFO: 1>, code=157, facility=<Facility.GUARDIAN: 9>, message='{prog_name}: {process_name} terminated', params={'prog_name': '/opt/firebird/bin/fbguard', 'process_name': '/opt/firebird/bin/firebird'})
LogMessage(origin='ultron', timestamp=datetime.datetime(2018, 10, 31, 13, 47, 44), level=<Severity.ERROR: 3>, code=284, facility=<Facility.SYSTEM: 1>, message='REMOTE INTERFACE/gds__detach: Unsuccesful detach from database.\\nUncommitted work may have been lost.\\n{err_msg}', params={'err_msg': 'Error writing data to the connection.'})
LogMessage(origin='ultron', timestamp=datetime.datetime(2018, 11, 14, 3, 32, 44), level=<Severity.ERROR: 3>, code=177, facility=<Facility.NET: 10>, message='INET/inet_error: {error} errno = {err_code}, {parameters}', params={'error': 'read', 'err_code': 104, 'parameters': 'client host = Terminal, address = 192.168.1.243/55120, user = frodo'})
LogMessage(origin='ultron', timestamp=datetime.datetime(2018, 12, 7, 9, 53, 53), level=<Severity.ERROR: 3>, code=185, facility=<Facility.AUTH: 11>, message='Authentication error\\nNo matching plugins on server', params={})
LogMessage(origin='ultron', timestamp=datetime.datetime(2019, 6, 9, 17, 26, 9), level=<Severity.ERROR: 3>, code=163, facility=<Facility.NET: 10>, message='INET/INET_connect: getaddrinfo({host},{protocol}) failed: {error}', params={'host': 'ocalhost', 'protocol': 'gds_db', 'error': 'Neznámé jméno nebo služba'})
LogMessage(origin='ultron', timestamp=datetime.datetime(2019, 6, 13, 7, 32, 51), level=<Severity.INFO: 1>, code=75, facility=<Facility.VALIDATION: 6>, message='Database: {database}\\nValidation started', params={'database': '/usr/local/data/mydb.FDB'})
LogMessage(origin='ultron', timestamp=datetime.datetime(2019, 6, 13, 7, 36, 41), level=<Severity.WARNING: 2>, code=81, facility=<Facility.VALIDATION: 6>, message='Database: {database}\\nWarning: Page {page_num} is an orphan', params={'database': '/usr/local/data/mydb.FDB', 'page_num': 3867207})
LogMessage(origin='ultron', timestamp=datetime.datetime(2019, 6, 13, 7, 36, 41), level=<Severity.INFO: 1>, code=76, facility=<Facility.VALIDATION: 6>, message='Database: {database}\\nValidation finished: {errors} errors, {warnings} warnings, {fixed} fixed', params={'database': '/usr/local/data/mydb.FDB', 'errors': 0, 'warnings': 663, 'fixed': 663})
"""
        self._check_events(data, output)
