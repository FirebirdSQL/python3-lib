#coding:utf-8
#
# PROGRAM/MODULE: firebird-lib
# FILE:           firebird/lib/log.py
# DESCRIPTION:    Module for parsing Firebird server log
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
# pylint: disable=C0302, W0212, R0902, R0912,R0913, R0914, R0915, R0904

"""firebird.lib.log - Module for parsing Firebird server log


"""

from __future__ import annotations
from typing import List, Dict, Any, Iterable, Optional, Union
from datetime import datetime
from dataclasses import dataclass
from contextlib import suppress
from firebird.base.types import Error, STOP, Sentinel
from firebird.lib.logmsgs import identify_msg, Severity, Facility

@dataclass(order=True, frozen=True)
class LogMessage:
    """Firebird log message.
    """
    #: Firebird server identification
    origin: str
    #: Date and time when message was written to log
    timestamp: datetime
    #: Severity level
    level: Severity
    #: Message identification code
    code: int
    #: Firebird server facility that wrote the message
    facility: Facility
    #: Message text. It may contain `str.format` `{<param_name>}` placeholders for
    #: message parameters.
    message: str
    #:  Dictionary with message parameters
    params: Dict[str, Any]

class LogParser:
    """Parser for firebird.log files.
    """
    def __init__(self):
        self.__buffer: List[str] = []
    def push(self, line: Union[str, Sentinel]) -> Optional[LogMessage]:
        """Push parser.

        Arguments:
            line: Single line from Firebird log, or `~firebird.base.types.STOP` sentinel.

        Returns:
            `LogMessage`, or None if method did not accumulated all lines for the whole
            log entry.
        """
        result = None
        if line is STOP:
            result = self.parse_entry(self.__buffer)
            self.__buffer.clear()
        elif line := line.strip():
            items = line.split()
            if len(items) >= 6:
                # potential new entry
                new_entry = False
                with suppress(ValueError):
                    datetime.strptime(' '.join(items[len(items)-5:]), '%a %b %d %H:%M:%S %Y')
                    new_entry = True
                if new_entry:
                    if self.__buffer:
                        result = self.parse_entry(self.__buffer)
                        self.__buffer.clear()
                    self.__buffer.append(line)
                else:
                    self.__buffer.append(line)
            else:
                self.__buffer.append(line)
        else:
            if self.__buffer:
                self.__buffer.append(line)
        return result
    def parse_entry(self, log_entry: List[str]) -> LogMessage:
        """Parse single log entry.

        Arguments:
            log_entry: List with log entry lines.
        """
        try:
            items = log_entry[0].split()
            timestamp = datetime.strptime(' '.join(items[len(items)-5:]),
                                          '%a %b %d %H:%M:%S %Y')
            origin = ' '.join(items[:len(items)-5])
        except Exception as exc:
            raise Error("Malformed log entry") from exc
        msg = '\n'.join(log_entry[1:]).strip()
        #
        if (found := identify_msg(msg)) is not None:
            log_msg = found[0]
            return LogMessage(origin, timestamp, log_msg.severity, log_msg.msg_id,
                              log_msg.facility, log_msg.get_pattern(found[2]), found[1])
        return LogMessage(origin, timestamp, Severity.UNKNOWN, 0, Facility.UNKNOWN, msg, {})
    def parse(self, lines: Iterable):
        """Parse output from Firebird log.

        Arguments:
            lines: Iterable that returns Firebird log lines.

        Yields:
            `.LogMessage` instances describing individual log entries.

        Raises:
            firebird.base.types.Error: When any problem is found in input stream.
        """
        for line in lines:
            result = self.push(line)
            if result is not None:
                yield result
        result = self.push(STOP)
        if result is not None:
            yield result
