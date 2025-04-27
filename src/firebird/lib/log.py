# SPDX-FileCopyrightText: 2020-present The Firebird Projects <www.firebirdsql.org>
#
# SPDX-License-Identifier: MIT
#
# PROGRAM/MODULE: firebird-lib
# FILE:           firebird/lib/log.py
# DESCRIPTION:    Module for parsing Firebird server log (`firebird.log`).

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

"""firebird.lib.log - Module for parsing Firebird server log (`firebird.log`).

This module provides the `LogParser` class to read and parse entries from
a Firebird server log file, yielding structured `LogMessage` objects.
It handles multi-line log entries and uses message definitions from `logmsgs`
to identify specific events and extract parameters.
"""

from __future__ import annotations

from collections.abc import Generator, Iterable
from contextlib import suppress
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from firebird.base.types import STOP, Error

from .logmsgs import Facility, Severity, identify_msg


@dataclass(order=True, frozen=True)
class LogMessage:
    """Represents a single, parsed entry from the Firebird log.

    Instances are immutable and orderable by timestamp.
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
    #: Message text. It may contain `str.format()` style `{param_name}` placeholders for
    #: message parameters found in the `params` dictionary.
    message: str
    #: Dictionary containing parameters extracted from the log message text.
    params: dict[str, Any]

class LogParser:
    """A stateful parser for Firebird server log files (`firebird.log`).

    It processes the log line by line, handling multi-line entries.
    Use the `push()` method for incremental parsing or the `parse()`
    method to process an entire iterable of lines.
    """
    def __init__(self):
        #: Internal buffer holding lines for the current log entry being processed.
        self.__buffer: list[str] = []
    def push(self, line: str| STOP) -> LogMessage | None:
        """Pushes a single line (or STOP sentinel) into the parser.

        This method accumulates lines in an internal buffer. When a new log entry
        starts or the `STOP` sentinel is received, it attempts to parse the
        buffered lines into a complete `LogMessage`.

        Arguments:
            line: Single line from Firebird log, or the `~firebird.base.types.STOP` sentinel
                  to signal the end of input and process any remaining buffered lines.

        Returns:
            A `LogMessage` instance if a complete log entry was parsed from the
            buffer, or `None` if more lines are needed for the current entry.
            Returns the final entry when `STOP` is pushed and the buffer is non-empty.
        """
        result = None
        if line is STOP:
            result = self.parse_entry(self.__buffer)
            self.__buffer.clear()
        elif line := line.strip():
            items = line.split()
            if len(items) >= 6: # noqa: PLR2004
                # potential new entry
                new_entry = False
                with suppress(ValueError):
                    datetime.strptime(' '.join(items[len(items)-5:]), '%a %b %d %H:%M:%S %Y') # noqa: DTZ007
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
        elif self.__buffer:
            self.__buffer.append(line)
        return result
    def parse_entry(self, log_entry: list[str]) -> LogMessage:
        """Parses a single, complete log entry from a list of lines.

        Assumes `log_entry` contains all lines belonging to exactly one log entry,
        with the first line being the header containing timestamp and origin.

        Arguments:
            log_entry: List of strings representing the lines of a single log entry.

        Returns:
            A `LogMessage` instance representing the parsed entry. If the specific
            message cannot be identified via `logmsgs`, a generic `LogMessage`
            with `Severity.UNKNOWN` and `Facility.UNKNOWN` is returned.

        Raises:
            firebird.base.types.Error: If the first line doesn't conform to the expected
                                       log entry header format (origin + timestamp).
        """
        try:
            items = log_entry[0].split()
            timestamp = datetime.strptime(' '.join(items[len(items)-5:]), # noqa: DTZ007
                                          '%a %b %d %H:%M:%S %Y')
            origin = ' '.join(items[:len(items)-5])
        except Exception as exc:
            raise Error("Malformed log entry") from exc
        msg = '\n'.join(log_entry[1:]).strip()
        #
        if (found := identify_msg(msg)) is not None:
            log_msg = found[0]
            return LogMessage(origin, timestamp, log_msg.severity, log_msg.msg_id,
                              log_msg.facility, log_msg.get_pattern(without_optional=found[2]),
                              found[1])
        return LogMessage(origin, timestamp, Severity.UNKNOWN, 0, Facility.UNKNOWN, msg, {})
    def parse(self, lines: Iterable) -> Generator[LogMessage, None, None]:
        """Parses Firebird log lines from an iterable source.

        This is a convenience method that iterates over `lines`, calls `push()`
        for each line, and yields complete `LogMessage` objects as they are parsed.
        It automatically handles the final `push(STOP)` call.

        Arguments:
            lines: An iterable yielding lines from a Firebird log
                   (e.g., a file object or list of strings).

        Yields:
            `.LogMessage` instances describing individual log entries.

        Raises:
            firebird.base.types.Error: When a malformed log entry header is detected
                                       by `parse_entry`.
        """
        for line in lines:
            result = self.push(line)
            if result is not None:
                yield result
        result = self.push(STOP)
        if result is not None:
            yield result
