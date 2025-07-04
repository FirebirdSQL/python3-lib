# SPDX-FileCopyrightText: 2020-present The Firebird Projects <www.firebirdsql.org>
#
# SPDX-License-Identifier: MIT
#
# PROGRAM/MODULE: firebird-lib
# FILE:           firebird/lib/gstat.py
# DESCRIPTION:    Module for parsing and representing Firebird gstat output.
# CREATED:        6.10.2020
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

"""firebird.lib.gstat - Module for parsing and representing Firebird gstat output.

This module provides classes and functions to parse the text output generated
by the Firebird gstat utility and represent the statistics in a structured
object model. The main class is `StatDatabase`.
"""

from __future__ import annotations

import datetime
import weakref
from collections.abc import Iterable
from dataclasses import dataclass
from enum import Enum

from firebird.base.collections import DataList
from firebird.base.types import STOP, Error

GSTAT_30 = 3

# Defines the structure for mapping gstat output lines to object attributes.
# Each tuple contains: (gstat_label, value_type_code, attribute_name_override | None)
# value_type_code: 'i'=int, 's'=str, 'd'=datetime, 'l'=list, 'f'=float, 'p'=%
TLogItemSpec = list[tuple[str, str, str | None]]

items_hdr: TLogItemSpec = [
    ('Flags', 'i', None),
    ('Checksum', 'i', None),
    ('Generation', 'i', None),
    ('System Change Number', 'i', 'system_change_number'),
    ('Page size', 'i', None),
    ('ODS version', 's', None),
    ('Oldest transaction', 'i', 'oit'),
    ('Oldest active', 'i', 'oat'),
    ('Oldest snapshot', 'i', 'ost'),
    ('Next transaction', 'i', None),
    ('Bumped transaction', 'i', None),
    ('Sequence number', 'i', None),
    ('Next attachment ID', 'i', None),
    ('Implementation ID', 'i', None),
    ('Implementation', 's', None),
    ('Shadow count', 'i', None),
    ('Page buffers', 'i', None),
    ('Next header page', 'i', None),
    ('Database dialect', 'i', None),
    ('Creation date', 'd', None),
    ('Attributes', 'l', None)]

items_var: TLogItemSpec = [
    ('Sweep interval:', 'i', None),
    ('Continuation file:', 's', None),
    ('Last logical page:', 'i', None),
    ('Database backup GUID:', 's', 'backup_guid'),
    ('Root file name:', 's', 'root_filename'),
    ('Replay logging file:', 's', None),
    ('Backup difference file:', 's', 'backup_diff_file')]

items_tbl3: TLogItemSpec = [
    ('Primary pointer page:', 'i', None),
    ('Index root page:', 'i', None),
    ('Total formats:', 'i', None),
    ('used formats:', 'i', None),
    ('Average record length:', 'f', 'avg_record_length'),
    ('total records:', 'i', None),
    ('Average version length:', 'f', 'avg_version_length'),
    ('total versions:', 'i', None),
    ('max versions:', 'i', None),
    ('Average fragment length:', 'f', 'avg_fragment_length'),
    ('total fragments:', 'i', None),
    ('max fragments:', 'i', None),
    ('Average unpacked length:', 'f', 'avg_unpacked_length'),
    ('compression ratio:', 'f', None),
    ('Pointer pages:', 'i', 'pointer_pages'),
    ('data page slots:', 'i', None),
    ('Data pages:', 'i', None),
    ('average fill:', 'p', 'avg_fill'),
    ('Primary pages:', 'i', None),
    ('secondary pages:', 'i', None),
    ('swept pages:', 'i', None),
    ('Empty pages:', 'i', None),
    ('full pages:', 'i', None),
    ('Blobs:', 'i', None),
    ('total length:', 'i', 'blobs_total_length'),
    ('blob pages:', 'i', None),
    ('Level 0:', 'i', None),
    ('Level 1:', 'i', None),
    ('Level 2:', 'i', None)]

items_idx3: TLogItemSpec = [
    ('Root page:', 'i', None),
    ('depth:', 'i', None),
    ('leaf buckets:', 'i', None),
    ('nodes:', 'i', None),
    ('Average node length:', 'f', 'avg_node_length'),
    ('total dup:', 'i', None),
    ('max dup:', 'i', None),
    ('Average key length:', 'f', 'avg_key_length'),
    ('compression ratio:', 'f', None),
    ('Average prefix length:', 'f', 'avg_prefix_length'),
    ('average data length:', 'f', 'avg_data_length'),
    ('Clustering factor:', 'f', None),
    ('ratio:', 'f', None)]

items_fill: list[str] = ['0 - 19%', '20 - 39%', '40 - 59%', '60 - 79%', '80 - 99%']

class DbAttribute(Enum):
    """Database attributes stored in header page clumplets.
    """
    WRITE = 'force write'
    NO_RESERVE = 'no reserve'
    NO_SHARED_CACHE = 'shared cache disabled'
    ACTIVE_SHADOW = 'active shadow'
    SHUTDOWN_MULTI = 'multi-user maintenance'
    SHUTDOWN_SINGLE = 'single-user maintenance'
    SHUTDOWN_FULL = 'full shutdown'
    READ_ONLY = 'read only'
    BACKUP_LOCK = 'backup lock'
    BACKUP_MERGE = 'backup merge'
    BACKUP_WRONG = 'wrong backup state'

@dataclass(frozen=True)
class FillDistribution:
    """Data/Index page fill distribution statistics.

    Stores the count of pages falling into specific fill percentage ranges.
    """
    #: Count of pages filled 0 - 19%
    d20: int
    #: Count of pages filled 20 - 39%
    d40: int
    #: Count of pages filled 40 - 59%
    d60: int
    #: Count of pages filled 60 - 79%
    d80: int
    #: Count of pages filled 80 - 99%
    d100: int

@dataclass(frozen=True)
class Encryption:
    """Page encryption status.
    """
    pages: int
    encrypted: int
    unencrypted: int

@dataclass
class _ParserState:
    #: Current line number being processed.
    line_no: int = 0
    #: Reference to the StatTable currently being parsed.
    table: StatTable | None = None
    #: Reference to the StatIndex currently being parsed.
    index: StatIndex | None = None
    #: Flag indicating if the next line starts a new table/index block.
    new_block: bool = True
    #: Flag indicating if the parser is currently processing table data (vs. index data).
    in_table: bool = False
    #: Current parsing step/section (0=Header/Seek, 1=Header, 2=Variable, 3=Files, 4=Data/Indices).
    step: int = 0

def empty_str(value: str) -> bool:
    """Return True if string is empty (whitespace don't count) or None.
    """
    return True if value is None else value.strip() == ''

class StatTable:
    """Statistics for a single database table, populated from gstat output.

    Attributes are populated by the `StatDatabase` parser. Default values are
    `None` or 0 until parsed from the input.
    """
    def __init__(self):
        #: Table name
        self.name: str | None = None
        #: Table ID
        self.table_id: int | None = None
        #: Primary Pointer Page for table
        self.primary_pointer_page: int | None = None
        #: Index Root Page for table
        self.index_root_page: int | None = None
        #: Average record length
        self.avg_record_length: float | None = None
        #: Total number of record in table
        self.total_records: int | None = None
        #: Average record version length
        self.avg_version_length: float | None = None
        #: Total number of record versions
        self.total_versions: int | None = None
        #: Max number of versions for single record
        self.max_versions: int | None = None
        #: Number of data pages for table
        self.data_pages: int | None = None
        #: Number of data page slots for table
        self.data_page_slots: int | None = None
        #: Average data page fill ratio
        self.avg_fill: float | None = None
        #: Data page fill distribution statistics. Final type is `FillDistribution`.
        self.distribution: FillDistribution | None = None
        #: Indices belonging to table. Items are `weakref.proxy` to `StatIndex`.
        self.indices: DataList[StatIndex] = DataList(type_spec=StatIndex, key_expr='item.name')
        #: Number of Pointer Pages
        self.pointer_pages: int | None = None
        #: Number of record formats
        self.total_formats: int | None = None
        #: Number of actually used record formats
        self.used_formats: int | None = None
        #: Average length of record fragments
        self.avg_fragment_length: float | None = None
        #: Total number of record fragments
        self.total_fragments: int | None = None
        #: Max number of fragments for single record
        self.max_fragments: int | None = None
        #: Average length of unpacked record
        self.avg_unpacked_length: float | None = None
        #: Record compression ratio
        self.compression_ratio: float | None = None
        #: Number of Primary Data Pages
        self.primary_pages: int | None = None
        #: Number of Secondary Data Pages
        self.secondary_pages: int | None = None
        #: Number of swept data pages
        self.swept_pages: int | None = None
        #: Number of empty data pages
        self.empty_pages: int | None = None
        #: Number of full data pages
        self.full_pages: int | None = None
        #: Number of BLOB values
        self.blobs: int | None = None
        #: Total length of BLOB values (bytes)
        self.blobs_total_length: int | None = None
        #: Number of BLOB pages
        self.blob_pages: int | None = None
        #: Number of Level 0 BLOB values
        self.level_0: int | None = None
        #: Number of Level 1 BLOB values
        self.level_1: int | None = None
        #: Number of Level 2 BLOB values
        self.level_2: int | None = None

class StatIndex:
    """Statistics for a single database index, populated from gstat output.

    Instances are linked to their parent `StatTable` via a weak reference.
    Attributes are populated by the `StatDatabase` parser.
    """
    def __init__(self, table):
        #: wekref.proxy: Proxy to parent `.StatTable`
        self.table: weakref.ProxyType = weakref.proxy(table)
        table.indices.append(weakref.proxy(self))
        #: Index name
        self.name: str | None = None
        #: Index ID
        self.index_id: int | None = None
        #: Depth of index tree
        self.depth: int | None = None
        #: Number of leaft index tree buckets
        self.leaf_buckets: int | None = None
        #: Number of index tree nodes
        self.nodes: int | None = None
        #: Average data length
        self.avg_data_length: float | None = None
        #: Total number of duplicate keys
        self.total_dup: int | None = None
        #: Max number of occurences for single duplicate key
        self.max_dup: int | None = None
        #: Index page fill distribution statistics
        self.distribution: FillDistribution | None = None
        #: Index Root page
        self.root_page: int | None = None
        #: Average node length
        self.avg_node_length: float | None = None
        #: Average key length
        self.avg_key_length: float | None = None
        #: Index key compression ratio
        self.compression_ratio: float | None = None
        #: Average key prefix length
        self.avg_prefix_length: float | None = None
        #: Index clustering factor
        self.clustering_factor: float | None = None
        #: Ratio
        self.ratio: float | None = None

class StatDatabase:
    """Parses and holds Firebird database statistics produced by the gstat utility.

    This is the main class for interacting with gstat output. Instantiate it
    and use the `parse()` or `push()` methods to feed it gstat output lines.
    Parsed statistics are stored in the instance's attributes and the `tables`
    and `indices` collections.
    """
    def __init__(self):
        #: GSTAT version
        self.gstat_version: int | None = None
        #: System change number
        self.system_change_number: int | None = None
        #: GSTAT execution timestamp
        self.executed: datetime.datetime | None = None
        #: GSTAT completion timestamp
        self.completed: datetime.datetime | None = None
        #: Database filename
        self.filename: str | None = None
        #: Database flags
        self.flags: int = 0
        #: Database header generation
        self.generation: int = 0
        #: Database page size
        self.page_size: int = 0
        #: Oldest Interesting Transaction
        self.oit: int = 0
        #: Oldest Active Transaction
        self.oat: int = 0
        #: Oldest Snapshot Transaction
        self.ost: int = 0
        #: Next Transaction
        self.next_transaction: int = 0
        #: Next attachment ID
        self.next_attachment_id: int = 0
        #: Implementation
        self.implementation: str | None = None
        #: Number of shadows
        self.shadow_count: int = 0
        #: Number of page buffers
        self.page_buffers: int = 0
        #: Next header page
        self.next_header_page: int = 0
        #: SQL Dialect
        self.database_dialect: int = 0
        #: Database creation timestamp
        self.creation_date: datetime.datetime | None = None
        #: Database attributes
        self.attributes: list[DbAttribute] = []
        # Variable data
        #: Sweep interval
        self.sweep_interval: int | None = None
        #: Continuation file
        self.continuation_file: str | None = None
        #: Last logical page
        self.last_logical_page: int | None = None
        #: Backup GUID
        self.backup_guid: str | None = None
        #: Root file name
        self.root_filename: str | None = None
        #: Replay logging file
        self.replay_logging_file: str | None = None
        #: Backup difference file
        self.backup_diff_file: str | None = None
        #: Encryption statistics for data pages.
        self.encrypted_data_pages: int | None = None
        #: Encryption statistics for index pages.
        self.encrypted_index_pages: int | None = None
        #: Encryption statistics for blob pages.
        self.encrypted_blob_pages: int | None = None
        #: Database file names
        self.continuation_files: list[str] = []
        #
        self.__line_no: int = 0
        self.__table: StatTable | None = None
        self.__index: StatIndex | None = None
        self.__new_block: bool = True
        self.__in_table: bool = False
        self.__step: int = 0
        self.__clear()
    def __clear(self):
        self.gstat_version = None
        self.system_change_number = None
        self.executed = None
        self.completed = None
        self.filename = None
        self.flags = 0
        self.generation = 0
        self.page_size = 0
        self.oit = 0
        self.oat = 0
        self.ost = 0
        self.next_transaction = 0
        self.next_attachment_id = 0
        self.implementation = None
        self.shadow_count = 0
        self.page_buffers = 0
        self.next_header_page = 0
        self.database_dialect = 0
        self.creation_date = None
        self.attributes.clear()
        self.sweep_interval = None
        self.continuation_file = None
        self.last_logical_page = None
        self.backup_guid = None
        self.root_filename = None
        self.replay_logging_file = None
        self.backup_diff_file = None
        self.encrypted_data_pages = None
        self.encrypted_index_pages = None
        self.encrypted_blob_pages = None
        self.continuation_files.clear()
        self.__tables: DataList[StatTable] = DataList(type_spec=StatTable, key_expr='item.name')
        self.__indices: DataList[StatIndex] = DataList(type_spec=StatIndex, key_expr='item.name')
        #
        self.__line_no = 0
        self.__table = None
        self.__index = None
        self.__new_block = True
        self.__in_table = False
        self.__step = 0
    def __parse_hdr(self, line: str) -> None:
        "Parse line from header"
        for key, valtype, name in items_hdr:
            if line.startswith(key):
                # Check for GSTAT_VERSION
                if self.gstat_version is None:
                    if key == 'System Change Number':
                        self.gstat_version = GSTAT_30
                    elif key == 'Checksum':
                        raise Error("Output from gstat older than Firebird 3 is not supported")
                #
                value: str = line[len(key):].strip()
                if valtype == 'i':  # integer
                    value = int(value)
                elif valtype == 's':  # string
                    pass
                elif valtype == 'd':  # date time
                    value = datetime.datetime.strptime(value, '%b %d, %Y %H:%M:%S') # noqa:DTZ007
                elif valtype == 'l':  # list
                    if value == '':
                        value = []
                    else:
                        value = [x.strip() for x in value.split(',')]
                        value = [DbAttribute(x) for x in value]
                else:
                    raise Error(f"Unknown value type {valtype}")
                if name is None:
                    name = key.lower().replace(' ', '_') # noqa: PLW2901
                setattr(self, name, value)
                return
        raise Error(f'Unknown information (line {self.__line_no})')
    def __parse_var(self, line: str) -> None:
        "Parse line from variable header data"
        if line == '*END*':
            return
        for key, valtype, name in items_var:
            if line.startswith(key):
                value = line[len(key):].strip()
                if valtype == 'i':  # integer
                    value = int(value)
                elif valtype == 's':  # string
                    pass
                elif valtype == 'd':  # date time
                    value = datetime.datetime.strptime(value, '%b %d, %Y %H:%M:%S') # noqa: DTZ007
                else:
                    raise Error(f"Unknown value type {valtype}")
                if name is None:
                    name = key.lower().strip(':').replace(' ', '_') # noqa: PLW2901
                setattr(self, name, value)
                return
        raise Error(f'Unknown information (line {self.__line_no})')
    def __parse_fseq(self, line: str) -> None:
        "Parse line from file sequence"
        if not line.startswith('File '):
            raise Error(f"Bad file specification (line {self.__line_no})")
        if 'is the only file' in line:
            return
        if ' is the ' in line:
            self.continuation_files.append(line[5:line.index(' is the ')])
        elif ' continues as' in line:
            self.continuation_files.append(line[5:line.index(' continues as')])
        else:
            raise Error(f"Bad file specification (line {self.__line_no})")
    def __parse_table(self, line: str) -> None:
        "Parse line from table data"
        if self.__table.name is None: # pylint: disable=R1702
            # We should parse header
            tname, tid = line.split(' (')
            self.__table.name = tname.strip(' "')
            self.__table.table_id = int(tid.strip('()'))
        elif ',' in line:  # Data values
            for item in line.split(','):
                item = item.strip() # noqa: PLW2901
                found = False
                items = items_tbl3
                for key, valtype, name in items:
                    if item.startswith(key):
                        value: str = item[len(key):].strip()
                        if valtype == 'i':  # integer
                            value = int(value)
                        elif valtype == 'f':  # float
                            value = float(value)
                        elif valtype == 'p':  # %
                            value = int(value.strip('%'))
                        else:
                            raise Error(f"Unknown value type {valtype}")
                        if name is None:
                            name = key.lower().strip(':').replace(' ', '_') # noqa: PLW2901
                        setattr(self.__table, name, value)
                        found = True
                        break
                if not found:
                    raise Error(f'Unknown information (line {self.__line_no})')
        elif '=' in line:
            fill_range, fill_value = line.split('=')
            i = items_fill.index(fill_range.strip())
            if self.__table.distribution is None:
                self.__table.distribution = [0, 0, 0, 0, 0]
            self.__table.distribution[i] = int(fill_value.strip())
        elif line.startswith('Fill distribution:'):
            pass
        else:
            raise Error(f'Unknown information (line {self.__line_no})')
    def __parse_index(self, line: str) -> None:
        "Parse line from index data"
        if self.__index.name is None: # pylint: disable=R1702
            # We should parse header
            iname, iid = line[6:].split(' (')
            self.__index.name = iname.strip(' "')
            self.__index.index_id = int(iid.strip('()'))
        elif ',' in line:  # Data values
            for item in line.split(','):
                item = item.strip() # noqa: PLW2901
                found = False
                items = items_idx3
                for key, valtype, name in items:
                    if item.startswith(key):
                        value: str = item[len(key):].strip()
                        if valtype == 'i':  # integer
                            value = int(value)
                        elif valtype == 'f':  # float
                            value = float(value)
                        elif valtype == 'p':  # %
                            value = int(value.strip('%'))
                        else:
                            raise Error(f"Unknown value type {valtype}")
                        if name is None:
                            name = key.lower().strip(':').replace(' ', '_') # noqa: PLW2901
                        setattr(self.__index, name, value)
                        found = True
                        break
                if not found:
                    raise Error(f'Unknown information (line {self.__line_no})')
        elif '=' in line:
            fill_range, fill_value = line.split('=')
            i = items_fill.index(fill_range.strip())
            if self.__index.distribution is None:
                self.__index.distribution = [0, 0, 0, 0, 0]
            self.__index.distribution[i] = int(fill_value.strip())
        elif line.startswith('Fill distribution:'):
            pass
        else:
            raise Error(f'Unknown information (line {self.__line_no})')
    def __parse_encryption(self, line: str) -> None:
        "Parse line from encryption data"
        try:
            total: str
            encrypted: str
            unencrypted: str
            total, encrypted, unencrypted = line.split(',')
            _, total = total.rsplit(' ', 1)
            total = int(total)
            _, encrypted = encrypted.rsplit(' ', 1)
            encrypted = int(encrypted)
            _, unencrypted = unencrypted.rsplit(' ', 1)
            unencrypted = int(unencrypted)
            data = Encryption(total, encrypted, unencrypted)
        except Exception as exc:
            raise Error(f'Malformed encryption information (line {self.__line_no})') from exc
        if 'Data pages:' in line:
            self.encrypted_data_pages = data
        elif 'Index pages:' in line:
            self.encrypted_index_pages = data
        elif 'Blob pages:' in line:
            self.encrypted_blob_pages = data
        else:
            raise Error(f'Unknown encryption information (line {self.__line_no})')
    def has_table_stats(self) -> bool:
        """Returns True if instance contains information about tables.

        .. important::

           This is not the same as check for empty :data:`tables` list. When gstat is run
           with `-i` without `-d` option, :data:`tables` list contains instances that does
           not have any other information about table but table name and its indices.
        """
        return self.tables[0].primary_pointer_page is not None if len(self.tables) > 0 else False
    def has_row_stats(self) -> bool:
        """Returns True if instance contains information about table rows.
        """
        return self.has_table_stats() and self.tables[0].avg_version_length is not None
    def has_index_stats(self) -> bool:
        """Returns True if instance contains information about indices.
        """
        return self.indices[0].depth is not None if len(self.indices) > 0 else False
    def has_encryption_stats(self) -> bool:
        """Returns True if instance contains information about database encryption.
        """
        return self.encrypted_data_pages is not None
    def has_system(self) -> bool:
        """Returns True if instance contains information about system tables.
        """
        return self.tables.contains("item.name.startswith('RDB$DATABASE')")
    def parse(self, lines: Iterable[str]) -> None:
        """Parses gstat output from an iterable source.

        Processes all lines from the iterable and finalizes parsing.

        Arguments:
            lines: Iterable that returns lines from database analysis produced
                   by Firebird gstat (e.g., a file object or list of strings).
        """
        for line in lines:
            self.push(line)
        self.push(STOP)
    def push(self, line: str | STOP) -> None:
        """Pushes a single line (or STOP sentinel) into the parser state machine.

        This method processes one line of gstat output at a time, updating the
        internal state and populating statistics attributes. Call with the
        `STOP` sentinel after the last line to finalize processing (e.g.,
        convert distributions, freeze lists).

        Arguments:
            line: Single gstat output line, or the `firebird.base.types.STOP` sentinel.
        """
        if self.__step == -1:
            self.__clear()
        if line is STOP:
            if self.has_table_stats():
                for table in self.tables:
                    table.distribution = FillDistribution(*table.distribution)
            if self.has_index_stats():
                for index in self.indices:
                    index.distribution = FillDistribution(*index.distribution)
            self.tables.freeze()
            self.indices.freeze()
            self.__step = -1
        else:
            line = line.strip()
            self.__line_no += 1
            if line.startswith('Gstat completion time'):
                self.completed = datetime.datetime.strptime(line[22:], '%a %b %d %H:%M:%S %Y') # noqa: DTZ007
            elif self.__step == 0:  # Looking for section or self name
                if line.startswith('Gstat execution time'):
                    self.executed = datetime.datetime.strptime(line[21:], '%a %b %d %H:%M:%S %Y') # noqa: DTZ007
                elif line.startswith('Database header page information:'):
                    self.__step = 1
                elif line.startswith('Variable header data:'):
                    self.__step = 2
                elif line.startswith('Database file sequence:'):
                    self.__step = 3
                elif 'encrypted' in line and 'non-crypted' in line:
                    self.__parse_encryption(line)
                elif line.startswith('Analyzing database pages ...'):
                    self.__step = 4
                elif empty_str(line):
                    pass
                elif line.startswith('Database "'):
                    _, filename = line.split(' ')
                    self.filename = filename.strip('"')
                    self.__step = 0
                else:
                    raise Error(f"Unrecognized data (line {self.__line_no})")
            elif self.__step == 1:  # Header
                if empty_str(line):  # section ends with empty line
                    self.__step = 0
                else:
                    self.__parse_hdr(line)
            elif self.__step == 2:  # Variable data
                if empty_str(line):  # section ends with empty line
                    self.__step = 0
                else:
                    self.__parse_var(line)
            elif self.__step == 3:  # File sequence
                if empty_str(line):  # section ends with empty line
                    self.__step = 0
                else:
                    self.__parse_fseq(line)
            elif self.__step == 4:  # Tables and indices
                if empty_str(line):  # section ends with empty line
                    self.__new_block = True
                elif self.__new_block:
                    self.__new_block = False
                    if not line.startswith('Index '):
                        # Should be table
                        self.__table = StatTable()
                        self.tables.append(self.__table)
                        self.__in_table = True
                        self.__parse_table(line)
                    else:  # It's index
                        self.__index = StatIndex(self.__table)
                        self.indices.append(self.__index)
                        self.__in_table = False
                        self.__parse_index(line)
                elif self.__in_table:
                    self.__parse_table(line)
                else:
                    self.__parse_index(line)
    @property
    def tables(self) -> DataList[StatTable]:
        """`~firebird.base.collections.DataList` of `.StatTable` instances."""
        return self.__tables
    @property
    def indices(self) -> DataList[StatIndex]:
        """`~firebird.base.collections.DataList` of `.StatIndex` instances."""
        return self.__indices
