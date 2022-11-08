#coding:utf-8
#
# PROGRAM/MODULE: Saturnin microservices
# FILE:           firebird/lib/logmsgs.py
# DESCRIPTION:    Firebird log messages for Firebird log parser
# CREATED:        22.11.2019
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
# Copyright (c) 2019 Firebird Project (www.firebirdsql.org)
# All Rights Reserved.
#
# Contributor(s): Pavel Císař (original code)
#                 ______________________________________.
# pylint: disable=C0302, W0212, R0902, R0912,R0913, R0914, R0915, R0904, C0301

"""Saturnin microservices - Firebird log messages for Firebird log parser microservice
"""

from __future__ import annotations
from typing import Optional, List, Tuple, Dict, Any
from dataclasses import dataclass
from enum import IntEnum

class Severity(IntEnum):
    """Firebird Log Message severity.
    """
    UNKNOWN = 0
    INFO = 1
    WARNING = 2
    ERROR = 3
    CRITICAL = 4

class Facility(IntEnum):
    """Firebird Log Server facility.
    """
    UNKNOWN = 0
    SYSTEM = 1
    CONFIG = 2
    INTL = 3
    FILEIO = 4
    USER = 5
    VALIDATION = 6
    SWEEP = 7
    PLUGIN = 8
    GUARDIAN = 9
    NET = 10
    AUTH = 11

@dataclass(order=True, frozen=True)
class MsgDesc:
    """Firebird log message descriptor.
    """
    #: Message ID
    msg_id: int
    #: Message severity level
    severity: Severity
    #: Firebird facility
    facility: Facility
    #: Message format description
    msg_format: List[str]
    def get_pattern(self, without_optional: bool) -> str:
        """Returns message pattern.

        Arguments:
            without_optional: When True, the pattern does not include optional part.
        """
        result = ''
        for part in self.msg_format:
            if part == 'OPTIONAL':
                if without_optional:
                    return result
            elif part.startswith('{'):
                result += f'{{{part[3:-1]}}}'
            else:
                result += part
        return result

#: List of Firebird server log message descriptors
messages = [
  # firebird/src/common/fb_exception.cpp:240
  MsgDesc(msg_id=1, severity=Severity.ERROR, facility=Facility.SYSTEM,
          msg_format=['Operating system call ', '{s:syscall}', ' failed. Error code ', '{d:error_code}']),
  # firebird/src/common/utils.cpp:464
  MsgDesc(msg_id=2, severity=Severity.ERROR, facility=Facility.SYSTEM,
          msg_format=['LoadLibrary failed for advapi32.dll. Error code: ', '{d:err_code}']),
  # firebird/src/common/utils.cpp:482
  MsgDesc(msg_id=3, severity=Severity.ERROR, facility=Facility.SYSTEM,
          msg_format=['Cannot access privilege management API']),
  # firebird/src/common/utils.cpp:490
  MsgDesc(msg_id=4, severity=Severity.ERROR, facility=Facility.SYSTEM,
          msg_format=['OpenProcessToken failed. Error code: ', '{d:err_code}']),
  # firebird/src/common/utils.cpp:509
  MsgDesc(msg_id=5, severity=Severity.ERROR, facility=Facility.SYSTEM,
          msg_format=['PrivilegeCheck failed. Error code: ', '{d:err_code}']),
  # firebird/src/common/db_alias.cpp:290
  MsgDesc(msg_id=6, severity=Severity.WARNING, facility=Facility.CONFIG,
          msg_format=['Value ', '{s:file}', ' configured for alias ', '{s:alias}', ' is not a fully qualified path name, ignored']),
  # firebird/src/common/db_alias.cpp:504
  MsgDesc(msg_id=7, severity=Severity.ERROR, facility=Facility.CONFIG,
          msg_format=['File databases.conf contains bad data: ', '{s:expr}']),
  # firebird/src/common/IntUtil.cpp:493
  MsgDesc(msg_id=8, severity=Severity.ERROR, facility=Facility.INTL,
          msg_format=['initUnicodeCollation failed - unexpected exception caught']),
  # firebird/src/common/IntUtil.cpp:531
  MsgDesc(msg_id=9, severity=Severity.ERROR, facility=Facility.INTL,
          msg_format=['initUnicodeCollation failed - UnicodeUtil::Utf16Collation::create failed']),
  # firebird/src/common/unicode_util.cpp:965
  MsgDesc(msg_id=10, severity=Severity.ERROR, facility=Facility.INTL,
          msg_format=['No versions']),
  # firebird/src/common/unicode_util.cpp:1002
  MsgDesc(msg_id=11, severity=Severity.ERROR, facility=Facility.INTL,
          msg_format=['failed to load UC icu module version ', '{s:version}']),
  # firebird/src/common/unicode_util.cpp:1010
  MsgDesc(msg_id=12, severity=Severity.ERROR, facility=Facility.INTL,
          msg_format=['failed to load IN icu module version ', '{s:version}']),
  # firebird/src/common/unicode_util.cpp:1067
  MsgDesc(msg_id=13, severity=Severity.ERROR, facility=Facility.INTL,
          msg_format=['u_init() error ', '{d:err_code}']),
  # firebird/src/common/unicode_util.cpp:1076
  MsgDesc(msg_id=14, severity=Severity.ERROR, facility=Facility.INTL,
          msg_format=['ucolOpen failed']),
  # firebird/src/common/unicode_util.cpp:1223, 1236, 1244
  MsgDesc(msg_id=15, severity=Severity.ERROR, facility=Facility.INTL,
          msg_format=['IntlUtil::convertUtf16ToAscii failed']),
  # firebird/src/common/unicode_util.cpp:1254
  MsgDesc(msg_id=16, severity=Severity.ERROR, facility=Facility.INTL,
          msg_format=['attributes (', '{x:attributes}', ') failed or ', '{d:spec_attr_count}', ' != ', '{d:attr_count}', ' ?']),
  # firebird/src/common/unicode_util.cpp:1266
  MsgDesc(msg_id=17, severity=Severity.ERROR, facility=Facility.INTL,
          msg_format=['loadICU failed']),
  # firebird/src/common/unicode_util.cpp:1275
  MsgDesc(msg_id=18, severity=Severity.ERROR, facility=Facility.INTL,
          msg_format=['ucolOpen failed']),
  # firebird/src/common/unicode_util.cpp:1282
  MsgDesc(msg_id=19, severity=Severity.ERROR, facility=Facility.INTL,
          msg_format=['ucolOpen failed']),
  # firebird/src/common/unicode_util.cpp:1290
  MsgDesc(msg_id=20, severity=Severity.ERROR, facility=Facility.INTL,
          msg_format=['ucolOpen failed']),
  # firebird/src/common/isc_sync.cpp:896
  MsgDesc(msg_id=21, severity=Severity.ERROR, facility=Facility.SYSTEM,
          msg_format=['semTable->get() failed']),
  # firebird/src/common/isc_sync.cpp:980
  MsgDesc(msg_id=22, severity=Severity.ERROR, facility=Facility.SYSTEM,
          msg_format=['semctl() failed, errno ', '{d:err_code}']),
  # firebird/src/common/isc_sync.cpp:1329
  MsgDesc(msg_id=23, severity=Severity.ERROR, facility=Facility.SYSTEM,
          msg_format=['ISC_event_wait: semop failed with errno = ', '{d:err_code}']),
  # firebird/src/common/isc_sync.cpp:1444
  MsgDesc(msg_id=24, severity=Severity.ERROR, facility=Facility.SYSTEM,
          msg_format=['ISC_event_post: semctl failed with errno = ', '{d:err_code}']),
  # firebird/src/common/isc_sync.cpp:1459
  MsgDesc(msg_id=25, severity=Severity.ERROR, facility=Facility.SYSTEM,
          msg_format=['ISC_event_post: pthread_cond_broadcast failed with errno = ', '{d:err_code}']),
  # firebird/src/common/isc_sync.cpp:1543
  MsgDesc(msg_id=26, severity=Severity.CRITICAL, facility=Facility.SYSTEM,
          msg_format=['{s:err_msg}', ' Segmentation Fault.\nThe code attempted to access memory\nwithout privilege to do so.\nThis exception will cause the Firebird server\nto terminate abnormally.']),
  # firebird/src/common/isc_sync.cpp:1543
  MsgDesc(msg_id=27, severity=Severity.CRITICAL, facility=Facility.SYSTEM,
          msg_format=['{s:err_msg}', ' Bus Error.\nThe code caused a system bus error.\nThis exception will cause the Firebird server\nto terminate abnormally.']),
  # firebird/src/common/isc_sync.cpp:1543
  MsgDesc(msg_id=28, severity=Severity.CRITICAL, facility=Facility.SYSTEM,
          msg_format=['{s:err_msg}', ' Illegal Instruction.\nThe code attempted to perfrom an\nillegal operation.This exception will cause the Firebird server\nto terminate abnormally.']),
  # firebird/src/common/isc_sync.cpp:1543
  MsgDesc(msg_id=29, severity=Severity.CRITICAL, facility=Facility.SYSTEM,
          msg_format=['{s:err_msg}', ' Floating Point Error.\nThe code caused an arithmetic exception\nor floating point exception.This exception will cause the Firebird server\nto terminate abnormally.']),
  # firebird/src/common/isc_sync.cpp:1543
  MsgDesc(msg_id=30, severity=Severity.CRITICAL, facility=Facility.SYSTEM,
          msg_format=['{s:err_msg}', ' Unknown Exception.\nException number ', '{d:sig_num}', '.This exception will cause the Firebird server\nto terminate abnormally.']),
  # firebird/src/common/isc_sync.cpp:1707
  MsgDesc(msg_id=31, severity=Severity.CRITICAL, facility=Facility.SYSTEM,
          msg_format=['{s:err_msg}', ' Access violation.\nThe code attempted to access a virtual\naddress without privilege to do so.\nThis exception will cause the Firebird server\nto terminate abnormally.']),
  # firebird/src/common/isc_sync.cpp:1707
  MsgDesc(msg_id=32, severity=Severity.CRITICAL, facility=Facility.SYSTEM,
          msg_format=['{s:err_msg}', ' Datatype misalignment.\nThe attempted to read or write a value\nthat was not stored on a memory boundary.\nThis exception will cause the Firebird server\nto terminate abnormally.']),
  # firebird/src/common/isc_sync.cpp:1707
  MsgDesc(msg_id=33, severity=Severity.CRITICAL, facility=Facility.SYSTEM,
          msg_format=['{s:err_msg}', ' Array bounds exceeded.\nThe code attempted to access an array\nelement that is out of bounds.\nThis exception will cause the Firebird server\nto terminate abnormally.']),
  # firebird/src/common/isc_sync.cpp:1707
  MsgDesc(msg_id=34, severity=Severity.CRITICAL, facility=Facility.SYSTEM,
          msg_format=['{s:err_msg}', ' Float denormal operand.\nOne of the floating-point operands is too\nsmall to represent as a standard floating-point\nvalue.\nThis exception will cause the Firebird server\nto terminate abnormally.']),
  # firebird/src/common/isc_sync.cpp:1707
  MsgDesc(msg_id=35, severity=Severity.CRITICAL, facility=Facility.SYSTEM,
          msg_format=['{s:err_msg}', ' Floating-point divide by zero.\nThe code attempted to divide a floating-point\nvalue by a floating-point divisor of zero.\nThis exception will cause the Firebird server\nto terminate abnormally.']),
  # firebird/src/common/isc_sync.cpp:1707
  MsgDesc(msg_id=36, severity=Severity.CRITICAL, facility=Facility.SYSTEM,
          msg_format=['{s:err_msg}', ' Floating-point inexact result.\nThe result of a floating-point operation cannot\nbe represented exactly as a decimal fraction.\nThis exception will cause the Firebird server\nto terminate abnormally.']),
  # firebird/src/common/isc_sync.cpp:1707
  MsgDesc(msg_id=37, severity=Severity.CRITICAL, facility=Facility.SYSTEM,
          msg_format=['{s:err_msg}', ' Floating-point invalid operand.\nAn indeterminant error occurred during a\nfloating-point operation.\nThis exception will cause the Firebird server\nto terminate abnormally.']),
  # firebird/src/common/isc_sync.cpp:1707
  MsgDesc(msg_id=38, severity=Severity.CRITICAL, facility=Facility.SYSTEM,
          msg_format=['{s:err_msg}', ' Floating-point overflow.\nThe exponent of a floating-point operation\nis greater than the magnitude allowed.\nThis exception will cause the Firebird server\nto terminate abnormally.']),
  # firebird/src/common/isc_sync.cpp:1707
  MsgDesc(msg_id=39, severity=Severity.CRITICAL, facility=Facility.SYSTEM,
          msg_format=['{s:err_msg}', ' Floating-point stack check.\nThe stack overflowed or underflowed as the\nresult of a floating-point operation.\nThis exception will cause the Firebird server\nto terminate abnormally.']),
  # firebird/src/common/isc_sync.cpp:1707
  MsgDesc(msg_id=40, severity=Severity.CRITICAL, facility=Facility.SYSTEM,
          msg_format=['{s:err_msg}', ' Floating-point underflow.\nThe exponent of a floating-point operation\nis less than the magnitude allowed.\nThis exception will cause the Firebird server\nto terminate abnormally.']),
  # firebird/src/common/isc_sync.cpp:1707
  MsgDesc(msg_id=41, severity=Severity.CRITICAL, facility=Facility.SYSTEM,
          msg_format=['{s:err_msg}', ' Integer divide by zero.\nThe code attempted to divide an integer value\nby an integer divisor of zero.\nThis exception will cause the Firebird server\nto terminate abnormally.']),
  # firebird/src/common/isc_sync.cpp:1707
  MsgDesc(msg_id=42, severity=Severity.CRITICAL, facility=Facility.SYSTEM,
          msg_format=['{s:err_msg}', ' Interger overflow.\nThe result of an integer operation caused the\nmost significant bit of the result to carry.\nThis exception will cause the Firebird server\nto terminate abnormally.']),
  # firebird/src/common/isc_sync.cpp:1707
  MsgDesc(msg_id=43, severity=Severity.CRITICAL, facility=Facility.SYSTEM,
          msg_format=['{s:err_msg}', ' An exception occurred that does\nnot have a description.  Exception number ', '{d:err_code}', '.\nThis exception will cause the Firebird server\nto terminate abnormally.']),
  # firebird/src/common/isc_sync.cpp:2755
  MsgDesc(msg_id=44, severity=Severity.ERROR, facility=Facility.SYSTEM,
          msg_format=['enterFastMutex: dead process detected, pid = ', '{d:pid}']),
  # firebird/src/common/os/win32/os_utils.cpp:212
  MsgDesc(msg_id=45, severity=Severity.ERROR, facility=Facility.FILEIO,
          msg_format=['Can\'t create directory "', '{s:dir}', '". OS errno is ', '{d:err_code}']),
  # firebird/src/common/os/win32/os_utils.cpp:223
  MsgDesc(msg_id=46, severity=Severity.ERROR, facility=Facility.FILEIO,
          msg_format=['Can\'t create directory "', '{s:dir}', '". File with same name already exists']),
  # firebird/src/common/os/win32/os_utils.cpp:234
  MsgDesc(msg_id=47, severity=Severity.ERROR, facility=Facility.FILEIO,
          msg_format=['Can\'t create directory "', '{s:dir}', '". Readonly directory with same name already exists']),
  # firebird/src/common/classes/ClumpletReader.cpp:79
  MsgDesc(msg_id=48, severity=Severity.INFO, facility=Facility.SYSTEM,
          msg_format=['*** DUMP ***']),
  # firebird/src/common/classes/ClumpletReader.cpp:83
  MsgDesc(msg_id=49, severity=Severity.INFO, facility=Facility.SYSTEM,
          msg_format=['recursion']),
  # firebird/src/common/classes/ClumpletReader.cpp:91
  MsgDesc(msg_id=50, severity=Severity.INFO, facility=Facility.SYSTEM,
          msg_format=['Tag=', '{d:tag}', ' Offset=', '{d:offset}', ' Length=', '{d:length}', ' Eof=', '{d:eof}']),
  # firebird/src/common/classes/ClumpletReader.cpp:94
  MsgDesc(msg_id=51, severity=Severity.INFO, facility=Facility.SYSTEM,
          msg_format=['Clump ', '{d:tag}', ' at offset ', '{d:offset}', ': ', '{s:hex_content}']),
  # firebird/src/common/classes/ClumpletReader.cpp:100
  MsgDesc(msg_id=52, severity=Severity.ERROR, facility=Facility.SYSTEM,
          msg_format=['Fatal exception during clumplet dump: ', '{s:error}']),
  # firebird/src/common/classes/ClumpletReader.cpp:103
  MsgDesc(msg_id=53, severity=Severity.INFO, facility=Facility.SYSTEM,
          msg_format=['Plain dump starting with offset ', '{d:offset}', ': ', '{s:hex_content}']),
  # firebird/src/common/config/dir_list.cpp:152
  MsgDesc(msg_id=54, severity=Severity.WARNING, facility=Facility.CONFIG,
          msg_format=["DirectoryList: unknown parameter '", '{s:value}', "', defaulting to None"]),
  # firebird/src/yvalve/PluginManager.cpp:385
  MsgDesc(msg_id=55, severity=Severity.WARNING, facility=Facility.SYSTEM,
          msg_format=['Failed to reset cleanup %p']),
  # firebird/src/yvalve/PluginManager.cpp:1033
  MsgDesc(msg_id=56, severity=Severity.WARNING, facility=Facility.SYSTEM,
          msg_format=['Unexpected call to register plugin ', '{s:name}', ', type ', '{d:interface_type}', ' - ignored']),
  # firebird/src/yvalve/PluginManager.cpp:1059
  MsgDesc(msg_id=57, severity=Severity.WARNING, facility=Facility.SYSTEM,
          msg_format=['Unexpected call to set module cleanup - ignored']),
  # firebird/src/jrd/IntlManager.cpp:508
  MsgDesc(msg_id=58, severity=Severity.ERROR, facility=Facility.INTL,
          msg_format=["INTL module '", '{s:filename}', "' is of incompatible version number ", '{d:version}']),
  # firebird/src/jrd/IntlManager.cpp:762
  MsgDesc(msg_id=59, severity=Severity.ERROR, facility=Facility.INTL,
          msg_format=['INTL plugin conflict: ', '{s:name}', ' defined in ', '{s:module_name}', ' and ', '{s:filename}']),
  # firebird/src/jrd/IntlManager.cpp:785
  MsgDesc(msg_id=60, severity=Severity.ERROR, facility=Facility.INTL,
          msg_format=['Unsupported character set ', '{s:charset}', '.. Only ASCII-based character sets are supported yet.']),
  # firebird/src/jrd/IntlManager.cpp:792
  MsgDesc(msg_id=61, severity=Severity.ERROR, facility=Facility.INTL,
          msg_format=['Unsupported character set ', '{s:charset}', '.. Wide character sets are not supported yet.']),
  # firebird/src/jrd/IntlManager.cpp:808
  MsgDesc(msg_id=62, severity=Severity.ERROR, facility=Facility.INTL,
          msg_format=['Unsupported character set ', '{s:charset}', '.. Wide space is not supported yet.']),
  # firebird/src/jrd/CryptoManager.cpp:274
  MsgDesc(msg_id=63, severity=Severity.ERROR, facility=Facility.SYSTEM,
          msg_format=['IO error loop Unwind to avoid a hang']),
  # firebird/src/jrd/event.cpp:961
  MsgDesc(msg_id=64, severity=Severity.ERROR, facility=Facility.SYSTEM,
          msg_format=['Out of memory. Failed to post all events.']),
  # firebird/src/jrd/pag.cpp:2349
  MsgDesc(msg_id=65, severity=Severity.CRITICAL, facility=Facility.FILEIO,
          msg_format=['Error extending file "', '{s:filename}', '" by ', '{d:ext_pages}', ' page(s).\nCurrently allocated ', '{d:max_page_number}', ' pages, requested page number ', '{d:page_num}']),
  # firebird/src/jrd/lck.cpp:881
  MsgDesc(msg_id=66, severity=Severity.CRITICAL, facility=Facility.SYSTEM,
          msg_format=['Fatal lock interface error: ', '{s:error}']),
  # firebird/src/jrd/blb.cpp:435
  MsgDesc(msg_id=67, severity=Severity.WARNING, facility=Facility.SYSTEM,
          msg_format=['going blob (', '{s:blob_id}', ') is not owned by relation (id = ', '{d:relation_id}', '), ignored']),
  # firebird/src/jrd/blb.cpp:474
  MsgDesc(msg_id=68, severity=Severity.WARNING, facility=Facility.SYSTEM,
          msg_format=['staying blob (', '{s:blob_id}', ') is not owned by relation (id = ', '{d:relation_id}', '), ignored']),
  # firebird/src/jrd/fun.epp:168
  MsgDesc(msg_id=69, severity=Severity.ERROR, facility=Facility.SYSTEM,
          msg_format=["ib_util init failed, UDFs can't be used - looks like firebird misconfigured\n", '{s:msg_1}', '\n', '{s:msg_2}', '\n', '{s:msg_3}', '\n', '{s:msg_4}']),
  # firebird/src/jrd/dfw.epp:767
  MsgDesc(msg_id=70, severity=Severity.ERROR, facility=Facility.USER,
          msg_format=['Modifying ', '{s:type}', ' ', '{s:name}', ' which is currently in use by active user requests']),
  # firebird/src/jrd/dfw.epp:919
  MsgDesc(msg_id=71, severity=Severity.ERROR, facility=Facility.USER,
          msg_format=['Deleting ', '{s:type}', ' ', '{s:name}', ' which is currently in use by active user requests']),
  # firebird/src/jrd/Mapping.cpp:678
  MsgDesc(msg_id=72, severity=Severity.ERROR, facility=Facility.SYSTEM,
          msg_format=['MappingIpc::clearMap() failed to find current process ', '{d:process_id}', ' in shared memory']),
  # firebird/src/jrd/jrd.cpp:4150
  MsgDesc(msg_id=73, severity=Severity.WARNING, facility=Facility.SYSTEM,
          msg_format=['Shutting down the server with ', '{d:con_count}', ' active connection(s) to ', '{d:db_count}', ' database(s), ', '{d:svc_count}', ' active service(s)']),
  # firebird/src/jrd/jrd.cpp:5369
  MsgDesc(msg_id=74, severity=Severity.ERROR, facility=Facility.FILEIO,
          msg_format=['Failed to open ', '{s:filename}']),
  # firebird/src/jrd/validation.cpp:1037
  MsgDesc(msg_id=75, severity=Severity.INFO, facility=Facility.VALIDATION,
          msg_format=['Database: ', '{s:database}', '\nValidation started']),
  # firebird/src/jrd/validation.cpp:1058
  MsgDesc(msg_id=76, severity=Severity.INFO, facility=Facility.VALIDATION,
          msg_format=['Database: ', '{s:database}', '\nValidation finished: ', '{d:errors}', ' errors, ', '{d:warnings}', ' warnings, ', '{d:fixed}', ' fixed']),
  # firebird/src/jrd/validation.cpp:1162
  MsgDesc(msg_id=77, severity=Severity.ERROR, facility=Facility.VALIDATION,
          msg_format=['Database: ', '{s:database}', '\nError: Page ', '{d:page_num}', ' wrong type (expected ', '{d:expected}', ' encountered ', '{d:found}', ')', 'OPTIONAL', ' in table ', '{s:table}', ' (', '{d:reletion_id}', ')']),
  # firebird/src/jrd/validation.cpp:1162
  MsgDesc(msg_id=78, severity=Severity.ERROR, facility=Facility.VALIDATION,
          msg_format=['Database: ', '{s:database}', '\nError: Checksum error on page ', '{d:page_num}', 'OPTIONAL', ' in table ', '{s:table}', ' (', '{d:reletion_id}', ')']),
  # firebird/src/jrd/validation.cpp:1162
  MsgDesc(msg_id=79, severity=Severity.ERROR, facility=Facility.VALIDATION,
          msg_format=['Database: ', '{s:database}', '\nError: Page ', '{d:page_num}', ' doubly allocated']),
  # firebird/src/jrd/validation.cpp:1162
  MsgDesc(msg_id=80, severity=Severity.ERROR, facility=Facility.VALIDATION,
          msg_format=['Database: ', '{s:database}', '\nError: Page ', '{d:page_num}', ' is used but marked free', 'OPTIONAL', ' in table ', '{s:table}', ' (', '{d:reletion_id}', ')']),
  # firebird/src/jrd/validation.cpp:1162
  MsgDesc(msg_id=81, severity=Severity.WARNING, facility=Facility.VALIDATION,
          msg_format=['Database: ', '{s:database}', '\nWarning: Page ', '{d:page_num}', ' is an orphan']),
  # firebird/src/jrd/validation.cpp:1162
  MsgDesc(msg_id=82, severity=Severity.WARNING, facility=Facility.VALIDATION,
          msg_format=['Database: ', '{s:database}', '\nWarning: Blob ', '{s:blob_id}', ' appears inconsistent', 'OPTIONAL', ' in table ', '{s:table}', ' (', '{d:reletion_id}', ')']),
  # firebird/src/jrd/validation.cpp:1162
  MsgDesc(msg_id=83, severity=Severity.ERROR, facility=Facility.VALIDATION,
          msg_format=['Database: ', '{s:database}', '\nError: Blob ', '{s:blob_id}', ' is corrupt', 'OPTIONAL', ' in table ', '{s:table}', ' (', '{d:reletion_id}', ')']),
  # firebird/src/jrd/validation.cpp:1162
  MsgDesc(msg_id=84, severity=Severity.ERROR, facility=Facility.VALIDATION,
          msg_format=['Database: ', '{s:database}', '\nError: Blob ', '{s:blob_id}', ' is truncated', 'OPTIONAL', ' in table ', '{s:table}', ' (', '{d:reletion_id}', ')']),
  # firebird/src/jrd/validation.cpp:1162
  MsgDesc(msg_id=85, severity=Severity.ERROR, facility=Facility.VALIDATION,
          msg_format=['Database: ', '{s:database}', '\nError: Chain for record ', '{s:record_id}', ' is broken', 'OPTIONAL', ' in table ', '{s:table}', ' (', '{d:reletion_id}', ')']),
  # firebird/src/jrd/validation.cpp:1162
  MsgDesc(msg_id=86, severity=Severity.ERROR, facility=Facility.VALIDATION,
          msg_format=['Database: ', '{s:database}', '\nError: Data page ', '{d:page_num}', ' {sequence ', '{d:sequence}', '} is confused', 'OPTIONAL', ' in table ', '{s:table}', ' (', '{d:reletion_id}', ')']),
  # firebird/src/jrd/validation.cpp:1162
  MsgDesc(msg_id=87, severity=Severity.ERROR, facility=Facility.VALIDATION,
          msg_format=['Database: ', '{s:database}', '\nError: Data page ', '{d:page_num}', ' {sequence ', '{d:sequence}', '}, line ', '{d:line}', ' is bad', 'OPTIONAL', ' in table ', '{s:table}', ' (', '{d:reletion_id}', ')']),
  # firebird/src/jrd/validation.cpp:1162
  MsgDesc(msg_id=88, severity=Severity.ERROR, facility=Facility.VALIDATION,
          msg_format=['Database: ', '{s:database}', '\nError: Index ', '{d:index_no}', ' is corrupt on page ', '{d:page_num}', ' level ', '{d:level}', ' at offset ', '{d:offset}', '. File: ', '{s:filename}', ', line: ', '{d:line}', '\n', 'OPTIONAL', ' in table ', '{s:table}', ' (', '{d:reletion_id}', ')']),
  # firebird/src/jrd/validation.cpp:1162
  MsgDesc(msg_id=89, severity=Severity.ERROR, facility=Facility.VALIDATION,
          msg_format=['Database: ', '{s:database}', '\nError: Pointer page {sequence ', '{d:sequence}', '} lost', 'OPTIONAL', ' in table ', '{s:table}', ' (', '{d:reletion_id}', ')']),
  # firebird/src/jrd/validation.cpp:1162
  MsgDesc(msg_id=90, severity=Severity.ERROR, facility=Facility.VALIDATION,
          msg_format=['Database: ', '{s:database}', '\nError: Pointer page ', '{d:page_num}', ' {sequence ', '{d:sequence}', '} inconsistent', 'OPTIONAL', ' in table ', '{s:table}', ' (', '{d:reletion_id}', ')']),
  # firebird/src/jrd/validation.cpp:1162
  MsgDesc(msg_id=91, severity=Severity.ERROR, facility=Facility.VALIDATION,
          msg_format=['Database: ', '{s:database}', '\nError: Record ', '{s:record_id}', ' is marked as damaged', 'OPTIONAL', ' in table ', '{s:table}', ' (', '{d:reletion_id}', ')']),
  # firebird/src/jrd/validation.cpp:1162
  MsgDesc(msg_id=92, severity=Severity.ERROR, facility=Facility.VALIDATION,
          msg_format=['Database: ', '{s:database}', '\nError: Record ', '{s:record_id}', ' has bad transaction ', '{s:tras_id}', 'OPTIONAL', ' in table ', '{s:table}', ' (', '{d:reletion_id}', ')']),
  # firebird/src/jrd/validation.cpp:1162
  MsgDesc(msg_id=93, severity=Severity.ERROR, facility=Facility.VALIDATION,
          msg_format=['Database: ', '{s:database}', '\nError: Fragmented record ', '{s:record_id}', ' is corrupt', 'OPTIONAL', ' in table ', '{s:table}', ' (', '{d:reletion_id}', ')']),
  # firebird/src/jrd/validation.cpp:1162
  MsgDesc(msg_id=94, severity=Severity.ERROR, facility=Facility.VALIDATION,
          msg_format=['Database: ', '{s:database}', '\nError: Record ', '{s:record_id}', ' is wrong length', 'OPTIONAL', ' in table ', '{s:table}', ' (', '{d:reletion_id}', ')']),
  # firebird/src/jrd/validation.cpp:1162
  MsgDesc(msg_id=95, severity=Severity.ERROR, facility=Facility.VALIDATION,
          msg_format=['Database: ', '{s:database}', '\nError: Missing index root page', 'OPTIONAL', ' in table ', '{s:table}', ' (', '{d:reletion_id}', ')']),
  # firebird/src/jrd/validation.cpp:1162
  MsgDesc(msg_id=96, severity=Severity.ERROR, facility=Facility.VALIDATION,
          msg_format=['Database: ', '{s:database}', '\nError: Transaction inventory pages lost', 'OPTIONAL', ' in table ', '{s:table}', ' (', '{d:reletion_id}', ')']),
  # firebird/src/jrd/validation.cpp:1162
  MsgDesc(msg_id=97, severity=Severity.ERROR, facility=Facility.VALIDATION,
          msg_format=['Database: ', '{s:database}', '\nError: Transaction inventory page lost, sequence ', '{d:sequence}', 'OPTIONAL', ' in table ', '{s:table}', ' (', '{d:reletion_id}', ')']),
  # firebird/src/jrd/validation.cpp:1162
  MsgDesc(msg_id=98, severity=Severity.ERROR, facility=Facility.VALIDATION,
          msg_format=['Database: ', '{s:database}', '\nError: Transaction inventory pages confused, sequence ', '{d:sequence}', 'OPTIONAL', ' in table ', '{s:table}', ' (', '{d:reletion_id}', ')']),
  # firebird/src/jrd/validation.cpp:1162
  MsgDesc(msg_id=99, severity=Severity.WARNING, facility=Facility.VALIDATION,
          msg_format=['Database: ', '{s:database}', '\nWarning: Relation has ', '{d:number}', ' orphan backversions {', '{d:in_use}', ' in use}', 'OPTIONAL', ' in table ', '{s:table}', ' (', '{d:reletion_id}', ')']),
  # firebird/src/jrd/validation.cpp:1162
  MsgDesc(msg_id=100, severity=Severity.ERROR, facility=Facility.VALIDATION,
          msg_format=['Database: ', '{s:database}', '\nError: Index ', '{d:index_id}', ' is corrupt {missing entries for record ', '{d:record_id}', '}', 'OPTIONAL', ' in table ', '{s:table}', ' (', '{d:reletion_id}', ')']),
  # firebird/src/jrd/validation.cpp:1162
  MsgDesc(msg_id=101, severity=Severity.WARNING, facility=Facility.VALIDATION,
          msg_format=['Database: ', '{s:database}', '\nWarning: Index ', '{d:index_id}', ' has orphan child page at page ', '{d:page_num}', 'OPTIONAL', ' in table ', '{s:table}', ' (', '{d:reletion_id}', ')']),
  # firebird/src/jrd/validation.cpp:1162
  MsgDesc(msg_id=102, severity=Severity.ERROR, facility=Facility.VALIDATION,
          msg_format=['Database: ', '{s:database}', '\nError: Index ', '{d:index_id}', ' has a circular reference at page ', '{d:page_num}', 'OPTIONAL', ' in table ', '{s:table}', ' (', '{d:reletion_id}', ')']),
  # firebird/src/jrd/validation.cpp:1162
  MsgDesc(msg_id=103, severity=Severity.ERROR, facility=Facility.VALIDATION,
          msg_format=['Database: ', '{s:database}', "\nError: SCN's page ", '{d:page_num}', ' {sequence ', '{d:sequence}', '} inconsistent', 'OPTIONAL', ' in table ', '{s:table}', ' (', '{d:reletion_id}', ')']),
  # firebird/src/jrd/validation.cpp:1162
  MsgDesc(msg_id=104, severity=Severity.WARNING, facility=Facility.VALIDATION,
          msg_format=['Database: ', '{s:database}', '\nWarning: Page ', '{d:page_num}', ' has SCN ', '{d:scn}', " while at SCN's page it is ", '{d:scn_2}', 'OPTIONAL', ' in table ', '{s:table}', ' (', '{d:reletion_id}', ')']),
  # firebird/src/jrd/validation.cpp:1162
  MsgDesc(msg_id=105, severity=Severity.ERROR, facility=Facility.VALIDATION,
          msg_format=['Database: ', '{s:database}', '\nError: Blob ', '{s:blob_id}', ' has unknown level ', '{d:level}', ' instead of {0, 1, 2}', 'OPTIONAL', ' in table ', '{s:table}', ' (', '{d:reletion_id}', ')']),
  # firebird/src/jrd/validation.cpp:1162
  MsgDesc(msg_id=106, severity=Severity.WARNING, facility=Facility.VALIDATION,
          msg_format=['Database: ', '{s:database}', '\nWarning: Index ', '{d:index_id}', ' has inconsistent left sibling pointer, page ', '{d:page_num}', ' level ', '{d:level}', ' at offset ', '{d:offset}', 'OPTIONAL', ' in table ', '{s:table}', ' (', '{d:reletion_id}', ')']),
  # firebird/src/jrd/validation.cpp:1162
  MsgDesc(msg_id=107, severity=Severity.WARNING, facility=Facility.VALIDATION,
          msg_format=['Database: ', '{s:database}', '\nWarning: Index ', '{d:index_id}', ' misses node on page ', '{d:page_num}', ' level ', '{d:level}', ' at offset ', '{d:offset}', 'OPTIONAL', ' in table ', '{s:table}', ' (', '{d:reletion_id}', ')']),
  # firebird/src/jrd/validation.cpp:1162
  MsgDesc(msg_id=108, severity=Severity.WARNING, facility=Facility.VALIDATION,
          msg_format=['Database: ', '{s:database}', '\nWarning: PIP ', '{d:pip_num}', ' (seq ', '{d:sequence}', ') have wrong pip_min (', '{d:pip_wrong}', '). Correct is ', '{d:pip_correct}', 'OPTIONAL', ' in table ', '{s:table}', ' (', '{d:reletion_id}', ')']),
  # firebird/src/jrd/validation.cpp:1162
  MsgDesc(msg_id=109, severity=Severity.WARNING, facility=Facility.VALIDATION,
          msg_format=['Database: ', '{s:database}', '\nWarning: PIP ', '{d:pip_num}', ' (seq ', '{d:sequence}', ') have wrong pip_extent (', '{d:pip_wrong}', '). Correct is ', '{d:pip_correct}', 'OPTIONAL', ' in table ', '{s:table}', ' (', '{d:reletion_id}', ')']),
  # firebird/src/jrd/validation.cpp:1162
  MsgDesc(msg_id=110, severity=Severity.WARNING, facility=Facility.VALIDATION,
          msg_format=['Database: ', '{s:database}', '\nWarning: PIP ', '{d:pip_num}', ' (seq ', '{d:sequence}', ') have wrong pip_used (', '{d:pip_wrong}', '). Correct is ', '{d:pip_correct}', 'OPTIONAL', ' in table ', '{s:table}', ' (', '{d:reletion_id}', ')']),
  # firebird/src/jrd/validation.cpp:1162
  MsgDesc(msg_id=111, severity=Severity.WARNING, facility=Facility.VALIDATION,
          msg_format=['Database: ', '{s:database}', '\nWarning: Pointer page ', '{d:page_num}', ' {sequence ', '{d:sequence}', '} bits {0x', '{s:bits}', ' ', '{s:value}', '} are not consistent with data page ', '{d:page_num2}', ' {sequence ', '{d:sequence2}', '} state {0x', '{s:bits2}', ' ', '{s:value2}', '}', 'OPTIONAL', ' in table ', '{s:table}', ' (', '{d:reletion_id}', ')']),
  # firebird/src/jrd/validation.cpp:1162
  MsgDesc(msg_id=112, severity=Severity.ERROR, facility=Facility.VALIDATION,
          msg_format=['Database: ', '{s:database}', '\nError: Data page ', '{d:page_num}', ' marked as free in PIP (', '{d:value_1}', ':', '{d:value_2}', ')', 'OPTIONAL', ' in table ', '{s:table}', ' (', '{d:reletion_id}', ')']),
  # firebird/src/jrd/validation.cpp:1162
  MsgDesc(msg_id=113, severity=Severity.ERROR, facility=Facility.VALIDATION,
          msg_format=['Database: ', '{s:database}', '\nError: Data page ', '{d:page_num}', ' is not in PP (', '{d:pp}', '). Slot (', '{d:slot}', ') is not found', 'OPTIONAL', ' in table ', '{s:table}', ' (', '{d:reletion_id}', ')']),
  # firebird/src/jrd/validation.cpp:1162
  MsgDesc(msg_id=114, severity=Severity.ERROR, facility=Facility.VALIDATION,
          msg_format=['Database: ', '{s:database}', '\nError: Data page ', '{d:page_num}', ' is not in PP (', '{d:pp}', '). Slot (', '{d:slot}', ') has value ', '{d:value}', 'OPTIONAL', ' in table ', '{s:table}', ' (', '{d:reletion_id}', ')']),
  # firebird/src/jrd/validation.cpp:1162
  MsgDesc(msg_id=115, severity=Severity.ERROR, facility=Facility.VALIDATION,
          msg_format=['Database: ', '{s:database}', '\nError: Pointer page is not found for data page ', '{d:page_num}', '. dpg_sequence (', '{d:sequence}', ') is invalid', 'OPTIONAL', ' in table ', '{s:table}', ' (', '{d:reletion_id}', ')']),
  # firebird/src/jrd/validation.cpp:1162
  MsgDesc(msg_id=116, severity=Severity.ERROR, facility=Facility.VALIDATION,
          msg_format=['Database: ', '{s:database}', '\nError: Data page ', '{d:page_num}', ' {sequence ', '{d:sequence}', '} marked as secondary but contains primary record versions', 'OPTIONAL', ' in table ', '{s:table}', ' (', '{d:reletion_id}', ')']),
  # firebird/src/jrd/validation.cpp:3110
  MsgDesc(msg_id=117, severity=Severity.ERROR, facility=Facility.VALIDATION,
          msg_format=['bugcheck during scan of table ', '{d:relation_id}', 'OPTIONAL', ' (', '{s:table_name}', ')']),
  # firebird/src/jrd/cch.cpp:902
  MsgDesc(msg_id=118, severity=Severity.ERROR, facility=Facility.SYSTEM,
          msg_format=['IO error loop Unwind to avoid a hang']),
  # firebird/src/jrd/cch.cpp:1505
  MsgDesc(msg_id=119, severity=Severity.WARNING, facility=Facility.SYSTEM,
          msg_format=['Database: ', '{s:database}', '\nAllocated ', '{d:allocated}', ' page buffers of ', '{d:requested}', ' requested"']),
  # firebird/src/jrd/sdw.cpp:319
  MsgDesc(msg_id=120, severity=Severity.ERROR, facility=Facility.SYSTEM,
          msg_format=['shadow ', '{s:shadow}', ' deleted from database ', '{s:database}', ' due to unavailability on write']),
  # firebird/src/jrd/sdw.cpp:401
  MsgDesc(msg_id=121, severity=Severity.INFO, facility=Facility.SYSTEM,
          msg_format=['conditional shadow ', '{d:shadow_num}', ' ', '{s:shadow}', ' activated for database ', '{s:database}']),
  # firebird/src/jrd/sdw.cpp:458
  MsgDesc(msg_id=122, severity=Severity.INFO, facility=Facility.SYSTEM,
          msg_format=['conditional shadow dumped for database ', '{s:database}']),
  # firebird/src/jrd/sdw.cpp:1107
  MsgDesc(msg_id=123, severity=Severity.ERROR, facility=Facility.SYSTEM,
          msg_format=['shadow ', '{s:shadow}', ' deleted from database ', '{s:database}', ' due to unavailability on attach']),
  # firebird/src/jrd/sdw.cpp:1130
  MsgDesc(msg_id=124, severity=Severity.INFO, facility=Facility.SYSTEM,
          msg_format=['activating shadow file ', '{s:shadow}']),
  # firebird/src/jrd/tra.cpp:2409
  MsgDesc(msg_id=125, severity=Severity.ERROR, facility=Facility.SYSTEM,
          msg_format=['Failure working with transactions list: ', '{s:reason}']),
  # firebird/src/jrd/tra.cpp:3773
  MsgDesc(msg_id=126, severity=Severity.INFO, facility=Facility.SWEEP,
          msg_format=['Sweep is started by ', '{s:user}', '\nDatabase "', '{s:database}', '"\nOIT ', '{d:oit}', ', OAT ', '{d:oat}', ', OST ', '{d:ost}', ', Next ', '{d:next}']),
  # firebird/src/jrd/tra.cpp:3862
  MsgDesc(msg_id=127, severity=Severity.INFO, facility=Facility.SWEEP,
          msg_format=['Sweep is finished\nDatabase "', '{s:database}', '"\nOIT ', '{d:oit}', ', OAT ', '{d:oat}', ', OST ', '{d:ost}', ', Next ', '{d:next}']),
  # firebird/src/jrd/trace/TraceConfigStorage.cpp:272
  MsgDesc(msg_id=128, severity=Severity.ERROR, facility=Facility.CONFIG,
          msg_format=['Audit configuration file "', '{s:filename}', '" is empty']),
  # firebird/src/jrd/trace/TraceManager.cpp:67
  MsgDesc(msg_id=129, severity=Severity.ERROR, facility=Facility.PLUGIN,
          msg_format=['Trace plugin ', '{s:module}', ' returned error on call ', '{s:function}', ', did not create plugin and provided no additional details on reasons of failure']),
  # firebird/src/jrd/trace/TraceManager.cpp:77
  MsgDesc(msg_id=130, severity=Severity.ERROR, facility=Facility.PLUGIN,
          msg_format=['Trace plugin ', '{s:module}', ' returned error on call ', '{s:function}', ', but provided no additional details on reasons of failure']),
  # firebird/src/jrd/trace/TraceManager.cpp:82
  MsgDesc(msg_id=131, severity=Severity.ERROR, facility=Facility.PLUGIN,
          msg_format=['Trace plugin ', '{s:module}', ' returned error on call ', '{s:function}', '.\nError details: ', '{s:error}']),
  # firebird/src/jrd/os/win32/winnt.cpp:1133
  MsgDesc(msg_id=132, severity=Severity.WARNING, facility=Facility.SYSTEM,
          msg_format=['Incorrect FileSystemCacheSize setting ', '{d:value}', '. Using default (30 percent).']),
  # firebird/src/jrd/os/win32/winnt.cpp:1180
  MsgDesc(msg_id=133, severity=Severity.ERROR, facility=Facility.SYSTEM,
          msg_format=['GetSystemFileCacheSize error ', '{d:err_code}']),
  # firebird/src/jrd/os/win32/winnt.cpp:1196
  MsgDesc(msg_id=134, severity=Severity.ERROR, facility=Facility.SYSTEM,
          msg_format=['Could not use 32-bit SetSystemFileCacheSize API to set cache size limit to ', '{d:value}', '. Please use 64-bit engine or configure cache size limit externally']),
  # firebird/src/jrd/os/win32/winnt.cpp:1205
  MsgDesc(msg_id=135, severity=Severity.ERROR, facility=Facility.SYSTEM,
          msg_format=['OpenProcessToken error ', '{d:err_code}']),
  # firebird/src/jrd/os/win32/winnt.cpp:1218
  MsgDesc(msg_id=136, severity=Severity.WARNING, facility=Facility.SYSTEM,
          msg_format=['SetSystemFileCacheSize error ', '{d:err_code}', '. The engine will continue to operate, but the system performance may degrade significantly when working with large databases']),
  # firebird/src/iscguard/iscguard.cpp:271
  MsgDesc(msg_id=137, severity=Severity.ERROR, facility=Facility.GUARDIAN,
          msg_format=['The guardian was unable to launch the server thread.']),
  # firebird/src/iscguard/iscguard.cpp:284
  MsgDesc(msg_id=138, severity=Severity.ERROR, facility=Facility.GUARDIAN,
          msg_format=['The Firebird Guardian failed to startup\nbecause another instance of the guardian\nis already running.']),
  # firebird/src/iscguard/iscguard.cpp:852
  MsgDesc(msg_id=139, severity=Severity.ERROR, facility=Facility.GUARDIAN,
          msg_format=['Create property sheet window failed. Error code ', '{d:err_code}']),
  # firebird/src/iscguard/iscguard.cpp:1138
  MsgDesc(msg_id=140, severity=Severity.ERROR, facility=Facility.GUARDIAN,
          msg_format=['Error opening Windows NT Event Log']),
  # firebird/src/iscguard/iscguard.cpp:1181
  MsgDesc(msg_id=141, severity=Severity.ERROR, facility=Facility.GUARDIAN,
          msg_format=['Unable to update NT Event Log.\nOS Message: ', '{s:message}']),
  # firebird/src/iscguard/iscguard.cpp:1190
  MsgDesc(msg_id=142, severity=Severity.ERROR, facility=Facility.GUARDIAN,
          msg_format=['{s:path}', ' : The guardian was unable to launch the server thread. errno : ', '{d:err_code}']),
  # firebird/src/iscguard/iscguard.cpp:1190
  MsgDesc(msg_id=143, severity=Severity.INFO, facility=Facility.GUARDIAN,
          msg_format=['Guardian starting: ', '{s:path}']),
  # firebird/src/iscguard/iscguard.cpp:1190
  MsgDesc(msg_id=144, severity=Severity.ERROR, facility=Facility.GUARDIAN,
          msg_format=['{s:path}', ': terminated because of startup errors (', '{d:err_code}', ')']),
  # firebird/src/iscguard/iscguard.cpp:1190
  MsgDesc(msg_id=145, severity=Severity.CRITICAL, facility=Facility.GUARDIAN,
          msg_format=['{s:path}', ': terminated abnormally (', '{d:err_code}', ')']),
  # firebird/src/iscguard/iscguard.cpp:1190
  MsgDesc(msg_id=146, severity=Severity.INFO, facility=Facility.GUARDIAN,
          msg_format=['{s:path}', ': normal shutdown']),
  # firebird/src/iscguard/cntl_guard.cpp:185
  MsgDesc(msg_id=147, severity=Severity.ERROR, facility=Facility.GUARDIAN,
          msg_format=['SC manager error ', '{d:err_code}']),
  # firebird/src/iscguard/cntl_guard.cpp:196
  MsgDesc(msg_id=148, severity=Severity.ERROR, facility=Facility.GUARDIAN,
          msg_format=['open services error ', '{d:err_code}']),
  # firebird/src/iscguard/cntl_guard.cpp:205
  MsgDesc(msg_id=149, severity=Severity.ERROR, facility=Facility.GUARDIAN,
          msg_format=['Control services error ', '{d:err_code}']),
  # firebird/src/utilities/ntrace/PluginLogWriter.cpp:227
  MsgDesc(msg_id=150, severity=Severity.ERROR, facility=Facility.PLUGIN,
          msg_format=['PluginLogWriter: mutex ', '{s:value}', ' error, status = ', '{d:state}']),
  # firebird/src/utilities/guard/util.cpp:93
  MsgDesc(msg_id=151, severity=Severity.INFO, facility=Facility.GUARDIAN,
          msg_format=['{s:prog_name}', ': guardian starting ', '{s:value}']),
  # firebird/src/utilities/guard/guard.cpp:195
  MsgDesc(msg_id=152, severity=Severity.ERROR, facility=Facility.GUARDIAN,
          msg_format=['{s:prog_name}', ': guardian could not start server']),
  # firebird/src/utilities/guard/guard.cpp:211
  MsgDesc(msg_id=153, severity=Severity.ERROR, facility=Facility.GUARDIAN,
          msg_format=['{s:prog_name}', ': guardian could not open ', '{s:filename}', ' for writing, error ', '{d:err_code}']),
  # firebird/src/utilities/guard/guard.cpp:230
  MsgDesc(msg_id=154, severity=Severity.ERROR, facility=Facility.GUARDIAN,
          msg_format=['{s:prog_name}', ': error while shutting down ', '{s"process_name}', ' (', '{d:err_code}', ')']),
  # firebird/src/utilities/guard/guard.cpp:235
  MsgDesc(msg_id=155, severity=Severity.ERROR, facility=Facility.GUARDIAN,
          msg_format=['{s:prog_name}', ': ', '{s:process_name}', ' killed (did not terminate)']),
  # firebird/src/utilities/guard/guard.cpp:238
  MsgDesc(msg_id=156, severity=Severity.ERROR, facility=Facility.GUARDIAN,
          msg_format=['{s:prog_name}', ': unable to shutdown ', '{s:process_name}']),
  # firebird/src/utilities/guard/guard.cpp:241
  MsgDesc(msg_id=157, severity=Severity.INFO, facility=Facility.GUARDIAN,
          msg_format=['{s:prog_name}', ': ', '{s:process_name}', ' terminated']),
  # firebird/src/utilities/guard/guard.cpp:251
  MsgDesc(msg_id=158, severity=Severity.ERROR, facility=Facility.GUARDIAN,
          msg_format=['{s:prog_name}', ': ', '{s:process_name}', ' terminated due to startup error (', '{d:err_code}', ')']),
  # firebird/src/utilities/guard/guard.cpp:255
  MsgDesc(msg_id=159, severity=Severity.WARNING, facility=Facility.GUARDIAN,
          msg_format=['{s:prog_name}', ': ', '{s:process_name}', ' terminated due to startup error (', '{d:err_code}', ')\n Trying again']),
  # firebird/src/utilities/guard/guard.cpp:262
  MsgDesc(msg_id=160, severity=Severity.ERROR, facility=Facility.GUARDIAN,
          msg_format=['{s:prog_name}', ': ', '{s:process_name}', ' terminated due to startup error (', '{d:err_code}', ')']),
  # firebird/src/utilities/guard/guard.cpp:270
  MsgDesc(msg_id=161, severity=Severity.ERROR, facility=Facility.GUARDIAN,
          msg_format=['{s:prog_name}', ': ', '{s:process_name}', ' terminated abnormally (', '{d:err_code}', ')']),
  # firebird/src/utilities/guard/guard.cpp:278
  MsgDesc(msg_id=162, severity=Severity.INFO, facility=Facility.GUARDIAN,
          msg_format=['{s:prog_name}', ': ', '{s:process_name}', ' normal shutdown.']),
  # firebird/src/remote/inet.cpp:909
  MsgDesc(msg_id=163, severity=Severity.ERROR, facility=Facility.NET,
          msg_format=['INET/INET_connect: getaddrinfo(', '{s:host}', ',', '{s:protocol}', ') failed: ', '{s:error}']),
  # firebird/src/remote/inet.cpp:921
  MsgDesc(msg_id=164, severity=Severity.ERROR, facility=Facility.NET,
          msg_format=['socket: error creating socket (family ', '{d:family}', ', socktype ', '{d:socket_type}', ', protocol ', '{d:protocol}']),
  # firebird/src/remote/inet.cpp:933
  MsgDesc(msg_id=165, severity=Severity.ERROR, facility=Facility.NET,
          msg_format=['setsockopt: error setting SO_KEEPALIVE']),
  # firebird/src/remote/inet.cpp:938
  MsgDesc(msg_id=166, severity=Severity.ERROR, facility=Facility.NET,
          msg_format=['setsockopt: error setting TCP_NODELAY']),
  # firebird/src/remote/inet.cpp:1000
  MsgDesc(msg_id=167, severity=Severity.ERROR, facility=Facility.NET,
          msg_format=['setsockopt: error setting IPV6_V6ONLY to ', '{d:value}']),
  # firebird/src/remote/inet.cpp:1167
  MsgDesc(msg_id=168, severity=Severity.ERROR, facility=Facility.NET,
          msg_format=['inet server err: setting KEEPALIVE socket option']),
  # firebird/src/remote/inet.cpp:1171
  MsgDesc(msg_id=169, severity=Severity.ERROR, facility=Facility.NET,
          msg_format=['inet server err: setting NODELAY socket option']),
  # firebird/src/remote/inet.cpp:1266
  MsgDesc(msg_id=170, severity=Severity.ERROR, facility=Facility.NET,
          msg_format=['inet_server: unable to cd to ', '{s:home}', ' errno ', '{d:err_code}']),
  # firebird/src/remote/inet.cpp:1530
  MsgDesc(msg_id=171, severity=Severity.ERROR, facility=Facility.NET,
          msg_format=['INET/aux_request: failed to get local address of the original socket']),
  # firebird/src/remote/inet.cpp:1854
  MsgDesc(msg_id=172, severity=Severity.ERROR, facility=Facility.NET,
          msg_format=['INET/inet_error: fork/DuplicateHandle errno = ', '{d:err_code}']),
  # firebird/src/remote/inet.cpp:1883
  MsgDesc(msg_id=173, severity=Severity.ERROR, facility=Facility.NET,
          msg_format=['INET/inet_error: fork/CreateProcess errno = ', '{d:err_code}']),
  # firebird/src/remote/inet.cpp:2219
  MsgDesc(msg_id=174, severity=Severity.ERROR, facility=Facility.NET,
          msg_format=['INET/select_wait: found "not a socket" socket : ', '{d:value}']),
  # firebird/src/remote/inet.cpp:2247
  MsgDesc(msg_id=175, severity=Severity.ERROR, facility=Facility.NET,
          msg_format=['INET/select_wait: client rundown complete, server exiting']),
  # firebird/src/remote/inet.cpp:2299
  MsgDesc(msg_id=176, severity=Severity.ERROR, facility=Facility.NET,
          msg_format=['INET/select_wait: select failed, errno = ', '{d:err_code}']),
  # firebird/src/remote/inet.cpp:2596
  MsgDesc(msg_id=177, severity=Severity.ERROR, facility=Facility.NET,
          msg_format=['INET/inet_error: ', '{s:error}', ' errno = ', '{d:err_code}', 'OPTIONAL', ', ', '{s:parameters}']),
  # firebird/src/remote/server/server.cpp:1565
  MsgDesc(msg_id=178, severity=Severity.CRITICAL, facility=Facility.NET,
          msg_format=['SRVR_multi_thread/RECEIVE: error on main_port, shutting down']),
  # firebird/src/remote/server/server.cpp:1672
  MsgDesc(msg_id=179, severity=Severity.CRITICAL, facility=Facility.SYSTEM,
          msg_format=['SRVR_multi_thread: shutting down due to unhandled exception']),
  # firebird/src/remote/server/server.cpp:1688
  MsgDesc(msg_id=180, severity=Severity.ERROR, facility=Facility.NET,
          msg_format=['SRVR_multi_thread: forcefully disconnecting a port']),
  # firebird/src/remote/server/server.cpp:1744
  MsgDesc(msg_id=181, severity=Severity.CRITICAL, facility=Facility.SYSTEM,
          msg_format=['SRVR_multi_thread: error during startup, shutting down']),
  # firebird/src/remote/server/server.cpp:4433
  MsgDesc(msg_id=182, severity=Severity.CRITICAL, facility=Facility.SYSTEM,
          msg_format=['SERVER/process_packet: Multi-client server shutdown']),
  # firebird/src/remote/server/server.cpp:4601
  MsgDesc(msg_id=183, severity=Severity.ERROR, facility=Facility.NET,
          msg_format=["SERVER/process_packet: don't understand packet type ", '{d:value}']),
  # firebird/src/remote/server/server.cpp:4611
  MsgDesc(msg_id=184, severity=Severity.CRITICAL, facility=Facility.NET,
          msg_format=['SERVER/process_packet: broken port, server exiting']),
  # firebird/src/remote/server/server.cpp:6670
  MsgDesc(msg_id=185, severity=Severity.ERROR, facility=Facility.AUTH,
          msg_format=['Authentication error\nNo matching plugins on server']),
  # firebird/src/remote/server/os/posix/inet_server.cpp:236
  MsgDesc(msg_id=186, severity=Severity.WARNING, facility=Facility.CONFIG,
          msg_format=['Switch -P ignored in CS mode']),
  # firebird/src/remote/server/os/posix/inet_server.cpp:283
  MsgDesc(msg_id=187, severity=Severity.CRITICAL, facility=Facility.CONFIG,
          msg_format=['Server misconfigured - to start it from (x)inetd add ServerMode=Classic to firebird.conf']),
  # firebird/src/remote/server/os/posix/inet_server.cpp:312
  MsgDesc(msg_id=188, severity=Severity.ERROR, facility=Facility.FILEIO,
          msg_format=['Could not change directory to ', '{s:dir}', ' due to errno ', '{d:err_code}']),
  # firebird/src/remote/server/os/posix/inet_server.cpp:381
  MsgDesc(msg_id=189, severity=Severity.CRITICAL, facility=Facility.SYSTEM,
          msg_format=['Unable to start INET_server']),
  # firebird/src/remote/server/os/posix/inet_server.cpp:503
  MsgDesc(msg_id=190, severity=Severity.ERROR, facility=Facility.SYSTEM,
          msg_format=['setrlimit() failed, errno=', '{d:err_code}']),
  # firebird/src/remote/server/os/posix/inet_server.cpp:509
  MsgDesc(msg_id=191, severity=Severity.ERROR, facility=Facility.SYSTEM,
          msg_format=['getrlimit() failed, errno=', '{d:err_code}']),
  # firebird/src/remote/server/os/win32/srvr_w32.cpp:421
  MsgDesc(msg_id=192, severity=Severity.CRITICAL, facility=Facility.SYSTEM,
          msg_format=["INET: can't start worker thread, connection terminated"]),
  # firebird/src/remote/server/os/win32/srvr_w32.cpp:469
  MsgDesc(msg_id=193, severity=Severity.CRITICAL, facility=Facility.SYSTEM,
          msg_format=["WNET: can't start worker thread, connection terminated"]),
  # firebird/src/remote/server/os/win32/srvr_w32.cpp:520
  MsgDesc(msg_id=194, severity=Severity.CRITICAL, facility=Facility.SYSTEM,
          msg_format=["XNET: can't start worker thread, connection terminated"]),
  # firebird/src/remote/server/os/win32/srvr_w32.cpp:670
  MsgDesc(msg_id=195, severity=Severity.ERROR, facility=Facility.SYSTEM,
          msg_format=['SERVER: OpenProcess failed. Errno = ', '{d:err_code}', ', parent PID = ', '{d:parent_pid}']),
  # firebird/src/remote/server/os/win32/srvr_w32.cpp:677
  MsgDesc(msg_id=196, severity=Severity.ERROR, facility=Facility.SYSTEM,
          msg_format=['SERVER: DuplicateHandle failed. Errno = ', '{d:err_code}', ', parent PID = ', '{d:parent_pid}']),
  # firebird/src/remote/server/os/win32/srvr_w32.cpp:801
  MsgDesc(msg_id=197, severity=Severity.WARNING, facility=Facility.SYSTEM,
          msg_format=['Timeout expired during remote server shutdown']),
  # firebird/src/remote/server/os/win32/property.cpp:119
  MsgDesc(msg_id=198, severity=Severity.ERROR, facility=Facility.SYSTEM,
          msg_format=['Create property sheet window failed. Error code ', '{d:err_code}']),
  # firebird/src/remote/server/os/win32/window.cpp:110
  MsgDesc(msg_id=199, severity=Severity.CRITICAL, facility=Facility.SYSTEM,
          msg_format=['Error registering main window class']),
  # firebird/src/remote/os/win32/wnet.cpp:392
  MsgDesc(msg_id=200, severity=Severity.CRITICAL, facility=Facility.SYSTEM,
          msg_format=['WNET/inet_error: fork/CreateProcess errno = ', '{d:err_code}']),
  # firebird/src/remote/os/win32/wnet.cpp:997
  MsgDesc(msg_id=201, severity=Severity.ERROR, facility=Facility.NET,
          msg_format=['WNET/wnet_error: ', '{s:function}', ' errno = ', '{d:err_code}']),
  # firebird/src/remote/os/win32/xnet.cpp:227
  MsgDesc(msg_id=202, severity=Severity.ERROR, facility=Facility.NET,
          msg_format=['XNET error: ', '{s:err_msg}']),
  # firebird/src/remote/os/win32/xnet.cpp:1754
  MsgDesc(msg_id=203, severity=Severity.ERROR, facility=Facility.NET,
          msg_format=['XNET/xnet_error: errno = ', '{d:err_code}']),
  # firebird/src/yvalve/why.cpp:764
  MsgDesc(msg_id=204, severity=Severity.CRITICAL, facility=Facility.SYSTEM,
          msg_format=['Unknown failure in shutdown thread in shutdownSemaphore->enter()']),
  # firebird/src/jrd/event.cpp:568
  MsgDesc(msg_id=205, severity=Severity.CRITICAL, facility=Facility.SYSTEM,
          msg_format=['Event table remap failed']),
  # firebird/src/jrd/event.cpp:632
  MsgDesc(msg_id=206, severity=Severity.CRITICAL, facility=Facility.SYSTEM,
          msg_format=['Event table space exhausted']),
  # firebird/src/jrd/event.cpp:1186
  MsgDesc(msg_id=207, severity=Severity.CRITICAL, facility=Facility.SYSTEM,
          msg_format=['EVENT: ', '{s:event}', ' error, status = ', '{d:err_code}']),
  # firebird/src/jrd/Monitoring.cpp:358
  MsgDesc(msg_id=208, severity=Severity.CRITICAL, facility=Facility.SYSTEM,
          msg_format=['MONITOR: mutex ', '{s:mutex}', ' error, status = ', '{d:err_code}']),
  # firebird/src/jrd/Mapping.cpp:790
  MsgDesc(msg_id=209, severity=Severity.CRITICAL, facility=Facility.SYSTEM,
          msg_format=['Fatal error in clearDeliveryThread']),
  # firebird/src/jrd/trace/TraceConfigStorage.cpp:177
  MsgDesc(msg_id=210, severity=Severity.CRITICAL, facility=Facility.SYSTEM,
          msg_format=['ConfigStorage: mutex ', '{s:mutex}', ' error, status = ', '{d:err_code}']),
  # firebird/src/jrd/trace/TraceLog.cpp:248
  MsgDesc(msg_id=211, severity=Severity.CRITICAL, facility=Facility.SYSTEM,
          msg_format=['TraceLog: mutex ', '{s:mutex}', ' error, status = ', '{d:err_code}']),
  # firebird/src/lock/lock.cpp:1678
  MsgDesc(msg_id=212, severity=Severity.CRITICAL, facility=Facility.SYSTEM,
          msg_format=['Fatal lock manager error: ', '{s:message}', ', errno: ', '{d:err_code}', 'OPTIONAL', '\n--', '{s:error}']),
  # firebird/src/lock/lock.cpp:2394, 2414
  MsgDesc(msg_id=213, severity=Severity.CRITICAL, facility=Facility.SYSTEM,
          msg_format=['Fatal lock manager error: lock manager out of room']),
  # firebird/src/jrd/cch.cpp:4201
  MsgDesc(msg_id=214, severity=Severity.ERROR, facility=Facility.SYSTEM,
          msg_format=['Database: ', '{s:database}', '\npage ', '{d:page_num}', ' page type ', '{d:page_type}', 'lock denied']),
  # firebird/src/jrd/cch.cpp:4244
  MsgDesc(msg_id=215, severity=Severity.ERROR, facility=Facility.SYSTEM,
          msg_format=['Database: ', '{s:database}', '\npage ', '{d:page_num}', ', page type ', '{d:page_type}', ' lock conversion denied']),
  # firebird/src/jrd/tra.cpp:2700
  MsgDesc(msg_id=216, severity=Severity.ERROR, facility=Facility.SWEEP,
          msg_format=['Database: ', '{s:database}', '\ncannot start sweep thread, Out of Memory']),
  # firebird/src/jrd/met.cpp:1956
  MsgDesc(msg_id=217, severity=Severity.ERROR, facility=Facility.SYSTEM,
          msg_format=['Database: ', '{s:database}', '\nRDB$FLAGS for trigger ', '{s:trigger_name}', ' in RDB$TRIGGERS is corrupted']),
  # firebird/src/common/os/win32/os_itils.cpp:160
  MsgDesc(msg_id=218, severity=Severity.ERROR, facility=Facility.FILEIO,
          msg_format=['Error adjusting access rights for folder "', '{s:dir}', '"\n', '{s:exception}']),
  # firebird/src/jrd/IntlManager:557
  MsgDesc(msg_id=219, severity=Severity.ERROR, facility=Facility.CONFIG,
          msg_format=['Error in INTL plugin config file ', '{s:filename}', '\n', '{s:exception}']),
  # firebird/src/jrd/svc.cpp:1985
  MsgDesc(msg_id=220, severity=Severity.ERROR, facility=Facility.SYSTEM,
          msg_format=['Exception in Service::run():\n', '{s:exception}']),
  # firebird/src/jrd/CryptoManager:1110
  MsgDesc(msg_id=221, severity=Severity.ERROR, facility=Facility.SYSTEM,
          msg_format=['Crypt thread:\n', '{s:exception}']),
  # firebird/src/jrd/event.cpp:1414
  MsgDesc(msg_id=222, severity=Severity.ERROR, facility=Facility.SYSTEM,
          msg_format=['Error in event watcher thread\n\n', '{s:exception}']),
  # firebird/src/jrd/event.cpp:1432
  MsgDesc(msg_id=223, severity=Severity.ERROR, facility=Facility.SYSTEM,
          msg_format=['Error closing event watcher thread\n\n', '{s:exception}']),
  # firebird/src/jrd/Monitoring.cpp:160
  MsgDesc(msg_id=224, severity=Severity.ERROR, facility=Facility.SYSTEM,
          msg_format=['MonitoringData: Cannot initialize the shared memory region\n', '{s:exception}']),
  # firebird/src/jrd/Mapping.cpp:733
  MsgDesc(msg_id=225, severity=Severity.ERROR, facility=Facility.SYSTEM,
          msg_format=['MappingIpc: Cannot initialize the shared memory region\n', '{s:exception}']),
  # firebird/src/jrd/Mapping.cpp:789
  MsgDesc(msg_id=226, severity=Severity.ERROR, facility=Facility.SYSTEM,
          msg_format=['Fatal error in clearDeliveryThread\n', '{s:exception}']),
  # firebird/src/jrd/jrd.cpp:7011
  MsgDesc(msg_id=227, severity=Severity.ERROR, facility=Facility.SYSTEM,
          msg_format=['Database: ', '{s:database}', '\nError at disconnect:\n', '{s:exception}']),
  # firebird/src/jrd/jrd.cpp:7408
  MsgDesc(msg_id=228, severity=Severity.ERROR, facility=Facility.SYSTEM,
          msg_format=['error while shutting down attachment\n', '{s:exception}']),
  # firebird/src/jrd/jrd.cpp:7441
  MsgDesc(msg_id=229, severity=Severity.ERROR, facility=Facility.SYSTEM,
          msg_format=['attachmentShutdownThread\n', '{s:exception}']),
  # firebird/src/jrd/jrd.cpp:7514
  MsgDesc(msg_id=230, severity=Severity.ERROR, facility=Facility.SYSTEM,
          msg_format=['Error at shutdown_thread\n', '{s:exception}']),
  # firebird/src/jrd/jrd.cpp:7893
  MsgDesc(msg_id=231, severity=Severity.ERROR, facility=Facility.SYSTEM,
          msg_format=['Error rolling back new transaction\n', '{s:exception}']),
  # firebird/src/jrd/Attachment.cpp:690
  MsgDesc(msg_id=232, severity=Severity.ERROR, facility=Facility.SYSTEM,
          msg_format=['Cannot dump the monitoring data\n', '{s:exception}']),
  # firebird/src/jrd/tra.cpp:1868
  MsgDesc(msg_id=233, severity=Severity.ERROR, facility=Facility.SWEEP,
          msg_format=['Error during sweep of ', '{s:database}', ':\n', '{s:exception}']),
  # firebird/src/jrd/tra.cpp:2685
  MsgDesc(msg_id=234, severity=Severity.ERROR, facility=Facility.SWEEP,
          msg_format=['cannot start sweep thread\n', '{s:exception}']),
  # firebird/src/jrd/trace/TraceConfigStorage.cpp:123
  MsgDesc(msg_id=235, severity=Severity.ERROR, facility=Facility.SYSTEM,
          msg_format=['ConfigStorage: Cannot initialize the shared memory region\n', '{s:exception}']),
  # firebird/src/jrd/trace/TraceConfigStorage.cpp:283
  MsgDesc(msg_id=236, severity=Severity.ERROR, facility=Facility.CONFIG,
          msg_format=['Cannot open audit configuration file\n', '{s:exception}']),
  # firebird/src/jrd/trace/TraceConfigStorage.cpp:609
  MsgDesc(msg_id=237, severity=Severity.ERROR, facility=Facility.FILEIO,
          msg_format=['TouchFile failed\n', '{s:exception}']),
  # firebird/src/jrd/trace/TraceLog.cpp:70
  MsgDesc(msg_id=238, severity=Severity.ERROR, facility=Facility.SYSTEM,
          msg_format=['TraceLog: cannot initialize the shared memory region\n', '{s:exception}']),
  # firebird/src/iscguard/cntl_guard.cpp:110
  MsgDesc(msg_id=239, severity=Severity.ERROR, facility=Facility.GUARDIAN,
          msg_format=['CNTL: cannot start service handler thread\n', '{s:exception}']),
  # firebird/src/remote/server/server.cpp:1345
  MsgDesc(msg_id=240, severity=Severity.ERROR, facility=Facility.SYSTEM,
          msg_format=['SRVR_main\n', '{s:exception}']),
  # firebird/src/remote/server/server.cpp:2502
  MsgDesc(msg_id=241, severity=Severity.ERROR, facility=Facility.SYSTEM,
          msg_format=["Unhandled exception in server's aux_request():\n", '{s:exception}']),
  # firebird/src/remote/server/server.cpp:5771
  MsgDesc(msg_id=242, severity=Severity.ERROR, facility=Facility.SYSTEM,
          msg_format=['start_crypt:\n', '{s:exception}']),
  # firebird/src/remote/server/server.cpp:6156
  MsgDesc(msg_id=243, severity=Severity.ERROR, facility=Facility.NET,
          msg_format=['Error while processing the incoming packet\n', '{s:exception}']),
  # firebird/src/remote/server/os/posix/inet_server.cpp:368
  MsgDesc(msg_id=244, severity=Severity.ERROR, facility=Facility.NET,
          msg_format=['startup:INET_connect:\n', '{s:exception}']),
  # firebird/src/remote/server/os/posix/inet_server.cpp:440
  MsgDesc(msg_id=245, severity=Severity.ERROR, facility=Facility.SYSTEM,
          msg_format=['Firebird startup error:\n', '{s:exception}']),
  # firebird/src/remote/server/os/win32/srvr_w32.cpp:306
  MsgDesc(msg_id=246, severity=Severity.ERROR, facility=Facility.SYSTEM,
          msg_format=['Server error\n', '{s:exception}']),
  # firebird/src/remote/server/os/win32/srvr_w32.cpp:403
  MsgDesc(msg_id=247, severity=Severity.ERROR, facility=Facility.NET,
          msg_format=['INET_connect\n', '{s:exception}']),
  # firebird/src/remote/server/os/win32/srvr_w32.cpp:458
  MsgDesc(msg_id=248, severity=Severity.ERROR, facility=Facility.NET,
          msg_format=['WNET_connect\n', '{s:exception}']),
  # firebird/src/remote/server/os/win32/srvr_w32.cpp:509
  MsgDesc(msg_id=249, severity=Severity.ERROR, facility=Facility.NET,
          msg_format=['XNET_connect\n', '{s:exception}']),
  # firebird/src/remote/server/os/win32/srvr_w32.cpp:567
  MsgDesc(msg_id=250, severity=Severity.ERROR, facility=Facility.SYSTEM,
          msg_format=["INET: can't start listener thread\n", '{s:exception}']),
  # firebird/src/remote/server/os/win32/srvr_w32.cpp:578
  MsgDesc(msg_id=251, severity=Severity.ERROR, facility=Facility.SYSTEM,
          msg_format=["WNET: can't start listener thread\n", '{s:exception}']),
  # firebird/src/remote/server/os/win32/srvr_w32.cpp:589
  MsgDesc(msg_id=252, severity=Severity.ERROR, facility=Facility.SYSTEM,
          msg_format=["XNET: can't start listener thread\n", '{s:exception}']),
  # firebird/src/remote/server/os/win32/cntl.cpp:104
  MsgDesc(msg_id=253, severity=Severity.ERROR, facility=Facility.SYSTEM,
          msg_format=['CNTL: cannot start service handler thread\n', '{s:exception}']),
  # firebird/src/lock/lock.cpp:1550
  MsgDesc(msg_id=254, severity=Severity.ERROR, facility=Facility.SYSTEM,
          msg_format=['Error in blocking action thread\n\n', '{s:exception}']),
  # firebird/src/lock/lock.cpp:1567
  MsgDesc(msg_id=255, severity=Severity.ERROR, facility=Facility.SYSTEM,
          msg_format=['Error closing blocking action thread\n\n', '{s:exception}']),
  # firebird/src/remote/os/win32/xnet.cpp:485
  MsgDesc(msg_id=256, severity=Severity.ERROR, facility=Facility.SYSTEM,
          msg_format=['XNET error: Unable to initialize child process\n', '{s:exception}']),
  # firebird/src/remote/os/win32/xnet.cpp:825
  MsgDesc(msg_id=257, severity=Severity.ERROR, facility=Facility.NET,
          msg_format=['XNET error: aux_connect() failed\n', '{s:exception}']),
  # firebird/src/remote/os/win32/xnet.cpp:960
  MsgDesc(msg_id=258, severity=Severity.ERROR, facility=Facility.NET,
          msg_format=['XNET error: aux_request() failed\n', '{s:exception}']),
  # firebird/src/remote/os/win32/xnet.cpp:1193
  MsgDesc(msg_id=259, severity=Severity.ERROR, facility=Facility.NET,
          msg_format=['XNET error: Server failed to respond on connect request\n', '{s:exception}']),
  # firebird/src/remote/os/win32/xnet.cpp:1394
  MsgDesc(msg_id=260, severity=Severity.ERROR, facility=Facility.SYSTEM,
          msg_format=['XNET error: WaitForSingleObject() failed\n', '{s:exception}']),
  # firebird/src/remote/os/win32/xnet.cpp:1437
  MsgDesc(msg_id=261, severity=Severity.ERROR, facility=Facility.NET,
          msg_format=['XNET error: Failed to allocate server port for communication\n', '{s:exception}']),
  # firebird/src/remote/os/win32/xnet.cpp:1664
  MsgDesc(msg_id=262, severity=Severity.ERROR, facility=Facility.NET,
          msg_format=['XNET error: Server shutdown detected\n', '{s:exception}']),
  # firebird/src/remote/os/win32/xnet.cpp:2277
  MsgDesc(msg_id=263, severity=Severity.ERROR, facility=Facility.SYSTEM,
          msg_format=['XNET error: XNET server initialization failed. Probably another instance of server is already running.\n', '{s:exception}']),
  # firebird/src/remote/os/win32/xnet.cpp:2405
  MsgDesc(msg_id=264, severity=Severity.ERROR, facility=Facility.SYSTEM,
          msg_format=['XNET error: CreateProcess() failed\n', '{s:exception}']),
  # firebird/src/common/unicode_util.cpp:1046
  MsgDesc(msg_id=265, severity=Severity.ERROR, facility=Facility.INTL,
          msg_format=['ICU load error\n', '{s:err_msg}']),
  # firebird/src/common/isc_sync.cpp:564
  MsgDesc(msg_id=266, severity=Severity.ERROR, facility=Facility.SYSTEM,
          msg_format=['Unlock error\n', '{s:err_msg}']),
  # firebird/src/common/isc_sync.cpp:942
  MsgDesc(msg_id=267, severity=Severity.ERROR, facility=Facility.SYSTEM,
          msg_format=['create_semaphores failed:\n', '{s:err_msg}']),
  # firebird/src/common/isc_sync.cpp:1079
  MsgDesc(msg_id=268, severity=Severity.ERROR, facility=Facility.SYSTEM,
          msg_format=['Pthread Error\n', '{s:err_msg}']),
  # firebird/src/common/isc_sync.cpp:1141
  MsgDesc(msg_id=269, severity=Severity.ERROR, facility=Facility.SYSTEM,
          msg_format=['event_init()\n', '{s:err_msg}']),
  # firebird/src/common/isc_sync.cpp:1249
  MsgDesc(msg_id=270, severity=Severity.ERROR, facility=Facility.SYSTEM,
          msg_format=['event_clear()\n', '{s:err_msg}']),
  # firebird/src/common/isc_sync.cpp:2022, 2051
  MsgDesc(msg_id=271, severity=Severity.ERROR, facility=Facility.SYSTEM,
          msg_format=['Pthread Error\n', '{s:err_msg}']),
  # firebird/src/common/isc_sync.cpp:3621
  MsgDesc(msg_id=272, severity=Severity.ERROR, facility=Facility.SYSTEM,
          msg_format=['unmapObject failed\n', '{s:err_msg}']),
  # firebird/src/jrd/Intlmanager.cpp:516
  MsgDesc(msg_id=273, severity=Severity.ERROR, facility=Facility.INTL,
          msg_format=["Can't load INTL module '", '{s:filename}', "'\n", '{s:err_msg}']),
  # firebird/src/jrd/event.cpp:563
  MsgDesc(msg_id=274, severity=Severity.ERROR, facility=Facility.SYSTEM,
          msg_format=['Remap file error:\n', '{s:err_msg}']),
  # firebird/src/jrd/Mapping.cpp:856
  MsgDesc(msg_id=275, severity=Severity.ERROR, facility=Facility.SYSTEM,
          msg_format=['Error when working with user mapping shared memory\n', '{s:err_msg}']),
  # firebird/src/jrd/jrd.cpp:4194
  MsgDesc(msg_id=276, severity=Severity.ERROR, facility=Facility.SYSTEM,
          msg_format=['JProvider::shutdown:\n', '{s:err_msg}']),
  # firebird/src/jrd/validation.cpp:1067
  MsgDesc(msg_id=277, severity=Severity.ERROR, facility=Facility.VALIDATION,
          msg_format=['Database: ', '{s:database}', '\nValidation aborted\n', '{s:err_msg}']),
  # firebird/src/jrd/trace/TraceManager.cpp:338
  MsgDesc(msg_id=278, severity=Severity.ERROR, facility=Facility.PLUGIN,
          msg_format=['Trace plugin ', '{s:name}', ' returned error on call trace_create.\n', '{s:err_msg}']),
  # firebird/src/jrd/extds/ValidatePassword.cpp:249
  MsgDesc(msg_id=279, severity=Severity.ERROR, facility=Facility.AUTH,
          msg_format=['Authentication failed, client plugin:\n', '{s:err_msg}']),
  # firebird/src/jrd/extds/ValidatePassword.cpp:278
  MsgDesc(msg_id=280, severity=Severity.ERROR, facility=Facility.AUTH,
          msg_format=['Authentication faled, server plugin:\n', '{s:err_msg}']),
  # firebird/src/auth/SecurityDatabase/LegacyServer.cpp:433
  MsgDesc(msg_id=281, severity=Severity.ERROR, facility=Facility.AUTH,
          msg_format=['Legacy security database timer handler\n', '{s:err_msg}']),
  # firebird/src/auth/SecurityDatabase/LegacyServer.cpp:465
  MsgDesc(msg_id=282, severity=Severity.ERROR, facility=Facility.AUTH,
          msg_format=['Legacy security database shutdown\n', '{s:err_msg}']),
  # firebird/src/remote/server/server.cpp:737, 2030
  MsgDesc(msg_id=283, severity=Severity.ERROR, facility=Facility.AUTH,
          msg_format=['Authentication error\n', '{s:err_msg}']),
  # firebird/src/remote/client/interface.cpp:1644
  MsgDesc(msg_id=284, severity=Severity.ERROR, facility=Facility.SYSTEM,
          msg_format=['REMOTE INTERFACE/gds__detach: Unsuccesful detach from database.\nUncommitted work may have been lost.\n', '{s:err_msg}']),
  # firebird/src/remote/client/interface.cpp:5357
  MsgDesc(msg_id=285, severity=Severity.ERROR, facility=Facility.AUTH,
          msg_format=['Authentication, client plugin:\n', '{s:err_msg}']),
  # firebird/src/lock/lock.cpp:233
  MsgDesc(msg_id=286, severity=Severity.ERROR, facility=Facility.SYSTEM,
          msg_format=['LockManager::LockManager()\n', '{s:err_msg}']),
  # firebird/src/common.isc.cpp:596
  MsgDesc(msg_id=287, severity=Severity.ERROR, facility=Facility.SYSTEM,
          msg_format=['Database: ', '{s:database}', '\n', '{s:err_msg}']),
]

_r_msgs = []
_h_msgs = {}

for _msg in messages:
    if _msg.msg_format[0].startswith('{'):
        _r_msgs.append(_msg)
    else:
        _parts = _msg.msg_format[0].split()
        _h_msgs.setdefault(_parts[0], []).append(_msg)

_END_CHUNK = object()

def identify_msg(msg: str) -> Optional[Tuple[MsgDesc, Dict[str, Any], bool]]:
    """Identify Firebird log message.

    Arguments:
        msg: Firebird log entry with message to be identified.

    Returns:
        Tuple with matched `.MsgDesc` instance, dictionary with extracted message
        parameters, and boolean flag indicating whether message has optional content. Returns
        `None` if message was not matched against any message descriptor.
    """
    parts = msg.split()
    if parts[0] in _h_msgs:
        candidates = _h_msgs[parts[0]]
    else:
        candidates = _r_msgs
    for candidate in candidates: # pylint: disable=R1702
        chunks = candidate.msg_format.copy()
        chunks.append(_END_CHUNK)
        params = {}
        data = msg
        i = 0
        without_optional = False
        while i < len(chunks):
            chunk = chunks[i]
            if chunk is _END_CHUNK:
                break
            if chunk.startswith('{'):
                if i + 1 < len(chunks):
                    end_chunk = chunks[i+1]
                    if end_chunk is _END_CHUNK:
                        value_str = data
                        data = ''
                    else:
                        if end_chunk == 'OPTIONAL':
                            end_chunk = chunks[i+2]
                        try:
                            k = data.index(end_chunk)
                        except ValueError:
                            # not found! wrong pattern?
                            if chunks[i+1] == 'OPTIONAL':
                                # optional part missing
                                value_str = data
                                data = ''
                                without_optional = True
                            else:
                                # wrong pattern?
                                break
                        else:
                            value_str = data[:k]
                            data = data[k:]
                #
                p_value = value_str
                p_format, p_name = chunk[1:-1].split(':')
                if p_format == 'd':
                    try:
                        p_value = int(p_value)
                    except ValueError:
                        # wrong pattern?
                        break
                #
                params[p_name] = p_value
                i += 1
            elif chunk == 'OPTIONAL':
                if not data.rstrip():
                    data = ''
                    without_optional = True
                    break
                i += 1
            else:
                if data.startswith(chunk):
                    data = data[len(chunk):]
                    i += 1
                else:
                    break
        #
        if data == '':
            return (candidate, params, without_optional)
    return None
