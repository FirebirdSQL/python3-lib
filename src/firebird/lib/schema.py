# SPDX-FileCopyrightText: 2020-present The Firebird Projects <www.firebirdsql.org>
#
# SPDX-License-Identifier: MIT
#
# PROGRAM/MODULE: firebird-lib
# FILE:           firebird/lib/schema.py
# DESCRIPTION:    Module for work with Firebird database schema
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

"""firebird.lib.schema - Introspect and represent Firebird database schema.

This module provides classes for exploring the metadata of a Firebird database.
The primary entry point is the `Schema` class, which connects to a database
(usually via `Connection.schema`) and provides access to various schema
elements like tables, views, procedures, domains, etc., by querying the `RDB$`
system tables.

Metadata is typically loaded lazily when corresponding properties on the `Schema`
object (e.g., `schema.tables`, `schema.procedures`) are first accessed.
Schema elements are represented by subclasses of `SchemaItem` (like `Table`,
`Procedure`, `Domain`), which offer properties to access their details and
methods to generate DDL SQL (`get_sql_for`).

It also includes enums representing various Firebird types and flags found
in the system tables (e.g., `ObjectType`, `FieldType`, `TriggerType`).
"""

from __future__ import annotations

import datetime
import weakref
from enum import Enum, IntEnum, IntFlag, auto
from itertools import groupby
from typing import Any, ClassVar, Self

from firebird.base.collections import DataList
from firebird.driver import Connection, Cursor, Error, Isolation, Statement, TraAccessMode, tpb
from firebird.driver.types import UserInfo


class FieldType(IntEnum):
    """Firebird field type codes.
    """
    NONE = 0
    SHORT = 7
    LONG = 8
    QUAD = 9
    FLOAT = 10
    DATE = 12
    TIME = 13
    TEXT = 14
    INT64 = 16
    BOOLEAN = 23
    DEC16 = 24
    DEC34 = 25
    INT128 = 26
    DOUBLE = 27
    TIME_TZ = 28
    TIMESTAMP_TZ = 29
    TIME_TZ_EX = 30
    TIMESTAMP_TZ_EX = 31
    TIMESTAMP = 35
    VARYING = 37
    CSTRING = 40
    BLOB_ID = 45
    BLOB = 261

class FieldSubType(IntEnum):
    """Field sub-types.
    """
    # BLOB sub-types
    BINARY = 0
    TEXT = 1
    BLR = 2
    ACL = 3
    RANGES = 4
    SUMMARY = 5
    FORMAT = 6
    TRANSACTION_DESCRIPTION = 7
    EXTERNAL_FILE_DESCRIPTION = 8
    DEBUG_INFORMATION = 9
    # Integral sub-types
    NUMERIC = 1
    DECIMAL = 2

# --- Lists and disctionary maps ---

#: Mapping from FieldType codes to SQL type names (approximations).
COLUMN_TYPES = {None: 'UNKNOWN', FieldType.SHORT: 'SMALLINT',
                FieldType.LONG: 'INTEGER', FieldType.QUAD: 'QUAD',
                FieldType.FLOAT: 'FLOAT', FieldType.TEXT: 'CHAR',
                FieldType.DOUBLE: 'DOUBLE PRECISION',
                FieldType.VARYING: 'VARCHAR', FieldType.CSTRING: 'CSTRING',
                FieldType.BLOB_ID: 'BLOB_ID', FieldType.BLOB: 'BLOB',
                FieldType.TIME: 'TIME', FieldType.DATE: 'DATE',
                FieldType.TIMESTAMP: 'TIMESTAMP', FieldType.INT64: 'BIGINT',
                FieldType.BOOLEAN: 'BOOLEAN'}
#: Mapping for FieldSubType codes used with integral types.
INTEGRAL_SUBTYPES = ('UNKNOWN', 'NUMERIC', 'DECIMAL')

class IndexType(Enum):
    """Index ordering."""
    ASCENDING = 'ASCENDING'
    DESCENDING = 'DESCENDING'

class ObjectType(IntEnum):
    """Dependent type codes.

    .. versionchanged:: 1.4.0 - `PACKAGE` renamed to `PACKAGE_HEADER`, added values 20-37
    """
    TABLE = 0
    VIEW = 1
    TRIGGER = 2
    DOMAIN = 3
    CHECK = 4
    PROCEDURE = 5
    INDEX_EXPR = 6
    EXCEPTION = 7
    USER = 8
    COLUMN = 9
    INDEX = 10
    CHARACTER_SET = 11
    USER_GROUP = 12
    ROLE = 13
    GENERATOR = 14
    UDF = 15
    BLOB_FILTER = 16
    COLLATION = 17
    PACKAGE_HEADER = 18
    PACKAGE_BODY = 19
    PRIVILEGE = 20
    # Object types for DDL operations
    DATABASE = 21
    RELATIONS = 22
    VIEWS = 23
    PROCEDURES = 24
    FUNCTIONS = 25
    PACKAGES = 26
    GENERATORS = 27
    DOMAINS = 28
    EXCEPTIONS = 29
    ROLES = 30
    CHARSETS = 31
    COLLATIONS = 32
    FILTERS = 33
    # Codes that could be used in RDB$DEPENDENCIES or RDB$USER_PRIVILEGES
    JOBS = 34
    TABLESPACE = 35
    TABLESPACES = 36
    INDEX_CONDITION = 37

class FunctionType(IntEnum):
    """Function type codes."""
    VALUE = 0
    BOOLEAN = 1

class Mechanism(IntEnum):
    """Mechanism codes."""
    BY_VALUE = 0
    BY_REFERENCE = 1
    BY_VMS_DESCRIPTOR = 2
    BY_ISC_DESCRIPTOR = 3
    BY_SCALAR_ARRAY_DESCRIPTOR = 4
    BY_REFERENCE_WITH_NULL = 5

class TransactionState(IntEnum):
    """Transaction state codes."""
    LIMBO = 1
    COMMITTED = 2
    ROLLED_BACK = 3

class SystemFlag(IntEnum):
    """System flag codes."""
    USER = 0
    SYSTEM = 1
    QLI = 2
    CHECK_CONSTRAINT = 3
    REFERENTIAL_CONSTRAINT = 4
    VIEW_CHECK = 5
    IDENTITY_GENERATOR = 6

class ShadowFlag(IntFlag):
    """Shadow file flags."""
    INACTIVE = 2
    MANUAL = 4
    CONDITIONAL = 16

class RelationType(IntEnum):
    """Relation type codes."""
    PERSISTENT = 0
    VIEW = 1
    EXTERNAL = 2
    VIRTUAL = 3
    GLOBAL_TEMPORARY_PRESERVE = 4
    GLOBAL_TEMPORARY_DELETE = 5

class ProcedureType(IntEnum):
    """Procedure type codes."""
    LEGACY = 0
    SELECTABLE = 1
    EXECUTABLE = 2

class ParameterMechanism(IntEnum):
    """Parameter mechanism type codes."""
    NORMAL = 0
    TYPE_OF = 1

class TypeFrom(IntEnum):
    """Source of parameter datatype codes."""
    DATATYPE = 0
    DOMAIN = 1
    TYPE_OF_DOMAIN = 2
    TYPE_OF_COLUMN = 3

class ParameterType(IntEnum):
    """Parameter type codes."""
    INPUT = 0
    OUTPUT = 1

class IdentityType(IntEnum):
    """Identity type codes."""
    ALWAYS = 0
    BY_DEFAULT = 1

class GrantOption(IntEnum):
    """Grant option codes."""
    NONE = 0
    GRANT_OPTION = 1
    ADMIN_OPTION = 2

class PageType(IntEnum):
    """Page type codes."""
    HEADER = 1
    PAGE_INVENTORY = 2
    TRANSACTION_INVENTORY = 3
    POINTER = 4
    DATA = 5
    INDEX_ROOT = 6
    INDEX_BUCKET = 7
    BLOB = 8
    GENERATOR = 9
    SCN_INVENTORY = 10

class MapTo(IntEnum):
    """Map to type codes."""
    USER = 0
    ROLE = 1

class TriggerType(IntEnum):
    """Trigger type codes."""
    DML = 0
    DB = 8192
    DDL = 16384

class DDLTrigger(IntEnum):
    """DDL trigger type codes."""
    ANY = 4611686018427375615
    CREATE_TABLE = 1
    ALTER_TABLE = 2
    DROP_TABLE = 3
    CREATE_PROCEDURE = 4
    ALTER_PROCEDURE = 5
    DROP_PROCEDURE = 6
    CREATE_FUNCTION = 7
    ALTER_FUNCTION = 8
    DROP_FUNCTION = 9
    CREATE_TRIGGER = 10
    ALTER_TRIGGER = 11
    DROP_TRIGGER = 12
    # gap for TRIGGER_TYPE_MASK - 3 bits
    CREATE_EXCEPTION = 16
    ALTER_EXCEPTION = 17
    DROP_EXCEPTION = 18
    CREATE_VIEW = 19
    ALTER_VIEW = 20
    DROP_VIEW = 21
    CREATE_DOMAIN = 22
    ALTER_DOMAIN = 23
    DROP_DOMAIN = 24
    CREATE_ROLE = 25
    ALTER_ROLE = 26
    DROP_ROLE = 27
    CREATE_INDEX = 28
    ALTER_INDEX = 29
    DROP_INDEX = 30
    CREATE_SEQUENCE = 31
    ALTER_SEQUENCE = 32
    DROP_SEQUENCE = 33
    CREATE_USER = 34
    ALTER_USER = 35
    DROP_USER = 36
    CREATE_COLLATION = 37
    DROP_COLLATION = 38
    ALTER_CHARACTER_SET = 39
    CREATE_PACKAGE = 40
    ALTER_PACKAGE = 41
    DROP_PACKAGE = 42
    CREATE_PACKAGE_BODY = 43
    DROP_PACKAGE_BODY = 44
    CREATE_MAPPING = 45
    ALTER_MAPPING = 46
    DROP_MAPPING = 47

class DBTrigger(IntEnum):
    """Database trigger type codes."""
    CONNECT = 0
    DISCONNECT = 1
    TRANSACTION_START = 2
    TRANSACTION_COMMIT = 3
    TRANSACTION_ROLLBACK = 4

class DMLTrigger(IntFlag):
    """DML trigger type codes."""
    INSERT = auto()
    UPDATE = auto()
    DELETE = auto()

class TriggerTime(IntEnum):
    """Trigger action time codes."""
    BEFORE = 0
    AFTER = 1

class ConstraintType(Enum):
    """Contraint type codes."""
    CHECK = 'CHECK'
    NOT_NULL = 'NOT NULL'
    FOREIGN_KEY = 'FOREIGN KEY'
    PRIMARY_KEY = 'PRIMARY KEY'
    UNIQUE = 'UNIQUE'

class Section(Enum):
    """DDL script sections. Used by `.Schema.get_metadata_ddl()`."""
    COLLATIONS = auto()
    CHARACTER_SETS = auto()
    UDFS = auto()
    GENERATORS = auto()
    EXCEPTIONS = auto()
    DOMAINS = auto()
    PACKAGE_DEFS = auto()
    FUNCTION_DEFS = auto()
    PROCEDURE_DEFS = auto()
    TABLES = auto()
    PRIMARY_KEYS = auto()
    UNIQUE_CONSTRAINTS = auto()
    CHECK_CONSTRAINTS = auto()
    FOREIGN_CONSTRAINTS = auto()
    INDICES = auto()
    VIEWS = auto()
    PACKAGE_BODIES = auto()
    PROCEDURE_BODIES = auto()
    FUNCTION_BODIES = auto()
    TRIGGERS = auto()
    ROLES = auto()
    GRANTS = auto()
    COMMENTS = auto()
    SHADOWS = auto()
    SET_GENERATORS = auto()
    INDEX_DEACTIVATIONS = auto()
    INDEX_ACTIVATIONS = auto()
    TRIGGER_DEACTIVATIONS = auto()
    TRIGGER_ACTIVATIONS = auto()

class Category(Enum):
    """Schema information collection categories."""
    TABLES = auto()
    VIEWS = auto()
    DOMAINS = auto()
    INDICES = auto()
    DEPENDENCIES = auto()
    GENERATORS = auto()
    SEQUENCES = GENERATORS
    TRIGGERS = auto()
    PROCEDURES = auto()
    CONSTRAINTS = auto()
    COLLATIONS = auto()
    CHARACTER_SETS = auto()
    EXCEPTIONS = auto()
    ROLES = auto()
    FUNCTIONS = auto()
    FILES = auto()
    SHADOWS = auto()
    PRIVILEGES = auto()
    USERS = auto()
    PACKAGES = auto()
    BACKUP_HISTORY = auto()
    FILTERS = auto()

class Privacy(IntEnum):
    """Privacy flag codes."""
    PUBLIC = 0
    PRIVATE = 1

class Legacy(IntEnum):
    """Legacy flag codes."""
    NEW_STYLE = 0
    LEGACY_STYLE = 1

class PrivilegeCode(Enum):
    """Priviledge codes."""
    SELECT = 'S'
    INSERT = 'I'
    UPDATE = 'U'
    DELETE = 'D'
    REFERENCES = 'R'
    EXECUTE = 'X'
    USAGE = 'G'
    CREATE = 'C'
    ALTER = 'L'
    DROP = 'O'
    MEMBERSHIP = 'M'

class CollationFlag(IntFlag):
    """Collation attribute flags."""
    NONE = 0
    PAD_SPACE = 1
    CASE_INSENSITIVE = 2
    ACCENT_INSENSITIVE = 4

#: Default order of sections for DDL script generation via `get_metadata_ddl()`.
SCRIPT_DEFAULT_ORDER = [Section.COLLATIONS, Section.CHARACTER_SETS,
                        Section.UDFS, Section.GENERATORS,
                        Section.EXCEPTIONS, Section.DOMAINS,
                        Section.PACKAGE_DEFS,
                        Section.FUNCTION_DEFS, Section.PROCEDURE_DEFS,
                        Section.TABLES, Section.PRIMARY_KEYS,
                        Section.UNIQUE_CONSTRAINTS,
                        Section.CHECK_CONSTRAINTS,
                        Section.FOREIGN_CONSTRAINTS, Section.INDICES,
                        Section.VIEWS, Section.PACKAGE_BODIES,
                        Section.PROCEDURE_BODIES,
                        Section.FUNCTION_BODIES, Section.TRIGGERS,
                        Section.GRANTS, Section.ROLES, Section.COMMENTS,
                        Section.SHADOWS, Section.SET_GENERATORS]


def get_grants(privileges: list[Privilege], grantors: list[str] | None=None) -> list[str]:
    """Get list of minimal set of SQL GRANT statamenets necessary to grant
    specified privileges.

    Arguments:
        privileges: list of :class:`Privilege` instances.

    Keyword Args:
        grantors: list of standard grantor names. Generates GRANTED BY
            clause for privileges granted by user that's not in list.
    """
    tp = {PrivilegeCode.SELECT, PrivilegeCode.INSERT, PrivilegeCode.UPDATE,
          PrivilegeCode.DELETE, PrivilegeCode.REFERENCES}

    def skey(item):
        return (item.user_name, item.user_type, item.grantor_name,
                item.subject_name, item.subject_type, item.has_grant(),
                item.privilege in tp, item.privilege.value, str(item.field_name),)
    def gkey(item):
        return (item.user_name, item.user_type, item.grantor_name,
                item.subject_name, item.subject_type, item.has_grant(),
                item.privilege in tp,)
    def gkey2(item):
        return item.privilege.name

    grants = []
    p = list(privileges)
    p.sort(key=skey)
    for _, g in groupby(p, gkey):
        g = list(g) # noqa: PLW2901
        item = g[0]
        if item.has_grant():
            admin_option = f" WITH {'ADMIN' if item.privilege is PrivilegeCode.MEMBERSHIP else 'GRANT'} OPTION"
        else:
            admin_option = ''
        uname = item.user_name
        user = item.user
        if isinstance(user, Procedure):
            utype = 'PROCEDURE '
        elif isinstance(user, Trigger):
            utype = 'TRIGGER '
        elif isinstance(user, View):
            utype = 'VIEW '
        else:
            utype = ''
        sname = item.subject_name
        if (grantors is not None) and (item.grantor_name not in grantors):
            granted_by = f' GRANTED BY {item.grantor_name}'
        else:
            granted_by = ''
        priv_list = []
        for _, items in groupby(g, gkey2):
            items = list(items) # noqa: PLW2901
            item = items[0]
            if item.privilege in tp:
                privilege = item.privilege.name
                if len(items) > 1:
                    privilege += f"({','.join(i.field_name for i in items if i.field_name)})"
                elif item.field_name is not None:
                    privilege += f'({item.field_name})'
                priv_list.append(privilege)
            elif item.privilege is PrivilegeCode.EXECUTE: # procedure
                privilege = 'EXECUTE ON PROCEDURE '
            elif item.privilege is PrivilegeCode.MEMBERSHIP:
                privilege = ''
        if priv_list:
            privilege = ', '.join(priv_list)
            privilege += ' ON '
        grants.append(f'GRANT {privilege}{sname} TO {utype}{uname}{admin_option}{granted_by}')
    return grants

def escape_single_quotes(text: str) -> str:
    """Returns `text` with any single quotes escaped (doubled).

    Arguments:
        text: Text to be escaped.
    """
    return text.replace("'", "''")

class Visitable:
    """Base class for Visitor Pattern support.
    """
    def accept(self, visitor: Visitor) -> None:
        """Accepts a Visitor object as part of the Visitor design pattern.

        Calls the `visit()` method on the provided `visitor` object, passing
        this `SchemaItem` instance (`self`) as the argument.

        Arguments:
            visitor: An object implementing the `.Visitor` interface.
        """
        visitor.visit(self)

class Visitor:
    """Base class for Visitor Pattern visitors.

    Descendants may implement methods to handle individual object types that follow naming
    pattern `visit_[class_name]`. Calls `.default_action()` if appropriate special method is
    not defined.

    .. important::

       This implementation uses Python Method Resolution Order (__mro__) to find special
       handling method, so special method for given class is used also for its decendants.

    Example::

       class Node(object): pass
       class A(Node): pass
       class B(Node): pass
       class C(A,B): pass

       class MyVisitor(object):
           def default_action(self, obj):
               print('default_action ', obj.__class__.__name__)

           def visit_b(self, obj):
               print('visit_b ', obj.__class__.__name__)


       a = A()
       b = B()
       c = C()
       visitor = MyVisitor()
       visitor.visit(a)
       visitor.visit(b)
       visitor.visit(c)

    Will create output::

       default_action A
       visit_b B
       visit_b C
    """
    def visit(self, obj: Visitable) -> Any:
        """Dispatch to method that handles `obj`.

        Arguments:
            obj: Object to be handled by visitor.

        First traverses the `obj.__mro__` to try find method with name following
        `visit_<lower_class_name>` pattern and calls it with `obj`. Otherwise it calls
        `.default_action()`.
        """
        meth = None
        for cls in obj.__class__.__mro__:
            if meth := getattr(self, 'visit_'+cls.__name__.lower(), None):
                break
        if not meth:
            meth = self.default_action
        return meth(obj)
    def default_action(self, obj: Visitable) -> None:
        """Default handler for visited objects.

        Arguments:
            obj: Object to be handled.

        Note:
            Default implementation does nothing!
        """

class Schema(Visitable):
    """Provides access to and represents the metadata of a Firebird database.

    This class serves as the main entry point for exploring the database schema.
    It connects to a database (typically obtained via `Connection.schema`) and
    provides access to various schema elements like tables, views, procedures,
    domains, etc., by querying the `RDB$` system tables using an internal,
    read-committed transaction.

    Key Behaviors:

    *   **Lazy Loading:** Metadata collections (like tables, procedures) are
        fetched from the database only when their corresponding property
        (e.g., `schema.tables`, `schema.procedures`) is first accessed after
        binding or clearing the cache.
    *   **Object Representation:** Schema elements are represented by instances
        of `.SchemaItem` subclasses (e.g., `.Table`, `.Procedure`, `.Domain`),
        offering properties to access details and methods like `get_sql_for()`
        to generate DDL.
    *   **Caching:** Fetched metadata is cached internally. Use `clear()` or
        `reload()` to refresh the cache.
    *   **Binding & Lifecycle:** An instance must be bound to a live
        `~firebird.driver.Connection` using the `bind()` method (this is
        done automatically when accessed via `Connection.schema`). It should
        be closed using `close()` (or via a `with` statement) to release
        resources when no longer needed.

    Configuration options control certain behaviors like SQL identifier quoting.
    Internal maps are populated during binding to translate system codes (e.g.,
    for object types, field types) into meaningful enums or names.
    """
    #: Configuration option: If True, always quote database object names in
    #: generated SQL, otherwise quote only when necessary (e.g., reserved words,
    #: non-standard characters). Defaults to False.
    opt_always_quote: bool = False
    #: Configuration option: SQL keyword to use for generators/sequences ('SEQUENCE'
    #: or 'GENERATOR'). Defaults to 'SEQUENCE'.
    opt_generator_keyword: str = 'SEQUENCE'
    #: Mapping from parameter source codes (RDB$PARAMETER_MECHANISM) to descriptive strings.
    param_type_from: ClassVar[dict[int, str]] = {0: 'DATATYPE', 1: 'DOMAIN',
                                                 2: 'TYPE OF DOMAIN', 3: 'TYPE OF COLUMN'}
    #: Mapping from object type codes (RDB$OBJECT_TYPE) to descriptive strings.
    object_types: ClassVar[dict[int, str]] = {}
    #: Reverse mapping from object type names to codes.
    object_type_codes:  ClassVar[dict[str, int]] = {}
    #: Mapping from character set IDs (RDB$CHARACTER_SET_ID) to names.
    character_set_names: ClassVar[dict[int, str]] = {}
    #: Mapping from field type codes (RDB$FIELD_TYPE) to SQL type names.
    field_types: ClassVar[dict[int, str]] = {}
    #: Mapping from field sub-type codes (RDB$FIELD_SUB_TYPE) to names.
    field_subtypes: ClassVar[dict[int, str]] = {}
    #: Mapping from function type codes (RDB$FUNCTION_TYPE) to names.
    function_types: ClassVar[dict[int, str]] = {}
    #: Mapping from mechanism codes (RDB$MECHANISM) to names.
    mechanism_types: ClassVar[dict[str, str]] = {}
    #: Mapping from parameter mechanism codes (RDB$PARAMETER_MECHANISM) to names.
    parameter_mechanism_types: ClassVar[dict[int, str]] = {}
    #: Mapping from procedure type codes (RDB$PROCEDURE_TYPE) to names.
    procedure_types: ClassVar[dict[int, str]] = {}
    #: Mapping from relation type codes (RDB$RELATION_TYPE) to names.
    relation_types: ClassVar[dict[int, str]] = {}
    #: Mapping from system flag codes (RDB$SYSTEM_FLAG) to names.
    system_flag_types: ClassVar[dict[int, str]] = {}
    #: Mapping from transaction state codes (RDB$TRANSACTION_STATE) to names.
    transaction_state_types: ClassVar[dict[int, str]] = {}
    #: Mapping from trigger type codes (RDB$TRIGGER_TYPE) to names.
    trigger_types: ClassVar[dict[int, str]] = {}
    #: Mapping from parameter type codes (RDB$PARAMETER_TYPE) to names.
    parameter_types: ClassVar[dict[int, str]] = {}
    #: Mapping from index activity codes (RDB$INDEX_INACTIVE) to names.
    index_activity_flags: ClassVar[dict[int, str]] = {}
    #: Mapping from index uniqueness codes (RDB$UNIQUE_FLAG) to names.
    index_unique_flags: ClassVar[dict[int, str]] = {}
    #: Mapping from trigger activity codes (RDB$TRIGGER_INACTIVE) to names.
    trigger_activity_flags: ClassVar[dict[int, str]] = {}
    #: Mapping from grant option codes (RDB$GRANT_OPTION) to names.
    grant_options: ClassVar[dict[int, str]] = {}
    #: Mapping from page type codes (RDB$PAGE_TYPE) to names.
    page_types: ClassVar[dict[int, str]] = {}
    #: Mapping from privacy flag codes (RDB$PRIVATE_FLAG) to names.
    privacy_flags: ClassVar[dict[int, str]] = {}
    #: Mapping from legacy flag codes (RDB$LEGACY_FLAG) to names.
    legacy_flags: ClassVar[dict[int, str]] = {}
    #: Mapping from determinism flag codes (RDB$DETERMINISTIC_FLAG) to names.
    deterministic_flags: ClassVar[dict[int, str]] = {}
    #: Mapping from identity type codes (RDB$IDENTITY_TYPE) to names.
    identity_type: ClassVar[dict[int, str]] = {}
    def __init__(self):
        #: The underlying driver Connection, or None if closed/unbound.
        self._con: Connection | None = None
        #: Internal cursor using a separate read-committed transaction for RDB$ queries.
        self._ic: Cursor | None = None
        #: Internal flag to prevent closing/rebinding if owned by Connection.
        self.__internal: bool = False
        #: List of reserved keywords for the connected database ODS version.
        self._reserved_: list[str] = []
        #: ODS version of the connected database (e.g., 12.0, 13.0).
        self.ods: float | None = None
        #: Raw attributes from RDB$DATABASE fetched during bind().
        self.__attrs: dict[str, Any] | None = None
        #: Default character set name for the database.
        self._default_charset_name: str | None = None
        #: Owner name fetched from RDB$RELATIONS for RDB$DATABASE.
        self.__owner: str | None = None
        # --- Cached Metadata Collections (Lazy Loaded) ---
        self.__tables: tuple[DataList, DataList] | None = None
        self.__views: tuple[DataList, DataList] | None = None
        self.__domains: tuple[DataList, DataList] | None = None
        self.__indices: tuple[DataList, DataList] | None = None
        self.__constraint_indices: dict[str, str] | None = None
        self.__dependencies: DataList | None = None
        self.__generators: tuple[DataList, DataList] | None = None
        self.__triggers: tuple[DataList, DataList] | None = None
        self.__procedures: tuple[DataList, DataList] | None = None
        self.__constraints: DataList | None = None
        self.__collations: DataList | None = None
        self.__character_sets: DataList | None = None
        self.__exceptions: DataList | None = None
        self.__roles: DataList | None = None
        self.__functions: tuple[DataList, DataList] | None = None
        self.__files: DataList | None = None
        self.__shadows: DataList | None = None
        self.__privileges: DataList | None = None
        self.__users: DataList | None = None
        self.__packages: DataList | None = None
        self.__backup_history: DataList | None = None
        self.__filters: DataList | None = None
    def __del__(self):
        if not self.closed:
            self._close()
    def __enter__(self) -> Self:
        return self
    def __exit__(self, exc_type, exc_value, traceback) -> None:
        self.close()
    def __fail_if_closed(self):
        if self.closed:
            raise Error("Schema is not binded to connection.")
    def _close(self) -> None:
        if self._ic is not None:
            self._ic.close()
        self._con = None
        self._ic = None
    def _set_internal(self, value: bool) -> None: # noqa: FBT001
        self.__internal = value
    def __clear(self, data: Category | list[Category] | tuple | None=None) -> None:
        if data:
            if not isinstance(data, list | tuple):
                data = (data, )
        else:
            data = list(Category)
        for item in data:
            if item is Category.TABLES:
                self.__tables: tuple[DataList, DataList] = None
            elif item is Category.VIEWS:
                self.__views: tuple[DataList, DataList] = None
            elif item is Category.DOMAINS:
                self.__domains: tuple[DataList, DataList] = None
            elif item is Category.INDICES:
                self.__indices: tuple[DataList, DataList] = None
                self.__constraint_indices = None
            elif item is Category.DEPENDENCIES:
                self.__dependencies: DataList = None
            elif item is Category.GENERATORS:
                self.__generators: tuple[DataList, DataList] = None
            elif item is Category.TRIGGERS:
                self.__triggers: tuple[DataList, DataList] = None
            elif item is Category.PROCEDURES:
                self.__procedures: tuple[DataList, DataList] = None
            elif item is Category.CONSTRAINTS:
                self.__constraints: DataList = None
            elif item is Category.COLLATIONS:
                self.__collations: DataList = None
            elif item is Category.CHARACTER_SETS:
                self.__character_sets: DataList = None
            elif item is Category.EXCEPTIONS:
                self.__exceptions: DataList = None
            elif item is Category.ROLES:
                self.__roles: DataList = None
            elif item is Category.FUNCTIONS:
                self.__functions: tuple[DataList, DataList] = None
            elif item is Category.FILES:
                self.__files: DataList = None
            elif item is Category.SHADOWS:
                self.__shadows: DataList = None
            elif item is Category.PRIVILEGES:
                self.__privileges: DataList = None
            elif item is Category.USERS:
                self.__users: DataList = None
            elif item is Category.PACKAGES:
                self.__packages: DataList = None
            elif item is Category.BACKUP_HISTORY:
                self.__backup_history: DataList = None
            elif item is Category.FILTERS:
                self.__filters: DataList = None
    def _select_row(self, cmd: Statement | str, params: list | None=None) -> dict[str, Any]:
        self._ic.execute(cmd, params)
        row = self._ic.fetchone()
        return {self._ic.description[i][0]: row[i] for i in range(len(row))}
    def _select(self, cmd: str, params: list | None=None) -> dict[str, Any]:
        self._ic.execute(cmd, params)
        desc = self._ic.description
        return ({desc[i][0]: row[i] for i in range(len(row))} for row in self._ic)
    def _get_field_dimensions(self, field) -> list[tuple[int, int]]:
        return [(r[0], r[1]) for r in
                self._ic.execute(f"""select RDB$LOWER_BOUND, RDB$UPPER_BOUND
                from RDB$FIELD_DIMENSIONS where RDB$FIELD_NAME = '{field.name}' order by RDB$DIMENSION""")]
    def _get_all_domains(self) -> tuple[DataList[Domain], DataList[Domain], DataList[Domain]]:
        if self.__domains is None:
            self.__fail_if_closed()
            cols = ['RDB$FIELD_NAME', 'RDB$VALIDATION_SOURCE', 'RDB$COMPUTED_SOURCE',
                    'RDB$DEFAULT_SOURCE', 'RDB$FIELD_LENGTH', 'RDB$FIELD_SCALE',
                    'RDB$FIELD_TYPE', 'RDB$FIELD_SUB_TYPE', 'RDB$DESCRIPTION',
                    'RDB$SYSTEM_FLAG', 'RDB$SEGMENT_LENGTH', 'RDB$EXTERNAL_LENGTH',
                    'RDB$EXTERNAL_SCALE', 'RDB$EXTERNAL_TYPE', 'RDB$DIMENSIONS',
                    'RDB$NULL_FLAG', 'RDB$CHARACTER_LENGTH', 'RDB$COLLATION_ID',
                    'RDB$CHARACTER_SET_ID', 'RDB$FIELD_PRECISION', 'RDB$SECURITY_CLASS',
                    'RDB$OWNER_NAME']
            domains = DataList((Domain(self, row) for row
                                in self._select(f"select {','.join(cols)} from RDB$FIELDS")),
                               Domain, 'item.name', frozen=True)
            sys_domains, user_domains = domains.split(lambda i: i.is_sys_object(), frozen=True)
            self.__domains = (user_domains, sys_domains, domains)
        return self.__domains
    def _get_all_tables(self) -> tuple[DataList[Table], DataList[Table], DataList[Table]]:
        if self.__tables is None:
            self.__fail_if_closed()
            tables = DataList((Table(self, row) for row
                               in self._select('select * from rdb$relations where rdb$view_blr is null')),
                              Table, 'item.name', frozen=True)
            sys_tables, user_tables = tables.split(lambda i: i.is_sys_object(), frozen=True)
            self.__tables = (user_tables, sys_tables, tables)
        return self.__tables
    def _get_all_views(self) -> tuple[DataList[View], DataList[View], DataList[View]]:
        if self.__views is None:
            self.__fail_if_closed()
            views = DataList((View(self, row) for row
                              in self._select('select * from rdb$relations where rdb$view_blr is not null')),
                             View, 'item.name', frozen=True)
            sys_views, user_views = views.split(lambda i: i.is_sys_object(), frozen=True)
            self.__views = (user_views, sys_views, views)
        return self.__views
    def _get_constraint_indices(self) -> dict[str, str]:
        if self.__constraint_indices is None:
            self.__fail_if_closed()
            self._ic.execute("""select RDB$INDEX_NAME, RDB$CONSTRAINT_NAME
            from RDB$RELATION_CONSTRAINTS where RDB$INDEX_NAME is not null""")
            self.__constraint_indices = {key.strip(): value.strip() for key, value
                                         in self._ic}
        return self.__constraint_indices
    def _get_all_indices(self) -> tuple[DataList[Index], DataList[Index], DataList[Index]]:
        if self.__indices is None:
            self.__fail_if_closed()
            # Dummy call to _get_constraint_indices() is necessary as
            # Index.is_sys_object() that is called in Index.__init__() will
            # drop result from internal cursor and we'll not load all indices.
            self._get_constraint_indices()
            ext = '' if self.ods <= 13.0 else  ', RDB$CONDITION_SOURCE'
            cmd = f"""select RDB$INDEX_NAME, RDB$RELATION_NAME, RDB$INDEX_ID,
            RDB$UNIQUE_FLAG, RDB$DESCRIPTION, RDB$SEGMENT_COUNT, RDB$INDEX_INACTIVE,
            RDB$INDEX_TYPE, RDB$FOREIGN_KEY, RDB$SYSTEM_FLAG, RDB$EXPRESSION_SOURCE,
            RDB$STATISTICS{ext} from RDB$INDICES"""
            indices = DataList((Index(self, row) for row in self._select(cmd)),
                               Index, 'item.name', frozen=True)
            sys_indices, user_indices = indices.split(lambda i: i.is_sys_object(), frozen=True)
            self.__indices = (user_indices, sys_indices, indices)
        return self.__indices
    def _get_all_generators(self) -> tuple[DataList[Sequence], DataList[Sequence], DataList[Sequence]]:
        if self.__generators is None:
            self.__fail_if_closed()
            cols = ['RDB$GENERATOR_NAME', 'RDB$GENERATOR_ID', 'RDB$DESCRIPTION',
                    'RDB$SYSTEM_FLAG', 'RDB$SECURITY_CLASS', 'RDB$OWNER_NAME',
                    'RDB$INITIAL_VALUE', 'RDB$GENERATOR_INCREMENT']
            generators = DataList((Sequence(self, row) for row
                                   in self._select(f"select {','.join(cols)} from rdb$generators")),
                                  Sequence, 'item.name', frozen=True)
            sys_generators, user_generators = generators.split(lambda i: i.is_sys_object(),
                                                               frozen=True)
            self.__generators = (user_generators, sys_generators, generators)
        return self.__generators
    def _get_all_triggers(self) -> tuple[DataList[Trigger], DataList[Trigger], DataList[Trigger]]:
        if self.__triggers is None:
            self.__fail_if_closed()
            cols = ['RDB$TRIGGER_NAME', 'RDB$RELATION_NAME', 'RDB$TRIGGER_SEQUENCE',
                    'RDB$TRIGGER_TYPE', 'RDB$TRIGGER_SOURCE', 'RDB$DESCRIPTION',
                    'RDB$TRIGGER_INACTIVE', 'RDB$SYSTEM_FLAG', 'RDB$FLAGS',
                    'RDB$VALID_BLR', 'RDB$ENGINE_NAME', 'RDB$ENTRYPOINT']
            triggers = DataList((Trigger(self, row) for row
                                 in self._select(f"select {','.join(cols)} from RDB$TRIGGERS")),
                                Trigger, 'item.name', frozen=True)
            sys_triggers, user_triggers = triggers.split(lambda i: i.is_sys_object(), frozen=True)
            self.__triggers = (user_triggers, sys_triggers, triggers)
        return self.__triggers
    def _get_all_procedures(self) -> tuple[DataList[Procedure], DataList[Procedure], DataList[Procedure]]:
        if self.__procedures is None:
            self.__fail_if_closed()
            cols = ['RDB$PROCEDURE_NAME', 'RDB$PROCEDURE_ID', 'RDB$PROCEDURE_INPUTS',
                    'RDB$PROCEDURE_OUTPUTS', 'RDB$DESCRIPTION', 'RDB$PROCEDURE_SOURCE',
                    'RDB$SECURITY_CLASS', 'RDB$OWNER_NAME', 'RDB$SYSTEM_FLAG',
                    'RDB$PROCEDURE_TYPE', 'RDB$VALID_BLR', 'RDB$ENGINE_NAME',
                    'RDB$ENTRYPOINT', 'RDB$PACKAGE_NAME', 'RDB$PRIVATE_FLAG']
            procedures = DataList((Procedure(self, row) for row
                                   in self._select(f"select {','.join(cols)} from rdb$procedures")),
                                  Procedure, 'item.name', frozen=True)
            sys_procedures, user_procedures = procedures.split(lambda i: i.is_sys_object(),
                                                               frozen=True)
            self.__procedures = (user_procedures, sys_procedures, procedures)
        return self.__procedures
    def _get_all_functions(self) -> tuple[DataList[Function], DataList[Function], DataList[Function]]:
        if self.__functions is None:
            self.__fail_if_closed()
            cols = ['RDB$FUNCTION_NAME', 'RDB$FUNCTION_TYPE', 'RDB$DESCRIPTION',
                    'RDB$MODULE_NAME', 'RDB$ENTRYPOINT', 'RDB$RETURN_ARGUMENT',
                    'RDB$SYSTEM_FLAG', 'RDB$ENGINE_NAME', 'RDB$PACKAGE_NAME',
                    'RDB$PRIVATE_FLAG', 'RDB$FUNCTION_SOURCE', 'RDB$FUNCTION_ID',
                    'RDB$VALID_BLR', 'RDB$SECURITY_CLASS', 'RDB$OWNER_NAME',
                    'RDB$LEGACY_FLAG', 'RDB$DETERMINISTIC_FLAG']
            functions = DataList((Function(self, row) for row
                                  in self._select(f"select {','.join(cols)} from rdb$functions")),
                                 Function, 'item.name', frozen=True)
            sys_functions, user_functions = functions.split(lambda i: i.is_sys_object(),
                                                            frozen=True)
            self.__functions = (user_functions, sys_functions, functions)
        return self.__functions
    def _get_users(self) -> DataList[UserInfo]:
        if self.__users is None:
            self.__fail_if_closed()
            self._ic.execute('select distinct(RDB$USER) FROM RDB$USER_PRIVILEGES')
            self.__users = DataList((UserInfo(user_name=row[0].strip()) for row in self._ic),
                                    UserInfo, 'item.user_name')
        return self.__users
    def bind(self, connection: Connection) -> Self:
        """Binds this Schema instance to a live database connection.

        This method establishes the connection, creates an internal cursor for
        querying system tables, fetches basic database attributes (ODS version,
        owner, default charset), determines reserved keywords, and populates
        internal enum/code mappings from `RDB$TYPES`.

        .. note::
            This method is primarily for internal use. Users typically access
            a bound Schema instance via `Connection.schema`. Calling `bind()` on
            an already bound or embedded schema may raise an `Error`.

        Arguments:
            connection: The `~firebird.driver.Connection` instance to bind to.

        Returns:
            The bound `Schema` instance (`self`).

        Raises:
            Error: If called on an embedded schema instance.
        """
        if self.__internal:
            raise Error("Call to 'bind' not allowed for embedded Schema.")
        self._con = connection
        self._ic = self._con.transaction_manager(tpb(Isolation.READ_COMMITTED_RECORD_VERSION,
                                                     access_mode=TraAccessMode.READ)).cursor()
        self._ic._logging_id_ = 'schema.internal_cursor'
        self.__clear()
        self.ods = self._con.info.ods
        if self.ods == 12.0: # Firebird 3
            self._reserved_ = ['ABS', 'ACOS', 'ACOSH', 'ACTIVE', 'ADD', 'ADMIN', 'AFTER',
                               'ALL', 'ALTER', 'AND', 'ANY', 'AS', 'ASC', 'ASCENDING',
                               'ASCII_CHAR', 'ASCII_VAL', 'ASIN', 'ASINH', 'AT', 'ATAN',
                               'ATAN2', 'ATANH', 'AUTO', 'AUTONOMOUS', 'AVG', 'BEFORE',
                               'BEGIN', 'BETWEEN', 'BIGINT', 'BIN_AND', 'BIN_NOT', 'BIN_OR',
                               'BIN_SHL', 'BIN_SHR', 'BIN_XOR', 'BIT_LENGTH', 'BLOB',
                               'BOOLEAN', 'BOTH', 'BY', 'CASE', 'CAST', 'CEIL', 'CEILING',
                               'CHAR', 'CHAR_LENGTH', 'CHAR_TO_UUID', 'CHARACTER',
                               'CHARACTER_LENGTH', 'CHECK', 'CLOSE', 'COLLATE', 'COLUMN',
                               'COMMIT', 'COMMITTED', 'COMPUTED', 'CONDITIONAL', 'CONNECT',
                               'CONSTRAINT', 'CONTAINING', 'CORR', 'COS', 'COSH', 'COT',
                               'COUNT', 'COVAR_POP', 'COVAR_SAMP', 'CREATE', 'CROSS',
                               'CSTRING', 'CURRENT', 'CURRENT_CONNECTION', 'CURRENT_DATE',
                               'CURRENT_ROLE', 'CURRENT_TIME', 'CURRENT_TIMESTAMP',
                               'CURRENT_TRANSACTION', 'CURRENT_USER', 'CURSOR', 'DATABASE',
                               'DATE', 'DATEADD', 'DATEDIFF', 'DAY', 'DDL', 'DEC',
                               'DECIMAL', 'DECLARE', 'DECODE', 'DEFAULT', 'DELETE',
                               'DENSE_RANK', 'DESC', 'DESCENDING', 'DETERMINISTIC',
                               'DISCONNECT', 'DISTINCT', 'DO', 'DOMAIN', 'DOUBLE', 'DROP',
                               'ELSE', 'END', 'ENTRY_POINT', 'ESCAPE', 'EXCEPTION',
                               'EXECUTE', 'EXISTS', 'EXIT', 'EXP', 'EXTERNAL', 'EXTRACT',
                               'FALSE', 'FETCH', 'FILE', 'FILTER', 'FIRST_VALUE',
                               'FIRSTNAME', 'FLOAT', 'FLOOR', 'FOR', 'FOREIGN', 'FROM',
                               'FULL', 'FUNCTION', 'GDSCODE', 'GENERATOR', 'GEN_ID',
                               'GEN_UUID', 'GLOBAL', 'GRANT', 'GRANTED', 'GROUP', 'HASH',
                               'HAVING', 'HOUR', 'IDENTITY', 'IF', 'IN', 'INACTIVE',
                               'INCREMENT', 'INDEX', 'INNER', 'INPUT_TYPE', 'INSENSITIVE',
                               'INSERT', 'INT', 'INTEGER', 'INTO', 'IS', 'ISOLATION',
                               'JOIN', 'KEY', 'LAG', 'LAST_VALUE', 'LASTNAME', 'LEAD',
                               'LEADING', 'LEFT', 'LENGTH', 'LEVEL', 'LIKE', 'list', 'LN',
                               'LOG', 'LOG10', 'LONG', 'LOWER', 'LPAD', 'MANUAL',
                               'MAPPING', 'MATCHED', 'MATCHING', 'MAX', 'MAXVALUE',
                               'MERGE', 'MILLISECOND', 'MIDDLENAME', 'MIN', 'MINUTE',
                               'MINVALUE', 'MOD', 'MODULE_NAME', 'MONTH', 'NAMES',
                               'NATIONAL', 'NATURAL', 'NCHAR', 'NO', 'NOT', 'NTH_VALUE',
                               'NULL', 'NUMERIC', 'OCTET_LENGTH', 'OF', 'OFFSET', 'ON',
                               'ONLY', 'OPEN', 'OPTION', 'OR', 'ORDER', 'OS_NAME', 'OUTER',
                               'OUTPUT_TYPE', 'OVER', 'OVERFLOW', 'OVERLAY', 'PAGE',
                               'PAGES', 'PAGE_SIZE', 'PARAMETER', 'PARTITION', 'PASSWORD',
                               'PI', 'PLACING', 'PLAN', 'POSITION', 'POST_EVENT', 'POWER',
                               'PRECISION', 'PRIMARY', 'PRIVILEGES', 'PROCEDURE',
                               'PROTECTED', 'RAND', 'RANK', 'RDB$DB_KEY',
                               'RDB$RECORD_VERSION', 'READ', 'REAL', 'RECORD_VERSION',
                               'RECREATE', 'RECURSIVE', 'REFERENCES', 'REGR_AVGX',
                               'REGR_AVGY', 'REGR_COUNT', 'REGR_INTERCEPT', 'REGR_R2',
                               'REGR_SLOPE', 'REGR_SXX', 'REGR_SXY', 'REGR_SYY', 'RELEASE',
                               'REPLACE', 'RESERV', 'RESERVING', 'RETAIN', 'RETURN',
                               'RETURNING_VALUES', 'RETURNS', 'REVERSE', 'REVOKE', 'RIGHT',
                               'ROLLBACK', 'ROUND', 'ROW', 'ROW_COUNT', 'ROW_NUMBER',
                               'ROWS', 'RPAD', 'SAVEPOINT', 'SCHEMA', 'SCROLL', 'SECOND',
                               'SEGMENT', 'SELECT', 'SENSITIVE', 'SET', 'SHADOW', 'SHARED',
                               'SIGN', 'SIMILAR', 'SIN', 'SINGULAR', 'SINH', 'SIZE',
                               'SMALLINT', 'SNAPSHOT', 'SOME', 'SORT', 'SQLCODE',
                               'SQLSTATE', 'SQRT', 'STABILITY', 'START', 'STARTING',
                               'STARTS', 'STATISTICS', 'STDDEV_POP', 'STDDEV_SAMP',
                               'SUB_TYPE', 'SUM', 'SUSPEND', 'TABLE', 'TAN', 'TANH',
                               'THEN', 'TIME', 'TIMESTAMP', 'TO', 'TRAILING',
                               'TRANSACTION', 'TRIGGER', 'TRIM', 'TRUE', 'TRUNC',
                               'TRUSTED', 'UNCOMMITTED', 'UNION', 'UNIQUE', 'UNKNOWN',
                               'UPDATE', 'UPPER', 'USER', 'USING', 'UUID_TO_CHAR', 'VALUE',
                               'VALUES', 'VAR_POP', 'VAR_SAMP', 'VARCHAR', 'VARIABLE',
                               'VARYING', 'VIEW', 'WAIT', 'WEEK', 'WHEN', 'WHERE', 'WHILE',
                               'WITH', 'WORK', 'WRITE', 'YEAR']
        elif self.ods == 13.0: # Firebird 4
            self._reserved_ = ['ADD', 'ADMIN', 'ALL', 'ALTER', 'AND', 'ANY', 'AS', 'AT',
                               'AVG', 'BEGIN', 'BETWEEN', 'BIGINT', 'BINARY', 'BIT_LENGTH',
                               'BLOB', 'BOOLEAN', 'BOTH', 'BY', 'CASE', 'CAST', 'CHAR',
                               'CHAR_LENGTH', 'CHARACTER', 'CHARACTER_LENGTH', 'CHECK',
                               'CLOSE', 'COLLATE', 'COLUMN', 'COMMENT', 'COMMIT',
                               'CONNECT', 'CONSTRAINT', 'CORR', 'COUNT', 'COVAR_POP',
                               'COVAR_SAMP', 'CREATE', 'CROSS', 'CURRENT',
                               'CURRENT_CONNECTION', 'CURRENT_DATE', 'CURRENT_ROLE',
                               'CURRENT_TIME', 'CURRENT_TIMESTAMP', 'CURRENT_TRANSACTION',
                               'CURRENT_USER', 'CURSOR', 'DATE', 'DAY', 'DEC', 'DECFLOAT',
                               'DECIMAL', 'DECLARE', 'DEFAULT', 'DELETE', 'DELETING',
                               'DETERMINISTIC', 'DISCONNECT', 'DISTINCT', 'DOUBLE', 'DROP',
                               'ELSE', 'END', 'ESCAPE', 'EXECUTE', 'EXISTS', 'EXTERNAL',
                               'EXTRACT', 'FALSE', 'FETCH', 'FILTER', 'FLOAT', 'FOR',
                               'FOREIGN', 'FROM', 'FULL', 'FUNCTION', 'GDSCODE', 'GLOBAL',
                               'GRANT', 'GROUP', 'HAVING', 'HOUR', 'IN', 'INDEX', 'INNER',
                               'INSENSITIVE', 'INSERT', 'INSERTING', 'INT', 'INT128',
                               'INTEGER', 'INTO', 'IS', 'JOIN', 'LEADING', 'LEFT', 'LIKE',
                               'LATERAL', 'LOCAL', 'LOCALTIME', 'LOCALTIMESTAMP', 'LONG',
                               'LOWER', 'MAX', 'MERGE', 'MIN', 'MINUTE', 'MONTH',
                               'NATIONAL', 'NATURAL', 'NCHAR', 'NO', 'NOT', 'NULL',
                               'NUMERIC', 'OCTET_LENGTH', 'OF', 'OFFSET', 'ON', 'ONLY',
                               'OPEN', 'OR', 'ORDER', 'OUTER', 'OVER', 'PARAMETER', 'PLAN',
                               'POSITION', 'POST_EVENT', 'PRECISION', 'PRIMARY',
                               'PROCEDURE', 'PUBLICATION', 'RDB$DB_KEY', 'RDB$ERROR',
                               'RDB$GET_CONTEXT', 'RDB$GET_TRANSACTION_CN',
                               'RDB$RECORD_VERSION', 'RDB$ROLE_IN_USE', 'RDB$SET_CONTEXT',
                               'RDB$SYSTEM_PRIVILEGE', 'REAL', 'RECORD_VERSION',
                               'RECREATE', 'RECURSIVE', 'REFERENCES', 'REGR_AVGX',
                               'REGR_AVGY', 'REGR_COUNT', 'REGR_INTERCEPT', 'REGR_R2',
                               'REGR_SLOPE', 'REGR_SXX', 'REGR_SXY', 'REGR_SYY', 'RELEASE',
                               'RETURN', 'RETURNING_VALUES', 'RETURNS', 'REVOKE', 'RIGHT',
                               'ROLLBACK', 'ROW', 'ROW_COUNT', 'ROWS', 'SAVEPOINT',
                               'SCHEMA', 'SCROLL', 'SECOND', 'SELECT', 'SENSITIVE', 'SET',
                               'SIMILAR', 'SMALLINT', 'SOME', 'SQLCODE', 'SQLSTATE',
                               'START', 'STDDEV_POP', 'STDDEV_SAMP', 'SUM', 'TABLE',
                               'THEN', 'TIME', 'TIMESTAMP', 'TIMEZONE_HOUR',
                               'TIMEZONE_MINUTE', 'TO', 'TRAILING', 'TRIGGER', 'TRIM',
                               'TRUE', 'UNBOUNDED', 'UNION', 'UNIQUE', 'UNKNOWN', 'UPDATE',
                               'UPDATING', 'UPPER', 'USER', 'USING', 'VALUE', 'VALUES',
                               'VAR_POP', 'VAR_SAMP', 'VARBINARY', 'VARCHAR', 'VARIABLE',
                               'VARYING', 'VIEW', 'WHEN', 'WHERE', 'WHILE', 'WINDOW',
                               'WITH', 'WITHOUT', 'YEAR']
        elif self.ods == 13.1: # Firebird 5.0
            self._ic.execute("SELECT RDB$KEYWORD_NAME FROM RDB$KEYWORDS WHERE RDB$KEYWORD_RESERVED")
            self._reserved_ = [r[0] for r in self._ic]
        else:
            raise Error(f"Unsupported ODS version: {self.ods}")
        self.__attrs = self._select_row('select * from RDB$DATABASE')
        self._default_charset_name = self.__attrs['RDB$CHARACTER_SET_NAME'].strip()
        self._ic.execute("select RDB$OWNER_NAME from RDB$RELATIONS where RDB$RELATION_NAME = 'RDB$DATABASE'")
        self.__owner = self._ic.fetchone()[0].strip()
        # Load enumerate types defined in RDB$TYPES table
        def enum_dict(enum_type):
            return {key: value.strip() for key, value
                    in self._ic.execute('select RDB$TYPE, RDB$TYPE_NAME from RDB$TYPES '
                                        'where RDB$FIELD_NAME = ?', (enum_type,))}
        # Object types
        self.object_types = enum_dict('RDB$OBJECT_TYPE')
        # Object type codes
        self.object_type_codes = {value: key for key, value in self.object_types.items()}
        # Character set names
        self.character_set_names = enum_dict('RDB$CHARACTER_SET_NAME')
        # Field types
        self.field_types = enum_dict('RDB$FIELD_TYPE')
        # Field sub types
        self.field_subtypes = enum_dict('RDB$FIELD_SUB_TYPE')
        # Function types
        self.function_types = enum_dict('RDB$FUNCTION_TYPE')
        # Mechanism Types
        self.mechanism_types = enum_dict('RDB$MECHANISM')
        # Parameter Mechanism Types
        self.parameter_mechanism_types = enum_dict('RDB$PARAMETER_MECHANISM')
        # Procedure Types
        self.procedure_types = enum_dict('RDB$PROCEDURE_TYPE')
        # Relation Types
        self.relation_types = enum_dict('RDB$RELATION_TYPE')
        # System Flag Types
        self.system_flag_types = enum_dict('RDB$SYSTEM_FLAG')
        # Transaction State Types
        self.transaction_state_types = enum_dict('RDB$TRANSACTION_STATE')
        # Trigger Types
        self.trigger_types = enum_dict('RDB$TRIGGER_TYPE')
        # Firebird 3.0
        # Parameter Types
        self.parameter_types = enum_dict('RDB$PARAMETER_TYPE')
        # Index activity
        self.index_activity_flags = enum_dict('RDB$INDEX_INACTIVE')
        # Index uniqueness
        self.index_unique_flags = enum_dict('RDB$UNIQUE_FLAG')
        # Trigger activity
        self.trigger_activity_flags = enum_dict('RDB$TRIGGER_INACTIVE')
        # Grant options
        self.grant_options = enum_dict('RDB$GRANT_OPTION')
        # Page types
        self.page_types = enum_dict('RDB$PAGE_TYPE')
        # Privacy
        self.privacy_flags = enum_dict('RDB$PRIVATE_FLAG')
        # Legacy
        self.legacy_flags = enum_dict('RDB$LEGACY_FLAG')
        # Determinism
        self.deterministic_flags = enum_dict('RDB$DETERMINISTIC_FLAG')
        # Identity
        self.identity_type = enum_dict('RDB$IDENTITY_TYPE')
        # Map to type
        self._map_to_type_ = enum_dict('RDB$MAP_TO_TYPE')
        return self
    def close(self) -> None:
        """Closes the internal cursor and transaction, detaching from the connection.

        After closing, the schema object cannot be used to fetch further metadata.
        Attempting to access unloaded properties will raise an Error.

        Raises:
            Error: If attempting to close an embedded schema instance
                   (obtained via `Connection.schema`).
        """
        if self.__internal:
            raise Error("Call to 'close' not allowed for embedded Schema.")
        self._close()
        self.__clear()
    def clear(self) -> None:
        """Clears all cached metadata collections (tables, views, etc.).

        Does not affect the binding to the connection or basic database attributes
        (like ODS version). Metadata will be reloaded from the database on the
        next access to a relevant property. Also commits the internal transaction
        if active.
        """
        self.__clear()
    def reload(self, data: Category | list[Category] | None=None) -> None:
        """Clears cached metadata for specified categories and commits the internal transaction.

        If `data` is None, clears all cached metadata collections. Otherwise, clears
        only the collections specified in the `data` iterable (e.g., `[Category.TABLES, Category.VIEWS]`).
        This forces the specified metadata to be reloaded from the database on next access.

        Arguments:
            data: A specific `.Category` enum member, an iterable of `.Category` members,
                  or `None` to clear all categories. Defaults to `None`.

        Raises:
            Error: If the instance is closed or if an invalid category is provided.
        """
        self.__clear(data)
        if not self.closed:
            self._ic.transaction.commit()
    def get_item(self, name: str, itype: ObjectType, subname: str | None=None) -> SchemaItem:
        """Retrieves a specific database object by its type and name(s).

        Arguments:
            name: The primary name of the database object (e.g., table name, procedure name).
            itype: The `.ObjectType` enum value specifying the type of object to retrieve.
            subname: An optional secondary name, typically used for columns (`itype=ObjectType.COLUMN`)
                     where `name` is the table/view name and `subname` is the column name.

        Returns:
            An instance of a `.SchemaItem` subclass (e.g., `.Table`, `.Procedure`),
            a `~firebird.driver.UserInfo` instance (for `itype=ObjectType.USER`),
            or `None` if the object is not found.
        """
        result = None
        if itype is ObjectType.TABLE:
            result = self.all_tables.get(name)
        elif itype is ObjectType.VIEW:
            result = self.all_views.get(name)
        elif itype is ObjectType.TRIGGER:
            result = self.all_triggers.get(name)
        elif itype is ObjectType.PROCEDURE:
            result = self.all_procedures.get(name)
        elif itype is ObjectType.USER:
            res = self._get_users().get(name)
            if not res:
                res = UserInfo(user_name=name)
                self.__users.append(res)
            result = res
        elif itype is ObjectType.COLUMN:
            if subname is None:
                result = self.all_domains.get(name)
            else:
                result = self.all_tables.get(name).columns.get(subname)
        elif itype is ObjectType.INDEX:
            result = self.all_indices.get(name)
        elif itype is ObjectType.CHARACTER_SET:
            result = self.character_sets.get(name)
        elif itype is ObjectType.ROLE:
            result = self.roles.get(name)
        elif itype is ObjectType.GENERATOR:
            result = self.all_generators.get(name)
        elif itype is ObjectType.UDF:
            result = self.all_functions.get(name)
        elif itype is ObjectType.COLLATION:
            result = self.collations.get(name)
        elif itype in (ObjectType.PACKAGE_HEADER, ObjectType.PACKAGE_BODY): # Package
            result = self.packages.get(name)
        return result
    def get_metadata_ddl(self, *, sections=SCRIPT_DEFAULT_ORDER) -> list[str]:
        """Generates a list of DDL SQL commands for creating database objects.

        Constructs a DDL script based on the schema information cached in this instance.

        Arguments:
            sections: An iterable of `.Section` enum members specifying which types of
                      database objects to include in the DDL script and the order
                      in which their creation statements should appear. Defaults to
                      `SCRIPT_DEFAULT_ORDER`.

        Returns:
            A list of strings, where each string is a single DDL SQL command.

        Raises:
            ValueError: If an unknown section code is provided in the `sections` list.
            Error: If required metadata for a requested section has not been loaded yet
                   (e.g., accessing `schema.tables` might be needed first).
        """
        def order_by_dependency(items, get_dependencies):
            ordered = []
            wlist = list(items)
            while len(wlist) > 0:
                item = wlist.pop(0)
                add = True
                for dep in get_dependencies(item):
                    if isinstance(dep.depended_on, View) and dep.depended_on not in ordered:
                        wlist.append(item)
                        add = False
                        break
                if add:
                    ordered.append(item)
            return ordered
        def view_dependencies(item):
            return [x for x in item.get_dependencies()
                    if x.depended_on_type == 1]
        #
        script = []
        for section in sections:
            if section == Section.COLLATIONS:
                for collation in self.collations:
                    if not collation.is_sys_object():
                        script.append(collation.get_sql_for('create'))
            elif section == Section.CHARACTER_SETS:
                for charset in self.character_sets:
                    if charset.name != charset.default_collation.name:
                        script.append(charset.get_sql_for('alter',
                                                          collation=charset.default_collation.name))
            elif section == Section.UDFS:
                for udf in self.functions:
                    if udf.is_external():
                        script.append(udf.get_sql_for('declare'))
            elif section == Section.GENERATORS:
                for generator in self.generators:
                    script.append(generator.get_sql_for('create'))
            elif section == Section.EXCEPTIONS:
                for e in self.exceptions:
                    script.append(e.get_sql_for('create'))
            elif section == Section.DOMAINS:
                for domain in self.domains:
                    script.append(domain.get_sql_for('create'))
            elif section == Section.PACKAGE_DEFS:
                for package in self.packages:
                    if not package.is_sys_object():
                        script.append(package.get_sql_for('create'))
            elif section == Section.FUNCTION_DEFS:
                for func in (x for x in self.functions if
                             not x.is_external() and
                             not x.is_packaged()):
                    script.append(func.get_sql_for('create', no_code=True))
            elif section == Section.PROCEDURE_DEFS:
                for proc in (x for x in self.procedures if not x.is_packaged()):
                    script.append(proc.get_sql_for('create', no_code=True))
            elif section == Section.TABLES:
                for table in self.tables:
                    script.append(table.get_sql_for('create', no_pk=True, no_unique=True))
            elif section == Section.PRIMARY_KEYS:
                for constraint in (x for x in self.constraints if x.is_pkey()):
                    script.append(constraint.get_sql_for('create'))
            elif section == Section.UNIQUE_CONSTRAINTS:
                for table in self.tables:
                    for constraint in (x for x in table.constraints if x.is_unique()):
                        script.append(constraint.get_sql_for('create'))
            elif section == Section.CHECK_CONSTRAINTS:
                for table in self.tables:
                    for constraint in (x for x in table.constraints if x.is_check()):
                        script.append(constraint.get_sql_for('create'))
            elif section == Section.FOREIGN_CONSTRAINTS:
                for table in self.tables:
                    for constraint in (x for x in table.constraints if x.is_fkey()):
                        script.append(constraint.get_sql_for('create'))
            elif section == Section.INDICES:
                for table in self.tables:
                    for index in (x for x in table.indices
                                  if not x.is_enforcer()):
                        script.append(index.get_sql_for('create'))
            elif section == Section.VIEWS:
                for view in order_by_dependency(self.views, view_dependencies):
                    script.append(view.get_sql_for('create'))
            elif section == Section.PACKAGE_BODIES:
                for package in self.packages:
                    if not package.is_sys_object():
                        script.append(package.get_sql_for('create', body=True))
            elif section == Section.PROCEDURE_BODIES:
                for proc in (x for x in self.procedures if not x.is_packaged()):
                    script.append('ALTER' + proc.get_sql_for('create')[6:])
            elif section == Section.FUNCTION_BODIES:
                for func in (x for x in self.functions if
                             not x.is_external() and
                             not x.is_packaged()):
                    script.append('ALTER' + func.get_sql_for('create')[6:])
            elif section == Section.TRIGGERS:
                for trigger in self.triggers:
                    script.append(trigger.get_sql_for('create'))
            elif section == Section.ROLES:
                for role in (x for x in self.roles if not x.is_sys_object()):
                    script.append(role.get_sql_for('create'))
            elif section == Section.GRANTS:
                for priv in (x for x in self.privileges
                             if x.user_name != 'SYSDBA'
                             and not x.subject.is_sys_object()):
                    script.append(priv.get_sql_for('grant'))
            elif section == Section.COMMENTS:
                for objects in (self.character_sets, self.collations,
                                self.exceptions, self.domains,
                                self.generators, self.tables,
                                self.indices, self.views,
                                self.triggers, self.procedures,
                                self.functions, self.roles):
                    for obj in objects:
                        if obj.description is not None:
                            script.append(obj.get_sql_for('comment'))
                        if isinstance(obj, Table | View):
                            for col in obj.columns:
                                if col.description is not None:
                                    script.append(col.get_sql_for('comment'))
                        elif isinstance(obj, Procedure):
                            if isinstance(obj, Table | View):
                                for par in obj.input_params:
                                    if par.description is not None:
                                        script.append(par.get_sql_for('comment'))
                                for par in obj.output_params:
                                    if par.description is not None:
                                        script.append(par.get_sql_for('comment'))
            elif section == Section.SHADOWS:
                for shadow in self.shadows:
                    script.append(shadow.get_sql_for('create'))
            elif section == Section.INDEX_DEACTIVATIONS:
                for index in self.indices:
                    script.append(index.get_sql_for('deactivate'))
            elif section == Section.INDEX_ACTIVATIONS:
                for index in self.indices:
                    script.append(index.get_sql_for('activate'))
            elif section == Section.SET_GENERATORS:
                for generator in self.generators:
                    script.append(generator.get_sql_for('alter', value=generator.value))
            elif section == Section.TRIGGER_DEACTIVATIONS:
                for trigger in self.triggers:
                    script.append(trigger.get_sql_for('alter', active=False))
            elif section == Section.TRIGGER_ACTIVATIONS:
                for trigger in self.triggers:
                    script.append(trigger.get_sql_for('alter', active=True))
            else:
                raise ValueError(f"Unknown section code {section}")
        return script
    def is_keyword(self, ident: str) -> bool:
        """Checks if the given identifier is a reserved keyword for the database's ODS version.

        Arguments:
            ident: The identifier string to check (case-insensitive comparison).

        Returns:
            `True` if the identifier is a reserved keyword, `False` otherwise.
        """
        return ident in self._reserved_
    def is_multifile(self) -> bool:
        """Checks if the database consists of multiple physical files (has secondary files).

        Returns:
            `True` if the database has secondary files defined, `False` otherwise.
        """
        return len(self.files) > 0
    def get_collation_by_id(self, charset_id: int, collation_id: int) -> Collation:
        """Retrieves a `.Collation` object by its character set ID and collation ID.

        Arguments:
            charset_id: The numeric ID of the character set (`RDB$CHARACTER_SET_ID`).
            collation_id: The numeric ID of the collation within the character set (`RDB$COLLATION_ID`).

        Returns:
            The matching `.Collation` instance, or `None` if not found or not loaded.
        """
        return self.collations.find(lambda i: i.character_set.id == charset_id and i.id == collation_id)
    def get_charset_by_id(self, charset_id: int) -> CharacterSet:
        """Retrieves a `.CharacterSet` object by its ID.

        Arguments:
            charset_id: The numeric ID of the character set (`RDB$CHARACTER_SET_ID`).

        Returns:
            The matching `.CharacterSet` instance, or `None` if not found or not loaded.
        """
        return self.character_sets.find(lambda i: i.id == charset_id)
    def get_privileges_of(self, user: str | UserInfo | Table | View | Procedure | Trigger | Role,
                          user_type: ObjectType | None=None) -> DataList[Privilege]:
        """Retrieves a list of all privileges granted *to* a specific user or database object (grantee).

        Arguments:
            user: The grantee, specified either as a string name, a `~firebird.driver.UserInfo` instance,
                  or a `.SchemaItem` subclass instance (e.g., `.Role`, `.Procedure`).
            user_type: The `.ObjectType` of the grantee. **Required if** `user` is provided
                       as a string name. Ignored otherwise.

        Returns:
            A `.DataList` containing `.Privilege` objects granted to the specified user/object.
            Returns an empty list if no privileges are found or privileges haven't been loaded.

        Raises:
            ValueError: If `user` is a string name and `user_type` is not provided.
        """
        if isinstance(user, str):
            if user_type is None:
                raise ValueError("Argument user_type required")
            uname = user
            utype = [user_type]
        elif isinstance(user, Table | View | Procedure | Trigger | Role):
            uname = user.name
            utype = user._type_code
        elif isinstance(user, UserInfo):
            uname = user.user_name
            utype = [ObjectType.USER]
        return self.privileges.extract(lambda p: (p.user_name == uname)
                                       and (p.user_type in utype), copy=True)
    @property
    def closed(self) -> bool:
        """`True` if the schema object is closed or not bound to a connection."""
        return self._con is None
    @property
    def description(self) -> str | None:
        """The database description string from `RDB$DATABASE`, or `None`."""
        return self.__attrs['RDB$DESCRIPTION']
    @property
    def owner_name(self) -> str | None:
        """The user name of the database owner, or `None` if not determined."""
        return self.__owner
    @property
    def default_character_set(self) -> CharacterSet:
        """Default `.CharacterSet` for database."""
        return self.character_sets.get(self._default_charset_name)
    @property
    def security_class(self) -> str:
        """Can refer to the security class applied as databasewide access control limits."""
        return self.__attrs['RDB$SECURITY_CLASS'].strip()
    @property
    def collations(self) -> DataList[Collation]:
        """`.DataList` of all `.Collation` objects defined in the database. Loads lazily."""
        if self.__collations is None:
            self.__fail_if_closed()
            self.__collations = DataList((Collation(self, row) for row
                                          in self._select('select * from rdb$collations')),
                                         Collation, 'item.name', frozen=True)
        return self.__collations
    @property
    def character_sets(self) -> DataList[CharacterSet]:
        """`.DataList` of all `.CharacterSet` objects defined in the database. Loads lazily."""
        if self.__character_sets is None:
            self.__fail_if_closed()
            self.__character_sets = DataList((CharacterSet(self, row) for row
                                              in self._select('select * from rdb$character_sets')),
                                             CharacterSet, 'item.name', frozen=True)
        return self.__character_sets
    @property
    def exceptions(self) -> DataList[DatabaseException]:
        """`.DataList` of all `.DatabaseException` objects defined in the database. Loads lazily."""
        if self.__exceptions is None:
            self.__fail_if_closed()
            self.__exceptions = DataList((DatabaseException(self, row) for row
                                          in self._select('select * from rdb$exceptions')),
                                         DatabaseException, 'item.name', frozen=True)

        return self.__exceptions
    @property
    def generators(self) -> DataList[Sequence]:
        """`.DataList` of all user-defined `.Sequence` objects (generators) in the database.
        Loads lazily."""
        return self._get_all_generators()[0]
    @property
    def sys_generators(self) -> DataList[Sequence]:
        """`.DataList` of all system `.Sequence` objects (generators) in the database. Loads lazily."""
        return self._get_all_generators()[1]
    @property
    def all_generators(self) -> DataList[Sequence]:
        """`.DataList` of all (user + system) `.Sequence` objects (generators) in the database. Loads lazily."""
        return self._get_all_generators()[2]
    @property
    def domains(self) ->  DataList[Domain]:
        """`.DataList` of all user-defined `.Domain` objects in the database. Loads lazily."""
        return self._get_all_domains()[0]
    @property
    def sys_domains(self) ->  DataList[Domain]:
        """`.DataList` of all system `.Domain` objects in the database. Loads lazily."""
        return self._get_all_domains()[1]
    @property
    def all_domains(self) ->  DataList[Domain]:
        """`.DataList` of all (user + system) `.Domain` objects in the database. Loads lazily."""
        return self._get_all_domains()[2]
    @property
    def indices(self) -> DataList[Index]:
        """`.DataList` of all user-defined `.Index` objects in the database. Loads lazily."""
        return self._get_all_indices()[0]
    @property
    def sys_indices(self) -> DataList[Index]:
        """`.DataList` of all system `.Index` objects in the database. Loads lazily."""
        return self._get_all_indices()[1]
    @property
    def all_indices(self) -> DataList[Index]:
        """`.DataList` of all (user + system) `.Index` objects in the database. Loads lazily."""
        return self._get_all_indices()[2]
    @property
    def tables(self) -> DataList[Table]:
        """`.DataList` of all user-defined `.Table` objects in the database. Loads lazily."""
        return self._get_all_tables()[0]
    @property
    def sys_tables(self) -> DataList[Table]:
        """`.DataList` of all system `.Table` objects in the database. Loads lazily."""
        return self._get_all_tables()[1]
    @property
    def all_tables(self) -> DataList[Table]:
        """`.DataList` of all (user + system) `.Table` objects in the database. Loads lazily."""
        return self._get_all_tables()[2]
    @property
    def views(self) -> DataList[View]:
        """`.DataList` of all user-defined `.View` objects in the database. Loads lazily."""
        return self._get_all_views()[0]
    @property
    def sys_views(self) -> DataList[View]:
        """`.DataList` of all system `.View` objects in the database. Loads lazily."""
        return self._get_all_views()[1]
    @property
    def all_views(self) -> DataList[View]:
        """`.DataList` of all (user + system) `.View` objects in the database. Loads lazily."""
        return self._get_all_views()[2]
    @property
    def triggers(self) -> DataList[Trigger]:
        """`.DataList` of all user-defined `.Trigger` objects in the database. Loads lazily."""
        return self._get_all_triggers()[0]
    @property
    def sys_triggers(self) -> DataList[Trigger]:
        """`.DataList` of all system `.Trigger` objects in the database. Loads lazily."""
        return self._get_all_triggers()[1]
    @property
    def all_triggers(self) -> DataList[Trigger]:
        """`.DataList` of all (user + system) `.Trigger` objects in the database. Loads lazily."""
        return self._get_all_triggers()[2]
    @property
    def procedures(self) -> DataList[Procedure]:
        """`.DataList` of all user-defined `.Procedure` objects in the database. Loads lazily."""
        return self._get_all_procedures()[0]
    @property
    def sys_procedures(self) -> DataList[Procedure]:
        """`.DataList` of all system `.Procedure` objects in the database. Loads lazily."""
        return self._get_all_procedures()[1]
    @property
    def all_procedures(self) -> DataList[Procedure]:
        """`.DataList` of all (user + system) `.Procedure` objects in the database. Loads lazily."""
        return self._get_all_procedures()[2]
    @property
    def constraints(self) -> DataList[Constraint]:
        """`.DataList` of all `.Constraint` objects in the database. Loads lazily."""
        if self.__constraints is None:
            self.__fail_if_closed()
            # Dummy call to _get_all_tables() is necessary as
            # Constraint.is_sys_object() that is called in Constraint.__init__()
            # will drop result from internal cursor and we'll not load all constraints.
            self._get_all_tables()
            cmd = """select c.RDB$CONSTRAINT_NAME,
c.RDB$CONSTRAINT_TYPE, c.RDB$RELATION_NAME, c.RDB$DEFERRABLE,
c.RDB$INITIALLY_DEFERRED, c.RDB$INDEX_NAME, r.RDB$CONST_NAME_UQ,
r.RDB$MATCH_OPTION,r.RDB$UPDATE_RULE,r.RDB$DELETE_RULE,
k.RDB$TRIGGER_NAME from rdb$relation_constraints C
left outer join rdb$ref_constraints R on C.rdb$constraint_name = R.rdb$constraint_name
left outer join rdb$check_constraints K on (C.rdb$constraint_name = K.rdb$constraint_name)
and (c.RDB$CONSTRAINT_TYPE in ('CHECK','NOT NULL'))"""
            self.__constraints = DataList((Constraint(self, row) for row
                                           in self._select(cmd)), Constraint, 'item.name')
            # Check constrains need special care because they're doubled
            # (select above returns two records for them with different trigger names)
            checks = self.__constraints.extract(lambda item: item.is_check())
            dchecks = {}
            for check in checks:
                dchecks.setdefault(check.name, []).append(check)
            for checklist in dchecks.values():
                names = [c._attributes['RDB$TRIGGER_NAME'] for c in checklist]
                check = checklist[0]
                check._attributes['RDB$TRIGGER_NAME'] = names
                self.__constraints.append(check)
            self.__constraints.freeze()
        return self.__constraints
    @property
    def roles(self) -> DataList[Role]:
        """`.DataList` of all `.Role` objects in the database. Loads lazily."""
        if self.__roles is None:
            self.__fail_if_closed()
            self.__roles = DataList((Role(self, row) for row
                                     in self._select('select * from rdb$roles')),
                                    Role, 'item.name')
            self.__roles.freeze()
        return self.__roles
    @property
    def dependencies(self) -> DataList[Dependency]:
        """`.DataList` of all `.Dependency` objects in the database. Loads lazily."""
        if self.__dependencies is None:
            self.__fail_if_closed()
            self.__dependencies = DataList((Dependency(self, row) for row
                                            in self._select('select * from rdb$dependencies')),
                                           Dependency)
        return self.__dependencies
    @property
    def functions(self) -> DataList[Function]:
        """`.DataList` of all user-defined `.Function` objects in the database. Loads lazily."""
        return self._get_all_functions()[0]
    @property
    def sys_functions(self) -> DataList[Function]:
        """`.DataList` of all system `.Function` objects in the database. Loads lazily."""
        return self._get_all_functions()[1]
    @property
    def all_functions(self) -> DataList[Function]:
        """`.DataList` of all (user + system) `.Function` objects in the database. Loads lazily."""
        return self._get_all_functions()[2]
    @property
    def files(self) -> DataList[DatabaseFile]:
        """`.DataList` of all `.DatabaseFile` objects in the database. Loads lazily."""
        if self.__files is None:
            self.__fail_if_closed()
            cmd = """select RDB$FILE_NAME, RDB$FILE_SEQUENCE,
RDB$FILE_START, RDB$FILE_LENGTH from RDB$FILES
where RDB$SHADOW_NUMBER = 0
order by RDB$FILE_SEQUENCE"""
            self.__files = DataList((DatabaseFile(self, row) for row
                                     in self._select(cmd)), DatabaseFile, 'item.name')
            self.__files.freeze()
        return self.__files
    @property
    def shadows(self) -> DataList[Shadow]:
        """`.DataList` of all `.Shadow` objects in the database. Loads lazily."""
        if self.__shadows is None:
            self.__fail_if_closed()
            cmd = """select RDB$FILE_FLAGS, RDB$SHADOW_NUMBER
from RDB$FILES
where RDB$SHADOW_NUMBER > 0 AND RDB$FILE_SEQUENCE = 0
order by RDB$SHADOW_NUMBER"""
            self.__shadows = DataList((Shadow(self, row) for row
                                       in self._select(cmd)), Shadow, 'item.name')
            self.__shadows.freeze()
        return self.__shadows
    @property
    def privileges(self) -> DataList[Privilege]:
        """`.DataList` of all `.Privilege` objects in the database. Loads lazily."""
        if self.__privileges is None:
            self.__fail_if_closed()
            cmd = """select RDB$USER, RDB$GRANTOR, RDB$PRIVILEGE,
RDB$GRANT_OPTION, RDB$RELATION_NAME, RDB$FIELD_NAME, RDB$USER_TYPE, RDB$OBJECT_TYPE
FROM RDB$USER_PRIVILEGES"""
            self.__privileges = DataList((Privilege(self, row) for row
                                          in self._select(cmd)), Privilege)
        return self.__privileges
    @property
    def backup_history(self) -> DataList[BackupHistory]:
        """`.DataList` of all `.BackupHistory` objects in the database. Loads lazily."""
        if self.__backup_history is None:
            self.__fail_if_closed()
            cmd = """SELECT RDB$BACKUP_ID, RDB$TIMESTAMP,
RDB$BACKUP_LEVEL, RDB$GUID, RDB$SCN, RDB$FILE_NAME
FROM RDB$BACKUP_HISTORY"""
            self.__backup_history = DataList((BackupHistory(self, row) for row
                                              in self._select(cmd)), BackupHistory, 'item.name')
            self.__backup_history.freeze()
        return self.__backup_history
    @property
    def filters(self) -> DataList[Filter]:
        """`.DataList` of all user-defiend `.Filter` objects in the database. Loads lazily."""
        if self.__filters is None:
            self.__fail_if_closed()
            cmd = """SELECT RDB$FUNCTION_NAME, RDB$DESCRIPTION,
RDB$MODULE_NAME, RDB$ENTRYPOINT, RDB$INPUT_SUB_TYPE, RDB$OUTPUT_SUB_TYPE, RDB$SYSTEM_FLAG
FROM RDB$FILTERS"""
            self.__filters = DataList((Filter(self, row) for row
                                       in self._select(cmd)), Filter, 'item.name')
            self.__filters.freeze()
        return self.__filters
    @property
    def packages(self) -> DataList[Package]:
        """`.DataList` of all `.Package` objects in the database. Loads lazily."""
        if self.__packages is None:
            self.__fail_if_closed()
            cmd = """select RDB$PACKAGE_NAME, RDB$PACKAGE_HEADER_SOURCE,
RDB$PACKAGE_BODY_SOURCE, RDB$VALID_BODY_FLAG, RDB$SECURITY_CLASS, RDB$OWNER_NAME,
RDB$SYSTEM_FLAG, RDB$DESCRIPTION
            FROM RDB$PACKAGES"""
            self.__packages = DataList((Package(self, row) for row
                                        in self._select(cmd)), Package, 'item.name')
            self.__packages.freeze()
        return self.__packages
    @property
    def linger(self) -> int | None:
        """Database linger value."""
        return self.__attrs['RDB$LINGER']

class SchemaItem(Visitable):
    """Abstract base class for all objects representing elements within a Firebird database schema.

    This class provides a common interface and shared functionality for objects
    like tables, views, procedures, domains, indices, etc. It holds metadata
    retrieved from the `RDB$` system tables and facilitates interaction with the
    schema structure.

    Key Features:

    *   Access to the parent `.Schema` instance (via a weak reference).
    *   Storage of raw metadata attributes fetched from system tables.
    *   Methods for retrieving the object's name, description, and quoted identifier.
    *   Functionality to find dependent and depended-on objects within the schema.
    *   A mechanism (`.get_sql_for()`) to generate DDL/DML SQL commands (like CREATE,
        ALTER, DROP, COMMENT) specific to the object type.
    *   Support for the Visitor pattern via the `.accept()` method.

    Subclasses typically override methods like `._get_name()` and implement
    specific `_get_<action>_sql()` methods to provide type-specific behavior.
    Instances are usually created and managed by the parent `.Schema` object.

    Arguments:
        schema: The parent `.Schema` instance this item belongs to.
        attributes: A dictionary containing the raw column names (e.g.,
                    'RDB$RELATION_NAME', 'RDB$SYSTEM_FLAG') and their
                    corresponding values fetched from the relevant RDB$
                    system table row for this schema object.
    """
    schema: Schema | None = None
    def __init__(self, schema: Schema, attributes: dict[str, Any]):
        #: Weak reference proxy to the parent `.Schema` instance.
        #: Provides access to the overall schema context without creating circular references.
        self.schema: Schema = schema if isinstance(schema, weakref.ProxyType) else weakref.proxy(schema)
        #: Internal list storing the `.ObjectType` enum values that this
        #: specific schema item class represents (used for dependency lookups).
        #: Populated by subclasses.
        self._type_code: list[ObjectType] = []
        #: Dictionary holding the raw attributes fetched from the monitoring table row
        #: (keys are typically 'RDB$...'). Subclasses access this to provide
        #: specific property values.
        self._attributes: dict[str, Any] = attributes
        #: List of action strings (lowercase, e.g., 'create', 'drop', 'alter')
        #: supported by the `get_sql_for()` method for this specific object type.
        #: Populated by subclasses.
        self._actions: list[str] = []
    def _strip_attribute(self, attr: str) -> None:
        """Internal helper: Removes leading/trailing whitespace from a string attribute if it exists."""
        if self._attributes.get(attr):
            self._attributes[attr] = self._attributes[attr].strip()
    def _check_params(self, params: dict[str, Any], param_names: list[str]) -> None:
        """Internal helper: Validates keyword arguments passed to `get_sql_for`.

        Checks if all keys in the `params` dictionary are present in the
        `param_names` list of allowed parameters for a specific SQL action.

        Arguments:
            params: The dictionary of parameters received by the `_get_<action>_sql` method.
            param_names: A list of strings representing the allowed parameter names for that action.

        Raises:
            ValueError: If `params` contains any key not found in `param_names`.
        """
        p = set(params.keys())
        n = set(param_names)
        if not p.issubset(n):
            raise ValueError(f"Unsupported parameter(s) '{','.join(p.difference(n))}'")
    def _needs_quoting(self, ident: str) -> bool:
        """Internal helper: Determines if an identifier requires double quotes in SQL.

        Checks based on `Schema.opt_always_quote` setting, reserved words
        (`Schema.is_keyword`), starting characters, and allowed characters
        (A-Z, 0-9, _, $).

        Returns:
            `True` if the identifier needs quoting, `False` otherwise. Returns `False`
            for empty or None identifiers.
        """
        if not ident:
            return False
        if self.schema.opt_always_quote:
            return True
        if ident and ident[0] not in 'ABCDEFGHIJKLMNOPQRSTUVWXYZ':
            return True
        for char in ident:
            if char not in 'ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789$_':
                return True
        return self.schema.is_keyword(ident)
    def _get_quoted_ident(self, ident: str) -> str:
        """Internal helper: Returns the identifier, quoted if necessary."""
        return f'"{ident}"' if self._needs_quoting(ident) else ident
    def _get_name(self) -> str | None:
        """Internal method: Retrieves the primary name of the schema object.

        Subclasses should override this to return the name from the
        appropriate 'RDB$...' attribute (e.g., 'RDB$RELATION_NAME',
        'RDB$PROCEDURE_NAME').

        Returns:
            The name of the object as a string, or `None` if the object
            conceptually lacks a name (though most schema items have one).
        """
        return None
    def _get_create_sql(self, **params) -> str:
        """Abstract method for generating 'CREATE' SQL. Subclasses must implement."""
        raise NotImplementedError
    def _get_recreate_sql(self, **params) -> str:
        """Generates 'RECREATE' SQL by prefixing the 'CREATE' SQL.
           Subclasses can override if 'RECREATE' differs more significantly."""
        return 'RE'+self._get_create_sql(**params)
    def _get_create_or_alter_sql(self, **params) -> str:
        """Generates 'CREATE OR ALTER' SQL by modifying the 'CREATE' SQL.
           Subclasses can override if needed."""
        return 'CREATE OR ALTER' + self._get_create_sql(**params)[6:]
    def is_sys_object(self) -> bool:
        """Checks if this schema object is a system-defined object.

        Typically determined by checking the `RDB$SYSTEM_FLAG` attribute (> 0).
        Subclasses may provide more specific logic (e.g., based on name prefixes like 'RDB$').

        Returns:
            `True` if it's considered a system object, `False` otherwise.
        """
        return self._attributes.get('RDB$SYSTEM_FLAG', 0) > 0
    def get_quoted_name(self) -> str:
        """Retrieves a list of database objects that depend on this object.

        Queries `Schema.dependencies` based on this object's `name` and `_type_code`.

        Returns:
            A `.DataList` containing `.Dependency` objects where this item is the
            `depended_on` object. Returns an empty list if no dependents are found
            or dependencies haven't been loaded in the parent schema.
        """
        return self._get_quoted_ident(self.name)
    def get_dependents(self) -> DataList[Dependency]:
        """Returns list of all database objects that depend on this one.
        """
        result = self.schema.dependencies.extract(lambda d: d.depended_on_name == self.name and
                                                  d.depended_on_type in self._type_code, copy=True)
        result.freeze()
        return result
    def get_dependencies(self) -> DataList[Dependency]:
        """Retrieves a list of database objects that this object depends on.

        Queries `Schema.dependencies` based on this object's `name` and `_type_code`.

        Returns:
            A `.DataList` containing `.Dependency` objects where this item is the
            `dependent` object. Returns an empty list if this object has no
            dependencies or dependencies haven't been loaded in the parent schema.
        """
        result = self.schema.dependencies.extract(lambda d: d.dependent_name == self.name and
                                                  d.dependent_type in self._type_code, copy=True)
        result.freeze()
        return result
    def get_sql_for(self, action: str, **params: dict) -> str:
        """Generates a DDL/DML SQL command for a specified action on this schema object.

        Arguments:
            action: The desired SQL action (e.g., 'create', 'drop', 'alter', 'comment').
                    The action must be present (case-insensitively) in the object's
                    `.actions` list.
            **params: Keyword arguments specific to the requested `action`. These are
                      validated and passed directly to the internal `_get_<action>_sql`
                      method implemented by the subclass. Consult the specific
                      subclass documentation for available parameters for each action.

        Returns:
            A string containing the generated SQL command.

        Raises:
            ValueError: If the requested `action` is not supported for this object
                        type, or if invalid/missing `params` are provided for the action.
            NotImplementedError: If the action is listed but the corresponding
                                 `_get_<action>_sql` method is not implemented.
        """
        if (_action := action.lower()) in self._actions:
            return getattr(self, f'_get_{_action}_sql')(**params)
        raise ValueError(f"Unsupported action '{action}'")
    @property
    def name(self) -> str | None:
        """The primary name of this schema object (e.g., table name, procedure name),
        or `None` if the object type does not have a name."""
        return self._get_name()
    @property
    def description(self) -> str | None:
        """The description (comment) associated with this object from `RDB$DESCRIPTION`,
        or `None` if it has no description."""
        return self._attributes.get('RDB$DESCRIPTION')
    @property
    def actions(self) -> list[str]:
        """A list of lowercase action strings (e.g., 'create', 'drop', 'alter')
        that are supported by the `get_sql_for()` method for this specific object type."""
        return self._actions

class Collation(SchemaItem):
    """Represents a specific collation, defining rules for character sorting and comparison.

    Collations are always associated with a specific `.CharacterSet`. They determine
    aspects like case sensitivity, accent sensitivity, and handling of padding spaces
    during comparisons.

    Instances of this class map data primarily from the `RDB$COLLATIONS` system table.
    They are typically accessed via `Schema.collations` or `CharacterSet.collations`.

    Supported SQL actions via `.get_sql_for()`:

    *   User-defined collations: `create`, `drop`, `comment`.
    *   System collations: `comment`.

    Arguments:
        schema: The parent `.Schema` instance.
        attributes: Raw data dictionary fetched from the `RDB$COLLATIONS` row.
    """
    def __init__(self, schema: Schema, attributes: dict[str, Any]):
        super().__init__(schema, attributes)
        self._type_code.append(ObjectType.COLLATION)
        self._strip_attribute('RDB$COLLATION_NAME')
        self._strip_attribute('RDB$BASE_COLLATION_NAME')
        self._strip_attribute('RDB$FUNCTION_NAME')
        self._strip_attribute('RDB$SECURITY_CLASS')
        self._strip_attribute('RDB$OWNER_NAME')
        self._actions.append('comment')
        if not self.is_sys_object():
            self._actions.extend(['create', 'drop'])
    def _get_drop_sql(self, **params) -> str:
        """Generates the SQL command to DROP this collation.

        Arguments:
            **params: Accepts no parameters.

        Returns:
            The `DROP COLLATION` SQL string.

        Raises:
            ValueError: If unexpected parameters are passed.
        """
        self._check_params(params, [])
        return f'DROP COLLATION {self.get_quoted_name()}'
    def _get_create_sql(self, **params) -> str:
        """Generates the SQL command to CREATE this collation.

        Constructs the `CREATE COLLATION` statement including character set,
        base collation (internal or external), padding, case/accent sensitivity,
        and specific attributes.

        Arguments:
            **params: Accepts no parameters.

        Returns:
            The `CREATE COLLATION` SQL string.

        Raises:
            ValueError: If unexpected parameters are passed.
        """
        self._check_params(params, [])
        if self.is_based_on_external():
            from_ = f"FROM EXTERNAL ('{self._attributes['RDB$BASE_COLLATION_NAME']}')"
        else:
            from_ = f"FROM {self.base_collation.get_quoted_name()}"
        spec = f"\n   '{self.specific_attributes}'" if self.specific_attributes else ''
        return f"CREATE COLLATION {self.get_quoted_name()}\n" \
               f"   FOR {self.character_set.get_quoted_name()}\n" \
               f"   {from_}\n" \
               f"   {'PAD SPACE' if CollationFlag.PAD_SPACE in self.attributes else 'NO PAD'}\n" \
               f"   {'CASE INSENSITIVE' if CollationFlag.CASE_INSENSITIVE in self.attributes else 'CASE SENSITIVE'}\n" \
               f"   {'ACCENT INSENSITIVE' if CollationFlag.ACCENT_INSENSITIVE in self.attributes else 'ACCENT SENSITIVE'}" \
               f"{spec}"
    def _get_comment_sql(self, **params) -> str:
        """Generates the SQL command to add or remove a COMMENT ON this collation.

        Arguments:
            **params: Accepts no parameters.

        Returns:
            The `COMMENT ON COLLATION` SQL string. Sets comment to `NULL` if
            `self.description` is None, otherwise uses the description text
            with proper escaping.
        """
        comment = 'NULL' if self.description is None \
            else f"'{escape_single_quotes(self.description)}'"
        return f'COMMENT ON COLLATION {self.get_quoted_name()} IS {comment}'
    def _get_name(self) -> str:
        """Returns the collation name (`RDB$COLLATION_NAME`)."""
        return self._attributes['RDB$COLLATION_NAME']
    def is_based_on_external(self) -> bool:
        """Checks if this collation is based on an external definition (e.g., ICU).

        Determines this by checking if `RDB$BASE_COLLATION_NAME` exists but
        does not correspond to another collation defined within the database schema.

        Returns:
            `True` if based on an external collation, `False` otherwise.
        """
        return self._attributes['RDB$BASE_COLLATION_NAME'] and not self.base_collation
    @property
    def id(self) -> int:
        """The unique numeric ID (`RDB$COLLATION_ID`) assigned to this collation within its character set."""
        return self._attributes['RDB$COLLATION_ID']
    @property
    def character_set(self) -> CharacterSet:
        """The `.CharacterSet` object this collation belongs to."""
        return self.schema.get_charset_by_id(self._attributes['RDB$CHARACTER_SET_ID'])
    @property
    def base_collation(self) -> Collation | None:
        """The base `.Collation` object this collation derives from, if any.

        Returns `None` if this collation is a primary collation for its character set
        or if it's based on an external definition (check `is_based_on_external()`).
        """
        base_name = self._attributes['RDB$BASE_COLLATION_NAME']
        return self.schema.collations.get(base_name) if base_name else None
    @property
    def attributes(self) -> CollationFlag:
        """A `.CollationFlag` enum value representing the combined attributes
        (pad space, case/accent sensitivity) defined by `RDB$COLLATION_ATTRIBUTES`."""
        return CollationFlag(self._attributes['RDB$COLLATION_ATTRIBUTES'])
    @property
    def specific_attributes(self) -> str:
        """Locale string or other specific configuration used by the collation
        engine (e.g., for ICU collations), stored in `RDB$SPECIFIC_ATTRIBUTES`.
        Returns `None` if not applicable."""
        return self._attributes['RDB$SPECIFIC_ATTRIBUTES']
    @property
    def function_name(self) -> str:
        """Not currently used."""
        return self._attributes['RDB$FUNCTION_NAME']
    @property
    def security_class(self) -> str | None:
        """The security class name associated with this collation, if any (`RDB$SECURITY_CLASS`).
        Returns `None` if no specific security class is assigned."""
        return self._attributes.get('RDB$SECURITY_CLASS')
    @property
    def owner_name(self) -> str:
        """The user name of the collation's owner/creator (`RDB$OWNER_NAME`)."""
        return self._attributes.get('RDB$OWNER_NAME')

class CharacterSet(SchemaItem):
    """Represents a character set defined in the database.

    A character set defines how characters are encoded (represented as bytes)
    and provides a default collation for sorting and comparison if none is
    explicitly specified.

    Instances of this class map data primarily from the `RDB$CHARACTER_SETS`
    system table. They are typically accessed via `Schema.character_sets`.

    Supported SQL actions via `.get_sql_for()`:

    *   `alter` (keyword argument `collation`: `.Collation` instance or collation name):
        Sets the default collation for this character set.
    *   `comment`: Adds or removes a descriptive comment for the character set.

    Arguments:
        schema: The parent `.Schema` instance.
        attributes: Raw data dictionary fetched from the `RDB$CHARACTER_SETS` row.
    """
    def __init__(self, schema: Schema, attributes: dict[str, Any]):
        super().__init__(schema, attributes)
        self._type_code.append(ObjectType.CHARACTER_SET)
        self._strip_attribute('RDB$CHARACTER_SET_NAME')
        self._strip_attribute('RDB$DEFAULT_COLLATE_NAME')
        self._strip_attribute('RDB$SECURITY_CLASS')
        self._strip_attribute('RDB$OWNER_NAME')
        self._actions.extend(['alter', 'comment'])
        self.__collations: DataList= None
    def _get_alter_sql(self, **params) -> str:
        """Generates the SQL command to ALTER this character set.

        Currently only supports setting the default collation.

        Arguments:
            **params: Accepts one keyword argument:

                      *   `collation` (Collation | str): The `.Collation` object or the
                          string name of the collation to set as the default for this
                          character set. **Required**.

        Returns:
            The `ALTER CHARACTER SET` SQL string.

        Raises:
            ValueError: If the required `collation` parameter is missing or if
                        unexpected parameters are passed.
        """
        self._check_params(params, ['collation'])
        collation = params.get('collation')
        if collation:
            return f'ALTER CHARACTER SET {self.get_quoted_name()} SET DEFAULT COLLATION ' \
                   f'{collation.get_quoted_name() if isinstance(collation, Collation) else collation}'
        raise ValueError("Missing required parameter: 'collation'.")
    def _get_comment_sql(self, **params) -> str:
        """Generates the SQL command to add or remove a COMMENT ON this character set.

        Arguments:
            **params: Accepts no parameters.

        Returns:
            The `COMMENT ON CHARACTER SET` SQL string. Sets comment to `NULL` if
            `self.description` is None, otherwise uses the description text
            with proper escaping.

        Raises:
            ValueError: If unexpected parameters are passed.
        """
        comment = 'NULL' if self.description is None \
            else f"'{escape_single_quotes(self.description)}'"
        return f'COMMENT ON CHARACTER SET {self.get_quoted_name()} IS {comment}'
    def _get_name(self) -> str:
        """Returns the character set name (`RDB$CHARACTER_SET_NAME`)."""
        return self._attributes['RDB$CHARACTER_SET_NAME']
    def get_collation_by_id(self, id_: int) -> Collation | None:
        """Retrieves a specific `.Collation` belonging to this character set by its ID.

        Searches the cached `collations` associated with this character set.

        Arguments:
            id_: The numeric ID (`RDB$COLLATION_ID`) of the collation to find.

        Returns:
            The matching `.Collation` object, or `None` if no collation with that ID
            exists within this character set (or if collations haven't been loaded).
        """
        return self.collations.find(lambda item: item.id == id_)
    @property
    def id(self) -> int:
        """The unique numeric ID (`RDB$CHARACTER_SET_ID`) assigned to this character set."""
        return self._attributes['RDB$CHARACTER_SET_ID']
    @property
    def bytes_per_character(self) -> int:
        """The maximum number of bytes required to store a single character in this set
        (`RDB$BYTES_PER_CHARACTER`)."""
        return self._attributes['RDB$BYTES_PER_CHARACTER']
    @property
    def default_collation(self) -> Collation:
        """The default `.Collation` object associated with this character set.

        Identified by `RDB$DEFAULT_COLLATE_NAME`. Returns `None` if the default
        collation cannot be found (which would indicate a schema inconsistency).
        """
        return self.collations.get(self._attributes['RDB$DEFAULT_COLLATE_NAME'])
    @property
    def collations(self) -> DataList[Collation]:
        """A lazily-loaded `.DataList` of all `.Collation` objects associated with this character set."""
        if self.__collations is None:
            self.__collations = self.schema.collations.extract(lambda i:
                                                               i._attributes['RDB$CHARACTER_SET_ID'] == self.id,
                                                               copy=True)
            self.__collations.freeze()
        return self.__collations
    @property
    def security_class(self) -> str | None:
        """The security class name associated with this character set, if any (`RDB$SECURITY_CLASS`).
        Returns `None` if no specific security class is assigned."""
        return self._attributes.get('RDB$SECURITY_CLASS')
    @property
    def owner_name(self) -> str:
        """The user name of the character set's owner/creator (`RDB$OWNER_NAME`)."""
        return self._attributes.get('RDB$OWNER_NAME')

class DatabaseException(SchemaItem):
    """Represents a named database exception, used for raising custom errors in PSQL.

    Database exceptions provide a way to signal specific error conditions from stored
    procedures or triggers using a symbolic name and an associated message text.

    Instances of this class map data primarily from the `RDB$EXCEPTIONS` system table.
    They are typically accessed via `Schema.exceptions`.

    Supported SQL actions via `.get_sql_for()`:

    *   User-defined exceptions:

        *   `create`: Creates the exception with its message.
        *   `recreate`: Recreates the exception (drops if exists, then creates).
        *   `alter` (keyword argument `message`: str): Changes the message text
            associated with the exception. **Required**.
        *   `create_or_alter`: Creates the exception or alters it if it already exists.
        *   `drop`: Removes the exception from the database.
        *   `comment`: Adds or removes a descriptive comment for the exception.

    *   System exceptions:

        *   `comment`: Adds or removes a descriptive comment.

    Arguments:
        schema: The parent `.Schema` instance.
        attributes: Raw data dictionary fetched from the `RDB$EXCEPTIONS` row.
    """
    def __init__(self, schema: Schema, attributes: dict[str, Any]):
        super().__init__(schema, attributes)
        self._type_code.append(ObjectType.EXCEPTION)
        self._strip_attribute('RDB$EXCEPTION_NAME')
        self._strip_attribute('RDB$SECURITY_CLASS')
        self._strip_attribute('RDB$OWNER_NAME')
        self._actions.append('comment')
        if not self.is_sys_object():
            self._actions.extend(['create', 'recreate', 'alter', 'create_or_alter', 'drop'])
    def _get_create_sql(self, **params) -> str:
        """Generates the SQL command to CREATE this exception.

        Arguments:
            **params: Accepts no parameters.

        Returns:
            The `CREATE EXCEPTION` SQL string, including the message text
            with single quotes properly escaped.

        Raises:
            ValueError: If unexpected parameters are passed.
        """
        self._check_params(params, [])
        return f"CREATE EXCEPTION {self.get_quoted_name()} '{escape_single_quotes(self.message)}'"
    def _get_alter_sql(self, **params) -> str:
        """Generates the SQL command to ALTER this exception's message.

        Arguments:
            **params: Accepts one keyword argument:

                      *   `message` (str): The new message text for the exception. **Required**.

        Returns:
            The `ALTER EXCEPTION` SQL string with the new message text,
            properly escaped.

        Raises:
            ValueError: If the required `message` parameter is missing, is not a string,
                        or if unexpected parameters are passed.
        """
        self._check_params(params, ['message'])
        message = params.get('message')
        if message:
            return f"ALTER EXCEPTION {self.get_quoted_name()} '{escape_single_quotes(message)}'"
        raise ValueError("Missing required parameter: 'message'.")
    def _get_drop_sql(self, **params) -> str:
        """Generates the SQL command to DROP this exception.

        Arguments:
            **params: Accepts no parameters.

        Returns:
            The `DROP EXCEPTION` SQL string.

        Raises:
            ValueError: If unexpected parameters are passed.
        """
        self._check_params(params, [])
        return f'DROP EXCEPTION {self.get_quoted_name()}'
    def _get_comment_sql(self, **params) -> str:
        """Generates the SQL command to add or remove a COMMENT ON this exception.

        Arguments:
            **params: Accepts no parameters.

        Returns:
            The `COMMENT ON EXCEPTION` SQL string. Sets comment to `NULL` if
            `self.description` is None, otherwise uses the description text
            with proper escaping.

        Raises:
            ValueError: If unexpected parameters are passed.
        """
        comment = 'NULL' if self.description is None \
            else f"'{escape_single_quotes(self.description)}'"
        return f'COMMENT ON EXCEPTION {self.get_quoted_name()} IS {comment}'
    def _get_name(self) -> str:
        """Returns the exception name (`RDB$EXCEPTION_NAME`)."""
        return self._attributes['RDB$EXCEPTION_NAME']
    @property
    def id(self) -> int:
        """The system-assigned unique numeric ID (`RDB$EXCEPTION_NUMBER`) for the exception."""
        return self._attributes['RDB$EXCEPTION_NUMBER']
    @property
    def message(self) -> str:
        """The custom message text associated with the exception (`RDB$MESSAGE`)."""
        return self._attributes['RDB$MESSAGE']
    @property
    def security_class(self) -> str | None:
        """The security class name associated with this exception, if any (`RDB$SECURITY_CLASS`).
        Returns `None` if no specific security class is assigned."""
        return self._attributes.get('RDB$SECURITY_CLASS')
    @property
    def owner_name(self) -> str:
        """The user name of the exception's owner/creator (`RDB$OWNER_NAME`)."""
        return self._attributes.get('RDB$OWNER_NAME')

class Sequence(SchemaItem):
    """Represents a database sequence (historically called generator).

    Sequences are used to generate unique, sequential numeric values, often
    employed for primary key generation or identity columns.

    Instances of this class map data primarily from the `RDB$GENERATORS` system table.
    They are typically accessed via `Schema.generators`, `Schema.sys_generators`, or
    `Schema.all_generators`. The current value is retrieved dynamically using the
    `GEN_ID()` function.

    The SQL keyword used (`SEQUENCE` or `GENERATOR`) in generated DDL depends on
    the `Schema.opt_generator_keyword` setting.

    Supported SQL actions via `.get_sql_for()`:

    *   User-defined sequences:

        *   `create` (optional keyword args: `value`: int, `increment`: int):
            Creates the sequence, optionally setting `START WITH` and `INCREMENT BY`.
        *   `alter` (optional keyword args: `value`: int, `increment`: int):
            Alters the sequence, optionally setting `RESTART WITH` and/or `INCREMENT BY`.
            At least one argument must be provided.
        *   `drop`: Removes the sequence from the database.
        *   `comment`: Adds or removes a descriptive comment for the sequence.

    *   System sequences (e.g., for IDENTITY columns):

        *   `comment`: Adds or removes a descriptive comment.

    Arguments:
        schema: The parent `.Schema` instance.
        attributes: Raw data dictionary fetched from the `RDB$GENERATORS` row.
    """
    def __init__(self, schema: Schema, attributes: dict[str, Any]):
        super().__init__(schema, attributes)
        self._type_code.append(ObjectType.GENERATOR)
        self._strip_attribute('RDB$GENERATOR_NAME')
        self._strip_attribute('RDB$SECURITY_CLASS')
        self._strip_attribute('RDB$OWNER_NAME')
        self._actions.append('comment')
        if not self.is_sys_object():
            self._actions.extend(['create', 'alter', 'drop'])
    def _get_create_sql(self, **params) -> str:
        """Generates the SQL command to CREATE this sequence/generator.

        Uses the keyword specified by `Schema.opt_generator_keyword`.

        Arguments:
            **params: Accepts optional keyword arguments:

                      * `value` (int): The initial value (`START WITH`).
                      * `increment` (int): The increment step (`INCREMENT BY`).

        Returns:
            The `CREATE SEQUENCE` or `CREATE GENERATOR` SQL string.

        Raises:
            ValueError: If unexpected parameters are passed.
        """
        self._check_params(params, ['value', 'increment'])
        value = params.get('value')
        inc = params.get('increment')
        cmd = f'CREATE {self.schema.opt_generator_keyword} {self.get_quoted_name()} ' \
              f'{f"START WITH {value}" if value else ""} ' \
              f'{f"INCREMENT BY {inc}" if inc else ""}'
        return cmd.strip()
    def _get_alter_sql(self, **params) -> str:
        """Generates the SQL command to ALTER this sequence/generator.

        Uses the keyword specified by `Schema.opt_generator_keyword`.

        Arguments:
            **params: Accepts optional keyword arguments:

                      * `value` (int): The value to restart the sequence with (`RESTART WITH`).
                      * `increment` (int): The new increment step (`INCREMENT BY`).

                      At least one of `value` or `increment` must be provided.

        Returns:
            The `ALTER SEQUENCE` or `ALTER GENERATOR` SQL string.

        Raises:
            ValueError: If neither `value` nor `increment` is provided, or if
                        unexpected parameters are passed.
        """
        self._check_params(params, ['value', 'increment'])
        value = params.get('value')
        inc = params.get('increment')
        cmd = f'ALTER {self.schema.opt_generator_keyword} {self.get_quoted_name()} ' \
              f'{f"RESTART WITH {value}" if isinstance(value,int) else ""} ' \
              f'{f"INCREMENT BY {inc}" if inc else ""}'
        return cmd.strip()
    def _get_drop_sql(self, **params) -> str:
        """Generates the SQL command to DROP this sequence/generator.

        Uses the keyword specified by `Schema.opt_generator_keyword`.

        Arguments:
            **params: Accepts no parameters.

        Returns:
            The `DROP SEQUENCE` or `DROP GENERATOR` SQL string.

        Raises:
            ValueError: If unexpected parameters are passed.
        """
        self._check_params(params, [])
        return f'DROP {self.schema.opt_generator_keyword} {self.get_quoted_name()}'
    def _get_comment_sql(self, **params) -> str:
        """Generates the SQL command to add or remove a COMMENT ON this sequence/generator.

        Uses the keyword specified by `Schema.opt_generator_keyword`.

        Arguments:
            **params: Accepts no parameters.

        Returns:
            The `COMMENT ON SEQUENCE` or `COMMENT ON GENERATOR` SQL string. Sets comment
            to `NULL` if `self.description` is None, otherwise uses the
            description text with proper escaping.

        Raises:
            ValueError: If unexpected parameters are passed.
        """
        comment = 'NULL' if self.description is None \
            else f"'{escape_single_quotes(self.description)}'"
        return f'COMMENT ON {self.schema.opt_generator_keyword} {self.get_quoted_name()} IS {comment}'
    def _get_name(self) -> str:
        """Returns the sequence/generator name (`RDB$GENERATOR_NAME`)."""
        return self._attributes['RDB$GENERATOR_NAME']
    def is_identity(self) -> bool:
        """Checks if this sequence is system-generated for an IDENTITY column.

        Determined by checking if `RDB$SYSTEM_FLAG` has the specific value 6.

        Returns:
            `True` if it's an identity sequence, `False` otherwise.
        """
        return self._attributes['RDB$SYSTEM_FLAG'] == 6
    @property
    def id(self) -> int:
        """The internal numeric ID (`RDB$GENERATOR_ID`) assigned to the sequence/generator."""
        return self._attributes['RDB$GENERATOR_ID']
    @property
    def value(self) -> int:
        """The current value of the sequence.

        .. important::
            Accessing this property executes `SELECT GEN_ID(name, 0) FROM RDB$DATABASE`
            against the database to retrieve the current value. It does **not**
            increment the sequence.

        Returns:
            The current integer value of the sequence.

        Raises:
            Error: If the schema is closed or the query fails.
        """
        return self.schema._select_row(f'select GEN_ID({self.get_quoted_name()},0) from RDB$DATABASE')['GEN_ID']
    @property
    def security_class(self) -> str | None:
        """The security class name associated with this sequence, if any (`RDB$SECURITY_CLASS`).
        Returns `None` if no specific security class is assigned."""
        return self._attributes.get('RDB$SECURITY_CLASS')
    @property
    def owner_name(self) -> str:
        """The user name of the sequence's owner/creator (`RDB$OWNER_NAME`)."""
        return self._attributes.get('RDB$OWNER_NAME')
    @property
    def inital_value(self) -> int | None:
        """The initial value (`START WITH`) defined for the sequence (`RDB$INITIAL_VALUE`).
        Returns `None` if not explicitly set (defaults may apply based on DB version).

        .. versionadded:: 1.4.0
           Support for reading this attribute (requires Firebird 4.0+). Older versions
           might return `None` even if a start value was conceptually set.
        """
        return self._attributes.get('RDB$INITIAL_VALUE')
    @property
    def increment(self) -> int:
        """The increment step (`INCREMENT BY`) defined for the sequence (`RDB$GENERATOR_INCREMENT`).
        Returns `None` if not explicitly set (defaults to 1).

        .. versionadded:: 1.4.0
           Support for reading this attribute (requires Firebird 4.0+). Older versions
           might return `None` even if an increment was conceptually set.
        """
        return self._attributes.get('RDB$GENERATOR_INCREMENT')

class TableColumn(SchemaItem):
    """Represents a column within a database table (`.Table`).

    This class holds metadata about a table column, such as its name, data type
    (derived from its underlying `.Domain`), nullability, default value,
    collation, position, and whether it's computed or an identity column.

    Instances map data primarily from the `RDB$RELATION_FIELDS` system table,
    linking to `RDB$FIELDS` via `RDB$FIELD_SOURCE` for domain/type information.
    They are typically accessed via the `.Table.columns` property.

    Supported SQL actions via `.get_sql_for()`:

    *   User table columns:

        *   `drop`: Generates `ALTER TABLE ... DROP COLUMN ...`.
        *   `comment`: Generates `COMMENT ON COLUMN ... IS ...`.
        *   `alter` (keyword args): Modifies the column definition. Only one
            type of alteration can be performed per call:
        *   `name` (str): Renames the column (`ALTER ... TO ...`).
        *   `position` (int): Changes the column's ordinal position.
        *   `datatype` (str): Changes the column's data type (`ALTER ... TYPE ...`).
            Cannot be used to change between computed/persistent.
        *   `expression` (str): Changes the `COMPUTED BY` expression.
            Requires the column to already be computed.
        *   `restart` (int | None): Restarts the sequence associated with an
            IDENTITY column. Provide an integer for `RESTART WITH value`,
            or `None` for `RESTART` (uses sequence's next value). Only
            applicable to identity columns.

    *   System table columns:

        *   `comment`: Adds or removes a descriptive comment.

    Arguments:
        schema: The parent `.Schema` instance.
        table: The parent `.Table` object this column belongs to.
        attributes: Raw data dictionary fetched from the `RDB$RELATION_FIELDS` row.
    """
    def __init__(self, schema: Schema, table: Table, attributes: dict[str, Any]):
        super().__init__(schema, attributes)
        self._type_code.extend([ObjectType.DOMAIN, ObjectType.COLUMN])
        self.__table = weakref.proxy(table)
        self._strip_attribute('RDB$FIELD_NAME')
        self._strip_attribute('RDB$RELATION_NAME')
        self._strip_attribute('RDB$FIELD_SOURCE')
        self._strip_attribute('RDB$SECURITY_CLASS')
        self._strip_attribute('RDB$GENERATOR_NAME')
        self._actions.append('comment')
        if not self.is_sys_object():
            self._actions.extend(['alter', 'drop'])
        self.__privileges: DataList = None
    def _get_alter_sql(self, **params) -> str:
        """Generates the SQL command to ALTER this table column.

        Only one type of alteration (rename, reposition, change type/expression,
        restart identity) can be performed per call.

        Arguments:
            **params: Accepts one of the following optional keyword arguments:

                      *   `name` (str): The new name for the column.
                      *   `position` (int): The new 1-based ordinal position for the column.
                      *   `datatype` (str): The new SQL data type definition (e.g., 'VARCHAR(100)',
                          'INTEGER'). Cannot be used if `expression` is also provided.
                      *   `expression` (str): The new `COMPUTED BY (...)` expression. Cannot be
                          used if `datatype` is also provided. Only applicable to computed columns.
                      *   `restart` (int | None): Restarts the identity sequence. Provide an integer
                          value for `WITH <value>` or `None` to restart without specifying a value.
                          Only applicable to identity columns.

        Returns:
            The `ALTER TABLE ... ALTER COLUMN ...` SQL string.

        Raises:
            ValueError: If multiple alteration types are specified, if attempting
                        invalid alterations (e.g., changing computed to persistent),
                        if required parameters are missing, or if unexpected parameters
                        are passed.
        """
        self._check_params(params, ['expression', 'datatype', 'name', 'position', 'restart'])
        new_expr = params.get('expression')
        new_type = params.get('datatype')
        new_name = params.get('name')
        new_position = params.get('position')
        if new_expr and not self.is_computed():
            raise ValueError("Change from persistent column to computed is not allowed.")
        if self.is_computed() and (new_type and not new_expr):
            raise ValueError("Change from computed column to persistent is not allowed.")
        sql = f'ALTER TABLE {self.table.get_quoted_name()} ALTER COLUMN {self.get_quoted_name()}'
        if new_name:
            return f'{sql} TO {self._get_quoted_ident(new_name)}'
        if new_position:
            return f'{sql} POSITION {new_position}'
        if new_type or new_expr:
            result = sql
            if new_type:
                result += f' TYPE {new_type}'
            if new_expr:
                result += f' COMPUTED BY {new_expr}'
            return result
        if 'restart' in params:
            restart = params.get('restart')
            sql += ' RESTART'
            if restart is not None:
                sql += f' WITH {restart}'
            return sql
        raise ValueError("Parameter required.")
    def _get_drop_sql(self, **params) -> str:
        """Generates the SQL command to DROP this table column.

        Arguments:
            **params: Accepts no parameters.

        Returns:
            The `ALTER TABLE ... DROP COLUMN ...` SQL string.

        Raises:
            ValueError: If unexpected parameters are passed.
        """
        self._check_params(params, [])
        return f'ALTER TABLE {self.table.get_quoted_name()} DROP {self.get_quoted_name()}'
    def _get_comment_sql(self, **params) -> str:
        """Generates the SQL command to add or remove a COMMENT ON this table column.

        Arguments:
            **params: Accepts no parameters.

        Returns:
            The `COMMENT ON COLUMN ... IS ...` SQL string. Sets comment to `NULL` if
            `self.description` is None, otherwise uses the description text
            with proper escaping.

        Raises:
            ValueError: If unexpected parameters are passed.
        """
        comment = 'NULL' if self.description is None \
            else f"'{escape_single_quotes(self.description)}'"
        return f'COMMENT ON COLUMN {self.table.get_quoted_name()}.{self.get_quoted_name()} IS {comment}'
    def _get_name(self) -> str:
        """Returns the column name (`RDB$FIELD_NAME`)."""
        return self._attributes['RDB$FIELD_NAME']
    def get_dependents(self) -> DataList[Dependency]:
        """Retrieves a list of database objects that depend on this specific column.

        Searches `.Schema.dependencies` matching the table name (`RDB$RELATION_NAME`),
        object type (0 for table), and this column's name (`RDB$FIELD_NAME`).

        Returns:
            A `.DataList` containing `.Dependency` objects where this column is
            part of the `depended_on` reference.
        """
        return self.schema.dependencies.extract(lambda d: d.depended_on_name == self._attributes['RDB$RELATION_NAME']
                and d.depended_on_type == 0 and d.field_name == self.name, copy=True)
    def get_dependencies(self) -> DataList[Dependency]:
        """Retrieves a list of database objects that this column depends on.

        This is typically relevant for computed columns, checking `.Schema.dependencies`
        where this column's table and name are the `dependent` reference.

        Returns:
            A `.DataList` containing `.Dependency` objects where this column is
            part of the `dependent` reference.
        """
        return self.schema.dependencies.extract(lambda d: d.dependent_name == self._attributes['RDB$RELATION_NAME']
                and d.dependent_type == 0 and d.field_name == self.name, copy=True)
    def get_computedby(self) -> str | None:
        """Returns the `COMPUTED BY (...)` expression string if this is a computed column.

        Returns:
            The expression string (without the `COMPUTED BY` keywords), or `None`
            if the column is not computed. Retrieves expression from the underlying domain.
        """
        return self.domain.expression
    def is_computed(self) -> bool:
        """Checks if this column is a computed column (`COMPUTED BY`).

        Returns:
            `True` if the underlying domain has a computed source, `False` otherwise.
        """
        return bool(self.domain.expression)
    def is_domain_based(self) -> bool:
        """Checks if this column is directly based on a user-defined domain.

        Returns:
            `True` if the underlying domain (`RDB$FIELD_SOURCE`) is not a system
            object, `False` otherwise (e.g., based on a system domain or defined inline).
        """
        return not self.domain.is_sys_object()
    def is_nullable(self) -> bool:
        """Checks if the column allows `NULL` values.

        Based on the `RDB$NULL_FLAG` attribute (0 = nullable, 1 = not nullable).

        Returns:
            `True` if the column can accept `NULL`, `False` otherwise.
        """
        return not self._attributes['RDB$NULL_FLAG']
    def is_writable(self) -> bool:
        """Checks if the column can be directly written to (e.g., not computed).

        Based on the `RDB$UPDATE_FLAG` attribute (1 = writable, 0 = not writable).

        Returns:
            `True` if the column is considered writable, `False` otherwise.
        """
        return bool(self._attributes['RDB$UPDATE_FLAG'])
    def is_identity(self) -> bool:
        """Checks if this column is an IDENTITY column (`GENERATED ... AS IDENTITY`).

        Determined by checking if `RDB$IDENTITY_TYPE` is not NULL.

        Returns:
            `True` if it's an identity column, `False` otherwise.
        """
        return self._attributes.get('RDB$IDENTITY_TYPE') is not None
    def has_default(self) -> bool:
        """Checks if the column has a `DEFAULT` value defined.

        Based on the presence of `RDB$DEFAULT_SOURCE`. Note that `.is_identity()`
        should be checked first, as identity columns may technically have a
        default source internally but are conceptually different.

        Returns:
            `True` if a default value source exists, `False` otherwise.
        """
        return bool(self._attributes.get('RDB$DEFAULT_SOURCE'))
    @property
    def id(self) -> int:
        """The internal numeric ID (`RDB$FIELD_ID`) assigned to the column within the table."""
        return self._attributes['RDB$FIELD_ID']
    @property
    def table(self) -> Table:
        """The parent `.Table` object this column belongs to."""
        return self.__table
    @property
    def domain(self) -> Domain:
        """The underlying `.Domain` object that defines this column's base data type
        and constraints (`RDB$FIELD_SOURCE`). May be a system domain or a user domain."""
        return self.schema.all_domains.get(self._attributes['RDB$FIELD_SOURCE'])
    @property
    def position(self) -> int:
        """The 0-based ordinal position (`RDB$FIELD_POSITION`) of the column within the table row."""
        return self._attributes['RDB$FIELD_POSITION']
    @property
    def security_class(self) -> str | None:
        """The security class name associated with this column, if any (`RDB$SECURITY_CLASS`).
        Returns `None` if no specific security class is assigned."""
        return self._attributes['RDB$SECURITY_CLASS']
    @property
    def default(self) -> str | None:
        """The `DEFAULT` value expression string defined for the column (`RDB$DEFAULT_SOURCE`).

        Returns the expression string (e.g., 'CURRENT_TIMESTAMP', "'ACTIVE'", '0')
        or `None` if no default is defined. The leading 'DEFAULT ' keyword is removed.
        """
        result = self._attributes.get('RDB$DEFAULT_SOURCE')
        if result:
            if result.upper().startswith('DEFAULT '):
                result = result[8:]
        return result
    @property
    def collation(self) -> Collation | None:
        """The specific `.Collation` object applied to this column (`RDB$COLLATION_ID`),
        if applicable (for character types).

        Returns `None` if the column type does not support collation or if the
        default collation of the underlying domain/character set is used.
        """
        return self.schema.get_collation_by_id(self.domain._attributes['RDB$CHARACTER_SET_ID'],
                                               self._attributes['RDB$COLLATION_ID'])
    @property
    def datatype(self) -> str:
        """A string representation of the column's complete SQL data type definition.

        This is derived from the underlying `.Domain`'s datatype property.
        Example: 'VARCHAR(100) CHARACTER SET UTF8 COLLATE UNICODE_CI'.
        """
        return self.domain.datatype
    @property
    def privileges(self) -> DataList[Privilege]:
        """A lazily-loaded `.DataList` of specific privileges (`SELECT`, `UPDATE`, `REFERENCES`)
        granted directly on this column."""
        return self.schema.privileges.extract(lambda p: (p.subject_name == self.table.name and
                                                        p.field_name == self.name and
                                                        p.subject_type in self.table._type_code),
                                                     copy = True)
    @property
    def generator(self) -> Sequence | None:
        """The `.Sequence` (generator) associated with this column if it's an
        IDENTITY column (`RDB$GENERATOR_NAME`).

        Returns:
            The related `.Sequence` object, or `None` if this is not an identity column
            or the sequence cannot be found.
        """
        return self.schema.all_generators.get(self._attributes.get('RDB$GENERATOR_NAME'))
    @property
    def identity_type(self) -> int | None:
        """The type of IDENTITY generation (`ALWAYS` or `BY DEFAULT`) specified for the column.

        Returns:
            An `.IdentityType` enum member if this is an identity column (`RDB$IDENTITY_TYPE`
            is not NULL), otherwise `None`.
        """
        return self._attributes.get('RDB$IDENTITY_TYPE')

class Index(SchemaItem):
    """Represents a database index used to speed up data retrieval or enforce constraints.

    Indexes can be defined on one or more columns (`segment`-based) or based on an
    expression (`expression`-based). They can be unique or non-unique, ascending or
    descending, and active or inactive. Indexes are also used internally to enforce
    PRIMARY KEY, UNIQUE, and FOREIGN KEY constraints.

    Instances of this class map data primarily from the `RDB$INDICES` and
    `RDB$INDEX_SEGMENTS` system tables. They are typically accessed via
    `.Schema.indices`, `.Schema.sys_indices`, `.Schema.all_indices`, or `.Table.indices`.

    Supported SQL actions via `get_sql_for()`:

    *   User-defined indexes:

        *   `create`: Creates the index (UNIQUE, ASC/DESC, on columns or COMPUTED BY).
        *   `activate`: Activates an inactive index (`ALTER INDEX ... ACTIVE`).
        *   `deactivate`: Deactivates an active index (`ALTER INDEX ... INACTIVE`).
        *   `recompute`: Requests recalculation of index statistics (`SET STATISTICS INDEX ...`).
        *   `drop`: Removes the index from the database.
        *   `comment`: Adds or removes a descriptive comment for the index.

    *   System indexes (used for constraints):

        *   `activate`: Activates the index.
        *   `recompute`: Recalculates statistics.
        *   `comment`: Adds or removes a comment.
        *   Note: System indexes usually cannot be dropped directly; drop the constraint instead.

    Arguments:
        schema: The parent `.Schema` instance.
        attributes: Raw data dictionary fetched from the `RDB$INDICES` row.
    """
    def __init__(self, schema: Schema, attributes: dict[str, Any]):
        super().__init__(schema, attributes)
        self._type_code.extend([ObjectType.INDEX_EXPR, ObjectType.INDEX])
        self.__segment_names = None
        self.__segment_statistics = None
        self._strip_attribute('RDB$INDEX_NAME')
        self._strip_attribute('RDB$RELATION_NAME')
        self._strip_attribute('RDB$FOREIGN_KEY')
        self._actions.extend(['activate', 'recompute', 'comment'])
        if not self.is_sys_object():
            self._actions.extend(['create', 'deactivate', 'drop'])
    def _get_create_sql(self, **params) -> str:
        """Generates the SQL command to CREATE this index.

        Handles UNIQUE, ASCENDING/DESCENDING attributes, and segment lists
        or COMPUTED BY expressions.

        Arguments:
            **params: Accepts no parameters.

        Returns:
            The `CREATE INDEX` SQL string.

        Raises:
            ValueError: If unexpected parameters are passed.
        """
        self._check_params(params, [])
        return f"CREATE {'UNIQUE ' if self.is_unique() else ''}{self.index_type.value} " \
               f"INDEX {self.get_quoted_name()} ON {self.table.get_quoted_name()} " \
               f"{f'COMPUTED BY {self.expression}' if self.is_expression() else '(%s)' % ','.join(self.segment_names)}"
    def _get_activate_sql(self, **params) -> str:
        """Generates the SQL command to ACTIVATE this index.

        Arguments:
            **params: Accepts no parameters.

        Returns:
            The `ALTER INDEX ... ACTIVE` SQL string.

        Raises:
            ValueError: If unexpected parameters are passed.
        """
        self._check_params(params, [])
        return f'ALTER INDEX {self.get_quoted_name()} ACTIVE'
    def _get_deactivate_sql(self, **params) -> str:
        """Generates the SQL command to DEACTIVATE this index.

        Arguments:
            **params: Accepts no parameters.

        Returns:
            The `ALTER INDEX ... INACTIVE` SQL string.

        Raises:
            ValueError: If unexpected parameters are passed or if trying to
                        deactivate a system index enforcing a constraint.
                        (Note: DB might prevent this, this check is for clarity).
        """
        self._check_params(params, [])
        return f'ALTER INDEX {self.get_quoted_name()} INACTIVE'
    def _get_recompute_sql(self, **params) -> str:
        """Generates the SQL command to request recalculation of index statistics.

        Arguments:
            **params: Accepts no parameters.

        Returns:
            The `SET STATISTICS INDEX ...` SQL string.

        Raises:
            ValueError: If unexpected parameters are passed.
        """
        self._check_params(params, [])
        return f'SET STATISTICS INDEX {self.get_quoted_name()}'
    def _get_drop_sql(self, **params) -> str:
        """Generates the SQL command to DROP this index.

        Arguments:
            **params: Accepts no parameters.

        Returns:
            The `DROP INDEX ...` SQL string.

        Raises:
            ValueError: If unexpected parameters are passed or if trying to
                        drop a system index enforcing a constraint.
                        (Note: DB prevents this, this check is for clarity).
        """
        self._check_params(params, [])
        return f'DROP INDEX {self.get_quoted_name()}'
    def _get_comment_sql(self, **params) -> str:
        """Generates the SQL command to add or remove a COMMENT ON this index.

        Arguments:
            **params: Accepts no parameters.

        Returns:
            The `COMMENT ON INDEX ... IS ...` SQL string. Sets comment to `NULL` if
            `self.description` is None, otherwise uses the description text
            with proper escaping.

        Raises:
            ValueError: If unexpected parameters are passed.
        """
        comment = 'NULL' if self.description is None \
            else f"'{escape_single_quotes(self.description)}'"
        return f'COMMENT ON INDEX {self.get_quoted_name()} IS {comment}'
    def _get_name(self) -> str:
        """Returns the index name (`RDB$INDEX_NAME`)."""
        return self._attributes['RDB$INDEX_NAME']
    def is_sys_object(self) -> bool:
        """Checks if this index is a system-defined object.

        Considers both the `RDB$SYSTEM_FLAG` and whether the index enforces
        a constraint and has a system-generated name (like 'RDB$...').

        Returns:
            `True` if it's considered a system object, `False` otherwise.
        """
        return bool(self._attributes['RDB$SYSTEM_FLAG']
                    or (self.is_enforcer() and self.name.startswith('RDB$')))
    def is_expression(self) -> bool:
        """Checks if this is an expression-based index (`COMPUTED BY`).

        Determined by checking if `RDB$EXPRESSION_SOURCE` is not NULL.

        Returns:
            `True` if it's an expression index, `False` otherwise.
        """
        return bool(self._attributes.get('RDB$EXPRESSION_SOURCE'))
        #return not self.segments
    def is_unique(self) -> bool:
        """Checks if the index enforces uniqueness (`UNIQUE`).

        Based on the `RDB$UNIQUE_FLAG` attribute (1 = unique, 0 = non-unique).

        Returns:
            `True` if the index is unique, `False` otherwise.
        """
        return self._attributes['RDB$UNIQUE_FLAG'] == 1
    def is_inactive(self) -> bool:
        """Checks if the index is currently inactive (`INACTIVE`).

        Based on the `RDB$INDEX_INACTIVE` attribute (1 = inactive, 0 = active).

        Returns:
            `True` if the index is inactive, `False` otherwise.
        """
        return self._attributes['RDB$INDEX_INACTIVE'] == 1
    def is_enforcer(self) -> bool:
        """Checks if this index is used to enforce a constraint (PK, UK, FK).

        Determines this by checking if its name exists as a key in the
        schema's internal constraint-to-index map.

        Returns:
            `True` if the index enforces a constraint, `False` otherwise.
        """
        return self.name in self.schema._get_constraint_indices()
    @property
    def table(self) -> Table:
        """The `.Table` object this index is defined on (`RDB$RELATION_NAME`)."""
        return self.schema.all_tables.get(self._attributes['RDB$RELATION_NAME'])
    @property
    def id(self) -> int:
        """The internal numeric ID (`RDB$INDEX_ID`) assigned to the index."""
        return self._attributes['RDB$INDEX_ID']
    @property
    def index_type(self) -> IndexType:
        """The index ordering type (`.IndexType.ASCENDING` or `.IndexType.DESCENDING`).

        Based on `RDB$INDEX_TYPE` (NULL or 0 = ascending, 1 = descending).
        """
        return (IndexType.DESCENDING if self._attributes['RDB$INDEX_TYPE'] == 1
                else IndexType.ASCENDING)
    @property
    def partner_index(self) -> Index | None:
        """For a FOREIGN KEY index, the associated PRIMARY KEY or UNIQUE key `.Index`
        it references (`RDB$FOREIGN_KEY` contains the partner index name).

        Returns:
            The partner `.Index` object, or `None` if this is not a foreign key index
            or the partner index cannot be found.
        """
        return (self.schema.all_indices.get(pname) if (pname := self._attributes['RDB$FOREIGN_KEY'])
                else None)
    @property
    def expression(self) -> str | None:
        """The expression string for an expression-based index (`RDB$EXPRESSION_SOURCE`).

        Returns:
             The expression string (typically enclosed in parentheses), or `None`
             if this is a segment-based index.
        """
        return self._attributes['RDB$EXPRESSION_SOURCE']
    @property
    def statistics(self) -> float:
        """The latest calculated selectivity statistic for the index (`RDB$STATISTICS`).
        Lower values indicate higher selectivity. May be `None` if statistics haven't been computed."""
        return self._attributes['RDB$STATISTICS']
    @property
    def segment_names(self) -> list[str]:
        """A list of column names that form the segments of this index.

        Returns an empty list for expression-based indexes. Fetched lazily from
        `RDB$INDEX_SEGMENTS`.
        """
        if self.__segment_names is None:
            if self._attributes['RDB$SEGMENT_COUNT'] > 0:
                self.__segment_names = [r['RDB$FIELD_NAME'].strip() for r
                                        in self.schema._select("""select rdb$field_name
from rdb$index_segments where rdb$index_name = ? order by rdb$field_position""", (self.name,))]
            else:
                self.__segment_names = []
        return self.__segment_names
    @property
    def segment_statistics(self) -> list[float]:
        """A list of selectivity statistics for each corresponding segment in `segment_names`.

        Returns an empty list for expression-based indexes or if statistics are unavailable.
        Fetched lazily from `RDB$INDEX_SEGMENTS`.
        """
        if self.__segment_statistics is None:
            if self._attributes['RDB$SEGMENT_COUNT'] > 0:
                self.__segment_statistics = [r['RDB$STATISTICS'] for r
                                             in self.schema._select("""select RDB$STATISTICS
from rdb$index_segments where rdb$index_name = ? order by rdb$field_position""", (self.name,))]
            else:
                self.__segment_statistics = []
        return self.__segment_statistics
    @property
    def segments(self) -> DataList[TableColumn]:
        """A `.DataList` of the `.TableColumn` objects corresponding to the index segments.

        Returns an empty list for expression-based indexes. Uses `segment_names` to
        look up columns in the associated `Table`.
        """
        return DataList(self.table.columns.get(colname) for colname in self.segment_names)
    @property
    def constraint(self) -> Constraint | None:
        """The `.Constraint` object (PK, UK, FK) that this index enforces, if any.

        Returns `None` if the index does not enforce a constraint (i.e., it's purely
        for performance).
        """
        return self.schema.constraints.get(self.schema._get_constraint_indices().get(self.name))
    # Firebird 5
    @property
    def condition(self) -> str | None:
        """The partial index condition string (`RDB$CONDITION_SOURCE`), if defined.

        Returns the condition string (typically enclosed in parentheses), or `None`
        if this is not a partial index.

        .. versionadded:: 1.4.0
           Requires Firebird 5.0+. Older versions will return `None`.
        """
        return self._attributes['RDB$CONDITION_SOURCE']

class ViewColumn(SchemaItem):
    """Represents a column within a database view (`.View`).

    View columns derive their properties (like data type, nullability) from the
    underlying query's output columns, which might originate from base tables,
    other views, or procedure outputs.

    Instances primarily map data from `RDB$RELATION_FIELDS` where the relation
    type is VIEW. Information about the source column/expression is often limited
    compared to table columns. They are accessed via the `View.columns` property.

    Supported SQL actions via `get_sql_for()`:

    *   `comment`: Adds or removes a descriptive comment for the view column
        (`COMMENT ON COLUMN view_name.column_name IS ...`).

    Arguments:
        schema: The parent `.Schema` instance.
        view: The parent `.View` object this column belongs to.
        attributes: Raw data dictionary fetched from the `RDB$RELATION_FIELDS` row
                    (potentially joined with `RDB$VIEW_RELATIONS` for base info).
    """
    def __init__(self, schema: Schema, view: View, attributes: dict[str, Any]):
        super().__init__(schema, attributes)
        self._type_code.extend([ObjectType.DOMAIN, ObjectType.COLUMN])
        self.__view = weakref.proxy(view)
        self._strip_attribute('RDB$FIELD_NAME')
        self._strip_attribute('RDB$BASE_FIELD')
        self._strip_attribute('RDB$RELATION_NAME')
        self._strip_attribute('RDB$FIELD_SOURCE')
        self._strip_attribute('RDB$SECURITY_CLASS')
        self._strip_attribute('BASE_RELATION')
        self._actions.append('comment')
    def _get_comment_sql(self, **params) -> str:
        """Generates the SQL command to add or remove a COMMENT ON this view column.

        Arguments:
            **params: Accepts no parameters.

        Returns:
            The `COMMENT ON COLUMN view_name.column_name IS ...` SQL string. Sets
            comment to `NULL` if `self.description` is None, otherwise uses the
            description text with proper escaping.

        Raises:
            ValueError: If unexpected parameters are passed.
        """
        comment = 'NULL' if self.description is None \
            else f"'{escape_single_quotes(self.description)}'"
        return f'COMMENT ON COLUMN {self.view.get_quoted_name()}.{self.get_quoted_name()} IS {comment}'
    def _get_name(self) -> str:
        """Returns the view column name (`RDB$FIELD_NAME`)."""
        return self._attributes['RDB$FIELD_NAME']
    def get_dependents(self) -> DataList[Dependency]:
        """Retrieves a list of database objects that depend on this specific view column.

        Searches `.Schema.dependencies` matching the view name (`RDB$RELATION_NAME`),
        object type (1 for view), and this column's name (`RDB$FIELD_NAME`).

        Returns:
            A `.DataList` containing `.Dependency` objects where this view column is
            part of the `depended_on` reference.
        """
        return self.schema.dependencies.extract(lambda d: d.depended_on_name == self._attributes['RDB$RELATION_NAME']
                and d.depended_on_type == 1 and d.field_name == self.name, copy=True)
    def get_dependencies(self) -> DataList[Dependency]:
        """Retrieves a list of database objects that this view column depends on.

        Searches `.Schema.dependencies` where this view column's view name and
        column name are the `dependent` reference (dependent type is 1 for VIEW).

        Returns:
            A `.DataList` containing `.Dependency` objects where this view column is
            part of the `dependent` reference.
        """
        return self.schema.dependencies.extract(lambda d: d.dependent_name == self._attributes['RDB$RELATION_NAME']
                and d.dependent_type == 1 and d.field_name == self.name, copy=True)
    def is_nullable(self) -> bool:
        """Checks if the view column allows `NULL` values.

        Based on the `RDB$NULL_FLAG` attribute derived from the underlying source
        or view definition.

        Returns:
            `True` if the column can contain `NULL`, `False` otherwise.
        """
        return not self._attributes['RDB$NULL_FLAG']
    def is_writable(self) -> bool:
        """Checks if the view column is potentially updatable.

        Based on the `RDB$UPDATE_FLAG`. Note that view updatability also depends
        on the view definition itself (e.g., joins, aggregates). This flag indicates
        if the underlying source column *could* be updated *through* the view,
        assuming the view itself is updatable.

        Returns:
            `True` if the column source is marked as updatable via the view,
            `False` otherwise.
        """
        return bool(self._attributes['RDB$UPDATE_FLAG'])
    @property
    def base_field(self) -> TableColumn | ViewColumn | ProcedureParameter:
        """The original source column or parameter from the underlying base object.

        Identified via `RDB$BASE_FIELD` (source column name) and `BASE_RELATION`
        (source object name). It attempts to find the source in tables, views,
        or procedures.

        Returns:
            A `.TableColumn`, `.ViewColumn`, or `.ProcedureParameter` instance representing
            the ultimate source, or `None` if the source cannot be determined (e.g.,
            an expression without a direct base column).

        Raises:
            Error: If the base relation name exists but the corresponding schema object
                   (table/view/procedure) cannot be found.
        """
        bfield = self._attributes['RDB$BASE_FIELD']
        if bfield:
            brel = self._attributes['BASE_RELATION']
            if item := self.schema.all_tables.get(brel):
                return item.columns.get(bfield)
            if item := self.schema.all_views.get(brel):
                return item.columns.get(bfield)
            if item := self.schema.all_procedures.get(brel):
                return item.get_outparam(bfield)
            raise Error("Can't locate base relation.")
        return None
    @property
    def view(self) -> View:
        """The parent `.View` object this column belongs to."""
        return self.__view
    @property
    def domain(self) -> Domain:
        """The underlying `.Domain` object that defines this column's base data type
        and constraints (`RDB$FIELD_SOURCE`)."""
        return self.schema.all_domains.get(self._attributes['RDB$FIELD_SOURCE'])
    @property
    def position(self) -> int:
        """The 0-based ordinal position (`RDB$FIELD_POSITION`) of the column within the view's definition."""
        return self._attributes['RDB$FIELD_POSITION']
    @property
    def security_class(self) -> str | None:
        """The security class name associated with this view column, if any (`RDB$SECURITY_CLASS`).
        Returns `None` if no specific security class is assigned."""
        return self._attributes['RDB$SECURITY_CLASS']
    @property
    def collation(self) -> Collation | None:
        """The specific `.Collation` object applied to this view column (`RDB$COLLATION_ID`),
        if applicable (for character types) and different from the domain default.

        Returns `None` if the column type does not support collation or if the
        default collation of the underlying domain/character set is used.
        """
        return self.schema.get_collation_by_id(self.domain._attributes['RDB$CHARACTER_SET_ID'],
                                               self._attributes['RDB$COLLATION_ID'])
    @property
    def datatype(self) -> str:
        """A string representation of the column's SQL data type definition.

        Derived from the underlying `.Domain`'s datatype property.
        Example: 'VARCHAR(50) CHARACTER SET UTF8'.
        """
        return self.domain.datatype
    @property
    def privileges(self) -> DataList[Privilege]:
        """A lazily-loaded `.DataList` of privileges (`SELECT`, `UPDATE`, `REFERENCES`)
        granted specifically on this view column.

        .. note::

           In `RDB$USER_PRIVILEGES`, privileges on view columns are often logged
           with the subject type as TABLE (0), not VIEW (1). This property
           accounts for that when filtering.
        """
        # Views are logged as Tables in RDB$USER_PRIVILEGES
        return self.schema.privileges.extract(lambda p: (p.subject_name == self.view.name and
                                                         p.field_name == self.name and
                                                         p.subject_type == 0), copy=True)

class Domain(SchemaItem):
    """Represents an SQL Domain, a reusable definition for data types and constraints.

    Domains allow defining a base data type (e.g., `VARCHAR(50)`, `DECIMAL(18,4)`),
    along with optional attributes like:

    *   `NOT NULL` constraint
    *   `DEFAULT` value
    *   `CHECK` constraint (validation rule)
    *   Collation (for character types)
    *   Array dimensions

    Table columns and PSQL variables/parameters can then be declared based on a domain,
    inheriting its properties. This promotes consistency and simplifies schema management.

    Instances map data primarily from the `RDB$FIELDS` system table. They are
    accessed via `Schema.domains`, `Schema.sys_domains`, or `Schema.all_domains`.

    Supported SQL actions via `get_sql_for()`:

    *   User-defined domains:

        *   `create`: Creates the domain with its full definition.
        *   `drop`: Removes the domain from the database.
        *   `comment`: Adds or removes a descriptive comment for the domain.
        *   `alter` (keyword args): Modifies the domain definition. Only *one*
            type of alteration can be performed per call:

            *   `name` (str): Renames the domain (`ALTER DOMAIN ... TO ...`).
            *   `default` (str | None): Sets or drops the default value.
                Provide the default expression string, or `None`/empty string
                to `DROP DEFAULT`.
            *   `check` (str | None): Adds or drops the check constraint.
                Provide the `CHECK (...)` expression string (without the
                `CHECK` keyword itself), or `None`/empty string to
                `DROP CONSTRAINT`.
            *   `datatype` (str): Changes the base data type (`ALTER DOMAIN ... TYPE ...`).

    *   System domains:

        *   `comment`: Adds or removes a descriptive comment.

    Arguments:
        schema: The parent `.Schema` instance.
        attributes: Raw data dictionary fetched from the `RDB$FIELDS` row.
    """
    def __init__(self, schema: Schema, attributes: dict[str, Any]):
        super().__init__(schema, attributes)
        self._type_code.append(ObjectType.COLUMN)
        self._strip_attribute('RDB$FIELD_NAME')
        self._strip_attribute('RDB$SECURITY_CLASS')
        self._strip_attribute('RDB$OWNER_NAME')
        self._actions.append('comment')
        if not self.is_sys_object():
            self._actions.extend(['create', 'alter', 'drop'])
    def _get_create_sql(self, **params) -> str:
        """Generates the SQL command to CREATE this domain.

        Includes the base data type, default value, nullability, check constraint,
        and collation, if defined.

        Arguments:
            **params: Accepts no parameters.

        Returns:
            The `CREATE DOMAIN` SQL string.

        Raises:
            ValueError: If unexpected parameters are passed.
        """
        self._check_params(params, [])
        sql = f'CREATE DOMAIN {self.get_quoted_name()} AS {self.datatype}'
        if self.has_default():
            sql += f' DEFAULT {self.default}'
        if not self.is_nullable():
            sql += ' NOT NULL'
        if self.is_validated():
            sql += ' ' + self.validation
        if self._attributes['RDB$COLLATION_ID']:
            if self.character_set._attributes['RDB$DEFAULT_COLLATE_NAME'] != self.collation.name:
                sql += f' COLLATE {self.collation.get_quoted_name()}'
        return sql
    def _get_alter_sql(self, **params) -> str:
        """Generates the SQL command to ALTER this domain.

        Only one type of alteration (rename, change default, change check,
        change type) can be performed per call.

        Arguments:
            **params: Accepts one of the following optional keyword arguments:

                      * `name` (str): The new name for the domain.
                      * `default` (str | None): The new default value expression, or `None`/""
                        to drop the default.
                      * `check` (str | None): The new check constraint expression (inside the
                        parentheses), or `None`/"" to drop the check constraint.
                      * `datatype` (str): The new base SQL data type definition.

        Returns:
            The `ALTER DOMAIN` SQL string.

        Raises:
            ValueError: If multiple alteration types are specified, if required
                        parameters are missing, or if unexpected parameters are passed.
        """
        self._check_params(params, ['name', 'default', 'check', 'datatype'])
        new_name = params.get('name')
        new_default = params.get('default', '')
        new_constraint = params.get('check', '')
        new_type = params.get('datatype')
        sql = f'ALTER DOMAIN {self.get_quoted_name()}'
        if len(params) > 1:
            raise ValueError("Only one parameter allowed.")
        if new_name:
            return f'{sql} TO {self._get_quoted_ident(new_name)}'
        if new_default != '':
            return (f'{sql} SET DEFAULT {new_default}' if new_default
                    else f'{sql} DROP DEFAULT')
        if new_constraint != '':
            return (f'{sql} ADD CHECK ({new_constraint})' if new_constraint
                    else f'{sql} DROP CONSTRAINT')
        if new_type:
            return f'{sql} TYPE {new_type}'
        raise ValueError("Parameter required.")
    def _get_drop_sql(self, **params) -> str:
        """Generates the SQL command to DROP this domain.

        Arguments:
            **params: Accepts no parameters.

        Returns:
            The `DROP DOMAIN` SQL string.

        Raises:
            ValueError: If unexpected parameters are passed.
        """
        self._check_params(params, [])
        return f'DROP DOMAIN {self.get_quoted_name()}'
    def _get_comment_sql(self, **params) -> str:
        """Generates the SQL command to add or remove a COMMENT ON this domain.

        Arguments:
            **params: Accepts no parameters.

        Returns:
            The `COMMENT ON DOMAIN` SQL string. Sets comment to `NULL` if
            `self.description` is None, otherwise uses the description text
            with proper escaping.

        Raises:
            ValueError: If unexpected parameters are passed.
        """
        comment = 'NULL' if self.description is None \
            else f"'{escape_single_quotes(self.description)}'"
        return f'COMMENT ON DOMAIN {self.get_quoted_name()} IS {comment}'
    def _get_name(self) -> str:
        """Returns the domain name (`RDB$FIELD_NAME`)."""
        return self._attributes['RDB$FIELD_NAME']
    def is_sys_object(self) -> bool:
        """Checks if this domain is a system-defined object.

        Considers both the `RDB$SYSTEM_FLAG` and whether the name starts with 'RDB$'.

        Returns:
            `True` if it's considered a system object, `False` otherwise.
        """
        return (self._attributes['RDB$SYSTEM_FLAG'] == 1) or self.name.startswith('RDB$')
    def is_nullable(self) -> bool:
        """Checks if the domain allows `NULL` values (i.e., not defined with `NOT NULL`).

        Based on the `RDB$NULL_FLAG` attribute (0 = nullable, 1 = not nullable).

        Returns:
            `True` if the domain allows `NULL`, `False` otherwise.
        """
        return not self._attributes['RDB$NULL_FLAG']
    def is_computed(self) -> bool:
        """Checks if this domain represents a `COMPUTED BY` definition.

        Based on the presence of `RDB$COMPUTED_SOURCE`. Note: Domains themselves
        aren't directly computed, but columns based on them can inherit this if
        the domain definition was used implicitly for a computed column type.
        Generally, user-defined domains should not have this set.

        Returns:
            `True` if `RDB$COMPUTED_SOURCE` has a value, `False` otherwise.
        """
        return bool(self._attributes['RDB$COMPUTED_SOURCE'])
    def is_validated(self) -> bool:
        """Checks if the domain has a `CHECK` constraint defined.

        Based on the presence of `RDB$VALIDATION_SOURCE`.

        Returns:
            `True` if a check constraint source exists, `False` otherwise.
        """
        return bool(self._attributes['RDB$VALIDATION_SOURCE'])
    def is_array(self) -> bool:
        """Checks if the domain defines an array data type.

        Based on the presence of `RDB$DIMENSIONS`.

        Returns:
            `True` if the domain defines an array, `False` otherwise.
        """
        return bool(self._attributes['RDB$DIMENSIONS'])
    def has_default(self) -> bool:
        """Checks if the domain has a `DEFAULT` value defined.

        Based on the presence of `RDB$DEFAULT_SOURCE`.

        Returns:
            `True` if a default value source exists, `False` otherwise.
        """
        return bool(self._attributes['RDB$DEFAULT_SOURCE'])
    @property
    def expression(self) -> str | None:
        """The `COMPUTED BY (...)` expression string, if applicable (`RDB$COMPUTED_SOURCE`).
        Typically `None` for user-defined domains."""
        return self._attributes['RDB$COMPUTED_SOURCE']
    @property
    def validation(self) -> str | None:
        """The `CHECK (...)` constraint expression string (`RDB$VALIDATION_SOURCE`).

        Returns the expression part *inside* the parentheses, or `None` if no
        check constraint is defined.
        """
        return self._attributes['RDB$VALIDATION_SOURCE']
    @property
    def default(self) -> str | None:
        """The `DEFAULT` value expression string (`RDB$DEFAULT_SOURCE`).

        Returns the expression string (e.g., 'CURRENT_TIMESTAMP', "'ACTIVE'", '0')
        or `None` if no default is defined. The leading 'DEFAULT ' keyword is removed.
        """
        if result := self._attributes.get('RDB$DEFAULT_SOURCE'):
            if result.upper().startswith('DEFAULT '):
                result = result[8:]
        return result
    @property
    def length(self) -> int:
        """The defined length of the data type in bytes (`RDB$FIELD_LENGTH`).
        Applicable to types like `CHAR`, `VARCHAR`, `BLOB`. May be `None`."""
        return self._attributes['RDB$FIELD_LENGTH']
    @property
    def scale(self) -> int:
        """The scale (number of digits to the right of the decimal point) for
        `NUMERIC` or `DECIMAL` types (`RDB$FIELD_SCALE`). Stored as a negative number.
        Returns `None` for non-exact numeric types."""
        return self._attributes['RDB$FIELD_SCALE']
    @property
    def field_type(self) -> FieldType:
        """The base data type code (`.FieldType`) defined for the domain (`RDB$FIELD_TYPE`)."""
        return FieldType(self._attributes['RDB$FIELD_TYPE'])
    @property
    def sub_type(self) -> int | None:
        """The field sub-type code (`RDB$FIELD_SUB_TYPE`).

        Commonly used for `BLOB` subtypes (0=binary, 1=text) or `NUMERIC`/`DECIMAL`
        indication (1=numeric, 2=decimal) for exact numeric types. Returns the raw
        integer if not a standard `.FieldSubType` enum member, or `None`.
        """
        return self._attributes['RDB$FIELD_SUB_TYPE']
    @property
    def segment_length(self) -> int | None:
        """Suggested segment size for `BLOB` types (`RDB$SEGMENT_LENGTH`).
        Returns `None` for non-BLOB types."""
        return self._attributes['RDB$SEGMENT_LENGTH']
    @property
    def external_length(self) -> int:
        """Length of the field if mapped from an external table (`RDB$EXTERNAL_LENGTH`).
        Typically 0 or `None` for regular domains."""
        return self._attributes['RDB$EXTERNAL_LENGTH']
    @property
    def external_scale(self) -> int | None:
        """Scale of the field if mapped from an external table (`RDB$EXTERNAL_SCALE`).
        Typically 0 or `None`."""
        return self._attributes['RDB$EXTERNAL_SCALE']
    @property
    def external_type(self) -> FieldType | None:
        """Data type code (`.FieldType`) of the field if mapped from an external table
        (`RDB$EXTERNAL_TYPE`). Returns `None` otherwise."""
        if (value := self._attributes['RDB$EXTERNAL_TYPE']) is not None:
            return FieldType(value)
        return None
    @property
    def dimensions(self) -> list[tuple[int, int]]:
        """A list of dimension bounds for array domains.

        Each tuple in the list represents one dimension `(lower_bound, upper_bound)`.
        Returns `None` if the domain is not an array type (`RDB$DIMENSIONS` is NULL).
        Fetched lazily by querying `RDB$FIELD_DIMENSIONS`.
        """
        if self._attributes['RDB$DIMENSIONS']:
            return self.schema._get_field_dimensions(self)
        return []
    @property
    def character_length(self) -> int:
        """Length of character types (`CHAR`, `VARCHAR`) in characters, not bytes
        (`RDB$CHARACTER_LENGTH`). Returns `None` for non-character types."""
        return self._attributes['RDB$CHARACTER_LENGTH']
    @property
    def collation(self) -> Collation | None:
        """The specific `.Collation` object defined for the domain (`RDB$COLLATION_ID`),
        if applicable (for character/text types).

        Returns `None` if the domain type does not support collation or if the
        default collation of the character set is used.
        """
        return self.schema.get_collation_by_id(self._attributes['RDB$CHARACTER_SET_ID'],
                                               self._attributes['RDB$COLLATION_ID'])
    @property
    def character_set(self) -> CharacterSet | None:
        """The `.CharacterSet` object associated with the domain (`RDB$CHARACTER_SET_ID`),
        if applicable (for character/text types). Returns `None` otherwise."""
        return self.schema.get_charset_by_id(self._attributes['RDB$CHARACTER_SET_ID'])
    @property
    def precision(self) -> int | None:
        """The precision (total number of digits) for exact numeric types
        (`NUMERIC`, `DECIMAL`) or approximate types (`FLOAT`, `DOUBLE`)
        (`RDB$FIELD_PRECISION`). Returns `None` if not applicable."""
        return self._attributes['RDB$FIELD_PRECISION']
    @property
    def datatype(self) -> str:
        """A string representation of the domain's complete SQL data type definition.

        Combines the base type, length/precision/scale, character set/collation,
        and array dimensions into a standard SQL type string.
        Example: `DECIMAL(18, 4)`, `VARCHAR(100) CHARACTER SET UTF8 COLLATE UNICODE_CI`,
        `INTEGER [1:10]`.
        """
        l = []
        precision_known = False
        if self.field_type in (FieldType.SHORT, FieldType.LONG, FieldType.INT64):
            if self.precision is not None:
                if self.sub_type in (FieldSubType.NUMERIC, FieldSubType.DECIMAL):
                    l.append(f'{INTEGRAL_SUBTYPES[self.sub_type]}({self.precision}, {-self.scale})')
                    precision_known = True
        if not precision_known:
            if (self.field_type == FieldType.SHORT) and (self.scale < 0):
                l.append(f'NUMERIC(4, {-self.scale})')
            elif (self.field_type == FieldType.LONG) and (self.scale < 0):
                l.append(f'NUMERIC(9, {-self.scale})')
            elif (self.field_type == FieldType.DOUBLE) and (self.scale < 0):
                l.append(f'NUMERIC(15, {-self.scale})')
            else:
                l.append(COLUMN_TYPES[self.field_type])
        if self.field_type in (FieldType.TEXT, FieldType.VARYING                        ):
            l.append(f'({self.length if self.character_length is None else self.character_length})')
        if self._attributes['RDB$DIMENSIONS'] is not None:
            l.append("[%s]" % ', '.join(f'{u}' if l == 1 else f'{l}:{u}' for l, u in self.dimensions))
        if self.field_type == FieldType.BLOB:
            if self.sub_type >= 0 and self.sub_type <= len(self.schema.field_subtypes):
                l.append(f' SUB_TYPE {self.schema.field_subtypes[self.sub_type]}')
            else:
                l.append(f' SUB_TYPE {self.sub_type}')
            l.append(f' SEGMENT SIZE {self.segment_length}')
        if self.field_type in (FieldType.TEXT, FieldType.VARYING                        , FieldType.BLOB):
            if (self._attributes['RDB$CHARACTER_SET_ID'] is not None
                and (self.character_set.name != self.schema.default_character_set.name)
                or self._attributes['RDB$COLLATION_ID']):
                if self._attributes['RDB$CHARACTER_SET_ID'] is not None:
                    l.append(f' CHARACTER SET {self.character_set.name}')
        return ''.join(l)
    @property
    def security_class(self) -> str | None:
        """The security class name associated with this domain, if any (`RDB$SECURITY_CLASS`).
        Returns `None` if no specific security class is assigned."""
        return self._attributes.get('RDB$SECURITY_CLASS')
    @property
    def owner_name(self) -> str:
        """The user name of the domain's owner/creator (`RDB$OWNER_NAME`)."""
        return self._attributes.get('RDB$OWNER_NAME')

class Dependency(SchemaItem):
    """Represents a dependency relationship between two database schema objects.

    This class maps a single row from the `RDB$DEPENDENCIES` system table,
    indicating that one object (`dependent`) relies on another object (`depended_on`).
    Understanding these dependencies is crucial for determining the correct order
    for DDL operations (e.g., dropping or altering objects).

    Instances of this class are typically accessed via `Schema.dependencies` or by
    calling `get_dependents()` or `get_dependencies()` on other `.SchemaItem` objects.

    This class itself does not support any direct SQL actions via `get_sql_for()`.

    Arguments:
        schema: The parent `.Schema` instance.
        attributes: Raw data dictionary fetched from the `RDB$DEPENDENCIES` row.
    """
    def __init__(self, schema: Schema, attributes: dict[str, Any]):
        super().__init__(schema, attributes)
        self._strip_attribute('RDB$DEPENDENT_NAME')
        self._strip_attribute('RDB$DEPENDED_ON_NAME')
        self._strip_attribute('RDB$FIELD_NAME')
        self._strip_attribute('RDB$PACKAGE_NAME')
    def _get_name(self) -> str | None:
        """Returns a descriptive string representation, not a standard object name.

        Dependencies don't have a unique name in the SQL sense. This returns
        `None` as per the base class expectation for unnamed items.
        """
        return None # Dependencies don't have a SQL name
    def is_sys_object(self) -> bool:
        """Indicates that dependency entries themselves are considered system metadata.

        Returns:
            `True` always.
        """
        return True
    def get_dependents(self) -> DataList:
        """Dependencies do not have further dependents.

        Returns:
            An empty `.DataList`.
        """
        return DataList()
    def get_dependencies(self) -> DataList:
        """Dependencies represent a relationship and do not have dependencies themselves.

        Returns:
            An empty `.DataList`.
        """
        return DataList()
    def is_packaged(self) -> bool:
        """Checks if this dependency involves an object defined within a PSQL package.

        Based on the presence of `RDB$PACKAGE_NAME`. This usually means the
        `dependent` object is inside the package.

        Returns:
            `True` if `RDB$PACKAGE_NAME` has a value, `False` otherwise.
        """
        return bool(self._attributes.get('RDB$PACKAGE_NAME'))
    @property
    def dependent(self) -> SchemaItem:
        """The database object that has the dependency (the one that relies on something else).

        Resolves the object based on `RDB$DEPENDENT_NAME` and `RDB$DEPENDENT_TYPE`.

        Returns:
            The `.SchemaItem` subclass instance (e.g., `.View`, `.Procedure`, `.Trigger`)
            representing the dependent object, or `None` if the object cannot be found
            or the type is currently unhandled.
        """
        result = None
        if self.dependent_type == 0: # TABLE
            result = self.schema.all_tables.get(self.dependent_name)
        elif self.dependent_type == 1: # VIEW
            result = self.schema.all_views.get(self.dependent_name)
        elif self.dependent_type == 2: # TRIGGER
            result = self.schema.all_triggers.get(self.dependent_name)
        elif self.dependent_type == 3: # COMPUTED FIELD (i.e. DOMAIN)
            result = self.schema.all_domains.get(self.dependent_name)
        elif self.dependent_type == 4:
            ## ToDo: Implement handler for VALIDATION if necessary
            result = None
        elif self.dependent_type == 5: #PROCEDURE
            result = self.schema.all_procedures.get(self.dependent_name)
        elif self.dependent_type == 6: # EXPRESSION INDEX
            result = self.schema.all_indices.get(self.dependent_name)
        elif self.dependent_type == 7: # EXCEPTION
            result = self.schema.exceptions.get(self.dependent_name)
        elif self.dependent_type == 8:
            ## ToDo: Implement handler for USER if necessary
            result = None
        elif self.dependent_type == 9: # FIELD (i.e. DOMAIN)
            result = self.schema.all_domains.get(self.dependent_name)
        elif self.dependent_type == 10: # INDEX
            result = self.schema.all_indices.get(self.dependent_name)
        elif self.dependent_type == 11:
            ## ToDo: Implement handler for DEPENDENT COUNT if necessary
            result = None
        elif self.dependent_type == 12:
            ## ToDo: Implement handler for USER GROUP if necessary
            result = None
        elif self.dependent_type == 13: # ROLE
            result = self.schema.roles.get(self.dependent_name)
        elif self.dependent_type == 14: # GENERATOR
            result = self.schema.all_generators.get(self.dependent_name)
        elif self.dependent_type == 15: # UDF
            result = self.schema.all_functions.get(self.dependent_name)
        elif self.dependent_type == 16:
            ## ToDo: Implement handler for BLOB_FILTER
            result = None
        elif self.dependent_type == 17: # Collation
            result = self.schema.collations.get(self.dependent_name)
        elif self.dependent_type in (18, 19): # Package + package body
            result = self.schema.packages.get(self.dependent_name)
        return result
    @property
    def dependent_name(self) -> str:
        """The name (`RDB$DEPENDENT_NAME`) of the object that has the dependency."""
        return self._attributes['RDB$DEPENDENT_NAME']
    @property
    def dependent_type(self) -> ObjectType:
        """The type (`.ObjectType`) of the object that has the dependency (`RDB$DEPENDENT_TYPE`)."""
        return ObjectType(value) if (value := self._attributes['RDB$DEPENDENT_TYPE']) is not None else None
    @property
    def field_name(self) -> str | None:
        """The specific field/column name (`RDB$FIELD_NAME`) involved in the dependency, if applicable.

        This is non-NULL when the dependency relates to a specific column (e.g.,
        a procedure depending on a table column, a view column based on a table column).
        Returns `None` if the dependency is on the object as a whole.
        """
        return self._attributes['RDB$FIELD_NAME']
    @property
    def depended_on(self) -> SchemaItem:
        """The database object that is being depended upon.

        Resolves the object based on `RDB$DEPENDED_ON_NAME`, `RDB$DEPENDED_ON_TYPE`,
        and potentially `RDB$FIELD_NAME`. If `field_name` is set, this property
        attempts to return the specific column object; otherwise, it returns the
        container object (table, view, procedure, etc.).

        Returns:
            The `.SchemaItem` subclass instance (e.g., `.Table`, `.Procedure`, `.Domain`)
            or a `.TableColumn` / `.ViewColumn` instance representing the object being
            depended upon, or `None` if it cannot be resolved or the type is unhandled.
        """
        result = None
        if self.depended_on_type == 0: # TABLE
            t = self.schema.all_tables.get(self.depended_on_name)
            if self.field_name:
                result = t.columns.get(self.field_name)
            else:
                result = t
        elif self.depended_on_type == 1: # VIEW
            t = self.schema.all_views.get(self.depended_on_name)
            if self.field_name:
                result = t.columns.get(self.field_name)
            else:
                result = t
        elif self.depended_on_type == 2: # TRIGGER
            result = self.schema.all_triggers.get(self.depended_on_name)
        elif self.depended_on_type == 3: # COMPUTED FIELD (i.e. DOMAIN)
            result = self.schema.all_domains.get(self.depended_on_name)
        elif self.depended_on_type == 4:
            ## ToDo: Implement handler for VALIDATION if necessary
            result = None
        elif self.depended_on_type == 5: #PROCEDURE
            result = self.schema.all_procedures.get(self.depended_on_name)
        elif self.depended_on_type == 6: # EXPRESSION INDEX
            result = self.schema.all_indices.get(self.depended_on_name)
        elif self.depended_on_type == 7: # EXCEPTION
            result = self.schema.exceptions.get(self.depended_on_name)
        elif self.depended_on_type == 8:
            ## ToDo: Implement handler for USER if necessary
            result = None
        elif self.depended_on_type == 9: # FIELD (i.e. DOMAIN)
            result = self.schema.all_domains.get(self.depended_on_name)
        elif self.depended_on_type == 10: # INDEX
            result = self.schema.all_indices.get(self.depended_on_name)
        elif self.depended_on_type == 11:
            ## ToDo: Implement handler for DEPENDENT COUNT if necessary
            result = None
        elif self.depended_on_type == 12:
            ## ToDo: Implement handler for USER GROUP if necessary
            result = None
        elif self.depended_on_type == 13: # ROLE
            result = self.schema.roles.get(self.depended_on_name)
        elif self.depended_on_type == 14: # GENERATOR
            result = self.schema.all_generators.get(self.depended_on_name)
        elif self.depended_on_type == 15: # UDF
            result = self.schema.all_functions.get(self.depended_on_name)
        elif self.depended_on_type == 16:
            ## ToDo: Implement handler for BLOB_FILTER
            result = None
        return result
    @property
    def depended_on_name(self) -> str:
        """The name (`RDB$DEPENDED_ON_NAME`) of the object being depended upon."""
        return self._attributes['RDB$DEPENDED_ON_NAME']
    @property
    def depended_on_type(self) -> ObjectType:
        """The type (`.ObjectType`) of the object being depended upon (`RDB$DEPENDED_ON_TYPE`)."""
        return ObjectType(value) if (value := self._attributes['RDB$DEPENDED_ON_TYPE']) is not None else None
    @property
    def package(self) -> Package | None:
        """The `.Package` object involved, if the dependency relates to a packaged object
        (`RDB$PACKAGE_NAME`).

        This typically means the `dependent` object is part of this package. Returns `None`
        if the dependency does not involve a package.
        """
        return self.schema.packages.get(self._attributes.get('RDB$PACKAGE_NAME'))

class Constraint(SchemaItem):
    """Represents a table or column constraint (PRIMARY KEY, UNIQUE, FOREIGN KEY, CHECK, NOT NULL).

    Constraints enforce data integrity rules within the database. They are associated
    with a specific table and may rely on an underlying index (for PK, UK, FK) or
    triggers (for CHECK, NOT NULL) for enforcement.

    Instances map data primarily from `RDB$RELATION_CONSTRAINTS`, potentially joined with
    `RDB$REF_CONSTRAINTS` (for FK) and `RDB$CHECK_CONSTRAINTS` (for CHECK).
    They are typically accessed via `.Schema.constraints` or `.Table.constraints`.

    Supported SQL actions via `get_sql_for()`:

    *   User-defined constraints (excluding NOT NULL):

        *   `create`: Generates the `ALTER TABLE ... ADD CONSTRAINT ...` statement.
            Handles PK, UNIQUE, FK (including rules), and CHECK constraints.
        *   `drop`: Generates the `ALTER TABLE ... DROP CONSTRAINT ...` statement.

    *   System constraints or NOT NULL constraints:

        *   No direct SQL actions supported via `get_sql_for()`. NOT NULL is
            typically managed as part of the column/domain definition. System
            constraints (on system tables) generally cannot be modified.

    .. note::

       NOT NULL constraints are represented by this class internally when fetched
       from system tables but do not support `create` or `drop` actions here.
       They are managed via `ALTER DOMAIN` or `ALTER TABLE ... ALTER COLUMN`.

    Arguments:
        schema: The parent `.Schema` instance.
        attributes: Raw data dictionary fetched from `RDB$RELATION_CONSTRAINTS`
                    (potentially joined with other constraint tables).
    """
    def __init__(self, schema: Schema, attributes: dict[str, Any]):
        super().__init__(schema, attributes)
        self._strip_attribute('RDB$CONSTRAINT_NAME')
        self._strip_attribute('RDB$CONSTRAINT_TYPE')
        self._strip_attribute('RDB$RELATION_NAME')
        self._strip_attribute('RDB$DEFERRABLE')
        self._strip_attribute('RDB$INITIALLY_DEFERRED')
        self._strip_attribute('RDB$INDEX_NAME')
        self._strip_attribute('RDB$TRIGGER_NAME')
        self._strip_attribute('RDB$CONST_NAME_UQ')
        self._strip_attribute('RDB$MATCH_OPTION')
        self._strip_attribute('RDB$UPDATE_RULE')
        self._strip_attribute('RDB$DELETE_RULE')
        if not (self.is_sys_object() or self.is_not_null()):
            self._actions.extend(['create', 'drop'])
    def _get_create_sql(self, **params) -> str:
        """Generates the SQL command to ADD this constraint to its table.

        Constructs `ALTER TABLE ... ADD CONSTRAINT ...` syntax for PRIMARY KEY,
        UNIQUE, FOREIGN KEY, and CHECK constraints.

        Arguments:
            **params: Accepts no parameters.

        Returns:
            The `ALTER TABLE ... ADD CONSTRAINT ...` SQL string.

        Raises:
            ValueError: If unexpected parameters are passed.
            Error: If called for a NOT NULL or unsupported constraint type,
                   or if required underlying objects (like index or partner
                   constraint) are missing.
        """
        self._check_params(params, [])
        const_def = f'ALTER TABLE {self.table.get_quoted_name()} ADD '
        if not self.name.startswith('INTEG_'):
            const_def += f'CONSTRAINT {self.get_quoted_name()}\n  '
        if self.is_check():
            const_def += self.triggers[0].source
        elif self.is_pkey() or self.is_unique():
            const_def += 'PRIMARY KEY' if self.is_pkey() else 'UNIQUE'
            i = self.index
            const_def += f" ({','.join(i.segment_names)})"
            if not i.is_sys_object():
                const_def += f'\n  USING {i.index_type.value} INDEX {i.get_quoted_name()}'
        elif self.is_fkey():
            const_def += f"FOREIGN KEY ({','.join(self.index.segment_names)})\n  "
            p = self.partner_constraint
            const_def += f"REFERENCES {p.table.get_quoted_name()} ({','.join(p.index.segment_names)})"
            if self.delete_rule != 'RESTRICT':
                const_def += f'\n  ON DELETE {self.delete_rule}'
            if self.update_rule != 'RESTRICT':
                const_def += f'\n  ON UPDATE {self.update_rule}'
            i = self.index
            if not i.is_sys_object():
                const_def += f'\n  USING {i.index_type.value} INDEX {i.get_quoted_name()}'
        else:
            raise Error(f"Unrecognized constraint type '{self.constraint_type}'")
        return const_def
    def _get_drop_sql(self, **params) -> str:
        """Generates the SQL command to DROP this constraint from its table.

        Arguments:
            **params: Accepts no parameters.

        Returns:
            The `ALTER TABLE ... DROP CONSTRAINT ...` SQL string.

        Raises:
            ValueError: If unexpected parameters are passed.
            Error: If called for a NOT NULL constraint or if the table cannot be found.
        """
        self._check_params(params, [])
        return f'ALTER TABLE {self.table.get_quoted_name()} DROP CONSTRAINT {self.get_quoted_name()}'
    def _get_name(self) -> str:
        """Returns the constraint name (`RDB$CONSTRAINT_NAME`)."""
        return self._attributes['RDB$CONSTRAINT_NAME']
    def is_sys_object(self) -> bool:
        """Checks if this constraint is defined on a system table.

        Returns:
            `True` if the associated table is a system object, `False` otherwise.
        """
        return self.schema.all_tables.get(self._attributes['RDB$RELATION_NAME']).is_sys_object()
    def is_not_null(self) -> bool:
        """Checks if this is a `NOT NULL` constraint.

        Returns:
            `True` if `constraint_type` is `.ConstraintType.NOT_NULL`, `False` otherwise.
        """
        return self.constraint_type == ConstraintType.NOT_NULL
    def is_pkey(self) -> bool:
        """Checks if this is a `PRIMARY KEY` constraint.

        Returns:
            `True` if `constraint_type` is `.ConstraintType.PRIMARY_KEY`, `False` otherwise.
        """
        return self.constraint_type == ConstraintType.PRIMARY_KEY
    def is_fkey(self) -> bool:
        """Checks if this is a `FOREIGN KEY` constraint.

        Returns:
            `True` if `constraint_type` is `.ConstraintType.FOREIGN_KEY`, `False` otherwise.
        """
        return self.constraint_type == ConstraintType.FOREIGN_KEY
    def is_unique(self) -> bool:
        """Checks if this is a `UNIQUE` constraint.

        Returns:
            `True` if `constraint_type` is `.ConstraintType.UNIQUE`, `False` otherwise.
        """
        return self.constraint_type == ConstraintType.UNIQUE
    def is_check(self) -> bool:
        """Checks if this is a `CHECK` constraint.

        Returns:
            `True` if `constraint_type` is `.ConstraintType.CHECK`, `False` otherwise.
        """
        return self.constraint_type == ConstraintType.CHECK
    def is_deferrable(self) -> bool:
        """Checks if the constraint is defined as `DEFERRABLE`.

        Based on `RDB$DEFERRABLE` ('YES' or 'NO').

        Returns:
            `True` if deferrable, `False` otherwise.
        """
        # RDB$DEFERRABLE = 'YES' means deferrable
        return self._attributes.get('RDB$DEFERRABLE', 'NO').upper() == 'YES'
    def is_deferred(self) -> bool:
        """Checks if the constraint is defined as `INITIALLY DEFERRED`.

        Based on `RDB$INITIALLY_DEFERRED` ('YES' or 'NO'). Relevant only if `is_deferrable()` is True.

        Returns:
            `True` if initially deferred, `False` otherwise.
        """
        # RDB$INITIALLY_DEFERRED = 'YES' means initially deferred
        return self._attributes.get('RDB$INITIALLY_DEFERRED', 'NO').upper() == 'YES'
    @property
    def constraint_type(self) -> ConstraintType:
        """The type of the constraint (`.ConstraintType` enum).

        Derived from `RDB$CONSTRAINT_TYPE` ('PRIMARY KEY', 'UNIQUE', etc.).
        Returns `None` if the type string is unrecognized.
        """
        return ConstraintType(self._attributes['RDB$CONSTRAINT_TYPE'])
    @property
    def table(self) -> Table:
        """The `.Table` object this constraint is defined on (`RDB$RELATION_NAME`)."""
        return self.schema.all_tables.get(self._attributes['RDB$RELATION_NAME'])
    @property
    def index(self) -> Index | None:
        """The `.Index` object used to enforce the constraint (`RDB$INDEX_NAME`).

        Relevant for PRIMARY KEY, UNIQUE, and FOREIGN KEY constraints.
        Returns `None` for CHECK and NOT NULL constraints, or if the index cannot be found.
        """
        return self.schema.all_indices.get(self._attributes['RDB$INDEX_NAME'])
    @property
    def trigger_names(self) -> list[str]:
        """For a `CHECK` constraint: A list of trigger names that enforce it.
           For a `NOT NULL` constraint: The name of the single column it applies to.
           Returns `None` for other constraint types.
        """
        if self.is_check():
            return self._attributes['RDB$TRIGGER_NAME']
        return []
    @property
    def triggers(self) -> DataList[Trigger]:
        """For a `CHECK` constraint: A `.DataList` of the `.Trigger` objects that enforce it.

        Returns an empty list for other constraint types or if triggers cannot be found.
        """
        return self.schema.all_triggers.extract(lambda x: x.name in self.trigger_names, copy=True)
    @property
    def column_name(self) -> str | None:
        """For a `NOT NULL` constraint: The name of the column it applies to.

        Returns `None` for other constraint types.
        """
        return self._attributes['RDB$TRIGGER_NAME'] if self.is_not_null() else None
    @property
    def partner_constraint(self) -> Constraint | None:
        """For a `FOREIGN KEY` constraint: The referenced `PRIMARY KEY` or `UNIQUE`
        `.Constraint` object (`RDB$CONST_NAME_UQ`).

        Returns `None` for other constraint types or if the partner constraint cannot be found.
        """
        return self.schema.constraints.get(self._attributes['RDB$CONST_NAME_UQ'])
    @property
    def match_option(self) -> str | None:
        """For a `FOREIGN KEY` constraint: The match option specified (`RDB$MATCH_OPTION`).
        Usually 'FULL' or 'SIMPLE' (though 'SIMPLE' might not be fully supported).
        Returns `None` for other constraint types."""
        return self._attributes['RDB$MATCH_OPTION']
    @property
    def update_rule(self) -> str | None:
        """For a `FOREIGN KEY` constraint: The action specified for `ON UPDATE`
        (`RDB$UPDATE_RULE`, e.g., 'RESTRICT', 'CASCADE', 'SET NULL', 'SET DEFAULT').
        Returns `None` for other constraint types."""
        return self._attributes['RDB$UPDATE_RULE']
    @property
    def delete_rule(self) -> str | None:
        """For a `FOREIGN KEY` constraint: The action specified for `ON DELETE`
        (`RDB$DELETE_RULE`, e.g., 'RESTRICT', 'CASCADE', 'SET NULL', 'SET DEFAULT').
        Returns `None` for other constraint types."""
        return self._attributes['RDB$DELETE_RULE']

class Table(SchemaItem):
    """Represents a database table, including persistent, global temporary, and external tables.

    This class serves as a container for the table's metadata, including its columns,
    constraints (primary key, foreign keys, unique, check), indexes, and triggers.
    It provides methods to generate SQL DDL for creating or dropping the table and
    its associated objects (partially, constraints/indexes might need separate creation).

    Instances map data primarily from the `RDB$RELATIONS` system table where
    `RDB$VIEW_BLR` is NULL. Associated objects like columns, constraints, etc.,
    are fetched from other system tables (`RDB$RELATION_FIELDS`, `RDB$RELATION_CONSTRAINTS`,
    `RDB$INDICES`, `RDB$TRIGGERS`).

    Access typically occurs via `.Schema.tables`, `.Schema.sys_tables`, or `.Schema.all_tables`.

    Supported SQL actions via `.get_sql_for()`:

    *   User-defined tables:

        *   `create` (optional keyword args: `no_pk`: bool=False, `no_unique`: bool=False):
            Generates `CREATE [GLOBAL TEMPORARY] TABLE ...` statement. Includes column
            definitions. Optionally includes inline PRIMARY KEY and UNIQUE constraints
            unless excluded by `no_pk=True` or `no_unique=True` respectively.
            CHECK and FOREIGN KEY constraints are *not* included inline by this method.
        *   `recreate` (optional keyword args: `no_pk`: bool=False, `no_unique`: bool=False):
            Generates `RECREATE [GLOBAL TEMPORARY] TABLE ...` (similar to `create`).
        *   `drop`: Generates `DROP TABLE ...`.
        *   `comment`: Generates `COMMENT ON TABLE ... IS ...`.
        *   `insert` (optional keyword args: `update`: bool=False, `returning`: list[str]=None,
            `matching`: list[str]=None): Generates an `INSERT` or `UPDATE OR INSERT`
            statement template with placeholders for all columns. Optionally adds
            `MATCHING` and `RETURNING` clauses.

    *   System tables:

        *   `comment`: Adds or removes a descriptive comment.

    Arguments:
        schema: The parent `.Schema` instance.
        attributes: Raw data dictionary fetched from the `RDB$RELATIONS` row.
    """
    def __init__(self, schema: Schema, attributes: dict[str, Any]):
        super().__init__(schema, attributes)
        self._type_code.append(ObjectType.TABLE)
        self.__columns = None
        self._strip_attribute('RDB$RELATION_NAME')
        self._strip_attribute('RDB$OWNER_NAME')
        self._strip_attribute('RDB$SECURITY_CLASS')
        self._strip_attribute('RDB$DEFAULT_CLASS')
        self._actions.append('comment')
        if not self.is_sys_object():
            self._actions.extend(['create', 'recreate', 'drop'])
    def _get_insert_sql(self, **params) -> str:
        """Generates an SQL INSERT or UPDATE OR INSERT statement template for this table.

        Includes all columns in the column list and corresponding placeholders (`?`)
        in the VALUES clause. Optionally adds MATCHING and RETURNING clauses.

        Arguments:
            **params: Accepts optional keyword arguments:

                      * `update` (bool): If `True`, generates `UPDATE OR INSERT` instead
                        of `INSERT`. Defaults to `False`.
                      * `returning` (list[str]): A list of column names or expressions
                        to include in the `RETURNING` clause. Defaults to `None`.
                      * `matching` (list[str]): A list of column names to include in the
                        `MATCHING (...)` clause (used with `UPDATE OR INSERT`). Defaults to `None`.

        Returns:
            The generated `INSERT` or `UPDATE OR INSERT` SQL statement string.

        Raises:
            ValueError: If unexpected parameters are passed.
        """
        try:
            self._check_params(params, ['update', 'returning', 'matching'])
            update = params.get('update', False)
            returning = params.get('returning')
            matching = params.get('returning')
            #
            result = f"{'UPDATE OR ' if update else ''}INSERT TABLE {self.get_quoted_name()}"
            result += f" ({','.join(col.get_quoted_name() for col in self.columns)})"
            result += f" VALUES ({','.join('?' for col in self.columns)})"
            if matching:
                result += f" MATCHING ({','.join(matching)})"
            if returning:
                result += f" RETURNING ({','.join(returning)})"
            return result
        except Exception as e:
            raise e
    def _get_create_sql(self, **params) -> str:
        """Generates the SQL command to CREATE this table.

        Includes column definitions based on their underlying domains or types.
        Optionally includes inline PRIMARY KEY and UNIQUE constraints. Does *not*
        include CHECK or FOREIGN KEY constraints inline; create those separately.

        Arguments:
            **params: Accepts optional keyword arguments:

                      * `no_pk` (bool): If `True`, excludes the inline PRIMARY KEY
                        constraint definition (if one exists). Defaults to `False`.
                      * `no_unique` (bool): If `True`, excludes inline UNIQUE constraint
                        definitions. Defaults to `False`.

        Returns:
            The `CREATE [GLOBAL TEMPORARY] TABLE` SQL string.

        Raises:
            ValueError: If unexpected parameters are passed.
            Error: If essential information (like columns) cannot be loaded.
        """
        try:
            self._check_params(params, ['no_pk', 'no_unique'])
            no_pk = params.get('no_pk', False)
            no_unique = params.get('no_unique', False)
            #
            tabdef = f"CREATE {'GLOBAL TEMPORARY ' if self.is_gtt() else ''}TABLE {self.get_quoted_name()}"
            if self.is_external():
                tabdef += f"  EXTERNAL FILE '{self.external_file}'\n"
            tabdef += ' ('
            partdefs = []
            for col in self.columns:
                coldef = f'\n  {col.get_quoted_name()} '
                collate = ''
                if col.is_domain_based():
                    coldef += col.domain.get_quoted_name()
                elif col.is_computed():
                    coldef += f'COMPUTED BY {col.get_computedby()}'
                else:
                    datatype = col.datatype
                    if datatype.rfind(' COLLATE ') > 0:
                        datatype, collate = datatype.split(' COLLATE ')
                    coldef += datatype
                if col.is_identity():
                    coldef += ' GENERATED BY DEFAULT AS IDENTITY'
                    if col.generator.inital_value != 0:
                        coldef += f' (START WITH {col.generator.inital_value})'
                else:
                    if col.has_default():
                        coldef += f' DEFAULT {col.default}'
                    if not col.is_nullable():
                        coldef += ' NOT NULL'
                    if col._attributes['RDB$COLLATION_ID'] is not None:
                        # Sometimes RDB$COLLATION_ID has a garbage value
                        if col.collation is not None:
                            cname = col.collation.name
                            if col.domain.character_set._attributes['RDB$DEFAULT_COLLATE_NAME'] != cname:
                                collate = cname
                if collate:
                    coldef += f' COLLATE {collate}'
                partdefs.append(coldef)
            if self.has_pkey() and not no_pk:
                pk = self.primary_key
                pkdef = '\n  '
                if not pk.name.startswith('INTEG_'):
                    pkdef += f'CONSTRAINT {pk.get_quoted_name()}\n  '
                i = pk.index
                pkdef += f"PRIMARY KEY ({','.join(i.segment_names)})"
                if not i.is_sys_object():
                    pkdef += f'\n    USING {i.index_type.value} INDEX {i.get_quoted_name()}'
                partdefs.append(pkdef)
            if not no_unique:
                for uq in self.constraints:
                    if uq.is_unique():
                        uqdef = '\n  '
                        if not uq.name.startswith('INTEG_'):
                            uqdef += f'CONSTRAINT {uq.get_quoted_name()}\n  '
                        i = uq.index
                        uqdef += f"UNIQUE ({','.join(i.segment_names)})"
                        if not i.is_sys_object():
                            uqdef += f'\n    USING {i.index_type.value} INDEX {i.get_quoted_name()}'
                        partdefs.append(uqdef)
            tabdef += ','.join(partdefs)
            tabdef += '\n)'
            return tabdef
        except Exception as e:
            raise e
    def _get_drop_sql(self, **params) -> str:
        """Generates the SQL command to DROP this table.

        Arguments:
            **params: Accepts no parameters.

        Returns:
            The `DROP TABLE` SQL string.

        Raises:
            ValueError: If unexpected parameters are passed.
        """
        self._check_params(params, [])
        return f'DROP TABLE {self.get_quoted_name()}'
    def _get_comment_sql(self, **params) -> str:
        """Generates the SQL command to add or remove a COMMENT ON this table.

        Arguments:
            **params: Accepts no parameters.

        Returns:
            The `COMMENT ON TABLE ... IS ...` SQL string. Sets comment to `NULL` if
            `self.description` is None, otherwise uses the description text
            with proper escaping.

        Raises:
            ValueError: If unexpected parameters are passed.
        """
        comment = 'NULL' if self.description is None \
            else f"'{escape_single_quotes(self.description)}'"
        return f'COMMENT ON TABLE {self.get_quoted_name()} IS {comment}'
    def _get_name(self) -> str:
        """Returns the table name (`RDB$RELATION_NAME`)."""
        return self._attributes['RDB$RELATION_NAME']
    def is_gtt(self) -> bool:
        """Checks if this table is a Global Temporary Table (GTT).

        Returns:
            `True` if `table_type` is `.RelationType.GLOBAL_TEMPORARY_DELETE` or
            `.RelationType.GLOBAL_TEMPORARY_PRESERVE`, `False` otherwise.
        """
        return self.table_type in (RelationType.GLOBAL_TEMPORARY_DELETE,
                                   RelationType.GLOBAL_TEMPORARY_PRESERVE)
    def is_persistent(self) -> bool:
        """Checks if this table is a standard persistent table or an external table.

        Excludes views and GTTs.

        Returns:
            `True` if `table_type` is `.RelationType.PERSISTENT` or
            `.RelationType.EXTERNAL`, `False` otherwise.
        """
        return self.table_type in (RelationType.PERSISTENT, RelationType.EXTERNAL)
    def is_external(self) -> bool:
        """Checks if this table is an External Table.

        Based on the presence of the `RDB$EXTERNAL_FILE` attribute.

        Returns:
            `True` if it's an external table, `False` otherwise.
        """
        return bool(self.external_file)
    def has_pkey(self) -> bool:
        """Checks if the table has a `PRIMARY KEY` constraint defined.

        Iterates through the table's constraints.

        Returns:
            `True` if a primary key constraint exists, `False` otherwise.
        """
        for const in self.constraints:
            if const.is_pkey():
                return True
        return False
    def has_fkey(self) -> bool:
        """Checks if the table has at least one `FOREIGN KEY` constraint defined.

        Iterates through the table's constraints.

        Returns:
            `True` if any foreign key constraints exist, `False` otherwise.
        """
        for const in self.constraints:
            if const.is_fkey():
                return True
        return False
    @property
    def id(self) -> int:
        """The internal numeric ID (`RDB$RELATION_ID`) assigned to the table/relation."""
        return self._attributes['RDB$RELATION_ID']
    @property
    def dbkey_length(self) -> int:
        """Length of the internal `RDB$DB_KEY` pseudo-column in bytes (`RDB$DBKEY_LENGTH`)."""
        return self._attributes['RDB$DBKEY_LENGTH']
    @property
    def format(self) -> int:
        """The internal format version number (`RDB$FORMAT`) for the table's record structure."""
        return self._attributes['RDB$FORMAT']
    @property
    def table_type(self) -> RelationType:
        """The type of the relation (`.RelationType` enum).

        Derived from `RDB$RELATION_TYPE` (0=Persistent, 2=External,
        4=GTT Preserve, 5=GTT Delete). Views (1) and Virtual (3) are excluded
        by the table loading logic.
        """
        return RelationType(self._attributes.get('RDB$RELATION_TYPE'))
    @property
    def security_class(self) -> str | None:
        """The security class name associated with this table, if any (`RDB$SECURITY_CLASS`).
        Used for access control limits. Returns `None` if not set."""
        return self._attributes['RDB$SECURITY_CLASS']
    @property
    def external_file(self) -> str | None:
        """The full path and filename of the external data file (`RDB$EXTERNAL_FILE`).
        Returns `None` if this is not an external table."""
        ext_file = self._attributes.get('RDB$EXTERNAL_FILE')
        return ext_file if ext_file else None
    @property
    def owner_name(self) -> str:
        """The user name of the table's owner/creator (`RDB$OWNER_NAME`)."""
        return self._attributes['RDB$OWNER_NAME']
    @property
    def default_class(self) -> str:
        """Default security class name (`RDB$DEFAULT_CLASS`). Usage may vary."""
        return self._attributes['RDB$DEFAULT_CLASS']
    @property
    def flags(self) -> int:
        """Internal flags (`RDB$FLAGS`) used by the engine. Interpretation may vary."""
        return self._attributes['RDB$FLAGS']
    @property
    def primary_key(self) -> Constraint | None:
        """The `PRIMARY KEY` `.Constraint` object defined for this table.

        Returns:
            The `.Constraint` object representing the primary key, or `None` if
            no primary key is defined on this table. Finds the first constraint
            marked as PK.
        """
        return self.constraints.find(lambda c: c.is_pkey())
    @property
    def foreign_keys(self) -> DataList[Constraint]:
        """A `.DataList` of all `FOREIGN KEY` `.Constraint` objects defined for this table."""
        return self.constraints.extract(lambda c: c.is_fkey(), copy=True)
    @property
    def columns(self) -> DataList[TableColumn]:
        """A lazily-loaded `.DataList` of all `.TableColumn` objects defined for this table.

        Columns are ordered by their position (`RDB$FIELD_POSITION`). Fetched from
        `RDB$RELATION_FIELDS`.
        """
        if self.__columns is None:
            cols = ['RDB$FIELD_NAME', 'RDB$RELATION_NAME', 'RDB$FIELD_SOURCE',
                    'RDB$FIELD_POSITION', 'RDB$UPDATE_FLAG', 'RDB$FIELD_ID',
                    'RDB$DESCRIPTION', 'RDB$SECURITY_CLASS', 'RDB$SYSTEM_FLAG',
                    'RDB$NULL_FLAG', 'RDB$DEFAULT_SOURCE', 'RDB$COLLATION_ID',
                    'RDB$GENERATOR_NAME', 'RDB$IDENTITY_TYPE']
            cmd = f"select {','.join(cols)} from RDB$RELATION_FIELDS " \
                  f"where RDB$RELATION_NAME = ? order by RDB$FIELD_POSITION"
            self.__columns = DataList((TableColumn(self.schema, self, row) for row
                                       in self.schema._select(cmd, (self.name,))),
                                      TableColumn, 'item.name', frozen=True)
        return self.__columns
    @property
    def constraints(self) -> DataList[Constraint]:
        """A `.DataList` of all `.Constraint` objects (PK, FK, UK, CHECK) defined for this table.

        Filters the main `.Schema.constraints` collection.
        """
        return self.schema.constraints.extract(lambda c: c._attributes['RDB$RELATION_NAME'] == self.name,
                                               copy=True)
    @property
    def indices(self) -> DataList[Index]:
        """A `.DataList` of all `.Index` objects defined for this table.

        Filters the main `.Schema.all_indices` collection.
        """
        return self.schema.all_indices.extract(lambda i: i._attributes['RDB$RELATION_NAME'] == self.name,
                                               copy=True)
    @property
    def triggers(self) -> DataList[Trigger]:
        """A `.DataList` of all `.Trigger` objects defined for this table.

        Filters the main `Schema.triggers` collection (which contains user triggers).
        Use `.Schema.all_triggers` if system triggers are needed.
        """
        return self.schema.triggers.extract(lambda t: t._attributes['RDB$RELATION_NAME'] == self.name,
                                            copy=True)
    @property
    def privileges(self) -> DataList[Privilege]:
        """A `.DataList` of all `.Privilege` objects granted *on* this table.

        Filters the main `.Schema.privileges` collection. Includes privileges granted
        on the table as a whole, not column-specific privileges (see `.TableColumn.privileges`).
        """
        return self.schema.privileges.extract(lambda p: ((p.subject_name == self.name) and
                                                         (p.subject_type in self._type_code)),
                                                     copy=True)

class View(SchemaItem):
    """Represents a database view, a virtual table based on a stored SQL query.

    Views provide a way to simplify complex queries, encapsulate logic, and control
    data access by presenting a predefined subset or transformation of data from
    one or more base tables or other views.

    Instances map data primarily from the `RDB$RELATIONS` system table where
    `RDB$VIEW_BLR` is NOT NULL. Associated columns are fetched from
    `RDB$RELATION_FIELDS`.

    Access typically occurs via `.Schema.views`, `.Schema.sys_views`, or `.Schema.all_views`.

    Supported SQL actions via `.get_sql_for()`:

    *   User-defined views:

        *   `create`: Generates `CREATE VIEW view_name (col1, ...) AS SELECT ...`.
             Includes the column list and the view's query (`AS SELECT ...`).
        *   `recreate`: Generates `RECREATE VIEW ...` (similar structure to `create`).
        *   `alter` (keyword args): Modifies the view definition.

            *   `columns` (str | list[str] | tuple[str], optional): A comma-separated string or
                list/tuple of new column names for the view definition. If omitted,
                the existing column list (if any) is generally assumed or derived by the DB.
            *   `query` (str, **required**): The new `SELECT ...` statement defining the view.
            *   `check` (bool, optional): If `True`, adds `WITH CHECK OPTION` to the
                view definition. Defaults to `False`.

        *   `create_or_alter`: Generates `CREATE OR ALTER VIEW ...` (combines create/alter logic).
        *   `drop`: Generates `DROP VIEW ...`.
        *   `comment`: Generates `COMMENT ON VIEW ... IS ...`.

    *   System views:

        *   `comment`: Adds or removes a descriptive comment.

    Arguments:
        schema: The parent `.Schema` instance.
        attributes: Raw data dictionary fetched from the `RDB$RELATIONS` row.
    """
    def __init__(self, schema: Schema, attributes: dict[str, Any]):
        super().__init__(schema, attributes)
        self._type_code.append(ObjectType.VIEW)
        self.__columns = None
        self._strip_attribute('RDB$RELATION_NAME')
        self._strip_attribute('RDB$VIEW_SOURCE')
        self._strip_attribute('RDB$OWNER_NAME')
        self._strip_attribute('RDB$SECURITY_CLASS')
        self._strip_attribute('RDB$DEFAULT_CLASS')
        self._actions.append('comment')
        if not self.is_sys_object():
            self._actions.extend(['create', 'recreate', 'alter', 'create_or_alter', 'drop'])
    def _get_create_sql(self, **params) -> str:
        """Generates the SQL command to CREATE this view.

        Includes the explicit column list and the `AS SELECT ...` query definition.

        Arguments:
            **params: Accepts no parameters.

        Returns:
            The `CREATE VIEW` SQL string.

        Raises:
            ValueError: If unexpected parameters are passed.
            Error: If view columns cannot be loaded.
        """
        self._check_params(params, [])
        return f"CREATE VIEW {self.get_quoted_name()}" \
               f" ({','.join([col.get_quoted_name() for col in self.columns])})\n" \
               f"   AS\n     {self.sql}"
    def _get_alter_sql(self, **params) -> str:
        """Generates the SQL command to ALTER this view.

        Allows changing the column list (optional), the underlying query (required),
        and adding/removing the `WITH CHECK OPTION`.

        Arguments:
            **params: Accepts keyword arguments:

                      * `columns` (str | list[str] | tuple[str], optional): New column list.
                        If provided as list/tuple, names are joined with commas. If omitted,
                        the existing column list structure is often retained or inferred by the DB.
                      * `query` (str, **required**): The new `SELECT ...` statement for the view.
                      * `check` (bool, optional): Set to `True` to add `WITH CHECK OPTION`.
                        Defaults to `False`.

        Returns:
            The `ALTER VIEW` SQL string.

        Raises:
            ValueError: If the required `query` parameter is missing, if parameter types
                        are incorrect, or if unexpected parameters are passed.
        """
        self._check_params(params, ['columns', 'query', 'check'])
        columns = params.get('columns')
        if isinstance(columns, list | tuple):
            columns = ','.join(columns)
        query = params.get('query')
        check = params.get('check', False)
        if query:
            columns = f'({columns})' if columns else ''
            if check:
                query = f'{query}\n     WITH CHECK OPTION'
            return f"ALTER VIEW {self.get_quoted_name()} {columns}\n   AS\n     {query}"
        raise ValueError("Missing required parameter: 'query'.")
    def _get_drop_sql(self, **params) -> str:
        """Generates the SQL command to DROP this view.

        Arguments:
            **params: Accepts no parameters.

        Returns:
            The `DROP VIEW` SQL string.

        Raises:
            ValueError: If unexpected parameters are passed.
        """
        self._check_params(params, [])
        return f'DROP VIEW {self.get_quoted_name()}'
    def _get_comment_sql(self, **params) -> str:
        """Generates the SQL command to add or remove a COMMENT ON this view.

        Arguments:
            **params: Accepts no parameters.

        Returns:
            The `COMMENT ON VIEW ... IS ...` SQL string. Sets comment to `NULL` if
            `self.description` is None, otherwise uses the description text
            with proper escaping.

        Raises:
            ValueError: If unexpected parameters are passed.
        """
        comment = 'NULL' if self.description is None \
            else f"'{escape_single_quotes(self.description)}'"
        return f'COMMENT ON VIEW {self.get_quoted_name()} IS {comment}'
    def _get_name(self) -> str:
        """Returns the view name (`RDB$RELATION_NAME`)."""
        return self._attributes['RDB$RELATION_NAME']
    def has_checkoption(self) -> bool:
        """Checks if the view definition likely includes `WITH CHECK OPTION`.

        Performs a case-insensitive search for "WITH CHECK OPTION" within the view's
        SQL source (`sql` property).

        .. warning::

           This is a simple text search and might produce false positives if the
           text appears within comments or string literals in the view definition.

        Returns:
            `True` if the text "WITH CHECK OPTION" is found, `False` otherwise.
        """
        return "WITH CHECK OPTION" in self.sql.upper()
    @property
    def id(self) -> int:
        """The internal numeric ID (`RDB$RELATION_ID`) assigned to the view/relation."""
        return self._attributes['RDB$RELATION_ID']
    @property
    def sql(self) -> str | None:
        """The `SELECT` statement text that defines the view (`RDB$VIEW_SOURCE`).
        Returns `None` if the source is not available."""
        return self._attributes['RDB$VIEW_SOURCE']
    @property
    def dbkey_length(self) -> int:
        """Length of the internal `RDB$DB_KEY` pseudo-column in bytes (`RDB$DBKEY_LENGTH`)."""
        return self._attributes['RDB$DBKEY_LENGTH']
    @property
    def format(self) -> int:
        """The internal format version number (`RDB$FORMAT`) for the view."""
        return self._attributes['RDB$FORMAT']
    @property
    def security_class(self) -> str | None:
        """The security class name associated with this view, if any (`RDB$SECURITY_CLASS`).
        Returns `None` if no specific security class is assigned."""
        return self._attributes['RDB$SECURITY_CLASS']
    @property
    def owner_name(self) -> str:
        """The user name of the view's owner/creator (`RDB$OWNER_NAME`)."""
        return self._attributes['RDB$OWNER_NAME']
    @property
    def default_class(self) -> str | None:
        """Default security class name (`RDB$DEFAULT_CLASS`). Usage may vary."""
        return self._attributes['RDB$DEFAULT_CLASS']
    @property
    def flags(self) -> int | None:
        """Internal flags (`RDB$FLAGS`) used by the engine. Interpretation may vary."""
        return self._attributes['RDB$FLAGS']
    @property
    def columns(self) -> DataList[ViewColumn]:
        """A lazily-loaded `.DataList` of all `.ViewColumn` objects defined for this view.

        Columns are ordered by their position (`RDB$FIELD_POSITION`). Fetched from
        `RDB$RELATION_FIELDS` potentially joined with `RDB$VIEW_RELATIONS`.
        """
        if self.__columns is None:
            self.__columns = DataList((ViewColumn(self.schema, self, row) for row
                                       in self.schema._select("""select r.RDB$FIELD_NAME,
r.RDB$RELATION_NAME, r.RDB$FIELD_SOURCE, r.RDB$FIELD_POSITION, r.RDB$UPDATE_FLAG,
r.RDB$FIELD_ID, r.RDB$DESCRIPTION, r.RDB$SYSTEM_FLAG, r.RDB$SECURITY_CLASS, r.RDB$NULL_FLAG,
r.RDB$DEFAULT_SOURCE, r.RDB$COLLATION_ID, r.RDB$BASE_FIELD, v.RDB$RELATION_NAME as BASE_RELATION
    from RDB$RELATION_FIELDS r
    left join RDB$VIEW_RELATIONS v on r.RDB$VIEW_CONTEXT = v.RDB$VIEW_CONTEXT and v.rdb$view_name = ?
    where r.RDB$RELATION_NAME = ?
    order by RDB$FIELD_POSITION""", (self.name, self.name))), ViewColumn, 'item.name', frozen=True)
        return self.__columns
    @property
    def triggers(self) -> DataList[Trigger]:
        """A `.DataList` of all user `.Trigger` objects defined for this view.

        Filters the main `.Schema.triggers` collection. Use `.Schema.all_triggers` if system
        triggers are needed.
        """
        return self.schema.triggers.extract(lambda t:
                                            t._attributes['RDB$RELATION_NAME'] == self.name,
                                            copy=True)
    @property
    def privileges(self) -> DataList[Privilege]:
        """A `.DataList` of all `.Privilege` objects granted *on* this view.

        Filters the main `Schema.privileges` collection. Includes privileges granted
        on the view as a whole. Column-specific privileges are accessed via
        `ViewColumn.privileges`.

        .. note::

           In `RDB$USER_PRIVILEGES`, privileges on views are often logged with the
           subject type as TABLE (0). This property accounts for that when filtering.
        """
        # Views are logged as Tables in RDB$USER_PRIVILEGES
        return self.schema.privileges.extract(lambda p: ((p.subject_name == self.name) and
                                                         (p.subject_type == 0)), copy=True)

class Trigger(SchemaItem):
    """Represents a database trigger, executing PSQL code in response to specific events.

    Triggers automate actions based on:

    *   Data Manipulation Language (DML) events on tables/views (`INSERT`, `UPDATE`, `DELETE`).
    *   Database-level events (`CONNECT`, `DISCONNECT`, `TRANSACTION START/COMMIT/ROLLBACK`).
    *   Data Definition Language (DDL) events (`CREATE/ALTER/DROP` of various objects).

    Triggers have an activation time (`BEFORE` or `AFTER` the event for DML/DDL) and a
    sequence/position to control execution order among multiple triggers for the same event.

    Instances map data primarily from the `RDB$TRIGGERS` system table. They are
    accessed via `.Schema.triggers`, `.Schema.sys_triggers`, `.Schema.all_triggers`,
    or `.Table.triggers`/`View.triggers`.

    Supported SQL actions via `.get_sql_for()`:

    *   User-defined triggers:

        *   `create` (optional keyword arg `inactive`: bool=False): Generates
            `CREATE TRIGGER ...` statement with full definition (relation/event,
            time, position, source code). Can create it initially inactive.
        *   `recreate`: Generates `RECREATE TRIGGER ...`.
        *   `create_or_alter`: Generates `CREATE OR ALTER TRIGGER ...`.
        *   `drop`: Generates `DROP TRIGGER ...`.
        *   `comment`: Generates `COMMENT ON TRIGGER ... IS ...`.
        *   `alter` (keyword args): Modifies the trigger definition. Allows changing
            `fire_on` (event string), `active` status, `sequence` position,
            `declare` section (variable declarations), and `code` (trigger body).
            At least one parameter must be provided. Trigger type (DML/DB/DDL)
            cannot be changed via `ALTER`.

    *   System triggers:

        *   `comment`: Adds or removes a descriptive comment.

    Arguments:
        schema: The parent `.Schema` instance.
        attributes: Raw data dictionary fetched from the `RDB$TRIGGERS` row.
    """
    def __init__(self, schema: Schema, attributes: dict[str, Any]):
        super().__init__(schema, attributes)
        self._type_code.append(ObjectType.TRIGGER)
        self._strip_attribute('RDB$TRIGGER_NAME')
        self._strip_attribute('RDB$RELATION_NAME')
        self._strip_attribute('RDB$ENGINE_NAME')
        self._strip_attribute('RDB$ENTRYPOINT')
        self._actions.append('comment')
        if not self.is_sys_object():
            self._actions.extend(['create', 'recreate', 'alter', 'create_or_alter', 'drop'])
        self.__m = list(DMLTrigger.__members__.values())
    def _get_create_sql(self, **params) -> str:
        """Generates the SQL command to CREATE this trigger.

        Includes target object (if DML), active status, event type and time,
        position, and the full PSQL source code.

        Arguments:
            **params: Accepts one optional keyword argument:

                      * `inactive` (bool): If `True`, creates the trigger in an
                        `INACTIVE` state. Defaults to `False` (creates `ACTIVE`).

        Returns:
            The `CREATE TRIGGER` SQL string.

        Raises:
            ValueError: If unexpected parameters are passed.
        """
        self._check_params(params, ['inactive'])
        inactive = params.get('inactive', False)
        result = f'CREATE TRIGGER {self.get_quoted_name()}'
        if self._attributes['RDB$RELATION_NAME']:
            result += f' FOR {self.relation.get_quoted_name()}'
        result += f" {'ACTIVE' if self.active and not inactive else 'INACTIVE'}\n" \
                  f"{self.get_type_as_string()} POSITION {self.sequence}\n" \
                  f"{self.source}"
        return result
    def _get_alter_sql(self, **params) -> str:
        """Generates the SQL command to ALTER this trigger.

        Allows modification of active status, event (within the same type DML/DB/DDL),
        position, and PSQL source code (declarations and body).

        Arguments:
            **params: Accepts optional keyword arguments:

                      * `fire_on` (str): The new event specification string (e.g.,
                        'AFTER INSERT OR UPDATE', 'ON CONNECT'). Must be compatible
                        with the existing trigger type (DML/DB/DDL).
                      * `active` (bool): Set to `True` for `ACTIVE`, `False` for `INACTIVE`.
                      * `sequence` (int): The new execution position.
                      * `declare` (str | list[str] | tuple[str]): New variable declarations
                        (replaces existing). Provided as a single string or list/tuple of lines.
                      * `code` (str | list[str] | tuple[str]): New trigger body code
                        (replaces existing). Provided as a single string or list/tuple of lines.
                        **Required if `declare` is provided.**

        Returns:
            The `ALTER TRIGGER` SQL string.

        Raises:
            ValueError: If no parameters are provided, if `declare` is provided
                        without `code`, if attempting to change trigger type via
                        `fire_on`, or if unexpected parameters are passed.
        """
        self._check_params(params, ['fire_on', 'active', 'sequence', 'declare', 'code'])
        action = params.get('fire_on')
        active = params.get('active')
        sequence = params.get('sequence')
        declare = params.get('declare')
        code = params.get('code')
        #
        header = ''
        if active is not None:
            header += ' ACTIVE' if active else ' INACTIVE'
        if action is not None:
            dbaction = action.upper().startswith('ON ')
            if (dbaction and not self.is_db_trigger()) or (not dbaction and self.is_db_trigger()):
                raise ValueError("Trigger type change is not allowed.")
            header += f'\n  {action}'
        if sequence is not None:
            header += f'\n  POSITION {sequence}'
        #
        if code is not None:
            if declare is None:
                d = ''
            elif isinstance(declare, list | tuple):
                d = ''
                for x in declare:
                    d += f'  {x}\n'
            else:
                d = f'{declare}\n'
            if isinstance(code, list | tuple):
                c = ''
                for x in code:
                    c += f'  {x}\n'
            else:
                c = f'{code}\n'
            body = f'\nAS\n{d}BEGIN\n{c}END'
        else:
            body = ''
        #
        if not (header or body):
            raise ValueError("Header or body definition required.")
        return f'ALTER TRIGGER {self.get_quoted_name()}{header}{body}'
    def _get_drop_sql(self, **params) -> str:
        """Generates the SQL command to DROP this trigger.

        Arguments:
            **params: Accepts no parameters.

        Returns:
            The `DROP TRIGGER` SQL string.

        Raises:
            ValueError: If unexpected parameters are passed.
        """
        self._check_params(params, [])
        return f'DROP TRIGGER {self.get_quoted_name()}'
    def _get_comment_sql(self, **params) -> str:
        """Generates the SQL command to add or remove a COMMENT ON this trigger.

        Arguments:
            **params: Accepts no parameters.

        Returns:
            The `COMMENT ON TRIGGER ... IS ...` SQL string. Sets comment to `NULL` if
            `self.description` is None, otherwise uses the description text
            with proper escaping.

        Raises:
            ValueError: If unexpected parameters are passed.
        """
        comment = 'NULL' if self.description is None \
            else f"'{escape_single_quotes(self.description)}'"
        return f'COMMENT ON TRIGGER {self.get_quoted_name()} IS {comment}'
    def _get_name(self) -> str:
        """Returns the trigger name (`RDB$TRIGGER_NAME`)."""
        return self._attributes['RDB$TRIGGER_NAME']
    def __ru(self, value: IntEnum) -> str:
        """Internal helper: Replaces underscores with spaces in enum names."""
        return value.name.replace('_', ' ')
    def _get_action_type(self, slot: int) -> DMLTrigger:
        """Internal helper: Decodes DML trigger type (INSERT/UPDATE/DELETE) from RDB$TRIGGER_TYPE."""
        if (code := ((self._attributes['RDB$TRIGGER_TYPE'] + 1) >> (slot * 2 - 1)) & 3) > 0:
            return self.__m[code - 1]
        return None
    def is_before(self) -> bool:
        """Checks if the trigger executes `BEFORE` the event (for DML/DDL triggers).

        Returns:
            `True` if it's a BEFORE trigger, `False` otherwise (AFTER or DB trigger).
        """
        return self.time is TriggerTime.BEFORE
    def is_after(self) -> bool:
        """Checks if the trigger executes `AFTER` the event (for DML/DDL triggers).

        Returns:
            `True` if it's an AFTER trigger, `False` otherwise (BEFORE or DB trigger).
        """
        return self.time is TriggerTime.AFTER
    def is_db_trigger(self) -> bool:
        """Checks if this is a database-level trigger (ON CONNECT, etc.).

        Returns:
            `True` if `trigger_type` is `.TriggerType.DB`, `False` otherwise.
        """
        return self.trigger_type is TriggerType.DB
    def is_ddl_trigger(self) -> bool:
        """Checks if this is a DDL trigger (ON CREATE TABLE, etc.).

        Returns:
            `True` if `trigger_type` is `.TriggerType.DDL`, `False` otherwise.
        """
        return self.trigger_type is TriggerType.DDL
    def is_insert(self) -> bool:
        """Checks if this is a DML trigger firing on `INSERT` events.

        Returns:
            `True` if it's an INSERT DML trigger, `False` otherwise.
        """
        return DMLTrigger.INSERT in self.action if self.trigger_type is TriggerType.DML else False
    def is_update(self) -> bool:
        """Checks if this is a DML trigger firing on `UPDATE` events.

        Returns:
            `True` if it's an UPDATE DML trigger, `False` otherwise.
        """
        return DMLTrigger.UPDATE in self.action if self.trigger_type is TriggerType.DML else False
    def is_delete(self) -> bool:
        """Checks if this is a DML trigger firing on `DELETE` events.

        Returns:
            `True` if it's a DELETE DML trigger, `False` otherwise.
        """
        return DMLTrigger.DELETE in self.action if self.trigger_type is TriggerType.DML else False
    def get_type_as_string(self) -> str:
        """Generates a human-readable string describing the trigger's event and time.

        Examples: "AFTER INSERT OR UPDATE", "ON CONNECT", "BEFORE ANY DDL STATEMENT".

        Returns:
            A string representation of the trigger type, action, and time.
        """
        l = []
        if self.is_ddl_trigger():
            l.append(self.time.name)
            l.append('ANY DDL STATEMENT' if self.action == DDLTrigger.ANY
                     else self.__ru(self.action))
        elif self.is_db_trigger():
            l.append('ON ' + self.__ru(self.action))
        else:
            l.append(self.time.name)
            l.append(self._get_action_type(1).name)
            if e:= self._get_action_type(2):
                l.append('OR')
                l.append(e.name)
            if e:= self._get_action_type(3):
                l.append('OR')
                l.append(e.name)
        return ' '.join(l)
    @property
    def relation(self) -> Table | View | None:
        """The `.Table` or `.View` object this trigger is associated with (`RDB$RELATION_NAME`).

        Returns `None` for database-level (DB) or DDL triggers.
        """
        rel = self.schema.all_tables.get(relname := self._attributes['RDB$RELATION_NAME'])
        if not rel:
            rel = self.schema.all_views.get(relname)
        return rel
    @property
    def sequence(self) -> int:
        """The execution sequence (position) number (`RDB$TRIGGER_SEQUENCE`) of the trigger
        relative to other triggers for the same event. Lower numbers execute first."""
        return self._attributes['RDB$TRIGGER_SEQUENCE']
    @property
    def trigger_type(self) -> TriggerType | None:
        """The broad type of the trigger (DML, DB, or DDL).

        Determined by masking the high bits of `RDB$TRIGGER_TYPE`.
        Returns `None` if the type code is unrecognized.
        """
        return TriggerType(self._attributes['RDB$TRIGGER_TYPE'] & (0x3 << 13))
    @property
    def action(self) -> DMLTrigger | DBTrigger | DDLTrigger:
        """The specific event that fires the trigger.

        Returns:
            Depends on trigger type:

            *   For DML triggers: A `.DMLTrigger` flag combination (e.g., `INSERT|UPDATE`).
            *   For DB triggers: A `.DBTrigger` enum member (e.g., `CONNECT`).
            *   For DDL triggers: A `.DDLTrigger` enum member (e.g., `CREATE_TABLE`).
        """
        if self.trigger_type == TriggerType.DDL:
            return DDLTrigger((self._attributes['RDB$TRIGGER_TYPE'] & ~TriggerType.DDL) >> 1)
        if self.trigger_type == TriggerType.DB:
            return DBTrigger(self._attributes['RDB$TRIGGER_TYPE'] & ~TriggerType.DB)
        # DML
        result = DMLTrigger(0)
        for i in range(1, 4):
            if (e := self._get_action_type(i)) is not None:
                result |= e
        return result
    @property
    def time(self) -> TriggerTime:
        """The execution time relative to the event (`.TriggerTime`: BEFORE or AFTER).
        """
        return TriggerTime((self._attributes['RDB$TRIGGER_TYPE'] + (0 if self.is_ddl_trigger() else 1)) & 1)
    @property
    def source(self) -> str | None:
        """The PSQL source code of the trigger body (`RDB$TRIGGER_SOURCE`).
        Returns `None` if source is unavailable."""
        return self._attributes['RDB$TRIGGER_SOURCE']
    @property
    def flags(self) -> int:
        """Internal flags (`RDB$FLAGS`) used by the engine. Interpretation may vary."""
        return self._attributes['RDB$FLAGS']
    @property
    def valid_blr(self) -> bool | None:
        """Indicates if the compiled BLR (Binary Language Representation) of the trigger
        is currently considered valid by the engine (`RDB$VALID_BLR`).

        Returns `True` if valid, `False` if invalid, `None` if the status is unknown
        or the attribute is missing.
        """
        result = self._attributes.get('RDB$VALID_BLR')
        return bool(result) if result is not None else None
    @property
    def engine_name(self) -> str | None:
        """The name of the external engine used, if this is an external trigger
        (`RDB$ENGINE_NAME`). Returns `None` for standard PSQL triggers."""
        return self._attributes.get('RDB$ENGINE_NAME')
    @property
    def entrypoint(self) -> str | None:
        """The entry point function name within the external engine's library, if
        this is an external trigger (`RDB$ENTRYPOINT`). Returns `None` for PSQL triggers."""
        return self._attributes.get('RDB$ENTRYPOINT')
    @property
    def active(self) -> bool:
        """Indicates if the trigger is currently active and will fire on its defined event.

        Based on `RDB$TRIGGER_INACTIVE` (0 = active, 1 = inactive).

        Returns:
            `True` if the trigger is active, `False` if inactive.
        """
        return self._attributes['RDB$TRIGGER_INACTIVE'] == 0

class ProcedureParameter(SchemaItem):
    """Represents an input or output parameter of a stored procedure (`.Procedure`).

    This class holds metadata about a single parameter, including its name,
    data type (derived from a domain, column type, or defined inline), direction
    (input/output), position, nullability, default value, and collation.

    Instances map data primarily from the `RDB$PROCEDURE_PARAMETERS` system table.
    They are accessed via the `.Procedure.input_params` or `.Procedure.output_params`
    properties.

    Supported SQL actions via `.get_sql_for()`:

    *   `comment`: Adds or removes a descriptive comment for the parameter
        (`COMMENT ON PARAMETER proc_name.param_name IS ...`).

    Arguments:
        schema: The parent `.Schema` instance.
        proc: The parent `.Procedure` object this parameter belongs to.
        attributes: Raw data dictionary fetched from the `RDB$PROCEDURE_PARAMETERS` row.
    """
    def __init__(self, schema: Schema, proc: Procedure, attributes: dict[str, Any]):
        super().__init__(schema, attributes)
        self.__proc: Procedure = proc
        self._strip_attribute('RDB$PARAMETER_NAME')
        self._strip_attribute('RDB$PROCEDURE_NAME')
        self._strip_attribute('RDB$FIELD_SOURCE')
        self._strip_attribute('RDB$RELATION_NAME')
        self._strip_attribute('RDB$FIELD_NAME')
        self._strip_attribute('RDB$PACKAGE_NAME')
        self._actions.append('comment')
    def _get_comment_sql(self, **params) -> str:
        """Generates the SQL command to add or remove a COMMENT ON this parameter.

        Arguments:
            **params: Accepts no parameters.

        Returns:
            The `COMMENT ON PARAMETER proc_name.param_name IS ...` SQL string. Sets
            comment to `NULL` if `self.description` is None, otherwise uses the
            description text with proper escaping.

        Raises:
            ValueError: If unexpected parameters are passed.
        """
        comment = 'NULL' if self.description is None \
            else f"'{escape_single_quotes(self.description)}'"
        return f'COMMENT ON PARAMETER {self.procedure.get_quoted_name()}.{self.get_quoted_name()} IS {comment}'
    def _get_name(self) -> str:
        """Returns the parameter name (`RDB$PARAMETER_NAME`)."""
        return self._attributes['RDB$PARAMETER_NAME']
    def get_sql_definition(self) -> str:
        """Generates the SQL string representation of the parameter's definition.

        Used when constructing `CREATE PROCEDURE` statements. Includes name, data type
        (handling TYPE OF variants), nullability, collation, and default value (for inputs).

        Example: `P_ID INTEGER NOT NULL`, `P_NAME VARCHAR(50) COLLATE WIN1252 = NULL`

        Returns:
            A string suitable for use within `CREATE PROCEDURE` parameter lists.
        """
        typedef = self.datatype
        if self.type_from is TypeFrom.DOMAIN:
            typedef = self.domain.get_quoted_name()
        elif self.type_from is TypeFrom.TYPE_OF_DOMAIN:
            typedef = f'TYPE OF {self.domain.get_quoted_name()}'
        elif self.type_from is TypeFrom.TYPE_OF_COLUMN:
            typedef = f'TYPE OF COLUMN {self.column.table.get_quoted_name()}.{self.column.get_quoted_name()}'
        result = f"{self.get_quoted_name()} {typedef}{'' if self.is_nullable() else ' NOT NULL'}"
        c = self.collation
        if c is not None:
            result += f' COLLATE {c.get_quoted_name()}'
        if self.is_input() and self.has_default():
            result += f' = {self.default}'
        return result
    def is_input(self) -> bool:
        """Checks if this is an INPUT parameter.

        Returns:
            `True` if `parameter_type` is `.ParameterType.INPUT`, `False` otherwise.
        """
        return self.parameter_type is ParameterType.INPUT
    def is_nullable(self) -> bool:
        """Checks if the parameter allows `NULL` values.

        Based on `RDB$NULL_FLAG` (0 = nullable, 1 = not nullable).

        Returns:
            `True` if the parameter allows `NULL`, `False` otherwise.
        """
        return not bool(self._attributes.get('RDB$NULL_FLAG'))
    def has_default(self) -> bool:
        """Checks if the parameter has a `DEFAULT` value defined.

        Based on the presence of `RDB$DEFAULT_SOURCE`. Only applicable to input parameters.

        Returns:
            `True` if a default value source exists, `False` otherwise.
        """
        return bool(self._attributes.get('RDB$DEFAULT_SOURCE'))
    def is_packaged(self) -> bool:
        """Checks if the parameter belongs to a procedure defined within a package.

        Based on the presence of `RDB$PACKAGE_NAME`.

        Returns:
            `True` if part of a packaged procedure, `False` otherwise.
        """
        return bool(self._attributes.get('RDB$PACKAGE_NAME'))
    @property
    def procedure(self) -> Procedure:
        """The parent `.Procedure` object this parameter belongs to."""
        return self.schema.all_procedures.get(self._attributes['RDB$PROCEDURE_NAME'])
    @property
    def sequence(self) -> int:
        """The 0-based sequence (position) number (`RDB$PARAMETER_NUMBER`) of the parameter
        within its input or output list."""
        return self._attributes['RDB$PARAMETER_NUMBER']
    @property
    def domain(self) -> Domain:
        """The underlying `.Domain` object that defines this parameter's base data type
        and constraints (`RDB$FIELD_SOURCE`)."""
        return self.schema.all_domains.get(self._attributes['RDB$FIELD_SOURCE'])
    @property
    def parameter_type(self) -> ParameterType:
        """The direction of the parameter ( INPUT or OUTPUT).

        Derived from `RDB$PARAMETER_TYPE` (0=Input, 1=Output).
        """
        return ParameterType(self._attributes['RDB$PARAMETER_TYPE'])
    @property
    def datatype(self) -> str:
        """A string representation of the parameter's complete SQL data type definition.

        Handles derivation from domain (`DOMAIN name`), column (`TYPE OF COLUMN t.c`),
        or base type (`INTEGER`, `VARCHAR(50)`).
        """
        if self.type_from is TypeFrom.DATATYPE:
            return self.domain.datatype
        if self.type_from is TypeFrom.DOMAIN:
            return self.domain.get_quoted_name()
        if self.type_from is TypeFrom.TYPE_OF_DOMAIN:
            return f'TYPE OF {self.domain.get_quoted_name()}'
        # TypeFrom.TYPE_OF_COLUMN
        table = self.schema.all_tables.get(self._attributes['RDB$RELATION_NAME'])
        return f"TYPE OF COLUMN {table.get_quoted_name()}." \
               f"{table.columns.get(self._attributes['RDB$FIELD_NAME']).get_quoted_name()}"
    @property
    def type_from(self) -> TypeFrom:
        """Indicates the source of the parameter's data type definition (`.TypeFrom`).

        Determined by `RDB$PARAMETER_MECHANISM`:

        *   `BY_VALUE` (0) implies DATATYPE or DOMAIN.
        *   `BY_REFERENCE` (1) implies TYPE OF DOMAIN or TYPE OF COLUMN.
        """
        m = self.mechanism
        if m is None:
            return TypeFrom.DATATYPE
        if m == Mechanism.BY_VALUE:
            return TypeFrom.DATATYPE if self.domain.is_sys_object() else TypeFrom.DOMAIN
        if m == Mechanism.BY_REFERENCE:
            if self._attributes.get('RDB$RELATION_NAME') is None:
                return TypeFrom.TYPE_OF_DOMAIN
            return TypeFrom.TYPE_OF_COLUMN
        raise Error(f"Unknown parameter mechanism code: {m}")
    @property
    def default(self) -> str | None:
        """The `DEFAULT` value expression string defined for the parameter (`RDB$DEFAULT_SOURCE`).

        Applies only to input parameters. Returns the expression string (e.g., '0', "'PENDING'")
        or `None` if no default is defined. The leading '= ' or 'DEFAULT ' keyword is removed.
        """
        if result := self._attributes.get('RDB$DEFAULT_SOURCE'):
            if result.upper().startswith('= '):
                result = result[2:]
            elif result.upper().startswith('DEFAULT '):
                result = result[8:]
        return result
    @property
    def collation(self) -> Collation | None:
        """The specific `.Collation` object applied to this parameter (`RDB$COLLATION_ID`),
        if applicable (for character types) and different from the domain default.

        Returns `None` if the parameter type does not support collation, if the
        default collation is used, or if domain info is unavailable.
        """
        return (None if (cid := self._attributes.get('RDB$COLLATION_ID')) is None
                else self.schema.get_collation_by_id(self.domain._attributes['RDB$CHARACTER_SET_ID'], cid))
    @property
    def mechanism(self) -> Mechanism | None:
        """The mechanism used for passing the parameter (`.Mechanism`), derived from
        `RDB$PARAMETER_MECHANISM`.

        Indicates if passed by value or reference, relevant for type determination.
        Returns `None` if the mechanism code is unrecognized or missing.
        """
        return Mechanism(code) if (code := self._attributes.get('RDB$PARAMETER_MECHANISM')) is not None else None
    @property
    def column(self) -> TableColumn | None:
        """If the parameter type is derived using `TYPE OF COLUMN`, this property
        returns the source `.TableColumn` object.

        Based on `RDB$RELATION_NAME` and `RDB$FIELD_NAME`. Returns `None` otherwise.
        """
        return (None if (rname := self._attributes.get('RDB$RELATION_NAME')) is None
                else self.schema.all_tables.get(rname).columns.get(self._attributes['RDB$FIELD_NAME']))
    @property
    def package(self) -> Package | None:
        """The `.Package` object this parameter's procedure belongs to, if any.

        Based on `RDB$PACKAGE_NAME`. Returns `None` if the procedure is standalone.
        """
        return self.schema.packages.get(self._attributes.get('RDB$PACKAGE_NAME'))

class Procedure(SchemaItem):
    """Represents a stored procedure defined in the database.

    Stored procedures encapsulate reusable PSQL logic, accepting input parameters
    and optionally returning output parameters (for selectable procedures) or
    single values (legacy functions implemented as procedures). They can be
    standalone or part of a `.Package`.

    Instances map data primarily from the `RDB$PROCEDURES` system table. Associated
    parameters are fetched from `RDB$PROCEDURE_PARAMETERS`. Procedures are
    accessed via `.Schema.procedures`, `.Schema.sys_procedures`, `.Schema.all_procedures`,
    or `.Package.procedures`.

    Supported SQL actions via `.get_sql_for()`:

    *   User-defined, standalone procedures:

        *   `create` (optional keyword arg `no_code`: bool=False): Generates
            `CREATE PROCEDURE ...` statement, including parameter definitions
            and the PSQL source code (unless `no_code=True`, which generates
            an empty `BEGIN END` block).
        *   `recreate` (optional keyword arg `no_code`: bool=False): Generates
            `RECREATE PROCEDURE ...`.
        *   `create_or_alter` (optional keyword arg `no_code`: bool=False): Generates
            `CREATE OR ALTER PROCEDURE ...`.
        *   `drop`: Generates `DROP PROCEDURE ...`.
        *   `comment`: Generates `COMMENT ON PROCEDURE ... IS ...`.
        *   `alter` (keyword args): Modifies the procedure. Allows changing
            input/output parameter lists, variable declarations, and code body.

            *   `input` (str | list[str] | tuple[str], optional): New list of input
                parameter definitions (full SQL like 'p_id INTEGER').
            *   `output` (str | list[str] | tuple[str], optional): New list of output
                parameter definitions (for `RETURNS (...)`).
            *   `declare` (str | list[str] | tuple[str], optional): New variable declarations.
            *   `code` (str | list[str] | tuple[str], **required**): New procedure body code.

    *   System procedures or packaged procedures:

        *   `comment`: Adds or removes a descriptive comment.
        *   Note: Packaged procedures are typically managed via `ALTER PACKAGE`.

    Arguments:
        schema: The parent `.Schema` instance.
        attributes: Raw data dictionary fetched from the `RDB$PROCEDURES` row.
    """
    def __init__(self, schema: Schema, attributes: dict[str, Any]):
        super().__init__(schema, attributes)
        self._type_code.append(ObjectType.PROCEDURE)
        self.__input_params = self.__output_params = None
        self._strip_attribute('RDB$PROCEDURE_NAME')
        self._strip_attribute('RDB$OWNER_NAME')
        self._strip_attribute('RDB$SECURITY_CLASS')
        self._strip_attribute('RDB$ENGINE_NAME')
        self._strip_attribute('RDB$ENTRYPOINT')
        self._strip_attribute('RDB$PACKAGE_NAME')
        self.__colsql = "select RDB$PARAMETER_NAME, RDB$PROCEDURE_NAME, RDB$PARAMETER_NUMBER," \
                        "RDB$PARAMETER_TYPE, RDB$FIELD_SOURCE, RDB$DESCRIPTION, RDB$SYSTEM_FLAG," \
                        "RDB$DEFAULT_SOURCE, RDB$COLLATION_ID, RDB$NULL_FLAG, RDB$PARAMETER_MECHANISM," \
                        "RDB$FIELD_NAME, RDB$RELATION_NAME, RDB$PACKAGE_NAME " \
                        "from rdb$procedure_parameters where rdb$procedure_name = ? " \
                        "and rdb$parameter_type = ? order by rdb$parameter_number"
        self._actions.append('comment')
        if not self.is_sys_object():
            self._actions.extend(['create', 'recreate', 'alter', 'create_or_alter', 'drop'])
    def _get_create_sql(self, **params) -> str:
        """Generates the SQL command to CREATE this procedure.

        Includes input parameter list `(...)`, output parameter list `RETURNS (...)`
        if applicable, and the `AS BEGIN ... END` block with PSQL source code.

        Arguments:
            **params: Accepts one optional keyword argument:

                      * `no_code` (bool): If `True`, generates an empty `BEGIN END`
                        block instead of the actual procedure source code. Useful for
                        creating procedure headers first. Defaults to `False`.

        Returns:
            The `CREATE PROCEDURE` SQL string.

        Raises:
            ValueError: If unexpected parameters are passed.
        """
        self._check_params(params, ['no_code'])
        no_code = params.get('no_code')
        result = f'CREATE PROCEDURE {self.get_quoted_name()}'
        if self.has_input():
            if self._attributes['RDB$PROCEDURE_INPUTS'] == 1:
                result += f' ({self.input_params[0].get_sql_definition()})\n'
            else:
                result += ' (\n'
                for p in self.input_params:
                    result += f"  {p.get_sql_definition()}" \
                              f"{'' if p.sequence+1 == self._attributes['RDB$PROCEDURE_INPUTS'] else ','}\n"
                result += ')\n'
        else:
            result += '\n'
        if self.has_output():
            if self._attributes['RDB$PROCEDURE_OUTPUTS'] == 1:
                result += f'RETURNS ({self.output_params[0].get_sql_definition()})\n'
            else:
                result += 'RETURNS (\n'
                for p in self.output_params:
                    result += f"  {p.get_sql_definition()}" \
                              f"{'' if p.sequence+1 == self._attributes['RDB$PROCEDURE_OUTPUTS'] else ','}\n"
                result += ')\n'
        return result+'AS\n'+(('BEGIN\nEND' if self.proc_type != 1
                               else 'BEGIN\n  SUSPEND;\nEND')
                              if no_code else self.source)
    def _get_alter_sql(self, **params) -> str:
        """Generates the SQL command to ALTER this procedure.

        Allows modification of input/output parameters, declarations, and code body.
        The `code` parameter is required.

        Arguments:
            **params: Accepts optional keyword arguments:

                      * `input` (str | list[str] | tuple[str], optional): New definition(s)
                        for input parameters (full SQL like 'p_id INTEGER'). Replaces existing.
                      * `output` (str | list[str] | tuple[str], optional): New definition(s)
                        for output parameters. Replaces existing.
                      * `declare` (str | list[str] | tuple[str], optional): New variable
                        declarations section. Replaces existing.
                      * `code` (str | list[str] | tuple[str], **required**): The new PSQL
                        code for the procedure body (between BEGIN and END).

        Returns:
            The `ALTER PROCEDURE` SQL string.

        Raises:
            ValueError: If the required `code` parameter is missing, if parameter
                        types are invalid, or if unexpected parameters are passed.
        """
        self._check_params(params, ['input', 'output', 'declare', 'code'])
        inpars = params.get('input')
        outpars = params.get('output')
        declare = params.get('declare')
        code = params.get('code')
        if 'code' not in params:
            raise ValueError("Missing required parameter: 'code'.")
        #
        header = ''
        if inpars is not None:
            if isinstance(inpars, list | tuple):
                numpars = len(inpars)
                if numpars == 1:
                    header = f' ({inpars})\n'
                else:
                    header = ' (\n'
                    i = 1
                    for p in inpars:
                        header += f"  {p}{'' if i == numpars else ','}\n"
                        i += 1
                    header += ')\n'
            else:
                header = f' ({inpars})\n'
        #
        if outpars is not None:
            if not header:
                header += '\n'
            if isinstance(outpars, list | tuple):
                numpars = len(outpars)
                if numpars == 1:
                    header += f'RETURNS ({outpars})\n'
                else:
                    header += 'RETURNS (\n'
                    i = 1
                    for p in outpars:
                        header += f"  {p}{'' if i == numpars else ','}\n"
                        i += 1
                    header += ')\n'
            else:
                header += f'RETURNS ({outpars})\n'
        #
        if code:
            if declare is None:
                d = ''
            elif isinstance(declare, list | tuple):
                d = ''
                for x in declare:
                    d += f'  {x}\n'
            else:
                d = f'{declare}\n'
            if isinstance(code, list | tuple):
                c = ''
                for x in code:
                    c += f'  {x}\n'
            else:
                c = f'{code}\n'
            h = '' if header else '\n'
            body = f"{h}AS\n{d}BEGIN\n{c}END"
        else:
            h = '' if header else '\n'
            body = f"{h}AS\nBEGIN\nEND"
        #
        return f'ALTER PROCEDURE {self.get_quoted_name()}{header}{body}'
    def _get_drop_sql(self, **params) -> str:
        """Generates the SQL command to DROP this procedure.

        Arguments:
            **params: Accepts no parameters.

        Returns:
            The `DROP PROCEDURE` SQL string.

        Raises:
            ValueError: If unexpected parameters are passed.
        """
        self._check_params(params, [])
        return f'DROP PROCEDURE {self.get_quoted_name()}'
    def _get_comment_sql(self, **params) -> str:
        """Generates the SQL command to add or remove a COMMENT ON this procedure.

        Arguments:
            **params: Accepts no parameters.

        Returns:
            The `COMMENT ON PROCEDURE ... IS ...` SQL string. Sets comment to `NULL` if
            `self.description` is None, otherwise uses the description text
            with proper escaping.

        Raises:
            ValueError: If unexpected parameters are passed.
        """
        comment = 'NULL' if self.description is None \
            else f"'{escape_single_quotes(self.description)}'"
        return f'COMMENT ON PROCEDURE {self.get_quoted_name()} IS {comment}'
    def _get_name(self) -> str:
        """Returns the procedure name (`RDB$PROCEDURE_NAME`)."""
        return self._attributes['RDB$PROCEDURE_NAME']
    def get_param(self, name: str) -> ProcedureParameter | None:
        """Retrieves a specific input or output parameter by its name.

        Searches output parameters first, then input parameters.

        Arguments:
            name: The case-sensitive name of the parameter to find.

        Returns:
            The matching `.ProcedureParameter` object, or `None` if no parameter
            with that name exists.
        """
        for p in self.output_params:
            if p.name == name:
                return p
        for p in self.input_params:
            if p.name == name:
                return p
        return None
    def has_input(self) -> bool:
        """Checks if the procedure defines any input parameters.

        Based on `RDB$PROCEDURE_INPUTS` > 0.

        Returns:
            `True` if input parameters exist, `False` otherwise.
        """
        return bool(self._attributes['RDB$PROCEDURE_INPUTS'])
    def has_output(self) -> bool:
        """Checks if the procedure defines any output parameters (`RETURNS`).

        Based on `RDB$PROCEDURE_OUTPUTS` > 0.

        Returns:
            `True` if output parameters exist, `False` otherwise.
        """
        return bool(self._attributes['RDB$PROCEDURE_OUTPUTS'])
    def is_packaged(self) -> bool:
        """Checks if the procedure is defined within a package.

        Based on the presence of `RDB$PACKAGE_NAME`.

        Returns:
            `True` if part of a package, `False` otherwise.
        """
        return bool(self._attributes.get('RDB$PACKAGE_NAME'))
    @property
    def id(self) -> int:
        """The internal numeric ID (`RDB$PROCEDURE_ID`) assigned to the procedure."""
        return self._attributes['RDB$PROCEDURE_ID']
    @property
    def source(self) -> str | None:
        """The PSQL source code of the procedure body (`RDB$PROCEDURE_SOURCE`).
        Returns `None` if the source is unavailable."""
        return self._attributes['RDB$PROCEDURE_SOURCE']
    @property
    def security_class(self) -> str | None:
        """The security class name associated with this procedure, if any (`RDB$SECURITY_CLASS`).
        Returns `None` if not set."""
        return self._attributes['RDB$SECURITY_CLASS']
    @property
    def owner_name(self) -> str:
        """The user name of the procedure's owner/creator (`RDB$OWNER_NAME`)."""
        return self._attributes['RDB$OWNER_NAME']
    @property
    def input_params(self) -> DataList[ProcedureParameter]:
        """A lazily-loaded `.DataList` of the procedure's input `.ProcedureParameter` objects.

        Ordered by position (`RDB$PARAMETER_NUMBER`). Returns an empty list if the
        procedure has no input parameters. Fetched from `RDB$PROCEDURE_PARAMETERS`.
        """
        if self.__input_params is None:
            if self.has_input():
                self.__input_params = DataList((ProcedureParameter(self.schema, self, row) for row in
                                                  self.schema._select(self.__colsql, (self.name, 0))),
                                                 ProcedureParameter, 'item.name')
            else:
                self.__input_params = DataList()
            self.__input_params.freeze()
        return self.__input_params
    @property
    def output_params(self) -> DataList[ProcedureParameter]:
        """A lazily-loaded `.DataList` of the procedure's output `.ProcedureParameter` objects.

        Ordered by position (`RDB$PARAMETER_NUMBER`). Returns an empty list if the
        procedure has no output parameters (i.e., does not have a `RETURNS` clause).
        Fetched from `RDB$PROCEDURE_PARAMETERS`.
        """
        if self.__output_params is None:
            if self.has_output():
                self.__output_params = DataList((ProcedureParameter(self.schema, self, row) for row in
                                                   self.schema._select(self.__colsql, (self.name, 1))),
                                                  ProcedureParameter, 'item.name')
            else:
                self.__output_params = DataList()
            self.__output_params.freeze()
        return self.__output_params
    @property
    def privileges(self) -> DataList[Privilege]:
        """A `.DataList` of `EXECUTE` `.Privilege` objects granted on this procedure.

        Filters the main `Schema.privileges` collection for this procedure's name
        and type (`ObjectType.PROCEDURE`).
        """
        return self.schema.privileges.extract(lambda p: ((p.subject_name == self.name) and
                                                         (p.subject_type in self._type_code)),
                                                     copy=True)
    @property
    def proc_type(self) -> ProcedureType:
        """The type of the procedure (LEGACY, SELECTABLE, EXECUTABLE).

        Derived from `RDB$PROCEDURE_TYPE`. Defaults to LEGACY if the attribute is missing.
        """
        return ProcedureType(self._attributes.get('RDB$PROCEDURE_TYPE', 0))
    @property
    def valid_blr(self) -> bool | None:
        """Indicates if the compiled BLR of the procedure is currently valid (`RDB$VALID_BLR`).

        Returns `True` if valid, `False` if invalid, `None` if status is unknown/missing.
        """
        return bool(result) if (result := self._attributes.get('RDB$VALID_BLR')) is not None else None
    @property
    def engine_name(self) -> str | None:
        """The name of the external engine used, if this is an external procedure
        (`RDB$ENGINE_NAME`). Returns `None` for standard PSQL procedures."""
        return self._attributes.get('RDB$ENGINE_NAME')
    @property
    def entrypoint(self) -> str | None:
        """The entry point function name within the external engine's library, if
        this is an external procedure (`RDB$ENTRYPOINT`). Returns `None` for PSQL procedures."""
        return self._attributes.get('RDB$ENTRYPOINT')
    @property
    def package(self) -> Package | None:
        """The `.Package` object this procedure belongs to, if any (`RDB$PACKAGE_NAME`).

        Returns `None` if the procedure is standalone.
        """
        return self.schema.packages.get(self._attributes.get('RDB$PACKAGE_NAME'))
    @property
    def privacy(self) -> Privacy:
        """The privacy flag (PUBLIC or PRIVATE) for packaged procedures.

        Derived from `RDB$PRIVATE_FLAG`. Returns `None` if not a packaged procedure
        or flag is missing.
        """
        return Privacy(code) if (code := self._attributes.get('RDB$PRIVATE_FLAG')) is not None else None

class Role(SchemaItem):
    """Represents a database role, a named collection of privileges.

    Roles simplify privilege management by allowing privileges to be granted to the
    role, and then the role granted to users or other roles. `RDB$ADMIN` is a
    predefined system role.

    Instances map data primarily from the `RDB$ROLES` system table. They are
    accessed via `.Schema.roles`.

    Supported SQL actions via `.get_sql_for()`:

    *   User-defined roles:

        *   `create`: Generates `CREATE ROLE role_name`.
        *   `drop`: Generates `DROP ROLE role_name`.
        *   `comment`: Generates `COMMENT ON ROLE role_name IS ...`.

    *   System roles (like `RDB$ADMIN`):

        *   `comment`: Adds or removes a descriptive comment.

    Arguments:
        schema: The parent `.Schema` instance.
        attributes: Raw data dictionary fetched from the `RDB$ROLES` row.
    """
    def __init__(self, schema: Schema, attributes: dict[str, Any]):
        super().__init__(schema, attributes)
        self._type_code.append(ObjectType.ROLE)
        self._strip_attribute('RDB$ROLE_NAME')
        self._strip_attribute('RDB$OWNER_NAME')
        self._strip_attribute('RDB$SECURITY_CLASS')
        self._actions.append('comment')
        if not self.is_sys_object():
            self._actions.extend(['create', 'drop'])
    def _get_create_sql(self, **params) -> str:
        """Generates the SQL command to CREATE this role.

        Arguments:
            **params: Accepts no parameters.

        Returns:
            The `CREATE ROLE` SQL string.

        Raises:
            ValueError: If unexpected parameters are passed.
        """
        self._check_params(params, [])
        return f'CREATE ROLE {self.get_quoted_name()}'
    def _get_drop_sql(self, **params) -> str:
        """Generates the SQL command to DROP this role.

        Arguments:
            **params: Accepts no parameters.

        Returns:
            The `DROP ROLE` SQL string.

        Raises:
            ValueError: If unexpected parameters are passed.
            Error: If attempting to drop a system role.
        """
        self._check_params(params, [])
        if self.is_sys_object():
            raise Error("Cannot drop system role")
        return f'DROP ROLE {self.get_quoted_name()}'
    def _get_comment_sql(self, **params) -> str:
        """Generates the SQL command to add or remove a COMMENT ON this role.

        Arguments:
            **params: Accepts no parameters.

        Returns:
            The `COMMENT ON ROLE ... IS ...` SQL string. Sets comment to `NULL` if
            `self.description` is None, otherwise uses the description text
            with proper escaping.

        Raises:
            ValueError: If unexpected parameters are passed.
        """
        comment = 'NULL' if self.description is None \
            else f"'{escape_single_quotes(self.description)}'"
        return f'COMMENT ON ROLE {self.get_quoted_name()} IS {comment}'
    def _get_name(self) -> str:
        """Returns the role name (`RDB$ROLE_NAME`)."""
        return self._attributes['RDB$ROLE_NAME']
    @property
    def owner_name(self) -> str:
        """The user name of the role's owner/creator (`RDB$OWNER_NAME`)."""
        return self._attributes['RDB$OWNER_NAME']
    @property
    def security_class(self) -> str | None:
        """The security class name associated with this role, if any (`RDB$SECURITY_CLASS`).
        Returns `None` if not set."""
        return self._attributes.get('RDB$SECURITY_CLASS')
    @property
    def privileges(self) -> DataList[Privilege]:
        """A `.DataList` of all `.Privilege` objects *granted to* this role.

        Filters the main `.Schema.privileges` collection where this role is the
        grantee (`RDB$USER`). This includes object privileges (SELECT, INSERT, etc.)
        and potentially membership in other roles granted TO this role.

        Returns:
            A list of privileges held by this role.
        """
        return self.schema.privileges.extract(lambda p: ((p.user_name == self.name) and
                                                         (p.user_type in self._type_code)),
                                                     copy=True)

class FunctionArgument(SchemaItem):
    """Represents an argument or the return value of a User-Defined Function (`.Function`).

    This class holds metadata about a single function argument/return value, including
    its name, data type, position, passing mechanism (e.g., by value,
    by descriptor), and nullability/default value (for PSQL function arguments).

    Instances map data primarily from the `RDB$FUNCTION_ARGUMENTS` system table.
    They are accessed via the `Function.arguments` or `Function.returns` properties.

    This class itself does not support any direct SQL actions via `get_sql_for()`.
    Its definition is part of the `DECLARE EXTERNAL FUNCTION` or `CREATE FUNCTION` statement.

    Arguments:
        schema: The parent `.Schema` instance.
        function: The parent `.Function` object this argument belongs to.
        attributes: Raw data dictionary fetched from the `RDB$FUNCTION_ARGUMENTS` row.
    """
    def __init__(self, schema: Schema, function: Function, attributes: dict[str, Any]):
        super().__init__(schema, attributes)
        self._type_code.append(ObjectType.UDF)
        self.__function = function
        self._strip_attribute('RDB$FUNCTION_NAME')
        self._strip_attribute('RDB$PACKAGE_NAME')
        self._strip_attribute('RDB$ARGUMENT_NAME')
        self._strip_attribute('RDB$FIELD_SOURCE')
        self._strip_attribute('RDB$DEFAULT_SOURCE')
        self._strip_attribute('RDB$FIELD_NAME')
        self._strip_attribute('RDB$RELATION_NAME')
        self._strip_attribute('RDB$DESCRIPTION')
    def _get_name(self) -> str:
        """Returns the argument name (`RDB$ARGUMENT_NAME`)."""
        return self.argument_name or f'{self.function.name}_{self.position}'
    def get_sql_definition(self) -> str:
        """Generates the SQL string representation of the argument's definition.

        Used when constructing `DECLARE EXTERNAL FUNCTION` or `CREATE FUNCTION` statements.
        Format varies depending on whether it's an external UDF or a PSQL function,
        and whether it's an input argument or the return value.

        Examples:

        *   External UDF input: `INTEGER BY DESCRIPTOR`
        *   External UDF return: `VARCHAR(100) CHARACTER SET WIN1252 BY DESCRIPTOR FREE_IT`
        *   PSQL function input: `P_ID INTEGER NOT NULL = 0`
        *   PSQL function return: `VARCHAR(50) CHARACTER SET UTF8 COLLATE UNICODE_CI`

        Returns:
            A string suitable for use within function DDL statements.
        """
        if self.function.is_external():
            return f"{self.datatype}" \
                   f"{' BY DESCRIPTOR' if self.is_by_descriptor() else ''}" \
                   f"{' BY VALUE' if self.is_by_value() and self.is_returning() else ''}"
        result = f"{self.get_quoted_name()+' ' if not self.is_returning() else ''}" \
                 f"{self.datatype}{'' if self.is_nullable() else ' NOT NULL'}"
        if (c := self.collation) is not None:
            result += f' COLLATE {c.get_quoted_name()}'
        if not self.is_returning() and self.has_default():
            result += f' = {self.default}'
        return result
    def is_by_value(self) -> bool:
        """Checks if the argument is passed by value (`RDB$MECHANISM = 0`).

        Returns:
            `True` if passed by value, `False` otherwise.
        """
        return self.mechanism == Mechanism.BY_VALUE
    def is_by_reference(self) -> bool:
        """Checks if the argument is passed by reference (`RDB$MECHANISM = 1 or 5`).

        Includes standard reference and reference with NULL support.

        Returns:
            `True` if passed by reference, `False` otherwise.
        """
        return self.mechanism in (Mechanism.BY_REFERENCE, Mechanism.BY_REFERENCE_WITH_NULL)
    def is_by_descriptor(self, *, any_desc: bool=False) -> bool:
        """Checks if the argument is passed using a descriptor mechanism.

        Arguments:
            any_desc: If `True`, checks for any descriptor type (`BY_VMS_DESCRIPTOR`,
                      `BY_ISC_DESCRIPTOR`, `BY_SCALAR_ARRAY_DESCRIPTOR`). If `False`
                      (default), specifically checks only for `BY_VMS_DESCRIPTOR` (legacy).

        Returns:
            `True` if the argument uses the specified descriptor mechanism(s), `False` otherwise.
        """
        return self.mechanism in (Mechanism.BY_VMS_DESCRIPTOR, Mechanism.BY_ISC_DESCRIPTOR,
                                  Mechanism.BY_SCALAR_ARRAY_DESCRIPTOR) if any_desc \
               else self.mechanism == Mechanism.BY_VMS_DESCRIPTOR
    def is_with_null(self) -> bool:
        """Checks if the argument is passed by reference with explicit NULL indicator support
        (`RDB$MECHANISM = 5`).

        Returns:
            `True` if mechanism is `BY_REFERENCE_WITH_NULL`, `False` otherwise.
        """
        return self.mechanism is Mechanism.BY_REFERENCE_WITH_NULL
    def is_freeit(self) -> bool:
        """Checks if the engine should free memory allocated for a `RETURNS BY DESCRIPTOR` value.

        Indicated by a negative value in `RDB$MECHANISM`.

        Returns:
            `True` if the `FREE_IT` convention applies, `False` otherwise.
        """
        return self._attributes['RDB$MECHANISM'] < 0
    def is_returning(self) -> bool:
        """Checks if this argument represents the function's return value.

        Determined by comparing `position` with the function's `RDB$RETURN_ARGUMENT`.

        Returns:
            `True` if this is the return argument/value, `False` otherwise.
        """
        return self.position == self.function._attributes['RDB$RETURN_ARGUMENT']
    def is_nullable(self) -> bool:
        """Checks if the argument/return value allows `NULL` (relevant for PSQL functions).

        Based on `RDB$NULL_FLAG` (0 = nullable, 1 = not nullable).

        Returns:
            `True` if `NULL` is allowed, `False` otherwise.
        """
        return not bool(self._attributes.get('RDB$NULL_FLAG'))
    def has_default(self) -> bool:
        """Checks if the argument has a `DEFAULT` value defined (relevant for PSQL function inputs).

        Based on the presence of `RDB$DEFAULT_SOURCE`.

        Returns:
            `True` if a default value source exists, `False` otherwise.
        """
        return bool(self._attributes.get('RDB$DEFAULT_SOURCE'))
    def is_packaged(self) -> bool:
        """Checks if the argument belongs to a function defined within a package.

        Based on the presence of `RDB$PACKAGE_NAME`.

        Returns:
            `True` if part of a packaged function, `False` otherwise.
        """
        return bool(self._attributes.get('RDB$PACKAGE_NAME'))
    @property
    def function(self) -> Function:
        """The parent `.Function` object this argument belongs to."""
        return self.__function
    @property
    def position(self) -> int:
        """The 1-based position (`RDB$ARGUMENT_POSITION`) of the argument in the function
        signature (or the return argument position number)."""
        return self._attributes['RDB$ARGUMENT_POSITION']
    @property
    def mechanism(self) -> Mechanism | None:
        """The mechanism (`.Mechanism` enum) used for passing the argument.

        Derived from the absolute value of `RDB$MECHANISM`. See `is_freeit()` for sign meaning.
        Returns `None` if the mechanism code is unrecognized or missing.
        """
        return None if (x := self._attributes['RDB$MECHANISM']) is None else Mechanism(abs(x))
    @property
    def field_type(self) -> FieldType | None:
        """The base data type code (`.FieldType`) of the argument (`RDB$FIELD_TYPE`).

        Returns `None` if the type code is missing or zero (may occur for PSQL params
        relying solely on domain/column type).
        """
        return None if (code := self._attributes['RDB$FIELD_TYPE']) in (None, 0) else FieldType(code)
    @property
    def length(self) -> int:
        """The defined length (`RDB$FIELD_LENGTH`) in bytes for types like `CHAR`, `VARCHAR`,
        `BLOB`."""
        return self._attributes['RDB$FIELD_LENGTH']
    @property
    def scale(self) -> int:
        """Negative number representing the scale of NUMBER and DECIMAL argument."""
        return self._attributes['RDB$FIELD_SCALE']
    @property
    def precision(self) -> int | None:
        """The precision (`RDB$FIELD_PRECISION`) for numeric/decimal/float types.
        Returns `None` if not applicable."""
        return self._attributes['RDB$FIELD_PRECISION']
    @property
    def sub_type(self) -> FieldSubType | None:
        """The field sub-type code (`RDB$FIELD_SUB_TYPE`).

        Returns a `.FieldSubType` enum member (e.g., `BINARY`, `TEXT`, `NUMERIC`, `DECIMAL`)
        if recognized, the raw integer code otherwise, or `None` if missing.
        """
        return None if (x := self._attributes['RDB$FIELD_SUB_TYPE']) is None else FieldSubType(x)
    @property
    def character_length(self) -> int:
        """Length in characters (`RDB$CHARACTER_LENGTH`) for `CHAR`, `VARCHAR`, `CSTRING`."""
        return self._attributes['RDB$CHARACTER_LENGTH']
    @property
    def character_set(self) -> CharacterSet | None:
        """The `.CharacterSet` object (`RDB$CHARACTER_SET_ID`) for character/text types.
        Returns `None` otherwise."""
        return self.schema.get_charset_by_id(self._attributes['RDB$CHARACTER_SET_ID'])
    @property
    def datatype(self) -> str:
        """A string representation of the argument's complete SQL data type definition.

        Handles differences between external UDF types (based on `field_type`, `length`, etc.)
        and PSQL function types (potentially derived from domain or column).
        """
        if self.field_type is None:
            # FB3 PSQL function, datatype defined via internal domain
            if self.type_from is TypeFrom.DATATYPE:
                return self.domain.datatype
            if self.type_from is TypeFrom.DOMAIN:
                return self.domain.get_quoted_name()
            if self.type_from is TypeFrom.TYPE_OF_DOMAIN:
                return f'TYPE OF {self.domain.get_quoted_name()}'
            # TypeFrom.TYPE_OF_COLUMN
            table = self.schema.all_tables.get(self._attributes['RDB$RELATION_NAME'])
            return f"TYPE OF COLUMN {table.get_quoted_name()}." \
                   f"{table.columns.get(self._attributes['RDB$FIELD_NAME']).get_quoted_name()}"
        else:
            # Classic external UDF
            l = []
            precision_known = False
            if self.field_type in (FieldType.SHORT, FieldType.LONG, FieldType.INT64):
                if self.precision is not None:
                    if self.sub_type in (FieldSubType.NUMERIC, FieldSubType.DECIMAL):
                        l.append(f'{INTEGRAL_SUBTYPES[self.sub_type]}({self.precision}, {-self.scale})')
                        precision_known = True
            if not precision_known:
                if (self.field_type == FieldType.SHORT) and (self.scale < 0):
                    l.append(f'NUMERIC(4, {-self.scale})')
                elif (self.field_type == FieldType.LONG) and (self.scale < 0):
                    l.append(f'NUMERIC(9, {-self.scale})')
                elif (self.field_type == FieldType.DOUBLE) and (self.scale < 0):
                    l.append(f'NUMERIC(15, {-self.scale})')
                else:
                    l.append(COLUMN_TYPES[self.field_type])
            if self.field_type in (FieldType.TEXT, FieldType.VARYING, FieldType.CSTRING):
                l.append(f'({self.length if (self.character_length is None) else self.character_length})')
            if self.field_type == FieldType.BLOB:
                if self.sub_type >= 0 and self.sub_type <= len(self.schema.field_subtypes):
                    if self.sub_type > 0:
                        l.append(f' SUB_TYPE {self.schema._field_subtypes_[self.sub_type]}')
                else:
                    l.append(f' SUB_TYPE {self.sub_type}')
            if self.field_type in (FieldType.TEXT, FieldType.VARYING, FieldType.CSTRING,
                                   FieldType.BLOB):
                if self._attributes['RDB$CHARACTER_SET_ID'] is not None and \
                  (self.character_set.name != self.schema.default_character_set.name):
                    l.append(f' CHARACTER SET {self.character_set.name}')
            return ''.join(l)
    @property
    def type_from(self) -> TypeFrom | None:
        """Indicates the source (`.TypeFrom`) of a PSQL parameter's data type definition.

        Returns `None` for external UDF arguments or if the source cannot be determined.
        """
        m = self.argument_mechanism
        if m is None:
            return TypeFrom.DATATYPE
        if m == Mechanism.BY_VALUE:
            return TypeFrom.DATATYPE if self.domain.is_sys_object() else TypeFrom.DOMAIN
        if m == Mechanism.BY_REFERENCE:
            if self._attributes.get('RDB$RELATION_NAME') is None:
                return TypeFrom.TYPE_OF_DOMAIN
            return TypeFrom.TYPE_OF_COLUMN
        raise Error(f"Unknown parameter mechanism code: {m}")
    @property
    def argument_name(self) -> str | None:
        """The explicit name (`RDB$ARGUMENT_NAME`) of the argument, if defined.
        Common for PSQL functions, may be `None` for external UDF arguments."""
        return self._attributes.get('RDB$ARGUMENT_NAME')
    @property
    def domain(self) -> Domain | None:
        """The underlying `.Domain` object (`RDB$FIELD_SOURCE`) for PSQL function parameters.
        Returns `None` for external UDF arguments or if no domain is associated."""
        return self.schema.all_domains.get(self._attributes.get('RDB$FIELD_SOURCE'))
    @property
    def default(self) -> str | None:
        """The `DEFAULT` value expression string (`RDB$DEFAULT_SOURCE`) for PSQL input arguments.
        Returns `None` if no default or not applicable."""
        if result := self._attributes.get('RDB$DEFAULT_SOURCE'):
            if result.upper().startswith('= '):
                result = result[2:]
            elif result.upper().startswith('DEFAULT '):
                result = result[8:]
        return result
    @property
    def collation(self) -> Collation | None:
        """The specific `.Collation` object (`RDB$COLLATION_ID`) for character types.
        Returns `None` if not applicable or using default."""
        return (None if (cid := self._attributes.get('RDB$COLLATION_ID')) is None
                else self.schema.get_collation_by_id(self.domain._attributes['RDB$CHARACTER_SET_ID'], cid))
    @property
    def argument_mechanism(self) -> Mechanism | None:
        """The mechanism (`.Mechanism`) used for passing PSQL function parameters
        (`RDB$ARGUMENT_MECHANISM`). Returns `None` for external UDFs or if unknown."""
        return None if (code := self._attributes.get('RDB$ARGUMENT_MECHANISM')) is None else Mechanism(code)
    @property
    def column(self) -> TableColumn | None:
        """The source `.TableColumn` if a PSQL parameter uses `TYPE OF COLUMN`.
        Returns `None` otherwise."""
        return (None if (rname := self._attributes.get('RDB$RELATION_NAME')) is None
                else self.schema.all_tables.get(rname).columns.get(self._attributes['RDB$FIELD_NAME']))
    @property
    def package(self) -> Package | None:
        """The `.Package` if the function is part of one (`RDB$PACKAGE_NAME`).
        Returns `None` otherwise."""
        return self.schema.packages.get(self._attributes.get('RDB$PACKAGE_NAME'))

class Function(SchemaItem):
    """Represents a User-Defined Function (UDF), either external or written in PSQL.

    Functions perform calculations or operations and return a single value. They can be:

    *   **External UDFs:** Implemented in an external library (DLL/SO), declared using
        `DECLARE EXTERNAL FUNCTION`. Arguments are passed by value, reference, or descriptor.
    *   **PSQL Functions:** Implemented directly in PSQL using `CREATE FUNCTION`, similar
        to stored procedures but must return a value via the `RETURNS` clause. Can be
        standalone or part of a `.Package`.

    Instances map data primarily from the `RDB$FUNCTIONS` system table. Associated
    arguments/return values are fetched from `RDB$FUNCTION_ARGUMENTS`. Functions are
    accessed via `.Schema.functions`, `.Schema.sys_functions`, `.Schema.all_functions`,
    or `.Package.functions`.

    Supported SQL actions via `.get_sql_for()`:

    *   External UDFs:

        *   `declare`: Generates `DECLARE [EXTERNAL] FUNCTION ... ENTRY_POINT ... MODULE_NAME ...`.
        *   `drop`: Generates `DROP [EXTERNAL] FUNCTION ...`.
        *   `comment`: Generates `COMMENT ON [EXTERNAL] FUNCTION ... IS ...`.

    *   User-defined, standalone PSQL Functions:

        *   `create` (optional keyword arg `no_code`: bool=False): Generates
            `CREATE FUNCTION ... RETURNS ... AS BEGIN ... END`. Includes parameter
            and return type definitions. Uses empty body if `no_code=True`.
        *   `recreate` (optional keyword arg `no_code`: bool=False): Generates
            `RECREATE FUNCTION ...`.
        *   `create_or_alter` (optional keyword arg `no_code`: bool=False): Generates
            `CREATE OR ALTER FUNCTION ...`.
        *   `drop`: Generates `DROP FUNCTION ...`.
        *   `comment`: Generates `COMMENT ON FUNCTION ... IS ...`.
        *   `alter` (keyword args): Modifies the function definition. Requires `code`.

            *   `arguments` (str | list[str] | tuple[str], optional): New input argument definitions.
            *   `returns` (str, **required**): New `RETURNS <type>` definition.
            *   `declare` (str | list[str] | tuple[str], optional): New variable declarations.
            *   `code` (str | list[str] | tuple[str], **required**): New function body code.

    *   System functions or packaged functions:

        *   `comment`: Adds or removes a descriptive comment.
        *   Note: Packaged functions are typically managed via `ALTER PACKAGE`.

    Arguments:
        schema: The parent `.Schema` instance.
        attributes: Raw data dictionary fetched from the `RDB$FUNCTIONS` row.
    """
    def __init__(self, schema: Schema, attributes: dict[str, Any]):
        super().__init__(schema, attributes)
        self._type_code.append(ObjectType.UDF)
        self.__arguments = None
        self.__returns = None
        self._strip_attribute('RDB$FUNCTION_NAME')
        self._strip_attribute('RDB$MODULE_NAME')
        self._strip_attribute('RDB$ENTRYPOINT')
        self._strip_attribute('RDB$ENGINE_NAME')
        self._strip_attribute('RDB$PACKAGE_NAME')
        self._strip_attribute('RDB$SECURITY_CLASS')
        self._strip_attribute('RDB$OWNER_NAME')
        if not self.is_sys_object():
            if self.is_external():
                self._actions.extend(['comment', 'declare', 'drop'])
            elif self._attributes.get('RDB$PACKAGE_NAME') is None:
                self._actions.extend(['create', 'recreate', 'alter', 'create_or_alter',
                                      'drop'])

    def _get_declare_sql(self, **params) -> str:
        """Generates the SQL command to DECLARE this external function (UDF).

        Includes input parameter definitions (type, mechanism) and the return value
        definition (type, mechanism, FREE_IT), plus ENTRY_POINT and MODULE_NAME.

        Arguments:
            **params: Accepts no parameters.

        Returns:
            The `DECLARE EXTERNAL FUNCTION` SQL string.

        Raises:
            ValueError: If unexpected parameters are passed.
            Error: If called on a non-external (PSQL) function or if arguments/return
                   value cannot be loaded.
        """
        self._check_params(params, [])
        if not self.is_external():
            raise Error("Cannot generate DECLARE SQL for non-external (PSQL) function.")
        fdef = f'DECLARE EXTERNAL FUNCTION {self.get_quoted_name()}\n'
        for p in self.arguments:
            fdef += f"  {p.get_sql_definition()}{'' if p.position == len(self.arguments) else ','}\n"
        if self.has_return():
            fdef += 'RETURNS '
            fdef += f"PARAMETER {self._attributes['RDB$RETURN_ARGUMENT']}" if self.has_return_argument() \
                else self.returns.get_sql_definition()
            fdef += f"{' FREE_IT' if self.returns.is_freeit() else ''}\n"
        return f"{fdef}ENTRY_POINT '{self.entrypoint}'\nMODULE_NAME '{self.module_name}'"
    def _get_drop_sql(self, **params) -> str:
        """Generates the SQL command to DROP this function.

        Arguments:
            **params: Accepts no parameters.

        Returns:
            The `DROP EXTERNAL FUNCTION` or `DROP FUNCTION` SQL string.

        Raises:
            ValueError: If unexpected parameters are passed.
        """
        self._check_params(params, [])
        return f"DROP{' EXTERNAL' if self.is_external() else ''} FUNCTION {self.get_quoted_name()}"
    def _get_comment_sql(self, **params) -> str:
        """Generates the SQL command to add or remove a COMMENT ON this function.

        Arguments:
            **params: Accepts no parameters.

        Returns:
            The `COMMENT ON [EXTERNAL] FUNCTION ... IS ...` SQL string. Sets comment to
            `NULL` if `self.description` is None, otherwise uses the description
            text with proper escaping.

        Raises:
            ValueError: If unexpected parameters are passed.
        """
        comment = 'NULL' if self.description is None \
            else f"'{escape_single_quotes(self.description)}'"
        return f"COMMENT ON{' EXTERNAL' if self.is_external() else ''} " \
               f"FUNCTION {self.get_quoted_name()} IS {comment}"
    def _get_create_sql(self, **params) -> str:
        """Generates the SQL command to CREATE this PSQL function.

        Includes input parameter list `(...)`, `RETURNS <type>` clause, and the
        `AS BEGIN ... END` block with PSQL source code.

        Arguments:
            **params: Accepts one optional keyword argument:

                      * `no_code` (bool): If `True`, generates an empty `BEGIN END`
                        block instead of the actual function source code. Defaults to `False`.

        Returns:
            The `CREATE FUNCTION` SQL string.

        Raises:
            ValueError: If unexpected parameters are passed.
            Error: If called on an external UDF, or if parameters/return type cannot be loaded.
        """
        self._check_params(params, ['no_code'])
        if self.is_external():
            raise Error("Cannot generate CREATE SQL for external UDF. Use 'declare' action.")
        no_code = params.get('no_code')
        result = f'CREATE FUNCTION {self.get_quoted_name()}'
        if self.has_arguments():
            if len(self.arguments) == 1:
                result += f' ({self.arguments[0].get_sql_definition()})\n'
            else:
                result += ' (\n'
                for p in self.arguments:
                    result += f"  {p.get_sql_definition()}" \
                              f"{'' if p.position == len(self.arguments) else ','}\n"
                result += ')\n'
        else:
            result += '\n'
        result += f'RETURNS {self.returns.get_sql_definition()}\n'
        return result+'AS\n'+('BEGIN\nEND' if no_code else self.source)
    def _get_alter_sql(self, **params) -> str:
        """Generates the SQL command to ALTER this PSQL function.

        Allows modification of input parameters, return type, declarations, and code body.
        Both `returns` and `code` parameters are required.

        Arguments:
            **params: Accepts keyword arguments:

                      * `arguments` (str | list[str] | tuple[str], optional): New definition(s)
                        for input parameters (full SQL like 'p_id INTEGER'). Replaces existing.
                      * `returns` (str, **required**): The new return type definition string
                        (e.g., 'VARCHAR(100) CHARACTER SET UTF8').
                      * `declare` (str | list[str] | tuple[str], optional): New variable
                        declarations section. Replaces existing.
                      * `code` (str | list[str] | tuple[str], **required**): The new PSQL
                        code for the function body (between BEGIN and END).

        Returns:
            The `ALTER FUNCTION` SQL string.

        Raises:
            ValueError: If required parameters (`returns`, `code`) are missing, if
                        parameter types are invalid, or if unexpected parameters are passed.
            Error: If called on an external UDF.
        """
        self._check_params(params, ['arguments', 'returns', 'declare', 'code'])
        if self.is_external():
            raise Error("Cannot generate ALTER SQL for external UDF.")
        arguments = params.get('arguments')
        for par in ('returns', 'code'):
            if par not in params:
                raise ValueError(f"Missing required parameter: '{par}'")
        returns = params.get('returns')
        code = params.get('code')
        declare = params.get('declare')
        #
        header = ''
        if arguments is not None:
            if isinstance(arguments, list | tuple):
                numpars = len(arguments)
                if numpars == 1:
                    header = f' ({arguments})\n'
                else:
                    header = ' (\n'
                    i = 1
                    for p in arguments:
                        header += f"  {p}{'' if i == numpars else ','}\n"
                        i += 1
                    header += ')\n'
            else:
                header = f' ({arguments})\n'
        #
        if not header:
            header += '\n'
        header += f'RETURNS {returns}\n'
        #
        if code:
            if declare is None:
                d = ''
            elif isinstance(declare, list | tuple):
                d = ''
                for x in declare:
                    d += f'  {x}\n'
            else:
                d = f'{declare}\n'
            if isinstance(code, list | tuple):
                c = ''
                for x in code:
                    c += f'  {x}\n'
            else:
                c = f'{code}\n'
            body = f"AS\n{d}BEGIN\n{c}END"
        else:
            body = 'AS\nBEGIN\nEND'
        #
        return f'ALTER FUNCTION {self.get_quoted_name()}{header}{body}'
    def _load_arguments(self, mock: dict[str, Any] | None=None) -> None:
        """Internal helper: Loads function arguments from RDB$FUNCTION_ARGUMENTS."""
        cols = ['RDB$FUNCTION_NAME', 'RDB$ARGUMENT_POSITION', 'RDB$MECHANISM',
                'RDB$FIELD_TYPE', 'RDB$FIELD_SCALE', 'RDB$FIELD_LENGTH',
                'RDB$FIELD_SUB_TYPE', 'RDB$CHARACTER_SET_ID', 'RDB$FIELD_PRECISION',
                'RDB$CHARACTER_LENGTH', 'RDB$PACKAGE_NAME', 'RDB$ARGUMENT_NAME',
                'RDB$FIELD_SOURCE', 'RDB$DEFAULT_SOURCE', 'RDB$COLLATION_ID', 'RDB$NULL_FLAG',
                'RDB$ARGUMENT_MECHANISM', 'RDB$FIELD_NAME', 'RDB$RELATION_NAME',
                'RDB$SYSTEM_FLAG', 'RDB$DESCRIPTION']
        self.__arguments = DataList((FunctionArgument(self.schema, self, row) for row in
                                     (mock or
                                      self.schema._select(f"""select {','.join(cols)} from rdb$function_arguments
where rdb$function_name = ? order by rdb$argument_position""", (self.name,)))),
                                    FunctionArgument, frozen=True)
        rarg = self._attributes['RDB$RETURN_ARGUMENT']
        if rarg is not None:
            for a in self.__arguments:
                if a.position == rarg:
                    self.__returns = weakref.ref(a)
    def _get_name(self) -> str:
        """Returns the function name (`RDB$FUNCTION_NAME`)."""
        return self._attributes['RDB$FUNCTION_NAME']
    def is_external(self) -> bool:
        """Checks if this is an external UDF (declared with `MODULE_NAME`).

        Returns:
            `True` if `module_name` is not None/empty, `False` otherwise (PSQL function).
        """
        return bool(self.module_name)
    def has_arguments(self) -> bool:
        """Checks if the function defines any input arguments.

        Excludes the argument designated as the return value.

        Returns:
            `True` if input arguments exist, `False` otherwise.
        """
        return bool(self.arguments)
    def has_return(self) -> bool:
        """Checks if the function is defined to return a value.

        Based on the presence of a return argument identified by `RDB$RETURN_ARGUMENT`.

        Returns:
            `True` if a return value/argument exists, `False` otherwise.
        """
        return self.returns is not None
    def has_return_argument(self) -> bool:
        """Checks if the function returns its value via one of its input arguments.

        This is a legacy mechanism where `RDB$RETURN_ARGUMENT` points to a non-zero
        argument position. Modern functions usually return directly (position 0 or implied).

        Returns:
            `True` if return is via an input parameter position, `False` otherwise.
        """
        return self.returns.position != 0 if self.returns is not None else False
    def is_packaged(self) -> bool:
        """Checks if the function is defined within a package.

        Based on the presence of `RDB$PACKAGE_NAME`.

        Returns:
            `True` if part of a package, `False` otherwise.
        """
        return bool(self._attributes.get('RDB$PACKAGE_NAME'))
    @property
    def module_name(self) -> str | None:
        """The external library name (`RDB$MODULE_NAME`) for external UDFs.
        Returns `None` for PSQL functions."""
        return self._attributes['RDB$MODULE_NAME']
    @property
    def entrypoint(self) -> str | None:
        """The function name within the external library (`RDB$ENTRYPOINT`) for external UDFs.
        Returns `None` for PSQL functions."""
        return self._attributes['RDB$ENTRYPOINT']
    @property
    def returns(self) -> FunctionArgument | None:
        """The `.FunctionArgument` object representing the function's return value.

        This argument is identified by the `RDB$RETURN_ARGUMENT` field in `RDB$FUNCTIONS`.
        Returns `None` if the function does not return a value or arguments are not loaded.
        Loads arguments lazily on first access.
        """
        if self.__arguments is None:
            self._load_arguments()
        return None if self.__returns is None else self.__returns()
    @property
    def arguments(self) -> DataList[FunctionArgument]:
        """A lazily-loaded `.DataList` of the function's input `.FunctionArgument` objects.

        Excludes the argument designated as the return value. Ordered by position.
        Returns an empty list if there are no input arguments.
        """
        if self.__arguments is None:
            self._load_arguments()
        return self.__arguments.extract(lambda a: a.position != 0, copy=True)
    @property
    def engine_mame(self) -> str | None:
        """The execution engine name (`RDB$ENGINE_NAME`), often 'UDR' for PSQL functions
        or `None` for external UDFs."""
        return self._attributes.get('RDB$ENGINE_NAME')
    @property
    def package(self) -> Package | None:
        """The `.Package` object this function belongs to, if any (`RDB$PACKAGE_NAME`).
        Returns `None` if the function is standalone."""
        return self.schema.packages.get(self._attributes.get('RDB$PACKAGE_NAME'))
    @property
    def private_flag(self) -> Privacy | None:
        """The privacy flag (`.Privacy`: PUBLIC or PRIVATE) for packaged functions.

        Derived from `RDB$PRIVATE_FLAG`. Returns `None` if not a packaged function.
        """
        return None if (code := self._attributes.get('RDB$PRIVATE_FLAG')) is None \
               else Privacy(code)
    @property
    def source(self) -> str | None:
        """The PSQL source code (`RDB$FUNCTION_SOURCE`) for PSQL functions.
        Returns `None` for external UDFs."""
        return self._attributes.get('RDB$FUNCTION_SOURCE')
    @property
    def id(self) -> int:
        """The internal numeric ID (`RDB$FUNCTION_ID`)"""
        return self._attributes.get('RDB$FUNCTION_ID')
    @property
    def valid_blr(self) -> bool | None:
        """Indicates if the compiled BLR of the PSQL function is valid (`RDB$VALID_BLR`).
        Returns `True`/`False`/`None`. Not applicable to external UDFs."""
        return None if (value := self._attributes.get('RDB$VALID_BLR')) is None \
               else bool(value)
    @property
    def security_class(self) -> str | None:
        """The security class name associated with this function (`RDB$SECURITY_CLASS`).
        Returns `None` if not set."""
        return self._attributes.get('RDB$SECURITY_CLASS')
    @property
    def owner_name(self) -> str:
        """The user name of the function's owner/creator (`RDB$OWNER_NAME`)."""
        return self._attributes.get('RDB$OWNER_NAME')
    @property
    def legacy_flag(self) -> Legacy:
        """Indicates if the function uses legacy syntax/behavior (`.Legacy` enum).

        Derived from `RDB$LEGACY_FLAG`.
        """
        return Legacy(self._attributes.get('RDB$LEGACY_FLAG'))
    @property
    def deterministic_flag(self) -> int | None:
        """Indicates if a PSQL function is declared as deterministic (`RDB$DETERMINISTIC_FLAG`).

        (Introduced in Firebird 4.0). 1 = Deterministic, 0 = Not Deterministic.
        Returns `None` if not applicable (external UDF, older FB) or flag is missing.
        """
        return self._attributes.get('RDB$DETERMINISTIC_FLAG')

class DatabaseFile(SchemaItem):
    """Represents a single physical file belonging to the main database or a shadow.

    Firebird databases can span multiple files. The first file is the primary,
    and subsequent files are secondary (continuation) files. This class holds
    information about one such file, including its name, sequence number within
    the database or shadow set, its starting page number, and its length in pages.

    Instances map data from the `RDB$FILES` system table. They are typically accessed
    via `.Schema.files` (for main database files) or `.Shadow.files` (for shadow files).

    This class represents a physical file component and does not support any direct
    SQL actions via `.get_sql_for()`. Database file management is done through other
    commands (e.g., `ALTER DATABASE ADD FILE`, `CREATE SHADOW`).

    Arguments:
        schema: The parent `.Schema` instance.
        attributes: Raw data dictionary fetched from the `RDB$FILES` row.
    """
    def __init__(self, schema: Schema, attributes: dict[str, Any]):
        super().__init__(schema, attributes)
        self._strip_attribute('RDB$FILE_NAME')
    def _get_name(self) -> str:
        """Returns a synthesized name based on sequence, not a standard object name.

        Database files don't have SQL names. This returns a name like 'FILE_N'
        based on the `sequence` number for identification purposes within the library.

        Returns:
            A string like 'FILE_0', 'FILE_1', etc., or `None` if sequence is missing.
        """
        return f'FILE_{self.sequence}'
    def is_sys_object(self) -> bool:
        """Indicates that database file entries themselves are system metadata.

        Returns:
            `True` always.
        """
        return True
    @property
    def filename(self) -> str:
        """The full operating system path and name of the database file (`RDB$FILE_NAME`)."""
        return self._attributes['RDB$FILE_NAME']
    @property
    def sequence(self) -> int:
        """The sequence number (`RDB$FILE_SEQUENCE`) of this file within its set
        (main database or a specific shadow). Starts from 0 for the primary file."""
        return self._attributes['RDB$FILE_SEQUENCE']
    @property
    def start(self) -> int:
        """The starting page number (`RDB$FILE_START`) allocated to this file within
        the logical database page space."""
        return self._attributes['RDB$FILE_START']
    @property
    def length(self) -> str:
        """The allocated length (`RDB$FILE_LENGTH`) of this file in database pages.
        A value of 0 often indicates the file can grow automatically."""
        return self._attributes['RDB$FILE_LENGTH']

class Shadow(SchemaItem):
    """Represents a database shadow, a physical copy of the database for disaster recovery.

    Shadows are maintained automatically by the Firebird engine (if AUTO) or manually
    (if MANUAL). They can be conditional, activating only if the main database becomes
    unavailable. A shadow can consist of one or more physical files.

    Instances primarily map data derived from `RDB$FILES` where `RDB$SHADOW_NUMBER` > 0.
    They are accessed via `Schema.shadows`.

    Supported SQL actions via `.get_sql_for()`:

    *   `create`: Generates the `CREATE SHADOW shadow_id [AUTO|MANUAL] [CONDITIONAL] FILE '...' [LENGTH N] [FILE '...' STARTING AT P [LENGTH N]] ...` statement.
    *   `drop` (optional keyword arg `preserve`: bool=False): Generates `DROP SHADOW shadow_id [PRESERVE FILE]`.
        If `preserve` is True, adds `PRESERVE FILE` clause.

    Note::

        Shadows do not have user-assigned names in SQL; they are identified by their numeric ID.

    Arguments:
        schema: The parent `.Schema` instance.
        attributes: Raw data dictionary containing `RDB$SHADOW_NUMBER` and `RDB$FILE_FLAGS`
                    fetched from the RDB$FILES row corresponding to the shadow's
                    primary file (sequence 0).
    """
    def __init__(self, schema: Schema, attributes: dict[str, Any]):
        super().__init__(schema, attributes)
        self.__files = None
        self._actions.extend(['create', 'drop'])
    def _get_create_sql(self, **params) -> str:
        """Generates the SQL command to CREATE this shadow.

        Includes the shadow ID, AUTO/MANUAL and CONDITIONAL flags, and the
        definition for all associated physical files (name, length, starting page).

        Arguments:
            **params: Accepts no parameters.

        Returns:
            The `CREATE SHADOW` SQL string.

        Raises:
            ValueError: If unexpected parameters are passed.
            Error: If the associated shadow files cannot be loaded.
        """
        self._check_params(params, [])
        result = f"CREATE SHADOW {self.id} " \
                 f"{'MANUAL' if self.is_manual() else 'AUTO'}" \
                 f"{' CONDITIONAL' if self.is_conditional() else ''}"
        if len(self.files) == 1:
            result += f" '{self.files[0].filename}'"
        else:
            f = self.files[0]
            length = f' LENGTH {f.length}' if f.length > 0 else ''
            result += f" '{f.filename}'{length}\n"
            for f in self.files[1:]:
                start = f' STARTING AT {f.start}' if f.start > 0 else ''
                length = f' LENGTH {f.length}' if f.length > 0 else ''
                result += f"  FILE '{f.filename}'{start}{length}"
                if f.sequence < len(self.files)-1:
                    result += '\n'
        return result
    def _get_drop_sql(self, **params) -> str:
        """Generates the SQL command to DROP this shadow.

        Arguments:
            **params: Accepts one optional keyword argument:

                      * `preserve` (bool): If `True`, adds the `PRESERVE FILE` clause
                        to prevent the physical shadow files from being deleted by the
                        engine. Defaults to `False`.

        Returns:
            The `DROP SHADOW` SQL string.

        Raises:
            ValueError: If unexpected parameters are passed.
        """
        self._check_params(params, ['preserve'])
        preserve = params.get('preserve')
        return f"DROP SHADOW {self.id}{' PRESERVE FILE' if preserve else ''}"
    def _get_name(self) -> str:
        """Returns a synthesized name 'SHADOW_N', as shadows don't have SQL names.

        Returns:
            A string like 'SHADOW_1', 'SHADOW_2', etc., based on the shadow ID.
        """
        return f'SHADOW_{self.id}'
    def is_sys_object(self) -> bool:
        """Indicates that shadows are user-defined objects, not system objects.

        Returns:
            `False` always.
        """
        return False
    def is_manual(self) -> bool:
        """Checks if the shadow requires manual intervention (`MANUAL`).

        Based on the `.ShadowFlag.MANUAL` flag.

        Returns:
            `True` if manual, `False` if automatic (AUTO).
        """
        return ShadowFlag.MANUAL in self.flags
    def is_inactive(self) -> bool:
        """Checks if the shadow is currently marked as inactive (`INACTIVE`).

        Based on the `.ShadowFlag.INACTIVE` flag. The engine typically ignores
        inactive shadows.

        Returns:
            `True` if inactive, `False` if active.
        """
        return ShadowFlag.INACTIVE in self.flags
    def is_conditional(self) -> bool:
        """Checks if the shadow is conditional (`CONDITIONAL`).

        Conditional shadows are only activated by the engine if the main database
        becomes inaccessible. Based on the `.ShadowFlag.CONDITIONAL` flag.

        Returns:
            `True` if conditional, `False` otherwise.
        """
        return ShadowFlag.CONDITIONAL in self.flags
    @property
    def id(self) -> int:
        """The numeric ID (`RDB$SHADOW_NUMBER`) that identifies this shadow set."""
        return self._attributes['RDB$SHADOW_NUMBER']
    @property
    def flags(self) -> ShadowFlag:
        """A `.ShadowFlag` enum value representing the combined flags
        (INACTIVE, MANUAL, CONDITIONAL) defined by `RDB$FILE_FLAGS` for this shadow."""
        return ShadowFlag(self._attributes['RDB$FILE_FLAGS'])
    @property
    def files(self) -> DataList[DatabaseFile]:
        """A lazily-loaded `.DataList` of the `.DatabaseFile` objects comprising this shadow.

        Ordered by sequence number. Fetched from `RDB$FILES` matching this shadow's ID.
        """
        if self.__files is None:
            self.__files = DataList((DatabaseFile(self, row) for row
                            in self.schema._select("""select RDB$FILE_NAME, RDB$FILE_SEQUENCE,
RDB$FILE_START, RDB$FILE_LENGTH from RDB$FILES
where RDB$SHADOW_NUMBER = ?
order by RDB$FILE_SEQUENCE""", (self._attributes['RDB$SHADOW_NUMBER'],))), frozen=True)
        return self.__files

class Privilege(SchemaItem):
    """Represents a single privilege granted on a database object to a user or another object.

    This class maps a single row from the `RDB$USER_PRIVILEGES` system table, detailing:

    *   Who granted the privilege (`grantor`).
    *   Who received the privilege (`user` / grantee).
    *   What the privilege is (`privilege`, e.g., SELECT, INSERT, EXECUTE, MEMBERSHIP).
    *   What object the privilege applies to (`subject`, e.g., Table, Procedure, Role).
    *   Optionally, which specific column it applies to (`field_name`).
    *   Whether the grantee can grant this privilege to others (`grant_option`).

    Instances are typically accessed via `Schema.privileges` or filtered using methods like
    `Schema.get_privileges_of()` or properties like `Table.privileges`.

    Supported SQL actions via `.get_sql_for()`:

    *   `grant` (optional keyword arg `grantors`: list[str]=['SYSDBA']): Generates the
        `GRANT ... ON ... TO ... [WITH GRANT/ADMIN OPTION] [GRANTED BY ...]` statement.
        The `GRANTED BY` clause is added if the actual grantor is not in the `grantors` list.
    *   `revoke` (optional keyword args: `grantors`: list[str]=['SYSDBA'], `grant_option`: bool=False):
        Generates the `REVOKE [GRANT/ADMIN OPTION FOR] ... FROM ... [GRANTED BY ...]` statement.
        If `grant_option` is True, revokes only the grant/admin option, not the privilege itself.
        The `GRANTED BY` clause is added if the actual grantor is not in the `grantors` list.

    Arguments:
        schema: The parent `.Schema` instance.
        attributes: Raw data dictionary fetched from the `RDB$USER_PRIVILEGES` row.
    """
    def __init__(self, schema: Schema, attributes: dict[str, Any]):
        super().__init__(schema, attributes)
        self._actions.extend(['grant', 'revoke'])
        self._strip_attribute('RDB$USER')
        self._strip_attribute('RDB$GRANTOR')
        self._strip_attribute('RDB$PRIVILEGE')
        self._strip_attribute('RDB$RELATION_NAME')
        self._strip_attribute('RDB$FIELD_NAME')
    def _get_grant_sql(self, **params) -> str:
        """Generates the SQL command to GRANT this specific privilege.

        Constructs the `GRANT` statement including the privilege type (and column
        if applicable), subject object, grantee, optional WITH GRANT/ADMIN OPTION,
        and optional GRANTED BY clause.

        Arguments:
            **params: Accepts one optional keyword argument:

                      * `grantors` (list[str]): A list of user/role names considered
                        standard grantors. If the actual `grantor_name` of this
                        privilege is not in this list, a `GRANTED BY` clause will be
                        appended. Defaults to `['SYSDBA']`.

        Returns:
            The `GRANT` SQL string.

        Raises:
            ValueError: If unexpected parameters are passed.
        """
        self._check_params(params, ['grantors'])
        grantors = params.get('grantors', ['SYSDBA'])
        privileges = [PrivilegeCode.SELECT, PrivilegeCode.INSERT, PrivilegeCode.UPDATE,
                      PrivilegeCode.DELETE, PrivilegeCode.REFERENCES]
        admin_option = ' WITH GRANT OPTION' if self.has_grant() else ''
        if self.privilege in privileges:
            privilege = self.privilege.name
            if self.field_name is not None:
                privilege += f'({self.field_name})'
            privilege += ' ON '
        elif self.privilege is PrivilegeCode.EXECUTE: # procedure
            privilege = 'EXECUTE ON PROCEDURE '
        elif self.privilege is PrivilegeCode.MEMBERSHIP:
            privilege = ''
            admin_option = ' WITH ADMIN OPTION' if self.has_grant() else ''
        user = self.user
        if isinstance(user, Procedure):
            utype = 'PROCEDURE '
        elif isinstance(user, Trigger):
            utype = 'TRIGGER '
        elif isinstance(user, View):
            utype = 'VIEW '
        else:
            utype = ''
        if (grantors is not None) and (self.grantor_name not in grantors):
            granted_by = f' GRANTED BY {self.grantor_name}'
        else:
            granted_by = ''
        return f'GRANT {privilege}{self.subject_name}' \
               f' TO {utype}{self.user_name}{admin_option}{granted_by}'
    def _get_revoke_sql(self, **params) -> str:
        """Generates the SQL command to REVOKE this specific privilege.

        Constructs the `REVOKE` statement including the privilege type (and column
        if applicable), subject object, grantee, optional GRANT/ADMIN OPTION FOR clause,
        and optional GRANTED BY clause.

        Arguments:
            **params: Accepts optional keyword arguments:

                      * `grantors` (list[str]): A list of standard grantor names. If the
                        actual `grantor_name` is not in this list, a `GRANTED BY`
                        clause will be appended. Defaults to `['SYSDBA']`.
                      * `grant_option` (bool): If `True`, revokes only the ability to grant
                        the privilege (`GRANT OPTION FOR` or `ADMIN OPTION FOR`), not the
                        privilege itself. Defaults to `False`.

        Returns:
            The `REVOKE` SQL string.

        Raises:
            ValueError: If `grant_option` is True but the privilege was not granted
                        with the grant/admin option, or if unexpected parameters are passed.
        """
        self._check_params(params, ['grant_option', 'grantors'])
        grantors = params.get('grantors', ['SYSDBA'])
        option_only = params.get('grant_option', False)
        if option_only and not self.has_grant():
            raise ValueError("Can't revoke grant option that wasn't granted.")
        privileges = [PrivilegeCode.SELECT, PrivilegeCode.INSERT, PrivilegeCode.UPDATE,
                      PrivilegeCode.DELETE, PrivilegeCode.REFERENCES]
        admin_option = 'GRANT OPTION FOR ' if self.has_grant() and option_only else ''
        if self.privilege in privileges:
            privilege = self.privilege.name
            if self.field_name is not None:
                privilege += f'([{self.field_name}])'
            privilege += ' ON '
        elif self.privilege is PrivilegeCode.EXECUTE:
            privilege = 'EXECUTE ON PROCEDURE '
        elif self.privilege is PrivilegeCode.MEMBERSHIP:
            privilege = ''
            admin_option = 'ADMIN OPTION FOR' if self.has_grant() and option_only else ''
        user = self.user
        if isinstance(user, Procedure):
            utype = 'PROCEDURE '
        elif isinstance(user, Trigger):
            utype = 'TRIGGER '
        elif isinstance(user, View):
            utype = 'VIEW '
        else:
            utype = ''
        if (grantors is not None) and (self.grantor_name not in grantors):
            granted_by = f' GRANTED BY {self.grantor_name}'
        else:
            granted_by = ''
        return f'REVOKE {admin_option}{privilege}{self.subject_name}' \
               f' FROM {utype}{self.user_name}{granted_by}'
    def _get_name(self) -> str | None:
        """Returns a synthesized name representing the privilege, not a standard object name.

        Combines grantee, privilege, and subject for identification within the library.
        Example: 'USER1_SELECT_ON_TABLE_T1'

        Returns:
            A synthesized string identifier, or `None` if components are missing.
        """
        # Privileges don't have a single SQL name. Synthesize one for internal use.
        parts = [
            self.user_name,
            self.privilege.name if self.privilege else 'UNKNOWNPRIV',
            'ON',
            self.subject_name
        ]
        if self.field_name:
            parts.extend(['COL', self.field_name])
        if None in parts: # Check if any essential part is missing
            return None
        return '_'.join(parts)
    def is_sys_object(self) -> bool:
        """Indicates that privilege entries themselves are system metadata.

        Returns:
            `True` always.
        """
        return True
    def has_grant(self) -> bool:
        """Checks if this privilege was granted with the `WITH GRANT OPTION` or
        `WITH ADMIN OPTION`.

        Returns:
            `True` if grant/admin option is present, `False` otherwise.
        """
        return self.grant_option and self.grant_option is not GrantOption.NONE
    def is_select(self) -> bool:
        """Checks if this is a `SELECT` privilege."""
        return self.privilege is PrivilegeCode.SELECT
    def is_insert(self) -> bool:
        """Checks if this is an `INSERT` privilege."""
        return self.privilege is PrivilegeCode.INSERT
    def is_update(self) -> bool:
        """Checks if this is an `UPDATE` privilege."""
        return self.privilege is PrivilegeCode.UPDATE
    def is_delete(self) -> bool:
        """Checks if this is a `DELETE` privilege."""
        return self.privilege is PrivilegeCode.DELETE
    def is_execute(self) -> bool:
        """Checks if this is an `EXECUTE` privilege (on a procedure or function)."""
        return self.privilege is PrivilegeCode.EXECUTE
    def is_reference(self) -> bool:
        """Checks if this is a `REFERENCES` privilege (for foreign keys)."""
        return self.privilege is  PrivilegeCode.REFERENCES
    def is_membership(self) -> bool:
        """Checks if this represents `ROLE` membership."""
        return self.privilege is PrivilegeCode.MEMBERSHIP
    def is_usage(self) -> bool:
        """Checks if this represents `USAGE` privilege."""
        return self.privilege == PrivilegeCode.USAGE
    @property
    def user(self) -> UserInfo | Role | Procedure | Trigger | View | None:
        """The grantee: the user or object receiving the privilege.

        Resolves based on `RDB$USER` (name) and `RDB$USER_TYPE`.

        Returns:
            A `~firebird.driver.UserInfo`, `.Role`, `.Procedure`, `.Trigger`, or `.View`
            object representing the grantee, or `None` if resolution fails.
        """
        return self.schema.get_item(self._attributes['RDB$USER'],
                                     ObjectType(self._attributes['RDB$USER_TYPE']))
    @property
    def grantor(self) -> UserInfo | None:
        """The grantor: the user who granted the privilege (`RDB$GRANTOR`).

        Returns:
            A `~firebird.driver.UserInfo` object representing the grantor, or `None`
            if the grantor name is missing.
        """
        return UserInfo(user_name=self._attributes['RDB$GRANTOR'])
    @property
    def privilege(self) -> PrivilegeCode:
        """The type of privilege granted (`.PrivilegeCode` enum).

        Derived from `RDB$PRIVILEGE` ('S', 'I', 'U', 'D', 'R', 'X', 'G', 'M', ...).
        """
        return PrivilegeCode(self._attributes['RDB$PRIVILEGE'])
    @property
    def subject_name(self) -> str:
        """The name (`RDB$RELATION_NAME`) of the object on which the privilege is granted
        (e.g., table name, procedure name, role name)."""
        return self._attributes['RDB$RELATION_NAME']
    @property
    def subject_type(self) -> ObjectType:
        """The type (`.ObjectType`) of the object to which the privilege is granted
        (`RDB$OBJECT_TYPE`)."""
        return ObjectType(self._attributes['RDB$OBJECT_TYPE'])
    @property
    def field_name(self) -> str:
        """The specific column/field name (`RDB$FIELD_NAME`) this privilege applies to.

        Relevant for column-level SELECT, UPDATE, REFERENCES privileges.
        Returns `None` if the privilege applies to the object as a whole.
        """
        return self._attributes['RDB$FIELD_NAME']
    @property
    def subject(self) -> Role | Table | View | Procedure | Function:
        """The database object (`.SchemaItem`) on which the privilege is granted.

        Resolves based on `subject_name` and `subject_type`. May return specific
        column objects if `field_name` is set.

        Returns:
            The specific object (e.g., `.Table`, `.Procedure`, `.Role`, `.TableColumn`),
            or `None` if resolution fails.
        """
        result = self.schema.get_item(self.subject_name, self.subject_type, self.field_name)
        if result is None and self.subject_type == ObjectType.TABLE:
            # Views are logged as tables, so try again for view code
            result = self.schema.get_item(self.subject_name, ObjectType.VIEW, self.field_name)
        return result
    @property
    def user_name(self) -> str:
        """The name (`RDB$USER`) of the grantee (user, role, procedure, etc.)."""
        return self._attributes['RDB$USER']
    @property
    def user_type(self) -> ObjectType:
        """The type (`.ObjectType`) of the grantee (`RDB$USER_TYPE`)."""
        return ObjectType(self._attributes['RDB$USER_TYPE'])
    @property
    def grantor_name(self) -> str:
        """The user name (`RDB$GRANTOR`) of the user who granted the privilege."""
        return self._attributes['RDB$GRANTOR']
    @property
    def grant_option(self) -> GrantOption | None:
        """Indicates if the privilege includes the grant/admin option (`.GrantOption`).

        Derived from `RDB$GRANT_OPTION` (0=None, 1=Grant, 2=Admin).
        Returns `None` if the option code is unrecognized or missing.
        """
        return None if (value := self._attributes['RDB$GRANT_OPTION']) is None \
               else GrantOption(value)

class Package(SchemaItem):
    """Represents a PSQL package, a container for related procedures and functions.

    Packages provide namespace management, encapsulation, and can define public
    (accessible outside the package) and private (internal use only) routines.
    They consist of two parts:

    *   **Header:** Declares public procedures, functions, variables, and types.
    *   **Body:** Contains the implementation (code) for the declared routines,
        and can also include private declarations and implementations.

    Instances map data primarily from the `RDB$PACKAGES` system table. Contained
    procedures and functions are linked via `RDB$PACKAGE_NAME` in their respective
    system tables (`RDB$PROCEDURES`, `RDB$FUNCTIONS`). Packages are accessed
    via `Schema.packages`.

    Supported SQL actions via `.get_sql_for()`:

    *   `create` (keyword argument `body`: bool=False): Generates `CREATE PACKAGE [BODY] ... AS ... END`.
        If `body` is `False` (default), creates the package header using `self.header`.
        If `body` is `True`, creates the package body using `self.body`.
    *   `recreate` (keyword argument `body`: bool=False): Generates `RECREATE PACKAGE [BODY] ...`.
        Same logic as `create` regarding the `body` parameter.
    *   `create_or_alter` (keyword argument `body`: bool=False): Generates `CREATE OR ALTER PACKAGE [BODY] ...`.
        Same logic as `create` regarding the `body` parameter.
    *   `drop` (keyword argument `body`: bool=False): Generates `DROP PACKAGE [BODY] ...`.
        If `body` is `False` (default), drops the package header (and implicitly the body).
        If `body` is `True`, drops only the package body.
    *   `comment`: Generates `COMMENT ON PACKAGE ... IS ...`.

    .. note::

       Altering the contents of a package typically involves using `CREATE OR ALTER PACKAGE [BODY]`
       with the complete new source code, rather than a specific `ALTER PACKAGE` command
       to modify parts (which is less common in Firebird PSQL).

    Arguments:
        schema: The parent `.Schema` instance.
        attributes: Raw data dictionary fetched from the `RDB$PACKAGES` row.
    """
    def __init__(self, schema: Schema, attributes: dict[str, Any]):
        super().__init__(schema, attributes)
        self._type_code.extend([ObjectType.PACKAGE_HEADER, ObjectType.PACKAGE_BODY])
        self._actions.extend(['create', 'recreate', 'create_or_alter', 'alter', 'drop',
                              'comment'])
        self._strip_attribute('RDB$PACKAGE_NAME')
        self._strip_attribute('RDB$SECURITY_CLASS')
        self._strip_attribute('RDB$OWNER_NAME')
    def _get_create_sql(self, **params) -> str:
        """Generates the SQL command to CREATE the package header or body.

        Arguments:
            **params: Accepts one optional keyword argument:

                      * `body` (str): If present, generates `CREATE PACKAGE BODY` using
                        the specified value as body source. If not present, generates
                        `CREATE PACKAGE` (header) using the `header` property as source.

        Returns:
            The `CREATE PACKAGE [BODY]` SQL string.

        Raises:
            ValueError: If unexpected parameters are passed.
        """
        self._check_params(params, ['body'])
        body = params.get('body')
        cbody = 'BODY ' if body else ''
        result = f'CREATE PACKAGE {cbody}{self.get_quoted_name()}'
        return result+'\nAS\n'+(self.body if body else self.header)
    def _get_alter_sql(self, **params) -> str:
        """Generates the SQL command to CREATE the package header or body.

        Arguments:
            **params: Accepts one optional keyword argument:

                      * `header` (str | list[str]): Source string or list of lines (without
                        line end)  for package header. If present, generates `ALTER PACKAGE`
                        using the value as source. Otherwise it generates `ALTER PACKAGE`
                        with empty source.

        Returns:
            The `ALTER PACKAGE` SQL string.

        Raises:
            ValueError: If unexpected parameters are passed.
        """
        self._check_params(params, ['header'])
        header = params.get('header')
        if not header:
            hdr = ''
        else:
            hdr = '\n'.join(header) if isinstance(header, list) else header
        return f'ALTER PACKAGE {self.get_quoted_name()}\nAS\nBEGIN\n{hdr}\nEND'
    def _get_drop_sql(self, **params) -> str:
        """Generates the SQL command to DROP the package header or body.

        Arguments:
            **params: Accepts one optional keyword argument:

                      * `body` (bool): If `True`, generates `DROP PACKAGE BODY`. If `False`
                        (default), generates `DROP PACKAGE` (which drops both header and body).

        Returns:
            The `DROP PACKAGE [BODY]` SQL string.

        Raises:
            ValueError: If unexpected parameters are passed.
        """
        self._check_params(params, ['body'])
        body = params.get('body')
        cbody = 'BODY ' if body else ''
        return f'DROP PACKAGE {cbody}{self.get_quoted_name()}'
    def _get_comment_sql(self, **params) -> str:
        """Generates the SQL command to add or remove a COMMENT ON this package.

        Arguments:
            **params: Accepts no parameters.

        Returns:
            The `COMMENT ON PACKAGE ... IS ...` SQL string. Sets comment to `NULL` if
            `self.description` is None, otherwise uses the description text
            with proper escaping.

        Raises:
            ValueError: If unexpected parameters are passed.
        """
        comment = 'NULL' if self.description is None \
            else f"'{escape_single_quotes(self.description)}'"
        return f'COMMENT ON PACKAGE {self.get_quoted_name()} IS {comment}'
    def _get_name(self) -> str:
        """Returns the package name (`RDB$PACKAGE_NAME`)."""
        return self._attributes['RDB$PACKAGE_NAME']
    def has_valid_body(self) -> bool | None:
        """Checks if the package body is currently marked as valid by the engine.

        Based on `RDB$VALID_BODY_FLAG`. A body might be invalid if the header
        changed since the body was compiled, or if the body failed compilation.

        Returns:
            `True` if the body is valid, `False` if invalid, `None` if the status
            is unknown or the flag is missing.
        """
        return None if (result := self._attributes.get('RDB$VALID_BODY_FLAG')) is None \
               else bool(result)
    @property
    def header(self) -> str | None:
        """The PSQL source code for the package header (`RDB$PACKAGE_HEADER_SOURCE`).
        Contains public declarations. Returns `None` if unavailable."""
        return self._attributes['RDB$PACKAGE_HEADER_SOURCE']
    @property
    def body(self) -> str | None:
        """The PSQL source code for the package body (`RDB$PACKAGE_BODY_SOURCE`).
        Contains implementations and private declarations. Returns `None` if unavailable."""
        return self._attributes['RDB$PACKAGE_BODY_SOURCE']
    @property
    def security_class(self) -> str | None:
        """The security class name associated with this package, if any (`RDB$SECURITY_CLASS`).
        Returns `None` if not set."""
        return self._attributes['RDB$SECURITY_CLASS']
    @property
    def owner_name(self) -> str:
        """The user name of the package's owner/creator (`RDB$OWNER_NAME`)."""
        return self._attributes['RDB$OWNER_NAME']
    @property
    def functions(self) -> DataList[Function]:
        """A `.DataList` of all `.Function` objects defined within this package.

        Filters the main `Schema.functions` collection based on package name.
        """
        return self.schema.functions.extract(lambda fn: fn._attributes['RDB$PACKAGE_NAME'] == self.name,
                                             copy=True)
    @property
    def procedures(self) -> DataList[Procedure]:
        """A `.DataList` of all `.Procedure` objects defined within this package.

        Filters the main `Schema.procedures` collection based on package name.
        """
        return self.schema.procedures.extract(lambda proc: proc._attributes['RDB$PACKAGE_NAME'] == self.name,
                                              copy=True)

class BackupHistory(SchemaItem):
    """Represents an entry in the database's NBackup history log.

    Each entry records details about a physical backup operation performed using
    the `nbackup` utility, such as the backup level (full or incremental),
    timestamp, SCN (System Change Number), GUID, and the location of the backup file.

    Instances map data from the `RDB$BACKUP_HISTORY` system table. They are accessed
    via `Schema.backup_history`.

    This class represents a historical record and does not support any direct
    SQL actions via `.get_sql_for()`. Backup history is managed implicitly by
    `nbackup` operations and potentially explicit `DELETE FROM RDB$BACKUP_HISTORY`
    statements (use with caution).

    Arguments:
        schema: The parent `.Schema` instance.
        attributes: Raw data dictionary fetched from the `RDB$BACKUP_HISTORY` row.
    """
    def __init__(self, schema: Schema, attributes: dict[str, Any]):
        super().__init__(schema, attributes)
        self._strip_attribute('RDB$FILE_NAME')
    def _get_name(self) -> str:
        """Returns a synthesized name based on SCN, not a standard object name.

        Backup history entries don't have SQL names. This returns a name like
        'BCKP_SCN_12345' based on the SCN for identification purposes within the library.

        Returns:
            A string like 'BCKP_SCN_12345', or `None` if SCN is missing.
        """
        return f'BCKP_{self.scn}'
    def is_sys_object(self) -> bool:
        """Indicates that backup history entries themselves are system metadata.

        Returns:
            `True` always.
        """
        return True
    @property
    def id(self) -> int:
        """The unique numeric identifier (`RDB$BACKUP_ID`) assigned by the engine to this backup record."""
        return self._attributes['RDB$BACKUP_ID']
    @property
    def filename(self) -> str:
        """The full operating system path and filename (`RDB$FILE_NAME`) of the primary
        backup file created during this operation."""
        return self._attributes['RDB$FILE_NAME']
    @property
    def created(self) -> datetime.datetime:
        """The date and time (`RDB$TIMESTAMP`) when the backup operation completed."""
        return self._attributes['RDB$TIMESTAMP']
    @property
    def level(self) -> int:
        """The backup level (`RDB$BACKUP_LEVEL`). Typically 0 for a full backup,
        and increasing integers for subsequent incremental backups based on the level 0."""
        return self._attributes['RDB$BACKUP_LEVEL']
    @property
    def scn(self) -> int:
        """The System Change Number (`RDB$SCN`) of the database at the time this
        backup operation started."""
        return self._attributes['RDB$SCN']
    @property
    def guid(self) -> str:
        """A unique identifier string (`RDB$GUID`) associated with the database state
        at the time of the backup. Used for validating incremental backups."""
        return self._attributes['RDB$GUID']

class Filter(SchemaItem):
    """Represents a user-defined BLOB filter, used for transforming BLOB data.

    BLOB filters are implemented as external functions residing in shared libraries (DLL/SO).
    They define a transformation between two BLOB subtypes (e.g., compressing text,
    encrypting binary data). Filters are declared in the database and can then be
    used implicitly when reading/writing BLOBs of matching subtypes.

    Instances map data from the `RDB$FILTERS` system table. They are typically accessed
    via `Schema.filters`.

    Supported SQL actions via `.get_sql_for()`:

    *   User-defined filters:

        *   `declare`: Generates `DECLARE FILTER filter_name ... ENTRY_POINT ... MODULE_NAME ...`.
        *   `drop`: Generates `DROP FILTER filter_name`.
        *   `comment`: Generates `COMMENT ON FILTER filter_name IS ...`.

    *   System filters (if any exist):

        *   `comment`: Adds or removes a descriptive comment.

    Arguments:
        schema: The parent `.Schema` instance.
        attributes: Raw data dictionary fetched from the `RDB$FILTERS` row.
    """
    def __init__(self, schema: Schema, attributes: dict[str, Any]):
        super().__init__(schema, attributes)
        self._type_code.append(ObjectType.BLOB_FILTER)
        self._strip_attribute('RDB$FUNCTION_NAME')
        self._strip_attribute('RDB$MODULE_NAME')
        self._strip_attribute('RDB$ENTRYPOINT')
        if not self.is_sys_object():
            self._actions.extend(['comment', 'declare', 'drop'])
    def _get_declare_sql(self, **params) -> str:
        """Generates the SQL command to DECLARE this BLOB filter.

        Includes the input and output BLOB subtypes, entry point, and module name.

        Arguments:
            **params: Accepts no parameters.

        Returns:
            The `DECLARE FILTER` SQL string.

        Raises:
            ValueError: If unexpected parameters are passed.
        """
        self._check_params(params, [])
        fdef = f'DECLARE FILTER {self.get_quoted_name()}\n' \
               f'INPUT_TYPE {self.input_sub_type} OUTPUT_TYPE {self.output_sub_type}\n'
        return f"{fdef}ENTRY_POINT '{self.entrypoint}' MODULE_NAME '{self.module_name}'"
    def _get_drop_sql(self, **params) -> str:
        """Generates the SQL command to DROP this BLOB filter.

        Arguments:
            **params: Accepts no parameters.

        Returns:
            The `DROP FILTER` SQL string.

        Raises:
            ValueError: If unexpected parameters are passed.
        """
        self._check_params(params, [])
        return f'DROP FILTER {self.get_quoted_name()}'
    def _get_comment_sql(self, **params) -> str:
        """Generates the SQL command to add or remove a COMMENT ON this BLOB filter.

        Arguments:
            **params: Accepts no parameters.

        Returns:
            The `COMMENT ON FILTER ... IS ...` SQL string. Sets comment to `NULL` if
            `self.description` is None, otherwise uses the description text
            with proper escaping.

        Raises:
            ValueError: If unexpected parameters are passed.
        """
        comment = 'NULL' if self.description is None \
            else f"'{escape_single_quotes(self.description)}'"
        return f'COMMENT ON FILTER {self.get_quoted_name()} IS {comment}'
    def _get_name(self) -> str:
        """Returns the filter name (stored in `RDB$FUNCTION_NAME`)."""
        return self._attributes['RDB$FUNCTION_NAME']
    @property
    def module_name(self) -> str:
        """The name (`RDB$MODULE_NAME`) of the external shared library (DLL/SO)
        containing the filter's implementation code."""
        return self._attributes['RDB$MODULE_NAME']
    @property
    def entrypoint(self) -> str:
        """The exported function name (`RDB$ENTRYPOINT`) within the external library
        that implements the filter logic."""
        return self._attributes['RDB$ENTRYPOINT']
    @property
    def input_sub_type(self) -> int:
        """The numeric BLOB subtype (`RDB$INPUT_SUB_TYPE`) that this filter accepts as input."""
        return self._attributes.get('RDB$INPUT_SUB_TYPE')
    @property
    def output_sub_type(self) -> int:
        """The numeric BLOB subtype (`RDB$OUTPUT_SUB_TYPE`) that this filter produces as output."""
        return self._attributes.get('RDB$OUTPUT_SUB_TYPE')
