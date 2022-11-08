#coding:utf-8
#
# PROGRAM/MODULE: firebird-lib
# FILE:           firebird/lib/schema.py
# DESCRIPTION:    Module work with Firebird database schema
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
# pylint: disable=C0302, C0301, W0212, R0902, R0912,R0913, R0914, R0915, R0904, C0103

"""firebird.lib.schema - Module work with Firebird database schema


"""

from __future__ import annotations
from typing import Dict, Tuple, List, Any, Optional, Union
import weakref
import datetime
from itertools import groupby
from enum import auto, Enum, IntEnum, IntFlag
from firebird.base.collections import DataList
from firebird.driver import Connection, Cursor, Statement, Isolation, TraAccessMode, Error, tpb
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

# Lists and disctionary maps
COLUMN_TYPES = {None: 'UNKNOWN', FieldType.SHORT: 'SMALLINT',
                FieldType.LONG: 'INTEGER', FieldType.QUAD: 'QUAD',
                FieldType.FLOAT: 'FLOAT', FieldType.TEXT: 'CHAR',
                FieldType.DOUBLE: 'DOUBLE PRECISION',
                FieldType.VARYING                        : 'VARCHAR', FieldType.CSTRING: 'CSTRING',
                FieldType.BLOB_ID: 'BLOB_ID', FieldType.BLOB: 'BLOB',
                FieldType.TIME: 'TIME', FieldType.DATE: 'DATE',
                FieldType.TIMESTAMP: 'TIMESTAMP', FieldType.INT64: 'BIGINT',
                FieldType.BOOLEAN: 'BOOLEAN'}

INTEGRAL_SUBTYPES = ('UNKNOWN', 'NUMERIC', 'DECIMAL')

class ObjectType(IntEnum):
    """Dependent type codes.
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
    PACKAGE = 18
    PACKAGE_BODY = 19

class IndexType(Enum):
    """Index ordering.
    """
    ASCENDING = 'ASCENDING'
    DESCENDING = 'DESCENDING'

class FunctionType(IntEnum):
    """Function type codes.
    """
    VALUE = 0
    BOOLEAN = 1

class Mechanism(IntEnum):
    """Mechanism codes.
    """
    BY_VALUE = 0
    BY_REFERENCE = 1
    BY_VMS_DESCRIPTOR = 2
    BY_ISC_DESCRIPTOR = 3
    BY_SCALAR_ARRAY_DESCRIPTOR = 4
    BY_REFERENCE_WITH_NULL = 5

class TransactionState(IntEnum):
    """Transaction state codes.
    """
    LIMBO = 1
    COMMITTED = 2
    ROLLED_BACK = 3

class SystemFlag(IntEnum):
    """System flag codes.
    """
    USER = 0
    SYSTEM = 1
    QLI = 2
    CHECK_CONSTRAINT = 3
    REFERENTIAL_CONSTRAINT = 4
    VIEW_CHECK = 5
    IDENTITY_GENERATOR = 6

class ShadowFlag(IntFlag):
    """Shadow file flags.
    """
    INACTIVE = 2
    MANUAL = 4
    CONDITIONAL = 16

class RelationType(IntEnum):
    """Relation type codes.
    """
    PERSISTENT = 0
    VIEW = 1
    EXTERNAL = 2
    VIRTUAL = 3
    GLOBAL_TEMPORARY_PRESERVE = 4
    GLOBAL_TEMPORARY_DELETE = 5

class ProcedureType(IntEnum):
    """Procedure type codes.
    """
    LEGACY = 0
    SELECTABLE = 1
    EXECUTABLE = 2

class ParameterMechanism(IntEnum):
    """Parameter mechanism type codes.
    """
    NORMAL = 0
    TYPE_OF = 1

class TypeFrom(IntEnum):
    """Source of parameter datatype codes.
    """
    DATATYPE = 0
    DOMAIN = 1
    TYPE_OF_DOMAIN = 2
    TYPE_OF_COLUMN = 3

class ParameterType(IntEnum):
    """Parameter type codes.
    """
    INPUT = 0
    OUTPUT = 1

class IdentityType(IntEnum):
    """Identity type codes.
    """
    ALWAYS = 0
    BY_DEFAULT = 1

class GrantOption(IntEnum):
    """Grant option codes.
    """
    NONE = 0
    GRANT_OPTION = 1
    ADMIN_OPTION = 2

class PageType(IntEnum):
    """Page type codes.
    """
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
    """Map to type codes.
    """
    USER = 0
    ROLE = 1

class TriggerType(IntEnum):
    """Trigger type codes.
    """
    DML = 0
    DB = 8192
    DDL = 16384

class DDLTrigger(IntEnum):
    """DDL trigger type codes.
    """
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
    """Database trigger type codes.
    """
    CONNECT = 0
    DISCONNECT = 1
    TRANSACTION_START = 2
    TRANSACTION_COMMIT = 3
    TRANSACTION_ROLLBACK = 4

class DMLTrigger(IntFlag):
    """DML trigger type codes.
    """
    INSERT = auto()
    UPDATE = auto()
    DELETE = auto()

class TriggerTime(IntEnum):
    """Trigger action time codes.
    """
    BEFORE = 0
    AFTER = 1


class ConstraintType(Enum):
    """Contraint type codes.
    """
    CHECK = 'CHECK'
    NOT_NULL = 'NOT NULL'
    FOREIGN_KEY = 'FOREIGN KEY'
    PRIMARY_KEY = 'PRIMARY KEY'
    UNIQUE = 'UNIQUE'

class Section(Enum):
    """DDL script sections. Used by `.Schema.get_metadata_ddl()`.
    """
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
    """Schema information collection categories.
    """
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
    """Privacy flag codes.
    """
    PUBLIC = 0
    PRIVATE = 1

class Legacy(IntEnum):
    """Legacy flag codes.
    """
    NEW_STYLE = 0
    LEGACY_STYLE = 1

class PrivilegeCode(Enum):
    """Priviledge codes.
    """
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
    """Collation attribute flags.
    """
    NONE = 0
    PAD_SPACE = 1
    CASE_INSENSITIVE = 2
    ACCENT_INSENSITIVE = 4

#: List of default sections (in order) for `.Schema.get_metadata_ddl()`
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


def get_grants(privileges: List[Privilege], grantors: List[str]=None) -> List[str]:
    """Get list of minimal set of SQL GRANT statamenets necessary to grant
    specified privileges.

    Arguments:
        privileges: List of :class:`Privilege` instances.

    Keyword Args:
        grantors: List of standard grantor names. Generates GRANTED BY
            clause for privileges granted by user that's not in list.
    """
    tp = set([PrivilegeCode.SELECT, PrivilegeCode.INSERT, PrivilegeCode.UPDATE,
             PrivilegeCode.DELETE, PrivilegeCode.REFERENCES])

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
        g = list(g)
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
            items = list(items)
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
    """
    return text.replace("'", "''")

class Visitable:
    """Base class for Visitor Pattern support.
    """
    def accept(self, visitor: Visitor) -> None:
        """Visitor Pattern support.

        Calls `visit(self)` on parameter object.

        Arguments:
            visitor: Visitor object of Visitor Pattern.
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
    """This class represents database schema.
    """
    #: Configuration option - Always quote db object names on output
    opt_always_quote: bool = False
    #: Configuration option - Keyword for generator/sequence
    opt_generator_keyword: str = 'SEQUENCE'
    #: Datatype declaration methods for procedure parameters (key = numID, value = name)
    param_type_from: Dict[int, str] = {0: 'DATATYPE',
                         1: 'DOMAIN',
                         2: 'TYPE OF DOMAIN',
                         3: 'TYPE OF COLUMN'}
    #: Object types (key = numID, value = type_name)
    object_types: Dict[int, str] = {}
    #: Object type codes (key = type_name, value = numID)
    object_type_codes:  Dict[str, int] = {}
    #: Character set names (key = numID, value = charset_name)
    character_set_names: Dict[int, str] = {}
    #: Field types (key = numID, value = type_name)
    field_types: Dict[int, str] = {}
    #: Field sub types (key = numID, value = type_name)
    field_subtypes: Dict[int, str] = {}
    #: Function types (key = numID, value = type_name)
    function_types: Dict[int, str] = {}
    #: Mechanism Types (key = numID, value = type_name)
    mechanism_types = {}
    #: Parameter Mechanism Types (key = numID, value = type_name)
    parameter_mechanism_types: Dict[int, str] = {}
    #: Procedure Types (key = numID, value = type_name)
    procedure_types: Dict[int, str] = {}
    #: Relation Types (key = numID, value = type_name)
    relation_types: Dict[int, str] = {}
    #: System Flag Types (key = numID, value = type_name)
    system_flag_types: Dict[int, str] = {}
    #: Transaction State Types (key = numID, value = type_name)
    transaction_state_types: Dict[int, str] = {}
    #: Trigger Types (key = numID, value = type_name)
    trigger_types: Dict[int, str] = {}
    #: Parameter Types (key = numID, value = type_name)
    parameter_types: Dict[int, str] = {}
    #: Index activity status (key = numID, value = flag_name)
    index_activity_flags: Dict[int, str] = {}
    #: Index uniqueness (key = numID, value = flag_name)
    index_unique_flags: Dict[int, str] = {}
    #: Trigger activity status (key = numID, value = flag_name)
    trigger_activity_flags: Dict[int, str] = {}
    #: Grant option (key = numID, value = option_name)
    grant_options: Dict[int, str] = {}
    #: Page type (key = numID, value = type_name)
    page_types: Dict[int, str] = {}
    #: Privacy flags (numID, value = flag_name)
    privacy_flags: Dict[int, str] = {}
    #: Legacy flags (numID, value = flag_name)
    legacy_flags: Dict[int, str] = {}
    #: Determinism flags (numID, value = flag_name)
    deterministic_flags: Dict[int, str] = {}
    #: Identity type (key = numID, value = type_name)
    identity_type: Dict[int, str] = {}
    def __init__(self):
        self._con: Connection = None
        self._ic: Cursor = None
        self.__internal: bool = False
        # Engine/ODS specific data
        self._reserved_: List[str] = []
        self.ods: float = None
        # database metadata
        self.__tables: Tuple[DataList, DataList] = None
        self.__views: Tuple[DataList, DataList] = None
        self.__domains: Tuple[DataList, DataList] = None
        self.__indices: Tuple[DataList, DataList] = None
        self.__constraint_indices = None
        self.__dependencies: DataList = None
        self.__generators: Tuple[DataList, DataList] = None
        self.__triggers: Tuple[DataList, DataList] = None
        self.__procedures: Tuple[DataList, DataList] = None
        self.__constraints: DataList = None
        self.__collations: DataList = None
        self.__character_sets: DataList = None
        self.__exceptions: DataList = None
        self.__roles: DataList = None
        self.__functions: Tuple[DataList, DataList] = None
        self.__files: DataList = None
        self.__shadows: DataList = None
        self.__privileges: DataList = None
        self.__users: DataList = None
        self.__packages: DataList = None
        self.__backup_history: DataList = None
        self.__filters: DataList = None
        self.__attrs = None
        self._default_charset_name = None
        self.__owner = None
    def __del__(self):
        if not self.closed:
            self._close()
    def __enter__(self) -> Schema:
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
    def _set_internal(self, value: bool) -> None:
        self.__internal = value
    def __clear(self, data: Union[Category, List[Category], Tuple]=None) -> None:
        if data:
            if not isinstance(data, (list, tuple)):
                data = (data, )
        else:
            data = list(Category)
        for item in data:
            if item is Category.TABLES:
                self.__tables: Tuple[DataList, DataList] = None
            elif item is Category.VIEWS:
                self.__views: Tuple[DataList, DataList] = None
            elif item is Category.DOMAINS:
                self.__domains: Tuple[DataList, DataList] = None
            elif item is Category.INDICES:
                self.__indices: Tuple[DataList, DataList] = None
                self.__constraint_indices = None
            elif item is Category.DEPENDENCIES:
                self.__dependencies: DataList = None
            elif item is Category.GENERATORS:
                self.__generators: Tuple[DataList, DataList] = None
            elif item is Category.TRIGGERS:
                self.__triggers: Tuple[DataList, DataList] = None
            elif item is Category.PROCEDURES:
                self.__procedures: Tuple[DataList, DataList] = None
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
                self.__functions: Tuple[DataList, DataList] = None
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
    def _select_row(self, cmd: Union[Statement, str], params: List=None) -> Dict[str, Any]:
        self._ic.execute(cmd, params)
        row = self._ic.fetchone()
        return {self._ic.description[i][0]: row[i] for i in range(len(row))}
    def _select(self, cmd: str, params: List=None) -> Dict[str, Any]:
        self._ic.execute(cmd, params)
        desc = self._ic.description
        return ({desc[i][0]: row[i] for i in range(len(row))} for row in self._ic)
    def _get_field_dimensions(self, field) -> List[Tuple[int, int]]:
        return [(r[0], r[1]) for r in
                self._ic.execute(f"""select RDB$LOWER_BOUND, RDB$UPPER_BOUND
                from RDB$FIELD_DIMENSIONS where RDB$FIELD_NAME = '{field.name}' order by RDB$DIMENSION""")]
    def _get_all_domains(self) -> Tuple[DataList[Domain], DataList[Domain], DataList[Domain]]:
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
    def _get_all_tables(self) -> Tuple[DataList[Table], DataList[Table], DataList[Table]]:
        if self.__tables is None:
            self.__fail_if_closed()
            tables = DataList((Table(self, row) for row
                               in self._select('select * from rdb$relations where rdb$view_blr is null')),
                              Table, 'item.name', frozen=True)
            sys_tables, user_tables = tables.split(lambda i: i.is_sys_object(), frozen=True)
            self.__tables = (user_tables, sys_tables, tables)
        return self.__tables
    def _get_all_views(self) -> Tuple[DataList[View], DataList[View], DataList[View]]:
        if self.__views is None:
            self.__fail_if_closed()
            views = DataList((View(self, row) for row
                              in self._select('select * from rdb$relations where rdb$view_blr is not null')),
                             View, 'item.name', frozen=True)
            sys_views, user_views = views.split(lambda i: i.is_sys_object(), frozen=True)
            self.__views = (user_views, sys_views, views)
        return self.__views
    def _get_constraint_indices(self) -> Dict[str, str]:
        if self.__constraint_indices is None:
            self.__fail_if_closed()
            self._ic.execute("""select RDB$INDEX_NAME, RDB$CONSTRAINT_NAME
            from RDB$RELATION_CONSTRAINTS where RDB$INDEX_NAME is not null""")
            self.__constraint_indices = {key.strip(): value.strip() for key, value
                                         in self._ic}
        return self.__constraint_indices
    def _get_all_indices(self) -> Tuple[DataList[Index], DataList[Index], DataList[Index]]:
        if self.__indices is None:
            self.__fail_if_closed()
            # Dummy call to _get_constraint_indices() is necessary as
            # Index.is_sys_object() that is called in Index.__init__() will
            # drop result from internal cursor and we'll not load all indices.
            self._get_constraint_indices()
            cmd = """select RDB$INDEX_NAME, RDB$RELATION_NAME, RDB$INDEX_ID,
            RDB$UNIQUE_FLAG, RDB$DESCRIPTION, RDB$SEGMENT_COUNT, RDB$INDEX_INACTIVE,
            RDB$INDEX_TYPE, RDB$FOREIGN_KEY, RDB$SYSTEM_FLAG, RDB$EXPRESSION_SOURCE,
            RDB$STATISTICS from RDB$INDICES"""
            indices = DataList((Index(self, row) for row in self._select(cmd)),
                               Index, 'item.name', frozen=True)
            sys_indices, user_indices = indices.split(lambda i: i.is_sys_object(), frozen=True)
            self.__indices = (user_indices, sys_indices, indices)
        return self.__indices
    def _get_all_generators(self) -> Tuple[DataList[Sequence], DataList[Sequence], DataList[Sequence]]:
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
    def _get_all_triggers(self) -> Tuple[DataList[Trigger], DataList[Trigger], DataList[Trigger]]:
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
    def _get_all_procedures(self) -> Tuple[DataList[Procedure], DataList[Procedure], DataList[Procedure]]:
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
    def _get_all_functions(self) -> Tuple[DataList[Function], DataList[Function], DataList[Function]]:
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
    def bind(self, connection: Connection) -> Schema:
        """Bind this instance to specified connection`.

        Arguments:
            connection: `~firebird.driver.core.Connection` instance.
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
                               'LEADING', 'LEFT', 'LENGTH', 'LEVEL', 'LIKE', 'LIST', 'LN',
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
        """Drops link to `~firebird.driver.core.Connection`.

        Raises:
            firebird.base.types.Error: When Schema is owned by Connection instance.
        """
        if self.__internal:
            raise Error("Call to 'close' not allowed for embedded Schema.")
        self._close()
        self.__clear()
    def clear(self) -> None:
        """Drop all cached metadata objects.
        """
        self.__clear()
    def reload(self, data: Union[Category, List[Category]]=None) -> None:
        """Commits query transaction and drops all or specified categories of cached
        metadata objects, so they're reloaded from database on next reference.

        Arguments:
            data: `None`, metadata category or list of categories.

        Raises:
            firebird.base.types.Error: For undefined metadata category.
        """
        self.__clear(data)
        if not self.closed:
            self._ic.transaction.commit()
    def get_item(self, name: str, itype: ObjectType, subname: str=None) -> SchemaItem:
        """Return database object by type and name.
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
        elif itype in (ObjectType.PACKAGE, ObjectType.PACKAGE_BODY): # Package
            result = self.packages.get(name)
        return result
    def get_metadata_ddl(self, *, sections=SCRIPT_DEFAULT_ORDER) -> List[str]:
        """Return list of DDL SQL commands for creation of specified categories of database objects.

        Keyword Args:
            sections (list): List of section identifiers.

        Returns:
            List with SQL commands.

        Sections are created in the order of occurence in list. Uses `SCRIPT_DEFAULT_ORDER`
        list when sections are not specified.
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
                    if charset.name != charset.default_collate.name:
                        script.append(charset.get_sql_for('alter',
                                                          collation=charset.default_collate.name))
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
                        if isinstance(obj, (Table, View)):
                            for col in obj.columns:
                                if col.description is not None:
                                    script.append(col.get_sql_for('comment'))
                        elif isinstance(obj, Procedure):
                            if isinstance(obj, (Table, View)):
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
        """Return True if `ident` is a Firebird keyword.
        """
        return ident in self._reserved_
    def is_multifile(self) -> bool:
        """Returns True if database has multiple files.
        """
        return len(self.files) > 0
    def get_collation_by_id(self, charset_id: int, collation_id: int) -> Collation:
        """Get `.Collation` by ID.

        Arguments:
            charset_id: Character set ID.
            collation_id: Collation ID.

        Returns:
            `.Collation` with specified ID or `None`.
        """
        return self.collations.find(lambda i: i.character_set.id == charset_id and i.id == collation_id)
    def get_charset_by_id(self, charset_id: int) -> CharacterSet:
        """
        Arguments:
            charset_id: CharacterSet ID.

        Returns:
            `.CharacterSet` with specified ID or `None`.
        """
        return self.character_sets.find(lambda i: i.id == charset_id)
    def get_privileges_of(self, user: Union[str, UserInfo, Table, View, Procedure, Trigger, Role],
                          user_type: ObjectType=None) -> DataList[Privilege]:
        """Get list of all privileges granted to user/database object.

        Arguments:
            user: User name or instance of class that represents possible user.

        Keyword Args:
            user_type: **Required if** `user` is provided as string name.

        Raises:
            ValueError: When `user` is string name and `user_type` is not provided.
        """
        if isinstance(user, str):
            if user_type is None:
                raise ValueError("Argument user_type required")
            uname = user
            utype = [user_type]
        elif isinstance(user, (Table, View, Procedure, Trigger, Role)):
            uname = user.name
            utype = user._type_code
        elif isinstance(user, UserInfo):
            uname = user.user_name
            utype = [ObjectType.USER]
        return self.privileges.extract(lambda p: (p.user_name == uname)
                                       and (p.user_type in utype), copy=True)
    @property
    def closed(self) -> bool:
        """True if schema is not bound to database connection.
        """
        return self._con is None
    @property
    def description(self) -> Optional[str]:
        """Database description or None if it doesn't have a description.
        """
        return self.__attrs['RDB$DESCRIPTION']
    @property
    def owner_name(self) -> str:
        """Database owner name.
        """
        return self.__owner
    @property
    def default_character_set(self) -> CharacterSet:
        """Default `.CharacterSet` for database.
        """
        return self.character_sets.get(self._default_charset_name)
    @property
    def security_class(self) -> str:
        """Can refer to the security class applied as databasewide access control limits.
        """
        return self.__attrs['RDB$SECURITY_CLASS'].strip()
    @property
    def collations(self) -> DataList[Collation]:
        """List of all collations in database.
        """
        if self.__collations is None:
            self.__fail_if_closed()
            self.__collations = DataList((Collation(self, row) for row
                                          in self._select('select * from rdb$collations')),
                                         Collation, 'item.name', frozen=True)
        return self.__collations
    @property
    def character_sets(self) -> DataList[CharacterSet]:
        """List of all character sets in database.
        """
        if self.__character_sets is None:
            self.__fail_if_closed()
            self.__character_sets = DataList((CharacterSet(self, row) for row
                                              in self._select('select * from rdb$character_sets')),
                                             CharacterSet, 'item.name', frozen=True)
        return self.__character_sets
    @property
    def exceptions(self) -> DataList[DatabaseException]:
        """List of all exceptions in database.
        """
        if self.__exceptions is None:
            self.__fail_if_closed()
            self.__exceptions = DataList((DatabaseException(self, row) for row
                                          in self._select('select * from rdb$exceptions')),
                                         DatabaseException, 'item.name', frozen=True)

        return self.__exceptions
    @property
    def generators(self) -> DataList[Sequence]:
        """List of all user generators in database.
        """
        return self._get_all_generators()[0]
    @property
    def sys_generators(self) -> DataList[Sequence]:
        """List of all system generators in database.
        """
        return self._get_all_generators()[1]
    @property
    def all_generators(self) -> DataList[Sequence]:
        """List of all (system + user) generators in database.
        """
        return self._get_all_generators()[2]
    @property
    def domains(self) ->  DataList[Domain]:
        """List of all user domains in database.
        """
        return self._get_all_domains()[0]
    @property
    def sys_domains(self) ->  DataList[Domain]:
        """List of all system domains in database.
        """
        return self._get_all_domains()[1]
    @property
    def all_domains(self) ->  DataList[Domain]:
        """List of all (system + user) domains in database.
        """
        return self._get_all_domains()[2]
    @property
    def indices(self) -> DataList[Index]:
        """List of all user indices in database.
        """
        return self._get_all_indices()[0]
    @property
    def sys_indices(self) -> DataList[Index]:
        """List of all system indices in database.
        """
        return self._get_all_indices()[1]
    @property
    def all_indices(self) -> DataList[Index]:
        """List of all (system + user) indices in database.
        """
        return self._get_all_indices()[2]
    @property
    def tables(self) -> DataList[Table]:
        """List of all user tables in database.
        """
        return self._get_all_tables()[0]
    @property
    def sys_tables(self) -> DataList[Table]:
        """List of all system tables in database.
        """
        return self._get_all_tables()[1]
    @property
    def all_tables(self) -> DataList[Table]:
        """List of all (system + user) tables in database.
        """
        return self._get_all_tables()[2]
    @property
    def views(self) -> DataList[View]:
        """List of all user views in database.
        """
        return self._get_all_views()[0]
    @property
    def sys_views(self) -> DataList[View]:
        """List of all system views in database.
        """
        return self._get_all_views()[1]
    @property
    def all_views(self) -> DataList[View]:
        """List of all system (system + user) in database.
        """
        return self._get_all_views()[2]
    @property
    def triggers(self) -> DataList[Trigger]:
        """List of all user triggers in database.
        """
        return self._get_all_triggers()[0]
    @property
    def sys_triggers(self) -> DataList[Trigger]:
        """List of all system triggers in database.
        """
        return self._get_all_triggers()[1]
    @property
    def all_triggers(self) -> DataList[Trigger]:
        """List of all (system + user) triggers in database.
        """
        return self._get_all_triggers()[2]
    @property
    def procedures(self) -> DataList[Procedure]:
        """List of all user procedures in database.
        """
        return self._get_all_procedures()[0]
    @property
    def sys_procedures(self) -> DataList[Procedure]:
        """List of all system procedures in database.
        """
        return self._get_all_procedures()[1]
    @property
    def all_procedures(self) -> DataList[Procedure]:
        """List of all (system + user) procedures in database.
        """
        return self._get_all_procedures()[2]
    @property
    def constraints(self) -> DataList[Constraint]:
        """List of all constraints in database.
        """
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
        """List of all roles in database.
        """
        if self.__roles is None:
            self.__fail_if_closed()
            self.__roles = DataList((Role(self, row) for row
                                     in self._select('select * from rdb$roles')),
                                    Role, 'item.name')
            self.__roles.freeze()
        return self.__roles
    @property
    def dependencies(self) -> DataList[Dependency]:
        """List of all dependencies in database.
        """
        if self.__dependencies is None:
            self.__fail_if_closed()
            self.__dependencies = DataList((Dependency(self, row) for row
                                            in self._select('select * from rdb$dependencies')),
                                           Dependency)
        return self.__dependencies
    @property
    def functions(self) -> DataList[Function]:
        """List of all user functions defined in database.
        """
        return self._get_all_functions()[0]
    @property
    def sys_functions(self) -> DataList[Function]:
        """List of all system functions defined in database.
        """
        return self._get_all_functions()[1]
    @property
    def all_functions(self) -> DataList[Function]:
        """List of all (system + user) functions defined in database.
        """
        return self._get_all_functions()[2]
    @property
    def files(self) -> DataList[DatabaseFile]:
        """List of all extension files defined for database.
        """
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
        """List of all shadows defined for database.
        """
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
        """List of all privileges defined for database.
        """
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
        """List of all nbackup hisotry records.
        """
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
        """List of all user-defined BLOB filters.
        """
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
        """List of all packages defined for database.
        """
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
    def linger(self) -> Optional[int]:
        """Database linger value.
        """
        return self.__attrs['RDB$LINGER']

class SchemaItem(Visitable):
    """Base class for all database schema objects.
    """
    schema: Schema = None
    def __init__(self, schema: Schema, attributes: Dict[str, Any]):
        #: Weak reference to parent `.Schema` instance.
        self.schema: Schema = schema if isinstance(schema, weakref.ProxyType) else weakref.proxy(schema)
        self._type_code: List[ObjectType] = []
        self._attributes: Dict[str, Any] = attributes
        self._actions: List[str] = []
    def _strip_attribute(self, attr: str) -> None:
        if self._attributes.get(attr):
            self._attributes[attr] = self._attributes[attr].strip()
    def _check_params(self, params: Dict[str, Any], param_names: List[str]) -> None:
        p = set(params.keys())
        n = set(param_names)
        if not p.issubset(n):
            raise ValueError(f"Unsupported parameter(s) '{','.join(p.difference(n))}'")
    def _needs_quoting(self, ident: str) -> bool:
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
        return f'"{ident}"' if self._needs_quoting(ident) else ident
    def _get_name(self) -> Optional[str]:
        return None
    def _get_create_sql(self, **params) -> str:
        raise NotImplementedError
    def _get_recreate_sql(self, **params) -> str:
        return 'RE'+self._get_create_sql(**params)
    def _get_create_or_alter_sql(self, **params) -> str:
        return 'CREATE OR ALTER' + self._get_create_sql(**params)[6:]
    def is_sys_object(self) -> bool:
        """Returns True if this database object is system object.
        """
        return self._attributes.get('RDB$SYSTEM_FLAG', 0) > 0
    def get_quoted_name(self) -> str:
        """Returns quoted (if necessary) name.
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
        """Returns list of all database objects that this object depend on.
        """
        result = self.schema.dependencies.extract(lambda d: d.dependent_name == self.name and
                                                  d.dependent_type in self._type_code, copy=True)
        result.freeze()
        return result
    def get_sql_for(self, action: str, **params: Dict) -> str:
        """Returns SQL command for specified action on metadata object.

        Supported actions are defined by `.actions` list.

        Raises:
            ValueError: For unsupported action or wrong parameters passed.
        """
        if (_action := action.lower()) in self._actions:
            return getattr(self, f'_get_{_action}_sql')(**params)
        raise ValueError(f"Unsupported action '{action}'")
    @property
    def name(self) -> str:
        """Database object name or None if object doesn't have a name.
        """
        return self._get_name()
    @property
    def description(self) -> str:
        """Database object description or None if object doesn't have a description.
        """
        return self._attributes.get('RDB$DESCRIPTION')
    @property
    def actions(self) -> List[str]:
        """List of supported SQL operations on metadata object instance.
        """
        return self._actions

class Collation(SchemaItem):
    """Represents collation.

    Supported SQL actions:
        - User collation: `create`, `drop`, `comment`
        - System collation: `comment`
    """
    def __init__(self, schema: Schema, attributes: Dict[str, Any]):
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
        "Returns SQL command to DROP collation."
        self._check_params(params, [])
        return f'DROP COLLATION {self.get_quoted_name()}'
    def _get_create_sql(self, **params) -> str:
        "Returns SQL command to CREATE collation."
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
        "Returns SQL command to COMMENT collation."
        comment = 'NULL' if self.description is None \
            else f"'{escape_single_quotes(self.description)}'"
        return f'COMMENT ON COLLATION {self.get_quoted_name()} IS {comment}'
    def _get_name(self) -> str:
        return self._attributes['RDB$COLLATION_NAME']
    def is_based_on_external(self) -> bool:
        """Returns True if collation is based on external collation definition.
        """
        return self._attributes['RDB$BASE_COLLATION_NAME'] and not self.base_collation
    @property
    def id(self) -> int:
        """Collation ID.
        """
        return self._attributes['RDB$COLLATION_ID']
    @property
    def character_set(self) -> CharacterSet:
        """Character set object associated with collation.
        """
        return self.schema.get_charset_by_id(self._attributes['RDB$CHARACTER_SET_ID'])
    @property
    def base_collation(self) -> Collation:
        """Base `.Collation` object that's extended by this one, or None.
        """
        base_name = self._attributes['RDB$BASE_COLLATION_NAME']
        return self.schema.collations.get(base_name) if base_name else None
    @property
    def attributes(self) -> CollationFlag:
        """Collation attributes.
        """
        return CollationFlag(self._attributes['RDB$COLLATION_ATTRIBUTES'])
    @property
    def specific_attributes(self) -> str:
        """Collation specific attributes.
        """
        return self._attributes['RDB$SPECIFIC_ATTRIBUTES']
    @property
    def function_name(self) -> str:
        """Not currently used.
        """
        return self._attributes['RDB$FUNCTION_NAME']
    @property
    def security_class(self) -> str:
        """Security class name or None.
        """
        return self._attributes.get('RDB$SECURITY_CLASS')
    @property
    def owner_name(self) -> str:
        """Creator's user name.
        """
        return self._attributes.get('RDB$OWNER_NAME')

class CharacterSet(SchemaItem):
    """Represents character set.

    Supported SQL actions:
        `alter` (collation=Collation instance or collation name), `comment`
    """
    def __init__(self, schema: Schema, attributes: Dict[str, Any]):
        super().__init__(schema, attributes)
        self._type_code.append(ObjectType.CHARACTER_SET)
        self._strip_attribute('RDB$CHARACTER_SET_NAME')
        self._strip_attribute('RDB$DEFAULT_COLLATE_NAME')
        self._strip_attribute('RDB$SECURITY_CLASS')
        self._strip_attribute('RDB$OWNER_NAME')
        self._actions.extend(['alter', 'comment'])
        self.__collations: DataList= None
    def _get_alter_sql(self, **params) -> str:
        "Returns SQL command to ALTER charset."
        self._check_params(params, ['collation'])
        collation = params.get('collation')
        if collation:
            return f'ALTER CHARACTER SET {self.get_quoted_name()} SET DEFAULT COLLATION ' \
                   f'{collation.get_quoted_name() if isinstance(collation, Collation) else collation}'
        raise ValueError("Missing required parameter: 'collation'.")
    def _get_comment_sql(self, **params) -> str:
        "Returns SQL command to COMMENT charset."
        comment = 'NULL' if self.description is None \
            else f"'{escape_single_quotes(self.description)}'"
        return f'COMMENT ON CHARACTER SET {self.get_quoted_name()} IS {comment}'
    def _get_name(self) -> str:
        return self._attributes['RDB$CHARACTER_SET_NAME']
    def get_collation_by_id(self, id_: int) -> Optional[Collation]:
        """Return :class:`Collation` object with specified `id_` that belongs to
        this character set.
        """
        return self.collations.find(lambda item: item.id == id_)
    @property
    def id(self) -> int:
        """Character set ID.
        """
        return self._attributes['RDB$CHARACTER_SET_ID']
    @property
    def bytes_per_character(self) -> int:
        """Size of characters in bytes.
        """
        return self._attributes['RDB$BYTES_PER_CHARACTER']
    @property
    def default_collate(self) -> Collation:
        """Collate object of default collate.
        """
        return self.collations.get(self._attributes['RDB$DEFAULT_COLLATE_NAME'])
    @property
    def collations(self) -> DataList[Collation]:
        """List of collations associated with character set.
        """
        if self.__collations is None:
            self.__collations = self.schema.collations.extract(lambda i:
                                                               i._attributes['RDB$CHARACTER_SET_ID'] == self.id,
                                                               copy=True)
            self.__collations.freeze()
        return self.__collations
    @property
    def security_class(self) -> str:
        """Security class name or None.
        """
        return self._attributes.get('RDB$SECURITY_CLASS')
    @property
    def owner_name(self) -> str:
        """Creator user name.
        """
        return self._attributes.get('RDB$OWNER_NAME')

class DatabaseException(SchemaItem):
    """Represents database exception.

    Supported SQL actions:
        - User exception: `create`, `recreate`, `alter` (message=string), `create_or_alter`,
          `drop`, `comment`
        - System exception: `comment`
    """
    def __init__(self, schema: Schema, attributes: Dict[str, Any]):
        super().__init__(schema, attributes)
        self._type_code.append(ObjectType.EXCEPTION)
        self._strip_attribute('RDB$EXCEPTION_NAME')
        self._strip_attribute('RDB$SECURITY_CLASS')
        self._strip_attribute('RDB$OWNER_NAME')
        self._actions.append('comment')
        if not self.is_sys_object():
            self._actions.extend(['create', 'recreate', 'alter', 'create_or_alter', 'drop'])
    def _get_create_sql(self, **params) -> str:
        "Returns SQL command to CREATE exception."
        self._check_params(params, [])
        return f"CREATE EXCEPTION {self.get_quoted_name()} '{escape_single_quotes(self.message)}'"
    def _get_alter_sql(self, **params) -> str:
        "Returns SQL command to ALTER exception."
        self._check_params(params, ['message'])
        message = params.get('message')
        if message:
            return f"ALTER EXCEPTION {self.get_quoted_name()} '{escape_single_quotes(message)}'"
        raise ValueError("Missing required parameter: 'message'.")
    def _get_drop_sql(self, **params) -> str:
        "Returns SQL command to DROP exception."
        self._check_params(params, [])
        return f'DROP EXCEPTION {self.get_quoted_name()}'
    def _get_comment_sql(self, **params) -> str:
        "Returns SQL command to COMMENT exception."
        comment = 'NULL' if self.description is None \
            else f"'{escape_single_quotes(self.description)}'"
        return f'COMMENT ON EXCEPTION {self.get_quoted_name()} IS {comment}'
    def _get_name(self) -> str:
        return self._attributes['RDB$EXCEPTION_NAME']
    @property
    def id(self) -> int:
        """System-assigned unique exception number.
        """
        return self._attributes['RDB$EXCEPTION_NUMBER']
    @property
    def message(self) -> str:
        """Custom message text.
        """
        return self._attributes['RDB$MESSAGE']
    @property
    def security_class(self) -> str:
        """Security class name or None.
        """
        return self._attributes.get('RDB$SECURITY_CLASS')
    @property
    def owner_name(self) -> str:
        """Creator's user name.
        """
        return self._attributes.get('RDB$OWNER_NAME')

class Sequence(SchemaItem):
    """Represents database generator/sequence.

    Supported SQL actions:
        - User sequence: `create` (value=number, increment=number),
          `alter` (value=number, increment=number), `drop`, `comment`
        - System sequence: `comment`
    """
    def __init__(self, schema: Schema, attributes: Dict[str, Any]):
        super().__init__(schema, attributes)
        self._type_code.append(ObjectType.GENERATOR)
        self._strip_attribute('RDB$GENERATOR_NAME')
        self._strip_attribute('RDB$SECURITY_CLASS')
        self._strip_attribute('RDB$OWNER_NAME')
        self._actions.append('comment')
        if not self.is_sys_object():
            self._actions.extend(['create', 'alter', 'drop'])
    def _get_create_sql(self, **params) -> str:
        "Returns SQL command to CREATE sequence."
        self._check_params(params, ['value', 'increment'])
        value = params.get('value')
        inc = params.get('increment')
        cmd = f'CREATE {self.schema.opt_generator_keyword} {self.get_quoted_name()} ' \
              f'{f"START WITH {value}" if value else ""} ' \
              f'{f"INCREMENT BY {inc}" if inc else ""}'
        return cmd.strip()
    def _get_alter_sql(self, **params) -> str:
        "Returns SQL command to ALTER sequence."
        self._check_params(params, ['value', 'increment'])
        value = params.get('value')
        inc = params.get('increment')
        cmd = f'ALTER {self.schema.opt_generator_keyword} {self.get_quoted_name()} ' \
              f'{f"RESTART WITH {value}" if isinstance(value,int) else ""} ' \
              f'{f"INCREMENT BY {inc}" if inc else ""}'
        return cmd.strip()
    def _get_drop_sql(self, **params) -> str:
        "Returns SQL command to DROP sequence."
        self._check_params(params, [])
        return f'DROP {self.schema.opt_generator_keyword} {self.get_quoted_name()}'
    def _get_comment_sql(self, **params) -> str:
        "Returns SQL command to COMMENT sequence."
        comment = 'NULL' if self.description is None \
            else f"'{escape_single_quotes(self.description)}'"
        return f'COMMENT ON {self.schema.opt_generator_keyword} {self.get_quoted_name()} IS {comment}'
    def _get_name(self) -> str:
        return self._attributes['RDB$GENERATOR_NAME']
    def is_identity(self) -> bool:
        """Returns True for system generators created for IDENTITY columns.
        """
        return self._attributes['RDB$SYSTEM_FLAG'] == 6
    @property
    def id(self) -> int:
        """Internal ID number of the sequence.
        """
        return self._attributes['RDB$GENERATOR_ID']
    @property
    def value(self) -> int:
        """Current sequence value.
        """
        return self.schema._select_row(f'select GEN_ID({self.get_quoted_name()},0) from RDB$DATABASE')['GEN_ID']
    @property
    def security_class(self) -> str:
        """Security class name or None.
        """
        return self._attributes.get('RDB$SECURITY_CLASS')
    @property
    def owner_name(self) -> str:
        """Creator's user name.
        """
        return self._attributes.get('RDB$OWNER_NAME')
    @property
    def inital_value(self) -> int:
        """Initial sequence value.
        """
        return self._attributes.get('RDB$INITIAL_VALUE')
    @property
    def increment(self) -> int:
        """Sequence increment.
        """
        return self._attributes.get('RDB$GENERATOR_INCREMENT')

class TableColumn(SchemaItem):
    """Represents table column.

    Supported SQL actions:
        - User column: `drop`, `comment`,
          `alter` (name=string, datatype=string_SQLTypeDef, position=number,
          expression=computed_by_expr, restart=None_or_init_value)
        - System column: `comment`
    """
    def __init__(self, schema: Schema, table: Table, attributes: Dict[str, Any]):
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
        "Returns SQL command to ALTER table column."
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
        "Returns SQL command to DROP table column."
        self._check_params(params, [])
        return f'ALTER TABLE {self.table.get_quoted_name()} DROP {self.get_quoted_name()}'
    def _get_comment_sql(self, **params) -> str:
        "Returns SQL command to COMMENT table column."
        comment = 'NULL' if self.description is None \
            else f"'{escape_single_quotes(self.description)}'"
        return f'COMMENT ON COLUMN {self.table.get_quoted_name()}.{self.get_quoted_name()} IS {comment}'
    def _get_name(self) -> str:
        return self._attributes['RDB$FIELD_NAME']
    def get_dependents(self) -> DataList[Dependency]:
        """Return list of all database objects that depend on this one.
        """
        return self.schema.dependencies.extract(lambda d: d.depended_on_name == self._attributes['RDB$RELATION_NAME']
                and d.depended_on_type == 0 and d.field_name == self.name, copy=True)
    def get_dependencies(self) -> DataList[Dependency]:
        """Return list of database objects that this object depend on.
        """
        return self.schema.dependencies.extract(lambda d: d.dependent_name == self._attributes['RDB$RELATION_NAME']
                and d.dependent_type == 0 and d.field_name == self.name, copy=True)
    def get_computedby(self) -> str:
        """Returns extression for column computation or None.
        """
        return self.domain.expression
    def is_computed(self) -> bool:
        """Returns True if column is computed.
        """
        return bool(self.domain.expression)
    def is_domain_based(self) -> bool:
        """Returns True if column is based on user domain.
        """
        return not self.domain.is_sys_object()
    def is_nullable(self) -> bool:
        """Returns True if column can accept NULL values.
        """
        return not self._attributes['RDB$NULL_FLAG']
    def is_writable(self) -> bool:
        """Returns True if column is writable (i.e. it's not computed etc.).
        """
        return bool(self._attributes['RDB$UPDATE_FLAG'])
    def is_identity(self) -> bool:
        """Returns True for identity type column.
        """
        return self._attributes.get('RDB$IDENTITY_TYPE') is not None
    def has_default(self) -> bool:
        """Returns True if column has default value.
        """
        return bool(self._attributes.get('RDB$DEFAULT_SOURCE'))
    @property
    def id(self) -> int:
        """Internam number ID for the column.
        """
        return self._attributes['RDB$FIELD_ID']
    @property
    def table(self) -> Table:
        """The `.Table` object this column belongs to.
        """
        return self.__table
    @property
    def domain(self) -> Domain:
        """`.Domain` object this column is based on.
        """
        return self.schema.all_domains.get(self._attributes['RDB$FIELD_SOURCE'])
    @property
    def position(self) -> int:
        """Column's sequence number in row.
        """
        return self._attributes['RDB$FIELD_POSITION']
    @property
    def security_class(self) -> str:
        """Security class name or None.
        """
        return self._attributes['RDB$SECURITY_CLASS']
    @property
    def default(self) -> str:
        """Default value for column or None.
        """
        result = self._attributes.get('RDB$DEFAULT_SOURCE')
        if result:
            if result.upper().startswith('DEFAULT '):
                result = result[8:]
        return result
    @property
    def collation(self) -> Collation:
        """`.Collation` object or None.
        """
        return self.schema.get_collation_by_id(self.domain._attributes['RDB$CHARACTER_SET_ID'],
                                               self._attributes['RDB$COLLATION_ID'])
    @property
    def datatype(self) -> str:
        """Comlete SQL datatype definition.
        """
        return self.domain.datatype
    @property
    def privileges(self) -> DataList[Privilege]:
        """List of privileges granted to column.
        """
        return self.schema.privileges.extract(lambda p: (p.subject_name == self.table.name and
                                                        p.field_name == self.name and
                                                        p.subject_type in self.table._type_code),
                                                     copy = True)
    @property
    def generator(self) -> Sequence:
        """Identity `.Sequence`.
        """
        return self.schema.all_generators.get(self._attributes.get('RDB$GENERATOR_NAME'))
    @property
    def identity_type(self) -> int:
        """Identity type, None for normal columns.
        """
        return self._attributes.get('RDB$IDENTITY_TYPE')

class Index(SchemaItem):
    """Represents database index.

    Supported SQL actions:
        - User index: `create`, `activate`, `deactivate`, `recompute`, `drop`, `comment`
        - System index: `activate`, `recompute`, `comment`
    """
    def __init__(self, schema: Schema, attributes: Dict[str, Any]):
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
        "Returns SQL command to CREATE index."
        self._check_params(params, [])
        return f"CREATE {'UNIQUE ' if self.is_unique() else ''}{self.index_type.value} " \
               f"INDEX {self.get_quoted_name()} ON {self.table.get_quoted_name()} " \
               f"{f'COMPUTED BY {self.expression}' if self.is_expression() else '(%s)' % ','.join(self.segment_names)}"
    def _get_activate_sql(self, **params) -> str:
        "Returns SQL command to ACTIVATE index."
        self._check_params(params, [])
        return f'ALTER INDEX {self.get_quoted_name()} ACTIVE'
    def _get_deactivate_sql(self, **params) -> str:
        "Returns SQL command to DEACTIVATE index."
        self._check_params(params, [])
        return f'ALTER INDEX {self.get_quoted_name()} INACTIVE'
    def _get_recompute_sql(self, **params) -> str:
        "Returns SQL command to recompute index statistics."
        self._check_params(params, [])
        return f'SET STATISTICS INDEX {self.get_quoted_name()}'
    def _get_drop_sql(self, **params) -> str:
        "Returns SQL command to DROP index."
        self._check_params(params, [])
        return f'DROP INDEX {self.get_quoted_name()}'
    def _get_comment_sql(self, **params) -> str:
        "Returns SQL command to COMMENT index."
        comment = 'NULL' if self.description is None \
            else f"'{escape_single_quotes(self.description)}'"
        return f'COMMENT ON INDEX {self.get_quoted_name()} IS {comment}'
    def _get_name(self) -> str:
        return self._attributes['RDB$INDEX_NAME']
    def is_sys_object(self) -> bool:
        """Returns True if this database object is system object.
        """
        return bool(self._attributes['RDB$SYSTEM_FLAG']
                    or (self.is_enforcer() and self.name.startswith('RDB$')))
    def is_expression(self) -> bool:
        """Returns True if index is expression index.
        """
        return not self.segments
    def is_unique(self) -> bool:
        """Returns True if index is UNIQUE.
        """
        return self._attributes['RDB$UNIQUE_FLAG'] == 1
    def is_inactive(self) -> bool:
        """Returns True if index is INACTIVE.
        """
        return self._attributes['RDB$INDEX_INACTIVE'] == 1
    def is_enforcer(self) -> bool:
        """Returns True if index is used to enforce a constraint.
        """
        return self.name in self.schema._get_constraint_indices()
    @property
    def table(self) -> Table:
        """The `.Table` instance the index applies to.
        """
        return self.schema.all_tables.get(self._attributes['RDB$RELATION_NAME'])
    @property
    def id(self) -> int:
        """Internal number ID of the index.
        """
        return self._attributes['RDB$INDEX_ID']
    @property
    def index_type(self) -> IndexType:
        """Index type (ASCENDING or DESCENDING).
        """
        return (IndexType.DESCENDING if self._attributes['RDB$INDEX_TYPE'] == 1
                else IndexType.ASCENDING)
    @property
    def partner_index(self) -> Index:
        """Associated unique/primary key :class:`Index` instance, or None.
        """
        return (self.schema.all_indices.get(pname) if (pname := self._attributes['RDB$FOREIGN_KEY'])
                else None)
    @property
    def expression(self) -> str:
        """Source of an expression or None.
        """
        return self._attributes['RDB$EXPRESSION_SOURCE']
    @property
    def statistics(self) -> float:
        """Latest selectivity of the index.
        """
        return self._attributes['RDB$STATISTICS']
    @property
    def segment_names(self) -> List[str]:
        """List of index segment names.
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
    def segment_statistics(self) -> List[float]:
        """List of index segment statistics.
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
        """List of index segments (table columns).
        """
        return DataList(self.table.columns.get(colname) for colname in self.segment_names)
    @property
    def constraint(self) -> Constraint:
        """`Constraint` instance that uses this index or None.
        """
        return self.schema.constraints.get(self.schema._get_constraint_indices().get(self.name))

class ViewColumn(SchemaItem):
    """Represents view column.

    Supported SQL actions:
        `comment`
    """
    def __init__(self, schema: Schema, view: View, attributes: Dict[str, Any]):
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
        "Returns SQL command to CREATE view column."
        comment = 'NULL' if self.description is None \
            else f"'{escape_single_quotes(self.description)}'"
        return f'COMMENT ON COLUMN {self.view.get_quoted_name()}.{self.get_quoted_name()} IS {comment}'
    def _get_name(self) -> str:
        return self._attributes['RDB$FIELD_NAME']
    def get_dependents(self) -> DataList[Dependency]:
        """Return list of all database objects that depend on this one.
        """
        return self.schema.dependencies.extract(lambda d: d.depended_on_name == self._attributes['RDB$RELATION_NAME']
                and d.depended_on_type == 1 and d.field_name == self.name, copy=True)
    def get_dependencies(self) -> DataList[Dependency]:
        """Return list of database objects that this object depend on.
        """
        return self.schema.dependencies.extract(lambda d: d.dependent_name == self._attributes['RDB$RELATION_NAME']
                and d.dependent_type == 1 and d.field_name == self.name, copy=True)
    def is_nullable(self) -> bool:
        """Returns True if column is NULLABLE.
        """
        return not self._attributes['RDB$NULL_FLAG']
    def is_writable(self) -> bool:
        """Returns True if column is writable.
        """
        return bool(self._attributes['RDB$UPDATE_FLAG'])
    @property
    def base_field(self) -> Union[TableColumn, ViewColumn, ProcedureParameter]:
        """The source column from the base relation. Result could be either `.TableColumn`,
        `.ViewColumn` or `.ProcedureParameter` instance or None.
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
        """View object this column belongs to.
        """
        return self.__view
    @property
    def domain(self) -> Domain:
        """Domain object this column is based on.
        """
        return self.schema.all_domains.get(self._attributes['RDB$FIELD_SOURCE'])
    @property
    def position(self) -> int:
        """Column's sequence number in row.
        """
        return self._attributes['RDB$FIELD_POSITION']
    @property
    def security_class(self) -> str:
        """Security class name or None.
        """
        return self._attributes['RDB$SECURITY_CLASS']
    @property
    def collation(self) -> Collation:
        """Collation object or None.
        """
        return self.schema.get_collation_by_id(self.domain._attributes['RDB$CHARACTER_SET_ID'],
                                               self._attributes['RDB$COLLATION_ID'])
    @property
    def datatype(self) -> str:
        """Complete SQL datatype definition.
        """
        return self.domain.datatype
    @property
    def privileges(self) -> DataList[Privilege]:
        """List of privileges granted to column.
        """
        # Views are logged as Tables in RDB$USER_PRIVILEGES
        return self.schema.privileges.extract(lambda p: (p.subject_name == self.view.name and
                                                         p.field_name == self.name and
                                                         p.subject_type == 0), copy=True)

class Domain(SchemaItem):
    """Represents SQl Domain.

    Supported SQL actions:
        - User domain: `create`, `drop`, `comment`,
          `alter` (name=string, default=string_definition_or_None,
          check=string_definition_or_None, datatype=string_SQLTypeDef)
        - System domain: `comment`
    """
    def __init__(self, schema: Schema, attributes: Dict[str, Any]):
        super().__init__(schema, attributes)
        self._type_code.append(ObjectType.COLUMN)
        self._strip_attribute('RDB$FIELD_NAME')
        self._strip_attribute('RDB$SECURITY_CLASS')
        self._strip_attribute('RDB$OWNER_NAME')
        self._actions.append('comment')
        if not self.is_sys_object():
            self._actions.extend(['create', 'alter', 'drop'])
    def _get_create_sql(self, **params) -> str:
        "Returns SQL command to CREATE domain."
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
        "Returns SQL command to ALTER domain."
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
        "Returns SQL command to DROP domain."
        self._check_params(params, [])
        return f'DROP DOMAIN {self.get_quoted_name()}'
    def _get_comment_sql(self, **params) -> str:
        "Returns SQL command to COMMENT dimain."
        comment = 'NULL' if self.description is None \
            else f"'{escape_single_quotes(self.description)}'"
        return f'COMMENT ON DOMAIN {self.get_quoted_name()} IS {comment}'
    def _get_name(self) -> str:
        return self._attributes['RDB$FIELD_NAME']
    def is_sys_object(self) -> bool:
        """Return True if this database object is system object.
        """
        return (self._attributes['RDB$SYSTEM_FLAG'] == 1) or self.name.startswith('RDB$')
    def is_nullable(self) -> bool:
        """Returns True if domain is not defined with NOT NULL.
        """
        return not self._attributes['RDB$NULL_FLAG']
    def is_computed(self) -> bool:
        """Returns True if domain is computed.
        """
        return bool(self._attributes['RDB$COMPUTED_SOURCE'])
    def is_validated(self) -> bool:
        """Returns True if domain has validation constraint.
        """
        return bool(self._attributes['RDB$VALIDATION_SOURCE'])
    def is_array(self) -> bool:
        """Returns True if domain defines an array.
        """
        return bool(self._attributes['RDB$DIMENSIONS'])
    def has_default(self) -> bool:
        """Returns True if domain has default value.
        """
        return bool(self._attributes['RDB$DEFAULT_SOURCE'])
    @property
    def expression(self) -> str:
        """Expression that defines the COMPUTED BY column or None.
        """
        return self._attributes['RDB$COMPUTED_SOURCE']
    @property
    def validation(self) -> str:
        """CHECK constraint for the domain or None.
        """
        return self._attributes['RDB$VALIDATION_SOURCE']
    @property
    def default(self) -> str:
        """Expression that defines the default value or None.
        """
        if result := self._attributes.get('RDB$DEFAULT_SOURCE'):
            if result.upper().startswith('DEFAULT '):
                result = result[8:]
        return result
    @property
    def length(self) -> int:
        """Length of the column in bytes.
        """
        return self._attributes['RDB$FIELD_LENGTH']
    @property
    def scale(self) -> int:
        """Negative number representing the scale of NUMBER and DECIMAL column.
        """
        return self._attributes['RDB$FIELD_SCALE']
    @property
    def field_type(self) -> FieldType:
        """Number code of the data type defined for the column.
        """
        return FieldType(self._attributes['RDB$FIELD_TYPE'])
    @property
    def sub_type(self) -> int:
        """Field sub-type.
        """
        return self._attributes['RDB$FIELD_SUB_TYPE']
    @property
    def segment_length(self) -> int:
        """For BLOB columns, a suggested length for BLOB buffers.
        """
        return self._attributes['RDB$SEGMENT_LENGTH']
    @property
    def external_length(self) -> int:
        """Length of field as it is in an external table. Always 0 for regular tables.
        """
        return self._attributes['RDB$EXTERNAL_LENGTH']
    @property
    def external_scale(self) -> int:
        """Scale factor of an integer field as it is in an external table.
        """
        return self._attributes['RDB$EXTERNAL_SCALE']
    @property
    def external_type(self) -> FieldType:
        """Data type of the field as it is in an external table.
        """
        if (value := self._attributes['RDB$EXTERNAL_TYPE']) is not None:
            return FieldType(value)
        return None
    @property
    def dimensions(self) -> List[Tuple[int, int]]:
        """List of dimension definition pairs if column is an array type. Always empty
        for non-array columns.
        """
        if self._attributes['RDB$DIMENSIONS']:
            return self.schema._get_field_dimensions(self)
        return []
    @property
    def character_length(self) -> int:
        """Length of CHAR and VARCHAR column, in characters (not bytes).
        """
        return self._attributes['RDB$CHARACTER_LENGTH']
    @property
    def collation(self) -> Collation:
        """Collation object for a character column or None.
        """
        return self.schema.get_collation_by_id(self._attributes['RDB$CHARACTER_SET_ID'],
                                               self._attributes['RDB$COLLATION_ID'])
    @property
    def character_set(self) -> CharacterSet:
        """CharacterSet object for a character or text BLOB column, or None.
        """
        return self.schema.get_charset_by_id(self._attributes['RDB$CHARACTER_SET_ID'])
    @property
    def precision(self) -> int:
        """Indicates the number of digits of precision available to the data type of the column.
        """
        return self._attributes['RDB$FIELD_PRECISION']
    @property
    def datatype(self) -> str:
        """Comlete SQL datatype definition.
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
            l.append('[%s]' % ', '.join(f'{u}' if l == 1 else f'{l}:{u}'
                                        for l, u in self.dimensions))
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
    def security_class(self) -> str:
        """Security class name or None.
        """
        return self._attributes.get('RDB$SECURITY_CLASS')
    @property
    def owner_name(self) -> str:
        """Creator's user name.
        """
        return self._attributes.get('RDB$OWNER_NAME')

class Dependency(SchemaItem):
    """Maps dependency between database objects.

    Supported SQL actions:
        `none`
    """
    def __init__(self, schema: Schema, attributes: Dict[str, Any]):
        super().__init__(schema, attributes)
        self._strip_attribute('RDB$DEPENDENT_NAME')
        self._strip_attribute('RDB$DEPENDED_ON_NAME')
        self._strip_attribute('RDB$FIELD_NAME')
        self._strip_attribute('RDB$PACKAGE_NAME')
    def is_sys_object(self) -> bool:
        """Returns True as dependency entries are considered as system objects.
        """
        return True
    def get_dependents(self) -> DataList:
        """Returns empty list because Dependency object never has dependents.
        """
        return DataList()
    def get_dependencies(self) -> DataList:
        """Returns empty list because Dependency object never has dependencies.
        """
        return DataList()
    def is_packaged(self) -> bool:
        """Returns True if dependency is defined in package.
        """
        return bool(self._attributes.get('RDB$PACKAGE_NAME'))
    @property
    def dependent(self) -> SchemaItem:
        """Dependent database object.
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
        """Dependent database object name.
        """
        return self._attributes['RDB$DEPENDENT_NAME']
    @property
    def dependent_type(self) -> ObjectType:
        """Dependent database object type.
        """
        return ObjectType(value) if (value := self._attributes['RDB$DEPENDENT_TYPE']) is not None else None
    @property
    def field_name(self) -> str:
        """Name of one column in `depended on` object.
        """
        return self._attributes['RDB$FIELD_NAME']
    @property
    def depended_on(self) -> SchemaItem:
        """Database object on which dependent depends.
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
        """Name of db object on which dependent depends.
        """
        return self._attributes['RDB$DEPENDED_ON_NAME']
    @property
    def depended_on_type(self) -> ObjectType:
        """Type of db object on which dependent depends.
        """
        return ObjectType(value) if (value := self._attributes['RDB$DEPENDED_ON_TYPE']) is not None else None
    @property
    def package(self) -> Package:
        """`.Package` instance if dependent depends on object in package or None.
        """
        return self.schema.packages.get(self._attributes.get('RDB$PACKAGE_NAME'))

class Constraint(SchemaItem):
    """Represents table or column constraint.

    Supported SQL actions:
        - Constraint on user table except NOT NULL constraint: `create`, `drop`
        - Constraint on system table: `none`
    """
    def __init__(self, schema: Schema, attributes: Dict[str, Any]):
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
        "Returns SQL command to CREATE constraint."
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
        "Returns SQL command to DROP constraint."
        self._check_params(params, [])
        return f'ALTER TABLE {self.table.get_quoted_name()} DROP CONSTRAINT {self.get_quoted_name()}'
    def _get_name(self) -> str:
        return self._attributes['RDB$CONSTRAINT_NAME']
    def is_sys_object(self) -> bool:
        """Returns True if this database object is system object.
        """
        return self.schema.all_tables.get(self._attributes['RDB$RELATION_NAME']).is_sys_object()
    def is_not_null(self) -> bool:
        """Returns True if it's NOT NULL constraint.
        """
        return self.constraint_type == ConstraintType.NOT_NULL
    def is_pkey(self) -> bool:
        """Returns True if it's PRIMARY KEY constraint.
        """
        return self.constraint_type == ConstraintType.PRIMARY_KEY
    def is_fkey(self) -> bool:
        """Returns True if it's FOREIGN KEY constraint.
        """
        return self.constraint_type == ConstraintType.FOREIGN_KEY
    def is_unique(self) -> bool:
        """Returns True if it's UNIQUE constraint.
        """
        return self.constraint_type == ConstraintType.UNIQUE
    def is_check(self) -> bool:
        """Returns True if it's CHECK constraint.
        """
        return self.constraint_type == ConstraintType.CHECK
    def is_deferrable(self) -> bool:
        """Returns True if it's DEFERRABLE constraint.
        """
        return self._attributes['RDB$DEFERRABLE'] != 'NO'
    def is_deferred(self) -> bool:
        """Returns True if it's INITIALLY DEFERRED constraint.
        """
        return self._attributes['RDB$INITIALLY_DEFERRED'] != 'NO'
    @property
    def constraint_type(self) -> ConstraintType:
        """Constraint type -> primary key/unique/foreign key/check/not null.
        """
        return ConstraintType(self._attributes['RDB$CONSTRAINT_TYPE'])
    @property
    def table(self) -> Table:
        """`.Table` instance this constraint applies to.
        """
        return self.schema.all_tables.get(self._attributes['RDB$RELATION_NAME'])
    @property
    def index(self) -> Index:
        """`.Index` instance that enforces the constraint.
        `None` if constraint is not primary key/unique or foreign key.
        """
        return self.schema.all_indices.get(self._attributes['RDB$INDEX_NAME'])
    @property
    def trigger_names(self) -> List[str]:
        """For a CHECK constraint contains trigger names that enforce the constraint.
        """
        if self.is_check():
            return self._attributes['RDB$TRIGGER_NAME']
        return []
    @property
    def triggers(self) -> DataList[Trigger]:
        """List of triggers that enforce the CHECK constraint.
        """
        return self.schema.all_triggers.extract(lambda x: x.name in self.trigger_names, copy=True)
    @property
    def column_name(self) -> str:
        """For a NOT NULL constraint, this is the name of the column to which
        the constraint applies.
        """
        return self._attributes['RDB$TRIGGER_NAME'] if self.is_not_null() else None
    @property
    def partner_constraint(self) -> Constraint:
        """For a FOREIGN KEY constraint, this is the unique or primary key
        `.Constraint` referred.
        """
        return self.schema.constraints.get(self._attributes['RDB$CONST_NAME_UQ'])
    @property
    def match_option(self) -> str:
        """For a FOREIGN KEY constraint only. Current value is FULL in all cases.
        """
        return self._attributes['RDB$MATCH_OPTION']
    @property
    def update_rule(self) -> str:
        """For a FOREIGN KEY constraint, this is the action applicable to when primary key
        is updated.
        """
        return self._attributes['RDB$UPDATE_RULE']
    @property
    def delete_rule(self) -> str:
        """For a FOREIGN KEY constraint, this is the action applicable to when primary key
        is deleted.
        """
        return self._attributes['RDB$DELETE_RULE']

class Table(SchemaItem):
    """Represents Table in database.

    Supported SQL actions:
        - User table: `create` (no_pk=bool, no_unique=bool), `recreate` (no_pk=bool, no_unique=bool),
          `drop`, `comment`, `insert (update=bool, returning=list[str], matching=list[str])`
        - System table: `comment`
    """
    def __init__(self, schema: Schema, attributes: Dict[str, Any]):
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
        "Returns SQL command to INSERT data to table."
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
        "Returns SQL command to CREATE table."
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
        "Returns SQL command to DROP table."
        self._check_params(params, [])
        return f'DROP TABLE {self.get_quoted_name()}'
    def _get_comment_sql(self, **params) -> str:
        "Returns SQL command to COMMENT table."
        comment = 'NULL' if self.description is None \
            else f"'{escape_single_quotes(self.description)}'"
        return f'COMMENT ON TABLE {self.get_quoted_name()} IS {comment}'
    def _get_name(self) -> str:
        return self._attributes['RDB$RELATION_NAME']
    def is_gtt(self) -> bool:
        """Returns True if table is GLOBAL TEMPORARY table.
        """
        return self.table_type in (RelationType.GLOBAL_TEMPORARY_DELETE,
                                   RelationType.GLOBAL_TEMPORARY_PRESERVE)
    def is_persistent(self) -> bool:
        """Returns True if table is persistent one.
        """
        return self.table_type in (RelationType.PERSISTENT, RelationType.EXTERNAL)
    def is_external(self) -> bool:
        """Returns True if table is external table.
        """
        return bool(self.external_file)
    def has_pkey(self) -> bool:
        """Returns True if table has PRIMARY KEY defined.
        """
        for const in self.constraints:
            if const.is_pkey():
                return True
        return False
    def has_fkey(self) -> bool:
        """Returns True if table has any FOREIGN KEY constraint.
        """
        for const in self.constraints:
            if const.is_fkey():
                return True
        return False
    @property
    def id(self) -> int:
        """Internal number ID for the table.
        """
        return self._attributes['RDB$RELATION_ID']
    @property
    def dbkey_length(self) -> int:
        """Length of the RDB$DB_KEY column in bytes.
        """
        return self._attributes['RDB$DBKEY_LENGTH']
    @property
    def format(self) -> int:
        """Internal format ID for the table.
        """
        return self._attributes['RDB$FORMAT']
    @property
    def table_type(self) -> RelationType:
        """Table type.
        """
        return RelationType(self._attributes.get('RDB$RELATION_TYPE'))
    @property
    def security_class(self) -> str:
        """Security class that define access limits to the table.
        """
        return self._attributes['RDB$SECURITY_CLASS']
    @property
    def external_file(self) -> str:
        """Full path to the external data file, if any.
        """
        return self._attributes['RDB$EXTERNAL_FILE']
    @property
    def owner_name(self) -> str:
        """User name of table's creator.
        """
        return self._attributes['RDB$OWNER_NAME']
    @property
    def default_class(self) -> str:
        """Default security class.
        """
        return self._attributes['RDB$DEFAULT_CLASS']
    @property
    def flags(self) -> int:
        """Internal flags.
        """
        return self._attributes['RDB$FLAGS']
    @property
    def primary_key(self) -> Optional[Constraint]:
        """PRIMARY KEY constraint for this table or None.
        """
        return self.constraints.find(lambda c: c.is_pkey())
    @property
    def foreign_keys(self) -> DataList[Constraint]:
        """List of FOREIGN KEY constraints for this table.
        """
        return self.constraints.extract(lambda c: c.is_fkey(), copy=True)
    @property
    def columns(self) -> DataList[TableColumn]:
        """List of columns defined for table.
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
        """List of constraints defined for table.
        """
        return self.schema.constraints.extract(lambda c: c._attributes['RDB$RELATION_NAME'] == self.name,
                                               copy=True)
    @property
    def indices(self) -> DataList[Index]:
        """List of indices defined for table.
        """
        return self.schema.all_indices.extract(lambda i: i._attributes['RDB$RELATION_NAME'] == self.name,
                                               copy=True)
    @property
    def triggers(self) -> DataList[Trigger]:
        """List of triggers defined for table.
        """
        return self.schema.triggers.extract(lambda t: t._attributes['RDB$RELATION_NAME'] == self.name,
                                            copy=True)
    @property
    def privileges(self) -> DataList[Privilege]:
        """List of privileges to table.
        """
        return self.schema.privileges.extract(lambda p: ((p.subject_name == self.name) and
                                                         (p.subject_type in self._type_code)),
                                                     copy=True)

class View(SchemaItem):
    """Represents database View.

    Supported SQL actions:
        - User views: `create`, `recreate`, `alter` (columns=string_or_list, query=string,check=bool),
          `create_or_alter`, `drop`, `comment`
        - System views: `comment`
    """
    def __init__(self, schema: Schema, attributes: Dict[str, Any]):
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
        "Returns SQL command to CREATE view."
        self._check_params(params, [])
        return f"CREATE VIEW {self.get_quoted_name()}" \
               f" ({','.join([col.get_quoted_name() for col in self.columns])})\n" \
               f"   AS\n     {self.sql}"
    def _get_alter_sql(self, **params) -> str:
        "Returns SQL command to ALTER view."
        self._check_params(params, ['columns', 'query', 'check'])
        columns = params.get('columns')
        if isinstance(columns, (list, tuple)):
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
        "Returns SQL command to DROP view."
        self._check_params(params, [])
        return f'DROP VIEW {self.get_quoted_name()}'
    def _get_comment_sql(self, **params) -> str:
        "Returns SQL command to COMMENT view."
        comment = 'NULL' if self.description is None \
            else f"'{escape_single_quotes(self.description)}'"
        return f'COMMENT ON VIEW {self.get_quoted_name()} IS {comment}'
    def _get_name(self) -> str:
        return self._attributes['RDB$RELATION_NAME']
    def has_checkoption(self) -> bool:
        """Returns True if View has WITH CHECK OPTION defined.
        """
        return "WITH CHECK OPTION" in self.sql.upper()
    @property
    def id(self) -> int:
        """Internal number ID for the view.
        """
        return self._attributes['RDB$RELATION_ID']
    @property
    def sql(self) -> str:
        """The query specification.
        """
        return self._attributes['RDB$VIEW_SOURCE']
    @property
    def dbkey_length(self) -> int:
        """Length of the RDB$DB_KEY column in bytes.
        """
        return self._attributes['RDB$DBKEY_LENGTH']
    @property
    def format(self) -> int:
        """Internal format ID for the view.
        """
        return self._attributes['RDB$FORMAT']
    @property
    def security_class(self) -> str:
        """Security class that define access limits to the view.
        """
        return self._attributes['RDB$SECURITY_CLASS']
    @property
    def owner_name(self) -> str:
        """User name of view's creator.
        """
        return self._attributes['RDB$OWNER_NAME']
    @property
    def default_class(self) -> str:
        """Default security class.
        """
        return self._attributes['RDB$DEFAULT_CLASS']
    @property
    def flags(self) -> int:
        """Internal flags.
        """
        return self._attributes['RDB$FLAGS']
    @property
    def columns(self) -> DataList[ViewColumn]:
        """List of columns defined for view.
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
        """List of triggers defined for view.
        """
        return self.schema.triggers.extract(lambda t:
                                            t._attributes['RDB$RELATION_NAME'] == self.name,
                                            copy=True)
    @property
    def privileges(self) -> DataList[Privilege]:
        """List of privileges granted to view.
        """
        # Views are logged as Tables in RDB$USER_PRIVILEGES
        return self.schema.privileges.extract(lambda p: ((p.subject_name == self.name) and
                                                         (p.subject_type == 0)), copy=True)

class Trigger(SchemaItem):
    """Represents trigger.

    Supported SQL actions:
        - User trigger: `create` (inactive=bool), `recreate`, `create_or_alter`, `drop`, `comment`,
          `alter` (fire_on=string, active=bool,sequence=int, declare=string_or_list, code=string_or_list)
        - System trigger: `comment`
    """
    def __init__(self, schema: Schema, attributes: Dict[str, Any]):
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
        "Returns SQL command to CREATE trigger."
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
        "Returns SQL command to ALTER trigger."
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
            elif isinstance(declare, (list, tuple)):
                d = ''
                for x in declare:
                    d += f'  {x}\n'
            else:
                d = f'{declare}\n'
            if isinstance(code, (list, tuple)):
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
        "Returns SQL command to DROP trigger."
        self._check_params(params, [])
        return f'DROP TRIGGER {self.get_quoted_name()}'
    def _get_comment_sql(self, **params) -> str:
        "Returns SQL command to COMMENT trigger."
        comment = 'NULL' if self.description is None \
            else f"'{escape_single_quotes(self.description)}'"
        return f'COMMENT ON TRIGGER {self.get_quoted_name()} IS {comment}'
    def _get_name(self) -> str:
        return self._attributes['RDB$TRIGGER_NAME']
    def __ru(self, value: IntEnum) -> str:
        return value.name.replace('_', ' ')
    def _get_action_type(self, slot: int) -> DMLTrigger:
        if (code := ((self._attributes['RDB$TRIGGER_TYPE'] + 1) >> (slot * 2 - 1)) & 3) > 0:
            return self.__m[code - 1]
        return None
    def is_before(self) -> bool:
        """Returns True if this trigger is set for BEFORE action.
        """
        return self.time is TriggerTime.BEFORE
    def is_after(self) -> bool:
        """Returns True if this trigger is set for AFTER action.
        """
        return self.time is TriggerTime.AFTER
    def is_db_trigger(self) -> bool:
        """Returns True if this trigger is database trigger.
        """
        return self.trigger_type is TriggerType.DB
    def is_ddl_trigger(self) -> bool:
        """Returns True if this trigger is DDL trigger.
        """
        return self.trigger_type is TriggerType.DDL
    def is_insert(self) -> bool:
        """Returns True if this trigger is set for INSERT operation.
        """
        return DMLTrigger.INSERT in self.action if self.trigger_type is TriggerType.DML else False
    def is_update(self) -> bool:
        """Returns True if this trigger is set for UPDATE operation.
        """
        return DMLTrigger.UPDATE in self.action if self.trigger_type is TriggerType.DML else False
    def is_delete(self) -> bool:
        """Returns True if this trigger is set for DELETE operation.
        """
        return DMLTrigger.DELETE in self.action if self.trigger_type is TriggerType.DML else False
    def get_type_as_string(self) -> str:
        """Return string with action and operation specification.
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
    def relation(self) -> Union[Table, View, None]:
        """`.Table` or `.View` that the trigger is for, or None for database triggers.
        """
        rel = self.schema.all_tables.get(relname := self._attributes['RDB$RELATION_NAME'])
        if not rel:
            rel = self.schema.all_views.get(relname)
        return rel
    @property
    def sequence(self) -> int:
        """Sequence (position) of trigger. Zero usually means no sequence defined.
        """
        return self._attributes['RDB$TRIGGER_SEQUENCE']
    @property
    def trigger_type(self) -> TriggerType:
        """Trigger type.
        """
        return TriggerType(self._attributes['RDB$TRIGGER_TYPE'] & (0x3 << 13))
    @property
    def action(self) -> Union[DMLTrigger, DBTrigger, DDLTrigger]:
        """Trigger action type.
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
        """Trigger time (BEFORE/AFTER event).
        """
        return TriggerTime((self._attributes['RDB$TRIGGER_TYPE'] + (0 if self.is_ddl_trigger() else 1)) & 1)
    @property
    def source(self) -> str:
        """PSQL source code.
        """
        return self._attributes['RDB$TRIGGER_SOURCE']
    @property
    def flags(self) -> int:
        """Internal flags.
        """
        return self._attributes['RDB$FLAGS']
    @property
    def valid_blr(self) -> bool:
        """Trigger BLR invalidation flag. Coul be True/False or None.
        """
        result = self._attributes.get('RDB$VALID_BLR')
        return bool(result) if result is not None else None
    @property
    def engine_name(self) -> str:
        """Engine name.
        """
        return self._attributes.get('RDB$ENGINE_NAME')
    @property
    def entrypoint(self) -> str:
        """Entrypoint.
        """
        return self._attributes.get('RDB$ENTRYPOINT')
    @property
    def active(self) -> bool:
        """True if this trigger is active.
        """
        return self._attributes['RDB$TRIGGER_INACTIVE'] == 0

class ProcedureParameter(SchemaItem):
    """Represents procedure parameter.

    Supported SQL actions:
        `comment`
    """
    def __init__(self, schema: Schema, proc: Procedure, attributes: Dict[str, Any]):
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
        "Returns SQL command to COMMENT procedure parameter."
        comment = 'NULL' if self.description is None \
            else f"'{escape_single_quotes(self.description)}'"
        return f'COMMENT ON PARAMETER {self.procedure.get_quoted_name()}.{self.get_quoted_name()} IS {comment}'
    def _get_name(self) -> str:
        return self._attributes['RDB$PARAMETER_NAME']
    def get_sql_definition(self) -> str:
        """Returns SQL definition for parameter.
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
        """Returns True if parameter is INPUT parameter.
        """
        return self.parameter_type is ParameterType.INPUT
    def is_nullable(self) -> bool:
        """Returns True if parameter allows NULL.
        """
        return not bool(self._attributes.get('RDB$NULL_FLAG'))
    def has_default(self) -> bool:
        """Returns True if parameter has default value.
        """
        return bool(self._attributes.get('RDB$DEFAULT_SOURCE'))
    def is_packaged(self) -> bool:
        """Returns True if procedure parameter is defined in package.
        """
        return bool(self._attributes.get('RDB$PACKAGE_NAME'))
    @property
    def procedure(self) -> Procedure:
        """`.Procedure` instance to which this parameter belongs.
        """
        return self.schema.all_procedures.get(self._attributes['RDB$PROCEDURE_NAME'])
    @property
    def sequence(self) -> int:
        """Sequence (position) of parameter.
        """
        return self._attributes['RDB$PARAMETER_NUMBER']
    @property
    def domain(self) -> Domain:
        """`.Domain` for this parameter.
        """
        return self.schema.all_domains.get(self._attributes['RDB$FIELD_SOURCE'])
    @property
    def parameter_type(self) -> ParameterType:
        """Parameter type (INPUT/OUTPUT).
        """
        return ParameterType(self._attributes['RDB$PARAMETER_TYPE'])
    @property
    def datatype(self) -> str:
        """Comlete SQL datatype definition.
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
        """Source for parameter data type.
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
    def default(self) -> str:
        """Default value.
        """
        if result := self._attributes.get('RDB$DEFAULT_SOURCE'):
            if result.upper().startswith('= '):
                result = result[2:]
            elif result.upper().startswith('DEFAULT '):
                result = result[8:]
        return result
    @property
    def collation(self) -> Collation:
        """`.Collation` for this parameter.
        """
        return (None if (cid := self._attributes.get('RDB$COLLATION_ID')) is None
                else self.schema.get_collation_by_id(self.domain._attributes['RDB$CHARACTER_SET_ID'], cid))
    @property
    def mechanism(self) -> Mechanism:
        """Parameter mechanism code.
        """
        return Mechanism(code) if (code := self._attributes.get('RDB$PARAMETER_MECHANISM')) is not None else None
    @property
    def column(self) -> TableColumn:
        """`.TableColumn` for this parameter.
        """
        return (None if (rname := self._attributes.get('RDB$RELATION_NAME')) is None
                else self.schema.all_tables.get(rname).columns.get(self._attributes['RDB$FIELD_NAME']))
    @property
    def package(self) -> Package:
        """`.Package` this procedure belongs to.
        """
        return self.schema.packages.get(self._attributes.get('RDB$PACKAGE_NAME'))

class Procedure(SchemaItem):
    """Represents stored procedure.

    Supported SQL actions:
        - User procedure: `create` (no_code=bool), `recreate`  no_code=bool),
          `create_or_alter` (no_code=bool), `drop`, `comment`
          `alter` (input=string_or_list, output=string_or_list, declare=string_or_list, code=string_or_list)
        - System procedure: `comment`
    """
    def __init__(self, schema: Schema, attributes: Dict[str, Any]):
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
        "Returns SQL command to CREATE procedure."
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
        "Returns SQL command to ALTER procedure."
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
            if isinstance(inpars, (list, tuple)):
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
            if isinstance(outpars, (list, tuple)):
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
            elif isinstance(declare, (list, tuple)):
                d = ''
                for x in declare:
                    d += f'  {x}\n'
            else:
                d = f'{declare}\n'
            if isinstance(code, (list, tuple)):
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
        "Returns SQL command to DROP procedure."
        self._check_params(params, [])
        return f'DROP PROCEDURE {self.get_quoted_name()}'
    def _get_comment_sql(self, **params) -> str:
        "Returns SQL command to COMMENT procedure."
        comment = 'NULL' if self.description is None \
            else f"'{escape_single_quotes(self.description)}'"
        return f'COMMENT ON PROCEDURE {self.get_quoted_name()} IS {comment}'
    def _get_name(self) -> str:
        return self._attributes['RDB$PROCEDURE_NAME']
    def get_param(self, name: str) -> ProcedureParameter:
        """Returns `.ProcedureParameter` with specified name or None.
        """
        for p in self.output_params:
            if p.name == name:
                return p
        for p in self.input_params:
            if p.name == name:
                return p
        return None
    def has_input(self) -> bool:
        """Returns True if procedure has any input parameters.
        """
        return bool(self._attributes['RDB$PROCEDURE_INPUTS'])
    def has_output(self) -> bool:
        """Returns True if procedure has any output parameters.
        """
        return bool(self._attributes['RDB$PROCEDURE_OUTPUTS'])
    def is_packaged(self) -> bool:
        """Returns True if procedure is defined in package.
        """
        return bool(self._attributes.get('RDB$PACKAGE_NAME'))
    @property
    def id(self) -> int:
        """Internal unique ID number.
        """
        return self._attributes['RDB$PROCEDURE_ID']
    @property
    def source(self) -> str:
        """PSQL source code.
        """
        return self._attributes['RDB$PROCEDURE_SOURCE']
    @property
    def security_class(self) -> str:
        """Security class that define access limits to the procedure.
        """
        return self._attributes['RDB$SECURITY_CLASS']
    @property
    def owner_name(self) -> str:
        """User name of procedure's creator.
        """
        return self._attributes['RDB$OWNER_NAME']
    @property
    def input_params(self) -> DataList[ProcedureParameter]:
        """List of input parameters.
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
        """List of output parameters.
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
        """List of privileges granted to procedure.
        """
        return self.schema.privileges.extract(lambda p: ((p.subject_name == self.name) and
                                                         (p.subject_type in self._type_code)),
                                                     copy=True)
    @property
    def proc_type(self) -> ProcedureType:
        """Procedure type.
        """
        return ProcedureType(self._attributes.get('RDB$PROCEDURE_TYPE', 0))
    @property
    def valid_blr(self) -> bool:
        """Procedure BLR invalidation flag. Coul be True/False or None.
        """
        return bool(result) if (result := self._attributes.get('RDB$VALID_BLR')) is not None else None
    @property
    def engine_name(self) -> str:
        """Engine name.
        """
        return self._attributes.get('RDB$ENGINE_NAME')
    @property
    def entrypoint(self) -> str:
        """Entrypoint.
        """
        return self._attributes.get('RDB$ENTRYPOINT')
    @property
    def package(self) -> Package:
        """Package this procedure belongs to.
        """
        return self.schema.packages.get(self._attributes.get('RDB$PACKAGE_NAME'))
    @property
    def privacy(self) -> Privacy:
        """Privacy flag.
        """
        return Privacy(code) if (code := self._attributes.get('RDB$PRIVATE_FLAG')) is not None else None

class Role(SchemaItem):
    """Represents user role.

    Supported SQL actions:
        - User role: `create`, `drop`, `comment`
        - System role: `comment`
    """
    def __init__(self, schema: Schema, attributes: Dict[str, Any]):
        super().__init__(schema, attributes)
        self._type_code.append(ObjectType.ROLE)
        self._strip_attribute('RDB$ROLE_NAME')
        self._strip_attribute('RDB$OWNER_NAME')
        self._strip_attribute('RDB$SECURITY_CLASS')
        self._actions.append('comment')
        if not self.is_sys_object():
            self._actions.extend(['create', 'drop'])
    def _get_create_sql(self, **params) -> str:
        "Returns SQL command to CREATE role."
        self._check_params(params, [])
        return f'CREATE ROLE {self.get_quoted_name()}'
    def _get_drop_sql(self, **params) -> str:
        "Returns SQL command to DROP role."
        self._check_params(params, [])
        return f'DROP ROLE {self.get_quoted_name()}'
    def _get_comment_sql(self, **params) -> str:
        "Returns SQL command to COMMENT role."
        comment = 'NULL' if self.description is None \
            else f"'{escape_single_quotes(self.description)}'"
        return f'COMMENT ON ROLE {self.get_quoted_name()} IS {comment}'
    def _get_name(self) -> str:
        return self._attributes['RDB$ROLE_NAME']
    @property
    def owner_name(self) -> str:
        """User name of role owner.
        """
        return self._attributes['RDB$OWNER_NAME']
    @property
    def security_class(self) -> str:
        """Security class name or None.
        """
        return self._attributes.get('RDB$SECURITY_CLASS')
    @property
    def privileges(self) -> DataList[Privilege]:
        """List of privileges granted to role.
        """
        return self.schema.privileges.extract(lambda p: ((p.user_name == self.name) and
                                                         (p.user_type in self._type_code)),
                                                     copy=True)

class FunctionArgument(SchemaItem):
    """Represets UDF argument.

    Supported SQL actions:
        `none`
    """
    def __init__(self, schema: Schema, function: Function, attributes: Dict[str, Any]):
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
        return self.argument_name or f'{self.function.name}_{self.position}'
    def get_sql_definition(self) -> str:
        """Returns SQL definition for parameter.
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
        """Returns True if argument is passed by value.
        """
        return self.mechanism == Mechanism.BY_VALUE
    def is_by_reference(self) -> bool:
        """Returns True if argument is passed by reference.
        """
        return self.mechanism in (Mechanism.BY_REFERENCE, Mechanism.BY_REFERENCE_WITH_NULL)
    def is_by_descriptor(self, any_=False) -> bool:
        """Returns True if argument is passed by descriptor.

        Arguments:
            any_: If True, method returns True if `any_` kind of descriptor is used (including
                 BLOB and ARRAY descriptors).
        """
        return self.mechanism in (Mechanism.BY_VMS_DESCRIPTOR, Mechanism.BY_ISC_DESCRIPTOR,
                                  Mechanism.BY_SCALAR_ARRAY_DESCRIPTOR) if any_ \
               else self.mechanism == Mechanism.BY_VMS_DESCRIPTOR
    def is_with_null(self) -> bool:
        """Returns True if argument is passed by reference with NULL support.
        """
        return self.mechanism is Mechanism.BY_REFERENCE_WITH_NULL
    def is_freeit(self) -> bool:
        """Returns True if (return) argument is declared as FREE_IT.
        """
        return self._attributes['RDB$MECHANISM'] < 0
    def is_returning(self) -> bool:
        """Returns True if argument represents return value for function.
        """
        return self.position == self.function._attributes['RDB$RETURN_ARGUMENT']
    def is_nullable(self) -> bool:
        """Returns True if parameter allows NULL.
        """
        return not bool(self._attributes.get('RDB$NULL_FLAG'))
    def has_default(self) -> bool:
        """Returns True if parameter has default value.
        """
        return bool(self._attributes.get('RDB$DEFAULT_SOURCE'))
    def is_packaged(self) -> bool:
        """Returns True if function argument is defined in package.
        """
        return bool(self._attributes.get('RDB$PACKAGE_NAME'))
    @property
    def function(self) -> Function:
        """`.Function` to which this argument belongs.
        """
        return self.__function
    @property
    def position(self) -> int:
        """Argument position.
        """
        return self._attributes['RDB$ARGUMENT_POSITION']
    @property
    def mechanism(self) -> Mechanism:
        """How argument is passed.
        """
        return None if (x := self._attributes['RDB$MECHANISM']) is None else Mechanism(abs(x))
    @property
    def field_type(self) -> FieldType:
        """Number code of the data type defined for the argument.
        """
        return None if (code := self._attributes['RDB$FIELD_TYPE']) in (None, 0) else FieldType(code)
    @property
    def length(self) -> int:
        """Length of the argument in bytes.
        """
        return self._attributes['RDB$FIELD_LENGTH']
    @property
    def scale(self) -> int:
        """Negative number representing the scale of NUMBER and DECIMAL argument.
        """
        return self._attributes['RDB$FIELD_SCALE']
    @property
    def precision(self) -> int:
        """Indicates the number of digits of precision available to the data type of the
        argument.
        """
        return self._attributes['RDB$FIELD_PRECISION']
    @property
    def sub_type(self) -> FieldSubType:
        """BLOB subtype.
        """
        return None if (x := self._attributes['RDB$FIELD_SUB_TYPE']) is None else FieldSubType(x)
    @property
    def character_length(self) -> int:
        """Length of CHAR and VARCHAR column, in characters (not bytes).
        """
        return self._attributes['RDB$CHARACTER_LENGTH']
    @property
    def character_set(self) -> CharacterSet:
        """`.CharacterSet` for a character/text BLOB argument, or None.
        """
        return self.schema.get_charset_by_id(self._attributes['RDB$CHARACTER_SET_ID'])
    @property
    def datatype(self) -> str:
        """Comlete SQL datatype definition.
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
    def type_from(self) -> TypeFrom:
        """Source for parameter data type.
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
    def argument_name(self) -> str:
        """Argument name.
        """
        return self._attributes.get('RDB$ARGUMENT_NAME')
    @property
    def domain(self) -> Domain:
        """`.Domain` for this parameter.
        """
        return self.schema.all_domains.get(self._attributes.get('RDB$FIELD_SOURCE'))
    @property
    def default(self) -> str:
        """Default value.
        """
        if result := self._attributes.get('RDB$DEFAULT_SOURCE'):
            if result.upper().startswith('= '):
                result = result[2:]
            elif result.upper().startswith('DEFAULT '):
                result = result[8:]
        return result
    @property
    def collation(self) -> Collation:
        """`.Collation` for this parameter.
        """
        return (None if (cid := self._attributes.get('RDB$COLLATION_ID')) is None
                else self.schema.get_collation_by_id(self.domain._attributes['RDB$CHARACTER_SET_ID'], cid))
    @property
    def argument_mechanism(self) -> Mechanism:
        """Argument mechanism.
        """
        return None if (code := self._attributes.get('RDB$ARGUMENT_MECHANISM')) is None else Mechanism(code)
    @property
    def column(self) -> TableColumn:
        """`.TableColumn` for this parameter.
        """
        return (None if (rname := self._attributes.get('RDB$RELATION_NAME')) is None
                else self.schema.all_tables.get(rname).columns.get(self._attributes['RDB$FIELD_NAME']))
    @property
    def package(self) -> Package:
        """`.Package` this function belongs to.
        """
        return self.schema.packages.get(self._attributes.get('RDB$PACKAGE_NAME'))

class Function(SchemaItem):
    """Represents user defined function.

    Supported SQL actions:
        - External UDF: `declare`, `drop`, `comment`
        - PSQL UDF (not declared in package): `create` (no_code=bool),
          `recreate` (no_code=bool), `create_or_alter` (no_code=bool), `drop`,
          `alter` (arguments=string_or_list, returns=string, declare=string_or_list, code=string_or_list)
        - System UDF: `none`
    """
    def __init__(self, schema: Schema, attributes: Dict[str, Any]):
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
            else:
                if self._attributes.get('RDB$PACKAGE_NAME') is None:
                    self._actions.extend(['create', 'recreate', 'alter', 'create_or_alter',
                                          'drop'])

    def _get_declare_sql(self, **params) -> str:
        "Returns SQL command to DECLARE function."
        self._check_params(params, [])
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
        "Returns SQL command to DROP function."
        self._check_params(params, [])
        return f"DROP{' EXTERNAL' if self.is_external() else ''} FUNCTION {self.get_quoted_name()}"
    def _get_comment_sql(self, **params) -> str:
        "Returns SQL command to COMMENT function."
        comment = 'NULL' if self.description is None \
            else f"'{escape_single_quotes(self.description)}'"
        return f"COMMENT ON{' EXTERNAL' if self.is_external() else ''} " \
               f"FUNCTION {self.get_quoted_name()} IS {comment}"
    def _get_create_sql(self, **params) -> str:
        "Returns SQL command to CREATE function."
        self._check_params(params, ['no_code'])
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
        "Returns SQL command to ALTER object."
        self._check_params(params, ['arguments', 'returns', 'declare', 'code'])
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
            if isinstance(arguments, (list, tuple)):
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
            elif isinstance(declare, (list, tuple)):
                d = ''
                for x in declare:
                    d += f'  {x}\n'
            else:
                d = f'{declare}\n'
            if isinstance(code, (list, tuple)):
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
    def _load_arguments(self, mock: Dict[str, Any]=None) -> None:
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
        return self._attributes['RDB$FUNCTION_NAME']
    def is_external(self) -> bool:
        """Returns True if function is external UDF, False for PSQL functions.
        """
        return bool(self.module_name)
    def has_arguments(self) -> bool:
        """Returns True if function has input arguments.
        """
        return bool(self.arguments)
    def has_return(self) -> bool:
        """Returns True if function returns a value.
        """
        return self.returns is not None
    def has_return_argument(self) -> bool:
        """Returns True if function returns a value in input argument.
        """
        return self.returns.position != 0 if self.returns is not None else False
    def is_packaged(self) -> bool:
        """Returns True if function is defined in package.
        """
        return bool(self._attributes.get('RDB$PACKAGE_NAME'))
    @property
    def module_name(self) -> str:
        """Module name.
        """
        return self._attributes['RDB$MODULE_NAME']
    @property
    def entrypoint(self) -> str:
        """Entrypoint in module.
        """
        return self._attributes['RDB$ENTRYPOINT']
    @property
    def returns(self) -> FunctionArgument:
        """Returning `.FunctionArgument` or None.
        """
        if self.__arguments is None:
            self._load_arguments()
        return None if self.__returns is None else self.__returns()
    @property
    def arguments(self) -> DataList[FunctionArgument]:
        """List of function arguments.
        """
        if self.__arguments is None:
            self._load_arguments()
        return self.__arguments.extract(lambda a: a.position != 0, copy=True)
    @property
    def engine_mame(self) -> str:
        """Engine name.
        """
        return self._attributes.get('RDB$ENGINE_NAME')
    @property
    def package(self) -> Package:
        """Package this function belongs to.
        """
        return self.schema.packages.get(self._attributes.get('RDB$PACKAGE_NAME'))
    @property
    def private_flag(self) -> Privacy:
        """Private flag.
        """
        return None if (code := self._attributes.get('RDB$PRIVATE_FLAG')) is None \
               else Privacy(code)
    @property
    def source(self) -> str:
        """Function source.
        """
        return self._attributes.get('RDB$FUNCTION_SOURCE')
    @property
    def id(self) -> int:
        """Function ID.
        """
        return self._attributes.get('RDB$FUNCTION_ID')
    @property
    def valid_blr(self) -> bool:
        """BLR validity flag.
        """
        return None if (value := self._attributes.get('RDB$VALID_BLR')) is None \
               else bool(value)
    @property
    def security_class(self) -> str:
        """Security class.
        """
        return self._attributes.get('RDB$SECURITY_CLASS')
    @property
    def owner_name(self) -> str:
        """Owner name.
        """
        return self._attributes.get('RDB$OWNER_NAME')
    @property
    def legacy_flag(self) -> Legacy:
        """Legacy flag.
        """
        return Legacy(self._attributes.get('RDB$LEGACY_FLAG'))
    @property
    def deterministic_flag(self) -> int:
        """Deterministic flag.
        """
        return self._attributes.get('RDB$DETERMINISTIC_FLAG')

class DatabaseFile(SchemaItem):
    """Represents database extension file.

    Supported SQL actions:
        `create`
    """
    def __init__(self, schema: Schema, attributes: Dict[str, Any]):
        super().__init__(schema, attributes)
        self._strip_attribute('RDB$FILE_NAME')
    def _get_name(self) -> str:
        return f'FILE_{self.sequence}'
    def is_sys_object(self) -> bool:
        """Always returns True.
        """
        return True
    @property
    def filename(self) -> str:
        """File name.
        """
        return self._attributes['RDB$FILE_NAME']
    @property
    def sequence(self) -> int:
        """File sequence number.
        """
        return self._attributes['RDB$FILE_SEQUENCE']
    @property
    def start(self) -> int:
        """File start page number.
        """
        return self._attributes['RDB$FILE_START']
    @property
    def length(self) -> str:
        """File length in pages.
        """
        return self._attributes['RDB$FILE_LENGTH']

class Shadow(SchemaItem):
    """Represents database shadow.

    Supported SQL actions:
        `create`, `drop` (preserve=bool)
    """
    def __init__(self, schema: Schema, attributes: Dict[str, Any]):
        super().__init__(schema, attributes)
        self.__files = None
        self._actions.extend(['create', 'drop'])
    def _get_create_sql(self, **params) -> str:
        "Returns SQL command to CREATE shadow."
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
        "Returns SQL command to DROP shadow."
        self._check_params(params, ['preserve'])
        preserve = params.get('preserve')
        return f"DROP SHADOW {self.id}{' PRESERVE FILE' if preserve else ''}"
    def _get_name(self) -> str:
        return f'SHADOW_{self.id}'
    def is_sys_object(self) -> bool:
        """Always returns False.
        """
        return False
    def is_manual(self) -> bool:
        """Returns True if it's MANUAL shadow.
        """
        return ShadowFlag.MANUAL in self.flags
    def is_inactive(self) -> bool:
        """Returns True if it's INACTIVE shadow.
        """
        return ShadowFlag.INACTIVE in self.flags
    def is_conditional(self) -> bool:
        """Returns True if it's CONDITIONAL shadow.
        """
        return ShadowFlag.CONDITIONAL in self.flags
    @property
    def id(self) -> int:
        """Shadow ID number.
        """
        return self._attributes['RDB$SHADOW_NUMBER']
    @property
    def flags(self) -> ShadowFlag:
        """Shadow flags.
        """
        return ShadowFlag(self._attributes['RDB$FILE_FLAGS'])
    @property
    def files(self) -> DataList[DatabaseFile]:
        """List of shadow files.
        """
        if self.__files is None:
            self.__files = DataList((DatabaseFile(self, row) for row
                            in self.schema._select("""select RDB$FILE_NAME, RDB$FILE_SEQUENCE,
RDB$FILE_START, RDB$FILE_LENGTH from RDB$FILES
where RDB$SHADOW_NUMBER = ?
order by RDB$FILE_SEQUENCE""", (self._attributes['RDB$SHADOW_NUMBER'],))), frozen=True)
        return self.__files

class Privilege(SchemaItem):
    """Represents priviledge to database object.

    Supported SQL actions:
        `grant` (grantors), `revoke` (grantors, grant_option)
    """
    def __init__(self, schema: Schema, attributes: Dict[str, Any]):
        super().__init__(schema, attributes)
        self._actions.extend(['grant', 'revoke'])
        self._strip_attribute('RDB$USER')
        self._strip_attribute('RDB$GRANTOR')
        self._strip_attribute('RDB$PRIVILEGE')
        self._strip_attribute('RDB$RELATION_NAME')
        self._strip_attribute('RDB$FIELD_NAME')
    def _get_grant_sql(self, **params) -> str:
        "Returns SQL command to GRANT privilege."
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
        "Returns SQL command to REVOKE privilege."
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
    def is_sys_object(self) -> bool:
        """Always returns True.
        """
        return True
    def has_grant(self) -> bool:
        """Returns True if privilege comes with GRANT OPTION.
        """
        return self.grant_option and self.grant_option is not GrantOption.NONE
    def is_select(self) -> bool:
        """Returns True if this is SELECT privilege.
        """
        return self.privilege is PrivilegeCode.SELECT
    def is_insert(self) -> bool:
        """Returns True if this is INSERT privilege.
        """
        return self.privilege is PrivilegeCode.INSERT
    def is_update(self) -> bool:
        """Returns True if this is UPDATE privilege.
        """
        return self.privilege is PrivilegeCode.UPDATE
    def is_delete(self) -> bool:
        """Returns True if this is DELETE privilege.
        """
        return self.privilege is PrivilegeCode.DELETE
    def is_execute(self) -> bool:
        """Returns True if this is EXECUTE privilege.
        """
        return self.privilege is PrivilegeCode.EXECUTE
    def is_reference(self) -> bool:
        """Returns True if this is REFERENCE privilege.
        """
        return self.privilege is  PrivilegeCode.REFERENCES
    def is_membership(self) -> bool:
        """Returns True if this is ROLE membership privilege.
        """
        return self.privilege is PrivilegeCode.MEMBERSHIP
    @property
    def user(self) -> Union[UserInfo, Role, Procedure, Trigger, View]:
        """Grantee. Either `~firebird.driver.UserInfo`, `.Role`, `.Procedure`, `.Trigger`
        or `.View` object.
        """
        return self.schema.get_item(self._attributes['RDB$USER'],
                                     ObjectType(self._attributes['RDB$USER_TYPE']))
    @property
    def grantor(self) -> UserInfo:
        """Grantor `~firebird.driver.User` object.
        """
        return UserInfo(user_name=self._attributes['RDB$GRANTOR'])
    @property
    def privilege(self) -> PrivilegeCode:
        """Privilege code.
        """
        return PrivilegeCode(self._attributes['RDB$PRIVILEGE'])
    @property
    def subject_name(self) -> str:
        """Subject name.
        """
        return self._attributes['RDB$RELATION_NAME']
    @property
    def subject_type(self) -> ObjectType:
        """Subject type.
        """
        return ObjectType(self._attributes['RDB$OBJECT_TYPE'])
    @property
    def field_name(self) -> str:
        """Field name.
        """
        return self._attributes['RDB$FIELD_NAME']
    @property
    def subject(self) -> Union[Role, Table, View, Procedure]:
        """Priviledge subject. Either `.Role`, `.Table`, `.View` or `.Procedure` instance.
        """
        result = self.schema.get_item(self.subject_name, self.subject_type, self.field_name)
        if result is None and self.subject_type == ObjectType.TABLE:
            # Views are logged as tables, so try again for view code
            result = self.schema.get_item(self.subject_name, ObjectType.VIEW, self.field_name)
        return result
    @property
    def user_name(self) -> str:
        """User name.
        """
        return self._attributes['RDB$USER']
    @property
    def user_type(self) -> ObjectType:
        """User type.
        """
        return ObjectType(self._attributes['RDB$USER_TYPE'])
    @property
    def grantor_name(self) -> str:
        """Grantor name.
        """
        return self._attributes['RDB$GRANTOR']
    @property
    def grant_option(self) -> GrantOption:
        """Grant option.
        """
        return None if (value := self._attributes['RDB$GRANT_OPTION']) is None \
               else GrantOption(value)

class Package(SchemaItem):
    """Represents PSQL package.

    Supported SQL actions:
        `create` (body=bool), `recreate` (body=bool), `create_or_alter` (body=bool),
        `alter` (header=string_or_list), `drop` (body=bool)
    """
    def __init__(self, schema: Schema, attributes: Dict[str, Any]):
        super().__init__(schema, attributes)
        self._type_code.extend([ObjectType.PACKAGE, ObjectType.PACKAGE_BODY])
        self._actions.extend(['create', 'recreate', 'create_or_alter', 'alter', 'drop',
                              'comment'])
        self._strip_attribute('RDB$PACKAGE_NAME')
        self._strip_attribute('RDB$SECURITY_CLASS')
        self._strip_attribute('RDB$OWNER_NAME')
    def _get_create_sql(self, **params) -> str:
        "Returns SQL command to CREATE package."
        self._check_params(params, ['body'])
        body = params.get('body')
        cbody = 'BODY ' if body else ''
        result = f'CREATE PACKAGE {cbody}{self.get_quoted_name()}'
        return result+'\nAS\n'+(self.body if body else self.header)
    def _get_alter_sql(self, **params) -> str:
        "Returns SQL command to ALTER package."
        self._check_params(params, ['header'])
        header = params.get('header')
        if not header:
            hdr = ''
        else:
            hdr = '\n'.join(header) if isinstance(header, list) else header
        return f'ALTER PACKAGE {self.get_quoted_name()}\nAS\nBEGIN\n{hdr}\nEND'
    def _get_drop_sql(self, **params) -> str:
        "Returns SQL command to DROP package."
        self._check_params(params, ['body'])
        body = params.get('body')
        cbody = 'BODY ' if body else ''
        return f'DROP PACKAGE {cbody}{self.get_quoted_name()}'
    def _get_comment_sql(self, **params) -> str:
        "Returns SQL command to COMMENT package."
        comment = 'NULL' if self.description is None \
            else f"'{escape_single_quotes(self.description)}'"
        return f'COMMENT ON PACKAGE {self.get_quoted_name()} IS {comment}'
    def _get_name(self) -> str:
        return self._attributes['RDB$PACKAGE_NAME']
    def has_valid_body(self) -> bool:
        """Returns True if package has valid body."""
        return None if (result := self._attributes.get('RDB$VALID_BODY_FLAG')) is None \
               else bool(result)
    @property
    def header(self) -> str:
        """Package header source.
        """
        return self._attributes['RDB$PACKAGE_HEADER_SOURCE']
    @property
    def body(self) -> str:
        """Package body source.
        """
        return self._attributes['RDB$PACKAGE_BODY_SOURCE']
    @property
    def security_class(self) -> str:
        """Security class name or None.
        """
        return self._attributes['RDB$SECURITY_CLASS']
    @property
    def owner_name(self) -> str:
        """User name of package creator.
        """
        return self._attributes['RDB$OWNER_NAME']
    @property
    def functions(self) -> DataList[Function]:
        """List of package functions.
        """
        return self.schema.functions.extract(lambda fn: fn._attributes['RDB$PACKAGE_NAME'] == self.name,
                                             copy=True)
    @property
    def procedures(self) -> DataList[Procedure]:
        """List of package procedures.
        """
        return self.schema.procedures.extract(lambda proc: proc._attributes['RDB$PACKAGE_NAME'] == self.name,
                                              copy=True)

class BackupHistory(SchemaItem):
    """Represents entry of history for backups performed using the nBackup utility.

    Supported SQL actions:
        `None`
    """
    def __init__(self, schema: Schema, attributes: Dict[str, Any]):
        super().__init__(schema, attributes)
        self._strip_attribute('RDB$FILE_NAME')
    def _get_name(self) -> str:
        return f'BCKP_{self.scn}'
    def is_sys_object(self) -> bool:
        """Always returns True.
        """
        return True
    @property
    def id(self) -> int:
        """The identifier assigned by the engine.
        """
        return self._attributes['RDB$BACKUP_ID']
    @property
    def filename(self) -> str:
        """Full path and file name of backup file.
        """
        return self._attributes['RDB$FILE_NAME']
    @property
    def created(self) -> datetime.datetime:
        """Backup date and time.
        """
        return self._attributes['RDB$TIMESTAMP']
    @property
    def level(self) -> int:
        """Backup level.
        """
        return self._attributes['RDB$BACKUP_LEVEL']
    @property
    def scn(self) -> int:
        """System (scan) number.
        """
        return self._attributes['RDB$SCN']
    @property
    def guid(self) -> str:
        """Unique identifier.
        """
        return self._attributes['RDB$GUID']

class Filter(SchemaItem):
    """Represents userdefined BLOB filter.

    Supported SQL actions:
        - BLOB filter: `declare`, `drop`, `comment`
        - System UDF: `none`
    """
    def __init__(self, schema: Schema, attributes: Dict[str, Any]):
        super().__init__(schema, attributes)
        self._type_code.append(ObjectType.BLOB_FILTER)
        self._strip_attribute('RDB$FUNCTION_NAME')
        self._strip_attribute('RDB$MODULE_NAME')
        self._strip_attribute('RDB$ENTRYPOINT')
        if not self.is_sys_object():
            self._actions.extend(['comment', 'declare', 'drop'])
    def _get_declare_sql(self, **params) -> str:
        "Returns SQL command to DECLARE filter."
        self._check_params(params, [])
        fdef = f'DECLARE FILTER {self.get_quoted_name()}\n' \
               f'INPUT_TYPE {self.input_sub_type} OUTPUT_TYPE {self.output_sub_type}\n'
        return f"{fdef}ENTRY_POINT '{self.entrypoint}' MODULE_NAME '{self.module_name}'"
    def _get_drop_sql(self, **params) -> str:
        "Returns SQL command to DROP filter."
        self._check_params(params, [])
        return f'DROP FILTER {self.get_quoted_name()}'
    def _get_comment_sql(self, **params) -> str:
        "Returns SQL command to COMMENT filter."
        comment = 'NULL' if self.description is None \
            else f"'{escape_single_quotes(self.description)}'"
        return f'COMMENT ON FILTER {self.get_quoted_name()} IS {comment}'
    def _get_name(self) -> str:
        return self._attributes['RDB$FUNCTION_NAME']
    @property
    def module_name(self) -> str:
        """The name of the dynamic library or shared object where the code of the BLOB
        filter is located.
        """
        return self._attributes['RDB$MODULE_NAME']
    @property
    def entrypoint(self) -> str:
        """The exported name of the BLOB filter in the filter library.
        """
        return self._attributes['RDB$ENTRYPOINT']
    @property
    def input_sub_type(self) -> int:
        """The BLOB subtype of the data to be converted by the function.
        """
        return self._attributes.get('RDB$INPUT_SUB_TYPE')
    @property
    def output_sub_type(self) -> int:
        """The BLOB subtype of the converted data.
        """
        return self._attributes.get('RDB$OUTPUT_SUB_TYPE')
