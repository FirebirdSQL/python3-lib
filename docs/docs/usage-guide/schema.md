<a id="working-with-database-schema"></a>

# Working with database schema

Description of database objects like tables, views, stored procedures, triggers or UDF
functions that represent database schema is stored in set of system tables present in
every database. Firebird users can query these tables to get information about these
objects and their relations. But querying system tables is inconvenient, as it requires
good knowledge how this information is structured and requires significant amount of Python
code. Changes in system tables between Firebird versions further add to this complexity.
The `firebird.lib.schema` module provides set of classes that transform information stored
in system tables into set of Python objects that surface the vital information in
meaningful way, and additionally provide set of methods for most commonly used operations
or checks.

Database schema could be accessed in two different ways, each suitable for different use case:

- By direct creation of `Schema` instances that are then
    [binded][firebird.lib.schema.Schema.bind] to particular `Connection`
    instance. This method is best if you want to work with schema only occasionally, or you
    want to keep connections as lightweight as possible.
- Accessing `Connection.schema` property. This
    method is more convenient than previous one, and represents a compromise between
    convenience and resource consumption because `Schema` instance is not created until
    first reference and is managed by connection itself. Individual metadata objects are not
    loaded from system tables until first reference.

**Examples:**

1. Using Schema instance:

```python
>>> from firebird.driver import connect
>>> from firebird.lib.schema import Schema
>>> with connect('employee', user='sysdba', password='masterkey') as con:
>>>     schema = Schema()
>>>     schema.bind(con)
>>>     print([t.name for t in schema.tables])
['COUNTRY', 'JOB', 'DEPARTMENT', 'EMPLOYEE', 'CUSTOMER', 'PROJECT', 'EMPLOYEE_PROJECT', 'PROJ_DEPT_BUDGET', 'SALARY_HISTORY', 'SALES']

```

2. Using Connection.schema:

```python
>>> from firebird.driver import connect
>>> with connect('employee', user='sysdba', password='masterkey') as con:
>>>     print([t.name for t in con.schema.tables])
['COUNTRY', 'JOB', 'DEPARTMENT', 'EMPLOYEE', 'CUSTOMER', 'PROJECT', 'EMPLOYEE_PROJECT', 'PROJ_DEPT_BUDGET', 'SALARY_HISTORY', 'SALES']

```

!!! important

    The `Connection.schema` property will raise an exception on access if `firebird-lib`
    package is not installed.

    While firebird-lib sets the dependency on [firebird-driver](https://pypi.org/project/firebird-driver/) package, the
    [firebird-driver](https://pypi.org/project/firebird-driver/) does not sets (symetric) dependency on `firebird-lib`. It means that
    installing the driver itself will not automatically install the firebird-lib package,
    while installing the library will ensure that the driver is also installed.

!!! note

    Individual metadata information (i.e. information about
    [domains][firebird.lib.schema.Schema.domains],
    [tables][firebird.lib.schema.Schema.tables] etc.) are loaded on first access and
    cached for further reference until it's [clared][firebird.lib.schema.Schema.clear] or
    [reload][firebird.lib.schema.Schema.reload] is requested.

    Because once loaded information is cached, it's good to
    [clear][firebird.lib.schema.Schema.clear] it when it's no longer needed to conserve memory.


## Information provided by `Schema`

The `Schema` provides information about:

- **Database:** [Owner name][firebird.lib.schema.Schema.owner_name],
    [default character set][firebird.lib.schema.Schema.default_character_set],
    [description][firebird.lib.schema.Schema.description],
    [security class][firebird.lib.schema.Schema.security_class], [linger option][firebird.lib.schema.Schema.linger] and whether
    database consist from [single or multiple files][firebird.lib.schema.Schema.is_multifile].
- **Facilities:** Available [character sets][firebird.lib.schema.Schema.character_sets],
    [collations][firebird.lib.schema.Schema.collations], BLOB [filters][firebird.lib.schema.Schema.filters], database
    [files][firebird.lib.schema.Schema.files] and [shadows][firebird.lib.schema.Schema.shadows].
- **User database objects:** [exceptions][firebird.lib.schema.Schema.exceptions],
    [generators][firebird.lib.schema.Schema.generators], [domains][firebird.lib.schema.Schema.domains],
    [tables][firebird.lib.schema.Schema.tables] and their [constraints][firebird.lib.schema.Schema.constraints],
    [indices][firebird.lib.schema.Schema.indices], [views][firebird.lib.schema.Schema.views],
    [triggers][firebird.lib.schema.Schema.triggers], [procedures][firebird.lib.schema.Schema.procedures],
    user [roles][firebird.lib.schema.Schema.roles], [user defined functions][firebird.lib.schema.Schema.functions] and
    [packages][firebird.lib.schema.Schema.packages].
- **System database objects:** [generators][firebird.lib.schema.Schema.sys_generators],
    [domains][firebird.lib.schema.Schema.sys_domains], [tables][firebird.lib.schema.Schema.sys_tables] and their
    constraints,  [indices][firebird.lib.schema.Schema.sys_indices], [views][firebird.lib.schema.Schema.sys_views],
    [triggers][firebird.lib.schema.Schema.sys_triggers], [procedures][firebird.lib.schema.Schema.sys_procedures],
    [functions][firebird.lib.schema.Schema.sys_functions] and [backup history][firebird.lib.schema.Schema.backup_history].
- **Relations between objects:** Through direct links between metadata objects and
    [dependencies][firebird.lib.schema.Schema.dependencies].
- **Privileges:** [All][firebird.lib.schema.Schema.privileges] privileges, or privileges granted for specific
    [table][firebird.lib.schema.Table.privileges], [table column][firebird.lib.schema.TableColumn.privileges],
    [view][firebird.lib.schema.View.privileges], [view column][firebird.lib.schema.ViewColumn.privileges],
    [procedure][firebird.lib.schema.Procedure.privileges] or [role][firebird.lib.schema.Role.privileges]. It's also
    possible to get all privileges [granted to][firebird.lib.schema.Schema.get_privileges_of] specific user, role,
    procedure, trigger or view.


## Metadata objects

Schema information is presented as Python objects of various classes with common parent
class `SchemaItem` (except `Schema` itself), that defines several common attributes and methods:

**Attributes:**

- [`SchemaItem.name`][firebird.lib.schema.SchemaItem.name]: Name of database object or None if object doesn't have a name.
- [`SchemaItem.description`][firebird.lib.schema.SchemaItem.description]: Description (documentation text) for object or None if object
    doesn't have a description.
- [`SchemaItem.actions`][firebird.lib.schema.SchemaItem.actions]: List of supported SQL operations on schema object instance.

**Methods:**

- [`Visitable.accept()`][firebird.lib.schema.Visitable.accept]: [Visitor Pattern support](#visitor-pattern-support).
- [`SchemaItem.is_sys_object()`][firebird.lib.schema.SchemaItem.is_sys_object]: Returns True if this database object is system object.
- [`SchemaItem.get_quoted_name()`][firebird.lib.schema.SchemaItem.get_quoted_name]: Returns quoted (if necessary) name of database object.
- [`SchemaItem.get_dependents()`][firebird.lib.schema.SchemaItem.get_dependents]: Returns list of all database objects that
    [depend](#object-dependencies) on this one.
- [`SchemaItem.get_dependencies()`][firebird.lib.schema.SchemaItem.get_dependencies]: Returns list of database objects that this object
    [depend](#object-dependencies) on.
- [`SchemaItem.get_sql_for()`][firebird.lib.schema.SchemaItem.get_sql_for]: Returns [SQL command string](#sql-operations) for
    specified action on database object.

There are next schema objects: `Collation`, `CharacterSet`, `DatabaseException`,
`Sequence` (Generator), `Domain`, `Index`, `Table`, `TableColumn`, `Constraint`,
`View`, `ViewColumn`, `Trigger`, `Procedure`, `ProcedureParameter`, `Function`,
`FunctionArgument`, `Role`, `Dependency`, `DatabaseFile`, `Shadow`, `Package`,
`Filter`, `BackupHistory` and `Privilege`.


<a id="visitor-pattern-support"></a>

## Visitor Pattern support

[Visitor Pattern](http://en.wikipedia.org/wiki/Visitor_pattern) is particularly useful when you need to process various objects that
need special handling in common algorithm (for example display information about them or
generate SQL commands to create them in new database). Each metadata objects (including
`Schema`) descend from `Visitable` class and thus support [`Visitable.accept()`][firebird.lib.schema.Visitable.accept] method
that calls visitor's [`Visitor.visit()`][firebird.lib.schema.Visitor.visit] method. This method dispatch calls to specific
class-handling method or [`Visitor.default_action()`][firebird.lib.schema.Visitor.default_action] if there is no such special
class-handling method defined in your visitor class. Special class-handling methods must
have a name that follows *visit_<class_name>* pattern, for example method that should
handle `Table` (or its descendants) objects must be named as *visit_Table*.


Next code uses visitor pattern to print all DROP SQL statements necessary to drop database
object, taking its dependencies into account, i.e. it could be necessary to first drop
other - dependent objects before it could be dropped.

```python
from firebird.driver import connect
from firebird.lib.schema import Visitor

# Object dropper
class ObjectDropper(Visitor):
    def __init__(self):
        self.seen = []
    def drop(self, obj):
        self.seen = []
        obj.accept(self) # You can call self.visit(obj) directly here as well
    def default_action(self, obj):
        if not obj.is_sys_object() and 'drop' in obj.actions:
            for dependency in obj.get_dependents():
                d = dependency.dependent
                if d and d not in self.seen:
                    d.accept(self)
            if obj not in self.seen:
                print(obj.get_sql_for('drop'))
                self.seen.append(obj)
    def visit_TableColumn(self, column):
        column.table.accept(self)
    def visit_ViewColumn(self, column):
        column.view.accept(self)
    def visit_ProcedureParameter(self, param):
        param.procedure.accept(self)
    def visit_FunctionArgument(self, arg):
        arg.function.accept(self)

# Sample use:

with connect('employee',user='sysdba', password='masterkey') as con:
    table = con.schema.tables.get('JOB')
    dropper = ObjectDropper()
    dropper.drop(table)

```

Will produce next result:

```text
DROP PROCEDURE ALL_LANGS
DROP PROCEDURE SHOW_LANGS
DROP TABLE JOB

```
<a id="object-dependencies"></a>


## Object dependencies

Close relations between metadata object like `ownership` (Table vs. TableColumn, Index or
Trigger) or `cooperation` (like FK Index vs. partner UQ/PK Index) are defined directly
using properties of particular schema objects. Besides close relations Firebird also uses
`dependencies`, that describe functional dependency between otherwise independent metadata
objects. For example stored procedure can call other stored procedures, define its
parameters using domains or work with tables or views. Removing or changing these objects
may/will cause the procedure to stop working correctly, so Firebird tracks these dependencies.
Schema module surfaces these dependencies as `Dependency` schema objects, and all schema
objects have [`SchemaItem.get_dependents()`][firebird.lib.schema.SchemaItem.get_dependents] and [`SchemaItem.get_dependencies()`][firebird.lib.schema.SchemaItem.get_dependencies] methods
to get list of `Dependency` instances that describe these dependencies.

`Dependency` object provides names and types of dependent/depended on database objects,
and access to their respective schema Python objects as well.

<a id="enhanced-object-list"></a>


## Enhanced list of objects

Whenever possible, schema module uses enhanced `firebird.base.collections.DataList` list
descendant for collections of metadata objects. This enhanced list provides several
convenient methods for advanced list processing:

- filtering - `filter()` and
    `filterfalse()`
- sorting - `sort()`
- finding - `find()`
- extracting/splitting - `extract()` and
    `split()`
- testing - `contains()`,
    `all()` and
    `any()`
- reporting - `occurrence()` and
    `report()`
- fast key access - `key_expr`,
    `frozen`,
    `freeze()` and
    `get()`

!!! important

    Schema module uses `frozen` DataLists for fast
    access to individual list items using their [`SchemaItem.name`][firebird.lib.schema.SchemaItem.name] as a key.

**Examples:**

```python
with connect('employee',user='sysdba',password='masterkey') as con:

    print("All tables that have column named 'JOB_CODE'")
    for table in con.schema.tables.filter(lambda tbl: tbl.columns.any("item.name=='JOB_CODE'")):
        print(table.name)

    print("Order of tables")
    print([i.name for i in con.schema.tables])

    print("Tables sorted by name")
    con.schema.tables.sort(attrs=['name'])
    print([i.name for i in con.schema.tables])

    print("Tables sorted by number of columns")
    con.schema.tables.sort(expr='len(item.columns)')
    print([i.name for i in con.schema.tables])

    print("Report: Tables with number of columns")
    for table_name, num_columns in con.schema.tables.report('item.name', 'len(item.columns)'):
        print(f'{table_name:32}:{num_columns}')

    computed, no_computed = con.schema.tables.split(lambda tbl: tbl.columns.any('item.is_computed()'))
    print("Tables with computed columns")
    for table in computed:
        print(table.name)
    print("Tables with without computed columns")
    for table in no_computed:
        print(table.name)

```

Will produce next result:

```text
All tables that have column named 'JOB_CODE'
JOB
EMPLOYEE
Order of tables
['COUNTRY', 'JOB', 'DEPARTMENT', 'EMPLOYEE', 'CUSTOMER', 'PROJECT', 'EMPLOYEE_PROJECT', 'PROJ_DEPT_BUDGET', 'SALARY_HISTORY', 'SALES']
Tables sorted by name
['COUNTRY', 'CUSTOMER', 'DEPARTMENT', 'EMPLOYEE', 'EMPLOYEE_PROJECT', 'JOB', 'PROJECT', 'PROJ_DEPT_BUDGET', 'SALARY_HISTORY', 'SALES']
Tables sorted by number of columns
['COUNTRY', 'EMPLOYEE_PROJECT', 'PROJECT', 'PROJ_DEPT_BUDGET', 'SALARY_HISTORY', 'DEPARTMENT', 'JOB', 'EMPLOYEE', 'CUSTOMER', 'SALES']
Report: Tables with number of columns
COUNTRY                         :2
EMPLOYEE_PROJECT                :2
PROJECT                         :5
PROJ_DEPT_BUDGET                :5
SALARY_HISTORY                  :6
DEPARTMENT                      :7
JOB                             :8
EMPLOYEE                        :11
CUSTOMER                        :12
SALES                           :13
Tables with computed columns
SALARY_HISTORY
EMPLOYEE
SALES
Tables with without computed columns
COUNTRY
EMPLOYEE_PROJECT
PROJECT
PROJ_DEPT_BUDGET
DEPARTMENT
JOB
CUSTOMER

```
<a id="sql-operations"></a>

## SQL operations

The schema module doesn't allow you to change database metadata directly using schema
objects. Instead it supports generation of DDL SQL commands from schema objects using
[`SchemaItem.get_sql_for()`][firebird.lib.schema.SchemaItem.get_sql_for] method present on all schema objects except Schema
itself. DDL commands that could be generated depend on object type and context (for
example it's not possible to generate all DDL commands for system database objects),
and list of DDL commands that could be generated for particular schema object could be
obtained from its [`SchemaItem.actions`][firebird.lib.schema.SchemaItem.actions] attribute.

Possible `actions` could be: `create`, `recreate`, `create_or_alter`, `alter`, `drop`,
`activate`, `deactivate`, `recompute` and `declare`. Some actions require/allow additional
parameters.

| Schema class | Action | Parameter | Required | Description |
| --- | --- | --- | --- | --- |
| `Collation` | create |  |  |  |
|  | drop |  |  |  |
|  | comment |  |  |  |
| `CharacterSet` | alter | collation | Yes | `Collation` instance or collation name |
|  | comment |  |  |  |
| `DatabaseException` | create |  |  |  |
|  | recreate |  |  |  |
|  | alter | message | Yes | string. |
|  | create_or_alter |  |  |  |
|  | drop |  |  |  |
|  | comment |  |  |  |
| `Sequence` | create |  |  |  |
|  | alter | value | Yes | integer |
|  | drop |  |  |  |
|  | comment |  |  |  |
| `Domain` | create |  |  |  |
|  | alter |  |  | One from next parameters required |
|  |  | name | No | string |
|  |  | default | No | string definition or None to drop default |
|  |  | check | No | string definition or None to drop check |
|  |  | datatype | No | string SQL datatype definition |
|  | drop |  |  |  |
|  | comment |  |  |  |
| `Constraint` | create |  |  |  |
|  | drop |  |  |  |
| `Index` | create |  |  |  |
|  | activate |  |  |  |
|  | deactivate |  |  |  |
|  | recompute |  |  |  |
|  | drop |  |  |  |
|  | comment |  |  |  |
| `Table` | create |  |  |  |
|  |  | no_pk | No | Do not generate PK constraint |
|  |  | no_unique | No | Do not generate unique constraints |
|  | recreate |  |  |  |
|  |  | no_pk | No | Do not generate PK constraint |
|  |  | no_unique | No | Do not generate unique constraints |
|  | drop |  |  |  |
|  | comment |  |  |  |
|  | insert |  |  |  |
|  |  | update | No | When set to True it generates UPDATE OR INSERT. Default False. |
|  |  | returning | No | List of column names for RETURNING clause. |
|  |  | matching | No | List of column names for MATCHING clause. |
| `TableColumn` | alter |  |  | One from next parameters required |
|  |  | name | No | string |
|  |  | datatype | No | string SQL type definition |
|  |  | position | No | integer |
|  |  | expression | No | string with COMPUTED BY expression |
|  |  | restart | No | None or initial value |
|  | drop |  |  |  |
|  | comment |  |  |  |
| `View` | create |  |  |  |
|  | recreate |  |  |  |
|  | alter | columns | No | string or list of strings |
|  |  | query | Yes | string |
|  |  | check | No | True for WITH CHECK OPTION clause |
|  | create_or_alter |  |  |  |
|  | drop |  |  |  |
|  | comment |  |  |  |
| `Trigger` | create | inactive | No | Create inactive trigger |
|  | recreate |  |  |  |
|  | create_or_alter |  |  |  |
|  | alter |  |  | Requires parameters for either header or body definition. |
|  |  | fire_on | No | string |
|  |  | active | No | bool |
|  |  | sequence | No | integer |
|  |  | declare | No | string or list of strings |
|  |  | code | No | string or list of strings |
|  | drop |  |  |  |
|  | comment |  |  |  |
| `Procedure` | create | no_code | No | True to suppress procedure body from output |
|  | recreate | no_code | No | True to suppress procedure body from output |
|  | create_or_alter | no_code | No | True to suppress procedure body from output |
|  | alter | input | No | Input parameters |
|  |  | output | No | Output parameters |
|  |  | declare | No | Variable declarations |
|  |  | code | Yes | Procedure code / body |
|  | drop |  |  |  |
|  | comment |  |  |  |
| `Role` | create |  |  |  |
|  | drop |  |  |  |
|  | comment |  |  |  |
| `Function` | declare |  |  |  |
|  | drop |  |  |  |
|  | create | no_code | No | Generate PSQL function code or not |
|  | create_or_alter | no_code | No | Generate PSQL function code or not |
|  | recreate | no_code | No | Generate PSQL function code or not |
|  | alter | arguments | No | Function arguments |
|  |  | returns | Yes | Function return value |
|  |  | declare | No | Variable declarations |
|  |  | code | Yes | PSQL function body / code |
|  | comment |  |  |  |
| `DatabaseFile` | create |  |  |  |
| `Shadow` | create |  |  |  |
|  | drop | preserve | No | Preserve file or not |
| `Privilege` | grant | grantors | No | List of grantor names. Generates GRANTED BY clause if grantor is not in list. |
|  | revoke | grantors | No | List of grantor names. Generates GRANTED BY clause if grantor is not in list. |
|  |  | grant_option | No | True to get REVOKE of GRANT/ADMIN OPTION only. Raises Error if privilege doesn't have such option. |
| `Package` | create | body | No | (bool) Generate package body |
|  | recreate | body | No | (bool) Generate package body |
|  | create_or_alter | body | No | (bool) Generate package body |
|  | alter | header | No | (string_or_list) Package header |
|  | drop | body | No | (bool) Drop only package body |

**Examples:**

```python
  >>> from firebird.driver import connect
  >>> con = fdb.connect('employee', user='sysdba', password='masterkey')
  >>> t = con.schema.tables.get('EMPLOYEE')
  >>> print(t.get_sql_for('create'))
  CREATE TABLE EMPLOYEE
  (
    EMP_NO EMPNO NOT NULL,
    FIRST_NAME "FIRSTNAME" NOT NULL,
    LAST_NAME "LASTNAME" NOT NULL,
    PHONE_EXT VARCHAR(4),
    HIRE_DATE TIMESTAMP DEFAULT 'NOW' NOT NULL,
    DEPT_NO DEPTNO NOT NULL,
    JOB_CODE JOBCODE NOT NULL,
    JOB_GRADE JOBGRADE NOT NULL,
    JOB_COUNTRY COUNTRYNAME NOT NULL,
    SALARY SALARY NOT NULL,
    FULL_NAME COMPUTED BY (last_name || ', ' || first_name),
    PRIMARY KEY (EMP_NO)
  )
  >>> for i in t.indices:
  ...    if 'create' in i.actions:
  ...        print(i.get_sql_for('create'))
  ...
  CREATE ASCENDING INDEX NAMEX ON EMPLOYEE (LAST_NAME,FIRST_NAME)
  >>> for c in [x for x in t.constraints if x.is_check() or x.is_fkey()]:
  ...    print(c.get_sql_for('create'))
  ...
  ALTER TABLE EMPLOYEE ADD FOREIGN KEY (DEPT_NO)
    REFERENCES DEPARTMENT (DEPT_NO)
  ALTER TABLE EMPLOYEE ADD FOREIGN KEY (JOB_CODE,JOB_GRADE,JOB_COUNTRY)
    REFERENCES JOB (JOB_CODE,JOB_GRADE,JOB_COUNTRY)
  ALTER TABLE EMPLOYEE ADD CHECK ( salary >= (SELECT min_salary FROM job WHERE
                          job.job_code = employee.job_code AND
                          job.job_grade = employee.job_grade AND
                          job.job_country = employee.job_country) AND
              salary <= (SELECT max_salary FROM job WHERE
                          job.job_code = employee.job_code AND
                          job.job_grade = employee.job_grade AND
                          job.job_country = employee.job_country))
  >>> p = con.schema.procedures.get('GET_EMP_PROJ')
  >>> print(p.get_sql_for('recreate', no_code=True))
  RECREATE PROCEDURE GET_EMP_PROJ (EMP_NO SMALLINT)
  RETURNS (PROJ_ID CHAR(5))
  AS
  BEGIN
    SUSPEND;
  END
  >>> print(p.get_sql_for('create_or_alter'))
  CREATE OR ALTER PROCEDURE GET_EMP_PROJ (EMP_NO SMALLINT)
  RETURNS (PROJ_ID CHAR(5))
  AS
  BEGIN
FOR SELECT proj_id
	FROM employee_project
	WHERE emp_no = :emp_no
	INTO :proj_id
DO
	SUSPEND;
  END
  >>> print(p.get_sql_for('alter',input=['In1 INTEGER','In2 VARCHAR(5)'],
  ... output='Out1 INETEGER,\nOut2 VARCHAR(10)',declare=['declare variable i integer = 1;'],
  ... code=['/* body */','Out1 = i',"Out2 = 'Value'"]))
  ALTER PROCEDURE GET_EMP_PROJ (
    In1 INTEGER,
    In2 VARCHAR(5)
  )
  RETURNS (Out1 INETEGER,
  Out2 VARCHAR(10))
  AS
    declare variable i integer = 1;
  BEGIN
    /* body */
    Out1 = i
    Out2 = 'Value'
  END

```


## Working with user privileges

User or database object privileges are part of database metadata accessible through
`Schema` class. Each discrete privilege is represented by `Privilege` instance. You can
access either [all][firebird.lib.schema.Schema.privileges] privileges, or privileges granted for specific
[table][firebird.lib.schema.Table.privileges], [table column][firebird.lib.schema.TableColumn.privileges],
[view][firebird.lib.schema.View.privileges], [view column][firebird.lib.schema.ViewColumn.privileges],
[procedure][firebird.lib.schema.Procedure.privileges] or [role][firebird.lib.schema.Role.privileges]. It's also possible to
get all privileges [granted to][firebird.lib.schema.Schema.get_privileges_of] specific user, role,
procedure, trigger or view.

`Privilege` class supports [`SchemaItem.get_sql_for()`][firebird.lib.schema.SchemaItem.get_sql_for] method to generate GRANT and
REVOKE SQL statements for given privilege. If you want to generate grant/revoke statements
for set of privileges (for example all privileges granted on specific object or grated to
specific user), it's more convenient to use function [`get_grants()`][firebird.lib.schema.get_grants] that returns list of
minimal set of SQL commands required for task.

**Examples:**

```python
>>> from firebird.driver import connect
>>> from firebird.lib.schema import get_grants
>>> con = connect('employee', user='sysdba', password='masterkey')
>>> t = con.schema.tables.get('EMPLOYEE')
>>> for p in t.privileges:
...    print(p.get_sql_for('grant'))
...
GRANT SELECT ON EMPLOYEE TO SYSDBA WITH GRANT OPTION
GRANT INSERT ON EMPLOYEE TO SYSDBA WITH GRANT OPTION
GRANT UPDATE ON EMPLOYEE TO SYSDBA WITH GRANT OPTION
GRANT DELETE ON EMPLOYEE TO SYSDBA WITH GRANT OPTION
GRANT REFERENCES ON EMPLOYEE TO SYSDBA WITH GRANT OPTION
GRANT SELECT ON EMPLOYEE TO PUBLIC WITH GRANT OPTION
GRANT INSERT ON EMPLOYEE TO PUBLIC WITH GRANT OPTION
GRANT UPDATE ON EMPLOYEE TO PUBLIC WITH GRANT OPTION
GRANT DELETE ON EMPLOYEE TO PUBLIC WITH GRANT OPTION
GRANT REFERENCES ON EMPLOYEE TO PUBLIC WITH GRANT OPTION
>>> for p in get_grants(t.privileges):
...    print(p)
...
GRANT DELETE, INSERT, REFERENCES, SELECT, UPDATE ON EMPLOYEE TO PUBLIC WITH GRANT OPTION
GRANT DELETE, INSERT, REFERENCES, SELECT, UPDATE ON EMPLOYEE TO SYSDBA WITH GRANT OPTION

```

Normally generated GRANT/REVOKE statements don't contain grantor's name. If you want to
get GRANT/REVOKE statements including this clause, use `grantors` parameter for `get_sql_for`
and `get_grants`. This parameter is a list of grantor names, and GRANTED BY clause is
generated **only** for privileges not granted by user from this list. It's useful to
suppress GRANTED BY clause for SYSDBA or database owner.
