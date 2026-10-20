# Schema

Use `firebird.lib.schema.Schema` with a live `firebird.driver.Connection` to inspect
`RDB$` metadata. The connection-managed `connection.schema` is convenient for ordinary
use and is created lazily. For explicit lifetime control, bind a separate instance:

```python
from firebird.driver import connect
from firebird.lib.schema import Schema

with connect(database, user=user, password=password) as connection:
    with Schema().bind(connection) as schema:
        table = schema.tables.get('EMPLOYEE')
        if table is not None:
            print(table.get_sql_for('create'))
```

`Schema` collections load on first access and then cache their objects. Use `clear()`
or `reload()` to observe metadata changes; `reload()` can select `Category` values.
Do not call `bind()` or `close()` on the connection-managed `connection.schema`: its
lifetime belongs to the connection. Close a separately bound schema when finished.

Object classes expose relationships, privileges, and supported DDL actions through
`SchemaItem.actions` and `get_sql_for(action)`. Use `Schema.get_metadata_ddl()` when
the ordering of a full script matters. Generated SQL describes metadata; executing
it is a separate application decision.

Firebird 4 introduced role system privileges and SQL SECURITY metadata. In Firebird
3, `Role.system_privileges` is empty and `sql_security` is `None`. On later versions,
`sql_security` is `"DEFINER"`, `"INVOKER"`, or `None`; `None` means no explicit mode.
Do not turn `None` into an explicit SQL SECURITY clause when generating SQL.
