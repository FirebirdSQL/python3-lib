<a id="working-with-monitoring-tables"></a>

# Working with monitoring tables

The Firebird engine offers a set of “virtual” tables (so-called "monitoring tables") that
provides the user with a snapshot of the current activity within the given database.
Firebrd-driver provides access to this information through set of classes (isolated in
separate module `firebird.lib.monitor`) that transform information stored in monitoring
tables into set of Python objects that surface the information in meaningful way, and
additionally provide set of methods for available operations or checks.

Like database schema, monitoring tables could be accessed in two different ways, each
suitable for different use case:

- By direct creation of `Monitor` instances that are binded to particular
    `Connection` instance. This method is best if you want to work
    with monitoring data only occasionally, or you want to keep connections as lightweight
    as possible.
- Accessing `Connection.monitor` property.
    This method is more convenient than previous one, and represents a compromise between
    convenience and resource consumption because `Monitor` instance is not created until
    first reference and is managed by connection itself.

**Examples:**

1. Using Monitor instance:

```python
>>> from firebird.driver import connect
>>> from firebird.lib.monitor import Monitor
>>> con = connect('employee', user='sysdba', password='masterkey')
>>> monitor = Monitor(con)
>>> monitor.db.name
'/var/lib/firebird/sample/employee.fdb'

```

2. Using Connection.monitor:

```python
>>> from firebird.driver import connect
>>> con = connect('employee', user='sysdba', password='masterkey')
>>> con.monitor.db.name
'/var/lib/firebird/sample/employee.fdb'

```

## Information provided by `Monitor`

The `Monitor` provides information about:

- [Database][firebird.lib.monitor.Monitor.db].
- [Connections][firebird.lib.monitor.Monitor.attachments] to database and [current][firebird.lib.monitor.Monitor.this_attachment] connection.
- [Transactions][firebird.lib.monitor.Monitor.transactions].
- Executed [SQL statements][firebird.lib.monitor.Monitor.statements].
- PSQL [callstack][firebird.lib.monitor.Monitor.callstack].
- [Page and row I/O statistics][firebird.lib.monitor.Monitor.iostats], including memory usage.
- [Table I/O statistics][firebird.lib.monitor.Monitor.tablestats].
- [Context variables][firebird.lib.monitor.Monitor.variables].

!!! tip

    The monitor module uses enhanced `firebird.base.collections.DataList` list
    descendant for collections of monitoring information objects. For details, see section
    [Enhanced list of objects](schema.md#enhanced-object-list).

## Activity snapshot

The key term of the monitoring feature is an `activity snapshot`. It represents the current
state of the database, comprising a variety of information about the database itself, active
attachments and users, transactions, prepared and running statements, and more.

A snapshot is created the first time any of the monitoring information is being accessed
from in the given `Monitor` instance, or whenever [`Monitor.take_snapshot()`][firebird.lib.monitor.Monitor.take_snapshot] is called.
All fetched information is preserved until instance is [closed][firebird.lib.monitor.Monitor.close],
[clared][firebird.lib.monitor.Monitor.clear] or new snapshot is [taken][firebird.lib.monitor.Monitor.take_snapshot], in order
that accessed information is always consistent.

There are two ways to refresh the snapshot:

1. Call [`Monitor.clear()`][firebird.lib.monitor.Monitor.clear] method. New snapshot will be taken on next access to monitoring
    information.
2. Call [`Monitor.take_snapshot()`][firebird.lib.monitor.Monitor.take_snapshot] method to take the new snapshot immediately.

!!! important

    In both cases, any instances of information objects your application may hold would be
    obsolete. Using them may result in error, or (more likely) provide outdated information.

!!! note

    Individual monitoring information (i.e. information about [connections][firebird.lib.monitor.Monitor.attachments],
    [transactions][firebird.lib.monitor.Monitor.transactions] etc.) is loaded from activity snapshot on first
    access and cached for further reference until it's [clared][firebird.lib.monitor.Monitor.clear] or new
    snapshot is [taken][firebird.lib.monitor.Monitor.take_snapshot].

    Because once loaded information is cached, it's good to [clear][firebird.lib.monitor.Monitor.clear] it
    when it's no longer needed to conserve memory.

## I/O statistics

Page & row I/O statistics (as `IOStatsInfo` instances) and table I/O statistics
(as `TableStatsInfo` instances) could be accessed in two different ways:

1. Properties [`Monitor.iostats`][firebird.lib.monitor.Monitor.iostats] and [`Monitor.tablestats`][firebird.lib.monitor.Monitor.tablestats] provide access to **all**
    information collected in current activity snapshot.
2. Where applicable, the individual information item classes have their own `iostats` and
    `tablestats` properties that provide access only to I/O statistics related to this
    particular object.
