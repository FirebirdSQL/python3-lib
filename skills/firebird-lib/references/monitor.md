# Monitoring

`firebird.lib.monitor.Monitor` reads `MON$` tables through a driver connection. Use
`connection.monitor` for a connection-managed instance, or use an explicit context
manager when the monitoring lifetime is short:

```python
from firebird.lib.monitor import Monitor

with Monitor(connection) as monitor:
    for attachment in monitor.attachments:
        print(attachment.id, attachment.user)
```

Properties such as `db`, `attachments`, `transactions`, `statements`, `callstack`,
`iostats`, and `tablestats` load lazily. The first query starts an activity snapshot;
cached objects continue to describe that snapshot. Call `take_snapshot()` for a fresh
one, or `clear()` to defer the next snapshot until the next access. Previously held
info objects then describe old data and should be reacquired. Close an explicitly
created monitor to release its cursor. In the current implementation, `clear()` does
not reset the compiled statements collection; create a new monitor when fresh
compiled statement data is required.

Some `MON$` tables and columns depend on the Firebird engine version. Optional
properties may return `None`, including wire transport details for embedded
attachments and compiled statement links when unavailable. Check for `None` before
using those values. Use the connected engine version rather than the Python package
version to decide whether a monitoring feature is available.
