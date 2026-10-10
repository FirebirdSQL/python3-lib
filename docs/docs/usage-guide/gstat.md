<a id="processing-gstat-output"></a>

# Processing output from gstat utility

The GSTAT utility analyzes low-level database structures and produces textual reports,
that could be used to evaluate efficiency of the database or diagnose various
storage-related problems. However, these reports are not well-suited for machine processing.
The `gstat` module provides `StatDatabase` class that parses gstat reports into set of
Python objects suitable for further processing.

## Parsing gstat output

There are two methods how to parse the gstat output:

1. Using [`StatDatabase.parse()`][firebird.lib.gstat.StatDatabase.parse] method that takes an iterable that return lines from database
    analysis produced by Firebird gstat. The source could be for example open file, list
    of strings, or generator expression yielding these lines.

    Example:

    ```text
    from firebird.lib.gstat import StatDatabase
    db = StatDatabase()
    with open(filename) as f:
        db.parse(f)

    ```
2. Using [`StatDatabase.push()`][firebird.lib.gstat.StatDatabase.push] to pass gstat report line by line, in a loop. When all
    lines are stored, it's necessary to call [`StatDatabase.push()`][firebird.lib.gstat.StatDatabase.push] with `STOP`
    sentinel to indicate the end of processing.

    Example:

   ```text
   from firebird.lib.gstat import StatDatabase
   from firebird.base.types import STOP

   db = StatDatabase()
   with open(filename) as f:
       for line in f:
           db.push(line)
   db.push(STOP)


   ```
When gstat report is fully parsed, you can start processing the information stored in `StatDatabase` instance.
