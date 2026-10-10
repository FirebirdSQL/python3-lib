<a id="processing-firebrid-log"></a>

# Processing Firebird server log

The Firebird server log contains vital information about errors, warnings or other important
events in Firebird engine. However, this log is not well-suited for machine processing,
as individual entries may have different number of lines and also the event message format
is not uniform. The `log` module provides `LogParser` class that parses Firebird server
log into series of `LogMessage` [dataclass](https://docs.python.org/3/library/dataclasses.html#dataclasses.dataclass) objects.

## Parsing the log

There are three methods how to parse the Firebird server log:

1. Using [`LogParser.parse()`][firebird.lib.log.LogParser.parse] method that takes an iterable that return lines from
    Firebird server log. The source could be for example open file, list of strings, or
    generator expression yielding these lines. This method yields `LogMessage` instances.
    These instances contain information about [`LogMessage.origin`][firebird.lib.log.LogMessage.origin], [`LogMessage.timestamp`][firebird.lib.log.LogMessage.timestamp],
    severity [`LogMessage.level`][firebird.lib.log.LogMessage.level], unique event [`LogMessage.code`][firebird.lib.log.LogMessage.code],  server [`LogMessage.facility`][firebird.lib.log.LogMessage.facility],
    [`LogMessage.message`][firebird.lib.log.LogMessage.message] and additional [`LogMessage.params`][firebird.lib.log.LogMessage.params].

    Example:

   ```text
   from firebird.lib.log import LogParser

   parser = LogParser()
   with open(filename) as f:
       for obj in parser.parse(f):
           print(str(obj))

    ```
2. Using [`LogParser.parse_entry()`][firebird.lib.log.LogParser.parse_entry] method that takes list of lines consisting **single**
    log entry, and returns it as single `LogMessage`.

    Example:

   ```text
   from firebird.lib.log import LogParser

   parser = LogParser()
   print(str(parser.parse_entry(entry_lines)))

    ```
3. Using [`LogParser.push()`][firebird.lib.log.LogParser.push] to pass server log line by line, in a loop. When last line
is stored, it's necessary to call [`LogParser.push()`][firebird.lib.log.LogParser.push] with `STOP`
sentinel to indicate the end of processing. This method returns either `LogMessage`, or
`None` if lines accumulated so far does not contain whole log entry.

   Example:

   ```text
   from firebird.lib.log import LogParser
   from firebird.base.types import STOP

   parser = LogParser()
   with open(filename) as f:
       for line in f:
           if event := parser.push(line):
               print(str(event))
   if event := parser.push(STOP):
       print(str(event))

   ```
