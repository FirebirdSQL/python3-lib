<a id="processing-firebrid-trace"></a>

# Processing output from Firebird server trace sessions

The Firebird trace & audit sessions are important tool to diagnose wide rande of problems
(for example to identify slow queries, unused indices, problems with transactions etc.).
However, the output from trace session is quite verbose textual output not well-suited for
machine processing, as individual entries have different number of lines and also the event
format is not uniform. The `trace` module provides `TraceParser` class that parses output
from Firebird trace session into series of [dataclass](https://docs.python.org/3/library/dataclasses.html#dataclasses.dataclass) objects
containing information about individual events.

## Parsing the output from trace session

There are three methods how to parse the Firebird trace session log:

1. Using [`TraceParser.parse()`][firebird.lib.trace.TraceParser.parse] method that takes an iterable that return lines produced
by Firebird trace session. The source could be for example open file, list of strings, or
generator expression yielding these lines from service. This method yields instances
of dataclasses that descend from `TraceInfo` or `TraceEvent`.

   Example:

   ```text
   from firebird.lib.trace import TraceParser

   parser = TraceParser()
   with open(filename) as f:
       for obj in parser.parse(f):
           print(str(obj))

   ```
2. Using [`TraceParser.parse_event()`][firebird.lib.trace.TraceParser.parse_event] method that takes list of lines consisting **single**
trace event, and returns single `TraceEvent` instance. However, this method may also produce
`TraceInfo` instances that relate to tre returned entry, which must be retrieved with
[`TraceParser.retrieve_info()`][firebird.lib.trace.TraceParser.retrieve_info] call.

   Example:

   ```text
   from firebird.lib.trace import TraceParser

   parser = TraceParser()
   print(str(parser.parse_event(event_lines)))
   for info in parser.retrieve_info():
       print(str(info))

   ```
3. Using [`TraceParser.push()`][firebird.lib.trace.TraceParser.push] to pass trace session line by line, in a loop. When last
line is stored, it's necessary to call [`TraceParser.push()`][firebird.lib.trace.TraceParser.push] with `STOP`
sentinel to indicate the end of processing. This method returns either a list of
`TraceInfo` / `TraceEvent` instances, or None if lines accumulated so far does not contain
whole trace event.

   Example:

   ```text
   from firebird.lib.trace import TraceParser
   from firebird.base.type import STOP

   parser = LogParser()
   with open(filename) as f:
       for line in f:
           if events := parser.push(line):
               for event in events:
                   print(str(event))
   if events := parser.push(STOP):
       for event in events:
           print(str(event))


   ```
[firebird-driver]: https://pypi.org/project/firebird-driver/
[Firebird]: http://www.firebirdsql.org
[Visitor Pattern]: http://en.wikipedia.org/wiki/Visitor_pattern
