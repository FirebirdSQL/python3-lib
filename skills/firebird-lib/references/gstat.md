# gstat reports

`firebird.lib.gstat.StatDatabase` parses text produced by Firebird `gstat` or by a
driver Services API statistics request. It does not itself run the service. For a
complete report, pass an iterable of lines to `parse()`:

```python
from firebird.lib.gstat import StatDatabase

stats = StatDatabase()
stats.parse(report_lines)
print(stats.tables, stats.indices)
```

For a live stream, call `push(line)` for each line and `push(STOP)` at the end;
`STOP` is from `firebird.base.types`. Finalization converts distributions and
freezes table and index lists. Do not read partial results as a complete report.

Reports differ by Firebird version and by requested gstat options. Sections can be
absent, so use `has_table_stats()`, `has_index_stats()`, and other `has_*` methods
before assuming their data is present. Parser support and exact attributes should
be checked against the installed `firebird-lib` version when handling newer output.
