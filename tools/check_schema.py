# SPDX-FileCopyrightText: 2026-present The Firebird Projects <www.firebirdsql.org>
#
# SPDX-License-Identifier: MIT
#
# PROGRAM/MODULE: firebird-lib
# FILE:           tools/check_schema.py
# DESCRIPTION:    Report RDB$ metadata not consumed by firebird.lib.schema

"""Print a Markdown inventory of RDB$ metadata not used by schema.py."""

from __future__ import annotations

import ast
import os
import re
import sys
from collections import defaultdict
from importlib.metadata import entry_points
from inspect import signature
from pathlib import Path
from tempfile import TemporaryDirectory

from firebird.driver import create_database, get_api, load_api
from firebird.lib.schema import Schema

# Next tables should eb exluded from check
EXCLUDE_TABLES = [
    'RDB$FORMATS', 'RDB$LOG_FILES', 'RDB$PAGES', 'RDB$SECURITY_CLASSES',
    'RDB$TRIGGER_MESSAGES'
]

# Next colums should be excluded from check (mapping TABLE_NAME:LIST_OF_COLUMNS)
EXCLUDE_COLUMNS = {
    'RDB$CHARACTER_SETS': ['RDB$FORM_OF_USE', 'RDB$FUNCTION_NAME', 'RDB$NUMBER_OF_CHARACTERS'],
    'RDB$DATABASE': ['RDB$RELATION_ID'],
    'RDB$FIELDS': ['RDB$COMPUTED_BLR', 'RDB$VALIDATION_BLR', 'RDB$DEFAULT_VALUE',
                   'RDB$EDIT_STRING', 'RDB$MISSING_SOURCE', 'RDB$MISSING_VALUE',
                   'RDB$QUERY_HEADER', 'RDB$QUERY_NAME'],
    'RDB$FILTERS': ['RDB$SECURITY_CLASS'],
    'RDB$FUNCTIONS': ['RDB$DEBUG_INFO', 'RDB$FUNCTION_BLR', 'RDB$FUNCTION_TYPE', 'RDB$QUERY_NAME'],
    'RDB$FUNCTION_ARGUMENTS': ['RDB$DEFAULT_VALUE'],
    'RDB$INDICES': ['RDB$EXPRESSION_BLR', 'RDB$CONDITION_BLR'],
    'RDB$PROCEDURES': ['RDB$DEBUG_INFO', 'RDB$PROCEDURE_BLR', 'RDB$RUNTIME'],
    'RDB$PROCEDURE_PARAMETERS': ['RDB$DEFAULT_VALUE'],
    'RDB$RELATIONS': ['RDB$EXTERNAL_DESCRIPTION', 'RDB$FIELD_ID', 'RDB$RUNTIME'],
    'RDB$RELATION_FIELDS': ['RDB$COMPLEX_NAME', 'RDB$DEFAULT_VALUE', 'RDB$EDIT_STRING',
                            'RDB$QUERY_HEADER', 'RDB$QUERY_NAME'],
    'RDB$TRIGGERS': ['RDB$DEBUG_INFO', 'RDB$TRIGGER_BLR '],
    'RDB$TYPES': ['RDB$DESCRIPTION', 'RDB$SYSTEM_FLAG'],
    'RDB$VIEW_RELATIONS': ['RDB$CONTEXT_NAME', 'RDB$CONTEXT_TYPE'],
}

SCHEMA_SOURCE = Path(__file__).resolve().parents[1] / 'src/firebird/lib/schema.py'
IDENTIFIER = re.compile(r'\bRDB\$[A-Z][A-Z_0-9]*\b', re.IGNORECASE)
TABLE = re.compile(r'\b(?:FROM|JOIN)\s+(RDB\$[A-Z][A-Z_0-9]*)(?:\s+(?:AS\s+)?([A-Z][A-Z_0-9]*))?',
                   re.IGNORECASE)
STOP_WORDS = {'WHERE', 'ON', 'LEFT', 'RIGHT', 'INNER', 'OUTER', 'JOIN', 'ORDER', 'GROUP', 'UNION'}

# Classes whose _attributes dictionaries originate from a single RDB$ table.
# Constraint is handled separately because its rows combine three tables.
CLASS_TABLE = {
    'Collation': 'RDB$COLLATIONS', 'CharacterSet': 'RDB$CHARACTER_SETS',
    'DatabaseException': 'RDB$EXCEPTIONS', 'Sequence': 'RDB$GENERATORS',
    'TableColumn': 'RDB$RELATION_FIELDS', 'ViewColumn': 'RDB$RELATION_FIELDS',
    'Index': 'RDB$INDICES', 'Domain': 'RDB$FIELDS', 'Dependency': 'RDB$DEPENDENCIES',
    'Table': 'RDB$RELATIONS', 'View': 'RDB$RELATIONS', 'Trigger': 'RDB$TRIGGERS',
    'ProcedureParameter': 'RDB$PROCEDURE_PARAMETERS', 'Procedure': 'RDB$PROCEDURES',
    'Role': 'RDB$ROLES', 'FunctionArgument': 'RDB$FUNCTION_ARGUMENTS',
    'Function': 'RDB$FUNCTIONS', 'DatabaseFile': 'RDB$FILES', 'Shadow': 'RDB$FILES',
    'Privilege': 'RDB$USER_PRIVILEGES', 'Package': 'RDB$PACKAGES',
    'BackupHistory': 'RDB$BACKUP_HISTORY', 'Filter': 'RDB$FILTERS',
}
CONSTRAINT_TABLES = {'RDB$RELATION_CONSTRAINTS', 'RDB$REF_CONSTRAINTS', 'RDB$CHECK_CONSTRAINTS'}


def _attribute_key(node: ast.AST) -> tuple[str, bool] | None:
    """Return a literal attributes key and whether the dictionary belongs to self."""
    if isinstance(node, ast.Subscript) and isinstance(node.ctx, ast.Load):
        owner, key = node.value, node.slice
    elif isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and node.func.attr == 'get':
        owner = node.func.value
        key = node.args[0] if node.args else None
    else:
        return None
    if not isinstance(owner, ast.Attribute) or owner.attr not in {'_attributes', '__attrs'}:
        return None
    if isinstance(key, ast.Constant) and isinstance(key.value, str):
        is_self = isinstance(owner.value, ast.Name) and owner.value.id == 'self'
        return key.value.upper(), is_self
    return None


class SourceInventory(ast.NodeVisitor):
    """Collect executable SQL, class-specific attribute reads, and enum_dict bindings."""

    def __init__(self):
        self.strings: list[str] = []
        self.direct_sql: list[str] = []
        self.reads: dict[str, set[str]] = defaultdict(set)
        self.other_reads: set[str] = set()
        self.alias_reads: set[str] = set()
        self.row_used: dict[str, set[str]] = defaultdict(set)
        self.row_uncertain: dict[str, set[str]] = defaultdict(set)
        self.type_maps: dict[str, str] = {}
        self.current_class: str | None = None
        self.row_keys: set[str] | None = None

    def _body(self, body: list[ast.stmt]) -> None:
        for index, node in enumerate(body):
            if index == 0 and isinstance(node, ast.Expr) and isinstance(node.value, ast.Constant) \
                    and isinstance(node.value.value, str):
                continue  # Docstring, not executable metadata use.
            self.visit(node)

    def visit_Module(self, node: ast.Module) -> None:
        self._body(node.body)

    def visit_ClassDef(self, node: ast.ClassDef) -> None:
        previous = self.current_class
        self.current_class = node.name
        self._body(node.body)
        self.current_class = previous

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        previous = self.row_keys
        self.row_keys = set()
        first_string = len(self.strings)
        self._body(node.body)
        tables = {match.group(1).upper() for sql in self.strings[first_string:]
                  for match in TABLE.finditer(sql)}
        for column in self.row_keys:
            if len(tables) == 1:
                self.row_used[next(iter(tables))].add(column)
            else:
                for table in tables:
                    self.row_uncertain[table].add(column)
        self.row_keys = previous

    visit_AsyncFunctionDef = visit_FunctionDef  # noqa: N815

    def visit_Constant(self, node: ast.Constant) -> None:
        if isinstance(node.value, str):
            self.strings.append(node.value)

    def visit_JoinedStr(self, node: ast.JoinedStr) -> None:
        self.strings.append(''.join(part.value for part in node.values
                                    if isinstance(part, ast.Constant) and isinstance(part.value, str)))
        self.generic_visit(node)

    def visit_Subscript(self, node: ast.Subscript) -> None:
        if item := _attribute_key(node):
            key, is_self = item
            if IDENTIFIER.fullmatch(key) and is_self:
                self.reads[self.current_class or ''].add(key)
            elif IDENTIFIER.fullmatch(key):
                self.other_reads.add(key)
            else:
                self.alias_reads.add(key)
        if self.row_keys is not None and isinstance(node.value, ast.Name) \
                and isinstance(node.slice, ast.Constant) and isinstance(node.slice.value, str) \
                and IDENTIFIER.fullmatch(node.slice.value) and isinstance(node.ctx, ast.Load):
            self.row_keys.add(node.slice.value.upper())
        self.generic_visit(node)

    def visit_Call(self, node: ast.Call) -> None:
        if item := _attribute_key(node):
            key, is_self = item
            if IDENTIFIER.fullmatch(key) and is_self:
                self.reads[self.current_class or ''].add(key)
            elif IDENTIFIER.fullmatch(key):
                self.other_reads.add(key)
            else:
                self.alias_reads.add(key)
        if isinstance(node.func, ast.Attribute) and node.func.attr == 'execute' \
                and isinstance(node.func.value, ast.Attribute) and node.func.value.attr == '_ic' \
                and node.args:
            sql = node.args[0]
            if isinstance(sql, ast.Constant) and isinstance(sql.value, str):
                self.direct_sql.append(sql.value)
            elif isinstance(sql, ast.JoinedStr):
                self.direct_sql.append(''.join(part.value for part in sql.values
                                               if isinstance(part, ast.Constant) and isinstance(part.value, str)))
        self.generic_visit(node)

    def visit_Assign(self, node: ast.Assign) -> None:
        if isinstance(node.value, ast.Call) and isinstance(node.value.func, ast.Name) \
                and node.value.func.id == 'enum_dict' and node.value.args \
                and isinstance(node.value.args[0], ast.Constant):
            for target in node.targets:
                if isinstance(target, ast.Attribute) and isinstance(target.value, ast.Name) \
                        and target.value.id == 'self':
                    self.type_maps[node.value.args[0].value] = target.attr
        self.generic_visit(node)


def analyze_source(source: str) -> SourceInventory:
    """Analyze source without executing its SQL or counting documentation text."""
    result = SourceInventory()
    result.visit(ast.parse(source))
    return result


def _sql_usage(strings: list[str], catalog: dict[str, set[str]]
               ) -> tuple[set[str], dict[str, set[str]], dict[str, set[str]]]:
    tables: set[str] = set()
    used: dict[str, set[str]] = defaultdict(set)
    uncertain: dict[str, set[str]] = defaultdict(set)
    for sql in strings:
        if not re.search(r'\b(?:SELECT|FROM|JOIN)\b', sql, re.IGNORECASE):
            continue
        found = list(TABLE.finditer(sql))
        query_tables = {match.group(1).upper() for match in found} & catalog.keys()
        tables.update(query_tables)
        aliases = {match.group(1).upper(): match.group(1).upper() for match in found}
        aliases.update({match.group(2).upper(): match.group(1).upper() for match in found
                        if match.group(2) and match.group(2).upper() not in STOP_WORDS})
        # SQL predicates and joins consume their operands. Projections alone do not.
        condition = re.search(r'\b(?:WHERE|ON|ORDER\s+BY|GROUP\s+BY)\b', sql, re.IGNORECASE)
        if condition is None:
            continue
        fragment = sql[condition.start():]
        qualified = set()
        for match in re.finditer(r'\b([A-Z][A-Z_0-9]*)\.(RDB\$[A-Z][A-Z_0-9]*)\b', fragment,
                                 re.IGNORECASE):
            table, column = aliases.get(match.group(1).upper()), match.group(2).upper()
            if table in query_tables and column in catalog[table]:
                used[table].add(column)
                qualified.add(column)
        for column in {item.upper() for item in IDENTIFIER.findall(fragment)} - qualified:
            candidates = {table for table in query_tables if column in catalog[table]}
            if len(candidates) == 1:
                used[next(iter(candidates))].add(column)
            else:
                for table in candidates:
                    uncertain[table].add(column)
    return tables, used, uncertain


def column_gaps(catalog: dict[str, set[str]], inventory: SourceInventory
                ) -> tuple[set[str], dict[str, set[str]], dict[str, set[str]]]:
    """Return queried tables, unused columns, and ambiguously attributed columns."""
    tables, used, uncertain = _sql_usage(inventory.strings, catalog)
    for sql in inventory.strings:
        aliases = {match.group(2).upper(): match.group(1).upper() for match in TABLE.finditer(sql)
                   if match.group(2) and match.group(2).upper() not in STOP_WORDS}
        for match in re.finditer(r'\b([A-Z][A-Z_0-9]*)\.(RDB\$[A-Z][A-Z_0-9]*)\s+AS\s+([A-Z][A-Z_0-9]*)',
                                 sql, re.IGNORECASE):
            table, column, alias = aliases.get(match.group(1).upper()), match.group(2).upper(), match.group(3).upper()
            if table in tables and column in catalog[table] and alias in inventory.alias_reads:
                used[table].add(column)
    for table in tables:
        used[table].update(inventory.row_used[table] & catalog[table])
        uncertain[table].update(inventory.row_uncertain[table] & catalog[table])
    for cls, table in CLASS_TABLE.items():
        if table in catalog and table in tables:
            used[table].update(inventory.reads[cls] & catalog[table])
            used[table].update(inventory.reads['SchemaItem'] & catalog[table])
    if 'RDB$DATABASE' in catalog:
        used['RDB$DATABASE'].update(inventory.reads['Schema'] & catalog['RDB$DATABASE'])
    for column in inventory.other_reads:
        for table in tables:
            if column in catalog[table]:
                uncertain[table].add(column)
    for sql in inventory.direct_sql:
        query_tables = {match.group(1).upper() for match in TABLE.finditer(sql)} & tables
        projection = re.search(r'\bSELECT\s+(.+?)\s+FROM\b', sql, re.IGNORECASE | re.DOTALL)
        if projection is None:
            continue
        aliases = {match.group(1).upper(): match.group(1).upper() for match in TABLE.finditer(sql)}
        aliases.update({match.group(2).upper(): match.group(1).upper() for match in TABLE.finditer(sql)
                        if match.group(2) and match.group(2).upper() not in STOP_WORDS})
        for match in re.finditer(r'\b([A-Z][A-Z_0-9]*)\.(RDB\$[A-Z][A-Z_0-9]*)\b', projection.group(1),
                                 re.IGNORECASE):
            table, column = aliases.get(match.group(1).upper()), match.group(2).upper()
            if table in query_tables and column in catalog[table]:
                used[table].add(column)
        for column in {item.upper() for item in IDENTIFIER.findall(projection.group(1))}:
            candidates = {table for table in query_tables if column in catalog[table]}
            if len(candidates) == 1:
                used[next(iter(candidates))].add(column)
            else:
                for table in candidates:
                    uncertain[table].add(column)
    for column in inventory.reads['Constraint']:
        candidates = {table for table in CONSTRAINT_TABLES & tables if column in catalog[table]}
        if len(candidates) == 1:
            used[next(iter(candidates))].add(column)
        else:
            for table in candidates:
                uncertain[table].add(column)
    # The direct cursor query in bind() consumes these three RDB$TYPES values.
    if 'RDB$TYPES' in tables:
        used['RDB$TYPES'].update({'RDB$FIELD_NAME', 'RDB$TYPE', 'RDB$TYPE_NAME'} & catalog['RDB$TYPES'])
    for table in tables:
        uncertain[table].difference_update(used[table])
    unused = {table: columns - used[table] - uncertain[table] for table, columns in catalog.items() if table in tables}
    return tables, unused, uncertain


def unmapped_type_rows(rows: list[tuple[str, int, str]], type_maps: dict[str, str], schema
                       ) -> list[tuple[str, int, str, str]]:
    """Compare each RDB$TYPES row with the dictionary populated by Schema.bind()."""
    gaps = []
    for raw_field, code, raw_name in rows:
        field, name = raw_field.strip(), raw_name.strip()
        attribute = type_maps.get(field)
        if attribute is None:
            gaps.append((field, code, name, 'No Schema dictionary'))
        elif (actual := getattr(schema, attribute, {}).get(code)) != name:
            gaps.append((field, code, name, f'{attribute}[{code!r}] = {actual!r}'))
    return gaps


def render_report(version: str, provider: str, *, catalog: dict[str, set[str]], tables: set[str],
                  unused: dict[str, set[str]], uncertain: dict[str, set[str]],
                  type_maps: dict[str, str], type_gaps: list[tuple[str, int, str, str]]) -> str:
    """Render the diagnostic inventory as Markdown."""
    excluded_tables = {name.strip().upper() for name in EXCLUDE_TABLES}
    excluded_columns = {table.strip().upper(): {name.strip().upper() for name in columns}
                        for table, columns in EXCLUDE_COLUMNS.items()}
    visible_tables = tables - excluded_tables
    visible_unused = {table: unused[table] - excluded_columns.get(table, set()) for table in visible_tables}
    visible_uncertain = {table: uncertain[table] - excluded_columns.get(table, set())
                         for table in visible_tables}
    lines = ['# Schema metadata usage report', '', f'- Engine version: {version}', f'- Provider: {provider}', '',
             '## RDB$ tables', '', '| Table | Status | Unused | Uncertain |', '| --- | --- | ---: | ---: |']
    for table in sorted(catalog.keys() - excluded_tables):
        status = 'used' if table in visible_tables else 'unused'
        lines.append(f'| `{table}` | {status} | {len(visible_unused[table]) if table in visible_tables else "—"} | '
                     f'{len(visible_uncertain[table]) if table in visible_tables else "—"} |')
    for table in sorted(visible_tables):
        lines.extend(('', f'### {table}', '', '**Unused columns:** ' +
                      (', '.join(f'`{item}`' for item in sorted(visible_unused[table])) or 'None'), '',
                      '**Uncertain attribution:** ' +
                      (', '.join(f'`{item}`' for item in sorted(visible_uncertain[table])) or 'None')))
    lines.extend(('', '## Schema type dictionaries', '', '| RDB$FIELD_NAME | Schema attribute | Visibility |',
                  '| --- | --- | --- |'))
    for field, attribute in sorted(type_maps.items()):
        visibility = 'internal' if attribute.startswith('_') else 'public'
        lines.append(f'| `{field}` | `{attribute}` | {visibility} |')
    lines.extend(('', '## RDB$TYPES rows without matching Schema dictionary entries', '',
                  '| Field name | Type | Type name | Reason |', '| --- | ---: | --- | --- |'))
    for field, code, name, reason in type_gaps:
        lines.append(f'| `{field}` | {code} | `{name}` | {reason} |')
    if not type_gaps:
        lines.append('| — | — | — | None |')
    lines.extend(('', ('Static column analysis identifies candidates for review; '
                       'uncertain references are listed separately.')))
    return '\n'.join(lines)


def _select_provider():
    providers = list(entry_points(group='firebird.runtime.providers'))
    if len(providers) > 1:
        raise RuntimeError('Multiple Firebird runtime providers are registered: ' +
                           ', '.join(item.name for item in providers))
    if providers:
        provider = providers[0].load()()
        os.environ['FIREBIRD'] = str(provider.root)
        accepts_provider = 'FirebirdProvider' in str(signature(load_api).parameters['filename'].annotation)
        load_api(provider if accepts_provider else provider.client_library)
    api = get_api()
    name = provider.name if providers else 'local'
    return f'{name} ({api.client_library_name})'


def main() -> int:
    provider = _select_provider()
    inventory = analyze_source(SCHEMA_SOURCE.read_text(encoding='utf-8'))
    with TemporaryDirectory(prefix='firebird-schema-check-') as directory:
        path = Path(directory) / 'schema-check.fdb'
        with create_database(path, user='SYSDBA', password='masterkey', charset='UTF8') as connection:
            try:
                with connection.cursor() as cursor:
                    cursor.execute("""select trim(r.rdb$relation_name), trim(f.rdb$field_name)
                                      from rdb$relations r join rdb$relation_fields f
                                        on f.rdb$relation_name = r.rdb$relation_name
                                      where r.rdb$relation_name starting with 'RDB$'""")
                    catalog = defaultdict(set)
                    for table, column in cursor:
                        catalog[table].add(column)
                    cursor.execute('select RDB$FIELD_NAME, RDB$TYPE, RDB$TYPE_NAME from RDB$TYPES')
                    type_rows = list(cursor)
                with Schema().bind(connection) as schema:
                    type_gaps = unmapped_type_rows(type_rows, inventory.type_maps, schema)
                tables, unused, uncertain = column_gaps(catalog, inventory)
                print(render_report(connection.info.version, provider, catalog=catalog, tables=tables,  # noqa: T201
                                    unused=unused, uncertain=uncertain,
                                    type_maps=inventory.type_maps, type_gaps=type_gaps))
            finally:
                connection.drop_database()
    return 0


if __name__ == '__main__':
    try:
        sys.exit(main())
    except Exception as exc:
        print(f'check_schema: {exc}', file=sys.stderr)  # noqa: T201
        sys.exit(1)
