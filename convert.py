"""Convert the legacy Sphinx pages in docs/ to Zensical Markdown in zdoc/docs/.

Run from the repository root with ``python3.11 convert.py --source OLD_DOCS``.
"""

import argparse
import ast
import os
import re
import shutil
from itertools import pairwise
from pathlib import Path

ROOT = Path(__file__).resolve().parent
SOURCE = None
TARGET = None
PAGES = []
ANCHORS = {}
LINKS = {}
HEADINGS = {}

HEADING = {'#': 1, '*': 2, '=': 2, '-': 3, '^': 4, '~': 5, '"': 5}
ADMONITIONS = {'note', 'tip', 'warning', 'important', 'caution', 'seealso', 'error', 'danger'}
ROLE = re.compile(r':(?:py:|c:)?(ref|doc|class|func|meth|mod|attr|data|exc|obj|term|program|file):`([^`]+)`')
DIRECTIVE = re.compile(r'^(\s*)\.\. ([\w:-]+)::\s*(.*)$')
HEADING_RULE = re.compile(r'^([#*=+^"~-])\1{2,}$')
LEGACY_API = re.compile(r'`~?\.((?:schema\.)?[A-Za-z]\w*(?:\.\w+)*)(\(\))?`')
API_MODULES = {}
for module_name in ('schema', 'monitor', 'gstat', 'log', 'trace', 'logmsgs'):
    source = ROOT / 'src' / 'firebird' / 'lib' / f'{module_name}.py'
    for node in ast.parse(source.read_text()).body:
        if isinstance(node, (ast.ClassDef, ast.FunctionDef)):
            API_MODULES.setdefault(node.name, []).append(module_name)


def api_reference(match, current_module=None):
    """Turn a Sphinx relative API role into a mkdocstrings link."""
    name, call = match.groups()
    name = name.removeprefix('schema.')
    modules = API_MODULES.get(name.split('.')[0], [])
    module = current_module.rsplit('.', 1)[-1] if current_module else None
    module = module if module in modules else next(iter(modules), None)
    if module is None:
        return f'`{name}{call or ""}`'
    return f'[`{name}{call or ""}`][firebird.lib.{module}.{name}]'


def inline(text, page, current_module=None):
    def api_link(match):
        label, target = match.groups()
        if target.startswith('firebird.driver.'):
            return f'`{label}`'
        if target == 'dataclasses.dataclass':
            return f'[{label}](https://docs.python.org/3/library/dataclasses.html#dataclasses.dataclass)'
        if target.startswith('.'):
            section = target.split('.')[1]
            module = 'monitor' if section == 'Monitor' else 'schema'
            target = f'firebird.lib.{module}{target}'
        return f'[{label}][{target}]'

    def role(match):
        kind, value = match.groups()
        label, sep, target = value.partition(' <')
        target = target.rstrip('>') if sep else label
        label = label.lstrip('~.')
        if kind == 'doc':
            return f'[{label}]({target}.md)'
        if kind == 'ref':
            if target.startswith('python:'):
                if target == 'python:typeiter':
                    return f'[{label}](https://docs.python.org/3/library/stdtypes.html#iterator-types)'
                return f'[{label}](https://docs.python.org/3/library/{target.split(":")[-1].removeprefix("module-")}.html)'
            if target not in ANCHORS and target not in HEADINGS.get(page, {}):
                return label
            return f'[{label}]({ANCHORS.get(target, page)}.md#{target})'
        name = target.lstrip('~.')
        display = label if sep else name.split('.')[-1] if target.startswith(('~', '.')) else name
        return f'`{display}`'

    # The source has one missing opening backtick in an API link.
    text = text.replace('Connections <.Monitor.attachments>`',
                        '[Connections][firebird.lib.monitor.Monitor.attachments]')
    text = ROLE.sub(role, text)
    text = re.sub(r'`([^`<>]+) <(https?://[^>]+)>`__?', r'[\1](\2)', text)
    text = re.sub(r'`([^`<>]+) <firebird\.base\.(\w+)>`',
                  r'[\1](https://firebird-base.readthedocs.io/en/latest/\2/)', text)
    text = re.sub(r'`([^`<>]+) <([^>]+)>`', api_link, text)
    text = LEGACY_API.sub(lambda match: api_reference(match, current_module), text)
    text = re.sub(r'`~([A-Za-z][\w.]*)`', lambda match: f'`{match.group(1).split(".")[-1]}`', text)
    text = text.replace('`DB API Specification <Python-DB-API-2.0.html>`__',
                        '[DB API Specification](https://peps.python.org/pep-0249/)')
    def rst_link(match):
        name = match.group(1)
        label, sep, target = name.partition(' <')
        target = target.rstrip('>') if sep else label
        for page_links in (LINKS[page], *LINKS.values()):
            if target in page_links:
                return f'[{label}]({page_links[target]})'
        for slug, heading in HEADINGS[page].items():
            if target.lower() == heading.lower() or target == slug:
                return f'[{label}]({page}.md#{slug})'
        if target in ANCHORS:
            return f'[{label}]({ANCHORS[target]}.md#{target})'
        return match.group(0)

    text = re.sub(r'`([^`]+)`_', rst_link, text)
    for name, url in {key: value for links in LINKS.values() for key, value in links.items()}.items():
        if ' ' not in name:
            text = re.sub(r'(?<![\w`])' + re.escape(name) + r'_(?!\w)', f'[{name}]({url})', text)
    return text


def convert(page, body_lines=None, current_module=None):
    lines = page.read_text().splitlines() if body_lines is None else body_lines
    result = []
    module = current_module or f'firebird.lib.{page.stem.removeprefix("ref-")}'
    i = 0
    while i < len(lines):
        line = lines[i]
        stripped = line.strip()
        indent = len(line) - len(line.lstrip())
        directive = DIRECTIVE.match(line)
        if directive:
            prefix, kind, arg = directive.groups()
            if kind in {'module', 'currentmodule', 'currentModule', 'py:currentmodule'}:
                module = arg
                i += 1
                while i < len(lines) and lines[i].strip().startswith(':'):
                    i += 1
                continue
            if kind.startswith('auto'):
                name = arg if '.' in arg else f'{module}.{arg}'
                options = []
                i += 1
                while i < len(lines) and (not lines[i].strip() or
                                           (len(lines[i]) - len(lines[i].lstrip()) > indent and
                                            lines[i].strip().startswith(':'))):
                    if lines[i].strip() == ':no-members:':
                        options.append('        members: false')
                    if lines[i].strip() == ':no-show-inheritance:':
                        options.append('        show_bases: false')
                    i += 1
                result.append(f'{prefix}::: {name}')
                if options:
                    result.extend([f'{prefix}    options:', *[prefix + x for x in options]])
                result.append('')
                continue
            if kind == 'toctree':
                i += 1
                while i < len(lines) and (not lines[i].strip() or lines[i].strip().startswith(':')):
                    i += 1
                while i < len(lines) and lines[i].strip() and len(lines[i]) - len(lines[i].lstrip()) > indent:
                    entry = lines[i].strip()
                    result.append(f'{prefix}- [{entry.replace("-", " ").title()}]({entry}.md)')
                    i += 1
                continue
            if kind == 'include':
                if arg == '../LICENSE':
                    result.extend((ROOT / 'LICENSE').read_text().splitlines())
                i += 1
                continue
            if kind == 'index':
                i += 1
                while i < len(lines) and lines[i].strip() and len(lines[i]) - len(lines[i].lstrip()) > indent:
                    i += 1
                continue
            if kind == 'hlist':
                i += 1
                while i < len(lines) and (not lines[i].strip() or lines[i].strip().startswith(':')):
                    i += 1
                continue
            if kind == 'list-table':
                i += 1
                while i < len(lines) and (not lines[i].strip() or lines[i].strip().startswith(':')):
                    i += 1
                rows = []
                while i < len(lines) and (not lines[i].strip() or lines[i].startswith(' ')):
                    match = re.match(r'^\s+\* -\s?(.*)$', lines[i])
                    if match:
                        rows.append([match.group(1)])
                    else:
                        match = re.match(r'^\s+-\s?(.*)$', lines[i])
                        if match and rows:
                            rows[-1].append(match.group(1))
                        elif rows and lines[i].strip():
                            rows[-1][-1] += ' ' + lines[i].strip()
                    i += 1
                if rows:
                    width = max(map(len, rows))
                    for row in rows:
                        row.extend([''] * (width - len(row)))
                    header = [inline(cell, page.stem, module).replace('|', '\\|') for cell in rows[0]]
                    result.append('| ' + ' | '.join(header) + ' |')
                    result.append('| ' + ' | '.join(['---'] * width) + ' |')
                    for row in rows[1:]:
                        cells = [inline(cell, page.stem, module).replace('|', '\\|') for cell in row]
                        result.append('| ' + ' | '.join(cells) + ' |')
                    result.append('')
                continue
            if kind in ADMONITIONS | {'sourcecode', 'code-block'}:
                if result and result[-1].strip():
                    result.append('')
                prefix = ' ' * max(4, indent) if indent else ''
                start = i + 1
                while start < len(lines) and not lines[start].strip():
                    start += 1
                end = start
                while end < len(lines):
                    current = lines[end]
                    if current.strip() and len(current) - len(current.lstrip()) <= indent:
                        break
                    end += 1
                body = lines[start:end]
                body_indent = min((len(x) - len(x.lstrip()) for x in body if x.strip()), default=indent + 3)
                if kind in {'sourcecode', 'code-block'}:
                    result.append(prefix + '```' + arg)
                    result.extend(prefix + x[body_indent:] if x.strip() else '' for x in body)
                    result.append(prefix + '```')
                else:
                    result.append(prefix + '!!! ' + ('info' if kind == 'seealso' else kind))
                    if arg:
                        result.append(prefix + '    ' + inline(arg, page.stem, module))
                    if body:
                        result.append('')
                        converted = convert(page, [x[body_indent:] if x.strip() else '' for x in body], module)
                        result.extend(prefix + '    ' + x if x else '' for x in converted.rstrip().splitlines())
                result.append('')
                i = end
                continue
        anchor = re.match(r'^\s*\.\. _([^:]+):\s*(.*)$', line)
        if anchor:
            name, url = anchor.groups()
            result.append(f'[{name}]: {url}' if url else f'<a id="{name}"></a>')
            i += 1
            continue
        if re.match(r'^\s*\.\. \|', line):
            i += 1
            continue
        overline_heading = (i + 2 < len(lines) and HEADING_RULE.fullmatch(stripped)
                            and lines[i + 1].strip() and HEADING_RULE.fullmatch(lines[i + 2].strip())
                            and lines[i + 2].strip()[0] == stripped[0])
        if overline_heading:
            result.append('# ' + inline(lines[i + 1].strip(), page.stem, module))
            i += 3
            continue
        underline_heading = (i + 1 < len(lines) and stripped
                             and HEADING_RULE.fullmatch(lines[i + 1].strip())
                             and len(lines[i + 1].strip()) >= len(stripped))
        if underline_heading:
            result.append('#' * HEADING[lines[i + 1].strip()[0]] + ' ' + inline(stripped, page.stem, module))
            i += 2
            continue
        if stripped.endswith('::') and not stripped.startswith('.. '):
            result.append(inline(line[:-1], page.stem, module))
            start = i + 1
            while start < len(lines) and not lines[start].strip():
                start += 1
            if start < len(lines):
                block_indent = len(lines[start]) - len(lines[start].lstrip())
                if block_indent > indent:
                    end = start
                    while end < len(lines) and (not lines[end].strip() or
                                                len(lines[end]) - len(lines[end].lstrip()) >= block_indent):
                        end += 1
                    result.append('')
                    result.append(' ' * indent + '```text')
                    result.extend(' ' * indent + x[block_indent:] if x.strip() else '' for x in lines[start:end])
                    result.append(' ' * indent + '```')
                    i = end
                    continue
            i += 1
            continue
        result.append(inline(re.sub(r'^(\s*)#\. ', r'\g<1>1. ', line), page.stem, module))
        i += 1
    rendered = '\n'.join(result).rstrip() + '\n'
    # RST accepts two-space list continuations; Python-Markdown requires four.
    normalized = []
    in_list = False
    in_fence = False
    for source_line in rendered.splitlines():
        normalized_line = source_line
        stripped = normalized_line.strip()
        if stripped.startswith('```'):
            in_fence = not in_fence
        if not in_fence:
            if re.match(r'^\s*(?:[*+-]|\d+\.) ', normalized_line):
                in_list = True
            elif stripped and not normalized_line.startswith((' ', '\t')):
                in_list = False
            elif in_list and re.match(r'^ {2,3}\S', normalized_line):
                normalized_line = ' ' * 4 + normalized_line.lstrip()
        normalized.append(normalized_line)
    rendered = '\n'.join(normalized) + '\n'
    if body_lines is None and page.stem == 'index':
        rendered = re.sub(r'\n## Indices and tables\n\n\* genindex\n\* modindex\n', '', rendered)
    return rendered


def split_usage_guide(path):
    """Move the six usage-guide sections into independently navigable pages."""
    names = ('introduction', 'schema', 'monitoring', 'gstat', 'firebird-log', 'trace')
    section_anchors = ('working-with-database-schema', 'working-with-monitoring-tables',
                       'processing-gstat-output', 'processing-firebrid-log',
                       'processing-firebrid-trace')
    lines = path.read_text().splitlines(keepends=True)
    boundaries = [0]
    for anchor in section_anchors:
        marker = f'<a id="{anchor}"></a>\n'
        boundaries.append(lines.index(marker))
    boundaries.append(len(lines))
    sections = [lines[start:end] for start, end in pairwise(boundaries)]
    destinations = {}
    for name, section in zip(names, sections, strict=True):
        for line in section:
            if match := re.match(r'<a id="([^"]+)"></a>', line):
                destinations[match.group(1)] = name
    directory = path.parent / 'usage-guide'
    directory.mkdir(exist_ok=True)
    for name, section in zip(names, sections, strict=True):
        in_fence = False
        for index, line in enumerate(section):
            if re.match(r'^\s*```', line):
                in_fence = not in_fence
            elif name != 'introduction' and not in_fence and (heading := re.match(r'^(#{2,6}) (.*)$', line)):
                level = heading.group(1)[1:]
                if name == 'schema' and heading.group(2) == 'Working with user privileges':
                    level = '##'
                section[index] = f'{level} {heading.group(2)}\n'
        body = ''.join(section)
        def local_link(match, current_name=name):
            anchor = match.group(1)
            target = destinations[anchor]
            href = f'#{anchor}' if target == current_name else f'{target}.md#{anchor}'
            return f'({href})'
        body = re.sub(r'\(usage-guide\.md#([\w-]+)\)', local_link, body)
        (directory / f'{name}.md').write_text(body.strip('\n') + '\n')
    path.unlink()


def main(source, target):
    global SOURCE, TARGET, PAGES  # noqa: PLW0603 - conversion state is shared by recursive page parsing
    SOURCE, TARGET = source.resolve(), target.resolve()
    PAGES = sorted(p for p in SOURCE.glob('*.txt')
                   if p.name not in {'changelog.txt', 'reference.txt', 'requirements.txt'})
    if not PAGES:
        raise SystemExit(f'No Sphinx .txt pages found in {SOURCE}')
    for page in PAGES:
        content = page.read_text()
        for anchor in re.findall(r'^\s*\.\. _([\w-]+):\s*$', content, re.M):
            ANCHORS[anchor] = page.stem
        LINKS[page.stem] = dict(re.findall(r'^\s*\.\. _([^:]+):\s+(https?://\S+)', content, re.M))
        HEADINGS[page.stem] = {re.sub(r'[^\w -]', '', title.lower()).replace(' ', '-'):
                               title for title in re.findall(r'^([^\n]+)\n[#*=+^"~-]{3,}\s*$', content, re.M)}
    TARGET.mkdir(parents=True, exist_ok=True)
    (TARGET / 'assets').mkdir(exist_ok=True)
    for page in PAGES:
        (TARGET / f'{page.stem}.md').write_text(convert(page))
    split_usage_guide(TARGET / 'usage-guide.md')
    index = TARGET / 'index.md'
    index.write_text(index.read_text().replace('(usage-guide.md)', '(usage-guide/introduction.md)')
                     .replace('(reference.md)', '(ref-schema.md)'))
    shutil.copyfile(SOURCE / '_static' / 'fb-favicon.png', TARGET / 'assets' / 'fb-favicon.png')
    changelog = TARGET / 'changelog.md'
    if not changelog.exists() and not changelog.is_symlink():
        changelog.symlink_to(os.path.relpath(ROOT / 'CHANGELOG.md', TARGET))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source', type=Path, required=True, help='Directory of Sphinx .txt pages')
    parser.add_argument('--target', type=Path, default=ROOT / 'zdoc' / 'docs',
                        help='Output directory for converted Markdown')
    args = parser.parse_args()
    main(args.source, args.target)
