# SPDX-FileCopyrightText: 2026-present The Firebird Projects <www.firebirdsql.org>
#
# SPDX-License-Identifier: MIT
#
# PROGRAM/MODULE: firebird-lib
# FILE:           tests/test_monitor_coverage.py
# DESCRIPTION:    Monitoring table and column coverage against the running engine

"""Keep the public monitor API in step with Firebird's MON$ catalog."""

from inspect import getattr_static

from firebird.lib.monitor import (
    AttachmentInfo,
    CallStackInfo,
    CompiledStatementInfo,
    ContextVariableInfo,
    DatabaseInfo,
    IOStatsInfo,
    Monitor,
    StatementInfo,
    TableStatsInfo,
    TransactionInfo,
)


def _columns(**accessors):
    """Expand catalog column suffixes to their public accessors."""
    return {f'MON${column}': accessor for column, accessor in accessors.items()}


# Each table maps to (Monitor property, info class, column -> public accessor).
# Statistics tables deliberately share IOStatsInfo; TABLE_STATS joins RECORD_STATS.
COVERAGE = {
    'MON$DATABASE': ('db', DatabaseInfo, _columns(
        DATABASE_NAME='name', PAGE_SIZE='page_size', ODS_MAJOR='ods', ODS_MINOR='ods',
        OLDEST_TRANSACTION='oit', OLDEST_ACTIVE='oat', OLDEST_SNAPSHOT='ost',
        NEXT_TRANSACTION='next_transaction', PAGE_BUFFERS='cache_size', SQL_DIALECT='sql_dialect',
        SHUTDOWN_MODE='shutdown_mode', SWEEP_INTERVAL='sweep_interval', READ_ONLY='read_only',
        FORCED_WRITES='forced_writes', RESERVE_SPACE='reserve_space', CREATION_DATE='created',
        PAGES='pages', STAT_ID='stat_id', BACKUP_STATE='backup_state', CRYPT_PAGE='crypt_page',
        OWNER='owner', SEC_DATABASE='security', CRYPT_STATE='crypt_state', GUID='guid',
        FILE_ID='file_id', NEXT_ATTACHMENT='next_attachment', NEXT_STATEMENT='next_statement',
        REPLICA_MODE='replica_mode')),
    'MON$ATTACHMENTS': ('attachments', AttachmentInfo, _columns(
        ATTACHMENT_ID='id', SERVER_PID='server_pid', STATE='state', ATTACHMENT_NAME='name',
        USER='user', ROLE='role', REMOTE_PROTOCOL='remote_protocol', REMOTE_ADDRESS='remote_address',
        REMOTE_PID='remote_pid', CHARACTER_SET_ID='character_set', TIMESTAMP='timestamp',
        GARBAGE_COLLECTION='is_gc_allowed', REMOTE_PROCESS='remote_process', STAT_ID='stat_id',
        CLIENT_VERSION='client_version', REMOTE_VERSION='remote_version', REMOTE_HOST='remote_host',
        REMOTE_OS_USER='remote_os_user', AUTH_METHOD='auth_method', SYSTEM_FLAG='system',
        IDLE_TIMEOUT='idle_timeout', IDLE_TIMER='idle_timer', STATEMENT_TIMEOUT='statement_timeout',
        WIRE_COMPRESSED='wire_compressed', WIRE_ENCRYPTED='wire_encrypted',
        WIRE_CRYPT_PLUGIN='wire_crypt_plugin', SESSION_TIMEZONE='session_timezone',
        PARALLEL_WORKERS='parallel_workers')),
    'MON$TRANSACTIONS': ('transactions', TransactionInfo, _columns(
        TRANSACTION_ID='id', ATTACHMENT_ID='attachment', STATE='state', TIMESTAMP='timestamp',
        TOP_TRANSACTION='top', OLDEST_TRANSACTION='oldest', OLDEST_ACTIVE='oldest_active',
        ISOLATION_MODE='isolation_mode', LOCK_TIMEOUT='lock_timeout', READ_ONLY='is_readonly',
        AUTO_COMMIT='is_autocommit', AUTO_UNDO='is_autoundo', STAT_ID='stat_id')),
    'MON$STATEMENTS': ('statements', StatementInfo, _columns(
        STATEMENT_ID='id', ATTACHMENT_ID='attachment', TRANSACTION_ID='transaction', STATE='state',
        TIMESTAMP='timestamp', SQL_TEXT='sql', STAT_ID='stat_id', EXPLAINED_PLAN='plan',
        STATEMENT_TIMEOUT='timeout', STATEMENT_TIMER='timer',
        COMPILED_STATEMENT_ID='compiled_statement')),
    'MON$CALL_STACK': ('callstack', CallStackInfo, _columns(
        CALL_ID='id', STATEMENT_ID='statement', CALLER_ID='caller', OBJECT_NAME='object_name',
        OBJECT_TYPE='object_type', TIMESTAMP='timestamp', SOURCE_LINE='line', SOURCE_COLUMN='column',
        STAT_ID='stat_id', PACKAGE_NAME='package_name',
        COMPILED_STATEMENT_ID='compiled_statement')),
    'MON$CONTEXT_VARIABLES': ('variables', ContextVariableInfo, _columns(
        ATTACHMENT_ID='attachment', TRANSACTION_ID='transaction', VARIABLE_NAME='name',
        VARIABLE_VALUE='value')),
    'MON$IO_STATS': ('iostats', IOStatsInfo, _columns(
        STAT_ID='stat_id', STAT_GROUP='group', PAGE_READS='reads', PAGE_WRITES='writes',
        PAGE_FETCHES='fetches', PAGE_MARKS='marks')),
    'MON$MEMORY_USAGE': ('iostats', IOStatsInfo, _columns(
        STAT_ID='stat_id', STAT_GROUP='group', MEMORY_USED='memory_used',
        MEMORY_ALLOCATED='memory_allocated', MAX_MEMORY_USED='max_memory_used',
        MAX_MEMORY_ALLOCATED='max_memory_allocated')),
    'MON$RECORD_STATS': ('iostats', IOStatsInfo, _columns(
        STAT_ID='stat_id', STAT_GROUP='group', RECORD_SEQ_READS='seq_reads',
        RECORD_IDX_READS='idx_reads', RECORD_INSERTS='inserts', RECORD_UPDATES='updates',
        RECORD_DELETES='deletes', RECORD_BACKOUTS='backouts', RECORD_PURGES='purges',
        RECORD_EXPUNGES='expunges', RECORD_LOCKS='locks', RECORD_WAITS='waits',
        RECORD_CONFLICTS='conflicts', BACKVERSION_READS='backversion_reads',
        FRAGMENT_READS='fragment_reads', RECORD_RPT_READS='repeated_reads',
        RECORD_IMGC='intermediate_gc')),
    'MON$TABLE_STATS': ('tablestats', TableStatsInfo, _columns(
        STAT_ID='stat_id', STAT_GROUP='group', TABLE_NAME='table_name',
        RECORD_STAT_ID='row_stat_id')),
    'MON$COMPILED_STATEMENTS': ('compiled_statements', CompiledStatementInfo, _columns(
        COMPILED_STATEMENT_ID='id', SQL_TEXT='sql', EXPLAINED_PLAN='plan', OBJECT_NAME='object_name',
        OBJECT_TYPE='object_type', PACKAGE_NAME='package_name', STAT_ID='stat_id')),
}


def test_monitor_catalog_coverage(db_connection, fb_vars):
    """Every live MON$ table and column must reach a documented public accessor."""
    catalog = {}
    with db_connection.cursor() as cursor:
        cursor.execute("""select trim(r.rdb$relation_name), trim(f.rdb$field_name)
                          from rdb$relations r join rdb$relation_fields f
                            on f.rdb$relation_name = r.rdb$relation_name
                          where r.rdb$relation_name starting with 'MON$'""")
        for table, column in cursor:
            catalog.setdefault(table, set()).add(column)

    expected_tables = set(COVERAGE)
    if fb_vars['version'].major < 5:
        expected_tables.remove('MON$COMPILED_STATEMENTS')
    assert set(catalog) == expected_tables, f'MON$ tables differ: {set(catalog) ^ expected_tables}'

    with db_connection.cursor() as cursor:
        cursor.execute("select RDB$SET_CONTEXT('USER_SESSION', 'MON_COVERAGE', '1') from rdb$database")
        cursor.fetchone()

    with Monitor(db_connection) as monitor:
        samples = {}
        projections = {}
        for collection in dict.fromkeys(['db', *(COVERAGE[table][0] for table in catalog)]):
            items = getattr(monitor, collection)
            projections[collection] = {column[0] for column in monitor._ic.description}
            samples[collection] = items if collection == 'db' else next(iter(items), None)

        for table, columns in catalog.items():
            collection, info_class, accessors = COVERAGE[table]
            missing = columns - accessors.keys()
            assert not missing, f'{table} lacks public accessors: {missing}'
            missing = columns - projections[collection]
            assert not missing, f'{table} columns not selected: {missing}'
            item = samples[collection]
            if collection != 'callstack':
                assert item is not None, f'{table} has no sample row'
            if item is not None:
                assert isinstance(item, info_class)
                missing = columns - item._attributes.keys()
                assert not missing, f'{table} columns not retained: {missing}'
            for column in columns:
                accessor = accessors[column]
                descriptor = getattr_static(info_class, accessor, None)
                assert isinstance(descriptor, property) or callable(descriptor), (table, column, accessor)
                if item is not None:
                    value = getattr(item, accessor)
                    if callable(value):
                        value()


def test_callstack_compiled_statement_absent(db_connection):
    """Older servers and null rows have no compiled statement reference."""
    with Monitor(db_connection) as monitor:
        call = CallStackInfo(monitor, {})
        assert call.compiled_statement is None
