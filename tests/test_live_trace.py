# SPDX-FileCopyrightText: 2026-present The Firebird Projects <www.firebirdsql.org>
#
# SPDX-License-Identifier: MIT
#
# PROGRAM/MODULE: firebird-lib
# FILE:           tests/test_live_trace.py
# DESCRIPTION:    Live trace service tests for firebird.lib.trace
# CREATED:        20.10.2026
#
# The contents of this file are subject to the MIT License
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
#
# Copyright (c) 2026 Firebird Project (www.firebirdsql.org)
# All Rights Reserved.
#
# Contributor(s): Pavel Císař (original code)

"""Exercise the trace parser with output from the selected Firebird engine."""

from contextlib import ExitStack
from dataclasses import dataclass
from datetime import datetime
from time import monotonic

import pytest

from firebird.base.types import STOP
from firebird.driver import TIMEOUT, connect, connect_server
from firebird.lib.trace import (
    EventAttach,
    EventCommit,
    EventCommitRetaining,
    EventDetach,
    EventFreeStatement,
    EventFunctionFinish,
    EventFunctionStart,
    EventPrepareStatement,
    EventProcedureFinish,
    EventProcedureStart,
    EventRollback,
    EventRollbackRetaining,
    EventServiceAttach,
    EventServiceStart,
    EventStatementFinish,
    EventStatementStart,
    EventSweepFinish,
    EventSweepProgress,
    EventSweepStart,
    EventTraceFinish,
    EventTraceInit,
    EventTransactionStart,
    EventTriggerFinish,
    EventTriggerStart,
    ParamSet,
    SQLInfo,
    TraceEvent,
    TraceParser,
)


@dataclass
class LiveTrace:
    """A trace reader plus the separate service connection used to stop it."""

    reader: object
    controller: object
    session_id: int
    stopped: bool = False

    def stop(self) -> None:
        if not self.stopped:
            self.controller.trace.stop(session_id=self.session_id)
            self.stopped = True

    def collect(self) -> list[str]:
        """Drain live output, stop the session, then drain its final records."""
        lines = []
        deadline = monotonic() + 15
        idle = 0
        while monotonic() < deadline:
            line = self.reader.readline_timed(1)
            if line is TIMEOUT or line is None:
                idle += 1
            else:
                lines.append(line)
                idle = 0
            if idle >= 2:
                break
        self.stop()
        idle = 0
        while monotonic() < deadline:
            line = self.reader.readline_timed(1)
            if line is TIMEOUT or line is None:
                idle += 1
            else:
                lines.append(line)
                idle = 0
            if idle >= 2:
                return lines
        pytest.fail(f'Timed out draining trace session {self.session_id}')


@pytest.fixture
def trace_session(fb_vars):
    """Start a user trace and guarantee its removal even after assertion failure."""
    sessions = []
    with ExitStack() as stack:
        def start(config: str) -> LiveTrace:
            reader = stack.enter_context(connect_server(fb_vars['host'], user=fb_vars['user'],
                                                        password=fb_vars['password']))
            controller = stack.enter_context(connect_server(fb_vars['host'], user=fb_vars['user'],
                                                            password=fb_vars['password']))
            session_id = reader.trace.start(config=config, name='firebird-lib-live-trace')
            result = LiveTrace(reader, controller, session_id)
            sessions.append(result)
            return result

        try:
            yield start
        finally:
            for session in reversed(sessions):
                session.stop()


@pytest.fixture
def executable_objects():
    """Create trace targets in the copied database and remove them afterward."""
    with connect('pytest', charset='UTF8') as connection:
        with connection.cursor() as cursor:
            cursor.execute('create table LIB_TRACE_EVENTS (ID integer)')
            cursor.execute('create trigger LIB_TRACE_BI for LIB_TRACE_EVENTS active '
                           'before insert as begin new.ID = new.ID + 1; end')
            cursor.execute('create procedure LIB_TRACE_PROC (X integer) returns (Y integer) '
                           'as begin Y = X + 1; end')
            cursor.execute('create function LIB_TRACE_FUNC (X integer) returns integer '
                           'as begin return X + 1; end')
        connection.commit()
    try:
        yield
    finally:
        with connect('pytest', charset='UTF8') as connection:
            with connection.cursor() as cursor:
                cursor.execute('drop function LIB_TRACE_FUNC')
                cursor.execute('drop procedure LIB_TRACE_PROC')
                cursor.execute('drop trigger LIB_TRACE_BI')
                cursor.execute('drop table LIB_TRACE_EVENTS')
            connection.commit()


def database_config(db_file, **options) -> str:
    """Configure trace events for the copied test database."""
    values = {'enabled': 'true', 'log_initfini': 'true', 'time_threshold': '0', **options}
    body = '\n'.join(f'    {key} = {value}' for key, value in values.items())
    filename = db_file.name.replace('-', '[-]').replace('.', '[.]')
    return f'database = %{filename}\n{{\n{body}\n}}\n'


def parse_both(lines: list[str]):
    """Check both parser entry points against the same complete service feed."""
    assert lines, 'Trace service returned no output'
    assert not lines[0].startswith('Error creating trace session'), ''.join(lines)
    try:
        parsed = list(TraceParser().parse(lines))
    except Exception as exc:
        pytest.fail(f'Live trace parse failed: {exc}\n{"".join(lines)}')
    parser = TraceParser()
    pushed = []
    for line in lines:
        pushed.extend(parser.push(line) or [])
    pushed.extend(parser.push(STOP) or [])
    assert pushed == parsed
    assert any(isinstance(obj, EventTraceFinish) for obj in parsed), 'Trace session did not finish'
    return parsed


def test_database_lifecycle(trace_session, db_file):
    trace = trace_session(database_config(db_file, log_connections='true',
                                          log_transactions='true'))
    with connect('pytest', charset='UTF8') as connection:
        with connection.cursor() as cursor:
            cursor.execute('select current_connection from rdb$database')
            assert cursor.fetchone()[0] > 0
        connection.commit()
    objects = parse_both(trace.collect())
    events = [obj for obj in objects if isinstance(obj, TraceEvent)]
    assert any(isinstance(obj, EventTraceInit) for obj in events)
    assert any(isinstance(obj, EventTraceFinish) for obj in events)
    assert any(isinstance(obj, EventAttach) and obj.database.endswith(db_file.name)
               for obj in events)
    assert any(isinstance(obj, EventDetach) for obj in events)
    assert any(isinstance(obj, EventTransactionStart) for obj in events)
    assert any(isinstance(obj, EventCommit) for obj in events)
    assert all(isinstance(obj.timestamp, datetime) for obj in events)


def test_transaction_outcomes(trace_session, db_file):
    trace = trace_session(database_config(db_file, log_transactions='true'))
    with connect('pytest', charset='UTF8') as connection:
        with connection.cursor() as cursor:
            for finish in (lambda: connection.commit(retaining=True),
                           lambda: connection.rollback(retaining=True),
                           connection.rollback, connection.commit):
                cursor.execute('select current_transaction from rdb$database')
                assert cursor.fetchone()[0] > 0
                finish()
    events = [obj for obj in parse_both(trace.collect()) if isinstance(obj, TraceEvent)]
    outcomes = [type(obj) for obj in events if isinstance(obj, (EventCommitRetaining,
                EventRollbackRetaining, EventRollback, EventCommit))]
    expected = iter((EventCommitRetaining, EventRollbackRetaining,
                     EventRollback, EventCommit))
    remaining = next(expected)
    for outcome in outcomes:
        if outcome is remaining:
            remaining = next(expected, None)
            if remaining is None:
                break
    assert remaining is None, outcomes
    assert any(isinstance(obj, EventTransactionStart) for obj in events)


@pytest.mark.parametrize('plan_mode', ['none', 'legacy', 'explained'])
@pytest.mark.parametrize('print_perf', [False, True], ids=['basic', 'detailed'])
def test_statement_variants(trace_session, db_file, plan_mode, print_perf):
    trace = trace_session(database_config(
        db_file, log_statement_prepare='true', log_statement_start='true',
        log_statement_finish='true', log_statement_free='true',
        print_plan=str(plan_mode != 'none').lower(),
        explain_plan=str(plan_mode == 'explained').lower(),
        print_perf=str(print_perf).lower(), max_sql_length='2048'))
    with connect('pytest', charset='UTF8') as connection:
        with connection.cursor() as cursor:
            cursor.execute('select country from country')
            cursor.fetchall()
            cursor.execute('select country from country where country = ?', ('USA',))
            cursor.fetchall()
        connection.commit()
    objects = parse_both(trace.collect())
    sql = {obj.sql_id: obj for obj in objects if isinstance(obj, SQLInfo)}
    params = {obj.par_id: obj for obj in objects if isinstance(obj, ParamSet)}
    starts = [obj for obj in objects if isinstance(obj, EventStatementStart)]
    finishes = [obj for obj in objects if isinstance(obj, EventStatementFinish)]
    plain = next(obj for obj in starts if sql[obj.sql_id].sql.lower() ==
                 'select country from country')
    bound = next(obj for obj in starts if sql[obj.sql_id].sql.lower() ==
                 'select country from country where country = ?')
    assert plain.param_id is None
    assert bound.param_id in params
    assert params[bound.param_id].params[0][1] == 'USA'
    assert any(obj.sql_id == plain.sql_id for obj in finishes)
    assert any(obj.sql_id == bound.sql_id and obj.param_id == bound.param_id
               for obj in finishes)
    for statement in (plain, bound):
        assert any(isinstance(obj, EventPrepareStatement) and obj.sql_id == statement.sql_id
                   for obj in objects)
        assert any(isinstance(obj, EventFreeStatement) and obj.sql_id == statement.sql_id
                   for obj in objects)
    for statement in (plain, bound):
        plan = sql[statement.sql_id].plan
        assert bool(plan) == (plan_mode != 'none')
        if plan_mode == 'explained':
            assert '\n' in plan, repr(plan)
    for event in finishes:
        assert event.run_time is None or event.run_time >= 0
        if event.sql_id in (plain.sql_id, bound.sql_id):
            assert event.access is None if not print_perf else event.access is not None


@pytest.mark.usefixtures('executable_objects')
def test_executable_objects(trace_session, db_file):
    trace = trace_session(database_config(
        db_file, log_trigger_start='true', log_trigger_finish='true',
        log_procedure_start='true', log_procedure_finish='true',
        log_function_start='true', log_function_finish='true'))
    with connect('pytest', charset='UTF8') as connection:
        with connection.cursor() as cursor:
            cursor.execute('insert into LIB_TRACE_EVENTS (ID) values (?)', (1,))
            cursor.execute('execute procedure LIB_TRACE_PROC (?)', (7,))
            assert cursor.fetchone()[0] == 8
            cursor.execute('select LIB_TRACE_FUNC (?) from rdb$database', (9,))
            assert cursor.fetchone()[0] == 10
        connection.commit()
    events = [obj for obj in parse_both(trace.collect()) if isinstance(obj, TraceEvent)]
    for event_type, name in ((EventTriggerStart, 'LIB_TRACE_BI'),
                             (EventTriggerFinish, 'LIB_TRACE_BI'),
                             (EventProcedureStart, 'LIB_TRACE_PROC'),
                             (EventProcedureFinish, 'LIB_TRACE_PROC'),
                             (EventFunctionStart, 'LIB_TRACE_FUNC'),
                             (EventFunctionFinish, 'LIB_TRACE_FUNC')):
        attribute = 'trigger' if 'Trigger' in event_type.__name__ else \
                    'procedure' if 'Procedure' in event_type.__name__ else 'function'
        assert any(isinstance(obj, event_type) and getattr(obj, attribute) == name
                   for obj in events), f'{event_type.__name__} for {name} was not emitted'


def test_sweep(trace_session, server_connection, db_file):
    trace = trace_session(database_config(db_file, log_sweep='true'))
    server_connection.database.sweep(database=db_file)
    events = [obj for obj in parse_both(trace.collect()) if isinstance(obj, TraceEvent)]
    assert any(isinstance(obj, EventSweepStart) for obj in events)
    assert any(isinstance(obj, EventSweepFinish) for obj in events)
    for obj in events:
        if isinstance(obj, EventSweepProgress):
            assert obj.run_time is None or obj.run_time >= 0


def test_service_activity(trace_session, fb_vars, db_file):
    config = """services
{
    enabled = true
    log_initfini = true
    log_services = true
    log_service_query = false
}
"""
    trace = trace_session(config)
    with connect_server(fb_vars['host'], user=fb_vars['user'],
                        password=fb_vars['password']) as service:
        service.database.get_statistics(database=db_file)
        assert service.readlines()
    objects = parse_both(trace.collect())
    assert any(isinstance(obj, EventServiceAttach) for obj in objects)
    assert any(isinstance(obj, EventServiceStart) and obj.action == 'Database Stats'
               for obj in objects)
