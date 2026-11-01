"""Verified lifecycle interruption parks effects without charging a task failure (#144397)."""
import json

import pytest

from hermes_cli import kanban_db as kb
from hermes_cli import kanban_db_dispatch as kbd
from tests.hermes_cli import test_kanban_managed_worker_loss as loss_fixtures
from tests.hermes_cli.test_kanban_managed_worker_loss import NOW, _absent_pid

board = loss_fixtures.board


def _result(board, tid, run, cause, *, code=130):
    row = board.state(tid)
    pid = _absent_pid(base=3_810_000)
    board.event(tid, run, 'worker_bound', {'pid': pid, 'claim_lock': row['claim_lock']}, NOW - 60)
    payload = {'pid': pid, 'claim_lock': row['claim_lock'], 'exit_code': code, 'last_output': ''}
    if cause is not None:
        payload['interrupt_cause'] = cause
    board.event(tid, run, 'worker_result', payload, NOW - 30)
    board.conn.commit()


@pytest.mark.parametrize('cause', ['gateway_restart', 'gateway_shutdown'])
@pytest.mark.parametrize('expired', [False, True])
@pytest.mark.parametrize('resume_status', ['ready', 'review'])
def test_lifecycle_interruption_parks_once_without_failure_or_replay(board, cause, expired, resume_status):
    tid = 't_lifecycle'
    run = board.running(tid, claim_expires_in=-1800 if expired else 900)
    board.conn.execute(
        "UPDATE task_events SET payload=json_set(payload,'$.source_status',?) WHERE task_id=? AND kind='claimed'",
        (resume_status, tid),
    )
    _result(board, tid, run, cause)
    board.conn.execute('UPDATE tasks SET consecutive_failures=2,last_failure_error=? WHERE id=?', ('earlier failure', tid))
    board.conn.commit()

    for _ in range(2):
        result = board.tick()
        kb.recompute_ready(board.conn)
        row = board.state(tid)
        assert row['consecutive_failures'] == 2
        assert row['last_failure_error'] == 'earlier failure'
        assert row['status'] == 'blocked'
        assert (row['claim_lock'], row['worker_pid'], row['current_run_id']) == (None, None, None)
        assert not result.spawned and tid not in result.crashed
    assert board.run(run)['outcome'] == 'interrupted'
    assert json.loads(board.run(run)['metadata'])['interrupt_cause'] == cause
    assert board.conn.execute("SELECT count(*) FROM task_events WHERE task_id=? AND kind='blocked'", (tid,)).fetchone()[0] == 1
    assert kb.unblock_task(board.conn, tid)
    assert board.state(tid)['status'] == resume_status


@pytest.mark.parametrize('cause,code', [(None, 130), (None, 1), ('untrusted', 130)])
def test_user_stop_failure_and_unrecognized_cause_keep_failure_accounting(board, cause, code):
    tid = 't_failure'
    run = board.running(tid)
    _result(board, tid, run, cause, code=code)
    board.tick()
    assert board.state(tid)['consecutive_failures'] == 1
    assert board.run(run)['outcome'] == 'crashed'


@pytest.mark.parametrize('invalid', [
    'result_claim', 'bound_claim', 'result_pid', 'float_pid', 'missing_bound', 'other_run', 'other_task',
    'malformed_cause', 'malformed_result', 'wrong_exit', 'worker_loss', 'unknown_admission',
    'unreadable_admission', 'noninterrupted_admission',
])
def test_invalid_or_incomplete_lifecycle_evidence_does_not_exempt_failure(board, invalid):
    import sqlite3
    tid = 't_invalid'
    run = board.running(tid)
    _result(board, tid, run, 'gateway_restart')
    kind = 'worker_bound' if invalid == 'bound_claim' else 'worker_result'
    row = board.conn.execute('SELECT id,payload FROM task_events WHERE task_id=? AND kind=?', (tid, kind)).fetchone()
    payload = json.loads(row['payload'])
    payload.update({
        'result_claim': {'claim_lock': 'foreign:claim'},
        'bound_claim': {'claim_lock': 'foreign:claim'},
        'result_pid': {'pid': _absent_pid(base=3_820_000)},
        'float_pid': {'pid': float(payload['pid'])},
        'malformed_cause': {'interrupt_cause': ['gateway_restart']},
        'wrong_exit': {'exit_code': 1},
    }.get(invalid, {}))
    board.conn.execute('UPDATE task_events SET payload=? WHERE id=?', (json.dumps(payload), row['id']))
    mutations = {
        'malformed_result': ("UPDATE task_events SET payload='[]' WHERE id=?", (row['id'],)),
        'missing_bound': ("DELETE FROM task_events WHERE task_id=? AND kind='worker_bound'", (tid,)),
    }
    if invalid in mutations:
        board.conn.execute(*mutations[invalid])
    if invalid in ('other_run', 'other_task'):
        other = board.running('t_other')
        key, value = ('run_id', other) if invalid == 'other_run' else ('task_id', 't_other')
        board.conn.execute(f'UPDATE task_events SET {key}=? WHERE id=?', (value, row['id']))
    if invalid == 'worker_loss':
        board.event(tid, run, 'worker_lost', {'reason': 'managed_worker_failed'}, NOW - 10)
    board.conn.commit()
    if invalid in ('unknown_admission', 'unreadable_admission', 'noninterrupted_admission'):
        with sqlite3.connect(board.ledger) as owner:
            if invalid == 'unreadable_admission':
                owner.execute('DELETE FROM session_admissions')
            elif invalid == 'unknown_admission':
                owner.execute("UPDATE session_admissions SET status='unknown',outcome=NULL")
            else:
                owner.execute("UPDATE session_admissions SET outcome='failed'")
    board.tick()
    assert board.state(tid)['consecutive_failures'] == 1
    assert board.run(run)['outcome'] == 'crashed'


def test_unknown_admission_with_live_bound_interpreter_keeps_existing_fence(board):
    import os
    tid = 't_unknown_live'
    run = board.running(tid, admission=('unknown', None))
    _result(board, tid, run, 'gateway_restart')
    for kind in ('worker_bound', 'worker_result'):
        row = board.conn.execute('SELECT id,payload FROM task_events WHERE task_id=? AND kind=?', (tid, kind)).fetchone()
        payload = json.loads(row['payload'])
        payload['pid'] = os.getpid()
        board.conn.execute('UPDATE task_events SET payload=? WHERE id=?', (json.dumps(payload), row['id']))
    board.conn.commit()
    board.tick()
    assert board.state(tid)['status'] == 'running'
    assert board.state(tid)['consecutive_failures'] == 0
    assert board.state(tid)['current_run_id'] == run


def test_park_and_run_close_roll_back_together_when_event_write_fails(board, monkeypatch):
    tid = 't_atomic'
    run = board.running(tid)
    _result(board, tid, run, 'gateway_shutdown')
    original = kb._append_event

    def fail_block(conn, task_id, kind, *args, **kwargs):
        if kind == 'blocked':
            raise RuntimeError('synthetic event write failure')
        return original(conn, task_id, kind, *args, **kwargs)

    monkeypatch.setattr(kb, '_append_event', fail_block)
    with pytest.raises(RuntimeError, match='synthetic event write failure'):
        kbd.detect_crashed_workers(board.conn)
    assert board.state(tid)['status'] == 'running'
    assert board.state(tid)['current_run_id'] == run
    assert board.state(tid)['consecutive_failures'] == 0
    assert board.run(run)['ended_at'] is None
