"""An ended worker must understand a refused transition, not retry it (#98750)."""
import json

import pytest

from tests.tools import test_kanban_tools as fixtures
from tests.tools import test_kanban_complete_parents_tool as parent_fixtures

worker_env = fixtures.worker_env
reopened_parent = parent_fixtures.running_child_with_parent


@pytest.mark.parametrize('verb,args', [
    ('kanban_complete', {'summary': 'finished'}),
    ('kanban_block', {'reason': 'waiting'}),
    ('kanban_schedule', {'reason': 'waiting'}),
    ('kanban_request_review', {'summary': 'review this'}),
    ('kanban_request_changes', {'reason': 'needs changes'}),
])
def test_ended_worker_refusal_names_run_and_stop_action(worker_env, verb, args):
    from hermes_cli import kanban_db as kb
    from hermes_cli import kanban_db_connect as kbc
    from tools import kanban_tools  # register the production handlers
    from tools.registry import registry

    with kbc.connect() as conn:
        old = kb.get_task(conn, worker_env).current_run_id
        assert kb.block_task(conn, worker_env, reason='prior attempt finished', expected_run_id=old)
        assert kb.unblock_task(conn, worker_env)
        assert kb.claim_task(conn, worker_env)
        before = kb.get_task(conn, worker_env)
        successor = kb.get_run(conn, before.current_run_id)
    result = json.loads(registry.dispatch(verb, args))
    error = result['error']
    assert f'Your run {old} ended' in error
    assert 'blocked' in error
    assert f'current run is {successor.id}' in error
    assert 'Stop working on this task' in error
    with kbc.connect() as conn:
        assert kb.get_task(conn, worker_env) == before
        assert kb.get_run(conn, successor.id) == successor


@pytest.mark.parametrize('identity', ['missing', 'foreign-ended', 'current-open'])
def test_refusal_does_not_invent_ended_run_evidence(worker_env, monkeypatch, identity):
    from hermes_cli import kanban_db as kb
    from hermes_cli import kanban_db_connect as kbc
    from tools import kanban_tools  # register the production handlers
    from tools.registry import registry

    with kbc.connect() as conn:
        before = kb.get_task(conn, worker_env)
        run_id = before.current_run_id
        if identity == 'missing':
            run_id += 10000
        elif identity == 'foreign-ended':
            foreign = kb.create_task(conn, title='another card', assignee='test-worker')
            assert kb.claim_task(conn, foreign)
            run_id = kb.get_task(conn, foreign).current_run_id
            assert kb.block_task(conn, foreign, reason='done', expected_run_id=run_id)
    monkeypatch.setenv('HERMES_KANBAN_RUN_ID', str(run_id))
    result = json.loads(registry.dispatch('kanban_request_changes', {'reason': 'not reviewer'}))
    assert result.get('error')
    assert 'Your run' not in result['error']
    assert 'Stop working on this task' not in result['error']
    with kbc.connect() as conn:
        assert kb.get_task(conn, worker_env) == before


def test_parent_reopen_refusal_tells_invalidated_worker_to_stop(reopened_parent):
    from hermes_cli import kanban_db as kb
    from hermes_cli import kanban_db_connect as kbc
    from tools import kanban_tools  # register the production handlers
    from tools.registry import registry

    parent, child = reopened_parent
    with kbc.connect() as conn:
        old = kb.get_task(conn, child).current_run_id
        kb.invalidate_descendants_for_parent_reopen(conn, parent, author='operator')
        before = kb.get_task(conn, child)
        ended = kb.get_run(conn, old)
        assert ended.outcome == 'reclaimed'
        assert before.current_run_id is None
    result = json.loads(registry.dispatch('kanban_complete', {'summary': 'late completion'}))
    assert f'Your run {old} ended (reclaimed)' in result['error']
    assert 'no current run' in result['error']
    assert 'Stop working on this task' in result['error']
    with kbc.connect() as conn:
        assert kb.get_task(conn, child) == before
        assert kb.get_run(conn, old) == ended
