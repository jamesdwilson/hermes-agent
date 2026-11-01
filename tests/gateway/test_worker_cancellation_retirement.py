"""Direct cancellation/retirement contracts for the locally authored #145022 fix.

These exercise authority state, not WebSocket disconnect or server shutdown.
"""
import asyncio
import subprocess
import sys
import threading

import psutil
import pytest

from gateway.session_contract import SessionRef
from gateway.session_controls import AuthorityConnection
from hermes_cli.process_identity import worker_birth
from hermes_state_runtime import register_worker_execution
from tests.gateway.test_offloop_write_retirement_join import _owner


@pytest.fixture
def worker_context(tmp_path, monkeypatch):
    from gateway import session_worker

    store, authority, _ = _owner(tmp_path, monkeypatch)
    child = subprocess.Popen([sys.executable, '-c', 'input()'], stdin=subprocess.PIPE,
                             stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    try:
        authority.db.create_session('parent', source='cli')
        client = AuthorityConnection(authority, object(),
                                     {'user_id': 'human', 'capabilities': ['worker:adopt']})
        ref = SessionRef(authority.profile_id, 'parent')
        identity = dict(profile_id=authority.profile_id, session_id=ref.session_id,
                        execution_id='compute', generation=0, pid=child.pid,
                        birth=worker_birth(psutil.Process(child.pid)), secret='synthetic-claim-token')
        register_worker_execution(authority.db, epoch=authority.epoch, execution_id='compute',
                                  session_id=ref.session_id, generation=0, kind='compute',
                                  adoption_secret=session_worker._claim(client, ref, identity))
        yield authority, client, ref, identity
    finally:
        original_error = sys.exc_info()[1]
        try:
            try:
                child.stdin.close()
                try:
                    child.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    child.kill()  # Only the synthetic test child, never a gateway process.
                    child.wait(timeout=5)
            finally:
                store.close_all_db_handles()
        except Exception as cleanup_error:
            if original_error is None:
                raise
            original_error.add_note(f'Worker fixture cleanup also failed: {cleanup_error}')


async def _join_requests(request, authority):
    tasks = {request, *getattr(authority, '_mutation_tasks', ())}
    done, pending = await asyncio.wait(tasks, timeout=10)
    if pending:
        for task in pending:
            task.cancel()
        cancelled, pending = await asyncio.wait(pending, timeout=5)
        done.update(cancelled)
    for task in done:
        if not task.cancelled():
            task.exception()
    assert not pending, 'worker request ignored cancellation during bounded cleanup'


@pytest.mark.asyncio
@pytest.mark.parametrize('boundary', ['validation', 'committed-finish'])
async def test_retirement_retains_worker_request_until_its_effects_finish(
        worker_context, monkeypatch, boundary):
    from gateway import session_worker
    from gateway.run_runtime import _retire_profile_authority

    authority, client, ref, identity = worker_context
    entered, release, returned = threading.Event(), threading.Event(), threading.Event()
    symbol = {'validation': '_claim', 'committed-finish': 'mutate_worker_execution'}[boundary]
    real_operation = getattr(session_worker, symbol)
    wakes = []
    real_wake = authority.wake_after_worker

    def observe_wake(session_id):
        real_wake(session_id)
        wakes.append(session_id)

    def paused_operation(*args, **kwargs):
        try:
            result = real_operation(*args, **kwargs) if boundary == 'committed-finish' else None
            entered.set()
            if not release.wait(10):
                raise TimeoutError('test did not release worker boundary')
            return real_operation(*args, **kwargs) if boundary == 'validation' else result
        finally:
            returned.set()

    monkeypatch.setattr(session_worker, symbol, paused_operation)
    monkeypatch.setattr(authority, 'wake_after_worker', observe_wake)
    operation, payload = {
        'validation': ('session.prompt', {'system_prompt': 'synthetic context'}),
        'committed-finish': ('execution.finish', {}),
    }[boundary]
    request = asyncio.create_task(client.worker_persist(ref, identity | dict(
        epoch=authority.epoch, sequence=1, operation=operation, payload=payload)))
    try:
        assert await asyncio.to_thread(entered.wait, 10), 'worker never reached the boundary'
        if boundary == 'committed-finish':
            assert authority.db._read_one('SELECT COUNT(*) FROM worker_receipts')[0] == 1
            assert authority.db._read_one('SELECT status FROM worker_executions')[0] == 'terminal'
            request.cancel()
            with pytest.raises(asyncio.CancelledError):
                await request
        retired_early = await _retire_profile_authority(authority, timeout=0)
        release.set()
        await _join_requests(request, authority)
        assert await asyncio.to_thread(returned.wait, 10)
        expected_wakes = ['parent'] if boundary == 'committed-finish' else []
        assert (retired_early, wakes) == (False, expected_wakes), 'ownership/follow-up lost at boundary'
        assert authority.db._read_one('SELECT COUNT(*) FROM worker_receipts')[0] == 1
        assert await _retire_profile_authority(authority, timeout=0)
        assert authority.retired
    finally:
        original_error = sys.exc_info()[1]
        release.set()
        try:
            await _join_requests(request, authority)
            assert await asyncio.to_thread(returned.wait, 10)
        except Exception as cleanup_error:
            if original_error is None:
                raise
            original_error.add_note(f'Worker request cleanup also failed: {cleanup_error}')
