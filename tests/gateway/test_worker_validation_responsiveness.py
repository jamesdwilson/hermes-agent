"""Worker validation must not freeze the owner loop or outlive its authority (#145022)."""
import asyncio
from contextlib import closing
import os
import sqlite3
import subprocess
import sys
import threading
import time
from types import SimpleNamespace

import psutil
import pytest

from gateway.session_contract import SessionRef
from hermes_cli.process_identity import worker_birth
from hermes_state import SessionDB
from hermes_state_runtime import RuntimeStoreError, begin_runtime_epoch, register_worker_execution


@pytest.mark.asyncio
@pytest.mark.parametrize('operation', ['session.prompt', 'child.reserve', 'delegation.dispatch'])
@pytest.mark.parametrize('retire', [False, True])
async def test_blocked_validation_keeps_loop_responsive_and_rechecks_retirement(tmp_path, monkeypatch, operation, retire):
    from gateway import session_worker as worker
    from tools.async_delegation import _initialize_schema

    db = SessionDB(tmp_path / 'state.db')
    process = await asyncio.to_thread(subprocess.Popen,
        [sys.executable, '-c', 'import sys; sys.stdin.read()'], stdin=subprocess.PIPE,
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    release = threading.Event()
    try:
        db.create_session('parent', source='cli')
        epoch = begin_runtime_epoch(db, instance_id='owner')
        with closing(sqlite3.connect(db.db_path)) as conn:
            _initialize_schema(conn)
        authority = SimpleNamespace(db=db, epoch=epoch, profile_id='profile', retired=False,
                                    _require_admission_open=lambda: None)
        connection = SimpleNamespace(authority=authority, actor=SimpleNamespace(
            profile_id='profile', subject='human', capabilities={'worker:adopt'}))
        ref = SessionRef('profile', 'parent')
        scope = dict(profile_id='profile', session_id='parent', execution_id='worker', generation=0,
                     pid=process.pid, birth=worker_birth(psutil.Process(process.pid)), secret='private-worker-secret')
        claim = worker._claim(connection, ref, scope)
        register_worker_execution(db, epoch=epoch, execution_id='worker', session_id='parent', generation=0,
                                  kind='compute', adoption_secret=claim)
        monkeypatch.setattr('gateway.session_worker_child.delegation_limits', lambda *args: (10, 1))
        payloads = {
            'session.prompt': {'system_prompt': 'persisted'},
            'child.reserve': {'secret': 'private-child-secret'},
            'delegation.dispatch': dict(delegation_id='deleg_owned', task={'goal': 'inert'}, dispatched_at=1,
                parent_session_id='parent', session_key='', origin_ui_session_id='', origin_session_id=''),
        }
        params = scope | dict(epoch=epoch, sequence=1, operation=operation, payload=payloads[operation])
        original = worker._claim
        loop = asyncio.get_running_loop()
        latency = []

        def probe(started):
            latency.append(time.monotonic() - started)
            authority.retired = retire
            release.set()

        def blocked_claim(*args):
            loop.call_soon_threadsafe(probe, time.monotonic())
            # Emergency release makes the baseline failure bounded, without a sleeping test.
            release.wait(timeout=5)
            return original(*args)

        monkeypatch.setattr(worker, '_claim', blocked_claim)
        if retire:
            with pytest.raises(RuntimeStoreError, match='runtime_retired'):
                await worker.worker_request(connection, ref, params, operation='persist')
            assert db._read_one('SELECT COUNT(*) FROM worker_receipts')[0] == 0
            assert db._read_one('SELECT COUNT(*) FROM worker_executions')[0] == 1
        else:
            await worker.worker_request(connection, ref, params, operation='persist')
            assert db._read_one('SELECT COUNT(*) FROM worker_receipts')[0] == 1
        assert latency and latency[0] < 2, 'worker verification starved the owner loop'
    finally:
        release.set()
        process.stdin.close()
        process.wait(timeout=5)
        db.close()
