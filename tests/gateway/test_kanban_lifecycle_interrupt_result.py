"""Lifecycle provenance survives only an exact, live managed Kanban result (#144397)."""

import json
from types import SimpleNamespace

import pytest

from gateway import session_kanban
from gateway.run import _INTERRUPT_REASON_GATEWAY_RESTART, _INTERRUPT_REASON_GATEWAY_SHUTDOWN
from hermes_cli.kanban_db_connect import connect_closing
from tests.gateway.test_kanban_result_exit import _bound_claim


@pytest.mark.parametrize("result,cause,exit_code", [
    ({"interrupted": True, "interrupt_message": _INTERRUPT_REASON_GATEWAY_RESTART}, "gateway_restart", 130),
    ({"interrupted": True, "interrupt_message": _INTERRUPT_REASON_GATEWAY_SHUTDOWN}, "gateway_shutdown", 130),
    ({"interrupted": True, "interrupt_message": "user requested stop"}, None, 130),
    ({"interrupted": True, "interrupt_message": []}, None, 130),
    ({"interrupted": True}, None, 130),
    ({"failed": True, "error": _INTERRUPT_REASON_GATEWAY_RESTART}, None, 1),
    ({"completed": True, "interrupt_message": _INTERRUPT_REASON_GATEWAY_RESTART}, None, 0),
    ({"interrupted": True, "interrupt_cause": "gateway_restart"}, None, 130),
])
def test_result_preserves_only_runtime_lifecycle_cause(tmp_path, monkeypatch, result, cause, exit_code):
    path, context = _bound_claim(tmp_path, monkeypatch)
    monkeypatch.setattr(session_kanban, "_run_task_turns", lambda *_args: result)
    frame = {"policy": {"kanban_json": json.dumps(context)}}

    session_kanban.run_worker_turns(SimpleNamespace(), frame, [])
    stored = session_kanban.worker_result(path, context)
    assert stored.get("interrupt_cause") == cause
    assert stored["exit_code"] == exit_code
    with connect_closing(path) as conn:
        row = conn.execute("SELECT status,consecutive_failures FROM tasks WHERE id=?", (context["task_id"],)).fetchone()
        assert row["status"] == "running" and row["consecutive_failures"] == 0


@pytest.mark.parametrize("invalid", ["claim", "run", "task", "binding", "settled", "superseded"])
def test_stale_or_foreign_writer_cannot_publish_lifecycle_cause(tmp_path, monkeypatch, invalid):
    path, context = _bound_claim(tmp_path, monkeypatch)
    original = dict(context)
    if invalid in ("claim", "run", "task"):
        key = {"claim": "claim_lock", "run": "run_id", "task": "task_id"}[invalid]
        context[key] = context[key] + 1 if invalid == "run" else "foreign"
    with connect_closing(path) as conn:
        if invalid == "binding":
            conn.execute("DELETE FROM task_events WHERE kind='worker_bound'")
        elif invalid == "settled":
            conn.execute("UPDATE tasks SET status='done',current_run_id=NULL WHERE id=?", (original["task_id"],))
        elif invalid == "superseded":
            conn.execute("UPDATE tasks SET claim_lock='successor' WHERE id=?", (original["task_id"],))
        conn.commit()
    monkeypatch.setattr(session_kanban, "_run_task_turns", lambda *_args: {
        "interrupted": True, "interrupt_message": _INTERRUPT_REASON_GATEWAY_RESTART,
    })
    session_kanban.run_worker_turns(SimpleNamespace(), {"policy": {"kanban_json": json.dumps(context)}}, [])
    assert "interrupt_cause" not in session_kanban.worker_result(path, original)
