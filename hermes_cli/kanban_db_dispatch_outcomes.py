"""Dead-worker sweep outcomes and non-replaying lifecycle disposition."""
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class _DeadWorker:
    """How ``detect_crashed_workers`` should book one dead worker."""

    kind: str
    code: Optional[int]
    error_text: str
    event_kind: str
    event_payload: dict
    protocol_violation: bool = False
    rate_limited: bool = False
    terminal_provider: bool = False
    """``KANBAN_TERMINAL_PROVIDER_EXIT_CODE``: the provider rejected the worker's
    credential/model — trips the breaker on this first occurrence."""

    @property
    def run_outcome(self) -> str:
        # A rate-limited requeue is recorded as ``rate_limited`` so board history
        # doesn't show a phantom crash for a quota wall.
        return "rate_limited" if self.rate_limited else "crashed"


@dataclass
class _CrashSweep:
    """Everything ``detect_crashed_workers`` collects inside its reclaim txn."""

    crashed: list[str] = field(default_factory=list)
    rate_limited: list[str] = field(default_factory=list)
    # ``(task_id, pid, claimer, dead_worker)``: accounted after the txn via
    # ``_record_task_failure`` (needs its own write_txn).
    crash_details: list[tuple[str, int, str, _DeadWorker]] = field(default_factory=list)
    # Worker-exit observer payloads, fired only after every reclaim/accounting
    # txn has committed.
    exited_hook_payloads: list[dict] = field(default_factory=list)



def park_lifecycle_interruption(conn, task_id, worker_pid, claim_lock):
    """Called inside the crash sweep write transaction, after its liveness/owner fences."""
    from hermes_cli import kanban_db as kb
    from hermes_cli.kanban_owner_recovery import managed_lifecycle_interrupt_cause

    cause = managed_lifecycle_interrupt_cause(conn, task_id, claim_lock)
    if cause is None:
        return None
    run_id = kb._current_run_id(conn, task_id)
    resume_status = kb._retry_status_for_run(conn, task_id)
    reason = 'Gateway lifecycle interruption; inspect effects before explicitly unblocking.'
    changed = conn.execute(
        "UPDATE tasks SET status='blocked', claim_lock=NULL, claim_expires=NULL, "
        "worker_pid=NULL, worker_started_at=NULL, block_kind='needs_input' "
        "WHERE id=? AND status='running' AND current_run_id=? AND worker_pid=? AND claim_lock=?",
        (task_id, run_id, worker_pid, claim_lock),
    ).rowcount
    if changed != 1:
        return None
    payload = {'interrupt_cause': cause, 'exit_code': 130, 'worker_pid': worker_pid,
               'claim_lock': claim_lock, 'resume_status': resume_status, 'kind': 'needs_input',
               'reason': reason}
    kb._end_run(conn, task_id, outcome='interrupted', summary=reason, metadata=payload)
    # The existing blocked event is the sticky fence read by recompute_ready/unblock_task.
    kb._append_event(conn, task_id, 'blocked', payload, run_id=run_id)
    return {'task_id': task_id, 'run_id': run_id, 'worker_pid': worker_pid,
            'exit_kind': 'interrupted', 'exit_code': 130, 'outcome': 'interrupted',
            'retry_status': 'blocked'}
