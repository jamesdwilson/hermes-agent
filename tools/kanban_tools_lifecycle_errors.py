"""Explain rejected lifecycle writes without granting or changing run authority."""


def require_lifecycle_result(ok, kb, conn, task_id, fallback):
    """An ended run is historical evidence; the current owner is only a read snapshot."""
    if ok:
        return
    from tools.kanban_tools import _Reject, _worker_run_id

    run_id = _worker_run_id(task_id)
    run = kb.get_run(conn, run_id) if run_id is not None else None
    if run is None or run.task_id != task_id or run.ended_at is None:
        raise _Reject(fallback)
    task = kb.get_task(conn, task_id)
    if task is None or task.current_run_id == run_id:
        raise _Reject(fallback)
    current = f'current run is {task.current_run_id}' if task.current_run_id is not None else 'no current run'
    # Outcomes are kernel enum values; never reflect a run's free-text summary/error here.
    known = {'completed', 'blocked', 'crashed', 'timed_out', 'spawn_failed', 'gave_up',
             'reclaimed', 'review_requested', 'changes_requested', 'scheduled'}
    outcome = run.outcome if run.outcome in known else 'terminal'
    raise _Reject(f'Your run {run_id} ended ({outcome}) at {run.ended_at}; '
                  f'task {task_id}: {current} at this read. '
                  'Stop working on this task; do not retry lifecycle actions from this run.')
