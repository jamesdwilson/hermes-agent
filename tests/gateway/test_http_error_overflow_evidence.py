"""HTTP status and history count do not prove overflow (related to #134261)."""
import asyncio

import pytest

from gateway.run_turn import is_context_overflow_failure_result


@pytest.mark.parametrize('error,history_len,overflow', [
    ('HTTP 400: {"detail":"Bad Request"}', 10, False),
    ('HTTP 400: {"detail":"Bad Request"}', 50, False),
    ('HTTP 400: {"detail":"Bad Request"}', 51, False),
    ('HTTP 400: {"detail":"Bad Request"}', 100, False),
    ('HTTP 400: maximum context length exceeded', 10, True),
    ('HTTP 400: maximum context length exceeded', 100, True),
    ('HTTP 401: invalid authentication token', 100, False),
    ('HTTP 429: rate limit exceeded', 100, False),
])
def test_only_overflow_evidence_classifies_as_overflow(error, history_len, overflow):
    assert is_context_overflow_failure_result({'failed': True, 'error': error}, history_len) is overflow


def test_explicit_compression_and_success_controls():
    assert is_context_overflow_failure_result({'failed': True, 'compression_exhausted': True}, 1)
    assert not is_context_overflow_failure_result({'failed': False, 'error': 'HTTP 400'}, 100)


@pytest.mark.parametrize('error,history_len,persist', [
    ('HTTP 400: {"detail":"Bad Request"}', 10, True),
    ('HTTP 400: {"detail":"Bad Request"}', 100, True),
    ('HTTP 400: maximum context length exceeded', 100, False),
    ('HTTP 401: invalid authentication token', 100, True),
    ('HTTP 429: rate limit exceeded', 100, True),
])
def test_failed_result_preserves_user_unless_overflow(tmp_path, error, history_len, persist):
    from gateway.config import GatewayConfig, Platform
    from gateway.platforms.event import MessageEvent
    from gateway.run import GatewayRunner
    from gateway.session import SessionSource, SessionStore

    async def scenario():
        store = SessionStore(tmp_path / 'sessions', GatewayConfig())
        runner = object.__new__(GatewayRunner)
        runner.session_store = store
        runner._session_db = None
        async def refresh(*args):
            return None
        runner._refresh_agent_cache_message_count = refresh
        source = SessionSource(platform=Platform.TELEGRAM, chat_id='synthetic', user_id='synthetic')
        entry = store.get_or_create_session(source)
        sid = entry.session_id
        db = store._db_for_session_id(sid)
        try:
            prepared = runner._PreparedTurn([{'role': 'user', 'content': 'fixture'}] * history_len,
                '', 'synthetic inbound', None, None, None, sid, 'synthetic-owner')
            result = {'failed': True, 'error': error, 'messages': [], 'agent_persisted': False}
            failed, hidden, overflow = runner._hmwa_classify_turn_failure(result, prepared.history, entry)
            await runner._hmwa_persist_turn_transcript(
                event=MessageEvent(text='synthetic inbound', source=source, message_id='synthetic-id'),
                source=source, session_entry=entry, session_key=entry.session_key,
                agent_result=result, agent_messages=[], prepared=prepared, response='',
                agent_failed_early=failed, hidden_reasoning_incomplete=hidden,
                is_context_overflow_failure=overflow)
            rows = db.get_messages(sid)
            assert any(row['role'] == 'user' and row['content'] == 'synthetic inbound' for row in rows) is persist
        finally:
            db.close()
    asyncio.run(scenario())


@pytest.mark.parametrize('error,overflow', [
    ('HTTP 400: {"detail":"Bad Request"}', False),
    ('HTTP 400: maximum context length exceeded', True),
])
def test_reply_requires_overflow_evidence(error, overflow):
    from gateway.run import _normalize_empty_agent_response, _context_overflow_reply
    reply = _normalize_empty_agent_response({'failed': True, 'error': error}, '', history_len=100)
    assert (reply == _context_overflow_reply()) is overflow


@pytest.mark.parametrize('status,error,history_len,overflow', [
    (400, 'Bad Request', 10, False),
    (400, 'Bad Request', 100, False),
    (500, 'Internal Server Error', 100, False),
    (400, 'maximum context length exceeded', 100, True),
    (400, 'maximum context length exceeded', 10, True),
])
def test_escaped_error_preserves_input_without_overflow_evidence(tmp_path, status, error, history_len, overflow):
    from gateway.config import GatewayConfig, Platform
    from gateway.platforms.event import MessageEvent
    from gateway.run import GatewayRunner, _context_overflow_reply
    from gateway.session import SessionSource, SessionStore

    async def scenario():
        store = SessionStore(tmp_path / 'sessions', GatewayConfig())
        runner = object.__new__(GatewayRunner)
        runner.session_store = store
        async def stop_typing(*args):
            return None
        runner._hmwa_stop_typing_for_turn = stop_typing
        source = SessionSource(platform=Platform.TELEGRAM, chat_id='synthetic', user_id='synthetic')
        entry = store.get_or_create_session(source)
        db = store._db_for_session_id(entry.session_id)
        prepared = runner._PreparedTurn([{'role': 'user', 'content': 'fixture'}] * history_len,
            '', 'synthetic inbound', None, None, None, entry.session_id, 'synthetic-owner')
        exc = RuntimeError(error)
        exc.status_code = status
        try:
            reply = await runner._hmwa_agent_error_reply(exc,
                MessageEvent(text='synthetic inbound', source=source, message_id='synthetic-id'),
                source, entry, entry.session_key, prepared)
            assert (reply == _context_overflow_reply()) is overflow
            rows = db.get_messages(entry.session_id)
            assert any(row['role'] == 'user' for row in rows) is (not overflow)
        finally:
            db.close()
    asyncio.run(scenario())
