"""Attachments recheck host lifetime and consent inside the real admission transaction."""
import asyncio
from datetime import UTC, datetime
import os
import threading

import pytest

from gateway.config import Platform
from gateway.session_identity import replace_source
from hermes_state_runtime import RuntimeStoreError, begin_runtime_epoch, list_session_admissions
from tests.gateway.test_session_attachment import OfflineDiscord, setup_bridge


def revoke(kind, runner, owner, ref, source, home):
    actions = {
        'consent': lambda: (home / 'config.yaml').write_text(
            'plugins:\n  entries:\n    talk-contract:\n      allow_gateway_injection: false\n'),
        'operator': lambda: os.environ.update(DISCORD_ALLOWED_USERS='someone-else'),
        'profile': lambda: setattr(owner, 'retiring', True),
        'adapter': lambda: runner.adapters.update({Platform.DISCORD: OfflineDiscord()}),
        'room': lambda: setattr(owner.sessions[ref.session_id], 'source', replace_source(source, chat_id='new-room')),
        'reset': lambda: runner.session_store.reset_session(runner._session_key_for_source(source)),
        'epoch': lambda: begin_runtime_epoch(owner.db, instance_id='successor'),
    }
    actions[kind]()


@pytest.mark.asyncio
@pytest.mark.parametrize('kind,reason', [('consent', 'permission_denied'), ('operator', 'permission_denied'),
    ('profile', 'runtime_draining'), ('adapter', 'stale_attachment'), ('room', 'stale_attachment'),
    ('reset', 'stale_attachment'), ('epoch', 'stale_epoch')])
async def test_attachment_rechecks_revocation_after_preflight_before_commit(tmp_path, monkeypatch, kind, reason):
    runner, owner, ref, _adapter, source, _ctx, (_invocation, attachment) = await setup_bridge(tmp_path, monkeypatch)
    monkeypatch.setattr(owner, '_schedule', lambda ref: None)
    entered, release = threading.Event(), threading.Event()
    write = owner._admit_native_write

    def blocked_write(**kwargs):
        entered.set()
        assert release.wait(10), 'test did not release the admission writer'
        return write(**kwargs)  # real codec, transaction, commit-time guard and SQLite

    monkeypatch.setattr(owner, '_admit_native_write', blocked_write)
    operation = asyncio.create_task(attachment.submit(attachment.scope, input_id='race',
        text='must not run after revocation', timestamp=datetime(2026, 1, 1, tzinfo=UTC)))
    try:
        assert await asyncio.to_thread(entered.wait, 10), 'admission never reached its writer'
        revoke(kind, runner, owner, ref, source, tmp_path)
        release.set()
        with pytest.raises(RuntimeStoreError, match=reason):
            await asyncio.wait_for(asyncio.shield(operation), 10)
        assert list_session_admissions(owner.db, session_id=ref.session_id, pending_only=False) == []
    finally:
        release.set()
        await asyncio.gather(operation, return_exceptions=True)
        runner.session_store.close_all_db_handles()


@pytest.mark.asyncio
@pytest.mark.parametrize('kind,reason', [('consent', 'permission_denied'), ('operator', 'permission_denied'),
    ('profile', 'stale_attachment'), ('adapter', 'stale_attachment'), ('room', 'stale_attachment'),
    ('reset', 'stale_attachment'), ('epoch', 'stale_epoch')])
async def test_revoked_attachment_cannot_read_results_or_interrupt(tmp_path, monkeypatch, kind, reason):
    runner, owner, ref, _adapter, source, _ctx, (_invocation, attachment) = await setup_bridge(tmp_path, monkeypatch)
    try:
        revoke(kind, runner, owner, ref, source, tmp_path)
        for operation in (lambda: attachment.snapshot(attachment.scope),
                          lambda: attachment.result(attachment.scope, 'unknown-admission'),
                          lambda: attachment.interrupt(attachment.scope, 1)):
            with pytest.raises(RuntimeStoreError, match=reason):
                await operation()
        assert list_session_admissions(owner.db, session_id=ref.session_id, pending_only=False) == []
    finally:
        runner.session_store.close_all_db_handles()
