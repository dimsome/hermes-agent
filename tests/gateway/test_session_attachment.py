"""Offline Talk consumer contract over a real gateway authority and SQLite store.

Scripted transcripts establish routing, not speech recognition or live Discord acceptance.
"""
from dataclasses import replace
from datetime import UTC, datetime

import pytest

from gateway.config import GatewayConfig, Platform, PlatformConfig
from gateway.platforms.base import BasePlatformAdapter, SendResult
from gateway.platforms.event import MessageEvent
from gateway.run import GatewayRunner
from gateway.session_authority import initialize_session_authority
from hermes_cli.plugins import PluginContext, PluginManager, PluginManifest
from hermes_state_runtime import RuntimeStoreError, list_session_admissions


class OfflineDiscord(BasePlatformAdapter):
    """No bot client, microphone or network; use the host's real inbound boundary."""
    def __init__(self):
        super().__init__(PlatformConfig(enabled=True), Platform.DISCORD)
        self.sent = []

    async def connect(self, *, is_reconnect=False):
        return True

    async def disconnect(self):
        pass

    async def send(self, chat_id, content, reply_to=None, metadata=None):
        self.sent.append(content)
        return SendResult(success=True, message_id=str(len(self.sent)))

    async def get_chat_info(self, chat_id):
        return {'id': chat_id}


async def setup_bridge(tmp_path, monkeypatch):
    monkeypatch.setenv('HERMES_HOME', str(tmp_path))
    import hermes_state
    monkeypatch.setattr(hermes_state, 'DEFAULT_DB_PATH', tmp_path / 'state.db')
    monkeypatch.setenv('DISCORD_ALLOWED_USERS', 'operator')
    (tmp_path / 'config.yaml').write_text(
        'plugins:\n  entries:\n    talk-contract:\n      allow_gateway_injection: true\n')
    runner = GatewayRunner(GatewayConfig(sessions_dir=tmp_path / 'sessions'))
    runner._running = True
    adapter = OfflineDiscord()
    runner.adapters = {Platform.DISCORD: adapter}
    authority = await initialize_session_authority(runner, profile_id=str(tmp_path), instance_id='offline-host')
    runner._wire_adapter_handlers(adapter)
    source = adapter.build_source(chat_id='room', chat_type='group', user_id='operator', guild_id='guild')
    runner._canonicalize(source)
    ref = authority.register(source)
    manager = PluginManager()
    ctx = PluginContext(PluginManifest(name='talk-contract', key='talk-contract', source='user'), manager)
    from gateway.session_identity import identity_of
    from gateway.session_authorities import authority_for_home
    identity = identity_of(source)
    assert manager.home_path == identity.runtime_home == tmp_path
    assert authority_for_home(runner, tmp_path) is authority, (authority.db.db_path, tmp_path)
    captured = []

    def talk(raw, invocation=None):
        attachment = invocation.capture_session_attachment()
        captured.append((invocation, attachment))
        return 'captured'

    ctx.register_command('talk-contract', talk, invocation_context=True)
    monkeypatch.setattr('hermes_cli.plugins.get_plugin_command_handler',
                        lambda name: manager._plugin_commands.get(name, {}).get('handler'))
    event = MessageEvent(text='/talk-contract', source=source, message_id='join')
    handled, output, _ = await runner._hm_dispatch_plugin_command(event, source, 'talk-contract')
    assert handled and output == 'captured'
    return runner, authority, ref, adapter, source, ctx, captured[0]


@pytest.mark.asyncio
async def test_talk_capture_expires_but_attachment_queues_in_the_existing_session(tmp_path, monkeypatch):
    runner, owner, ref, _adapter, _source, _ctx, (invocation, attachment) = await setup_bridge(tmp_path, monkeypatch)
    monkeypatch.setattr(owner, '_schedule', lambda ref: None)
    try:
        with pytest.raises(RuntimeStoreError, match='invocation_expired'):
            invocation.capture_session_attachment()
        stamp = datetime(2026, 1, 1, tzinfo=UTC)
        receipt = await attachment.submit(attachment.scope, input_id='utterance-1', text='inspect the repo', timestamp=stamp)
        duplicate = await attachment.submit(attachment.scope, input_id='utterance-1', text='inspect the repo', timestamp=stamp)
        assert duplicate.admission_id == receipt.admission_id
        assert receipt.ref == ref and receipt.status == 'queued'
        rows = list_session_admissions(owner.db, session_id=ref.session_id)
        assert len(rows) == 1
        assert rows[0]['payload']['native_text_v1']['source']['user_id'] == 'operator'
        assert 'surface_v1' not in rows[0]['payload']  # no auxiliary voice persona/model route
        with pytest.raises(RuntimeStoreError, match='admission_conflict'):
            await attachment.submit(attachment.scope, input_id='utterance-1', text='different request', timestamp=stamp)
        attachment.close()
        with pytest.raises(RuntimeStoreError, match='attachment_closed'):
            await attachment.snapshot(attachment.scope)
    finally:
        runner.session_store.close_all_db_handles()


@pytest.mark.asyncio
@pytest.mark.parametrize('field,value', [('profile_id', 'foreign'), ('chat_id', 'other-room'),
                                       ('operator_user_id', 'stranger'), ('thread_id', 'other-thread'),
                                       ('guild_id', 'other-guild')])
async def test_talk_refuses_foreign_scope_before_admission(tmp_path, monkeypatch, field, value):
    runner, owner, ref, _adapter, _source, _ctx, (_invocation, attachment) = await setup_bridge(tmp_path, monkeypatch)
    monkeypatch.setattr(owner, '_schedule', lambda ref: None)
    try:
        foreign = replace(attachment.scope, **{field: value})
        with pytest.raises(RuntimeStoreError, match='attachment_scope_mismatch'):
            await attachment.submit(foreign, input_id='foreign', text='must not run', timestamp=datetime.now(UTC))
        assert list_session_admissions(owner.db, session_id=ref.session_id) == []
    finally:
        runner.session_store.close_all_db_handles()


@pytest.mark.asyncio
async def test_talk_cannot_use_transcript_as_an_approval_or_other_gateway_command(tmp_path, monkeypatch):
    runner, owner, ref, _adapter, _source, _ctx, (_invocation, attachment) = await setup_bridge(tmp_path, monkeypatch)
    monkeypatch.setattr(owner, '_schedule', lambda ref: None)
    try:
        for text in ('/approve always', '/deny', '/stop', '/new', '/model paid-provider'):
            with pytest.raises(RuntimeStoreError, match='invalid_params'):
                await attachment.submit(attachment.scope, input_id=text, text=text, timestamp=datetime.now(UTC))
        assert list_session_admissions(owner.db, session_id=ref.session_id) == []
    finally:
        runner.session_store.close_all_db_handles()


@pytest.mark.asyncio
async def test_talk_refuses_revoked_consent_and_authorization(tmp_path, monkeypatch):
    runner, owner, ref, _adapter, _source, ctx, (_invocation, attachment) = await setup_bridge(tmp_path, monkeypatch)
    monkeypatch.setattr(owner, '_schedule', lambda ref: None)
    try:
        monkeypatch.setenv('DISCORD_ALLOWED_USERS', 'someone-else')
        with pytest.raises(RuntimeStoreError, match='permission_denied'):
            await attachment.snapshot(attachment.scope)
        monkeypatch.setenv('DISCORD_ALLOWED_USERS', 'operator')
        monkeypatch.setattr(ctx, '_gateway_injection_allowed', lambda: False)
        with pytest.raises(RuntimeStoreError, match='permission_denied'):
            await attachment.submit(attachment.scope, input_id='revoked', text='no', timestamp=datetime.now(UTC))
        assert list_session_admissions(owner.db, session_id=ref.session_id) == []
    finally:
        runner.session_store.close_all_db_handles()
