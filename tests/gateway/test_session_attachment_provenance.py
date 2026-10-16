"""Attached transcripts keep native homes and connector ownership through FIFO claim."""
from datetime import UTC, datetime
from pathlib import Path

import pytest

from gateway.config import Platform
from gateway.platforms.event import MessageEvent
from gateway.profile_routing import ProfileRoute
from gateway.run import _profile_runtime_scope
from gateway.run_runtime import initialize_gateway_runtime
from gateway.runtime_ownership import process_ownership
from gateway.session_envelope import check_native_route
from gateway.session_identity import identity_of
from gateway.session_ingress_context import native_callback
from hermes_cli.plugins import PluginContext, PluginManager, PluginManifest
from hermes_state_runtime import RuntimeStoreError, list_session_admissions
from tests.gateway.test_retry_native_provenance import _reserve_homes, _runner
from tests.gateway.test_session_attachment import OfflineDiscord, setup_bridge


STAMP = datetime(2026, 1, 1, tzinfo=UTC)


@pytest.mark.asyncio
@pytest.mark.parametrize('transport,runtime', [
    ('default', 'default'), ('default', 'beta'), ('beta', 'default'),
])
async def test_multiplex_attachment_retains_native_transport_and_runtime_homes(
        tmp_path, monkeypatch, transport, runtime):
    root, homes = _reserve_homes(tmp_path, monkeypatch)
    by_name = dict(homes)
    for name, home in homes:
        (home / 'config.yaml').write_text(
            'plugins:\n  entries:\n    talk-contract:\n      allow_gateway_injection: true\n')
        allowed = 'operator' if name == transport else 'runtime-only'
        (home / '.env').write_text(f'DISCORD_ALLOWED_USERS={allowed}\n')
    runner = _runner(root, homes)
    runner._running = True
    runner._primary_profile_name = 'default'
    if transport != runtime:
        runner.config.profile_routes = [ProfileRoute(
            name='attachment-route', platform='discord', profile=runtime, chat_id='room',
            bot_profile=None if transport == 'default' else transport)]
    process_ownership.reserve([home for _, home in homes])
    try:
        await initialize_gateway_runtime(runner)
        for owner in runner.session_authorities:
            monkeypatch.setattr(owner, '_schedule', lambda ref: None)
        primary = OfflineDiscord()
        primary.config.token = 'primary-connector-fixture'
        primary.gateway_runner = runner
        runner.adapters = {Platform.DISCORD: primary}
        runner._wire_adapter_handlers(primary)
        adapter = primary
        if transport != 'default':
            adapter = OfflineDiscord()
            adapter.config.token = 'secondary-connector-fixture'
            adapter.gateway_runner = runner
            runner._profile_adapters = {transport: {Platform.DISCORD: adapter}}
            runner._configure_profile_adapter(adapter, transport, Platform.DISCORD)
        source = adapter.build_source(
            chat_id='room', chat_type='group', user_id='operator', guild_id='guild')
        identity = runner._canonicalize(source)
        assert identity.authorization_home == by_name[transport]
        assert identity.runtime_home == by_name[runtime]
        owner = runner.session_authorities.for_home(identity.runtime_home)
        assert Path(owner.db.db_path).parent == by_name[runtime]
        captured = []
        with _profile_runtime_scope(identity.runtime_home):
            manager = PluginManager()
            ctx = PluginContext(PluginManifest(
                name='talk-contract', key='talk-contract', source='user'), manager)

            def join(raw, invocation=None):
                captured.append(invocation.capture_session_attachment())
                return 'captured'

            ctx.register_command('attachment-mux', join, invocation_context=True)
            monkeypatch.setattr('hermes_cli.plugins.get_plugin_command_handler',
                                lambda name: manager._plugin_commands.get(name, {}).get('handler'))
            ordinary = MessageEvent(text='ordinary multiplex input', source=source,
                                    message_id='ordinary-mux', timestamp=STAMP)
            profile = None if transport == 'default' else transport
            with native_callback(runner, ordinary, by_name[transport], profile):
                admitted = await owner.admit_native(ordinary)
            command = MessageEvent(text='/attachment-mux', source=source)
            with native_callback(runner, command, by_name[transport], profile):
                handled, output, _ = await runner._hm_dispatch_plugin_command(command, source, 'attachment-mux')
        assert handled and output == 'captured' and len(captured) == 1
        attachment = captured[0]
        assert attachment.scope.session_id == admitted.ref.session_id
        assert identity_of(attachment._check(attachment.scope)) == identity
        # This runs after dispatch/callback exit, not in the original message's context.
        receipt = await attachment.submit(attachment.scope, input_id='mux-audio',
                                          text='attached multiplex input', timestamp=STAMP)
        assert receipt.ref == admitted.ref and receipt.status == 'queued'
        rows = list_session_admissions(owner.db, session_id=receipt.ref.session_id)
        text_row, voice_row = rows
        provenance = voice_row['payload']['native_text_v1']['provenance']
        assert provenance == text_row['payload']['native_text_v1']['provenance']
        assert provenance['transport_home'] == str(by_name[transport])
        assert provenance['runtime_home'] == str(by_name[runtime])
        assert provenance['transport_profile'] == profile
        assert voice_row['payload']['native_text_v1']['event']['allow_gateway_control'] is False
        # Exercise the actual owner preflight and claim, not just envelope inspection.
        await owner.cancel_queued(attachment._actor, receipt.ref, text_row['admission_id'])
        claimed, first = await owner._claim_next(receipt.ref, owner.sessions[receipt.ref.session_id])
        assert claimed['admission_id'] == first['admission_id'] == receipt.admission_id
        assert claimed['status'] == 'started'
    finally:
        runner.session_store.close_all_db_handles()
        for owner in getattr(runner, 'session_authorities', []):
            owner.db.close()
        for _, home in homes:
            process_ownership.release(home)


@pytest.mark.asyncio
async def test_queued_attachment_refuses_replacement_connector_at_real_authority_claim(tmp_path, monkeypatch):
    home = tmp_path / '.hermes'
    home.mkdir()
    runner, owner, ref, adapter, source, _, (_, attachment) = await setup_bridge(home, monkeypatch)
    runner._wire_adapter_handlers(adapter)  # authority exists, as at production startup
    monkeypatch.setattr(owner, '_schedule', lambda ref: None)
    adapter.config.token = 'original-connector-fixture'
    try:
        ordinary = MessageEvent(text='ordinary input', source=source, message_id='ordinary-owned', timestamp=STAMP)
        with native_callback(runner, ordinary, home):
            await owner.admit_native(ordinary)
        voice = await attachment.submit(attachment.scope, input_id='owned-audio', text='attached input', timestamp=STAMP)
        text_row, voice_row = list_session_admissions(owner.db, session_id=ref.session_id)
        await owner.cancel_queued(attachment._actor, ref, text_row['admission_id'])
        replacement = OfflineDiscord()
        replacement.config.token = 'replacement-connector-fixture'
        runner.adapters[source.platform] = replacement
        runner._wire_adapter_handlers(replacement)
        available = replacement.build_source(
            chat_id='room', chat_type='group', user_id='operator', guild_id='guild')
        runner._canonicalize(available)
        with pytest.raises(RuntimeStoreError, match='profile_mismatch'):
            await check_native_route(runner, text_row['payload'], ref.session_id, available, replacement)
        with pytest.raises(RuntimeStoreError, match='stale_attachment'):
            await attachment.snapshot(attachment.scope)
        before = list_session_admissions(owner.db, session_id=ref.session_id)
        with pytest.raises(RuntimeStoreError, match='profile_mismatch'):
            await owner._claim_next(ref, owner.sessions[ref.session_id])
        assert list_session_admissions(owner.db, session_id=ref.session_id) == before
        assert voice_row['payload']['native_text_v1']['provenance'] == text_row['payload']['native_text_v1']['provenance']
        # Refusal leaves accepted work queued; the original connector may still claim it.
        runner.adapters[source.platform] = adapter
        claimed, first = await owner._claim_next(ref, owner.sessions[ref.session_id])
        assert claimed['admission_id'] == first['admission_id'] == voice.admission_id
        assert claimed['status'] == 'started'
    finally:
        runner.session_store.close_all_db_handles()
