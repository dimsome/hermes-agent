"""Shared native routes retain each attachment's separately authorized operator."""
from dataclasses import replace
from datetime import UTC, datetime

import pytest

from gateway.platforms.event import MessageEvent
from gateway.session_ingress_context import native_callback
from hermes_state_runtime import RuntimeStoreError, list_session_admissions
from tests.gateway.test_session_attachment import setup_bridge


STAMP = datetime(2026, 1, 1, tzinfo=UTC)


def capture_command(ctx):
    captured = []

    def join(raw, invocation=None):
        captured.append(invocation.capture_session_attachment())
        return 'captured'

    ctx.register_command('attachment-participant', join, invocation_context=True)
    return captured


async def dispatch(runner, source):
    return await runner._hm_dispatch_plugin_command(
        MessageEvent(text='/attachment-participant', source=source), source, 'attachment-participant')


@pytest.mark.asyncio
@pytest.mark.parametrize('thread_id', ['shared-thread', None], ids=['shared-thread', 'shared-group'])
async def test_second_authorized_participant_attaches_without_replacing_first_source(tmp_path, monkeypatch, thread_id):
    home = tmp_path / '.hermes'
    home.mkdir()
    runner, owner, _, adapter, _, ctx, _ = await setup_bridge(home, monkeypatch)
    runner._wire_adapter_handlers(adapter)
    monkeypatch.setattr(owner, '_schedule', lambda ref: None)
    monkeypatch.setenv('DISCORD_ALLOWED_USERS', 'operator,other-operator')
    runner.config.group_sessions_per_user = False
    sources = [adapter.build_source(chat_id='room', chat_type='group', user_id=user,
                                    thread_id=thread_id, guild_id='guild')
               for user in ('other-operator', 'operator', 'unauthorized')]
    first, second, unauthorized = sources
    for source in sources:
        runner._canonicalize(source)
    captured = capture_command(ctx)
    try:
        ordinary = MessageEvent(text='first participant', source=first, message_id='shared-before', timestamp=STAMP)
        with native_callback(runner, ordinary, home):
            admitted = await owner.admit_native(ordinary)
        ref = admitted.ref
        live = owner.sessions[ref.session_id]
        original_source = live.source
        assert runner._session_key_for_source(first) == runner._session_key_for_source(second)
        assert runner._is_user_authorized_for_source(second, allow_adapter_delegation=False)
        handled, output, _ = await dispatch(runner, second)
        assert handled and output == 'captured' and len(captured) == 1
        attachment = captured[0]
        assert attachment.scope.session_id == ref.session_id
        assert attachment.scope.operator_user_id == second.user_id
        assert live.source is original_source and live.source.user_id == first.user_id
        voice = await attachment.submit(attachment.scope, input_id='second-audio',
                                        text='second participant input', timestamp=STAMP)
        assert voice.ref == ref
        rows = list_session_admissions(owner.db, session_id=ref.session_id)
        assert rows[-1]['payload']['native_text_v1']['source']['user_id'] == second.user_id
        assert live.source is original_source and live.source.user_id == first.user_id
        # Same-route users still need authorization; they cannot borrow the captured scope.
        assert not runner._is_user_authorized_for_source(unauthorized, allow_adapter_delegation=False)
        handled, _, _ = await dispatch(runner, unauthorized)
        assert not handled and len(captured) == 1
        with pytest.raises(RuntimeStoreError, match='attachment_scope_mismatch'):
            await attachment.submit(replace(attachment.scope, operator_user_id=unauthorized.user_id),
                                    input_id='foreign', text='not authorized', timestamp=STAMP)
        monkeypatch.setenv('DISCORD_ALLOWED_USERS', 'other-operator')
        with pytest.raises(RuntimeStoreError, match='permission_denied'):
            await attachment.snapshot(attachment.scope)
        with pytest.raises(RuntimeStoreError, match='permission_denied'):
            await attachment.submit(attachment.scope, input_id='revoked', text='not authorized', timestamp=STAMP)
        assert list_session_admissions(owner.db, session_id=ref.session_id) == rows
    finally:
        runner.session_store.close_all_db_handles()


@pytest.mark.asyncio
@pytest.mark.parametrize('thread_id', ['isolated-thread', None], ids=['per-user-thread', 'per-user-group'])
async def test_per_user_route_cannot_capture_or_submit_to_another_participants_session(tmp_path, monkeypatch, thread_id):
    home = tmp_path / '.hermes'
    home.mkdir()
    runner, owner, _, adapter, _, ctx, _ = await setup_bridge(home, monkeypatch)
    runner._wire_adapter_handlers(adapter)
    monkeypatch.setattr(owner, '_schedule', lambda ref: None)
    monkeypatch.setenv('DISCORD_ALLOWED_USERS', 'operator,other-operator')
    runner.config.thread_sessions_per_user = True
    sources = [adapter.build_source(chat_id='isolated-room', chat_type='group', user_id=user,
                                    thread_id=thread_id, guild_id='guild')
               for user in ('other-operator', 'operator')]
    first, second = sources
    for source in sources:
        runner._canonicalize(source)
    captured = capture_command(ctx)
    try:
        assert runner._session_key_for_source(first) != runner._session_key_for_source(second)
        ordinary = MessageEvent(text='first isolated participant', source=first,
                                message_id='isolated-first', timestamp=STAMP)
        with native_callback(runner, ordinary, home):
            first_receipt = await owner.admit_native(ordinary)
        first_rows = list_session_admissions(owner.db, session_id=first_receipt.ref.session_id)
        handled, _, _ = await dispatch(runner, second)
        assert not handled and captured == []  # no existing session for this operator
        own = MessageEvent(text='second isolated participant', source=second,
                           message_id='isolated-second', timestamp=STAMP)
        with native_callback(runner, own, home):
            own_receipt = await owner.admit_native(own)
        assert own_receipt.ref != first_receipt.ref
        handled, output, _ = await dispatch(runner, second)
        assert handled and output == 'captured' and len(captured) == 1
        attachment = captured[0]
        assert attachment.scope.session_id == own_receipt.ref.session_id
        with pytest.raises(RuntimeStoreError, match='attachment_scope_mismatch'):
            await attachment.submit(replace(attachment.scope, session_id=first_receipt.ref.session_id),
                                    input_id='wrong-session', text='must not run', timestamp=STAMP)
        voice = await attachment.submit(attachment.scope, input_id='isolated-audio',
                                        text='own session only', timestamp=STAMP)
        assert voice.ref == own_receipt.ref
        assert list_session_admissions(owner.db, session_id=first_receipt.ref.session_id) == first_rows
        live = owner.sessions[own_receipt.ref.session_id]
        live.source = replace(live.source, user_id=first.user_id)
        with pytest.raises(RuntimeStoreError, match='stale_attachment'):
            await attachment.snapshot(attachment.scope)
    finally:
        runner.session_store.close_all_db_handles()
