"""Command-issued attachment to an existing native session, not an execution runtime.

A trusted transport supplies stable transcript ids/timestamps and its observed scope.
Audio provenance, STT/TTS and listener authorization remain the transport's job. This
bridge never creates an agent, selects a model, dispatches a tool or answers approvals.
"""
from dataclasses import asdict, dataclass
from datetime import datetime
import json
import uuid

from gateway.platforms.event import MessageEvent
from gateway.session_authorities import authority_for_home
from gateway.session_contract import Principal, SessionRef
from gateway.session_identity import clear_identity, identity_of, replace_source
from gateway.session_ingress_context import native_callback
from hermes_constants import hermes_home_key
from hermes_state_runtime import RuntimeStoreError, _epoch


@dataclass(frozen=True)
class AttachmentScope:
    profile_id: str
    session_id: str
    platform: str
    chat_id: str
    thread_id: str | None
    guild_id: str | None
    operator_user_id: str


def _native_route(source):
    return (source.platform.value, source.chat_id, source.thread_id, source.guild_id)


class SessionAttachment:
    """A profile-local plugin capability, bounded to one host-observed operator and route.

    ``submit`` explicitly queues in the host FIFO; ``interrupt`` uses its exact running
    generation. Neither changes the text adapter's busy/steer policy. Closing only retires
    this attachment, never the session or accepted work. Capture again after host restart,
    reset, route/adapter change or grant revocation. No RPC or bearer-token surface is added.
    """
    def __init__(self, runner, source, ctx):
        self._runner, self._ctx = runner, ctx
        self._source = replace_source(source)
        self._identity = identity_of(source)
        if self._identity is None or self._identity.adapter() is None or not source.user_id:
            raise RuntimeStoreError('native_source_required')
        self._transport_profile = runner._owning_profile(self._identity.adapter(), source.platform)[1]
        home = self._identity.runtime_home
        self._authority = authority_for_home(runner, home)
        if self._authority is None or hermes_home_key(ctx._manager.home_path) != hermes_home_key(home):
            raise RuntimeStoreError('profile_mismatch')
        self._route = runner._session_key_for_source(source)
        entry = runner.session_store.lookup_by_session_key(self._route)
        if entry is None:
            raise RuntimeStoreError('not_found')
        self._ref = SessionRef(self._authority.profile_id, self._authority.logical_owner(entry.session_id))
        self._live = self._authority.sessions.get(self._ref.session_id)
        self._epoch = self._authority.epoch
        self._closed = False
        self.scope = AttachmentScope(self._authority.profile_id, self._ref.session_id, *_native_route(source), source.user_id)
        principal = json.dumps([source.profile, source.platform.value, source.chat_id,
                                source.thread_id, source.user_id], separators=(',', ':'))
        self._actor = Principal('messaging:' + principal, self._ref.profile_id,
                                frozenset({'session:read', 'session:submit', 'session:control'}), uuid.uuid4().hex)
        self._check(self.scope)

    def _check(self, scope, conn=None):
        if self._closed:
            raise RuntimeStoreError('attachment_closed')
        if type(scope) is not AttachmentScope or scope != self.scope:
            raise RuntimeStoreError('attachment_scope_mismatch')
        runner, owner = self._runner, self._authority
        if (authority_for_home(runner, self._identity.runtime_home) is not owner
                or owner.retired or owner.retiring or not runner._running or runner._draining):
            raise RuntimeStoreError('stale_attachment')
        with runner._profile_scope_for_source(self._source):
            if not self._ctx._gateway_injection_allowed():
                raise RuntimeStoreError('permission_denied')
            source = replace_source(self._source)
            clear_identity(source)
            identity = runner._canonicalize(source)
            if (identity != self._identity or identity.adapter() is not self._identity.adapter()
                    or runner._intake_adapter_for(source) is not self._identity.adapter()
                    or not runner._owning_profile(self._identity.adapter(), source.platform)[0]):
                raise RuntimeStoreError('stale_attachment')
            if not runner._is_user_authorized_for_source(source, allow_adapter_delegation=False):
                raise RuntimeStoreError('permission_denied')
            entry = runner.session_store.lookup_by_session_key(self._route)
            if (entry is None or entry.suspended or self._live is None
                    or owner.sessions.get(self._ref.session_id) is not self._live
                    or self._live.route != self._route
                    or _native_route(self._live.source) != _native_route(self._source)
                    or runner._session_key_for_source(self._live.source) != self._route
                    or runner._session_key_for_source(source) != self._route
                    or entry.session_id != owner.physical_target(self._ref)):
                raise RuntimeStoreError('stale_attachment')
            if conn is None:
                with owner.db._read_ctx() as read:
                    _epoch(read, self._epoch)
            else:
                _epoch(conn, self._epoch)
        return source

    async def submit(self, scope, *, input_id, text, timestamp):
        """ACK only after durable native admission. An identical retry cannot execute twice.

        ``timestamp`` is the transport's stable capture time, not retry time. Transcript
        text is input to Hermes, never a gateway slash command or approval response.
        """
        if (not isinstance(input_id, str) or not input_id.strip() or len(input_id) > 256
                or not isinstance(text, str) or not text.strip()
                or not isinstance(timestamp, datetime) or timestamp.utcoffset() is None):
            raise RuntimeStoreError('invalid_params')
        async with self._runner._async_profile_scope_for_source(self._source):
            source = self._check(scope)
            event = MessageEvent(text=text, source=source, timestamp=timestamp,
                message_id=json.dumps(['plugin-session', self._ctx.plugin_id, input_id], separators=(',', ':')))
            if event.is_command():
                raise RuntimeStoreError('invalid_params')
            event.allow_gateway_control = False
            # The private callback binds this exact new event to the retained receiving bot,
            # not the routed runtime's home or any caller-supplied provenance.
            with native_callback(self._runner, event, self._identity.authorization_home, self._transport_profile):
                return await self._authority.admit_native(event, _expected_ref=self._ref,
                    _authorize_write=lambda conn: self._check(scope, conn))

    async def snapshot(self, scope):
        """Read the canonical host history without taking a viewer slot or execution lease."""
        async with self._runner._async_profile_scope_for_source(self._source):
            self._check(scope)
            handle = await self._authority.resolve(self._actor, self._ref)
            from gateway.session_local_recovery import local_history
            return {'handle': asdict(handle), 'history': local_history(self._authority, self._ref)}

    async def result(self, scope, admission_id):
        """Return the host's receipt and durably committed result, never a generated answer."""
        async with self._runner._async_profile_scope_for_source(self._source):
            self._check(scope)
            receipt = await self._authority.receipt(self._actor, self._ref, admission_id)
            from gateway.session_results import admission_result
            saved = admission_result(self._authority.db, admission_id) if receipt.status == 'terminal' else None
            return {'receipt': receipt, 'result': saved}

    async def interrupt(self, scope, generation):
        async with self._runner._async_profile_scope_for_source(self._source):
            self._check(scope)
            if type(generation) is not int or generation < 1:
                raise RuntimeStoreError('invalid_params')
            return await self._authority.interrupt(self._actor, self._ref, generation)

    def close(self):
        self._closed = True
