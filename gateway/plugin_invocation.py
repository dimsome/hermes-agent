"""Opt-in plugin command context, issued and retired by the gateway dispatch."""
from contextlib import contextmanager
from contextvars import ContextVar
from functools import wraps

from hermes_state_runtime import RuntimeStoreError

_current = ContextVar('plugin_command_invocation', default=None)


class GatewayCommandInvocation:
    def __init__(self, runner, event, ctx=None, *, owner=None):
        self._runner, self._event, self._ctx = runner, event, ctx
        self._owner = owner or self
        self._active = True

    def for_plugin(self, ctx):
        return GatewayCommandInvocation(self._runner, self._event, ctx, owner=self._owner)

    def capture_session_attachment(self):
        """Capture only the dispatch's existing native session, never a caller-selected target."""
        if not self._owner._active:
            raise RuntimeStoreError('invocation_expired')
        if self._ctx is None or self._event.internal or self._event.allow_gateway_control is not True:
            raise RuntimeStoreError('permission_denied')
        from gateway.session_attachment import SessionAttachment
        return SessionAttachment(self._runner, self._event.source, self._ctx)


@contextmanager
def command_invocation(runner, event):
    invocation = GatewayCommandInvocation(runner, event)
    token = _current.set(invocation)
    try:
        yield
    finally:
        invocation._active = False
        _current.reset(token)


def bind_command_invocation(ctx, handler):
    """Keep legacy handlers untouched; opted-in handlers receive ``invocation=None`` off-gateway."""
    @wraps(handler)
    def wrapped(raw_args):
        current = _current.get()
        return handler(raw_args, invocation=current.for_plugin(ctx) if current is not None else None)
    return wrapped
