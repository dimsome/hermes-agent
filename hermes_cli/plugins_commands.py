"""Plugin slash-command registration, including opt-in gateway invocation context."""
from contextlib import suppress
import logging

logger = logging.getLogger('hermes_cli.plugins')


def register_command(ctx, name, handler, description, args_hint, argument_mode, invocation_context):
    clean = name.lower().strip().lstrip('/').replace(' ', '-')
    if not clean:
        logger.warning("Plugin '%s' tried to register a command with an empty name.", ctx.manifest.name)
        return None
    with suppress(Exception):  # reject if it conflicts with a built-in command
        from hermes_cli.commands import resolve_command
        if resolve_command(clean) is not None:
            logger.warning("Plugin '%s' tried to register command '/%s' which conflicts "
                           'with a built-in command. Skipping.', ctx.manifest.name, clean)
            return None
    hint = (args_hint or '').strip()
    if invocation_context:
        from gateway.plugin_invocation import bind_command_invocation
        handler = bind_command_invocation(ctx, handler)
    entry = {
        'handler': handler, 'description': description or 'Plugin command',
        'plugin': ctx.manifest.name, 'plugin_key': ctx.plugin_id, 'args_hint': hint,
        'argument_mode': argument_mode if argument_mode in {'options', 'text', 'mixed'}
        else ('text' if hint else None),
    }
    return ctx._register_entry('command', clean, ctx._manager._plugin_commands, entry,
                               "Plugin %s registered command: /%s", clean)
