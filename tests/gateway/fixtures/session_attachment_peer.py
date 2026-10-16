"""Isolated production host probe. Only audio, Discord and model wire are scripted."""
import asyncio
from dataclasses import asdict, replace
from datetime import UTC, datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import shutil
import socket
import sqlite3
import subprocess
import sys
import threading
import traceback

HOME = Path(os.environ['HERMES_HOME'])
# Project-file approval rules deliberately exclude the Hermes state tree.
REPO = HOME.parent / 'disposable-repo'
MARKER = 'real-host-file-tool-evidence\n'
NETWORK = []


def guard_network():
    connect = socket.socket.connect

    def local_only(sock, address):
        if isinstance(address, tuple) and address[0] not in {'127.0.0.1', '::1'}:
            NETWORK.append(str(address))
            raise AssertionError('offline probe refused non-loopback network')
        return connect(sock, address)

    socket.socket.connect = local_only


class ScriptedModel(BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def do_GET(self):
        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.end_headers()
        self.wfile.write(json.dumps({'data': [{'id': 'offline-tool-model', 'object': 'model'}]}).encode())

    def do_POST(self):
        request = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
        self.server.requests.append(request)
        messages = request.get('messages', [])
        index = next((i for i in range(len(messages) - 1, -1, -1) if messages[i]['role'] == 'user'), -1)
        text = str(messages[index].get('content', '')) if index >= 0 else ''
        tools = [m for m in messages[index + 1:] if m['role'] == 'tool']
        delta, finish = {'role': 'assistant', 'content': 'TEXT_ACK'}, 'stop'
        if 'VOICE_WRITE' in text or 'APPROVAL_WRITE' in text:
            path = REPO / ('AGENTS.md' if 'APPROVAL_WRITE' in text else 'voice-proof.txt')
            if tools:
                delta['content'] = 'HOST_TOOL_RESULT: ' + str(tools[-1]['content'])
            else:
                delta = {'role': 'assistant', 'content': None, 'tool_calls': [{
                    'index': 0, 'id': 'proof-' + path.name, 'type': 'function',
                    'function': {'name': 'write_file', 'arguments': json.dumps({'path': str(path), 'content': MARKER})}}]}
                finish = 'tool_calls'
        elif 'TEXT_AFTER' in text:
            assert any(m['role'] == 'tool' and 'voice-proof.txt' in str(m.get('content')) for m in messages)
            delta['content'] = 'TEXT_SEES_CANONICAL_TOOL_HISTORY'
        elif 'BLOCK_TURN' in text:
            self.server.blocked.set()
            assert self.server.release.wait(30), 'blocked model request timed out'
            delta['content'] = 'INTERRUPTED_WIRE_TAIL'
        base = {'id': 'offline-completion', 'model': 'offline-tool-model', 'created': 1}
        self.send_response(200)
        self.send_header('Content-Type', 'text/event-stream' if request.get('stream') else 'application/json')
        self.end_headers()
        if request.get('stream'):
            for body, reason in [(delta, None), ({}, finish)]:
                chunk = {**base, 'object': 'chat.completion.chunk', 'choices': [
                    {'index': 0, 'delta': body, 'finish_reason': reason}]}
                self.wfile.write(('data: ' + json.dumps(chunk) + '\n\n').encode())
                self.wfile.flush()
            self.wfile.write(b'data: [DONE]\n\n')
        else:
            delta.pop('index', None)
            self.wfile.write(json.dumps({**base, 'object': 'chat.completion', 'choices': [
                {'index': 0, 'message': delta, 'finish_reason': finish}],
                'usage': {'prompt_tokens': 10, 'completion_tokens': 5, 'total_tokens': 15}}).encode())


async def refused(operation, reason):
    from hermes_state_runtime import RuntimeStoreError
    try:
        await operation
    except RuntimeStoreError as exc:
        assert exc.reason == reason, (exc.reason, reason)
    else:
        raise AssertionError('host accepted input after ' + reason)


async def verify_attachment_guards(runner, owner, consumer, receipt, provider):
    from hermes_cli.config import atomic_config_write, load_config
    from hermes_state_runtime import begin_runtime_epoch, list_session_admissions
    rows = list_session_admissions(owner.db, session_id=consumer.scope.session_id, pending_only=False)
    calls = len(provider.requests)
    stamp = datetime(2026, 1, 1, tzinfo=UTC)
    checked = []
    for field, value in [('profile_id', 'foreign'), ('session_id', 'unknown-session'),
                         ('platform', 'telegram'), ('chat_id', 'foreign-room'),
                         ('thread_id', 'foreign-thread'), ('guild_id', 'foreign-guild'),
                         ('operator_user_id', 'stranger')]:
        scope = replace(consumer.scope, **{field: value})
        await refused(consumer.feed(b'\0\0', 'must not run', input_id=field, timestamp=stamp, scope=scope),
                      'attachment_scope_mismatch')
        await refused(consumer.attachment.snapshot(scope), 'attachment_scope_mismatch')
        checked.append('foreign_' + field)
    await refused(consumer.feed(b'\0\0', 'conflicting retry', input_id='audio-1', timestamp=stamp),
                  'admission_conflict')
    for text in ('/approve', '/deny', '/stop', '/new'):
        await refused(consumer.feed(b'\0\0', text, input_id=text, timestamp=stamp), 'invalid_params')
    checked.extend(['conflicting_retry', 'command_transcript'])

    config = load_config()
    config['plugins']['entries']['talk-session-fixture']['allow_gateway_injection'] = False
    atomic_config_write(HOME / 'config.yaml', config)
    await refused(consumer.saved(receipt.admission_id), 'permission_denied')
    await refused(consumer.feed(b'\0\0', 'revoked', input_id='revoked', timestamp=stamp), 'permission_denied')
    config['plugins']['entries']['talk-session-fixture']['allow_gateway_injection'] = True
    atomic_config_write(HOME / 'config.yaml', config)
    checked.append('revoked_plugin_consent')
    os.environ['DISCORD_ALLOWED_USERS'] = 'someone-else'
    await refused(consumer.snapshot(), 'permission_denied')
    await refused(consumer.attachment.interrupt(consumer.scope, 1), 'permission_denied')
    os.environ['DISCORD_ALLOWED_USERS'] = 'operator'
    checked.append('revoked_operator')
    owner.retiring = True
    await refused(consumer.snapshot(), 'stale_attachment')
    owner.retiring = False
    checked.append('retiring_profile')
    begin_runtime_epoch(owner.db, instance_id='offline-successor')
    await refused(consumer.snapshot(), 'stale_epoch')
    await refused(consumer.feed(b'\0\0', 'stale', input_id='stale', timestamp=stamp), 'stale_epoch')
    checked.append('stale_epoch')
    assert list_session_admissions(owner.db, session_id=consumer.scope.session_id, pending_only=False) == rows
    assert len(provider.requests) == calls, 'a refused attachment reached inference'
    return checked


async def probe(provider):
    from gateway.config import GatewayConfig, Platform
    from gateway.platforms.event import MessageEvent
    from gateway.run import GatewayRunner
    from gateway.session_authority import initialize_session_authority
    from hermes_cli.plugins import discover_plugins, get_plugin_manager, get_plugin_command_handler
    from hermes_state_runtime import list_session_admissions
    from run_agent import AIAgent
    from tests.gateway.test_session_attachment import OfflineDiscord
    from tests.waiting import async_wait_until
    from tools.approval import _gateway_queues

    runner = GatewayRunner(GatewayConfig(sessions_dir=HOME / 'sessions'))
    runner._running = True
    owner = await initialize_session_authority(runner, profile_id=str(HOME), instance_id='offline-tool-host')
    adapter = OfflineDiscord()
    runner.adapters = {Platform.DISCORD: adapter}
    runner._wire_adapter_handlers(adapter)
    source = adapter.build_source(chat_id='room', chat_type='group', user_id='operator', guild_id='guild')

    async def settled():
        await async_wait_until(lambda: not adapter._active_sessions, timeout=35, what='adapter turn completion')
        for live in owner.sessions.values():
            if live.task is not None:
                await asyncio.wait_for(asyncio.shield(live.task), 35)

    await adapter.handle_message(MessageEvent(text='TEXT_BEFORE baseline context', source=source, message_id='text-before'))
    await settled()
    entry = runner.session_store.get_or_create_session(source)
    agent = runner._cached_agent_for(entry.session_key)
    assert isinstance(agent, AIAgent), adapter.sent
    assert agent.session_id == entry.session_id
    assert agent._session_db is owner.db, 'the host must keep its existing store owner'
    tool_names = set(agent.valid_tool_names)
    assert 'write_file' in tool_names
    discover_plugins()
    manager = get_plugin_manager()
    plugin = manager._plugins['talk-session-fixture']
    assert plugin.module is not None, plugin
    assert get_plugin_command_handler('talk-fixture')('') == 'host invocation required'
    assert get_plugin_command_handler('talk-legacy')('test') == 'legacy:test'
    await adapter.handle_message(MessageEvent(text='/talk-fixture', source=source, message_id='join-offline'))
    await settled()
    consumer = plugin.module.SESSION
    assert consumer is not None, adapter.sent
    assert consumer.scope.session_id == entry.session_id and consumer.scope.profile_id == str(HOME)
    snapshot = await consumer.snapshot()
    assert 'TEXT_BEFORE' in json.dumps(snapshot)
    stamp = datetime(2026, 1, 1, tzinfo=UTC)
    pcm = b'\0\0' * 160  # scripted PCM, NOT speech and NOT transcribed by a provider
    receipt, concurrent = await asyncio.gather(*(
        consumer.feed(pcm, 'VOICE_WRITE harmless file', input_id='audio-1', timestamp=stamp) for _ in range(2)))
    assert receipt.admission_id == concurrent.admission_id
    await settled()
    result = await consumer.saved(receipt.admission_id)
    assert result['receipt'].status == 'terminal' and result['receipt'].outcome == 'completed', result
    assert (REPO / 'voice-proof.txt').read_text() == MARKER
    assert 'voice-proof.txt' in json.dumps(result['result'])
    before_calls = len(provider.requests)
    duplicate = await consumer.feed(pcm, 'VOICE_WRITE harmless file', input_id='audio-1', timestamp=stamp)
    await settled()
    assert duplicate.admission_id == receipt.admission_id and len(provider.requests) == before_calls
    await adapter.handle_message(MessageEvent(text='TEXT_AFTER what happened?', source=source, message_id='text-after'))
    await settled()
    assert any('TEXT_SEES_CANONICAL_TOOL_HISTORY' in message for message in adapter.sent)
    assert runner._cached_agent_for(entry.session_key) is agent
    assert agent._session_db is owner.db and set(agent.valid_tool_names) == tool_names
    assert not getattr(agent, '_voice_turn_pending', False)
    requests = [r for r in provider.requests if 'messages' in r]
    systems = [[m for m in r['messages'] if m['role'] == 'system'] for r in requests]
    assert systems and all(s == systems[0] for s in systems), 'voice changed the cached identity prefix'
    assert 'HOST_IDENTITY_SENTINEL' in json.dumps(systems[0])
    assert all(r.get('tools') == requests[0].get('tools') for r in requests), 'voice changed the tool surface'

    # A protected-file write is harmless in this disposable repo, but must still wait
    # at the real host approval boundary. The attachment has no approval API.
    approval = await consumer.feed(pcm, 'APPROVAL_WRITE deny this', input_id='audio-approval', timestamp=stamp)
    await async_wait_until(lambda: bool(_gateway_queues.get(entry.session_key)), timeout=20, what='real tool approval')
    pending = _gateway_queues[entry.session_key][0]
    assert not pending.event.is_set() and not (REPO / 'AGENTS.md').exists()
    assert not hasattr(consumer.attachment, 'respond')
    queued = await consumer.feed(pcm, 'yes', input_id='audio-yes', timestamp=stamp)
    assert not pending.event.is_set(), 'a transcript silently answered the approval'
    await adapter.handle_message(MessageEvent(text='/deny', source=source, message_id='deny-real-approval'))
    await settled()
    assert pending.event.is_set() and not (REPO / 'AGENTS.md').exists()
    denied = await consumer.saved(approval.admission_id)
    assert 'denied' in json.dumps(denied['result']).lower(), denied
    assert (await consumer.saved(queued.admission_id))['receipt'].status == 'terminal'

    # Explicit queue/interrupt semantics belong to the same host generation. A text
    # turn blocks on the scripted provider; the voice follow-up stays durably queued.
    await adapter.handle_message(MessageEvent(text='BLOCK_TURN', source=source, message_id='text-block'))
    assert await asyncio.to_thread(provider.blocked.wait, 15)
    waiting = await consumer.feed(pcm, 'queued after interrupt', input_id='audio-queued', timestamp=stamp)
    assert waiting.status == 'queued'
    handle = (await consumer.snapshot())['handle']
    await refused(consumer.attachment.interrupt(consumer.scope, handle['execution_generation'] - 1), 'stale_generation')
    assert not provider.release.is_set() and not agent.is_interrupted
    await consumer.attachment.interrupt(consumer.scope, handle['execution_generation'])
    provider.release.set()
    await settled()
    rows = list_session_admissions(owner.db, session_id=entry.session_id, pending_only=False)
    blocked = next(row for row in rows if row['request_id'] == 'text-block')
    assert blocked['outcome'] == 'interrupted', blocked
    assert (await consumer.saved(waiting.admission_id))['receipt'].outcome == 'completed'
    assert runner._cached_agent_for(entry.session_key) is agent

    # Read through a fresh SQLite connection, not the live agent or model reply.
    with sqlite3.connect(f'file:{owner.db.db_path}?mode=ro', uri=True) as db:
        history = db.execute('SELECT role,content,tool_calls,tool_call_id FROM messages WHERE session_id=? ORDER BY id',
                             (entry.session_id,)).fetchall()
        admissions = db.execute('SELECT request_id,status,outcome FROM session_admissions WHERE target_session_id=? ORDER BY seq',
                                (entry.session_id,)).fetchall()
        sessions = db.execute('SELECT id,source FROM sessions').fetchall()
    assert any(role == 'tool' and 'voice-proof.txt' in (content or '') for role, content, *_ in history), history
    assert len({sid for sid, _ in sessions}) == 1, sessions
    assert len([r for r in rows if r['admission_id'] == receipt.admission_id]) == 1
    assert len([h for h in history if h[0] == 'tool' and 'voice-proof.txt' in (h[1] or '')]) == 1
    guards = await verify_attachment_guards(runner, owner, consumer, receipt, provider)
    assert not NETWORK, NETWORK
    evidence = {'mode': 'offline scripted PCM/transcript + loopback model; real host/tool/SQLite',
                'session_id': entry.session_id, 'same_agent': True, 'same_store_owner': True,
                'identity_and_tool_surface_preserved': True, 'real_file': str(REPO / 'voice-proof.txt'),
                'file_content': (REPO / 'voice-proof.txt').read_text(), 'voice_receipt': asdict(result['receipt']),
                'voice_result': result['result'], 'history': history, 'admissions': admissions,
                'approval_denied_without_write': True, 'queue_and_interrupt': True,
                'guard_checks': guards, 'db_path': str(owner.db.db_path),
                'runtime_python': sys.executable,
                'host_modules': {name: sys.modules[name].__file__ for name in
                                 ('gateway.run', 'run_agent', 'model_tools', 'tools.file_tools')},
                'non_loopback_network': NETWORK, 'model_requests': len(provider.requests)}
    (HOME / 'evidence.json').write_text(json.dumps(evidence, indent=2))
    (HOME / 'provider-requests.json').write_text(json.dumps(provider.requests, indent=2))
    print(json.dumps(evidence, indent=2))


def prepare():
    HOME.mkdir(parents=True, exist_ok=True)
    REPO.mkdir()
    subprocess.run(['git', 'init', '--quiet', str(REPO)], check=True, stdin=subprocess.DEVNULL)
    (HOME / 'SOUL.md').write_text('You are Hermes. HOST_IDENTITY_SENTINEL. Use the host tools and approval policy.\n')
    shutil.copytree(Path(__file__).parent / 'talk_session_plugin', HOME / 'plugins/talk-session-fixture')
    provider = ThreadingHTTPServer(('127.0.0.1', 0), ScriptedModel)
    provider.requests, provider.blocked, provider.release = [], threading.Event(), threading.Event()
    threading.Thread(target=provider.serve_forever, daemon=True).start()
    base = f'http://127.0.0.1:{provider.server_port}/v1'
    os.environ.update(OPENAI_API_KEY='offline-placeholder-only', OPENAI_BASE_URL=base,
                      DISCORD_ALLOWED_USERS='operator', NO_PROXY='127.0.0.1,localhost', HERMES_DISABLE_LAZY_INSTALLS='1')
    (HOME / 'config.yaml').write_text(
        f'model:\n  default: offline-tool-model\n  provider: custom\n  base_url: {base}\n'
        f'terminal:\n  backend: local\n  cwd: {REPO}\n'
        'tools:\n  discord:\n    enabled: [file]\n'
        'auxiliary:\n  title_generation:\n    enabled: false\n'
        'memory:\n  memory_enabled: false\n  user_profile_enabled: false\n'
        'display:\n  busy_input_mode: queue\n  tool_progress: off\n'
        'plugins:\n  enabled: [talk-session-fixture]\n  entries:\n'
        '    talk-session-fixture:\n      allow_gateway_injection: true\n')
    return provider


if __name__ == '__main__':
    provider = prepare()
    guard_network()
    status = 0
    try:
        asyncio.run(probe(provider))
    except BaseException:
        traceback.print_exc()
        status = 1
    finally:
        provider.release.set()
        provider.shutdown()
        provider.server_close()
        sys.stdout.flush()
        sys.stderr.flush()
    os._exit(status)  # this disposable child owns the host's background worker threads
