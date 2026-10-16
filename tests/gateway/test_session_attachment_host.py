"""Real GatewayRunner/AIAgent/tool/SQLite path; only inference and audio are scripted."""
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys


def run_probe(root):
    """Keep imports, subprocesses and all host state inside this disposable root."""
    source = Path(__file__).resolve().parents[2]
    for name in ('home', '.hermes', 'tmp', 'cache', 'runtime', 'data', 'config', 'state'):
        (root / name).mkdir(parents=True)
    # A terminal .hermes component prevents discovery of an enclosing named profile
    # when this checkout and pytest scratch happen to live under that profile.
    env = {
        'PATH': str(Path(sys.executable).parent) + os.pathsep + os.defpath,
        'HOME': str(root / 'home'), 'HERMES_HOME': str(root / '.hermes'),
        'HERMES_RUNTIME_DIR': str(root / 'runtime'),
        'TMPDIR': str(root / 'tmp'), 'TMP': str(root / 'tmp'), 'TEMP': str(root / 'tmp'),
        'XDG_CACHE_HOME': str(root / 'cache'), 'XDG_DATA_HOME': str(root / 'data'),
        'XDG_CONFIG_HOME': str(root / 'config'), 'XDG_STATE_HOME': str(root / 'state'),
        'PYTHONPATH': str(source), 'PYTHONDONTWRITEBYTECODE': '1',
        'HERMES_DISABLE_LAZY_INSTALLS': '1', 'TZ': 'UTC', 'LANG': 'C.UTF-8', 'LC_ALL': 'C.UTF-8',
    }
    command = [sys.executable, '-m', 'tests.gateway.fixtures.session_attachment_peer']
    log = root / 'probe.log'
    with log.open('w', encoding='utf-8') as output:
        completed = subprocess.run(command, cwd=root, env=env, stdin=subprocess.DEVNULL,
                                   stdout=output, stderr=subprocess.STDOUT, timeout=150, check=False)
    assert completed.returncode == 0, f'{log}\n{log.read_text(encoding="utf-8")}'
    return json.loads((root / '.hermes/evidence.json').read_text(encoding='utf-8'))


def test_production_host_keeps_tool_history_approvals_and_interrupts_in_one_session(tmp_path):
    evidence = run_probe(tmp_path / 'host')
    sid = evidence['session_id']
    with sqlite3.connect(f'file:{tmp_path / "host/.hermes/state.db"}?mode=ro', uri=True) as db:
        history = db.execute('SELECT role,content FROM messages WHERE session_id=? ORDER BY id', (sid,)).fetchall()
        result = db.execute('SELECT value FROM state_meta WHERE key=?',
                            ('gateway.admission.result.v1.' + evidence['voice_receipt']['admission_id'],)).fetchone()
    assert [content for role, content in history if role == 'user'][:3] == [
        'TEXT_BEFORE baseline context', 'VOICE_WRITE harmless file', 'TEXT_AFTER what happened?']
    tool = next(json.loads(content) for role, content in history if role == 'tool' and 'voice-proof.txt' in content)
    assert tool['verified'] is True and tool['bytes_written'] == len(evidence['file_content'].encode())
    assert Path(tool['resolved_path']).read_text() == evidence['file_content']
    assert result and 'voice-proof.txt' in result[0]
    assert evidence['same_agent'] and evidence['same_store_owner']
    assert evidence['identity_and_tool_surface_preserved']
    assert evidence['approval_denied_without_write'] and evidence['queue_and_interrupt']
    assert evidence['non_loopback_network'] == []
