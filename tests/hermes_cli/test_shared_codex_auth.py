"""Explicit Codex sharing keeps one token owner and default profile isolation."""
import json
from contextlib import contextmanager
from pathlib import Path

import pytest

from hermes_cli import auth
from hermes_cli.auth_codex import resolve_codex_runtime_credentials
from hermes_constants import reset_hermes_home_override, set_hermes_home_override


@contextmanager
def scope(home):
    from agent.secret_scope import set_secret_scope, reset_secret_scope
    token = set_hermes_home_override(home)
    secret_token = set_secret_scope({})
    try:
        yield
    finally:
        reset_secret_scope(secret_token)
        reset_hermes_home_override(token)


def write(path, value):
    path.write_text(json.dumps(value), encoding="utf-8")


def store(access="fixture-access", refresh="fixture-refresh"):
    return {
        "providers": {"openai-codex": {"tokens": {
            "access_token": access, "refresh_token": refresh}},
            "nous": {"access_token": "unrelated-fixture"}},
        "credential_pool": {"openai-codex": [{
            "id": "singleton", "source": "device_code", "auth_type": "oauth",
            "priority": 0, "access_token": access, "refresh_token": refresh}],
            "openrouter": [{"id": "unrelated", "access_token": "unrelated-fixture"}]},
    }


@pytest.fixture
def homes(tmp_path, monkeypatch):
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    root = tmp_path / ".hermes"
    root.mkdir()
    monkeypatch.setenv("HERMES_HOME", str(root))
    write(root / "config.yaml", {"security": {"shared_codex_profiles": ["coder"]}})
    write(root / "auth.json", store())
    for name in ("coder", "other"):
        home = root / "profiles" / name
        home.mkdir(parents=True)
        write(home / "config.yaml", {"model": {"provider": "openai-codex", "default": "gpt-5"}})
    # A failed implementation must never try a real transport or external CLI recovery.
    def no_network(*a, **kw):
        raise AssertionError("unexpected network / external credential access")
    monkeypatch.setattr(auth, "refresh_codex_oauth_pure", no_network)
    monkeypatch.setattr(auth, "_import_codex_cli_tokens", no_network)
    return root


@pytest.mark.parametrize("mode", ["allowed", "off", "other", "local", "local-pool", "missing", "malformed", "arbitrary"])
def test_explicit_source_contract(homes, mode):
    from agent.credential_pool import load_pool
    from cron.scheduler_preflight import _preflight_check_provider_key
    from hermes_cli.runtime_provider import resolve_runtime_provider

    root = homes
    home = root / "profiles" / ("other" if mode == "other" else "coder")
    if mode == "off":
        write(root / "config.yaml", {})
    elif mode in ("local", "local-pool"):
        local = store("independent-access", "independent-refresh")
        if mode == "local-pool":
            local["providers"].pop("openai-codex")
        write(home / "auth.json", local)
    elif mode == "missing":
        (root / "auth.json").unlink()
    elif mode == "malformed":
        (root / "auth.json").write_text("{broken", encoding="utf-8")
    elif mode == "arbitrary":
        home = root / "not-a-profile" / "coder"
        home.mkdir(parents=True)
        write(home / "config.yaml", {"security": {"shared_codex_profiles": ["coder"]}})

    before = (root / "auth.json").read_bytes() if (root / "auth.json").exists() else None
    with scope(home):
        if mode == "allowed":
            expected = store()["providers"]["openai-codex"]["tokens"]["access_token"]
            assert resolve_codex_runtime_credentials(refresh_if_expiring=False)["api_key"] == expected
            assert load_pool("openai-codex").select().runtime_api_key == expected
            assert resolve_runtime_provider(requested="openai-codex")["api_key"] == expected
            assert _preflight_check_provider_key({"id": "fixture", "provider": "openai-codex"}, {}) is None
            assert auth.get_provider_auth_state("nous") is None
            assert auth.read_credential_pool("openrouter") == []
        else:
            with pytest.raises(auth.AuthError):
                resolve_codex_runtime_credentials(refresh_if_expiring=False)
            assert _preflight_check_provider_key({"id": "fixture", "provider": "openai-codex"}, {}) is not None
    if before is not None:
        assert (root / "auth.json").read_bytes() == before
    if mode not in ("local", "local-pool"):
        assert not (home / "auth.json").exists()
