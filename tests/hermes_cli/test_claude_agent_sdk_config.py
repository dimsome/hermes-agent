"""Tests for the agent.claude_agent_sdk config block.

The claude-agent-sdk provider reads its behavioural flags exclusively from
config.yaml, so the canonical defaults must be registered in DEFAULT_CONFIG.
The example file alone does not make them real config options for
default-driven config tooling.

Adapted from upstream PR #65982 commit 10659717722effd9bb7738423a2399770f50a428.
"""

from __future__ import annotations

from pathlib import Path

from hermes_cli.config import DEFAULT_CONFIG


class TestClaudeAgentSdkDefaults:
    def test_default_config_has_the_block(self):
        agent = DEFAULT_CONFIG.get("agent")
        assert isinstance(agent, dict)
        assert "claude_agent_sdk" in agent

    def test_canonical_defaults(self):
        # Subscription-only is invariant runtime behavior, not a configurable
        # escape hatch. Additional directory access is opt-in and empty by
        # default.
        assert DEFAULT_CONFIG["agent"]["claude_agent_sdk"] == {
            "streaming": False,
            "append_file": "",
            "add_dirs": [],
        }

    def test_example_documents_empty_additional_directories(self):
        example = Path(__file__).resolve().parents[2] / "cli-config.yaml.example"
        assert "add_dirs: []" in example.read_text()


class TestUserConfigMerge:
    """Existing config gets defaults while explicit user values survive."""

    def _load(self, tmp_path, monkeypatch, user_cfg):
        import importlib

        import yaml

        home = tmp_path / ".hermes"
        home.mkdir()
        (home / "config.yaml").write_text(yaml.safe_dump(user_cfg))

        monkeypatch.setenv("HERMES_HOME", str(home))
        import hermes_cli.config as cfg_mod

        importlib.reload(cfg_mod)
        return cfg_mod.load_config()

    def test_config_without_block_gets_defaults(self, tmp_path, monkeypatch):
        cfg = self._load(tmp_path, monkeypatch, {"agent": {"max_turns": 5}})
        assert cfg["agent"]["claude_agent_sdk"] == {
            "streaming": False,
            "append_file": "",
            "add_dirs": [],
        }
        assert cfg["agent"]["max_turns"] == 5

    def test_explicit_user_values_survive_merge(self, tmp_path, monkeypatch):
        cfg = self._load(
            tmp_path,
            monkeypatch,
            {"agent": {"claude_agent_sdk": {"streaming": True}}},
        )
        assert cfg["agent"]["claude_agent_sdk"]["streaming"] is True
        assert "allow_metered_key" not in cfg["agent"]["claude_agent_sdk"]
        assert cfg["agent"]["claude_agent_sdk"]["append_file"] == ""
        assert cfg["agent"]["claude_agent_sdk"]["add_dirs"] == []
