"""Unit tests for scripts/generate-opencode.py.

Verifies frontmatter parsing, agent/command conversion rules for OpenCode,
and file collection invariants.
"""
from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest


def _load_generator():
    import sys
    spec_path = Path(__file__).resolve().parent.parent / "scripts" / "generate-opencode.py"
    spec = importlib.util.spec_from_file_location("generate_opencode_mod", spec_path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules["generate_opencode_mod"] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def gen():
    return _load_generator()


class TestParseFrontmatterAndBody:
    def test_parses_frontmatter_and_body(self, gen):
        text = """---
name: sample-agent
description: A sample agent for testing
model: sonnet
---

# Sample Agent
System instructions go here.
"""
        fm, body = gen.parse_frontmatter_and_body(text)
        assert fm["name"] == "sample-agent"
        assert fm["description"] == "A sample agent for testing"
        assert fm["model"] == "sonnet"
        assert "# Sample Agent" in body


class TestConvertAgentForOpenCode:
    def test_specialized_agent_gets_subagent_mode_and_no_model(self, gen):
        text = """---
name: schema-designer
description: Star schema specialist
tier: T2
model: sonnet
color: cyan
---

# Schema Designer
Do schema design.
"""
        converted = gen.convert_agent_for_opencode(text)
        assert "mode: subagent" in converted
        assert "model: sonnet" not in converted
        assert "name: schema-designer" in converted
        assert "# Schema Designer" in converted

    def test_workflow_agent_gets_all_mode(self, gen):
        text = """---
name: brainstorm-agent
description: Collaborative exploration specialist
tier: T2
model: sonnet
---

# Brainstorm Agent
Brainstorm ideas.
"""
        converted = gen.convert_agent_for_opencode(text)
        assert "mode: all" in converted
        assert "model: sonnet" not in converted


class TestConvertCommandForOpenCode:
    def test_mapped_command_binds_agent(self, gen):
        text = """---
name: pipeline
description: DAG/pipeline scaffolding
---

# Pipeline
Scaffold a data pipeline.
"""
        converted = gen.convert_command_for_opencode("pipeline", text)
        assert "agent: pipeline-architect" in converted
        assert "description: DAG/pipeline scaffolding" in converted
        assert "# Pipeline" in converted

    def test_unmapped_command_preserves_body(self, gen):
        text = """---
description: Run test suite
---

Execute tests now.
"""
        converted = gen.convert_command_for_opencode("test", text)
        assert "description: Run test suite" in converted
        assert "Execute tests now." in converted


class TestFileCollection:
    def test_collects_expected_agent_and_command_counts(self, gen):
        files = gen.collect_generated_files()
        agent_files = [p for p in files if p.parent == gen.OPENCODE_AGENTS_DIR]
        command_files = [p for p in files if p.parent == gen.OPENCODE_COMMANDS_DIR]

        assert len(agent_files) == 59
        assert len(command_files) == 31
