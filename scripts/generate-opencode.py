#!/usr/bin/env python3
"""Generate OpenCode agents and commands from canonical .claude/ source.

Single source of truth:
  - Agents:   .claude/agents/**/*.md
  - Commands: .claude/commands/**/*.md

Outputs:
  - .opencode/agents/<name>.md      (flat subagent definitions for OpenCode @agent)
  - .opencode/commands/<name>.md    (flat command templates for OpenCode /command)

Supports:
  python scripts/generate-opencode.py          # Generate/sync files
  python scripts/generate-opencode.py --check  # Fail if drift detected
"""
from __future__ import annotations

import argparse
import difflib
import re
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
CLAUDE_AGENTS_DIR = REPO_ROOT / ".claude" / "agents"
CLAUDE_COMMANDS_DIR = REPO_ROOT / ".claude" / "commands"

OPENCODE_DIR = REPO_ROOT / ".opencode"
OPENCODE_AGENTS_DIR = OPENCODE_DIR / "agents"
OPENCODE_COMMANDS_DIR = OPENCODE_DIR / "commands"

SKIP_FILES = frozenset({"README.md", "_template.md"})

# Workflow agents can function as both primary or subagent
WORKFLOW_AGENTS = frozenset({
    "brainstorm-agent",
    "define-agent",
    "design-agent",
    "build-agent",
    "ship-agent",
    "iterate-agent",
})

# Explicit command -> agent delegations
COMMAND_AGENT_MAP = {
    "brainstorm": "brainstorm-agent",
    "define": "define-agent",
    "design": "design-agent",
    "build": "build-agent",
    "ship": "ship-agent",
    "iterate": "iterate-agent",
    "pipeline": "pipeline-architect",
    "schema": "schema-designer",
    "data-quality": "data-quality-analyst",
    "sql-review": "sql-optimizer",
    "data-contract": "data-contract-specialist",
    "migration": "migration-specialist",
    "security-review": "data-security-specialist",
    "incident": "data-reliability-engineer",
    "create-kb": "kb-architect",
    "review": "code-reviewer",
    "meeting": "meeting-analyst",
    "status": "the-planner",
    "sync-context": "codebase-explorer",
}


def parse_frontmatter_and_body(text: str) -> tuple[dict, str]:
    """Split markdown into frontmatter dict and raw body."""
    m = re.match(r"^---\r?\n(.*?)\r?\n---\r?\n(.*)$", text, re.DOTALL)
    if not m:
        return {}, text

    fm_raw = m.group(1)
    body = m.group(2)
    fm: dict = {}

    for key in ("name", "tier", "model", "color", "agent"):
        val_m = re.search(rf"^{key}:\s*(.+)$", fm_raw, re.MULTILINE)
        if val_m:
            fm[key] = val_m.group(1).strip()

    desc_m = re.search(r"^description:\s*\|\s*\n((?:[ \t]+.*\r?\n?)+)", fm_raw, re.MULTILINE)
    if desc_m:
        fm["description"] = desc_m.group(1)
    else:
        desc_single = re.search(r"^description:\s*(.+)$", fm_raw, re.MULTILINE)
        if desc_single:
            fm["description"] = desc_single.group(1).strip()

    return fm, body


def convert_agent_for_opencode(source_text: str) -> str:
    """Convert a Claude Code agent file to an OpenCode agent definition."""
    fm, body = parse_frontmatter_and_body(source_text)
    name = fm.get("name", "")
    description = fm.get("description", "")

    # Mode: "all" for core workflow agents (can be primary or subagent); "subagent" for specialized agents
    mode = "all" if name in WORKFLOW_AGENTS else "subagent"

    # Format YAML frontmatter for OpenCode
    lines = ["---"]
    if name:
        lines.append(f"name: {name}")

    if description:
        if "\n" in description:
            lines.append("description: |")
            for dline in description.splitlines():
                if dline.strip():
                    lines.append(f"  {dline.strip()}")
                else:
                    lines.append("")
        else:
            lines.append(f"description: {description}")

    lines.append(f"mode: {mode}")
    # Note: We deliberately omit hardcoded model: sonnet so OpenCode can use
    # whatever model the user has active (including free tier models).
    lines.append("---")
    lines.append("")
    lines.append(body.strip())
    lines.append("")

    return "\n".join(lines)


def convert_command_for_opencode(command_name: str, source_text: str) -> str:
    """Convert a Claude Code command file to an OpenCode command definition."""
    fm, body = parse_frontmatter_and_body(source_text)
    description = fm.get("description", "")
    agent = COMMAND_AGENT_MAP.get(command_name, fm.get("agent", ""))

    lines = ["---"]
    if description:
        if "\n" in description:
            lines.append("description: |")
            for dline in description.splitlines():
                if dline.strip():
                    lines.append(f"  {dline.strip()}")
                else:
                    lines.append("")
        else:
            lines.append(f"description: {description}")

    if agent:
        lines.append(f"agent: {agent}")

    lines.append("---")
    lines.append("")
    lines.append(body.strip())
    lines.append("")

    return "\n".join(lines)


def collect_generated_files() -> dict[Path, str]:
    """Collect all target OpenCode files and their expected content."""
    files: dict[Path, str] = {}

    # 1. Process Agents
    for md in sorted(CLAUDE_AGENTS_DIR.rglob("*.md")):
        if md.name in SKIP_FILES:
            continue
        rel = md.relative_to(CLAUDE_AGENTS_DIR)
        if len(rel.parts) < 2:
            continue
        text = md.read_text(encoding="utf-8")
        converted = convert_agent_for_opencode(text)
        target_path = OPENCODE_AGENTS_DIR / md.name
        files[target_path] = converted

    # 2. Process Commands
    for md in sorted(CLAUDE_COMMANDS_DIR.rglob("*.md")):
        if md.name in SKIP_FILES:
            continue
        cmd_name = md.stem
        text = md.read_text(encoding="utf-8")
        converted = convert_command_for_opencode(cmd_name, text)
        target_path = OPENCODE_COMMANDS_DIR / md.name
        files[target_path] = converted

    return files


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate OpenCode agents and commands")
    parser.add_argument("--check", action="store_true", help="Check if generated OpenCode files are up to date")
    args = parser.parse_args()

    expected_files = collect_generated_files()

    if args.check:
        has_drift = False

        # Check existing vs expected
        for path, expected_content in expected_files.items():
            if not path.exists():
                print(f"[DRIFT] Missing file: {path.relative_to(REPO_ROOT)}", file=sys.stderr)
                has_drift = True
                continue
            actual_content = path.read_text(encoding="utf-8").replace("\r\n", "\n")
            expected_normalized = expected_content.replace("\r\n", "\n")
            if actual_content != expected_normalized:
                diff = difflib.unified_diff(
                    actual_content.splitlines(keepends=True),
                    expected_normalized.splitlines(keepends=True),
                    fromfile=f"{path.relative_to(REPO_ROOT)} (on disk)",
                    tofile=f"{path.relative_to(REPO_ROOT)} (generated)",
                    n=3,
                )
                print(f"[DRIFT] File out of date: {path.relative_to(REPO_ROOT)}", file=sys.stderr)
                sys.stderr.writelines(diff)
                has_drift = True

        # Check for unexpected files in destination directories
        for dest_dir in (OPENCODE_AGENTS_DIR, OPENCODE_COMMANDS_DIR):
            if dest_dir.exists():
                for actual_path in dest_dir.glob("*.md"):
                    if actual_path not in expected_files:
                        print(f"[DRIFT] Unexpected extra file: {actual_path.relative_to(REPO_ROOT)}", file=sys.stderr)
                        has_drift = True

        if has_drift:
            print("[FAIL] OpenCode configuration has drifted. Run: python scripts/generate-opencode.py", file=sys.stderr)
            return 1

        agent_count = sum(1 for p in expected_files if p.parent == OPENCODE_AGENTS_DIR)
        command_count = sum(1 for p in expected_files if p.parent == OPENCODE_COMMANDS_DIR)
        print(f"[OK] OpenCode configuration is up to date ({agent_count} agents, {command_count} commands)")
        return 0

    # Write files
    OPENCODE_AGENTS_DIR.mkdir(parents=True, exist_ok=True)
    OPENCODE_COMMANDS_DIR.mkdir(parents=True, exist_ok=True)

    agent_count = 0
    command_count = 0

    for path, content in expected_files.items():
        path.write_text(content, encoding="utf-8")
        if path.parent == OPENCODE_AGENTS_DIR:
            agent_count += 1
        elif path.parent == OPENCODE_COMMANDS_DIR:
            command_count += 1

    print(f"[OK] Generated {agent_count} OpenCode agents in .opencode/agents/")
    print(f"[OK] Generated {command_count} OpenCode commands in .opencode/commands/")
    return 0


if __name__ == "__main__":
    sys.exit(main())
