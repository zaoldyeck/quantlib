# Codex Persistence

This directory contains Codex-facing repository configuration.

## Source Of Truth

- Repository rules: `AGENTS.md`
- Codex agents: `.codex/agents/*.toml`
- Codex skills: `.agents/skills/*/SKILL.md`
- Durable project facts: `docs/`, `research/trading/strategy_registry.py`, and
  `research/strat_lab/results/`

Claude-specific files are intentionally not used as Codex policy.

## Commands

There is no separate Codex command layer in this repository. Reusable operations
should be implemented as package-manager commands, scripts, skills, or agents.
Do not mirror `.claude/commands/` into Codex unless the repository adopts a
real Codex command mechanism.

## Memory

External Codex memory is read-only context unless the user explicitly asks to
remember something. Strategy research, broker state, validation records, and
data-audit conclusions should be written to repository artifacts instead of
non-existent external `project_*.md` memory files.

## Validation

After editing Codex persistence:

```bash
uv run --project research python - <<'PY'
from pathlib import Path
import tomllib
for path in sorted(Path('.codex/agents').glob('*.toml')):
    with path.open('rb') as f:
        tomllib.load(f)
    print(f'OK {path}')
PY

rg -n '(~/.Codex|project_[A-Za-z0-9_]+\.md|uv run python|shioaji|Sinotrade)' AGENTS.md .agents .codex/agents
```
