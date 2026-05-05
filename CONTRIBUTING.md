# Contributing to valkey-ci-agent

Thanks for your interest in contributing. This repo is an AI agent that
automates backporting merged PRs onto Valkey release branches using
Claude Code for conflict resolution.

## Development setup

```bash
pip install -r requirements.txt
pip install -r requirements-dev.txt
```

## Running tests

```bash
pytest --cov=scripts -v
```

## Linting

```bash
ruff check scripts/ tests/
mypy scripts/ --ignore-missing-imports
```

## Code structure

- `scripts/backport/` — core backport pipeline (main, sweep, config, models, risk, pr_creator, utils)
- `scripts/backport/cherry_pick.py` — low-level git cherry-pick executor
- `scripts/backport/conflict_resolver.py` — AI conflict resolution via Claude Code
- `scripts/ai/runtime.py` → `claude_code.py` → `claude_workspace.py` — Claude Code subprocess management
- `scripts/common/` — shared infrastructure (config, git_auth, github_client, publish_guard, commit_signoff)

## Commits

All commits must include a DCO sign-off:

```bash
git commit -s -m "your message"
```
