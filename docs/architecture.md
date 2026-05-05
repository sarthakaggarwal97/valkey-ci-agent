# Architecture

The Valkey CI Agent automates backport cherry-picks across release branches.

## Layers

```
scripts/
  backport/    Backport workflow (active)
  ai/          Claude Code subprocess orchestration
  common/      Shared infrastructure
```

## Backport Flow

```
sweep.py (weekly cron or manual dispatch)
  → discovers PRs from GitHub Project boards
  → for each release branch:
      cherry_pick.py  → git cherry-pick
      conflict_resolver.py → Claude Code resolves conflicts
      pr_creator.py → opens/updates draft PR on push repo
```

### Entry Points

- `scripts/backport/sweep.py` — weekly sweep across all release branches
- `scripts/backport/main.py` — single-PR backport (manual dispatch)

### AI Layer

The only AI usage is conflict resolution:

```
conflict_resolver.py
  → runtime.run_agent("conflict_resolve_edit_only", prompt, cwd=repo)
    → claude_code.run_claude_code(prompt, ...)
      → subprocess: claude --print (Claude Code CLI via Bedrock)
```

Claude gets the repo checkout with conflict markers, reads both sides,
edits files in place, and runs `make` to verify compilation.

### Common Infrastructure

- `git_auth.py` — GIT_ASKPASS credential helper
- `github_client.py` — retry wrapper for GitHub API
- `publish_guard.py` — blocks accidental writes to upstream repos
- `commit_signoff.py` — DCO sign-off handling

## Planned Workflows

Future sibling modules to `backport/`:

- **PR Reviewer** — two-stage code review with skeptic pass
- **Fuzzer Monitor** — triage fuzzer failures, file issues
- **Daily CI Analysis** — detect flaky tests, generate fix PRs
- **Health Dashboard** — publish CI metrics to GitHub Pages
