# Architecture

The Valkey CI Agent automates backport cherry-picks across Valkey repositories
defined in the central `repos.yml` registry.

## Layers

```text
scripts/
  backport/    Backport workflow (active)
  ai/          Claude Code subprocess orchestration
  common/      Shared infrastructure
repos.yml      Registry of repos, release branches, and project boards
```

## Backport Flow

```text
sweep.py (daily cron or manual dispatch)
  -> reads repos.yml and fans out one job per {repo, branch}
  -> discovers PRs from each branch's GitHub Project board
  -> for each registered release branch:
      cherry_pick.py -> git cherry-pick
      conflict_resolver.py -> Claude Code resolves conflicts
      pr_creator.py -> opens/updates PR on the upstream repo
```

### Poll

The daily sweep tops a rolling backport PR up to `--max-candidates` validated
cherry-picks and then waits for the next cron tick, so a merged sweep PR is not
topped back up until the following day. The poll workflow (`backport-poll.yml`)
closes that gap by running hourly. For each registered `{repo, branch}` it runs
the same sweep, but only when no sweep PR is currently open for that branch:

```text
poller.py (short cron or manual dispatch)
  -> reads repos.yml and fans out one job per {repo, branch}
  -> find_existing_pr(...) -> open sweep PR for this branch?
       yes -> skip; a human is reviewing it
       no  -> run_backport_sweep(...) opens a fresh PR
```

The open-PR check is the entire state model: a merge closes the sweep PR, the
next poll finds the gap and tops the board back up, and the new PR locks the
branch again until it too merges. The poll job shares the
`backport-sweep-{repo}-{branch}` concurrency group with the daily sweep so the
two never race for the same branch.

### Entry Points

- `scripts/backport/sweep.py` — daily sweep across registered repos and release branches
- `scripts/backport/poller.py` — short-cron poll that sweeps a branch only when no sweep PR is open
- `scripts/backport/main.py` — single-PR backport (manual dispatch)
- `scripts/backport/matrix.py` — GitHub Actions matrix generation from `repos.yml`
- `scripts/backport/registry.py` — typed registry loader and validation
- `scripts/backport/sweep_*.py` — focused sweep support modules:
  typed sweep results, Git workspace operations, GitHub PR operations,
  GraphQL access, validation command execution, and Markdown reporting

### AI Layer

The only AI usage is conflict resolution:

```text
conflict_resolver.py
  → runtime.run_agent("conflict_resolve_edit_only", prompt, cwd=repo)
    → claude_code.run_claude_code(prompt, ...)
      → subprocess: claude --print (Claude Code CLI via Bedrock)
```

Claude gets the repo checkout with conflict markers, reads both sides, and edits
only the conflicted files in place. The prompt is parameterized by the repo
language from `repos.yml`.

Validation first runs the registry's optional `validation_setup_commands`,
then validates the branch after each cherry-pick. The sweep branch is kept
green: a cherry-pick is only kept if the whole branch still validates, and a
failure is reset off the branch so it can never block later candidates. The
run keeps a single validated cherry-pick (`--max-candidates 1`) and records
skipped or failed candidates in the PR's "Needs attention" section without
committing them. When `repair_validation_failures` is enabled, Claude Code
gets one edit-only repair attempt scoped to the backport diff before a failing
cherry-pick is dropped. Repos with no `build_commands` configured rely on
upstream CI for verification.

### Common Infrastructure

- `git_auth.py` — GIT_ASKPASS credential helper
- `github_client.py` — retry wrapper for GitHub API

## Repository Model

The standard model is direct upstream branches: the agent pushes
`agent/backport/...` branches to `repo` and opens PRs in that same
repository. This keeps the registry small and matches the GitHub App
permissions used by the workflows.

`push_repo` is optional and exists only as a different-owner fork escape hatch.
Same-owner `push_repo` values are rejected so staging repositories do not become
the normal deployment model.

## Planned Workflows

Future sibling modules to `backport/`:

- **PR Reviewer** — two-stage code review with skeptic pass
- **Fuzzer Monitor** — triage fuzzer failures, file issues
- **Daily CI Analysis** — detect flaky tests, generate fix PRs
