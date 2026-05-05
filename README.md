# valkey-ci-agent

An AI-powered CI automation agent for the Valkey project. Uses Claude Code (Anthropic Claude Opus via Bedrock) to perform tasks that require code understanding — conflict resolution, code review, failure analysis, and more.

## Architecture

The agent is structured as a layered framework:

```
scripts/
  ai/          AI layer: Claude Code subprocess orchestration
  backport/    Workflow 1: automated backports (active)
  common/      Shared infrastructure (git auth, GitHub client, safety guards)
```

New workflows are added as sibling directories to `backport/`. Each workflow picks an agent profile (tools, timeout, effort) and writes its own prompt. The AI layer and shared infra stay unchanged.

**Workflows:**

| Workflow | Status | Description |
|----------|--------|-------------|
| Backport | Active | Cherry-picks merged PRs onto release branches with AI conflict resolution |
| PR Reviewer | Planned | Two-stage code review with skeptic pass |
| Fuzzer Monitor | Planned | Analyzes fuzzer runs, triages failures, files issues |
| Daily CI Analysis | Planned | Detects flaky tests, generates fix PRs |
| Health Dashboard | Planned | Publishes CI health metrics to GitHub Pages |

## Backport Workflow

The currently active workflow. Cherry-picks merged PRs from `unstable` onto release branches (7.2, 8.0, 8.1, 9.0, 9.1) with AI-powered conflict resolution.

### How it works

1. **Weekly sweep** — every Monday at 09:00 UTC, queries GitHub Project v2 boards for PRs marked "To be backported"
2. **Cherry-pick** — attempts `git cherry-pick` for each candidate onto the target release branch
3. **AI conflict resolution** — when cherry-pick conflicts, Claude Code reads both sides, resolves the conflict, and runs `make -j$(nproc)` to verify compilation
4. **PR creation** — pushes the branch and opens (or updates) a draft PR with a summary table of applied/skipped commits

Manual single-PR backports are also supported via `workflow_dispatch`.

### Installation

#### Prerequisites

- A GitHub App (or PAT) with:
  - `contents:write` on the push target (fork or same repo)
  - `pull_requests:write` on the upstream repo (to open PRs)
  - `projects:read` on the org (to query project boards)
- An AWS account with Bedrock access to `us.anthropic.claude-opus-4-7`
- An OIDC trust between GitHub Actions and your AWS account

#### Step 1: Set up the push target

Create a fork of `valkey-io/valkey` for the agent to push branches to (e.g., `valkey-io/valkey-ci-agent-fork`). Or use the same repo if the App has `contents:write` on upstream directly.

#### Step 2: Configure secrets and variables

On the repo hosting the agent workflows:

| Type | Name | Value |
|------|------|-------|
| Secret | `AWS_ROLE_ARN` | OIDC role ARN with Bedrock `InvokeModel` permission |
| Secret | `VALKEY_GITHUB_TOKEN` | PAT or App installation token with `read:project` + `contents:write` + `pull_requests:write` |
| Variable | `VALKEY_BACKPORT_PUSH_REPO` | e.g., `valkey-io/valkey-ci-agent-fork` |

#### Step 3: Copy the caller workflow

Copy [`examples/backport-caller-workflow.yml`](examples/backport-caller-workflow.yml) into your repo's `.github/workflows/` directory. It calls the reusable `weekly-backport-sweep.yml` from this repo.

#### Step 4: (Optional) Backport config

Copy [`examples/backport-config.yml`](examples/backport-config.yml) to `.github/backport-agent.yml` in the target repo to customize labels and conflict limits.

### Usage

#### Weekly sweep (automatic)

Runs every Monday via cron. Sweeps all 5 release-branch project boards in parallel:

| Project | Branch |
|---------|--------|
| 1 | 7.2 |
| 2 | 8.0 |
| 14 | 8.1 |
| 18 | 9.0 |
| 41 | 9.1 |

Each leg produces one PR (e.g., `[backport] Weekly backport sweep for 8.1`) bundling all pending backports for that branch.

#### Manual backport (on-demand)

```bash
gh workflow run manual-backport.yml \
  --repo <agent-repo> \
  --field pr_url=https://github.com/valkey-io/valkey/pull/3601 \
  --field target_branch=9.0 \
  --field push_to_fork=<push-repo>
```

Creates one PR per source PR, named `[Backport 9.0] <original title>`.

### Configuration

See [`examples/backport-config.yml`](examples/backport-config.yml) for all available fields.

| Field | Default | Description |
|-------|---------|-------------|
| `backport_label` | `backport` | Label applied to created PRs |
| `llm_conflict_label` | `llm-resolved-conflicts` | Label when Claude resolved conflicts |
| `max_conflicting_files` | 100 | Skip if more files conflict than this |

## Safety

- **Publish guard** — blocks writes to `valkey-io/valkey` unless `VALKEY_CI_AGENT_ALLOW_VALKEY_IO_PUBLISH=1` is explicitly set
- **Credential isolation** — all GitHub auth uses `GIT_ASKPASS`; tokens never appear in `.git/config` or URLs
- **Claude Code env isolation** — `GITHUB_TOKEN`, `GH_TOKEN`, and `*_SECRET` are stripped from the subprocess environment. Claude cannot see credentials.
- **Fork sync** — before cherry-picking, the agent fast-forwards the fork's release branch to match upstream
- **Stale branch pruning** — if a previous backport PR was closed without merging, the agent deletes the orphaned branch before starting fresh
- **DCO** — all agent commits are signed off

## Documentation

- [docs/architecture.md](docs/architecture.md) — full system design including planned workflows
- [CONTRIBUTING.md](CONTRIBUTING.md) — development setup and code structure
- [AGENTS.md](AGENTS.md) — coding guidelines for LLM-generated code
