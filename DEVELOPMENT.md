# Development Guide

This guide covers local setup, local validation, and the GitHub Actions workflows
used to test and run `valkey-ci-agent`. It includes registry and verifier
onboarding for the CI-fix subsystem.

For how the system is designed (the layers, per-workflow flows, and shared
infrastructure), see [docs/architecture.md](docs/architecture.md).

## Prerequisites

- Python 3.9 or later. CI runs a 3.9 and 3.11 matrix, so 3.11 is the closest
  match for the lint, coverage, and type-check jobs.
- GitHub CLI (`gh`) for dispatching workflows.
- A GitHub token for local API-backed runs.
- AWS credentials with Bedrock access when running workflows that invoke Claude
  Code through Bedrock.
- Claude Code CLI 2.1.170 or later when running AI-backed flows locally. The
  workflows pin 2.1.175 because the default model is Fable 5:

```bash
npm install -g @anthropic-ai/claude-code@2.1.175
```

## Local Environment

Create a local environment file from the template:

```bash
cp .env.example .env.local
```

Fill in the values you need, then source it before API-backed runs:

```bash
set -a
source .env.local
set +a
```

For local development, prefer `AWS_PROFILE` plus `AWS_REGION`. In GitHub
Actions, AWS access is configured with OIDC and the `AWS_ROLE_ARN` secret.

## Install Dependencies

Install the package and development tools in editable mode:

```bash
python -m pip install -e ".[dev]"
```

This pulls in the runtime dependencies declared in `pyproject.toml` plus the
test and lint tooling. For a runtime-only install, use `python -m pip install .`.

## Test Locally

Run the unit test suite:

```bash
python -m pytest
```

Run the same validation commands used by CI:

```bash
ruff check scripts/ tests/
pytest -v
pytest --cov=scripts --cov-report=term-missing --cov-report=xml --cov-fail-under=60 -v
mypy scripts/
```

CI splits these across its matrix: `pytest -v` runs on 3.9, while ruff, the
coverage run, and mypy run on 3.11.

For a focused CI-fix iteration:

```bash
python -m pytest -q \
  tests/test_ci_fix_registry.py \
  tests/test_ci_fix_pipeline.py \
  tests/test_ci_fix_review.py \
  tests/test_ci_fix_verify_linux.py \
  tests/test_ci_fix_verify_target_workflow.py \
  tests/test_ci_fix_verify_macos.py \
  tests/test_ci_fix_workflow_env.py
```

Resolve the live CI-fix policy without minting a token or dispatching a
workflow:

```bash
python -m scripts.ci_fix.registry resolve \
  --registry repos.yml \
  --repo valkey-io/valkey

python -m scripts.ci_fix.registry poll-matrix --registry repos.yml
```

## Commit Requirements

All commits require DCO sign-off:

```bash
git commit -s
```

To check registry matrix generation without running a workflow:

```bash
python -m scripts.backport.matrix --registry repos.yml
python -m scripts.backport.matrix --registry repos.yml --repo valkey-io/valkey
python -m scripts.backport.matrix --registry repos.yml --repo valkey-io/valkey --project-number 14
```

To test backport sweep discovery locally without cherry-picking or pushing:

```bash
python -m scripts.backport.sweep \
  --registry repos.yml \
  --repo valkey-io/valkey \
  --branch 9.0 \
  --target-token "$GITHUB_TOKEN" \
  --discover-only \
  --verbose
```

The single-PR backport command is write-capable when it reaches the push and PR
creation stages. Use it only with an intended target repository and token:

```bash
BACKPORT_GITHUB_TOKEN="$GITHUB_TOKEN" python -m scripts.backport.main \
  --registry repos.yml \
  --repo valkey-io/valkey \
  --pr-number 3601 \
  --target-branch 9.0 \
  --verbose
```

To reconcile a project board against branch reality without mutating it, run the
mark-done step in dry-run mode. Omit `--target-branch` to reconcile every branch
configured for the repo:

```bash
python -m scripts.backport.mark_done \
  --registry repos.yml \
  --repo valkey-io/valkey \
  --target-branch 9.0 \
  --target-token "$GITHUB_TOKEN" \
  --dry-run \
  --verbose
```

Drop `--dry-run` to move items from "To be backported" to "Done" once the
backport has actually landed on the target branch.

## GitHub Actions

### CI

`.github/workflows/ci.yml` runs on pushes and pull requests to `main`, across a
Python 3.9 and 3.11 matrix. It installs dev dependencies through the
`setup-agent` action (equivalent to `python -m pip install -e ".[dev]"`) and
runs, depending on the Python version:

```bash
ruff check scripts/ tests/
pytest -v
pytest --cov=scripts --cov-report=term-missing --cov-report=xml --cov-fail-under=60 -v
mypy scripts/
```

The coverage run and the lint/type checks run on 3.11; the plain `pytest -v` run
covers 3.9.

### Required Secrets and Variables

Operational workflows require these values in the `valkey-ci-agent` repository:

| Type | Name | Purpose |
|------|------|---------|
| Secret | `AWS_ROLE_ARN` | OIDC role ARN with Bedrock `InvokeModel` permission |
| Secret | `VALKEYRIE_BOT_APP_ID` | Valkeyrie GitHub App ID |
| Secret | `VALKEYRIE_BOT_PRIVATE_KEY` | Valkeyrie GitHub App private key |
| Variable | `AWS_REGION` | AWS region, for example `us-east-1` |

Claude Code defaults to Fable 5 through the `us.anthropic.claude-fable-5`
Bedrock inference profile. The AWS account must have access to that profile and
must use Bedrock `provider_data_share` data retention; Fable 5 is not available
under zero data retention. Set `CI_AGENT_CLAUDE_MODEL=opus` to temporarily fall
back to the pinned Opus 4.8 profile.

The workflows mint a short-lived GitHub App installation token for repository
reads, branch pushes, PR creation, status comments, and project-board queries.

### Manual Backport

Run a single-PR backport with `workflow_dispatch`:

```bash
gh workflow run manual-backport.yml \
  --repo valkey-io/valkey-ci-agent \
  --field pr_url=https://github.com/valkey-io/valkey/pull/3601 \
  --field target_branch=9.0
```

### Backport Sweep

The scheduled sweep runs daily at 09:00 UTC. To run it manually in discovery
mode, leave `dry_run` enabled:

```bash
gh workflow run backport-sweep.yml \
  --repo valkey-io/valkey-ci-agent \
  --field repo=valkey-io/valkey \
  --field project_number=14 \
  --field dry_run=true
```

To allow cherry-picks, pushes, and PR updates, dispatch the same workflow with
`dry_run=false` after confirming the target repository and registry entry are
correct.

Both backport workflows upload result JSON and `agent-evidence` artifacts when
available.

### CI Fix

Dispatch one registered repository and failing run:

```bash
gh workflow run ci-fix.yml \
  --repo valkey-io/valkey-ci-agent \
  --field repo=valkey-io/valkey \
  --field pr=<pr-number> \
  --field run_url=https://github.com/valkey-io/valkey/actions/runs/<run-id>
```

The linked run must be complete and must match the PR's current head SHA and
branch. The requesting actor must satisfy the repository's configured
organization/team policy. `ci-fix-comment-poll.yml` provides the equivalent
comment-driven path for entries with `poll_comments: true`.

Remote verifier load and wall time are repository policy:
`remote_parallelism` limits concurrent samples,
`remote_sample_timeout_minutes` limits one sample, and
`remote_budget_minutes` limits the complete remote campaign. Budget exhaustion
returns a reviewed handoff. Production passes the App installation ids and key
to the control plane so API, artifact, and final push credentials can refresh
without broadening beyond the registry-selected repository.

Do not test a new target verifier by pointing `verification_ref` at a candidate
branch. Install the workflow on the target's default branch, put the trusted
implementation on a protected ref, validate its input and credential policy,
then add both registry fields in one change. The complete input/run-name
contract and a safety checklist are in
[docs/ci-fix-verifier.md](docs/ci-fix-verifier.md).

### Mark Merged Backports Done

`.github/workflows/backport-mark-done-poll.yml` runs hourly (cron `30 * * * *`)
and reconciles each project board against branch reality, moving items to "Done"
once the backport has actually landed on the target branch. It is self-healing:
it reconciles the whole board every run, so it picks up backports applied by the
sweep, by an earlier run, or by a manual cherry-pick.

Dispatch it manually, optionally filtering to a single repo or project. This
workflow always reconciles the live board; to preview changes first, use the
local `scripts.backport.mark_done` command with `--dry-run` shown above.

```bash
gh workflow run backport-mark-done-poll.yml \
  --repo valkey-io/valkey-ci-agent \
  --field repo=valkey-io/valkey
```

## Registry Changes

`repos.yml` is the source of truth for onboarded repositories, release branches,
project boards, labels, validation commands, and CI-fix policy. Use
`examples/repos.yml` as a multi-repository reference.

Registry-configured `build_commands` run before pushing a generated backport
branch. A non-zero exit blocks the push. Repositories with no `build_commands`
configured rely on their upstream CI for validation.

To onboard CI Fix:

1. Add `ci_fix.enabled`, authorization, and at least one
   `allowed_branch_prefixes` value.
2. Add maintained release lines to `history_branches` only as a rare fallback.
   Discovery returns default-branch candidates first and does not inspect these
   branches unless the default branch yields none.
3. Set `baseline_runs`, `flaky_verify_runs`, and `minimum_confidence`.
4. Restrict `auto_publish_paths` to test/scaffolding ownership and retain
   workflow, local-action, and CODEOWNERS patterns in `protected_paths`.
5. Set `poll_comments: true` only after the App is installed on the target.
6. For exact environment coverage, install the protected target workflow and
   set `verification_workflow` plus `verification_ref` together.

The target App installation needs `actions:write` only when exact verification
is enabled. CI Fix never requests workflow-file write permission. Without a
target verifier, unsupported environments produce reviewed handoff patches
instead of early refusals or approximate passes.
