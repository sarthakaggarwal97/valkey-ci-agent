# Development Guide

This guide covers local setup, local validation, and the GitHub Actions workflows
used to test and run `valkey-ci-agent`. The workflow examples focus on the
backport subsystem; the ci-fix, fuzzer-monitor, and test-failure-detector
subsystems follow the same local setup and validation steps.

For how the system is designed — the layers, per-workflow flows, and shared
infrastructure — see [docs/architecture.md](docs/architecture.md).

## Prerequisites

- Python 3.9 through 3.11. CI tests the oldest and newest supported minor
  versions; use 3.11 locally to match the lint, coverage, type, and primary
  audit jobs.
- GitHub CLI (`gh`) for dispatching workflows.
- A GitHub token for local API-backed runs.
- Access to the configured Anthropic-compatible model gateway when exercising
  AI-backed workflows.
- Node.js and npm for rebuilding the locked Claude runtime.
- Docker for the isolated AI runtime and typed validation adapters.

```bash
npm ci --prefix .github/actions/setup-agent/claude --no-audit --no-fund
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

AI-backed workflow tests require `AI_GATEWAY_UPSTREAM_URL` and a narrow
`AI_GATEWAY_TOKEN`; ordinary unit tests do not.

## Install Dependencies

CI installs the hash-locked files under `requirements/`. For an exact local
development environment, use:

```bash
python -m pip install --require-hashes -r requirements/dev.txt
python -m pip install --no-deps --no-build-isolation -e .
```

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
# Python 3.11
pip-audit --requirement requirements/runtime.txt
```

CI runs the full test suite and dependency audit on Python 3.9 and 3.11. Lint,
coverage, and type checking run on Python 3.11.

The fixed Requests and urllib3 releases no longer support Python 3.9. For that
runtime, the package applies the reviewed upstream fixes in
`scripts/common/python39_http_hardening.py`. Audit the conditional lock while
acknowledging only those exact backports:

```bash
pip-audit --requirement requirements/runtime.txt \
  --ignore-vuln PYSEC-2026-2275 \
  --ignore-vuln PYSEC-2026-141 \
  --ignore-vuln PYSEC-2026-142
```

The Python 3.9 CI leg runs this audit and the hardening regression tests. The
exceptions suppress package-version metadata findings, not unmitigated code.

Dependency pull requests additionally run GitHub's dependency-review action.
It blocks newly introduced vulnerabilities rated moderate or higher and rejects
AGPL or GPL 2/3 dependencies. Apache-2.0, permissive, weak-copyleft, and
separately reviewed proprietary tool dependencies remain eligible. Runtime and
development Python dependencies must stay hash-locked, and the Claude runtime
must stay version- and integrity-locked through `npm ci`.

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

To test candidate discovery locally without cherry-picking or pushing:

```bash
DISCOVERY_GITHUB_TOKEN="$GITHUB_TOKEN" python -m scripts.backport.candidate_matrix \
  --registry repos.yml \
  --repo valkey-io/valkey \
  --project-number 18 \
  --max-candidates 2 \
  --output /tmp/backport-candidates.json
```

The former operator module paths remain available as phase routers. Each
invocation performs exactly one trust-domain phase; no local command combines
AI or target-code execution with a publisher token:

```bash
python -m scripts.backport.main --help
python -m scripts.backport.sweep --help
python -m scripts.backport.poller --help
python -m scripts.ci_fix.main --help
python -m scripts.fuzzer.main --help
```

Use the phased GitHub workflow when one dispatch should run the complete
write-capable operation.

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

`.github/workflows/ci.yml` runs on pushes and pull requests to `main` with
Python 3.9 and 3.11. The `setup-agent` action installs the matching
hash-locked development set. Python 3.9 runs the full unit suite; Python 3.11
runs:

```bash
ruff check scripts/ tests/
pytest -v
pytest --cov=scripts --cov-report=term-missing --cov-report=xml --cov-fail-under=60 -v
mypy scripts/
```

### Required Secrets and Variables

Operational workflows require these values in the `valkey-ci-agent` repository:

| Type | Name | Purpose |
|------|------|---------|
| Secret | `AI_GATEWAY_TOKEN` | Narrow token accepted by the configured model gateway |
| Secret | `VALKEYRIE_BOT_APP_ID` | Valkeyrie GitHub App ID |
| Secret | `VALKEYRIE_BOT_PRIVATE_KEY` | Valkeyrie GitHub App private key |
| Variable | `AI_GATEWAY_UPSTREAM_URL` | HTTPS origin of the Anthropic-compatible model gateway |
| Variable | `VALKEY_CI_AGENT_KILL_SWITCH` | Set to `true` to stop every operational workflow before token minting |
| Variable | `VALKEY_CI_AGENT_DISABLED_REPOSITORIES` | Optional comma-separated `owner/name` disable list |
| Variable | `VALKEY_CI_AGENT_DISABLE_BACKPORT` | Set to `true` to stop backport discovery, AI, validation, and publishing |
| Variable | `VALKEY_CI_AGENT_DISABLE_CI_FIX` | Set to `true` to stop CI-fix polling, verification, and publishing |
| Variable | `VALKEY_CI_AGENT_DISABLE_FUZZER` | Set to `true` to stop fuzzer analysis and publishing |
| Variable | `VALKEY_CI_AGENT_DISABLE_TEST_FAILURE_DETECTOR` | Set to `true` to stop test-failure issue publishing |
| Variable | `VALKEY_CI_AGENT_DISABLE_METADATA_RECONCILER` | Set to `true` to stop label and desired-comment reconciliation |

The workflows mint a short-lived GitHub App installation token for repository
reads, branch pushes, PR creation, status comments, and project-board queries.

The model gateway is part of the enforcement boundary, not a transparent
Anthropic proxy. It must implement `POST /v1/controls/admit`, atomically account
by the supplied repository and policy digest, and deny requests that exceed the
daily token/cost budget, queue depth, publication budget, or open circuit.
`scripts/ai/gateway_proxy.py` fails closed if this admission response is absent
or invalid. Repository defaults live in the strict `automation` block in
`repos.yml`; fuzzer monitoring uses the same bounded defaults under its own
repository identity.

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

`repos.yml` is the source of truth for repositories, release branches, project
boards, labels, operational limits, and typed validation adapters. The strict
schema rejects unknown keys. Each repository must define bounded automation
budgets and either a digest-pinned `container-argv-v1` or
`container-argv-v2` adapter, or an approved, expiring `validation_waiver`;
silent validation skips are not supported. V2 adds size- and SHA-256-pinned
GitHub release assets, extracted without root and mounted read-only into the
otherwise networkless validation container. Use `examples/repos.yml` as a
reference. Set `repair_validation_failures: true` to preserve one edit-only,
path-scoped repair attempt after a candidate validation failure. Repair output
must pass a fresh credentialless validation job before publication.

Before enabling a new or changed entry, run the live onboarding preflight with
an App installation token carrying the complete production permission grant:

```bash
REGISTRY_PREFLIGHT_GITHUB_TOKEN="$GITHUB_TOKEN" \
  python -m scripts.backport.registry_preflight \
  --registry repos.yml \
  --repo valkey-io/valkey
```

The preflight resolves the source and optional push repositories, every release
branch and project, the required Status options and labels, each validation
image digest, and the effective App permissions. It is read-only and fails
closed on missing or truncated state.
