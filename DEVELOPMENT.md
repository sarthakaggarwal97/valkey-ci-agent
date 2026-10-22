# Development

Local setup, tests, and how to change the agent. For what the workflows do
and how to run them, see the [docs](docs/) and [README](README.md).

## Prerequisites

- Python 3.9 or later. CI runs 3.9 and 3.11; 3.11 is what the lint, coverage
  and type-check jobs use.
- GitHub CLI (`gh`) to dispatch workflows.
- A GitHub token for API-backed local runs.
- AWS credentials with Bedrock access for anything that calls Claude Code.
- Claude Code CLI 2.1.170 or later for AI-backed flows locally (the workflows
  pin 2.1.175):

```bash
npm install -g @anthropic-ai/claude-code@2.1.175
```

## Setup

```bash
python -m pip install -e ".[dev]"
```

This installs the runtime dependencies from `pyproject.toml` plus the test
and lint tooling. `python -m pip install .` is the runtime-only install.

For API-backed runs, copy the environment template, fill in what you need,
and source it:

```bash
cp .env.example .env.local
```

```bash
set -a; source .env.local; set +a
```

Prefer `AWS_PROFILE` plus `AWS_REGION` locally. In GitHub Actions, AWS access
is OIDC through the `AWS_ROLE_ARN` secret ([docs/setup.md](docs/setup.md)).

## Tests and checks

```bash
python -m pytest
```

What CI runs:

```bash
ruff check scripts/ tests/
pytest -v
pytest --cov=scripts --cov-report=term-missing --cov-report=xml --cov-fail-under=60 -v
mypy scripts/
```

`ci.yml` runs on pushes and PRs to `main`. `pytest -v` runs on 3.9; ruff,
the coverage run and mypy run on 3.11. The 3.11 leg also installs the pinned
Claude Code CLI, creates a `ci-fix-agent` account and sets
`CI_FIX_TEST_AGENT_USER` and `REQUIRE_CLAUDE_CLI`, so the isolation and
confinement tests run for real; a separate `isolation-as-root` job runs
`tests/test_isolation.py` as root in an Alpine container. Without that
account and CLI those tests skip locally.

Tests live in `tests/`, one file per module (`tests/test_ci_fix_engine.py`
for `scripts/ci_fix/engine.py`), with shared git helpers in
`tests/git_helpers.py` and fixtures under `tests/fixtures/`.

## Commits

All commits need a DCO sign-off:

```bash
git commit -s
```

## Running pieces locally

Each workflow doc has a "Running it" section with the `gh workflow run`
dispatches and the local commands for that family:

- [Backport](docs/backport.md#running-it): matrix generation, sweep
  discovery, mark-done preview, eligibility, single-PR backport
- [CI fix](docs/ci-fix.md#running-it)
- [Test Failure Detector](docs/test-failure-detector.md#running-it)
- [Fuzzer monitor](docs/fuzzer-monitor.md#running-it)
- [Release notes](docs/release-notes.md#recipes)
- [CVE scan](docs/cve-scan.md#running-it)

Commands that reach a push or PR creation are write-capable. Run them only
against an intended target repository and token; most have a `--dry-run` or
`--discover-only` flag, and manual workflow dispatches default to a dry run
where one exists.

## Changing the registry

`repos.yml` is the source of truth for onboarded repositories, release
branches, project boards, labels and validation commands. The keys and their
semantics are in [docs/registry.md](docs/registry.md);
`examples/repos.yml` is a multi-repository reference. Check matrix generation
after an edit:

```bash
python -m scripts.backport.matrix --registry repos.yml
```

## Adding a workflow

1. Add a sibling package under `scripts/` with a `main.py` entry point. Keep
   the front door (authorize, bind to exact evidence, produce a typed task),
   the engine, and the publisher separate; see
   [docs/architecture.md](docs/architecture.md#the-shape-of-a-workflow).
2. Reuse `scripts/common/` for git auth, clones, GitHub API retries,
   artifacts, issue dedup and polling rather than reimplementing them.
3. If it calls the model, add an agent profile in `scripts/ai/` with the
   narrowest tool set that works, and write the prompt in the new package.
   Never pass a GitHub token to the subprocess.
4. Add a workflow under `.github/workflows/` that checks out this repository
   and uses `.github/actions/setup-agent`. Pin every external action by SHA
   and request only the permissions the job needs; the workflow-standards
   tests enforce both.
5. Write only inside the bot's branch namespaces, revalidate the destination
   right before pushing, and leave merging to humans.
6. Add tests, a `docs/<workflow>.md` following the existing layout (what it
   does, workflows table, how it works, running it, configuration,
   internals), and a row in the README's workflow table.
