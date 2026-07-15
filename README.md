# valkey-ci-agent

An AI-powered CI automation agent for the Valkey project. It runs Claude Code
through a narrow model gateway for conflict resolution, code review, and failure
analysis while deterministic code owns validation and publication decisions.

## Architecture

The agent is structured as a layered framework:

```text
scripts/
  ai/          AI layer: Claude Code subprocess orchestration
  backport/    Automated backports (active)
  fuzzer/      Fuzzer run monitoring (active)
  ci_fix/      On-demand CI test-fix bot (active)
  common/      Shared infrastructure (git auth, GitHub client, safety guards)
.github/actions/setup-agent
              Shared hash-locked Python 3.9/3.11 setup
.github/actions/setup-ai-runtime
              Isolated Claude tool container and credential gateway
repos.yml      Central registry of repos, branches, and project boards
```

New workflows are added as sibling directories to `backport/`. Each workflow picks an agent profile (tools, timeout, effort) and writes its own prompt. The AI layer and shared infra stay unchanged.

**Workflows:**

| Workflow | Status | Description |
|----------|--------|-------------|
| Backport | Active | Cherry-picks merged PRs onto release branches with AI conflict resolution |
| Fuzzer Monitor | Active | Analyzes scheduled fuzzer runs and files issues for anomalous failures |
| CI Fix | Active | On-demand `@valkeyrie-bot fix <ci-link>` - diagnoses and fixes a failing test on a backport PR |
| Test Failure Detector | Active | Detects test failures from Daily CI, files/updates GitHub issues |
| PR Reviewer | Planned | Two-stage code review with skeptic pass |
| Additional Daily CI Analysis | Planned | Detects flaky tests, generates fix PRs |

## Backport Workflow

The currently active workflow. Cherry-picks merged PRs onto release branches with AI-powered conflict resolution. Works for any repo defined in `repos.yml` - Valkey core, Valkey modules (bloom, search, json), or anything else following the per-branch project-board pattern.

### How it works

1. **Candidate discovery** - a read-only job queries each Project v2 board and emits a bounded matrix of immutable PR identities.
2. **AI preparation** - one credentialless job per candidate cherry-picks onto the exact target SHA and resolves text conflicts inside the isolated AI runtime.
3. **Validation** - a separate credentialless job applies the content-addressed patch and runs the registry's typed container adapter.
4. **Repair** - when enabled, a failed candidate gets one credentialless, edit-only repair attempt restricted to the original changed paths, then full validation in a fresh job.
5. **Failure reporting** - refused or still-failing candidates produce a verified, idempotent “Backport needs attention” record on the source PR using a comment-only token.
6. **Preflight** - the publisher rechecks every manifest, policy digest, patch hash, source SHA, base SHA, and resulting tree before minting a write token.
7. **Publication** - a fresh clone applies the validated patch, records immutable provenance, and pushes one `agent/backport/...` branch for maintainer review.
8. **Status sync** - after merge, the Project v2 item is reconciled against immutable source-to-target provenance and target-branch ancestry.

Manual single-PR backports are also supported via `workflow_dispatch`.
Scheduled and polled runs retain one rolling
`agent/backport/sweep/<target-branch>` PR. Candidates are validated
individually, combined without credentials, and then the complete rolling tree
is validated again before that PR is created or updated.

### Registry (`repos.yml`)

The registry is the single source of truth. To onboard a new repo, add an entry to `repos.yml`:

```yaml
schema_version: 2
repos:
  - repo: valkey-io/valkey
    project_owner: valkey-io
    project_owner_type: organization
    language: c
    automation:
      enabled: true
      daily_ai_requests: 24
      daily_input_tokens: 2000000
      daily_output_tokens: 500000
      daily_cost_microusd: 20000000
      max_queue_depth: 4
      failure_threshold: 3
      circuit_cooldown_seconds: 21600
      max_publications_per_day: 20
      run_ai_requests: 8
    validation:
      adapter: container-argv-v1
      image: "gcc@sha256:5e927c284bf55a7dc796262e311a0703344f62f41f5621eb56843111b1d37e15"
      platform: linux/amd64
      network: none
      resources:
        cpus: 2
        memory_mb: 4096
        pids: 512
        output_bytes: 16777216
        tmpfs_mb: 512
      default_commands: [build]
      commands:
        - id: build
          argv: ["make", "-j2"]
          working_directory: "."
          timeout_seconds: 1800
          inputs: ["**"]
          expected_artifacts: ["src/valkey-server"]
      rules: []
    repair_validation_failures: true
    backport_label: backport
    llm_conflict_label: ai-resolved-conflicts
    max_conflicting_files: 100
    branches:
      - branch: "8.1"
        project_number: 14
      - branch: "9.0"
        project_number: 18
```

Run `python -m scripts.backport.registry_preflight` with a full App installation
token before enabling the entry. The command verifies the live repository,
branches, projects and Status options, labels, validation image digest, and App
permission contract.

By default, agent branches are pushed directly to `repo` under the `agent/backport/...` namespace and PRs are opened in that same upstream repository. `push_repo` is optional and only exists as an escape hatch for a real different-owner fork; same-owner `push_repo` values are rejected so staging repositories do not become the normal model.

Validation is mandatory. A repository must define a digest-pinned,
networkless typed adapter or an explicit, approved, expiring waiver. V2
adapters may also declare bounded HTTPS release assets whose exact size and
SHA-256 digest are policy-bound; they are unpacked without root and mounted
read-only. Arbitrary shell command fields and repository-built privileged
validation images are rejected by the versioned registry schema. Valkey Search
uses this path to keep backport automation enabled with build and unit-test
validation. `repair_validation_failures` preserves one bounded repair attempt;
the repaired tree must pass the same adapter in a fresh credentialless job.

See [`examples/repos.yml`](examples/repos.yml) for a multi-module example.

### Setup and Usage

See [DEVELOPMENT.md](DEVELOPMENT.md) for local setup, local validation commands,
required GitHub Actions secrets, and manual workflow dispatch examples.

## Fuzzer Monitor Workflow

The fuzzer monitor watches scheduled `valkey-io/valkey-fuzzer` workflow runs, analyzes their artifacts, and files issues for runs that look anomalous.

### How it works

1. **Cron** - every 4 hours, the monitor processes every scheduled run after its durable high-water mark
2. **Deterministic scan** - pattern-matches crash/sanitizer/failover/RDB signals against artifact JSON and node logs; ignores chaos-expected noise (CLUSTERDOWN, replication link loss)
3. **Claude Code analysis** - drops the artifacts in a tempdir, shallow-clones `valkey-io/valkey` at the tested commit and `valkey-io/valkey-fuzzer` at the run's HEAD, then asks Claude (with read-only `Read,Grep,Glob` tools) to correlate the failure with source and decide whether the run reflects a real bug or chaos-expected noise. If a clone fails the prompt tells Claude not to cite source line numbers.
4. **Issue upsert** - anomalous runs file (or update) an issue on `valkey-io/valkey-fuzzer`, deduplicated by a stable fingerprint over root cause and anomaly shape
5. **Audit** - per-run JSON results and Claude evidence are uploaded as workflow artifacts

The Claude container runs under the `fuzzer_analysis_readonly` profile with
`Read,Grep,Glob` only. Its checkout is read-only and its only network peer is
the internal model gateway; cloud and GitHub credentials stay outside the
container. Each cycle discovers at most four completed runs, and the durable
cursor advances only after each run's issue state is reconciled.

### Configuration

The monitor uses the shared `AI_GATEWAY_TOKEN` and Valkeyrie GitHub App
credentials documented in [DEVELOPMENT.md](DEVELOPMENT.md). The App needs
`actions:read`, `contents:read`, and `issues:write` on
`valkey-io/valkey-fuzzer`; each phase mints only the short-lived installation
token it needs.

### Manual run

```bash
# Run live against the latest scheduled fuzzer run (default)
gh workflow run monitor-fuzzer.yml --repo valkey-io/valkey-ci-agent

# Probe without invoking Claude or filing issues
gh workflow run monitor-fuzzer.yml \
  --repo valkey-io/valkey-ci-agent \
  --field dry_run=true
```

Scheduled runs always run live.

## Test Failure Detector

Monitors the Daily CI workflow on `valkey-io/valkey`, detects test failures, and automatically creates or updates GitHub issues to track them.

### How it works

The detector is a thin pipeline (`scripts/test_failure_detector/`) layered on shared building blocks in `scripts/common/` - `ArtifactClient` for artifact download and `IssueDedupPublisher` for issue dedup/publishing - so the same primitives back the Fuzzer Monitor.

1. **Daily sweep** - every day at 23:00 UTC the workflow runs on `valkey-io/valkey-ci-agent` and reads from `valkey-io/valkey`. Valkey Daily CI runs daily at 00:00 UTC, so this detector workflow will capture the current day's run. In case of a miss for any reason, manual dispatch functionality is available (more detail in 'Scheduled' section below).
2. **Find the run** - locates the most recent completed, non-cancelled/skipped Daily workflow run on the target branch (`unstable` by default), or uses a manually supplied run ID
3. **Download artifact** - `ArtifactClient` fetches the `all-test-failures` artifact, handling the auth-header-stripping redirect to Azure blob storage, transient-failure retries, and expired-artifact (404) cases
4. **Get job URLs** - fetches job metadata from the run to build CI links for each failure, with normalized name variants for fuzzy matching against artifact names
5. **Parse and deduplicate** - iterates the nested JSON (`{job -> suite -> [failures]}`) and groups by a `{test_name}::{test_file}` fingerprint, so a test failing across multiple jobs becomes one `UniqueFailure` with multiple job references
6. **Create or update issues** - `IssueDedupPublisher` upserts one issue per fingerprint, matching on a hidden body marker (`<!-- valkey-ci-agent:test-failure:{test_name}::{test_file} -->`) rather than the title. Per-failure rendering (title, body, recurrence comment, `test-failure` label) lives in `issue_renderer.py`. Each failure resolves to one of three outcomes:
   - **created** - no matching issue exists: opens one titled `[TEST-FAILURE] {test_name} in {test_file}` with the `test-failure` label, error trace, CI links, and environment list
   - **updated** - a matching issue exists: merges any new failing environments into the body and bumps the occurrence counter / adds a recurrence comment
   - **skipped** - the run ID matches the `last-key` marker already recorded on the issue, so a re-triggered sweep over the same CI run does not inflate the occurrence count or post a duplicate comment

A GitHub Actions job summary is emitted at every exit path with a table of metrics (failures detected, issues created/updated).

#### Prerequisites: Cross-repo Authentication

The workflow generates a GitHub App installation token scoped to the `valkey-io` org using the same App secrets as the backport workflow (`VALKEYRIE_BOT_APP_ID` + `VALKEYRIE_BOT_PRIVATE_KEY`). This token provides `actions:read` (to download artifacts) and `issues:write` (to create/update issues) on `valkey-io/valkey`.

### Usage

#### Scheduled (automatic)

Runs daily at 23:00 UTC via cron. The workflow runs on `valkey-io/valkey-ci-agent` and uses a GitHub App token to read artifacts from the most recent completed, non-cancelled/skipped Daily workflow run on the target branch (`unstable` by default), and write issues to `valkey-io/valkey`. Valkey Daily CI runs daily at 00:00 UTC, with runs typically completing within 4-7 hours, with slight exception (from valkey-io/valkey's history of 411 completed runs, 6 runs exceed 7 hours, with the longest lasting 10h 02m), such that Test Failure Detector should always capture the current day's workflow (workflow completes within seconds). In the event of a missed run, the current detector includes manual dispatch functionality, targeting a given run ID. Manual dispatches can be performed so long as Daily CI artifacts persist, currently set at 30 days.

#### Manual dispatch

```bash
gh workflow run test-failure-detector-sweep.yml \
  --repo valkey-io/valkey-ci-agent \
  --field repo=valkey-io/valkey \
  --field run_id=12345678 \
  --field dry_run=true
```

- `repo` - target repository to scan (default: `valkey-io/valkey`)
- `run_id` - specific workflow run ID to analyze (empty = latest Daily run)
- `dry_run` - parse and report only, don't create/update issues

## CI Fix Workflow

An on-demand workflow that fixes a single failing test on a backport PR when a
maintainer asks for it. From this agent repository, run it explicitly:

```bash
gh workflow run ci-fix.yml \
  --repo valkey-io/valkey-ci-agent \
  --field repo=valkey-io/valkey \
  --field pr=<pr-number> \
  --field run_url=https://github.com/valkey-io/valkey/actions/runs/<run_id> \
  --field correlation_id="$(openssl rand -hex 16)"
```

The workflow is scoped to `valkey-io/valkey`, matching the GitHub App token it
mints. Maintainers can dispatch it manually, or comment on a `valkey-io/valkey`
PR and let `ci-fix-comment-poll.yml` dispatch it. The invocation must start the
comment, and the hint is only the rest of that line, so a conversational comment
that merely quotes or mentions the command does not trigger a run. The intended
comment shape is:

```text
@valkeyrie-bot fix https://github.com/valkey-io/valkey/actions/runs/<run_id>
```

Add a free-text hint via the dispatch `hint` input to steer the diagnosis
(e.g. `look at the valgrind timeout`). The bot fixes one test per invocation;
re-run it to address the next failing test in the same run.

### How it works

The division of labor is the whole design: **AI judges, code executes and
owns every verdict.** The AI never runs a command and never pushes.

1. **Gate** (code, fail-closed) - parses the command, verifies the commenter
   is an active member of `valkey-io/contributors`, and binds the failed run
   to the PR head (`head_repo` + `head_branch` + `head_sha`). If the branch
   moved since the run, it refuses - the log no longer describes the code.
2. **Fetch** (code) - downloads the failed run's logs and shallow-clones the
   repo at the exact failed commit.
3. **Diagnose** (AI, read-only) - reads the log and the repo, including the
   project's *own* CI workflow files to learn how it builds and tests, then
   returns a structured proposal: port an existing upstream fix, author a
   test-scaffolding fix, or refuse. Nothing about the test framework is
   hardcoded, so the same engine works for any repo.
4. **Select the verifier** (code) - code, not the AI, decides where the fix is
   verified. It lists the jobs that actually failed in the linked run, requires
   the AI's job hint to match one of them, and classifies that job's runner from
   its workflow definition: an x86 Linux job uses a digest-pinned verifier
   image, a container job uses the content identity resolved from that image,
   and a macOS job uses an isolated macOS worker. Anything it cannot classify
   safely (arm, self-hosted, dynamic) is refused.
5. **Verify + review** - the verification policy depends on the fix path:
   - PORT: an existing default-branch commit must be code-discovered,
     ancestry-checked, cherry-picked cleanly, and pass the same failing-baseline
     and patched verification policy as an authored fix.
   - Linux/Docker: run a clean failing baseline and the patched checks in fresh
     containers. The checkout is copied without `.git` into quota-bound tmpfs;
     there are no host mounts, credentials, capabilities, or network. A
     read-only root, non-root UID, cgroup CPU/memory/PID limits, output cap,
     and wall timeout bound execution. The image is resolved once to a content
     identity used by both baseline and patched runs.
   - macOS: run in a separate credentialless GitHub-hosted VM under a dedicated
     non-admin UID. A default-deny Seatbelt profile blocks network and writes
     outside a quota-bound APFS checkout copied without `.git`; `ulimit`,
     process-group termination, and UID-wide cleanup bound resources and
     descendants.
   The real exit code is the verdict. The build runs once and the verify
   command must pass `CI_FIX_VERIFY_RUNS` times in a row (default 2).
   A skeptic review (read-only AI) judges whether the fix addresses the root
   cause rather than silencing the symptom. A push requires both a passing
   verification and an approving review.
6. **Push** (code) - extracts only the approved patch, applies it in a fresh
   trusted clone at the gated SHA, commits authored as the bot (no DCO
   sign-off - a human must certify before merge), and pushes to the PR's own
   `agent/backport/...` branch. The checkout that ran tests never receives
   credentials. The PR's normal CI re-runs as the authoritative check. The bot
   never merges.

This is targeted verification of the one failing check, not a replay of the
whole CI job. Every refusal posts a PR comment explaining why, with evidence,
so when the bot can't safely fix something (a real product bug, a flaky test,
an unverifiable environment), a maintainer can take over immediately.

### Configuration

Uses the shared model-gateway and GitHub App configuration documented in
[DEVELOPMENT.md](DEVELOPMENT.md). The workflow mints narrowly scoped,
short-lived App tokens:

- On `valkey-io/valkey`: `members:read` (team authorization), `actions:read`
  (run logs and failed-job listing), `contents:write` (push the fix),
  `issues:write` (PR comments), `pull-requests:write` (PR metadata).

`ci-fix-comment-poll.yml` runs hourly and polls twice inside the same runner,
30 minutes apart. The in-run loop is capped below the GitHub App token lifetime,
so the second tick does not depend on GitHub scheduling another workflow exactly
on time. Optional poller tuning lives in `CI_FIX_POLL_INTERVAL_SECONDS` and
`CI_FIX_POLL_DURATION_SECONDS`.

Optional verification tuning: `CI_FIX_VERIFY_RUNS` sets how many times a
fix must pass the verify command before it is trusted (default 2, maximum 10).
The build runs once regardless, so raising it only repeats the verify step.

## Operational Controls

Every registered repository has a strict `automation` policy. Before an AI
container starts, the policy is exported with a SHA-256 digest. The
credential-holding gateway must atomically admit each request through
`POST /v1/controls/admit`; it enforces per-repository UTC daily request,
input-token, output-token, and micro-dollar budgets, the in-flight queue limit,
the publication limit, and the configured failure circuit/cooldown. A missing,
malformed, or denied admission fails closed before the model request is sent.
The local proxy additionally caps requests per run and opens its own circuit
after repeated upstream failures. Token, cache-token, turn, and cost totals are
recorded in the hashed AI evidence.

Operators can stop all automation immediately with the repository variable
`VALKEY_CI_AGENT_KILL_SWITCH=true`. A comma-separated
`VALKEY_CI_AGENT_DISABLED_REPOSITORIES` variable disables selected
`owner/name` repositories. Feature switches
`VALKEY_CI_AGENT_DISABLE_BACKPORT`, `VALKEY_CI_AGENT_DISABLE_CI_FIX`,
`VALKEY_CI_AGENT_DISABLE_FUZZER`, and
`VALKEY_CI_AGENT_DISABLE_TEST_FAILURE_DETECTOR`, and
`VALKEY_CI_AGENT_DISABLE_METADATA_RECONCILER` stop their respective workflows.
Setting `automation.enabled: false` disables a registry repository.
Global and feature checks run before App token minting. Multi-repository
discovery applies each repository's disable policy before using its read token.
Publishers repeat the checks and reserve the central publication budget before
minting write credentials.

## Safety

- **Credential separation** - discovery uses read-only tokens; AI and validation jobs have no GitHub or cloud credentials; publishers mint write tokens only after artifact preflight.
- **AI boundary** - Claude runs as a non-root user in a read-only container with only the explicit workspace mounted and egress limited to an internal credential-holding model gateway.
- **Validation boundary** - digest-pinned adapters run without network, Git metadata, capabilities, or host credentials and with CPU, memory, PID, output, tmpfs, and timeout limits.
- **Locked Git** - every Git operation disables hooks, credential helpers, external diff/filter execution, recursive submodules, and ambient Git configuration.
- **Immutable handoff** - phase manifests bind repository, source/base SHA, patch, policy, command plan, logs, validated tree, and publisher permit by SHA-256.
- **Operational containment** - per-repository quotas, bounded queues, failure circuits, module disable controls, and an organization-wide kill switch fail closed before AI or publication.
- **Branch namespace** - publishers write only bot-owned `agent/backport/...` branches and never merge.

## Documentation

- [docs/architecture.md](docs/architecture.md) — full system design including planned workflows
- [docs/audit-remediation.md](docs/audit-remediation.md) — finding-by-finding remediation evidence
- [DEVELOPMENT.md](DEVELOPMENT.md) — local setup, testing, and GitHub Actions usage
