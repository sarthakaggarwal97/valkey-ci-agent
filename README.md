# valkey-ci-agent

An AI-powered CI automation agent for the Valkey project. Uses Claude Code (Anthropic Claude Fable 5 via Bedrock) to perform tasks that require code understanding - conflict resolution, code review, failure analysis, and more.

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
              Shared workflow setup for Python deps and optional Claude Code
repos.yml      Central registry of repos, branches, and project boards
```

New workflows are added as sibling directories to `backport/`. Each workflow picks an agent profile (tools, timeout, effort) and writes its own prompt. The AI layer and shared infra stay unchanged.

**Workflows:**

| Workflow | Status | Description |
|----------|--------|-------------|
| Backport | Active | Cherry-picks merged PRs onto release branches with AI conflict resolution |
| Fuzzer Monitor | Active | Analyzes scheduled fuzzer runs and files issues for anomalous failures |
| CI Fix | Active | Repairs agent backport PRs or opens draft fixes from failed default-branch CI |
| Test Failure Detector | Active | Detects test failures from Daily CI, files/updates GitHub issues |
| PR Reviewer | Planned | Two-stage code review with skeptic pass |
| Additional Daily CI Analysis | Planned | Detects flaky tests, generates fix PRs |

## Backport Workflow

The currently active workflow. Cherry-picks merged PRs onto release branches with AI-powered conflict resolution. Works for any repo defined in `repos.yml` - Valkey core, Valkey modules (bloom, search, json), or anything else following the per-branch project-board pattern.

### How it works

1. **Daily sweep** - every day at 09:00 UTC, the preflight job reads `repos.yml` and generates one matrix leg per `{repo, branch}` pair
2. **Project discovery** - each leg queries the GitHub Project v2 board for PRs marked "To be backported"
3. **Cherry-pick** - attempts `git cherry-pick` for each candidate onto the target release branch
4. **AI conflict resolution** - when cherry-pick conflicts, Claude Code reads both sides and resolves the conflict in place
5. **Validation** - registry-configured build commands run before push; any failure blocks the push
6. **PR creation** - pushes the branch and opens (or updates) a PR with a summary table
7. **Status sync** - after a backport PR is merged into the release branch, the source PR's Project v2 status can be moved from "To be backported" to "Done"

Manual single-PR backports are also supported via `workflow_dispatch`.

### Registry (`repos.yml`)

The registry is the single source of truth. To onboard a new repo, add an entry to `repos.yml`:

```yaml
repos:
  - repo: valkey-io/valkey
    project_owner: valkey-io
    project_owner_type: organization
    language: c                          # used in conflict resolver prompt
    ci_fix:
      enabled: true                      # explicit CI-fix write/poll opt-in
    validation_setup_commands:
      - "./ci/setup-backport-validation.sh" # optional; run once in clone
    build_commands:
      - "make -j$(nproc)"                # run before push; empty = skip
    repair_validation_failures: false    # optional; one AI repair attempt on failure
    backport_label: backport
    llm_conflict_label: ai-resolved-conflicts
    max_conflicting_files: 100
    branches:
      - branch: "8.1"
        project_number: 14
      - branch: "9.0"
        project_number: 18
```

By default, agent branches are pushed directly to `repo` under the `agent/backport/...` namespace and PRs are opened in that same upstream repository. `push_repo` is optional and only exists as an escape hatch for a real different-owner fork; same-owner `push_repo` values are rejected so staging repositories do not become the normal model.

CI fixing is a separate, explicit capability. Set `ci_fix.enabled: true` on a
registry entry to allow the CI-fix workflow to mint a repository-scoped token,
poll its PR and eligible issue comments, fix its agent-owned backport branches,
and open issue-driven draft PRs. Omitting the setting leaves CI fixing disabled
even when the repository supports backports.

The sweep branch is always kept green: a candidate is only kept if the whole branch still validates after the cherry-pick, so one bad commit can never block later candidates. Each scheduled run keeps up to two validated cherry-picks (`--max-candidates 2`) and reports candidates that were skipped or failed validation in the PR's "Needs attention" section without committing them. When `repair_validation_failures` is enabled, Claude Code gets one narrow edit-only attempt to fix a failing cherry-pick before it is dropped.

See [`examples/repos.yml`](examples/repos.yml) for a multi-module example.

### Setup and Usage

See [DEVELOPMENT.md](DEVELOPMENT.md) for local setup, local validation commands,
required GitHub Actions secrets, and manual workflow dispatch examples.

## Fuzzer Monitor Workflow

The fuzzer monitor watches scheduled `valkey-io/valkey-fuzzer` workflow runs, analyzes their artifacts, and files issues for runs that look anomalous.

### How it works

1. **Cron** - every 4 hours, the monitor checks the latest scheduled fuzzer run
2. **Deterministic scan** - pattern-matches crash/sanitizer/failover/RDB signals against artifact JSON and node logs; ignores chaos-expected noise (CLUSTERDOWN, replication link loss)
3. **Claude Code analysis** - drops the artifacts in a tempdir, shallow-clones `valkey-io/valkey` at the tested commit and `valkey-io/valkey-fuzzer` at the run's HEAD, then asks Claude (with read-only `Read,Grep,Glob` tools) to correlate the failure with source and decide whether the run reflects a real bug or chaos-expected noise. If a clone fails the prompt tells Claude not to cite source line numbers.
4. **Issue upsert** - anomalous runs file (or update) an issue on `valkey-io/valkey-fuzzer`, deduplicated by a stable fingerprint over root cause and anomaly shape
5. **Audit** - per-run JSON results and Claude evidence are uploaded as workflow artifacts

The Claude Code subprocess runs under the `fuzzer_analysis_readonly` agent profile with `Read,Grep,Glob` tools only - no editing, no Bash, no network access beyond the Bedrock call itself.

### Configuration

The monitor reuses the same secrets and OIDC role as the backport workflow (see [Step 1](#step-1-configure-secrets-and-variables) above). The Valkeyrie GitHub App needs `actions:read`, `contents:read`, and `issues:write` on `valkey-io/valkey-fuzzer`; the workflow mints a short-lived installation token scoped to that repository only.

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

An on-demand workflow with two additive modes:

- **Backport PR repair** updates an existing `agent/backport/...` PR branch.
- **Issue repair** creates a new draft PR from the current default branch. An
  explicit failed-run URL works on any open issue; a Test Failure Detector issue
  created by the configured App may reuse a run URL recorded by that same App.

Run either mode explicitly from this agent repository:

```bash
# Repair an existing agent backport PR.
gh workflow run ci-fix.yml \
  --repo valkey-io/valkey-ci-agent \
  --field repo=valkey-io/valkey \
  --field pr=<pr-number> \
  --field run_url=https://github.com/valkey-io/valkey/actions/runs/<run_id>

# Create a draft fix from an open source issue.
gh workflow run ci-fix.yml \
  --repo valkey-io/valkey-ci-agent \
  --field repo=valkey-io/valkey \
  --field issue=<issue-number> \
  --field run_url=https://github.com/valkey-io/valkey/actions/runs/<run_id>
```

Exactly one of `pr` or `issue` must be supplied. `run_url` is required for PR
repairs and ordinary issues. It may be omitted only for a detector issue created
by the configured App that already records a valid Actions run URL.

The workflow accepts every repository explicitly opted in through
`ci_fix.enabled` in `repos.yml`. Today that is `valkey-io/valkey` and
`valkey-io/valkey-search`. It resolves the dispatch target through the registry
before minting a token scoped only to that repository. Maintainers can dispatch
it manually, or comment on an eligible PR or open issue and let
`ci-fix-comment-poll.yml` dispatch it:

```text
@valkeyrie-ops fix https://github.com/valkey-io/valkey/actions/runs/<run_id>
```

The invocation must start the comment, and the optional hint is only the rest
of that line. An ordinary issue must include the URL. On a detector issue
created by the configured App, `@valkeyrie-ops fix <hint>` may omit it; the gate
selects the latest valid CI link recorded by that same App. The bot fixes one
test per invocation.

### How it works

The division of labor is the whole design: **AI judges, code executes and owns
every verdict.** The AI never runs a command and never pushes.

1. **Gate** (code, fail-closed) - verifies active membership in
   `valkey-io/contributors`. PR repair binds the run to the exact current PR
   head. Issue repair requires an open issue, an explicit failed-run URL or a
   `test-failure` issue created by the configured detector App and carrying its
   marker, a completed same-repository run whose commit remains in the current
   default branch's ancestry, and no prior PR for the generated issue/run
   branch.
2. **Fetch** (code) - downloads run logs and clones the repository at the exact
   failed commit.
3. **Diagnose** (AI, read-only) - reads bounded log, source-issue, source, and
   workflow evidence and returns a structured proposal. PR repair may port an
   existing default-branch fix; issue repair permits only a causal authored fix
   or a refusal. Arbitrary sleeps, weakened assertions, skips, and product
   changes without a narrow test-failure cause are refused.
4. **Select the verifier** (code) - requires the proposed job to be one that
   actually failed, then derives local, Docker, or macOS verification from the
   repository's workflow. Unsupported or ambiguous environments are refused.
5. **Verify + review** - Linux and Docker execute in a sanitized subprocess;
   macOS uses an agent-owned workflow. PR repair requires a failing baseline
   before an authored push. Issue repair repeats both the unpatched baseline
   and post-change command. A clean or unavailable baseline can produce only
   an explicitly unverified draft handoff. Verification performed at an older
   default-branch commit is also reported as historical, with the draft PR's CI
   authoritative for the published branch. A skeptic review must approve the
   exact patch and reject symptom masking.
6. **Publish** (code) - PR repair applies the exact approved patch to the gated
   SHA and pushes fast-forward-only to the existing `agent/backport/...`
   branch. Issue repair applies it to the latest default-branch tip, pushes
   `agent/ci-fix/issue-<issue>-run-<run>`, and opens a linked draft PR. The
   checkout that ran tests never receives credentials. The bot never marks a
   draft ready, certifies DCO, or merges.

This is targeted verification of one failing check, not a replay of the whole
CI job. Every outcome is posted to the triggering PR or issue. A non-reproduced
issue failure is labeled as unverified in both the issue comment and draft PR;
green PR CI remains the authority.

### Configuration

Reuses the same secrets and OIDC role as the other workflows (see
[Step 1](#step-1-configure-secrets-and-variables)). The workflow mints two
short-lived App tokens:

- On the registry-resolved target repository: `members:read` (team
  authorization), `actions:read` (run logs and failed-job listing),
  `contents:write` (push the fix), `issues:write` (PR comments), and
  `pull-requests:write` (PR metadata).
- On `valkey-io/valkey-ci-agent`: `actions:write` (dispatch and read the
  macOS verification workflow). Used only for the macOS backend.

`ci-fix-comment-poll.yml` runs hourly and polls twice inside the same runner,
30 minutes apart. Each tick polls all `ci_fix.enabled` repositories from the
registry. A GitHub API failure in one repository does not block the others, but
the tick fails if every repository is inaccessible so token or App-installation
problems are visible. The in-run loop is capped below the GitHub App token
lifetime, so the second tick does not depend on GitHub scheduling another
workflow exactly on time. Optional poller tuning lives in
`CI_FIX_POLL_INTERVAL_SECONDS` and `CI_FIX_POLL_DURATION_SECONDS`.

Optional verification tuning: `CI_FIX_VERIFY_RUNS` sets how many times a
Linux/Docker fix must pass the verify command before it is trusted (default 2,
maximum 10). `CI_FIX_ISSUE_BASELINE_RUNS` and
`CI_FIX_ISSUE_VERIFY_RUNS` independently control issue-driven pre-fix and
post-change repetitions across Linux, Docker, and macOS (default 5 each,
minimum 2, maximum 20). Issue mode is capped at 55 minutes so its final push and
report occur before the one-hour App and AWS credentials expire; tune repetition
counts down for slow checks. Existing PR-repair macOS verification remains
one-shot.

## Safety

- **Branch namespace** - the agent writes only `agent/backport/...` and `agent/ci-fix/issue-...` branches; issue fixes always open as drafts.
- **Credential isolation** - all GitHub auth uses `GIT_ASKPASS`; tokens never appear in `.git/config` or URLs
- **Claude Code env isolation** - `GITHUB_TOKEN`, `GH_TOKEN`, and `*_SECRET` are stripped from the subprocess environment. Claude cannot see credentials.
- **Deterministic validation** - registry-configured build commands run before push. A validation failure blocks the push.
- **Fork sync** - when a different-owner `push_repo` is configured, the agent fast-forwards that fork's release branch to match upstream before cherry-picking
- **Stale branch pruning** - if a previous backport PR was closed without merging, the agent deletes the orphaned branch before starting fresh
- **DCO** - backport commits are signed off. ci_fix commits are authored by the bot without a sign-off, so a human certifies the change before merge.

## Documentation

- [docs/architecture.md](docs/architecture.md) — full system design including planned workflows
- [DEVELOPMENT.md](DEVELOPMENT.md) — local setup, testing, and GitHub Actions usage
