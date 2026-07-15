# Architecture

The Valkey CI Agent runs workflows that act on Valkey repositories defined in
the central `repos.yml` registry. Four workflows are active today: backports,
fuzzer monitoring, test-failure detection, and the on-demand CI test-fix bot.

## Layers

```text
scripts/
  backport/    Backport workflow
  fuzzer/      Fuzzer monitor workflow
  test_failure_detector/ Test Failure Detector workflow
  ci_fix/      CI test-fix bot
  ai/          Claude Code subprocess orchestration
  common/      Shared infrastructure
repos.yml      Registry of repos, release branches, and project boards
```

## Backport Flow

```text
backport-candidates.yml
  discovery (read-only GitHub token)
    -> project_discovery.py queries every Project page
    -> candidate_matrix.py emits a bounded candidate matrix
  backport (one reusable workflow invocation per candidate)
    -> discovery resolves immutable source/base identities
    -> ai-prepare creates a content-addressed patch without GitHub credentials
    -> validation runs a typed adapter without GitHub/model credentials
    -> on candidate failure, ai-repair may make one path-scoped edit-only attempt
    -> repair-validation validates the repaired artifact in a fresh job
    -> unresolved failures are verified and reported on the source PR
    -> publisher verifies the handoff, then mints a write token
    -> fresh clone applies the exact patch and opens a PR
  scheduled/polled aggregation
    -> validated candidate handoffs are combined without credentials
    -> the complete rolling branch is validated in a fresh job
    -> a fresh publisher clone updates agent/backport/sweep/<branch>
```

The registry accepts only versioned `container-argv` validation adapters. They
declare a digest-pinned image, argv arrays, working directory, input and
artifact paths, platform, no-network policy, timeout, and resource limits. V2
can additionally bind immutable GitHub release inputs by URL, exact size, and
SHA-256. A credentialless host fetches those inputs, then a locked,
networkless container extracts them without root and mounts them read-only.
Validation runs in a fresh container with no `.git`, capabilities, ambient
credentials, or host network. A repository without an adapter must carry an
explicit, approved, expiring waiver; there is no implicit successful skip.
When `repair_validation_failures` is enabled, only candidate failures may enter
the isolated repair job. The repaired patch is lineage-bound to the original
patch and failed validation, cannot change the backport path set, and is not
publishable until a separate validation job succeeds.
Preparation refusals and validation failures are content-addressed before a
comment-only token is minted. The source PR carries one durable
target-branch-specific needs-attention record; a later successful publication
updates that record instead of leaving stale failure state.

The former credentialed manual and rolling-sweep mutation engines were
replaced. Scheduled, polled, and manually requested candidates all enter the
same candidate phases. Manual runs publish one PR; scheduled and polled runs
hand successful candidates to `aggregate.py`, which preserves the rolling PR
while independently validating the combined branch. The caller's
`max_candidates` value caps successful aggregate additions; failed or skipped
attempts do not consume it. Repository queue depth remains a separate bound on
the number of attempts admitted in one run.

### Entry Points

- `.github/workflows/backport-candidates.yml` - scheduled/poll candidate fan-out
- `.github/workflows/backport.yml` - reusable credential-separated workflow
- `scripts/backport/main.py` - compatible local router for one candidate phase
- `scripts/backport/sweep.py` and `poller.py` - compatible local routers for candidate and rolling-aggregate phases
- `scripts/backport/candidate_matrix.py` - bounded Project candidate discovery
- `scripts/backport/phased.py` - phase-specific preparation, validation, repair, and publication
- `scripts/backport/aggregate.py` - rolling branch preparation, full-tree validation, and publication
- `scripts/backport/project_discovery.py` - paginated read-only Project queries
- `scripts/backport/registry.py` - typed registry loader and validation
- `scripts/backport/provenance.py` - immutable source-to-target provenance

## Fuzzer Flow

```text
monitor-fuzzer.yml (cron every 4 hours)
  discovery (read-only actions/issues token)
    -> state.py reads the durable high-water cursor
    -> phased.py visits every completed run above it, oldest first
    -> ArtifactClient writes bounded, hashed evidence files
  analysis (no GitHub or cloud credentials)
    -> deterministic scan gathers bounded samples and counts
    -> read-only Claude profile returns a strict versioned schema
    -> invalid AI output falls back to deterministic human triage
    -> analyzed.json links every analysis and AI transcript by SHA-256
  publisher (issue-only token)
    -> preflight reloads every strict artifact
    -> IssueDedupPublisher reconciles the incident event ledger
    -> state.py advances the cursor only after that run is fully published
    -> publication.json records publisher and final issue/cursor state
```

Claude is given `Read,Grep,Glob` only. Its explicit workspace is mounted
read-only, and the container can reach only the internal model gateway. Source
clones are pinned to the tested SHAs. Missing, expired, corrupt, oversized, or
ambiguous artifact evidence remains an explicit operational state rather than
being interpreted as a clean run.

Unlike the backport flow, the fuzzer monitor never writes to `valkey-io/valkey`
or `valkey-io/valkey-fuzzer` source - its only side effect is creating or
updating issues on `valkey-fuzzer`.

### Entry Points

- `.github/workflows/monitor-fuzzer.yml` - credential-separated scheduled flow
- `scripts/fuzzer/main.py` - compatible local router for one fuzzer phase
- `scripts/fuzzer/phased.py` - discovery, analysis, preflight, and publication entry point
- `scripts/fuzzer/state.py` - durable consecutive-run cursor
- `scripts/fuzzer/schema.py` - strict AI and stored-analysis schemas
- `scripts/fuzzer/phase_artifact.py` - bounded cross-job artifacts
- `scripts/fuzzer/analyzer.py` - deterministic scan and Claude integration
- `scripts/fuzzer/issue_renderer.py` - fuzzer-specific title/body/comment rendering
- `scripts/fuzzer/models.py` - typed dataclasses for the analysis pipeline

## CI Fix Flow

On-demand, triggered by a maintainer comment or manual dispatch. The comment
poller persists every request in an append-only issue ledger before dispatch,
then reconciles it through `observed -> authorized -> dispatching ->
dispatched(run_id) -> completed`.

```text
ci-fix.yml
  discovery (read-only GitHub token)
    -> fail-closed team authorization and PR/run/head-SHA binding
    -> exact workflow file is loaded at the run SHA
    -> failed job IDs, display names, matrix, runner, and image are captured
    -> bounded run logs and workflow source are hashed into discovery.json
  ai-prepare (no GitHub/cloud credentials)
    -> clone the exact head and code-discover upstream port candidates
    -> read-only diagnosis selects one captured failed job
    -> code either applies an ancestry-checked port or an edit-only authored fix
    -> skeptical review approves the bounded patch
    -> prepared.json links proposal, review, patch, and complete AI evidence
  validation (credentialless Linux or macOS worker)
    -> reproduce a clean failing baseline
    -> apply the exact patch and verify its tree
    -> run from a .git-free quota filesystem with network denied
    -> enforce non-root identity, resource limits, and descendant cleanup
    -> require repeated patched passes under a recorded isolation contract
    -> validated.json hashes commands, baseline, result, image, and tree
  publisher
    -> preflight the complete handoff before minting a write token
    -> create a desired-comment record before publication
    -> apply the patch to a fresh locked clone at the gated SHA
    -> push only the owned agent/backport branch, never merge
    -> reconcile the final comment/reaction after the expected head is visible
    -> write publication.json with publisher and final remote state
```

The AI proposes a failed-job hint, fix, and targeted command. Code resolves that
hint against the exact captured workflow/job metadata, selects the verifier,
owns every pass/fail decision, and performs publication. The AI cannot execute
commands or touch a remote. The worker that executes target code has no
repository, model, or cloud credential, and its checkout is never reused by the
publisher. Verification intentionally targets one failed check and records
which Actions semantics it does and does not reproduce.

The deployment is not yet fully registry-driven. `ci-fix.yml` and
`ci-fix-comment-poll.yml` are operationally scoped to `valkey-io/valkey`: the
workflow guards on the repo, mints a token for it, and the poller watches only
it. The comment poller runs hourly but uses a shared in-run polling loop to scan
immediately and again 30 minutes later, avoiding dependence on GitHub's
scheduled-workflow queue for every half-hour tick. The loop is capped below the
GitHub App token lifetime. Onboarding another repo (e.g. Valkey Search) still
needs a registry-driven token/poll/dispatch path in the workflows; the Python
engine being repo-agnostic is a precondition, not the whole job.

Every published patch, including a code-discovered port, requires a failing
baseline and patched success. A baseline that is green, un-runnable, or
incomplete causes refusal; it is never converted into publication evidence.

### Entry Points

- `.github/workflows/ci-fix.yml` - credential-separated workflow
- `scripts/ci_fix/main.py` - compatible local router for one CI-fix phase
- `scripts/ci_fix/phased.py` - discovery, preparation, validation, refusal, and publication
- `scripts/ci_fix/phase_artifact.py` - strict cross-job artifacts
- `scripts/ci_fix/comment_poll.py` - authorized request polling and reconciliation
- `scripts/ci_fix/dispatch_ledger.py` - append-only dispatch state machine
- `scripts/ci_fix/gate.py` - command parsing, fail-closed team auth, SHA-bound run gating
- `scripts/ci_fix/diagnose.py` - read-only AI diagnosis into a structured proposal (fix + job hint)
- `scripts/ci_fix/apply.py` - edit-only AI fix application
- `scripts/ci_fix/runner.py` - isolated Linux/macOS execution contract
- `scripts/ci_fix/review.py` - baseline, repeated verification, and skeptical review
- `scripts/ci_fix/port_policy.py` - branch ownership and portable-commit policy
- `scripts/ci_fix/comment.py` - render the outcome into a PR comment
- `scripts/ci_fix/selection.py` - deterministic failed-job and port selection
- `scripts/ci_fix/models.py` - typed dataclasses for the pipeline
- `scripts/ci_fix/verify/job_metadata.py` - exact workflow job/matrix resolution

## AI Layer

```text
runtime.run_agent(profile, prompt, cwd=...)
  -> claude_code.run_claude_code(...)
    -> non-root, read-only Claude container
      -> local credential-holding gateway proxy
        -> central /v1/controls/admit quota and circuit decision
        -> Anthropic-compatible model gateway
```

Profiles registered today:

- `conflict_resolve_edit_only` - backport conflict resolution (Read/Edit/MultiEdit/Grep/Glob; Bash and Write denied)
- `validation_repair_edit_only` - scoped backport/CI-fix repair (Read/Edit/MultiEdit/Grep/Glob; Bash and Write denied)
- `fuzzer_analysis_readonly` - fuzzer triage (Read/Grep/Glob only, no writes)
- `ci_fix_diagnose_readonly` - CI-fix diagnosis and skeptic review (Read/Grep/Glob only, no writes)

The tool container contains only its explicit workspace, has a read-only root,
dropped capabilities, a non-root UID, PID/memory/CPU limits, and an internal
Docker network whose only peer is the gateway. It receives no gateway, GitHub,
or cloud credential. The gateway permits only message/count endpoints and
requires atomic central admission before forwarding. Repository policy sets
daily request/input/output/cost budgets, queue depth, per-run calls, publication
budget, and the failure threshold/cooldown. AI evidence records the full bounded
prompt/transcript plus token, turn, and cost totals.

## Operational Controls

The registry's strict `automation` object is content-addressed into every
backport handoff. `operational_controls.py` rejects disabled repositories and
organization/module kill switches before token minting. Candidate discovery
caps each repository at `max_queue_depth`; AI jobs also use bounded workflow
concurrency. The central gateway owns cross-run daily accounting and automatic
disable circuits, while the local gateway owns per-run limits and a fast
upstream-failure circuit. Publisher jobs repeat the kill-switch check and
atomically reserve the central publication budget before minting write
credentials.

## Common Infrastructure

Workflow-agnostic helpers in `scripts/common/`:

- `git_auth.py` - GIT_ASKPASS credential helper
- `github_client.py` - retry wrapper for GitHub API
- `github_rest.py` - the only private PyGithub requester boundary, with strict
  response contracts for REST operations not exposed by public objects
- `text_utils.py` - ANSI stripping for log scanning
- `workflow_artifacts.py` - list and download GitHub Actions workflow runs
  and their uploaded artifact bundles, plus `download_run_logs(...)` for a
  run's raw console logs. Used by the fuzzer flow (artifacts) and the CI-fix
  flow (run logs).
- `git_clone.py` - `shallow_clone_at_sha(repo, dest, sha)` - defensive
  shallow clone of a public repo at a specific commit, with input
  validation against argument injection. Gives the AI source access at the
  tested SHA in both the fuzzer and CI-fix flows.
- `polling.py` - shared one-shot-or-sustained poll loop helpers for scheduled
  pollers that need a predictable in-run cadence.
- `proc.py` - `git_output(...)` (run a git command and return stdout) and
  locked Git/process-group execution with scrubbed environments and output,
  timeout, and descendant-process bounds.
- `ai_output.py` - `extract_json_object(stdout, required_key=...)` parses a
  structured verdict out of Claude Code's stream-json output. Shared by the
  fuzzer and CI-fix flows.
- `incidents.py` - `compute_fingerprint(namespace, shapes)` produces a stable
  hash over normalized anomaly shapes for issue deduplication.
- `issue_dedup.py` - `IssueDedupPublisher` creates or updates a GitHub
  issue keyed by a fingerprint marker and append-only source-event ledger.
- `markdown.py` - bounded GitHub Markdown rendering, dynamic fences, mention
  neutralization, URL validation, and table/inline escaping
- `phase_artifact.py`, `ai_evidence.py`, and `publication_manifest.py` -
  strict content-addressed handoffs and replay evidence
- `desired_comments.py` and `metadata_reconciler.py` - durable desired-state
  records and idempotent convergence after source publication
- `operational_controls.py` - repository budgets, disable controls, policy
  digests, and gateway environment

Python setup is centralized in `.github/actions/setup-agent` and installs
hash-locked dependencies for the supported Python 3.9 through 3.11 range. CI
tests both boundary minors, while operational workflows use Python 3.11.
Because upstream Requests and urllib3 fixed releases dropped Python 3.9,
`scripts/common/python39_http_hardening.py` applies the reviewed fixes for that
runtime and CI audits only those exact exceptions. AI jobs separately use
`setup-ai-runtime`, which builds the digest-pinned, lockfile-installed Claude
and gateway images. Workflow standards tests scan workflows, composite actions,
Docker bases, and the Claude lockfile so external code does not drift.

## Repository Model

The standard model is direct upstream branches: the agent pushes
`agent/backport/...` branches to `repo` and opens PRs in that same
repository. This keeps the registry small and matches the GitHub App
permissions used by the workflows.

`push_repo` is optional and exists only as a different-owner fork escape hatch.
Same-owner `push_repo` values are rejected so staging repositories do not become
the normal deployment model.

## Test Failure Detector Flow

```text
main.py (daily cron or manual dispatch)
  -> get_latest_daily_run() or use provided run_id
  -> require a source workflow conclusion that permits artifact analysis
  -> download_all_test_failures() as a typed artifact state
  -> get_job_urls() for CI links
  -> parse_and_deduplicate() groups by {test_name, test_file}
  -> process_failures() reconciles bounded Markdown issues
```

Missing, expired, corrupt, oversized, or transport-failed required artifacts
are operational failures. Only source workflow evidence can establish a clean
run; an empty collection is not used as a substitute for artifact state.

### Entry Points

- `scripts/test_failure_detector/main.py` - CLI entry point and pipeline orchestration
- `scripts/test_failure_detector/download.py` - workflow run discovery and artifact download
- `scripts/test_failure_detector/parse_failures.py` - JSON parsing and deduplication
- `scripts/test_failure_detector/manage_issues.py` - orchestration over the shared dedup publisher to create/update issues
- `scripts/test_failure_detector/issue_renderer.py` - test-failure-specific title/body/comment rendering and label assignment

## Planned Workflows

Future sibling modules and extensions:

- **PR Reviewer** - two-stage code review with skeptic pass
- **Autonomous CI-fix poller** - the CI-fix engine, driven by a poller that
  detects red backport PRs (or test-failure issues) instead of a maintainer
  `@`-mention. Same pipeline, a different front door.
- **Additional Daily CI Analysis** - detect flaky tests, generate fix PRs
