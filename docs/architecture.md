# Architecture

The Valkey CI Agent runs workflows that act on Valkey repositories defined in
the central `repos.yml` registry. Five workflows are active today: backports,
fuzzer monitoring, and the test-failure detector (scheduled), and the CI
test-fix bot and release-notes cut (on-demand).

## Layers

```text
scripts/
  backport/    Backport workflow
  fuzzer/      Fuzzer monitor workflow
  test_failure_detector/ Test Failure Detector workflow
  ci_fix/      CI test-fix bot
  release_notes/ Release-notes cutter: AI notes + version bump
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

Validation first runs the registry's optional `validation_setup_commands`,
then validates the branch after each cherry-pick. The sweep branch is kept
green: a cherry-pick is only kept if the whole branch still validates, and a
failure is reset off the branch so it can never block later candidates. The
run keeps up to two validated cherry-picks (`--max-candidates 2`) and records
skipped or failed candidates in the PR's "Needs attention" section without
committing them. When `repair_validation_failures` is enabled, Claude Code
gets one edit-only repair attempt scoped to the backport diff before a failing
cherry-pick is dropped. Repos with no `build_commands` configured rely on
upstream CI for verification.

### Poll

The daily sweep tops a rolling backport PR up to `--max-candidates` validated
cherry-picks and then waits for the next cron tick, so a merged sweep PR is not
topped back up until the following day. The poll workflow (`backport-poll.yml`)
closes that gap by starting hourly and polling immediately, then once more 30
minutes later inside the same runner. For each registered `{repo, branch}` it
runs the same sweep, but only when no sweep PR is currently open for that
branch:

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
two never race for the same branch. Manual dispatches are one-shot; only
scheduled runs use the sustained in-run cadence.

### Entry Points

- `scripts/backport/sweep.py` - daily sweep across registered repos and release branches
- `scripts/backport/poller.py` - short-cron poll that sweeps a branch only when no sweep PR is open
- `scripts/backport/main.py` - single-PR backport (manual dispatch)
- `scripts/backport/matrix.py` - GitHub Actions matrix generation from `repos.yml`
- `scripts/backport/registry.py` - typed registry loader and validation
- `scripts/backport/sweep_*.py` - focused sweep support modules:
  typed sweep results, Git workspace operations, GitHub PR operations,
  GraphQL access, validation command execution, and Markdown reporting

## Fuzzer Flow

```text
fuzzer/main.py (cron every 4 hours)
  -> common.workflow_artifacts.ArtifactClient.list_recent_runs(...)
  -> FuzzerRunAnalyzer.analyze(run)
       common.workflow_artifacts -> download the artifact bundle
       analyzer._scan_logs() -> deterministic regex pass
       analyzer._invoke_claude() -> drop artifacts in a tempdir,
                                    common.git_clone -> shallow-clone
                                    valkey + valkey-fuzzer at the tested
                                    SHAs, run Claude under
                                    fuzzer_analysis_readonly profile,
                                    parse JSON verdict
       common.incidents.compute_fingerprint() -> stable hash over the
                                    normalized anomaly shapes
  -> common.issue_dedup.IssueDedupPublisher.upsert(...)
       fuzzer.issue_renderer.render_for(analysis) -> title/body/comment
```

Claude is given `Read,Grep,Glob` only - no edits, no shell, no network. The
clones give Claude line-level access so it can grep for assertion text or
crash handlers in `valkey/src/` to distinguish known-benign asserts from new
crashes. If a clone fails, the prompt tells Claude not to cite source line
numbers and the analyzer falls back to artifact-only analysis. If Claude
itself fails, the analyzer falls back to deterministic findings and labels
the verdict `needs-human-triage` rather than silently reporting "normal".
If the fuzzer run produced no artifact bundle the analyzer surfaces that
as an error rather than triaging from raw logs.

Unlike the backport flow, the fuzzer monitor never writes to `valkey-io/valkey`
or `valkey-io/valkey-fuzzer` source - its only side effect is creating or
updating issues on `valkey-fuzzer`.

### Entry Points

- `scripts/fuzzer/main.py` - CLI entry point (cron / manual dispatch)
- `scripts/fuzzer/analyzer.py` - orchestration, deterministic scan, Claude Code integration
- `scripts/fuzzer/issue_renderer.py` - fuzzer-specific title/body/comment rendering
- `scripts/fuzzer/models.py` - typed dataclasses for the analysis pipeline

## CI Fix Flow

On-demand, triggered by a maintainer commenting `@valkeyrie-bot fix <ci-link>`
on a backport PR. Decoupled from the backport sweep; it shares only the
common infrastructure.

```text
ci_fix/main.py (workflow_dispatch event)
  -> gate.build_fix_request(...)        fail-closed auth (contributors team)
                                        + SHA-bound run gating
  -> pipeline.run_ci_fix(...)
       verify.github_runs        -> list the jobs that actually failed (code,
                                    not the AI, owns this)
       common.workflow_artifacts -> download the failed run's logs
       common.git_clone          -> shallow-clone the repo at the failed SHA
       port_discovery            -> code-discovers default-branch candidates
                                    first; configured release branches are
                                    searched only as a no-candidate fallback
       diagnose.diagnose_failure -> read-only AI returns a FixProposal
                                    (port | author | refuse) + a failing-job hint,
                                    using the discovered port candidates so
                                    missing backports are preferred over refusals
       pipeline._plan_verification
                                 -> code matches the hint to a real failed job
                                    -> configured target workflow (preferred), or
                                    -> conservative workflow classification:
                                       local | docker(image) | macos |
                                       unsupported/handoff
       port:
         pipeline._verify_port_and_push
                                 -> sample the clean checkout with the selected
                                    backend
                                 -> apply.apply_port_commit without committing
                                 -> require repeated candidate samples
                                 -> enforce protected-path policy
                                 -> push.commit_and_push_port in a fresh clone,
                                    preserving original authorship
       all backends (separate GitHub Actions runs):
         pipeline._remote_fix_loop
                                 -> dispatch repeated clean baseline samples
                                    -> apply + skeptic review
                                    -> dispatch repeated candidate samples
         verify.agent_workflow   -> shared patch/input transport and verdicts
         verify.linux            -> no-secret ubuntu runner; optional sandboxed
                                    static container
         verify.macos            -> no-secret macOS runner
         verify.target_workflow  -> target-owned recipe, protected ref,
                                    UUID + creation-time run correlation
       unsupported without target integration:
         apply + skeptic review  -> HANDOFF patch; never an unverified push
       policy.authored_publication_decision
                                 -> only allowlisted test paths can auto-publish;
                                    workflows/actions/CODEOWNERS are protected
       push.commit_and_push_fix  -> extract approved patch
                                    -> apply in a fresh trusted clone
                                    -> commit (no sign-off), push to the PR's own
                                       agent/backport/... branch (never merge)
  -> comment.render_comment(outcome) -> posted on the PR
```

The defining invariant is the AI/code split. The AI proposes a diagnosis, edit,
targeted command, and failed-job hint. Code confirms that the hinted job really
failed, chooses the backend, executes verification, owns every pass/fail fact,
evaluates publication policy, and performs any push. The model never selects an
authoritative environment and never receives a repository token.

For a configured target verifier, the AI-authored command is not part of the
dispatch at all. The protected workflow receives only failed run/job identity,
the gated SHA, patch, phase, and repetition index. It maps those values to a
target-owned recipe shared with normal CI. This is the only path that can claim
environment fidelity for ARM, s390x emulation, FreeBSD, dynamic containers,
macOS/Xcode matrices, services, and specialized setup. See
[`ci-fix-verifier.md`](ci-fix-verifier.md).

Jobs using exactly `ubuntu-latest` or `macos-latest`, plus static containers on
`ubuntu-latest`, can run in agent-owned workflows with `permissions: {}`, no
secrets, and no cloud identity. Versioned labels are not approximated with the
corresponding `*-latest` image. Every sample gets a fresh hosted runner, checks
out the gated SHA with persisted credentials disabled, and conditionally
applies the reviewed patch. The generic workflow does not replay target YAML
steps. The diagnosis recipe must include the prerequisites needed by its
targeted command. On host Linux and macOS, classification permits an auxiliary
checkout only when the checkout action, repository, full commit SHA, and
relative path are static and the step is unconditional. Dynamic checkout setup
is unsupported, as is any auxiliary checkout in a container because Docker
samples disable networking. Recipe phases run fail-fast. Docker samples also
override the image entrypoint, drop capabilities, set `no-new-privileges`, and
run as the hosted runner's UID/GID. Only the named targeted-verification step
supplies a verdict; workflow checkout, patch, or runner failures are unavailable
evidence. This separate job boundary is required.

Diagnosis, editing, and skeptic review also consume untrusted checkout/log
content in the credential-bearing controller, so CI-fix gives those Claude
processes a second boundary: bubblewrap creates private PID and mount
namespaces, fresh `/proc` and `/tmp`, read-only system directories, and one
mount containing only the disposable CI-fix workspace. Read-only profiles mount
that workspace read-only; edit profiles can modify the worktree but Git
metadata is overlaid read-only so later controller Git commands cannot consume
AI-planted repository configuration. Bubblewrap is
required by the CI-fix agent profiles and missing/invalid sandbox setup fails
closed. The subprocess environment is filtered to Bedrock credentials, but
filtering is only defense in depth: by itself it would not stop same-user
access through `/proc` or reads of runner temporary files.

Publication never reuses a verification checkout: authored publication applies
the approved patch in a fresh clone, while a port cherry-picks the selected
commit and preserves authorship. The target-workflow contract likewise requires
read-only repository permissions and no cloud identity. Target maintainers
enforce that contract in their workflow; the agent can restrict its dispatch
token and validate the correlated conclusion, but cannot impose permissions
inside the target-owned job.

Every backend samples the unmodified checkout first.
Repeated observations classify deterministic failures, confirmed flakes,
all-green/not-reproduced failures, and unavailable environments. Confirmed or
diagnosed flakes use the repository's stronger repetition count. All-green or
unavailable baselines may produce a reviewed patch or identified port, but only
as `HANDOFF`. Remote samples run in bounded concurrent batches with per-sample
and total campaign deadlines. Missing or timed-out evidence is never success.

The registry is the control plane. `ci-fix.yml` resolves an enabled target
before minting its token; `ci-fix-comment-poll.yml` builds one polling matrix
leg per enabled repository. Target polling and agent-workflow dispatch use
separate scoped tokens. Per-repository settings cover authorization, branch
namespaces, rare release-branch fallback search, confidence and sampling
thresholds, remote concurrency/time budgets, target verifier workflow/ref,
protected paths, and auto-publication paths. Long campaigns refresh installation
tokens with the same exact repository and permission restrictions; artifact and
git operations resolve the refreshed token only when they execute.

Every terminal condition becomes a `FixOutcome` and PR comment. This includes
unrunnable environments, unavailable target integration, real product bugs,
flakes, stale branch heads, unauthorized callers, review rejection, and
publication-policy handoffs.

### Entry Points

- `scripts/ci_fix/main.py` - registry-aware workflow entry point and verifier construction
- `scripts/ci_fix/registry.py` - typed CI-fix target, sampling, verifier, and publication policy
- `scripts/ci_fix/gate.py` - command parsing, fail-closed team auth, SHA-bound run gating
- `scripts/ci_fix/diagnose.py` - read-only AI diagnosis into a structured proposal (fix + job hint)
- `scripts/ci_fix/apply.py` - edit-only AI fix application
- `scripts/ci_fix/review.py` - skeptic review, command guards, patch bounds, and sampling policy
- `scripts/ci_fix/push.py` - patch handoff, commit (no sign-off), namespace-restricted push
- `scripts/ci_fix/comment.py` - render the outcome into a PR comment
- `scripts/ci_fix/pipeline.py` - top-level orchestration; code-owned verifier selection
- `scripts/ci_fix/models.py` - typed dataclasses for the pipeline
- `scripts/ci_fix/verify/` - the verifier layer:
  - `base.py` - VerifyEnv, FailedJob, VerificationPlan, VerificationResult, the VerifyBackend protocol
  - `workflow_env.py` - classify a failed job's runner and reproducible static checkout setup
  - `github_runs.py` - list the jobs that actually failed in a run (code-owned)
  - `actions.py` - shared workflow dispatch, UUID correlation, polling, and log transport
  - `agent_workflow.py` - shared no-secret agent-workflow input and verdict contract
  - `linux.py` - agent-owned Linux and static-container verification
  - `target_workflow.py` - dispatch/correlate a target-owned exact-environment recipe
  - `macos.py` - macOS-specific workflow inputs and verdict mapping
- `scripts/ci_fix/policy.py` - protected-path and test-only auto-publication decisions
- `.github/workflows/ci-fix-verify-linux.yml` - no-secret Linux/container verifier
- `.github/workflows/ci-fix-verify-macos.yml` - no-secret macOS verifier

## AI Layer

```text
runtime.run_agent(profile, prompt, cwd=...)
  -> claude_code.run_claude_code(...)
    -> subprocess: claude --print (Claude Code CLI via Bedrock)
```

Profiles registered today:

- `conflict_resolve_edit_only` - backport conflict resolution (Read/Edit/Bash, writes allowed)
- `fuzzer_analysis_readonly` - fuzzer triage (Read/Grep/Glob only, no writes)
- `ci_fix_diagnose_readonly` - CI-fix diagnosis and skeptic review (Read/Grep/Glob only, no writes)

## Common Infrastructure

Workflow-agnostic helpers in `scripts/common/`:

- `git_auth.py` - GIT_ASKPASS credential helper
- `github_client.py` - retry wrapper for GitHub API
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
  `filter_env(allowlist)` (the single place that turns an env allowlist into
  a concrete, scrubbed subprocess environment).
- `ai_output.py` - `extract_json_object(stdout, required_key=...)` parses a
  structured verdict out of Claude Code's stream-json output. Shared by the
  fuzzer and CI-fix flows.
- `incidents.py` - `compute_fingerprint(namespace, shapes)` produces a stable
  hash over normalized anomaly shapes for issue deduplication.
- `issue_dedup.py` - `IssueDedupPublisher` creates or updates a GitHub
  issue keyed by a fingerprint marker. Workflows supply the rendered title,
  body, and comment via a small `render(marker, occurrences) -> IssueContent`
  callback; the publisher owns the dedup machinery.

Workflow setup is centralized in `.github/actions/setup-agent`. Jobs still
check out the agent repository explicitly, then use that local composite action
to install the pinned Python toolchain, project dependencies, and optionally
the pinned Claude Code CLI. The workflow standards tests scan both workflows
and local action metadata so external actions and Claude installs do not drift.

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
  -> download_all_test_failures() from the run's artifacts
  -> get_job_urls() for CI links
  -> parse_and_deduplicate() groups by {test_name, test_file}
  -> process_failures() creates/updates GitHub issues
```

### Entry Points

- `scripts/test_failure_detector/main.py` - CLI entry point and pipeline orchestration
- `scripts/test_failure_detector/download.py` - workflow run discovery and artifact download
- `scripts/test_failure_detector/parse_failures.py` - JSON parsing and deduplication
- `scripts/test_failure_detector/manage_issues.py` - orchestration over the shared dedup publisher to create/update issues
- `scripts/test_failure_detector/issue_renderer.py` - test-failure-specific title/body/comment rendering and label assignment

## Release Notes Flow

```text
main.py (manual dispatch: version, optional stage, urgency, dry_run)
  -> validate + canonicalize inputs; infer patch GA only when PATCH > 0
  -> dry_run selects preview (true) or PR-opening execution (false)
  -> clone valkey (full depth + tags), validate --base-ref
  -> release_cut.cut()
       -> collect_advisory_fixes()   (if --security-from-advisories)
       -> resolve_branch_plan()      verify M.m branch exists, derive target
       -> pipeline.regenerate_unreleased()
            -> discover()  PRs over base..HEAD, deduped by PR number
            -> classify()  include, AI candidate, or no-release-notes hard exclusion
            -> PRDiffCollector  bounded, attributable commit diffs cached once
            -> triage()    completeness-first AI decision + deterministic
                            release-impact override for risky exclusions/no verdicts
            -> generate()  AI: one categorized bullet per included PR
            -> normalize operator-output categories and canonical bullet format
            -> dedup bullets by PR number (surfaces duplicate_prs)
            -> group_bullets()  {category: [canonical bullet line, ...]}
       -> _drop_already_credited()   dedup against PRs the line already ships
       -> promote_and_bump()         dated section + version.h bump + contributors
       -> _commit_push_release_pr()  prep branch (force-with-lease) + PR into the line
```

The branch model is tag-driven: all stages (rc1, rc2, ..., ga) target the existing
M.m branch (e.g. `9.1`). Maintainers create the branch and push tags before
dispatching. Tags determine the discovery range (rc1 uses the previous release tag,
rc2+ finds the prior rc tag, ga finds the last rc/patch tag). The cut lands on an
`agent/release-cut/...` prep branch and opens a PR into M.m, so the line only
advances when a human merges. The normal dispatch has four inputs and defaults to
a dry run; patch versions may omit stage and infer `ga`, while `M.m.0` always
requires an explicit stage. The advanced dispatch is a thin wrapper around the
same reusable workflow.

The prep branch name is deterministic for a version and stage. Re-dispatching
while its PR is open regenerates the full tagged range through the latest M.m
tip, rebuilds the prep commit on that tip, pushes it with force-with-lease, and
updates the existing PR. Full regeneration is intentional: it includes new
merges and also re-evaluates changed labels and PR metadata without treating a
previous AI-generated branch as durable input.

Signals fall into two tiers. Malformed inputs, a missing target branch, an
already-released/backward target, or a target branch that advances during
generation are hard errors that abort before any PR. Warnings (out-of-sequence
stages, unresolved PRs, empty notes, security mismatches, AI-triage decisions,
deterministic triage overrides, and `LOW`/`MODERATE` urgency paired with
release-impact signals) hold the PR as a draft with a banner naming them. Impact
detection is a review trigger, not an automatic security or severity
classification. Re-dispatch reconciles draft state automatically. `force_ready`,
available only on the advanced dispatch, bypasses holds. An omitted release date
is explicitly the current UTC date; the advanced dispatch accepts an explicit
calendar date.

### Entry Points

- `scripts/release_notes/main.py` - CLI entry point, input validation, clone
- `.github/workflows/release-notes-cut.yml` - simple dispatch and shared reusable release job
- `.github/workflows/release-notes-cut-advanced.yml` - advanced-input wrapper around the shared job
- `scripts/release_notes/release_cut.py` - branch-plan resolution, notes rendering, PR body + `_hold_reasons` (draft-hold decision)
- `scripts/release_notes/pipeline.py` - discover -> classify -> triage -> generate -> render orchestration
- `scripts/release_notes/discover.py` - range resolution and PR discovery by graph reachability
- `scripts/release_notes/backport_refs.py` - recover the original PR of a backported commit (verified sweep Applied table, standalone backport-of URL, subject, -x trailer, branch name)
- `scripts/release_notes/classify.py` - label split: release-notes -> include, no-release-notes -> exclude, else -> triage
- `scripts/release_notes/ai_inputs.py` - shared, bounded PR prompt payloads and SHA-cached diffs; combined sweep diffs are omitted when they cannot be attributed to one source PR
- `scripts/release_notes/triage.py` - completeness-first Claude include/exclude plus deterministic release-impact guardrail for PRs without `release-notes` (no tools; PR data inlined in prompt)
- `scripts/release_notes/generate.py` - Claude bullet generation (no tools; PR data inlined in prompt)
- `scripts/release_notes/models.py` - typed dataclasses for the pipeline
- `scripts/release_notes/security.py` - Security Fixes from published GitHub advisories (never AI-authored)
- `scripts/release_notes/render.py` - canonical `00-RELEASENOTES` rendering
- `scripts/release_notes/publish.py` - find/open/update the release PR; `_reconcile_draft` flips draft state on re-dispatch
- `scripts/release_notes/release_format.py` - `00-RELEASENOTES` dated-section rendering
- `scripts/release_notes/version_bump.py` - `src/version.h` macro rewriting
- `scripts/release_notes/contributors.py` - contributor discovery; reconciles display-name, login, co-author trailers, and PR-resolved logins

## Planned Workflows

Future sibling modules and extensions:

- **PR Reviewer** - two-stage code review with skeptic pass
- **Autonomous CI-fix poller** - the CI-fix engine, driven by a poller that
  detects red backport PRs (or test-failure issues) instead of a maintainer
  `@`-mention. Same pipeline, a different front door.
- **Additional Daily CI Analysis** - detect flaky tests, generate fix PRs
