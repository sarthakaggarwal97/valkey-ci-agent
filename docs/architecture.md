# Architecture

How the agent is put together: the layers, the pattern every workflow
follows, the AI layer, and the shared infrastructure. Each workflow's own
flow is in its document (see [Workflows](#workflows)).

## Principles

- **Code decides, AI proposes.** Deterministic Python binds every request to
  exact evidence (a run attempt, a job ID, a SHA), validates what the model
  hands back, and owns every commit and push. Claude Code is called for the
  parts that need code understanding: conflict resolution, failure diagnosis,
  release-note wording, fuzzer triage, backport eligibility. The CVE scan uses
  no AI at all.
- **Bot-owned branches, human merges.** Every change lands on a
  `backport/...`, `agent/backport/...`, `agent/ci-fix/...` or
  `agent/release-cut/...` branch and goes through the target repository's
  normal PR CI and review. Release lines are never pushed directly and
  published sweep branches are never rewritten.
- **No GitHub token for the model.** The Claude subprocess receives only the
  model's AWS session credentials; `GITHUB_TOKEN`, `GH_TOKEN` and `*_SECRET`
  are stripped from its environment. Git authentication goes through
  `GIT_ASKPASS`, so tokens never appear in `.git/config` or URLs. The CI fix
  agent additionally runs as a separate account.
- **Fail closed.** A validation failure blocks a push; an unreadable registry
  files nothing; an unverified CVE fix dispatches nothing; a held release PR
  opens as a draft.
- **Least privilege, short-lived.** Each job mints an App installation token
  scoped to the repository and permissions it needs, and jobs that run the
  model hold no GitHub token at all.

## Layers

```text
scripts/
  backport/              Backport sweep, poll, eligibility, CI follow-up, onboarding
  ci_fix/                CI fix engine and its front doors
  test_failure_detector/ Daily CI failures -> issues
  fuzzer/                Fuzzer run triage -> issues
  release_notes/         Release-notes cut and review-comment handling
  release/               Release preparation, progress and publication
  cve_scan/              Container CVE scan, verification and rebuild dispatch
  parsers/               Trivy output parsing
  ai/                    Claude Code subprocess orchestration and agent profiles
  common/                Shared infrastructure
.github/workflows/       One or more workflows per family
.github/actions/setup-agent
                         Composite action: Python, dependencies, optional Claude CLI
repos.yml                Registry of repositories, release branches and project boards
release_policy.yml       Authorized teams and policy for release automation
```

Workflows are added as sibling directories. Each picks or adds an agent
profile (tools, timeout, effort) and writes its own prompt; the AI layer and
`common/` stay unchanged.

## The shape of a workflow

Most families follow the same three-part shape:

1. **A front door** (a scheduled poller or a `workflow_dispatch`) that
   authorizes the request, binds it to exact evidence, and produces a typed
   task. Front doors contain no diagnosis or publication logic.
2. **An engine** that prepares one candidate from that task: clones, searches,
   reproduces, calls the model, and checks what comes back.
3. **A publisher** that revalidates the destination immediately before
   writing (exact head, branch, ownership), writes only inside the bot's
   namespaces, and reports the result where the request came from.

The CI fix engine is the clearest example (`FixTask` -> `prepare_fix` ->
`publish_candidate`); the backport sweep, release-notes cut and release
automation follow the same division.

## Workflows

| Family | Doc | Entry points |
|---|---|---|
| Backport | [backport.md](backport.md) | `scripts/backport/sweep.py`, `poller.py`, `ci_followup.py`, `eligibility.py`, `mark_done.py`, `main.py` |
| CI fix | [ci-fix.md](ci-fix.md) | `scripts/ci_fix/run.py`, `main.py`, `comment_poll.py`, `issues.py`, `issue_dispatch.py` |
| Test Failure Detector | [test-failure-detector.md](test-failure-detector.md) | `scripts/test_failure_detector/main.py` |
| Fuzzer monitor | [fuzzer-monitor.md](fuzzer-monitor.md) | `scripts/fuzzer/main.py` |
| Release notes | [release-notes.md](release-notes.md) | `scripts/release_notes/main.py`, `review_poll.py`, `review_handler.py` |
| Release automation | [release-automation.md](release-automation.md) | `scripts/release/main.py` |
| CVE scan | [cve-scan.md](cve-scan.md) | `scripts/cve_scan/sweep.py`, `verify_candidate.py`, `collect_verification.py` |

Planned: a PR reviewer (two-stage review with a skeptic pass) and flaky-test
detection across Daily runs.

## AI layer

```text
runtime.run_agent(profile, prompt, cwd=...)
  -> claude_code.run_claude_code(...)
    -> subprocess: claude --print (Claude Code CLI via Bedrock)
```

A profile fixes the tools, timeout and effort a session gets. Registered
profiles:

| Profile | Tools | Used for |
|---|---|---|
| `conflict_resolve_edit_only` | Read, Edit, MultiEdit, Grep, Glob, Bash; no Write | Backport conflict resolution |
| `validation_repair_edit_only` | Read, Edit, MultiEdit, Grep, Glob; no Bash or Write | One repair attempt on a backport that failed validation |
| `test_adaptation_edit_only` | Read, Edit, MultiEdit, Grep, Glob; no Bash or Write | Porting an unsupported test into a branch-native one |
| `fuzzer_analysis_readonly` | Read, Grep, Glob | Fuzzer triage |
| `ci_fix_edit` | Read, Edit, MultiEdit, Write, Grep, Glob, Bash; no commit or push; isolated | CI fix diagnosis and editing for bot-owned branches |
| `ci_fix_review` | Read, Grep; confined and isolated | Independent review of a CI fix candidate |
| `ci_fix_suggest` | Read, Edit, MultiEdit, Write, Grep; no Bash or Glob; confined | Patch suggestions for contributor PRs |
| `release_notes_review_edit_only` | Read, Edit, MultiEdit, Grep, Glob; no shell or file creation | Review-comment edits on a release PR |

Release-note triage and generation, and backport eligibility, call the model
with no tools: the PR data is inlined in the prompt. `common/ai_output.py`
parses a structured verdict out of Claude Code's stream-json output.

## Common infrastructure

Workflow-agnostic helpers in `scripts/common/`:

| Module | Provides |
|---|---|
| `git_auth.py` | `GIT_ASKPASS` credential helper |
| `git_clone.py` | `shallow_clone_at_sha(repo, dest, sha)`: defensive shallow clone of a public repo at a commit, with argument-injection checks |
| `proc.py` | `git_output(...)` and `filter_env(allowlist)`, the one place an env allowlist becomes a scrubbed subprocess environment |
| `isolation.py` | Run a command, or the Claude Code CLI, as the separate `CI_FIX_AGENT_USER` account over a checkout handed to it, with a fresh home and temp directory, and take the checkout back afterwards |
| `github_client.py` | Retry wrapper for the GitHub API |
| `workflow_artifacts.py` | List and download workflow runs, their artifact bundles, and exact job-console logs |
| `workflow_jobs.py` | One immutable jobs snapshot of a run's exact attempt |
| `valkey_ci.py` | Parse runtest result lines |
| `build_validator.py` | Run registry build, rule, profile and generated-file validation |
| `ai_output.py` | `extract_json_object(stdout, required_key=...)` |
| `incidents.py` | `compute_fingerprint(namespace, shapes)`: a stable hash over normalized anomaly shapes |
| `issue_dedup.py` | `IssueDedupPublisher`: create or update an issue keyed by a hidden fingerprint marker; the workflow supplies a `render(marker, occurrences) -> IssueContent` callback; opt-in `closed_lookback` suppresses a recently fixed failure |
| `identity.py` | Bot identities |
| `polling.py` | One-shot-or-sustained poll loop for schedulers that need a predictable in-run cadence |
| `job_summary.py` | GitHub Actions job summaries |
| `logging_utils.py`, `text_utils.py` | Logging setup; ANSI stripping for log scanning |

## Repository model

The standard model is direct upstream branches: the agent pushes its branches
to the registered repository and opens PRs there. This keeps the registry
small and matches the App permissions. `push_repo` is an escape hatch for a
different-owner fork only; same-owner values are rejected so staging
repositories do not become the deployment model. See [registry.md](registry.md).

## Testing the boundaries

`ci.yml` runs the suite on Python 3.9 and 3.11. The 3.11 leg installs the
pinned Claude Code CLI, creates a `ci-fix-agent` account and sets
`CI_FIX_TEST_AGENT_USER` and `REQUIRE_CLAUDE_CLI`, so the isolation and
confinement tests (`tests/test_isolation.py`,
`tests/test_claude_code_confinement.py`) run for real; a separate
`isolation-as-root` job runs the isolation tests as root in an Alpine
container, as a Valkey container job would. `tests/test_workflow_pinning.py`
and the workflow-semantics tests keep the workflows' external actions, Claude
installs and token scopes from drifting.
