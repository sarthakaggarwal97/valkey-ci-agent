# Audit Remediation Evidence

This document maps every finding in the 2026-07-15 audit of commit
`858232f68614d088578ffa2d409b7795b179117e` to its current implementation and
regression evidence. No intended product capability was removed. Legacy sweep
and CI-fix implementations were rewritten as credential-separated,
content-addressed phases while retaining their user-facing behavior.

## Enforced Architecture

- Discovery uses read-only GitHub credentials and emits immutable identifiers.
- AI preparation receives no GitHub, AWS, or publisher credentials. Claude runs
  in a container with only the explicit checkout and an internal model gateway.
- Validation runs target code in a credentialless job and isolated runtime.
- Publisher preflight runs before token minting. The publisher freshly clones,
  verifies the exact base, patch, tree, policy, and phase digests, then pushes
  without executing target code.
- Every candidate uses a disposable checkout and a content-addressed handoff.
  Scheduled/polled candidates are then rebuilt onto the rolling sweep branch
  in a separate credentialless phase, and the complete aggregate is validated
  again before publication. Repositories may also enable one credentialless,
  edit-only validation-repair attempt per candidate.
- Mutable labels and comments are represented as durable desired state and
  converged by an idempotent reconciler.

## Functional Preservation

- Valkey Search backports remain enabled for `1.0`, `1.1`, and `1.2`, with
  build-and-test validation, immutable dependencies, and validation repair.
- Manual, scheduled, and polled backports remain supported. Scheduled runs
  retain rolling PRs, successful-candidate limits, AI resolution metadata,
  draft promotion, labels, diff comments, and needs-attention failures.
- CI-fix retains comment and manual triggers, upstream-port and authored-fix
  paths, Linux and macOS verification, result comments, and reactions.
- Fuzzer monitoring and test-failure detection retain their scheduled and
  manual flows, durable state, issue creation, and issue updates.
- `scripts.backport.main`, `scripts.backport.sweep`,
  `scripts.backport.poller`, `scripts.ci_fix.main`, and
  `scripts.fuzzer.main` remain available as one-phase operator routers. The
  complete write-capable operations run through their phased workflows.

## Critical Requirement Coverage

| Finding | Report remediation | Direct evidence |
|---|---|---|
| C-01.1 | Run target setup, build, and test without App, cloud, secret, or OIDC credentials. | The `validation` job in `.github/workflows/backport.yml` has only read-only workflow permission, receives no secret expression, and runs the adapter in `scripts/common/validation_adapter.py`; `tests/test_workflow_pinning.py::test_manual_backport_workflow_separates_trust_domains` enforces the job boundary. |
| C-01.2 | Hand off only bounded content-addressed results and publish from a fresh verified clone. | `scripts/common/phase_artifact.py` bounds and hashes every handoff; `scripts/backport/phased.py::preflight_publish` and `publish` independently reconstruct the exact tree; `tests/test_backport_phased.py::test_manual_backport_phases_publish_only_the_validated_tree` proves the published tree identity. |
| C-01.3 | Remove privileged validation and enforce an isolated, pinned, non-root runtime. | `scripts/common/validation_adapter.py` requires a digest-pinned image, no network, non-root UID, dropped capabilities, read-only root, PID/CPU/memory/tmpfs/output limits, and no Docker socket mount. `tests/test_validation_adapter.py::test_real_adapter_denies_parent_host_network_device_and_root_write` exercises the real boundary. |
| C-01.4 | Treat all configured validation commands as untrusted. | The registry accepts only typed argv adapters and rejects the legacy shell interface. Every adapter command uses the same sandbox in `scripts/common/validation_adapter.py`; `tests/test_registry.py::test_old_shell_command_interface_is_rejected` and `tests/test_validation_adapter.py` enforce this. |
| C-02.1 | Run model tools with only a sanitized checkout and bounded evidence. | `scripts/ai/claude_code.py` requires one explicit workspace mount and uses a read-only root plus bounded tmpfs, process, memory, CPU, turn, time, and transcript limits. |
| C-02.2 | Keep the upstream model credential outside the model/tool process. | `.github/actions/setup-ai-runtime/action.yml` starts a separate gateway container holding the credential. The tool container receives only an internal placeholder and can reach only that gateway; `tests/test_ai_gateway.py` verifies endpoint and budget enforcement. |
| C-02.3 | Remove Bash from conflict resolution. | Every profile in `scripts/ai/runtime.py` denies Bash, and edit profiles also deny unrestricted Write. `tests/test_agent_runtime.py` and `tests/test_claude_code.py` verify the effective tool arguments. |
| C-02.4 | Enforce filesystem, identity, process, and network boundaries. | The runtime command in `scripts/ai/claude_code.py` uses an exact mount, numeric non-root UID, internal-only Docker network, dropped capabilities, and no host socket. The setup action probes parent `/proc`, host `~/.aws`, Git config, neighboring workflows, root writes, devices, metadata, and gateway-only connectivity. |
| C-02.5 | Replace security claims with properties covered by adversarial tests. | `README.md` and `docs/architecture.md` describe the enforced boundary. `tests/test_workflow_pinning.py::test_ai_runtime_runs_adversarial_boundary_probe` requires every report-named probe. |
| C-03.1 | Move verification to a credentialless job or VM. | `.github/workflows/ci-fix.yml` has separate Linux and macOS validation jobs with no secret expressions or token-minting actions; `tests/test_workflow_pinning.py::test_ci_fix_workflow_separates_ai_validation_and_publication` enforces this. |
| C-03.2 | Use an ephemeral namespace boundary with no host socket, credentials, or `.git`. | `scripts/ci_fix/runner.py` copies the checkout without `.git` into an unmounted container tmpfs on Linux and a dedicated APFS volume plus Seatbelt sandbox on macOS. Real Linux and macOS tests in `tests/test_ci_fix_runner.py` verify denial. |
| C-03.3 | Kill the complete process tree and bound CPU, memory, PID, disk, output, and wall time. | Linux timeout destroys the complete container cgroup; macOS kills the dedicated verifier UID; the common runner also uses a new process group. `test_low_level_timeout_kills_descendant_processes` and `test_real_linux_sandbox_timeout_removes_container_descendants` prove descendant cleanup. |
| C-03.4 | Default to no network. | Linux uses Docker network `none`; macOS Seatbelt has no network allow rule. Runtime boundary probes require metadata and socket denial. |
| C-04.1 | Route production Git through one audited API. | `scripts/common/proc.py` is the sole production Git subprocess owner. `tests/test_git_invocation_policy.py` fails on direct production subprocess Git calls. |
| C-04.2 | Neutralize hooks, helpers, filters, drivers, editors, paging, signing, and unsafe transports. | `LOCKED_GIT_CONFIG`, `_locked_git_env`, and dynamic repository-config overrides in `scripts/common/proc.py` apply to every invocation. `tests/test_proc_git.py` plants each executable path and verifies it never runs. |
| C-04.3 | Authenticate only a fresh publishing clone. | Backport and CI-fix publishers reconstruct from the validated artifact in a new temporary clone and introduce App authentication only for the remote operation; phased integration tests verify the exact resulting tree. |
| C-04.4 | Make direct subprocess Git a lint failure. | `tests/test_git_invocation_policy.py::test_production_has_no_direct_subprocess_git_invocations` statically checks every production Python module. |

## Critical Findings

| ID | Implemented disposition | Primary evidence |
|---|---|---|
| C-01 | Backport discovery, AI preparation, validation, and publication are separate jobs. Validation uses a typed, digest-pinned, networkless, non-root container without `.git`; privileged validation was removed without disabling Valkey Search backports. Search dependencies are size- and SHA-256-pinned, extracted without root, and mounted read-only for build and unit-test validation. | `.github/workflows/backport.yml`, `scripts/common/validation_adapter.py`, `repos.yml`, `tests/test_workflow_pinning.py`, `tests/test_validation_adapter.py`, `tests/test_registry.py` |
| C-02 | Claude receives no AWS or GitHub credentials. It runs with a read-only root, exact workspace mount, internal-only network, no capabilities, and a narrow gateway holding the upstream credential. Bash is denied in all profiles. The setup action executes an adversarial boundary probe. | `scripts/ai/claude_code.py`, `scripts/ai/runtime.py`, `scripts/ai/gateway_proxy.py`, `.github/actions/setup-ai-runtime/action.yml`, `tests/test_claude_code.py`, `tests/test_ai_gateway.py` |
| C-03 | CI-fix validation moved to credentialless Linux/macOS jobs. Linux uses an unmounted tmpfs container; macOS uses a dedicated UID, APFS workspace, and Seatbelt allowlist. Both run kernel-boundary probes, bounded output/resources, and process-group cleanup. | `scripts/ci_fix/runner.py`, `.github/workflows/ci-fix.yml`, `.github/workflows/ci.yml`, `tests/test_ci_fix_runner.py` |
| C-04 | All production Git runs through the locked byte-safe API. Hooks, helpers, executable drivers, proxies, signing, paging, editors, filters, and unsafe protocols are neutralized; direct subprocess Git is a test failure. | `scripts/common/proc.py`, `tests/test_proc_git.py`, `tests/test_git_invocation_policy.py` |

## High Findings

| ID | Implemented disposition | Primary evidence |
|---|---|---|
| H-01 | Rolling sweep branches are preserved without credentialed mutation. Each candidate is validated independently; a credentialless aggregate phase rebuilds or rebases the existing `agent/backport/sweep/<branch>`, binds the commit series and combined tree, and validates that complete tree before a fresh publisher clone updates the rolling PR. | `scripts/backport/aggregate.py`, `.github/workflows/backport-candidates.yml`, `tests/test_backport_aggregate.py` |
| H-02 | The exact target base is validated before the candidate. A red base emits a content-addressed, non-publishable failure artifact and is never attributed to or repaired as a candidate failure. | `scripts/backport/phased.py`, `tests/test_backport_phased.py::test_validation_stops_when_target_baseline_is_red` |
| H-03 | Validation repair is preserved through a separate credentialless AI job. It gets one edit-only attempt, cannot change paths outside the original backport, emits a new lineage-bound prepared artifact, and must pass fresh credentialless validation before publication. | `scripts/backport/phased.py`, `scripts/common/phase_artifact.py`, `.github/workflows/backport.yml`, `tests/test_backport_phased.py::test_failed_backport_is_repaired_once_and_requires_fresh_validation` |
| H-03a | Refused and still-failing candidates retain the former needs-attention capability as a durable source-PR record. A strict failure artifact is verified before a comment-only token is minted, and later success reconciles the same record to recovered. | `scripts/backport/phased.py`, `.github/workflows/backport.yml`, `tests/test_backport_phased.py::test_failed_backport_reports_and_later_resolves_needs_attention` |
| H-04 | Every candidate gets a fresh temporary clone that is destroyed on exit. AI has no Bash, and subsequent validation/publication reconstruct from a bounded patch in fresh clones. | `scripts/backport/phased.py`, `scripts/ai/runtime.py`, `tests/test_agent_runtime.py`, `tests/test_backport_phased.py` |
| H-05 | Shell verification uses fixed Bash fail-fast and `pipefail` semantics. Backport adapters use argv, and Linux/macOS runtime contracts record the shell. | `scripts/common/proc.py`, `scripts/ci_fix/runner.py`, `tests/test_ci_fix_runner.py::test_fail_fast_shell_forms_cannot_report_success` |
| H-06 | Discovery binds the workflow path/content at the run SHA and captures workflow ID, attempt, job database ID, display name, labels, matrix, runner, and environment. Resolution is against that exact workflow. | `scripts/ci_fix/phased.py`, `scripts/ci_fix/verify/job_metadata.py`, `tests/test_ci_fix_job_metadata.py`, `tests/test_ci_fix_selection.py` |
| H-07 | Linux and macOS use the same validation phase: exact clean failing baseline followed by repeated patched passes under one attested runtime. | `scripts/ci_fix/phased.py`, `.github/workflows/ci-fix.yml`, `tests/test_ci_fix_phased.py` |
| H-08 | Artifact listing paginates. Downloads stream to disk with compressed, uncompressed, member, path, count, and compression-ratio limits and extract only requested regular files. | `scripts/common/workflow_artifacts.py`, `scripts/common/github_rest.py`, `tests/test_workflow_artifacts.py` |
| H-09 | Test-failure downloads return typed states. Only source-workflow success is clean; missing, expired, corrupt, oversized, and transport failures are operational errors. | `scripts/test_failure_detector/download.py`, `scripts/test_failure_detector/main.py`, `tests/test_testfailuredetector_download.py`, `tests/test_testfailuredetector_main.py` |
| H-10 | Comment requests use an append-only state machine with correlation IDs and retries: observed, authorized, dispatching, dispatched, completed. Reactions are display-only. | `scripts/ci_fix/dispatch_ledger.py`, `scripts/ci_fix/comment_poll.py`, `tests/test_ci_fix_dispatch_ledger.py`, `tests/test_ci_fix_comment_poll.py` |
| H-11 | Issue events use immutable hashed markers committed before mutable body edits. Serialized publishers and retry reconciliation prevent duplicate counting and repair partial failures. | `scripts/common/issue_dedup.py`, `tests/test_issue_dedup.py`, workflow concurrency tests in `tests/test_workflow_pinning.py` |
| H-12 | Fuzzer monitoring uses a compare-and-set high-water cursor and processes every completed run in order. AI output has strict versioned enums, keys, and bounds; invalid output falls back to deterministic triage. | `scripts/fuzzer/phased.py`, `scripts/fuzzer/state.py`, `scripts/fuzzer/schema.py`, `tests/test_fuzzer_phased.py`, `tests/test_fuzzer_analyzer.py` |
| H-13 | Publication embeds content-addressed source/base/target/patch/tree provenance. Mark-done verifies the bot PR, exact commits and trees, current source identity, merge ancestry, and paginated project state. | `scripts/backport/provenance.py`, `scripts/backport/mark_done.py`, `tests/test_backport_provenance.py`, `tests/test_backport_mark_done.py` |
| H-14 | Python runtime/dev sets and Claude npm installation are version and integrity locked. Python 3.10/3.11 use fixed upstream HTTP releases. Python 3.9 retains support through conditional pins plus reviewed backports for the three advisories whose fixed releases dropped 3.9. CI audits both dependency sets, runs dependency review, and enforces license policy. | `requirements/runtime.txt`, `requirements/dev.txt`, `scripts/common/python39_http_hardening.py`, `.github/actions/setup-agent/action.yml`, `.github/workflows/ci.yml`, `tests/test_python39_http_hardening.py`, `tests/test_workflow_pinning.py` |

## Medium Findings

| ID | Implemented disposition | Primary evidence |
|---|---|---|
| M-01 | The single phased candidate engine carries and enforces `max_conflicting_files` before invoking AI. | `scripts/backport/phased.py`, `tests/test_backport_phased.py::test_prepare_enforces_shared_max_conflicting_files_policy` |
| M-02 | Manual, discovered, and CI-fix branches use `agent/backport/`. Publishers restrict the namespace and backport heads carry digest-checked provenance. | `scripts/backport/utils.py`, `scripts/ci_fix/port_policy.py`, `scripts/backport/provenance.py`, `tests/test_backport_utils.py` |
| M-03 | Existing direct or fork branches are resolved to an exact SHA and updated with explicit force-with-lease. Publication retries verify existing provenance before reuse. | `scripts/backport/phased.py`, `tests/test_backport_phased.py::test_stale_fork_branch_uses_an_exact_force_with_lease` |
| M-04 | Registry schema v2 rejects unknown keys and unsafe values. Live preflight resolves repositories, branches, projects/status options, labels, image digests, and effective App permissions. | `scripts/backport/registry.py`, `scripts/backport/registry_preflight.py`, `tests/test_registry.py`, `tests/test_registry_preflight.py` |
| M-05 | Project items, field values, project fields, and item field values all paginate and fail closed on missing cursors. Discovery no longer truncates PR commit identity. | `scripts/backport/project_discovery.py`, `scripts/backport/mark_done.py`, `tests/test_project_discovery.py`, `tests/test_backport_mark_done.py` |
| M-06 | Git path APIs use NUL-delimited bytes and strict UTF-8 conversion. Binary or undecodable conflicts become human handoffs instead of lossy writes. | `scripts/common/proc.py`, `scripts/backport/cherry_pick.py`, `scripts/backport/conflict_resolver.py`, `tests/test_backport_cherry_pick.py`, `tests/test_proc_git.py` |
| M-07 | One Markdown module handles dynamic fences, mentions, tables, inline text, URLs, and exact byte budgets. Renderers validate source types and preserve required markers. | `scripts/common/markdown.py`, `scripts/test_failure_detector/issue_renderer.py`, `scripts/fuzzer/issue_renderer.py`, `tests/test_markdown.py` |
| M-08 | Artifact outcomes are typed with status, retry classification, diagnostics, sizes, paths, and hashes. Empty evidence is distinct from transport and integrity failure. | `scripts/common/workflow_artifacts.py`, `tests/test_workflow_artifacts.py` |
| M-09 | Every registry entry must define a non-empty typed adapter or an approved, expiring waiver. Selected command IDs, immutable inputs, and complete plans are recorded and digest-bound. Valkey Search is enabled with an active adapter rather than a waiver. | `scripts/backport/registry.py`, `scripts/common/validation_adapter.py`, `scripts/backport/phased.py`, `repos.yml`, `tests/test_registry.py` |
| M-10 | Registry shell strings were replaced by typed container adapters specifying image and input digests, argv, workdir, source inputs, network, resources, timeout, and expected artifacts. | `scripts/common/validation_adapter.py`, `repos.yml`, `tests/test_validation_adapter.py`, `tests/test_registry.py::test_old_shell_command_interface_is_rejected` |
| M-11 | Every phase emits strict content-addressed manifests. Evidence binds discovery, policy, patch, tree, commands, runtime identity, logs/transcripts, publisher identity, and final remote/API state. | `scripts/common/phase_artifact.py`, `scripts/common/ai_evidence.py`, `scripts/common/publication_manifest.py`, `tests/test_phase_artifact.py`, `tests/test_publication_manifest.py` |
| M-12 | State-machine and real-Git fault tests cover moved heads, red baselines, validator side effects, stale leases, ambiguous push/PR outcomes, partial issue batches, dispatch retries, and runtime timeouts. Real Docker descendant cleanup and macOS APFS/Seatbelt boundary regressions run in CI. | `tests/test_backport_phased.py`, `tests/test_ci_fix_phased.py`, `tests/test_fuzzer_phased.py`, `tests/test_issue_dedup.py`, `tests/test_ci_fix_runner.py` |
| M-13 | Local verification is schema-labeled `targeted-approximation-v1`; reproduced and omitted semantics are recorded. Comments and manifests identify pull-request CI as authoritative. | `scripts/ci_fix/verify/job_metadata.py`, `scripts/ci_fix/phase_artifact.py`, `scripts/ci_fix/comment.py`, `tests/test_ci_fix_job_metadata.py` |
| M-14 | Desired labels/comments are recorded before publication and reconciled idempotently. Publisher retries recover already-pushed exact results, and the scheduled metadata reconciler converges partial API effects. | `scripts/common/desired_comments.py`, `scripts/common/metadata_reconciler.py`, `.github/workflows/metadata-reconcile.yml`, `tests/test_desired_comments.py`, `tests/test_metadata_reconciler.py` |

## Low Findings

| ID | Implemented disposition | Primary evidence |
|---|---|---|
| L-01 | Security documentation now distinguishes enforced isolation, targeted CI approximation, publisher behavior, and remaining trust assumptions. | `README.md`, `docs/architecture.md`, `DEVELOPMENT.md` |
| L-02 | Manual and project discovery both delegate each PR to the same phased candidate workflow. Manual runs publish individual PRs; scheduled/polled runs export validated candidates to the separately validated rolling aggregate workflow. | `.github/workflows/backport-candidates.yml`, `.github/workflows/backport.yml`, `scripts/backport/phased.py`, `scripts/backport/aggregate.py`, `tests/test_workflow_pinning.py` |
| L-03 | PyGithub private requester use is isolated behind one validated, versioned REST adapter. Reactions use public APIs where available. | `scripts/common/github_rest.py`, `scripts/ci_fix/comment_poll.py`, `tests/test_github_rest.py` |
| L-04 | Runtime support remains Python 3.9 through 3.11. Package metadata, mypy, Ruff, hash-locked conditional dependencies, reviewed compatibility backports, CI boundary-version tests, and development documentation enforce that contract. | `pyproject.toml`, `requirements/runtime.txt`, `requirements/dev.txt`, `scripts/common/python39_http_hardening.py`, `.github/actions/setup-agent/action.yml`, `.github/workflows/ci.yml`, `DEVELOPMENT.md` |
| L-05 | Strict per-repository request/token/cost/publication budgets, queue limits, failure circuits, cooldowns, feature switches, repository disables, and a global kill switch are enforced before token minting. | `scripts/common/operational_controls.py`, `scripts/ai/gateway_proxy.py`, workflow `if` gates, `tests/test_operational_controls.py`, `tests/test_ai_gateway.py` |

## Verification Gates

The completion gate is:

```text
Python 3.9: pytest -q
Python 3.11: pytest --cov=scripts --cov-fail-under=60 -q
ruff check scripts tests
mypy scripts
pip-audit for both conditional dependency sets
actionlint
git diff --check
```

Linux Docker boundary tests execute when Docker is present. The dedicated
`isolation-macos` CI job provisions a non-admin verifier account and executes
the real APFS/Seatbelt denial test without repository or cloud secrets.

Latest local verification on 2026-07-15:

| Gate | Result |
|---|---|
| Python 3.9 `pytest -q` | 663 passed, 1 skipped |
| Python 3.11 coverage suite | 663 passed, 1 skipped; 78.77% coverage |
| `ruff check scripts tests` | Passed with Ruff 0.15.20 |
| `mypy scripts` | Passed for 88 source files with mypy 1.19.1 |
| `pip-audit --requirement requirements/runtime.txt` | No known vulnerabilities with Python 3.11 and pip-audit 2.10.1 |
| Python 3.9 conditional dependency audit | No unignored vulnerabilities; three exact advisories are covered by reviewed compatibility backports |
| Bandit High severity / High confidence | No findings across 88 production modules with Bandit 1.8.6 |
| Semgrep `p/security-audit`, Error severity | No findings across 88 production modules with Semgrep 1.136.0 |
| actionlint with ShellCheck | Passed with actionlint 1.7.12 and ShellCheck 0.11.0 |
| `git diff --check` | Passed |

The Python 3.9 audit exceptions are `PYSEC-2026-2275`,
`PYSEC-2026-141`, and `PYSEC-2026-142`. The compatibility module backports
unpredictable ZIP extraction, cross-origin redirect header stripping, and
draining compressed responses without decompression; it also rejects the
affected optional Brotli implementation. Exact-version guards and regression
tests fail closed if the conditional pins or patched behavior drift.

An additional `linux/amd64` QEMU exercise built Valkey Search from a fresh
checkout, produced `libsearch.so` and `tests.out`, and passed all 21 unit-test
executables. Because emulation crossed the one-hour per-command bound, test
execution resumed from a quiesced build snapshot. Local-only overrides used 16
CPUs, 32 GiB memory, 4 GiB tmpfs, and eight build workers. This does not replace
native CI verification of the checked-in 2 CPU, 8 GiB, 1 GiB tmpfs, two-worker
profile.

The sole local skip is the macOS APFS/Seatbelt test. The real
validation-adapter boundary, CI-fix Docker boundary, and Docker
descendant-cleanup tests passed locally. The dedicated `isolation-macos` job
executes the macOS test.
