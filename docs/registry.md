# The registry (`repos.yml`)

`repos.yml` at the repository root is the single source of truth for which
repositories the backport workflows act on, their release branches and project
boards, and how a backport candidate is validated. The Test Failure Detector
and the CI fix engine read `ci_followup_ignored_jobs` from it too.
[`examples/repos.yml`](../examples/repos.yml) shows a multi-module setup.

## Example

```yaml
repos:
  - repo: valkey-io/valkey
    project_owner: valkey-io
    project_owner_type: organization
    language: c                            # named in the conflict-resolution prompt
    validation_setup_commands:             # optional; run once per clone
      - "./ci/setup-backport-validation.sh"
    build_commands:                        # run before any push; empty = rely on upstream CI
      - "make -j$(nproc)"
    validation_profile: valkey-core        # optional diff-aware checks
    validation_rules:                      # optional path -> commands
      - paths: ["src/rdb.c"]
        commands: ["./runtest --single unit/dump --clients 1"]
    generated_file_rules:                  # optional deterministic regeneration
      - paths: ["src/unit/*.c"]
        command: "python3 utils/generate-unit-test-header.py"
        outputs: ["src/unit/test_files.h"]
    test_path_patterns: ["tests/*.tcl"]    # optional; which paths count as tests
    repair_validation_failures: false      # one AI repair attempt on a validation failure
    automatic_ci_followup: false           # hand failed CI on bot-owned PRs to the CI fix engine
    ci_followup_ignored_jobs: ["*dco*"]    # informational Actions jobs, never fixed or filed
    backport_label: backport
    llm_conflict_label: ai-resolved-conflicts
    max_conflicting_files: 100
    branches:
      - branch: "8.1"
        project_number: 14
      - branch: "9.0"
        project_number: 18
```

## Keys

| Key | Required | Default | Meaning |
|---|---|---|---|
| `repo` | yes | | `owner/name` of the upstream repository |
| `project_owner` | yes | | Owner of the GitHub Project v2 boards |
| `project_owner_type` | no | `organization` | `organization` or `user` |
| `language` | yes | | Named in the conflict-resolution prompt |
| `branches` | yes | | One entry per release branch: `branch` and the board's `project_number` |
| `push_repo` | no | unset | A different-owner fork to push to; same-owner values are rejected |
| `validation_setup_commands` | no | `[]` | Run once per clone before validation |
| `build_commands` | no | `[]` | Run before every push; a non-zero exit blocks it |
| `validation_rules` | no | `[]` | `{paths, commands}`: commands to run when a candidate touches matching paths |
| `validation_profile` | no | `""` | A named profile that derives checks from the exact changed files; `valkey-core` is the one that exists |
| `generated_file_rules` | no | `[]` | `{paths, command, outputs}`: regenerate declared outputs when matching paths change |
| `test_path_patterns` | no | `tests/*.tcl`, `src/unit/test_*.c`, `.cc`, `.cpp` | Globs for paths that count as tests, for the repair and test-adaptation rules |
| `repair_validation_failures` | no | `false` | Let Claude Code make one edit-only repair attempt before a failing candidate is dropped |
| `automatic_ci_followup` | no | `false` | Inspect CI on open bot-owned PRs into each branch |
| `ci_followup_ignored_jobs` | no | `[]` | Actions job-name patterns the follow-up and the detector skip |
| `backport_label` | no | `backport` | Label on backport PRs |
| `llm_conflict_label` | no | `ai-resolved-conflicts` | Label added when a conflict was AI-resolved |
| `max_conflicting_files` | no | `100` | A candidate with more conflicting files is dropped |

## Semantics

**Repository model.** Agent branches are pushed to `repo` under the
`agent/backport/...` namespace and PRs are opened there. `push_repo` is an
escape hatch for a real different-owner fork; when set, the agent
fast-forwards that fork's release branch to match upstream before
cherry-picking. Same-owner values are rejected so staging repositories do not
become the normal model.

**Validation** runs in the order setup, build, rules, profile, generated
files, against each candidate's own diff. How the sweep uses the result, the
`valkey-core` profile's checks, and the repair and test-adaptation passes are
described in [backport.md](backport.md#keeping-the-sweep-branch-green).
`generated_file_rules` must list every tracked output a generator may edit;
the validator runs the generator twice and fails if it touches anything else
or does not converge.

**CI follow-up.** `automatic_ci_followup` is opt-in per repository. The
follow-up, the detector-issue front door and the Test Failure Detector's
job-level check all skip jobs matching `ci_followup_ignored_jobs`, which
should name the informational Actions jobs the bot must never try to repair:
for Valkey core, the `daily.yml` result aggregators and the coverage upload.
Patterns match Actions job names only. Checks published by other GitHub Apps
(DCO, Codecov) are never workflow jobs, so they cannot be candidates in the
first place; the `*dco*` patterns stay as a cheap guard in case one is ever
reimplemented as a job.

**Onboarding a new release line.** Add a `branches` entry with the new
branch's project number. For Valkey core, [release automation](release-automation.md)
opens an onboarding issue and a registry PR for every `X.Y.0` release.
