# valkey-ci-agent

Automation for the Valkey project's backports, CI failures, releases and
container images. Deterministic Python does everything that must be exact:
git, the GitHub API, validation, publication. Claude Code, run through AWS
Bedrock, is called only for the parts that need code understanding, such as
resolving a cherry-pick conflict, diagnosing a failing test, or wording a
release note. Every change the agent makes lands on a bot-owned branch and
goes through the target repository's normal CI and human review. The bot
never merges.

## Workflows

| Workflow | Runs | What it does | Docs |
|---|---|---|---|
| **Backport** | daily sweep, hourly poll | Cherry-picks PRs marked "To be backported" onto each release branch, resolves conflicts with AI, validates the result, and keeps one rolling PR per branch | [docs/backport.md](docs/backport.md) |
| **Backport eligibility** | daily | Comments on newly merged PRs that look worth backporting, naming the release branches that carry the changed code | [docs/backport.md](docs/backport.md#eligibility) |
| **CI fix** | on request, hourly follow-up | Fixes one failed CI job: reproduces it, has Claude diagnose and patch it inside the failed job's own environment, verifies and reviews the patch, then pushes it to a bot PR or posts it as a suggestion | [docs/ci-fix.md](docs/ci-fix.md) |
| **Test Failure Detector** | daily | Reads Daily CI's failures and opens or updates one issue per failing test | [docs/test-failure-detector.md](docs/test-failure-detector.md) |
| **Fuzzer monitor** | every 4 hours | Triages scheduled fuzzer runs and files an issue when a run looks like a real bug | [docs/fuzzer-monitor.md](docs/fuzzer-monitor.md) |
| **Release notes** | on demand | Cuts a release for Valkey or a module: generates the notes, bumps the version, opens the release PR, and applies maintainers' review comments to it | [docs/release-notes.md](docs/release-notes.md) |
| **Release automation** | on demand | Takes a release from "prepare" to a published tag with two human approvals and a tracking issue | [docs/release-automation.md](docs/release-automation.md) |
| **CVE scan** | weekly | Scans the container images, rebuilds each candidate to prove the fix, and dispatches a targeted rebuild | [docs/cve-scan.md](docs/cve-scan.md) |

Planned: a PR reviewer with a skeptic pass, and flaky-test detection across
Daily runs.

## Using it

Most of the time nothing needs to be done: the scheduled workflows run on
their own and open PRs or issues for maintainers. The manual entry points:

- **Ask for a CI fix** on a `valkey-io/valkey` PR by starting a comment with
  `@valkeyrie-ops fix`, optionally with a run or job link and a hint. Members
  of `valkey-io/contributors` only.
- **Cut a release** with Prepare Release from this repository's Actions tab,
  or run the notes cutter alone with Cut Release Notes.
- **Backport one PR** with Manual Backport.
- **Re-run anything** with `gh workflow run`; every scheduled workflow can be
  dispatched by hand, and most default to a dry run. Each doc's "Running it"
  section has the commands.

Where everything runs and when: [docs/setup.md](docs/setup.md). Which
repositories and branches are covered: [docs/registry.md](docs/registry.md)
(`repos.yml`).

## How it stays safe

- **Bot-owned branches only.** The agent writes `backport/...`,
  `agent/backport/...`, `agent/ci-fix/...` and `agent/release-cut/...`
  branches and opens PRs. It never pushes a release line directly, never
  rewrites a published sweep branch, and never merges.
- **The model holds no GitHub token.** Claude Code gets only the model's AWS
  credentials; code owns every commit and push. The CI fix agent also runs as
  a separate account that cannot read the workflow's environment.
- **Validate before publishing.** Registry build and test commands run before
  any push and a failure blocks it. A CI fix is reproduced, re-run with the
  patch, and reviewed by a second read-only session before it is pushed. A
  CVE rebuild is dispatched only after the fixed image was built and scanned.
- **Fail closed.** An unreadable registry files nothing; a held release PR
  opens as a draft; an unverified fix is reported as unproven.
- **Short-lived, scoped credentials.** Each job mints a GitHub App token for
  the repositories and permissions it needs; jobs that run the model hold no
  GitHub token at all.

The full design is in [docs/architecture.md](docs/architecture.md).

## Repository layout

```text
scripts/
  backport/               Backport sweep, poll, eligibility, CI follow-up
  ci_fix/                 CI fix engine and front doors
  test_failure_detector/  Daily CI failures -> issues
  fuzzer/                 Fuzzer run triage -> issues
  release_notes/          Release-notes cut and review handling
  release/                Release preparation and publication
  cve_scan/               Container CVE scan and verified rebuild
  ai/                     Claude Code orchestration and agent profiles
  common/                 Shared git, GitHub, artifact, isolation and polling code
.github/workflows/        The workflows
.github/actions/setup-agent
                          Shared setup: Python, dependencies, Claude Code CLI
docs/                     One document per workflow, plus setup and design
repos.yml                 Registry of repositories, release branches and boards
tests/                    One test module per source module
```

## Documentation

- [docs/setup.md](docs/setup.md) - secrets, App permissions, environments, schedules
- [docs/registry.md](docs/registry.md) - `repos.yml` reference
- [docs/architecture.md](docs/architecture.md) - design, AI layer, shared infrastructure
- [DEVELOPMENT.md](DEVELOPMENT.md) - local setup, tests, adding a workflow
- One document per workflow under [docs/](docs/)

## License

BSD 3-Clause. See [LICENSE](LICENSE).
