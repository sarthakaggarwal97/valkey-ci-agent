# Setup and operations

What the agent needs to run in GitHub Actions on `valkey-io/valkey-ci-agent`,
and when each workflow runs.

## Secrets and variables

| Type | Name | Purpose |
|---|---|---|
| Secret | `AWS_ROLE_ARN` | OIDC role ARN with Bedrock `InvokeModel` permission |
| Secret | `VALKEYRIE_BOT_APP_ID` | Valkeyrie Bot GitHub App ID |
| Secret | `VALKEYRIE_BOT_PRIVATE_KEY` | Valkeyrie Bot GitHub App private key |
| Variable | `AWS_REGION` | AWS region, for example `us-east-1` |
| Variable | `CI_AGENT_CLAUDE_MODEL` | Optional model override for the agents, for example `opus` |
| Variable | `CI_FIX_ISSUES_AUTO_DISPATCH` | `true` lets the detector-issue dispatcher's schedule start fixes |
| Variable | `CI_FIX_ISSUES_MAX_RUNS` | Detector-issue fixes active at once (default 2) |
| Variable | `CI_FIX_ISSUES_PUBLISH` | `comment` (default) posts a detector issue's fix on the issue; `draft-pr` opens a draft PR |

Every workflow authenticates to GitHub by minting a short-lived App
installation token scoped to the repositories and permissions that job needs.
AWS access uses OIDC and `AWS_ROLE_ARN`; no long-lived cloud credential is
stored.

### Model

Claude Code defaults to Fable 5 through the `us.anthropic.claude-fable-5`
Bedrock inference profile. The AWS account must have access to that profile
and must use Bedrock `provider_data_share` data retention; Fable 5 is not
available under zero data retention. Set `CI_AGENT_CLAUDE_MODEL=opus` to fall
back to the pinned Opus 4.8 profile. The workflows pin Claude Code CLI 2.1.175.

## GitHub App permissions

One App, the Valkeyrie Bot, serves every workflow today. Grant it only what
the workflows request:

| Installation target | Permissions | Used by |
|---|---|---|
| `valkey` | `administration:read`, `actions:read`, `checks:read`, `contents:write`, `issues:write`, `metadata:read`, `pull-requests:write` | backport, CI fix, detector, release notes, release automation |
| `valkey-search`, `valkey-json`, `valkey-bloom` | `contents:write`, `metadata:read`, `pull-requests:write` | backport, release notes |
| any repository using advisory-backed cuts | `repository-advisories:read` | release notes (advanced) |
| `valkey-fuzzer` | `actions:read`, `contents:read`, `issues:write` | fuzzer monitor |
| `valkey-container` | `actions:write`, `contents:read`, `metadata:read` | CVE scan rebuild dispatch |
| `valkey-ci-agent` | `contents:write`, `issues:write`, `metadata:read`, `pull-requests:write` | release automation (tracking issue, onboarding PR) |
| `valkey-release-automation` | `actions:read`, `contents:read`, `metadata:read` | release automation |
| organization | `members:read` (release and CI-fix authorization); `organization-projects:read` (board discovery, eligibility, first-GA onboarding) and `organization-projects:write` (mark done) | backport, CI fix, release automation |

The release-notes installation must include every repository that can be
selected, or token creation fails for that repository.

## Environments

| Environment | Rule | Purpose |
|---|---|---|
| `release-control` | default branch only | Where the release control App token is minted |
| `release` | default branch only; required reviewer; no self-review; no admin bypass | Publication approval; the publication job mints its token here |
| `cve-rebuild-dispatch` | deployment branch `main` only | Credential boundary for the CVE rebuild dispatch (not an approval gate) |

Release-specific hardening (tag ruleset, actor policy, splitting the App) is
in [release-automation.md](release-automation.md#security-setup).

## Schedules

| Workflow | Schedule (UTC) | Doc |
|---|---|---|
| Backport Sweep | daily 09:00 | [backport](backport.md) |
| Backport Poll | hourly :00 (polls again at +30 min) | [backport](backport.md#poll) |
| Backport CI Follow-up | hourly :20 | [backport](backport.md#automatic-ci-follow-up) |
| Backport Mark Done Poll | hourly :30 | [backport](backport.md#mark-done) |
| Backport Eligibility | daily 00:30 | [backport](backport.md#eligibility) |
| CI Fix Comment Poll | hourly :00 | [ci-fix](ci-fix.md) |
| CI Fix Issue Dispatch | hourly :23 (inert until `CI_FIX_ISSUES_AUTO_DISPATCH=true`) | [ci-fix](ci-fix.md#detector-issue-fixes) |
| Test Failure Detector | daily 23:00 | [test-failure-detector](test-failure-detector.md) |
| Monitor Valkey Fuzzer Runs | every 4 hours at :41 | [fuzzer-monitor](fuzzer-monitor.md) |
| Release Notes Review Poll | hourly :00 (then every 5 min for 55) | [release-notes](release-notes.md#review-comments-on-a-release-pr) |
| Refresh Release Progress | hourly :00 (then every 5 min for 55) | [release-automation](release-automation.md) |
| CVE Scan | Mondays 06:00 | [cve-scan](cve-scan.md) |

Dispatch-only: Manual Backport, Manual Revert Commit, CI Fix Bot (`ci-fix.yml`),
CI Fix Issues, Cut Release Notes (and Advanced), Address Release Note Reviews,
Prepare Release, Publish Release. Reusable (`workflow_call`): Backport PR
(`backport.yml`), CI Fix Run (`ci-fix-run.yml`).

Pollers that run inside one runner for most of an hour (`backport-poll`,
`release-notes-review-poll`, `release-progress`) use the shared
`scripts/common/polling.py` loop, which bridges delays in GitHub's
scheduled-workflow queue while staying below the App token's one-hour
lifetime. Manual dispatches of those workflows are one-shot.

## Shared workflow setup

Every job checks out the agent repository and runs the local composite action
`.github/actions/setup-agent`, which installs the pinned Python toolchain,
the project's dependencies (`runtime` or `dev`), and optionally the pinned
Claude Code CLI. Its `path` input names the checkout when it is not the
workspace, and `system-packages` installs Python, Node and the agent
account's tools from the distribution instead, for a job that runs in a
container. The workflow-standards tests scan workflows and the action so
external actions and Claude installs do not drift.
