# Release automation

Takes a Valkey core release from "prepare" to a published GitHub release with
two human approvals and no durable controller. The identities that bind the
pipeline are the preparation PR and the exact release-branch commit.

| Workflow | File | Runs | Purpose |
|---|---|---|---|
| Prepare Release | `release-prepare.yml` | by hand | Authorize, derive the version, open the tracking issue, cut the notes PR |
| Refresh Release Progress | `release-progress.yml` | hourly (then every 5 minutes for 55) | Derive status from live state; dispatch publication once the notes PR merges |
| Publish Release | `release-publish.yml` | dispatched by the watcher, or by hand | Validate, qualify, approve, tag, publish |

## The maintainer's path

1. Dispatch **Prepare Release** from this repository's Actions tab with the
   release line (`9.1`), the kind (`rc`, `ga`, `patch`) and the urgency.
2. Review and merge the `agent/release-cut/...` notes PR.
3. Approve the `release` environment.
4. Approve `release-publish` in `valkey-release-automation`.

Everything between those decisions advances automatically. The
`Release <tag>` tracking issue is the one place to find the current run,
failure and next action.

## How it works

```text
Prepare Release (dispatched by a maintainer)
  -> authorize the dispatcher live against valkey-committers or valkey-release
  -> derive the version from the branch and existing tags
  -> create or update the non-authoritative "Release <tag>" tracking issue
  -> invoke the release-notes cutter: open or refresh agent/release-cut/<version>-<stage>
  -> maintainer review and merge

Refresh Release Progress (one hourly runner; five-minute polls for 55 minutes)
  -> derive status from live PR, branch, workflow and release state
  -> if the canonical notes PR merged and its merge SHA is still branch HEAD:
       dispatch Publish Release <branch, merge SHA> once
  -> update the tracking issue with failure and next action

Publish Release <branch, candidate SHA>
  -> require candidate == the canonical notes PR's merge commit
  -> require candidate == current branch head
  -> validate version.h, the notes section, tag availability; report ci.yml status
  -> call valkey-release-automation's secretless qualification workflow synchronously
  -> protected `release` environment approval
  -> re-authorize the recorded approver live; recompute and compare the plan digest
  -> atomically create the tag at the candidate SHA; publish the GitHub release
  -> the release event starts production automation in valkey-release-automation
       -> archives, RPM/DEB, hashes, container, docs, website, Try Valkey
       -> wait for the public container images, then update Bundle
       -> draft Helm bump for GA; a maintainer makes it ready once its image is public
  -> for X.Y.0, open the bounded backport-registry onboarding issue and PR
```

**Prepare Release** authorizes the dispatching maintainer against
`valkey-io/valkey-committers` or `valkey-io/valkey-release`, derives the next
version from the line and tags, opens one tracking issue, and invokes the
[release-notes cutter](release-notes.md). Real preparation is the default; a
dry run only displays the derived release.

**Refresh Release Progress** derives status from live workflow, PR, branch and
release state; it holds no state of its own. The issue displays progress and
the next action; its bot-owned status comment carries the tracker identity.
The editable issue body and checkboxes cannot authorize a transition, select a
SHA, or skip a gate. The runner stays alive for 55 minutes, polling every five
minutes, to bridge delays in GitHub's scheduled-workflow queue while staying
under the App token's one-hour lifetime.

**Publish Release** requires the exact SHA to be both branch HEAD and the
canonical notes PR's merge commit. It verifies `version.h`, the notes section,
advisory `ci.yml` status on that SHA, tag availability and the latest-release
decision, then calls the synchronous `valkey-release-automation`
qualification workflow and waits for that exact call to pass before entering
the protected `release` environment. After approval it repeats every
validation, checks the approver is still a live team member, requires the
approved plan digest to match, atomically creates the tag, and publishes the
release. Production then runs in `valkey-release-automation`, which resolves
the published tag to its commit and requires any supplied SHA to match. The
Helm PR stays draft until the public container image exists; that small human
ordering check replaces a long-lived post-release polling loop. Once
production starts, the dashboard shows links for every downstream target
(hashes, container, documentation, website, Helm, Bundle); the production run
remains the completion authority.

If the release branch moves after the merge, automatic publication stops and
the dashboard asks for a fresh preparation PR. If a build or qualification
fails, the dashboard links the failed run and gives the next action. The
watcher does not repair failures or infer success from issue text. A moved
branch, failed check or failed qualification is recovered by fixing the
repository state and rerunning the appropriate workflow.

### Aborting a release

Close the tracking issue. The progress workflow scans only open trackers, so
closing it stops reconciliation and any new publication dispatch. To stop all
release reconciliation during an incident, disable **Refresh Release
Progress** in `valkey-ci-agent`; re-enable it after every abandoned tracker
is closed. There is no abort command.

## Security setup

- Restrict the manual workflows with the GitHub Actions actor policy so only
  release maintainers can run them. The live team check stays as defense in
  depth.
- **Environments.** `release-control` must allow only the exact default
  branch; the control App is available only through it and is not a tag
  ruleset bypass actor. `release` must allow only the default branch, require
  a reviewer, prevent self-review, and disallow admin bypass; the publication
  job mints its token there. `valkey-release-automation` keeps its own
  `release-publish` production approval. GitHub environments need only one of
  their configured reviewers, so the two approvals are the separate
  `release` and `release-publish` gates, not two names on one environment.
- **One App today.** Control and publication run as one App (the Valkeyrie
  Bot) using the shared `VALKEYRIE_BOT_APP_ID` and
  `VALKEYRIE_BOT_PRIVATE_KEY` secrets. To split the roles without code
  changes, create a dedicated publication App, store its id and key as
  environment-scoped secrets of the same names inside `release` (environment
  secrets shadow repository secrets), and re-point the ruleset bypass to it.
- **App permissions.** The full matrix is in [setup.md](setup.md#github-app-permissions).
  The App named as the tag-ruleset bypass needs `administration:read`,
  `actions:read`, `checks:read`, `contents:write`, `metadata:read` and
  `pull-requests:read`, plus organization `members:read`, on `valkey`.
- **Tag ruleset.** The workflow verifies the release-tag ruleset is ACTIVE and
  IMMUTABLE (restricts creation, update and deletion) before publishing,
  which needs only `administration:read`. The bypass list is deliberately not
  enumerated: GitHub reveals `bypass_actors` only to tokens that could edit
  the ruleset, so checking it would force `administration:write` onto an
  unattended credential, and the drift it would catch comes from the same
  admin role that can delete the ruleset outright. Keep the bypass list to
  exactly the publication App by admin review.
- **Qualification** is a secretless synchronous reusable-workflow call, so it
  needs no cross-repository Actions-write token. Deploy
  `valkey-release-automation` first: its `qualify-release.yml` must expose
  `workflow_call`, and this repository's Actions policy must allow
  first-party reusable workflows from `valkey-io`.

Releases start from this repository's own Actions UI, so there is no relay
workflow, no forwarded-identity input, and no cross-repository Actions-write
grant.

## Internals

- `scripts/release/main.py` - CLI used by preparation and publication
- `scripts/release/authorize.py` - live team authorization
- `scripts/release/versioning.py` - derive the next version from branch and tags
- `scripts/release/tracker.py` - the tracking issue and its status comment
- `scripts/release/checks.py` - version, notes, tag and CI validation
- `scripts/release/publish.py` - plan, plan digest, prepare and publish
- `scripts/release/policy.py` - authorized teams and release policy (`release_policy.yml`)
- `scripts/release/models.py` - `ReleaseIntent` and friends
- `scripts/backport/onboarding.py` - the first-GA backport onboarding issue and registry PR
