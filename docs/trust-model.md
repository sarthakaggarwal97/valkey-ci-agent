# Release Controller Trust Model

What the release flow trusts, which credentials it mints, what its markers
and receipts actually guarantee, and what breaks if this repository is
compromised. Derived from the workflows under `.github/workflows/` and the
code under `scripts/release/`; when this document and the workflow files
disagree, the workflow files win and this document is stale.

## Token inventory

Every token the release workflows mint, from the actual
`create-github-app-token` steps and `permissions:` blocks. The App is the
VALKEYRIE bot App (`VALKEYRIE_BOT_APP_ID` / `VALKEYRIE_BOT_PRIVATE_KEY`
repository secrets); on forks (owner is not `valkey-io`) every App mint
falls back to an `AUTOMATION_PAT` / `VALKEY_GITHUB_TOKEN` secret when the
App is not configured.

| Workflow / job | Token | App permissions requested | Repositories | Held by | Environment-gated? |
|---|---|---|---|---|---|
| release-start.yml / start | runner `GITHUB_TOKEN` | n/a (`contents:read`) | this repo | start job | No |
| release-start.yml / start | App token | contents:read, issues:write, members:read, metadata:read | valkey | start job (authorize, derive version, create tracker) | No |
| release-start.yml / cut-notes | runner `GITHUB_TOKEN` + OIDC | n/a (`contents:read`, `id-token:write` for Bedrock) | this repo | reusable release-notes-cut.yml jobs | No |
| release-notes-cut.yml / cut | App token | contents:write, pull-requests:write, metadata:read (plus repository-advisories:read on the advisory path) | valkey | notes cut (push prep branch, open the notes PR) | No |
| release-reconcile.yml / reconcile | runner `GITHUB_TOKEN` (`AGENT_GITHUB_TOKEN`) | n/a (`contents:read`, `actions:write`) | this repo | reconcile (dispatches release-publish.yml in this repo) | No |
| release-reconcile.yml / reconcile | App token (valkey, read + tracker) | contents:read, issues:write, pull-requests:read, checks:read, actions:read, metadata:read | valkey | reconcile (`RELEASE_GITHUB_TOKEN`) | No |
| release-reconcile.yml / reconcile | App token (downstream, write) | contents:write, pull-requests:write, checks:read, actions:write, metadata:read | valkey-release-automation, valkey-bundle, valkey-helm | reconcile (`RELEASE_DOWNSTREAM_TOKEN`: qualification/build dispatches, bundle repository_dispatch, helm bump PR) | No |
| release-adopt.yml / adopt | runner `GITHUB_TOKEN` | n/a (`contents:read`) | this repo | adopt job | No |
| release-adopt.yml / adopt | App token | contents:read, issues:write, pull-requests:read, checks:read, actions:read, members:read, metadata:read | valkey | adopt (live team check, adoption comment) | No |
| release-publish.yml / validate | runner `GITHUB_TOKEN` | n/a (`contents:read`) | this repo | validate job | No |
| release-publish.yml / validate | App token (read-only validation) | contents:read, issues:write, pull-requests:read, checks:read, actions:read, members:read, metadata:read | valkey | validate (plan + approval evidence) | No |
| release-publish.yml / validate | App token (downstream reads) | actions:read, contents:read, metadata:read | valkey-release-automation, valkey-ci-agent | validate (qualification revalidation) | No |
| release-publish.yml / publish | runner `GITHUB_TOKEN` | n/a (`contents:read`, `actions:read` for the approvals endpoint) | this repo | publish job (approver resolution) | Yes (`release`) |
| release-publish.yml / publish | App token (publication scope) | **contents:write**, issues:write, pull-requests:read, checks:read, actions:read, members:read, metadata:read | valkey | publish job: the ONLY step in the flow that can create the release and its tag | Yes (`release`) |
| release-publish.yml / publish | App token (downstream reads) | actions:read, contents:read, metadata:read | valkey-release-automation, valkey-ci-agent | publish job (revalidation) | Yes (`release`) |

Two structural properties fall out of the table:

- The one `contents:write`-on-valkey token in the whole flow is minted
  inside the environment-gated publish job, after a human approves, and
  scoped to the single repository publication writes to.
- The scheduled path (reconcile) deliberately holds no `contents:write` on
  valkey at all, so unattended code cannot create the release or its tag
  through its own credentials; its write scope is confined to the three
  downstream repos it dispatches and PRs against.

## Marker and receipt inventory

All markers live in issue comments (or the tracker body) under the
`valkey-ci-agent:release` namespace, assembled by `scripts/release/issue.py`.
Read-back is restricted to trusted authors: the static bot identities, the
`RELEASE_BOT_LOGIN` the workflows export for a fork's own App, and the
authenticated login on PAT runs. Markers must start a line outside code
fences (`marker_present`), so quoting one never counts.

**Honest limits, up front.** GitHub grants everyone with repository write
access the ability to edit and delete issue comments, including
bot-authored ones. Every marker below therefore RAISES THE BAR (a drive-by
account cannot forge state; casual tampering is visible in comment edit
history) rather than forming a capability boundary. The capability
boundaries in this system are the protected `release` environment, the
token scoping above, and valkey's tag ruleset; the receipts are detection
and bookkeeping. A ledger outside editable comments is the known redesign
that would upgrade them.

| Marker | Writer | Reader | What forging it yields an attacker | What deleting it yields |
|---|---|---|---|---|
| `release:<branch>` (tracker body identity) | controller, at tracker creation | tracker discovery (fallback; the `release-tracker` + `release:<branch>` label pair is primary) | a lookalike tracker can confuse discovery; it cannot authorize anything (nothing is parsed from bodies to make decisions) | discovery falls back to labels; no effect |
| `binding version/stage/notes_pr/merge_sha` | controller (`write_binding`) | reconciliation, before any PR scan | rebinding the release to a different notes PR or identity; the bound PR is still revalidated every pass (repo, base, head shape, author trust) and drift freezes with an alert | reconciliation rescans; the scan enforces upstream-head + trusted-author rules, so eviction cannot swap in a fork PR |
| `adopt:<sha>` | `adopt` entry point, after a live team-membership check | candidate resolution | adopting an arbitrary SHA as candidate; it must still be the exact branch head or the pinned notes merge, must pass qualification on that exact SHA, and publication still requires human approval | the candidate reverts to INVALIDATED and the release blocks until a real adoption |
| `publication-receipt` + `Published **tag** at \`sha\`` carrier line | protected publish path, post-write | `_published_status`: a release with no matching trusted receipt is quarantined as unverified | an out-of-band release reads as controller-published and downstream verification proceeds on it; this is exactly the editable-comment limit above, and why the receipt is detection, not a boundary | a legitimate release quarantines as unverified (alert noise, downstream verification withheld) until the publish workflow is re-run to restore it |
| `qual-nonce:<sha>:<nonce>` | qualification dispatch receipt (intent comment) | `evaluate_qualification` via `recorded_qualification_nonce` | a wrong nonce refuses legitimate evidence (denial of progress); accepting a hostile run additionally requires a manifest passing every identity and count binding from a default-branch run of the automation repo | evaluation falls back to legacy behavior (nonce is evidence detail only) |
| `autofix-intent:<key>:<fp>` / `autofix-done:<key>:<fp>` | two-phase dispatch receipts (`_autofix_two_phase`) | the same helper on later passes | forging `done` suppresses a needed dispatch (quiet denial of progress; the standing failure notification still fires); forging `intent` triggers at most one bounded extra dispatch | deleting `done` risks one bounded duplicate dispatch; deleting `intent` the same |
| `notify:<fp>`, `wedge:<fp>`, `nudge:<fp>` | one-shot notification gates | the same posting paths | forging suppresses a team ping for that exact state (silence attack); any state change re-pings | the next pass re-posts the notification |
| `notify-gen:<n>` / `notify-state:<s>` | recovery-generation bookkeeping | notification fingerprinting | suppressing or duplicating re-pings after recovery | bookkeeping reinitializes from notification history |
| `complete` | completion receipt | start-release guard, close path, heal path | faking completion lets a new release start over an abandoned one; publication evidence is unaffected | a finished release reads abandoned and the next start refuses until resolved |
| `closed-warning` | abandoned-tracker warning gate | the same path | suppresses the one-shot warning | the warning re-posts once |
| `approval-evidence` | validate job (edited in place) | humans (the approver) | misleading checklist text for the approver; the run page's job summary, rendered from the same plan, is the second copy, and execution re-validates and refuses on any drift from the approved digest | the next validation re-posts it |

## Blast radius: compromise of this repository's default branch

A change merged to this repo's default branch runs as the release
workflows with access to the repository secrets, including the App private
key. Stated plainly:

**What it can do.**

- Mint App installation tokens with ANY permissions the App installation
  grants, not just the narrowed sets the honest workflows request: the App
  private key is an ordinary repository secret, readable by any workflow
  on the default branch. Since the publish job's token proves the
  installation can grant `contents:write` on valkey, a compromised default
  branch can mint that same power without the environment gate. Branch
  protection and review on THIS repository are therefore part of the
  release trust base, not a convenience.
- Exercise everything the scheduled reconcile legitimately holds: write to
  valkey-release-automation, valkey-bundle, and valkey-helm; dispatch
  qualification and build runs; post on the valkey tracker.
- Forge or delete any marker or receipt (see the honest limit above).

**What it cannot do.**

- Approve the `release` environment. Required reviewers are humans, the
  environment prevents self-review and admin bypass, and deployments are
  restricted to specific branches; `ensure_environment_protected` refuses
  publication when any of that is loosened. The gate constrains the
  controller-driven publication path end to end: the plan digest binds
  what the approver saw, and execution refuses on any drift.
- Move or delete valkey's protected release tags: upstream's active tag
  ruleset (probed by `tag_ruleset_protected`, and only claimed when it
  carries creation, update, and deletion rules with zero bypass actors)
  is enforced by GitHub, outside this repo's reach.
- Escape detection cheaply. An out-of-band release or tag surfaces on the
  next honest reconcile pass as an untrusted-tag alert or an unreceipted
  quarantine, and comment edit history records marker tampering. Detection
  assumes an honest controller is running again; it is a tripwire, not a
  prevention.

The corollary the scheduled-token design encodes: unattended code paths
never hold the credential that publishes. Publication happens only where
a human has clicked Approve, and the only way around that gate runs
through a reviewed merge to this repository's default branch.
