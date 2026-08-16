# Upstream PR Drafts

Three PRs, to be opened in this order (each names its companions). Nothing
is opened until you say so; branches are on the fork, review-ready.

---

## PR 1 — valkey-io/valkey-ci-agent
**Branch**: `sarthakaggarwal97:upstream-release-controller` → `valkey-io:main` (2 commits)
**Title**: Add a release controller driven by tracking issues

### Body

A stateless controller that runs Valkey releases end to end from a GitHub
tracking issue, with exactly three human touches per release: start it,
merge the release-notes PR, and click one environment approval.

On an hourly schedule with an internal poll loop (five passes per
hour), it recomputes release state from live GitHub evidence (no
database, no memory between runs) and takes the next step: cut notes,
bind the candidate to the exact merge SHA, dispatch a no-publish
qualification build, park publication behind a protected-environment
approval bound to a plan digest, publish with an atomic tag claim, then
verify every downstream output (tarballs, packages, containers, docs,
website, Bundle, Helm) fail-closed before closing the tracker.

<details><summary>Safety model</summary>

- Team membership is verified live at every privileged entry point;
  editing an issue or a policy's rendered output authorizes nothing.
- Every action is receipt-gated in two phases (intent, then done), so a
  crash can neither duplicate a dispatch nor suppress one permanently.
- Evidence binds to exact identities: candidate SHA, qualification
  manifest contents (downloaded and validated field by field), plan
  digests, and SHA-stamped run names. "A similar run at a similar time"
  never counts.
- Publication requires the controller's own receipt on read-back; an
  out-of-band release at a known SHA quarantines behind an alert with
  downstream dispatch unreachable.
- Fail-closed throughout: unreadable evidence blocks rather than passes.
  In fork testing this refused to proceed when GitHub's API reported a
  run green while two of its jobs were still executing.

</details>

<details><summary>Validation</summary>

- Live end-to-end cycles on a fork: 8.0.10, 9.0.6, and 8.0.11 published
  through the full chain, including branch-movement adoption, stale-gate
  replacement, bounded auto-remediation, and a real macOS build break
  caught by the candidate gate. This is the primary evidence.
- 2,177 unit tests (attack-test style: forged receipts, lookalike PRs,
  quoted markers, hostile payloads, bypass actors), ruff and mypy clean.
- Four AI-adversarial review rounds plus three maintainer-persona
  cold reads; every patchable finding is closed and the remainder is
  scoped below. The controller itself is deterministic and never
  invokes AI; the AI-assisted step is release-notes drafting, whose
  output lands behind a human-merged PR.
- The trust model (every token, every marker, blast radius) is
  docs/trust-model.md; the on-call runbook is docs/operations.md.

</details>

<details><summary>Scoped future work (deliberately not in this PR)</summary>

All four reviews converged on the same next step: a durable release
ledger. Issue comments are editable by repository writers, so receipts
raise the bar without being a capability boundary; a dedicated publisher
App whose key lives only in the protected environment, one immutable
release identity carried through every dispatch, and build-once/promote
artifact provenance are the follow-up design, kept out of this PR to
stay reviewable. Hash-pinned Python dependencies for the privileged
workflows are a declared follow-up as well.

</details>

Companion PRs: valkey-release-automation (qualification producer and
hardening) and valkey (thin Start Release relay). The controller works
without the relay; qualification requires the automation PR.

---

## PR 2 — valkey-io/valkey-release-automation
**Branch**: `sarthakaggarwal97:upstream-release-automation` → `valkey-io:main` (2 commits)
**Title**: Add release qualification and harden the release workflows

### Body

Two changes that let the release controller (companion PR) treat this
repository's builds as evidence, plus hardening that stands alone.

**Qualification** (new workflow): builds the exact candidate SHA across
the full archive and package matrix without publishing, and emits a
manifest (nonce, base version, tag, source SHA, automation SHA, job
counts) that the controller validates field by field. An empty matrix
fails rather than skipping.

**Hardening** (existing workflows): dispatch values enter shell through
quoted env with validation first; id-token is job-scoped; third-party
actions are SHA-pinned; production publication is gated behind the
release-publish environment with package publishing defaulted off;
downstream App tokens request minimal permissions; the approved source
SHA travels from the dispatch payload through every builder; and the
silent moving-branch fallback is removed from release builds (a failed
tag download now fails loudly).

<details><summary>Also in this PR</summary>

- The near-identical x86 and ARM archive workflows merge into one shared
  workflow; caller job names are unchanged (they are the controller's
  evidence contract).
- The website update gains its missing dependency edge on the Try Valkey
  upload and is gated off for release candidates.
- Fork pushes skip the dev archive builds that cannot succeed without
  the AWS role (opt back in with the RELEASE_DEV_BUILDS variable).

</details>

Known pre-existing issues in this repository (metadata regeneration,
concurrency, OIDC trust breadth) are documented findings from the review
rounds and deliberately not mixed into this diff; happy to file them as
issues.

---

## PR 3 — valkey-io/valkey
**Branch**: (cherry-pick `.github/workflows/release-start.yml` + the
trigger-build-release hardening from fork `unstable`) → `valkey-io:unstable`
**Title**: Add a Start Release relay to the release controller

### Body

A thin trampoline so maintainers start releases from this repository:
it forwards the dispatch (branch, intent, urgency, dry run, the
triggering actor, and its own run id) to the release controller, which
performs the live team authorization and opens the tracking issue here.
The relay runs with zero permissions, only from the default branch, and
carries no authority of its own.

Also hardens the existing build trigger: untrusted values (release tag
names, manual inputs) enter through env with validation before the
job's token is minted, the release commit is resolved and forwarded so
production builds from the exact SHA, and the relay verifies the
controller heard it (a dispatch that lands nowhere warns instead of
ending green). The yamlfmt diagnostic fix rides separately, not in
this PR.

---

## Open questions for you before opening
1. PR 3 branch: I'll cut a clean `upstream-start-relay` branch from
   valkey-io/valkey unstable with just those commits — confirm.
2. The fork policy file (`release_policy.fork.yml`) ships in PR 1;
   reviews suggested removing it from the production branch. Keep (it
   documents the test harness) or strip?
3. Reconcile cadence in PR 1 ships as the hourly poll loop; the
   event-driven redesign is in future work. OK as proposed?
