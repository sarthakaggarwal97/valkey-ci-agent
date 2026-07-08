# Release Notes Automation Design

## Core Principle

Release notes should be derived from what actually landed on the release branch,
then enriched with explicit source metadata.

Do not depend on squash commit messages.
Do not use GitHub Projects as the primary source of truth.

Git and merged pull requests answer what happened. Projects answer what should
happen, so Projects are useful as an audit layer but not as the release-note
source of truth.

## Core Contract

The release-note generator must identify changes from release-branch evidence:

- the resolved release range
- commits in that range
- pull requests merged into the release branch
- backport manifests embedded in backport pull request bodies

Commit subjects may be used as a last-resort hint, but never as the only
identity for a release-note entry.

## ReleaseChange Model

Every logical release-note candidate is represented as a `ReleaseChange`.

```text
ReleaseChange
- id: source PR number if available, otherwise release PR number or deterministic local id
- landed_by_pr: PR merged into the release branch
- source_pr: original unstable PR, optional
- release_commits: post-cherry-pick SHAs that exist on the release branch
- source_commits: original source branch SHAs, optional metadata only
- kind: source_pr | backport_sweep_entry | direct_release_pr | security | release_admin | local_change
- title: source PR title when available
- body: source PR body when available
- author: source PR or release PR author
- labels: labels used for release-note disposition
- disposition: include | exclude | triage
- evidence: why the tool believes this change landed
```

`release_commits` are the only commit SHAs used for coverage. They must be SHAs
that exist in the release branch range. `source_commits` are useful for humans
and traceability, but they cannot prove release-branch coverage because squash
merges and cherry-picks can rewrite them.

## Backport Manifest

Backport tooling should write a hidden, machine-readable manifest into every
backport PR body. This applies to both single backport PRs and sweep PRs.

Visible Markdown tables should stay for humans, but automation should read the
hidden JSON manifest.

```html
<!-- valkey-release-manifest
{
  "schema": 1,
  "type": "backport-sweep",
  "target_branch": "8.1",
  "entries": [
    {
      "source_pr": 3342,
      "status": "applied",
      "release_commits": ["830d1fec..."],
      "source_commits": ["abc123..."],
      "release_notes": "inherit"
    },
    {
      "source_pr": 2872,
      "status": "applied",
      "release_commits": ["bb71d903..."],
      "source_commits": ["def456..."],
      "release_notes": "inherit"
    },
    {
      "local_change": "Add objectGetVal and objectSetVal wrappers",
      "status": "preparation",
      "release_commits": ["f7d782b32..."],
      "release_notes": "no"
    }
  ]
}
-->
```

Manifest field rules:

- `release_commits` are release-branch SHAs created by the backport process.
- `source_commits` are optional source-branch SHAs.
- `release_notes: inherit` means use the source PR labels.
- `release_notes: yes` explicitly includes the entry.
- `release_notes: no` explicitly excludes the entry.
- `release_notes: triage` forces maintainer review.
- `local_change` entries must have deterministic ids derived from their release
  PR, release commits, and description.

Malformed manifests in new backport PRs should block release generation. Legacy
sweep PRs may use visible Markdown tables as a fallback, but that fallback should
be reported clearly.

## Release Modes

### Patch GA

For a normal patch release from an existing release branch:

```text
target_branch = 8.1
version = 8.1.9
base = previous patch tag on that branch, for example 8.1.8
head = origin/8.1
range = 8.1.8..origin/8.1
```

### RC1

For the first release candidate:

```text
target_branch = pre-release-M.m.p
base = previous release tag or explicit base_ref
head = selected source branch
range = base..head
```

### RC2+ Or GA After RC

Do not rely on RC tags unless the workflow actually creates and maintains them.
The stable default base should be the previous generated release-cut state.

```text
base_sha = head_sha from the previous valkey-generated-release-cut manifest
head = current release or pre-release branch
range = base_sha..head
```

If that previous generated state cannot be found, require an explicit `base_ref`
instead of guessing.

## Release Cut Flow

1. Resolve the release range.
   Record the exact `base_ref`, `base_sha`, `head_ref`, and `head_sha`.

2. List commits in the range.
   These commits are evidence for coverage, not the primary identity of changes.

3. Query merged PRs into the release branch.
   Keep PRs whose merge, squash, associated, or release-branch commits intersect
   the resolved range.

4. Normalize each merged release-branch PR.
   If the PR has a release manifest, expand it. If it is a direct release PR,
   treat the PR itself as a candidate. If it only changes release-admin files,
   classify it as `release_admin`. If classification is unclear, send it to
   triage or block the cut.

5. Expand backport sweep PRs.
   A sweep PR is a container. It should not produce a release-note bullet for
   itself. Each applied source PR entry becomes its own `ReleaseChange`.

6. Fetch source PR metadata.
   For expanded entries, fetch the source PR title, body, author, labels, and
   original PR URL.

7. Apply release-note disposition.
   Include entries with `release-notes`. Exclude entries with
   `no-release-notes`. Entries with both labels, neither label, missing source PR
   metadata, or an explicit `triage` marker go to triage unless a documented
   migration override is enabled.

8. Run coverage checks.
   Every commit in the release range must be covered by a merged PR, manifest
   entry, release-admin classification, merge commit classification, empty
   cherry-pick classification, revert-pair classification, or explicit local
   change. Unknown commits block or loudly triage the release cut.

9. Deduplicate entries.
   Deduplicate by `source_pr` when available. If the same source PR appears more
   than once, the latest `applied` entry wins. Failed, skipped, or needs-attention
   entries never beat an applied entry.

10. Generate release-note text.
    Generate notes only for included `ReleaseChange` entries. Use source PR
    title and body as the primary context. Optionally inspect source diffs. Never
    use the commit message as the only identity.

11. Handle security fixes separately.
    Security fixes should come from the explicit security input and should be
    factual, not AI-authored. If a source PR is listed as a security fix, exclude
    it from normal AI-generated bullets to avoid double-listing.

12. Render `00-RELEASENOTES`.
    Prepend the new section. Preserve older sections. Do not regenerate old
    release-note content from stale commit text.

13. Store a generated release-cut manifest.
    The release PR body should include a hidden generated manifest that records
    the exact range, included entries, excluded entries, triage entries,
    uncovered commits, label snapshot, and tool version.

Example generated release-cut manifest:

```html
<!-- valkey-generated-release-cut
{
  "schema": 1,
  "version": "8.1.9",
  "target_branch": "8.1",
  "base_ref": "8.1.8",
  "base_sha": "1111111...",
  "head_ref": "origin/8.1",
  "head_sha": "2222222...",
  "included": [
    {
      "id": "source_pr:3342",
      "source_pr": 3342,
      "landed_by_pr": 3737,
      "labels": ["release-notes"],
      "disposition": "include"
    }
  ],
  "excluded": [],
  "triage": [],
  "uncovered_commits": []
}
-->
```

## Backport Sweep Handling

A backport sweep PR is a transport container for many source PRs.

For example, a sweep PR like `valkey#3737` should expand into source PR entries
such as `#3342`, `#2872`, `#3359`, and `#3209`. Each source PR should then be
classified individually by labels or explicit manifest disposition.

The sweep PR itself is evidence that those entries landed on the release branch.
It is not a release-note bullet.

## Legacy Backport Sweep Fallback

Existing sweep PRs may not have manifests. For those PRs, the tool may parse the
visible `Applied` table as a legacy fallback.

Fallback behavior should be conservative:

- report that legacy parsing was used
- treat missing labels as triage by default
- require human review for ambiguous entries
- never silently infer source PRs only from commit subjects

This allows old release branches to keep working while making new backport PRs
use the stronger manifest contract.

## Projects Role

Projects should be audit-only.

Useful Project audit findings:

- Project expected a PR to be backported, but no landed evidence was found.
- A PR landed on the release branch, but was not represented in the Project.
- A Project item is marked complete, but no matching release-branch evidence
  exists.

Projects should not decide release notes because Project state can drift, be
edited manually, or represent intent rather than actual landed code.

## Idempotency

The generated release-cut manifest should snapshot:

- resolved range
- source PR ids
- release PR ids
- label set used for each decision
- resolved disposition
- generated release-note entry ids
- tool version

On rerun:

- same range plus unchanged labels should produce the same candidate set
- changed labels should be reported as an explicit diff
- new commits in the range should appear as new evidence
- removed or rewritten generated state should block or require explicit override

Live labels are allowed to change, but they should not silently change a
previously generated release cut without being reported.

## Manifest Integrity

PR bodies are human-editable, so manifests need validation.

Rules:

- Backport tooling is the only normal writer of release manifests.
- New malformed manifests block release generation.
- Generated release-cut manifests are regenerated by the tool, not hand-edited.
- The tool should validate schema version, required fields, commit SHAs, target
  branch, entry status, and release-note disposition.
- If a manifest claims release commits outside the resolved range, the entry
  should block or triage.

## Failure Policy

Block the release cut when:

- the release range cannot be resolved
- release-line branch state is ambiguous
- a new manifest is malformed
- commits in the release range are uncovered
- source PR metadata cannot be fetched for an included or triage-required entry
- labels are missing or conflicting under the configured policy
- release-admin detection is uncertain
- RC2+ or GA-after-RC cannot find the previous generated `head_sha` and no
  explicit `base_ref` was provided

Warn, but do not necessarily block, when:

- a legacy Markdown fallback was used
- duplicate source PRs were deduplicated
- Projects audit shows mismatches
- live labels differ from the previous generated snapshot
- an AI-generated note has low confidence and needs maintainer review

## What This Fixes

This design fixes the main brittle points in commit-subject-based release-note
generation:

- squash commit messages can be overwritten safely
- backport sweep PRs are handled as containers
- helper and preparation commits do not become stale release-note bullets
- missing labels do not silently disappear
- security fixes are not double-listed
- RC2+ and GA-after-RC cuts have a deterministic base
- existing release notes are not regenerated from stale commit text
- maintainers can audit exactly why each bullet exists

## Required Changes To PR #54

The current PR should pivot to this model before it is merged.

Required changes:

- Add hidden release manifests to backport PRs and backport sweep PRs.
- Define manifest commit semantics clearly: `release_commits` are release-branch
  SHAs, while `source_commits` are optional metadata.
- Replace commit-subject identity with merged release-branch PR discovery plus
  manifest expansion.
- Keep the commit range as landed evidence and coverage checking only.
- Treat sweep PRs as containers, not release-note entries.
- Use source PR labels for expanded backport entries.
- Snapshot labels and resolved disposition in the generated release-cut manifest.
- Use previous generated `head_sha` for RC2+ and GA-after-RC base resolution.
- Block or loudly triage unresolved coverage and label ambiguity.
- Keep Projects as optional audit input, not the source of truth.

## Minimal Functional Scope

The smallest useful version does not need every audit feature on day one.

Required for correctness:

- release range resolution
- merged release-branch PR discovery
- hidden backport manifest parsing
- sweep PR expansion
- source PR metadata fetch
- label-based include, exclude, and triage
- release-branch commit coverage
- generated release-cut manifest with exact range and decisions

Optional follow-ups:

- Projects audit
- legacy Markdown fallback for old sweep PRs
- AI confidence scoring
- richer revert-pair detection
- advisory-specific security rendering
- historical migration overrides for old branches

Projects audit is useful, but it should not block the main pivot. The brittle
part that must be fixed first is identity: the generator must stop depending on
commit subjects and start using landed PRs plus explicit manifests.
