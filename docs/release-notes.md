# Release notes

Cuts a release in one shot for Valkey core or a module repository
(`valkey-search`, `valkey-json`, `valkey-bloom`): generates the notes, bumps
the version file, refreshes the contributor list, and opens one PR into the
release line for review. The release line changes only when a maintainer
merges that PR. A second, narrower flow applies maintainers' inline review
comments on that PR.

For the end-to-end release pipeline that calls this cutter, tracks the PR and
publishes the tag, see [release-automation.md](release-automation.md).

| Workflow | File | Runs | Purpose |
|---|---|---|---|
| Cut Release Notes | `release-notes-cut.yml` | by hand (defaults to dry run) | The normal cut: `repo`, `version`, `stage`, `urgency`, `dry_run` |
| Cut Release Notes (Advanced) | `release-notes-cut-advanced.yml` | by hand | Same job, plus explicit date or baseline, contributor override, security entries, `no_align_prior_wording`, `force_ready` |
| Release Notes Review Poll | `release-notes-review-poll.yml` | hourly (then every 5 minutes for 55) | Find unresolved inline comments on automated release PRs and dispatch the handler |
| Address Release Note Reviews | `release-notes-review.yml` | dispatched by the poll | One guarded AI edit of the current dated section |

## Cutting a release

### Branch model

One `M.m` branch per minor, tag-driven. Maintainers create the branch and push
release tags before dispatching. The agent never creates, deletes or
force-pushes a release-line branch; it writes only its
`agent/release-cut/<version>-<stage>` prep branch.

| Cutting | `version` | `stage` | Target branch | Baseline the notes span |
|---|---|---|---|---|
| First RC of a new minor | `9.2.0` | `rc1` | `9.2` | the previous release tag (e.g. `9.1.0`) |
| Next RC on the same line | `9.2.0` | `rc2` | `9.2` | the `9.2.0-rc1` tag on `9.2` |
| GA after the final RC | `9.2.0` | `ga` | `9.2` | the last rc tag (e.g. `9.2.0-rc2`) |
| Patch GA | `9.1.9` | *(empty -> `ga`)* | `9.1` | the previous patch tag (`9.1.8`) |
| First RC of a new major | `10.0.0` | `rc1` | `10.0` | the highest earlier release tag |

Two things that are easy to get wrong:

- **`version` is always `M.m.p`, the release you are cutting.** Use `9.2.0`
  for every RC of that release and its GA, not `9.2.0-rc2`; the `-rcN`
  suffix comes from `stage`. `stage` is required when the patch component is
  zero, because the same version can mean rc1, a later RC, or GA; a patch
  version omits it and infers `ga`. It is case-insensitive.
- **`base_ref` stays empty for normal cuts.** The baseline resolves from the
  tags and the PR body shows the exact `base..head` range. Use the advanced
  workflow's override only to correct a range the preview proves wrong.

### Recipes

Every normal dispatch defaults to `dry_run=true`: it logs the resolved plan
and the exact range (both refs and SHAs) without pushing or opening a PR.
After reviewing the preview, repeat the dispatch with `--field dry_run=false`.

```bash
# rc1 of a new minor line
gh workflow run release-notes-cut.yml --repo valkey-io/valkey-ci-agent \
  --field repo=valkey --field version=9.1.0 --field stage=rc1 --field urgency=LOW

# Next RC (the 9.1.0-rc1 tag must exist on the 9.1 branch)
gh workflow run release-notes-cut.yml --repo valkey-io/valkey-ci-agent \
  --field repo=valkey --field version=9.1.0 --field stage=rc2 --field urgency=LOW

# GA after the final RC (the last rc tag must exist on the 9.1 branch)
gh workflow run release-notes-cut.yml --repo valkey-io/valkey-ci-agent \
  --field repo=valkey --field version=9.1.0 --field stage=ga --field urgency=LOW

# Patch GA (the 9.1.0 tag must exist on the 9.1 branch)
gh workflow run release-notes-cut.yml --repo valkey-io/valkey-ci-agent \
  --field repo=valkey --field version=9.1.1 --field urgency=LOW

# Module repo patch GA (the 1.2.1 tag must exist on the 1.2 branch)
gh workflow run release-notes-cut.yml --repo valkey-io/valkey-ci-agent \
  --field repo=valkey-search --field version=1.2.2 --field urgency=LOW
```

`urgency` is one of `LOW`, `MODERATE`, `HIGH`, `CRITICAL`, `SECURITY`.

**Re-dispatching.** If more changes merge into `M.m` while the release PR is
open, dispatch the same `version` and `stage` again with `dry_run=false`. A
rerun regenerates the complete range through the latest `M.m` tip, rebuilds
the same prep branch from that tip with `--force-with-lease`, and updates the
open PR in place. It deliberately re-evaluates earlier entries too, so changed
labels or PR metadata are picked up. Manual edits made directly on the
generated branch are not retained.

**Several release lines.** When one change ships on several lines, the first
line to *merge* its release PR fixes the wording for the rest: a backported
change is worded once and reused verbatim by the other lines. The reuse reads
merged release-line branches only, so a line cut while a sibling's PR is still
open words the note itself. Cutting a line whose siblings have already merged
is the cheapest path to consistent notes. If you cut several lines in
parallel, re-dispatch the still-open ones after the first merges.

**Dates.** An omitted date is the current UTC date. Use the advanced
workflow's `date` input when the release date follows another timezone's
calendar day.

### How it works

The rendered commit lands on the prep branch and opens a PR into the release
line.

1. **Resolve the plan** (code). Normalize an explicit stage, or infer `ga`
   when `PATCH > 0`; an omitted stage for `M.m.0` is rejected. Canonicalize
   the version once (`M.m.p`, no leading zeros or stray whitespace) so the
   version file, dated heading, commit title and branch names agree. The
   requested state must be newer than both the version file and every
   existing tag on that line; an already-released stage or a downgrade is
   rejected before the AI runs.
2. **Discover the range** (code). Resolve `base..head` and walk it by graph
   reachability, deduplicating to one entry per originating PR. The `M.m` tip
   is fetched once and pinned to an immutable SHA used by discovery,
   contributors and the prep worktree. Backports are credited to their merged
   source PR after provenance validation: the structured backport summary,
   cherry-pick, subject or branch metadata, or a standalone
   `backport of <PR URL>` marker.
3. **Classify** (code). `release-notes` PRs are included directly;
   `no-release-notes` PRs are hard-excluded (dropped before triage, never
   noted, and listed in the PR body so a mislabel can be caught); everything
   else is a triage candidate. If a PR carries both labels,
   `no-release-notes` wins.
4. **Triage** (AI). Claude decides per candidate whether the change is
   user-facing enough to note, with a short reason. Patch-release triage
   defaults uncertain correctness and safety fixes to inclusion. Code
   independently scans PR titles and bodies for release-impact signals
   (crash, memory safety, corruption, access control, protocol,
   compatibility, and similar); if the AI excludes such a candidate or omits
   its verdict, the guardrail forces it into generation as uncertain. All
   decisions and overrides are listed in the PR body. A candidate with no
   verdict and no signal falls back to human triage.
5. **Generate** (AI). Claude writes one categorized, user-facing bullet per
   included PR. Code normalizes generic INFO, metrics, ACL LOG and logging
   classifications to `Observability and Logging` and flags that for review.
   The model never emits the `(#N)` reference or `by @handle`; code strips
   accidental duplicates and terminal punctuation and appends the canonical
   attribution (`render.py`).
6. **Align wording across release lines** (code). A backported change is
   discovered under its original source PR number on every line, so the
   trailing `(#N)` is a branch-invariant join key produced by code. Before
   rendering, the cut reads the sibling lines' merged `00-RELEASENOTES` from
   the existing clone (`refs/remotes/origin/<M.m>`; no network, no writes)
   and, where a note's PR already has a published bullet, replaces this
   cut's prose with that text verbatim. Only the prose is carried; category,
   credit and reference stay this cut's. Reuse is restricted to bullets under
   the canonical `### <category>` headings this tool writes and to text that
   round-trips through the renderer unchanged, so hand-authored legacy
   sections are never mined. Every carry and declined carry is listed in the
   PR body. Two disagreements refuse the carry and hold the PR: the lines
   crediting different authors, or naming different environment boundaries
   (one says 32-bit, the other does not), the signature of an adapted rather
   than cherry-picked backport. `no_align_prior_wording` opts a line out.
7. **Render and bump** (code). Prepend the new dated section to
   `00-RELEASENOTES` (`release_format.py`), append the cumulative contributor
   list (`contributors.py`, deduplicated by case-insensitive display name or
   login; PR-resolved logins give squash-merged authors proper handles), and
   bump the version file the repository profile names. These primitives live
   here because upstream ships no such tooling, so a cut runs against an
   unmodified upstream.
8. **Open the PR** (code). Commit on the prep branch, push with
   `--force-with-lease`, and open or update a PR into the line with a body
   that explains the cut and shows the resolved range (mode, both branches,
   both ends as `ref @ <sha>`) and any advisories. Immediately before
   changing the prep branch, the agent re-fetches `M.m` and aborts if its tip
   moved. When the cut flags anything a maintainer should address first, the
   PR opens as a draft.

### Per-repository conventions

Changelog heading, version file, note categories and prompt wording are
`ProjectProfile`s in `scripts/release_notes/projects.py`. Fork owners resolve
the upstream profile.

| Repo | Version file | Stage recorded | Unstable sentinel |
|---|---|---|---|
| valkey | `src/version.h` (`VALKEY_VERSION` macros) | yes | `255.255.255-dev` |
| valkey-search | `src/version.h` (`kModuleVersion` + `MODULE_RELEASE_STAGE`) | yes | - |
| valkey-json | `CMakeLists.txt` (`project(... VERSION M.m.p)`) | no | `99.99.99` |
| valkey-bloom | `Cargo.toml` (`[package] version`) | no | `99.99.99-dev` |

For repositories whose version file records no stage, tag validation remains
the check against re-cutting an already-tagged stage. The version bumper
recognizes only its repository's sentinel and replaces it when a new line is
cut. The valkey-search 1.0 line (version inline in `src/module_loader.cc`)
is not supported and fails with a clear error.

Before their first automated cut, the three module repositories need a
one-time `00-RELEASENOTES` normalization: the trailing contributor block must
become a `### Contributors` heading followed by one `* Display Name @handle`
per line. Legacy text inside dated sections may stay, since the renderer
carries dated history forward verbatim.

### Holds

Malformed inputs fail at argparse (exit 2) before the clone: a non-`M.m.p`
version, an omitted stage for `M.m.0`, a bad stage, an unknown urgency, a
non-ISO date. After the clone, an explicit `--base-ref` that resolves to
nothing, a missing `M.m` branch, or a target at or behind the version file or
an existing tag is refused before generation.

Anything a maintainer should look at before merging opens the PR as a draft
(GitHub refuses to merge a draft). The body leads with a banner naming the
held items, each with a section below. Resolve them and click **Ready for
review**, re-dispatch after fixing the cause, or use `force_ready` on the
advanced workflow to open ready anyway (the banner then records the
override). `--dry-run` prints the same decision. The signals:

- **RC out of sequence** - a re-cut rc or a skipped rc number.
- **Unanchored baseline** - rc1 of `M.0.0` with no `--base-ref` fell back to
  the nearest tag, which may over-broaden the range.
- **Empty release notes** - no PRs in range, or none labelled and none
  triaged in; the body says which.
- **Duplicate, declined or low-confidence** - a PR credited in more than one
  bullet, an included PR the model wrote no bullet for, or a note it flagged
  low-confidence.
- **AI triage** - the model decided inclusion for unlabelled PRs, so a
  maintainer confirms the include/exclude table; a low-confidence call also
  holds.
- **Release-impact review** - the guardrail overrode an AI exclusion or
  missing verdict, or impact signals were found while urgency is `LOW` or
  `MODERATE`. The body lists every signal; code never assigns severity.
- **Security** - `SECURITY` urgency with no security fixes, or advisories
  that could not be read.
- **Unresolved changes** - a range commit with no resolvable PR, a commit
  whose PR could not be fetched, or a note credited to a backport PR because
  the original could not be recovered.
- **Triage** - unlabelled PRs the AI returned no verdict for.
- **Release lines disagree** - a sibling line already published a bullet for
  the same PR but credits a different author or names a different environment
  boundary. This cut keeps its own wording and holds.

## Review comments on a release PR

Leave an inline comment on the current `00-RELEASENOTES` change in an
automated release PR; no command is needed. The hourly poll scans immediately
and then every five minutes for 55 minutes, so a comment is normally found by
the next scan plus any Actions queue delay. PR creation does not start a timer.

The poller batches every unresolved, current notes comment whose latest human
author is an active `valkey-io/contributors` member, posts
`Addressing N release-note review comments.` on the PR, and dispatches one
serialized handler run. A hidden head-SHA and batch marker prevents duplicate
work.

The handler re-fetches the PR and the complete batch, runs one edit-only
Claude pass (profile `release_notes_review_edit_only`), and requires the edit
to touch only the current dated section. It applies the patch in a clean
clone, rechecks the PR head and batch, and pushes with a normal fast-forward.
On success it updates the status comment, replies with the commit, and
resolves only unchanged threads. Failures stay visible on the PR and are
retried by a later poll; addressing markers expire after 90 minutes so a job
that never reaches its handler cannot block the PR. Editing a comment creates
a new batch. The handler records the proposed commit before pushing, so if
the push succeeds but the job dies during bookkeeping, the poller finishes the
replies and resolutions without another AI edit. Prompt and GraphQL bounds
fail explicitly rather than processing a partial batch. The publication token
is never in the AI checkout.

## Configuration

Reuses the shared secrets and OIDC role ([setup.md](setup.md)). Each cut
mints one App token scoped to the selected repository with `contents:write`,
`pull-requests:write` and `metadata:read`; an advisory-backed cut
(`security_from_advisories`) additionally requests
`repository-advisories:read`, so an ordinary cut is never blocked when the
installation lacks it. The App installation must include every repository
that can be selected (`valkey`, `valkey-search`, `valkey-json`,
`valkey-bloom`). Token creation fails when the installation lacks the
repository or a requested permission.

## Internals

```text
main.py (dispatch: repo, version, optional stage, urgency, dry_run)
  -> resolve the repo's ProjectProfile (unsupported repos fail fast)
  -> validate + canonicalize inputs; infer patch GA only when PATCH > 0
  -> clone the target repo (full depth + tags), validate --base-ref
  -> release_cut.cut()
       -> collect_advisory_fixes()   (if --security-from-advisories)
       -> resolve_branch_plan()      verify M.m exists, derive target
       -> pipeline.regenerate_unreleased()
            -> discover()   PRs over base..HEAD, deduped by PR number
            -> classify()   include, AI candidate, or hard exclusion
            -> PRDiffCollector  bounded, attributable commit diffs cached once
            -> triage()     AI decision + deterministic release-impact override
            -> generate()   AI: one categorized bullet per included PR
            -> normalize categories and bullet form; dedup by PR number
            -> prior_notes.align_wording()  reuse a sibling line's published text
            -> group_bullets()  {category: [bullet, ...]}
       -> _drop_already_credited()   dedup against PRs the line already ships
       -> promote_and_bump()         dated section + version bump + contributors
       -> _commit_push_release_pr()  prep branch (force-with-lease) + PR into the line

release-notes-review-poll.yml (hourly runner; immediate + five-minute ticks)
  -> review_poll.poll_once()
       -> validate automated release PR and authorized review threads
       -> batch every actionable thread; post the status comment
       -> dispatch release-notes-review.yml

release-notes-review.yml (serialized per repo/PR; trusted App sender only)
  -> re-fetch PR, head, status comment, threads, and author membership
  -> public clone at the exact head -> one edit-only AI pass
  -> require only the current dated section to change
  -> apply in a clean clone; re-fetch the batch; commit; fast-forward push
  -> update status, reply, resolve unchanged threads
```

The AI passes (`triage.py`, `generate.py`) run with no tools; PR data is
inlined in the prompt. `prior_notes` depends only on the shared grammar in
`release_format` (`trailing_pr_numbers`, the bullet form), which keeps it
importable from `pipeline` without a `release_cut -> pipeline` cycle.

Entry points:

- `scripts/release_notes/main.py` - CLI entry point, input validation, clone
- `scripts/release_notes/release_cut.py` - branch-plan resolution, rendering, PR body and `_hold_reasons`
- `scripts/release_notes/pipeline.py` - discover, classify, triage, generate, render
- `scripts/release_notes/discover.py` - range resolution and PR discovery by graph reachability
- `scripts/release_notes/backport_refs.py` - recover the original PR of a backported commit
- `scripts/release_notes/classify.py` - label split
- `scripts/release_notes/ai_inputs.py` - bounded PR prompt payloads and SHA-cached diffs
- `scripts/release_notes/triage.py` - AI include/exclude plus the release-impact guardrail
- `scripts/release_notes/generate.py` - AI bullet generation
- `scripts/release_notes/verdicts.py`, `code_age.py` - verdict parsing and code-age evidence
- `scripts/release_notes/prior_notes.py` - sibling-line wording reuse (local git reads only)
- `scripts/release_notes/projects.py` - `ProjectProfile`s and version bumpers
- `scripts/release_notes/security.py` - Security Fixes from published advisories (never AI-authored)
- `scripts/release_notes/render.py`, `release_format.py` - canonical `00-RELEASENOTES` rendering
- `scripts/release_notes/version_bump.py` - version file rewriting
- `scripts/release_notes/contributors.py` - contributor discovery and reconciliation
- `scripts/release_notes/publish.py` - find, open or update the release PR; `_reconcile_draft`
- `scripts/release_notes/review_poll.py`, `review.py`, `review_handler.py` - the review-comment flow
- `scripts/release_notes/models.py` - typed dataclasses
