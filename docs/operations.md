# Release Controller Operations Runbook

For the on-call maintainer staring at a stuck or confusing release. One
principle covers most of this page: **re-running Reconcile Releases and
Publish Release is ALWAYS safe.** Reconciliation recomputes truth from
live GitHub state and skips work that already happened (marker-gated
dispatches, no-op tracker edits), and publication resumes idempotently
around its two writes. When in doubt, reconcile and read the tracker.

All workflows live under this repository's Actions tab.

## Symptom to safe action

| Symptom | Safe action |
|---|---|
| The release branch moved and the tracker says the candidate is **Invalidated** | Dispatch **Adopt Release Candidate** with the branch and the FULL 40-character head SHA (or the pinned notes-merge SHA, to reconfirm shipping it despite the movement). The controller verifies your team membership live and that the SHA is genuinely adoptable; qualification then continues on the next reconcile pass. |
| The publish run died midway (tag exists but no release; release exists but the tracker shows an unreceipted-release alert) | Re-dispatch **Publish Release** for the branch with the same tag and candidate SHA. Resumption is idempotent by construction: a tag already at the approved SHA resumes STAGE 2 instead of refusing, and a release that exists without its receipt completes by posting the receipt. Readiness is re-proven live on resume, so a pre-created tag with no qualification evidence still refuses. |
| Qualification failed (the tracker names the failed run and jobs) | Fix the cause, then dispatch **Reconcile Releases**. The retry is bounded: the first failure retries once automatically; after that a human decides. A fresh qualification run for the same candidate SHA supersedes the failed one (the newest matching run wins), so getting a new green run recorded is the whole fix. When re-dispatching the qualification workflow by hand after the bounded retry, pass the dispatch nonce the tracker's receipt recorded (a mismatch refusal names both values), or the new run's manifest will not bind. |
| The tracker shows **needs-attention** or a team mention and you are not sure why | Dispatch **Reconcile Releases** and read the tracker's blocker list: every blocker line states what is holding the release and what resolves it. The notification comments repeat only when the failure state changes, so the newest one describes the current state. |
| A downstream output (tarballs, container images, docs, website, Bundle, Helm) sits FAILED or stalled | Fix the downstream cause (re-run the failed downstream job, merge the closed PR's replacement, etc.), then dispatch **Reconcile Releases**. Verification is re-observed from canonical public locations every pass; nothing is cached. Auto-remediation (bundle re-dispatch, build re-dispatch, helm PR) is marker-gated once per candidate, so reconcile never spams downstream. |
| The tracker was closed while the release was still moving | Reopen the tracker (or dispatch **Reconcile Releases** and follow the abandoned-tracker warning it posts). A closed tracker without the controller's completion marker also blocks the next start on that branch on purpose. |
| A start fails with "multiple open release trackers" | Close the duplicate tracker(s), keeping the real one, then re-dispatch **Start Release**. The refusal exists so two mid-flight releases never race one branch. |
| Anything else unclear | Dispatch **Reconcile Releases** and read the tracker's blocker list. Reconcile is a read-recompute-render pass with marker-gated side effects; running it can only bring the tracker closer to the truth. |

## Why the re-runs are safe

- **Reconcile Releases** recomputes the entire release state from live
  GitHub (notes PR, branch head, qualification runs, public endpoints) on
  every pass. Every side effect it takes is gated by a receipt on the
  tracker (a two-phase intent/done marker pair), so a re-run never
  repeats a dispatch that already happened, and an unchanged state edits
  nothing at all.
- **Publish Release** re-runs the full validation, requires the freshly
  computed plan to reproduce exactly what the approver saw (tag, SHA, and
  the plan digest), and treats both crash artifacts as resumable: a tag
  already at the approved SHA, and a release missing only its receipt.
  Anything else (a tag at a different SHA, missing qualification
  evidence) refuses loudly instead of guessing.

## What is deliberately NOT safe to do

- Do not hand-create the release or the tag on the valkey repository. The
  controller quarantines out-of-band releases (untrusted-tag alerts,
  unreceipted-release alerts) and downstream verification is withheld.
- Do not edit the tracker body or paste marker text into comments; the
  body is overwritten by the next reconcile pass and markers from
  untrusted authors are ignored.
- Do not close a mid-flight tracker to "silence" it; that blocks the next
  start on the branch until the abandonment is resolved.
