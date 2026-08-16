"""Idempotent progress actions taken during reconciliation.

Reconciliation recomputes state, then performs at most the small set of
side effects that state calls for, each guarded so a rerun never repeats
completed work:

- dispatch the qualification run (once the candidate SHA is bound and no
  qualification run exists for that exact SHA; required-check results are
  informational and never hold the dispatch);
- dispatch the Bundle update (only when the verifier says the ordering gate
  is satisfied and no update is in flight, signalled via ``action``);
- open the Helm chart bump PR (same gating, controller-authored since
  valkey-helm has no automation of its own);
- notify the authorized team exactly once per distinct failure state
  (keyed on a fingerprint marker in a bot comment; the same failure never
  notifies twice, a *new* failure does);
- nudge the authorized team exactly once per distinct awaiting-human state
  (an open notes PR, a moved branch) with the same fingerprint pattern;
- auto-dispatch build-release once per candidate when the valkey-side
  release trigger failed before dispatching the build (marker-gated, never
  loops; the failure notification still stands until the build verifies);
- retry a failed qualification run exactly once per candidate (same
  marker gate; a second failure waits for a human);
- signal completion of a release (return ``close_when_complete=True`` so
  the caller can render the final tracker body and then close the issue).

Every dispatch here goes through the two-phase receipt helper
:func:`_autofix_two_phase`: an ``autofix-intent:<key>:<fp>`` marker posts
BEFORE the dispatch API call and an ``autofix-done:<key>:<fp>`` marker is
stamped in place AFTER the dispatch succeeds. A pass that finds intent
without done means the previous pass crashed between the two writes; the
helper looks for a matching run and either backfills done (dispatch
succeeded, the follow-up write failed) or retries the dispatch ONCE and
stamps done regardless of the retry outcome (bounded so a chronic failure
never loops). This closes the "marker was posted, action never happened"
gap while keeping fail-closed dispatch semantics.

Publication is deliberately absent: it only happens through the protected
publish workflow.
"""

from __future__ import annotations

import hashlib
import logging
import re
import uuid
from collections.abc import Iterator
from functools import partial
from typing import Any

from github.GithubException import GithubException

from scripts.common.github_client import retry_github_call
from scripts.release import issue as issue_mod
from scripts.release import qualification as qual_mod
from scripts.release import verify as verify_mod
from scripts.release.models import (
    CandidateState,
    DailyCiState,
    OutputState,
    QualificationStatus,
    ReleasePhase,
    ReleaseStatus,
    release_tag,
)
from scripts.release.policy import RepoReleasePolicy
from scripts.release.release_refs import workflow_handle
from scripts.release_notes.release_format import parse_version

logger = logging.getLogger(__name__)

_CHART_PATH = "valkey/Chart.yaml"
_CHART_README_PATH = "valkey/README.md"


def _fp(source: str) -> str:
    """The 12-hex fingerprint of *source*, the width every marker uses."""
    return hashlib.sha256(source.encode("utf-8")).hexdigest()[:12]


# The marker envelope is owned by issue.py; alias it so every marker in
# the system is assembled in one place.
_marker = issue_mod.marker


def _edit_comment(comment: Any, body: str, description: str,
                  tracking_issue: Any) -> None:
    """Edit *comment* to *body* (retried), keeping the in-process comment
    view and the memo consistent so a same-pass re-read sees the change.
    Real PyGithub Comment objects also update on edit."""
    retry_github_call(
        lambda: comment.edit(body=body),
        retries=2, description=description,
    )
    try:
        comment.body = body
    except Exception:
        pass
    issue_mod.invalidate_comment_memo(tracking_issue)


def _post_marked_once(gh: Any, tracking_issue: Any, marker: str, body: str,
                      description: str) -> bool:
    """Post *body* under *marker* unless a trusted comment already carries
    the marker. True when this pass newly posted."""
    if issue_mod.has_marker(tracking_issue, marker, gh):
        return False
    issue_mod.post_comment(tracking_issue, f"{marker}\n{body}", description)
    return True


def _scan(listing: Any, limit: int) -> "Iterator[Any]":
    """At most *limit* runs from *listing* (newest first). The shared
    bound that keeps every run-correlation walk from paging a pathological
    listing on an old repo with many same-workflow runs."""
    for index, run in enumerate(listing):
        if index >= limit:
            return
        yield run


class AdvanceResult(list):
    """The list of actions performed, plus a ``close_when_complete`` flag.

    Extends ``list`` so existing ``for performed in advance(...)`` iteration
    stays valid. The flag tells the caller (reconcile) that the release
    reached COMPLETE with no alerts on this pass; reconcile is expected to
    render the final tracker body FIRST and then close the tracker as its
    last write, so a crash mid-close never leaves a stale-body closed
    tracker. advance() itself deliberately never closes.
    """

    __slots__ = ("close_when_complete",)

    def __init__(self, performed: "list[str] | None" = None, *,
                 close_when_complete: bool = False) -> None:
        super().__init__(performed or [])
        self.close_when_complete = close_when_complete


def advance(
    gh: Any, policy: RepoReleasePolicy, *,
    status: ReleaseStatus, tracking_issue: Any,
    gh_agent: Any = None, agent_repo: str = "",
    agent_head_sha: str = "",
) -> AdvanceResult:
    """Perform the actions the recomputed *status* calls for; returns an
    :class:`AdvanceResult` carrying the log of what was done and a
    ``close_when_complete`` flag telling the caller whether the tracker is
    ready to be closed (advance() itself never closes: the render/sync must
    land first so a crash mid-close cannot leave a permanently stale
    closed tracker).

    ``agent_head_sha`` is the agent repo's default-branch head (resolved
    once per pass by the caller): a publish run parked at the approval
    gate on any other commit is stale and gets cancelled rather than
    blocking a fresh dispatch. "" disables staleness detection.
    """
    performed: list[str] = []

    if status.phase is ReleasePhase.QUALIFICATION:
        if status.qualification == QualificationStatus():
            dispatched = _dispatch_qualification_once(gh, policy, status,
                                                     tracking_issue)
            if dispatched:
                performed.append(dispatched)
        elif (
            status.qualification.run_id
            and not status.qualification.pending
            and not status.qualification.passed
        ):
            retried = _retry_qualification_once(gh, policy, status, tracking_issue)
            if retried:
                performed.append(retried)

    if (
        status.phase is ReleasePhase.READY
        and gh_agent is not None and agent_repo
    ):
        # Minimum-clicks: READY auto-starts the publish pipeline. This is
        # safe to automate because the dispatch was never the gate: the
        # validate job posts the approval evidence and the publish job holds
        # at the protected environment until a human approves.
        publish_note = _advance_publish(gh, gh_agent, agent_repo, status,
                                        tracking_issue, agent_head_sha)
        if publish_note:
            performed.append(publish_note)

    for output in status.outputs:
        if output.action == "dispatch-bundle":
            dispatched = _dispatch_bundle_once(gh, policy, status, tracking_issue)
            if dispatched:
                performed.append(dispatched)
        elif output.action == "open-helm-pr":
            url = _open_helm_pr(gh, policy, status.version)
            performed.append(f"opened helm chart bump PR: {url}")
        elif output.action == "dispatch-build-release":
            dispatched = _dispatch_build_once(gh, policy, status, tracking_issue, output)
            if dispatched:
                performed.append(dispatched)

    # Recovery-aware notifications: the fingerprint of every notify and
    # wedge comment hashes (generation, sorted keys). A pass observing
    # ZERO failure and ZERO wedge items records recovery ONLY on the
    # transition from a dirty generation to a healthy one, so a steadily
    # healthy release does not churn the bookkeeping comment on every
    # reconcile pass.
    failures = _failure_items(status)
    wedges = _wedge_items(status)
    generation = _sync_generation_state(gh, tracking_issue,
                                        dirty=bool(failures or wedges))

    if failures:
        note = _notify_once(gh, policy, status, tracking_issue,
                            failures, generation)
        if note:
            performed.append(note)

    if wedges:
        wedge_note = _wedge_nudge_once(gh, policy, status, tracking_issue,
                                       wedges, generation)
        if wedge_note:
            performed.append(wedge_note)

    nudge = _nudge_once(gh, policy, status, tracking_issue)
    if nudge:
        performed.append(nudge)

    # Alerts block completion: a standing alert (an untrusted tag, broken
    # release metadata) must keep the tracker open for a human even if the
    # phase machine were ever to report COMPLETE alongside one.
    close_when_complete = False
    if status.phase is ReleasePhase.COMPLETE and not status.alerts:
        marked = _mark_complete_once(gh, status, tracking_issue)
        if marked:
            performed.append(f"marked release complete on tracker "
                             f"#{tracking_issue.number}")
        # Signal that reconcile should render one last time and close the
        # tracker as its final write. The signal is always True on a
        # COMPLETE+no-alerts pass, whether or not the completion comment
        # was newly posted this pass -- an earlier crash between comment
        # post and close leaves the tracker open, and the very next
        # advance() must still request the close so the caller can finish
        # the job.
        close_when_complete = tracking_issue.state != "closed"

    return AdvanceResult(performed, close_when_complete=close_when_complete)


def _dispatch_bundle(gh: Any, policy: RepoReleasePolicy, tag: str) -> None:
    """Fire the Bundle repo's own update hook (repository_dispatch).

    The payload carries the tag, mirroring the upstream trigger (which
    sends the release's tag_name as its version).

    ``retries=1``: repository_dispatch has no dispatch-echo id in its
    response and its firing is not idempotent, so a transient 5xx after
    the payload was accepted would fire the downstream twice. The
    two-phase caller (:func:`_dispatch_bundle_once`) provides idempotency
    at the outer layer instead -- an intent marker survives one crash and
    the next pass either backfills done or retries once.
    """
    repo = retry_github_call(
        lambda: gh.get_repo(policy.downstream.bundle_repo),
        retries=2, description=f"get repo {policy.downstream.bundle_repo}",
    )
    retry_github_call(
        lambda: repo.create_repository_dispatch(
            event_type="valkey-release",
            client_payload={"version": tag, "component": "valkey"},
        ),
        retries=1, description="dispatch bundle update",
    )
    logger.info("Dispatched bundle update for valkey %s", tag)


def _dispatch_bundle_once(gh: Any, policy: RepoReleasePolicy,
                          status: ReleaseStatus, tracking_issue: Any) -> str:
    """Bundle dispatch, marker-gated per (tag, candidate).

    Bundle updates rebuild an image; firing twice for the same candidate
    is a waste of the downstream's runners, and without a marker every
    reconcile pass would re-fire while versions.json remained stale (no
    Bundle-side PR yet visible to the verifier). Re-arm only when the
    candidate changes (new fingerprint) or the verifier stops asking
    (output action clears).

    Fires through the two-phase helper: intent marker before, done marker
    after. A crash between them leaves the intent standing so the next
    pass retries once. No live run-correlation lookup exists for
    repository_dispatch (the target repo picks it up asynchronously and
    no immediate handle is returned), so the retry always runs when
    intent-only is observed -- bounded to one extra attempt.
    """
    tag = release_tag(status.version, status.stage)
    performed = _autofix_two_phase(
        gh, tracking_issue, key="bundle-dispatch",
        fingerprint_source=f"{tag}:{status.candidate.sha}",
        intent_callout=(
            f"> [!NOTE]\n"
            f"> **Auto-remediation:** Dispatching the bundle update for "
            f"`{tag}` (versions.json is stale for this candidate)."
        ),
        dispatch_fn=lambda: _dispatch_bundle(gh, policy, tag),
        run_exists_fn=None,
        on_dispatch_failure_instruction=(
            f"Dispatch the bundle update for `{tag}` manually."
        ),
    )
    if performed:
        return f"dispatched bundle update for {tag}"
    return ""


def _autofix_two_phase(
    gh: Any, tracking_issue: Any, *, key: str,
    fingerprint_source: str, intent_callout: str,
    dispatch_fn: "Any",
    run_exists_fn: "Any" = None,
    on_dispatch_failure_instruction: str = "",
) -> bool:
    """Perform *dispatch_fn* at most once per (key, fingerprint) with a
    two-phase intent/done receipt so a crash between the receipt and the
    dispatch cannot permanently suppress the action.

    Semantics:

    - A ``autofix-done:<key>:<fp>`` marker on a trusted comment means the
      action already completed successfully; return False (suppress).
    - A ``autofix-intent:<key>:<fp>`` marker WITHOUT ``done`` means the
      previous pass posted intent and then either crashed or its dispatch
      failed. Check whether a matching run already exists (through
      ``run_exists_fn`` when the caller can supply one); if it does,
      stamp the done marker and return False (backfill: the dispatch
      succeeded, only the follow-up write failed). Otherwise retry the
      dispatch ONCE and stamp done regardless of retry outcome (bounded
      so a chronic failure never loops).
    - Neither marker: post an intent comment, run *dispatch_fn*, and on
      success stamp the done marker into the intent comment. On failure
      leave the intent standing so the next pass runs the retry-once path.

    Returns True when the dispatch (fresh or retry) actually ran and
    succeeded; False when suppressed, backfilled, or dispatch failed. The
    return value is exclusively for the caller's log line: the marker
    machinery is authoritative.

    Never dispatches without first observing (or posting) an intent
    marker: a raise during comment-post short-circuits the entire flow so
    the action cannot happen unrecorded.
    """
    fingerprint = _fp(fingerprint_source)
    intent_marker = _marker(f"autofix-intent:{key}:{fingerprint}")
    done_marker = _marker(f"autofix-done:{key}:{fingerprint}")

    if issue_mod.has_marker(tracking_issue, done_marker, gh):
        return False  # completed on an earlier pass
    intent_comment = issue_mod.find_marked_comment(tracking_issue,
                                                   intent_marker, gh)

    if intent_comment is not None:
        # Intent without done: possible crash between intent-post and
        # dispatch, OR dispatch succeeded but the done-stamp write failed.
        # A caller-supplied run-existence check distinguishes the two.
        if run_exists_fn is not None:
            try:
                exists = bool(run_exists_fn(intent_comment))
            except Exception:
                logger.exception("run-exists check for %s failed; "
                                 "assuming no run and retrying", key)
                exists = False
            if exists:
                _stamp_done_marker(intent_comment, done_marker, tracking_issue)
                logger.info("Backfilled autofix-done for %s (matching run "
                            "found; earlier stamp write must have failed)",
                            key)
                return False

        # No matching run (or no correlation available): retry once and
        # stamp done regardless of outcome so a chronic failure cannot
        # loop past two attempts total.
        dispatched = _run_dispatch_safely(
            dispatch_fn, tracking_issue=tracking_issue,
            key=key, instruction=on_dispatch_failure_instruction,
        )
        _stamp_done_marker(intent_comment, done_marker, tracking_issue)
        return dispatched

    # Fresh: post intent, dispatch, stamp done. Losing the intent write
    # would silently disable the retry-once recovery path, so if that
    # write raises, the dispatch never runs (fail closed).
    created = issue_mod.post_comment(tracking_issue,
                                     f"{intent_marker}\n{intent_callout}",
                                     f"post {key} intent marker")
    if created is None:
        # No comment handle returned: we cannot stamp done later.
        # Rather than dispatch and be unable to record it, refuse; the
        # next pass will retry the intent-post.
        logger.error("Intent comment for %s was not returned by GitHub; "
                     "refusing to dispatch without a recording handle", key)
        return False

    dispatched = _run_dispatch_safely(
        dispatch_fn, tracking_issue=tracking_issue,
        key=key, instruction=on_dispatch_failure_instruction,
    )
    if dispatched:
        _stamp_done_marker(created, done_marker, tracking_issue)
    return dispatched


def _stamp_done_marker(comment: Any, done_marker: str,
                       tracking_issue: Any) -> None:
    """Prepend *done_marker* to *comment*; a no-op when already present so
    a re-observation is idempotent."""
    body = comment.body if isinstance(comment.body, str) else ""
    if issue_mod.marker_present(body, done_marker):
        return
    _edit_comment(comment, f"{done_marker}\n{body}" if body else done_marker,
                  "stamp autofix-done marker", tracking_issue)


def _run_dispatch_safely(dispatch_fn: "Any", *, tracking_issue: Any,
                         key: str, instruction: str) -> bool:
    """Run *dispatch_fn* once; return True on success, False on Exception.

    On failure, post a plain follow-up comment (when an instruction is
    provided) so the human sees the tracker's 'Dispatching' callout did
    not land. No autofix marker on it: the intent marker already posted
    (fail closed), so the two-phase gate remains armed. The exception is
    swallowed: advance() must not skip its remaining notify/nudge/render
    steps just because one dispatch raised.
    """
    try:
        dispatch_fn()
    except Exception:
        logger.exception("Auto-remediation dispatch (%s) failed", key)
        if instruction:
            issue_mod.post_comment(
                tracking_issue,
                f"> [!WARNING]\n"
                f"> **Auto-remediation failed:** The dispatch itself failed. "
                f"{instruction}",
                f"post {key} dispatch-failure comment",
            )
        return False
    return True


def _autofix_marker_once(gh: Any, tracking_issue: Any, *, key: str,
                         fingerprint_source: str, callout: str) -> bool:
    """Post a marker-only autofix comment once per (key, fingerprint).

    For callouts that do NOT dispatch any action of their own (the publish
    halt warning, for example): a single marker gates repost. No two-phase
    receipt is needed because there is no work to correlate -- the marker
    IS the work. Callers that dispatch must use :func:`_autofix_two_phase`.
    """
    marker = _marker(f"autofix:{key}:{_fp(fingerprint_source)}")
    return _post_marked_once(gh, tracking_issue, marker, callout,
                             f"post {key} auto-remediation comment")


def _dispatch_build_once(
    gh: Any, policy: RepoReleasePolicy, status: ReleaseStatus,
    tracking_issue: Any, output: Any,
) -> str:
    """Dispatch build-release directly, once per candidate, when the
    valkey-side release trigger failed before reaching the automation repo.

    The fingerprint is the candidate SHA: strictly one auto-dispatch per
    candidate (with the two-phase safety net letting a crashed dispatch
    retry exactly once), even across distinct failed trigger runs. When
    the done marker already exists nothing happens (the normal failure
    notification covers the state).
    """
    tag = release_tag(status.version, status.stage)
    performed = _autofix_two_phase(
        gh, tracking_issue, key="build-dispatch",
        fingerprint_source=status.candidate.sha,
        intent_callout=(
            f"> [!NOTE]\n"
            f"> **Auto-remediation:** Dispatching the build pipeline for "
            f"`{tag}` directly (the [release trigger run]({output.url}) "
            f"did not succeed)."
        ),
        dispatch_fn=lambda: _dispatch_build_release(
            gh, policy, tag, status.candidate.sha,
        ),
        run_exists_fn=lambda _c: _build_run_exists_for_tag(
            gh, policy, tag,
        ),
        on_dispatch_failure_instruction=(
            f"Dispatch build-release for `{tag}` manually."
        ),
    )
    if not performed:
        return ""
    logger.info("Auto-dispatched build-release for %s", tag)
    return f"auto-dispatched build-release for {tag} (release trigger failed)"


def _build_run_exists_for_tag(gh: Any, policy: RepoReleasePolicy,
                              tag: str) -> bool:
    """True when a build-release run for *tag* exists on the automation
    repo. Used by the two-phase autofix to backfill the done marker when
    the dispatch succeeded on a previous pass but its follow-up write did
    not land. Any exception yields False so the retry path runs (the
    correlation is best-effort, not authoritative -- a bounded double
    dispatch is preferred to a permanent miss).
    """
    try:
        workflow = workflow_handle(
            gh, policy.downstream.automation_repo,
            policy.downstream.build_workflow,
        )
        if workflow is None:
            return False
        runs = retry_github_call(
            workflow.get_runs, retries=1,
            description=f"list {policy.downstream.build_workflow} runs "
                        f"for correlation",
        )
        marker = f"Build Release {tag} "
        for run in _scan(runs, _AUTOFIX_CORRELATION_SCAN_LIMIT):
            if marker in f"{run.display_title or ''} ":
                return True
    except Exception:
        logger.exception("Build-run correlation lookup for %s raised; "
                         "assuming no run", tag)
    return False


# Cap the correlation scan: a dispatched build-run is the newest of its
# workflow's runs, so a few dozen entries are more than enough. The bound
# only protects a pathological listing on an old repo with many
# same-workflow runs.
_AUTOFIX_CORRELATION_SCAN_LIMIT = 30


def _qual_dispatch_nonce(gh: Any, tracking_issue: Any, *, key: str,
                         sha: str) -> str:
    """The per-dispatch qualification nonce for (*key*, *sha*).

    A standing intent receipt for this exact (key, candidate) means a
    previous pass posted intent and crashed before (or during) dispatch;
    the retry must dispatch the SAME nonce that receipt already recorded,
    or the recorded nonce and the dispatched one would diverge and the
    evaluator would skip the run's manifest forever (a non-echoing run is
    invisible). With no standing intent, a fresh uuid4 hex is minted.
    """
    intent_marker = _marker(f"autofix-intent:{key}:{_fp(sha)}")
    comment = issue_mod.find_marked_comment(tracking_issue, intent_marker, gh)
    if comment is not None:
        recorded = issue_mod.qual_nonce_in_body(comment.body, sha)
        if recorded:
            return recorded
    return uuid.uuid4().hex


def _dispatch_qualification_once(
    gh: Any, policy: RepoReleasePolicy, status: ReleaseStatus,
    tracking_issue: Any,
) -> str:
    """First qualification dispatch for a candidate, marker-gated.

    Historically the first dispatch was ungated: reconcile would fire
    ``qual_mod.dispatch_qualification`` whenever ``status.qualification``
    was empty. A restart or a second controller invocation between
    dispatch and the run appearing in GitHub's UI would dispatch again,
    duplicating work. The two-phase gate stops that: the intent marker
    survives across passes even before the run is queryable, and the done
    marker suppresses further dispatch once the API call succeeds.

    The intent receipt also records the per-dispatch nonce the producer
    must echo into its manifest, so the evaluator can bind the run's
    evidence to this exact dispatch.
    """
    tag = release_tag(status.version, status.stage)
    sha = status.candidate.sha
    nonce = _qual_dispatch_nonce(gh, tracking_issue, key="qual-dispatch",
                                 sha=sha)
    performed = _autofix_two_phase(
        gh, tracking_issue, key="qual-dispatch",
        fingerprint_source=sha,
        intent_callout=(
            f"{issue_mod.qual_nonce_marker(sha, nonce)}\n"
            f"> [!NOTE]\n"
            f"> **Dispatching qualification** for `{tag}` "
            f"@ `{sha[:12]}`.\n"
            f"> Dispatch nonce: `{nonce}` (an integrity binding, not a "
            f"secret: the run's manifest must echo it, and a manual "
            f"re-dispatch must pass it as the `nonce` input or its run "
            f"is ignored)."
        ),
        dispatch_fn=lambda: qual_mod.dispatch_qualification(
            gh, policy, tag=tag, sha=sha, nonce=nonce,
        ),
        run_exists_fn=lambda _c: _qual_run_exists(
            gh, policy, tag=tag, sha=sha,
        ),
        on_dispatch_failure_instruction=(
            f"Dispatch the qualification workflow for `{tag}` manually "
            f"with `{nonce}` as its `nonce` input (the recorded dispatch "
            f"nonce; a run that does not echo it is ignored, never "
            f"counted as evidence)."
        ),
    )
    if not performed:
        return ""
    return (f"dispatched qualification of {tag} @ "
            f"{sha[:12]}")


def _qual_run_exists(gh: Any, policy: RepoReleasePolicy, *, tag: str,
                     sha: str, exclude_run_id: int = 0) -> bool:
    """True when a qualification run for exactly (*tag*, *sha*) is visible
    on the automation repo. Correlation for the two-phase autofix; any
    lookup failure yields False (retry path preferred to permanent miss).

    *exclude_run_id* names an already-known failed run to skip, so the
    retry path only counts a NEW run as evidence the retry dispatch
    landed on a prior pass with only the follow-up write lost.
    """
    try:
        found = qual_mod._find_run(gh, policy, tag, sha)
    except Exception:
        logger.exception("Qualification-run correlation lookup for %s @ %s "
                         "raised; assuming no run", tag, sha[:12])
        return False
    if found is None:
        return False
    return not exclude_run_id or getattr(found, "id", 0) != exclude_run_id


def _dispatch_build_release(gh: Any, policy: RepoReleasePolicy, tag: str,
                            source_sha: str) -> None:
    """Fire the automation repo's build workflow for *tag* in prod.

    The version/environment inputs mirror the upstream release trigger
    (it sends the release's tag_name as its version), so the resulting run
    carries the ``Build Release <tag> (prod)`` run-name the build-run
    verifier matches. ``source_sha`` additionally names the exact
    candidate commit this build must represent, so the automation repo can
    verify its checkout against the commit the controller vetted instead
    of trusting the tag ref alone; the automation side treats the field as
    optional for backward compatibility, so payloads without it (the
    upstream trigger's) keep working.

    ``retries=1`` on the dispatch call: workflow_dispatch has no
    dispatch-echo id in its response, so a transient 5xx after the run
    was accepted would double-dispatch. The two-phase caller
    (:func:`_dispatch_build_once`) provides idempotency at the outer
    layer -- an intent marker survives one crash and the next pass either
    backfills done via :func:`_build_run_exists_for_tag` or retries once.
    """
    down = policy.downstream
    workflow = workflow_handle(gh, down.automation_repo, down.build_workflow)
    if workflow is None:
        raise RuntimeError(
            f"{down.build_workflow} does not exist on {down.automation_repo}"
        )
    repo = retry_github_call(
        lambda: gh.get_repo(down.automation_repo),
        retries=2, description=f"get repo {down.automation_repo}",
    )
    dispatched = retry_github_call(
        lambda: workflow.create_dispatch(
            repo.default_branch,
            inputs={"version": tag, "environment": "prod",
                    "source_sha": source_sha},
        ),
        retries=1, description="dispatch build-release run",
    )
    if not dispatched:
        raise RuntimeError(
            f"build-release dispatch was rejected by "
            f"{down.automation_repo}/{down.build_workflow}"
        )


def _retry_qualification_once(
    gh: Any, policy: RepoReleasePolicy, status: ReleaseStatus, tracking_issue: Any,
) -> str:
    """Re-dispatch qualification exactly once per candidate after a failure.

    The deliberate never-redispatch-over-a-failed-run stance is softened by
    exactly one step: one automatic retry per candidate SHA. A second
    failure changes nothing further; the normal failure notification
    stands and a human decides. The retry's intent receipt records a fresh
    nonce (the retry is a new dispatch), superseding the initial
    dispatch's recorded nonce for evaluation.
    """
    tag = release_tag(status.version, status.stage)
    sha = status.candidate.sha
    run_link = f"[run {status.qualification.run_id}]({status.qualification.url})"
    failed_run_id = status.qualification.run_id
    nonce = _qual_dispatch_nonce(gh, tracking_issue, key="qual-retry",
                                 sha=sha)
    performed = _autofix_two_phase(
        gh, tracking_issue, key="qual-retry",
        fingerprint_source=sha,
        intent_callout=(
            f"{issue_mod.qual_nonce_marker(sha, nonce)}\n"
            f"> [!NOTE]\n"
            f"> **Auto-remediation:** Retrying qualification for `{tag}` "
            f"once (the previous run failed: {run_link}).\n"
            f"> Dispatch nonce: `{nonce}` (an integrity binding, not a "
            f"secret: the run's manifest must echo it, and a manual "
            f"re-dispatch must pass it as the `nonce` input or its run "
            f"is ignored)."
        ),
        dispatch_fn=lambda: qual_mod.dispatch_qualification(
            gh, policy, tag=tag, sha=sha, nonce=nonce,
        ),
        run_exists_fn=lambda _c: _qual_run_exists(
            gh, policy, tag=tag, sha=sha,
            exclude_run_id=failed_run_id,
        ),
        on_dispatch_failure_instruction=(
            f"Dispatch the qualification workflow for `{tag}` manually "
            f"with `{nonce}` as its `nonce` input (the recorded dispatch "
            f"nonce; a run that does not echo it is ignored, never "
            f"counted as evidence)."
        ),
    )
    if not performed:
        return ""
    logger.info("Auto-retried qualification of %s @ %s", tag, sha[:12])
    return f"auto-retried qualification of {tag} @ {sha[:12]}"


def _open_helm_pr(gh: Any, policy: RepoReleasePolicy, version: str) -> str:
    """Open the chart bump PR: appVersion -> version, chart patch +1,
    README badge line updated. Idempotent via the fixed head branch (the
    verifier only requests this action when no such PR is open)."""
    down = policy.downstream
    repo = retry_github_call(
        lambda: gh.get_repo(down.helm_repo),
        retries=2, description=f"get repo {down.helm_repo}",
    )
    default_branch = repo.default_branch
    head_sha = retry_github_call(
        lambda: repo.get_branch(default_branch).commit.sha,
        retries=2, description=f"resolve {down.helm_repo} {default_branch} head",
    )

    chart_file = retry_github_call(
        lambda: repo.get_contents(_CHART_PATH, ref=head_sha),
        retries=2, description=f"read {_CHART_PATH}",
    )
    chart_text = chart_file.decoded_content.decode("utf-8")
    new_chart, old_chart_version, new_chart_version = _bump_chart(chart_text, version)

    readme_file = retry_github_call(
        lambda: repo.get_contents(_CHART_README_PATH, ref=head_sha),
        retries=2, description=f"read {_CHART_README_PATH}",
    )
    readme_text = readme_file.decoded_content.decode("utf-8")
    new_readme = _bump_readme_badges(readme_text, new_chart_version, version)

    branch = verify_mod.helm_update_branch(version)
    ref = f"refs/heads/{branch}"
    try:
        retry_github_call(
            lambda: repo.create_git_ref(ref=ref, sha=head_sha),
            retries=2, description=f"create branch {branch}",
        )
    except GithubException as exc:
        if exc.status != 422:  # already exists (from an interrupted earlier run)
            raise
        retry_github_call(
            lambda: repo.get_git_ref(f"heads/{branch}").edit(sha=head_sha, force=True),
            retries=2, description=f"reset branch {branch}",
        )

    retry_github_call(
        lambda: repo.update_file(
            _CHART_PATH,
            f"Update valkey chart to {new_chart_version} (appVersion {version})",
            new_chart, chart_file.sha, branch=branch,
        ),
        retries=2, description=f"update {_CHART_PATH}",
    )
    readme_on_branch = retry_github_call(
        lambda: repo.get_contents(_CHART_README_PATH, ref=branch),
        retries=2, description=f"re-read {_CHART_README_PATH} on {branch}",
    )
    retry_github_call(
        lambda: repo.update_file(
            _CHART_README_PATH,
            f"Update chart version badges for {new_chart_version}",
            new_readme, readme_on_branch.sha, branch=branch,
        ),
        retries=2, description=f"update {_CHART_README_PATH}",
    )

    pr = retry_github_call(
        lambda: repo.create_pull(
            title=f"Update valkey chart to appVersion {version}",
            body=(
                f"Automated chart bump for the Valkey {version} release: "
                f"`appVersion` {version}, chart `version` "
                f"{old_chart_version} -> {new_chart_version}.\n\n"
                f"Opened by the release controller after verifying "
                f"`docker.io/{down.dockerhub_repo}:{version}` is public."
            ),
            head=branch,
            base=default_branch,
        ),
        retries=2, description="open helm bump PR",
    )
    logger.info("Opened helm chart bump PR #%s", pr.number)
    return pr.html_url


def _bump_chart(chart_text: str, version: str) -> tuple[str, str, str]:
    """Return (new Chart.yaml, old chart version, new chart version)."""
    chart_match = verify_mod.CHART_VERSION_RE.search(chart_text)
    app_match = verify_mod.CHART_APP_VERSION_RE.search(chart_text)
    if chart_match is None or app_match is None:
        raise RuntimeError("cannot parse valkey/Chart.yaml version fields")
    old_chart_version = chart_match.group(1)
    major, minor, patch = parse_version(old_chart_version)
    new_chart_version = f"{major}.{minor}.{patch + 1}"
    text = verify_mod.CHART_VERSION_RE.sub(f"version: {new_chart_version}", chart_text, count=1)
    text = verify_mod.CHART_APP_VERSION_RE.sub(f'appVersion: "{version}"', text, count=1)
    return text, old_chart_version, new_chart_version


def _bump_readme_badges(readme: str, chart_version: str, app_version: str) -> str:
    """Update the shields badge line (``![Version: X]`` / ``![AppVersion: Y]``)."""
    readme = re.sub(r"!\[Version: [^\]]*\]", f"![Version: {chart_version}]", readme, count=1)
    # (?<!App): the plain Version pattern must never rewrite the AppVersion
    # badge, regardless of badge order in the README.
    readme = re.sub(r"(?<!App)Version-[0-9.]+-informational", f"Version-{chart_version}-informational", readme, count=1)
    readme = re.sub(r"!\[AppVersion: [^\]]*\]", f"![AppVersion: {app_version}]", readme, count=1)
    return re.sub(r"AppVersion-[0-9.]+-informational", f"AppVersion-{app_version}-informational", readme, count=1)


def _notify_once(
    gh: Any, policy: RepoReleasePolicy, status: ReleaseStatus, tracking_issue: Any,
    failures: "list[tuple[str, str]]", generation: int,
) -> str:
    """Mention the authorized team once per distinct failure state.

    The failure fingerprint is stamped into the notification comment; while
    the observed failure set is unchanged no further comment is posted, and
    a different failure set notifies again, exactly once. *generation* (the
    recovery generation, see :func:`_sync_generation_state`) is hashed into the
    fingerprint so the SAME failure set recurring after a clean pass
    notifies again.
    """
    # Fingerprint over (generation, stable keys), not the rendered prose: a
    # wording tweak in a detail string must never re-ping the team, while a
    # NEW failure (a new failed run id, a new failing check) or a recurrence
    # after recovery must.
    marker = _marker(f"notify:{_notification_fingerprint(generation, [key for key, _ in failures])}")
    tag = release_tag(status.version, status.stage)
    rows = "\n".join(
        f"| {index} | {_problem_cell(text)} |"
        for index, (_, text) in enumerate(failures, start=1)
    )
    body = (
        f"> [!WARNING]\n"
        f"> **{policy.mention}: Release `{tag}` Needs Attention.**\n"
        f"\n"
        f"| # | Problem |\n"
        f"|---|---|\n"
        f"{rows}\n"
        f"\n"
        f"<sub>This notification repeats only if the failure state "
        f"changes.</sub>"
    )
    if not _post_marked_once(gh, tracking_issue, marker, body,
                             "post failure notification"):
        return ""
    logger.info("Notified %s of %d failure(s)", policy.authorized_team, len(failures))
    return f"notified {policy.authorized_team} ({len(failures)} failure(s))"


def _problem_cell(item: str) -> str:
    """One failure item as a table cell: 'name: detail' items get the name
    bolded; anything else renders verbatim."""
    name, sep, detail = item.partition(": ")
    if sep and detail:
        return f"**{name}:** {detail}"
    return item


def _notification_fingerprint(generation: int, keys: "list[str]") -> str:
    """The 12-hex fingerprint of (recovery generation, sorted stable keys).

    Hashing the generation in means an identical key set recurring AFTER a
    recovery (see :func:`_sync_generation_state`) produces a new
    fingerprint and so re-notifies exactly once, while an unchanged state
    within one generation stays suppressed.
    """
    return _fp("\n".join([str(generation), *sorted(keys)]))


_NOTIFY_GEN_MARKER_RE = re.compile(
    rf"<!-- {re.escape(issue_mod.MARKER_NAMESPACE)}:notify-gen:(\d+) -->"
)


# The dirty flag encoded alongside the generation marker: 'dirty' means
# the current generation has observed failure or wedge items since it
# started, 'healthy' means we have recovered to this generation. The
# recovery bump only fires on the transition dirty -> healthy, so a
# steadily healthy release never re-edits the bookkeeping comment.
_NOTIFY_STATE_DIRTY = "dirty"
_NOTIFY_STATE_HEALTHY = "healthy"
_NOTIFY_STATE_MARKER_RE = re.compile(
    rf"<!-- {re.escape(issue_mod.MARKER_NAMESPACE)}:notify-state:(\w+) -->"
)


def _notify_generation(gh: Any, tracking_issue: Any) -> "tuple[int, Any, str]":
    """(current recovery generation, the trusted comment recording it,
    the current dirty/healthy state).

    (0, None, "") before any recovery was ever recorded. The newest
    (highest) generation on a trusted comment wins; markers on untrusted
    comments are ignored exactly like every other marker read-back. The
    state is read from the notify-state sub-marker in the same comment
    when present, "" otherwise.
    """
    best_generation, best_comment, best_state = 0, None, ""
    for comment, match in issue_mod.marker_matches(
            tracking_issue, _NOTIFY_GEN_MARKER_RE, gh):
        if int(match.group(1)) < best_generation:
            continue
        best_generation, best_comment = int(match.group(1)), comment
        state_match = _NOTIFY_STATE_MARKER_RE.search(comment.body or "")
        best_state = (
            state_match.group(1)
            if state_match and issue_mod.marker_present(comment.body,
                                                        state_match.group(0))
            else ""
        )
    return best_generation, best_comment, best_state


def _sync_generation_state(gh: Any, tracking_issue: Any, *,
                           dirty: bool) -> int:
    """Return the current fingerprint generation, updating the marker
    only on a state TRANSITION, so steady states never churn the
    bookkeeping comment.

    Semantics:

    - dirty=True observed while the recorded state is healthy: edit the
      marker in place to state=dirty (fingerprints from now on include
      failure/wedge items so they cannot re-ping across a
      resolved-then-recurring cycle without a fresh transition).
    - dirty=False observed while the recorded state is dirty (or unknown,
      pre-schema comments): failure->healthy TRANSITION. Bump the
      generation and record state=healthy. A later recurrence hashes to
      the new generation and re-notifies exactly once.
    - Any observation matching the recorded state is a no-op: a steadily
      healthy release does not churn the bookkeeping comment on every
      pass, and a steady failure state does not either.
    - No prior generation marker AND dirty=True: no write. The
      notify/wedge comments posted this pass ARE the "was dirty"
      evidence a future healthy pass will read via
      :func:`_has_notification_history`; there is nothing to record
      until the transition happens.
    - No prior generation marker AND dirty=False AND notification
      history exists: this is the very first healthy pass after an
      unrecorded failure run. Initialize the marker at generation 1,
      state=healthy (skipping over the implicit generation 0 seed the
      failure ran under). Without history the pass is a no-op: a happy
      release gets no bookkeeping comment.
    """
    generation, comment, state = _notify_generation(gh, tracking_issue)

    if dirty:
        if comment is None or state == _NOTIFY_STATE_DIRTY:
            return generation
        # state is healthy: transition healthy -> dirty (edit in place).
        _write_generation_state(tracking_issue,
                                generation=generation,
                                state=_NOTIFY_STATE_DIRTY,
                                existing=comment)
        return generation

    # dirty=False (healthy pass)
    if comment is None:
        if not _has_notification_history(gh, tracking_issue):
            return 0  # happy path: no bookkeeping needed
        # First healthy pass after an unrecorded dirty run.
        _write_generation_state(tracking_issue,
                                generation=1,
                                state=_NOTIFY_STATE_HEALTHY,
                                existing=None)
        return 1
    if state == _NOTIFY_STATE_HEALTHY:
        return generation
    # state is dirty (or unknown from a pre-state-schema marker):
    # transition dirty -> healthy. Bump generation.
    new_generation = generation + 1
    _write_generation_state(tracking_issue,
                            generation=new_generation,
                            state=_NOTIFY_STATE_HEALTHY,
                            existing=comment)
    return new_generation


def _write_generation_state(tracking_issue: Any, *, generation: int,
                            state: str, existing: Any) -> None:
    """Create or edit-in-place the generation-state bookkeeping comment."""
    body = (
        f"{_marker(f'notify-gen:{generation}')}\n"
        f"{_marker(f'notify-state:{state}')}\n"
        f"<sub>Notification bookkeeping: generation {generation} "
        f"({state}, edited in place).</sub>"
    )
    if existing is None:
        issue_mod.post_comment(tracking_issue, body,
                               "post recovery-generation comment")
    else:
        _edit_comment(existing, body, "advance recovery-generation comment",
                      tracking_issue)


def _has_notification_history(gh: Any, tracking_issue: Any) -> bool:
    """True when any trusted comment carries a notify or wedge marker."""
    return any(
        issue_mod.has_marker(tracking_issue, prefix, gh)
        for prefix in (f"<!-- {issue_mod.MARKER_NAMESPACE}:notify:",
                       f"<!-- {issue_mod.MARKER_NAMESPACE}:wedge:")
    )


def _wedge_items(status: ReleaseStatus) -> "list[tuple[str, str]]":
    """(stable key, rendered text) per silently wedged gate.

    A MISSING/STALE daily gate is not a failure, so :func:`_failure_items`
    never escalates it, yet nothing is running that could change it: the
    release waits silently forever. No time-based grace this round (status
    carries no tracker age): observing the state at reconcile time is the
    whole trigger, keyed on the evidence identity so an unchanged wedge
    nudges once per generation.

    Required-check states never wedge: nothing gates on them, so a MISSING
    check does not block progress and must not page anyone.
    """
    items: "list[tuple[str, str]]" = []
    if status.daily.state in (DailyCiState.MISSING, DailyCiState.STALE):
        detail = status.daily.detail or (
            f"Daily CI is {status.daily.state.value} on `{status.branch}`."
        )
        if not detail.endswith("."):
            detail += "."
        items.append((
            f"wedge:daily:{status.branch}:{status.daily.state.value}:"
            f"{status.daily.run_id}",
            f"Blocked without progress: {detail} "
            f"This does not resolve on its own.",
        ))
    return items


def _wedge_nudge_once(
    gh: Any, policy: RepoReleasePolicy, status: ReleaseStatus, tracking_issue: Any,
    wedges: "list[tuple[str, str]]", generation: int,
) -> str:
    """Mention the authorized team once per distinct wedged state: a
    silently blocked gate must page a human exactly once, since nothing
    running will ever change it.

    Same fingerprint-marker pattern as :func:`_notify_once`, in its own
    ``wedge:`` marker family: an unchanged wedge never re-pings within a
    generation; a resolved-then-recurring one re-pings once through the
    generation bump.
    """
    marker = _marker(f"wedge:{_notification_fingerprint(generation, [key for key, _ in wedges])}")
    tag = release_tag(status.version, status.stage)
    lines = "\n".join(f"> {text}" for _, text in wedges)
    body = (
        f"> [!IMPORTANT]\n"
        f"> **{policy.mention}: Release `{tag}` Is Blocked Without "
        f"Progress.**\n"
        f">\n"
        f"{lines}\n"
        f"\n"
        f"<sub>One-time nudge: posts again only if the blocked state "
        f"changes.</sub>"
    )
    if not _post_marked_once(gh, tracking_issue, marker, body,
                             "post blocked-without-progress nudge"):
        return ""
    logger.info("Nudged %s about %d wedged gate(s)", policy.authorized_team,
                len(wedges))
    return f"nudged {policy.authorized_team} ({len(wedges)} wedged gate(s))"


def _nudge_once(
    gh: Any, policy: RepoReleasePolicy, status: ReleaseStatus, tracking_issue: Any,
) -> str:
    """Mention the authorized team once per distinct awaiting-human state.

    Failures already escalate through :func:`_notify_once`; this covers the
    two states that otherwise wait silently on a human: an open notes PR
    and a moved branch. Same fingerprint-marker pattern: an unchanged state
    never re-pings, a changed one (new notes PR, new branch head) does.
    """
    nudge = _nudge_item(status)
    if nudge is None:
        return ""
    key, message = nudge
    marker = _marker(f"nudge:{_fp(key)}")
    tag = release_tag(status.version, status.stage)
    body = (
        f"> [!IMPORTANT]\n"
        f"> **{policy.mention}: Action Needed for `{tag}`.**\n"
        f">\n"
        f"> {message}\n"
        f"\n"
        f"<sub>One-time nudge: posts again only if the state changes.</sub>"
    )
    if not _post_marked_once(gh, tracking_issue, marker, body,
                             "post action-needed nudge"):
        return ""
    logger.info("Nudged %s (%s)", policy.authorized_team, key)
    return f"nudged {policy.authorized_team} ({key})"


def _nudge_item(status: ReleaseStatus) -> "tuple[str, str] | None":
    """(fingerprint key, message) for the awaiting-human state, or None.

    The awaiting-publish-approval state is deliberately absent: the approval
    evidence comment posted by the publish pipeline carries the mention.
    """
    if status.published:
        return None
    if status.notes_pr_number and not status.notes_pr_merged:
        tag = release_tag(status.version, status.stage)
        return (
            f"notes-pr:{status.notes_pr_number}",
            f"Review and merge the release-notes PR {status.notes_pr_url} "
            f"to proceed with `{tag}`.",
        )
    if status.candidate.state is CandidateState.INVALIDATED:
        head = status.candidate.branch_head
        return (
            f"branch-moved:{head}",
            f"Branch `{status.branch}` moved to `{head[:12]}` after the "
            f"candidate was established. Adopt the new head "
            f"(Actions → Adopt Release Candidate) or ship the pinned "
            f"candidate.",
        )
    return None


def _alert_key(text: str) -> str:
    """Stable notification key for an alert string.

    Alerts are produced elsewhere (reconcile, verify) as full prose, out of
    this module's reach, so the key is derived here: hex ids (7 to 40
    chars) and then all digit runs are stripped before hashing, so a
    rewording that only swaps a run id or SHA keeps the alert's identity
    and never re-pings. Known limitation, accepted: two DIFFERENT alerts
    whose prose differs only in ids or numbers collapse to one key, so the
    second one does not re-notify on its own (a recovery generation bump
    still re-arms it).
    """
    normalized = re.sub(r"[0-9a-fA-F]{7,40}", "", text)
    normalized = re.sub(r"[0-9]+", "", normalized)
    return f"alert:{_fp(normalized)}"


def _failure_items(status: ReleaseStatus) -> "list[tuple[str, str]]":
    """(stable key, rendered text) per failure.

    Keys are identifiers, not prose: they feed the notification fingerprint,
    so rewording a detail never re-pings, while a new failed run id does.
    Texts render into the comment body.

    Required-check results are deliberately absent: they are informational
    display, not a gate, so a red check on the candidate never pages anyone
    and never contributes to needs-attention.
    """
    items: "list[tuple[str, str]]" = [(_alert_key(alert), alert)
                                      for alert in status.alerts]
    if status.qualification.failed_jobs:
        run_id = status.qualification.run_id
        items.append((
            f"qual:{run_id}",
            f"Qualification run {run_id} failed: "
            + ", ".join(status.qualification.failed_jobs[:5]),
        ))
    # Daily FAILED only: STALE and MISSING are blockers (waiting states),
    # not failures. The key carries the run id, so the same failing run
    # never re-pings while a NEW failing run does.
    if status.daily.state is DailyCiState.FAILED:
        items.append((
            f"daily:{status.daily.run_id}",
            f"Daily CI run {status.daily.run_id} failed: {status.daily.url}",
        ))
    # run_id may be 0/empty; included anyway so a NEW failed run (with a
    # real id) re-pings exactly once.
    items.extend(
        (f"output:{output.name}:{output.run_id}",
         f"{output.name}: {output.detail}")
        for output in status.outputs
        if output.state is OutputState.FAILED
    )
    return items


def _mark_complete_once(gh: Any, status: ReleaseStatus,
                        tracking_issue: Any) -> bool:
    """Post the completion marker comment once; NEVER close the issue.

    Closing the tracker is the caller's job (reconcile), performed
    AFTER the final render/sync so a crash mid-close cannot leave a stale
    body on a closed tracker. advance() only stamps the completion marker
    so the caller can see this release was signed off, and so a rerun does
    not duplicate the comment.

    Returns True when this pass newly posted the completion comment,
    False when the marker was already present (or the tracker is already
    closed -- nothing more to stamp).
    """
    if tracking_issue.state == "closed":
        return False
    if issue_mod.has_completion_marker(tracking_issue, gh):
        return False
    issue_mod.post_comment(
        tracking_issue,
        issue_mod.completion_comment(status.version, status.stage),
        "post completion comment",
    )
    return True


_PUBLISH_WORKFLOW = "release-publish.yml"
_START_WORKFLOW = "release-start.yml"
_CUT_WORKFLOW = "release-notes-cut.yml"
_CUT_RUN_SCAN_LIMIT = 15


def notes_cut_run_url(gh_agent: Any, agent_repo: str, branch: str,
                      version: str = "") -> str:
    """The in-flight (or newest failed) notes-cut run for *branch*, or "".

    During the window between start and the PR appearing, the tracker
    would otherwise show nothing an operator can watch. The chained cut
    runs inside "Start Release on {branch}"; a standalone cut runs as
    "Cut Release Notes {version}" (version known only once bound). An
    active run of either is returned first; failing that, the newest
    FAILED run (a failed cut leaves no PR, and the run link is the only
    evidence of why). Display only: any lookup failure returns "".
    """
    markers = [(_START_WORKFLOW, f"Start Release on {branch} ")]
    if version:
        markers.append((_CUT_WORKFLOW, f"Cut Release Notes {version} "))
    failed_url = ""
    for workflow_file, marker in markers:
        try:
            workflow = workflow_handle(gh_agent, agent_repo, workflow_file)
            if workflow is None:
                continue
            runs = retry_github_call(
                workflow.get_runs,
                retries=2, description=f"list {workflow_file} runs",
            )
            for run in _scan(runs, _CUT_RUN_SCAN_LIMIT):
                if marker not in f"{run.display_title or ''} ":
                    continue
                if run.status in ("queued", "in_progress", "waiting", "pending"):
                    return run.html_url
                if not failed_url and run.conclusion not in ("success", None):
                    failed_url = run.html_url
        except GithubException:
            continue
    return failed_url

# Per-listing cap on each server-side filtered runs query. The status
# filter (not this cap) is what keeps a long-waiting run visible: it can
# never fall out of a newest-N window regardless of how many completed
# runs pile up above it. The cap only bounds pathological volume within
# one filtered listing.
_PUBLISH_RUN_SCAN_LIMIT = 50

# Statuses that can hold the slot BEFORE the approval gate; these are the
# only statuses stale-run cleanup may ever cancel. in_progress is
# deliberately not here: it is past the gate (a human approved), so it is
# always active and never cancelled.
_PUBLISH_RUN_GATED_STATUSES = ("queued", "waiting", "pending")

# in_progress first: when both an in_progress and a gate-parked run match,
# the in_progress one (past the gate, the publication itself) is the one
# reported as active.
_PUBLISH_RUN_ACTIVE_STATUSES = ("in_progress",) + _PUBLISH_RUN_GATED_STATUSES

# The candidate binding release-publish.yml stamps into its run-name:
# "Publish Release on <branch> · <tag> @ <sha> (requested by <actor>)".
# Correlation is deliberately prefix-agnostic: runs are located by workflow
# file, then matched on the " on <branch> " marker and this binding regex,
# never on the leading words. Runs titled with the legacy lowercase prefix
# ("Publish release on ...", dispatched before the workflow rename) are
# therefore still recognized, held, and cancellable exactly like new runs;
# this note can be dropped after the next successful publish.
_PUBLISH_TITLE_BINDING_RE = re.compile(
    r" · (?P<tag>\S+) @ (?P<sha>[0-9a-fA-F]{7,40})(?![0-9a-zA-Z])"
)


def agent_head_sha(gh_agent: Any, agent_repo: str) -> str:
    """The agent repo's default-branch head SHA, "" when unresolvable.

    The trusted controller version for publish runs: a run parked at the
    approval gate on any other commit would publish stale logic. "" means
    staleness cannot be judged, so no run is cancelled and any waiting run
    still counts as active (fail-safe: today's behavior).
    """
    try:
        repo = retry_github_call(
            lambda: gh_agent.get_repo(agent_repo),
            retries=2, description=f"get repo {agent_repo}",
        )
        return retry_github_call(
            lambda: repo.get_branch(repo.default_branch).commit.sha,
            retries=2, description=f"resolve {agent_repo} default-branch head",
        ) or ""
    except Exception:
        logger.exception("Cannot resolve the %s default-branch head; "
                         "skipping stale publish-run detection", agent_repo)
        return ""


def _run_binding(run: Any) -> "tuple[str, str]":
    """(tag, candidate SHA) the run-name carries, ("", "") when the run was
    dispatched without bindings (legacy runs from before the workflow
    required bindings). Only runs whose bindings can be extracted are
    treated as relevant to the current slot; an unbound run must never
    hold or halt the slot, or any repo writer could stage a run that
    permanently denies dispatch."""
    match = _PUBLISH_TITLE_BINDING_RE.search(run.display_title or "")
    if match is None:
        return "", ""
    return match.group("tag"), match.group("sha").lower()


def _matches_branch(run: Any, branch: str) -> bool:
    return f" on {branch} " in f"{run.display_title or ''} "


def _binding_matches_current(run: Any, *, tag: str,
                             candidate_sha: str) -> bool:
    """True when the run carries bindings AND they exactly match the
    current tag+candidate.

    An unbound run (no binding in the run name -- historical, or a
    stray dispatch that predates the workflow's required-input change) is
    treated as NOT MATCHING the current slot: it can neither hold nor halt
    it. Only a run whose bindings exactly identify this candidate may
    occupy the current slot. When *tag* or *candidate_sha* are empty the
    controller has not resolved the current candidate yet, so no match is
    ever claimed (fail closed).
    """
    if not tag or not candidate_sha:
        return False
    run_tag, run_sha = _run_binding(run)
    if not run_tag or not run_sha:
        return False  # unbound: cannot occupy this slot
    return run_tag == tag and run_sha == candidate_sha.lower()


def _is_stale_binding(run: Any, head_sha: str, tag: str,
                      candidate_sha: str) -> bool:
    """True when a gate-parked run is bound to a different controller head
    or (via its run-name) a different tag or candidate than the current
    one. A run whose name carries no binding is treated as unrelated to
    this slot and is neither active nor stale here -- the caller filters
    unbound runs out separately via :func:`_binding_matches_current`."""
    if head_sha and (run.head_sha or "") != head_sha:
        return True
    run_tag, run_sha = _run_binding(run)
    if candidate_sha and run_sha and run_sha != candidate_sha.lower():
        return True
    if tag and run_tag and run_tag != tag:
        return True
    return False


def _list_publish_runs(workflow: Any, statuses: "tuple[str, ...]") -> "list[Any]":
    """All runs in *statuses*, one server-side filtered listing per status.

    Each listing is re-checked client-side (run.status must equal the
    queried status) so an unfiltered or cached listing can never smuggle a
    completed run into the active set, and each run appears exactly once.
    """
    runs: "list[Any]" = []
    for wanted in statuses:
        listing = retry_github_call(
            partial(workflow.get_runs, status=wanted),
            retries=2, description=f"list {wanted} publish runs",
        )
        for run in _scan(listing, _PUBLISH_RUN_SCAN_LIMIT):
            if run.status != wanted:
                continue
            runs.append(run)
    return runs


def find_publish_runs(workflow: Any, branch: str, head_sha: str = "", *,
                      tag: str = "", candidate_sha: str = "",
                      ) -> "tuple[Any, list[Any]]":
    """Pure lookup of the publish runs currently holding *branch*'s slot.

    Returns ``(active, stale)`` and performs NO side effects (cancellation
    is :func:`cancel_stale_publish_runs`' job, wired separately by the
    caller):

    - *active* is a run genuinely in flight for the current controller
      head and candidate. An in_progress run whose bindings match the
      current tag+candidate (or an in_progress run when no bindings are
      set to check against) is always active -- it is past the approval
      gate, publication is in flight, and never cancellable. A
      gate-parked run whose head AND run-name bindings match this
      candidate is also active. Unbound gate-parked runs are IGNORED
      entirely: an unbound run must never hold or halt the slot.
    - *stale* lists gate-parked runs (queued, waiting, pending ONLY) bound
      to a different controller head, tag, or candidate.

    With *head_sha* "" head staleness cannot be judged and no run is
    head-stale (fail-safe); with *tag*/*candidate_sha* "" the run-name
    binding is not checked and unbound in_progress runs remain active
    (fail-safe: a running publication must not be presumed irrelevant).
    """
    active: Any = None
    stale: "list[Any]" = []
    for run in _list_publish_runs(workflow, _PUBLISH_RUN_ACTIVE_STATUSES):
        if not _matches_branch(run, branch):
            continue
        # An in_progress run is past the gate: it IS the publication in
        # progress. When we have bindings to check against, only an
        # in_progress run whose bindings match this candidate holds the
        # active slot; another candidate's in-flight publish does not
        # block a fresh dispatch for THIS candidate. When we have no
        # bindings yet (empty tag/candidate_sha inputs), any in_progress
        # run keeps the fail-safe today: it is never cancelled and it
        # blocks dispatch.
        if run.status == "in_progress":
            if tag and candidate_sha:
                if _binding_matches_current(run, tag=tag,
                                            candidate_sha=candidate_sha):
                    if active is None:
                        active = run
                continue
            if active is None:
                active = run
            continue

        # Gate-parked run: only runs whose bindings identify this
        # candidate may hold the current slot. Unbound gate-parked runs
        # are neither active nor stale here -- they are irrelevant, so
        # they can neither hold nor halt the slot.
        if tag and candidate_sha and not _run_binding(run)[1]:
            continue  # unbound: skip entirely
        if _is_stale_binding(run, head_sha, tag, candidate_sha):
            stale.append(run)
        elif _binding_matches_current(run, tag=tag,
                                      candidate_sha=candidate_sha):
            if active is None:
                active = run
        elif not tag or not candidate_sha:
            # No bindings to check against: keep the legacy behavior
            # (any branch-matching run counts) so a controller call
            # missing tag/candidate context (out-of-flow display path)
            # does not lose runs. Bound runs win over unbound in that
            # case.
            if active is None:
                active = run
    return active, stale


def _cancel_stale_run(run: Any) -> bool:
    """Cancel one gate-parked stale publish run.

    True when the cancel succeeded (the run no longer counts as active);
    False when it failed, in which case the caller treats the run as
    active so a fresh dispatch never races a gate that might still fire.
    """
    try:
        cancelled = retry_github_call(
            run.cancel,
            retries=2, description=f"cancel stale publish run {run.id}",
        )
    except Exception:
        logger.exception("Failed to cancel stale publish run %s; "
                         "treating it as active", run.id)
        return False
    if not cancelled:
        logger.error("Cancel of stale publish run %s was rejected; "
                     "treating it as active", run.id)
        return False
    logger.info("Cancelled stale publish run %s (head %s)", run.id,
                (run.head_sha or "")[:12])
    return True


def cancel_stale_publish_runs(runs: "list[Any]", *, act: bool = True) -> bool:
    """Cancel every stale run in *runs* (the side-effect half of the
    finder/cancel split; :func:`find_publish_runs` never calls this).

    *runs* must come from the finder's stale list, which only ever contains
    queued/waiting/pending runs: an in_progress run is past the approval
    gate and is never staged for cancellation. The guard here is defense in
    depth for a caller passing a hand-built list.

    With ``act`` False this is a strict no-op returning False (observation
    mode: the runs still count as active). Returns True only when every run
    was cancelled; any failure returns False so the caller fails safe.
    """
    if not act:
        return False
    all_cancelled = True
    for run in runs:
        if run.status not in _PUBLISH_RUN_GATED_STATUSES:
            logger.error("Refusing to cancel publish run %s (status %s): "
                         "only gate-parked runs are ever cancelled",
                         run.id, run.status)
            all_cancelled = False
            continue
        if not _cancel_stale_run(run):
            all_cancelled = False
    return all_cancelled


def _active_publish_run(workflow: Any, branch: str, head_sha: str = "", *,
                        tag: str = "", candidate_sha: str = "",
                        act: bool = True) -> Any:
    """The run currently holding *branch*'s publish slot, cancelling stale
    gate-parked runs along the way; None when the slot is free.

    A stale run that could not (or, with ``act`` False, must not) be
    cancelled still holds the slot: its gate might still fire, so a fresh
    dispatch would race it.
    """
    active, stale = find_publish_runs(workflow, branch, head_sha,
                                      tag=tag, candidate_sha=candidate_sha)
    if stale and not cancel_stale_publish_runs(stale, act=act):
        return active if active is not None else stale[0]
    return active


def waiting_publish_run_url(gh_agent: Any, agent_repo: str, branch: str,
                            head_sha: str = "", *, tag: str = "",
                            candidate_sha: str = "") -> str:
    """The html_url of the active publish run for *branch*, "" when none
    is visible (including when the workflow itself is unreadable).

    Display only: reconciliation threads it into the READY callout's
    approval link; nothing gates on it. Observation only (finder, no
    cancel step): a stale run or a run bound to a different tag/candidate
    is simply never presented as the place to approve.
    """
    workflow = workflow_handle(gh_agent, agent_repo, _PUBLISH_WORKFLOW)
    if workflow is None:
        return ""
    active, _stale = find_publish_runs(workflow, branch, head_sha,
                                       tag=tag, candidate_sha=candidate_sha)
    if active is None:
        return ""
    return active.html_url or ""


def _halted_publish_failure(workflow: Any, branch: str, head_sha: str, *,
                            tag: str = "", candidate_sha: str = "") -> Any:
    """The completed publish run that halts re-dispatch, or None.

    The newest COMPLETED publish run for *branch* at the current
    controller head, whose bindings EXACTLY identify the current tag and
    candidate, is inspected: any non-success conclusion halts, success (or
    no such run) does not. The halt covers every non-success
    conclusion GitHub Actions may report (``failure``, ``cancelled``,
    ``timed_out``, ``action_required``, ``startup_failure``, ``skipped``,
    ``neutral``, ``stale``), not just failure/cancelled -- a
    ``timed_out``/``startup_failure`` publish would otherwise re-dispatch
    every reconcile pass forever. A run on another controller head or
    another candidate is skipped entirely, so a new controller head or a
    new candidate re-arms dispatch by construction. An unbound run
    (no binding in its run-name) cannot be proven to belong to the
    current candidate and is IGNORED here as well: it can neither hold
    nor halt the slot. With *head_sha* "" the head filter is skipped:
    halting on the newest matching failure beats looping when staleness
    cannot be judged.
    """
    listing = retry_github_call(
        lambda: workflow.get_runs(status="completed"),
        retries=2, description="list completed publish runs",
    )
    for run in _scan(listing, _PUBLISH_RUN_SCAN_LIMIT):
        if run.status != "completed":
            continue
        if not _matches_branch(run, branch):
            continue
        if head_sha and (run.head_sha or "") != head_sha:
            continue  # another controller version's run: a new head re-arms
        # Only bound runs that exactly match the current candidate
        # may halt the slot. Unbound and cross-candidate runs are ignored.
        if not _binding_matches_current(run, tag=tag,
                                        candidate_sha=candidate_sha):
            continue
        # Any non-success conclusion halts. success passes
        # through; a None conclusion cannot occur alongside
        # status=="completed" but is treated as pass-through for safety.
        conclusion = run.conclusion
        if conclusion and conclusion != "success":
            return run
        return None  # the newest relevant completed run did not fail
    return None


def _advance_publish(gh: Any, gh_agent: Any, agent_repo: str,
                     status: ReleaseStatus, tracking_issue: Any,
                     head_sha: str) -> str:
    """Move the READY phase forward: dispatch the publish pipeline unless a
    run already holds the slot or a completed non-success run halts
    re-dispatch.

    The halt fires for EVERY non-success conclusion, not just
    failure/cancelled -- a timed_out or startup_failure publish run would
    otherwise loop forever. The one-shot marker-gated warning names the
    concluding state so the human sees WHICH kind of failure blocks
    re-dispatch, and a new controller head or a new candidate re-arms
    dispatch (the failed run then no longer matches).
    """
    tag = release_tag(status.version, status.stage)
    candidate_sha = status.candidate.sha
    workflow = workflow_handle(gh_agent, agent_repo, _PUBLISH_WORKFLOW)
    if workflow is None:
        return ""  # cannot see the workflow: do not dispatch blind
    if _active_publish_run(workflow, status.branch, head_sha, tag=tag,
                           candidate_sha=candidate_sha) is not None:
        return ""
    halted = _halted_publish_failure(workflow, status.branch, head_sha,
                                     tag=tag, candidate_sha=candidate_sha)
    if halted is not None:
        conclusion = halted.conclusion or "unknown"
        posted = _autofix_marker_once(
            gh, tracking_issue, key="publish-halt",
            fingerprint_source=f"{candidate_sha}:{halted.id}",
            callout=(
                f"> [!WARNING]\n"
                f"> **The publish pipeline concluded `{conclusion}`;** "
                f"the controller will not re-dispatch until the controller "
                f"code changes or a human re-runs it: "
                f"[run {halted.id}]({halted.html_url})."
            ),
        )
        if posted:
            return (f"halted publish re-dispatch for {status.branch} "
                    f"(publish run {halted.id} concluded "
                    f"{conclusion})")
        return ""
    _dispatch_publish(gh_agent, agent_repo, status.branch, tag=tag,
                      candidate_sha=candidate_sha)
    return (f"dispatched the publish pipeline for {status.branch} "
            f"(holds at the approval gate)")


def _dispatch_publish(gh_agent: Any, agent_repo: str, branch: str,
                      tag: str = "", candidate_sha: str = "") -> None:
    """Dispatch release-publish.yml for *branch*, binding the run to the
    exact *tag* and *candidate_sha* it was dispatched for (stamped into the
    run-name so later passes can correlate by candidate, not just branch).

    ``retries=1`` on ``create_dispatch``: workflow_dispatch has no
    dispatch-echo id in its response, so retrying on a 5xx after the run
    was accepted would leave two publish runs racing the approval gate.
    On an ambiguous failure the next reconcile pass will find the
    accepted run (or its absence) through :func:`find_publish_runs` and
    act accordingly, so retrying here inside the call is redundant and
    unsafe."""
    workflow = workflow_handle(gh_agent, agent_repo, _PUBLISH_WORKFLOW)
    default_branch = retry_github_call(
        lambda: gh_agent.get_repo(agent_repo).default_branch,
        retries=2, description=f"resolve {agent_repo} default branch",
    )
    # "unattended" is the explicit, typed signal that the controller (not
    # a human) dispatched this run: the workflow's plan-only step keys its
    # --unattended flag off this input, never off an actor-name literal.
    # Only this auto-dispatch ever sends true; a human dispatching from
    # the Actions form gets the input's false default.
    inputs = {"branch": branch, "tag": tag, "candidate_sha": candidate_sha,
              "unattended": "true"}
    retry_github_call(
        lambda: workflow.create_dispatch(default_branch, inputs=inputs),
        retries=1, description="dispatch publish pipeline",
    )
    logger.info("Dispatched the publish pipeline for %s (%s @ %s)",
                branch, tag or "<no tag>", candidate_sha[:12] or "<no sha>")
