# Test Failure Detector

Reads the latest Daily CI run on `valkey-io/valkey`, finds every failing test,
and opens or updates one GitHub issue per failure. A fix for an issue it opens
can be prepared by the [CI fix](ci-fix.md#detector-issue-fixes) engine.

| Workflow | File | Runs |
|---|---|---|
| Test Failure Detector | `test-failure-detector-sweep.yml` | daily 23:00 UTC, or by hand |

Valkey's Daily CI starts at 00:00 UTC and usually finishes within 4 to 7
hours (of 411 completed runs, 6 exceeded 7 hours; the longest took 10h 02m),
so the 23:00 UTC sweep sees the current day's run. A missed run can be swept
by hand with its run ID for as long as its artifacts persist (30 days).

## How it works

```text
main.py
  -> get_latest_daily_run() or use the provided run_id
  -> jobs_for_run_attempt(): one jobs snapshot of the run's current attempt
  -> download_all_test_failures() from the run's artifacts
  -> job_urls_for_jobs() for CI links
  -> parse_and_deduplicate() groups by {test_name, test_file}
  -> find_unrecorded_failures() adds what only the failed jobs' logs show
  -> process_failures() creates/updates GitHub issues
```

1. **Find the run.** The most recent completed, non-cancelled, non-skipped
   Daily run on the target branch (`unstable` by default), or the run ID given
   on dispatch.
2. **Download the artifact.** `ArtifactClient` fetches `all-test-failures`,
   handling the auth-header-stripping redirect to Azure blob storage, transient
   retries, and expired (404) artifacts.
3. **Build CI links.** Job metadata gives each failure a link, with normalized
   name variants for fuzzy matching against artifact names.
4. **Parse and deduplicate.** The nested JSON (`{job -> suite -> [failures]}`)
   is grouped by a `{test_name}::{test_file}` fingerprint, so a test failing
   in several jobs becomes one `UniqueFailure` with several job references.
5. **Read failed jobs the artifact does not record.** A job also fails on a
   test timeout, a valgrind or sanitizer report, the shutdown leak check, an
   exception in the test framework, or a failed C unit test (Google Test);
   none of those reach the artifact. For each failed job the detector reads
   what runtest prints: its closing summary ("The following tests failed:")
   and an `[exception]:` line.
   - A line naming a test and its file joins that test's issue.
   - Anything else becomes a job-level issue titled by its cause,
     `[JOB-FAILURE] <cause> in Daily`: the exception with the test file its
     stack points at, or a valgrind or sanitizer report's first error and
     frames. One cause is one issue however many jobs and steps it fails; a
     job that printed two causes is in both.
   - A failed C unit test is filed by name and file:
     `[JOB-FAILURE] Unit test Suite.Test failed in test_x.cpp in Daily`.
   - A failed job that printed no cause (a build break, a crash, a mirror
     outage) is not filed, since nothing identifies what failed; the run's
     summary lists it.
   - The registry's `ci_followup_ignored_jobs` (the result aggregators) and
     setup-step failures are not reported.
   - A failed job whose log cannot be read is listed under *Unread job logs*
     in the summary and fails the run. If the registry cannot be read, nothing
     is filed and the run fails, since the jobs it says to ignore would be
     filed.
6. **Create or update issues.** `IssueDedupPublisher` upserts one issue per
   fingerprint, matched on a hidden body marker
   (`<!-- valkey-ci-agent:test-failure:<hex-hash> -->`, hashed from the test
   name and file) rather than the title. Each failure ends in one of:
   - **created** - no matching issue: opens one titled
     `[TEST-FAILURE] {test_name} in {test_file}` with the `test-failure`
     label, error trace, CI links, and environment list.
   - **updated** - a match exists: merges new failing environments into the
     body and bumps the occurrence counter, with a recurrence comment.
   - **skipped** - the run ID matches the `last-key` marker already on the
     issue, so re-sweeping one run does not inflate the count or duplicate the
     comment.
   - **skipped-recently-closed** - no open issue matches, but one with the
     same marker (or exact title, for issues from the older fingerprint
     scheme) was closed within the past day, so the failure was likely just
     fixed. This check is opt-in on the shared publisher and enabled only
     here, so the fuzzer monitor never suppresses a recurring incident.

A job summary with the metrics (failures detected, issues created and
updated) is emitted on every exit path.

## Running it

```bash
gh workflow run test-failure-detector-sweep.yml \
  --repo valkey-io/valkey-ci-agent \
  --field repo=valkey-io/valkey \
  --field run_id=12345678 \
  --field dry_run=true
```

- `repo` - repository to scan (default `valkey-io/valkey`)
- `run_id` - a specific run (empty = the latest Daily run)
- `dry_run` - parse and report only; create or update nothing

## Configuration

The workflow mints a GitHub App installation token from the shared App
secrets ([setup.md](setup.md)) with `actions:read` (artifacts and logs) and
`issues:write` on `valkey-io/valkey`.

## Internals

The detector is a thin pipeline over shared building blocks in
`scripts/common/`: `ArtifactClient` (artifact download), `workflow_jobs` (one
exact-attempt jobs snapshot, shared with the CI fix engine), `valkey_ci`
(runtest result lines, also shared), and `IssueDedupPublisher` (issue upsert,
shared with the fuzzer monitor).

- `scripts/test_failure_detector/main.py` - CLI entry point and pipeline orchestration
- `scripts/test_failure_detector/download.py` - run discovery and artifact download
- `scripts/test_failure_detector/parse_failures.py` - JSON parsing and deduplication
- `scripts/test_failure_detector/job_failures.py` - failed jobs the artifact does not record, from their logs
- `scripts/test_failure_detector/manage_issues.py` - orchestration over the shared dedup publisher
- `scripts/test_failure_detector/issue_renderer.py` - title, body, recurrence comment, and label
