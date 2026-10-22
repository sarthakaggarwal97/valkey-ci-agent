# Fuzzer monitor

Watches scheduled `valkey-io/valkey-fuzzer` runs, analyzes their artifacts,
and files an issue on `valkey-fuzzer` for runs that look like a real bug
rather than chaos-expected noise. It never writes to source in either
repository; its only side effect is creating or updating issues.

| Workflow | File | Runs |
|---|---|---|
| Monitor Valkey Fuzzer Runs | `monitor-fuzzer.yml` | every 4 hours, or by hand |

## How it works

1. **Find the run.** The latest scheduled fuzzer run.
2. **Deterministic scan.** Pattern-match crash, sanitizer, failover and RDB
   signals in the artifact JSON and node logs, ignoring chaos-expected noise
   (CLUSTERDOWN, replication link loss).
3. **Claude Code analysis.** Drop the artifacts in a tempdir, shallow-clone
   `valkey-io/valkey` at the tested commit and `valkey-io/valkey-fuzzer` at the
   run's HEAD, and ask Claude, with read-only `Read,Grep,Glob` tools, to
   correlate the failure with source and decide whether it is a real bug. The
   clones let it grep for assertion text or crash handlers in `valkey/src/` to
   tell a known-benign assert from a new crash.
   - If a clone fails, the prompt tells Claude not to cite source line numbers
     and the analysis falls back to the artifacts alone.
   - If Claude itself fails, the deterministic findings stand and the verdict
     is labelled `needs-human-triage` rather than "normal".
   - A run with no artifact bundle is reported as an error, not triaged from
     raw logs.
4. **File or update an issue.** Anomalous runs upsert an issue on
   `valkey-io/valkey-fuzzer`, deduplicated by a stable fingerprint over the
   root cause and anomaly shape.
5. **Audit.** Per-run JSON results and Claude's evidence are uploaded as
   workflow artifacts.

The Claude subprocess runs under the `fuzzer_analysis_readonly` profile: no
editing, no shell, no network beyond the Bedrock call.

## Running it

```bash
# Live, against the latest scheduled fuzzer run (the default)
gh workflow run monitor-fuzzer.yml --repo valkey-io/valkey-ci-agent

# Probe without invoking Claude or filing issues
gh workflow run monitor-fuzzer.yml \
  --repo valkey-io/valkey-ci-agent \
  --field dry_run=true
```

Scheduled runs always run live.

## Configuration

Reuses the shared secrets and OIDC role ([setup.md](setup.md)). The
Valkeyrie GitHub App needs `actions:read`, `contents:read` and `issues:write`
on `valkey-io/valkey-fuzzer`; the workflow mints a token scoped to that
repository only.

## Internals

```text
fuzzer/main.py
  -> common.workflow_artifacts.ArtifactClient.list_recent_runs(...)
  -> FuzzerRunAnalyzer.analyze(run)
       common.workflow_artifacts -> download the artifact bundle
       analyzer._scan_logs()     -> deterministic regex pass
       analyzer._invoke_claude() -> artifacts in a tempdir; common.git_clone
                                    shallow-clones valkey + valkey-fuzzer at
                                    the tested SHAs; Claude under the
                                    fuzzer_analysis_readonly profile; JSON verdict
       common.incidents.compute_fingerprint() -> stable hash over the anomaly shapes
  -> common.issue_dedup.IssueDedupPublisher.upsert(...)
       fuzzer.issue_renderer.render_for(analysis) -> title/body/comment
```

- `scripts/fuzzer/main.py` - CLI entry point
- `scripts/fuzzer/analyzer.py` - orchestration, deterministic scan, Claude Code integration
- `scripts/fuzzer/issue_renderer.py` - title, body and comment rendering
- `scripts/fuzzer/models.py` - typed dataclasses for the analysis pipeline
