# Fuzzer monitor

[valkey-fuzzer](https://github.com/valkey-io/valkey-fuzzer) runs Valkey
clusters through randomized workloads and deliberate chaos: killing nodes,
forcing failovers, cutting replication links. Some of what it reports is the
expected result of that chaos, and some of it is a real bug. Reading every
run by hand to tell them apart doesn't scale, so the fuzzer monitor does a
first pass and files an issue on `valkey-io/valkey-fuzzer` when a run looks
like a real problem.

The monitor only ever creates and updates issues. It never changes code in
either repository.

## How it works

Every four hours, the monitor takes the latest scheduled fuzzer run and
downloads its artifacts.

It starts with a quick scan for the signs of real trouble: crashes, sanitizer
reports, failed failovers and RDB errors in the run's results and node logs.
It ignores things the chaos is expected to cause, such as `CLUSTERDOWN` or a
lost replication link.

Then Claude takes a closer look. The monitor puts the artifacts in a
temporary directory next to a copy of Valkey at the commit that was tested
and a copy of the fuzzer at the run's commit. Claude can read and search
those files but can't change anything, run commands or reach the network. With
the source in front of it, Claude can look up an assertion message or a crash
handler and tell a known, harmless assertion apart from a new crash. It
returns a verdict on whether the run shows a real bug.

When something goes wrong along the way, the monitor errs on the side of
asking a human. If it can't fetch the source, Claude works from the artifacts
alone and is told not to cite line numbers. If Claude itself fails, the
quick scan's findings stand and the run is marked `needs-human-triage` rather
than reported as normal. A run that produced no artifacts is reported as an
error.

Runs that look like a real problem get an issue on `valkey-fuzzer`. Each
issue is identified by a fingerprint of the root cause and the shape of the
failure, so the same problem showing up in later runs updates the existing
issue instead of opening a new one. The monitor uploads its results and
Claude's evidence as artifacts on every run.

## Running it by hand

```bash
# Check the latest scheduled fuzzer run, for real
gh workflow run monitor-fuzzer.yml --repo valkey-io/valkey-ci-agent

# Try it without calling Claude or filing anything
gh workflow run monitor-fuzzer.yml --repo valkey-io/valkey-ci-agent --field dry_run=true
```

Scheduled runs always run for real.

## Configuration

The monitor uses the shared secrets described in [setup.md](setup.md). The
GitHub App needs to read Actions and contents and write issues on
`valkey-io/valkey-fuzzer`, and the workflow asks for a token limited to that
one repository.

## Where the code lives

`scripts/fuzzer/main.py` is the entry point. `analyzer.py` does the work:
the quick scan, cloning the source, and calling Claude with the read-only
`fuzzer_analysis_readonly` profile. `issue_renderer.py` writes the issue, and
`models.py` holds the data types. Downloading artifacts, cloning, the
fingerprint and issue updates all come from `scripts/common/`.
