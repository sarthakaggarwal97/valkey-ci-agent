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

Every four hours, the monitor takes the latest scheduled run of the fuzzer's
`fuzzer-run.yml`. That workflow runs a batch, `valkey-fuzzer batch ... --out
$FUZZER_ARTIFACT_DIR/batch`, and uploads it as the
`fuzzer-run-artifacts-<run id>` bundle. The monitor downloads the bundle and
reads the batch folder in it:

- `batch/summary.json`, the batch's summary: its worst result, the runs by
  status, every run (`runs`, each with its status, summary and the path of
  its run report, or null when the run left none), the runs that did not
  pass grouped by the fuzzer's failure signature (`signatures`), each with
  the verdict of its serial re-runs, the bug zoo's verdicts (`zoo`) and what
  the batch had to clean up after its runs (`leaks`);
- for each run, `batch/runs/<id>/report.json`, the fuzzer's run report: the
  outcome (`PASS`, `FAIL`, `ERROR` or `SKIP_UNSUPPORTED`), the scenario as
  run, every component's result, the events and evidence, and the reason a
  run did not pass;
- `batch/runs/<id>/scenario.yaml`, the scenario, which `valkey-fuzzer run`
  accepts as is, and `error.json` when a job could not be generated or
  compiled;
- `batch/runs/<id>/logs/`, each instance's server log,
  `<seed>-<deployment id>-valkey-N.log`, and its captured output,
  `<seed>-<deployment id>-valkey-N.stdio.log`.

It reads every run report and starts with a quick scan of each. From a
report it takes the outcome, each component that failed or errored with its
summary or the exception that stopped it, the required events and evidence
that were never observed, and any cleanup errors. In the report and the
node logs it looks for the signs of real trouble: crashes, sanitizer
reports, failed failovers and RDB errors. It ignores things the chaos is
expected to cause, such as `CLUSTERDOWN` or a lost replication link.

Then it turns the batch into findings, and each finding is one issue:

- **A failure signature.** The fuzzer groups the runs that did not pass by
  their failure signature: the same outcome, the same failing components
  with the same verdicts, the same missing events and evidence. The monitor
  keeps that grouping, so one bug seen in three runs is one issue, listing
  the three runs. The batch re-runs some runs of each signature one at a
  time, with nothing else on the host. When the re-runs reproduce the
  signature, a `FAIL` the scan rated a possible Valkey bug becomes a likely
  one; when they don't, the failure points to flakiness or load, and a
  likely Valkey bug becomes a possible one. Either way the issue says what
  the re-runs showed.
- **A run that passed but shows something.** A passing run is quiet unless
  its scan finds something, such as a crash in a node log or a failed
  optional component; then it is a finding of its own.
- **A bug zoo case that doesn't hold.** The zoo is a set of Valkey builds
  with known, deliberate bugs. A case that doesn't hold means the fuzzer
  missed a bug it is known to catch, or its clean control failed: the fuzzer
  itself went blind. That is filed as `needs-human-triage` for valkey-fuzzer
  and never as a Valkey bug. The zoo's own runs fail on purpose and are
  never findings.
- **Leaks.** What a run left behind (servers, iptables chains, tmpfs mounts,
  data directories) is a bug in the fuzzer's cleanup, filed for
  valkey-fuzzer the same way, one issue per experiment and kind of leftover.
  A run that passed and only leaked is not also filed as a failure.
- **The batch itself**, when it didn't finish.

The monitor fails closed. A bundle without `batch/summary.json`, or with one
that isn't shaped like a batch summary, is one `needs-human-triage` finding.
So is a run whose report is missing, unreadable or not a run report, or a
run that left no report at all: the finding it belongs to goes to a human,
and Claude can't talk it out of that. The monitor recognizes a run report by
its shape: the keys every report carries (`generated_at`, `status`,
`summary`, `scenario`, `outcome`, `coverage`, `reproducer` and
`provenance`), a known outcome, and an outcome object with its own status and
the component results. A run that errored or was skipped as unsupported goes
to a human too, since it tested nothing.

Then Claude takes a closer look at each finding that isn't the fuzzer's own
problem, up to five per batch. The monitor puts the batch summary and the
finding's run folders (up to three runs of a signature) in a temporary
directory next to a copy of Valkey at the commit that was tested and a copy
of the fuzzer at the run's commit. Claude can read and search those files
but can't change anything, run commands or reach the network. The prompt
explains the summary, the report's fields, the serial re-runs and how the
log files map to the instances in a report. With the source in front of it,
Claude can look up an assertion message or a crash handler and tell a
known, harmless assertion apart from a new crash. It returns a verdict on
whether the finding shows a real bug. The monitor checks that verdict: an
answer outside the expected values becomes `needs-human-triage`, a run the
fuzzer didn't pass is never reported as `normal`, and the serial re-runs
adjust it as above.

When something goes wrong along the way, the monitor errs on the side of
asking a human. If it can't fetch the source, Claude works from the artifacts
alone and is told not to cite line numbers. If Claude itself fails, the
quick scan's verdict stands and the issue says so. A run that produced no
artifacts is reported as an error.

Findings that look like a real problem get an issue on `valkey-fuzzer`. A
signature's issue names the runs, the signature and its serial re-runs, the
fuzzer's outcome, the experiment, the scenario, the seed and the Valkey
commit, and how to run it again from the downloaded bundle:
`valkey-fuzzer replay batch/runs/<id>/report.json --valkey-binary
/path/to/valkey-server` with a build of that commit. A signature's issue is
identified by the fuzzer's signature key, so the same failure in later
batches updates the existing issue instead of opening a new one; a zoo case
by its name; a leak by its experiment and kind. The monitor uploads its
results and Claude's evidence as artifacts on every run.

## Running it by hand

```bash
# Check the latest scheduled fuzzer run, for real
gh workflow run monitor-fuzzer.yml --repo valkey-io/valkey-ci-agent

# Try it without calling Claude or filing anything
gh workflow run monitor-fuzzer.yml --repo valkey-io/valkey-ci-agent --field dry_run=true
```

Scheduled runs always run for real.

The fuzzer's other scheduled workflow, Valkey Experiments Live, uploads only
its pytest output, so the monitor doesn't watch it.

## Configuration

The monitor uses the shared secrets described in [setup.md](setup.md). The
GitHub App needs to read Actions and contents and write issues on
`valkey-io/valkey-fuzzer`, and the workflow asks for a token limited to that
one repository.

## Where the code lives

`scripts/fuzzer/main.py` is the entry point. `batch.py` reads the batch
folder. `analyzer.py` does the work: the quick scan, the findings, cloning
the source, and calling Claude with the read-only `fuzzer_analysis_readonly`
profile. `issue_renderer.py` writes the issue, and
`models.py` holds the data types. Downloading artifacts, cloning, the
fingerprint and issue updates all come from `scripts/common/`.
