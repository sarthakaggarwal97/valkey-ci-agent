# Fuzzer scenario proposals

[valkey-fuzzer](https://github.com/valkey-io/valkey-fuzzer) runs scenarios: each
one picks an experiment, such as cluster failovers under process kills or
replication with compression, gives it inputs, and describes the deployment.
Its random runs cover a lot of ground, but they don't know what a particular
change to Valkey touches. Fuzzer proposals close that gap. Given a
`valkey-io/valkey` pull request, the agent asks Claude to read the change and
write scenarios aimed at what it could break, and keeps only the ones the
fuzzer accepts.

Proposals never change code and never run anything on their own. The result
is a set of scenario files and a short reason for each, uploaded as an
artifact. Running them as a PR batch in valkey-fuzzer is a separate step
that is off unless asked for.

## How it works

Someone starts the workflow with a PR number. The agent looks the PR up,
pins it to its head and base commits, and downloads its diff. It also asks
the installed fuzzer what it has, with `valkey-fuzzer list experiments` and
`valkey-fuzzer list components`.

Claude then gets a temporary directory holding the diff, a copy of Valkey at
the PR's head, a copy of the fuzzer, and the fuzzer's list of experiments and
components. Claude can read and search those files but can't change
anything, run commands or reach the network, and it never has a GitHub token.
It reads the change and the code around it, works out what could go wrong,
picks experiments that reach that code, and answers with up to the requested
number of scenarios, each with a sentence or two on what it targets.

Claude's answer is only text. The agent checks every scenario before the
fuzzer sees it:

- it must be one YAML mapping with only `seed`, `experiment` and
  `deployment`;
- it may not choose the build, the deployment provider, data directories,
  artifacts, resources or policies, since those belong to whoever runs it;
- the agent names it `ai-<hash>` from its content, so the same scenario
  always gets the same name and duplicates are dropped.

The agent then writes each scenario to a file and runs
`valkey-fuzzer validate` on it, which compiles it and runs every check that
needs no server. A scenario that fails, either the agent's checks or the
fuzzer's, goes back to Claude once, in a new session, together with the
error. Whatever still fails after that one repair round is dropped. Only
scenarios that validated are written to the output.

## What you get

The run uploads `fuzzer-proposals-<run id>`:

| File | Contents |
|---|---|
| `scenarios/ai-<hash>.yaml` | Each scenario that validated, ready for `valkey-fuzzer run`. |
| `proposals.json` | The PR and its commits, each validated scenario with its file and reason, and each dropped one with its error. |

The job summary lists the scenarios and their reasons. To run one yourself,
build Valkey at the PR's head and run:

```bash
valkey-fuzzer run scenarios/ai-<hash>.yaml --valkey-binary /path/to/valkey-server
```

## Running them as a PR batch

With `dispatch_batch` on, the agent also starts valkey-fuzzer's
`fuzzer-pr-batch.yml` workflow ("Valkey Fuzzer PR Batch") with its inputs:
`scenarios`, a JSON object mapping each scenario name (`ai-<hash>`) to its
YAML; `valkey_repository`; `valkey_ref`, `refs/pull/<n>/head` (the head
commit for a range); and `valkey_sha`, the full head commit. Proposals never
need its optional `spec_bundles`. Before it dispatches, the agent reads the
files back and checks that each one is still the scenario that validated,
and it refuses anything the workflow's own input check would refuse: a
`scenarios` over 60,000 characters, a repository that isn't `OWNER/NAME`, a
commit that isn't a full 40-character lowercase SHA, a name outside letters,
digits, `.`, `_` and `-`, a scenario that isn't a YAML mapping or that sets
`deployment.valkey_binary`. The dispatch uses its own token, made after
Claude has finished, that can start workflows on valkey-fuzzer and do
nothing else. A dry run never dispatches.

The PR batch builds that Valkey commit, runs the scenarios through
`valkey-fuzzer batch` with the candidate profile, and uploads the batch
folder as `fuzzer-batch-<run id>`. It is read with the same reader the
[fuzzer monitor](fuzzer-monitor.md) uses for the scheduled batch:
`summary.json`, then each run's report at `runs/<id>/report.json`. A
scenario `ai-<hash>` ran as the job `scenario-ai-<hash>`:

```bash
# Download the PR batch run's artifact and read it
gh run download <run id> --repo valkey-io/valkey-fuzzer --name fuzzer-batch-<run id> --dir pr-batch
python -m scripts.fuzzer.pr_batch --batch pr-batch

# Or let the agent download it, with a token that can read Actions on valkey-fuzzer in TARGET_TOKEN
python -m scripts.fuzzer.pr_batch --run-id <run id>
```

It writes `fuzzer-batch-results/batch-results.json` and a job summary:
each scenario's status and summary, the failure signatures with their serial
re-runs, and a verdict. The verdict is `pass` when every run passed and
`fail` when one didn't. It is `needs-human-triage` when the folder can't be
trusted: a run without a readable report, a batch that didn't finish, a bug
zoo case that doesn't hold, or a leak. A summary that can't be read is an
error. The command exits non-zero unless the verdict is `pass`.

## Running it by hand

```bash
# Check the inputs and the fuzzer install, without fetching the PR or calling Claude
gh workflow run fuzzer-propose.yml --repo valkey-io/valkey-ci-agent --field pr_number=1234

# Propose scenarios for real
gh workflow run fuzzer-propose.yml --repo valkey-io/valkey-ci-agent \
  --field pr_number=1234 --field count=5 --field dry_run=false

# ... and run them as a PR batch in valkey-fuzzer
gh workflow run fuzzer-propose.yml --repo valkey-io/valkey-ci-agent \
  --field pr_number=1234 --field dry_run=false --field dispatch_batch=true
```

The workflow does a dry run unless you turn it off.

On your own machine, with valkey-fuzzer checked out and installed, AWS
credentials for Bedrock and a GitHub token in `TARGET_TOKEN`:

```bash
python -m scripts.fuzzer.propose propose --pr 1234 \
  --fuzzer-dir ../valkey-fuzzer --fuzzer-bin valkey-fuzzer --out fuzzer-proposals

# A commit range works too
python -m scripts.fuzzer.propose propose --range 9.0.0..unstable \
  --fuzzer-dir ../valkey-fuzzer --fuzzer-bin valkey-fuzzer

# See what would be dispatched
python -m scripts.fuzzer.propose dispatch --out fuzzer-proposals --dry-run
```

## Configuration

The workflow uses the shared secrets described in [setup.md](setup.md). It
asks for two tokens, each only when it needs it: one that can read
`valkey-io/valkey` to fetch the PR, and, when dispatching, one that can start
workflows on `valkey-io/valkey-fuzzer`.

## Where the code lives

`scripts/fuzzer/propose.py` holds both commands. `propose` resolves the
request, prepares the directory Claude reads, calls Claude with the read-only
`fuzzer_proposal_readonly` profile, and checks, validates and repairs what
comes back. `dispatch` reads the result back and starts the PR batch.
`scripts/fuzzer/pr_batch.py` checks the PR batch's inputs and reads the
batch it uploads, with `scripts/fuzzer/batch.py`. The workflow is
`.github/workflows/fuzzer-propose.yml`.
