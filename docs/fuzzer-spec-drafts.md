# Fuzzer spec drafts

[valkey-fuzzer](https://github.com/valkey-io/valkey-fuzzer) can check a
protocol against a TLA+ model through its
[spec bridge](https://github.com/valkey-io/valkey-fuzzer/blob/main/docs/spec-bridge.md).
A spec bundle holds the claims about the protocol, the model, the configs TLC
checks, a negative control for every property, and a mapping from the model to
the evidence a fuzzer run records. The fuzzer model-checks the bundle, turns
each control's counterexample into scenarios, and checks the claims on real
runs. Writing a bundle by hand takes a long time. Given a `valkey-io/valkey`
pull request, the agent asks Claude to draft one for the protocol the change
touches, and the fuzzer's own tools decide whether the draft can be used.

**AI drafts never gate merges.** A draft always has `status: draft`; Claude
can't approve it and neither can the agent. A bundle can become a required
check only after a maintainer reviews its claims and mapping, sets
`status: approved` and `approved_by`, and the fuzzer runs
`valkey-fuzzer spec check --require-approved` on it. Until then a draft that
passes every gate is useful evidence: its scenarios can run at once, and a
maintainer has a starting point for a real bundle.

Spec drafts never change code. The result is the bundle, the fuzzer's reports,
the translated scenarios and a short summary, uploaded as an artifact. Running
the scenarios as a PR batch in valkey-fuzzer is a separate step that is off
unless asked for.

## How it works

Someone starts the workflow with a PR number. The agent looks the PR up, pins
it to its head and base commits, downloads its diff and checks Valkey out at
the head. Before calling Claude it checks the tools: it asks the installed
fuzzer for its experiments and components, and runs `valkey-fuzzer spec check`
on the fuzzer's own example bundle, so a broken Java or `tla2tools.jar` stops
the run before anything is spent.

Claude then gets a temporary directory holding the diff, Valkey at the PR's
head, a copy of the fuzzer (its spec-bridge docs and the example bundles
`formal/cluster-election/` and `tests/fixtures/spec/register/`), and the
fuzzer's list of experiments and components. It runs under the read-only
`fuzzer_spec_draft_readonly` profile: it can read and search those files, but
it can't change anything, run commands or reach the network, and it never has
a GitHub token.

Claude first decides whether the change touches a protocol a model can say
something about: an ordering, durability, replication, failover, election or
cluster-state rule whose correctness depends on interleavings, crashes or lost
messages. If it doesn't, Claude says so with a reason, and the run ends there.
Otherwise it answers with the bundle's files as text:

- `claims.yaml`, with `status: draft`: the properties, each with its
  statement, the constants it holds under and its negative controls, plus the
  scope, assumptions, bounds and exclusions;
- `model.tla` (`MODULE model`);
- one or more expected-safe `.cfg` files;
- `mapping.yaml`;
- `negative-controls/`, at least one per property;
- optionally `experiments/<id>.yaml`: when no installed experiment can inject
  the faults the mapping names, a data-only experiment composed only of
  installed components, as the fuzzer's register bundle has (see the spec
  bridge's "Onboarding a protocol with data only");
- a note on why the change is worth a model and what each counterexample
  means for Valkey.

## The gates

The agent writes the files and runs the gates in order. The first that fails
stops the attempt.

| Gate | Passes when |
|---|---|
| `answer` | The reply holds a bundle in the expected format, with only bundle paths (`claims.yaml`, `mapping.yaml`, `model.tla`, `<name>.cfg`, `negative-controls/<name>.cfg` or `.tla`, `experiments/<id>.yaml`). |
| `draft` | The bundle has the required files, and `claims.yaml` says `status: draft` with no `approved_by`. |
| `spec_check` | `valkey-fuzzer spec check --timeout 1800 --output assurance.json` passes every gate of its report: the bundle parses, its modules use only the standard TLA+ modules, SANY accepts them, every claim is checked by a safe config, TLC finds no error in any safe config, every negative control violates its property, each control's counterexample compiles into scenarios whose mapped faults are fault components of the experiment, and the mapping is complete. |
| `translate` | `valkey-fuzzer spec translate --control` writes scenarios for every control the `experiment` gate translated. A `no-fault` control, whose counterexample has no mapped fault, has none. |
| `validate` | `valkey-fuzzer validate`, with `--experiment-dir` for the bundle's own `experiments/`, accepts every translated scenario, and none sets the build, the provider or the data directories. |

The agent checks only what the fuzzer can't know, that the claims are a
draft; everything else is the fuzzer's own gates. Each draft is written to
`<attempt>/<protocol>/`. When it has `experiments/`, the attempt directory
goes on `VALKEY_FUZZER_SPEC_PATH`, so the experiment's components find the
bundle by its protocol name.

The negative-control gate is what keeps a claim honest. A property that cannot
fail, such as `Inv == TRUE`, passes every safe config; its negative control
then finds no violation, and the draft is rejected.

A draft that fails a gate goes back to Claude, in a new session, with the
gate's output and the failed bundle. It gets at most two of these repair
rounds. A draft that still fails is kept as rejected, with the reason; its
scenarios are never written, so it can never be dispatched. A model too big
for TLC to finish in 30 minutes is stopped by the fuzzer's `--timeout`, fails
`spec_check` (the report marks the run `timed_out`), and is sent back to
shrink its bounds. A broken fuzzer, Java or jar fails the job instead, since that is
not the draft's fault.

## What you get

The run uploads `fuzzer-spec-draft-<run id>`:

| File | Contents |
|---|---|
| `summary.md` | For maintainers: the verdict, the claims, assumptions, exclusions and bounds, what each counterexample means, and every round's gate results. It is also the job summary. |
| `spec-draft.json` | The PR and its commits, the verdict (`passed`, `rejected` or `no_protocol_change`) and reason, every round, the final gates, the counterexamples, and each scenario with its file and SHA-256. |
| `bundle/<protocol>/` | The final draft, passed or rejected. Put `bundle/` on `VALKEY_FUZZER_SPEC_PATH` to run the fuzzer's spec tools on it by name. |
| `assurance.json` | The fuzzer's `spec check` report on it. |
| `scenarios/<control>/` | Only for a draft that passed: the scenarios `spec translate` wrote, and its `translation.json`. |
| `validate.json` | Only for a draft that passed: the fuzzer's `validate` report. |

To run a scenario yourself, build Valkey at the PR's head and run (the
`--experiment-dir` only when the bundle brings its own experiment):

```bash
export VALKEY_FUZZER_SPEC_PATH="$PWD/bundle"
valkey-fuzzer run scenarios/<control>/spec-<protocol>-<control>.yaml \
  --experiment-dir bundle/<protocol>/experiments --valkey-binary /path/to/valkey-server
```

To adopt a draft, copy `bundle/<protocol>/` to `formal/<protocol>/` in valkey-fuzzer,
review it there, and approve it as described in the spec bridge's
[Writing a bundle](https://github.com/valkey-io/valkey-fuzzer/blob/main/docs/spec-bridge.md#writing-a-bundle).

## Running the scenarios as a PR batch

With `dispatch_batch` on, the agent also starts the `fuzzer-pr-batch.yml`
workflow in valkey-fuzzer with the scenarios of a draft that passed every
gate, the same way [Fuzzer proposals](fuzzer-proposals.md#running-them-as-a-pr-batch)
do. Before it does, it reads each file back and checks its SHA-256 against
the one recorded when it validated. The PR batch's inputs and checks are
the same as for proposals; each scenario is named by its ID, and an ID that
`spec translate` gave characters the workflow's names can't hold, such as
`:`, has them replaced with `-` (the scenario's own `scenario_id` is
unchanged).

`spec-draft.json` records each scenario's experiment and whether the
fuzzer has it installed, and the published bundle's files with their
SHA-256s. When a scenario runs the draft's own `experiments/<id>.yaml`, the
bundle goes with the scenarios in `spec_bundles`, under its protocol name,
after each file is checked against its SHA-256: the PR batch writes it where
`VALKEY_FUZZER_SPEC_PATH` finds it and loads its `experiments/`. The
workflow's checks of `spec_bundles` are refused before dispatch too: at most
60,000 characters, a lowercase name of letters, digits and `-`,
`claims.yaml`, `model.tla` and `mapping.yaml`, and only `<name>.cfg`,
`negative-controls/<name>.cfg` or `.tla` and `experiments/<name>.yaml`
besides. A scenario whose experiment is neither installed nor in the bundle,
such as one from `--experiment-dir`, can't run there, and the draft is
refused with the scenarios named. A rejected draft, or one with no protocol
change, dispatches nothing. The dispatch uses its own token, made after
Claude has finished, that can start workflows on valkey-fuzzer and do
nothing else. A dry run never dispatches.

The PR batch uploads its batch folder as `fuzzer-batch-<run id>`, read with
`python -m scripts.fuzzer.pr_batch` exactly as for
[proposals](fuzzer-proposals.md#running-them-as-a-pr-batch): the scenario
`<name>` ran as the job `scenario-<name>`.

## Running it by hand

```bash
# Check the inputs, the fuzzer and the TLA+ tools, without fetching the PR or calling Claude
gh workflow run fuzzer-spec-draft.yml --repo valkey-io/valkey-ci-agent --field pr_number=1234

# Draft a bundle for real
gh workflow run fuzzer-spec-draft.yml --repo valkey-io/valkey-ci-agent \
  --field pr_number=1234 --field dry_run=false

# ... and run a passing draft's scenarios as a PR batch in valkey-fuzzer
gh workflow run fuzzer-spec-draft.yml --repo valkey-io/valkey-ci-agent \
  --field pr_number=1234 --field dry_run=false --field dispatch_batch=true
```

The workflow does a dry run unless you turn it off.

On your own machine, with valkey-fuzzer checked out and installed, Java 11 or
later, `tla2tools.jar` v1.7.4, AWS credentials for Bedrock and a GitHub token
in `TARGET_TOKEN`:

```bash
python -m scripts.fuzzer.spec_draft draft --pr 1234 \
  --fuzzer-dir ../valkey-fuzzer --fuzzer-bin valkey-fuzzer \
  --tla2tools tla2tools.jar --out fuzzer-spec-draft

# A commit range works too
python -m scripts.fuzzer.spec_draft draft --range 9.0.0..unstable \
  --fuzzer-dir ../valkey-fuzzer --tla2tools tla2tools.jar

# See what would be dispatched
python -m scripts.fuzzer.spec_draft dispatch --out fuzzer-spec-draft --dry-run
```

`--experiment-dir DIR` also loads the data-only experiments in `DIR` in
every gate. A bundle's own `experiments/` needs no flag.

The end-to-end tests run the real fuzzer and TLC, with a stub in place of
Claude that answers with the fuzzer's register bundle and with a vacuous
variant of it. They skip unless you point them at the tools:

```bash
VALKEY_FUZZER_DIR=../valkey-fuzzer VALKEY_FUZZER_BIN=valkey-fuzzer TLA2TOOLS_JAR=tla2tools.jar \
  python -m pytest tests/test_fuzzer_spec_draft.py -k e2e
```

## Configuration

The workflow uses the shared secrets described in [setup.md](setup.md). It
asks for two tokens, each only when it needs it: one that can read
`valkey-io/valkey` to fetch the PR, and, when dispatching, one that can start
workflows on `valkey-io/valkey-fuzzer`. It downloads `tla2tools.jar` v1.7.4
and checks its SHA-256, the same pin as valkey-fuzzer's TLA+ Model Check
workflow; update both together.

## Where the code lives

`scripts/fuzzer/spec_draft.py` holds both commands. `draft` resolves the
request, prepares the directory Claude reads, calls Claude with the read-only
`fuzzer_spec_draft_readonly` profile, runs the gates and the repair rounds,
and writes the result. `dispatch` reads the result back and starts the
PR batch. The request and workspace code is shared with
`scripts/fuzzer/propose.py`, and the PR batch's inputs and results with
`scripts/fuzzer/pr_batch.py`. The workflow is
`.github/workflows/fuzzer-spec-draft.yml`.
