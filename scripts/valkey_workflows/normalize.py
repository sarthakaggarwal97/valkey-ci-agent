"""Turn one of Valkey's workflow files into a copy this repository can call.

The CI-fix bot fixes a failure inside the very job that failed: same runner,
build flags, setup and test command. Valkey's ``daily.yml`` and ``ci.yml``
cannot be called that way as they are: their jobs choose themselves from the
triggering event and its dispatch inputs, most branches' ``daily.yml`` has no
``workflow_call`` trigger, and ``ci.yml`` takes no test arguments. A
normalized copy keeps every job's ``runs-on``, ``container``, ``strategy`` and
steps, and changes only this:

- it is triggered by ``workflow_call`` alone, with explicit inputs, and every
  ``github.event.inputs.*`` reference reads those inputs instead;
- exactly one job runs, the one named by the ``job`` input; the jobs that
  only aggregate others' results (``needs:``) are dropped;
- the main checkout takes ``use_repo``/``use_git_ref``;
- every test-suite step is gated by ``skiptests`` and receives ``test_args``
  (``--single``/``--only``/``--loops``), so only the chosen tests run;
- whether ``--accurate`` is passed is the ``accurate`` input, not the event,
  and steps meant for a dispatched run always run;
- the compatibility steps that test against old Redis servers run only when
  ``compat`` asks for them, and no job runs longer than ``MAX_JOB_MINUTES``.

A job the fix agent can run in (an Ubuntu or macOS runner, or a container on
an Ubuntu runner, but no VM; one ``make`` step and one ``runtest`` command)
also gets a fix mode, chosen by the ``fix`` input, the name of a fix planned
by ``ci-fix-run.yml``: the job's own steps run up to its tests, and instead of
the tests the CI-fix agent prepares that fix in the job's checkout, rebuilding
and testing with the job's own commands (``scripts/ci_fix/job.sh``). In a container the agent's own tools
come from the distribution's packages. A matrix job runs the fix in the one
entry ``fix_matrix`` names.

A copy receives only ``contents: read`` and the OIDC token its fix mode needs
for the model's AWS credentials, and no other secret.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any

import yaml

# A job reproduces or fixes one failure, never a full Daily run.
MAX_JOB_MINUTES = 180
CHECKOUT = "actions/checkout@de0fac2e4500dabe0009e67214ff5f5447ce83dd"  # v6.0.2
DOWNLOAD_ARTIFACT = "actions/download-artifact@018cc2cf5baa6db3ef3c5f8a56943fffe632ef53"  # v6.0.0
UPLOAD_ARTIFACT = "actions/upload-artifact@ea165f8d65b6e75b540449e92b4886f43607fa02"  # v4.6.2
CONFIGURE_AWS = "aws-actions/configure-aws-credentials@61815dcd50bd041e203e49132bacad1fd04d2708"  # v5.1.1
# Where a fix-mode job checks the agent out, and the artifacts it reads and writes.
AGENT_DIR = ".ci-fix-agent"
PLAN_ARTIFACT = "ci-fix-plan"
RESULT_ARTIFACT = "ci-fix-result"
# The model's AWS session; the fix's whole preparation fits in it
# (scripts/ci_fix/engine.py PREPARE_BUDGET_S).
AWS_SESSION_S = 75 * 60

_SUITE_COMMANDS = (
    # (suite as daily.yml's skiptests names it, command that runs it)
    ("modules", re.compile(r"\bruntest-moduleapi\b")),
    ("sentinel", re.compile(r"\bruntest-sentinel\b")),
    ("cluster", re.compile(r"\bruntest-cluster\b")),
    ("unittest", re.compile(r"\bmake test-unit\b|valkey-unit-(?:g)?tests\b")),
    ("valkey", re.compile(r"(?:^|[\s/])runtest(?:\s|$)")),
)
# Suites whose runner takes ``test_args``; sentinel and the legacy cluster
# suite take ``cluster_test_args`` (daily.yml already passes those).
_TAKES_TEST_ARGS = {"valkey", "modules"}
_COMPAT_STEP_RE = re.compile(r"(?i)redis oss|compatib")
_RUNTEST_LINE_RE = re.compile(r"(?:^|[\s/])runtest(?:-moduleapi)?(?:\s|$)")
_AGGREGATOR_JOBS = {"consolidate-test-failures", "notify-about-job-results"}
_INPUT_REF_RE = re.compile(r"\binputs\.([A-Za-z_][\w-]*)")

INPUTS: dict[str, dict[str, Any]] = {
    "job": {"description": "The id of the one job to run", "required": True, "type": "string"},
    "use_repo": {"description": "Repository to check out", "type": "string", "default": "valkey-io/valkey"},
    "use_git_ref": {"description": "Branch or commit to check out", "required": True, "type": "string"},
    "skiptests": {"description": "Test suites to skip (daily.yml's names)", "type": "string", "default": ""},
    "test_args": {"description": "Extra runtest arguments", "type": "string", "default": ""},
    "valgrind_test": {"description": "The one test file valgrind jobs run", "type": "string", "default": ""},
    "cluster_test_args": {"description": "Extra sentinel/cluster arguments", "type": "string", "default": ""},
    "accurate": {"description": "Pass --accurate, as scheduled runs do", "type": "boolean", "default": True},
    "compat": {"description": "Run the old-Redis compatibility steps", "type": "boolean", "default": False},
}
FIX_INPUTS: dict[str, dict[str, Any]] = {
    "fix": {"description": "Prepare the fix ci-fix-run.yml planned under this name, instead of testing",
            "type": "string", "default": ""},
    "fix_matrix": {"description": "The matrix value of the one entry that prepares the fix",
                   "type": "string", "default": ""},
}
# Fix mode assumes an AWS role for the model with the job's OIDC token.
PERMISSIONS = {"contents": "read", "id-token": "write"}
_FIX = "inputs.fix != ''"
_NOT_FIX = "inputs.fix == ''"


@dataclass
class Copy:
    """A normalized workflow, and what the planner needs to know about it."""

    text: str
    # Job id -> the display names its runs take: the name as written (it may
    # hold ``${{ matrix.* }}``), or each matrix entry's own ``name``.
    jobs: dict[str, list[str]] = field(default_factory=dict)
    # Jobs that run one targeted file from ``valgrind_test`` (newer branches'
    # valgrind jobs); the others take their files in ``test_args``.
    targeted: list[str] = field(default_factory=list)
    # Jobs with a fix mode -> the matrix key ``fix_matrix`` selects ("" for
    # none: no matrix, or one ``valgrind_test`` collapses to a single entry).
    fixable: dict[str, str] = field(default_factory=dict)


def normalize(source: str, *, origin: str) -> Copy:
    """The callable copy of workflow ``source``; ``origin`` names where it came from."""
    text = source.replace("github.event.inputs.", "inputs.")
    text = text.replace("github.event_name != 'pull_request'", "inputs.accurate")
    # Setup steps meant for a dispatched run always run in a copy, whatever
    # event the caller had.
    text = text.replace("github.event_name == 'workflow_dispatch' || github.event_name == 'workflow_call'", "true")
    text = text.replace("if: github.event_name == 'workflow_dispatch'\n", "if: true\n")
    workflow = yaml.safe_load(text)
    copy = Copy(text="")
    jobs: dict[str, Any] = {}
    for job_id, job in (workflow.get("jobs") or {}).items():
        if job_id in _AGGREGATOR_JOBS or job.get("needs"):
            continue
        jobs[job_id] = _normalize_job(job_id, job, copy)
        copy.jobs[job_id] = _run_names(job_id, job)
        if "inputs.valgrind_test" in yaml.safe_dump(jobs[job_id]):
            copy.targeted.append(job_id)
    # The source may read inputs no copy is given (``skipjobs``, or a name it
    # never declared): they stay declared, and empty.
    extra = {
        name: {"description": "Read by the source workflow; empty in a copy", "type": "string", "default": ""}
        for name in sorted(set(_INPUT_REF_RE.findall(text)) - set(INPUTS) - set(FIX_INPUTS))
    }
    normalized = {
        "name": f"Valkey copy: {origin}",
        "on": {"workflow_call": {
            "inputs": {**INPUTS, **FIX_INPUTS, **extra},
            "secrets": {"AWS_ROLE_ARN": {"description": "The role fix mode assumes for the model",
                                         "required": False}},
        }},
        "permissions": PERMISSIONS,
        "jobs": jobs,
    }
    header = (
        f"# Generated by scripts/valkey_workflows/sync.py from {origin}.\n"
        "# Do not edit: the weekly sync rewrites it. See scripts/valkey_workflows/normalize.py.\n"
    )
    copy.text = header + _dump(normalized)
    return copy


def _run_names(job_id: str, job: dict[str, Any]) -> list[str]:
    name = str(job.get("name") or job_id)
    if name.replace(" ", "") == "${{matrix.name}}":
        include = ((job.get("strategy") or {}).get("matrix") or {}).get("include") or []
        names = [str(entry["name"]) for entry in include if isinstance(entry, dict) and entry.get("name")]
        if names:
            return names
    return [name]


def _normalize_job(job_id: str, job: dict[str, Any], copy: Copy) -> dict[str, Any]:
    job = dict(job)
    job["if"] = f"inputs.job == '{job_id}'"
    job["timeout-minutes"] = min(int(job.get("timeout-minutes") or MAX_JOB_MINUTES), MAX_JOB_MINUTES)
    job.pop("concurrency", None)
    steps: list[dict[str, Any]] = []
    checked_out = False
    for step in job.get("steps") or []:
        step = dict(step)
        if not checked_out and _is_main_checkout(step):
            step["with"] = {
                **(step.get("with") or {}),
                "repository": "${{ inputs.use_repo }}",
                "ref": "${{ inputs.use_git_ref }}",
            }
            steps.append(step)
            checked_out = True
            continue
        steps.append(_normalize_step(step))
    job["steps"] = steps
    fix = _fix_mode(job) if checked_out else None
    if fix is not None:
        matrix_key, env = fix
        container = bool(job.get("container"))
        # Fix mode runs the job up to its tests; the agent takes over there.
        # A test step that also sets up (installs Tcl, builds the test
        # modules) keeps that setup: the agent's own runs need it just as the
        # job's tests do, so it is hoisted out before the step is gated off.
        first_test = next(i for i, step in enumerate(steps) if _suite(_script(step)[1]))
        gated: list[dict[str, Any]] = []
        for step in steps[first_test:]:
            setup, step = _hoisted_setup(step)
            if setup is not None:
                gated.append(setup)
            gated.append(_gate(step, _NOT_FIX))
        steps = steps[:first_test] + gated
        selects = f" && matrix.{matrix_key} == inputs.fix_matrix" if matrix_key else ""
        if matrix_key:
            # The other matrix entries skip everything, build included.
            steps = [_gate(step, f"{_NOT_FIX} || matrix.{matrix_key} == inputs.fix_matrix") for step in steps]
        job["steps"] = steps + fix_steps(_FIX + selects, env, system_packages=container)
        copy.fixable[job_id] = matrix_key
    return job


def _fix_mode(job: dict[str, Any]) -> tuple[str, dict[str, str]] | None:
    """(matrix key, the fix step's environment) when the agent can run in ``job``, else None.

    The agent needs a runner it can add an account to: an Ubuntu or macOS
    runner, or a container on an Ubuntu runner (as root), but not a VM. It
    needs the job's build and test commands too: one ``make`` step and one
    ``runtest`` command, which the agent's rebuilds and test runs repeat.
    """
    if not str(job.get("runs-on", "")).startswith(("ubuntu", "macos")):
        return None
    matrix_key = _matrix_key(job)
    if matrix_key is None:
        return None
    steps = job.get("steps") or []
    if any("run" in (step.get("with") or {}) for step in steps):  # a VM
        return None
    builds = [step for step in steps if str(step.get("name") or "") == "make" and "run" in step]
    runtest = _suite_command(steps, "valkey", "./runtest")
    if len(builds) != 1 or not runtest:
        return None
    exports = {**(job.get("env") or {}), **(builds[0].get("env") or {})}
    build = "\n".join([
        *(f"export {key}={_quoted(str(value))}" for key, value in exports.items()),
        *(line for line in str(builds[0]["run"]).strip().splitlines() if not line.lstrip().startswith("sudo ")),
    ])
    env = {
        "CI_FIX_BUILD": build,
        "CI_FIX_RUNTEST": runtest,
        "CI_FIX_RUNTEST_MODULEAPI": _suite_command(steps, "modules", "./runtest-moduleapi"),
    }
    if job.get("container"):
        # Without git in the container, the checkout is an archive of this.
        env |= {"CI_FIX_REPOSITORY": "${{ inputs.use_repo }}", "CI_FIX_COMMIT": "${{ inputs.use_git_ref }}"}
    return matrix_key, env


def _matrix_key(job: dict[str, Any]) -> str | None:
    """The key that tells one entry of ``job``'s matrix: "" for no matrix, None if none can.

    It is ``name`` for a job named by its entries' names: ``fix_matrix`` is
    then the failed job's own name.
    """
    matrix = (job.get("strategy") or {}).get("matrix")
    if not matrix:
        return ""
    if "inputs.valgrind_test" in yaml.safe_dump(matrix):
        return ""  # one targeted shard
    include = matrix.get("include") if isinstance(matrix, dict) else None
    if (list(matrix) == ["include"] and str(job.get("name", "")).replace(" ", "") == "${{matrix.name}}"
            and isinstance(include, list) and all(isinstance(entry, dict) and entry.get("name") for entry in include)):
        return "name"
    if isinstance(matrix, dict) and len(matrix) == 1:
        (key, values), = matrix.items()
        if isinstance(values, list) and all(isinstance(value, str) for value in values):
            return str(key)
    return None


_TEST_ARGS_RE = re.compile(r"\$\{\{\s*inputs\.test_args\s*\}\}")
# Options of the job's own test selection and fixtures: the fix runs its own
# tests, and a path to another server the fix mode never downloads.
_DROPPED_OPTION_RE = re.compile(
    r"""\s--(?:failures-output|single|only|other-server-path)\s+(?:"[^"]*"|'[^']*'|\S+)"""
)
_SHELL_VARIABLE_RE = re.compile(r"(?:^|\s)\S*\$(?!\{\{)\S*")


def _suite_command(steps: list[dict[str, Any]], suite: str, command: str) -> str:
    """The job's one ``command`` line for ``suite``, without the per-run test selection, or ""."""
    invocation = re.compile(re.escape(command) + r"(?:\s|$)")
    lines = []
    for step in steps:
        script = _script(step)[1]
        if _suite(script) != suite or "docker run" in script or _COMPAT_STEP_RE.search(str(step.get("name") or "")):
            continue
        joined = re.sub(r"\\\n\s*", " ", script)
        lines += [line.strip() for line in joined.splitlines()
                  if invocation.search(line) and not line.lstrip().startswith("#")]
    if len(lines) != 1:
        return ""
    line = _TEST_ARGS_RE.sub("", lines[0])
    line = _DROPPED_OPTION_RE.sub("", line)
    line = _SHELL_VARIABLE_RE.sub("", line)
    return " ".join(line.split())


def _quoted(value: str) -> str:
    return "'" + value.replace("'", "'\\''") + "'"


def fix_steps(
    when: str,
    env: dict[str, str],
    *,
    key: str = "${{ inputs.fix }}",
    install_dependencies: bool = False,
    system_packages: bool = False,
) -> list[dict[str, Any]]:
    """The steps that hand a job the fix named ``key`` (``scripts/ci_fix/job.sh``), run ``when``.

    ``ci-fix-run.yml`` runs the same steps, with Valkey's build and test
    dependencies installed first, for a failure no copied job can host. In a
    container (``system_packages``), the agent's tools come from the
    distribution's packages.
    """
    agent = f"./{AGENT_DIR}"
    steps: list[dict[str, Any]] = [
        {
            "name": "CI fix: check out the agent",
            "uses": CHECKOUT,
            "with": {"repository": "${{ github.repository }}", "ref": "${{ github.sha }}",
                     "path": AGENT_DIR, "persist-credentials": False},
        },
        *([{
            "name": "Install Valkey build and test dependencies",
            "shell": "bash",
            "run": (
                "sudo apt-get update -q\n"
                "sudo apt-get install -y -q --no-install-recommends \\\n"
                "  build-essential pkg-config libssl-dev tcl8.6 tclx tcl-tls valgrind\n"
            ),
        }] if install_dependencies else []),
        {
            "name": "CI fix: set up the agent",
            "uses": f"{agent}/.github/actions/setup-agent",
            "with": {"path": AGENT_DIR, "install-claude": "true",
                     **({"system-packages": "true"} if system_packages else {})},
        },
        {
            "name": "CI fix: download the plan",
            "uses": DOWNLOAD_ARTIFACT,
            "with": {"name": f"{PLAN_ARTIFACT}-{key}", "path": "${{ runner.temp }}/ci-fix-plan"},
        },
        {
            "name": "CI fix: configure AWS credentials",
            "uses": CONFIGURE_AWS,
            "with": {"role-to-assume": "${{ secrets.AWS_ROLE_ARN }}",
                     "role-session-name": "ci-fix-${{ github.run_id }}",
                     "aws-region": "${{ vars.AWS_REGION || 'us-east-1' }}",
                     "role-duration-seconds": AWS_SESSION_S},
        },
        {
            "name": "CI fix: prepare the fix in this job",
            "shell": "bash",
            "env": {"CI_AGENT_CLAUDE_MODEL": "${{ vars.CI_AGENT_CLAUDE_MODEL }}", **env},
            "run": f"bash {AGENT_DIR}/scripts/ci_fix/job.sh",
        },
        {
            "name": "CI fix: upload the result",
            "if": f"always() && {when}" if when else "always()",
            "uses": UPLOAD_ARTIFACT,
            "with": {"name": f"{RESULT_ARTIFACT}-{key}", "path": "${{ runner.temp }}/ci-fix-out/",
                     "if-no-files-found": "ignore", "retention-days": 30},
        },
    ]
    if when:
        for step in steps[:-1]:
            step["if"] = when
    return steps


# A line of a test step that is the step's own setup: one plain command,
# with no shell state the test invocation beside it may depend on.
_SETUP_LINE_RE = re.compile(
    r"^(?:if|then|else|elif|fi|for|while|until|do|done|case|esac|\{|\})\b|[;&|]\s*$|^\w+="
)


def _hoisted_setup(step: dict[str, Any]) -> tuple[dict[str, Any] | None, dict[str, Any]]:
    """``(setup step, step without it)`` for a test step that also sets up, else ``(None, step)``.

    Only the plain commands before the test invocation move: a line that
    assigns a variable, uses one, or opens a control structure belongs to the
    invocation's own shell and stays with it.
    """
    key, script = _script(step)
    if not script or not _suite(script):
        return None, step
    lines = script.splitlines()
    invocation = next((index for index, line in enumerate(lines) if _suite(line)), None)
    if invocation is None:
        return None, step
    setup = [line for line in lines[:invocation] if _is_setup_line(line)]
    if not setup:
        return None, step
    kept = [line for line in lines if line not in setup]
    name = str(step.get("name") or "").strip()
    return (
        _with_script({**step, "name": f"{name} setup" if name else "setup"}, key, "\n".join(setup)),
        _with_script(step, key, "\n".join(kept)),
    )


def _is_setup_line(line: str) -> bool:
    text = line.strip()
    if not text or text.startswith("#") or text.endswith("\\") or "$" in text:
        return False
    return not _SETUP_LINE_RE.search(text)


def _is_main_checkout(step: dict[str, Any]) -> bool:
    return str(step.get("uses", "")).startswith("actions/checkout@") and not (step.get("with") or {}).get("path")


def _normalize_step(step: dict[str, Any]) -> dict[str, Any]:
    name = str(step.get("name") or "")
    script_key, script = _script(step)
    if _COMPAT_STEP_RE.search(name):
        return _gate(step, "inputs.compat")
    suite = _suite(script) if script else ""
    if not suite:
        return step
    if f"inputs.skiptests, '{suite}'" not in str(step.get("if") or ""):
        step = _gate(step, f"!contains(inputs.skiptests, '{suite}')")
    if suite in _TAKES_TEST_ARGS and "inputs.test_args" not in script:
        step = _with_script(step, script_key, _append_test_args(script))
    return step


def _script(step: dict[str, Any]) -> tuple[str, str]:
    """Which key holds the step's shell script, and the script."""
    if "run" in step:
        return "run", str(step["run"])
    with_ = step.get("with") or {}
    if "run" in with_:  # the FreeBSD VM action
        return "with.run", str(with_["run"])
    return "", ""


def _with_script(step: dict[str, Any], key: str, script: str) -> dict[str, Any]:
    if key == "run":
        return {**step, "run": script}
    return {**step, "with": {**(step.get("with") or {}), "run": script}}


def _suite(script: str) -> str:
    for suite, command in _SUITE_COMMANDS:
        if command.search(script):
            return suite
    return ""


def _gate(step: dict[str, Any], condition: str) -> dict[str, Any]:
    existing = step.get("if")
    if existing not in (None, "") and "||" in condition:
        condition = f"({condition})"  # && binds tighter
    combined = f"({_bare(existing)}) && {condition}" if existing not in (None, "") else condition
    return {**step, "if": combined}


def _bare(condition: Any) -> str:
    if isinstance(condition, bool):
        return "true" if condition else "false"
    text = str(condition).strip()
    if text.startswith("${{") and text.endswith("}}"):
        text = text[3:-2].strip()
    return text


def _append_test_args(script: str) -> str:
    """``script`` with ``${{ inputs.test_args }}`` ending each runtest command."""
    lines = script.split("\n")
    out: list[str] = []
    in_command = False
    for line in lines:
        starts = bool(_RUNTEST_LINE_RE.search(line)) and not line.lstrip().startswith("#")
        if starts:
            in_command = True
        if in_command and not line.rstrip().endswith("\\"):
            line = f"{line.rstrip()} ${{{{ inputs.test_args }}}}"
            in_command = False
        out.append(line)
    return "\n".join(out)


class _Dumper(yaml.SafeDumper):
    pass


def _str(dumper: yaml.SafeDumper, value: str) -> yaml.ScalarNode:
    style = "|" if "\n" in value else None
    return dumper.represent_scalar("tag:yaml.org,2002:str", value, style=style)


_Dumper.add_representer(str, _str)


def _dump(data: dict[str, Any]) -> str:
    return yaml.dump(data, Dumper=_Dumper, sort_keys=False, width=10_000, allow_unicode=True)
