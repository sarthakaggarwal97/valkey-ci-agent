"""Turn one of Valkey's workflow files into a copy that prepares a CI fix.

The CI-fix bot fixes a failure inside the very job that failed: same runner,
container, setup, build flags and test command. Valkey's ``daily.yml`` and
``ci.yml`` cannot be called that way as they are: their jobs choose
themselves from the triggering event and its dispatch inputs, and most
branches' ``daily.yml`` has no ``workflow_call`` trigger. A copy keeps the
jobs the fix agent can run in: an Ubuntu or macOS runner, or a container on
an Ubuntu runner, but no VM; one ``make`` step and one ``runtest`` command.
Each keeps its ``runs-on``, ``container``, ``strategy`` and steps up to its
tests, with the setup a test step does before its test command (installing
Tcl, say), and changes only this:

- it is triggered by ``workflow_call`` alone, with explicit inputs, and every
  ``github.event.inputs.*`` reference reads those inputs instead; steps
  meant for a dispatched run always run;
- exactly one job runs, the one named by the ``job`` input, for no longer
  than ``MAX_JOB_MINUTES``;
- the main checkout takes ``use_repo``/``use_git_ref``;
- instead of the tests, the CI-fix agent prepares the fix ``ci-fix-run.yml``
  planned under the name ``fix``, in the job's checkout, rebuilding and
  testing with the job's own ``make`` and ``runtest`` commands
  (``scripts/ci_fix/job.sh``); whether ``--accurate`` is passed is the
  ``accurate`` input. In a container the agent's own tools come from the
  distribution's packages. A matrix job runs in the one entry ``fix_matrix``
  names.

A copy receives only ``contents: read`` and the OIDC token for the model's
AWS credentials, and no other secret.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any

import yaml

# A job fixes one failure, never runs a full Daily.
MAX_JOB_MINUTES = 180
CHECKOUT = "actions/checkout@de0fac2e4500dabe0009e67214ff5f5447ce83dd"  # v6.0.2
DOWNLOAD_ARTIFACT = "actions/download-artifact@018cc2cf5baa6db3ef3c5f8a56943fffe632ef53"  # v6.0.0
UPLOAD_ARTIFACT = "actions/upload-artifact@ea165f8d65b6e75b540449e92b4886f43607fa02"  # v4.6.2
CONFIGURE_AWS = "aws-actions/configure-aws-credentials@61815dcd50bd041e203e49132bacad1fd04d2708"  # v5.1.1
# Where a job checks the agent out, and the artifacts it reads and writes.
AGENT_DIR = ".ci-fix-agent"
PLAN_ARTIFACT = "ci-fix-plan"
RESULT_ARTIFACT = "ci-fix-result"
# The model's AWS session; the fix's whole preparation fits in it
# (scripts/ci_fix/engine.py PREPARE_BUDGET_S).
AWS_SESSION_S = 75 * 60

_SUITE_COMMANDS = (
    # (Valkey's name for a test suite, the command that runs it)
    ("modules", re.compile(r"\bruntest-moduleapi\b")),
    ("sentinel", re.compile(r"\bruntest-sentinel\b")),
    ("cluster", re.compile(r"\bruntest-cluster\b")),
    ("unittest", re.compile(r"\bmake test-unit\b|valkey-unit-(?:g)?tests\b")),
    ("valkey", re.compile(r"(?:^|[\s/])runtest(?:\s|$)")),
)
_COMPAT_STEP_RE = re.compile(r"(?i)redis oss|compatib")
_INPUT_REF_RE = re.compile(r"\binputs\.([A-Za-z_][\w-]*)")

INPUTS: dict[str, dict[str, Any]] = {
    "job": {"description": "The id of the one job to run", "required": True, "type": "string"},
    "use_repo": {"description": "Repository to check out", "type": "string", "default": "valkey-io/valkey"},
    "use_git_ref": {"description": "Branch or commit to check out", "required": True, "type": "string"},
    "fix": {"description": "The name ci-fix-run.yml planned the fix under", "required": True, "type": "string"},
    "fix_matrix": {"description": "The matrix value of the one entry that runs", "type": "string", "default": ""},
    "valgrind_test": {"description": "The one test file a targeted valgrind job runs", "type": "string",
                      "default": ""},
    "accurate": {"description": "Pass --accurate, as scheduled runs do", "type": "boolean", "default": True},
}
# The model's AWS role is assumed with the job's OIDC token.
PERMISSIONS = {"contents": "read", "id-token": "write"}


@dataclass
class Copy:
    """A normalized workflow, and what the planner needs to know about it."""

    text: str
    # Job id -> the display names its runs take: the name as written (it may
    # hold ``${{ matrix.* }}``), or each matrix entry's own ``name``.
    jobs: dict[str, list[str]] = field(default_factory=dict)
    # Jobs whose valgrind shards ``valgrind_test`` collapses to one.
    targeted: list[str] = field(default_factory=list)
    # Job id -> the matrix key ``fix_matrix`` selects ("" for none: no
    # matrix, or one ``valgrind_test`` collapses to a single entry).
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
        normalized_job = None if job.get("needs") else _normalize_job(job_id, job, copy)
        if normalized_job is None:
            continue  # it aggregates others' results, or the agent cannot run in it
        jobs[job_id] = normalized_job
        copy.jobs[job_id] = _run_names(job_id, job)
        if "inputs.valgrind_test" in yaml.safe_dump(normalized_job):
            copy.targeted.append(job_id)
    # The steps kept may read inputs no copy is given (``skipjobs``, or a
    # name the source never declared): they stay declared, and empty.
    extra = {
        name: {"description": "Read by the source workflow; empty in a copy", "type": "string", "default": ""}
        for name in sorted(set(_INPUT_REF_RE.findall(yaml.safe_dump(jobs))) - set(INPUTS))
    }
    normalized = {
        "name": f"Valkey copy: {origin}",
        "on": {"workflow_call": {
            "inputs": {**INPUTS, **extra},
            "secrets": {"AWS_ROLE_ARN": {"description": "The role assumed for the model", "required": False}},
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


def _normalize_job(job_id: str, job: dict[str, Any], copy: Copy) -> dict[str, Any] | None:
    """The copied job, or None when the agent cannot run in it."""
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
            checked_out = True
        steps.append(step)
    fix = _fix_mode(job | {"steps": steps}) if checked_out else None
    if fix is None:
        return None
    matrix_key, env = fix
    # The job runs up to its tests; the agent takes over there. A test step
    # that also sets up (installs Tcl, builds the test modules) keeps that
    # setup: the agent's own runs need it just as the job's tests do.
    first_test = next(i for i, step in enumerate(steps) if _suite(_script(step)[1]))
    steps = steps[:first_test] + [setup for setup in map(_setup_of, steps[first_test:]) if setup is not None]
    when = ""
    if matrix_key:
        # The other matrix entries skip everything, build included.
        when = f"matrix.{matrix_key} == inputs.fix_matrix"
        steps = [_gate(step, when) for step in steps]
    job["steps"] = steps + fix_steps(when, env, system_packages=bool(job.get("container")))
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
    # Without its comments: the fix runs these lines as one command
    # (scripts/ci_fix/reproduce.py), where a comment would hide the rest.
    build = "\n".join([
        *(f"export {key}={_quoted(str(value))}" for key, value in exports.items()),
        *(stripped for line in str(builds[0]["run"]).strip().splitlines()
          if not line.lstrip().startswith("sudo ") and (stripped := _without_comment(line))),
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


def _without_comment(line: str) -> str:
    """``line`` without a trailing shell comment, or "" when it is only one.

    As the shell reads it: ``#`` starts a comment only at the start of a word,
    never inside quotes or a word (``-DX#Y``, ``${#list[@]}``, ``a\\ #b``), and
    a backslash outside single quotes makes the next character literal.
    """
    quote = ""
    word_start = True
    index = 0
    while index < len(line):
        char = line[index]
        if quote:
            if char == "\\" and quote == '"':
                index += 1
            elif char == quote:
                quote = ""
        elif char == "\\":
            index += 1
            word_start = False
        elif char in " \t":
            word_start = True
        elif char == "#" and word_start:
            return line[:index].rstrip()
        else:
            if char in "\"'":
                quote = char
            # A shell operator ends a word, so a comment may follow it.
            word_start = char in ";&|()<>"
        index += 1
    return line.rstrip()


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
    line = _TEST_ARGS_RE.sub("", _without_comment(lines[0]))
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


# The only commands a test step's setup may hoist: they install something or
# build test fixtures, which a later step's fresh shell sees just the same.
# Anything else (``export``, ``cd``, an assignment, a control structure) is
# the invocation's own shell state and stays with it.
_HOISTABLE_RE = re.compile(
    r"^(?:sudo\s+)?(?:"
    r"apt-get\s+(?:update|install)\b|apt\s+install\b|(?:yum|dnf)\s+(?:-y\s+)?install\b"
    r"|apk\s+add\b|brew\s+install\b|(?:python3?\s+-m\s+)?pip3?\s+install\b"
    r"|make\s+-C\s+\S+"
    r")[^;&|<>`$\\]*$"
)


def _setup_of(step: dict[str, Any]) -> dict[str, Any] | None:
    """The setup a test step does before its test invocation, as a step of its own, or None.

    Only installs and fixture builds before the test invocation count
    (``_HOISTABLE_RE``): any other line is the invocation's own shell state,
    which the fresh shell of a step of its own would not see.
    """
    key, script = _script(step)
    if not script or not _suite(script):
        return None
    lines = script.splitlines()
    invocation = next((index for index, line in enumerate(lines) if _suite(line)), None)
    if invocation is None:
        return None
    setup = [line for line in lines[:invocation] if _HOISTABLE_RE.match(_without_comment(line).strip())]
    if not setup:
        return None
    name = str(step.get("name") or "").strip()
    return _with_script({**step, "name": f"{name} setup" if name else "setup"}, key, "\n".join(setup))


def _is_main_checkout(step: dict[str, Any]) -> bool:
    return str(step.get("uses", "")).startswith("actions/checkout@") and not (step.get("with") or {}).get("path")


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
    combined = f"({_bare(existing)}) && {condition}" if existing not in (None, "") else condition
    return {**step, "if": combined}


def _bare(condition: Any) -> str:
    if isinstance(condition, bool):
        return "true" if condition else "false"
    text = str(condition).strip()
    if text.startswith("${{") and text.endswith("}}"):
        text = text[3:-2].strip()
    return text


class _Dumper(yaml.SafeDumper):
    pass


def _str(dumper: yaml.SafeDumper, value: str) -> yaml.ScalarNode:
    style = "|" if "\n" in value else None
    return dumper.represent_scalar("tag:yaml.org,2002:str", value, style=style)


_Dumper.add_representer(str, _str)


def _dump(data: dict[str, Any]) -> str:
    return yaml.dump(data, Dumper=_Dumper, sort_keys=False, width=10_000, allow_unicode=True)
