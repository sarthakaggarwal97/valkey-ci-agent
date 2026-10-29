"""Resolve and safely replay the failed CI job around one focused fix.

:func:`resolve` reads the job from the workflow file at the failed commit.
The matrix entry is selected by its displayed job name, and the expressions
needed by the job are evaluated in the failed run's context. The resulting
:class:`JobSpec` records the runner, checkouts, setup up to the tests, and the
job's own build and test commands.

Only Ubuntu and macOS host jobs covered by the code-owned dependency profile
run on their original runner. Trusted workflow code installs a fixed package
set; :func:`setup_script` removes only those canonical install commands,
rewrites the pinned libbacktrace install to an unprivileged local prefix, and
leaves the remaining setup for a disposable account. Containers, VMs,
unknown privilege, secrets, unsupported actions or expressions, and jobs
without a focused Valkey test command fall back to the bot's own runner.
"""

from __future__ import annotations

import json
import math
import re
import shlex
from dataclasses import astuple, dataclass, field
from typing import Any, Callable, Optional

import yaml

from scripts.ci_fix.paths import checkout_subdirectory

# A job fixes one failure, never runs a full Daily; and however short the
# job's own tests, the fix needs its setup and build plus the engine's budget
# (scripts/ci_fix/engine.py PREPARE_BUDGET_S).
MAX_JOB_MINUTES = 180
# The engine owns 60 minutes after credentials are configured. Exact jobs
# additionally need bounded dependency bootstrap and target setup before that,
# plus checkout, preflight, cleanup, and artifact-upload headroom.
MIN_JOB_MINUTES = 120
_DEFAULT_JOB_MINUTES = 360  # GitHub's
_HOST_RUNNERS = frozenset({
    "ubuntu-latest",
    "ubuntu-22.04",
    "ubuntu-24.04",
    "ubuntu-24.04-arm",
    "ubuntu-26.04",
    "ubuntu-26.04-arm",
    "macos-latest",
    "macos-13",
    "macos-14",
    "macos-15",
    "macos-15-intel",
    "macos-26",
    "macos-26-intel",
})


class Unsupported(ValueError):
    """The workflow does something a fix job cannot repeat exactly."""


@dataclass(frozen=True)
class JobCommands:
    """The build and test commands of the CI job the fix is prepared in.

    Its ``make`` step, its ``runtest`` and ``runtest-moduleapi`` lines
    without the job's own test selection, and its ``make test-unit`` line.
    """

    build: str
    runtest: str
    moduleapi: str = ""
    # Its ``make test-unit`` line, with the job's own test arguments
    # (``accurate=1``) and make variables; "" when it runs none.
    unittest: str = ""
    # A dependency checkout the workflow placed inside the main checkout.
    # This code-owned path may remain untracked after setup; arbitrary
    # setup-created untracked paths are not silently reclassified as build
    # output by the engine.
    preserved_paths: tuple[str, ...] = ()
    # Workflow/job environment plus the selected test step's environment.
    # Build and focused test run in separate shells, so each suite carries
    # the environment GitHub gave that step.
    runtest_env: dict[str, str] = field(default_factory=dict)
    moduleapi_env: dict[str, str] = field(default_factory=dict)
    unittest_env: dict[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class Step:
    """One of the job's setup steps: a script, the shell that runs it, and its environment."""

    name: str
    run: str
    shell: str = ""  # "" (GitHub's default), "bash" or "sh"
    env: dict[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class Checkout:
    """A repository the job checks out next to its own (``libbacktrace``)."""

    repository: str
    ref: str
    path: str


@dataclass(frozen=True)
class JobSpec:
    """One instance of a job, resolved: what a fix job needs to repeat it."""

    job_id: str
    name: str
    runs_on: str
    container: str  # an image, or "" for none
    timeout_minutes: int
    env: dict[str, str]
    steps: tuple[Step, ...]
    checkout: Optional[Checkout]
    commands: JobCommands
    # The resolved failed job's identity, not the outer ci-agent workflow's.
    # Kept separate from target-controlled ``env`` so setup admission can
    # reject attempts to override these values.
    setup_env: dict[str, str] = field(default_factory=dict)


def workflow_source(gh: Any, repository: str, workflow: str, ref: str) -> str:
    """The text of ``repository``'s ``.github/workflows/<workflow>.yml`` at ``ref``, read through the API."""
    contents = gh.get_repo(repository).get_contents(f".github/workflows/{workflow}.yml", ref=ref)
    return str(contents.decoded_content.decode("utf-8"))


def resolve(source: str, job_name: str, *, scheduled: bool, repository: str, sha: str, ref: str) -> JobSpec:
    """The job instance of workflow ``source`` that ran as ``job_name``.

    The run was a scheduled one or a pull request's, in ``repository`` at
    ``sha`` and ``ref``: that is the ``github`` context its expressions read.
    Dispatch inputs are empty, as in both runs. Any other ``github`` property
    is unknown, and reading it is :class:`Unsupported`.
    """

    def github(job_id: str) -> dict[str, Any]:
        return _Known({
            "event_name": "schedule" if scheduled else "pull_request",
            "repository": repository,
            "ref": ref,
            "sha": sha,
            "job": job_id,
            "event": _Known({"inputs": {}}),
        })

    try:
        workflow = yaml.safe_load(source)
    except yaml.YAMLError as exc:
        raise Unsupported(f"the workflow is not YAML: {exc}") from None
    if not isinstance(workflow, dict) or not isinstance(workflow.get("jobs"), dict):
        raise Unsupported("the workflow has no jobs")
    if workflow.get("defaults"):
        raise Unsupported("the workflow sets step defaults")
    found = []
    for job_id, job in workflow["jobs"].items():
        if not isinstance(job, dict):
            continue
        context = {"github": github(str(job_id)), "inputs": {}}
        try:
            entries = _matrix_entries(job, context)
        except Unsupported:
            continue  # its runs cannot be named, so none is the failed one
        for entry in entries:
            try:
                name = _display_name(str(job_id), job, entry, context)
            except Unsupported:
                continue
            if name == job_name:
                found.append((str(job_id), job, entry))
    if len(found) != 1:
        raise Unsupported(f"{len(found)} jobs run as {job_name!r}")
    job_id, job, entry = found[0]
    context = {"github": github(job_id), "inputs": {}, "matrix": entry}
    workflow_env = _env(workflow.get("env"), context)
    return _spec(job_id, job_name, job, {**context, "env": workflow_env})


# --- the matrix, and the job's display name -----------------------------------


def _matrix_entries(job: dict[str, Any], context: dict[str, Any]) -> list[dict[str, Any]]:
    """Every matrix entry of ``job``: one ``{}`` without a matrix."""
    strategy = job.get("strategy")
    if strategy is not None and not isinstance(strategy, dict):
        raise Unsupported("the strategy is not a mapping")
    matrix = (strategy or {}).get("matrix")
    if matrix is None:
        return [{}]
    if not isinstance(matrix, dict):
        raise Unsupported("the matrix is an expression")
    if list(matrix) == ["include"]:
        include = matrix["include"]
        if not isinstance(include, list) or not all(isinstance(entry, dict) for entry in include):
            raise Unsupported("the matrix's include is not a list of entries")
        return [dict(entry) for entry in include]
    if len(matrix) == 1 and not {"include", "exclude"} & set(matrix):
        ((key, values),) = matrix.items()
        values = value(values, context)
        if not isinstance(values, list):
            raise Unsupported(f"matrix.{key} is not a list")
        return [{key: item} for item in values]
    raise Unsupported("the matrix has several keys, or include and exclude")


_MATRIX_REF_RE = re.compile(r"\$\{\{[^}]*\bmatrix\b")


def _display_name(job_id: str, job: dict[str, Any], entry: dict[str, Any], context: dict[str, Any]) -> str:
    """The name GitHub shows for ``entry`` of ``job``.

    A name that uses the matrix is the name; otherwise GitHub appends the
    entry's values to it (or to the job's id): ``test-sanitizer-address (gcc)``.
    """
    written = job.get("name")
    name = render(str(written), {**context, "matrix": entry}) if written is not None else job_id
    if entry and not (written is not None and _MATRIX_REF_RE.search(str(written))):
        if not all(item is None or isinstance(item, (str, int, float, bool)) for item in entry.values()):
            raise Unsupported("a matrix value is not a scalar")
        name += " (" + ", ".join(_string(item) for item in entry.values()) + ")"
    return name


# --- the job itself -----------------------------------------------------------

# A job's keys that change nothing a fix job repeats, or that the fix job
# sets itself (``if``: the job ran; ``permissions``: the fix job has its own).
_JOB_KEYS = {
    "name", "runs-on", "container", "timeout-minutes", "strategy", "env", "steps",
    "if", "needs", "permissions", "concurrency", "outputs", "continue-on-error",
}
_RUN_STEP_KEYS = {"name", "id", "if", "run", "shell", "env"}
_CHECKOUT_KEYS = {"name", "id", "if", "uses", "with"}
_SHELLS = ("", "bash", "sh")


def _spec(job_id: str, name: str, job: dict[str, Any], context: dict[str, Any]) -> JobSpec:
    """``job``'s instance named ``name``; ``context`` has its matrix entry and the workflow's env."""
    unknown = set(job) - _JOB_KEYS
    if unknown:
        raise Unsupported(f"the job sets {', '.join(sorted(unknown))}")
    runs_on = value(job.get("runs-on"), context)
    if not isinstance(runs_on, str) or runs_on not in _HOST_RUNNERS:
        raise Unsupported(
            f"the job runs on {runs_on!r}, not a supported GitHub-hosted runner"
        )
    container = _container(job.get("container"), context)
    if container and not runs_on.startswith("ubuntu"):
        raise Unsupported("the job's container is not on an Ubuntu runner")
    minutes = value(job.get("timeout-minutes", _DEFAULT_JOB_MINUTES), context)
    if not isinstance(minutes, (int, float)) or isinstance(minutes, bool) or minutes <= 0:
        raise Unsupported(f"the job's timeout is {minutes!r}")
    env = {**context["env"], **_env(job.get("env"), context)}
    context = {**context, "env": env}

    steps = job.get("steps")
    if not isinstance(steps, list) or not all(isinstance(step, dict) for step in steps):
        raise Unsupported("the job has no steps")
    if any("run" in (step.get("with") or {}) for step in steps):
        raise Unsupported("the job runs its tests in a VM")
    first_test = next((index for index, step in enumerate(steps) if _suite(_script(step))), None)
    if first_test is None:
        raise Unsupported("the job runs no tests")
    builds = [step for step in steps if str(step.get("name") or "") == "make" and "run" in step]
    if len(builds) != 1:
        raise Unsupported(f"the job has {len(builds)} make steps")

    build_step = builds[0]
    build_index = steps.index(build_step)
    setup: list[Step] = []
    checkouts: list[Checkout] = []
    main_checkouts = 0
    build_runs = False
    for index, step in enumerate(steps[:first_test]):
        if not condition(step.get("if", True), context):
            continue
        if index == build_index:
            build_runs = True
            continue
        if str(step.get("uses", "")).startswith("actions/checkout@"):
            checkout = _checkout(step, context)
            if index > build_index and not container:
                raise Unsupported(
                    f"the checkout step {_label(step)!r} runs after the job's make step "
                    "and cannot safely run before it"
                )
            if checkout is None:
                main_checkouts += 1  # the fix job checks the commit out itself
            else:
                checkouts.append(checkout)
        elif "run" in step:
            setup_step = _step(step, context)
            if (
                index > build_index
                and not _can_move_setup_before_build(
                    setup_step,
                    runs_on=runs_on,
                    container=container,
                )
            ):
                raise Unsupported(
                    f"the setup step {setup_step.name!r} runs after the job's make step "
                    "and cannot safely run before it"
                )
            setup.append(setup_step)
        else:
            raise Unsupported(f"the setup step {_label(step)!r} is neither a script nor a checkout")
    if main_checkouts != 1 or len(checkouts) > 1:
        raise Unsupported(f"the job has {main_checkouts} main and {len(checkouts)} other checkouts")
    if build_index >= first_test or not build_runs:
        raise Unsupported("the job's make step does not run before its tests")
    # A test step that also sets up identifies that setup separately. Only a
    # canonical package action can move ahead of the reproduction build;
    # fixture builds or unknown commands fail closed below. Any other script
    # between the tests is setup the fix would not replay, unless it is for
    # compatibility tests, which the fix never runs. What follows the last
    # test (a memory summary, an upload) is not.
    last_test = max(index for index, step in enumerate(steps) if _suite(_script(step)))
    for step in steps[first_test:last_test + 1]:
        if "run" not in step or not condition(step.get("if", True), context):
            continue
        hoisted = _setup_of(step)
        if hoisted is not None:
            setup_step = _step(hoisted, context)
            if not _can_move_setup_before_build(
                setup_step,
                runs_on=runs_on,
                container=container,
            ):
                raise Unsupported(
                    f"the setup step {setup_step.name!r} runs after the job's make step "
                    "and cannot safely run before it"
                )
            setup.append(setup_step)
        elif not _suite(_script(step)) and not _COMPAT_STEP_RE.search(_label(step)):
            raise Unsupported(f"the step {_label(step)!r} runs between the job's tests")

    checkout = checkouts[0] if checkouts else None
    dependency_env: dict[str, str] = {}
    if checkout is not None and _uses_isolated_libbacktrace(checkout, tuple(setup)):
        prefix = f"{checkout.path}/.ci-fix-install"
        scopes = (
            env,
            *(step.env for step in (*setup, _step(build_step, context))),
            *(_env(step.get("env"), context) for step in steps if _suite(_script(step))),
        )
        if any(
            "LIBBACKTRACE_PREFIX" in scope
            and scope["LIBBACKTRACE_PREFIX"] != prefix
            for scope in scopes
        ):
            raise Unsupported("the job overrides the isolated libbacktrace prefix")
        if any(
            re.search(r"\bLIBBACKTRACE_PREFIX\s*=", _without_comment(line))
            for step in (
                *setup,
                _step(build_step, context),
                *(_step(step, context) for step in steps if _suite(_script(step))),
            )
            for line in step.run.splitlines()
        ):
            raise Unsupported("the job script overrides the isolated libbacktrace prefix")
        dependency_env["LIBBACKTRACE_PREFIX"] = prefix

    tests = [
        (
            str(step.get("name") or ""),
            _rendered_run(step, context),
            _env(step.get("env"), context),
        )
        for step in steps
        if _suite(_script(step))
    ]
    runtest = _suite_command(tests, "valkey", "./runtest")
    if not runtest:
        raise Unsupported("the job has no single runtest command")
    make = _step(build_step, context)
    if reason := _unsupported_build_setup(
        make,
        runs_on=runs_on,
        container=container,
    ):
        raise Unsupported(reason)
    # The fix job reads these as workflow outputs, one line each.
    if any("\n" in text for text in (name, runs_on, container, *(astuple(checkout) if checkout else ()))):
        raise Unsupported("the job's name, runner, container or checkout spans lines")
    return JobSpec(
        job_id=job_id,
        name=name,
        runs_on=runs_on,
        container=container,
        timeout_minutes=min(max(int(minutes), MIN_JOB_MINUTES), MAX_JOB_MINUTES),
        env=env,
        steps=tuple(setup),
        checkout=checkout,
        commands=JobCommands(
            build=_build_command({**env, **make.env, **dependency_env}, make.run),
            runtest=runtest,
            moduleapi=_suite_command(tests, "modules", "./runtest-moduleapi"),
            unittest=_suite_command(tests, "unittest", "make test-unit"),
            preserved_paths=(checkout.path,) if checkout else (),
            runtest_env=_suite_env(tests, "valkey", {**env, **dependency_env}),
            moduleapi_env=_suite_env(tests, "modules", {**env, **dependency_env}),
            unittest_env=_suite_env(tests, "unittest", {**env, **dependency_env}),
        ),
        setup_env={
            "GITHUB_EVENT_NAME": _string(context["github"]["event_name"]),
            "GITHUB_JOB": _string(context["github"]["job"]),
            "GITHUB_REF": _string(context["github"]["ref"]),
            "GITHUB_REPOSITORY": _string(context["github"]["repository"]),
            "GITHUB_SHA": _string(context["github"]["sha"]),
        },
    )


def _container(container: Any, context: dict[str, Any]) -> str:
    if isinstance(container, dict):
        if set(container) != {"image"}:
            raise Unsupported("the job's container sets more than its image")
        container = container["image"]
    image = value(container, context) if container is not None else ""
    if image is None:
        return ""
    if not isinstance(image, str):
        raise Unsupported(f"the job's container is {image!r}")
    return image


def _env(env: Any, context: dict[str, Any]) -> dict[str, str]:
    if env is None:
        return {}
    if not isinstance(env, dict):
        raise Unsupported("an env is an expression")
    rendered = {}
    for name, item in env.items():
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", str(name)):
            raise Unsupported(f"{name!r} is not a variable name")
        rendered[str(name)] = _string(value(item, context))
    return rendered


def _checkout(step: dict[str, Any], context: dict[str, Any]) -> Checkout | None:
    """The other repository a checkout step adds, or None for the job's own."""
    unknown = set(step) - _CHECKOUT_KEYS
    with_ = step.get("with") or {}
    if unknown or set(with_) - {"repository", "ref", "path"}:
        raise Unsupported(f"the checkout {_label(step)!r} sets {', '.join(sorted(unknown | set(with_)))}")
    if not with_.get("path"):
        return None
    path = _checkout_path(_string(value(with_["path"], context)))
    repository = _string(value(with_.get("repository", "${{ github.repository }}"), context))
    ref = _string(value(with_.get("ref", ""), context))
    if not re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", repository):
        raise Unsupported(f"the extra checkout repository {repository!r} is not an owner/repository name")
    if not re.fullmatch(r"[0-9A-Fa-f]{40}", ref):
        raise Unsupported("the extra checkout is not pinned to a full commit SHA")
    return Checkout(
        repository=repository,
        ref=ref,
        path=path,
    )


def _checkout_path(value: str) -> str:
    """A canonical descendant that cannot collide with CI-fix control data."""
    try:
        checkout_subdirectory(value)
    except ValueError:
        raise Unsupported(f"the checkout path {value!r} is not a safe repository subdirectory")
    return value


def _step(step: dict[str, Any], context: dict[str, Any]) -> Step:
    unknown = set(step) - _RUN_STEP_KEYS
    if unknown:
        raise Unsupported(f"the step {_label(step)!r} sets {', '.join(sorted(unknown))}")
    shell = str(step.get("shell") or "")
    if shell not in _SHELLS:
        raise Unsupported(f"the step {_label(step)!r} runs in {shell}")
    return Step(name=_label(step), run=_rendered_run(step, context), shell=shell, env=_env(step.get("env"), context))


def _rendered_run(step: dict[str, Any], context: dict[str, Any]) -> str:
    """``step``'s script, rendered in its own environment."""
    env = _env(step.get("env"), context)
    return render(_script(step), {**context, "env": {**context["env"], **env}})


def _label(step: dict[str, Any]) -> str:
    return str(step.get("name") or step.get("uses") or _script(step).strip().split("\n")[0])


# --- the job's build and test commands ----------------------------------------

_SUITE_COMMANDS = (
    # (Valkey's name for a test suite, the command that runs it)
    ("modules", re.compile(r"\bruntest-moduleapi\b")),
    ("sentinel", re.compile(r"\bruntest-sentinel\b")),
    ("cluster", re.compile(r"\bruntest-cluster\b")),
    # Google Test, the older C harness, and 7.2's ``valkey-server test``.
    ("unittest", re.compile(r"\bmake test-unit\b|valkey-unit-(?:g)?tests\b|valkey-server test\b")),
    ("valkey", re.compile(r"(?:^|[\s/])runtest(?:\s|$)")),
)
_COMPAT_STEP_RE = re.compile(r"(?i)redis oss|compatib")
# A build line that changes the system: admitted lines are supplied by trusted
# bootstrap and removed from the extracted unprivileged build.
_SYSTEM_SETUP_RE = re.compile(r"(?:sudo|apt-get|apt|dnf|yum|apk|zypper|update-alternatives)\s")
# Host setup runs without the workflow account, OIDC variables, or sudo. The
# trusted workflow installs this fixed Ubuntu superset before replay. Only an
# exact install whose packages are a subset may be omitted from target setup.
_BOOTSTRAP_APT_PACKAGES = frozenset({
    "build-essential",
    "pkg-config",
    "libssl-dev",
    "libzstd-dev",
    "libgtest-dev",
    "libgmock-dev",
    "tcl8.6",
    "tclx",
    "tcl-tls",
    "valgrind",
})
_APT_OPTIONS = frozenset({"-y", "-q", "--no-install-recommends"})
_LIBBACKTRACE_REPOSITORY = "ianlancetaylor/libbacktrace"
_PRIVILEGED_SETUP_RE = re.compile(
    r"""(?ix)
    (?:^|[;&|()]\s*)
    (?:
        sudo|doas|pkexec|su
      | apt(?:-get)?|dnf|yum|apk|zypper|update-alternatives
      | brew\s+(?:install|upgrade|services)
      | (?:pip|pip3|python(?:3(?:\.\d+)?)?\s+-m\s+pip)\s+install
      | npm\s+install\s+(?:--global|-g)
      | systemctl|service|mount|umount|chown
      | docker|podman|nsenter
    )\b
    """
)
_PERSISTENT_ENV_RE = re.compile(r"\bGITHUB_(?:ENV|PATH)\b")
_PROTECTED_SETUP_ENV = frozenset({"HOME", "TMP", "TMPDIR", "USER", "LOGNAME"})
# Options of the job's own test selection and fixtures: the fix runs its own
# tests, and a path to another server the fix job never downloads.
_DROPPED_OPTION_RE = re.compile(
    r"""\s--(?:failures-output|single|only|other-server-path)\s+(?:"[^"]*"|'[^']*'|\S+)"""
)
# The runtest options that take a value: a value the shell would expand is
# dropped together with its option, or the option would take the next one.
_VALUE_OPTION_RE = re.compile(
    r"--(?:host|port|baseport|portcount|clients|timeout|single|only|skiptest|skipunit|skipfile"
    r"|config|loops|tags|io-threads|other-server-path|failures-output)"
)


def _build_command(exports: dict[str, str], script: str) -> str:
    """The ``make`` step as one command, with the environment it ran in.

    Without its comments: the fix runs these lines as one command
    (scripts/ci_fix/reproduce.py), where a comment would hide the rest.
    Without admitted system setup that trusted bootstrap already supplied:
    the fix rebuilds as an unprivileged account. A line continued with a
    backslash is one line, so that a dropped install does not leave its
    packages as a command.
    """
    joined = re.sub(r"\s*\\\n\s*", " ", script.strip())
    return "\n".join([
        *(f"export {key}={_quoted(item)}" for key, item in exports.items()),
        *(stripped for line in joined.splitlines()
          if not _SYSTEM_SETUP_RE.match(line.lstrip()) and (stripped := _without_comment(line))),
    ])


def _suite_command(tests: list[tuple[str, str, dict[str, str]]], suite: str, command: str) -> str:
    """The job's one ``command`` line for ``suite``, without the per-run test selection, or "".

    ``tests`` are the job's test steps, as (name, script, environment).
    """
    invocation = re.compile(re.escape(command) + r"(?:\s|$)")
    lines = []
    for name, script, _env_ in tests:
        if _suite(script) != suite or "docker run" in script or _COMPAT_STEP_RE.search(name):
            continue
        joined = re.sub(r"\\\n\s*", " ", script)
        lines += [line.strip() for line in joined.splitlines()
                  if invocation.search(line) and not line.lstrip().startswith("#")]
    if len(lines) != 1:
        return ""
    words: list[str] = []
    for word in _DROPPED_OPTION_RE.sub("", _without_comment(lines[0])).split():
        if "$" not in word:
            words.append(word)
        elif words and _VALUE_OPTION_RE.fullmatch(words[-1]):
            words.pop()
    return " ".join(words)


def _suite_env(
    tests: list[tuple[str, str, dict[str, str]]],
    suite: str,
    base: dict[str, str],
) -> dict[str, str]:
    """The environment of the one selected step for ``suite``."""
    matches = [
        step_env
        for name, script, step_env in tests
        if _suite(script) == suite
        and "docker run" not in script
        and not _COMPAT_STEP_RE.search(name)
    ]
    return {**base, **matches[0]} if len(matches) == 1 else {}


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


def _bootstrap_apt_line(line: str) -> bool:
    """Whether every ``&&`` segment is a fixed, already-satisfied apt action."""
    segments = [segment.strip() for segment in line.split("&&")]
    if not segments or any(not segment for segment in segments):
        return False
    for segment in segments:
        try:
            words = shlex.split(segment, posix=True)
        except ValueError:
            return False
        if words[:1] != ["sudo"]:
            return False
        words = words[1:]
        if words[:1] == ["-n"]:
            words = words[1:]
        if not words or words[0] not in {"apt", "apt-get"}:
            return False
        command = words[1:]
        if not command:
            return False
        if command[0] == "update":
            if any(word not in _APT_OPTIONS for word in command[1:]):
                return False
            continue
        if command[0] != "install":
            return False
        packages = [word for word in command[1:] if word not in _APT_OPTIONS]
        if not packages or any(word.startswith("-") for word in packages):
            return False
        if not set(packages) <= _BOOTSTRAP_APT_PACKAGES:
            return False
    return True


def _script_lines(script: str) -> tuple[str, ...]:
    """The substantive commands in ``script``, with continuations joined."""
    joined = re.sub(r"\s*\\\n\s*", " ", script)
    return tuple(
        stripped
        for line in joined.splitlines()
        if (stripped := _without_comment(line).strip())
    )


def _can_move_setup_before_build(
    step: Step,
    *,
    runs_on: str,
    container: str,
) -> bool:
    """Whether a post-build setup step is known not to consume build output.

    Exact host setup is replayed before the code-owned reproduction build.
    The only post-build workflow commands that can move there are canonical
    Ubuntu package installs: trusted bootstrap already performed them, so
    setup replay omits them entirely. An unknown command may read or execute
    build output, and therefore makes the exact-job path fall back.

    Container jobs already fall back before setup replay, so preserving their
    resolved metadata here cannot make an unknown step executable.
    """
    if container:
        return True
    lines = _script_lines(step.run)
    return not lines or (
        runs_on.startswith("ubuntu")
        and all(_bootstrap_apt_line(line) for line in lines)
    )


def _unsupported_build_setup(
    step: Step,
    *,
    runs_on: str,
    container: str,
) -> str:
    """Why privileged setup embedded in the extracted build is unsupported."""
    if container:
        return ""
    for line in _script_lines(step.run):
        if not (
            _SYSTEM_SETUP_RE.search(line)
            or _PRIVILEGED_SETUP_RE.search(line)
        ):
            continue
        if runs_on.startswith("ubuntu") and _bootstrap_apt_line(line):
            continue
        return (
            f"the job's make step {step.name!r} requires setup outside "
            "the code-owned dependency profile"
        )
    return ""


def _isolated_libbacktrace_line(line: str, checkout: Checkout | None) -> str | None:
    """Rewrite the one pinned dependency install into its user-owned prefix."""
    if (
        checkout is None
        or checkout.repository != _LIBBACKTRACE_REPOSITORY
        or not re.fullmatch(r"[0-9A-Fa-f]{40}", checkout.ref)
    ):
        return None
    parts = [part.strip() for part in line.split("&&")]
    if len(parts) != 4:
        return None
    try:
        words = [shlex.split(part, posix=True) for part in parts]
    except ValueError:
        return None
    if (
        words[0] != ["cd", checkout.path]
        or words[1] != ["./configure"]
        or words[2] != ["make"]
        or words[3] not in (["sudo", "make", "install"], ["sudo", "-n", "make", "install"])
    ):
        return None
    return (
        f"cd {_quoted(checkout.path)} && "
        './configure --prefix="$(pwd)/.ci-fix-install" && make && make install'
    )


def _uses_isolated_libbacktrace(checkout: Checkout | None, steps: tuple[Step, ...]) -> bool:
    return any(
        _isolated_libbacktrace_line(_without_comment(line).strip(), checkout) is not None
        for step in steps
        for line in step.run.splitlines()
    )


def _isolated_setup_steps(spec: JobSpec) -> tuple[Step, ...]:
    """Sanitize setup for the disposable account, or reject unsupported privilege."""
    if spec.container:
        raise Unsupported(
            "container setup is not yet covered by a code-owned dependency profile"
        )
    if spec.checkout is not None and not _uses_isolated_libbacktrace(spec.checkout, spec.steps):
        raise Unsupported("the extra checkout is not covered by a code-owned dependency profile")
    protected = [
        name
        for env in (spec.env, *(step.env for step in spec.steps))
        for name in env
        if (
            name in _PROTECTED_SETUP_ENV
            or name.startswith("GITHUB_")
            or name.startswith("RUNNER_")
        )
    ]
    if protected:
        raise Unsupported(
            "the setup overrides protected runner environment "
            + ", ".join(sorted(set(protected)))
        )
    sanitized: list[Step] = []
    for step in spec.steps:
        lines: list[str] = []
        for line in step.run.splitlines():
            stripped = _without_comment(line).strip()
            if _PERSISTENT_ENV_RE.search(stripped):
                raise Unsupported(
                    f"the setup step {step.name!r} changes cross-step environment"
                )
            if spec.runs_on.startswith("ubuntu") and stripped and _bootstrap_apt_line(stripped):
                continue
            replacement = _isolated_libbacktrace_line(stripped, spec.checkout)
            lines.append(replacement if replacement is not None else line)
        run = "\n".join(lines).strip()
        if not run:
            continue
        if any(
            _PRIVILEGED_SETUP_RE.search(stripped)
            for line in run.splitlines()
            if (stripped := _without_comment(line).strip())
        ):
            raise Unsupported(f"the setup step {step.name!r} requires unsupported privileges")
        sanitized.append(Step(step.name, run, step.shell, step.env))
    return tuple(sanitized)


def isolated_setup_reason(spec: JobSpec) -> str:
    """Why ``spec`` cannot run under the code-owned setup profile."""
    try:
        _isolated_setup_steps(spec)
    except Unsupported as exc:
        return str(exc)
    return ""


# The only commands identifiable as setup before a test invocation. Resolution
# separately rejects a fixture build because exact-job setup runs before the
# main build; canonical package installs are the only identified commands it
# can safely move there. Anything else (``export``, ``cd``, an assignment, a
# control structure) is the invocation's own shell state and stays with it.
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
    lines = _script(step).splitlines()
    invocation = next((index for index, line in enumerate(lines) if _suite(line)), None)
    if invocation is None:
        return None
    setup = [line for line in lines[:invocation] if _HOISTABLE_RE.match(_without_comment(line).strip())]
    if not setup:
        return None
    name = str(step.get("name") or "").strip()
    hoisted = {key: item for key, item in step.items() if key != "if"}
    return {**hoisted, "name": f"{name} setup" if name else "setup", "run": "\n".join(setup)}


def _script(step: dict[str, Any]) -> str:
    return str(step.get("run") or "")


def _suite(script: str) -> str:
    for suite, command in _SUITE_COMMANDS:
        if command.search(script):
            return suite
    return ""


def _quoted(text: str) -> str:
    return "'" + text.replace("'", "'\\''") + "'"


# --- the setup driver ---------------------------------------------------------

# Runs one step as GitHub does: its own script file and shell, from the
# workspace, with its own environment and fresh GITHUB_ENV and GITHUB_PATH
# files, which the later steps of this script see. Never the workflow's own
# GITHUB_ENV and GITHUB_PATH: the job's build runs as the workflow user, and
# a path it prepended would be what the workflow's later steps, which hold
# the model's credentials, resolve their shell from.
_DRIVER = r"""#!/bin/sh
# @TITLE@
# Generated by scripts/ci_fix/ci_job.py: each step runs as GitHub runs it.
# The driver's own variables start with ci_fix_, apart from the job's.
set -u
if command -v bash >/dev/null 2>&1; then ci_fix_default="bash -e"; else ci_fix_default="sh -e"; fi
ci_fix_steps="$(mktemp -d)"

# ci_fix_load_env FILE: export each NAME=value line and NAME<<DELIMITER block of FILE.
ci_fix_load_env() {
  ci_fix_delimiter=""
  while IFS= read -r ci_fix_line || [ -n "${ci_fix_line}" ]; do
    if [ -n "${ci_fix_delimiter}" ]; then
      if [ "${ci_fix_line}" = "${ci_fix_delimiter}" ]; then
        ci_fix_export "${ci_fix_key}" "${ci_fix_value}"
        ci_fix_delimiter=""
      elif [ -z "${ci_fix_started}" ]; then
        ci_fix_value="${ci_fix_line}"
        ci_fix_started=1
      else
        ci_fix_value="${ci_fix_value}
${ci_fix_line}"
      fi
      continue
    fi
    ci_fix_key="${ci_fix_line%%=*}"
    ci_fix_heredoc="${ci_fix_line%%<<*}"
    if [ "${ci_fix_key}" != "${ci_fix_line}" ] && [ "${#ci_fix_key}" -lt "${#ci_fix_heredoc}" ]; then
      ci_fix_export "${ci_fix_key}" "${ci_fix_line#*=}"
    elif [ "${ci_fix_heredoc}" != "${ci_fix_line}" ]; then
      ci_fix_key="${ci_fix_heredoc}"
      ci_fix_delimiter="${ci_fix_line#*<<}"
      ci_fix_value=""
      ci_fix_started=""
    fi
  done < "$1"
}

ci_fix_export() {
  case "$1" in
    "" | [0-9]* | *[!A-Za-z0-9_]*) echo "::warning::not a variable name: $1" ;;
    *) export "$1=$2" ;;
  esac
}

# ci_fix_step NAME SHELL SCRIPT [NAME=VALUE...]
ci_fix_step() {
  ci_fix_name="$1" ci_fix_shell="$2" ci_fix_script="$3"
  shift 3
  : > "${ci_fix_steps}/env"
  : > "${ci_fix_steps}/path"
  : > "${ci_fix_steps}/output"
  : > "${ci_fix_steps}/state"
  (
    cd "${GITHUB_WORKSPACE}" || exit 1
    for ci_fix_assignment in "$@"; do export "${ci_fix_assignment}"; done
    export GITHUB_ENV="${ci_fix_steps}/env" GITHUB_PATH="${ci_fix_steps}/path"
    export GITHUB_OUTPUT="${ci_fix_steps}/output" GITHUB_STATE="${ci_fix_steps}/state"
    exec ${ci_fix_shell} "${ci_fix_script}"
  )
  ci_fix_status=$?
  if [ "${ci_fix_status}" -ne 0 ]; then
    echo "::error::The job's setup step '${ci_fix_name}' failed with exit status ${ci_fix_status}"
    exit "${ci_fix_status}"
  fi
  ci_fix_load_env "${ci_fix_steps}/env"
  while IFS= read -r ci_fix_line || [ -n "${ci_fix_line}" ]; do
    if [ -n "${ci_fix_line}" ]; then PATH="${ci_fix_line}:${PATH}"; fi
  done < "${ci_fix_steps}/path"
  export PATH
}
"""
_SHELL_COMMANDS = {"": "${ci_fix_default}", "bash": "bash --noprofile --norc -eo pipefail", "sh": "sh -e"}


def setup_script(spec: JobSpec) -> str:
    """A POSIX ``sh`` script that runs ``spec``'s setup steps as GitHub runs them, in the job's env.

    It stops at the first step that fails, naming it.
    """
    lines = [_DRIVER.replace("@TITLE@", f"The setup steps of {spec.name}.").rstrip("\n"), ""]
    lines += [f"export {name}={_quoted(item)}" for name, item in spec.env.items()]
    lines += [f"export {name}={_quoted(item)}" for name, item in spec.setup_env.items()]
    if spec.checkout is not None and _uses_isolated_libbacktrace(spec.checkout, spec.steps):
        lines.append(
            "export LIBBACKTRACE_PREFIX="
            + _quoted(f"{spec.checkout.path}/.ci-fix-install")
        )
    for index, step in enumerate(_isolated_setup_steps(spec), 1):
        delimiter = _delimiter(step.run)
        lines += [
            "",
            f"cat > \"${{ci_fix_steps}}/{index}.sh\" <<'{delimiter}'",
            step.run.rstrip("\n"),
            delimiter,
            " ".join([
                "ci_fix_step", _quoted(step.name), f'"{_SHELL_COMMANDS[step.shell]}"', f'"${{ci_fix_steps}}/{index}.sh"',
                *(_quoted(f"{name}={item}") for name, item in step.env.items()),
            ]),
        ]
    return "\n".join(lines) + "\n"


def _delimiter(text: str) -> str:
    lines = set(text.splitlines())
    delimiter = "CI_FIX_STEP"
    while delimiter in lines:
        delimiter += "_"
    return delimiter


# --- GitHub expressions -------------------------------------------------------
#
# The grammar Valkey's workflows use: property paths, literals, ==, !=, &&,
# ||, !, parentheses, and a few functions, with GitHub's semantics. Anything
# else is Unsupported.


class _Known(dict):  # type: ignore[type-arg]
    """A context whose every property is known: reading another one is Unsupported."""

    def __missing__(self, key: str) -> Any:
        raise Unsupported(f"the expression reads {key!r}, which a fix job does not know")


_TOKEN_RE = re.compile(r"""
    \s*(?:
      (?P<string>'(?:[^']|'')*')
    | (?P<number>-?(?:0x[0-9a-fA-F]+|\d+(?:\.\d+)?(?:[eE][+-]?\d+)?))
    | (?P<op>==|!=|&&|\|\||[!().,])
    | (?P<name>[A-Za-z_][A-Za-z0-9_-]*)
    )""", re.VERBOSE)
_LITERALS = {"true": True, "false": False, "null": None}

Node = Callable[[], Any]


def evaluate(expression: str, context: dict[str, Any]) -> Any:
    """The value of a GitHub ``expression`` (without ``${{ }}``) in ``context``."""
    return _Parser(expression, context).parse()()


def render(text: str, context: dict[str, Any]) -> str:
    """``text`` with each ``${{ }}`` replaced by its value, as GitHub renders it."""
    out = []
    position = 0
    while (start := text.find("${{", position)) != -1:
        end = _expression_end(text, start + 3)
        out += [text[position:start], _string(evaluate(text[start + 3:end], context))]
        position = end + 2
    return "".join(out) + text[position:]


def value(item: Any, context: dict[str, Any]) -> Any:
    """``item``'s value: a lone ``${{ }}`` keeps its type, other text is rendered."""
    if not isinstance(item, str):
        return item
    stripped = item.strip()
    if stripped.startswith("${{") and _expression_end(stripped, 3) == len(stripped) - 2:
        return evaluate(stripped[3:-2], context)
    return render(item, context)


def condition(item: Any, context: dict[str, Any]) -> bool:
    """Whether a step's ``if:`` holds; ``always()`` and ``success()`` do, as the job ran to here."""
    if isinstance(item, str):
        stripped = item.strip()
        if stripped.startswith("${{") and _expression_end(stripped, 3) == len(stripped) - 2:
            stripped = stripped[3:-2]
        elif "${{" in stripped:
            raise Unsupported(f"the condition {item!r} mixes text and expressions")
        return _truthy(evaluate(stripped, context))
    return _truthy(item)


def _expression_end(text: str, start: int) -> int:
    """The index of the ``}}`` that closes the expression starting at ``start``."""
    quoted = False
    for index in range(start, len(text)):
        if text[index] == "'":
            quoted = not quoted
        elif not quoted and text.startswith("}}", index):
            return index
    raise Unsupported(f"an expression in {text!r} does not end")


class _Parser:
    """Parses one expression into a node: a function that evaluates it, short-circuiting as GitHub does."""

    def __init__(self, expression: str, context: dict[str, Any]) -> None:
        self.expression = expression
        self.context = context
        self.tokens: list[tuple[str, str]] = []
        position = 0
        while expression[position:].strip():
            match = _TOKEN_RE.match(expression, position)
            if match is None or match.lastgroup is None:
                raise Unsupported(f"cannot read the expression {expression!r}")
            self.tokens.append((match.lastgroup, match.group(match.lastgroup)))
            position = match.end()
        self.index = 0

    def parse(self) -> Node:
        node = self._or()
        if self.index != len(self.tokens):
            raise Unsupported(f"cannot read the expression {self.expression!r}")
        return node

    def _accept(self, *ops: str) -> str:
        if self.index < len(self.tokens) and self.tokens[self.index] in {("op", op) for op in ops}:
            self.index += 1
            return self.tokens[self.index - 1][1]
        return ""

    def _next(self) -> tuple[str, str]:
        if self.index >= len(self.tokens):
            raise Unsupported(f"the expression {self.expression!r} ends early")
        self.index += 1
        return self.tokens[self.index - 1]

    def _or(self) -> Node:
        nodes = [self._and()]
        while self._accept("||"):
            nodes.append(self._and())
        return nodes[0] if len(nodes) == 1 else lambda: _first(nodes, until=True)

    def _and(self) -> Node:
        nodes = [self._comparison()]
        while self._accept("&&"):
            nodes.append(self._comparison())
        return nodes[0] if len(nodes) == 1 else lambda: _first(nodes, until=False)

    def _comparison(self) -> Node:
        node = self._unary()
        while op := self._accept("==", "!="):
            node = _comparison(node, self._unary(), equal=op == "==")
        return node

    def _unary(self) -> Node:
        if self._accept("!"):
            operand = self._unary()
            return lambda: not _truthy(operand())
        return self._primary()

    def _primary(self) -> Node:
        kind, text = self._next()
        if kind == "string":
            literal: Any = text[1:-1].replace("''", "'")
            return lambda: literal
        if kind == "number":
            number = float(int(text, 16)) if text.lower().lstrip("-").startswith("0x") else float(text)
            return lambda: number
        if (kind, text) == ("op", "("):
            node = self._or()
            if not self._accept(")"):
                raise Unsupported(f"unbalanced parentheses in {self.expression!r}")
            return node
        if kind != "name":
            raise Unsupported(f"cannot read the expression {self.expression!r}")
        if text in _LITERALS:
            return lambda: _LITERALS[text]
        if self._accept("("):
            return self._call(text)
        if text not in self.context:
            raise Unsupported(f"the expression reads {text}, which a fix job does not have")
        path = [text]
        while self._accept("."):
            kind, part = self._next()
            if kind != "name":
                raise Unsupported(f"cannot read the expression {self.expression!r}")
            path.append(part)
        return lambda: _lookup(self.context, path)

    def _call(self, name: str) -> Node:
        function = _FUNCTIONS.get(name.lower())
        if function is None:
            raise Unsupported(f"the expression calls {name}(), which a fix job does not evaluate")
        arguments: list[Node] = []
        if not self._accept(")"):
            arguments.append(self._or())
            while self._accept(","):
                arguments.append(self._or())
            if not self._accept(")"):
                raise Unsupported(f"unbalanced parentheses in {self.expression!r}")
        arity, implementation = function
        if len(arguments) != arity:
            raise Unsupported(f"{name}() takes {arity} arguments")
        return lambda: implementation(*(argument() for argument in arguments))


def _comparison(left: Node, right: Node, *, equal: bool) -> Node:
    return lambda: _equal(left(), right()) == equal


def _first(nodes: list[Node], *, until: bool) -> Any:
    """``||`` (``until`` truthy) or ``&&``: the first operand whose truth is ``until``, or the last."""
    result = None
    for node in nodes:
        result = node()
        if _truthy(result) == until:
            return result
    return result


def _lookup(context: dict[str, Any], path: list[str]) -> Any:
    result: Any = context[path[0]]
    for part in path[1:]:
        result = result[part] if isinstance(result, _Known) else (
            result.get(part) if isinstance(result, dict) else None
        )
    return result


def _truthy(item: Any) -> bool:
    if item is None or isinstance(item, bool):
        return bool(item)
    if isinstance(item, (int, float)):
        return item != 0 and not math.isnan(item)
    if isinstance(item, str):
        return item != ""
    return True


def _number(item: Any) -> float:
    if item is None:
        return 0.0
    if isinstance(item, (bool, int, float)):
        return float(item)
    if isinstance(item, str):
        text = item.strip()
        if not text:
            return 0.0
        try:
            return float(int(text, 16)) if text.lower().startswith("0x") else float(text)
        except ValueError:
            return math.nan
    return math.nan


def _equal(left: Any, right: Any) -> bool:
    """GitHub's ``==``: strings ignore case; values of different types compare as numbers."""
    if isinstance(left, (dict, list)) or isinstance(right, (dict, list)):
        return left is right
    if isinstance(left, str) and isinstance(right, str):
        return left.casefold() == right.casefold()
    if type(left) is type(right) and not isinstance(left, (int, float)):
        return bool(left == right)
    return _number(left) == _number(right)


def _string(item: Any) -> str:
    """How GitHub writes a value into text."""
    if item is None:
        return ""
    if isinstance(item, bool):
        return "true" if item else "false"
    if isinstance(item, (int, float)):
        if isinstance(item, float) and item.is_integer():
            return str(int(item))
        return str(item)
    if isinstance(item, str):
        return item
    raise Unsupported("an object or array is written into text")


def _contains(search: Any, item: Any) -> bool:
    if isinstance(search, list):
        return any(_equal(element, item) for element in search)
    return _string(item).casefold() in _string(search).casefold()


def _from_json(text: Any) -> Any:
    try:
        return json.loads(_string(text))
    except ValueError:
        raise Unsupported(f"fromJSON() of {text!r}") from None


_FUNCTIONS: dict[str, tuple[int, Callable[..., Any]]] = {
    "contains": (2, _contains),
    "startswith": (2, lambda text, prefix: _string(text).casefold().startswith(_string(prefix).casefold())),
    "endswith": (2, lambda text, suffix: _string(text).casefold().endswith(_string(suffix).casefold())),
    "fromjson": (1, _from_json),
    "always": (0, lambda: True),
    "success": (0, lambda: True),
}
