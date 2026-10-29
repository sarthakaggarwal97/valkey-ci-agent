from __future__ import annotations

import shutil
import stat
from pathlib import Path

import pytest

from scripts.ci_fix import prepare_job_workspace as workspace
from scripts.ci_fix.ci_job import Checkout, JobCommands, JobSpec
from scripts.ci_fix.models import (
    Failure,
    FixPlan,
    FixPolicy,
    FixTarget,
    FixTask,
    FrontDoor,
    TargetKind,
)
from scripts.ci_fix.run import JOB_FILE, PLAN_FILE
from scripts.ci_fix.state import save

_HEAD = "a" * 40
_EXTRA_HEAD = "b" * 40


def _plan(*, pull_number: int = 42) -> FixPlan:
    return FixPlan(
        front_door=FrontDoor.PR,
        task=FixTask(
            failure=Failure(
                repo_full_name="valkey-io/valkey",
                run_id=1,
                head_sha=_HEAD,
                job_id=2,
                job_name="test",
                pr_number=pull_number,
            ),
            target=FixTarget(
                kind=TargetKind.UPDATE_PR,
                repo_full_name="valkey-io/valkey",
                expected_sha=_HEAD,
                execution_trusted=True,
            ),
            policy=FixPolicy.GENERAL,
        ),
    )


def _job(checkout: Checkout | None) -> JobSpec:
    return JobSpec(
        job_id="test",
        name="test",
        runs_on="ubuntu-24.04",
        container="",
        timeout_minutes=120,
        env={},
        steps=(),
        checkout=checkout,
        commands=JobCommands(build="make", runtest="./runtest"),
    )


def _layout(tmp_path: Path, monkeypatch, *, job: JobSpec | None) -> tuple[Path, Path, list[tuple]]:
    plan_dir = tmp_path / "plan"
    plan_dir.mkdir()
    save(plan_dir / PLAN_FILE, _plan())
    if job is not None:
        save(plan_dir / JOB_FILE, job)
    temp_root = tmp_path / "physical-tmp"
    temp_root.mkdir()
    monkeypatch.setattr(workspace, "canonical_temp_root", lambda: temp_root)

    clones: list[tuple] = []
    heads: dict[Path, str] = {}

    def clone(repository, destination, commit, *, pull_number=0):
        destination.mkdir(parents=True)
        (destination / ".git").mkdir()
        clones.append((repository, destination, commit, pull_number))
        heads[destination] = commit
        return True

    monkeypatch.setattr(workspace, "shallow_clone_at_sha", clone)
    monkeypatch.setattr(
        workspace,
        "git_output",
        lambda repo, *_args: heads[Path(repo)] + "\n",
    )
    return plan_dir, temp_root, clones


def test_workspace_is_one_stable_repo_below_a_runner_owned_physical_tmp_anchor(
    tmp_path,
    monkeypatch,
):
    plan_dir, temp_root, clones = _layout(tmp_path, monkeypatch, job=_job(None))
    state_file = tmp_path / "runner-temp" / "ci-fix-sandbox-state"
    state_file.parent.mkdir()

    state = workspace.create_workspace(plan_dir, state_file)

    assert state.path.name == "repo"
    assert state.path.parent.parent == temp_root
    assert state.path.parent.name.startswith("valkey-ci-fix-")
    assert stat.S_IMODE(state.path.parent.stat().st_mode) == 0o711
    assert clones == [
        ("valkey-io/valkey", state.path, _HEAD, 42),
    ]
    assert workspace.read_workspace_state(state_file) == state
    assert stat.S_IMODE(state_file.stat().st_mode) == 0o600


def test_workspace_refuses_a_primary_clone_at_the_wrong_head(tmp_path, monkeypatch):
    plan_dir, temp_root, _clones = _layout(tmp_path, monkeypatch, job=None)
    state_file = tmp_path / "runner-temp" / "ci-fix-sandbox-state"
    state_file.parent.mkdir()
    monkeypatch.setattr(
        workspace,
        "git_output",
        lambda *_args: f"{'c' * 40}\n",
    )

    with pytest.raises(RuntimeError, match="isolated checkout resolved"):
        workspace.create_workspace(plan_dir, state_file)

    assert not state_file.exists()
    assert list(temp_root.iterdir()) == []


def test_exact_full_sha_extra_checkout_is_cloned_inside_the_same_sandbox(
    tmp_path,
    monkeypatch,
):
    extra = Checkout(
        repository="ianlancetaylor/libbacktrace",
        ref=_EXTRA_HEAD,
        path="deps/libbacktrace",
    )
    plan_dir, _temp_root, clones = _layout(
        tmp_path,
        monkeypatch,
        job=_job(extra),
    )
    state_file = tmp_path / "runner-temp" / "ci-fix-sandbox-state"
    state_file.parent.mkdir()

    state = workspace.create_workspace(plan_dir, state_file)

    assert state.in_job
    assert clones == [
        ("valkey-io/valkey", state.path, _HEAD, 42),
        (
            "ianlancetaylor/libbacktrace",
            state.path / "deps/libbacktrace",
            _EXTRA_HEAD,
            0,
        ),
    ]


def test_workspace_refuses_an_extra_checkout_at_the_wrong_head(tmp_path, monkeypatch):
    extra = Checkout(
        repository="ianlancetaylor/libbacktrace",
        ref=_EXTRA_HEAD,
        path="deps/libbacktrace",
    )
    plan_dir, temp_root, _clones = _layout(
        tmp_path,
        monkeypatch,
        job=_job(extra),
    )
    state_file = tmp_path / "runner-temp" / "ci-fix-sandbox-state"
    state_file.parent.mkdir()

    def resolved_head(repo, *_args):
        return f"{_HEAD if Path(repo).name == 'repo' else 'c' * 40}\n"

    monkeypatch.setattr(workspace, "git_output", resolved_head)

    with pytest.raises(RuntimeError, match="extra checkout resolved a different commit"):
        workspace.create_workspace(plan_dir, state_file)

    assert not state_file.exists()
    assert list(temp_root.iterdir()) == []


def test_local_mode_still_uses_the_same_precredential_exact_checkout(
    tmp_path,
    monkeypatch,
):
    plan_dir, _temp_root, _clones = _layout(tmp_path, monkeypatch, job=None)
    state_file = tmp_path / "runner-temp" / "ci-fix-sandbox-state"
    state_file.parent.mkdir()

    state = workspace.create_workspace(plan_dir, state_file)

    assert not state.in_job
    assert state.commit == _HEAD


def test_workspace_is_refused_before_clone_when_physical_tmp_is_too_full(
    tmp_path,
    monkeypatch,
):
    plan_dir, temp_root, clones = _layout(tmp_path, monkeypatch, job=None)
    state_file = tmp_path / "runner-temp" / "ci-fix-sandbox-state"
    state_file.parent.mkdir()
    usage = workspace.shutil.disk_usage(temp_root)
    monkeypatch.setattr(
        workspace.shutil,
        "disk_usage",
        lambda _path: usage._replace(free=workspace._MINIMUM_FREE_BYTES - 1),
    )

    with pytest.raises(RuntimeError, match="less than 1 GiB"):
        workspace.create_workspace(plan_dir, state_file)

    assert clones == []
    assert list(temp_root.iterdir()) == []
    assert not state_file.exists()


@pytest.mark.parametrize(
    "checkout",
    (
        Checkout("ianlancetaylor/libbacktrace", "main", "libbacktrace"),
        Checkout("ianlancetaylor/libbacktrace", _EXTRA_HEAD, "../outside"),
    ),
)
def test_unpinned_or_unsafe_extra_checkout_is_rejected_and_anchor_removed(
    tmp_path,
    monkeypatch,
    checkout,
):
    plan_dir, temp_root, _clones = _layout(
        tmp_path,
        monkeypatch,
        job=_job(checkout),
    )
    state_file = tmp_path / "runner-temp" / "ci-fix-sandbox-state"
    state_file.parent.mkdir()

    with pytest.raises(ValueError):
        workspace.create_workspace(plan_dir, state_file)

    assert not state_file.exists()
    assert list(temp_root.iterdir()) == []


def test_cleanup_deletes_the_validated_anchor_not_a_redirected_target(
    tmp_path,
    monkeypatch,
):
    plan_dir, _temp_root, _clones = _layout(tmp_path, monkeypatch, job=None)
    state_file = tmp_path / "runner-temp" / "ci-fix-sandbox-state"
    state_file.parent.mkdir()
    state = workspace.create_workspace(plan_dir, state_file)
    outside = tmp_path / "outside"
    outside.mkdir()
    sentinel = outside / "sentinel"
    sentinel.write_text("keep\n", encoding="utf-8")
    (state.path / "outside").symlink_to(outside, target_is_directory=True)
    abandoned_home = state.path.parent / "ci-fix-home-abandoned"
    abandoned_home.mkdir()
    (abandoned_home / "session-state").write_text("private\n", encoding="utf-8")

    workspace.cleanup_workspace(state_file)

    assert not state.path.parent.exists()
    assert not state_file.exists()
    assert sentinel.read_text(encoding="utf-8") == "keep\n"


def test_cleanup_detaches_nested_mounts_deepest_first(
    tmp_path,
    monkeypatch,
):
    plan_dir, _temp_root, _clones = _layout(tmp_path, monkeypatch, job=None)
    state_file = tmp_path / "runner-temp" / "ci-fix-sandbox-state"
    state_file.parent.mkdir()
    state = workspace.create_workspace(plan_dir, state_file)
    cleanup = state.path.parent / ".cleanup"
    outer = cleanup / "build"
    inner = outer / "nested"
    mount_scans = iter(({outer, inner}, set(), set()))
    unmounted: list[Path] = []
    monkeypatch.setattr(
        workspace,
        "_descendant_mounts",
        lambda _anchor: next(mount_scans),
    )
    monkeypatch.setattr(workspace, "_unmount", unmounted.append)

    workspace.cleanup_workspace(state_file)

    assert unmounted == [inner, outer]
    assert not state.path.parent.exists()
    assert not state_file.exists()


def test_cleanup_quarantines_instead_of_crossing_a_surviving_mount(
    tmp_path,
    monkeypatch,
):
    plan_dir, temp_root, _clones = _layout(tmp_path, monkeypatch, job=None)
    state_file = tmp_path / "runner-temp" / "ci-fix-sandbox-state"
    state_file.parent.mkdir()
    state = workspace.create_workspace(plan_dir, state_file)
    mount = state.path / "mounted"
    monkeypatch.setattr(workspace, "_descendant_mounts", lambda _anchor: {mount})
    monkeypatch.setattr(workspace, "_unmount", lambda _mount: None)

    with pytest.raises(RuntimeError, match="quarantined"):
        workspace.cleanup_workspace(state_file)

    quarantined = list(temp_root.iterdir())
    assert len(quarantined) == 1
    assert quarantined[0].name.startswith(".valkey-ci-fix-cleanup-")
    assert stat.S_IMODE(quarantined[0].stat().st_mode) == 0
    assert state_file.exists()
    quarantined[0].chmod(0o711)
    shutil.rmtree(quarantined[0])


def test_mount_table_paths_decode_kernel_octal_escapes():
    assert workspace._mount_path(r"/private/tmp/a\040b\011c") == Path(
        "/private/tmp/a b\tc"
    )


def test_macos_mount_parser_uses_the_final_on_separator_and_rejects_truncation():
    anchor = Path("/private/tmp/valkey-ci-fix-example")
    mount = anchor / "repo/name (with parens)"
    output = (
        f"/Volumes/source on odd name on {mount} (nullfs, local)\n"
        "/dev/disk1s1 on /Volumes/elsewhere (apfs, local)\n"
    )

    assert workspace._macos_mounts(output, anchor) == {mount}
    with pytest.raises(RuntimeError, match="malformed"):
        workspace._macos_mounts(output + "truncated mount line\n", anchor)


def test_redirected_state_or_sandbox_path_is_refused(tmp_path, monkeypatch):
    plan_dir, _temp_root, _clones = _layout(tmp_path, monkeypatch, job=None)
    state_file = tmp_path / "runner-temp" / "ci-fix-sandbox-state"
    state_file.parent.mkdir()
    state = workspace.create_workspace(plan_dir, state_file)
    real_repo = state.path.parent / "real-repo"
    state.path.rename(real_repo)
    state.path.symlink_to(real_repo, target_is_directory=True)

    with pytest.raises(ValueError, match="physical"):
        workspace.read_workspace_state(state_file)
