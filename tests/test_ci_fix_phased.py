from __future__ import annotations

import subprocess
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from scripts.ci_fix import phased
from scripts.ci_fix.models import FixPath, FixProposal, ReviewVerdict, RunResult
from scripts.ci_fix.phase_artifact import load_prepared, load_validated
from scripts.ci_fix.review import PatchReview
from scripts.ci_fix.runner import verification_runtime_contract
from scripts.common.phase_artifact import ArtifactError, sha256_bytes, write_json
from scripts.common.proc import build_approved_patch, git_output, run_git
from scripts.common.publication_manifest import load_publication_manifest


class _LocalTestSandbox:
    def __init__(self, repo_dir: str) -> None:
        self.repo_dir = Path(repo_dir)

    def __enter__(self) -> "_LocalTestSandbox":
        return self

    def __exit__(self, *_args: Any) -> None:
        return None

    def run(
        self,
        repo_dir: str,
        command: str,
        *,
        workdir: str = "",
        timeout: int = 1800,
        **_kwargs: Any,
    ) -> RunResult:
        assert Path(repo_dir) == self.repo_dir
        result = subprocess.run(
            [
                "/bin/bash",
                "--noprofile",
                "--norc",
                "-euo",
                "pipefail",
                "-c",
                command,
            ],
            cwd=self.repo_dir / workdir,
            capture_output=True,
            text=True,
            timeout=timeout,
            check=False,
        )
        return RunResult(
            ran=True,
            passed=result.returncode == 0,
            exit_code=result.returncode,
            command=command,
            output_tail=result.stdout + result.stderr,
        )


class _LocalTestRuntime:
    image_identity = "sha256:" + "a" * 64
    sandbox_uid = None

    def sandbox(self, repo_dir: str) -> _LocalTestSandbox:
        return _LocalTestSandbox(repo_dir)

    def contract(self) -> dict[str, object]:
        return verification_runtime_contract(
            platform="linux",
            image_identity=self.image_identity,
        )


@pytest.fixture(autouse=True)
def _isolated_runtime_test_double(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        phased,
        "prepare_verification_runtime",
        lambda **_kwargs: _LocalTestRuntime(),
    )


def _repo(tmp_path: Path) -> tuple[Path, str, str]:
    repo = tmp_path / "source"
    repo.mkdir()
    run_git(str(repo), "init", "-q")
    run_git(str(repo), "config", "user.name", "Test")
    run_git(str(repo), "config", "user.email", "test@example.com")
    (repo / "value.txt").write_text("broken\n", encoding="utf-8")
    run_git(str(repo), "add", "value.txt")
    run_git(str(repo), "commit", "-qm", "broken head")
    head = git_output(str(repo), "rev-parse", "HEAD").strip()
    branch = "agent/backport/7-to-1.0"
    run_git(str(repo), "branch", "-M", branch)
    run_git(str(repo), "config", "receive.denyCurrentBranch", "updateInstead")
    return repo, branch, head


def _discovery(root: Path, *, branch: str, head: str) -> None:
    workflow = b"""
jobs:
  test:
    name: Unit Test
    runs-on: ubuntu-24.04
"""
    log = b"Unit Test failed: expected fixed, got broken\n"
    (root / "workflow.yml").write_bytes(workflow)
    (root / "log-0000.txt").write_bytes(log)
    write_json(
        root / "discovery.json",
        {
            "schema_version": 1,
            "kind": "ci-fix-discovery",
            "request": {
                "repository": "org/repo",
                "pr_number": 7,
                "head_repository": "org/repo",
                "head_branch": branch,
                "head_sha": head,
                "run_id": 123,
                "requested_by": "maintainer",
                "hint": "",
                "comment_id": 456,
            },
            "workflow": {
                "workflow_id": 9,
                "workflow_path": ".github/workflows/ci.yml",
                "run_attempt": 1,
                "file": "workflow.yml",
                "sha256": sha256_bytes(workflow),
            },
            "failed_jobs": [
                {
                    "database_id": 11,
                    "display_name": "Unit Test",
                    "conclusion": "failure",
                    "labels": ["ubuntu-24.04"],
                    "runner_name": "GitHub Actions 1",
                    "runner_group_id": 0,
                    "job_id": "test",
                    "matrix": {},
                    "environment": "local",
                    "image": "",
                    "fidelity": {
                        "mode": "targeted-approximation-v1",
                        "authoritative_check": "pull-request-ci",
                        "reproduced": ["targeted-command"],
                        "not_reproduced": ["complete-step-order"],
                    },
                    "reason": "",
                }
            ],
            "logs": [
                {
                    "source_name": "test/step.txt",
                    "file": "log-0000.txt",
                    "sha256": sha256_bytes(log),
                    "bytes": len(log),
                }
            ],
        },
    )


def _prepare_author_fix(
    monkeypatch: pytest.MonkeyPatch,
    artifact: Path,
    proposal: FixProposal,
) -> None:
    monkeypatch.setattr(phased, "diagnose_failure", lambda *_args, **_kwargs: proposal)
    monkeypatch.setattr(phased, "discover_port_candidates", lambda *_args: ())

    def fake_apply(repo_dir: str, _proposal: FixProposal) -> tuple[bool, tuple[str, ...]]:
        Path(repo_dir, "value.txt").write_text("fixed\n", encoding="utf-8")
        return True, ("value.txt",)

    monkeypatch.setattr(phased, "apply_fix", fake_apply)
    monkeypatch.setattr(
        phased,
        "build_and_review_patch",
        lambda repo_dir, changed, _proposal: PatchReview(
            ok=True,
            patch=build_approved_patch(repo_dir, changed),
            review=ReviewVerdict(True, "minimal fixture correction"),
        ),
    )
    assert phased.prepare(artifact_directory=artifact) == ("ready", "local")


def test_ci_fix_phases_require_failing_baseline_and_publish_validated_tree(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    remote, branch, head = _repo(tmp_path)
    artifact = tmp_path / "artifact"
    artifact.mkdir()
    _discovery(artifact, branch=branch, head=head)
    monkeypatch.setattr(phased, "github_https_url", lambda _repo: str(remote))

    proposal = FixProposal(
        path=FixPath.AUTHOR,
        failing_check="Unit Test",
        root_cause="value.txt contains the stale value",
        reasoning="replace the stale fixture value",
        confidence=0.9,
        failing_job_hint="Unit Test",
        verify_command='test "$(cat value.txt)" = fixed',
    )
    monkeypatch.setattr(phased, "diagnose_failure", lambda *_args, **_kwargs: proposal)
    monkeypatch.setattr(phased, "discover_port_candidates", lambda *_args: ())

    def fake_apply(repo_dir: str, _proposal: FixProposal) -> tuple[bool, tuple[str, ...]]:
        Path(repo_dir, "value.txt").write_text("fixed\n", encoding="utf-8")
        return True, ("value.txt",)

    def fake_review(
        repo_dir: str,
        changed: tuple[str, ...],
        _proposal: FixProposal,
    ) -> PatchReview:
        return PatchReview(
            ok=True,
            patch=build_approved_patch(repo_dir, changed),
            review=ReviewVerdict(True, "minimal fixture correction"),
        )

    monkeypatch.setattr(phased, "apply_fix", fake_apply)
    monkeypatch.setattr(phased, "build_and_review_patch", fake_review)

    status, environment = phased.prepare(artifact_directory=artifact)
    assert (status, environment) == ("ready", "local")
    assert load_prepared(artifact).changed_paths == ("value.txt",)

    phased.validate(
        artifact_directory=artifact,
        platform="linux",
        verify_runs=2,
    )
    validated = load_validated(artifact)
    phased.preflight_publish(artifact_directory=artifact)

    class FakeRepo:
        full_name = "org/repo"

        def get_pull(self, _number: int) -> Any:
            return SimpleNamespace(
                head=SimpleNamespace(sha=head, ref=branch),
            )

        def get_issue(self, _number: int) -> Any:
            return SimpleNamespace(number=7)

    class FakeGithub:
        def get_repo(self, _name: str) -> FakeRepo:
            return FakeRepo()

    comments: list[str] = []
    reactions: list[tuple[int, Any]] = []
    monkeypatch.setattr(phased, "Github", lambda **_kwargs: FakeGithub())
    monkeypatch.setattr(
        phased,
        "record_desired_comment",
        lambda _issue, desired, **_kwargs: (
            comments.append(desired.body) or SimpleNamespace()
        ),
    )
    monkeypatch.setattr(
        phased,
        "reconcile_desired_comment",
        lambda _repo, _comment, desired, **_kwargs: (
            reactions.append((desired.reaction_comment_id, desired.reaction)) or True
        ),
    )
    monkeypatch.setenv("PUBLISHER_IDENTITY", "publisher[bot]")

    push_calls: list[tuple[str, ...]] = []
    real_run_git = phased.run_git

    def recording_run_git(repo_dir, *args, **kwargs):
        if args and args[0] == "push":
            push_calls.append(args)
        return real_run_git(repo_dir, *args, **kwargs)

    monkeypatch.setattr(phased, "run_git", recording_run_git)
    commit = phased.publish(
        artifact_directory=artifact,
        token="publisher-token",
    )

    assert git_output(str(remote), "rev-parse", f"refs/heads/{branch}").strip() == commit
    assert git_output(str(remote), "rev-parse", f"{commit}^{{tree}}").strip() == (
        validated.prepared.result_tree
    )
    assert comments and "Unit Test" in comments[0]
    assert "not a replay of the complete Actions job" in comments[0]
    assert reactions and reactions[0][0] == 456
    assert len(push_calls) == 1
    assert (
        f"--force-with-lease=refs/heads/{branch}:{head}"
        in push_calls[0]
    )
    publication = load_publication_manifest(
        artifact,
        expected_kind="ci-fix-publication",
        final_state_keys=phased._PUBLICATION_STATE_KEYS,
        expected_source_file="validated.json",
        expected_source_sha256=validated.manifest_sha256,
    )
    assert publication.final_state["published_commit"] == commit
    assert publication.final_state["published_tree"] == validated.prepared.result_tree


def test_validation_rejects_a_green_baseline(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    remote, branch, head = _repo(tmp_path)
    artifact = tmp_path / "artifact"
    artifact.mkdir()
    _discovery(artifact, branch=branch, head=head)
    monkeypatch.setattr(phased, "github_https_url", lambda _repo: str(remote))
    proposal = FixProposal(
        path=FixPath.AUTHOR,
        failing_check="Unit Test",
        root_cause="fixture",
        reasoning="fixture",
        confidence=0.9,
        failing_job_hint="Unit Test",
        verify_command="test -f value.txt",
    )
    monkeypatch.setattr(phased, "diagnose_failure", lambda *_args, **_kwargs: proposal)
    monkeypatch.setattr(phased, "discover_port_candidates", lambda *_args: ())
    monkeypatch.setattr(
        phased,
        "apply_fix",
        lambda repo_dir, _proposal: (
            Path(repo_dir, "value.txt").write_text("fixed\n", encoding="utf-8") > 0,
            ("value.txt",),
        ),
    )
    monkeypatch.setattr(
        phased,
        "build_and_review_patch",
        lambda repo_dir, changed, _proposal: PatchReview(
            ok=True,
            patch=build_approved_patch(repo_dir, changed),
            review=ReviewVerdict(True, "ok"),
        ),
    )
    phased.prepare(artifact_directory=artifact)

    with pytest.raises(ArtifactError, match="baseline passed"):
        phased.validate(
            artifact_directory=artifact,
            platform="linux",
        )


def test_preflight_rejects_pr_branch_movement(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    remote, branch, head = _repo(tmp_path)
    artifact = tmp_path / "artifact"
    artifact.mkdir()
    _discovery(artifact, branch=branch, head=head)
    monkeypatch.setattr(phased, "github_https_url", lambda _repo: str(remote))
    proposal = FixProposal(
        path=FixPath.AUTHOR,
        failing_check="Unit Test",
        root_cause="fixture",
        reasoning="fixture",
        confidence=0.9,
        failing_job_hint="Unit Test",
        verify_command='test "$(cat value.txt)" = fixed',
    )
    _prepare_author_fix(monkeypatch, artifact, proposal)
    phased.validate(
        artifact_directory=artifact,
        platform="linux",
        verify_runs=1,
    )

    (remote / "value.txt").write_text("branch advanced\n", encoding="utf-8")
    run_git(str(remote), "commit", "-qam", "advance branch")

    with pytest.raises(ArtifactError, match="PR branch moved"):
        phased.preflight_publish(artifact_directory=artifact)
    assert not (artifact / "publisher-permit.json").exists()


def test_validation_rejects_tracked_file_side_effect(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    remote, branch, head = _repo(tmp_path)
    artifact = tmp_path / "artifact"
    artifact.mkdir()
    _discovery(artifact, branch=branch, head=head)
    monkeypatch.setattr(phased, "github_https_url", lambda _repo: str(remote))
    proposal = FixProposal(
        path=FixPath.AUTHOR,
        failing_check="Unit Test",
        root_cause="fixture",
        reasoning="fixture",
        confidence=0.9,
        failing_job_hint="Unit Test",
        verify_command=(
            "if grep -q broken value.txt; then exit 1; fi; "
            "printf 'validation side effect\\n' > value.txt"
        ),
    )
    _prepare_author_fix(monkeypatch, artifact, proposal)

    with pytest.raises(ArtifactError, match="modified tracked"):
        phased.validate(
            artifact_directory=artifact,
            platform="linux",
            verify_runs=1,
        )
    assert not (artifact / "validated.json").exists()


def test_publisher_retry_recovers_after_push_before_comment_reconciliation(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    remote, branch, head = _repo(tmp_path)
    artifact = tmp_path / "artifact"
    artifact.mkdir()
    _discovery(artifact, branch=branch, head=head)
    monkeypatch.setattr(phased, "github_https_url", lambda _repo: str(remote))
    proposal = FixProposal(
        path=FixPath.AUTHOR,
        failing_check="Unit Test",
        root_cause="fixture",
        reasoning="fixture",
        confidence=0.9,
        failing_job_hint="Unit Test",
        verify_command='test "$(cat value.txt)" = fixed',
    )
    _prepare_author_fix(monkeypatch, artifact, proposal)
    phased.validate(
        artifact_directory=artifact,
        platform="linux",
        verify_runs=1,
    )
    phased.preflight_publish(artifact_directory=artifact)

    class FakeRepo:
        full_name = "org/repo"

        def get_pull(self, _number: int) -> Any:
            current = git_output(
                str(remote),
                "rev-parse",
                f"refs/heads/{branch}",
            ).strip()
            return SimpleNamespace(
                head=SimpleNamespace(sha=current, ref=branch),
            )

        def get_issue(self, _number: int) -> Any:
            return SimpleNamespace(number=7)

    class FakeGithub:
        def get_repo(self, _name: str) -> FakeRepo:
            return FakeRepo()

    pending_comment = SimpleNamespace()
    reconciliations = iter([False, True])
    push_calls: list[tuple[str, ...]] = []
    real_run_git = phased.run_git

    def recording_run_git(repo_dir, *args, **kwargs):
        if args and args[0] == "push":
            push_calls.append(args)
        return real_run_git(repo_dir, *args, **kwargs)

    monkeypatch.setattr(phased, "Github", lambda **_kwargs: FakeGithub())
    monkeypatch.setattr(
        phased,
        "record_desired_comment",
        lambda *_args, **_kwargs: pending_comment,
    )
    monkeypatch.setattr(
        phased,
        "reconcile_desired_comment",
        lambda *_args, **_kwargs: next(reconciliations),
    )
    monkeypatch.setattr(phased, "run_git", recording_run_git)
    monkeypatch.setenv("PUBLISHER_IDENTITY", "publisher[bot]")

    with pytest.raises(ArtifactError, match="desired comment state"):
        phased.publish(
            artifact_directory=artifact,
            token="publisher-token",
        )
    first_published = git_output(
        str(remote),
        "rev-parse",
        f"refs/heads/{branch}",
    ).strip()
    assert first_published != head
    assert not (artifact / "publication.json").exists()

    recovered = phased.publish(
        artifact_directory=artifact,
        token="publisher-token",
    )

    assert recovered == first_published
    assert len(push_calls) == 1
    publication = load_publication_manifest(
        artifact,
        expected_kind="ci-fix-publication",
        final_state_keys=phased._PUBLICATION_STATE_KEYS,
    )
    assert publication.final_state["published_commit"] == first_published
