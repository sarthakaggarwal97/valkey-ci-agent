from __future__ import annotations

from scripts.ci_fix.verify.base import VerifyEnv
from scripts.ci_fix.verify.job_metadata import resolve_workflow_job


def test_resolves_static_display_name_to_yaml_job_id() -> None:
    workflow = """
jobs:
  test:
    name: Ubuntu Test
    runs-on: ubuntu-24.04
"""
    resolved = resolve_workflow_job(workflow, "Ubuntu Test")

    assert resolved.job_id == "test"
    assert resolved.environment.env is VerifyEnv.LOCAL
    assert resolved.fidelity["mode"] == "targeted-approximation-v1"
    assert "complete-step-order" in resolved.fidelity["not_reproduced"]


def test_resolves_default_matrix_name_and_runner() -> None:
    workflow = """
jobs:
  test:
    strategy:
      matrix:
        os: [ubuntu-24.04, macos-15]
        compiler: [gcc, clang]
    runs-on: ${{ matrix.os }}
"""
    resolved = resolve_workflow_job(workflow, "test (macos-15, clang)")

    assert resolved.job_id == "test"
    assert dict(resolved.matrix) == {"compiler": "clang", "os": "macos-15"}
    assert resolved.environment.env is VerifyEnv.MACOS


def test_resolves_explicit_matrix_name_and_container() -> None:
    workflow = """
jobs:
  build:
    name: Build ${{ matrix.variant }}
    strategy:
      matrix:
        variant: [bookworm, alpine]
        include:
          - variant: bookworm
            image: debian:bookworm
          - variant: alpine
            image: alpine:3.20
    runs-on: ubuntu-24.04
    container:
      image: ${{ matrix.image }}
"""
    resolved = resolve_workflow_job(workflow, "Build alpine")

    assert resolved.job_id == "build"
    assert resolved.environment.env is VerifyEnv.DOCKER
    assert resolved.environment.image == "alpine:3.20"


def test_rejects_name_from_a_different_workflow() -> None:
    workflow = """
jobs:
  test:
    runs-on: ubuntu-latest
"""
    resolved = resolve_workflow_job(workflow, "Ubuntu Test")

    assert resolved.environment.env is VerifyEnv.UNSUPPORTED
    assert "does not resolve" in resolved.reason


def test_rejects_ambiguous_display_name() -> None:
    workflow = """
jobs:
  first:
    name: Test
    runs-on: ubuntu-latest
  second:
    name: Test
    runs-on: macos-15
"""
    resolved = resolve_workflow_job(workflow, "Test")

    assert resolved.environment.env is VerifyEnv.UNSUPPORTED
    assert "multiple" in resolved.reason


def test_rejects_unbounded_matrix() -> None:
    values = ", ".join(str(index) for index in range(257))
    workflow = f"""
jobs:
  test:
    strategy:
      matrix:
        shard: [{values}]
    runs-on: ubuntu-latest
"""
    resolved = resolve_workflow_job(workflow, "test (1)")

    assert resolved.environment.env is VerifyEnv.UNSUPPORTED
