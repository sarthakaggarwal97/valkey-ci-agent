"""Tests for deterministic job-environment classification.

Code owns environment selection: parse runs-on and container.image only, and
refuse anything it does not clearly understand.
"""

from __future__ import annotations

import pytest

from scripts.ci_fix.verify.base import VerifyEnv
from scripts.ci_fix.verify.workflow_env import classify_job_environment

_WF = """
jobs:
  test-ubuntu-latest:
    runs-on: ubuntu-latest
    steps:
      - run: make test
  build-almalinux8:
    runs-on: ubuntu-latest
    container: almalinux:8
    steps:
      - run: make
  build-debian:
    runs-on: ubuntu-latest
    container:
      image: debian:bullseye
    steps:
      - run: make
  build-macos-latest:
    runs-on: macos-latest
    steps:
      - run: make
  bench:
    runs-on: ["self-hosted", "arm64"]
    steps:
      - run: make
  matrix-runner:
    runs-on: ${{ matrix.os }}
    steps:
      - run: make
  dynamic-image:
    runs-on: ubuntu-latest
    container: ${{ matrix.container }}
    steps:
      - run: make
"""


def test_plain_ubuntu_is_local():
    env = classify_job_environment(_WF, "test-ubuntu-latest")
    assert env.env is VerifyEnv.LOCAL
    assert env.image == ""


def test_container_string_is_docker():
    env = classify_job_environment(_WF, "build-almalinux8")
    assert env.env is VerifyEnv.DOCKER
    assert env.image == "almalinux:8"


def test_container_mapping_image_is_docker():
    env = classify_job_environment(_WF, "build-debian")
    assert env.env is VerifyEnv.DOCKER
    assert env.image == "debian:bullseye"


def test_macos_runner():
    assert classify_job_environment(_WF, "build-macos-latest").env is VerifyEnv.MACOS


def test_self_hosted_list_is_unsupported():
    env = classify_job_environment(_WF, "bench")
    assert env.env is VerifyEnv.UNSUPPORTED
    assert "unsupported runner" in env.reason


def test_matrix_runner_is_unsupported():
    assert classify_job_environment(_WF, "matrix-runner").env is VerifyEnv.UNSUPPORTED


def test_dynamic_container_image_is_unsupported():
    env = classify_job_environment(_WF, "dynamic-image")
    assert env.env is VerifyEnv.UNSUPPORTED
    assert "dynamic or malformed" in env.reason


def test_missing_job_is_unsupported():
    env = classify_job_environment(_WF, "no-such-job")
    assert env.env is VerifyEnv.UNSUPPORTED
    assert "not found" in env.reason
    assert env.matched is False


def test_malformed_yaml_is_unsupported():
    env = classify_job_environment("jobs: [this is: not valid", "x")
    assert env.env is VerifyEnv.UNSUPPORTED
    assert env.matched is False


_ARM_WF = """
jobs:
  test-arm:
    runs-on: ubuntu-24.04-arm
    steps:
      - run: make
  test-x86:
    runs-on: ubuntu-24.04
    steps:
      - run: make
  test-versioned-macos:
    runs-on: macos-14
    steps:
      - run: make
"""


def test_arm_runner_is_unsupported():
    # ubuntu-*-arm must NOT be classified local/x86.
    env = classify_job_environment(_ARM_WF, "test-arm")
    assert env.env is VerifyEnv.UNSUPPORTED


def test_versioned_host_labels_are_unsupported_when_verifier_uses_latest():
    assert classify_job_environment(_ARM_WF, "test-x86").env is VerifyEnv.UNSUPPORTED
    assert (
        classify_job_environment(
            _ARM_WF,
            "test-versioned-macos",
        ).env
        is VerifyEnv.UNSUPPORTED
    )


_REGISTRY_WF = """
jobs:
  port-image:
    runs-on: ubuntu-latest
    container: ghcr.io:443/org/image:tag
    steps:
      - run: make
  digest-image:
    runs-on: ubuntu-latest
    container:
      image: ghcr.io/org/image@sha256:""" + ("a" * 64) + """
    steps:
      - run: make
"""


def test_registry_port_and_digest_images_classify_docker():
    assert classify_job_environment(_REGISTRY_WF, "port-image").env is VerifyEnv.DOCKER
    assert classify_job_environment(_REGISTRY_WF, "digest-image").env is VerifyEnv.DOCKER


_EXACT_ONLY_WF = """
jobs:
  matrix-container:
    strategy:
      matrix:
        image: [fedora:latest, alpine:latest]
    runs-on: ubuntu-latest
    container: ${{ matrix.image }}
  emulated-s390x:
    runs-on: ubuntu-latest
    steps:
      - uses: uraimo/run-on-arch-action@0123456789abcdef
        with:
          arch: s390x
  freebsd:
    runs-on: ubuntu-latest
    steps:
      - uses: cross-platform-actions/action@0123456789abcdef
        with:
          operating_system: freebsd
  setup-xcode:
    runs-on: macos-14
    steps:
      - uses: maxim-lobanov/setup-xcode@0123456789abcdef
  service-job:
    runs-on: ubuntu-latest
    services:
      valkey:
        image: valkey/valkey:latest
  reusable:
    uses: ./.github/workflows/_test.yml
  multiline-platform:
    runs-on: ubuntu-latest
    steps:
      - run: |
          docker run --rm \\
            --platform linux/386 \\
            alpine:latest true
  downloaded-build:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/download-artifact@0123456789abcdef
  near-match-local-action:
    runs-on: ubuntu-latest
    steps:
      - uses: ./.github/actions/upload-test-failures-extra
"""


def test_matrix_and_specialized_setup_require_target_owned_verifier():
    for job in (
        "matrix-container",
        "emulated-s390x",
        "freebsd",
        "setup-xcode",
        "service-job",
        "reusable",
        "multiline-platform",
        "downloaded-build",
        "near-match-local-action",
    ):
        env = classify_job_environment(_EXACT_ONLY_WF, job)
        assert env.env is VerifyEnv.UNSUPPORTED
        assert "target-owned" in env.reason
        assert env.matched is True


_VALKEY_DAILY_STATIC_CHECKOUT_WF = """
jobs:
  test-ubuntu-jemalloc:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@de0fac2e4500dabe0009e67214ff5f5447ce83dd
        with:
          repository: ${{ inputs.use_repo || github.repository }}
          ref: ${{ inputs.use_git_ref || github.ref }}
      - name: Install libbacktrace
        uses: actions/checkout@de0fac2e4500dabe0009e67214ff5f5447ce83dd
        with:
          repository: ianlancetaylor/libbacktrace
          ref: b9e40069c0b47a722286b94eb5231f7f05c08713
          path: libbacktrace
      - run: cd libbacktrace && ./configure && make && sudo make install
      - run: make all-with-unit-tests USE_LIBBACKTRACE=yes
  test-macos-latest:
    runs-on: macos-latest
    steps:
      - uses: actions/checkout@de0fac2e4500dabe0009e67214ff5f5447ce83dd
      - name: Install libbacktrace
        uses: actions/checkout@de0fac2e4500dabe0009e67214ff5f5447ce83dd
        with:
          repository: ianlancetaylor/libbacktrace
          ref: b9e40069c0b47a722286b94eb5231f7f05c08713
          path: libbacktrace
      - run: cd libbacktrace && ./configure && make && sudo make install
      - run: make USE_LIBBACKTRACE=yes
  test-ubuntu-container:
    runs-on: ubuntu-latest
    container: ubuntu:24.04
    steps:
      - uses: actions/checkout@de0fac2e4500dabe0009e67214ff5f5447ce83dd
      - name: Install libbacktrace
        uses: actions/checkout@de0fac2e4500dabe0009e67214ff5f5447ce83dd
        with:
          repository: ianlancetaylor/libbacktrace
          ref: b9e40069c0b47a722286b94eb5231f7f05c08713
          path: libbacktrace
"""


def test_valkey_daily_static_checkout_routes_host_jobs_to_agent_runners():
    linux = classify_job_environment(
        _VALKEY_DAILY_STATIC_CHECKOUT_WF,
        "test-ubuntu-jemalloc",
    )
    macos = classify_job_environment(
        _VALKEY_DAILY_STATIC_CHECKOUT_WF,
        "test-macos-latest",
    )

    assert linux.env is VerifyEnv.LOCAL
    assert macos.env is VerifyEnv.MACOS


def test_static_checkout_stays_unsupported_in_network_disabled_container():
    env = classify_job_environment(
        _VALKEY_DAILY_STATIC_CHECKOUT_WF,
        "test-ubuntu-container",
    )

    assert env.env is VerifyEnv.UNSUPPORTED
    assert "network-disabled container" in env.reason
    assert "target-owned" in env.reason


@pytest.mark.parametrize(
    ("old", "new"),
    [
        (
            "ref: b9e40069c0b47a722286b94eb5231f7f05c08713",
            "ref: unstable",
        ),
        (
            "path: libbacktrace",
            "path: ${{ matrix.checkout_path }}",
        ),
        (
            "repository: ianlancetaylor/libbacktrace",
            "repository: ${{ matrix.repository }}",
        ),
        (
            "name: Install libbacktrace\n"
            "        uses: actions/checkout@de0fac2e4500dabe0009e67214ff5f5447ce83dd",
            "name: Install libbacktrace\n"
            "        uses: actions/checkout@v6",
        ),
    ],
)
def test_dynamic_or_unpinned_auxiliary_checkout_requires_target_verifier(old, new):
    workflow = _VALKEY_DAILY_STATIC_CHECKOUT_WF.replace(old, new, 1)
    env = classify_job_environment(workflow, "test-ubuntu-jemalloc")

    assert env.env is VerifyEnv.UNSUPPORTED
    assert "additional checkout" in env.reason
    assert "target-owned" in env.reason


def test_conditional_auxiliary_checkout_requires_target_verifier():
    workflow = _VALKEY_DAILY_STATIC_CHECKOUT_WF.replace(
        "      - name: Install libbacktrace\n",
        "      - name: Install libbacktrace\n        if: github.event_name == 'schedule'\n",
        1,
    )

    env = classify_job_environment(workflow, "test-ubuntu-jemalloc")

    assert env.env is VerifyEnv.UNSUPPORTED
    assert "additional checkout" in env.reason
    assert "target-owned" in env.reason
