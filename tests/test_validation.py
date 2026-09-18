"""Tests for path-based validation command selection."""

from __future__ import annotations

import subprocess

from scripts.backport.registry import ValidationRule
from scripts.backport.validation import (
    changed_paths_since_base,
    select_validation_commands,
)


def test_select_validation_commands_appends_matching_rules_once() -> None:
    commands = select_validation_commands(
        ["make"],
        [
            ValidationRule(paths=("src/cluster_legacy.c",), commands=("cluster-smoke",)),
            ValidationRule(paths=("tests/unit/cluster/*.tcl",), commands=("cluster-smoke", "tcl-smoke")),
            ValidationRule(paths=("src/networking.c",), commands=("network-smoke",)),
        ],
        ["tests/unit/cluster/cli.tcl", "README.md"],
    )

    assert commands == ["make", "cluster-smoke", "tcl-smoke"]


def test_changed_paths_since_base_uses_merge_base(tmp_path) -> None:
    subprocess.run(["git", "init"], cwd=tmp_path, check=True, capture_output=True)
    subprocess.run(["git", "config", "user.name", "test"], cwd=tmp_path, check=True)
    subprocess.run(["git", "config", "user.email", "test@example.com"], cwd=tmp_path, check=True)
    (tmp_path / "base.txt").write_text("base\n", encoding="utf-8")
    subprocess.run(["git", "add", "base.txt"], cwd=tmp_path, check=True)
    subprocess.run(["git", "commit", "-m", "base"], cwd=tmp_path, check=True, capture_output=True)
    subprocess.run(["git", "branch", "base"], cwd=tmp_path, check=True)

    (tmp_path / "changed.txt").write_text("changed\n", encoding="utf-8")
    subprocess.run(["git", "add", "changed.txt"], cwd=tmp_path, check=True)
    subprocess.run(["git", "commit", "-m", "changed"], cwd=tmp_path, check=True, capture_output=True)

    assert changed_paths_since_base(str(tmp_path), "base") == ("changed.txt",)


def test_valkey_profile_runs_changed_tests_format_and_subsystem_checks(tmp_path) -> None:
    (tmp_path / "src").mkdir()
    (tmp_path / "src/.clang-format").write_text("BasedOnStyle: LLVM\n", encoding="utf-8")
    for path in (
        "src/rdb.c",
        "tests/integration/corrupt-dump.tcl",
        "tests/unit/cluster/packet.tcl",
    ):
        destination = tmp_path / path
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_text("test body\n", encoding="utf-8")

    commands = select_validation_commands(
        ["make -j4 BUILD_TLS=yes"],
        [ValidationRule(paths=("src/rdb.c",), commands=("rdb-smoke",))],
        [
            "src/rdb.c",
            "tests/integration/corrupt-dump.tcl",
            "tests/unit/cluster/packet.tcl",
        ],
        validation_profile="valkey-core",
        repo_dir=str(tmp_path),
    )

    assert commands[0] == "git diff --check"
    assert "clang-format-18 --dry-run --Werror -- src/rdb.c" in commands
    assert "make -j4 BUILD_TLS=yes" in commands
    assert "rdb-smoke" in commands
    assert "./runtest --single integration/corrupt-dump --clients 1" in commands
    assert "./runtest --single unit/cluster/packet --clients 1" in commands
    assert any("-DLOG_REQ_RES" in command for command in commands)
    assert any("--log-req-res" in command for command in commands)
    assert commands[-1].startswith("./utils/req-res-log-validator.py")


def test_valkey_profile_only_clang_formats_paths_upstream_formats(tmp_path) -> None:
    """Upstream runs clang-format inside src/ only, using src/.clang-format.

    Checking a C file outside src/ falls back to clang's built-in LLVM style and
    fails a candidate whose upstream CI is green.
    """
    for path in (
        "src/.clang-format",
        "src/rdb.c",
        "tests/modules/basics.c",
        "deps/lua/src/lapi.c",
    ):
        destination = tmp_path / path
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_text("BasedOnStyle: LLVM\n", encoding="utf-8")

    commands = select_validation_commands(
        [],
        [],
        ["src/rdb.c", "tests/modules/basics.c", "deps/lua/src/lapi.c"],
        validation_profile="valkey-core",
        repo_dir=str(tmp_path),
    )

    formatting = [command for command in commands if command.startswith("clang-format-18")]
    assert formatting == ["clang-format-18 --dry-run --Werror -- src/rdb.c"]


def test_valkey_profile_skips_clang_format_when_release_has_no_style(tmp_path) -> None:
    """Valkey 7.2 has no src/.clang-format, so clang's LLVM default is unrelated."""
    source = tmp_path / "src/server.c"
    source.parent.mkdir(parents=True)
    source.write_text("void f(void) {}\n", encoding="utf-8")

    commands = select_validation_commands(
        [],
        [],
        ["src/server.c"],
        validation_profile="valkey-core",
        repo_dir=str(tmp_path),
    )

    assert not any(command.startswith("clang-format-18") for command in commands)


def test_valkey_profile_runs_a_smoke_set_for_harness_changes(tmp_path) -> None:
    """A whole serialized suite cannot finish inside the per-command timeout.

    Upstream budgets 24 hours and three shards for that run, so a tests/support
    change is proven with a fixed smoke set instead.
    """
    commands = select_validation_commands(
        [],
        [],
        ["tests/support/util.tcl"],
        validation_profile="valkey-core",
        repo_dir=str(tmp_path),
    )

    assert commands == [
        "git diff --check",
        "./runtest --single unit/other --single unit/keyspace --single unit/dump "
        "--single unit/protocol --single unit/type/incr --clients 1 --tags -slow",
    ]
    assert "./runtest --clients 1" not in commands


def test_valkey_profile_targets_moduleapi_units_that_load_the_module(tmp_path) -> None:
    """Module sources map to units by the .so path they name, not by basename.

    tests/modules/defragtest.c is only exercised by defrag.tcl, so a basename
    rule would silently skip the one unit that covers the change.
    """
    modules_dir = tmp_path / "tests/unit/moduleapi"
    modules_dir.mkdir(parents=True)
    (modules_dir / "defrag.tcl").write_text(
        "set testmodule [file normalize tests/modules/defragtest.so]\n",
        encoding="utf-8",
    )
    (modules_dir / "hash.tcl").write_text(
        "set testmodule [file normalize tests/modules/hash.so]\n",
        encoding="utf-8",
    )
    (tmp_path / "tests/modules").mkdir(parents=True)

    commands = select_validation_commands(
        [],
        [],
        ["tests/modules/defragtest.c"],
        validation_profile="valkey-core",
        repo_dir=str(tmp_path),
    )

    assert "./runtest-moduleapi --single unit/moduleapi/defrag --clients 1" in commands
    assert not any(command.rstrip() == "./runtest-moduleapi --clients 1" for command in commands)
    assert any(
        command.startswith("CFLAGS='-Werror' ./runtest-moduleapi --single unit/moduleapi/defrag ")
        for command in commands
    )


def test_valkey_profile_falls_back_to_whole_moduleapi_suite_when_unmappable(tmp_path) -> None:
    """Module build infrastructure cannot be narrowed to a unit, so run everything.

    The fallback drops --clients 1 as well: the whole suite serialized at one
    client is exactly the shape that cannot finish inside the timeout.
    """
    (tmp_path / "tests/unit/moduleapi").mkdir(parents=True)

    commands = select_validation_commands(
        [],
        [],
        ["tests/modules/Makefile"],
        validation_profile="valkey-core",
        repo_dir=str(tmp_path),
    )

    assert "./runtest-moduleapi" in commands
    assert "./runtest-moduleapi --clients 1" not in commands


def test_valkey_profile_pre_cleans_when_no_runtest_leg_wrote_logs(tmp_path) -> None:
    """--dont-pre-clean only exists to preserve logs a preceding ./runtest wrote.

    Kept unconditionally, a previous candidate's tests/tmp reqres files survive
    into this run and get validated against this branch's schemas.
    """
    (tmp_path / "tests/unit/moduleapi").mkdir(parents=True)
    (tmp_path / "tests/unit/moduleapi/hash.tcl").write_text(
        "set testmodule [file normalize tests/modules/hash.so]\n",
        encoding="utf-8",
    )

    commands = select_validation_commands(
        [],
        [],
        ["tests/modules/hash.c"],
        validation_profile="valkey-core",
        repo_dir=str(tmp_path),
    )

    reply = [command for command in commands if "--log-req-res" in command]
    assert len(reply) == 1
    assert "--dont-pre-clean" not in reply[0]


def test_valkey_profile_does_not_reply_log_top_level_skipped_test(tmp_path) -> None:
    test_path = tmp_path / "tests/integration/corrupt-dump-fuzzer.tcl"
    test_path.parent.mkdir(parents=True)
    test_path.write_text(
        'tags {"dump" "logreqres:skip"} {\n    test body\n}\n',
        encoding="utf-8",
    )

    commands = select_validation_commands(
        [],
        [],
        ["tests/integration/corrupt-dump-fuzzer.tcl"],
        validation_profile="valkey-core",
        repo_dir=str(tmp_path),
    )

    assert "./runtest --single integration/corrupt-dump-fuzzer --clients 1" in commands
    assert not any("--log-req-res" in command for command in commands)


def test_valkey_profile_uses_module_runner_for_moduleapi_test(tmp_path) -> None:
    test_path = tmp_path / "tests/unit/moduleapi/blockonkeys.tcl"
    test_path.parent.mkdir(parents=True)
    test_path.write_text("test body\n", encoding="utf-8")

    commands = select_validation_commands(
        [],
        [],
        ["tests/unit/moduleapi/blockonkeys.tcl"],
        validation_profile="valkey-core",
        repo_dir=str(tmp_path),
    )

    assert "./runtest-moduleapi --single unit/moduleapi/blockonkeys --clients 1" in commands
    assert "./runtest --single unit/moduleapi/blockonkeys --clients 1" not in commands
    assert any(
        command.startswith("CFLAGS='-Werror' ./runtest-moduleapi")
        and "--log-req-res" in command
        for command in commands
    )


def test_valkey_profile_targets_moduleapi_directly_with_legacy_wrapper(tmp_path) -> None:
    """The 7.2/8.0 wrapper appends selection after a hard-coded full suite."""
    test_path = tmp_path / "tests/unit/moduleapi/blockonkeys.tcl"
    test_path.parent.mkdir(parents=True)
    test_path.write_text("test body\n", encoding="utf-8")
    (tmp_path / "runtest-moduleapi").write_text(
        "$MAKE -C tests/modules && tclsh tests/test_helper.tcl "
        "--single unit/moduleapi/basics \"${@}\"\n",
        encoding="utf-8",
    )

    commands = select_validation_commands(
        [],
        [],
        ["tests/unit/moduleapi/blockonkeys.tcl"],
        validation_profile="valkey-core",
        repo_dir=str(tmp_path),
    )

    assert (
        "make -C tests/modules && ./runtest "
        "--single unit/moduleapi/blockonkeys --clients 1"
    ) in commands
    assert any(
        command.startswith(
            "CFLAGS='-Werror' make -C tests/modules && ./runtest "
            "--single unit/moduleapi/blockonkeys --clients 1 "
        )
        for command in commands
    )
    assert not any(
        command.startswith("./runtest-moduleapi --single") for command in commands
    )


def test_valkey_profile_uses_existing_cluster_smoke_on_legacy_branch(tmp_path) -> None:
    """7.2's cluster runner rejects --clients and has a different test namespace."""
    source = tmp_path / "src/cluster.c"
    source.parent.mkdir(parents=True)
    source.write_text("void cluster(void) {}\n", encoding="utf-8")
    smoke = tmp_path / "tests/unit/cluster/misc.tcl"
    smoke.parent.mkdir(parents=True)
    smoke.write_text("test body\n", encoding="utf-8")

    commands = select_validation_commands(
        [],
        [],
        ["src/cluster.c"],
        validation_profile="valkey-core",
        repo_dir=str(tmp_path),
    )

    assert "./runtest --single unit/cluster/misc --clients 1" in commands
    assert not any(command.startswith("./runtest-cluster") for command in commands)
