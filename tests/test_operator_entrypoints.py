from __future__ import annotations

from scripts.backport import main as backport_main
from scripts.backport import poller, sweep
from scripts.ci_fix import main as ci_fix_main
from scripts.fuzzer import main as fuzzer_main


def test_backport_main_routes_to_phased_engine(monkeypatch) -> None:
    calls: list[list[str] | None] = []
    monkeypatch.setattr(
        backport_main.phased,
        "main",
        lambda argv=None: calls.append(argv) or 7,
    )

    assert backport_main.main(["validate", "--artifact-directory", "artifact"]) == 7
    assert calls == [["validate", "--artifact-directory", "artifact"]]


def test_ci_fix_main_routes_to_phased_engine(monkeypatch) -> None:
    calls: list[list[str] | None] = []
    monkeypatch.setattr(
        ci_fix_main.phased,
        "main",
        lambda argv=None: calls.append(argv) or 8,
    )

    assert ci_fix_main.main(["prepare", "--artifact-directory", "artifact"]) == 8
    assert calls == [["prepare", "--artifact-directory", "artifact"]]


def test_fuzzer_main_routes_to_phased_engine(monkeypatch) -> None:
    calls: list[list[str] | None] = []
    monkeypatch.setattr(
        fuzzer_main.phased,
        "main",
        lambda argv=None: calls.append(argv) or 9,
    )

    assert fuzzer_main.main(["analyze", "--artifact-directory", "artifact"]) == 9
    assert calls == [["analyze", "--artifact-directory", "artifact"]]


def test_sweep_routes_every_operation_to_its_hardened_engine(monkeypatch) -> None:
    calls: list[tuple[str, list[str] | None]] = []
    monkeypatch.setattr(
        sweep.candidate_matrix,
        "main",
        lambda argv=None: calls.append(("candidates", argv)) or 1,
    )
    monkeypatch.setattr(
        sweep.phased,
        "main",
        lambda argv=None: calls.append(("candidate", argv)) or 2,
    )
    monkeypatch.setattr(
        sweep.aggregate,
        "main",
        lambda argv=None: calls.append(("aggregate", argv)) or 3,
    )

    assert sweep.main(["candidates", "--repo", "org/repo"]) == 1
    assert sweep.main(["candidate", "validate", "--artifact-directory", "a"]) == 2
    assert sweep.main(["aggregate", "validate", "--artifact-directory", "b"]) == 3
    assert calls == [
        ("candidates", ["--repo", "org/repo"]),
        ("candidate", ["validate", "--artifact-directory", "a"]),
        ("aggregate", ["validate", "--artifact-directory", "b"]),
    ]


def test_poller_retains_operator_path_and_uses_sweep_router(monkeypatch) -> None:
    calls: list[list[str] | None] = []
    monkeypatch.setattr(
        poller.sweep,
        "main",
        lambda argv=None: calls.append(argv) or 4,
    )

    assert poller.main(["candidates", "--max-candidates", "2"]) == 4
    assert calls == [["candidates", "--max-candidates", "2"]]
