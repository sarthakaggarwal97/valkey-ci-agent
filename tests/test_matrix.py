from __future__ import annotations

import json

from scripts.backport.matrix import build_matrix


def _write_registry(tmp_path) -> str:
    path = tmp_path / "repos.yml"
    path.write_text(
        """
schema_version: 2
repos:
  - repo: org/core
    project_owner: org
    project_owner_type: organization
    language: c
    validation:
      adapter: container-argv-v1
      image: "gcc@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
      platform: linux/amd64
      network: none
      resources:
        cpus: 1
        memory_mb: 512
        pids: 64
        output_bytes: 65536
        tmpfs_mb: 64
      default_commands: [build]
      commands:
        - id: build
          argv: ["make", "test"]
          working_directory: "."
          timeout_seconds: 600
          inputs: ["**"]
          expected_artifacts: []
      rules: []
    branches:
      - branch: "1.0"
        project_number: 1
      - branch: "2.0"
        project_number: 2
  - repo: org/module
    project_owner: org
    project_owner_type: organization
    language: c++
    validation_waiver:
      reason: Unit-test module has no build system.
      approved_by: test suite
      expires: "2099-01-01"
    branches:
      - branch: "1.0"
        project_number: 3
""",
        encoding="utf-8",
    )
    return str(path)


def test_build_matrix_emits_one_leg_per_registered_branch(tmp_path) -> None:
    matrix = build_matrix(_write_registry(tmp_path))

    assert [entry["repo"] for entry in matrix["include"]] == [
        "org/core",
        "org/core",
        "org/module",
    ]
    assert matrix["include"][0]["branch"] == "1.0"
    assert matrix["include"][0]["repo_slug"] == "org-core"
    assert matrix["include"][0]["project_number"] == 1
    assert matrix["include"][0]["push_repo"] == "org/core"
    assert matrix["include"][0]["language"] == "c"
    validation = json.loads(matrix["include"][0]["validation_json"])
    assert validation["commands"][0]["argv"] == ["make", "test"]
    assert json.loads(matrix["include"][0]["validation_waiver_json"]) is None


def test_build_matrix_filters_by_repo_and_project_number(tmp_path) -> None:
    matrix = build_matrix(
        _write_registry(tmp_path),
        repo_filter="org/core",
        project_number_filter=2,
    )

    assert matrix == {
        "include": [
            {
                "repo": "org/core",
                "repo_slug": "org-core",
                "project_owner": "org",
                "project_owner_type": "organization",
                "project_number": 2,
                "branch": "2.0",
                "push_repo": "org/core",
                "language": "c",
                "validation_json": json.dumps(
                    json.loads(build_matrix(_write_registry(tmp_path))["include"][0]["validation_json"])
                ),
                "validation_waiver_json": json.dumps(None),
            }
        ]
    }
