from __future__ import annotations

import json
from pathlib import Path

from scripts.codemap.build import (
    build_graph,
    collect_definitions,
    collect_imports,
    discover_modules,
    find_entrypoints,
    write_graph,
)


def _write(root: Path, relative: str, body: str) -> Path:
    path = root / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(body.lstrip("\n"), encoding="utf-8")
    return path


def _repo(tmp_path: Path, files: dict[str, str], workflows: dict[str, str] | None = None) -> Path:
    for relative, body in files.items():
        _write(tmp_path, relative, body)
    for name, body in (workflows or {}).items():
        _write(tmp_path, f".github/workflows/{name}", body)
    return tmp_path


def _modules(tmp_path: Path):
    modules = discover_modules(tmp_path / "scripts", tmp_path)
    for module in modules.values():
        collect_imports(module)
        collect_definitions(module)
    return modules


def _edge(graph: dict, source: str, target: str) -> dict | None:
    for edge in graph["edges"]:
        if edge["source"] == source and edge["target"] == target:
            return edge
    return None


def _node(graph: dict, node_id: str) -> dict | None:
    for node in graph["nodes"]:
        if node["id"] == node_id:
            return node
    return None


# --------------------------------------------------------------- imports


def test_collect_imports_records_every_alias_form(tmp_path: Path):
    _repo(
        tmp_path,
        {
            "scripts/__init__.py": "",
            "scripts/pkg/__init__.py": "",
            "scripts/pkg/helpers.py": "def helper():\n    pass\n",
            "scripts/pkg/entry.py": """
import os
import scripts.pkg.helpers as helpers
from scripts.pkg.helpers import helper
from scripts.pkg.helpers import helper as aliased
from . import helpers as relative_helpers
""",
        },
    )
    imports = _modules(tmp_path)["scripts.pkg.entry"].imports

    assert imports["os"] == "os"
    assert imports["helpers"] == "scripts.pkg.helpers"
    assert imports["helper"] == "scripts.pkg.helpers:helper"
    assert imports["aliased"] == "scripts.pkg.helpers:helper"
    assert imports["relative_helpers"] == "scripts.pkg:helpers"


# ------------------------------------------------------------ resolution


def test_resolves_direct_and_module_qualified_calls(tmp_path: Path):
    _repo(
        tmp_path,
        {
            "scripts/__init__.py": "",
            "scripts/pkg/__init__.py": "",
            "scripts/pkg/helpers.py": "def helper():\n    pass\n\n\ndef other():\n    pass\n",
            "scripts/pkg/entry.py": """
import scripts.pkg.helpers as helpers
from scripts.pkg.helpers import helper


def local():
    pass


def run():
    local()
    helper()
    helpers.other()
""",
        },
    )
    graph = build_graph(tmp_path)
    run = "scripts.pkg.entry:run"

    for target in (
        "scripts.pkg.entry:local",
        "scripts.pkg.helpers:helper",
        "scripts.pkg.helpers:other",
    ):
        edge = _edge(graph, run, target)
        assert edge is not None, f"missing edge to {target}"
        assert edge["confidence"] == "exact"


def test_resolves_self_and_inherited_methods(tmp_path: Path):
    _repo(
        tmp_path,
        {
            "scripts/__init__.py": "",
            "scripts/pkg/__init__.py": "",
            "scripts/pkg/svc.py": """
class Base:
    def shared(self):
        pass


class Child(Base):
    def own(self):
        pass

    def run(self):
        self.own()
        self.shared()
""",
        },
    )
    graph = build_graph(tmp_path)
    run = "scripts.pkg.svc:Child.run"

    own = _edge(graph, run, "scripts.pkg.svc:Child.own")
    inherited = _edge(graph, run, "scripts.pkg.svc:Base.shared")
    assert own is not None and own["confidence"] == "exact"
    assert inherited is not None and inherited["confidence"] == "exact"


def test_unique_method_name_is_inferred_and_duplicates_are_ambiguous(tmp_path: Path):
    _repo(
        tmp_path,
        {
            "scripts/__init__.py": "",
            "scripts/pkg/__init__.py": "",
            "scripts/pkg/models.py": """
class Only:
    def unique_method(self):
        pass


class First:
    def shared_name(self):
        pass


class Second:
    def shared_name(self):
        pass
""",
            "scripts/pkg/entry.py": """
def run(thing, other):
    thing.unique_method()
    other.shared_name()
""",
        },
    )
    graph = build_graph(tmp_path)
    run = "scripts.pkg.entry:run"

    inferred = _edge(graph, run, "scripts.pkg.models:Only.unique_method")
    assert inferred is not None
    assert inferred["confidence"] == "inferred"

    for owner in ("First", "Second"):
        edge = _edge(graph, run, f"scripts.pkg.models:{owner}.shared_name")
        assert edge is not None
        assert edge["confidence"] == "ambiguous"


def test_self_recursion_is_not_an_edge(tmp_path: Path):
    _repo(
        tmp_path,
        {
            "scripts/__init__.py": "",
            "scripts/pkg/__init__.py": "",
            "scripts/pkg/entry.py": """
def walk(depth):
    if depth:
        walk(depth - 1)
""",
        },
    )
    graph = build_graph(tmp_path)
    assert _edge(graph, "scripts.pkg.entry:walk", "scripts.pkg.entry:walk") is None


def test_call_sites_are_recorded_and_deduplicated(tmp_path: Path):
    _repo(
        tmp_path,
        {
            "scripts/__init__.py": "",
            "scripts/pkg/__init__.py": "",
            "scripts/pkg/entry.py": """
def helper():
    pass


def run(flag):
    helper()
    if flag:
        helper()
""",
        },
    )
    graph = build_graph(tmp_path)
    edge = _edge(graph, "scripts.pkg.entry:run", "scripts.pkg.entry:helper")

    assert edge is not None
    assert edge["lines"] == [6, 8]


# ------------------------------------------------------------- externals


def test_notable_externals_recorded_and_builtin_noise_dropped(tmp_path: Path):
    _repo(
        tmp_path,
        {
            "scripts/__init__.py": "",
            "scripts/pkg/__init__.py": "",
            "scripts/pkg/entry.py": """
import subprocess


def run(items):
    subprocess.run(["git", "status"])
    return len(list(items))
""",
        },
    )
    graph = build_graph(tmp_path)
    node = _node(graph, "scripts.pkg.entry:run")

    assert node is not None
    assert "subprocess.run" in node["externals"]
    assert not any(name in node["externals"] for name in ("len", "list"))


# ----------------------------------------------------------- entrypoints


def test_find_entrypoints_handles_every_workflow_invocation_shape(tmp_path: Path):
    inline = "jobs:\n  a:\n    steps:\n      - run: python -m scripts.alpha.main\n"
    array = (
        "jobs:\n  b:\n    steps:\n      - run: |\n"
        "          args=(-m scripts.beta.main)\n"
        '          python "${args[@]}"\n'
    )
    continuation = (
        "jobs:\n  c:\n    steps:\n      - run: |\n"
        "          python \\\n"
        "            -m scripts.gamma.main \\\n"
        "            --flag\n"
    )
    script_path = "jobs:\n  d:\n    steps:\n      - run: python scripts/delta/run.py\n"

    _repo(
        tmp_path,
        {"scripts/__init__.py": ""},
        workflows={
            "inline.yml": inline,
            "array.yml": array,
            "continuation.yml": continuation,
            "path.yml": script_path,
        },
    )
    entries = find_entrypoints(tmp_path / ".github" / "workflows")

    assert entries["scripts.alpha.main"] == ["inline.yml"]
    assert entries["scripts.beta.main"] == ["array.yml"]
    assert entries["scripts.gamma.main"] == ["continuation.yml"]
    assert entries["scripts.delta.run"] == ["path.yml"]


def test_entry_module_and_main_are_tagged_with_workflows(tmp_path: Path):
    _repo(
        tmp_path,
        {
            "scripts/__init__.py": "",
            "scripts/pkg/__init__.py": "",
            "scripts/pkg/main.py": """
def main():
    pass


if __name__ == "__main__":
    main()
""",
        },
        workflows={"run.yml": "- run: python -m scripts.pkg.main\n"},
    )
    graph = build_graph(tmp_path)

    module = next(m for m in graph["modules"] if m["id"] == "scripts.pkg.main")
    assert module["entryWorkflows"] == ["run.yml"]
    assert module["hasMainGuard"] is True

    node = _node(graph, "scripts.pkg.main:main")
    assert node is not None
    assert node["entryWorkflows"] == ["run.yml"]


def test_find_entrypoints_tolerates_missing_workflow_dir(tmp_path: Path):
    assert find_entrypoints(tmp_path / "nope") == {}


# ------------------------------------------------------------ signatures


def test_signature_renders_annotations_defaults_and_special_args(tmp_path: Path):
    _repo(
        tmp_path,
        {
            "scripts/__init__.py": "",
            "scripts/pkg/__init__.py": "",
            "scripts/pkg/entry.py": """
def plain(a, b=1):
    pass


def typed(a: int, *rest: str, flag: bool = False, **kwargs: object) -> dict:
    pass


def kwonly(a, *, b, c=2):
    pass
""",
        },
    )
    definitions = _modules(tmp_path)["scripts.pkg.entry"].definitions

    assert definitions["plain"].signature == "plain(a, b=1)"
    assert definitions["typed"].signature == (
        "typed(a: int, *rest: str, flag: bool = False, **kwargs: object) -> dict"
    )
    assert definitions["kwonly"].signature == "kwonly(a, *, b, c=2)"


def test_source_slice_includes_decorators(tmp_path: Path):
    _repo(
        tmp_path,
        {
            "scripts/__init__.py": "",
            "scripts/pkg/__init__.py": "",
            "scripts/pkg/entry.py": """
from dataclasses import dataclass


@dataclass
class Payload:
    value: int
""",
        },
    )
    payload = _modules(tmp_path)["scripts.pkg.entry"].definitions["Payload"]

    assert payload.source.startswith("@dataclass")
    assert payload.decorators == ["dataclass"]
    assert payload.line == 4


def test_docstring_first_paragraph_only(tmp_path: Path):
    _repo(
        tmp_path,
        {
            "scripts/__init__.py": "",
            "scripts/pkg/__init__.py": "",
            "scripts/pkg/entry.py": '''
def run():
    """First line continues here.

    Second paragraph is dropped.
    """
''',
        },
    )
    definition = _modules(tmp_path)["scripts.pkg.entry"].definitions["run"]

    assert definition.doc == "First line continues here."


# ---------------------------------------------------------- module graph


def test_module_edges_aggregate_call_counts(tmp_path: Path):
    _repo(
        tmp_path,
        {
            "scripts/__init__.py": "",
            "scripts/pkg/__init__.py": "",
            "scripts/pkg/helpers.py": "def one():\n    pass\n\n\ndef two():\n    pass\n",
            "scripts/pkg/entry.py": """
from scripts.pkg.helpers import one, two


def run():
    one()
    two()
""",
        },
    )
    graph = build_graph(tmp_path)
    edges = [
        edge
        for edge in graph["moduleEdges"]
        if edge["source"] == "scripts.pkg.entry" and edge["target"] == "scripts.pkg.helpers"
    ]

    assert len(edges) == 1
    assert edges[0]["weight"] == 2


def test_intra_module_calls_are_not_module_edges(tmp_path: Path):
    _repo(
        tmp_path,
        {
            "scripts/__init__.py": "",
            "scripts/pkg/__init__.py": "",
            "scripts/pkg/entry.py": "def helper():\n    pass\n\n\ndef run():\n    helper()\n",
        },
    )
    graph = build_graph(tmp_path)
    assert graph["moduleEdges"] == []


def test_caller_and_callee_counts_are_reported(tmp_path: Path):
    _repo(
        tmp_path,
        {
            "scripts/__init__.py": "",
            "scripts/pkg/__init__.py": "",
            "scripts/pkg/entry.py": """
def leaf():
    pass


def middle():
    leaf()


def run():
    middle()
    leaf()
""",
        },
    )
    graph = build_graph(tmp_path)

    leaf = _node(graph, "scripts.pkg.entry:leaf")
    run = _node(graph, "scripts.pkg.entry:run")
    assert leaf is not None and leaf["callers"] == 2 and leaf["callees"] == 0
    assert run is not None and run["callers"] == 0 and run["callees"] == 2


def test_syntax_errors_do_not_abort_the_build(tmp_path: Path):
    _repo(
        tmp_path,
        {
            "scripts/__init__.py": "",
            "scripts/pkg/__init__.py": "",
            "scripts/pkg/broken.py": "def oops(:\n",
            "scripts/pkg/entry.py": "def run():\n    pass\n",
        },
    )
    graph = build_graph(tmp_path)

    assert _node(graph, "scripts.pkg.entry:run") is not None
    assert not any(node["module"] == "scripts.pkg.broken" for node in graph["nodes"])


def test_pycache_is_excluded(tmp_path: Path):
    _repo(
        tmp_path,
        {
            "scripts/__init__.py": "",
            "scripts/pkg/__init__.py": "",
            "scripts/pkg/__pycache__/stale.py": "def ghost():\n    pass\n",
            "scripts/pkg/entry.py": "def run():\n    pass\n",
        },
    )
    graph = build_graph(tmp_path)
    assert not any(node["name"] == "ghost" for node in graph["nodes"])


# ----------------------------------------------------------------- output


def test_write_graph_emits_loadable_json(tmp_path: Path):
    _repo(
        tmp_path,
        {
            "scripts/__init__.py": "",
            "scripts/pkg/__init__.py": "",
            "scripts/pkg/entry.py": "def run():\n    pass\n",
        },
    )
    output = tmp_path / "site" / "graph.json"
    written = write_graph(tmp_path, output)
    reloaded = json.loads(output.read_text(encoding="utf-8"))

    assert reloaded["meta"]["nodeCount"] == written["meta"]["nodeCount"]
    assert {"meta", "modules", "moduleEdges", "nodes", "edges"} <= set(reloaded)


def test_graph_is_byte_identical_for_identical_sources(tmp_path: Path):
    """The committed graph must not churn on unrelated commits.

    It is a 1.6 MB tracked artifact, so anything that varies per build -- a git
    SHA, a timestamp, set iteration order -- shows up as a diff on every commit
    and on every ``serve``. Identical sources must produce identical bytes.
    """
    _repo(
        tmp_path,
        {
            "scripts/__init__.py": "",
            "scripts/pkg/__init__.py": "",
            "scripts/pkg/helpers.py": "def one():\n    pass\n\n\ndef two():\n    pass\n",
            "scripts/pkg/entry.py": (
                "from scripts.pkg.helpers import one, two\n\n\n"
                "def run():\n    one()\n    two()\n"
            ),
        },
        workflows={"run.yml": "- run: python -m scripts.pkg.entry\n"},
    )
    first = write_graph(tmp_path, tmp_path / "a.json")
    second = write_graph(tmp_path, tmp_path / "b.json")

    assert (tmp_path / "a.json").read_bytes() == (tmp_path / "b.json").read_bytes()
    assert first == second


def test_graph_metadata_carries_no_git_state(tmp_path: Path):
    _repo(
        tmp_path,
        {
            "scripts/__init__.py": "",
            "scripts/pkg/__init__.py": "",
            "scripts/pkg/entry.py": "def run():\n    pass\n",
        },
    )
    meta = build_graph(tmp_path)["meta"]

    assert set(meta) == {
        "moduleCount",
        "nodeCount",
        "edgeCount",
        "edgeConfidence",
        "packages",
    }


def test_real_repo_graph_is_well_formed():
    """The generator must stay consistent against this repository's own source."""
    graph = build_graph(Path("."))
    ids = {node["id"] for node in graph["nodes"]}

    assert graph["meta"]["nodeCount"] > 500
    assert graph["meta"]["edgeCount"] > 500

    for edge in graph["edges"]:
        assert edge["source"] in ids
        assert edge["target"] in ids
        assert edge["confidence"] in {"exact", "inferred", "ambiguous"}

    module_ids = {module["id"] for module in graph["modules"]}
    for edge in graph["moduleEdges"]:
        assert edge["source"] in module_ids
        assert edge["target"] in module_ids

    # Every workflow entry point must resolve to a real main().
    entries = [node for node in graph["nodes"] if node.get("entryWorkflows")]
    assert len(entries) >= 10
    assert all(node["name"] == "main" for node in entries)
