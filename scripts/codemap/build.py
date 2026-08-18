"""Build a static call graph of ``scripts/`` for the interactive code map.

The graph is derived purely from the AST, so it describes what *can* call what
rather than what ran in any particular workflow. Every edge carries a
confidence level so the UI can distinguish a call resolved through an explicit
import from one guessed by matching a method name.
"""

from __future__ import annotations

import ast
import json
import re
import subprocess
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

# Packages get a stable colour in the UI so the same subsystem always reads the
# same way between the module and function views.
PACKAGE_COLORS = {
    "backport": "#7c9cf5",
    "ci_fix": "#f2a35e",
    "release_notes": "#6cc4a1",
    "test_failure_detector": "#d98ec1",
    "common": "#9aa4b2",
    "ai": "#c9a0f5",
    "fuzzer": "#e4c86b",
    "codemap": "#6b8fa3",
}

EXCLUDED_DIR_PARTS = {"__pycache__", ".venv", "build", "dist"}

# Call targets that are pure noise in a walkthrough: they say nothing about the
# agent's own structure. Recorded as "external effects" instead of graph nodes.
NOISE_EXTERNALS = {
    "len",
    "str",
    "int",
    "bool",
    "float",
    "list",
    "dict",
    "set",
    "tuple",
    "sorted",
    "enumerate",
    "isinstance",
    "getattr",
    "hasattr",
    "range",
    "print",
    "min",
    "max",
    "sum",
    "any",
    "all",
    "zip",
    "repr",
    "type",
    "abs",
    "reversed",
    "format",
    "super",
    "iter",
    "next",
}

# Externals worth surfacing in the detail panel because they explain what a
# function actually *does* (shells out, calls the API, touches the filesystem).
NOTABLE_EXTERNAL_ROOTS = {
    "subprocess",
    "requests",
    "urllib",
    "os",
    "shutil",
    "pathlib",
    "Path",
    "json",
    "yaml",
    "time",
    "tempfile",
    "logging",
    "hashlib",
    "difflib",
    "re",
}

MAX_AMBIGUOUS_EDGES = 3


@dataclass
class Definition:
    """A function, method, or class defined somewhere in ``scripts/``."""

    node_id: str
    kind: str
    name: str
    qualname: str
    module: str
    package: str
    file: str
    line: int
    end_line: int
    signature: str
    doc: str
    source: str
    is_private: bool
    is_async: bool
    decorators: list[str]
    class_name: str | None
    bases: list[str] = field(default_factory=list)

    def to_json(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "id": self.node_id,
            "kind": self.kind,
            "name": self.name,
            "qualname": self.qualname,
            "module": self.module,
            "package": self.package,
            "file": self.file,
            "line": self.line,
            "endLine": self.end_line,
            "loc": self.end_line - self.line + 1,
            "signature": self.signature,
            "doc": self.doc,
            "source": self.source,
            "private": self.is_private,
        }
        if self.is_async:
            payload["async"] = True
        if self.decorators:
            payload["decorators"] = self.decorators
        if self.class_name:
            payload["class"] = self.class_name
        if self.bases:
            payload["bases"] = self.bases
        return payload


@dataclass
class ModuleInfo:
    """One parsed source file plus the tables needed to resolve its calls."""

    name: str
    package: str
    file: str
    doc: str
    loc: int
    tree: ast.Module
    source_lines: list[str]
    imports: dict[str, str] = field(default_factory=dict)
    definitions: dict[str, Definition] = field(default_factory=dict)
    entry_workflows: list[str] = field(default_factory=list)
    has_main_guard: bool = False


def _is_excluded(path: Path) -> bool:
    return any(part in EXCLUDED_DIR_PARTS for part in path.parts)


def _module_name(path: Path, root: Path) -> str:
    relative = path.relative_to(root).with_suffix("")
    parts = list(relative.parts)
    if parts and parts[-1] == "__init__":
        parts.pop()
    return ".".join(parts)


def _package_of(module: str) -> str:
    parts = module.split(".")
    return parts[1] if len(parts) > 1 else parts[0]


def _first_paragraph(docstring: str | None) -> str:
    if not docstring:
        return ""
    lines: list[str] = []
    for raw in docstring.strip().splitlines():
        stripped = raw.strip()
        if not stripped:
            break
        lines.append(stripped)
    return " ".join(lines)


def _unparse(node: ast.AST | None) -> str:
    if node is None:
        return ""
    try:
        return ast.unparse(node)
    except Exception:  # pragma: no cover - defensive, unparse is total in 3.9+
        return "..."


def _format_argument(arg: ast.arg, default: ast.expr | None) -> str:
    rendered = arg.arg
    annotation = _unparse(arg.annotation)
    if annotation:
        rendered += f": {annotation}"
    if default is not None:
        separator = " = " if annotation else "="
        rendered += f"{separator}{_unparse(default)}"
    return rendered


def _format_signature(node: ast.FunctionDef | ast.AsyncFunctionDef) -> str:
    args = node.args
    positional = list(getattr(args, "posonlyargs", [])) + list(args.args)
    defaults: list[ast.expr | None] = list(args.defaults)
    padding: list[ast.expr | None] = [None] * (len(positional) - len(defaults))
    positional_defaults = padding + defaults

    parts: list[str] = []
    posonly_count = len(getattr(args, "posonlyargs", []))
    for index, (arg, default) in enumerate(zip(positional, positional_defaults)):
        parts.append(_format_argument(arg, default))
        if posonly_count and index == posonly_count - 1:
            parts.append("/")

    if args.vararg is not None:
        parts.append(f"*{_format_argument(args.vararg, None)}")
    elif args.kwonlyargs:
        parts.append("*")

    for arg, default in zip(args.kwonlyargs, args.kw_defaults):
        parts.append(_format_argument(arg, default))

    if args.kwarg is not None:
        parts.append(f"**{_format_argument(args.kwarg, None)}")

    returns = _unparse(node.returns)
    suffix = f" -> {returns}" if returns else ""
    return f"{node.name}({', '.join(parts)}){suffix}"


def _definition_start(
    node: ast.FunctionDef | ast.AsyncFunctionDef | ast.ClassDef,
) -> int:
    """First line of a definition, counting any decorators above it."""
    lines = [node.lineno] + [decorator.lineno for decorator in node.decorator_list]
    return min(lines)


def _source_slice(lines: list[str], start: int, end: int) -> str:
    return "\n".join(lines[start - 1 : end])


def discover_modules(root: Path, repo_root: Path) -> dict[str, ModuleInfo]:
    """Parse every source file under ``root`` into a :class:`ModuleInfo`."""
    modules: dict[str, ModuleInfo] = {}
    for path in sorted(root.rglob("*.py")):
        if _is_excluded(path):
            continue
        text = path.read_text(encoding="utf-8")
        try:
            tree = ast.parse(text)
        except SyntaxError:
            continue
        name = _module_name(path, repo_root)
        lines = text.splitlines()
        modules[name] = ModuleInfo(
            name=name,
            package=_package_of(name),
            file=str(path.relative_to(repo_root)),
            doc=_first_paragraph(ast.get_docstring(tree)),
            loc=len(lines),
            tree=tree,
            source_lines=lines,
        )
    return modules


def collect_imports(module: ModuleInfo) -> None:
    """Map every local alias in a module to the dotted target it refers to."""
    for node in ast.walk(module.tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                local = alias.asname or alias.name.split(".")[0]
                target = alias.name if alias.asname else alias.name.split(".")[0]
                module.imports[local] = target
        elif isinstance(node, ast.ImportFrom):
            if node.level:
                # Relative import: rebuild the absolute package path.
                base_parts = module.name.split(".")
                trimmed = base_parts[: max(0, len(base_parts) - node.level)]
                prefix = ".".join(trimmed + ([node.module] if node.module else []))
            else:
                prefix = node.module or ""
            for alias in node.names:
                local = alias.asname or alias.name
                module.imports[local] = f"{prefix}:{alias.name}" if prefix else alias.name


def collect_definitions(module: ModuleInfo) -> None:
    """Index every function, method, and class defined in a module."""

    def record(
        node: ast.FunctionDef | ast.AsyncFunctionDef | ast.ClassDef,
        class_name: str | None,
    ) -> None:
        qualname = f"{class_name}.{node.name}" if class_name else node.name
        start = _definition_start(node)
        end = node.end_lineno or start
        if isinstance(node, ast.ClassDef):
            kind = "class"
            signature = f"class {node.name}"
            bases = [_unparse(base) for base in node.bases]
        else:
            kind = "method" if class_name else "function"
            signature = _format_signature(node)
            bases = []
        module.definitions[qualname] = Definition(
            node_id=f"{module.name}:{qualname}",
            kind=kind,
            name=node.name,
            qualname=qualname,
            module=module.name,
            package=module.package,
            file=module.file,
            line=start,
            end_line=end,
            signature=signature,
            doc=_first_paragraph(ast.get_docstring(node)),
            source=_source_slice(module.source_lines, start, end),
            is_private=node.name.startswith("_"),
            is_async=isinstance(node, ast.AsyncFunctionDef),
            decorators=[_unparse(d) for d in getattr(node, "decorator_list", [])],
            class_name=class_name,
            bases=bases,
        )

    for node in module.tree.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            record(node, None)
        elif isinstance(node, ast.ClassDef):
            record(node, None)
            for child in node.body:
                if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    record(child, node.name)
        elif isinstance(node, ast.If) and _is_main_guard(node):
            module.has_main_guard = True


def _is_main_guard(node: ast.If) -> bool:
    test = node.test
    if not isinstance(test, ast.Compare) or not isinstance(test.left, ast.Name):
        return False
    if test.left.id != "__name__":
        return False
    return any(
        isinstance(comparator, ast.Constant) and comparator.value == "__main__"
        for comparator in test.comparators
    )


class CallResolver:
    """Resolves call sites to internal definitions, with a confidence label."""

    def __init__(self, modules: dict[str, ModuleInfo]) -> None:
        self.modules = modules
        self.by_name: dict[str, list[Definition]] = {}
        for module in modules.values():
            for definition in module.definitions.values():
                self.by_name.setdefault(definition.name, []).append(definition)

    def resolve_name(self, module: ModuleInfo, name: str) -> tuple[list[str], str]:
        """Resolve a bare ``name(...)`` call."""
        if name in module.definitions:
            return [module.definitions[name].node_id], "exact"

        target = module.imports.get(name)
        if target and ":" in target:
            target_module, symbol = target.split(":", 1)
            owner = self.modules.get(target_module)
            if owner and symbol in owner.definitions:
                return [owner.definitions[symbol].node_id], "exact"
        elif target and target in self.modules:
            owner = self.modules[target]
            if name in owner.definitions:
                return [owner.definitions[name].node_id], "exact"

        return [], "external"

    def resolve_attribute(
        self,
        module: ModuleInfo,
        node: ast.Attribute,
        class_name: str | None,
    ) -> tuple[list[str], str]:
        """Resolve ``something.attr(...)``."""
        attr = node.attr
        value = node.value

        if isinstance(value, ast.Name):
            base = value.id

            if base == "self" and class_name:
                owned = module.definitions.get(f"{class_name}.{attr}")
                if owned:
                    return [owned.node_id], "exact"
                inherited = self._resolve_inherited(module, class_name, attr)
                if inherited:
                    return [inherited], "exact"

            dotted = module.imports.get(base)
            if dotted:
                owner = self.modules.get(dotted)
                if owner and attr in owner.definitions:
                    return [owner.definitions[attr].node_id], "exact"
                if ":" in dotted:
                    target_module, symbol = dotted.split(":", 1)
                    owner = self.modules.get(target_module)
                    if owner:
                        member = owner.definitions.get(f"{symbol}.{attr}")
                        if member:
                            return [member.node_id], "exact"
                    submodule = self.modules.get(f"{target_module}.{symbol}")
                    if submodule and attr in submodule.definitions:
                        return [submodule.definitions[attr].node_id], "exact"

        flattened = _unparse(value)
        if flattened in self.modules and attr in self.modules[flattened].definitions:
            return [self.modules[flattened].definitions[attr].node_id], "exact"

        return self._infer_by_name(attr)

    def _resolve_inherited(
        self, module: ModuleInfo, class_name: str, attr: str
    ) -> str | None:
        owner = module.definitions.get(class_name)
        if not owner:
            return None
        for base in owner.bases:
            candidate = module.definitions.get(f"{base}.{attr}")
            if candidate:
                return candidate.node_id
        return None

    def _infer_by_name(self, attr: str) -> tuple[list[str], str]:
        """Last resort: match the attribute against known method names."""
        candidates = [d for d in self.by_name.get(attr, []) if d.kind == "method"]
        if len(candidates) == 1:
            return [candidates[0].node_id], "inferred"
        if 1 < len(candidates) <= MAX_AMBIGUOUS_EDGES:
            return [d.node_id for d in candidates], "ambiguous"
        return [], "external"


def _external_label(node: ast.Call) -> str | None:
    """Human-readable label for a call that leaves the agent's own code."""
    func = node.func
    if isinstance(func, ast.Name):
        if func.id in NOISE_EXTERNALS:
            return None
        return func.id
    if isinstance(func, ast.Attribute):
        root = func.value
        while isinstance(root, ast.Attribute):
            root = root.value
        if isinstance(root, ast.Name):
            if root.id == "self":
                return None
            label = f"{root.id}.{func.attr}"
            if root.id in NOTABLE_EXTERNAL_ROOTS:
                return label
            return label
        return func.attr
    return None


def build_edges(
    modules: dict[str, ModuleInfo], resolver: CallResolver
) -> tuple[list[dict[str, Any]], dict[str, list[str]]]:
    """Walk every definition body and record its outgoing calls."""
    edges: dict[tuple[str, str], dict[str, Any]] = {}
    externals: dict[str, list[str]] = {}

    for module in modules.values():
        for definition in module.definitions.values():
            if definition.kind == "class":
                continue
            body = _definition_ast(module, definition)
            if body is None:
                continue
            seen_externals: list[str] = []
            for call in ast.walk(body):
                if not isinstance(call, ast.Call):
                    continue
                if isinstance(call.func, ast.Name):
                    targets, confidence = resolver.resolve_name(module, call.func.id)
                elif isinstance(call.func, ast.Attribute):
                    targets, confidence = resolver.resolve_attribute(
                        module, call.func, definition.class_name
                    )
                else:
                    targets, confidence = [], "external"

                if not targets:
                    label = _external_label(call)
                    if label and label not in seen_externals:
                        seen_externals.append(label)
                    continue

                for target in targets:
                    if target == definition.node_id:
                        continue  # skip self-recursion, it clutters the layout
                    key = (definition.node_id, target)
                    existing = edges.get(key)
                    if existing is None:
                        edges[key] = {
                            "source": definition.node_id,
                            "target": target,
                            "confidence": confidence,
                            "lines": [call.lineno],
                        }
                    else:
                        if call.lineno not in existing["lines"]:
                            existing["lines"].append(call.lineno)
                        if _confidence_rank(confidence) < _confidence_rank(
                            existing["confidence"]
                        ):
                            existing["confidence"] = confidence

            if seen_externals:
                externals[definition.node_id] = sorted(seen_externals)

    ordered = sorted(edges.values(), key=lambda edge: (edge["source"], edge["target"]))
    for edge in ordered:
        edge["lines"].sort()
    return ordered, externals


def _confidence_rank(confidence: str) -> int:
    return {"exact": 0, "inferred": 1, "ambiguous": 2, "external": 3}.get(confidence, 3)


def _definition_ast(
    module: ModuleInfo, definition: Definition
) -> ast.FunctionDef | ast.AsyncFunctionDef | None:
    """Find the AST node for an already-indexed definition."""
    for node in ast.walk(module.tree):
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        if node.name != definition.name:
            continue
        if _definition_start(node) == definition.line:
            return node
    return None


def find_entrypoints(workflow_dir: Path) -> dict[str, list[str]]:
    """Map module name -> workflow files that execute it.

    Workflows here invoke modules in several shapes -- inline
    ``python -m scripts.x``, a bash array (``args=(-m scripts.x)``), and a
    backslash continuation with ``-m scripts.x`` on its own line -- so the
    match deliberately keys off ``-m`` rather than the interpreter.
    """
    entries: dict[str, list[str]] = {}
    if not workflow_dir.is_dir():
        return entries
    module_pattern = re.compile(r"-m\s+(scripts\.[\w.]+)")
    path_pattern = re.compile(r"(scripts/[\w/]+)\.py")
    for workflow in sorted(workflow_dir.glob("*.y*ml")):
        text = workflow.read_text(encoding="utf-8")
        found = set(module_pattern.findall(text))
        found.update(match.replace("/", ".") for match in path_pattern.findall(text))
        for module in sorted(found):
            entries.setdefault(module, []).append(workflow.name)
    return entries


def build_module_edges(
    edges: list[dict[str, Any]], nodes: dict[str, Definition]
) -> list[dict[str, Any]]:
    """Aggregate function-level edges into module-level ones."""
    weights: dict[tuple[str, str], int] = {}
    for edge in edges:
        source = nodes.get(edge["source"])
        target = nodes.get(edge["target"])
        if source is None or target is None:
            continue
        if source.module == target.module:
            continue
        key = (source.module, target.module)
        weights[key] = weights.get(key, 0) + 1
    return [
        {"source": source, "target": target, "weight": weight}
        for (source, target), weight in sorted(weights.items())
    ]


def _git_commit(repo_root: Path) -> str:
    try:
        result = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            cwd=repo_root,
            capture_output=True,
            text=True,
            check=True,
        )
    except (OSError, subprocess.CalledProcessError):
        return "unknown"
    return result.stdout.strip()


def build_graph(repo_root: Path) -> dict[str, Any]:
    """Produce the full graph payload consumed by the code-map page."""
    source_root = repo_root / "scripts"
    modules = discover_modules(source_root, repo_root)
    for module in modules.values():
        collect_imports(module)
        collect_definitions(module)

    entries = find_entrypoints(repo_root / ".github" / "workflows")
    for module_name, workflows in entries.items():
        entry_module = modules.get(module_name)
        if entry_module is not None:
            entry_module.entry_workflows = workflows

    resolver = CallResolver(modules)
    edges, externals = build_edges(modules, resolver)

    definitions: dict[str, Definition] = {}
    for module in modules.values():
        for definition in module.definitions.values():
            definitions[definition.node_id] = definition

    incoming: dict[str, int] = {}
    outgoing: dict[str, int] = {}
    for edge in edges:
        outgoing[edge["source"]] = outgoing.get(edge["source"], 0) + 1
        incoming[edge["target"]] = incoming.get(edge["target"], 0) + 1

    node_payload: list[dict[str, Any]] = []
    for node_id, definition in sorted(definitions.items()):
        payload = definition.to_json()
        payload["callers"] = incoming.get(node_id, 0)
        payload["callees"] = outgoing.get(node_id, 0)
        if node_id in externals:
            payload["externals"] = externals[node_id]
        module = modules[definition.module]
        if module.entry_workflows and definition.name == "main":
            payload["entryWorkflows"] = module.entry_workflows
        node_payload.append(payload)

    module_payload = [
        {
            "id": module.name,
            "package": module.package,
            "file": module.file,
            "doc": module.doc,
            "loc": module.loc,
            "symbols": sorted(
                d.node_id for d in module.definitions.values() if d.kind != "method"
            ),
            "symbolCount": len(module.definitions),
            "entryWorkflows": module.entry_workflows,
            "hasMainGuard": module.has_main_guard,
        }
        for module in sorted(modules.values(), key=lambda m: m.name)
        if module.definitions or module.entry_workflows
    ]

    packages = sorted({module.package for module in modules.values()})
    confidence_counts: dict[str, int] = {}
    for edge in edges:
        confidence_counts[edge["confidence"]] = (
            confidence_counts.get(edge["confidence"], 0) + 1
        )

    return {
        "meta": {
            "commit": _git_commit(repo_root),
            "moduleCount": len(module_payload),
            "nodeCount": len(node_payload),
            "edgeCount": len(edges),
            "edgeConfidence": confidence_counts,
            "packages": [
                {"name": name, "color": PACKAGE_COLORS.get(name, "#8b95a5")}
                for name in packages
            ],
        },
        "modules": module_payload,
        "moduleEdges": build_module_edges(edges, definitions),
        "nodes": node_payload,
        "edges": edges,
    }


def write_graph(repo_root: Path, output: Path) -> dict[str, Any]:
    """Build the graph and write it to ``output`` as JSON."""
    graph = build_graph(repo_root)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(graph, indent=1, sort_keys=False), encoding="utf-8")
    return graph
