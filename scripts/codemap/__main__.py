"""CLI for the code map: regenerate the graph, or serve the page locally."""

from __future__ import annotations

import argparse
import functools
import http.server
import socketserver
from pathlib import Path

from scripts.codemap.build import write_graph

REPO_ROOT = Path(__file__).resolve().parents[2]
SITE_DIR = REPO_ROOT / "docs" / "codemap"
GRAPH_PATH = SITE_DIR / "graph.json"


def _build() -> None:
    graph = write_graph(REPO_ROOT, GRAPH_PATH)
    meta = graph["meta"]
    confidence = meta["edgeConfidence"]
    print(f"wrote {GRAPH_PATH.relative_to(REPO_ROOT)}")
    print(
        f"  {meta['moduleCount']} modules, {meta['nodeCount']} symbols, "
        f"{meta['edgeCount']} call edges"
    )
    for level in ("exact", "inferred", "ambiguous"):
        if level in confidence:
            print(f"  {level:<10} {confidence[level]}")


def _serve(port: int) -> None:
    handler = functools.partial(
        http.server.SimpleHTTPRequestHandler, directory=str(SITE_DIR)
    )
    socketserver.TCPServer.allow_reuse_address = True
    with socketserver.TCPServer(("127.0.0.1", port), handler) as httpd:
        print(f"code map on http://127.0.0.1:{port}/ (ctrl-c to stop)")
        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            print()


def main() -> None:
    parser = argparse.ArgumentParser(prog="python -m scripts.codemap")
    subparsers = parser.add_subparsers(dest="command")
    subparsers.add_parser("build", help="regenerate docs/codemap/graph.json")
    serve_parser = subparsers.add_parser(
        "serve", help="rebuild, then serve the page locally"
    )
    serve_parser.add_argument("--port", type=int, default=8765)
    args = parser.parse_args()

    if args.command == "serve":
        _build()
        _serve(args.port)
    else:
        _build()


if __name__ == "__main__":
    main()
