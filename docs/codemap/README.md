# Code map

An interactive call graph of `scripts/`, for reading the agent rather than
running it. Pick the workflow you care about, see which function it starts at,
and walk outward through what that function calls — with the signature,
docstring, and real source of every node alongside the graph.

## Run it

```bash
python -m scripts.codemap serve      # rebuild + http://127.0.0.1:8765
python -m scripts.codemap build      # regenerate graph.json only
```

The page needs to be served over HTTP; opening `index.html` from the filesystem
fails because the browser blocks the `fetch` of `graph.json`.

## Reading the graph

- **Modules / Functions** toggle switches granularity. Modules is the 30,000-ft
  view (which subsystem leans on which); Functions is the actual call flow.
- **Entry points** lists every `python -m scripts.*` invocation found in
  `.github/workflows/`, so each root is the real start of a CI run.
- **Direction** flips between "what this calls" and "who calls this" — the
  second is the one that answers *can I safely change this?*
- **Depth** bounds how far the walk goes. Double-clicking a node re-roots the
  graph there and adds a breadcrumb.
- **Edge colour** is resolution confidence, not importance:

  | Colour | Meaning |
  | --- | --- |
  | green (`exact`) | resolved through an explicit import, module scope, or `self` |
  | yellow (`inferred`) | a method call on an untyped value whose name matches exactly one known method |
  | red (`ambiguous`) | the same method name exists on several classes; every candidate is drawn |

- Dashed edges are **bridged**: they appear when a filter hides an intermediate
  node, so the chain stays connected instead of silently truncating.
- **External effects** in the detail panel lists calls that leave our code
  (`subprocess.run`, `requests.get`, …). That is usually the fastest way to see
  what a function actually *does* to the outside world.

## What it cannot tell you

The graph is built from the AST, so it describes what **can** call what, not
what ran. Dynamic dispatch on a value whose type is not visible in the file
lands in the `inferred`/`ambiguous` buckets, and a call made purely through
`getattr` is not represented at all. On the current tree ~94% of edges resolve
exactly; treat the other 6% as leads, not facts.

## Keeping it current

`graph.json` is committed so the page works on GitHub Pages with no build step.
It therefore goes stale as `scripts/` changes — rerun
`python -m scripts.codemap build` and commit the result, or wire the same
command into a workflow that regenerates it on push.
