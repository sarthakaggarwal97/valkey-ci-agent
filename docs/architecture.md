# Architecture

The Valkey CI Agent runs scheduled workflows that act on Valkey repositories
defined in the central `repos.yml` registry. Two workflows are active today:
backports and fuzzer monitoring.

## Layers

```text
scripts/
  backport/    Backport workflow
  fuzzer/      Fuzzer monitor workflow
  ai/          Claude Code subprocess orchestration
  common/      Shared infrastructure
repos.yml      Registry of repos, release branches, and project boards
```

## Backport Flow

```text
sweep.py (daily cron or manual dispatch)
  -> reads repos.yml and fans out one job per {repo, branch}
  -> discovers PRs from each branch's GitHub Project board
  -> for each registered release branch:
      cherry_pick.py -> git cherry-pick
      conflict_resolver.py -> Claude Code resolves conflicts
      pr_creator.py -> opens/updates PR on the upstream repo
```

### Entry Points

- `scripts/backport/sweep.py` — daily sweep across registered repos and release branches
- `scripts/backport/main.py` — single-PR backport (manual dispatch)
- `scripts/backport/matrix.py` — GitHub Actions matrix generation from `repos.yml`
- `scripts/backport/registry.py` — typed registry loader and validation
- `scripts/backport/sweep_*.py` — focused sweep support modules:
  typed sweep results, Git workspace operations, GitHub PR operations,
  GraphQL access, validation command execution, and Markdown reporting

## Fuzzer Flow

```text
fuzzer/main.py (cron every 4 hours)
  -> ArtifactClient.list_recent_runs(valkey-io/valkey-fuzzer, fuzzer-run.yml)
  -> FuzzerRunAnalyzer.analyze(run)
       artifacts.py -> download artifacts and run logs
       analyzer._scan_logs() -> deterministic pattern matching
       analyzer._invoke_claude() -> shallow-clones valkey + valkey-fuzzer at
                                    the tested SHA, runs Claude under the
                                    fuzzer_analysis_readonly profile, parses
                                    JSON verdict
       incidents.compute_fingerprint() -> stable hash for dedup
  -> FuzzerIssuePublisher.upsert_issue(...) when overall_status == anomalous
```

Claude is given `Read,Grep,Glob` only — no edits, no shell, no network. If the
clone or Claude call fails, the analyzer falls back to deterministic findings
and labels the verdict `needs-human-triage` rather than silently reporting
"normal".

Unlike the backport flow, the fuzzer monitor never writes to `valkey-io/valkey`
or `valkey-io/valkey-fuzzer` source — its only side effect is creating or
updating issues on `valkey-fuzzer`.

### Entry Points

- `scripts/fuzzer/main.py` — CLI entry point (cron / manual dispatch)
- `scripts/fuzzer/analyzer.py` — orchestration, deterministic scan, Claude Code integration
- `scripts/fuzzer/artifacts.py` — workflow run artifact and log download
- `scripts/fuzzer/issue_publisher.py` — GitHub issue create/update with fingerprint dedup
- `scripts/fuzzer/incidents.py` — fingerprint computation

## AI Layer

```text
runtime.run_agent(profile, prompt, cwd=...)
  -> claude_code.run_claude_code(...)
    -> subprocess: claude --print (Claude Code CLI via Bedrock)
```

Profiles registered today:

- `conflict_resolve_edit_only` — backport conflict resolution (Read/Edit/Bash, writes allowed)
- `fuzzer_analysis_readonly` — fuzzer triage (Read/Grep/Glob only, no writes)

## Common Infrastructure

- `git_auth.py` — GIT_ASKPASS credential helper
- `github_client.py` — retry wrapper for GitHub API
- `text_utils.py` — ANSI stripping for log scanning

## Repository Model

The standard model is direct upstream branches: the agent pushes
`agent/backport/...` branches to `repo` and opens PRs in that same
repository. This keeps the registry small and matches the GitHub App
permissions used by the workflows.

`push_repo` is optional and exists only as a different-owner fork escape hatch.
Same-owner `push_repo` values are rejected so staging repositories do not become
the normal deployment model.

## Planned Workflows

Future sibling modules to `backport/` and `fuzzer/`:

- **PR Reviewer** — two-stage code review with skeptic pass
- **Daily CI Analysis** — detect flaky tests, generate fix PRs

