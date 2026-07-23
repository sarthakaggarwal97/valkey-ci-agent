# CI Fix Target Verifier

CI Fix can use a workflow owned by each target repository as its authoritative
verification backend. This is the preferred integration for ARM, s390x,
FreeBSD, macOS/Xcode, dynamic containers, services, reusable workflows, and
jobs with repository-specific setup.

The target workflow owns every command and environment choice. CI Fix never
sends the AI-authored shell command to this backend.

## Registry Configuration

Enable CI Fix and name a workflow plus a protected ref in `repos.yml`:

```yaml
repos:
  - repo: valkey-io/example
    ci_fix:
      enabled: true
      poll_comments: true
      verification_workflow: .github/workflows/ci-agent-verify.yml
      verification_ref: protected-verifier
      baseline_runs: 5
      flaky_verify_runs: 20
      remote_parallelism: 5
      remote_sample_timeout_minutes: 15
      remote_budget_minutes: 45
      minimum_confidence: 0.8
      allowed_branch_prefixes:
        - agent/backport/
      protected_paths:
        - .github/workflows/**
        - .github/actions/**
        - "**/CODEOWNERS"
      auto_publish_paths:
        - tests/**
```

`verification_workflow` and `verification_ref` must be set together. The
workflow file must exist on GitHub's default branch so `workflow_dispatch` can
address it, and the configured ref must contain the protected implementation
used to judge candidates. Do not point `verification_ref` at a pull request or
agent-owned branch.

When this pair is configured, CI Fix prefers the target workflow for every
candidate, whether authored or selected from trusted history. Without it, jobs
using exactly `ubuntu-latest` or `macos-latest`, plus static containers on
`ubuntu-latest`, use no-secret workflows in the agent repository. Versioned
runner labels require the target-owned verifier. Host jobs may use an
unconditional auxiliary checkout only when it is pinned to a full commit and
has a static repository and path; the diagnosis recipe recreates that checkout
and its required setup. The network-disabled container fallback does not
support auxiliary checkouts. Jobs whose setup cannot be represented faithfully
still get an authored and skeptically reviewed patch, but only as a human
handoff.

## Dispatch Contract

The workflow must declare these `workflow_dispatch` string inputs:

| Input | Meaning |
|---|---|
| `head_sha` | Gated PR SHA from the linked failed run |
| `patch_b64` | Base64 patch; empty for a clean baseline |
| `failing_run_id` | Linked failed Actions run |
| `failing_job` | Exact failed job display name confirmed through the API |
| `phase` | `baseline` or `candidate` |
| `repetition` | One-based sample index |
| `repetition_count` | Total samples required by policy |
| `correlation` | Fresh dispatch UUID |

The run name must carry the correlation marker exactly:

```yaml
run-name: "ci-fix verify [token:${{ inputs.correlation }}]"
```

The agent lists runs for the configured workflow and ref, requires this marker,
rejects runs created before the dispatch, and waits for that run ID to complete.
Only `success` and `failure` are test verdicts. Cancelled, skipped, stale, and
other conclusions are unavailable evidence and force a handoff.

The dispatch intentionally has no `verify_command`, runner, image, or arbitrary
recipe input. The target workflow maps the immutable `(failing_run_id,
failing_job)` identity to a repository-owned recipe and validates that the job
is supported. It must fail closed when it cannot resolve one unambiguous recipe.

## Workflow Requirements

The protected workflow must:

1. Validate every input before using it.
2. Resolve the failed job to a target-owned, allowlisted recipe.
3. Check out exactly `head_sha` with persisted credentials disabled.
4. Apply no patch for `baseline`; decode and apply `patch_b64` for `candidate`.
5. Recreate the job's runner, architecture, container, services, setup, matrix
   values, and toolchain through target-owned code.
6. Execute one independent sample and use the job conclusion as its verdict.
7. Use `repetition` to vary a seed or fresh worker where the recipe supports it.
8. Expose no repository write token, cloud OIDC credential, or inherited secret
   to candidate code.

A minimal outer policy is:

```yaml
permissions:
  contents: read

jobs:
  verify:
    # Runner and recipe selection must come from trusted target-owned mapping,
    # not directly from a dispatch string.
    runs-on: <target-owned selection>
    steps:
      - uses: actions/checkout@de0fac2e4500dabe0009e67214ff5f5447ce83dd # v6.0.2
        with:
          ref: ${{ inputs.head_sha }}
          persist-credentials: false
      # Validate identity, conditionally apply patch, and invoke the shared
      # target-owned recipe used by normal CI.
```

Normal CI and this verifier should call the same reusable workflow, script, or
recipe implementation. Reconstructing selected YAML steps in the agent is not
equivalent and should not be treated as exact verification.

## Sampling And Publication

CI Fix dispatches each baseline and candidate repetition separately:

- `baseline_runs` samples the unmodified `head_sha`.
- Mixed clean results classify the failure as flaky.
- All-green clean results classify it as not reproduced.
- Deterministic fixes use `CI_FIX_VERIFY_RUNS` candidate samples.
- Confirmed, diagnosed, or not-reproduced flakes use `flaky_verify_runs`.
- `remote_parallelism` caps concurrently dispatched samples. Results are
  consumed in repetition order even when runs complete out of order.
- `remote_sample_timeout_minutes` caps one run, while
  `remote_budget_minutes` caps the baseline and all candidate retries together.

Every required candidate sample must pass. An all-green or unavailable baseline
can still produce a human handoff, but never an automatic push. A failed
candidate run feeds its bounded log tail into the next authoring attempt for an
authored fix; an immutable historical port is refused.
Timeout or campaign-budget exhaustion is unavailable evidence and returns the
candidate as a human handoff instead of relying on the outer Actions job
timeout.

Even after verification, authored changes auto-publish only when every changed
path matches `auto_publish_paths` and no path matches `protected_paths`.
Historical ports are not limited to `auto_publish_paths`, but `protected_paths`
still force a human handoff. Workflow, local-action, and CODEOWNERS changes are
therefore always human handoffs.

The target verifier proves a candidate; the pull request's ordinary CI remains
the final integration check. CI Fix never merges.

The read-only permission and no-cloud-identity requirements above are a contract
implemented by target maintainers. The agent can restrict its dispatch token and
validate run identity and conclusion, but GitHub does not let it enforce the
permissions declared inside a separately owned target workflow.
