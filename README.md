# valkey-ci-bot
An AI bot for Valkey CI failure remediation and PR review

## PR review bot

The repository also includes a reusable PR reviewer workflow at `.github/workflows/review-pr.yml`.

It reviews pull requests through the GitHub API without checking out PR head code in the privileged workflow. The reviewer can:

- post or update a PR summary comment
- generate optional release notes
- publish focused review comments
- answer follow-up `/reviewbot` questions in PR comments and review threads

Example consumer-repo files:

- `examples/pr-review-caller-workflow.yml`
- `examples/pr-review-config.yml`

## Bedrock KB refresh

The repository includes a scheduled GitHub Actions workflow at `.github/workflows/refresh-bedrock-kb.yml`.

It runs a full Bedrock knowledge-base refresh every 6 hours and can also be started manually with `workflow_dispatch`.

Required repository secrets:

- `AWS_ACCESS_KEY_ID`
- `AWS_SECRET_ACCESS_KEY`
- `AWS_SESSION_TOKEN` if your AWS credentials are session-based
- `AWS_REGION` if you do not want the workflow default of `us-east-1`

What the workflow does:

- refreshes the Valkey code KB from the current `unstable` branch using Bedrock custom-document ingestion
- deletes stale code documents that disappeared from the source branch
- refreshes the Valkey documentation KB through the dedicated web crawler data source

Manual run options:

- `missing_only=true` resumes a partial code refresh without resending already-present document IDs. This is for recovery, not normal freshness, because a full refresh is what updates changed file contents.
- `skip_web_sync=true` refreshes code only.
- `verbose=true` enables debug logging.

Local command:

```bash
python scripts/bedrock_kb_refresh.py --region us-east-1
```
