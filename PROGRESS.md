# Fuzzer walk

Updated 2026-11-05T02:29+00:00.

| Repository | Done | Remaining | Backlog cursor | Last 24 hours | Last 7 days (per day) |
|---|---|---|---|---|---|
| valkey-io/valkey | 7 | 2480 | #5138 (2026-11-04) | 7 | 1.0 |
| valkey-io/valkey-bloom | 5 | 104 | #93 (2026-04-26) | 5 | 0.7 |
| valkey-io/valkey-json | 6 | 56 | #115 (2026-06-24) | 6 | 0.9 |
| valkey-io/valkey-search | 0 | 667 | #1634 (2026-11-03) | 0 | 0.0 |

## Last runs

### valkey-io/valkey

[2026-11-05T02:29:07+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/41936033117): 3 merge(s) tested, 2 without a code change.

- [valkey-io/valkey#5110](https://github.com/valkey-io/valkey/pull/5110) ([tests](tests/valkey/5110-130b36084/)): Stop zset ops from rewriting keyspace set members during lookups (#5110): 0 of 5 test(s) reached the change, coverage 0%
- [valkey-io/valkey#5283](https://github.com/valkey-io/valkey/pull/5283) ([tests](tests/valkey/5283-745775127/)): bgiteration: remove RO list commands as format changing (#5283): 0 of 3 test(s) reached the change
- [valkey-io/valkey#5274](https://github.com/valkey-io/valkey/pull/5274) ([tests](tests/valkey/5274-c86f6ee51/)): Free valkey-cli test hint before exiting (#5274): declined: The change frees a heap-allocated hint string in valkey-cli's testHint() immediately before exit(0), purely to silence a leak report in a test-only code path (--test_hint). The hint is printed before the free, sdsfree handles both valid and NULL hints, and the process exits on the next lin

### valkey-io/valkey-bloom

[2026-11-05T00:41:15+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/41936041312): 3 merge(s) tested, 0 without a code change.

- [valkey-io/valkey-bloom#110](https://github.com/valkey-io/valkey-bloom/pull/110) ([tests](tests/valkey-bloom/110-9ab369980/)): Match bloom memory tracking by small over count of bytes after a defrag operation (#110): 0 of 2 test(s) reached the change
- [valkey-io/valkey-bloom#97](https://github.com/valkey-io/valkey-bloom/pull/97) ([tests](tests/valkey-bloom/97-c340f0ec2/)): Support reusing an externally built valkey-server binary for test (#97): declined: The change modifies only build.sh and README.md of valkey-bloom's test harness: it lets the integration-test setup reuse an externally built valkey-server binary (VALKEY_SERVER_PATH) and whitelists '9.1' as a SERVER_VERSION. No module code, command handling, data format, or runtime behavio
- [valkey-io/valkey-bloom#94](https://github.com/valkey-io/valkey-bloom/pull/94) ([tests](tests/valkey-bloom/94-712a1d7ee/)): remove 'validate_scale_to' check in replica (#94): 3 of 3 test(s) reached the change

### valkey-io/valkey-json

[2026-11-05T00:15:13+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/41936037415): 3 merge(s) tested, 1 without a code change.

- [valkey-io/valkey-json#65](https://github.com/valkey-io/valkey-json/pull/65) ([tests](tests/valkey-json/65-52c38bb0f/)): Correct spelling and terminology across JSON documentation and tests (#65): declined: The change is purely spelling/terminology edits: README and test-file text, renamed unit/integration test functions, comment-only edits in json.cc, keytable, and the rapidjson headers, a macro renamed with an identical string value, a string inside trace() which is an empty macro (compiles
- [valkey-io/valkey-json#117](https://github.com/valkey-io/valkey-json/pull/117) ([tests](tests/valkey-json/117-7d0acb4c9/)): Making PR's run unstable and latest only, adding a daily job for all versions (#117): declined: The change only modifies the GitHub Actions CI workflow (which server-version matrix runs on PRs versus the daily schedule) and adds a SERVER_VERSION validation check to build.sh. No module source code, command behaviour, reply format, or data path is touched, so there is no runtime behavi
- [valkey-io/valkey-json#95](https://github.com/valkey-io/valkey-json/pull/95) ([tests](tests/valkey-json/95-dcaa82c2e/)): feat: JSON.MERGE plus unit and integration tests (#95): 6 of 6 test(s) reached the change; [crash, predates #95](https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/42)

### valkey-io/valkey-search

[2026-11-05T01:54:48+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/41928158333): 2 merge(s) tested, 0 without a code change.

- [valkey-io/valkey-search#1576](https://github.com/valkey-io/valkey-search/pull/1576): to retry, 'reached'
- [valkey-io/valkey-search#1542](https://github.com/valkey-io/valkey-search/pull/1542): to retry, Claude Code failed (rc=1): timeout after 3000s

## Totals

- Merges whose tests reached the change: 6 of 9 tested (67%)
- Changed-line coverage: median 0% of 1 merge(s) measured, 1 under 50%
- Recall: 2 parents flagged of 2 fix merges
- Claude: $82.07 in the last 24 hours, $82.07 in the last 7 days, $82.07 in all
- Issues filed: 3
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/40
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/41
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/42

## Retries

- [valkey-io/valkey-search#1576](https://github.com/valkey-io/valkey-search/pull/1576): attempt 1 of 3, 'reached'
- [valkey-io/valkey-search#1542](https://github.com/valkey-io/valkey-search/pull/1542): attempt 1 of 3, Claude Code failed (rc=1): timeout after 3000s
