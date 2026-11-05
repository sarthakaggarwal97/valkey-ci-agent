# Fuzzer walk

Updated 2026-11-05T00:15+00:00.

| Repository | Done | Remaining | Backlog cursor | Last 24 hours | Last 7 days (per day) |
|---|---|---|---|---|---|
| valkey-io/valkey | 2 | 2480 | #5138 (2026-11-04) | 2 | 0.3 |
| valkey-io/valkey-bloom | 2 | 107 | #97 (2026-06-04) | 2 | 0.3 |
| valkey-io/valkey-json | 6 | 56 | #115 (2026-06-24) | 6 | 0.9 |

## Last runs

### valkey-io/valkey

[2026-11-04T23:33:34+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/41928150529): 2 merge(s) tested, 0 without a code change.

- [valkey-io/valkey#5122](https://github.com/valkey-io/valkey/pull/5122) ([tests](tests/valkey/5122-71a43e9dd/)): Return an empty string from lm_asprintf() when vsnprintf() fails (#5122): 2 of 2 test(s) reached the change
- [valkey-io/valkey#5170](https://github.com/valkey-io/valkey/pull/5170) ([tests](tests/valkey/5170-9f05a35a6/)): Rehash in proportion to scan work when sampling a shrinking hashtable (#5170): 2 of 2 test(s) reached the change; [check, introduced by #5170](https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/41); check, not reproduced (recorded); [check, appeared after #5170](https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/41); check, not reproduced (recorded)

### valkey-io/valkey-bloom

[2026-11-04T23:21:01+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/41928161790): 2 merge(s) tested, 1 without a code change.

- [valkey-io/valkey-bloom#110](https://github.com/valkey-io/valkey-bloom/pull/110): to retry, 'reached'
- [valkey-io/valkey-bloom#104](https://github.com/valkey-io/valkey-bloom/pull/104) ([tests](tests/valkey-bloom/104-f428f3e31/)): `BF.INFO`: documentation and reabability nits (#104): declined: The change is purely cosmetic: it reorders two mutually exclusive string-literal match arms (EXPANSION/TIGHTENING) inside bloom_filter_info, which cannot alter which arm matches or any BF.INFO reply, and edits src/commands/bf.info.json, a documentation-only metadata file that no code loads

### valkey-io/valkey-json

[2026-11-05T00:15:13+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/41936037415): 3 merge(s) tested, 1 without a code change.

- [valkey-io/valkey-json#65](https://github.com/valkey-io/valkey-json/pull/65) ([tests](tests/valkey-json/65-52c38bb0f/)): Correct spelling and terminology across JSON documentation and tests (#65): declined: The change is purely spelling/terminology edits: README and test-file text, renamed unit/integration test functions, comment-only edits in json.cc, keytable, and the rapidjson headers, a macro renamed with an identical string value, a string inside trace() which is an empty macro (compiles
- [valkey-io/valkey-json#117](https://github.com/valkey-io/valkey-json/pull/117) ([tests](tests/valkey-json/117-7d0acb4c9/)): Making PR's run unstable and latest only, adding a daily job for all versions (#117): declined: The change only modifies the GitHub Actions CI workflow (which server-version matrix runs on PRs versus the daily schedule) and adds a SERVER_VERSION validation check to build.sh. No module source code, command behaviour, reply format, or data path is touched, so there is no runtime behavi
- [valkey-io/valkey-json#95](https://github.com/valkey-io/valkey-json/pull/95) ([tests](tests/valkey-json/95-dcaa82c2e/)): feat: JSON.MERGE plus unit and integration tests (#95): 6 of 6 test(s) reached the change; [crash, predates #95](https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/42)

## Totals

- Merges whose tests reached the change: 5 of 5 tested (100%)
- Changed-line coverage: none measured
- Recall: 2 parents flagged of 2 fix merges
- Claude: $30.57 in the last 24 hours, $30.57 in the last 7 days, $30.57 in all
- Issues filed: 3
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/40
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/41
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/42

## Retries

- [valkey-io/valkey-bloom#110](https://github.com/valkey-io/valkey-bloom/pull/110): attempt 1 of 3, 'reached'
