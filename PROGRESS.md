# Fuzzer walk

Updated 2026-11-04T23:21+00:00.

| Repository | Done | Remaining | Backlog cursor | Last 24 hours | Last 7 days (per day) |
|---|---|---|---|---|---|
| valkey-io/valkey-bloom | 2 | 107 | #97 (2026-06-04) | 2 | 0.3 |
| valkey-io/valkey-json | 2 | 60 | #119 (2026-08-27) | 2 | 0.3 |

## Last runs

### valkey-io/valkey-bloom

[2026-11-04T23:21:01+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/41928161790): 2 merge(s) tested, 1 without a code change.

- [valkey-io/valkey-bloom#110](https://github.com/valkey-io/valkey-bloom/pull/110): to retry, 'reached'
- [valkey-io/valkey-bloom#104](https://github.com/valkey-io/valkey-bloom/pull/104) ([tests](tests/valkey-bloom/104-f428f3e31/)): `BF.INFO`: documentation and reabability nits (#104): declined: The change is purely cosmetic: it reorders two mutually exclusive string-literal match arms (EXPANSION/TIGHTENING) inside bloom_filter_info, which cannot alter which arm matches or any BF.INFO reply, and edits src/commands/bf.info.json, a documentation-only metadata file that no code loads

### valkey-io/valkey-json

[2026-11-04T23:15:05+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/41928154386): 2 merge(s) tested, 0 without a code change.

- [valkey-io/valkey-json#123](https://github.com/valkey-io/valkey-json/pull/123) ([tests](tests/valkey-json/123-aeaa68237/)): Fix JSONPath filters over object children (#123): 3 of 3 test(s) reached the change; [regressed-fix, the fix of #123 regressed](https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/40); [regressed-fix, the fix of #123 regressed](https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/40); [regressed-fix, the fix of #123 regressed](https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/40)
- [valkey-io/valkey-json#120](https://github.com/valkey-io/valkey-json/pull/120) ([tests](tests/valkey-json/120-58faf1ca5/)): Fixing crash on recursive path mutation (#120): 2 of 5 test(s) reached the change; crash, fixed by #120 (recorded); crash, fixed by #120 (recorded)

## Totals

- Merges whose tests reached the change: 2 of 2 tested (100%)
- Recall: 2 parents flagged of 2 fix merges
- Claude: $12.07 in the last 24 hours, $12.07 in the last 7 days, $12.07 in all
- Issues filed: 1
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/40

## Retries

- [valkey-io/valkey-bloom#110](https://github.com/valkey-io/valkey-bloom/pull/110): attempt 1 of 3, 'reached'
