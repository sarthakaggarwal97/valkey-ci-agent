# Fuzzer walk

Updated 2026-11-10T00:28+00:00.

| Repository | Done | Remaining | Backlog cursor | Last 24 hours | Last 7 days (per day) |
|---|---|---|---|---|---|
| valkey-io/valkey | 1323 | 1187 | #1819 (2025-04-17) | 296 | 189.0 |
| valkey-io/valkey-bloom | 109 | 0 | ce516d0ef6e3 (2024-04-26) | 0 | 15.6 |
| valkey-io/valkey-json | 63 | 0 | c1423d7e4b38 (2024-11-25) | 1 | 9.0 |
| valkey-io/valkey-search | 670 | 0 | f8106e72e984 (2024-11-27) | 15 | 95.7 |

## Last runs

### valkey-io/valkey

[2026-11-09T23:29:13+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42694658068): 20 merge(s) tested, 4 without a code change.

- [valkey-io/valkey#1995](https://github.com/valkey-io/valkey/pull/1995) ([tests](tests/valkey/1995-49e44e531/)): Environment variable REDISCLI_AUTH to VALKEYCLI_AUTH conversion (#1995): 0 of 1 test(s) reached the change, coverage 100%
- [valkey-io/valkey#1671](https://github.com/valkey-io/valkey/pull/1671) ([tests](tests/valkey/1671-2fe08f864/)): Add multi-database support to cluster mode (#1671): 9 of 14 test(s) reached the change, coverage 67%
- [valkey-io/valkey#2034](https://github.com/valkey-io/valkey/pull/2034) ([tests](tests/valkey/2034-8dca8019d/)): Remove redundancy if condition statement (#2034): declined: Pure refactor with no behavior change: the removed `if (sdslen(value) > 0)` guard in setrangeCommand was dead code, since both the non-existing-key branch (t_string.c:523-526) and the existing-key branch (t_string.c:543-546) return early when the value is empty before reaching it. The writ
- [valkey-io/valkey#2036](https://github.com/valkey-io/valkey/pull/2036) ([tests](tests/valkey/2036-4eeffda56/)): Properly escape double quotes and backslash in sdscatrepr (#2036): 0 of 1 test(s) reached the change, coverage 100%
- [valkey-io/valkey#1859](https://github.com/valkey-io/valkey/pull/1859) ([tests](tests/valkey/1859-dd772c490/)): Hyperloglog ARM NEON SIMD optimization (#1859): 0 of 2 test(s) reached the change, coverage 100%
- [valkey-io/valkey#2009](https://github.com/valkey-io/valkey/pull/2009) ([tests](tests/valkey/2009-8df0a6b67/)): Accept limited number of new cluster link connections per cycle (#2009): 0 of 3 test(s) reached the change, coverage 100%
- [valkey-io/valkey#1944](https://github.com/valkey-io/valkey/pull/1944) ([tests](tests/valkey/1944-70f205761/)): Optimize string2ll performance using AVX512 (#1944): 0 of 3 test(s) reached the change, coverage 14%
- [valkey-io/valkey#2001](https://github.com/valkey-io/valkey/pull/2001) ([tests](tests/valkey/2001-2d200dfa3/)): Minor change for sadd command (#2001): declined: The change moves `server.dirty += added` inside the `if (added)` guard in saddCommand. When added == 0 the old code added 0 to server.dirty, which is a no-op, and when added > 0 both versions execute identically. The dirty counter delta (and thus replication propagation), keyspace notifica
- [valkey-io/valkey#2015](https://github.com/valkey-io/valkey/pull/2015) ([tests](tests/valkey/2015-9cbcaa258/)): valkey-benchmark: Add test for MGET (#2015): 0 of 3 test(s) reached the change, coverage 100%
- [valkey-io/valkey#1961](https://github.com/valkey-io/valkey/pull/1961) ([tests](tests/valkey/1961-c9c49b446/)): Introduce MPTCP for replication (#1961): 1 of 8 test(s) reached the change, coverage 100%
- [valkey-io/valkey#2007](https://github.com/valkey-io/valkey/pull/2007) ([tests](tests/valkey/2007-249495a3f/)): Convert pubsub dicts to hashtables (#2007): declined: no proposed test validated
- [valkey-io/valkey#2002](https://github.com/valkey-io/valkey/pull/2002) ([tests](tests/valkey/2002-5065fcf12/)): Correct rpushx description (#2002): 2 of 2 test(s) reached the change
- [valkey-io/valkey#1976](https://github.com/valkey-io/valkey/pull/1976) ([tests](tests/valkey/1976-d3b6559d7/)): Clean up unnecessary use of strlen (#1976): 2 of 8 test(s) reached the change, coverage 63%
- [valkey-io/valkey#1476](https://github.com/valkey-io/valkey/pull/1476) ([tests](tests/valkey/1476-5df95b3df/)): Refactor syncWithPrimary and replicaTryPartialResynchronization (#1476): 0 of 18 test(s) reached the change, coverage 61%
- [valkey-io/valkey#2006](https://github.com/valkey-io/valkey/pull/2006) ([tests](tests/valkey/2006-30e9109d1/)): Add output buffer limiting for unauthenticated clients (#2006): 2 of 8 test(s) reached the change, coverage 91%
- [valkey-io/valkey#1933](https://github.com/valkey-io/valkey/pull/1933) ([tests](tests/valkey/1933-83d297902/)): Add --hotkeys-count option for Valkey-cli (#1933): 3 of 3 test(s) reached the change, coverage 93%; crash, introduced and since fixed (recorded)
- [valkey-io/valkey#1989](https://github.com/valkey-io/valkey/pull/1989) ([tests](tests/valkey/1989-38846504d/)): blocking client followup fix related to log_req_res (#1989): declined: The change only relocates the reqresSaveClientReplyOffset call inside _addReplyToBufferOrList so the request/response-log offset is saved before replies are diverted to the deferred-reply or pending-push lists. That function is compiled only under the LOG_REQ_RES debug build flag; in stand
- [valkey-io/valkey#1984](https://github.com/valkey-io/valkey/pull/1984) ([tests](tests/valkey/1984-176641649/)): Allow scripts to support null characters again (#1984): 5 of 8 test(s) reached the change, coverage 100%
- [valkey-io/valkey#1985](https://github.com/valkey-io/valkey/pull/1985) ([tests](tests/valkey/1985-9fb850dab/)): Only enable defrag for vendored jemalloc (#1985): declined: This is a build-time guard: HAVE_DEFRAG now requires VALKEY_VENDORED_JEMALLOC, a macro the vendored jemalloc header defines. The fuzzer's builds of both the change and its parent use the vendored jemalloc, so HAVE_DEFRAG is defined on both and runtime behavior (activedefrag config, defrag 
- [valkey-io/valkey#573](https://github.com/valkey-io/valkey/pull/573) ([tests](tests/valkey/573-05d8fd474/)): Avoid shard id update of replica if not matching with primary shard id (#573): 0 of 5 test(s) reached the change, coverage 40%

### valkey-io/valkey-bloom

[2026-11-10T00:28:18+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42728453422): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-json

[2026-11-10T00:28:25+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42728473895): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-search

[2026-11-09T23:13:05+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42718519413): 0 merge(s) tested, 0 without a code change.


## Totals

- Merges whose tests reached the change: 696 of 1062 tested (66%)
- Changed-line coverage: median 100% of 607 merge(s) measured, 51 under 50%
- Recall: 191 parents flagged of 291 fix merges
- Claude: $1239.87 in the last 24 hours, $7797.39 in the last 7 days, $7797.39 in all
- Issues filed: 64
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/100
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/101
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/102
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/103
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/104
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/105
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/106
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/107
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/108
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/109
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/110
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/111
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/112
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/113
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/40
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/41
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/42
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/43
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/44
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/45
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/47
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/48
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/49
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/50
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/53
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/54
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/55
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/56
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/57
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/58
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/59
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/60
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/61
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/62
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/63
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/64
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/65
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/66
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/67
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/68
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/70
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/72
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/73
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/75
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/76
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/78
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/79
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/80
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/81
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/82
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/83
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/84
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/85
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/86
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/87
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/89
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/90
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/91
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/92
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/93
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/94
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/95
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/96
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/98

## Retries

None.
