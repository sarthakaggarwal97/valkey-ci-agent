# Fuzzer walk

Updated 2026-11-09T11:35+00:00.

| Repository | Done | Remaining | Backlog cursor | Last 24 hours | Last 7 days (per day) |
|---|---|---|---|---|---|
| valkey-io/valkey | 1165 | 1342 | #2283 (2025-07-03) | 255 | 166.4 |
| valkey-io/valkey-bloom | 109 | 0 | ce516d0ef6e3 (2024-04-26) | 0 | 15.6 |
| valkey-io/valkey-json | 62 | 0 | c1423d7e4b38 (2024-11-25) | 0 | 8.9 |
| valkey-io/valkey-search | 669 | 0 | f8106e72e984 (2024-11-27) | 73 | 95.6 |

## Last runs

### valkey-io/valkey

[2026-11-09T10:19:10+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42589548904): 20 merge(s) tested, 7 without a code change.

- [valkey-io/valkey#3800](https://github.com/valkey-io/valkey/pull/3800) ([tests](tests/valkey/3800-aa7488f4b/)): Fix heap-use-after-free in ACL LOAD when client free is deferred (#3800): 0 of 2 test(s) reached the change, coverage 8%
- [valkey-io/valkey#2365](https://github.com/valkey-io/valkey/pull/2365) ([tests](tests/valkey/2365-663dac97a/)): Prevent calling `lookupkey` twice in some module functions that use `setkey` (#2365): declined: no proposed test validated
- [valkey-io/valkey#2202](https://github.com/valkey-io/valkey/pull/2202) ([tests](tests/valkey/2202-e6ea0b344/)): Remove `get_type` from `ConnectionType` (#2202): 0 of 4 test(s) reached the change, coverage 63%
- [valkey-io/valkey#2310](https://github.com/valkey-io/valkey/pull/2310) ([tests](tests/valkey/2310-78e7208b0/)): Add extensions supported to cluster link layer to propagate extension faster during handshake (#2310): 0 of 4 test(s) reached the change, coverage 100%
- [valkey-io/valkey#2353](https://github.com/valkey-io/valkey/pull/2353) ([tests](tests/valkey/2353-de765863f/)): Fix large allocations crashing Valkey during active defrag (#2353): 0 of 1 test(s) reached the change, coverage 100%; crash, fixed by #2353 (recorded)
- [valkey-io/valkey#1920](https://github.com/valkey-io/valkey/pull/1920) ([tests](tests/valkey/1920-2cd62fa80/)): Add support for automatic client authentication via TLS certificate fields (#1920): 3 of 3 test(s) reached the change, coverage 23%
- [valkey-io/valkey#2335](https://github.com/valkey-io/valkey/pull/2335) ([tests](tests/valkey/2335-7a19c864e/)): Exit early when all the covered slots are deleted (#2335): 0 of 5 test(s) reached the change, coverage 100%
- [valkey-io/valkey#2301](https://github.com/valkey-io/valkey/pull/2301) ([tests](tests/valkey/2301-507042df9/)): Fix replica (the old primary) claims to sitll have slots after manual failover (#2301): 0 of 4 test(s) reached the change, coverage 82%
- [valkey-io/valkey#2337](https://github.com/valkey-io/valkey/pull/2337) ([tests](tests/valkey/2337-ba24e0d4f/)): Avoid logging sender role on each cluster message (#2337): 0 of 3 test(s) reached the change, coverage 100%
- [valkey-io/valkey#2333](https://github.com/valkey-io/valkey/pull/2333) ([tests](tests/valkey/2333-c782b5aea/)): Fix DEBUG CLUSTERLINK KILL args check to avoid crash (#2333): 3 of 3 test(s) reached the change, coverage 100%; crash, fixed by #2333 (recorded)
- [valkey-io/valkey#2334](https://github.com/valkey-io/valkey/pull/2334) ([tests](tests/valkey/2334-6e24028f7/)): Change default values of valkey-cli to use valkey naming, and added fallback to old values (#2334): 0 of 1 test(s) reached the change, coverage 28%
- [valkey-io/valkey#2328](https://github.com/valkey-io/valkey/pull/2328) ([tests](tests/valkey/2328-dcf32220c/)): handle prefetch-batch-max-size config update from zero to positive value (#2328): 2 of 3 test(s) reached the change, coverage 100%
- [valkey-io/valkey#2315](https://github.com/valkey-io/valkey/pull/2315) ([tests](tests/valkey/2315-cb10d9d78/)): retry accept on transient errors (CVE-2025-48367) (#2315): 0 of 4 test(s) reached the change, coverage 81%
- [valkey-io/valkey#2314](https://github.com/valkey-io/valkey/pull/2314) ([tests](tests/valkey/2314-20f5199d9/)): Apply fixed for CVE-2025-32023 (#2314): 5 of 5 test(s) reached the change, coverage 100%; crash, fixed by #2314 (recorded); crash, fixed by #2314 (recorded)
- [valkey-io/valkey#2245](https://github.com/valkey-io/valkey/pull/2245) ([tests](tests/valkey/2245-57b176169/)): Spelling 9 (#2245): declined: The change is purely spelling fixes: comment text, documentation (README, valkey.conf comments, install_server.sh header), Makefile comments, TCL test-suite strings, and two consistently renamed C macros (AOF_REWRITE_LIMITE_* -> AOF_REWRITE_LIMIT_*) whose values and all use sites are uncha
- [valkey-io/valkey#2246](https://github.com/valkey-io/valkey/pull/2246) ([tests](tests/valkey/2246-d75bc311b/)): Spelling 10 (#2246): declined: The change is a spelling cleanup only: comment fixes across several files, a constant macro rename (UTILIZATION_THRESHOLD_FACTOR_MILI -> MILLI, same value 125, both uses updated), a typedef alias rename in module.c with an unchanged struct layout, a reworded log message, and a test-name ty
- [valkey-io/valkey#2308](https://github.com/valkey-io/valkey/pull/2308) ([tests](tests/valkey/2308-d08aab134/)): [optimization] Improve the speed of sending switch-master from sentinel (#2308): declined: The change only reorders when Sentinel emits +switch-master within one sentinelTimer tick: a pure latency optimization in sentinel.c with no change to replies, data, or the failover outcome, and no new iterator hazard (the call already ran before dictReleaseIterator, and sentinelResetPrima
- [valkey-io/valkey#2095](https://github.com/valkey-io/valkey/pull/2095) ([tests](tests/valkey/2095-130ea3973/)): Allow shrinking hashtables in low memory situations (#2095): 2 of 3 test(s) reached the change, coverage 94%
- [valkey-io/valkey#2102](https://github.com/valkey-io/valkey/pull/2102) ([tests](tests/valkey/2102-61b63ab98/)): [valkey-benchmark] Allow multiple random (or sequential) placeholders and fix zadd --sequential to create expected data (#2102): 3 of 3 test(s) reached the change, coverage 94%
- [valkey-io/valkey#2304](https://github.com/valkey-io/valkey/pull/2304) ([tests](tests/valkey/2304-9c45cf334/)): Make unit test report each memory leak only once (#2304): declined: The change only edits Valkey's unit test harness (src/unit/test_main.c, test_hashtable.c, test_rax.c, test_zmalloc.c, test_files.h) so that leak checks compare against a baseline of zmalloc_used_memory() taken before each suite instead of asserting zero, and it removes two now-redundant un

### valkey-io/valkey-bloom

[2026-11-09T11:35:50+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42610211037): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-json

[2026-11-09T11:08:07+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42606416082): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-search

[2026-11-09T11:14:51+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42607366831): 0 merge(s) tested, 0 without a code change.


## Totals

- Merges whose tests reached the change: 652 of 977 tested (67%)
- Changed-line coverage: median 100% of 532 merge(s) measured, 41 under 50%
- Recall: 181 parents flagged of 275 fix merges
- Claude: $979.37 in the last 24 hours, $7048.30 in the last 7 days, $7048.30 in all
- Issues filed: 63
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
