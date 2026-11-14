# Fuzzer walk

Updated 2026-11-14T11:02+00:00.

| Repository | Done | Remaining | Backlog cursor | Last 24 hours | Last 7 days (per day) |
|---|---|---|---|---|---|
| valkey-io/valkey | 2346 | 194 | #12899 (2024-01-27) | 327 | 247.7 |
| valkey-io/valkey-bloom | 109 | 0 | ce516d0ef6e3 (2024-04-26) | 0 | 0.0 |
| valkey-io/valkey-json | 66 | 0 | c1423d7e4b38 (2024-11-25) | 2 | 0.6 |
| valkey-io/valkey-search | 690 | 0 | f8106e72e984 (2024-11-27) | 9 | 42.4 |

## Last runs

### valkey-io/valkey

[2026-11-14T10:39:59+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/43577709618): 20 merge(s) tested, 2 without a code change.

- [redis/redis#13008](https://github.com/redis/redis/pull/13008) ([tests](tests/valkey/13008-0777dc789/)): Improve error handling in connSocketBlockingConnect for various connction failures (#13008): 1 of 1 test(s) reached the change, coverage 100%
- [redis/redis#13033](https://github.com/redis/redis/pull/13033) ([tests](tests/valkey/13033-809651543/)): Fix invalid dictNext usage when pubsubshard_channels became empty (#13033): 0 of 5 test(s) reached the change, coverage 100%
- [redis/redis#13020](https://github.com/redis/redis/pull/13020) ([tests](tests/valkey/13020-13bd3643c/)): Re-compute active_defrag_running after adjusting defrag configurations (#13020): declined: The change only affects how promptly the internal active-defrag pacing value (server.active_defrag_running) reacts to CONFIG SET of the defrag cycle/threshold configs. This is unreachable and non-comparable in the fuzzer: it requires a jemalloc build (activedefrag is rejected otherwise), i
- [redis/redis#13027](https://github.com/redis/redis/pull/13027) ([tests](tests/valkey/13027-87eaf119c/)): Minor optimization for expire dict in defragKey (#13027): 0 of 3 test(s) reached the change, coverage 100%
- [redis/redis#13031](https://github.com/redis/redis/pull/13031) ([tests](tests/valkey/13031-84fd745d6/)): Fix kvstore unable to push resize_cursor for resize when dict is NULL (#13031): 2 of 2 test(s) reached the change, coverage 100%
- [redis/redis#12822](https://github.com/redis/redis/pull/12822) ([tests](tests/valkey/12822-8cd62f82c/)): Refactor the per-slot dict-array db.c into a new kvstore data structure (#12822): 2 of 11 test(s) reached the change, coverage 80%; crash, fixed by redis/redis#12822 (recorded)
- [redis/redis#13030](https://github.com/redis/redis/pull/13030) ([tests](tests/valkey/13030-f20774ece/)): Fix active expire timeout when db done the scanning (#13030): declined: This is a performance/timeout fix to the local active-expire cron loop. The added db_done flag only changes the repeat heuristic so a fully-scanned expires dictionary is not needlessly re-scanned; it does not change which keys are reclaimed (the full first pass already expires and propagat
- [redis/redis#12434](https://github.com/redis/redis/pull/12434) ([tests](tests/valkey/12434-02a87885e/)): Add missing structural API changes to JSON file (#12434): 2 of 2 test(s) reached the change
- [redis/redis#12949](https://github.com/redis/redis/pull/12949) ([tests](tests/valkey/12949-c1041c2c0/)): Make db->avg_ttl more precise (#12949): 0 of 4 test(s) reached the change, coverage 100%
- [redis/redis#12925](https://github.com/redis/redis/pull/12925) ([tests](tests/valkey/12925-62153b3b2/)): Refine the purpose of rdb saving with accurate flags (#12925): declined: The change only passes more accurate RDBFLAGS values into save functions, but no save-path code consumes RDBFLAGS_REPLICATION on this checkout: rdbSaveInfoAuxFields and rdbSaveDb test only RDBFLAGS_AOF_PREAMBLE, rdbSaveInternal tests only RDBFLAGS_KEEP_CACHE, and startSaving's module-subev
- [redis/redis#13015](https://github.com/redis/redis/pull/13015) ([tests](tests/valkey/13015-6016973ac/)): Fix module assertion crash when timer and timeout are unlocked in the same event loop (#13015): 2 of 5 test(s) reached the change, coverage 0%; crash, fixed by redis/redis#13015 (recorded)
- [redis/redis#13017](https://github.com/redis/redis/pull/13017) ([tests](tests/valkey/13017-74a6e48a3/)): Fix module unblock crash due to no timeout_callback (#13017): 0 of 4 test(s) reached the change, coverage 0%
- [redis/redis#12765](https://github.com/redis/redis/pull/12765) ([tests](tests/valkey/12765-f469dd8ca/)): Add novalues option to command HSCAN. (#12765): 3 of 3 test(s) reached the change, coverage 100%
- [redis/redis#12299](https://github.com/redis/redis/pull/12299) ([tests](tests/valkey/12299-24f6d08b3/)): Implement `CLIENT KILL MAXAGE <maxage>` (#12299): 2 of 2 test(s) reached the change, coverage 100%
- [redis/redis#13011](https://github.com/redis/redis/pull/13011) ([tests](tests/valkey/13011-45a35a79c/)): Fix timeout not being set in module blockClient case (#13011): 4 of 5 test(s) reached the change, coverage 100%; [crash, predates redis/redis#13011](https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/157)
- [redis/redis#10609](https://github.com/redis/redis/pull/10609) ([tests](tests/valkey/10609-76adbf6ff/)): Adds connection timeout option to redis-cli (#10609): 0 of 4 test(s) reached the change, coverage 0%
- [redis/redis#13004](https://github.com/redis/redis/pull/13004) ([tests](tests/valkey/13004-492021db9/)): Fix blocking commands timeout is reset due to re-processing command (#13004): 0 of 7 test(s) reached the change, coverage 0%
- [redis/redis#12819](https://github.com/redis/redis/pull/12819) ([tests](tests/valkey/12819-af7ceeb76/)): Optimize resizing hash table to resize not only non-empty dicts. (#12819): 3 of 5 test(s) reached the change, coverage 92%
- [redis/redis#12985](https://github.com/redis/redis/pull/12985) ([tests](tests/valkey/12985-c5273cae1/)): Add RM_TryCalloc() and RM_TryRealloc() (#12985): 2 of 2 test(s) reached the change, coverage 100%
- [redis/redis#13000](https://github.com/redis/redis/pull/13000) ([tests](tests/valkey/13000-acd960522/)): Fix maxmemory-samples stack overflow crash in evictionPoolPopulate, limit its value to [1,64] (#13000): 2 of 2 test(s) reached the change; crash, fixed by redis/redis#13000 (recorded)

### valkey-io/valkey-bloom

[2026-11-14T11:02:31+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/43587234785): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-json

[2026-11-14T10:21:22+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/43584820580): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-search

[2026-11-14T10:32:07+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/43585669327): 0 merge(s) tested, 0 without a code change.


## Totals

- Merges whose tests reached the change: 932 of 1487 tested (63%)
- Changed-line coverage: median 100% of 973 merge(s) measured, 74 under 50%
- Recall: 245 parents flagged of 388 fix merges
- Claude: $731.59 in the last 24 hours, $6625.46 in the last 7 days, $10915.95 in all
- Issues filed: 79
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
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/114
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/115
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/117
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/122
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/125
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/126
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/127
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/128
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/129
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/145
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/146
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/147
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/153
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/156
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/157
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
- Model updates: 0 asked for, 0 with no model change, 0 rejected, 0 failed; 0 pull request(s)

## Retries

None.
