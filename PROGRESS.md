# Fuzzer walk

Updated 2026-11-09T13:08+00:00.

| Repository | Done | Remaining | Backlog cursor | Last 24 hours | Last 7 days (per day) |
|---|---|---|---|---|---|
| valkey-io/valkey | 1185 | 1322 | #2260 (2025-06-24) | 244 | 169.3 |
| valkey-io/valkey-bloom | 109 | 0 | ce516d0ef6e3 (2024-04-26) | 0 | 15.6 |
| valkey-io/valkey-json | 62 | 0 | c1423d7e4b38 (2024-11-25) | 0 | 8.9 |
| valkey-io/valkey-search | 669 | 0 | f8106e72e984 (2024-11-27) | 62 | 95.6 |

## Last runs

### valkey-io/valkey

[2026-11-09T11:54:24+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42600016857): 20 merge(s) tested, 1 without a code change.

- [valkey-io/valkey#2283](https://github.com/valkey-io/valkey/pull/2283) ([tests](tests/valkey/2283-e279b00f8/)): Generate a new shard_id when the replica executes CLUSTER RESET SOFT (#2283): 2 of 2 test(s) reached the change, coverage 100%; check, introduced and since fixed (recorded)
- [valkey-io/valkey#2262](https://github.com/valkey-io/valkey/pull/2262) ([tests](tests/valkey/2262-293be3e32/)): spelling: md (#2262): declined: The change edits a single comment in src/unit/test_help.h, correcting a filename typo (README.me to README.md). It is a comment-only spelling fix with no behaviour change in any Valkey binary, so no scenario is needed.
- [valkey-io/valkey#2288](https://github.com/valkey-io/valkey/pull/2288) ([tests](tests/valkey/2288-e53e048c6/)): Fix leak when shrinking a hashtable without entries (#2288): 0 of 2 test(s) reached the change, coverage 100%
- [valkey-io/valkey#1761](https://github.com/valkey-io/valkey/pull/1761) ([tests](tests/valkey/1761-51b81d0ec/)): Implement RPS control for benchmark using token bucket algorithm (#1761): 2 of 2 test(s) reached the change, coverage 100%
- [valkey-io/valkey#2179](https://github.com/valkey-io/valkey/pull/2179) ([tests](tests/valkey/2179-06a281ee8/)): Update LPOP and RPOP command json file description (#2179): declined: The change only rewords the 'summary' documentation string for LPOP and RPOP in their command JSON files and the regenerated commands.def. Command behaviour, arity, flags, key specs and replies are untouched; the only observable difference is the intended doc text in COMMAND DOCS output. T
- [valkey-io/valkey#2296](https://github.com/valkey-io/valkey/pull/2296) ([tests](tests/valkey/2296-a62f83db8/)): Make valkey-server --help/-h output to stdout and exit with 0 (#2296): declined: The change affects only the valkey-server --help/-h code path: usage() in src/server.c is called solely from the argv[1] == --help/-h branch of main() (server.c:7099), and now prints to stdout and exits 0 instead of stderr/1. No running server, tool behaviour, reply, replication, or persis
- [valkey-io/valkey#2287](https://github.com/valkey-io/valkey/pull/2287): to retry, the job left no result
- [valkey-io/valkey#2290](https://github.com/valkey-io/valkey/pull/2290) ([tests](tests/valkey/2290-74137d33b/)): Fix MEMORY USAGE to consider embedded keys (#2290): 2 of 2 test(s) reached the change, coverage 100%
- [valkey-io/valkey#2195](https://github.com/valkey-io/valkey/pull/2195) ([tests](tests/valkey/2195-1e05724f5/)): Add SAFE option to SHUTDOWN to reject shutdown in unsafe situations (#2195): 5 of 7 test(s) reached the change, coverage 100%
- [valkey-io/valkey#1475](https://github.com/valkey-io/valkey/pull/1475) ([tests](tests/valkey/1475-fef4dbf04/)): Move auth check to the front, before command exist/arity/protected check (#1475): 2 of 2 test(s) reached the change, coverage 100%
- [valkey-io/valkey#2280](https://github.com/valkey-io/valkey/pull/2280) ([tests](tests/valkey/2280-d37dc527d/)): Use objectGetExpire instead of getExpire to reduce expire hashtable lookup (#2280): 0 of 8 test(s) reached the change, coverage 84%
- [valkey-io/valkey#2141](https://github.com/valkey-io/valkey/pull/2141) ([tests](tests/valkey/2141-0b0b813ac/)): Update redis to server in internal variable names in benchmark source code (#2141): declined: The change only renames the internal variable/struct field `redis_config` to `server_config` in src/valkey-benchmark.c (in the global config struct and clusterNode struct), updating all references consistently. No logic, output, memory management, or user-visible behavior of valkey-benchma
- [valkey-io/valkey#2209](https://github.com/valkey-io/valkey/pull/2209) ([tests](tests/valkey/2209-23fb9f2b5/)): Start failover without waiting for the next cluster cron cycle (#2209): 0 of 3 test(s) reached the change, coverage 91%
- [valkey-io/valkey#1677](https://github.com/valkey-io/valkey/pull/1677) ([tests](tests/valkey/1677-a0b8d5e37/)): Save config file after replica migration and brocast a PONG when role changed (#1677): 2 of 5 test(s) reached the change, coverage 100%
- [valkey-io/valkey#2269](https://github.com/valkey-io/valkey/pull/2269) ([tests](tests/valkey/2269-db639c99f/)): Check module NO_FAILOVER flag before calling clusterHandleReplicaFailover (#2269): 0 of 3 test(s) reached the change, coverage 100%
- [valkey-io/valkey#2264](https://github.com/valkey-io/valkey/pull/2264) ([tests](tests/valkey/2264-b65beca83/)): Optimize CLUSTER INFO (#2264): 0 of 4 test(s) reached the change, coverage 95%
- [valkey-io/valkey#2240](https://github.com/valkey-io/valkey/pull/2240) ([tests](tests/valkey/2240-57459f77d/)): Spelling 3 (#2240): declined: The change is purely cosmetic: every hunk fixes a spelling mistake in a comment, README, test comment, or valgrind suppression description. The only source-code edit is renaming the static callback updateAppendonly to updateAppendOnly in src/config.c, with its sole call site updated in the
- [valkey-io/valkey#2257](https://github.com/valkey-io/valkey/pull/2257) ([tests](tests/valkey/2257-ad2c6b049/)): Fix hashtablePauseAutoShrink and use it in hashtableScanDefrag (#2257): 0 of 3 test(s) reached the change, coverage 100%
- [valkey-io/valkey#2258](https://github.com/valkey-io/valkey/pull/2258) ([tests](tests/valkey/2258-868721cbc/)): Add ENGINE_CHECK_RDB_NAME to the 'test' target dependencies in Makefile (#2258): declined: The change only adds ENGINE_CHECK_RDB_NAME (valkey-check-rdb) to the Makefile 'test' target's prerequisites so it gets built before the test suite runs. It is a build/CI change with no modification to any runtime code path, command behavior, reply, persistence, or tool logic; there is noth
- [valkey-io/valkey#1482](https://github.com/valkey-io/valkey/pull/1482) ([tests](tests/valkey/1482-94a95cd36/)): Cleanup aof temp files in sigShutdownHandler before exiting (#1482): declined: no proposed test validated

### valkey-io/valkey-bloom

[2026-11-09T13:08:09+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42622714202): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-json

[2026-11-09T12:40:17+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42618877512): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-search

[2026-11-09T12:47:24+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42619809625): 0 merge(s) tested, 0 without a code change.


## Totals

- Merges whose tests reached the change: 658 of 989 tested (67%)
- Changed-line coverage: median 100% of 544 merge(s) measured, 41 under 50%
- Recall: 182 parents flagged of 278 fix merges
- Claude: $959.83 in the last 24 hours, $7146.88 in the last 7 days, $7146.88 in all
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

- [valkey-io/valkey#2287](https://github.com/valkey-io/valkey/pull/2287): attempt 0 of 3, the job left no result
