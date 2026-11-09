# Fuzzer walk

Updated 2026-11-09T03:00+00:00.

| Repository | Done | Remaining | Backlog cursor | Last 24 hours | Last 7 days (per day) |
|---|---|---|---|---|---|
| valkey-io/valkey | 1055 | 1452 | #2512 (2025-08-19) | 215 | 150.7 |
| valkey-io/valkey-bloom | 109 | 0 | ce516d0ef6e3 (2024-04-26) | 0 | 15.6 |
| valkey-io/valkey-json | 62 | 0 | c1423d7e4b38 (2024-11-25) | 0 | 8.9 |
| valkey-io/valkey-search | 669 | 0 | f8106e72e984 (2024-11-27) | 139 | 95.6 |

## Last runs

### valkey-io/valkey

[2026-11-09T00:52:29+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42531463432): 20 merge(s) tested, 8 without a code change.

- [valkey-io/valkey#2553](https://github.com/valkey-io/valkey/pull/2553) ([tests](tests/valkey/2553-d6e011f95/)): Reduce active defrag test latency by lowering hit threshold (#2553): 0 of 3 test(s) reached the change, coverage 100%
- [valkey-io/valkey#2565](https://github.com/valkey-io/valkey/pull/2565) ([tests](tests/valkey/2565-6774d173c/)): Fix the issue of incorrect commandlog metrics in the script (#2565): 2 of 2 test(s) reached the change, coverage 100%
- [valkey-io/valkey#2458](https://github.com/valkey-io/valkey/pull/2458) ([tests](tests/valkey/2458-3db75f555/)): Reset cluster related stats in CONFIG RESETSTATS (#2458): 2 of 2 test(s) reached the change, coverage 100%; [check, appeared after #2458](https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/108); check, introduced and since fixed (recorded); check, introduced and since fixed (recorded)
- [valkey-io/valkey#2494](https://github.com/valkey-io/valkey/pull/2494) ([tests](tests/valkey/2494-88cd2086a/)): Split SLOT_EXPORT_AUTHENTICATING into SEND and READ to avoid synchronous reading of auth response (#2494): 4 of 6 test(s) reached the change, coverage 80%
- [valkey-io/valkey#2547](https://github.com/valkey-io/valkey/pull/2547) ([tests](tests/valkey/2547-39fade42d/)): Do not migrate function in new atomic slot migration (#2547): 3 of 3 test(s) reached the change; check, introduced and since fixed (recorded)
- [valkey-io/valkey#2560](https://github.com/valkey-io/valkey/pull/2560) ([tests](tests/valkey/2560-682fc0b41/)): Remove the trailing chars of the --cluster reshard log message in valkey-cli (#2560): declined: The change only removes the trailing ': ' from the informational 'Moving slot ...' progress message clusterManagerMoveSlot() prints on valkey-cli's stdout during --cluster reshard/fix; no server code, command reply, data path, or exit status changes, so there is no behavior a server-side e
- [valkey-io/valkey#2548](https://github.com/valkey-io/valkey/pull/2548) ([tests](tests/valkey/2548-06cefc181/)): Attempt to fix sub-replica getting out of sync (#2548): 0 of 5 test(s) reached the change, coverage 100%
- [valkey-io/valkey#2541](https://github.com/valkey-io/valkey/pull/2541) ([tests](tests/valkey/2541-3086e61d4/)): Update reply schema for LMOVE and BLMOVE (#2541): declined: The change edits only the reply_schema metadata in lmove.json and blmove.json to document the existing nil reply when the source key is missing. No C code or runtime behaviour changes: LMOVE/BLMOVE reply identically on the parent and the change, so a compared sequence would find no differe
- [valkey-io/valkey#2530](https://github.com/valkey-io/valkey/pull/2530) ([tests](tests/valkey/2530-9d682bad0/)): bio.c: Organize all worker data in a struct (#2530): declined: The change is a mechanical refactor of bio.c that gathers the four parallel per-worker arrays (thread id, mutex, condvar, job list) into one struct array, plus __thread→_Thread_local spelling swaps in io_threads.c, networking.c and vset.c. Every lock/condvar/queue access maps 1:1 to the sa
- [valkey-io/valkey#2538](https://github.com/valkey-io/valkey/pull/2538) ([tests](tests/valkey/2538-9aeab8ccb/)): Consistently use static_assert across code (#2538): declined: The change only alters how the static_assert fallback macro is defined (removing duplicate per-file fallbacks and switching the header fallback from the extern-negative-array trick to C11 _Static_assert). static_assert is a compile-time construct that generates no runtime code: a passing a
- [valkey-io/valkey#2497](https://github.com/valkey-io/valkey/pull/2497) ([tests](tests/valkey/2497-8c5554799/)): Don't allow slot migration to myself node (#2497): 2 of 2 test(s) reached the change, coverage 100%
- [valkey-io/valkey#2092](https://github.com/valkey-io/valkey/pull/2092) ([tests](tests/valkey/2092-09fb436cf/)): Optimize pipelining by parsing and prefetching multiple commands (#2092): 0 of 19 test(s) reached the change, coverage 98%; [check, predates #2092](https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/109)
- [valkey-io/valkey#2522](https://github.com/valkey-io/valkey/pull/2522) ([tests](tests/valkey/2522-d2eee78a1/)): Module API: Add READONLY flag to ClientInfo.flags output structure (#2522): 2 of 2 test(s) reached the change, coverage 100%; check, not reproduced (recorded); check, introduced and since fixed (recorded); check, not reproduced (recorded); check, introduced and since fixed (recorded)
- [valkey-io/valkey#2527](https://github.com/valkey-io/valkey/pull/2527) ([tests](tests/valkey/2527-b8357e5be/)): Fix slot range lists overlap to rewind the nested list again  (#2527): 2 of 2 test(s) reached the change, coverage 100%
- [valkey-io/valkey@2db4eeb1fcd8](https://github.com/valkey-io/valkey/commit/2db4eeb1fcd8316a792476c3023e705be24fdd70) ([tests](tests/valkey/2db4eeb1f/)): Remove temporary build correction for RDMA and libvalkey 0.1.0: declined: The change is build-system only: it removes a temporary CMake link-order workaround (explicitly linking valkey::valkey after valkey::valkey_rdma) that was needed for a bug in libvalkey 0.1.0, now fixed upstream. No source file, command, reply, replication, cluster, or persistence behaviour
- [valkey-io/valkey@11780db513b1](https://github.com/valkey-io/valkey/commit/11780db513b14034b3a314d4d4e5f52a3be44477) ([tests](tests/valkey/11780db51/)): Update deps/libvalkey to version 0.2.1: 0 of 3 test(s) reached the change
- [valkey-io/valkey#2514](https://github.com/valkey-io/valkey/pull/2514) ([tests](tests/valkey/2514-84fd62641/)): Refactor scanLaterList to fix latency issue (#2514): 0 of 5 test(s) reached the change, coverage 75%
- [valkey-io/valkey#2526](https://github.com/valkey-io/valkey/pull/2526) ([tests](tests/valkey/2526-c6c91d152/)): Fix assumptions that pthread functions set errno (#2526): declined: The change only corrects the error string logged when pthread_create or pthread_sigmask fails (using the returned error code instead of errno). Control flow is unchanged: pthread_create failure still exits(1), pthread_sigmask failure still only logs a warning. These failure paths fire only
- [valkey-io/valkey#2509](https://github.com/valkey-io/valkey/pull/2509) ([tests](tests/valkey/2509-23112fad8/)): fix hsetex handling of wrong number of fields (#2509): 2 of 2 test(s) reached the change, coverage 100%; crash, fixed by #2509 (recorded)
- [valkey-io/valkey#2523](https://github.com/valkey-io/valkey/pull/2523) ([tests](tests/valkey/2523-39d5c6e6e/)): Fix vset unittest compilation warning for bad signedness comparison (#2523): declined: The change edits only src/unit/test_vset.c, changing a unit-test loop counter from int to size_t to fix a compiler signedness warning. It is a build/test hygiene fix with no change to any server code path, command behaviour, reply, or data handling, so no fuzzer scenario can reach or exerc

### valkey-io/valkey-bloom

[2026-11-09T02:53:43+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42553337589): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-json

[2026-11-09T02:57:01+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42553627334): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-search

[2026-11-09T03:00:17+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42553890250): 0 merge(s) tested, 0 without a code change.


## Totals

- Merges whose tests reached the change: 627 of 921 tested (68%)
- Changed-line coverage: median 100% of 477 merge(s) measured, 36 under 50%
- Recall: 173 parents flagged of 258 fix merges
- Claude: $1189.51 in the last 24 hours, $6706.12 in the last 7 days, $6706.12 in all
- Issues filed: 60
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

- [valkey-io/valkey#3800](https://github.com/valkey-io/valkey/pull/3800): attempt 0 of 3, Claude Code failed (rc=1): API Error: Claude Code is unable to respond to this request, which appears to violate our Usage Policy (https://www.anthropic.com/legal/aup). This request triggered restrict
