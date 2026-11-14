# Fuzzer walk

Updated 2026-11-14T06:29+00:00.

| Repository | Done | Remaining | Backlog cursor | Last 24 hours | Last 7 days (per day) |
|---|---|---|---|---|---|
| valkey-io/valkey | 2279 | 261 | #13115 (2024-03-10) | 282 | 244.7 |
| valkey-io/valkey-bloom | 109 | 0 | ce516d0ef6e3 (2024-04-26) | 0 | 0.0 |
| valkey-io/valkey-json | 66 | 0 | c1423d7e4b38 (2024-11-25) | 2 | 0.6 |
| valkey-io/valkey-search | 688 | 1 | f8106e72e984 (2024-11-27) | 15 | 45.1 |

## Last runs

### valkey-io/valkey

[2026-11-14T05:10:43+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/43551524115): 20 merge(s) tested, 17 without a code change.

- [valkey-io/valkey#35](https://github.com/valkey-io/valkey/pull/35): to retry, 2 of 2 run(s) could not start: the run could not start: environment acquisition failed (artifacts.parent acquire: FileNotFoundError)
- [valkey-io/valkey#30](https://github.com/valkey-io/valkey/pull/30) ([tests](tests/valkey/30-1c9710fca/)): Fix the CI sentinel break issue (#30): declined: The change only updates the executable-name substring that enables Sentinel mode (redis-sentinel -> placeholderkv-sentinel during the project rename) and the matching binary name in the CI test harness (tests/instances.tcl). The behaviour depends solely on the filename the server binary is
- [valkey-io/valkey@bd7387354a9d](https://github.com/valkey-io/valkey/commit/bd7387354a9d9b9664ca6215bf3e6d955e07eb03) ([tests](tests/valkey/bd7387354/)): Fixed fork/exec problem: declined: The change only alters which process names (argv[0] basenames) trigger check-rdb/check-aof mode in main(): the old code matched 'redis-check-rdb'/'redis-check-aof', the fix matches 'placeholderkv-check-rdb'/'placeholderkv-check-aof', the names this era's Makefile actually builds. The dispa
- [valkey-io/valkey@9a6721bd11ca](https://github.com/valkey-io/valkey/commit/9a6721bd11ca9454b425a695c9455434564c2dd5) ([tests](tests/valkey/9a6721bd1/)): Fix check aof: declined: The change only edits the project's own test harness (tests/integration/aof.tcl, swapping the hardcoded binary path src/redis-check-aof for the renamed src/placeholderkv-check-aof in exec calls) and a help-text string in utils/generate-commands-json.py. No server code, tool code, or runtim
- [valkey-io/valkey@0aaae734f461](https://github.com/valkey-io/valkey/commit/0aaae734f46101e621f9d56d76c42d04029d3c99) ([tests](tests/valkey/0aaae734f/)): Fix incorrect find/replace: declined: The change only corrects a mangled library filename (libhiENGINE_ssl.a -> libhiredis_ssl.a) in src/Makefile's BUILD_TLS=yes/module link flags. It is a build-system fix: with the bug, a TLS build fails to link; with the fix, it links. No runtime code, command reply, replication, persistence
- [valkey-io/valkey@38632278fd06](https://github.com/valkey-io/valkey/commit/38632278fd06fe186f7707e4fa099f666d805547) ([tests](tests/valkey/38632278f/)): A single commit to get stuff building: declined: The commit touches only build and CI machinery and docs: it renames make variables (REDIS_CFLAGS/REDIS_LDFLAGS to SERVER_CFLAGS/SERVER_LDFLAGS), renames the produced binaries to placeholderkv-*, rearranges where -flto/-O3 flags are applied in src/Makefile, and updates .github/workflows/ci.
- [redis/redis#13154](https://github.com/redis/redis/pull/13154) ([tests](tests/valkey/13154-e64d91c37/)): Fix dict use-after-free problem in kvs->rehashing (#13154): 0 of 5 test(s) reached the change, coverage 50%
- [redis/redis#13148](https://github.com/redis/redis/pull/13148) ([tests](tests/valkey/13148-bad33f873/)): fix wrong data type conversion in zrangeResultBeginStore (#13148): 2 of 2 test(s) reached the change, coverage 100%
- [redis/redis#13141](https://github.com/redis/redis/pull/13141) ([tests](tests/valkey/13141-e04d41d78/)): Prevent lua error_reply abuse from causing errorstats to become larger (#13141): 2 of 2 test(s) reached the change, coverage 100%
- [redis/redis#13143](https://github.com/redis/redis/pull/13143) ([tests](tests/valkey/13143-aeada2014/)): Avoid unnecessary dict shrink in zremrangeGenericCommand (#13143): declined: The change only skips dictShrinkIfNeeded on a sorted set's dict when the ZREMRANGEBY* delete emptied it and dbDelete is about to free the entire object anyway; when members remain the shrink still runs as before. It is a performance-only refactor with no observable change to replies, keysp
- [redis/redis#13135](https://github.com/redis/redis/pull/13135) ([tests](tests/valkey/13135-7b070423b/)): Fix dictionary use-after-free in active expire and make kvstore iter to respect EMPTY flag (#13135): 0 of 5 test(s) reached the change, coverage 100%; crash, fixed by redis/redis#13135 (recorded)
- [redis/redis#13147](https://github.com/redis/redis/pull/13147) ([tests](tests/valkey/13147-98a6e55d4/)): Add missing REDIS_STATIC in quicklist (#13147): declined: The change only adds REDIS_STATIC to two forward declarations in quicklist.c so they match the static definitions of _quicklistSplitNode and _quicklistMergeNodes. REDIS_STATIC expands to 'static' (or nothing in test builds), so this affects symbol linkage/visibility at compile time only; n
- [redis/redis#13073](https://github.com/redis/redis/pull/13073) ([tests](tests/valkey/13073-1d77a8e2c/)): Makefile respect user's REDIS_CFLAGS and OPT (#13073): declined: The change is confined to src/Makefile: it relocates default optimization flags (-flto, -fno-omit-frame-pointer) from REDIS_CFLAGS into the OPTIMIZATION variable and adds $(OPT) to FINAL_LDFLAGS so user-provided REDIS_CFLAGS and OPT are honored. It is purely a build/CI flag-handling change
- [redis/redis#13072](https://github.com/redis/redis/pull/13072) ([tests](tests/valkey/13072-3b3d16f74/)): Add KVSTORE_FREE_EMPTY_DICTS to cluster mode keys / expires kvstore (#13072): 0 of 2 test(s) reached the change, coverage 100%
- [redis/redis#13108](https://github.com/redis/redis/pull/13108) ([tests](tests/valkey/13108-ad28d222e/)): Lua eval scripts first in first out LRU eviction (#13108): 2 of 2 test(s) reached the change, coverage 93%
- [redis/redis#13117](https://github.com/redis/redis/pull/13117) ([tests](tests/valkey/13117-a8e745117/)): Xread last entry in stream (#7388) (#13117): 3 of 3 test(s) reached the change, coverage 100%
- [redis/redis#13069](https://github.com/redis/redis/pull/13069) ([tests](tests/valkey/13069-9efc6ad6a/)): Add API RedisModule_ClusterKeySlot and RedisModule_ClusterCanonicalKeyNameInSlot (#13069): 2 of 2 test(s) reached the change, coverage 100%
- [redis/redis#13104](https://github.com/redis/redis/pull/13104) ([tests](tests/valkey/13104-9c065c417/)): Enable accept4() on *BSD (#13104): declined: The change only widens the HAVE_ACCEPT4 preprocessor guard in src/config.h to define it on OpenBSD 5.7+, FreeBSD 10+ and NetBSD 8.0+. On Linux, where the fuzzer builds and runs, __linux__ already defined HAVE_ACCEPT4 before and after the change, so the compiled binaries are identical and n
- [redis/redis#12958](https://github.com/redis/redis/pull/12958) ([tests](tests/valkey/12958-da727ad44/)): Fix redis-check-aof incorrectly considering data in manifest format as MP-AOF (#12958): 0 of 3 test(s) reached the change, coverage 0%
- [redis/redis#12986](https://github.com/redis/redis/pull/12986) ([tests](tests/valkey/12986-3c8d15f8c/)): Pick random slot for a node to distribute operation across slots in redis-benchmark (#12986): 0 of 5 test(s) reached the change, coverage 0%

### valkey-io/valkey-bloom

[2026-11-14T06:26:43+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/43566308255): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-json

[2026-11-14T06:29:03+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/43566499044): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-search

[2026-11-14T04:40:11+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/43553099941): 1 merge(s) tested, 0 without a code change.

- [valkey-io/valkey-search#1700](https://github.com/valkey-io/valkey-search/pull/1700): to retry, the job left no result

## Totals

- Merges whose tests reached the change: 906 of 1446 tested (63%)
- Changed-line coverage: median 100% of 937 merge(s) measured, 69 under 50%
- Recall: 235 parents flagged of 371 fix merges
- Claude: $588.95 in the last 24 hours, $6604.77 in the last 7 days, $10619.11 in all
- Issues filed: 78
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

- [valkey-io/valkey-search#1700](https://github.com/valkey-io/valkey-search/pull/1700): attempt 0 of 3, the job left no result
- [valkey-io/valkey#35](https://github.com/valkey-io/valkey/pull/35): attempt 0 of 3, 2 of 2 run(s) could not start: the run could not start: environment acquisition failed (artifacts.parent acquire: FileNotFoundError)
