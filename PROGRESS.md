# Fuzzer walk

Updated 2026-11-13T16:04+00:00.

| Repository | Done | Remaining | Backlog cursor | Last 24 hours | Last 7 days (per day) |
|---|---|---|---|---|---|
| valkey-io/valkey | 2094 | 442 | #406 (2024-04-30) | 123 | 248.0 |
| valkey-io/valkey-bloom | 109 | 0 | ce516d0ef6e3 (2024-04-26) | 0 | 0.0 |
| valkey-io/valkey-json | 64 | 0 | c1423d7e4b38 (2024-11-25) | 0 | 0.3 |
| valkey-io/valkey-search | 681 | 0 | f8106e72e984 (2024-11-27) | 8 | 60.9 |

## Last runs

### valkey-io/valkey

[2026-11-13T15:43:18+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/43435021082): 20 merge(s) tested, 6 without a code change.

- [valkey-io/valkey#455](https://github.com/valkey-io/valkey/pull/455) ([tests](tests/valkey/455-1aca85e3d/)): Update module api and variable to valkey accordingly. (#455): declined: The change is a pure mechanical rename inside src/module.c: the private struct field ValkeyModuleCommand->rediscmd becomes serverCmd, two static functions (freeRedisModuleAsyncRMCallPromise, flushRedisModuleIOBuffer) are renamed with their call sites, and two comments swap 'Redis' for 'Val
- [valkey-io/valkey#448](https://github.com/valkey-io/valkey/pull/448) ([tests](tests/valkey/448-2ed71de8e/)): Migrate util.c unit tests to new test framework (#448): declined: The change is a pure test-infrastructure migration: it moves util.c's SERVER_TEST-only unit tests (string2ll, string2l, ll2string, ld2string, fixedpoint_d2string, reclaimFilePageCache) into the new unit test framework (src/unit/test_util.c, test_files.h) and deletes the old #ifdef SERVER_T
- [valkey-io/valkey#451](https://github.com/valkey-io/valkey/pull/451) ([tests](tests/valkey/451-cde8ec1b4/)): Don't try to set SO_REUSEADDR on sockets of AF_UNIX (#451): 0 of 1 test(s) reached the change, coverage 100%
- [valkey-io/valkey#324](https://github.com/valkey-io/valkey/pull/324) ([tests](tests/valkey/324-ba9dd7b23/)): Add noscores option to command ZSCAN. (#324): 2 of 2 test(s) reached the change, coverage 93%
- [valkey-io/valkey#445](https://github.com/valkey-io/valkey/pull/445) ([tests](tests/valkey/445-6e7af9471/)): Slot migration improvement (#445): 7 of 13 test(s) reached the change, coverage 81%; crash, introduced and since fixed (recorded); crash, introduced and since fixed (recorded)
- [valkey-io/valkey#440](https://github.com/valkey-io/valkey/pull/440) ([tests](tests/valkey/440-93f8a19b6/)): Change strlcat function name from redis to valkey (#440): declined: The change only renames the internal C helper redis_strlcat to valkey_strlcat (declaration in util.h, definition in strl.c, two call sites in rdb.c's rdbRemoveTempFile, and the text of a compiler deprecation attribute in fmacros.h). The function body and call-site semantics are unchanged; 
- [valkey-io/valkey#437](https://github.com/valkey-io/valkey/pull/437) ([tests](tests/valkey/437-1b3199e07/)): Fix unit test issues on address sanitizer and fortify (#437): declined: The change only edits CI workflow steps (.github/workflows/daily.yml) and unit-test source files (src/unit/test_crc64combine.c, src/unit/test_intset.c): an uninitialized-variable fix in a test benchmark helper, removal of a leaked duplicate intsetNew() call in a test, and GCC pragmas suppr
- [valkey-io/valkey#327](https://github.com/valkey-io/valkey/pull/327) ([tests](tests/valkey/327-cc703aa3b/)): Input output traffic stats and command process count for each client. (#327): 1 of 1 test(s) reached the change, coverage 100%
- [valkey-io/valkey#436](https://github.com/valkey-io/valkey/pull/436) ([tests](tests/valkey/436-9ebbd5f03/)): Fix issues for older versions of Darwin and improve PowerPC support (#436): declined: The change is entirely platform-conditional build/portability code for older Darwin and Apple PowerPC: macro-name fixes in src/config.h (MAC_OS_X_VERSION_MAX_ALLOWED spelling, BYTE_ORDER for __APPLE__&&__POWERPC__) and crash-handler register access in src/debug.c behind __APPLE__/__ppc__/_
- [valkey-io/valkey#433](https://github.com/valkey-io/valkey/pull/433) ([tests](tests/valkey/433-472c1ca26/)): Update links in module API docs (generated from module.c) (#433): declined: The change only rewrites URLs inside C comments in src/module.c (documentation strings used to generate the module API docs) and fixes a require path in utils/generate-module-api-doc.rb, a standalone doc-generation Ruby script. No compiled code, command behaviour, reply, replication, persi
- [valkey-io/valkey#430](https://github.com/valkey-io/valkey/pull/430) ([tests](tests/valkey/430-8f974484e/)): Log the real reason for why posix_fadvise failed (#430): declined: The change only sets errno to posix_fadvise's return code before returning -1 in reclaimFilePageCache (src/util.c), so callers log the real failure reason via strerror. The function's return value and all behavior are unchanged; the only observable effect is the wording of a log warning, a
- [valkey-io/valkey#344](https://github.com/valkey-io/valkey/pull/344) ([tests](tests/valkey/344-5b1fd222e/)): An initial simple unit test framework (#344): declined: The change only adds a standalone unit test framework: a new src/unit/ directory, a valkey-unit-tests binary and Makefile/CI wiring, and it moves the existing #ifdef SERVER_TEST test code for crc64 and intset out of the production files into that framework. The production implementations o
- [valkey-io/valkey#416](https://github.com/valkey-io/valkey/pull/416) ([tests](tests/valkey/416-443d80f16/)): Fix typo in comment in quicklist.h (#416): declined: The change only fixes a typo in a comment in src/quicklist.h ('of of' -> 'of the'). Comments are stripped at compile time, so no runtime behaviour, reply, persistence, replication, or build path is affected; there is nothing a scenario could exercise.
- [valkey-io/valkey#389](https://github.com/valkey-io/valkey/pull/389) ([tests](tests/valkey/389-68ca258b0/)): Changed links and naming to valkey instead of redis (#389): declined: The change edits only documentation text (00-RELEASENOTES, MANIFESTO, deps/README.md, deps/linenoise/README.markdown, src/commands/README.md), renaming Redis to Valkey in prose and links. No source, build, config, command or tool code changes, so there is no runtime behaviour to test.
- [valkey-io/valkey#393](https://github.com/valkey-io/valkey/pull/393) ([tests](tests/valkey/393-8abeb79f5/)): Rename redis in aof logs and proc title redis-aof-rewrite to valkey-aof-rewrite (#393): 0 of 8 test(s) reached the change, coverage 80%
- [valkey-io/valkey#350](https://github.com/valkey-io/valkey/pull/350) ([tests](tests/valkey/350-f4e10eee0/)): CRC64 perf improvements from Redis patches (#350): 0 of 6 test(s) reached the change, coverage 95%
- [valkey-io/valkey#404](https://github.com/valkey-io/valkey/pull/404) ([tests](tests/valkey/404-89f72bc3a/)): Don't include config.h from serverassert.h (#404): declined: The change only removes the #include of config.h from serverassert.h and inlines equivalent definitions of valkey_unreachable and likely (same __builtin_unreachable/abort and __builtin_expect expansions guarded by the same GCC version checks). The assert/panic macros expand identically on 
- [valkey-io/valkey#400](https://github.com/valkey-io/valkey/pull/400) ([tests](tests/valkey/400-44f273d13/)): Delete unused declaration (#400): declined: The change only deletes an unused function declaration (dictEntryMetadata) from src/dict.h. It is a header cleanup with no code, behavior, reply, or data-path change; no runtime scenario can distinguish the two builds.
- [valkey-io/valkey#376](https://github.com/valkey-io/valkey/pull/376) ([tests](tests/valkey/376-e43fda568/)): Modify mem_freed variable in evict.c and Update debug.c (#376): declined: The change is a pure refactor with no behavior change: debug.c only fixes a comment typo ('the the' -> 'the'), and evict.c moves the 'mem_freed = 0' initialization from a statement before the eviction loop into the variable declaration. The early 'goto update_metrics' paths that bypass the
- [valkey-io/valkey#378](https://github.com/valkey-io/valkey/pull/378) ([tests](tests/valkey/378-0cb97653b/)): Change default pidfile from redis.pid to valkey.pid (#378): declined: The change only renames the default pidfile path constant used when the server is daemonized with no pidfile configured, plus test-asset configs and a comment. The pidfile config still defaults to empty, so CONFIG GET pidfile and every command reply are identical on both builds; the defaul

### valkey-io/valkey-bloom

[2026-11-13T16:04:05+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/43458794102): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-json

[2026-11-13T15:33:31+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/43453660060): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-search

[2026-11-13T15:50:29+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/43456456374): 0 merge(s) tested, 0 without a code change.


## Totals

- Merges whose tests reached the change: 882 of 1411 tested (63%)
- Changed-line coverage: median 100% of 909 merge(s) measured, 66 under 50%
- Recall: 230 parents flagged of 364 fix merges
- Claude: $406.28 in the last 24 hours, $7534.06 in the last 7 days, $10353.37 in all
- Issues filed: 77
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
