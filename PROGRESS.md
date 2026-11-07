# Fuzzer walk

Updated 2026-11-07T11:56+00:00.

| Repository | Done | Remaining | Backlog cursor | Last 24 hours | Last 7 days (per day) |
|---|---|---|---|---|---|
| valkey-io/valkey | 612 | 1895 | #3509 (2026-04-17) | 300 | 87.4 |
| valkey-io/valkey-bloom | 109 | 0 | ce516d0ef6e3 (2024-04-26) | 0 | 15.6 |
| valkey-io/valkey-json | 62 | 0 | c1423d7e4b38 (2024-11-25) | 0 | 8.9 |
| valkey-io/valkey-search | 415 | 254 | #536 (2025-12-10) | 187 | 59.3 |

## Last runs

### valkey-io/valkey

[2026-11-07T10:39:11+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42354781729): 20 merge(s) tested, 5 without a code change.

- [valkey-io/valkey#3578](https://github.com/valkey-io/valkey/pull/3578): Fix Deferred Reply Placeholders in Active Deferred Buffers (#3578): error: 2 of 2 run(s) could not start: the run could not start: environment acquisition failed
- [valkey-io/valkey#3554](https://github.com/valkey-io/valkey/pull/3554) ([tests](tests/valkey/3554-28ecbd204/)): Ensure client slot migration pointer is cleared during reset (#3554): 0 of 2 test(s) reached the change, coverage 100%
- [valkey-io/valkey#3548](https://github.com/valkey-io/valkey/pull/3548) ([tests](tests/valkey/3548-a3e44a55d/)): Fix lua-enable-insecure-api default value cannot be changed to yes (#3548): declined: no proposed test validated
- [valkey-io/valkey#3557](https://github.com/valkey-io/valkey/pull/3557) ([tests](tests/valkey/3557-ac9ca9de3/)): Fix rdmaServer leaks when create listen cm id error (#3557): declined: The change only fixes a resource leak (event channel and addrinfo not freed) on an error path in rdmaServer when rdma_create_id fails during RDMA listener setup. It has no observable behaviour change: the listen attempt fails with the same error either way. Reaching the path requires an RD
- [valkey-io/valkey#3533](https://github.com/valkey-io/valkey/pull/3533) ([tests](tests/valkey/3533-bb8866557/)): hashtable: fix dismissHashtable madvise size (#3533): 0 of 6 test(s) reached the change, coverage 100%
- [valkey-io/valkey#3555](https://github.com/valkey-io/valkey/pull/3555) ([tests](tests/valkey/3555-edc0d26ad/)): Strip LTO flags from static Lua module build (#3555): declined: The change is build-system only: it strips LTO flags (-flto, -flto=auto, -ffat-lto-objects) from the static Lua module Makefile so that archiving into libvalkeylua.a does not produce LTO bitcode objects the system linker cannot read. No runtime behaviour, command reply, replication, persis
- [valkey-io/valkey@a7d495352ac0](https://github.com/valkey-io/valkey/commit/a7d495352ac0ba724eeec51fd57b615e8e805f52) ([tests](tests/valkey/a7d495352/)): extra UT: declined: The change adds only a new unit test (safe_iterator_release_before_cleanup) to src/unit/test_hashtable.cpp; no server, tool, or library code changes. There is no runtime behaviour to exercise, so no scenario is needed.
- [valkey-io/valkey@54980ece3a46](https://github.com/valkey-io/valkey/commit/54980ece3a46a1bf83eea40870542c09c843b8fa) ([tests](tests/valkey/54980ece3/)): hashtable iterator safety: invalidate on exhaustion: 0 of 9 test(s) reached the change, coverage 100%, 4 skipped (required capability 'build' is unsupported)
- [valkey-io/valkey#3545](https://github.com/valkey-io/valkey/pull/3545) ([tests](tests/valkey/3545-d2db0c268/)): Fix module commandresult event cleanup during unsubscribe and module unload (#3545): 0 of 5 test(s) reached the change, coverage 100%
- [valkey-io/valkey#3504](https://github.com/valkey-io/valkey/pull/3504) ([tests](tests/valkey/3504-5abf79e0e/)): Add zmalloc_aligned() and fix SPMC queue buffer alignment (#3504): 0 of 5 test(s) reached the change, coverage 81%
- [valkey-io/valkey#3535](https://github.com/valkey-io/valkey/pull/3535) ([tests](tests/valkey/3535-970984344/)): Optimize HGETDEL to pause auto shrink when deleting multiple items (#3535): 0 of 3 test(s) reached the change, coverage 100%
- [valkey-io/valkey#3541](https://github.com/valkey-io/valkey/pull/3541) ([tests](tests/valkey/3541-651c40a89/)): Fix FD leak in connSocketBlockingConnect on timeout (#3541): declined: The change only moves conn->fd = fd before the aeWait timeout check in connSocketBlockingConnect, closing an fd that was previously leaked on a connect timeout. Its sole caller is MIGRATE to an unresponsive target. The client-visible behaviour is identical on both builds: both send -IOERR 
- [valkey-io/valkey#3498](https://github.com/valkey-io/valkey/pull/3498): to retry, Claude Code failed (rc=1): API Error: Claude Code is unable to respond to this request, which appears to violate our Usage Policy (https://www.anthropic.com/legal/aup). This request triggered restrict
- [valkey-io/valkey#3458](https://github.com/valkey-io/valkey/pull/3458) ([tests](tests/valkey/3458-3ab9d9797/)): Fix race condition during async client freeing with IO threading enabled (#3458): 0 of 3 test(s) reached the change, coverage 33%
- [valkey-io/valkey#3518](https://github.com/valkey-io/valkey/pull/3518) ([tests](tests/valkey/3518-fc00f7be0/)): Fix VLA warning in io_threads (#3518): declined: The change only replaces a `const int BATCH_SIZE = 32` local with `#define BATCH_SIZE 32` in io_threads.c so the `batch_jobs[BATCH_SIZE]` array is no longer a compiler-flagged VLA. The array size and all runtime behavior are identical; this is a warning-silencing refactor with no observabl
- [valkey-io/valkey#2936](https://github.com/valkey-io/valkey/pull/2936): to retry, 5 of 5 run(s) could not start: the run could not start: environment acquisition failed
- [valkey-io/valkey#3520](https://github.com/valkey-io/valkey/pull/3520) ([tests](tests/valkey/3520-9d51f5ff8/)): Document VALKEYCLI_HOST/PORT variables in help (#3520): declined: The change only edits the static usage/help text printed by valkey-cli (documenting the pre-existing VALKEYCLI_HOST/VALKEYCLI_PORT environment variables for -h and -p). No code path that parses options, reads the environment variables, connects, or executes commands is modified, so there i
- [valkey-io/valkey#3392](https://github.com/valkey-io/valkey/pull/3392) ([tests](tests/valkey/3392-0327c2713/)): Add Static Module Support (#3392): 6 of 15 test(s) reached the change, coverage 59%
- [valkey-io/valkey#3306](https://github.com/valkey-io/valkey/pull/3306) ([tests](tests/valkey/3306-269b1c5ed/)): Improve COB memory tracking with copy avoidance (#3306): 0 of 4 test(s) reached the change, coverage 95%
- [valkey-io/valkey#3516](https://github.com/valkey-io/valkey/pull/3516) ([tests](tests/valkey/3516-4a42c9585/)): Fix HPERSIST RESP protocol violation on wrong-type key (#3516): 2 of 2 test(s) reached the change, coverage 100%

### valkey-io/valkey-bloom

[2026-11-07T11:55:15+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42370159341): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-json

[2026-11-07T11:56:32+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42370248414): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-search

[2026-11-07T11:46:02+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42352953225): 20 merge(s) tested, 2 without a code change.

- [valkey-io/valkey-search#603](https://github.com/valkey-io/valkey-search/pull/603) ([tests](tests/valkey-search/603-423716ace/)): Followup of #441 Position Optimization (#603): 0 of 2 test(s) reached the change; crash, predates #603, since fixed (recorded)
- [valkey-io/valkey-search#587](https://github.com/valkey-io/valkey-search/pull/587) ([tests](tests/valkey-search/587-1ba297abc/)): Add a new config to enable mutation queue draining before RDB save (#587): 2 of 2 test(s) reached the change
- [valkey-io/valkey-search#591](https://github.com/valkey-io/valkey-search/pull/591) ([tests](tests/valkey-search/591-4f003bf67/)): Addressing memory deallocation overhead by minimizing the mutex contension (#591): 0 of 6 test(s) reached the change; crash, predates #591, since fixed (recorded)
- [valkey-io/valkey-search#604](https://github.com/valkey-io/valkey-search/pull/604) ([tests](tests/valkey-search/604-543e97ac8/)): Add optimization for pure exact phrase queries by avoiding the filtering step and finishing the search in entries fetcher (#604): 0 of 5 test(s) reached the change; crash, predates #604, since fixed (recorded); crash, introduced and since fixed (recorded)
- [valkey-io/valkey-search#602](https://github.com/valkey-io/valkey-search/pull/602) ([tests](tests/valkey-search/602-4b6878bf7/)): fixing tag query parsing (#602): declined: The only behavioral change is in src/coordinator/search_converter.cc: gRPC fanout requests now re-parse tag query strings with the query-language '|' separator (FilterParser::ParseQueryTags) instead of the index's schema separator. That code runs only when the coordinator is active, which 
- [valkey-io/valkey-search#600](https://github.com/valkey-io/valkey-search/pull/600) ([tests](tests/valkey-search/600-96ad1caad/)): Fix build warnings (#600): declined: The change only adds the `override` specifier to five already-overriding virtual methods in src/query/predicate.h (no change to dispatch or behavior, just warning suppression) and moves a Snowball include directory from PUBLIC to PRIVATE in third_party/snowball/CMakeLists.txt (build-time i
- [valkey-io/valkey-search#597](https://github.com/valkey-io/valkey-search/pull/597) ([tests](tests/valkey-search/597-0f9360759/)): Update QueryOperations to be sent via protobuf allowing remote nodes to benefit from optimizations (#597): 2 of 2 test(s) reached the change; [regressed-fix, the fix of #597 regressed](https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/91)
- [valkey-io/valkey-search#596](https://github.com/valkey-io/valkey-search/pull/596) ([tests](tests/valkey-search/596-7c28ad201/)): Apply clang format to text_index.* (#596): declined: The change only applies clang-format to text_index.cc and text_index.h: every hunk merely moves '&' and '*' from the type to the identifier (whitespace/style placement). No declaration, logic, locking, or data structure changes semantically, so the module's behavior is identical on both bu
- [valkey-io/valkey-search@97c1ae34bc5c](https://github.com/valkey-io/valkey-search/commit/97c1ae34bc5c02a6ca85f28c1a24131af3a1147e) ([tests](tests/valkey-search/97c1ae34b/)): Merge pull request #594 from valkey-io/fulltext: 4 of 4 test(s) reached the change
- [valkey-io/valkey-search#452](https://github.com/valkey-io/valkey-search/pull/452) ([tests](tests/valkey-search/452-1b165430e/)): Fix tag OR syntax in hybrid queries and add filter expression tests (#452): 3 of 3 test(s) reached the change; crash, predates #452, since fixed (recorded)
- [valkey-io/valkey-search#569](https://github.com/valkey-io/valkey-search/pull/569) ([tests](tests/valkey-search/569-6b172614f/)): Fix cross-module memory allocator crash by hiding wrapper symbols (#569): declined: This is a build/link change only: the linker version script now hides __wrap_*, __real_* and operator new/delete symbols from libsearch.so's dynamic symbol table (plus a new Python integration test). Its sole behavioural effect is on ELF symbol interposition when a second module (valkey-js
- [valkey-io/valkey-search#565](https://github.com/valkey-io/valkey-search/pull/565) ([tests](tests/valkey-search/565-a3fdb5c8f/)): Fix use-after-free race in DefaultReplyScoreAs() (#565): 0 of 2 test(s) reached the change
- [valkey-io/valkey-search#556](https://github.com/valkey-io/valkey-search/pull/556) ([tests](tests/valkey-search/556-df918dd23/)): Fix delete bug (#556): 0 of 2 test(s) reached the change
- [valkey-io/valkey-search#548](https://github.com/valkey-io/valkey-search/pull/548) ([tests](tests/valkey-search/548-0e4b6dbb8/)): Fix deprecated std::atomic_load/store warnings for shared_ptr (#548): declined: The change only adds #pragma GCC diagnostic push/ignored(-Wdeprecated-declarations)/pop around the existing std::atomic_load/std::atomic_store calls on cluster_map_ in valkey_search.cc and valkey_search.h. No code path, argument, or logic changes: the same atomic load/store calls execute i
- [valkey-io/valkey-search#504](https://github.com/valkey-io/valkey-search/pull/504) ([tests](tests/valkey-search/504-c812cc4c6/)): Update arguments for ft.create and ft.search json (#369) (#504): declined: The change edits only the command-documentation metadata files src/commands/ft.aggregate.json, ft.create.json and ft.search.json, which document the existing argument grammar for the valkey.io command reference. These JSON files are not consumed anywhere: src/commands/CMakeLists.txt compil
- [valkey-io/valkey-search#537](https://github.com/valkey-io/valkey-search/pull/537) ([tests](tests/valkey-search/537-f31c52efb/)): Implement background search neighbor deque limiting optimization  (#537): 2 of 2 test(s) reached the change; crash, predates #537, since fixed (recorded)
- [valkey-io/valkey-search#550](https://github.com/valkey-io/valkey-search/pull/550) ([tests](tests/valkey-search/550-567f32e54/)): Better handling of nullptr in FromString String config (#550): declined: The change hardens vmsdk config::String::FromString to reject a string_view with a null data pointer instead of silently ignoring it. This path is unreachable from any server-visible input: FromString is only invoked by ModuleConfigManager::UpdateConfigFromKeyVal when parsing module-load a
- [valkey-io/valkey-search#542](https://github.com/valkey-io/valkey-search/pull/542) ([tests](tests/valkey-search/542-1c9931284/)): Add general utility thread pool for "execute later at low priority" routines and schedule Search Result deletions on this  (#542): 2 of 2 test(s) reached the change
- [valkey-io/valkey-search#549](https://github.com/valkey-io/valkey-search/pull/549) ([tests](tests/valkey-search/549-3b15005be/)): Fix non-vector aggregation result. Enhance Aggregation tests (#549): 2 of 2 test(s) reached the change
- [valkey-io/valkey-search#547](https://github.com/valkey-io/valkey-search/pull/547) ([tests](tests/valkey-search/547-a25fcad6c/)): Add getter in config for the default value (#547): declined: The change adds an internal GetDefaultValue() accessor to the vmsdk config classes that no production code in valkey-search calls (only the new unit test uses it), fixes a comment, and reworks String::FromString to record the default — a path reachable only when a string-typed module confi

## Totals

- Merges whose tests reached the change: 423 of 617 tested (69%)
- Changed-line coverage: median 100% of 286 merge(s) measured, 18 under 50%
- Recall: 126 parents flagged of 186 fix merges
- Claude: $1785.90 in the last 24 hours, $4399.40 in the last 7 days, $4399.40 in all
- Issues filed: 44
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

## Retries

- [valkey-io/valkey#3800](https://github.com/valkey-io/valkey/pull/3800): attempt 0 of 3, Claude Code failed (rc=1): API Error: Claude Code is unable to respond to this request, which appears to violate our Usage Policy (https://www.anthropic.com/legal/aup). This request triggered restrict
- [valkey-io/valkey#3613](https://github.com/valkey-io/valkey/pull/3613): attempt 0 of 3, Claude Code failed (rc=1): API Error: Claude Code is unable to respond to this request, which appears to violate our Usage Policy (https://www.anthropic.com/legal/aup). This request triggered restrict
- [valkey-io/valkey#3498](https://github.com/valkey-io/valkey/pull/3498): attempt 0 of 3, Claude Code failed (rc=1): API Error: Claude Code is unable to respond to this request, which appears to violate our Usage Policy (https://www.anthropic.com/legal/aup). This request triggered restrict
- [valkey-io/valkey#2936](https://github.com/valkey-io/valkey/pull/2936): attempt 1 of 3, 5 of 5 run(s) could not start: the run could not start: environment acquisition failed
