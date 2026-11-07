# Fuzzer walk

Updated 2026-11-07T12:35+00:00.

| Repository | Done | Remaining | Backlog cursor | Last 24 hours | Last 7 days (per day) |
|---|---|---|---|---|---|
| valkey-io/valkey | 638 | 1869 | #3366 (2026-04-02) | 326 | 91.1 |
| valkey-io/valkey-bloom | 109 | 0 | ce516d0ef6e3 (2024-04-26) | 0 | 15.6 |
| valkey-io/valkey-json | 62 | 0 | c1423d7e4b38 (2024-11-25) | 0 | 8.9 |
| valkey-io/valkey-search | 415 | 254 | #536 (2025-12-10) | 187 | 59.3 |

## Last runs

### valkey-io/valkey

[2026-11-07T12:35:33+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42364860296): 20 merge(s) tested, 7 without a code change.

- [valkey-io/valkey#2936](https://github.com/valkey-io/valkey/pull/2936): to retry, 6 of 6 run(s) could not start: the run could not start: environment acquisition failed
- [valkey-io/valkey#3464](https://github.com/valkey-io/valkey/pull/3464) ([tests](tests/valkey/3464-b2d08c9ef/)): Fix use-after-unload crash in test auth module's blocking thread (#3464): declined: The change is confined to a test module's unload cleanup (tests/modules/auth.c ValkeyModule_OnUnload), adding a pthread_join to avoid a use-after-unload crash when dlclose unmaps the module while a 500ms blocking-auth thread is still sleeping. The crash is only reachable via MODULE UNLOAD 
- [valkey-io/valkey#3460](https://github.com/valkey-io/valkey/pull/3460) ([tests](tests/valkey/3460-8a91a1239/)): Unique samples in hashtableSampleEntries (#3460): 0 of 3 test(s) reached the change, coverage 100%
- [valkey-io/valkey#3396](https://github.com/valkey-io/valkey/pull/3396) ([tests](tests/valkey/3396-b2e0f6322/)): Add cluster bus network traffic usage metric in bytes (#3396): 2 of 4 test(s) reached the change, coverage 92%
- [valkey-io/valkey#3499](https://github.com/valkey-io/valkey/pull/3499) ([tests](tests/valkey/3499-a34f125d8/)): Change rdbSaveStreamConsumers return type from size_t to ssize_t (#3499): declined: The change only alters the declared return type of rdbSaveStreamConsumers from size_t to ssize_t. The function already returned -1 on error and its sole caller assigns the result to an ssize_t and compares with -1; the previous implicit size_t/ssize_t conversions round-tripped -1 unchanged
- [valkey-io/valkey#3402](https://github.com/valkey-io/valkey/pull/3402) ([tests](tests/valkey/3402-baa5a48b5/)): Env: Read VALKEYCLI_HOST and VALKEYCLI_PORT environment variables (#3402): 1 of 1 test(s) reached the change, coverage 100%; [crash, appeared after #3402](https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/81)
- [valkey-io/valkey#3472](https://github.com/valkey-io/valkey/pull/3472) ([tests](tests/valkey/3472-c4db208e9/)): enhance raxLowWalk: use memchr() for child-edge lookup in non-compressed nodes (#3472): declined: The change replaces the open-coded first-match byte scan in raxLowWalk's non-compressed branch with memchr(), which returns the same first-occurrence index and the same not-found condition; the only divergent state, the local j after a not-found break, is never observable because *splitpos
- [valkey-io/valkey#3324](https://github.com/valkey-io/valkey/pull/3324) ([tests](tests/valkey/3324-a3a839972/)): Redesign IO threading communication model (#3324): 2 of 20 test(s) reached the change, coverage 84%
- [valkey-io/valkey#3122](https://github.com/valkey-io/valkey/pull/3122) ([tests](tests/valkey/3122-0ec173351/)): Adds new (hidden) ValkeyModule_CallArgv API functions (#3122): 2 of 12 test(s) reached the change, coverage 70%
- [valkey-io/valkey#3372](https://github.com/valkey-io/valkey/pull/3372) ([tests](tests/valkey/3372-2871efd43/)): Add cluster-config-save-behavior option to control nodes.conf save behavior (#3372): 2 of 5 test(s) reached the change, coverage 100%
- [valkey-io/valkey#3471](https://github.com/valkey-io/valkey/pull/3471) ([tests](tests/valkey/3471-c0289c6a7/)): Defer argument redaction by storing indices instead of rewriting argv (#3471): 3 of 6 test(s) reached the change, coverage 100%
- [valkey-io/valkey#3453](https://github.com/valkey-io/valkey/pull/3453) ([tests](tests/valkey/3453-be52d8b81/)): Fix trivial double-free issue in rdbLoadObject (#3453): declined: The fix removes a double-free in rdbLoadObject's HASH_ZIPMAP->listpack conversion (src/rdb.c:2409-2421). The only sub-condition that actually reaches the double-free is lpSafeToAdd() failing after the field was already added to dupSearchHashtable; the other error-branch triggers (duplicate
- [valkey-io/valkey#3440](https://github.com/valkey-io/valkey/pull/3440) ([tests](tests/valkey/3440-890b82ac3/)): Fix config rewrite producing negative values for unsigned memory configs (#3440): 3 of 3 test(s) reached the change, coverage 100%; crash, fixed by #3440 (recorded)
- [valkey-io/valkey#3443](https://github.com/valkey-io/valkey/pull/3443) ([tests](tests/valkey/3443-dba128f13/)): Fix slot-migration-max-failover-repl-bytes unable to accept -1 (#3443): 3 of 4 test(s) reached the change, coverage 100%
- [valkey-io/valkey#3422](https://github.com/valkey-io/valkey/pull/3422) ([tests](tests/valkey/3422-2585f60eb/)): Fix RDB expiry write length and leak when loading zipmap (#3422): 0 of 3 test(s) reached the change, coverage 100%
- [valkey-io/valkey#3397](https://github.com/valkey-io/valkey/pull/3397) ([tests](tests/valkey/3397-034d16ac4/)): Increase embedded string threshold from 64 to 128 bytes (#3397): 2 of 4 test(s) reached the change, coverage 100%
- [valkey-io/valkey#3309](https://github.com/valkey-io/valkey/pull/3309) ([tests](tests/valkey/3309-ecba3e312/)): Grammar, spelling, and punctuation fixes across the summary fields for Valkey commands. (#3309): 2 of 2 test(s) reached the change
- [valkey-io/valkey#3420](https://github.com/valkey-io/valkey/pull/3420) ([tests](tests/valkey/3420-0c81b53d6/)): Avoid having server.h being included by cli and benchmark (#3420): declined: Pure refactor with no behavior change: the diff moves the COMMAND_* and CMD_* macro definitions verbatim from server.h to commands.h so cli/benchmark need not include server.h, swaps server.h for stdint.h/zmalloc.h/util.h includes in crc16.c and fuzzer_command_generator.c, and reformats th
- [valkey-io/valkey#3439](https://github.com/valkey-io/valkey/pull/3439) ([tests](tests/valkey/3439-e8ca6c07b/)): Fix VLA warning in linenoise and enable -Werror (#3439): declined: The change is build-only with no behavior change: it replaces a const-int VLA dimension in deps/linenoise/linenoise.c with an equivalent #define of the same value (8), leaving the buffer size, loop bounds and escape-sequence parsing identical, and adds -Werror to the linenoise Makefile. Co
- [valkey-io/valkey#3401](https://github.com/valkey-io/valkey/pull/3401) ([tests](tests/valkey/3401-1db8bab50/)): Big endian bitmap byte order mismatch fix (#3401): 0 of 3 test(s) reached the change, 1 skipped (required capability 'save-behavior-support' is unsupported)

### valkey-io/valkey-bloom

[2026-11-07T12:26:01+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42372408477): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-json

[2026-11-07T12:27:10+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42372503053): 0 merge(s) tested, 0 without a code change.


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

- Merges whose tests reached the change: 433 of 630 tested (69%)
- Changed-line coverage: median 100% of 297 merge(s) measured, 18 under 50%
- Recall: 129 parents flagged of 191 fix merges
- Claude: $1873.07 in the last 24 hours, $4486.58 in the last 7 days, $4486.58 in all
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
- [valkey-io/valkey#2936](https://github.com/valkey-io/valkey/pull/2936): attempt 2 of 3, 6 of 6 run(s) could not start: the run could not start: environment acquisition failed
