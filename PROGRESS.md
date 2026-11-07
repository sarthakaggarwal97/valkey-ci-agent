# Fuzzer walk

Updated 2026-11-07T14:02+00:00.

| Repository | Done | Remaining | Backlog cursor | Last 24 hours | Last 7 days (per day) |
|---|---|---|---|---|---|
| valkey-io/valkey | 638 | 1869 | #3366 (2026-04-02) | 280 | 91.1 |
| valkey-io/valkey-bloom | 109 | 0 | ce516d0ef6e3 (2024-04-26) | 0 | 15.6 |
| valkey-io/valkey-json | 62 | 0 | c1423d7e4b38 (2024-11-25) | 0 | 8.9 |
| valkey-io/valkey-search | 435 | 234 | #401 (2025-11-21) | 180 | 62.1 |

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

[2026-11-07T14:02:59+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42379692318): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-json

[2026-11-07T13:32:26+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42377377106): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-search

[2026-11-07T13:32:51+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42369561194): 20 merge(s) tested, 2 without a code change.

- [valkey-io/valkey-search#536](https://github.com/valkey-io/valkey-search/pull/536) ([tests](tests/valkey-search/536-f06b8b2ca/)): ignore venv (#536): declined: The change only adds venv/, env/, .venv/ and .env/ entries to .gitignore. It is a repository housekeeping change with no code, build, or runtime behaviour change in the valkey-search module, so there is nothing a scenario could exercise or compare.
- [valkey-io/valkey-search#533](https://github.com/valkey-io/valkey-search/pull/533) ([tests](tests/valkey-search/533-3f444fae0/)): Feature/ft create sortable (#533): 2 of 2 test(s) reached the change
- [valkey-io/valkey-search#531](https://github.com/valkey-io/valkey-search/pull/531) ([tests](tests/valkey-search/531-4da6e969e/)): refresh cluster map in background (#531): declined: The change adds one branch in ValkeySearch::OnServerCronCallback that refreshes the cluster map in the background, gated on IsCluster() && UsingCoordinator(). UsingCoordinator() is true only when the hidden, startup-only 'use-coordinator' config is set, and the only mechanism that sets it 
- [valkey-io/valkey-search#528](https://github.com/valkey-io/valkey-search/pull/528) ([tests](tests/valkey-search/528-c3fa174e8/)): Change default Valkey to 9.0.1 release (#528): declined: The change is a CI version bump (scripts/common.rc, testing/integration/run.sh set default Valkey to 9.0.1) plus raising kMinimumServerVersion in src/version.h from 8.1.1 to 9.0.1. That constant is read only once, in vmsdk/src/module.cc's load-time gate: if the server version is below it, 
- [valkey-io/valkey-search#527](https://github.com/valkey-io/valkey-search/pull/527) ([tests](tests/valkey-search/527-ed0648404/)): Fix Aggregation processing of LOAD clause and validation of vector query parameters. (#527): 2 of 2 test(s) reached the change; crash, predates #527, since fixed (recorded)
- [valkey-io/valkey-search#524](https://github.com/valkey-io/valkey-search/pull/524) ([tests](tests/valkey-search/524-31d37d91d/)): use negate flag parser in ft.info parser (#524): declined: Pure refactor with no behaviour change: the diff replaces two inline lambda parsers for FT.INFO's ALLSHARDS and INCONSISTENT keywords with the GENERATE_NEGATED_FLAG_PARSER macro, which expands (vmsdk/src/command_parser.h:211-216) to an identical lambda that sets the same field (enable_part
- [valkey-io/valkey-search#510](https://github.com/valkey-io/valkey-search/pull/510) ([tests](tests/valkey-search/510-990efa0ee/)): Fix replica crash by using legacy ACL path during replication (#510): 0 of 1 test(s) reached the change
- [valkey-io/valkey-search#410](https://github.com/valkey-io/valkey-search/pull/410) ([tests](tests/valkey-search/410-fd9d5ea4b/)): Full support for DB numbers other than zero in cluster mode. (#410): 4 of 4 test(s) reached the change; crash, introduced and since fixed (recorded); crash, fixed by #410 (recorded); crash, fixed by #410 (recorded); crash, fixed by #410 (recorded)
- [valkey-io/valkey-search#522](https://github.com/valkey-io/valkey-search/pull/522) ([tests](tests/valkey-search/522-6706ea268/)): Limit and Nocontent support for non vector queries (#522): 2 of 2 test(s) reached the change
- [valkey-io/valkey-search#469](https://github.com/valkey-io/valkey-search/pull/469) ([tests](tests/valkey-search/469-b81574a20/)): FT.INFO partition and consistency controls (#469): 3 of 3 test(s) reached the change; crash, predates #469, since fixed (recorded)
- [valkey-io/valkey-search#464](https://github.com/valkey-io/valkey-search/pull/464) ([tests](tests/valkey-search/464-0026dfcf1/)): FT.SEARCH partition and consistency controls (#464): 2 of 2 test(s) reached the change
- [valkey-io/valkey-search#299](https://github.com/valkey-io/valkey-search/pull/299) ([tests](tests/valkey-search/299-c50f8413a/)): feat: set command info for ft.create (#299): 2 of 2 test(s) reached the change; crash, introduced and since fixed (recorded); crash, fixed by #299 (recorded); crash, fixed by #299 (recorded)
- [valkey-io/valkey-search#496](https://github.com/valkey-io/valkey-search/pull/496) ([tests](tests/valkey-search/496-de6457112/)): Fix Override Specifier and Suppress MacOS Compiler Warning (#496): declined: The change modifies only CMakeLists.txt: it reformats the SAN_BUILD cache variable and sanitizer messages, and adds -Wno-defaulted-function-deleted on Apple builds to suppress a macOS compiler warning. The diff is complete (diff_truncated: false) and contains no C++ source changes, so no r
- [valkey-io/valkey-search#503](https://github.com/valkey-io/valkey-search/pull/503) ([tests](tests/valkey-search/503-d09a78fc9/)): Fix warnings and formatting (#503): declined: The change is a warnings-and-formatting cleanup with no behaviour change. The only production-code edit adds `default: CHECK(false)` to the KeyAccessToStringView switch in src/acl.cc, which is unreachable: acl::KeyAccess has exactly two enumerators (kRead, kWrite) and every call site passe
- [valkey-io/valkey-search#480](https://github.com/valkey-io/valkey-search/pull/480) ([tests](tests/valkey-search/480-57e5dfde0/)): Populate fingerprint and version to IndexSchema (#480): declined: Every behavioral change sits behind the coordinator, which only the module load-time argument --use-coordinator can enable: the 'use-coordinator' config is built Hidden (valkey_search_options.cc:129) and ModuleConfigManager::Init skips hidden entries, so it is not registered with Valkey's 
- [valkey-io/valkey-search#428](https://github.com/valkey-io/valkey-search/pull/428) ([tests](tests/valkey-search/428-cad62657f/)): Implement queue wait time aware local node preference in fanout operations (#428): 2 of 2 test(s) reached the change
- [valkey-io/valkey-search#495](https://github.com/valkey-io/valkey-search/pull/495) ([tests](tests/valkey-search/495-be8b2368f/)): Fix flaky save/restore test (#495): declined: The change is a test fix plus diagnostics: the integration test gains a wait for index backfill after restart (test code only), log.cc loses a blank line, and module OnLoad gains a read-only loop over /proc/self/maps that logs each loaded .so's base address at NOTICE level to make addr2lin
- [valkey-io/valkey-search#494](https://github.com/valkey-io/valkey-search/pull/494): to retry, building valkey-search left no .build-release/libsearch.so
- [valkey-io/valkey-search#479](https://github.com/valkey-io/valkey-search/pull/479): to retry, building valkey-search left no .build-release/libsearch.so
- [valkey-io/valkey-search#492](https://github.com/valkey-io/valkey-search/pull/492) ([tests](tests/valkey-search/492-d90a35802/)): fix mem leak in unit test (#492): declined: The change touches only testing/valkey_search_test.cc, the module's own gtest unit test: it replaces a raw heap allocation of a mock CLUSTER SLOTS reply with a std::unique_ptr so the mock reply is freed when the test doesn't delete it, fixing a leak in the test harness itself. No shipped m

## Totals

- Merges whose tests reached the change: 441 of 639 tested (69%)
- Changed-line coverage: median 100% of 297 merge(s) measured, 18 under 50%
- Recall: 130 parents flagged of 193 fix merges
- Claude: $1736.40 in the last 24 hours, $4555.71 in the last 7 days, $4555.71 in all
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

- [valkey-io/valkey-search#494](https://github.com/valkey-io/valkey-search/pull/494): attempt 1 of 3, building valkey-search left no .build-release/libsearch.so
- [valkey-io/valkey-search#479](https://github.com/valkey-io/valkey-search/pull/479): attempt 1 of 3, building valkey-search left no .build-release/libsearch.so
- [valkey-io/valkey#3800](https://github.com/valkey-io/valkey/pull/3800): attempt 0 of 3, Claude Code failed (rc=1): API Error: Claude Code is unable to respond to this request, which appears to violate our Usage Policy (https://www.anthropic.com/legal/aup). This request triggered restrict
- [valkey-io/valkey#3613](https://github.com/valkey-io/valkey/pull/3613): attempt 0 of 3, Claude Code failed (rc=1): API Error: Claude Code is unable to respond to this request, which appears to violate our Usage Policy (https://www.anthropic.com/legal/aup). This request triggered restrict
- [valkey-io/valkey#3498](https://github.com/valkey-io/valkey/pull/3498): attempt 0 of 3, Claude Code failed (rc=1): API Error: Claude Code is unable to respond to this request, which appears to violate our Usage Policy (https://www.anthropic.com/legal/aup). This request triggered restrict
- [valkey-io/valkey#2936](https://github.com/valkey-io/valkey/pull/2936): attempt 2 of 3, 6 of 6 run(s) could not start: the run could not start: environment acquisition failed
