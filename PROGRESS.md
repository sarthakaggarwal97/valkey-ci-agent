# Fuzzer walk

Updated 2026-11-06T21:56+00:00.

| Repository | Done | Remaining | Backlog cursor | Last 24 hours | Last 7 days (per day) |
|---|---|---|---|---|---|
| valkey-io/valkey | 436 | 2062 | #3581 (2026-07-06) | 232 | 62.3 |
| valkey-io/valkey-bloom | 109 | 0 | ce516d0ef6e3 (2024-04-26) | 0 | 15.6 |
| valkey-io/valkey-json | 62 | 0 | c1423d7e4b38 (2024-11-25) | 0 | 8.9 |
| valkey-io/valkey-search | 309 | 359 | #797 (2026-02-20) | 184 | 44.1 |

## Last runs

### valkey-io/valkey

[2026-11-06T21:25:50+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42267007259): 20 merge(s) tested, 11 without a code change.

- [valkey-io/valkey#4275](https://github.com/valkey-io/valkey/pull/4275) ([tests](tests/valkey/4275-a9d54c590/)): Fix ZRANGE XX option schema and test failures under force-resp3 (#4275): declined: The change only edits command JSON reply_schema metadata (compiled in solely under the LOG_REQ_RES CI build, absent from standard binaries) and fixes Tcl test assertions to accept the RESP3 null under --force-resp3. No C code changed; ZRANGE/ZRANGEBYSCORE/ZRANGEBYLEX and their REV variants
- [valkey-io/valkey#2978](https://github.com/valkey-io/valkey/pull/2978) ([tests](tests/valkey/2978-4733aed65/)): Add XX option to ZRANGE commands for null/empty distinction (#2978): 2 of 2 test(s) reached the change, coverage 100%
- [valkey-io/valkey#3769](https://github.com/valkey-io/valkey/pull/3769) ([tests](tests/valkey/3769-dd30872fd/)): Preserve module load order in CONFIG REWRITE (#3769): 0 of 8 test(s) reached the change, coverage 91%; [crash, predates #3769](https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/72)
- [valkey-io/valkey#4210](https://github.com/valkey-io/valkey/pull/4210) ([tests](tests/valkey/4210-f4dc3ca09/)): Check and reject invalid slot import job names during RDB load (#4210): 1 of 2 test(s) reached the change, coverage 50%
- [valkey-io/valkey#4229](https://github.com/valkey-io/valkey/pull/4229) ([tests](tests/valkey/4229-4691888e7/)): Validate slot import ranges when loading from RDB (#4229): declined: This is a defensive guard against corrupt/crafted RDB input: clusterRDBLoadSlotImport now rejects slot-import ranges with start_slot or end_slot >= CLUSTER_SLOTS or start > end, and the kvstore asserts now bound-check didx>=0. It is a real crash fix (the parent aborts on assert, or reads o
- [valkey-io/valkey#4234](https://github.com/valkey-io/valkey/pull/4234) ([tests](tests/valkey/4234-b400e053b/)): Fix use-after-free in tlsProcessPendingData() on CLIENT KILL (#4234): declined: The fix is a use-after-free confined to tlsProcessPendingData() in src/tls.c, reachable only with multiple concurrent TLS client connections that have SSL-buffered pending data, one of which is freed synchronously (CLIENT KILL -> freeClient -> connTLSClose -> listDelNode) while another is 
- [valkey-io/valkey#3362](https://github.com/valkey-io/valkey/pull/3362): to retry, Claude Code failed (rc=1): timeout after 600s
- [valkey-io/valkey#4104](https://github.com/valkey-io/valkey/pull/4104) ([tests](tests/valkey/4104-50a0397f4/)): Avoid offloading writes to IO threads for the slot migration export job while snapshotting. (#4104): 0 of 4 test(s) reached the change, coverage 100%; [check, predates #4104](https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/83)
- [valkey-io/valkey#4186](https://github.com/valkey-io/valkey/pull/4186) ([tests](tests/valkey/4186-79fc84157/)): Always log "possibly failing" when a node reaches PFAIL (#4186): declined: The change only moves a serverLog(LL_NOTICE, "possibly failing") call in clusterCron so it is emitted for voting primaries too, instead of only for non-voting nodes. The PFAIL flag setting, update_state, and markNodeAsFailingIfNeeded logic are unchanged, and the log statement reuses the ex
- [valkey-io/valkey#3039](https://github.com/valkey-io/valkey/pull/3039) ([tests](tests/valkey/3039-3a27559ab/)): Prevent module loading during async replication and slot migration (#3039): 2 of 11 test(s) reached the change, coverage 61%
- [valkey-io/valkey#4142](https://github.com/valkey-io/valkey/pull/4142) ([tests](tests/valkey/4142-7bb7b7f01/)): Out of Bounds read in valkey-benchmark (#4142): 1 of 1 test(s) reached the change, coverage 100%; [crash, appeared after #4142](https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/81); crash, fixed by #4142 (recorded)
- [valkey-io/valkey#4060](https://github.com/valkey-io/valkey/pull/4060) ([tests](tests/valkey/4060-cdf98a251/)): Fix io_last_written bookmark desync that corrupts replies with IO threads (#4060): 0 of 6 test(s) reached the change, coverage 72%
- [valkey-io/valkey#4063](https://github.com/valkey-io/valkey/pull/4063) ([tests](tests/valkey/4063-c03970afd/)): stream: strip redundant LIMIT when rewriting XADD/XTRIM for propagation (#4063): 0 of 3 test(s) reached the change, coverage 100%
- [valkey-io/valkey#4126](https://github.com/valkey-io/valkey/pull/4126) ([tests](tests/valkey/4126-d3824527b/)): feat: Add VALKEYCLI_USER env var (#4126): 1 of 1 test(s) reached the change, coverage 100%
- [valkey-io/valkey#3050](https://github.com/valkey-io/valkey/pull/3050) ([tests](tests/valkey/3050-0e52ec879/)): Add support for CONFIG INFO <config> to display config type and valid values (#3050): 2 of 2 test(s) reached the change, coverage 99%
- [valkey-io/valkey#4123](https://github.com/valkey-io/valkey/pull/4123) ([tests](tests/valkey/4123-6c315de39/)): CI: add make test-unit to macOS build and auto-detect Homebrew LLVM path (#4123): declined: The change is purely build/CI infrastructure: it edits the GitHub Actions macOS workflow (installs llvm/googletest earlier, sets CC/CXX/AR/RANLIB as job env, runs make test-unit) and adds Homebrew LLVM path auto-detection to src/unit/Makefile, which only affects compiling the unit-test har
- [valkey-io/valkey#4092](https://github.com/valkey-io/valkey/pull/4092) ([tests](tests/valkey/4092-301b6cde1/)): Fix CLUSTER BUMPEPOCH getting stuck when a dead node holds a high configEpoch (#4092): 0 of 3 test(s) reached the change, coverage 0%
- [valkey-io/valkey#4116](https://github.com/valkey-io/valkey/pull/4116) ([tests](tests/valkey/4116-f0312a243/)): Add static_assert to guard node flags fit in uint16_t (#4116): declined: The change only adds a compile-time static_assert (and the CLUSTER_NODE_MAX macro it uses) to guard that cluster node flags fit in uint16_t. It alters no runtime behavior: no command, reply, replication, cluster-bus message, or persistence path changes. Any violation would be a build failu
- [valkey-io/valkey#4105](https://github.com/valkey-io/valkey/pull/4105) ([tests](tests/valkey/4105-c2b8c17a9/)): Send replica version on dual-channel RDB connection (#4105): 1 of 3 test(s) reached the change
- [valkey-io/valkey#4100](https://github.com/valkey-io/valkey/pull/4100) ([tests](tests/valkey/4100-327fbd345/)): Avoid UB in slot-entry parsing of cluster config (#4100): declined: This is a UB/robustness fix confined to clusterLoadConfig, the nodes.conf text parser, which runs only once at startup (src/cluster_legacy.c:1499). It is unreachable by any installed experiment or writable one, and would false-positive if forced. The changed branches are entered only by a 

### valkey-io/valkey-bloom

[2026-11-06T21:56:33+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42298765886): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-json

[2026-11-06T21:12:26+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42294679094): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-search

[2026-11-06T19:57:04+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42257567208): 20 merge(s) tested, 8 without a code change.

- [valkey-io/valkey-search#851](https://github.com/valkey-io/valkey-search/pull/851) ([tests](tests/valkey-search/851-0e5a1707a/)): Using absl::btree_map for PositionMap (#851): declined: The change only swaps the transient per-token PositionMap (and the matching FlatPositionMap::Create parameter) from std::map<Position, FieldMask> to absl::btree_map<Position, FieldMask>. Both are ordered maps over uint32_t with identical comparator, iteration order, size() and try_emplace 
- [valkey-io/valkey-search#726](https://github.com/valkey-io/valkey-search/pull/726) ([tests](tests/valkey-search/726-6ea37b0dc/)): Multi DB tests in CME/CMD for FT.SEARCH (#726): 0 of 3 test(s) reached the change
- [valkey-io/valkey-search#846](https://github.com/valkey-io/valkey-search/pull/846) ([tests](tests/valkey-search/846-5a3fc51d6/)): Support GetSerializationRange in Aggregation (#846): 2 of 2 test(s) reached the change
- [valkey-io/valkey-search#845](https://github.com/valkey-io/valkey-search/pull/845) ([tests](tests/valkey-search/845-a2354e47e/)): Improve text.cc locking (#845): 0 of 3 test(s) reached the change
- [valkey-io/valkey-search#839](https://github.com/valkey-io/valkey-search/pull/839) ([tests](tests/valkey-search/839-b8dff62fd/)): Quick fix/ perf improvement for Field Mask Create (#839): declined: The change replaces the heap-allocated virtual FieldMask class hierarchy with a plain 16-byte value struct. Every production call site was checked: SetField keeps the same bounds CHECK and bit semantics; CountSetFields returns the same counts for all field counts 1-64 (the old single-field
- [valkey-io/valkey-search#818](https://github.com/valkey-io/valkey-search/pull/818) ([tests](tests/valkey-search/818-86cb04971/)): Fix TSAN build for integration tests (#818): declined: The change modifies only build/CI scripts (build.sh and scripts/common.rc): it sets TSAN_OPTIONS instead of LSAN_OPTIONS for thread-sanitizer CI builds, adds a TSAN-specific third-party build directory, and sed-patches Valkey core sources during sanitizer CI builds. No module source code c
- [valkey-io/valkey-search#831](https://github.com/valkey-io/valkey-search/pull/831) ([tests](tests/valkey-search/831-81b7c80ac/)): Fix crash due to lazy expiration in the middle of Search commands (#831): 0 of 4 test(s) reached the change
- [valkey-io/valkey-search@996e8e6b8e02](https://github.com/valkey-io/valkey-search/commit/996e8e6b8e02451106cee6a26eaf5e1ce3b0e546) ([tests](tests/valkey-search/996e8e6b8/)): Merge pull request #830 from BCathcart/brennan-text-subtree-key-count-follow-up: 0 of 3 test(s) reached the change; crash, predates 996e8e6b8e02, since fixed (recorded)
- [valkey-io/valkey-search#837](https://github.com/valkey-io/valkey-search/pull/837) ([tests](tests/valkey-search/837-bc32b356b/)): Serialize position map before lock context (#837): 0 of 3 test(s) reached the change
- [valkey-io/valkey-search#828](https://github.com/valkey-io/valkey-search/pull/828) ([tests](tests/valkey-search/828-35643b20b/)): Bump to rc2 (#828): declined: The change only edits the MODULE_RELEASE_STAGE macro in src/version.h from "rc1" to "rc2". A repo-wide search shows this macro is defined but never referenced anywhere else in valkey-search: module registration uses kModuleVersion (unchanged at 1.2.0), and RDB serialization and coordinator
- [valkey-io/valkey-search#824](https://github.com/valkey-io/valkey-search/pull/824) ([tests](tests/valkey-search/824-c903fc43e/)): Text Size Estimation (#824): 1 of 1 test(s) reached the change
- [valkey-io/valkey-search#813](https://github.com/valkey-io/valkey-search/pull/813) ([tests](tests/valkey-search/813-246f564dd/)): Using "Min Heap" approach for TermIterator (#813): 0 of 6 test(s) reached the change
- [valkey-io/valkey-search#793](https://github.com/valkey-io/valkey-search/pull/793) ([tests](tests/valkey-search/793-ba2265dee/)): Fix mutation sequence number timing bug (#793): 0 of 3 test(s) reached the change; crash, fixed by #793 (recorded)
- [valkey-io/valkey-search#803](https://github.com/valkey-io/valkey-search/pull/803) ([tests](tests/valkey-search/803-037b4b725/)): Remove Unused Variables And Add `UnusedVar` Helper Template (#803): declined: The change only removes dead local variables (lexer.cc, proximity.cc, test files) and marks still-performed loads as [[maybe_unused]] (index_schema.cc keeps the LoadObject<bool>() call, so the serialization stream is consumed identically). No command reply, indexing, persistence, or replic
- [valkey-io/valkey-search#806](https://github.com/valkey-io/valkey-search/pull/806) ([tests](tests/valkey-search/806-406e1e3eb/)): Add multi/lua integration tests with search commands (#806): declined: no proposed test validated
- [valkey-io/valkey-search#799](https://github.com/valkey-io/valkey-search/pull/799) ([tests](tests/valkey-search/799-da45def0d/)): Address unsolved text path in PreFilter Evaluators (#799): 2 of 2 test(s) reached the change; crash, fixed by #799 (recorded); crash, fixed by #799 (recorded)
- [valkey-io/valkey-search#810](https://github.com/valkey-io/valkey-search/pull/810) ([tests](tests/valkey-search/810-358b848b1/)): Sanitize module logging: DEBUG logs (#810): declined: The change only sanitizes DEBUG-level log message text: it wraps user-controlled strings in vmsdk::config::RedactIfNeeded(), an inline helper that returns either the unchanged string_view or the constant '*redacted*' based on the pre-existing search.hide-user-data-from-log config, and move
- [valkey-io/valkey-search#785](https://github.com/valkey-io/valkey-search/pull/785) ([tests](tests/valkey-search/785-0ec279263/)): Fix unexpected vk-search flag in RDB after dropped all indexs (#785): declined: The fix only changes DoesGlobalMetadataContainEntry in src/coordinator/metadata_manager.cc, reached solely through MetadataManager::GetSectionsCount and SaveMetadata, the RDB aux-section callbacks of the coordinator's global-metadata manager. That manager is constructed only when the modul
- [valkey-io/valkey-search#804](https://github.com/valkey-io/valkey-search/pull/804) ([tests](tests/valkey-search/804-7e36ed730/)): Increase max value for ef-runtime and ef-construction (#804): 3 of 3 test(s) reached the change
- [valkey-io/valkey-search#760](https://github.com/valkey-io/valkey-search/pull/760) ([tests](tests/valkey-search/760-5fe1676e8/)): fix: add null check for thread_monitor_ in GetAvgCPUPercentage (#760): 0 of 2 test(s) reached the change; crash, fixed by #760 (recorded)

## Totals

- Merges whose tests reached the change: 345 of 482 tested (72%)
- Changed-line coverage: median 100% of 218 merge(s) measured, 11 under 50%
- Recall: 102 parents flagged of 144 fix merges
- Claude: $1601.74 in the last 24 hours, $3292.94 in the last 7 days, $3292.94 in all
- Issues filed: 37
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

## Retries

- [valkey-io/valkey#3362](https://github.com/valkey-io/valkey/pull/3362): attempt 1 of 3, Claude Code failed (rc=1): timeout after 600s
