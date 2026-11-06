# Fuzzer walk

Updated 2026-11-06T20:41+00:00.

| Repository | Done | Remaining | Backlog cursor | Last 24 hours | Last 7 days (per day) |
|---|---|---|---|---|---|
| valkey-io/valkey | 406 | 2092 | #4275 (2026-07-27) | 227 | 58.0 |
| valkey-io/valkey-bloom | 109 | 0 | ce516d0ef6e3 (2024-04-26) | 0 | 15.6 |
| valkey-io/valkey-json | 62 | 0 | c1423d7e4b38 (2024-11-25) | 0 | 8.9 |
| valkey-io/valkey-search | 309 | 359 | #797 (2026-02-20) | 209 | 44.1 |

## Last runs

### valkey-io/valkey

[2026-11-06T18:01:55+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42252345373): 20 merge(s) tested, 5 without a code change.

- [valkey-io/valkey#3611](https://github.com/valkey-io/valkey/pull/3611) ([tests](tests/valkey/3611-eb5a7857e/)): Fix RDMA + IO threads re-entrancy and busy-loop via connection postpone masks (#3611): 0 of 4 test(s) reached the change, coverage 96%
- [valkey-io/valkey#4320](https://github.com/valkey-io/valkey/pull/4320) ([tests](tests/valkey/4320-a251aeb5d/)): Fix io-thread race in slot migration establish response handler (#4320): 0 of 2 test(s) reached the change, coverage 100%
- [valkey-io/valkey#4322](https://github.com/valkey-io/valkey/pull/4322) ([tests](tests/valkey/4322-327db78a7/)): reset scan cursor when table is deleted (#4322): 2 of 2 test(s) reached the change, coverage 100%
- [valkey-io/valkey#4279](https://github.com/valkey-io/valkey/pull/4279) ([tests](tests/valkey/4279-0fc8cdafc/)): Update module.c with missing FLAG_REPLICA description (#4279): declined: The diff only edits a doc comment in src/module.c: it rewords the description of VALKEYMODULE_CLIENTINFO_FLAG_PRIMARY and adds the previously missing description of VALKEYMODULE_CLIENTINFO_FLAG_REPLICA. No executable code, flag value, reply, or behavior changes, so there is no path a scena
- [valkey-io/valkey#4328](https://github.com/valkey-io/valkey/pull/4328) ([tests](tests/valkey/4328-c07e9b6cf/)): Fix and complete slot migration pipe comments (#4328): declined: The diff only edits comments in src/server.h: it replaces the placeholder 'xxxx' comment on slot_migration_pipe_conn with a real description and clarifies the comment on slot_migration_pipe_bufflen. No executable code, data structure, config, or protocol behaviour changes, so no scenario c
- [valkey-io/valkey#4148](https://github.com/valkey-io/valkey/pull/4148) ([tests](tests/valkey/4148-573b77f33/)): Add CMD_STALE flag to SYNC/PSYNC commands (#4148): 2 of 4 test(s) reached the change
- [valkey-io/valkey#4140](https://github.com/valkey-io/valkey/pull/4140) ([tests](tests/valkey/4140-cb2d83513/)): Retry PSYNC on -BUSY error instead of downgrading to SYNC (#4140): 1 of 4 test(s) reached the change, coverage 100%
- [valkey-io/valkey#4253](https://github.com/valkey-io/valkey/pull/4253) ([tests](tests/valkey/4253-af321c4df/)): Fix RESP3 push frame torn apart on self-publish with copy avoidance (#4253): 1 of 3 test(s) reached the change, coverage 100%
- [valkey-io/valkey#4300](https://github.com/valkey-io/valkey/pull/4300) ([tests](tests/valkey/4300-527a38222/)): Fix FIELDS token validation for HPERSIST and HTTL family commands (#4300): 3 of 3 test(s) reached the change, coverage 100%
- [valkey-io/valkey#4211](https://github.com/valkey-io/valkey/pull/4211) ([tests](tests/valkey/4211-b349fe282/)): Prevent double-free of the module timer when the callback stops it (#4211): 2 of 4 test(s) reached the change, coverage 100%
- [valkey-io/valkey#4302](https://github.com/valkey-io/valkey/pull/4302) ([tests](tests/valkey/4302-f75e2d55f/)): Skip bad-arity commands when prefetching queued command keys (#4302): 3 of 5 test(s) reached the change, coverage 100%; crash, fixed by #4302 (recorded); crash, fixed by #4302 (recorded)
- [valkey-io/valkey#4301](https://github.com/valkey-io/valkey/pull/4301) ([tests](tests/valkey/4301-c739b9296/)): Fix out-of-bounds read in getNodeByQuery's cross-DB COPY check (#4301): 2 of 4 test(s) reached the change, coverage 100%; crash, fixed by #4301 (recorded)
- [valkey-io/valkey#3510](https://github.com/valkey-io/valkey/pull/3510) ([tests](tests/valkey/3510-12e5e384d/)): Fix TLS infinite busy loop when write/read handlers are removed (#3510): declined: The change is confined to src/tls.c (updateSSLEvent and tlsHandleEvent), code compiled only with BUILD_TLS and executed only for TLS-type connections. The fuzzer has no TLS capability: no installed component speaks TLS or provisions certificates, every client connection (workloads, compare
- [valkey-io/valkey#3730](https://github.com/valkey-io/valkey/pull/3730) ([tests](tests/valkey/3730-6736cc580/)): conf: document chown(2) requirement for unixsocketgroup (#3730): declined: The change only adds a documentation comment to valkey.conf (explaining that unixsocketgroup performs chown(2) and must be permitted under syscall-filtering sandboxes). No server code, config parsing, or runtime behaviour is modified, so no scenario can reach or validate it.
- [valkey-io/valkey#4155](https://github.com/valkey-io/valkey/pull/4155) ([tests](tests/valkey/4155-8eaefda4f/)): Fix MOVE/COPY command skip the current-DB ACL check (#4155): 2 of 2 test(s) reached the change, coverage 100%
- [valkey-io/valkey#3905](https://github.com/valkey-io/valkey/pull/3905) ([tests](tests/valkey/3905-fb94efbd9/)): Optimize .clang-format and add enum/argument formatting options (#3905): declined: The change only edits the .clang-format configuration and reformats existing code to match it: enum declarations in cluster_legacy.c, defrag.c and server.h and a struct initializer list in db.c are reflowed onto multiple lines. Every edit is whitespace and line-break only; no identifier, v
- [valkey-io/valkey#4285](https://github.com/valkey-io/valkey/pull/4285) ([tests](tests/valkey/4285-6df96c44c/)): Optimize notifyKeyspaceEvent to avoid unnecessary allocations (#4285): 0 of 2 test(s) reached the change, coverage 100%
- [valkey-io/valkey#4277](https://github.com/valkey-io/valkey/pull/4277) ([tests](tests/valkey/4277-74249be76/)): Add comment and test for bio RDB disk transfer receive timeout (#4277): declined: The change adds only comments in replication.c (both hunks insert comment lines above pre-existing, unchanged code: the connRecvTimeout call in replicaReceiveRDBFromPrimaryToDisk and the abort-flag/bioDrainWorker logic in cancelReplicationHandshake) plus a TCL integration test documenting 
- [valkey-io/valkey#4252](https://github.com/valkey-io/valkey/pull/4252) ([tests](tests/valkey/4252-dc0411ae1/)): valkey-benchmark: fix -r option to support keyspace values above INT_MAX (#4252): 1 of 1 test(s) reached the change, coverage 91%
- [valkey-io/valkey#4281](https://github.com/valkey-io/valkey/pull/4281) ([tests](tests/valkey/4281-aed05498a/)): Fix macOS unit test by wrapping test object symbols (#4281): declined: The change touches only src/Makefile and src/unit/Makefile: it fixes the macOS unit-test build by wrapping test object symbols, deleting stale static archives before re-creating them, and removing flat-namespace linker flags from the test binary link. No server, command, replication, persi

### valkey-io/valkey-bloom

[2026-11-06T20:40:30+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42290231699): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-json

[2026-11-06T20:41:29+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42290371127): 0 merge(s) tested, 0 without a code change.


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

- Merges whose tests reached the change: 338 of 470 tested (72%)
- Changed-line coverage: median 100% of 207 merge(s) measured, 10 under 50%
- Recall: 102 parents flagged of 142 fix merges
- Claude: $1715.24 in the last 24 hours, $3174.13 in the last 7 days, $3174.13 in all
- Issues filed: 36
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

## Retries

None.
