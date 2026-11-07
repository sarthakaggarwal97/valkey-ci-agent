# Fuzzer walk

Updated 2026-11-07T21:37+00:00.

| Repository | Done | Remaining | Backlog cursor | Last 24 hours | Last 7 days (per day) |
|---|---|---|---|---|---|
| valkey-io/valkey | 785 | 1722 | #3126 (2026-01-29) | 349 | 112.1 |
| valkey-io/valkey-bloom | 109 | 0 | ce516d0ef6e3 (2024-04-26) | 0 | 15.6 |
| valkey-io/valkey-json | 62 | 0 | c1423d7e4b38 (2024-11-25) | 0 | 8.9 |
| valkey-io/valkey-search | 481 | 188 | #362 (2025-09-09) | 172 | 68.7 |

## Last runs

### valkey-io/valkey

[2026-11-07T21:22:56+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42407462314): 20 merge(s) tested, 8 without a code change.

- [valkey-io/valkey#3135](https://github.com/valkey-io/valkey/pull/3135) ([tests](tests/valkey/3135-751a09964/)): Add remaining_repl_size field in CLUSTER MIGRATESLOTS output (#3135): 3 of 3 test(s) reached the change, coverage 100%; [check, introduced by #3135](https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/100); [check, introduced by #3135](https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/101)
- [valkey-io/valkey#2868](https://github.com/valkey-io/valkey/pull/2868) ([tests](tests/valkey/2868-87caeb764/)): Add more client info flags to module API (#2868): 3 of 3 test(s) reached the change, coverage 100%; check, not reproduced (recorded); check, introduced and since fixed (recorded); check, not reproduced (recorded); check, introduced and since fixed (recorded); [crash, appeared after #2868](https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/102); crash, predates #2868, since fixed (recorded)
- [valkey-io/valkey#3167](https://github.com/valkey-io/valkey/pull/3167) ([tests](tests/valkey/3167-ca34cc0b8/)): Make TLS disable/enable INFO checks deterministic for module mode (#3167): declined: The only behavioral change is in TLS module mode (BUILD_TLS=module): the server's non-OpenSSL tlsResetCertInfo stub no longer wipes the INFO TLS cert serial/expiry fields that the loaded valkey-tls module populated, when a TLS config is set while TLS stays enabled. The fuzzer cannot reach 
- [valkey-io/valkey#3173](https://github.com/valkey-io/valkey/pull/3173) ([tests](tests/valkey/3173-e84852dca/)): Avoid crash in genClusterInfoString when myself is uninitialized (#3173): declined: The fix adds a NULL guard in genClusterInfoString so that nodeEpoch is not called on a NULL `myself`. `myself` is only ever NULL in one window: during clusterInit() itself, if clusterLoadConfig() hits a corrupted cluster-config-file and calls serverPanic (cluster_legacy.c:1445/1012) before
- [valkey-io/valkey#3159](https://github.com/valkey-io/valkey/pull/3159) ([tests](tests/valkey/3159-55b846064/)): Minor cleanup in memtoull to also check ERANGE when calling strtoull (#3159): 2 of 2 test(s) reached the change, coverage 100%
- [valkey-io/valkey#3174](https://github.com/valkey-io/valkey/pull/3174) ([tests](tests/valkey/3174-980ef5782/)): make test_empty_buckets_rehashing deterministic across hash seeds (#3174): declined: The change only edits the unit test src/unit/test_hashtable.c to make test_empty_buckets_rehashing deterministic across randomized hash seeds (choosing which key to keep by its bucket index and adding fresh keys instead of reused ones). No Valkey server code, command, data structure, repli
- [valkey-io/valkey#3155](https://github.com/valkey-io/valkey/pull/3155) ([tests](tests/valkey/3155-666472c76/)): Fix arr bound err in test-sanitizer-undefined (gcc) (#3155): 0 of 3 test(s) reached the change
- [valkey-io/valkey#3150](https://github.com/valkey-io/valkey/pull/3150) ([tests](tests/valkey/3150-346c11dde/)): Rehashing more empty buckets to optimize the rehashing speed (#3150): 1 of 3 test(s) reached the change, coverage 80%
- [valkey-io/valkey#3154](https://github.com/valkey-io/valkey/pull/3154) ([tests](tests/valkey/3154-9f374e158/)): Skips the internal clients from logresreq checks (#3154): declined: The change only touches reqresShouldLog() in src/logreqres.c, which is entirely inside #ifdef LOG_REQ_RES — a compile-time feature the Makefile enables only for the reply-schema validation CI tooling, and which is driven solely by the hidden, immutable req-res-logfile config that itself ex
- [valkey-io/valkey#2839](https://github.com/valkey-io/valkey/pull/2839) ([tests](tests/valkey/2839-5d7d54249/)): Add cache for COMMAND (#2839): 0 of 4 test(s) reached the change, coverage 97%
- [valkey-io/valkey#2913](https://github.com/valkey-io/valkey/pull/2913) ([tests](tests/valkey/2913-e4804d50f/)): Add server side TLS certificate expiry tracking and INFO telemetry (#2913): 2 of 2 test(s) reached the change, coverage 78%
- [valkey-io/valkey#3144](https://github.com/valkey-io/valkey/pull/3144) ([tests](tests/valkey/3144-8ed270f44/)): Optimize SREM/ZREM/HDEL to pause auto shrink when deleting multiple items (#3144): 0 of 3 test(s) reached the change, coverage 100%
- [valkey-io/valkey#3141](https://github.com/valkey-io/valkey/pull/3141) ([tests](tests/valkey/3141-79c8948ed/)): Optimize rehash completion check for empty hash table (#3141): 0 of 3 test(s) reached the change, coverage 100%
- [valkey-io/valkey#3142](https://github.com/valkey-io/valkey/pull/3142) ([tests](tests/valkey/3142-cfbe0937f/)): Extend hashtableGetStatsMsg to always show empty table and rehash index (#3142): 3 of 3 test(s) reached the change, coverage 100%
- [valkey-io/valkey#3116](https://github.com/valkey-io/valkey/pull/3116) ([tests](tests/valkey/3116-eabfe235d/)): Check expiration time first before lookup the key in GETEX command (#3116): 2 of 2 test(s) reached the change, coverage 100%
- [valkey-io/valkey#3137](https://github.com/valkey-io/valkey/pull/3137) ([tests](tests/valkey/3137-9e0303bc5/)): Revert #3088 (#3137): 0 of 1 test(s) reached the change
- [valkey-io/valkey#2948](https://github.com/valkey-io/valkey/pull/2948) ([tests](tests/valkey/2948-851eaafe6/)): Free replica local buffer in async way in dual channel replication (#2948): 1 of 8 test(s) reached the change, coverage 100%
- [valkey-io/valkey#3128](https://github.com/valkey-io/valkey/pull/3128) ([tests](tests/valkey/3128-73838a0dd/)): Remove empty list dead code condition in lmoveGenericCommand code (#3128): declined: This is a pure dead-code removal refactor with no observable behavior change. The deleted branch in lmoveGenericCommand handled an existing key holding an empty (zero-length) list, but no client-reachable path can produce such a key: every list loader in rdb.c routes zero-element lists to 
- [valkey-io/valkey#3123](https://github.com/valkey-io/valkey/pull/3123) ([tests](tests/valkey/3123-b8650a5a3/)): Fix Lua Module Build For MacOS Compatibility (#3123): declined: The change is purely a CMake build-system fix for macOS: it adds -fPIC to the fpconv static library, selects libvalkeylua.dylib as the module name on Apple, and bundles sha1.c, rand.c and fpconv into the valkeylua shared library so it links on macOS. No C source, command, reply, replicatio
- [valkey-io/valkey#3001](https://github.com/valkey-io/valkey/pull/3001) ([tests](tests/valkey/3001-fd1438063/)): HSETEX - Always issue keyspace notifications after validation (#3001): 7 of 7 test(s) reached the change, coverage 100%

### valkey-io/valkey-bloom

[2026-11-07T21:37:37+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42416441043): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-json

[2026-11-07T21:36:06+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42416293159): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-search

[2026-11-07T19:24:49+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42394506825): 20 merge(s) tested, 3 without a code change.

- [valkey-io/valkey-search#494](https://github.com/valkey-io/valkey-search/pull/494) ([tests](tests/valkey-search/494-cdfe0c4b7/)): Fixed regression which broke Ninja builds OO (#494): declined: The change modifies only build.sh in valkey-search: it moves the Ninja/make build-tool selection after the function definitions and fixes ninja vs ninja-build detection so Ninja builds work again. It is purely a build-script/CI fix with no change to any module source, command behavior, rep
- [valkey-io/valkey-search#479](https://github.com/valkey-io/valkey-search/pull/479) ([tests](tests/valkey-search/479-e9f277cfd/)): Add support for checking ACL key prefix permissions using the new Valkey module API. (#479): 4 of 4 test(s) reached the change
- [valkey-io/valkey-search#405](https://github.com/valkey-io/valkey-search/pull/405) ([tests](tests/valkey-search/405-f683fffb3/)): Add more integration tests for FT.DROPINDEX consistency check (#405): 3 of 3 test(s) reached the change; crash, predates #405, since fixed (recorded)
- [valkey-io/valkey-search@b2aca8c691f3](https://github.com/valkey-io/valkey-search/commit/b2aca8c691f3a9d38098ab720268549de9d14ce0) ([tests](tests/valkey-search/b2aca8c69/)): Fix typo in ft.aggregate commit: declined: The change only fixes a typo in an error string inside the filter_str.empty() branch of the file-local ParseKNN (ft_search_parser.cc:185). That branch is unreachable: ParseKNN's sole caller, PreParseQueryString at ft_search_parser.cc:393, is guarded by !vector_filter.empty() on the very st
- [valkey-io/valkey-search#345](https://github.com/valkey-io/valkey-search/pull/345) ([tests](tests/valkey-search/345-b3b3449fb/)): Implementation of FT.AGGREGATE Command (#345): 4 of 4 test(s) reached the change
- [valkey-io/valkey-search#408](https://github.com/valkey-io/valkey-search/pull/408) ([tests](tests/valkey-search/408-0cd447cfc/)): Implement server version checking on load (#408): 0 of 3 test(s) reached the change
- [valkey-io/valkey-search#393](https://github.com/valkey-io/valkey-search/pull/393) ([tests](tests/valkey-search/393-73138472e/)): FT.DROPINDEX consistency check (#393): declined: The change is real behaviour (FT.DROPINDEX in cluster mode now fans out a consistency check over the coordinator's gRPC channel, retrying until every node reports the index gone; gRPC NOT_FOUND is reclassified as INDEX_NAME_ERROR; FT.INFO CLUSTER retries on index-name errors), but every ch
- [valkey-io/valkey-search#404](https://github.com/valkey-io/valkey-search/pull/404) ([tests](tests/valkey-search/404-5ee8c81d3/)): add .kiro folder to .gitignore; enable developers to use kiro while prevent uploading kiro steering files (#404): declined: The change adds a single line (`.kiro/`) to .gitignore so developers' Kiro steering files aren't committed. It touches no source code, build configuration, or runtime behavior of the valkey-search module; there is no command, reply, replication, or persistence path to exercise.
- [valkey-io/valkey-search#386](https://github.com/valkey-io/valkey-search/pull/386) ([tests](tests/valkey-search/386-2812344e7/)): FT.CREATE consistency check (#386): declined: The only behavioural change sits behind the coordinator, which this fuzzer cannot enable. FT.CREATE's new consistency-check fanout runs only when `ValkeySearch::Instance().IsCluster() && UsingCoordinator()` (src/commands/ft_create.cc:63-70); otherwise the command takes the unchanged else-b
- [valkey-io/valkey-search#384](https://github.com/valkey-io/valkey-search/pull/384) ([tests](tests/valkey-search/384-95c6fde85/)): Add --format option to build script (#384): declined: The change only modifies build.sh, adding a --format option that runs clang-format on the source tree as a developer convenience. It is a build/tooling change with no effect on the compiled module's runtime behavior, commands, replies, persistence, or replication, so there is nothing for a
- [valkey-io/valkey-search#337](https://github.com/valkey-io/valkey-search/pull/337) ([tests](tests/valkey-search/337-4be4f5680/)): Adding retry to fanout base class (#337): 3 of 3 test(s) reached the change
- [valkey-io/valkey-search#390](https://github.com/valkey-io/valkey-search/pull/390) ([tests](tests/valkey-search/390-0e185efac/)): Captured errors should be formatted as such. (#390): declined: The change is confined to the unit-test infrastructure: vmsdk/src/testing_infra/module.h's ReplyCapture mock (used only by gtest) now formats captured errors as RESP errors ('-msg') instead of bulk strings, and testing/ft_create_test.cc, ft_info_test.cc and ft_search_test.cc update their e
- [valkey-io/valkey-search#395](https://github.com/valkey-io/valkey-search/pull/395) ([tests](tests/valkey-search/395-d3c87b5ce/)): tag casesentive updated to be key-value pair (#395): 2 of 2 test(s) reached the change
- [valkey-io/valkey-search#370](https://github.com/valkey-io/valkey-search/pull/370) ([tests](tests/valkey-search/370-af2e9a8e0/)): added and removed stats and attributes from ft.info (#370): 2 of 2 test(s) reached the change
- [valkey-io/valkey-search#388](https://github.com/valkey-io/valkey-search/pull/388) ([tests](tests/valkey-search/388-7cbef1e01/)): fix memory leak at config get (#388): declined: The change rewrites OnGetStringConfig, the CONFIG GET callback for string-typed module configs in vmsdk (caching the ValkeyModuleString instead of leaking a fresh one per call). That callback is only installed by config::String::Register, and valkey-search at this commit never instantiates
- [valkey-io/valkey-search#387](https://github.com/valkey-io/valkey-search/pull/387) ([tests](tests/valkey-search/387-7182af06d/)): Remove command alloed acl checks is it is already done by Valkey (#387): 2 of 2 test(s) reached the change
- [valkey-io/valkey-search#363](https://github.com/valkey-io/valkey-search/pull/363) ([tests](tests/valkey-search/363-329da6072/)): Fix valkey and json module versions in build and integration test. (#363): declined: The change edits only scripts/common.rc, a build/CI helper script that clones and builds valkey-server and the valkey-json module for valkey-search's own integration tests: it makes VALKEY_VERSION and VALKEY_JSON_VERSION overridable env vars and clones valkey-json by branch instead of a pi
- [valkey-io/valkey-search#382](https://github.com/valkey-io/valkey-search/pull/382) ([tests](tests/valkey-search/382-e8b166bfd/)): Early return mutation handling on record not found and not tracked (#382): 2 of 2 test(s) reached the change; crash, predates #382, since fixed (recorded)
- [valkey-io/valkey-search#381](https://github.com/valkey-io/valkey-search/pull/381) ([tests](tests/valkey-search/381-688cb371d/)): Fixed: clients are blocked after MULTI operation (#381): 2 of 2 test(s) reached the change
- [valkey-io/valkey-search#379](https://github.com/valkey-io/valkey-search/pull/379) ([tests](tests/valkey-search/379-90e550e13/)): - Remove unused variable from test (#379): declined: The change only removes unused variables and magic numbers from a unit test file (thread_pool_test.cc) and adds `inline` to two explicit template specializations in debug.h to fix ODR/linkage for a header-defined function. The function bodies are unchanged, so no command, reply, indexing, 

## Totals

- Merges whose tests reached the change: 487 of 716 tested (68%)
- Changed-line coverage: median 100% of 350 merge(s) measured, 22 under 50%
- Recall: 140 parents flagged of 210 fix merges
- Claude: $1838.70 in the last 24 hours, $5131.64 in the last 7 days, $5131.64 in all
- Issues filed: 53
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/100
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/101
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/102
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
- [valkey-io/valkey#3613](https://github.com/valkey-io/valkey/pull/3613): attempt 0 of 3, Claude Code failed (rc=1): API Error: Claude Code is unable to respond to this request, which appears to violate our Usage Policy (https://www.anthropic.com/legal/aup). This request triggered restrict
- [valkey-io/valkey#3498](https://github.com/valkey-io/valkey/pull/3498): attempt 0 of 3, Claude Code failed (rc=1): API Error: Claude Code is unable to respond to this request, which appears to violate our Usage Policy (https://www.anthropic.com/legal/aup). This request triggered restrict
