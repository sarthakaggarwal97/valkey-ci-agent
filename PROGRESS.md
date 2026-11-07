# Fuzzer walk

Updated 2026-11-07T01:34+00:00.

| Repository | Done | Remaining | Backlog cursor | Last 24 hours | Last 7 days (per day) |
|---|---|---|---|---|---|
| valkey-io/valkey | 485 | 2019 | #3811 (2026-06-12) | 233 | 69.3 |
| valkey-io/valkey-bloom | 109 | 0 | ce516d0ef6e3 (2024-04-26) | 0 | 15.6 |
| valkey-io/valkey-json | 62 | 0 | c1423d7e4b38 (2024-11-25) | 0 | 8.9 |
| valkey-io/valkey-search | 351 | 318 | #707 (2026-02-05) | 204 | 50.1 |

## Last runs

### valkey-io/valkey

[2026-11-07T00:56:10+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42311854497): 20 merge(s) tested, 9 without a code change.

- [valkey-io/valkey#5258](https://github.com/valkey-io/valkey/pull/5258) ([tests](tests/valkey/5258-aa8d09d6e/)): Make unit tests independent of the order they run in (#5258): declined: Test-and-CI-only change: it fixes gtest unit suites (test_bgiteration, test_cmdflags, test_networking, test_object, test_throttle, test_throttle_repl) to reset process-wide state so they pass in any order, and updates the CI workflow to run them in one shuffled process. The only non-test c
- [valkey-io/valkey#5316](https://github.com/valkey-io/valkey/pull/5316) ([tests](tests/valkey/5316-79aff7c07/)): Close bgIteration MULTI when no replication work is pending (#5316): 0 of 3 test(s) reached the change, coverage 100%
- [valkey-io/valkey#5320](https://github.com/valkey-io/valkey/pull/5320) ([tests](tests/valkey/5320-5b7984e90/)): Revert "Make unit tests independent of the order they run in" (#5320): declined: The change is a revert touching only unit-test files (src/unit/test_*.cpp), the CI workflow, and the removal of a test-only helper function in src/object.c (unitTestOnly_objectResetMetadataSize) that is never called by server code. No runtime behaviour of valkey-server or its tools changes
- [valkey-io/valkey#3362](https://github.com/valkey-io/valkey/pull/3362): Include watched keys and pubsub channel/pattern memory in client memory overhead (#3362): error: Claude Code failed (rc=1): timeout after 600s
- [valkey-io/valkey#4024](https://github.com/valkey-io/valkey/pull/4024) ([tests](tests/valkey/4024-091d5ca62/)): Restore client's selected DB after module keyspace notification (#4024): 2 of 2 test(s) reached the change, coverage 100%
- [valkey-io/valkey#4047](https://github.com/valkey-io/valkey/pull/4047) ([tests](tests/valkey/4047-33cf4a413/)): Fix HRANDFIELD CASE 4 infinite loop when valid fields fewer than count (#4047): 0 of 6 test(s) reached the change, coverage 100%
- [valkey-io/valkey#4059](https://github.com/valkey-io/valkey/pull/4059) ([tests](tests/valkey/4059-689906a91/)): Cleanup redundant incrRefCount and fix new_argv alloc size around HDEL (#4059): declined: Pure cleanup with no behaviour change. incrRefCount(shared.hdel) was a no-op: shared.hdel is created with OBJ_SHARED_REFCOUNT (server.c:2219) and both incrRefCount and decrRefCount return early for shared objects (object.c:116,619,644), so adding or removing the call changes nothing on eit
- [valkey-io/valkey#4016](https://github.com/valkey-io/valkey/pull/4016) ([tests](tests/valkey/4016-aaa859d53/)): Fix build warnings with OpenSSL 4.0 (#4016): declined: The change is a build-warning fix in src/tls.c for OpenSSL 4.0 API deprecations. The new isCertValid logic is compiled only under OPENSSL_VERSION_NUMBER >= 0x40000000L, so on the CI build (OpenSSL < 4.0) that code is dead and behavior is identical to the parent. The getCertSubjectFieldByNa
- [valkey-io/valkey#4058](https://github.com/valkey-io/valkey/pull/4058) ([tests](tests/valkey/4058-9cd063ee1/)): Add logging for rejected MFSTART messages in cluster primary (#4058): 0 of 6 test(s) reached the change, coverage 33%
- [valkey-io/valkey#4049](https://github.com/valkey-io/valkey/pull/4049) ([tests](tests/valkey/4049-d38ff73aa/)): Fix HGETDEL FIELDS token validation (#4049): 2 of 2 test(s) reached the change, coverage 100%
- [valkey-io/valkey#3699](https://github.com/valkey-io/valkey/pull/3699) ([tests](tests/valkey/3699-c4cfa0fba/)): Skip NOT_KEY commands in client tracking (#3699): 1 of 1 test(s) reached the change, coverage 100%
- [valkey-io/valkey#3675](https://github.com/valkey-io/valkey/pull/3675) ([tests](tests/valkey/3675-047379319/)): Tighten NOT_KEY handling and add test coverage for NOT_KEY commands (#3675): 0 of 2 test(s) reached the change, coverage 100%
- [valkey-io/valkey#3843](https://github.com/valkey-io/valkey/pull/3843) ([tests](tests/valkey/3843-2253589b2/)): Increase max proctitle length from 255 to 1024 (#3843): declined: no proposed test validated
- [valkey-io/valkey#2648](https://github.com/valkey-io/valkey/pull/2648) ([tests](tests/valkey/2648-21f5000e7/)): Make commandlog-request-larger-than and commandlog-reply-larger-than become SIGNED_MEMORY_CONFIG (#2648): 2 of 2 test(s) reached the change
- [valkey-io/valkey#3357](https://github.com/valkey-io/valkey/pull/3357) ([tests](tests/valkey/3357-6ac85d082/)): Remove unused dict callback parameter from functionReset and related functions (#3357): declined: Pure refactor: the change removes a callback parameter that every caller except emptyData already passed as NULL. The one non-NULL caller forwarded replicationEmptyDbCallback through an incompatible function-pointer cast (the deleted TODO), and that callback ignores its argument; its only 
- [valkey-io/valkey#3640](https://github.com/valkey-io/valkey/pull/3640) ([tests](tests/valkey/3640-bee30d47d/)): Extend zmalloc_purge to also trim the libc main arena (#3640): 3 of 5 test(s) reached the change, coverage 100%; [check, predates #3640](https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/86)
- [valkey-io/valkey#3588](https://github.com/valkey-io/valkey/pull/3588) ([tests](tests/valkey/3588-436dcaeee/)): Improve maintainability for kvstoreScan (#3588): declined: Pure maintainability refactor of kvstoreScan's cursor packing with provably identical behavior. The old mask `num_hashtables - 1` equals the new `(1 << num_hashtables_bits) - 1` because num_hashtables is always exactly 2^num_hashtables_bits (kvstore.c:302-303); the removed `num_hashtables 
- [valkey-io/valkey#3803](https://github.com/valkey-io/valkey/pull/3803) ([tests](tests/valkey/3803-5ae43a403/)): hashtable: scan no-duplicate guarantee and add HasPassedKey API (#3803): declined: The diff adds only (1) documentation comments describing already-existing scan guarantees, (2) a new API function hashtableScanHasPassedKey that is declared in hashtable.h and defined in hashtable.c but never called from any server code path — grep shows its only callers are the new unit t
- [valkey-io/valkey#3968](https://github.com/valkey-io/valkey/pull/3968) ([tests](tests/valkey/3968-6774c0997/)): Add REUSE compliance for machine-readable licensing and SBOM generation (#3968): declined: The change is licensing/CI metadata only: a new REUSE compliance GitHub Actions workflow, canonical license texts under LICENSES/, REUSE.toml, COPYING reformatting, a deps/README.md edit, and SPDX-identifier fixes inside source-file comment headers. No executable code, command behavior, re
- [valkey-io/valkey#3597](https://github.com/valkey-io/valkey/pull/3597) ([tests](tests/valkey/3597-ed6e9a91d/)): Enable multi-command parsing for replicated clients (#3597): 0 of 6 test(s) reached the change, coverage 84%

### valkey-io/valkey-bloom

[2026-11-07T01:07:25+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42322236505): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-json

[2026-11-07T01:11:00+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42322572225): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-search

[2026-11-07T01:34:15+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42306322056): 20 merge(s) tested, 1 without a code change.

- [valkey-io/valkey-search#1546](https://github.com/valkey-io/valkey-search/pull/1546) ([tests](tests/valkey-search/1546-6e403afa1/)): feat(vector-range): add epsilon-aware HNSW range traversal (#1546): 4 of 4 test(s) reached the change
- [valkey-io/valkey-search#667](https://github.com/valkey-io/valkey-search/pull/667) ([tests](tests/valkey-search/667-fa3ae057c/)): Raise limit on number of attributes. Correct misnamed max-vector-attribute (#667): 5 of 5 test(s) reached the change
- [valkey-io/valkey-search#749](https://github.com/valkey-io/valkey-search/pull/749) ([tests](tests/valkey-search/749-b383d752f/)): Make fanout async threshold configurable (#749): 3 of 3 test(s) reached the change
- [valkey-io/valkey-search#746](https://github.com/valkey-io/valkey-search/pull/746) ([tests](tests/valkey-search/746-311be1eb3/)): Use consistent + performant casefolding logic in both query/ingestion (#746): 2 of 2 test(s) reached the change
- [valkey-io/valkey-search#711](https://github.com/valkey-io/valkey-search/pull/711) ([tests](tests/valkey-search/711-da4e28f2d/)): Hybrid vector text query integration test (#711): declined: The change is test-and-CI only: it adds hybrid vector/text query cases to integration/test_fulltext.py (refactoring existing assertions into a shared HYBRID_QUERY_EXPECTED_RESULTS table and adding a KNN variant of the same queries) and amends scripts/common.rc to pip-install an extra requi
- [valkey-io/valkey-search#743](https://github.com/valkey-io/valkey-search/pull/743) ([tests](tests/valkey-search/743-a6a71b394/)):  ftdebug to list configs in search module (#743): 0 of 2 test(s) reached the change; [crash, appeared after #743](https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/87)
- [valkey-io/valkey-search#674](https://github.com/valkey-io/valkey-search/pull/674) ([tests](tests/valkey-search/674-a2c814f7d/)): Add grpc coordinator threads CPU time metric to INFO command (#674): declined: The change only does anything when the module is loaded with the --use-coordinator module argument: the ThreadGroupCPUMonitor is created in ValkeySearch::Startup only under GetUseCoordinator(), and the new INFO field search_coordinator_threads_cpu_time_sec is VisibleIf(UsingCoordinator()),
- [valkey-io/valkey-search#722](https://github.com/valkey-io/valkey-search/pull/722) ([tests](tests/valkey-search/722-e015fa874/)): Implement SLOP and INORDER in FT.AGGREGATE (#722): 3 of 3 test(s) reached the change
- [valkey-io/valkey-search#741](https://github.com/valkey-io/valkey-search/pull/741): to retry, `deps=/opt/valkey-search-deps` failed in parent (exit 1): [138/223] Building CXX object src/commands/CMakeFiles/commands.dir/commands.cc.o FAILED: [code=1] src/commands/CMakeFiles/commands.dir/command
- [valkey-io/valkey-search#735](https://github.com/valkey-io/valkey-search/pull/735): to retry, `deps=/opt/valkey-search-deps` failed in merge (exit 1): [138/223] Building CXX object src/commands/CMakeFiles/commands.dir/commands.cc.o FAILED: [code=1] src/commands/CMakeFiles/commands.dir/commands
- [valkey-io/valkey-search#734](https://github.com/valkey-io/valkey-search/pull/734): to retry, `deps=/opt/valkey-search-deps` failed in merge (exit 1): [137/221] Building CXX object src/commands/CMakeFiles/commands.dir/commands.cc.o FAILED: [code=1] src/commands/CMakeFiles/commands.dir/commands
- [valkey-io/valkey-search#621](https://github.com/valkey-io/valkey-search/pull/621): to retry, `deps=/opt/valkey-search-deps` failed in merge (exit 1): [136/221] Building CXX object src/commands/CMakeFiles/commands.dir/commands.cc.o FAILED: [code=1] src/commands/CMakeFiles/commands.dir/commands
- [valkey-io/valkey-search#618](https://github.com/valkey-io/valkey-search/pull/618) ([tests](tests/valkey-search/618-9b29188d0/)): Feature/sortby implementation (#618): 5 of 5 test(s) reached the change
- [valkey-io/valkey-search#639](https://github.com/valkey-io/valkey-search/pull/639) ([tests](tests/valkey-search/639-444fb186b/)): Increase default for allowed number of indexes to 1000 (#639): 3 of 3 test(s) reached the change
- [valkey-io/valkey-search#671](https://github.com/valkey-io/valkey-search/pull/671) ([tests](tests/valkey-search/671-cff18a5a0/)): Endurance Testing - adding more search functionality testing. (#671): declined: The change modifies only valkey-search's own integration-test harness: testing/integration/stability_runner.py (new memtier test documents, a different FT.SEARCH query mix for TEXT indexes, consolidated delete/expire memtier commands, and nostem/with_suffix_trie tweaks in the test's index 
- [valkey-io/valkey-search#731](https://github.com/valkey-io/valkey-search/pull/731) ([tests](tests/valkey-search/731-0345b6268/)): Endurance test added as a GitHub workflow (#731): declined: The change is purely CI/tooling: it adds a new GitHub workflow (.github/workflows/endurance_tests.yml), a benchmark driver script (scripts/benchmark/run_endurance_test.sh), and installs jq in the devcontainer Dockerfile. No module source, command handling, indexing, replication, or persist
- [valkey-io/valkey-search#541](https://github.com/valkey-io/valkey-search/pull/541) ([tests](tests/valkey-search/541-6bdc2dde5/)): Block Search Response on Conflicting In-Flight Keys for fulltext queries (#541): 2 of 2 test(s) reached the change
- [valkey-io/valkey-search#712](https://github.com/valkey-io/valkey-search/pull/712) ([tests](tests/valkey-search/712-945d29388/)): Ensure content is always retrieved on correct DB during search (#712): declined: no proposed test validated
- [valkey-io/valkey-search#708](https://github.com/valkey-io/valkey-search/pull/708) ([tests](tests/valkey-search/708-703331414/)): Undo the renaming of an existing config (#708): 2 of 2 test(s) reached the change
- [valkey-io/valkey-search#685](https://github.com/valkey-io/valkey-search/pull/685) ([tests](tests/valkey-search/685-cd722c45b/)): Fix OOM Handling for SearchBased ReadOnly commands (#685): 2 of 2 test(s) reached the change

## Totals

- Merges whose tests reached the change: 374 of 529 tested (71%)
- Changed-line coverage: median 100% of 235 merge(s) measured, 15 under 50%
- Recall: 109 parents flagged of 154 fix merges
- Claude: $1679.27 in the last 24 hours, $3625.23 in the last 7 days, $3625.23 in all
- Issues filed: 41
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

## Retries

- [valkey-io/valkey-search#741](https://github.com/valkey-io/valkey-search/pull/741): attempt 1 of 3, `deps=/opt/valkey-search-deps` failed in parent (exit 1): [138/223] Building CXX object src/commands/CMakeFiles/commands.dir/commands.cc.o FAILED: [code=1] src/commands/CMakeFiles/commands.dir/command
- [valkey-io/valkey-search#735](https://github.com/valkey-io/valkey-search/pull/735): attempt 1 of 3, `deps=/opt/valkey-search-deps` failed in merge (exit 1): [138/223] Building CXX object src/commands/CMakeFiles/commands.dir/commands.cc.o FAILED: [code=1] src/commands/CMakeFiles/commands.dir/commands
- [valkey-io/valkey-search#734](https://github.com/valkey-io/valkey-search/pull/734): attempt 1 of 3, `deps=/opt/valkey-search-deps` failed in merge (exit 1): [137/221] Building CXX object src/commands/CMakeFiles/commands.dir/commands.cc.o FAILED: [code=1] src/commands/CMakeFiles/commands.dir/commands
- [valkey-io/valkey-search#621](https://github.com/valkey-io/valkey-search/pull/621): attempt 1 of 3, `deps=/opt/valkey-search-deps` failed in merge (exit 1): [136/221] Building CXX object src/commands/CMakeFiles/commands.dir/commands.cc.o FAILED: [code=1] src/commands/CMakeFiles/commands.dir/commands
