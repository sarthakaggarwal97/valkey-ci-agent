# Fuzzer walk

Updated 2026-11-07T01:07+00:00.

| Repository | Done | Remaining | Backlog cursor | Last 24 hours | Last 7 days (per day) |
|---|---|---|---|---|---|
| valkey-io/valkey | 485 | 2019 | #3811 (2026-06-12) | 233 | 69.3 |
| valkey-io/valkey-bloom | 109 | 0 | ce516d0ef6e3 (2024-04-26) | 0 | 15.6 |
| valkey-io/valkey-json | 62 | 0 | c1423d7e4b38 (2024-11-25) | 0 | 8.9 |
| valkey-io/valkey-search | 334 | 334 | #667 (2026-02-13) | 187 | 47.7 |

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

[2026-11-07T00:40:14+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42319562997): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-search

[2026-11-06T22:39:00+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42284111779): 20 merge(s) tested, 5 without a code change.

- [valkey-io/valkey-search#794](https://github.com/valkey-io/valkey-search/pull/794) ([tests](tests/valkey-search/794-c7c21976a/)): FlatPosiitonMap::ReadCounts Optimization (#794): declined: The change replaces memcpy-based little-endian count reads in FlatPositionMap::ReadCounts with an equivalent byte-wise ReadVarUint loop and deduplicates that helper out of PositionIterator into an anonymous namespace. On the little-endian platform the fuzzer runs on, the new decode compute
- [valkey-io/valkey-search#796](https://github.com/valkey-io/valkey-search/pull/796) ([tests](tests/valkey-search/796-63eeaf926/)): Remove an allocation which is not needed in text search (#796): 0 of 2 test(s) reached the change
- [valkey-io/valkey-search#790](https://github.com/valkey-io/valkey-search/pull/790) ([tests](tests/valkey-search/790-ffdec7e08/)): Bump version to module 1.2 and add check for GetMinVersion (#790): 2 of 2 test(s) reached the change
- [valkey-io/valkey-search#786](https://github.com/valkey-io/valkey-search/pull/786) ([tests](tests/valkey-search/786-a876eed5f/)): Fix a memory leak bug in search.cc (#786): 0 of 2 test(s) reached the change
- [valkey-io/valkey-search#782](https://github.com/valkey-io/valkey-search/pull/782) ([tests](tests/valkey-search/782-bf70d7327/)): Enhance overview description, correct aggregation (#782): declined: The change modifies only documentation: markdown files under docs/ and the src/commands/*.json command-specification files. The JSON specs are not compiled, embedded, or loaded by the module (src/commands/CMakeLists.txt lists only .cc/.h sources; no source or build rule references the .jso
- [valkey-io/valkey-search#728](https://github.com/valkey-io/valkey-search/pull/728) ([tests](tests/valkey-search/728-be0f584a5/)): Sanitize module logging  (#728): 6 of 6 test(s) reached the change; crash, predates #728, since fixed (recorded)
- [valkey-io/valkey-search#747](https://github.com/valkey-io/valkey-search/pull/747) ([tests](tests/valkey-search/747-aa4832d46/)): Add THIRD_PARTY_LICENSES file (#747): declined: The change adds a THIRD_PARTY_NOTICES license file and nothing else: no source, build, or configuration file is modified, so no runtime behaviour of the valkey-search module can change. There is no path for a scenario to reach.
- [valkey-io/valkey-search#764](https://github.com/valkey-io/valkey-search/pull/764) ([tests](tests/valkey-search/764-8b04556b7/)): Fix for handling trailing punctuations in query (#764): 2 of 2 test(s) reached the change
- [valkey-io/valkey-search#742](https://github.com/valkey-io/valkey-search/pull/742) ([tests](tests/valkey-search/742-154b80cfc/)): Restructure Content Fetching to match key blocking semantics (#742): 0 of 2 test(s) reached the change
- [valkey-io/valkey-search#776](https://github.com/valkey-io/valkey-search/pull/776) ([tests](tests/valkey-search/776-6c47e4276/)): Fix config name and add check for duplicate config names (#776): 2 of 2 test(s) reached the change
- [valkey-io/valkey-search#775](https://github.com/valkey-io/valkey-search/pull/775) ([tests](tests/valkey-search/775-419c5d3e5/)): Change default RDB version to 2 (#775): 1 of 1 test(s) reached the change; crash, predates #775, since fixed (recorded)
- [valkey-io/valkey-search#740](https://github.com/valkey-io/valkey-search/pull/740) ([tests](tests/valkey-search/740-21de43864/)): untrack missing fields to properly support negate queries (#740): 0 of 3 test(s) reached the change; crash, introduced and since fixed (recorded)
- [valkey-io/valkey-search#774](https://github.com/valkey-io/valkey-search/pull/774) ([tests](tests/valkey-search/774-ff60845fc/)): Quick fix for unwanted concurrency on index_key_info_ (#774): 0 of 2 test(s) reached the change
- [valkey-io/valkey-search#772](https://github.com/valkey-io/valkey-search/pull/772) ([tests](tests/valkey-search/772-f6d08509e/)): Add VERBATIM to FT AGGREGATE (#772): 2 of 2 test(s) reached the change
- [valkey-io/valkey-search#768](https://github.com/valkey-io/valkey-search/pull/768) ([tests](tests/valkey-search/768-921a95408/)): Remove incorrect evaluation stemming assertion (#768): 0 of 2 test(s) reached the change
- [valkey-io/valkey-search#754](https://github.com/valkey-io/valkey-search/pull/754) ([tests](tests/valkey-search/754-df2edfda9/)): app metric review changes (#754): 2 of 2 test(s) reached the change
- [valkey-io/valkey-search#744](https://github.com/valkey-io/valkey-search/pull/744) ([tests](tests/valkey-search/744-a906ddceb/)): Add attribute data to FT.INFO PRIMARY and CLUSTER mode (#744): 4 of 4 test(s) reached the change
- [valkey-io/valkey-search#598](https://github.com/valkey-io/valkey-search/pull/598) ([tests](tests/valkey-search/598-ff145d2e2/)): Implement single slot indexes (resolves #141) (#598): 3 of 3 test(s) reached the change
- [valkey-io/valkey-search#644](https://github.com/valkey-io/valkey-search/pull/644) ([tests](tests/valkey-search/644-05abf6048/)): [FEATURE] Implement SKIPINITIALSCAN option on FT.CREATE command (#644): 2 of 2 test(s) reached the change
- [valkey-io/valkey-search#753](https://github.com/valkey-io/valkey-search/pull/753) ([tests](tests/valkey-search/753-7328c42cd/)): Add unit test 'pagination_offset_exceeds_remaining' (#753): declined: The change adds only a new parameterized unit test case ('pagination_offset_exceeds_remaining') to testing/ft_search_test.cc, asserting the already-existing LIMIT offset/count reply behaviour of FT.SEARCH's SendReply path. No production source is touched: no command handling, reply encodin

## Totals

- Merges whose tests reached the change: 364 of 518 tested (70%)
- Changed-line coverage: median 100% of 235 merge(s) measured, 15 under 50%
- Recall: 108 parents flagged of 153 fix merges
- Claude: $1603.01 in the last 24 hours, $3548.96 in the last 7 days, $3548.96 in all
- Issues filed: 40
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

## Retries

None.
