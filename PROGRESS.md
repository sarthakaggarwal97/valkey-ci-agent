# Fuzzer walk

Updated 2026-11-06T22:39+00:00.

| Repository | Done | Remaining | Backlog cursor | Last 24 hours | Last 7 days (per day) |
|---|---|---|---|---|---|
| valkey-io/valkey | 436 | 2062 | #3581 (2026-07-06) | 232 | 62.3 |
| valkey-io/valkey-bloom | 109 | 0 | ce516d0ef6e3 (2024-04-26) | 0 | 15.6 |
| valkey-io/valkey-json | 62 | 0 | c1423d7e4b38 (2024-11-25) | 0 | 8.9 |
| valkey-io/valkey-search | 334 | 334 | #667 (2026-02-13) | 209 | 47.7 |

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

[2026-11-06T22:27:15+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42304749345): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-json

[2026-11-06T22:27:49+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42304808352): 0 merge(s) tested, 0 without a code change.


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

- Merges whose tests reached the change: 355 of 498 tested (71%)
- Changed-line coverage: median 100% of 218 merge(s) measured, 11 under 50%
- Recall: 104 parents flagged of 148 fix merges
- Claude: $1714.01 in the last 24 hours, $3405.21 in the last 7 days, $3405.21 in all
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
