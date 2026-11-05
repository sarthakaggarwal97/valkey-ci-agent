# Fuzzer walk

Updated 2026-11-05T09:55+00:00.

| Repository | Done | Remaining | Backlog cursor | Last 24 hours | Last 7 days (per day) |
|---|---|---|---|---|---|
| valkey-io/valkey | 85 | 2405 | #5186 (2026-10-30) | 85 | 12.1 |
| valkey-io/valkey-bloom | 50 | 59 | #26 (2024-12-06) | 50 | 7.1 |
| valkey-io/valkey-json | 62 | 0 | c1423d7e4b38 (2024-11-25) | 62 | 8.9 |
| valkey-io/valkey-search | 24 | 644 | #1481 (2026-10-14) | 24 | 3.4 |

## Last runs

### valkey-io/valkey

[2026-11-05T09:36:25+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/41987127456): 20 merge(s) tested, 0 without a code change.

- [valkey-io/valkey#5199](https://github.com/valkey-io/valkey/pull/5199) ([tests](tests/valkey/5199-4f3dba63f/)): Keep the 'n' keyspace event flag when 'A' is set (#5199): 2 of 2 test(s) reached the change, coverage 100%
- [valkey-io/valkey#5151](https://github.com/valkey-io/valkey/pull/5151) ([tests](tests/valkey/5151-3e04271dd/)): Fix writevToClient stack overflow for empty bulk strings with copy avoidance (#5151): 3 of 3 test(s) reached the change, coverage 100%
- [valkey-io/valkey#5163](https://github.com/valkey-io/valkey/pull/5163) ([tests](tests/valkey/5163-932ee1e45/)): Fix integer overflow in _quicklistNodeAllowMerge size check (#5163): declined: The fix changes the type of a local sum (merge_sz) in the static function _quicklistNodeAllowMerge from unsigned int to size_t, preventing a 32-bit wrap when two packed quicklist nodes' combined size exceeds UINT32_MAX (~4 GiB). This is only reachable with two packed nodes totaling over 4 
- [valkey-io/valkey#5153](https://github.com/valkey-io/valkey/pull/5153) ([tests](tests/valkey/5153-3664d77c5/)): Do not let forked script debug sessions postpone automatic saves (#5153): 3 of 3 test(s) reached the change, coverage 100%; crash, introduced and since fixed (recorded); [crash, appeared after #5153](https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/55)
- [valkey-io/valkey#5168](https://github.com/valkey-io/valkey/pull/5168) ([tests](tests/valkey/5168-a7201627e/)): Keep a lone quote in the last CSV dataset field literally (#5168): 1 of 1 test(s) reached the change, coverage 100%; crash, fixed by #5168 (recorded)
- [valkey-io/valkey#5166](https://github.com/valkey-io/valkey/pull/5166) ([tests](tests/valkey/5166-84d6c236b/)): Don't pause rehashing when iterating a hashtable with no table (#5166): 1 of 1 test(s) reached the change, coverage 100%; crash, fixed by #5166 (recorded)
- [valkey-io/valkey#5132](https://github.com/valkey-io/valkey/pull/5132) ([tests](tests/valkey/5132-e7205778f/)): Hash HyperLogLog elements with a size_t length (#5132): declined: The change only widens MurmurHash64A's length parameter from int to size_t. Its sole command path is PFADD, which hashes each element using sdslen as the length. The result differs between the two builds only for a single element of 2^31 bytes (2 GiB) or larger, where the old signed int ov
- [valkey-io/valkey#5135](https://github.com/valkey-io/valkey/pull/5135) ([tests](tests/valkey/5135-d6aff3537/)): Print usage when valkey-check-rdb --output has no file name (#5135): 1 of 1 test(s) reached the change, coverage 100%; crash, fixed by #5135 (recorded)
- [valkey-io/valkey#5147](https://github.com/valkey-io/valkey/pull/5147) ([tests](tests/valkey/5147-22033385b/)): Check argc before DEBUG SLOTMIGRATION reads its arguments (#5147): 1 of 1 test(s) reached the change, coverage 100%; crash, fixed by #5147 (recorded)
- [valkey-io/valkey#5140](https://github.com/valkey-io/valkey/pull/5140) ([tests](tests/valkey/5140-fb8e73259/)): Reject RESET on slot migration clients (#5140): 2 of 2 test(s) reached the change, coverage 100%; crash, fixed by #5140 (recorded)
- [valkey-io/valkey#5145](https://github.com/valkey-io/valkey/pull/5145) ([tests](tests/valkey/5145-b64ba79eb/)): Ignore new nodes gossiped by a sender still in handshake (#5145): 0 of 3 test(s) reached the change, coverage 100%, 1 skipped (required capability 'host' is unsupported)
- [valkey-io/valkey#5114](https://github.com/valkey-io/valkey/pull/5114) ([tests](tests/valkey/5114-d20193819/)): Allow server.log() during FUNCTION LOAD (#5114): 2 of 2 test(s) reached the change; crash, fixed by #5114 (recorded)
- [valkey-io/valkey#5126](https://github.com/valkey-io/valkey/pull/5126) ([tests](tests/valkey/5126-cdacae6ce/)): Reject a +CONTINUE reply to PSYNC when there is no cached primary (#5126): declined: The fix guards against a primary that violates the replication protocol by replying +CONTINUE to a PSYNC ? -1 (full-resync) request from a replica with no cached primary. A conformant Valkey primary never does this (primaryTryPartialResynchronization always forces a full resync when the re
- [valkey-io/valkey#5180](https://github.com/valkey-io/valkey/pull/5180) ([tests](tests/valkey/5180-4c25c23b9/)): Redact HELLO, AUTH and MIGRATE credentials before validating arguments (#5180): 2 of 2 test(s) reached the change, coverage 100%; check, not reproduced (recorded); check, introduced and since fixed (recorded); check, not reproduced (recorded); check, introduced and since fixed (recorded)
- [valkey-io/valkey#5178](https://github.com/valkey-io/valkey/pull/5178) ([tests](tests/valkey/5178-7a0927136/)): Check XADD element size before creating the stream (#5178): declined: The change adds one guard to xaddCommand that sums the field/value byte lengths and errors before creating the stream key when the total exceeds STREAM_LISTPACK_MAX_SIZE, a fixed compile-time constant of 1<<30 (1 GiB) that no config lowers. The new path only fires when a single XADD carrie
- [valkey-io/valkey#5174](https://github.com/valkey-io/valkey/pull/5174) ([tests](tests/valkey/5174-90f3534dc/)): Fix GEOSEARCH BYPOLYGON when the vertex centroid is outside geohash range (#5174): 2 of 2 test(s) reached the change, coverage 100%
- [valkey-io/valkey#5182](https://github.com/valkey-io/valkey/pull/5182) ([tests](tests/valkey/5182-c0826654b/)): Exempt slot import SYNCSLOTS FINISH from the write pause assertion (#5182): 0 of 5 test(s) reached the change, coverage 0%; crash, predates #5182, since fixed (recorded)
- [valkey-io/valkey#5177](https://github.com/valkey-io/valkey/pull/5177) ([tests](tests/valkey/5177-cd4b2c691/)): Size valkey-check-rdb stats for every selected DB (#5177): 1 of 1 test(s) reached the change, coverage 100%; crash, fixed by #5177 (recorded)
- [valkey-io/valkey#5176](https://github.com/valkey-io/valkey/pull/5176) ([tests](tests/valkey/5176-b8dff68cb/)): Quote the replicaof host when rewriting the config file (#5176): 3 of 3 test(s) reached the change, coverage 100%; [check, introduced by #5176](https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/53)
- [valkey-io/valkey#5185](https://github.com/valkey-io/valkey/pull/5185) ([tests](tests/valkey/5185-679b404d3/)): Replace line breaks in server.log() messages with spaces (#5185): 0 of 2 test(s) reached the change

### valkey-io/valkey-bloom

[2026-11-05T09:55:42+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/41994064121): 20 merge(s) tested, 3 without a code change.

- [valkey-io/valkey-bloom#92](https://github.com/valkey-io/valkey-bloom/pull/92) ([tests](tests/valkey-bloom/92-8a897673d/)): Fixing bug in bloom defrag, adding digest checks to integration test, create new unit test that catches before mentioned bug (#92): 0 of 2 test(s) reached the change
- [valkey-io/valkey-bloom#45](https://github.com/valkey-io/valkey-bloom/pull/45) ([tests](tests/valkey-bloom/45-12fa9358a/)): Adding usage of must_obey_client in wrapper to improve performace (#45): 0 of 1 test(s) reached the change
- [valkey-io/valkey-bloom#49](https://github.com/valkey-io/valkey-bloom/pull/49): to retry, `cargo build --release` failed in parent (exit 101): Compiling heck v0.5.0 Compiling rustc-demangle v0.1.28 Compiling nix v0.26.4 Compiling strum_macros v0.26.4 Compiling backtrace v0.3.77 Compiling v
- [valkey-io/valkey-bloom#48](https://github.com/valkey-io/valkey-bloom/pull/48): to retry, `cargo build --release` failed in merge (exit 101): Compiling lazy_static v1.5.1 Compiling rustc-demangle v0.1.28 Compiling valkey-module-macros-internals v0.1.15 Compiling strum_macros v0.26.4 Compil
- [valkey-io/valkey-bloom#46](https://github.com/valkey-io/valkey-bloom/pull/46) ([tests](tests/valkey-bloom/46-8f1c9f42f/)): Add License file following Valkey (#46): declined: The change only adds a LICENSE file containing the BSD 3-Clause license text. It touches no module source, build configuration, or runtime behaviour, so there is no code path to exercise and no scenario is needed.
- [valkey-io/valkey-bloom#44](https://github.com/valkey-io/valkey-bloom/pull/44) ([tests](tests/valkey-bloom/44-8077d5874/)): Code cleanup: removing unused dependencies in tests, removing digest (#44): 2 of 2 test(s) reached the change; crash, introduced and since fixed (recorded); crash, fixed by #44 (recorded); crash, fixed by #44 (recorded)
- [valkey-io/valkey-bloom#43](https://github.com/valkey-io/valkey-bloom/pull/43) ([tests](tests/valkey-bloom/43-56f631f74/)): Fixing capacity calcualtion and adding a unit test to make sure capacity caluclation matches actual capacity (#43): 2 of 2 test(s) reached the change
- [valkey-io/valkey-bloom#41](https://github.com/valkey-io/valkey-bloom/pull/41) ([tests](tests/valkey-bloom/41-14ad3d053/)): Adding optional arg to BF.INSERT to allow users to check if their bloom filter can reach the desired size (#41): 4 of 4 test(s) reached the change
- [valkey-io/valkey-bloom#39](https://github.com/valkey-io/valkey-bloom/pull/39) ([tests](tests/valkey-bloom/39-1bb7d2520/)): Update README.md (#39): declined: The change only rewrites README.md and replaces a code comment in src/bloom/command_handler.rs (the lines changed are all `//` comments; the WrongArity return and all surrounding logic are untouched). No command behaviour, reply, persistence, or replication path differs between the change 
- [valkey-io/valkey-bloom#38](https://github.com/valkey-io/valkey-bloom/pull/38) ([tests](tests/valkey-bloom/38-a9b578eff/)): ACL Updates, BF.INSERT updates, other misc updates (#38): 6 of 6 test(s) reached the change
- [valkey-io/valkey-bloom#37](https://github.com/valkey-io/valkey-bloom/pull/37) ([tests](tests/valkey-bloom/37-ab158bb6c/)): Large bloom object handling + Rename to BloomFilterType to BloomObject (#37): declined: no proposed test validated
- [valkey-io/valkey-bloom@6a514bfbe8bf](https://github.com/valkey-io/valkey-bloom/commit/6a514bfbe8bf873967c470b694b17bf7e5f10dd3) ([tests](tests/valkey-bloom/6a514bfbe/)): Fix size check and update build script to fail on warnings: 2 of 2 test(s) reached the change
- [valkey-io/valkey-bloom#31](https://github.com/valkey-io/valkey-bloom/pull/31) ([tests](tests/valkey-bloom/31-9baf6fcb8/)): Add TIGHTENING Ratio and FP_Rate configs support (#31): 4 of 4 test(s) reached the change; crash, introduced and since fixed (recorded)
- [valkey-io/valkey-bloom#34](https://github.com/valkey-io/valkey-bloom/pull/34) ([tests](tests/valkey-bloom/34-98ccdc6b4/)): Deflaking tests to remove chance of false positives causing tests to fail (#34): declined: The change modifies only test code: the Rust unit-test helper inside `mod tests` in src/bloom/utils.rs and the Python integration tests under tests/, relaxing assertions so probabilistic bloom-filter false positives don't fail CI. No shipped module code (command handling, replies, persiste
- [valkey-io/valkey-bloom#33](https://github.com/valkey-io/valkey-bloom/pull/33) ([tests](tests/valkey-bloom/33-3589d3075/)): Adding bloom defrag hits and misses to info, added check these metrics get incremented in defrag test (#33): 2 of 2 test(s) reached the change
- [valkey-io/valkey-bloom#32](https://github.com/valkey-io/valkey-bloom/pull/32) ([tests](tests/valkey-bloom/32-8b94c177b/)): Deterministic replication - bloom object creation should have the same properties on replica nodes to match the object on the primary (#32): 5 of 5 test(s) reached the change
- [valkey-io/valkey-bloom#28](https://github.com/valkey-io/valkey-bloom/pull/28) ([tests](tests/valkey-bloom/28-4a8af8002/)): Update default configuration and related test (#28): 2 of 2 test(s) reached the change; check, introduced and since fixed (recorded)
- [valkey-io/valkey-bloom#29](https://github.com/valkey-io/valkey-bloom/pull/29) ([tests](tests/valkey-bloom/29-a5ec8dea6/)): Adding bloom acl category to commands. Added tests to check that the commands contain the correct acl categories (#29): 3 of 3 test(s) reached the change; check, introduced and since fixed (recorded)
- [valkey-io/valkey-bloom#24](https://github.com/valkey-io/valkey-bloom/pull/24) ([tests](tests/valkey-bloom/24-d8a05083c/)): Updating defragmentation to defrag both bloomfiltertype and bloomfiler structs (#24): 8 of 8 test(s) reached the change; check, introduced and since fixed (recorded)
- [valkey-io/valkey-bloom#30](https://github.com/valkey-io/valkey-bloom/pull/30) ([tests](tests/valkey-bloom/30-eaa218fda/)): Adding a wrong type error message when performing a bloom operation on a non bloom key (#30): 2 of 2 test(s) reached the change; check, introduced and since fixed (recorded)

### valkey-io/valkey-json

[2026-11-05T09:53:39+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42006043712): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-search

[2026-11-05T06:22:48+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/41956212865): 20 merge(s) tested, 8 without a code change.

- [valkey-io/valkey-search#1650](https://github.com/valkey-io/valkey-search/pull/1650): to retry, Claude Code failed (rc=1): timeout after 3000s
- [valkey-io/valkey-search#1576](https://github.com/valkey-io/valkey-search/pull/1576) ([tests](tests/valkey-search/1576-7750ee8fa/)): Add Record whether SEPARATOR was given for TAG fields (#1576): 5 of 5 test(s) reached the change
- [valkey-io/valkey-search#1542](https://github.com/valkey-io/valkey-search/pull/1542) ([tests](tests/valkey-search/1542-eda3508f7/)): Stability wedge detection and NaN fix for vectors (#1542): 0 of 1 test(s) reached the change
- [valkey-io/valkey-search#1634](https://github.com/valkey-io/valkey-search/pull/1634): to retry, Claude Code failed (rc=1): timeout after 3000s
- [valkey-io/valkey-search#1528](https://github.com/valkey-io/valkey-search/pull/1528) ([tests](tests/valkey-search/1528-92b779bdd/)): Type-sensitive expression compiler, matching Redis 8 (#1528): 6 of 6 test(s) reached the change
- [valkey-io/valkey-search#1606](https://github.com/valkey-io/valkey-search/pull/1606) ([tests](tests/valkey-search/1606-d11b59102/)): [BUG fix] Fix FT.AGGREGATE ignoring VERBATIM, INORDER and SLOP (#1606): 3 of 3 test(s) reached the change
- [valkey-io/valkey-search#1396](https://github.com/valkey-io/valkey-search/pull/1396) ([tests](tests/valkey-search/1396-fb5cfc828/)): Make SimSIMD dynamic dispatch thread-safe to fix a NULL-call race (#1396): 0 of 2 test(s) reached the change
- [valkey-io/valkey-search#1381](https://github.com/valkey-io/valkey-search/pull/1381): to retry, Claude Code failed (rc=1): timeout after 3000s
- [valkey-io/valkey-search#1436](https://github.com/valkey-io/valkey-search/pull/1436) ([tests](tests/valkey-search/1436-4bb501a45/)): Scoring compatibility tests (#1436): declined: The only product change is a narrow TOCTOU concurrency fix in TextIndexSchema::DeleteKeyData: it skips removing an emptied word from the stem tree if a concurrent add has re-added the word. It has no single-threaded observable effect (empty_words is only populated when the word has no rema
- [valkey-io/valkey-search#1621](https://github.com/valkey-io/valkey-search/pull/1621) ([tests](tests/valkey-search/1621-771657ae8/)): Bracket IPv6 hosts in FT.HYBRID fanout targets (#1621): declined: The only production change is one line in PerformMultiSearchFanoutAsync (src/query/fanout.cc:754): the gRPC dial target for FT.HYBRID's remote-shard fanout is now built with coordinator::FormatAddressWithPort, which brackets IPv6 hosts. That code runs only when valkey-search's coordinator 
- [valkey-io/valkey-search#1491](https://github.com/valkey-io/valkey-search/pull/1491) ([tests](tests/valkey-search/1491-42e85f582/)): fix: Tag revalidation scoring doesn't reliably use latest text field value (#1439) (#1491): 2 of 2 test(s) reached the change
- [valkey-io/valkey-search#1511](https://github.com/valkey-io/valkey-search/pull/1511) ([tests](tests/valkey-search/1511-859e32846/)): test(cursor): Fix race in DestructionObeysBackgroundCleanupSetting (#1511): declined: The change modifies only a unit test file (testing/ft_cursor_test.cc), suspending the utility thread pool to remove a race in the DestructionObeysBackgroundCleanupSetting test's assertions. No production code in the valkey-search module is touched, so the built module at the change is beha
- [valkey-io/valkey-search#1494](https://github.com/valkey-io/valkey-search/pull/1494) ([tests](tests/valkey-search/1494-4d50a150b/)): fix FT.AGGREGATE reports BM25 relevance instead of the KNN distance for hybrid text=>[KNN] queries (#1410) (#1494): 2 of 2 test(s) reached the change
- [valkey-io/valkey-search#1610](https://github.com/valkey-io/valkey-search/pull/1610) ([tests](tests/valkey-search/1610-87d29cc4c/)): Wait for in-flight mutations before sync-path revalidation (#1602) (#1610): 2 of 2 test(s) reached the change
- [valkey-io/valkey-search#1447](https://github.com/valkey-io/valkey-search/pull/1447) ([tests](tests/valkey-search/1447-ad7f1d679/)): fix: compare metadata minimum version during reconciliation (#1447): declined: The change swaps top_level_version for top_level_min_version in the single reconciliation gate in MetadataManager::ReconcileMetadata. A differing accept/reject decision requires proposed metadata whose logical counter (top_level_version) and minimum-version (top_level_min_version) straddle
- [valkey-io/valkey-search#1490](https://github.com/valkey-io/valkey-search/pull/1490) ([tests](tests/valkey-search/1490-c672fd7a7/)): fix: hybrid tag => KNN queries return correct score (issue #1414) (#1490): 4 of 4 test(s) reached the change
- [valkey-io/valkey-search#1472](https://github.com/valkey-io/valkey-search/pull/1472): to retry, Claude Code failed (rc=1): timeout after 3000s
- [valkey-io/valkey-search#1404](https://github.com/valkey-io/valkey-search/pull/1404) ([tests](tests/valkey-search/1404-584d697f0/)): Drop index schemas in every database on FLUSHALL (#1404): 2 of 2 test(s) reached the change
- [valkey-io/valkey-search#1574](https://github.com/valkey-io/valkey-search/pull/1574) ([tests](tests/valkey-search/1574-78eaedf84/)): fix native build on Amazon Linux 2 (#1574): declined: The change modifies only build and CI infrastructure: ci/check_module_allocators.sh gets an expanded allowlist of linked shared libraries (OpenSSL 1.1, libpthread/librt/libdl) with explanatory comments, and src/CMakeLists.txt adds a configure-time check that falls back to the BFD linker wh
- [valkey-io/valkey-search#1319](https://github.com/valkey-io/valkey-search/pull/1319) ([tests](tests/valkey-search/1319-6788bf861/)): ci: add Codecov workflow for unit and integration test coverage (#1319): declined: The change is purely CI/test infrastructure: it adds a Codecov GitHub Actions workflow and codecov.yml, installs lcov in the devcontainer image, hardens ci/build_ubuntu.sh (array-based argument forwarding, safer artifact copying, propagating cleanup errors into the exit code, a --debug fla

## Totals

- Merges whose tests reached the change: 94 of 119 tested (79%)
- Changed-line coverage: median 100% of 47 merge(s) measured, 3 under 50%
- Recall: 23 parents flagged of 29 fix merges
- Claude: $694.16 in the last 24 hours, $694.16 in the last 7 days, $694.16 in all
- Issues filed: 15
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

## Retries

- [valkey-io/valkey-bloom#49](https://github.com/valkey-io/valkey-bloom/pull/49): attempt 1 of 3, `cargo build --release` failed in parent (exit 101): Compiling heck v0.5.0 Compiling rustc-demangle v0.1.28 Compiling nix v0.26.4 Compiling strum_macros v0.26.4 Compiling backtrace v0.3.77 Compiling v
- [valkey-io/valkey-bloom#48](https://github.com/valkey-io/valkey-bloom/pull/48): attempt 1 of 3, `cargo build --release` failed in merge (exit 101): Compiling lazy_static v1.5.1 Compiling rustc-demangle v0.1.28 Compiling valkey-module-macros-internals v0.1.15 Compiling strum_macros v0.26.4 Compil
- [valkey-io/valkey-search#1650](https://github.com/valkey-io/valkey-search/pull/1650): attempt 1 of 3, Claude Code failed (rc=1): timeout after 3000s
- [valkey-io/valkey-search#1634](https://github.com/valkey-io/valkey-search/pull/1634): attempt 1 of 3, Claude Code failed (rc=1): timeout after 3000s
- [valkey-io/valkey-search#1381](https://github.com/valkey-io/valkey-search/pull/1381): attempt 1 of 3, Claude Code failed (rc=1): timeout after 3000s
- [valkey-io/valkey-search#1472](https://github.com/valkey-io/valkey-search/pull/1472): attempt 1 of 3, Claude Code failed (rc=1): timeout after 3000s
