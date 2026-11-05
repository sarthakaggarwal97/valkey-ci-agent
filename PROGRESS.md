# Fuzzer walk

Updated 2026-11-05T09:36+00:00.

| Repository | Done | Remaining | Backlog cursor | Last 24 hours | Last 7 days (per day) |
|---|---|---|---|---|---|
| valkey-io/valkey | 85 | 2405 | #5186 (2026-10-30) | 85 | 12.1 |
| valkey-io/valkey-bloom | 29 | 80 | #45 (2025-03-11) | 29 | 4.1 |
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

[2026-11-05T04:40:06+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/41956216161): 20 merge(s) tested, 5 without a code change.

- [valkey-io/valkey-bloom#93](https://github.com/valkey-io/valkey-bloom/pull/93) ([tests](tests/valkey-bloom/93-c6c1558fe/)): Moving metric increment to very end of decode (#93): 0 of 1 test(s) reached the change
- [valkey-io/valkey-bloom#91](https://github.com/valkey-io/valkey-bloom/pull/91) ([tests](tests/valkey-bloom/91-fc0655648/)): Adding validation on tightening_ratio and fp_rate for load (#91): declined: The change only adds defensive (0,1) range validation for fp_rate and tightening_ratio to the bloom module's RDB-load callback (BloomObject::load_from_rdb in src/bloom/data_type.rs); on an out-of-range value it now returns None, failing the load, where the parent accepted it. Every command
- [valkey-io/valkey-bloom#92](https://github.com/valkey-io/valkey-bloom/pull/92): to retry, Claude Code failed (rc=1): timeout after 3000s
- [valkey-io/valkey-bloom#89](https://github.com/valkey-io/valkey-bloom/pull/89) ([tests](tests/valkey-bloom/89-523e19a98/)): Prevalidate in BF.LOAD (#89): declined: Pure reordering refactor of BloomObject::decode_object (the decoder behind BF.LOAD): the capacity<=0 and num_items>capacity validation checks are unchanged context lines present in both builds — the change only splits the single validate+increment loop into a validate-all loop followed by 
- [valkey-io/valkey-bloom#86](https://github.com/valkey-io/valkey-bloom/pull/86) ([tests](tests/valkey-bloom/86-8606eb073/)): Fix replica crash on partial-success BF.MADD/BF.INSERT replication (#86): 3 of 3 test(s) reached the change; crash, fixed by #86 (recorded)
- [valkey-io/valkey-bloom#88](https://github.com/valkey-io/valkey-bloom/pull/88) ([tests](tests/valkey-bloom/88-348bd9f65/)): Add more validation in the RDB Load and BF.LOAD paths (#88): 2 of 2 test(s) reached the change; crash, fixed by #88 (recorded)
- [valkey-io/valkey-bloom#82](https://github.com/valkey-io/valkey-bloom/pull/82) ([tests](tests/valkey-bloom/82-a68614b6e/)): Set ModuleOptions to Handle IO Errors (#82): 2 of 2 test(s) reached the change; crash, fixed by #82 (recorded)
- [valkey-io/valkey-bloom#79](https://github.com/valkey-io/valkey-bloom/pull/79) ([tests](tests/valkey-bloom/79-9b774acd5/)): Fix typo in bf.info.json from 'TIGHTENTING' to 'TIGHTENING' (#79): declined: The change fixes a typo ('TIGHTENTING' -> 'TIGHTENING') in src/commands/bf.info.json, a command-metadata documentation file. These JSON files are not compiled into the module: there is no build.rs, no include_str!/include_bytes!, and no set-command-info call in the crate; commands are regi
- [valkey-io/valkey-bloom#81](https://github.com/valkey-io/valkey-bloom/pull/81) ([tests](tests/valkey-bloom/81-097c2a452/)): Fix AOF rewrite and restore BF tests (#81): declined: The change modifies only the module's own CI test harness: requirements.txt bumps pytest from 6 to 7.4.3, and tests/test_bloom_aofrewrite.py is fixed to enable appendonly before writing data and to keep appendonly=yes across the test's server restart so the AOF is actually replayed. No mod
- [valkey-io/valkey-bloom#73](https://github.com/valkey-io/valkey-bloom/pull/73) ([tests](tests/valkey-bloom/73-d9998e8ce/)): update build.sh to ensure POSIX compliance for dash shell (#73): declined: The change edits only build.sh, converting bash-specific [[ ]] conditionals to POSIX [ ] tests for dash compatibility. It is a build/CI script change with no modification to the module's source code, so the compiled module's runtime behaviour (commands, replies, persistence, replication) i
- [valkey-io/valkey-bloom#76](https://github.com/valkey-io/valkey-bloom/pull/76) ([tests](tests/valkey-bloom/76-148c86d2b/)): Update README to fix 8.0 build and update build.sh to support 9.0 (#76): declined: The change edits only README.md (documentation wording for SERVER_VERSION) and build.sh (accepting '9.0' as a SERVER_VERSION for the integration-test build script). No module source code is touched, so the compiled valkey-bloom artifact at the change is behaviorally identical to the parent
- [valkey-io/valkey-bloom#68](https://github.com/valkey-io/valkey-bloom/pull/68) ([tests](tests/valkey-bloom/68-223d780d2/)): Spelling (#68): declined: The change is a spelling-only PR: it edits CI step names, README/QUICK_START prose, code comments, Python test comments and an assertion message, and the wording of one internal log-warning string. The only code change is renaming the internal Rust constant BLOOM_DEFRAG_DEAFULT to BLOOM_DE
- [valkey-io/valkey-bloom#67](https://github.com/valkey-io/valkey-bloom/pull/67) ([tests](tests/valkey-bloom/67-bf60c40e3/)): Adding clean option for build script and fixing current clippy issues (#67): declined: The change is build/CI and documentation only: a `clean` subcommand added to build.sh, a README note for it, and re-indentation of doc comments in src/bloom/utils.rs to fix a clippy doc-formatting lint. No executable Rust code changes — the module's commands, replies, serialization, and co
- [valkey-io/valkey-bloom#57](https://github.com/valkey-io/valkey-bloom/pull/57) ([tests](tests/valkey-bloom/57-148930ed7/)): Cleaning up usage of valkey-test-framework (#57): declined: no proposed test validated
- [valkey-io/valkey-bloom#47](https://github.com/valkey-io/valkey-bloom/pull/47) ([tests](tests/valkey-bloom/47-c97e0c6da/)): Adding Bloom command JSON files (#47): declined: The change only adds static JSON command-documentation files under src/commands/ (and a trailing newline in .gitignore). Nothing consumes them: valkey-bloom has no build.rs and no include_str!/include_bytes! or other reference to src/commands/; all BF.* commands, their arity, flags and ACL
- [valkey-io/valkey-bloom#56](https://github.com/valkey-io/valkey-bloom/pull/56) ([tests](tests/valkey-bloom/56-d6b8763bc/)): Updating build script and github workflow to get test framework from github and removing it locally (#56): declined: The change modifies only build/CI/test infrastructure: ci.yml and build.sh now clone the valkey-test-framework repo from GitHub, the vendored copy under tests/valkeytests/ is deleted, .gitignore gains an entry, and one test file loses a trailing newline. No module source code (src/) is tou
- [valkey-io/valkey-bloom#55](https://github.com/valkey-io/valkey-bloom/pull/55) ([tests](tests/valkey-bloom/55-5d514e051/)): Set default unstable version and update readme.md (#55): 2 of 2 test(s) reached the change
- [valkey-io/valkey-bloom#54](https://github.com/valkey-io/valkey-bloom/pull/54) ([tests](tests/valkey-bloom/54-9d403c4c7/)): Adding a quick start guide for bloom (#54): declined: The change only adds a new documentation file (QUICK_START.md) and removes an unused Windows branch from build.sh; the build script change cannot affect a Linux build's output, and no module source, command behaviour, replies, persistence or replication code changed. There is no runtime be
- [valkey-io/valkey-bloom#52](https://github.com/valkey-io/valkey-bloom/pull/52) ([tests](tests/valkey-bloom/52-d23399a41/)): Update development script's tests use module extensions from different platforms + Define default/unstable version, stage + add placeholder release notes (#52): 2 of 2 test(s) reached the change
- [valkey-io/valkey-bloom#50](https://github.com/valkey-io/valkey-bloom/pull/50) ([tests](tests/valkey-bloom/50-e308fedf1/)): Adding tightening ratio to bf.info. Making BF.BLOOM-EXPANSION also (#50): 2 of 2 test(s) reached the change

### valkey-io/valkey-json

[2026-11-05T09:22:53+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42001807909): 0 merge(s) tested, 0 without a code change.


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

- Merges whose tests reached the change: 82 of 105 tested (78%)
- Changed-line coverage: median 100% of 47 merge(s) measured, 3 under 50%
- Recall: 21 parents flagged of 26 fix merges
- Claude: $593.64 in the last 24 hours, $593.64 in the last 7 days, $593.64 in all
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

- [valkey-io/valkey-bloom#92](https://github.com/valkey-io/valkey-bloom/pull/92): attempt 1 of 3, Claude Code failed (rc=1): timeout after 3000s
- [valkey-io/valkey-search#1650](https://github.com/valkey-io/valkey-search/pull/1650): attempt 1 of 3, Claude Code failed (rc=1): timeout after 3000s
- [valkey-io/valkey-search#1634](https://github.com/valkey-io/valkey-search/pull/1634): attempt 1 of 3, Claude Code failed (rc=1): timeout after 3000s
- [valkey-io/valkey-search#1381](https://github.com/valkey-io/valkey-search/pull/1381): attempt 1 of 3, Claude Code failed (rc=1): timeout after 3000s
- [valkey-io/valkey-search#1472](https://github.com/valkey-io/valkey-search/pull/1472): attempt 1 of 3, Claude Code failed (rc=1): timeout after 3000s
