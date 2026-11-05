# Fuzzer walk

Updated 2026-11-05T07:05+00:00.

| Repository | Done | Remaining | Backlog cursor | Last 24 hours | Last 7 days (per day) |
|---|---|---|---|---|---|
| valkey-io/valkey | 45 | 2443 | #5213 (2026-11-02) | 45 | 6.4 |
| valkey-io/valkey-bloom | 29 | 80 | #45 (2025-03-11) | 29 | 4.1 |
| valkey-io/valkey-json | 48 | 14 | c1423d7e4b38 (2024-11-25) | 48 | 6.9 |
| valkey-io/valkey-search | 24 | 644 | #1481 (2026-10-14) | 24 | 3.4 |

## Last runs

### valkey-io/valkey

[2026-11-05T05:58:54+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/41964118901): 20 merge(s) tested, 1 without a code change.

- [valkey-io/valkey#5146](https://github.com/valkey-io/valkey/pull/5146) ([tests](tests/valkey/5146-fb22585dc/)): Fix integer overflow in SORT BY/GET pattern key substitution (#5146): declined: The fix only changes behaviour when prefixlen+sublen+postfixlen of the SORT BY/GET substituted key reaches 2^31: as int it overflowed to a negative value that sign-extended to ~2^64 as size_t and caused an OOM crash, while below 2^31 the int and size_t arithmetic are identical, so parent a
- [valkey-io/valkey#5244](https://github.com/valkey-io/valkey/pull/5244) ([tests](tests/valkey/5244-67e84ec77/)): Mark mutually exclusive ZADD and LCS options (#5244): 2 of 2 test(s) reached the change
- [valkey-io/valkey#5245](https://github.com/valkey-io/valkey/pull/5245) ([tests](tests/valkey/5245-e6c24adda/)): Mark HEXPIREAT and HPEXPIREAT timestamps as unix-time (#5245): 2 of 2 test(s) reached the change
- [valkey-io/valkey#4797](https://github.com/valkey-io/valkey/pull/4797) ([tests](tests/valkey/4797-6454bcc62/)): Forkless Full-Sync (#4797): 0 of 8 test(s) reached the change, coverage 74%
- [valkey-io/valkey#5120](https://github.com/valkey-io/valkey/pull/5120) ([tests](tests/valkey/5120-f9262571d/)): Free repeated register_function named arguments (#5120): 0 of 2 test(s) reached the change
- [valkey-io/valkey#5242](https://github.com/valkey-io/valkey/pull/5242) ([tests](tests/valkey/5242-9ff39f353/)): Rebuild the Lua module when its sources change (#5242): declined: The change only touches Makefiles: it adds a FORCE phony target so the Lua module sub-make always runs, and -MMD dependency-file generation in src/modules/lua/Makefile so incremental builds rebuild objects when their sources change. It is a build/CI correctness fix with no change to any co
- [valkey-io/valkey#5261](https://github.com/valkey-io/valkey/pull/5261) ([tests](tests/valkey/5261-6c4f6fb85/)): Rename bgsave-default-method to forkless-policy (#5261): 2 of 4 test(s) reached the change, coverage 83%; crash, introduced and since fixed (recorded)
- [valkey-io/valkey#5067](https://github.com/valkey-io/valkey/pull/5067) ([tests](tests/valkey/5067-7220c38f9/)): Feature pathhash pathdel (#5067): 8 of 8 test(s) reached the change, coverage 100%
- [valkey-io/valkey#5068](https://github.com/valkey-io/valkey/pull/5068): to retry, 4 of 4 run(s) could not start: the run could not start: environment acquisition failed
- [valkey-io/valkey#5071](https://github.com/valkey-io/valkey/pull/5071) ([tests](tests/valkey/5071-1b84c1365/)): Replace PHCARD with prefix-aware PHPATHCOUNT (#5071): 5 of 5 test(s) reached the change, coverage 100%
- [valkey-io/valkey#5070](https://github.com/valkey-io/valkey/pull/5070) ([tests](tests/valkey/5070-b3b5ab983/)): Feature pathhash prefixes (#5070): 5 of 5 test(s) reached the change, coverage 100%
- [valkey-io/valkey#5066](https://github.com/valkey-io/valkey/pull/5066) ([tests](tests/valkey/5066-14f16537d/)): Feature pathhash scan (#5066): 6 of 6 test(s) reached the change, coverage 100%; check, not reproduced (recorded); check, introduced and since fixed (recorded); check, not reproduced (recorded); check, introduced and since fixed (recorded); check, not reproduced (recorded); check, introduced and since fixed (recorded)
- [valkey-io/valkey#5069](https://github.com/valkey-io/valkey/pull/5069) ([tests](tests/valkey/5069-f986f56b5/)): Rename PHEXISTS to PHPATHEXISTS (#5069): 3 of 3 test(s) reached the change, coverage 100%
- [valkey-io/valkey#5208](https://github.com/valkey-io/valkey/pull/5208) ([tests](tests/valkey/5208-d80033654/)): Fix SENTINEL SIMULATE-FAILURE argument metadata (#5208): 1 of 1 test(s) reached the change
- [valkey-io/valkey#5212](https://github.com/valkey-io/valkey/pull/5212) ([tests](tests/valkey/5212-0d089ef9b/)): Mark RESTORE IDLETIME and FREQ as mutually exclusive (#5212): 3 of 3 test(s) reached the change; [check, introduced by #5212](https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/49)
- [valkey-io/valkey#5159](https://github.com/valkey-io/valkey/pull/5159) ([tests](tests/valkey/5159-8519d0c27/)): Fill unmatched postponed reply lengths left by a module (#5159): 2 of 2 test(s) reached the change, coverage 66%; [crash, introduced by #5159](https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/50)
- [valkey-io/valkey#5121](https://github.com/valkey-io/valkey/pull/5121): to retry, Claude Code failed (rc=1): timeout after 3000s
- [valkey-io/valkey#5116](https://github.com/valkey-io/valkey/pull/5116) ([tests](tests/valkey/5116-c30596075/)): Notify forkless iteration of SWAPDB from the swap itself (#5116): 4 of 4 test(s) reached the change, coverage 100%; [check, introduced by #5116](https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/49)
- [valkey-io/valkey#5118](https://github.com/valkey-io/valkey/pull/5118) ([tests](tests/valkey/5118-0bfc99f58/)): Read Lua error table fields without invoking metamethods (#5118): 2 of 2 test(s) reached the change; crash, fixed by #5118 (recorded)
- [valkey-io/valkey#5237](https://github.com/valkey-io/valkey/pull/5237) ([tests](tests/valkey/5237-05db2e39a/)): Group XACKDEL, XDELEX ids array in command JSON files (#5237): 2 of 2 test(s) reached the change

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

[2026-11-05T07:05:17+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/41983439161): 6 merge(s) tested, 0 without a code change.

- [valkey-io/valkey-json#18](https://github.com/valkey-io/valkey-json/pull/18): to retry, the job left no result
- [valkey-io/valkey-json#22](https://github.com/valkey-io/valkey-json/pull/22): to retry, the job left no result
- [valkey-io/valkey-json@1e33774dd28b](https://github.com/valkey-io/valkey-json/commit/1e33774dd28bdba94bc7f807a4e453f01e1f8f7c): to retry, the job left no result
- [valkey-io/valkey-json#14](https://github.com/valkey-io/valkey-json/pull/14): to retry, the job left no result
- [valkey-io/valkey-json#13](https://github.com/valkey-io/valkey-json/pull/13): to retry, the job left no result
- [valkey-io/valkey-json#1](https://github.com/valkey-io/valkey-json/pull/1): to retry, the job left no result

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

- Merges whose tests reached the change: 55 of 68 tested (81%)
- Changed-line coverage: median 100% of 22 merge(s) measured, 1 under 50%
- Recall: 15 parents flagged of 19 fix merges
- Claude: $426.40 in the last 24 hours, $426.40 in the last 7 days, $426.40 in all
- Issues filed: 10
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

## Retries

- [valkey-io/valkey-bloom#92](https://github.com/valkey-io/valkey-bloom/pull/92): attempt 1 of 3, Claude Code failed (rc=1): timeout after 3000s
- [valkey-io/valkey-json#18](https://github.com/valkey-io/valkey-json/pull/18): attempt 0 of 3, the job left no result
- [valkey-io/valkey-json#22](https://github.com/valkey-io/valkey-json/pull/22): attempt 0 of 3, the job left no result
- [valkey-io/valkey-json@1e33774dd28b](https://github.com/valkey-io/valkey-json/commit/1e33774dd28bdba94bc7f807a4e453f01e1f8f7c): attempt 0 of 3, the job left no result
- [valkey-io/valkey-json#14](https://github.com/valkey-io/valkey-json/pull/14): attempt 0 of 3, the job left no result
- [valkey-io/valkey-json#13](https://github.com/valkey-io/valkey-json/pull/13): attempt 0 of 3, the job left no result
- [valkey-io/valkey-json#1](https://github.com/valkey-io/valkey-json/pull/1): attempt 0 of 3, the job left no result
- [valkey-io/valkey-json#41](https://github.com/valkey-io/valkey-json/pull/41): attempt 0 of 3, `export CMAKE_POLICY_VERSION_MINIMUM=3.5` failed in parent (exit 2): | ^~~~~ /usr/include/c++/15/bits/alloc_traits.h:85:31: note: ‘std::integral_constant<bool, false>::value’ evaluates to false /home/
- [valkey-io/valkey-json#39](https://github.com/valkey-io/valkey-json/pull/39): attempt 0 of 3, `export CMAKE_POLICY_VERSION_MINIMUM=3.5` failed in merge (exit 2): /home/runner/work/valkey-ci-agent/valkey-ci-agent/walk-work/merge/src/json/dom.cc:1435:9: warning: the address of ‘str’ will never b
- [valkey-io/valkey-json#35](https://github.com/valkey-io/valkey-json/pull/35): attempt 0 of 3, `export CMAKE_POLICY_VERSION_MINIMUM=3.5` failed in merge (exit 2): | ^~~~~ /usr/include/c++/15/bits/alloc_traits.h:85:31: note: ‘std::integral_constant<bool, false>::value’ evaluates to false /home/r
- [valkey-io/valkey-json@ac4f334af7a6](https://github.com/valkey-io/valkey-json/commit/ac4f334af7a6cd028b2f2ab898d52ea61f501c70): attempt 0 of 3, `export CMAKE_POLICY_VERSION_MINIMUM=3.5` failed in merge (exit 2): /home/runner/work/valkey-ci-agent/valkey-ci-agent/walk-work/merge/src/json/dom.cc:1419:9: warning: the address of ‘str’ will never b
- [valkey-io/valkey-json@d3ce5afb6dd3](https://github.com/valkey-io/valkey-json/commit/d3ce5afb6dd3fb88086aa9218cfef3197b75b388): attempt 0 of 3, `export CMAKE_POLICY_VERSION_MINIMUM=3.5` failed in merge (exit 2): /home/runner/work/valkey-ci-agent/valkey-ci-agent/walk-work/merge/src/json/dom.cc:1420:9: warning: the address of ‘str’ will never b
- [valkey-io/valkey-json#27](https://github.com/valkey-io/valkey-json/pull/27): attempt 0 of 3, `export CMAKE_POLICY_VERSION_MINIMUM=3.5` failed in merge (exit 2): /usr/include/c++/15/bits/alloc_traits.h:85:31: note: ‘std::integral_constant<bool, false>::value’ evaluates to false make[3]: *** [s
- [valkey-io/valkey-json#21](https://github.com/valkey-io/valkey-json/pull/21): attempt 0 of 3, `export CMAKE_POLICY_VERSION_MINIMUM=3.5` failed in merge (exit 2): /home/runner/work/valkey-ci-agent/valkey-ci-agent/walk-work/merge/src/json/dom.cc:1420:9: warning: the address of ‘str’ will never b
- [valkey-io/valkey-json#23](https://github.com/valkey-io/valkey-json/pull/23): attempt 0 of 3, `export CMAKE_POLICY_VERSION_MINIMUM=3.5` failed in merge (exit 2): | ^~~~~ /usr/include/c++/15/bits/alloc_traits.h:85:31: note: ‘std::integral_constant<bool, false>::value’ evaluates to false /home/r
- [valkey-io/valkey-search#1650](https://github.com/valkey-io/valkey-search/pull/1650): attempt 1 of 3, Claude Code failed (rc=1): timeout after 3000s
- [valkey-io/valkey-search#1634](https://github.com/valkey-io/valkey-search/pull/1634): attempt 1 of 3, Claude Code failed (rc=1): timeout after 3000s
- [valkey-io/valkey-search#1381](https://github.com/valkey-io/valkey-search/pull/1381): attempt 1 of 3, Claude Code failed (rc=1): timeout after 3000s
- [valkey-io/valkey-search#1472](https://github.com/valkey-io/valkey-search/pull/1472): attempt 1 of 3, Claude Code failed (rc=1): timeout after 3000s
- [valkey-io/valkey#5068](https://github.com/valkey-io/valkey/pull/5068): attempt 1 of 3, 4 of 4 run(s) could not start: the run could not start: environment acquisition failed
- [valkey-io/valkey#5121](https://github.com/valkey-io/valkey/pull/5121): attempt 1 of 3, Claude Code failed (rc=1): timeout after 3000s
