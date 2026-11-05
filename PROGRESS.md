# Fuzzer walk

Updated 2026-11-05T14:01+00:00.

| Repository | Done | Remaining | Backlog cursor | Last 24 hours | Last 7 days (per day) |
|---|---|---|---|---|---|
| valkey-io/valkey | 114 | 2377 | #5030 (2026-10-26) | 114 | 16.3 |
| valkey-io/valkey-bloom | 98 | 11 | b11970ee2c0a (2024-09-10) | 98 | 14.0 |
| valkey-io/valkey-json | 62 | 0 | c1423d7e4b38 (2024-11-25) | 62 | 8.9 |
| valkey-io/valkey-search | 77 | 591 | #1362 (2026-09-12) | 77 | 11.0 |

## Last runs

### valkey-io/valkey

[2026-11-05T12:16:47+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42003770568): 20 merge(s) tested, 10 without a code change.

- [valkey-io/valkey#5186](https://github.com/valkey-io/valkey/pull/5186) ([tests](tests/valkey/5186-a4ffea938/)): Skip RESP3 attributes when reading replies in valkey-cli (#5186): 1 of 1 test(s) reached the change, coverage 100%
- [valkey-io/valkey#5175](https://github.com/valkey-io/valkey/pull/5175) ([tests](tests/valkey/5175-663fff858/)): Stop valkey-benchmark function script generation when the buffer is full (#5175): 3 of 3 test(s) reached the change, coverage 100%; crash, fixed by #5175 (recorded)
- [valkey-io/valkey#5181](https://github.com/valkey-io/valkey/pull/5181) ([tests](tests/valkey/5181-9c4c8739d/)): Apply the library load timeout to FUNCTION RESTORE (#5181): 0 of 10 test(s) reached the change, coverage 37%
- [valkey-io/valkey#5173](https://github.com/valkey-io/valkey/pull/5173): to retry, the job left no result
- [valkey-io/valkey#5187](https://github.com/valkey-io/valkey/pull/5187) ([tests](tests/valkey/5187-eef69cb6d/)): Defer freeing the client that runs a replication role change (#5187): 0 of 4 test(s) reached the change, coverage 81%; crash, fixed by #5187 (recorded); crash, fixed by #5187 (recorded); [check, introduced by #5187](https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/59)
- [valkey-io/valkey#5098](https://github.com/valkey-io/valkey/pull/5098) ([tests](tests/valkey/5098-45cd91459/)): Skip QoS event draining when AE_FILE_EVENTS is not set (#5098): declined: The change guards aeProcessQoSEvents in aeProcessEvents behind the AE_FILE_EVENTS flag, but every production caller already passes that flag: aeMain uses AE_ALL_EVENTS and processEventsWhileBlocked uses AE_FILE_EVENTS|AE_DONT_WAIT, and these are the only two call sites outside unit tests. 
- [valkey-io/valkey#5141](https://github.com/valkey-io/valkey/pull/5141) ([tests](tests/valkey/5141-6e3eddf49/)): Don't block CLUSTER SETSLOT on replica acks when the client cannot block (#5141): 2 of 4 test(s) reached the change, coverage 100%; crash, fixed by #5141 (recorded)
- [valkey-io/valkey#5148](https://github.com/valkey-io/valkey/pull/5148) ([tests](tests/valkey/5148-3350565b3/)): Allow user to pass testing option via make file & use that to dump logs in coverage job (#5148): declined: The change is purely build/CI plumbing: it adds a TEST_OPTIONS variable to the Makefile's test, test-modules, test-sentinel, test-cluster and lcov targets so extra flags can be passed to the runtest scripts, and uses it in the codecov workflow to pass --dump-logs. No Valkey server, command
- [valkey-io/valkey#5109](https://github.com/valkey-io/valkey/pull/5109) ([tests](tests/valkey/5109-13d7ed2aa/)): Free Lua function name and description with ValkeyModule_FreeString (#5109): 1 of 1 test(s) reached the change
- [valkey-io/valkey#5111](https://github.com/valkey-io/valkey/pull/5111) ([tests](tests/valkey/5111-88089c54e/)): Fix clang-format in the VM_CallArgv doc comment (#5111): declined: The change edits a single line of a documentation comment above VM_CallArgv in src/module.c, correcting indentation for clang-format. No executable code, API behaviour, reply, or build configuration is altered, so there is nothing a scenario could exercise.
- [valkey-io/valkey#3911](https://github.com/valkey-io/valkey/pull/3911) ([tests](tests/valkey/3911-20f640930/)): Make VM_CallArgv() API public (#3911): 4 of 6 test(s) reached the change, coverage 74%; [check, introduced by #3911](https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/60)
- [valkey-io/valkey#5091](https://github.com/valkey-io/valkey/pull/5091) ([tests](tests/valkey/5091-5a70cde9e/)): Fix VM_Call crash on script-mode validation errors (#5091): 2 of 2 test(s) reached the change, coverage 100%
- [valkey-io/valkey#5092](https://github.com/valkey-io/valkey/pull/5092) ([tests](tests/valkey/5092-3aa4cbce4/)): Count expired but unreaped hash fields in objectComputeSize (#5092): 2 of 2 test(s) reached the change, coverage 100%
- [valkey-io/valkey#5046](https://github.com/valkey-io/valkey/pull/5046) ([tests](tests/valkey/5046-f2ca837ed/)): Fix memory leak in GetCommandKeysWithFlags (#5046): declined: The fix adds getKeysFreeResult() to VM_GetCommandKeysWithFlags, a module-only API that leaks the heap-allocated getKeysResult.keys array only when a command produces more than 256 keys (MAX_KEYS_BUFFER). It is reachable exclusively through RM_GetCommandKeys/WithFlags, which among all test 
- [valkey-io/valkey#4877](https://github.com/valkey-io/valkey/pull/4877) ([tests](tests/valkey/4877-3192cf694/)): Block DEBUG RELOAD and DEBUG LOADAOF in loading and async-loading (#4877): 2 of 8 test(s) reached the change, coverage 100%
- [valkey-io/valkey#4995](https://github.com/valkey-io/valkey/pull/4995) ([tests](tests/valkey/4995-faec379ec/)): Fix GEOSEARCH repeated BYPOLYGON memory leak (#4995): declined: Pure memory-leak fix: repeated GEOSEARCH/GEOSEARCHSTORE BYPOLYGON now frees the earlier polygon-vertex allocation before re-allocating. Both builds already accept repeated BYPOLYGON and use the last polygon, so all replies, errors, and stored data are identical before and after the fix; th
- [valkey-io/valkey#4863](https://github.com/valkey-io/valkey/pull/4863) ([tests](tests/valkey/4863-255018c00/)): Fix module blocking of RESP3 Pub/Sub clients (#4863): 0 of 1 test(s) reached the change, coverage 100%; [check, introduced by #4863](https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/58)
- [valkey-io/valkey#4655](https://github.com/valkey-io/valkey/pull/4655) ([tests](tests/valkey/4655-ee247dbd4/)): [Bug Fix] Fix fast-fail double-counting a NACK voter that later fails (#4655): 0 of 5 test(s) reached the change, coverage 95%, 1 skipped (required capability 'host' is unsupported)
- [valkey-io/valkey#4954](https://github.com/valkey-io/valkey/pull/4954) ([tests](tests/valkey/4954-25d3b7d9e/)): Match listpack for lex range commands on mixed-score btree zsets (#4954): 3 of 3 test(s) reached the change, coverage 100%
- [valkey-io/valkey#4886](https://github.com/valkey-io/valkey/pull/4886) ([tests](tests/valkey/4886-938007fd9/)): Only warn about a command line password when -a or -u set it (#4886): 1 of 1 test(s) reached the change, coverage 100%

### valkey-io/valkey-bloom

[2026-11-05T13:26:13+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42029790709): 20 merge(s) tested, 0 without a code change.

- [valkey-io/valkey-bloom@ec72262033e7](https://github.com/valkey-io/valkey-bloom/commit/ec72262033e76d83783573a8ee17fd38794e7b48) ([tests](tests/valkey-bloom/ec7226203/)): Migrate to valkeymodule-rs: 0 of 4 test(s) reached the change; check, predates ec72262033e7, since fixed (recorded)
- [valkey-io/valkey-bloom@3ebb15b1df2d](https://github.com/valkey-io/valkey-bloom/commit/3ebb15b1df2d3d747d2c0de7ffd0bd1c8b5db08f) ([tests](tests/valkey-bloom/3ebb15b1d/)): Add more documentation: declined: The change is documentation-only: it edits TODO comments in bloom_config.rs, removes stale comments in commands/bloom.rs and bloom_util.rs, adds Rust doc comments to the command handlers in lib.rs, and fixes a comment in wrapper/bloom_callback.rs. The sole non-comment edit is removing `pub
- [valkey-io/valkey-bloom@b7504e965e06](https://github.com/valkey-io/valkey-bloom/commit/b7504e965e06f4b87f024b12fa17da9d96f3fbf6) ([tests](tests/valkey-bloom/b7504e965/)): Add support for COPY commands on BloomFilter datatypes: 3 of 3 test(s) reached the change; check, introduced and since fixed (recorded)
- [valkey-io/valkey-bloom@6ca68c5eef40](https://github.com/valkey-io/valkey-bloom/commit/6ca68c5eef40deb0a6ccd2a88d62372bec428b01) ([tests](tests/valkey-bloom/6ca68c5ee/)): Cargo fmt: declined: The change is purely cosmetic: cargo fmt re-wrapping, whitespace removal, and import reordering in the Rust sources, removal of an optional clippy build-dependency from Cargo.toml (lint tooling only, not compiled into the module), and README build-instruction edits. Every hunk preserves th
- [valkey-io/valkey-bloom@ef4e21538d29](https://github.com/valkey-io/valkey-bloom/commit/ef4e21538d29c02d506a2c708f65ce1f0ca08754) ([tests](tests/valkey-bloom/ef4e21538/)): Specify package license: declined: The change adds a license field to Cargo.toml and a license mention to README.md. It is pure packaging/documentation metadata: no Rust source, command handling, data encoding, replication, or configuration behaviour changes, so no runtime behaviour can differ between the two builds and no 
- [valkey-io/valkey-bloom@bebf72ac79df](https://github.com/valkey-io/valkey-bloom/commit/bebf72ac79dfc8996dfbedad2a99ebcb008240c5) ([tests](tests/valkey-bloom/bebf72ac7/)): Update README and address all clippy warnings: declined: The change is a README update, an optional clippy build-dependency in Cargo.toml, and mechanical clippy-lint refactors: &Vec<RedisString> parameters become &[RedisString], explicit index loops become equivalent iterator chains (take(len).skip(start) covers the same range in the same order)
- [valkey-io/valkey-bloom@159c9385b0ac](https://github.com/valkey-io/valkey-bloom/commit/159c9385b0acbcb142bbf156e065b54cfed48f6f) ([tests](tests/valkey-bloom/159c9385b/)): fix clippy warnings: declined: Both hunks are mechanical clippy fixes with identical semantics. In bloom_filter_reserve, `num >= 0.0 && num < 1.0` is rewritten as `(0.0..1.0).contains(&num)`, and Range::contains is defined as `start <= item && item < end`, so the BF.RESERVE error-rate guard accepts and rejects exactly t
- [valkey-io/valkey-bloom@3f85cae12cec](https://github.com/valkey-io/valkey-bloom/commit/3f85cae12ceca4441475f189677208a49a17c7cf) ([tests](tests/valkey-bloom/3f85cae12/)): fixed few clippy warnings: 2 of 2 test(s) reached the change; check, introduced and since fixed (recorded)
- [valkey-io/valkey-bloom@753575a84166](https://github.com/valkey-io/valkey-bloom/commit/753575a84166b399c691ffb7385d09f171ed4fc7) ([tests](tests/valkey-bloom/753575a84/)): Type conversions + struct updates to use less memory: 3 of 3 test(s) reached the change; check, introduced and since fixed (recorded)
- [valkey-io/valkey-bloom@a2343b744303](https://github.com/valkey-io/valkey-bloom/commit/a2343b744303d5323d17d35b0a592b2d5ef50784) ([tests](tests/valkey-bloom/a2343b744/)): Update error messages: 0 of 2 test(s) reached the change; check, predates a2343b744303, since fixed (recorded)
- [valkey-io/valkey-bloom@fbdfb5bf7906](https://github.com/valkey-io/valkey-bloom/commit/fbdfb5bf790644a1c2d49b664efccb953a17f3e6) ([tests](tests/valkey-bloom/fbdfb5bf7/)): Add support for BF.INSERT and fix multi add logic: 6 of 6 test(s) reached the change; check, introduced and since fixed (recorded); crash, fixed by fbdfb5bf7906 (recorded)
- [valkey-io/valkey-bloom@053d5e9ade3b](https://github.com/valkey-io/valkey-bloom/commit/053d5e9ade3bd19b571834e2490bd290c86e44cb) ([tests](tests/valkey-bloom/053d5e9ad/)): minor refactoring: declined: The change is a pure refactor: it renames BloomFilterType2 to BloomFilterType and BLOOM_FILTER_TYPE2 to BLOOM_FILTER_TYPE (Rust identifiers only — the RedisType name string "bloomtype" and encoding version 0 are unchanged, so RDB and TYPE behaviour are identical), replaces the string liter
- [valkey-io/valkey-bloom@d00fa993ba24](https://github.com/valkey-io/valkey-bloom/commit/d00fa993ba2471f4fdb0b3328c254e2c6c0aa431) ([tests](tests/valkey-bloom/d00fa993b/)): Update Cargo.toml: declined: The change only adds package metadata (authors, repository, readme, description, homepage) to Cargo.toml. It touches no source code, dependencies, features, or build configuration, so the compiled module's behaviour is identical on both builds and there is nothing to test.
- [valkey-io/valkey-bloom@1c1b6abee7ca](https://github.com/valkey-io/valkey-bloom/commit/1c1b6abee7ca999874989ec2737693f7cfd82c33) ([tests](tests/valkey-bloom/1c1b6abee/)): Fix mem_usage calculation: 4 of 4 test(s) reached the change; check, predates 1c1b6abee7ca, since fixed (recorded)
- [valkey-io/valkey-bloom@ff608170b4c7](https://github.com/valkey-io/valkey-bloom/commit/ff608170b4c794acd48335afc366ede7ffef6672) ([tests](tests/valkey-bloom/ff608170b/)): Update TODOs: 3 of 3 test(s) reached the change
- [valkey-io/valkey-bloom@481ecac43632](https://github.com/valkey-io/valkey-bloom/commit/481ecac436320f14bb91ad1fd4350c91f95f1557) ([tests](tests/valkey-bloom/481ecac43/)): Refactoring: declined: Pure refactor with no behaviour change: BloomFilterType2 and BloomFilter are moved verbatim from bloom_data_type.rs to bloom_util.rs; item parameters change from &RedisString to &[u8] but the identical byte slices were already what reached Bloom<[u8]>::set/check (new_with_item used item.as
- [valkey-io/valkey-bloom@42d015cd68c8](https://github.com/valkey-io/valkey-bloom/commit/42d015cd68c80565d5a605bc9d3739b26f8c5091) ([tests](tests/valkey-bloom/42d015cd6/)): Fix RDB Save for scaled filters: 4 of 4 test(s) reached the change; check, introduced and since fixed (recorded); crash, fixed by 42d015cd68c8 (recorded)
- [valkey-io/valkey-bloom@5545680b5ef9](https://github.com/valkey-io/valkey-bloom/commit/5545680b5ef99b1aaaf14ebbbb0a96d3d3667bb8) ([tests](tests/valkey-bloom/5545680b5/)): Add auto scaling support for bloom filters: 2 of 6 test(s) reached the change; check, introduced and since fixed (recorded); crash, introduced and since fixed (recorded); crash, introduced and since fixed (recorded); crash, introduced and since fixed (recorded)
- [valkey-io/valkey-bloom@8f5e3fc41134](https://github.com/valkey-io/valkey-bloom/commit/8f5e3fc411342ce5491c595b1e8db80b92383755) ([tests](tests/valkey-bloom/8f5e3fc41/)): Update error handling / messages and update expansion logic: 3 of 3 test(s) reached the change; check, introduced and since fixed (recorded)
- [valkey-io/valkey-bloom@bdf5f0dc5471](https://github.com/valkey-io/valkey-bloom/commit/bdf5f0dc5471c87235fe77ccf196adf2d8028fa2) ([tests](tests/valkey-bloom/bdf5f0dc5/)): Add support for BF.MADD: 3 of 3 test(s) reached the change

### valkey-io/valkey-json

[2026-11-05T13:36:43+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42037206907): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-search

[2026-11-05T14:01:10+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42007906355): 20 merge(s) tested, 6 without a code change.

- [valkey-io/valkey-search#1464](https://github.com/valkey-io/valkey-search/pull/1464) ([tests](tests/valkey-search/1464-0cbca0e60/)): Fix GROUPBY 0 being rejected in FT.AGGREGATE (#1464): 2 of 2 test(s) reached the change
- [valkey-io/valkey-search#1487](https://github.com/valkey-io/valkey-search/pull/1487) ([tests](tests/valkey-search/1487-c24e74581/)): perf: Restore HNSW ingestion and search throughput to 1.2 levels (#1487): declined: no proposed test validated
- [valkey-io/valkey-search#1304](https://github.com/valkey-io/valkey-search/pull/1304) ([tests](tests/valkey-search/1304-7687ca2e9/)): Skip metadata reconciliation after replica demotion (#1304): declined: The only behavioral change is a race guard added inside the asynchronous RunByMain callback in MetadataManager::HandleBroadcastedMetadata: it re-checks vmsdk::IsReplica(ctx) on the main thread after the gRPC GetGlobalMetadata fetch returns, discarding the reconciliation if the node was dem
- [valkey-io/valkey-search#1445](https://github.com/valkey-io/valkey-search/pull/1445) ([tests](tests/valkey-search/1445-0a8e615a2/)): fixed: sort key should be nil instead of `#` when sortby is missing (#1445): 2 of 2 test(s) reached the change
- [valkey-io/valkey-search#1395](https://github.com/valkey-io/valkey-search/pull/1395) ([tests](tests/valkey-search/1395-20f414ee1/)): Accept NOHL and SORTABLE UNF in FT.CREATE, and report sortable/unf in FT.INFO (#1395): 4 of 4 test(s) reached the change
- [valkey-io/valkey-search#1354](https://github.com/valkey-io/valkey-search/pull/1354) ([tests](tests/valkey-search/1354-cc6cb5e7f/)): Scoring: BM25STD scoring for stemming queries (#1354): 4 of 4 test(s) reached the change
- [valkey-io/valkey-search#1394](https://github.com/valkey-io/valkey-search/pull/1394) ([tests](tests/valkey-search/1394-cad356055/)): Add WITHCURSOR to FT.AGGREGATE and FT.SEARCH, and the FT.CURSOR command (#1394): 4 of 4 test(s) reached the change; [crash, appeared after #1394](https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/61); check, introduced and since fixed (recorded); crash, fixed by #1394 (recorded)
- [valkey-io/valkey-search#967](https://github.com/valkey-io/valkey-search/pull/967) ([tests](tests/valkey-search/967-d719c30c2/)): feat: Implement INKEYS parameter for FT.SEARCH (#967): 3 of 3 test(s) reached the change
- [valkey-io/valkey-search#1400](https://github.com/valkey-io/valkey-search/pull/1400) ([tests](tests/valkey-search/1400-0e1bc3303/)): Various small scoring changes (#1400): 4 of 4 test(s) reached the change
- [valkey-io/valkey-search#1083](https://github.com/valkey-io/valkey-search/pull/1083): to retry, the job left no result
- [valkey-io/valkey-search#1402](https://github.com/valkey-io/valkey-search/pull/1402) ([tests](tests/valkey-search/1402-e968b168c/)): Fixing Alpine Build (#1402): 0 of 6 test(s) reached the change
- [valkey-io/valkey-search#1406](https://github.com/valkey-io/valkey-search/pull/1406) ([tests](tests/valkey-search/1406-ef5771391/)): Fix long-running clang-format after running integration tests (#1406): declined: The change modifies only the format() function in build.sh, adding a -prune for .build* directories so clang-format skips build artifacts after integration tests. It is a developer-tooling/build-script fix with no change to any module source file, command, reply, persistence, or replicatio
- [valkey-io/valkey-search#1389](https://github.com/valkey-io/valkey-search/pull/1389) ([tests](tests/valkey-search/1389-6428b669b/)): Bug fix: only last RETURN in SEARCH should be effective (#1389): 2 of 2 test(s) reached the change
- [valkey-io/valkey-search#1350](https://github.com/valkey-io/valkey-search/pull/1350) ([tests](tests/valkey-search/1350-4f6ef4b04/)): Scoring: BM25STD scoring for prefix, suffix and fuzzy queries (#1350): 7 of 7 test(s) reached the change
- [valkey-io/valkey-search#1218](https://github.com/valkey-io/valkey-search/pull/1218) ([tests](tests/valkey-search/1218-03f27d2ac/)): Support field-scoped text group @field:(a|b|c) (#1214) (#1218): 2 of 2 test(s) reached the change
- [valkey-io/valkey-search#1090](https://github.com/valkey-io/valkey-search/pull/1090) ([tests](tests/valkey-search/1090-0cc7609c6/)): Fix AS parameter substitution for vector score aliases (#1090): 2 of 2 test(s) reached the change
- [valkey-io/valkey-search#1279](https://github.com/valkey-io/valkey-search/pull/1279) ([tests](tests/valkey-search/1279-20757bae4/)): RFC for multi-language support in FTS (#1279): declined: The change is documentation-only: it adds rfc/multi-language-support.md (a proposal document for future multi-language FTS support) and excludes that file from the typos checker in .config/typos.toml. No source code, build configuration, or runtime behaviour of the module changes, so the c
- [valkey-io/valkey-search#984](https://github.com/valkey-io/valkey-search/pull/984) ([tests](tests/valkey-search/984-cbf0fda34/)): Implement FILTER option on FT.CREATE (#984): 5 of 5 test(s) reached the change
- [valkey-io/valkey-search#1373](https://github.com/valkey-io/valkey-search/pull/1373) ([tests](tests/valkey-search/1373-707324aa1/)): FT.AGGREGATE: SORTBY ignores a following LIMIT, breaking paging past the first page (#1373): 2 of 2 test(s) reached the change
- [valkey-io/valkey-search#1366](https://github.com/valkey-io/valkey-search/pull/1366) ([tests](tests/valkey-search/1366-9db11eff5/)): Move compatibility reference to redis:latest and fix what it exposed (supersedes #1068, #1239) (#1366): 4 of 8 test(s) reached the change

## Totals

- Merges whose tests reached the change: 153 of 191 tested (80%)
- Changed-line coverage: median 100% of 60 merge(s) measured, 4 under 50%
- Recall: 38 parents flagged of 48 fix merges
- Claude: $1145.12 in the last 24 hours, $1145.12 in the last 7 days, $1145.12 in all
- Issues filed: 19
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

## Retries

- [valkey-io/valkey-search#1083](https://github.com/valkey-io/valkey-search/pull/1083): attempt 0 of 3, the job left no result
- [valkey-io/valkey#5173](https://github.com/valkey-io/valkey/pull/5173): attempt 0 of 3, the job left no result
