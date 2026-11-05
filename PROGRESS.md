# Fuzzer walk

Updated 2026-11-05T13:36+00:00.

| Repository | Done | Remaining | Backlog cursor | Last 24 hours | Last 7 days (per day) |
|---|---|---|---|---|---|
| valkey-io/valkey | 114 | 2377 | #5030 (2026-10-26) | 114 | 16.3 |
| valkey-io/valkey-bloom | 98 | 11 | b11970ee2c0a (2024-09-10) | 98 | 14.0 |
| valkey-io/valkey-json | 62 | 0 | c1423d7e4b38 (2024-11-25) | 62 | 8.9 |
| valkey-io/valkey-search | 52 | 616 | #1464 (2026-10-01) | 52 | 7.4 |

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

[2026-11-05T10:06:17+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/41980157759): 20 merge(s) tested, 8 without a code change.

- [valkey-io/valkey-search#1650](https://github.com/valkey-io/valkey-search/pull/1650) ([tests](tests/valkey-search/1650-dd28a1246/)): fix: copy FT.AGGREGATE/FT.HYBRID params into owned storage (#1650): 0 of 2 test(s) reached the change
- [valkey-io/valkey-search#1634](https://github.com/valkey-io/valkey-search/pull/1634) ([tests](tests/valkey-search/1634-7e7adf25e/)): FT.HYBRID: rescore mutated documents in a match-all SEARCH arm (#1634): 0 of 2 test(s) reached the change
- [valkey-io/valkey-search#1381](https://github.com/valkey-io/valkey-search/pull/1381) ([tests](tests/valkey-search/1381-af257c5f3/)): LOAD * plus autoloaded Aggregation fields generates incompatible JSON result. (#1381): 3 of 3 test(s) reached the change
- [valkey-io/valkey-search#1472](https://github.com/valkey-io/valkey-search/pull/1472) ([tests](tests/valkey-search/1472-8360dc56e/)): Filtering/scoring caching and main thread locking improvements (#1472): 0 of 2 test(s) reached the change
- [valkey-io/valkey-search#1481](https://github.com/valkey-io/valkey-search/pull/1481) ([tests](tests/valkey-search/1481-3df4bca49/)): fix flaky index consistency test from unbounded index update count (#1481): declined: The diff changes only testing/index_schema_test.cc, a unit test in valkey-search's own test suite: it restructures the index-consistency test into two mutation waves and tightens its counter assertions to remove flakiness. No production code of the module changes, so the built module's beh
- [valkey-io/valkey-search#1536](https://github.com/valkey-io/valkey-search/pull/1536) ([tests](tests/valkey-search/1536-c47ae7b73/)): Fix Gate non-UTF-8 TAG indexing behavior change behind search.emulate-release (#1536): 2 of 2 test(s) reached the change
- [valkey-io/valkey-search#1361](https://github.com/valkey-io/valkey-search/pull/1361) ([tests](tests/valkey-search/1361-1ce64680f/)): Skip orphan multi/exec keys in SaveIndexExtension instead of aborting (#1361): declined: no proposed test validated
- [valkey-io/valkey-search#1407](https://github.com/valkey-io/valkey-search/pull/1407) ([tests](tests/valkey-search/1407-c1819da5b/)): fix(query): do not error on unused PARAMS in FT.SEARCH/FT.AGGREGATE (#1372) (#1407): 2 of 2 test(s) reached the change
- [valkey-io/valkey-search#1562](https://github.com/valkey-io/valkey-search/pull/1562) ([tests](tests/valkey-search/1562-53ed36499/)): Apply debug-mode before other module args (#1562): declined: The change affects only ModuleConfigManager::ParseAndLoadArgv, which runs once at module load over the arguments written after the module's path on the loadmodule directive: it applies --debug-mode in a first pass so Dev() configs given before it on the command line no longer fail the load
- [valkey-io/valkey-search#1505](https://github.com/valkey-io/valkey-search/pull/1505) ([tests](tests/valkey-search/1505-c338559c0/)): Fix weight 0 is rejected (#1505): 2 of 2 test(s) reached the change
- [valkey-io/valkey-search#1549](https://github.com/valkey-io/valkey-search/pull/1549) ([tests](tests/valkey-search/1549-d8cf85ed1/)): Sync command JSON files and docs with 1.3.0 behavior (#1549): declined: The change edits only documentation markdown (docs/commands/*.md) and command-metadata JSON files (src/commands/*.json). Those JSON files are not compiled into the module, not read at runtime, and not consumed by any build rule, script, or test in the repository; command registration happe
- [valkey-io/valkey-search#1529](https://github.com/valkey-io/valkey-search/pull/1529) ([tests](tests/valkey-search/1529-b7f6135a2/)): fix: FT.HYBRID drops coordinator's local shard together when one arm fails (#1529): 2 of 2 test(s) reached the change
- [valkey-io/valkey-search#1263](https://github.com/valkey-io/valkey-search/pull/1263) ([tests](tests/valkey-search/1263-23423a3b3/)): feat: introduce multi-language support for 12 snowball stemmer languages  (#1263): 8 of 8 test(s) reached the change
- [valkey-io/valkey-search#1520](https://github.com/valkey-io/valkey-search/pull/1520) ([tests](tests/valkey-search/1520-42d395d2e/)): Fix GCC 12 build and 'local' outside function in CI script (#1520): declined: The change is a build/CI fix with no behaviour change: the CI script edit removes an invalid 'local' outside a function in ci/clang-format-tidy.sh (lint tooling only), and the src/index_schema.cc edit swaps std::format for absl::StrFormat in an error-message string to fix GCC 12 compilatio
- [valkey-io/valkey-search#1507](https://github.com/valkey-io/valkey-search/pull/1507) ([tests](tests/valkey-search/1507-5ec61f4c4/)): Sync INFO SEARCH metrics doc with current App metrics (#1507): declined: no proposed test validated
- [valkey-io/valkey-search#1460](https://github.com/valkey-io/valkey-search/pull/1460) ([tests](tests/valkey-search/1460-c52efbe20/)): feat(query): support HYBRID_POLICY runtime override (#1460): 4 of 4 test(s) reached the change
- [valkey-io/valkey-search#1495](https://github.com/valkey-io/valkey-search/pull/1495) ([tests](tests/valkey-search/1495-ecb06e2d8/)): FT.CREATE and FT.SEARCH documentation updated (#1495): declined: The change edits only src/commands/ft.create.json and ft.search.json, documentation metadata files describing command syntax (FILTER, FLOAT16/BFLOAT16, SCORER, WITHSCORES). These files are not referenced by the CMake build, build.sh, or any source file, so they are not compiled into or loa
- [valkey-io/valkey-search#985](https://github.com/valkey-io/valkey-search/pull/985) ([tests](tests/valkey-search/985-a12bb364f/)): feat: add Vector Range query support to FT.SEARCH (#985): 5 of 5 test(s) reached the change
- [valkey-io/valkey-search#1499](https://github.com/valkey-io/valkey-search/pull/1499) ([tests](tests/valkey-search/1499-ee0a84a4b/)): chore: bump min engine version for 1.3.0 (#1499): declined: The change only bumps the module's kMinimumServerVersion constant from 9.0.1 to 9.1.0 (plus a QUICK_START.md docs edit). That constant's sole use is the load-time gate in vmsdk/src/module.cc, which rejects module load on servers older than the minimum; it is not reflected in any command re
- [valkey-io/valkey-search#1485](https://github.com/valkey-io/valkey-search/pull/1485) ([tests](tests/valkey-search/1485-f04b8651c/)): make dev config mutable (#1485): 4 of 4 test(s) reached the change

## Totals

- Merges whose tests reached the change: 139 of 176 tested (79%)
- Changed-line coverage: median 100% of 60 merge(s) measured, 4 under 50%
- Recall: 33 parents flagged of 42 fix merges
- Claude: $1011.42 in the last 24 hours, $1011.42 in the last 7 days, $1011.42 in all
- Issues filed: 18
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

## Retries

- [valkey-io/valkey#5173](https://github.com/valkey-io/valkey/pull/5173): attempt 0 of 3, the job left no result
