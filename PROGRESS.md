# Fuzzer walk

Updated 2026-11-05T11:55+00:00.

| Repository | Done | Remaining | Backlog cursor | Last 24 hours | Last 7 days (per day) |
|---|---|---|---|---|---|
| valkey-io/valkey | 85 | 2405 | #5186 (2026-10-30) | 85 | 12.1 |
| valkey-io/valkey-bloom | 76 | 33 | 053d5e9ade3b (2024-09-10) | 76 | 10.9 |
| valkey-io/valkey-json | 62 | 0 | c1423d7e4b38 (2024-11-25) | 62 | 8.9 |
| valkey-io/valkey-search | 52 | 616 | #1464 (2026-10-01) | 52 | 7.4 |

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

[2026-11-05T11:55:40+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42011110824): 20 merge(s) tested, 0 without a code change.

- [valkey-io/valkey-bloom#49](https://github.com/valkey-io/valkey-bloom/pull/49): to retry, `cargo build --release` failed in parent (exit 101): Compiling lazy_static v1.5.1 Compiling heck v0.5.0 Compiling strum_macros v0.26.4 Compiling valkey-module-macros-internals v0.1.15 Compiling nix v0
- [valkey-io/valkey-bloom#48](https://github.com/valkey-io/valkey-bloom/pull/48): to retry, `cargo build --release` failed in merge (exit 101): Compiling lazy_static v1.5.1 Compiling bitflags v1.3.2 Compiling valkey-module-macros-internals v0.1.15 Compiling nix v0.26.4 Compiling serde_syn v0
- [valkey-io/valkey-bloom@85e20fd38641](https://github.com/valkey-io/valkey-bloom/commit/85e20fd3864140ad8f46c9ed3be37ca56e4c62f6) ([tests](tests/valkey-bloom/85e20fd38/)): Add Python testing framework to support Integration testing of the bloom module + Add basic sanity tests: declined: The change adds only a Python integration-testing framework and CI/build tooling (build.sh, requirements.txt, pytest test files, .gitignore and README edits). No module source code changes, so the bloom module's runtime behaviour is identical at the change and its parent; there is no behav
- [valkey-io/valkey-bloom@0d2ad8a48fc3](https://github.com/valkey-io/valkey-bloom/commit/0d2ad8a48fc36097f9c9854572176340d0eaaddf) ([tests](tests/valkey-bloom/0d2ad8a48/)): Support keyspace notifications for write operations: 4 of 4 test(s) reached the change
- [valkey-io/valkey-bloom@c66243d12c58](https://github.com/valkey-io/valkey-bloom/commit/c66243d12c58823390bf3528d2fed3efe8dad339) ([tests](tests/valkey-bloom/c66243d12/)): Update data type name and use static str for errors: 3 of 3 test(s) reached the change
- [valkey-io/valkey-bloom@5b0df156a336](https://github.com/valkey-io/valkey-bloom/commit/5b0df156a3367e118f4ba5839649f205377b2001) ([tests](tests/valkey-bloom/5b0df156a/)): Replication support + Update Module/Datatype name + Refactor: 4 of 4 test(s) reached the change
- [valkey-io/valkey-bloom@ffb722f5da6d](https://github.com/valkey-io/valkey-bloom/commit/ffb722f5da6d456c5256f418cee48cf73bd07114) ([tests](tests/valkey-bloom/ffb722f5d/)): Minor Refactoring + Handle Non Scaling Filters which are filled to capacity: 5 of 5 test(s) reached the change; check, fixed by ffb722f5da6d (recorded)
- [valkey-io/valkey-bloom@3c285d5595ae](https://github.com/valkey-io/valkey-bloom/commit/3c285d5595aeaa12ce34e5e89a22c0553f16f30b) ([tests](tests/valkey-bloom/3c285d559/)): Defrag Bloom Object callback: declined: no proposed test validated
- [valkey-io/valkey-bloom@28164741debf](https://github.com/valkey-io/valkey-bloom/commit/28164741debfa76aca09451bc923ca19873a3039) ([tests](tests/valkey-bloom/28164741d/)): Implement free_effort datatype callback, Make aux_load cb ignore invocations, update documentation: 0 of 4 test(s) reached the change
- [valkey-io/valkey-bloom@ec72262033e7](https://github.com/valkey-io/valkey-bloom/commit/ec72262033e76d83783573a8ee17fd38794e7b48): to retry, `cargo build --release` failed in parent (exit 101): Compiling num-traits v0.1.43 Compiling object v0.40.0 Compiling bitflags v1.3.2 Compiling lazy_static v1.5.1 Compiling heck v0.4.1 Compiling rustc-
- [valkey-io/valkey-bloom@3ebb15b1df2d](https://github.com/valkey-io/valkey-bloom/commit/3ebb15b1df2d3d747d2c0de7ffd0bd1c8b5db08f): to retry, `cargo build --release` failed in merge (exit 101): Compiling num-traits v0.1.43 Compiling object v0.40.0 Compiling heck v0.4.1 Compiling bitflags v1.3.2 Compiling rustc-demangle v0.1.28 Compiling laz
- [valkey-io/valkey-bloom@b7504e965e06](https://github.com/valkey-io/valkey-bloom/commit/b7504e965e06f4b87f024b12fa17da9d96f3fbf6): to retry, `cargo build --release` failed in merge (exit 101): Compiling num-traits v0.1.43 Compiling object v0.40.0 Compiling bitflags v1.3.2 Compiling heck v0.4.1 Compiling rustc-demangle v0.1.28 Compiling laz
- [valkey-io/valkey-bloom@6ca68c5eef40](https://github.com/valkey-io/valkey-bloom/commit/6ca68c5eef40deb0a6ccd2a88d62372bec428b01): to retry, `cargo build --release` failed in merge (exit 101): Compiling num-traits v0.1.43 Compiling object v0.40.0 Compiling heck v0.4.1 Compiling lazy_static v1.5.1 Compiling bitflags v1.3.2 Compiling rustc-d
- [valkey-io/valkey-bloom@ef4e21538d29](https://github.com/valkey-io/valkey-bloom/commit/ef4e21538d29c02d506a2c708f65ce1f0ca08754): to retry, `cargo build --release` failed in merge (exit 101): Compiling num-traits v0.1.43 Compiling object v0.40.0 Compiling lazy_static v1.5.1 Compiling rustc-demangle v0.1.28 Compiling bitflags v1.3.2 Compil
- [valkey-io/valkey-bloom@bebf72ac79df](https://github.com/valkey-io/valkey-bloom/commit/bebf72ac79dfc8996dfbedad2a99ebcb008240c5): to retry, `cargo build --release` failed in merge (exit 101): Compiling num-traits v0.1.43 Compiling object v0.40.0 Compiling lazy_static v1.5.1 Compiling rustc-demangle v0.1.28 Compiling bitflags v1.3.2 Compil
- [valkey-io/valkey-bloom@159c9385b0ac](https://github.com/valkey-io/valkey-bloom/commit/159c9385b0acbcb142bbf156e065b54cfed48f6f): to retry, `cargo build --release` failed in merge (exit 101): Compiling num-traits v0.1.43 Compiling object v0.40.0 Compiling heck v0.4.1 Compiling lazy_static v1.5.1 Compiling rustc-demangle v0.1.28 Compiling 
- [valkey-io/valkey-bloom@3f85cae12cec](https://github.com/valkey-io/valkey-bloom/commit/3f85cae12ceca4441475f189677208a49a17c7cf): to retry, `cargo build --release` failed in merge (exit 101): Compiling num-traits v0.1.43 Compiling object v0.40.0 Compiling lazy_static v1.5.1 Compiling heck v0.4.1 Compiling bitflags v1.3.2 Compiling rustc-d
- [valkey-io/valkey-bloom@753575a84166](https://github.com/valkey-io/valkey-bloom/commit/753575a84166b399c691ffb7385d09f171ed4fc7): to retry, `cargo build --release` failed in merge (exit 101): Compiling num-traits v0.1.43 Compiling object v0.40.0 Compiling rustc-demangle v0.1.28 Compiling heck v0.4.1 Compiling bitflags v1.3.2 Compiling laz
- [valkey-io/valkey-bloom@a2343b744303](https://github.com/valkey-io/valkey-bloom/commit/a2343b744303d5323d17d35b0a592b2d5ef50784): to retry, `cargo build --release` failed in merge (exit 101): Compiling num-traits v0.1.43 Compiling object v0.40.0 Compiling heck v0.4.1 Compiling bitflags v1.3.2 Compiling rustc-demangle v0.1.28 Compiling laz
- [valkey-io/valkey-bloom@fbdfb5bf7906](https://github.com/valkey-io/valkey-bloom/commit/fbdfb5bf790644a1c2d49b664efccb953a17f3e6): to retry, `cargo build --release` failed in merge (exit 101): Compiling num-traits v0.1.43 Compiling object v0.40.0 Compiling rustc-demangle v0.1.28 Compiling lazy_static v1.5.1 Compiling heck v0.4.1 Compiling 

### valkey-io/valkey-json

[2026-11-05T11:53:52+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42022577126): 0 merge(s) tested, 0 without a code change.


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

- Merges whose tests reached the change: 119 of 148 tested (80%)
- Changed-line coverage: median 100% of 47 merge(s) measured, 3 under 50%
- Recall: 28 parents flagged of 35 fix merges
- Claude: $886.77 in the last 24 hours, $886.77 in the last 7 days, $886.77 in all
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

- [valkey-io/valkey-bloom#49](https://github.com/valkey-io/valkey-bloom/pull/49): attempt 1 of 3, `cargo build --release` failed in parent (exit 101): Compiling lazy_static v1.5.1 Compiling heck v0.5.0 Compiling strum_macros v0.26.4 Compiling valkey-module-macros-internals v0.1.15 Compiling nix v0
- [valkey-io/valkey-bloom#48](https://github.com/valkey-io/valkey-bloom/pull/48): attempt 1 of 3, `cargo build --release` failed in merge (exit 101): Compiling lazy_static v1.5.1 Compiling bitflags v1.3.2 Compiling valkey-module-macros-internals v0.1.15 Compiling nix v0.26.4 Compiling serde_syn v0
- [valkey-io/valkey-bloom@ec72262033e7](https://github.com/valkey-io/valkey-bloom/commit/ec72262033e76d83783573a8ee17fd38794e7b48): attempt 1 of 3, `cargo build --release` failed in parent (exit 101): Compiling num-traits v0.1.43 Compiling object v0.40.0 Compiling bitflags v1.3.2 Compiling lazy_static v1.5.1 Compiling heck v0.4.1 Compiling rustc-
- [valkey-io/valkey-bloom@3ebb15b1df2d](https://github.com/valkey-io/valkey-bloom/commit/3ebb15b1df2d3d747d2c0de7ffd0bd1c8b5db08f): attempt 1 of 3, `cargo build --release` failed in merge (exit 101): Compiling num-traits v0.1.43 Compiling object v0.40.0 Compiling heck v0.4.1 Compiling bitflags v1.3.2 Compiling rustc-demangle v0.1.28 Compiling laz
- [valkey-io/valkey-bloom@b7504e965e06](https://github.com/valkey-io/valkey-bloom/commit/b7504e965e06f4b87f024b12fa17da9d96f3fbf6): attempt 1 of 3, `cargo build --release` failed in merge (exit 101): Compiling num-traits v0.1.43 Compiling object v0.40.0 Compiling bitflags v1.3.2 Compiling heck v0.4.1 Compiling rustc-demangle v0.1.28 Compiling laz
- [valkey-io/valkey-bloom@6ca68c5eef40](https://github.com/valkey-io/valkey-bloom/commit/6ca68c5eef40deb0a6ccd2a88d62372bec428b01): attempt 1 of 3, `cargo build --release` failed in merge (exit 101): Compiling num-traits v0.1.43 Compiling object v0.40.0 Compiling heck v0.4.1 Compiling lazy_static v1.5.1 Compiling bitflags v1.3.2 Compiling rustc-d
- [valkey-io/valkey-bloom@ef4e21538d29](https://github.com/valkey-io/valkey-bloom/commit/ef4e21538d29c02d506a2c708f65ce1f0ca08754): attempt 1 of 3, `cargo build --release` failed in merge (exit 101): Compiling num-traits v0.1.43 Compiling object v0.40.0 Compiling lazy_static v1.5.1 Compiling rustc-demangle v0.1.28 Compiling bitflags v1.3.2 Compil
- [valkey-io/valkey-bloom@bebf72ac79df](https://github.com/valkey-io/valkey-bloom/commit/bebf72ac79dfc8996dfbedad2a99ebcb008240c5): attempt 1 of 3, `cargo build --release` failed in merge (exit 101): Compiling num-traits v0.1.43 Compiling object v0.40.0 Compiling lazy_static v1.5.1 Compiling rustc-demangle v0.1.28 Compiling bitflags v1.3.2 Compil
- [valkey-io/valkey-bloom@159c9385b0ac](https://github.com/valkey-io/valkey-bloom/commit/159c9385b0acbcb142bbf156e065b54cfed48f6f): attempt 1 of 3, `cargo build --release` failed in merge (exit 101): Compiling num-traits v0.1.43 Compiling object v0.40.0 Compiling heck v0.4.1 Compiling lazy_static v1.5.1 Compiling rustc-demangle v0.1.28 Compiling 
- [valkey-io/valkey-bloom@3f85cae12cec](https://github.com/valkey-io/valkey-bloom/commit/3f85cae12ceca4441475f189677208a49a17c7cf): attempt 1 of 3, `cargo build --release` failed in merge (exit 101): Compiling num-traits v0.1.43 Compiling object v0.40.0 Compiling lazy_static v1.5.1 Compiling heck v0.4.1 Compiling bitflags v1.3.2 Compiling rustc-d
- [valkey-io/valkey-bloom@753575a84166](https://github.com/valkey-io/valkey-bloom/commit/753575a84166b399c691ffb7385d09f171ed4fc7): attempt 1 of 3, `cargo build --release` failed in merge (exit 101): Compiling num-traits v0.1.43 Compiling object v0.40.0 Compiling rustc-demangle v0.1.28 Compiling heck v0.4.1 Compiling bitflags v1.3.2 Compiling laz
- [valkey-io/valkey-bloom@a2343b744303](https://github.com/valkey-io/valkey-bloom/commit/a2343b744303d5323d17d35b0a592b2d5ef50784): attempt 1 of 3, `cargo build --release` failed in merge (exit 101): Compiling num-traits v0.1.43 Compiling object v0.40.0 Compiling heck v0.4.1 Compiling bitflags v1.3.2 Compiling rustc-demangle v0.1.28 Compiling laz
- [valkey-io/valkey-bloom@fbdfb5bf7906](https://github.com/valkey-io/valkey-bloom/commit/fbdfb5bf790644a1c2d49b664efccb953a17f3e6): attempt 1 of 3, `cargo build --release` failed in merge (exit 101): Compiling num-traits v0.1.43 Compiling object v0.40.0 Compiling rustc-demangle v0.1.28 Compiling lazy_static v1.5.1 Compiling heck v0.4.1 Compiling 
