# Fuzzer walk

Updated 2026-11-05T11:53+00:00.

| Repository | Done | Remaining | Backlog cursor | Last 24 hours | Last 7 days (per day) |
|---|---|---|---|---|---|
| valkey-io/valkey | 85 | 2405 | #5186 (2026-10-30) | 85 | 12.1 |
| valkey-io/valkey-bloom | 69 | 40 | 85e20fd38641 (2024-09-10) | 69 | 9.9 |
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

[2026-11-05T10:29:17+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42006438529): 20 merge(s) tested, 1 without a code change.

- [valkey-io/valkey-bloom#49](https://github.com/valkey-io/valkey-bloom/pull/49): to retry, `cargo build --release` failed in parent (exit 101): Compiling heck v0.5.0 Compiling rustc-demangle v0.1.28 Compiling strum_macros v0.26.4 Compiling valkey-module-macros-internals v0.1.15 Compiling ni
- [valkey-io/valkey-bloom#48](https://github.com/valkey-io/valkey-bloom/pull/48): to retry, `cargo build --release` failed in merge (exit 101): Compiling lazy_static v1.5.1 Compiling pin-utils v0.1.1 Compiling nix v0.26.4 Compiling valkey-module-macros-internals v0.1.15 Compiling serde_syn v
- [valkey-io/valkey-bloom#26](https://github.com/valkey-io/valkey-bloom/pull/26) ([tests](tests/valkey-bloom/26-f10a4e551/)): Add tightening_ratio support in BloomFilterType structure (#26): 4 of 4 test(s) reached the change; check, introduced and since fixed (recorded)
- [valkey-io/valkey-bloom#27](https://github.com/valkey-io/valkey-bloom/pull/27) ([tests](tests/valkey-bloom/27-1b0d819a4/)): Support random seed per bloom object by default (configurable) (#27): 3 of 3 test(s) reached the change; check, introduced and since fixed (recorded)
- [valkey-io/valkey-bloom#23](https://github.com/valkey-io/valkey-bloom/pull/23) ([tests](tests/valkey-bloom/23-34d6fb086/)): Updating how we create BloomFilter from rdb loads and upgrading bloomfilter dependency to version 3.0 (#23): 6 of 6 test(s) reached the change
- [valkey-io/valkey-bloom#21](https://github.com/valkey-io/valkey-bloom/pull/21) ([tests](tests/valkey-bloom/21-7c2546803/)): Support for DEBUG DIGEST module data type callback (#21): 4 of 4 test(s) reached the change; crash, predates #21, since fixed (recorded)
- [valkey-io/valkey-bloom#20](https://github.com/valkey-io/valkey-bloom/pull/20) ([tests](tests/valkey-bloom/20-a33e0e3d6/)): Add new metrics to show capacity and items across objects (#20): 3 of 3 test(s) reached the change
- [valkey-io/valkey-bloom#17](https://github.com/valkey-io/valkey-bloom/pull/17) ([tests](tests/valkey-bloom/17-b301baa73/)): add bf.load to support bloom filter aofrewrite. (#17): 2 of 4 test(s) reached the change; crash, fixed by #17 (recorded); crash, fixed by #17 (recorded)
- [valkey-io/valkey-bloom#19](https://github.com/valkey-io/valkey-bloom/pull/19) ([tests](tests/valkey-bloom/19-31d21c0ab/)): Adding info handler, that contains three fields, num_objects, num_filters and memory_bytes. Additionally updated drop and created tests around this new info handler (#19): 7 of 7 test(s) reached the change
- [valkey-io/valkey-bloom#18](https://github.com/valkey-io/valkey-bloom/pull/18) ([tests](tests/valkey-bloom/18-12031b237/)): Handle bloom object max allowed size limit, switch fp rate to f64 (#18): 5 of 5 test(s) reached the change
- [valkey-io/valkey-bloom#16](https://github.com/valkey-io/valkey-bloom/pull/16) ([tests](tests/valkey-bloom/16-57a79012f/)): Add integration tests for maxmemory scenarios and replication correctness. Fixed others tests and updated the build.sh script for running single integration tests (#16): declined: The change is tests and build tooling only. The sole source modification (src/bloom/command_handler.rs) rewrites three match arms from Ok(_)/Ok(_v) to Ok(()), a semantically identical pattern-match style change on a Result<(), E> with no behavioral effect. Everything else adds pytest integ
- [valkey-io/valkey-bloom#15](https://github.com/valkey-io/valkey-bloom/pull/15) ([tests](tests/valkey-bloom/15-f5aa39e60/)): Add Integration Testing for correctness of scaling and non scaling filters and other core functionality validation (#15): declined: The change is entirely test code: the only src/ edit is inside the #[cfg-style] `mod tests` block of src/bloom/utils.rs (refactoring unit-test helpers like item_exists_test -> check_items_exist and fixing index bookkeeping in them), and the rest of the diff adds or renames Python integrati
- [valkey-io/valkey-bloom#14](https://github.com/valkey-io/valkey-bloom/pull/14) ([tests](tests/valkey-bloom/14-fde4d3726/)): Updating _ to -in lib.rs. Also updating loading from rdb to use a trait instead (#14): declined: no proposed test validated
- [valkey-io/valkey-bloom#7](https://github.com/valkey-io/valkey-bloom/pull/7) ([tests](tests/valkey-bloom/7-4d386495d/)): Integration tests for commands, delete operations, expiry, compatibility (#7): 2 of 2 test(s) reached the change
- [valkey-io/valkey-bloom@687bec76fa70](https://github.com/valkey-io/valkey-bloom/commit/687bec76fa70cecb8f2c4916f4cffc68b20c81e5) ([tests](tests/valkey-bloom/687bec76f/)): Update build.sh and fix import in save/restore pytest: declined: The change modifies only build.sh (build-script environment-variable handling, defaulting SERVER_VERSION to 'unstable' instead of failing) and adds a missing test-fixture import to tests/test_save_and_restore.py. No module source code, command behaviour, data format, or replication path ch
- [valkey-io/valkey-bloom#2](https://github.com/valkey-io/valkey-bloom/pull/2) ([tests](tests/valkey-bloom/2-282599d21/)): RDB format optimization: Using a fixed seed for bloom filters (#2): 4 of 4 test(s) reached the change; check, fixed by #2 (recorded); check, not reproduced (recorded); check, not reproduced (recorded)
- [valkey-io/valkey-bloom@145f9b285eb3](https://github.com/valkey-io/valkey-bloom/commit/145f9b285eb3e61284872fbf30b4dd3cb290072e) ([tests](tests/valkey-bloom/145f9b285/)): Merge pull request #5 from zackcam/unstable: declined: The change is entirely build/CI/test infrastructure: it adds a GitHub Actions workflow, reworks build.sh and the README to use SERVER_VERSION (dropping 7.2.x for 8.0.0), and refactors the Python integration-test harness to pass a server path instead of a version enum (plus a typo fix). No 
- [valkey-io/valkey-bloom@6d0a4b60c284](https://github.com/valkey-io/valkey-bloom/commit/6d0a4b60c284922db0633b7efe5c8006c07b9cee) ([tests](tests/valkey-bloom/6d0a4b60c/)): Merge pull request #3 from KarthikSubbarao/unstable: 2 of 2 test(s) reached the change
- [valkey-io/valkey-bloom@a180f42678d4](https://github.com/valkey-io/valkey-bloom/commit/a180f42678d48a479582de5c5b703f9bfe8646b0) ([tests](tests/valkey-bloom/a180f4267/)): Add unit testing support using the valkeymodule-rs enable-system-alloc feature: declined: This is a build/CI-only change: it edits Cargo.toml metadata, bumps the valkey-module SDK from 0.1.1 to 0.1.2, adds an optional enable-system-alloc cargo feature, and re-enables `cargo test --features enable-system-alloc` in build.sh. The enable-system-alloc feature exists solely so unit t
- [valkey-io/valkey-bloom@0b9a90dccef5](https://github.com/valkey-io/valkey-bloom/commit/0b9a90dccef5b55651cd5333f132299e8212cc67) ([tests](tests/valkey-bloom/0b9a90dcc/)): Types, Ranges, limit updates and overflow handling: 3 of 4 test(s) reached the change; crash, fixed by 0b9a90dccef5 (recorded)

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

- Merges whose tests reached the change: 115 of 143 tested (80%)
- Changed-line coverage: median 100% of 47 merge(s) measured, 3 under 50%
- Recall: 28 parents flagged of 35 fix merges
- Claude: $857.06 in the last 24 hours, $857.06 in the last 7 days, $857.06 in all
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

- [valkey-io/valkey-bloom#49](https://github.com/valkey-io/valkey-bloom/pull/49): attempt 0 of 3, `cargo build --release` failed in parent (exit 101): Compiling heck v0.5.0 Compiling rustc-demangle v0.1.28 Compiling strum_macros v0.26.4 Compiling valkey-module-macros-internals v0.1.15 Compiling ni
- [valkey-io/valkey-bloom#48](https://github.com/valkey-io/valkey-bloom/pull/48): attempt 0 of 3, `cargo build --release` failed in merge (exit 101): Compiling lazy_static v1.5.1 Compiling pin-utils v0.1.1 Compiling nix v0.26.4 Compiling valkey-module-macros-internals v0.1.15 Compiling serde_syn v
