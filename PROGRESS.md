# Fuzzer walk

Updated 2026-11-08T20:29+00:00.

| Repository | Done | Remaining | Backlog cursor | Last 24 hours | Last 7 days (per day) |
|---|---|---|---|---|---|
| valkey-io/valkey | 997 | 1510 | #2658 (2025-09-28) | 240 | 142.4 |
| valkey-io/valkey-bloom | 109 | 0 | ce516d0ef6e3 (2024-04-26) | 0 | 15.6 |
| valkey-io/valkey-json | 62 | 0 | c1423d7e4b38 (2024-11-25) | 0 | 8.9 |
| valkey-io/valkey-search | 655 | 14 | f8106e72e984 (2024-11-27) | 174 | 93.6 |

## Last runs

### valkey-io/valkey

[2026-11-08T19:07:20+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42499012726): 20 merge(s) tested, 11 without a code change.

- [valkey-io/valkey#2498](https://github.com/valkey-io/valkey/pull/2498) ([tests](tests/valkey/2498-af45feea1/)): Remove the outupdated unknown key/value pairs comment in CLUSTER SYNCSLOTS ESTABLISH (#2498): declined: The change only edits a code comment in clusterCommandSyncSlotsEstablish (src/cluster_migrateslots.c), removing an outdated note about skipping unknown key/value pairs. No executable code, replies, parsing, or configuration are modified, so there is no behaviour to exercise or regress.
- [valkey-io/valkey#2734](https://github.com/valkey-io/valkey/pull/2734) ([tests](tests/valkey/2734-54da8344c/)): Fix double MOVED reply on unblock at failover (#2734): 2 of 3 test(s) reached the change, coverage 100%
- [valkey-io/valkey#2721](https://github.com/valkey-io/valkey/pull/2721) ([tests](tests/valkey/2721-28e5dcce2/)): Fix crash that occurs sometimes when aborting a slot migration while child snapshot is active (#2721): 3 of 7 test(s) reached the change, coverage 80%
- [valkey-io/valkey#2729](https://github.com/valkey-io/valkey/pull/2729) ([tests](tests/valkey/2729-181ca483b/)): Bump CLUSTER SHARDS command update version to 9.1.0 (#2729): 2 of 2 test(s) reached the change
- [valkey-io/valkey#747](https://github.com/valkey-io/valkey/pull/747) ([tests](tests/valkey/747-0d500c2cc/)): Update the misleading zdiff() comment on empty first key handling. (#747): declined: The change adds only a code comment above zdiff() in src/t_zset.c explaining the existing empty-first-key no-op behaviour; no executable code, reply, config, or protocol behaviour changes, so there is nothing a scenario could exercise or compare.
- [valkey-io/valkey#2716](https://github.com/valkey-io/valkey/pull/2716) ([tests](tests/valkey/2716-8182f4a0b/)): HSETEX with FXX should not create an object if it does not exist (#2716): 4 of 4 test(s) reached the change, coverage 100%
- [valkey-io/valkey#2710](https://github.com/valkey-io/valkey/pull/2710) ([tests](tests/valkey/2710-f7c95277d/)): Add invalid RDB signature to log statement (#2710): declined: The change only appends the first 9 bytes of the rejected RDB header to an existing LL_WARNING log line in rdbLoadRioWithLoadingCtx (src/rdb.c:3111). The buffer is NUL-terminated at buf[9] and the format is bounded with %.9s, so there is no memory-safety risk; the function still returns C_
- [valkey-io/valkey#2362](https://github.com/valkey-io/valkey/pull/2362) ([tests](tests/valkey/2362-155b0bb82/)): Fix memory leak with CLIENT LIST/KILL duplicate filters (#2362): 0 of 1 test(s) reached the change, coverage 100%
- [valkey-io/valkey#2568](https://github.com/valkey-io/valkey/pull/2568) ([tests](tests/valkey/2568-5bbbc6bd9/)): Add shard id field to CLUSTER SHARDS response (#2568): 2 of 3 test(s) reached the change, coverage 100%
- [valkey-io/valkey#1673](https://github.com/valkey-io/valkey/pull/1673) ([tests](tests/valkey/1673-3390b1e60/)): Allow TCL 9.0 for tests (#1673): declined: The change is purely to the project's own test infrastructure and CI: runtest launcher scripts now accept Tcl 9.0, 'package require Tcl 8.5' is removed from test-support TCL files, the test harness switches fconfigure from -encoding binary to -translation binary, and a Fedora-specific CI a
- [valkey-io/valkey#2688](https://github.com/valkey-io/valkey/pull/2688) ([tests](tests/valkey/2688-a4317e911/)): Introduce SYNCSLOTS CAPA for forwards compatibility (#2688): 2 of 2 test(s) reached the change, coverage 100%
- [valkey-io/valkey#2635](https://github.com/valkey-io/valkey/pull/2635) ([tests](tests/valkey/2635-383ffe487/)): Prevent exposure of importing keys on replicas during atomic slot migration (#2635): 9 of 11 test(s) reached the change, coverage 81%
- [valkey-io/valkey#2679](https://github.com/valkey-io/valkey/pull/2679) ([tests](tests/valkey/2679-064674945/)): Fix format issues with CVE fix (#2679): declined: The change is purely cosmetic: it reformats whitespace and switch-case indentation in luaSetTableProtectionForBasicTypes in src/lua/script_lua.c with byte-for-byte identical semantics (same pushed values, same loop, same calls), and adds an external:skip tag to one TCL test. No server beha
- [valkey-io/valkey@6dd003e88fea](https://github.com/valkey-io/valkey/commit/6dd003e88feace83e55491f32376f6927896e31e) ([tests](tests/valkey/6dd003e88/)): Merge commit from fork: 3 of 3 test(s) reached the change, coverage 100%
- [valkey-io/valkey#2639](https://github.com/valkey-io/valkey/pull/2639) ([tests](tests/valkey/2639-d5bb986fd/)): Add slot migration client flags and module context flags (#2639): 6 of 6 test(s) reached the change, coverage 84%; crash, introduced and since fixed (recorded)
- [valkey-io/valkey#2656](https://github.com/valkey-io/valkey/pull/2656) ([tests](tests/valkey/2656-307397904/)): Defrag if slab 1/8 full to fix defrag didn't stop issue (#2656): 0 of 3 test(s) reached the change, coverage 100%
- [valkey-io/valkey#2646](https://github.com/valkey-io/valkey/pull/2646) ([tests](tests/valkey/2646-a9a65abc8/)): Implement a lolwut for version 9 (#2646): 2 of 2 test(s) reached the change, coverage 100%
- [valkey-io/valkey#2661](https://github.com/valkey-io/valkey/pull/2661) ([tests](tests/valkey/2661-f39a80971/)): Fix module key memory usage accounting (#2661): declined: no proposed test validated
- [valkey-io/valkey#2636](https://github.com/valkey-io/valkey/pull/2636) ([tests](tests/valkey/2636-e99380f75/)): Fix atomic slot migration snapshot never proceeding with hz 1 (#2636): 3 of 3 test(s) reached the change, coverage 100%
- [valkey-io/valkey#2329](https://github.com/valkey-io/valkey/pull/2329) ([tests](tests/valkey/2329-ae58b3d5b/)): Redirect blocked clients after failover (#2329): 6 of 8 test(s) reached the change, coverage 89%

### valkey-io/valkey-bloom

[2026-11-08T19:05:06+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42513162238): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-json

[2026-11-08T20:08:35+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42518538068): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-search

[2026-11-08T20:29:42+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42508756598): 20 merge(s) tested, 0 without a code change.

- [valkey-io/valkey-search#172](https://github.com/valkey-io/valkey-search/pull/172) ([tests](tests/valkey-search/172-0730baaeb/)): Refactored protobuf generation code (cmake) (#172): declined: The change is purely a CMake build refactor: it replaces the generic protobuf_generate() custom-command machinery with configure-time execute_process calls to protoc (plus a timestamp-based skip and extra status messages) in protobuf_generate.cmake and the CMakeLists.txt files that call it
- [valkey-io/valkey-search#144](https://github.com/valkey-io/valkey-search/pull/144) ([tests](tests/valkey-search/144-cd26f4d1d/)): Fix GetRedisLocalPort function to support TLS (#144): declined: no proposed test validated
- [valkey-io/valkey-search#168](https://github.com/valkey-io/valkey-search/pull/168) ([tests](tests/valkey-search/168-8f17a7c72/)): Alpine Linux related fixes (#168): declined: The change only edits submodules/CMakeLists.txt: it detects Alpine Linux at configure time and patches a type in grpc's vendored re2 header (mutable int32_t -> mutable int) so the module compiles under musl. It is a build/CI fix with no change to any command, reply, index, replication, or 
- [valkey-io/valkey-search#166](https://github.com/valkey-io/valkey-search/pull/166) ([tests](tests/valkey-search/166-a835a4639/)): Fixed drop index bug (#166): 2 of 4 test(s) reached the change
- [valkey-io/valkey-search#162](https://github.com/valkey-io/valkey-search/pull/162) ([tests](tests/valkey-search/162-73f2d5302/)): Enable integration tests + ASan (#162): declined: The change modifies only CI and test infrastructure: a new GitHub Actions workflow that runs integration tests under ASan, build.sh plumbing to forward an --asan flag, an LSan suppression file (ci/asan.supp), and a refactor of testing/integration/run.sh (ASan build dir suffix, env-var prin
- [valkey-io/valkey-search#161](https://github.com/valkey-io/valkey-search/pull/161) ([tests](tests/valkey-search/161-4d1dd2539/)): CI updates (#161): declined: The change is purely to CI and build infrastructure: .devcontainer/Dockerfile (adds wget), .gitignore, build.sh (test-output clearing and script exit code), ci/build_ubuntu.sh and submodules/package-submodules.sh (ASan-suffixed dependency .deb packaging and downloading debs from GitHub rel
- [valkey-io/valkey-search#159](https://github.com/valkey-io/valkey-search/pull/159) ([tests](tests/valkey-search/159-1a4e2db86/)): Use a managed pointer for thread safe context in coordinator (#159): declined: no proposed test validated
- [valkey-io/valkey-search#158](https://github.com/valkey-io/valkey-search/pull/158) ([tests](tests/valkey-search/158-80e3d9a41/)): Use differnt ubuntu mirror (#158): declined: The change modifies only .devcontainer/Dockerfile, switching the Ubuntu apt mirror used when building the development container image. No module source, command behaviour, data path, or runtime configuration changes; the built valkey-search module is identical on both sides, so no scenario
- [valkey-io/valkey-search#148](https://github.com/valkey-io/valkey-search/pull/148) ([tests](tests/valkey-search/148-e06cf7a93/)): Small integ tests fixes (#148): declined: The change touches only testing/integration/run.sh, the script that builds dependencies and launches valkey-search's own integration tests in CI: it stops deleting build directories between runs, switches the valkey-server build from make to cmake/ninja with updated binary paths, adds colo
- [valkey-io/valkey-search#154](https://github.com/valkey-io/valkey-search/pull/154) ([tests](tests/valkey-search/154-54b62de78/)): fix compilation failure for release option (#154): declined: The change only replaces a bare static_assert(false) with a dependent-false static_assert inside the never-instantiated else branch of the templated ConfigBuilder Builder() function in vmsdk/src/module_config.h. This is a build fix: the assert exists solely to fail compilation if an unsupp
- [valkey-io/valkey-search#152](https://github.com/valkey-io/valkey-search/pull/152) ([tests](tests/valkey-search/152-45222a225/)): More resilient builds on different distros (#152): declined: The change modifies only submodules/CMakeLists.txt, adding a helper that locates dependency CMake config files under lib/ or lib64/ when setting CMAKE_PREFIX_PATH, so builds succeed on RedHat-based distros where configs land in lib64/. It is purely a build-configuration change: no C++ sour
- [valkey-io/valkey-search#149](https://github.com/valkey-io/valkey-search/pull/149) ([tests](tests/valkey-search/149-ef08ab65e/)): Module-Config: fix a possibility of of a double free (#149): declined: The fix changes ConfigBuilder (vmsdk/src/module_config.h) to hold its ConfigBase in a shared_ptr from construction instead of wrapping a raw pointer in a new shared_ptr inside Build(). The double free it prevents requires Build() to be called twice on one builder (two independent shared_pt
- [valkey-io/valkey-search#146](https://github.com/valkey-io/valkey-search/pull/146) ([tests](tests/valkey-search/146-ca578e70b/)): Dropped the deprecated `--threads` flag (#146): declined: The change removes the deprecated, hidden `--threads` module startup flag and its guard over later reader-threads/writer-threads updates. That behavior is only observable when `--threads` is passed as a module load-time argument: the flag was REDISMODULE_CONFIG_HIDDEN, so on both builds it
- [valkey-io/valkey-search#129](https://github.com/valkey-io/valkey-search/pull/129) ([tests](tests/valkey-search/129-2af78ef31/)): Allow changing the reader/writer threads count at runtime (#129): 5 of 5 test(s) reached the change; crash, predates #129, since fixed (recorded); crash, predates #129, since fixed (recorded)
- [valkey-io/valkey-search#143](https://github.com/valkey-io/valkey-search/pull/143) ([tests](tests/valkey-search/143-37b90f2eb/)): Use background context rather than IO context during Aux save. (#143): 0 of 2 test(s) reached the change; crash, predates #143, since fixed (recorded)
- [valkey-io/valkey-search#85](https://github.com/valkey-io/valkey-search/pull/85) ([tests](tests/valkey-search/85-eec38de7f/)): allow building with external libs (#85): declined: The change is purely a build and CI change: it adds a WITH_SUBMODULES_SYSTEM CMake option, a ci/build_ubuntu.sh script, prebuilt dependency .deb packages, and rewrites cmake modules to locate gRPC/protobuf/abseil/GTest/highwayhash via find_package instead of hardcoded submodule paths, rena
- [valkey-io/valkey-search#82](https://github.com/valkey-io/valkey-search/pull/82) ([tests](tests/valkey-search/82-ffa9e8c13/)): Create placeholder release notes and development versions (#82): 2 of 2 test(s) reached the change; crash, predates #82, since fixed (recorded)
- [valkey-io/valkey-search#84](https://github.com/valkey-io/valkey-search/pull/84) ([tests](tests/valkey-search/84-6de50580c/)): Update printed binary file path (#84): declined: The change only fixes the directory name printed by build.sh after a successful build (.build_${BUILD_CONFIG} -> .build-${BUILD_CONFIG} in two printf messages). It is a build-script cosmetic/documentation fix: no module source, command behaviour, reply, persistence, or replication path is 
- [valkey-io/valkey-search#75](https://github.com/valkey-io/valkey-search/pull/75) ([tests](tests/valkey-search/75-e0765255a/)): Fixed build warning - wrong inculde path (cmake) (#75): declined: The change is a one-line CMake fix in vmsdk/testing/CMakeLists.txt that corrects the include path (a source-list variable mistakenly used as a directory) for the concurrency_test unit-test executable. It is a build/CI-only change affecting how a test binary compiles; no module runtime code
- [valkey-io/valkey-search#80](https://github.com/valkey-io/valkey-search/pull/80): bazel cleanup (#80): error: building valkey-search left no .build-release/libsearch.so

## Totals

- Merges whose tests reached the change: 610 of 887 tested (69%)
- Changed-line coverage: median 100% of 454 merge(s) measured, 36 under 50%
- Recall: 167 parents flagged of 247 fix merges
- Claude: $1400.08 in the last 24 hours, $6431.22 in the last 7 days, $6431.22 in all
- Issues filed: 56
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/100
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/101
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/102
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/103
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/104
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/105
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
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/89
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/90
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/91
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/92
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/93
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/94
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/95
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/96
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/98

## Retries

- [valkey-io/valkey-search#68](https://github.com/valkey-io/valkey-search/pull/68): attempt 0 of 3, requeued: its build failed before the walk's build fix (CMake 4 policy, gRPC header race)
- [valkey-io/valkey-search#77](https://github.com/valkey-io/valkey-search/pull/77): attempt 0 of 3, requeued: its build failed before the walk's build fix (CMake 4 policy, gRPC header race)
- [valkey-io/valkey-search#76](https://github.com/valkey-io/valkey-search/pull/76): attempt 0 of 3, requeued: its build failed before the walk's build fix (CMake 4 policy, gRPC header race)
- [valkey-io/valkey-search#73](https://github.com/valkey-io/valkey-search/pull/73): attempt 0 of 3, requeued: its build failed before the walk's build fix (CMake 4 policy, gRPC header race)
- [valkey-io/valkey-search#60](https://github.com/valkey-io/valkey-search/pull/60): attempt 0 of 3, requeued: its build failed before the walk's build fix (CMake 4 policy, gRPC header race)
- [valkey-io/valkey-search#72](https://github.com/valkey-io/valkey-search/pull/72): attempt 0 of 3, requeued: its build failed before the walk's build fix (CMake 4 policy, gRPC header race)
- [valkey-io/valkey-search#71](https://github.com/valkey-io/valkey-search/pull/71): attempt 0 of 3, requeued: its build failed before the walk's build fix (CMake 4 policy, gRPC header race)
- [valkey-io/valkey-search#70](https://github.com/valkey-io/valkey-search/pull/70): attempt 0 of 3, requeued: its build failed before the walk's build fix (CMake 4 policy, gRPC header race)
- [valkey-io/valkey-search#49](https://github.com/valkey-io/valkey-search/pull/49): attempt 0 of 3, requeued: its build failed before the walk's build fix (CMake 4 policy, gRPC header race)
- [valkey-io/valkey-search@1d8e30e25ae6](https://github.com/valkey-io/valkey-search/commit/1d8e30e25ae640522f809f839ed2a81fb191e5c1): attempt 0 of 3, requeued: its build failed before the walk's build fix (CMake 4 policy, gRPC header race)
- [valkey-io/valkey-search#65](https://github.com/valkey-io/valkey-search/pull/65): attempt 0 of 3, requeued: its build failed before the walk's build fix (CMake 4 policy, gRPC header race)
- [valkey-io/valkey-search#64](https://github.com/valkey-io/valkey-search/pull/64): attempt 0 of 3, requeued: its build failed before the walk's build fix (CMake 4 policy, gRPC header race)
- [valkey-io/valkey-search#63](https://github.com/valkey-io/valkey-search/pull/63): attempt 0 of 3, requeued: its build failed before the walk's build fix (CMake 4 policy, gRPC header race)
- [valkey-io/valkey-search#57](https://github.com/valkey-io/valkey-search/pull/57): attempt 0 of 3, requeued: its build failed before the walk's build fix (CMake 4 policy, gRPC header race)
- [valkey-io/valkey#3800](https://github.com/valkey-io/valkey/pull/3800): attempt 0 of 3, Claude Code failed (rc=1): API Error: Claude Code is unable to respond to this request, which appears to violate our Usage Policy (https://www.anthropic.com/legal/aup). This request triggered restrict
