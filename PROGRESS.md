# Fuzzer walk

Updated 2026-11-08T17:02+00:00.

| Repository | Done | Remaining | Backlog cursor | Last 24 hours | Last 7 days (per day) |
|---|---|---|---|---|---|
| valkey-io/valkey | 966 | 1541 | #2741 (2025-10-16) | 269 | 138.0 |
| valkey-io/valkey-bloom | 109 | 0 | ce516d0ef6e3 (2024-04-26) | 0 | 15.6 |
| valkey-io/valkey-json | 62 | 0 | c1423d7e4b38 (2024-11-25) | 0 | 8.9 |
| valkey-io/valkey-search | 635 | 34 | f8106e72e984 (2024-11-27) | 200 | 90.7 |

## Last runs

### valkey-io/valkey

[2026-11-08T16:16:00+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42481344991): 20 merge(s) tested, 5 without a code change.

- [valkey-io/valkey#3498](https://github.com/valkey-io/valkey/pull/3498) ([tests](tests/valkey/3498-c403eecd5/)): Fix double free in stream consumer PEL loading with corrupt RDB data (#3498): 0 of 1 test(s) reached the change
- [valkey-io/valkey#2787](https://github.com/valkey-io/valkey/pull/2787) ([tests](tests/valkey/2787-189c69e31/)): Fix: ltrim should not call signalModifiedKey when no elements are removed (#2787): 2 of 3 test(s) reached the change, coverage 100%
- [valkey-io/valkey#2785](https://github.com/valkey-io/valkey/pull/2785) ([tests](tests/valkey/2785-43ee46da3/)): Authenticate slot migration client on source node to internal user (#2785): 3 of 3 test(s) reached the change, coverage 100%; check, introduced and since fixed (recorded)
- [valkey-io/valkey#2791](https://github.com/valkey-io/valkey/pull/2791) ([tests](tests/valkey/2791-84eb459cd/)): Add ValkeyModule_ReplyWithCustomErrorFormat to module API (#2791): 3 of 3 test(s) reached the change, coverage 87%
- [valkey-io/valkey#2786](https://github.com/valkey-io/valkey/pull/2786) ([tests](tests/valkey/2786-f54818cc6/)): Bug fix: reset io_last_written on c->buf resize to prevent stale pointers (#2786): declined: Memory-safety fix in clientsCronResizeOutputBuffer: when the client reply buffer c->buf is reallocated by the server cron, a pending partial-write pointer (c->io_last_written.buf) that still referenced the freed old buffer is now repointed to the new buffer, avoiding a stale/dangling point
- [valkey-io/valkey#2782](https://github.com/valkey-io/valkey/pull/2782) ([tests](tests/valkey/2782-864de555c/)): Make ValkeyModule_Call compatible with calling commands from scripting engines (#2782): 5 of 5 test(s) reached the change, coverage 48%
- [valkey-io/valkey#2738](https://github.com/valkey-io/valkey/pull/2738) ([tests](tests/valkey/2738-ea103da5d/)): New INFO section for scripting engines (#2738): 3 of 3 test(s) reached the change, coverage 95%
- [valkey-io/valkey#2776](https://github.com/valkey-io/valkey/pull/2776) ([tests](tests/valkey/2776-f3b2dee3b/)): Add monotonic clock calibration handling if clock speed is not found (#2776): declined: The change is confined to monotonicInit_x86linux() in src/monotonic.c, which is compiled only when USE_PROCESSOR_CLOCK is defined (a non-default build flag set via CFLAGS=-DUSE_PROCESSOR_CLOCK). The fuzzer runs the builds the runner supplies, which use the default POSIX clock_gettime path,
- [valkey-io/valkey#2737](https://github.com/valkey-io/valkey/pull/2737) ([tests](tests/valkey/2737-909d082cd/)): Reorder valkey.conf: move configs to correct sections (#2737): declined: The change only reorders commented-out documentation blocks inside valkey.conf (availability-zone, server-cpulist/bio-cpulist/aof-rewrite-cpulist/bgsave-cpulist, ignore-warnings) into their correct sections. Every moved line begins with '#'; no active config directive or server code change
- [valkey-io/valkey#2763](https://github.com/valkey-io/valkey/pull/2763) ([tests](tests/valkey/2763-861d0794b/)): Sentinel: Skip IS-PRIMARY-DOWN-BY-ADDR requests when primary not SDOWN (#2763): declined: The change is confined to Sentinel-mode code (sentinelAskPrimaryStateToOtherSentinels in src/sentinel.c, the IS-PRIMARY-DOWN-BY-ADDR exchange between sentinels). The fuzzer cannot reach it: deployment modes are standalone and cluster only (Sentinel mode is explicitly unimplemented per docs
- [valkey-io/valkey#1701](https://github.com/valkey-io/valkey/pull/1701) ([tests](tests/valkey/1701-7043c0f54/)): Scripting Engine Debugger Support (#1701): 12 of 16 test(s) reached the change, coverage 80%; check, introduced and since fixed (recorded)
- [valkey-io/valkey#2761](https://github.com/valkey-io/valkey/pull/2761) ([tests](tests/valkey/2761-746d9eca9/)): Use the fetched TLS and TCP ports in clusterProcessGossipSection (#2761): declined: The change only replaces inline ternary expressions in clusterProcessGossipSection's address-update condition with the msg_tls_port/msg_tcp_port variables already computed by getClientPortFromGossip a few lines above. Those variables are defined by exactly the same expressions that were re
- [valkey-io/valkey#2471](https://github.com/valkey-io/valkey/pull/2471) ([tests](tests/valkey/2471-7e0b3bbd5/)): Show RPS histogram in valkey-benchmark (#2471): 1 of 1 test(s) reached the change, coverage 100%
- [valkey-io/valkey#2758](https://github.com/valkey-io/valkey/pull/2758) ([tests](tests/valkey/2758-0e5477fb9/)): Fix atomic slot migration module notification comment (#2758): declined: The change is a pure documentation fix: it deletes two stale lines (source_node_id and target_node_id field descriptions) from a C comment in src/module.c describing the atomic slot migration module notification struct. No executable code, API, data layout, or behaviour changes, so there i
- [valkey-io/valkey#2757](https://github.com/valkey-io/valkey/pull/2757) ([tests](tests/valkey/2757-35b4e2f1a/)): Update module api generation and format module.c (#2757): declined: The change touches only doc comments and whitespace in src/module.c (reflowed example code in comments, 'redis.call()' renamed to 'server.call()' in a comment, and the VM_RegisterCommandFilter signature re-wrapped across lines with no code change) and rewrites utils/generate-module-api-doc
- [valkey-io/valkey#2752](https://github.com/valkey-io/valkey/pull/2752) ([tests](tests/valkey/2752-9acea3684/)): Fix outdated comment around clusterLink->flags (#2752): declined: The change only edits a comment on clusterLink->flags in src/cluster_legacy.h (one line, no code change). It cannot alter behaviour, replies, cluster bus traffic, or persistence, so no scenario is needed.
- [valkey-io/valkey#2753](https://github.com/valkey-io/valkey/pull/2753) ([tests](tests/valkey/2753-1cf0df9fc/)): Fix invalid memory address caused by hashtable shrinking during safe iteration (#2753): 0 of 5 test(s) reached the change, coverage 0%, 5 skipped (required capability 'build' is unsupported: default: build rejects startup configuration: *** FATAL CONFIG FILE ERROR (Version 255.255.255) *** Reading the configuration file, at line 11 >>> 'forkless-infrastructure-enabled "yes"' Bad directive or wrong number of arguments; default deployment config)
- [valkey-io/valkey#2750](https://github.com/valkey-io/valkey/pull/2750) ([tests](tests/valkey/2750-5d3cb3d04/)): Initialize the lua attributes of the luaFunction script (#2750): declined: The change fixes an uninitialized-field bug: in the two FUNCTION-registration paths the luaFunction script was zmalloc'd, so script->lua held garbage, and luaEngineFreeFunction's `if (lua == script->lua) lua_unref(...)` guard then skipped releasing the Lua registry reference when an indivi
- [valkey-io/valkey#2749](https://github.com/valkey-io/valkey/pull/2749) ([tests](tests/valkey/2749-2a914aa52/)): Fix incorrect kvstore size and BIT accounting after completed migration (#2749): 3 of 3 test(s) reached the change, coverage 100%
- [valkey-io/valkey#1826](https://github.com/valkey-io/valkey/pull/1826) ([tests](tests/valkey/1826-b4c93cc9c/)): FUNCTION FLUSH re-create lua VM, fix flush not gc, fix flush async + load crash (#1826): 0 of 12 test(s) reached the change, coverage 90%; crash, fixed by #1826 (recorded); crash, fixed by #1826 (recorded); crash, fixed by #1826 (recorded)

### valkey-io/valkey-bloom

[2026-11-08T17:02:37+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42502889881): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-json

[2026-11-08T16:40:59+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42501081303): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-search

[2026-11-08T16:02:29+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42495120230): 3 merge(s) tested, 0 without a code change.

- [valkey-io/valkey-search@965d40ad2aa1](https://github.com/valkey-io/valkey-search/commit/965d40ad2aa18f99c8f6cac0a89dc73e5fe8d109): Compilation cleanup and sync: error: `export NINJA_TOOL=ninja` failed in merge (exit 2): grep: build.sh: No such file or directory bash: line 12: ./build.sh: No such file or directory ls: cannot access '.build-release/src/coordinator/coordinator.grpc.pb.h': No such file or directory
- [valkey-io/valkey-search@a4c1a046f08b](https://github.com/valkey-io/valkey-search/commit/a4c1a046f08b338bda26388800eb0feaeb6a7028): fixing use after free by the allocator: error: `export NINJA_TOOL=ninja` failed in merge (exit 2): grep: build.sh: No such file or directory bash: line 12: ./build.sh: No such file or directory ls: cannot access '.build-release/src/coordinator/coordinator.grpc.pb.h': No such file or directory
- [valkey-io/valkey-search@9ca99aa8f85a](https://github.com/valkey-io/valkey-search/commit/9ca99aa8f85a5021b0489a071c919db0166a42c4): Changing the module target name: error: `export NINJA_TOOL=ninja` failed in merge (exit 2): grep: build.sh: No such file or directory bash: line 12: ./build.sh: No such file or directory ls: cannot access '.build-release/src/coordinator/coordinator.grpc.pb.h': No such file or directory

## Totals

- Merges whose tests reached the change: 595 of 869 tested (68%)
- Changed-line coverage: median 100% of 441 merge(s) measured, 36 under 50%
- Recall: 163 parents flagged of 241 fix merges
- Claude: $1550.97 in the last 24 hours, $6274.23 in the last 7 days, $6274.23 in all
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

- [valkey-io/valkey-search#172](https://github.com/valkey-io/valkey-search/pull/172): attempt 0 of 3, requeued: its build failed before the walk's build fix (CMake 4 policy, gRPC header race)
- [valkey-io/valkey-search#144](https://github.com/valkey-io/valkey-search/pull/144): attempt 0 of 3, requeued: its build failed before the walk's build fix (CMake 4 policy, gRPC header race)
- [valkey-io/valkey-search#168](https://github.com/valkey-io/valkey-search/pull/168): attempt 0 of 3, requeued: its build failed before the walk's build fix (CMake 4 policy, gRPC header race)
- [valkey-io/valkey-search#166](https://github.com/valkey-io/valkey-search/pull/166): attempt 0 of 3, requeued: its build failed before the walk's build fix (CMake 4 policy, gRPC header race)
- [valkey-io/valkey-search#162](https://github.com/valkey-io/valkey-search/pull/162): attempt 0 of 3, requeued: its build failed before the walk's build fix (CMake 4 policy, gRPC header race)
- [valkey-io/valkey-search#161](https://github.com/valkey-io/valkey-search/pull/161): attempt 0 of 3, requeued: its build failed before the walk's build fix (CMake 4 policy, gRPC header race)
- [valkey-io/valkey-search#159](https://github.com/valkey-io/valkey-search/pull/159): attempt 0 of 3, requeued: its build failed before the walk's build fix (CMake 4 policy, gRPC header race)
- [valkey-io/valkey-search#158](https://github.com/valkey-io/valkey-search/pull/158): attempt 0 of 3, requeued: its build failed before the walk's build fix (CMake 4 policy, gRPC header race)
- [valkey-io/valkey-search#148](https://github.com/valkey-io/valkey-search/pull/148): attempt 0 of 3, requeued: its build failed before the walk's build fix (CMake 4 policy, gRPC header race)
- [valkey-io/valkey-search#154](https://github.com/valkey-io/valkey-search/pull/154): attempt 0 of 3, requeued: its build failed before the walk's build fix (CMake 4 policy, gRPC header race)
- [valkey-io/valkey-search#152](https://github.com/valkey-io/valkey-search/pull/152): attempt 0 of 3, requeued: its build failed before the walk's build fix (CMake 4 policy, gRPC header race)
- [valkey-io/valkey-search#149](https://github.com/valkey-io/valkey-search/pull/149): attempt 0 of 3, requeued: its build failed before the walk's build fix (CMake 4 policy, gRPC header race)
- [valkey-io/valkey-search#146](https://github.com/valkey-io/valkey-search/pull/146): attempt 0 of 3, requeued: its build failed before the walk's build fix (CMake 4 policy, gRPC header race)
- [valkey-io/valkey-search#129](https://github.com/valkey-io/valkey-search/pull/129): attempt 0 of 3, requeued: its build failed before the walk's build fix (CMake 4 policy, gRPC header race)
- [valkey-io/valkey-search#143](https://github.com/valkey-io/valkey-search/pull/143): attempt 0 of 3, requeued: its build failed before the walk's build fix (CMake 4 policy, gRPC header race)
- [valkey-io/valkey-search#85](https://github.com/valkey-io/valkey-search/pull/85): attempt 0 of 3, requeued: its build failed before the walk's build fix (CMake 4 policy, gRPC header race)
- [valkey-io/valkey-search#82](https://github.com/valkey-io/valkey-search/pull/82): attempt 0 of 3, requeued: its build failed before the walk's build fix (CMake 4 policy, gRPC header race)
- [valkey-io/valkey-search#84](https://github.com/valkey-io/valkey-search/pull/84): attempt 0 of 3, requeued: its build failed before the walk's build fix (CMake 4 policy, gRPC header race)
- [valkey-io/valkey-search#75](https://github.com/valkey-io/valkey-search/pull/75): attempt 0 of 3, requeued: its build failed before the walk's build fix (CMake 4 policy, gRPC header race)
- [valkey-io/valkey-search#80](https://github.com/valkey-io/valkey-search/pull/80): attempt 0 of 3, requeued: its build failed before the walk's build fix (CMake 4 policy, gRPC header race)
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
