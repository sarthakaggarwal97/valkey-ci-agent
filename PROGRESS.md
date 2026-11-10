# Fuzzer walk

Updated 2026-11-10T23:46+00:00.

| Repository | Done | Remaining | Backlog cursor | Last 24 hours | Last 7 days (per day) |
|---|---|---|---|---|---|
| valkey-io/valkey | 1610 | 912 | #1209 (2024-11-29) | 287 | 230.0 |
| valkey-io/valkey-bloom | 109 | 0 | ce516d0ef6e3 (2024-04-26) | 0 | 15.6 |
| valkey-io/valkey-json | 64 | 0 | c1423d7e4b38 (2024-11-25) | 1 | 9.1 |
| valkey-io/valkey-search | 672 | 0 | f8106e72e984 (2024-11-27) | 2 | 96.0 |

## Last runs

### valkey-io/valkey

[2026-11-10T22:39:05+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42889123124): 20 merge(s) tested, 6 without a code change.

- [valkey-io/valkey#5338](https://github.com/valkey-io/valkey/pull/5338) ([tests](tests/valkey/5338-9abbfd255/)): Use the effective replica output buffer limit for the steady-state throttle target (#5338): 0 of 3 test(s) reached the change, coverage 100%
- [valkey-io/valkey@3eb8314be6af](https://github.com/valkey-io/valkey/commit/3eb8314be6af0777e69f852b65f933dd9186d30b) ([tests](tests/valkey/3eb8314be/)): Replace dict with hashtable for keys, expires and pubsub channels: 6 of 23 test(s) reached the change, coverage 80%
- [valkey-io/valkey#1324](https://github.com/valkey-io/valkey/pull/1324) ([tests](tests/valkey/1324-9cfe1b3d8/)): Set Command with IFEQ Support (#1324): 3 of 3 test(s) reached the change, coverage 87%
- [valkey-io/valkey#1410](https://github.com/valkey-io/valkey/pull/1410) ([tests](tests/valkey/1410-1ba85d002/)): Use binary representation in assert crash log, cleanup in crash log (#1410): declined: The change is a cleanup/refactor of crash-log formatting in src/debug.c (_serverAssertPrintClientInfo and logCurrentClient), which only executes while the server is already aborting. It changes no command reply, data, replication, or persistence behavior; its only effect is the textual for
- [valkey-io/valkey#1389](https://github.com/valkey-io/valkey/pull/1389) ([tests](tests/valkey/1389-5be4ce6d2/)): Optimize ZRANK to avoid path comparisons (#1389): 0 of 4 test(s) reached the change, coverage 100%
- [valkey-io/valkey#1339](https://github.com/valkey-io/valkey/pull/1339) ([tests](tests/valkey/1339-924729eb1/)): Fix the election was reset wrongly before failover epoch was obtained (#1339): 0 of 3 test(s) reached the change, coverage 100%
- [valkey-io/valkey#1317](https://github.com/valkey-io/valkey/pull/1317) ([tests](tests/valkey/1317-e8078b731/)): Allow MEMORY MALLOC-STATS and MEMORY PURGE during loading phase (#1317): 3 of 3 test(s) reached the change; [check, introduced by #1317](https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/117)
- [valkey-io/valkey#887](https://github.com/valkey-io/valkey/pull/887) ([tests](tests/valkey/887-176fafcaf/)): Add a note to conf about the dangers of modifying dir at runtime (#887): declined: The change touches only valkey.conf comments (a note about modifying 'dir' at runtime) and adds a TCL test that starting the server with --dir given twice works. No source code changes: no command, reply, config handling, persistence or replication behaviour differs between the change and 
- [valkey-io/valkey#1402](https://github.com/valkey-io/valkey/pull/1402) ([tests](tests/valkey/1402-f20d629db/)): Fix sanitizer builds with clang (#1402): declined: The change only moves the #include <stdatomic.h> line in src/unit/test_networking.c (a unit-test source file) so the file compiles under clang sanitizer builds. It is a build fix for test code with no change to server behaviour, commands, replication, persistence or tools, so no fuzzer sce
- [valkey-io/valkey#1395](https://github.com/valkey-io/valkey/pull/1395) ([tests](tests/valkey/1395-6df376d68/)): Fix coredump when use hellodict example module (#1395): declined: The change only edits src/modules/hellodict.c, an example module that is not compiled into valkey-server and is only reachable via an explicit MODULE LOAD of a separately built hellodict.so. The fuzzer's experiments cannot load modules (MODULE LOAD/LOADEX are refused in command sequences, 
- [valkey-io/valkey#1386](https://github.com/valkey-io/valkey/pull/1386) ([tests](tests/valkey/1386-6b3e1228c/)): RDMA: Fix dead loop when transfer large data (20KB) (#1386): declined: The change is confined to src/rdma.c: it implements the already-existing postpone_update_state/update_state connection hooks for the RDMA transport to fix a dead loop on large (~20KB) transfers. That code is compiled only with BUILD_RDMA and executes only for clients connected over an RDMA
- [valkey-io/valkey#1041](https://github.com/valkey-io/valkey/pull/1041) ([tests](tests/valkey/1041-71560a2a4/)): Add API UpdateRuntimeArgs for updating the module arguments during runtime (#1041): declined: The change is a purely additive module API (ValkeyModule_UpdateRuntimeArgs) plus a new test module that is its only caller. No core command, reply, replication, persistence, or cluster path changes: MODULE LIST's arg display and CONFIG REWRITE's loadmodule rewriting are both unchanged and 
- [valkey-io/valkey#1388](https://github.com/valkey-io/valkey/pull/1388) ([tests](tests/valkey/1388-349bc7547/)): defrag: use monotime in module interface (#1388): declined: The change is an internally consistent clock refactor with no behavior the fuzzer can observe. Both the producer (defragLaterItem in defrag.c, which previously converted the monotonic deadline to wall-clock) and the consumer (VM_DefragShouldStop and the ValkeyModuleDefragCtx.endtime field 
- [valkey-io/valkey#1360](https://github.com/valkey-io/valkey/pull/1360) ([tests](tests/valkey/1360-9f8b174c2/)): Optimize IO thread offload for modified argv (#1360): 0 of 6 test(s) reached the change, coverage 95%
- [valkey-io/valkey#1242](https://github.com/valkey-io/valkey/pull/1242) ([tests](tests/valkey/1242-397201c48/)): Refactor of ActiveDefrag to reduce latencies (#1242): 3 of 6 test(s) reached the change, coverage 92%
- [valkey-io/valkey#1293](https://github.com/valkey-io/valkey/pull/1293) ([tests](tests/valkey/1293-3df609ef0/)): Optimize PFCOUNT, PFMERGE command by SIMD acceleration (#1293): 5 of 5 test(s) reached the change, coverage 93%
- [valkey-io/valkey#1188](https://github.com/valkey-io/valkey/pull/1188) ([tests](tests/valkey/1188-fbbfe5d3d/)): Print logs when the cluster state changes to fail or the fail reason changes (#1188): 0 of 7 test(s) reached the change, coverage 91%
- [valkey-io/valkey#905](https://github.com/valkey-io/valkey/pull/905) ([tests](tests/valkey/905-90475af59/)): Free strings during BGSAVE/BGAOFRW to reduce copy-on-write (#905): 0 of 4 test(s) reached the change, coverage 100%
- [valkey-io/valkey#1149](https://github.com/valkey-io/valkey/pull/1149) ([tests](tests/valkey/1149-9c48f5679/)): Reset repl_down_since to zero only on state change (#1149): 0 of 6 test(s) reached the change, coverage 100%
- [valkey-io/valkey#1376](https://github.com/valkey-io/valkey/pull/1376) ([tests](tests/valkey/1376-c8ceb2ee2/)): Use zfree_with_size for client buffer (#1376): 0 of 4 test(s) reached the change, coverage 100%

### valkey-io/valkey-bloom

[2026-11-10T23:32:17+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42924102443): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-json

[2026-11-10T23:46:58+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42926071978): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-search

[2026-11-10T23:16:39+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42921872891): 0 merge(s) tested, 0 without a code change.


## Totals

- Merges whose tests reached the change: 765 of 1197 tested (64%)
- Changed-line coverage: median 100% of 725 merge(s) measured, 57 under 50%
- Recall: 204 parents flagged of 324 fix merges
- Claude: $926.21 in the last 24 hours, $8723.60 in the last 7 days, $8723.60 in all
- Issues filed: 67
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/100
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/101
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/102
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/103
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/104
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/105
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/106
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/107
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/108
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/109
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/110
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/111
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/112
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/113
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/114
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/115
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/117
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

- [valkey-io/valkey#1948](https://github.com/valkey-io/valkey/pull/1948): attempt 0 of 3, Claude Code failed (rc=1): API Error: Claude Code is unable to respond to this request, which appears to violate our Usage Policy (https://www.anthropic.com/legal/aup). This request triggered restrict
- [valkey-io/valkey#1535](https://github.com/valkey-io/valkey/pull/1535): attempt 0 of 3, Claude Code failed (rc=1): API Error: Claude Code is unable to respond to this request, which appears to violate our Usage Policy (https://www.anthropic.com/legal/aup). This request triggered restrict
