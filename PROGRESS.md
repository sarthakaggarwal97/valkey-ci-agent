# Fuzzer walk

Updated 2026-11-11T05:28+00:00.

| Repository | Done | Remaining | Backlog cursor | Last 24 hours | Last 7 days (per day) |
|---|---|---|---|---|---|
| valkey-io/valkey | 1677 | 845 | #1239 (2024-10-30) | 256 | 239.6 |
| valkey-io/valkey-bloom | 109 | 0 | ce516d0ef6e3 (2024-04-26) | 0 | 15.6 |
| valkey-io/valkey-json | 64 | 0 | c1423d7e4b38 (2024-11-25) | 1 | 9.1 |
| valkey-io/valkey-search | 672 | 0 | f8106e72e984 (2024-11-27) | 1 | 96.0 |

## Last runs

### valkey-io/valkey

[2026-11-11T04:12:30+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42944944932): 20 merge(s) tested, 3 without a code change.

- [valkey-io/valkey#1948](https://github.com/valkey-io/valkey/pull/1948) ([tests](tests/valkey/1948-ee9a305b5/)): Fix panic in primary when blocking shutdown after previous block with timeout (#1948): 1 of 1 test(s) reached the change, coverage 100%; [crash, introduced by #1948](https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/122); crash, fixed by #1948 (recorded)
- [valkey-io/valkey#1009](https://github.com/valkey-io/valkey/pull/1009) ([tests](tests/valkey/1009-a2d22c63c/)): Fix replica not able to initate election in time when epoch fails (#1009): 0 of 6 test(s) reached the change, coverage 100%
- [valkey-io/valkey#1081](https://github.com/valkey-io/valkey/pull/1081) ([tests](tests/valkey/1081-167e8ab8d/)): Trigger the election immediately when doing a manual failover (#1081): 0 of 3 test(s) reached the change, coverage 100%
- [valkey-io/valkey#1279](https://github.com/valkey-io/valkey/pull/1279) ([tests](tests/valkey/1279-9300a7ebc/)): Set fields to NULL after free in freeClient() (#1279): 0 of 3 test(s) reached the change, coverage 100%
- [valkey-io/valkey#1282](https://github.com/valkey-io/valkey/pull/1282) ([tests](tests/valkey/1282-0b5b2c748/)): Log as primary role (M) instead of child process (C) during startup (#1282): declined: The change only moves the server.pid = getpid() assignment from initServer() to main() so that startup log lines show the primary role character 'M' instead of the child character 'C'. In a foreground (non-daemonized) server, which is the only way the fuzzer can run instances, server.pid r
- [valkey-io/valkey#1250](https://github.com/valkey-io/valkey/pull/1250) ([tests](tests/valkey/1250-45d596e12/)): RDMA: Use conn ref counter to prevent double close (#1250): declined: The change is confined to src/rdma.c, the RDMA connection type's close/handler path. RDMA is not compiled into the runner's builds (BUILD_RDMA defaults to no and is module-only), loading it would need MODULE LOAD (refused) plus librdmacm, and exercising it needs RDMA hardware or a root-con
- [valkey-io/valkey#1272](https://github.com/valkey-io/valkey/pull/1272) ([tests](tests/valkey/1272-e972d5646/)): Make sure to copy null terminator byte in dual channel code (#1272): 0 of 3 test(s) reached the change, coverage 100%
- [valkey-io/valkey#1196](https://github.com/valkey-io/valkey/pull/1196) ([tests](tests/valkey/1196-07b3e7ae7/)): Add CMake build system for valkey (#1196): declined: The change only adds a CMake build system: new CMakeLists.txt files, cmake modules, a cmake-format config, CI workflow and README/gitignore updates. The sole source-file hunk (src/server.c) removes one blank line before a comment, which cannot alter behavior. No command, reply, replication
- [valkey-io/valkey#1265](https://github.com/valkey-io/valkey/pull/1265) ([tests](tests/valkey/1265-3672f9b2c/)): Revert "Decline unsubscribe related command in non-subscribed mode" (#1265): declined: no proposed test validated
- [valkey-io/valkey#1255](https://github.com/valkey-io/valkey/pull/1255) ([tests](tests/valkey/1255-1c18c8084/)): Fix incorrect cache_memory reset in functionsLibCtxClear (#1255): 3 of 5 test(s) reached the change, coverage 100%
- [valkey-io/valkey#1258](https://github.com/valkey-io/valkey/pull/1258) ([tests](tests/valkey/1258-a0b1cbad8/)): Change errno from EEXIST to EALREADY in serverFork if child process exists (#1258): declined: The change only swaps errno from EEXIST to EALREADY in the serverFork branch that refuses a mutually-exclusive fork while a child is active. All in-tree command paths (BGSAVE, BGREWRITEAOF, replica full-sync) check hasActiveChildProcess() and bail out before calling serverFork, and the ser
- [valkey-io/valkey#1259](https://github.com/valkey-io/valkey/pull/1259) ([tests](tests/valkey/1259-12c5af03b/)): Remove empty DB check branch in KEYS command (#1259): 0 of 2 test(s) reached the change
- [valkey-io/valkey#1264](https://github.com/valkey-io/valkey/pull/1264) ([tests](tests/valkey/1264-48ebe21ad/)): fix: clean up refactoring leftovers (#1264): declined: The change only deletes three redundant server.repl_state assignments inside the dual-channel handshake helpers in replication.c. The caller, dualChannelSetupMainConnForPsync, already sets the identical state value after each helper returns C_OK (unchanged context lines in the same diff hu
- [valkey-io/valkey#1252](https://github.com/valkey-io/valkey/pull/1252) ([tests](tests/valkey/1252-3c32ee1bd/)): Add a filter option to drop all cluster packets (#1252): 0 of 3 test(s) reached the change, coverage 100%, 1 skipped (the run could not start: runner execution failed during deployment.provision: LocalDeploymentError: Instance valkey-0 failed to start within 60s (process output from /home/runner/work/valkey-ci-agent/valkey-ci-agent/walk-work/runs/ai-d5812612ef67-merge-7101/artifacts/data/logs/7101-dae728eb-valkey-0)
- [valkey-io/valkey#1246](https://github.com/valkey-io/valkey/pull/1246) ([tests](tests/valkey/1246-0d7b2344b/)): correct type internal to kvstore (minor) (#1246): declined: The change only narrows two private struct fields in src/kvstore.c (num_dicts, num_dicts_bits) from long long to int. Their values are capped by assert(num_dicts_bits <= 16) in kvstoreCreate, so num_dicts <= 65536 always fits an int, and the public accessor kvstoreNumDicts already returned
- [valkey-io/valkey#1244](https://github.com/valkey-io/valkey/pull/1244) ([tests](tests/valkey/1244-e985ead7f/)): RDMA: Prevent IO for child process (#1244): declined: The change is confined to src/rdma.c, the RDMA transport, adding a guard that stops forked child processes (BGSAVE/AOF-rewrite children) from issuing RDMA IO on memory regions that are invalid in the child. This code is unreachable in the fuzzer's environment: RDMA is not compiled by defau
- [valkey-io/valkey#1230](https://github.com/valkey-io/valkey/pull/1230) ([tests](tests/valkey/1230-1c222f77c/)): Improve performance of sdssplitargs (#1230): 0 of 4 test(s) reached the change, coverage 100%
- [valkey-io/valkey#1234](https://github.com/valkey-io/valkey/pull/1234) ([tests](tests/valkey/1234-91cbf7744/)): Eliminate snprintf usage at setDeferredAggregateLen (#1234): 0 of 4 test(s) reached the change, coverage 100%
- [valkey-io/valkey#1237](https://github.com/valkey-io/valkey/pull/1237) ([tests](tests/valkey/1237-ab98f375d/)): RDMA: Delete keepalive timer on closing (#1237): declined: The change is confined to src/rdma.c, which is only compiled when BUILD_RDMA=module (the default is BUILD_RDMA=no) and ships as a loadable module that requires librdmacm/libibverbs and an RDMA-capable network device (hardware or root-configured soft-RoCE). The fuzzer's servers communicate 
- [valkey-io/valkey#1235](https://github.com/valkey-io/valkey/pull/1235) ([tests](tests/valkey/1235-789a73b0d/)): Minor fix to debug logging in replicationFeedStreamFromPrimaryStream (#1235): declined: The change flips a condition inside an `if (0)` block in replicationFeedStreamFromPrimaryStream — developer-only debug printf code that is statically dead and never executes in any build. There is no observable behaviour change (no command replies, replication stream, logging, or persisten

### valkey-io/valkey-bloom

[2026-11-11T05:10:45+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42963941343): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-json

[2026-11-11T05:26:46+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42965755225): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-search

[2026-11-11T05:28:52+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42965948726): 0 merge(s) tested, 0 without a code change.


## Totals

- Merges whose tests reached the change: 779 of 1230 tested (63%)
- Changed-line coverage: median 100% of 757 merge(s) measured, 60 under 50%
- Recall: 209 parents flagged of 330 fix merges
- Claude: $882.09 in the last 24 hours, $8961.89 in the last 7 days, $8961.89 in all
- Issues filed: 68
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
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/122
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

- [valkey-io/valkey#1535](https://github.com/valkey-io/valkey/pull/1535): attempt 0 of 3, Claude Code failed (rc=1): API Error: Claude Code is unable to respond to this request, which appears to violate our Usage Policy (https://www.anthropic.com/legal/aup). This request triggered restrict
- [valkey-io/valkey#1310](https://github.com/valkey-io/valkey/pull/1310): attempt 0 of 3, Claude Code failed (rc=1): API Error: Claude Code is unable to respond to this request, which appears to violate our Usage Policy (https://www.anthropic.com/legal/aup). This request triggered restrict
