# Fuzzer walk

Updated 2026-11-12T04:00+00:00.

| Repository | Done | Remaining | Backlog cursor | Last 24 hours | Last 7 days (per day) |
|---|---|---|---|---|---|
| valkey-io/valkey | 1923 | 607 | #766 (2024-07-09) | 269 | 271.1 |
| valkey-io/valkey-bloom | 109 | 0 | ce516d0ef6e3 (2024-04-26) | 0 | 15.0 |
| valkey-io/valkey-json | 64 | 0 | c1423d7e4b38 (2024-11-25) | 0 | 8.3 |
| valkey-io/valkey-search | 673 | 0 | f8106e72e984 (2024-11-27) | 1 | 96.1 |

## Last runs

### valkey-io/valkey

[2026-11-12T01:32:05+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/43116260972): 20 merge(s) tested, 8 without a code change.

- [valkey-io/valkey#762](https://github.com/valkey-io/valkey/pull/762) ([tests](tests/valkey/762-59aa00823/)): Replicas with the same offset queue up for election (#762): 0 of 3 test(s) reached the change, coverage 100%; crash, predates #762, since fixed (recorded)
- [valkey-io/valkey#810](https://github.com/valkey-io/valkey/pull/810) ([tests](tests/valkey/810-9a7bf910c/)): Fix extra reply in debug sleep-after-fork-seconds error path (#810): 2 of 2 test(s) reached the change
- [valkey-io/valkey#712](https://github.com/valkey-io/valkey/pull/712) ([tests](tests/valkey/712-5000c050b/)): Add cpu-usec metric support under CLUSTER SLOT-STATS command (#20). (#712): 3 of 5 test(s) reached the change, coverage 100%; crash, introduced and since fixed (recorded)
- [valkey-io/valkey#776](https://github.com/valkey-io/valkey/pull/776) ([tests](tests/valkey/776-14e09e981/)): Fix the wrong woff when execute WAIT / WAITAOF in script (#776): 2 of 3 test(s) reached the change, coverage 100%
- [valkey-io/valkey#807](https://github.com/valkey-io/valkey/pull/807) ([tests](tests/valkey/807-6eb19cf38/)): Typo fix in hyperloglog.c (#807): declined: The change only fixes a spelling typo inside a code comment in src/hyperloglog.c (hypreloglog -> hyperloglog). No executable code, behaviour, reply, or data format is affected, so there is nothing to test.
- [valkey-io/valkey#782](https://github.com/valkey-io/valkey/pull/782) ([tests](tests/valkey/782-15a829023/)): Optimize failover time when the new primary node is down again (#782): 0 of 3 test(s) reached the change, coverage 100%
- [valkey-io/valkey#790](https://github.com/valkey-io/valkey/pull/790) ([tests](tests/valkey/790-816accea7/)): Generate correct slot information in cluster shards command on primary failure  (#790): 0 of 3 test(s) reached the change, coverage 95%
- [valkey-io/valkey#763](https://github.com/valkey-io/valkey/pull/763) ([tests](tests/valkey/763-94bc15cb7/)): Io thread work offload (#763): 3 of 8 test(s) reached the change, coverage 80%
- [valkey-io/valkey#803](https://github.com/valkey-io/valkey/pull/803) ([tests](tests/valkey/803-8b480310a/)): Remove read handler upon RDB connection close (#803): 0 of 3 test(s) reached the change, coverage 0%
- [valkey-io/valkey#801](https://github.com/valkey-io/valkey/pull/801) ([tests](tests/valkey/801-36e81d9e7/)): Fix rdb_child_exit_pipe incorrect close call (#801): declined: The change only moves close(server.rdb_child_exit_pipe) inside the existing !dual_channel branches in rdbSaveToReplicasSockets. In non-dual-channel diskless sync the closed fds and their order are identical on both builds. In dual-channel sync rdb_child_exit_pipe is always -1 (asserted at 
- [valkey-io/valkey#800](https://github.com/valkey-io/valkey/pull/800) ([tests](tests/valkey/800-c58437150/)): Fix rdb pipe uninitialized false positive warning (#800): declined: The change only initializes two local variables (rdb_pipe_write, safe_to_exit_pipe) to 0 in rdbSaveToReplicasSockets to silence a false-positive 'may be used uninitialized' compiler warning. Every read or close of those variables is guarded by !dual_channel, the same path that assigns them
- [valkey-io/valkey#60](https://github.com/valkey-io/valkey/pull/60) ([tests](tests/valkey/60-ff6b780fe/)): Dual channel replication (#60): 6 of 25 test(s) reached the change, coverage 79%
- [valkey-io/valkey#754](https://github.com/valkey-io/valkey/pull/754) ([tests](tests/valkey/754-66d0f7d9a/)): Ensure only primary sender drives slot ownership updates (#754): 0 of 4 test(s) reached the change, coverage 91%
- [valkey-io/valkey#647](https://github.com/valkey-io/valkey/pull/647) ([tests](tests/valkey/647-1a8bd045f/)): Replace master-reboot-down-after-period with primary-reboot-down-after-period in sentinel.conf (#647): declined: The change is confined to Sentinel mode: sentinelHandleConfiguration (parsing the 'sentinel primary-reboot-down-after-period' directive at startup), sentinelSetCommand (SENTINEL SET), and rewriteConfigSentinelOption (CONFIG REWRITE in a Sentinel), plus a sentinel.conf comment and a Sentine
- [valkey-io/valkey#477](https://github.com/valkey-io/valkey/pull/477) ([tests](tests/valkey/477-dd4bd5065/)): Introduce Valkey Over RDMA transport (experimental) (#477): declined: The change only adds a new optional RDMA transport module (src/rdma.c) that is compiled solely when BUILD_RDMA=module is set (default BUILD_RDMA=no does nothing), plus Makefile/CI/README/test-harness additions. No existing server code path is modified. Exercising it would require a build w
- [valkey-io/valkey#500](https://github.com/valkey-io/valkey/pull/500) ([tests](tests/valkey/500-418901dec/)): Limit tracking custom errors (e.g. from LUA) while allowing non custom errors to be tracked normally (#500): 2 of 2 test(s) reached the change, coverage 100%
- [valkey-io/valkey#738](https://github.com/valkey-io/valkey/pull/738) ([tests](tests/valkey/738-34649bd03/)): Configurable cluster blacklist TTL (#738): 2 of 3 test(s) reached the change, coverage 100%
- [valkey-io/valkey#756](https://github.com/valkey-io/valkey/pull/756) ([tests](tests/valkey/756-b4ac2c406/)): Update gitignore to ignore the cluster-cluster test files (#756): declined: The change modifies only .gitignore, widening three ignore patterns (dump.rdb -> dump*.rdb, appendonlydir -> appendonlydir*, nodes.conf -> nodes*.conf) so cluster-cluster test artifacts are untracked. It is a repository housekeeping/CI-hygiene change with no code, config, protocol, or beha
- [valkey-io/valkey#736](https://github.com/valkey-io/valkey/pull/736) ([tests](tests/valkey/736-a323dce89/)): Dual stack and client-specific IPs in cluster (#736): 2 of 7 test(s) reached the change, coverage 70%; crash, introduced and since fixed (recorded); crash, introduced and since fixed (recorded); crash, introduced and since fixed (recorded)
- [valkey-io/valkey#752](https://github.com/valkey-io/valkey/pull/752) ([tests](tests/valkey/752-6a5a11f21/)): Fix ULong config boundary checking (#752): declined: The fix routes NUMERIC_TYPE_ULONG configs through the unsigned boundary-check branch of numericBoundaryCheck instead of the signed one, but no observable behavior changes on the test platform. The only ULONG configs are active-defrag-max-scan-fields (bounds 1..LONG_MAX), slowlog-max-len an

### valkey-io/valkey-bloom

[2026-11-12T03:57:57+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/43151582814): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-json

[2026-11-12T03:57:53+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/43151579237): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-search

[2026-11-12T04:00:48+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/43151851352): 0 merge(s) tested, 0 without a code change.


## Totals

- Merges whose tests reached the change: 836 of 1337 tested (63%)
- Changed-line coverage: median 100% of 849 merge(s) measured, 66 under 50%
- Recall: 224 parents flagged of 353 fix merges
- Claude: $819.18 in the last 24 hours, $9612.17 in the last 7 days, $9733.98 in all
- Issues filed: 71
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
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/125
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/126
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/127
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

- [valkey-io/valkey#1310](https://github.com/valkey-io/valkey/pull/1310): attempt 0 of 3, Claude Code failed (rc=1): API Error: Claude Code is unable to respond to this request, which appears to violate our Usage Policy (https://www.anthropic.com/legal/aup). This request triggered restrict
