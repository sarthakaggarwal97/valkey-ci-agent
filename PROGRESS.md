# Fuzzer walk

Updated 2026-11-09T04:32+00:00.

| Repository | Done | Remaining | Backlog cursor | Last 24 hours | Last 7 days (per day) |
|---|---|---|---|---|---|
| valkey-io/valkey | 1084 | 1423 | #2469 (2025-08-11) | 244 | 154.9 |
| valkey-io/valkey-bloom | 109 | 0 | ce516d0ef6e3 (2024-04-26) | 0 | 15.6 |
| valkey-io/valkey-json | 62 | 0 | c1423d7e4b38 (2024-11-25) | 0 | 8.9 |
| valkey-io/valkey-search | 669 | 0 | f8106e72e984 (2024-11-27) | 118 | 95.6 |

## Last runs

### valkey-io/valkey

[2026-11-09T04:26:44+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42542536339): 20 merge(s) tested, 9 without a code change.

- [valkey-io/valkey#2512](https://github.com/valkey-io/valkey/pull/2512) ([tests](tests/valkey/2512-3b6961814/)): Update pong_received time via gossip only if the node is healthy from our view (#2512): 0 of 3 test(s) reached the change, coverage 100%
- [valkey-io/valkey#2510](https://github.com/valkey-io/valkey/pull/2510) ([tests](tests/valkey/2510-e3bfc5f8c/)): Atomically update cluster and replication layer while marking self node as primary (#2510): 0 of 3 test(s) reached the change, coverage 83%
- [valkey-io/valkey#2434](https://github.com/valkey-io/valkey/pull/2434) ([tests](tests/valkey/2434-c8548f65d/)): Skip failure reports for already failed nodes (#2434): 0 of 4 test(s) reached the change, coverage 100%
- [valkey-io/valkey#2466](https://github.com/valkey-io/valkey/pull/2466) ([tests](tests/valkey/2466-a97d584cc/)): Fix slot-info expand not working, kvstoreHashtableExpand always creat… (#2466): 0 of 7 test(s) reached the change, coverage 35%
- [valkey-io/valkey#2505](https://github.com/valkey-io/valkey/pull/2505) ([tests](tests/valkey/2505-bb81f8d14/)): fix hash ignore ttl management during active expiry (#2505): 0 of 5 test(s) reached the change, coverage 100%
- [valkey-io/valkey#2500](https://github.com/valkey-io/valkey/pull/2500) ([tests](tests/valkey/2500-fb9503bfd/)): CLUSTER SYNCSLOTS ESTABLISH added source node == myself check (#2500): 2 of 2 test(s) reached the change, coverage 100%
- [valkey-io/valkey#2449](https://github.com/valkey-io/valkey/pull/2449) ([tests](tests/valkey/2449-2ca0dd878/)): Make cluster failover delay relative to node timeout (#2449): 0 of 8 test(s) reached the change, coverage 77%; crash, introduced and since fixed (recorded)
- [valkey-io/valkey#2503](https://github.com/valkey-io/valkey/pull/2503) ([tests](tests/valkey/2503-7a5c0d0eb/)): Fix memory leak in saveSnapshotToConnectionSockets (#2503): 0 of 7 test(s) reached the change, coverage 30%
- [valkey-io/valkey#2493](https://github.com/valkey-io/valkey/pull/2493) ([tests](tests/valkey/2493-390e0c955/)): CONFIG GET command return sorted output (#2493): 2 of 3 test(s) reached the change, coverage 100%
- [valkey-io/valkey#2502](https://github.com/valkey-io/valkey/pull/2502) ([tests](tests/valkey/2502-4a1202170/)): Fix memory leak in rdbLoadObject when loading a wrong HFE (#2502): 0 of 6 test(s) reached the change, coverage 100%
- [valkey-io/valkey#2484](https://github.com/valkey-io/valkey/pull/2484) ([tests](tests/valkey/2484-f36cc2083/)): Fix cluster test module to pass null terminated node-id to SendClusterMessage (#2484): declined: The change only modifies the test module tests/modules/cluster.c, null-terminating sender_id before passing it to ValkeyModule_SendClusterMessage. On both the change's build and its parent, the server already delivers sender_id to module cluster-message receivers as a null-terminated sds (
- [valkey-io/valkey#2490](https://github.com/valkey-io/valkey/pull/2490) ([tests](tests/valkey/2490-e7bb2354d/)): Don't call SSL_write() with num=0 (#2490): 0 of 6 test(s) reached the change, coverage 100%
- [valkey-io/valkey#2465](https://github.com/valkey-io/valkey/pull/2465) ([tests](tests/valkey/2465-aefed3d36/)): Don't allow resize hashtable if rehashing is ongoing (#2465): 2 of 6 test(s) reached the change, coverage 100%; [crash, shown by ASan on unstable, the one build with it](https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/110)
- [valkey-io/valkey#2481](https://github.com/valkey-io/valkey/pull/2481) ([tests](tests/valkey/2481-0b4352c98/)): Consolidate slot migration logs by grouping consecutive slot migrations into a single log entry (#2481): 0 of 3 test(s) reached the change, coverage 100%
- [valkey-io/valkey#2477](https://github.com/valkey-io/valkey/pull/2477) ([tests](tests/valkey/2477-3d1ff2a38/)): Remove if condition and disable the new failover test (#2477): 0 of 3 test(s) reached the change, coverage 100%
- [valkey-io/valkey#2475](https://github.com/valkey-io/valkey/pull/2475) ([tests](tests/valkey/2475-15355124d/)): HSETEX 'hset' notification should only be generated if not expired (#2475): 0 of 3 test(s) reached the change, coverage 100%; check, predates #2475, since fixed (recorded)
- [valkey-io/valkey#2474](https://github.com/valkey-io/valkey/pull/2474) ([tests](tests/valkey/2474-c93b2971e/)): Increment expired_fields stat when assigned TTL is in the past (#2474): 3 of 3 test(s) reached the change, coverage 100%
- [valkey-io/valkey#2430](https://github.com/valkey-io/valkey/pull/2430) ([tests](tests/valkey/2430-58f5562d2/)): Fix duplicate Acks for RDMA events and fix extremely large max latency for RDMA benchmark. (#2430): declined: The change is confined to RDMA-only code: src/rdma.c is compiled only under USE_RDMA, the valkey-benchmark changes are inside #ifdef USE_RDMA / config.ct == VALKEY_CONN_RDMA guards (the TCP branch of createClient is behaviorally unchanged), and tests/rdma/rdma-test.c is an RDMA test harnes
- [valkey-io/valkey#2431](https://github.com/valkey-io/valkey/pull/2431) ([tests](tests/valkey/2431-7a9ef29f1/)): Change the same shard failover assert to if condition to avoid crash (#2431): declined: no proposed test validated
- [valkey-io/valkey#1949](https://github.com/valkey-io/valkey/pull/1949) ([tests](tests/valkey/1949-d7993b78d/)): Introduce atomic slot migration (#1949): 11 of 16 test(s) reached the change, coverage 73%; crash, introduced and since fixed (recorded); check, not reproduced (recorded); check, introduced and since fixed (recorded)

### valkey-io/valkey-bloom

[2026-11-09T04:25:37+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42561707357): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-json

[2026-11-09T04:29:04+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42562016877): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-search

[2026-11-09T04:32:57+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42562374062): 0 merge(s) tested, 0 without a code change.


## Totals

- Merges whose tests reached the change: 632 of 938 tested (67%)
- Changed-line coverage: median 100% of 494 merge(s) measured, 38 under 50%
- Recall: 173 parents flagged of 262 fix merges
- Claude: $1206.36 in the last 24 hours, $6814.90 in the last 7 days, $6814.90 in all
- Issues filed: 61
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

- [valkey-io/valkey#3800](https://github.com/valkey-io/valkey/pull/3800): attempt 0 of 3, Claude Code failed (rc=1): API Error: Claude Code is unable to respond to this request, which appears to violate our Usage Policy (https://www.anthropic.com/legal/aup). This request triggered restrict
