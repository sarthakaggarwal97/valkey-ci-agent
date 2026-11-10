# Fuzzer walk

Updated 2026-11-10T13:47+00:00.

| Repository | Done | Remaining | Backlog cursor | Last 24 hours | Last 7 days (per day) |
|---|---|---|---|---|---|
| valkey-io/valkey | 1506 | 1014 | #1541 (2025-01-11) | 296 | 215.1 |
| valkey-io/valkey-bloom | 109 | 0 | ce516d0ef6e3 (2024-04-26) | 0 | 15.6 |
| valkey-io/valkey-json | 63 | 0 | c1423d7e4b38 (2024-11-25) | 1 | 9.0 |
| valkey-io/valkey-search | 672 | 0 | f8106e72e984 (2024-11-27) | 3 | 96.0 |

## Last runs

### valkey-io/valkey

[2026-11-10T11:41:16+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42787464969): 20 merge(s) tested, 14 without a code change.

- [valkey-io/valkey#5033](https://github.com/valkey-io/valkey/pull/5033) ([tests](tests/valkey/5033-39f27052b/)): Respect NOFAILOVER during shutdown failover (#5033): 2 of 3 test(s) reached the change, coverage not measured (`make -j4 BUILD_ZSTD=yes gcov OPTIMIZATION=-O0` failed in coverage (exit 2): /usr/bin/x86_64-linux-gnu-ld.bfd: liblua.a(lzio.o): in function `_sub_D_00100_1': lzio.c:(.text.exit+0x5): undefined refere)
- [valkey-io/valkey#1606](https://github.com/valkey-io/valkey/pull/1606) ([tests](tests/valkey/1606-ad60d6b7b/)): Initialize one variable in struct to avoid risk (#1606): declined: The change only adds `server.cluster->failover_auth_sent = 0` in clusterInit as defensive initialization. On every reachable path the field was already written before being read: the read at cluster_legacy.c:3220 is short-circuited by failover_auth_time (initialized to 0 and set non-zero o
- [valkey-io/valkey#1604](https://github.com/valkey-io/valkey/pull/1604) ([tests](tests/valkey/1604-e9b8970e7/)): Relaxed RDB version check (#1604): 3 of 6 test(s) reached the change, coverage 72%; [check, introduced by #1604](https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/114)
- [valkey-io/valkey#1626](https://github.com/valkey-io/valkey/pull/1626) ([tests](tests/valkey/1626-7699a3a94/)): Fix use-after-free in hashtableTwoPhasePopDelete (#1626): 0 of 2 test(s) reached the change, coverage 100%
- [valkey-io/valkey#1618](https://github.com/valkey-io/valkey/pull/1618) ([tests](tests/valkey/1618-a18fcdb37/)): Deflake hashtable random fairness test (#1618): declined: The change modifies only the unit test file src/unit/test_hashtable.c: it seeds the hashtable hash function in the test harness and relaxes the random-fairness assertion threshold to deflake the test. No runtime Valkey behaviour (commands, replies, replication, persistence, cluster) is alt
- [valkey-io/valkey#1294](https://github.com/valkey-io/valkey/pull/1294) ([tests](tests/valkey/1294-3f21705a6/)): Feature COMMANDLOG to record slow execution and large request/reply (#1294): 4 of 5 test(s) reached the change, coverage 92%
- [valkey-io/valkey#1568](https://github.com/valkey-io/valkey/pull/1568) ([tests](tests/valkey/1568-f2510783f/)): Accelerate hash table iterator with value prefetching (#1568): 0 of 16 test(s) reached the change, coverage 92%; check, not reproduced (recorded)
- [valkey-io/valkey#1586](https://github.com/valkey-io/valkey/pull/1586) ([tests](tests/valkey/1586-3032ccd48/)): Change the shared format for dual channel replication logs (#1586): declined: The change only alters the log-message prefix string in the dualChannelServerLog macro (from "<Dual Channel> " to "Dual channel replication: ") and updates the matching pattern in the dual-channel-replication test. It is a cosmetic log-format change with no effect on replication behaviour,
- [valkey-io/valkey#1561](https://github.com/valkey-io/valkey/pull/1561) ([tests](tests/valkey/1561-2d0b8e360/)): Update comments and log message in cluster_legacy.c (#1561): declined: The change only rewrites code comments and rewords two serverLog message strings in clusterProcessPacket (cluster_legacy.c). No control flow, data handling, protocol, or reply behaviour changes: the same branches run, the same freeClusterLink/assert calls execute, only log text and comment
- [valkey-io/valkey#1574](https://github.com/valkey-io/valkey/pull/1574) ([tests](tests/valkey/1574-c9aea6d2d/)): Fix memory leak in forgotten node ping ext code path (#1574): 0 of 2 test(s) reached the change, coverage 100%
- [valkey-io/valkey#1563](https://github.com/valkey-io/valkey/pull/1563) ([tests](tests/valkey/1563-87cc3d7a7/)): Fix cluster info sent stats for message with light header (#1563): 0 of 2 test(s) reached the change, coverage 100%
- [valkey-io/valkey#1312](https://github.com/valkey-io/valkey/pull/1312) ([tests](tests/valkey/1312-af71619c4/)): Extract the scripting engine code from the functions unit (#1312): 3 of 7 test(s) reached the change, coverage 83%; crash, fixed by #1312 (recorded)
- [valkey-io/valkey#1517](https://github.com/valkey-io/valkey/pull/1517) ([tests](tests/valkey/1517-921ba19ac/)): Incr expired_keys if the unix-time is already expired for EXPIREAT and other commands(#1517): declined: no proposed test validated
- [valkey-io/valkey#1401](https://github.com/valkey-io/valkey/pull/1401) ([tests](tests/valkey/1401-6a8f068e3/)): Adding Missing filters to CLIENT LIST and Dedup Parsing (#1401): 11 of 11 test(s) reached the change, coverage 86%
- [valkey-io/valkey#1519](https://github.com/valkey-io/valkey/pull/1519) ([tests](tests/valkey/1519-c5a158554/)): add paused_actions for INFO Clients (#1519): 2 of 2 test(s) reached the change, coverage 100%
- [valkey-io/valkey#1553](https://github.com/valkey-io/valkey/pull/1553) ([tests](tests/valkey/1553-2a1a65b4c/)): Introduce const_sds for const-content sds (#1553): declined: The change only introduces a `const_sds` typedef (`const char *`) and updates read-only sds function signatures from `const sds` to `const_sds` for const-correctness. No function body, data layout, or control flow changes; the server.c hunk merely adapts a callback signature to the new typ
- [valkey-io/valkey#1502](https://github.com/valkey-io/valkey/pull/1502) ([tests](tests/valkey/1502-d13aad45f/)): Replace dict with new hashtable: hash datatype (#1502): 0 of 22 test(s) reached the change, coverage 87%
- [valkey-io/valkey#1534](https://github.com/valkey-io/valkey/pull/1534) ([tests](tests/valkey/1534-11cb8ee27/)): Add latency stats around cluster config file operations (#1534): 0 of 3 test(s) reached the change, coverage 84%
- [valkey-io/valkey#1191](https://github.com/valkey-io/valkey/pull/1191) ([tests](tests/valkey/1191-10357ceda/)): Mark the node as FAIL when the node is marked as NOADDR and broadcast the FAIL (#1191): 2 of 3 test(s) reached the change, coverage 100%; check, not reproduced (recorded); check, introduced and since fixed (recorded)
- [valkey-io/valkey#1018](https://github.com/valkey-io/valkey/pull/1018) ([tests](tests/valkey/1018-211b250aa/)): Do election in order based on failed primary rank to avoid voting conflicts (#1018): 0 of 4 test(s) reached the change, coverage 95%

### valkey-io/valkey-bloom

[2026-11-10T13:47:30+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42827661692): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-json

[2026-11-10T13:16:47+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42822969181): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-search

[2026-11-10T13:27:45+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42824614661): 0 merge(s) tested, 0 without a code change.


## Totals

- Merges whose tests reached the change: 744 of 1147 tested (65%)
- Changed-line coverage: median 100% of 680 merge(s) measured, 55 under 50%
- Recall: 200 parents flagged of 314 fix merges
- Claude: $1134.57 in the last 24 hours, $8360.81 in the last 7 days, $8360.81 in all
- Issues filed: 65
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
