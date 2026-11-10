# Fuzzer walk

Updated 2026-11-10T11:13+00:00.

| Repository | Done | Remaining | Backlog cursor | Last 24 hours | Last 7 days (per day) |
|---|---|---|---|---|---|
| valkey-io/valkey | 1472 | 1047 | #1606 (2025-01-28) | 307 | 210.3 |
| valkey-io/valkey-bloom | 109 | 0 | ce516d0ef6e3 (2024-04-26) | 0 | 15.6 |
| valkey-io/valkey-json | 63 | 0 | c1423d7e4b38 (2024-11-25) | 1 | 9.0 |
| valkey-io/valkey-search | 672 | 0 | f8106e72e984 (2024-11-27) | 3 | 96.0 |

## Last runs

### valkey-io/valkey

[2026-11-10T09:03:08+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42774225967): 20 merge(s) tested, 9 without a code change.

- [valkey-io/valkey#4526](https://github.com/valkey-io/valkey/pull/4526) ([tests](tests/valkey/4526-35373fa80/)): Compact sparse B+tree leaves inline on the sorted set delete path (#4526): 3 of 6 test(s) reached the change, coverage not measured (`make -j4 BUILD_ZSTD=yes gcov OPTIMIZATION=-O0` failed in coverage (exit 2): /usr/bin/x86_64-linux-gnu-ld.bfd: liblua.a(lzio.o): in function `_sub_D_00100_1': lzio.c:(.text.exit+0x5): undefined refere)
- [valkey-io/valkey#5183](https://github.com/valkey-io/valkey/pull/5183) ([tests](tests/valkey/5183-6187f98a6/)): Make BCAST tracking cost proportional to the prefixes a key matches (#5183): 0 of 3 test(s) reached the change, coverage not measured (`make -j4 BUILD_ZSTD=yes gcov OPTIMIZATION=-O0` failed in coverage (exit 2): /usr/bin/x86_64-linux-gnu-ld.bfd: liblua.a(lauxlib.o): in function `_sub_I_00100_0': lauxlib.c:(.text.startup+0xc): undefin)
- [valkey-io/valkey#1684](https://github.com/valkey-io/valkey/pull/1684) ([tests](tests/valkey/1684-61a854dbb/)): Fix client trackinginfo crash when tracking off by default (#1684): 2 of 2 test(s) reached the change, coverage 100%; crash, fixed by #1684 (recorded)
- [valkey-io/valkey#1691](https://github.com/valkey-io/valkey/pull/1691) ([tests](tests/valkey/1691-e8ae5b185/)): Fix log message "DB 9: 100 keys (0 volatile) in 16 slots HT" (#1691): declined: The change only corrects the slot count printed in a verbosity-debug serverCron log message ("DB %d: ... in %lld slots HT") by multiplying kvstoreBuckets by a new constant accessor hashtableEntriesPerBucket(). No command reply, data path, replication, persistence, or config behaviour is af
- [valkey-io/valkey#1485](https://github.com/valkey-io/valkey/pull/1485) ([tests](tests/valkey/1485-de3672a7f/)): Offload replication writes to IO threads (#1485): 0 of 7 test(s) reached the change, coverage 96%
- [valkey-io/valkey#1685](https://github.com/valkey-io/valkey/pull/1685) ([tests](tests/valkey/1685-7fa784a27/)): Disable mem usage test case for non-jemalloc allocators (#1685): 1 of 1 test(s) reached the change, coverage 100%
- [valkey-io/valkey#1564](https://github.com/valkey-io/valkey/pull/1564) ([tests](tests/valkey/1564-da3f1c614/)): Add paused_reason to INFO CLIENTS (#1564): 6 of 7 test(s) reached the change, coverage 90%
- [valkey-io/valkey#1678](https://github.com/valkey-io/valkey/pull/1678) ([tests](tests/valkey/1678-0579103ae/)): Add reply schema for LATENCY-LATESTS new fields (#1678): declined: The diff only edits the reply_schema metadata in src/commands/latency-latest.json to document the sum and count fields LATENCY LATEST already returns; the reply-producing code in src/latency.c is unchanged and already emits six elements on both the parent and the change. Reply schemas are 
- [valkey-io/valkey#1676](https://github.com/valkey-io/valkey/pull/1676) ([tests](tests/valkey/1676-a3c9f6766/)): Fix kvstore overhead_hashtable_lut (#1676): declined: The fix only changes kvstoreCreate for kvstores created without KVSTORE_ALLOCATE_HASHTABLES_ON_DEMAND (the eager hashtable-creation loop previously ran before overhead_hashtable_lut was zeroed, discarding its accounting). Every kvstore the server creates (per-db keys/expires in server.c, p
- [valkey-io/valkey#1613](https://github.com/valkey-io/valkey/pull/1613) ([tests](tests/valkey/1613-8d8ce19da/)): Embed keys and hash fields as SDS type 5 (#1613): 4 of 6 test(s) reached the change, coverage 96%
- [valkey-io/valkey#1667](https://github.com/valkey-io/valkey/pull/1667) ([tests](tests/valkey/1667-f875fceea/)): Fix client querybuf resize test by skipping the clientsCron (#1667): 0 of 2 test(s) reached the change, coverage 100%
- [valkey-io/valkey#1387](https://github.com/valkey-io/valkey/pull/1387) ([tests](tests/valkey/1387-c75e86617/)): move clientCron onto a separate timer (#1387): 5 of 8 test(s) reached the change, coverage 93%
- [valkey-io/valkey#1479](https://github.com/valkey-io/valkey/pull/1479) ([tests](tests/valkey/1479-2eac2ccd2/)): skip CRC checksumming during diskless full sync with TLS enabled. (#1479): 2 of 11 test(s) reached the change, coverage 78%
- [valkey-io/valkey#1570](https://github.com/valkey-io/valkey/pull/1570) ([tests](tests/valkey/1570-6eaf0887c/)): Extend LATENCY LATEST to add sum / cnt stats (#1570): 2 of 2 test(s) reached the change, coverage 100%
- [valkey-io/valkey#1651](https://github.com/valkey-io/valkey/pull/1651) ([tests](tests/valkey/1651-aced268c2/)): Fix ACL regression with renamed commands (#1651): 0 of 6 test(s) reached the change, coverage 87%
- [valkey-io/valkey#1572](https://github.com/valkey-io/valkey/pull/1572) ([tests](tests/valkey/1572-e7dbce3a1/)): Enable module API SendClusterMessage to use light message header type (#1572): 3 of 5 test(s) reached the change, coverage 68%
- [valkey-io/valkey#1557](https://github.com/valkey-io/valkey/pull/1557) ([tests](tests/valkey/1557-26c6f1af9/)): Loop optimization: move maxlen check outside to reduce unnecessary checks (#1557): declined: The change is a pure loop micro-optimization in hashtableLongestBucketChain() in src/hashtable.c, whose only caller is the unit test src/unit/test_hashtable.c; it is unreachable from any server command, replication, or persistence path. The rewrite is semantics-preserving (chainlen is mono
- [valkey-io/valkey#1643](https://github.com/valkey-io/valkey/pull/1643) ([tests](tests/valkey/1643-ff8a528fd/)): Fix a heap-use-after-free bug in cluster bus (#1643): 0 of 2 test(s) reached the change, coverage 100%
- [valkey-io/valkey#1614](https://github.com/valkey-io/valkey/pull/1614) ([tests](tests/valkey/1614-4b8f3ed9a/)): Do command existence and arity checks when loading AOF to avoid crash (#1614): 3 of 3 test(s) reached the change, coverage 83%; crash, fixed by #1614 (recorded); crash, fixed by #1614 (recorded); crash, fixed by #1614 (recorded)
- [valkey-io/valkey#1602](https://github.com/valkey-io/valkey/pull/1602) ([tests](tests/valkey/1602-d72a97edf/)): RDMA: Protect RDMA memory regions (#1602): declined: The change only touches src/rdma.c, which is compiled only with BUILD_RDMA and whose changed functions (rdmaMemoryAlloc/rdmaMemoryFree, called from rdmaSetupIoBuf, rdmaAdjustSendbuf and rdmaDestroyIoBuf) execute only when an RDMA client establishes a connection. That requires RDMA hardware

### valkey-io/valkey-bloom

[2026-11-10T11:13:49+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42805659008): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-json

[2026-11-10T10:43:17+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42801423217): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-search

[2026-11-10T10:53:23+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42802805950): 0 merge(s) tested, 0 without a code change.


## Totals

- Merges whose tests reached the change: 737 of 1133 tested (65%)
- Changed-line coverage: median 100% of 667 merge(s) measured, 55 under 50%
- Recall: 200 parents flagged of 311 fix merges
- Claude: $1188.34 in the last 24 hours, $8236.64 in the last 7 days, $8236.64 in all
- Issues filed: 64
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
