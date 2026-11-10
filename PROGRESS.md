# Fuzzer walk

Updated 2026-11-10T01:36+00:00.

| Repository | Done | Remaining | Backlog cursor | Last 24 hours | Last 7 days (per day) |
|---|---|---|---|---|---|
| valkey-io/valkey | 1345 | 1165 | #1918 (2025-04-05) | 290 | 192.1 |
| valkey-io/valkey-bloom | 109 | 0 | ce516d0ef6e3 (2024-04-26) | 0 | 15.6 |
| valkey-io/valkey-json | 63 | 0 | c1423d7e4b38 (2024-11-25) | 1 | 9.0 |
| valkey-io/valkey-search | 671 | 0 | f8106e72e984 (2024-11-27) | 16 | 95.9 |

## Last runs

### valkey-io/valkey

[2026-11-10T00:42:00+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42720840888): 20 merge(s) tested, 3 without a code change.

- [valkey-io/valkey#1819](https://github.com/valkey-io/valkey/pull/1819) ([tests](tests/valkey/1819-57118a475/)): Fix engine crash on module client blocking during keyspace events (#1819): 0 of 4 test(s) reached the change, coverage 15%; crash, introduced and since fixed (recorded)
- [valkey-io/valkey#1931](https://github.com/valkey-io/valkey/pull/1931) ([tests](tests/valkey/1931-1119765df/)): Fix CLUSTER RESET to use lazyfree-lazy-user-flush to do the lazyfree (#1931): 2 of 2 test(s) reached the change, coverage 100%
- [valkey-io/valkey#1952](https://github.com/valkey-io/valkey/pull/1952) ([tests](tests/valkey/1952-c8bfd23d9/)): Fix incorrect lag reported in XINFO GROUPS (#1952): 2 of 2 test(s) reached the change, coverage 100%
- [valkey-io/valkey#1904](https://github.com/valkey-io/valkey/pull/1904) ([tests](tests/valkey/1904-5d6e77f1c/)): Add sentinel_total_tilt to sentinel INFO sentinel (#1904): declined: The change is confined to Sentinel mode: sentinel.c adds an int total_tilt counter, increments it when sentinelCheckTiltCondition() enters TILT, and prints it as a new sentinel_total_tilt line in INFO sentinel. That code only runs in a valkey-sentinel process (started with --sentinel or th
- [valkey-io/valkey#1910](https://github.com/valkey-io/valkey/pull/1910) ([tests](tests/valkey/1910-30dc9a7ae/)): Add node pfail and fail count to cluster info metrics (#1910): 2 of 3 test(s) reached the change, coverage 92%
- [valkey-io/valkey#1777](https://github.com/valkey-io/valkey/pull/1777) ([tests](tests/valkey/1777-d1053ac31/)): Ignore stale gossip packets that arrive out of order (#1777): 0 of 4 test(s) reached the change, coverage 66%
- [valkey-io/valkey#1948](https://github.com/valkey-io/valkey/pull/1948): to retry, Claude Code failed (rc=1): API Error: Claude Code is unable to respond to this request, which appears to violate our Usage Policy (https://www.anthropic.com/legal/aup). This request triggered restrict
- [valkey-io/valkey#1950](https://github.com/valkey-io/valkey/pull/1950) ([tests](tests/valkey/1950-5854995f0/)): Fix cluster slot stats assertion during promotion of replica (#1950): 0 of 2 test(s) reached the change, coverage 100%
- [valkey-io/valkey#1955](https://github.com/valkey-io/valkey/pull/1955) ([tests](tests/valkey/1955-2399f1077/)): Fix crash during TLS handshake with I/O threads (#1955): declined: The change adds one atomic_thread_fence in ioThreadAccept (io_threads.c), which runs only when a TLS accept is offloaded to an I/O thread: trySendAcceptToIOThreads is called solely from connTLSAccept in tls.c, so on plain-TCP connections the changed code never executes. The fuzzer cannot c
- [valkey-io/valkey#1811](https://github.com/valkey-io/valkey/pull/1811) ([tests](tests/valkey/1811-4a92db917/)): Introduce MPTCP (#1811): 1 of 9 test(s) reached the change, coverage 66%
- [valkey-io/valkey#1466](https://github.com/valkey-io/valkey/pull/1466) ([tests](tests/valkey/1466-be60586f1/)): [Client Introspection] Client Commands Extended Filtering (#1466): 8 of 8 test(s) reached the change, coverage 80%; crash, predates #1466, since fixed (recorded)
- [valkey-io/valkey#1934](https://github.com/valkey-io/valkey/pull/1934) ([tests](tests/valkey/1934-51387ddc9/)): Update function clusterNodeSetSlotBit() return type to void (#1934): declined: The change only alters clusterNodeSetSlotBit()'s return type from int to void. Its sole caller, clusterAddSlot() (cluster_legacy.c:5651), already discarded the return value before the change, and the function's side effects (setting the slot bit, incrementing numslots, setting CLUSTER_NODE
- [valkey-io/valkey#1945](https://github.com/valkey-io/valkey/pull/1945) ([tests](tests/valkey/1945-e2b28c6a0/)): Rebranding in security warning log (#1945): declined: The change only substitutes the SERVER_TITLE macro ("Valkey") for the hardcoded string "Redis" in the security-warning log message of securityWarningCommand (networking.c). No reply, connection handling, rate limiting, or any other runtime behaviour changes: the client is still aborted the
- [valkey-io/valkey#1091](https://github.com/valkey-io/valkey/pull/1091) ([tests](tests/valkey/1091-44dafba2c/)): Trigger manual failover on SIGTERM / shutdown to cluster primary (#1091): 2 of 5 test(s) reached the change, coverage 85%
- [valkey-io/valkey#1825](https://github.com/valkey-io/valkey/pull/1825) ([tests](tests/valkey/1825-f7da0c157/)): fix: add samples to stream object consumer trees (#1825): 2 of 2 test(s) reached the change, coverage 100%
- [valkey-io/valkey#1793](https://github.com/valkey-io/valkey/pull/1793) ([tests](tests/valkey/1793-041043617/)): Replaced the use of sdsReplyDictType and hashDictType. (#1793): 0 of 5 test(s) reached the change, coverage 94%
- [valkey-io/valkey#1926](https://github.com/valkey-io/valkey/pull/1926) ([tests](tests/valkey/1926-772bd8b57/)): Kill RDB child process on CLUSTER RESET command (#1926): 0 of 2 test(s) reached the change, coverage 100%
- [valkey-io/valkey#1674](https://github.com/valkey-io/valkey/pull/1674) ([tests](tests/valkey/1674-74075203d/)): Implement: CLUSTER REPLICATE NO ONE (#1674): 4 of 4 test(s) reached the change, coverage 100%
- [valkey-io/valkey#1922](https://github.com/valkey-io/valkey/pull/1922) ([tests](tests/valkey/1922-204097dac/)): Fix the build on less common platforms in zmalloc.c (#1922): declined: The change only relocates the declaration of the used_memory_thread pointer into the #if/#else in zmalloc.c so that on non-x86/ARM/PowerPC platforms it is declared _Atomic size_t* to match the _Atomic array, fixing a compile error there. On the platforms the fuzzer runs (linux x86_64/arm) 
- [valkey-io/valkey#1919](https://github.com/valkey-io/valkey/pull/1919) ([tests](tests/valkey/1919-dfdcbfe91/)): Fix minor comment typo (#1919): declined: The change only rewords a comment above quicklistCompare in src/quicklist.c; no executable code, configuration, or build file changes, so there is no behaviour to exercise and no scenario is needed.

### valkey-io/valkey-bloom

[2026-11-10T01:29:49+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42736106811): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-json

[2026-11-10T01:29:55+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42736125221): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-search

[2026-11-10T01:36:53+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42736917394): 0 merge(s) tested, 0 without a code change.


## Totals

- Merges whose tests reached the change: 705 of 1076 tested (66%)
- Changed-line coverage: median 100% of 620 merge(s) measured, 52 under 50%
- Recall: 194 parents flagged of 296 fix merges
- Claude: $1266.94 in the last 24 hours, $7911.35 in the last 7 days, $7911.35 in all
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
