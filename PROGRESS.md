# Fuzzer walk

Updated 2026-11-09T16:23+00:00.

| Repository | Done | Remaining | Backlog cursor | Last 24 hours | Last 7 days (per day) |
|---|---|---|---|---|---|
| valkey-io/valkey | 1232 | 1275 | #2146 (2025-05-28) | 266 | 176.0 |
| valkey-io/valkey-bloom | 109 | 0 | ce516d0ef6e3 (2024-04-26) | 0 | 15.6 |
| valkey-io/valkey-json | 62 | 0 | c1423d7e4b38 (2024-11-25) | 0 | 8.9 |
| valkey-io/valkey-search | 669 | 0 | f8106e72e984 (2024-11-27) | 34 | 95.6 |

## Last runs

### valkey-io/valkey

[2026-11-09T15:54:16+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42626948188): 20 merge(s) tested, 2 without a code change.

- [valkey-io/valkey#1566](https://github.com/valkey-io/valkey/pull/1566) ([tests](tests/valkey/1566-1b531e560/)): Call makeObjectShared to normalize a shared object into a true shared object (#1566): 0 of 5 test(s) reached the change, coverage 100%
- [valkey-io/valkey#2192](https://github.com/valkey-io/valkey/pull/2192) ([tests](tests/valkey/2192-477166068/)): Update extended-redis-compatibility conf to remove the specific words (#2192): declined: The change edits only a comment in valkey.conf, rewording the deprecation timeline of extended-redis-compatibility ('Valkey 9.0/10.0' becomes 'a future version'). No code path, config default, parsing, or server behaviour changes; the config option itself is untouched. A documentation-only
- [valkey-io/valkey#2074](https://github.com/valkey-io/valkey/pull/2074) ([tests](tests/valkey/2074-767302e3a/)): Update commandlog parameters conf text to mention special value settings (#2074): declined: The change edits only comment text in valkey.conf (documentation for the commandlog-* parameters: deprecation note and the meaning of -1/0 thresholds). No configuration defaults, parsing, or server code change; behaviour is identical on both builds, so there is nothing a scenario could rea
- [valkey-io/valkey#1609](https://github.com/valkey-io/valkey/pull/1609) ([tests](tests/valkey/1609-8e2f35576/)): On-demand database allocation instead of preallocation (#1609): 0 of 16 test(s) reached the change, coverage 100%
- [valkey-io/valkey#2097](https://github.com/valkey-io/valkey/pull/2097) ([tests](tests/valkey/2097-b5e012a10/)): Fix invalid functionname processMultibulkBuffer typo in comments. (#2097): declined: The diff only edits a comment in src/networking.c, fixing the capitalization of the function name processMultibulkBuffer referenced in the readToQueryBuf comment. No executable code, protocol behaviour, replies, or configuration are affected, so there is no changed path for a scenario to r
- [valkey-io/valkey#2186](https://github.com/valkey-io/valkey/pull/2186) ([tests](tests/valkey/2186-5cc2b2575/)): Fix cluster myself CLUSTER SLOTS/NODES wrong port after updating port/tls-port (#2186): 1 of 1 test(s) reached the change, coverage 50%
- [valkey-io/valkey#2117](https://github.com/valkey-io/valkey/pull/2117) ([tests](tests/valkey/2117-3bc40be6c/)): CLIENT UNBLOCK should't be able to unpause paused clients (#2117): 1 of 5 test(s) reached the change, coverage 80%; crash, fixed by #2117 (recorded)
- [valkey-io/valkey#2178](https://github.com/valkey-io/valkey/pull/2178) ([tests](tests/valkey/2178-476671be1/)): Fix replica can't finish failover when config epoch is outdated (#2178): 0 of 3 test(s) reached the change, coverage 0%
- [valkey-io/valkey#2070](https://github.com/valkey-io/valkey/pull/2070) ([tests](tests/valkey/2070-1941d28ac/)): [NEW] Introduce lttng based tracing (#2070): 2 of 6 test(s) reached the change, coverage 68%
- [valkey-io/valkey#2078](https://github.com/valkey-io/valkey/pull/2078) ([tests](tests/valkey/2078-838ba44cd/)): Reply Copy Avoidance (#2078): 6 of 10 test(s) reached the change, coverage 95%
- [valkey-io/valkey#2010](https://github.com/valkey-io/valkey/pull/2010) ([tests](tests/valkey/2010-988297d19/)): Avoid log spam about cluster node failure detection by each primary (#2010): declined: The change only lowers the log level of two cluster gossip messages (failure report added / removed) from LL_NOTICE to LL_VERBOSE in clusterProcessGossipSection. The failure-detection logic itself (clusterNodeAddFailureReport, clusterNodeDelFailureReport, markNodeAsFailingIfNeeded) is unch
- [valkey-io/valkey#2175](https://github.com/valkey-io/valkey/pull/2175) ([tests](tests/valkey/2175-4dae83dba/)): Remove unnecessary refcount increment in propagateDelete (#2175): 0 of 2 test(s) reached the change
- [valkey-io/valkey#2168](https://github.com/valkey-io/valkey/pull/2168) ([tests](tests/valkey/2168-0039adcc7/)): Remove dead conditions around the multi/exec check (#2168): declined: Pure dead-code removal with no behaviour change. The removed conditions (c->cmd->proc != multiCommand && c->cmd->proc != watchCommand) in the transaction-queueing check at server.c:4377 are unreachable on both builds: MULTI and WATCH carry the CMD_NO_MULTI flag (src/commands/multi.json, sr
- [valkey-io/valkey#2165](https://github.com/valkey-io/valkey/pull/2165) ([tests](tests/valkey/2165-3789b29e9/)): Offload slot calculation and cross-slot detection to I/O threads (#2165): 0 of 6 test(s) reached the change, coverage 82%
- [valkey-io/valkey#2129](https://github.com/valkey-io/valkey/pull/2129) ([tests](tests/valkey/2129-5699c8c05/)): Combine range element ranks calculation with range elements search to improve zcount performance (#2129): 0 of 3 test(s) reached the change, coverage 96%
- [valkey-io/valkey#1384](https://github.com/valkey-io/valkey/pull/1384) ([tests](tests/valkey/1384-992c5b987/)): Add CLUSTER FLUSHSLOT command (#1384): 4 of 5 test(s) reached the change, coverage 88%
- [valkey-io/valkey#2156](https://github.com/valkey-io/valkey/pull/2156) ([tests](tests/valkey/2156-36312085e/)): Have threads gracefully exit instead of killing them when checking thread count (#2156): 0 of 4 test(s) reached the change, coverage 100%
- [valkey-io/valkey#2151](https://github.com/valkey-io/valkey/pull/2151) ([tests](tests/valkey/2151-831220a30/)): Skip empty db when doing FLUSHALL to reduce some NOP (#2151): 0 of 3 test(s) reached the change, coverage 100%
- [valkey-io/valkey#2152](https://github.com/valkey-io/valkey/pull/2152) ([tests](tests/valkey/2152-23ecd7a96/)): Unify behavior of CLIENT REPLY in multi with other NO_MULTI commands (#2152): 2 of 2 test(s) reached the change, coverage 100%
- [valkey-io/valkey#2150](https://github.com/valkey-io/valkey/pull/2150) ([tests](tests/valkey/2150-92c859de6/)): Update tests to catch module context leaks if using aux save/load  (#2150): 0 of 2 test(s) reached the change; [crash, shown by ASan on unstable, the one build with it](https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/61); crash, predates #2150, since fixed (recorded)

### valkey-io/valkey-bloom

[2026-11-09T16:12:32+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42652293537): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-json

[2026-11-09T16:15:14+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42652757575): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-search

[2026-11-09T16:23:53+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42654211459): 0 merge(s) tested, 0 without a code change.


## Totals

- Merges whose tests reached the change: 671 of 1016 tested (66%)
- Changed-line coverage: median 100% of 568 merge(s) measured, 46 under 50%
- Recall: 187 parents flagged of 285 fix merges
- Claude: $1093.72 in the last 24 hours, $7367.95 in the last 7 days, $7367.95 in all
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

None.
