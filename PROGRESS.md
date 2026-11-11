# Fuzzer walk

Updated 2026-11-11T08:12+00:00.

| Repository | Done | Remaining | Backlog cursor | Last 24 hours | Last 7 days (per day) |
|---|---|---|---|---|---|
| valkey-io/valkey | 1723 | 799 | #1105 (2024-10-06) | 280 | 246.1 |
| valkey-io/valkey-bloom | 109 | 0 | ce516d0ef6e3 (2024-04-26) | 0 | 15.6 |
| valkey-io/valkey-json | 64 | 0 | c1423d7e4b38 (2024-11-25) | 1 | 9.1 |
| valkey-io/valkey-search | 672 | 0 | f8106e72e984 (2024-11-27) | 0 | 96.0 |

## Last runs

### valkey-io/valkey

[2026-11-11T08:12:58+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42968082740): 20 merge(s) tested, 6 without a code change.

- [valkey-io/valkey#1160](https://github.com/valkey-io/valkey/pull/1160) ([tests](tests/valkey/1160-64cfdf61e/)): Introduce connection context for Unix socket (#1160): declined: Pure refactor with no behaviour change: it moves the existing unixsocketperm/unixsocketgroup configs into a new serverUnixContextConfig struct and merges connListener's priv1/priv2 into one priv pointer, updating the two users (unix.c listen, rdma.c). Config names, types, flags, defaults a
- [valkey-io/valkey#1179](https://github.com/valkey-io/valkey/pull/1179) ([tests](tests/valkey/1179-a62d1f177/)): Fix false sharing issue between main thread and io-threads when access `used_memory_thread`. (#1179): declined: The change only pads and cache-line-aligns the static used_memory_thread array in zmalloc.c to avoid false sharing between the main thread's counter and the io-threads' counters. It is a performance optimization with no behaviour change: the per-thread memory accounting values, zmalloc_use
- [valkey-io/valkey#1075](https://github.com/valkey-io/valkey/pull/1075) ([tests](tests/valkey/1075-701ab7242/)): Remove the restriction that cli --cluster create requires at least 3 primary nodes (#1075): 2 of 2 test(s) reached the change, coverage 100%
- [valkey-io/valkey#1155](https://github.com/valkey-io/valkey/pull/1155) ([tests](tests/valkey/1155-136d0fd21/)): Add 'WithDictIndex' expiry API and update RANDOMKEY command (#1155): 4 of 7 test(s) reached the change, coverage 100%; crash, fixed by #1155 (recorded)
- [valkey-io/valkey#1168](https://github.com/valkey-io/valkey/pull/1168) ([tests](tests/valkey/1168-06cfe2c25/)): Improved hashing algorithm in luaS_newlstr (#1168): 2 of 2 test(s) reached the change
- [valkey-io/valkey#945](https://github.com/valkey-io/valkey/pull/945) ([tests](tests/valkey/945-b0f23df16/)): Refactor return and goto statements (#945): 0 of 18 test(s) reached the change, coverage 75%
- [valkey-io/valkey#1171](https://github.com/valkey-io/valkey/pull/1171) ([tests](tests/valkey/1171-247a8f23c/)): Fix FUNCTION KILL error message being displayed as SCRIPT KILL (#1171): 0 of 2 test(s) reached the change, coverage 0%
- [valkey-io/valkey#977](https://github.com/valkey-io/valkey/pull/977) ([tests](tests/valkey/977-dc05a327f/)): Take hz into account in activerehashing to avoid CPU spikes (#977): 0 of 3 test(s) reached the change, coverage 100%
- [valkey-io/valkey#1169](https://github.com/valkey-io/valkey/pull/1169) ([tests](tests/valkey/1169-e30ae762a/)): Rename z{malloc,calloc,realloc,free} into valkey_{malloc,calloc,realloc,free} (#1169): declined: The change only renames the z{malloc,calloc,realloc,free} linker symbols to valkey_{malloc,calloc,realloc,free} via #define in src/zmalloc.h and updates matching prototypes in the hdr_histogram shim, to avoid symbol collisions with zlib under static linking. All call sites are unchanged an
- [valkey-io/valkey#1163](https://github.com/valkey-io/valkey/pull/1163) ([tests](tests/valkey/1163-87b5e1346/)): Use listLast to replace listIndex -1 (#1163): declined: The change swaps listIndex(list, -1) for listLast(list) in getLastIncrAofName. listIndex(-1) returns list->tail (walking zero steps from the tail) and listLast is the macro ((l)->tail); both return NULL on an empty list, and the caller guarantees non-empty anyway. This is a semantics-prese
- [valkey-io/valkey#1154](https://github.com/valkey-io/valkey/pull/1154) ([tests](tests/valkey/1154-1a5c80fe9/)): Minor comments cleanup around replication.c (#1154): declined: The change only edits comments and an ASCII diagram in replication.c, plus the capitalization of a human-readable serverLog/systemd status string. No control flow, data handling, protocol bytes, or replies change, so there is no behaviour to exercise or regress.
- [valkey-io/valkey#1119](https://github.com/valkey-io/valkey/pull/1119) ([tests](tests/valkey/1119-262d970a5/)): Move prepareClientToWrite out of loop for HGETALL command (#1119): 0 of 2 test(s) reached the change, coverage 100%
- [valkey-io/valkey#1150](https://github.com/valkey-io/valkey/pull/1150) ([tests](tests/valkey/1150-ef971a34e/)): Correct the note details for deprecated config 'io-threads-do-reads' (#1150): declined: The change edits only a comment in valkey.conf, rewording the deprecation note for 'io-threads-do-reads'. No source code, config parsing, or runtime behaviour is modified, so there is nothing a fuzzer scenario could exercise or regress.
- [valkey-io/valkey#1142](https://github.com/valkey-io/valkey/pull/1142) ([tests](tests/valkey/1142-014219879/)): Fix typo last_procssed -> last_processed (#1142): declined: The change only renames a local variable (last_procssed -> last_processed) inside beforeSleep in src/server.c. The logic, control flow, and all observable behaviour are identical; it is a pure refactor/typo fix with no testable behaviour change, so no scenario is needed.
- [valkey-io/valkey#1138](https://github.com/valkey-io/valkey/pull/1138) ([tests](tests/valkey/1138-079f18ad9/)): Add io-threads-do-reads config to deprecated config table to have no effect. (#1138): 2 of 5 test(s) reached the change
- [valkey-io/valkey#1140](https://github.com/valkey-io/valkey/pull/1140) ([tests](tests/valkey/1140-9b8a06137/)): Fix empty response for ACL CAT category subcommand for module defined categories (#1140): declined: no proposed test validated
- [valkey-io/valkey#909](https://github.com/valkey-io/valkey/pull/909) ([tests](tests/valkey/909-cd8de095c/)): Add flush-before-load option for repl-diskless-load (#909): 2 of 5 test(s) reached the change, coverage 100%
- [valkey-io/valkey#1084](https://github.com/valkey-io/valkey/pull/1084) ([tests](tests/valkey/1084-1892f8a73/)): Add server log when module load fails with busy name (#1084): declined: The change only rewords a server log message in moduleLoad(): when OnLoad fails with ctx.module == NULL (busy module name), the log now says 'Module name is busy' instead of the generic text. The error reply to MODULE LOAD, the cleanup path (moduleFreeContext, dlclose, C_ERR), and all clie
- [valkey-io/valkey#1136](https://github.com/valkey-io/valkey/pull/1136) ([tests](tests/valkey/1136-cba8eaf4c/)): fix typo (#1136): declined: The change only fixes a typo in a comment inside objectSetLRUOrLFU in src/object.c ("This be handled" -> "This will be handled"). No executable code, config, protocol, or tool behaviour changes, so there is nothing to test.
- [valkey-io/valkey#1132](https://github.com/valkey-io/valkey/pull/1132) ([tests](tests/valkey/1132-e617bf2dd/)): Removing incorrect comment about a warning (#1132): declined: The change only restructures dead code in clusterNodeIterNext (src/cluster_legacy.c): an unreachable serverPanic is moved from a default case to after the switch statement and its comment removed. Both before and after, every reachable iterator type returns before the panic, so no command 

### valkey-io/valkey-bloom

[2026-11-11T07:44:18+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42981599314): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-json

[2026-11-11T08:00:17+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42983533478): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-search

[2026-11-11T08:03:25+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42983914019): 0 merge(s) tested, 0 without a code change.


## Totals

- Merges whose tests reached the change: 791 of 1249 tested (63%)
- Changed-line coverage: median 100% of 771 merge(s) measured, 62 under 50%
- Recall: 211 parents flagged of 334 fix merges
- Claude: $961.20 in the last 24 hours, $9094.55 in the last 7 days, $9094.55 in all
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
