# Fuzzer walk

Updated 2026-11-09T09:10+00:00.

| Repository | Done | Remaining | Backlog cursor | Last 24 hours | Last 7 days (per day) |
|---|---|---|---|---|---|
| valkey-io/valkey | 1138 | 1369 | #2365 (2025-07-17) | 251 | 162.6 |
| valkey-io/valkey-bloom | 109 | 0 | ce516d0ef6e3 (2024-04-26) | 0 | 15.6 |
| valkey-io/valkey-json | 62 | 0 | c1423d7e4b38 (2024-11-25) | 0 | 8.9 |
| valkey-io/valkey-search | 669 | 0 | f8106e72e984 (2024-11-27) | 94 | 95.6 |

## Last runs

### valkey-io/valkey

[2026-11-09T08:56:04+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42581126511): 20 merge(s) tested, 5 without a code change.

- [valkey-io/valkey#2409](https://github.com/valkey-io/valkey/pull/2409) ([tests](tests/valkey/2409-1d20026b9/)): Dictionary Implementation for Migrating and Importing slots (#2409): 0 of 6 test(s) reached the change, coverage 81%
- [valkey-io/valkey#2391](https://github.com/valkey-io/valkey/pull/2391) ([tests](tests/valkey/2391-a50690f02/)): Update swapdb command comment to mention why cluster mode is not allowed (#2391): declined: The diff only edits a source comment above swapdbCommand's cluster-mode check in src/db.c; no executable code, reply, or configuration changes, so there is no behaviour to test and a compared run could never show a difference.
- [valkey-io/valkey#2395](https://github.com/valkey-io/valkey/pull/2395) ([tests](tests/valkey/2395-36bd5bf92/)): Cleanup and rename isExpiryTableValidForSamplingCb to expiryTableShouldSkipForSamplingCb (#2395): declined: Pure refactor with no behaviour change: the callback is renamed and its return values changed from C_ERR/C_OK (-1/0) to 1/0, but its only caller, kvstoreScan at src/kvstore.c:400, uses the result as a boolean (skip_cb && skip_cb(ht)), and -1 and 1 are equally truthy while 0 is unchanged. T
- [valkey-io/valkey#2381](https://github.com/valkey-io/valkey/pull/2381) ([tests](tests/valkey/2381-34480ac35/)): Initialize rdbstate.key_type to -1 at the beginning (#2381): 1 of 1 test(s) reached the change, coverage 100%
- [valkey-io/valkey#2417](https://github.com/valkey-io/valkey/pull/2417) ([tests](tests/valkey/2417-7d63bc5ba/)): Increase TID count to accomodate valkey-search reader/writer pools (#2417): declined: The change only bumps TIDS_MAX_SIZE from 50 to 150 in src/debug.c, the buffer the crash-report signal handler uses in writeStacktraces()/get_ready_to_signal_threads_tids() to enumerate thread TIDs when dumping stack traces. It changes behavior only when the process has more than 50 threads
- [valkey-io/valkey#2382](https://github.com/valkey-io/valkey/pull/2382) ([tests](tests/valkey/2382-e16fc8d8c/)): Fix check-rdb --stats crash with empty RDB (#2382): 2 of 2 test(s) reached the change, coverage 100%; crash, fixed by #2382 (recorded)
- [valkey-io/valkey#2374](https://github.com/valkey-io/valkey/pull/2374) ([tests](tests/valkey/2374-43c4af0b0/)): Remove obsolete BLOCKED_WAITAOF comment (#2374): declined: The change edits only a comment in src/server.h (removing an obsolete mention of BLOCKED_WAITAOF above the blockingState fields). No code, data structure, or behaviour changes; the compiled binary is identical in behaviour to the parent, so no scenario can reach or distinguish the change.
- [valkey-io/valkey#2400](https://github.com/valkey-io/valkey/pull/2400) ([tests](tests/valkey/2400-3b12132ac/)): Rename trace bgsave type to rdb and aof (#2400): declined: The change only renames LTTng tracepoint providers (valkey_bgsave -> valkey_rdb), adds an aof fork tracepoint instance, and routes serverFork's trace by child type, plus build-file and README updates. All of it is behind the USE_LTTNG compile flag; in normal builds latencyTraceIfNeeded is 
- [valkey-io/valkey#2378](https://github.com/valkey-io/valkey/pull/2378) ([tests](tests/valkey/2378-1fbf5fb1f/)): support negative filtering for client command filters (#2378): 4 of 4 test(s) reached the change, coverage 98%
- [valkey-io/valkey#1784](https://github.com/valkey-io/valkey/pull/1784) ([tests](tests/valkey/1784-025f416b7/)): Save to Disk in Bio Thread and refactor readSyncBulkPayload (#1784): 0 of 9 test(s) reached the change, coverage 80%
- [valkey-io/valkey#2397](https://github.com/valkey-io/valkey/pull/2397) ([tests](tests/valkey/2397-8df8c84ff/)): Same number of hyphens for summary output (#2397): 1 of 1 test(s) reached the change, coverage 100%
- [valkey-io/valkey#2388](https://github.com/valkey-io/valkey/pull/2388) ([tests](tests/valkey/2388-ec58a4843/)): Add helper function for padded pointer copy (#2388): declined: The change only extracts already-identical inline code (memcpy of a pointer plus zero padding on 32-bit) into a new helper writePointerWithPadding(), used by encodeFailureReportKey() and encodeTimeoutKey(). The bytes written to the radix-tree keys are identical before and after on both 32-
- [valkey-io/valkey#2370](https://github.com/valkey-io/valkey/pull/2370) ([tests](tests/valkey/2370-a481fe228/)): Update clusterMoveNodeSlots to also move importing slots and migrating slots (#2370): 1 of 3 test(s) reached the change, coverage 92%; check, introduced and since fixed (recorded)
- [valkey-io/valkey#2277](https://github.com/valkey-io/valkey/pull/2277) ([tests](tests/valkey/2277-2a44506b5/)): Optimize cluster failure report tracking with RAX (#2277): 0 of 5 test(s) reached the change, coverage 100%
- [valkey-io/valkey#2379](https://github.com/valkey-io/valkey/pull/2379) ([tests](tests/valkey/2379-f21beaf4f/)): Defrag should only skip non-existent dbs, not empty dbs (#2379): 0 of 3 test(s) reached the change, coverage 100%
- [valkey-io/valkey#2364](https://github.com/valkey-io/valkey/pull/2364) ([tests](tests/valkey/2364-a73953110/)): Fixing open comments on #1920 PR (#2364): declined: The change is confined to the TLS certificate auto-authentication branch of clientAcceptHandler, reachable only when a client connects over TLS with tls-auth-clients-user enabled and presents an X.509 certificate whose CN names an ACL user (connGetPeerUsername returns non-NULL only for the
- [valkey-io/valkey#2154](https://github.com/valkey-io/valkey/pull/2154) ([tests](tests/valkey/2154-f21015c90/)): Limiting the new reconnections for failed nodes (#2154): 0 of 5 test(s) reached the change, coverage 93%
- [valkey-io/valkey#2292](https://github.com/valkey-io/valkey/pull/2292) ([tests](tests/valkey/2292-89e0e6496/)): Auto-failover on shutdown unified config (#2292): 3 of 8 test(s) reached the change, coverage 100%
- [valkey-io/valkey#2360](https://github.com/valkey-io/valkey/pull/2360) ([tests](tests/valkey/2360-e5956d0c2/)): Fix client tracking memory overhead calculation (#2360): 0 of 2 test(s) reached the change, coverage 100%
- [valkey-io/valkey#2286](https://github.com/valkey-io/valkey/pull/2286) ([tests](tests/valkey/2286-0999007f5/)): Include command fullname in error message when returning NO_MULTI error (#2286): 2 of 2 test(s) reached the change, coverage 100%

### valkey-io/valkey-bloom

[2026-11-09T09:01:48+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42590163419): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-json

[2026-11-09T09:04:55+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42590556935): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-search

[2026-11-09T09:10:50+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42591288669): 0 merge(s) tested, 0 without a code change.


## Totals

- Merges whose tests reached the change: 646 of 962 tested (67%)
- Changed-line coverage: median 100% of 517 merge(s) measured, 38 under 50%
- Recall: 177 parents flagged of 269 fix merges
- Claude: $1024.04 in the last 24 hours, $6954.82 in the last 7 days, $6954.82 in all
- Issues filed: 63
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
