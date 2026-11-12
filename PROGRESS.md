# Fuzzer walk

Updated 2026-11-12T06:00+00:00.

| Repository | Done | Remaining | Backlog cursor | Last 24 hours | Last 7 days (per day) |
|---|---|---|---|---|---|
| valkey-io/valkey | 1948 | 582 | #351 (2024-06-27) | 251 | 272.0 |
| valkey-io/valkey-bloom | 109 | 0 | ce516d0ef6e3 (2024-04-26) | 0 | 11.6 |
| valkey-io/valkey-json | 64 | 0 | c1423d7e4b38 (2024-11-25) | 0 | 4.3 |
| valkey-io/valkey-search | 673 | 0 | f8106e72e984 (2024-11-27) | 1 | 96.1 |

## Last runs

### valkey-io/valkey

[2026-11-12T05:14:03+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/43135168590): 20 merge(s) tested, 5 without a code change.

- [valkey-io/valkey#725](https://github.com/valkey-io/valkey/pull/725) ([tests](tests/valkey/725-548b4e0ea/)): Calculate the actual mask to be removed in the eventloop before aeApiDelEvent (#725): 0 of 4 test(s) reached the change, coverage 100%
- [valkey-io/valkey#758](https://github.com/valkey-io/valkey/pull/758) ([tests](tests/valkey/758-bbfd04189/)): Async IO threads (#758): 2 of 25 test(s) reached the change, coverage 81%
- [valkey-io/valkey#755](https://github.com/valkey-io/valkey/pull/755) ([tests](tests/valkey/755-5f0ccf147/)): Remove duplicate definition of UNUSED(V) (#755): declined: The change only deletes a duplicate `#define UNUSED(V)` macro inside the `#ifdef SERVER_TEST` section of src/dict.c. The identical macro remains defined elsewhere, and this block is only compiled for the server's internal unit tests, not for normal builds. No command, reply, replication, p
- [valkey-io/valkey#741](https://github.com/valkey-io/valkey/pull/741) ([tests](tests/valkey/741-f2bbd1ff0/)): Fix minor memory leak in clusterLoadConfig (#741): declined: The change only adds sdsfreesplitres(argv, argc) before a goto fmterr in clusterLoadConfig; the fmterr path immediately logs and calls exit(1), so the freed memory would have been reclaimed by process termination anyway. There is no observable behavior change on any input — same log, same 
- [valkey-io/valkey#706](https://github.com/valkey-io/valkey/pull/706) ([tests](tests/valkey/706-168037884/)): Update redis keyword to valkey in some sentinel functions (Redis Legacy) (#706): declined: The change is a pure rename refactor in src/sentinel.c: sentinelRedisInstance and related functions/types are renamed to sentinelValkeyInstance, redisAeEvents to ValkeyAeEvents, and the REDIS_SENTINEL_PORT macro to VALKEY_SENTINEL_PORT with the same value (26379). Every hunk is an identifi
- [valkey-io/valkey#723](https://github.com/valkey-io/valkey/pull/723) ([tests](tests/valkey/723-6bf1d02ed/)): Nested MULTI or WATCH in MULTI now will abort the transaction (#723): 2 of 2 test(s) reached the change
- [valkey-io/valkey#735](https://github.com/valkey-io/valkey/pull/735) ([tests](tests/valkey/735-2d6791bb1/)): Use clusterNodeIsVotingPrimary function to check the right (#735): declined: This is a semantics-preserving refactor: the inline checks `nodeIsReplica(node) || node->numslots == 0` in clearNodeFailureIfNeeded and clusterSendFailoverAuthIfNeeded are replaced by `!clusterNodeIsVotingPrimary(node)`, whose definition ((flags & CLUSTER_NODE_PRIMARY) && numslots) is logi
- [valkey-io/valkey#541](https://github.com/valkey-io/valkey/pull/541) ([tests](tests/valkey/541-8faf2788a/)): Embed key into dict entry (#541): 4 of 12 test(s) reached the change, coverage 99%
- [valkey-io/valkey#731](https://github.com/valkey-io/valkey/pull/731) ([tests](tests/valkey/731-1ea49e584/)): Make valkey compatible with redis-sentinel to start sentinel (#731): declined: The change only adds a second strstr on argv[0]'s basename so that a redis-sentinel symlink starts Sentinel mode. The trigger is the name the binary is invoked under, which nothing in the fuzzer controls: the runner supplies the build and launches it as valkey-server, valkey.action.tools r
- [valkey-io/valkey#674](https://github.com/valkey-io/valkey/pull/674) ([tests](tests/valkey/674-3323e422a/)): Introduce thread-local storage variable to update thread's own used_memory and sum when reading to reduce atomic contention. (#674): 0 of 6 test(s) reached the change, coverage 81%
- [valkey-io/valkey#729](https://github.com/valkey-io/valkey/pull/729) ([tests](tests/valkey/729-0cc16d029/)): Fix wrong reserved bits in ClientFlags (#729): declined: The change only corrects the ClientFlags reserved bitfield from 11 to 10 bits so the 54 named flag bits plus reserved fit exactly in the one uint64_t that is unioned with raw_flag. The reserved field is never read or written anywhere in the codebase, no code serializes ClientFlags or depen
- [valkey-io/valkey#693](https://github.com/valkey-io/valkey/pull/693) ([tests](tests/valkey/693-fa01a2936/)): Allow Module authentication to succeed when cluster is down (#693): 0 of 3 test(s) reached the change, coverage 100%
- [valkey-io/valkey#718](https://github.com/valkey-io/valkey/pull/718) ([tests](tests/valkey/718-9f4f6036b/)): Restore comments for client flags (#718): declined: The diff only restores inline comments on the ClientFlags bitfield in src/server.h; every field's name, width and order is unchanged, so no compiled behaviour differs between the change and its parent. A comment-only change needs no scenario.
- [valkey-io/valkey#721](https://github.com/valkey-io/valkey/pull/721) ([tests](tests/valkey/721-752b6ee8f/)): Avoid compilation error oin valkey-cli (#721): declined: The change only adds an unreachable `return 0;` after an `assert(0)` in clusterManagerNodePrimaryRandom() in valkey-cli.c to fix a compilation warning/error about control reaching the end of a non-void function. The added statement can never execute at runtime, so no behavior of valkey-cli
- [valkey-io/valkey#614](https://github.com/valkey-io/valkey/pull/614) ([tests](tests/valkey/614-e4c1f6d45/)): Replace client flags to bitfield (#614): 0 of 26 test(s) reached the change, coverage 61%
- [valkey-io/valkey#702](https://github.com/valkey-io/valkey/pull/702) ([tests](tests/valkey/702-7415a576a/)): Add prompt when Ctrl-C pressed (#702): declined: The change adds only an informational printf (plus fflush) to valkey-cli's interactive Ctrl-C handler in pubsub mode; the reconnect logic and all state handling are unchanged. The branch is reachable only when SIGINT is delivered to an interactive valkey-cli REPL session (sigIntHandler is 
- [valkey-io/valkey#710](https://github.com/valkey-io/valkey/pull/710) ([tests](tests/valkey/710-2979fe606/)): CLUSTER SLOT-STATS ORDERBY when stats are the same, compare by slot in ascending order (#710): 0 of 1 test(s) reached the change, coverage 100%
- [valkey-io/valkey#709](https://github.com/valkey-io/valkey/pull/709) ([tests](tests/valkey/709-518f0bf79/)): Fix limit undefined behavior crash in CLUSTER SLOT-STATS (#709): 2 of 2 test(s) reached the change, coverage 100%
- [valkey-io/valkey#711](https://github.com/valkey-io/valkey/pull/711) ([tests](tests/valkey/711-7f7ef9a3f/)): Update availability-zone to use the flag instead of the number 0 (#711): declined: The change replaces the literal 0 with the macro ALLOW_EMPTY_STRING in the availability-zone config definition in src/config.c; ALLOW_EMPTY_STRING is #defined as 0 (src/config.c:1900), so the compiled behavior is identical. This is a readability-only refactor with no behavior change, so no
- [valkey-io/valkey#581](https://github.com/valkey-io/valkey/pull/581) ([tests](tests/valkey/581-4fbe31ab8/)): Fix the TLS and REPS issues about CLUSTER SLOTS cache (#581): 0 of 2 test(s) reached the change, coverage 100%

### valkey-io/valkey-bloom

[2026-11-12T06:00:53+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/43165073409): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-json

[2026-11-12T06:00:53+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/43165086384): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-search

[2026-11-12T05:33:29+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/43162087021): 0 merge(s) tested, 0 without a code change.


## Totals

- Merges whose tests reached the change: 840 of 1347 tested (62%)
- Changed-line coverage: median 100% of 858 merge(s) measured, 66 under 50%
- Recall: 225 parents flagged of 355 fix merges
- Claude: $840.88 in the last 24 hours, $9565.38 in the last 7 days, $9850.90 in all
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
