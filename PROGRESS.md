# Fuzzer walk

Updated 2026-11-11T09:41+00:00.

| Repository | Done | Remaining | Backlog cursor | Last 24 hours | Last 7 days (per day) |
|---|---|---|---|---|---|
| valkey-io/valkey | 1752 | 770 | #1044 (2024-09-23) | 280 | 250.3 |
| valkey-io/valkey-bloom | 109 | 0 | ce516d0ef6e3 (2024-04-26) | 0 | 15.6 |
| valkey-io/valkey-json | 64 | 0 | c1423d7e4b38 (2024-11-25) | 1 | 9.1 |
| valkey-io/valkey-search | 672 | 0 | f8106e72e984 (2024-11-27) | 0 | 96.0 |

## Last runs

### valkey-io/valkey

[2026-11-11T09:41:13+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42985281313): 20 merge(s) tested, 10 without a code change.

- [valkey-io/valkey#1105](https://github.com/valkey-io/valkey/pull/1105) ([tests](tests/valkey/1105-b5eb79307/)): Eliminate hashTypeIterator memory allocation by assigning it on stack (#1105): 0 of 9 test(s) reached the change, coverage 90%
- [valkey-io/valkey#1101](https://github.com/valkey-io/valkey/pull/1101) ([tests](tests/valkey/1101-a1cc7c263/)): Reuse `obey_client` variable in `processCommand()` function (#1101): declined: The change only caches the result of mustObeyClient(c) in a const local and reuses it at the two later call sites in processCommand(). mustObeyClient() depends only on c->id and c->flag.primary, and nothing between the computation and the former call sites (auth check, CMD_NO_MULTI check, 
- [valkey-io/valkey#1125](https://github.com/valkey-io/valkey/pull/1125) ([tests](tests/valkey/1125-b96f8813b/)): Add tags into .gitignore (#1125): declined: The change adds a single line ('tags') to .gitignore so that ctags index files are ignored by git. It touches no source code, build script, or configuration; no Valkey binary or runtime behaviour changes, so no scenario can reach it and none is needed.
- [valkey-io/valkey#1118](https://github.com/valkey-io/valkey/pull/1118) ([tests](tests/valkey/1118-c8aaceed4/)): Correct the typo in valkey.conf file (#1118): declined: The change edits only a comment in valkey.conf, fixing a duplicated word ('or or' -> '"never"') in the transparent hugepage section's documentation. Comment lines are ignored by the config parser, so no server behaviour, reply, persistence, replication, or tooling path changes. No scenario
- [valkey-io/valkey#1102](https://github.com/valkey-io/valkey/pull/1102) ([tests](tests/valkey/1102-d8cd3527b/)): Removing Redis from internal lua function names and comments (#1102): 7 of 7 test(s) reached the change, coverage 100%
- [valkey-io/valkey#1126](https://github.com/valkey-io/valkey/pull/1126) ([tests](tests/valkey/1126-6a8540cef/)): Fix some unitialized fields in `client` struct (#1126): declined: The change only initializes client->io_last_reply_block and client->io_last_bufpos in createClient. Every read of these fields happens exclusively in the I/O-thread write path (ioThreadWriteToClient -> _writeToClient/writevToClient), which can only run after trySendWriteToIOThreads has unc
- [valkey-io/valkey#1122](https://github.com/valkey-io/valkey/pull/1122) ([tests](tests/valkey/1122-dcac3e149/)): Fix undefined-santitizer warning in rax test (#1122): declined: The change only adds a (uint32_t) cast in src/unit/test_rax.c, a unit-test helper, to fix a UB-sanitizer warning in the test binary. No server, command, replication, or persistence code changes; no runtime behaviour differs between the builds, so there is nothing a fuzzer scenario could ex
- [valkey-io/valkey#1115](https://github.com/valkey-io/valkey/pull/1115): to retry, Claude Code failed (rc=1): API Error: Claude's response exceeded the 64000 output token maximum. To configure this behavior, set the CLAUDE_CODE_MAX_OUTPUT_TOKENS environment variable.
- [valkey-io/valkey#688](https://github.com/valkey-io/valkey/pull/688) ([tests](tests/valkey/688-f85d8bfde/)): Rax size tracking (#688): 6 of 6 test(s) reached the change, coverage 82%
- [valkey-io/valkey#1100](https://github.com/valkey-io/valkey/pull/1100) ([tests](tests/valkey/1100-613e4e028/)): Update keyspace notifications link to valkey.io in code comment (#1100): declined: The change edits only a comment in src/notify.c, replacing a redis.io documentation link with valkey.io. No executable code, reply, config, or persistence/replication behaviour changes, so there is nothing a scenario could reach or distinguish between the two builds.
- [valkey-io/valkey#1092](https://github.com/valkey-io/valkey/pull/1092) ([tests](tests/valkey/1092-ac569c09f/)): Create empty lua tables with specified initial capacity as much as possible (#1092): 0 of 2 test(s) reached the change, coverage 100%
- [valkey-io/valkey#1097](https://github.com/valkey-io/valkey/pull/1097) ([tests](tests/valkey/1097-c873287d1/)): avoid double close on replica main channel (#1097): 0 of 7 test(s) reached the change, coverage 0%
- [valkey-io/valkey#1089](https://github.com/valkey-io/valkey/pull/1089) ([tests](tests/valkey/1089-a7cbca406/)): RDMA: Support .is_local method (#1089): declined: The change only adds an .is_local implementation to the RDMA connection type in src/rdma.c. That code is compiled only with BUILD_RDMA=module (not in the default build the fuzzer uses), is loaded via MODULE LOAD (refused in sequences), and runs only on a live RDMA connection, which needs R
- [valkey-io/valkey#1057](https://github.com/valkey-io/valkey/pull/1057) ([tests](tests/valkey/1057-a37dee4b3/)): Change return value of aeTimeProc callback function to long long. (#1057): declined: The change only widens the aeTimeProc return type from int to long long, removing a 32-bit truncation of the reschedule interval in processTimeEvents (src/ae.c:366-375). Of all callbacks, only moduleTimerHandler (src/module.c:9116-9161) can return a value above INT_MAX, and only when the n
- [valkey-io/valkey#1074](https://github.com/valkey-io/valkey/pull/1074) ([tests](tests/valkey/1074-983bb5110/)): Fix RDMA build dependence (#1074): declined: The change is a one-line Makefile fix: the valkey-rdma.so target's prerequisite is corrected from the stale $(REDIS_SERVER_NAME) variable to $(SERVER_NAME). It only affects build dependency ordering for the optional RDMA module and changes no runtime behaviour, command replies, replication
- [valkey-io/valkey#1072](https://github.com/valkey-io/valkey/pull/1072) ([tests](tests/valkey/1072-99865b197/)): Fix bug for CLUSTER SLOTS from EVAL over TLS (#1072): declined: The fix changes clusterCommandSlots' conn_type selection only for a caller with no real connection on the executing client: CLUSTER SLOTS via EVAL/module from a TLS client, or a module call with tls-cluster set. Over plain TCP or a unix socket the old code (fake client's conn==NULL -> not 
- [valkey-io/valkey#1073](https://github.com/valkey-io/valkey/pull/1073) ([tests](tests/valkey/1073-6b3a90e40/)): Added new reformat commit to .git-blame-ignore-revs (#1073): declined: The change only appends a reformat commit hash to .git-blame-ignore-revs, a git tooling metadata file used by `git blame`. No source code, configuration, protocol, or build behavior changes, so no runtime path exists for a scenario to exercise.
- [valkey-io/valkey#1063](https://github.com/valkey-io/valkey/pull/1063) ([tests](tests/valkey/1063-80fcbd3fe/)): Fix module / script call CLUSTER SLOTS / SHARDS fake client check crash (#1063): 6 of 6 test(s) reached the change, coverage 75%; crash, fixed by #1063 (recorded)
- [valkey-io/valkey#1067](https://github.com/valkey-io/valkey/pull/1067) ([tests](tests/valkey/1067-6e0216471/)): Trigger the election as soon as possible when doing a forced manual failover (#1067): 0 of 3 test(s) reached the change, coverage 100%
- [valkey-io/valkey#1045](https://github.com/valkey-io/valkey/pull/1045) ([tests](tests/valkey/1045-af811748e/)): clang-format: set ColumnLimit to 0 and reformat (#1045): declined: The change is a pure clang-format reformat: .clang-format's ColumnLimit is set to 0 and ~60 source files are mechanically rewrapped (whitespace after commas, macro line-continuation realignment, removal of clang-format off/on guards, re-indented string arrays with identical string contents

### valkey-io/valkey-bloom

[2026-11-11T09:28:31+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42995396892): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-json

[2026-11-11T09:08:25+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42992551529): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-search

[2026-11-11T09:10:19+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42992798859): 0 merge(s) tested, 0 without a code change.


## Totals

- Merges whose tests reached the change: 794 of 1256 tested (63%)
- Changed-line coverage: median 100% of 778 merge(s) measured, 63 under 50%
- Recall: 212 parents flagged of 335 fix merges
- Claude: $906.77 in the last 24 hours, $9143.41 in the last 7 days, $9143.41 in all
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
- [valkey-io/valkey#1115](https://github.com/valkey-io/valkey/pull/1115): attempt 1 of 3, Claude Code failed (rc=1): API Error: Claude's response exceeded the 64000 output token maximum. To configure this behavior, set the CLAUDE_CODE_MAX_OUTPUT_TOKENS environment variable.
