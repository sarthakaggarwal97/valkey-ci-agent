# Fuzzer walk

Updated 2026-11-11T11:14+00:00.

| Repository | Done | Remaining | Backlog cursor | Last 24 hours | Last 7 days (per day) |
|---|---|---|---|---|---|
| valkey-io/valkey | 1779 | 743 | #1001 (2024-09-10) | 307 | 254.1 |
| valkey-io/valkey-bloom | 109 | 0 | ce516d0ef6e3 (2024-04-26) | 0 | 15.6 |
| valkey-io/valkey-json | 64 | 0 | c1423d7e4b38 (2024-11-25) | 1 | 9.1 |
| valkey-io/valkey-search | 672 | 0 | f8106e72e984 (2024-11-27) | 0 | 96.0 |

## Last runs

### valkey-io/valkey

[2026-11-11T11:14:53+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42997269033): 20 merge(s) tested, 8 without a code change.

- [valkey-io/valkey#1115](https://github.com/valkey-io/valkey/pull/1115): to retry, Claude Code failed (rc=1): API Error: Claude's response exceeded the 64000 output token maximum. To configure this behavior, set the CLAUDE_CODE_MAX_OUTPUT_TOKENS environment variable.
- [valkey-io/valkey#1039](https://github.com/valkey-io/valkey/pull/1039) ([tests](tests/valkey/1039-c15eee340/)): Changes `tcmalloc.h` header location (#1039): declined: The diff only changes the #include path for the tcmalloc header in src/zmalloc.h (google/tcmalloc.h -> gperftools/tcmalloc.h), effective only when compiled with USE_TCMALLOC. It is a build fix with no runtime behaviour change: no command, reply, replication, persistence or memory-accountin
- [valkey-io/valkey#1064](https://github.com/valkey-io/valkey/pull/1064) ([tests](tests/valkey/1064-56fba564b/)): Print an empty primary log when primary lost its last slot (#1064): 0 of 2 test(s) reached the change, coverage 83%
- [valkey-io/valkey#1053](https://github.com/valkey-io/valkey/pull/1053) ([tests](tests/valkey/1053-d07c29791/)): Use _Thread_local to solve threads.h build issue (#1053): declined: The change only replaces a conditional #include <threads.h>/__thread fallback with the equivalent C11 keyword _Thread_local for the thread_local macro in zmalloc.c. It is a build/portability fix with identical runtime semantics (the same thread-local storage class for the used_memory threa
- [valkey-io/valkey#1048](https://github.com/valkey-io/valkey/pull/1048) ([tests](tests/valkey/1048-56c90b78e/)): Fix a typo in the valkey.conf (#1048): declined: The change only fixes a typo in a comment line of valkey.conf (capitalized 'BE' changed to 'be'). Comments in the sample config are never parsed or acted on by the server; no code, default, or directive changed, so there is no behaviour to test.
- [valkey-io/valkey#1040](https://github.com/valkey-io/valkey/pull/1040) ([tests](tests/valkey/1040-ea7a7995e/)): Fix default value of primary-reboot-down-after-period in sentinel.conf (#1040): declined: The change only edits the example sentinel.conf shipped with the source, renaming the primary in the default 'SENTINEL primary-reboot-down-after-period' line from 'myprimary' to 'mymaster' so it matches the 'SENTINEL monitor mymaster' directive in the same file. No code path, command behav
- [valkey-io/valkey#1058](https://github.com/valkey-io/valkey/pull/1058) ([tests](tests/valkey/1058-7fab15795/)): Add log about old primary after myself failover (#1058): declined: The change only adds a serverLog(LL_NOTICE) line in clusterFailoverReplaceYourPrimary() recording the old primary when a replica promotes itself. It changes no command reply, state transition, replication, or persistence behavior. The logged values are safe: the function returns early when
- [valkey-io/valkey#1046](https://github.com/valkey-io/valkey/pull/1046) ([tests](tests/valkey/1046-4593dc2f0/)): Fix memory allocation for server databases (#1046): 2 of 2 test(s) reached the change, coverage 100%
- [valkey-io/valkey#1042](https://github.com/valkey-io/valkey/pull/1042) ([tests](tests/valkey/1042-ba71c7e56/)): Copy 'errno' and use copied value in the if check of retry in cluster migrate commands socket_err block. (#1042): 0 of 3 test(s) reached the change, coverage 100%
- [valkey-io/valkey#1007](https://github.com/valkey-io/valkey/pull/1007) ([tests](tests/valkey/1007-f89ff3137/)): Add --moduleapi option to better use runtest-moduleapi (#1007): declined: The change only edits the project's own test harness: runtest-moduleapi and tests/test_helper.tcl gain a --moduleapi flag that globs unit/moduleapi/*.tcl instead of hardcoding the test list. No server, tool, or module code changes; no runtime behaviour, reply, replication, persistence, or 
- [valkey-io/valkey#1029](https://github.com/valkey-io/valkey/pull/1029) ([tests](tests/valkey/1029-9f8185f5c/)): Update valkey-benchmark log output to reference 'server' instead of 'Redis' (#1029): 2 of 2 test(s) reached the change, coverage 100%
- [valkey-io/valkey#926](https://github.com/valkey-io/valkey/pull/926) ([tests](tests/valkey/926-17390383b/)): Replica flush the old data after RDB file is ok in disk-based replication (#926): 0 of 6 test(s) reached the change, coverage 90%
- [valkey-io/valkey#943](https://github.com/valkey-io/valkey/pull/943) ([tests](tests/valkey/943-09def3cf0/)): Improve code readability in dict.c (#943): declined: The change is a pure readability refactor of dict.c: internal helpers are renamed (underscore prefixes removed), relocated, and marked static, a stale TODO comment is deleted, and unit tests are updated to the new names. Every moved function body (dictRehashStep, dictNextExp, dictInit, dic
- [valkey-io/valkey#981](https://github.com/valkey-io/valkey/pull/981) ([tests](tests/valkey/981-dcc7678fc/)): Fix replica unable trigger migration when it received CLUSTER SETSLOT in advance (#981): 2 of 4 test(s) reached the change, coverage 100%; [check, appeared after #981](https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/125)
- [valkey-io/valkey#1025](https://github.com/valkey-io/valkey/pull/1025) ([tests](tests/valkey/1025-d090fbefd/)): Add the missing help output for new command: client capa redirect (#1025): 2 of 2 test(s) reached the change
- [valkey-io/valkey#822](https://github.com/valkey-io/valkey/pull/822) ([tests](tests/valkey/822-38457b732/)): Trigger a save of the cluster configuration file before shutting down (#822): 0 of 4 test(s) reached the change, coverage 100%
- [valkey-io/valkey#1023](https://github.com/valkey-io/valkey/pull/1023) ([tests](tests/valkey/1023-3513f2202/)): Make clang-format insert a newline at end of file if missing (#1023): declined: The change adds InsertNewlineAtEOF to src/.clang-format, a formatting/tooling configuration only. It alters no compiled code and no runtime behaviour of the server or tools, so no scenario can exercise it.
- [valkey-io/valkey#1013](https://github.com/valkey-io/valkey/pull/1013) ([tests](tests/valkey/1013-8cca11ac5/)): Fix wrong count for replica's tot-net-out (#1013): 2 of 3 test(s) reached the change, coverage 100%
- [valkey-io/valkey#995](https://github.com/valkey-io/valkey/pull/995) ([tests](tests/valkey/995-fa348e2e5/)): Optimize the per slot dictionary by checking for cluster mode earlier (#995): 0 of 6 test(s) reached the change, coverage 100%
- [valkey-io/valkey#1010](https://github.com/valkey-io/valkey/pull/1010) ([tests](tests/valkey/1010-c77e8f223/)): Added .git-blame-ignore-revs (#1010): declined: The change adds only a .git-blame-ignore-revs file, metadata for git-blame to skip formatting/rename commits. It contains no code, config, build, or behavior change in Valkey itself, so no scenario can reach it and none is needed.

### valkey-io/valkey-bloom

[2026-11-11T11:01:56+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/43008449466): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-json

[2026-11-11T10:44:12+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/43006016112): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-search

[2026-11-11T10:45:36+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/43006184427): 0 merge(s) tested, 0 without a code change.


## Totals

- Merges whose tests reached the change: 799 of 1266 tested (63%)
- Changed-line coverage: median 100% of 787 merge(s) measured, 63 under 50%
- Recall: 215 parents flagged of 338 fix merges
- Claude: $978.56 in the last 24 hours, $9215.20 in the last 7 days, $9215.20 in all
- Issues filed: 69
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
- [valkey-io/valkey#1115](https://github.com/valkey-io/valkey/pull/1115): attempt 2 of 3, Claude Code failed (rc=1): API Error: Claude's response exceeded the 64000 output token maximum. To configure this behavior, set the CLAUDE_CODE_MAX_OUTPUT_TOKENS environment variable.
