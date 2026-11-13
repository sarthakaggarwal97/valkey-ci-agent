# Fuzzer walk

Updated 2026-11-13T06:19+00:00.

| Repository | Done | Remaining | Backlog cursor | Last 24 hours | Last 7 days (per day) |
|---|---|---|---|---|---|
| valkey-io/valkey | 1997 | 539 | #613 (2024-06-08) | 49 | 246.9 |
| valkey-io/valkey-bloom | 109 | 0 | ce516d0ef6e3 (2024-04-26) | 0 | 0.0 |
| valkey-io/valkey-json | 64 | 0 | c1423d7e4b38 (2024-11-25) | 0 | 0.3 |
| valkey-io/valkey-search | 673 | 0 | f8106e72e984 (2024-11-27) | 0 | 71.9 |

## Last runs

### valkey-io/valkey

[2026-11-13T06:14:32+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/43364255319): 20 merge(s) tested, 6 without a code change.

- [valkey-io/valkey#5104](https://github.com/valkey-io/valkey/pull/5104) ([tests](tests/valkey/5104-b4a6e7d69/)): Join server.log() arguments in linear time (#5104): 0 of 1 test(s) reached the change, coverage 100%
- [valkey-io/valkey#5376](https://github.com/valkey-io/valkey/pull/5376) ([tests](tests/valkey/5376-d5763ea44/)): Check Sentinel config dir is writable at startup (#5376): declined: The change adds a startup-only check inside sentinelCheckConfigFile(), called solely under `if (server.sentinel_mode)` (src/server.c:8138); Sentinel mode is selected by argv0/--sentinel, not by any server config. The fuzzer has no Sentinel deployment mode, valkey.action.commands' private s
- [valkey-io/valkey#5379](https://github.com/valkey-io/valkey/pull/5379) ([tests](tests/valkey/5379-89353577e/)): Skip the crash memory test under Valgrind and bound its region list (#5379): declined: The only server-code change bounds a fixed stack array in memtest_test_linux_anonymous_maps(), which runs exclusively inside the SIGSEGV crash-report handler (doFastMemoryTest) after the process is already crashing; it produces no command reply, no persisted or replicated state, and requir
- [valkey-io/valkey#638](https://github.com/valkey-io/valkey/pull/638) ([tests](tests/valkey/638-5a51bf504/)): Combine events to eliminate redundant kevent(2) calls (#638): declined: The change only touches src/ae_kqueue.c, the kqueue event-loop backend that is compiled solely on BSD/macOS (HAVE_KQUEUE); the fuzzer runs on Linux, where Valkey uses the epoll backend, so the changed code is never built or executed and no scenario can reach it. The change is also a pure s
- [valkey-io/valkey#634](https://github.com/valkey-io/valkey/pull/634) ([tests](tests/valkey/634-db6d3c113/)): Only primary with slots has the right to mark a node as failed (#634): 1 of 3 test(s) reached the change, coverage 87%
- [valkey-io/valkey#461](https://github.com/valkey-io/valkey/pull/461) ([tests](tests/valkey/461-a81c32079/)): Make cluster meet reliable under link failures (#461): 2 of 4 test(s) reached the change, coverage 92%
- [valkey-io/valkey#615](https://github.com/valkey-io/valkey/pull/615) ([tests](tests/valkey/615-93123f97a/)): Format yaml files (#615): declined: The change only reformats YAML whitespace and indentation in CI/workflow configuration files (.github/workflows/*.yml, dependabot.yml, codecov.yml) and adds a .config/format.yml formatter config. No Valkey source code, command behaviour, replication, persistence, or tooling is touched, so 
- [valkey-io/valkey#631](https://github.com/valkey-io/valkey/pull/631) ([tests](tests/valkey/631-6faa48a35/)): Don't initialize the key buffer in getKeysResult (#631): 0 of 7 test(s) reached the change, coverage 83%
- [valkey-io/valkey#637](https://github.com/valkey-io/valkey/pull/637) ([tests](tests/valkey/637-d5496e42b/)): Lua scripts promoted from eval to script load to avoid evict (#637): 3 of 3 test(s) reached the change, coverage 100%
- [valkey-io/valkey#644](https://github.com/valkey-io/valkey/pull/644) ([tests](tests/valkey/644-4c6bf30f5/)): Latency report: Rebranding and refine Dave dialog (#644): 4 of 4 test(s) reached the change, coverage 85%
- [valkey-io/valkey#651](https://github.com/valkey-io/valkey/pull/651) ([tests](tests/valkey/651-8a776c350/)): Fix potential infinite loop in `clusterNodeGetPrimary` (#651): 0 of 6 test(s) reached the change, coverage 100%; crash, predates #651, since fixed (recorded)
- [valkey-io/valkey#636](https://github.com/valkey-io/valkey/pull/636) ([tests](tests/valkey/636-d309b9b23/)): Make configs dir/dbfilename/cluster-config-file reject empty string (#636): 2 of 4 test(s) reached the change, coverage 63%
- [valkey-io/valkey#642](https://github.com/valkey-io/valkey/pull/642) ([tests](tests/valkey/642-76fc04168/)): represent cluster node flags with bitwise shift value (#642): declined: The change only rewrites the CLUSTER_NODE_* flag macros in cluster_legacy.h from decimal literals (1, 2, 4, ..., 1024) to the mathematically identical bit-shift expressions ((1 << 0) ... (1 << 10)). Every macro evaluates to the same value as before, no code using the flags is touched, and 
- [valkey-io/valkey#596](https://github.com/valkey-io/valkey/pull/596) ([tests](tests/valkey/596-b546dd26e/)): Allow CLUSTER NODES/INFO/MYID/MYSHARDID during loading state (#596): 2 of 3 test(s) reached the change
- [valkey-io/valkey#628](https://github.com/valkey-io/valkey/pull/628) ([tests](tests/valkey/628-4bb7cc471/)): Remove unnecessary clang-format off annotations (#628): declined: The change (#628) removes clang-format off/on comment annotations and reformats the enclosed code (spacing after commas, switch indentation, splitting fall-through case labels onto separate lines). Every hunk alters only whitespace, line breaks and comments; all expressions, case labels, f
- [valkey-io/valkey#583](https://github.com/valkey-io/valkey/pull/583) ([tests](tests/valkey/583-e65b2d235/)): Update rewriteConfigSaveOption function code to rewrite multiple save in one line. (#583): 0 of 2 test(s) reached the change, coverage 100%
- [valkey-io/valkey#612](https://github.com/valkey-io/valkey/pull/612) ([tests](tests/valkey/612-a3f1535b5/)): Fix misuse of safe iterators (#612): 0 of 9 test(s) reached the change, coverage 100%; crash, predates #612, since fixed (recorded)
- [valkey-io/valkey#620](https://github.com/valkey-io/valkey/pull/620) ([tests](tests/valkey/620-95a753b18/)): Add BSD license explicitly (#620): declined: The change only adds 'BSD 3-Clause License' header lines to the COPYING file. It is a documentation/licensing text edit with no code, config, command, replication, persistence, or tooling behaviour change, so no scenario can reach it and none is needed.
- [valkey-io/valkey#607](https://github.com/valkey-io/valkey/pull/607) ([tests](tests/valkey/607-71dd85dc5/)): src/Makefile: Link libatomic on POWER systems (#607): declined: The change only edits src/Makefile to link libatomic when the build machine is a POWER (ppc*) system, fixing a link-time failure on that architecture. It alters no runtime behaviour, no command, reply, replication, persistence or cluster path; on the CI's Linux x86 host the added Makefile 
- [valkey-io/valkey#592](https://github.com/valkey-io/valkey/pull/592) ([tests](tests/valkey/592-09b5825b2/)): Moving client->authenticated to a flag instead of an int (#592): 0 of 6 test(s) reached the change, coverage 84%

### valkey-io/valkey-bloom

[2026-11-13T06:19:27+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/43377755766): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-json

[2026-11-13T06:19:16+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/43377741788): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-search

[2026-11-12T07:37:39+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/43176563592): 0 merge(s) tested, 0 without a code change.


## Totals

- Merges whose tests reached the change: 856 of 1373 tested (62%)
- Changed-line coverage: median 100% of 879 merge(s) measured, 66 under 50%
- Recall: 225 parents flagged of 358 fix merges
- Claude: $179.26 in the last 24 hours, $7833.21 in the last 7 days, $10030.17 in all
- Issues filed: 73
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
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/128
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/129
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
- Model updates: 0 asked for, 0 with no model change, 0 rejected, 0 failed; 0 pull request(s)

## Retries

None.
