# Fuzzer walk

Updated 2026-11-20T13:54+00:00.

| Repository | Done | Remaining | Backlog cursor | Last 24 hours | Last 7 days (per day) |
|---|---|---|---|---|---|
| valkey-io/valkey | 2570 | 0 | #12477 (2023-08-14) | 23 | 71.7 |
| valkey-io/valkey-bloom | 109 | 0 | ce516d0ef6e3 (2024-04-26) | 0 | 0.0 |
| valkey-io/valkey-json | 66 | 0 | c1423d7e4b38 (2024-11-25) | 0 | 0.3 |
| valkey-io/valkey-search | 697 | 0 | f8106e72e984 (2024-11-27) | 0 | 2.3 |

## Last runs

### valkey-io/valkey

[2026-11-20T13:54:59+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/44729153757): 18 merge(s) tested, 5 without a code change.

- [valkey-io/valkey#5377](https://github.com/valkey-io/valkey/pull/5377) ([tests](tests/valkey/5377-e575e686d/)): Add p99.9 latency in valkey-benchmark result (#5377): 1 of 1 test(s) reached the change, coverage 100%
- [valkey-io/valkey#5436](https://github.com/valkey-io/valkey/pull/5436) ([tests](tests/valkey/5436-1a5d47e92/)): Report forkless progress INFO fields during diskless full sync (#5436): 1 of 3 test(s) reached the change, coverage 100%
- [valkey-io/valkey#5445](https://github.com/valkey-io/valkey/pull/5445) ([tests](tests/valkey/5445-8008cad20/)): Don't write the LFU counter back when saving a key (#5445): 0 of 3 test(s) reached the change, coverage 100%
- [valkey-io/valkey#4853](https://github.com/valkey-io/valkey/pull/4853) ([tests](tests/valkey/4853-40e00fcaf/)): Fix cluster CLI exit status on slot migration failure (#4853): 1 of 2 test(s) reached the change, coverage 50%; [crash, shown by ASan on unstable, the one build with it](https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/81)
- [valkey-io/valkey#5442](https://github.com/valkey-io/valkey/pull/5442) ([tests](tests/valkey/5442-97ff16e9e/)): Move forkless fallback counters to INFO debug (#5442): 2 of 3 test(s) reached the change
- [valkey-io/valkey#5435](https://github.com/valkey-io/valkey/pull/5435) ([tests](tests/valkey/5435-339d8610e/)): Don't free the client that is executing a command when a blocked client's re-executed command evicts clients (#5435): 0 of 7 test(s) reached the change, coverage 80%
- [valkey-io/valkey#5448](https://github.com/valkey-io/valkey/pull/5448) ([tests](tests/valkey/5448-8c82072a6/)): Internal API for objectGetLFUFrequencyReadOnly (#5448): declined: The change only extracts already-existing inline code in rdbSaveKeyValuePair (lfu_getFrequency(objectGetLRU(val), &freq)) into a new internal helper objectGetLFUFrequencyReadOnly with an identical body, plus a server.h declaration and comment updates. The helper is called from the same sin
- [valkey-io/valkey#5299](https://github.com/valkey-io/valkey/pull/5299) ([tests](tests/valkey/5299-0bc71f0ea/)): Fix build of select and evport ae backends: undeclared UNUSED() (#5299): declined: The change is a build fix only: it swaps the undeclared UNUSED(state) macro for AE_NOTUSED(state) in the aeApiGetPollFd stubs of ae_select.c and ae_evport.c so those event-loop backends compile again. Both macros are identical no-op void casts, so compiled behaviour is unchanged; moreover 
- [valkey-io/valkey#5438](https://github.com/valkey-io/valkey/pull/5438) ([tests](tests/valkey/5438-0be477195/)): Skip unblocked clients in processPendingCommandAndInputBuffer (#5438): 0 of 3 test(s) reached the change, coverage 100%
- [valkey-io/valkey#5461](https://github.com/valkey-io/valkey/pull/5461) ([tests](tests/valkey/5461-1fae4b43d/)): Remove the old RDB version forkless fallback (#5461): declined: no proposed test validated
- [valkey-io/valkey#5462](https://github.com/valkey-io/valkey/pull/5462) ([tests](tests/valkey/5462-355aeb471/)): bgiteration: fix for flush/swap/termination interaction (#5462): 0 of 6 test(s) reached the change, coverage 100%
- [valkey-io/valkey#4951](https://github.com/valkey-io/valkey/pull/4951) ([tests](tests/valkey/4951-b0be9f97b/)): Fix MSETEX key discovery in cluster mode (#4951): 2 of 4 test(s) reached the change, coverage 100%; crash, fixed by #4951 (recorded)
- [valkey-io/valkey#4439](https://github.com/valkey-io/valkey/pull/4439) ([tests](tests/valkey/4439-06cded9e4/)): Support system jemalloc with MALLOC=system-jemalloc (#4439): declined: The change is purely a build-system/CI/docs/test-harness change: it adds a MALLOC=system-jemalloc build option to the Makefile and CMake files, renames and extends a CI job, documents the option in README.md, and makes two TCL tests skip gracefully on jemalloc builds without defrag support
- [valkey-io/valkey#5460](https://github.com/valkey-io/valkey/pull/5460) ([tests](tests/valkey/5460-5c8a56e61/)): bgiteration: handle replication of HDEL during field expiration (#5460): 0 of 6 test(s) reached the change, coverage 100%
- [valkey-io/valkey#5366](https://github.com/valkey-io/valkey/pull/5366) ([tests](tests/valkey/5366-6357e3dc4/)): Fix double mutex unlock when shrinking IO threads (#5366): 0 of 5 test(s) reached the change
- [valkey-io/valkey#5417](https://github.com/valkey-io/valkey/pull/5417) ([tests](tests/valkey/5417-16debeace/)): Abort dual-channel sync if the primary link breaks while streaming (#5417): 0 of 3 test(s) reached the change, coverage 100%
- [valkey-io/valkey#5468](https://github.com/valkey-io/valkey/pull/5468) ([tests](tests/valkey/5468-8c9c4ac35/)): Don't resend data to a close_asap client after an IO-thread write (#5468): 0 of 6 test(s) reached the change, coverage 100%
- [valkey-io/valkey#5112](https://github.com/valkey-io/valkey/pull/5112) ([tests](tests/valkey/5112-6c11c7fb2/)): Pause rehashing of path hash payloads during background iteration (#5112): 0 of 3 test(s) reached the change, coverage 100%; crash, fixed by #5112 (recorded)

### valkey-io/valkey-bloom

[2026-11-16T18:28:09+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/43928943942): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-json

[2026-11-16T18:28:05+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/43928932190): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-search

[2026-11-16T18:28:04+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/43928938170): 0 merge(s) tested, 0 without a code change.


## Totals

- Merges whose tests reached the change: 1003 of 1610 tested (62%)
- Changed-line coverage: median 100% of 1079 merge(s) measured, 79 under 50%
- Recall: 266 parents flagged of 417 fix merges
- Claude: $105.56 in the last 24 hours, $1499.82 in the last 7 days, $11808.07 in all
- Issues filed: 85
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
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/145
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/146
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/147
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/153
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/156
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/157
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/162
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/164
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/165
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/166
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/167
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/168
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
- Model updates: 2 asked for, 2 with no model change, 0 rejected, 0 failed; 0 pull request(s)

## Retries

None.
