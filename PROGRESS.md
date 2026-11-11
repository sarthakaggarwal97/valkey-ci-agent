# Fuzzer walk

Updated 2026-11-11T14:56+00:00.

| Repository | Done | Remaining | Backlog cursor | Last 24 hours | Last 7 days (per day) |
|---|---|---|---|---|---|
| valkey-io/valkey | 1825 | 698 | #951 (2024-08-27) | 319 | 260.7 |
| valkey-io/valkey-bloom | 109 | 0 | ce516d0ef6e3 (2024-04-26) | 0 | 15.6 |
| valkey-io/valkey-json | 64 | 0 | c1423d7e4b38 (2024-11-25) | 1 | 9.1 |
| valkey-io/valkey-search | 672 | 0 | f8106e72e984 (2024-11-27) | 0 | 96.0 |

## Last runs

### valkey-io/valkey

[2026-11-11T14:56:03+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/43030160751): 20 merge(s) tested, 6 without a code change.

- [valkey-io/valkey#877](https://github.com/valkey-io/valkey/pull/877) ([tests](tests/valkey/877-5fdb47c2e/)): Add configuration hide-user-data-from-log to hide user data from server logs (#877): 2 of 11 test(s) reached the change, coverage 33%
- [valkey-io/valkey#966](https://github.com/valkey-io/valkey/pull/966) ([tests](tests/valkey/966-32116d09b/)): Use metadata to handle the reference relationship between kvstore and dict (#966): 0 of 3 test(s) reached the change, coverage 94%
- [valkey-io/valkey#913](https://github.com/valkey-io/valkey/pull/913) ([tests](tests/valkey/913-70624ea63/)): Change all the lazyfree configurations to yes by default (#913): 1 of 4 test(s) reached the change
- [valkey-io/valkey#975](https://github.com/valkey-io/valkey/pull/975) ([tests](tests/valkey/975-089048d36/)): Fix zipmap test null pointer (#975): declined: The change edits only the unit test src/unit/test_zipmap.c, replacing a NULL expected value with an empty string to avoid a null-pointer dereference inside the test itself. It changes no server, command, replication, persistence or tool behavior, so no runtime scenario can reach or observe
- [valkey-io/valkey#865](https://github.com/valkey-io/valkey/pull/865) ([tests](tests/valkey/865-e3af1a30e/)): Fast path in SET if the expiration time is expired (#865): 2 of 4 test(s) reached the change, coverage 100%
- [valkey-io/valkey#973](https://github.com/valkey-io/valkey/pull/973) ([tests](tests/valkey/973-5d458c629/)): Delete unused parts of zipmap (#973): declined: The change only deletes zipmap functions that nothing in the server references (zipmapNew/Set/Del/Get/Exists/Len/BlobLen and their static helpers) and rewrites the unit tests to use hand-built byte literals instead of the deleted writers. The sole runtime user of zipmap is rdb.c's loading 
- [valkey-io/valkey#895](https://github.com/valkey-io/valkey/pull/895) ([tests](tests/valkey/895-743f5ac2a/)): standalone -REDIRECT handles special case of MULTI context (#895): 2 of 2 test(s) reached the change, coverage 100%
- [valkey-io/valkey#474](https://github.com/valkey-io/valkey/pull/474) ([tests](tests/valkey/474-2b76c8fbe/)): Migrate zipmap unit test to new framework (#474): declined: The change is a pure test-infrastructure migration: it removes the zipmap entry from the legacy serverTests[] table in server.c and adds an equivalent standalone unit test file (src/unit/test_zipmap.c) registered in test_files.h. No server runtime behaviour, command, reply, data structure,
- [valkey-io/valkey#944](https://github.com/valkey-io/valkey/pull/944) ([tests](tests/valkey/944-ecbfb6a7e/)): Fix reconfiguring sub-replica causing data loss when myself change shard_id (#944): 2 of 4 test(s) reached the change, coverage 100%
- [valkey-io/valkey#961](https://github.com/valkey-io/valkey/pull/961) ([tests](tests/valkey/961-4a9b4f667/)): free client's multi state when it becomes dirty (#961): 3 of 3 test(s) reached the change, coverage 100%
- [valkey-io/valkey#963](https://github.com/valkey-io/valkey/pull/963) ([tests](tests/valkey/963-ad0ede302/)): Exclude '.' and ':' from `isValidAuxChar`'s banned charset (#963): 0 of 3 test(s) reached the change, coverage 100%; crash, fixed by #963 (recorded)
- [valkey-io/valkey#964](https://github.com/valkey-io/valkey/pull/964) ([tests](tests/valkey/964-75b824052/)): Revert make KEYS to be an exact match if there is no pattern (#964): 0 of 3 test(s) reached the change
- [valkey-io/valkey#958](https://github.com/valkey-io/valkey/pull/958) ([tests](tests/valkey/958-927c2a8cd/)): Delete files MANIFESTO, BUGS and INSTALL (#958): declined: The change deletes three plain-text documentation files (MANIFESTO, BUGS, INSTALL) from the repository root. No source code, build system, configuration, command, or protocol behaviour changes, so no server or tool behaviour can differ and there is nothing to test.
- [valkey-io/valkey#956](https://github.com/valkey-io/valkey/pull/956) ([tests](tests/valkey/956-2b71a7824/)): Add comment explaining log file reopening for rotation support (#956): declined: The change adds a three-line comment above the fopen in serverLogRaw explaining that the log file is reopened on every call to support rotation. No code path, reply, config, or behaviour changes — it is a comment-only change, so there is nothing a scenario could exercise or distinguish fro
- [valkey-io/valkey#846](https://github.com/valkey-io/valkey/pull/846) ([tests](tests/valkey/846-744b13e30/)): Using intrinsics to optimize counting HyperLogLog trailing bits (#846): 0 of 3 test(s) reached the change, coverage 100%
- [valkey-io/valkey#860](https://github.com/valkey-io/valkey/pull/860) ([tests](tests/valkey/860-076bf6605/)): Move prepareClientToWrite out of loop for lrange command to reduce the redundant call. (#860): 0 of 3 test(s) reached the change, coverage 96%
- [valkey-io/valkey#923](https://github.com/valkey-io/valkey/pull/923) ([tests](tests/valkey/923-4f29ad458/)): Use sdsAllocSize instead of sdsZmallocSize (#923): 2 of 5 test(s) reached the change, coverage 90%
- [valkey-io/valkey#908](https://github.com/valkey-io/valkey/pull/908) ([tests](tests/valkey/908-1ff2a3b6a/)): Remove `dual-channel-replication` Feature Flag's Protection (#908): 2 of 4 test(s) reached the change
- [valkey-io/valkey#953](https://github.com/valkey-io/valkey/pull/953) ([tests](tests/valkey/953-54c0f743d/)): Connection minor fixes (#953): 0 of 3 test(s) reached the change
- [valkey-io/valkey#861](https://github.com/valkey-io/valkey/pull/861) ([tests](tests/valkey/861-04d76d8b0/)): Improve multithreaded performance with memory prefetching (#861): 3 of 8 test(s) reached the change, coverage 97%

### valkey-io/valkey-bloom

[2026-11-11T14:36:56+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/43040052965): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-json

[2026-11-11T14:55:28+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/43043050914): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-search

[2026-11-11T14:56:51+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/43043259830): 0 merge(s) tested, 0 without a code change.


## Totals

- Merges whose tests reached the change: 813 of 1293 tested (63%)
- Changed-line coverage: median 100% of 809 merge(s) measured, 65 under 50%
- Recall: 218 parents flagged of 344 fix merges
- Claude: $1049.30 in the last 24 hours, $9410.11 in the last 7 days, $9410.11 in all
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

- [valkey-io/valkey#1535](https://github.com/valkey-io/valkey/pull/1535): attempt 0 of 3, Claude Code failed (rc=1): API Error: Claude Code is unable to respond to this request, which appears to violate our Usage Policy (https://www.anthropic.com/legal/aup). This request triggered restrict
- [valkey-io/valkey#1310](https://github.com/valkey-io/valkey/pull/1310): attempt 0 of 3, Claude Code failed (rc=1): API Error: Claude Code is unable to respond to this request, which appears to violate our Usage Policy (https://www.anthropic.com/legal/aup). This request triggered restrict
