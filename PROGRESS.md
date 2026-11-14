# Fuzzer walk

Updated 2026-11-14T21:54+00:00.

| Repository | Done | Remaining | Backlog cursor | Last 24 hours | Last 7 days (per day) |
|---|---|---|---|---|---|
| valkey-io/valkey | 2461 | 80 | #12758 (2023-11-13) | 292 | 239.4 |
| valkey-io/valkey-bloom | 109 | 0 | ce516d0ef6e3 (2024-04-26) | 0 | 0.0 |
| valkey-io/valkey-json | 66 | 0 | c1423d7e4b38 (2024-11-25) | 1 | 0.6 |
| valkey-io/valkey-search | 692 | 0 | f8106e72e984 (2024-11-27) | 10 | 26.7 |

## Last runs

### valkey-io/valkey

[2026-11-14T21:36:33+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/43632223968): 20 merge(s) tested, 2 without a code change.

- [redis/redis#12738](https://github.com/redis/redis/pull/12738) ([tests](tests/valkey/12738-1bd0b5495/)): Optimize the efficiency of active expiration when databases exceeds 16. (#12738): 3 of 3 test(s) reached the change, coverage 100%
- [redis/redis#11144](https://github.com/redis/redis/pull/11144) ([tests](tests/valkey/11144-56ec1ff1c/)): Call signalModifiedKey after the key modification is completed (#11144): 1 of 6 test(s) reached the change, coverage 100%
- [redis/redis#12658](https://github.com/redis/redis/pull/12658) ([tests](tests/valkey/12658-2e854bccc/)): Fix async safety in signal handlers (#12658): 0 of 15 test(s) reached the change, coverage 74%; [crash, appeared after redis/redis#12658](https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/164)
- [redis/redis#12636](https://github.com/redis/redis/pull/12636) ([tests](tests/valkey/12636-5e403099b/)): Fix misleading error message in redis.log when loglevel is invalid (#12636): 2 of 2 test(s) reached the change, coverage 100%
- [redis/redis#12795](https://github.com/redis/redis/pull/12795) ([tests](tests/valkey/12795-c9aa586b6/)): rdb.c: Avoid potential file handle leak (Coverity 404720) (#12795): declined: The change only widens a guard from `rdb_fd > 0` to `rdb_fd >= 0` when re-opening the RDB file after a successful load to queue a background close/page-cache-reclaim job. It differs from the parent only when open() returns fd 0, which requires the server's stdin to already be closed — a st
- [redis/redis#12797](https://github.com/redis/redis/pull/12797) ([tests](tests/valkey/12797-ae09d4d3e/)): redis-check-aof.c: Avoid leaking file handle if file is zero bytes (#12797): declined: This is a pure resource-leak fix: redis-check-aof.c now calls fclose(fp) before returning AOF_CHECK_EMPTY for a zero-byte AOF file. The tool's observable behavior (exit status, stdout, stderr) is byte-identical on both builds for every input: an empty file is reported 'is empty' and the to
- [redis/redis#12796](https://github.com/redis/redis/pull/12796) ([tests](tests/valkey/12796-1c48d3dab/)): config.c: Avoid leaking file handle if redis_fstat() fails (#12796): declined: The change only adds fclose(fp) on an error path in rewriteConfigReadOldFile() that is taken when redis_fstat() fails on a file descriptor that was just successfully opened via fopen(). That failure cannot be provoked without syscall-level fault injection, which the fuzzer does not have: C
- [redis/redis#12800](https://github.com/redis/redis/pull/12800) ([tests](tests/valkey/12800-157e5d47b/)): util.c: Don't leak directory handle if recursive call to dirRemove fails (#12800): declined: The change adds closedir() on an error path of dirRemove() in util.c to avoid leaking a directory handle when a recursive removal fails. dirRemove has no callers anywhere in the Valkey source (only its declaration in util.h), so the code is unreachable from any command, persistence, or rep
- [redis/redis#12789](https://github.com/redis/redis/pull/12789) ([tests](tests/valkey/12789-463476933/)): Optimize dict expand check, move allow check after the thresholds check (#12789): declined: The change only reorders two pure checks in _dictExpandIfNeeded (dict.c): the dictTypeExpandAllowed call moves from before the size-threshold checks to inside the branch taken when an expand is due. The only expandAllowed implementation (dictExpandAllowed -> overMaxmemoryAfterAlloc) is sid
- [redis/redis#12793](https://github.com/redis/redis/pull/12793) ([tests](tests/valkey/12793-ef1ca6c88/)): add some file level comments and copyright (#12793): declined: The change (PR #12793) only adds file-level descriptive comments and BSD copyright headers to cluster.c and cluster_legacy.c and rewords an existing comment block. Every added or removed line in the diff is a comment or blank line; no executable code, config, build rule, or tool behaviour 
- [redis/redis#12773](https://github.com/redis/redis/pull/12773) ([tests](tests/valkey/12773-dedbf99a8/)): Fix dbExpand not dividing by slots, resulting in consuming slots times the dictExpand (#12773): 2 of 2 test(s) reached the change, coverage 100%
- [redis/redis#12742](https://github.com/redis/redis/pull/12742) ([tests](tests/valkey/12742-58cb30252/)): Cluster refactor, common API for different implementations (#12742): 0 of 6 test(s) reached the change, coverage 69%
- [redis/redis#12790](https://github.com/redis/redis/pull/12790) ([tests](tests/valkey/12790-2c41b1350/)): Block DEBUG POPULATE in loading and async-loading (#12790): 2 of 4 test(s) reached the change, coverage 100%
- [redis/redis#12735](https://github.com/redis/redis/pull/12735) ([tests](tests/valkey/12735-4278ed8de/)): redis-cli --bigkeys ,--hotkeys and --memkeys to replica in cluster mode (#12735): 2 of 2 test(s) reached the change, coverage 83%
- [redis/redis#12751](https://github.com/redis/redis/pull/12751) ([tests](tests/valkey/12751-a1f91ffa1/)): Add an explanation for URI with -u in redis-cli --help (#12751): declined: The change only rewords the usage/help text strings in redis-cli and redis-benchmark (explaining the -u URI format and adding an example line). No code path, option parsing, or server behaviour changes; it is documentation-only, so no scenario is needed.
- [redis/redis#12777](https://github.com/redis/redis/pull/12777) ([tests](tests/valkey/12777-eb392c0a6/)): replace calculateKeySlot with known slot in evictionPoolPopulate (#12777): 0 of 3 test(s) reached the change, coverage 100%
- [redis/redis#12775](https://github.com/redis/redis/pull/12775) ([tests](tests/valkey/12775-6013122c8/)): check dbSize when rewrite AOF, in case unnecessary SELECT (#12775): 1 of 3 test(s) reached the change, coverage 100%
- [redis/redis#12770](https://github.com/redis/redis/pull/12770) ([tests](tests/valkey/12770-9b6dded42/)): Fix empty rehashing list in swapdb mode (#12770): 0 of 5 test(s) reached the change, coverage 100%; crash, fixed by redis/redis#12770 (recorded)
- [redis/redis#12764](https://github.com/redis/redis/pull/12764) ([tests](tests/valkey/12764-4366bbaa6/)): Empty rehashing list in emptyDbStructure (#12764): 0 of 2 test(s) reached the change, coverage 100%
- [redis/redis#12757](https://github.com/redis/redis/pull/12757) ([tests](tests/valkey/12757-fe3630634/)): Fix DB iterator not resetting pauserehash causing dict being unable to rehash (#12757): 2 of 10 test(s) reached the change, coverage 87%

### valkey-io/valkey-bloom

[2026-11-14T21:48:04+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/43644667460): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-json

[2026-11-14T21:54:19+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/43645205846): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-search

[2026-11-14T21:46:15+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/43631714931): 1 merge(s) tested, 0 without a code change.

- [valkey-io/valkey-search#923](https://github.com/valkey-io/valkey-search/pull/923) ([tests](tests/valkey-search/923-57ae2bcff/)): Implement metric to CPU consumption for remote query (#923): 4 of 4 test(s) reached the change

## Totals

- Merges whose tests reached the change: 966 of 1547 tested (62%)
- Changed-line coverage: median 100% of 1027 merge(s) measured, 78 under 50%
- Recall: 256 parents flagged of 402 fix merges
- Claude: $919.64 in the last 24 hours, $6157.42 in the last 7 days, $11381.07 in all
- Issues filed: 81
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
