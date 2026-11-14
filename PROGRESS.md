# Fuzzer walk

Updated 2026-11-14T09:50+00:00.

| Repository | Done | Remaining | Backlog cursor | Last 24 hours | Last 7 days (per day) |
|---|---|---|---|---|---|
| valkey-io/valkey | 2324 | 216 | #13008 (2024-02-06) | 305 | 247.9 |
| valkey-io/valkey-bloom | 109 | 0 | ce516d0ef6e3 (2024-04-26) | 0 | 0.0 |
| valkey-io/valkey-json | 66 | 0 | c1423d7e4b38 (2024-11-25) | 2 | 0.6 |
| valkey-io/valkey-search | 690 | 0 | f8106e72e984 (2024-11-27) | 9 | 42.4 |

## Last runs

### valkey-io/valkey

[2026-11-14T08:50:26+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/43568547734): 20 merge(s) tested, 5 without a code change.

- [valkey-io/valkey#35](https://github.com/valkey-io/valkey/pull/35) ([tests](tests/valkey/35-3782446a4/)): Untrademark json files (#35): 2 of 2 test(s) reached the change
- [redis/redis#12996](https://github.com/redis/redis/pull/12996) ([tests](tests/valkey/12996-f6785df66/)): Defragger improvements around large bins (#12996): 2 of 5 test(s) reached the change, coverage 94%
- [redis/redis#13056](https://github.com/redis/redis/pull/13056) ([tests](tests/valkey/13056-ca5cac998/)): xinfo-stream add minimum to seen-time, skip logreqres in fuzzer (#13056): declined: The change edits only metadata and test harness files: it adds a 'minimum: 0' annotation to the seen-time field in src/commands/xinfo-stream.json (documentation/schema metadata, not used by the server at runtime to compute or format replies) and adds a 'logreqres:skip' tag to tests/integra
- [redis/redis#13062](https://github.com/redis/redis/pull/13062) ([tests](tests/valkey/13062-3c2ea1ea9/)): Fix wathced client test timing issue caused by late close (#13062): declined: The change is test-only: it fixes a flaky assertion in tests/unit/info.tcl by adding a wait_for_watched_clients_count helper (tests/support/util.tcl) and polling instead of asserting watching_clients immediately after a client close. The only change under src/ is a one-line comment fix in 
- [redis/redis#12973](https://github.com/redis/redis/pull/12973) ([tests](tests/valkey/12973-fc3a68d8f/)): add -fno-omit-frame-pointer to default complication flags (#12973): declined: The change only adds -fno-omit-frame-pointer to the default compiler flags in src/Makefile (a build/CI change). It affects code generation for debuggability and profiling (keeping the frame pointer register) but introduces no behavioral, protocol, replication, persistence, or data-path dif
- [redis/redis#13063](https://github.com/redis/redis/pull/13063) ([tests](tests/valkey/13063-8876d264a/)): Calculate the incremental rehash time more precisely (#13063): declined: The change only refactors how the ~1ms incremental-rehash CPU budget is measured and split across dicts in kvstoreIncrementallyRehash and databasesCron: work is now done before the time check and each call receives the remaining rather than the full budget. dictRehash/dictRehashMicrosecond
- [redis/redis#13041](https://github.com/redis/redis/pull/13041) ([tests](tests/valkey/13041-9103ccc39/)): AOF_FSYNC_EVERYSEC higher resolution, change aof_last_fsync and aof_flush_postponed_start to use mstime (#13041): 0 of 5 test(s) reached the change, coverage 100%
- [redis/redis#13059](https://github.com/redis/redis/pull/13059) ([tests](tests/valkey/13059-dd92dd8fb/)): redis-cli - fix sscanf incorrect return-value check warnings (#13059): declined: The change only tightens two sscanf return-value checks in redis-cli's matchNoTokenArg, which is used solely to compute the interactive linenoise syntax hint text (hintsCallback). That code runs only in interactive raw-TTY mode; with piped stdin (as in the fuzzer's tools runner) linenoise 
- [redis/redis#12966](https://github.com/redis/redis/pull/12966) ([tests](tests/valkey/12966-50d6fe8c4/)): Add metrics for WATCH (#12966): 2 of 2 test(s) reached the change, coverage 100%
- [redis/redis#13054](https://github.com/redis/redis/pull/13054) ([tests](tests/valkey/13054-c85487374/)): Minor optimization in kvstoreDictAddRaw when dict exists (#13054): declined: The change is a pure refactor with no behaviour change: createDictIfNeeded now returns the dict pointer so kvstoreDictAddRaw avoids one redundant kvstoreGetDict lookup, with identical creation condition, allocated_dicts accounting and cumulativeKeyCountAdd path. The remaining hunks are com
- [redis/redis#13052](https://github.com/redis/redis/pull/13052) ([tests](tests/valkey/13052-063de675e/)): zunionInterDiffGenericCommand use ztrycalloc to avoid OOM panic (#13052): declined: This is an OOM-hardening change: zcalloc of the transient src table in zunionInterDiffGenericCommand (ZUNION/ZINTER/ZDIFF and *STORE/ZINTERCARD) is replaced by ztrycalloc with a NULL check returning an error instead of aborting. Its only observable difference from the parent is when that s
- [redis/redis#13046](https://github.com/redis/redis/pull/13046) ([tests](tests/valkey/13046-c1d2ac2a7/)): Do not include gossip about receiver in cluster messages (#13046): 0 of 3 test(s) reached the change, coverage 100%
- [redis/redis#13050](https://github.com/redis/redis/pull/13050) ([tests](tests/valkey/13050-e9c795e77/)): Fix loading rdb opcode RDB_OPCODE_RESIZEDB (#13050): 1 of 2 test(s) reached the change, coverage 100%; crash, introduced and since fixed (recorded); crash, fixed by redis/redis#13050 (recorded)
- [redis/redis#13049](https://github.com/redis/redis/pull/13049) ([tests](tests/valkey/13049-7ca0b84af/)): Fix loading rdb opcode RDB_OPCODE_SLOT_INFO (#13049): 1 of 3 test(s) reached the change, coverage 100%
- [redis/redis#13013](https://github.com/redis/redis/pull/13013) ([tests](tests/valkey/13013-676f27acb/)): Fix the failure of defrag test under 32-bit (#13013): 0 of 9 test(s) reached the change, coverage 95%
- [redis/redis#13043](https://github.com/redis/redis/pull/13043) ([tests](tests/valkey/13043-493e31e3a/)): Add new DEBUG dict-resizing command to disable the dict resize (#13043): 2 of 2 test(s) reached the change, coverage 100%
- [redis/redis#13042](https://github.com/redis/redis/pull/13042) ([tests](tests/valkey/13042-813327b23/)): Fix SORT STORE quicklist with the right options (#13042): 3 of 5 test(s) reached the change, coverage 88%
- [redis/redis#13040](https://github.com/redis/redis/pull/13040) ([tests](tests/valkey/13040-1e8dc1da0/)): Fix crash due to merge of quicklist node introduced by #12955 (#13040): 2 of 2 test(s) reached the change, coverage 87%; crash, fixed by redis/redis#13040 (recorded)
- [redis/redis#13038](https://github.com/redis/redis/pull/13038) ([tests](tests/valkey/13038-81666a651/)): Fix heap-use-after-free when pubsubshard_channels became NULL (#13038): 0 of 7 test(s) reached the change, coverage 100%
- [redis/redis#12955](https://github.com/redis/redis/pull/12955) ([tests](tests/valkey/12955-1f00c951c/)): Prevent LSET command from causing quicklist plain node size to exceed 4GB (#12955): 0 of 8 test(s) reached the change, coverage 95%

### valkey-io/valkey-bloom

[2026-11-14T09:50:31+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/43582390624): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-json

[2026-11-14T09:50:29+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/43582394584): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-search

[2026-11-14T09:30:21+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/43580795093): 0 merge(s) tested, 0 without a code change.


## Totals

- Merges whose tests reached the change: 921 of 1470 tested (63%)
- Changed-line coverage: median 100% of 958 merge(s) measured, 70 under 50%
- Recall: 241 parents flagged of 381 fix merges
- Claude: $625.88 in the last 24 hours, $6626.76 in the last 7 days, $10810.25 in all
- Issues filed: 78
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
