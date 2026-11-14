# Fuzzer walk

Updated 2026-11-14T17:42+00:00.

| Repository | Done | Remaining | Backlog cursor | Last 24 hours | Last 7 days (per day) |
|---|---|---|---|---|---|
| valkey-io/valkey | 2394 | 146 | #12804 (2023-12-27) | 276 | 242.4 |
| valkey-io/valkey-bloom | 109 | 0 | ce516d0ef6e3 (2024-04-26) | 0 | 0.0 |
| valkey-io/valkey-json | 66 | 0 | c1423d7e4b38 (2024-11-25) | 2 | 0.6 |
| valkey-io/valkey-search | 691 | 0 | f8106e72e984 (2024-11-27) | 10 | 33.3 |

## Last runs

### valkey-io/valkey

[2026-11-14T16:08:06+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/43599271117): 20 merge(s) tested, 5 without a code change.

- [redis/redis#12950](https://github.com/redis/redis/pull/12950) ([tests](tests/valkey/12950-f81c3fd89/)): Optimize dictTypeResizeAllowed to avoid mistaken OOM judgement. (#12950): declined: This is a memory-management micro-optimization with no observable behavioral change. dictTypeResizeAllowed now receives the resize target as a parameter; the expand call site passes the identical value as before (d->ht_used[0] + 1), and only the shrink call site changes (d->ht_used[0] inst
- [redis/redis#12906](https://github.com/redis/redis/pull/12906) ([tests](tests/valkey/12906-bb2b6e292/)): fix scripts access wrong slot if they disagree with pre-declared keys (#12906): 4 of 4 test(s) reached the change, coverage 72%
- [redis/redis#12943](https://github.com/redis/redis/pull/12943) ([tests](tests/valkey/12943-284ef21ea/)): Fix fd check in memtest_test_linux_anonymous_maps (#12943): declined: The change fixes an error-path fd check (-1 vs 0) in memtest_test_linux_anonymous_maps, which runs only inside the crash report handler (doFastMemoryTest after SIGSEGV/SIGBUS) and only when open() on the logfile fails at crash time. No command, replication, persistence or tool path reaches
- [redis/redis#12897](https://github.com/redis/redis/pull/12897) ([tests](tests/valkey/12897-87786342a/)): Correct bytes_per_key computing. (#12897): 1 of 1 test(s) reached the change, coverage 100%
- [redis/redis#12930](https://github.com/redis/redis/pull/12930) ([tests](tests/valkey/12930-964f4a457/)): Avoid double free of cluster link (#12930): 3 of 3 test(s) reached the change, coverage 100%; crash, fixed by redis/redis#12930 (recorded)
- [redis/redis#12824](https://github.com/redis/redis/pull/12824) ([tests](tests/valkey/12824-b3aaa0a13/)): When one shard, sole primary node marks potentially failed replica as FAIL instead of PFAIL (#12824): 0 of 2 test(s) reached the change, coverage 75%
- [redis/redis#12924](https://github.com/redis/redis/pull/12924) ([tests](tests/valkey/12924-f7b1d0287/)): Fix possible corruption in sdsResize (CVE-2023-41056) (#12924): declined: no proposed test validated
- [redis/redis#12919](https://github.com/redis/redis/pull/12919) ([tests](tests/valkey/12919-14e4a9835/)): Fix minor fd leak in rdbSaveToSlavesSockets (#12919): declined: The change adds one close(server.rdb_child_exit_pipe) solely in the parent's fork-failure branch of rdbSaveToSlavesSockets (rdb.c:3639, the childpid == -1 cleanup). The successful diskless-replication path already manages that fd correctly (kept open, then closed by backgroundSaveDoneHandl
- [redis/redis#12914](https://github.com/redis/redis/pull/12914) ([tests](tests/valkey/12914-50b8b9976/)): Re-indent code and reduce code being complied on Solaris for anetKeepAlive (#12914): declined: The change is a re-indentation and comment reformat of src/anet.c plus a restructuring of the Solaris-only (#ifdef __sun) section of anetKeepAlive from an '#if ... return' followed by always-compiled fallback code into an #if/#else/#endif. The previously always-compiled fallback was unreac
- [redis/redis#12838](https://github.com/redis/redis/pull/12838) ([tests](tests/valkey/12838-c452e414a/)): Optimize performance when many clients [p|s]unsubscribe simultaneously (#12838): 0 of 7 test(s) reached the change, coverage 95%; crash, introduced and since fixed (recorded); check, not reproduced (recorded)
- [redis/redis#12917](https://github.com/redis/redis/pull/12917) ([tests](tests/valkey/12917-4730563e9/)): Change destination key's key-spec flag from RW to OW for SINTERSTORE command (#12917): 2 of 2 test(s) reached the change
- [redis/redis#12832](https://github.com/redis/redis/pull/12832) ([tests](tests/valkey/12832-5b0c6a825/)): Fix CLUSTER SHARDS crash in 7.0/7.2 mixed clusters where shard ids are not sync (#12832): 0 of 2 test(s) reached the change, coverage 100%
- [redis/redis#12905](https://github.com/redis/redis/pull/12905) ([tests](tests/valkey/12905-ca1f67af8/)): Make RM_Yield thread-safe (#12905): 0 of 4 test(s) reached the change, coverage 90%
- [redis/redis#12805](https://github.com/redis/redis/pull/12805) ([tests](tests/valkey/12805-4cae66f5e/)): Use shard-id of the master if the replica does not support shard-id (#12805): declined: The only behavioral addition fires exclusively when a gossiping peer omits the shard-id ping extension, i.e. a pre-7.2 node that does not support shard-id. Both builds the fuzzer runs (the change at 4cae66f and its parent at 38f0234) emit the shard-id extension unconditionally (cluster_leg
- [redis/redis#12857](https://github.com/redis/redis/pull/12857) ([tests](tests/valkey/12857-068051e37/)): Handle recursive serverAsserts and provide more information for recursive segfaults (#12857): declined: The change only affects crash-report/diagnostic output in src/debug.c: it labels recursive asserts/panics, avoids a duplicate printCrashReport, and prints a reduced stack trace instead of aborting on a recursive segfault. This code runs only after a crash or assertion has already happened 
- [redis/redis#12178](https://github.com/redis/redis/pull/12178) ([tests](tests/valkey/12178-c3f8b542e/)): Manage number of new connections per cycle (#12178): 2 of 5 test(s) reached the change, coverage 100%
- [redis/redis#12895](https://github.com/redis/redis/pull/12895) ([tests](tests/valkey/12895-9d0158bf8/)): Reorder signalModifiedKey in xaddCommand. (#12895): 1 of 4 test(s) reached the change, coverage 100%
- [redis/redis#12627](https://github.com/redis/redis/pull/12627) ([tests](tests/valkey/12627-12b611b37/)): WAITAOF: Try to wake blocked clients ASAP in the next beforeSleep (#12627): 0 of 3 test(s) reached the change, coverage 100%
- [redis/redis#12896](https://github.com/redis/redis/pull/12896) ([tests](tests/valkey/12896-99c468c38/)): Fix crash caused by pubsubShardUnsubscribeAllChannelsInSlot not deleting the client (#12896): 2 of 3 test(s) reached the change, coverage 100%; crash, fixed by redis/redis#12896 (recorded)
- [redis/redis#12892](https://github.com/redis/redis/pull/12892) ([tests](tests/valkey/12892-5b1fe925f/)): Adjust redis-cli --cluster create arity from -2 to -1 (#12892): 1 of 1 test(s) reached the change

### valkey-io/valkey-bloom

[2026-11-14T17:42:15+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/43622488021): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-json

[2026-11-14T17:16:19+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/43620124369): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-search

[2026-11-14T17:17:53+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/43620268459): 0 merge(s) tested, 0 without a code change.


## Totals

- Merges whose tests reached the change: 948 of 1514 tested (63%)
- Changed-line coverage: median 100% of 996 merge(s) measured, 76 under 50%
- Recall: 249 parents flagged of 393 fix merges
- Claude: $724.92 in the last 24 hours, $6301.47 in the last 7 days, $11103.92 in all
- Issues filed: 79
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
