# Fuzzer walk

Updated 2026-11-15T01:46+00:00.

| Repository | Done | Remaining | Backlog cursor | Last 24 hours | Last 7 days (per day) |
|---|---|---|---|---|---|
| valkey-io/valkey | 2515 | 26 | #12557 (2023-09-08) | 321 | 243.3 |
| valkey-io/valkey-bloom | 109 | 0 | ce516d0ef6e3 (2024-04-26) | 0 | 0.0 |
| valkey-io/valkey-json | 66 | 0 | c1423d7e4b38 (2024-11-25) | 0 | 0.6 |
| valkey-io/valkey-search | 693 | 0 | f8106e72e984 (2024-11-27) | 6 | 23.3 |

## Last runs

### valkey-io/valkey

[2026-11-15T01:40:29+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/43652637572): 20 merge(s) tested, 5 without a code change.

- [redis/redis#11695](https://github.com/redis/redis/pull/11695) ([tests](tests/valkey/11695-0270abda8/)): Replace cluster metadata with slot specific dictionaries (#11695): 1 of 15 test(s) reached the change, coverage 81%
- [redis/redis#12577](https://github.com/redis/redis/pull/12577) ([tests](tests/valkey/12577-b784c5375/)): Unsubscribe all clients from replica for shard channel if the master ownership changes (#12577): 2 of 3 test(s) reached the change, coverage 100%
- [redis/redis#12646](https://github.com/redis/redis/pull/12646) ([tests](tests/valkey/12646-b705049a7/)): Replace `emptyDb()` with new `emptyData()` (#12646): declined: The change edits only comments and documentation: a README line and comment text in src/db.c and src/replication.c that update references from emptyDb() to the new emptyData() name. No executable code, config, or protocol behaviour changes, so there is nothing a scenario could reach or dis
- [redis/redis#12596](https://github.com/redis/redis/pull/12596) ([tests](tests/valkey/12596-77a65e82b/)): support XREAD[GROUP] with BLOCK option in scripts (#12596): 3 of 3 test(s) reached the change
- [redis/redis#12604](https://github.com/redis/redis/pull/12604) ([tests](tests/valkey/12604-e5ef16137/)): Fix crash when running rebalance command in a mixed cluster of 7.0 and 7.2 (#12604): 0 of 4 test(s) reached the change, coverage 100%
- [redis/redis#12571](https://github.com/redis/redis/pull/12571) ([tests](tests/valkey/12571-4de4fcf28/)): Fix redis-cli pubsub_mode and connect minor prompt / crash issue (#12571): 1 of 1 test(s) reached the change, coverage 100%; crash, fixed by redis/redis#12571 (recorded)
- [redis/redis#12633](https://github.com/redis/redis/pull/12633) ([tests](tests/valkey/12633-8d92f7f2b/)): Support NO ONE block in REPLICAOF command json (#12633): 2 of 2 test(s) reached the change
- [redis/redis#12611](https://github.com/redis/redis/pull/12611) ([tests](tests/valkey/12611-a2b0701d2/)): Fix compile on macOS 13 (#12611): declined: Build-only fix: the diff changes macOS version-detection preprocessor macros in src/config.h and src/debug.c, all guarded by #if defined(__APPLE__). On the Linux hosts the fuzzer runs on, the preprocessor output and compiled binary are identical to the parent, so no runtime path differs; t
- [redis/redis#12630](https://github.com/redis/redis/pull/12630) ([tests](tests/valkey/12630-fe37e4fc8/)): Cleanup nested module keyspace notifications (#12630): declined: The change saves and restores sub->active around a module keyspace-notification callback instead of resetting it to 0. The flag is read in exactly one place, the dispatch condition (sub->active == 0 || module has REDISMODULE_OPTIONS_ALLOW_NESTED_KEYSPACE_NOTIFICATIONS). A nested re-invocat
- [redis/redis#12539](https://github.com/redis/redis/pull/12539) ([tests](tests/valkey/12539-31c3172d9/)): Better standardize around assertions (#12539): declined: The change is a refactor of assertion infrastructure with no behavior change on any reachable path. It (1) replaces <assert.h> includes with redisassert.h in defrag.c, memtest.c, monotonic.c, rax.c, sds.c, siphash.c, zmalloc.c, server.c/h, so assert() routes to the existing _serverAssert l
- [redis/redis#12541](https://github.com/redis/redis/pull/12541) ([tests](tests/valkey/12541-9d31768cb/)): Fix a couple of tabs that caused misindentation (#12541): declined: The change (#12541) only replaces tabs with spaces in indentation and one comment across cluster.c, config.c, redis-cli.c and zmalloc.c. No statement, expression, control flow, or string literal changes; the compiled binaries behave identically. A pure formatting fix needs no test scenario
- [redis/redis#12623](https://github.com/redis/redis/pull/12623) ([tests](tests/valkey/12623-4ba9e18ef/)): fix crash in crash-report and other improvements (#12623): 0 of 7 test(s) reached the change, coverage 0%
- [redis/redis#12622](https://github.com/redis/redis/pull/12622) ([tests](tests/valkey/12622-c2a4b7849/)): WAITAOF: Update fsynced_reploff_pending even if there's nothing to fsync (#12622): 2 of 3 test(s) reached the change, coverage 100%
- [redis/redis#12620](https://github.com/redis/redis/pull/12620) ([tests](tests/valkey/12620-bfa3931a0/)): WAITAOF: Update fsynced_reploff_pending just before starting the initial AOFRW fork (#12620): 2 of 3 test(s) reached the change, coverage 100%
- [redis/redis#12257](https://github.com/redis/redis/pull/12257) ([tests](tests/valkey/12257-f924bebd8/)): Rewrite huge printf calls to smaller ones for readability (#12257): 0 of 2 test(s) reached the change, coverage 84%
- [redis/redis#12564](https://github.com/redis/redis/pull/12564) ([tests](tests/valkey/12564-8cdeddc81/)): Clear owner_not_claiming_slot bit for the slot in clusterDelSlot (#12564): 0 of 3 test(s) reached the change, coverage 100%
- [redis/redis#12474](https://github.com/redis/redis/pull/12474) ([tests](tests/valkey/12474-24187ed8e/)): Fix overflow calculation for next timer event (#12474): declined: The fix casts a timer callback's returned period to monotime before multiplying by 1000 in processTimeEvents (ae.c), preventing 32-bit overflow when the returned period exceeds ~2,147,483 ms (~35.8 minutes). The only aeTimeProc that can return such a value is moduleTimerHandler, when a loa
- [redis/redis#12453](https://github.com/redis/redis/pull/12453) ([tests](tests/valkey/12453-cc2be6399/)): Print stack trace from all threads in crash report (#12453): 0 of 11 test(s) reached the change, coverage 91%
- [redis/redis#12569](https://github.com/redis/redis/pull/12569) ([tests](tests/valkey/12569-2aad03fa3/)): Use server.current_client to decide whether cluster commands should return TLS info. (#12569): declined: The change only alters which port cluster commands report — the TLS port vs. the plaintext port — and only for fake clients (Lua EVAL, module RM_call) whose connection is NULL; for real client connections server.current_client is that same client, so shouldReturnTlsInfo() returns exactly w
- [redis/redis#12561](https://github.com/redis/redis/pull/12561) ([tests](tests/valkey/12561-4031a1873/)): Fix that slot return in CLUSTER SHARDS should be integer (#12561): 2 of 2 test(s) reached the change, coverage 100%

### valkey-io/valkey-bloom

[2026-11-15T01:23:39+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/43662836785): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-json

[2026-11-15T01:46:09+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/43664572169): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-search

[2026-11-15T01:42:46+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/43664306008): 0 merge(s) tested, 0 without a code change.


## Totals

- Merges whose tests reached the change: 982 of 1574 tested (62%)
- Changed-line coverage: median 100% of 1051 merge(s) measured, 79 under 50%
- Recall: 260 parents flagged of 409 fix merges
- Claude: $1044.85 in the last 24 hours, $6162.89 in the last 7 days, $11564.48 in all
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
