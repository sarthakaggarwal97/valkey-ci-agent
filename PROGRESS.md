# Fuzzer walk

Updated 2026-11-11T21:14+00:00.

| Repository | Done | Remaining | Backlog cursor | Last 24 hours | Last 7 days (per day) |
|---|---|---|---|---|---|
| valkey-io/valkey | 1873 | 653 | #855 (2024-08-03) | 289 | 267.6 |
| valkey-io/valkey-bloom | 109 | 0 | ce516d0ef6e3 (2024-04-26) | 0 | 15.6 |
| valkey-io/valkey-json | 64 | 0 | c1423d7e4b38 (2024-11-25) | 1 | 9.1 |
| valkey-io/valkey-search | 672 | 0 | f8106e72e984 (2024-11-27) | 0 | 96.0 |

## Last runs

### valkey-io/valkey

[2026-11-11T21:14:56+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/43091847369): 20 merge(s) tested, 4 without a code change.

- [valkey-io/valkey#5351](https://github.com/valkey-io/valkey/pull/5351): to retry, the job left no result
- [valkey-io/valkey#479](https://github.com/valkey-io/valkey/pull/479) ([tests](tests/valkey/479-adf53c212/)): Add lfu support for DEBUG OBJECT command, added lfu_freq and lfu_access_time_minutes fields (#479): 2 of 2 test(s) reached the change, coverage 100%
- [valkey-io/valkey#5209](https://github.com/valkey-io/valkey/pull/5209) ([tests](tests/valkey/5209-5a4ed2e08/)): Allow null in GEOHASH and OBJECT reply schemas (#5209): declined: The change only edits reply_schema metadata in src/commands/*.json (GEOHASH, OBJECT FREQ/IDLETIME/REFCOUNT) to allow null, plus Tcl tests. The server code already returned null for missing keys/members on both builds (geo.c addReplyNull, object.c addReplyNull), and reply schemas are docume
- [valkey-io/valkey#881](https://github.com/valkey-io/valkey/pull/881) ([tests](tests/valkey/881-fc9f29103/)): Make a light weight version of DEBUG OBJECT, add FAST option (#881): 2 of 2 test(s) reached the change, coverage 100%
- [valkey-io/valkey#839](https://github.com/valkey-io/valkey/pull/839) ([tests](tests/valkey/839-103365fe0/)): Log unexpected $ENDOFF responses in dual channel replication (#839): declined: The diff adds only a serverLog(LL_WARNING, ...) call on an already-existing error path in fullSyncWithPrimary (dual-channel replication): when sscanf fails to parse the $ENDOFF response, the code goes to the same goto error as before, now with a log line first. There is no change to replie
- [valkey-io/valkey#740](https://github.com/valkey-io/valkey/pull/740) ([tests](tests/valkey/740-4d284daef/)): Copyright update to reflect IP transfer from salvatore to Redis (#740): 1 of 1 test(s) reached the change, coverage not measured (the build left no gcov notes of src/ae_epoll.c, src/ae_select.c, src/modules/helloblock.c, src/modules/hellocluster.c, src/modules/hellodict.c, src/modules/hellohook.c, src/modules/hellotimer.c, src/m)
- [valkey-io/valkey#907](https://github.com/valkey-io/valkey/pull/907) ([tests](tests/valkey/907-68b227094/)): Prevent later accesses to unallocated memory (#907): 0 of 4 test(s) reached the change, coverage 100%
- [valkey-io/valkey#871](https://github.com/valkey-io/valkey/pull/871) ([tests](tests/valkey/871-131857e80/)): To avoid bouncing -REDIRECT during FAILOVER (#871): 2 of 3 test(s) reached the change, coverage 100%
- [valkey-io/valkey#876](https://github.com/valkey-io/valkey/pull/876) ([tests](tests/valkey/876-370bdb3e4/)): Change server.daylight_active to an atomic variable (#876): declined: The change only makes server.daylight_active an _Atomic int with relaxed load/store, annotating a benign data race between the cron thread updating the DST flag and other threads reading it while formatting log timestamps. It changes no command reply, no log output, no replication/persiste
- [valkey-io/valkey#867](https://github.com/valkey-io/valkey/pull/867): to retry, the job left no result
- [valkey-io/valkey#829](https://github.com/valkey-io/valkey/pull/829) ([tests](tests/valkey/829-76f809bc1/)): Optimize ZUNION[STORE] command by removing unnecessary accumulator dict (#829): 0 of 3 test(s) reached the change, coverage 100%
- [valkey-io/valkey#874](https://github.com/valkey-io/valkey/pull/874) ([tests](tests/valkey/874-6dfb8203c/)): Add debug-context config (#874): declined: no proposed test validated
- [valkey-io/valkey#837](https://github.com/valkey-io/valkey/pull/837) ([tests](tests/valkey/837-27fce2950/)): Fix dual-channel-replication related issues (#837): 0 of 9 test(s) reached the change, coverage 100%
- [valkey-io/valkey#896](https://github.com/valkey-io/valkey/pull/896) ([tests](tests/valkey/896-1c198a95a/)): Add debug assert on duplicate freeClientAsync (#896): 0 of 3 test(s) reached the change, coverage 100%
- [valkey-io/valkey#811](https://github.com/valkey-io/valkey/pull/811) ([tests](tests/valkey/811-929283fc6/)): Dual channel replication should not update lastbgsave_status when transfer error (#811): 1 of 6 test(s) reached the change, coverage 50%
- [valkey-io/valkey#805](https://github.com/valkey-io/valkey/pull/805) ([tests](tests/valkey/805-5166d489d/)): Correctly recode client infomation to the slowlog when runing script (#805): 3 of 3 test(s) reached the change, coverage 100%
- [valkey-io/valkey#838](https://github.com/valkey-io/valkey/pull/838) ([tests](tests/valkey/838-7424620ca/)): Check if the server is currently running the feature before cron run (#838): declined: The change only reorders two side-effect-free condition checks in serverCron: run_with_period(100) is a pure macro (an if over server.cronloops and server.hz with no state mutation, server.h:638), so testing server.cluster_enabled or moduleCount() before it instead of after yields the exac
- [valkey-io/valkey#847](https://github.com/valkey-io/valkey/pull/847): to retry, the job left no result
- [valkey-io/valkey#780](https://github.com/valkey-io/valkey/pull/780) ([tests](tests/valkey/780-380f70081/)): Improve cluster cant failover log conditions (#780): declined: The change only alters diagnostic logging in clusterLogCantFailover: it removes the suppression of the cant-failover log while the primary's FAIL state is recent, and lowers the re-log period from 10s to 1s. No command reply, cluster election decision, replication, persistence, or data pat
- [valkey-io/valkey#854](https://github.com/valkey-io/valkey/pull/854) ([tests](tests/valkey/854-0fc43edc6/)): Update sentinel conf access string to allow hello channel access (#854): declined: The change edits only a commented-out example ACL line in sentinel.conf (documentation inside a # comment block). No code, config parsing, or runtime behaviour changed, so there is no path for a scenario to reach or break.

### valkey-io/valkey-bloom

[2026-11-11T20:16:25+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/43094842020): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-json

[2026-11-11T20:10:00+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/43093825374): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-search

[2026-11-11T20:13:55+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/43094395980): 0 merge(s) tested, 0 without a code change.


## Totals

- Merges whose tests reached the change: 823 of 1313 tested (63%)
- Changed-line coverage: median 100% of 827 merge(s) measured, 65 under 50%
- Recall: 220 parents flagged of 348 fix merges
- Claude: $944.05 in the last 24 hours, $9582.54 in the last 7 days, $9582.54 in all
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
- [valkey-io/valkey#5351](https://github.com/valkey-io/valkey/pull/5351): attempt 0 of 3, the job left no result
- [valkey-io/valkey#867](https://github.com/valkey-io/valkey/pull/867): attempt 0 of 3, the job left no result
- [valkey-io/valkey#847](https://github.com/valkey-io/valkey/pull/847): attempt 0 of 3, the job left no result
