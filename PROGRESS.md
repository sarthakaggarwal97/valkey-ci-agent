# Fuzzer walk

Updated 2026-11-12T00:53+00:00.

| Repository | Done | Remaining | Backlog cursor | Last 24 hours | Last 7 days (per day) |
|---|---|---|---|---|---|
| valkey-io/valkey | 1895 | 633 | #762 (2024-07-22) | 262 | 270.4 |
| valkey-io/valkey-bloom | 109 | 0 | ce516d0ef6e3 (2024-04-26) | 0 | 15.0 |
| valkey-io/valkey-json | 64 | 0 | c1423d7e4b38 (2024-11-25) | 0 | 8.3 |
| valkey-io/valkey-search | 672 | 0 | f8106e72e984 (2024-11-27) | 0 | 96.0 |

## Last runs

### valkey-io/valkey

[2026-11-11T22:59:21+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/43101419716): 20 merge(s) tested, 2 without a code change.

- [valkey-io/valkey#5246](https://github.com/valkey-io/valkey/pull/5246) ([tests](tests/valkey/5246-ae7060ae9/)): Allow null in SENTINEL GET-PRIMARY-ADDR-BY-NAME reply schema (#5246): declined: The diff only changes the reply_schema metadata in the sentinel-get-primary-addr-by-name.json and sentinel-get-master-addr-by-name.json command definition files (to document that a null reply is valid for an unknown primary) and adds a Tcl regression test. The server's reply code in src/se
- [valkey-io/valkey#4913](https://github.com/valkey-io/valkey/pull/4913) ([tests](tests/valkey/4913-255233918/)): Expire stale Sentinel primary-down reports before quorum checks (#4913): declined: The change is entirely inside src/sentinel.c (sentinelCheckObjectivelyDown / sentinelAskPrimaryStateToOtherSentinels): stale SRI_PRIMARY_DOWN reports from peer Sentinels are now expired before the quorum count, in code that only executes in a process running in Sentinel mode while it monit
- [valkey-io/valkey#5351](https://github.com/valkey-io/valkey/pull/5351) ([tests](tests/valkey/5351-6dad976b8/)): Use leaf counts in the zset leaf compaction pause test (#5351): 2 of 2 test(s) reached the change, coverage 100%
- [valkey-io/valkey#867](https://github.com/valkey-io/valkey/pull/867) ([tests](tests/valkey/867-f622e375a/)): Better messages when valkey-cli cluster --fix meet value check failed (#867): declined: The diff changes only src/valkey-cli.c, inside the cluster manager's BUSYKEY value check: when `valkey-cli --cluster fix` (or reshard/rebalance) retries a MIGRATE that failed with BUSYKEY, clusterManagerCompareKeysValues now captures the DEBUG DIGEST-VALUE error reply and prints a clearer 
- [valkey-io/valkey#847](https://github.com/valkey-io/valkey/pull/847) ([tests](tests/valkey/847-109cc2126/)): Assert network bytes out for replication slot stat computation is only allowed on primary (#847): 0 of 3 test(s) reached the change, coverage 100%
- [valkey-io/valkey#855](https://github.com/valkey-io/valkey/pull/855) ([tests](tests/valkey/855-facd123ce/)): Update redis.conf to valkey.conf in log message (#855): declined: The change only rewords a log message (redis.conf -> valkey.conf) in the replication bulk-transfer timeout warning in replicationCron. No command reply, replication logic, persistence, or config behaviour changes, so there is nothing a scenario could exercise or an oracle could judge; no t
- [valkey-io/valkey#864](https://github.com/valkey-io/valkey/pull/864) ([tests](tests/valkey/864-054ffd140/)): Fix outdated comment of migrate in valkey-cli --cluster (#864): declined: The change edits only a code comment in valkey-cli's clusterManagerMigrateKeysInSlot (describing when --cluster-replace applies); no executable code, output, or behaviour is modified, so there is nothing a scenario could observe differing between the two builds.
- [valkey-io/valkey#851](https://github.com/valkey-io/valkey/pull/851) ([tests](tests/valkey/851-4b8de6b1b/)): Update replica version comparison to handle version 8 RC candidates (#851): declined: The change only alters how CLUSTER SETSLOT classifies a replica whose reported version falls in [7.2.255, 8.0.0) (the 8.0 release candidates): the primary now replicates SETSLOT to such replicas instead of using the non-replicated fallback. Reaching the branch requires a genuine cluster re
- [valkey-io/valkey#834](https://github.com/valkey-io/valkey/pull/834) ([tests](tests/valkey/834-1d1884207/)): Fix bug in writeToClient (#834): 0 of 4 test(s) reached the change, coverage 100%
- [valkey-io/valkey#635](https://github.com/valkey-io/valkey/pull/635) ([tests](tests/valkey/635-fa238dc04/)): Update dir in valkey.conf to mention cluster-config-file (#635): declined: The change adds three comment lines to the sample valkey.conf documenting that a relative cluster-config-file path is resolved under 'dir'. It is documentation only: no source code, config parsing, or runtime behaviour changes, so there is no path a scenario could reach or distinguish betw
- [valkey-io/valkey#840](https://github.com/valkey-io/valkey/pull/840) ([tests](tests/valkey/840-aaa783436/)): Handle underflow condition of network out slot stats metric (#840): 0 of 3 test(s) reached the change, coverage 100%
- [valkey-io/valkey#836](https://github.com/valkey-io/valkey/pull/836) ([tests](tests/valkey/836-b4d96caa7/)): Remove static to avoid compiler warning (#836): declined: The change removes 'static' from createClusterMsgSendBlock in cluster_legacy.c solely to silence a compiler warning. It changes the function's linkage only; the function body, call sites, and all runtime behavior (cluster message allocation and sending) are identical to the parent build. A
- [valkey-io/valkey#826](https://github.com/valkey-io/valkey/pull/826) ([tests](tests/valkey/826-e32518d65/)): Fix unexpected behavior when turning appendonly on and off within a transaction (#826): declined: The fix only guards stopAppendOnly() against operating on an already-closed descriptor (aof_fd == -1) and adds log lines. That path is reached when AOF is enabled-but-merely-scheduled (inside EXEC, or with another background child active) and then disabled in the same transaction, leaving 
- [valkey-io/valkey#720](https://github.com/valkey-io/valkey/pull/720) ([tests](tests/valkey/720-e1d936b33/)): Add network-bytes-in and network-bytes-out metric support under CLUSTER SLOT-STATS command (#20) (#720): 4 of 6 test(s) reached the change, coverage 92%; crash, predates #720, since fixed (recorded)
- [valkey-io/valkey#654](https://github.com/valkey-io/valkey/pull/654) ([tests](tests/valkey/654-e745e9c24/)): Adds Light-weight cluster bus header for pubsub message. (#654): 0 of 4 test(s) reached the change, coverage 86%; crash, predates #654, since fixed (recorded)
- [valkey-io/valkey#804](https://github.com/valkey-io/valkey/pull/804) ([tests](tests/valkey/804-48ca2c917/)): Improve dual channel replication stability and fix compatibility issues (#804): 1 of 10 test(s) reached the change, coverage 85%
- [valkey-io/valkey#806](https://github.com/valkey-io/valkey/pull/806) ([tests](tests/valkey/806-da286a599/)): Optimize the logic for checking the conversion of zset from listpack to skiplist during the ZADD operation. (#806): 2 of 2 test(s) reached the change, coverage 100%
- [valkey-io/valkey#817](https://github.com/valkey-io/valkey/pull/817) ([tests](tests/valkey/817-5c073a58e/)): Increase rioConnsetWrite max chunk size to 16K (#817): 0 of 3 test(s) reached the change, coverage 100%
- [valkey-io/valkey#823](https://github.com/valkey-io/valkey/pull/823) ([tests](tests/valkey/823-3cca26881/)): Improve CPU usage in tests with IO threads (#823): 0 of 3 test(s) reached the change, coverage 100%
- [valkey-io/valkey#818](https://github.com/valkey-io/valkey/pull/818) ([tests](tests/valkey/818-f00c8f621/)): Modify clusterSaveConfig function call to use C_OK / C_ERR return value (#818): declined: Pure cosmetic refactor: the change substitutes the macros C_OK/C_ERR for the literals 0/-1 in two comparisons of clusterSaveConfig's return value (clusterSaveConfigOrDie and CLUSTER SAVECONFIG) and updates a comment. server.h defines C_OK as 0 and C_ERR as -1, so the compiled behaviour is 

### valkey-io/valkey-bloom

[2026-11-11T20:16:25+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/43094842020): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-json

[2026-11-12T00:53:40+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/43130531504): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-search

[2026-11-11T20:13:55+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/43094395980): 0 merge(s) tested, 0 without a code change.


## Totals

- Merges whose tests reached the change: 827 of 1323 tested (63%)
- Changed-line coverage: median 100% of 837 merge(s) measured, 65 under 50%
- Recall: 221 parents flagged of 350 fix merges
- Claude: $813.94 in the last 24 hours, $9593.12 in the last 7 days, $9629.81 in all
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
