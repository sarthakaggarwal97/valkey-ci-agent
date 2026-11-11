# Fuzzer walk

Updated 2026-11-11T20:13+00:00.

| Repository | Done | Remaining | Backlog cursor | Last 24 hours | Last 7 days (per day) |
|---|---|---|---|---|---|
| valkey-io/valkey | 1852 | 672 | #881 (2024-08-16) | 268 | 264.6 |
| valkey-io/valkey-bloom | 109 | 0 | ce516d0ef6e3 (2024-04-26) | 0 | 15.6 |
| valkey-io/valkey-json | 64 | 0 | c1423d7e4b38 (2024-11-25) | 1 | 9.1 |
| valkey-io/valkey-search | 672 | 0 | f8106e72e984 (2024-11-27) | 0 | 96.0 |

## Last runs

### valkey-io/valkey

[2026-11-11T19:56:31+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/43062859018): 20 merge(s) tested, 0 without a code change.

- [valkey-io/valkey#1535](https://github.com/valkey-io/valkey/pull/1535) ([tests](tests/valkey/1535-e60990e57/)): Fix crash when freeing newly created node when nodeIp2String fail (#1535): 0 of 9 test(s) reached the change, coverage 50%
- [valkey-io/valkey#951](https://github.com/valkey-io/valkey/pull/951) ([tests](tests/valkey/951-694246cfa/)): Drop the outdated script replication example comments (#951): declined: The change is purely a comment edit in afterErrorReply (src/networking.c): it replaces an outdated EVAL script-replication example with updated explanatory text. No executable code, control flow, reply, replication behaviour, or configuration changes, so there is no behaviour to regress an
- [valkey-io/valkey#792](https://github.com/valkey-io/valkey/pull/792) ([tests](tests/valkey/792-d66a06e81/)): Make KEYS to be an exact match if there is no pattern (#792): 0 of 3 test(s) reached the change, coverage 100%
- [valkey-io/valkey#948](https://github.com/valkey-io/valkey/pull/948) ([tests](tests/valkey/948-73698fa02/)): Fix invalid escape sequence in utils, minor cleanup in python script  (#948): declined: The change edits only utils/generate-unit-test-header.py, a build-time Python helper that generates the unit-test header: it makes the TEST_PROTOTYPE regex a raw string to fix a Python invalid-escape-sequence warning. The regex matches the same patterns as before, and no server, replicatio
- [valkey-io/valkey#938](https://github.com/valkey-io/valkey/pull/938) ([tests](tests/valkey/938-9f4b1adbe/)): Add explicit assert to ensure thread_shared_qb won't expand (#938): 0 of 1 test(s) reached the change, coverage 100%
- [valkey-io/valkey#816](https://github.com/valkey-io/valkey/pull/816) ([tests](tests/valkey/816-c7d1daea0/)): Add epoch information to failover auth denied logs (#816): declined: The change only edits the text of three serverLog warning messages in clusterSendFailoverAuthIfNeeded, appending the requested epoch to the 'Failover auth denied' logs. No control flow, vote logic, cluster bus message, reply, or persisted state changes, so there is no behaviour a scenario'
- [valkey-io/valkey#594](https://github.com/valkey-io/valkey/pull/594) ([tests](tests/valkey/594-0053429a0/)): Update "Total" message and used_memory_human log information in serverCron() function (#594): declined: The change only edits the text of a periodic LL_DEBUG log line in serverCron() (adding a human-readable form of zmalloc_used_memory() via the existing bytesToHuman helper, whose prototype is added to server.h). It alters no command replies, no replication, persistence, or cluster behaviour
- [valkey-io/valkey#901](https://github.com/valkey-io/valkey/pull/901) ([tests](tests/valkey/901-b48596a91/)): Add support for setting the group on a unix domain socket (#901): 3 of 7 test(s) reached the change, coverage 66%
- [valkey-io/valkey#928](https://github.com/valkey-io/valkey/pull/928) ([tests](tests/valkey/928-804599497/)): valkey-cli make source node ignores NOREPLICAS when doing the last CLUSTER SETSLOT (#928): declined: The change is confined to valkey-cli's cluster manager: clusterManagerMoveSlot now treats a NOREPLICAS reply to the final CLUSTER SETSLOT <slot> NODE on the source primary as benign (valkey-cli.c:5044). That reply only occurs on a multi-primary cluster where the source primary pre-replicat
- [valkey-io/valkey#879](https://github.com/valkey-io/valkey/pull/879) ([tests](tests/valkey/879-5d97f5133/)): Fix CLUSTER SETSLOT block and unblock error when all replicas are down (#879): 1 of 1 test(s) reached the change, coverage 100%
- [valkey-io/valkey#930](https://github.com/valkey-io/valkey/pull/930) ([tests](tests/valkey/930-0a11c4a14/)): Delete redundant declaration clusterNodeCoversSlot and countKeysInSlot (#930): declined: The diff deletes two redundant forward declarations (clusterNodeCoversSlot and countKeysInSlot) from src/cluster_legacy.c; the functions' definitions, call sites, and remaining declarations are unchanged, so the compiled code and all runtime behaviour are identical. This is a pure code-cle
- [valkey-io/valkey#759](https://github.com/valkey-io/valkey/pull/759) ([tests](tests/valkey/759-959dd3485/)): Decline unsubscribe related command in non-subscribed mode (#759): declined: no proposed test validated
- [valkey-io/valkey#884](https://github.com/valkey-io/valkey/pull/884) ([tests](tests/valkey/884-08aaeea4b/)): Avoid to re-establish replication if node is already myself primary in CLUSTER REPLICATE (#884): 0 of 3 test(s) reached the change, coverage 100%
- [valkey-io/valkey#911](https://github.com/valkey-io/valkey/pull/911) ([tests](tests/valkey/911-a1ac459ef/)): Set repl-backlog-size from 1mb to 10mb by default (#911): 1 of 4 test(s) reached the change
- [valkey-io/valkey#916](https://github.com/valkey-io/valkey/pull/916) ([tests](tests/valkey/916-2673320b6/)): RDMA: Support user keepalive command (#916): declined: The change is confined to src/rdma.c: a per-connection keepalive timer, zero-initialized ValkeyRdmaCmd structs, and event-loop plumbing in the RDMA accept/disconnect path. That code runs only when a client connects over the RDMA transport, which requires a build with BUILD_RDMA and an RDMA
- [valkey-io/valkey#885](https://github.com/valkey-io/valkey/pull/885) ([tests](tests/valkey/885-e1b362918/)): Fix data loss when replica do a failover with a old history repl offset (#885): 1 of 12 test(s) reached the change, coverage 85%
- [valkey-io/valkey#925](https://github.com/valkey-io/valkey/pull/925) ([tests](tests/valkey/925-829243e76/)): Correct RDB_EOF_MARK_SIZE usage where EOF mark is relevant (#925): declined: The change replaces CONFIG_RUN_ID_SIZE with RDB_EOF_MARK_SIZE in readSyncBulkPayload's diskless-sync EOF-mark handling. Both macros are defined as 40 (src/server.h:128-129), so every buffer declaration, memcpy, memmove, memcmp and ftruncate offset in the changed code compiles to the identi
- [valkey-io/valkey#919](https://github.com/valkey-io/valkey/pull/919) ([tests](tests/valkey/919-e2ab7ffd8/)): Make use of a single listNode pointer for blocking utility lists (#919): 0 of 4 test(s) reached the change, coverage 100%; crash, predates #919, since fixed (recorded)
- [valkey-io/valkey#787](https://github.com/valkey-io/valkey/pull/787) ([tests](tests/valkey/787-70b928580/)): Optimize linear search of WAIT and WAITAOF when unblocking the client (#787): 0 of 5 test(s) reached the change, coverage 100%
- [valkey-io/valkey#789](https://github.com/valkey-io/valkey/pull/789) ([tests](tests/valkey/789-33c7ca41b/)): Add 4 commands for sentinel and update most test cases and json files (#789): declined: The change only adds alias subcommand names (SENTINEL PRIMARIES/PRIMARY/IS-PRIMARY-DOWN-BY-ADDR/GET-PRIMARY-ADDR-BY-NAME) inside sentinelCommand plus command-table/JSON metadata and Sentinel TCL test renames; the old MASTER* spellings keep working, so no existing behaviour changes. Every S

### valkey-io/valkey-bloom

[2026-11-11T19:45:39+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/43090077338): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-json

[2026-11-11T20:10:00+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/43093825374): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-search

[2026-11-11T20:13:55+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/43094395980): 0 merge(s) tested, 0 without a code change.


## Totals

- Merges whose tests reached the change: 817 of 1303 tested (63%)
- Changed-line coverage: median 100% of 818 merge(s) measured, 65 under 50%
- Recall: 220 parents flagged of 347 fix merges
- Claude: $877.91 in the last 24 hours, $9516.40 in the last 7 days, $9516.40 in all
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
- [valkey-io/valkey#479](https://github.com/valkey-io/valkey/pull/479): attempt 0 of 3, the job left no result
- [valkey-io/valkey#5209](https://github.com/valkey-io/valkey/pull/5209): attempt 0 of 3, the job left no result
