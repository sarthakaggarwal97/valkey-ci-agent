# Fuzzer walk

Updated 2026-11-10T02:07+00:00.

| Repository | Done | Remaining | Backlog cursor | Last 24 hours | Last 7 days (per day) |
|---|---|---|---|---|---|
| valkey-io/valkey | 1369 | 1144 | #1842 (2025-03-13) | 314 | 195.6 |
| valkey-io/valkey-bloom | 109 | 0 | ce516d0ef6e3 (2024-04-26) | 0 | 15.6 |
| valkey-io/valkey-json | 63 | 0 | c1423d7e4b38 (2024-11-25) | 1 | 9.0 |
| valkey-io/valkey-search | 671 | 0 | f8106e72e984 (2024-11-27) | 16 | 95.9 |

## Last runs

### valkey-io/valkey

[2026-11-10T02:07:16+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42730296056): 20 merge(s) tested, 4 without a code change.

- [valkey-io/valkey#5263](https://github.com/valkey-io/valkey/pull/5263) ([tests](tests/valkey/5263-5ed2d2564/)): Loop over forkless-policy values directly in rdb tests (#5263): declined: The change only refactors Valkey's own TCL test suite (tests/integration/rdb.tcl), replacing expr-based ternaries with direct foreach loops over equivalent forkless-policy values, and rewords a comment in valkey.conf. No server code, command behavior, config semantics, or persistence logic
- [valkey-io/valkey#5117](https://github.com/valkey-io/valkey/pull/5117) ([tests](tests/valkey/5117-d5b4fda74/)): Allow the Lua debugger to start on an empty script (#5117): 2 of 2 test(s) reached the change; crash, fixed by #5117 (recorded)
- [valkey-io/valkey#5134](https://github.com/valkey-io/valkey/pull/5134) ([tests](tests/valkey/5134-116e7f953/)): Do not adopt the MYSELF flag from a gossip entry (#5134): declined: The fix strips CLUSTER_NODE_MYSELF when creating a node from a gossip entry in clusterProcessGossipSection. That path fires only when a cluster-bus gossip entry about an unknown node carries the MYSELF flag, which a conforming Valkey never emits (clusterSetGossipEntry copies a node's own f
- [valkey-io/valkey#1918](https://github.com/valkey-io/valkey/pull/1918) ([tests](tests/valkey/1918-cc31c20a4/)): Add human node name to log statement with node name (#1918): declined: The change only adds the already-always-initialized human_nodename field (an sds set to sdsempty() at node creation, never NULL) to two existing LL_DEBUG log format strings in cluster_legacy.c, matching the pattern used by dozens of other log lines in the file. It alters no command replies
- [valkey-io/valkey#1915](https://github.com/valkey-io/valkey/pull/1915) ([tests](tests/valkey/1915-add716b7d/)): Fix valkey-cli port parse logic for invalid string (#1915): 1 of 1 test(s) reached the change, coverage 100%
- [valkey-io/valkey#1559](https://github.com/valkey-io/valkey/pull/1559) ([tests](tests/valkey/1559-5cce16031/)): In LOLWUT's reply, change "Redis ver." to "Valkey ver." (#1559): 2 of 2 test(s) reached the change, coverage 100%
- [valkey-io/valkey#1911](https://github.com/valkey-io/valkey/pull/1911) ([tests](tests/valkey/1911-e5ce98459/)): Initialize child process pipe file descriptor to -1 (#1911): declined: The change initializes two local file descriptors in rdbSaveToReplicasSockets (src/rdb.c) to -1 instead of 0. Both variables are used only inside !dual_channel-guarded blocks, and on every !dual_channel path (including both anetPipe error paths) they are assigned real pipe fds before any c
- [valkey-io/valkey#1908](https://github.com/valkey-io/valkey/pull/1908) ([tests](tests/valkey/1908-f1d8d77fa/)): Bump CMake to 3.10.0 to fix build error on Ubuntu-24 (#1908): declined: The change only raises cmake_minimum_required in deps/hiredis/CMakeLists.txt from 3.0.0 to 3.10.0 to fix a build error on Ubuntu 24. It is a build-system-only change with no effect on server behaviour, commands, replication, persistence, or tools at runtime, so there is nothing for a fuzze
- [valkey-io/valkey#1899](https://github.com/valkey-io/valkey/pull/1899) ([tests](tests/valkey/1899-74b6330f6/)): Update ACL SETUSER command help message (#1899): 2 of 2 test(s) reached the change
- [valkey-io/valkey#1839](https://github.com/valkey-io/valkey/pull/1839) ([tests](tests/valkey/1839-0af8a120d/)): Add --sequential to valkey-benchmark (for populating entire keyspace) (#1839): 3 of 3 test(s) reached the change, coverage 90%
- [valkey-io/valkey#1889](https://github.com/valkey-io/valkey/pull/1889) ([tests](tests/valkey/1889-09f963017/)): Redact protocol error log when hide-user-data-from-log enabled (#1889): 0 of 2 test(s) reached the change, coverage 100%
- [valkey-io/valkey#1891](https://github.com/valkey-io/valkey/pull/1891) ([tests](tests/valkey/1891-da3244352/)): Remove TLSCONN_DEBUG dead code in tls.c (#1891): declined: The change only removes the TLSCONN_DEBUG macro definition and its lone invocation in tls.c. The macro expands to nothing unless TLS_DEBUGGING is defined at compile time, which no supported build enables (the removed call even references an undeclared variable, so it could not compile with
- [valkey-io/valkey#1894](https://github.com/valkey-io/valkey/pull/1894) ([tests](tests/valkey/1894-9abb51846/)): Fix merge error introduced in #1186 (#1894): declined: The fix only changes the deadline value passed to module data-type late-defrag callbacks during active defrag (monotonic endtime instead of an erroneous wall-clock conversion). Reaching defragLaterItem's OBJ_MODULE branch requires loading a module that registers a data type with a defrag c
- [valkey-io/valkey#1885](https://github.com/valkey-io/valkey/pull/1885) ([tests](tests/valkey/1885-f1934cba4/)): Fix crash in VM_GetCurrentUserName when no valid user (#1885): declined: The fix adds a defensive NULL guard to VM_GetCurrentUserName so it returns EINVAL instead of dereferencing ctx->client->user->name when the context has no associated client/user. That NULL-user state only arises for module temp-client contexts (timers, event-loop, server-event callbacks), 
- [valkey-io/valkey#1843](https://github.com/valkey-io/valkey/pull/1843) ([tests](tests/valkey/1843-06990c2e6/)): Update COPY and MOVE command description clearer in JSON file (#1843): declined: The change only rewords two human-readable 'description' strings in the reply_schema sections of copy.json and move.json. Reply schemas are documentation metadata: they are not compiled into normal server builds (only into special reply-schema-validation builds) and never affect command pa
- [valkey-io/valkey#1869](https://github.com/valkey-io/valkey/pull/1869) ([tests](tests/valkey/1869-d56a1f79b/)): [cluster] Add node id to log statement for closing link on first message as lightweight (#1869): declined: The change only adds the sender node id to an existing LL_NOTICE log message when closing a cluster link whose first message was lightweight; the link-closing behaviour, packet handling, and all observable replies/state are unchanged, and hdr->sender is part of the fixed message header alr
- [valkey-io/valkey#1865](https://github.com/valkey-io/valkey/pull/1865) ([tests](tests/valkey/1865-d9ae08684/)): Minor cleanup remove unnecessary cast since slot is int (#1865): declined: The change deletes a no-op (int) cast on a variable that is already an int in an addReplyErrorFormat call in checkSlotAssignmentsOrReply (src/cluster_legacy.c). The format string, argument value, and resulting error reply are identical on both builds, so there is no behaviour to exercise o
- [valkey-io/valkey#1850](https://github.com/valkey-io/valkey/pull/1850) ([tests](tests/valkey/1850-aa8845344/)): Fix RANDOMKEY infinite loop during CLIENT PAUSE (#1850): 0 of 2 test(s) reached the change, coverage 0%
- [valkey-io/valkey#1845](https://github.com/valkey-io/valkey/pull/1845) ([tests](tests/valkey/1845-89b1f8fb1/)): Update valkey-benchmark parseURI function name and comment (#1845): declined: The change renames parseRedisUri to parseUri and updates a comment; the URI parsing logic is byte-for-byte unchanged and all four schemes (valkey://, valkeys://, redis://, rediss://) were already accepted before the change. The only observable difference is the tool_name string in valkey-b
- [valkey-io/valkey#1801](https://github.com/valkey-io/valkey/pull/1801) ([tests](tests/valkey/1801-7d2a69d12/)): Fix defrag when type/encoding changes during scan (#1801): 0 of 6 test(s) reached the change, coverage 100%

### valkey-io/valkey-bloom

[2026-11-10T02:00:28+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42739648049): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-json

[2026-11-10T02:00:49+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42739658018): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-search

[2026-11-10T01:36:53+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42736917394): 0 merge(s) tested, 0 without a code change.


## Totals

- Merges whose tests reached the change: 710 of 1084 tested (65%)
- Changed-line coverage: median 100% of 626 merge(s) measured, 53 under 50%
- Recall: 195 parents flagged of 299 fix merges
- Claude: $1291.07 in the last 24 hours, $7935.48 in the last 7 days, $7935.48 in all
- Issues filed: 64
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

- [valkey-io/valkey#1948](https://github.com/valkey-io/valkey/pull/1948): attempt 0 of 3, Claude Code failed (rc=1): API Error: Claude Code is unable to respond to this request, which appears to violate our Usage Policy (https://www.anthropic.com/legal/aup). This request triggered restrict
