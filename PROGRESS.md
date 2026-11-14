# Fuzzer walk

Updated 2026-11-14T02:22+00:00.

| Repository | Done | Remaining | Backlog cursor | Last 24 hours | Last 7 days (per day) |
|---|---|---|---|---|---|
| valkey-io/valkey | 2217 | 322 | #142 (2024-04-02) | 246 | 247.4 |
| valkey-io/valkey-bloom | 109 | 0 | ce516d0ef6e3 (2024-04-26) | 0 | 0.0 |
| valkey-io/valkey-json | 66 | 0 | c1423d7e4b38 (2024-11-25) | 2 | 0.6 |
| valkey-io/valkey-search | 687 | 1 | f8106e72e984 (2024-11-27) | 14 | 48.0 |

## Last runs

### valkey-io/valkey

[2026-11-14T01:52:04+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/43533185226): 20 merge(s) tested, 3 without a code change.

- [valkey-io/valkey#5389](https://github.com/valkey-io/valkey/pull/5389) ([tests](tests/valkey/5389-74b083139/)): Don't queue a throttled primary when a timed-out script ends (#5389): 0 of 5 test(s) reached the change, coverage 100%
- [valkey-io/valkey#181](https://github.com/valkey-io/valkey/pull/181) ([tests](tests/valkey/181-edee864b3/)): rename redisOp to serverOp (#181): declined: The change only renames the typedef redisOp to serverOp in src/server.h and updates the three references in src/server.c (serverOpArrayAppend, serverOpArrayFree, propagatePendingCommands). A C typedef rename is purely a compile-time identifier change: struct layout, sizeof, allocation, and
- [valkey-io/valkey#180](https://github.com/valkey-io/valkey/pull/180) ([tests](tests/valkey/180-8b3ab8f74/)): Rename redisAtomic to serverAtomic (#180): declined: The change is a mechanical identifier rename: the redisAtomic macro becomes serverAtomic, with the same expansion (#define serverAtomic / #define serverAtomic _Atomic) and every declaration site renamed in lockstep. No behaviour, reply, data format, or concurrency semantics change; an inco
- [valkey-io/valkey#179](https://github.com/valkey-io/valkey/pull/179) ([tests](tests/valkey/179-f753db514/)): rename redis functions in server.h (#179): declined: The change is a pure identifier rename (redisFork/redisSetProcTitle/redisSetCpuAffinity/redisCommunicateSystemd/redisPopcount to server* equivalents) across server.h and its call sites, plus one comment update. No function body, argument, return value, reply, config or protocol behavior ch
- [valkey-io/valkey#178](https://github.com/valkey-io/valkey/pull/178) ([tests](tests/valkey/178-add5f5615/)): Rename some redis structs to server (#178): declined: The change only renames internal C struct typedefs (redisFunctionSym, redisSortObject, redisSortOperation) to server-prefixed names and updates their use sites in sort.c and the build-static-symbols.tcl generator. Struct layouts, logic, and all behaviour are identical; it is a pure refacto
- [valkey-io/valkey#29](https://github.com/valkey-io/valkey/pull/29) ([tests](tests/valkey/29-1736018aa/)): Remove trademarked wording on configuration file and individual configs (#29): declined: The change is purely cosmetic: it renames the example config template redis.conf to valkey.conf, rewords comments in src/*.c/.h, updates the --help usage strings in server.c, and touches Tcl test-harness and utils/ scripts. No config directive, command, reply, replication, persistence, or 
- [valkey-io/valkey#177](https://github.com/valkey-io/valkey/pull/177) ([tests](tests/valkey/177-1629e28f8/)): Rename redisError to serverError (#177): declined: The change is a pure refactor: the struct 'redisError' is renamed to 'serverError' in server.h and its two use sites in server.c (incrementErrorCount and the INFO errorstat section). The struct layout and all logic are byte-for-byte identical; no command reply, data path, replication, or p
- [valkey-io/valkey#176](https://github.com/valkey-io/valkey/pull/176) ([tests](tests/valkey/176-c413834da/)): Rename redisTLSContextConfig to serverTLSContextConfig (#176): declined: The change only renames the typedef redisTLSContextConfig to serverTLSContextConfig in server.h and tls.c (including a comment). The struct's fields, layout, and all logic are untouched, so the compiled behaviour is identical on both builds. It is a pure refactor with no testable behaviour
- [valkey-io/valkey#175](https://github.com/valkey-io/valkey/pull/175) ([tests](tests/valkey/175-25122b140/)): Rename redisObject to serverObject (#175): declined: The change is a pure compile-time refactor: it renames the C struct tag redisObject to serverObject (and forward declarations of it in rio.h and module.c, plus one comment in evict.c), updating the `typedef struct serverObject robj` alias in server.h in the same commit. The struct's fields
- [valkey-io/valkey#174](https://github.com/valkey-io/valkey/pull/174) ([tests](tests/valkey/174-b19ebaf55/)): Rename redisCommand to serverCommand (#174): declined: The change is a mechanical rename of the internal C struct 'redisCommand' to 'serverCommand': all 340 changed lines in the diff contain the old or new type name, and none alters a string literal, reply, error message, config, protocol byte, or module-API symbol. It is a compile-time refact
- [valkey-io/valkey#173](https://github.com/valkey-io/valkey/pull/173) ([tests](tests/valkey/173-a236fc8ef/)): Rename redisCommandProc, redisGetKeysProc to server prefix (#173): declined: The change is a pure identifier rename: the typedefs redisCommandProc and redisGetKeysProc become serverCommandProc and serverGetKeysProc, with the three references in server.h, db.c and networking.c updated to match. The function signatures, struct fields and all runtime logic are unchang
- [valkey-io/valkey#172](https://github.com/valkey-io/valkey/pull/172) ([tests](tests/valkey/172-99bdcc0ed/)): Rename redisCommandGroup to serverCommandGroup (#172): declined: The change only renames the typedef redisCommandGroup to serverCommandGroup and updates two comments referencing it; the enum values, COMMAND_GROUP_STR contents, struct redisCommand layout and all runtime behaviour are unchanged, so the compiled binaries are behaviourally identical and no 
- [valkey-io/valkey#166](https://github.com/valkey-io/valkey/pull/166) ([tests](tests/valkey/166-def09488a/)): Rename redis_member2struct ro  server_member2struct (#166): declined: The change only renames the macro redis_member2struct to server_member2struct in server.h and updates its two call sites in multi.c; the macro body and all runtime behavior are identical, so this is a pure refactor with no observable behavior change and nothing to test.
- [valkey-io/valkey#165](https://github.com/valkey-io/valkey/pull/165) ([tests](tests/valkey/165-69d28be0f/)): Rename redis to valkey in create-cluster script (#165): declined: The change edits only utils/create-cluster/create-cluster, a developer helper shell script, replacing the redis-server/redis-cli binary names with valkey-server/valkey-cli and updating its usage text. No server, valkey-cli, valkey-benchmark, or check-tool code changes; the script is not sh
- [valkey-io/valkey#157](https://github.com/valkey-io/valkey/pull/157) ([tests](tests/valkey/157-730174445/)): Rename redisOpArray to serverOpArray (#157): declined: The change is a pure refactor: it renames the type redisOpArray to serverOpArray and the functions redisOpArrayAppend/redisOpArrayFree to serverOpArrayAppend/serverOpArrayFree in server.c and server.h, updating the two call sites (alsoPropagate, propagatePendingCommands) and a comment. The
- [valkey-io/valkey#160](https://github.com/valkey-io/valkey/pull/160) ([tests](tests/valkey/160-cbbaf69d1/)): Remove unused REDIS_TEST_MAIN dead code in crc64.c (#160): declined: The change only deletes a REDIS_TEST_MAIN-guarded main() in src/crc64.c, dead code never compiled into any shipped binary (the macro is not defined by the build). No runtime behaviour of valkey-server or the tools changes, so no scenario can reach or distinguish it.
- [valkey-io/valkey#156](https://github.com/valkey-io/valkey/pull/156) ([tests](tests/valkey/156-717dfe802/)): Rename redisDb to serverDb (#156): declined: The change is a mechanical rename of the internal typedef redisDb to serverDb across the source tree. Every changed line in the diff touches only that identifier (type declarations, function prototypes, local variables); no function body logic, struct field, config, command, reply, or on-w
- [valkey-io/valkey#159](https://github.com/valkey-io/valkey/pull/159) ([tests](tests/valkey/159-98892bb5c/)): Rename redisMemOverhead to serverMemOverhead (#159): declined: The change is a pure source-level rename of the C struct redisMemOverhead to serverMemOverhead across its declaration in server.h and its uses in object.c and server.c. No struct field, logic, command reply, config or wire format changes; any missed reference would be a compile error, not 
- [valkey-io/valkey#132](https://github.com/valkey-io/valkey/pull/132) ([tests](tests/valkey/132-7a7288b29/)): Log unused module configuration entries that cause redis to abort (#132): declined: The change only adds serverLog lines enumerating unused module config names immediately before a pre-existing exit(1) at startup; the abort behavior, exit path, and all runtime behavior are unchanged. The path fires only when a config file names a module config without its loadmodule direc
- [valkey-io/valkey#63](https://github.com/valkey-io/valkey/pull/63) ([tests](tests/valkey/63-42e00635a/)): Fix inconsistencies in the json command file descriptions (#63): declined: The change only edits the natural-language 'description' strings inside reply_schema blocks of src/commands/*.json (capitalization and trailing periods). These strings are compiled into the server only under -DLOG_REQ_RES (src/Makefile gates --with-reply-schema on it), which the standard b

### valkey-io/valkey-bloom

[2026-11-14T02:20:26+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/43545678555): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-json

[2026-11-14T02:22:47+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/43545906391): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-search

[2026-11-14T01:36:57+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/43535775954): 1 merge(s) tested, 2 without a code change.

- [valkey-io/valkey-search#1647](https://github.com/valkey-io/valkey-search/pull/1647): to retry, the job left no result

## Totals

- Merges whose tests reached the change: 899 of 1433 tested (63%)
- Changed-line coverage: median 100% of 926 merge(s) measured, 67 under 50%
- Recall: 232 parents flagged of 366 fix merges
- Claude: $586.63 in the last 24 hours, $6908.48 in the last 7 days, $10533.71 in all
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

- [valkey-io/valkey-search#1647](https://github.com/valkey-io/valkey-search/pull/1647): attempt 0 of 3, the job left no result
