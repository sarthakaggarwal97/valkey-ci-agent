# Fuzzer walk

Updated 2026-11-14T00:17+00:00.

| Repository | Done | Remaining | Backlog cursor | Last 24 hours | Last 7 days (per day) |
|---|---|---|---|---|---|
| valkey-io/valkey | 2194 | 343 | #181 (2024-04-03) | 223 | 248.3 |
| valkey-io/valkey-bloom | 109 | 0 | ce516d0ef6e3 (2024-04-26) | 0 | 0.0 |
| valkey-io/valkey-json | 66 | 0 | c1423d7e4b38 (2024-11-25) | 2 | 0.6 |
| valkey-io/valkey-search | 685 | 0 | f8106e72e984 (2024-11-27) | 12 | 50.1 |

## Last runs

### valkey-io/valkey

[2026-11-14T00:17:21+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/43501198317): 20 merge(s) tested, 5 without a code change.

- [valkey-io/valkey#213](https://github.com/valkey-io/valkey/pull/213) ([tests](tests/valkey/213-620d325fd/)): Adding server.call/pcall option to LUA scripting. (#136) (#213): 4 of 4 test(s) reached the change, coverage 100%
- [valkey-io/valkey#232](https://github.com/valkey-io/valkey/pull/232) ([tests](tests/valkey/232-bc28fb4ac/)): Update Server version to valkey version (#232): 4 of 9 test(s) reached the change, coverage 93%
- [valkey-io/valkey#194](https://github.com/valkey-io/valkey/pull/194) ([tests](tests/valkey/194-aaec32121/)): Remove REDISMODULE_ prefixes and introduce compatibility header (#194): 0 of 12 test(s) reached the change, coverage 18%; [check, predates #194](https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/156)
- [valkey-io/valkey#237](https://github.com/valkey-io/valkey/pull/237) ([tests](tests/valkey/237-906c8e8f9/)): delete cluster fail time script (#237): declined: The change only deletes utils/cluster_fail_time.tcl, a standalone developer utility Tcl script used manually to measure PFAIL->FAIL transition time. It removes no server, tool, or library code and changes no runtime behaviour of any build artifact, so no scenario can reach or test it.
- [valkey-io/valkey#106](https://github.com/valkey-io/valkey/pull/106) ([tests](tests/valkey/106-47444c67d/)): Update Makefile comments to relect name change (#106): declined: The change only renames the TLS module build artifact in src/Makefile (redis-tls.so to valkey-tls.so, applied when BUILD_TLS=module). It is a build-system change with no runtime behaviour: no server code, command, replication, persistence or protocol path differs between the two builds, so
- [valkey-io/valkey#218](https://github.com/valkey-io/valkey/pull/218) ([tests](tests/valkey/218-bb1a3fffe/)): Fix CI break issue due to serverTests merged issue (#218): declined: The change is a one-line compile fix in src/server.c's internal test-harness path (`valkey-server test all`): it renames `serverTest`/`struct redisTest` to the correct identifiers `serverTests`/`struct serverTest` so the file compiles. This code only runs when the binary is invoked with th
- [valkey-io/valkey#216](https://github.com/valkey-io/valkey/pull/216) ([tests](tests/valkey/216-4646d0825/)): Redis in HELP commands (#216): 2 of 2 test(s) reached the change
- [valkey-io/valkey#171](https://github.com/valkey-io/valkey/pull/171) ([tests](tests/valkey/171-29621bc35/)): Update Valkey keyword in sentinel.conf (#171): declined: The change edits only sentinel.conf, the sample Sentinel config template: comment wording (Redis -> Valkey, redis.io -> valkey.io URLs) and the template's default pidfile path string. No source code, command behavior, config parsing, replication, or persistence path changes, and the fuzzer
- [valkey-io/valkey#203](https://github.com/valkey-io/valkey/pull/203) ([tests](tests/valkey/203-6ea6a693e/)): Rename 'redis' to 'server' functions missing (#203): declined: The change is a pure rename refactor: redisIsSupervised becomes serverIsSupervised in server.c and the redisConfig struct with getRedisConfig/freeRedisConfig becomes serverConfig with getServerConfig/freeServerConfig in valkey-benchmark.c. All call sites are updated and no function body, l
- [valkey-io/valkey#113](https://github.com/valkey-io/valkey/pull/113) ([tests](tests/valkey/113-9c5e2bb22/)): Changes references to redis binaries in output of "--help", "--version" (#113): 1 of 1 test(s) reached the change, coverage 100%
- [valkey-io/valkey#191](https://github.com/valkey-io/valkey/pull/191) ([tests](tests/valkey/191-e3e1f9a37/)): Rename 'redis' to 'server' and redisNodeFlags to clusterNodeFlags (#191): declined: The change is a mechanical rename of internal C identifiers (redisBitpos->serverBitpos, redisCommandTable->serverCommandTable, redisNodeFlags->clusterNodeFlags, redisLrand48->serverLrand48, etc.) with every call site updated and no change to any table contents, reply, replication, cluster 
- [valkey-io/valkey#190](https://github.com/valkey-io/valkey/pull/190) ([tests](tests/valkey/190-9a02b775c/)): Replace Valkey in runtest scripts error prints (#190): declined: The change only rewords the 'You need tcl 8.5 or newer' error messages in the runtest, runtest-cluster, runtest-moduleapi and runtest-sentinel test-runner shell scripts, replacing 'Redis' with 'Valkey'. No server code, command behavior, replication, persistence, or bundled tool is touched,
- [valkey-io/valkey#47](https://github.com/valkey-io/valkey/pull/47) ([tests](tests/valkey/47-39d0f457a/)): Update versioning fields for compatibility (#47): 2 of 9 test(s) reached the change, coverage 95%
- [valkey-io/valkey#189](https://github.com/valkey-io/valkey/pull/189) ([tests](tests/valkey/189-55de74e0d/)): The usage (--help) message now refers to valkey (#189): declined: The change rewrites only the fprintf string literals in usage() in src/server.c (the text shown for valkey-server -h/--help or bad arguments) and nothing else: no parsing, reply, persistence, replication, or startup behaviour changes, and the function still just prints to stderr and exits 
- [valkey-io/valkey#167](https://github.com/valkey-io/valkey/pull/167) ([tests](tests/valkey/167-e1cb4c8a8/)): Rename #include guards (#167): declined: The change only renames #include guard macros (__REDIS_* to VALKEY_*) in nine header files. Include guards are compile-time-only symbols that prevent double inclusion; renaming them changes no generated code, no command behaviour, no reply, no persistence or replication path. A successful 
- [valkey-io/valkey#188](https://github.com/valkey-io/valkey/pull/188) ([tests](tests/valkey/188-e4d61c482/)): Rename cli benchmark check-aof and check-rdb src and object files to valkey respectively (#188): declined: The change only renames source files (redis-cli.c, redis-benchmark.c, redis-check-rdb.c, redis-check-aof.c to valkey-* names, all 100% similarity renames with no content change) and updates the Makefile object-file lists and comments accordingly. The compiled binaries keep their existing n
- [valkey-io/valkey#186](https://github.com/valkey-io/valkey/pull/186) ([tests](tests/valkey/186-f3ccfbb01/)): Rename TLS test cert files to valkey (#186): declined: The change only renames TLS test certificate filenames (redis.* to valkey.*) in documentation, the TCL test harness, hiredis's test script, and the cert-generation helper script. No file under src/ changes, so no server, replication, TLS runtime, command, or tool behavior is affected; ther
- [valkey-io/valkey#184](https://github.com/valkey-io/valkey/pull/184) ([tests](tests/valkey/184-125a2987a/)): rename git sha related (#184): declined: The change is a pure symbol rename (redisGitSHA1/redisGitDirty/redisBuildId* -> serverGitSHA1/serverGitDirty/serverBuildId*) across release.c, server.h, server.c, cli_common.c and tls.c. Every call site is updated consistently, the function bodies are untouched, and all externally visible 
- [valkey-io/valkey#183](https://github.com/valkey-io/valkey/pull/183) ([tests](tests/valkey/183-598b951fb/)): rename redisServer to valkeyServer (#183): declined: The change only renames the C struct tag 'redisServer' to 'valkeyServer' in src/server.c and src/server.h. The struct's fields, the global variable name, and all code using it are unchanged, so the compiled behaviour is identical: no command reply, replication, persistence, or cluster path
- [valkey-io/valkey#182](https://github.com/valkey-io/valkey/pull/182) ([tests](tests/valkey/182-3a0ba0ad9/)): rename redisCommandArgType serverCommandArgType (#182): declined: The change only renames the C identifiers redisCommandArgType/redisCommandArg to serverCommandArgType/serverCommandArg and updates all references and one comment. The struct layout, the module-API conversion logic (moduleCopyCommandArgs, moduleConvertArgType), and the COMMAND DOCS reply co

### valkey-io/valkey-bloom

[2026-11-14T00:17:30+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/43533113901): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-json

[2026-11-13T23:48:45+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/43529777655): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-search

[2026-11-14T00:10:25+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/43532258586): 0 merge(s) tested, 0 without a code change.


## Totals

- Merges whose tests reached the change: 899 of 1432 tested (63%)
- Changed-line coverage: median 100% of 925 merge(s) measured, 67 under 50%
- Recall: 232 parents flagged of 366 fix merges
- Claude: $572.54 in the last 24 hours, $7035.86 in the last 7 days, $10519.63 in all
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
