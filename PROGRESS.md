# Fuzzer walk

Updated 2026-11-13T20:43+00:00.

| Repository | Done | Remaining | Backlog cursor | Last 24 hours | Last 7 days (per day) |
|---|---|---|---|---|---|
| valkey-io/valkey | 2169 | 367 | #213 (2024-04-05) | 198 | 251.9 |
| valkey-io/valkey-bloom | 109 | 0 | ce516d0ef6e3 (2024-04-26) | 0 | 0.0 |
| valkey-io/valkey-json | 65 | 0 | c1423d7e4b38 (2024-11-25) | 1 | 0.4 |
| valkey-io/valkey-search | 682 | 0 | f8106e72e984 (2024-11-27) | 9 | 53.3 |

## Last runs

### valkey-io/valkey

[2026-11-13T20:23:56+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/43477102097): 20 merge(s) tested, 4 without a code change.

- [valkey-io/valkey#268](https://github.com/valkey-io/valkey/pull/268) ([tests](tests/valkey/268-b669af0ea/)): Rename 'redis_tls_ctx' and 'redis_tls_client_ctx' global variables (#268): declined: The change only renames the internal global variables redis_tls_ctx/redis_tls_client_ctx to valkey_tls_ctx/valkey_tls_client_ctx in src/tls.c and src/sentinel.c. Control flow, TLS configuration, cleanup and connection creation are unchanged; the symbols are not externally visible behaviour
- [valkey-io/valkey#284](https://github.com/valkey-io/valkey/pull/284) ([tests](tests/valkey/284-2e4604662/)): Rename macros in valkey-cli.c and redis_strlcpy to valkey (#284): declined: The change is a pure identifier rename: redis_strlcpy becomes valkey_strlcpy in its definition, declaration and every call site, and valkey-cli.c's REDIS_CLI_* macros become CLI_* while keeping the same values — the environment variable names (REDISCLI_AUTH, REDISCLI_HISTFILE, REDISCLI_RCF
- [valkey-io/valkey#281](https://github.com/valkey-io/valkey/pull/281) ([tests](tests/valkey/281-f8cec23a9/)): Delete old deprecated cli program redis-trib (#281): declined: The change only deletes src/redis-trib.rb, a deprecated standalone Ruby stub script that merely printed a deprecation notice pointing to redis-cli --cluster and exited 1, plus a comment-only edit in a Tcl test helper. No server code, no shipped binary (valkey-server, valkey-cli, valkey-ben
- [valkey-io/valkey#222](https://github.com/valkey-io/valkey/pull/222) ([tests](tests/valkey/222-05d16579e/)): Rename redis in valkey-cli file comments and prints (#222): 5 of 6 test(s) reached the change, coverage 85%
- [valkey-io/valkey#259](https://github.com/valkey-io/valkey/pull/259) ([tests](tests/valkey/259-4a8b4f422/)): Rename redis.info to valkey.info in LCOV (#259): declined: The change only renames the LCOV coverage output file from redis.info to valkey.info in the src/Makefile lcov target and updates src/.gitignore to match. It is a build/CI-only change: no server code, command behaviour, replies, replication, persistence or tooling is affected, so no runtime
- [valkey-io/valkey#275](https://github.com/valkey-io/valkey/pull/275) ([tests](tests/valkey/275-03650e91b/)): Revert the default PID file back to the real default (#275): declined: The change edits only a comment in valkey.conf, correcting the documented default PID file path from /var/run/valkey.pid back to /var/run/redis.pid. No C source, config default, or runtime behaviour changes; the server's actual pidfile handling in src/config.c and src/server.c is untouched
- [valkey-io/valkey#270](https://github.com/valkey-io/valkey/pull/270) ([tests](tests/valkey/270-da831c0d2/)): rename procedure redis_deferring_client to valkey_deferring_client (#270): declined: The change only renames the Tcl test helper proc redis_deferring_client to valkey_deferring_client throughout the project's own test suite (tests/*.tcl) and updates a comment in src/logreqres.c that referenced the old proc name. No server code path, command reply, replication, persistence 
- [valkey-io/valkey#223](https://github.com/valkey-io/valkey/pull/223) ([tests](tests/valkey/223-df5db0627/)): Remove trademarked language in code comments (#223): declined: The change only rewords code comments (and comment lines in deps/Makefile) to remove trademarked 'Redis' language. A scan of the full diff shows every added/removed line is a comment: no code, string literal, config, or build rule changes, so there is no behaviour to test and a compared ru
- [valkey-io/valkey#266](https://github.com/valkey-io/valkey/pull/266) ([tests](tests/valkey/266-1aa13decf/)): Remove single node cluster validation check from benchmark (#266): 2 of 2 test(s) reached the change, coverage 100%
- [valkey-io/valkey#265](https://github.com/valkey-io/valkey/pull/265) ([tests](tests/valkey/265-e59dd41e4/)): Maintain determinstic ordering of replica(s) in CLUSTER SLOTS response (#265): 2 of 3 test(s) reached the change, coverage 100%
- [valkey-io/valkey#234](https://github.com/valkey-io/valkey/pull/234) ([tests](tests/valkey/234-5bccd7b80/)): Rename systemd files and content to valkey from redis (#234): declined: The change only renames example systemd service unit files in utils/ (systemd-redis_server.service to systemd-valkey_server.service and the multiple-servers template) and rewords their comments, descriptions, and example paths from redis to valkey. These are packaging/documentation artifac
- [valkey-io/valkey#263](https://github.com/valkey-io/valkey/pull/263) ([tests](tests/valkey/263-750e94cad/)): Update crash wording to include our repo (#263): declined: The change only edits static log-message strings in bugReportEnd (the crash bug-report footer), swapping Redis wording and URLs for Valkey ones. No command behaviour, reply, replication, persistence or control-flow path changes, so there is nothing an experiment's oracles or a compared seq
- [valkey-io/valkey#52](https://github.com/valkey-io/valkey/pull/52) ([tests](tests/valkey/52-ebfb44062/)): Pass extensions to node if extension processing is handled by it (#52): 0 of 2 test(s) reached the change, coverage 100%
- [valkey-io/valkey#262](https://github.com/valkey-io/valkey/pull/262) ([tests](tests/valkey/262-d92dc78cb/)): Update ValkeyModuleEvent_MasterLinkChange to ValkeyModuleEvent_PrimaryLinkChange (#262): declined: The change only renames the header symbol ValkeyModuleEvent_MasterLinkChange to ValkeyModuleEvent_PrimaryLinkChange (plus the matching Redis-compat #define, a doc comment, and the hooks.c test module's use of the name). The event struct is static const in the header with an unchanged value
- [valkey-io/valkey#251](https://github.com/valkey-io/valkey/pull/251) ([tests](tests/valkey/251-6411629c6/)): Madelyn's attempt as a logo (#251): declined: The change only swaps the ASCII art logo string in src/asciilogo.h, printed to the log at startup. The printf-style format specifiers (%s (%s/%d) %s bit, %s mode, Port: %d, PID: %ld) are unchanged in kind and order, so the banner rendering code remains correct. No command behaviour, replie
- [valkey-io/valkey#252](https://github.com/valkey-io/valkey/pull/252) ([tests](tests/valkey/252-d26d596b3/)): Log branding (#252): declined: The change is pure log-message branding: three serverLog format strings in server.c change the word 'Redis' to 'Valkey' in the startup banner, the shutdown 'ready to exit' message, and the no-config-file warning. No command, reply, config, replication, persistence or control-flow behaviour
- [valkey-io/valkey#244](https://github.com/valkey-io/valkey/pull/244) ([tests](tests/valkey/244-1b47daff0/)): Fix: Typo clock_getting -> clock_gettime (#244): declined: The change edits only a comment in src/monotonic.c, correcting the typo 'clock_getting' to 'clock_gettime'. No executable code, reply, config, or behaviour changes, so no scenario can exercise it and none is needed.
- [valkey-io/valkey#248](https://github.com/valkey-io/valkey/pull/248) ([tests](tests/valkey/248-9f03dfc1f/)): Fix two typos that were flagged in the 7.2 build (#248): declined: The change edits only comments: a typo in a Makefile comment ('unstall' -> 'uninstall') and a typo in a C comment in src/eval.c ('everytime' -> 'every time'). No executable code, build rule, config, or protocol behavior changes, so no command reply, replication, persistence or tool path ca
- [valkey-io/valkey#243](https://github.com/valkey-io/valkey/pull/243) ([tests](tests/valkey/243-717ec1e14/)): Rename ValkeyModule_DefragModuleString to ValkeyModule_DefragValkeyModuleString (#243): declined: This is a cosmetic rename of a module-API symbol (ValkeyModule_DefragModuleString -> ValkeyModule_DefragValkeyModuleString) in module.c, redismodule.h and valkeymodule.h, with no change to the function body or behavior. The registered API name only has to agree between a module and the ser
- [valkey-io/valkey#193](https://github.com/valkey-io/valkey/pull/193) ([tests](tests/valkey/193-ba0c93cbd/)): Add redis symlinks at the same place as the installed binaries (#193): declined: The change is make install/uninstall machinery (redis-* compatibility symlinks) plus an argv[0] dispatch in main() that runs redis_check_rdb_main/redis_check_aof_main only when the binary is invoked under a redis-* name. Those names exist only as symlinks created by `make install`, which t

### valkey-io/valkey-bloom

[2026-11-13T20:41:37+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/43503795926): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-json

[2026-11-13T20:43:16+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/43499125197): 1 merge(s) tested, 0 without a code change.

- [valkey-io/valkey-json#126](https://github.com/valkey-io/valkey-json/pull/126) ([tests](tests/valkey-json/126-aa89a711d/)): fix(jsonpath): apply wildcard and filter segments sequentially (#126): 4 of 4 test(s) reached the change

### valkey-io/valkey-search

[2026-11-13T20:32:30+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/43501885386): 0 merge(s) tested, 0 without a code change.


## Totals

- Merges whose tests reached the change: 893 of 1425 tested (63%)
- Changed-line coverage: median 100% of 920 merge(s) measured, 66 under 50%
- Recall: 232 parents flagged of 366 fix merges
- Claude: $514.34 in the last 24 hours, $7287.30 in the last 7 days, $10461.43 in all
- Issues filed: 77
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
