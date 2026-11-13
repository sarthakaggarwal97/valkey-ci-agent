# Fuzzer walk

Updated 2026-11-13T17:36+00:00.

| Repository | Done | Remaining | Backlog cursor | Last 24 hours | Last 7 days (per day) |
|---|---|---|---|---|---|
| valkey-io/valkey | 2118 | 418 | #238 (2024-04-18) | 147 | 248.1 |
| valkey-io/valkey-bloom | 109 | 0 | ce516d0ef6e3 (2024-04-26) | 0 | 0.0 |
| valkey-io/valkey-json | 64 | 0 | c1423d7e4b38 (2024-11-25) | 0 | 0.3 |
| valkey-io/valkey-search | 681 | 0 | f8106e72e984 (2024-11-27) | 8 | 57.1 |

## Last runs

### valkey-io/valkey

[2026-11-13T16:33:14+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/43455453577): 20 merge(s) tested, 4 without a code change.

- [valkey-io/valkey#406](https://github.com/valkey-io/valkey/pull/406) ([tests](tests/valkey/406-6e05d0fcb/)): Update script to generate Valkey Module API docs (#406): declined: The change only modifies utils/generate-module-api-doc.rb, an offline Ruby script used to generate Module API documentation (renaming RM_/RedisModule_ patterns to VM_/ValkeyModule_ and sourcing the 'since' map from module-api-since.rb). It is a build/docs tooling change with no effect on v
- [valkey-io/valkey#390](https://github.com/valkey-io/valkey/pull/390) ([tests](tests/valkey/390-05251c55d/)): Change default syslog-ident from redis to valkey (#390): 2 of 2 test(s) reached the change
- [valkey-io/valkey#293](https://github.com/valkey-io/valkey/pull/293) ([tests](tests/valkey/293-948cd8f2c/)): Use SOCK_NONBLOCK to reduce system calls for outgoing connections (#293): 0 of 3 test(s) reached the change, coverage 80%
- [valkey-io/valkey#395](https://github.com/valkey-io/valkey/pull/395) ([tests](tests/valkey/395-b0d5a0f58/)): Make sure standard library is imported when using abort (#395): declined: The change only adds #include <stdlib.h> to the fallback branch of the valkey_unreachable macro in src/config.h, taken solely on compilers older than GCC 4.5 that lack __builtin_unreachable. It is a build/portability fix with no runtime behavior change: on any build the fuzzer can run, the
- [valkey-io/valkey#380](https://github.com/valkey-io/valkey/pull/380) ([tests](tests/valkey/380-a5a1377df/)): Update REDIS* to VALKEY* in object.c and utils/create-cluster/README (#380): 6 of 6 test(s) reached the change, coverage 100%
- [valkey-io/valkey#381](https://github.com/valkey-io/valkey/pull/381) ([tests](tests/valkey/381-19c4c647e/)): Fix incorrect comment for count in clusterMsg (#381): declined: The change edits a single source comment in cluster_legacy.h (the description of the clusterMsg count field); no code, data layout, or behaviour changes, so there is nothing to test.
- [valkey-io/valkey#382](https://github.com/valkey-io/valkey/pull/382) ([tests](tests/valkey/382-d0ee4188c/)): Don't let install flags affect build (#382): declined: Build-system change with no reachable behavior difference. The Makefile stops passing -DUSE_REDIS_SYMLINKS, and server.c makes the argv[0] dispatch to redis_check_rdb_main/redis_check_aof_main unconditional. Because the Makefile default is USE_REDIS_SYMLINKS?=yes, both the parent and the c
- [valkey-io/valkey#373](https://github.com/valkey-io/valkey/pull/373) ([tests](tests/valkey/373-9eaefc77b/)): Mention one of rediss:// and valkeys:// in error message, not both (#373): 1 of 1 test(s) reached the change, coverage 100%
- [valkey-io/valkey#294](https://github.com/valkey-io/valkey/pull/294) ([tests](tests/valkey/294-4948d536b/)): Detect accept4() on specific versions of various platforms (#294): declined: The change only edits preprocessor platform-detection conditions in src/config.h (HAVE_ACCEPT4 and HAVE_FADVISE) for FreeBSD, DragonFly BSD, NetBSD and OpenBSD. On Linux, where the fuzzer runs, defined(__linux__) satisfied both conditions before and after the change, so both builds compile
- [valkey-io/valkey#367](https://github.com/valkey-io/valkey/pull/367) ([tests](tests/valkey/367-74a0486e3/)): Update redis* to valkey* in server.c and module.c (#367): declined: The change only renames 'redis-server' to 'valkey-server' in source comments (dict.c, kvstore.c, ziplist.c), a stderr usage-example string for --test-memory in server.c, and a log-notice string in tls.c. No command behaviour, reply, replication, persistence, or any other runtime semantics 
- [valkey-io/valkey#365](https://github.com/valkey-io/valkey/pull/365) ([tests](tests/valkey/365-2864fffe7/)): Update redis* to valkey* in syscheck.c (#365): declined: The change only edits human-readable warning text in syscheck.c (renaming 'redis-server'/'Redis' to 'valkey-server'/'Valkey' in the xen clocksource and THP startup advisories). No logic, return codes, replies, persistence or replication behaviour changes, so there is nothing a scenario cou
- [valkey-io/valkey#32](https://github.com/valkey-io/valkey/pull/32) ([tests](tests/valkey/32-be81469cd/)): Update COPYING to Valkey (#32): declined: The change modifies only the COPYING file, adding the Valkey contributors' BSD license text above the existing Redis license. It touches no source code, configuration, build system, or tooling, so there is no runtime behaviour to exercise and no scenario is needed.
- [valkey-io/valkey#283](https://github.com/valkey-io/valkey/pull/283) ([tests](tests/valkey/283-191be272b/)): Rename redis.tcl to valkey.tcl (#283): declined: The change is confined to Valkey's own TCL test harness and docs: it renames tests/support/redis.tcl to tests/support/valkey.tcl, updates the source statements in test helper scripts (test_helper.tcl, instances.tcl, bg_block_op.tcl, bg_complex_data.tcl, gen_write_load.tcl, utils/speed-regr
- [valkey-io/valkey#357](https://github.com/valkey-io/valkey/pull/357) ([tests](tests/valkey/357-d09a59c3b/)): Rename redis_init_script file and its content (#357): declined: The change only renames utils/redis_init_script(.tpl) to valkey_init_script(.tpl) and updates variable names and user-facing strings inside those shell scripts and utils/install_server.sh. These are installation/init-script helpers, not part of valkey-server, the tools, or any runtime beha
- [valkey-io/valkey#349](https://github.com/valkey-io/valkey/pull/349) ([tests](tests/valkey/349-f0e1edc27/)): Updated modules examples and tests to use valkey vs redis (#349): 0 of 3 test(s) reached the change
- [valkey-io/valkey#257](https://github.com/valkey-io/valkey/pull/257) ([tests](tests/valkey/257-393c8fde2/)): Rename macros in config.h (#257): declined: The change only renames preprocessor macros in config.h (redis_fsync->valkey_fsync, redis_stat/fstat->valkey_stat/fstat, redis_unreachable->valkey_unreachable, REDIS_NO_SANITIZE->VALKEY_NO_SANITIZE, redis_set_thread_title->valkey_set_thread_title, REDIS_THREAD_STACK_SIZE->VALKEY_THREAD_STA
- [valkey-io/valkey#199](https://github.com/valkey-io/valkey/pull/199) ([tests](tests/valkey/199-87a5bfc00/)): Support connection schemes valkey:// and valkeys:// (#199): 1 of 1 test(s) reached the change, coverage 90%
- [valkey-io/valkey#342](https://github.com/valkey-io/valkey/pull/342) ([tests](tests/valkey/342-a989ee5c0/)): Updated modules examples to reference Valkey* (#342): declined: The change only renames RedisModule* identifiers to their ValkeyModule* aliases inside the example modules in src/modules/ (helloacl, helloblock, hellocluster, hellodict, hellohook, hellotimer, hellotype, helloworld). These examples are not built into or loaded by valkey-server, so no serv
- [valkey-io/valkey#341](https://github.com/valkey-io/valkey/pull/341) ([tests](tests/valkey/341-4693aa258/)): Rename redis in install_server.sh file (#341): declined: The change only edits utils/install_server.sh, an interactive root-only installer shell script that writes /etc/init.d services and system config files. It renames environment variables (REDIS_* to SERVER_*), default paths (/etc/redis to /etc/valkey), init-script names, and prompt text. No
- [valkey-io/valkey#333](https://github.com/valkey-io/valkey/pull/333) ([tests](tests/valkey/333-7809df0c9/)): Remove REDIS tag from test macros. (#333): declined: The change only renames the REDIS_TEST_* macros in testhelp.h to TEST_* and updates their uses in the internal unit-test functions (dictTest, listpackTest, quicklistTest, utilTest, ziplistTest) and the 'valkey-server test' flag parser. The macro values are identical and the code is test-ha

### valkey-io/valkey-bloom

[2026-11-13T17:06:02+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/43469244119): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-json

[2026-11-13T17:36:53+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/43474382178): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-search

[2026-11-13T17:23:15+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/43472151165): 0 merge(s) tested, 0 without a code change.


## Totals

- Merges whose tests reached the change: 886 of 1417 tested (63%)
- Changed-line coverage: median 100% of 913 merge(s) measured, 66 under 50%
- Recall: 230 parents flagged of 364 fix merges
- Claude: $431.92 in the last 24 hours, $7365.74 in the last 7 days, $10379.00 in all
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
