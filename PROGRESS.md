# Fuzzer walk

Updated 2026-11-13T14:00+00:00.

| Repository | Done | Remaining | Backlog cursor | Last 24 hours | Last 7 days (per day) |
|---|---|---|---|---|---|
| valkey-io/valkey | 2068 | 468 | #455 (2024-05-07) | 97 | 244.3 |
| valkey-io/valkey-bloom | 109 | 0 | ce516d0ef6e3 (2024-04-26) | 0 | 0.0 |
| valkey-io/valkey-json | 64 | 0 | c1423d7e4b38 (2024-11-25) | 0 | 0.3 |
| valkey-io/valkey-search | 681 | 0 | f8106e72e984 (2024-11-27) | 8 | 60.9 |

## Last runs

### valkey-io/valkey

[2026-11-13T13:35:36+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/43423712111): 20 merge(s) tested, 2 without a code change.

- [valkey-io/valkey#460](https://github.com/valkey-io/valkey/pull/460) ([tests](tests/valkey/460-546cef668/)): Initial cleanup for cluster refactoring (#460): 0 of 7 test(s) reached the change, coverage 50%; crash, fixed by #460 (recorded)
- [valkey-io/valkey#493](https://github.com/valkey-io/valkey/pull/493) ([tests](tests/valkey/493-741ee702c/)): [New] Migrate zmalloc.c unit tests to new test framework. (#493): declined: The change is a pure test-infrastructure migration: it moves zmalloc's unit tests from the old SERVER_TEST in-binary framework (code compiled only under #ifdef SERVER_TEST, absent from production builds) into the new standalone unit test framework (new src/unit/test_zmalloc.c plus test_fil
- [valkey-io/valkey#494](https://github.com/valkey-io/valkey/pull/494) ([tests](tests/valkey/494-72f2a8743/)): Minor fix in module API doc script (#494): declined: The change edits only utils/generate-module-api-doc.rb, a documentation-generation helper script (a regex for linking URLs and a removed front-matter line). It is not compiled into valkey-server or any tool, changes no command, reply, replication, persistence or cluster behaviour, and cann
- [valkey-io/valkey#492](https://github.com/valkey-io/valkey/pull/492) ([tests](tests/valkey/492-4e18e326a/)): Remove endian coverage from server.c (#492): declined: The diff only removes the endianconvTest entry from the serverTest table in server.c, the registry for the built-in unit-test harness (`valkey-server test <name>`). No runtime code path changes: endian conversion functions are untouched, and no command, reply, replication, cluster, or pers
- [valkey-io/valkey#458](https://github.com/valkey-io/valkey/pull/458) ([tests](tests/valkey/458-c7ad9feb5/)): Migrate endianconv.c unit tests to new test framework (#458): declined: The change only migrates endianconv unit tests to the new test framework: it removes the SERVER_TEST-gated endianconvTest() from endianconv.c/h and adds an equivalent src/unit/test_endianconv.c registered in test_files.h. No production code path changes — memrev16/32/64 and intrev* are unt
- [valkey-io/valkey#450](https://github.com/valkey-io/valkey/pull/450) ([tests](tests/valkey/450-dca172234/)): Use kqueue as the backend of AE on DragonFlyBSD (#450): declined: The change is a platform-detection tweak in src/config.h: it adds __DragonFly__ to the preprocessor check that defines HAVE_KQUEUE, so DragonFlyBSD builds use the kqueue AE backend instead of select. On Linux, where the fuzzer runs, HAVE_EPOLL still selects epoll and the compiled code is b
- [valkey-io/valkey#454](https://github.com/valkey-io/valkey/pull/454) ([tests](tests/valkey/454-07367df98/)): Update rdb and module's child proc name to valkey accordingly (compatible with redis symlink) (#454): 0 of 3 test(s) reached the change, coverage 66%
- [valkey-io/valkey#470](https://github.com/valkey-io/valkey/pull/470) ([tests](tests/valkey/470-e24279986/)): Migrate sha1 unit test to new framework (#470): declined: This change only migrates the sha1 unit test from the legacy SERVER_TEST framework to the new unit test framework: it deletes test-only code (sha1Test, compiled only under #ifdef SERVER_TEST) and adds src/unit/test_sha1.c with the same test logic plus an expected-digest assertion. The prod
- [valkey-io/valkey#466](https://github.com/valkey-io/valkey/pull/466) ([tests](tests/valkey/466-138a7d984/)):  Handle role change error in `cluster setslot` when migrating the last slot away with allow-replica-migration enabled (#466): declined: The only behavioural change is in valkey-cli's clusterManagerMoveSlot (src/valkey-cli.c): after moving a primary's last slot away with cluster-allow-replica-migration enabled, it now also tolerates a '-UNBLOCKED' role-change error (in addition to the existing 'Please use SETSLOT only with 
- [valkey-io/valkey#462](https://github.com/valkey-io/valkey/pull/462) ([tests](tests/valkey/462-8048abb2f/)): Support pipe2() on *BSD (#462): declined: This is a portability refactor: it introduces a HAVE_PIPE2 macro so pipe2() is also used on OpenBSD/NetBSD/DragonFly. On Linux, where the fuzzer runs, HAVE_PIPE2 is defined exactly where the old __linux__ guard applied, so anetPipe() compiles to the identical code path on both the change a
- [valkey-io/valkey#478](https://github.com/valkey-io/valkey/pull/478) ([tests](tests/valkey/478-0342a81b7/)): Migrate sds.c unit tests to new test framework. (#478): declined: The change only migrates sds.c's embedded SERVER_TEST unit tests into the new standalone unit test framework (new src/unit/test_sds.c, registration in test_files.h, removal of the old sdsTest entry from server.c's test table). No production code or runtime behaviour changes: the sds functi
- [valkey-io/valkey#465](https://github.com/valkey-io/valkey/pull/465) ([tests](tests/valkey/465-4e1d8e172/)): [Fix] move deps from slowlog.c into slowlog.h (#465): declined: The change is a pure build/include refactor: it moves `#include "server.h"` from slowlog.c into slowlog.h so the header is self-contained. No code, data structure, command reply, config, or runtime path changes; the compiled output is identical in behaviour. Compilation itself (done by the
- [valkey-io/valkey#481](https://github.com/valkey-io/valkey/pull/481) ([tests](tests/valkey/481-2559f64f5/)): [FIX] Remove redundant statement and return (#481): declined: The diff only deletes a redundant forward declaration of freeClientArgv and a trailing bare 'return;' at the end of the void function aofDelTempIncrAofFile in src/aof.c. Both are no-ops: the declaration affects compilation only, and the final return in a void function changes no control fl
- [valkey-io/valkey#475](https://github.com/valkey-io/valkey/pull/475) ([tests](tests/valkey/475-b166980c8/)): Fix UNUSED repetition issue in test sources (#475): declined: The change only adds an #ifndef guard around the UNUSED macro in src/unit/test_help.h and removes two #undef UNUSED lines in unit test files (test_kvstore.c, test_util.c). It affects compilation of the unit test harness only, not the valkey-server binary, any command, replication, persiste
- [valkey-io/valkey#471](https://github.com/valkey-io/valkey/pull/471) ([tests](tests/valkey/471-6cff0d6a7/)): Remove intsettest declaration from intset.h (#471): declined: The change only deletes an unused `#ifdef SERVER_TEST` declaration of intsetTest from intset.h. It is a dead-code/header cleanup with no change to any runtime behaviour, command reply, encoding, replication or persistence path, so no scenario can distinguish the builds.
- [valkey-io/valkey#415](https://github.com/valkey-io/valkey/pull/415) ([tests](tests/valkey/415-1125bdbb8/)): Update serverpanic output based on 'extended-redis-compatibility' config. (#415): declined: The change only edits comments and the text of two serverPanic messages (choosing 'Redis' vs 'Valkey' by the extended-redis-compatibility config). The panic paths fire only on an internal invariant violation in the built-in command table (key-spec positions inconsistent with arity), which 
- [valkey-io/valkey#468](https://github.com/valkey-io/valkey/pull/468) ([tests](tests/valkey/468-6af51f509/)): Prevent clang-format in certain places (#468): 2 of 11 test(s) reached the change, coverage 71%
- [valkey-io/valkey#473](https://github.com/valkey-io/valkey/pull/473) ([tests](tests/valkey/473-2278dfd25/)): Fix build error for unit test (#473): declined: The change only edits src/Makefile to add missing static libraries (libhiredis.a, liblua.a, libhdrhistogram.a) to the link command for the valkey-unit-tests binary, fixing a build/link error for the unit test target. It changes no server code, no command behavior, no replication, persisten
- [valkey-io/valkey#456](https://github.com/valkey-io/valkey/pull/456) ([tests](tests/valkey/456-315b7573c/)): Update server function's name to valkey (#456): declined: The change only renames internal C functions (genRedisInfoString and its CommandStats/ACLStats/LatencyStats helpers to genValkeyInfo*) and updates every call site in debug.c, module.c, sentinel.c, server.c and the declarations in server.h, plus two comments. No function body, format string
- [valkey-io/valkey#446](https://github.com/valkey-io/valkey/pull/446) ([tests](tests/valkey/446-4e944cede/)): Migrate kvstore.c unit tests to new test framework. (#446): declined: The change is a pure test-infrastructure migration: it moves kvstore.c's unit tests (previously compiled only under #ifdef SERVER_TEST) into the new standalone unit test framework (src/unit/test_kvstore.c), removes the kvstoreTest declaration and its entry in server.c's developer-only test

### valkey-io/valkey-bloom

[2026-11-13T14:00:40+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/43438587307): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-json

[2026-11-13T14:00:44+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/43438567863): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-search

[2026-11-13T13:46:26+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/43436454377): 0 merge(s) tested, 0 without a code change.


## Totals

- Merges whose tests reached the change: 879 of 1405 tested (63%)
- Changed-line coverage: median 100% of 903 merge(s) measured, 66 under 50%
- Recall: 230 parents flagged of 364 fix merges
- Claude: $361.17 in the last 24 hours, $7488.94 in the last 7 days, $10308.25 in all
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
