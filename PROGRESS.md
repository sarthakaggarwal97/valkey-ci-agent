# Fuzzer walk

Updated 2026-11-14T04:56+00:00.

| Repository | Done | Remaining | Backlog cursor | Last 24 hours | Last 7 days (per day) |
|---|---|---|---|---|---|
| valkey-io/valkey | 2243 | 297 | #35 (2024-03-26) | 272 | 247.3 |
| valkey-io/valkey-bloom | 109 | 0 | ce516d0ef6e3 (2024-04-26) | 0 | 0.0 |
| valkey-io/valkey-json | 66 | 0 | c1423d7e4b38 (2024-11-25) | 2 | 0.6 |
| valkey-io/valkey-search | 688 | 1 | f8106e72e984 (2024-11-27) | 15 | 48.1 |

## Last runs

### valkey-io/valkey

[2026-11-14T03:24:57+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/43543104975): 20 merge(s) tested, 6 without a code change.

- [valkey-io/valkey#5365](https://github.com/valkey-io/valkey/pull/5365) ([tests](tests/valkey/5365-c98c0e2e6/)): bgiteration: don't queue a FLUSHDB item for a completed iterator (#5365): 0 of 6 test(s) reached the change, coverage 100%
- [valkey-io/valkey#142](https://github.com/valkey-io/valkey/pull/142) ([tests](tests/valkey/142-621edbafb/)): Rename redisassert to serverassert in comment (#142): declined: The change edits only comment text in src/serverassert.c and src/serverassert.h (renaming 'redisassert' to 'serverassert' in file-header comments). No executable code, macro, build file, or configuration changes, so there is no behaviour to exercise and no scenario is needed.
- [valkey-io/valkey#138](https://github.com/valkey-io/valkey/pull/138) ([tests](tests/valkey/138-a1516d53d/)): Rename files redisassert to serverassert (#138): declined: The change only renames src/redisassert.c/.h to src/serverassert.c/.h (100% similarity renames), updates the matching #include lines in ae.c, dict.c, intset.c, kvstore.c, listpack.c, memtest.c, monotonic.c, quicklist.c, rax.c, sds.c, ziplist.c, zmalloc.c, and swaps redisassert.o for server
- [valkey-io/valkey#133](https://github.com/valkey-io/valkey/pull/133) ([tests](tests/valkey/133-98e7b41b8/)): Fix rename redis to valkey to pass reply-schemas-validator job (#133): declined: The change edits only utils/req-res-log-validator.py, a CI helper script for the reply-schemas-validator job: it renames the default redis-server/redis-cli binary paths to valkey-server/valkey-cli and updates print/comment strings. No server code, command behavior, reply, tool shipped in t
- [valkey-io/valkey#116](https://github.com/valkey-io/valkey/pull/116) ([tests](tests/valkey/116-ba8bda9cf/)): Fix shutdown syntax hint, following intention (#116): 2 of 2 test(s) reached the change
- [valkey-io/valkey#139](https://github.com/valkey-io/valkey/pull/139) ([tests](tests/valkey/139-83507d74b/)): Update for latest 'typos' (#139): declined: The change only edits .config/typos.toml (the typos linter's CI configuration, adding ignore words like 'ro', 'ang', 'arange') and fixes a spelling error ('potentional' -> 'potential') inside a comment in tests/unit/acl.tcl. No C source, command behavior, protocol, replication, persistence
- [valkey-io/valkey#131](https://github.com/valkey-io/valkey/pull/131) ([tests](tests/valkey/131-e35d86f2a/)): Fix rename REDIS_TEST to SERVER_TEST to pass the Daily workflow (#131): declined: The change only renames the REDIS_TEST compile-time macro to SERVER_TEST in #ifdef guards around embedded unit-test code (and one test-only struct field in quicklist.c). That macro is defined only in special test builds via the Makefile/CI; in the ordinary builds the fuzzer runs, all guard
- [valkey-io/valkey#130](https://github.com/valkey-io/valkey/pull/130) ([tests](tests/valkey/130-4d7fff9ab/)): Remove unused var desc in luaRegisterFunctionReadPositionalArgs (#130): declined: The change only deletes an unused local variable (`sds desc = NULL;`) and its dead cleanup (`if (desc) sdsfree(desc);`) in luaRegisterFunctionReadPositionalArgs; desc was never assigned, so no code path, reply, or memory behaviour changes. This is a pure refactor with no testable behaviour
- [valkey-io/valkey#103](https://github.com/valkey-io/valkey/pull/103) ([tests](tests/valkey/103-bdd7314f4/)): Fine-tune ASCII art (#103): declined: The change only adjusts spacing and glyphs in the startup ASCII art logo (src/asciilogo.h), a cosmetic string printed to the log at boot. It affects no command behavior, replies, replication, persistence, or any code path an oracle could judge, so no scenario is needed.
- [valkey-io/valkey#72](https://github.com/valkey-io/valkey/pull/72) ([tests](tests/valkey/72-253fe9dce/)): Fix typos and replace 'codespell' with 'typos' (#72): declined: The change is purely typo fixes: comments and identifier renames (local variables, a prototype parameter name, Tcl test proc names renamed consistently at definition and every call site), CI configuration replacing codespell with typos, and one printf string inside an #ifdef-guarded self-t
- [valkey-io/valkey#84](https://github.com/valkey-io/valkey/pull/84) ([tests](tests/valkey/84-af1b0de92/)): Fix typo (#84): declined: The change only fixes a typo in a code comment in src/acl.c ("resopnsible" -> "responsible"). No executable code, config, protocol, or tool behaviour changes, so there is nothing a scenario could reach or regress.
- [valkey-io/valkey#77](https://github.com/valkey-io/valkey/pull/77) ([tests](tests/valkey/77-b2a397366/)): Change ascii logo with temporal valkey logo (#77): declined: The change only swaps the ASCII-art banner string in src/asciilogo.h for a Valkey-branded one. Its sole consumer is the snprintf in server.c's logo printing, and the new string keeps exactly the same printf specifiers in the same order and types (%s, %s, %d, %s, %s, %d, %ld) matching the s
- [valkey-io/valkey#66](https://github.com/valkey-io/valkey/pull/66) ([tests](tests/valkey/66-3ab066f71/)): For additional compatibility this adds REDIS_CFLAGS and REDIS_LDFLAGS support to MAKEFILE (#66): declined: The change is build-system only: src/Makefile gets comment wording updates and new optional REDIS_CFLAGS/REDIS_LDFLAGS variables that merely alias SERVER_CFLAGS/SERVER_LDFLAGS at compile time. No runtime code path, command behavior, reply, or protocol is affected, so no fuzzer scenario can
- [valkey-io/valkey#86](https://github.com/valkey-io/valkey/pull/86) ([tests](tests/valkey/86-289eb47eb/)): Replace offensive term (#86): declined: The change edits only comment text: one line in the top-level Makefile comment and one comment in deps/linenoise/linenoise.c, replacing an offensive term. No executable code, build rules, configuration, or behaviour is touched, so there is nothing a scenario could exercise or compare.
- [valkey-io/valkey#70](https://github.com/valkey-io/valkey/pull/70) ([tests](tests/valkey/70-1950acd1e/)): Fix remaining names in CONTRIBUTING.md (#70): declined: The change only touches CONTRIBUTING.md (renaming Redis to Valkey, reformatting text) and adds a .cache entry to .gitignore. It is pure documentation/repo-metadata with no code, config, command, protocol, or tool behavior change, so no scenario can reach it and none is needed.
- [valkey-io/valkey#71](https://github.com/valkey-io/valkey/pull/71) ([tests](tests/valkey/71-1fa8da335/)): Rename functionsLibMataData to functionsLibMetaData (#71): declined: The change only renames the C typedef functionsLibMataData to functionsLibMetaData in src/functions.c and updates the type name at its use sites (functionExtractLibMetaData, functionFreeLibMetaData, and a local variable in functionsCreateWithLibraryCtx). No fields, logic, replies, persiste
- [valkey-io/valkey#62](https://github.com/valkey-io/valkey/pull/62) ([tests](tests/valkey/62-57789d4d0/)): Update naming to to Valkey (#62): declined: This is a pure build/naming change: the Makefile renames the built executables from placeholderkv-* to valkey-* and src/server.c updates the matching strstr(exec_name, ...) strings that dispatch into sentinel/check-rdb/check-aof mode, keeping dispatch consistent with each build's own binar
- [valkey-io/valkey#13](https://github.com/valkey-io/valkey/pull/13) ([tests](tests/valkey/13-c120a4587/)): Sharded pubsub command execution within multi/exec (#13): declined: no proposed test validated
- [valkey-io/valkey#55](https://github.com/valkey-io/valkey/pull/55) ([tests](tests/valkey/55-c7df0f06e/)): Makefile respect user's SERVER_CFLAGS and OPT (#55): declined: The change only touches src/Makefile: it moves the default -flto/-fno-omit-frame-pointer flags from SERVER_CFLAGS/SERVER_LDFLAGS into the OPTIMIZATION variable and adds $(OPT) to FINAL_LDFLAGS, so user-provided SERVER_CFLAGS and OPT are no longer clobbered by defaults. This is a build-syst
- [valkey-io/valkey#45](https://github.com/valkey-io/valkey/pull/45) ([tests](tests/valkey/45-11ddd8b28/)): Changing Redis references in files documenting development guidelines (#26) (#45): declined: The change edits only documentation text: the BUGS file and CONTRIBUTING.md, swapping Redis GitHub URLs for placeholderkv ones. No source file, command, config, replication, persistence or tool behaviour changes, so there is no runtime path for a scenario to reach.

### valkey-io/valkey-bloom

[2026-11-14T04:54:14+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/43558861541): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-json

[2026-11-14T04:56:49+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/43559050069): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-search

[2026-11-14T04:40:11+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/43553099941): 1 merge(s) tested, 0 without a code change.

- [valkey-io/valkey-search#1700](https://github.com/valkey-io/valkey-search/pull/1700): to retry, the job left no result

## Totals

- Merges whose tests reached the change: 901 of 1436 tested (63%)
- Changed-line coverage: median 100% of 927 merge(s) measured, 67 under 50%
- Recall: 233 parents flagged of 367 fix merges
- Claude: $619.15 in the last 24 hours, $6858.09 in the last 7 days, $10566.24 in all
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

- [valkey-io/valkey-search#1700](https://github.com/valkey-io/valkey-search/pull/1700): attempt 0 of 3, the job left no result
