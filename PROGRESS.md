# Fuzzer walk

Updated 2026-11-07T21:01+00:00.

| Repository | Done | Remaining | Backlog cursor | Last 24 hours | Last 7 days (per day) |
|---|---|---|---|---|---|
| valkey-io/valkey | 757 | 1750 | #3135 (2026-02-10) | 351 | 108.1 |
| valkey-io/valkey-bloom | 109 | 0 | ce516d0ef6e3 (2024-04-26) | 0 | 15.6 |
| valkey-io/valkey-json | 62 | 0 | c1423d7e4b38 (2024-11-25) | 0 | 8.9 |
| valkey-io/valkey-search | 481 | 188 | #362 (2025-09-09) | 172 | 68.7 |

## Last runs

### valkey-io/valkey

[2026-11-07T19:41:43+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42398172464): 20 merge(s) tested, 8 without a code change.

- [valkey-io/valkey#3197](https://github.com/valkey-io/valkey/pull/3197) ([tests](tests/valkey/3197-1f473323c/)): Fix getKeysUsingKeySpecs keynum case ignoring keystep when calc the last key (#3197): declined: The fix changes getKeysUsingKeySpecs's last-key formula from first+numkeys-1 to first+(numkeys-1)*step, which differs only when a KEYNUM key spec has keystep > 1. Every built-in command with a KEYNUM spec (EVAL/EVALSHA/FCALL families, ZUNION/ZINTER/ZDIFF/ZINTERCARD, SINTERCARD, LMPOP/BLMPO
- [valkey-io/valkey#2846](https://github.com/valkey-io/valkey/pull/2846) ([tests](tests/valkey/2846-130befc81/)): Dual-channel-replication announces itself at replica-announce-ip if configured (#2846): 0 of 2 test(s) reached the change, coverage 0%
- [valkey-io/valkey#3216](https://github.com/valkey-io/valkey/pull/3216) ([tests](tests/valkey/3216-24b67843a/)): Update deps/libvalkey to version 0.4.0 (#3216): 0 of 5 test(s) reached the change
- [valkey-io/valkey#3230](https://github.com/valkey-io/valkey/pull/3230) ([tests](tests/valkey/3230-13dd4b8b9/)): Skip large-memory unit test with ASan that got OOM (#3230): declined: The change edits only the unit-test harness file src/unit/test_quicklist.c, adding an early return under VALKEY_ADDRESS_SANITIZER so the large-memory quicklist compress/decompress unit test is skipped in ASan CI runs to avoid OOM. No server, command, data-structure, replication, or persist
- [valkey-io/valkey#3217](https://github.com/valkey-io/valkey/pull/3217) ([tests](tests/valkey/3217-d0a32a70f/)): Add `ALLOW_BUSY` flag to `SELECT` and various `CLIENT` sub-commands (#3217): 3 of 3 test(s) reached the change
- [valkey-io/valkey#3160](https://github.com/valkey-io/valkey/pull/3160) ([tests](tests/valkey/3160-385895a96/)): [BUG][issue-2797] Block module unload when ACL references module subcommands (#3160): 5 of 5 test(s) reached the change, coverage 86%; crash, fixed by #3160 (recorded)
- [valkey-io/valkey#3223](https://github.com/valkey-io/valkey/pull/3223) ([tests](tests/valkey/3223-b00c9367f/)): Always untrack initialized safe iterator (#3223): 0 of 4 test(s) reached the change, coverage 100%
- [valkey-io/valkey#3178](https://github.com/valkey-io/valkey/pull/3178) ([tests](tests/valkey/3178-051a5ebaa/)): Fixes memory leak when the job crashes before it's freed (#3178): 0 of 11 test(s) reached the change, coverage 93%
- [valkey-io/valkey#3215](https://github.com/valkey-io/valkey/pull/3215) ([tests](tests/valkey/3215-80bd689ac/)): Fix database ACL check for alldbs commands (#3215): 3 of 3 test(s) reached the change, coverage 100%
- [valkey-io/valkey#1840](https://github.com/valkey-io/valkey/pull/1840) ([tests](tests/valkey/1840-223bfa840/)): Added comment to maxmemory-clients conf to mention the performance issue (#1840): declined: The change only edits documentation comments in valkey.conf (rewording the maxmemory-clients description and adding a commented-out example line). No code path, config default, or parsing behaviour changes, so there is nothing a runtime scenario could exercise.
- [valkey-io/valkey#3201](https://github.com/valkey-io/valkey/pull/3201) ([tests](tests/valkey/3201-fde31b08f/)): Optimize endian conversion functions (#3201): 0 of 5 test(s) reached the change
- [valkey-io/valkey#3193](https://github.com/valkey-io/valkey/pull/3193) ([tests](tests/valkey/3193-e0a467eda/)): Optimize ustime() with monotonic delta (#3193): 0 of 3 test(s) reached the change, coverage 100%
- [valkey-io/valkey#3205](https://github.com/valkey-io/valkey/pull/3205) ([tests](tests/valkey/3205-62686985e/)): Fix bug causing no response flush sometimes when IO threads are busy (#3205): 0 of 3 test(s) reached the change, coverage 100%
- [valkey-io/valkey#3034](https://github.com/valkey-io/valkey/pull/3034) ([tests](tests/valkey/3034-fd57c2171/)): add option to use libbacktrace for backtraces in crash reports (#3034): declined: The change is entirely behind a new compile-time flag USE_LIBBACKTRACE (default no) that the runner's standard build does not set, so the new symbolizeWithLibbacktrace code in src/debug.c is compiled out of both binaries under test. Everything else in the diff is CI workflow, .gitignore an
- [valkey-io/valkey#3186](https://github.com/valkey-io/valkey/pull/3186) ([tests](tests/valkey/3186-42fc851b7/)): Fix pointer-to-int-cast warnings for RDMA address handling (#3186): declined: The change only inserts intermediate (uintptr_t) casts when converting between pointers and uint64_t in src/rdma.c and tests/rdma/rdma-test.c to fix compiler pointer-to-int-cast warnings. On the supported 64-bit platforms the resulting values are bit-identical to the parent build, so there
- [valkey-io/valkey#3185](https://github.com/valkey-io/valkey/pull/3185) ([tests](tests/valkey/3185-9cbe1045d/)): Update and pin github actions to full SHAs for supply chain security (#3185): declined: The change pins GitHub Actions workflows to full SHAs (CI-only files under .github/workflows/) and fixes typos: comments, unit-test function/message spellings, TCL test comments, and semantically identical local variable renames (cpy->copy in sds.c/networking.c, aof_preable->aof_preamble i
- [valkey-io/valkey#3182](https://github.com/valkey-io/valkey/pull/3182) ([tests](tests/valkey/3182-a95a75ab4/)): Fix server assert on ACL LOAD and resetchannels (#3182): 6 of 7 test(s) reached the change, coverage 91%; crash, fixed by #3182 (recorded)
- [valkey-io/valkey#3190](https://github.com/valkey-io/valkey/pull/3190) ([tests](tests/valkey/3190-c2aca7255/)): Build Lua by default with make test (#3190): declined: The change only edits src/Makefile: it factors the 'all' target's prerequisites into a variable and makes 'make test' build the same full prerequisite set (including the Lua/TLS/RDMA modules) before running the test suite. It alters what gets compiled before tests, not any runtime behaviou
- [valkey-io/valkey#3184](https://github.com/valkey-io/valkey/pull/3184) ([tests](tests/valkey/3184-06d30f293/)): Fix PFADD corrupted sparse HLL handling by restoring hllSparseSet return type (#3184): 2 of 2 test(s) reached the change, coverage 100%
- [valkey-io/valkey#3180](https://github.com/valkey-io/valkey/pull/3180) ([tests](tests/valkey/3180-06ccdf990/)): Fix remaining_repl_size should be in CLUSTER GETSLOTMIGRATIONS in conf (#3180): declined: The change edits only a comment in valkey.conf, correcting the documented command name ('CLUSTER MIGRATESLOTS' -> 'CLUSTER GETSLOTMIGRATIONS') for where the remaining_repl_size field appears. No code, config parsing, command behaviour, replication, or persistence path is touched — the comm

### valkey-io/valkey-bloom

[2026-11-07T20:32:56+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42410753615): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-json

[2026-11-07T21:01:46+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42413812876): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-search

[2026-11-07T19:24:49+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42394506825): 20 merge(s) tested, 3 without a code change.

- [valkey-io/valkey-search#494](https://github.com/valkey-io/valkey-search/pull/494) ([tests](tests/valkey-search/494-cdfe0c4b7/)): Fixed regression which broke Ninja builds OO (#494): declined: The change modifies only build.sh in valkey-search: it moves the Ninja/make build-tool selection after the function definitions and fixes ninja vs ninja-build detection so Ninja builds work again. It is purely a build-script/CI fix with no change to any module source, command behavior, rep
- [valkey-io/valkey-search#479](https://github.com/valkey-io/valkey-search/pull/479) ([tests](tests/valkey-search/479-e9f277cfd/)): Add support for checking ACL key prefix permissions using the new Valkey module API. (#479): 4 of 4 test(s) reached the change
- [valkey-io/valkey-search#405](https://github.com/valkey-io/valkey-search/pull/405) ([tests](tests/valkey-search/405-f683fffb3/)): Add more integration tests for FT.DROPINDEX consistency check (#405): 3 of 3 test(s) reached the change; crash, predates #405, since fixed (recorded)
- [valkey-io/valkey-search@b2aca8c691f3](https://github.com/valkey-io/valkey-search/commit/b2aca8c691f3a9d38098ab720268549de9d14ce0) ([tests](tests/valkey-search/b2aca8c69/)): Fix typo in ft.aggregate commit: declined: The change only fixes a typo in an error string inside the filter_str.empty() branch of the file-local ParseKNN (ft_search_parser.cc:185). That branch is unreachable: ParseKNN's sole caller, PreParseQueryString at ft_search_parser.cc:393, is guarded by !vector_filter.empty() on the very st
- [valkey-io/valkey-search#345](https://github.com/valkey-io/valkey-search/pull/345) ([tests](tests/valkey-search/345-b3b3449fb/)): Implementation of FT.AGGREGATE Command (#345): 4 of 4 test(s) reached the change
- [valkey-io/valkey-search#408](https://github.com/valkey-io/valkey-search/pull/408) ([tests](tests/valkey-search/408-0cd447cfc/)): Implement server version checking on load (#408): 0 of 3 test(s) reached the change
- [valkey-io/valkey-search#393](https://github.com/valkey-io/valkey-search/pull/393) ([tests](tests/valkey-search/393-73138472e/)): FT.DROPINDEX consistency check (#393): declined: The change is real behaviour (FT.DROPINDEX in cluster mode now fans out a consistency check over the coordinator's gRPC channel, retrying until every node reports the index gone; gRPC NOT_FOUND is reclassified as INDEX_NAME_ERROR; FT.INFO CLUSTER retries on index-name errors), but every ch
- [valkey-io/valkey-search#404](https://github.com/valkey-io/valkey-search/pull/404) ([tests](tests/valkey-search/404-5ee8c81d3/)): add .kiro folder to .gitignore; enable developers to use kiro while prevent uploading kiro steering files (#404): declined: The change adds a single line (`.kiro/`) to .gitignore so developers' Kiro steering files aren't committed. It touches no source code, build configuration, or runtime behavior of the valkey-search module; there is no command, reply, replication, or persistence path to exercise.
- [valkey-io/valkey-search#386](https://github.com/valkey-io/valkey-search/pull/386) ([tests](tests/valkey-search/386-2812344e7/)): FT.CREATE consistency check (#386): declined: The only behavioural change sits behind the coordinator, which this fuzzer cannot enable. FT.CREATE's new consistency-check fanout runs only when `ValkeySearch::Instance().IsCluster() && UsingCoordinator()` (src/commands/ft_create.cc:63-70); otherwise the command takes the unchanged else-b
- [valkey-io/valkey-search#384](https://github.com/valkey-io/valkey-search/pull/384) ([tests](tests/valkey-search/384-95c6fde85/)): Add --format option to build script (#384): declined: The change only modifies build.sh, adding a --format option that runs clang-format on the source tree as a developer convenience. It is a build/tooling change with no effect on the compiled module's runtime behavior, commands, replies, persistence, or replication, so there is nothing for a
- [valkey-io/valkey-search#337](https://github.com/valkey-io/valkey-search/pull/337) ([tests](tests/valkey-search/337-4be4f5680/)): Adding retry to fanout base class (#337): 3 of 3 test(s) reached the change
- [valkey-io/valkey-search#390](https://github.com/valkey-io/valkey-search/pull/390) ([tests](tests/valkey-search/390-0e185efac/)): Captured errors should be formatted as such. (#390): declined: The change is confined to the unit-test infrastructure: vmsdk/src/testing_infra/module.h's ReplyCapture mock (used only by gtest) now formats captured errors as RESP errors ('-msg') instead of bulk strings, and testing/ft_create_test.cc, ft_info_test.cc and ft_search_test.cc update their e
- [valkey-io/valkey-search#395](https://github.com/valkey-io/valkey-search/pull/395) ([tests](tests/valkey-search/395-d3c87b5ce/)): tag casesentive updated to be key-value pair (#395): 2 of 2 test(s) reached the change
- [valkey-io/valkey-search#370](https://github.com/valkey-io/valkey-search/pull/370) ([tests](tests/valkey-search/370-af2e9a8e0/)): added and removed stats and attributes from ft.info (#370): 2 of 2 test(s) reached the change
- [valkey-io/valkey-search#388](https://github.com/valkey-io/valkey-search/pull/388) ([tests](tests/valkey-search/388-7cbef1e01/)): fix memory leak at config get (#388): declined: The change rewrites OnGetStringConfig, the CONFIG GET callback for string-typed module configs in vmsdk (caching the ValkeyModuleString instead of leaking a fresh one per call). That callback is only installed by config::String::Register, and valkey-search at this commit never instantiates
- [valkey-io/valkey-search#387](https://github.com/valkey-io/valkey-search/pull/387) ([tests](tests/valkey-search/387-7182af06d/)): Remove command alloed acl checks is it is already done by Valkey (#387): 2 of 2 test(s) reached the change
- [valkey-io/valkey-search#363](https://github.com/valkey-io/valkey-search/pull/363) ([tests](tests/valkey-search/363-329da6072/)): Fix valkey and json module versions in build and integration test. (#363): declined: The change edits only scripts/common.rc, a build/CI helper script that clones and builds valkey-server and the valkey-json module for valkey-search's own integration tests: it makes VALKEY_VERSION and VALKEY_JSON_VERSION overridable env vars and clones valkey-json by branch instead of a pi
- [valkey-io/valkey-search#382](https://github.com/valkey-io/valkey-search/pull/382) ([tests](tests/valkey-search/382-e8b166bfd/)): Early return mutation handling on record not found and not tracked (#382): 2 of 2 test(s) reached the change; crash, predates #382, since fixed (recorded)
- [valkey-io/valkey-search#381](https://github.com/valkey-io/valkey-search/pull/381) ([tests](tests/valkey-search/381-688cb371d/)): Fixed: clients are blocked after MULTI operation (#381): 2 of 2 test(s) reached the change
- [valkey-io/valkey-search#379](https://github.com/valkey-io/valkey-search/pull/379) ([tests](tests/valkey-search/379-90e550e13/)): - Remove unused variable from test (#379): declined: The change only removes unused variables and magic numbers from a unit test file (thread_pool_test.cc) and adds `inline` to two explicit template specializations in debug.h to fix ODR/linkage for a header-defined function. The function bodies are unchanged, so no command, reply, indexing, 

## Totals

- Merges whose tests reached the change: 478 of 702 tested (68%)
- Changed-line coverage: median 100% of 338 merge(s) measured, 22 under 50%
- Recall: 140 parents flagged of 209 fix merges
- Claude: $1857.01 in the last 24 hours, $5031.14 in the last 7 days, $5031.14 in all
- Issues filed: 50
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

- [valkey-io/valkey#3800](https://github.com/valkey-io/valkey/pull/3800): attempt 0 of 3, Claude Code failed (rc=1): API Error: Claude Code is unable to respond to this request, which appears to violate our Usage Policy (https://www.anthropic.com/legal/aup). This request triggered restrict
- [valkey-io/valkey#3613](https://github.com/valkey-io/valkey/pull/3613): attempt 0 of 3, Claude Code failed (rc=1): API Error: Claude Code is unable to respond to this request, which appears to violate our Usage Policy (https://www.anthropic.com/legal/aup). This request triggered restrict
- [valkey-io/valkey#3498](https://github.com/valkey-io/valkey/pull/3498): attempt 0 of 3, Claude Code failed (rc=1): API Error: Claude Code is unable to respond to this request, which appears to violate our Usage Policy (https://www.anthropic.com/legal/aup). This request triggered restrict
