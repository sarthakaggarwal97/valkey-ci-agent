# Fuzzer walk

Updated 2026-11-13T12:59+00:00.

| Repository | Done | Remaining | Backlog cursor | Last 24 hours | Last 7 days (per day) |
|---|---|---|---|---|---|
| valkey-io/valkey | 2046 | 490 | #460 (2024-05-14) | 75 | 244.0 |
| valkey-io/valkey-bloom | 109 | 0 | ce516d0ef6e3 (2024-04-26) | 0 | 0.0 |
| valkey-io/valkey-json | 64 | 0 | c1423d7e4b38 (2024-11-25) | 0 | 0.3 |
| valkey-io/valkey-search | 681 | 0 | f8106e72e984 (2024-11-27) | 8 | 60.9 |

## Last runs

### valkey-io/valkey

[2026-11-13T12:15:55+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/43403215072): 20 merge(s) tested, 7 without a code change.

- [valkey-io/valkey#502](https://github.com/valkey-io/valkey/pull/502) ([tests](tests/valkey/502-417660449/)): Adjust sds types (#502): 3 of 8 test(s) reached the change, coverage 74%
- [valkey-io/valkey#548](https://github.com/valkey-io/valkey/pull/548) ([tests](tests/valkey/548-7ba7e4d05/)): Update zfree on data in test_crc64combine before return. (#548): declined: The change only adds zfree(data) cleanup on early-return error paths inside the unit test file src/unit/test_crc64combine.c, fixing a memory leak in the test harness itself. It does not touch any server, command, replication, persistence or tool code, so no runtime behaviour of valkey-serv
- [valkey-io/valkey#538](https://github.com/valkey-io/valkey/pull/538) ([tests](tests/valkey/538-84157890f/)): Set up clang-format github action (#538): declined: The change is a CI/build change with mechanical formatting fixes only: it adds a clang-format GitHub Action workflow and reformats source files to pass it (whitespace/line-wrapping around unchanged expressions, renaming C++-keyword identifiers like new->new_user, delete->del_ele, not->not_
- [valkey-io/valkey#554](https://github.com/valkey-io/valkey/pull/554) ([tests](tests/valkey/554-045d475a9/)): Implement REPLCONF VERSION (#554): 2 of 6 test(s) reached the change, coverage 96%
- [valkey-io/valkey#556](https://github.com/valkey-io/valkey/pull/556) ([tests](tests/valkey/556-e4ead9442/)): Make CLUSTER SETSLOT with TIMEOUT 0 block indefinitely (#556): 3 of 3 test(s) reached the change, coverage 92%
- [valkey-io/valkey#490](https://github.com/valkey-io/valkey/pull/490) ([tests](tests/valkey/490-5d0f4bc9f/)): Require C11 atomics (#490): 2 of 21 test(s) reached the change, coverage 84%, 1 skipped (the run could not start: runner execution failed during deployment.provision: LocalDeploymentError: Instance valkey-0 failed to start within 60s (proc […] 5) *** | Reading the configuration file, at line 18 | >>> 'dual-channel-replication-enabled "yes"' | Bad directive or wrong number of arguments)); [crash, predates #490](https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/153)
- [valkey-io/valkey#552](https://github.com/valkey-io/valkey/pull/552) ([tests](tests/valkey/552-fbbabe354/)): Revert format updates on config.c file for config block (#552): declined: The change only reverts clang-format whitespace in src/config.c: each create*Config entry is collapsed back onto one line and a clang-format off/on guard is added around the static_configs table. Every config's name, alias, flags, bounds, default value, validator and apply callback is unch
- [valkey-io/valkey#495](https://github.com/valkey-io/valkey/pull/495) ([tests](tests/valkey/495-d72ba06dd/)): Make cluster replicas return ASK and TRYAGAIN (#495): 2 of 3 test(s) reached the change, coverage 85%
- [valkey-io/valkey#536](https://github.com/valkey-io/valkey/pull/536) ([tests](tests/valkey/536-3dd2f5a58/)): Undeprecate cluster slots command (#536): 2 of 2 test(s) reached the change
- [valkey-io/valkey#537](https://github.com/valkey-io/valkey/pull/537) ([tests](tests/valkey/537-a0aebb6b6/)): Reinitialize pointer 'p' after ziplistDeleteRange to fix head deletion bug (#537): declined: The change touches only src/unit/test_ziplist.c: it fixes a stale pointer in the unit test test_ziplistDeleteInclusiveRange0To0 by reinitializing 'p' via ziplistIndex after ziplistDeleteRange. No server source changes — ziplistDeleteRange itself is unmodified — so no command reply, data st
- [valkey-io/valkey#323](https://github.com/valkey-io/valkey/pull/323) ([tests](tests/valkey/323-c41dd77a3/)): Add clang-format configs (#323): declined: The change only adds clang-format configuration files (src/.clang-format, .clang-format-ignore additions) and applies the mechanical clang-format reformatting across the C sources: comment reflowing, indentation and brace-style changes. clang-format alters whitespace and line breaks only, 
- [valkey-io/valkey#53](https://github.com/valkey-io/valkey/pull/53) ([tests](tests/valkey/53-c4782066e/)): Cache CLUSTER SLOTS response for improving throughput and reduced latency. (#53): 0 of 2 test(s) reached the change, coverage 87%
- [valkey-io/valkey#486](https://github.com/valkey-io/valkey/pull/486) ([tests](tests/valkey/486-72538622f/)): Migrate ziplist.c unit tests to new framework (#486): declined: The change is a pure test migration: it moves ziplist.c's SERVER_TEST-gated unit tests into the new unit test framework (src/unit/test_ziplist.c, test_files.h) and deletes the old #ifdef SERVER_TEST block from ziplist.c/ziplist.h plus its entry in server.c's serverTests[]. No production co
- [valkey-io/valkey#518](https://github.com/valkey-io/valkey/pull/518) ([tests](tests/valkey/518-acb74f8da/)): Delete dead code "zfree_usable"  (#518): declined: The change deletes the unused function zfree_usable (and its s_free_usable macro alias) from zmalloc.c/zmalloc.h/sdsalloc.h. A grep of the checkout confirms no remaining callers of zfree_usable or s_free_usable anywhere in the tree, so the function was dead code: removing it changes no com
- [valkey-io/valkey#521](https://github.com/valkey-io/valkey/pull/521) ([tests](tests/valkey/521-122cba510/)): Quick fix of failure when use libc allocator in daily CI. (#521): 2 of 2 test(s) reached the change, coverage 100%; crash, fixed by #521 (recorded)
- [valkey-io/valkey#516](https://github.com/valkey-io/valkey/pull/516) ([tests](tests/valkey/516-d52c8f30e/)): Include stddef in zmalloc.h (#516): declined: The change only adds #include <stddef.h> to zmalloc.h so the header compiles standalone (size_t is otherwise undeclared when zmalloc.h is included first). It is a build-time header fix with no runtime behaviour change: no command, reply, memory-allocation semantics, replication or persiste
- [valkey-io/valkey#514](https://github.com/valkey-io/valkey/pull/514) ([tests](tests/valkey/514-dcc9fd4fe/)): Resolve numtests counter error (#514): declined: The change only edits src/unit/test_main.c, the main() of Valkey's internal unit-test runner, fixing the array-size computation for the test-suite loop counter. It is test-infrastructure code compiled into the valkey-unit-tests binary and does not alter valkey-server behaviour, any command
- [valkey-io/valkey#501](https://github.com/valkey-io/valkey/pull/501) ([tests](tests/valkey/501-efa8ba519/)): Finish postponed SCAN changes (#501): 2 of 2 test(s) reached the change, coverage 100%
- [valkey-io/valkey#476](https://github.com/valkey-io/valkey/pull/476) ([tests](tests/valkey/476-7a9951fb8/)): Correct the actual allocated size from allocator when call sdsRedize to align the logic with sdsnewlen function. (#476): 0 of 3 test(s) reached the change, coverage 100%
- [valkey-io/valkey#499](https://github.com/valkey-io/valkey/pull/499) ([tests](tests/valkey/499-6e4a61093/)): Make it to so that unit tests build on mac (#499): declined: The change edits only src/unit/test_util.c, restructuring an #if defined(__linux__) guard so the unit test binary builds on macOS (test_reclaimFilePageCache becomes a no-op off Linux, and a printf is removed). It is a build fix to test-only code; no Valkey server behaviour, command reply, 

### valkey-io/valkey-bloom

[2026-11-13T12:28:23+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/43425335233): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-json

[2026-11-13T12:59:01+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/43429631773): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-search

[2026-11-13T12:44:35+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/43427630046): 0 merge(s) tested, 0 without a code change.


## Totals

- Merges whose tests reached the change: 878 of 1402 tested (63%)
- Changed-line coverage: median 100% of 900 merge(s) measured, 66 under 50%
- Recall: 230 parents flagged of 364 fix merges
- Claude: $316.71 in the last 24 hours, $7485.33 in the last 7 days, $10263.80 in all
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
