# Fuzzer walk

Updated 2026-11-06T17:07+00:00.

| Repository | Done | Remaining | Backlog cursor | Last 24 hours | Last 7 days (per day) |
|---|---|---|---|---|---|
| valkey-io/valkey | 381 | 2117 | #3465 (2026-08-05) | 224 | 54.4 |
| valkey-io/valkey-bloom | 109 | 0 | ce516d0ef6e3 (2024-04-26) | 1 | 15.6 |
| valkey-io/valkey-json | 62 | 0 | c1423d7e4b38 (2024-11-25) | 0 | 8.9 |
| valkey-io/valkey-search | 281 | 387 | #851 (2026-03-04) | 204 | 40.1 |

## Last runs

### valkey-io/valkey

[2026-11-06T16:26:03+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42227216893): 20 merge(s) tested, 3 without a code change.

- [valkey-io/valkey#3971](https://github.com/valkey-io/valkey/pull/3971) ([tests](tests/valkey/3971-8eb992eb0/)): Fix GEORADIUS STORE ACL bypass via duplicate options (#3971): 2 of 3 test(s) reached the change, coverage 100%
- [valkey-io/valkey#4359](https://github.com/valkey-io/valkey/pull/4359) ([tests](tests/valkey/4359-a34bb4c8b/)): Replace the ZSET skiplist with a B+ tree behind an ordered index (#4359): 14 of 18 test(s) reached the change, coverage 76%
- [valkey-io/valkey#4342](https://github.com/valkey-io/valkey/pull/4342) ([tests](tests/valkey/4342-ee7f28805/)): Fix malformed AOF after a short read in MULTI (#4342): declined: no proposed test validated
- [valkey-io/valkey#3973](https://github.com/valkey-io/valkey/pull/3973) ([tests](tests/valkey/3973-00e907878/)): Clear import-source connection flag on RESET (#3973): 2 of 2 test(s) reached the change, coverage 100%
- [valkey-io/valkey#4351](https://github.com/valkey-io/valkey/pull/4351) ([tests](tests/valkey/4351-ea9738ab7/)): Fix NULL deref in usUntilEarliestTimer when all time events are deleted (#4351): 2 of 3 test(s) reached the change, coverage 100%
- [valkey-io/valkey#4383](https://github.com/valkey-io/valkey/pull/4383) ([tests](tests/valkey/4383-0afdf6a4d/)): Tests: Add subject alternative names to the TLS test certificates (#4383): declined: The change modifies only test infrastructure: tests/unit/tls.tcl (the TCL test suite's TLS client setup) and utils/gen-test-certs.sh (the script that generates self-signed test certificates with added subjectAltName extensions). No Valkey server, command, replication, persistence, or tool 
- [valkey-io/valkey#4272](https://github.com/valkey-io/valkey/pull/4272) ([tests](tests/valkey/4272-8abd07fff/)): Fix slow-clocksource check for HW monotonic clock and non-x86 advisories (#4272): declined: The entire diff is confined to checkClocksource() in src/syscheck.c, which is only executed by the 'valkey-server --check-system' diagnostic subcommand (server.c:7611). It is never called during normal server startup, by any command, or by any of the four tools valkey.action.tools can run 
- [valkey-io/valkey#4376](https://github.com/valkey-io/valkey/pull/4376) ([tests](tests/valkey/4376-2fa20454a/)): Fix re-entry into processPendingCommandAndInputBuffer on blocked clients (#4376): declined: no proposed test validated
- [valkey-io/valkey#2204](https://github.com/valkey-io/valkey/pull/2204) ([tests](tests/valkey/2204-fd3d2f929/)): Add cluster-replica-priority to allow better ranking in auto failover (#2204): 4 of 7 test(s) reached the change, coverage 100%
- [valkey-io/valkey#4215](https://github.com/valkey-io/valkey/pull/4215) ([tests](tests/valkey/4215-be0a7cbd2/)):  valkey-benchmark: reject empty command sequence before connecting (#4215): 1 of 1 test(s) reached the change, coverage 93%; [crash, appeared after #4215](https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/81)
- [valkey-io/valkey#4307](https://github.com/valkey-io/valkey/pull/4307) ([tests](tests/valkey/4307-8ee7b372f/)): Fix VLA stack overflow in TLS writev after write failure (#4307): declined: The only behavioral change is the fix to the VLA stack overflow in src/tls.c connTLSWritev, reachable solely on a TLS client connection after a forced TLS write failure followed by a large scatter/gather reply (connTLSWritev is the TLS connection type's writev; plain/unix connections use t
- [valkey-io/valkey#4346](https://github.com/valkey-io/valkey/pull/4346) ([tests](tests/valkey/4346-dad4b1e66/)): Fix frozen monotonic clock on unsynchronised TSC hosts (#4346): declined: The change is confined to one-time TSC calibration in monotonicInit_x86linux() at process startup. Its new branches fire only when the host's TSC reads backwards or a calibration sample is degenerate, i.e. on hardware with unsynchronized TSCs. On the fuzzer's hosts (synchronized constant T
- [valkey-io/valkey#2891](https://github.com/valkey-io/valkey/pull/2891) ([tests](tests/valkey/2891-2d69e5e9b/)): Reset prefetch and ACL stats via CONFIG RESETSTAT (#2891): 3 of 3 test(s) reached the change, coverage 100%
- [valkey-io/valkey#4212](https://github.com/valkey-io/valkey/pull/4212) ([tests](tests/valkey/4212-b2fb0e13f/)): Fix/ready key blocked client uaf (#4212): 2 of 2 test(s) reached the change, coverage 100%
- [valkey-io/valkey#4161](https://github.com/valkey-io/valkey/pull/4161) ([tests](tests/valkey/4161-49d7d96fb/)): Optimize exact XTRIM MAXLEN zero (#4161): 0 of 3 test(s) reached the change, coverage 100%
- [valkey-io/valkey#4152](https://github.com/valkey-io/valkey/pull/4152) ([tests](tests/valkey/4152-114e415b6/)): Account for deferred client frees during eviction (#4152): 0 of 4 test(s) reached the change, coverage 22%
- [valkey-io/valkey#3956](https://github.com/valkey-io/valkey/pull/3956) ([tests](tests/valkey/3956-28aa048e2/)): Fix garbage earliest-expiry read in vsetEstimatedEarliestExpiry RAX path (#3956): declined: The fix touches only vsetEstimatedEarliestExpiry() in src/vset.c, and a repository-wide search shows that function has no callers outside its own declaration (src/vset.h:91) and the new unit test (src/unit/test_vset.cpp): the hash-field expiry paths (vsetRemoveExpired via hashTypeDeleteExp
- [valkey-io/valkey#4341](https://github.com/valkey-io/valkey/pull/4341) ([tests](tests/valkey/4341-1050daf65/)): Add unit test coverage for vsetMemUsage(), vsetClear() and vsetIsValid() (#4341): declined: The change only adds C++ unit tests (test_vset.cpp) and a build-system tweak restricting -fno-var-tracking-assignments to GCC for the unit-test target. No server, replication, persistence, command, or tool code changes: there is no runtime behaviour a scenario could exercise or compare.
- [valkey-io/valkey#4312](https://github.com/valkey-io/valkey/pull/4312) ([tests](tests/valkey/4312-26f44a2e1/)): Fix signed overflow in vset bucket-timestamp rounding  (#4312): 1 of 2 test(s) reached the change, coverage 90%; crash, fixed by #4312 (recorded)
- [valkey-io/valkey#4091](https://github.com/valkey-io/valkey/pull/4091) ([tests](tests/valkey/4091-21c0d49a0/)): Fix crash when processing heartbeat from a node with no local role (#4091): 0 of 6 test(s) reached the change, coverage 35%

### valkey-io/valkey-bloom

[2026-11-06T17:06:12+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42258427239): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-json

[2026-11-06T17:07:17+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42258562411): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-search

[2026-11-06T17:00:04+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42221340802): 20 merge(s) tested, 6 without a code change.

- [valkey-io/valkey-search#904](https://github.com/valkey-io/valkey-search/pull/904) ([tests](tests/valkey-search/904-0e8a35959/)): fix: prevent infinite drain loop in forked BGSave child process (#904): 0 of 1 test(s) reached the change; check, predates #904, since fixed (recorded)
- [valkey-io/valkey-search#901](https://github.com/valkey-io/valkey-search/pull/901) ([tests](tests/valkey-search/901-2cf5844ae/)): Update to oom flags on search commands and fix error propagation in coordinator / iscancelled cases (#901): 3 of 3 test(s) reached the change
- [valkey-io/valkey-search@125b077a3f00](https://github.com/valkey-io/valkey-search/commit/125b077a3f0080dd85d83f5b00984a9f90351951) ([tests](tests/valkey-search/125b077a3/)): Merge pull request #892 from bandalgomsu/gh-729: 2 of 2 test(s) reached the change
- [valkey-io/valkey-search#811](https://github.com/valkey-io/valkey-search/pull/811) ([tests](tests/valkey-search/811-8cc37f6da/)): Drop indexes on test teardown (#811): declined: The diff's only code change is inside a test-only branch of the InfoIndexPartition gRPC handler that runs solely when the debug controlled variable ForceRemoteFailCount is non-zero. Setting it requires FT._DEBUG, which is disabled (replies 'unknown command') unless the module is loaded wit
- [valkey-io/valkey-search#885](https://github.com/valkey-io/valkey-search/pull/885) ([tests](tests/valkey-search/885-e87e5767f/)): Fix Limit of 64 text indexes isn't detected at creation time (#885): 2 of 2 test(s) reached the change
- [valkey-io/valkey-search#887](https://github.com/valkey-io/valkey-search/pull/887) ([tests](tests/valkey-search/887-f8bb344af/)): update snowball compilation check to see if lto_supported (#887): declined: The change only modifies third_party/snowball/CMakeLists.txt to gate LTO behind a check_ipo_supported probe (replacing an unconditional -flto with INTERPROCEDURAL_OPTIMIZATION plus -fno-fat-lto-objects when supported) while keeping -O3 and -w. It is a build/CI compilation-flags change with
- [valkey-io/valkey-search#878](https://github.com/valkey-io/valkey-search/pull/878) ([tests](tests/valkey-search/878-c2a46b4be/)): Simplify text size estimation for non-vector queries (#878): 0 of 3 test(s) reached the change; crash, predates #878, since fixed (recorded)
- [valkey-io/valkey-search#822](https://github.com/valkey-io/valkey-search/pull/822) ([tests](tests/valkey-search/822-44fb9193b/)): test: add comprehensive timeout coverage for all search execution paths (#822): 0 of 2 test(s) reached the change
- [valkey-io/valkey-search#826](https://github.com/valkey-io/valkey-search/pull/826) ([tests](tests/valkey-search/826-c6f5ec55e/)): Make the build of snowball stemmer use optimal compile flags (#826): declined: The change only adds -O3 -flto to the compile flags of the vendored snowball stemmer in third_party/snowball/CMakeLists.txt. No source code, command behaviour, reply, index format, or config changes: it is a build/performance change with no intended behaviour change, so there is nothing fo
- [valkey-io/valkey-search#876](https://github.com/valkey-io/valkey-search/pull/876) ([tests](tests/valkey-search/876-84cd483ed/)): Improve Evaluator Locking (#876): 0 of 3 test(s) reached the change
- [valkey-io/valkey-search#874](https://github.com/valkey-io/valkey-search/pull/874) ([tests](tests/valkey-search/874-ce3baefd2/)): Use vector for storing stem variants (#874): 0 of 2 test(s) reached the change
- [valkey-io/valkey-search#875](https://github.com/valkey-io/valkey-search/pull/875) ([tests](tests/valkey-search/875-92645864c/)): Add 10k tag filter test and Change prefiltering-threshold-ratio and text-rax-target-mutex-pool-size to Dev configs (#875): 2 of 2 test(s) reached the change
- [valkey-io/valkey-search#872](https://github.com/valkey-io/valkey-search/pull/872) ([tests](tests/valkey-search/872-5ffd58559/)): Removed multiple reverse_token string allocations from MutateTarget() (#872): 2 of 2 test(s) reached the change; crash, introduced and since fixed (recorded); crash, introduced and since fixed (recorded); [regressed-fix, the fix of #872 regressed](https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/82)
- [valkey-io/valkey-search#862](https://github.com/valkey-io/valkey-search/pull/862) ([tests](tests/valkey-search/862-7c138ea16/)): Remove upper bound check for stemmed_length (#862): declined: The removed clause `stemmed_length <= word.length()` is unreachable for the only supported stemmer language (English). GetStemmer() always builds the English Snowball stemmer (GetLanguageString returns "english" and CHECK-fails otherwise; LANGUAGE_FIELD is rejected). Every slice_from_s in 
- [valkey-io/valkey-search#853](https://github.com/valkey-io/valkey-search/pull/853) ([tests](tests/valkey-search/853-ee6c9c5fe/)): Refactor Lexer::Tokenize / Stem / Normalize - reduce string allocations (#853): 0 of 4 test(s) reached the change
- [valkey-io/valkey-search#855](https://github.com/valkey-io/valkey-search/pull/855) ([tests](tests/valkey-search/855-fa3ee82d3/)): raxMutate() optimization (#855): declined: no proposed test validated
- [valkey-io/valkey-search#843](https://github.com/valkey-io/valkey-search/pull/843) ([tests](tests/valkey-search/843-7d674fcfa/)): Index locking optimization during ingestion. (#843): 5 of 5 test(s) reached the change; crash, introduced and since fixed (recorded)
- [valkey-io/valkey-search#856](https://github.com/valkey-io/valkey-search/pull/856) ([tests](tests/valkey-search/856-ed4dcc5e5/)): Remove incomplete metrics and incomplete NestedMemoryScope usage (#856): 2 of 2 test(s) reached the change
- [valkey-io/valkey-search#763](https://github.com/valkey-io/valkey-search/pull/763) ([tests](tests/valkey-search/763-8b208870b/)): Add fanout-data-uniformity config (#763): 3 of 3 test(s) reached the change
- [valkey-io/valkey-search#821](https://github.com/valkey-io/valkey-search/pull/821) ([tests](tests/valkey-search/821-9effed130/)): Add config to limit keys from pre-content fetching phase (#821): 3 of 3 test(s) reached the change

## Totals

- Merges whose tests reached the change: 324 of 445 tested (73%)
- Changed-line coverage: median 100% of 195 merge(s) measured, 10 under 50%
- Recall: 95 parents flagged of 132 fix merges
- Claude: $1750.83 in the last 24 hours, $3013.26 in the last 7 days, $3013.26 in all
- Issues filed: 36
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

## Retries

None.
