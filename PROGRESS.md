# Fuzzer walk

Updated 2026-11-06T18:37+00:00.

| Repository | Done | Remaining | Backlog cursor | Last 24 hours | Last 7 days (per day) |
|---|---|---|---|---|---|
| valkey-io/valkey | 406 | 2092 | #4275 (2026-07-27) | 249 | 58.0 |
| valkey-io/valkey-bloom | 109 | 0 | ce516d0ef6e3 (2024-04-26) | 1 | 15.6 |
| valkey-io/valkey-json | 62 | 0 | c1423d7e4b38 (2024-11-25) | 0 | 8.9 |
| valkey-io/valkey-search | 281 | 387 | #851 (2026-03-04) | 181 | 40.1 |

## Last runs

### valkey-io/valkey

[2026-11-06T18:01:55+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42252345373): 20 merge(s) tested, 5 without a code change.

- [valkey-io/valkey#3611](https://github.com/valkey-io/valkey/pull/3611) ([tests](tests/valkey/3611-eb5a7857e/)): Fix RDMA + IO threads re-entrancy and busy-loop via connection postpone masks (#3611): 0 of 4 test(s) reached the change, coverage 96%
- [valkey-io/valkey#4320](https://github.com/valkey-io/valkey/pull/4320) ([tests](tests/valkey/4320-a251aeb5d/)): Fix io-thread race in slot migration establish response handler (#4320): 0 of 2 test(s) reached the change, coverage 100%
- [valkey-io/valkey#4322](https://github.com/valkey-io/valkey/pull/4322) ([tests](tests/valkey/4322-327db78a7/)): reset scan cursor when table is deleted (#4322): 2 of 2 test(s) reached the change, coverage 100%
- [valkey-io/valkey#4279](https://github.com/valkey-io/valkey/pull/4279) ([tests](tests/valkey/4279-0fc8cdafc/)): Update module.c with missing FLAG_REPLICA description (#4279): declined: The diff only edits a doc comment in src/module.c: it rewords the description of VALKEYMODULE_CLIENTINFO_FLAG_PRIMARY and adds the previously missing description of VALKEYMODULE_CLIENTINFO_FLAG_REPLICA. No executable code, flag value, reply, or behavior changes, so there is no path a scena
- [valkey-io/valkey#4328](https://github.com/valkey-io/valkey/pull/4328) ([tests](tests/valkey/4328-c07e9b6cf/)): Fix and complete slot migration pipe comments (#4328): declined: The diff only edits comments in src/server.h: it replaces the placeholder 'xxxx' comment on slot_migration_pipe_conn with a real description and clarifies the comment on slot_migration_pipe_bufflen. No executable code, data structure, config, or protocol behaviour changes, so no scenario c
- [valkey-io/valkey#4148](https://github.com/valkey-io/valkey/pull/4148) ([tests](tests/valkey/4148-573b77f33/)): Add CMD_STALE flag to SYNC/PSYNC commands (#4148): 2 of 4 test(s) reached the change
- [valkey-io/valkey#4140](https://github.com/valkey-io/valkey/pull/4140) ([tests](tests/valkey/4140-cb2d83513/)): Retry PSYNC on -BUSY error instead of downgrading to SYNC (#4140): 1 of 4 test(s) reached the change, coverage 100%
- [valkey-io/valkey#4253](https://github.com/valkey-io/valkey/pull/4253) ([tests](tests/valkey/4253-af321c4df/)): Fix RESP3 push frame torn apart on self-publish with copy avoidance (#4253): 1 of 3 test(s) reached the change, coverage 100%
- [valkey-io/valkey#4300](https://github.com/valkey-io/valkey/pull/4300) ([tests](tests/valkey/4300-527a38222/)): Fix FIELDS token validation for HPERSIST and HTTL family commands (#4300): 3 of 3 test(s) reached the change, coverage 100%
- [valkey-io/valkey#4211](https://github.com/valkey-io/valkey/pull/4211) ([tests](tests/valkey/4211-b349fe282/)): Prevent double-free of the module timer when the callback stops it (#4211): 2 of 4 test(s) reached the change, coverage 100%
- [valkey-io/valkey#4302](https://github.com/valkey-io/valkey/pull/4302) ([tests](tests/valkey/4302-f75e2d55f/)): Skip bad-arity commands when prefetching queued command keys (#4302): 3 of 5 test(s) reached the change, coverage 100%; crash, fixed by #4302 (recorded); crash, fixed by #4302 (recorded)
- [valkey-io/valkey#4301](https://github.com/valkey-io/valkey/pull/4301) ([tests](tests/valkey/4301-c739b9296/)): Fix out-of-bounds read in getNodeByQuery's cross-DB COPY check (#4301): 2 of 4 test(s) reached the change, coverage 100%; crash, fixed by #4301 (recorded)
- [valkey-io/valkey#3510](https://github.com/valkey-io/valkey/pull/3510) ([tests](tests/valkey/3510-12e5e384d/)): Fix TLS infinite busy loop when write/read handlers are removed (#3510): declined: The change is confined to src/tls.c (updateSSLEvent and tlsHandleEvent), code compiled only with BUILD_TLS and executed only for TLS-type connections. The fuzzer has no TLS capability: no installed component speaks TLS or provisions certificates, every client connection (workloads, compare
- [valkey-io/valkey#3730](https://github.com/valkey-io/valkey/pull/3730) ([tests](tests/valkey/3730-6736cc580/)): conf: document chown(2) requirement for unixsocketgroup (#3730): declined: The change only adds a documentation comment to valkey.conf (explaining that unixsocketgroup performs chown(2) and must be permitted under syscall-filtering sandboxes). No server code, config parsing, or runtime behaviour is modified, so no scenario can reach or validate it.
- [valkey-io/valkey#4155](https://github.com/valkey-io/valkey/pull/4155) ([tests](tests/valkey/4155-8eaefda4f/)): Fix MOVE/COPY command skip the current-DB ACL check (#4155): 2 of 2 test(s) reached the change, coverage 100%
- [valkey-io/valkey#3905](https://github.com/valkey-io/valkey/pull/3905) ([tests](tests/valkey/3905-fb94efbd9/)): Optimize .clang-format and add enum/argument formatting options (#3905): declined: The change only edits the .clang-format configuration and reformats existing code to match it: enum declarations in cluster_legacy.c, defrag.c and server.h and a struct initializer list in db.c are reflowed onto multiple lines. Every edit is whitespace and line-break only; no identifier, v
- [valkey-io/valkey#4285](https://github.com/valkey-io/valkey/pull/4285) ([tests](tests/valkey/4285-6df96c44c/)): Optimize notifyKeyspaceEvent to avoid unnecessary allocations (#4285): 0 of 2 test(s) reached the change, coverage 100%
- [valkey-io/valkey#4277](https://github.com/valkey-io/valkey/pull/4277) ([tests](tests/valkey/4277-74249be76/)): Add comment and test for bio RDB disk transfer receive timeout (#4277): declined: The change adds only comments in replication.c (both hunks insert comment lines above pre-existing, unchanged code: the connRecvTimeout call in replicaReceiveRDBFromPrimaryToDisk and the abort-flag/bioDrainWorker logic in cancelReplicationHandshake) plus a TCL integration test documenting 
- [valkey-io/valkey#4252](https://github.com/valkey-io/valkey/pull/4252) ([tests](tests/valkey/4252-dc0411ae1/)): valkey-benchmark: fix -r option to support keyspace values above INT_MAX (#4252): 1 of 1 test(s) reached the change, coverage 91%
- [valkey-io/valkey#4281](https://github.com/valkey-io/valkey/pull/4281) ([tests](tests/valkey/4281-aed05498a/)): Fix macOS unit test by wrapping test object symbols (#4281): declined: The change touches only src/Makefile and src/unit/Makefile: it fixes the macOS unit-test build by wrapping test object symbols, deleting stale static archives before re-creating them, and removing flat-namespace linker flags from the test binary link. No server, command, replication, persi

### valkey-io/valkey-bloom

[2026-11-06T18:37:26+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42267788074): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-json

[2026-11-06T17:38:16+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42263323247): 0 merge(s) tested, 0 without a code change.


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

- Merges whose tests reached the change: 334 of 458 tested (73%)
- Changed-line coverage: median 100% of 207 merge(s) measured, 10 under 50%
- Recall: 100 parents flagged of 139 fix merges
- Claude: $1723.17 in the last 24 hours, $3077.63 in the last 7 days, $3077.63 in all
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
