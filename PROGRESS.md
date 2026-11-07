# Fuzzer walk

Updated 2026-11-07T04:14+00:00.

| Repository | Done | Remaining | Backlog cursor | Last 24 hours | Last 7 days (per day) |
|---|---|---|---|---|---|
| valkey-io/valkey | 512 | 1992 | #3658 (2026-05-31) | 243 | 73.1 |
| valkey-io/valkey-bloom | 109 | 0 | ce516d0ef6e3 (2024-04-26) | 0 | 15.6 |
| valkey-io/valkey-json | 62 | 0 | c1423d7e4b38 (2024-11-25) | 0 | 8.9 |
| valkey-io/valkey-search | 351 | 318 | #707 (2026-02-05) | 181 | 50.1 |

## Last runs

### valkey-io/valkey

[2026-11-07T02:48:53+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42321212483): 20 merge(s) tested, 8 without a code change.

- [valkey-io/valkey#3811](https://github.com/valkey-io/valkey/pull/3811) ([tests](tests/valkey/3811-b4ea71ee1/)): Fix off_t to int truncation in bio repl transfer size reporting (#3811): 2 of 2 test(s) reached the change, coverage 100%
- [valkey-io/valkey#3939](https://github.com/valkey-io/valkey/pull/3939) ([tests](tests/valkey/3939-ce8d17759/)): Fix RESP3 type violation in addReplyCommandSubCommands (#3939): 3 of 3 test(s) reached the change, coverage 66%
- [valkey-io/valkey#3945](https://github.com/valkey-io/valkey/pull/3945) ([tests](tests/valkey/3945-69d3f50c8/)): Exit with error when getRDB ftruncate fails (#3945): declined: The change only alters valkey-cli's exit status (exit 1 instead of a silent exit 0) when ftruncate() fails while stripping the EOF marker from an RDB downloaded via the diskless-sync path in getRDB(). On the normal success path both builds are byte-for-byte identical. No installed fuzzer c
- [valkey-io/valkey#3921](https://github.com/valkey-io/valkey/pull/3921) ([tests](tests/valkey/3921-98005d1c8/)): Reject NAN scores in listpack/ziplist-encoded sorted sets on RDB load (#3921): 2 of 2 test(s) reached the change, coverage 90%; crash, fixed by #3921 (recorded)
- [valkey-io/valkey#3920](https://github.com/valkey-io/valkey/pull/3920) ([tests](tests/valkey/3920-63dc7484b/)): Reject integer overflow of length fields in zipmapValidateIntegrity (#3920): 0 of 1 test(s) reached the change, coverage 100%
- [valkey-io/valkey#3964](https://github.com/valkey-io/valkey/pull/3964) ([tests](tests/valkey/3964-f3bdf50db/)): Omit alldbs rule in ACL SAVE/LIST and CONFIG REWRITE for compatibility (#3964): 3 of 3 test(s) reached the change
- [valkey-io/valkey#3374](https://github.com/valkey-io/valkey/pull/3374): to retry, 2 of 2 run(s) could not start: the run could not start: environment acquisition failed
- [valkey-io/valkey#3950](https://github.com/valkey-io/valkey/pull/3950) ([tests](tests/valkey/3950-e1bc58c72/)): Fix crash when active field-expiry leaves a single-entry HT vset bucket (#3950): 0 of 3 test(s) reached the change, coverage 0%
- [valkey-io/valkey#3947](https://github.com/valkey-io/valkey/pull/3947) ([tests](tests/valkey/3947-c3766df27/)): Align THP advice in createLatencyReport with checkTHPEnabled (#3947): declined: The change only rewords the human-readable THP advice string inside createLatencyReport (LATENCY DOCTOR output); no logic, reply structure, data path, or config behaviour changes. The message is also emitted only when the kernel has allocated anonymous huge pages to the process, an environ
- [valkey-io/valkey#3938](https://github.com/valkey-io/valkey/pull/3938) ([tests](tests/valkey/3938-859649d9a/)): Fix IO-Threads redesign cleanup perf regression from #3544 (#3938): 0 of 8 test(s) reached the change, coverage 85%
- [valkey-io/valkey#3941](https://github.com/valkey-io/valkey/pull/3941) ([tests](tests/valkey/3941-d0ffbabb0/)): Avoid random() % 0 undefined behaviour when cluster-node-timeout < 30 (#3941): 0 of 2 test(s) reached the change, coverage 100%; crash, fixed by #3941 (recorded)
- [valkey-io/valkey#3942](https://github.com/valkey-io/valkey/pull/3942) ([tests](tests/valkey/3942-69004cae7/)): Fix shard_id format specifier in UPDATE message log (#3942): declined: Logging-only fix: it changes the printf format specifier for the fixed 40-byte, non-NUL-terminated shard_id buffer from %s to %.40s in one cluster UPDATE-message LL_NOTICE log line. The pre-fix %s over-reads past the 40-byte array, but only into adjacent fields of the same clusterNode heap
- [valkey-io/valkey#3897](https://github.com/valkey-io/valkey/pull/3897) ([tests](tests/valkey/3897-d9a80af14/)): Assertion check on null key for volatile fields based object (#3897): 0 of 3 test(s) reached the change, coverage 100%
- [valkey-io/valkey#3937](https://github.com/valkey-io/valkey/pull/3937) ([tests](tests/valkey/3937-f7b236dfe/)): Fix typo option name in repl-ping-replica-period comment in valkey.conf (#3937): declined: The change edits only a comment line in valkey.conf, correcting the spelling of the repl-ping-replica-period option name in documentation text. No code path, config parsing, default value, or runtime behaviour changes, so there is nothing a scenario could exercise or compare.
- [valkey-io/valkey#3925](https://github.com/valkey-io/valkey/pull/3925) ([tests](tests/valkey/3925-2013e029c/)): Fix server crash on FUNCTION DELETE after case-only function name collision (#3925): 2 of 2 test(s) reached the change; crash, fixed by #3925 (recorded)
- [valkey-io/valkey#3888](https://github.com/valkey-io/valkey/pull/3888) ([tests](tests/valkey/3888-b0137ef85/)): Report exact dbid for COPY in ACL LOG when db= access is denied (#3888): 4 of 4 test(s) reached the change, coverage 100%; check, not reproduced (recorded); check, introduced and since fixed (recorded); check, not reproduced (recorded); check, introduced and since fixed (recorded)
- [valkey-io/valkey#3848](https://github.com/valkey-io/valkey/pull/3848) ([tests](tests/valkey/3848-d7384b919/)): Fix cluster AUX-field control-character and delimiter injection (#3848): declined: no proposed test validated
- [valkey-io/valkey#3801](https://github.com/valkey-io/valkey/pull/3801) ([tests](tests/valkey/3801-12061032d/)): Validate every DB clause in COPY against ACL db= permissions (#3801): 3 of 3 test(s) reached the change, coverage 94%
- [valkey-io/valkey#3847](https://github.com/valkey-io/valkey/pull/3847) ([tests](tests/valkey/3847-e71299d59/)): Harden SENTINEL commands and config rewrite against control-character injection (#3847): declined: The change is entirely inside src/sentinel.c and only executes when valkey-server runs in Sentinel mode: sentinelValidateArgs guards SENTINEL SET / SENTINEL MONITOR / SENTINEL CONFIG SET, and the sdscatrepr escaping runs in rewriteConfigSentinelOption, which rewrites sentinel state. Sentin
- [valkey-io/valkey#3806](https://github.com/valkey-io/valkey/pull/3806) ([tests](tests/valkey/3806-7abe49a6c/)): Add ALL_DBS flag to CLUSTER FLUSHSLOT for database-level ACL (#3806): 2 of 2 test(s) reached the change

### valkey-io/valkey-bloom

[2026-11-07T04:14:04+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42337481629): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-json

[2026-11-07T03:44:32+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42335321056): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-search

[2026-11-07T01:34:15+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42306322056): 20 merge(s) tested, 1 without a code change.

- [valkey-io/valkey-search#1546](https://github.com/valkey-io/valkey-search/pull/1546) ([tests](tests/valkey-search/1546-6e403afa1/)): feat(vector-range): add epsilon-aware HNSW range traversal (#1546): 4 of 4 test(s) reached the change
- [valkey-io/valkey-search#667](https://github.com/valkey-io/valkey-search/pull/667) ([tests](tests/valkey-search/667-fa3ae057c/)): Raise limit on number of attributes. Correct misnamed max-vector-attribute (#667): 5 of 5 test(s) reached the change
- [valkey-io/valkey-search#749](https://github.com/valkey-io/valkey-search/pull/749) ([tests](tests/valkey-search/749-b383d752f/)): Make fanout async threshold configurable (#749): 3 of 3 test(s) reached the change
- [valkey-io/valkey-search#746](https://github.com/valkey-io/valkey-search/pull/746) ([tests](tests/valkey-search/746-311be1eb3/)): Use consistent + performant casefolding logic in both query/ingestion (#746): 2 of 2 test(s) reached the change
- [valkey-io/valkey-search#711](https://github.com/valkey-io/valkey-search/pull/711) ([tests](tests/valkey-search/711-da4e28f2d/)): Hybrid vector text query integration test (#711): declined: The change is test-and-CI only: it adds hybrid vector/text query cases to integration/test_fulltext.py (refactoring existing assertions into a shared HYBRID_QUERY_EXPECTED_RESULTS table and adding a KNN variant of the same queries) and amends scripts/common.rc to pip-install an extra requi
- [valkey-io/valkey-search#743](https://github.com/valkey-io/valkey-search/pull/743) ([tests](tests/valkey-search/743-a6a71b394/)):  ftdebug to list configs in search module (#743): 0 of 2 test(s) reached the change; [crash, appeared after #743](https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/87)
- [valkey-io/valkey-search#674](https://github.com/valkey-io/valkey-search/pull/674) ([tests](tests/valkey-search/674-a2c814f7d/)): Add grpc coordinator threads CPU time metric to INFO command (#674): declined: The change only does anything when the module is loaded with the --use-coordinator module argument: the ThreadGroupCPUMonitor is created in ValkeySearch::Startup only under GetUseCoordinator(), and the new INFO field search_coordinator_threads_cpu_time_sec is VisibleIf(UsingCoordinator()),
- [valkey-io/valkey-search#722](https://github.com/valkey-io/valkey-search/pull/722) ([tests](tests/valkey-search/722-e015fa874/)): Implement SLOP and INORDER in FT.AGGREGATE (#722): 3 of 3 test(s) reached the change
- [valkey-io/valkey-search#741](https://github.com/valkey-io/valkey-search/pull/741): to retry, `deps=/opt/valkey-search-deps` failed in parent (exit 1): [138/223] Building CXX object src/commands/CMakeFiles/commands.dir/commands.cc.o FAILED: [code=1] src/commands/CMakeFiles/commands.dir/command
- [valkey-io/valkey-search#735](https://github.com/valkey-io/valkey-search/pull/735): to retry, `deps=/opt/valkey-search-deps` failed in merge (exit 1): [138/223] Building CXX object src/commands/CMakeFiles/commands.dir/commands.cc.o FAILED: [code=1] src/commands/CMakeFiles/commands.dir/commands
- [valkey-io/valkey-search#734](https://github.com/valkey-io/valkey-search/pull/734): to retry, `deps=/opt/valkey-search-deps` failed in merge (exit 1): [137/221] Building CXX object src/commands/CMakeFiles/commands.dir/commands.cc.o FAILED: [code=1] src/commands/CMakeFiles/commands.dir/commands
- [valkey-io/valkey-search#621](https://github.com/valkey-io/valkey-search/pull/621): to retry, `deps=/opt/valkey-search-deps` failed in merge (exit 1): [136/221] Building CXX object src/commands/CMakeFiles/commands.dir/commands.cc.o FAILED: [code=1] src/commands/CMakeFiles/commands.dir/commands
- [valkey-io/valkey-search#618](https://github.com/valkey-io/valkey-search/pull/618) ([tests](tests/valkey-search/618-9b29188d0/)): Feature/sortby implementation (#618): 5 of 5 test(s) reached the change
- [valkey-io/valkey-search#639](https://github.com/valkey-io/valkey-search/pull/639) ([tests](tests/valkey-search/639-444fb186b/)): Increase default for allowed number of indexes to 1000 (#639): 3 of 3 test(s) reached the change
- [valkey-io/valkey-search#671](https://github.com/valkey-io/valkey-search/pull/671) ([tests](tests/valkey-search/671-cff18a5a0/)): Endurance Testing - adding more search functionality testing. (#671): declined: The change modifies only valkey-search's own integration-test harness: testing/integration/stability_runner.py (new memtier test documents, a different FT.SEARCH query mix for TEXT indexes, consolidated delete/expire memtier commands, and nostem/with_suffix_trie tweaks in the test's index 
- [valkey-io/valkey-search#731](https://github.com/valkey-io/valkey-search/pull/731) ([tests](tests/valkey-search/731-0345b6268/)): Endurance test added as a GitHub workflow (#731): declined: The change is purely CI/tooling: it adds a new GitHub workflow (.github/workflows/endurance_tests.yml), a benchmark driver script (scripts/benchmark/run_endurance_test.sh), and installs jq in the devcontainer Dockerfile. No module source, command handling, indexing, replication, or persist
- [valkey-io/valkey-search#541](https://github.com/valkey-io/valkey-search/pull/541) ([tests](tests/valkey-search/541-6bdc2dde5/)): Block Search Response on Conflicting In-Flight Keys for fulltext queries (#541): 2 of 2 test(s) reached the change
- [valkey-io/valkey-search#712](https://github.com/valkey-io/valkey-search/pull/712) ([tests](tests/valkey-search/712-945d29388/)): Ensure content is always retrieved on correct DB during search (#712): declined: no proposed test validated
- [valkey-io/valkey-search#708](https://github.com/valkey-io/valkey-search/pull/708) ([tests](tests/valkey-search/708-703331414/)): Undo the renaming of an existing config (#708): 2 of 2 test(s) reached the change
- [valkey-io/valkey-search#685](https://github.com/valkey-io/valkey-search/pull/685) ([tests](tests/valkey-search/685-cd722c45b/)): Fix OOM Handling for SearchBased ReadOnly commands (#685): 2 of 2 test(s) reached the change

## Totals

- Merges whose tests reached the change: 382 of 542 tested (70%)
- Changed-line coverage: median 100% of 245 merge(s) measured, 16 under 50%
- Recall: 112 parents flagged of 159 fix merges
- Claude: $1511.19 in the last 24 hours, $3708.15 in the last 7 days, $3708.15 in all
- Issues filed: 41
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

## Retries

- [valkey-io/valkey-search#741](https://github.com/valkey-io/valkey-search/pull/741): attempt 1 of 3, `deps=/opt/valkey-search-deps` failed in parent (exit 1): [138/223] Building CXX object src/commands/CMakeFiles/commands.dir/commands.cc.o FAILED: [code=1] src/commands/CMakeFiles/commands.dir/command
- [valkey-io/valkey-search#735](https://github.com/valkey-io/valkey-search/pull/735): attempt 1 of 3, `deps=/opt/valkey-search-deps` failed in merge (exit 1): [138/223] Building CXX object src/commands/CMakeFiles/commands.dir/commands.cc.o FAILED: [code=1] src/commands/CMakeFiles/commands.dir/commands
- [valkey-io/valkey-search#734](https://github.com/valkey-io/valkey-search/pull/734): attempt 1 of 3, `deps=/opt/valkey-search-deps` failed in merge (exit 1): [137/221] Building CXX object src/commands/CMakeFiles/commands.dir/commands.cc.o FAILED: [code=1] src/commands/CMakeFiles/commands.dir/commands
- [valkey-io/valkey-search#621](https://github.com/valkey-io/valkey-search/pull/621): attempt 1 of 3, `deps=/opt/valkey-search-deps` failed in merge (exit 1): [136/221] Building CXX object src/commands/CMakeFiles/commands.dir/commands.cc.o FAILED: [code=1] src/commands/CMakeFiles/commands.dir/commands
- [valkey-io/valkey#3374](https://github.com/valkey-io/valkey/pull/3374): attempt 1 of 3, 2 of 2 run(s) could not start: the run could not start: environment acquisition failed
