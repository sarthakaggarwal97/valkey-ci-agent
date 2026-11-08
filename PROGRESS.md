# Fuzzer walk

Updated 2026-11-08T05:24+00:00.

| Repository | Done | Remaining | Backlog cursor | Last 24 hours | Last 7 days (per day) |
|---|---|---|---|---|---|
| valkey-io/valkey | 862 | 1645 | #1032 (2025-12-22) | 320 | 123.1 |
| valkey-io/valkey-bloom | 109 | 0 | ce516d0ef6e3 (2024-04-26) | 0 | 15.6 |
| valkey-io/valkey-json | 62 | 0 | c1423d7e4b38 (2024-11-25) | 0 | 8.9 |
| valkey-io/valkey-search | 563 | 106 | #152 (2025-05-29) | 191 | 80.4 |

## Last runs

### valkey-io/valkey

[2026-11-08T05:24:21+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42436474417): 20 merge(s) tested, 2 without a code change.

- [valkey-io/valkey#2964](https://github.com/valkey-io/valkey/pull/2964) ([tests](tests/valkey/2964-82a312d8a/)): replace info cluster info in a dedicated new section (#2964): 2 of 2 test(s) reached the change, coverage 100%
- [valkey-io/valkey#3007](https://github.com/valkey-io/valkey/pull/3007) ([tests](tests/valkey/3007-dbe07b7c9/)): Untrack key based on old->hasembkey (#3007): 2 of 4 test(s) reached the change, coverage 100%; crash, fixed by #3007 (recorded)
- [valkey-io/valkey#3003](https://github.com/valkey-io/valkey/pull/3003) ([tests](tests/valkey/3003-e4a3e9f0a/)): fix(hash): Untrack hash with volatile fields when it is overwritten  (#3003): 2 of 2 test(s) reached the change, coverage 100%; crash, fixed by #3003 (recorded)
- [valkey-io/valkey#2975](https://github.com/valkey-io/valkey/pull/2975) ([tests](tests/valkey/2975-fd56b21df/)): Add sdsnsplitargs to reduce sds allocation. (#2975): 0 of 8 test(s) reached the change, coverage 90%
- [valkey-io/valkey#2998](https://github.com/valkey-io/valkey/pull/2998) ([tests](tests/valkey/2998-263d9ea41/)): Fix zero length hash creation with HSETEX (#2998): 3 of 3 test(s) reached the change, coverage 100%
- [valkey-io/valkey#2985](https://github.com/valkey-io/valkey/pull/2985) ([tests](tests/valkey/2985-d10b06009/)): Avoid useless seek in xtrim (#2985): declined: The change adds an early return in streamTrim when trim_strategy is MAXLEN and s->length <= maxlen, purely to skip allocating and seeking a rax iterator. The identical condition was already the first check inside the trim loop (t_stream.c:726), so the old code broke out of the loop on the 
- [valkey-io/valkey#3000](https://github.com/valkey-io/valkey/pull/3000) ([tests](tests/valkey/3000-9a8f1c8c1/)): fix(HSETEX): replace strcmp with strcasecmp (#3000): 3 of 3 test(s) reached the change, coverage 100%
- [valkey-io/valkey#2425](https://github.com/valkey-io/valkey/pull/2425) ([tests](tests/valkey/2425-50667e0ad/)): Skip sorting empty lists in sortCommandGeneric to prevent undefined behavior when calling pqsort (#2425): 0 of 6 test(s) reached the change, coverage 100%
- [valkey-io/valkey#2895](https://github.com/valkey-io/valkey/pull/2895) ([tests](tests/valkey/2895-15e79ef00/)): Add FIFO and mutexQueue with bio.c refactored (#2895): 0 of 18 test(s) reached the change, coverage 48%
- [valkey-io/valkey#2340](https://github.com/valkey-io/valkey/pull/2340) ([tests](tests/valkey/2340-e843c18f1/)): Valkey Fuzzer (#2340): 7 of 7 test(s) reached the change, coverage 86%
- [valkey-io/valkey#2973](https://github.com/valkey-io/valkey/pull/2973) ([tests](tests/valkey/2973-b56543285/)): avoid memory leak of new argv when hexpire commands target only non-exiting fields (#2973): 0 of 2 test(s) reached the change, coverage 100%
- [valkey-io/valkey#2974](https://github.com/valkey-io/valkey/pull/2974) ([tests](tests/valkey/2974-7cef1e8ee/)): fix hincrby* update volatile key tracking (#2974): 2 of 2 test(s) reached the change, coverage 100%; check, introduced and since fixed (recorded)
- [valkey-io/valkey#2983](https://github.com/valkey-io/valkey/pull/2983) ([tests](tests/valkey/2983-9c5d004bf/)): Fix chained replica crash when doing dual channel replication (#2983): 0 of 6 test(s) reached the change, coverage 0%
- [valkey-io/valkey#2981](https://github.com/valkey-io/valkey/pull/2981) ([tests](tests/valkey/2981-e179fddcf/)): Revert hgetall dynamic deferred response  (#2981): 0 of 3 test(s) reached the change, coverage 100%
- [valkey-io/valkey#2963](https://github.com/valkey-io/valkey/pull/2963) ([tests](tests/valkey/2963-2a6137be0/)): Fix compile overflow warnings (#2963): declined: The only change adds a defensive guard (`sdslen(parts[0]) < 2`) in evalExtractShebangFlags to silence a compiler integer-underflow warning on `sdslen(parts[0]) - 2`. The function body only runs when the script starts with the literal `#!`, and the first token from sdssplitargs always begin
- [valkey-io/valkey#2944](https://github.com/valkey-io/valkey/pull/2944) ([tests](tests/valkey/2944-26162effd/)): Restrict ttl from being negative and avoid crash in import-mode (#2944): 2 of 2 test(s) reached the change, coverage 100%; crash, fixed by #2944 (recorded)
- [valkey-io/valkey#2966](https://github.com/valkey-io/valkey/pull/2966) ([tests](tests/valkey/2966-75707ac43/)): Avoid hgetall deferred response (#2966): 0 of 3 test(s) reached the change, coverage 100%
- [valkey-io/valkey#2309](https://github.com/valkey-io/valkey/pull/2309) ([tests](tests/valkey/2309-122070cf8/)): Database-level access control (#2309): 7 of 7 test(s) reached the change, coverage not measured ([Errno 2] No such file or directory: 'walk-work/coverage/src/cli_commands.c.gcov')
- [valkey-io/valkey#2755](https://github.com/valkey-io/valkey/pull/2755) ([tests](tests/valkey/2755-328b3ce3c/)): Add support for Atomic Slot Migration to CLI (#2755): 5 of 5 test(s) reached the change, coverage 72%
- [valkey-io/valkey#2935](https://github.com/valkey-io/valkey/pull/2935) ([tests](tests/valkey/2935-992b88626/)): Fix incorrect comment about STATS_METRIC_* Macro in server.h (#2935): declined: The change only edits two comments in server.h describing the STATS_METRIC_NET_INPUT and STATS_METRIC_NET_INPUT_REPLICATION enum values ('read to network' → 'read from network'). No code, behaviour, reply, or binary output changes, so there is nothing to test.

### valkey-io/valkey-bloom

[2026-11-08T05:02:34+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42449148231): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-json

[2026-11-08T05:24:47+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42450641740): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-search

[2026-11-08T04:33:16+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42441524138): 20 merge(s) tested, 2 without a code change.

- [valkey-io/valkey-search#207](https://github.com/valkey-io/valkey-search/pull/207) ([tests](tests/valkey-search/207-b6f344a85/)): Add FT._DEBUG command (#207): 3 of 3 test(s) reached the change; [crash, appeared after #207](https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/87)
- [valkey-io/valkey-search#202](https://github.com/valkey-io/valkey-search/pull/202) ([tests](tests/valkey-search/202-71e80207c/)): Revision of info framework. (#202): 3 of 3 test(s) reached the change
- [valkey-io/valkey-search#194](https://github.com/valkey-io/valkey-search/pull/194) ([tests](tests/valkey-search/194-b91e54993/)): Rebranding module to use Valkey instead of Redis (#194): 0 of 3 test(s) reached the change
- [valkey-io/valkey-search#198](https://github.com/valkey-io/valkey-search/pull/198) ([tests](tests/valkey-search/198-08b14a76f/)): Load JSON module into the test framework (#198): declined: The change is confined to valkey-search's own test/CI infrastructure: integration/run.sh gains a venv setup and a step that clones and builds the valkey-json module, the pytest fixture loads that JSON module next to the search module, and scripts/common.rc gets a log helper. No source file
- [valkey-io/valkey-search#173](https://github.com/valkey-io/valkey-search/pull/173) ([tests](tests/valkey-search/173-254b2cb53/)): Spelling (#173): declined: The change is a spelling-only PR: it fixes typos in documentation (COMMANDS.md, README.md, QUICK_START.md, rfc/rdb-format.md), code comments, and build scripts, and renames internal identifiers consistently at declaration and every use site in the same diff (CalcBestMatchingPrefiltereddKey
- [valkey-io/valkey-search#188](https://github.com/valkey-io/valkey-search/pull/188) ([tests](tests/valkey-search/188-03b5b06a3/)): Integrate valkey-test-framework (#188): declined: The change only adds integration-test infrastructure to the valkey-search repository: a new Python test harness (integration/valkey_search_test_case.py, integration/test_vss_basic.py), a test runner script (integration/run.sh), a README, and shell helpers (scripts/common.rc). No file under
- [valkey-io/valkey-search#192](https://github.com/valkey-io/valkey-search/pull/192) ([tests](tests/valkey-search/192-cc56d7979/)): Fix broken CI (#192): declined: The change modifies only CI infrastructure scripts: ci/build_ubuntu.sh (removes wget --show-progress, affecting only CI log verbosity) and testing/integration/run.sh (pins the valkey-json dependency to a fixed commit and builds it with --release for the integration test harness). No module
- [valkey-io/valkey-search#12](https://github.com/valkey-io/valkey-search/pull/12) ([tests](tests/valkey-search/12-403d7b574/)): Initial text search RFC (#12): declined: The change adds only a documentation file (rfc/text-field.md, an RFC proposing a future TEXT field type) and a .vscode editor settings tweak. No source code, build file, command, or configuration of the module is modified, so there is no behaviour change to test.
- [valkey-io/valkey-search#178](https://github.com/valkey-io/valkey-search/pull/178) ([tests](tests/valkey-search/178-09411fb98/)): The module can now be built on macOS (#178): declined: The change is a macOS build-portability PR: almost all hunks are CMake/build.sh changes, macOS-only submodule patches, or test-only code. The three runtime hunks are no-ops on the Linux/gcc builds the fuzzer runs: the filter_parser infinity constants only change under #if defined(__clang__
- [valkey-io/valkey-search#177](https://github.com/valkey-io/valkey-search/pull/177) ([tests](tests/valkey-search/177-221d4c602/)): Add coordinator port info metric (#177): declined: no proposed test validated
- [valkey-io/valkey-search#172](https://github.com/valkey-io/valkey-search/pull/172): to retry, `export NINJA_TOOL=ninja` failed in parent (exit 1): [22/206] Building C object third_party/hdrhistogram_c/src/CMakeFiles/hdrhistogram_c.dir/hdr_encoding.c.o [23/206] Building C object third_party/hdr
- [valkey-io/valkey-search#144](https://github.com/valkey-io/valkey-search/pull/144): to retry, `export NINJA_TOOL=ninja` failed in merge (exit 1): [20/206] Linking CXX static library vmsdk/src/libmemory_allocation_overrides.a [21/206] Building C object third_party/hdrhistogram_c/src/CMakeFiles/
- [valkey-io/valkey-search#168](https://github.com/valkey-io/valkey-search/pull/168): to retry, `export NINJA_TOOL=ninja` failed in merge (exit 1): [10/206] Running cpp protocol buffer compiler on /home/runner/work/valkey-ci-agent/valkey-ci-agent/walk-work/merge/third_party/hnswlib/index.proto [
- [valkey-io/valkey-search#166](https://github.com/valkey-io/valkey-search/pull/166): to retry, `export NINJA_TOOL=ninja` failed in merge (exit 1): [15/206] Linking CXX static library vmsdk/src/status/libstatus_macros.a [16/206] Building CXX object vmsdk/src/CMakeFiles/log.dir/log.cc.o [17/206] 
- [valkey-io/valkey-search#162](https://github.com/valkey-io/valkey-search/pull/162): to retry, `export NINJA_TOOL=ninja` failed in merge (exit 1): [22/206] Building CXX object third_party/hnswlib/CMakeFiles/index_cc_proto.dir/index.pb.cc.o [23/206] Building C object third_party/hdrhistogram_c/s
- [valkey-io/valkey-search#161](https://github.com/valkey-io/valkey-search/pull/161): to retry, `export NINJA_TOOL=ninja` failed in merge (exit 1): [21/206] Building C object third_party/hdrhistogram_c/src/CMakeFiles/hdrhistogram_c.dir/hdr_encoding.c.o [22/206] Building CXX object src/CMakeFiles
- [valkey-io/valkey-search#159](https://github.com/valkey-io/valkey-search/pull/159): to retry, `export NINJA_TOOL=ninja` failed in merge (exit 1): [20/206] Linking CXX static library src/libattribute_data_type.a [21/206] Linking CXX static library vmsdk/src/libmemory_allocation_overrides.a [22/
- [valkey-io/valkey-search#158](https://github.com/valkey-io/valkey-search/pull/158): to retry, `export NINJA_TOOL=ninja` failed in merge (exit 1): [13/206] Linking CXX static library vmsdk/src/libmemory_allocation.a [14/206] Building CXX object src/indexes/CMakeFiles/index_schema_cc_proto.dir/_
- [valkey-io/valkey-search#148](https://github.com/valkey-io/valkey-search/pull/148): to retry, `export NINJA_TOOL=ninja` failed in merge (exit 1): [15/206] Linking CXX static library src/indexes/libindex_schema_cc_proto.a [16/206] Building CXX object vmsdk/src/CMakeFiles/log.dir/log.cc.o [17/20
- [valkey-io/valkey-search#154](https://github.com/valkey-io/valkey-search/pull/154): to retry, `export NINJA_TOOL=ninja` failed in merge (exit 1): [20/206] Building CXX object third_party/hnswlib/CMakeFiles/index_cc_proto.dir/index.pb.cc.o [21/206] Building CXX object vmsdk/src/CMakeFiles/memor

## Totals

- Merges whose tests reached the change: 556 of 805 tested (69%)
- Changed-line coverage: median 100% of 394 merge(s) measured, 26 under 50%
- Recall: 157 parents flagged of 230 fix merges
- Claude: $1812.44 in the last 24 hours, $5746.66 in the last 7 days, $5746.66 in all
- Issues filed: 53
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/100
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/101
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/102
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

- [valkey-io/valkey-search#172](https://github.com/valkey-io/valkey-search/pull/172): attempt 1 of 3, `export NINJA_TOOL=ninja` failed in parent (exit 1): [22/206] Building C object third_party/hdrhistogram_c/src/CMakeFiles/hdrhistogram_c.dir/hdr_encoding.c.o [23/206] Building C object third_party/hdr
- [valkey-io/valkey-search#144](https://github.com/valkey-io/valkey-search/pull/144): attempt 1 of 3, `export NINJA_TOOL=ninja` failed in merge (exit 1): [20/206] Linking CXX static library vmsdk/src/libmemory_allocation_overrides.a [21/206] Building C object third_party/hdrhistogram_c/src/CMakeFiles/
- [valkey-io/valkey-search#168](https://github.com/valkey-io/valkey-search/pull/168): attempt 1 of 3, `export NINJA_TOOL=ninja` failed in merge (exit 1): [10/206] Running cpp protocol buffer compiler on /home/runner/work/valkey-ci-agent/valkey-ci-agent/walk-work/merge/third_party/hnswlib/index.proto [
- [valkey-io/valkey-search#166](https://github.com/valkey-io/valkey-search/pull/166): attempt 1 of 3, `export NINJA_TOOL=ninja` failed in merge (exit 1): [15/206] Linking CXX static library vmsdk/src/status/libstatus_macros.a [16/206] Building CXX object vmsdk/src/CMakeFiles/log.dir/log.cc.o [17/206] 
- [valkey-io/valkey-search#162](https://github.com/valkey-io/valkey-search/pull/162): attempt 1 of 3, `export NINJA_TOOL=ninja` failed in merge (exit 1): [22/206] Building CXX object third_party/hnswlib/CMakeFiles/index_cc_proto.dir/index.pb.cc.o [23/206] Building C object third_party/hdrhistogram_c/s
- [valkey-io/valkey-search#161](https://github.com/valkey-io/valkey-search/pull/161): attempt 1 of 3, `export NINJA_TOOL=ninja` failed in merge (exit 1): [21/206] Building C object third_party/hdrhistogram_c/src/CMakeFiles/hdrhistogram_c.dir/hdr_encoding.c.o [22/206] Building CXX object src/CMakeFiles
- [valkey-io/valkey-search#159](https://github.com/valkey-io/valkey-search/pull/159): attempt 1 of 3, `export NINJA_TOOL=ninja` failed in merge (exit 1): [20/206] Linking CXX static library src/libattribute_data_type.a [21/206] Linking CXX static library vmsdk/src/libmemory_allocation_overrides.a [22/
- [valkey-io/valkey-search#158](https://github.com/valkey-io/valkey-search/pull/158): attempt 1 of 3, `export NINJA_TOOL=ninja` failed in merge (exit 1): [13/206] Linking CXX static library vmsdk/src/libmemory_allocation.a [14/206] Building CXX object src/indexes/CMakeFiles/index_schema_cc_proto.dir/_
- [valkey-io/valkey-search#148](https://github.com/valkey-io/valkey-search/pull/148): attempt 1 of 3, `export NINJA_TOOL=ninja` failed in merge (exit 1): [15/206] Linking CXX static library src/indexes/libindex_schema_cc_proto.a [16/206] Building CXX object vmsdk/src/CMakeFiles/log.dir/log.cc.o [17/20
- [valkey-io/valkey-search#154](https://github.com/valkey-io/valkey-search/pull/154): attempt 1 of 3, `export NINJA_TOOL=ninja` failed in merge (exit 1): [20/206] Building CXX object third_party/hnswlib/CMakeFiles/index_cc_proto.dir/index.pb.cc.o [21/206] Building CXX object vmsdk/src/CMakeFiles/memor
- [valkey-io/valkey#3800](https://github.com/valkey-io/valkey/pull/3800): attempt 0 of 3, Claude Code failed (rc=1): API Error: Claude Code is unable to respond to this request, which appears to violate our Usage Policy (https://www.anthropic.com/legal/aup). This request triggered restrict
- [valkey-io/valkey#3613](https://github.com/valkey-io/valkey/pull/3613): attempt 0 of 3, Claude Code failed (rc=1): API Error: Claude Code is unable to respond to this request, which appears to violate our Usage Policy (https://www.anthropic.com/legal/aup). This request triggered restrict
- [valkey-io/valkey#3498](https://github.com/valkey-io/valkey/pull/3498): attempt 0 of 3, Claude Code failed (rc=1): API Error: Claude Code is unable to respond to this request, which appears to violate our Usage Policy (https://www.anthropic.com/legal/aup). This request triggered restrict
