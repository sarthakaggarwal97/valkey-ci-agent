# Fuzzer walk

Updated 2026-11-09T19:01+00:00.

| Repository | Done | Remaining | Backlog cursor | Last 24 hours | Last 7 days (per day) |
|---|---|---|---|---|---|
| valkey-io/valkey | 1275 | 1234 | #2039 (2025-05-12) | 309 | 182.1 |
| valkey-io/valkey-bloom | 109 | 0 | ce516d0ef6e3 (2024-04-26) | 0 | 15.6 |
| valkey-io/valkey-json | 63 | 0 | c1423d7e4b38 (2024-11-25) | 1 | 9.0 |
| valkey-io/valkey-search | 669 | 0 | f8106e72e984 (2024-11-27) | 34 | 95.6 |

## Last runs

### valkey-io/valkey

[2026-11-09T19:01:56+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42663944049): 20 merge(s) tested, 4 without a code change.

- [valkey-io/valkey#5280](https://github.com/valkey-io/valkey/pull/5280) ([tests](tests/valkey/5280-702a10751/)): Fix full sync loop after PSYNC to SYNC fallback (#5280): declined: The fix changes behaviour only on the PSYNC-to-SYNC fallback path, which is entered solely when a primary rejects PSYNC with a generic -ERR (PSYNC_NOT_SUPPORTED). No real Valkey primary ever does that - it always answers +FULLRESYNC/+CONTINUE/+DUALCHANNELSYNC or a transient -NOMASTERLINK/-
- [valkey-io/valkey#2146](https://github.com/valkey-io/valkey/pull/2146): to retry, Claude Code failed (rc=1): API Error: Claude's response exceeded the 64000 output token maximum. To configure this behavior, set the CLAUDE_CODE_MAX_OUTPUT_TOKENS environment variable.
- [valkey-io/valkey#2099](https://github.com/valkey-io/valkey/pull/2099) ([tests](tests/valkey/2099-4092c9c8e/)): Optimize string2ll with load-time CPU feature check using IFUNC resolver (#2099): 0 of 3 test(s) reached the change, coverage 85%
- [valkey-io/valkey#2118](https://github.com/valkey-io/valkey/pull/2118) ([tests](tests/valkey/2118-36b51c9e1/)): Show more decimal digits for `avg chain length` field in `debug htstats` full output (#2118): 2 of 2 test(s) reached the change
- [valkey-io/valkey#2126](https://github.com/valkey-io/valkey/pull/2126) ([tests](tests/valkey/2126-db14ac2f9/)): Fix UBSAN run for hashtable unittest (#2126): declined: The change modifies only src/unit/test_hashtable.c, a unit-test file: it replaces a 1-element stack array with a zmalloc'd array to silence a UBSAN out-of-bounds pointer computation in the test itself, and frees it afterwards. No Valkey server, command, replication, persistence or tool cod
- [valkey-io/valkey@f56ff1026864](https://github.com/valkey-io/valkey/commit/f56ff1026864ea7a19164565724a65b04cf7eb85) ([tests](tests/valkey/f56ff1026/)): Remove an unnecessary check that seems to overflow in 32-bit: declined: The diff only modifies the project's own C unit test (src/unit/test_hashtable.c), deleting two TEST_ASSERT range checks on a pointer in test_random_entry_sparse_table that could overflow on 32-bit. No server code, command behaviour, replication, persistence or tool code changes, so there i
- [valkey-io/valkey@a59635c73f62](https://github.com/valkey-io/valkey/commit/a59635c73f62ea8bc464d7a4ae0e3f7cc572eac3) ([tests](tests/valkey/a59635c73/)): Fix object unit test for 32-bit builds: declined: The change modifies only src/unit/test_object.c, adjusting test string lengths so the embedded-string unit test accounts for 32-bit pointer sizes. It is a test-only fix: no server code, command behaviour, replication, or persistence path changes, so there is nothing for a fuzzer scenario t
- [valkey-io/valkey#1975](https://github.com/valkey-io/valkey/pull/1975) ([tests](tests/valkey/1975-0012ca73c/)): Add DELIFEQ command (#1975): 4 of 4 test(s) reached the change, coverage 85%; check, introduced and since fixed (recorded)
- [valkey-io/valkey#2107](https://github.com/valkey-io/valkey/pull/2107) ([tests](tests/valkey/2107-c55110803/)): Optimize WATCH by equalStringObjects early length check  (#2107): declined: The change adds an early length-check short-circuit to equalStringObjects(): when both objects are sds-encoded and sdslen differs, it returns 0 without calling compareStringObjects(). This is semantically identical to the old path, since compareStringObjects() always returns non-zero for s
- [valkey-io/valkey#2113](https://github.com/valkey-io/valkey/pull/2113) ([tests](tests/valkey/2113-30599c493/)): fix commandlog argument schema to use pure tokens (#2113): 2 of 2 test(s) reached the change
- [valkey-io/valkey#1725](https://github.com/valkey-io/valkey/pull/1725) ([tests](tests/valkey/1725-1531b44a1/)): Respect process umask when creating data files (#1725): 0 of 3 test(s) reached the change, coverage 100%
- [valkey-io/valkey#2088](https://github.com/valkey-io/valkey/pull/2088) ([tests](tests/valkey/2088-496f7d142/)): Optimize saving cluster config file using sdscatfmt (#2088): 0 of 3 test(s) reached the change, coverage 38%
- [valkey-io/valkey#2098](https://github.com/valkey-io/valkey/pull/2098) ([tests](tests/valkey/2098-daea05b1e/)): Allow mixing quoted and unquoted inline args (#2098): 1 of 3 test(s) reached the change, coverage 100%
- [valkey-io/valkey#2096](https://github.com/valkey-io/valkey/pull/2096) ([tests](tests/valkey/2096-8b0d952c2/)): Further improve performance of sdssplitargs (#2096): declined: no proposed test validated
- [valkey-io/valkey#2085](https://github.com/valkey-io/valkey/pull/2085) ([tests](tests/valkey/2085-45d03e616/)): Fix random element in skewed sparse hash table (#2085): declined: The change only alters how hashtableFairRandomEntry/hashtableSampleEntries pick a random entry in a sparse/skewed hash table: fewer samples when sparse and a fresh random cursor per scan iteration. This is a performance-and-fairness fix to an internal random sampler with no deterministic o
- [valkey-io/valkey#2091](https://github.com/valkey-io/valkey/pull/2091) ([tests](tests/valkey/2091-8d686dd74/)): Fix minor memory leak in valkey-cli when command table hint fails due to NOAUTH (#2091): declined: no proposed test validated
- [valkey-io/valkey#2087](https://github.com/valkey-io/valkey/pull/2087) ([tests](tests/valkey/2087-11a2b6c4c/)): Fix string embedding decision with keys (#2087): declined: no proposed test validated
- [valkey-io/valkey#2080](https://github.com/valkey-io/valkey/pull/2080) ([tests](tests/valkey/2080-c4f9b4635/)): Redundant Calculation of mstime() (#2080): declined: The change only hoists a redundant mstime() call out of a loop in clusterNodeAddFailureReport, storing the same timestamp value computed once instead of calling mstime() at each exit path. Control flow, return values, and failure-report semantics are identical; this is a performance/readab
- [valkey-io/valkey#2081](https://github.com/valkey-io/valkey/pull/2081) ([tests](tests/valkey/2081-41a93a2a7/)): Reverting the panic() introduced in #2027 (#2081): 2 of 4 test(s) reached the change, coverage 100%; crash, predates #2081, since fixed (recorded); crash, fixed by #2081 (recorded)
- [valkey-io/valkey#2014](https://github.com/valkey-io/valkey/pull/2014) ([tests](tests/valkey/2014-20a49ff01/)): Delete dead code in dict (#2014): declined: Pure dead-code deletion in dict.c/dict.h with no behavior change. The deleted entry variants (no_value, keys_are_odd, embedded_entry, no_incremental_rehash, embedKey, the ENTRY_PTR_* tagged pointers), dictTwoPhaseUnlinkFind/Free, dictGetStats*, and the defrag entry callbacks have zero user

### valkey-io/valkey-bloom

[2026-11-09T17:44:47+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42667499759): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-json

[2026-11-09T17:44:19+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42667417480): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-search

[2026-11-09T17:56:56+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42669380235): 0 merge(s) tested, 0 without a code change.


## Totals

- Merges whose tests reached the change: 682 of 1036 tested (66%)
- Changed-line coverage: median 100% of 584 merge(s) measured, 49 under 50%
- Recall: 190 parents flagged of 290 fix merges
- Claude: $1243.16 in the last 24 hours, $7517.39 in the last 7 days, $7517.39 in all
- Issues filed: 64
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

- [valkey-io/valkey#2146](https://github.com/valkey-io/valkey/pull/2146): attempt 2 of 3, Claude Code failed (rc=1): API Error: Claude's response exceeded the 64000 output token maximum. To configure this behavior, set the CLAUDE_CODE_MAX_OUTPUT_TOKENS environment variable.
