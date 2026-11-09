# Fuzzer walk

Updated 2026-11-09T08:34+00:00.

| Repository | Done | Remaining | Backlog cursor | Last 24 hours | Last 7 days (per day) |
|---|---|---|---|---|---|
| valkey-io/valkey | 1113 | 1394 | #2427 (2025-08-05) | 226 | 159.0 |
| valkey-io/valkey-bloom | 109 | 0 | ce516d0ef6e3 (2024-04-26) | 0 | 15.6 |
| valkey-io/valkey-json | 62 | 0 | c1423d7e4b38 (2024-11-25) | 0 | 8.9 |
| valkey-io/valkey-search | 669 | 0 | f8106e72e984 (2024-11-27) | 94 | 95.6 |

## Last runs

### valkey-io/valkey

[2026-11-09T07:43:29+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42561872310): 20 merge(s) tested, 9 without a code change.

- [valkey-io/valkey#2469](https://github.com/valkey-io/valkey/pull/2469) ([tests](tests/valkey/2469-725c3608f/)): Throw a useful error when underlying operations failed with tls (#2469): declined: The change only alters connTLSGetLastError in src/tls.c to return strerror(last_errno) instead of NULL when no SSL error string is stored. The function is the TLS connection type's get_last_error implementation, reachable only over TLS connections, and every caller uses the result solely t
- [valkey-io/valkey#2274](https://github.com/valkey-io/valkey/pull/2274) ([tests](tests/valkey/2274-a89e52993/)): Redact user data when hide-user-data-from-log enabled (#2274): declined: The change only alters the text of a serverPanic log message in moduleRDBLoadError: it substitutes '*redacted*' for the raw key name when hide-user-data-from-log is set. This is a log-redaction-only change with no effect on command replies, stored data, replication, or persistence. The fuz
- [valkey-io/valkey#2467](https://github.com/valkey-io/valkey/pull/2467) ([tests](tests/valkey/2467-ee305ce29/)): Replace generic C assert with serverAssert (#2467): declined: The change only replaces the <assert.h> include with serverassert.h in src/rdma.c and removes unused <assert.h> includes from two unit-test files. No assertion conditions, logic, or command behaviour change: the only difference is the diagnostic output if an assertion ever fails. Moreover 
- [valkey-io/valkey#2464](https://github.com/valkey-io/valkey/pull/2464) ([tests](tests/valkey/2464-ecca21331/)): Fix out-of-bound memory access when num-fields is not provided (#2464): 0 of 2 test(s) reached the change, coverage 100%; crash, fixed by #2464 (recorded)
- [valkey-io/valkey#2432](https://github.com/valkey-io/valkey/pull/2432) ([tests](tests/valkey/2432-d32f9be04/)): Use bool for return types in hashtable functions (#2432): declined: The change is a pure refactor: hashtable/kvstore functions that already returned exactly 0 or 1 now return C99 bool (false/true, which compare equal to 0/1), plus comment updates and de-nesting rewrites with identical control flow. The one deleted function, kvstoreHashtableAddOrFind, has n
- [valkey-io/valkey#2462](https://github.com/valkey-io/valkey/pull/2462) ([tests](tests/valkey/2462-bddc9db08/)): Fix HGETEX to return array response when the hash object is empty (#2462): 3 of 3 test(s) reached the change, coverage 100%
- [valkey-io/valkey#2454](https://github.com/valkey-io/valkey/pull/2454) ([tests](tests/valkey/2454-e606fcbaf/)): Fix AOF rewrite behavior for hashes with expirations (#2454): 1 of 2 test(s) reached the change, coverage 100%; crash, introduced and since fixed (recorded); crash, fixed by #2454 (recorded)
- [valkey-io/valkey#2452](https://github.com/valkey-io/valkey/pull/2452) ([tests](tests/valkey/2452-cc465383c/)): Adding backup directory check for valkey-cli --cluster backup (#2452): declined: no proposed test validated
- [valkey-io/valkey#2344](https://github.com/valkey-io/valkey/pull/2344) ([tests](tests/valkey/2344-2f1b25067/)): Remove unused includes (#2344): declined: The change only deletes unused #include lines in db.c, server.c and server.h. No executable code, command behavior, reply, replication, persistence or configuration path changes; the sole risk is a compilation failure, which the build itself catches before any fuzzer scenario could run. A 
- [valkey-io/valkey#2347](https://github.com/valkey-io/valkey/pull/2347) ([tests](tests/valkey/2347-fd8270a0a/)): [CRASH] Fix missing check for executing client (#2347): 0 of 4 test(s) reached the change, coverage 100%
- [valkey-io/valkey#2436](https://github.com/valkey-io/valkey/pull/2436) ([tests](tests/valkey/2436-ba7334c7c/)): Fix hfe no malloc size unit (#2436): declined: The diff only changes a unit-test mock in src/unit/test_vset.c and adds a build-time guard in src/zmalloc.h that affects which malloc-size macro is compiled on __APPLE__ when NO_MALLOC_USABLE_SIZE is defined (a special test build configuration). On the Linux CI platform the compiled code i
- [valkey-io/valkey#2419](https://github.com/valkey-io/valkey/pull/2419) ([tests](tests/valkey/2419-db11e41df/)): Add STALE command flag to SCRIPT-EXISTS, SCRIPT-SHOW and SCRIPT-FLUSH (#2419): 2 of 2 test(s) reached the change
- [valkey-io/valkey#2422](https://github.com/valkey-io/valkey/pull/2422) ([tests](tests/valkey/2422-33a43f2a8/)): bump the RDB version for Valkey 9.0(#2422): 2 of 4 test(s) reached the change, coverage 94%
- [valkey-io/valkey@5a5848a44cb0](https://github.com/valkey-io/valkey/commit/5a5848a44cb0c098e214a33135d5bfa3b313b1ec) ([tests](tests/valkey/5a5848a44/)): Add ACTIVE-EXPIRY and ACTIVE-DEFRAG for hash objects with volatile items: 9 of 14 test(s) reached the change, coverage 95%; [check, appeared after 5a5848a44cb0](https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/111); [crash, appeared after 5a5848a44cb0](https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/112)
- [valkey-io/valkey@c3b1b0317de9](https://github.com/valkey-io/valkey/commit/c3b1b0317de96a3fe2a1336a868c7740c6e7b93c) ([tests](tests/valkey/c3b1b0317/)): Introduce volatile set -------------: 2 of 20 test(s) reached the change, coverage 71%; [check, appeared after c3b1b0317de9](https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/111); [crash, appeared after c3b1b0317de9](https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/112); crash, introduced and since fixed (recorded)
- [valkey-io/valkey@65215e5378db](https://github.com/valkey-io/valkey/commit/65215e5378db7b47c3460567da134ab7f0bfeb58) ([tests](tests/valkey/65215e537/)): Introduce HASH items expiration: 5 of 8 test(s) reached the change, coverage 80%
- [valkey-io/valkey#2387](https://github.com/valkey-io/valkey/pull/2387) ([tests](tests/valkey/2387-3c738f08a/)): Remove the unused value duplicate API from dict of libvalkey (#2387): declined: The change deletes the unused valDup function pointer from libvalkey's dictType and simplifies dictSetVal accordingly. No dictType in the tree ever set valDup (grep finds no other reference), so dictSetVal already always executed de->val = val; the compiled behavior of the client library's
- [valkey-io/valkey#2418](https://github.com/valkey-io/valkey/pull/2418) ([tests](tests/valkey/2418-b7fe0b643/)): Strengthen undefined behavior prevention in checkSignedBitfieldOverflow (#2418): 0 of 2 test(s) reached the change, coverage 100%
- [valkey-io/valkey#2414](https://github.com/valkey-io/valkey/pull/2414) ([tests](tests/valkey/2414-52b5519c5/)): Use unsigned long for `maxiterations`, fixing undefined behavior in scan with extremely large count (#2414): 0 of 2 test(s) reached the change, coverage 100%
- [valkey-io/valkey#2394](https://github.com/valkey-io/valkey/pull/2394) ([tests](tests/valkey/2394-aac3c76c7/)): Fix prefetchNextBucketEntries so it fetches the correct next 2 buckets (#2394): declined: The change only corrects which buckets prefetchNextBucketEntries passes to __builtin_prefetch during hashtable iteration. Prefetch is a pure cache hint with no architectural effect, the looked-up buckets are bounds-checked and valid in both the old and new code, and the result never affect

### valkey-io/valkey-bloom

[2026-11-09T08:31:08+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42586448298): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-json

[2026-11-09T08:34:14+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42586836984): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-search

[2026-11-09T08:09:07+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42583782425): 0 merge(s) tested, 0 without a code change.


## Totals

- Merges whose tests reached the change: 639 of 949 tested (67%)
- Changed-line coverage: median 100% of 504 merge(s) measured, 38 under 50%
- Recall: 176 parents flagged of 267 fix merges
- Claude: $971.36 in the last 24 hours, $6902.13 in the last 7 days, $6902.13 in all
- Issues filed: 63
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
