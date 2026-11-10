# Fuzzer walk

Updated 2026-11-10T04:03+00:00.

| Repository | Done | Remaining | Backlog cursor | Last 24 hours | Last 7 days (per day) |
|---|---|---|---|---|---|
| valkey-io/valkey | 1396 | 1119 | #1785 (2025-02-26) | 341 | 199.4 |
| valkey-io/valkey-bloom | 109 | 0 | ce516d0ef6e3 (2024-04-26) | 0 | 15.6 |
| valkey-io/valkey-json | 63 | 0 | c1423d7e4b38 (2024-11-25) | 1 | 9.0 |
| valkey-io/valkey-search | 671 | 0 | f8106e72e984 (2024-11-27) | 2 | 95.9 |

## Last runs

### valkey-io/valkey

[2026-11-10T03:21:30+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42740535273): 20 merge(s) tested, 7 without a code change.

- [valkey-io/valkey#4893](https://github.com/valkey-io/valkey/pull/4893) ([tests](tests/valkey/4893-7c053b303/)): Strip every repeated LIMIT when rewriting XADD/XTRIM for propagation (#4893): 0 of 4 test(s) reached the change, coverage 100%
- [valkey-io/valkey#1842](https://github.com/valkey-io/valkey/pull/1842) ([tests](tests/valkey/1842-1a975faf7/)): Fix ACL LOAD crash on replica since the primary client don't has a user (#1842): 2 of 2 test(s) reached the change, coverage 100%; crash, fixed by #1842 (recorded)
- [valkey-io/valkey#1813](https://github.com/valkey-io/valkey/pull/1813) ([tests](tests/valkey/1813-bcd2f952d/)): Save config file and brocast the PONG when configEpoch changed (#1813): 3 of 6 test(s) reached the change, coverage 100%
- [valkey-io/valkey#1834](https://github.com/valkey-io/valkey/pull/1834) ([tests](tests/valkey/1834-fc199e0dc/)): Update CloseKey module API documentation to avoid use-after-free behavior (#1834): declined: The change modifies only a doc comment on VM_CloseKey in src/module.c (clarifying that the key handle is freed and must not be reused). No code path, reply, or behavior changes, so no scenario can distinguish the change from its parent and none is needed.
- [valkey-io/valkey#1829](https://github.com/valkey-io/valkey/pull/1829) ([tests](tests/valkey/1829-f8bc378de/)): [CMake] Check both arm64 and aarch64 for ARM based system architecture (#1829): declined: The change is a build-system-only fix: cmake/Modules/ValkeySetup.cmake now sets BUILDING_ARM64 when CMAKE_SYSTEM_PROCESSOR is 'aarch64' as well as 'arm64'. It modifies no runtime source code, commands, replication, persistence, or protocol behaviour — it only corrects architecture detectio
- [valkey-io/valkey#1823](https://github.com/valkey-io/valkey/pull/1823) ([tests](tests/valkey/1823-8221a1556/)): Fix bug where invalidation messages were getting sent to closing clients (#1823): 0 of 2 test(s) reached the change, coverage 100%
- [valkey-io/valkey#1798](https://github.com/valkey-io/valkey/pull/1798) ([tests](tests/valkey/1798-bb944c786/)): Adds a memory leak check after running a unit test (#1798): declined: The change only modifies src/unit/test_main.c, the standalone unit-test harness (valkey-unit-tests): after each test suite runs, it checks zmalloc_used_memory() and reports the suite as failed if memory was not freed. This code is compiled only into the unit-test binary and never into valk
- [valkey-io/valkey#1802](https://github.com/valkey-io/valkey/pull/1802) ([tests](tests/valkey/1802-fe4241586/)): Use the wrapper from cli_common instead of hiredis (#1802): 0 of 2 test(s) reached the change, coverage 92%
- [valkey-io/valkey#1800](https://github.com/valkey-io/valkey/pull/1800) ([tests](tests/valkey/1800-cab500cf6/)): Fix incorrect assertion in client list operations (#1800): 0 of 4 test(s) reached the change, coverage 100%
- [valkey-io/valkey#1812](https://github.com/valkey-io/valkey/pull/1812) ([tests](tests/valkey/1812-7a2d50a15/)): Drop lua object files on clean (#1812): declined: The change only edits the `clean` target in src/Makefile so that `make clean` also removes lua/*.o and lua/*.d object files. It is a build-system change with no effect on the compiled server, its commands, replication, persistence, or any runtime behaviour, so there is nothing a fuzzer sce
- [valkey-io/valkey#1756](https://github.com/valkey-io/valkey/pull/1756) ([tests](tests/valkey/1756-0cc0bf722/)): make net_input_bytes_curr_cmd more readable (#1756): declined: The change is a pure readability refactor of how net_input_bytes_curr_cmd is accumulated in processMultibulkBuffer, plus comment updates. The old total (multibulklen_slen+1) + Σ bulklen_slen + (argv_len_sum + argc*5 + 2) and the new total (multibulklen_slen+3) + Σ (bulklen_slen+3) + (argv_
- [valkey-io/valkey#1806](https://github.com/valkey-io/valkey/pull/1806) ([tests](tests/valkey/1806-f31cf189c/)): Fixed build error with CMake (#1806): declined: The change only adds an explicit cast ((long double)LLONG_MAX) in getTimeoutFromObjectOrReply to fix a CMake build error. Under C's usual arithmetic conversions the long long operand was already implicitly converted to long double in the comparison, so the generated semantics are identical
- [valkey-io/valkey#1799](https://github.com/valkey-io/valkey/pull/1799) ([tests](tests/valkey/1799-6156590fa/)): Fix hashTypeEntryDefrag returning bad pointer (#1799): 0 of 2 test(s) reached the change, coverage 100%; check, introduced and since fixed (recorded); crash, introduced and since fixed (recorded); crash, fixed by #1799 (recorded)
- [valkey-io/valkey#1690](https://github.com/valkey-io/valkey/pull/1690) ([tests](tests/valkey/1690-e03b3f1fc/)): Add cluster-manual-failover-timeout to configure the timeout for manual failover (#1690): 1 of 3 test(s) reached the change, coverage 100%
- [valkey-io/valkey#1797](https://github.com/valkey-io/valkey/pull/1797) ([tests](tests/valkey/1797-677d5755f/)): Fix memory leak in test_quicklist.c unit test (#1797): declined: The change only adds a zfree() in the unit test file src/unit/test_quicklist.c to fix a memory leak in the test harness itself. No server source code, command behaviour, replication, persistence or tooling is touched, so no fuzzer scenario can reach or exercise the change; it is covered by
- [valkey-io/valkey#1788](https://github.com/valkey-io/valkey/pull/1788) ([tests](tests/valkey/1788-089b83047/)): Consistent look and feel of licenses (#1788): declined: The change (PR #1788, 'Consistent look and feel of licenses') only rewrites license header comments in source files and the COPYING text file: it adds/normalizes SPDX-License-Identifier lines and copyright comment blocks. Every added or removed line in the diff is comment text or whitespac
- [valkey-io/valkey#1794](https://github.com/valkey-io/valkey/pull/1794) ([tests](tests/valkey/1794-e23a7d6f3/)): Fix clang build error in bitops.c (#1794): declined: The change only defines __MM_MALLOC_H before including immintrin.h in bitops.c to fix a clang compile error; it prevents importing unused aligned-allocation declarations and changes no runtime behaviour, reply, or data path. A build-only fix needs no scenario.
- [valkey-io/valkey#1686](https://github.com/valkey-io/valkey/pull/1686) ([tests](tests/valkey/1686-b3e5e8a91/)): Add Cluster Bus Port out of range error message for Cluster Meet command (#1686): 2 of 3 test(s) reached the change, coverage 100%
- [valkey-io/valkey#1760](https://github.com/valkey-io/valkey/pull/1760) ([tests](tests/valkey/1760-49663575c/)): cmd's out bytes need count deferred reply (#1760): 3 of 3 test(s) reached the change, coverage 100%
- [valkey-io/valkey#1782](https://github.com/valkey-io/valkey/pull/1782) ([tests](tests/valkey/1782-587a6fce1/)): valkey-cli: ensure output ends with a newline if missing when printing reply (#1782): 1 of 1 test(s) reached the change, coverage 100%

### valkey-io/valkey-bloom

[2026-11-10T04:03:18+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42753292881): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-json

[2026-11-10T03:33:04+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42750102773): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-search

[2026-11-10T03:40:30+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42750883751): 0 merge(s) tested, 0 without a code change.


## Totals

- Merges whose tests reached the change: 716 of 1095 tested (65%)
- Changed-line coverage: median 100% of 637 merge(s) measured, 53 under 50%
- Recall: 197 parents flagged of 303 fix merges
- Claude: $1283.95 in the last 24 hours, $7990.06 in the last 7 days, $7990.06 in all
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

- [valkey-io/valkey#1948](https://github.com/valkey-io/valkey/pull/1948): attempt 0 of 3, Claude Code failed (rc=1): API Error: Claude Code is unable to respond to this request, which appears to violate our Usage Policy (https://www.anthropic.com/legal/aup). This request triggered restrict
