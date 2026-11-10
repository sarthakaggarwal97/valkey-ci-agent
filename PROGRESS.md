# Fuzzer walk

Updated 2026-11-10T05:44+00:00.

| Repository | Done | Remaining | Backlog cursor | Last 24 hours | Last 7 days (per day) |
|---|---|---|---|---|---|
| valkey-io/valkey | 1421 | 1095 | #1730 (2025-02-13) | 337 | 203.0 |
| valkey-io/valkey-bloom | 109 | 0 | ce516d0ef6e3 (2024-04-26) | 0 | 15.6 |
| valkey-io/valkey-json | 63 | 0 | c1423d7e4b38 (2024-11-25) | 1 | 9.0 |
| valkey-io/valkey-search | 672 | 0 | f8106e72e984 (2024-11-27) | 3 | 96.0 |

## Last runs

### valkey-io/valkey

[2026-11-10T04:49:52+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42748918829): 20 merge(s) tested, 5 without a code change.

- [valkey-io/valkey#5045](https://github.com/valkey-io/valkey/pull/5045) ([tests](tests/valkey/5045-2d84f93bb/)): fbtree: count leaves exactly and validate the leaf chain (#5045): 10 of 15 test(s) reached the change, coverage 46%
- [valkey-io/valkey#1785](https://github.com/valkey-io/valkey/pull/1785) ([tests](tests/valkey/1785-1ad0c4fea/)): Remove unicode optimization in Lua cjson library (#1785): 0 of 2 test(s) reached the change
- [valkey-io/valkey#1786](https://github.com/valkey-io/valkey/pull/1786) ([tests](tests/valkey/1786-7db01d88c/)): Fix undefined behaviour in bitops unit test (#1786): declined: The change modifies only the unit test file src/unit/test_bitops.c, replacing a zero-length VLA (undefined behaviour in the test harness itself) with a minimum size of 1. No server code, command behaviour, replies, replication, or persistence paths change, so no runtime scenario can exerci
- [valkey-io/valkey#1741](https://github.com/valkey-io/valkey/pull/1741) ([tests](tests/valkey/1741-79d504742/)): Optimize bitcount command by SIMD (#1741): 0 of 2 test(s) reached the change, coverage 100%
- [valkey-io/valkey#1721](https://github.com/valkey-io/valkey/pull/1721) ([tests](tests/valkey/1721-b360f96a7/)): Fix temp file leak druing replication error handling (#1721): 0 of 4 test(s) reached the change, coverage 87%
- [valkey-io/valkey#1779](https://github.com/valkey-io/valkey/pull/1779) ([tests](tests/valkey/1779-221ff441d/)): defrag: remove assert on defrag_later (#1779): 0 of 1 test(s) reached the change
- [valkey-io/valkey#1763](https://github.com/valkey-io/valkey/pull/1763) ([tests](tests/valkey/1763-aea3eadab/)): Enable TCP_NODELAY by default in incoming and outgoing connections (#1763): 0 of 12 test(s) reached the change, coverage 66%
- [valkey-io/valkey#1579](https://github.com/valkey-io/valkey/pull/1579) ([tests](tests/valkey/1579-421b2a40e/)): Embed hash value in hash type entry (#1579): 3 of 5 test(s) reached the change, coverage 87%
- [valkey-io/valkey#1773](https://github.com/valkey-io/valkey/pull/1773) ([tests](tests/valkey/1773-ee8465ce3/)): Fixed active-expire-effort description in conf file (#1773): declined: The change only edits a descriptive comment in valkey.conf (fixing the wording of the active-expire-effort description). Comments in the conf file are ignored by the config parser, so no server behaviour, replies, persistence, or replication paths are affected; there is nothing a scenario 
- [valkey-io/valkey#1748](https://github.com/valkey-io/valkey/pull/1748) ([tests](tests/valkey/1748-be9d9487b/)): Fix murmur32 on large strings (#1748): declined: The change fixes a 32-bit int overflow in murmur32 (deps/lua/src/lstring.c), the hash used only by Lua's internal string-interning table (luaS_newlstr is its sole caller). The overflow only triggers for strings larger than ~8.59 GB, and even then it merely yields a wrong hash with no out-o
- [valkey-io/valkey#1737](https://github.com/valkey-io/valkey/pull/1737) ([tests](tests/valkey/1737-3a64260e4/)): Fix error "SSL routines::bad length" when connTLSWrite is called second time with smaller buffer (#1737): declined: The change is confined to src/tls.c (connTLSWrite/connTLSWritev): it only executes on a TLS connection, which requires a BUILD_TLS build, a tls-port with certificates, and a TLS client or TLS replication link. The fuzzer cannot produce any of these: tls-port is on the provider's rejected c
- [valkey-io/valkey#1706](https://github.com/valkey-io/valkey/pull/1706) ([tests](tests/valkey/1706-b7116d4a7/)): Move TCP/TLS specific options from generic client to connection type (#1706): 0 of 4 test(s) reached the change, coverage 100%
- [valkey-io/valkey#1357](https://github.com/valkey-io/valkey/pull/1357) ([tests](tests/valkey/1357-54c4bbcec/)): Add new module API flag to bypass command validation (#1357): 0 of 6 test(s) reached the change, coverage 30%
- [valkey-io/valkey#1751](https://github.com/valkey-io/valkey/pull/1751) ([tests](tests/valkey/1751-f3361cc03/)): Allow to specify the random number generator seed in unit tests (#1751): declined: The change only adds a --seed flag to the standalone unit-test binary (src/unit/test_main.c) and refactors src/util.c's getRandomBytes to expose its seed via new setRandomSeedCString/getRandomSeedCString helpers used exclusively by the test harness. The refactored getRandomBytes is behavio
- [valkey-io/valkey#1708](https://github.com/valkey-io/valkey/pull/1708) ([tests](tests/valkey/1708-206f7f60e/)): Pass null-terminated node ID for `VM_RegisterClusterMessageReceiver` and add test coverage (#1708): 2 of 2 test(s) reached the change, coverage 100%
- [valkey-io/valkey#1750](https://github.com/valkey-io/valkey/pull/1750) ([tests](tests/valkey/1750-cf19deb61/)): Fix hashtable memory leak and kvstore `overhead_hashtable_lut` assert (#1750): 0 of 5 test(s) reached the change, coverage 100%
- [valkey-io/valkey#1745](https://github.com/valkey-io/valkey/pull/1745) ([tests](tests/valkey/1745-f85c93301/)): Comment out assert in kvstore for overhead lut (#1745): declined: The change only comments out a debug consistency assert in kvstoreRelease(); no code path, reply, data structure, or persistence/replication behaviour changes. The only state in which the two builds would differ is one where the overhead_hashtable_lut accounting is nonzero at release time,
- [valkey-io/valkey#1722](https://github.com/valkey-io/valkey/pull/1722) ([tests](tests/valkey/1722-d588bb3be/)): Fix raxRemove crash at memcpy() due to key size exceeds max Rax size (#1722): declined: The fix adds a size-limit guard (comprsize + h->size > RAX_NODE_MAX_SIZE, ~512MB) to the recompression populate-loop in raxRemove(), preventing a heap-overflow memcpy that crashes only when a single rax key spans a compressible chain exceeding RAX_NODE_MAX_SIZE. All command-reachable rax t
- [valkey-io/valkey#1733](https://github.com/valkey-io/valkey/pull/1733) ([tests](tests/valkey/1733-462cba580/)): Modify parameter of clientMatchesFilter function. (#1733): declined: The change converts clientMatchesFilter's clientFilter parameter from pass-by-value to pass-by-pointer and updates its two call sites (getAllFilteredClientsInfoString and clientKillCommand) accordingly. The function only reads the filter's fields and never mutates them, and the callers pas
- [valkey-io/valkey#994](https://github.com/valkey-io/valkey/pull/994) ([tests](tests/valkey/994-99ec47a78/)): Simplify isNodeAvailable function and add comments (#994): declined: Pure refactor with no behavior change: isNodeAvailable previously computed node->repl_offset via clusterNodeReplOffset and replaced it with getNodeReplicationOffset(node) only for the myself node; getNodeReplicationOffset already performs exactly that branching internally (live offset for 

### valkey-io/valkey-bloom

[2026-11-10T05:35:21+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42763001114): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-json

[2026-11-10T05:36:02+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42763074155): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-search

[2026-11-10T05:44:00+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42763865652): 0 merge(s) tested, 1 without a code change.


## Totals

- Merges whose tests reached the change: 719 of 1106 tested (65%)
- Changed-line coverage: median 100% of 646 merge(s) measured, 55 under 50%
- Recall: 197 parents flagged of 305 fix merges
- Claude: $1264.91 in the last 24 hours, $8079.80 in the last 7 days, $8079.80 in all
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
