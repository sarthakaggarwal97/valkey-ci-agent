# Fuzzer walk

Updated 2026-11-13T09:39+00:00.

| Repository | Done | Remaining | Backlog cursor | Last 24 hours | Last 7 days (per day) |
|---|---|---|---|---|---|
| valkey-io/valkey | 2019 | 517 | #548 (2024-05-28) | 48 | 243.9 |
| valkey-io/valkey-bloom | 109 | 0 | ce516d0ef6e3 (2024-04-26) | 0 | 0.0 |
| valkey-io/valkey-json | 64 | 0 | c1423d7e4b38 (2024-11-25) | 0 | 0.3 |
| valkey-io/valkey-search | 681 | 0 | f8106e72e984 (2024-11-27) | 8 | 69.3 |

## Last runs

### valkey-io/valkey

[2026-11-13T09:39:56+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/43377333170): 20 merge(s) tested, 3 without a code change.

- [valkey-io/valkey#613](https://github.com/valkey-io/valkey/pull/613) ([tests](tests/valkey/613-d28ae5200/)): Remove redundant function `nextPingExt()` (#613): declined: The change deletes the duplicate function nextPingExt() and replaces its four call sites in writePingExt() with the pre-existing getNextPingExt(). Both functions compute the identical expression ((char *)ext + ntohl(ext->length)) — getNextPingExt just goes through the getPingExtLength() ac
- [valkey-io/valkey#586](https://github.com/valkey-io/valkey/pull/586) ([tests](tests/valkey/586-aad6769a8/)): Replicate slot migration states via RDB aux fields (#586): 0 of 8 test(s) reached the change, coverage 91%
- [valkey-io/valkey#591](https://github.com/valkey-io/valkey/pull/591) ([tests](tests/valkey/591-54c974793/)): Remove `master` and `slave` from source code (#591): 0 of 26 test(s) reached the change, coverage 55%; crash, predates #591, since fixed (recorded)
- [valkey-io/valkey#598](https://github.com/valkey-io/valkey/pull/598) ([tests](tests/valkey/598-bce240eab/)): Replace masteruser and masterauth with primaryuser and primaryauth (#598): 5 of 5 test(s) reached the change, coverage 100%; [check, introduced by #598](https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/147)
- [valkey-io/valkey#606](https://github.com/valkey-io/valkey/pull/606) ([tests](tests/valkey/606-ad5fd5b95/)): More rebranding (#606): 6 of 8 test(s) reached the change, coverage 76%; [crash, shown by ASan on unstable, the one build with it](https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/146); crash, introduced and since fixed (recorded); crash, fixed by #606 (recorded)
- [valkey-io/valkey#603](https://github.com/valkey-io/valkey/pull/603) ([tests](tests/valkey/603-278ce0cae/)): Rebrand the Lua debugger (#603): 2 of 2 test(s) reached the change, coverage 85%
- [valkey-io/valkey#600](https://github.com/valkey-io/valkey/pull/600) ([tests](tests/valkey/600-60c10a5a4/)): Remove valdup from BenchmarkDictType (#600): declined: The change removes a NULL valdup initializer from BenchmarkDictType in src/dict.c, a dict type used only by dictTest(), the internal './valkey-server test dict' benchmark harness. It is never used by the server at runtime, and the removed callback was NULL, so no command reply, replication
- [valkey-io/valkey#550](https://github.com/valkey-io/valkey/pull/550) ([tests](tests/valkey/550-9319f7aec/)): Replace valkey in log and panic messages (#550): declined: The change only rewrites the wording of two fatal diagnostic messages (a serverPanic for an unknown flag in the cluster config file, and the FATAL log when an RDB names a dbid >= server.dbnum) to use SERVER_TITLE instead of a hardcoded 'redis'/'Redis'. Control flow, conditions, format argu
- [valkey-io/valkey#443](https://github.com/valkey-io/valkey/pull/443) ([tests](tests/valkey/443-0700c441c/)): Remove unused valDup (#443): declined: The change removes the valDup callback field from dictType. valDup was NULL in every initializer in the tree, so dictSetVal's behavior is identical before and after (de->v.val = val in both cases); all positional and designated initializers were updated consistently, and no reference to va
- [valkey-io/valkey#502](https://github.com/valkey-io/valkey/pull/502): to retry, the job left no result
- [valkey-io/valkey#588](https://github.com/valkey-io/valkey/pull/588) ([tests](tests/valkey/588-30f277a86/)): Enable debug asserts for cluster and sentinel tests (#588): 2 of 4 test(s) reached the change, coverage 100%
- [valkey-io/valkey#250](https://github.com/valkey-io/valkey/pull/250) ([tests](tests/valkey/250-d16b4ec1b/)): Unshare object to avoid LRU/LFU being messed up (#250): 3 of 4 test(s) reached the change, coverage 100%
- [valkey-io/valkey#584](https://github.com/valkey-io/valkey/pull/584) ([tests](tests/valkey/584-2b97aa617/)): Introduce `enable-debug-assert` to enable/disable debug asserts at runtime (#584): 1 of 3 test(s) reached the change
- [valkey-io/valkey#562](https://github.com/valkey-io/valkey/pull/562) ([tests](tests/valkey/562-f927565d2/)): Consolidate various BLOCKED_WAIT* states (#562): 0 of 9 test(s) reached the change, coverage 70%
- [valkey-io/valkey#526](https://github.com/valkey-io/valkey/pull/526) ([tests](tests/valkey/526-6fb90adf4/)): Fix crash where command duration is not reset when client is blocked … (#526): 2 of 2 test(s) reached the change, coverage 100%; crash, fixed by #526 (recorded)
- [valkey-io/valkey#572](https://github.com/valkey-io/valkey/pull/572) ([tests](tests/valkey/572-0d2ba9b94/)): Update redis legacy word when run TLS cert file (#572): declined: The change only edits utils/gen-test-certs.sh, a helper script that generates self-signed certificates for Valkey's own test suite, replacing the certificate subject string "/O=Redis Test" with "/O=Valkey Test". No server, tool, or library code changes: no command replies, replication, per
- [valkey-io/valkey#564](https://github.com/valkey-io/valkey/pull/564) ([tests](tests/valkey/564-6bab2d796/)): Make sure clear the CLUSTER SLOTS cache on time when updating hostname (#564): 0 of 1 test(s) reached the change, coverage 100%
- [valkey-io/valkey#570](https://github.com/valkey-io/valkey/pull/570) ([tests](tests/valkey/570-168da8b52/)): Fix bitops.c clang-format properly (#570): declined: The change is a clang-format whitespace-only fix in bitcountCommand (src/bitops.c): it reflows an if statement onto one line, strips trailing whitespace, and adds spaces around operators. No code tokens, logic, or replies change, so there is no behaviour to test.
- [valkey-io/valkey#118](https://github.com/valkey-io/valkey/pull/118) ([tests](tests/valkey/118-96dcd1183/)): Change BITCOUNT 'end' as optional like BITPOS (#118): 2 of 2 test(s) reached the change, coverage 100%
- [valkey-io/valkey#258](https://github.com/valkey-io/valkey/pull/258) ([tests](tests/valkey/258-fd58b73f0/)): Introduce shared query buffer for client reads (#258): 2 of 6 test(s) reached the change, coverage 96%

### valkey-io/valkey-bloom

[2026-11-13T09:23:51+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/43400884079): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-json

[2026-11-13T09:23:43+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/43400872453): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-search

[2026-11-13T09:38:40+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/43402929930): 0 merge(s) tested, 0 without a code change.


## Totals

- Merges whose tests reached the change: 870 of 1392 tested (62%)
- Changed-line coverage: median 100% of 891 merge(s) measured, 66 under 50%
- Recall: 229 parents flagged of 363 fix merges
- Claude: $237.28 in the last 24 hours, $7667.92 in the last 7 days, $10184.37 in all
- Issues filed: 76
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

- [valkey-io/valkey#502](https://github.com/valkey-io/valkey/pull/502): attempt 0 of 3, the job left no result
