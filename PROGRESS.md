# Fuzzer walk

Updated 2026-11-09T17:44+00:00.

| Repository | Done | Remaining | Backlog cursor | Last 24 hours | Last 7 days (per day) |
|---|---|---|---|---|---|
| valkey-io/valkey | 1252 | 1255 | #2099 (2025-05-23) | 286 | 178.9 |
| valkey-io/valkey-bloom | 109 | 0 | ce516d0ef6e3 (2024-04-26) | 0 | 15.6 |
| valkey-io/valkey-json | 63 | 0 | c1423d7e4b38 (2024-11-25) | 1 | 9.0 |
| valkey-io/valkey-search | 669 | 0 | f8106e72e984 (2024-11-27) | 34 | 95.6 |

## Last runs

### valkey-io/valkey

[2026-11-09T17:22:07+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42649336725): 20 merge(s) tested, 1 without a code change.

- [valkey-io/valkey#2146](https://github.com/valkey-io/valkey/pull/2146): to retry, Claude Code failed (rc=1): API Error: Claude's response exceeded the 64000 output token maximum. To configure this behavior, set the CLAUDE_CODE_MAX_OUTPUT_TOKENS environment variable.
- [valkey-io/valkey#2033](https://github.com/valkey-io/valkey/pull/2033) ([tests](tests/valkey/2033-4aaaf8833/)): Allow dynamic modification of io-threads num (#2033): 2 of 6 test(s) reached the change, coverage 96%; crash, introduced and since fixed (recorded)
- [valkey-io/valkey#2144](https://github.com/valkey-io/valkey/pull/2144) ([tests](tests/valkey/2144-30d7f08a4/)): Correctly cast the extension lengths (#2144): declined: Correct one-token fix (uint16_t -> uint32_t) for truncating a cluster-bus ping-extension length. It is a genuine remote-input bug, but it only manifests when a received PING/PONG/MEET extension is >= 65536 bytes, and the fuzzer cannot deliver that: there is no cluster-bus packet injector, 
- [valkey-io/valkey#1838](https://github.com/valkey-io/valkey/pull/1838) ([tests](tests/valkey/1838-f5b92f526/)): Prioritize replication traffic in the replica (#1838): 0 of 3 test(s) reached the change, coverage 100%
- [valkey-io/valkey#1943](https://github.com/valkey-io/valkey/pull/1943) ([tests](tests/valkey/1943-b4dbcef90/)): Improve clarity of errors for GEO commands when member does not exist (#1943): 2 of 2 test(s) reached the change, coverage 80%
- [valkey-io/valkey#2142](https://github.com/valkey-io/valkey/pull/2142) ([tests](tests/valkey/2142-ff7135836/)): Update GEOSEARCH and GEOSEARCHSTORE history documentation for the BYPOLYGON option (#2142): 2 of 2 test(s) reached the change
- [valkey-io/valkey#2115](https://github.com/valkey-io/valkey/pull/2115) ([tests](tests/valkey/2115-872ee0d6f/)): Add size_t cast in modules (#2115): declined: The change only adds (size_t) casts to the vector-length argument of ValkeyModule_Call in the test modules under tests/modules/ (aclcheck, blockedclient, commandfilter, misc, usercall). module.c reads that variadic argument as size_t ('v' format in moduleCreateArgvFromUserFormat), so the c
- [valkey-io/valkey#2012](https://github.com/valkey-io/valkey/pull/2012) ([tests](tests/valkey/2012-689a3b158/)): Flip pfail flag while marking node as failed (#2012): declined: The change adds one line that clears the stray CLUSTER_NODE_PFAIL flag when a node is marked NOADDR+FAIL after a node-ID mismatch (a peer that did CLUSTER RESET), restoring the PFAIL/FAIL mutual-exclusion invariant. It has no functional effect: the NOADDR flag already excludes this node fr
- [valkey-io/valkey#2140](https://github.com/valkey-io/valkey/pull/2140) ([tests](tests/valkey/2140-17e66863a/)): Detect SSL_new() returning NULL in outgoing connections (#2140): declined: The change only alters behavior when OpenSSL's SSL_new() returns NULL inside createTLSConnection, i.e. under OpenSSL-internal allocation failure on a TLS-enabled server; when SSL_new() succeeds the code is behaviorally identical to the parent on both the accepted and outgoing paths. The fu
- [valkey-io/valkey#1809](https://github.com/valkey-io/valkey/pull/1809) ([tests](tests/valkey/1809-cf5c5f570/)): Support BYPOLYGON option for GEOSEARCH (#1809): 3 of 3 test(s) reached the change, coverage 87%
- [valkey-io/valkey#2132](https://github.com/valkey-io/valkey/pull/2132) ([tests](tests/valkey/2132-258213ff7/)): Free module context even if there was no content written in auxsave2 (#2132): declined: The change only frees a lazily-allocated ValkeyModuleCtx on the early-return path of rdbSaveSingleModuleAux when a module's aux_save2 callback wrote nothing — a pure memory-leak fix with no observable behaviour change (no reply, RDB content, replication or persistence difference). Triggeri
- [valkey-io/valkey#2108](https://github.com/valkey-io/valkey/pull/2108) ([tests](tests/valkey/2108-3ceae81fc/)): Fix a case of out of bound read when cluster node ID is provided with wrong length in CLUSTER FORGET (#2108): 0 of 5 test(s) reached the change, coverage 100%
- [valkey-io/valkey#2101](https://github.com/valkey-io/valkey/pull/2101) ([tests](tests/valkey/2101-374718b2a/)): Fix unsigned difference expression compared to zero (#2101): declined: The change rewrites `prev->size - prev->used > 0` as `prev->used < prev->size` in setDeferredReply (src/networking.c). Both fields are size_t with the invariant used <= size, so the two conditions are equivalent for every reachable state; the rewrite only silences a static-analysis 'unsign
- [valkey-io/valkey#2131](https://github.com/valkey-io/valkey/pull/2131) ([tests](tests/valkey/2131-0b11d347a/)): Correct VM_Yield timing and deflake blockedclient.tcl (#2131): 0 of 6 test(s) reached the change, coverage 0%
- [valkey-io/valkey#2109](https://github.com/valkey-io/valkey/pull/2109) ([tests](tests/valkey/2109-d00fb8e71/)): Only mark the client reprocessing flag when unblocked on keys (#2109): 0 of 3 test(s) reached the change, coverage 25%
- [valkey-io/valkey#2116](https://github.com/valkey-io/valkey/pull/2116) ([tests](tests/valkey/2116-729ca117c/)):  Optimize GEORADIUS command performance with  pre-allocated buffer (#2116): 0 of 3 test(s) reached the change, coverage 100%
- [valkey-io/valkey#2133](https://github.com/valkey-io/valkey/pull/2133) ([tests](tests/valkey/2133-9dd5a02ce/)): Fix the GNU IFUNC error in alpine (non-glibc) environment (#2133): declined: The change only adds a __GLIBC__ guard to the HAVE_IFUNC preprocessor condition in src/config.h, fixing a compile/link error of the GNU ifunc attribute on musl (Alpine). On glibc Linux, where the fuzzer builds and runs, the condition evaluates identically before and after the change, so pa
- [valkey-io/valkey#2137](https://github.com/valkey-io/valkey/pull/2137) ([tests](tests/valkey/2137-bd5dcb281/)): Fix bad slot used in sharded pubsub unsubscribe (#2137): 2 of 2 test(s) reached the change, coverage 100%; crash, fixed by #2137 (recorded)
- [valkey-io/valkey#2120](https://github.com/valkey-io/valkey/pull/2120) ([tests](tests/valkey/2120-28f48ff63/)): Changes and fixes for the new DELIFEQ command (#2120): 3 of 4 test(s) reached the change, coverage 100%
- [valkey-io/valkey#2130](https://github.com/valkey-io/valkey/pull/2130) ([tests](tests/valkey/2130-053bf9e5b/)): Mark string2ll_resolver as used (#2130): declined: The change only adds the `used` attribute to the static ifunc resolver string2ll_resolver in src/util.c, preventing compilers from garbage-collecting the symbol and breaking the link. It is a build fix: the resolver still selects the same string2llAVX512/string2llScalar implementations wit

### valkey-io/valkey-bloom

[2026-11-09T17:44:47+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42667499759): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-json

[2026-11-09T17:44:19+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42667417480): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-search

[2026-11-09T17:25:50+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42664398526): 0 merge(s) tested, 0 without a code change.


## Totals

- Merges whose tests reached the change: 677 of 1028 tested (66%)
- Changed-line coverage: median 100% of 578 merge(s) measured, 48 under 50%
- Recall: 189 parents flagged of 289 fix merges
- Claude: $1162.73 in the last 24 hours, $7436.95 in the last 7 days, $7436.95 in all
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

- [valkey-io/valkey#2146](https://github.com/valkey-io/valkey/pull/2146): attempt 1 of 3, Claude Code failed (rc=1): API Error: Claude's response exceeded the 64000 output token maximum. To configure this behavior, set the CLAUDE_CODE_MAX_OUTPUT_TOKENS environment variable.
