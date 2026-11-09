# Fuzzer walk

Updated 2026-11-09T22:25+00:00.

| Repository | Done | Remaining | Backlog cursor | Last 24 hours | Last 7 days (per day) |
|---|---|---|---|---|---|
| valkey-io/valkey | 1299 | 1211 | #1995 (2025-05-03) | 302 | 185.6 |
| valkey-io/valkey-bloom | 109 | 0 | ce516d0ef6e3 (2024-04-26) | 0 | 15.6 |
| valkey-io/valkey-json | 63 | 0 | c1423d7e4b38 (2024-11-25) | 1 | 9.0 |
| valkey-io/valkey-search | 669 | 0 | f8106e72e984 (2024-11-27) | 14 | 95.6 |

## Last runs

### valkey-io/valkey

[2026-11-09T20:36:46+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42679435884): 20 merge(s) tested, 4 without a code change.

- [valkey-io/valkey#5326](https://github.com/valkey-io/valkey/pull/5326) ([tests](tests/valkey/5326-dfdea1799/)): bgiteration: process ITER_CLOSED after items queued behind it (#5326): 0 of 4 test(s) reached the change, coverage 100%; crash, introduced and since fixed (recorded)
- [valkey-io/valkey#2146](https://github.com/valkey-io/valkey/pull/2146): Incorporate Redis CVE for CVE-2025-27151 (#2146): error: Claude Code failed (rc=1): API Error: Claude's response exceeded the 64000 output token maximum. To configure this behavior, set the CLAUDE_CODE_MAX_OUTPUT_TOKENS environment variable.
- [valkey-io/valkey#2027](https://github.com/valkey-io/valkey/pull/2027) ([tests](tests/valkey/2027-9f7aa040d/)): Additional assertions for listpack (#2027): declined: Defensive hardening of listpack internals: it adds assertions that an LP_EOF byte lies at the end of the allocation, makes lpValidateNext reject a mid-listpack LP_EOF, and makes lpGetWithSize panic on an invalid encoding byte instead of returning a sentinel integer. On every valid listpack
- [valkey-io/valkey#2063](https://github.com/valkey-io/valkey/pull/2063) ([tests](tests/valkey/2063-b7f4b20bc/)): Make sure that the same point reports for geo comparisons (#2063): 2 of 2 test(s) reached the change, coverage 100%
- [valkey-io/valkey#2057](https://github.com/valkey-io/valkey/pull/2057) ([tests](tests/valkey/2057-fe97ba419/)): valkey-benchmark: Arbitrary command sequence (#2057): 4 of 4 test(s) reached the change, coverage 92%
- [valkey-io/valkey#2071](https://github.com/valkey-io/valkey/pull/2071) ([tests](tests/valkey/2071-4ede540e9/)): Fix variable scope confusion (#2071): declined: The change only removes a local 'size_t len' that shadowed the outer 'uint64_t len' loop counter in rdbLoadObject's RDB_TYPE_LIST branch, inlining sdslen(dec->ptr) into the quicklistPushTail call. The while(len--) condition always referred to the outer counter (the inner declaration is sco
- [valkey-io/valkey#1867](https://github.com/valkey-io/valkey/pull/1867) ([tests](tests/valkey/1867-af25a5247/)): Optimize BITCOUNT using ARM NEON SIMD (#1867): 0 of 2 test(s) reached the change
- [valkey-io/valkey#2068](https://github.com/valkey-io/valkey/pull/2068) ([tests](tests/valkey/2068-6cf69a44b/)): valkey-cli: remove RESP version assertion from getDatabases() (#2068): declined: The change only deletes a defensive assertion in valkey-cli's getDatabases() that required the CONFIG GET reply type to match config.current_resp3. That tracked value can only disagree with the server's actual reply type through the cliSwitchProto() desync (implicit --json RESP3 request pl
- [valkey-io/valkey#2060](https://github.com/valkey-io/valkey/pull/2060) ([tests](tests/valkey/2060-2205bf99c/)): move statement in if condition for zremrangeGenericCommand function (#2060): declined: The change moves `server.dirty += deleted` inside the `if (deleted)` block in zremrangeGenericCommand (ZREMRANGEBYRANK/BYSCORE/BYLEX). When deleted != 0 both versions add the same amount; when deleted == 0 the old code added zero, a no-op. server.dirty, replies, keyspace notifications and 
- [valkey-io/valkey#2067](https://github.com/valkey-io/valkey/pull/2067) ([tests](tests/valkey/2067-0862f9f77/)): Support MPTCP for valkey-cli and benchmark (#2067): 3 of 3 test(s) reached the change, coverage 90%
- [valkey-io/valkey#2059](https://github.com/valkey-io/valkey/pull/2059) ([tests](tests/valkey/2059-70106f06b/)): Support RDMA for valkey-cli and benchmark (#2059): 2 of 7 test(s) reached the change, coverage 71%
- [valkey-io/valkey#2030](https://github.com/valkey-io/valkey/pull/2030) ([tests](tests/valkey/2030-22c219124/)): Optimize findBucket performance using SIMD (#2030): 0 of 5 test(s) reached the change, coverage 100%
- [valkey-io/valkey#2061](https://github.com/valkey-io/valkey/pull/2061) ([tests](tests/valkey/2061-a2001a387/)): Fixed minor grammatical error (#2061): declined: The change only rewords a sentence in a source comment in src/bio.c ('put here server specific background tasks' -> 'put server specific background tasks here'). No executable code, configuration, protocol, or build artifact changes, so no runtime behaviour can differ and no scenario is ne
- [valkey-io/valkey#1966](https://github.com/valkey-io/valkey/pull/1966) ([tests](tests/valkey/1966-9e87473b3/)): Disallowing Client Reply is On / Off / Skip when Client is Multi (#1966): 2 of 2 test(s) reached the change, coverage 100%
- [valkey-io/valkey#2032](https://github.com/valkey-io/valkey/pull/2032) ([tests](tests/valkey/2032-b5c774397/)): Replace dependency `hiredis` with `libvalkey` (#2032): 0 of 3 test(s) reached the change
- [valkey-io/valkey#2046](https://github.com/valkey-io/valkey/pull/2046) ([tests](tests/valkey/2046-7859ba9b1/)): valkey-cli: Count databases correctly in --cluster subcommands (#2046): 1 of 1 test(s) reached the change, coverage 100%
- [valkey-io/valkey#2048](https://github.com/valkey-io/valkey/pull/2048) ([tests](tests/valkey/2048-c17af4ac4/)): Return only the relevant bit in sdsGetAuxBit() (#2048): declined: The change masks sdsGetAuxBit()'s return value to a single bit (`& 1U`), but it is purely defensive hardening with no observable behaviour change: the only caller is the hash entry encoding in t_hash.c, which uses only aux bit 0, and the embedded field's flags byte is always written by sds
- [valkey-io/valkey#2024](https://github.com/valkey-io/valkey/pull/2024) ([tests](tests/valkey/2024-911641b29/)): Remove duplicate macro definitions of HASH_SET_* (#2024): declined: The change only deletes duplicate #define lines for HASH_SET_TAKE_FIELD, HASH_SET_TAKE_VALUE and HASH_SET_COPY from t_hash.c; identical definitions with the same values remain in server.h (lines 3268-3270), so preprocessing, hashTypeSet and all hash commands compile to exactly the same cod
- [valkey-io/valkey#2038](https://github.com/valkey-io/valkey/pull/2038) ([tests](tests/valkey/2038-4571886b4/)): Fix VALKEYCLI_AUTH env variable fallback to REDISCLI_AUTH mistake (#2038): 1 of 1 test(s) reached the change, coverage 100%
- [valkey-io/valkey#2025](https://github.com/valkey-io/valkey/pull/2025) ([tests](tests/valkey/2025-91f6c9737/)): Compile with -Wundef (#2025): declined: The change only adds the -Wundef/-Werror=undef compiler flag and rewrites '#if MACRO' conditionals to '#if defined(MACRO) && MACRO'. Since an undefined macro already evaluates to 0 in a preprocessor #if, every rewritten conditional (config.h, crcspeed.c, debug.c, rdma.c, rio.c, sentinel.c,

### valkey-io/valkey-bloom

[2026-11-09T22:25:15+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42711625631): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-json

[2026-11-09T21:54:42+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42706827311): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-search

[2026-11-09T20:53:20+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42697097484): 0 merge(s) tested, 0 without a code change.


## Totals

- Merges whose tests reached the change: 689 of 1047 tested (66%)
- Changed-line coverage: median 100% of 593 merge(s) measured, 49 under 50%
- Recall: 191 parents flagged of 291 fix merges
- Claude: $1218.69 in the last 24 hours, $7649.91 in the last 7 days, $7649.91 in all
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

None.
