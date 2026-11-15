# Fuzzer walk

Updated 2026-11-15T06:51+00:00.

| Repository | Done | Remaining | Backlog cursor | Last 24 hours | Last 7 days (per day) |
|---|---|---|---|---|---|
| valkey-io/valkey | 2538 | 3 | #12386 (2023-08-16) | 259 | 239.4 |
| valkey-io/valkey-bloom | 109 | 0 | ce516d0ef6e3 (2024-04-26) | 0 | 0.0 |
| valkey-io/valkey-json | 66 | 0 | c1423d7e4b38 (2024-11-25) | 0 | 0.6 |
| valkey-io/valkey-search | 693 | 0 | f8106e72e984 (2024-11-27) | 5 | 17.6 |

## Last runs

### valkey-io/valkey

[2026-11-15T03:00:56+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/43664200825): 20 merge(s) tested, 3 without a code change.

- [redis/redis#12557](https://github.com/redis/redis/pull/12557) ([tests](tests/valkey/12557-96e9dec41/)): Bump codespell from 2.2.4 to 2.2.5 (#12557): declined: The change is a CI dependency bump (codespell 2.2.4 -> 2.2.5) plus the typo fixes it flagged: every hunk edits comments in C sources or Tcl test files except one, which adds a space to the SENTINEL CONFIG usage-error string in sentinel.c. No executable behaviour, reply semantics, replicati
- [redis/redis#12545](https://github.com/redis/redis/pull/12545) ([tests](tests/valkey/12545-90e9fc387/)): Update command tips on more admin / configuration commands (#12545): 3 of 3 test(s) reached the change
- [redis/redis#12538](https://github.com/redis/redis/pull/12538) ([tests](tests/valkey/12538-a2046c1eb/)): Check shard_id pointer validity in updateShardId (#12538): declined: The guard added to updateShardId only matters when ext_shardid is NULL, which happens solely in clusterProcessPingExtensions when a peer's gossip message carries no shard-id extension. writePingExt appends the shard-id extension unconditionally and myself->shard_id is always populated, so 
- [redis/redis#9414](https://github.com/redis/redis/pull/9414) ([tests](tests/valkey/9414-044e29dd3/)): redis-benchmark - add the support for binary strings (#9414): 1 of 1 test(s) reached the change, coverage 100%
- [redis/redis#12533](https://github.com/redis/redis/pull/12533) ([tests](tests/valkey/12533-49f7d173b/)): Remove unnecessary use of sds and mem copy in module.c (#12533): 0 of 2 test(s) reached the change, coverage 100%
- [redis/redis#12447](https://github.com/redis/redis/pull/12447) ([tests](tests/valkey/12447-370d38016/)): Fix potential crash on failed OpenSSL init (#12447): declined: The change adds a NULL check after SSL_CTX_new in createSSLContext (src/tls.c). SSL_CTX_new only returns NULL when OpenSSL itself fails to initialize or allocate — an out-of-memory or broken-OpenSSL-installation condition, not anything reachable through server configuration, commands, repl
- [redis/redis#12450](https://github.com/redis/redis/pull/12450) ([tests](tests/valkey/12450-b26e8e321/)): Optimize ZRANGE offset location from linear search to skiplist jump. (#12450): 0 of 4 test(s) reached the change, coverage 92%
- [redis/redis#12534](https://github.com/redis/redis/pull/12534) ([tests](tests/valkey/12534-9ce8c54d7/)): Update sort_ro reply_schema to mention the null reply (#12534): declined: The change only corrects documentation metadata: sort_ro.json's reply_schema now mentions the null element that SORT_RO with GET already returned, sort.json gets a whitespace-only description fix, and a TCL test is extended. No C code or generated command table changes, so server behaviour
- [redis/redis#12486](https://github.com/redis/redis/pull/12486) ([tests](tests/valkey/12486-751996052/)): Allows modules to declare new ACL categories. (#12486): 3 of 3 test(s) reached the change, coverage 63%
- [redis/redis#12522](https://github.com/redis/redis/pull/12522) ([tests](tests/valkey/12522-b59f53efb/)): Fix sort_ro get-keys function return wrong key number (#12522): 2 of 2 test(s) reached the change, coverage 100%
- [redis/redis#12476](https://github.com/redis/redis/pull/12476) ([tests](tests/valkey/12476-e3d4b30d0/)): Add two stats to count client input and output buffer oom. (#12476): 2 of 4 test(s) reached the change, coverage 100%
- [redis/redis#12530](https://github.com/redis/redis/pull/12530) ([tests](tests/valkey/12530-4b281ce51/)): Align CONFIG RESETSTAT/REWRITE tips with SET. (#12530): 3 of 3 test(s) reached the change
- [redis/redis#12506](https://github.com/redis/redis/pull/12506) ([tests](tests/valkey/12506-a6eff389b/)): redis.conf: Add data loss warning to "appendonly" (#12506): declined: The change adds a documentation comment to redis.conf (a warning about toggling appendonly in the config file of an existing database). No source code, configuration default, or runtime behaviour changes, so there is nothing for a scenario to exercise.
- [redis/redis#11734](https://github.com/redis/redis/pull/11734) ([tests](tests/valkey/11734-1407ac1f3/)): BITCOUNT and BITPOS with non-existing key and illegal arguments should return error, not 0 (#11734): 2 of 2 test(s) reached the change, coverage 100%
- [redis/redis#12394](https://github.com/redis/redis/pull/12394) ([tests](tests/valkey/12394-45d331069/)): BITCOUNT: check for argument, before checking for key (#12394): 2 of 2 test(s) reached the change, coverage 100%
- [redis/redis#12465](https://github.com/redis/redis/pull/12465) ([tests](tests/valkey/12465-c98a28a84/)): Fix LREM count LONG_MIN overflow minor issue (#12465): 2 of 2 test(s) reached the change, coverage 100%
- [redis/redis#12505](https://github.com/redis/redis/pull/12505) ([tests](tests/valkey/12505-16988208b/)): config.memkeys init for consistency (#12505): declined: The change adds an explicit `config.memkeys = 0;` in valkey-cli's main(), but `config` is a file-scope static struct (src/redis-cli.c:214) that C zero-initializes before main() runs, so the field was already 0 on every path. This is a pure consistency refactor with no behaviour change on e
- [redis/redis#12426](https://github.com/redis/redis/pull/12426) ([tests](tests/valkey/12426-fe47c2027/)): Signal handler attributes (#12426): 0 of 12 test(s) reached the change, coverage 78%; [crash, predates redis/redis#12426](https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/165); [crash, predates redis/redis#12426](https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/166); [crash, predates redis/redis#12426](https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/167)
- [redis/redis#12279](https://github.com/redis/redis/pull/12279) ([tests](tests/valkey/12279-44cc0fcb9/)): redis-cli --stat take dbnum value from CONFIG GET to output total keys (#12279): 1 of 1 test(s) reached the change, coverage 80%; [crash, shown by ASan on unstable, the one build with it](https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/81)
- [redis/redis#11698](https://github.com/redis/redis/pull/11698) ([tests](tests/valkey/11698-ac6bc5d1a/)): redis-cli: Fix print of keys per cluster host when over int max (#11698): declined: The change is a pure integer-overflow correctness fix in valkey-cli's `--cluster info` output: the per-host `dbsize`, the cluster-wide `keys` total, and their printf specifiers go from int/%d to long long/%lld. The two builds produce identical output for every value that fits in a 32-bit i

### valkey-io/valkey-bloom

[2026-11-15T06:51:00+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/43687252454): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-json

[2026-11-15T06:50:57+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/43687248614): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-search

[2026-11-15T06:51:01+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/43687250469): 0 merge(s) tested, 0 without a code change.


## Totals

- Merges whose tests reached the change: 992 of 1587 tested (63%)
- Changed-line coverage: median 100% of 1062 merge(s) measured, 79 under 50%
- Recall: 262 parents flagged of 411 fix merges
- Claude: $1004.08 in the last 24 hours, $5856.54 in the last 7 days, $11623.19 in all
- Issues filed: 84
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
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/153
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/156
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/157
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/162
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/164
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/165
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/166
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/167
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

None.
