# Fuzzer walk

Updated 2026-11-14T19:20+00:00.

| Repository | Done | Remaining | Backlog cursor | Last 24 hours | Last 7 days (per day) |
|---|---|---|---|---|---|
| valkey-io/valkey | 2416 | 124 | #12847 (2023-12-08) | 271 | 241.0 |
| valkey-io/valkey-bloom | 109 | 0 | ce516d0ef6e3 (2024-04-26) | 0 | 0.0 |
| valkey-io/valkey-json | 66 | 0 | c1423d7e4b38 (2024-11-25) | 2 | 0.6 |
| valkey-io/valkey-search | 691 | 0 | f8106e72e984 (2024-11-27) | 9 | 33.3 |

## Last runs

### valkey-io/valkey

[2026-11-14T18:15:49+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/43613936860): 20 merge(s) tested, 2 without a code change.

- [redis/redis#12804](https://github.com/redis/redis/pull/12804) ([tests](tests/valkey/12804-852795959/)): Replace slots_to_channels radix tree with slot specific dictionaries for shard channels. (#12804): 0 of 4 test(s) reached the change, coverage 44%
- [redis/redis#12828](https://github.com/redis/redis/pull/12828) ([tests](tests/valkey/12828-fa751f9be/)): config.c: Avoid leaking file handle if file is 0 bytes (#12828): declined: This is a resource-leak fix: rewriteConfigReadOldFile (reached only via CONFIG REWRITE) now fcloses the file handle when the config file is exactly 0 bytes instead of leaking one FILE* per call. It changes no reply, no persisted data, and no replication/failover/persistence behavior -- bot
- [redis/redis#12782](https://github.com/redis/redis/pull/12782) ([tests](tests/valkey/12782-1aa633d61/)): Implement TCP Keep-Alives across most Unix-like systems (#12782): declined: The change only rewrites anetKeepAlive in src/anet.c for cross-platform portability (Solaris, macOS, BSDs, AIX). On Linux, where the fuzzer's servers run, the preprocessor selects code identical to the old #ifdef __linux__ branch: the same setsockopt calls (SO_KEEPALIVE, TCP_KEEPIDLE=inter
- [redis/redis#12878](https://github.com/redis/redis/pull/12878) ([tests](tests/valkey/12878-27a8e3b04/)): fix missing comments (#12878): declined: The change edits only a comment in src/quicklist.h, documenting the existing dont_compress bit-field and correcting the spare-bit count from 10 to 9. No code, struct layout, or behaviour changes, so there is nothing a scenario could exercise.
- [redis/redis#12884](https://github.com/redis/redis/pull/12884) ([tests](tests/valkey/12884-baf5699d7/)): fix comment of aeProcessEvents (#12884): declined: The change is a comment-only fix in src/ae.c, correcting the documentation of aeProcessEvents' event ordering. No executable code, behaviour, reply, or configuration changes, so there is nothing to test.
- [redis/redis#12885](https://github.com/redis/redis/pull/12885) ([tests](tests/valkey/12885-71f31da66/)): Add restart option to create-cluster script (#12885): declined: The change adds a 'restart' option to utils/create-cluster/create-cluster, a developer convenience shell script for spinning up local test clusters, and updates its usage text. No server code, command, tool binary (valkey-cli, valkey-benchmark, valkey-check-rdb/aof), replication, persisten
- [redis/redis#12171](https://github.com/redis/redis/pull/12171) ([tests](tests/valkey/12171-20214b26a/)): Don't disconnect all clients in ACL LOAD (#12171): 7 of 7 test(s) reached the change, coverage 94%
- [redis/redis#11315](https://github.com/redis/redis/pull/11315) ([tests](tests/valkey/11315-09e0d338f/)): redis-cli adds -4 / -6 options to determine IPV4 / IPV6 priority in DNS lookup (#11315): 1 of 1 test(s) reached the change, coverage 84%
- [redis/redis#10856](https://github.com/redis/redis/pull/10856) ([tests](tests/valkey/10856-23e980e77/)): Move cliVersion to cli_common and add --version support for redis-check-aof (#10856): 2 of 2 test(s) reached the change, coverage 69%
- [redis/redis#12862](https://github.com/redis/redis/pull/12862) ([tests](tests/valkey/12862-adbb534f0/)): Always keep an in-memory history of all commands in redis-cli (#12862): declined: The change only affects valkey-cli's interactive line-editing history, and every changed code path is gated on isatty(fileno(stdin)) being true (redis-cli.c:3366-3375 sets history/historyfile only on a real terminal; FAKETTY only fakes stdout's tty at redis-cli.c:9907). The fuzzer's valkey
- [redis/redis#12848](https://github.com/redis/redis/pull/12848) ([tests](tests/valkey/12848-d8a21c576/)): Unified db rehash method for both standalone and cluster (#12848): 0 of 7 test(s) reached the change, coverage 100%
- [redis/redis#12837](https://github.com/redis/redis/pull/12837) ([tests](tests/valkey/12837-967fb3c6e/)): Extend rax usage by allowing any long long value (#12837): 0 of 11 test(s) reached the change, coverage 69%
- [redis/redis#12728](https://github.com/redis/redis/pull/12728) ([tests](tests/valkey/12728-e95a5d483/)): Support by/get options for sort(_ro) in cluster mode when pattern implies slot. (#12728): 2 of 2 test(s) reached the change, coverage 100%
- [redis/redis#12860](https://github.com/redis/redis/pull/12860) ([tests](tests/valkey/12860-3c0fd2520/)): Redact ACL username information and mark *-key-file-pass configs as sensitive (#12860): 3 of 3 test(s) reached the change, coverage 100%
- [redis/redis#12849](https://github.com/redis/redis/pull/12849) ([tests](tests/valkey/12849-f9cc25c1d/)): Add metric to INFO CLIENTS: pubsub_clients. (#12849): 2 of 2 test(s) reached the change, coverage 70%
- [redis/redis#12745](https://github.com/redis/redis/pull/12745) ([tests](tests/valkey/12745-c85a9b789/)): Fix delKeysInSlot server events are not executed inside an execution unit (#12745): 0 of 6 test(s) reached the change, coverage 100%; [crash, appeared after redis/redis#12745](https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/162); crash, predates redis/redis#12745, since fixed (recorded)
- [redis/redis#12763](https://github.com/redis/redis/pull/12763) ([tests](tests/valkey/12763-62419c01d/)): Handle missing fields in dbSwapDatabases and swapMainDbWithTempDb (#12763): 0 of 6 test(s) reached the change, coverage 100%
- [redis/redis#12846](https://github.com/redis/redis/pull/12846) ([tests](tests/valkey/12846-e6423b7a7/)): Fix rehashingStarted miscalculating bucket_count in dict initialization (#12846): 2 of 2 test(s) reached the change, coverage 100%
- [redis/redis#12767](https://github.com/redis/redis/pull/12767) ([tests](tests/valkey/12767-a3ae2ed37/)): Remove dead code around should_expand_db (#12767): declined: The change is a pure refactor with no behavior change. In src/rdb.c it removes error-handling branches after dbExpand(..., try_expand=0) calls that were unreachable dead code: dbExpand only returns C_ERR on paths guarded by `try_expand && result == DICT_ERR`, and with try_expand=0 it alway
- [redis/redis#12784](https://github.com/redis/redis/pull/12784) ([tests](tests/valkey/12784-7410d985b/)): Remove overhead.hashtable.slot-to-keys from memory-stats reply_schema (#12784): declined: The change only removes a stale entry (overhead.hashtable.slot-to-keys) from memory-stats.json's reply_schema and a blank line in object.c. No code in the checkout emits that field (grep for slot-to-keys finds nothing in src/), so the MEMORY STATS reply is byte-identical on both builds; re

### valkey-io/valkey-bloom

[2026-11-14T19:14:22+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/43630942379): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-json

[2026-11-14T19:20:39+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/43631524731): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-search

[2026-11-14T18:52:00+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/43628892454): 0 merge(s) tested, 0 without a code change.


## Totals

- Merges whose tests reached the change: 955 of 1526 tested (63%)
- Changed-line coverage: median 100% of 1008 merge(s) measured, 77 under 50%
- Recall: 251 parents flagged of 395 fix merges
- Claude: $799.88 in the last 24 hours, $6336.04 in the last 7 days, $11202.64 in all
- Issues filed: 80
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
