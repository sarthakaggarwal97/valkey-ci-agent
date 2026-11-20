# Fuzzer walk

Updated 2026-11-20T21:52+00:00.

| Repository | Done | Remaining | Backlog cursor | Last 24 hours | Last 7 days (per day) |
|---|---|---|---|---|---|
| valkey-io/valkey | 2570 | 0 | #12477 (2023-08-14) | 23 | 57.3 |
| valkey-io/valkey-bloom | 112 | 0 | ce516d0ef6e3 (2024-04-26) | 3 | 0.4 |
| valkey-io/valkey-json | 66 | 0 | c1423d7e4b38 (2024-11-25) | 0 | 0.1 |
| valkey-io/valkey-search | 705 | 0 | f8106e72e984 (2024-11-27) | 8 | 3.3 |

## Last runs

### valkey-io/valkey

[2026-11-20T21:37:58+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/44821942402): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-bloom

[2026-11-20T21:37:54+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/44821921038): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-json

[2026-11-20T21:37:44+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/44821911241): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-search

[2026-11-20T21:52:16+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/44791600818): 6 merge(s) tested, 2 without a code change.

- [valkey-io/valkey-search#1729](https://github.com/valkey-io/valkey-search/pull/1729) ([tests](tests/valkey-search/1729-67e82c198/)): Report compatibility-fix usage in a single INFO field (#1729): 5 of 5 test(s) reached the change
- [valkey-io/valkey-search#1676](https://github.com/valkey-io/valkey-search/pull/1676) ([tests](tests/valkey-search/1676-73dbb8071/)): fix(vector-registry): handle RENAME/MOVE while keys await pending unshare (#1676): 0 of 4 test(s) reached the change; crash, fixed by #1676 (recorded); crash, fixed by #1676 (recorded); crash, fixed by #1676 (recorded)
- [valkey-io/valkey-search#1730](https://github.com/valkey-io/valkey-search/pull/1730) ([tests](tests/valkey-search/1730-f30b4c8b2/)): Change 1.3.0-RC1 to 1.3.0 in command JSON files (#1730): declined: The change edits only version strings ("1.3.0-RC1" -> "1.3.0") in the history and module_since fields of src/commands/ft.*.json. These JSON files are documentation metadata: no build rule, cmake script, or source file in valkey-search references or embeds them, and the module registers its
- [valkey-io/valkey-search#1747](https://github.com/valkey-io/valkey-search/pull/1747) ([tests](tests/valkey-search/1747-a25ed796a/)): Fix HNSW delete race on label_lookup_ with concurrent inserts (#1747): 0 of 6 test(s) reached the change
- [valkey-io/valkey-search#1734](https://github.com/valkey-io/valkey-search/pull/1734) ([tests](tests/valkey-search/1734-2d99b27ae/)): fix(vector-registry): keep shared vectors until a logically expired key or field is deleted (#1734): 3 of 3 test(s) reached the change
- [valkey-io/valkey-search#1735](https://github.com/valkey-io/valkey-search/pull/1735) ([tests](tests/valkey-search/1735-8433b8fff/)): docs: align 1.3.0 documentation and command JSON with the implementation (#1735): declined: The change touches only documentation (Markdown files under docs/ and the top-level COMMANDS.md, COMPATIBILITY.md, README.md, QUICK_START.md) and the src/commands/*.json command-description files. Those JSON files are not referenced by the build or by any source file: they are doc metadata

## Totals

- Merges whose tests reached the change: 1005 of 1614 tested (62%)
- Changed-line coverage: median 100% of 1079 merge(s) measured, 79 under 50%
- Recall: 269 parents flagged of 421 fix merges
- Claude: $126.13 in the last 24 hours, $1367.21 in the last 7 days, $11828.64 in all
- Issues filed: 85
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
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/168
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
- Model updates: 2 asked for, 2 with no model change, 0 rejected, 0 failed; 0 pull request(s)

## Retries

None.
