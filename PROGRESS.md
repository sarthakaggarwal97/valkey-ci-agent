# Fuzzer walk

Updated 2026-11-16T19:49+00:00.

| Repository | Done | Remaining | Backlog cursor | Last 24 hours | Last 7 days (per day) |
|---|---|---|---|---|---|
| valkey-io/valkey | 2547 | 0 | #12477 (2023-08-14) | 6 | 181.7 |
| valkey-io/valkey-bloom | 109 | 0 | ce516d0ef6e3 (2024-04-26) | 0 | 0.0 |
| valkey-io/valkey-json | 66 | 0 | c1423d7e4b38 (2024-11-25) | 0 | 0.4 |
| valkey-io/valkey-search | 697 | 0 | f8106e72e984 (2024-11-27) | 3 | 4.0 |

## Last runs

### valkey-io/valkey

[2026-11-16T19:49:01+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/43928924926): 4 merge(s) tested, 1 without a code change.

- [valkey-io/valkey#5432](https://github.com/valkey-io/valkey/pull/5432) ([tests](tests/valkey/5432-f0464f05e/)): Allow a module thread holding the GIL to replicate during a forkless save (#5432): 0 of 3 test(s) reached the change, coverage 100%
- [valkey-io/valkey#5340](https://github.com/valkey-io/valkey/pull/5340) ([tests](tests/valkey/5340-0dbc5344f/)): Remove the INFO hotkeys section (#5340): 3 of 3 test(s) reached the change
- [valkey-io/valkey#4681](https://github.com/valkey-io/valkey/pull/4681) ([tests](tests/valkey/4681-d77fbf91f/)): Fix dynamic priority-subnets demotion of replicas/slot migration links and counter drift (#4674) (#4681): 3 of 8 test(s) reached the change, coverage 100%
- [valkey-io/valkey#5359](https://github.com/valkey-io/valkey/pull/5359) ([tests](tests/valkey/5359-9d8f3c003/)): Reset slot migration snapshot pipes when the snapshot fork fails (#5359): 1 of 1 test(s) reached the change, coverage 100%; crash, fixed by #5359 (recorded)
- model `slot-migration` for [valkey-io/valkey#5359](https://github.com/valkey-io/valkey/pull/5359): no model change: The rule checked is the model's SFork action (model.tla:154-163), which maps to slotMigrationJobReadEstablishResponse and slotExportJobBeginSnapshotToTargetSocket in claims.yaml's scope: S reads +OK, exp goes "establish" -> "stream", and the snapshot is enqueued to T. SFork models o

### valkey-io/valkey-bloom

[2026-11-16T18:28:09+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/43928943942): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-json

[2026-11-16T18:28:05+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/43928932190): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-search

[2026-11-16T18:28:04+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/43928938170): 0 merge(s) tested, 0 without a code change.


## Totals

- Merges whose tests reached the change: 998 of 1596 tested (63%)
- Changed-line coverage: median 100% of 1067 merge(s) measured, 79 under 50%
- Recall: 264 parents flagged of 413 fix merges
- Claude: $56.88 in the last 24 hours, $4185.12 in the last 7 days, $11702.51 in all
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
