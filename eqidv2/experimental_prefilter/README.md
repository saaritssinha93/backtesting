# Experimental Pre-Filter V1

This package is a standalone research module. It is not imported by V7 or V11,
has no scheduler or launcher, and never writes unless an experimental/research
output path is explicitly supplied.

## Scope

V1 implements only the approved research phases:

1. Structured feed-to-scanner latency attribution.
2. A deterministic, causal, long/short statistical shadow ranker.
3. Nested shortlist evaluation for `K=100,150,...,400` against a separate
   full-universe oracle.

LightGBM, reinforcement learning, sector gates, production consumption,
carry-over, and dynamic budgets are intentionally absent or disabled.

## Safety contract

`shadow-rank` accepts a slot only when all of the following hold:

- The archived feed marker has `source=final` and `complete=true`.
- Expected/written ticker accounting is exact and all failure counts are zero.
- Marker and canonical universe slots, counts, and hashes match.
- Every requested Parquet ends at the exact final slot.
- Marker and universe files are unchanged when re-read after the Parquets.

The output path must contain `experiment` or `research` and cannot be inside
the V7/V11/live-trading runtime roots.

## Commands

All commands are manual. Nothing here creates or starts a session.

Dry-run one completed slot and print only a summary:

```powershell
python -m experimental_prefilter shadow-rank
```

Write research outputs explicitly:

```powershell
python -m experimental_prefilter shadow-rank `
  --output-dir C:\TradingData\eqidv2_experiments\prefilter_v1
```

Use `--limit-symbols 10` only for an explicit smoke test. A subset run is not a
valid recall or latency benchmark.

Profile archived feed/scanner completion markers without writing:

```powershell
python -m experimental_prefilter profile-latency --date 2026-08-04
```

Replay one historical day at 09:20 and then hourly through 15:20. This reads
five-minute bars only through each slot and writes research outputs only:

```powershell
python -m experimental_prefilter hourly-replay `
  --date 2026-08-03 `
  --budget 300 `
  --output-dir C:\TradingData\eqidv2_experiments\prefilter_hourly_replay
```

The archived universe manifest must match the count and hash in every final
slot marker. Each replay writes the normal per-slot JSON/ranking files plus a
combined `hourly_candidates_YYYYMMDD.csv` and run summary. The files remain
research-only and are not consumed by V7 or V11.

Evaluate a full ranking against a causal full-universe oracle:

```powershell
python -m experimental_prefilter evaluate `
  --ranking C:\TradingData\eqidv2_experiments\prefilter_v1\latest_prefilter_ranking.csv `
  --oracle C:\path\to\full_universe_oracle.csv `
  --universe-count 1236
```

## Output contract

When explicitly enabled, each slot produces:

- `prefilter_candidates_YYYYMMDD_HHMM.json`: selected research candidates and
  frozen provenance.
- `prefilter_ranking_YYYYMMDD_HHMM.csv`: the full auditable ranking with nested
  selection flags for each K.
- Equivalent `latest_...` files under the same experimental directory.

Every JSON declares `mode=SHADOW_RESEARCH_ONLY` and
`production_consumption_allowed=false`.

## Interpreting results

The filter is intended to reduce downstream computation while preserving
unchanged downstream signals. It cannot create trades by itself. Promotion is
out of scope until an unchanged full-universe oracle and live shadow period
demonstrate recall, P&L non-inferiority, and deadline improvement.
