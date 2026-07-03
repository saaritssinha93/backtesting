# A_MOD_BREAK_C1_HIGH — Winner/Loser Study (1-minute path geometry)

_Generated 2026-07-03. Basis: FIT window only (leak-safe design), R1 base = first-signal-per-
ticker-day + genuine 20-bar-high break → 5,569 signals. Source: `paths/summary.csv` (validated
99% vs canonical resolver; per-trade fills/outcomes 400/400 identical after entry-timing fix)._

## Excursion geometry from the next-1m-open entry

| stat | p25 | p50 | p75 | p90 |
|---|---:|---:|---:|---:|
| MFE (max favorable) % | 0.386 | **0.874** | 1.675 | 2.825 |
| MAE (max adverse) % | -1.243 | -0.653 | -0.270 | -0.081 |
| **MAE before MFE** % | -0.443 | **-0.188** | -0.055 | 0.000 |
| EOD return % | -0.647 | +0.032 | +0.833 | +1.826 |
| min low, first 30m % | -0.541 | -0.282 | -0.112 | -0.023 |
| min low, first 60m % | -0.772 | -0.400 | -0.163 | -0.039 |

## What this says about the old design

1. **The 0.70% SL was noise-level**: median *total* MAE is −0.65%, so most trades eventually
   touch −0.7%… but median MAE **before the peak** is only −0.19%. The old bracket let winners
   round-trip into stops. A stop in the 0.45–0.55% zone protects 70-75% of trades until their MFE.
2. **The 1.00% target was above the median MFE** (0.87%) — most trades could never pay it.
3. **EOD drift ≈ 0** (51.1% positive): holding to close captures nothing. The MFE must be
   *harvested* — target or trail, not hope.

## Winner/loser separator: 1-minute confirmation

Share of signals whose 1-min high exceeds the 5-min signal-bar high: 60.0% ≤5m, 68.9% ≤10m,
80.1% ≤30m, **9.9% never**.

| group | n | median MFE | median EOD | median MAE-before-MFE |
|---|---:|---:|---:|---:|
| confirmed ≤10 min | 3,838 | **+1.025%** | **+0.135%** | -0.146% |
| never confirmed | 553 | +0.109% | **-0.925%** | — |

Signals that don't take out the signal-bar high within minutes are dead-on-arrival. A stop-buy
above the signal high is simultaneously an entry and the strongest loser filter found in either
campaign — at signal-bar time this information does not exist (no 5-min feature separated
winners this cleanly), it only exists in the 1-minute future path, i.e. in HOW you enter.

## Retest behaviour

54.1% of signals trade ≥0.25% below the next-open entry within 30 min; 27.7% trade ≥0.5% below.
A resting limit ~0.25-0.30% below captures half the population at materially better cost basis —
at the price of missing some of the strongest runners (tested as Block C).

## Best/worst context (from campaigns 1-2, unchanged)

- Time: detector fires mostly ≥11:00; morning subset cleaner per-book, afternoon better per-trade.
- Symbols/days: losses diffuse; no event concentration; nothing to blacklist honestly.
- Volume: vol_ratio ≥2.6-3.0 mildly positive; most other 5-min features uninformative.

## Design implications carried into the iteration blocks

| finding | design response |
|---|---|
| MAE-before-MFE ~0.2% vs MFE ~0.9% | SL 0.35-0.55 x TGT 0.8-1.25 grids (Block A) |
| EOD drift zero | time-cap exits, trailing stops (A) |
| never-confirmed = losers | confirmation stop-buy entry (B) |
| 54% retest ≥0.25% | retest-limit entry (C) |
| morning vs midday split | time-window variants (D) |
