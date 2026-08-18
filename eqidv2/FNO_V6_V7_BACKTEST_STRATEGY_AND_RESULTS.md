# FnO V6 and V7 Backtesting Strategy and Results

**Report date:** 2026-08-18  
**Status:** Detailed strategy audit and corrected research comparison  
**Important:** The currently generated V6 and V7 performance artifacts contain a cross-session forward-path defect. Their hashes and provenance remain useful, but their reported performance is not valid. The primary comparison in this document uses a separate, corrected same-session replay against the frozen source data.

## 1. Executive summary

V6 and V7 share the same:

- dated futures universe;
- cash-equity price and volume source;
- futures OI source;
- five-minute trend, price, OI and volume filters;
- ten slot-and-side setup legs;
- ranking rules and entry caps;
- stop-loss and target percentages;
- five-basis-point round-trip cost assumption.

The only intended V6-to-V7 strategy change is the one-minute confirmation gate:

- **V6:** requires a directional confirmation candle, a close beyond the five-minute signal close, and setup-specific body/wick morphology.
- **V7:** accepts any valid positive-range confirmation candle, ignores its colour and morphology, and lets a later break of its high/low confirm direction.

Corrected, same-session, apples-to-apples research results through 2026-08-17:

| Metric | V6 strict confirmation | V7 high/low breakout |
|---|---:|---:|
| Sessions | 57 | 57 |
| Orders | 222 | 440 |
| Fills | 221 | 405 |
| Fill rate | 99.5% | 92.0% |
| Trade profit factor | **2.171** | 1.551 |
| Net return sum | **+99.17 percentage points** | +98.82 percentage points |
| Maximum cumulative daily drawdown | **5.60 points** | 16.49 points |

V7 obtained almost the same additive net result by taking far more trades. Its profit factor was materially lower and its drawdown was approximately three times V6. The corrected evidence therefore supports V6 as the stronger risk-adjusted baseline. V7 remains a research strategy, not a promoted live strategy.

## 2. Version identities

| Item | V6 | V7 |
|---|---|---|
| Strategy version | `FNO_V6_BEST_NET_CASH_EQUITY_20260811` | `FNO_V7_V6_CLONE_1M_HIGH_LOW_BREAKOUT_20260818` |
| Objective/config source | `BEST_NET`; selected from V5 full-history research | Exact V6 copy except one-minute confirmation |
| Status | Promoted V6 configuration; historical metrics require regeneration | Research-only; no V7 live deployment |
| Confirmation policy | `v6_strict` | `v7_high_low_breakout` |
| Setup-book source | Frozen V6 ten-leg setup book | V6 setup book copied and hash-pinned |
| V6 setup-book SHA-256 pinned by V7 | — | `3c3e59187768afbc015024b5735d1c1b62d91128e8d6888ccfaa6f1c6c15694a` |

Code:

- V6: `fno_oi_ema_confirm_0925_0930_0935_0940_0945_v6.py`
- V7: `fno_oi_ema_confirm_0925_0930_0935_0940_0945_v7.py`
- Candidate construction and bracket simulator: `fno_oi_ema_confirm_sweep.py`
- Eligibility, ranking, caps and daily curve: `fno_v5_hybrid_backtest.py`
- Cash-equity/futures-OI data contract: `fno_oi_hybrid_data.py`

## 3. Universe and source-data contract

### 3.1 Dated universe

Both versions use:

`C:\TradingData\eqidv2\fno_oi\universe\near_month_2026-08-11.parquet`

The dated file contains 213 contracts: 208 mapped stock futures used by the strategy and five excluded index futures.

| Fingerprint | SHA-256 |
|---|---|
| Universe file | `24170f39c7cf99021553396e40e0d88a435f857364b2423dcfbe9312539dbf09` |
| Full semantic universe | `18c496bbf9e09b6914d073cba21c4c6c56305da1ed5759f4f91cc8cb66c19ad5` |
| Mapped 208-stock universe | `2cc160189f87bff4eb987a15a4684d95619ee9c810db3cd37276b114ad5824bf` |
| Mapped symbol set | `d42f87a9c5fc8ab1710b09b6c4c9832c9d19ecc440ef92b84cad6981499a05a3` |

### 3.2 Price, volume and OI sources

The shared data contract is `fno_v5_equity_real_5m_futures_oi_v4`:

- NSE cash-equity one-minute data supplies OHLCV, the reconstructed five-minute prices and volume, the one-minute confirmation candle, the entry path and the exit path.
- Mapped NFO stock-future five-minute data supplies only `oi`, `prev_oi` and `oi_change_pct`.
- Price returns are therefore cash-equity returns; futures data contributes OI confirmation rather than futures execution prices.

Five-minute cash candles are built only from five exact, completed, end-labelled one-minute rows. Flagged `gap_filled`, `opening_snapshot` or `provisional_stale` rows are excluded when those lineage fields exist. An incomplete five-row group is not used.

### 3.3 Five-minute features

For each completed cash-equity five-minute candle:

- `EMA9`, `EMA20` and `EMA50` are calculated from cash closes.
- `price_change_pct = (close / previous_5m_close - 1) × 100`.
- `volume_ratio = current_5m_volume / mean(previous 20 completed 5m volumes)`, with at least five prior bars required.
- `traded_value = close × volume`.
- `oi_change_pct = (future_OI / previous_future_OI - 1) × 100` when both OI values are finite and positive.

## 4. Shared five-minute candidate generation

Before the ten V6/V7 setup legs are applied, the broad builder requires:

| Condition | LONG | SHORT |
|---|---|---|
| Cash EMA structure | `EMA9 > EMA20 > EMA50` | `EMA9 < EMA20 < EMA50` |
| Cash five-minute price move | `>= +0.10%` | `<= -0.10%` |
| Futures OI change | `>= +0.05%` and current OI > previous OI | Same |
| Cash volume ratio | `>= 0.80` | Same |

Only the five signal ends used by the setup book—09:25, 09:30, 09:35, 09:40 and 09:45—can ultimately produce V6/V7 orders.

## 5. Exact ten-leg setup book

Definitions:

- **Price:** minimum absolute directional five-minute cash price move.
- **OI:** minimum futures OI increase.
- **Volume:** minimum cash five-minute volume ratio.
- **Body:** V6 minimum one-minute `abs(close-open)/(high-low)`.
- **Wick:** V6 maximum adverse-wick ratio. It is the upper wick for LONG and lower wick for SHORT.
- **Picker:** ranking field after all eligibility rules pass.
- **Cap:** maximum selected names for that day, slot and side.

| Signal | Confirm | Side | Cap | Picker | Price % | OI % | Volume | V6 body min | V6 wick max | Stop % | Target % |
|---|---|---|---:|---|---:|---:|---:|---:|---:|---:|---:|
| 09:25 | 09:26 | LONG | 1 | Max liquidity | 0.30 | 0.10 | 3.00 | 0.60 | 0.50 | 0.50 | 3.00 |
| 09:25 | 09:26 | SHORT | 2 | Max volume | 0.20 | 0.10 | 1.50 | 0.40 | 0.50 | 0.75 | 3.00 |
| 09:30 | 09:31 | LONG | 1 | Max absolute move | 0.65 | 0.10 | 1.00 | 0.50 | 0.50 | 1.00 | 2.50 |
| 09:30 | 09:31 | SHORT | 1 | Max absolute move | 0.20 | 0.25 | 1.00 | 0.40 | 0.50 | 1.00 | 3.00 |
| 09:35 | 09:36 | LONG | 1 | Max liquidity | 0.20 | 0.10 | 1.00 | 0.60 | 0.50 | 1.00 | 2.50 |
| 09:35 | 09:36 | SHORT | 2 | Max liquidity | 0.50 | 1.00 | 1.00 | 0.40 | 0.50 | 1.00 | 3.00 |
| 09:40 | 09:41 | LONG | 1 | Max liquidity | 0.20 | 0.10 | 2.00 | 0.50 | 0.50 | 0.50 | 2.50 |
| 09:40 | 09:41 | SHORT | 1 | Max absolute move | 0.20 | 0.10 | 1.00 | 0.40 | 0.50 | 1.00 | 3.00 |
| 09:45 | 09:46 | LONG | 1 | Max absolute move | 0.65 | 0.10 | 1.00 | 0.40 | 0.50 | 1.00 | 3.00 |
| 09:45 | 09:46 | SHORT | 1 | Max volume | 0.20 | 0.75 | 1.00 | 0.40 | 0.30 | 1.00 | 2.00 |

The theoretical daily maximum is 12 selections: five LONG and seven SHORT. A symbol can be selected again in a later slot because every slot occurrence is a separate signal.

## 6. V6 one-minute entry strategy

For a five-minute signal ending at time `S`, V6 requires the exact cash-equity candle ending at `S + 1 minute`.

For confirmation OHLC `O1,H1,L1,C1` and five-minute signal close `C5`:

### 6.1 Direction gate

- LONG: `C1 > O1` and `C1 > C5`.
- SHORT: `C1 < O1` and `C1 < C5`.

### 6.2 Morphology gate

The candle must have positive range `R = H1 - L1`.

- `body_ratio = abs(C1 - O1) / R`.
- LONG adverse wick: `(H1 - max(O1,C1)) / R`.
- SHORT adverse wick: `(min(O1,C1) - L1) / R`.

The setup-specific minimum body and maximum wick are shown in the setup table.

### 6.3 Entry trigger

- LONG stop-entry trigger: `H1`.
- SHORT stop-entry trigger: `L1`.
- Entry search starts with the following one-minute candle. The confirmation candle cannot fill its own order.

V6 therefore asks the one-minute candle itself to show direction and displacement, then asks later price action to break its extreme.

## 7. V7 one-minute entry strategy

V7 deliberately changes only the confirmation seam.

### 7.1 Candle validity

The exact `S + 1 minute` cash candle must have:

- finite, strictly positive OHLC;
- valid OHLC geometry;
- nonnegative volume;
- positive range;
- no flagged synthetic or stale lineage row.

### 7.2 Removed V6 conditions

V7 does **not** require:

- candle colour to agree with LONG/SHORT;
- confirmation close to move beyond the five-minute signal close;
- a minimum body ratio;
- a maximum adverse-wick ratio.

Internally, the cloned V7 setup book sets body minimum to `0.0` and wick maximum to `1.0`. All other setup fields remain equal to V6 and are checked against the pinned V6 setup-book hash.

There is one operational qualification to “only the confirmation changed”: V7 also performs explicit finite/positive OHLC, geometry, volume and lineage validation that the historical V6 builder did not apply as strictly. This is a fail-closed data-quality improvement, but it can change admission independently of candle morphology.

### 7.3 Entry trigger

- LONG stop-entry trigger: confirmation high.
- SHORT stop-entry trigger: confirmation low.
- A later one-minute candle must trade through that extreme.
- No same-confirmation-candle fill is allowed.

The intended interpretation is that candle morphology does not predict direction; the later high/low break supplies the direction confirmation.

## 8. Shared selection order

For each day, slot and side:

1. Apply the broad five-minute trend/OI/volume conditions.
2. Apply the setup-specific five-minute price/OI/volume/traded-value thresholds.
3. Apply the version-specific one-minute gate.
4. Rank eligible candidates by the setup picker, descending.
5. Break ties by `traded_value` descending and symbol alphabetically ascending.
6. Keep only the configured cap.

This is gate-before-rank. V7 can therefore select a different symbol from V6, not merely add an order after the V6 selections. Once a selected order remains unfilled, the implemented backtest does not substitute a lower-ranked name later.

## 9. Implemented backtest execution and accounting

The following mechanics are shared by the current V6 and V7 backtests:

- Entry begins on the one-minute candle after confirmation.
- The first forward candle touching the trigger fills at exactly the trigger price.
- The original implementation does not model adverse gap-through entry price, bid/ask spread, tick rounding or stochastic slippage.
- Stop and target are calculated from the theoretical trigger.
- When both stop and target are touched in the same one-minute candle, stop wins.
- If neither bracket is hit, the trade exits at the last stored forward close.
- A flat five-basis-point round-trip cost is deducted once per filled trade.
- An unfilled order contributes no return and no cost.
- The original backtest has no intraday order-expiry rule before square-off.

The backtest return is normalized rather than rupee-sized:

- each filled trade contributes its percentage return after cost;
- returns for the day are added;
- daily returns are cumulatively summed;
- there is no compounding, shared-capital constraint, margin contention, futures-lot rounding or overlapping-position constraint.

The V6 live sizing contract—₹10,000 capital per entry, 5× leverage and approximately ₹50,000 target exposure—is separate and is not used to calculate the backtest tables below.

## 10. Critical result-validity defect

The current forward-path builder takes the next `max_forward_bars` rows and filters them only by `HHMM <= square_off`. It does not require the forward row's trading date to equal the signal date.

Affected code: `fno_oi_ema_confirm_sweep.py`, around lines 304–314.

Consequences:

- early-session signals can contain next-session morning candles;
- an unfilled stop-entry can trigger the next day;
- a trade can hit a stop or target the next day;
- the fallback “square-off” close can be a next-session close rather than the same-day close;
- current V6 and V7 generated performance is invalid for an intraday strategy.

The audit removed **60,997 next-session rows** from the 1,347 five-minute-qualified candidate paths. Almost the entire V7 selected book was exposed to the defect.

Immutable hashes and provenance establish exactly which data and program outputs were used; they do not make a logically incorrect path construction valid.

## 11. Invalidated generated artifacts

These are retained only for traceability and must not be presented as valid strategy performance:

| Generated artifact | Sessions | Orders/fills | Trade PF | Day PF | Net return sum | Why invalid |
|---|---:|---:|---:|---:|---:|---|
| V6 current-source report | 53 | 210/209 | 2.811 | 6.062 | +146.711% | Cross-session-capable forward paths |
| V6 protected legacy selection | 53 | 206/205 | 2.796 | 5.968 | +144.003% | Cross-session-capable forward paths; original source provenance not recoverable |
| V7 generated report | 57 | 440/414 | 1.665 | 3.178 | +132.293% | Cross-session-capable forward paths |

The V6 and V7 rows also cover different end dates, so they would not be an apples-to-apples comparison even without the defect.

## 12. Corrected same-session headline results

### 12.1 Replay contract

The corrected research replay used:

- the frozen V7 physical source snapshot;
- 57 sessions from 2026-05-27 through 2026-08-17;
- 1,347 candidates that passed the fixed V6 five-minute eligibility;
- forward rows restricted to the signal date and time `<= 15:30`;
- no order expiry;
- entry beginning after confirmation;
- exact trigger-price fills with no gap/slippage/tick buffer;
- original V6 per-leg brackets;
- stop-first same-candle ambiguity handling;
- final available same-session close when neither bracket was hit;
- five-basis-point round-trip cost;
- additive percentage-point accounting.

These figures are the most direct corrected comparison of the implemented V6 and V7 strategy definitions. They are **ad-hoc research results**, not yet regenerated canonical reports or provenance artifacts.

### 12.2 Full-period comparison

| Metric | V6 | V7 | Interpretation |
|---|---:|---:|---|
| Sessions | 57 | 57 | Same date set |
| Orders | 222 | 440 | V7 selected 98.2% more orders |
| Fills | 221 | 405 | V7 filled 83.3% more trades |
| Fill rate | 99.5% | 92.0% | V6 selections were more likely to trigger eventually |
| Trade PF | **2.171** | 1.551 | V6 had substantially better payoff quality |
| Net return sum | **+99.17** | +98.82 | Nearly identical additive result |
| Max cumulative daily DD | **5.60** | 16.49 | V7 drawdown was about 2.9× V6 |

### 12.3 Descriptive chronological splits

| Period | Sessions | V6 fills | V6 PF | V6 net | V7 fills | V7 PF | V7 net |
|---|---:|---:|---:|---:|---:|---:|---:|
| Train, through 2026-07-16 | 35 | 130 | 2.315 | +65.86 | 244 | 1.419 | +46.49 |
| Validation, 2026-07-17 to 2026-08-03 | 12 | 63 | 2.305 | +28.23 | 93 | 1.664 | +26.61 |
| Recent, 2026-08-04 to 2026-08-17 | 10 | 28 | 1.392 | +5.07 | 68 | 1.912 | +25.71 |

These labels are descriptive, not clean out-of-sample evidence. The V6 five-minute setup book was selected using history through 2026-08-11, so part of every displayed historical comparison is conditional on prior model selection. The strong ten-session recent V7 result is interesting but too small to justify promotion.

## 13. Exploratory execution sensitivities

The following corrected same-session studies change execution beyond the implemented V6/V7 contracts. They should not be called official V6 or V7 results.

| Research variant | Fills | PF | Net points | Max DD | Comment |
|---|---:|---:|---:|---:|---|
| V6 strict, raw trigger, cancel after 10m | 205 | 2.125 | +90.439 | 5.730 | Preserves quality but gives up late-fill return |
| V7 raw trigger, cancel after 5m | 294 | 1.630 | +83.160 | 13.365 | Lower exposure than no-expiry V7; still weaker than V6 |
| V7 raw trigger, cancel after 10m | 331 | 1.627 | +92.431 | 12.850 | More coverage, still materially larger drawdown |
| V7 trigger 0.05% beyond high/low, cancel after 3m | 212 | 1.820 | +74.967 | 8.56 | Best clean relaxed-quality sensitivity; not implemented V7 |

A more complicated morphology variant—body at least 0.40, adverse wick at most 0.60, 0.05% trigger buffer and five-minute expiry—produced 205 fills, PF 1.683, +62.20 points and 8.54 drawdown. A five-session block bootstrap did not show superiority over strict V6: mean daily difference was -0.649 points with a 95% interval of `[-1.407, +0.092]`.

The research conclusion is therefore not “add more one-minute indicators.” The simple V7 relaxation increased participation but reduced trade quality. Strict V6 remains the stronger benchmark.

### 13.1 Fifteen-basis-point stress

The gap-aware finite-expiry study also stressed a 15-basis-point round trip:

| Research variant | Fills | PF at 15 bps | Net points at 15 bps |
|---|---:|---:|---:|
| V6 strict, cancel after 3m | 186 | 1.787 | +64.339 |
| V6 strict, cancel after 5m | 197 | 1.782 | +67.725 |
| V6 strict, cancel after 10m | 205 | 1.771 | +69.939 |
| V7 raw trigger, cancel after 3m | 260 | 1.358 | +47.028 |
| V7 raw trigger, cancel after 5m | 294 | 1.361 | +53.760 |
| V7 raw trigger, cancel after 10m | 331 | 1.357 | +59.331 |
| V7 0.05% buffered trigger, cancel after 3m | 212 | 1.521 | +53.767 |

These are deliberately conservative execution sensitivities and are not the implemented strategy outputs. They preserve the same qualitative conclusion: V6 retains the higher payoff quality.

## 14. Provenance and artifacts

### 14.1 V6

- Report: `C:\TradingData\eqidv2\fno_oi\latest\latest_fno_oi_ema_confirm_v6_best_net.md`
- Daily: `C:\TradingData\eqidv2\fno_oi\strategy_research\ema_confirm_0925_0930_0935_0940_0945_v6_best_net_daily.csv`
- Trades: `C:\TradingData\eqidv2\fno_oi\strategy_research\ema_confirm_0925_0930_0935_0940_0945_v6_best_net_trades.csv`
- Setups: `C:\TradingData\eqidv2\fno_oi\strategy_research\ema_confirm_0925_0930_0935_0940_0945_v6_best_net_setups.csv`
- Cache: `C:\TradingData\eqidv2\fno_oi\strategy_research\_signal_cache_equity_1m_aggregated_5m_futures_oi_v4\`
- Latest run provenance: `C:\TradingData\eqidv2\fno_oi\strategy_research\backtest_provenance\fno_v6_best_net_20260818T003541391816+0530_199effd6d7aa.json`
- Backtest input fingerprint: `199effd6d7aa430444a33f43fff4530925b131c15e47da226b953cc27687178d`

V6 provenance fingerprints the recreated current whole source files; it does not claim to reconstruct the unavailable original source state used when V6 was first selected.

### 14.2 V7

- Report: `C:\TradingData\eqidv2\fno_oi\latest\latest_fno_oi_ema_confirm_v7_extreme_break.md`
- Daily: `C:\TradingData\eqidv2\fno_oi\strategy_research\ema_confirm_0925_0930_0935_0940_0945_v7_extreme_break_daily.csv`
- Trades: `C:\TradingData\eqidv2\fno_oi\strategy_research\ema_confirm_0925_0930_0935_0940_0945_v7_extreme_break_trades.csv`
- Setups: `C:\TradingData\eqidv2\fno_oi\strategy_research\ema_confirm_0925_0930_0935_0940_0945_v7_extreme_break_setups.csv`
- Dedicated cache: `C:\TradingData\eqidv2\fno_oi\strategy_research\_signal_cache_equity_1m_aggregated_5m_futures_oi_v7_high_low_breakout_v1\`
- Latest run provenance: `C:\TradingData\eqidv2\fno_oi\strategy_research\backtest_provenance\fno_v7_extreme_break_20260818T150757858598+0530_1025a3ad67c9.json`
- Generated-run input fingerprint: `1025a3ad67c998a8160d41ebb743c036de1804690d030a683f6cd8578ff5532d`

V7 uses a separate cache because V6's signal builder removes candles that fail V6 direction before the setup replay. Reusing the V6 cache would silently prevent V7 from seeing the candles it is intended to admit.

### 14.3 V7 frozen physical snapshot

- Manifest: `C:\TradingData\eqidv2\fno_oi\strategy_research\_source_snapshots_v7_high_low_breakout_v1\snapshot_20260818T145532763230+0530_4cdqg0jp\manifest.json`
- Snapshot fingerprint: `4be7500f183d8bf9b23c23d43d13a976ffd645f5d2ad982fc4b48e64c8007bd7`
- Captures: 416/416 mapped source files—208 futures five-minute files and 208 cash-equity one-minute files—plus the dated universe.

The physical snapshot is valuable because the corrected replay can be reproduced from stable bytes after the path engine is repaired. The current cached `paths.npz`, however, already contains the incorrect cross-session slices and must be invalidated.

## 15. Required repair before official rerun

1. Add a same-date condition to every forward-path mask:

   `forward_date == signal_date AND forward_time <= square_off`.

2. Store forward timestamps and opens in the path cache, not only high/low/close.
3. Require strictly increasing, same-session timestamps after confirmation.
4. Define whether a missing exact square-off bar makes a trade/day incomplete or permits the last available same-session close.
5. Model adverse gap-through entry prices rather than always filling at the trigger.
6. Recompute stop and target from the modeled actual fill if matching live behavior.
7. Retain stop-first primary handling for an ambiguous one-minute candle and report an optimistic sensitivity.
8. Version the path policy and cache schema so old V6/V7 caches cannot be reused.
9. Rebuild both caches and regenerate the daily, trade, setup, report and provenance artifacts.
10. Keep corrected artifacts versioned; never overwrite the legacy files without preserving the invalidation record.

## 16. Reproducible rerun commands after repair

V6 current-source replay:

```powershell
python .\fno_oi_ema_confirm_0925_0930_0935_0940_0945_v6.py `
  --through-day 2026-08-17 `
  --split-day 2026-07-17 `
  --cost-bps 5 `
  --square-off 1530 `
  --rebuild-cache
```

V7 frozen-source replay:

```powershell
python .\fno_oi_ema_confirm_0925_0930_0935_0940_0945_v7.py `
  --source-snapshot "C:\TradingData\eqidv2\fno_oi\strategy_research\_source_snapshots_v7_high_low_breakout_v1\snapshot_20260818T145532763230+0530_4cdqg0jp\manifest.json" `
  --through-day 2026-08-17 `
  --split-day 2026-07-17 `
  --cost-bps 5 `
  --square-off 1530 `
  --rebuild-cache
```

These commands must not be used to claim corrected performance until the shared path builder and cache policy have been repaired.

## 17. Methodology limitations

- The V6 setup book was selected using displayed history through 2026-08-11; historical splits are not a final independent test.
- The dated 2026-08-11 universe creates survivorship and universe-selection considerations for earlier sessions.
- Historical OI uses the mapped August futures contract rather than a fully rolling historical near-month series.
- Current source files may contain later repairs or backfills that were not visible live at the original decision time.
- The corrected headline still fills exactly at the trigger and therefore does not model all gaps, spread or slippage.
- Recent frozen equity files generally end at 15:15 rather than the declared 15:30. Same-session correction prevents overnight leakage, but some fallback exits use the last available 15:15 close.
- Net return is an additive sum of trade percentages, not rupee P&L, compounded return or return on a capital-constrained portfolio.
- Multiple positions in the same symbol and overlapping slot positions are treated independently.
- V7 was formulated after observing V6 and 2026-08-18 behavior; it requires prospective shadow evidence before promotion.

## 18. Verification and test coverage

The maintained test tree passed:

- **689 tests passed**;
- **213 subtests passed**.

Covered invariants include:

- exact V6-to-V7 setup cloning;
- universe and output/cache isolation;
- V6 versus V7 confirmation-policy routing;
- V7 admission of opposite-colour and non-displaced candles;
- retention of V6 five-minute filters, pickers, caps and brackets;
- no same-confirmation-candle fill;
- V7 invalid-candle and source-lineage rejection;
- cache invalidation on policy, source or artifact drift;
- physical-snapshot validation and tamper rejection;
- universe/source/provenance fail-closed behavior.

The critical missing regression is the one discovered by this audit: every forward path must remain on the signal date. Additional missing execution tests include adverse gap fills, stored path timestamps/opens, tick rounding, entry expiry, exact square-off completeness and portfolio-capital overlap.

## 19. Bottom line

- **V6:** fewer, higher-quality entries; corrected PF 2.171 and drawdown 5.60 points.
- **V7:** much broader participation; corrected PF 1.551 and drawdown 16.49 points for essentially the same aggregate net.
- **Decision:** retain V6 as the benchmark. Keep V7 research-only until the path engine is fixed, artifacts are regenerated, and a frozen V7 rule survives prospective shadow testing.

A defensible promotion test should begin only after the corrected rule and fingerprints are frozen and should run for at least 20 new sessions and 100 filled orders, whichever takes longer.
