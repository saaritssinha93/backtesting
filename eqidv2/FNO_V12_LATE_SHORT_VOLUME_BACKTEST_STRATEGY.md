# FnO V12 — Late-SHORT Volume Filter, Locked Full-History Backtest

```powershell
cd "C:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2"
python -u fno_v12_backtest.py run --all-usable-history
```

**Profile ID:** `V12_S06_LATE_SHORT_VOLUME_MIN_150` ·
**Stage:** `STAGE_06D_LATE_SHORT_VOLUME` ·
**Family:** `SELECTION_FIVE_MINUTE_VOLUME_MIN` ·
**Schema:** `fno_v12_late_short_volume_150_locked_backtest_v1`

| Hash | Value |
|---|---|
| Profile SHA-256 | `067c5f1c14b7f626b0c112524c2a0c63bc9f379f6d081547bfc747e1c8fa7cbe` |
| Registry SHA-256 | `4948ba186095a5baea6b538a64255bc7304e96720ba98da512d6d21490328c35` |
| Resolved config SHA-256 | `660ab5d2d06290d23e6b39593ddbb5afe03f51e3b6bb714099134eff7481ca4f` |
| Input binding SHA-256 | `78c4d7088f7cf500ec8da587a200314c43cf669a56e2df2aca52b74ec025e62c` |

`headline_valid = false` · `research_only = true` ·
`promotion_eligible = false` · `live_or_paper_authority = false`

Validated run:
`…\v12_s06_late_short_volume_min150_full_history_v1\run_20260831T001454752119+0530`

| | |
|---|---|
| Launcher | [fno_v12_backtest.py](fno_v12_backtest.py) |
| Variant registry | [fno_v12_variant_registry.py](fno_v12_variant_registry.py) |
| Selection runtime | [fno_v12_selection_runtime.py](fno_v12_selection_runtime.py) |
| Execution runtime | [fno_v12_execution_runtime.py](fno_v12_execution_runtime.py) |
| Staged research runner | [fno_v12_staged_backtest.py](fno_v12_staged_backtest.py) |
| Deep-study report generator | [fno_v12_full_historical_report.py](fno_v12_full_historical_report.py) |
| Parent V11 | [fno_v11_backtest.py](fno_v11_backtest.py) |
| **Execution engine** | [fno_v8_windowed_1m_entry_backtest.py](fno_v8_windowed_1m_entry_backtest.py) |
| Source report | [report_v12.md](report_v12.md) · assets in `report_v12_assets/` |

---

## 1. The whole change, in one line

> **V12 raises the five-minute volume-ratio floor on two SHORT legs — 09:40 and 09:45 — from 1.00 to 1.50 inclusive. Nothing else changes.**

No new runtime mechanism. No new entry timing. No new portfolio rule. No new
exit rule. The profile's own `execution_stack` says so:

```
V11_FIXED_RUNTIME_OUTER      → V11's S+3 rule and same-side-max-2 ledger
V12_NEUTRAL_RUNTIME_INNER    → V12 adds NO runtime mechanism
V11_STRONG_IDENTITY_GAP2_INNERMOST  → V11's strong-reference 2 bps gap guard
```

### Headline against its two ancestors

| Strategy | Sessions | Fills | W–L | Win rate | PF | Net points | Net P&L | Max daily DD |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| V10 frozen | 65 | 232 | 116–116 | 50.00% | 1.8327 | +73.0544 | +Rs 36,312.05 | 9.3513 |
| V11 frozen | 65 | 237 | 123–114 | 51.90% | 2.1452 | +94.6309 | +Rs 46,783.23 | 8.5674 |
| **V12 selected** | 65 | **229** | 120–109 | **52.40%** | **2.2356** | **+96.4444** | **+Rs 47,503.84** | **5.2693** |

**The real story is drawdown.** Net points move +1.81 (+1.9%) and P&L +Rs 721
(+1.5%) — both marginal. Maximum daily drawdown falls from **8.5674 to 5.2693
points, a 38.5% reduction**, on eight fewer trades. V12 is a risk-shaping
change dressed as a return improvement.

> ⚠ **And the incremental edge is not statistically decisive.** The paired
> bootstrap over 65 sessions gives P(Δ > 0) = **0.695** with a 95% interval of
> **[−4.11, +8.55] points** — it crosses zero. Only **8 of 65 daily results
> differ** from V11.

---

## 2. Exact configuration

### 2.1 Global overlays and economics

| Layer | Parameter | Value | Scope |
|---|---|---|---|
| Identity | V12 profile | `V12_S06_LATE_SHORT_VOLUME_MIN_150` | locked standalone |
| **Selection** | **09:40 SHORT minimum volume ratio** | **1.50 inclusive** | **V12 change** |
| **Selection** | **09:45 SHORT minimum volume ratio** | **1.50 inclusive** | **V12 change** |
| Selection | 09:40 LONG directional move floor | 0.40% inclusive | inherited V11 |
| Selection | 09:35 LONG directional move ceiling | 0.50% inclusive | inherited V11 |
| Ranking | Rerank after selection | `True` | each setup/side/slot |
| 1m timing | 09:30 SHORT earliest trigger-fill | S+3 | inherited V11 |
| Gap | Maximum adverse trigger gap | 2 bps | strong-identity gap events |
| Portfolio | Same symbol + same side concurrent limit | 2 | all setups |
| Portfolio | Same symbol + opposite side | prohibited | all setups |
| Portfolio | Modeled capital | Rs 120,000 | proxy global ledger |
| Portfolio | Margin reservation per entry | Rs 10,000 | proxy global ledger |
| Sizing | Target cash-equivalent exposure | Rs 50,000 | `quantity = floor(exposure / entry)` |
| Exit | Same-bar collision | `STOP_FIRST` | conservative OHLC rule |
| Exit | Square-off clock | 15:30 | when a real bar exists |
| Exit | Terminal policy | `LAST_REAL_BAR_SENSITIVITY` | partial-path sensitivity |
| Costs | Reference | 15 bps + 0 bps entry slippage | headline |
| Costs | Stress | 20 bps + 2 bps entry slippage | sensitivity |
| Costs | Harsh | 25 bps + 5 bps entry slippage | sensitivity |

### 2.2 Five-minute selection book — all ten legs

| Setup | Side | Cap | Picker | EMA rule | Effective move rule | OI ≥ | **Vol ≥** | Min TV | V12 changed |
|---|---|---:|---|---|---|---:|---:|---:|---|
| 09:25_LONG | LONG | 4 | max_move | EMA9>20>50 | ≥ +0.30% | 0.10% | 3.00 | 0 | — |
| 09:25_SHORT | SHORT | 4 | max_move | EMA9<20<50 | ≤ −0.20% | 0.10% | 1.50 | Rs 2.5 cr | — |
| 09:30_LONG | LONG | 1 | max_move | EMA9>20>50 | ≥ +0.65% | 0.10% | 1.00 | 0 | — |
| 09:30_SHORT | SHORT | 4 | max_volume | EMA9<20<50 | ≤ −0.20% | 1.00% | 1.00 | Rs 2.5 cr | — |
| 09:35_LONG | LONG | 1 | max_liquidity | EMA9>20>50 | ≥ +0.20% **and ≤ +0.50%** | 0.10% | 1.00 | 0 | — |
| 09:35_SHORT | SHORT | 2 | max_liquidity | EMA9<20<50 | ≤ −0.50% | 1.00% | 1.00 | 0 | — |
| 09:40_LONG | LONG | 1 | max_liquidity | EMA9>20>50 | **≥ +0.40%** | 0.10% | 2.00 | 0 | — |
| **09:40_SHORT** | SHORT | 1 | max_move | EMA9<20<50 | ≤ −0.20% | 0.10% | **1.50** | 0 | **1.00 → 1.50** |
| 09:45_LONG | LONG | 1 | max_move | EMA9>20>50 | ≥ +0.65% | 0.10% | 1.00 | 0 | — |
| **09:45_SHORT** | SHORT | 1 | max_volume | EMA9<20<50 | ≤ −0.20% | 0.75% | **1.50** | 0 | **1.00 → 1.50** |

`max_entries` is a **setup/side/slot cap, not a daily cap**. LONG and SHORT
buckets are independent. Candidates are ranked by the setup picker, then the
portfolio ledger applies chronological reservations.

### 2.3 One-minute confirmation and trade book

| Setup | Body ≥ | Adv wick ≤ | CLV ≥ | Max conf min | Earliest fill | Buffer bps | Midpoint | Expiry | Stop % | Target % | R:R |
|---|---:|---:|---|---:|---:|---:|---|---:|---:|---:|---:|
| 09:25_LONG | 0.0000 | 0.5000 | — | 3 | 2 | 0.0 | No | 5 | 0.40 | 1.00 | 1:2.5 |
| 09:25_SHORT | 0.6000 | 0.6000 | — | 3 | 2 | 2.0 | No | 5 | 0.50 | 3.00 | 1:6.0 |
| 09:30_LONG | 0.5000 | 0.5000 | — | 1 | 2 | 0.0 | No | 5 | 1.00 | 2.50 | 1:2.5 |
| 09:30_SHORT | 0.4500 | 0.3000 | **0.5000** | 3 | **3** | 0.0 | **Yes** | 5 | 1.00 | 4.00 | 1:4.0 |
| 09:35_LONG | 0.6000 | 0.5000 | — | 1 | 2 | 0.0 | No | 5 | 1.00 | 2.50 | 1:2.5 |
| 09:35_SHORT | 0.4000 | 0.5000 | — | 1 | 2 | 0.0 | No | 5 | 1.00 | 3.00 | 1:3.0 |
| 09:40_LONG | 0.5000 | 0.5000 | — | 1 | 2 | 0.0 | No | 5 | 0.50 | 2.50 | 1:5.0 |
| 09:40_SHORT | 0.4000 | 0.5000 | — | 1 | 2 | 0.0 | No | 5 | 1.00 | 3.00 | 1:3.0 |
| 09:45_LONG | 0.4000 | 0.5000 | — | 1 | 2 | 0.0 | No | 5 | 1.00 | 3.00 | 1:3.0 |
| 09:45_SHORT | 0.4000 | 0.3000 | — | 1 | 2 | 0.0 | No | 5 | 1.00 | 2.00 | 1:2.0 |

All ten legs share `post_confirmation_cancel = Yes`,
`allow_cap_reassignment = Yes`, `same_bar_policy = STOP_FIRST`.

### 2.4 Indicator definitions and causality

| Feature | Formula | Causal note |
|---|---|---|
| 5m construction | exactly five valid end-labelled 1m rows; O/H/L/C/V = first/max/min/last/sum | completed slot only |
| EMA9/20/50 | `close.ewm(span=N, adjust=False).mean()` | cash-equity 5m closes through S |
| `price_change_pct` | `100 × (C[S] / C[S−5m] − 1)` | side-aware threshold |
| `oi_change_pct` | `100 × (OI[S] / OI[S−5m] − 1)` | exact preceding futures 5m timestamp |
| `volume_ratio` | `V[S] / mean(V[S−20..S−1])`, `min_periods=5` | **current volume excluded from the denominator** |
| `traded_value` | cash-equity `C[S] × V[S]` | liquidity picker / minimum |
| Broad base gates | move ≥ 0.10%, OI change ≥ 0.05%, volume ratio ≥ 0.80 | every setup threshold is equal or stricter |
| Confirmation body ratio | `abs(C−O)/(H−L)` | completed S+N 1m candle |
| LONG adverse wick | `(H − max(O,C))/(H−L)` | SHORT mirrors on the lower wick |
| Directional close location | LONG `(C−L)/(H−L)`; SHORT `(H−C)/(H−L)` | higher is stronger |
| Entry trigger | LONG confirmation H + buffer; SHORT confirmation L − buffer; tick-rounded | **cannot fill on the confirmation bar** |
| Stop / target | `actual fill × (1 ± setup pct)`, adversely tick-rounded | `STOP_FIRST` if both touch in one OHLC bar |
| Quantity | `floor(Rs 50,000 / cash-equity entry price)` | **not futures lot sizing** |
| Net return | side-aware gross return % − `cost_bps/100` | configured slippage affects **entry only** |
| PF | `sum(positive net-return points) / abs(sum(negative net-return points))` | trade-return points, not account PF |

> **Note the OI asymmetry:** futures OI drives *selection*, while price, EMA,
> volume, confirmation, entry, stop, target and P&L all use **NSE cash-equity
> bars**. This is inherited from the whole V6→V12 lineage.

---

## 3. Selection-to-exit funnel

### 3.1 Overall

| Step | Count | Share of prior |
|---|---:|---:|
| Base 5-minute candidates | 1,241 | — |
| After V12 filters | 1,017 | 81.9% |
| 1-minute confirmed | 383 | 37.7% |
| Filled | 229 | 59.8% |
| Winners | 120 | 52.4% |

**Only 18% of base candidates are removed by selection; 62% of survivors then
fail the one-minute confirmation gate.** Confirmation remains the dominant
filter, exactly as in V8/V10/V11.

### 3.2 The four selection rejections

| Rejection reason | Setup | Rejections | Affected sessions | Median move | Median vol ratio |
|---|---|---:|---:|---:|---:|
| `REJECTED:MOVE_0935_LONG_MAX` | 09:35_LONG | 77 | 40 | 0.67% | 2.7574 |
| `REJECTED:MOVE_0940_LONG_MIN` | 09:40_LONG | 30 | 22 | 0.28% | 2.4443 |
| **`REJECTED:VOLUME_0940_SHORT_MIN`** | 09:40_SHORT | **81** | 34 | −0.33% | 1.2632 |
| **`REJECTED:VOLUME_0945_SHORT_MIN`** | 09:45_SHORT | **36** | 23 | −0.36% | 1.1982 |

```
1,241 base
  − 77  (09:35 LONG ceiling, inherited V10)
  − 30  (09:40 LONG floor,   inherited V10 Stage 7)
  = 1,134  ← this is exactly V11's selected count
  − 81  (09:40 SHORT volume ≥ 1.50, NEW)
  − 36  (09:45 SHORT volume ≥ 1.50, NEW)
  = 1,017  ← V12
```

The two new filters remove **117 candidates**, and their median rejected volume
ratios (1.26 and 1.20) sit just under the new 1.50 floor — these are marginal
participation shorts, not obvious outliers.

### 3.3 By setup

| Setup | Base | Selected | Confirmed | Fills | W | L | Win % | PF | Net points | Net P&L |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 09:25_LONG | 122 | 122 | 78 | 61 | 32 | 29 | 52.46% | 1.6844 | +11.0095 | +Rs 5,289.78 |
| 09:25_SHORT | 261 | 261 | 114 | 62 | 28 | 34 | 45.16% | 2.0559 | +22.3665 | +Rs 10,997.77 |
| 09:30_LONG | 65 | 65 | 17 | 11 | 5 | 6 | 45.45% | 1.6174 | +3.5120 | +Rs 1,709.49 |
| 09:30_SHORT | 101 | 101 | 41 | 19 | 12 | 7 | 63.16% | 3.3855 | +14.7457 | +Rs 7,157.02 |
| 09:35_LONG | 248 | 171 | 35 | 17 | 9 | 8 | 52.94% | 1.5052 | +3.7417 | +Rs 1,914.06 |
| 09:35_SHORT | 36 | 36 | 15 | 10 | 6 | 4 | 60.00% | 3.1215 | +9.7864 | +Rs 4,914.43 |
| 09:40_LONG | 106 | 76 | 25 | 18 | 9 | 9 | 50.00% | 3.1108 | +12.3896 | +Rs 5,942.03 |
| **09:40_SHORT** | 178 | **97** | 34 | **15** | 8 | 7 | 53.33% | **1.8434** | **+6.1070** | **+Rs 3,311.43** |
| 09:45_LONG | 36 | 36 | 13 | 9 | 6 | 3 | 66.67% | 4.7245 | +8.7639 | +Rs 4,366.10 |
| **09:45_SHORT** | 88 | **52** | 11 | **7** | 5 | 2 | 71.43% | **3.8060** | **+4.0221** | **+Rs 1,901.73** |

Both targeted legs improved: **09:40 SHORT** went from V11's 18 fills / PF 1.506
/ +Rs 2,671 to **15 fills / PF 1.843 / +Rs 3,311**; **09:45 SHORT** from 12
fills / PF 1.940 / +Rs 1,822 to **7 fills / PF 3.806 / +Rs 1,902**. Every other
leg is byte-identical to V11.

### 3.4 By side, slot, picker and rank

| Side | Base | Selected | Confirmed | Fills | Win % | PF | Net points | Net P&L |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| LONG | 577 | 470 | 168 | 116 | 52.59% | 2.0538 | +39.4167 | +Rs 19,221.46 |
| SHORT | 664 | 547 | 215 | 113 | 52.21% | 2.4029 | +57.0277 | +Rs 28,282.38 |

| Slot | Fills | Win % | PF | Net points | Net P&L |
|---|---:|---:|---:|---:|---:|
| 09:25 | 123 | 48.78% | 1.8956 | +33.3760 | +Rs 16,287.55 |
| 09:30 | 30 | 56.67% | 2.5382 | +18.2576 | +Rs 8,866.51 |
| 09:35 | 27 | 55.56% | 2.1255 | +13.5281 | +Rs 6,828.50 |
| 09:40 | 33 | 51.52% | 2.4108 | +18.4966 | +Rs 9,253.46 |
| 09:45 | 16 | 68.75% | 4.3768 | +12.7860 | +Rs 6,267.83 |

| Picker | Fills | Win % | PF | Net points | Net P&L |
|---|---:|---:|---:|---:|---:|
| max_move | 158 | 50.00% | 1.9849 | +51.7589 | +Rs 25,674.57 |
| max_liquidity | 45 | 53.33% | 2.4488 | +25.9177 | +Rs 12,770.52 |
| max_volume | 26 | 65.38% | 3.4647 | +18.7678 | +Rs 9,058.75 |

| Frozen rank | Selected | Fills | Win % | PF | Net points | Net P&L |
|---|---:|---:|---:|---:|---:|---:|
| 1 | 378 | 106 | 53.77% | 2.2786 | +46.4343 | +Rs 22,873.74 |
| 2 | 224 | 44 | 45.45% | 2.3529 | +21.1632 | +Rs 10,440.33 |
| **3** | 138 | 28 | 39.29% | **0.6892** | **−4.2588** | **−Rs 2,018.32** |
| 4 | 90 | 23 | 47.83% | 2.6235 | +12.3157 | +Rs 6,052.50 |
| 5 | 64 | 14 | 71.43% | 4.6627 | +10.7921 | +Rs 5,374.92 |
| 6+ | 123 | 14 | 78.57% | 6.3776 | +9.9979 | +Rs 4,780.66 |

> **Rank performance is non-monotonic.** Rank 3 is the only losing bucket while
> ranks 4, 5 and 6+ are strongly profitable. A weak observed rank cannot safely
> become a blacklist; any rank-margin hypothesis needs setup-stratified
> prospective replay.

### 3.5 Terminal states

| Status | Count | Share | Reason breakdown |
|---|---:|---:|---|
| `NO_CONFIRMATION` | 602 | 59.19% | `CONFIRMATION_WINDOW_EXPIRED` 602 |
| `STOPPED` | 97 | 9.54% | `STOP` 97 |
| `POSTCONF_CANCELLED` | 97 | 9.54% | `CLOSE_REVERSED_THROUGH_SIGNAL_CLOSE` 75 + `ADVERSE_GAP_GUARD_REJECTED` 22 |
| `SQUARE_OFF` | 66 | 6.49% | `LAST_REAL_BAR_SENSITIVITY` 66 |
| `TARGETED` | 66 | 6.49% | `TARGET` 66 |
| `WINDOW_EXPIRED` | 54 | 5.31% | `ENTRY_WINDOW_EXPIRED` 54 |
| `PRECONF_INVALIDATED` | 32 | 3.15% | `CLOSE_CROSSED_FIVE_MINUTE_MIDPOINT` 32 |
| `DUPLICATE_REJECTED` | 3 | 0.29% | `DUPLICATE_SYMBOL_SAME_SIDE_LIMIT_2:CONSERVATIVE_NO_BACKFILL` 3 |

### 3.6 One-minute rejection codes

| Code | Occurrences |
|---|---:|
| `CLOSE_NOT_BEYOND_FIVE_MINUTE_CLOSE` | 860 |
| `WRONG_CANDLE_DIRECTION` | 781 |
| `BODY_RATIO_BELOW_MINIMUM` | 697 |
| `ADVERSE_WICK_RATIO_ABOVE_MAXIMUM` | 279 |
| `CLOSE_LOCATION_BELOW_MINIMUM` | 87 |
| `PRECONF_MIDPOINT_INVALIDATED` | 32 |
| `NONPOSITIVE_RANGE` | 1 |

Counts are failed-check occurrences across monitored candles; codes overlap and
one candidate can contribute more than once.

---

## 4. Results

### 4.1 Cost and slippage robustness

| Scenario | Cost bps | Slip bps | Fills | W–L | Win % | PF | Net points | Net P&L | Max DD | P&L retained |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| **REFERENCE_15_0** | 15.0 | 0.0 | 229 | 120–109 | 52.40% | 2.2356 | +96.4444 | +Rs 47,503.84 | 5.2693 | 100.00% |
| STRESS_20_2 | 20.0 | 2.0 | 229 | 116–113 | 50.66% | 1.9423 | +80.3389 | +Rs 39,710.99 | 6.2661 | 83.60% |
| STRESS_25_5 | 25.0 | 5.0 | 229 | 111–118 | **48.47%** | 1.6286 | +59.8045 | +Rs 29,759.08 | 7.2787 | **62.65%** |

**Fill count is identical across all three** — slippage changes the fill price,
not whether the trigger was touched. Under the harshest case, win rate crosses
below 50% and 37% of P&L evaporates, but the result stays positive. Break-even
would need an **extra 42.78 bps** on fixed notional.

### 4.2 Period slices

| Period | Sessions | Fills | Win % | PF | Net points | Net P&L | Payoff ratio |
|---|---:|---:|---:|---:|---:|---:|---:|
| FULL_65 | 65 | 229 | 52.40% | 2.2356 | +96.4444 | +Rs 47,503.84 | 2.0307 |
| CORE_59 | 59 | 207 | 53.14% | 2.2898 | +90.3549 | +Rs 44,480.16 | 2.0192 |
| FORWARD_6 | 6 | 22 | 45.45% | 1.7612 | +6.0895 | +Rs 3,023.68 | 2.1134 |
| FIRST_HALF_32 | 32 | 93 | 55.91% | 2.5152 | +49.2326 | +Rs 24,233.24 | 1.9832 |
| SECOND_HALF_33 | 33 | 136 | 50.00% | 2.0362 | +47.2118 | +Rs 23,270.60 | 2.0362 |
| LAST_14_USABLE | 14 | 41 | 46.34% | 1.8290 | +12.6005 | +Rs 6,428.26 | 2.1177 |

The half-split is nearly even (+49.23 vs +47.21), but win rate decays from
55.91% to 50.00% and the last 14 sessions are the weakest slice.

### 4.3 Monthly

| Month | Sessions | Fills | Win % | PF | Net points | Net P&L |
|---|---:|---:|---:|---:|---:|---:|
| 2026-05 | 2 | 6 | 66.67% | 7.3628 | +7.0839 | +Rs 3,498.36 |
| 2026-06 | 21 | 46 | 45.65% | 1.4383 | +9.2688 | +Rs 4,548.33 |
| **2026-07** | 23 | 117 | 58.12% | **2.9004** | **+63.8337** | **+Rs 31,110.64** |
| 2026-08 | 19 | 60 | 45.00% | 1.7322 | +16.2580 | +Rs 8,346.51 |

**July supplies 66.19% of net points on 35% of sessions.** Same concentration as
every generation before it.

### 4.4 Consecutive ten-session blocks

| Block | Sessions | Fills | Win % | PF | Net points | Net P&L |
|---|---:|---:|---:|---:|---:|---:|
| B1: 05-27 … 06-10 | 10 | 24 | 54.17% | 2.2134 | +12.0071 | +Rs 5,848.40 |
| **B2: 06-11 … 06-24** | 10 | 16 | **31.25%** | **0.8255** | **−1.4136** | **−Rs 605.69** |
| B3: 06-25 … 07-09 | 10 | 43 | 62.79% | 3.1036 | +28.0691 | +Rs 13,759.31 |
| B4: 07-10 … 07-23 | 10 | 54 | 53.70% | 2.6291 | +26.5076 | +Rs 12,807.87 |
| B5: 07-24 … 08-06 | 10 | 50 | 52.00% | 2.1694 | +17.8247 | +Rs 8,873.06 |
| B6: 08-07 … 08-20 | 10 | 22 | 45.45% | 1.6336 | +5.6474 | +Rs 2,950.07 |
| B7: 08-21 … 08-28 | 5 | 20 | 50.00% | 2.2409 | +7.8020 | +Rs 3,870.82 |

**One of seven blocks is negative.** B2 is the only losing stretch.

### 4.5 Weekday

| Weekday | Sessions | Fills | Win % | PF | Net points | Net P&L |
|---|---:|---:|---:|---:|---:|---:|
| Monday | 13 | 40 | 47.50% | 2.2525 | +15.7277 | +Rs 7,745.57 |
| Tuesday | 13 | 52 | 53.85% | 2.2525 | +22.2822 | +Rs 11,160.08 |
| Wednesday | 13 | 47 | 55.32% | 2.6393 | +26.2626 | +Rs 12,886.43 |
| Thursday | 13 | 50 | 48.00% | 1.5985 | +12.0863 | +Rs 5,806.61 |
| Friday | 13 | 40 | 57.50% | 2.7478 | +20.0855 | +Rs 9,905.16 |

Every weekday is positive; Thursday is weakest. **13 sessions each — far too few
to act on.**

### 4.6 Daily regime buckets

| Dimension | Regime | Sessions | Fills | Pos/Neg days | Net points | Avg daily P&L |
|---|---|---:|---:|---:|---:|---:|
| candidate_activity | LOW (1–11) | 26 | 47 | 7 / 16 | **−4.6743** | −Rs 75.53 |
| candidate_activity | MID (12–17) | 20 | 59 | 16 / 3 | +33.5101 | +Rs 832.27 |
| candidate_activity | HIGH (18–48) | 19 | 123 | 14 / 5 | +67.6085 | +Rs 1,727.49 |
| five_min_range | LOW | 22 | 63 | 11 / 10 | +21.3102 | +Rs 478.13 |
| five_min_range | MID | 21 | 88 | 13 / 6 | +46.1432 | +Rs 1,084.92 |
| five_min_range | HIGH | 22 | 78 | 13 / 8 | +28.9910 | +Rs 645.53 |
| long_share | LOW | 22 | 90 | 13 / 8 | +59.8696 | +Rs 1,324.90 |
| long_share | MID | 21 | 67 | 12 / 8 | +15.7431 | +Rs 377.09 |
| long_share | HIGH | 22 | 72 | 12 / 8 | +20.8317 | +Rs 474.41 |

**Low-breadth days are net negative.** That is a market-regime *hypothesis*, not
evidence for a same-history minimum-breadth threshold.

### 4.7 Exits

| Exit reason | Fills | W | L | Win % | PF | Net points | Net P&L | Avg win | Avg loss |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| TARGET | 66 | 66 | 0 | 100.00% | ∞ | +117.7871 | +Rs 57,126.54 | +1.7847 | — |
| **LAST_REAL_BAR_SENSITIVITY** | 66 | 54 | 12 | 81.82% | 14.1031 | **+52.6902** | +Rs 25,934.31 | +1.0502 | −0.3351 |
| STOP | 97 | 0 | 97 | 0.00% | 0.0000 | −74.0329 | −Rs 35,557.02 | — | −0.7632 |

> ⚠ **The 66 last-real-bar exits supply +52.69 points — 54.63% of the total.**
> These are not clean 15:30 square-offs; they are positions closed at the last
> real bar available in a partial path.

**Economic dependence on the terminal policy:**

| Terminal view | Fills | Win % | PF | Net points | Net P&L |
|---|---:|---:|---:|---:|---:|
| ALL_REFERENCE_TRADES | 229 | 52.40% | 2.2356 | +96.4444 | +Rs 47,503.84 |
| **TARGET_AND_STOP_ONLY** | 163 | 40.49% | **1.5910** | **+43.7542** | **+Rs 21,569.52** |
| ALL_LAST_REAL_BAR | 66 | 81.82% | 14.1031 | +52.6902 | +Rs 25,934.31 |
| LAST_REAL_BAR_AT_1530 | 55 | 83.64% | 16.6006 | +44.4024 | +Rs 21,837.62 |
| LAST_REAL_BAR_AT_1515 | 11 | 72.73% | 8.0534 | +8.2877 | +Rs 4,096.70 |

Strip out every last-real-bar exit and the strategy still works — **PF 1.59,
+Rs 21,570** — but it is materially weaker. Path coverage: **238 selected
candidates stop at 15:15, 779 at 15:30.**

### 4.8 Holding duration

| Holding | Fills | Win % | PF | Net points | Net P&L |
|---|---:|---:|---:|---:|---:|
| ≤ 5m | 40 | **15.00%** | **0.2355** | **−16.4784** | **−Rs 8,064.56** |
| 6–15m | 28 | 50.00% | 1.9505 | +9.6703 | +Rs 4,827.74 |
| 16–30m | 20 | 45.00% | 1.0203 | +0.1516 | +Rs 172.89 |
| 31–60m | 19 | 52.63% | 2.8580 | +14.2726 | +Rs 6,861.60 |
| 61–120m | 20 | 40.00% | 1.3915 | +4.4327 | +Rs 2,552.99 |
| **120m+** | 102 | **71.57%** | **5.2529** | **+84.3956** | **+Rs 41,153.17** |

The shape is stark: fast exits are stop-outs (15% win rate), and **the entire
edge lives in trades held beyond two hours**. Holding time is a *realized
outcome*, not an entry feature — it cannot be used as a filter.

### 4.9 Excursion and missed-runner diagnostics

| Outcome | Trades | Median MFE lower | Median MFE upper | Median MAE lower | Median MAE upper | Median net R | Median hold |
|---|---:|---:|---:|---:|---:|---:|---:|
| LOSS | 109 | 0.35% | 0.35% | 0.50% | 0.56% | −1.2981 | 21 min |
| WIN | 120 | 1.76% | 1.81% | 0.22% | 0.25% | +2.1029 | 276.5 min |

**Losers that reached favourable R before failing:**

| Cohort | Threshold | Trades | Cohort | Share |
|---|---:|---:|---:|---:|
| All losers (upper bound) | 0.25 R | 80 | 109 | 73.39% |
| All losers (upper bound) | 0.50 R | 62 | 109 | 56.88% |
| All losers (upper bound) | 1.00 R | 32 | 109 | 29.36% |
| Stop exits (lower bound) | 1.00 R | 24 | 97 | 24.74% |

A quarter of stopped trades had already banked 1 R on a conservative bound.
That looks like a break-even-stop opportunity — **and the report explicitly
blocks that test**, because 224 of 229 fills have boundary-ambiguous excursion
paths (median MFE bound width 0.00%, MAE 0.01%, but entry-bar ambiguous 206,
exit-bar ambiguous 161).

### 4.10 Gap guard, portfolio, exposure

| Gap path | Candidates | Fills | Median adverse gap | Win % | PF | Net points |
|---|---:|---:|---:|---:|---:|---:|
| NO_GAP_OBSERVED | 972 | 206 | — | 52.91% | 2.2623 | +86.6170 |
| GAP_ACCEPTED | 23 | 23 | 0.61 bps | 47.83% | 2.0417 | +9.8274 |
| **GAP_REJECTED** | 22 | 0 | **9.51 bps** | — | — | +0.0000 |

> The rejected gaps had a median adverse distance of **9.51 bps** — well beyond
> the 2 bps cap. But **a real resting stop-market order cannot reject a gap
> after the opening price is observed.** The Gap2 rule needs an explicitly
> executable synthetic-trigger or stop-limit design before live use.

| Portfolio view | Fills | Win % | PF | Net points | Net P&L |
|---|---:|---:|---:|---:|---:|
| ACTUAL_CAP2_LEDGER | 229 | 52.40% | 2.2356 | +96.4444 | +Rs 47,503.84 |
| UNCONSTRAINED_CANDIDATE_OUTCOMES | 232 | 51.72% | 2.1870 | +94.7084 | +Rs 46,652.79 |

The ledger **added** +1.74 points by blocking three duplicate-symbol
reservations (LTM ×2 on 06-03, SHRIRAMFIN on 08-06) whose unconstrained
outcomes were −0.07%, −0.52% and −1.15%.

| Max open positions | Max deployed notional | Median deployed when active | Capital | Margin/entry | Max reservations | Same-symbol same-side |
|---:|---:|---:|---:|---:|---:|---:|
| 8 | Rs 394,734.05 | Rs 95,940.00 | Rs 120,000 | Rs 10,000 | 12 | 2 |

**Peak concurrency was 8, never the 12-slot cap** — the ledger was not the
binding constraint in this history.

### 4.11 Concentration

| Metric | Value |
|---|---:|
| Unique symbols | 122 |
| Positive / negative symbols | 70 / 52 |
| One-fill symbols | 59 |
| Top-5 positive symbols' share of net | 30.69% |
| Best-5 days' share of net | 47% |
| Best-10 trades' share of net | 31.60% |
| Absolute symbol-points HHI | 0.0150 |

Symbol concentration is low (HHI 0.015), but **the best five days carry 47% of
the result**. Top symbols: PAYTM +Rs 3,309, OFSS +Rs 3,146, MCX +Rs 3,084.
Worst: KPITTECH −Rs 1,146, DMART −Rs 1,091, POWERINDIA −Rs 995.

### 4.12 Risk

| Metric | Value |
|---|---:|
| Best day | 2026-07-07, +Rs 5,602.68 |
| Worst day | 2026-07-09, −Rs 1,192.41 |
| Average / median daily P&L | +Rs 730.83 / +Rs 349.48 |
| Daily P&L std | Rs 1,546.47 |
| Max consecutive positive / negative days | 5 / 2 |
| Max consecutive winning / losing trades | 7 / 5 |
| Max drawdown | 5.2693 points / Rs 2,591.12 |
| Win rate Wilson 95% CI | 45.95% – 58.78% |
| Extra break-even cost | 42.78 bps |

**Ten drawdown episodes, all recovered.** The deepest ran 16 sessions
(2026-06-03 → 06-18, recovered 06-29) at Rs 2,591.

---

## 5. V12 versus V11 — exact mechanism accounting

### 5.1 The eight changed sessions

| Session | Fills V11 → V12 | Net % V11 | Net % V12 | Δ points | Δ P&L | Cumulative Δ |
|---|---|---:|---:|---:|---:|---:|
| 2026-05-27 | 4 → 3 | 2.7824 | 1.8217 | **−0.9607** | −Rs 479.01 | −0.9607 |
| 2026-06-09 | 4 → 2 | −2.3166 | −0.0141 | **+2.3025** | +Rs 966.39 | +1.3418 |
| 2026-06-10 | 1 → 0 | −0.9956 | 0.0000 | +0.9956 | +Rs 488.38 | +2.3374 |
| 2026-06-29 | 7 → 6 | 5.8985 | 4.8608 | −1.0377 | −Rs 501.16 | +1.2997 |
| 2026-07-09 | 4 → 3 | −2.5823 | −2.4543 | +0.1280 | +Rs 58.15 | +1.4277 |
| 2026-07-10 | 7 → 6 | 4.0132 | 5.1675 | +1.1544 | +Rs 570.08 | +2.5821 |
| 2026-07-14 | 5 → 5 | −1.5368 | −1.5298 | +0.0070 | +Rs 3.39 | +2.5891 |
| 2026-07-20 | 5 → 4 | −0.4791 | −1.2547 | −0.7756 | −Rs 385.62 | **+1.8135** |

**57 of 65 sessions are economically identical.** Five improved, three worsened.

### 5.2 Mechanism decomposition

| Mechanism | Count | W | L | PF | Δ points | Δ P&L |
|---|---:|---:|---:|---:|---:|---:|
| V11 selected candidates excluded by V12 late-SHORT filters | 117 | — | — | — | — | — |
| **V11 fills removed by V12** | **8** | 3 | 5 | **0.6056** | **+1.8065** | +Rs 717.21 |
| V12 fills added versus V11 | **0** | — | — | — | +0.0000 | +Rs 0.00 |
| Common-fill economics changed after reranking/ledger ordering | 1 | — | — | — | +0.0070 | +Rs 3.39 |
| **Total V12 − V11** | 8 | — | — | — | **+1.8135** | **+Rs 720.61** |

> **V12 adds nothing. It only subtracts.** Of 117 excluded candidates, only 8
> would have become fills; those 8 had 3 wins and 5 losses with a combined
> PF of 0.6056. Removing them is the whole change.

The eight removed fills, individually:

| Candidate | Session | Setup | Symbol | V11 net % | Δ points |
|---|---|---|---|---:|---:|
| RVNL | 2026-05-27 | 09:45_SHORT | RVNL | +0.9607 | −0.9607 |
| INFY | 2026-06-09 | 09:40_SHORT | INFY | −1.1525 | +1.1525 |
| FORCEMOT | 2026-06-09 | 09:45_SHORT | FORCEMOT | −1.1500 | +1.1500 |
| MUTHOOTFIN | 2026-06-10 | 09:45_SHORT | MUTHOOTFIN | −0.9956 | +0.9956 |
| TATAELXSI | 2026-06-29 | 09:40_SHORT | TATAELXSI | +1.0377 | −1.0377 |
| POLYCAB | 2026-07-09 | 09:45_SHORT | POLYCAB | −0.1280 | +0.1280 |
| DRREDDY | 2026-07-10 | 09:40_SHORT | DRREDDY | −1.1544 | +1.1544 |
| BHEL | 2026-07-20 | 09:45_SHORT | BHEL | +0.7756 | −0.7756 |
| WAAREEENER | 2026-07-14 | 09:40_SHORT | WAAREEENER | *kept*, economics changed | +0.0070 |

### 5.3 V11 counterfactual for excluded candidates

| Setup | Removed | V11 confirmed | Fills | W | L | Win % | PF | Net points |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| 09:40_SHORT | 81 | 16 | 3 | 1 | 2 | 33.33% | 0.4498 | −1.2692 |
| 09:45_SHORT | 36 | 10 | 5 | 2 | 3 | 40.00% | 0.7637 | −0.5373 |

These are V11-control outcomes for candidates removed by V12, **not** outcomes
observed after rejection. Portfolio displacement means their arithmetic total is
descriptive rather than a causal decomposition.

### 5.4 Statistical uncertainty of the improvement

| Scenario | Paired sessions | Observed Δ | Bootstrap P2.5 | Median | P97.5 | P(Δ > 0) | +/−/0 sessions |
|---|---:|---:|---:|---:|---:|---:|---|
| REFERENCE_15_0 | 65 | +1.8135 | **−4.1148** | 1.5240 | **+8.5459** | **0.6950** | 5 / 3 / 57 |

> **The interval crosses zero.** V12's incremental advantage over V11 is not
> statistically decisive. And this bootstrap is conditional on the selected
> history — it does **not** correct the 39-challenger winner-selection process.

---

## 6. How V12 was selected — and why the rank matters

V12 was chosen from **39 isolated predeclared challengers**. It was **not the
best observed variant**:

| Rank | Variant | Fills | Win % | PF | Net points | Max DD | Gate |
|---:|---|---:|---:|---:|---:|---:|---|
| 1 | V12_S06_0935_LONG_VOLUME_MIN_125 | 233 | 52.36% | 2.2142 | +97.0295 | 8.5674 | INSUFFICIENT |
| 2 | V12_S07_LONG_ENTRY_EXPIRY_3 | 230 | 52.61% | 2.2320 | +96.9097 | 8.5674 | INSUFFICIENT |
| **3** | **V12_S06_LATE_SHORT_VOLUME_MIN_150** | **229** | **52.40%** | **2.2356** | **+96.4444** | **5.2693** | **PASS** |
| 4 | V12_S06_0935_LONG_VOLUME_MIN_150 | 233 | 52.36% | 2.1918 | +96.0698 | 8.1067 | FAIL |
| 5 | V12_S06_0940_SHORT_VOLUME_MIN_150 | 234 | 52.14% | 2.1939 | +95.9071 | 7.4148 | FAIL |
| 10 | V12_S06_0945_SHORT_VOLUME_MIN_150 | 232 | 52.16% | 2.1843 | +95.1681 | 6.4218 | FAIL |
| — | **V11_STAGE0_FROZEN_CONTROL** | 237 | 51.90% | 2.1452 | +94.6309 | 8.5674 | control |
| 13 | V12_S06_LATE_SHORT_VOLUME_MIN_125 | 235 | 51.91% | 2.1637 | +94.8201 | 7.4174 | FAIL |
| 14 | V12_S06_0940_SHORT_VOLUME_MIN_125 | 237 | 51.90% | 2.1452 | +94.6309 | 8.5674 | FAIL |

**Observed rank 3, but gate-passing rank 1.** The two higher-ranked variants were
marked `INSUFFICIENT` — they did not clear the predeclared gate. This is the
discipline working: the selection was made on a gate, not on the raw leaderboard.

Note also that `V12_S06_0940_SHORT_VOLUME_MIN_125` (rank 14) reproduces the V11
control *exactly* — a 1.25 floor on 09:40 SHORT alone removes nothing.

### The predeclared gate for the selected variant

| Criterion | Value |
|---|---:|
| Affected decisions | 118 |
| Net ratio, REFERENCE_15_0 | 1.0192 |
| PF delta, REFERENCE_15_0 | +0.0904 |
| Net ratio, STRESS_25_5 | 1.0526 |
| PF delta, STRESS_25_5 | +0.0629 |
| Reference MDD ratio | **0.6150** |
| Reference fill retention | 0.9662 |
| Ex-July delta points | +1.2997 |
| Forward-extension delta points | +0.0000 |
| Both sides harsh positive | Yes |
| **Gate status** | **PASS** |

The MDD ratio of 0.615 is the strongest single number in the gate — and note
`ex_july_delta_points = +1.2997`, meaning the improvement is **not** purely a
July artifact. `forward_extension_delta_points = 0.0000`: V12 and V11 are
identical across the six forward sessions.

---

## 7. Integrity, data contract and blocked tests

### 7.1 What was verified

- All **40** inventoried standalone artifacts passed size, hash and file-set validation.
- Profile, registry, resolved configuration and input bindings revalidated.
- Calendar span contains 66 expected regular sessions; **missing validated session: 2026-08-26**.
- Every selected V12 candidate has a stored path; `data_incomplete_candidates` is **zero**.

### 7.2 Source segments

| Segment | From | Through | Contract | Universe date | Sessions | Candidates | Expected symbol-sessions | Incomplete | Incomplete % |
|---|---|---|---|---|---:|---:|---:|---:|---:|
| AUG_CORE_59 | 2026-05-27 | 2026-08-19 | 26AUG | 2026-08-11 | 59 | 1,126 | 12,272 | 5,922 | 48.26% |
| AUG_EXTENSION_20_21 | 2026-08-20 | 2026-08-21 | 26AUG | 2026-08-11 | 2 | 27 | 416 | 416 | **100.00%** |
| SEP_ROLLOVER_24_25 | 2026-08-24 | 2026-08-25 | 26SEP | 2026-08-24 | 2 | 48 | 414 | 414 | **100.00%** |
| SEP_DIAGNOSTIC_27 | 2026-08-27 | 2026-08-27 | 26SEP | 2026-08-27 | 1 | 22 | 210 | 210 | **100.00%** |
| SEP_DIAGNOSTIC_28 | 2026-08-28 | 2026-08-28 | 26SEP | 2026-08-28 | 1 | 18 | 210 | 210 | **100.00%** |

**Strict source completeness failed for 7,172 of 13,522 symbol-sessions
(53.04%)** — and all four non-core segments are 100% incomplete.

### 7.3 Thirteen validity tests blocked by the data contract

| Stage | Test | Reason |
|---|---|---|
| DATA_VALIDITY | POINT_IN_TIME_UNIVERSE_FULL_HISTORY | Core history reuses a later static futures universe backward |
| DATA_VALIDITY | AUG_26_COMPLETE_REPLAY | 2026-08-26 has no validated comparable cache |
| DATA_VALIDITY | UNIFORM_EXACT_1530_PATHS | 238 selected paths stop at 15:15 rather than 15:30 |
| FUTURES_EXECUTION | ROLLING_FRONT_MONTH_FUTURES_1M | Complete dated rolling futures 1m paths are absent |
| FUTURES_EXECUTION | DATED_LOT_TICK_MARGIN_COSTS | Historical lot/tick/margin/spread snapshots are absent |
| STRUCTURAL_FILTERS | FUTURES_OI_PERSISTENCE | One signal OI observation, no causal two-bar OI sidecar |
| MARKET_CONTEXT | INDEX_SECTOR_VWAP_ALIGNMENT | Point-in-time index/sector/membership histories are absent |
| MARKET_CONTEXT | OPENING_MARKET_BREADTH | No snapshot-bound causal opening-breadth series |
| MARKET_CONTEXT | HISTORICAL_FUTURES_SPREAD_DEPTH | No historical bid/ask, depth or impact observations |
| PORTFOLIO_RISK | ACTUAL_FUTURES_RISK_SIZING | Dated futures prices, lots and margins incomplete |
| PORTFOLIO_RISK | AGGREGATE_MARGIN_AND_STOP_RISK_CAP | No honest executable futures capital ledger |
| EXIT_RESEARCH | EXACT_1530_EXIT_GRID | Mixed 15:15/15:30 boundary invalidates an exact clock grid |
| EXIT_RESEARCH | PATH_SAFE_MFE_MAE_EXIT_RULES | Most stored excursion paths have boundary ambiguity |

**This is the most valuable table in the report.** It states, test by test, what
cannot yet be answered — rather than answering it badly.

---

## 8. Exploratory indicator study — and its verdict

The report ran a full corrected exploratory pass across 15 numeric features
(directional move, OI change, volume ratio, traded value, 5m range/body/adverse
wick/close location, EMA total gap, confirmation volume/body/wick/close
location, trigger distance, entry delay), with cohort distributions, winner-vs-
loser medians, BH-corrected comparisons, correlations, data-derived quartiles and
fixed bins.

**The verdict:**

> Across the corrected exploratory tests, **0 numeric features separate winners
> from losers at BH q < 0.05**, while **4 separate filled from non-filled
> candidates**. These are post-selection associations, not permission to change
> thresholds.

That is a genuinely useful negative result: nothing in the five-minute or
one-minute feature set predicts *trade outcome* once multiple testing is
controlled. The features that do separate are about whether a candidate becomes
a *fill* — which is mechanical, not predictive.

---

## 9. What is supported and what is not

**Supported descriptively**

- The sealed V12 run reproduces exactly.
- All three cost cases remain positive.
- The late-SHORT volume filter reduces observed drawdown (−38.5%) and slightly
  improves the selected-history result versus V11.

**Not established**

- Live futures profitability.
- Untouched out-of-sample accuracy.
- Causal superiority over V11.
- The profitability of rejected candidates after portfolio displacement.

**Main risks**

| Risk | Detail |
|---|---|
| Statistical | V12 chosen after 39 isolated challengers; only 8 daily results differ; paired reference interval crosses zero |
| Data | Incomplete symbol-session coverage (53%), one missing regular session, static/potentially future-known universes, mixed terminal times |
| Execution | Cash-equity paths and proxy sizing replace rolling futures contracts, lots, margins, spread and market impact |
| Terminal policy | 54.63% of net points come from last-real-bar exits |
| Concentration | July = 66% of net; best 5 days = 47% of net |

**Indicators not present in frozen V12:** ATR, RSI, ADX, VWAP, point-in-time
index/sector regime, opening breadth, order-book liquidity.

**Report discipline:** indicator bins, symbol tables, exit reasons, MFE and MAE
are **hypothesis generators, not post-hoc filters**.

---

## 10. The staged improvement plan

### Stage A — freeze the comparator set
1. Preserve the exact V10, V11 and V12 hashes. Register every future test before reading its result.
2. Use V11 as control and V12 as challenger. **Do not replace the control because V12 has the best observed drawdown.**

### Stage B — repair market-data validity
1. Reconstruct daily point-in-time F&O membership and deterministic front-month rolls.
2. Bind actual futures 1-minute/tick price and OI, dated lots/ticks/margins, complete session paths and a verified pre-close exit.
3. Re-run V10/V11/V12 on the common repaired input and **reject improvements that disappear**.

### Stage C — prospective mechanism validation
1. Freeze volume ratio 1.50 and collect genuinely new sessions without tuning.
2. Record V11 and V12 decisions side by side, especially the late-SHORT exclusions and all portfolio displacement.
3. Require enough **affected decisions**, not merely 100 total fills — most V11/V12 trades are identical.

### Stage D — five-minute quality research
1. Treat the 1.50 late-SHORT volume rule as the **only** active hypothesis. Do not select another threshold from this report.
2. If new data supports it, predeclare **one** setup-specific test involving prior-OI quality, relative rank margin or market/sector context. Point-in-time inputs only.
3. Apply multiple-testing control and preserve the complete candidate stream so rejected-candidate counterfactuals stay available.

### Stage E — one-minute entry research
1. Test setup-specific confirmation/entry timing only after reviewing prospective V11/V12 parity.
2. Keep confirmation-bar non-fill, tick rounding, cancellations and portfolio reservations identical between replay and paper.

### Stage F — executable gap, cost and risk model
1. Replace Gap2 with an executable policy: accept stop-market gaps, model stop-limit non-fills, or use a synthetic trigger with measured latency.
2. Model both entry and exit spread/impact, partial fills, rejects, broker margins and actual futures lots.
3. Add daily-loss, gross exposure, sector concentration and kill-switch gates to a separate fail-closed paper adapter.

### Stage G — exit research after path repair
1. Resolve the mixed 15:15/15:30 boundary before testing time stops, break-even or trailing rules.
2. Use tick/event paths for intrabar ordering and predeclare each exit hypothesis.

### Promotion rule

> Promote nothing unless V12 beats V11 on **untouched repaired futures data**,
> remains positive in both stress cases, preserves drawdown and concentration,
> and achieves decision/fill parity in shadow and paper execution. Otherwise
> retain V11/V12 as research controls.

---

## 11. Commands

```powershell
# The locked full-history backtest (all three cost scenarios)
python -u fno_v12_backtest.py run --all-usable-history

# Reference cost only
python -u fno_v12_backtest.py run --all-usable-history --reference-only

# Print the immutable profile
python fno_v12_backtest.py profile

# Validate a completed run
python -u fno_v12_backtest.py validate --provenance "<run-dir>\provenance.json"

# Regenerate this deep-study report
python -u fno_v12_full_historical_report.py `
  --source-run "<run-dir>" `
  --lineage-run "<staged-research-run-dir>" `
  --report ".\report_v12.md" `
  --assets-dir ".\report_v12_assets"
```

The report writes **83 CSV tables** and **6 charts** into `report_v12_assets/`.
The sealed run was validated and read but **not modified**.

---

## 12. Where V12 sits in the lineage

| | V6 live | V8 | V10 | V11 | **V12** |
|---|---|---|---|---|---|
| Purpose | Production paper/live | Execution model | Selection rules | Entry timing + portfolio | **Late-SHORT volume quality** |
| Fill model | at trigger | at open if gapped | gaps > 2 bps rejected | + strong-identity guard | inherited |
| Entry timing | S+1 fixed | B0–B5 | S+1 global | + 09:30 SHORT ≥ S+3 | inherited |
| Portfolio | 12/day, no ledger | 1/symbol | 1/symbol | **2 same-side/symbol** | inherited |
| New mechanism | — | state machine | 2 selection filters | 2 runtime + 1 fix | **2 selection filters** |
| Base candidates | — | — | 1,241 → 1,134 | 1,134 | **1,241 → 1,017** |
| Fills (65 sessions) | — | — | 232 | 237 | **229** |
| PF (ref) | — | — | 1.8327 | 2.1452 | **2.2356** |
| Net P&L (ref) | — | — | Rs 36,312 | Rs 46,783 | **Rs 47,504** |
| Max daily DD | — | — | 9.3513 | 8.5674 | **5.2693** |
| Status | PAPER, running | not promotable | not promotable | not promotable | **not promotable** |

See [FNO_V6_LIVE_STRATEGY.md](FNO_V6_LIVE_STRATEGY.md),
[FNO_V8_BACKTEST_STRATEGY.md](FNO_V8_BACKTEST_STRATEGY.md),
[FNO_V10_MAX050_GAP2_BACKTEST_STRATEGY.md](FNO_V10_MAX050_GAP2_BACKTEST_STRATEGY.md) and
[FNO_V11_STAGE10_BACKTEST_STRATEGY.md](FNO_V11_STAGE10_BACKTEST_STRATEGY.md).

---

# Appendix A — Complete parameter reference

## A.1 Profile payload

```python
{
  "schema_version": "fno_v12_late_short_volume_150_locked_backtest_v1",
  "profile_id":     "V12_S06_LATE_SHORT_VOLUME_MIN_150",
  "stage_id":       "STAGE_06D_LATE_SHORT_VOLUME",
  "family":         "SELECTION_FIVE_MINUTE_VOLUME_MIN",
  "description":    "09:40 and 09:45 SHORT five-minute volume ratio minimum 1.50 inclusive",

  "selection_origin": {
      "isolated_predeclared_variant":    True,
      "gate_passing_observed":           True,
      "winner_selected_after_v12_research": True,
      "stage12_combination":             False,
  },
  "execution_stack": [
      "V11_FIXED_RUNTIME_OUTER",
      "V12_NEUTRAL_RUNTIME_INNER",
      "V11_STRONG_IDENTITY_GAP2_INNERMOST",
  ],
  "selection_contract": {
      "start_from":             "ALL_1241_INPUT_CANDIDATES",
      "move_0935_long_max_pct": 0.50,
      "move_0940_long_min_pct": 0.40,
      "volume_0940_short_min":  1.50,
      "volume_0945_short_min":  1.50,
      "bounds":                 "INCLUSIVE",
      "rerank_after_selection": True,
  },
  "gap_guard": {"variant": "MAX_2_BPS", "max_adverse_gap_bps": 2.0,
                "identity_policy": "STRONG_REFERENCE_AND_IS_CHECK"},
  "headline_valid": False, "research_only": True,
  "promotion_eligible": False, "live_or_paper_authority": False,
}
```

Note `stage12_combination: False` — unlike V11 Stage 10, V12 is a **single
isolated mechanism**, not a post-hoc composite. It is still a winner selected
after research, hence `winner_selected_after_v12_research: True`.

## A.2 Pinned constants

| Constant | Value |
|---|---|
| `TARGET_EXPOSURE_PER_ENTRY_RS` | 50,000.0 |
| `SQUARE_OFF` | `15:30` |
| `EOD_POLICY` | `LAST_REAL_BAR_SENSITIVITY` |
| `GAP_VARIANT` | `MAX_2_BPS` |
| `EXPECTED_SESSION_COUNT` | 65 |
| `EXPECTED_ALL_CANDIDATES` | 1,241 |
| `EXPECTED_SELECTED_CANDIDATES` | 1,017 |
| `EXPECTED_REGISTRY_SHA256` | `4948ba18…90328c35` |
| `EXPECTED_RESOLVED_CONFIG_SHA256` | `660ab5d2…7481ca4f` |
| `EXPECTED_PROFILE_SHA256` | `067c5f1c…7e1c8fa7cbe` |
| `EXPECTED_INPUT_BINDING_SHA256` | `78c4d708…c025e62c` |
| `_BENCHMARK_ABS_TOLERANCE` | 1e-9 |

## A.3 Pinned expected economics

| Field | REFERENCE_15_0 | STRESS_20_2 | STRESS_25_5 |
|---|---:|---:|---:|
| sessions / candidates / fills | 65 / 1,017 / 229 | 65 / 1,017 / 229 | 65 / 1,017 / 229 |
| wins / losses / flat | 120 / 109 / 0 | 116 / 113 / 0 | 111 / 118 / 0 |
| win_rate_pct | 52.40174672489083 | 50.65502183406113 | 48.47161572052402 |
| profit_factor | 2.235608588019062 | 1.942252656412753 | 1.6285926256965166 |
| net_return_points | 96.44436250687984 | 80.33885933960273 | 59.8044590382387 |
| net_pnl_rs | 47,503.83646266349 | 39,710.98746792726 | 29,759.080444006366 |
| max_daily_drawdown_points | 5.269268744424497 | 6.266063217303861 | 7.278721516986862 |
| positive / negative / flat days | 37 / 24 / 4 | 34 / 27 / 4 | 32 / 29 / 4 |
| remaining_gap_fills | 23 | 23 | 23 |
| guard_rejections | 22 | 22 | 22 |
| data_incomplete_candidates | 0 | 0 | 0 |

Float fields compared at 1e-9: `win_rate_pct`, `profit_factor`,
`net_return_points`, `net_pnl_rs`, `max_daily_drawdown_points`.

## A.4 Closed-trade economic fingerprints

SHA-256 over 17 canonical columns (`candidate_id, setup_id, symbol, side,
entry_time, entry_price, stop_price, target_price, exit_time, exit_price,
exit_reason, gross_return_pct, net_return_pct, quantity, gross_pnl_rs,
estimated_cost_rs, net_pnl_rs`):

| Scenario | Fingerprint |
|---|---|
| `REFERENCE_15_0` | `b200e6b5ce29044462a6b3edc43ac09736643b9a04129ff431cdffe08c612428` |
| `STRESS_20_2` | `a452e295d15aed4eeeccaa97d79efb9814b638296140cb63c521b6f81db58816` |
| `STRESS_25_5` | `1befedf2c49f8af4f7647b31cf6ba69e061216097233b8d25fc1f055fdc46c63` |

## A.5 Confirmation and entry-minute distribution

| Confirmation minute | Fills | Win % | PF | Net points | Net P&L |
|---|---:|---:|---:|---:|---:|
| M1 | 166 | 53.01% | 2.3116 | +75.7829 | +Rs 37,387.51 |
| M2 | 45 | 46.67% | 1.5021 | +7.6223 | +Rs 3,827.87 |
| M3 | 18 | 61.11% | 3.5606 | +13.0392 | +Rs 6,288.46 |

| Entry minute | Fills | Win % | PF | Net points | Net P&L |
|---|---:|---:|---:|---:|---:|
| S+2 | 132 | 53.79% | 2.3964 | +63.2349 | +Rs 31,421.13 |
| S+3 | 65 | 47.69% | 1.6728 | +15.1977 | +Rs 7,384.07 |
| S+4 | 23 | 52.17% | 2.4318 | +11.1839 | +Rs 5,254.84 |
| S+5 | 9 | 66.67% | 3.8822 | +6.8279 | +Rs 3,443.80 |

Confirmation and entry minute are **causal** features, but any new timing rule
must be replayed inside each setup — a global minute ban removes profitable legs
along with weak ones.

## A.6 Glossary

| Term | Definition |
|---|---|
| **Net return points** | arithmetic sum of per-trade net percentage returns; **not** compounded portfolio return |
| **PF** | gross positive net-return points ÷ absolute gross negative net-return points |
| **MDD** | maximum peak-to-trough drawdown of cumulative daily summed return points unless marked Rs |
| **WR** | winning closed trades ÷ closed trades |
| **S+N** | the Nth completed one-minute bar after the five-minute signal closes |
| **MFE/MAE** | bounded favourable/adverse excursion after entry; **future outcome data**, not an entry feature |
| **BH q-value** | multiple-test-adjusted p-value; low q reduces but does not eliminate false-discovery risk |
| **Research-only** | reproducible hypothesis evidence without paper/live authority or a claim of achievable returns |
