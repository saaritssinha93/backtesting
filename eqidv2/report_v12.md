# V12 FNO selected strategy — full historical deep-study report

Generated: 2026-08-31T00:37:45.007486+05:30
Validated standalone run: `C:\TradingData\eqidv2\fno_oi\strategy_research\v12_s06_late_short_volume_min150_full_history_v1\run_20260831T001454752119+0530`
Profile: `V12_S06_LATE_SHORT_VOLUME_MIN_150`
Profile SHA-256: `067c5f1c14b7f626b0c112524c2a0c63bc9f379f6d081547bfc747e1c8fa7cbe`
Historical input binding: `78c4d7088f7cf500ec8da587a200314c43cf669a56e2df2aca52b74ec025e62c`

> **Research boundary:** `headline_valid=false`, `research_only=true`, `promotion_eligible=false`, and `live_or_paper_authority=false`. The figures are reproducible cash-equity execution proxies selected after V12 research; they are not live futures evidence.

## Executive conclusion

The sealed replay covers **65 usable sessions** from **2026-05-27 through 2026-08-28** and records **229 fills, 120-109, WR 52.40%, PF 2.2356, +96.4444 net points and Rs +47,503.84 modeled P&L**. Daily MDD is 5.2693 points.

Under 25 bps costs plus 5 bps entry slippage, the result remains positive: PF 1.6286, +59.8045 points and Rs +29,759.08.

The evidence is concentrated: July supplies 66.19% of net points, and 66 last-real-bar exits supply 54.63%. The six-session extension earns +6.0895 points but contributed to variant selection.

V12 changes only two five-minute filters: 09:40 SHORT and 09:45 SHORT require volume ratio >= 1.50. Versus V11, only 8 of 65 daily results change. The staged paired bootstrap therefore matters more than the headline improvement.

Across the corrected exploratory tests, **0** numeric features separate winners from losers at BH q < 0.05, while **4** separate filled from non-filled candidates. These are post-selection associations, not permission to change thresholds.

## 1. Integrity, data contract and scope

- All **40** inventoried standalone artifacts passed size, hash and file-set validation.
- Profile, registry, resolved configuration and input bindings revalidated: `067c5f1c14b7f626b0c112524c2a0c63bc9f379f6d081547bfc747e1c8fa7cbe`, `4948ba186095a5baea6b538a64255bc7304e96720ba98da512d6d21490328c35`, `660ab5d2d06290d23e6b39593ddbb5afe03f51e3b6bb714099134eff7481ca4f`, `78c4d7088f7cf500ec8da587a200314c43cf669a56e2df2aca52b74ec025e62c`.
- Calendar span contains 66 expected regular sessions; missing validated session: **2026-08-26**.
- Strict source completeness failed for **7,172 of 13,522 symbol-sessions (53.04%)**. This is universe/path coverage, not the selected-candidate `data_incomplete_candidates`, which is zero.
- Every selected V12 candidate has a stored path; exact terminal coverage is 238 at 15:15, 779 at 15:30.
- The candidate cache contains base-qualified five-minute rows; it does not retain every symbol that failed the base screen. Full filter counterfactuals require rebuilding the complete source stream.
- Futures OI drives selection, while price, EMA, volume, confirmation, entry, stop, target and P&L use NSE cash-equity bars.

### Source segments

| Segment Id | From Day | Through Day | Contract Month | Universe Master Date | Sessions | Candidates | Expected Symbol Sessions | Source Incomplete Symbol Sessions | Source Incomplete Pct | Headline Source Complete |
|---|---|---|---|---|---|---|---|---|---|---|
| AUG_CORE_59 | 2026-05-27 | 2026-08-19 | 26AUG | 2026-08-11 | 59 | 1,126 | 12,272 | 5,922 | 48.26% | No |
| AUG_EXTENSION_20_21 | 2026-08-20 | 2026-08-21 | 26AUG | 2026-08-11 | 2 | 27 | 416 | 416 | 100.00% | No |
| SEP_ROLLOVER_24_25 | 2026-08-24 | 2026-08-25 | 26SEP | 2026-08-24 | 2 | 48 | 414 | 414 | 100.00% | No |
| SEP_DIAGNOSTIC_27 | 2026-08-27 | 2026-08-27 | 26SEP | 2026-08-27 | 1 | 22 | 210 | 210 | 100.00% | No |
| SEP_DIAGNOSTIC_28 | 2026-08-28 | 2026-08-28 | 26SEP | 2026-08-28 | 1 | 18 | 210 | 210 | 100.00% | No |

### Validity tests blocked by the current data contract

| Stage Id | Test Id | Status | Reason |
|---|---|---|---|
| STAGE_01_DATA_VALIDITY | POINT_IN_TIME_UNIVERSE_FULL_HISTORY | BLOCKED_VALIDITY | The core history reuses a later static futures universe backward. |
| STAGE_01_DATA_VALIDITY | AUG_26_COMPLETE_REPLAY | BLOCKED_VALIDITY | 2026-08-26 has no validated comparable full-session cache. |
| STAGE_01_DATA_VALIDITY | UNIFORM_EXACT_1530_PATHS | BLOCKED_VALIDITY | 238 selected paths stop at 15:15 rather than the intended 15:30. |
| STAGE_02_FUTURES_EXECUTION | ROLLING_FRONT_MONTH_FUTURES_1M | BLOCKED_VALIDITY | Complete dated rolling futures one-minute price paths are absent. |
| STAGE_02_FUTURES_EXECUTION | DATED_LOT_TICK_MARGIN_COSTS | BLOCKED_VALIDITY | Historical lot, tick, margin, spread, and full cost snapshots are absent. |
| STAGE_08_STRUCTURAL_FILTERS | FUTURES_OI_PERSISTENCE | BLOCKED_VALIDITY | The cache has one signal OI observation but no causal two-bar OI sidecar. |
| STAGE_09_MARKET_CONTEXT | INDEX_SECTOR_VWAP_ALIGNMENT | BLOCKED_VALIDITY | Point-in-time index, sector, and dated membership histories are absent. |
| STAGE_09_MARKET_CONTEXT | OPENING_MARKET_BREADTH | BLOCKED_VALIDITY | A snapshot-bound causal opening-breadth series is absent. |
| STAGE_09_MARKET_CONTEXT | HISTORICAL_FUTURES_SPREAD_DEPTH | BLOCKED_VALIDITY | Historical bid/ask spread, depth, and impact observations are absent. |
| STAGE_10_PORTFOLIO_RISK | ACTUAL_FUTURES_RISK_SIZING | BLOCKED_VALIDITY | Dated futures prices, lots, and historical margins are incomplete. |
| STAGE_10_PORTFOLIO_RISK | AGGREGATE_MARGIN_AND_STOP_RISK_CAP | BLOCKED_VALIDITY | An executable futures capital ledger cannot be reconstructed honestly. |
| STAGE_11_EXIT_RESEARCH | EXACT_1530_EXIT_GRID | BLOCKED_VALIDITY | The mixed 15:15/15:30 path boundary invalidates an exact clock grid. |
| STAGE_11_EXIT_RESEARCH | PATH_SAFE_MFE_MAE_EXIT_RULES | BLOCKED_VALIDITY | Most stored excursion paths have boundary ambiguity. |

## 2. Exact strategy and parameter values

### Global overlays and economics

| Layer | Parameter | Value | Scope |
|---|---|---|---|
| Identity | V12 profile | V12_S06_LATE_SHORT_VOLUME_MIN_150 | locked standalone |
| Identity | Profile SHA-256 | 067c5f1c14b7f626b0c112524c2a0c63bc9f379f6d081547bfc747e1c8fa7cbe | entire profile |
| Selection | 09:40 SHORT minimum volume ratio | 1.50 inclusive | V12 change |
| Selection | 09:45 SHORT minimum volume ratio | 1.50 inclusive | V12 change |
| Selection | 09:40 LONG directional move floor | 0.40% inclusive | inherited V11 |
| Selection | 09:35 LONG directional move ceiling | 0.50% inclusive | inherited V11 |
| Ranking | Rerank after selection | True | each setup/side/slot |
| 1m timing | 09:30 SHORT earliest trigger-fill | S+3 | inherited V11 |
| Gap | Maximum adverse trigger gap | 2 bps | strong-identity gap events |
| Portfolio | Same symbol + same side concurrent limit | 2 | all setups |
| Portfolio | Same symbol + opposite side | Prohibited | all setups |
| Portfolio | Modeled capital | Rs 120,000 | proxy global ledger |
| Portfolio | Margin reservation per entry | Rs 10,000 | proxy global ledger |
| Sizing | Target cash-equivalent exposure | Rs 50,000 | quantity=floor(exposure/entry) |
| Exit | Same-bar collision | STOP_FIRST | conservative OHLC rule |
| Exit | Square-off clock | 15:30 | when a real bar exists |
| Exit | Terminal policy | LAST_REAL_BAR_SENSITIVITY | partial-path sensitivity |
| Costs | Reference | 15 bps + 0 bps entry slippage | headline |
| Costs | Stress | 20 bps + 2 bps entry slippage | sensitivity |
| Costs | Harsh | 25 bps + 5 bps entry slippage | sensitivity |

### Five-minute selection book

| Setup Id | Signal End | Side | Max Entries | Picker | Five Minute Ema Rule | Effective Move Rule | Five Minute Oi Change Min Pct | Five Minute Volume Ratio Min | Five Minute Traded Value Min Cr | V12 Changed Field |
|---|---|---|---|---|---|---|---|---|---|---|
| 09:25_LONG | 09:25 | LONG | 4 | max_move | EMA9>EMA20>EMA50 | >= +0.30% | 0.10% | 3.0000 | 0.0000 | inherited unchanged |
| 09:25_SHORT | 09:25 | SHORT | 4 | max_move | EMA9<EMA20<EMA50 | <= -0.20% | 0.10% | 1.5000 | 2.5000 | inherited unchanged |
| 09:30_LONG | 09:30 | LONG | 1 | max_move | EMA9>EMA20>EMA50 | >= +0.65% | 0.10% | 1.0000 | 0.0000 | inherited unchanged |
| 09:30_SHORT | 09:30 | SHORT | 4 | max_volume | EMA9<EMA20<EMA50 | <= -0.20% | 1.00% | 1.0000 | 2.5000 | inherited unchanged |
| 09:35_LONG | 09:35 | LONG | 1 | max_liquidity | EMA9>EMA20>EMA50 | >= +0.20% and <= +0.50% | 0.10% | 1.0000 | 0.0000 | inherited unchanged |
| 09:35_SHORT | 09:35 | SHORT | 2 | max_liquidity | EMA9<EMA20<EMA50 | <= -0.50% | 1.00% | 1.0000 | 0.0000 | inherited unchanged |
| 09:40_LONG | 09:40 | LONG | 1 | max_liquidity | EMA9>EMA20>EMA50 | >= +0.40% | 0.10% | 2.0000 | 0.0000 | inherited unchanged |
| 09:40_SHORT | 09:40 | SHORT | 1 | max_move | EMA9<EMA20<EMA50 | <= -0.20% | 0.10% | 1.5000 | 0.0000 | volume_ratio raised 1.00 -> 1.50 |
| 09:45_LONG | 09:45 | LONG | 1 | max_move | EMA9>EMA20>EMA50 | >= +0.65% | 0.10% | 1.0000 | 0.0000 | inherited unchanged |
| 09:45_SHORT | 09:45 | SHORT | 1 | max_volume | EMA9<EMA20<EMA50 | <= -0.20% | 0.75% | 1.5000 | 0.0000 | volume_ratio raised 1.00 -> 1.50 |

`max_entries` is a setup/side/slot cap, not a daily cap. LONG and SHORT buckets are independent. Candidates are ranked by the setup picker, then the portfolio ledger applies chronological reservations.

### One-minute confirmation and trade book

| Setup Id | One Minute Confirmation Body Ratio Min | One Minute Confirmation Adverse Wick Ratio Max | Effective Close Location Min | Effective Max Confirmation Minute | Effective Earliest Fill Minute | Effective Buffer Bps | Effective Midpoint Invalidation | Entry Expiry Minute | Stop Pct | Target Pct | Post Confirmation Cancel | Allow Cap Reassignment | Same Bar Policy |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 09:25_LONG | 0.0000 | 0.5000 | — | 3 | 2 | 0.0000 | No | 5 | 0.40% | 1.00% | Yes | Yes | STOP_FIRST |
| 09:25_SHORT | 0.6000 | 0.6000 | — | 3 | 2 | 2.0000 | No | 5 | 0.50% | 3.00% | Yes | Yes | STOP_FIRST |
| 09:30_LONG | 0.5000 | 0.5000 | — | 1 | 2 | 0.0000 | No | 5 | 1.00% | 2.50% | Yes | Yes | STOP_FIRST |
| 09:30_SHORT | 0.4500 | 0.3000 | 0.5000 | 3 | 3 | 0.0000 | Yes | 5 | 1.00% | 4.00% | Yes | Yes | STOP_FIRST |
| 09:35_LONG | 0.6000 | 0.5000 | — | 1 | 2 | 0.0000 | No | 5 | 1.00% | 2.50% | Yes | Yes | STOP_FIRST |
| 09:35_SHORT | 0.4000 | 0.5000 | — | 1 | 2 | 0.0000 | No | 5 | 1.00% | 3.00% | Yes | Yes | STOP_FIRST |
| 09:40_LONG | 0.5000 | 0.5000 | — | 1 | 2 | 0.0000 | No | 5 | 0.50% | 2.50% | Yes | Yes | STOP_FIRST |
| 09:40_SHORT | 0.4000 | 0.5000 | — | 1 | 2 | 0.0000 | No | 5 | 1.00% | 3.00% | Yes | Yes | STOP_FIRST |
| 09:45_LONG | 0.4000 | 0.5000 | — | 1 | 2 | 0.0000 | No | 5 | 1.00% | 3.00% | Yes | Yes | STOP_FIRST |
| 09:45_SHORT | 0.4000 | 0.3000 | — | 1 | 2 | 0.0000 | No | 5 | 1.00% | 2.00% | Yes | Yes | STOP_FIRST |

### Indicator definitions and causality

| Feature | Formula | Causal Note |
|---|---|---|
| 5m construction | exact five valid end-labelled 1m rows; O/H/L/C/V = first/max/min/last/sum | completed slot only |
| EMA9/20/50 | pandas EWM(close, span=N, adjust=False) | cash-equity 5m closes through S |
| price_change_pct | 100 * (C[S] / C[S-5m] - 1) | side-aware threshold |
| OI change pct | 100 * (OI[S] / OI[S-5m] - 1) | exact preceding futures 5m timestamp |
| volume_ratio | V[S] / mean(V[S-20..S-1]); min_periods=5 | current volume excluded from denominator |
| traded_value | cash-equity C[S] * V[S] | used for liquidity picker/minimum |
| broad base gates | directional move >=0.10%, OI change >=0.05%, volume ratio >=0.80 | all setup thresholds are equal or stricter |
| confirmation body ratio | abs(C-O)/(H-L) | completed S+N 1m candle |
| LONG adverse wick | (H-max(O,C))/(H-L) | SHORT mirrors on lower wick |
| directional close location | LONG (C-L)/(H-L); SHORT (H-C)/(H-L) | higher is stronger |
| entry trigger | LONG confirmation H + buffer; SHORT confirmation L - buffer; tick-rounded | cannot fill on confirmation bar |
| stop/target | actual fill * (1 +/- setup stop/target pct), adversely tick-rounded | STOP_FIRST if both touch in one OHLC bar |
| quantity | floor(Rs 50,000 / cash-equity entry price) | not futures lot sizing |
| net return | side-aware gross return pct - cost_bps/100 | configured slippage affects entry only |
| PF | sum(positive net-return points) / abs(sum(negative net-return points)) | trade-return points, not account PF |

## 3. Selection-to-exit funnel

### Overall funnel

| Step | Count |
|---|---|
| Base 5m | 1,241 |
| After V12 filters | 1,017 |
| 1m confirmed | 383 |
| Filled | 229 |
| Winners | 120 |

### By setup

| Setup Id | Raw Base 5M Candidates | Post Overlay Selected | One Minute Confirmed | Fills | Wins | Losses | Win Rate Pct | Profit Factor | Net Return Points | Net Pnl Rs | Max Entries | Picker |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 09:25_LONG | 122 | 122 | 78 | 61 | 32 | 29 | 52.46% | 1.6844 | +11.0095 | +5,289.78 | 4 | max_move |
| 09:25_SHORT | 261 | 261 | 114 | 62 | 28 | 34 | 45.16% | 2.0559 | +22.3665 | +10,997.77 | 4 | max_move |
| 09:30_LONG | 65 | 65 | 17 | 11 | 5 | 6 | 45.45% | 1.6174 | +3.5120 | +1,709.49 | 1 | max_move |
| 09:30_SHORT | 101 | 101 | 41 | 19 | 12 | 7 | 63.16% | 3.3855 | +14.7457 | +7,157.02 | 4 | max_volume |
| 09:35_LONG | 248 | 171 | 35 | 17 | 9 | 8 | 52.94% | 1.5052 | +3.7417 | +1,914.06 | 1 | max_liquidity |
| 09:35_SHORT | 36 | 36 | 15 | 10 | 6 | 4 | 60.00% | 3.1215 | +9.7864 | +4,914.43 | 2 | max_liquidity |
| 09:40_LONG | 106 | 76 | 25 | 18 | 9 | 9 | 50.00% | 3.1108 | +12.3896 | +5,942.03 | 1 | max_liquidity |
| 09:40_SHORT | 178 | 97 | 34 | 15 | 8 | 7 | 53.33% | 1.8434 | +6.1070 | +3,311.43 | 1 | max_move |
| 09:45_LONG | 36 | 36 | 13 | 9 | 6 | 3 | 66.67% | 4.7245 | +8.7639 | +4,366.10 | 1 | max_move |
| 09:45_SHORT | 88 | 52 | 11 | 7 | 5 | 2 | 71.43% | 3.8060 | +4.0221 | +1,901.73 | 1 | max_volume |

### By side

| Side | Raw Base 5M Candidates | Post Overlay Selected | One Minute Confirmed | Fills | Wins | Losses | Win Rate Pct | Profit Factor | Net Return Points | Net Pnl Rs |
|---|---|---|---|---|---|---|---|---|---|---|
| LONG | 577 | 470 | 168 | 116 | 61 | 55 | 52.59% | 2.0538 | +39.4167 | +19,221.46 |
| SHORT | 664 | 547 | 215 | 113 | 59 | 54 | 52.21% | 2.4029 | +57.0277 | +28,282.38 |

### By five-minute signal time

| Signal End | Raw Base 5M Candidates | Post Overlay Selected | One Minute Confirmed | Fills | Wins | Losses | Win Rate Pct | Profit Factor | Net Return Points | Net Pnl Rs |
|---|---|---|---|---|---|---|---|---|---|---|
| 09:25 | 383 | 383 | 192 | 123 | 60 | 63 | 48.78% | 1.8956 | +33.3760 | +16,287.55 |
| 09:30 | 166 | 166 | 58 | 30 | 17 | 13 | 56.67% | 2.5382 | +18.2576 | +8,866.51 |
| 09:35 | 284 | 207 | 50 | 27 | 15 | 12 | 55.56% | 2.1255 | +13.5281 | +6,828.50 |
| 09:40 | 284 | 173 | 59 | 33 | 17 | 16 | 51.52% | 2.4108 | +18.4966 | +9,253.46 |
| 09:45 | 124 | 88 | 24 | 16 | 11 | 5 | 68.75% | 4.3768 | +12.7860 | +6,267.83 |

### By picker

| Picker | Raw Base 5M Candidates | Post Overlay Selected | One Minute Confirmed | Fills | Wins | Losses | Win Rate Pct | Profit Factor | Net Return Points | Net Pnl Rs |
|---|---|---|---|---|---|---|---|---|---|---|
| max_move | 662 | 581 | 256 | 158 | 79 | 79 | 50.00% | 1.9849 | +51.7589 | +25,674.57 |
| max_liquidity | 390 | 283 | 75 | 45 | 24 | 21 | 53.33% | 2.4488 | +25.9177 | +12,770.52 |
| max_volume | 189 | 153 | 52 | 26 | 17 | 9 | 65.38% | 3.4647 | +18.7678 | +9,058.75 |

### By frozen rank

| Rank Bucket | Selected | Confirmed | Fills | Wins | Losses | Win Rate Pct | Profit Factor | Net Return Points | Net Pnl Rs |
|---|---|---|---|---|---|---|---|---|---|
| 1 | 378 | 142 | 106 | 57 | 49 | 53.77% | 2.2786 | +46.4343 | +22,873.74 |
| 2 | 224 | 76 | 44 | 20 | 24 | 45.45% | 2.3529 | +21.1632 | +10,440.33 |
| 3 | 138 | 52 | 28 | 11 | 17 | 39.29% | 0.6892 | -4.2588 | -2,018.32 |
| 4 | 90 | 37 | 23 | 11 | 12 | 47.83% | 2.6235 | +12.3157 | +6,052.50 |
| 5 | 64 | 25 | 14 | 10 | 4 | 71.43% | 4.6627 | +10.7921 | +5,374.92 |
| 6+ | 123 | 51 | 14 | 11 | 3 | 78.57% | 6.3776 | +9.9979 | +4,780.66 |

Rank performance is non-monotonic. A weak observed rank cannot safely become a blacklist when later ranks remain profitable; any rank-margin hypothesis needs setup-stratified prospective replay.

### V12 five-minute filter rejections

| Selection Reason | Setup Id | Rejections | Affected Sessions | Median Price Change Pct | Median Volume Ratio |
|---|---|---|---|---|---|
| REJECTED:MOVE_0935_LONG_MAX | 09:35_LONG | 77 | 40 | 0.67% | 2.7574 |
| REJECTED:MOVE_0940_LONG_MIN | 09:40_LONG | 30 | 22 | 0.28% | 2.4443 |
| REJECTED:VOLUME_0940_SHORT_MIN | 09:40_SHORT | 81 | 34 | -0.33% | 1.2632 |
| REJECTED:VOLUME_0945_SHORT_MIN | 09:45_SHORT | 36 | 23 | -0.36% | 1.1982 |

### Candidate state and reason counts

| Status | Count | Share Pct |
|---|---|---|
| NO_CONFIRMATION | 602 | 59.19% |
| STOPPED | 97 | 9.54% |
| POSTCONF_CANCELLED | 97 | 9.54% |
| SQUARE_OFF | 66 | 6.49% |
| TARGETED | 66 | 6.49% |
| WINDOW_EXPIRED | 54 | 5.31% |
| PRECONF_INVALIDATED | 32 | 3.15% |
| DUPLICATE_REJECTED | 3 | 0.29% |
| Reason | Count | Share Pct |
|---|---|---|
| CONFIRMATION_WINDOW_EXPIRED | 602 | 59.19% |
| STOP | 97 | 9.54% |
| CLOSE_REVERSED_THROUGH_SIGNAL_CLOSE | 75 | 7.37% |
| LAST_REAL_BAR_SENSITIVITY | 66 | 6.49% |
| TARGET | 66 | 6.49% |
| ENTRY_WINDOW_EXPIRED | 54 | 5.31% |
| CLOSE_CROSSED_FIVE_MINUTE_MIDPOINT | 32 | 3.15% |
| ADVERSE_GAP_GUARD_REJECTED | 22 | 2.16% |
| DUPLICATE_SYMBOL_SAME_SIDE_LIMIT_2:CONSERVATIVE_NO_BACKFILL | 3 | 0.29% |

## 4. Complete day-wise results

| Session Date | Raw Base 5M Candidates | Post Overlay Selected | One Minute Confirmed | Fills | Wins | Losses | Win Rate Pct | Profit Factor | Net Return Points | Net Pnl Rs | Cumulative Net Return Points | Cumulative Net Pnl Rs | Drawdown Return Points | Drawdown Pnl Rs |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 2026-05-27 | 9 | 6 | 3 | 3 | 2 | 1 | 66.67% | 4.2654 | +1.8217 | +907.49 | +1.8217 | +907.49 | +0.0000 | +0.00 |
| 2026-05-29 | 24 | 17 | 4 | 3 | 2 | 1 | 66.67% | 10.4738 | +5.2622 | +2,590.87 | +7.0839 | +3,498.36 | +0.0000 | +0.00 |
| 2026-06-01 | 9 | 6 | 1 | 1 | 0 | 1 | 0.00% | 0.0000 | -1.1520 | -565.65 | +5.9319 | +2,932.70 | -1.1520 | -565.65 |
| 2026-06-02 | 13 | 13 | 6 | 3 | 1 | 2 | 33.33% | 1.2366 | +0.5451 | +218.61 | +6.4770 | +3,151.31 | -0.6069 | -347.04 |
| 2026-06-03 | 18 | 15 | 11 | 7 | 6 | 1 | 85.71% | 8.1089 | +8.2162 | +4,024.88 | +14.6932 | +7,176.19 | +0.0000 | +0.00 |
| 2026-06-04 | 12 | 12 | 2 | 1 | 0 | 1 | 0.00% | 0.0000 | -0.5567 | -277.84 | +14.1365 | +6,898.36 | -0.5567 | -277.84 |
| 2026-06-05 | 12 | 11 | 4 | 4 | 1 | 3 | 25.00% | 0.2860 | -2.1153 | -1,042.68 | +12.0212 | +5,855.68 | -2.6720 | -1,320.52 |
| 2026-06-08 | 27 | 15 | 3 | 0 | 0 | 0 | — | — | +0.0000 | +0.00 | +12.0212 | +5,855.68 | -2.6720 | -1,320.52 |
| 2026-06-09 | 10 | 6 | 2 | 2 | 1 | 1 | 50.00% | 0.9784 | -0.0141 | -7.28 | +12.0071 | +5,848.40 | -2.6861 | -1,327.80 |
| 2026-06-10 | 8 | 4 | 1 | 0 | 0 | 0 | — | — | +0.0000 | +0.00 | +12.0071 | +5,848.40 | -2.6861 | -1,327.80 |
| 2026-06-11 | 6 | 5 | 2 | 2 | 1 | 1 | 50.00% | 0.2676 | -0.8440 | -402.17 | +11.1630 | +5,446.22 | -3.5302 | -1,729.97 |
| 2026-06-12 | 10 | 9 | 2 | 1 | 0 | 1 | 0.00% | 0.0000 | -0.6549 | -327.44 | +10.5081 | +5,118.78 | -4.1851 | -2,057.42 |
| 2026-06-15 | 15 | 14 | 6 | 4 | 2 | 2 | 50.00% | 1.7060 | +0.7022 | +349.48 | +11.2103 | +5,468.26 | -3.4829 | -1,707.93 |
| 2026-06-16 | 20 | 14 | 3 | 2 | 0 | 2 | 0.00% | 0.0000 | -0.6280 | -312.49 | +10.5822 | +5,155.78 | -4.1110 | -2,020.42 |
| 2026-06-17 | 10 | 9 | 1 | 0 | 0 | 0 | — | — | +0.0000 | +0.00 | +10.5822 | +5,155.78 | -4.1110 | -2,020.42 |
| 2026-06-18 | 3 | 3 | 2 | 1 | 0 | 1 | 0.00% | 0.0000 | -1.1583 | -570.71 | +9.4239 | +4,585.07 | -5.2693 | -2,591.12 |
| 2026-06-19 | 11 | 10 | 1 | 1 | 1 | 0 | 100.00% | ∞ | +2.3418 | +1,161.03 | +11.7657 | +5,746.10 | -2.9275 | -1,430.09 |
| 2026-06-22 | 3 | 1 | 0 | 0 | 0 | 0 | — | — | +0.0000 | +0.00 | +11.7657 | +5,746.10 | -2.9275 | -1,430.09 |
| 2026-06-23 | 15 | 9 | 1 | 1 | 0 | 1 | 0.00% | 0.0000 | -0.5525 | -274.27 | +11.2133 | +5,471.84 | -3.4799 | -1,704.36 |
| 2026-06-24 | 14 | 11 | 6 | 4 | 1 | 3 | 25.00% | 0.7907 | -0.6198 | -229.13 | +10.5935 | +5,242.70 | -4.0997 | -1,933.49 |
| 2026-06-25 | 19 | 17 | 7 | 4 | 3 | 1 | 75.00% | 5.9220 | +3.2024 | +1,578.61 | +13.7959 | +6,821.32 | -0.8973 | -354.88 |
| 2026-06-29 | 23 | 21 | 10 | 6 | 4 | 2 | 66.67% | 4.7103 | +4.8608 | +2,370.99 | +18.6567 | +9,192.30 | +0.0000 | +0.00 |
| 2026-06-30 | 13 | 9 | 3 | 2 | 0 | 2 | 0.00% | 0.0000 | -2.3040 | -1,145.62 | +16.3527 | +8,046.69 | -2.3040 | -1,145.62 |
| 2026-07-01 | 13 | 12 | 4 | 3 | 2 | 1 | 66.67% | 3.2076 | +2.5413 | +1,221.89 | +18.8940 | +9,268.58 | +0.0000 | +0.00 |
| 2026-07-02 | 17 | 17 | 5 | 2 | 1 | 1 | 50.00% | 1.8692 | +1.0045 | +675.41 | +19.8986 | +9,943.98 | +0.0000 | +0.00 |
| 2026-07-03 | 29 | 26 | 6 | 5 | 3 | 2 | 60.00% | 0.9327 | -0.1215 | -45.04 | +19.7771 | +9,898.95 | -0.1215 | -45.04 |
| 2026-07-06 | 19 | 16 | 3 | 3 | 2 | 1 | 66.67% | 4.8335 | +2.1324 | +1,065.76 | +21.9095 | +10,964.71 | +0.0000 | +0.00 |
| 2026-07-07 | 39 | 36 | 15 | 8 | 7 | 1 | 87.50% | 18.6128 | +11.4999 | +5,602.68 | +33.4093 | +16,567.39 | +0.0000 | +0.00 |
| 2026-07-08 | 24 | 22 | 13 | 7 | 5 | 2 | 71.43% | 6.9127 | +7.7076 | +3,627.04 | +41.1169 | +20,194.42 | +0.0000 | +0.00 |
| 2026-07-09 | 10 | 8 | 3 | 3 | 0 | 3 | 0.00% | 0.0000 | -2.4543 | -1,192.41 | +38.6626 | +19,002.02 | -2.4543 | -1,192.41 |
| 2026-07-10 | 19 | 15 | 6 | 6 | 4 | 2 | 66.67% | 5.6808 | +5.1675 | +2,567.51 | +43.8301 | +21,569.53 | +0.0000 | +0.00 |
| 2026-07-13 | 23 | 19 | 6 | 4 | 3 | 1 | 75.00% | 115.1783 | +5.4024 | +2,663.71 | +49.2326 | +24,233.24 | +0.0000 | +0.00 |
| 2026-07-14 | 33 | 21 | 10 | 5 | 2 | 3 | 40.00% | 0.3497 | -1.5298 | -763.09 | +47.7028 | +23,470.15 | -1.5298 | -763.09 |
| 2026-07-15 | 26 | 21 | 8 | 6 | 2 | 4 | 33.33% | 1.0467 | +0.1644 | +95.39 | +47.8671 | +23,565.54 | -1.3654 | -667.70 |
| 2026-07-16 | 27 | 22 | 6 | 4 | 1 | 3 | 25.00% | 0.5107 | -1.2090 | -603.99 | +46.6581 | +22,961.56 | -2.5744 | -1,271.69 |
| 2026-07-17 | 21 | 17 | 5 | 2 | 1 | 1 | 50.00% | 19.7384 | +0.8016 | +397.40 | +47.4597 | +23,358.95 | -1.7728 | -874.29 |
| 2026-07-20 | 30 | 27 | 10 | 4 | 1 | 3 | 25.00% | 0.2470 | -1.2547 | -625.81 | +46.2051 | +22,733.14 | -3.0275 | -1,500.10 |
| 2026-07-21 | 15 | 14 | 5 | 4 | 2 | 2 | 50.00% | 1.1115 | +0.1472 | +133.74 | +46.3523 | +22,866.88 | -2.8803 | -1,366.36 |
| 2026-07-22 | 41 | 38 | 17 | 8 | 5 | 3 | 62.50% | 5.7089 | +6.9763 | +3,464.53 | +53.3286 | +26,331.41 | +0.0000 | +0.00 |
| 2026-07-23 | 65 | 43 | 17 | 11 | 8 | 3 | 72.73% | 6.2297 | +11.8417 | +5,478.47 | +65.1702 | +31,809.89 | +0.0000 | +0.00 |
| 2026-07-24 | 58 | 48 | 20 | 8 | 4 | 4 | 50.00% | 2.6215 | +4.2355 | +2,124.16 | +69.4057 | +33,934.04 | +0.0000 | +0.00 |
| 2026-07-27 | 34 | 29 | 14 | 7 | 2 | 5 | 28.57% | 0.4912 | -1.7545 | -919.61 | +67.6512 | +33,014.43 | -1.7545 | -919.61 |
| 2026-07-28 | 53 | 43 | 24 | 10 | 7 | 3 | 70.00% | 5.9645 | +8.0743 | +3,977.04 | +75.7255 | +36,991.47 | +0.0000 | +0.00 |
| 2026-07-29 | 17 | 12 | 3 | 1 | 1 | 0 | 100.00% | ∞ | +0.8497 | +413.42 | +76.5752 | +37,404.89 | +0.0000 | +0.00 |
| 2026-07-30 | 25 | 21 | 5 | 2 | 2 | 0 | 100.00% | ∞ | +1.2706 | +627.44 | +77.8457 | +38,032.33 | +0.0000 | +0.00 |
| 2026-07-31 | 18 | 15 | 4 | 4 | 3 | 1 | 75.00% | 5.2329 | +2.3406 | +1,125.00 | +80.1864 | +39,157.33 | +0.0000 | +0.00 |
| 2026-08-03 | 23 | 19 | 8 | 5 | 3 | 2 | 60.00% | 4.5491 | +4.3181 | +2,184.24 | +84.5044 | +41,341.57 | +0.0000 | +0.00 |
| 2026-08-04 | 11 | 10 | 5 | 2 | 0 | 2 | 0.00% | 0.0000 | -1.3049 | -567.42 | +83.1996 | +40,774.14 | -1.3049 | -567.43 |
| 2026-08-05 | 15 | 11 | 3 | 2 | 0 | 2 | 0.00% | 0.0000 | -1.4217 | -695.06 | +81.7779 | +40,079.08 | -2.7266 | -1,262.49 |
| 2026-08-06 | 21 | 18 | 12 | 9 | 4 | 5 | 44.44% | 1.3978 | +1.2171 | +603.87 | +82.9949 | +40,682.95 | -1.5095 | -658.62 |
| 2026-08-07 | 15 | 14 | 1 | 1 | 1 | 0 | 100.00% | ∞ | +0.8489 | +392.62 | +83.8439 | +41,075.57 | -0.6606 | -265.99 |
| 2026-08-10 | 9 | 9 | 3 | 3 | 1 | 2 | 33.33% | 2.5545 | +1.7315 | +864.22 | +85.5754 | +41,939.79 | +0.0000 | +0.00 |
| 2026-08-11 | 12 | 10 | 4 | 3 | 1 | 2 | 33.33% | 0.4655 | -0.9674 | -334.32 | +84.6080 | +41,605.47 | -0.9674 | -334.32 |
| 2026-08-12 | 17 | 15 | 7 | 4 | 1 | 3 | 25.00% | 0.3448 | -1.6103 | -755.46 | +82.9977 | +40,850.01 | -2.5777 | -1,089.78 |
| 2026-08-13 | 12 | 11 | 3 | 3 | 2 | 1 | 66.67% | 1.4691 | +0.5414 | +259.84 | +83.5391 | +41,109.85 | -2.0363 | -829.94 |
| 2026-08-14 | 7 | 5 | 2 | 1 | 1 | 0 | 100.00% | ∞ | +1.4836 | +735.62 | +85.0227 | +41,845.47 | -0.5527 | -94.32 |
| 2026-08-17 | 10 | 7 | 2 | 1 | 0 | 1 | 0.00% | 0.0000 | -0.6556 | -326.24 | +84.3671 | +41,519.23 | -1.2083 | -420.56 |
| 2026-08-18 | 8 | 8 | 3 | 2 | 2 | 0 | 100.00% | ∞ | +4.3505 | +2,149.48 | +88.7176 | +43,668.71 | +0.0000 | +0.00 |
| 2026-08-19 | 6 | 5 | 2 | 2 | 1 | 1 | 50.00% | 187.3564 | +1.6372 | +811.45 | +90.3549 | +44,480.16 | +0.0000 | +0.00 |
| 2026-08-20 | 14 | 11 | 3 | 2 | 0 | 2 | 0.00% | 0.0000 | -1.7125 | -847.14 | +88.6424 | +43,633.02 | -1.7125 | -847.14 |
| 2026-08-21 | 13 | 11 | 4 | 1 | 0 | 1 | 0.00% | 0.0000 | -0.6508 | -325.30 | +87.9916 | +43,307.72 | -2.3632 | -1,172.44 |
| 2026-08-24 | 16 | 14 | 4 | 2 | 1 | 1 | 50.00% | 4.5300 | +1.3971 | +684.48 | +89.3887 | +43,992.20 | -0.9662 | -487.96 |
| 2026-08-25 | 32 | 26 | 16 | 8 | 5 | 3 | 62.50% | 3.1752 | +4.9659 | +2,483.00 | +94.3546 | +46,475.20 | +0.0000 | +0.00 |
| 2026-08-27 | 22 | 19 | 8 | 6 | 2 | 4 | 33.33% | 1.3921 | +0.9435 | +477.23 | +95.2981 | +46,952.43 | +0.0000 | +0.00 |
| 2026-08-28 | 18 | 15 | 3 | 3 | 2 | 1 | 66.67% | 3.0764 | +1.1462 | +551.40 | +96.4444 | +47,503.84 | +0.0000 | +0.00 |

The supporting `daily_setup_performance.csv` expands every day into each five-minute setup. `selection_entry_exit_detail.csv` contains all selected candidates with five-minute indicators, confirmation candles, entry state and exits.

## 5. Stability by period, month, week and weekday

### Period slices

| Period | Sessions | Raw Base 5M Candidates | Post Overlay Selected | One Minute Confirmed | Fills | Wins | Losses | Flat Trades | Win Rate Pct | Profit Factor | Net Return Points | Net Pnl Rs | Gross Profit Points | Gross Loss Points | Gross Profit Rs | Gross Loss Rs | Average Return Points | Median Return Points | Average Pnl Rs | Median Pnl Rs | Average Win Points | Average Loss Points | Payoff Ratio |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| FULL_65 | 65 | 1,241 | 1,017 | 383 | 229 | 120 | 109 | 0 | 52.40% | 2.2356 | +96.4444 | +47,503.84 | +174.4985 | +78.0541 | +85,015.90 | +37,512.06 | +0.4212 | +0.2306 | +207.44 | +112.70 | +1.4542 | -0.7161 | 2.0307 |
| CORE_59 | 59 | 1,126 | 921 | 345 | 207 | 110 | 97 | 0 | 53.14% | 2.2898 | +90.3549 | +44,480.16 | +160.4090 | +70.0541 | +78,066.75 | +33,586.59 | +0.4365 | +0.2312 | +214.88 | +115.54 | +1.4583 | -0.7222 | 2.0192 |
| FORWARD_6 | 6 | 115 | 96 | 38 | 22 | 10 | 12 | 0 | 45.45% | 1.7612 | +6.0895 | +3,023.68 | +14.0895 | +8.0000 | +6,949.15 | +3,925.47 | +0.2768 | -0.4729 | +137.44 | -224.78 | +1.4090 | -0.6667 | 2.1134 |
| FIRST_HALF_32 | 32 | 497 | 408 | 142 | 93 | 52 | 41 | 0 | 55.91% | 2.5152 | +49.2326 | +24,233.24 | +81.7239 | +32.4914 | +39,702.34 | +15,469.10 | +0.5294 | +0.3642 | +260.57 | +168.29 | +1.5716 | -0.7925 | 1.9832 |
| SECOND_HALF_33 | 33 | 744 | 609 | 241 | 136 | 68 | 68 | 0 | 50.00% | 2.0362 | +47.2118 | +23,270.60 | +92.7746 | +45.5628 | +45,313.56 | +22,042.97 | +0.3471 | +0.0348 | +171.11 | +17.87 | +1.3643 | -0.6700 | 2.0362 |
| LAST_14_USABLE | 14 | 196 | 166 | 64 | 41 | 19 | 22 | 0 | 46.34% | 1.8290 | +12.6005 | +6,428.26 | +27.8009 | +15.2004 | +13,741.71 | +7,313.44 | +0.3073 | -0.3958 | +156.79 | -180.36 | +1.4632 | -0.6909 | 2.1177 |

### Monthly

| Period | Sessions | Raw Base 5M Candidates | Post Overlay Selected | One Minute Confirmed | Fills | Wins | Losses | Flat Trades | Win Rate Pct | Profit Factor | Net Return Points | Net Pnl Rs | Gross Profit Points | Gross Loss Points | Gross Profit Rs | Gross Loss Rs | Average Return Points | Median Return Points | Average Pnl Rs | Median Pnl Rs | Average Win Points | Average Loss Points | Payoff Ratio |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 2026-05 | 2 | 33 | 23 | 7 | 6 | 4 | 2 | 0 | 66.67% | 7.3628 | +7.0839 | +3,498.36 | +8.1972 | +1.1133 | +4,053.35 | +554.99 | +1.1806 | +1.1898 | +583.06 | +592.57 | +2.0493 | -0.5567 | 3.6814 |
| 2026-06 | 21 | 271 | 214 | 74 | 46 | 21 | 25 | 0 | 45.65% | 1.4383 | +9.2688 | +4,548.33 | +30.4171 | +21.1483 | +14,666.97 | +10,118.64 | +0.2015 | -0.4147 | +98.88 | -197.72 | +1.4484 | -0.8459 | 1.7122 |
| 2026-07 | 23 | 656 | 542 | 209 | 117 | 68 | 49 | 0 | 58.12% | 2.9004 | +63.8337 | +31,110.64 | +97.4232 | +33.5895 | +47,307.87 | +16,197.23 | +0.5456 | +0.3788 | +265.90 | +188.57 | +1.4327 | -0.6855 | 2.0900 |
| 2026-08 | 19 | 281 | 238 | 93 | 60 | 27 | 33 | 0 | 45.00% | 1.7322 | +16.2580 | +8,346.51 | +38.4610 | +22.2030 | +18,987.72 | +10,641.21 | +0.2710 | -0.5502 | +139.11 | -264.14 | +1.4245 | -0.6728 | 2.1172 |

### Weekly

| Period | Sessions | Raw Base 5M Candidates | Post Overlay Selected | One Minute Confirmed | Fills | Wins | Losses | Flat Trades | Win Rate Pct | Profit Factor | Net Return Points | Net Pnl Rs | Gross Profit Points | Gross Loss Points | Gross Profit Rs | Gross Loss Rs | Average Return Points | Median Return Points | Average Pnl Rs | Median Pnl Rs | Average Win Points | Average Loss Points | Payoff Ratio |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 2026-W22 | 2 | 33 | 23 | 7 | 6 | 4 | 2 | 0 | 66.67% | 7.3628 | +7.0839 | +3,498.36 | +8.1972 | +1.1133 | +4,053.35 | +554.99 | +1.1806 | +1.1898 | +583.06 | +592.57 | +2.0493 | -0.5567 | 3.6814 |
| 2026-W23 | 5 | 64 | 57 | 24 | 16 | 8 | 8 | 0 | 50.00% | 1.6073 | +4.9373 | +2,357.32 | +13.0678 | +8.1305 | +6,354.34 | +3,997.02 | +0.3086 | -0.1630 | +147.33 | -82.57 | +1.6335 | -1.0163 | 1.6073 |
| 2026-W24 | 5 | 61 | 39 | 10 | 5 | 2 | 3 | 0 | 40.00% | 0.3846 | -1.5131 | -736.90 | +0.9458 | +2.4589 | +472.13 | +1,209.03 | -0.3026 | -0.6515 | -147.38 | -325.62 | +0.4729 | -0.8196 | 0.5770 |
| 2026-W25 | 5 | 59 | 50 | 13 | 8 | 3 | 5 | 0 | 37.50% | 1.4523 | +1.2576 | +627.32 | +4.0385 | +2.7808 | +1,987.93 | +1,360.60 | +0.1572 | -0.3140 | +78.42 | -156.24 | +1.3462 | -0.5562 | 2.4204 |
| 2026-W26 | 4 | 51 | 38 | 14 | 9 | 4 | 5 | 0 | 44.44% | 1.4875 | +2.0301 | +1,075.21 | +6.1941 | +4.1640 | +2,837.83 | +1,762.61 | +0.2256 | -0.5525 | +119.47 | -274.27 | +1.5485 | -0.8328 | 1.8594 |
| 2026-W27 | 5 | 95 | 85 | 28 | 18 | 10 | 8 | 0 | 55.56% | 1.7742 | +5.9812 | +3,077.63 | +13.7068 | +7.7256 | +6,671.97 | +3,594.34 | +0.3323 | +0.0559 | +170.98 | +27.18 | +1.3707 | -0.9657 | 1.4194 |
| 2026-W28 | 5 | 111 | 97 | 40 | 27 | 18 | 9 | 0 | 66.67% | 4.9620 | +24.0531 | +11,670.58 | +30.1241 | +6.0710 | +14,637.54 | +2,966.96 | +0.8909 | +0.8495 | +432.24 | +421.03 | +1.6736 | -0.6746 | 2.4810 |
| 2026-W29 | 5 | 130 | 100 | 35 | 21 | 9 | 12 | 0 | 42.86% | 1.4303 | +3.6296 | +1,789.42 | +12.0638 | +8.4343 | +5,966.99 | +4,177.57 | +0.1728 | -0.0473 | +85.21 | -23.55 | +1.3404 | -0.7029 | 1.9071 |
| 2026-W30 | 5 | 209 | 170 | 69 | 35 | 20 | 15 | 0 | 57.14% | 3.3484 | +21.9460 | +10,575.09 | +31.2912 | +9.3452 | +15,064.38 | +4,489.29 | +0.6270 | +0.2097 | +302.15 | +89.20 | +1.5646 | -0.6230 | 2.5113 |
| 2026-W31 | 5 | 147 | 120 | 50 | 24 | 15 | 9 | 0 | 62.50% | 2.9157 | +10.7806 | +5,223.28 | +16.4083 | +5.6277 | +7,981.72 | +2,758.44 | +0.4492 | +0.6331 | +217.64 | +288.65 | +1.0939 | -0.6253 | 1.7494 |
| 2026-W32 | 5 | 85 | 72 | 29 | 19 | 8 | 11 | 0 | 42.11% | 1.5223 | +3.6575 | +1,918.25 | +10.6600 | +7.0025 | +5,246.01 | +3,327.77 | +0.1925 | -0.5511 | +100.96 | -268.37 | +1.3325 | -0.6366 | 2.0932 |
| 2026-W33 | 5 | 57 | 50 | 19 | 14 | 6 | 8 | 0 | 42.86% | 1.1804 | +1.1788 | +769.90 | +7.7149 | +6.5360 | +3,828.14 | +3,058.24 | +0.0842 | -0.5569 | +54.99 | -274.78 | +1.2858 | -0.8170 | 1.5738 |
| 2026-W34 | 5 | 51 | 42 | 14 | 8 | 3 | 5 | 0 | 37.50% | 1.9806 | +2.9689 | +1,462.25 | +5.9965 | +3.0276 | +2,964.42 | +1,502.17 | +0.3711 | -0.2827 | +182.78 | -140.27 | +1.9988 | -0.6055 | 3.3010 |
| 2026-W35 | 4 | 88 | 74 | 31 | 19 | 10 | 9 | 0 | 52.63% | 2.4996 | +8.4527 | +4,196.12 | +14.0895 | +5.6368 | +6,949.15 | +2,753.03 | +0.4449 | +0.4444 | +220.85 | +220.41 | +1.4090 | -0.6263 | 2.2496 |

### Weekday

| Period | Sessions | Raw Base 5M Candidates | Post Overlay Selected | One Minute Confirmed | Fills | Wins | Losses | Flat Trades | Win Rate Pct | Profit Factor | Net Return Points | Net Pnl Rs | Gross Profit Points | Gross Loss Points | Gross Profit Rs | Gross Loss Rs | Average Return Points | Median Return Points | Average Pnl Rs | Median Pnl Rs | Average Win Points | Average Loss Points | Payoff Ratio |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| Monday | 13 | 241 | 197 | 70 | 40 | 19 | 21 | 0 | 47.50% | 2.2525 | +15.7277 | +7,745.57 | +28.2844 | +12.5567 | +13,874.95 | +6,129.38 | +0.3932 | -0.2215 | +193.64 | -101.95 | +1.4887 | -0.5979 | 2.4896 |
| Tuesday | 13 | 274 | 219 | 97 | 52 | 28 | 24 | 0 | 53.85% | 2.2525 | +22.2822 | +11,160.08 | +40.0722 | +17.7900 | +19,639.84 | +8,479.76 | +0.4285 | +0.3097 | +214.62 | +147.54 | +1.4312 | -0.7412 | 1.9307 |
| Wednesday | 13 | 218 | 181 | 79 | 47 | 26 | 21 | 0 | 55.32% | 2.6393 | +26.2626 | +12,886.43 | +42.2829 | +16.0203 | +20,424.90 | +7,538.47 | +0.5588 | +0.5146 | +274.18 | +257.27 | +1.6263 | -0.7629 | 2.1318 |
| Thursday | 13 | 253 | 207 | 75 | 50 | 24 | 26 | 0 | 48.00% | 1.5985 | +12.0863 | +5,806.61 | +32.2814 | +20.1950 | +15,566.27 | +9,759.66 | +0.2417 | -0.5502 | +116.13 | -270.69 | +1.3451 | -0.7767 | 1.7317 |
| Friday | 13 | 255 | 213 | 62 | 40 | 23 | 17 | 0 | 57.50% | 2.7478 | +20.0855 | +9,905.16 | +31.5776 | +11.4921 | +15,509.95 | +5,604.79 | +0.5021 | +0.3115 | +247.63 | +153.37 | +1.3729 | -0.6760 | 2.0310 |

### Consecutive ten-session blocks

| Period | Sessions | Raw Base 5M Candidates | Post Overlay Selected | One Minute Confirmed | Fills | Wins | Losses | Flat Trades | Win Rate Pct | Profit Factor | Net Return Points | Net Pnl Rs | Gross Profit Points | Gross Loss Points | Gross Profit Rs | Gross Loss Rs | Average Return Points | Median Return Points | Average Pnl Rs | Median Pnl Rs | Average Win Points | Average Loss Points | Payoff Ratio |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| B1: 2026-05-27..2026-06-10 | 10 | 142 | 105 | 37 | 24 | 13 | 11 | 0 | 54.17% | 2.2134 | +12.0071 | +5,848.40 | +21.9024 | +9.8953 | +10,726.02 | +4,877.63 | +0.5003 | +0.3891 | +243.68 | +190.58 | +1.6848 | -0.8996 | 1.8729 |
| B2: 2026-06-11..2026-06-24 | 10 | 107 | 85 | 24 | 16 | 5 | 11 | 0 | 31.25% | 0.8255 | -1.4136 | -605.69 | +6.6879 | +8.1015 | +3,089.02 | +3,694.71 | -0.0884 | -0.4972 | -37.86 | -238.70 | +1.3376 | -0.7365 | 1.8161 |
| B3: 2026-06-25..2026-07-09 | 10 | 206 | 184 | 69 | 43 | 27 | 16 | 0 | 62.79% | 3.1036 | +28.0691 | +13,759.31 | +41.4123 | +13.3432 | +20,110.74 | +6,351.43 | +0.6528 | +0.8446 | +319.98 | +393.47 | +1.5338 | -0.8340 | 1.8392 |
| B4: 2026-07-10..2026-07-23 | 10 | 300 | 237 | 90 | 54 | 29 | 25 | 0 | 53.70% | 2.6291 | +26.5076 | +12,807.87 | +42.7790 | +16.2713 | +20,718.32 | +7,910.45 | +0.4909 | +0.1563 | +237.18 | +65.36 | +1.4751 | -0.6509 | 2.2665 |
| B5: 2026-07-24..2026-08-06 | 10 | 275 | 226 | 98 | 50 | 26 | 24 | 0 | 52.00% | 2.1694 | +17.8247 | +8,873.06 | +33.0670 | +15.2423 | +16,237.47 | +7,364.40 | +0.3565 | +0.2731 | +177.46 | +135.97 | +1.2718 | -0.6351 | 2.0025 |
| B6: 2026-08-07..2026-08-20 | 10 | 110 | 95 | 30 | 22 | 10 | 12 | 0 | 45.45% | 1.6336 | +5.6474 | +2,950.07 | +14.5603 | +8.9129 | +7,185.18 | +4,235.12 | +0.2567 | -0.2818 | +134.09 | -137.42 | +1.4560 | -0.7427 | 1.9604 |
| B7: 2026-08-21..2026-08-28 | 5 | 101 | 85 | 35 | 20 | 10 | 10 | 0 | 50.00% | 2.2409 | +7.8020 | +3,870.82 | +14.0895 | +6.2876 | +6,949.15 | +3,078.33 | +0.3901 | +0.0243 | +193.54 | +20.02 | +1.4090 | -0.6288 | 2.2409 |

### Daily activity/range/side regimes

| Regime Dimension | Regime | Sessions | Measure Min | Measure Median | Measure Max | Fills | Positive Days | Negative Days | Net Return Points | Net Pnl Rs | Average Daily Pnl Rs |
|---|---|---|---|---|---|---|---|---|---|---|---|
| candidate_activity | LOW | 26 | 1.0000 | 9.0000 | 11.0000 | 47 | 7 | 16 | -4.6743 | -1,963.72 | -75.53 |
| candidate_activity | MID | 20 | 12.0000 | 15.0000 | 17.0000 | 59 | 16 | 3 | +33.5101 | +16,645.31 | +832.27 |
| candidate_activity | HIGH | 19 | 18.0000 | 22.0000 | 48.0000 | 123 | 14 | 5 | +67.6085 | +32,822.25 | +1,727.49 |
| five_min_range | LOW | 22 | 0.5153 | 0.6187 | 0.6613 | 63 | 11 | 10 | +21.3102 | +10,518.92 | +478.13 |
| five_min_range | MID | 21 | 0.6716 | 0.7310 | 0.7815 | 88 | 13 | 6 | +46.1432 | +22,783.29 | +1,084.92 |
| five_min_range | HIGH | 22 | 0.7934 | 0.8535 | 1.2638 | 78 | 13 | 8 | +28.9910 | +14,201.63 | +645.53 |
| long_share | LOW | 22 | 0.0000 | 23.2684 | 35.7143 | 90 | 13 | 8 | +59.8696 | +29,147.90 | +1,324.90 |
| long_share | MID | 21 | 36.3636 | 50.0000 | 62.5000 | 67 | 12 | 8 | +15.7431 | +7,918.96 | +377.09 |
| long_share | HIGH | 22 | 63.1579 | 73.5088 | 100.0000 | 72 | 12 | 8 | +20.8317 | +10,436.98 | +474.41 |

Higher candidate activity coincides with materially better results in this sample. That is a market-regime hypothesis, not evidence for a same-history minimum-breadth threshold.

Rolling ten-session net P&L ranges from Rs -2,313.29 to Rs +18,608.43. Full windows are in `rolling_10_session_metrics.csv`.

## 6. V12 selection mechanism and comparison

### Frozen comparators

| Strategy | Sessions | Fills | Wins | Losses | Win Rate Pct | Profit Factor | Net Return Points | Net Pnl Rs | Max Daily Drawdown Points |
|---|---|---|---|---|---|---|---|---|---|
| V10 frozen | 65 | 232 | 116 | 116 | 50.00% | 1.8327 | +73.0544 | +36,312.05 | +9.3513 |
| V11 frozen | 65 | 237 | 123 | 114 | 51.90% | 2.1452 | +94.6309 | +46,783.23 | +8.5674 |
| V12 selected | 65 | 229 | 120 | 109 | 52.40% | 2.2356 | +96.4444 | +47,503.84 | +5.2693 |

### Day-wise V12 minus V11 — changed sessions

| Session Date | Fills V11 | Fills V12 | Net Return Pct V11 | Net Return Pct V12 | Delta Net Return Points | Net Pnl Rs V11 | Net Pnl Rs V12 | Delta Net Pnl Rs | Cumulative Delta Net Return Points |
|---|---|---|---|---|---|---|---|---|---|
| 2026-05-27 | 4 | 3 | 2.7824 | 1.8217 | -0.9607 | +1,386.50 | +907.49 | -479.01 | -0.9607 |
| 2026-06-09 | 4 | 2 | -2.3166 | -0.0141 | +2.3025 | -973.67 | -7.28 | +966.39 | +1.3418 |
| 2026-06-10 | 1 | 0 | -0.9956 | 0.0000 | +0.9956 | -488.38 | +0.00 | +488.38 | +2.3374 |
| 2026-06-29 | 7 | 6 | 5.8985 | 4.8608 | -1.0377 | +2,872.14 | +2,370.99 | -501.16 | +1.2997 |
| 2026-07-09 | 4 | 3 | -2.5823 | -2.4543 | +0.1280 | -1,250.56 | -1,192.41 | +58.15 | +1.4277 |
| 2026-07-10 | 7 | 6 | 4.0132 | 5.1675 | +1.1544 | +1,997.44 | +2,567.51 | +570.08 | +2.5821 |
| 2026-07-14 | 5 | 5 | -1.5368 | -1.5298 | +0.0070 | -766.48 | -763.09 | +3.39 | +2.5891 |
| 2026-07-20 | 5 | 4 | -0.4791 | -1.2547 | -0.7756 | -240.19 | -625.81 | -385.62 | +1.8135 |

### Exact mechanism accounting

| Mechanism | Count | Wins | Losses | Profit Factor | Net Return Points Effect | Net Pnl Rs Effect |
|---|---|---|---|---|---|---|
| V11 selected candidates excluded by V12 late-SHORT filters | 117 | — | — | — | — | — |
| V11 fills removed by V12 | 8 | 3 | 5 | 0.6056 | +1.8065 | +717.21 |
| V12 fills added versus V11 | 0 | — | — | — | +0.0000 | +0.00 |
| Common-fill economics changed after reranking/ledger ordering | 1 | — | — | — | +0.0070 | +3.39 |
| Total V12 minus V11 (changed sessions) | 8 | — | — | — | +1.8135 | +720.61 |

### Changed fills/economics

| Candidate Id | Session Date | Setup Id | Side | Symbol | Change Type | Net Return Pct V11 | Net Return Pct V12 | Delta Net Return Points | Net Pnl Rs V11 | Net Pnl Rs V12 | Delta Net Pnl Rs |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 2026-05-27\|09:45_SHORT\|RVNL | 2026-05-27 | 09:45_SHORT | SHORT | RVNL | V11_FILL_REMOVED_IN_V12 | 0.9607 | — | -0.9607 | +479.01 | — | -479.01 |
| 2026-06-09\|09:40_SHORT\|INFY | 2026-06-09 | 09:40_SHORT | SHORT | INFY | V11_FILL_REMOVED_IN_V12 | -1.1525 | — | +1.1525 | -568.49 | — | +568.49 |
| 2026-06-09\|09:45_SHORT\|FORCEMOT | 2026-06-09 | 09:45_SHORT | SHORT | FORCEMOT | V11_FILL_REMOVED_IN_V12 | -1.1500 | — | +1.1500 | -397.90 | — | +397.90 |
| 2026-06-10\|09:45_SHORT\|MUTHOOTFIN | 2026-06-10 | 09:45_SHORT | SHORT | MUTHOOTFIN | V11_FILL_REMOVED_IN_V12 | -0.9956 | — | +0.9956 | -488.38 | — | +488.38 |
| 2026-06-29\|09:40_SHORT\|TATAELXSI | 2026-06-29 | 09:40_SHORT | SHORT | TATAELXSI | V11_FILL_REMOVED_IN_V12 | 1.0377 | — | -1.0377 | +501.16 | — | -501.16 |
| 2026-07-09\|09:45_SHORT\|POLYCAB | 2026-07-09 | 09:45_SHORT | SHORT | POLYCAB | V11_FILL_REMOVED_IN_V12 | -0.1280 | — | +0.1280 | -58.15 | — | +58.15 |
| 2026-07-10\|09:40_SHORT\|DRREDDY | 2026-07-10 | 09:40_SHORT | SHORT | DRREDDY | V11_FILL_REMOVED_IN_V12 | -1.1544 | — | +1.1544 | -570.08 | — | +570.08 |
| 2026-07-14\|09:40_SHORT\|WAAREEENER | 2026-07-14 | 09:40_SHORT | SHORT | WAAREEENER | COMMON_FILL_ECONOMICS_CHANGED | 0.4091 | 0.4162 | +0.0070 | +196.54 | +199.94 | +3.39 |
| 2026-07-20\|09:45_SHORT\|BHEL | 2026-07-20 | 09:45_SHORT | SHORT | BHEL | V11_FILL_REMOVED_IN_V12 | 0.7756 | — | -0.7756 | +385.62 | — | -385.62 |

### V11 counterfactual outcomes among candidates excluded by V12

| Setup Id | V11 Candidates Removed By V12 | V11 Confirmed | Fills | Wins | Losses | Win Rate Pct | Profit Factor | Net Return Points | Net Pnl Rs |
|---|---|---|---|---|---|---|---|---|---|
| 09:40_SHORT | 81 | 16 | 3 | 1 | 2 | 33.33% | 0.4498 | -1.2692 | -637.41 |
| 09:45_SHORT | 36 | 10 | 5 | 2 | 3 | 40.00% | 0.7637 | -0.5373 | -79.81 |

These are V11-control outcomes for candidates removed by V12, not outcomes observed after rejection. Portfolio displacement means their arithmetic total is descriptive rather than a causal decomposition.

### Predeclared development gate for selected V12

| Variant Id | Affected Decisions | Net Ratio Reference 15 0 | Pf Delta Reference 15 0 | Net Ratio Stress 25 5 | Pf Delta Stress 25 5 | Reference Mdd Ratio | Reference Fill Retention | Ex July Delta Points | Forward Extension Delta Points | Both Sides Harsh Positive | Gate Status | Observed Rank | Gate Passing Rank |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| V12_S06_LATE_SHORT_VOLUME_MIN_150 | 118 | 1.0192 | 0.0904 | 1.0526 | 0.0629 | 0.6150 | 0.9662 | +1.2997 | +0.0000 | Yes | PASS | 3 | 1.0000 |

### Paired V12-minus-V11 uncertainty

| Scenario | Paired Sessions | Observed Delta Net Points | Observed Delta Net Pnl Rs | Bootstrap Delta Sum P025 | Bootstrap Delta Sum Median | Bootstrap Delta Sum P975 | Bootstrap Probability Delta Positive | Positive Delta Sessions | Negative Delta Sessions | Zero Delta Sessions | Max Cumulative Delta Drawdown Points |
|---|---|---|---|---|---|---|---|---|---|---|---|
| REFERENCE_15_0 | 65 | +1.8135 | +720.61 | -4.1148 | 1.5240 | 8.5459 | 0.6950 | 5 | 3 | 57 | +1.0377 |

The interval crossing zero means V12's incremental advantage over V11 is not statistically decisive. This bootstrap is conditional on the selected history and does not correct the 39-challenger winner-selection process.

### Top observed V12 variants on the development history

| Variant Id | Stage Id | Family | Fills | Win Rate Pct | Profit Factor | Net Return Points | Net Pnl Rs | Max Daily Drawdown Points | Gate Status | Observed Rank | Gate Passing Rank |
|---|---|---|---|---|---|---|---|---|---|---|---|
| V12_S06_0935_LONG_VOLUME_MIN_125 | STAGE_06A_0935_LONG_VOLUME | SELECTION_FIVE_MINUTE_VOLUME_MIN | 233 | 52.36% | 2.2142 | +97.0295 | +47,965.51 | +8.5674 | INSUFFICIENT | 1.0000 | — |
| V12_S07_LONG_ENTRY_EXPIRY_3 | STAGE_07_LONG_ENTRY_EXPIRY | ENTRY_LONG_EXPIRY | 230 | 52.61% | 2.2320 | +96.9097 | +47,943.83 | +8.5674 | INSUFFICIENT | 2.0000 | — |
| V12_S06_LATE_SHORT_VOLUME_MIN_150 | STAGE_06D_LATE_SHORT_VOLUME | SELECTION_FIVE_MINUTE_VOLUME_MIN | 229 | 52.40% | 2.2356 | +96.4444 | +47,503.84 | +5.2693 | PASS | 3.0000 | 1.0000 |
| V12_S06_0935_LONG_VOLUME_MIN_150 | STAGE_06A_0935_LONG_VOLUME | SELECTION_FIVE_MINUTE_VOLUME_MIN | 233 | 52.36% | 2.1918 | +96.0698 | +47,475.82 | +8.1067 | FAIL | 4.0000 | — |
| V12_S06_0940_SHORT_VOLUME_MIN_150 | STAGE_06B_0940_SHORT_VOLUME | SELECTION_FIVE_MINUTE_VOLUME_MIN | 234 | 52.14% | 2.1939 | +95.9071 | +47,424.03 | +7.4148 | FAIL | 5.0000 | — |
| V12_S05_0925_BOTH_MOVE_MAX_125 | STAGE_05C_0925_BOTH_STRETCH | SELECTION_0925_OPENING_STRETCH | 233 | 52.36% | 2.1811 | +95.5353 | +47,235.72 | +8.5674 | INSUFFICIENT | 6.0000 | — |
| V12_S07_LONG_ENTRY_EXPIRY_4 | STAGE_07_LONG_ENTRY_EXPIRY | ENTRY_LONG_EXPIRY | 234 | 52.14% | 2.1799 | +95.4897 | +47,206.63 | +8.5674 | INSUFFICIENT | 7.0000 | — |
| V12_S04_M2_0930_SHORT_DELAY_S4 | STAGE_04B_M2_0930_SHORT | ENTRY_M2_SHORT | 232 | 52.16% | 2.1876 | +95.3391 | +46,976.02 | +8.5674 | INSUFFICIENT | 8.0000 | — |
| V12_S05_0925_SHORT_ONLY_MOVE_MAX_125 | STAGE_05B_0925_SHORT_STRETCH | SELECTION_0925_OPENING_STRETCH | 236 | 52.12% | 2.1622 | +95.2825 | +47,106.56 | +8.5674 | INSUFFICIENT | 9.0000 | — |
| V12_S06_0945_SHORT_VOLUME_MIN_150 | STAGE_06C_0945_SHORT_VOLUME | SELECTION_FIVE_MINUTE_VOLUME_MIN | 232 | 52.16% | 2.1843 | +95.1681 | +46,863.03 | +6.4218 | FAIL | 10.0000 | — |
| V12_S05_0925_LONG_ONLY_MOVE_MAX_125 | STAGE_05A_0925_LONG_STRETCH | SELECTION_0925_OPENING_STRETCH | 234 | 52.14% | 2.1637 | +94.8837 | +46,912.39 | +8.5674 | INSUFFICIENT | 11.0000 | — |
| V12_S06_0945_SHORT_VOLUME_MIN_125 | STAGE_06C_0945_SHORT_VOLUME | SELECTION_FIVE_MINUTE_VOLUME_MIN | 235 | 51.91% | 2.1637 | +94.8201 | +46,702.12 | +7.4174 | INSUFFICIENT | 12.0000 | — |
| V12_S06_LATE_SHORT_VOLUME_MIN_125 | STAGE_06D_LATE_SHORT_VOLUME | SELECTION_FIVE_MINUTE_VOLUME_MIN | 235 | 51.91% | 2.1637 | +94.8201 | +46,702.12 | +7.4174 | FAIL | 13.0000 | — |
| V11_STAGE0_FROZEN_CONTROL | STAGE_00_FROZEN_V11 | CONTROL | 237 | 51.90% | 2.1452 | +94.6309 | +46,783.23 | +8.5674 | — | — | — |
| V12_S06_0940_SHORT_VOLUME_MIN_125 | STAGE_06B_0940_SHORT_VOLUME | SELECTION_FIVE_MINUTE_VOLUME_MIN | 237 | 51.90% | 2.1452 | +94.6309 | +46,783.23 | +8.5674 | FAIL | 14.0000 | — |

## 7. Cost and slippage robustness

| Scenario | Cost Bps | Slippage Bps | Fills | Wins | Losses | Win Rate Pct | Profit Factor | Net Return Points | Net Pnl Rs | Max Daily Drawdown Points | Net Pnl Retained Vs Reference Pct |
|---|---|---|---|---|---|---|---|---|---|---|---|
| REFERENCE_15_0 | 15.0000 | 0.0000 | 229 | 120 | 109 | 52.40% | 2.2356 | +96.4444 | +47,503.84 | +5.2693 | 100.00% |
| STRESS_20_2 | 20.0000 | 2.0000 | 229 | 116 | 113 | 50.66% | 1.9423 | +80.3389 | +39,710.99 | +6.2661 | 83.60% |
| STRESS_25_5 | 25.0000 | 5.0000 | 229 | 111 | 118 | 48.47% | 1.6286 | +59.8045 | +29,759.08 | +7.2787 | 62.65% |

### Setup-level reference versus harsh stress

| Setup Id | Fills Reference | Wins Reference | Losses Reference | Profit Factor Reference | Net Return Points Reference | Profit Factor Harsh | Net Return Points Harsh | Net Pnl Rs Harsh |
|---|---|---|---|---|---|---|---|---|
| 09:25_LONG | 61 | 32 | 29 | 1.6844 | +11.0095 | 1.1781 | +3.4969 | +1,601.07 |
| 09:25_SHORT | 62 | 28 | 34 | 2.0559 | +22.3665 | 1.2756 | +7.6596 | +3,845.71 |
| 09:40_SHORT | 15 | 8 | 7 | 1.8434 | +6.1070 | 1.5368 | +4.2908 | +2,439.86 |
| 09:30_SHORT | 19 | 12 | 7 | 3.3855 | +14.7457 | 2.7210 | +12.2085 | +5,938.20 |
| 09:35_SHORT | 10 | 6 | 4 | 3.1215 | +9.7864 | 2.7330 | +8.6765 | +4,364.92 |
| 09:45_LONG | 9 | 6 | 3 | 4.7245 | +8.7639 | 3.8410 | +7.7004 | +3,851.92 |
| 09:30_LONG | 11 | 5 | 6 | 1.6174 | +3.5120 | 1.3437 | +2.1976 | +1,056.49 |
| 09:35_LONG | 17 | 9 | 8 | 1.5052 | +3.7417 | 1.1360 | +1.1826 | +654.86 |
| 09:45_SHORT | 7 | 5 | 2 | 3.8060 | +4.0221 | 1.6476 | +1.9071 | +987.08 |
| 09:40_LONG | 18 | 9 | 9 | 3.1108 | +12.3896 | 2.5265 | +10.4845 | +5,018.97 |

Fixed bps cases do not reproduce bid/ask spread, futures basis, depth, latency, partial fills, rejects or exit impact.

## 8. Five-minute indicator study

### Cohort distributions

| Indicator | Cohort | Observations | Mean | Median | P25 | P75 |
|---|---|---|---|---|---|---|
| directional_move_pct | ALL_SELECTED | 1,017 | 0.5949 | 0.4803 | 0.3281 | 0.7361 |
| directional_move_pct | CONFIRMED | 383 | 0.5964 | 0.5371 | 0.3361 | 0.7500 |
| directional_move_pct | FILLED | 229 | 0.6331 | 0.5611 | 0.3692 | 0.7972 |
| directional_move_pct | WINNERS | 120 | 0.5996 | 0.5399 | 0.3318 | 0.7536 |
| directional_move_pct | LOSERS | 109 | 0.6700 | 0.6016 | 0.4185 | 0.8364 |
| oi_change_pct | ALL_SELECTED | 1,017 | 5.8087 | 0.8383 | 0.3453 | 1.8957 |
| oi_change_pct | CONFIRMED | 383 | 11.6685 | 0.8333 | 0.3716 | 1.9273 |
| oi_change_pct | FILLED | 229 | 17.9501 | 0.8840 | 0.3604 | 2.2727 |
| oi_change_pct | WINNERS | 120 | 3.1163 | 0.9016 | 0.3148 | 2.5470 |
| oi_change_pct | LOSERS | 109 | 34.2809 | 0.8827 | 0.4489 | 1.9198 |
| volume_ratio | ALL_SELECTED | 1,017 | 3.3735 | 2.6532 | 1.8620 | 4.1321 |
| volume_ratio | CONFIRMED | 383 | 3.4383 | 2.8397 | 1.9256 | 4.1661 |
| volume_ratio | FILLED | 229 | 3.6315 | 3.0209 | 1.9657 | 4.4606 |
| volume_ratio | WINNERS | 120 | 3.4187 | 3.0188 | 1.8960 | 4.4094 |
| volume_ratio | LOSERS | 109 | 3.8657 | 3.0891 | 2.1032 | 4.7061 |
| traded_value_cr | ALL_SELECTED | 1,017 | 27.4297 | 14.8695 | 6.9562 | 31.3387 |
| traded_value_cr | CONFIRMED | 383 | 26.9069 | 15.4433 | 7.2652 | 30.4582 |
| traded_value_cr | FILLED | 229 | 31.0515 | 17.7519 | 8.4819 | 36.1087 |
| traded_value_cr | WINNERS | 120 | 34.6111 | 17.9240 | 8.2530 | 39.6939 |
| traded_value_cr | LOSERS | 109 | 27.1326 | 17.7519 | 8.7378 | 32.4418 |
| five_min_range_pct | ALL_SELECTED | 1,017 | 0.8343 | 0.7215 | 0.5415 | 0.9743 |
| five_min_range_pct | CONFIRMED | 383 | 0.8443 | 0.7606 | 0.5608 | 1.0136 |
| five_min_range_pct | FILLED | 229 | 0.8897 | 0.8060 | 0.6057 | 1.0410 |
| five_min_range_pct | WINNERS | 120 | 0.8580 | 0.7588 | 0.5938 | 0.9930 |
| five_min_range_pct | LOSERS | 109 | 0.9246 | 0.8167 | 0.6163 | 1.1502 |
| five_min_body_ratio | ALL_SELECTED | 1,017 | 0.6953 | 0.7134 | 0.5625 | 0.8402 |
| five_min_body_ratio | CONFIRMED | 383 | 0.6925 | 0.7134 | 0.5795 | 0.8376 |
| five_min_body_ratio | FILLED | 229 | 0.6935 | 0.7204 | 0.5784 | 0.8384 |
| five_min_body_ratio | WINNERS | 120 | 0.6885 | 0.7208 | 0.5556 | 0.8416 |
| five_min_body_ratio | LOSERS | 109 | 0.6990 | 0.7204 | 0.6000 | 0.8211 |
| five_min_adverse_wick_ratio | ALL_SELECTED | 1,017 | 0.1807 | 0.1538 | 0.0676 | 0.2625 |
| five_min_adverse_wick_ratio | CONFIRMED | 383 | 0.1835 | 0.1616 | 0.0687 | 0.2667 |
| five_min_adverse_wick_ratio | FILLED | 229 | 0.1883 | 0.1667 | 0.0701 | 0.2712 |
| five_min_adverse_wick_ratio | WINNERS | 120 | 0.1955 | 0.1584 | 0.0740 | 0.3034 |
| five_min_adverse_wick_ratio | LOSERS | 109 | 0.1803 | 0.1786 | 0.0698 | 0.2381 |
| five_min_directional_close_location | ALL_SELECTED | 1,017 | 0.8193 | 0.8462 | 0.7375 | 0.9324 |
| five_min_directional_close_location | CONFIRMED | 383 | 0.8165 | 0.8384 | 0.7333 | 0.9313 |
| five_min_directional_close_location | FILLED | 229 | 0.8117 | 0.8333 | 0.7288 | 0.9299 |
| five_min_directional_close_location | WINNERS | 120 | 0.8045 | 0.8416 | 0.6966 | 0.9260 |
| five_min_directional_close_location | LOSERS | 109 | 0.8197 | 0.8214 | 0.7619 | 0.9302 |
| ema_fast_gap_pct | ALL_SELECTED | 1,017 | 0.4697 | 0.3392 | 0.1975 | 0.5846 |
| ema_fast_gap_pct | CONFIRMED | 383 | 0.4634 | 0.3351 | 0.1935 | 0.5836 |
| ema_fast_gap_pct | FILLED | 229 | 0.4772 | 0.3461 | 0.2065 | 0.5975 |
| ema_fast_gap_pct | WINNERS | 120 | 0.5147 | 0.3460 | 0.1777 | 0.6138 |
| ema_fast_gap_pct | LOSERS | 109 | 0.4360 | 0.3461 | 0.2173 | 0.5916 |
| ema_slow_gap_pct | ALL_SELECTED | 1,017 | 0.4153 | 0.3038 | 0.1567 | 0.5260 |
| ema_slow_gap_pct | CONFIRMED | 383 | 0.4043 | 0.3033 | 0.1526 | 0.5175 |
| ema_slow_gap_pct | FILLED | 229 | 0.4150 | 0.3143 | 0.1481 | 0.5226 |
| ema_slow_gap_pct | WINNERS | 120 | 0.4354 | 0.2827 | 0.1383 | 0.5139 |
| ema_slow_gap_pct | LOSERS | 109 | 0.3925 | 0.3374 | 0.1671 | 0.5517 |
| ema_total_gap_pct | ALL_SELECTED | 1,017 | 0.8850 | 0.6461 | 0.3871 | 1.0950 |
| ema_total_gap_pct | CONFIRMED | 383 | 0.8678 | 0.6560 | 0.3811 | 1.0874 |
| ema_total_gap_pct | FILLED | 229 | 0.8922 | 0.7254 | 0.3811 | 1.1167 |
| ema_total_gap_pct | WINNERS | 120 | 0.9501 | 0.6839 | 0.3523 | 1.1183 |
| ema_total_gap_pct | LOSERS | 109 | 0.8285 | 0.7445 | 0.4086 | 1.1132 |
| confirmation_body_ratio | ALL_SELECTED | 383 | 0.7199 | 0.7273 | 0.6250 | 0.8341 |
| confirmation_body_ratio | CONFIRMED | 383 | 0.7199 | 0.7273 | 0.6250 | 0.8341 |
| confirmation_body_ratio | FILLED | 229 | 0.7218 | 0.7391 | 0.6316 | 0.8421 |
| confirmation_body_ratio | WINNERS | 120 | 0.7153 | 0.7414 | 0.6129 | 0.8372 |
| confirmation_body_ratio | LOSERS | 109 | 0.7288 | 0.7333 | 0.6473 | 0.8469 |
| confirmation_adverse_wick_ratio | ALL_SELECTED | 383 | 0.1501 | 0.1374 | 0.0000 | 0.2375 |
| confirmation_adverse_wick_ratio | CONFIRMED | 383 | 0.1501 | 0.1374 | 0.0000 | 0.2375 |
| confirmation_adverse_wick_ratio | FILLED | 229 | 0.1455 | 0.1364 | 0.0000 | 0.2247 |
| confirmation_adverse_wick_ratio | WINNERS | 120 | 0.1528 | 0.1379 | 0.0272 | 0.2331 |
| confirmation_adverse_wick_ratio | LOSERS | 109 | 0.1376 | 0.1333 | 0.0000 | 0.2188 |
| confirmation_close_location | ALL_SELECTED | 383 | 0.8499 | 0.8626 | 0.7625 | 1.0000 |
| confirmation_close_location | CONFIRMED | 383 | 0.8499 | 0.8626 | 0.7625 | 1.0000 |
| confirmation_close_location | FILLED | 229 | 0.8545 | 0.8636 | 0.7753 | 1.0000 |
| confirmation_close_location | WINNERS | 120 | 0.8472 | 0.8621 | 0.7669 | 0.9728 |
| confirmation_close_location | LOSERS | 109 | 0.8624 | 0.8667 | 0.7812 | 1.0000 |
| trigger_distance_c5_bps | ALL_SELECTED | 383 | 26.9911 | 22.6653 | 15.0565 | 35.3263 |
| trigger_distance_c5_bps | CONFIRMED | 383 | 26.9911 | 22.6653 | 15.0565 | 35.3263 |
| trigger_distance_c5_bps | FILLED | 229 | 29.5588 | 24.9750 | 16.1468 | 38.5276 |
| trigger_distance_c5_bps | WINNERS | 120 | 29.5401 | 22.9819 | 15.5456 | 38.7632 |
| trigger_distance_c5_bps | LOSERS | 109 | 29.5793 | 26.3875 | 16.6886 | 37.8792 |

### Winner versus loser medians

| Indicator | Winner Observations | Loser Observations | Winner Median | Loser Median | Median Delta | Winner Mean | Loser Mean |
|---|---|---|---|---|---|---|---|
| directional_move_pct | 120 | 109 | 0.5399 | 0.6016 | -0.0617 | 0.5996 | 0.6700 |
| directional_five_min_body_pct | 120 | 109 | 0.5184 | 0.5923 | -0.0739 | 0.5863 | 0.6572 |
| oi_change_pct | 120 | 109 | 0.9016 | 0.8827 | 0.0190 | 3.1163 | 34.2809 |
| volume_ratio | 120 | 109 | 3.0188 | 3.0891 | -0.0703 | 3.4187 | 3.8657 |
| traded_value_cr | 120 | 109 | 17.9240 | 17.7519 | 0.1721 | 34.6111 | 27.1326 |
| five_min_range_pct | 120 | 109 | 0.7588 | 0.8167 | -0.0579 | 0.8580 | 0.9246 |
| five_min_body_ratio | 120 | 109 | 0.7208 | 0.7204 | 0.0004 | 0.6885 | 0.6990 |
| five_min_adverse_wick_ratio | 120 | 109 | 0.1584 | 0.1786 | -0.0201 | 0.1955 | 0.1803 |
| five_min_directional_close_location | 120 | 109 | 0.8416 | 0.8214 | 0.0201 | 0.8045 | 0.8197 |
| ema_fast_gap_pct | 120 | 109 | 0.3460 | 0.3461 | -0.0001 | 0.5147 | 0.4360 |
| ema_slow_gap_pct | 120 | 109 | 0.2827 | 0.3374 | -0.0547 | 0.4354 | 0.3925 |
| ema_total_gap_pct | 120 | 109 | 0.6839 | 0.7445 | -0.0606 | 0.9501 | 0.8285 |
| directional_close_ema9_pct | 120 | 109 | 1.0526 | 1.2126 | -0.1600 | 1.3113 | 1.3189 |
| confirmation_volume_ratio | 120 | 109 | 1.0655 | 0.8759 | 0.1896 | 1.2794 | 1.1633 |
| confirmation_body_ratio | 120 | 109 | 0.7414 | 0.7333 | 0.0081 | 0.7153 | 0.7288 |
| confirmation_adverse_wick_ratio | 120 | 109 | 0.1379 | 0.1333 | 0.0046 | 0.1528 | 0.1376 |
| confirmation_close_location | 120 | 109 | 0.8621 | 0.8667 | -0.0046 | 0.8472 | 0.8624 |
| trigger_distance_c5_bps | 120 | 109 | 22.9819 | 26.3875 | -3.4056 | 29.5401 | 29.5793 |
| confirmation_minute | 120 | 109 | 1.0000 | 1.0000 | 0.0000 | 1.3583 | 1.3486 |
| entry_minute | 120 | 109 | 2.0000 | 2.0000 | 0.0000 | 2.6083 | 2.5963 |

### Multiple-test-corrected comparisons

| Comparison | Indicator | Positive Observations | Negative Observations | Positive Median | Negative Median | Auc Positive Higher | P Value Two Sided | Bh Q Value |
|---|---|---|---|---|---|---|---|---|
| CONFIRMED_VS_NOT_CONFIRMED | five_min_range_pct | 383 | 634 | 0.7606 | 0.7019 | 0.5336 | 0.0721 | 0.4514 |
| CONFIRMED_VS_NOT_CONFIRMED | volume_ratio | 383 | 634 | 2.8397 | 2.5874 | 0.5291 | 0.1198 | 0.4514 |
| CONFIRMED_VS_NOT_CONFIRMED | directional_five_min_body_pct | 383 | 634 | 0.5230 | 0.4587 | 0.5258 | 0.1672 | 0.4514 |
| CONFIRMED_VS_NOT_CONFIRMED | directional_move_pct | 383 | 634 | 0.5371 | 0.4625 | 0.5255 | 0.1718 | 0.4514 |
| CONFIRMED_VS_NOT_CONFIRMED | directional_close_ema9_pct | 383 | 634 | 1.1039 | 0.9965 | 0.5254 | 0.1736 | 0.4514 |
| CONFIRMED_VS_NOT_CONFIRMED | traded_value_cr | 383 | 634 | 15.4433 | 14.3850 | 0.5198 | 0.2892 | 0.6266 |
| CONFIRMED_VS_NOT_CONFIRMED | oi_change_pct | 383 | 634 | 0.8333 | 0.8393 | 0.5161 | 0.3893 | 0.7230 |
| CONFIRMED_VS_NOT_CONFIRMED | five_min_adverse_wick_ratio | 383 | 634 | 0.1616 | 0.1498 | 0.5071 | 0.7020 | 0.8593 |
| CONFIRMED_VS_NOT_CONFIRMED | five_min_directional_close_location | 383 | 634 | 0.8384 | 0.8502 | 0.4929 | 0.7020 | 0.8593 |
| CONFIRMED_VS_NOT_CONFIRMED | five_min_body_ratio | 383 | 634 | 0.7134 | 0.7129 | 0.4929 | 0.7050 | 0.8593 |
| CONFIRMED_VS_NOT_CONFIRMED | ema_slow_gap_pct | 383 | 634 | 0.3033 | 0.3048 | 0.4957 | 0.8173 | 0.8593 |
| CONFIRMED_VS_NOT_CONFIRMED | ema_fast_gap_pct | 383 | 634 | 0.3351 | 0.3440 | 0.4961 | 0.8350 | 0.8593 |
| CONFIRMED_VS_NOT_CONFIRMED | ema_total_gap_pct | 383 | 634 | 0.6560 | 0.6426 | 0.4967 | 0.8593 | 0.8593 |
| FILLED_VS_NOT_FILLED | five_min_range_pct | 229 | 788 | 0.8060 | 0.6963 | 0.5761 | 0.0005 | 0.0068 |
| FILLED_VS_NOT_FILLED | directional_move_pct | 229 | 788 | 0.5611 | 0.4589 | 0.5668 | 0.0021 | 0.0144 |
| FILLED_VS_NOT_FILLED | traded_value_cr | 229 | 788 | 17.7519 | 14.1204 | 0.5646 | 0.0029 | 0.0144 |
| FILLED_VS_NOT_FILLED | directional_five_min_body_pct | 229 | 788 | 0.5523 | 0.4591 | 0.5596 | 0.0060 | 0.0226 |
| FILLED_VS_NOT_FILLED | volume_ratio | 229 | 788 | 3.0209 | 2.5945 | 0.5466 | 0.0314 | 0.0943 |
| FILLED_VS_NOT_FILLED | confirmation_volume_ratio | 229 | 154 | 0.9604 | 0.8417 | 0.5587 | 0.0514 | 0.1178 |
| FILLED_VS_NOT_FILLED | directional_close_ema9_pct | 229 | 788 | 1.1067 | 1.0080 | 0.5416 | 0.0550 | 0.1178 |
| FILLED_VS_NOT_FILLED | oi_change_pct | 229 | 788 | 0.8840 | 0.8308 | 0.5237 | 0.2752 | 0.5161 |
| FILLED_VS_NOT_FILLED | five_min_adverse_wick_ratio | 229 | 788 | 0.1667 | 0.1512 | 0.5140 | 0.5172 | 0.7132 |
| FILLED_VS_NOT_FILLED | five_min_directional_close_location | 229 | 788 | 0.8333 | 0.8488 | 0.4860 | 0.5172 | 0.7132 |
| FILLED_VS_NOT_FILLED | confirmation_body_ratio | 229 | 154 | 0.7391 | 0.7212 | 0.5181 | 0.5475 | 0.7132 |
| FILLED_VS_NOT_FILLED | ema_total_gap_pct | 229 | 788 | 0.7254 | 0.6299 | 0.5118 | 0.5856 | 0.7132 |
| FILLED_VS_NOT_FILLED | ema_fast_gap_pct | 229 | 788 | 0.3461 | 0.3376 | 0.5108 | 0.6181 | 0.7132 |
| FILLED_VS_NOT_FILLED | ema_slow_gap_pct | 229 | 788 | 0.3143 | 0.3034 | 0.5039 | 0.8571 | 0.9183 |
| FILLED_VS_NOT_FILLED | five_min_body_ratio | 229 | 788 | 0.7204 | 0.7120 | 0.5008 | 0.9712 | 0.9712 |
| WINNER_VS_LOSER | directional_move_pct | 120 | 109 | 0.5399 | 0.6016 | 0.4284 | 0.0614 | 0.7053 |
| WINNER_VS_LOSER | directional_five_min_body_pct | 120 | 109 | 0.5184 | 0.5923 | 0.4307 | 0.0705 | 0.7053 |
| WINNER_VS_LOSER | five_min_range_pct | 120 | 109 | 0.7588 | 0.8167 | 0.4429 | 0.1360 | 0.8491 |
| WINNER_VS_LOSER | confirmation_adverse_wick_ratio | 120 | 109 | 0.1379 | 0.1333 | 0.5403 | 0.2886 | 0.8491 |
| WINNER_VS_LOSER | confirmation_close_location | 120 | 109 | 0.8621 | 0.8667 | 0.4597 | 0.2886 | 0.8491 |
| WINNER_VS_LOSER | directional_close_ema9_pct | 120 | 109 | 1.0526 | 1.2126 | 0.4608 | 0.3060 | 0.8491 |
| WINNER_VS_LOSER | volume_ratio | 120 | 109 | 3.0188 | 3.0891 | 0.4654 | 0.3672 | 0.8491 |
| WINNER_VS_LOSER | ema_slow_gap_pct | 120 | 109 | 0.2827 | 0.3374 | 0.4697 | 0.4296 | 0.8491 |
| WINNER_VS_LOSER | trigger_distance_c5_bps | 120 | 109 | 22.9819 | 26.3875 | 0.4717 | 0.4605 | 0.8491 |
| WINNER_VS_LOSER | ema_total_gap_pct | 120 | 109 | 0.6839 | 0.7445 | 0.4771 | 0.5511 | 0.8491 |
| WINNER_VS_LOSER | confirmation_volume_ratio | 120 | 109 | 1.0655 | 0.8759 | 0.5229 | 0.5511 | 0.8491 |
| WINNER_VS_LOSER | five_min_adverse_wick_ratio | 120 | 109 | 0.1584 | 0.1786 | 0.5228 | 0.5522 | 0.8491 |
| WINNER_VS_LOSER | five_min_directional_close_location | 120 | 109 | 0.8416 | 0.8214 | 0.4772 | 0.5522 | 0.8491 |
| WINNER_VS_LOSER | ema_fast_gap_pct | 120 | 109 | 0.3460 | 0.3461 | 0.4823 | 0.6453 | 0.8491 |
| WINNER_VS_LOSER | five_min_body_ratio | 120 | 109 | 0.7208 | 0.7204 | 0.4835 | 0.6669 | 0.8491 |
| WINNER_VS_LOSER | oi_change_pct | 120 | 109 | 0.9016 | 0.8827 | 0.4841 | 0.6793 | 0.8491 |
| WINNER_VS_LOSER | confirmation_body_ratio | 120 | 109 | 0.7414 | 0.7333 | 0.4892 | 0.7782 | 0.8556 |
| WINNER_VS_LOSER | traded_value_cr | 120 | 109 | 17.9240 | 17.7519 | 0.5094 | 0.8067 | 0.8556 |
| WINNER_VS_LOSER | entry_minute | 120 | 109 | 2.0000 | 2.0000 | 0.4919 | 0.8128 | 0.8556 |
| WINNER_VS_LOSER | confirmation_minute | 120 | 109 | 1.0000 | 1.0000 | 0.4956 | 0.8842 | 0.8842 |

AUC around 0.5 indicates weak univariate separation. The pooled fill tests can also reflect differences in setup/time composition; fill probability is not accuracy. Quartiles and fixed bins are exploratory; selecting a favorable boundary from these tables and reporting it on the same history would be leakage.

### Correlation with realized net return

| Indicator | Observations | Spearman Vs Net Return | Pearson Vs Net Return |
|---|---|---|---|
| holding_minutes | 229 | +0.344 | +0.292 |
| confirmation_adverse_wick_ratio | 229 | +0.097 | +0.060 |
| confirmation_close_location | 229 | -0.097 | -0.060 |
| traded_value_cr | 229 | +0.083 | +0.170 |
| confirmation_body_ratio | 229 | -0.072 | -0.008 |
| confirmation_volume_ratio | 229 | +0.055 | +0.114 |
| directional_move_pct | 229 | -0.046 | -0.004 |
| directional_five_min_body_pct | 229 | -0.046 | +0.002 |
| ema_total_gap_pct | 229 | +0.045 | +0.216 |
| trigger_distance_c5_bps | 229 | -0.041 | +0.069 |
| ema_fast_gap_pct | 229 | +0.039 | +0.205 |
| initial_stop_risk_pct | 229 | -0.032 | +0.105 |
| ema_slow_gap_pct | 229 | +0.031 | +0.211 |
| oi_change_pct | 229 | +0.029 | -0.053 |
| entry_minute | 229 | -0.029 | -0.000 |
| confirmation_minute | 229 | -0.020 | -0.002 |
| directional_close_ema9_pct | 229 | +0.020 | +0.076 |
| five_min_adverse_wick_ratio | 229 | +0.015 | -0.017 |
| five_min_directional_close_location | 229 | -0.015 | +0.017 |
| five_min_body_ratio | 229 | -0.014 | +0.004 |
| five_min_range_pct | 229 | -0.003 | +0.026 |
| volume_ratio | 229 | -0.002 | -0.095 |

### Data-derived quartiles

| Indicator | Quartile | Observed Range | Selected | Confirmed | Fills | Fill Rate Pct | Wins | Losses | Win Rate Pct | Profit Factor | Net Return Points | Net Pnl Rs |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| directional_move_pct | Q1 | (0.199, 0.328] | 255 | 90 | 45 | 17.65% | 28 | 17 | 62.22% | 3.0088 | +26.0991 | +12,805.15 |
| directional_move_pct | Q2 | (0.328, 0.48] | 254 | 87 | 48 | 18.90% | 26 | 22 | 54.17% | 1.8325 | +13.0932 | +6,845.08 |
| directional_move_pct | Q3 | (0.48, 0.736] | 254 | 107 | 67 | 26.38% | 34 | 33 | 50.75% | 2.3751 | +33.3631 | +16,456.83 |
| directional_move_pct | Q4 | (0.736, 5.559] | 254 | 99 | 69 | 27.17% | 32 | 37 | 46.38% | 1.9528 | +23.8890 | +11,396.79 |
| directional_five_min_body_pct | Q1 | (0.0288, 0.325] | 255 | 92 | 47 | 18.43% | 27 | 20 | 57.45% | 2.4185 | +22.7473 | +11,348.20 |
| directional_five_min_body_pct | Q2 | (0.325, 0.474] | 254 | 82 | 47 | 18.50% | 30 | 17 | 63.83% | 3.1936 | +22.7865 | +11,282.04 |
| directional_five_min_body_pct | Q3 | (0.474, 0.728] | 254 | 112 | 69 | 27.17% | 34 | 35 | 49.28% | 2.1449 | +29.7231 | +14,819.70 |
| directional_five_min_body_pct | Q4 | (0.728, 5.886] | 254 | 97 | 66 | 25.98% | 29 | 37 | 43.94% | 1.8254 | +21.1874 | +10,053.91 |
| oi_change_pct | Q1 | (0.099, 0.345] | 255 | 89 | 57 | 22.35% | 34 | 23 | 59.65% | 2.7215 | +26.8669 | +13,111.17 |
| oi_change_pct | Q2 | (0.345, 0.838] | 254 | 103 | 54 | 21.26% | 24 | 30 | 44.44% | 1.3803 | +7.7990 | +3,978.68 |
| oi_change_pct | Q3 | (0.838, 1.896] | 254 | 93 | 51 | 20.08% | 23 | 28 | 45.10% | 1.4145 | +8.2519 | +4,207.46 |
| oi_change_pct | Q4 | (1.896, 3300.0] | 254 | 98 | 67 | 26.38% | 39 | 28 | 58.21% | 3.4295 | +53.5266 | +26,206.52 |
| volume_ratio | Q1 | (1.0070000000000001, 1.862] | 255 | 86 | 48 | 18.82% | 29 | 19 | 60.42% | 3.3398 | +32.9866 | +16,242.65 |
| volume_ratio | Q2 | (1.862, 2.653] | 254 | 96 | 55 | 21.65% | 26 | 29 | 47.27% | 1.4967 | +11.1042 | +5,961.24 |
| volume_ratio | Q3 | (2.653, 4.132] | 254 | 102 | 59 | 23.23% | 32 | 27 | 54.24% | 2.6429 | +30.6617 | +14,904.45 |
| volume_ratio | Q4 | (4.132, 50.207] | 254 | 99 | 67 | 26.38% | 33 | 34 | 49.25% | 1.9456 | +21.6918 | +10,395.49 |
| traded_value_cr | Q1 | (0.965, 6.956] | 255 | 87 | 42 | 16.47% | 25 | 17 | 59.52% | 3.1932 | +25.3484 | +12,686.08 |
| traded_value_cr | Q2 | (6.956, 14.87] | 254 | 98 | 59 | 23.23% | 29 | 30 | 49.15% | 1.3405 | +7.9070 | +3,998.93 |
| traded_value_cr | Q3 | (14.87, 31.339] | 254 | 104 | 62 | 24.41% | 29 | 33 | 46.77% | 1.9367 | +21.2767 | +10,670.01 |
| traded_value_cr | Q4 | (31.339, 465.646] | 254 | 94 | 66 | 25.98% | 37 | 29 | 56.06% | 3.0386 | +41.9122 | +20,148.83 |
| five_min_range_pct | Q1 | (0.257, 0.541] | 255 | 89 | 39 | 15.29% | 23 | 16 | 58.97% | 2.1863 | +16.1923 | +7,884.97 |
| five_min_range_pct | Q2 | (0.541, 0.722] | 254 | 85 | 52 | 20.47% | 32 | 20 | 61.54% | 3.1997 | +29.2610 | +14,714.77 |
| five_min_range_pct | Q3 | (0.722, 0.974] | 254 | 100 | 69 | 27.17% | 33 | 36 | 47.83% | 1.9038 | +23.4331 | +11,468.12 |
| five_min_range_pct | Q4 | (0.974, 6.681] | 254 | 109 | 69 | 27.17% | 32 | 37 | 46.38% | 2.0946 | +27.5580 | +13,435.98 |
| five_min_body_ratio | Q1 | (0.062200000000000005, 0.562] | 255 | 89 | 53 | 20.78% | 31 | 22 | 58.49% | 2.6547 | +26.0426 | +13,229.35 |
| five_min_body_ratio | Q2 | (0.562, 0.713] | 254 | 103 | 60 | 23.62% | 28 | 32 | 46.67% | 1.6820 | +15.0897 | +7,415.12 |
| five_min_body_ratio | Q3 | (0.713, 0.84] | 254 | 99 | 60 | 23.62% | 30 | 30 | 50.00% | 2.1175 | +24.6480 | +11,926.17 |
| five_min_body_ratio | Q4 | (0.84, 1.0] | 254 | 92 | 56 | 22.05% | 31 | 25 | 55.36% | 2.6910 | +30.6641 | +14,933.20 |
| five_min_adverse_wick_ratio | Q1 | (-0.001, 0.0676] | 255 | 95 | 54 | 21.18% | 28 | 26 | 51.85% | 2.4730 | +25.8745 | +12,855.85 |
| five_min_adverse_wick_ratio | Q2 | (0.0676, 0.154] | 254 | 89 | 54 | 21.26% | 30 | 24 | 55.56% | 2.5938 | +30.7595 | +15,211.86 |
| five_min_adverse_wick_ratio | Q3 | (0.154, 0.262] | 254 | 100 | 60 | 23.62% | 23 | 37 | 38.33% | 1.2315 | +6.1260 | +2,836.15 |
| five_min_adverse_wick_ratio | Q4 | (0.262, 0.777] | 254 | 99 | 61 | 24.02% | 39 | 22 | 63.93% | 3.2882 | +33.6843 | +16,599.98 |
| five_min_directional_close_location | Q1 | (0.222, 0.738] | 255 | 100 | 61 | 23.92% | 39 | 22 | 63.93% | 3.2882 | +33.6843 | +16,599.98 |
| five_min_directional_close_location | Q2 | (0.738, 0.846] | 254 | 99 | 60 | 23.62% | 23 | 37 | 38.33% | 1.2315 | +6.1260 | +2,836.15 |
| five_min_directional_close_location | Q3 | (0.846, 0.932] | 254 | 90 | 54 | 21.26% | 30 | 24 | 55.56% | 2.5938 | +30.7595 | +15,211.86 |
| five_min_directional_close_location | Q4 | (0.932, 1.0] | 254 | 94 | 54 | 21.26% | 28 | 26 | 51.85% | 2.4730 | +25.8745 | +12,855.85 |
| ema_fast_gap_pct | Q1 | (0.00253, 0.198] | 255 | 98 | 55 | 21.57% | 34 | 21 | 61.82% | 3.0240 | +30.5388 | +14,784.60 |
| ema_fast_gap_pct | Q2 | (0.198, 0.339] | 254 | 96 | 57 | 22.44% | 25 | 32 | 43.86% | 1.4192 | +9.2589 | +4,743.07 |
| ema_fast_gap_pct | Q3 | (0.339, 0.585] | 254 | 93 | 54 | 21.26% | 27 | 27 | 50.00% | 1.5962 | +10.8302 | +5,195.01 |
| ema_fast_gap_pct | Q4 | (0.585, 4.401] | 254 | 96 | 63 | 24.80% | 34 | 29 | 53.97% | 3.0170 | +45.8165 | +22,781.16 |
| ema_slow_gap_pct | Q1 | (-0.00063, 0.157] | 255 | 99 | 62 | 24.31% | 36 | 26 | 58.06% | 2.8876 | +30.0193 | +14,859.50 |
| ema_slow_gap_pct | Q2 | (0.157, 0.304] | 254 | 93 | 50 | 19.69% | 27 | 23 | 54.00% | 1.5706 | +10.2037 | +5,229.58 |
| ema_slow_gap_pct | Q3 | (0.304, 0.526] | 254 | 99 | 61 | 24.02% | 30 | 31 | 49.18% | 2.0879 | +22.2670 | +10,741.82 |
| ema_slow_gap_pct | Q4 | (0.526, 3.745] | 254 | 92 | 56 | 22.05% | 27 | 29 | 48.21% | 2.4266 | +33.9543 | +16,672.94 |
| ema_total_gap_pct | Q1 | (0.0369, 0.387] | 255 | 97 | 58 | 22.75% | 34 | 24 | 58.62% | 2.7941 | +29.0675 | +14,418.69 |
| ema_total_gap_pct | Q2 | (0.387, 0.646] | 254 | 94 | 49 | 19.29% | 25 | 24 | 51.02% | 1.6399 | +10.3797 | +5,284.99 |
| ema_total_gap_pct | Q3 | (0.646, 1.095] | 254 | 100 | 62 | 24.41% | 29 | 33 | 46.77% | 1.5912 | +13.4014 | +6,487.38 |
| ema_total_gap_pct | Q4 | (1.095, 8.146] | 254 | 92 | 60 | 23.62% | 32 | 28 | 53.33% | 2.8986 | +43.5957 | +21,312.78 |
| directional_close_ema9_pct | Q1 | (-1.0639999999999998, 0.652] | 255 | 94 | 47 | 18.43% | 28 | 19 | 59.57% | 2.5682 | +23.8091 | +11,593.56 |
| directional_close_ema9_pct | Q2 | (0.652, 1.033] | 254 | 80 | 55 | 21.65% | 30 | 25 | 54.55% | 2.1561 | +18.8796 | +9,191.51 |
| directional_close_ema9_pct | Q3 | (1.033, 1.564] | 254 | 102 | 60 | 23.62% | 27 | 33 | 45.00% | 1.5891 | +13.6942 | +6,805.03 |
| directional_close_ema9_pct | Q4 | (1.564, 8.619] | 254 | 107 | 67 | 26.38% | 35 | 32 | 52.24% | 2.7198 | +40.0614 | +19,913.74 |
| confirmation_volume_ratio | Q1 | (0.047599999999999996, 0.576] | 96 | 96 | 49 | 51.04% | 28 | 21 | 57.14% | 2.1124 | +17.7323 | +8,760.50 |
| confirmation_volume_ratio | Q2 | (0.576, 0.924] | 96 | 96 | 59 | 61.46% | 23 | 36 | 38.98% | 1.4117 | +10.3185 | +5,049.86 |
| confirmation_volume_ratio | Q3 | (0.924, 1.476] | 95 | 95 | 57 | 60.00% | 30 | 27 | 52.63% | 2.6802 | +29.2248 | +14,193.92 |
| confirmation_volume_ratio | Q4 | (1.476, 5.851] | 96 | 96 | 64 | 66.67% | 39 | 25 | 60.94% | 2.9928 | +39.1688 | +19,499.57 |
| confirmation_body_ratio | Q1 | (0.0931, 0.625] | 96 | 96 | 54 | 56.25% | 33 | 21 | 61.11% | 3.3866 | +31.8259 | +15,271.94 |
| confirmation_body_ratio | Q2 | (0.625, 0.727] | 96 | 96 | 58 | 60.42% | 25 | 33 | 43.10% | 1.4985 | +12.6604 | +6,310.36 |
| confirmation_body_ratio | Q3 | (0.727, 0.834] | 95 | 95 | 57 | 60.00% | 30 | 27 | 52.63% | 2.7063 | +27.3752 | +13,654.63 |
| confirmation_body_ratio | Q4 | (0.834, 1.0] | 96 | 96 | 60 | 62.50% | 32 | 28 | 53.33% | 2.0562 | +24.5829 | +12,266.91 |
| confirmation_adverse_wick_ratio | Q1 | (-0.001, 0.137] | 192 | 192 | 116 | 60.42% | 59 | 57 | 50.86% | 2.0896 | +44.7752 | +22,106.53 |
| confirmation_adverse_wick_ratio | Q2 | (0.137, 0.237] | 95 | 95 | 61 | 64.21% | 31 | 30 | 50.82% | 1.9699 | +20.9364 | +10,671.27 |
| confirmation_adverse_wick_ratio | Q3 | (0.237, 0.5] | 96 | 96 | 52 | 54.17% | 30 | 22 | 57.69% | 2.9988 | +30.7328 | +14,726.03 |
| confirmation_close_location | Q1 | (0.499, 0.763] | 96 | 96 | 52 | 54.17% | 30 | 22 | 57.69% | 2.9988 | +30.7328 | +14,726.03 |
| confirmation_close_location | Q2 | (0.763, 0.863] | 96 | 96 | 62 | 64.58% | 31 | 31 | 50.00% | 1.9221 | +20.4000 | +10,403.34 |
| confirmation_close_location | Q3 | (0.863, 1.0] | 191 | 191 | 115 | 60.21% | 59 | 56 | 51.30% | 2.1173 | +45.3116 | +22,374.46 |
| trigger_distance_c5_bps | Q1 | (1.778, 15.056] | 96 | 96 | 51 | 53.12% | 30 | 21 | 58.82% | 2.9766 | +25.7200 | +12,450.07 |
| trigger_distance_c5_bps | Q2 | (15.056, 22.665] | 96 | 96 | 50 | 52.08% | 30 | 20 | 60.00% | 2.7953 | +26.5837 | +13,223.26 |
| trigger_distance_c5_bps | Q3 | (22.665, 35.326] | 95 | 95 | 61 | 64.21% | 23 | 38 | 37.70% | 1.1486 | +3.8278 | +1,920.89 |
| trigger_distance_c5_bps | Q4 | (35.326, 121.625] | 96 | 96 | 67 | 69.79% | 37 | 30 | 55.22% | 2.6472 | +40.3128 | +19,909.62 |
| entry_minute | Q1 | (1.999, 3.0] | 197 | 197 | 197 | 100.00% | 102 | 95 | 51.78% | 2.1556 | +78.4326 | +38,805.20 |
| entry_minute | Q2 | (3.0, 5.0] | 32 | 32 | 32 | 100.00% | 18 | 14 | 56.25% | 2.7693 | +18.0118 | +8,698.63 |

### Fixed bins — `directional_move_pct`

| Bin | Selected | Confirmed | Fills | Confirmation Rate Pct | Fill Rate Pct | Wins | Losses | Win Rate Pct | Profit Factor | Net Return Points | Net Pnl Rs |
|---|---|---|---|---|---|---|---|---|---|---|---|
| <0.30 | 194 | 71 | 33 | 36.60% | 17.01% | 20 | 13 | 60.61% | 3.0588 | +20.0486 | +9,973.79 |
| 0.30–0.50 | 342 | 112 | 65 | 32.75% | 19.01% | 36 | 29 | 55.38% | 1.9545 | +19.8917 | +10,068.71 |
| 0.50–0.75 | 237 | 104 | 63 | 43.88% | 26.58% | 33 | 30 | 52.38% | 2.4936 | +33.4638 | +16,477.72 |
| 0.75–1.00 | 140 | 58 | 40 | 41.43% | 28.57% | 19 | 21 | 47.50% | 1.9178 | +13.2919 | +6,386.28 |
| 1.00–1.50 | 76 | 30 | 22 | 39.47% | 28.95% | 9 | 13 | 40.91% | 1.4763 | +3.9653 | +1,708.93 |
| 1.50+ | 28 | 8 | 6 | 28.57% | 21.43% | 3 | 3 | 50.00% | 3.5545 | +5.7831 | +2,888.40 |

### Fixed bins — `oi_change_pct`

| Bin | Selected | Confirmed | Fills | Confirmation Rate Pct | Fill Rate Pct | Wins | Losses | Win Rate Pct | Profit Factor | Net Return Points | Net Pnl Rs |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 0.10–0.50 | 346 | 126 | 78 | 36.42% | 22.54% | 47 | 31 | 60.26% | 2.4385 | +32.7151 | +16,114.45 |
| 0.50–1.00 | 218 | 86 | 45 | 39.45% | 20.64% | 15 | 30 | 33.33% | 1.0388 | +0.7188 | +503.79 |
| 1.00–2.00 | 213 | 78 | 43 | 36.62% | 20.19% | 22 | 21 | 51.16% | 1.9995 | +15.4200 | +7,403.13 |
| 2.00–5.00 | 155 | 58 | 41 | 37.42% | 26.45% | 21 | 20 | 51.22% | 2.3674 | +21.3318 | +10,906.10 |
| 5.00+ | 85 | 35 | 22 | 41.18% | 25.88% | 15 | 7 | 68.18% | 5.5427 | +26.2586 | +12,576.37 |

### Fixed bins — `volume_ratio`

| Bin | Selected | Confirmed | Fills | Confirmation Rate Pct | Fill Rate Pct | Wins | Losses | Win Rate Pct | Profit Factor | Net Return Points | Net Pnl Rs |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 1.00–1.50 | 97 | 30 | 18 | 30.93% | 18.56% | 8 | 10 | 44.44% | 1.7594 | +5.7635 | +2,854.56 |
| 1.50–2.00 | 218 | 78 | 43 | 35.78% | 19.72% | 29 | 14 | 67.44% | 4.2059 | +34.5905 | +17,158.44 |
| 2.00–3.00 | 251 | 88 | 51 | 35.06% | 20.32% | 22 | 29 | 43.14% | 1.3315 | +7.5727 | +3,831.20 |
| 3.00–5.00 | 297 | 128 | 75 | 43.10% | 25.25% | 42 | 33 | 56.00% | 2.7236 | +37.5257 | +18,296.01 |
| 5.00+ | 154 | 59 | 42 | 38.31% | 27.27% | 19 | 23 | 45.24% | 1.7297 | +10.9920 | +5,363.63 |

### Fixed bins — `traded_value_cr`

| Bin | Selected | Confirmed | Fills | Confirmation Rate Pct | Fill Rate Pct | Wins | Losses | Win Rate Pct | Profit Factor | Net Return Points | Net Pnl Rs |
|---|---|---|---|---|---|---|---|---|---|---|---|
| <2.5cr | 26 | 3 | 0 | 11.54% | 0.00% | 0 | 0 | — | — | +0.0000 | +0.00 |
| 2.5–5cr | 143 | 50 | 20 | 34.97% | 13.99% | 11 | 9 | 55.00% | 2.8580 | +10.4652 | +5,283.61 |
| 5–10cr | 196 | 76 | 49 | 38.78% | 25.00% | 25 | 24 | 51.02% | 1.6656 | +12.3132 | +6,292.76 |
| 10–25cr | 333 | 133 | 74 | 39.94% | 22.22% | 39 | 35 | 52.70% | 2.1144 | +28.5557 | +14,003.13 |
| 25–50cr | 181 | 64 | 45 | 35.36% | 24.86% | 19 | 26 | 42.22% | 1.5853 | +10.3616 | +5,243.54 |
| 50cr+ | 138 | 57 | 41 | 41.30% | 29.71% | 26 | 15 | 63.41% | 4.2802 | +34.7486 | +16,680.80 |

### Fixed bins — `five_min_range_pct`

| Bin | Selected | Confirmed | Fills | Confirmation Rate Pct | Fill Rate Pct | Wins | Losses | Win Rate Pct | Profit Factor | Net Return Points | Net Pnl Rs |
|---|---|---|---|---|---|---|---|---|---|---|---|
| <0.30 | 9 | 1 | 1 | 11.11% | 11.11% | 0 | 1 | 0.00% | 0.0000 | -1.1584 | -577.93 |
| 0.30–0.50 | 182 | 66 | 29 | 36.26% | 15.93% | 16 | 13 | 55.17% | 2.0913 | +11.7722 | +5,699.08 |
| 0.50–0.75 | 348 | 119 | 68 | 34.20% | 19.54% | 43 | 25 | 63.24% | 3.0586 | +35.9749 | +18,156.42 |
| 0.75–1.00 | 241 | 96 | 65 | 39.83% | 26.97% | 31 | 34 | 47.69% | 1.9804 | +23.0402 | +11,142.71 |
| 1.00–1.50 | 170 | 76 | 49 | 44.71% | 28.82% | 24 | 25 | 48.98% | 2.3683 | +23.0369 | +11,185.65 |
| 1.50+ | 67 | 25 | 17 | 37.31% | 25.37% | 6 | 11 | 35.29% | 1.4554 | +3.7786 | +1,897.90 |

### Fixed bins — `five_min_body_ratio`

| Bin | Selected | Confirmed | Fills | Confirmation Rate Pct | Fill Rate Pct | Wins | Losses | Win Rate Pct | Profit Factor | Net Return Points | Net Pnl Rs |
|---|---|---|---|---|---|---|---|---|---|---|---|
| <0.20 | 6 | 1 | 1 | 16.67% | 16.67% | 0 | 1 | 0.00% | 0.0000 | -1.1524 | -555.97 |
| 0.20–0.40 | 63 | 28 | 20 | 44.44% | 31.75% | 10 | 10 | 50.00% | 2.1308 | +7.7370 | +3,826.99 |
| 0.40–0.60 | 230 | 78 | 45 | 33.91% | 19.57% | 28 | 17 | 62.22% | 3.0087 | +25.8861 | +12,876.85 |
| 0.60–0.80 | 391 | 155 | 89 | 39.64% | 22.76% | 43 | 46 | 48.31% | 1.9183 | +29.2074 | +14,415.04 |
| 0.80+ | 327 | 121 | 74 | 37.00% | 22.63% | 39 | 35 | 52.70% | 2.3705 | +34.7663 | +16,940.93 |

### Fixed bins — `five_min_adverse_wick_ratio`

| Bin | Selected | Confirmed | Fills | Confirmation Rate Pct | Fill Rate Pct | Wins | Losses | Win Rate Pct | Profit Factor | Net Return Points | Net Pnl Rs |
|---|---|---|---|---|---|---|---|---|---|---|---|
| <0.10 | 359 | 140 | 80 | 39.00% | 22.28% | 41 | 39 | 51.25% | 2.4214 | +39.7749 | +19,418.37 |
| 0.10–0.20 | 258 | 83 | 54 | 32.17% | 20.93% | 29 | 25 | 53.70% | 2.6084 | +27.1617 | +13,469.35 |
| 0.20–0.30 | 189 | 82 | 45 | 43.39% | 23.81% | 19 | 26 | 42.22% | 1.2361 | +4.7048 | +2,380.34 |
| 0.30–0.40 | 116 | 43 | 26 | 37.07% | 22.41% | 15 | 11 | 57.69% | 2.2233 | +8.9765 | +4,421.19 |
| 0.40–0.50 | 62 | 20 | 14 | 32.26% | 22.58% | 11 | 3 | 78.57% | 6.7157 | +13.4562 | +6,587.85 |
| 0.50+ | 33 | 15 | 10 | 45.45% | 30.30% | 5 | 5 | 50.00% | 1.6649 | +2.3702 | +1,226.73 |

### Fixed bins — `five_min_directional_close_location`

| Bin | Selected | Confirmed | Fills | Confirmation Rate Pct | Fill Rate Pct | Wins | Losses | Win Rate Pct | Profit Factor | Net Return Points | Net Pnl Rs |
|---|---|---|---|---|---|---|---|---|---|---|---|
| <0.50 | 31 | 14 | 10 | 45.16% | 32.26% | 5 | 5 | 50.00% | 1.6649 | +2.3702 | +1,226.73 |
| 0.50–0.60 | 62 | 21 | 14 | 33.87% | 22.58% | 11 | 3 | 78.57% | 6.7157 | +13.4562 | +6,587.85 |
| 0.60–0.75 | 181 | 72 | 39 | 39.78% | 21.55% | 23 | 16 | 58.97% | 2.6644 | +16.6538 | +8,228.12 |
| 0.75–0.90 | 382 | 135 | 86 | 35.34% | 22.51% | 40 | 46 | 46.51% | 1.7084 | +24.1892 | +12,042.76 |
| 0.90+ | 361 | 141 | 80 | 39.06% | 22.16% | 41 | 39 | 51.25% | 2.4214 | +39.7749 | +19,418.37 |

### Fixed bins — `ema_total_gap_pct`

| Bin | Selected | Confirmed | Fills | Confirmation Rate Pct | Fill Rate Pct | Wins | Losses | Win Rate Pct | Profit Factor | Net Return Points | Net Pnl Rs |
|---|---|---|---|---|---|---|---|---|---|---|---|
| <0.10 | 16 | 7 | 6 | 43.75% | 37.50% | 5 | 1 | 83.33% | 18.6960 | +7.0035 | +3,511.18 |
| 0.10–0.25 | 105 | 43 | 23 | 40.95% | 21.90% | 15 | 8 | 65.22% | 3.0873 | +12.6428 | +6,121.76 |
| 0.25–0.50 | 249 | 94 | 57 | 37.75% | 22.89% | 27 | 30 | 47.37% | 1.6890 | +13.4135 | +6,728.10 |
| 0.50–1.00 | 352 | 129 | 72 | 36.65% | 20.45% | 33 | 39 | 45.83% | 1.2841 | +7.6475 | +4,040.12 |
| 1.00+ | 295 | 110 | 71 | 37.29% | 24.07% | 40 | 31 | 56.34% | 3.2103 | +55.7371 | +27,102.68 |

### Fixed bins — `confirmation_volume_ratio`

| Bin | Selected | Confirmed | Fills | Confirmation Rate Pct | Fill Rate Pct | Wins | Losses | Win Rate Pct | Profit Factor | Net Return Points | Net Pnl Rs |
|---|---|---|---|---|---|---|---|---|---|---|---|
| <0.50 | 70 | 70 | 38 | 100.00% | 54.29% | 22 | 16 | 57.89% | 2.2282 | +14.5732 | +7,136.53 |
| 0.50–0.75 | 78 | 78 | 40 | 100.00% | 51.28% | 18 | 22 | 45.00% | 1.4419 | +7.1837 | +3,603.49 |
| 0.75–1.00 | 64 | 64 | 42 | 100.00% | 65.62% | 17 | 25 | 40.48% | 1.6537 | +10.7991 | +5,235.49 |
| 1.00–1.50 | 78 | 78 | 48 | 100.00% | 61.54% | 27 | 21 | 56.25% | 3.2015 | +30.2880 | +14,758.56 |
| 1.50–2.00 | 50 | 50 | 30 | 100.00% | 60.00% | 17 | 13 | 56.67% | 1.7835 | +8.6895 | +4,385.06 |
| 2.00+ | 43 | 43 | 31 | 100.00% | 72.09% | 19 | 12 | 61.29% | 3.9089 | +24.9109 | +12,384.71 |

### Fixed bins — `confirmation_body_ratio`

| Bin | Selected | Confirmed | Fills | Confirmation Rate Pct | Fill Rate Pct | Wins | Losses | Win Rate Pct | Profit Factor | Net Return Points | Net Pnl Rs |
|---|---|---|---|---|---|---|---|---|---|---|---|
| <0.40 | 15 | 15 | 11 | 100.00% | 73.33% | 7 | 4 | 63.64% | 2.6837 | +3.7271 | +1,733.76 |
| 0.40–0.50 | 21 | 21 | 12 | 100.00% | 57.14% | 4 | 8 | 33.33% | 1.1416 | +0.7128 | +411.48 |
| 0.50–0.60 | 41 | 41 | 20 | 100.00% | 48.78% | 15 | 5 | 75.00% | 6.9537 | +21.2094 | +10,074.60 |
| 0.60–0.75 | 124 | 124 | 74 | 100.00% | 59.68% | 35 | 39 | 47.30% | 1.8205 | +23.5807 | +11,703.98 |
| 0.75+ | 182 | 182 | 112 | 100.00% | 61.54% | 59 | 53 | 52.68% | 2.2261 | +47.2143 | +23,580.01 |

### Fixed bins — `confirmation_adverse_wick_ratio`

| Bin | Selected | Confirmed | Fills | Confirmation Rate Pct | Fill Rate Pct | Wins | Losses | Win Rate Pct | Profit Factor | Net Return Points | Net Pnl Rs |
|---|---|---|---|---|---|---|---|---|---|---|---|
| <0.10 | 154 | 154 | 92 | 100.00% | 59.74% | 45 | 47 | 48.91% | 1.9755 | +34.1160 | +16,839.69 |
| 0.10–0.20 | 89 | 89 | 56 | 100.00% | 62.92% | 28 | 28 | 50.00% | 1.8646 | +16.3386 | +7,946.34 |
| 0.20–0.30 | 78 | 78 | 49 | 100.00% | 62.82% | 30 | 19 | 61.22% | 3.2032 | +32.3612 | +16,306.24 |
| 0.30–0.40 | 47 | 47 | 24 | 100.00% | 51.06% | 12 | 12 | 50.00% | 1.9879 | +7.6424 | +3,515.80 |
| 0.40–0.50 | 14 | 14 | 8 | 100.00% | 57.14% | 5 | 3 | 62.50% | 4.4012 | +5.9862 | +2,895.77 |
| 0.50+ | 1 | 1 | 0 | 100.00% | 0.00% | 0 | 0 | — | — | +0.0000 | +0.00 |

### Fixed bins — `confirmation_close_location`

| Bin | Selected | Confirmed | Fills | Confirmation Rate Pct | Fill Rate Pct | Wins | Losses | Win Rate Pct | Profit Factor | Net Return Points | Net Pnl Rs |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 0.50–0.60 | 15 | 15 | 8 | 100.00% | 53.33% | 5 | 3 | 62.50% | 4.4012 | +5.9862 | +2,895.77 |
| 0.60–0.75 | 75 | 75 | 41 | 100.00% | 54.67% | 24 | 17 | 58.54% | 3.0953 | +25.7957 | +12,353.45 |
| 0.75–0.90 | 138 | 138 | 87 | 100.00% | 63.04% | 45 | 42 | 51.72% | 2.0404 | +30.1822 | +15,246.63 |
| 0.90+ | 155 | 155 | 93 | 100.00% | 60.00% | 46 | 47 | 49.46% | 1.9859 | +34.4802 | +17,007.98 |

### Fixed bins — `trigger_distance_c5_bps`

| Bin | Selected | Confirmed | Fills | Confirmation Rate Pct | Fill Rate Pct | Wins | Losses | Win Rate Pct | Profit Factor | Net Return Points | Net Pnl Rs |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 0–10 | 49 | 49 | 27 | 100.00% | 55.10% | 16 | 11 | 59.26% | 3.0583 | +13.4956 | +6,636.32 |
| 10–20 | 111 | 111 | 54 | 100.00% | 48.65% | 30 | 24 | 55.56% | 2.0422 | +18.2233 | +8,814.37 |
| 20–30 | 101 | 101 | 62 | 100.00% | 61.39% | 30 | 32 | 48.39% | 2.1447 | +24.6973 | +12,222.81 |
| 30–50 | 85 | 85 | 58 | 100.00% | 68.24% | 28 | 30 | 48.28% | 1.9729 | +21.6978 | +10,719.42 |
| 50–100 | 34 | 34 | 25 | 100.00% | 73.53% | 15 | 10 | 60.00% | 2.9566 | +16.2934 | +8,332.73 |
| 100+ | 3 | 3 | 3 | 100.00% | 100.00% | 1 | 2 | 33.33% | 2.1274 | +2.0370 | +778.20 |

### Fixed bins — `entry_delay_minutes`

| Bin | Selected | Confirmed | Fills | Confirmation Rate Pct | Fill Rate Pct | Wins | Losses | Win Rate Pct | Profit Factor | Net Return Points | Net Pnl Rs |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 1–2 | 186 | 186 | 186 | 100.00% | 100.00% | 97 | 89 | 52.15% | 2.2196 | +77.8999 | +38,476.03 |
| 2–3 | 33 | 33 | 33 | 100.00% | 100.00% | 18 | 15 | 54.55% | 2.5023 | +15.1951 | +7,429.58 |
| 3–4 | 6 | 6 | 6 | 100.00% | 100.00% | 3 | 3 | 50.00% | 1.3785 | +0.8946 | +403.90 |
| 4+ | 4 | 4 | 4 | 100.00% | 100.00% | 2 | 2 | 50.00% | 2.4410 | +2.4547 | +1,194.32 |

## 9. One-minute confirmation and entry timing

### Confirmation minute

| Confirmation Minute | Fills | Wins | Losses | Flat Trades | Win Rate Pct | Profit Factor | Net Return Points | Net Pnl Rs | Gross Profit Points | Gross Loss Points | Gross Profit Rs | Gross Loss Rs | Average Return Points | Median Return Points | Average Pnl Rs | Median Pnl Rs | Average Win Points | Average Loss Points | Payoff Ratio | Sessions With Fills | Gross Pnl Rs | Estimated Cost Rs | Position Notional Rs |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 | 166 | 88 | 78 | 0 | 53.01% | 2.3116 | +75.7829 | +37,387.51 | +133.5631 | +57.7803 | +65,246.89 | +27,859.38 | +0.4565 | +0.2731 | +225.23 | +135.97 | +1.5178 | -0.7408 | 2.0489 | 56 | +49,475.07 | +12,087.56 | +8,058,376.33 |
| 2 | 45 | 21 | 24 | 0 | 46.67% | 1.5021 | +7.6223 | +3,827.87 | +22.8039 | +15.1815 | +11,086.84 | +7,258.97 | +0.1694 | -0.3688 | +85.06 | -183.40 | +1.0859 | -0.6326 | 1.7167 | 32 | +7,101.67 | +3,273.80 | +2,182,536.17 |
| 3 | 18 | 11 | 7 | 0 | 61.11% | 3.5606 | +13.0392 | +6,288.46 | +18.1315 | +5.0923 | +8,682.17 | +2,393.71 | +0.7244 | +0.4797 | +349.36 | +235.71 | +1.6483 | -0.7275 | 2.2658 | 13 | +7,581.64 | +1,293.18 | +862,116.72 |

### Entry minute

| Entry Minute | Fills | Wins | Losses | Flat Trades | Win Rate Pct | Profit Factor | Net Return Points | Net Pnl Rs | Gross Profit Points | Gross Loss Points | Gross Profit Rs | Gross Loss Rs | Average Return Points | Median Return Points | Average Pnl Rs | Median Pnl Rs | Average Win Points | Average Loss Points | Payoff Ratio | Sessions With Fills | Gross Pnl Rs | Estimated Cost Rs | Position Notional Rs |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 2 | 132 | 71 | 61 | 0 | 53.79% | 2.3964 | +63.2349 | +31,421.13 | +108.5198 | +45.2850 | +53,128.37 | +21,707.24 | +0.4791 | +0.3497 | +238.04 | +172.46 | +1.5284 | -0.7424 | 2.0589 | 52 | +41,011.72 | +9,590.59 | +6,393,729.81 |
| 3 | 65 | 31 | 34 | 0 | 47.69% | 1.6728 | +15.1977 | +7,384.07 | +37.7867 | +22.5890 | +18,284.44 | +10,900.37 | +0.2338 | -0.5338 | +113.60 | -252.78 | +1.2189 | -0.6644 | 1.8347 | 39 | +12,124.38 | +4,740.31 | +3,160,204.83 |
| 4 | 23 | 12 | 11 | 0 | 52.17% | 2.4318 | +11.1839 | +5,254.84 | +18.9950 | +7.8111 | +9,101.85 | +3,847.02 | +0.4863 | +0.2306 | +228.47 | +112.70 | +1.5829 | -0.7101 | 2.2291 | 17 | +6,938.67 | +1,683.84 | +1,122,557.93 |
| 5 | 9 | 6 | 3 | 0 | 66.67% | 3.8822 | +6.8279 | +3,443.80 | +9.1969 | +2.3690 | +4,501.23 | +1,057.44 | +0.7587 | +0.5477 | +382.64 | +268.47 | +1.5328 | -0.7897 | 1.9411 | 8 | +4,083.60 | +639.80 | +426,536.65 |

### Setup by confirmation minute

| Setup Confirmation | Fills | Wins | Losses | Win Rate Pct | Profit Factor | Net Return Points | Net Pnl Rs |
|---|---|---|---|---|---|---|---|
| 09:25_LONG / M1 | 46 | 24 | 22 | 52.17% | 1.6642 | +8.1073 | +3,867.43 |
| 09:25_LONG / M2 | 14 | 8 | 6 | 57.14% | 2.0426 | +3.4612 | +1,700.55 |
| 09:25_LONG / M3 | 1 | 0 | 1 | 0.00% | 0.0000 | -0.5590 | -278.20 |
| 09:25_SHORT / M1 | 27 | 11 | 16 | 40.74% | 2.1389 | +11.1792 | +5,450.21 |
| 09:25_SHORT / M2 | 24 | 10 | 14 | 41.67% | 1.4200 | +3.8382 | +1,821.94 |
| 09:25_SHORT / M3 | 11 | 7 | 4 | 63.64% | 4.2974 | +7.3491 | +3,725.63 |
| 09:30_LONG / M1 | 11 | 5 | 6 | 45.45% | 1.6174 | +3.5120 | +1,709.49 |
| 09:30_SHORT / M1 | 6 | 5 | 1 | 83.33% | 8.0926 | +8.1736 | +4,010.61 |
| 09:30_SHORT / M2 | 7 | 3 | 4 | 42.86% | 1.1185 | +0.3229 | +305.38 |
| 09:30_SHORT / M3 | 6 | 4 | 2 | 66.67% | 3.7116 | +6.2491 | +2,841.03 |
| 09:35_LONG / M1 | 17 | 9 | 8 | 52.94% | 1.5052 | +3.7417 | +1,914.06 |
| 09:35_SHORT / M1 | 10 | 6 | 4 | 60.00% | 3.1215 | +9.7864 | +4,914.43 |
| 09:40_LONG / M1 | 18 | 9 | 9 | 50.00% | 3.1108 | +12.3896 | +5,942.03 |
| 09:40_SHORT / M1 | 15 | 8 | 7 | 53.33% | 1.8434 | +6.1070 | +3,311.43 |
| 09:45_LONG / M1 | 9 | 6 | 3 | 66.67% | 4.7245 | +8.7639 | +4,366.10 |
| 09:45_SHORT / M1 | 7 | 5 | 2 | 71.43% | 3.8060 | +4.0221 | +1,901.73 |

### One-minute rejection codes

| Reason | Occurrences |
|---|---|
| CLOSE_NOT_BEYOND_FIVE_MINUTE_CLOSE | 860 |
| WRONG_CANDLE_DIRECTION | 781 |
| BODY_RATIO_BELOW_MINIMUM | 697 |
| ADVERSE_WICK_RATIO_ABOVE_MAXIMUM | 279 |
| CLOSE_LOCATION_BELOW_MINIMUM | 87 |
| PRECONF_MIDPOINT_INVALIDATED | 32 |
| NONPOSITIVE_RANGE | 1 |

Counts are failed-check occurrences across monitored candles; codes can overlap and one candidate can contribute more than once. Confirmation and entry minute are causal features, but any new timing rule must be replayed inside each setup. A global minute ban can remove profitable legs along with weak ones.

## 10. Gap guard, portfolio and exposure

### Gap paths

| Gap Guard Path | Candidates | Fills | Median Adverse Gap Bps | Wins | Losses | Win Rate Pct | Profit Factor | Net Return Points | Net Pnl Rs |
|---|---|---|---|---|---|---|---|---|---|
| NO_GAP_OBSERVED | 972 | 206 | — | 109 | 97 | 52.91% | 2.2623 | +86.6170 | +42,606.99 |
| GAP_ACCEPTED | 23 | 23 | 0.6100 | 11 | 12 | 47.83% | 2.0417 | +9.8274 | +4,896.84 |
| GAP_REJECTED | 22 | 0 | 9.5105 | 0 | 0 | — | — | +0.0000 | +0.00 |

A real resting stop-market order cannot reject a gap after the opening price is observed. The 2 bps Gap2 rule needs an explicitly executable synthetic-trigger or stop-limit design before live use.

### Portfolio actual versus unconstrained

| Portfolio View | Fills | Wins | Losses | Flat Trades | Win Rate Pct | Profit Factor | Net Return Points | Net Pnl Rs | Gross Profit Points | Gross Loss Points | Gross Profit Rs | Gross Loss Rs | Average Return Points | Median Return Points | Average Pnl Rs | Median Pnl Rs | Average Win Points | Average Loss Points | Payoff Ratio | Sessions With Fills | Gross Pnl Rs | Estimated Cost Rs | Position Notional Rs |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| ACTUAL_CAP2_LEDGER | 229 | 120 | 109 | 0 | 52.40% | 2.2356 | +96.4444 | +47,503.84 | +174.4985 | +78.0541 | +85,015.90 | +37,512.06 | +0.4212 | +0.2306 | +207.44 | +112.70 | +1.4542 | -0.7161 | 2.0307 | 61 | +64,158.38 | +16,654.54 | +11,103,029.22 |
| UNCONSTRAINED_CANDIDATE_OUTCOMES | 232 | 120 | 112 | 0 | 51.72% | 2.1870 | +94.7084 | +46,652.79 | +174.4985 | +79.7901 | +85,015.90 | +38,363.11 | +0.4082 | +0.1563 | +201.09 | +65.36 | +1.4542 | -0.7124 | 2.0412 | 61 | +63,527.08 | +16,874.29 | +11,249,524.52 |

### Portfolio rejections

| Candidate Id | Session Date | Setup Id | Side | Symbol | Portfolio Reject Reason | Unconstrained Status | Unconstrained Net Return Pct | Unconstrained Net Pnl Rs |
|---|---|---|---|---|---|---|---|---|
| 2026-06-03\|09:40_SHORT\|LTM | 2026-06-03 | 09:40_SHORT | SHORT | LTM | DUPLICATE_SYMBOL_SAME_SIDE_LIMIT_2 | SQUARE_OFF | -0.07% | -32.28 |
| 2026-06-03\|09:45_SHORT\|LTM | 2026-06-03 | 09:45_SHORT | SHORT | LTM | DUPLICATE_SYMBOL_SAME_SIDE_LIMIT_2 | SQUARE_OFF | -0.52% | -250.35 |
| 2026-08-06\|09:45_LONG\|SHRIRAMFIN | 2026-08-06 | 09:45_LONG | LONG | SHRIRAMFIN | DUPLICATE_SYMBOL_SAME_SIDE_LIMIT_2 | STOPPED | -1.15% | -568.41 |

### Exposure

| Maximum Open Positions | Maximum Deployed Cash Equivalent Notional Rs | Median Deployed Notional When Active Rs | Modeled Capital Rs | Margin Reservation Per Entry Rs | Maximum Global Reservations | Same Symbol Same Side Limit |
|---|---|---|---|---|---|---|
| 8 | +394,734.05 | +95,940.00 | +120,000.00 | +10,000.00 | 12 | 2 |

These exposure figures are cash-equivalent proxies, not futures capital or margin usage.

## 11. Exits, holding time and excursions

### Exit reason

| Exit Reason | Fills | Wins | Losses | Flat Trades | Win Rate Pct | Profit Factor | Net Return Points | Net Pnl Rs | Gross Profit Points | Gross Loss Points | Gross Profit Rs | Gross Loss Rs | Average Return Points | Median Return Points | Average Pnl Rs | Median Pnl Rs | Average Win Points | Average Loss Points | Payoff Ratio | Sessions With Fills | Gross Pnl Rs | Estimated Cost Rs | Position Notional Rs |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| TARGET | 66 | 66 | 0 | 0 | 100.00% | ∞ | +117.7871 | +57,126.54 | +117.7871 | -0.0000 | +57,126.54 | -0.00 | +1.7847 | +1.8438 | +865.55 | +894.02 | +1.7847 | — | — | 35 | +61,936.87 | +4,810.33 | +3,206,884.88 |
| LAST_REAL_BAR_SENSITIVITY | 66 | 54 | 12 | 0 | 81.82% | 14.1031 | +52.6902 | +25,934.31 | +56.7114 | +4.0212 | +27,889.36 | +1,955.04 | +0.7983 | +0.5925 | +392.94 | +293.40 | +1.0502 | -0.3351 | 3.1340 | 36 | +30,762.61 | +4,828.30 | +3,218,864.09 |
| STOP | 97 | 0 | 97 | 0 | 0.00% | 0.0000 | -74.0329 | -35,557.02 | +0.0000 | +74.0329 | +0.00 | +35,557.02 | -0.7632 | -0.6527 | -366.57 | -322.71 | — | -0.7632 | — | 49 | -28,541.10 | +7,015.92 | +4,677,280.25 |

The 66 last-real-bar exits contribute +52.6902 points (54.63% of total). Exit reason, holding time and MFE/MAE are realized outcomes and cannot be used directly as entry filters.

### Selected-candidate source path terminal coverage

| Terminal Clock | Selected Candidates | Sessions | Earliest Session | Latest Session |
|---|---|---|---|---|
| 15:15 | 238 | 19 | 2026-08-03 | 2026-08-28 |
| 15:30 | 779 | 46 | 2026-05-27 | 2026-07-31 |

### Economic dependence on terminal policy

| Terminal View | Fills | Wins | Losses | Win Rate Pct | Profit Factor | Net Return Points | Net Pnl Rs |
|---|---|---|---|---|---|---|---|
| ALL_REFERENCE_TRADES | 229 | 120 | 109 | 52.40% | 2.2356 | +96.4444 | +47,503.84 |
| TARGET_AND_STOP_ONLY | 163 | 66 | 97 | 40.49% | 1.5910 | +43.7542 | +21,569.52 |
| ALL_LAST_REAL_BAR | 66 | 54 | 12 | 81.82% | 14.1031 | +52.6902 | +25,934.31 |
| LAST_REAL_BAR_AT_1530 | 55 | 46 | 9 | 83.64% | 16.6006 | +44.4024 | +21,837.62 |
| LAST_REAL_BAR_AT_1515 | 11 | 8 | 3 | 72.73% | 8.0534 | +8.2877 | +4,096.70 |

`TARGET_AND_STOP_ONLY` remains positive, but materially weaker. The 15:15 slice measures the direct effect of incomplete terminal coverage; all last-real-bar exits additionally depend on the decision to hold unresolved positions to the known terminal close.

### Terminal clock among last-real-bar exits

| Exit Clock | Fills | Wins | Losses | Net Return Points | Net Pnl Rs |
|---|---|---|---|---|---|
| 15:15 | 11 | 8 | 3 | +8.2877 | +4,096.70 |
| 15:30 | 55 | 46 | 9 | +44.4024 | +21,837.62 |

### Holding duration

| Holding Bin | Fills | Wins | Losses | Flat Trades | Win Rate Pct | Profit Factor | Net Return Points | Net Pnl Rs | Gross Profit Points | Gross Loss Points | Gross Profit Rs | Gross Loss Rs | Average Return Points | Median Return Points | Average Pnl Rs | Median Pnl Rs | Average Win Points | Average Loss Points | Payoff Ratio | Sessions With Fills | Gross Pnl Rs | Estimated Cost Rs | Position Notional Rs |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| <=5m | 40 | 6 | 34 | 0 | 15.00% | 0.2355 | -16.4784 | -8,064.56 | +5.0756 | +21.5539 | +2,501.86 | +10,566.42 | -0.4120 | -0.5573 | -201.61 | -277.76 | +0.8459 | -0.6339 | 1.3344 | 28 | -5,123.63 | +2,940.93 | +1,960,621.03 |
| 120m+ | 102 | 73 | 29 | 0 | 71.57% | 5.2529 | +84.3956 | +41,153.17 | +104.2399 | +19.8443 | +50,773.11 | +9,619.94 | +0.8274 | +0.6632 | +403.46 | +330.19 | +1.4279 | -0.6843 | 2.0868 | 48 | +48,586.71 | +7,433.54 | +4,955,692.35 |
| 61-120m | 20 | 8 | 12 | 0 | 40.00% | 1.3915 | +4.4327 | +2,552.99 | +15.7549 | +11.3223 | +7,800.65 | +5,247.66 | +0.2216 | -0.6515 | +127.65 | -316.61 | +1.9694 | -0.9435 | 2.0872 | 17 | +3,988.67 | +1,435.68 | +957,117.44 |
| 16-30m | 20 | 9 | 11 | 0 | 45.00% | 1.0203 | +0.1516 | +172.89 | +7.6294 | +7.4778 | +3,690.63 | +3,517.74 | +0.0076 | -0.5514 | +8.64 | -273.02 | +0.8477 | -0.6798 | 1.2470 | 14 | +1,619.20 | +1,446.31 | +964,208.51 |
| 31-60m | 19 | 10 | 9 | 0 | 52.63% | 2.8580 | +14.2726 | +6,861.60 | +21.9544 | +7.6818 | +10,574.37 | +3,712.77 | +0.7512 | +0.8471 | +361.14 | +372.11 | +2.1954 | -0.8535 | 2.5722 | 15 | +8,233.59 | +1,371.99 | +914,657.37 |
| 6-15m | 28 | 14 | 14 | 0 | 50.00% | 1.9505 | +9.6703 | +4,827.74 | +19.8443 | +10.1740 | +9,675.28 | +4,847.54 | +0.3454 | +0.1449 | +172.42 | +97.84 | +1.4175 | -0.7267 | 1.9505 | 20 | +6,853.84 | +2,026.10 | +1,350,732.52 |

### MFE/MAE bounds by outcome

| Outcome | Trades | Median Mfe Lower Pct | Median Mfe Upper Pct | Median Mae Lower Pct | Median Mae Upper Pct | Median Net R | Median Holding Minutes |
|---|---|---|---|---|---|---|---|
| LOSS | 109 | 0.35% | 0.35% | 0.50% | 0.56% | -1.2981 | 21.0000 |
| WIN | 120 | 1.76% | 1.81% | 0.22% | 0.25% | 2.1029 | 276.5000 |

### Losing trades that reached favorable R thresholds

| Cohort | Mfe Bound | Threshold R | Trades | Cohort Trades | Share Of Cohort Pct |
|---|---|---|---|---|---|
| ALL_LOSERS_UPPER_BOUND | mfe_upper_r | 0.2500 | 80 | 109 | 73.39% |
| STOP_EXITS_LOWER_BOUND | mfe_lower_r | 0.2500 | 65 | 97 | 67.01% |
| ALL_LOSERS_UPPER_BOUND | mfe_upper_r | 0.5000 | 62 | 109 | 56.88% |
| STOP_EXITS_LOWER_BOUND | mfe_lower_r | 0.5000 | 49 | 97 | 50.52% |
| ALL_LOSERS_UPPER_BOUND | mfe_upper_r | 0.7500 | 44 | 109 | 40.37% |
| STOP_EXITS_LOWER_BOUND | mfe_lower_r | 0.7500 | 34 | 97 | 35.05% |
| ALL_LOSERS_UPPER_BOUND | mfe_upper_r | 1.0000 | 32 | 109 | 29.36% |
| STOP_EXITS_LOWER_BOUND | mfe_lower_r | 1.0000 | 24 | 97 | 24.74% |
| ALL_LOSERS_UPPER_BOUND | mfe_upper_r | 1.5000 | 18 | 109 | 16.51% |
| STOP_EXITS_LOWER_BOUND | mfe_lower_r | 1.5000 | 13 | 97 | 13.40% |

### OHLC excursion quality

| Fills | Entry Bar Ambiguous | Exit Bar Ambiguous | Boundary Ambiguous | Median Mfe Bound Width Pct | Median Mae Bound Width Pct |
|---|---|---|---|---|---|
| 229 | 206 | 161 | 224 | 0.00% | 0.01% |

Minute OHLC cannot reveal the exact high/low sequence around entry and exits. Excursion-based stop or trailing research should wait for repaired tick/event paths.

## 12. Symbols, concentration and extreme trades

| Unique Symbols | Positive Symbols | Negative Symbols | One Fill Symbols | Top 5 Positive Symbols Share Of Net Pct | Top 5 Positive Symbols Share Of Net Points Pct | Best 5 Days Share Of Net Pct | Best 5 Days Share Of Net Points Pct | Best 10 Trades Share Of Net Pct | Best 10 Trades Share Of Net Points Pct | Absolute Symbol Points Hhi |
|---|---|---|---|---|---|---|---|---|---|---|
| 122 | 70 | 52 | 59 | 30.69% | 31.76% | 47 | 49 | 31.60% | 31.88% | 0.0150 |

### Top 15 symbols

| Symbol | Fills | Wins | Losses | Win Rate Pct | Profit Factor | Net Return Points | Net Pnl Rs |
|---|---|---|---|---|---|---|---|
| PAYTM | 9 | 5 | 4 | 55.56% | 2.6726 | +6.7176 | +3,309.02 |
| OFSS | 5 | 3 | 2 | 60.00% | 5.2819 | +7.3181 | +3,145.83 |
| MCX | 3 | 3 | 0 | 100.00% | ∞ | +6.3164 | +3,084.43 |
| PERSISTENT | 4 | 3 | 1 | 75.00% | 13.9684 | +5.1324 | +2,522.24 |
| MOTILALOFS | 3 | 2 | 1 | 66.67% | 8.7178 | +5.0462 | +2,516.67 |
| TCS | 3 | 2 | 1 | 66.67% | 10.3343 | +5.1433 | +2,463.88 |
| TECHM | 2 | 2 | 0 | 100.00% | ∞ | +4.9198 | +2,422.84 |
| IDEA | 3 | 2 | 1 | 66.67% | 8.9587 | +4.5449 | +2,272.14 |
| TRENT | 2 | 2 | 0 | 100.00% | ∞ | +4.6971 | +2,269.59 |
| BANDHANBNK | 3 | 2 | 1 | 66.67% | 7.3754 | +3.5379 | +1,765.70 |
| INDUSTOWER | 3 | 3 | 0 | 100.00% | ∞ | +3.3938 | +1,690.80 |
| ADANIGREEN | 1 | 1 | 0 | 100.00% | ∞ | +3.1097 | +1,514.16 |
| ANGELONE | 3 | 2 | 1 | 66.67% | 62.5540 | +2.9125 | +1,447.71 |
| VMM | 1 | 1 | 0 | 100.00% | ∞ | +2.8489 | +1,423.26 |
| TATAELXSI | 1 | 1 | 0 | 100.00% | ∞ | +2.8498 | +1,363.43 |

### Bottom 15 symbols

| Symbol | Fills | Wins | Losses | Win Rate Pct | Profit Factor | Net Return Points | Net Pnl Rs |
|---|---|---|---|---|---|---|---|
| KPITTECH | 2 | 0 | 2 | 0.00% | 0.0000 | -2.3040 | -1,145.62 |
| DMART | 2 | 0 | 2 | 0.00% | 0.0000 | -2.3036 | -1,090.98 |
| POWERINDIA | 3 | 0 | 3 | 0.00% | 0.0000 | -2.9712 | -994.72 |
| BPCL | 2 | 0 | 2 | 0.00% | 0.0000 | -1.8190 | -906.27 |
| LAURUSLABS | 3 | 1 | 2 | 33.33% | 0.1667 | -1.6024 | -774.13 |
| HDFCAMC | 3 | 1 | 2 | 33.33% | 0.2254 | -1.3968 | -675.74 |
| CANBK | 2 | 0 | 2 | 0.00% | 0.0000 | -1.2093 | -603.65 |
| POLICYBZR | 2 | 0 | 2 | 0.00% | 0.0000 | -1.2084 | -594.12 |
| GODFRYPHLP | 2 | 0 | 2 | 0.00% | 0.0000 | -1.2049 | -589.84 |
| NATIONALUM | 3 | 0 | 3 | 0.00% | 0.0000 | -1.1806 | -587.28 |
| BHARATFORG | 1 | 0 | 1 | 0.00% | 0.0000 | -1.1541 | -576.30 |
| HINDUNILVR | 1 | 0 | 1 | 0.00% | 0.0000 | -1.1525 | -573.89 |
| PNBHOUSING | 1 | 0 | 1 | 0.00% | 0.0000 | -1.1558 | -570.08 |
| VEDL | 2 | 0 | 2 | 0.00% | 0.0000 | -1.1169 | -555.85 |
| ABB | 2 | 0 | 2 | 0.00% | 0.0000 | -1.2071 | -554.52 |

### Best 15 trades

| Session Date | Setup Id | Side | Symbol | Entry Time | Exit Time | Exit Reason | Net Return Pct | Net Pnl Rs | Mfe Pct Ohlc Lower Bound | Mae Pct Ohlc Upper Bound |
|---|---|---|---|---|---|---|---|---|---|---|
| 2026-05-29 | 09:30_SHORT | SHORT | MCX | 2026-05-29 09:35:00+05:30 | 2026-05-29 15:24:00+05:30 | TARGET | 3.85% | +1,884.96 | 3.9999 | 0.5065 |
| 2026-07-23 | 09:30_SHORT | SHORT | OFSS | 2026-07-23 09:34:00+05:30 | 2026-07-23 10:05:00+05:30 | TARGET | 3.84% | +1,670.80 | 3.9937 | 0.5613 |
| 2026-07-22 | 09:30_SHORT | SHORT | ADANIGREEN | 2026-07-22 09:34:00+05:30 | 2026-07-22 15:30:00+05:30 | LAST_REAL_BAR_SENSITIVITY | 3.11% | +1,514.16 | 3.7395 | 0.6506 |
| 2026-07-23 | 09:25_SHORT | SHORT | VMM | 2026-07-23 09:27:00+05:30 | 2026-07-23 12:40:00+05:30 | TARGET | 2.85% | +1,423.26 | 2.9989 | 0.3999 |
| 2026-07-07 | 09:35_SHORT | SHORT | MOTHERSON | 2026-07-07 09:37:00+05:30 | 2026-07-07 10:37:00+05:30 | TARGET | 2.85% | +1,422.30 | 2.9979 | 0.0629 |
| 2026-07-22 | 09:35_SHORT | SHORT | BANDHANBNK | 2026-07-22 09:37:00+05:30 | 2026-07-22 10:30:00+05:30 | TARGET | 2.85% | +1,421.17 | 2.9987 | 0.2040 |
| 2026-07-24 | 09:40_SHORT | SHORT | MOTILALOFS | 2026-07-24 09:42:00+05:30 | 2026-07-24 09:56:00+05:30 | TARGET | 2.85% | +1,420.44 | 3.0000 | 0.0899 |
| 2026-07-15 | 09:25_SHORT | SHORT | PATANJALI | 2026-07-15 09:27:00+05:30 | 2026-07-15 09:34:00+05:30 | TARGET | 2.84% | +1,418.90 | 2.9935 | 0.3382 |
| 2026-07-24 | 09:35_SHORT | SHORT | MOTILALOFS | 2026-07-24 09:37:00+05:30 | 2026-07-24 09:48:00+05:30 | TARGET | 2.85% | +1,418.59 | 3.0000 | 0.1547 |
| 2026-07-08 | 09:25_SHORT | SHORT | JIOFIN | 2026-07-08 09:28:00+05:30 | 2026-07-08 14:28:00+05:30 | TARGET | 2.84% | +1,416.30 | 2.9939 | 0.3373 |
| 2026-08-10 | 09:40_SHORT | SHORT | PFC | 2026-08-10 09:42:00+05:30 | 2026-08-10 11:42:00+05:30 | TARGET | 2.85% | +1,413.77 | 2.9954 | 0.3218 |
| 2026-07-28 | 09:25_SHORT | SHORT | GVT&D | 2026-07-28 09:27:00+05:30 | 2026-07-28 10:20:00+05:30 | TARGET | 2.85% | +1,399.92 | 3.0000 | 0.4055 |
| 2026-08-25 | 09:45_LONG | LONG | IDEA | 2026-08-25 09:48:00+05:30 | 2026-08-25 12:15:00+05:30 | TARGET | 2.78% | +1,391.31 | 2.9332 | 0.6821 |
| 2026-06-29 | 09:35_SHORT | SHORT | PERSISTENT | 2026-06-29 09:37:00+05:30 | 2026-06-29 14:07:00+05:30 | TARGET | 2.84% | +1,389.67 | 2.9928 | 0.8551 |
| 2026-07-10 | 09:45_LONG | LONG | PAYTM | 2026-07-10 09:47:00+05:30 | 2026-07-10 14:30:00+05:30 | TARGET | 2.85% | +1,388.41 | 2.9992 | 0.3037 |

### Worst 15 trades

| Session Date | Setup Id | Side | Symbol | Entry Time | Exit Time | Exit Reason | Net Return Pct | Net Pnl Rs | Mfe Pct Ohlc Lower Bound | Mae Pct Ohlc Upper Bound |
|---|---|---|---|---|---|---|---|---|---|---|
| 2026-07-16 | 09:35_LONG | LONG | BPCL | 2026-07-16 09:37:00+05:30 | 2026-07-16 13:26:00+05:30 | STOP | -1.16% | -580.33 | 0.9356 | 1.1259 |
| 2026-07-15 | 09:40_SHORT | SHORT | PATANJALI | 2026-07-15 09:42:00+05:30 | 2026-07-15 09:47:00+05:30 | STOP | -1.16% | -579.16 | 1.9963 | 1.5498 |
| 2026-08-25 | 09:35_LONG | LONG | LICI | 2026-08-25 09:39:00+05:30 | 2026-08-25 14:23:00+05:30 | STOP | -1.16% | -577.93 | 0.4104 | 1.1961 |
| 2026-08-13 | 09:35_LONG | LONG | JIOFIN | 2026-08-13 09:37:00+05:30 | 2026-08-13 11:48:00+05:30 | STOP | -1.15% | -576.75 | 0.4249 | 1.1201 |
| 2026-06-05 | 09:30_LONG | LONG | ETERNAL | 2026-06-05 09:32:00+05:30 | 2026-06-05 09:34:00+05:30 | STOP | -1.16% | -576.49 | 0.0000 | 1.0271 |
| 2026-07-23 | 09:40_SHORT | SHORT | BHARATFORG | 2026-07-23 09:42:00+05:30 | 2026-07-23 10:19:00+05:30 | STOP | -1.15% | -576.30 | 0.0461 | 1.0594 |
| 2026-07-27 | 09:45_LONG | LONG | KFINTECH | 2026-07-27 09:50:00+05:30 | 2026-07-27 11:11:00+05:30 | STOP | -1.15% | -575.39 | 0.7822 | 1.0135 |
| 2026-07-03 | 09:30_SHORT | SHORT | UNIONBANK | 2026-07-03 09:34:00+05:30 | 2026-07-03 10:17:00+05:30 | STOP | -1.15% | -575.20 | 0.4987 | 1.0960 |
| 2026-06-30 | 09:35_SHORT | SHORT | KPITTECH | 2026-06-30 09:38:00+05:30 | 2026-06-30 10:55:00+05:30 | STOP | -1.15% | -574.76 | 1.1066 | 1.1286 |
| 2026-08-12 | 09:40_SHORT | SHORT | RECLTD | 2026-08-12 09:42:00+05:30 | 2026-08-12 12:54:00+05:30 | STOP | -1.15% | -574.68 | 0.1031 | 1.1042 |
| 2026-06-02 | 09:30_SHORT | SHORT | HINDUNILVR | 2026-06-02 09:33:00+05:30 | 2026-06-02 13:35:00+05:30 | STOP | -1.15% | -573.89 | 0.4386 | 1.0266 |
| 2026-06-30 | 09:40_SHORT | SHORT | KPITTECH | 2026-06-30 09:42:00+05:30 | 2026-06-30 10:52:00+05:30 | STOP | -1.15% | -570.86 | 0.7794 | 1.1765 |
| 2026-06-18 | 09:45_SHORT | SHORT | PAYTM | 2026-06-18 09:47:00+05:30 | 2026-06-18 10:00:00+05:30 | STOP | -1.16% | -570.71 | 0.6442 | 1.0830 |
| 2026-08-20 | 09:30_LONG | LONG | PNBHOUSING | 2026-08-20 09:32:00+05:30 | 2026-08-20 09:39:00+05:30 | STOP | -1.16% | -570.08 | 0.1579 | 1.1554 |
| 2026-07-15 | 09:30_LONG | LONG | PAYTM | 2026-07-15 09:32:00+05:30 | 2026-07-15 09:40:00+05:30 | STOP | -1.15% | -567.37 | 0.0000 | 1.0590 |

Symbol blacklists or whitelists are not supported by small, selected samples and would introduce survivorship risk.

## 13. Risk and statistical uncertainty

### Risk summary

| Best Pnl Day | Best Day Pnl Rs | Worst Pnl Day | Worst Day Pnl Rs | Average Daily Pnl Rs | Median Daily Pnl Rs | Daily Pnl Std Rs | Max Consecutive Positive Days | Max Consecutive Negative Days | Max Consecutive Winning Trades | Max Consecutive Losing Trades | Max Drawdown Points | Max Drawdown Pnl Rs | Win Rate Wilson 95 Low Pct | Win Rate Wilson 95 High Pct | Extra Break Even Cost Bps On Fixed Notional |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 2026-07-07 | +5,602.68 | 2026-07-09 | -1,192.41 | +730.83 | +349.48 | +1,546.47 | 5 | 2 | 7 | 5 | +5.2693 | +2,591.12 | 45.95% | 58.78% | 42.7846 |

### Drawdown episodes

| Start Session | Trough Session | Recovery Session | Underwater Sessions | Depth Pnl Rs | Depth Return Points | Recovered |
|---|---|---|---|---|---|---|
| 2026-06-03 | 2026-06-18 | 2026-06-29 | 16 | +2,591.12 | +5.2693 | Yes |
| 2026-07-13 | 2026-07-20 | 2026-07-22 | 6 | +1,500.10 | +3.0275 | Yes |
| 2026-08-03 | 2026-08-05 | 2026-08-10 | 4 | +1,262.49 | +2.7266 | Yes |
| 2026-07-08 | 2026-07-09 | 2026-07-10 | 1 | +1,192.41 | +2.4543 | Yes |
| 2026-08-19 | 2026-08-21 | 2026-08-25 | 3 | +1,172.44 | +2.3632 | Yes |
| 2026-06-29 | 2026-06-30 | 2026-07-01 | 1 | +1,145.62 | +2.3040 | Yes |
| 2026-08-10 | 2026-08-12 | 2026-08-18 | 5 | +1,089.78 | +2.5777 | Yes |
| 2026-07-24 | 2026-07-27 | 2026-07-28 | 1 | +919.61 | +1.7545 | Yes |
| 2026-05-29 | 2026-06-01 | 2026-06-03 | 2 | +565.65 | +1.1520 | Yes |
| 2026-07-02 | 2026-07-03 | 2026-07-06 | 1 | +45.04 | +0.1215 | Yes |

### IID session bootstrap — conditional on this sample

| Scenario | Bootstrap Unit | Bootstrap Replicates | Probability Positive Total Pnl Pct | Total Pnl Rs P025 | Total Pnl Rs Median | Total Pnl Rs P975 | Net Points P025 | Net Points Median | Net Points P975 | Pf P025 | Pf Median | Pf P975 |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| REFERENCE_15_0 | SESSION_WITH_REPLACEMENT | 10,000 | 99.99% | +24,309.46 | +47,164.50 | +72,378.32 | +48.6106 | +95.8618 | +147.8527 | 1.5931 | 2.2320 | 3.0227 |
| STRESS_20_2 | SESSION_WITH_REPLACEMENT | 10,000 | 100.00% | +17,117.13 | +39,373.00 | +63,894.31 | +34.0788 | +79.5766 | +130.4053 | 1.3805 | 1.9379 | 2.6258 |
| STRESS_25_5 | SESSION_WITH_REPLACEMENT | 10,000 | 99.71% | +8,283.06 | +29,632.75 | +53,121.73 | +15.5173 | +59.5377 | +108.0346 | 1.1564 | 1.6307 | 2.2092 |

### Random ordering of the realized daily P&Ls

| Method | Replicates | Observed Mdd Pnl Rs | Mdd Pnl Rs P50 | Mdd Pnl Rs P75 | Mdd Pnl Rs P90 | Mdd Pnl Rs P95 | Mdd Pnl Rs P975 |
|---|---|---|---|---|---|---|---|
| RANDOM_SESSION_ORDER | 5,000 | +2,591.12 | +2,559.61 | +3,100.85 | +3,723.65 | +4,166.62 | +4,537.07 |

Resampling quantifies conditional sample/order uncertainty only. It does not repair model selection, static-universe bias, missing paths, cash-versus-futures mismatch or live execution risk.

## 14. What is supported and what is not

- **Supported descriptively:** the sealed V12 run reproduces exactly; all three cost cases remain positive; the late-SHORT volume filter reduces observed drawdown and slightly improves the selected-history result versus V11.
- **Not established:** live futures profitability, untouched out-of-sample accuracy, causal superiority over V11, or the profitability of rejected candidates after portfolio displacement.
- **Main statistical risk:** V12 was chosen after 39 isolated challengers; only eight daily results differ from V11 and the paired reference interval crosses zero.
- **Main data risk:** incomplete symbol-session coverage, one missing regular session, static/potentially future-known universes and mixed terminal times.
- **Main execution risk:** cash-equity paths and proxy sizing replace rolling futures contracts, lots, margins, spread and market impact.
- **Indicators not present:** ATR, RSI, ADX, VWAP, point-in-time index/sector regime, opening breadth and order-book liquidity are not part of frozen V12.
- **Main report discipline:** indicator bins, symbol tables, exit reasons, MFE and MAE are hypothesis generators, not post-hoc filters.

## 15. Safe staged improvement plan

### Stage A — freeze the comparator set

1. Preserve the exact V10, V11 and V12 hashes. Register every future test before reading its result.
2. Use V11 as control and V12 as challenger. Do not replace the control because V12 has the best observed drawdown.

### Stage B — repair market-data validity

1. Reconstruct daily point-in-time F&O membership and deterministic front-month rolls.
2. Bind actual futures one-minute/tick price and OI, dated lots/ticks/margins, complete session paths and a verified pre-close exit.
3. Re-run V10/V11/V12 on the common repaired input and reject improvements that disappear.

### Stage C — prospective mechanism validation

1. Freeze volume ratio 1.50 and collect genuinely new sessions without tuning.
2. Record V11 and V12 decisions side by side, especially the late-SHORT exclusions and all portfolio displacement.
3. Require enough affected decisions, not merely 100 total fills; most V11/V12 trades are identical.

### Stage D — five-minute quality research

1. Treat the 1.50 late-SHORT volume rule as the only active hypothesis. Do not select another threshold from this report.
2. If new data supports it, predeclare one setup-specific test involving prior-OI quality, relative rank margin or market/sector context. Use point-in-time inputs only.
3. Apply multiple-testing control and preserve the complete candidate stream so rejected-candidate counterfactuals remain available.

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

Promote nothing unless V12 beats V11 on untouched repaired futures data, remains positive in both stress cases, preserves drawdown and concentration, and achieves decision/fill parity in shadow and paper execution. Otherwise retain V11/V12 as research controls.

## 16. Reproducibility and supporting evidence

Backtest command used:

```powershell
cd "C:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2"
python -u fno_v12_backtest.py run --all-usable-history
```

Validation command:

```powershell
python -u fno_v12_backtest.py validate --provenance "C:\TradingData\eqidv2\fno_oi\strategy_research\v12_s06_late_short_volume_min150_full_history_v1\run_20260831T001454752119+0530\provenance.json"
```

Report command:

```powershell
python -u fno_v12_full_historical_report.py --source-run "C:\TradingData\eqidv2\fno_oi\strategy_research\v12_s06_late_short_volume_min150_full_history_v1\run_20260831T001454752119+0530" --lineage-run "C:\TradingData\eqidv2\fno_oi\strategy_research\v12_fno_staged_research_v1\run_20260830T233338596157+0530" --report "C:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2\report_v12.md" --assets-dir "C:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2\report_v12_assets"
```

Supporting tables and charts: `C:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2\report_v12_assets`.
The sealed run was validated and read but not modified. This report writes **83 CSV tables** and **6 charts** outside it.

## 17. Glossary

- **Net return points:** arithmetic sum of per-trade net percentage returns; not compounded portfolio return.
- **PF:** gross positive net-return points divided by absolute gross negative net-return points.
- **MDD:** maximum peak-to-trough drawdown of cumulative daily summed return points unless marked Rs.
- **WR:** winning closed trades divided by closed trades.
- **S+N:** the Nth completed one-minute bar after the five-minute signal closes.
- **MFE/MAE:** bounded favorable/adverse excursion after entry; future outcome data, not an entry feature.
- **BH q-value:** multiple-test-adjusted p-value; low q reduces but does not eliminate false-discovery risk.
- **Research-only:** reproducible hypothesis evidence without paper/live authority or a claim of achievable returns.
