# V11 FNO Stage 10 — full historical deep-study report

Generated: 2026-08-30T22:12:59.794452+05:30
Validated standalone run: `C:\TradingData\eqidv2\fno_oi\strategy_research\v11_stage10_fixed_full_history_v1\run_20260830T213455896360+0530`
Profile: `V11_S10_POST_HOC_TOP2_1436C7D363`
Profile SHA-256: `8dfc162701705c0daa89d7ba2faa8dd7ddd3ff8eb6605370d96de1fdaa1f6fe1`
Historical input binding: `24e4da6c580693637bd7ce9c50c618b07d2e8a6a8dfded4498658d8eab113f2b`

> **Research boundary:** this result is explicitly `research_only=true`, `headline_valid=false`, `promotion_eligible=false`, and has no live/paper authority. It is the strongest post-hoc configuration observed on this history, not an untouched validation result.

## Executive conclusion

The sealed reference replay covers **65 usable sessions** from **2026-05-27 through 2026-08-28** and records **237 fills, 123-114, WR 51.90%, PF 2.1452, +94.6309 net points and Rs +46,783.23 modeled P&L**, with daily MDD 8.5674 points.

The result remains positive in the harsh 25 bps cost + 5 bps slippage case: PF 1.5657, +56.8171 points and Rs +28,481.76. Both sides and all ten setup buckets remain positive under that case: **yes**.

The evidence is encouraging but concentrated: July contributes **66.91%** of reference net points, and last-real-bar exits contribute **57.42%**. The six-session extension earns +6.0895 points, but it was used in model selection and is not an untouched holdout.

Most importantly, none of the 20 tested numeric features available by entry separates winners from losers at BH-adjusted q < 0.05. Several activity features predict whether an order gets filled, but fill probability is not accuracy. The safest conclusion is to freeze V11, repair execution/data validity, and validate prospectively before changing global thresholds.

## 1. Integrity, data contract and scope

- Provenance and all **36** inventoried run artifacts passed size/hash/set validation.
- The profile and input were re-bound to `8dfc162701705c0daa89d7ba2faa8dd7ddd3ff8eb6605370d96de1fdaa1f6fe1` and `24e4da6c580693637bd7ce9c50c618b07d2e8a6a8dfded4498658d8eab113f2b`.
- Calendar span contains **66** expected regular sessions; missing validated session: **2026-08-26**.
- Strict source completeness failed for **7,172 of 13,522 symbol-sessions (53.04%)**. This is universe/path coverage, not the selected-candidate `data_incomplete_candidates` count, which is zero.
- The cache contains base-qualified 5-minute candidates, not every universe symbol that failed base selection. Filter counterfactual P&L is therefore unavailable unless the full stream is replayed.
- Futures OI drives selection, while 5-minute price/EMA/volume and 1-minute execution use NSE cash-equity paths. Quantity is cash-equivalent share sizing, not dated futures-lot sizing.

### Source segments

| Segment Id | From Day | Through Day | Contract Month | Universe Master Date | Sessions | Candidates | Expected Symbol Sessions | Source Incomplete Symbol Sessions | Source Incomplete Pct | Headline Source Complete |
|---|---|---|---|---|---|---|---|---|---|---|
| AUG_CORE_59 | 2026-05-27 | 2026-08-19 | 26AUG | 2026-08-11 | 59 | 1,126 | 12,272 | 5,922 | 48.26% | No |
| AUG_EXTENSION_20_21 | 2026-08-20 | 2026-08-21 | 26AUG | 2026-08-11 | 2 | 27 | 416 | 416 | 100.00% | No |
| SEP_ROLLOVER_24_25 | 2026-08-24 | 2026-08-25 | 26SEP | 2026-08-24 | 2 | 48 | 414 | 414 | 100.00% | No |
| SEP_DIAGNOSTIC_27 | 2026-08-27 | 2026-08-27 | 26SEP | 2026-08-27 | 1 | 22 | 210 | 210 | 100.00% | No |
| SEP_DIAGNOSTIC_28 | 2026-08-28 | 2026-08-28 | 26SEP | 2026-08-28 | 1 | 18 | 210 | 210 | 100.00% | No |

### Validity tests that could not be honestly executed

| Stage Id | Test Id | Status | Reason |
|---|---|---|---|
| STAGE_01_DATA_VALIDITY | POINT_IN_TIME_UNIVERSE_FULL_HISTORY | BLOCKED_VALIDITY | Dated masters/universes exist for only 13 recent sessions; the 59-session core reuses an Aug-11 universe backward. |
| STAGE_01_DATA_VALIDITY | AUG_26_EXACT_1530_REPLAY | BLOCKED_VALIDITY | Aug-26 equity 1-minute paths end at 15:15; no exact 15:30 current-strategy replay is available. |
| STAGE_01_DATA_VALIDITY | FULL_EXACT_1530_PATHS | BLOCKED_VALIDITY | 246 of 1,134 selected candidate paths stop at 15:15; only 888 reach 15:30. |
| STAGE_02_FUTURES_EXECUTION | ROLLING_FRONT_MONTH_FUTURES_1M | BLOCKED_VALIDITY | Complete actual front-month futures 1-minute histories for MAY through SEP are absent. |
| STAGE_02_FUTURES_EXECUTION | DATED_LOT_TICK_MARGIN_COSTS | BLOCKED_VALIDITY | Full-history dated masters and historical SPAN/exposure margin snapshots are absent. |
| STAGE_08_STRUCTURAL_FILTERS | FUTURES_PRICE_OI_PERSISTENCE | BLOCKED_VALIDITY | Current immutable cache has futures OI selection but no complete rolling futures 1-minute price/execution path or causal OI-persistence sidecar. |
| STAGE_08_STRUCTURAL_FILTERS | INDEX_SECTOR_VWAP_ALIGNMENT | BLOCKED_VALIDITY | No snapshot-bound point-in-time index/sector histories and dated membership mappings cover all 65 sessions. |
| STAGE_08_STRUCTURAL_FILTERS | ATR_NORMALIZED_RISK | BLOCKED_VALIDITY | The frozen candidate cache lacks a bound prior-session ATR history; deriving it from partial paths would change the data contract. |
| STAGE_09_PORTFOLIO | ACTUAL_FUTURES_RISK_SIZING | BLOCKED_VALIDITY | Actual contract prices, dated lot sizes, and historical margins are not complete for full history. |

## 2. Exact locked strategy and parameter values

### Global V11 overlays and economic assumptions

| Layer | Parameter | Value | Scope |
|---|---|---|---|
| Identity | V11 profile | V11_S10_POST_HOC_TOP2_1436C7D363 | locked standalone |
| Identity | Profile SHA-256 | 8dfc162701705c0daa89d7ba2faa8dd7ddd3ff8eb6605370d96de1fdaa1f6fe1 | entire profile |
| Base | V10 base profile | V10_STAGE7_LOCKED_BACKTEST_20260827 | all setups |
| 5m selection | 09:40 LONG directional move floor | 0.40% | 09:40_LONG only |
| 5m selection | 09:35 LONG directional move ceiling | 0.50% | 09:35_LONG only |
| 1m timing | Earliest trigger-fill minute | S+3 | 09:30_SHORT only |
| Gap | Maximum adverse trigger gap | 2 bps | strong identity gap events |
| Gap | Reject every gap | False | gap <= 2 bps can fill |
| Portfolio | Same symbol + same side concurrent limit | 2 | all setups |
| Portfolio | Same symbol + opposite side | Prohibited | all setups |
| Portfolio | Modeled capital | Rs 120,000 | global ledger |
| Portfolio | Margin reservation per entry | Rs 10,000 | global ledger |
| Portfolio | Maximum reservations | 12 | global ledger |
| Sizing | Target cash-equivalent exposure per fill | Rs 50,000 | quantity=floor(exposure/entry) |
| Exit | Dynamic exit overlay | None | base stop/target remains |
| Exit | Same-bar collision | STOP_FIRST | conservative OHLC rule |
| Exit | Square-off clock | 15:30 | when a real bar exists |
| Exit | Terminal policy | LAST_REAL_BAR_SENSITIVITY | partial-path sensitivity |
| Costs | Reference | 15 bps cost + 0 bps slippage | headline diagnostic |
| Costs | Stress | 20 bps cost + 2 bps slippage | robustness |
| Costs | Harsh stress | 25 bps cost + 5 bps slippage | robustness |

### Five-minute selection book

Each row is one side-specific setup for the 5-minute candle ending at `signal_end`. The mixed `max_entries` value is the maximum ranked candidates for that setup/side/slot—not a daily maximum and not the same as the concurrent same-symbol limit. Both LONG and SHORT rows can select on the same slot. `picker` decides ranking inside the eligible bucket; portfolio rules are applied later in chronological order.

| Setup Id | Signal End | Side | Max Entries | Picker | Five Minute Ema Rule | Effective Move Rule | Oi Change Pct | Volume Ratio | Min Traded Value Cr |
|---|---|---|---|---|---|---|---|---|---|
| 09:25_LONG | 09:25 | LONG | 4 | max_move | EMA9>EMA20>EMA50 | >= +0.3% | 0.10% | 3.0000 | 0.0000 |
| 09:25_SHORT | 09:25 | SHORT | 4 | max_move | EMA9<EMA20<EMA50 | <= -0.2% | 0.10% | 1.5000 | 2.5000 |
| 09:30_LONG | 09:30 | LONG | 1 | max_move | EMA9>EMA20>EMA50 | >= +0.65% | 0.10% | 1.0000 | 0.0000 |
| 09:30_SHORT | 09:30 | SHORT | 4 | max_volume | EMA9<EMA20<EMA50 | <= -0.2% | 1.00% | 1.0000 | 2.5000 |
| 09:35_LONG | 09:35 | LONG | 1 | max_liquidity | EMA9>EMA20>EMA50 | >= +0.20% and <= +0.50% | 0.10% | 1.0000 | 0.0000 |
| 09:35_SHORT | 09:35 | SHORT | 2 | max_liquidity | EMA9<EMA20<EMA50 | <= -0.5% | 1.00% | 1.0000 | 0.0000 |
| 09:40_LONG | 09:40 | LONG | 1 | max_liquidity | EMA9>EMA20>EMA50 | >= +0.40% (Stage 7 floor) | 0.10% | 2.0000 | 0.0000 |
| 09:40_SHORT | 09:40 | SHORT | 1 | max_move | EMA9<EMA20<EMA50 | <= -0.2% | 0.10% | 1.0000 | 0.0000 |
| 09:45_LONG | 09:45 | LONG | 1 | max_move | EMA9>EMA20>EMA50 | >= +0.65% | 0.10% | 1.0000 | 0.0000 |
| 09:45_SHORT | 09:45 | SHORT | 1 | max_volume | EMA9<EMA20<EMA50 | <= -0.2% | 0.75% | 1.0000 | 0.0000 |

### One-minute confirmation, entry and exit book

A candidate monitors setup-relative one-minute bars, requires the side-aware candle checks below, then places a stop trigger at the signal extreme plus any buffer. Entry expires at S+5. The frozen same-bar rule is `STOP_FIRST`. Only `09:30_SHORT` has the V11 S+3 earliest-fill overlay.

| Setup Id | Body Ratio | Max Wick Ratio | Effective Close Location Min | Effective Max Confirmation Minute | Effective Buffer Bps | Effective Midpoint Invalidation | Entry Expiry Minute | Stop Pct | Target Pct | Post Confirmation Cancel | Allow Cap Reassignment | Same Bar Policy |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 09:25_LONG | 0.0000 | 0.5000 | — | 3 | 0.0000 | No | 5 | 0.40% | 1.00% | Yes | Yes | STOP_FIRST |
| 09:25_SHORT | 0.6000 | 0.6000 | — | 3 | 2.0000 | No | 5 | 0.50% | 3.00% | Yes | Yes | STOP_FIRST |
| 09:30_LONG | 0.5000 | 0.5000 | — | 1 | 0.0000 | No | 5 | 1.00% | 2.50% | Yes | Yes | STOP_FIRST |
| 09:30_SHORT | 0.4500 | 0.3000 | 0.5000 | 3 | 0.0000 | Yes | 5 | 1.00% | 4.00% | Yes | Yes | STOP_FIRST |
| 09:35_LONG | 0.6000 | 0.5000 | — | 1 | 0.0000 | No | 5 | 1.00% | 2.50% | Yes | Yes | STOP_FIRST |
| 09:35_SHORT | 0.4000 | 0.5000 | — | 1 | 0.0000 | No | 5 | 1.00% | 3.00% | Yes | Yes | STOP_FIRST |
| 09:40_LONG | 0.5000 | 0.5000 | — | 1 | 0.0000 | No | 5 | 0.50% | 2.50% | Yes | Yes | STOP_FIRST |
| 09:40_SHORT | 0.4000 | 0.5000 | — | 1 | 0.0000 | No | 5 | 1.00% | 3.00% | Yes | Yes | STOP_FIRST |
| 09:45_LONG | 0.4000 | 0.5000 | — | 1 | 0.0000 | No | 5 | 1.00% | 3.00% | Yes | Yes | STOP_FIRST |
| 09:45_SHORT | 0.4000 | 0.3000 | — | 1 | 0.0000 | No | 5 | 1.00% | 2.00% | Yes | Yes | STOP_FIRST |

## 3. Selection-to-exit funnel

| Step | Count |
|---|---|
| Base 5m | 1,241 |
| After overlays | 1,134 |
| 1m confirmed | 409 |
| Filled | 237 |
| Winners | 123 |

Retention is **91.38%** from the 1,241 cached base candidates to post-overlay selection, confirmation is **36.07%** of selected, and fills are **20.90%** of selected / **57.95%** of confirmed.

### Five-minute overlay exclusions

| Selection Reason | Rejections | Affected Sessions | Median Price Change Pct |
|---|---|---|---|
| 0935_LONG_MOVE_ABOVE_CHALLENGER_MAX | 77 | 40 | 0.67% |
| STAGE7_0940_LONG_MOVE_BELOW_040 | 30 | 22 | 0.28% |

The 107 excluded rows were not replayed through entry/exit. They are **selection exclusions**, not proven avoided losses.

### Final candidate lifecycle states

| Status | Count | Share Pct |
|---|---|---|
| NO_CONFIRMATION | 693 | 61.11% |
| POSTCONF_CANCELLED | 106 | 9.35% |
| STOPPED | 100 | 8.82% |
| SQUARE_OFF | 71 | 6.26% |
| TARGETED | 66 | 5.82% |
| WINDOW_EXPIRED | 63 | 5.56% |
| PRECONF_INVALIDATED | 32 | 2.82% |
| DUPLICATE_REJECTED | 3 | 0.26% |

### Terminal/rejection reasons

| Reason | Count | Share Pct |
|---|---|---|
| CONFIRMATION_WINDOW_EXPIRED | 693 | 61.11% |
| STOP | 100 | 8.82% |
| CLOSE_REVERSED_THROUGH_SIGNAL_CLOSE | 82 | 7.23% |
| LAST_REAL_BAR_SENSITIVITY | 71 | 6.26% |
| TARGET | 66 | 5.82% |
| ENTRY_WINDOW_EXPIRED | 63 | 5.56% |
| CLOSE_CROSSED_FIVE_MINUTE_MIDPOINT | 32 | 2.82% |
| ADVERSE_GAP_GUARD_REJECTED | 24 | 2.12% |
| DUPLICATE_SYMBOL_SAME_SIDE_LIMIT_2:CONSERVATIVE_NO_BACKFILL | 3 | 0.26% |

### One-minute failed-check occurrences

| Reason | Occurrences |
|---|---|
| CLOSE_NOT_BEYOND_FIVE_MINUTE_CLOSE | 926 |
| WRONG_CANDLE_DIRECTION | 843 |
| BODY_RATIO_BELOW_MINIMUM | 740 |
| ADVERSE_WICK_RATIO_ABOVE_MAXIMUM | 305 |
| CLOSE_LOCATION_BELOW_MINIMUM | 87 |
| PRECONF_MIDPOINT_INVALIDATED | 32 |
| NONPOSITIVE_RANGE | 1 |

Failure codes can overlap within a candle/candidate, so their counts do not sum to candidate totals.

![Selection funnel](report_v11_assets/selection_funnel.png)

## 4. Headline economics, risk and cost sensitivity

| Sessions | Fills | Wins | Losses | Win Rate Pct | Profit Factor | Gross Return Points | Net Return Points | Gross Pnl Rs | Estimated Cost Rs | Net Pnl Rs | Max Drawdown Points |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 65 | 237 | 123 | 114 | 51.90% | 2.1452 | +130.1809 | +94.6309 | +64,001.28 | +17,218.05 | +46,783.23 | +8.5674 |

Average win is +1.4412 points, average loss -0.7249, payoff ratio 1.9882, and expectancy +0.3993 points/fill. The 95% Wilson interval around historical WR is 45.56%–58.18%.

### Three sealed cost cases

| Scenario | Cost Bps | Slippage Bps | Fills | Wins | Losses | Win Rate Pct | Profit Factor | Net Return Points | Net Pnl Rs | Max Daily Drawdown Points | Positive Days | Negative Days | Net Pnl Change Vs Reference Rs | Net Pnl Retained Vs Reference Pct |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| REFERENCE_15_0 | 15.0000 | 0.0000 | 237 | 123 | 114 | 51.90% | 2.1452 | +94.6309 | +46,783.23 | +8.5674 | 37 | 25 | +0.00 | 100.00% |
| STRESS_20_2 | 20.0000 | 2.0000 | 237 | 119 | 118 | 50.21% | 1.8627 | +77.8776 | +38,681.92 | +9.8708 | 34 | 28 | -8,101.31 | 82.68% |
| STRESS_25_5 | 25.0000 | 5.0000 | 237 | 114 | 123 | 48.10% | 1.5657 | +56.8171 | +28,481.76 | +11.0343 | 32 | 30 | -18,301.47 | 60.88% |

![Cost scenarios](report_v11_assets/cost_scenarios.png)

### Outcome changes as costs rise

| Reference 15 0 | Stress 20 2 | Stress 25 5 | Trades |
|---|---|---|---|
| LOSS | LOSS | LOSS | 114 |
| WIN | WIN | WIN | 114 |
| WIN | WIN | LOSS | 5 |
| WIN | LOSS | LOSS | 4 |

Reference modeled costs remove 35.5500 points / Rs 17,218.05. The fixed-trade arithmetic break-even cushion is about 40.76 additional bps on summed notional; this is not a live capacity estimate because fills and prices can change with friction.

![Equity and drawdown](report_v11_assets/equity_and_drawdown.png)

## 5. Stability through time

### Core, extension, halves and recent window

| Period | Sessions | Post Overlay Selected | Fills | Wins | Losses | Win Rate Pct | Profit Factor | Net Return Points | Net Pnl Rs |
|---|---|---|---|---|---|---|---|---|---|
| FULL_65 | 65 | 1,134 | 237 | 123 | 114 | 51.90% | 2.1452 | +94.6309 | +46,783.23 |
| CORE_59 | 59 | 1,035 | 215 | 113 | 102 | 52.56% | 2.1863 | +88.5414 | +43,759.55 |
| FORWARD_6 | 6 | 99 | 22 | 10 | 12 | 45.45% | 1.7612 | +6.0895 | +3,023.68 |
| FIRST_HALF_32 | 32 | 466 | 100 | 54 | 46 | 54.00% | 2.2584 | +46.6505 | +23,130.41 |
| SECOND_HALF_33 | 33 | 668 | 137 | 69 | 68 | 50.36% | 2.0531 | +47.9804 | +23,652.82 |
| LAST_14_USABLE | 14 | 173 | 41 | 19 | 22 | 46.34% | 1.8290 | +12.6005 | +6,428.26 |

The six-session extension has only 22 fills and was part of the Stage 10 selection gate. It is a sensitivity slice, not independent evidence.

### Monthly

| Period | Sessions | Fills | Wins | Losses | Win Rate Pct | Profit Factor | Net Return Points | Net Pnl Rs |
|---|---|---|---|---|---|---|---|---|
| 2026-05 | 2 | 7 | 5 | 2 | 71.43% | 8.2258 | +8.0446 | +3,977.37 |
| 2026-06 | 21 | 50 | 22 | 28 | 44.00% | 1.2867 | +7.0084 | +3,594.72 |
| 2026-07 | 23 | 120 | 69 | 51 | 57.50% | 2.8158 | +63.3199 | +30,864.64 |
| 2026-08 | 19 | 60 | 27 | 33 | 45.00% | 1.7322 | +16.2580 | +8,346.51 |

![Monthly P&L](report_v11_assets/monthly_net_pnl.png)

### Weekly

| Period | Sessions | Fills | Wins | Losses | Profit Factor | Net Return Points | Net Pnl Rs |
|---|---|---|---|---|---|---|---|
| 2026-W22 | 2 | 7 | 5 | 2 | 8.2258 | +8.0446 | +3,977.37 |
| 2026-W23 | 5 | 16 | 8 | 8 | 1.6073 | +4.9373 | +2,357.32 |
| 2026-W24 | 5 | 8 | 2 | 6 | 0.1643 | -4.8112 | -2,191.67 |
| 2026-W25 | 5 | 8 | 3 | 5 | 1.4523 | +1.2576 | +627.32 |
| 2026-W26 | 4 | 9 | 4 | 5 | 1.4875 | +2.0301 | +1,075.21 |
| 2026-W27 | 5 | 19 | 11 | 8 | 1.9085 | +7.0189 | +3,578.79 |
| 2026-W28 | 5 | 29 | 18 | 11 | 4.0966 | +22.7707 | +11,042.35 |
| 2026-W29 | 5 | 21 | 9 | 12 | 1.4295 | +3.6225 | +1,786.03 |
| 2026-W30 | 5 | 36 | 21 | 15 | 3.4314 | +22.7216 | +10,960.71 |
| 2026-W31 | 5 | 24 | 15 | 9 | 2.9157 | +10.7806 | +5,223.28 |
| 2026-W32 | 5 | 19 | 8 | 11 | 1.5223 | +3.6575 | +1,918.25 |
| 2026-W33 | 5 | 14 | 6 | 8 | 1.1804 | +1.1788 | +769.90 |
| 2026-W34 | 5 | 8 | 3 | 5 | 1.9806 | +2.9689 | +1,462.25 |
| 2026-W35 | 4 | 19 | 10 | 9 | 2.4996 | +8.4527 | +4,196.12 |

### Sequential ten-session blocks

| Period | Sessions | Fills | Wins | Losses | Profit Factor | Net Return Points | Net Pnl Rs |
|---|---|---|---|---|---|---|---|
| B1: 2026-05-27..2026-06-10 | 10 | 28 | 14 | 14 | 1.7329 | +9.6697 | +4,872.63 |
| B2: 2026-06-11..2026-06-24 | 10 | 16 | 5 | 11 | 0.8255 | -1.4136 | -605.69 |
| B3: 2026-06-25..2026-07-09 | 10 | 45 | 28 | 17 | 3.1512 | +28.9788 | +14,202.32 |
| B4: 2026-07-10..2026-07-23 | 10 | 56 | 30 | 26 | 2.4990 | +26.1218 | +12,620.02 |
| B5: 2026-07-24..2026-08-06 | 10 | 50 | 26 | 24 | 2.1694 | +17.8247 | +8,873.06 |
| B6: 2026-08-07..2026-08-20 | 10 | 22 | 10 | 12 | 1.6336 | +5.6474 | +2,950.07 |
| B7: 2026-08-21..2026-08-28 | 5 | 20 | 10 | 10 | 2.2409 | +7.8020 | +3,870.82 |

Best rolling ten-session window: **2026-07-21 through 2026-08-03**, Rs +18,608.43, PF 3.6371. Worst: **2026-06-05 through 2026-06-18**, Rs -3,768.06, PF 0.3034.

### Weekday

| Period | Sessions | Fills | Wins | Losses | Profit Factor | Net Return Points | Net Pnl Rs |
|---|---|---|---|---|---|---|---|
| Monday | 13 | 42 | 21 | 21 | 2.3969 | +17.5410 | +8,632.34 |
| Tuesday | 13 | 54 | 28 | 26 | 1.9940 | +19.9727 | +10,190.29 |
| Wednesday | 13 | 49 | 27 | 22 | 2.5414 | +26.2277 | +12,877.05 |
| Thursday | 13 | 51 | 24 | 27 | 1.5884 | +11.9584 | +5,748.46 |
| Friday | 13 | 41 | 23 | 18 | 2.4969 | +18.9311 | +9,335.08 |

All weekdays are profitable here. That is descriptive; weekday filters would still be post-hoc calendar mining.

### Daily activity/range composition diagnostics

| Regime Dimension | Regime | Sessions | Measure Min | Measure Median | Measure Max | Fills | Positive Days | Negative Days | Net Return Points | Net Pnl Rs |
|---|---|---|---|---|---|---|---|---|---|---|
| candidate_activity | LOW | 26 | 2.0000 | 9.0000 | 12.0000 | 50 | 8 | 16 | -2.2312 | -678.84 |
| candidate_activity | MID | 17 | 13.0000 | 15.0000 | 18.0000 | 55 | 13 | 4 | +18.9827 | +9,586.95 |
| candidate_activity | HIGH | 22 | 19.0000 | 27.0000 | 60.0000 | 132 | 16 | 5 | +77.8793 | +37,875.11 |
| five_min_range | LOW | 22 | 0.4935 | 0.5888 | 0.6419 | 66 | 11 | 10 | +23.3716 | +11,621.34 |
| five_min_range | MID | 21 | 0.6539 | 0.6842 | 0.7456 | 87 | 12 | 7 | +48.9679 | +23,932.09 |
| five_min_range | HIGH | 22 | 0.7496 | 0.8421 | 1.2638 | 84 | 14 | 8 | +22.2914 | +11,229.80 |
| long_share | LOW | 23 | 0.0000 | 20.8333 | 33.3333 | 94 | 12 | 10 | +56.1024 | +27,472.97 |
| long_share | MID | 20 | 35.7143 | 47.0588 | 57.1429 | 64 | 12 | 7 | +17.9009 | +8,976.70 |
| long_share | HIGH | 22 | 57.8947 | 72.0779 | 100.0000 | 79 | 13 | 8 | +20.6276 | +10,333.56 |

Buckets are sample-derived terciles. End-of-day fill count is future information and cannot be used as a live filter; only a predeclared causal opening-breadth proxy could be tested.

### Full 65-session day-wise audit

| Session Date | Raw Base 5M Candidates | Post Overlay Selected | One Minute Confirmed | Fills | Wins | Losses | Win Rate Pct | Profit Factor | Net Return Points | Net Pnl Rs | Cumulative Net Pnl Rs | Drawdown Pnl Rs |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 2026-05-27 | 9 | 8 | 5 | 4 | 3 | 1 | 75.00% | 5.9875 | +2.7824 | +1,386.50 | +1,386.50 | +0.00 |
| 2026-05-29 | 24 | 20 | 4 | 3 | 2 | 1 | 66.67% | 10.4738 | +5.2622 | +2,590.87 | +3,977.37 | +0.00 |
| 2026-06-01 | 9 | 9 | 1 | 1 | 0 | 1 | 0.00% | 0.0000 | -1.1520 | -565.65 | +3,411.71 | -565.65 |
| 2026-06-02 | 13 | 13 | 6 | 3 | 1 | 2 | 33.33% | 1.2366 | +0.5451 | +218.61 | +3,630.32 | -347.04 |
| 2026-06-03 | 18 | 17 | 11 | 7 | 6 | 1 | 85.71% | 8.1089 | +8.2162 | +4,024.88 | +7,655.20 | +0.00 |
| 2026-06-04 | 12 | 12 | 2 | 1 | 0 | 1 | 0.00% | 0.0000 | -0.5567 | -277.84 | +7,377.37 | -277.84 |
| 2026-06-05 | 12 | 12 | 4 | 4 | 1 | 3 | 25.00% | 0.2860 | -2.1153 | -1,042.68 | +6,334.69 | -1,320.52 |
| 2026-06-08 | 27 | 27 | 3 | 0 | 0 | 0 | — | — | +0.0000 | +0.00 | +6,334.69 | -1,320.52 |
| 2026-06-09 | 10 | 9 | 4 | 4 | 1 | 3 | 25.00% | 0.2158 | -2.3166 | -973.67 | +5,361.02 | -2,294.19 |
| 2026-06-10 | 8 | 6 | 2 | 1 | 0 | 1 | 0.00% | 0.0000 | -0.9956 | -488.38 | +4,872.63 | -2,782.57 |
| 2026-06-11 | 6 | 6 | 2 | 2 | 1 | 1 | 50.00% | 0.2676 | -0.8440 | -402.17 | +4,470.46 | -3,184.74 |
| 2026-06-12 | 10 | 9 | 2 | 1 | 0 | 1 | 0.00% | 0.0000 | -0.6549 | -327.44 | +4,143.02 | -3,512.19 |
| 2026-06-15 | 15 | 14 | 6 | 4 | 2 | 2 | 50.00% | 1.7060 | +0.7022 | +349.48 | +4,492.50 | -3,162.70 |
| 2026-06-16 | 20 | 18 | 6 | 2 | 0 | 2 | 0.00% | 0.0000 | -0.6280 | -312.49 | +4,180.01 | -3,475.19 |
| 2026-06-17 | 10 | 9 | 1 | 0 | 0 | 0 | — | — | +0.0000 | +0.00 | +4,180.01 | -3,475.19 |
| 2026-06-18 | 3 | 3 | 2 | 1 | 0 | 1 | 0.00% | 0.0000 | -1.1583 | -570.71 | +3,609.31 | -4,045.90 |
| 2026-06-19 | 11 | 11 | 1 | 1 | 1 | 0 | 100.00% | ∞ | +2.3418 | +1,161.03 | +4,770.34 | -2,884.86 |
| 2026-06-22 | 3 | 2 | 0 | 0 | 0 | 0 | — | — | +0.0000 | +0.00 | +4,770.34 | -2,884.86 |
| 2026-06-23 | 15 | 12 | 2 | 1 | 0 | 1 | 0.00% | 0.0000 | -0.5525 | -274.27 | +4,496.08 | -3,159.13 |
| 2026-06-24 | 14 | 14 | 6 | 4 | 1 | 3 | 25.00% | 0.7907 | -0.6198 | -229.13 | +4,266.94 | -3,388.26 |
| 2026-06-25 | 19 | 19 | 7 | 4 | 3 | 1 | 75.00% | 5.9220 | +3.2024 | +1,578.61 | +5,845.55 | -1,809.65 |
| 2026-06-29 | 23 | 22 | 11 | 7 | 5 | 2 | 71.43% | 5.5024 | +5.8985 | +2,872.14 | +8,717.70 | +0.00 |
| 2026-06-30 | 13 | 13 | 3 | 2 | 0 | 2 | 0.00% | 0.0000 | -2.3040 | -1,145.62 | +7,572.08 | -1,145.62 |
| 2026-07-01 | 13 | 12 | 4 | 3 | 2 | 1 | 66.67% | 3.2076 | +2.5413 | +1,221.89 | +8,793.97 | +0.00 |
| 2026-07-02 | 17 | 17 | 5 | 2 | 1 | 1 | 50.00% | 1.8692 | +1.0045 | +675.41 | +9,469.38 | +0.00 |
| 2026-07-03 | 29 | 28 | 6 | 5 | 3 | 2 | 60.00% | 0.9327 | -0.1215 | -45.04 | +9,424.34 | -45.04 |
| 2026-07-06 | 19 | 17 | 3 | 3 | 2 | 1 | 66.67% | 4.8335 | +2.1324 | +1,065.76 | +10,490.10 | +0.00 |
| 2026-07-07 | 39 | 38 | 15 | 8 | 7 | 1 | 87.50% | 18.6128 | +11.4999 | +5,602.68 | +16,092.78 | +0.00 |
| 2026-07-08 | 24 | 24 | 13 | 7 | 5 | 2 | 71.43% | 6.9127 | +7.7076 | +3,627.04 | +19,719.82 | +0.00 |
| 2026-07-09 | 10 | 9 | 4 | 4 | 0 | 4 | 0.00% | 0.0000 | -2.5823 | -1,250.56 | +18,469.26 | -1,250.56 |
| 2026-07-10 | 19 | 17 | 7 | 7 | 4 | 3 | 57.14% | 2.7770 | +4.0132 | +1,997.44 | +20,466.70 | +0.00 |
| 2026-07-13 | 23 | 19 | 6 | 4 | 3 | 1 | 75.00% | 115.1783 | +5.4024 | +2,663.71 | +23,130.41 | +0.00 |
| 2026-07-14 | 33 | 29 | 16 | 5 | 2 | 3 | 40.00% | 0.3467 | -1.5368 | -766.48 | +22,363.93 | -766.48 |
| 2026-07-15 | 26 | 23 | 9 | 6 | 2 | 4 | 33.33% | 1.0467 | +0.1644 | +95.39 | +22,459.31 | -671.09 |
| 2026-07-16 | 27 | 27 | 7 | 4 | 1 | 3 | 25.00% | 0.5107 | -1.2090 | -603.99 | +21,855.33 | -1,275.08 |
| 2026-07-17 | 21 | 18 | 5 | 2 | 1 | 1 | 50.00% | 19.7384 | +0.8016 | +397.40 | +22,252.72 | -877.68 |
| 2026-07-20 | 30 | 28 | 11 | 5 | 2 | 3 | 40.00% | 0.7125 | -0.4791 | -240.19 | +22,012.53 | -1,117.88 |
| 2026-07-21 | 15 | 14 | 5 | 4 | 2 | 2 | 50.00% | 1.1115 | +0.1472 | +133.74 | +22,146.27 | -984.13 |
| 2026-07-22 | 41 | 41 | 17 | 8 | 5 | 3 | 62.50% | 5.7089 | +6.9763 | +3,464.53 | +25,610.81 | +0.00 |
| 2026-07-23 | 65 | 60 | 19 | 11 | 8 | 3 | 72.73% | 6.2297 | +11.8417 | +5,478.47 | +31,089.28 | +0.00 |
| 2026-07-24 | 58 | 56 | 20 | 8 | 4 | 4 | 50.00% | 2.6215 | +4.2355 | +2,124.16 | +33,213.43 | +0.00 |
| 2026-07-27 | 34 | 29 | 14 | 7 | 2 | 5 | 28.57% | 0.4912 | -1.7545 | -919.61 | +32,293.82 | -919.61 |
| 2026-07-28 | 53 | 46 | 26 | 10 | 7 | 3 | 70.00% | 5.9645 | +8.0743 | +3,977.04 | +36,270.86 | +0.00 |
| 2026-07-29 | 17 | 13 | 3 | 1 | 1 | 0 | 100.00% | ∞ | +0.8497 | +413.42 | +36,684.28 | +0.00 |
| 2026-07-30 | 25 | 22 | 6 | 2 | 2 | 0 | 100.00% | ∞ | +1.2706 | +627.44 | +37,311.72 | +0.00 |
| 2026-07-31 | 18 | 16 | 4 | 4 | 3 | 1 | 75.00% | 5.2329 | +2.3406 | +1,125.00 | +38,436.72 | +0.00 |
| 2026-08-03 | 23 | 19 | 8 | 5 | 3 | 2 | 60.00% | 4.5491 | +4.3181 | +2,184.24 | +40,620.96 | +0.00 |
| 2026-08-04 | 11 | 10 | 5 | 2 | 0 | 2 | 0.00% | 0.0000 | -1.3049 | -567.42 | +40,053.53 | -567.43 |
| 2026-08-05 | 15 | 11 | 3 | 2 | 0 | 2 | 0.00% | 0.0000 | -1.4217 | -695.06 | +39,358.47 | -1,262.49 |
| 2026-08-06 | 21 | 19 | 12 | 9 | 4 | 5 | 44.44% | 1.3978 | +1.2171 | +603.87 | +39,962.34 | -658.62 |
| 2026-08-07 | 15 | 14 | 1 | 1 | 1 | 0 | 100.00% | ∞ | +0.8489 | +392.62 | +40,354.96 | -265.99 |
| 2026-08-10 | 9 | 9 | 3 | 3 | 1 | 2 | 33.33% | 2.5545 | +1.7315 | +864.22 | +41,219.18 | +0.00 |
| 2026-08-11 | 12 | 10 | 4 | 3 | 1 | 2 | 33.33% | 0.4655 | -0.9674 | -334.32 | +40,884.86 | -334.32 |
| 2026-08-12 | 17 | 17 | 7 | 4 | 1 | 3 | 25.00% | 0.3448 | -1.6103 | -755.46 | +40,129.40 | -1,089.78 |
| 2026-08-13 | 12 | 11 | 3 | 3 | 2 | 1 | 66.67% | 1.4691 | +0.5414 | +259.84 | +40,389.24 | -829.94 |
| 2026-08-14 | 7 | 5 | 2 | 1 | 1 | 0 | 100.00% | ∞ | +1.4836 | +735.62 | +41,124.86 | -94.32 |
| 2026-08-17 | 10 | 9 | 2 | 1 | 0 | 1 | 0.00% | 0.0000 | -0.6556 | -326.24 | +40,798.62 | -420.56 |
| 2026-08-18 | 8 | 8 | 3 | 2 | 2 | 0 | 100.00% | ∞ | +4.3505 | +2,149.48 | +42,948.10 | +0.00 |
| 2026-08-19 | 6 | 5 | 2 | 2 | 1 | 1 | 50.00% | 187.3564 | +1.6372 | +811.45 | +43,759.55 | +0.00 |
| 2026-08-20 | 14 | 11 | 3 | 2 | 0 | 2 | 0.00% | 0.0000 | -1.7125 | -847.14 | +42,912.41 | -847.14 |
| 2026-08-21 | 13 | 12 | 4 | 1 | 0 | 1 | 0.00% | 0.0000 | -0.6508 | -325.30 | +42,587.11 | -1,172.44 |
| 2026-08-24 | 16 | 14 | 4 | 2 | 1 | 1 | 50.00% | 4.5300 | +1.3971 | +684.48 | +43,271.59 | -487.96 |
| 2026-08-25 | 32 | 28 | 16 | 8 | 5 | 3 | 62.50% | 3.1752 | +4.9659 | +2,483.00 | +45,754.60 | +0.00 |
| 2026-08-27 | 22 | 19 | 8 | 6 | 2 | 4 | 33.33% | 1.3921 | +0.9435 | +477.23 | +46,231.82 | +0.00 |
| 2026-08-28 | 18 | 15 | 3 | 3 | 2 | 1 | 66.67% | 3.0764 | +1.1462 | +551.40 | +46,783.23 | +0.00 |

## 6. V11 lineage and attribution versus frozen V10

| Variant Id | Scenario | Fills | Wins | Losses | Win Rate Pct | Profit Factor | Net Return Points | Net Pnl Rs | Max Daily Drawdown Points |
|---|---|---|---|---|---|---|---|---|---|
| V10_STAGE0_FROZEN_CONTROL | REFERENCE_15_0 | 232 | 116 | 116 | 50.00% | 1.8327 | +73.0544 | +36,312.05 | +9.3513 |
| V11_STAGE3_DETERMINISTIC_GAP_REBASELINE | STRESS_25_5 | 231 | 105 | 126 | 45.45% | 1.2961 | +31.4655 | +16,141.87 | +11.0797 |
| V11_STAGE3_DETERMINISTIC_GAP_REBASELINE | STRESS_20_2 | 232 | 112 | 120 | 48.28% | 1.5923 | +56.5340 | +28,322.68 | +10.6275 |
| V11_STAGE3_DETERMINISTIC_GAP_REBASELINE | REFERENCE_15_0 | 232 | 116 | 116 | 50.00% | 1.8327 | +73.0544 | +36,312.05 | +9.3513 |
| V11_S4_0930_SHORT_ENTRY_NOT_BEFORE_S3 | STRESS_25_5 | 217 | 103 | 114 | 47.47% | 1.4630 | +43.1846 | +21,889.34 | +9.8265 |
| V11_S4_0930_SHORT_ENTRY_NOT_BEFORE_S3 | STRESS_20_2 | 218 | 108 | 110 | 49.54% | 1.7408 | +62.4233 | +31,192.93 | +9.4246 |
| V11_S4_0930_SHORT_ENTRY_NOT_BEFORE_S3 | REFERENCE_15_0 | 218 | 112 | 106 | 51.38% | 2.0140 | +78.1497 | +38,793.50 | +8.1986 |
| V11_S9_SAME_SYMBOL_SAME_SIDE_MAX_2 | STRESS_25_5 | 251 | 116 | 135 | 46.22% | 1.3976 | +45.0980 | +22,734.30 | +12.2875 |
| V11_S9_SAME_SYMBOL_SAME_SIDE_MAX_2 | STRESS_20_2 | 251 | 123 | 128 | 49.00% | 1.7096 | +71.9884 | +35,811.67 | +11.0737 |
| V11_S9_SAME_SYMBOL_SAME_SIDE_MAX_2 | REFERENCE_15_0 | 251 | 127 | 124 | 50.60% | 1.9598 | +89.5356 | +44,301.78 | +9.7200 |
| V11_S10_POST_HOC_TOP2_1436C7D363 | STRESS_25_5 | 237 | 114 | 123 | 48.10% | 1.5657 | +56.8171 | +28,481.76 | +11.0343 |
| V11_S10_POST_HOC_TOP2_1436C7D363 | STRESS_20_2 | 237 | 119 | 118 | 50.21% | 1.8627 | +77.8776 | +38,681.92 | +9.8708 |
| V11_S10_POST_HOC_TOP2_1436C7D363 | REFERENCE_15_0 | 237 | 123 | 114 | 51.90% | 2.1452 | +94.6309 | +46,783.23 | +8.5674 |

Against the frozen V10 control, V11 adds **+21.5764 points / Rs +10,471.18 (+29.53%)**, with +5 net fills and MDD changing from 9.3513 to 8.5674 points.

### Component trade-set attribution

| Scenario | Component Effect | Fills | Wins | Losses | Win Rate Pct | Profit Factor | Net Return Points | Net Pnl Rs |
|---|---|---|---|---|---|---|---|---|
| REFERENCE_15_0 | CAP2_ADDED_VS_DELAY_ONLY | 19 | 11 | 8 | 57.89% | 3.9640 | +16.4811 | +7,989.73 |
| REFERENCE_15_0 | S3_DELAY_REMOVED_VS_CAP2_ONLY | 14 | 4 | 10 | 28.57% | 0.5218 | -5.0953 | -2,481.45 |
| STRESS_20_2 | CAP2_ADDED_VS_DELAY_ONLY | 19 | 11 | 8 | 57.89% | 3.5722 | +15.4543 | +7,488.99 |
| STRESS_20_2 | S3_DELAY_REMOVED_VS_CAP2_ONLY | 14 | 4 | 10 | 28.57% | 0.4731 | -5.8892 | -2,870.25 |
| STRESS_25_5 | CAP2_ADDED_VS_DELAY_ONLY | 20 | 11 | 9 | 55.00% | 2.9032 | +13.6325 | +6,592.42 |
| STRESS_25_5 | S3_DELAY_REMOVED_VS_CAP2_ONLY | 14 | 2 | 12 | 14.29% | 0.0985 | -11.7191 | -5,747.46 |

`CAP2_ADDED` compares Stage 10 with the delay-only component; `S3_DELAY_REMOVED` reports the early 09:30 SHORT trades present under cap-two without the delay but absent in Stage 10. This is exact replay attribution on the development sample, not prospective causal proof.

### Day-wise V11 minus V10

| Session Date | V10 Fills | V11 Fills | V10 Net Return Points | V11 Net Return Points | Delta Net Return Points | V10 Net Pnl Rs | V11 Net Pnl Rs | Delta Net Pnl Rs | Cumulative Delta Net Pnl Rs |
|---|---|---|---|---|---|---|---|---|---|
| 2026-05-27 | 4 | 4 | +2.7824 | +2.7824 | +0.0000 | +1,386.50 | +1,386.50 | +0.00 | +0.00 |
| 2026-05-29 | 3 | 3 | +5.2622 | +5.2622 | +0.0000 | +2,590.87 | +2,590.87 | +0.00 | +0.00 |
| 2026-06-01 | 1 | 1 | -1.1520 | -1.1520 | +0.0000 | -565.65 | -565.65 | +0.00 | +0.00 |
| 2026-06-02 | 4 | 3 | -0.6157 | +0.5451 | +1.1608 | -360.87 | +218.61 | +579.48 | +579.48 |
| 2026-06-03 | 5 | 7 | +5.6647 | +8.2162 | +2.5515 | +2,752.90 | +4,024.88 | +1,271.98 | +1,851.46 |
| 2026-06-04 | 1 | 1 | -0.5567 | -0.5567 | +0.0000 | -277.84 | -277.84 | +0.00 | +1,851.46 |
| 2026-06-05 | 4 | 4 | -2.1153 | -2.1153 | +0.0000 | -1,042.68 | -1,042.68 | +0.00 | +1,851.46 |
| 2026-06-08 | 0 | 0 | +0.0000 | +0.0000 | +0.0000 | +0.00 | +0.00 | +0.00 | +1,851.46 |
| 2026-06-09 | 4 | 4 | -2.3166 | -2.3166 | +0.0000 | -973.67 | -973.67 | +0.00 | +1,851.46 |
| 2026-06-10 | 1 | 1 | -0.9956 | -0.9956 | +0.0000 | -488.38 | -488.38 | +0.00 | +1,851.46 |
| 2026-06-11 | 2 | 2 | -0.8440 | -0.8440 | +0.0000 | -402.17 | -402.17 | +0.00 | +1,851.46 |
| 2026-06-12 | 1 | 1 | -0.6549 | -0.6549 | +0.0000 | -327.44 | -327.44 | +0.00 | +1,851.46 |
| 2026-06-15 | 4 | 4 | +0.7022 | +0.7022 | +0.0000 | +349.48 | +349.48 | +0.00 | +1,851.46 |
| 2026-06-16 | 2 | 2 | -1.4119 | -0.6280 | +0.7839 | -682.73 | -312.49 | +370.25 | +2,221.70 |
| 2026-06-17 | 0 | 0 | +0.0000 | +0.0000 | +0.0000 | +0.00 | +0.00 | +0.00 | +2,221.70 |
| 2026-06-18 | 1 | 1 | -1.1583 | -1.1583 | +0.0000 | -570.71 | -570.71 | +0.00 | +2,221.70 |
| 2026-06-19 | 1 | 1 | +2.3418 | +2.3418 | +0.0000 | +1,161.03 | +1,161.03 | +0.00 | +2,221.70 |
| 2026-06-22 | 0 | 0 | +0.0000 | +0.0000 | +0.0000 | +0.00 | +0.00 | +0.00 | +2,221.70 |
| 2026-06-23 | 1 | 1 | -0.5525 | -0.5525 | +0.0000 | -274.27 | -274.27 | +0.00 | +2,221.70 |
| 2026-06-24 | 3 | 4 | +0.5380 | -0.6198 | -1.1578 | +244.19 | -229.13 | -473.32 | +1,748.38 |
| 2026-06-25 | 5 | 4 | +3.3271 | +3.2024 | -0.1247 | +1,640.71 | +1,578.61 | -62.10 | +1,686.28 |
| 2026-06-29 | 7 | 7 | +4.6595 | +5.8985 | +1.2390 | +2,271.58 | +2,872.14 | +600.56 | +2,286.84 |
| 2026-06-30 | 1 | 2 | -1.1540 | -2.3040 | -1.1500 | -574.76 | -1,145.62 | -570.86 | +1,715.98 |
| 2026-07-01 | 3 | 3 | +2.5413 | +2.5413 | +0.0000 | +1,221.89 | +1,221.89 | +0.00 | +1,715.98 |
| 2026-07-02 | 2 | 2 | +1.0045 | +1.0045 | +0.0000 | +675.41 | +675.41 | +0.00 | +1,715.98 |
| 2026-07-03 | 5 | 5 | -0.1215 | -0.1215 | +0.0000 | -45.04 | -45.04 | +0.00 | +1,715.98 |
| 2026-07-06 | 3 | 3 | +2.1324 | +2.1324 | +0.0000 | +1,065.76 | +1,065.76 | +0.00 | +1,715.98 |
| 2026-07-07 | 7 | 8 | +9.6513 | +11.4999 | +1.8486 | +4,714.73 | +5,602.68 | +887.95 | +2,603.93 |
| 2026-07-08 | 7 | 7 | +7.7076 | +7.7076 | +0.0000 | +3,627.04 | +3,627.04 | +0.00 | +2,603.93 |
| 2026-07-09 | 4 | 4 | -2.5823 | -2.5823 | +0.0000 | -1,250.56 | -1,250.56 | +0.00 | +2,603.93 |
| 2026-07-10 | 6 | 7 | +1.1639 | +4.0132 | +2.8492 | +609.03 | +1,997.44 | +1,388.41 | +3,992.34 |
| 2026-07-13 | 4 | 4 | +5.4024 | +5.4024 | +0.0000 | +2,663.71 | +2,663.71 | +0.00 | +3,992.34 |
| 2026-07-14 | 4 | 5 | -0.8854 | -1.5368 | -0.6514 | -443.77 | -766.48 | -322.71 | +3,669.63 |
| 2026-07-15 | 6 | 6 | +0.1644 | +0.1644 | +0.0000 | +95.39 | +95.39 | +0.00 | +3,669.63 |
| 2026-07-16 | 4 | 4 | -1.7224 | -1.2090 | +0.5134 | -861.24 | -603.99 | +257.25 | +3,926.88 |
| 2026-07-17 | 2 | 2 | +0.8016 | +0.8016 | +0.0000 | +397.40 | +397.40 | +0.00 | +3,926.88 |
| 2026-07-20 | 6 | 5 | -1.6316 | -0.4791 | +1.1526 | -814.48 | -240.19 | +574.29 | +4,501.17 |
| 2026-07-21 | 4 | 4 | +0.1472 | +0.1472 | +0.0000 | +133.74 | +133.74 | +0.00 | +4,501.17 |
| 2026-07-22 | 10 | 8 | +5.9358 | +6.9763 | +1.0404 | +2,979.54 | +3,464.53 | +485.00 | +4,986.17 |
| 2026-07-23 | 10 | 11 | +6.1584 | +11.8417 | +5.6833 | +2,897.10 | +5,478.47 | +2,581.37 | +7,567.54 |
| 2026-07-24 | 9 | 8 | +4.0688 | +4.2355 | +0.1667 | +2,026.46 | +2,124.16 | +97.70 | +7,665.23 |
| 2026-07-27 | 7 | 7 | -1.7545 | -1.7545 | +0.0000 | -919.61 | -919.61 | +0.00 | +7,665.23 |
| 2026-07-28 | 9 | 10 | +8.7274 | +8.0743 | -0.6531 | +4,296.52 | +3,977.04 | -319.48 | +7,345.75 |
| 2026-07-29 | 1 | 1 | +0.8497 | +0.8497 | +0.0000 | +413.42 | +413.42 | +0.00 | +7,345.75 |
| 2026-07-30 | 2 | 2 | +1.2706 | +1.2706 | +0.0000 | +627.44 | +627.44 | +0.00 | +7,345.75 |
| 2026-07-31 | 4 | 4 | +2.3406 | +2.3406 | +0.0000 | +1,125.00 | +1,125.00 | +0.00 | +7,345.75 |
| 2026-08-03 | 4 | 5 | +1.9752 | +4.3181 | +2.3429 | +1,036.24 | +2,184.24 | +1,148.00 | +8,493.75 |
| 2026-08-04 | 2 | 2 | -1.3049 | -1.3049 | +0.0000 | -567.42 | -567.42 | +0.00 | +8,493.75 |
| 2026-08-05 | 2 | 2 | -1.4217 | -1.4217 | +0.0000 | -695.06 | -695.06 | +0.00 | +8,493.75 |
| 2026-08-06 | 8 | 9 | +1.8673 | +1.2171 | -0.6502 | +922.47 | +603.87 | -318.60 | +8,175.15 |
| 2026-08-07 | 1 | 1 | +0.8489 | +0.8489 | +0.0000 | +392.62 | +392.62 | +0.00 | +8,175.15 |
| 2026-08-10 | 3 | 3 | +1.7315 | +1.7315 | +0.0000 | +864.22 | +864.22 | +0.00 | +8,175.15 |
| 2026-08-11 | 3 | 3 | -0.9674 | -0.9674 | +0.0000 | -334.32 | -334.32 | +0.00 | +8,175.15 |
| 2026-08-12 | 5 | 4 | -2.7677 | -1.6103 | +1.1574 | -1,332.44 | -755.46 | +576.98 | +8,752.13 |
| 2026-08-13 | 3 | 3 | +0.5414 | +0.5414 | +0.0000 | +259.84 | +259.84 | +0.00 | +8,752.13 |
| 2026-08-14 | 1 | 1 | +1.4836 | +1.4836 | +0.0000 | +735.62 | +735.62 | +0.00 | +8,752.13 |
| 2026-08-17 | 1 | 1 | -0.6556 | -0.6556 | +0.0000 | -326.24 | -326.24 | +0.00 | +8,752.13 |
| 2026-08-18 | 2 | 2 | +4.3505 | +4.3505 | +0.0000 | +2,149.48 | +2,149.48 | +0.00 | +8,752.13 |
| 2026-08-19 | 2 | 2 | +1.6372 | +1.6372 | +0.0000 | +811.45 | +811.45 | +0.00 | +8,752.13 |
| 2026-08-20 | 2 | 2 | -1.7125 | -1.7125 | +0.0000 | -847.14 | -847.14 | +0.00 | +8,752.13 |
| 2026-08-21 | 1 | 1 | -0.6508 | -0.6508 | +0.0000 | -325.30 | -325.30 | +0.00 | +8,752.13 |
| 2026-08-24 | 3 | 2 | +1.1506 | +1.3971 | +0.2464 | +577.16 | +684.48 | +107.32 | +8,859.46 |
| 2026-08-25 | 6 | 8 | +1.7384 | +4.9659 | +3.2275 | +871.28 | +2,483.00 | +1,611.72 | +10,471.18 |
| 2026-08-27 | 6 | 6 | +0.9435 | +0.9435 | +0.0000 | +477.23 | +477.23 | +0.00 | +10,471.18 |
| 2026-08-28 | 3 | 3 | +1.1462 | +1.1462 | +0.0000 | +551.40 | +551.40 | +0.00 | +10,471.18 |

### Development gates

| Variant Id | Stage Id | Development Gate Passed | Gate Classification | Reference Net Ratio Vs Baseline | Reference Mdd Ratio Vs Baseline | Promotion Gate Passed | Promotion Blocker |
|---|---|---|---|---|---|---|---|
| V11_S10_POST_HOC_TOP2_1436C7D363 | STAGE_10_POST_HOC_COMBINATION | Yes | PASS_IMPROVEMENT | 1.2953 | 0.9162 | No | NO_UNTOUCHED_PROSPECTIVE_SAMPLE_AND_EXECUTION_DATA_INVALID |
| V11_S9_SAME_SYMBOL_SAME_SIDE_MAX_2 | STAGE_09_PORTFOLIO | Yes | PASS_IMPROVEMENT | 1.2256 | 1.0394 | No | NO_UNTOUCHED_PROSPECTIVE_SAMPLE_AND_EXECUTION_DATA_INVALID |
| V11_S4_0930_SHORT_ENTRY_NOT_BEFORE_S3 | STAGE_04_ENTRY_TIMING | Yes | PASS_IMPROVEMENT | 1.0697 | 0.8767 | No | NO_UNTOUCHED_PROSPECTIVE_SAMPLE_AND_EXECUTION_DATA_INVALID |
| V11_STAGE3_DETERMINISTIC_GAP_REBASELINE | STAGE_03_REBASELINE | Yes | DETERMINISTIC_COMPARISON_BASELINE | 1.0000 | 1.0000 | No | NO_UNTOUCHED_PROSPECTIVE_SAMPLE_AND_EXECUTION_DATA_INVALID |
| V10_STAGE0_FROZEN_CONTROL | STAGE_00_FROZEN_V10 | No | FROZEN_LEGACY_CONTROL_REFERENCE_ONLY | 1.0000 | 1.0000 | No | FROZEN_REFERENCE_ONLY_ARCHIVAL_CONTROL |

## 7. Setup, side, slot, picker and rank

### Setup funnel and contribution

| Setup Id | Max Entries | Picker | Raw Base 5M Candidates | Post Overlay Selected | One Minute Confirmed | Confirmation Rate Pct | Fills | Fill Rate Pct | Wins | Losses | Win Rate Pct | Profit Factor | Net Return Points | Net Pnl Rs |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 09:25_LONG | 4 | max_move | 122 | 122 | 78 | 63.93% | 61 | 50.00% | 32 | 29 | 52.46% | 1.6844 | +11.0095 | +5,289.78 |
| 09:25_SHORT | 4 | max_move | 261 | 261 | 114 | 43.68% | 62 | 23.75% | 28 | 34 | 45.16% | 2.0559 | +22.3665 | +10,997.77 |
| 09:30_LONG | 1 | max_move | 65 | 65 | 17 | 26.15% | 11 | 16.92% | 5 | 6 | 45.45% | 1.6174 | +3.5120 | +1,709.49 |
| 09:30_SHORT | 4 | max_volume | 101 | 101 | 41 | 40.59% | 19 | 18.81% | 12 | 7 | 63.16% | 3.3855 | +14.7457 | +7,157.02 |
| 09:35_LONG | 1 | max_liquidity | 248 | 171 | 35 | 20.47% | 17 | 9.94% | 9 | 8 | 52.94% | 1.5052 | +3.7417 | +1,914.06 |
| 09:35_SHORT | 2 | max_liquidity | 36 | 36 | 15 | 41.67% | 10 | 27.78% | 6 | 4 | 60.00% | 3.1215 | +9.7864 | +4,914.43 |
| 09:40_LONG | 1 | max_liquidity | 106 | 76 | 25 | 32.89% | 18 | 23.68% | 9 | 9 | 50.00% | 3.1108 | +12.3896 | +5,942.03 |
| 09:40_SHORT | 1 | max_move | 178 | 178 | 50 | 28.09% | 18 | 10.11% | 9 | 9 | 50.00% | 1.5059 | +4.8307 | +2,670.63 |
| 09:45_LONG | 1 | max_move | 36 | 36 | 13 | 36.11% | 9 | 25.00% | 6 | 3 | 66.67% | 4.7245 | +8.7639 | +4,366.10 |
| 09:45_SHORT | 1 | max_volume | 88 | 88 | 21 | 23.86% | 12 | 13.64% | 7 | 5 | 58.33% | 1.9401 | +3.4848 | +1,821.92 |

![Setup contribution](report_v11_assets/setup_net_pnl.png)

### Setup survival under harsh costs

| Setup Id | Fills Reference | Wins | Losses | Win Rate Pct | Profit Factor Reference | Net Return Points Reference | Profit Factor Harsh | Net Return Points Harsh | Net Pnl Rs Harsh |
|---|---|---|---|---|---|---|---|---|---|
| 09:25_LONG | 61 | 32 | 29 | 52.46% | 1.6844 | +11.0095 | 1.1781 | +3.4969 | +1,601.07 |
| 09:25_SHORT | 62 | 28 | 34 | 45.16% | 2.0559 | +22.3665 | 1.2756 | +7.6596 | +3,845.71 |
| 09:30_LONG | 11 | 5 | 6 | 45.45% | 1.6174 | +3.5120 | 1.3437 | +2.1976 | +1,056.49 |
| 09:30_SHORT | 19 | 12 | 7 | 63.16% | 3.3855 | +14.7457 | 2.7210 | +12.2085 | +5,938.20 |
| 09:35_LONG | 17 | 9 | 8 | 52.94% | 1.5052 | +3.7417 | 1.1360 | +1.1826 | +654.86 |
| 09:35_SHORT | 10 | 6 | 4 | 60.00% | 3.1215 | +9.7864 | 2.7330 | +8.6765 | +4,364.92 |
| 09:40_LONG | 18 | 9 | 9 | 50.00% | 3.1108 | +12.3896 | 2.5265 | +10.4845 | +5,018.97 |
| 09:40_SHORT | 18 | 9 | 9 | 50.00% | 1.5059 | +4.8307 | 1.2535 | +2.6619 | +1,627.05 |
| 09:45_LONG | 9 | 6 | 3 | 66.67% | 4.7245 | +8.7639 | 3.8410 | +7.7004 | +3,851.92 |
| 09:45_SHORT | 12 | 7 | 5 | 58.33% | 1.9401 | +3.4848 | 1.0957 | +0.5487 | +522.56 |

All ten setups stay net-positive under harsh costs. Deleting a setup based on this same sample would discard diversification without out-of-sample evidence.

### Side

| Side | Post Overlay Selected | Fills | Wins | Losses | Win Rate Pct | Profit Factor | Net Return Points | Net Pnl Rs |
|---|---|---|---|---|---|---|---|---|
| LONG | 470 | 116 | 61 | 55 | 52.59% | 2.0538 | +39.4167 | +19,221.46 |
| SHORT | 664 | 121 | 62 | 59 | 51.24% | 2.2207 | +55.2142 | +27,561.77 |

### Signal slot

| Signal End | Post Overlay Selected | One Minute Confirmed | Fills | Wins | Losses | Profit Factor | Net Return Points | Net Pnl Rs |
|---|---|---|---|---|---|---|---|---|
| 09:25 | 383 | 192 | 123 | 60 | 63 | 1.8956 | +33.3760 | +16,287.55 |
| 09:30 | 166 | 58 | 30 | 17 | 13 | 2.5382 | +18.2576 | +8,866.51 |
| 09:35 | 207 | 50 | 27 | 15 | 12 | 2.1255 | +13.5281 | +6,828.50 |
| 09:40 | 254 | 75 | 36 | 18 | 18 | 2.1169 | +17.2203 | +8,612.65 |
| 09:45 | 124 | 34 | 21 | 13 | 8 | 3.0212 | +12.2488 | +6,188.02 |

### Picker

| Picker | Post Overlay Selected | Fills | Wins | Losses | Profit Factor | Net Return Points | Net Pnl Rs |
|---|---|---|---|---|---|---|---|
| max_move | 662 | 161 | 80 | 81 | 1.9202 | +50.4827 | +25,033.76 |
| max_liquidity | 283 | 45 | 24 | 21 | 2.4488 | +25.9177 | +12,770.52 |
| max_volume | 189 | 31 | 19 | 12 | 2.8437 | +18.2305 | +8,978.94 |

### Frozen rank

| Rank Bucket | Selected | Confirmed | Fills | Wins | Losses | Win Rate Pct | Profit Factor | Net Return Points | Net Pnl Rs |
|---|---|---|---|---|---|---|---|---|---|
| 1 | 398 | 145 | 109 | 57 | 52 | 52.29% | 2.0952 | +42.2686 | +20,854.50 |
| 2 | 243 | 82 | 47 | 22 | 25 | 46.81% | 2.4230 | +23.8980 | +11,931.42 |
| 3 | 148 | 55 | 29 | 11 | 18 | 37.93% | 0.6600 | -5.0506 | -2,407.32 |
| 4 | 104 | 41 | 24 | 12 | 12 | 50.00% | 2.6774 | +12.7248 | +6,249.05 |
| 5 | 69 | 26 | 14 | 10 | 4 | 71.43% | 4.6627 | +10.7921 | +5,374.92 |
| 6+ | 172 | 60 | 14 | 11 | 3 | 78.57% | 6.3776 | +9.9979 | +4,780.66 |

Rank performance is non-monotonic. In particular, a weak historical rank 3 next to profitable ranks 4–6 is a warning against an exact-rank blacklist; a causal relative-score or breadth hypothesis would be safer to test.

## 8. Stage 10 mechanism diagnostics

### 09:30 SHORT S+3 earliest-fill rule

| Early Touch Group | Selected | Confirmed | Early Fill Checks Skipped | Fills | Wins | Losses | Win Rate Pct | Profit Factor | Net Return Points | Net Pnl Rs |
|---|---|---|---|---|---|---|---|---|---|---|
| NO_EARLY_TOUCH | 83 | 23 | 6 | 16 | 10 | 6 | 62.50% | 3.8401 | +14.2825 | +6,920.56 |
| EARLY_TOUCH_OBSERVED | 18 | 18 | 18 | 3 | 2 | 1 | 66.67% | 1.4020 | +0.4632 | +236.47 |

Within 09:30 SHORT, 41 candidates confirmed, 24 recorded skipped early-fill checks, and 18 showed an early touch. The lineage table—not this outcome-conditioned subgroup—is the proper component comparison.

### Same-symbol/same-side maximum two

| Portfolio View | Fills | Wins | Losses | Win Rate Pct | Profit Factor | Net Return Points | Net Pnl Rs |
|---|---|---|---|---|---|---|---|
| ACTUAL_CAP2_LEDGER | 237 | 123 | 114 | 51.90% | 2.1452 | +94.6309 | +46,783.23 |
| UNCONSTRAINED_CANDIDATE_OUTCOMES | 240 | 123 | 117 | 51.25% | 2.1010 | +92.8948 | +45,932.18 |
| Candidate Id | Session Date | Setup Id | Side | Symbol | Portfolio Reject Reason | Unconstrained Status | Unconstrained Net Return Pct | Unconstrained Net Pnl Rs |
|---|---|---|---|---|---|---|---|---|
| 2026-06-03\|09:40_SHORT\|LTM | 2026-06-03 | 09:40_SHORT | SHORT | LTM | DUPLICATE_SYMBOL_SAME_SIDE_LIMIT_2 | SQUARE_OFF | -0.07% | -32.28 |
| 2026-06-03\|09:45_SHORT\|LTM | 2026-06-03 | 09:45_SHORT | SHORT | LTM | DUPLICATE_SYMBOL_SAME_SIDE_LIMIT_2 | SQUARE_OFF | -0.52% | -250.35 |
| 2026-08-06\|09:45_LONG\|SHRIRAMFIN | 2026-08-06 | 09:45_LONG | LONG | SHRIRAMFIN | DUPLICATE_SYMBOL_SAME_SIDE_LIMIT_2 | STOPPED | -1.15% | -568.41 |

Only three would-be third same-side/symbol reservations were rejected; all three stored unconstrained paths lost here. N=3 is far too small to justify a cap-three conclusion or further cap tuning.

### Portfolio exposure and overlap

| Modeled Capital Rs | Margin Per Reservation Rs | Maximum Reservations | Pending Reserves Margin | Target Exposure Per Entry Rs | Same Symbol Same Side Limit | Same Symbol Opposite Side Prohibited | Peak Open Positions | Peak Deployed Notional Rs | Peak Deployed Timestamp | Peak Notional To Modeled Capital | Maximum Active At Reservation | Maximum Reserved Margin Rs | Mean Time Weighted Open Positions | Mean Time Weighted Deployed Notional Rs | Mean Trade Notional Rs | Minimum Trade Notional Rs | Maximum Trade Notional Rs |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| +120,000.00 | +10,000.00 | 12 | Yes | +50,000.00 | 2 | Yes | 8 | +394,734.05 | 2026-07-28 09:42:00+05:30 | 3.2895 | 9.0000 | +90,000.00 | 1.5064 | +73,171.81 | +48,433.33 | +32,005.00 | +49,998.50 |

The exposure figures are internally consistent with the cash-equivalent model, but are not executable futures leverage: dated lots, ticks, margins, spread and front-month rollover are absent.

### Strong-identity 2 bps gap guard

| Gap Guard Path | Candidates | Fills | Median Adverse Gap Bps | Wins | Losses | Win Rate Pct | Profit Factor | Net Return Points | Net Pnl Rs |
|---|---|---|---|---|---|---|---|---|---|
| NO_GAP_OBSERVED | 1,086 | 213 | — | 111 | 102 | 52.11% | 2.1529 | +84.3944 | +41,689.84 |
| GAP_ACCEPTED | 24 | 24 | 0.6588 | 12 | 12 | 50.00% | 2.0850 | +10.2365 | +5,093.38 |
| GAP_REJECTED | 24 | 0 | 9.2073 | 0 | 0 | — | — | +0.0000 | +0.00 |

### Observed adverse-gap bins

| Adverse Gap Bin | Observed | Guard Rejections | Fills | Wins | Losses | Profit Factor | Net Return Points | Net Pnl Rs |
|---|---|---|---|---|---|---|---|---|
| 0–1 | 8 | 0 | 8 | 5 | 3 | 3.0373 | +6.0247 | +2,975.36 |
| <=0 | 9 | 0 | 9 | 4 | 5 | 1.1434 | +0.5831 | +354.71 |
| 5+ | 20 | 20 | 0 | 0 | 0 | — | +0.0000 | +0.00 |
| 2–3 | 2 | 2 | 0 | 0 | 0 | — | +0.0000 | +0.00 |
| 3–5 | 2 | 2 | 0 | 0 | 0 | — | +0.0000 | +0.00 |
| 1–2 | 7 | 0 | 7 | 3 | 4 | 2.5054 | +3.6287 | +1,763.31 |

Accepted 1–2 bps gap fills remain profitable in this sample. Tightening to 1 bps would remove observed winners as well as losers. Rejected gaps have no post-rejection execution counterfactual, so the report makes no claim that the guard 'avoided losses.'

## 9. Five-minute and confirmation-indicator study

### Cohort distributions

| Indicator | Cohort | Observations | Mean | Median | P25 | P75 |
|---|---|---|---|---|---|---|
| directional_move_pct | ALL_SELECTED | 1,134 | 0.5705 | 0.4555 | 0.3173 | 0.7041 |
| directional_move_pct | CONFIRMED | 409 | 0.5803 | 0.4985 | 0.3281 | 0.7155 |
| directional_move_pct | FILLED | 237 | 0.6233 | 0.5540 | 0.3645 | 0.7739 |
| directional_move_pct | WINNERS | 123 | 0.5913 | 0.5203 | 0.3261 | 0.7488 |
| directional_move_pct | LOSERS | 114 | 0.6577 | 0.5809 | 0.4100 | 0.8281 |
| oi_change_pct | ALL_SELECTED | 1,134 | 5.5310 | 0.8743 | 0.3526 | 1.9223 |
| oi_change_pct | CONFIRMED | 409 | 11.2594 | 0.8555 | 0.3685 | 1.9231 |
| oi_change_pct | FILLED | 237 | 17.4305 | 0.9120 | 0.3624 | 2.2727 |
| oi_change_pct | WINNERS | 123 | 3.1083 | 0.9120 | 0.3110 | 2.5814 |
| oi_change_pct | LOSERS | 114 | 32.8834 | 0.9060 | 0.4813 | 2.0017 |
| volume_ratio | ALL_SELECTED | 1,134 | 3.1532 | 2.4119 | 1.6271 | 3.9063 |
| volume_ratio | CONFIRMED | 409 | 3.2984 | 2.6039 | 1.7729 | 4.0045 |
| volume_ratio | FILLED | 237 | 3.5533 | 2.8638 | 1.8961 | 4.4407 |
| volume_ratio | WINNERS | 123 | 3.3675 | 2.7889 | 1.8282 | 4.3153 |
| volume_ratio | LOSERS | 114 | 3.7537 | 2.9087 | 1.9696 | 4.5467 |
| traded_value_cr | ALL_SELECTED | 1,134 | 25.7196 | 13.5845 | 6.1733 | 29.0457 |
| traded_value_cr | CONFIRMED | 409 | 25.8344 | 14.8644 | 7.0826 | 29.7559 |
| traded_value_cr | FILLED | 237 | 30.5452 | 17.5325 | 8.4270 | 33.5171 |
| traded_value_cr | WINNERS | 123 | 34.1194 | 17.5325 | 8.2518 | 37.7224 |
| traded_value_cr | LOSERS | 114 | 26.6887 | 17.5121 | 8.5459 | 31.9284 |
| five_min_range_pct | ALL_SELECTED | 1,134 | 0.8039 | 0.6912 | 0.5193 | 0.9476 |
| five_min_range_pct | CONFIRMED | 409 | 0.8252 | 0.7401 | 0.5368 | 0.9972 |
| five_min_range_pct | FILLED | 237 | 0.8812 | 0.7830 | 0.5941 | 1.0354 |
| five_min_range_pct | WINNERS | 123 | 0.8561 | 0.7588 | 0.5926 | 0.9972 |
| five_min_range_pct | LOSERS | 114 | 0.9082 | 0.8065 | 0.6028 | 1.1306 |
| five_min_body_ratio | ALL_SELECTED | 1,134 | 0.6936 | 0.7086 | 0.5625 | 0.8368 |
| five_min_body_ratio | CONFIRMED | 409 | 0.6887 | 0.7119 | 0.5758 | 0.8333 |
| five_min_body_ratio | FILLED | 237 | 0.6886 | 0.7187 | 0.5758 | 0.8361 |
| five_min_body_ratio | WINNERS | 123 | 0.6792 | 0.7121 | 0.5468 | 0.8405 |
| five_min_body_ratio | LOSERS | 114 | 0.6988 | 0.7207 | 0.6007 | 0.8208 |
| five_min_adverse_wick_ratio | ALL_SELECTED | 1,134 | 0.1828 | 0.1579 | 0.0692 | 0.2674 |
| five_min_adverse_wick_ratio | CONFIRMED | 409 | 0.1872 | 0.1667 | 0.0701 | 0.2699 |
| five_min_adverse_wick_ratio | FILLED | 237 | 0.1912 | 0.1667 | 0.0752 | 0.2727 |
| five_min_adverse_wick_ratio | WINNERS | 123 | 0.1991 | 0.1600 | 0.0727 | 0.3086 |
| five_min_adverse_wick_ratio | LOSERS | 114 | 0.1827 | 0.1782 | 0.0766 | 0.2441 |
| five_min_directional_close_location | ALL_SELECTED | 1,134 | 0.8172 | 0.8421 | 0.7326 | 0.9308 |
| five_min_directional_close_location | CONFIRMED | 409 | 0.8128 | 0.8333 | 0.7301 | 0.9299 |
| five_min_directional_close_location | FILLED | 237 | 0.8088 | 0.8333 | 0.7273 | 0.9248 |
| five_min_directional_close_location | WINNERS | 123 | 0.8009 | 0.8400 | 0.6914 | 0.9273 |
| five_min_directional_close_location | LOSERS | 114 | 0.8173 | 0.8218 | 0.7559 | 0.9234 |
| ema_fast_gap_pct | ALL_SELECTED | 1,134 | 0.4697 | 0.3391 | 0.1976 | 0.5818 |
| ema_fast_gap_pct | CONFIRMED | 409 | 0.4598 | 0.3308 | 0.1928 | 0.5818 |
| ema_fast_gap_pct | FILLED | 237 | 0.4745 | 0.3452 | 0.2065 | 0.5975 |
| ema_fast_gap_pct | WINNERS | 123 | 0.5086 | 0.3452 | 0.1765 | 0.6028 |
| ema_fast_gap_pct | LOSERS | 114 | 0.4378 | 0.3447 | 0.2214 | 0.5950 |
| ema_slow_gap_pct | ALL_SELECTED | 1,134 | 0.4204 | 0.3116 | 0.1591 | 0.5374 |
| ema_slow_gap_pct | CONFIRMED | 409 | 0.4066 | 0.3069 | 0.1520 | 0.5259 |
| ema_slow_gap_pct | FILLED | 237 | 0.4219 | 0.3285 | 0.1520 | 0.5441 |
| ema_slow_gap_pct | WINNERS | 123 | 0.4378 | 0.2832 | 0.1424 | 0.5194 |
| ema_slow_gap_pct | LOSERS | 114 | 0.4047 | 0.3394 | 0.1796 | 0.5753 |
| ema_total_gap_pct | ALL_SELECTED | 1,134 | 0.8900 | 0.6526 | 0.3865 | 1.1018 |
| ema_total_gap_pct | CONFIRMED | 409 | 0.8664 | 0.6519 | 0.3739 | 1.0879 |
| ema_total_gap_pct | FILLED | 237 | 0.8964 | 0.7319 | 0.3928 | 1.1169 |
| ema_total_gap_pct | WINNERS | 123 | 0.9463 | 0.6864 | 0.3478 | 1.1196 |
| ema_total_gap_pct | LOSERS | 114 | 0.8425 | 0.7559 | 0.4104 | 1.1158 |
| confirmation_body_ratio | ALL_SELECTED | 409 | 0.7203 | 0.7273 | 0.6267 | 0.8334 |
| confirmation_body_ratio | CONFIRMED | 409 | 0.7203 | 0.7273 | 0.6267 | 0.8334 |
| confirmation_body_ratio | FILLED | 237 | 0.7211 | 0.7369 | 0.6333 | 0.8356 |
| confirmation_body_ratio | WINNERS | 123 | 0.7136 | 0.7391 | 0.6129 | 0.8352 |
| confirmation_body_ratio | LOSERS | 114 | 0.7292 | 0.7351 | 0.6523 | 0.8435 |
| confirmation_adverse_wick_ratio | ALL_SELECTED | 409 | 0.1502 | 0.1375 | 0.0000 | 0.2393 |
| confirmation_adverse_wick_ratio | CONFIRMED | 409 | 0.1502 | 0.1375 | 0.0000 | 0.2393 |
| confirmation_adverse_wick_ratio | FILLED | 237 | 0.1466 | 0.1374 | 0.0000 | 0.2308 |
| confirmation_adverse_wick_ratio | WINNERS | 123 | 0.1530 | 0.1383 | 0.0266 | 0.2361 |
| confirmation_adverse_wick_ratio | LOSERS | 114 | 0.1398 | 0.1348 | 0.0000 | 0.2237 |
| confirmation_close_location | ALL_SELECTED | 409 | 0.8498 | 0.8625 | 0.7607 | 1.0000 |
| confirmation_close_location | CONFIRMED | 409 | 0.8498 | 0.8625 | 0.7607 | 1.0000 |
| confirmation_close_location | FILLED | 237 | 0.8534 | 0.8626 | 0.7692 | 1.0000 |
| confirmation_close_location | WINNERS | 123 | 0.8470 | 0.8617 | 0.7639 | 0.9734 |
| confirmation_close_location | LOSERS | 114 | 0.8602 | 0.8652 | 0.7763 | 1.0000 |
| trigger_distance_c5_bps | ALL_SELECTED | 409 | 26.2827 | 22.2020 | 13.9444 | 34.6333 |
| trigger_distance_c5_bps | CONFIRMED | 409 | 26.2827 | 22.2020 | 13.9444 | 34.6333 |
| trigger_distance_c5_bps | FILLED | 237 | 29.0548 | 24.4630 | 15.8063 | 37.8792 |
| trigger_distance_c5_bps | WINNERS | 123 | 29.1239 | 22.5020 | 14.3539 | 38.5841 |
| trigger_distance_c5_bps | LOSERS | 114 | 28.9803 | 25.3341 | 16.5282 | 35.9523 |

### Winner-versus-loser medians

| Indicator | Winner Observations | Loser Observations | Winner Median | Loser Median | Median Delta | Winner Mean | Loser Mean |
|---|---|---|---|---|---|---|---|
| directional_move_pct | 123 | 114 | 0.5203 | 0.5809 | -0.0606 | 0.5913 | 0.6577 |
| directional_five_min_body_pct | 123 | 114 | 0.4961 | 0.5718 | -0.0757 | 0.5771 | 0.6448 |
| oi_change_pct | 123 | 114 | 0.9120 | 0.9060 | 0.0059 | 3.1083 | 32.8834 |
| volume_ratio | 123 | 114 | 2.7889 | 2.9087 | -0.1198 | 3.3675 | 3.7537 |
| traded_value_cr | 123 | 114 | 17.5325 | 17.5121 | 0.0204 | 34.1194 | 26.6887 |
| five_min_range_pct | 123 | 114 | 0.7588 | 0.8065 | -0.0477 | 0.8561 | 0.9082 |
| five_min_body_ratio | 123 | 114 | 0.7121 | 0.7207 | -0.0086 | 0.6792 | 0.6988 |
| five_min_adverse_wick_ratio | 123 | 114 | 0.1600 | 0.1782 | -0.0182 | 0.1991 | 0.1827 |
| five_min_directional_close_location | 123 | 114 | 0.8400 | 0.8218 | 0.0182 | 0.8009 | 0.8173 |
| ema_fast_gap_pct | 123 | 114 | 0.3452 | 0.3447 | 0.0005 | 0.5086 | 0.4378 |
| ema_slow_gap_pct | 123 | 114 | 0.2832 | 0.3394 | -0.0562 | 0.4378 | 0.4047 |
| ema_total_gap_pct | 123 | 114 | 0.6864 | 0.7559 | -0.0694 | 0.9463 | 0.8425 |
| directional_close_ema9_pct | 123 | 114 | 1.0420 | 1.2067 | -0.1647 | 1.2855 | 1.2954 |
| confirmation_volume_ratio | 123 | 114 | 1.0419 | 0.8730 | 0.1689 | 1.2599 | 1.1606 |
| confirmation_body_ratio | 123 | 114 | 0.7391 | 0.7351 | 0.0041 | 0.7136 | 0.7292 |
| confirmation_adverse_wick_ratio | 123 | 114 | 0.1383 | 0.1348 | 0.0035 | 0.1530 | 0.1398 |
| confirmation_close_location | 123 | 114 | 0.8617 | 0.8652 | -0.0035 | 0.8470 | 0.8602 |
| trigger_distance_c5_bps | 123 | 114 | 22.5020 | 25.3341 | -2.8321 | 29.1239 | 28.9803 |
| confirmation_minute | 123 | 114 | 1.0000 | 1.0000 | 0.0000 | 1.3496 | 1.3333 |
| entry_minute | 123 | 114 | 2.0000 | 2.0000 | 0.0000 | 2.6098 | 2.6140 |

### Multiple-test-corrected binary comparisons

| Comparison | Indicator | Positive Observations | Negative Observations | Positive Median | Negative Median | Auc Positive Higher | P Value Two Sided | Bh Q Value |
|---|---|---|---|---|---|---|---|---|
| CONFIRMED_VS_NOT_CONFIRMED | volume_ratio | 409 | 725 | 2.6039 | 2.2967 | 0.5534 | 0.0028 | 0.0363 |
| CONFIRMED_VS_NOT_CONFIRMED | five_min_range_pct | 409 | 725 | 0.7401 | 0.6644 | 0.5451 | 0.0115 | 0.0749 |
| CONFIRMED_VS_NOT_CONFIRMED | traded_value_cr | 409 | 725 | 14.8644 | 13.2039 | 0.5356 | 0.0465 | 0.1554 |
| CONFIRMED_VS_NOT_CONFIRMED | directional_move_pct | 409 | 725 | 0.4985 | 0.4419 | 0.5353 | 0.0478 | 0.1554 |
| CONFIRMED_VS_NOT_CONFIRMED | directional_five_min_body_pct | 409 | 725 | 0.4926 | 0.4382 | 0.5324 | 0.0694 | 0.1804 |
| CONFIRMED_VS_NOT_CONFIRMED | directional_close_ema9_pct | 409 | 725 | 1.0573 | 0.9522 | 0.5274 | 0.1253 | 0.2716 |
| CONFIRMED_VS_NOT_CONFIRMED | ema_slow_gap_pct | 409 | 725 | 0.3069 | 0.3142 | 0.4891 | 0.5424 | 0.7200 |
| CONFIRMED_VS_NOT_CONFIRMED | five_min_adverse_wick_ratio | 409 | 725 | 0.1667 | 0.1538 | 0.5100 | 0.5736 | 0.7200 |
| CONFIRMED_VS_NOT_CONFIRMED | five_min_directional_close_location | 409 | 725 | 0.8333 | 0.8462 | 0.4900 | 0.5736 | 0.7200 |
| CONFIRMED_VS_NOT_CONFIRMED | five_min_body_ratio | 409 | 725 | 0.7119 | 0.7067 | 0.4903 | 0.5869 | 0.7200 |
| CONFIRMED_VS_NOT_CONFIRMED | ema_total_gap_pct | 409 | 725 | 0.6519 | 0.6532 | 0.4915 | 0.6324 | 0.7200 |
| CONFIRMED_VS_NOT_CONFIRMED | ema_fast_gap_pct | 409 | 725 | 0.3308 | 0.3445 | 0.4924 | 0.6701 | 0.7200 |
| CONFIRMED_VS_NOT_CONFIRMED | oi_change_pct | 409 | 725 | 0.8555 | 0.8811 | 0.5064 | 0.7200 | 0.7200 |
| FILLED_VS_NOT_FILLED | five_min_range_pct | 237 | 897 | 0.7830 | 0.6613 | 0.5961 | 0.0000 | 0.0001 |
| FILLED_VS_NOT_FILLED | traded_value_cr | 237 | 897 | 17.5325 | 13.0902 | 0.5880 | 0.0000 | 0.0002 |
| FILLED_VS_NOT_FILLED | directional_move_pct | 237 | 897 | 0.5540 | 0.4406 | 0.5822 | 0.0001 | 0.0004 |
| FILLED_VS_NOT_FILLED | volume_ratio | 237 | 897 | 2.8638 | 2.2959 | 0.5817 | 0.0001 | 0.0004 |
| FILLED_VS_NOT_FILLED | directional_five_min_body_pct | 237 | 897 | 0.5482 | 0.4386 | 0.5725 | 0.0006 | 0.0018 |
| FILLED_VS_NOT_FILLED | directional_close_ema9_pct | 237 | 897 | 1.0741 | 0.9650 | 0.5503 | 0.0171 | 0.0427 |
| FILLED_VS_NOT_FILLED | confirmation_volume_ratio | 237 | 172 | 0.9501 | 0.8417 | 0.5532 | 0.0661 | 0.1417 |
| FILLED_VS_NOT_FILLED | oi_change_pct | 237 | 897 | 0.9120 | 0.8713 | 0.5214 | 0.3114 | 0.5838 |
| FILLED_VS_NOT_FILLED | ema_total_gap_pct | 237 | 897 | 0.7319 | 0.6398 | 0.5150 | 0.4771 | 0.7196 |
| FILLED_VS_NOT_FILLED | five_min_adverse_wick_ratio | 237 | 897 | 0.1667 | 0.1551 | 0.5128 | 0.5445 | 0.7196 |
| FILLED_VS_NOT_FILLED | five_min_directional_close_location | 237 | 897 | 0.8333 | 0.8449 | 0.4872 | 0.5445 | 0.7196 |
| FILLED_VS_NOT_FILLED | ema_fast_gap_pct | 237 | 897 | 0.3452 | 0.3379 | 0.5108 | 0.6093 | 0.7196 |
| FILLED_VS_NOT_FILLED | confirmation_body_ratio | 237 | 172 | 0.7369 | 0.7212 | 0.5142 | 0.6237 | 0.7196 |
| FILLED_VS_NOT_FILLED | ema_slow_gap_pct | 237 | 897 | 0.3285 | 0.3105 | 0.5072 | 0.7335 | 0.7858 |
| FILLED_VS_NOT_FILLED | five_min_body_ratio | 237 | 897 | 0.7187 | 0.7059 | 0.4983 | 0.9372 | 0.9372 |
| WINNER_VS_LOSER | directional_move_pct | 123 | 114 | 0.5203 | 0.5809 | 0.4306 | 0.0652 | 0.7639 |
| WINNER_VS_LOSER | directional_five_min_body_pct | 123 | 114 | 0.4961 | 0.5718 | 0.4333 | 0.0764 | 0.7639 |
| WINNER_VS_LOSER | five_min_range_pct | 123 | 114 | 0.7588 | 0.8065 | 0.4558 | 0.2401 | 0.7817 |
| WINNER_VS_LOSER | directional_close_ema9_pct | 123 | 114 | 1.0420 | 1.2067 | 0.4593 | 0.2793 | 0.7817 |
| WINNER_VS_LOSER | ema_slow_gap_pct | 123 | 114 | 0.2832 | 0.3394 | 0.4628 | 0.3236 | 0.7817 |
| WINNER_VS_LOSER | confirmation_adverse_wick_ratio | 123 | 114 | 0.1383 | 0.1348 | 0.5346 | 0.3542 | 0.7817 |
| WINNER_VS_LOSER | confirmation_close_location | 123 | 114 | 0.8617 | 0.8652 | 0.4654 | 0.3542 | 0.7817 |
| WINNER_VS_LOSER | ema_total_gap_pct | 123 | 114 | 0.6864 | 0.7559 | 0.4695 | 0.4187 | 0.7817 |
| WINNER_VS_LOSER | five_min_body_ratio | 123 | 114 | 0.7121 | 0.7207 | 0.4734 | 0.4800 | 0.7817 |
| WINNER_VS_LOSER | trigger_distance_c5_bps | 123 | 114 | 22.5020 | 25.3341 | 0.4748 | 0.5026 | 0.7817 |
| WINNER_VS_LOSER | ema_fast_gap_pct | 123 | 114 | 0.3452 | 0.3447 | 0.4752 | 0.5099 | 0.7817 |
| WINNER_VS_LOSER | five_min_adverse_wick_ratio | 123 | 114 | 0.1600 | 0.1782 | 0.5225 | 0.5495 | 0.7817 |
| WINNER_VS_LOSER | five_min_directional_close_location | 123 | 114 | 0.8400 | 0.8218 | 0.4775 | 0.5495 | 0.7817 |
| WINNER_VS_LOSER | volume_ratio | 123 | 114 | 2.7889 | 2.9087 | 0.4775 | 0.5509 | 0.7817 |
| WINNER_VS_LOSER | oi_change_pct | 123 | 114 | 0.9120 | 0.9060 | 0.4795 | 0.5863 | 0.7817 |
| WINNER_VS_LOSER | confirmation_volume_ratio | 123 | 114 | 1.0419 | 0.8730 | 0.5144 | 0.7024 | 0.8296 |
| WINNER_VS_LOSER | confirmation_body_ratio | 123 | 114 | 0.7391 | 0.7351 | 0.4857 | 0.7052 | 0.8296 |
| WINNER_VS_LOSER | traded_value_cr | 123 | 114 | 17.5325 | 17.5121 | 0.5095 | 0.8016 | 0.8509 |
| WINNER_VS_LOSER | entry_minute | 123 | 114 | 2.0000 | 2.0000 | 0.4919 | 0.8084 | 0.8509 |
| WINNER_VS_LOSER | confirmation_minute | 123 | 114 | 1.0000 | 1.0000 | 0.4983 | 0.9540 | 0.9540 |

At q < 0.05, **6** tested features distinguish filled from non-filled candidates, but **0** distinguish winners from losers. AUC near 0.5 means little separation; below 0.5 means the positive group tends to have lower values. These are univariate, post-selection tests—not threshold backtests.

### Spearman/Pearson association with realized net return

| Indicator | Observations | Spearman Vs Net Return | Pearson Vs Net Return |
|---|---|---|---|
| holding_minutes | 237 | +0.337 | +0.281 |
| confirmation_adverse_wick_ratio | 237 | +0.089 | +0.053 |
| confirmation_close_location | 237 | -0.089 | -0.053 |
| confirmation_body_ratio | 237 | -0.076 | -0.014 |
| traded_value_cr | 237 | +0.075 | +0.172 |
| initial_stop_risk_pct | 237 | -0.048 | +0.081 |
| directional_move_pct | 237 | -0.044 | +0.004 |
| directional_five_min_body_pct | 237 | -0.042 | +0.009 |
| confirmation_volume_ratio | 237 | +0.042 | +0.109 |
| five_min_body_ratio | 237 | -0.035 | -0.012 |
| trigger_distance_c5_bps | 237 | -0.031 | +0.077 |
| five_min_adverse_wick_ratio | 237 | +0.026 | -0.009 |
| five_min_directional_close_location | 237 | -0.026 | +0.009 |
| entry_minute | 237 | -0.024 | -0.008 |
| volume_ratio | 237 | +0.024 | -0.079 |
| ema_total_gap_pct | 237 | +0.023 | +0.204 |
| ema_fast_gap_pct | 237 | +0.022 | +0.198 |
| directional_close_ema9_pct | 237 | +0.021 | +0.081 |
| oi_change_pct | 237 | +0.019 | -0.051 |
| five_min_range_pct | 237 | +0.015 | +0.039 |
| ema_slow_gap_pct | 237 | +0.010 | +0.192 |
| confirmation_minute | 237 | -0.009 | +0.007 |

### Data-derived quartiles

| Indicator | Quartile | Observed Range | Selected | Confirmed | Fills | Fill Rate Pct | Wins | Losses | Win Rate Pct | Profit Factor | Net Return Points | Net Pnl Rs |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| directional_move_pct | Q1 | (0.199, 0.317] | 284 | 95 | 46 | 16.20% | 30 | 16 | 65.22% | 3.5937 | +29.6024 | +14,558.62 |
| directional_move_pct | Q2 | (0.317, 0.456] | 283 | 92 | 46 | 16.25% | 23 | 23 | 50.00% | 1.4584 | +7.8495 | +4,269.07 |
| directional_move_pct | Q3 | (0.456, 0.704] | 283 | 112 | 69 | 24.38% | 35 | 34 | 50.72% | 2.1456 | +29.2499 | +14,766.37 |
| directional_move_pct | Q4 | (0.704, 5.559] | 284 | 110 | 76 | 26.76% | 35 | 41 | 46.05% | 1.9777 | +27.9290 | +13,189.16 |
| directional_five_min_body_pct | Q1 | (0.0288, 0.31] | 284 | 96 | 45 | 15.85% | 28 | 17 | 62.22% | 3.1084 | +26.8871 | +13,387.49 |
| directional_five_min_body_pct | Q2 | (0.31, 0.453] | 283 | 90 | 50 | 17.67% | 28 | 22 | 56.00% | 1.9243 | +14.5998 | +7,421.72 |
| directional_five_min_body_pct | Q3 | (0.453, 0.693] | 283 | 116 | 70 | 24.73% | 37 | 33 | 52.86% | 2.3304 | +32.2716 | +16,218.85 |
| directional_five_min_body_pct | Q4 | (0.693, 5.886] | 284 | 107 | 72 | 25.35% | 30 | 42 | 41.67% | 1.6997 | +20.8724 | +9,755.16 |
| oi_change_pct | Q1 | (0.099, 0.353] | 284 | 99 | 58 | 20.42% | 35 | 23 | 60.34% | 2.7880 | +27.9046 | +13,612.32 |
| oi_change_pct | Q2 | (0.353, 0.874] | 283 | 109 | 57 | 20.14% | 25 | 32 | 43.86% | 1.3734 | +8.3320 | +4,252.26 |
| oi_change_pct | Q3 | (0.874, 1.922] | 283 | 98 | 53 | 18.73% | 23 | 30 | 43.40% | 1.1972 | +4.3790 | +2,456.31 |
| oi_change_pct | Q4 | (1.922, 3300.0] | 284 | 103 | 69 | 24.30% | 40 | 29 | 57.97% | 3.4002 | +54.0153 | +26,462.33 |
| volume_ratio | Q1 | (0.9999999999999999, 1.627] | 284 | 82 | 41 | 14.44% | 20 | 21 | 48.78% | 1.9855 | +16.8528 | +8,446.68 |
| volume_ratio | Q2 | (1.627, 2.412] | 283 | 107 | 59 | 20.85% | 35 | 24 | 59.32% | 2.5513 | +28.5975 | +14,444.19 |
| volume_ratio | Q3 | (2.412, 3.906] | 283 | 109 | 63 | 22.26% | 30 | 33 | 47.62% | 1.8703 | +19.5402 | +9,429.88 |
| volume_ratio | Q4 | (3.906, 50.207] | 284 | 111 | 74 | 26.06% | 38 | 36 | 51.35% | 2.2026 | +29.6404 | +14,462.49 |
| traded_value_cr | Q1 | (0.702, 6.173] | 284 | 85 | 34 | 11.97% | 19 | 15 | 55.88% | 2.6298 | +16.8749 | +8,652.88 |
| traded_value_cr | Q2 | (6.173, 13.585] | 283 | 109 | 66 | 23.32% | 35 | 31 | 53.03% | 1.6841 | +16.1360 | +8,078.23 |
| traded_value_cr | Q3 | (13.585, 29.046] | 283 | 109 | 62 | 21.91% | 28 | 34 | 45.16% | 1.9265 | +21.7427 | +10,737.88 |
| traded_value_cr | Q4 | (29.046, 465.646] | 284 | 106 | 75 | 26.41% | 41 | 34 | 54.67% | 2.5808 | +39.8773 | +19,314.23 |
| five_min_range_pct | Q1 | (0.226, 0.519] | 284 | 87 | 38 | 13.38% | 20 | 18 | 52.63% | 1.7494 | +11.8391 | +5,735.31 |
| five_min_range_pct | Q2 | (0.519, 0.691] | 283 | 100 | 54 | 19.08% | 33 | 21 | 61.11% | 2.9293 | +27.0274 | +13,803.62 |
| five_min_range_pct | Q3 | (0.691, 0.948] | 283 | 105 | 69 | 24.38% | 34 | 35 | 49.28% | 1.9592 | +24.7456 | +12,104.30 |
| five_min_range_pct | Q4 | (0.948, 6.681] | 284 | 117 | 76 | 26.76% | 36 | 40 | 47.37% | 2.1476 | +31.0188 | +15,139.99 |
| five_min_body_ratio | Q1 | (0.062200000000000005, 0.562] | 284 | 98 | 57 | 20.07% | 34 | 23 | 59.65% | 2.8081 | +28.6886 | +14,536.98 |
| five_min_body_ratio | Q2 | (0.562, 0.709] | 283 | 103 | 56 | 19.79% | 27 | 29 | 48.21% | 1.7941 | +16.0934 | +8,089.44 |
| five_min_body_ratio | Q3 | (0.709, 0.837] | 283 | 107 | 65 | 22.97% | 30 | 35 | 46.15% | 1.6734 | +17.9899 | +8,621.96 |
| five_min_body_ratio | Q4 | (0.837, 1.0] | 284 | 101 | 59 | 20.77% | 32 | 27 | 54.24% | 2.6103 | +31.8590 | +15,534.85 |
| five_min_adverse_wick_ratio | Q1 | (-0.001, 0.0692] | 284 | 100 | 56 | 19.72% | 30 | 26 | 53.57% | 2.6508 | +28.9983 | +14,405.52 |
| five_min_adverse_wick_ratio | Q2 | (0.0692, 0.158] | 283 | 96 | 57 | 20.14% | 31 | 26 | 54.39% | 2.4897 | +31.9519 | +15,465.80 |
| five_min_adverse_wick_ratio | Q3 | (0.158, 0.267] | 283 | 109 | 63 | 22.26% | 24 | 39 | 38.10% | 1.0655 | +1.8851 | +1,215.02 |
| five_min_adverse_wick_ratio | Q4 | (0.267, 0.777] | 284 | 104 | 61 | 21.48% | 38 | 23 | 62.30% | 3.1413 | +31.7955 | +15,696.90 |
| five_min_directional_close_location | Q1 | (0.222, 0.733] | 284 | 104 | 61 | 21.48% | 38 | 23 | 62.30% | 3.1413 | +31.7955 | +15,696.90 |
| five_min_directional_close_location | Q2 | (0.733, 0.842] | 283 | 109 | 63 | 22.26% | 24 | 39 | 38.10% | 1.0655 | +1.8851 | +1,215.02 |
| five_min_directional_close_location | Q3 | (0.842, 0.931] | 283 | 96 | 57 | 20.14% | 31 | 26 | 54.39% | 2.4897 | +31.9519 | +15,465.80 |
| five_min_directional_close_location | Q4 | (0.931, 1.0] | 284 | 100 | 56 | 19.72% | 30 | 26 | 53.57% | 2.6508 | +28.9983 | +14,405.52 |
| ema_fast_gap_pct | Q1 | (0.00253, 0.198] | 284 | 106 | 56 | 19.72% | 35 | 21 | 62.50% | 3.0928 | +31.5765 | +15,285.75 |
| ema_fast_gap_pct | Q2 | (0.198, 0.339] | 283 | 103 | 60 | 21.20% | 26 | 34 | 43.33% | 1.3461 | +8.2578 | +4,426.51 |
| ema_fast_gap_pct | Q3 | (0.339, 0.582] | 283 | 98 | 56 | 19.79% | 28 | 28 | 50.00% | 1.5914 | +11.1300 | +5,348.27 |
| ema_fast_gap_pct | Q4 | (0.582, 4.401] | 284 | 102 | 65 | 22.89% | 34 | 31 | 52.31% | 2.7561 | +43.6665 | +21,722.70 |
| ema_slow_gap_pct | Q1 | (-0.00063, 0.159] | 284 | 107 | 62 | 21.83% | 36 | 26 | 58.06% | 2.8876 | +30.0193 | +14,859.50 |
| ema_slow_gap_pct | Q2 | (0.159, 0.312] | 283 | 102 | 52 | 18.37% | 29 | 23 | 55.77% | 1.7879 | +14.0893 | +7,153.03 |
| ema_slow_gap_pct | Q3 | (0.312, 0.537] | 283 | 101 | 63 | 22.26% | 29 | 34 | 46.03% | 1.7803 | +17.4785 | +8,383.12 |
| ema_slow_gap_pct | Q4 | (0.537, 3.745] | 284 | 99 | 60 | 21.13% | 29 | 31 | 48.33% | 2.2494 | +33.0437 | +16,387.58 |
| ema_total_gap_pct | Q1 | (0.0369, 0.387] | 284 | 108 | 59 | 20.77% | 35 | 24 | 59.32% | 2.8581 | +30.1052 | +14,919.85 |
| ema_total_gap_pct | Q2 | (0.387, 0.653] | 283 | 97 | 50 | 17.67% | 25 | 25 | 50.00% | 1.5311 | +9.2272 | +4,716.50 |
| ema_total_gap_pct | Q3 | (0.653, 1.102] | 283 | 105 | 65 | 22.97% | 30 | 35 | 46.15% | 1.5384 | +12.8920 | +6,413.55 |
| ema_total_gap_pct | Q4 | (1.102, 8.146] | 284 | 99 | 63 | 22.18% | 33 | 30 | 52.38% | 2.6887 | +42.4065 | +20,733.33 |
| directional_close_ema9_pct | Q1 | (-1.0639999999999998, 0.611] | 284 | 103 | 49 | 17.25% | 30 | 19 | 61.22% | 2.6879 | +25.6241 | +12,630.76 |
| directional_close_ema9_pct | Q2 | (0.611, 0.994] | 283 | 81 | 54 | 19.08% | 28 | 26 | 51.85% | 1.7985 | +14.9344 | +7,260.54 |
| directional_close_ema9_pct | Q3 | (0.994, 1.5] | 283 | 110 | 62 | 21.91% | 27 | 35 | 43.55% | 1.4074 | +9.6799 | +4,723.68 |
| directional_close_ema9_pct | Q4 | (1.5, 8.619] | 284 | 115 | 72 | 25.35% | 38 | 34 | 52.78% | 2.7765 | +44.3925 | +22,168.25 |
| confirmation_volume_ratio | Q1 | (0.047599999999999996, 0.574] | 103 | 103 | 51 | 49.51% | 30 | 21 | 58.82% | 2.2208 | +19.4615 | +9,621.73 |
| confirmation_volume_ratio | Q2 | (0.574, 0.903] | 102 | 102 | 62 | 60.78% | 23 | 39 | 37.10% | 1.2787 | +7.6640 | +3,900.86 |
| confirmation_volume_ratio | Q3 | (0.903, 1.476] | 102 | 102 | 58 | 56.86% | 31 | 27 | 53.45% | 2.7527 | +30.4864 | +14,819.53 |
| confirmation_volume_ratio | Q4 | (1.476, 5.851] | 102 | 102 | 66 | 64.71% | 39 | 27 | 59.09% | 2.6977 | +37.0189 | +18,441.11 |
| confirmation_body_ratio | Q1 | (0.0931, 0.627] | 103 | 103 | 57 | 55.34% | 34 | 23 | 59.65% | 2.9492 | +30.4783 | +14,641.68 |
| confirmation_body_ratio | Q2 | (0.627, 0.727] | 102 | 102 | 59 | 57.84% | 26 | 33 | 44.07% | 1.6033 | +14.6136 | +7,255.32 |
| confirmation_body_ratio | Q3 | (0.727, 0.833] | 102 | 102 | 60 | 58.82% | 31 | 29 | 51.67% | 2.4232 | +26.1104 | +13,189.40 |
| confirmation_body_ratio | Q4 | (0.833, 1.0] | 102 | 102 | 61 | 59.80% | 32 | 29 | 52.46% | 1.9590 | +23.4285 | +11,696.83 |
| confirmation_adverse_wick_ratio | Q1 | (-0.001, 0.138] | 205 | 205 | 120 | 58.54% | 61 | 59 | 50.83% | 2.0695 | +46.2489 | +22,656.30 |
| confirmation_adverse_wick_ratio | Q2 | (0.138, 0.239] | 102 | 102 | 63 | 61.76% | 31 | 32 | 49.21% | 1.8200 | +18.3397 | +9,563.97 |
| confirmation_adverse_wick_ratio | Q3 | (0.239, 0.5] | 102 | 102 | 54 | 52.94% | 31 | 23 | 57.41% | 2.7644 | +30.0422 | +14,562.95 |
| confirmation_close_location | Q1 | (0.499, 0.761] | 103 | 103 | 54 | 52.43% | 31 | 23 | 57.41% | 2.7644 | +30.0422 | +14,562.95 |
| confirmation_close_location | Q2 | (0.761, 0.862] | 102 | 102 | 64 | 62.75% | 32 | 32 | 50.00% | 1.9473 | +21.1878 | +10,786.58 |
| confirmation_close_location | Q3 | (0.862, 1.0] | 204 | 204 | 119 | 58.33% | 60 | 59 | 50.42% | 2.0037 | +43.4008 | +21,433.69 |
| trigger_distance_c5_bps | Q1 | (1.778, 13.944] | 103 | 103 | 53 | 51.46% | 31 | 22 | 58.49% | 2.7203 | +25.2157 | +12,386.99 |
| trigger_distance_c5_bps | Q2 | (13.944, 22.202] | 102 | 102 | 48 | 47.06% | 28 | 20 | 58.33% | 2.4645 | +21.4157 | +10,674.15 |
| trigger_distance_c5_bps | Q3 | (22.202, 34.633] | 102 | 102 | 64 | 62.75% | 26 | 38 | 40.62% | 1.3765 | +9.7688 | +4,787.50 |
| trigger_distance_c5_bps | Q4 | (34.633, 121.625] | 102 | 102 | 72 | 70.59% | 38 | 34 | 52.78% | 2.3951 | +38.2306 | +18,934.59 |
| entry_minute | Q1 | (1.999, 3.0] | 203 | 203 | 203 | 100.00% | 105 | 98 | 51.72% | 2.0945 | +77.8996 | +38,711.24 |
| entry_minute | Q2 | (3.0, 5.0] | 34 | 34 | 34 | 100.00% | 18 | 16 | 52.94% | 2.4599 | +16.7313 | +8,071.99 |

Quartiles repeatedly show non-monotonic results. They are hypothesis generators only; choosing a winning quartile after seeing these outcomes would be leakage.

### Fixed bins — `directional_move_pct`

| Bin | Selected | Confirmed | Fills | Confirmation Rate Pct | Fill Rate Pct | Wins | Losses | Win Rate Pct | Profit Factor | Net Return Points | Net Pnl Rs |
|---|---|---|---|---|---|---|---|---|---|---|---|
| <0.30 | 244 | 83 | 37 | 34.02% | 15.16% | 23 | 14 | 62.16% | 3.3003 | +22.6946 | +11,281.43 |
| 0.30–0.50 | 393 | 123 | 68 | 31.30% | 17.30% | 36 | 32 | 52.94% | 1.6872 | +16.5865 | +8,610.54 |
| 0.50–0.75 | 252 | 107 | 64 | 42.46% | 25.40% | 33 | 31 | 51.56% | 2.3714 | +32.3094 | +15,907.65 |
| 0.75–1.00 | 141 | 58 | 40 | 41.13% | 28.37% | 19 | 21 | 47.50% | 1.9178 | +13.2919 | +6,386.28 |
| 1.00–1.50 | 76 | 30 | 22 | 39.47% | 28.95% | 9 | 13 | 40.91% | 1.4763 | +3.9653 | +1,708.93 |
| 1.50+ | 28 | 8 | 6 | 28.57% | 21.43% | 3 | 3 | 50.00% | 3.5545 | +5.7831 | +2,888.40 |

### Fixed bins — `oi_change_pct`

| Bin | Selected | Confirmed | Fills | Confirmation Rate Pct | Fill Rate Pct | Wins | Losses | Win Rate Pct | Profit Factor | Net Return Points | Net Pnl Rs |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 0.10–0.50 | 377 | 135 | 79 | 35.81% | 20.95% | 48 | 31 | 60.76% | 2.4838 | +33.7457 | +16,612.21 |
| 0.50–1.00 | 237 | 90 | 46 | 37.97% | 19.41% | 15 | 31 | 32.61% | 0.9779 | -0.4337 | -64.70 |
| 1.00–2.00 | 248 | 85 | 46 | 34.27% | 18.55% | 23 | 23 | 50.00% | 1.7834 | +13.8912 | +6,820.77 |
| 2.00–5.00 | 179 | 61 | 42 | 34.08% | 23.46% | 21 | 21 | 50.00% | 2.3481 | +21.2038 | +10,847.95 |
| 5.00+ | 93 | 38 | 24 | 40.86% | 25.81% | 16 | 8 | 66.67% | 4.8701 | +26.2238 | +12,567.00 |

### Fixed bins — `volume_ratio`

| Bin | Selected | Confirmed | Fills | Confirmation Rate Pct | Fill Rate Pct | Wins | Losses | Win Rate Pct | Profit Factor | Net Return Points | Net Pnl Rs |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 1.00–1.50 | 214 | 56 | 26 | 26.17% | 12.15% | 11 | 15 | 42.31% | 1.3252 | +3.9570 | +2,137.35 |
| 1.50–2.00 | 218 | 78 | 43 | 35.78% | 19.72% | 29 | 14 | 67.44% | 4.2059 | +34.5905 | +17,158.44 |
| 2.00–3.00 | 251 | 88 | 51 | 35.06% | 20.32% | 22 | 29 | 43.14% | 1.3312 | +7.5657 | +3,827.80 |
| 3.00–5.00 | 297 | 128 | 75 | 43.10% | 25.25% | 42 | 33 | 56.00% | 2.7236 | +37.5257 | +18,296.01 |
| 5.00+ | 154 | 59 | 42 | 38.31% | 27.27% | 19 | 23 | 45.24% | 1.7297 | +10.9920 | +5,363.63 |

### Fixed bins — `traded_value_cr`

| Bin | Selected | Confirmed | Fills | Confirmation Rate Pct | Fill Rate Pct | Wins | Losses | Win Rate Pct | Profit Factor | Net Return Points | Net Pnl Rs |
|---|---|---|---|---|---|---|---|---|---|---|---|
| <2.5cr | 49 | 8 | 1 | 16.33% | 2.04% | 1 | 0 | 100.00% | ∞ | +1.0377 | +501.16 |
| 2.5–5cr | 174 | 53 | 20 | 30.46% | 11.49% | 11 | 9 | 55.00% | 2.8580 | +10.4652 | +5,283.61 |
| 5–10cr | 222 | 83 | 51 | 37.39% | 22.97% | 25 | 26 | 49.02% | 1.4925 | +10.1676 | +5,406.48 |
| 10–25cr | 358 | 141 | 76 | 39.39% | 21.23% | 40 | 36 | 52.63% | 2.1409 | +29.3814 | +14,420.59 |
| 25–50cr | 190 | 67 | 48 | 35.26% | 25.26% | 20 | 28 | 41.67% | 1.4413 | +8.8303 | +4,490.60 |
| 50cr+ | 141 | 57 | 41 | 40.43% | 29.08% | 26 | 15 | 63.41% | 4.2802 | +34.7486 | +16,680.80 |

### Fixed bins — `five_min_range_pct`

| Bin | Selected | Confirmed | Fills | Confirmation Rate Pct | Fill Rate Pct | Wins | Losses | Win Rate Pct | Profit Factor | Net Return Points | Net Pnl Rs |
|---|---|---|---|---|---|---|---|---|---|---|---|
| <0.30 | 12 | 2 | 1 | 16.67% | 8.33% | 0 | 1 | 0.00% | 0.0000 | -1.1584 | -577.93 |
| 0.30–0.50 | 242 | 77 | 31 | 31.82% | 12.81% | 16 | 15 | 51.61% | 1.7440 | +9.6241 | +4,642.21 |
| 0.50–0.75 | 388 | 131 | 73 | 33.76% | 18.81% | 45 | 28 | 61.64% | 2.7850 | +35.5339 | +18,107.06 |
| 0.75–1.00 | 253 | 97 | 65 | 38.34% | 25.69% | 31 | 34 | 47.69% | 1.9804 | +23.0402 | +11,142.71 |
| 1.00–1.50 | 172 | 77 | 50 | 44.77% | 29.07% | 25 | 25 | 50.00% | 2.4144 | +23.8125 | +11,571.27 |
| 1.50+ | 67 | 25 | 17 | 37.31% | 25.37% | 6 | 11 | 35.29% | 1.4554 | +3.7786 | +1,897.90 |

### Fixed bins — `five_min_body_ratio`

| Bin | Selected | Confirmed | Fills | Confirmation Rate Pct | Fill Rate Pct | Wins | Losses | Win Rate Pct | Profit Factor | Net Return Points | Net Pnl Rs |
|---|---|---|---|---|---|---|---|---|---|---|---|
| <0.20 | 7 | 2 | 2 | 28.57% | 28.57% | 1 | 1 | 50.00% | 0.6730 | -0.3768 | -170.35 |
| 0.20–0.40 | 67 | 32 | 23 | 47.76% | 34.33% | 12 | 11 | 52.17% | 2.3784 | +9.6074 | +4,749.01 |
| 0.40–0.60 | 263 | 83 | 45 | 31.56% | 17.11% | 28 | 17 | 62.22% | 3.0082 | +25.8791 | +12,873.45 |
| 0.60–0.80 | 438 | 167 | 92 | 38.13% | 21.00% | 43 | 49 | 46.74% | 1.7303 | +25.7505 | +12,878.57 |
| 0.80+ | 359 | 125 | 75 | 34.82% | 20.89% | 39 | 36 | 52.00% | 2.2810 | +33.7707 | +16,452.54 |

### Fixed bins — `five_min_adverse_wick_ratio`

| Bin | Selected | Confirmed | Fills | Confirmation Rate Pct | Fill Rate Pct | Wins | Losses | Win Rate Pct | Profit Factor | Net Return Points | Net Pnl Rs |
|---|---|---|---|---|---|---|---|---|---|---|---|
| <0.10 | 394 | 145 | 81 | 36.80% | 20.56% | 42 | 39 | 51.85% | 2.4491 | +40.5505 | +19,803.99 |
| 0.10–0.20 | 285 | 91 | 57 | 31.93% | 20.00% | 29 | 28 | 50.88% | 2.1817 | +23.8592 | +11,842.41 |
| 0.20–0.30 | 217 | 88 | 46 | 40.55% | 21.20% | 19 | 27 | 41.30% | 1.1687 | +3.5548 | +1,982.44 |
| 0.30–0.40 | 128 | 44 | 26 | 34.38% | 20.31% | 15 | 11 | 57.69% | 2.2224 | +8.9694 | +4,417.79 |
| 0.40–0.50 | 72 | 22 | 15 | 30.56% | 20.83% | 12 | 3 | 80.00% | 7.1564 | +14.4939 | +7,089.00 |
| 0.50+ | 38 | 19 | 12 | 50.00% | 31.58% | 6 | 6 | 50.00% | 1.8673 | +3.2029 | +1,647.59 |

### Fixed bins — `five_min_directional_close_location`

| Bin | Selected | Confirmed | Fills | Confirmation Rate Pct | Fill Rate Pct | Wins | Losses | Win Rate Pct | Profit Factor | Net Return Points | Net Pnl Rs |
|---|---|---|---|---|---|---|---|---|---|---|---|
| <0.50 | 36 | 18 | 12 | 50.00% | 33.33% | 6 | 6 | 50.00% | 1.8673 | +3.2029 | +1,647.59 |
| 0.50–0.60 | 72 | 23 | 15 | 31.94% | 20.83% | 12 | 3 | 80.00% | 7.1564 | +14.4939 | +7,089.00 |
| 0.60–0.75 | 206 | 77 | 39 | 37.38% | 18.93% | 23 | 16 | 58.97% | 2.6637 | +16.6468 | +8,224.73 |
| 0.75–0.90 | 424 | 145 | 90 | 34.20% | 21.23% | 40 | 50 | 44.44% | 1.5113 | +19.7367 | +10,017.92 |
| 0.90+ | 396 | 146 | 81 | 36.87% | 20.45% | 42 | 39 | 51.85% | 2.4491 | +40.5505 | +19,803.99 |

### Fixed bins — `ema_total_gap_pct`

| Bin | Selected | Confirmed | Fills | Confirmation Rate Pct | Fill Rate Pct | Wins | Losses | Win Rate Pct | Profit Factor | Net Return Points | Net Pnl Rs |
|---|---|---|---|---|---|---|---|---|---|---|---|
| <0.10 | 17 | 7 | 6 | 41.18% | 35.29% | 5 | 1 | 83.33% | 18.6960 | +7.0035 | +3,511.18 |
| 0.10–0.25 | 116 | 47 | 24 | 40.52% | 20.69% | 16 | 8 | 66.67% | 3.2587 | +13.6805 | +6,622.91 |
| 0.25–0.50 | 274 | 102 | 57 | 37.23% | 20.80% | 27 | 30 | 47.37% | 1.6890 | +13.4135 | +6,728.10 |
| 0.50–1.00 | 395 | 135 | 75 | 34.18% | 18.99% | 34 | 41 | 45.33% | 1.2533 | +7.1425 | +3,799.10 |
| 1.00+ | 332 | 118 | 75 | 35.54% | 22.59% | 41 | 34 | 54.67% | 2.8722 | +53.3908 | +26,121.94 |

### Fixed bins — `confirmation_volume_ratio`

| Bin | Selected | Confirmed | Fills | Confirmation Rate Pct | Fill Rate Pct | Wins | Losses | Win Rate Pct | Profit Factor | Net Return Points | Net Pnl Rs |
|---|---|---|---|---|---|---|---|---|---|---|---|
| <0.50 | 75 | 75 | 40 | 100.00% | 53.33% | 24 | 16 | 60.00% | 2.3739 | +16.3025 | +7,997.76 |
| 0.50–0.75 | 86 | 86 | 43 | 100.00% | 50.00% | 18 | 25 | 41.86% | 1.2544 | +4.7532 | +2,578.95 |
| 0.75–1.00 | 68 | 68 | 43 | 100.00% | 63.24% | 18 | 25 | 41.86% | 1.7165 | +11.8368 | +5,736.65 |
| 1.00–1.50 | 81 | 81 | 48 | 100.00% | 59.26% | 27 | 21 | 56.25% | 3.2015 | +30.2880 | +14,758.56 |
| 1.50–2.00 | 53 | 53 | 32 | 100.00% | 60.38% | 17 | 15 | 53.12% | 1.4939 | +6.5395 | +3,326.61 |
| 2.00+ | 46 | 46 | 31 | 100.00% | 67.39% | 19 | 12 | 61.29% | 3.9089 | +24.9109 | +12,384.71 |

### Fixed bins — `confirmation_body_ratio`

| Bin | Selected | Confirmed | Fills | Confirmation Rate Pct | Fill Rate Pct | Wins | Losses | Win Rate Pct | Profit Factor | Net Return Points | Net Pnl Rs |
|---|---|---|---|---|---|---|---|---|---|---|---|
| <0.40 | 15 | 15 | 11 | 100.00% | 73.33% | 7 | 4 | 63.64% | 2.6837 | +3.7271 | +1,733.76 |
| 0.40–0.50 | 22 | 22 | 12 | 100.00% | 54.55% | 4 | 8 | 33.33% | 1.1416 | +0.7128 | +411.48 |
| 0.50–0.60 | 43 | 43 | 21 | 100.00% | 48.84% | 16 | 5 | 76.19% | 7.2234 | +22.1701 | +10,553.61 |
| 0.60–0.75 | 138 | 138 | 79 | 100.00% | 57.25% | 36 | 43 | 45.57% | 1.6505 | +20.9232 | +10,573.28 |
| 0.75+ | 191 | 191 | 114 | 100.00% | 59.69% | 60 | 54 | 52.63% | 2.1875 | +47.0976 | +23,511.09 |

### Fixed bins — `confirmation_adverse_wick_ratio`

| Bin | Selected | Confirmed | Fills | Confirmation Rate Pct | Fill Rate Pct | Wins | Losses | Win Rate Pct | Profit Factor | Net Return Points | Net Pnl Rs |
|---|---|---|---|---|---|---|---|---|---|---|---|
| <0.10 | 164 | 164 | 94 | 100.00% | 57.32% | 46 | 48 | 48.94% | 1.9338 | +33.7372 | +16,655.23 |
| 0.10–0.20 | 93 | 93 | 57 | 100.00% | 61.29% | 28 | 29 | 49.12% | 1.7709 | +15.3360 | +7,454.56 |
| 0.20–0.30 | 88 | 88 | 54 | 100.00% | 61.36% | 32 | 22 | 59.26% | 2.8652 | +31.9290 | +16,261.86 |
| 0.30–0.40 | 49 | 49 | 24 | 100.00% | 48.98% | 12 | 12 | 50.00% | 1.9879 | +7.6424 | +3,515.80 |
| 0.40–0.50 | 14 | 14 | 8 | 100.00% | 57.14% | 5 | 3 | 62.50% | 4.4012 | +5.9862 | +2,895.77 |
| 0.50+ | 1 | 1 | 0 | 100.00% | 0.00% | 0 | 0 | — | — | +0.0000 | +0.00 |

### Fixed bins — `confirmation_close_location`

| Bin | Selected | Confirmed | Fills | Confirmation Rate Pct | Fill Rate Pct | Wins | Losses | Win Rate Pct | Profit Factor | Net Return Points | Net Pnl Rs |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 0.50–0.60 | 15 | 15 | 8 | 100.00% | 53.33% | 5 | 3 | 62.50% | 4.4012 | +5.9862 | +2,895.77 |
| 0.60–0.75 | 84 | 84 | 44 | 100.00% | 52.38% | 25 | 19 | 56.82% | 2.6733 | +24.4539 | +11,866.07 |
| 0.75–0.90 | 145 | 145 | 90 | 100.00% | 62.07% | 46 | 44 | 51.11% | 1.9985 | +30.0893 | +15,197.86 |
| 0.90+ | 165 | 165 | 95 | 100.00% | 57.58% | 47 | 48 | 49.47% | 1.9439 | +34.1014 | +16,823.53 |

### Fixed bins — `trigger_distance_c5_bps`

| Bin | Selected | Confirmed | Fills | Confirmation Rate Pct | Fill Rate Pct | Wins | Losses | Win Rate Pct | Profit Factor | Net Return Points | Net Pnl Rs |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 0–10 | 57 | 57 | 29 | 100.00% | 50.88% | 17 | 12 | 58.62% | 2.7257 | +13.3038 | +6,546.84 |
| 10–20 | 122 | 122 | 59 | 100.00% | 48.36% | 32 | 27 | 54.24% | 1.8986 | +17.7560 | +8,753.31 |
| 20–30 | 107 | 107 | 63 | 100.00% | 58.88% | 30 | 33 | 47.62% | 2.0358 | +23.5429 | +11,652.73 |
| 30–50 | 86 | 86 | 58 | 100.00% | 67.44% | 28 | 30 | 48.28% | 1.9729 | +21.6978 | +10,719.42 |
| 50–100 | 34 | 34 | 25 | 100.00% | 73.53% | 15 | 10 | 60.00% | 2.9566 | +16.2934 | +8,332.73 |
| 100+ | 3 | 3 | 3 | 100.00% | 100.00% | 1 | 2 | 33.33% | 2.1274 | +2.0370 | +778.20 |

### Fixed bins — `entry_delay_minutes`

| Bin | Selected | Confirmed | Fills | Confirmation Rate Pct | Fill Rate Pct | Wins | Losses | Win Rate Pct | Profit Factor | Net Return Points | Net Pnl Rs |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 1–2 | 190 | 190 | 190 | 100.00% | 100.00% | 98 | 92 | 51.58% | 2.1302 | +75.9201 | +37,684.37 |
| 2–3 | 35 | 35 | 35 | 100.00% | 100.00% | 20 | 15 | 57.14% | 2.6453 | +16.6419 | +8,127.28 |
| 3–4 | 7 | 7 | 7 | 100.00% | 100.00% | 3 | 4 | 42.86% | 1.3077 | +0.7666 | +345.75 |
| 4+ | 5 | 5 | 5 | 100.00% | 100.00% | 2 | 3 | 40.00% | 1.4560 | +1.3022 | +625.83 |

## 10. One-minute timing and trigger quality

### Confirmation minute

| Confirmation Minute | Fills | Wins | Losses | Win Rate Pct | Profit Factor | Net Return Points | Net Pnl Rs |
|---|---|---|---|---|---|---|---|
| 1 | 174 | 91 | 83 | 52.30% | 2.1862 | +73.9694 | +36,666.90 |
| 2 | 45 | 21 | 24 | 46.67% | 1.5021 | +7.6223 | +3,827.87 |
| 3 | 18 | 11 | 7 | 61.11% | 3.5606 | +13.0392 | +6,288.46 |

### Confirmation minute under harsh costs

| Confirmation Minute | Fills Reference | Wins | Losses | Win Rate Pct | Profit Factor Reference | Net Return Points Reference | Profit Factor Harsh | Net Return Points Harsh | Net Pnl Rs Harsh |
|---|---|---|---|---|---|---|---|---|---|
| 1 | 174 | 91 | 83 | 52.30% | 2.1862 | +73.9694 | 1.6670 | +49.2975 | +24,779.29 |
| 2 | 45 | 21 | 24 | 46.67% | 1.5021 | +7.6223 | 0.9100 | -1.7935 | -760.08 |
| 3 | 18 | 11 | 7 | 61.11% | 3.5606 | +13.0392 | 2.4121 | +9.3131 | +4,462.54 |

### Confirmation minute by setup

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
| 09:40_SHORT / M1 | 18 | 9 | 9 | 50.00% | 1.5059 | +4.8307 | +2,670.63 |
| 09:45_LONG / M1 | 9 | 6 | 3 | 66.67% | 4.7245 | +8.7639 | +4,366.10 |
| 09:45_SHORT / M1 | 12 | 7 | 5 | 58.33% | 1.9401 | +3.4848 | +1,821.92 |

### Entry minute

| Entry Minute | Fills | Wins | Losses | Win Rate Pct | Profit Factor | Net Return Points | Net Pnl Rs |
|---|---|---|---|---|---|---|---|
| 2 | 136 | 72 | 64 | 52.94% | 2.2608 | +61.2550 | +30,629.46 |
| 3 | 67 | 33 | 34 | 49.25% | 1.7368 | +16.6445 | +8,081.77 |
| 4 | 24 | 12 | 12 | 50.00% | 2.3926 | +11.0559 | +5,196.68 |
| 5 | 10 | 6 | 4 | 60.00% | 2.6116 | +5.6754 | +2,875.31 |

Minute 2 is weak under harsh costs specifically in some SHORT setups, while minute-2 LONG remains useful. If tested, use a predeclared minute-2 SHORT quality/reconfirmation rule—not a global minute ban or a retrospective deletion.

## 11. Exit, holding-time and excursion diagnostics

### Exit reason

| Exit Reason | Fills | Wins | Losses | Win Rate Pct | Profit Factor | Net Return Points | Net Pnl Rs | Average Return Points |
|---|---|---|---|---|---|---|---|---|
| STOP | 100 | 0 | 100 | 0.00% | 0.0000 | -77.4898 | -37,093.48 | -0.7749 |
| LAST_REAL_BAR_SENSITIVITY | 71 | 57 | 14 | 80.28% | 11.5610 | +54.3336 | +26,750.17 | +0.7653 |
| TARGET | 66 | 66 | 0 | 100.00% | ∞ | +117.7871 | +57,126.54 | +1.7847 |

### Exit reason under harsh costs

| Exit Reason | Fills Reference | Wins Reference | Losses Reference | Profit Factor Reference | Net Return Points Reference | Net Pnl Rs Reference | Fills Harsh | Wins Harsh | Losses Harsh | Profit Factor Harsh | Net Return Points Harsh | Net Pnl Rs Harsh |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| LAST_REAL_BAR_SENSITIVITY | 71 | 57 | 14 | 11.5610 | +54.3336 | +26,750.17 | 65 | 50 | 15 | 9.0025 | +44.0036 | +21,542.61 |
| STOP | 100 | 0 | 100 | 0.0000 | -77.4898 | -37,093.48 | 106 | 0 | 106 | 0.0000 | -93.3976 | -44,743.64 |
| STOP_GAP | — | — | — | — | — | — | 2 | 0 | 2 | 0.0000 | -1.5375 | -699.66 |
| TARGET | 66 | 66 | 0 | ∞ | +117.7871 | +57,126.54 | 64 | 64 | 0 | ∞ | +107.7486 | +52,382.44 |

The 71 last-real-bar exits contribute +54.3336 points (57.42% of total). Exit reason and holding duration are realized outcomes; they cannot be used as entry filters.

### Terminal clock under last-real-bar policy

| Exit Clock | Fills | Wins | Losses | Net Return Points | Net Pnl Rs |
|---|---|---|---|---|---|
| 15:15 | 11 | 8 | 3 | +8.2877 | +4,096.70 |
| 15:30 | 60 | 49 | 11 | +46.0458 | +22,653.47 |

### Holding-time buckets

| Holding Bin | Fills | Wins | Losses | Win Rate Pct | Profit Factor | Net Return Points | Net Pnl Rs |
|---|---|---|---|---|---|---|---|
| <5 | 34 | 6 | 28 | 17.65% | 0.2931 | -12.2418 | -5,952.05 |
| 120+ | 109 | 77 | 32 | 70.64% | 4.9661 | +87.7319 | +42,814.31 |
| 60–120 | 21 | 9 | 12 | 42.86% | 1.4665 | +5.2823 | +2,933.63 |
| 5–15 | 32 | 14 | 18 | 43.75% | 1.5146 | +6.7426 | +3,366.50 |
| 15–30 | 21 | 9 | 12 | 42.86% | 0.9264 | -0.6065 | -203.53 |
| 30–60 | 20 | 8 | 12 | 40.00% | 1.7329 | +7.7224 | +3,824.37 |

### Exit-time buckets

| Exit Time Bucket | Fills | Wins | Losses | Profit Factor | Net Return Points | Net Pnl Rs |
|---|---|---|---|---|---|---|
| BEFORE_10 | 83 | 29 | 54 | 0.9185 | -2.8880 | -1,204.74 |
| 15_PLUS | 75 | 59 | 16 | 8.8159 | +58.2270 | +28,669.26 |
| 10–12 | 50 | 21 | 29 | 1.7249 | +19.0150 | +9,697.17 |
| 12–14 | 22 | 9 | 13 | 1.7141 | +8.3604 | +4,153.77 |
| 14–15 | 7 | 5 | 2 | 7.5886 | +11.9165 | +5,467.76 |

### MFE/MAE by outcome

| Outcome | Fills | Median Mfe Lower Pct | Median Mfe Upper Pct | Median Mae Lower Pct | Median Mae Upper Pct | Median Mfe Lower R | Median Mfe Upper R | Median Mae Upper R | Median Holding Minutes | Median Gross Mfe Capture Pct |
|---|---|---|---|---|---|---|---|---|---|---|
| LOSS | 114 | 0.37% | 0.37% | 0.50% | 0.57% | 0.5524 | 0.5524 | 1.0958 | 26.0000 | -159.78% |
| WIN | 123 | 1.74% | 1.78% | 0.22% | 0.25% | 2.4793 | 2.6721 | 0.4050 | 297.0000 | 88.47% |

### Losing trades that first reached favorable R thresholds

| Diagnostic | Trades | Share Of Losses Pct |
|---|---|---|
| LOSERS_WITH_MFE_LOWER_AT_LEAST_0.25R | 80 | 70 |
| LOSERS_WITH_MFE_LOWER_AT_LEAST_0.50R | 63 | 55 |
| LOSERS_WITH_MFE_LOWER_AT_LEAST_0.75R | 43 | 37 |
| LOSERS_WITH_MFE_LOWER_AT_LEAST_1.00R | 32 | 28 |
| LOSERS_WITH_MFE_LOWER_AT_LEAST_2.00R | 8 | 7 |

### OHLC-boundary quality

| Filled Trades | Intrabar Trigger Fills | Ambiguous Entry Bars | Exit At Bar Open | Excursion Boundary Ambiguous | Median Mfe Bound Width Pct | Maximum Mfe Bound Width Pct | Median Mae Bound Width Pct | Maximum Mae Bound Width Pct |
|---|---|---|---|---|---|---|---|---|
| 237 | 213 | 2 | 2 | 231 | 0.00% | 2.43% | 0.01% | 0.54% |

Because most excursion boundaries are ambiguous/incomplete, MFE/MAE can motivate a separately replayed exit hypothesis but cannot support direct trailing-stop optimization yet.

### OI percentage low-base anomalies

| Session Date | Setup Id | Side | Symbol | Oi | Prev Oi | Oi Change Pct | Net Return Pct | Net Pnl Rs |
|---|---|---|---|---|---|---|---|---|
| 2026-05-27 | 09:25_LONG | LONG | VEDL | 39100.0000 | 1150.0000 | 3300.00% | -0.56% | -277.65 |
| 2026-06-02 | 09:30_SHORT | SHORT | PAYTM | 10150.0000 | 2900.0000 | 250.00% | -1.15% | -565.96 |
| 2026-05-27 | 09:40_SHORT | SHORT | ONGC | 15750.0000 | 9000.0000 | 75.00% | 1.63% | +811.34 |
| 2026-05-27 | 09:25_SHORT | SHORT | HDFCBANK | 30550.0000 | 19500.0000 | 56.67% | 0.75% | +373.80 |
| 2026-06-03 | 09:30_SHORT | SHORT | LTM | 6750.0000 | 5550.0000 | 21.62% | 0.23% | +112.70 |

Very large OI-change percentages can come from a small prior-OI denominator. A minimum prior-OI/data-quality rule is worth testing causally; these rows are not enough to choose its threshold.

## 12. Symbols, concentration and extremes

| Unique Symbols | Positive Symbols | Negative Symbols | One Fill Symbols | Top 1 Positive Symbol Share Of Net Pct | Top 5 Positive Symbols Share Of Net Pct | Top 10 Positive Symbols Share Of Net Pct | Top 10 Absolute Symbol Share Pct | Best 5 Days Share Of Net Pct | Best 10 Days Share Of Net Pct | Best 10 Trades Share Of Net Pct |
|---|---|---|---|---|---|---|---|---|---|---|
| 126 | 70 | 56 | 60 | 7.07% | 31.16% | 55.30% | 27.81% | 48 | 78 | 32.09% |

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
| TATAELXSI | 2 | 2 | 0 | 100.00% | ∞ | +3.8874 | +1,864.59 |
| BANDHANBNK | 3 | 2 | 1 | 66.67% | 7.3754 | +3.5379 | +1,765.70 |
| INDUSTOWER | 3 | 3 | 0 | 100.00% | ∞ | +3.3938 | +1,690.80 |
| ADANIGREEN | 1 | 1 | 0 | 100.00% | ∞ | +3.1097 | +1,514.16 |
| ANGELONE | 3 | 2 | 1 | 66.67% | 62.5540 | +2.9125 | +1,447.71 |
| VMM | 1 | 1 | 0 | 100.00% | ∞ | +2.8489 | +1,423.26 |

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
| DRREDDY | 1 | 0 | 1 | 0.00% | 0.0000 | -1.1544 | -570.08 |
| VEDL | 2 | 0 | 2 | 0.00% | 0.0000 | -1.1169 | -555.85 |

Sixty symbols have only one fill. Symbol blacklists/whitelists would have extreme sampling error and survivorship risk.

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
| 2026-07-10 | 09:40_SHORT | SHORT | DRREDDY | 2026-07-10 09:42:00+05:30 | 2026-07-10 10:12:00+05:30 | STOP | -1.15% | -570.08 | 0.1701 | 1.1259 |

## 13. Statistical uncertainty and drawdown

### Risk summary

| Best Pnl Day | Best Day Pnl Rs | Worst Pnl Day | Worst Day Pnl Rs | Average Daily Pnl Rs | Median Daily Pnl Rs | Daily Pnl Std Rs | Positive Days | Negative Days | Flat Days | Max Consecutive Positive Days | Max Consecutive Negative Days | Max Consecutive Winning Trades | Max Consecutive Losing Trades | Max Drawdown Points | Max Drawdown Pnl Rs | Recovery Factor | Win Rate Wilson 95 Low Pct | Win Rate Wilson 95 High Pct | Extra Break Even Cost Bps On Fixed Notional |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 2026-07-07 | +5,602.68 | 2026-07-09 | -1,250.56 | +719.74 | +349.48 | +1,562.40 | 37 | 25 | 3 | 5 | 4 | 7 | 5 | +8.5674 | +4,045.90 | 11.5631 | 45.56% | 58.18% | 40.7566 |

### Drawdown episodes

| Start Session | Trough Session | Recovery Session | Underwater Sessions | Depth Pnl Rs | Depth Return Points | Recovered |
|---|---|---|---|---|---|---|
| 2026-06-03 | 2026-06-18 | 2026-06-29 | 16 | +4,045.90 | +8.5674 | Yes |
| 2026-07-13 | 2026-07-16 | 2026-07-22 | 6 | +1,275.08 | +2.5815 | Yes |
| 2026-08-03 | 2026-08-05 | 2026-08-10 | 4 | +1,262.49 | +2.7266 | Yes |
| 2026-07-08 | 2026-07-09 | 2026-07-10 | 1 | +1,250.56 | +2.5823 | Yes |
| 2026-08-19 | 2026-08-21 | 2026-08-25 | 3 | +1,172.44 | +2.3632 | Yes |
| 2026-06-29 | 2026-06-30 | 2026-07-01 | 1 | +1,145.62 | +2.3040 | Yes |
| 2026-08-10 | 2026-08-12 | 2026-08-18 | 5 | +1,089.78 | +2.5777 | Yes |
| 2026-07-24 | 2026-07-27 | 2026-07-28 | 1 | +919.61 | +1.7545 | Yes |
| 2026-05-29 | 2026-06-01 | 2026-06-03 | 2 | +565.65 | +1.1520 | Yes |
| 2026-07-02 | 2026-07-03 | 2026-07-06 | 1 | +45.04 | +0.1215 | Yes |

### IID session bootstrap — conditional on the observed strategy and data

| Scenario | Bootstrap Unit | Bootstrap Replicates | Probability Positive Total Pnl Pct | Total Pnl Rs P025 | Total Pnl Rs Median | Total Pnl Rs P975 | Net Points P025 | Net Points Median | Net Points P975 | Pf P025 | Pf Median | Pf P975 |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| REFERENCE_15_0 | SESSION_WITH_REPLACEMENT | 10,000 | 99.99% | +23,352.84 | +46,428.01 | +72,033.52 | +46.1333 | +93.9487 | +146.7581 | 1.5273 | 2.1443 | 2.9053 |
| STRESS_20_2 | SESSION_WITH_REPLACEMENT | 10,000 | 99.99% | +16,008.11 | +38,323.58 | +63,132.34 | +31.0491 | +77.0881 | +128.9569 | 1.3261 | 1.8594 | 2.5195 |
| STRESS_25_5 | SESSION_WITH_REPLACEMENT | 10,000 | 99.51% | +6,946.61 | +28,417.08 | +52,539.38 | +12.3649 | +56.6585 | +106.4761 | 1.1161 | 1.5674 | 2.1207 |

### Random ordering of the realized 65 daily P&Ls

| Method | Replicates | Observed Mdd Pnl Rs | Mdd Pnl Rs P50 | Mdd Pnl Rs P75 | Mdd Pnl Rs P90 | Mdd Pnl Rs P95 | Mdd Pnl Rs P975 |
|---|---|---|---|---|---|---|---|
| RANDOM_SESSION_ORDER | 5,000 | +4,045.90 | +2,740.71 | +3,310.27 | +3,951.32 | +4,414.00 | +4,874.73 |

These resamples quantify conditional sampling/order uncertainty only. They do **not** account for Stage 10 being chosen post-hoc, multiple strategy trials, missing sessions/paths, static-universe bias, or the cash-versus-futures execution mismatch. They are not confirmatory p-values.

## 14. What the evidence supports—and what it does not

- **Supported descriptively:** positive reference and both stress cases; both sides and all ten setup buckets remain positive under harsh costs; Stage 4 and Stage 9 changes reproduce exactly in the locked composite; cap-two rejected three historically losing third reservations.
- **Not established:** live futures profitability, true out-of-sample accuracy, causal benefit of any new indicator threshold, or profitability of the 107 excluded five-minute candidates and 24 rejected gaps.
- **Main structural risk:** Stage 10 is post-hoc and July/day activity dominate returns. The forward six sessions were part of the selection gate.
- **Main execution risk:** cash-equity bars and cash-equivalent quantities stand in for rolling futures lots, margins, spreads and impact.
- **Main path risk:** 71 exits depend on last-real-bar handling and 231 excursion paths have boundary ambiguity.
- **Main indicator result:** activity/range/liquidity can increase fill probability, but no individual tested feature shows corrected winner/loss separation. Global tightening is unsupported.

## 15. Safe staged improvement plan

### Stage A — freeze and register

1. Keep this exact V11 profile/hash unchanged as the benchmark. Record every future test, including failures, before reading results.
2. Keep V10 Stage 0 and isolated Stage 4 as comparators. Do not use this same history to repeatedly redefine the winner.

### Stage B — repair validity before parameter tuning

1. Reconstruct point-in-time daily universes and rolling front-month futures contracts, with dated lots/ticks/margins.
2. Bind complete futures 1-minute price paths, observed spreads, impact assumptions and exact 15:30 bars; repair 26-Aug separately.
3. Re-run V10/V11 parity on the repaired data. Reject any improvement that exists only in cash-equity proxy execution or partial terminal paths.

### Stage C — genuinely prospective evaluation

1. Freeze thresholds and collect untouched sessions. Report V11, V10 and Stage 4 each day under all three cost cases.
2. Predeclare acceptance gates for PF, net, drawdown, cost robustness and concentration; do not select on a six-session extension already used during development.
3. Use rolling or nested walk-forward selection when enough history exists, leaving a final untouched block.

### Stage D — first entry-quality hypothesis

1. Test only a setup-specific **minute-2 SHORT** reconfirmation/quality rule. The weakness is concentrated in 09:25/09:30 SHORT; a global minute-2 ban discards profitable LONG trades.
2. Prefer a causal relative picker-quality, breadth or liquidity margin over excluding exact frozen rank 3. Rank performance here is non-monotonic.
3. Replay each proposal from the complete pre-overlay candidate stream and preserve setup caps/global ledger ordering. One change per stage.

### Stage E — execution, gap and portfolio tests

1. Keep the current 2 bps gap guard as control. The accepted 1–2 bps bucket is profitable; no evidence supports tightening it now.
2. Keep same-symbol/same-side cap two. Do not raise it based on three rejected candidates, even though all three lost historically.
3. Stress sector/side clustering and actual futures capital/margin capacity. Current peak cash-equivalent notional is materially above modeled capital.

### Stage F — exit research only after path repair

1. Resolve exact terminal bars first because EOD/last-real-bar exits supply a majority of net points.
2. Only then replay predeclared time-stop, break-even or trailing variants. Never filter on realized exit reason, holding duration, MFE or MAE.

### Stage G — decision rule

Promote nothing unless it beats frozen V11 and comparators on untouched/repaired data, stays positive under both stress cases, improves or preserves drawdown, avoids new concentration, and has economically executable futures sizing. Otherwise retain V11 unchanged.

## 16. Reproducibility and supporting evidence

Backtest command used for the sealed result:

```powershell
cd "C:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2"
python -u fno_v11_backtest.py run --all-usable-history
```

Report command:

```powershell
python -u fno_v11_full_historical_report.py --source-run "C:\TradingData\eqidv2\fno_oi\strategy_research\v11_stage10_fixed_full_history_v1\run_20260830T213455896360+0530" --report "C:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2\report_v11.md" --assets-dir "C:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2\report_v11_assets"
```

Supporting tables and charts: `C:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2\report_v11_assets`.
The sealed backtest directory was read and validated but not modified. The report contains **77 CSV tables** and **5 charts**.

Key evidence files include full daily results, all cost/period rows, expanded confirmation checks, all indicator bins/quartiles/tests, setup and symbol tables, component attribution, portfolio timeline, gap rejections, excursions, bootstrap output, and source-coverage tables.

## 17. Glossary and interpretation

- **Net return points:** arithmetic sum of per-trade net percentage returns; it is not compounded portfolio return.
- **PF:** gross positive net-return points divided by absolute gross negative net-return points.
- **MDD:** maximum peak-to-trough drawdown of cumulative daily summed return points unless marked Rs.
- **WR:** winning closed trades divided by closed trades.
- **S+N:** Nth one-minute bar after the 5-minute signal candle closes.
- **MFE/MAE:** favorable/adverse post-entry excursion bounded by available OHLC paths; future information, not an entry feature.
- **BH q-value:** Benjamini–Hochberg multiple-test-adjusted p-value; low q reduces, but does not eliminate, false-discovery risk.
- **Research-only:** useful for hypothesis development; not an authorization or estimate of achievable live returns.
