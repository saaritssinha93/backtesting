# V11 Lab Setup Improvement Report - 2026-07-10

## Guardrail

This work is V11 lab/backtesting only. No V7 live/paper trading code, production scanner, production config, or live execution wiring was modified.

## Goal

Improve the V11 lab book so it produces more trades and better average daily P&L without accepting obvious overtrade damage.

Minimum practical gate for a lab candidate:

- More trades/day than the conservative control.
- Net P&L above the conservative control over the full fixed window.
- Last 20, last 10, and last 5 completed sessions not worse than the control.
- Profit factor preferably above 2.5.
- No single lucky day explaining the result.
- Forward shadow for 5-10 trading days before any V7 paper/live discussion.

## Fixed Validation Window

- Mode: V11 `live_parity`
- Candidate source: archived V7 5-minute candidate JSON snapshots
- Dates: `2026-05-21` through `2026-07-10`
- Usable sessions: 35
- Skipped date: `2026-05-28`
- Output folder: `C:\TradingData\eqidv2\backtesting_result_v11\v11_lab_books_confabc_20260710`

## Lab Books Tested

| Book | Purpose | Result |
| --- | --- | --- |
| `final_setup_conf_v11_conf_a` | Conservative control, same as prune2 | Good PF, too sparse |
| `final_setup_conf_v11_conf_b` | Remove B_HUGE time cap | More trades, but weak extra edge |
| `final_setup_conf_v11_conf_c` | Aggressive widening reference | Rejected, overtrades |
| `final_setup_conf_v11_conf_d` | Filtered B_HUGE expansion | Current shadow candidate |

## Window Results

| Book | Window | Trades | Trades/Day | Win % | Net P&L | PF | Avg/Day | Max Losing Day |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| CONF_A | All 35 | 36 | 1.03 | 72.22 | 24,908.79 | 4.051 | 711.68 | -2,095.62 |
| CONF_B | All 35 | 55 | 1.57 | 61.82 | 25,053.19 | 2.735 | 715.81 | -2,350.14 |
| CONF_C | All 35 | 83 | 2.37 | 55.42 | 20,506.37 | 1.758 | 585.90 | -3,009.21 |
| CONF_D | All 35 | 43 | 1.23 | 72.09 | 29,343.64 | 4.157 | 838.39 | -2,095.62 |

| Book | Last 20 Net | Last 10 Net | Last 5 Net |
| --- | ---: | ---: | ---: |
| CONF_A | 14,043.13 | 9,631.35 | 1,826.21 |
| CONF_B | 10,686.38 | 8,721.75 | 3,073.61 |
| CONF_C | 8,783.12 | 6,850.34 | 1,975.13 |
| CONF_D | 14,552.90 | 10,141.11 | 2,890.33 |

## Decision

`CONF_D` is the best V11 lab candidate from this pass.

It improves over CONF_A by:

- Trades: 43 vs 36
- Net P&L: Rs 29,343.64 vs Rs 24,908.79
- Avg/day: Rs 838.39 vs Rs 711.68
- PF: 4.157 vs 4.051
- Last 20: Rs 14,552.90 vs Rs 14,043.13
- Last 10: Rs 10,141.11 vs Rs 9,631.35
- Last 5: Rs 2,890.33 vs Rs 1,826.21
- Max losing day unchanged at Rs -2,095.62

## Exact CONF_D Change

Only `B_HUGE_RED_FAILED_BOUNCE` changed versus CONF_A/prune2.

Old CONF_A B_HUGE mask:

```python
[["signal_minute", "<=", 690]]
```

New CONF_D B_HUGE mask:

```python
[
    ["atr_pct", ">=", 0.00185],
    ["signal_range_pct", ">=", 0.279],
]
```

All other prune2 protections remain unchanged.

## Why This Makes Sense

The loose B_HUGE expansion in CONF_B added 19 trades versus CONF_A, but those extra trades were nearly breakeven: about Rs 144 net, PF near 1.02.

The better version is not "take all later B_HUGE." It is "take B_HUGE only when the failed bounce has real movement behind it." The ATR/range gate keeps the short setup closer to its intended behavior: a meaningful red failure, not a low-energy drift.

## Setup-Level Result

| Book | Side | Setup | Trades | Win % | Net P&L | PF |
| --- | --- | --- | ---: | ---: | ---: | ---: |
| CONF_D | LONG | E_ORB_BREAKOUT_LONG | 9 | 66.67 | 13,479.37 | 5.508 |
| CONF_D | SHORT | B_HUGE_RED_FAILED_BOUNCE | 10 | 70.00 | 5,832.71 | 6.002 |
| CONF_D | SHORT | A_MOD_BREAK_C1_LOW | 16 | 68.75 | 5,622.29 | 2.204 |
| CONF_D | SHORT | C_OR_BREAKDOWN | 6 | 83.33 | 2,809.47 | 6.999 |
| CONF_D | SHORT | G_LOWER_LOW_BREAK | 2 | 100.00 | 1,599.80 | inf |

## Rejected Ideas

`CONF_B` is not enough:

- It produces more trades, but the extra B_HUGE basket is too close to breakeven.
- It lowers PF from 4.051 to 2.735.
- It worsens max drawdown.

`CONF_C` is rejected:

- It adds too many late A_MOD/C_OR shorts.
- Short-side PF drops to 1.292.
- Full-book PF drops to 1.758.
- Last 20 and last 10 are both worse than A and D.

## Forward Monitoring

The V11 lab shadow monitor is now pointed at:

```text
final_setup_conf_v11_conf_d
```

Scheduled wrapper:

```text
bat\run_v11_lab_shadow_monitor.bat
```

Latest report:

```text
C:\TradingData\eqidv2\v11_lab_shadow\latest\latest_v11_lab_shadow_monitor.md
```

Daily ledger:

```text
C:\TradingData\eqidv2\v11_lab_shadow\daily\daily_v11_lab_shadow_results.csv
```

Today smoke result for `2026-07-10`:

- Current V11 baseline: Rs -2,481.43
- CONF_D candidate: Rs +151.48
- Candidate delta: Rs +2,632.91

## Strategy Going Forward

Keep improving setup by setup, but promote nothing directly.

Current active lab stance:

- Keep `E_ORB_BREAKOUT_LONG`; it is the best long contributor, but do not expand weak longs yet.
- Keep `A_MOD_BREAK_C1_LOW`; useful, but late expansion hurt in CONF_C.
- Keep `C_OR_BREAKDOWN`; prune2 time/body protection is important.
- Keep `G_LOWER_LOW_BREAK`; small sample, profitable, keep shadow-size caution.
- Expand `B_HUGE_RED_FAILED_BOUNCE` only through CONF_D's ATR/range quality gate.
- Do not revive `L_DOUBLE_BOTTOM_VWAP`, `G_HIGHER_HIGH_BREAK`, or broad late breakout longs yet.

Promotion path:

1. Track CONF_D for 5-10 live sessions in V11 shadow.
2. Compare daily net P&L, setup P&L, missed winners, and false positives.
3. If forward shadow stays positive and not best-day dependent, run a wider historical validation.
4. Only after that discuss V7 paper parity.
5. No V7 live promotion without explicit approval.
