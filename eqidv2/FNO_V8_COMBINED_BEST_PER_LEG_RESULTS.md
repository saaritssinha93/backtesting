# FNO V8-Combined best-per-leg backtest

## Strategy

`fno_v8_combined_best_per_leg_backtest.py` is an independent, hash-pinned V8
launcher. It uses the train-selected per-leg mapping:

| Slot | LONG | SHORT |
|---|---|---|
| 09:25 | Retuned V8 | Retuned V8 |
| 09:30 | Common | Retuned V8 |
| 09:35 | Common | Common |
| 09:40 | Common | V8-Strict |
| 09:45 | Common | Common |

The literal setup-book SHA-256 is
`ee97e86d3689767df95d1bbe8cb71215cf8a0cb40934f4e08d4c0805d90ee675`.

The launcher has a distinct cache, run, snapshot and provenance namespace at
`C:\TradingData\eqidv2\fno_oi\strategy_research\v8_combined_best_per_leg_v1`.
It imports neither parent strategy launcher.

## 40-session diagnostic result

- Window: 2026-06-24 through 2026-08-19
- Train/test split: 2026-08-06
- Cost/slippage: 15/0 bps
- EOD: `LAST_REAL_BAR_SENSITIVITY`
- Target exposure: approximately Rs50,000 per filled cash-equity position

| Metric | Combined result |
|---|---:|
| Candidates | 890 |
| Closed fills | 184 (4.60/session) |
| Wins / losses | 93 / 91 |
| Win rate | **50.54%** |
| Diagnostic PF | **1.892** |
| Additive net | **+60.16 points** |
| Sizing-proxy net P&L | **+Rs29,890** |
| Maximum cumulative daily drawdown | 6.00 points |
| Positive / negative / flat days | 28 / 12 / 0 |
| Train fills / PF / net | 153 / **1.983** / +53.93 |
| Test fills / PF / net | 31 / **1.494** / +6.23 |

## Comparison at 15 bps

| Strategy | Fills | Win % | PF | Net points | Max DD | Train PF | Test PF |
|---|---:|---:|---:|---:|---:|---:|---:|
| V8-Strict | 136 | 50.74% | 1.885 | +49.81 | 5.23 | 1.888 | **1.870** |
| Retuned V8 | **191** | 49.21% | 1.768 | +55.62 | **4.51** | 1.828 | 1.408 |
| **V8-Combined** | 184 | **50.54%** | **1.892** | **+60.16** | 6.00 | **1.983** | 1.494 |

V8-Combined has the best full diagnostic PF, net and train PF of the three,
but the uplift is selected on the training history. Its test PF is below
V8-Strict and fractionally below the proposed 1.50 floor. It is therefore a
shadow/PAPER research candidate, not a live-production configuration.

## Direct comparison with V6

### Conservative/common 15 bps cost

| Metric | V6 | V8-Combined | Better diagnostic |
|---|---:|---:|---|
| Closed fills | 176 | **184** | V8-Combined |
| Fills/session | 4.40 | **4.60** | V8-Combined |
| Win rate | **52.84%** | 50.54% | V6 |
| PF | **2.009** | 1.892 | V6 |
| Net points | **+72.04** | +60.16 | V6 |
| Sizing-proxy P&L | **+Rs36,234** | +Rs29,890 | V6 |
| Max drawdown | 7.10 | **6.00** | V8-Combined |
| Train PF | **2.159** | 1.983 | V6 |
| Test PF | 1.356 | **1.494** | V8-Combined |

### Common 5 bps cost

| Metric | V6 | V8-Combined | Better diagnostic |
|---|---:|---:|---|
| Closed fills | 176 | **184** | V8-Combined |
| Win rate | **52.84%** | 50.54% | V6 |
| PF | **2.425** | 2.341 | V6 |
| Net points | **+89.64** | +78.56 | V6 |
| Sizing-proxy P&L | **+Rs44,768** | +Rs38,788 | V6 |
| Max drawdown | 5.60 | **4.02** | V8-Combined |
| Train PF | **2.606** | 2.455 | V6 |
| Test PF | 1.633 | **1.848** | V8-Combined |

V6 wins the full-window profitability comparison. V8-Combined has slightly
more fills, lower drawdown and better held-out PF. This remains a diagnostic,
not an exact execution-parity comparison: V6 uses legacy independent-trade,
exact-trigger and longer-lived order assumptions, whereas V8-Combined uses
S+5 expiry, adverse gap fills, brackets from actual modeled fill, same-session
paths and one global duplicate/capital ledger.

## Per-leg combined results

| Leg | Fills | Win % | PF | Net points |
|---|---:|---:|---:|---:|
| 09:25 LONG | 45 | 55.56% | 1.910 | +10.085 |
| 09:25 SHORT | 51 | 43.14% | 1.942 | +17.256 |
| 09:30 LONG | 8 | 62.50% | 3.904 | +8.250 |
| 09:30 SHORT | 21 | 52.38% | 1.515 | +5.387 |
| 09:35 LONG | 17 | 52.94% | 1.103 | +0.847 |
| 09:35 SHORT | 6 | 66.67% | 4.941 | +9.084 |
| 09:40 LONG | 13 | 38.46% | 2.099 | +4.948 |
| 09:40 SHORT | 14 | 42.86% | 1.129 | +1.086 |
| 09:45 LONG | 4 | 50.00% | 1.672 | +0.803 |
| 09:45 SHORT | 5 | 80.00% | 19.872 | +2.416 |

Late-slot PF values are based on very few fills and should not be interpreted
as stable edge.

## Validity and verification

Official headline metrics remain N/A because 3,538 of 8,320 expected
symbol-sessions are incomplete and the result uses last-real-bar sensitivity.
The combined run is not promotion eligible.

- Validated run:
  `C:\TradingData\eqidv2\fno_oi\strategy_research\v8_combined_best_per_leg_v1\runs\fno_v8_vc_20260820T174309351502+0530_af9cdf2ca31b`
- Backtest fingerprint:
  `af9cdf2ca31b830de32a3640bcf8fa0e4bb98c60da18c262ecfdd70da041ec53`
- Common-cost 5 bps run:
  `C:\TradingData\eqidv2\fno_oi\strategy_research\v8_combined_best_per_leg_v1\runs\fno_v8_vc_20260820T193555446060+0530_5f0ee2861cde`
- Common-cost 5 bps fingerprint:
  `5f0ee2861cde72ea8e2f987fe85bbb11bac3c559e4cab4db17c4de884456e13c`
- Focused V8 launcher/engine/data-contract tests: 92 passed.
