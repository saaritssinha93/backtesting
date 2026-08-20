# FNO V8 B0 Full-History Detailed Results

## Scope and validity

| Field | Value |
|---|---:|
| Frozen historical window | 2026-05-27 through 2026-08-17 |
| Official sessions | 57 |
| Mapped stock universe | 208 |
| Expected symbol-sessions | 11,856 |
| Source-complete symbol-sessions | 6,350 (53.56%) |
| Incomplete symbol-sessions | 5,506 (46.44%) |
| Exact-15:30 symbol-sessions | 9,568 (80.70%) |
| Unexpected non-calendar sessions | 0 |
| Five-minute candidates observed | 1,298 |
| Cache fingerprint | `8e02e775793334bd9bfeb32fe4c0c458cafea34a57adc1f84cad5fd64b9ab9cf` |

The full-window headline is **invalid** because upstream coverage is incomplete.
All numerical performance below is an available-data diagnostic, not an official
full-history return or a promotion result. Missing whole symbol-days can suppress
candidates entirely; candidate counts therefore do not repair or measure the
coverage deficit.

Only five sessions are source-complete for every mapped symbol: 2026-07-24,
2026-07-27, 2026-07-28, 2026-07-29, and 2026-07-31. Every one of the 208 symbols
has at least one incomplete session.

## Policy comparison

| Policy | Candidates | Fills | Finite closed fills | Incomplete filled paths | Diagnostic PF | Diagnostic net points | Diagnostic net P&L | Positive / negative / flat days | Headline valid |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---|
| Exact 15:30 | 1,298 | 161 | 153 | 8 | 1.521 | +37.795 | +Rs 19,466.06 | 28 / 23 / 6 | No |
| Last-real-bar sensitivity | 1,298 | 161 | 161 | 0 | 1.609 | +44.614 | +Rs 22,823.78 | 30 / 21 / 6 | No |

The sensitivity only replaces a missing trailing session tail with the last real
bar. It does not repair missing full days, internal minutes, confirmation bars,
or futures/OI slots.

## Exact-policy monthly diagnostics

| Month | Sessions | Closed fills | Net points | Net P&L | Positive / negative / flat days |
|---|---:|---:|---:|---:|---:|
| 2026-05 | 2 | 4 | +4.283 | +Rs 2,119.44 | 2 / 0 / 0 |
| 2026-06 | 21 | 38 | +2.646 | +Rs 1,450.64 | 7 / 9 / 5 |
| 2026-07 | 23 | 93 | +33.423 | +Rs 16,917.61 | 17 / 6 / 0 |
| 2026-08 | 11 | 18 | -2.559 | -Rs 1,021.63 | 2 / 8 / 1 |

## Exact-policy side diagnostics

| Side | Candidates | Confirmed | Fills | Closed | Wins | Losses | Net points | Net P&L | PF |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| LONG | 487 | 120 | 69 | 65 | 28 | 37 | +17.429 | +Rs 8,470.51 | 1.614 |
| SHORT | 811 | 224 | 92 | 88 | 41 | 47 | +20.365 | +Rs 10,995.55 | 1.462 |

## Exact-policy setup diagnostics

| Setup | Candidates | Confirmed | Fills | Closed | Wins | Losses | Net points | Net P&L | PF |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 09:25 LONG | 103 | 25 | 18 | 16 | 6 | 10 | +4.932 | +Rs 2,461.62 | 1.813 |
| 09:25 SHORT | 247 | 70 | 36 | 34 | 17 | 17 | +10.407 | +Rs 5,207.53 | 1.677 |
| 09:30 LONG | 58 | 15 | 10 | 10 | 5 | 5 | +6.559 | +Rs 3,226.20 | 2.447 |
| 09:30 SHORT | 266 | 68 | 21 | 20 | 8 | 12 | +3.611 | +Rs 2,185.44 | 1.302 |
| 09:35 LONG | 211 | 39 | 22 | 20 | 8 | 12 | -3.291 | -Rs 1,563.83 | 0.728 |
| 09:35 SHORT | 36 | 15 | 7 | 7 | 4 | 3 | +5.770 | +Rs 2,911.33 | 2.668 |
| 09:40 LONG | 87 | 31 | 13 | 13 | 5 | 8 | +4.940 | +Rs 2,286.82 | 2.098 |
| 09:40 SHORT | 174 | 50 | 17 | 16 | 6 | 10 | +0.013 | +Rs 277.69 | 1.001 |
| 09:45 LONG | 28 | 10 | 6 | 6 | 4 | 2 | +4.289 | +Rs 2,059.70 | 4.589 |
| 09:45 SHORT | 88 | 21 | 11 | 11 | 6 | 5 | +0.565 | +Rs 413.56 | 1.152 |

## Exact-policy signal-slot diagnostics

| Slot | Candidates | Confirmed | Fills | Closed | Wins | Losses | Net points | Net P&L | PF |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 09:25 | 350 | 95 | 54 | 50 | 23 | 27 | +15.340 | +Rs 7,669.15 | 1.715 |
| 09:30 | 324 | 83 | 31 | 30 | 13 | 17 | +10.169 | +Rs 5,411.64 | 1.617 |
| 09:35 | 247 | 54 | 29 | 27 | 12 | 15 | +2.479 | +Rs 1,347.50 | 1.159 |
| 09:40 | 261 | 81 | 30 | 29 | 11 | 18 | +4.953 | +Rs 2,564.51 | 1.351 |
| 09:45 | 116 | 31 | 17 | 17 | 10 | 7 | +4.854 | +Rs 2,473.26 | 1.990 |

## Exact-policy exit diagnostics

| Exit | Trades | Wins | Losses | Net points | Net P&L | PF |
|---|---:|---:|---:|---:|---:|---:|
| Target | 26 | 26 | 0 | +69.482 | +Rs 34,124.03 | infinite |
| Square-off | 59 | 43 | 16 | +35.117 | +Rs 17,275.94 | 7.156 |
| Stop | 66 | 0 | 66 | -64.442 | -Rs 30,793.02 | 0 |
| Stop gap | 2 | 0 | 2 | -2.363 | -Rs 1,140.89 | 0 |

## Largest exact-policy closed trades

| Date | Side | Setup | Symbol | Exit | Net P&L |
|---|---|---|---|---|---:|
| 2026-07-23 | SHORT | 09:25 | VMM | Target | +Rs 1,423.25 |
| 2026-06-30 | SHORT | 09:30 | KPITTECH | Target | +Rs 1,422.62 |
| 2026-07-07 | SHORT | 09:35 | MOTHERSON | Target | +Rs 1,422.30 |
| 2026-07-15 | LONG | 09:25 | ETERNAL | Target | +Rs 1,421.41 |
| 2026-07-22 | SHORT | 09:35 | BANDHANBNK | Target | +Rs 1,421.17 |

## Largest exact-policy losses

| Date | Side | Setup | Symbol | Exit | Net P&L |
|---|---|---|---|---|---:|
| 2026-07-16 | LONG | 09:35 | BPCL | Stop | -Rs 580.33 |
| 2026-07-15 | SHORT | 09:40 | PATANJALI | Stop | -Rs 579.16 |
| 2026-07-14 | SHORT | 09:40 | ABCAPITAL | Stop | -Rs 578.91 |
| 2026-07-29 | LONG | 09:35 | COFORGE | Stop gap | -Rs 577.75 |
| 2026-08-12 | SHORT | 09:30 | GODREJCP | Stop | -Rs 576.98 |

## Artifacts

- Exact-policy run: `C:\TradingData\eqidv2\fno_oi\strategy_research\v8_windowed_strict_v1\runs\fno_v8_b0_20260819T082957669962+0530_08edbed07a67`
- Last-real sensitivity: `C:\TradingData\eqidv2\fno_oi\strategy_research\v8_windowed_strict_v1\runs\fno_v8_b0_20260819T083017708654+0530_221a90fd9824`
- Exact-policy detailed candidate audit: `candidate_order_audit.csv` in the exact run directory.
- Exact-policy daily table: `daily.csv` in the exact run directory.
- Exact-policy long-form diagnostic table: `diagnostic_breakdowns.csv` in the exact run directory.
- Full cache: `C:\TradingData\eqidv2\fno_oi\strategy_research\v8_windowed_strict_v1\cache\8e02e775793334bd`

Both run provenance artifacts passed the V8 validator. The cash-equity sizing
proxy uses Rs 50,000 target exposure per filled trade, 15-bps round-trip costs,
zero added slippage, and the configured global pending-margin/duplicate-symbol
portfolio overlay. Additive net points are not account returns.
