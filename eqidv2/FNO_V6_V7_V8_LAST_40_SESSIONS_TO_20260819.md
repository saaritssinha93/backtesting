# FnO V6, V7 and V8 — last 40 sessions through 2026-08-19

## Scope

- Sessions: 40 official NSE F&O sessions, 2026-06-24 through 2026-08-19.
- Train diagnostic: 30 sessions, 2026-06-24 through 2026-08-05.
- Held-out replay diagnostic: 10 sessions, 2026-08-06 through 2026-08-19.
- Universe: fixed later-dated mapped universe of 208 stocks; August futures provide OI, NSE cash equity provides all execution prices.
- Sources: immutable physical snapshot fingerprint `6734204d53315d386a2c3949f01b272d4399a8a1d3b44b9cfd556a1b859190cc`.

## Critical validity warning

None of these figures is promotion-grade performance. Only 4,782 of 8,320 expected symbol-sessions are complete under V8's strict source contract: 3,538 are incomplete (42.52%). Every one of the 208 symbols has at least one incomplete session in this window. V6/V7 use the same source snapshot but their legacy engines do not headline-gate missing symbol-sessions, so their figures are also availability-biased diagnostics.

V8 correctly sets its official PF, return, P&L and drawdown to N/A. Numeric V8 figures below are explicitly its `diagnostic_closed_trade_metrics` under `LAST_REAL_BAR_SENSITIVITY`.

## Common-cost comparison — 5 bps

| Strategy | Fills | Fills/session | Wins/losses | PF | Net points | P&L proxy | Max DD | Positive days |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| V6 strict | 176 | 4.40 | 93/83 | **2.425** | **+89.64** | **+₹44,768** | 5.60 | 25/40 |
| V7 naked S+1 high/low | **298** | **7.45** | 137/161 | 1.655 | +83.64 | +₹40,506 | 8.48 | **26/40** |
| V8 B0 windowed | 191 | 4.78 | 94/97 | 2.187 | +74.72 | +₹36,751 | **3.10** | **26/40** |

Interpretation: V6 has the best full-window PF and net return. V7 generates 69% more fills than V6 but has lower PF and about 51% more drawdown. V8 produces slightly more trades than V6, with lower PF/net but the smallest drawdown.

## Train versus held-out replay — 5 bps

| Strategy | Train fills | Train PF | Train net | Test fills | Test PF | Test net |
|---|---:|---:|---:|---:|---:|---:|
| V6 | 149 | **2.606** | **+82.21** | 27 | 1.633 | +7.43 |
| V7 | **234** | 1.523 | +53.75 | **64** | **2.207** | **+29.89** |
| V8 | 164 | 2.255 | +67.82 | 27 | 1.776 | +6.90 |

V7 is strongest in the final 10 sessions, but that segment is small and source-incomplete. V6 and V8 are stronger in the 30-session train segment. No settings were tuned against the 10-session segment during this comparison.

## Common conservative cost stress — 15 bps

V8's native research cost is 15 bps. V6/V7 below are deterministic cost sensitivities: their selected trades and native exact-trigger mechanics are unchanged, with an additional 10 bps deducted per filled trade.

| Strategy | Fills | PF | Net points | P&L proxy | Max DD | Train PF/net | Test PF/net |
|---|---:|---:|---:|---:|---:|---|---|
| V6 | 176 | **2.009** | **+72.04** | **+₹36,234** | 7.10 | 2.159 / +67.31 | 1.356 / +4.73 |
| V7 | **298** | 1.374 | +53.84 | +₹26,129 | 15.08 | 1.262 / +30.35 | **1.838 / +23.49** |
| V8 native | 191 | 1.768 | +55.62 | +₹27,500 | **4.51** | 1.828 / +51.42 | 1.408 / +4.20 |

## LONG/SHORT breakdown — 5 bps

| Strategy | LONG fills | LONG PF | LONG net | SHORT fills | SHORT PF | SHORT net |
|---|---:|---:|---:|---:|---:|---:|
| V6 | 77 | 2.201 | +33.20 | 99 | **2.600** | **+56.44** |
| V7 | **126** | 1.745 | **+37.42** | **172** | 1.597 | +46.22 |
| V8 | 87 | **2.411** | +33.63 | 104 | 2.051 | +41.09 |

## Held-out replay LONG/SHORT — 5 bps

| Strategy | LONG fills/PF/net | SHORT fills/PF/net |
|---|---|---|
| V6 | 15 / 1.993 / +6.10 | 12 / 1.238 / +1.33 |
| V7 | 31 / **2.702** / **+20.38** | 33 / **1.744** / **+9.51** |
| V8 | 17 / 2.165 / +5.90 | 10 / 1.263 / +1.01 |

V8's held-out short side is weak: PF 1.263 at 5 bps and approximately 1.001 at its native 15 bps. V7's recent side results are stronger but must not be promoted from ten incomplete sessions.

## V8 candidate funnel — native 15 bps

| Item | Count |
|---|---:|
| Five-minute candidates entering V8 audit | 822 |
| Finite closed fills | 191 |
| No confirmation | 476 |
| Window expired | 59 |
| Post-confirmation cancelled | 49 |
| Pre-confirmation invalidated | 23 |
| Duplicate rejected | 24 |
| Stopped | 84 |
| Targeted | 46 |
| Last-real sensitivity square-off | 61 |
| Unresolved filled trades | 0 |

## Source coverage

| Coverage item | Result |
|---|---:|
| Expected symbol-sessions | 8,320 |
| Complete symbol-sessions | 4,782 (57.48%) |
| Incomplete symbol-sessions | 3,538 (42.52%) |
| Symbol-sessions with an exact 15:30 bar | 5,616 (67.50%) |
| Symbols affected by some incompleteness | 208/208 |

## Strategy mechanics are not identical

- V6: strict exact S+1 direction/displacement plus setup body/wick morphology; later confirmation high/low trigger; no finite entry expiry; 5 bps.
- V7: any valid positive-range exact S+1 candle; later high/low trigger; V6 five-minute setup book, ranking, caps and brackets; no finite entry expiry; 5 bps.
- V8 B0: independent chronological engine with current per-leg overrides, finite confirmation/entry windows, tick/buffer rounding, post-confirmation cancellation, adverse gap fills, brackets from actual fill, portfolio/duplicate controls and native 15 bps.

The 15 bps sensitivity equalizes only cost, not these execution mechanics.

## Evidence-backed conclusion

- Best full-window quality: **V6** — highest PF and net return at both 5 and 15 bps.
- Highest trade count and strongest recent 10-session diagnostic: **V7** — but with lower full-window PF and materially higher drawdown.
- Best risk/engine honesty balance: **V8** — lowest drawdown and strongest fail-closed reporting, but lower full-window net than V6 and weak recent SHORT performance.
- Promotion decision: **none**. The fixed later-dated universe, static August-futures OI, legacy row lineage and 42.52% incomplete symbol-session panel prevent an honest efficacy claim.

## Reproducibility

- V7 corrected same-session run provenance: `C:\TradingData\eqidv2\fno_oi\strategy_research\backtest_provenance\fno_v7_extreme_break_20260820T142050287674+0530_2b8f9893bea5.json`
- V7 cache input fingerprint: `3feaa392006b3aa493bd12bfb0d7dcdfec93ff19643b0e61c9b1a7c8a945c079`
- V8 native 15 bps run: `C:\TradingData\eqidv2\fno_oi\strategy_research\v8_windowed_strict_v1\runs\fno_v8_b0_20260820T152418821263+0530_fa078c5dbf97`
- V8 native run validation fingerprint: `fa078c5dbf97e25d5f3c9cbdde24ce57f96944b8601f024201b2b265337d394c`
- V8 5 bps comparison run: `C:\TradingData\eqidv2\fno_oi\strategy_research\v8_windowed_strict_v1\runs\fno_v8_b0_20260820T152530715084+0530_f3c70bcd1a3a`
- V8 5 bps validation fingerprint: `f3c70bcd1a3a4aa9f86e6850d1055e2b04fadcd01abfd77114b503f14bf80c8d`
