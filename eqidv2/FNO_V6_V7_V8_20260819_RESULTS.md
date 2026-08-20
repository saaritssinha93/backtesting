# FnO V6, V7 and V8 backtest results — 2026-08-19

## Verdict

These are **research sensitivity results through the last real cash-equity bar at 15:15 IST**. They are not valid official-15:30 headline results: none of the 208 fixed-universe symbols has a real 15:30 cash-equity bar for 2026-08-19. The local 15:20/15:25/15:30 rows are synthetic, zero-volume gap fills and were excluded.

At the common 5 bps cost assumption, V6 performed best on the available path. V8 was close behind. V7 lost money because three of its four relaxed entries stopped out.

| Strategy/view | Pre-entry candidates | Confirmed/armed | Fills | Wins / losses | PF | Additive net return | Cash-equity sizing proxy P&L |
|---|---:|---:|---:|---:|---:|---:|---:|
| V6 native rule, 5 bps | — | 2 | 2 | 2 / 0 | Infinity | +1.8780 points | +₹919.01 |
| V7 native rule, 5 bps | — | 4 | 4 | 1 / 3 | 0.0464 | -2.2886 points | -₹1,134.58 |
| V8 B0, common-cost view, 5 bps | 6 | 2 | 2 | 2 / 0 | Infinity | +1.8372 points | +₹900.62 |
| V8 B0, native research cost, 15 bps | 6 | 2 | 2 | 1 / 1 | 187.356 | +1.6372 points | +₹811.45 |

“Additive net return” is the sum of independent trade percentage returns, not an account return. V6/V7 rupee P&L uses the same comparison proxy as V8: `floor(₹50,000 / entry price)` cash-equity units. It is not futures-lot P&L.

## V6 detailed results — native strategy, 5 bps

V6 requires the exact S+1 candle to confirm direction and displacement, then applies its setup-specific body/wick filter. Entry is a later touch of that confirmation candle's high/low. Both selected shorts filled at 09:27 and remained open through the last real bar.

| Symbol | Side | Signal | Confirmation | Entry | Stop | Target | Exit | Exit reason | Qty proxy | Net return | Net P&L |
|---|---|---:|---:|---:|---:|---:|---:|---|---:|---:|---:|
| COLPAL | SHORT | 09:25 | 09:26 | 09:27 @ 1,904.60 | 1,918.88 | 1,847.46 | 15:15 @ 1,870.00 | Last real bar | 26 | +1.7667% | +₹874.84 |
| SOLARINDS | SHORT | 09:25 | 09:26 | 09:27 @ 19,832.00 | 19,980.74 | 19,237.04 | 15:15 @ 19,800.00 | Last real bar | 2 | +0.1114% | +₹44.17 |
| **Total** |  |  |  |  |  |  |  |  |  | **+1.8780 points** | **+₹919.01** |

## V7 detailed results — native strategy, 5 bps

V7 keeps V6's five-minute filters, setup ranking, caps and brackets, but accepts any valid positive-range S+1 candle and uses its directional high/low as the later stop-entry trigger. This admitted two additional losing POLICYBZR longs and the losing GVT&D short.

| Symbol | Side | Signal | Confirmation | Entry | Stop | Target | Exit | Exit reason | Qty proxy | Net return | Net P&L |
|---|---|---:|---:|---:|---:|---:|---:|---|---:|---:|---:|
| GVT&D | SHORT | 09:25 | 09:26 | 09:32 @ 4,060.30 | 4,090.75 | 3,938.49 | 10:20 @ 4,090.75 | Stop | 12 | -0.8000% | -₹389.79 |
| SOLARINDS | SHORT | 09:25 | 09:26 | 09:27 @ 19,832.00 | 19,980.74 | 19,237.04 | 15:15 @ 19,800.00 | Last real bar | 2 | +0.1114% | +₹44.17 |
| POLICYBZR | LONG | 09:35 | 09:36 | 09:37 @ 1,779.90 | 1,762.10 | 1,824.40 | 10:01 @ 1,762.10 | Stop | 28 | -1.0500% | -₹523.29 |
| POLICYBZR | LONG | 09:40 | 09:41 | 09:44 @ 1,789.00 | 1,780.06 | 1,833.73 | 09:46 @ 1,780.06 | Stop | 27 | -0.5500% | -₹265.67 |
| **Total** |  |  |  |  |  |  |  |  |  | **-2.2886 points** | **-₹1,134.58** |

## V8 B0 detailed results

V8 is an independent chronological engine. The current setup book is not a literal V6/V7 clone: it has per-leg changes to caps, pickers, morphology, brackets and entry parameters. For the selected 09:25 shorts it used a 2 bps breakdown buffer, tick rounding, actual-fill brackets, a finite window and the portfolio ledger.

| Symbol | Side | Signal | Confirmation | Entry | Stop | Target | Exit | Exit reason | Qty | Net @ 5 bps | P&L @ 5 bps | Net @ 15 bps | P&L @ 15 bps |
|---|---|---:|---:|---:|---:|---:|---:|---|---:|---:|---:|---:|---:|
| SOLARINDS | SHORT | 09:25 | S+1, 09:26 | 09:27 @ 19,828.00 | 19,928.00 | 19,234.00 | 15:15 @ 19,800.00 | Last real bar | 2 | +0.0912% | +₹36.17 | -0.0088% | -₹3.48 |
| COLPAL | SHORT | 09:25 | S+1, 09:26 | 09:27 @ 1,904.20 | 1,913.80 | 1,847.10 | 15:15 @ 1,870.00 | Last real bar | 26 | +1.7460% | +₹864.45 | +1.6460% | +₹814.94 |
| **Total** |  |  |  |  |  |  |  |  |  | **+1.8372 points** | **+₹900.62** | **+1.6372 points** | **+₹811.45** |

V8 non-entries:

| Symbol/setup | Final status | Reason |
|---|---|---|
| GVT&D 09:25 SHORT | No confirmation | Confirmation window expired |
| VOLTAS 09:25 SHORT | No confirmation | Confirmation window expired |
| POLICYBZR 09:35 LONG | No confirmation | Confirmation window expired |
| POLICYBZR 09:40 LONG | No confirmation | Confirmation window expired |

## Source coverage and validity

| Check | Result |
|---|---:|
| Fixed mapped stock universe | 208 symbols |
| Equity sources with 2026-08-19 data | 206/208 |
| Equity rows for each covered symbol | 360, exact end labels 09:16–15:15 |
| Symbols with no 2026-08-19 equity rows | IDEA, LTM |
| Futures 5-minute coverage | 208/208, 75 bars through 15:30 |
| Symbols with a real equity 15:30 bar | 0/208 |
| Headline-complete symbol-sessions | 0/208 |

The absent 15:30 cash data matters most to positions still open at 15:15: V6's two fills, V7's SOLARINDS fill, and both V8 fills. Their displayed last-real-bar exits can change if genuine 15:16–15:30 data is later repaired. The three V7 stop exits occurred earlier and do not depend on the missing tail.

## Reproducibility records

- Frozen physical source snapshot: `C:\TradingData\eqidv2\fno_oi\strategy_research\v8_windowed_strict_v1\snapshots\snapshot_20260820T124734626995+0530_mnofor_c\manifest.json`
- Snapshot fingerprint: `6734204d53315d386a2c3949f01b272d4399a8a1d3b44b9cfd556a1b859190cc`
- Corrected V7 same-session cache input fingerprint: `a46e95dce8e8958c582430bbf88852aec5259ada5818df6b2aeb3ccd3d513cd7`
- V7 immutable run provenance: `C:\TradingData\eqidv2\fno_oi\strategy_research\backtest_provenance\fno_v7_extreme_break_20260820T131507325596+0530_8906d9d961da.json`
- V8 5 bps run: `C:\TradingData\eqidv2\fno_oi\strategy_research\v8_windowed_strict_v1\runs\fno_v8_b0_20260820T130425252068+0530_c1f135e87fba`
- V8 15 bps run: `C:\TradingData\eqidv2\fno_oi\strategy_research\v8_windowed_strict_v1\runs\fno_v8_b0_20260820T130724417621+0530_ac61b71740cf`

The shared legacy V6/V7 forward-path builder was repaired before this run to enforce both `path date == signal date` and `time <= square-off`; the cache-policy fingerprint was bumped, so the old cross-session arrays were not reused. V6 was evaluated from the same immutable corrected broad cache using its original strict direction/morphology policy and setup book. The mutable live-root V6 rebuild was deliberately abandoned when its end-of-build source fingerprint detected concurrent source changes.

## Interpretation

For this one incomplete session, V6's stricter confirmation avoided the three losing trades admitted by V7. V8 also avoided them, while its 2 bps buffered trigger and different bracket configuration slightly changed both winning entries. This is useful day-level evidence, not enough evidence to choose or promote a strategy.
