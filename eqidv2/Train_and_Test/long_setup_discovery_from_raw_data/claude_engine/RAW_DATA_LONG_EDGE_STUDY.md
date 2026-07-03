# RAW_DATA_LONG_EDGE_STUDY — what precedes a FAST +0.75% LONG pop

Universe = top-250 liquid NSE names. **TRAIN sessions only** (2026-04-30..2026-06-12, 30 sessions) — TEST excluded from discovery.

Signal set = union of 10 raw 5-min LONG family triggers = **112,445** entries (one per ticker×signal-bar, entered next 1-min open). Outcome resolved on 1-min bars (SL-first on same-bar tie).

**'TARGET-first%' = P(+target% touched before −SL%)** — the pure price-path edge (slippage-free). 'fast%' = target hit within ~15 min (≤3 five-min bars). win/PF metrics are NET of statutory cost + slippage.

## 1. Bracket base rates (TRAIN union) + break-even win-rate
| bracket (SL/Tgt) | TARGET-first% | fast≤15m% | SL% | EOD% | net-win%@5bps | PF@5bps | exp@5bps Rs | net-win%@15bps | PF@15bps | breakeven-win%@5bps | @15bps |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 0.75/0.75 | 32.4 | 6.4 | 33.8 | 33.8 | 39.6 | 0.49 | -202.6 | 35.2 | 0.23 | 56.7 | 70.0 |
| 0.50/0.50 | 39.4 | 13.3 | 42.4 | 18.2 | 42.0 | 0.39 | -198.7 | 39.7 | 0.10 | 60.0 | 80.0 |
| 0.60/0.60 | 36.7 | 9.8 | 39.0 | 24.3 | 41.0 | 0.44 | -200.4 | 37.7 | 0.17 | 58.3 | 75.0 |
| 0.75/1.00 | 24.1 | 3.4 | 35.8 | 40.1 | 35.7 | 0.53 | -196.8 | 30.3 | 0.29 | 48.6 | 60.0 |
| 0.50/0.75 | 28.4 | 6.2 | 47.1 | 24.5 | 34.7 | 0.47 | -192.1 | 30.8 | 0.21 | 48.0 | 64.0 |

**Cost reality:** fixed per-leg slippage is a large fraction of a sub-1% target. At 15 bps/leg the break-even win-rate for 0.75/0.75 is ~70% (near-impossible); at a realistic **5 bps/leg for liquid names it is ~57%**. Larger targets (0.75/1.00) carry a LOWER break-even because the fixed cost is a smaller fraction of the target — a key lever the search exploits while staying near the tight theme.

## 2. Per-family raw edge (anchor 0.75/0.75 and 0.75/1.00, TRAIN)
| family | label | n | TARGET-first% (.75/.75) | fast≤15m% | net-win%@5bps | PF@5bps | TARGET-first% (.75/1.0) | PF@5bps(.75/1.0) |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| F1_VWAP_RECLAIM | LONG VWAP Reclaim Momentum | 21,212 | 35.5 | 7.9 | 41.0 | 0.50 | 27.2 | 0.55 |
| F2_PRESSURE_BURST | LONG Pressure Burst Breakout | 64,033 | 33.5 | 6.9 | 40.5 | 0.51 | 25.0 | 0.55 |
| F3_CONSOL_EXPANSION | LONG Consolidation Expansion Breakout | 39,382 | 33.1 | 6.4 | 40.9 | 0.52 | 24.4 | 0.56 |
| F4_FAILED_BREAKDOWN | LONG Failed Breakdown Reversal | 20,016 | 26.6 | 3.3 | 34.7 | 0.41 | 19.3 | 0.44 |
| F5_PULLBACK_CONT | LONG Pullback Continuation | 38,193 | 32.4 | 7.1 | 40.2 | 0.50 | 23.8 | 0.54 |
| F6_VOLUME_EXPANSION | LONG Volume Expansion Breakout | 17,467 | 31.4 | 9.1 | 40.0 | 0.54 | 23.3 | 0.60 |
| F7_TREND_CONT | LONG EMA/VWAP Trend Continuation | 11,752 | 36.4 | 12.4 | 43.3 | 0.55 | 27.3 | 0.58 |
| F8_OPENING_STRENGTH | LONG Opening Strength Continuation | 2,649 | 45.1 | 15.8 | 46.2 | 0.54 | 36.5 | 0.57 |
| F9_MIDDAY_RECLAIM | LONG Midday Reclaim Continuation | 9,977 | 38.9 | 5.9 | 43.6 | 0.52 | 29.0 | 0.56 |
| F10_RANGE_EXPANSION | LONG Range Expansion After Compression | 22,958 | 33.9 | 7.7 | 41.4 | 0.53 | 25.1 | 0.58 |

Best raw target-first families: F8_OPENING_STRENGTH (45%), F9_MIDDAY_RECLAIM (39%), F7_TREND_CONT (36%), F1_VWAP_RECLAIM (35%).

## 3. Feature edges — P(+0.75% before −0.75%) by quintile (TRAIN union)
Monotonic rise/fall across quintiles = a usable LONG threshold. base = overall TARGET-first%.

- **Base TARGET-first% (0.75/0.75) = 32.4%**

| feature | Q1 | Q2 | Q3 | Q4 | Q5 | spread(pp) | direction |
|---|---:|---:|---:|---:|---:|---:|---|
| vol_ratio | 37 | 34 | 32 | 30 | 29 | 8.4 | falling |
| atr_pct | 16 | 26 | 33 | 40 | 47 | 30.5 | rising |
| range_pct | 23 | 28 | 32 | 36 | 43 | 19.1 | rising |
| body_frac | 30 | 32 | 33 | 33 | 34 | 4.2 | rising |
| close_loc | 32 | 33 | 32 | 33 | 32 | 0.9 | falling |
| upper_wick | 32 | 33 | 32 | 33 | 32 | 0.9 | rising |
| lower_wick | 34 | 35 | 33 | 30 | 29 | 6.0 | falling |
| rsi | 32 | 31 | 31 | 32 | 36 | 5.6 | rising |
| adx | 26 | 30 | 33 | 35 | 38 | 11.7 | rising |
| macd_hist | 32 | 29 | 30 | 33 | 38 | 9.5 | rising |
| ema20_slope | 32 | 29 | 31 | 33 | 37 | 7.8 | rising |
| mom2_pct | 27 | 28 | 30 | 35 | 42 | 14.5 | rising |
| mom3_pct | 29 | 27 | 29 | 34 | 43 | 15.2 | rising |
| vwap_dist_atr | 28 | 34 | 35 | 35 | 30 | 6.8 | rising |
| ema20_dist_atr | 31 | 31 | 31 | 33 | 36 | 5.8 | rising |
| compress5_atr | 33 | 31 | 31 | 31 | 33 | 2.3 | falling |
| slot | 45 | 41 | 35 | 29 | 10 | 35.0 | falling |

### Strongest single-feature edges (by quintile spread)
- **slot**: spread 35.0pp, falling; best in Q1 (45% target-first).
- **atr_pct**: spread 30.5pp, rising; best in Q5 (47% target-first).
- **range_pct**: spread 19.1pp, rising; best in Q5 (43% target-first).
- **mom3_pct**: spread 15.2pp, rising; best in Q5 (43% target-first).
- **mom2_pct**: spread 14.5pp, rising; best in Q5 (42% target-first).
- **adx**: spread 11.7pp, rising; best in Q5 (38% target-first).
- **macd_hist**: spread 9.5pp, rising; best in Q5 (38% target-first).
- **vol_ratio**: spread 8.4pp, falling; best in Q1 (37% target-first).

## 4. Time-of-day — fast LONG follow-through (TARGET-first%, 0.75/0.75)
| hour IST | n | TARGET-first% |
|---|---:|---:|
| 09:xx | 10,274 | 44.6 |
| 10:xx | 18,608 | 44.2 |
| 11:xx | 19,797 | 40.2 |
| 12:xx | 18,045 | 35.7 |
| 13:xx | 17,771 | 31.5 |
| 14:xx | 18,519 | 17.9 |
| 15:xx | 9,431 | 3.1 |

By session-slot bucket (5-min bars from open): (0, 3]=45%, (3, 6]=44%, (6, 12]=46%, (12, 24]=43%, (24, 42]=37%, (42, 100]=21%.

## 5. Best / worst symbols for fast LONG follow-through (min 150 signals, TRAIN)
Best 10: CARTRADE(50%/n489), HSCL(50%/n497), MTARTECH(49%/n445), OLAELEC(49%/n414), TEJASNET(48%/n432), BBOX(48%/n457), CPPLUS(46%/n407), SYRMA(46%/n463), ATHERENERG(46%/n459), DIACABS(45%/n469)

Worst 10: OIL(18%/n408), POWERGRID(18%/n455), SBILIFE(18%/n398), BRITANNIA(17%/n448), INFY(16%/n433), NESTLEIND(16%/n484), TCS(15%/n415), HCLTECH(13%/n423), ITC(12%/n429), ONGC(11%/n377)

Day TARGET-first% range across TRAIN: min 13% / median 29% / max 51% (best 2026-06-12, worst 2026-05-12).

## 6. Failed-breakout / overextension patterns (lower target-first% = avoid)
- long upper wick ≥0.40 (rejection): n=14,563, TARGET-first%=32.3 (base 32.4)
- far above VWAP ≥3 ATR (overextended): n=14,791, TARGET-first%=27.8 (base 32.4)
- 3 prior green candles (exhaustion risk): n=15,244, TARGET-first%=36.4 (base 32.4)
- very low ATR% ≤0.15 (no room): n=3,395, TARGET-first%=11.4 (base 32.4)
- very high ATR% ≥0.8 (−0.75% is noise): n=2,202, TARGET-first%=49.3 (base 32.4)
- RSI ≥80 (overbought): n=3,461, TARGET-first%=41.4 (base 32.4)
- weak close (close_loc ≤0.4): n=3,041, TARGET-first%=34.7 (base 32.4)
- below-average volume (<1.0x): n=61,857, TARGET-first%=34.6 (base 32.4)

## 7. Symmetric threshold comparison — P(+X% before −X%) (TRAIN union)
| ±X% | TARGET-first% | fast≤15m% | source |
|---|---:|---:|---|
| ±0.50 | 39.4 | 13.3 | cached |
| ±0.60 | 36.7 | 9.8 | cached |
| ±0.75 | 32.4 | 6.4 | cached |
| ±1.00 | 25.8 | 3.4 | resolved on-the-fly |
| ±1.50 | 15.8 | 1.2 | resolved on-the-fly |

**Read:** tighter targets hit far more often (more fills, as intended) but their break-even win-rate is higher (fixed cost is a bigger fraction). Wider targets convert less but need a lower win-rate to pay. The discovery searches all brackets but anchors on the tight 0.75% theme.

## Design implications (drive Stage 4 rules)
1. Tight symmetric 0.75% on the *raw union* is a coin-flip-minus (base ~32% target-first) — structure alone is not enough; must stack the top feature edges above.
2. Favor the families + feature quintiles with the highest target-first lift (Section 2-3).
3. Avoid overextension (Section 6): long upper wick, far-above-VWAP, exhausted green runs, dead-low ATR.
4. Slot/time matters (Section 4) — bias to the windows with the best fast follow-through.
5. The 0.75/1.00 bracket needs a far lower win-rate to pay; it is the most cost-robust tight variant.