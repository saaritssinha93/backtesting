# v17r_nonf — Setups Catalog

All long/short setups that exist in or have been tested against the v17r_nonf pipeline.
Honest PF figures use v17D per-row costs (ADV-bucket aware), 5x leverage, sizing-aware,
TGT 1.5% / SL 0.75% exit geometry, breadth gate (loose) where noted.

Last updated: 2026-05-16. Honest test window: 2026-02-05 to 2026-05-05 (3 months).

Legend:
- **LIVE** — currently in the production whitelist of `avwap_combined_runner_v17r_nonf_5min.py`.
- **CASCADE-DORMANT** — exists in the v16/v17 cascade but suppressed by ADV gate, V17M cleanup, or keep-chain whitelist.
- **TESTED-NEW** — new setup designed and scanned in `_v17r_nonf_advsetups_*.py`; result recorded.
- **CONCEPT** — described but not yet implemented or tested.

---

## 1. LIVE — currently shipping in v17r_nonf

| Side | Setup | Trigger / Detection Logic | Strategy / Why It Works | Filter Chain (keep) | Honest PF (3mo / 11mo) |
|---|---|---|---|---|---|
| SHORT | A_MOD_BREAK_C1_LOW | Moderate-impulse bar breaks below the prior bar's low (lag-1 entry) | Continuation short on intraday weakness; structural lower-low confirms supply | `adx >= 19.12 AND rsi >= 23.22 AND atr_pct <= 0.0063` | **2.46 / 1.54** |
| SHORT | D_AVWAP_LOSE_REVERSAL | Price loses session AVWAP from above and reverses lower | Loss of intraday mean = institutional sellers winning; reversal continuation | `quality_score >= 0.7904` | **1.98 / 3.41** |
| LONG | A_MOD_BREAK_C1_HIGH | Moderate-impulse bar breaks above the prior bar's high (lag-1 entry) | Continuation long on intraday strength; structural higher-high confirms demand | `quality_score >= 7.104` | **1.34 / 1.55** |

Aggregate live config: 3-month honest PF **2.07** (n=47, win 51%, MaxDD 16%) · 11-month PF **1.78** (n=151, OOS PF 3.26, decay 2.16, MaxDD 16%).

---

## 2. CASCADE-DORMANT — exist in v16/v17 but currently suppressed

These setups are coded in the cascade but produce zero v17r_nonf trades because either (a) the ADV gate drops their long_tail majority, or (b) V17M low-win-cleanup eliminates them entirely, or (c) they are not on the keep-chain whitelist.

### LONG side

| Setup | Trigger / Detection Logic | Strategy / Why It Should Work | Why It's Dormant | Honest PF (mined, old geom) |
|---|---|---|---|---|
| A_MOD_CLOSE_CONTINUATION_BREAK | Moderate-impulse bar closes near high, next bar breaks the high | Two-bar confirmation of trend continuation | Filter chain on broken-run CSV gave PF 0.99 at 55 trades (OLD geom) | 0.99 → re-tested at new geom: PF 1.63 IS, 0.65 OOS (overfit; rejected) |
| B_HUGE_C1_CLOSE_RECLAIM_BREAK | Huge-impulse candle, prior close reclaimed, then break of that level | Reclaim of broken support = supply absorbed | Lab PF 1.24, OOS PF 0.91, decay 0.68 — failed gates | 1.24 |
| B_HUGE_PULLBACK_HOLD_BREAK | Huge candle then pullback holds prior support, then breaks higher | Bull flag on outsized impulse | Not produced by current cascade scan in noNF mode | n/a |
| B_HUGE_GREEN_PULLBACK_HOLD_THEN_BREAK | Green huge candle + pullback hold + break of pullback high | Bull flag variant — explicit green-candle origin | Not produced in current scan | n/a |
| B_AVWAP_RECLAIM_REVERSAL | Price below AVWAP, reclaims it with bullish bar, reverses higher | AVWAP reclaim = intraday mean shift back to bulls | Not produced in current scan (lag config) | n/a |
| A_PULLBACK_C2_THEN_BREAK_C2_HIGH | After moderate impulse, second candle pulls back then breaks prior high | Pullback continuation in micro-trend | Only n=3 trades produced; sample too small | 0.20 |
| C_OR_BREAKOUT | Price breaks above the opening range high | Opening-range expansion = direction set for day | Killed by V17M low-win cleanup (was PF 0.47 at old geom) | 0.47 |
| D_EMA20_BOUNCE | Price pulls back to EMA20 and bounces (bullish bar at EMA20) | Mean-reversion to short-term trend support | Killed by V17M cleanup (was PF 0.59 at old geom); MAY come alive at TGT 1.5% | 0.59 — UNTESTED at new geom |
| G_HIGHER_HIGH_BREAK | Price breaks a multi-bar swing high | Structural higher-high = trend confirmation | Killed by V17M cleanup (was PF 0.58); MAY come alive at TGT 1.5% | 0.58 — UNTESTED at new geom |

### SHORT side

| Setup | Trigger / Detection Logic | Strategy / Why It Should Work | Why It's Dormant | Honest PF (mined, old geom) |
|---|---|---|---|---|
| C_OR_BREAKDOWN | Price breaks below the opening range low | Opening-range failure = direction set down | Lab found PF 1.16 with strict chain; live re-run delivered PF 0.94 (n=17) | 1.16 (lab) / 0.94 (live) |
| C_SHORT_CONTINUATION_BREAK | Short-side continuation break of prior pivot low | Lower-pivot break confirms supply | Not produced in noNF mode | n/a |
| D_EMA20_REJECTION | Price bounces to EMA20 and rejects (bearish bar at EMA20) | Mean-reversion failure in downtrend | Lab PF 0.66 even filtered; failed gates | 0.66 |
| E_VWAP_BAND_FADE | Price tags upper Bollinger / VWAP band and fades | Band touch = stretched intraday move; fade the extension | Lab PF 1.49 with tight chain; live re-run PF 1.13 (n=38, marginal) | 1.49 (lab) / 1.13 (live) |
| G_LOWER_LOW_BREAK | Price breaks a multi-bar swing low | Structural lower-low = downtrend confirmation | Lab PF 0.68; no chain rescues | 0.68 |
| B_HUGE_FAILED_BOUNCE | Huge candle bottom, weak bounce attempt fails, breakdown | Failed buyer rally = supply still in control | Not produced in current scan | n/a |
| B_HUGE_RED_FAILED_BOUNCE | Red huge candle + bounce attempt + bearish rejection | Distribution short on confirmed supply | n=15 produced, PF 0.28; too small / too weak | 0.28 |
| A_PULLBACK_C2_THEN_BREAK_C2_LOW | Mirror of long pullback-break: bounce then break prior low | Bear flag short | Not produced in current scan | n/a |
| H_FAILED_BREAKOUT_TRAP | Price breaks resistance then fails back below | Bull trap reversal — late buyers stuck | Not produced in current scan | n/a |

---

## 3. TESTED-NEW v1 — designed using base 5-min indicators (Feb-Apr 2026 scan)

Logic source: `_v17r_nonf_advsetups_scan.py`. All re-resolved at TGT 1.5%/SL 0.75% with breadth gate.

| Side | Setup | Trigger / Detection Logic | Strategy / Story | n | PF | Win% | Verdict |
|---|---|---|---|--:|--:|--:|---|
| LONG | L_MACD_BULL_VWAP | MACD bull cross above zero + close > VWAP > EMA20 > EMA50 + ADX≥25 + RSI 55-68 + bullish body | Trend-aligned MACD momentum kick; multi-EMA stack confirms regime | 9 | 0.60 | 33% | REJECT — n too thin |
| LONG | L_BB_SQUEEZE_LONG | BB width ≤ 40% of 100-bar mean for 5 bars, then close > UB×1.003 + vol_ratio ≥ 2.0 + body_eff ≥ 0.65 | Volatility compression → expansion = breakout from coil | 35 | 0.82 | 31% | REJECT |
| LONG | L_TREND_PULLBACK | EMA20>EMA50>EMA200 stacked + pullback within 0.20 ATR of EMA20 + bullish reversal bar | Pullback in clean uptrend buys the dip at trend support | 528 | 0.59 | 20% | REJECT — chain-mine: no rescue |
| SHORT | S_MACD_BEAR_VWAP | MACD bear cross below zero + close < VWAP < EMA20 < EMA50 + ADX≥25 + RSI 32-45 + bearish body | Trend-aligned bear momentum kick | 0 | — | — | NEVER FIRED |
| SHORT | S_BB_SQUEEZE_SHORT | BB squeeze → break below LB×0.997 + vol_ratio ≥ 2.0 + body_eff ≥ 0.65 | Compression → expansion downside | 11 | 2.55 | 27% | INTERESTING but n=11 too thin |
| SHORT | S_TREND_REJECT | EMA20<EMA50<EMA200 stacked down + bounce to within 0.20 ATR of EMA20 + bearish rejection bar | Mean-reversion failure at EMA20 in downtrend | 315 | 0.50 | 16% | REJECT — chain-mine: no rescue |

---

## 4. TESTED-NEW v2 — wider indicator universe (MFI/CCI/OBV/pressure/sweeps/gap-fills)

Logic source: `_v17r_nonf_advsetups_v2.py`. Same exit geometry + breadth gate. Chain-mined per setup with full feature suite.

### LONG side

| Setup | Trigger / Detection Logic | Strategy / Story | n | PF | Win% | Best Filtered |
|---|---|---|--:|--:|--:|---|
| L_MFI_OVERSOLD_RECLAIM | MFI was < 25 within 3 bars, now > 35 + close > VWAP + lower wick > 20% + ADX ≥ 18 + vol_ratio ≥ 1.3 | Volume-confirmed oversold capitulation, reclaimed = absorption complete | 51 | 0.37 | 14% | 0.60 — REJECT |
| L_CCI_EXTREME_FLIP | CCI < -150 within 3 bars, now > -80 + close > EMA20 + body_eff ≥ 0.55 + ADX ≥ 18 + RSI ≥ 40 | CCI extreme + flip = momentum exhaustion, reversal | 2403 | 0.46 | 19% | no rescue — REJECT |
| L_DOUBLE_BOTTOM_VWAP | Current low within 0.4 ATR of intraday low 8 bars ago + held above + close > VWAP + vol surge | Double bottom at intraday support = buyers defending level | 531 | 0.50 | 19% | 0.58 — REJECT |
| L_PRESSURE_BURST_VWAP | buy_pressure_5 / sell_pressure_5 ≥ 3.0 + close > VWAP > EMA20 + vol_z ≥ 1.5 + RSI 50-75 | Directional volume surge = institutional buyers active | 1006 | 0.62 | 22% | no rescue — REJECT |
| L_PREV_DAY_LOW_SWEEP | Low wicked below prev day low then close reclaimed + lower wick > 25% + vol_ratio ≥ 1.5 | Liquidity sweep below resting stops, then rejection = institutional accumulation | 184 | 0.69 | 26% | filtered → 1.99 (n=30, IS=0 — overfit) |
| L_GAP_DOWN_REVERSAL | Day opened below prev close, made intraday lower low, now close > prev close + bullish body | Gap-down reversal day = strong buyer return | 1746 | 0.55 | 24% | no rescue — REJECT |

### SHORT side

| Setup | Trigger / Detection Logic | Strategy / Story | n | PF | Win% | Best Filtered |
|---|---|---|--:|--:|--:|---|
| S_MFI_OVERBOUGHT_FAIL | MFI was > 75, now < 65 + close < VWAP + upper wick > 20% + ADX ≥ 18 + vol_ratio ≥ 1.3 | Volume-confirmed distribution at top, VWAP loss confirms | 58 | 0.38 | 16% | 0.67 — REJECT |
| S_CCI_EXTREME_FLIP | CCI > 150 within 3 bars, now < 80 + close < EMA20 + body_eff ≥ 0.55 + RSI ≤ 60 | Momentum exhaustion at extreme, reversal | 1344 | 0.58 | 22% | no rescue — REJECT |
| S_DOUBLE_TOP_VWAP | Current high within 0.4 ATR of intraday high 8 bars ago + close < VWAP + vol surge | Double top at intraday resistance = sellers defending | 408 | 0.54 | 19% | no rescue — REJECT |
| S_PRESSURE_DUMP_VWAP | buy_pressure_5 / sell_pressure_5 ≤ 0.33 + close < VWAP < EMA20 + vol_z ≥ 1.5 + RSI 25-50 | Directional sell volume surge = distribution | 810 | 0.52 | 15% | no rescue — REJECT |
| S_PREV_DAY_HIGH_FAIL | High wicked above prev day high then close failed back below + upper wick > 25% + vol surge | Liquidity sweep above stops, then rejection = supply trap | 101 | 0.59 | 23% | filtered → 1.87 (n=21, OOS PF 1.17) — borderline |
| S_GAP_UP_REJECTION | Day opened above prev close, intraday made higher high, now close < prev close + bearish body | Gap-up rejection day = sellers reassert | 678 | 0.52 | 23% | no rescue — REJECT |
| S_MACD_HIST_FLIP | MACD_Hist flipped negative + MACD still > 0 (early bearish divergence) + close < VWAP + RSI ≤ 55 | Momentum top forming, anticipates trend change | 952 | 0.51 | 19% | no rescue — REJECT |

---

## 5. CONCEPT — described but not implemented or untested

These are ideas with clear logical basis. Most overlap with what `advanced_indicators_calculator.py` already defines; could be ported into the runner pipeline as a future exercise.

### LONG side

| Setup | Trigger / Detection Logic | Strategy / Story | Source |
|---|---|---|---|
| C_COMPRESSION_BREAKOUT_LONG | Multi-bar compression_score ≥ 70 + close ≥ rolling 10-bar high + RS_vs_NIFTY > 0 + vol_z_tod > 2.0 + bullish body + above AVWAP | Pre-coiled multi-day energy releases on confirmed breakout with regime alignment | `advanced_indicators_calculator._compression_breakout_long` |
| L_MOMENTUM_BREAKOUT_OR | break_opening_high + RS > 0 + vol_z_tod > 2.0 + bullish body + above AVWAP + not extended (< 1.5 ATR from AVWAP) | Classic OR breakout with cross-check on RS, volume, and stretch | `advanced_indicators_calculator._momentum_breakout_long` |
| L_SECTOR_RS_LEADER | rs_vs_sector_30m_pct > +1.0 + uptrend day + breakout entry pattern | Sector RS leader = real institutional money flow | `advanced_indicators_calculator._add_sector_rs` |
| L_OBV_DIVERGENCE_VWAP | Price made lower low over 10 bars but OBV made higher low + reclaim of VWAP with bullish body | Volume diverging from price = quiet accumulation | Standard OBV divergence pattern |
| L_DOJI_REVERSAL_AT_SUPPORT | Doji-like bar (body_eff < 0.3) at confirmed support level (prev day low or VWAP) + next bar bullish confirmation | Indecision at level + decision = reversal start | Classical candlestick + level |
| L_MULTI_TF_ALIGN | 5-min entry only if 15-min trend (close > 15m EMA20) AND 1-hr trend (close > 1h EMA20) | Multi-timeframe confluence reduces noise | Common MTF approach |
| L_VWAP_RECLAIM_TREND | Was below session VWAP for ≥ 3 bars, now closed back above with bullish body + RS > 0 + ADX ≥ 20 | VWAP reclaim from below = intraday mean shift to bulls | Cascade has B_AVWAP_RECLAIM_REVERSAL — but dormant |

### SHORT side

| Setup | Trigger / Detection Logic | Strategy / Story | Source |
|---|---|---|---|
| S_LIQUIDITY_SWEEP_REVERSAL | failed_prev_day_high_short + next bar's close < prev_day_high | Liquidity sweep + rejection within one bar = trap | `advanced_indicators_calculator._failed_breakout_short` |
| S_MOMENTUM_BREAKDOWN_OR | break_opening_low + RS < 0 + vol_z_tod > 2.0 + bearish body + below AVWAP + not extended | OR breakdown with full alignment check | `advanced_indicators_calculator._momentum_breakdown_short` |
| S_SECTOR_RS_LAGGARD | rs_vs_sector_30m_pct < -1.0 + downtrend day + breakdown entry | Sector laggard = relative weakness reinforced | `advanced_indicators_calculator._add_sector_rs` |
| S_OBV_DIVERGENCE_VWAP | Price higher high over 10 bars but OBV lower high + breaks VWAP with bearish body | Volume diverging from price upside = distribution | Standard OBV bearish divergence |
| S_SHOOTING_STAR_AT_RESISTANCE | Upper wick > 2× body at confirmed resistance + next bar bearish confirmation | Failed buying attempt at level = supply zone confirmed | Classical candlestick |
| S_MULTI_TF_BEAR_ALIGN | 5-min short entry only if 15-min AND 1-hr trend both bearish (close < EMA20s) | MTF confluence for shorts | Common MTF approach |
| S_VWAP_LOSE_TREND | Was above session VWAP ≥ 3 bars, now closed below with bearish body + RS < 0 + ADX ≥ 20 | Symmetric to L_VWAP_RECLAIM | Cascade has D_AVWAP_LOSE_REVERSAL (LIVE) |

---

## 6. Summary by status

| Status | LONG count | SHORT count | Total |
|---|--:|--:|--:|
| LIVE (shipping) | 1 | 2 | 3 |
| CASCADE-DORMANT | 9 | 9 | 18 |
| TESTED-NEW v1 | 3 | 3 | 6 |
| TESTED-NEW v2 | 6 | 7 | 13 |
| CONCEPT (untested) | 7 | 7 | 14 |
| **TOTAL CATALOGUED** | **26** | **28** | **54** |

Of all tested setups outside the LIVE list, **zero passed the strict OOS gates** (PF ≥ 1.30 kept, OOS PF ≥ 1.20, OOS n ≥ 10, decay ≥ 0.65) at the TGT 1.5%/SL 0.75% geometry on the 3-month window.

---

## 7. Practical reading

- **The cascade's pre-built microstructure setups (A_MOD, B_HUGE, D_AVWAP) already capture the meaningful intraday edges.** Pure indicator-combination setups (MACD/BB/RSI/MFI/CCI/etc.) add noise at this geometry — they get arbitraged out by the rest of the market.
- **The most promising untested path** is to unlock cascade-dormant setups at the new exit geometry — specifically `D_EMA20_BOUNCE`, `G_HIGHER_HIGH_BREAK`, and `B_AVWAP_RECLAIM_REVERSAL`. These have real microstructure logic and were dropped at the OLD 0.8%/0.75% geometry; the new TGT 1.5% may rescue them. This requires disabling V17M low-win-cleanup for those setups (separate exercise).
- **Concept setups from `advanced_indicators_calculator.py`** (momentum_breakout_long, compression_breakout_long, liquidity_sweep_reversal_long, failed_breakout_short, momentum_breakdown_short) are the strongest untested candidates — they layer RS + volume + body + AVWAP alignment in a single mechanical check. They would need to be wired into the v17r_nonf scan path to test honestly.
- **Sector RS** is a major dimension the current pipeline does not use. Even one sector-RS gate could substantially differentiate trades in the same direction.
