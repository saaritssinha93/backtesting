# SETUP_FAMILY_IDEAS

All families are LONG-only, causal on the current/previous 5-minute bars, and use next 1-minute open entry.
Default exit theme is a tight bracket centered on 0.75% / 0.75% with 1-minute intrabar resolution.

## Exit Grid
- `sl0.75_tgt0.75_tb3`: SL 0.75% / target 0.75% / time exit 3 bars
- `sl0.75_tgt0.75_tb6`: SL 0.75% / target 0.75% / time exit 6 bars
- `sl0.75_tgt0.75_tb9`: SL 0.75% / target 0.75% / time exit 9 bars
- `sl0.5_tgt0.5_tb3`: SL 0.5% / target 0.5% / time exit 3 bars
- `sl0.5_tgt0.5_tb6`: SL 0.5% / target 0.5% / time exit 6 bars
- `sl0.5_tgt0.5_tb9`: SL 0.5% / target 0.5% / time exit 9 bars
- `sl0.6_tgt0.6_tb3`: SL 0.6% / target 0.6% / time exit 3 bars
- `sl0.6_tgt0.6_tb6`: SL 0.6% / target 0.6% / time exit 6 bars
- `sl0.6_tgt0.6_tb9`: SL 0.6% / target 0.6% / time exit 9 bars
- `sl0.75_tgt1_tb3`: SL 0.75% / target 1% / time exit 3 bars
- `sl0.75_tgt1_tb6`: SL 0.75% / target 1% / time exit 6 bars
- `sl0.75_tgt1_tb9`: SL 0.75% / target 1% / time exit 9 bars
- `sl0.5_tgt0.75_tb3`: SL 0.5% / target 0.75% / time exit 3 bars
- `sl0.5_tgt0.75_tb6`: SL 0.5% / target 0.75% / time exit 6 bars
- `sl0.5_tgt0.75_tb9`: SL 0.5% / target 0.75% / time exit 9 bars
- `sl0.75_tgt0.75_tb6_be0p4`: SL 0.75% / target 0.75% / time exit 6 bars / move SL to breakeven after +0.4%
- `sl0.6_tgt0.6_tb6_be0p4`: SL 0.6% / target 0.6% / time exit 6 bars / move SL to breakeven after +0.4%
- `sl0.75_tgt1_tb6_be0p4`: SL 0.75% / target 1% / time exit 6 bars / move SL to breakeven after +0.4%
- `sl0p75_tgt1_tb9_trail0p5_after0p75`: SL 0.75% / target 1% / time exit 9 bars / trail 0.5% after +0.75%

## Guards
- `g_base`: min_slot=2, max_slot=60, top_n_per_slot=None, max_per_symbol_day=1, cooldown_after_sl_bars=0
- `g_morning`: min_slot=2, max_slot=22, top_n_per_slot=None, max_per_symbol_day=1, cooldown_after_sl_bars=0
- `g_no_open`: min_slot=5, max_slot=60, top_n_per_slot=None, max_per_symbol_day=1, cooldown_after_sl_bars=0
- `g_top3_slot`: min_slot=2, max_slot=60, top_n_per_slot=3, max_per_symbol_day=1, cooldown_after_sl_bars=0
- `g_top5_slot`: min_slot=2, max_slot=60, top_n_per_slot=5, max_per_symbol_day=1, cooldown_after_sl_bars=0
- `g_two_per_symbol`: min_slot=2, max_slot=60, top_n_per_slot=5, max_per_symbol_day=2, cooldown_after_sl_bars=6

## Families And Rule Variants
### LONG_VWAP_RECLAIM_MOMENTUM_vol1.1_cl0.62
- family: LONG_VWAP_RECLAIM_MOMENTUM
- entry trigger: Current 5m close reclaims VWAP after previous close was at/below VWAP.
- indicator filters: RSI delta >= 0, vol_ratio >= 1.1, close_loc >= 0.62
- non-indicator rules: close crosses above VWAP; positive 5m body; not more than 1.2% above VWAP
- pre-momentum filter: Volume rising or current relative volume confirms the reclaim; prior bars not both bearish.
- rationale: designed to catch a quick +0.75% pop without chasing late extension.

### LONG_VWAP_RECLAIM_MOMENTUM_vol1.1_cl0.72
- family: LONG_VWAP_RECLAIM_MOMENTUM
- entry trigger: Current 5m close reclaims VWAP after previous close was at/below VWAP.
- indicator filters: RSI delta >= 0, vol_ratio >= 1.1, close_loc >= 0.72
- non-indicator rules: close crosses above VWAP; positive 5m body; not more than 1.2% above VWAP
- pre-momentum filter: Volume rising or current relative volume confirms the reclaim; prior bars not both bearish.
- rationale: designed to catch a quick +0.75% pop without chasing late extension.

### LONG_VWAP_RECLAIM_MOMENTUM_vol1.25_cl0.62
- family: LONG_VWAP_RECLAIM_MOMENTUM
- entry trigger: Current 5m close reclaims VWAP after previous close was at/below VWAP.
- indicator filters: RSI delta >= 0, vol_ratio >= 1.25, close_loc >= 0.62
- non-indicator rules: close crosses above VWAP; positive 5m body; not more than 1.2% above VWAP
- pre-momentum filter: Volume rising or current relative volume confirms the reclaim; prior bars not both bearish.
- rationale: designed to catch a quick +0.75% pop without chasing late extension.

### LONG_VWAP_RECLAIM_MOMENTUM_vol1.25_cl0.72
- family: LONG_VWAP_RECLAIM_MOMENTUM
- entry trigger: Current 5m close reclaims VWAP after previous close was at/below VWAP.
- indicator filters: RSI delta >= 0, vol_ratio >= 1.25, close_loc >= 0.72
- non-indicator rules: close crosses above VWAP; positive 5m body; not more than 1.2% above VWAP
- pre-momentum filter: Volume rising or current relative volume confirms the reclaim; prior bars not both bearish.
- rationale: designed to catch a quick +0.75% pop without chasing late extension.

### LONG_VWAP_RECLAIM_MOMENTUM_vol1.5_cl0.62
- family: LONG_VWAP_RECLAIM_MOMENTUM
- entry trigger: Current 5m close reclaims VWAP after previous close was at/below VWAP.
- indicator filters: RSI delta >= 0, vol_ratio >= 1.5, close_loc >= 0.62
- non-indicator rules: close crosses above VWAP; positive 5m body; not more than 1.2% above VWAP
- pre-momentum filter: Volume rising or current relative volume confirms the reclaim; prior bars not both bearish.
- rationale: designed to catch a quick +0.75% pop without chasing late extension.

### LONG_VWAP_RECLAIM_MOMENTUM_vol1.5_cl0.72
- family: LONG_VWAP_RECLAIM_MOMENTUM
- entry trigger: Current 5m close reclaims VWAP after previous close was at/below VWAP.
- indicator filters: RSI delta >= 0, vol_ratio >= 1.5, close_loc >= 0.72
- non-indicator rules: close crosses above VWAP; positive 5m body; not more than 1.2% above VWAP
- pre-momentum filter: Volume rising or current relative volume confirms the reclaim; prior bars not both bearish.
- rationale: designed to catch a quick +0.75% pop without chasing late extension.

### LONG_PRESSURE_BURST_BREAKOUT_vol1.15_body0.18
- family: LONG_PRESSURE_BURST_BREAKOUT
- entry trigger: Close breaks above the prior 5m high on a strong green pressure candle.
- indicator filters: vol_ratio >= 1.15, green_body_pct >= 0.18, close_loc >= 0.68
- non-indicator rules: close above previous candle high; upper wick <= 0.45%; green_streak_3 <= 2
- pre-momentum filter: RSI and MACD histogram are rising into the trigger.
- rationale: designed to catch a quick +0.75% pop without chasing late extension.

### LONG_PRESSURE_BURST_BREAKOUT_vol1.15_body0.28
- family: LONG_PRESSURE_BURST_BREAKOUT
- entry trigger: Close breaks above the prior 5m high on a strong green pressure candle.
- indicator filters: vol_ratio >= 1.15, green_body_pct >= 0.28, close_loc >= 0.68
- non-indicator rules: close above previous candle high; upper wick <= 0.45%; green_streak_3 <= 2
- pre-momentum filter: RSI and MACD histogram are rising into the trigger.
- rationale: designed to catch a quick +0.75% pop without chasing late extension.

### LONG_PRESSURE_BURST_BREAKOUT_vol1.35_body0.18
- family: LONG_PRESSURE_BURST_BREAKOUT
- entry trigger: Close breaks above the prior 5m high on a strong green pressure candle.
- indicator filters: vol_ratio >= 1.35, green_body_pct >= 0.18, close_loc >= 0.68
- non-indicator rules: close above previous candle high; upper wick <= 0.45%; green_streak_3 <= 2
- pre-momentum filter: RSI and MACD histogram are rising into the trigger.
- rationale: designed to catch a quick +0.75% pop without chasing late extension.

### LONG_PRESSURE_BURST_BREAKOUT_vol1.35_body0.28
- family: LONG_PRESSURE_BURST_BREAKOUT
- entry trigger: Close breaks above the prior 5m high on a strong green pressure candle.
- indicator filters: vol_ratio >= 1.35, green_body_pct >= 0.28, close_loc >= 0.68
- non-indicator rules: close above previous candle high; upper wick <= 0.45%; green_streak_3 <= 2
- pre-momentum filter: RSI and MACD histogram are rising into the trigger.
- rationale: designed to catch a quick +0.75% pop without chasing late extension.

### LONG_PRESSURE_BURST_BREAKOUT_vol1.6_body0.18
- family: LONG_PRESSURE_BURST_BREAKOUT
- entry trigger: Close breaks above the prior 5m high on a strong green pressure candle.
- indicator filters: vol_ratio >= 1.6, green_body_pct >= 0.18, close_loc >= 0.68
- non-indicator rules: close above previous candle high; upper wick <= 0.45%; green_streak_3 <= 2
- pre-momentum filter: RSI and MACD histogram are rising into the trigger.
- rationale: designed to catch a quick +0.75% pop without chasing late extension.

### LONG_PRESSURE_BURST_BREAKOUT_vol1.6_body0.28
- family: LONG_PRESSURE_BURST_BREAKOUT
- entry trigger: Close breaks above the prior 5m high on a strong green pressure candle.
- indicator filters: vol_ratio >= 1.6, green_body_pct >= 0.28, close_loc >= 0.68
- non-indicator rules: close above previous candle high; upper wick <= 0.45%; green_streak_3 <= 2
- pre-momentum filter: RSI and MACD histogram are rising into the trigger.
- rationale: designed to catch a quick +0.75% pop without chasing late extension.

### LONG_CONSOLIDATION_EXPANSION_BREAKOUT_comp0.72_exp1.2
- family: LONG_CONSOLIDATION_EXPANSION_BREAKOUT
- entry trigger: Range compression over prior bars, then close breaks 3-bar high with expansion.
- indicator filters: ATR% >= 0.20, range expansion after compression; RSI delta >= -1
- non-indicator rules: compression <= 0.72, range_expansion >= 1.2; close above prior 3-bar high
- pre-momentum filter: Prior two candles avoid bearish pressure; volume is at least above prior average.
- rationale: designed to catch a quick +0.75% pop without chasing late extension.

### LONG_CONSOLIDATION_EXPANSION_BREAKOUT_comp0.72_exp1.45
- family: LONG_CONSOLIDATION_EXPANSION_BREAKOUT
- entry trigger: Range compression over prior bars, then close breaks 3-bar high with expansion.
- indicator filters: ATR% >= 0.20, range expansion after compression; RSI delta >= -1
- non-indicator rules: compression <= 0.72, range_expansion >= 1.45; close above prior 3-bar high
- pre-momentum filter: Prior two candles avoid bearish pressure; volume is at least above prior average.
- rationale: designed to catch a quick +0.75% pop without chasing late extension.

### LONG_CONSOLIDATION_EXPANSION_BREAKOUT_comp0.85_exp1.2
- family: LONG_CONSOLIDATION_EXPANSION_BREAKOUT
- entry trigger: Range compression over prior bars, then close breaks 3-bar high with expansion.
- indicator filters: ATR% >= 0.20, range expansion after compression; RSI delta >= -1
- non-indicator rules: compression <= 0.85, range_expansion >= 1.2; close above prior 3-bar high
- pre-momentum filter: Prior two candles avoid bearish pressure; volume is at least above prior average.
- rationale: designed to catch a quick +0.75% pop without chasing late extension.

### LONG_CONSOLIDATION_EXPANSION_BREAKOUT_comp0.85_exp1.45
- family: LONG_CONSOLIDATION_EXPANSION_BREAKOUT
- entry trigger: Range compression over prior bars, then close breaks 3-bar high with expansion.
- indicator filters: ATR% >= 0.20, range expansion after compression; RSI delta >= -1
- non-indicator rules: compression <= 0.85, range_expansion >= 1.45; close above prior 3-bar high
- pre-momentum filter: Prior two candles avoid bearish pressure; volume is at least above prior average.
- rationale: designed to catch a quick +0.75% pop without chasing late extension.

### LONG_FAILED_BREAKDOWN_REVERSAL_wick0.25_cl0.58
- family: LONG_FAILED_BREAKDOWN_REVERSAL
- entry trigger: Price undercuts recent 3-bar low but reclaims into the upper half of the candle.
- indicator filters: RSI delta >= -2, MACD histogram not sharply deteriorating
- non-indicator rules: lower_wick_pct >= 0.25, close_loc >= 0.58; current close above prior close
- pre-momentum filter: Requires rejection wick and no extended green streak before the reversal.
- rationale: designed to catch a quick +0.75% pop without chasing late extension.

### LONG_FAILED_BREAKDOWN_REVERSAL_wick0.25_cl0.68
- family: LONG_FAILED_BREAKDOWN_REVERSAL
- entry trigger: Price undercuts recent 3-bar low but reclaims into the upper half of the candle.
- indicator filters: RSI delta >= -2, MACD histogram not sharply deteriorating
- non-indicator rules: lower_wick_pct >= 0.25, close_loc >= 0.68; current close above prior close
- pre-momentum filter: Requires rejection wick and no extended green streak before the reversal.
- rationale: designed to catch a quick +0.75% pop without chasing late extension.

### LONG_FAILED_BREAKDOWN_REVERSAL_wick0.4_cl0.58
- family: LONG_FAILED_BREAKDOWN_REVERSAL
- entry trigger: Price undercuts recent 3-bar low but reclaims into the upper half of the candle.
- indicator filters: RSI delta >= -2, MACD histogram not sharply deteriorating
- non-indicator rules: lower_wick_pct >= 0.4, close_loc >= 0.58; current close above prior close
- pre-momentum filter: Requires rejection wick and no extended green streak before the reversal.
- rationale: designed to catch a quick +0.75% pop without chasing late extension.

### LONG_FAILED_BREAKDOWN_REVERSAL_wick0.4_cl0.68
- family: LONG_FAILED_BREAKDOWN_REVERSAL
- entry trigger: Price undercuts recent 3-bar low but reclaims into the upper half of the candle.
- indicator filters: RSI delta >= -2, MACD histogram not sharply deteriorating
- non-indicator rules: lower_wick_pct >= 0.4, close_loc >= 0.68; current close above prior close
- pre-momentum filter: Requires rejection wick and no extended green streak before the reversal.
- rationale: designed to catch a quick +0.75% pop without chasing late extension.

### LONG_PULLBACK_CONTINUATION_pull0.0_vol0.85
- family: LONG_PULLBACK_CONTINUATION
- entry trigger: Trend is intact above VWAP/EMA20, prior candle pulls back, current candle breaks prior high.
- indicator filters: EMA20 slope positive, close above VWAP and EMA20
- non-indicator rules: previous bar non-green pullback; current close above previous high
- pre-momentum filter: RSI delta >= 0 and distance from VWAP <= 1.1% to avoid poor tight-stop reward.
- rationale: designed to catch a quick +0.75% pop without chasing late extension.

### LONG_PULLBACK_CONTINUATION_pull0.0_vol1.05
- family: LONG_PULLBACK_CONTINUATION
- entry trigger: Trend is intact above VWAP/EMA20, prior candle pulls back, current candle breaks prior high.
- indicator filters: EMA20 slope positive, close above VWAP and EMA20
- non-indicator rules: previous bar non-green pullback; current close above previous high
- pre-momentum filter: RSI delta >= 0 and distance from VWAP <= 1.1% to avoid poor tight-stop reward.
- rationale: designed to catch a quick +0.75% pop without chasing late extension.

### LONG_PULLBACK_CONTINUATION_pullm0.08_vol0.85
- family: LONG_PULLBACK_CONTINUATION
- entry trigger: Trend is intact above VWAP/EMA20, prior candle pulls back, current candle breaks prior high.
- indicator filters: EMA20 slope positive, close above VWAP and EMA20
- non-indicator rules: previous bar non-green pullback; current close above previous high
- pre-momentum filter: RSI delta >= 0 and distance from VWAP <= 1.1% to avoid poor tight-stop reward.
- rationale: designed to catch a quick +0.75% pop without chasing late extension.

### LONG_PULLBACK_CONTINUATION_pullm0.08_vol1.05
- family: LONG_PULLBACK_CONTINUATION
- entry trigger: Trend is intact above VWAP/EMA20, prior candle pulls back, current candle breaks prior high.
- indicator filters: EMA20 slope positive, close above VWAP and EMA20
- non-indicator rules: previous bar non-green pullback; current close above previous high
- pre-momentum filter: RSI delta >= 0 and distance from VWAP <= 1.1% to avoid poor tight-stop reward.
- rationale: designed to catch a quick +0.75% pop without chasing late extension.

### LONG_VOLUME_EXPANSION_BREAKOUT_vol1.5_h3
- family: LONG_VOLUME_EXPANSION_BREAKOUT
- entry trigger: Relative-volume expansion breaks prior 3-bar high.
- indicator filters: vol_ratio >= 1.5, RSI >= 48, ADX >= 12
- non-indicator rules: close above prior 3-bar high; not overextended from VWAP
- pre-momentum filter: Volume rising into the breakout and candle closes in top 35% of its range.
- rationale: designed to catch a quick +0.75% pop without chasing late extension.

### LONG_VOLUME_EXPANSION_BREAKOUT_vol1.5_h5
- family: LONG_VOLUME_EXPANSION_BREAKOUT
- entry trigger: Relative-volume expansion breaks prior 5-bar high.
- indicator filters: vol_ratio >= 1.5, RSI >= 48, ADX >= 12
- non-indicator rules: close above prior 5-bar high; not overextended from VWAP
- pre-momentum filter: Volume rising into the breakout and candle closes in top 35% of its range.
- rationale: designed to catch a quick +0.75% pop without chasing late extension.

### LONG_VOLUME_EXPANSION_BREAKOUT_vol2_h3
- family: LONG_VOLUME_EXPANSION_BREAKOUT
- entry trigger: Relative-volume expansion breaks prior 3-bar high.
- indicator filters: vol_ratio >= 2, RSI >= 48, ADX >= 12
- non-indicator rules: close above prior 3-bar high; not overextended from VWAP
- pre-momentum filter: Volume rising into the breakout and candle closes in top 35% of its range.
- rationale: designed to catch a quick +0.75% pop without chasing late extension.

### LONG_VOLUME_EXPANSION_BREAKOUT_vol2_h5
- family: LONG_VOLUME_EXPANSION_BREAKOUT
- entry trigger: Relative-volume expansion breaks prior 5-bar high.
- indicator filters: vol_ratio >= 2, RSI >= 48, ADX >= 12
- non-indicator rules: close above prior 5-bar high; not overextended from VWAP
- pre-momentum filter: Volume rising into the breakout and candle closes in top 35% of its range.
- rationale: designed to catch a quick +0.75% pop without chasing late extension.

### LONG_EMA_VWAP_TREND_CONTINUATION_slope0_adx10
- family: LONG_EMA_VWAP_TREND_CONTINUATION
- entry trigger: Trend stack continuation above VWAP, EMA20, and EMA50 with prior-high break.
- indicator filters: EMA20 slope >= 0, ADX >= 10; RSI delta >= -1
- non-indicator rules: close > EMA20 > EMA50; close above VWAP; close above previous high
- pre-momentum filter: Rejects late exhaustion with green_streak_3 <= 2 and VWAP distance <= 1.25%.
- rationale: designed to catch a quick +0.75% pop without chasing late extension.

### LONG_EMA_VWAP_TREND_CONTINUATION_slope0_adx16
- family: LONG_EMA_VWAP_TREND_CONTINUATION
- entry trigger: Trend stack continuation above VWAP, EMA20, and EMA50 with prior-high break.
- indicator filters: EMA20 slope >= 0, ADX >= 16; RSI delta >= -1
- non-indicator rules: close > EMA20 > EMA50; close above VWAP; close above previous high
- pre-momentum filter: Rejects late exhaustion with green_streak_3 <= 2 and VWAP distance <= 1.25%.
- rationale: designed to catch a quick +0.75% pop without chasing late extension.

### LONG_EMA_VWAP_TREND_CONTINUATION_slope0.04_adx10
- family: LONG_EMA_VWAP_TREND_CONTINUATION
- entry trigger: Trend stack continuation above VWAP, EMA20, and EMA50 with prior-high break.
- indicator filters: EMA20 slope >= 0.04, ADX >= 10; RSI delta >= -1
- non-indicator rules: close > EMA20 > EMA50; close above VWAP; close above previous high
- pre-momentum filter: Rejects late exhaustion with green_streak_3 <= 2 and VWAP distance <= 1.25%.
- rationale: designed to catch a quick +0.75% pop without chasing late extension.

### LONG_EMA_VWAP_TREND_CONTINUATION_slope0.04_adx16
- family: LONG_EMA_VWAP_TREND_CONTINUATION
- entry trigger: Trend stack continuation above VWAP, EMA20, and EMA50 with prior-high break.
- indicator filters: EMA20 slope >= 0.04, ADX >= 16; RSI delta >= -1
- non-indicator rules: close > EMA20 > EMA50; close above VWAP; close above previous high
- pre-momentum filter: Rejects late exhaustion with green_streak_3 <= 2 and VWAP distance <= 1.25%.
- rationale: designed to catch a quick +0.75% pop without chasing late extension.

### LONG_OPENING_STRENGTH_CONTINUATION_slot4_vol1.1
- family: LONG_OPENING_STRENGTH_CONTINUATION
- entry trigger: Early-session strength continuation after the first few 5m bars.
- indicator filters: vol_ratio >= 1.1, RSI >= 50; MACD histogram improving
- non-indicator rules: slot <= 4; close above prior high; strong close location
- pre-momentum filter: Avoids first raw bar; requires current pressure and no three-candle extension.
- rationale: designed to catch a quick +0.75% pop without chasing late extension.

### LONG_OPENING_STRENGTH_CONTINUATION_slot4_vol1.4
- family: LONG_OPENING_STRENGTH_CONTINUATION
- entry trigger: Early-session strength continuation after the first few 5m bars.
- indicator filters: vol_ratio >= 1.4, RSI >= 50; MACD histogram improving
- non-indicator rules: slot <= 4; close above prior high; strong close location
- pre-momentum filter: Avoids first raw bar; requires current pressure and no three-candle extension.
- rationale: designed to catch a quick +0.75% pop without chasing late extension.

### LONG_OPENING_STRENGTH_CONTINUATION_slot7_vol1.1
- family: LONG_OPENING_STRENGTH_CONTINUATION
- entry trigger: Early-session strength continuation after the first few 5m bars.
- indicator filters: vol_ratio >= 1.1, RSI >= 50; MACD histogram improving
- non-indicator rules: slot <= 7; close above prior high; strong close location
- pre-momentum filter: Avoids first raw bar; requires current pressure and no three-candle extension.
- rationale: designed to catch a quick +0.75% pop without chasing late extension.

### LONG_OPENING_STRENGTH_CONTINUATION_slot7_vol1.4
- family: LONG_OPENING_STRENGTH_CONTINUATION
- entry trigger: Early-session strength continuation after the first few 5m bars.
- indicator filters: vol_ratio >= 1.4, RSI >= 50; MACD histogram improving
- non-indicator rules: slot <= 7; close above prior high; strong close location
- pre-momentum filter: Avoids first raw bar; requires current pressure and no three-candle extension.
- rationale: designed to catch a quick +0.75% pop without chasing late extension.

### LONG_MIDDAY_RECLAIM_CONTINUATION_s15_36_vol0.9
- family: LONG_MIDDAY_RECLAIM_CONTINUATION
- entry trigger: Midday VWAP/EMA20 reclaim after a quiet pullback.
- indicator filters: RSI delta >= 0, EMA20 slope >= -0.03; vol_ratio >= 0.9
- non-indicator rules: 15 <= slot <= 36; close reclaims VWAP or EMA20; close near candle high
- pre-momentum filter: Looks for pre-momentum improvement after lower midday activity.
- rationale: designed to catch a quick +0.75% pop without chasing late extension.

### LONG_MIDDAY_RECLAIM_CONTINUATION_s15_36_vol1.15
- family: LONG_MIDDAY_RECLAIM_CONTINUATION
- entry trigger: Midday VWAP/EMA20 reclaim after a quiet pullback.
- indicator filters: RSI delta >= 0, EMA20 slope >= -0.03; vol_ratio >= 1.15
- non-indicator rules: 15 <= slot <= 36; close reclaims VWAP or EMA20; close near candle high
- pre-momentum filter: Looks for pre-momentum improvement after lower midday activity.
- rationale: designed to catch a quick +0.75% pop without chasing late extension.

### LONG_MIDDAY_RECLAIM_CONTINUATION_s20_48_vol0.9
- family: LONG_MIDDAY_RECLAIM_CONTINUATION
- entry trigger: Midday VWAP/EMA20 reclaim after a quiet pullback.
- indicator filters: RSI delta >= 0, EMA20 slope >= -0.03; vol_ratio >= 0.9
- non-indicator rules: 20 <= slot <= 48; close reclaims VWAP or EMA20; close near candle high
- pre-momentum filter: Looks for pre-momentum improvement after lower midday activity.
- rationale: designed to catch a quick +0.75% pop without chasing late extension.

### LONG_MIDDAY_RECLAIM_CONTINUATION_s20_48_vol1.15
- family: LONG_MIDDAY_RECLAIM_CONTINUATION
- entry trigger: Midday VWAP/EMA20 reclaim after a quiet pullback.
- indicator filters: RSI delta >= 0, EMA20 slope >= -0.03; vol_ratio >= 1.15
- non-indicator rules: 20 <= slot <= 48; close reclaims VWAP or EMA20; close near candle high
- pre-momentum filter: Looks for pre-momentum improvement after lower midday activity.
- rationale: designed to catch a quick +0.75% pop without chasing late extension.

### LONG_RANGE_EXPANSION_AFTER_COMPRESSION_comp0.65_vol1
- family: LONG_RANGE_EXPANSION_AFTER_COMPRESSION
- entry trigger: Compression-then-range expansion close above prior high.
- indicator filters: ATR% in workable band, MACD delta >= -0.02; vol_ratio >= 1
- non-indicator rules: compression <= 0.65; range expansion >= 1.35; close above previous high
- pre-momentum filter: Prior bars are not strongly bearish; entry not too far from VWAP/EMA20.
- rationale: designed to catch a quick +0.75% pop without chasing late extension.

### LONG_RANGE_EXPANSION_AFTER_COMPRESSION_comp0.65_vol1.25
- family: LONG_RANGE_EXPANSION_AFTER_COMPRESSION
- entry trigger: Compression-then-range expansion close above prior high.
- indicator filters: ATR% in workable band, MACD delta >= -0.02; vol_ratio >= 1.25
- non-indicator rules: compression <= 0.65; range expansion >= 1.35; close above previous high
- pre-momentum filter: Prior bars are not strongly bearish; entry not too far from VWAP/EMA20.
- rationale: designed to catch a quick +0.75% pop without chasing late extension.

### LONG_RANGE_EXPANSION_AFTER_COMPRESSION_comp0.8_vol1
- family: LONG_RANGE_EXPANSION_AFTER_COMPRESSION
- entry trigger: Compression-then-range expansion close above prior high.
- indicator filters: ATR% in workable band, MACD delta >= -0.02; vol_ratio >= 1
- non-indicator rules: compression <= 0.8; range expansion >= 1.35; close above previous high
- pre-momentum filter: Prior bars are not strongly bearish; entry not too far from VWAP/EMA20.
- rationale: designed to catch a quick +0.75% pop without chasing late extension.

### LONG_RANGE_EXPANSION_AFTER_COMPRESSION_comp0.8_vol1.25
- family: LONG_RANGE_EXPANSION_AFTER_COMPRESSION
- entry trigger: Compression-then-range expansion close above prior high.
- indicator filters: ATR% in workable band, MACD delta >= -0.02; vol_ratio >= 1.25
- non-indicator rules: compression <= 0.8; range expansion >= 1.35; close above previous high
- pre-momentum filter: Prior bars are not strongly bearish; entry not too far from VWAP/EMA20.
- rationale: designed to catch a quick +0.75% pop without chasing late extension.
