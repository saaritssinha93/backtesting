# D_EMA20_BOUNCE Redesigned Setup Ideas

- Original with better filters: one or two signal-side thresholds from RSI/ADX/VWAP/volume/candle features.
- Original with better pre-momentum: pre-entry 1-minute momentum, range, close-position, RSI-direction, and ADX gates.
- Original with better exits: fixed SL/target grid from tight scalps to wider continuation exits.
- Simpler version: raw detector plus one structural term only.
- Stricter version: one signal filter plus one pre-momentum filter.
- Time-window version: min/max slot guards and top_n slot ranking.
- VWAP/EMA confirmation version: distance-to-VWAP/EMA and candle-location filters.
- Volume plus candle quality version: vol_ratio, body_pct, close_loc, wick-skew.
- Volatility-regime version: atr_pct, signal_range_pct, and market_abs_ret_pct checked on train side.
- Failed-signal avoidance: avoid overextended, low-quality, or late-session entries.
