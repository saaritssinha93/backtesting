# Hilega Milega Research Setups

## Status

- Research-only and disabled by default.
- Not present in `FINAL_SETUP_CONF` or the live allowed-setup book.
- Enable raw detection only with `EQIDV2_ENABLE_HILEGA_MILEGA_RESEARCH=1`.
- No accuracy or profitability is assumed. Promotion requires the normal
  train/test, cost, robustness, and paper-trading gates.

## Shared Indicators

- `HM_RSI_9`: Wilder RSI of close, period 9.
- `HM_RSI_EMA_3`: EMA(3) of `HM_RSI_9`.
- `HM_RSI_WMA_21`: linear WMA(21) of `HM_RSI_9`.
- `HM_BB_MID_20`: 20-period SMA of close.
- `HM_BB_UPPER_20` and `HM_BB_LOWER_20`: 20-period Bollinger Bands at two
  population standard deviations.

`HM_RSI_WMA_21` is not volume-weighted. A standard WMA gives larger weights to
newer RSI observations. The transcript's description of this line as volume is
not mathematically true unless WMA is replaced by VWMA or volume is otherwise
added explicitly.

## Entry Setups

### L_HM_RSI50_REVERSAL

- RSI crosses from at or below 50 to above 50 on the completed bar.
- RSI is above both RSI moving averages.
- EMA(3) and WMA(21) are both rising.
- The signal candle is bullish, closes above the previous high, and closes
  above the Bollinger 20-SMA.

### S_HM_RSI50_REVERSAL

- RSI crosses from at or above 50 to below 50 on the completed bar.
- RSI is below both RSI moving averages.
- EMA(3) and WMA(21) are both falling.
- The signal candle is bearish, closes below the previous low, and closes
  below the Bollinger 20-SMA.

The transcript explicitly gives the previous-high confirmation for bottoms.
The previous-low short rule is a deliberate symmetric implementation, not a
verbatim rule from the speaker.

### L_HM_BB20_PULLBACK

- RSI was above 50 on the previous completed bar and remains above 50.
- RSI is above both rising RSI moving averages.
- Price touches or moves below the Bollinger 20-SMA intrabar, then closes back
  above it with a bullish candle.

### S_HM_BB20_PULLBACK

- RSI was below 50 on the previous completed bar and remains below 50.
- RSI is below both falling RSI moving averages.
- Price touches or moves above the Bollinger 20-SMA intrabar, then closes back
  below it with a bearish candle.

## Diagnostics And Exits

- `HM_BOTTOM_FORMING_WARNING`: RSI crosses above an RSI average while still
  below 50. This is a warning, never an entry by itself.
- `HM_TOP_FORMING_WARNING`: inverse warning while RSI is still above 50.
- `HM_LINE_DISTANCE`: visual momentum-distance measure. It is recorded but no
  invented numeric threshold is imposed because the transcript supplies none.
- `HM_NO_TRADE`: neither a clean rising bullish alignment nor a clean falling
  bearish alignment exists.
- `HM_EXIT_LONG_WMA_CROSS`: WMA crosses from below RSI to above RSI.
- `HM_EXIT_SHORT_WMA_CROSS`: WMA crosses from above RSI to below RSI.
- Signal-candle low/high and Bollinger targets are exposed as diagnostic
  columns. The existing v2 adapter still uses its normal research execution
  model; these transcript exits are not silently substituted into live code.

## Timeframe Note

The indicator calculations are timeframe-agnostic. The transcript recommends
one-hour bars, says not to go below 15-minute bars, and suggests entries after
10:15 IST. The v2 adapter is a five-minute research adaptation and therefore
must be evaluated separately from the speaker's preferred hourly usage.

