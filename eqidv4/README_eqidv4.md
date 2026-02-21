# EQIDV4 - ORB + VWAP + RVOL Intraday Strategy (NSE)

EQIDV4 is now a production-oriented intraday strategy for NSE equities using:
- Opening Range Breakout (ORB)
- 15m VWAP + EMA trend filter
- 5m time-of-day normalized Relative Volume (RVOL)

It supports long and short, strict completed-candle logic, portfolio risk controls, and separate live/backtest entry points.

## 1) Session and execution controls
- Market session: `09:15` to `15:30` IST
- No new entries after: `14:45` IST
- Force exit window: `15:10` to `15:15` IST
- Strategy evaluates entries only on completed 5m bars.

## 2) Universe selection
Universe is selected daily from 15m data using 20-session historical liquidity:
- Metric: median daily traded value (`sum(close * volume)` per day)
- Default universe size: top `500` symbols (`--universe-top-n`)
- Filters:
  - minimum history sessions: `10`
  - minimum median traded value: `5e7`
  - minimum median price: `40`

Cached file:
- `outputs/universe_turnover_15m.csv`

## 3) Indicators and no-leakage handling
The implementation uses only completed candles and computes session features safely:
- 15m context:
  - session VWAP (reset each day)
  - EMA20, EMA50
  - ATR(14)
- 5m execution:
  - ATR(14)
  - EMA20 (for optional trailing)
  - RVOL with time-of-day normalization:
    - `RVOL = current 5m volume / median(volume at same 5m slot over prior sessions)`

No leakage rules in code:
- entry decision uses current completed 5m close
- context uses latest completed 15m candle via backward asof join

## 4) ORB definition
Supported OR windows:
- ORB-15 (`--orb-minutes 15`): first 15 minutes
- ORB-30 (`--orb-minutes 30`): first 30 minutes

Opening range values:
- `OR_High`, `OR_Low`, `OR_Range`

## 5) Default parameters
- OR buffer: `max(0.05% of price, 0.10 * ATR_15m)`
- RVOL threshold: `1.5`
- Anti-chop: skip if `OR_Range < 0.25 * ATR_15m`
- Stop distance: `0.8 * ATR_5m`
- Partial: 50% at `+1R`
- Time stop: if max favorable move `< +0.5R` within 6 bars, exit
- Max open positions: `10`
- Daily loss stop: `-1.5R`

## 6) Entry rules (implemented)
### Long
- breakout: `Close_5m > OR_High + buffer`
- 15m VWAP alignment: `Close_15m > VWAP_15m`
- 15m trend: `EMA20_15m > EMA50_15m` and `EMA20_15m rising`
- participation: `RVOL >= threshold`
- anti-chop pass
- optional 2-close confirmation (`--require-two-close-confirm`)

### Short
Mirror of long:
- `Close_5m < OR_Low - buffer`
- `Close_15m < VWAP_15m`
- `EMA20_15m < EMA50_15m` and falling
- `RVOL >= threshold`
- anti-chop pass

## 7) Exit and risk rules (implemented)
- position risk per trade: `0.10%` capital (default)
- initial stop:
  - long: `entry - 0.8 * ATR_5m`
  - short: `entry + 0.8 * ATR_5m`
- partial profit: 50% at `+1R`
- trailing (choose one):
  - default `vwap15`: exit on 15m VWAP close-through
  - optional `ema20_5m`
- time stop: 6 bars rule with `+0.5R` threshold
- force close by `15:10-15:15`
- no entries after `14:45`

## 8) Market regime filter
Optional index gate is supported (`use_index_regime_filter`):
- long only when index is above VWAP and EMA20 > EMA50
- short only when index is below VWAP and EMA20 < EMA50

If index file is unavailable in 15m data (`<index_ticker>_stocks_indicators_15min.parquet`), the gate is skipped.

## 9) Key files
- `eqidv4_orb_strategy_core.py`
  - config, universe ranking, data prep, RVOL, ORB, signal checks
- `eqidv4_orb_vwap_rvol_backtest.py`
  - portfolio/day simulation, costs, exits, reports
- `eqidv4_orb_vwap_rvol_live.py`
  - run-once live scan and replay-date scan
- `bat/run_eqidv4_live_signals.bat`
  - run-once live signal job

## 10) Backtest usage
From `eqidv4` folder:

```powershell
python eqidv4_orb_vwap_rvol_backtest.py --start-date 2026-02-19 --end-date 2026-02-20 --universe-top-n 300 --ticker-limit 300 --require-two-close-confirm --verbose
```

Important options:
- `--orb-minutes 15|30`
- `--rvol-min 1.2|1.5|1.8`
- `--stop-atr5-mult 0.6|0.8|1.0`
- `--trail-method vwap15|ema20_5m`
- `--disable-index-regime`
- `--refresh-universe-cache`

Outputs:
- `outputs/eqidv4_orb_backtest_trades_<timestamp>.csv`
- `outputs/eqidv4_orb_backtest_summary_<timestamp>.json`

## 11) Live usage
### Run once (latest completed 5m bar, today)
```powershell
python eqidv4_orb_vwap_rvol_live.py --run-once --require-two-close-confirm
```

### Replay a historical date
```powershell
python eqidv4_orb_vwap_rvol_live.py --replay-date 2026-02-20 --replay-full-day --require-two-close-confirm
```

Replay output:
- `out_eqidv4_orb_live_signals_5m/replay_signals_<date>.csv`

Live output:
- `live_signals/signals_<date>.csv`
- state file for dedupe: `logs/eqidv4_orb_live_state.json`

## 12) Batch files
- `bat/run_eqidv4_live_signals.bat`
  - now runs: `python eqidv4_orb_vwap_rvol_live.py --run-once --require-two-close-confirm`
- `bat/run_eqidv4_15m_updater.bat`
  - unchanged data updater entrypoint
- `bat/run_eqidv4_eod_1540.bat`
  - unchanged EOD updater entrypoint

## 13) Notes
- Sector cap is not enabled by default because no sector map is bundled in this folder.
- Slippage and transaction cost are modeled and configurable in `StrategyConfig`.
- This strategy is intraday only; all positions are forced flat before close.
