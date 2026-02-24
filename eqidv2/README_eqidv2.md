# EQID v2 — Complete Strategy Documentation

## Anchored VWAP (AVWAP) v11 Intraday Momentum Strategy

**A comprehensive guide to understanding, backtesting, and live-trading the EQID v2 system — written so that even a complete beginner can follow along.**

---

## Table of Contents

1. [What Is This System?](#1-what-is-this-system)
2. [Core Concept — The Big Idea](#2-core-concept--the-big-idea)
3. [Strategy Flow Diagrams](#3-strategy-flow-diagrams)
4. [Indicators Used (Explained Simply)](#4-indicators-used-explained-simply)
5. [Entry Logic — SHORT Side](#5-entry-logic--short-side)
6. [Entry Logic — LONG Side](#6-entry-logic--long-side)
7. [Exit Management (SL / Target / Breakeven / Trailing / EOD)](#7-exit-management)
8. [Every Parameter Explained](#8-every-parameter-explained)
9. [ML Meta-Label Overlay](#9-ml-meta-label-overlay)
10. [Backtesting Pipeline](#10-backtesting-pipeline)
11. [Live Trading Pipeline](#11-live-trading-pipeline)
12. [File-by-File Map](#12-file-by-file-map)
13. [Typical Workflows (Step-by-Step)](#13-typical-workflows)
14. [Output Files & Charts](#14-output-files--charts)
15. [Glossary](#15-glossary)

---

## 1. What Is This System?

EQID v2 is an **intraday algorithmic trading system** for **NSE (National Stock Exchange of India) equities**. It:

- Scans hundreds of stocks every 15 minutes during market hours (09:15-15:30 IST)
- Identifies high-probability SHORT and LONG trade setups using price action + technical indicators
- Manages exits with stop-loss, profit targets, breakeven protection, and trailing stops
- Optionally applies a Machine Learning filter to skip low-quality signals
- Can run in **backtest mode** (test on historical data) or **live mode** (generate real-time signals)

**Market**: NSE India equities (cash segment)
**Timeframe**: 15-minute candles for entries, 5-minute candles for exit resolution
**Style**: Intraday momentum (all positions closed by market close)

---

## 2. Core Concept — The Big Idea

> **"When a stock shows a strong directional impulse candle during a trending session, it is likely to continue in that direction after a brief pause."**

The strategy works like this:

1. **DETECT** a strong impulse candle (a big red candle for shorts, a big green candle for longs)
2. **CONFIRM** the trend is real using ADX, RSI, Stochastic, and EMA alignment
3. **VALIDATE** with AVWAP — the impulse should be in the direction away from the day's Anchored VWAP
4. **WAIT** for a small pullback/pause, then enter when price breaks past the impulse extreme
5. **MANAGE** the trade with SL, target, breakeven, and trailing stop rules
6. **EXIT** at target, stop-loss, breakeven, trailing stop, or end-of-day — whichever comes first

---

## 3. Strategy Flow Diagrams

### 3.1 Backtesting Flow (Historical Testing)

```
 +------------------+     +------------------+     +------------------+
 |  Historical      |     |  Prepare         |     |  Scan Each       |
 |  15m Parquet     |---->|  Indicators      |---->|  Ticker x Day    |
 |  Data Files      |     |  (ATR, RSI, ADX, |     |  for Impulse     |
 |  per Ticker      |     |   EMA, Stoch,    |     |  Candles         |
 +------------------+     |   AVWAP)         |     +--------+---------+
                           +------------------+              |
                                                             v
                                                  +------------------+
                                                  |  Apply Filters:  |
                                                  |  * Impulse Size  |
                                                  |  * Trend (ADX/   |
                                                  |    RSI/Stoch)    |
                                                  |  * EMA Alignment |
                                                  |  * AVWAP Rule    |
                                                  |  * Volume Filter |
                                                  |  * Time Window   |
                                                  +--------+---------+
                                                           |
                                              +------------+------------+
                                              |                         |
                                              v                         v
                                      +---------------+       +---------------+
                                      |  SHORT Setups |       |  LONG Setups  |
                                      |  * A_MOD      |       |  * A_MOD      |
                                      |  * A_PULLBACK |       |  * A_PULLBACK |
                                      |  * B_HUGE     |       |  * B_HUGE     |
                                      +-------+-------+       +-------+-------+
                                              |                       |
                                              +-----------+-----------+
                                                          v
                                                  +---------------+
                                                  | Simulate Exits|
                                                  | (15m or 5m)   |
                                                  | SL/TGT/BE/    |
                                                  | TRAIL/EOD     |
                                                  +-------+-------+
                                                          v
                                                  +---------------+
                                                  | Slippage +    |
                                                  | Commission    |
                                                  | (5bps + 3bps) |
                                                  +-------+-------+
                                                          v
                                                  +---------------+
                                                  | Metrics +     |
                                                  | Charts + CSV  |
                                                  +---------------+
```

### 3.2 Live Trading Flow (Real-Time Signal Generation + Execution)

```
 +------------------+     +------------------+     +------------------+
 |  Kite/Broker     |     |  EOD Scheduler   |     |  15m Parquet     |
 |  Auth Session    |---->|  Updates Data    |---->|  Files (Latest)  |
 |  (TOTP Login)    |     |  Every 15 mins   |     |  Per Ticker      |
 +------------------+     +------------------+     +--------+---------+
                                                             |
                                                             v
                                                  +------------------+
                                                  |  Live Signal     |
                                                  |  Generator       |
                                                  |  (Runs every 15m |
                                                  |   at candle close)|
                                                  +--------+---------+
                                                           |
                                              +------------+------------+
                                              v                         v
                                      +---------------+       +---------------+
                                      |  SHORT Scan   |       |  LONG Scan    |
                                      |  (same rules  |       |  (same rules  |
                                      |  as backtest)  |       |  as backtest)  |
                                      +-------+-------+       +-------+-------+
                                              |                       |
                                              +-----------+-----------+
                                                          v
                                                  +------------------+
                                                  |  ML Meta-Label   |
                                                  |  Gate (Optional) |
                                                  |  p_win >= 0.60?  |
                                                  +--------+---------+
                                                           v
                                                  +------------------+
                                                  |  Deduplicate +   |
                                                  |  Write signals   |
                                                  |  CSV             |
                                                  +--------+---------+
                                                           v
                                              +------------+------------+
                                              v                         v
                                      +---------------+       +------------------+
                                      |  Paper Trade  |       |  Real Execution  |
                                      |  Executor     |       |  (Zerodha/Kite   |
                                      |  (Simulation) |       |   Order Place)   |
                                      +---------------+       +------------------+
```

### 3.3 SHORT Entry Decision Tree

```
                        +-------------------+
                        | For each 15m bar  |
                        +---------+---------+
                                  |
                                  v
                     +------------------------+
                     |  Is it a RED candle?    |--No--> Skip
                     |  (close < open)         |
                     +-----------+------------+
                                 | Yes
                                 v
                     +------------------------+
                     |  Classify impulse:      |
                     |  body >= 0.45*ATR AND   |--No--> Skip
                     |  body <= 1.0*ATR AND    |
                     |  close near low (25%)?  |
                     |  => MODERATE            |
                     |                         |
                     |  body >= 1.6*ATR OR     |
                     |  range >= 2.0*ATR?      |
                     |  => HUGE                |
                     +-----------+------------+
                                 | MODERATE or HUGE
                                 v
                     +------------------------+
                     |  Volume >= 1.2x avg?    |--No--> Skip
                     +-----------+------------+
                                 | Yes
                                 v
                     +------------------------+
                     |  Trend Filter:          |
                     |  * ADX >= 25, rising    |
                     |  * RSI <= 55, falling   |--No--> Skip
                     |  * Stoch K < D, <=75    |
                     |  * EMA20 < EMA50        |
                     |  * Close < EMA20        |
                     |  * Close < AVWAP        |
                     +-----------+------------+
                                 | All pass
                                 v
                +----------------+----------------+
                |                                 |
                v                                 v
        +---------------+               +---------------+
        |  MODERATE:    |               |  HUGE:        |
        |  Setup A      |               |  Setup B      |
        |               |               |               |
        |  Wait for     |               |  Wait for 1-3 |
        |  next bar to  |               |  small green  |
        |  break impulse|               |  bounce bars  |
        |  low (-buffer)|               |  that FAIL to |
        |               |               |  close above  |
        |  OR: small    |               |  AVWAP, then  |
        |  green pull-  |               |  break bounce |
        |  back then    |               |  low (-buffer)|
        |  break its low|               |               |
        +-------+-------+               +-------+-------+
                |                               |
                +---------------+---------------+
                                v
                     +------------------------+
                     |  AVWAP Rejection OK?    |
                     |  (touch+reject OR 2+   |--No--> Skip
                     |   consecutive closes    |
                     |   below AVWAP)          |
                     +-----------+------------+
                                 | Yes
                                 v
                     +------------------------+
                     |  Close confirms entry?  |--No--> Skip
                     |  (close < trigger)      |
                     +-----------+------------+
                                 | Yes
                                 v
                     +------------------------+
                     |   >>> ENTER SHORT <<<  |
                     +------------------------+
```

### 3.4 Exit Management Flow (Both Sides)

```
                     +------------------------+
                     |   TRADE IS OPEN        |
                     |   Track bar-by-bar     |
                     +-----------+------------+
                                 |
                                 v
                     +------------------------+
                +----| Did price hit SL?      |--Yes--> EXIT (SL)
                |    +-----------+------------+
                |                | No
                |                v
                |    +------------------------+
                |    | Did price hit TARGET?   |--Yes--> EXIT (TARGET)
                |    +-----------+------------+
                |                | No
                |                v
                |    +------------------------+
                |    | Price moved 0.50%/0.60% |
                |    | in our favor?           |--Yes--+
                |    +-----------+------------+       |
                |                | No                  v
                |                |            +---------------+
                |                |            | ARM BREAKEVEN |
                |                |            | Move SL to    |
                |                |            | entry + 0.01% |
                |                |            +-------+-------+
                |                |                    |
                |                |                    v
                |                |            +---------------+
                |                |            | TRAILING STOP |
                |                |            | Trail 0.30%   |
                |                |            | from best     |
                |                |            | price seen    |
                |                |            +-------+-------+
                |                |                    |
                |                v                    v
                |    +------------------------+
                |    | Is it market close?     |--Yes--> EXIT (EOD)
                |    | (15:30 IST)            |
                |    +-----------+------------+
                |                | No
                |    +-----------v------------+
                +--->| Next bar...            |
                     +------------------------+
```

---

## 4. Indicators Used (Explained Simply)

### ATR (Average True Range) -- Period: 14
**What it measures**: How much a stock moves on average per bar (volatility).
**Why we use it**: To size impulse candles relative to "normal" movement. A candle body of 0.5x ATR is a decent move; 1.6x ATR is huge.

### RSI (Relative Strength Index) -- Period: 14
**What it measures**: Whether a stock is overbought (>70) or oversold (<30).
**Why we use it**: For SHORT, we want RSI <= 55 and falling (bearish momentum). For LONG, RSI >= 45 and rising (bullish momentum).

### Stochastic Oscillator (%K / %D) -- Period: 14/3
**What it measures**: Where the current close sits within the recent high-low range.
**Why we use it**: For SHORT, %K should be below %D and <= 75 (bearish crossover). For LONG, %K above %D and >= 25 (bullish crossover).

### ADX (Average Directional Index) -- Period: 14
**What it measures**: How strong the current trend is (regardless of direction). ADX > 25 = trending.
**Why we use it**: We only trade when there is a real trend (ADX >= 25 and rising).

### EMA 20 & EMA 50 (Exponential Moving Averages)
**What they measure**: Smoothed price trends over 20 and 50 bars.
**Why we use them**: EMA alignment confirms the trend direction. SHORT needs EMA20 < EMA50 (bearish). LONG needs EMA20 > EMA50 (bullish).

### AVWAP (Anchored Volume-Weighted Average Price)
**What it measures**: The average price weighted by volume, anchored from market open each day.
**Why we use it**: AVWAP acts as an institutional reference. For SHORT, price should be below AVWAP (sellers in control). For LONG, price should be above AVWAP (buyers in control).

**AVWAP Formula**: `AVWAP = cumulative(TypicalPrice * Volume) / cumulative(Volume)` where TypicalPrice = (High + Low + Close) / 3

### Volume SMA (20-period Simple Moving Average of Volume)
**Why we use it**: The impulse bar must have above-average volume (>= 1.2x average), confirming institutional participation.

---

## 5. Entry Logic -- SHORT Side

### Setup A: MODERATE Impulse

**Step 1 -- Detect red impulse candle (C1)**:
- Must be a RED candle (close < open)
- Body size between 0.45x ATR and 1.0x ATR
- Close must be near the low (within 25% of the candle's range)

**Step 2 -- Option 1: Break C1 low on next bar**:
- On the NEXT bar (C2), the low must break below C1's low minus a buffer
- Buffer = max(Rs.0.05, price * 0.02%)

**Step 2 -- Option 2: Pullback then break**:
- C2 is a small green pullback (body <= 0.20x ATR) that stays below AVWAP
- On C3, the low breaks below C2's low minus buffer

**Step 3 -- Validate all filters** (see Section 8)

### Setup B: HUGE Impulse

**Step 1 -- Detect huge red impulse candle (C1)**:
- Body >= 1.6x ATR, OR range >= 2.0x ATR

**Step 2 -- Wait for failed bounce (1-3 bars)**:
- At least one small green candle (body <= 0.20x ATR)
- The bounce must FAIL to close above AVWAP (touch high >= AVWAP but close < AVWAP)

**Step 3 -- Break the bounce low**:
- A subsequent bar's low breaks below the bounce window's lowest low minus buffer
- Price must still be below AVWAP at entry

### Default SHORT Parameters
| Parameter | Value | Meaning |
|-----------|-------|---------|
| Stop-Loss | 0.75% | Exit if price rises 0.75% against you |
| Target | 1.20% | Exit if price falls 1.20% in your favor |
| BE Trigger | 0.50% | Move SL to breakeven after 0.50% favorable move |
| Trail | 0.30% | Trail SL 0.30% from best price after BE |
| R:R Ratio | 1.6:1 | Risk-reward = 1.20% target / 0.75% stop |

---

## 6. Entry Logic -- LONG Side

### Setup A: MODERATE Impulse

**Step 1 -- Detect green impulse candle (C1)**:
- Must be a GREEN candle (close > open)
- Body size between 0.30x ATR and 1.0x ATR (note: lower threshold than SHORT)
- Close must be near the high (within 25% of the candle's range)

**Step 2 -- Option 1: Break C1 high on next bar**:
- On the NEXT bar, the high must break above C1's high plus buffer

**Step 2 -- Option 2: Pullback then break** (disabled by default):
- C2 is a small red pullback that stays above AVWAP, then C3 breaks C2's high

**Step 3 -- Validate all filters**

### Setup B: HUGE Impulse

**Step 1 -- Detect huge green impulse candle (C1)**:
- Body >= 1.6x ATR, OR range >= 2.0x ATR

**Step 2 -- Wait for pullback that holds (1-3 bars)**:
- Small red candles (body <= 0.20x ATR)
- Pullback lows must stay above C1's mid-body OR closes must stay above AVWAP

**Step 3 -- Break the pullback high**:
- A subsequent bar's high breaks above the pullback window's highest high plus buffer

### Default LONG Parameters
| Parameter | Value | Meaning |
|-----------|-------|---------|
| Stop-Loss | 0.75% | Exit if price falls 0.75% against you |
| Target | 1.50% | Exit if price rises 1.50% in your favor |
| BE Trigger | 0.60% | Move SL to breakeven after 0.60% favorable move |
| Trail | 0.30% | Trail SL 0.30% from best price after BE |
| R:R Ratio | 2.0:1 | Risk-reward = 1.50% target / 0.75% stop |

---

## 7. Exit Management

The exit system runs bar-by-bar after entry. Priority order when multiple conditions trigger on the same bar: **SL wins ties over TARGET** (conservative approach).

### Exit Types

| Exit Type | Meaning | When It Triggers |
|-----------|---------|------------------|
| **TARGET** | Profit target hit | Price moves to target level |
| **SL** | Stop-loss hit | Price moves to stop-loss level |
| **BE** | Breakeven exit | SL was moved to breakeven, then hit |
| **EOD** | End-of-day close | Market closes (15:30 IST) with position still open |

### Breakeven + Trailing Stop Mechanism

1. **Initial state**: SL is at the fixed stop-loss level
2. **BE Trigger**: When price moves 0.50% (SHORT) or 0.60% (LONG) in your favor, the SL is moved to entry price + 0.01% (a tiny pad so you don't lose money)
3. **Trailing Stop**: After BE is armed, the SL "trails" the best price seen by 0.30%. The trailing stop can only get tighter (never loosens)
4. **Result**: You lock in partial gains instead of giving back to flat breakeven

---

## 8. Every Parameter Explained

### 8.1 Direction & Data Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `side` | "SHORT" or "LONG" | Which direction the config is for |
| `dir_15m` | `stocks_indicators_15min_eq` | Folder containing 15-minute parquet files |
| `end_15m` | `_stocks_indicators_15min.parquet` | File suffix for 15m data files |
| `parquet_engine` | `pyarrow` | Engine used to read parquet files |

### 8.2 Risk Parameters

| Parameter | SHORT | LONG | Description |
|-----------|-------|------|-------------|
| `stop_pct` | 0.0075 (0.75%) | 0.0075 (0.75%) | Maximum loss per trade before exit |
| `target_pct` | 0.0120 (1.20%) | 0.0150 (1.50%) | Profit target per trade |

### 8.3 Slippage & Commission

| Parameter | Value | Description |
|-----------|-------|-------------|
| `slippage_pct` | 0.0005 (5 bps) | Price impact on entry/exit (one-way) |
| `commission_pct` | 0.0003 (3 bps) | Round-trip brokerage + STT (approximate) |

**How slippage works**: For SHORT, entry price is reduced by 5 bps (you sell lower), exit price is increased by 5 bps + 3 bps (you buy higher). For LONG, entry price is increased by 5 bps, exit price is reduced by 5 bps + 3 bps. Total cost per trade approx 16 bps.

### 8.4 Impulse Detection Parameters

| Parameter | SHORT | LONG | Description |
|-----------|-------|------|-------------|
| `mod_impulse_min_atr` | 0.45 | 0.30 | Min body size for MODERATE impulse (x ATR) |
| `mod_impulse_max_atr` | 1.00 | 1.00 | Max body size for MODERATE impulse (x ATR) |
| `huge_impulse_min_atr` | 1.60 | 1.60 | Min body size for HUGE impulse (x ATR) |
| `huge_impulse_min_range_atr` | 2.00 | 2.00 | Alt: min range for HUGE impulse (x ATR) |
| `close_near_extreme_max` | 0.25 | 0.25 | Close must be within 25% of candle extreme |
| `small_counter_max_atr` | 0.20 | 0.20 | Max body size for pullback/bounce candles (x ATR) |

### 8.5 Entry Buffer

| Parameter | Value | Description |
|-----------|-------|-------------|
| `buffer_abs` | Rs. 0.05 | Minimum absolute buffer below/above trigger |
| `buffer_pct` | 0.0002 (2 bps) | Percentage buffer (whichever is larger is used) |

### 8.6 Session & Time Windows

| Parameter | Value | Description |
|-----------|-------|-------------|
| `session_start` | 09:15 | Market open (IST) |
| `session_end` | 15:30 | Market close (IST) |
| `use_time_windows` | True | Enable signal time windowing |
| `signal_windows` | [(09:15, 14:30)] | Only generate signals within this window |

### 8.7 Trend Filter Parameters

| Parameter | SHORT | LONG | Description |
|-----------|-------|------|-------------|
| `adx_min` | 25.0 | 25.0 | Minimum ADX value (confirms trending market) |
| `adx_slope_min` | 1.25 | 0.80 | Minimum ADX rise over 2 bars (trend strengthening) |
| `rsi_max_short` | 55.0 | -- | RSI must be <= 55 for SHORT (bearish) |
| `rsi_min_long` | -- | 45.0 | RSI must be >= 45 for LONG (bullish) |
| `stochk_max` | 75.0 | 95.0 | Stochastic %K upper limit |
| `stochk_min` | -- | 25.0 | Stochastic %K lower limit (LONG) |

**Additional trend requirements**:
- ADX must be increasing for 2 consecutive bars
- RSI must be decreasing (SHORT) / increasing (LONG) for 2 consecutive bars
- Stochastic %K must be below %D (SHORT) / above %D (LONG) and decreasing/increasing
- EMA alignment: SHORT needs EMA20 < EMA50, close < EMA20, close < AVWAP; LONG is the mirror

### 8.8 Volatility & Volume Filters

| Parameter | Value | Description |
|-----------|-------|-------------|
| `use_atr_pct_filter` | True | Enable ATR% filter (skip low-volatility stocks) |
| `atr_pct_min` | 0.0020 (0.20%) | Min ATR as % of price (ensures enough movement) |
| `use_volume_filter` | True | Enable volume filter |
| `volume_sma_period` | 20 | Lookback for average volume |
| `volume_min_ratio` | 1.2 | Impulse bar volume must be >= 1.2x average |

### 8.9 AVWAP Rule Parameters

| Parameter | Value | Description |
|-----------|-------|-------------|
| `require_avwap_rule` | True | Enforce AVWAP validation |
| `avwap_touch` | True | Require touch evidence (high/low touches AVWAP, close rejects) |
| `avwap_min_consec_closes` | 2 | Min consecutive closes on the correct side of AVWAP |
| `avwap_mode` | "any" | "any" = touch OR consecutive; "both" = touch AND consecutive |
| `avwap_dist_atr_mult` | 0.25 | At entry, price must be >= 0.25x ATR away from AVWAP |

### 8.10 Breakeven & Trailing Stop

| Parameter | SHORT | LONG | Description |
|-----------|-------|------|-------------|
| `enable_breakeven` | True | True | Enable breakeven protection |
| `be_trigger_pct` | 0.0050 (0.50%) | 0.0060 (0.60%) | Favorable move needed to arm BE |
| `be_pad_pct` | 0.0001 (0.01%) | 0.0001 (0.01%) | Tiny pad so BE exit is slightly profitable |
| `enable_trailing_stop` | True | True | Enable trailing stop after BE arms |
| `trail_pct` | 0.0030 (0.30%) | 0.0030 (0.30%) | Trail distance from best favorable price |

### 8.11 Quality & Trade Limits

| Parameter | Value | Description |
|-----------|-------|-------------|
| `max_trades_per_ticker_per_day` | 1 | Max entries per stock per day (avoids overtrading) |
| `require_entry_close_confirm` | True | Entry bar's close must confirm the breakout direction |
| `min_bars_left_after_entry` | 4 | Need at least 4 bars remaining (1 hour) after entry |
| `min_bars_for_scan` | 7 | Skip days with fewer than 7 bars of data |
| `enable_topn_per_day` | True | Limit total trades per day by quality ranking |
| `topn_per_day` | 10 | Keep only the top 10 trades per day (by quality score) |

### 8.12 Signal-to-Entry Lag Controls (in 15-min bars)

| Parameter | Value | Description |
|-----------|-------|-------------|
| `lag_bars_short_a_mod_break_c1_low` | 1 | MODERATE SHORT: enter 1 bar after impulse |
| `lag_bars_short_a_pullback_c2_break_c2_low` | 2 | PULLBACK SHORT: enter 2 bars after impulse |
| `lag_bars_short_b_huge_failed_bounce` | -1 | HUGE SHORT: dynamic (first valid bar) |
| `lag_bars_long_a_mod_break_c1_high` | 1 | MODERATE LONG: enter 1 bar after impulse |
| `lag_bars_long_a_pullback_c2_break_c2_high` | 2 | PULLBACK LONG: enter 2 bars after impulse |
| `lag_bars_long_b_huge_pullback_hold_break` | -1 | HUGE LONG: dynamic (first valid bar) |

### 8.13 Portfolio & Leverage (Runner-Level)

| Parameter | Value | Description |
|-----------|-------|-------------|
| `POSITION_SIZE_RS_SHORT` | Rs. 50,000 | Capital (margin) per SHORT trade |
| `POSITION_SIZE_RS_LONG` | Rs. 1,00,000 | Capital (margin) per LONG trade |
| `INTRADAY_LEVERAGE_SHORT` | 5x | MIS leverage for SHORT trades |
| `INTRADAY_LEVERAGE_LONG` | 5x | MIS leverage for LONG trades |
| `PORTFOLIO_START_CAPITAL_RS` | Rs. 10,00,000 | Starting capital for portfolio simulation |

**Notional exposure** = Capital x Leverage. E.g., SHORT: Rs.50,000 x 5 = Rs.2,50,000 notional.

### 8.14 Quality Score Formula

**SHORT quality score** (0 to 1):
```
score = 0.45 * adx_normalized
      + 0.35 * avwap_distance_normalized
      + 0.10 * ema_gap_normalized
      + 0.10 * impulse_bonus (1.0 if HUGE, 0.6 if MODERATE)
```

**LONG quality score**:
```
score = 0.04 * ADX
      + 0.20 * ADX_slope_2bar
      + 1.20 * AVWAP_distance_in_ATR
      + 0.80 * EMA_gap_in_ATR
      + 0.25 (if HUGE impulse)
```

---

## 9. ML Meta-Label Overlay

The ML layer is **optional** -- it sits ON TOP of the base strategy and filters/sizes trades.

### How It Works

```
+------------------+     +------------------+     +------------------+
|  Base AVWAP      |     |  ML Model        |     |  Filtered Trades |
|  Trade Signal    |---->|  predict_pwin()  |---->|  (high confidence|
|                  |     |                  |     |   trades only)   |
|  Features:       |     |  p_win >= 0.60?  |     |                  |
|  * quality       |     |  Yes: TAKE trade |     |  Position sized  |
|  * atr_pct       |     |  No:  SKIP trade |     |  by confidence   |
|  * rsi           |     |                  |     |  multiplier      |
|  * adx           |     |  Conf. mult:     |     |  (0.8x to 1.6x) |
|  * side          |     |  0.8x to 1.6x   |     |                  |
+------------------+     +------------------+     +------------------+
```

### Feature Engineering
| Feature | Formula | Range |
|---------|---------|-------|
| `quality_score` | Raw quality score from strategy | 0 to ~3 |
| `atr_pct` | ATR / Close price | 0.002+ |
| `rsi_centered` | (RSI - 50) / 50 | -1 to +1 |
| `adx_norm` | ADX / 50 | 0 to ~2 |
| `side` | +1 for LONG, -1 for SHORT | -1 or +1 |

### Training Pipeline (Triple Barrier Labeling)

1. **Input**: Candidate trades CSV from backtest runner
2. **Label creation**: Look forward 12 bars (60 min) in 5-minute data
   - Label = 1 if TARGET hit before SL (winning trade)
   - Label = 0 if SL hit first or timeout (losing trade)
   - Conservative: if both hit in same bar -> Label = 0
3. **Model**: LogisticRegression (balanced class weights), walk-forward validated
4. **Output**: `meta_model.pkl` + `meta_features.json`

### Confidence Multiplier
- p_win < threshold (0.60) -> **multiplier = 0** (skip trade entirely)
- p_win = threshold -> multiplier = 0.8x (reduce size)
- p_win = 1.0 -> multiplier = 1.6x (increase size)
- Linear interpolation between 0.8x and 1.6x above threshold

---

## 10. Backtesting Pipeline

### Step-by-Step Guide

```bash
# Step 1: Ensure you have historical 15-minute parquet data
# (should already exist in stocks_indicators_15min_eq/)

# Step 2: Run the combined backtest
python -m avwap_v11_refactored.avwap_combined_runner

# Step 3: Check outputs
# -> outputs/avwap_longshort_trades_ALL_DAYS_YYYYMMDD_HHMMSS.csv
# -> outputs/avwap_combined_runner_YYYYMMDD_HHMMSS.txt
# -> outputs/charts/enhanced/*.png  (20 charts)
# -> outputs/charts/legacy/*.png    (8 charts)
```

### What the Backtest Runner Does

1. **Phase 1**: Scans all tickers (parallel, 4 workers) for SHORT + LONG entry signals using 15-minute data
2. **Phase 2**: Re-resolves exits using 5-minute data for higher precision SL/target tracking
3. **Phase 3**: Applies leverage-aware P&L (capital ROI + notional rupees)
4. **Phase 4**: Computes comprehensive metrics (Sharpe, Sortino, Calmar, drawdown, profit factor, win rate)
5. **Phase 5**: Generates 28+ charts (legacy + enhanced suites)
6. **Phase 6**: Exports CSV trade log + console log

### Key Metrics Computed

| Metric | What It Tells You |
|--------|-------------------|
| Win Rate | % of trades that are profitable |
| Profit Factor | Gross profit / Gross loss (>1 = profitable) |
| Sharpe Ratio | Risk-adjusted return (annualized, >1 is good) |
| Sortino Ratio | Like Sharpe but only penalizes downside volatility |
| Calmar Ratio | Total return / Max drawdown |
| Max Drawdown | Worst peak-to-trough decline |
| Avg Win / Avg Loss | Average sizes of winning vs losing trades |

---

## 11. Live Trading Pipeline

### Step-by-Step Guide

```bash
# Step 1: Authenticate with broker (Zerodha Kite)
python authentication_v2.py

# Step 2: Start data updater (keeps 15m parquet files fresh)
python eqidv2_eod_scheduler_for_15mins_data.py

# Step 3: Start live signal generator (scans every 15 minutes)
python avwap_live_signal_generator.py --ml-threshold 0.62

# Step 4: Start paper trade executor (watches signal CSV)
python avwap_trade_execution_PAPER_TRADE_TRUE.py

# Step 5: For real execution (after paper trade validation):
python avwap_trade_execution_PAPER_TRADE_FALSE.py
```

### Risk Controls (Paper/Live)

| Control | Value | Description |
|---------|-------|-------------|
| Max daily loss | Rs. 5,000 | Stop taking new trades after this loss |
| Max open positions | 10 | Concurrent position limit |
| Max capital deployed | Rs. 5,00,000 | Total margin limit |
| Forced close time | 15:20 IST | Close all positions before broker square-off |
| Slippage simulation | 5 bps | Applied to entry price |
| LTP poll interval | 5 seconds | Price check frequency |

---

## 12. File-by-File Map

### Core Strategy (avwap_v11_refactored/)

| File | Purpose |
|------|---------|
| `avwap_common.py` | Shared backbone: StrategyConfig, indicators, slippage model, Trade dataclass, metrics, charts |
| `avwap_short_strategy.py` | SHORT logic: red impulse, AVWAP rejection, SHORT exit, trend filter |
| `avwap_long_strategy.py` | LONG logic: green impulse, AVWAP support, LONG exit, trend filter |
| `avwap_combined_runner.py` | Orchestrator: parallel scan, 5m exit resolution, leverage P&L, 20 charts |

### Live Signal Generation

| File | Purpose |
|------|---------|
| `avwap_live_signal_generator.py` | Runs every 15m, detects entries, applies ML gate, writes signal CSV |
| `eqidv2_live_combined_analyser_csv.py` | Live signal analyser (runner-parity detection) |

### Trade Execution

| File | Purpose |
|------|---------|
| `avwap_trade_execution_PAPER_TRADE_TRUE.py` | Paper executor: watchdog CSV, LTP polling, risk limits |
| `avwap_trade_execution_PAPER_TRADE_FALSE.py` | Real executor: Zerodha/Kite order placement |
| `authentication_v2.py` | Kite login with TOTP, token generation |

### Data Ingestion

| File | Purpose |
|------|---------|
| `trading_data_..._parquet_stocksonly.py` | Multi-timeframe (5m+15m) historical data builder |
| `trading_data_..._15minonly.py` | 15-minute only data builder (lighter) |
| `eqidv2_eod_scheduler_for_15mins_data.py` | Scheduled 15m data refresh during market hours |
| `eqidv2_eod_scheduler_for_1540_update.py` | One-shot 15:40 IST data capture |

### ML Pipeline

| File | Purpose |
|------|---------|
| `ml_meta_filter.py` | ML inference: MetaLabelFilter, predict_pwin(), confidence_multiplier() |
| `eqidv2_meta_label_triple_barrier.py` | Triple-barrier labeling from 5-min candles |
| `eqidv2_meta_train_walkforward.py` | Walk-forward model training + evaluation |
| `eqidv2_ml_backtest.py` | Apply ML filter to historical trades, compute improvement |

### Universe & Config

| File | Purpose |
|------|---------|
| `filtered_stocks.py` | Curated stock universe list |
| `models/meta_model.pkl` | Trained ML model (serialized) |
| `models/meta_features.json` | Feature schema for ML model |

---

## 13. Typical Workflows

### A) Run a Full Backtest

```bash
python -m avwap_v11_refactored.avwap_combined_runner
# Review: outputs/*.csv, outputs/charts/enhanced/*.png
```

### B) Train the ML Model

```bash
python eqidv2_meta_label_triple_barrier.py \
  --trades-csv outputs/avwap_longshort_trades_ALL_DAYS_*.csv \
  --candles-dir stocks_indicators_5min_eq \
  --out-csv datasets/meta_dataset.csv --horizon-bars 12

python eqidv2_meta_train_walkforward.py

python eqidv2_ml_backtest.py \
  --input-csv outputs/avwap_longshort_trades_ALL_DAYS_*.csv --threshold 0.60
```

### C) Go Live (Paper First)

```bash
python authentication_v2.py
python eqidv2_eod_scheduler_for_15mins_data.py &
python avwap_live_signal_generator.py --ml-threshold 0.62 &
python avwap_trade_execution_PAPER_TRADE_TRUE.py
```

---

## 14. Output Files & Charts

### CSV Trade Log Columns

| Column | Description |
|--------|-------------|
| `trade_date` | Date of trade (YYYY-MM-DD) |
| `ticker` | Stock symbol (e.g., RELIANCE, TCS) |
| `side` | SHORT or LONG |
| `setup` | Setup type (A_MOD_BREAK_C1_LOW, B_HUGE_RED_FAILED_BOUNCE, etc.) |
| `impulse_type` | MODERATE or HUGE |
| `entry_price` / `exit_price` | Entry and exit prices |
| `sl_price` / `target_price` | Stop-loss and target levels |
| `outcome` | TARGET / SL / BE / EOD |
| `pnl_pct` | Net P&L % (after slippage + commission) |
| `quality_score` | Signal quality ranking |
| `pnl_rs` | P&L in Rupees (notional) |

### Chart Suite (20 Enhanced Charts)

1. Cumulative P&L (Combined/Short/Long)
2. Daily P&L Bar Chart
3. Equity Curve + Drawdown
4. Win Rate by Side
5. P&L Distribution (Combined)
6. P&L Distribution by Side
7. Outcome Breakdown (Pie)
8. Outcome by Side (Grouped Bar)
9. Monthly P&L Heatmap
10. Weekday P&L Analysis
11. Hourly Entry Distribution
12. Rolling Sharpe Ratio
13. Trade Duration Distribution
14. Top 10 Winners & Losers
15. Cumulative Trade Count
16a. P&L by Setup Type
16b. P&L by Impulse Type
17. Quality Score vs P&L Scatter
18. Win Rate by Month
19. Average P&L by Hour
20. Risk-Reward Scatter

---

## 15. Glossary

| Term | Definition |
|------|-----------|
| **AVWAP** | Anchored Volume-Weighted Average Price, anchored from market open |
| **ATR** | Average True Range, a measure of volatility over 14 bars |
| **Impulse candle** | A candle with a significantly larger-than-normal body (measured in ATR multiples) |
| **MODERATE** | Body between 0.3-1.0x ATR with close near the extreme |
| **HUGE** | Body >= 1.6x ATR or range >= 2.0x ATR |
| **BE** | Breakeven -- moving the stop-loss to entry price after a favorable move |
| **Trailing stop** | A stop-loss that follows the price at a fixed distance |
| **EOD** | End-of-day forced exit at market close |
| **bps** | Basis points -- 1 bps = 0.01% |
| **Notional exposure** | Capital x Leverage = actual market exposure |
| **MIS** | Margin Intraday Square-off (Zerodha's intraday leverage product) |
| **Parquet** | Columnar file format for efficient data storage |
| **Walk-forward** | Training on past data, testing on unseen future data, rolling forward |
| **Triple barrier** | Labeling method: target (upper), SL (lower), or timeout (vertical) |
| **p_win** | ML model's predicted probability that a trade will be profitable |
| **Quality score** | Composite score ranking how good a setup is |
| **Profit factor** | Total gross profits / Total gross losses |
| **Sharpe ratio** | (Mean return / Std deviation) x sqrt(252) -- risk-adjusted return |

---

*This document was generated from a full code analysis of the EQID v2 codebase.*
