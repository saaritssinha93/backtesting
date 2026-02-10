# Backtesting Algorithmic Trading Strategies (Stocks) — A Beginner’s Guide

Backtesting is the process of **testing a trading strategy on historical market data** to understand how it *would have performed* in the past. In algorithmic (algo) trading, backtesting is one of the most important steps before moving to paper trading or live trading.

This README explains the **core ideas, workflow, and best practices** for backtesting stock-market trading algorithms—without assuming prior experience.

---

## What is backtesting?

Backtesting answers questions like:

- If I used *these rules* to buy/sell in the past, **how much would I have made or lost?**
- How often does the strategy win/lose?
- What is the worst drawdown I might face?
- Is performance consistent across different periods and different stocks?

Backtesting does **not** guarantee future results—but it helps you avoid blindly trading an untested idea.

---

## Why backtest?

### ✅ Benefits
- **Validate an idea** before risking real money
- Measure **risk** (drawdown, losing streaks, volatility)
- Compare strategies objectively
- Improve strategy rules and risk controls
- Identify market regimes where the strategy works or fails

### ⚠️ Limitations
Backtests can be misleading if:
- you use unrealistic assumptions (no slippage, no fees)
- you accidentally “peek” into the future (look-ahead bias)
- you over-optimize parameters to the past (overfitting)

---

## What you need to backtest

### 1) Historical data
Common intraday + daily fields:
- **OHLCV**: Open, High, Low, Close, Volume
- Corporate actions adjustments (splits, dividends) for long-term tests
- For intraday: accurate timestamps and session rules

### 2) Strategy rules
A strategy is a set of **deterministic rules**, for example:
- Entry: “Buy when price crosses above 20 EMA and RSI > 50”
- Exit: “Sell at 1% profit target or 0.5% stop-loss”
- Filters: “Trade only between 09:30–14:30”
- Position sizing: “Risk 1% of capital per trade”

### 3) Execution model
You must decide how trades are filled:
- Market order at next candle open?
- Limit order at a certain price?
- Partial fills?
- One trade per symbol per day?

### 4) Costs & frictions
Realism requires:
- Brokerage / commissions
- Slippage (price impact, spreads)
- Taxes (optional but important in India)
- Borrow costs (if shorting, optional/advanced)

### 5) Risk management rules
Examples:
- max trades per day
- max loss per day
- position limits
- stop-loss, trailing stop, break-even stop
- time-based exit (close positions at EOD)

---

## The standard backtesting workflow

### Step 1: Define the hypothesis
Example: “Stocks that reject AVWAP after a strong move tend to mean-revert intraday.”

### Step 2: Build the signal
Convert the idea to code:
- compute indicators (VWAP, AVWAP, RSI, ATR, EMA, etc.)
- detect conditions candle-by-candle

### Step 3: Simulate trades
For each symbol/day:
1. scan candles in chronological order  
2. when entry conditions match → open a position  
3. manage exits (target/SL/time)  
4. record trade outcomes

### Step 4: Measure performance
Key metrics include:
- **Win rate** (% profitable trades)
- **Average win / average loss**
- **Profit factor** (gross profit / gross loss)
- **Expectancy** (average P&L per trade)
- **Max drawdown**
- **Sharpe / Sortino**
- **Consistency** across days, symbols, months

### Step 5: Validate robustness
Make sure you’re not fooling yourself:
- test different time periods (bull/bear/sideways)
- test out-of-sample (OOS) periods
- avoid too many parameters
- evaluate on different stocks/sectors

### Step 6: Paper trade before live
Use a paper trading account or small capital to confirm:
- strategy behaves similarly in real-time
- order execution and latency are acceptable

---

## Important concepts (must-know)

### Look-ahead bias
Using information you wouldn’t have had at that time (future candle values) will make results unrealistically good.

✅ Correct: use only past and current candle data at each step  
❌ Incorrect: using future high/low to decide entry “as if you knew it”

### Survivorship bias
Testing only stocks that exist today can ignore delisted/failed companies—making results look better than reality.

### Overfitting (curve fitting)
If you tune many parameters until backtest looks perfect, it may fail live.

A simple warning sign:
- Amazing backtest results
- Poor performance on unseen data (OOS)

### Regime dependence
A strategy may work only in certain market conditions:
- trending markets
- mean-reverting markets
- high volatility vs low volatility

---

## Position sizing and leverage (intraday)

Backtests should distinguish:
- **Capital / margin**: the money you actually allocate (e.g., ₹50,000)
- **Notional exposure**: capital × leverage (e.g., ₹50,000 × 5 = ₹250,000)

This affects:
- **rupee P&L** (usually driven by notional exposure)
- **ROI% on capital** (pnl / capital)

A realistic backtest reports both:
- price-return %
- ROI% on capital (levered)
- rupee P&L with costs included

---

## What “good” looks like in a backtest

A “good” strategy usually shows:
- positive expectancy over long periods
- drawdowns you can tolerate
- stable performance across symbols and time
- realistic assumptions about costs and slippage

A “too good to be true” strategy often has:
- extremely high win rate with huge profits
- very few losing trades
- too many tuned parameters
- performance collapses in OOS testing

---

## Practical best practices

- Always include **fees + slippage**
- Use **walk-forward / OOS** validation
- Test multiple market regimes and time windows
- Track **trade distribution** (not just averages)
- Keep strategy rules **as simple as possible**
- Maintain clean logs and export results (CSV/TXT)
- Use risk limits: daily loss limit, max positions, circuit breakers

---

## Common backtesting architecture (high-level)

Typical algo backtest system has:

1. **Data Layer**
   - historical candles, clean + consistent
2. **Indicator Layer**
   - compute VWAP/AVWAP/RSI/ATR/EMA etc.
3. **Signal Layer**
   - rule logic to generate entries/exits
4. **Execution Simulator**
   - fills, slippage, costs, position sizing
5. **Portfolio Layer**
   - combines trades across symbols, enforces capital constraints
6. **Reporting Layer**
   - metrics, equity curve, drawdown, logs

---

## Disclaimer

Backtesting is research, not a promise. Markets change, execution differs, and slippage/latency matter.  
Always paper trade first and use proper risk management.

---

## Suggested next steps

If you're building your own backtester, consider adding:
- out-of-sample testing
- parameter sweeps with strict controls
- Monte Carlo resampling of trade order
- realistic fill models (spread-based slippage)
- detailed trade logs and session-aware constraints

---

### Want this README customized?
If you tell me:
- market (NSE/BSE/US)
- timeframe (intraday/daily)
- style (trend/mean reversion)
- broker constraints (leverage, shorting)
I can tailor the README to your exact setup.
