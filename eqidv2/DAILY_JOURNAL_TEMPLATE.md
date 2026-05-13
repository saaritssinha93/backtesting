# Daily Trading Journal — v17D

Copy this template for each session. Fill in 5 minutes after market close.
Stored in `~/v17D_logs/journal_YYYYMMDD.md`.

---

## Session: YYYY-MM-DD

### Capital and ramp
- **Capital deployed:** Rs. _____
- **Pilot stage:** _____ (Day N at 0.10x / 0.30x / 0.50x / 1.00x)
- **Kill-switch state:** clear / armed (reason: _____)

### Performance
- **PnL:** Rs. _____ ( ___% on capital)
- **Trades:** _____ total ( __ LONG / __ SHORT)
- **Win rate:** ____% ( __ winners / __ losers / __ EOD / __ TIME_STOP)
- **PF:** _____
- **Best trade:** _____ (Rs ____ / setup _____ / ticker _____)
- **Worst trade:** _____ (Rs ____ / setup _____ / ticker _____)

### Per-setup count + outcome
| Setup | Side | Trades | Wins | PnL Rs |
|---|---|---|---|---|
| | | | | |

### Governor activity
- G1 time-window drops: _____
- G2 daily side cap drops: _____
- G3 loss-streak halts: _____  (sides halted: _____)
- G4 setup cooling: _____  (setups affected: _____)
- G5 per-setup cap: _____
- G6 rolling PF kill: _____  (setups affected: _____)

### Market regime observation
- NIFTY range: ____% (___pts)
- BANKNIFTY range: ____%
- Predominant move: trending-up / trending-down / choppy / gap-day
- ADX-30+ tickers (estimated count): _____
- Sector standouts: _____

### Anomalies / surprises
- ...
- ...

### Operational notes
- Broker: smooth / issues (_____)
- Data feed: smooth / lag (_____)
- Slippage: as expected / worse than expected (specify: _____)
- Reconciliation: clean / mismatches (specify: _____)
- Drift alerts: none / flagged features (_____)

### Decisions for tomorrow
- Setups to disable: _____
- Setups to size down: _____
- Tickers to restrict: _____
- Config changes (if any): _____
- On-call escalation needed: yes / no (reason: _____)

### Rolling 5-day stats (paste from monitor)
- 5-day PnL: Rs. _____
- 5-day trades: _____
- 5-day PF: _____
- Drawdown from rolling high: ____%
- Days to weekly DD kill (if degrading): _____

### Personal
- Stress level (1–5): _____
- Slept well: yes / no
- Confidence in next session: high / med / low
- Any reason to pause tomorrow: _____

---

## Weekly review (every Friday EOD)
- Week PnL: Rs. _____ ( ___% )
- Week PF: _____
- Worst day: _____
- Best day: _____
- Setups that hit kill-PF threshold this week: _____
- Setups whose rolling PF dropped > 0.5 vs 4 weeks ago: _____
- Drift detection alerts this week: _____
- Any setup graveyard appends: _____  (link: SETUP_GRAVEYARD.md)
- Decision: continue as-is / size down / pause for retune / drop a setup

---

## Monthly review (first Friday of next month)
- Month PnL: Rs. _____  vs target: Rs. _____
- Month PF: _____
- Number of trading days: _____
- Avg trades/day: _____  (target: 4.5+)
- Active setups in production: _____
- Setups in graveyard added this month: _____
- Live PF vs backtest PF (% retention): ____%
- v17D version label: v17D-r___ (bump on any config/code change)
- Roadmap progress: Phase ___, items shipped: _____
