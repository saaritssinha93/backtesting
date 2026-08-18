# Six-month frozen L009216 V12 backtest

## Result

- Window: 2026-02-05 through 2026-08-04, 120 sessions.
- Trades: 448 (3.73/session; median
  3.0).
- Active sessions: 117/120.
- Net P&L: Rs -39,911.76.
- Profit factor: 0.718.
- Win rate: 35.9%.
- Max drawdown: Rs -53,250.05, realized-exit order.

## Configuration fidelity

`L009216_PULLBACK_BOUNCE` was loaded from the frozen configuration artifact.  No
threshold, rank, time window, stop, target, sizing, cost, or fallback setting
was optimized or changed.  The original July 6-August 4 result reproduced
exactly: 66 trades, net Rs 11,150.59,
PF 1.833.

## Earlier period versus selected month

- Earlier February 5-July 3: 382 trades,
  3.90/session, net Rs
  -51,062.35, PF 0.601.
- Original July 6-August 4 selection month: 66 trades,
  3.00/session, net Rs
  11,150.59, PF 1.833.

## Monthly results

- 2026-02: 52 trades, 3.06/session, net Rs -2,888.34, PF 0.814.
- 2026-03: 59 trades, 3.11/session, net Rs -8,673.00, PF 0.571.
- 2026-04: 112 trades, 5.60/session, net Rs -14,128.61, PF 0.624.
- 2026-05: 71 trades, 3.74/session, net Rs -11,248.59, PF 0.531.
- 2026-06: 76 trades, 3.80/session, net Rs -10,971.15, PF 0.581.
- 2026-07: 73 trades, 3.17/session, net Rs 7,631.10, PF 1.467.
- 2026-08: 5 trades, 2.50/session, net Rs 366.83, PF 1.242.

## Interpretation

This is a fixed historical replay, not a new optimization.  It is still not a
genuinely fresh forward holdout because earlier six-month research influenced
the broader strategy-development process.  The reconstructed prefilter used a
static current 1,237-stock universe because no point-in-time universe was
available, creating survivorship-bias risk.  The April 9 and April 10 09:20
prefilter slots ended at rank 243 rather than 300; all other 838 hourly slots
had the complete 101-stock rank band.  `PRODUCTION_APPROVED=False`; no live or
production process was changed, enabled, or restarted.
