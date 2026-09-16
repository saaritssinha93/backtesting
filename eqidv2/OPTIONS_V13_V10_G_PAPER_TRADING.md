# Options V13-V10-G one-lot paper trading

## Behavior

- Source: an actual filled V13-V10-G equity order state (`OPEN` or `CLOSED` with a positive entry price and entry timestamp).
- Direction: equity `LONG` buys ATM `CE`; equity `SHORT` buys ATM `PE`.
- Size: exactly one current exchange lot from the same-day NFO instrument master.
- ATM anchor: the actual equity fill price. The chosen option contract is frozen for the trade.
- Entry: current NFO best ask plus 10 bps adverse slippage, tick rounded upward.
- Exit: 30% premium stop, 40.4% premium target, or 15:15 IST square-off. A sell uses the best bid with 10 bps adverse slippage, tick rounded downward.
- Capital: Rs15 lakh full-premium paper account with charges reserved and reconciled.
- Source preference: a filled quantity-one LIVE equity state is preferred; the canonical G PAPER fill is used when no filled LIVE state exists for that signal.

The options runtime is paper-only and contains no broker order call.

## Sessions

| Start | Session |
|---|---|
| 09:07 | FnO ATM CE/PE Options Fetch (5-Minute + 1-Minute) |
| 09:15 | Options V13-V10-G LONG ATM CE Buy Paper Entry Session |
| 09:15 | Options V13-V10-G SHORT ATM PE Buy Paper Entry Session |
| 09:15 | Options V13-V10-G Continuous Paper Trade Log |
| 09:15 | Options V13-V10-G Paper Net Result |

The data fetcher uses the configured app1 through app8 Kite lanes with per-app pacing, authentication failover, rate-limit backoff, atomic archive writes, map-drift detection, and completeness markers. The entry workers rotate the same eight credentials for read-only NFO quote failover.

## Entry safeguards

- Exact V13-V10-G strategy version and fingerprint
- Actual equity fill only; a signal or pending equity entry cannot trigger an option entry
- Same-day option master and exact monthly expiry
- Deterministic nearest strike, with lower strike as the tie-break
- Exact one-lot quantity and positive tick size/token
- Entry within three minutes of the equity fill; no restart can create a late retrospective entry
- Fresh two-sided market depth, positive volume, and spread no wider than 10%
- Full premium plus charges available before admission
- Shared long/short process lock, atomic JSON/CSV writes, and idempotent signal ID
- Missing square-off quote becomes `UNRESOLVED`; no exit price is invented

## September 15 replay

The replay used the actual equity fill price to select each contract, then the next exact five-minute option candle as the reproducible historical fill proxy. It used native five-minute fallback where one-minute history had gaps.

| Equity | Option bought | Quantity | Exit | Net P&L |
|---|---|---:|---|---:|
| IDEA | IDEA26SEP15PE | 71,475 | Time exit | Rs 6,974.73 |
| ABB | ABB26SEP7100PE | 125 | Target | Rs 6,978.92 |
| APOLLOHOSP | APOLLOHOSP26SEP8900PE | 125 | Target | Rs 6,823.89 |
| POWERINDIA | POWERINDIA26SEP30500PE | 25 | Target | Rs 6,590.70 |
| **Total** |  |  | **4 closed** | **Rs 27,368.23** |

Live paper execution does not wait for the next five-minute boundary: it maps from the equity fill price and uses the first guarded option quote observed within the three-minute entry window.

## Operations

- Install or review tasks: `bat/schedule_fno_v13_v10_g_options_weekday.ps1`
- Runtime: `fno_v13_v10_g_options_paper.py`
- One-lot replay engine: `fno_v13_v10_g_options_one_lot_execution.py`
- State root: `C:\TradingData\eqidv2\fno_oi\v13_v10_g_options_paper`
- Latest reports: `C:\TradingData\eqidv2\fno_oi\latest\latest_fno_v13_v10_g_options_*.md`
