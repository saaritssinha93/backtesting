# V12 distilled late-compression LONG replay

## Verdict

**REJECT_NOT_PROVEN** — one or more immutable profitability, robustness, or sample-size gates failed

This is an isolated research result. `production_approved` remains `false`, and no live/paper configuration was changed.

## Frozen rule

- Strict completed hourly K300 `primary_side == LONG` membership.
- Completed 5-minute signals at 14:00..14:25 IST; old list owns `xx:20`, new list starts at `xx:25`.
- Hourly refresh never exits an open trade or resets ticker/day attempt state; the same ticker in consecutive lists is not re-armed.
- Price >= Rs 50; completed 5-minute traded value >= Rs 1,000,000.
- Previous Bollinger(20,2) bandwidth <= the causal 25th percentile of the twenty bandwidth readings before it.
- Close strictly above the prior 10 completed same-session highs, at/above causal session VWAP.
- Same-time-of-day causal relative volume >= 1.25; NIFTY proxy at/above causal session VWAP.
- First fully qualifying signal per ticker/day is the only armed attempt.
- Buy-stop signal high + one Rs 0.05 tick for the next 3 one-minute bars; cancel checked before trigger; gap cap 0.20%.
- Quantity is min(Rs 100,000 notional, 2% of completed-5m-volume/5); 42 filled trades were capacity-limited.
- Entry slippage 5 bps; stop 0.70%; target 0.75%; non-target exit slippage 5 bps; forced exit 15:15; full statutory costs.

## Results

| Window | Sessions | Trades | Net PF | Net P&L | Win rate |
|---|---:|---:|---:|---:|---:|
| Full | 44 | 81 | 0.421 | Rs -12,612.96 | 35.8% |
| First half | 22 | 40 | 0.474 | Rs -6,185.07 | 37.5% |
| Second half | 22 | 41 | 0.359 | Rs -6,427.89 | 34.1% |

Full-window PF before statutory costs was `0.577`; the rule therefore lost even before costs.

Robustness: day-bootstrap 95% lower PF `0.226`; after removing the five best **trades** PF `0.269`; after removing the five best **days** PF `0.204`; costs +25% PF `0.389`.

## Evidence gate

- Frozen honest-validator decision: **REJECT**.
- Failed checks: at_least_300_trades, at_least_60_sessions, at_least_40_active_days, net_pf_at_least_1p60, first_half_net_pf_at_least_1p10, first_half_net_pnl_positive, second_half_net_pf_at_least_1p10, second_half_net_pnl_positive, bootstrap_95pct_lower_pf_at_least_1p20, top5_removed_pf_at_least_1p20, cost_plus_25pct_stressed_pf_at_least_1p20.
- The requested two complete months contain only 44 sessions, so the >=60-session / >=300-trade promotion gate cannot pass on this replay alone.
- The hourly lists use a reconstructed static-current universe and are not point-in-time constituent data; survivorship bias remains possible.
- The NIFTY gate uses the repository's end-labelled traded NIFTYBEES proxy because the cash index has zero volume and no true traded VWAP.
- NIFTY signal-grid coverage: 263/264; missing/invalid bars fail closed. Missing pre-qualified candidate rows: 3.

## Boundary and implementation audit

- Signals at `xx:20`: 28.
- All such signals owned by the previous hourly list: True.
- Existing production setup was not overwritten: `L_LATE_BB10_COMPRESSION_BREAKOUT`.
- Setup ID: `L_V12_HOURLY_LATE_COMPRESSION_SIMPLE_RESEARCH`.
