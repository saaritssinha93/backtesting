# G-7 V5 Nifty and OI admission research — 2 November 2026

## Decision

Keep the frozen G-7 V4 lock as the selected policy. G-7 V5 is a separate,
backtest-only experiment. No live or paper strategy was changed.

The requested cutoff was Friday, 30 October 2026. A comparable V4 source is
verified only for **59 sessions, 29 July–27 October**. The 28–30 October
sessions are absent from the frozen V4 source; the raw 30 October replay was
incomplete. This report must not be presented as a through-Friday backtest.

## Methods

The V4 source, selection allowlist, Nifty-futures direction permission, exits,
profit protection, cash sizing, and two-loss daily admission lock were held
fixed. Candidate selection was regenerated before per-slot ranking and quota.
The unchanged selection identities matched the V4 source in all 59 sessions
for the affected slots. The full portfolio replay also matched the published
V4 control exactly before evaluating V5 changes.

Intraday tests changed the 09:51 and 10:46 LONG OI floor from +0.05% to 0%,
−0.10%, or −0.25%, and the candle-body floor from 0.50 to 0.40 or 0.35,
conditionally or unconditionally. The conditional gate used completed Nifty
spot bars with day gain at least 0.25%, nonnegative last-15-minute move,
positive move from the opening, price above the 20-bar EMA, at least 55% of
stocks above prior close, at least 50% above opening, 95% source coverage,
and two consecutive qualifying checks. The original stricter strong-market
gate activated at neither 09:50 nor 10:45 in the 59-session calendar, so it
could not test a material admission change.

Opening tests used only the completed 09:20 Nifty and stock bars: Nifty up at
least 0.25% or 0.50% versus the prior close, positive change from the 09:20
open, at least 60% of stocks above prior close, at least 50% above opening,
and at least 95% source coverage. Eligible slots were 09:21, 09:26, 09:41,
and 09:51 LONG. The 09:36 and 09:46 candidate reconstruction produced eight
extra baseline signals versus frozen V4, so those two slots were excluded
instead of mixing incompatible starting portfolios. All changed rules were
replayed at 5, 10, and 20 bps costs.

The 0.25% opening condition qualified on five of 59 days; the 0.50%
condition qualified on three. Neither admitted a new 09:21 or 09:26 trade.
The original opening sweep's synthetic pass values were 0.05% OI and 0.50
candle body. Those do **not** reach the stricter native 09:21 or 09:26
thresholds (0.10% OI with 0.75 and 0.60 body respectively). Its opening
results therefore do not test effective relaxation of those early slots.
The later `--slot-aware` option corrects this for a separate experiment;
the historical table below has not been relabeled as a slot-aware result.

## Full-period results at 5 bps

| Policy | Trades | Wins | Losses | Win rate | Net P&L | Change from V4 | Losing days |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Frozen V4 | 338 | 286 | 52 | 84.62% | ₹811,978 | — | 5 |
| Best observed intraday gate: OI ≥0% and body ≥0.35 | 340 | 288 | 52 | 84.71% | ₹815,704 | +₹3,726 | 5 |
| Best changed 0.25% opening gate: body ≥0.35 | 339 | 286 | 53 | 84.37% | ₹805,737 | −₹6,241 | 6 |
| Best observed 0.50% opening gate: OI ≥0% and body ≥0.35 | 339 | 287 | 52 | 84.66% | ₹815,477 | +₹3,499 | 5 |
| Unconditional OI ≥0% in the opening slots | 340 | 285 | 55 | 83.82% | ₹791,168 | −₹20,810 | 5 |

The best intraday gate gained ₹3,499 from one 29 July TATASTEEL trade and
₹227 from one 23 September AUBANK trade. After the first eight sessions,
whose F&O stock cohort is after-dated, its incremental gain was only ₹227.
Its October result and losing-day count were identical to V4. The 0.50%
opening gate's entire gain was the same 29 July TATASTEEL trade. The 0.25%
opening gate and unconditional OI relaxation reduced net P&L. None of the
tested opening changes added a 09:21 or 09:26 selection.

At 10 and 20 bps, the best intraday gate still showed small positive
differences of ₹3,227 and ₹2,227, respectively. That is sensitivity to an
assumed cost, not independent validation. The V4 slot allowlist and the V5
parameter comparison both use these same historical sessions; there is no
untouched holdout. One or two newly selected trades do not support a claim
that V5 will improve future win rate, profit, or losing days.

## October comparison at 5 bps

| Policy | Trades | Wins | Losses | Net P&L | Losing days |
| --- | ---: | ---: | ---: | ---: | ---: |
| V4 | 87 | 70 | 17 | ₹162,865 | 2 |
| Best intraday V5 | 87 | 70 | 17 | ₹162,865 | 2 |
| Best changed 0.25% opening V5 | 88 | 70 | 18 | ₹156,624 | 3 |
| Best 0.50% opening V5 | 87 | 70 | 17 | ₹162,865 | 2 |

## Reproduction and detailed files

Run `python fno_v13_v10_g_7_v5_backtest.py --output <new-output-directory>`
from the repository root. The runner verifies the V4 lock hash, rebuilds
selection parity, computes the completed-bar market ledger, runs the three
parameter sweeps, and performs full portfolio comparisons. The V4 lock file
is untouched. The V5 research configuration sets `selected_policy` to
`V4_UNCHANGED` and `execution_authority` to `false`.

Detailed results are in `outputs/g7_v5_20261102/`:

- `all_experiments_comparison.csv`: every variant at 5, 10, and 20 bps,
  including full-period and October P&L, win rate, loss count, and losing days.
- `daywise_key_variants.csv`: 59 daily V4 and representative V5 outcomes.
- `report_summary.json`: cutoff, validity, and best observed result.
- `parity_full_59/parity.json` and `parity_opening_4/parity.json`: source
  selection checks.
- `portfolio_sweep_59/changed_executions.csv` and the corresponding opening
  experiment folders: exact trades added or removed.
