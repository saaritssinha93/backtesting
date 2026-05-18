# V17G Research Notes

Findings dossier — populated as each A/B run completes. Each section is filled in as evidence arrives, not pre-written.

Cross-references:
- design rationale: `v17f_new_design_proposal.md`
- engineering checklist: `v17g_implementation_checklist.md`
- harness: `eqidv2/v17g_ab_runner.py`
- raw outputs: `outputs_v17g_ab/<run_name>/`
- comparison summary: `v17g_ab_comparison.md` (auto-generated)

## Backtest Window

| Field | Value |
|---|---|
| Bar interval | 5-min |
| IS window | TBD (fill at first run) |
| OOS window | TBD (fill at first run, last 4 months held out) |
| Universe | TBD (record from runner output) |
| Baseline reference | v17f canonical |

## Run Status

| Run | Status | Date | Notes |
|---|---|---|---|
| baseline | pending | — | v17f as-is reference |
| v17g-1 | pending | — | LONG pullback bug fix |
| v17g-2 | pending | — | LONG reversal setup |
| v17g-3 | pending | — | Setup-level sizing |
| v17g-4 | pending | — | Consolidate SHORT pockets |
| v17g-5 | pending | — | LONG AVWAP distance cap |
| v17g-all | pending | — | All five changes |

## Per-Hypothesis Findings

### H1 — LONG pullback bug fix unlocks orthogonal alpha

Run: `v17g-1`

| Metric | baseline | v17g-1 | Delta | Verdict |
|---|---|---|---|---|
| trade_count | | | | |
| pullback_setup_count | 0 | | | |
| pullback_setup_pf | n/a | | | |
| day_win_pct | | | | |
| max_drawdown_pct | | | | |
| Sharpe | | | | |

Observations: TBD

Decision: TBD (advance / drop / re-tune)

### H2 — LONG reversal adds participation on gap-down-reclaim days

Run: `v17g-2`

| Metric | baseline | v17g-2 | Delta | Verdict |
|---|---|---|---|---|
| trade_count | | | | |
| reversal_setup_count | 0 | | | |
| reversal_setup_pf | n/a | | | |
| reversal_setup_dayhit_pct | n/a | | | |
| correlation_with_existing_long_setups | n/a | | | |

Observations: TBD

Decision: TBD

### H3 — Setup-level sizing reduces MaxDD without hurting PF

Run: `v17g-3`

| Metric | baseline | v17g-3 | Delta | Verdict |
|---|---|---|---|---|
| profit_factor | | | | |
| max_drawdown_pct | | | | |
| Sharpe | | | | |
| Calmar | | | | |
| max_consec_losing_days | | | | |

Per-tier P&L share before vs after:

| Tier | baseline P&L share | v17g-3 P&L share |
|---|---|---|
| Core | | |
| Core-subtype | | |
| Pullback | | |
| Event | | |
| Reversal | | |

Observations: TBD

Decision: TBD

### H4 — Consolidated SHORT rule matches fragmented stack

Run: `v17g-4`

| Metric | baseline | v17g-4 | Delta | Verdict |
|---|---|---|---|---|
| short_trade_count | | | | |
| short_pf | | | | |
| short_day_win_pct | | | | |
| trades_admitted_in_old_pockets | 0 | | | |
| trades_dropped_by_new_rule | 0 | | | |

Per-bucket distribution check (10:30-11:00, 11:30-12:00, 12:15-12:45):

| Bucket | baseline trades | v17g-4 trades | v17g-4 PF in bucket |
|---|---|---|---|
| 10:30-11:00 | 0 | | |
| 11:30-12:00 | 0 | | |
| 12:15-12:45 | 0 | | |

Observations: TBD

Decision: TBD

### H5 — LONG AVWAP distance cap clips worst tail outcomes

Run: `v17g-5`

| Metric | baseline | v17g-5 | Delta | Verdict |
|---|---|---|---|---|
| long_trade_count | | | | |
| long_pf | | | | |
| dropped_by_avwap_cap | 0 | | | |
| avg_pnl_of_dropped | n/a | | | |
| worst_decile_pnl_long | | | | |
| max_drawdown_pct | | | | |

Observations: TBD

Decision: TBD

## v17g-all Combined Result

Run: `v17g-all`

| Metric | baseline | v17g-all | IS delta | OOS delta | Pass? |
|---|---|---|---|---|---|
| profit_factor | | | | | |
| day_win_pct | | | | | |
| max_drawdown_pct | | | | | |
| Sharpe | | | | | |
| Calmar | | | | | |
| trade_count | | | | | |

OOS pass criterion: beats baseline on >=3 of {PF, MaxDD, Sharpe}.

Verdict: TBD

## Surprising Observations (Save To Memory When Confirmed)

Empty — populate as findings emerge that are non-obvious from code/git.

## Discrepancies vs Expected Behavioral Profile

Expected (from `v17f_new_design_proposal.md`):
- Trade count: +10-20%
- Day-win: ±1pp
- PF: ±0.05
- MaxDD: -2 to -5pp
- Sharpe: +5-10%
- Per-setup P&L: core 60-70%, pullback 10-15%, reversal 5-10%, event 5-10%

Observed deltas vs expected: TBD per run.

## Decisions Log

| Date | Decision | Reason |
|---|---|---|
| TBD | | |

## Open Questions

- Should `B_AVWAP_RECLAIM_REVERSAL` use `lag = 1` fixed or `lag = -1` dynamic? (Decide from H2 sub-A/B.)
- LONG signal window `12:00-13:00` audit: net positive or negative P&L contribution? (Decide before final v17g shipping.)
- Does the consolidated SHORT rule need an additional ATR% floor, or does the existing v17f ATR%>=0.70% rule cover it?
- Per-tier sizing — are the multipliers right? Especially 0.50× for new reversal setup. Validate after H2.

## Next Actions On Promotion

If `v17g-all` passes OOS:
1. Make v17g the new canonical runner.
2. Update `MEMORY.md` with `project_v17g_5min_results.md` summary entry.
3. Run live parity smokecheck on v17g (mirror of `v17f_live_parity_smokecheck_20260420.md`).
4. Update `v17h_parked_items.md` to reflect what should now move to active research.
