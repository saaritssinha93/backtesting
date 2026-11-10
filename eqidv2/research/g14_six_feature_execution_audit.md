# G14 six-feature execution audit

The G14 six-feature selector must start from the full dated prefilter pool. The
older `research/g14_morning_backtest.py` begins with the already selected G7
opportunities, so it cannot measure a stock that a new six-feature rule admits.
It remains a reference for portfolio parity, not a candidate source.

## Reusable execution API

`research/g14_six_feature_execution.py` exposes:

```python
from research.g14_six_feature_execution import simulate_day, simulate_allocated

book_5bps = simulate_day(selected, minutes, base_config, 5.0, "g14_six_feature")
books_by_cost = simulate_allocated(
    selected, minutes, (5.0, 9.0, 15.0), base_config=base_config,
    policy="g14_six_feature",
)
```

Call this once per session. `selected` is the complete **newly selected,
pre-portfolio** inventory after the six-feature entry rule, reranking and
per-setup quota. The module does not run any entry alpha filter. An optional
`allowed` SID-indexed boolean Series on `simulate_allocated` can represent a
separately declared pre-entry market veto. Omit it for a six-field-only entry
study. Do not pass only previously executed rows or only winning candidates.

Required order columns: `sid`, `day`, `tradingsymbol`, `setup_id`, `side`,
`signal_ts`, `confirmation_ts`, `confirmation_high`, `confirmation_low`,
`trigger`, `native_stop_pct`, `native_target_pct`. `sid` must be unique; the
signal clock must be one of 09:20, 09:25, 09:30, 09:35, 09:40, 09:45 IST;
confirmation must be exactly the next completed minute. Set `trigger` to the
confirmation high for LONG or confirmation low for SHORT. Copy each setup's
published target from the G7 parent configuration; stop is 1.2%. The order
frame can retain all raw feature and source provenance fields. `minutes`
requires dated `ts` (or `date`), `tradingsymbol` and one-minute OHLC from
09:16 through 15:15 for each selected stock. Only complete, unflagged,
contiguous minute paths pass validation. Include a zero-order session in the
calendar even when the returned ledger is empty.

The execution pipeline calls, in order:

1. `g3_v18_20261102.data._normalize_minutes` for selected symbols, then
   `execution_paths` and `fno_v13_v9_backtest.validate_paths`. Paths start
   strictly after the confirmation close and end at exactly 15:15.
2. `fno_v13_corrected_v5_backtest.simulate_native` through
   `fno_v13_v10_g_backtest.v9.v5` for a fresh 10-minute stop-trigger fill.
   Gaps through a trigger fill at the worse observed open.
3. `research.g7_v4_selected_backtest.replay` for the G7 V4 exit: 1.2% initial
   stop, 1.0% stop after 120 minutes, published setup target, and a gross
   +0.1% protected stop starting at the **next minute open** after a completed
   full post-entry candle closes at +0.6%. Intrabar stop/target ties resolve
   stop first; adverse stop gaps use the observed open.
4. `research.g7_v4_practical_research.execute` with `integer=True`,
   `clock=True`, `limit=2`. It reapplies fixed capital and 5x exposure,
   per-symbol/capacity limits, and blocks new admissions strictly after the
   second realized net losing exit. It reruns this allocation independently
   at each cost setting. Earlier open positions are managed normally.

The parent allocation configuration is the hash-verified G7 V4 configuration
from `research.slot_win70_backtest.source_inputs()`. The module checks the
inherited initial/timed stops. Returned rows are marked `BACKTEST_ONLY` and
`execution_authority=False`.

## Parity checks performed

The archived G7 V4 Lock2 source was
`outputs/full_backtest_20261029/shared_replay/g7_v4_lock2_portfolio_trades.csv`.
The corresponding frozen stock minutes came from
`g3_indicator_lab_20261103/inputs/<day>/minutes.parquet`.

- 2026-07-31, archived 09:21 LONG BAJAJFINSV: exact 1,971.300049 entry,
  2,010.726050 exit, and 2.00% gross return reproduced.
- 2026-08-03, all five archived morning orders: exact entry price, exit price,
  and gross return at absolute tolerance `1e-8` for all five. With each
  source row's inherited `base_entry_allowed` permission supplied, the
  morning-only executed flags and per-order net rupees also matched the
  archived book (rupee tolerance `1e-5`). Without that
  permission mask, inventory changes can alter which overlapping position is
  allocated; that is expected and is why permissions must be explicit.
- 2026-09-11, archived unfilled 09:41 SHORT DELHIVERY: remained unfilled,
  unexecuted and zero-P&L. This checks the all-unfilled-day schema path.

These are execution parity checks, not evidence that a new six-feature
selection rule achieves the parent's return. A newly admitted candidate needs
its own minute replay and can consume capacity or cause the two-loss lock to
activate earlier.

## Analysis constraints

The G3 indicator lab's frozen full prefilter pool covers 63 sessions through
2026-11-02. Some 09:40/09:45 rows for the four most recent sessions are in
its separate `expanded_slots` artifact; the selection runner must merge those
with unique `(day, symbol, signal clock)` identity and source-hash proof. The
G3 source preserves historical universe and stock-day exclusions, some of
which were determined after the session; therefore a result using it is a
retrospective backtest, not fully point-in-time eligibility validation.

The six entry fields should be measured at or before the signal/confirmation
clock. One-minute confirmation OHLC is needed to define the order trigger and
execute it, but it must not be covertly used as a seventh ranking/filter
indicator. Likewise, retaining the old Nifty direction or low-efficiency veto
would make the rule more than six fields; if tested, label it a separate
variant. Historical parameter search and a 70% threshold on the same 63
sessions are in-sample evidence. Reserve later complete sessions for a true
forward check and disclose counts, costs and loss days alongside win rate.
