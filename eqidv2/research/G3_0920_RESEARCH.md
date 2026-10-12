# G-3 09:20 LONG/SHORT backtest research

The isolated `g3_0920_strategy.py` variant copies the current 09:25 LONG and
SHORT setup filters, quotas, rank order, stops, and targets at a 09:20 signal.
It requires a real 09:21 confirmation minute, so the first possible entry bar
is 09:22. No active G-3 setup, live order schedule, paper order schedule, or
published backtest pointer changes.

The opening price signal is the completed 09:15–09:20 cash bar close divided
by its 09:15 open. It is not the previous-close gap. Opening OI change uses
the same dated near-month futures contract's real 09:16 minute OI and completed
09:20 bar OI. This spans **four minutes**, not five. A missing, zero, flagged,
or mismatched OI observation stays missing; it is never replaced by a proxy.
EMA9/20/50, volume ratios, traded value, signal body and wicks, NIFTY first
bar, and the 09:21 confirmation are taken from genuine saved candles.

The batch exporter `g3_0920_history.py` includes one row per mapped stock and
day, even when opening OI is missing. It records source timestamps and
`missing_reasons`. A `COMPLETE` row means research features are present, not
that a setup passes. `signal_inputs_received_by_0921` and
`complete_decision_evidence_by_0922` test whether the saved acquisition times
support a live-as-of decision. Historical recovery after market hours remains
retrospective evidence only.

Persisted dated universes are preferred. For baseline dates without a usable
dated mapping, `--allow-sealed-fallback` explicitly enables a research-only
mapping from the hash-verified September 25 historical bundle. The exporter
checks its per-date stock/futures symbols and tokens against a hash-checked
monthly universe and labels it `SEALED_MONTHLY_DATASET`. This retrospective
monthly mapping cannot establish the actual live universe available that day.
July 29–August 10 and September 21–23 require the fallback. August 24–25
lack eligible saved near-month mappings and are outside the published G-3
baseline. September 21–23 have no valid archived 09:16 OI, so their 09:20
OI-dependent setups cannot be assessed.

Generate a full sealed feature export in a **new** output directory:

```powershell
python -m research.g3_0920_history --start 2026-07-29 --through 2026-10-12 --data-root C:\TradingData\eqidv2 --output-root outputs\g3_0920_research_full --allow-sealed-fallback --skip-closed --skip-missing-universe
```

Then run the paired backtest, substituting the actual immutable export path:

```powershell
python -m research.g3_0920_backtest --features outputs\g3_0920_research_full\2026-07-29_through_2026-10-12\features.parquet --output-dir C:\TradingData\eqidv2\backtesting_result_v13_v10_g_3\research_0920\runs\my_new_run
```

The runner validates exporter artifact hashes, every stock/contract mapping,
all 49 dates in the published baseline, selected opening source values, and
each new trade's complete real 09:22–15:15 minute path. Existing selected
trade outcomes come from the hash-verified published G-3 result; only new
09:20 orders are simulated. Both sets are then reallocated under the same
portfolio capital and cost rules, including any displacement of later trades.
This is a retrospective additive what-if, not a live performance claim.
