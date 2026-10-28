# G-3 V13 PAPER/LIVE configuration alignment

G-3 production V13 uses the same 51 enabled entry setups as G-3 backtest V14. The
unchanged V12 donor remains available for historical 2026-10-23 orders and replay.
The target cutoff disables these ten V12 setups: `1236_LONG`, `1246_LONG`,
`1246_SHORT`, `1251_LONG`, `1251_SHORT`, `1256_SHORT`, `1306_LONG`, `1316_LONG`,
`1316_SHORT`, `1326_LONG`. It explicitly retains `0951_LONG` (0.75%),
`1006_LONG` (0.56%), and `1016_SHORT` (0.68%). Retained setup filters, stop
percentages, targets, ranking, and timing are identical to the V14 backtest
contract except for the production identity.

The G-3 PAPER worker and LIVE coordinator both import
`fno_v13_v10_g_3_live_config.py`, now bound to
`configs/strategies/v13_v10_g_3_production_v13.json`. The production version is
`V13-V10-G-3-PRODUCTION-V13` with fingerprint
`74ff6bf6a2c1a68a08a324d8090809dbfe10a3ccacf289ab1953fb03636ca615`.
LIVE sizing remains ₹10,000 capital per trade at 5× exposure. Neither this
contract nor the active profile grants order authority: the profile is in
`PAPER_VALIDATION` with `live_activation_authorized=false`. The V13 standing
policy artifact is disabled. A fresh V13-bound approval and ordinary broker,
calendar, kill-switch, and arm checks are required before LIVE can resume.

Rollback, if needed before a new session: stop/reconcile any V13 workers and
orders first; point the live config and active profile back to the preserved V12
contract/config and its exact fingerprint, and keep LIVE disarmed until a
separate dated approval. Do not relabel existing V12 or V13 order records.
The G-3 V14 backtest and V12 production donor files are not modified by this
deployment.

This is rule/configuration parity, not a claim of fill or P&L parity. PAPER
uses observed quotes, and LIVE is subject to broker fills, costs, slippage,
data-readiness checks, and execution controls absent from the historical replay.
