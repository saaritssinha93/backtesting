# FNO V10 Stage 7 locked backtester

`fno_v10_backtest.py` is the canonical backtest-only front door for the selected
V10 Stage 7 configuration. It is permanently locked to experiment variant
`0940_LONG_MOVE_040`.

The sole strategy change from V10B is:

- `09:40_LONG.price_change_pct >= 0.40%` on the completed five-minute signal.

The other nine setup legs, 1-minute confirmation and entry rules, S+5 expiry,
brackets, portfolio controls and cost accounting remain unchanged. The launcher
rejects any different `--variant` before delegation. It has no live or paper
authority and does not alter the frozen `fno_v10_unified_5m_1m_backtest.py`
parity baseline.

## Extended stored-snapshot replay

The selected frozen snapshot spans 59 expected exchange sessions from
2026-05-27 through 2026-08-19. This extends the earlier 40-session Stage 7
evaluation; it is not a reproduction of that result. Futures/OI files do not
begin before May 27, and individual symbols have later starts or earlier ends,
so the per-symbol panel remains incomplete throughout the extended window.

```powershell
python fno_v10_backtest.py run `
  --source-snapshot C:\TradingData\eqidv2\fno_oi\strategy_research\v8_windowed_strict_v1\snapshots\snapshot_20260820T124734626995+0530_mnofor_c\manifest.json `
  --from-day 2026-05-27 `
  --through-day 2026-08-19 `
  --split-day 2026-08-06 `
  --cost-bps 15 `
  --slippage-bps 0 `
  --square-off 15:30 `
  --eod-policy LAST_REAL_BAR_SENSITIVITY `
  --rebuild-cache
```

The last-real-bar policy makes numerical performance a sensitivity diagnostic.
Missing symbol-sessions and the static August-futures universe still prevent a
promotion-grade headline.

Inspect the exact locked profile or validate a Stage 7 run with:

```powershell
python fno_v10_backtest.py profile
python fno_v10_backtest.py validate --provenance <run-dir>\provenance.json
```

## V10 `.50 + Gap2` full usable-history replay

The same front door also contains the composed research profile:

- locked Stage 7 `09:40_LONG.price_change_pct >= 0.40%`;
- `09:35_LONG.price_change_pct <= 0.50%`, followed by within-slot reranking;
- maximum 2 bps adverse one-minute entry-gap guard;
- complete causal confirmation, entry, exit, portfolio, and cost state machine.

Run all validated, non-overlapping historical segments and all three declared
cost scenarios with:

```powershell
python -u fno_v10_backtest.py max050-gap2 --all-usable-history
```

The pinned usable-history contract currently contains 65 sessions from
2026-05-27 through 2026-08-28 across August and September futures segments.
There is no validated cache for the regular 2026-08-26 session; the launcher
records that calendar gap and never substitutes a flat day. Outputs remain
research-only because the source panel is incomplete and uses the
last-real-bar sensitivity policy.
