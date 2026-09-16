# V13-V6 / V13-V7 Implementation Status

Status date: 2026-09-09 IST

## Version contract

- V13-V5 remains the immutable signal and exit control.
- V13-V6 changes execution/accounting only.
- V13-V7 changes only maximum holding time and remains a shadow.
- V13-V8 is a research-only causal feature dataset; it cannot promote itself or alter V13-V5 selections.
- No volume, OI, NIFTY, setup, stop, target, or ranking threshold has been optimized in these versions.

## Implemented

### V13-V6 stock portfolio engine

File: `fno_v13_v6_portfolio_backtest.py`

- Causal capital reservation and release.
- Maximum position, symbol-position, gross-exposure, and open-risk constraints.
- Deterministic simultaneous-order priority using confirmation/setup order and each setup's documented picker value.
- Explicit reject reasons and zero P&L for rejected source fills.
- Peak reserved capital, exposure, position count, open risk, PF, P&L, and drawdown.
- Immutable run directories; source/output hashes; manifest committed last.

### V13-V6 capacity research

File: `fno_v13_v6_research.py`

- Frozen V13-V5 control versus 1/2/3/5-slot scenarios.
- Eligible zero-trade sessions retained from the V13-V5 eligibility calendar.
- Per-month and all-period metrics.
- Scenario trade ledgers and a checksum manifest.

### V13-V6 options execution

File: `fno_v13_v6_options_backtest.py`

- Explicit option data root; no import-time default-path binding.
- Multiple base/daily data roots with deterministic `trade_id` deduplication.
- Exact entry-candle requirement.
- Entry volume-capacity reject or integer-lot resize.
- Whole-lot target fills can complete over multiple eligible candles.
- Protective-stop/breakeven remainders carry forward as market exits at later adverse opens.
- Unresolved quantities remain explicitly open; realized P&L is separated and final net P&L stays null.
- A fill-by-fill entry/exit ledger records bar volume, capacity and capacity already consumed.
- Same-bar entry and exit consume volume separately.
- Later-bar stops gap through at the adverse open.
- Tick rounding and optional adverse ticks on both sides.
- Entry and exit cost bps charged separately.
- Optional per-trade risk budget and premium-outlay budget.
- Run-specific outputs and complete source/output hashes.

### V13-V6 causal option selector

File: `fno_v13_v6_option_liquidity_selector.py`

- Audits the fetched ATM +/-2 ladder and writes a shadow coverage map.
- Uses only volume, OI and close observations strictly before the planned option entry.
- Applies absolute liquidity floors, then retains the nearest strike within 50% of the best causal volume.
- Preserves the original ATM contract as fallback when no candidate meets the pre-registered floors.
- Does not change production selection or the V13-V5 control.

### V13-V7 exit shadow

File: `fno_v13_v7_exit_shadow.py`

- Exact V13-V5 control replay with a fail-fast P&L/exit-reason parity check.
- One pre-registered candidate: 180-minute maximum hold.
- Paired trade-delta ledger.
- Optional V13-V6 capital constraints applied independently to both arms.
- Explicit `BLOCKED_PENDING_FORWARD_SAMPLE_GATES` promotion status.

### Dashboard run-vintage gate

File: `fno_v13_run_vintage.py`; read-only card wired into `log_dashboard_server.py`.

- Validates the newest run only, including completion flag, run identity, output location and SHA-256 hashes.
- Blocks mixed data-through dates across V13 artifact families.
- Never silently falls back from a bad newest run to an older green run.

### V13-V8 causal futures feature shadow

File: `fno_v13_v8_feature_shadow.py`

- Five-minute futures volume, OI-change and return features from completed pre-entry minutes.
- Same-contract, same-time-of-day baselines use strictly earlier sessions only.
- Futures/cash basis and side-aligned price checks.
- Outcomes are joined only after feature calculation for descriptive reporting.
- Explicit `RESEARCH_ONLY_NOT_PROMOTED` status.

## Verification

Focused suite:

```powershell
python -m pytest tests/test_fno_v13_v6_execution.py tests/test_fno_v13_corrected_v5_backtest.py tests/test_fno_v13_v5_options_backtest.py -q
```

Current result: 90 relevant tests passed: 48 strategy/derivative tests plus 42 dashboard tests. The V6/V7/V8-focused file has 22 passing tests.

Real-data smoke runs are under `.codex_tmp/v13_v6_smoke/`. They are diagnostics and are not production-promoted artifacts.

## Example commands

Capital-constrained V13-V6 replay:

```powershell
python fno_v13_v6_portfolio_backtest.py --portfolio-capital-rupees 300000 --max-positions 3
```

V13-V6 capacity comparison:

```powershell
python fno_v13_v6_research.py --slot-counts 1,2,3,5
```

Combined base plus daily options packages:

```powershell
python fno_v13_v6_options_backtest.py `
  --data-root C:\TradingData\eqidv2\fno_oi\strategy_research\v13_corrected_v5\derivative_market_data `
  --data-root C:\TradingData\eqidv2\fno_oi\strategy_research\v13_corrected_v5\daily_options\2026-09-08\derivative_market_data
```

V13-V7 180-minute exit shadow with a three-slot research portfolio:

```powershell
python fno_v13_v7_exit_shadow.py `
  --portfolio-capital-rupees 300000 `
  --max-positions 3
```

Causal ATM +/-2 option-selection shadow:

```powershell
python fno_v13_v6_option_liquidity_selector.py `
  --data-root C:\TradingData\eqidv2\fno_oi\strategy_research\v13_corrected_v5\derivative_market_data
```

Replay the audited selector coverage through the V13-V6 execution model:

```powershell
python fno_v13_v6_options_backtest.py `
  --coverage-csv <selector-run>\fno_v13_v6_liquidity_selected_coverage.csv
```

V13 dashboard vintage gate:

```powershell
python fno_v13_run_vintage.py --strict
```

V13-V8 futures-feature shadow:

```powershell
python fno_v13_v8_feature_shadow.py
```

## Current smoke observations (not selection decisions)

- V13-V6 native ATM options, September through 2026-09-08: 9 executed trades, 5 wins / 4 losses, net +Rs 4,411.62, PF 1.081 after two-sided cost. Partial-fill handling recovered the previously non-executable MPHASIS exit.
- Causal liquidity-selector shadow over the same dates: 10 executed trades, 5 wins / 5 losses, net -Rs 33,927.96, PF 0.568. The selector therefore remains rejected as a production change; its extra MANAPPURAM fill alone lost Rs 20,505.
- V13-V8 feature coverage is intentionally unavailable for the current September signals because the retained same-contract history generally has only one prior session versus the pre-registered minimum of three.

## Still pending

- Historical bid/ask or a calibrated spread model.
- Additional same-contract historical futures sessions for September; the current retained package often has only one prior session, so the V13-V8 time-of-day gate correctly remains unavailable for those signals.
- Walk-forward evaluation of the V13-V8 shadow checks without threshold fitting on the evaluation window.
- Required untouched forward sample before any V13-V7 promotion.
