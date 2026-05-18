# Phase 2 — SE Early-Emit Implementation Plan

Date: 2026-04-22
Binding inputs: `phase0_design_decisions.md`, `setup_early_emit_map.md`.

## Scope split

Phase 2 delivers an end-to-end *shadow* early-emit pipeline for the two most common setups, then extends to the rest. This avoids a 500-line write-and-pray commit and bounds drift risk.

- **Phase 2a (this commit)**: infra + A_MOD_BREAK_C1_HIGH (LONG, lag=1) + A_MOD_BREAK_C1_LOW (SHORT, lag=1). These are the overwhelming majority of live signals historically.
- **Phase 2b (next commit)**: A_MOD_CLOSE_CONTINUATION_BREAK (LONG), B_HUGE_C1_CLOSE_RECLAIM_BREAK (LONG) — both C1-anchored, mechanical extension.
- **Phase 2c**: A_PULLBACK_C2_* (both sides) — trigger_iso = C2, slightly more complex.
- **Phase 2d**: B_HUGE_FAILED_BOUNCE (SHORT) — dynamic window, 40-min entry tail.

Dead setup B_HUGE_PULLBACK_HOLD_BREAK is not implemented.

## Module design

New file: `eqidv2/eqidv2_early_emit_v16_5min.py`

Responsibilities:
1. `evaluate_slot_candidates(ticker, df_prepared, slot_ts, short_cfg, long_cfg) -> List[CandidateRow]` — runs the precondition ladder for each setup's anchor bar at slot_ts, returns candidate dicts.
2. `write_candidate_pool(candidates, date_str, runtime_root)` — merges into `candidate_signals_<date>_v16_5min.json` with idempotent dedup on signal_id.
3. Pure helpers: `_compute_signal_id(ticker, side, trigger_iso, setup)`, `_candidate_row(...)`.

No class machinery. No dataframe mutations. Deterministic ordering.

## Precondition evaluator architecture

One evaluator per setup; they share a common "C1 gate ladder" helper. For A_MOD setups the C1 gate ladder is:

```python
def _c1_short_gate_ok(row_c1, prior_rows, cfg) -> bool:
    # Lines 403-478 of short scanner's scan_one_day, minus the break-level check.
    if not in_signal_window(row_c1.date, cfg): return False
    if classify_red_impulse(row_c1, cfg) != "MODERATE": return False
    if not market_regime_pass(row_c1.date, "SHORT", cfg): return False
    atr1 = float(row_c1.ATR15); close1 = float(row_c1.close)
    if not (atr1 > 0 and close1 > 0): return False
    if cfg.use_atr_pct_filter and (atr1/close1) < cfg.atr_pct_min: return False
    if not volume_filter_pass(row_c1, cfg): return False
    # day_mode branch
    day_mode = _live_day_mode(prior_rows, cfg)  # prev_close=None in live; matches scanner
    if day_mode == "trend":
        if not _trend_filter_short(df, i, row_c1, cfg): return False
    else:
        if not _reversal_filter_short(df, i, row_c1, cfg): return False
    # sweep check
    # avwap_no_trade_zone_block
    # signal_avwap_dist_atr_max gate
    return True
```

Notes:
- `_trend_filter_*`/`_reversal_filter_*` take the full df and index — the evaluator passes df_prepared and the computed index i, same as the scanner.
- All helpers are imported from the exact same modules the scanner imports from, via the exact same aliases v16_runner already uses. No reimplementation.

## Hook into SE

Minimal addition to `eqidv2/eqidv2_signal_engine_v16_5min.py`:

```python
# New import at top:
from eqidv2_early_emit_v16_5min import evaluate_slot_candidates, write_candidate_pool

# In SE main loop after _write_pending_pool(short, long, slot, rs_pct, date_str):
try:
    candidates = []
    for ticker, df_prepared in prepared_frames.items():
        candidates.extend(
            evaluate_slot_candidates(ticker, df_prepared, slot_ist, short_cfg, long_cfg)
        )
    write_candidate_pool(candidates, date_str, runtime_root)
except Exception as exc:
    log.warning("early_emit shadow failed: %s", exc)  # never kill SE
```

Shadow path MUST NOT raise. `try/except Exception` wraps the whole early-emit block.

## Pool file schema

`C:/TradingData/eqidv2/live_signals/candidate_signals_<date>_v16_5min.json`:

```json
{
  "generated_at_ist": "2026-04-22T13:35:02+05:30",
  "date": "2026-04-22",
  "version": "v16_5min_early_emit_v1",
  "candidates": [
    {
      "signal_id": "b4c8...",
      "ticker": "RELIANCE",
      "side": "LONG",
      "setup": "A_MOD_BREAK_C1_HIGH",
      "trigger_iso": "2026-04-22 09:30:00+05:30",
      "break_level": 1243.55,
      "entry_window_start_iso": "2026-04-22 09:35:00+05:30",
      "entry_window_end_iso":   "2026-04-22 09:40:00+05:30",
      "state": "CANDIDATE_WRITTEN",
      "emitted_at_iso": "2026-04-22 09:31:02+05:30",
      "c1_close": 1240.10,
      "c1_atr": 3.45,
      "preconds_snapshot": {
        "impulse": "MODERATE",
        "day_mode": "trend",
        "market_regime_pass": true,
        "volume_filter_pass": true,
        "trend_filter_pass": true,
        "sweep_ctx": true,
        "avwap_dist_atr": 1.12
      }
    }
  ]
}
```

Dedup: on re-write, existing signal_ids are retained with their original `emitted_at_iso` and `state`. New entries are appended. State transitions (CONFIRMED/EXPIRED/INVALIDATED) are Phase 3/4; in Phase 2 all rows stay at CANDIDATE_WRITTEN.

## Risks + mitigations

| Risk | Mitigation |
|---|---|
| Scanner helper behaviour drifts; evaluator outputs different signals than scanner would have | Use EXACT same import aliases v16_runner uses. Evaluator ladder mirrors `scan_one_day` line-for-line. Phase 3 parity harness compares candidate→confirm vs actual pending pool. |
| `prev_close=None` in live (Q2.4) causes different `day_mode` than backtest | This divergence is PRE-EXISTING in SE. Evaluator matches SE scanner behaviour, not backtest. Flagged in docs. |
| SE hook raises and kills live SE | Try/except around the whole shadow block. Log warning, swallow exception, do not propagate. |
| Candidate file grows unbounded | One file per date. Rotated naturally. No cross-day retention. |
| Write atomicity during concurrent SE scans | Use tmp-file + os.replace pattern (same as `_write_pending_pool`). |

## Out of scope in Phase 2

- DE confirmation logic (Phase 4).
- Executor changes (Phase 5).
- Scanner modifications (never).
- Any edit to `pending_signals_*.json` path.

## Exit criteria

Phase 2a is done when:
1. `eqidv2_early_emit_v16_5min.py` exists and imports cleanly (`python -c "import eqidv2_early_emit_v16_5min"`).
2. SE hook is in place and guarded by try/except.
3. Running SE (or a replay) on recent data produces a non-empty `candidate_signals_*.json` with valid schema.
4. For each A_MOD_BREAK_C1_HIGH / A_MOD_BREAK_C1_LOW row in `pending_signals_<date>_v16_5min.json`, there is a corresponding CANDIDATE row at `trigger_iso = source_slot - 5min` with the same ticker/side/setup. (Diff tool in Phase 3.)

Phase 2a does NOT need to achieve parity for A_PULLBACK or HUGE setups — those are deferred.
