# DOC5C_ORB_GAP_GO_LONG (LONG) — BASELINE_RESULT

_Generated 2026-07-01._

- No baseline config found in final_setup_conf.py (none (no conf entry)); this setup has no tuned config of record. Baseline = raw detection only.

- TEST = calendar sessions >= 2026-06-20 (4 sessions).
- TRAIN 2026-05-18..2026-06-18 (15) | TEST 2026-06-22..2026-06-30 (4)

---

## RAW baseline metrics (computed by `scripts/gap_knob_sweep.py`; @15 bps/leg, statutory NSE cost)

The setup card describes DOC5C as: **controlled gap-up (0.5–4.0%) that holds and breaks the
opening range, in the 09:45–11:00 window, `close>open`, `close_loc≥0.60`, `rs_pct>0.40`,
`vol_ratio≥1.5`**. Detection knobs present in the pool: `gap_pct`, `orh_dist_atr`,
`vwap_slope_atr`, `vol_ratio`, `close_loc`, `rs_pct`, `body_pct`, `atr_pct`, `quality_score`,
`vwap_dist_atr`, `regime`, `market_ret_pct`, `signal_minute`. No pre-tuned config of record.

Raw detection (no mask / no pre-mom / no guard), doc-suggested exit **SL 0.85 / Tgt 1.50**:

| window | n | net PF | net Rs | win% | tgt-fill% | SL/TGT/EOD | avg win / avg loss |
|---|---:|---:|---:|---:|---:|---:|---|
| FIT   | 47 | 0.103 | −36,134 | 12.8 | 4.3  | 35/2/10 | Rs694 / −Rs983 |
| VAL   | 54 | 0.305 | −26,526 | 25.9 | 13.0 | 34/7/13 | Rs833 / −Rs955 |
| **TRAIN** | 101 | **0.202** | **−62,660** | 19.8 | 8.9 | 69/9/23 | Rs799 / −Rs? |
| **TEST**  | 23 | **0.140** | **−16,558** | 17.4 | 8.7 | 17/2/4 | — |

Best exit over the full 7×7 grid (SL {0.5..1.5} × Tgt {0.6..2.5}) reaches only **TRAIN PF ≈ 0.25**
(SL 1.20 / Tgt 2.00). Every one of the 49 exit brackets is a deep loser.

## Initial diagnosis

- Raw DOC5C is **net-negative in every window** (TRAIN PF 0.20, TEST PF 0.14), ~20% win, ~9%
  target-fill, SL-and-EOD dominated. This matches the source doc's own warning that gap-and-go
  is *"hit hardest by 5-min-only"*: the earliest fill is the next-bar open, so you enter ~5 min
  into the gap breakout and it mean-reverts.
- The binding constraints are **(a) no edge** (PF « 1.0 raw) and **(b) thin OOS** (23 raw TEST
  candidates over 4 sessions → ≤6 after any real gate). Target: TRAIN PF ∈ [1.30,1.70], TEST PF > 1.40.
- See `PARAMETER_SWEEP_SUMMARY.md` for the full knob-by-knob search (incl. the gap-and-go levers
  the canonical engine's feature list omits) and `ITERATION_LOG.md` for the staged iterations.