# Go-Live Checklist - final_setup_conf (16-setup book) on v7 live

Status legend: DONE / PARTIAL / OPEN / OUT-OF-SCOPE

Master switch: `EQIDV2_USE_FINAL_SETUP_CONF=1` (default OFF -> live unchanged).
Preflight: `EQIDV2_USE_FINAL_SETUP_CONF=1 py -3.12 conf_live_preflight.py`

## A. Entry-Signal Parity
- DONE: Conf is the single source of truth and is wired into scanner + entry engine behind the flag.
- DONE: All 16 setups detect live: native candidate_scan setups plus 4 Tier-C live detectors.
- DONE: Component parity: conf mask, pre-momentum gates, and exit levels are wired from the conf.
- DONE: Exit mechanism is untouched; only SL/target levels are sourced from the conf.
- DONE: Entry engine bypasses the old `_apply_v11_entry_overlay` in conf mode.
- DONE: Scanner match-v11 path implemented: 6 native setups pass v8/research normally; only the 10 v11 readmit-provenance setups bypass v8+research before the final conf mask.
- DONE: Tier-C scanner-source CSVs rebuilt on 2026-06-15 with current corrected-VWAP code/data. `validate_conf_tier_c_parity.py --sample-per-setup 60` passes 240/240 sampled current-source scans: stale_source=0, live_miss=0, causality=0.
- OPEN: First same-day paper-vs-v11 entry diff is still the forward end-to-end confirmation.

## B. P0-19 Paper Mirrors Live
- DONE: Conf-mode paper defaults are intended to mirror live values: 20 positions, Rs10k daily brake, env-overridable.
- OPEN: Config attestation snapshot at window start.

## C. P0-18 MTM-Aware Brake
- DONE: `eqidv2_risk_brake.py` has realized+open-MTM brake logic, throttle, per-setup caps, and flag-gated flatten behavior.
- PARTIAL: Paper executor observe wiring exists; watch one paper day before enabling act mode.
- OPEN: Live executor MTM brake wiring should mirror paper only after paper observe logs are trusted.

## D. P0-17 Gatekeeper Decision
- PARTIAL: In conf mode, the final conf mask is the gate of record after the v11-equivalent path. Native setups still pass v8/research; readmit-provenance setups bypass v8+research like v11. Decide whether the hand-curated corrected-VWAP train/test validation is acceptable for the qualification window, or run `gate_promotion.py` to author/attest the conf set.

## E. P0-16 Reporting Truth
- OPEN: Single NET aggregation and reporting reconciliation. This does not block paper qualification.

## F. Forward Confirmation
- OPEN: Enable the flag on the paper scanner + entry engine for one trading day, then diff the paper entries against the v11 same-day backtest using `--selected_strategy_profile final_setup_conf`.

## Verdict
Component parity and Tier-C source parity are proven. Paper-trading the conf path is ready for forward confirmation. Real capital remains NO-GO until the MTM brake is watched in paper and the first same-day paper-vs-v11 entry diff is clean.
