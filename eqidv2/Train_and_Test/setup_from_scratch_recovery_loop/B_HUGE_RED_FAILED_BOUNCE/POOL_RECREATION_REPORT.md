# B_HUGE_RED_FAILED_BOUNCE (SHORT) — POOL_RECREATION_REPORT (recovery loop)

_Generated 2026-07-03. From-scratch recovery loop. Research-only; NO live trades; NO final_setup_conf.py edits._

- Pool carried over from the verified 2026-07-02/03 recreation (same mandate windows), then ENRICHED with ~38 point-in-time indicator/price-action features. Lineage: `Train_and_Test/setup_pf_1_4_full_loop/B_HUGE_RED_FAILED_BOUNCE/pools/` -> `C:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2\Train_and_Test\setup_from_scratch_recovery_loop\B_HUGE_RED_FAILED_BOUNCE\pools`.
- Basis: RAW candidates (production clean-pool scanner, ab-gate enabled) — same 4-segment basis as the A_MOD full-loop campaigns
- requested TRAIN ['2026-03-01', '2026-05-30'] -> actual ['2026-03-02', '2026-05-29'] (53 sessions)
- requested TEST ['2026-06-01', '2026-07-02'] -> actual ['2026-06-01', '2026-06-30'] (20 sessions)
- excluded sessions: ['2026-07-02'] (1-min EOD sync incomplete at build time (rows stop 09:30) -> exits unresolvable)
- rows 1998, symbols 820
- 5-min: stocks_indicators_5min_eq_live2 (signals + enrichment); 1-min: stocks_indicators_1min_eq via v11 loader (entries + exits to 15:20 IST).
- windows in this run: {"FIT_s": "2026-03-02..2026-04-27 (32)", "VAL_s": "2026-04-28..2026-05-29 (21)", "TRAIN_s": "2026-03-02..2026-05-29 (53)", "TEST_s": "2026-06-01..2026-06-30 (20)"}