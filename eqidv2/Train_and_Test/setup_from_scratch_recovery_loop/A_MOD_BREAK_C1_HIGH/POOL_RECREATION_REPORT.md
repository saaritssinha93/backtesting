# A_MOD_BREAK_C1_HIGH — Pool Recreation Report (recovery loop)

_Generated 2026-07-03._

## Provenance

`pools/pool_base/` is the verified pool recreated from raw data on 2026-07-02/03 for the exact
requested windows (TRAIN 2026-03-01..05-30 / TEST 2026-06-01..07-02), copied byte-identical from
`Train_and_Test/setup_pf_1_4_full_loop/A_MOD_BREAK_C1_HIGH/pools/pool_enriched/`. Recreating it
again from the same immutable raw parquet would produce the same file; the copy preserves compute.
Full recreation detail (sources, commands, gap regeneration): see the campaign-1 report
`../../setup_pf_1_4_full_loop/A_MOD_BREAK_C1_HIGH/POOL_RECREATION_REPORT.md`.

## Numbers (verified on copy)

- Rows: **26,277**; sessions: **74** (2026-03-04..2026-07-01); symbols ≈ 1,118 with data.
- Columns: **137** = 94 native pool schema + 40 enriched features (recomputed leak-safe at signal
  bar from the 5-min parquet) + 3 plumbing.
- TRAIN 2026-03-04..05-29 = 52 sessions; TEST 2026-06-01..07-01 = 22 sessions.
  FIT = first 60% of TRAIN sessions (31), VAL = remaining 40% (21). 07-02 incomplete → excluded.
- Data coverage: 5-min `stocks_indicators_5min_eq_live2`; 1-min `stocks_indicators_1min_eq`
  (through 07-01 inclusive; morning-backfill transient corruption noted — reads retried).
- Missing sessions: 05-28, 06-26 (raw-store holes, unrecoverable); NSE holidays as documented.

## New in this loop: 1-minute path store

`paths/paths.parquet` — full 1-min OHLC path (signal → 15:20 IST) for every signal;
`paths/summary.csv` — per-signal entry px/ts, MFE/MAE geometry, confirmation timing;
`paths/validation.json` — bracket-walk match-rate vs the canonical `setup_train_test` resolver
(must be ≈100% before any redesign result is trusted).

Rebuild: `py -3.12 scripts/extract_1m_paths.py`
