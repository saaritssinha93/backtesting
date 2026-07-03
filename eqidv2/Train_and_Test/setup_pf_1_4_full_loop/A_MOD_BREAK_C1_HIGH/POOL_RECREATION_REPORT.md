# A_MOD_BREAK_C1_HIGH — Pool Recreation Report

_Generated 2026-07-02. Research-only._

## Raw Data Sources

| source | role | span used |
|---|---|---|
| `C:\TradingData\eqidv2\outputs_ID_v11_unified_pool\` (master unified pool, `_basis=raw` for this setup) | base extract | 2026-03-04..2026-06-24 |
| fresh `avwap_5min_ID_v11_backtesting.py --mode historical_all_available` (raw candidates, pre-gate) over `stocks_indicators_5min_eq_live2` | tail + gap-fill | 2026-06-17..2026-07-01 |
| 1-min store `stocks_indicators_1min_eq` | exit resolution inside `setup_train_test` | as needed |

Both sources produce the identical raw (pre-gate) candidate basis — same detector (`avwap_5min_ID_v2_backtesting._scan_day`), same probe pipeline, same 94-column schema. Costs/slippage: repo cost model at 15 bps (`setup_train_test.SLIPPAGE_BPS`), the same the approval-loop engine uses.

## Recreated Pool

- Path: `Train_and_Test/setup_pf_1_4_full_loop/A_MOD_BREAK_C1_HIGH/pools/pool_full/historical_all_available_pre_dedupe_live_candidates.csv`
- Rows: **26,277** (44,170 master rows + 11,756 fresh-gen rows → date-filtered, deduped on ticker|side|setup|signal_time_ist)
- Sessions: **74** — span **2026-03-04 .. 2026-07-01**
- Symbols: full ~1,280-ticker probe universe
- Manifest: `pools/pool_full/_manifest.json` (exact session list)
- Intermediate artifacts: `pools/pool_master_only/` (extract before fresh gen), `pools/_tail_raw_gen/`, `pools/_gapfill_raw_gen/`

## Requested vs Actual Windows

| window | requested | actual sessions used | count |
|---|---|---|---|
| TRAIN | 2026-03-01..2026-05-30 | 2026-03-04..2026-05-29 | **52** |
| TEST | 2026-06-01..2026-07-02 | 2026-06-01..2026-07-01 | **22** |

- FIT = first 60% of TRAIN sessions (31), VAL = remaining 40% (21).
- 2026-07-02 (today) is an incomplete session → excluded (completed sessions only).

## Missing Dates — classified

**Regenerated (were upstream pool gaps, raw data existed):** 2026-06-17, 06-18, 06-19, 06-22*, 06-23 (*06-22 partially present before; deduped).

**Unrecoverable (no bars in the raw 5-min store across all 1,295 tickers; regeneration attempted and returned "no available historical dates"):**
- 2026-05-28 — holiday or feed outage; indistinguishable offline.
- 2026-06-26 — note: a `data_verify_2026-06-26.json` artifact exists from that day, so data likely existed and was later lost in store maintenance.

**Likely NSE holidays (zero rows across ALL setups in master; not regenerated):** 2026-03-03, 03-26, 03-31, 04-03 (Good Friday), 04-14, 05-01.

**Genuine detector silence (other setups have rows, this setup zero — kept as-is, correctly):** 2026-03-02, 03-11, 03-27, 04-24, 05-12 (crash day — regime BEAR gates out this LONG), 05-21.

## Data Quality Issues

1. The unified master pool ends 2026-06-24 and silently lacked 5 mid-June sessions — worth knowing for every other setup tuned off this master.
2. `tt._premom` returned **empty feature dicts** on a 1,000-row FIT sample → pre-momentum terms are effectively unavailable for this pool (likely missing 1-min lookback data for these rows). The optimizer therefore searches masks/exits/guards only.
3. 2026-07-01 rows resolve correctly (1-min EOD backfill landed 07-02 09:44).

## Rerun Commands

```powershell
# tail / gap-fill generation (raw candidates)
py -3.12 avwap_5min_ID_v11_backtesting.py --mode historical_all_available --start_date 2026-06-25 --end_date 2026-07-01 --workers 4 --out Train_and_Test\setup_pf_1_4_full_loop\A_MOD_BREAK_C1_HIGH\pools\_tail_raw_gen --selected_strategy_profile none
py -3.12 avwap_5min_ID_v11_backtesting.py --mode historical_all_available --start_date 2026-06-17 --end_date 2026-06-23 --workers 4 --out Train_and_Test\setup_pf_1_4_full_loop\A_MOD_BREAK_C1_HIGH\pools\_gapfill_raw_gen\d0617_0623 --selected_strategy_profile none

# combine + filter + dedupe
py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_MOD_BREAK_C1_HIGH\scripts\build_pool_a_mod_c1_high.py --tail_dir Train_and_Test\setup_pf_1_4_full_loop\A_MOD_BREAK_C1_HIGH\pools --out Train_and_Test\setup_pf_1_4_full_loop\A_MOD_BREAK_C1_HIGH\pools\pool_full
```
