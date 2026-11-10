# G14 V2 six-feature source audit (2026-11-03)

This is a research-only, frozen input for the 63 available trading sessions from 2026-07-29 through 2026-11-02. The strategy configuration is `configs/strategies/v13_v10_g_14_six_feature_v2.json`. The candidate builder is `research/g14_six_feature_prepare.py`; its output is `C:/TradingData/eqidv2/backtesting_result_v13_v10_g_14/six_feature_source/`.

## Causal feature definitions

The six numeric selection fields are computed from the completed signal bar and a prior-session official EQ close:

| Field | Exact calculation | Earliest availability |
|---|---|---|
| `open_gap_pct` | `100 × (09:15 cash opening price / prior official NSE EQ ClsPric − 1)` | Opening cash bar, before the first 09:20 decision |
| `pdc_pct` | `100 × (completed signal cash 5m close / prior official NSE EQ ClsPric − 1)` | At the signal clock |
| `price_5m_pct` | `100 × (signal cash close / preceding completed 5m cash close − 1)`; at 09:20 use 09:15 cash open as the base | At the signal clock |
| `oi` | Same-contract futures open interest in the completed signal bar | At the signal clock |
| `oi_delta_5m_pct` | `100 × (signal futures OI / preceding futures OI − 1)` | At the signal clock |
| `oi_day_pct` | `100 × (signal futures OI / first completed 09:20 OI of the **same contract** − 1)` | At the signal clock |

At **09:20**, the archived opening OI comparison is 09:16 to 09:20, a **four-minute proxy** labeled `OPEN_4M_0916_TO_0920`, although the requested common field remains `oi_delta_5m_pct`. Later slots use exact preceding five-minute OI. `oi_day_pct` is zero by construction at 09:20. An absent opening OI baseline or a contract change invalidates OI day; there is no imputation or cross-contract comparison.

The `side` is LONG for a positive signed `price_5m_pct` and SHORT for a negative one. A zero move has no directional side and is rejected. There is one candidate per stock and signal clock. The 12 slot/side `setup_id`, original stop and target percentages, and the next-minute confirmation high/low trigger are carried for replay; they are **not** among the six selection inputs. `integrity_ok` uses only signal-time features, timestamps, and source checks. `execution_ready` checks next-minute confirmation separately; it must not enter candidate ranking or admission.

## Official PDC evidence

- Fetched 71 verified official NSE Capital Market EQ bhavcopy ZIPs from `https://nsearchives.nseindia.com/content/cm/` across 74 requested weekdays. The unavailable archive dates were 2026-09-14, 2026-10-02, and 2026-10-20. Every represented signal date has an official archive and a verified preceding archived session.
- Each ZIP's URL, HTTP last-modified header, fetch UTC timestamp, ZIP SHA256, CSV-member SHA256, CSV schema/date validation, and EQ row count are frozen in `source_manifest.json`; original ZIP bytes are retained alongside it.
- `pdc.csv` has the exact key/source fields `day,tradingsymbol,pdc,prior_day,source_sha256`. It covers 13,255 of 13,256 stock-days (99.992%). The only missing prior close is **ICICILIFE on 2026-10-23**: the symbol is absent from the official 2026-10-22 EQ archive. Its six clock candidates fail closed. The current-day archive contains a previous-close field, but that after-close value is diagnostic only; it was never used for the morning signal. Among available comparisons it disagreed with the causal prior EQ close on zero rows.
- `raw.prev_close` is the previous **five-minute** cash close. It is not a previous-day close. The frozen intraday bars end before the official final close, so no 15:15 value was substituted for PDC.

## Candidate coverage and verification

| Signal clock | Candidate rows | `integrity_ok` rows | Finite OI delta rows |
|---|---:|---:|---:|
| 09:20 | 13,256 | 6,516 | 6,548 |
| 09:25 | 13,256 | 13,038 | 13,244 |
| 09:30 | 13,256 | 12,763 | 13,040 |
| 09:35 | 13,256 | 12,779 | 13,252 |
| 09:40 | 13,256 | 12,916 | 13,252 |
| 09:45 | 13,256 | 12,903 | 13,252 |
| **Total** | **79,536** | **70,915** | — |

The 09:40/09:45 rows for 2026-10-28, 2026-10-29, 2026-10-30, and 2026-11-02 came from the already archived `g3_indicator_lab_20261103/expanded_slots/{day}.parquet` because those clocks were absent in the corresponding raw snapshot. The supplement fills only missing clocks and retains exact prior OI evidence. The full grid has unique day/stock/clock and globally unique `sid` values.

`integrity_reason` counts overlap: 7,128 nonfinite six-feature rows, 6,948 invalid signal OI rows, 6,708 unavailable opening 4m comparisons, 1,344 zero cash-price moves, 486 prior-OI-bar mismatches, 246 rows (41 stock-days) without an opening OI baseline, and six missing-PDC rows. In particular, the 6,708 missing 09:20 OI comparisons are **not** filled from later bars; the 09:20 sample is materially smaller. All 79,536 rows have archived next-minute confirmation candles, but their availability is assessed only at execution time.

The computed price 5m percentage matches the archived `price_change_pct` within 0.000006 percentage point on all comparable rows. The computed OI delta matches `oi_change_pct` within floating-point precision. All source price rows identify `NSE_EQUITY_1M_CAUSAL_5X_AGGREGATION` and all OI rows identify `NFO_FUTURE`. The source grid, V14 V2 configuration, official files, prepared parquet, and 63 per-day parquets have SHA256 records in `manifest.json`.

This source audit verifies historical input provenance and availability, not future profitability. These 63 sessions have already been examined in prior strategy work, so any parameter search on them remains in-sample until evaluated on later untouched sessions.

## Freeze identifiers and freshness

The official files were fetched on 2026-11-03 between 09:26:59 and 09:27:03 UTC; the latest signal/archive session is 2026-11-02. NSE reports the latest archive's HTTP last-modified header as `Mon, 02 Nov 2026 11:03:03 GMT` and its ZIP SHA256 as `52dbeb12f276c70a5f2d30a719e539b925189360da4709bc37916b970c1ca6e4`. No 2026-11-03 intraday source is claimed. The file-level SHA256 identifiers are:

| File | SHA256 |
|---|---|
| `six_feature_source/pdc.csv` | `cffcb42ec35e92d7250faed6ad236b485dec216900cb4486031f25cba7992c24` |
| `six_feature_source/candidates.parquet` | `b3880cba1f5670fdf8189aab189152ac03b821abfd67b567e85da8dbdc1f47e3` |
| `six_feature_source/manifest.json` | `4b1c3931c458789d6b170c3a492b86428a6a623569a28eb733dccb43b7870dbc` |

The configuration SHA256 recorded inside the candidate manifest is `11283977c2985f7dccbb021648b406df4d017574fc3a1d6b03f2011c71972301`.
