# Missing donor-slot reconstruction

This is an isolated input supplement for the `all51_slots` ablation only. It does not change the control, other indicator arms, existing raw inputs, live trading, paper trading, or a current-result pointer.

The 51 donor long/short setups use 35 distinct signal clocks. Original feature ledgers cover all those clocks on 59 of 63 sessions. Four later sessions contain only the 28 active G3 clocks. Treating those ledgers as a full 51-setup test would omit seven possible signal times.

The reconstructed signal clocks are **09:40, 09:45, 10:00, 11:45, 12:25, 13:20 and 13:25 IST**, each with its existing one-minute-later confirmation contract. These are signal clocks, not entry timestamps.

| Date | Existing stock cohort | Supplemental stock-clock rows | Missing stock-clock rows | Invalid supplemental OI rows | Existing non-opening rows checked |
|---|---:|---:|---:|---:|---:|
| 2026-10-28 | 215 | 1,505 | 0 | 0 | 5,805 |
| 2026-10-29 | 216 | 1,512 | 0 | 0 | 5,832 |
| 2026-10-30 | 216 | 1,512 | 0 | 0 | 5,832 |
| 2026-11-02 | 215 | 1,505 | 0 | 0 | 5,805 |

The 6,034 new rows are the entire missing-clock cross product for the **existing** cohorts. This does not restore excluded stocks or prove a full 218-stock live universe. In particular, October 28 and November 2 inherit the original after-close stock-day eligibility restrictions. The source restrictions and retrospective nature of the research still apply.

## Source identity and calculation

`../expand_slots.py` resolves each input's `source_provenance.json`, verifies the snapshot-manifest hash, verifies its canonical fingerprint and verifies every captured snapshot member's SHA-256. October 28's compact snapshot has only day execution candles. Its hash-linked original full-history snapshot is therefore used for reconstruction; the compact file is not used to invent historical warmup.

The original raw ledger and feature-ledger hashes are checked. Only original raw-ledger symbols and their original futures contracts are admitted. No data is downloaded. No source file or original lab raw row is edited or replaced.

Native `_observed_pool` arithmetic constructs completed cash five-minute candles, exact confirmation features and futures OI joins. For speed, parquet reads retain the latest 20 calendar days and native rolling calculations use 1,000 preceding minutes plus the current session. The five-minute EMA9/20/50 and one-minute EMA20 continue the exact native `adjust=False` recurrence from the earliest sealed earlier feature value. All new clocks occur after these anchors. Anchors never use a later row, and indicators use completed minutes only.

At every existing non-opening stock-clock, 26 numeric features are compared against the immutable ledger at `rtol=1e-10, atol=1e-10`. All **23,274 rows** passed. Opening rows are retained unchanged; their separate 09:16-to-09:20 OI and cash-opening conventions are not replaced by generic native five-minute arithmetic.

A separate method check compared the seven new clocks and all 26 fields for October 29/360ONE with a full-history native replay: maximum numerical difference was **0.0**. This is a method check, not a claim that every supplemental row was separately replayed from the beginning of its price history.

The shorter calculation window exposed a pandas dtype issue before publication. For FORCEMOT on October 29, `int16` close 17,080 times `int16` volume 9,067 overflowed to 2,792 rather than the archived value 154,864,360. The supplement evaluates the unchanged `close * volume` formula in float64. This is an **export-only arithmetic correction in this lab**, with existing-row parity required afterward; production/native strategy files and earlier input files are unchanged. Details are in [method_validation.json](method_validation.json).

For a new clock, futures OI must have the exact `t-5 minutes` predecessor. If it does not, the supplement sets prior OI and OI change to missing so the immutable selector rejects that candidate. A missing current OI bar is recorded as uncovered. Missing new-clock OI never causes retrospective deletion of an existing stock's other rows. No such missing/invalid OI remains in these four completed supplements.

## Consumer contract and frozen outputs

Read [manifest.json](manifest.json). Require an empty `pending_days` list and inspect each day status and coverage. Each supplemental parquet deliberately omits `sid`; the caller assigns globally unique IDs and appends rows **only for `all51_slots`**. All other arms retain their original inputs. Existing `[tradingsymbol, signal_ts]` keys cannot overlap the supplement.

[consumer_validation.json](consumer_validation.json) confirms that adding the supplemental pool leaves control selections unchanged on all four days. It reports selected candidates, not executed trades or P&L. The native replay remains responsible for fills, costs, capacity, sizing and exits.

Verified published supplements are reused without rewriting them. The October 30 consumer's captured hash was independently confirmed by the parent runner to match the final artifact.

| Supplemental file | SHA-256 |
|---|---|
| `2026-10-28.parquet` | `626101d186567c190b092e543f40f90ec10364f0b9223a88ff38feadc902c755` |
| `2026-10-29.parquet` | `ef54796a041b3e74f6f51f21661ccc22f8062351bb2eb12ade6c4a375532af3b` |
| `2026-10-30.parquet` | `fe9603c59cf236d6317d3154cfba287cab8b7a74ae719e863826d4fe11efb985` |
| `2026-11-02.parquet` | `21a3fb90ddc96088f59c3508733c304fb523f9b0530927541811086904e661f0` |

Each `*.proof.json` records exact source paths/hashes, immutable raw-input hash, per-stock coverage, numerical parity tolerances and maximum differences. Re-running `python -B -m g3_indicator_lab_20261103.expand_slots` reuses those verified exports. There is no parameter search in this reconstruction.
