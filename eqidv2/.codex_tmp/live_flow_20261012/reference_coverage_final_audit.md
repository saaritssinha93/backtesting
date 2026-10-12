# Dashboard Flow reference coverage audit

Final read-only verification on 12 October 2026, after the reporting-only daily cache was populated. No broker calls, trading-indicator edits, signal writes, or order operations were performed by this audit.

## Final coverage

| Check | Result |
|---|---:|
| Selectable trading sessions | 95 |
| First / latest selectable session | 2026-05-27 / 2026-10-12 |
| Sessions with daily previous-close coverage for every universe stock | 95 / 95 |
| Completed historical sessions with daily opening coverage for every universe stock | 94 / 94 |
| Today's previous-close coverage | 213 / 213 |
| Today's completed opening-bar coverage, independently audited | 213 / 213 |
| Missing previous-close symbols | 0 |
| Missing historical opening symbols | 0 |

The final coverage audit uses the dashboard's `_daily_valid` checks, the exchange-calendar `_previous_session` lookup, each session's dated or reconstructed universe, and the explicit `LTIM -> LTM` predecessor mapping. Historical-only `DALBHARAT` is present in the new daily cache. Today's unfinished daily candle is not used for opening data.

The daily source is `C:/TradingData/eqidv2/fno_oi/raw_equity_daily/<symbol>_day.parquet`. The audit validates the declared `KITE_HISTORICAL_DAY` source, `VALID` quality, finite prices/volume, OHLC consistency, positive open/close, and duplicate-day exclusion. All 95 sessions pass with no remaining missing references. The former selectable 2026-09-14 holiday is excluded by the calendar.

Per-session counts and missing-symbol lists are in `daily_reference_coverage_after_backfill.json`. The reproducible, read-only audit is `audit_daily_reference_coverage.py`.

## Causes of the original missing references

- Every one of the 213 current-universe stocks has October 9 historical one-minute data ending at 15:15. Both live and historical five-minute sources contain synthetic or gap-filled 15:30 rows for all 213 stocks. These sources could not supply a verified previous closing reference for October 12.
- All 1,041 old daily indicator files end before the first selectable session: 1,019 end May 15; 12 end April 17; five end April 9; five end March 2. Existing daily/one-minute/five-minute stock-cache directories are empty.
- Historical symbol membership adds `DALBHARAT` and `LTIM` to the 213 current symbols. LTIM's original one-minute history ends April 2 and its historical five-minute file ends May 19; the established LTM predecessor mapping resolves the daily reference. DALBHARAT needed its own historical daily file.
- A 09:15 `opening_snapshot` row is not an independently captured opening quote. For RELIANCE on October 12, it copies the complete 09:20 OHLC, including the later close. The reconstructed opening uses the first completed bar's **open**, with its availability identified separately.
- The old calendar inferred the previous session from available files. The corrected calendar uses the exchange's trading sessions, so a missing local day is not silently skipped.

The earlier `close_open_source_audit.*` files are preliminary intraday-only diagnostics. Their full historical close counts predate the added copied-OHLCV guard and must not be used as final validation. The final daily-reference coverage above supersedes those counts. No historical backtest results or as-recorded strategy decisions were recomputed.
