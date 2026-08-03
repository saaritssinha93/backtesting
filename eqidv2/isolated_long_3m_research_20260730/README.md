# Isolated three-month intraday LONG research

This directory is intentionally self-contained. The research reads the existing
five-minute and one-minute parquet stores but does not import, edit, overwrite,
or write beside any existing backtesting/live file.

Research window and split:

- Latest 60 completed sessions through 2026-07-29
- TRAIN: first 40 sessions
- VALIDATION: next 10 sessions
- TEST: final 10 sessions, untouched until model and exit selection is frozen

Run:

```powershell
python .\isolated_long_3m_research_20260730\research.py research
```

All caches, candidates, trades, diagnostics, and reports are written beneath
`isolated_long_3m_research_20260730/outputs`.

