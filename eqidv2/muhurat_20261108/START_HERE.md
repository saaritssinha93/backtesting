# G3 Muhurat - 8 November 2026 only

The isolated market-data, PAPER simulation, daily replay and read-only dashboard are configured for **8 November 2026 only**. LIVE order placement was not enabled or rescheduled. Normal weekday calendars, strategy files, live controls and existing backtest histories were not changed.

| Activity | Time (IST) |
|:--|:--|
| Cash session opens | 14:20 |
| Completed five-minute signals | 14:25, 14:30, 14:35, 14:40, 14:45, 14:50, 14:55, 15:00, 15:05 |
| One-minute confirmations | 14:26 through 15:06, every five minutes |
| Paper square-off | 15:10 |
| Cash data collection ends | 15:15 |
| Futures/OI collection ends | 15:40 |
| Final replay and worker finish | 15:45 |
| Dashboard/date override expires | Midnight beginning 9 November |

G3 here trades cash stocks from the F&O universe. The paper cutoff precedes Zerodha's 15:12 cash-stock MIS cutoff. Futures data collected until 15:40 supplies OI and research; this does not enable futures orders. [Official session schedule](https://zerodha.com/marketintel/bulletin/461711/muhurat-trading-session-on-account-of-diwali-november-2026).

All nine scan clocks have LONG and SHORT rules: **18 pairs**, comprising eight inherited Friday-active pairs and ten explicitly enabled donor pairs. The approved source range is **09:25-10:05 in order**, applied only to today's paper simulation and daily replay. Three missing same-clock SHORT rules use the nearest earlier same-side donor. The full mapping, thresholds, targets, sizing, caps and fingerprints are in [plan.json](../outputs/muhurat_20261108/plan.json).

| Actual scan (IST) | Morning source | LONG donor | SHORT donor |
|:--|:--|:--|:--|
| 14:25 | 09:25 | 0926_LONG | 0926_SHORT |
| 14:30 | 09:30 | 0931_LONG | 0931_SHORT |
| 14:35 | 09:35 | 0936_LONG | 0936_SHORT |
| 14:40 | 09:40 | 0941_LONG | 0941_SHORT |
| 14:45 | 09:45 | 0946_LONG | 0946_SHORT |
| 14:50 | 09:50 | 0951_LONG | 0951_SHORT |
| 14:55 | 09:55 | 0956_LONG | 0951_SHORT (09:50 fallback) |
| 15:00 | 10:00 | 1001_LONG | 0951_SHORT (09:50 fallback) |
| 15:05 | 10:05 | 1006_LONG | 0951_SHORT (09:50 fallback) |

Donor IDs use their one-minute confirmation clock. Each actual confirmation remains one minute after the listed scan, with paper square-off at 15:10. The first actual SHORT scan keeps the 09:25 donor's NIFTY confirmation requirement. Targets, stops, entry caps and thresholds are copied from the source rules without retuning.

This session follows Friday's production G3 rules, including their 1.25% initial stop and individual targets. The separately adopted V23 breakeven research baseline was not promoted into this production-style paper session. Existing per-pair entry caps, one open position per symbol and Rs10 lakh portfolio capital remain. The added pairs and shorter session make this a distinct experiment.

The actual Sunday opening cash candle and 14:21 first-minute futures OI are used. Friday histories provide indicator warm-up only; no Friday candle is relabelled as a real Sunday candle. All new data and results remain in outputs/muhurat_20261108.

The Windows task is **EQIDV2_Muhurat_PAPER_BACKTEST_20261108**. It starts in advance and has a single 14:10 trigger, no next-day catch-up and a singleton lock against duplicate workers. It stops session processing by 15:45. A date guard prevents work on another day, and the dashboard releases its local port after midnight.

**Authentication is currently required:** all eight Kite applications returned TokenException during setup. Refresh today's Kite authentication using the existing login flow. The watcher reloads credentials every five minutes and can use a subset of healthy apps. It cannot fetch fresh prices or generate genuine paper fills until authentication succeeds.

Local warm-up is ready for 218 equities and 224 futures contracts, including indices. Failed apps and missing/corrupt stock files are handled individually. Reports distinguish complete processing with exclusions from missing entire slots. No qualifying signal is a valid outcome; configuration does not guarantee trades.

Paper decisions record when data actually became available. New fills cannot precede that decision. Late or missing internal bars are not silently backfilled as new paper entries. Decisions and orders persist across restarts. Daily replay can reconstruct the session after data arrives, so its fills can differ from prospective paper fills; the dashboard shows them separately.

During the session, the watcher writes paper_trades.csv, slots.csv and scan_audit.csv. After 15:10 it publishes daily_backtest_trades.csv, daily_backtest_summary.json and DAILY_BACKTEST.md, followed by a final refresh after data collection. Pending or unfilled orders are not counted as filled trades.

[Open the local Muhurat dashboard](OPEN_DASHBOARD.url). The shortcut contains a local access token; keep it private. The page links to the existing standard dashboard in read-only mode. All write requests are disabled.

[Current status](../outputs/muhurat_20261108/status.json) | [Session plan](../outputs/muhurat_20261108/plan.json) | [Warm-up evidence](../outputs/muhurat_20261108/data/bootstrap_manifest.json)

To inspect without modifying the task:

```powershell
Get-ScheduledTask -TaskName EQIDV2_Muhurat_PAPER_BACKTEST_20261108
python -m pytest muhurat_20261108 -q
```
