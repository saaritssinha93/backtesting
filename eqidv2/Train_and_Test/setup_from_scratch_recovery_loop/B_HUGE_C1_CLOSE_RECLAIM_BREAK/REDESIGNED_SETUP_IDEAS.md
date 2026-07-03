# B_HUGE_C1_CLOSE_RECLAIM_BREAK (LONG) — REDESIGNED_SETUP_IDEAS

_Generated 2026-07-03. From-scratch recovery loop. Research-only; NO live trades; NO final_setup_conf.py edits._

Setup intent: momentum continuation — break of a prior HUGE GREEN bar's high in a non-bear regime.

| family | idea | why it makes sense | outcome |
|---|---|---|---|
| F1_exit_engineering | break-even / trailing / time-stop exits on the round-2 anchor | family losses are SL+EOD heavy; reshape the loss tail without changing selection | no config cleared FIT/VAL+TRAIN gates |
| F2_retest_entry | resting limit at a pullback of alpha*ATR (cancel after K min) | huge-bar signals are extended at the close; chasing the next open pays the worst price | no config cleared FIT/VAL+TRAIN gates |
| F3_fit_mined_filters | filters mined from FIT winners-vs-losers medians | let the data name the confirmation instead of guessing; validated on untouched VAL | no config cleared FIT/VAL+TRAIN gates |
| F4_time_topn | open-vs-midday windows + stricter per-slot ranking | hour-of-day PnL is uneven; duplicates within a slot dilute quality | no config cleared FIT/VAL+TRAIN gates |
| F5_fade | flip the side of the detection | if continuation systematically fails, the failure itself may be the trade | not run (gated off by diagnostics) |