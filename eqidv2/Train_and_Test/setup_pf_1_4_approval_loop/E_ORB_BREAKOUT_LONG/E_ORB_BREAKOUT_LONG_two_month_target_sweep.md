# E_ORB_BREAKOUT_LONG Two-Month Target Sweep

- Window: 2026-04-27..2026-06-24 (36 completed sessions)
- Pool: `C:\TradingData\eqidv2\outputs_ID_v11_conf_fresh_20260629`
- Fixed rules: `rs_pct >= 5.606893` AND `vwap_dist_atr <= 0.979716`
- Fixed SL: `1.0%`; target swept over `[0.8, 1.0, 1.2, 1.25, 1.5, 1.75, 2.0, 2.25, 2.5, 2.75, 3.0, 3.5, 4.0]`
- Cost: `statutory`, slippage `15.0` bps per leg

## Sweep Table

| Tgt | Trades | PF | Net Rs | Win% | T/SL/EOD | Avg W/L | Max DD | Top Day | Top Sym | Day p |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 0.80 | 22 | 0.664 | -3,721 | 59.1 | 13/9/0 | 565/-1,230 | -4,653 | nan | nan | 0.7881 |
| 1.00 | 22 | 0.898 | -1,128 | 59.1 | 13/9/0 | 765/-1,230 | -3,554 | nan | nan | 0.5688 |
| 1.20 | 22 | 0.784 | -2,922 | 50.0 | 11/11/0 | 964/-1,230 | -4,580 | nan | nan | 0.7032 |
| 1.25 | 22 | 0.825 | -2,374 | 50.0 | 11/11/0 | 1,014/-1,230 | -4,181 | nan | nan | 0.6683 |
| 1.50 | 22 | 1.027 | 369 | 50.0 | 11/11/0 | 1,264/-1,230 | -2,464 | 6.844 | 3.432 | 0.46 |
| 1.75 | 22 | 1.025 | 368 | 45.5 | 10/12/0 | 1,513/-1,230 | -4,638 | 8.219 | 4.121 | 0.4665 |
| 2.00 | 22 | 1.194 | 2,861 | 45.5 | 10/12/0 | 1,762/-1,230 | -4,389 | 1.231 | 0.617 | 0.3655 |
| 2.25 | 22 | 1.363 | 5,354 | 45.5 | 10/12/0 | 2,011/-1,230 | -4,139 | 0.751 | 0.376 | 0.2473 |
| 2.50 | 22 | 1.532 | 7,847 | 45.5 | 10/12/0 | 2,261/-1,230 | -3,890 | 0.576 | 0.289 | 0.1898 |
| 2.75 | 22 | 1.701 | 10,341 | 45.5 | 10/12/0 | 2,510/-1,230 | -3,686 | 0.485 | 0.243 | 0.1227 |
| 3.00 | 22 | 1.553 | 8,847 | 40.9 | 9/13/0 | 2,760/-1,230 | -7,379 | 0.623 | 0.313 | 0.1849 |
| 3.50 | 22 | 0.780 | -4,603 | 22.7 | 5/17/0 | 3,260/-1,230 | -8,651 | nan | nan | 0.7399 |
| 4.00 | 22 | 0.899 | -2,108 | 22.7 | 5/17/0 | 3,759/-1,230 | -8,610 | nan | nan | 0.5953 |

## Ranked By PF

1. Target 2.75: PF 1.701, n=22, net Rs 10,341, topDay=0.485, day_p=0.1227
2. Target 3.00: PF 1.553, n=22, net Rs 8,847, topDay=0.623, day_p=0.1849
3. Target 2.50: PF 1.532, n=22, net Rs 7,847, topDay=0.576, day_p=0.1898
4. Target 2.25: PF 1.363, n=22, net Rs 5,354, topDay=0.751, day_p=0.2473
5. Target 2.00: PF 1.194, n=22, net Rs 2,861, topDay=1.231, day_p=0.3655

## Notes

- Approval-style stability still cares about top day/symbol dominance and day-block p, not just PF.
- Same entry set is used for every target; only resolver target changes.
- No final config files were edited.
