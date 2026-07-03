# C_OR_BREAKOUT — ITERATION_LOG

_Generated 2026-07-03. Optimizer: Optuna TPE. Total logged iterations: 2363 (full detail in iteration_log.csv)._

Protocol: each iteration changes ONE logical group (version base / exit / guard / one mask term / one premom term / combo trial), is scored on FIT and VAL, and only stable configs were confirmed on full TRAIN. TEST was scored ONCE per confirmed config (0 of max 15 TEST evaluations used). Band objective: reward(min(FIT_PF,VAL_PF)) tent-peaked at 1.80 − 0.8·|FIT_PF−VAL_PF|.

## Stage row counts

| stage       |   count |
|:------------|--------:|
| S4_sweep    |    1477 |
| S8_round2   |     473 |
| S5_combo    |     400 |
| S3_versions |      13 |

## TRAIN confirmations + TEST verdicts

| iter | stage | change | cfg | TRAIN n/PF/net | TEST n/PF/net | decision |
|---|---|---|---|---|---|---|
| 2361 | S8_round2 | broad+volcap_q25+rs_q75+|1.1/2.5|{'top_n': 2} | `SL1.1/T2.5 mask[vol_ratio<=1.964849;rs_pct>=3.792928] pm[sig5_adx_calc>=30.0;pre1_adx<=25.0] g{"top_n": 2}` | 21/1.167/Rs2,026 | - | reject_train |
| 2362 | S8_round2 | broad+volcap_q25+rs_q75+|1.5/2.5|{'top_n': 2} | `SL1.5/T2.5 mask[vol_ratio<=1.964849;rs_pct>=3.792928] pm[sig5_adx_calc>=30.0;pre1_adx<=25.0] g{"top_n": 2}` | 21/1.034/Rs465 | - | reject_train |
| 2363 | S8_round2 | broad+volcap_q25+rs_q75+|1.1/2.0|{'top_n': 2} | `SL1.1/T2.0 mask[vol_ratio<=1.964849;rs_pct>=3.792928] pm[sig5_adx_calc>=30.0;pre1_adx<=25.0] g{"top_n": 2}` | 21/1.041/Rs497 | - | reject_train |

## Top 20 FIT/VAL iterations overall

| iter | stage | change | FIT n/PF | VAL n/PF | score |
|---|---|---|---|---|---|
| 2304 | S8_round2 | broad+volcap_q25+rs_q75+ exit 1.1/2.5 g={'top_n': 2} | 10.0/1.114 | 11.0/1.21 | 1.0372 |
| 2312 | S8_round2 | broad+volcap_q25+rs_q75+ exit 1.5/2.5 g={'top_n': 2} | 10.0/1.034 | 11.0/1.034 | 1.034 |
| 2300 | S8_round2 | broad+volcap_q25+rs_q75+ exit 1.1/2.0 g={'top_n': 2} | 10.0/1.108 | 11.0/0.986 | 0.8884 |
| 1747 | S5_combo | trial 257 | 39.0/0.787 | 19.0/0.828 | 0.7542 |
| 2262 | S8_round2 | nogate+volcap_q25+near_vwap+p5mom_q75 exit 1.5/2.5 g={'min_slot': '12:30'} | 18.0/0.926 | 14.0/0.82 | 0.7352 |
| 1561 | S5_combo | trial 71 | 16.0/0.761 | 9.0/0.741 | 0.725 |
| 1562 | S5_combo | trial 72 | 16.0/0.761 | 9.0/0.741 | 0.725 |
| 1563 | S5_combo | trial 73 | 16.0/0.761 | 9.0/0.741 | 0.725 |
| 2308 | S8_round2 | broad+volcap_q25+rs_q75+ exit 1.5/2.0 g={'top_n': 2} | 10.0/1.029 | 11.0/0.842 | 0.6924 |
| 2201 | S8_round2 | nogate+volcap_q25+rs_q75+pms_q75 exit 1.1/2.0 g=None | 23.0/0.703 | 18.0/0.686 | 0.6724 |
| 2303 | S8_round2 | broad+volcap_q25+rs_q75+ exit 1.1/2.5 g={'max_slot': '12:30'} | 11.0/0.685 | 10.0/0.668 | 0.6544 |
| 2265 | S8_round2 | broad+bigbar_q90+fresh1+p5mom_q75 exit 0.9/2.0 g=None | 17.0/0.696 | 10.0/0.668 | 0.6456 |
| 2261 | S8_round2 | nogate+volcap_q25+near_vwap+p5mom_q75 exit 1.5/2.5 g=None | 37.0/0.733 | 22.0/0.845 | 0.6434 |
| 2333 | S8_round2 | nogate+volcap_q25+rs_q75+p5mom_q75 exit 1.5/2.5 g=None | 26.0/0.738 | 21.0/0.684 | 0.6408 |
| 1760 | S5_combo | trial 270 | 79.0/0.687 | 48.0/0.658 | 0.6348 |
| 1745 | S5_combo | trial 255 | 79.0/0.687 | 48.0/0.658 | 0.6348 |
| 1740 | S5_combo | trial 250 | 79.0/0.687 | 48.0/0.658 | 0.6348 |
| 1768 | S5_combo | trial 278 | 79.0/0.687 | 48.0/0.658 | 0.6348 |
| 1797 | S5_combo | trial 307 | 79.0/0.687 | 48.0/0.658 | 0.6348 |
| 1781 | S5_combo | trial 291 | 79.0/0.687 | 48.0/0.658 | 0.6348 |