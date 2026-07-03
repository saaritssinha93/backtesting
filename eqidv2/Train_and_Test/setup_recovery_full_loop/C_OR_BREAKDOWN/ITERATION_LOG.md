# C_OR_BREAKDOWN — ITERATION_LOG

_Generated 2026-07-03. Optimizer: Optuna TPE. Total logged iterations: 1805 (full detail in iteration_log.csv)._

Protocol: each iteration changes ONE logical group (version base / exit / guard / one mask term / one premom term / combo trial), is scored on FIT and VAL, and only stable configs were confirmed on full TRAIN. TEST was scored ONCE per confirmed config (0 of max 15 TEST evaluations used). Band objective: reward(min(FIT_PF,VAL_PF)) tent-peaked at 1.80 − 0.8·|FIT_PF−VAL_PF|.

## Stage row counts

| stage       |   count |
|:------------|--------:|
| S4_sweep    |     968 |
| S8_round2   |     423 |
| S5_combo    |     400 |
| S3_versions |      13 |
| S6_confirm  |       1 |

## TRAIN confirmations + TEST verdicts

| iter | stage | change | cfg | TRAIN n/PF/net | TEST n/PF/net | decision |
|---|---|---|---|---|---|---|
| 1382 | S6_confirm | confirm | `SL0.9/T2.0 pm[sig5_adx_calc>=39.670518;pre1_adx<=21.368044;pre3_range_r<=0.121555]` | 32/1.031/Rs407 | - | reject_train |
| 1793 | S8_round2 | conf+srp_q25+body_q50+p3r_q25|0.9/1.25|None | `SL0.9/T1.25 mask[signal_range_pct<=0.271261;body_pct>=0.721523] pm[sig5_adx_calc>=39.670518;pre1_adx<=21.368044;pre3_range_r<=0.19008]` | 24/1.731/Rs4,684 | - | reject_train |
| 1794 | S8_round2 | conf+body_q50+volcap_q50+p3r_q50|1.1/2.0|None | `SL1.1/T2.0 mask[body_pct>=0.721523;vol_ratio<=2.347911] pm[sig5_adx_calc>=39.670518;pre1_adx<=21.368044;pre3_range_r<=0.304862]` | 35/1.453/Rs6,047 | - | reject_train |
| 1795 | S8_round2 | conf+srp_q25+body_q50+p3r_q25|0.9/1.5|None | `SL0.9/T1.5 mask[signal_range_pct<=0.271261;body_pct>=0.721523] pm[sig5_adx_calc>=39.670518;pre1_adx<=21.368044;pre3_range_r<=0.19008]` | 24/2.074/Rs6,880 | - | reject_train |
| 1796 | S8_round2 | conf+body_q50+volcap_q50+p3r_q50|1.1/1.5|None | `SL1.1/T1.5 mask[body_pct>=0.721523;vol_ratio<=2.347911] pm[sig5_adx_calc>=39.670518;pre1_adx<=21.368044;pre3_range_r<=0.304862]` | 35/1.349/Rs4,656 | - | reject_train |
| 1797 | S8_round2 | conf+body_q50+fresh3+p3r_q25|0.9/1.5|None | `SL0.9/T1.5 mask[body_pct>=0.721523;fresh_age_bars<=3.0] pm[sig5_adx_calc>=39.670518;pre1_adx<=21.368044;pre3_range_r<=0.19008]` | 25/1.547/Rs4,551 | - | reject_train |
| 1798 | S8_round2 | conf+srp_q25+body_q50+p3r_q25|0.9/2.0|None | `SL0.9/T2.0 mask[signal_range_pct<=0.271261;body_pct>=0.721523] pm[sig5_adx_calc>=39.670518;pre1_adx<=21.368044;pre3_range_r<=0.19008]` | 24/1.808/Rs6,084 | - | reject_train |
| 1799 | S8_round2 | conf+body_q50+fresh3+p3r_q50|1.1/1.5|None | `SL1.1/T1.5 mask[body_pct>=0.721523;fresh_age_bars<=3.0] pm[sig5_adx_calc>=39.670518;pre1_adx<=21.368044;pre3_range_r<=0.304862]` | 42/1.334/Rs5,197 | - | reject_train |
| 1800 | S8_round2 | conf+body_q50+fresh3+p3r_q50|0.9/1.5|None | `SL0.9/T1.5 mask[body_pct>=0.721523;fresh_age_bars<=3.0] pm[sig5_adx_calc>=39.670518;pre1_adx<=21.368044;pre3_range_r<=0.304862]` | 42/1.239/Rs3,902 | - | reject_train |
| 1801 | S8_round2 | conf+body_q50+fresh3+p3r_q50|1.1/2.0|None | `SL1.1/T2.0 mask[body_pct>=0.721523;fresh_age_bars<=3.0] pm[sig5_adx_calc>=39.670518;pre1_adx<=21.368044;pre3_range_r<=0.304862]` | 42/1.267/Rs4,508 | - | reject_train |
| 1802 | S8_round2 | conf+body_q50+volcap_q50+p3r_q50|0.9/2.0|None | `SL0.9/T2.0 mask[body_pct>=0.721523;vol_ratio<=2.347911] pm[sig5_adx_calc>=39.670518;pre1_adx<=21.368044;pre3_range_r<=0.304862]` | 35/1.43/Rs5,832 | - | reject_train |
| 1803 | S8_round2 | conf+body_q50+fresh3+rsidir_q75cap|0.9/2.0|None | `SL0.9/T2.0 mask[body_pct>=0.721523;fresh_age_bars<=3.0] pm[sig5_adx_calc>=39.670518;pre1_adx<=21.368044;sig5_rsi_dir<=73.187544]` | 52/1.323/Rs6,403 | - | reject_train |
| 1804 | S8_round2 | conf+body_q50+volcap_q50+p3r_q50|0.9/1.5|None | `SL0.9/T1.5 mask[body_pct>=0.721523;vol_ratio<=2.347911] pm[sig5_adx_calc>=39.670518;pre1_adx<=21.368044;pre3_range_r<=0.304862]` | 35/1.327/Rs4,440 | - | reject_train |
| 1805 | S8_round2 | closest-near-miss (n<35) scored on TEST once for the record | `SL0.9/T1.5 mask[signal_range_pct<=0.271261;body_pct>=0.721523] pm[sig5_adx_calc>=39.670518;pre1_adx<=21.368044;pre3_range_r<=0.19008]` | 24/2.074/Rs6,880 | 10/0.007/Rs-6,376 | diagnostic_only |

## Top 20 FIT/VAL iterations overall

| iter | stage | change | FIT n/PF | VAL n/PF | score |
|---|---|---|---|---|---|
| 1619 | S8_round2 | conf+srp_q25+body_q50+p3r_q25 exit 0.9/1.25 g=None | 10.0/1.787 | 14.0/1.685 | 1.6334 |
| 1682 | S8_round2 | conf+body_q50+volcap_q50+p3r_q50 exit 1.1/2.0 g=None | 18.0/1.456 | 17.0/1.449 | 1.4944 |
| 1616 | S8_round2 | conf+srp_q25+body_q50+p3r_q25 exit 0.9/1.5 g=None | 10.0/2.217 | 14.0/1.957 | 1.3865 |
| 1419 | S8_round2 | conf+srp_q25+body_q50+p3r_q25 | 10.0/2.217 | 14.0/1.957 | 1.3865 |
| 1679 | S8_round2 | conf+body_q50+volcap_q50+p3r_q50 exit 1.1/1.5 g=None | 18.0/1.318 | 17.0/1.391 | 1.3106 |
| 1471 | S8_round2 | conf+body_q50+fresh3+p3r_q25 | 12.0/1.443 | 13.0/1.666 | 1.3006 |
| 1634 | S8_round2 | conf+body_q50+fresh3+p3r_q25 exit 0.9/1.5 g=None | 12.0/1.443 | 13.0/1.666 | 1.3006 |
| 1613 | S8_round2 | conf+srp_q25+body_q50+p3r_q25 exit 0.9/2.0 g=None | 10.0/1.594 | 14.0/2.052 | 1.2576 |
| 1661 | S8_round2 | conf+body_q50+fresh3+p3r_q50 exit 1.1/1.5 g=None | 25.0/1.373 | 17.0/1.27 | 1.1876 |
| 1652 | S8_round2 | conf+body_q50+fresh3+p3r_q50 exit 0.9/1.5 g=None | 25.0/1.257 | 17.0/1.207 | 1.167 |
| 1472 | S8_round2 | conf+body_q50+fresh3+p3r_q50 | 25.0/1.257 | 17.0/1.207 | 1.167 |
| 1664 | S8_round2 | conf+body_q50+fresh3+p3r_q50 exit 1.1/2.0 g=None | 25.0/1.236 | 17.0/1.327 | 1.1632 |
| 1667 | S8_round2 | conf+body_q50+volcap_q50+p3r_q50 exit 0.9/2.0 g=None | 18.0/1.541 | 17.0/1.301 | 1.16 |
| 1775 | S8_round2 | conf+body_q50+fresh3+rsidir_q75cap exit 0.9/2.0 g=None | 30.0/1.367 | 22.0/1.252 | 1.16 |
| 1460 | S8_round2 | conf+body_q50+volcap_q50+p3r_q50 | 18.0/1.395 | 17.0/1.248 | 1.1304 |
| 1670 | S8_round2 | conf+body_q50+volcap_q50+p3r_q50 exit 0.9/1.5 g=None | 18.0/1.395 | 17.0/1.248 | 1.1304 |
| 1736 | S8_round2 | conf+body_q50+rsidir_q75cap exit 1.1/2.0 g=None | 34.0/1.117 | 29.0/1.142 | 1.097 |
| 1649 | S8_round2 | conf+body_q50+fresh3+p3r_q50 exit 0.9/2.0 g=None | 25.0/1.159 | 17.0/1.261 | 1.0774 |
| 1673 | S8_round2 | conf+body_q50+volcap_q50+p3r_q50 exit 0.9/1.25 g=None | 18.0/1.123 | 17.0/1.096 | 1.0744 |
| 1706 | S8_round2 | conf+srp_q50+body_q50+p3r_q25 exit 0.9/1.5 g=None | 13.0/1.149 | 20.0/1.249 | 1.069 |