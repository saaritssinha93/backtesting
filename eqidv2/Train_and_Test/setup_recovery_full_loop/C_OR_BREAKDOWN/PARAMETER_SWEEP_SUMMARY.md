# C_OR_BREAKDOWN — PARAMETER_SWEEP_SUMMARY (Stage 4)

_Generated 2026-07-03._

One-knob-at-a-time sweeps from each version base: 968 iterations across 8 versions. FIT quantile grids; VAL as check.

## Top 30 sweep iterations

| iter | version | change | FIT n/PF | VAL n/PF | score |
|---|---|---|---|---|---|
| 233 | V1_conf_gate | pm +pre3_range_r<=0.121555 | 16.0/0.973 | 16.0/1.083 | 0.885 |
| 212 | V1_conf_gate | mask +signal_range_pct<=0.182476 | 17.0/0.99 | 24.0/1.286 | 0.7532 |
| 181 | V1_conf_gate | mask +vol_ratio<=2.347911 | 79.0/0.735 | 52.0/0.748 | 0.7246 |
| 822 | V11_broad_gate | mask +lower_wick_pct<=0.0 | 110.0/0.703 | 86.0/0.695 | 0.6886 |
| 803 | V11_broad_gate | mask +close_loc<=0.0 | 110.0/0.703 | 86.0/0.695 | 0.6886 |
| 196 | V1_conf_gate | mask +body_pct>=0.721523 | 52.0/0.773 | 40.0/0.885 | 0.6834 |
| 817 | V11_broad_gate | mask +signal_range_pct<=0.182476 | 58.0/0.817 | 62.0/0.742 | 0.682 |
| 200 | V1_conf_gate | mask +vwap_dist_atr>=-7.163915 | 107.0/0.663 | 78.0/0.682 | 0.6478 |
| 838 | V11_broad_gate | pm +pre3_range_r<=0.121555 | 52.0/0.7 | 50.0/0.769 | 0.6448 |
| 190 | V1_conf_gate | mask +atr_pct<=0.00285 | 50.0/0.667 | 61.0/0.697 | 0.643 |
| 170 | V1_conf_gate | guard {'max_slot': '13:30'} | 98.0/0.643 | 51.0/0.663 | 0.627 |
| 247 | V1_conf_gate | pm +sig5_vol_ratio20>=1.607392 | 103.0/0.632 | 69.0/0.646 | 0.6208 |
| 223 | V1_conf_gate | pm +sig5_adx_calc>=49.212418 | 49.0/0.737 | 28.0/0.897 | 0.609 |
| 839 | V11_broad_gate | pm +pre3_range_r<=0.304862 | 198.0/0.614 | 174.0/0.599 | 0.587 |
| 243 | V1_conf_gate | pm +sig5_rsi_dir<=66.001616 | 52.0/0.943 | 56.0/0.74 | 0.5776 |
| 177 | V1_conf_gate | mask +vol_ratio>=1.608214 | 103.0/0.632 | 82.0/0.704 | 0.5744 |
| 841 | V11_broad_gate | pm +pre3_range_r>=0.121555 | 283.0/0.57 | 213.0/0.569 | 0.5682 |
| 823 | V11_broad_gate | mask +lower_wick_pct<=0.107385 | 279.0/0.583 | 191.0/0.607 | 0.5638 |
| 171 | V1_conf_gate | guard {'min_slot': '11:30'} | 102.0/0.636 | 88.0/0.736 | 0.556 |
| 192 | V1_conf_gate | mask +atr_pct>=0.001853 | 112.0/0.624 | 80.0/0.71 | 0.5552 |
| 821 | V11_broad_gate | mask +upper_wick_pct>=0.08156 | 122.0/0.65 | 61.0/0.597 | 0.5546 |
| 845 | V11_broad_gate | pm +pre_entry_momentum_score>=66.533269 | 178.0/0.572 | 85.0/0.597 | 0.552 |
| 234 | V1_conf_gate | pm +pre3_range_r<=0.304862 | 63.0/0.658 | 63.0/0.794 | 0.5492 |
| 199 | V1_conf_gate | mask +close_loc<=0.25 | 91.0/0.676 | 71.0/0.835 | 0.5488 |
| 158 | V1_conf_gate | exit 1.1/1.25 | 120.0/0.621 | 94.0/0.712 | 0.5482 |
| 254 | V1_conf_gate | pm +pre10_mom_r<=0.397876 | 92.0/0.652 | 72.0/0.782 | 0.548 |
| 773 | V11_broad_gate | exit 1.5/2.5 | 288.0/0.56 | 230.0/0.576 | 0.5472 |
| 810 | V11_broad_gate | mask +vwap_dist_atr<=-1.981105 | 300.0/0.57 | 219.0/0.556 | 0.5448 |
| 596 | V6_vol_band | pm +pre3_range_r<=0.121555 | 209.0/0.559 | 274.0/0.577 | 0.5446 |
| 195 | V1_conf_gate | mask +body_pct>=0.517246 | 101.0/0.599 | 78.0/0.672 | 0.5406 |

## Best score by knob family (top 25)

| change                                  |   best_score |   n |
|:----------------------------------------|-------------:|----:|
| pm +pre3_range_r<=0.121555              |       0.885  |   8 |
| mask +signal_range_pct<=0.182476        |       0.7532 |   8 |
| mask +vol_ratio<=2.347911               |       0.7246 |   8 |
| mask +close_loc<=0.0                    |       0.6886 |   8 |
| mask +lower_wick_pct<=0.0               |       0.6886 |   8 |
| mask +body_pct>=0.721523                |       0.6834 |   8 |
| mask +vwap_dist_atr>=-7.163915          |       0.6478 |   8 |
| mask +atr_pct<=0.00285                  |       0.643  |   8 |
| guard {'max_slot': '13:30'}             |       0.627  |   8 |
| pm +sig5_vol_ratio20>=1.607392          |       0.6208 |   8 |
| pm +sig5_adx_calc>=49.212418            |       0.609  |   8 |
| pm +pre3_range_r<=0.304862              |       0.587  |   8 |
| pm +sig5_rsi_dir<=66.001616             |       0.5776 |   8 |
| mask +vol_ratio>=1.608214               |       0.5744 |   8 |
| pm +pre3_range_r>=0.121555              |       0.5682 |   8 |
| mask +lower_wick_pct<=0.107385          |       0.5638 |   8 |
| guard {'min_slot': '11:30'}             |       0.556  |   8 |
| mask +atr_pct>=0.001853                 |       0.5552 |   8 |
| mask +upper_wick_pct>=0.08156           |       0.5546 |   8 |
| pm +pre_entry_momentum_score>=66.533269 |       0.552  |   8 |
| mask +close_loc<=0.25                   |       0.5488 |   8 |
| exit 1.1                                |       0.5482 |  56 |
| pm +pre10_mom_r<=0.397876               |       0.548  |   8 |
| exit 1.5                                |       0.5472 |  56 |
| mask +vwap_dist_atr<=-1.981105          |       0.5448 |   8 |
