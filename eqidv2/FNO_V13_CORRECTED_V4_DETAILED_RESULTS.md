# FNO V13 corrected v4 - detailed historical results

## Verdict

Across **25 sessions** (2026-07-29 through 2026-09-03), V13-v4 produced **78 fills**, **70.51% win rate**, **55.13% T1-hit rate**, **2.880 PF**, and **+42.000%** summed net return at 5.0 bps.

This is an experimental shadow selected from the same 25-session history, not a production promotion.

## Frozen V13-v4 exit configuration

1. Keep every V13-v3 entry, NIFTY gate, confirmation rule, picker, and OI cap unchanged.
2. Initial stop: 1.50% from the stop-entry trigger.
3. At +1.05%, book 20% and immediately move the remaining 80% stop to breakeven.
4. Runner target: +2.60%; otherwise square off at the final cached close through 15:30.
5. Initial stop wins a same-minute tie with T1; breakeven wins a same-minute tie with the runner target.
6. The configured round-trip cost is deducted once from the weighted whole-position return.

## V13-v3 versus V13-v4

| metric              |   v13_v3 |   v13_v4 |   delta |
|:--------------------|---------:|---------:|--------:|
| orders              |   79.000 |   79.000 |   0.000 |
| fills               |   78.000 |   78.000 |   0.000 |
| wins                |   44.000 |   55.000 |  11.000 |
| losses              |   34.000 |   23.000 | -11.000 |
| win_rate_pct        |   56.410 |   70.513 |  14.103 |
| target_hits         |   17.000 |   43.000 |  26.000 |
| target_hit_rate_pct |   21.795 |   55.128 |  33.333 |
| profit_factor       |    2.770 |    2.880 |   0.110 |
| net_pct             |   46.034 |   42.000 |  -4.034 |
| expectancy_pct      |    0.590 |    0.538 |  -0.052 |
| max_drawdown_pct    |   -3.041 |   -2.867 |   0.174 |

T1-hit rate means the trade reached +1.05% and booked 20%; it does not mean the full position reached +2.60%.

## Period results

| strategy   | period         |   sessions |   fills |   wins |   win_rate_pct |   target_hits |   target_hit_rate_pct |   profit_factor |   net_pct |   max_drawdown_pct |
|:-----------|:---------------|-----------:|--------:|-------:|---------------:|--------------:|----------------------:|----------------:|----------:|-------------------:|
| V13-v3     | ORIGINAL_TRAIN |         12 |      41 |     24 |         58.537 |            11 |                26.829 |           2.992 |    27.503 |             -1.050 |
| V13-v4     | ORIGINAL_TRAIN |         12 |      41 |     29 |         70.732 |            24 |                58.537 |           2.904 |    24.907 |             -1.550 |
| V13-v3     | ORIGINAL_TEST  |         11 |      35 |     20 |         57.143 |             6 |                17.143 |           2.899 |    20.131 |             -3.041 |
| V13-v4     | ORIGINAL_TEST  |         11 |      35 |     26 |         74.286 |            19 |                54.286 |           3.440 |    18.693 |             -2.867 |
| V13-v3     | SEP02_PLUS     |          2 |       2 |      0 |          0.000 |             0 |                 0.000 |           0.000 |    -1.600 |             -1.600 |
| V13-v4     | SEP02_PLUS     |          2 |       2 |      0 |          0.000 |             0 |                 0.000 |           0.000 |    -1.600 |             -1.600 |
| V13-v3     | ALL            |         25 |      78 |     44 |         56.410 |            17 |                21.795 |           2.770 |    46.034 |             -3.041 |
| V13-v4     | ALL            |         25 |      78 |     55 |         70.513 |            43 |                55.128 |           2.880 |    42.000 |             -2.867 |

## Cost stress

| strategy   |   cost_bps |   fills |   win_rate_pct |   target_hit_rate_pct |   profit_factor |   net_pct |   max_drawdown_pct |
|:-----------|-----------:|--------:|---------------:|----------------------:|----------------:|----------:|-------------------:|
| V13-v3     |      5.000 |      78 |         56.410 |                21.795 |           2.770 |    46.034 |             -3.041 |
| V13-v4     |      5.000 |      78 |         70.513 |                55.128 |           2.880 |    42.000 |             -2.867 |
| V13-v3     |     10.000 |      78 |         55.128 |                21.795 |           2.520 |    42.134 |             -3.441 |
| V13-v4     |     10.000 |      78 |         69.231 |                55.128 |           2.621 |    38.100 |             -3.267 |
| V13-v3     |     15.000 |      78 |         53.846 |                21.795 |           2.297 |    38.234 |             -4.150 |
| V13-v4     |     15.000 |      78 |         67.949 |                55.128 |           2.384 |    34.200 |             -3.711 |
| V13-v3     |     20.000 |      78 |         51.282 |                21.795 |           2.096 |    34.334 |             -5.650 |
| V13-v4     |     20.000 |      78 |         66.667 |                55.128 |           2.166 |    30.300 |             -4.561 |

## Exit breakdown

| exit_reason       |   orders |   fills |   wins |   losses |   net_pct |
|:------------------|---------:|--------:|-------:|---------:|----------:|
| EOD_NO_T1         |       26 |      26 |     12 |       14 |    -4.851 |
| FULL_STOP         |        9 |       9 |      0 |        9 |   -13.950 |
| RUNNER_TARGET     |       19 |      19 |     19 |        0 |    42.560 |
| T1_THEN_BREAKEVEN |       11 |      11 |     11 |        0 |     1.760 |
| T1_THEN_EOD       |       13 |      13 |     13 |        0 |    16.481 |
| UNFILLED          |        1 |       0 |      0 |        0 |     0.000 |

## Per-setup results

| setup_id   |   signal_end | side   |   orders |   fills |   wins |   losses |   win_rate_pct |   t1_hits |   t1_hit_rate_pct |   profit_factor |   net_pct |
|:-----------|-------------:|:-------|---------:|--------:|-------:|---------:|---------------:|----------:|------------------:|----------------:|----------:|
| 0926_LONG  |          925 | LONG   |       11 |      11 |      8 |        3 |         72.727 |         8 |            72.727 |           5.928 |     8.428 |
| 0926_SHORT |          925 | SHORT  |       12 |      12 |     11 |        1 |         91.667 |         6 |            50.000 |          10.511 |     7.877 |
| 0931_LONG  |          930 | LONG   |        7 |       7 |      5 |        2 |         71.429 |         5 |            71.429 |           5.921 |     8.826 |
| 0931_SHORT |          930 | SHORT  |        5 |       5 |      5 |        0 |        100.000 |         2 |            40.000 |         inf     |     3.171 |
| 0936_LONG  |          935 | LONG   |        9 |       9 |      6 |        3 |         66.667 |         5 |            55.556 |           1.395 |     1.350 |
| 0941_LONG  |          940 | LONG   |        8 |       8 |      4 |        4 |         50.000 |         4 |            50.000 |           1.153 |     0.636 |
| 0941_SHORT |          940 | SHORT  |        7 |       7 |      4 |        3 |         57.143 |         3 |            42.857 |           0.945 |    -0.257 |
| 0946_LONG  |          945 | LONG   |        1 |       1 |      0 |        1 |          0.000 |         0 |             0.000 |           0.000 |    -0.643 |
| 0956_LONG  |          955 | LONG   |       10 |      10 |      7 |        3 |         70.000 |         5 |            50.000 |           3.391 |     6.695 |
| 1001_LONG  |         1000 | LONG   |        9 |       8 |      5 |        3 |         62.500 |         5 |            62.500 |           3.530 |     5.920 |

## Day-wise V13-v3 versus V13-v4

| day        | contract_month   |   nifty_first_bar_return_pct |   v13_v3_fills |   v13_v3_wins |   v13_v3_target_hits |   v13_v3_net_pct |   v13_v4_fills |   v13_v4_wins |   v13_v4_target_hits |   v13_v4_net_pct |   delta_net_pct |   v13_v4_cumulative_net_pct |
|:-----------|:-----------------|-----------------------------:|---------------:|--------------:|---------------------:|-----------------:|---------------:|--------------:|---------------------:|-----------------:|----------------:|----------------------------:|
| 2026-07-29 | 26AUG            |                        0.165 |              5 |             3 |                    2 |            4.879 |              5 |             4 |                    3 |            4.669 |          -0.210 |                       4.669 |
| 2026-07-30 | 26AUG            |                        0.183 |              2 |             2 |                    1 |            2.976 |              2 |             2 |                    1 |            2.766 |          -0.210 |                       7.435 |
| 2026-07-31 | 26AUG            |                        0.070 |              3 |             3 |                    0 |            3.142 |              3 |             3 |                    3 |            4.440 |           1.299 |                      11.875 |
| 2026-08-03 | 26AUG            |                        0.067 |              6 |             2 |                    2 |            1.200 |              6 |             4 |                    4 |            1.700 |           0.500 |                      13.575 |
| 2026-08-04 | 26AUG            |                       -0.084 |              3 |             2 |                    0 |            1.315 |              3 |             2 |                    1 |            0.763 |          -0.552 |                      14.338 |
| 2026-08-05 | 26AUG            |                        0.017 |              0 |             0 |                    0 |            0.000 |              0 |             0 |                    0 |            0.000 |           0.000 |                      14.338 |
| 2026-08-06 | 26AUG            |                        0.049 |              5 |             3 |                    1 |            3.556 |              5 |             4 |                    4 |            4.157 |           0.600 |                      18.495 |
| 2026-08-07 | 26AUG            |                        0.012 |              2 |             1 |                    1 |            1.900 |              2 |             1 |                    1 |            1.182 |          -0.718 |                      19.676 |
| 2026-08-10 | 26AUG            |                        0.020 |              3 |             3 |                    3 |            8.850 |              3 |             3 |                    3 |            6.720 |          -2.130 |                      26.396 |
| 2026-08-11 | 26AUG            |                       -0.179 |              4 |             1 |                    1 |            0.300 |              4 |             2 |                    2 |           -0.700 |          -1.000 |                      25.696 |
| 2026-08-12 | 26AUG            |                       -0.236 |              7 |             4 |                    0 |            0.435 |              7 |             4 |                    2 |            0.761 |           0.325 |                      26.457 |
| 2026-08-13 | 26AUG            |                       -0.119 |              1 |             0 |                    0 |           -1.050 |              1 |             0 |                    0 |           -1.550 |          -0.500 |                      24.907 |
| 2026-08-14 | 26AUG            |                       -0.234 |              3 |             1 |                    0 |           -0.016 |              3 |             1 |                    1 |           -0.155 |          -0.139 |                      24.752 |
| 2026-08-17 | 26AUG            |                       -0.211 |              2 |             0 |                    0 |           -1.350 |              2 |             1 |                    0 |           -0.367 |           0.983 |                      24.385 |
| 2026-08-18 | 26AUG            |                       -0.060 |              3 |             2 |                    1 |            1.695 |              3 |             3 |                    1 |            2.984 |           1.290 |                      27.370 |
| 2026-08-19 | 26AUG            |                       -0.152 |              2 |             2 |                    0 |            1.878 |              2 |             2 |                    2 |            1.902 |           0.024 |                      29.272 |
| 2026-08-20 | 26AUG            |                        0.038 |              3 |             1 |                    0 |           -1.513 |              3 |             1 |                    0 |           -2.335 |          -0.822 |                      26.937 |
| 2026-08-21 | 26AUG            |                       -0.105 |              5 |             2 |                    0 |           -1.529 |              5 |             3 |                    0 |           -0.532 |           0.996 |                      26.405 |
| 2026-08-26 | 26SEP            |                       -0.139 |              3 |             3 |                    0 |            4.755 |              3 |             3 |                    3 |            4.404 |          -0.351 |                      30.809 |
| 2026-08-27 | 26SEP            |                        0.002 |              3 |             1 |                    1 |            0.850 |              3 |             2 |                    2 |            2.008 |           1.158 |                      32.817 |
| 2026-08-28 | 26SEP            |                       -0.017 |              4 |             3 |                    0 |            4.223 |              4 |             4 |                    4 |            5.360 |           1.137 |                      38.177 |
| 2026-08-31 | 26SEP            |                       -0.163 |              4 |             2 |                    2 |            4.615 |              4 |             3 |                    3 |            2.325 |          -2.290 |                      40.502 |
| 2026-09-01 | 26SEP            |                       -0.097 |              3 |             3 |                    2 |            6.522 |              3 |             3 |                    3 |            3.098 |          -3.424 |                      43.600 |
| 2026-09-02 | 26SEP            |                       -0.030 |              1 |             0 |                    0 |           -1.050 |              1 |             0 |                    0 |           -1.550 |          -0.500 |                      42.050 |
| 2026-09-03 | 26SEP            |                        0.028 |              1 |             0 |                    0 |           -0.550 |              1 |             0 |                    0 |           -0.050 |           0.500 |                      42.000 |

## Complete V13-v4 order ledger

| day        |   hhmm_int | tradingsymbol   | side   | setup_id   | filled   | t1_hit   | exit_reason       |   net_return_pct |   price_change_pct |   oi_change_pct |   volume_ratio |   body_ratio |   nifty_first_bar_return_pct |
|:-----------|-----------:|:----------------|:-------|:-----------|:---------|:---------|:------------------|-----------------:|-------------------:|----------------:|---------------:|-------------:|-----------------------------:|
| 2026-07-29 |        925 | UNITDSPR        | LONG   | 0926_LONG  | True     | True     | T1_THEN_BREAKEVEN |            0.160 |              0.330 |           0.113 |          4.588 |        0.643 |                        0.165 |
| 2026-07-29 |        930 | SWIGGY          | LONG   | 0931_LONG  | True     | True     | RUNNER_TARGET     |            2.240 |              0.659 |           0.215 |          2.151 |        0.888 |                        0.165 |
| 2026-07-29 |        935 | LAURUSLABS      | LONG   | 0936_LONG  | True     | False    | EOD_NO_T1         |           -0.314 |              0.684 |           0.307 |          3.920 |        0.632 |                        0.165 |
| 2026-07-29 |        955 | JSWSTEEL        | LONG   | 0956_LONG  | True     | False    | EOD_NO_T1         |            0.343 |              0.652 |           0.167 |          2.394 |        0.917 |                        0.165 |
| 2026-07-29 |       1000 | KAYNES          | LONG   | 1001_LONG  | True     | True     | RUNNER_TARGET     |            2.240 |              0.857 |           0.262 |          3.120 |        0.565 |                        0.165 |
| 2026-07-30 |        930 | KALYANKJIL      | LONG   | 0931_LONG  | True     | True     | RUNNER_TARGET     |            2.240 |              0.685 |           0.206 |          1.989 |        0.778 |                        0.183 |
| 2026-07-30 |        940 | GODREJPROP      | SHORT  | 0941_SHORT | True     | False    | EOD_NO_T1         |            0.526 |             -0.563 |           0.155 |          1.773 |        0.441 |                        0.183 |
| 2026-07-31 |        930 | BSE             | LONG   | 0931_LONG  | True     | True     | T1_THEN_EOD       |            1.659 |              1.063 |           0.448 |          4.441 |        0.578 |                        0.070 |
| 2026-07-31 |        935 | LAURUSLABS      | LONG   | 0936_LONG  | True     | True     | T1_THEN_EOD       |            0.541 |              0.222 |           0.165 |          1.226 |        0.842 |                        0.070 |
| 2026-07-31 |        955 | KAYNES          | LONG   | 0956_LONG  | True     | True     | RUNNER_TARGET     |            2.240 |              0.863 |           0.255 |          2.866 |        0.537 |                        0.070 |
| 2026-08-03 |        925 | ABCAPITAL       | LONG   | 0926_LONG  | True     | True     | T1_THEN_BREAKEVEN |            0.160 |              0.596 |           0.523 |          3.593 |        0.870 |                        0.067 |
| 2026-08-03 |        930 | PAYTM           | LONG   | 0931_LONG  | True     | True     | RUNNER_TARGET     |            2.240 |              2.006 |           0.680 |          4.399 |        0.521 |                        0.067 |
| 2026-08-03 |        935 | FORTIS          | LONG   | 0936_LONG  | True     | True     | T1_THEN_BREAKEVEN |            0.160 |              0.504 |           0.297 |          3.251 |        0.735 |                        0.067 |
| 2026-08-03 |        940 | PAYTM           | LONG   | 0941_LONG  | True     | True     | RUNNER_TARGET     |            2.240 |              0.598 |           0.807 |          3.425 |        0.700 |                        0.067 |
| 2026-08-03 |        955 | INDIANB         | LONG   | 0956_LONG  | True     | False    | FULL_STOP         |           -1.550 |              0.206 |           0.244 |          1.822 |        0.667 |                        0.067 |
| 2026-08-03 |       1000 | GODFRYPHLP      | LONG   | 1001_LONG  | True     | False    | FULL_STOP         |           -1.550 |              1.068 |           0.666 |          4.789 |        0.830 |                        0.067 |
| 2026-08-04 |        925 | PAGEIND         | SHORT  | 0926_SHORT | True     | False    | EOD_NO_T1         |            0.200 |             -0.249 |           0.172 |          4.707 |        1.000 |                       -0.084 |
| 2026-08-04 |        930 | UPL             | SHORT  | 0931_SHORT | True     | True     | T1_THEN_EOD       |            1.532 |             -0.394 |           0.285 |          3.680 |        0.692 |                       -0.084 |
| 2026-08-04 |        940 | ABB             | LONG   | 0941_LONG  | True     | False    | EOD_NO_T1         |           -0.969 |              0.957 |           0.620 |          2.302 |        0.690 |                       -0.084 |
| 2026-08-06 |        925 | HAL             | LONG   | 0926_LONG  | True     | True     | RUNNER_TARGET     |            2.240 |              1.058 |           0.147 |         14.657 |        0.646 |                        0.049 |
| 2026-08-06 |        930 | SWIGGY          | LONG   | 0931_LONG  | True     | True     | RUNNER_TARGET     |            2.240 |              0.820 |           0.241 |          3.976 |        1.000 |                        0.049 |
| 2026-08-06 |        935 | SHRIRAMFIN      | LONG   | 0936_LONG  | True     | True     | T1_THEN_BREAKEVEN |            0.160 |              0.311 |           0.438 |          1.420 |        0.833 |                        0.049 |
| 2026-08-06 |        940 | SHRIRAMFIN      | LONG   | 0941_LONG  | True     | True     | T1_THEN_BREAKEVEN |            0.160 |              0.638 |           0.639 |          3.189 |        1.000 |                        0.049 |
| 2026-08-06 |        945 | SHRIRAMFIN      | LONG   | 0946_LONG  | True     | False    | EOD_NO_T1         |           -0.643 |              0.766 |           0.653 |          2.138 |        0.579 |                        0.049 |
| 2026-08-07 |        955 | ADANIPOWER      | LONG   | 0956_LONG  | True     | False    | EOD_NO_T1         |           -1.058 |              0.491 |           0.119 |          3.235 |        0.848 |                        0.012 |
| 2026-08-07 |       1000 | MOTHERSON       | LONG   | 1001_LONG  | True     | True     | RUNNER_TARGET     |            2.240 |              0.422 |           0.420 |          2.312 |        0.739 |                        0.012 |
| 2026-08-10 |        925 | PAYTM           | LONG   | 0926_LONG  | True     | True     | RUNNER_TARGET     |            2.240 |              0.448 |           0.412 |          3.735 |        0.636 |                        0.020 |
| 2026-08-10 |        940 | PFC             | SHORT  | 0941_SHORT | True     | True     | RUNNER_TARGET     |            2.240 |             -0.259 |           0.122 |          3.193 |        0.941 |                        0.020 |
| 2026-08-10 |        955 | PAYTM           | LONG   | 0956_LONG  | True     | True     | RUNNER_TARGET     |            2.240 |              0.374 |           0.297 |          3.188 |        0.667 |                        0.020 |
| 2026-08-11 |        925 | OIL             | LONG   | 0926_LONG  | True     | True     | T1_THEN_BREAKEVEN |            0.160 |              0.423 |           0.120 |          3.387 |        0.803 |                       -0.179 |
| 2026-08-11 |        935 | HEROMOTOCO      | LONG   | 0936_LONG  | True     | False    | FULL_STOP         |           -1.550 |              0.663 |           0.254 |          3.091 |        0.857 |                       -0.179 |
| 2026-08-11 |        940 | SOLARINDS       | SHORT  | 0941_SHORT | True     | False    | FULL_STOP         |           -1.550 |             -0.468 |           0.449 |          3.943 |        0.690 |                       -0.179 |
| 2026-08-11 |       1000 | NAUKRI          | LONG   | 1001_LONG  | True     | True     | RUNNER_TARGET     |            2.240 |              0.465 |           0.278 |          1.119 |        0.865 |                       -0.179 |
| 2026-08-12 |        925 | PNB             | LONG   | 0926_LONG  | True     | True     | RUNNER_TARGET     |            2.240 |              0.837 |           0.105 |          7.233 |        0.793 |                       -0.236 |
| 2026-08-12 |        925 | AMBER           | SHORT  | 0926_SHORT | True     | False    | EOD_NO_T1         |            0.456 |             -0.249 |           0.272 |          2.496 |        0.500 |                       -0.236 |
| 2026-08-12 |        930 | BOSCHLTD        | LONG   | 0931_LONG  | True     | False    | EOD_NO_T1         |           -0.244 |              1.321 |           0.385 |          4.163 |        0.680 |                       -0.236 |
| 2026-08-12 |        930 | APOLLOHOSP      | SHORT  | 0931_SHORT | True     | False    | EOD_NO_T1         |            0.253 |             -0.382 |           0.454 |          3.051 |        0.517 |                       -0.236 |
| 2026-08-12 |        940 | PNB             | LONG   | 0941_LONG  | True     | True     | T1_THEN_BREAKEVEN |            0.160 |              0.274 |           0.099 |          4.698 |        0.870 |                       -0.236 |
| 2026-08-12 |        940 | RECLTD          | SHORT  | 0941_SHORT | True     | False    | FULL_STOP         |           -1.550 |             -0.235 |           0.485 |          2.103 |        0.600 |                       -0.236 |
| 2026-08-12 |       1000 | PNB             | LONG   | 1001_LONG  | True     | False    | EOD_NO_T1         |           -0.555 |              0.415 |           0.095 |          3.862 |        0.796 |                       -0.236 |
| 2026-08-13 |        935 | JIOFIN          | LONG   | 0936_LONG  | True     | False    | FULL_STOP         |           -1.550 |              0.369 |           0.480 |          1.883 |        0.714 |                       -0.119 |
| 2026-08-14 |        940 | CIPLA           | LONG   | 0941_LONG  | True     | False    | EOD_NO_T1         |           -1.431 |              0.225 |           0.326 |          2.470 |        1.000 |                       -0.234 |
| 2026-08-14 |        940 | AUROPHARMA      | SHORT  | 0941_SHORT | True     | True     | T1_THEN_EOD       |            1.467 |             -0.300 |           0.430 |          3.522 |        0.634 |                       -0.234 |
| 2026-08-14 |        955 | PETRONET        | LONG   | 0956_LONG  | True     | False    | EOD_NO_T1         |           -0.192 |              0.213 |           0.103 |          2.377 |        0.875 |                       -0.234 |
| 2026-08-17 |        925 | AMBER           | LONG   | 0926_LONG  | True     | False    | EOD_NO_T1         |           -0.788 |              0.357 |           0.502 |          3.132 |        0.625 |                       -0.211 |
| 2026-08-17 |        925 | COCHINSHIP      | SHORT  | 0926_SHORT | True     | False    | EOD_NO_T1         |            0.421 |             -0.690 |           0.324 |          2.258 |        1.000 |                       -0.211 |
| 2026-08-18 |        925 | PAYTM           | SHORT  | 0926_SHORT | True     | False    | EOD_NO_T1         |            0.700 |             -0.623 |           0.686 |          4.279 |        0.718 |                       -0.060 |
| 2026-08-18 |        930 | COLPAL          | SHORT  | 0931_SHORT | True     | False    | EOD_NO_T1         |            0.045 |             -0.551 |           0.491 |          4.261 |        0.770 |                       -0.060 |
| 2026-08-18 |        940 | TIINDIA         | LONG   | 0941_LONG  | True     | True     | RUNNER_TARGET     |            2.240 |              1.113 |           0.131 |          5.057 |        0.569 |                       -0.060 |
| 2026-08-19 |        925 | COLPAL          | SHORT  | 0926_SHORT | True     | True     | T1_THEN_EOD       |            1.613 |             -0.282 |           0.521 |          1.987 |        0.812 |                       -0.152 |
| 2026-08-19 |        925 | SOLARINDS       | SHORT  | 0926_SHORT | True     | True     | T1_THEN_EOD       |            0.289 |             -1.006 |           0.221 |          3.738 |        0.750 |                       -0.152 |
| 2026-08-20 |        925 | SHRIRAMFIN      | LONG   | 0926_LONG  | True     | False    | EOD_NO_T1         |           -0.872 |              0.623 |           0.301 |          5.219 |        0.667 |                        0.038 |
| 2026-08-20 |        930 | PNBHOUSING      | LONG   | 0931_LONG  | True     | False    | FULL_STOP         |           -1.550 |              0.849 |           0.648 |          2.864 |        0.727 |                        0.038 |
| 2026-08-20 |        935 | ETERNAL         | LONG   | 0936_LONG  | True     | False    | EOD_NO_T1         |            0.087 |              0.539 |           0.789 |          4.543 |        0.889 |                        0.038 |
| 2026-08-21 |        925 | INDIANB         | SHORT  | 0926_SHORT | True     | False    | EOD_NO_T1         |            0.225 |             -0.489 |           0.296 |          1.608 |        0.667 |                       -0.105 |
| 2026-08-21 |        925 | PNB             | SHORT  | 0926_SHORT | True     | False    | EOD_NO_T1         |           -0.828 |             -0.506 |           0.192 |          4.247 |        0.761 |                       -0.105 |
| 2026-08-21 |        930 | BRITANNIA       | SHORT  | 0931_SHORT | True     | False    | EOD_NO_T1         |            0.127 |             -0.278 |           0.474 |          5.542 |        0.556 |                       -0.105 |
| 2026-08-21 |        940 | MUTHOOTFIN      | LONG   | 0941_LONG  | True     | False    | EOD_NO_T1         |           -0.214 |              0.303 |           0.118 |          2.200 |        0.533 |                       -0.105 |
| 2026-08-21 |        955 | PAYTM           | LONG   | 0956_LONG  | True     | False    | EOD_NO_T1         |            0.159 |              0.296 |           0.230 |          1.065 |        0.696 |                       -0.105 |
| 2026-08-26 |        935 | HINDZINC        | LONG   | 0936_LONG  | True     | True     | T1_THEN_EOD       |            1.575 |              0.496 |           0.276 |          6.172 |        0.895 |                       -0.139 |
| 2026-08-26 |        955 | HINDZINC        | LONG   | 0956_LONG  | True     | True     | T1_THEN_EOD       |            1.450 |              0.446 |           0.341 |          1.145 |        0.800 |                       -0.139 |
| 2026-08-26 |       1000 | KOTAKBANK       | LONG   | 1001_LONG  | True     | True     | T1_THEN_EOD       |            1.379 |              0.441 |           0.066 |          1.467 |        0.583 |                       -0.139 |
| 2026-08-27 |        935 | ADANIPOWER      | LONG   | 0936_LONG  | True     | True     | RUNNER_TARGET     |            2.240 |              0.446 |           0.846 |          5.538 |        0.788 |                        0.002 |
| 2026-08-27 |        940 | AMBER           | LONG   | 0941_LONG  | True     | False    | FULL_STOP         |           -1.550 |              0.777 |           0.884 |          8.719 |        0.781 |                        0.002 |
| 2026-08-27 |        955 | KALYANKJIL      | LONG   | 0956_LONG  | True     | True     | T1_THEN_EOD       |            1.318 |              0.210 |           0.385 |          3.558 |        0.533 |                        0.002 |
| 2026-08-28 |        925 | COFORGE         | LONG   | 0926_LONG  | True     | True     | RUNNER_TARGET     |            2.240 |              0.565 |           0.624 |          6.082 |        0.933 |                       -0.017 |
| 2026-08-28 |        930 | SHREECEM        | SHORT  | 0931_SHORT | True     | True     | T1_THEN_EOD       |            1.215 |             -0.308 |           0.511 |          1.029 |        0.667 |                       -0.017 |
| 2026-08-28 |        955 | SAGILITY        | LONG   | 0956_LONG  | True     | True     | T1_THEN_EOD       |            1.745 |              0.421 |           0.747 |          5.200 |        0.727 |                       -0.017 |
| 2026-08-28 |       1000 | SAGILITY        | LONG   | 1001_LONG  | True     | True     | T1_THEN_BREAKEVEN |            0.160 |              0.507 |           0.902 |          2.361 |        0.455 |                       -0.017 |
| 2026-08-31 |        925 | ADANIENT        | SHORT  | 0926_SHORT | True     | True     | RUNNER_TARGET     |            2.240 |             -0.245 |           0.218 |          5.443 |        0.645 |                       -0.163 |
| 2026-08-31 |        925 | KAYNES          | SHORT  | 0926_SHORT | True     | True     | T1_THEN_BREAKEVEN |            0.160 |             -0.807 |           0.910 |          5.977 |        0.916 |                       -0.163 |
| 2026-08-31 |        940 | PRESTIGE        | SHORT  | 0941_SHORT | True     | True     | T1_THEN_BREAKEVEN |            0.160 |             -0.431 |           0.184 |          1.016 |        0.643 |                       -0.163 |
| 2026-08-31 |       1000 | TVSMOTOR        | LONG   | 1001_LONG  | True     | False    | EOD_NO_T1         |           -0.235 |              0.453 |           0.120 |          1.373 |        0.407 |                       -0.163 |
| 2026-09-01 |        925 | RELIANCE        | LONG   | 0926_LONG  | True     | True     | T1_THEN_EOD       |            0.698 |              0.522 |           0.189 |          3.045 |        0.767 |                       -0.097 |
| 2026-09-01 |        925 | ASHOKLEY        | SHORT  | 0926_SHORT | True     | True     | T1_THEN_BREAKEVEN |            0.160 |             -0.876 |           0.174 |          1.766 |        0.558 |                       -0.097 |
| 2026-09-01 |        925 | HYUNDAI         | SHORT  | 0926_SHORT | True     | True     | RUNNER_TARGET     |            2.240 |             -0.621 |           0.448 |          3.041 |        0.649 |                       -0.097 |
| 2026-09-02 |        940 | MANAPPURAM      | SHORT  | 0941_SHORT | True     | False    | FULL_STOP         |           -1.550 |             -0.224 |           0.117 |          1.092 |        0.857 |                       -0.030 |
| 2026-09-03 |        925 | SOLARINDS       | LONG   | 0926_LONG  | True     | False    | EOD_NO_T1         |           -0.050 |              0.802 |           0.600 |          8.356 |        0.667 |                        0.028 |
| 2026-09-03 |       1000 | SIEMENS         | LONG   | 1001_LONG  | False    | False    | UNFILLED          |          nan     |              0.406 |           0.196 |          4.549 |        0.544 |                        0.028 |

## Interpretation

- Returns are summed filled-trade percentage returns, not a capital-constrained or lot-sized portfolio simulation.
- T1 frequency varies materially by contract regime; the observed result is not yet statistically established above 50%.
- Partial exits add operational complexity and may incur more real slippage than the single round-trip cost model captures.
- Freeze this configuration and forward-test it across at least two new expiry regimes before considering promotion.
