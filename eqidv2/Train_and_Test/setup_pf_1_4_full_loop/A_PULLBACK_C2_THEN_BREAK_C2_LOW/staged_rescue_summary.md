# A_PULLBACK_C2_THEN_BREAK_C2_LOW - STAGED_RESCUE_SUMMARY

- Engine: deterministic staged 5m-enriched rescue sweeps
- Configs evaluated: 6000
- Mask terms built from FIT: 1638
- Pre-momentum terms built from FIT: 244
- Feature cache rows/status: 8100 / {'ok': 8100}
- Acceptance: TRAIN PF 1.3-1.8, TEST PF > 1.4, positive TRAIN/TEST net, no domination.

## Baseline Reference

- TRAIN: n=238 PF=0.538 net=Rs-77,654 win%=36.6 avgW=Rs1,039 avgL=Rs-1,113 SL/TGT/EOD=103/62/73 tpd=5.41 domT/D/S=0.014/9.99/9.99
- TEST: n=64 PF=0.897 net=Rs-3,443 win%=50.0 avgW=Rs932 avgL=Rs-1,040 SL/TGT/EOD=19/19/26 tpd=4.27 domT/D/S=0.042/9.99/9.99

## Top 25 FIT/VAL Rows

| iter | stage | group | config | FIT | VAL | TRAIN | TEST | keep/reject |
|---|---|---|---|---|---|---|---|---|
| 3453 | single_premom_x_exit_guard | exit+pre_momentum+guard | SL/Tgt=1.2/2.0 mask=[(none)] premom=[pre_entry_momentum_score<=37.084516] guard={"max_slot": "12:30", "top_n": 1} | 6/1.637/Rs909.0 | 17/1.476/Rs3031.0 | 23.0/1.505/Rs3940.0 | 19.0/0.812/Rs-1807.0 | REJECT_TEST_OR_STABILITY |
| 5763 | mask_pair | indicator+price_action | SL/Tgt=1.2/1.5 mask=[body_pct<=0.492005; feat5_close_location>=0.377357] premom=[(none)] guard={} | 5/1.534/Rs771.0 | 10/1.826/Rs2127.0 | 15.0/1.721/Rs2897.0 | nan/nan/Rsnan | REJECT_FULL_TRAIN |
| 3414 | single_premom_x_exit_guard | exit+pre_momentum+guard | SL/Tgt=1.2/1.25 mask=[(none)] premom=[pre_entry_momentum_score<=41.016776] guard={"max_slot": "11:30", "top_n": 1} | 7/1.523/Rs1495.0 | 5/1.428/Rs609.0 | 12.0/1.491/Rs2103.0 | nan/nan/Rsnan | REJECT_FULL_TRAIN |
| 3418 | single_premom_x_exit_guard | pre_momentum+guard | SL/Tgt=1.2/1.5 mask=[(none)] premom=[pre_entry_momentum_score<=41.016776] guard={"max_slot": "11:30", "top_n": 1} | 7/1.477/Rs1365.0 | 5/1.779/Rs1108.0 | 12.0/1.577/Rs2473.0 | nan/nan/Rsnan | REJECT_FULL_TRAIN |
| 3941 | single_premom_x_exit_guard | pre_momentum+guard | SL/Tgt=1.2/1.5 mask=[(none)] premom=[pre5_mom_r<=-0.172632] guard={"max_slot": "12:30", "top_n": 1} | 9/2.007/Rs2072.0 | 21/1.403/Rs3193.0 | 30.0/1.528/Rs5265.0 | 19.0/1.035/Rs280.0 | REJECT_TEST_OR_STABILITY |
| 3458 | single_premom_x_exit_guard | exit+pre_momentum+guard | SL/Tgt=1.2/2.5 mask=[(none)] premom=[pre_entry_momentum_score<=37.084516] guard={"max_slot": "12:30", "top_n": 1} | 6/1.637/Rs909.0 | 17/1.28/Rs1788.0 | 23.0/1.346/Rs2696.0 | 19.0/0.891/Rs-1046.0 | REJECT_TEST_OR_STABILITY |
| 3946 | single_premom_x_exit_guard | exit+pre_momentum+guard | SL/Tgt=1.2/2.0 mask=[(none)] premom=[pre5_mom_r<=-0.172632] guard={"max_slot": "12:30", "top_n": 1} | 9/1.828/Rs1704.0 | 21/1.289/Rs2700.0 | 30.0/1.386/Rs4404.0 | 19.0/1.302/Rs2388.0 | REJECT_TEST_OR_STABILITY |
| 2544 | single_mask_x_exit_guard | exit+price_action+guard | SL/Tgt=1.2/2.0 mask=[body_pct<=0.526309] premom=[(none)] guard={"max_slot": "12:30", "top_n": 1} | 23/1.079/Rs1042.0 | 14/1.078/Rs536.0 | 37.0/1.079/Rs1578.0 | nan/nan/Rsnan | REJECT_FULL_TRAIN |
| 2573 | single_mask_x_exit_guard | exit+indicator+price_action+guard | SL/Tgt=1.2/2.0 mask=[feat5_body_efficiency>=-0.526309] premom=[(none)] guard={"max_slot": "12:30", "top_n": 1} | 23/1.079/Rs1042.0 | 23/1.372/Rs3808.0 | 46.0/1.207/Rs4850.0 | nan/nan/Rsnan | REJECT_FULL_TRAIN |
| 2558 | single_mask_x_exit_guard | exit+price_action+guard | SL/Tgt=0.7/1.25 mask=[body_pct<=0.526309] premom=[(none)] guard={"max_slot": "11:30", "top_n": 1} | 10/1.096/Rs447.0 | 4/1.077/Rs134.0 | 14.0/1.091/Rs581.0 | nan/nan/Rsnan | REJECT_FULL_TRAIN |
| 4343 | mask_pair | exit+indicator+price_action | SL/Tgt=1.0/1.25 mask=[feat5_body_efficiency>=-0.491015; feat5_close_location>=0.377357] premom=[(none)] guard={} | 6/1.025/Rs62.0 | 71/1.071/Rs1869.0 | 77.0/1.067/Rs1931.0 | nan/nan/Rsnan | REJECT_FULL_TRAIN |
| 3448 | single_premom_x_exit_guard | pre_momentum+guard | SL/Tgt=1.2/1.5 mask=[(none)] premom=[pre_entry_momentum_score<=37.084516] guard={"max_slot": "12:30", "top_n": 1} | 6/1.894/Rs1277.0 | 17/1.267/Rs1700.0 | 23.0/1.382/Rs2977.0 | 19.0/0.646/Rs-3408.0 | REJECT_TEST_OR_STABILITY |
| 2351 | single_mask_x_exit_guard | exit+filter+guard | SL/Tgt=1.5/1.5 mask=[rs_pct>=2.433826] premom=[(none)] guard={"max_slot": "12:30", "top_n": 1} | 41/1.041/Rs904.0 | 7/1.153/Rs531.0 | 48.0/1.057/Rs1436.0 | nan/nan/Rsnan | REJECT_FULL_TRAIN |
| 3425 | single_premom_x_exit_guard | exit+pre_momentum+guard | SL/Tgt=1.2/2.0 mask=[(none)] premom=[pre_entry_momentum_score<=41.016776] guard={"top_n": 1} | 34/1.292/Rs3669.0 | 55/1.014/Rs332.0 | 89.0/1.11/Rs4002.0 | nan/nan/Rsnan | REJECT_FULL_TRAIN |
| 3424 | single_premom_x_exit_guard | exit+pre_momentum+guard | SL/Tgt=1.2/2.0 mask=[(none)] premom=[pre_entry_momentum_score<=41.016776] guard={"max_slot": "12:30", "top_n": 1} | 18/1.011/Rs87.0 | 22/1.056/Rs588.0 | 40.0/1.036/Rs675.0 | nan/nan/Rsnan | REJECT_FULL_TRAIN |
| 3409 | single_premom_x_exit_guard | exit+pre_momentum+guard | SL/Tgt=1.2/1.0 mask=[(none)] premom=[pre_entry_momentum_score<=41.016776] guard={"max_slot": "11:30", "top_n": 1} | 7/1.332/Rs951.0 | 5/1.077/Rs110.0 | 12.0/1.248/Rs1060.0 | nan/nan/Rsnan | REJECT_FULL_TRAIN |
| 3444 | single_premom_x_exit_guard | exit+pre_momentum+guard | SL/Tgt=1.2/1.25 mask=[(none)] premom=[pre_entry_momentum_score<=37.084516] guard={"max_slot": "12:30", "top_n": 1} | 6/2.005/Rs1435.0 | 17/1.222/Rs1413.0 | 23.0/1.365/Rs2848.0 | 19.0/0.543/Rs-4400.0 | REJECT_TEST_OR_STABILITY |
| 3477 | single_premom_x_exit_guard | pre_momentum+guard | SL/Tgt=1.2/1.5 mask=[(none)] premom=[pre5_mom_r<=-0.083567] guard={"max_slot": "12:30", "top_n": 1} | 16/1.823/Rs4043.0 | 36/1.084/Rs1211.0 | 52.0/1.271/Rs5254.0 | nan/nan/Rsnan | REJECT_FULL_TRAIN |
| 2331 | single_mask_x_exit_guard | filter+guard | SL/Tgt=1.2/1.5 mask=[rs_pct>=2.433826] premom=[(none)] guard={"max_slot": "12:30", "top_n": 1} | 41/1.024/Rs513.0 | 7/1.395/Rs1131.0 | 48.0/1.067/Rs1644.0 | nan/nan/Rsnan | REJECT_FULL_TRAIN |
| 3434 | single_premom_x_exit_guard | exit+pre_momentum+guard | SL/Tgt=1.2/0.8 mask=[(none)] premom=[pre_entry_momentum_score<=37.084516] guard={"max_slot": "12:30", "top_n": 1} | 6/1.616/Rs879.0 | 17/1.086/Rs495.0 | 23.0/1.191/Rs1374.0 | nan/nan/Rsnan | REJECT_FULL_TRAIN |
| 5771 | mask_pair | exit+indicator+price_action | SL/Tgt=1.2/2.0 mask=[body_pct<=0.492005; feat5_close_location>=0.377357] premom=[(none)] guard={} | 5/1.377/Rs544.0 | 10/2.214/Rs3126.0 | 15.0/1.913/Rs3671.0 | nan/nan/Rsnan | REJECT_FULL_TRAIN |
| 2543 | single_mask_x_exit_guard | exit+price_action+guard | SL/Tgt=1.2/2.0 mask=[body_pct<=0.526309] premom=[(none)] guard={"max_slot": "11:30", "top_n": 1} | 10/2.239/Rs5277.0 | 4/1.28/Rs578.0 | 14.0/1.927/Rs5855.0 | nan/nan/Rsnan | REJECT_FULL_TRAIN |
| 3951 | single_premom_x_exit_guard | exit+pre_momentum+guard | SL/Tgt=1.2/2.5 mask=[(none)] premom=[pre5_mom_r<=-0.172632] guard={"max_slot": "12:30", "top_n": 1} | 9/1.828/Rs1704.0 | 21/1.076/Rs714.0 | 30.0/1.212/Rs2419.0 | nan/nan/Rsnan | REJECT_FULL_TRAIN |
| 2533 | single_mask_x_exit_guard | price_action+guard | SL/Tgt=1.2/1.5 mask=[body_pct<=0.526309] premom=[(none)] guard={"max_slot": "11:30", "top_n": 1} | 10/1.848/Rs3613.0 | 4/1.038/Rs78.0 | 14.0/1.584/Rs3691.0 | nan/nan/Rsnan | REJECT_FULL_TRAIN |
| 3439 | single_premom_x_exit_guard | exit+pre_momentum+guard | SL/Tgt=1.2/1.0 mask=[(none)] premom=[pre_entry_momentum_score<=37.084516] guard={"max_slot": "12:30", "top_n": 1} | 6/2.171/Rs1672.0 | 17/1.131/Rs825.0 | 23.0/1.323/Rs2497.0 | 19.0/0.862/Rs-931.0 | REJECT_TEST_OR_STABILITY |

## Outcome Counts

- {'REJECT_FITVAL': 5800, 'REJECT_FULL_TRAIN': 189, 'REJECT_TEST_OR_STABILITY': 11}

- Full TRAIN-band rows tested on TEST: 11

## Recommendation

- No passing staged candidate. Do not promote this setup from the staged rescue search.

## Rerun

```powershell
python Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_LOW\scripts\staged_rescue_sweeps.py --top_mask_terms 50 --top_pm_terms 25 --max_configs 6000 --min_split_trades 5
```