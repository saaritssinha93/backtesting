# Parameter Sweep Summary - A_PULLBACK_C2_THEN_BREAK_C2_HIGH

## baseline
- tested iterations: 1
- baseline_raw: TRAIN trades=2646, wins=835, losses=1811, win_rate=31.56%, PF=0.3398, net=Rs -996,356, avg_win=Rs 614, avg_loss=Rs -833, SL/TGT/EOD=1495/739/412, top_trade/day/symbol=0.0013/None/None; TEST not run; reason train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- rejected ranges/classes: {'train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0': 1}

## combination
- tested iterations: 49
- combo_atr_pct_market_abs_ret_pct_sl1.1_t2.5: TRAIN trades=1423, wins=401, losses=1022, win_rate=28.18%, PF=0.4484, net=Rs -577,830, avg_win=Rs 1,172, avg_loss=Rs -1,025, SL/TGT/EOD=652/131/640, top_trade/day/symbol=0.0048/None/None; TEST not run; reason train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- combo_atr_pct_rs_pct_sl1.1_t2.5: TRAIN trades=1553, wins=435, losses=1118, win_rate=28.01%, PF=0.4298, net=Rs -654,336, avg_win=Rs 1,134, avg_loss=Rs -1,026, SL/TGT/EOD=713/132/708, top_trade/day/symbol=0.0046/None/None; TEST not run; reason train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- combo_atr_pct_rs_pct_sl0.85_t2.0: TRAIN trades=1846, wins=492, losses=1354, win_rate=26.65%, PF=0.4337, net=Rs -700,320, avg_win=Rs 1,090, avg_loss=Rs -913, SL/TGT/EOD=1027/223/596, top_trade/day/symbol=0.0033/None/None; TEST not run; reason train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- combo_atr_pct_notional_sl1.1_t2.5: TRAIN trades=1557, wins=422, losses=1135, win_rate=27.1%, PF=0.415, net=Rs -689,466, avg_win=Rs 1,159, avg_loss=Rs -1,038, SL/TGT/EOD=727/132/698, top_trade/day/symbol=0.0046/None/None; TEST not run; reason train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- combo_atr_pct_rs_pct_sl0.85_t1.8: TRAIN trades=1894, wins=516, losses=1378, win_rate=27.24%, PF=0.4342, net=Rs -714,112, avg_win=Rs 1,062, avg_loss=Rs -916, SL/TGT/EOD=1046/275/573, top_trade/day/symbol=0.0029/None/None; TEST not run; reason train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- rejected ranges/classes: {'train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0': 49}

## exit
- tested iterations: 36
- exit_sl1.2_tgt2.0: TRAIN trades=1554, wins=473, losses=1081, win_rate=30.44%, PF=0.4442, net=Rs -627,509, avg_win=Rs 1,060, avg_loss=Rs -1,044, SL/TGT/EOD=624/206/724, top_trade/day/symbol=0.0035/None/None; TEST not run; reason train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- exit_sl1.1_tgt2.0: TRAIN trades=1609, wins=474, losses=1135, win_rate=29.46%, PF=0.4435, net=Rs -632,418, avg_win=Rs 1,063, avg_loss=Rs -1,001, SL/TGT/EOD=693/207/709, top_trade/day/symbol=0.0035/None/None; TEST not run; reason train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- exit_sl1.2_tgt2.5: TRAIN trades=1491, wins=427, losses=1064, win_rate=28.64%, PF=0.4361, net=Rs -626,536, avg_win=Rs 1,135, avg_loss=Rs -1,044, SL/TGT/EOD=613/132/746, top_trade/day/symbol=0.0047/None/None; TEST not run; reason train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- exit_sl0.85_tgt1.8: TRAIN trades=1933, wins=532, losses=1401, win_rate=27.52%, PF=0.4342, net=Rs -715,661, avg_win=Rs 1,032, avg_loss=Rs -903, SL/TGT/EOD=1042/269/622, top_trade/day/symbol=0.0029/None/None; TEST not run; reason train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- exit_sl1.4_tgt2.0: TRAIN trades=1430, wins=451, losses=979, win_rate=31.54%, PF=0.4342, net=Rs -610,210, avg_win=Rs 1,039, avg_loss=Rs -1,102, SL/TGT/EOD=481/189/760, top_trade/day/symbol=0.0038/None/None; TEST not run; reason train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- rejected ranges/classes: {'train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0': 36}

## filter
- tested iterations: 16
- filter_market_ret_pct<=-0.21279: TRAIN trades=507, wins=196, losses=311, win_rate=38.66%, PF=0.4716, net=Rs -135,219, avg_win=Rs 616, avg_loss=Rs -823, SL/TGT/EOD=260/174/73, top_trade/day/symbol=0.0055/None/None; TEST not run; reason train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- filter_market_ret_pct<=-0.029546: TRAIN trades=942, wins=345, losses=597, win_rate=36.62%, PF=0.4398, net=Rs -268,079, avg_win=Rs 610, avg_loss=Rs -802, SL/TGT/EOD=468/301/173, top_trade/day/symbol=0.0032/None/None; TEST not run; reason train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- filter_signal_minute<=720.0: TRAIN trades=1047, wins=340, losses=707, win_rate=32.47%, PF=0.3447, net=Rs -403,908, avg_win=Rs 625, avg_loss=Rs -872, SL/TGT/EOD=639/315/93, top_trade/day/symbol=0.0031/None/None; TEST not run; reason train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- filter_signal_minute<=700.0: TRAIN trades=750, wins=242, losses=508, win_rate=32.27%, PF=0.3392, net=Rs -294,483, avg_win=Rs 625, avg_loss=Rs -877, SL/TGT/EOD=464/224/62, top_trade/day/symbol=0.0044/None/None; TEST not run; reason train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- filter_market_abs_ret_pct>=0.132184: TRAIN trades=2074, wins=655, losses=1419, win_rate=31.58%, PF=0.3437, net=Rs -774,655, avg_win=Rs 619, avg_loss=Rs -832, SL/TGT/EOD=1173/587/314, top_trade/day/symbol=0.0016/None/None; TEST not run; reason train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- rejected ranges/classes: {'train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0': 16}

## guard
- tested iterations: 17
- guard_daily_loss_5000: TRAIN trades=1468, wins=492, losses=976, win_rate=33.51%, PF=0.3649, net=Rs -527,267, avg_win=Rs 616, avg_loss=Rs -851, SL/TGT/EOD=847/441/180, top_trade/day/symbol=0.0022/None/None; TEST not run; reason train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- guard_daily_loss_3000: TRAIN trades=1230, wins=407, losses=823, win_rate=33.09%, PF=0.3577, net=Rs -451,070, avg_win=Rs 617, avg_loss=Rs -853, SL/TGT/EOD=719/366/145, top_trade/day/symbol=0.0027/None/None; TEST not run; reason train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- guard_max_slot12:00: TRAIN trades=1101, wins=375, losses=726, win_rate=34.06%, PF=0.3693, net=Rs -399,509, avg_win=Rs 624, avg_loss=Rs -873, SL/TGT/EOD=655/346/100, top_trade/day/symbol=0.0029/None/None; TEST not run; reason train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- guard_max_slot12:30: TRAIN trades=1491, wins=496, losses=995, win_rate=33.27%, PF=0.3616, net=Rs -546,221, avg_win=Rs 624, avg_loss=Rs -860, SL/TGT/EOD=876/454/161, top_trade/day/symbol=0.0022/None/None; TEST not run; reason train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- guard_daily_loss_7500: TRAIN trades=1722, wins=561, losses=1161, win_rate=32.58%, PF=0.35, net=Rs -641,157, avg_win=Rs 615, avg_loss=Rs -850, SL/TGT/EOD=1003/502/217, top_trade/day/symbol=0.0019/None/None; TEST not run; reason train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- rejected ranges/classes: {'train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0': 17}

## indicator
- tested iterations: 16
- indicator_rs_pct<=-1.287037: TRAIN trades=620, wins=212, losses=408, win_rate=34.19%, PF=0.3767, net=Rs -209,380, avg_win=Rs 597, avg_loss=Rs -823, SL/TGT/EOD=330/179/111, top_trade/day/symbol=0.0053/None/None; TEST not run; reason train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- indicator_rs_pct>=-1.287037: TRAIN trades=2532, wins=796, losses=1736, win_rate=31.44%, PF=0.3382, net=Rs -950,945, avg_win=Rs 611, avg_loss=Rs -828, SL/TGT/EOD=1416/699/417, top_trade/day/symbol=0.0014/None/None; TEST not run; reason train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- indicator_rs_pct>=-0.863904: TRAIN trades=2469, wins=769, losses=1700, win_rate=31.15%, PF=0.3359, net=Rs -929,779, avg_win=Rs 612, avg_loss=Rs -824, SL/TGT/EOD=1371/678/420, top_trade/day/symbol=0.0014/None/None; TEST not run; reason train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- indicator_atr_pct>=0.002228: TRAIN trades=2582, wins=825, losses=1757, win_rate=31.95%, PF=0.3435, net=Rs -975,928, avg_win=Rs 619, avg_loss=Rs -846, SL/TGT/EOD=1495/744/343, top_trade/day/symbol=0.0013/None/None; TEST not run; reason train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- indicator_quality_score>=29.113851: TRAIN trades=2544, wins=786, losses=1758, win_rate=30.9%, PF=0.326, net=Rs -989,387, avg_win=Rs 609, avg_loss=Rs -835, SL/TGT/EOD=1460/689/395, top_trade/day/symbol=0.0014/None/None; TEST not run; reason train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- rejected ranges/classes: {'train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0': 16}

## non_indicator_price_action
- tested iterations: 24
- non_indicator_price_action_vol_ratio<=1.783383: TRAIN trades=656, wins=228, losses=428, win_rate=34.76%, PF=0.4104, net=Rs -192,200, avg_win=Rs 587, avg_loss=Rs -762, SL/TGT/EOD=306/191/159, top_trade/day/symbol=0.005/None/None; TEST not run; reason train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- non_indicator_price_action_wick_skew_pct>=-0.072404: TRAIN trades=2475, wins=778, losses=1697, win_rate=31.43%, PF=0.3382, net=Rs -928,862, avg_win=Rs 610, avg_loss=Rs -827, SL/TGT/EOD=1386/682/407, top_trade/day/symbol=0.0014/None/None; TEST not run; reason train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- non_indicator_price_action_lower_wick_pct>=0.0: TRAIN trades=2646, wins=835, losses=1811, win_rate=31.56%, PF=0.3398, net=Rs -996,356, avg_win=Rs 614, avg_loss=Rs -833, SL/TGT/EOD=1495/739/412, top_trade/day/symbol=0.0013/None/None; TEST not run; reason train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- non_indicator_price_action_upper_wick_pct>=0.0: TRAIN trades=2646, wins=835, losses=1811, win_rate=31.56%, PF=0.3398, net=Rs -996,356, avg_win=Rs 614, avg_loss=Rs -833, SL/TGT/EOD=1495/739/412, top_trade/day/symbol=0.0013/None/None; TEST not run; reason train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- non_indicator_price_action_body_pct>=0.620145: TRAIN trades=2521, wins=795, losses=1726, win_rate=31.54%, PF=0.3372, net=Rs -948,510, avg_win=Rs 607, avg_loss=Rs -829, SL/TGT/EOD=1416/696/409, top_trade/day/symbol=0.0014/None/None; TEST not run; reason train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- rejected ranges/classes: {'train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0': 24}

## pre_momentum
- tested iterations: 34
- premom_pre5_dir_count>=2.0: TRAIN trades=2534, wins=820, losses=1714, win_rate=32.36%, PF=0.3512, net=Rs -922,969, avg_win=Rs 609, avg_loss=Rs -830, SL/TGT/EOD=1409/713/412, top_trade/day/symbol=0.0013/None/None; TEST not run; reason train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- premom_pre5_body_sum_r>=0.01455: TRAIN trades=2461, wins=787, losses=1674, win_rate=31.98%, PF=0.3469, net=Rs -907,929, avg_win=Rs 613, avg_loss=Rs -830, SL/TGT/EOD=1380/691/390, top_trade/day/symbol=0.0014/None/None; TEST not run; reason train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- premom_pre3_vol_ratio20>=0.50685: TRAIN trades=2554, wins=829, losses=1725, win_rate=32.46%, PF=0.3521, net=Rs -931,325, avg_win=Rs 611, avg_loss=Rs -833, SL/TGT/EOD=1427/727/400, top_trade/day/symbol=0.0013/None/None; TEST not run; reason train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- premom_pre10_close_pos>=0.676894: TRAIN trades=2467, wins=781, losses=1686, win_rate=31.66%, PF=0.3433, net=Rs -914,485, avg_win=Rs 612, avg_loss=Rs -826, SL/TGT/EOD=1372/688/407, top_trade/day/symbol=0.0014/None/None; TEST not run; reason train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- premom_sig5_adx_calc>=17.161032: TRAIN trades=2499, wins=794, losses=1705, win_rate=31.77%, PF=0.3436, net=Rs -929,605, avg_win=Rs 613, avg_loss=Rs -831, SL/TGT/EOD=1404/701/394, top_trade/day/symbol=0.0014/None/None; TEST not run; reason train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- rejected ranges/classes: {'train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0': 34}

## regime
- tested iterations: 3
- regime_align_0.3: TRAIN trades=2271, wins=696, losses=1575, win_rate=30.65%, PF=0.3257, net=Rs -885,664, avg_win=Rs 615, avg_loss=Rs -834, SL/TGT/EOD=1295/617/359, top_trade/day/symbol=0.0016/None/None; TEST not run; reason train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- regime_align_0.15: TRAIN trades=2176, wins=640, losses=1536, win_rate=29.41%, PF=0.3057, net=Rs -888,626, avg_win=Rs 611, avg_loss=Rs -833, SL/TGT/EOD=1260/563/353, top_trade/day/symbol=0.0017/None/None; TEST not run; reason train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- regime_align_0.0: TRAIN trades=1873, wins=531, losses=1342, win_rate=28.35%, PF=0.288, net=Rs -797,054, avg_win=Rs 607, avg_loss=Rs -834, SL/TGT/EOD=1099/467/307, top_trade/day/symbol=0.0021/None/None; TEST not run; reason train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- rejected ranges/classes: {'train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0': 3}

## Overfit-Risk Notes
- Candidates with TRAIN PF above 1.80 were not treated as success unless TEST and stability also passed.
- TEST was only run after FIT/VAL/full-TRAIN passed the train-side gate.
- Market-return/time pockets were logged as higher overfit risk and not promoted unless they cleared stability.

## Adaptive All-Knob Addendum
- Dynamic lowercase indicator/non-indicator search completed in `C:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2\Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\ADAPTIVE_ALL_KNOB_SEARCH.md`.
- Approximate candidates generated: 1200.
- Exact candidates evaluated: 312.
- Passing approval-required candidates: 0.
- Numeric columns tested included: atr_pct, body_pct, close_loc, lower_wick_pct, market_abs_ret_pct, market_ret_pct, notional, quality_score, rs_pct, signal_close, signal_high, signal_low, signal_minute, signal_open, signal_range_pct, signal_volume, upper_wick_pct, vol_ratio, vwap_dist_atr, wick_skew_pct.
