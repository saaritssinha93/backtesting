# Iteration Log - A_PULLBACK_C2_THEN_BREAK_C2_HIGH

## Iteration 001 - baseline_raw
- changed rule/parameter: current raw setup baseline
- parameter group: baseline
- old value: n/a
- new value: raw detection, SL/TGT 0.7/0.9
- command: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\scripts\full_loop_a_pullback_c2_high.py --max_iterations 200`
- FIT metrics: trades=1553, wins=522, losses=1031, win_rate=33.61%, PF=0.377, net=Rs -537,333, avg_win=Rs 623, avg_loss=Rs -837, SL/TGT/EOD=863/471/219, top_trade/day/symbol=0.0021/None/None
- VAL metrics: trades=1093, wins=313, losses=780, win_rate=28.64%, PF=0.2901, net=Rs -459,023, avg_win=Rs 599, avg_loss=Rs -829, SL/TGT/EOD=632/268/193, top_trade/day/symbol=0.0036/None/None
- full TRAIN metrics: trades=2646, wins=835, losses=1811, win_rate=31.56%, PF=0.3398, net=Rs -996,356, avg_win=Rs 614, avg_loss=Rs -833, SL/TGT/EOD=1495/739/412, top_trade/day/symbol=0.0013/None/None
- TEST metrics: not run
- keep/reject: REJECT
- failure classification: train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- next action: continue train-side search

## Iteration 002 - exit_sl0.5_tgt0.7
- changed rule/parameter: fixed SL/target grid sweep
- parameter group: exit
- old value: 0.7/0.9
- new value: 0.5/0.7
- command: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\scripts\full_loop_a_pullback_c2_high.py --max_iterations 200`
- FIT metrics: trades=2079, wins=597, losses=1482, win_rate=28.72%, PF=0.2608, net=Rs -767,205, avg_win=Rs 453, avg_loss=Rs -700, SL/TGT/EOD=1383/575/121, top_trade/day/symbol=0.0017/None/None
- VAL metrics: trades=1477, wins=383, losses=1094, win_rate=25.93%, PF=0.2254, net=Rs -586,115, avg_win=Rs 445, avg_loss=Rs -692, SL/TGT/EOD=997/362/118, top_trade/day/symbol=0.0027/None/None
- full TRAIN metrics: trades=3556, wins=980, losses=2576, win_rate=27.56%, PF=0.2459, net=Rs -1,353,320, avg_win=Rs 450, avg_loss=Rs -697, SL/TGT/EOD=2380/937/239, top_trade/day/symbol=0.0011/None/None
- TEST metrics: not run
- keep/reject: REJECT
- failure classification: train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- next action: continue train-side search

## Iteration 003 - exit_sl0.5_tgt1.0
- changed rule/parameter: fixed SL/target grid sweep
- parameter group: exit
- old value: 0.7/0.9
- new value: 0.5/1.0
- command: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\scripts\full_loop_a_pullback_c2_high.py --max_iterations 200`
- FIT metrics: trades=1880, wins=446, losses=1434, win_rate=23.72%, PF=0.3107, net=Rs -687,337, avg_win=Rs 695, avg_loss=Rs -695, SL/TGT/EOD=1325/383/172, top_trade/day/symbol=0.0025/None/None
- VAL metrics: trades=1372, wins=294, losses=1078, win_rate=21.43%, PF=0.2598, net=Rs -548,262, avg_win=Rs 655, avg_loss=Rs -687, SL/TGT/EOD=976/236/160, top_trade/day/symbol=0.004/None/None
- full TRAIN metrics: trades=3252, wins=740, losses=2512, win_rate=22.76%, PF=0.289, net=Rs -1,235,599, avg_win=Rs 679, avg_loss=Rs -692, SL/TGT/EOD=2301/619/332, top_trade/day/symbol=0.0015/None/None
- TEST metrics: not run
- keep/reject: REJECT
- failure classification: train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- next action: continue train-side search

## Iteration 004 - exit_sl0.5_tgt1.5
- changed rule/parameter: fixed SL/target grid sweep
- parameter group: exit
- old value: 0.7/0.9
- new value: 0.5/1.5
- command: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\scripts\full_loop_a_pullback_c2_high.py --max_iterations 200`
- FIT metrics: trades=1687, wins=352, losses=1335, win_rate=20.87%, PF=0.3636, net=Rs -589,330, avg_win=Rs 957, avg_loss=Rs -694, SL/TGT/EOD=1230/227/230, top_trade/day/symbol=0.0038/None/None
- VAL metrics: trades=1233, wins=217, losses=1016, win_rate=17.6%, PF=0.2671, net=Rs -506,023, avg_win=Rs 850, avg_loss=Rs -680, SL/TGT/EOD=903/114/216, top_trade/day/symbol=0.0069/None/None
- full TRAIN metrics: trades=2920, wins=569, losses=2351, win_rate=19.49%, PF=0.3224, net=Rs -1,095,354, avg_win=Rs 916, avg_loss=Rs -688, SL/TGT/EOD=2133/341/446, top_trade/day/symbol=0.0024/None/None
- TEST metrics: not run
- keep/reject: REJECT
- failure classification: train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- next action: continue train-side search

## Iteration 005 - exit_sl0.5_tgt2.0
- changed rule/parameter: fixed SL/target grid sweep
- parameter group: exit
- old value: 0.7/0.9
- new value: 0.5/2.0
- command: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\scripts\full_loop_a_pullback_c2_high.py --max_iterations 200`
- FIT metrics: trades=1598, wins=306, losses=1292, win_rate=19.15%, PF=0.3599, net=Rs -572,890, avg_win=Rs 1,053, avg_loss=Rs -693, SL/TGT/EOD=1190/132/276, top_trade/day/symbol=0.0055/None/None
- VAL metrics: trades=1181, wins=200, losses=981, win_rate=16.93%, PF=0.2962, net=Rs -469,647, avg_win=Rs 988, avg_loss=Rs -680, SL/TGT/EOD=874/80/227, top_trade/day/symbol=0.0089/None/None
- full TRAIN metrics: trades=2779, wins=506, losses=2273, win_rate=18.21%, PF=0.3327, net=Rs -1,042,537, avg_win=Rs 1,027, avg_loss=Rs -687, SL/TGT/EOD=2064/212/503, top_trade/day/symbol=0.0034/None/None
- TEST metrics: not run
- keep/reject: REJECT
- failure classification: train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- next action: continue train-side search

## Iteration 006 - exit_sl0.5_tgt2.5
- changed rule/parameter: fixed SL/target grid sweep
- parameter group: exit
- old value: 0.7/0.9
- new value: 0.5/2.5
- command: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\scripts\full_loop_a_pullback_c2_high.py --max_iterations 200`
- FIT metrics: trades=1528, wins=283, losses=1245, win_rate=18.52%, PF=0.3677, net=Rs -543,602, avg_win=Rs 1,117, avg_loss=Rs -691, SL/TGT/EOD=1141/85/302, top_trade/day/symbol=0.0072/None/None
- VAL metrics: trades=1149, wins=184, losses=965, win_rate=16.01%, PF=0.2754, net=Rs -476,476, avg_win=Rs 984, avg_loss=Rs -681, SL/TGT/EOD=863/44/242, top_trade/day/symbol=0.0125/None/None
- full TRAIN metrics: trades=2677, wins=467, losses=2210, win_rate=17.44%, PF=0.3277, net=Rs -1,020,078, avg_win=Rs 1,065, avg_loss=Rs -687, SL/TGT/EOD=2004/129/544, top_trade/day/symbol=0.0046/None/None
- TEST metrics: not run
- keep/reject: REJECT
- failure classification: train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- next action: continue train-side search

## Iteration 007 - exit_sl0.6_tgt0.8
- changed rule/parameter: fixed SL/target grid sweep
- parameter group: exit
- old value: 0.7/0.9
- new value: 0.6/0.8
- command: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\scripts\full_loop_a_pullback_c2_high.py --max_iterations 200`
- FIT metrics: trades=1794, wins=578, losses=1216, win_rate=32.22%, PF=0.3291, net=Rs -628,068, avg_win=Rs 533, avg_loss=Rs -770, SL/TGT/EOD=1070/530/194, top_trade/day/symbol=0.0018/None/None
- VAL metrics: trades=1265, wins=345, losses=920, win_rate=27.27%, PF=0.2549, net=Rs -524,712, avg_win=Rs 520, avg_loss=Rs -765, SL/TGT/EOD=794/309/162, top_trade/day/symbol=0.0032/None/None
- full TRAIN metrics: trades=3059, wins=923, losses=2136, win_rate=30.17%, PF=0.2973, net=Rs -1,152,780, avg_win=Rs 528, avg_loss=Rs -768, SL/TGT/EOD=1864/839/356, top_trade/day/symbol=0.0012/None/None
- TEST metrics: not run
- keep/reject: REJECT
- failure classification: train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- next action: continue train-side search

## Iteration 008 - exit_sl0.6_tgt1.2
- changed rule/parameter: fixed SL/target grid sweep
- parameter group: exit
- old value: 0.7/0.9
- new value: 0.6/1.2
- command: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\scripts\full_loop_a_pullback_c2_high.py --max_iterations 200`
- FIT metrics: trades=1577, wins=404, losses=1173, win_rate=25.62%, PF=0.3645, net=Rs -569,881, avg_win=Rs 809, avg_loss=Rs -765, SL/TGT/EOD=1023/308/246, top_trade/day/symbol=0.003/None/None
- VAL metrics: trades=1126, wins=255, losses=871, win_rate=22.65%, PF=0.291, net=Rs -467,928, avg_win=Rs 753, avg_loss=Rs -758, SL/TGT/EOD=744/177/205, top_trade/day/symbol=0.005/None/None
- full TRAIN metrics: trades=2703, wins=659, losses=2044, win_rate=24.38%, PF=0.3334, net=Rs -1,037,810, avg_win=Rs 788, avg_loss=Rs -762, SL/TGT/EOD=1767/485/451, top_trade/day/symbol=0.0019/None/None
- TEST metrics: not run
- keep/reject: REJECT
- failure classification: train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- next action: continue train-side search

## Iteration 009 - exit_sl0.6_tgt1.8
- changed rule/parameter: fixed SL/target grid sweep
- parameter group: exit
- old value: 0.7/0.9
- new value: 0.6/1.8
- command: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\scripts\full_loop_a_pullback_c2_high.py --max_iterations 200`
- FIT metrics: trades=1399, wins=335, losses=1064, win_rate=23.95%, PF=0.41, net=Rs -479,764, avg_win=Rs 995, avg_loss=Rs -764, SL/TGT/EOD=929/164/306, top_trade/day/symbol=0.0047/None/None
- VAL metrics: trades=1032, wins=204, losses=828, win_rate=19.77%, PF=0.3103, net=Rs -428,962, avg_win=Rs 946, avg_loss=Rs -751, SL/TGT/EOD=695/91/246, top_trade/day/symbol=0.0081/None/None
- full TRAIN metrics: trades=2431, wins=539, losses=1892, win_rate=22.17%, PF=0.3668, net=Rs -908,726, avg_win=Rs 977, avg_loss=Rs -759, SL/TGT/EOD=1624/255/552, top_trade/day/symbol=0.003/None/None
- TEST metrics: not run
- keep/reject: REJECT
- failure classification: train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- next action: continue train-side search

## Iteration 010 - exit_sl0.6_tgt2.5
- changed rule/parameter: fixed SL/target grid sweep
- parameter group: exit
- old value: 0.7/0.9
- new value: 0.6/2.5
- command: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\scripts\full_loop_a_pullback_c2_high.py --max_iterations 200`
- FIT metrics: trades=1318, wins=286, losses=1032, win_rate=21.7%, PF=0.4024, net=Rs -468,849, avg_win=Rs 1,104, avg_loss=Rs -760, SL/TGT/EOD=894/85/339, top_trade/day/symbol=0.0072/None/None
- VAL metrics: trades=980, wins=180, losses=800, win_rate=18.37%, PF=0.2938, net=Rs -423,760, avg_win=Rs 980, avg_loss=Rs -750, SL/TGT/EOD=670/42/268, top_trade/day/symbol=0.0129/None/None
- full TRAIN metrics: trades=2298, wins=466, losses=1832, win_rate=20.28%, PF=0.3554, net=Rs -892,609, avg_win=Rs 1,056, avg_loss=Rs -756, SL/TGT/EOD=1564/127/607, top_trade/day/symbol=0.0046/None/None
- TEST metrics: not run
- keep/reject: REJECT
- failure classification: train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- next action: continue train-side search

## Iteration 011 - exit_sl0.7_tgt1.2
- changed rule/parameter: fixed SL/target grid sweep
- parameter group: exit
- old value: 0.7/0.9
- new value: 0.7/1.2
- command: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\scripts\full_loop_a_pullback_c2_high.py --max_iterations 200`
- FIT metrics: trades=1405, wins=412, losses=993, win_rate=29.32%, PF=0.4066, net=Rs -489,963, avg_win=Rs 815, avg_loss=Rs -831, SL/TGT/EOD=825/319/261, top_trade/day/symbol=0.0029/None/None
- VAL metrics: trades=986, wins=255, losses=731, win_rate=25.86%, PF=0.3245, net=Rs -402,905, avg_win=Rs 759, avg_loss=Rs -816, SL/TGT/EOD=580/177/229, top_trade/day/symbol=0.005/None/None
- full TRAIN metrics: trades=2391, wins=667, losses=1724, win_rate=27.9%, PF=0.3721, net=Rs -892,868, avg_win=Rs 793, avg_loss=Rs -825, SL/TGT/EOD=1405/496/490, top_trade/day/symbol=0.0018/None/None
- TEST metrics: not run
- keep/reject: REJECT
- failure classification: train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- next action: continue train-side search

## Iteration 012 - exit_sl0.7_tgt1.5
- changed rule/parameter: fixed SL/target grid sweep
- parameter group: exit
- old value: 0.7/0.9
- new value: 0.7/1.5
- command: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\scripts\full_loop_a_pullback_c2_high.py --max_iterations 200`
- FIT metrics: trades=1321, wins=365, losses=956, win_rate=27.63%, PF=0.4425, net=Rs -440,035, avg_win=Rs 957, avg_loss=Rs -826, SL/TGT/EOD=787/237/297, top_trade/day/symbol=0.0036/None/None
- VAL metrics: trades=930, wins=220, losses=710, win_rate=23.66%, PF=0.3301, net=Rs -385,414, avg_win=Rs 863, avg_loss=Rs -810, SL/TGT/EOD=552/119/259, top_trade/day/symbol=0.0067/None/None
- full TRAIN metrics: trades=2251, wins=585, losses=1666, win_rate=25.99%, PF=0.3951, net=Rs -825,449, avg_win=Rs 922, avg_loss=Rs -819, SL/TGT/EOD=1339/356/556, top_trade/day/symbol=0.0023/None/None
- TEST metrics: not run
- keep/reject: REJECT
- failure classification: train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- next action: continue train-side search

## Iteration 013 - exit_sl0.7_tgt1.8
- changed rule/parameter: fixed SL/target grid sweep
- parameter group: exit
- old value: 0.7/0.9
- new value: 0.7/1.8
- command: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\scripts\full_loop_a_pullback_c2_high.py --max_iterations 200`
- FIT metrics: trades=1238, wins=329, losses=909, win_rate=26.58%, PF=0.4409, net=Rs -420,499, avg_win=Rs 1,008, avg_loss=Rs -827, SL/TGT/EOD=752/160/326, top_trade/day/symbol=0.0047/None/None
- VAL metrics: trades=896, wins=208, losses=688, win_rate=23.21%, PF=0.36, net=Rs -356,792, avg_win=Rs 965, avg_loss=Rs -810, SL/TGT/EOD=536/96/264, top_trade/day/symbol=0.0078/None/None
- full TRAIN metrics: trades=2134, wins=537, losses=1597, win_rate=25.16%, PF=0.4065, net=Rs -777,292, avg_win=Rs 991, avg_loss=Rs -820, SL/TGT/EOD=1288/256/590, top_trade/day/symbol=0.0029/None/None
- TEST metrics: not run
- keep/reject: REJECT
- failure classification: train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- next action: continue train-side search

## Iteration 014 - exit_sl0.7_tgt2.0
- changed rule/parameter: fixed SL/target grid sweep
- parameter group: exit
- old value: 0.7/0.9
- new value: 0.7/2.0
- command: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\scripts\full_loop_a_pullback_c2_high.py --max_iterations 200`
- FIT metrics: trades=1208, wins=313, losses=895, win_rate=25.91%, PF=0.4404, net=Rs -412,868, avg_win=Rs 1,038, avg_loss=Rs -824, SL/TGT/EOD=736/131/341, top_trade/day/symbol=0.0054/None/None
- VAL metrics: trades=878, wins=201, losses=677, win_rate=22.89%, PF=0.3669, net=Rs -346,764, avg_win=Rs 1,000, avg_loss=Rs -809, SL/TGT/EOD=527/82/269, top_trade/day/symbol=0.0088/None/None
- full TRAIN metrics: trades=2086, wins=514, losses=1572, win_rate=24.64%, PF=0.409, net=Rs -759,633, avg_win=Rs 1,023, avg_loss=Rs -818, SL/TGT/EOD=1263/213/610, top_trade/day/symbol=0.0034/None/None
- TEST metrics: not run
- keep/reject: REJECT
- failure classification: train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- next action: continue train-side search

## Iteration 015 - exit_sl0.7_tgt2.5
- changed rule/parameter: fixed SL/target grid sweep
- parameter group: exit
- old value: 0.7/0.9
- new value: 0.7/2.5
- command: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\scripts\full_loop_a_pullback_c2_high.py --max_iterations 200`
- FIT metrics: trades=1152, wins=291, losses=861, win_rate=25.26%, PF=0.4498, net=Rs -392,013, avg_win=Rs 1,101, avg_loss=Rs -828, SL/TGT/EOD=711/84/357, top_trade/day/symbol=0.0071/None/None
- VAL metrics: trades=852, wins=184, losses=668, win_rate=21.6%, PF=0.35, net=Rs -351,951, avg_win=Rs 1,030, avg_loss=Rs -811, SL/TGT/EOD=521/49/282, top_trade/day/symbol=0.012/None/None
- full TRAIN metrics: trades=2004, wins=475, losses=1529, win_rate=23.7%, PF=0.4067, net=Rs -743,963, avg_win=Rs 1,074, avg_loss=Rs -820, SL/TGT/EOD=1232/133/639, top_trade/day/symbol=0.0044/None/None
- TEST metrics: not run
- keep/reject: REJECT
- failure classification: train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- next action: continue train-side search

## Iteration 016 - exit_sl0.85_tgt0.9
- changed rule/parameter: fixed SL/target grid sweep
- parameter group: exit
- old value: 0.7/0.9
- new value: 0.85/0.9
- command: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\scripts\full_loop_a_pullback_c2_high.py --max_iterations 200`
- FIT metrics: trades=1381, wins=519, losses=862, win_rate=37.58%, PF=0.407, net=Rs -470,676, avg_win=Rs 622, avg_loss=Rs -921, SL/TGT/EOD=657/469/255, top_trade/day/symbol=0.0021/None/None
- VAL metrics: trades=990, wins=317, losses=673, win_rate=32.02%, PF=0.3122, net=Rs -420,049, avg_win=Rs 602, avg_loss=Rs -907, SL/TGT/EOD=494/272/224, top_trade/day/symbol=0.0035/None/None
- full TRAIN metrics: trades=2371, wins=836, losses=1535, win_rate=35.26%, PF=0.3658, net=Rs -890,726, avg_win=Rs 614, avg_loss=Rs -915, SL/TGT/EOD=1151/741/479, top_trade/day/symbol=0.0013/None/None
- TEST metrics: not run
- keep/reject: REJECT
- failure classification: train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- next action: continue train-side search

## Iteration 017 - exit_sl0.85_tgt1.2
- changed rule/parameter: fixed SL/target grid sweep
- parameter group: exit
- old value: 0.7/0.9
- new value: 0.85/1.2
- command: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\scripts\full_loop_a_pullback_c2_high.py --max_iterations 200`
- FIT metrics: trades=1238, wins=414, losses=824, win_rate=33.44%, PF=0.4582, net=Rs -407,954, avg_win=Rs 833, avg_loss=Rs -914, SL/TGT/EOD=627/332/279, top_trade/day/symbol=0.0028/None/None
- VAL metrics: trades=900, wins=251, losses=649, win_rate=27.89%, PF=0.3363, net=Rs -385,351, avg_win=Rs 778, avg_loss=Rs -895, SL/TGT/EOD=469/181/250, top_trade/day/symbol=0.005/None/None
- full TRAIN metrics: trades=2138, wins=665, losses=1473, win_rate=31.1%, PF=0.4051, net=Rs -793,306, avg_win=Rs 812, avg_loss=Rs -905, SL/TGT/EOD=1096/513/529, top_trade/day/symbol=0.0018/None/None
- TEST metrics: not run
- keep/reject: REJECT
- failure classification: train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- next action: continue train-side search

## Iteration 018 - exit_sl0.85_tgt1.5
- changed rule/parameter: fixed SL/target grid sweep
- parameter group: exit
- old value: 0.7/0.9
- new value: 0.85/1.5
- command: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\scripts\full_loop_a_pullback_c2_high.py --max_iterations 200`
- FIT metrics: trades=1171, wins=364, losses=807, win_rate=31.08%, PF=0.4919, net=Rs -372,238, avg_win=Rs 990, avg_loss=Rs -908, SL/TGT/EOD=612/248/311, top_trade/day/symbol=0.0035/None/None
- VAL metrics: trades=837, wins=210, losses=627, win_rate=25.09%, PF=0.3401, net=Rs -367,966, avg_win=Rs 903, avg_loss=Rs -889, SL/TGT/EOD=447/121/269, top_trade/day/symbol=0.0067/None/None
- full TRAIN metrics: trades=2008, wins=574, losses=1434, win_rate=28.59%, PF=0.4263, net=Rs -740,204, avg_win=Rs 958, avg_loss=Rs -900, SL/TGT/EOD=1059/369/580, top_trade/day/symbol=0.0023/None/None
- TEST metrics: not run
- keep/reject: REJECT
- failure classification: train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- next action: continue train-side search

## Iteration 019 - exit_sl0.85_tgt1.8
- changed rule/parameter: fixed SL/target grid sweep
- parameter group: exit
- old value: 0.7/0.9
- new value: 0.85/1.8
- command: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\scripts\full_loop_a_pullback_c2_high.py --max_iterations 200`
- FIT metrics: trades=1109, wins=330, losses=779, win_rate=29.76%, PF=0.4882, net=Rs -363,073, avg_win=Rs 1,049, avg_loss=Rs -911, SL/TGT/EOD=596/171/342, top_trade/day/symbol=0.0045/None/None
- VAL metrics: trades=824, wins=202, losses=622, win_rate=24.51%, PF=0.3654, net=Rs -352,588, avg_win=Rs 1,005, avg_loss=Rs -893, SL/TGT/EOD=446/98/280, top_trade/day/symbol=0.0077/None/None
- full TRAIN metrics: trades=1933, wins=532, losses=1401, win_rate=27.52%, PF=0.4342, net=Rs -715,661, avg_win=Rs 1,032, avg_loss=Rs -903, SL/TGT/EOD=1042/269/622, top_trade/day/symbol=0.0029/None/None
- TEST metrics: not run
- keep/reject: REJECT
- failure classification: train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- next action: continue train-side search

## Iteration 020 - exit_sl0.85_tgt2.0
- changed rule/parameter: fixed SL/target grid sweep
- parameter group: exit
- old value: 0.7/0.9
- new value: 0.85/2.0
- command: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\scripts\full_loop_a_pullback_c2_high.py --max_iterations 200`
- FIT metrics: trades=1080, wins=314, losses=766, win_rate=29.07%, PF=0.4857, net=Rs -356,265, avg_win=Rs 1,072, avg_loss=Rs -904, SL/TGT/EOD=582/136/362, top_trade/day/symbol=0.0053/None/None
- VAL metrics: trades=796, wins=190, losses=606, win_rate=23.87%, PF=0.3642, net=Rs -342,626, avg_win=Rs 1,033, avg_loss=Rs -889, SL/TGT/EOD=430/81/285, top_trade/day/symbol=0.009/None/None
- full TRAIN metrics: trades=1876, wins=504, losses=1372, win_rate=26.87%, PF=0.4325, net=Rs -698,891, avg_win=Rs 1,057, avg_loss=Rs -898, SL/TGT/EOD=1012/217/647, top_trade/day/symbol=0.0033/None/None
- TEST metrics: not run
- keep/reject: REJECT
- failure classification: train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- next action: continue train-side search

## Iteration 021 - exit_sl0.85_tgt2.5
- changed rule/parameter: fixed SL/target grid sweep
- parameter group: exit
- old value: 0.7/0.9
- new value: 0.85/2.5
- command: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\scripts\full_loop_a_pullback_c2_high.py --max_iterations 200`
- FIT metrics: trades=1034, wins=284, losses=750, win_rate=27.47%, PF=0.4972, net=Rs -341,884, avg_win=Rs 1,190, avg_loss=Rs -907, SL/TGT/EOD=569/91/374, top_trade/day/symbol=0.0067/None/None
- VAL metrics: trades=778, wins=176, losses=602, win_rate=22.62%, PF=0.3512, net=Rs -349,186, avg_win=Rs 1,074, avg_loss=Rs -894, SL/TGT/EOD=431/49/298, top_trade/day/symbol=0.012/None/None
- full TRAIN metrics: trades=1812, wins=460, losses=1352, win_rate=25.39%, PF=0.4327, net=Rs -691,070, avg_win=Rs 1,146, avg_loss=Rs -901, SL/TGT/EOD=1000/140/672, top_trade/day/symbol=0.0043/None/None
- TEST metrics: not run
- keep/reject: REJECT
- failure classification: train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- next action: continue train-side search

## Iteration 022 - exit_sl0.9_tgt1.0
- changed rule/parameter: fixed SL/target grid sweep
- parameter group: exit
- old value: 0.7/0.9
- new value: 0.9/1.0
- command: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\scripts\full_loop_a_pullback_c2_high.py --max_iterations 200`
- FIT metrics: trades=1276, wins=461, losses=815, win_rate=36.13%, PF=0.4172, net=Rs -445,612, avg_win=Rs 692, avg_loss=Rs -938, SL/TGT/EOD=596/398/282, top_trade/day/symbol=0.0024/None/None
- VAL metrics: trades=917, wins=280, losses=637, win_rate=30.53%, PF=0.3136, net=Rs -405,610, avg_win=Rs 662, avg_loss=Rs -928, SL/TGT/EOD=448/228/241, top_trade/day/symbol=0.0041/None/None
- full TRAIN metrics: trades=2193, wins=741, losses=1452, win_rate=33.79%, PF=0.372, net=Rs -851,222, avg_win=Rs 681, avg_loss=Rs -934, SL/TGT/EOD=1044/626/523, top_trade/day/symbol=0.0015/None/None
- TEST metrics: not run
- keep/reject: REJECT
- failure classification: train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- next action: continue train-side search

## Iteration 023 - exit_sl0.9_tgt1.5
- changed rule/parameter: fixed SL/target grid sweep
- parameter group: exit
- old value: 0.7/0.9
- new value: 0.9/1.5
- command: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\scripts\full_loop_a_pullback_c2_high.py --max_iterations 200`
- FIT metrics: trades=1121, wins=347, losses=774, win_rate=30.95%, PF=0.4744, net=Rs -376,735, avg_win=Rs 980, avg_loss=Rs -926, SL/TGT/EOD=556/233/332, top_trade/day/symbol=0.0037/None/None
- VAL metrics: trades=809, wins=209, losses=600, win_rate=25.83%, PF=0.3379, net=Rs -364,586, avg_win=Rs 890, avg_loss=Rs -918, SL/TGT/EOD=416/119/274, top_trade/day/symbol=0.0068/None/None
- full TRAIN metrics: trades=1930, wins=556, losses=1374, win_rate=28.81%, PF=0.4151, net=Rs -741,320, avg_win=Rs 946, avg_loss=Rs -922, SL/TGT/EOD=972/352/606, top_trade/day/symbol=0.0024/None/None
- TEST metrics: not run
- keep/reject: REJECT
- failure classification: train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- next action: continue train-side search

## Iteration 024 - exit_sl0.9_tgt2.0
- changed rule/parameter: fixed SL/target grid sweep
- parameter group: exit
- old value: 0.7/0.9
- new value: 0.9/2.0
- command: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\scripts\full_loop_a_pullback_c2_high.py --max_iterations 200`
- FIT metrics: trades=1041, wins=302, losses=739, win_rate=29.01%, PF=0.4703, net=Rs -363,518, avg_win=Rs 1,069, avg_loss=Rs -929, SL/TGT/EOD=542/131/368, top_trade/day/symbol=0.0055/None/None
- VAL metrics: trades=766, wins=185, losses=581, win_rate=24.15%, PF=0.3551, net=Rs -342,602, avg_win=Rs 1,020, avg_loss=Rs -914, SL/TGT/EOD=398/78/290, top_trade/day/symbol=0.0094/None/None
- full TRAIN metrics: trades=1807, wins=487, losses=1320, win_rate=26.95%, PF=0.42, net=Rs -706,120, avg_win=Rs 1,050, avg_loss=Rs -922, SL/TGT/EOD=940/209/658, top_trade/day/symbol=0.0035/None/None
- TEST metrics: not run
- keep/reject: REJECT
- failure classification: train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- next action: continue train-side search

## Iteration 025 - exit_sl0.9_tgt2.5
- changed rule/parameter: fixed SL/target grid sweep
- parameter group: exit
- old value: 0.7/0.9
- new value: 0.9/2.5
- command: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\scripts\full_loop_a_pullback_c2_high.py --max_iterations 200`
- FIT metrics: trades=997, wins=272, losses=725, win_rate=27.28%, PF=0.4643, net=Rs -362,102, avg_win=Rs 1,154, avg_loss=Rs -932, SL/TGT/EOD=531/84/382, top_trade/day/symbol=0.0072/None/None
- VAL metrics: trades=743, wins=170, losses=573, win_rate=22.88%, PF=0.3341, net=Rs -349,627, avg_win=Rs 1,032, avg_loss=Rs -916, SL/TGT/EOD=393/44/306, top_trade/day/symbol=0.0129/None/None
- full TRAIN metrics: trades=1740, wins=442, losses=1298, win_rate=25.4%, PF=0.4073, net=Rs -711,729, avg_win=Rs 1,107, avg_loss=Rs -925, SL/TGT/EOD=924/128/688, top_trade/day/symbol=0.0046/None/None
- TEST metrics: not run
- keep/reject: REJECT
- failure classification: train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- next action: continue train-side search

## Iteration 026 - exit_sl1.0_tgt1.2
- changed rule/parameter: fixed SL/target grid sweep
- parameter group: exit
- old value: 0.7/0.9
- new value: 1.0/1.2
- command: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\scripts\full_loop_a_pullback_c2_high.py --max_iterations 200`
- FIT metrics: trades=1113, wins=387, losses=726, win_rate=34.77%, PF=0.4531, net=Rs -385,854, avg_win=Rs 826, avg_loss=Rs -972, SL/TGT/EOD=488/308/317, top_trade/day/symbol=0.003/None/None
- VAL metrics: trades=802, wins=229, losses=573, win_rate=28.55%, PF=0.3158, net=Rs -381,355, avg_win=Rs 769, avg_loss=Rs -973, SL/TGT/EOD=377/163/262, top_trade/day/symbol=0.0055/None/None
- full TRAIN metrics: trades=1915, wins=616, losses=1299, win_rate=32.17%, PF=0.3925, net=Rs -767,209, avg_win=Rs 805, avg_loss=Rs -972, SL/TGT/EOD=865/471/579, top_trade/day/symbol=0.0019/None/None
- TEST metrics: not run
- keep/reject: REJECT
- failure classification: train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- next action: continue train-side search

## Iteration 027 - exit_sl1.0_tgt1.5
- changed rule/parameter: fixed SL/target grid sweep
- parameter group: exit
- old value: 0.7/0.9
- new value: 1.0/1.5
- command: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\scripts\full_loop_a_pullback_c2_high.py --max_iterations 200`
- FIT metrics: trades=1055, wins=344, losses=711, win_rate=32.61%, PF=0.4969, net=Rs -343,210, avg_win=Rs 985, avg_loss=Rs -959, SL/TGT/EOD=469/234/352, top_trade/day/symbol=0.0037/None/None
- VAL metrics: trades=753, wins=198, losses=555, win_rate=26.29%, PF=0.3322, net=Rs -356,544, avg_win=Rs 896, avg_loss=Rs -962, SL/TGT/EOD=356/114/283, top_trade/day/symbol=0.0071/None/None
- full TRAIN metrics: trades=1808, wins=542, losses=1266, win_rate=29.98%, PF=0.4246, net=Rs -699,754, avg_win=Rs 953, avg_loss=Rs -961, SL/TGT/EOD=825/348/635, top_trade/day/symbol=0.0025/None/None
- TEST metrics: not run
- keep/reject: REJECT
- failure classification: train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- next action: continue train-side search

## Iteration 028 - exit_sl1.0_tgt2.0
- changed rule/parameter: fixed SL/target grid sweep
- parameter group: exit
- old value: 0.7/0.9
- new value: 1.0/2.0
- command: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\scripts\full_loop_a_pullback_c2_high.py --max_iterations 200`
- FIT metrics: trades=974, wins=291, losses=683, win_rate=29.88%, PF=0.4804, net=Rs -342,374, avg_win=Rs 1,088, avg_loss=Rs -965, SL/TGT/EOD=461/130/383, top_trade/day/symbol=0.0056/None/None
- VAL metrics: trades=720, wins=179, losses=541, win_rate=24.86%, PF=0.3485, net=Rs -343,403, avg_win=Rs 1,026, avg_loss=Rs -974, SL/TGT/EOD=351/77/292, top_trade/day/symbol=0.0096/None/None
- full TRAIN metrics: trades=1694, wins=470, losses=1224, win_rate=27.74%, PF=0.4218, net=Rs -685,777, avg_win=Rs 1,064, avg_loss=Rs -969, SL/TGT/EOD=812/207/675, top_trade/day/symbol=0.0035/None/None
- TEST metrics: not run
- keep/reject: REJECT
- failure classification: train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- next action: continue train-side search

## Iteration 029 - exit_sl1.0_tgt2.5
- changed rule/parameter: fixed SL/target grid sweep
- parameter group: exit
- old value: 0.7/0.9
- new value: 1.0/2.5
- command: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\scripts\full_loop_a_pullback_c2_high.py --max_iterations 200`
- FIT metrics: trades=936, wins=265, losses=671, win_rate=28.31%, PF=0.4862, net=Rs -333,390, avg_win=Rs 1,191, avg_loss=Rs -967, SL/TGT/EOD=451/86/399, top_trade/day/symbol=0.0072/None/None
- VAL metrics: trades=694, wins=163, losses=531, win_rate=23.49%, PF=0.3274, net=Rs -346,805, avg_win=Rs 1,036, avg_loss=Rs -971, SL/TGT/EOD=343/43/308, top_trade/day/symbol=0.0134/None/None
- full TRAIN metrics: trades=1630, wins=428, losses=1202, win_rate=26.26%, PF=0.4159, net=Rs -680,195, avg_win=Rs 1,132, avg_loss=Rs -969, SL/TGT/EOD=794/129/707, top_trade/day/symbol=0.0047/None/None
- TEST metrics: not run
- keep/reject: REJECT
- failure classification: train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- next action: continue train-side search

## Iteration 030 - exit_sl1.1_tgt1.5
- changed rule/parameter: fixed SL/target grid sweep
- parameter group: exit
- old value: 0.7/0.9
- new value: 1.1/1.5
- command: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\scripts\full_loop_a_pullback_c2_high.py --max_iterations 200`
- FIT metrics: trades=1005, wins=339, losses=666, win_rate=33.73%, PF=0.4972, net=Rs -334,457, avg_win=Rs 976, avg_loss=Rs -999, SL/TGT/EOD=410/226/369, top_trade/day/symbol=0.0038/None/None
- VAL metrics: trades=709, wins=198, losses=511, win_rate=27.93%, PF=0.3474, net=Rs -331,295, avg_win=Rs 891, avg_loss=Rs -993, SL/TGT/EOD=300/114/295, top_trade/day/symbol=0.0072/None/None
- full TRAIN metrics: trades=1714, wins=537, losses=1177, win_rate=31.33%, PF=0.4324, net=Rs -665,752, avg_win=Rs 944, avg_loss=Rs -996, SL/TGT/EOD=710/340/664, top_trade/day/symbol=0.0025/None/None
- TEST metrics: not run
- keep/reject: REJECT
- failure classification: train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- next action: continue train-side search

## Iteration 031 - exit_sl1.1_tgt2.0
- changed rule/parameter: fixed SL/target grid sweep
- parameter group: exit
- old value: 0.7/0.9
- new value: 1.1/2.0
- command: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\scripts\full_loop_a_pullback_c2_high.py --max_iterations 200`
- FIT metrics: trades=932, wins=290, losses=642, win_rate=31.12%, PF=0.4801, net=Rs -334,784, avg_win=Rs 1,066, avg_loss=Rs -1,003, SL/TGT/EOD=402/123/407, top_trade/day/symbol=0.0057/None/None
- VAL metrics: trades=677, wins=184, losses=493, win_rate=27.18%, PF=0.3957, net=Rs -297,634, avg_win=Rs 1,059, avg_loss=Rs -999, SL/TGT/EOD=291/84/302, top_trade/day/symbol=0.0091/None/None
- full TRAIN metrics: trades=1609, wins=474, losses=1135, win_rate=29.46%, PF=0.4435, net=Rs -632,418, avg_win=Rs 1,063, avg_loss=Rs -1,001, SL/TGT/EOD=693/207/709, top_trade/day/symbol=0.0035/None/None
- TEST metrics: not run
- keep/reject: REJECT
- failure classification: train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- next action: continue train-side search

## Iteration 032 - exit_sl1.1_tgt2.5
- changed rule/parameter: fixed SL/target grid sweep
- parameter group: exit
- old value: 0.7/0.9
- new value: 1.1/2.5
- command: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\scripts\full_loop_a_pullback_c2_high.py --max_iterations 200`
- FIT metrics: trades=891, wins=259, losses=632, win_rate=29.07%, PF=0.4721, net=Rs -332,516, avg_win=Rs 1,148, avg_loss=Rs -997, SL/TGT/EOD=390/81/420, top_trade/day/symbol=0.0076/None/None
- VAL metrics: trades=654, wins=167, losses=487, win_rate=25.54%, PF=0.3649, net=Rs -308,331, avg_win=Rs 1,061, avg_loss=Rs -997, SL/TGT/EOD=286/45/323, top_trade/day/symbol=0.0128/None/None
- full TRAIN metrics: trades=1545, wins=426, losses=1119, win_rate=27.57%, PF=0.4254, net=Rs -640,847, avg_win=Rs 1,114, avg_loss=Rs -997, SL/TGT/EOD=676/126/743, top_trade/day/symbol=0.0048/None/None
- TEST metrics: not run
- keep/reject: REJECT
- failure classification: train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- next action: continue train-side search

## Iteration 033 - exit_sl1.2_tgt1.5
- changed rule/parameter: fixed SL/target grid sweep
- parameter group: exit
- old value: 0.7/0.9
- new value: 1.2/1.5
- command: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\scripts\full_loop_a_pullback_c2_high.py --max_iterations 200`
- FIT metrics: trades=965, wins=337, losses=628, win_rate=34.92%, PF=0.495, net=Rs -331,684, avg_win=Rs 965, avg_loss=Rs -1,046, SL/TGT/EOD=367/223/375, top_trade/day/symbol=0.0039/None/None
- VAL metrics: trades=692, wins=200, losses=492, win_rate=28.9%, PF=0.3584, net=Rs -327,572, avg_win=Rs 915, avg_loss=Rs -1,038, SL/TGT/EOD=277/119/296, top_trade/day/symbol=0.0069/None/None
- full TRAIN metrics: trades=1657, wins=537, losses=1120, win_rate=32.41%, PF=0.4352, net=Rs -659,256, avg_win=Rs 946, avg_loss=Rs -1,042, SL/TGT/EOD=644/342/671, top_trade/day/symbol=0.0025/None/None
- TEST metrics: not run
- keep/reject: REJECT
- failure classification: train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- next action: continue train-side search

## Iteration 034 - exit_sl1.2_tgt2.0
- changed rule/parameter: fixed SL/target grid sweep
- parameter group: exit
- old value: 0.7/0.9
- new value: 1.2/2.0
- command: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\scripts\full_loop_a_pullback_c2_high.py --max_iterations 200`
- FIT metrics: trades=892, wins=288, losses=604, win_rate=32.29%, PF=0.4779, net=Rs -331,062, avg_win=Rs 1,052, avg_loss=Rs -1,050, SL/TGT/EOD=356/121/415, top_trade/day/symbol=0.0058/None/None
- VAL metrics: trades=662, wins=185, losses=477, win_rate=27.95%, PF=0.4011, net=Rs -296,447, avg_win=Rs 1,073, avg_loss=Rs -1,038, SL/TGT/EOD=268/85/309, top_trade/day/symbol=0.0089/None/None
- full TRAIN metrics: trades=1554, wins=473, losses=1081, win_rate=30.44%, PF=0.4442, net=Rs -627,509, avg_win=Rs 1,060, avg_loss=Rs -1,044, SL/TGT/EOD=624/206/724, top_trade/day/symbol=0.0035/None/None
- TEST metrics: not run
- keep/reject: REJECT
- failure classification: train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- next action: continue train-side search

## Iteration 035 - exit_sl1.2_tgt2.5
- changed rule/parameter: fixed SL/target grid sweep
- parameter group: exit
- old value: 0.7/0.9
- new value: 1.2/2.5
- command: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\scripts\full_loop_a_pullback_c2_high.py --max_iterations 200`
- FIT metrics: trades=852, wins=260, losses=592, win_rate=30.52%, PF=0.4817, net=Rs -320,073, avg_win=Rs 1,144, avg_loss=Rs -1,043, SL/TGT/EOD=345/82/425, top_trade/day/symbol=0.0076/None/None
- VAL metrics: trades=639, wins=167, losses=472, win_rate=26.13%, PF=0.379, net=Rs -306,462, avg_win=Rs 1,120, avg_loss=Rs -1,046, SL/TGT/EOD=268/50/321, top_trade/day/symbol=0.0121/None/None
- full TRAIN metrics: trades=1491, wins=427, losses=1064, win_rate=28.64%, PF=0.4361, net=Rs -626,536, avg_win=Rs 1,135, avg_loss=Rs -1,044, SL/TGT/EOD=613/132/746, top_trade/day/symbol=0.0047/None/None
- TEST metrics: not run
- keep/reject: REJECT
- failure classification: train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- next action: continue train-side search

## Iteration 036 - exit_sl1.4_tgt2.0
- changed rule/parameter: fixed SL/target grid sweep
- parameter group: exit
- old value: 0.7/0.9
- new value: 1.4/2.0
- command: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\scripts\full_loop_a_pullback_c2_high.py --max_iterations 200`
- FIT metrics: trades=831, wins=282, losses=549, win_rate=33.94%, PF=0.4883, net=Rs -310,603, avg_win=Rs 1,051, avg_loss=Rs -1,106, SL/TGT/EOD=277/118/436, top_trade/day/symbol=0.006/None/None
- VAL metrics: trades=599, wins=169, losses=430, win_rate=28.21%, PF=0.3647, net=Rs -299,607, avg_win=Rs 1,018, avg_loss=Rs -1,097, SL/TGT/EOD=204/71/324, top_trade/day/symbol=0.0103/None/None
- full TRAIN metrics: trades=1430, wins=451, losses=979, win_rate=31.54%, PF=0.4342, net=Rs -610,210, avg_win=Rs 1,039, avg_loss=Rs -1,102, SL/TGT/EOD=481/189/760, top_trade/day/symbol=0.0038/None/None
- TEST metrics: not run
- keep/reject: REJECT
- failure classification: train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- next action: continue train-side search

## Iteration 037 - exit_sl1.4_tgt2.5
- changed rule/parameter: fixed SL/target grid sweep
- parameter group: exit
- old value: 0.7/0.9
- new value: 1.4/2.5
- command: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\scripts\full_loop_a_pullback_c2_high.py --max_iterations 200`
- FIT metrics: trades=796, wins=258, losses=538, win_rate=32.41%, PF=0.4917, net=Rs -301,716, avg_win=Rs 1,131, avg_loss=Rs -1,103, SL/TGT/EOD=270/80/446, top_trade/day/symbol=0.0078/None/None
- VAL metrics: trades=577, wins=154, losses=423, win_rate=26.69%, PF=0.354, net=Rs -301,589, avg_win=Rs 1,073, avg_loss=Rs -1,104, SL/TGT/EOD=203/43/331, top_trade/day/symbol=0.0137/None/None
- full TRAIN metrics: trades=1373, wins=412, losses=961, win_rate=30.01%, PF=0.4311, net=Rs -603,305, avg_win=Rs 1,110, avg_loss=Rs -1,104, SL/TGT/EOD=473/123/777, top_trade/day/symbol=0.005/None/None
- TEST metrics: not run
- keep/reject: REJECT
- failure classification: train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- next action: continue train-side search

## Iteration 038 - guard_min_slot09:30
- changed rule/parameter: entry guard/time-window/top_n sweep
- parameter group: guard
- old value: {}
- new value: {"min_slot": "09:30"}
- command: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\scripts\full_loop_a_pullback_c2_high.py --max_iterations 200`
- FIT metrics: trades=1553, wins=522, losses=1031, win_rate=33.61%, PF=0.377, net=Rs -537,333, avg_win=Rs 623, avg_loss=Rs -837, SL/TGT/EOD=863/471/219, top_trade/day/symbol=0.0021/None/None
- VAL metrics: trades=1093, wins=313, losses=780, win_rate=28.64%, PF=0.2901, net=Rs -459,023, avg_win=Rs 599, avg_loss=Rs -829, SL/TGT/EOD=632/268/193, top_trade/day/symbol=0.0036/None/None
- full TRAIN metrics: trades=2646, wins=835, losses=1811, win_rate=31.56%, PF=0.3398, net=Rs -996,356, avg_win=Rs 614, avg_loss=Rs -833, SL/TGT/EOD=1495/739/412, top_trade/day/symbol=0.0013/None/None
- TEST metrics: not run
- keep/reject: REJECT
- failure classification: train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- next action: continue train-side search

## Iteration 039 - guard_min_slot09:45
- changed rule/parameter: entry guard/time-window/top_n sweep
- parameter group: guard
- old value: {}
- new value: {"min_slot": "09:45"}
- command: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\scripts\full_loop_a_pullback_c2_high.py --max_iterations 200`
- FIT metrics: trades=1553, wins=522, losses=1031, win_rate=33.61%, PF=0.377, net=Rs -537,333, avg_win=Rs 623, avg_loss=Rs -837, SL/TGT/EOD=863/471/219, top_trade/day/symbol=0.0021/None/None
- VAL metrics: trades=1093, wins=313, losses=780, win_rate=28.64%, PF=0.2901, net=Rs -459,023, avg_win=Rs 599, avg_loss=Rs -829, SL/TGT/EOD=632/268/193, top_trade/day/symbol=0.0036/None/None
- full TRAIN metrics: trades=2646, wins=835, losses=1811, win_rate=31.56%, PF=0.3398, net=Rs -996,356, avg_win=Rs 614, avg_loss=Rs -833, SL/TGT/EOD=1495/739/412, top_trade/day/symbol=0.0013/None/None
- TEST metrics: not run
- keep/reject: REJECT
- failure classification: train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- next action: continue train-side search

## Iteration 040 - guard_min_slot10:00
- changed rule/parameter: entry guard/time-window/top_n sweep
- parameter group: guard
- old value: {}
- new value: {"min_slot": "10:00"}
- command: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\scripts\full_loop_a_pullback_c2_high.py --max_iterations 200`
- FIT metrics: trades=1553, wins=522, losses=1031, win_rate=33.61%, PF=0.377, net=Rs -537,333, avg_win=Rs 623, avg_loss=Rs -837, SL/TGT/EOD=863/471/219, top_trade/day/symbol=0.0021/None/None
- VAL metrics: trades=1093, wins=313, losses=780, win_rate=28.64%, PF=0.2901, net=Rs -459,023, avg_win=Rs 599, avg_loss=Rs -829, SL/TGT/EOD=632/268/193, top_trade/day/symbol=0.0036/None/None
- full TRAIN metrics: trades=2646, wins=835, losses=1811, win_rate=31.56%, PF=0.3398, net=Rs -996,356, avg_win=Rs 614, avg_loss=Rs -833, SL/TGT/EOD=1495/739/412, top_trade/day/symbol=0.0013/None/None
- TEST metrics: not run
- keep/reject: REJECT
- failure classification: train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- next action: continue train-side search

## Iteration 041 - guard_min_slot10:30
- changed rule/parameter: entry guard/time-window/top_n sweep
- parameter group: guard
- old value: {}
- new value: {"min_slot": "10:30"}
- command: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\scripts\full_loop_a_pullback_c2_high.py --max_iterations 200`
- FIT metrics: trades=1553, wins=522, losses=1031, win_rate=33.61%, PF=0.377, net=Rs -537,333, avg_win=Rs 623, avg_loss=Rs -837, SL/TGT/EOD=863/471/219, top_trade/day/symbol=0.0021/None/None
- VAL metrics: trades=1093, wins=313, losses=780, win_rate=28.64%, PF=0.2901, net=Rs -459,023, avg_win=Rs 599, avg_loss=Rs -829, SL/TGT/EOD=632/268/193, top_trade/day/symbol=0.0036/None/None
- full TRAIN metrics: trades=2646, wins=835, losses=1811, win_rate=31.56%, PF=0.3398, net=Rs -996,356, avg_win=Rs 614, avg_loss=Rs -833, SL/TGT/EOD=1495/739/412, top_trade/day/symbol=0.0013/None/None
- TEST metrics: not run
- keep/reject: REJECT
- failure classification: train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- next action: continue train-side search

## Iteration 042 - guard_max_slot12:00
- changed rule/parameter: entry guard/time-window/top_n sweep
- parameter group: guard
- old value: {}
- new value: {"max_slot": "12:00"}
- command: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\scripts\full_loop_a_pullback_c2_high.py --max_iterations 200`
- FIT metrics: trades=618, wins=217, losses=401, win_rate=35.11%, PF=0.3903, net=Rs -215,657, avg_win=Rs 636, avg_loss=Rs -882, SL/TGT/EOD=371/206/41, top_trade/day/symbol=0.0048/None/None
- VAL metrics: trades=483, wins=158, losses=325, win_rate=32.71%, PF=0.3429, net=Rs -183,852, avg_win=Rs 607, avg_loss=Rs -861, SL/TGT/EOD=284/140/59, top_trade/day/symbol=0.007/None/None
- full TRAIN metrics: trades=1101, wins=375, losses=726, win_rate=34.06%, PF=0.3693, net=Rs -399,509, avg_win=Rs 624, avg_loss=Rs -873, SL/TGT/EOD=655/346/100, top_trade/day/symbol=0.0029/None/None
- TEST metrics: not run
- keep/reject: REJECT
- failure classification: train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- next action: continue train-side search

## Iteration 043 - guard_max_slot12:30
- changed rule/parameter: entry guard/time-window/top_n sweep
- parameter group: guard
- old value: {}
- new value: {"max_slot": "12:30"}
- command: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\scripts\full_loop_a_pullback_c2_high.py --max_iterations 200`
- FIT metrics: trades=844, wins=292, losses=552, win_rate=34.6%, PF=0.3865, net=Rs -294,276, avg_win=Rs 635, avg_loss=Rs -869, SL/TGT/EOD=498/275/71, top_trade/day/symbol=0.0036/None/None
- VAL metrics: trades=647, wins=204, losses=443, win_rate=31.53%, PF=0.33, net=Rs -251,946, avg_win=Rs 608, avg_loss=Rs -849, SL/TGT/EOD=378/179/90, top_trade/day/symbol=0.0054/None/None
- full TRAIN metrics: trades=1491, wins=496, losses=995, win_rate=33.27%, PF=0.3616, net=Rs -546,221, avg_win=Rs 624, avg_loss=Rs -860, SL/TGT/EOD=876/454/161, top_trade/day/symbol=0.0022/None/None
- TEST metrics: not run
- keep/reject: REJECT
- failure classification: train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- next action: continue train-side search

## Iteration 044 - guard_max_slot13:00
- changed rule/parameter: entry guard/time-window/top_n sweep
- parameter group: guard
- old value: {}
- new value: {"max_slot": "13:00"}
- command: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\scripts\full_loop_a_pullback_c2_high.py --max_iterations 200`
- FIT metrics: trades=1044, wins=359, losses=685, win_rate=34.39%, PF=0.3834, net=Rs -363,558, avg_win=Rs 630, avg_loss=Rs -861, SL/TGT/EOD=607/333/104, top_trade/day/symbol=0.0029/None/None
- VAL metrics: trades=785, wins=241, losses=544, win_rate=30.7%, PF=0.3148, net=Rs -316,498, avg_win=Rs 603, avg_loss=Rs -849, SL/TGT/EOD=462/209/114, top_trade/day/symbol=0.0046/None/None
- full TRAIN metrics: trades=1829, wins=600, losses=1229, win_rate=32.8%, PF=0.3533, net=Rs -680,056, avg_win=Rs 619, avg_loss=Rs -856, SL/TGT/EOD=1069/542/218, top_trade/day/symbol=0.0018/None/None
- TEST metrics: not run
- keep/reject: REJECT
- failure classification: train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- next action: continue train-side search

## Iteration 045 - guard_max_slot14:00
- changed rule/parameter: entry guard/time-window/top_n sweep
- parameter group: guard
- old value: {}
- new value: {"max_slot": "14:00"}
- command: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\scripts\full_loop_a_pullback_c2_high.py --max_iterations 200`
- FIT metrics: trades=1373, wins=468, losses=905, win_rate=34.09%, PF=0.3812, net=Rs -475,261, avg_win=Rs 625, avg_loss=Rs -849, SL/TGT/EOD=778/425/170, top_trade/day/symbol=0.0023/None/None
- VAL metrics: trades=990, wins=285, losses=705, win_rate=28.79%, PF=0.2907, net=Rs -421,262, avg_win=Rs 606, avg_loss=Rs -842, SL/TGT/EOD=588/249/153, top_trade/day/symbol=0.0039/None/None
- full TRAIN metrics: trades=2363, wins=753, losses=1610, win_rate=31.87%, PF=0.3417, net=Rs -896,522, avg_win=Rs 618, avg_loss=Rs -846, SL/TGT/EOD=1366/674/323, top_trade/day/symbol=0.0014/None/None
- TEST metrics: not run
- keep/reject: REJECT
- failure classification: train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- next action: continue train-side search

## Iteration 046 - guard_min_slot09:45_max_slot13:00
- changed rule/parameter: entry guard/time-window/top_n sweep
- parameter group: guard
- old value: {}
- new value: {"max_slot": "13:00", "min_slot": "09:45"}
- command: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\scripts\full_loop_a_pullback_c2_high.py --max_iterations 200`
- FIT metrics: trades=1044, wins=359, losses=685, win_rate=34.39%, PF=0.3834, net=Rs -363,558, avg_win=Rs 630, avg_loss=Rs -861, SL/TGT/EOD=607/333/104, top_trade/day/symbol=0.0029/None/None
- VAL metrics: trades=785, wins=241, losses=544, win_rate=30.7%, PF=0.3148, net=Rs -316,498, avg_win=Rs 603, avg_loss=Rs -849, SL/TGT/EOD=462/209/114, top_trade/day/symbol=0.0046/None/None
- full TRAIN metrics: trades=1829, wins=600, losses=1229, win_rate=32.8%, PF=0.3533, net=Rs -680,056, avg_win=Rs 619, avg_loss=Rs -856, SL/TGT/EOD=1069/542/218, top_trade/day/symbol=0.0018/None/None
- TEST metrics: not run
- keep/reject: REJECT
- failure classification: train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- next action: continue train-side search

## Iteration 047 - guard_min_slot10:00_max_slot14:00
- changed rule/parameter: entry guard/time-window/top_n sweep
- parameter group: guard
- old value: {}
- new value: {"max_slot": "14:00", "min_slot": "10:00"}
- command: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\scripts\full_loop_a_pullback_c2_high.py --max_iterations 200`
- FIT metrics: trades=1373, wins=468, losses=905, win_rate=34.09%, PF=0.3812, net=Rs -475,261, avg_win=Rs 625, avg_loss=Rs -849, SL/TGT/EOD=778/425/170, top_trade/day/symbol=0.0023/None/None
- VAL metrics: trades=990, wins=285, losses=705, win_rate=28.79%, PF=0.2907, net=Rs -421,262, avg_win=Rs 606, avg_loss=Rs -842, SL/TGT/EOD=588/249/153, top_trade/day/symbol=0.0039/None/None
- full TRAIN metrics: trades=2363, wins=753, losses=1610, win_rate=31.87%, PF=0.3417, net=Rs -896,522, avg_win=Rs 618, avg_loss=Rs -846, SL/TGT/EOD=1366/674/323, top_trade/day/symbol=0.0014/None/None
- TEST metrics: not run
- keep/reject: REJECT
- failure classification: train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- next action: continue train-side search

## Iteration 048 - guard_max_slot13:00_top_n1
- changed rule/parameter: entry guard/time-window/top_n sweep
- parameter group: guard
- old value: {}
- new value: {"max_slot": "13:00", "top_n": 1}
- command: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\scripts\full_loop_a_pullback_c2_high.py --max_iterations 200`
- FIT metrics: trades=447, wins=149, losses=298, win_rate=33.33%, PF=0.3668, net=Rs -163,037, avg_win=Rs 634, avg_loss=Rs -864, SL/TGT/EOD=266/137/44, top_trade/day/symbol=0.0071/None/None
- VAL metrics: trades=379, wins=107, losses=272, win_rate=28.23%, PF=0.2876, net=Rs -163,172, avg_win=Rs 616, avg_loss=Rs -842, SL/TGT/EOD=227/96/56, top_trade/day/symbol=0.0101/None/None
- full TRAIN metrics: trades=826, wins=256, losses=570, win_rate=30.99%, PF=0.3295, net=Rs -326,209, avg_win=Rs 626, avg_loss=Rs -854, SL/TGT/EOD=493/233/100, top_trade/day/symbol=0.0042/None/None
- TEST metrics: not run
- keep/reject: REJECT
- failure classification: train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- next action: continue train-side search

## Iteration 049 - guard_max_slot14:00_top_n2
- changed rule/parameter: entry guard/time-window/top_n sweep
- parameter group: guard
- old value: {}
- new value: {"max_slot": "14:00", "top_n": 2}
- command: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\scripts\full_loop_a_pullback_c2_high.py --max_iterations 200`
- FIT metrics: trades=1103, wins=378, losses=725, win_rate=34.27%, PF=0.3776, net=Rs -377,848, avg_win=Rs 606, avg_loss=Rs -837, SL/TGT/EOD=613/327/163, top_trade/day/symbol=0.0029/None/None
- VAL metrics: trades=828, wins=238, losses=590, win_rate=28.74%, PF=0.2938, net=Rs -348,290, avg_win=Rs 609, avg_loss=Rs -836, SL/TGT/EOD=484/209/135, top_trade/day/symbol=0.0046/None/None
- full TRAIN metrics: trades=1931, wins=616, losses=1315, win_rate=31.9%, PF=0.34, net=Rs -726,138, avg_win=Rs 607, avg_loss=Rs -837, SL/TGT/EOD=1097/536/298, top_trade/day/symbol=0.0018/None/None
- TEST metrics: not run
- keep/reject: REJECT
- failure classification: train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- next action: continue train-side search

## Iteration 050 - indicator_atr_pct>=0.001937
- changed rule/parameter: single indicator threshold sweep
- parameter group: indicator
- old value: none
- new value: ['atr_pct', '>=', 0.001937]
- command: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\scripts\full_loop_a_pullback_c2_high.py --max_iterations 200`
- FIT metrics: trades=1559, wins=527, losses=1032, win_rate=33.8%, PF=0.3777, net=Rs -542,582, avg_win=Rs 625, avg_loss=Rs -845, SL/TGT/EOD=883/480/196, top_trade/day/symbol=0.002/None/None
- VAL metrics: trades=1104, wins=307, losses=797, win_rate=27.81%, PF=0.2773, net=Rs -485,074, avg_win=Rs 606, avg_loss=Rs -842, SL/TGT/EOD=666/269/169, top_trade/day/symbol=0.0036/None/None
- full TRAIN metrics: trades=2663, wins=834, losses=1829, win_rate=31.32%, PF=0.334, net=Rs -1,027,657, avg_win=Rs 618, avg_loss=Rs -844, SL/TGT/EOD=1549/749/365, top_trade/day/symbol=0.0013/None/None
- TEST metrics: not run
- keep/reject: REJECT
- failure classification: train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- next action: continue train-side search

## Iteration 051 - indicator_quality_score>=29.113851
- changed rule/parameter: single indicator threshold sweep
- parameter group: indicator
- old value: none
- new value: ['quality_score', '>=', 29.113851]
- command: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\scripts\full_loop_a_pullback_c2_high.py --max_iterations 200`
- FIT metrics: trades=1496, wins=482, losses=1014, win_rate=32.22%, PF=0.349, net=Rs -554,319, avg_win=Rs 617, avg_loss=Rs -840, SL/TGT/EOD=855/429/212, top_trade/day/symbol=0.0022/None/None
- VAL metrics: trades=1048, wins=304, losses=744, win_rate=29.01%, PF=0.2942, net=Rs -435,067, avg_win=Rs 597, avg_loss=Rs -829, SL/TGT/EOD=605/260/183, top_trade/day/symbol=0.0037/None/None
- full TRAIN metrics: trades=2544, wins=786, losses=1758, win_rate=30.9%, PF=0.326, net=Rs -989,387, avg_win=Rs 609, avg_loss=Rs -835, SL/TGT/EOD=1460/689/395, top_trade/day/symbol=0.0014/None/None
- TEST metrics: not run
- keep/reject: REJECT
- failure classification: train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- next action: continue train-side search

## Iteration 052 - indicator_rs_pct>=-1.287037
- changed rule/parameter: single indicator threshold sweep
- parameter group: indicator
- old value: none
- new value: ['rs_pct', '>=', -1.287037]
- command: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\scripts\full_loop_a_pullback_c2_high.py --max_iterations 200`
- FIT metrics: trades=1485, wins=490, losses=995, win_rate=33.0%, PF=0.3698, net=Rs -519,467, avg_win=Rs 622, avg_loss=Rs -828, SL/TGT/EOD=820/443/222, top_trade/day/symbol=0.0022/None/None
- VAL metrics: trades=1047, wins=306, losses=741, win_rate=29.23%, PF=0.2958, net=Rs -431,478, avg_win=Rs 592, avg_loss=Rs -827, SL/TGT/EOD=596/256/195, top_trade/day/symbol=0.0037/None/None
- full TRAIN metrics: trades=2532, wins=796, losses=1736, win_rate=31.44%, PF=0.3382, net=Rs -950,945, avg_win=Rs 611, avg_loss=Rs -828, SL/TGT/EOD=1416/699/417, top_trade/day/symbol=0.0014/None/None
- TEST metrics: not run
- keep/reject: REJECT
- failure classification: train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- next action: continue train-side search

## Iteration 053 - indicator_vwap_dist_atr>=0.822683
- changed rule/parameter: single indicator threshold sweep
- parameter group: indicator
- old value: none
- new value: ['vwap_dist_atr', '>=', 0.822683]
- command: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\scripts\full_loop_a_pullback_c2_high.py --max_iterations 200`
- FIT metrics: trades=1487, wins=500, losses=987, win_rate=33.62%, PF=0.3736, net=Rs -516,794, avg_win=Rs 617, avg_loss=Rs -836, SL/TGT/EOD=827/445/215, top_trade/day/symbol=0.0022/None/None
- VAL metrics: trades=1049, wins=294, losses=755, win_rate=28.03%, PF=0.28, net=Rs -453,524, avg_win=Rs 600, avg_loss=Rs -834, SL/TGT/EOD=613/252/184, top_trade/day/symbol=0.0038/None/None
- full TRAIN metrics: trades=2536, wins=794, losses=1742, win_rate=31.31%, PF=0.3331, net=Rs -970,318, avg_win=Rs 610, avg_loss=Rs -835, SL/TGT/EOD=1440/697/399, top_trade/day/symbol=0.0014/None/None
- TEST metrics: not run
- keep/reject: REJECT
- failure classification: train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- next action: continue train-side search

## Iteration 054 - indicator_atr_pct<=0.001937
- changed rule/parameter: single indicator threshold sweep
- parameter group: indicator
- old value: none
- new value: ['atr_pct', '<=', 0.001937]
- command: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\scripts\full_loop_a_pullback_c2_high.py --max_iterations 200`
- FIT metrics: trades=352, wins=82, losses=270, win_rate=23.3%, PF=0.2312, net=Rs -141,264, avg_win=Rs 518, avg_loss=Rs -681, SL/TGT/EOD=148/55/149, top_trade/day/symbol=0.0157/None/None
- VAL metrics: trades=361, wins=92, losses=269, win_rate=25.48%, PF=0.2524, net=Rs -133,800, avg_win=Rs 491, avg_loss=Rs -665, SL/TGT/EOD=134/56/171, top_trade/day/symbol=0.0148/None/None
- full TRAIN metrics: trades=713, wins=174, losses=539, win_rate=24.4%, PF=0.2416, net=Rs -275,064, avg_win=Rs 504, avg_loss=Rs -673, SL/TGT/EOD=282/111/320, top_trade/day/symbol=0.0076/None/None
- TEST metrics: not run
- keep/reject: REJECT
- failure classification: train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- next action: continue train-side search

## Iteration 055 - indicator_quality_score<=29.113851
- changed rule/parameter: single indicator threshold sweep
- parameter group: indicator
- old value: none
- new value: ['quality_score', '<=', 29.113851]
- command: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\scripts\full_loop_a_pullback_c2_high.py --max_iterations 200`
- FIT metrics: trades=264, wins=112, losses=152, win_rate=42.42%, PF=0.5722, net=Rs -51,208, avg_win=Rs 612, avg_loss=Rs -788, SL/TGT/EOD=113/98/53, top_trade/day/symbol=0.0126/None/None
- VAL metrics: trades=331, wins=77, losses=254, win_rate=23.26%, PF=0.2277, net=Rs -146,055, avg_win=Rs 559, avg_loss=Rs -745, SL/TGT/EOD=168/62/101, top_trade/day/symbol=0.0155/None/None
- full TRAIN metrics: trades=595, wins=189, losses=406, win_rate=31.76%, PF=0.3613, net=Rs -197,263, avg_win=Rs 590, avg_loss=Rs -761, SL/TGT/EOD=281/160/154, top_trade/day/symbol=0.0077/None/None
- TEST metrics: not run
- keep/reject: REJECT
- failure classification: train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- next action: continue train-side search

## Iteration 056 - indicator_rs_pct<=-1.287037
- changed rule/parameter: single indicator threshold sweep
- parameter group: indicator
- old value: none
- new value: ['rs_pct', '<=', -1.287037]
- command: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\scripts\full_loop_a_pullback_c2_high.py --max_iterations 200`
- FIT metrics: trades=384, wins=145, losses=239, win_rate=37.76%, PF=0.4387, net=Rs -112,053, avg_win=Rs 604, avg_loss=Rs -835, SL/TGT/EOD=200/124/60, top_trade/day/symbol=0.0076/None/None
- VAL metrics: trades=236, wins=67, losses=169, win_rate=28.39%, PF=0.286, net=Rs -97,328, avg_win=Rs 582, avg_loss=Rs -807, SL/TGT/EOD=130/55/51, top_trade/day/symbol=0.0171/None/None
- full TRAIN metrics: trades=620, wins=212, losses=408, win_rate=34.19%, PF=0.3767, net=Rs -209,380, avg_win=Rs 597, avg_loss=Rs -823, SL/TGT/EOD=330/179/111, top_trade/day/symbol=0.0053/None/None
- TEST metrics: not run
- keep/reject: REJECT
- failure classification: train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- next action: continue train-side search

## Iteration 057 - indicator_vwap_dist_atr<=0.822683
- changed rule/parameter: single indicator threshold sweep
- parameter group: indicator
- old value: none
- new value: ['vwap_dist_atr', '<=', 0.822683]
- command: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\scripts\full_loop_a_pullback_c2_high.py --max_iterations 200`
- FIT metrics: trades=363, wins=129, losses=234, win_rate=35.54%, PF=0.4189, net=Rs -108,812, avg_win=Rs 608, avg_loss=Rs -800, SL/TGT/EOD=180/112/71, top_trade/day/symbol=0.0085/None/None
- VAL metrics: trades=353, wins=97, losses=256, win_rate=27.48%, PF=0.2618, net=Rs -143,846, avg_win=Rs 526, avg_loss=Rs -761, SL/TGT/EOD=177/69/107, top_trade/day/symbol=0.0131/None/None
- full TRAIN metrics: trades=716, wins=226, losses=490, win_rate=31.56%, PF=0.3388, net=Rs -252,658, avg_win=Rs 573, avg_loss=Rs -780, SL/TGT/EOD=357/181/178, top_trade/day/symbol=0.0052/None/None
- TEST metrics: not run
- keep/reject: REJECT
- failure classification: train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- next action: continue train-side search

## Iteration 058 - indicator_atr_pct>=0.002228
- changed rule/parameter: single indicator threshold sweep
- parameter group: indicator
- old value: none
- new value: ['atr_pct', '>=', 0.002228]
- command: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\scripts\full_loop_a_pullback_c2_high.py --max_iterations 200`
- FIT metrics: trades=1519, wins=519, losses=1000, win_rate=34.17%, PF=0.3823, net=Rs -523,913, avg_win=Rs 625, avg_loss=Rs -848, SL/TGT/EOD=863/473/183, top_trade/day/symbol=0.0021/None/None
- VAL metrics: trades=1063, wins=306, losses=757, win_rate=28.79%, PF=0.2921, net=Rs -452,015, avg_win=Rs 610, avg_loss=Rs -844, SL/TGT/EOD=632/271/160, top_trade/day/symbol=0.0036/None/None
- full TRAIN metrics: trades=2582, wins=825, losses=1757, win_rate=31.95%, PF=0.3435, net=Rs -975,928, avg_win=Rs 619, avg_loss=Rs -846, SL/TGT/EOD=1495/744/343, top_trade/day/symbol=0.0013/None/None
- TEST metrics: not run
- keep/reject: REJECT
- failure classification: train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- next action: continue train-side search

## Iteration 059 - indicator_quality_score>=39.707469
- changed rule/parameter: single indicator threshold sweep
- parameter group: indicator
- old value: none
- new value: ['quality_score', '>=', 39.707469]
- command: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\scripts\full_loop_a_pullback_c2_high.py --max_iterations 200`
- FIT metrics: trades=1454, wins=465, losses=989, win_rate=31.98%, PF=0.3441, net=Rs -546,364, avg_win=Rs 616, avg_loss=Rs -842, SL/TGT/EOD=832/414/208, top_trade/day/symbol=0.0023/None/None
- VAL metrics: trades=1003, wins=289, losses=714, win_rate=28.81%, PF=0.2932, net=Rs -419,310, avg_win=Rs 602, avg_loss=Rs -831, SL/TGT/EOD=582/247/174, top_trade/day/symbol=0.0038/None/None
- full TRAIN metrics: trades=2457, wins=754, losses=1703, win_rate=30.69%, PF=0.3229, net=Rs -965,674, avg_win=Rs 611, avg_loss=Rs -838, SL/TGT/EOD=1414/661/382, top_trade/day/symbol=0.0014/None/None
- TEST metrics: not run
- keep/reject: REJECT
- failure classification: train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- next action: continue train-side search

## Iteration 060 - indicator_rs_pct>=-0.863904
- changed rule/parameter: single indicator threshold sweep
- parameter group: indicator
- old value: none
- new value: ['rs_pct', '>=', -0.863904]
- command: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\scripts\full_loop_a_pullback_c2_high.py --max_iterations 200`
- FIT metrics: trades=1451, wins=471, losses=980, win_rate=32.46%, PF=0.3641, net=Rs -515,327, avg_win=Rs 626, avg_loss=Rs -827, SL/TGT/EOD=802/430/219, top_trade/day/symbol=0.0023/None/None
- VAL metrics: trades=1018, wins=298, losses=720, win_rate=29.27%, PF=0.2972, net=Rs -414,452, avg_win=Rs 588, avg_loss=Rs -819, SL/TGT/EOD=569/248/201, top_trade/day/symbol=0.0038/None/None
- full TRAIN metrics: trades=2469, wins=769, losses=1700, win_rate=31.15%, PF=0.3359, net=Rs -929,779, avg_win=Rs 612, avg_loss=Rs -824, SL/TGT/EOD=1371/678/420, top_trade/day/symbol=0.0014/None/None
- TEST metrics: not run
- keep/reject: REJECT
- failure classification: train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- next action: continue train-side search

## Iteration 061 - indicator_vwap_dist_atr>=1.362123
- changed rule/parameter: single indicator threshold sweep
- parameter group: indicator
- old value: none
- new value: ['vwap_dist_atr', '>=', 1.362123]
- command: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\scripts\full_loop_a_pullback_c2_high.py --max_iterations 200`
- FIT metrics: trades=1406, wins=470, losses=936, win_rate=33.43%, PF=0.3722, net=Rs -488,193, avg_win=Rs 616, avg_loss=Rs -831, SL/TGT/EOD=775/417/214, top_trade/day/symbol=0.0023/None/None
- VAL metrics: trades=1008, wins=283, losses=725, win_rate=28.08%, PF=0.2809, net=Rs -433,162, avg_win=Rs 598, avg_loss=Rs -831, SL/TGT/EOD=587/240/181, top_trade/day/symbol=0.0039/None/None
- full TRAIN metrics: trades=2414, wins=753, losses=1661, win_rate=31.19%, PF=0.3323, net=Rs -921,354, avg_win=Rs 609, avg_loss=Rs -831, SL/TGT/EOD=1362/657/395, top_trade/day/symbol=0.0015/None/None
- TEST metrics: not run
- keep/reject: REJECT
- failure classification: train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- next action: continue train-side search

## Iteration 062 - indicator_atr_pct<=0.002228
- changed rule/parameter: single indicator threshold sweep
- parameter group: indicator
- old value: none
- new value: ['atr_pct', '<=', 0.002228]
- command: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\scripts\full_loop_a_pullback_c2_high.py --max_iterations 200`
- FIT metrics: trades=541, wins=153, losses=388, win_rate=28.28%, PF=0.3009, net=Rs -194,313, avg_win=Rs 547, avg_loss=Rs -716, SL/TGT/EOD=235/110/196, top_trade/day/symbol=0.0103/None/None
- VAL metrics: trades=489, wins=112, losses=377, win_rate=22.9%, PF=0.2205, net=Rs -204,658, avg_win=Rs 517, avg_loss=Rs -696, SL/TGT/EOD=212/75/202, top_trade/day/symbol=0.0115/None/None
- full TRAIN metrics: trades=1030, wins=265, losses=765, win_rate=25.73%, PF=0.2619, net=Rs -398,971, avg_win=Rs 534, avg_loss=Rs -707, SL/TGT/EOD=447/185/398, top_trade/day/symbol=0.0061/None/None
- TEST metrics: not run
- keep/reject: REJECT
- failure classification: train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- next action: continue train-side search

## Iteration 063 - indicator_quality_score<=39.707469
- changed rule/parameter: single indicator threshold sweep
- parameter group: indicator
- old value: none
- new value: ['quality_score', '<=', 39.707469]
- command: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\scripts\full_loop_a_pullback_c2_high.py --max_iterations 200`
- FIT metrics: trades=469, wins=185, losses=284, win_rate=39.45%, PF=0.5053, net=Rs -109,309, avg_win=Rs 603, avg_loss=Rs -778, SL/TGT/EOD=213/159/97, top_trade/day/symbol=0.006/None/None
- VAL metrics: trades=473, wins=118, losses=355, win_rate=24.95%, PF=0.2538, net=Rs -195,897, avg_win=Rs 565, avg_loss=Rs -740, SL/TGT/EOD=238/96/139, top_trade/day/symbol=0.01/None/None
- full TRAIN metrics: trades=942, wins=303, losses=639, win_rate=32.17%, PF=0.3687, net=Rs -305,206, avg_win=Rs 588, avg_loss=Rs -757, SL/TGT/EOD=451/255/236, top_trade/day/symbol=0.0037/None/None
- TEST metrics: not run
- keep/reject: REJECT
- failure classification: train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- next action: continue train-side search

## Iteration 064 - indicator_rs_pct<=-0.863904
- changed rule/parameter: single indicator threshold sweep
- parameter group: indicator
- old value: none
- new value: ['rs_pct', '<=', -0.863904]
- command: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\scripts\full_loop_a_pullback_c2_high.py --max_iterations 200`
- FIT metrics: trades=548, wins=213, losses=335, win_rate=38.87%, PF=0.4564, net=Rs -151,168, avg_win=Rs 596, avg_loss=Rs -830, SL/TGT/EOD=278/177/93, top_trade/day/symbol=0.0053/None/None
- VAL metrics: trades=370, wins=94, losses=276, win_rate=25.41%, PF=0.2456, net=Rs -168,278, avg_win=Rs 583, avg_loss=Rs -808, SL/TGT/EOD=214/78/78, top_trade/day/symbol=0.0122/None/None
- full TRAIN metrics: trades=918, wins=307, losses=611, win_rate=33.44%, PF=0.3626, net=Rs -319,446, avg_win=Rs 592, avg_loss=Rs -820, SL/TGT/EOD=492/255/171, top_trade/day/symbol=0.0037/None/None
- TEST metrics: not run
- keep/reject: REJECT
- failure classification: train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- next action: continue train-side search

## Iteration 065 - indicator_vwap_dist_atr<=1.362123
- changed rule/parameter: single indicator threshold sweep
- parameter group: indicator
- old value: none
- new value: ['vwap_dist_atr', '<=', 1.362123]
- command: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\scripts\full_loop_a_pullback_c2_high.py --max_iterations 200`
- FIT metrics: trades=600, wins=211, losses=389, win_rate=35.17%, PF=0.401, net=Rs -187,067, avg_win=Rs 593, avg_loss=Rs -803, SL/TGT/EOD=300/177/123, top_trade/day/symbol=0.0053/None/None
- VAL metrics: trades=530, wins=136, losses=394, win_rate=25.66%, PF=0.2448, net=Rs -232,130, avg_win=Rs 553, avg_loss=Rs -780, SL/TGT/EOD=281/105/144, top_trade/day/symbol=0.0089/None/None
- full TRAIN metrics: trades=1130, wins=347, losses=783, win_rate=30.71%, PF=0.3235, net=Rs -419,197, avg_win=Rs 578, avg_loss=Rs -791, SL/TGT/EOD=581/282/267, top_trade/day/symbol=0.0033/None/None
- TEST metrics: not run
- keep/reject: REJECT
- failure classification: train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- next action: continue train-side search

## Iteration 066 - non_indicator_price_action_body_pct>=0.620145
- changed rule/parameter: single non_indicator_price_action threshold sweep
- parameter group: non_indicator_price_action
- old value: none
- new value: ['body_pct', '>=', 0.620145]
- command: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\scripts\full_loop_a_pullback_c2_high.py --max_iterations 200`
- FIT metrics: trades=1489, wins=502, losses=987, win_rate=33.71%, PF=0.3743, net=Rs -516,199, avg_win=Rs 615, avg_loss=Rs -836, SL/TGT/EOD=827/447/215, top_trade/day/symbol=0.0022/None/None
- VAL metrics: trades=1032, wins=293, losses=739, win_rate=28.39%, PF=0.2868, net=Rs -432,311, avg_win=Rs 593, avg_loss=Rs -820, SL/TGT/EOD=589/249/194, top_trade/day/symbol=0.0038/None/None
- full TRAIN metrics: trades=2521, wins=795, losses=1726, win_rate=31.54%, PF=0.3372, net=Rs -948,510, avg_win=Rs 607, avg_loss=Rs -829, SL/TGT/EOD=1416/696/409, top_trade/day/symbol=0.0014/None/None
- TEST metrics: not run
- keep/reject: REJECT
- failure classification: train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- next action: continue train-side search

## Iteration 067 - non_indicator_price_action_close_loc>=0.731712
- changed rule/parameter: single non_indicator_price_action threshold sweep
- parameter group: non_indicator_price_action
- old value: none
- new value: ['close_loc', '>=', 0.731712]
- command: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\scripts\full_loop_a_pullback_c2_high.py --max_iterations 200`
- FIT metrics: trades=1463, wins=497, losses=966, win_rate=33.97%, PF=0.3825, net=Rs -496,653, avg_win=Rs 619, avg_loss=Rs -833, SL/TGT/EOD=805/445/213, top_trade/day/symbol=0.0022/None/None
- VAL metrics: trades=1008, wins=277, losses=731, win_rate=27.48%, PF=0.2737, net=Rs -433,786, avg_win=Rs 590, avg_loss=Rs -817, SL/TGT/EOD=578/232/198, top_trade/day/symbol=0.0041/None/None
- full TRAIN metrics: trades=2471, wins=774, losses=1697, win_rate=31.32%, PF=0.3361, net=Rs -930,439, avg_win=Rs 609, avg_loss=Rs -826, SL/TGT/EOD=1383/677/411, top_trade/day/symbol=0.0014/None/None
- TEST metrics: not run
- keep/reject: REJECT
- failure classification: train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- next action: continue train-side search

## Iteration 068 - non_indicator_price_action_lower_wick_pct>=0.0
- changed rule/parameter: single non_indicator_price_action threshold sweep
- parameter group: non_indicator_price_action
- old value: none
- new value: ['lower_wick_pct', '>=', 0.0]
- command: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\scripts\full_loop_a_pullback_c2_high.py --max_iterations 200`
- FIT metrics: trades=1553, wins=522, losses=1031, win_rate=33.61%, PF=0.377, net=Rs -537,333, avg_win=Rs 623, avg_loss=Rs -837, SL/TGT/EOD=863/471/219, top_trade/day/symbol=0.0021/None/None
- VAL metrics: trades=1093, wins=313, losses=780, win_rate=28.64%, PF=0.2901, net=Rs -459,023, avg_win=Rs 599, avg_loss=Rs -829, SL/TGT/EOD=632/268/193, top_trade/day/symbol=0.0036/None/None
- full TRAIN metrics: trades=2646, wins=835, losses=1811, win_rate=31.56%, PF=0.3398, net=Rs -996,356, avg_win=Rs 614, avg_loss=Rs -833, SL/TGT/EOD=1495/739/412, top_trade/day/symbol=0.0013/None/None
- TEST metrics: not run
- keep/reject: REJECT
- failure classification: train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- next action: continue train-side search

## Iteration 069 - non_indicator_price_action_signal_range_pct>=0.322068
- changed rule/parameter: single non_indicator_price_action threshold sweep
- parameter group: non_indicator_price_action
- old value: none
- new value: ['signal_range_pct', '>=', 0.322068]
- command: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\scripts\full_loop_a_pullback_c2_high.py --max_iterations 200`
- FIT metrics: trades=1524, wins=516, losses=1008, win_rate=33.86%, PF=0.378, net=Rs -527,756, avg_win=Rs 622, avg_loss=Rs -842, SL/TGT/EOD=856/465/203, top_trade/day/symbol=0.0021/None/None
- VAL metrics: trades=1099, wins=313, losses=786, win_rate=28.48%, PF=0.2833, net=Rs -471,825, avg_win=Rs 596, avg_loss=Rs -838, SL/TGT/EOD=645/267/187, top_trade/day/symbol=0.0036/None/None
- full TRAIN metrics: trades=2623, wins=829, losses=1794, win_rate=31.61%, PF=0.3366, net=Rs -999,581, avg_win=Rs 612, avg_loss=Rs -840, SL/TGT/EOD=1501/732/390, top_trade/day/symbol=0.0013/None/None
- TEST metrics: not run
- keep/reject: REJECT
- failure classification: train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- next action: continue train-side search

## Iteration 070 - non_indicator_price_action_upper_wick_pct>=0.0
- changed rule/parameter: single non_indicator_price_action threshold sweep
- parameter group: non_indicator_price_action
- old value: none
- new value: ['upper_wick_pct', '>=', 0.0]
- command: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\scripts\full_loop_a_pullback_c2_high.py --max_iterations 200`
- FIT metrics: trades=1553, wins=522, losses=1031, win_rate=33.61%, PF=0.377, net=Rs -537,333, avg_win=Rs 623, avg_loss=Rs -837, SL/TGT/EOD=863/471/219, top_trade/day/symbol=0.0021/None/None
- VAL metrics: trades=1093, wins=313, losses=780, win_rate=28.64%, PF=0.2901, net=Rs -459,023, avg_win=Rs 599, avg_loss=Rs -829, SL/TGT/EOD=632/268/193, top_trade/day/symbol=0.0036/None/None
- full TRAIN metrics: trades=2646, wins=835, losses=1811, win_rate=31.56%, PF=0.3398, net=Rs -996,356, avg_win=Rs 614, avg_loss=Rs -833, SL/TGT/EOD=1495/739/412, top_trade/day/symbol=0.0013/None/None
- TEST metrics: not run
- keep/reject: REJECT
- failure classification: train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- next action: continue train-side search

## Iteration 071 - non_indicator_price_action_vol_ratio>=1.783383
- changed rule/parameter: single non_indicator_price_action threshold sweep
- parameter group: non_indicator_price_action
- old value: none
- new value: ['vol_ratio', '>=', 1.783383]
- command: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\scripts\full_loop_a_pullback_c2_high.py --max_iterations 200`
- FIT metrics: trades=1489, wins=483, losses=1006, win_rate=32.44%, PF=0.3558, net=Rs -540,827, avg_win=Rs 618, avg_loss=Rs -834, SL/TGT/EOD=837/433/219, top_trade/day/symbol=0.0022/None/None
- VAL metrics: trades=1048, wins=297, losses=751, win_rate=28.34%, PF=0.2829, net=Rs -447,101, avg_win=Rs 594, avg_loss=Rs -830, SL/TGT/EOD=607/250/191, top_trade/day/symbol=0.0038/None/None
- full TRAIN metrics: trades=2537, wins=780, losses=1757, win_rate=30.74%, PF=0.3247, net=Rs -987,929, avg_win=Rs 609, avg_loss=Rs -833, SL/TGT/EOD=1444/683/410, top_trade/day/symbol=0.0014/None/None
- TEST metrics: not run
- keep/reject: REJECT
- failure classification: train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- next action: continue train-side search

## Iteration 072 - non_indicator_price_action_wick_skew_pct>=-0.072404
- changed rule/parameter: single non_indicator_price_action threshold sweep
- parameter group: non_indicator_price_action
- old value: none
- new value: ['wick_skew_pct', '>=', -0.072404]
- command: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\scripts\full_loop_a_pullback_c2_high.py --max_iterations 200`
- FIT metrics: trades=1460, wins=484, losses=976, win_rate=33.15%, PF=0.3701, net=Rs -507,500, avg_win=Rs 616, avg_loss=Rs -825, SL/TGT/EOD=804/429/227, top_trade/day/symbol=0.0022/None/None
- VAL metrics: trades=1015, wins=294, losses=721, win_rate=28.97%, PF=0.2952, net=Rs -421,361, avg_win=Rs 600, avg_loss=Rs -829, SL/TGT/EOD=582/253/180, top_trade/day/symbol=0.0038/None/None
- full TRAIN metrics: trades=2475, wins=778, losses=1697, win_rate=31.43%, PF=0.3382, net=Rs -928,862, avg_win=Rs 610, avg_loss=Rs -827, SL/TGT/EOD=1386/682/407, top_trade/day/symbol=0.0014/None/None
- TEST metrics: not run
- keep/reject: REJECT
- failure classification: train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- next action: continue train-side search

## Iteration 073 - non_indicator_price_action_body_pct<=0.620145
- changed rule/parameter: single non_indicator_price_action threshold sweep
- parameter group: non_indicator_price_action
- old value: none
- new value: ['body_pct', '<=', 0.620145]
- command: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\scripts\full_loop_a_pullback_c2_high.py --max_iterations 200`
- FIT metrics: trades=415, wins=122, losses=293, win_rate=29.4%, PF=0.3175, net=Rs -155,623, avg_win=Rs 593, avg_loss=Rs -778, SL/TGT/EOD=210/103/102, top_trade/day/symbol=0.0092/None/None
- VAL metrics: trades=311, wins=87, losses=224, win_rate=27.97%, PF=0.2792, net=Rs -123,298, avg_win=Rs 549, avg_loss=Rs -764, SL/TGT/EOD=154/64/93, top_trade/day/symbol=0.014/None/None
- full TRAIN metrics: trades=726, wins=209, losses=517, win_rate=28.79%, PF=0.3011, net=Rs -278,920, avg_win=Rs 575, avg_loss=Rs -772, SL/TGT/EOD=364/167/195, top_trade/day/symbol=0.0056/None/None
- TEST metrics: not run
- keep/reject: REJECT
- failure classification: train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- next action: continue train-side search

## Iteration 074 - non_indicator_price_action_close_loc<=0.731712
- changed rule/parameter: single non_indicator_price_action threshold sweep
- parameter group: non_indicator_price_action
- old value: none
- new value: ['close_loc', '<=', 0.731712]
- command: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\scripts\full_loop_a_pullback_c2_high.py --max_iterations 200`
- FIT metrics: trades=417, wins=116, losses=301, win_rate=27.82%, PF=0.294, net=Rs -168,536, avg_win=Rs 605, avg_loss=Rs -793, SL/TGT/EOD=223/100/94, top_trade/day/symbol=0.0095/None/None
- VAL metrics: trades=298, wins=94, losses=204, win_rate=31.54%, PF=0.3338, net=Rs -108,420, avg_win=Rs 578, avg_loss=Rs -798, SL/TGT/EOD=155/76/67, top_trade/day/symbol=0.0123/None/None
- full TRAIN metrics: trades=715, wins=210, losses=505, win_rate=29.37%, PF=0.3102, net=Rs -276,956, avg_win=Rs 593, avg_loss=Rs -795, SL/TGT/EOD=378/176/161, top_trade/day/symbol=0.0054/None/None
- TEST metrics: not run
- keep/reject: REJECT
- failure classification: train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- next action: continue train-side search

## Iteration 075 - non_indicator_price_action_lower_wick_pct<=0.0
- changed rule/parameter: single non_indicator_price_action threshold sweep
- parameter group: non_indicator_price_action
- old value: none
- new value: ['lower_wick_pct', '<=', 0.0]
- command: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\scripts\full_loop_a_pullback_c2_high.py --max_iterations 200`
- FIT metrics: trades=958, wins=304, losses=654, win_rate=31.73%, PF=0.3522, net=Rs -346,025, avg_win=Rs 619, avg_loss=Rs -817, SL/TGT/EOD=524/271/163, top_trade/day/symbol=0.0046/None/None
- VAL metrics: trades=653, wins=173, losses=480, win_rate=26.49%, PF=0.2596, net=Rs -282,969, avg_win=Rs 574, avg_loss=Rs -796, SL/TGT/EOD=367/139/147, top_trade/day/symbol=0.0067/None/None
- full TRAIN metrics: trades=1611, wins=477, losses=1134, win_rate=29.61%, PF=0.3136, net=Rs -628,994, avg_win=Rs 602, avg_loss=Rs -808, SL/TGT/EOD=891/410/310, top_trade/day/symbol=0.003/None/None
- TEST metrics: not run
- keep/reject: REJECT
- failure classification: train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- next action: continue train-side search

## Iteration 076 - non_indicator_price_action_signal_range_pct<=0.322068
- changed rule/parameter: single non_indicator_price_action threshold sweep
- parameter group: non_indicator_price_action
- old value: none
- new value: ['signal_range_pct', '<=', 0.322068]
- command: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\scripts\full_loop_a_pullback_c2_high.py --max_iterations 200`
- FIT metrics: trades=377, wins=116, losses=261, win_rate=30.77%, PF=0.3474, net=Rs -121,126, avg_win=Rs 556, avg_loss=Rs -711, SL/TGT/EOD=159/88/130, top_trade/day/symbol=0.0103/None/None
- VAL metrics: trades=316, wins=78, losses=238, win_rate=24.68%, PF=0.2484, net=Rs -124,357, avg_win=Rs 527, avg_loss=Rs -695, SL/TGT/EOD=140/54/122, top_trade/day/symbol=0.0162/None/None
- full TRAIN metrics: trades=693, wins=194, losses=499, win_rate=27.99%, PF=0.3007, net=Rs -245,482, avg_win=Rs 544, avg_loss=Rs -704, SL/TGT/EOD=299/142/252, top_trade/day/symbol=0.0063/None/None
- TEST metrics: not run
- keep/reject: REJECT
- failure classification: train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- next action: continue train-side search

## Iteration 077 - non_indicator_price_action_upper_wick_pct<=0.0
- changed rule/parameter: single non_indicator_price_action threshold sweep
- parameter group: non_indicator_price_action
- old value: none
- new value: ['upper_wick_pct', '<=', 0.0]
- command: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\scripts\full_loop_a_pullback_c2_high.py --max_iterations 200`
- FIT metrics: trades=658, wins=220, losses=438, win_rate=33.43%, PF=0.3803, net=Rs -221,078, avg_win=Rs 617, avg_loss=Rs -814, SL/TGT/EOD=351/197/110, top_trade/day/symbol=0.0049/None/None
- VAL metrics: trades=463, wins=120, losses=343, win_rate=25.92%, PF=0.2464, net=Rs -203,088, avg_win=Rs 553, avg_loss=Rs -786, SL/TGT/EOD=254/92/117, top_trade/day/symbol=0.01/None/None
- full TRAIN metrics: trades=1121, wins=340, losses=781, win_rate=30.33%, PF=0.3227, net=Rs -424,166, avg_win=Rs 594, avg_loss=Rs -802, SL/TGT/EOD=605/289/227, top_trade/day/symbol=0.0033/None/None
- TEST metrics: not run
- keep/reject: REJECT
- failure classification: train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- next action: continue train-side search

## Iteration 078 - non_indicator_price_action_vol_ratio<=1.783383
- changed rule/parameter: single non_indicator_price_action threshold sweep
- parameter group: non_indicator_price_action
- old value: none
- new value: ['vol_ratio', '<=', 1.783383]
- command: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\scripts\full_loop_a_pullback_c2_high.py --max_iterations 200`
- FIT metrics: trades=399, wins=150, losses=249, win_rate=37.59%, PF=0.4782, net=Rs -100,095, avg_win=Rs 611, avg_loss=Rs -770, SL/TGT/EOD=187/132/80, top_trade/day/symbol=0.0073/None/None
- VAL metrics: trades=257, wins=78, losses=179, win_rate=30.35%, PF=0.3135, net=Rs -92,105, avg_win=Rs 539, avg_loss=Rs -749, SL/TGT/EOD=119/59/79, top_trade/day/symbol=0.0159/None/None
- full TRAIN metrics: trades=656, wins=228, losses=428, win_rate=34.76%, PF=0.4104, net=Rs -192,200, avg_win=Rs 587, avg_loss=Rs -762, SL/TGT/EOD=306/191/159, top_trade/day/symbol=0.005/None/None
- TEST metrics: not run
- keep/reject: REJECT
- failure classification: train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- next action: continue train-side search

## Iteration 079 - non_indicator_price_action_wick_skew_pct<=-0.072404
- changed rule/parameter: single non_indicator_price_action threshold sweep
- parameter group: non_indicator_price_action
- old value: none
- new value: ['wick_skew_pct', '<=', -0.072404]
- command: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\scripts\full_loop_a_pullback_c2_high.py --max_iterations 200`
- FIT metrics: trades=438, wins=148, losses=290, win_rate=33.79%, PF=0.3662, net=Rs -154,193, avg_win=Rs 602, avg_loss=Rs -839, SL/TGT/EOD=241/127/70, top_trade/day/symbol=0.0075/None/None
- VAL metrics: trades=282, wins=75, losses=207, win_rate=26.6%, PF=0.2711, net=Rs -120,131, avg_win=Rs 596, avg_loss=Rs -796, SL/TGT/EOD=159/64/59, top_trade/day/symbol=0.0149/None/None
- full TRAIN metrics: trades=720, wins=223, losses=497, win_rate=30.97%, PF=0.3278, net=Rs -274,324, avg_win=Rs 600, avg_loss=Rs -821, SL/TGT/EOD=400/191/129, top_trade/day/symbol=0.005/None/None
- TEST metrics: not run
- keep/reject: REJECT
- failure classification: train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- next action: continue train-side search

## Iteration 080 - non_indicator_price_action_body_pct>=0.677425
- changed rule/parameter: single non_indicator_price_action threshold sweep
- parameter group: non_indicator_price_action
- old value: none
- new value: ['body_pct', '>=', 0.677425]
- command: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\scripts\full_loop_a_pullback_c2_high.py --max_iterations 200`
- FIT metrics: trades=1430, wins=495, losses=935, win_rate=34.62%, PF=0.3943, net=Rs -471,032, avg_win=Rs 620, avg_loss=Rs -832, SL/TGT/EOD=778/445/207, top_trade/day/symbol=0.0022/None/None
- VAL metrics: trades=987, wins=273, losses=714, win_rate=27.66%, PF=0.2744, net=Rs -426,165, avg_win=Rs 590, avg_loss=Rs -823, SL/TGT/EOD=568/231/188, top_trade/day/symbol=0.0041/None/None
- full TRAIN metrics: trades=2417, wins=768, losses=1649, win_rate=31.77%, PF=0.3427, net=Rs -897,196, avg_win=Rs 609, avg_loss=Rs -828, SL/TGT/EOD=1346/676/395, top_trade/day/symbol=0.0014/None/None
- TEST metrics: not run
- keep/reject: REJECT
- failure classification: train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- next action: continue train-side search

## Iteration 081 - non_indicator_price_action_close_loc>=0.787234
- changed rule/parameter: single non_indicator_price_action threshold sweep
- parameter group: non_indicator_price_action
- old value: none
- new value: ['close_loc', '>=', 0.787234]
- command: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\scripts\full_loop_a_pullback_c2_high.py --max_iterations 200`
- FIT metrics: trades=1398, wins=477, losses=921, win_rate=34.12%, PF=0.3878, net=Rs -465,987, avg_win=Rs 619, avg_loss=Rs -826, SL/TGT/EOD=760/425/213, top_trade/day/symbol=0.0023/None/None
- VAL metrics: trades=974, wins=269, losses=705, win_rate=27.62%, PF=0.2755, net=Rs -416,765, avg_win=Rs 589, avg_loss=Rs -816, SL/TGT/EOD=554/226/194, top_trade/day/symbol=0.0042/None/None
- full TRAIN metrics: trades=2372, wins=746, losses=1626, win_rate=31.45%, PF=0.3394, net=Rs -882,751, avg_win=Rs 608, avg_loss=Rs -822, SL/TGT/EOD=1314/651/407, top_trade/day/symbol=0.0015/None/None
- TEST metrics: not run
- keep/reject: REJECT
- failure classification: train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- next action: continue train-side search

## Iteration 082 - non_indicator_price_action_signal_range_pct>=0.403636
- changed rule/parameter: single non_indicator_price_action threshold sweep
- parameter group: non_indicator_price_action
- old value: none
- new value: ['signal_range_pct', '>=', 0.403636]
- command: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\scripts\full_loop_a_pullback_c2_high.py --max_iterations 200`
- FIT metrics: trades=1499, wins=503, losses=996, win_rate=33.56%, PF=0.3704, net=Rs -530,226, avg_win=Rs 620, avg_loss=Rs -846, SL/TGT/EOD=856/453/190, top_trade/day/symbol=0.0021/None/None
- VAL metrics: trades=1081, wins=309, losses=772, win_rate=28.58%, PF=0.2825, net=Rs -465,409, avg_win=Rs 593, avg_loss=Rs -840, SL/TGT/EOD=637/264/180, top_trade/day/symbol=0.0036/None/None
- full TRAIN metrics: trades=2580, wins=812, losses=1768, win_rate=31.47%, PF=0.3322, net=Rs -995,635, avg_win=Rs 610, avg_loss=Rs -843, SL/TGT/EOD=1493/717/370, top_trade/day/symbol=0.0013/None/None
- TEST metrics: not run
- keep/reject: REJECT
- failure classification: train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- next action: continue train-side search

## Iteration 083 - non_indicator_price_action_vol_ratio>=2.019425
- changed rule/parameter: single non_indicator_price_action threshold sweep
- parameter group: non_indicator_price_action
- old value: none
- new value: ['vol_ratio', '>=', 2.019425]
- command: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\scripts\full_loop_a_pullback_c2_high.py --max_iterations 200`
- FIT metrics: trades=1435, wins=471, losses=964, win_rate=32.82%, PF=0.3621, net=Rs -514,648, avg_win=Rs 620, avg_loss=Rs -837, SL/TGT/EOD=808/423/204, top_trade/day/symbol=0.0023/None/None
- VAL metrics: trades=1011, wins=282, losses=729, win_rate=27.89%, PF=0.2797, net=Rs -434,009, avg_win=Rs 598, avg_loss=Rs -827, SL/TGT/EOD=584/238/189, top_trade/day/symbol=0.004/None/None
- full TRAIN metrics: trades=2446, wins=753, losses=1693, win_rate=30.78%, PF=0.3269, net=Rs -948,657, avg_win=Rs 612, avg_loss=Rs -832, SL/TGT/EOD=1392/661/393, top_trade/day/symbol=0.0014/None/None
- TEST metrics: not run
- keep/reject: REJECT
- failure classification: train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- next action: continue train-side search

## Iteration 084 - non_indicator_price_action_wick_skew_pct>=-0.031671
- changed rule/parameter: single non_indicator_price_action threshold sweep
- parameter group: non_indicator_price_action
- old value: none
- new value: ['wick_skew_pct', '>=', -0.031671]
- command: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\scripts\full_loop_a_pullback_c2_high.py --max_iterations 200`
- FIT metrics: trades=1406, wins=460, losses=946, win_rate=32.72%, PF=0.3604, net=Rs -500,582, avg_win=Rs 613, avg_loss=Rs -827, SL/TGT/EOD=780/406/220, top_trade/day/symbol=0.0024/None/None
- VAL metrics: trades=990, wins=285, losses=705, win_rate=28.79%, PF=0.2908, net=Rs -413,771, avg_win=Rs 595, avg_loss=Rs -828, SL/TGT/EOD=570/241/179, top_trade/day/symbol=0.0039/None/None
- full TRAIN metrics: trades=2396, wins=745, losses=1651, win_rate=31.09%, PF=0.3306, net=Rs -914,353, avg_win=Rs 606, avg_loss=Rs -827, SL/TGT/EOD=1350/647/399, top_trade/day/symbol=0.0015/None/None
- TEST metrics: not run
- keep/reject: REJECT
- failure classification: train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- next action: continue train-side search

## Iteration 085 - non_indicator_price_action_body_pct<=0.677425
- changed rule/parameter: single non_indicator_price_action threshold sweep
- parameter group: non_indicator_price_action
- old value: none
- new value: ['body_pct', '<=', 0.677425]
- command: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\scripts\full_loop_a_pullback_c2_high.py --max_iterations 200`
- FIT metrics: trades=656, wins=198, losses=458, win_rate=30.18%, PF=0.3241, net=Rs -246,478, avg_win=Rs 597, avg_loss=Rs -796, SL/TGT/EOD=345/166/145, top_trade/day/symbol=0.0056/None/None
- VAL metrics: trades=489, wins=141, losses=348, win_rate=28.83%, PF=0.2913, net=Rs -192,960, avg_win=Rs 562, avg_loss=Rs -782, SL/TGT/EOD=253/108/128, top_trade/day/symbol=0.0084/None/None
- full TRAIN metrics: trades=1145, wins=339, losses=806, win_rate=29.61%, PF=0.3101, net=Rs -439,439, avg_win=Rs 583, avg_loss=Rs -790, SL/TGT/EOD=598/274/273, top_trade/day/symbol=0.0034/None/None
- TEST metrics: not run
- keep/reject: REJECT
- failure classification: train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- next action: continue train-side search

## Iteration 086 - non_indicator_price_action_close_loc<=0.787234
- changed rule/parameter: single non_indicator_price_action threshold sweep
- parameter group: non_indicator_price_action
- old value: none
- new value: ['close_loc', '<=', 0.787234]
- command: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\scripts\full_loop_a_pullback_c2_high.py --max_iterations 200`
- FIT metrics: trades=677, wins=208, losses=469, win_rate=30.72%, PF=0.3271, net=Rs -255,407, avg_win=Rs 597, avg_loss=Rs -809, SL/TGT/EOD=362/177/138, top_trade/day/symbol=0.0054/None/None
- VAL metrics: trades=480, wins=134, losses=346, win_rate=27.92%, PF=0.2804, net=Rs -198,765, avg_win=Rs 578, avg_loss=Rs -798, SL/TGT/EOD=264/108/108, top_trade/day/symbol=0.0086/None/None
- full TRAIN metrics: trades=1157, wins=342, losses=815, win_rate=29.56%, PF=0.3074, net=Rs -454,172, avg_win=Rs 590, avg_loss=Rs -805, SL/TGT/EOD=626/285/246, top_trade/day/symbol=0.0033/None/None
- TEST metrics: not run
- keep/reject: REJECT
- failure classification: train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- next action: continue train-side search

## Iteration 087 - non_indicator_price_action_signal_range_pct<=0.403636
- changed rule/parameter: single non_indicator_price_action threshold sweep
- parameter group: non_indicator_price_action
- old value: none
- new value: ['signal_range_pct', '<=', 0.403636]
- command: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\scripts\full_loop_a_pullback_c2_high.py --max_iterations 200`
- FIT metrics: trades=583, wins=179, losses=404, win_rate=30.7%, PF=0.3378, net=Rs -197,866, avg_win=Rs 564, avg_loss=Rs -740, SL/TGT/EOD=267/138/178, top_trade/day/symbol=0.0085/None/None
- VAL metrics: trades=441, wins=105, losses=336, win_rate=23.81%, PF=0.2359, net=Rs -182,987, avg_win=Rs 538, avg_loss=Rs -713, SL/TGT/EOD=205/76/160, top_trade/day/symbol=0.0118/None/None
- full TRAIN metrics: trades=1024, wins=284, losses=740, win_rate=27.73%, PF=0.2924, net=Rs -380,853, avg_win=Rs 554, avg_loss=Rs -727, SL/TGT/EOD=472/214/338, top_trade/day/symbol=0.0055/None/None
- TEST metrics: not run
- keep/reject: REJECT
- failure classification: train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- next action: continue train-side search

## Iteration 088 - non_indicator_price_action_vol_ratio<=2.019425
- changed rule/parameter: single non_indicator_price_action threshold sweep
- parameter group: non_indicator_price_action
- old value: none
- new value: ['vol_ratio', '<=', 2.019425]
- command: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\scripts\full_loop_a_pullback_c2_high.py --max_iterations 200`
- FIT metrics: trades=625, wins=213, losses=412, win_rate=34.08%, PF=0.3948, net=Rs -195,793, avg_win=Rs 600, avg_loss=Rs -785, SL/TGT/EOD=312/181/132, top_trade/day/symbol=0.0052/None/None
- VAL metrics: trades=418, wins=118, losses=300, win_rate=28.23%, PF=0.2771, net=Rs -166,123, avg_win=Rs 540, avg_loss=Rs -766, SL/TGT/EOD=212/89/117, top_trade/day/symbol=0.0105/None/None
- full TRAIN metrics: trades=1043, wins=331, losses=712, win_rate=31.74%, PF=0.346, net=Rs -361,916, avg_win=Rs 578, avg_loss=Rs -777, SL/TGT/EOD=524/270/249, top_trade/day/symbol=0.0035/None/None
- TEST metrics: not run
- keep/reject: REJECT
- failure classification: train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- next action: continue train-side search

## Iteration 089 - non_indicator_price_action_wick_skew_pct<=-0.031671
- changed rule/parameter: single non_indicator_price_action threshold sweep
- parameter group: non_indicator_price_action
- old value: none
- new value: ['wick_skew_pct', '<=', -0.031671]
- command: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\scripts\full_loop_a_pullback_c2_high.py --max_iterations 200`
- FIT metrics: trades=667, wins=221, losses=446, win_rate=33.13%, PF=0.3615, net=Rs -234,618, avg_win=Rs 601, avg_loss=Rs -824, SL/TGT/EOD=363/190/114, top_trade/day/symbol=0.005/None/None
- VAL metrics: trades=481, wins=121, losses=360, win_rate=25.16%, PF=0.2502, net=Rs -212,169, avg_win=Rs 585, avg_loss=Rs -786, SL/TGT/EOD=261/100/120, top_trade/day/symbol=0.0094/None/None
- full TRAIN metrics: trades=1148, wins=342, losses=806, win_rate=29.79%, PF=0.3131, net=Rs -446,787, avg_win=Rs 595, avg_loss=Rs -807, SL/TGT/EOD=624/290/234, top_trade/day/symbol=0.0033/None/None
- TEST metrics: not run
- keep/reject: REJECT
- failure classification: train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- next action: continue train-side search

## Iteration 090 - filter_market_abs_ret_pct>=0.073994
- changed rule/parameter: single filter threshold sweep
- parameter group: filter
- old value: none
- new value: ['market_abs_ret_pct', '>=', 0.073994]
- command: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\scripts\full_loop_a_pullback_c2_high.py --max_iterations 200`
- FIT metrics: trades=1448, wins=477, losses=971, win_rate=32.94%, PF=0.365, net=Rs -515,005, avg_win=Rs 621, avg_loss=Rs -835, SL/TGT/EOD=813/428/207, top_trade/day/symbol=0.0023/None/None
- VAL metrics: trades=899, wins=255, losses=644, win_rate=28.36%, PF=0.2892, net=Rs -376,968, avg_win=Rs 602, avg_loss=Rs -824, SL/TGT/EOD=511/221/167, top_trade/day/symbol=0.0043/None/None
- full TRAIN metrics: trades=2347, wins=732, losses=1615, win_rate=31.19%, PF=0.335, net=Rs -891,973, avg_win=Rs 614, avg_loss=Rs -831, SL/TGT/EOD=1324/649/374, top_trade/day/symbol=0.0015/None/None
- TEST metrics: not run
- keep/reject: REJECT
- failure classification: train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- next action: continue train-side search

## Iteration 091 - filter_market_ret_pct>=-0.21279
- changed rule/parameter: single filter threshold sweep
- parameter group: filter
- old value: none
- new value: ['market_ret_pct', '>=', -0.21279]
- command: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\scripts\full_loop_a_pullback_c2_high.py --max_iterations 200`
- FIT metrics: trades=1205, wins=377, losses=828, win_rate=31.29%, PF=0.3373, net=Rs -462,936, avg_win=Rs 625, avg_loss=Rs -844, SL/TGT/EOD=696/342/167, top_trade/day/symbol=0.0028/None/None
- VAL metrics: trades=1029, wins=289, losses=740, win_rate=28.09%, PF=0.2841, net=Rs -434,863, avg_win=Rs 597, avg_loss=Rs -821, SL/TGT/EOD=590/246/193, top_trade/day/symbol=0.0039/None/None
- full TRAIN metrics: trades=2234, wins=666, losses=1568, win_rate=29.81%, PF=0.3126, net=Rs -897,798, avg_win=Rs 613, avg_loss=Rs -833, SL/TGT/EOD=1286/588/360, top_trade/day/symbol=0.0016/None/None
- TEST metrics: not run
- keep/reject: REJECT
- failure classification: train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- next action: continue train-side search

## Iteration 092 - filter_notional>=99157.14
- changed rule/parameter: single filter threshold sweep
- parameter group: filter
- old value: none
- new value: ['notional', '>=', 99157.14]
- command: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\scripts\full_loop_a_pullback_c2_high.py --max_iterations 200`
- FIT metrics: trades=1491, wins=493, losses=998, win_rate=33.07%, PF=0.3666, net=Rs -532,346, avg_win=Rs 625, avg_loss=Rs -842, SL/TGT/EOD=843/446/202, top_trade/day/symbol=0.0022/None/None
- VAL metrics: trades=1056, wins=301, losses=755, win_rate=28.5%, PF=0.2868, net=Rs -450,791, avg_win=Rs 602, avg_loss=Rs -837, SL/TGT/EOD=614/259/183, top_trade/day/symbol=0.0037/None/None
- full TRAIN metrics: trades=2547, wins=794, losses=1753, win_rate=31.17%, PF=0.3324, net=Rs -983,137, avg_win=Rs 616, avg_loss=Rs -840, SL/TGT/EOD=1457/705/385, top_trade/day/symbol=0.0014/None/None
- TEST metrics: not run
- keep/reject: REJECT
- failure classification: train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- next action: continue train-side search

## Iteration 093 - filter_signal_minute>=700.0
- changed rule/parameter: single filter threshold sweep
- parameter group: filter
- old value: none
- new value: ['signal_minute', '>=', 700.0]
- command: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\scripts\full_loop_a_pullback_c2_high.py --max_iterations 200`
- FIT metrics: trades=1321, wins=462, losses=859, win_rate=34.97%, PF=0.4006, net=Rs -423,350, avg_win=Rs 612, avg_loss=Rs -822, SL/TGT/EOD=693/407/221, top_trade/day/symbol=0.0024/None/None
- VAL metrics: trades=929, wins=254, losses=675, win_rate=27.34%, PF=0.2747, net=Rs -398,592, avg_win=Rs 594, avg_loss=Rs -814, SL/TGT/EOD=527/212/190, top_trade/day/symbol=0.0044/None/None
- full TRAIN metrics: trades=2250, wins=716, losses=1534, win_rate=31.82%, PF=0.3455, net=Rs -821,943, avg_win=Rs 606, avg_loss=Rs -819, SL/TGT/EOD=1220/619/411, top_trade/day/symbol=0.0015/None/None
- TEST metrics: not run
- keep/reject: REJECT
- failure classification: train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- next action: continue train-side search

## Iteration 094 - filter_market_abs_ret_pct<=0.073994
- changed rule/parameter: single filter threshold sweep
- parameter group: filter
- old value: none
- new value: ['market_abs_ret_pct', '<=', 0.073994]
- command: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\scripts\full_loop_a_pullback_c2_high.py --max_iterations 200`
- FIT metrics: trades=231, wins=81, losses=150, win_rate=35.06%, PF=0.4185, net=Rs -70,602, avg_win=Rs 627, avg_loss=Rs -809, SL/TGT/EOD=117/74/40, top_trade/day/symbol=0.0131/None/None
- VAL metrics: trades=376, wins=109, losses=267, win_rate=28.99%, PF=0.3012, net=Rs -145,498, avg_win=Rs 575, avg_loss=Rs -780, SL/TGT/EOD=195/89/92, top_trade/day/symbol=0.0106/None/None
- full TRAIN metrics: trades=607, wins=190, losses=417, win_rate=31.3%, PF=0.3444, net=Rs -216,100, avg_win=Rs 598, avg_loss=Rs -790, SL/TGT/EOD=312/163/132, top_trade/day/symbol=0.0059/None/None
- TEST metrics: not run
- keep/reject: REJECT
- failure classification: train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- next action: continue train-side search

## Iteration 095 - filter_market_ret_pct<=-0.21279
- changed rule/parameter: single filter threshold sweep
- parameter group: filter
- old value: none
- new value: ['market_ret_pct', '<=', -0.21279]
- command: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\scripts\full_loop_a_pullback_c2_high.py --max_iterations 200`
- FIT metrics: trades=406, wins=163, losses=243, win_rate=40.15%, PF=0.5038, net=Rs -98,862, avg_win=Rs 616, avg_loss=Rs -820, SL/TGT/EOD=203/144/59, top_trade/day/symbol=0.0066/None/None
- VAL metrics: trades=101, wins=33, losses=68, win_rate=32.67%, PF=0.3583, net=Rs -36,357, avg_win=Rs 615, avg_loss=Rs -833, SL/TGT/EOD=57/30/14, top_trade/day/symbol=0.0328/None/None
- full TRAIN metrics: trades=507, wins=196, losses=311, win_rate=38.66%, PF=0.4716, net=Rs -135,219, avg_win=Rs 616, avg_loss=Rs -823, SL/TGT/EOD=260/174/73, top_trade/day/symbol=0.0055/None/None
- TEST metrics: not run
- keep/reject: REJECT
- failure classification: train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- next action: continue train-side search

## Iteration 096 - filter_notional<=99157.14
- changed rule/parameter: single filter threshold sweep
- parameter group: filter
- old value: none
- new value: ['notional', '<=', 99157.14]
- command: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\scripts\full_loop_a_pullback_c2_high.py --max_iterations 200`
- FIT metrics: trades=397, wins=136, losses=261, win_rate=34.26%, PF=0.388, net=Rs -118,004, avg_win=Rs 550, avg_loss=Rs -739, SL/TGT/EOD=180/106/111, top_trade/day/symbol=0.0088/None/None
- VAL metrics: trades=323, wins=81, losses=242, win_rate=25.08%, PF=0.2431, net=Rs -132,828, avg_win=Rs 527, avg_loss=Rs -725, SL/TGT/EOD=161/59/103, top_trade/day/symbol=0.0155/None/None
- full TRAIN metrics: trades=720, wins=217, losses=503, win_rate=30.14%, PF=0.319, net=Rs -250,832, avg_win=Rs 541, avg_loss=Rs -732, SL/TGT/EOD=341/165/214, top_trade/day/symbol=0.0056/None/None
- TEST metrics: not run
- keep/reject: REJECT
- failure classification: train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- next action: continue train-side search

## Iteration 097 - filter_signal_minute<=700.0
- changed rule/parameter: single filter threshold sweep
- parameter group: filter
- old value: none
- new value: ['signal_minute', '<=', 700.0]
- command: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\scripts\full_loop_a_pullback_c2_high.py --max_iterations 200`
- FIT metrics: trades=409, wins=137, losses=272, win_rate=33.5%, PF=0.3631, net=Rs -155,031, avg_win=Rs 645, avg_loss=Rs -895, SL/TGT/EOD=258/132/19, top_trade/day/symbol=0.0075/None/None
- VAL metrics: trades=341, wins=105, losses=236, win_rate=30.79%, PF=0.3104, net=Rs -139,451, avg_win=Rs 598, avg_loss=Rs -857, SL/TGT/EOD=206/92/43, top_trade/day/symbol=0.0106/None/None
- full TRAIN metrics: trades=750, wins=242, losses=508, win_rate=32.27%, PF=0.3392, net=Rs -294,483, avg_win=Rs 625, avg_loss=Rs -877, SL/TGT/EOD=464/224/62, top_trade/day/symbol=0.0044/None/None
- TEST metrics: not run
- keep/reject: REJECT
- failure classification: train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- next action: continue train-side search

## Iteration 098 - filter_market_abs_ret_pct>=0.132184
- changed rule/parameter: single filter threshold sweep
- parameter group: filter
- old value: none
- new value: ['market_abs_ret_pct', '>=', 0.132184]
- command: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\scripts\full_loop_a_pullback_c2_high.py --max_iterations 200`
- FIT metrics: trades=1332, wins=440, losses=892, win_rate=33.03%, PF=0.3682, net=Rs -471,541, avg_win=Rs 625, avg_loss=Rs -837, SL/TGT/EOD=750/399/183, top_trade/day/symbol=0.0024/None/None
- VAL metrics: trades=742, wins=215, losses=527, win_rate=28.98%, PF=0.3014, net=Rs -303,114, avg_win=Rs 608, avg_loss=Rs -823, SL/TGT/EOD=423/188/131, top_trade/day/symbol=0.0051/None/None
- full TRAIN metrics: trades=2074, wins=655, losses=1419, win_rate=31.58%, PF=0.3437, net=Rs -774,655, avg_win=Rs 619, avg_loss=Rs -832, SL/TGT/EOD=1173/587/314, top_trade/day/symbol=0.0016/None/None
- TEST metrics: not run
- keep/reject: REJECT
- failure classification: train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- next action: continue train-side search

## Iteration 099 - filter_market_ret_pct>=-0.029546
- changed rule/parameter: single filter threshold sweep
- parameter group: filter
- old value: none
- new value: ['market_ret_pct', '>=', -0.029546]
- command: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\scripts\full_loop_a_pullback_c2_high.py --max_iterations 200`
- FIT metrics: trades=1075, wins=322, losses=753, win_rate=29.95%, PF=0.3154, net=Rs -435,233, avg_win=Rs 623, avg_loss=Rs -844, SL/TGT/EOD=635/292/148, top_trade/day/symbol=0.0033/None/None
- VAL metrics: trades=886, wins=241, losses=645, win_rate=27.2%, PF=0.2725, net=Rs -383,038, avg_win=Rs 595, avg_loss=Rs -816, SL/TGT/EOD=504/207/175, top_trade/day/symbol=0.0046/None/None
- full TRAIN metrics: trades=1961, wins=563, losses=1398, win_rate=28.71%, PF=0.296, net=Rs -818,272, avg_win=Rs 611, avg_loss=Rs -831, SL/TGT/EOD=1139/499/323, top_trade/day/symbol=0.0019/None/None
- TEST metrics: not run
- keep/reject: REJECT
- failure classification: train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- next action: continue train-side search

## Iteration 100 - filter_notional>=99557.34
- changed rule/parameter: single filter threshold sweep
- parameter group: filter
- old value: none
- new value: ['notional', '>=', 99557.34]
- command: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\scripts\full_loop_a_pullback_c2_high.py --max_iterations 200`
- FIT metrics: trades=1442, wins=471, losses=971, win_rate=32.66%, PF=0.3592, net=Rs -524,925, avg_win=Rs 625, avg_loss=Rs -844, SL/TGT/EOD=822/426/194, top_trade/day/symbol=0.0023/None/None
- VAL metrics: trades=1021, wins=284, losses=737, win_rate=27.82%, PF=0.2752, net=Rs -446,146, avg_win=Rs 596, avg_loss=Rs -835, SL/TGT/EOD=600/242/179, top_trade/day/symbol=0.0039/None/None
- full TRAIN metrics: trades=2463, wins=755, losses=1708, win_rate=30.65%, PF=0.3231, net=Rs -971,070, avg_win=Rs 614, avg_loss=Rs -840, SL/TGT/EOD=1422/668/373, top_trade/day/symbol=0.0014/None/None
- TEST metrics: not run
- keep/reject: REJECT
- failure classification: train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- next action: continue train-side search

## Iteration 101 - filter_signal_minute>=720.0
- changed rule/parameter: single filter threshold sweep
- parameter group: filter
- old value: none
- new value: ['signal_minute', '>=', 720.0]
- command: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\scripts\full_loop_a_pullback_c2_high.py --max_iterations 200`
- FIT metrics: trades=1202, wins=405, losses=797, win_rate=33.69%, PF=0.3822, net=Rs -398,023, avg_win=Rs 608, avg_loss=Rs -808, SL/TGT/EOD=625/349/228, top_trade/day/symbol=0.0027/None/None
- VAL metrics: trades=819, wins=222, losses=597, win_rate=27.11%, PF=0.2735, net=Rs -348,126, avg_win=Rs 590, avg_loss=Rs -803, SL/TGT/EOD=449/183/187, top_trade/day/symbol=0.0051/None/None
- full TRAIN metrics: trades=2021, wins=627, losses=1394, win_rate=31.02%, PF=0.3358, net=Rs -746,149, avg_win=Rs 602, avg_loss=Rs -806, SL/TGT/EOD=1074/532/415, top_trade/day/symbol=0.0018/None/None
- TEST metrics: not run
- keep/reject: REJECT
- failure classification: train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- next action: continue train-side search

## Iteration 102 - filter_market_abs_ret_pct<=0.132184
- changed rule/parameter: single filter threshold sweep
- parameter group: filter
- old value: none
- new value: ['market_abs_ret_pct', '<=', 0.132184]
- command: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\scripts\full_loop_a_pullback_c2_high.py --max_iterations 200`
- FIT metrics: trades=388, wins=130, losses=258, win_rate=33.51%, PF=0.3809, net=Rs -129,542, avg_win=Rs 613, avg_loss=Rs -811, SL/TGT/EOD=202/115/71, top_trade/day/symbol=0.0084/None/None
- VAL metrics: trades=527, wins=149, losses=378, win_rate=28.27%, PF=0.2858, net=Rs -213,686, avg_win=Rs 574, avg_loss=Rs -792, SL/TGT/EOD=279/121/127, top_trade/day/symbol=0.0078/None/None
- full TRAIN metrics: trades=915, wins=279, losses=636, win_rate=30.49%, PF=0.325, net=Rs -343,228, avg_win=Rs 592, avg_loss=Rs -799, SL/TGT/EOD=481/236/198, top_trade/day/symbol=0.004/None/None
- TEST metrics: not run
- keep/reject: REJECT
- failure classification: train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- next action: continue train-side search

## Iteration 103 - filter_market_ret_pct<=-0.029546
- changed rule/parameter: single filter threshold sweep
- parameter group: filter
- old value: none
- new value: ['market_ret_pct', '<=', -0.029546]
- command: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\scripts\full_loop_a_pullback_c2_high.py --max_iterations 200`
- FIT metrics: trades=568, wins=228, losses=340, win_rate=40.14%, PF=0.5116, net=Rs -134,379, avg_win=Rs 617, avg_loss=Rs -809, SL/TGT/EOD=276/202/90, top_trade/day/symbol=0.0047/None/None
- VAL metrics: trades=374, wins=117, losses=257, win_rate=31.28%, PF=0.3427, net=Rs -133,700, avg_win=Rs 596, avg_loss=Rs -792, SL/TGT/EOD=192/99/83, top_trade/day/symbol=0.0096/None/None
- full TRAIN metrics: trades=942, wins=345, losses=597, win_rate=36.62%, PF=0.4398, net=Rs -268,079, avg_win=Rs 610, avg_loss=Rs -802, SL/TGT/EOD=468/301/173, top_trade/day/symbol=0.0032/None/None
- TEST metrics: not run
- keep/reject: REJECT
- failure classification: train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- next action: continue train-side search

## Iteration 104 - filter_notional<=99557.34
- changed rule/parameter: single filter threshold sweep
- parameter group: filter
- old value: none
- new value: ['notional', '<=', 99557.34]
- command: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\scripts\full_loop_a_pullback_c2_high.py --max_iterations 200`
- FIT metrics: trades=664, wins=218, losses=446, win_rate=32.83%, PF=0.3732, net=Rs -212,251, avg_win=Rs 580, avg_loss=Rs -759, SL/TGT/EOD=320/180/164, top_trade/day/symbol=0.0052/None/None
- VAL metrics: trades=489, wins=132, losses=357, win_rate=26.99%, PF=0.2748, net=Rs -192,507, avg_win=Rs 553, avg_loss=Rs -744, SL/TGT/EOD=242/102/145, top_trade/day/symbol=0.0091/None/None
- full TRAIN metrics: trades=1153, wins=350, losses=803, win_rate=30.36%, PF=0.33, net=Rs -404,758, avg_win=Rs 569, avg_loss=Rs -752, SL/TGT/EOD=562/282/309, top_trade/day/symbol=0.0033/None/None
- TEST metrics: not run
- keep/reject: REJECT
- failure classification: train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- next action: continue train-side search

## Iteration 105 - filter_signal_minute<=720.0
- changed rule/parameter: single filter threshold sweep
- parameter group: filter
- old value: none
- new value: ['signal_minute', '<=', 720.0]
- command: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\scripts\full_loop_a_pullback_c2_high.py --max_iterations 200`
- FIT metrics: trades=585, wins=198, losses=387, win_rate=33.85%, PF=0.3718, net=Rs -213,639, avg_win=Rs 639, avg_loss=Rs -879, SL/TGT/EOD=357/189/39, top_trade/day/symbol=0.0053/None/None
- VAL metrics: trades=462, wins=142, losses=320, win_rate=30.74%, PF=0.3113, net=Rs -190,269, avg_win=Rs 606, avg_loss=Rs -863, SL/TGT/EOD=282/126/54, top_trade/day/symbol=0.0078/None/None
- full TRAIN metrics: trades=1047, wins=340, losses=707, win_rate=32.47%, PF=0.3447, net=Rs -403,908, avg_win=Rs 625, avg_loss=Rs -872, SL/TGT/EOD=639/315/93, top_trade/day/symbol=0.0031/None/None
- TEST metrics: not run
- keep/reject: REJECT
- failure classification: train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- next action: continue train-side search

## Iteration 106 - premom_pre10_body_sum_r>=0.139794
- changed rule/parameter: single pre-entry momentum threshold sweep
- parameter group: pre_momentum
- old value: none
- new value: ['pre10_body_sum_r', '>=', 0.139794]
- command: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\scripts\full_loop_a_pullback_c2_high.py --max_iterations 200`
- FIT metrics: trades=1458, wins=488, losses=970, win_rate=33.47%, PF=0.3715, net=Rs -510,809, avg_win=Rs 619, avg_loss=Rs -838, SL/TGT/EOD=818/440/200, top_trade/day/symbol=0.0022/None/None
- VAL metrics: trades=1078, wins=314, losses=764, win_rate=29.13%, PF=0.2989, net=Rs -444,122, avg_win=Rs 603, avg_loss=Rs -829, SL/TGT/EOD=619/269/190, top_trade/day/symbol=0.0035/None/None
- full TRAIN metrics: trades=2536, wins=802, losses=1734, win_rate=31.62%, PF=0.3397, net=Rs -954,931, avg_win=Rs 613, avg_loss=Rs -834, SL/TGT/EOD=1437/709/390, top_trade/day/symbol=0.0014/None/None
- TEST metrics: not run
- keep/reject: REJECT
- failure classification: train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- next action: continue train-side search

## Iteration 107 - premom_pre10_close_pos>=0.676894
- changed rule/parameter: single pre-entry momentum threshold sweep
- parameter group: pre_momentum
- old value: none
- new value: ['pre10_close_pos', '>=', 0.676894]
- command: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\scripts\full_loop_a_pullback_c2_high.py --max_iterations 200`
- FIT metrics: trades=1479, wins=491, losses=988, win_rate=33.2%, PF=0.371, net=Rs -520,636, avg_win=Rs 625, avg_loss=Rs -838, SL/TGT/EOD=828/446/205, top_trade/day/symbol=0.0022/None/None
- VAL metrics: trades=988, wins=290, losses=698, win_rate=29.35%, PF=0.3027, net=Rs -393,848, avg_win=Rs 590, avg_loss=Rs -809, SL/TGT/EOD=544/242/202, top_trade/day/symbol=0.0039/None/None
- full TRAIN metrics: trades=2467, wins=781, losses=1686, win_rate=31.66%, PF=0.3433, net=Rs -914,485, avg_win=Rs 612, avg_loss=Rs -826, SL/TGT/EOD=1372/688/407, top_trade/day/symbol=0.0014/None/None
- TEST metrics: not run
- keep/reject: REJECT
- failure classification: train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- next action: continue train-side search

## Iteration 108 - premom_pre10_dir_count>=3.0
- changed rule/parameter: single pre-entry momentum threshold sweep
- parameter group: pre_momentum
- old value: none
- new value: ['pre10_dir_count', '>=', 3.0]
- command: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\scripts\full_loop_a_pullback_c2_high.py --max_iterations 200`
- FIT metrics: trades=1497, wins=509, losses=988, win_rate=34.0%, PF=0.3793, net=Rs -511,283, avg_win=Rs 614, avg_loss=Rs -834, SL/TGT/EOD=825/448/224, top_trade/day/symbol=0.0021/None/None
- VAL metrics: trades=1078, wins=311, losses=767, win_rate=28.85%, PF=0.2961, net=Rs -446,822, avg_win=Rs 604, avg_loss=Rs -828, SL/TGT/EOD=616/269/193, top_trade/day/symbol=0.0035/None/None
- full TRAIN metrics: trades=2575, wins=820, losses=1755, win_rate=31.84%, PF=0.3431, net=Rs -958,104, avg_win=Rs 610, avg_loss=Rs -831, SL/TGT/EOD=1441/717/417, top_trade/day/symbol=0.0013/None/None
- TEST metrics: not run
- keep/reject: REJECT
- failure classification: train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- next action: continue train-side search

## Iteration 109 - premom_pre10_mom_r>=0.204232
- changed rule/parameter: single pre-entry momentum threshold sweep
- parameter group: pre_momentum
- old value: none
- new value: ['pre10_mom_r', '>=', 0.204232]
- command: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\scripts\full_loop_a_pullback_c2_high.py --max_iterations 200`
- FIT metrics: trades=1464, wins=499, losses=965, win_rate=34.08%, PF=0.383, net=Rs -500,494, avg_win=Rs 623, avg_loss=Rs -841, SL/TGT/EOD=816/453/195, top_trade/day/symbol=0.0021/None/None
- VAL metrics: trades=1084, wins=310, losses=774, win_rate=28.6%, PF=0.2885, net=Rs -459,446, avg_win=Rs 601, avg_loss=Rs -834, SL/TGT/EOD=632/266/186, top_trade/day/symbol=0.0036/None/None
- full TRAIN metrics: trades=2548, wins=809, losses=1739, win_rate=31.75%, PF=0.3411, net=Rs -959,941, avg_win=Rs 614, avg_loss=Rs -838, SL/TGT/EOD=1448/719/381, top_trade/day/symbol=0.0013/None/None
- TEST metrics: not run
- keep/reject: REJECT
- failure classification: train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- next action: continue train-side search

## Iteration 110 - premom_pre10_range_r>=0.556437
- changed rule/parameter: single pre-entry momentum threshold sweep
- parameter group: pre_momentum
- old value: none
- new value: ['pre10_range_r', '>=', 0.556437]
- command: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\scripts\full_loop_a_pullback_c2_high.py --max_iterations 200`
- FIT metrics: trades=1498, wins=501, losses=997, win_rate=33.44%, PF=0.3705, net=Rs -527,743, avg_win=Rs 620, avg_loss=Rs -841, SL/TGT/EOD=847/452/199, top_trade/day/symbol=0.0021/None/None
- VAL metrics: trades=1096, wins=306, losses=790, win_rate=27.92%, PF=0.2748, net=Rs -482,499, avg_win=Rs 598, avg_loss=Rs -842, SL/TGT/EOD=651/262/183, top_trade/day/symbol=0.0036/None/None
- full TRAIN metrics: trades=2594, wins=807, losses=1787, win_rate=31.11%, PF=0.3282, net=Rs -1,010,243, avg_win=Rs 612, avg_loss=Rs -841, SL/TGT/EOD=1498/714/382, top_trade/day/symbol=0.0014/None/None
- TEST metrics: not run
- keep/reject: REJECT
- failure classification: train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- next action: continue train-side search

## Iteration 111 - premom_pre10_vol_ratio20>=1.024219
- changed rule/parameter: single pre-entry momentum threshold sweep
- parameter group: pre_momentum
- old value: none
- new value: ['pre10_vol_ratio20', '>=', 1.024219]
- command: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\scripts\full_loop_a_pullback_c2_high.py --max_iterations 200`
- FIT metrics: trades=1433, wins=468, losses=965, win_rate=32.66%, PF=0.3636, net=Rs -509,597, avg_win=Rs 622, avg_loss=Rs -830, SL/TGT/EOD=799/425/209, top_trade/day/symbol=0.0023/None/None
- VAL metrics: trades=1052, wins=312, losses=740, win_rate=29.66%, PF=0.304, net=Rs -425,577, avg_win=Rs 596, avg_loss=Rs -826, SL/TGT/EOD=596/265/191, top_trade/day/symbol=0.0036/None/None
- full TRAIN metrics: trades=2485, wins=780, losses=1705, win_rate=31.39%, PF=0.3378, net=Rs -935,174, avg_win=Rs 612, avg_loss=Rs -828, SL/TGT/EOD=1395/690/400, top_trade/day/symbol=0.0014/None/None
- TEST metrics: not run
- keep/reject: REJECT
- failure classification: train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- next action: continue train-side search

## Iteration 112 - premom_pre1_adx>=20.623265
- changed rule/parameter: single pre-entry momentum threshold sweep
- parameter group: pre_momentum
- old value: none
- new value: ['pre1_adx', '>=', 20.623265]
- command: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\scripts\full_loop_a_pullback_c2_high.py --max_iterations 200`
- FIT metrics: trades=1471, wins=506, losses=965, win_rate=34.4%, PF=0.3899, net=Rs -489,474, avg_win=Rs 618, avg_loss=Rs -831, SL/TGT/EOD=799/452/220, top_trade/day/symbol=0.0021/None/None
- VAL metrics: trades=1040, wins=296, losses=744, win_rate=28.46%, PF=0.2871, net=Rs -440,402, avg_win=Rs 599, avg_loss=Rs -830, SL/TGT/EOD=603/255/182, top_trade/day/symbol=0.0038/None/None
- full TRAIN metrics: trades=2511, wins=802, losses=1709, win_rate=31.94%, PF=0.3452, net=Rs -929,875, avg_win=Rs 611, avg_loss=Rs -831, SL/TGT/EOD=1402/707/402, top_trade/day/symbol=0.0014/None/None
- TEST metrics: not run
- keep/reject: REJECT
- failure classification: train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- next action: continue train-side search

## Iteration 113 - premom_pre1_body_r>=-0.060876
- changed rule/parameter: single pre-entry momentum threshold sweep
- parameter group: pre_momentum
- old value: none
- new value: ['pre1_body_r', '>=', -0.060876]
- command: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\scripts\full_loop_a_pullback_c2_high.py --max_iterations 200`
- FIT metrics: trades=1423, wins=488, losses=935, win_rate=34.29%, PF=0.3876, net=Rs -476,286, avg_win=Rs 618, avg_loss=Rs -832, SL/TGT/EOD=779/435/209, top_trade/day/symbol=0.0022/None/None
- VAL metrics: trades=988, wins=277, losses=711, win_rate=28.04%, PF=0.2814, net=Rs -415,899, avg_win=Rs 588, avg_loss=Rs -814, SL/TGT/EOD=559/231/198, top_trade/day/symbol=0.0041/None/None
- full TRAIN metrics: trades=2411, wins=765, losses=1646, win_rate=31.73%, PF=0.3423, net=Rs -892,184, avg_win=Rs 607, avg_loss=Rs -824, SL/TGT/EOD=1338/666/407, top_trade/day/symbol=0.0014/None/None
- TEST metrics: not run
- keep/reject: REJECT
- failure classification: train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- next action: continue train-side search

## Iteration 114 - premom_pre1_close_pos>=0.0
- changed rule/parameter: single pre-entry momentum threshold sweep
- parameter group: pre_momentum
- old value: none
- new value: ['pre1_close_pos', '>=', 0.0]
- command: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\scripts\full_loop_a_pullback_c2_high.py --max_iterations 200`
- FIT metrics: trades=1553, wins=522, losses=1031, win_rate=33.61%, PF=0.377, net=Rs -537,333, avg_win=Rs 623, avg_loss=Rs -837, SL/TGT/EOD=863/471/219, top_trade/day/symbol=0.0021/None/None
- VAL metrics: trades=1093, wins=313, losses=780, win_rate=28.64%, PF=0.2901, net=Rs -459,023, avg_win=Rs 599, avg_loss=Rs -829, SL/TGT/EOD=632/268/193, top_trade/day/symbol=0.0036/None/None
- full TRAIN metrics: trades=2646, wins=835, losses=1811, win_rate=31.56%, PF=0.3398, net=Rs -996,356, avg_win=Rs 614, avg_loss=Rs -833, SL/TGT/EOD=1495/739/412, top_trade/day/symbol=0.0013/None/None
- TEST metrics: not run
- keep/reject: REJECT
- failure classification: train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- next action: continue train-side search

## Iteration 115 - premom_pre1_range_r>=0.076787
- changed rule/parameter: single pre-entry momentum threshold sweep
- parameter group: pre_momentum
- old value: none
- new value: ['pre1_range_r', '>=', 0.076787]
- command: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\scripts\full_loop_a_pullback_c2_high.py --max_iterations 200`
- FIT metrics: trades=1487, wins=509, losses=978, win_rate=34.23%, PF=0.3853, net=Rs -501,214, avg_win=Rs 617, avg_loss=Rs -834, SL/TGT/EOD=816/453/218, top_trade/day/symbol=0.0021/None/None
- VAL metrics: trades=1057, wins=305, losses=752, win_rate=28.86%, PF=0.2979, net=Rs -435,604, avg_win=Rs 606, avg_loss=Rs -825, SL/TGT/EOD=606/266/185, top_trade/day/symbol=0.0036/None/None
- full TRAIN metrics: trades=2544, wins=814, losses=1730, win_rate=32.0%, PF=0.3476, net=Rs -936,818, avg_win=Rs 613, avg_loss=Rs -830, SL/TGT/EOD=1422/719/403, top_trade/day/symbol=0.0013/None/None
- TEST metrics: not run
- keep/reject: REJECT
- failure classification: train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- next action: continue train-side search

## Iteration 116 - premom_pre2_mom_r>=-0.043652
- changed rule/parameter: single pre-entry momentum threshold sweep
- parameter group: pre_momentum
- old value: none
- new value: ['pre2_mom_r', '>=', -0.043652]
- command: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\scripts\full_loop_a_pullback_c2_high.py --max_iterations 200`
- FIT metrics: trades=1453, wins=500, losses=953, win_rate=34.41%, PF=0.3851, net=Rs -491,059, avg_win=Rs 615, avg_loss=Rs -838, SL/TGT/EOD=800/444/209, top_trade/day/symbol=0.0022/None/None
- VAL metrics: trades=950, wins=261, losses=689, win_rate=27.47%, PF=0.2707, net=Rs -407,193, avg_win=Rs 579, avg_loss=Rs -810, SL/TGT/EOD=539/211/200, top_trade/day/symbol=0.0044/None/None
- full TRAIN metrics: trades=2403, wins=761, losses=1642, win_rate=31.67%, PF=0.338, net=Rs -898,252, avg_win=Rs 603, avg_loss=Rs -826, SL/TGT/EOD=1339/655/409, top_trade/day/symbol=0.0015/None/None
- TEST metrics: not run
- keep/reject: REJECT
- failure classification: train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- next action: continue train-side search

## Iteration 117 - premom_pre3_body_sum_r>=-0.033393
- changed rule/parameter: single pre-entry momentum threshold sweep
- parameter group: pre_momentum
- old value: none
- new value: ['pre3_body_sum_r', '>=', -0.033393]
- command: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\scripts\full_loop_a_pullback_c2_high.py --max_iterations 200`
- FIT metrics: trades=1507, wins=511, losses=996, win_rate=33.91%, PF=0.3791, net=Rs -519,296, avg_win=Rs 621, avg_loss=Rs -840, SL/TGT/EOD=839/458/210, top_trade/day/symbol=0.0021/None/None
- VAL metrics: trades=972, wins=284, losses=688, win_rate=29.22%, PF=0.2984, net=Rs -392,654, avg_win=Rs 588, avg_loss=Rs -813, SL/TGT/EOD=541/235/196, top_trade/day/symbol=0.004/None/None
- full TRAIN metrics: trades=2479, wins=795, losses=1684, win_rate=32.07%, PF=0.3468, net=Rs -911,950, avg_win=Rs 609, avg_loss=Rs -829, SL/TGT/EOD=1380/693/406, top_trade/day/symbol=0.0014/None/None
- TEST metrics: not run
- keep/reject: REJECT
- failure classification: train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- next action: continue train-side search

## Iteration 118 - premom_pre3_close_pos>=0.385701
- changed rule/parameter: single pre-entry momentum threshold sweep
- parameter group: pre_momentum
- old value: none
- new value: ['pre3_close_pos', '>=', 0.385701]
- command: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\scripts\full_loop_a_pullback_c2_high.py --max_iterations 200`
- FIT metrics: trades=1482, wins=510, losses=972, win_rate=34.41%, PF=0.3885, net=Rs -496,705, avg_win=Rs 619, avg_loss=Rs -836, SL/TGT/EOD=815/456/211, top_trade/day/symbol=0.0021/None/None
- VAL metrics: trades=958, wins=275, losses=683, win_rate=28.71%, PF=0.2925, net=Rs -394,221, avg_win=Rs 593, avg_loss=Rs -816, SL/TGT/EOD=540/228/190, top_trade/day/symbol=0.0041/None/None
- full TRAIN metrics: trades=2440, wins=785, losses=1655, win_rate=32.17%, PF=0.3494, net=Rs -890,926, avg_win=Rs 610, avg_loss=Rs -827, SL/TGT/EOD=1355/684/401, top_trade/day/symbol=0.0014/None/None
- TEST metrics: not run
- keep/reject: REJECT
- failure classification: train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- next action: continue train-side search

## Iteration 119 - premom_pre3_dir_count>=1.0
- changed rule/parameter: single pre-entry momentum threshold sweep
- parameter group: pre_momentum
- old value: none
- new value: ['pre3_dir_count', '>=', 1.0]
- command: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\scripts\full_loop_a_pullback_c2_high.py --max_iterations 200`
- FIT metrics: trades=1514, wins=513, losses=1001, win_rate=33.88%, PF=0.3788, net=Rs -519,295, avg_win=Rs 617, avg_loss=Rs -835, SL/TGT/EOD=838/456/220, top_trade/day/symbol=0.0021/None/None
- VAL metrics: trades=1046, wins=305, losses=741, win_rate=29.16%, PF=0.2977, net=Rs -427,570, avg_win=Rs 594, avg_loss=Rs -822, SL/TGT/EOD=590/257/199, top_trade/day/symbol=0.0037/None/None
- full TRAIN metrics: trades=2560, wins=818, losses=1742, win_rate=31.95%, PF=0.3447, net=Rs -946,865, avg_win=Rs 609, avg_loss=Rs -829, SL/TGT/EOD=1428/713/419, top_trade/day/symbol=0.0013/None/None
- TEST metrics: not run
- keep/reject: REJECT
- failure classification: train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- next action: continue train-side search

## Iteration 120 - premom_pre3_mom_r>=-0.022488
- changed rule/parameter: single pre-entry momentum threshold sweep
- parameter group: pre_momentum
- old value: none
- new value: ['pre3_mom_r', '>=', -0.022488]
- command: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\scripts\full_loop_a_pullback_c2_high.py --max_iterations 200`
- FIT metrics: trades=1501, wins=510, losses=991, win_rate=33.98%, PF=0.3812, net=Rs -514,111, avg_win=Rs 621, avg_loss=Rs -838, SL/TGT/EOD=834/460/207, top_trade/day/symbol=0.0021/None/None
- VAL metrics: trades=969, wins=277, losses=692, win_rate=28.59%, PF=0.2918, net=Rs -398,895, avg_win=Rs 593, avg_loss=Rs -814, SL/TGT/EOD=543/232/194, top_trade/day/symbol=0.0041/None/None
- full TRAIN metrics: trades=2470, wins=787, losses=1683, win_rate=31.86%, PF=0.3451, net=Rs -913,006, avg_win=Rs 611, avg_loss=Rs -828, SL/TGT/EOD=1377/692/401, top_trade/day/symbol=0.0014/None/None
- TEST metrics: not run
- keep/reject: REJECT
- failure classification: train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- next action: continue train-side search

## Iteration 121 - premom_pre3_range_r>=0.227447
- changed rule/parameter: single pre-entry momentum threshold sweep
- parameter group: pre_momentum
- old value: none
- new value: ['pre3_range_r', '>=', 0.227447]
- command: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\scripts\full_loop_a_pullback_c2_high.py --max_iterations 200`
- FIT metrics: trades=1525, wins=526, losses=999, win_rate=34.49%, PF=0.3885, net=Rs -514,699, avg_win=Rs 622, avg_loss=Rs -843, SL/TGT/EOD=853/475/197, top_trade/day/symbol=0.002/None/None
- VAL metrics: trades=1090, wins=306, losses=784, win_rate=28.07%, PF=0.2847, net=Rs -469,622, avg_win=Rs 611, avg_loss=Rs -837, SL/TGT/EOD=644/268/178, top_trade/day/symbol=0.0036/None/None
- full TRAIN metrics: trades=2615, wins=832, losses=1783, win_rate=31.82%, PF=0.343, net=Rs -984,322, avg_win=Rs 618, avg_loss=Rs -840, SL/TGT/EOD=1497/743/375, top_trade/day/symbol=0.0013/None/None
- TEST metrics: not run
- keep/reject: REJECT
- failure classification: train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- next action: continue train-side search

## Iteration 122 - premom_pre3_vol_ratio20>=0.50685
- changed rule/parameter: single pre-entry momentum threshold sweep
- parameter group: pre_momentum
- old value: none
- new value: ['pre3_vol_ratio20', '>=', 0.50685]
- command: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\scripts\full_loop_a_pullback_c2_high.py --max_iterations 200`
- FIT metrics: trades=1515, wins=526, losses=989, win_rate=34.72%, PF=0.3909, net=Rs -505,068, avg_win=Rs 616, avg_loss=Rs -838, SL/TGT/EOD=835/467/213, top_trade/day/symbol=0.0021/None/None
- VAL metrics: trades=1039, wins=303, losses=736, win_rate=29.16%, PF=0.2992, net=Rs -426,257, avg_win=Rs 600, avg_loss=Rs -826, SL/TGT/EOD=592/260/187, top_trade/day/symbol=0.0037/None/None
- full TRAIN metrics: trades=2554, wins=829, losses=1725, win_rate=32.46%, PF=0.3521, net=Rs -931,325, avg_win=Rs 611, avg_loss=Rs -833, SL/TGT/EOD=1427/727/400, top_trade/day/symbol=0.0013/None/None
- TEST metrics: not run
- keep/reject: REJECT
- failure classification: train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- next action: continue train-side search

## Iteration 123 - premom_pre5_body_sum_r>=0.01455
- changed rule/parameter: single pre-entry momentum threshold sweep
- parameter group: pre_momentum
- old value: none
- new value: ['pre5_body_sum_r', '>=', 0.01455]
- command: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\scripts\full_loop_a_pullback_c2_high.py --max_iterations 200`
- FIT metrics: trades=1498, wins=502, losses=996, win_rate=33.51%, PF=0.3738, net=Rs -522,452, avg_win=Rs 621, avg_loss=Rs -838, SL/TGT/EOD=838/451/209, top_trade/day/symbol=0.0021/None/None
- VAL metrics: trades=963, wins=285, losses=678, win_rate=29.6%, PF=0.3066, net=Rs -385,477, avg_win=Rs 598, avg_loss=Rs -820, SL/TGT/EOD=542/240/181, top_trade/day/symbol=0.0039/None/None
- full TRAIN metrics: trades=2461, wins=787, losses=1674, win_rate=31.98%, PF=0.3469, net=Rs -907,929, avg_win=Rs 613, avg_loss=Rs -830, SL/TGT/EOD=1380/691/390, top_trade/day/symbol=0.0014/None/None
- TEST metrics: not run
- keep/reject: REJECT
- failure classification: train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- next action: continue train-side search

## Iteration 124 - premom_pre5_close_pos>=0.592587
- changed rule/parameter: single pre-entry momentum threshold sweep
- parameter group: pre_momentum
- old value: none
- new value: ['pre5_close_pos', '>=', 0.592587]
- command: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\scripts\full_loop_a_pullback_c2_high.py --max_iterations 200`
- FIT metrics: trades=1489, wins=505, losses=984, win_rate=33.92%, PF=0.3806, net=Rs -511,529, avg_win=Rs 622, avg_loss=Rs -839, SL/TGT/EOD=830/455/204, top_trade/day/symbol=0.0021/None/None
- VAL metrics: trades=924, wins=264, losses=660, win_rate=28.57%, PF=0.2885, net=Rs -383,612, avg_win=Rs 589, avg_loss=Rs -817, SL/TGT/EOD=522/217/185, top_trade/day/symbol=0.0043/None/None
- full TRAIN metrics: trades=2413, wins=769, losses=1644, win_rate=31.87%, PF=0.3442, net=Rs -895,141, avg_win=Rs 611, avg_loss=Rs -830, SL/TGT/EOD=1352/672/389, top_trade/day/symbol=0.0014/None/None
- TEST metrics: not run
- keep/reject: REJECT
- failure classification: train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- next action: continue train-side search

## Iteration 125 - premom_pre5_dir_count>=2.0
- changed rule/parameter: single pre-entry momentum threshold sweep
- parameter group: pre_momentum
- old value: none
- new value: ['pre5_dir_count', '>=', 2.0]
- command: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\scripts\full_loop_a_pullback_c2_high.py --max_iterations 200`
- FIT metrics: trades=1504, wins=514, losses=990, win_rate=34.18%, PF=0.3827, net=Rs -509,865, avg_win=Rs 615, avg_loss=Rs -834, SL/TGT/EOD=828/452/224, top_trade/day/symbol=0.0021/None/None
- VAL metrics: trades=1030, wins=306, losses=724, win_rate=29.71%, PF=0.3075, net=Rs -413,104, avg_win=Rs 600, avg_loss=Rs -824, SL/TGT/EOD=581/261/188, top_trade/day/symbol=0.0036/None/None
- full TRAIN metrics: trades=2534, wins=820, losses=1714, win_rate=32.36%, PF=0.3512, net=Rs -922,969, avg_win=Rs 609, avg_loss=Rs -830, SL/TGT/EOD=1409/713/412, top_trade/day/symbol=0.0013/None/None
- TEST metrics: not run
- keep/reject: REJECT
- failure classification: train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- next action: continue train-side search

## Iteration 126 - premom_pre5_mom_r>=0.040896
- changed rule/parameter: single pre-entry momentum threshold sweep
- parameter group: pre_momentum
- old value: none
- new value: ['pre5_mom_r', '>=', 0.040896]
- command: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\scripts\full_loop_a_pullback_c2_high.py --max_iterations 200`
- FIT metrics: trades=1478, wins=503, losses=975, win_rate=34.03%, PF=0.3828, net=Rs -504,569, avg_win=Rs 622, avg_loss=Rs -839, SL/TGT/EOD=822/452/204, top_trade/day/symbol=0.0021/None/None
- VAL metrics: trades=962, wins=278, losses=684, win_rate=28.9%, PF=0.2939, net=Rs -396,971, avg_win=Rs 594, avg_loss=Rs -822, SL/TGT/EOD=549/232/181, top_trade/day/symbol=0.004/None/None
- full TRAIN metrics: trades=2440, wins=781, losses=1659, win_rate=32.01%, PF=0.3466, net=Rs -901,540, avg_win=Rs 612, avg_loss=Rs -832, SL/TGT/EOD=1371/684/385, top_trade/day/symbol=0.0014/None/None
- TEST metrics: not run
- keep/reject: REJECT
- failure classification: train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- next action: continue train-side search

## Iteration 127 - premom_pre5_range_r>=0.374369
- changed rule/parameter: single pre-entry momentum threshold sweep
- parameter group: pre_momentum
- old value: none
- new value: ['pre5_range_r', '>=', 0.374369]
- command: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\scripts\full_loop_a_pullback_c2_high.py --max_iterations 200`
- FIT metrics: trades=1503, wins=505, losses=998, win_rate=33.6%, PF=0.3714, net=Rs -529,720, avg_win=Rs 620, avg_loss=Rs -844, SL/TGT/EOD=852/452/199, top_trade/day/symbol=0.0021/None/None
- VAL metrics: trades=1070, wins=300, losses=770, win_rate=28.04%, PF=0.2744, net=Rs -473,129, avg_win=Rs 597, avg_loss=Rs -847, SL/TGT/EOD=647/255/168, top_trade/day/symbol=0.0037/None/None
- full TRAIN metrics: trades=2573, wins=805, losses=1768, win_rate=31.29%, PF=0.3291, net=Rs -1,002,850, avg_win=Rs 611, avg_loss=Rs -845, SL/TGT/EOD=1499/707/367, top_trade/day/symbol=0.0014/None/None
- TEST metrics: not run
- keep/reject: REJECT
- failure classification: train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- next action: continue train-side search

## Iteration 128 - premom_pre5_vol_ratio20>=0.90386
- changed rule/parameter: single pre-entry momentum threshold sweep
- parameter group: pre_momentum
- old value: none
- new value: ['pre5_vol_ratio20', '>=', 0.90386]
- command: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\scripts\full_loop_a_pullback_c2_high.py --max_iterations 200`
- FIT metrics: trades=1491, wins=507, losses=984, win_rate=34.0%, PF=0.3819, net=Rs -509,637, avg_win=Rs 621, avg_loss=Rs -838, SL/TGT/EOD=828/457/206, top_trade/day/symbol=0.0021/None/None
- VAL metrics: trades=1000, wins=287, losses=713, win_rate=28.7%, PF=0.2883, net=Rs -421,287, avg_win=Rs 595, avg_loss=Rs -830, SL/TGT/EOD=579/243/178, top_trade/day/symbol=0.0039/None/None
- full TRAIN metrics: trades=2491, wins=794, losses=1697, win_rate=31.87%, PF=0.3428, net=Rs -930,924, avg_win=Rs 612, avg_loss=Rs -835, SL/TGT/EOD=1407/700/384, top_trade/day/symbol=0.0014/None/None
- TEST metrics: not run
- keep/reject: REJECT
- failure classification: train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- next action: continue train-side search

## Iteration 129 - premom_pre_entry_momentum_score>=51.369564
- changed rule/parameter: single pre-entry momentum threshold sweep
- parameter group: pre_momentum
- old value: none
- new value: ['pre_entry_momentum_score', '>=', 51.369564]
- command: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\scripts\full_loop_a_pullback_c2_high.py --max_iterations 200`
- FIT metrics: trades=1500, wins=509, losses=991, win_rate=33.93%, PF=0.3786, net=Rs -517,118, avg_win=Rs 619, avg_loss=Rs -840, SL/TGT/EOD=836/454/210, top_trade/day/symbol=0.0021/None/None
- VAL metrics: trades=931, wins=264, losses=667, win_rate=28.36%, PF=0.2862, net=Rs -388,503, avg_win=Rs 590, avg_loss=Rs -816, SL/TGT/EOD=527/218/186, top_trade/day/symbol=0.0043/None/None
- full TRAIN metrics: trades=2431, wins=773, losses=1658, win_rate=31.8%, PF=0.342, net=Rs -905,621, avg_win=Rs 609, avg_loss=Rs -830, SL/TGT/EOD=1363/672/396, top_trade/day/symbol=0.0014/None/None
- TEST metrics: not run
- keep/reject: REJECT
- failure classification: train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- next action: continue train-side search

## Iteration 130 - premom_sig5_adx_calc>=17.161032
- changed rule/parameter: single pre-entry momentum threshold sweep
- parameter group: pre_momentum
- old value: none
- new value: ['sig5_adx_calc', '>=', 17.161032]
- command: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\scripts\full_loop_a_pullback_c2_high.py --max_iterations 200`
- FIT metrics: trades=1470, wins=492, losses=978, win_rate=33.47%, PF=0.3743, net=Rs -512,025, avg_win=Rs 623, avg_loss=Rs -837, SL/TGT/EOD=820/445/205, top_trade/day/symbol=0.0022/None/None
- VAL metrics: trades=1029, wins=302, losses=727, win_rate=29.35%, PF=0.3015, net=Rs -417,580, avg_win=Rs 597, avg_loss=Rs -822, SL/TGT/EOD=584/256/189, top_trade/day/symbol=0.0037/None/None
- full TRAIN metrics: trades=2499, wins=794, losses=1705, win_rate=31.77%, PF=0.3436, net=Rs -929,605, avg_win=Rs 613, avg_loss=Rs -831, SL/TGT/EOD=1404/701/394, top_trade/day/symbol=0.0014/None/None
- TEST metrics: not run
- keep/reject: REJECT
- failure classification: train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- next action: continue train-side search

## Iteration 131 - premom_sig5_rsi_dir>=56.728215
- changed rule/parameter: single pre-entry momentum threshold sweep
- parameter group: pre_momentum
- old value: none
- new value: ['sig5_rsi_dir', '>=', 56.728215]
- command: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\scripts\full_loop_a_pullback_c2_high.py --max_iterations 200`
- FIT metrics: trades=1471, wins=502, losses=969, win_rate=34.13%, PF=0.3836, net=Rs -498,042, avg_win=Rs 617, avg_loss=Rs -834, SL/TGT/EOD=806/449/216, top_trade/day/symbol=0.0022/None/None
- VAL metrics: trades=1042, wins=297, losses=745, win_rate=28.5%, PF=0.2841, net=Rs -446,684, avg_win=Rs 597, avg_loss=Rs -837, SL/TGT/EOD=610/253/179, top_trade/day/symbol=0.0038/None/None
- full TRAIN metrics: trades=2513, wins=799, losses=1714, win_rate=31.79%, PF=0.3402, net=Rs -944,726, avg_win=Rs 610, avg_loss=Rs -835, SL/TGT/EOD=1416/702/395, top_trade/day/symbol=0.0014/None/None
- TEST metrics: not run
- keep/reject: REJECT
- failure classification: train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- next action: continue train-side search

## Iteration 132 - premom_sig5_vol_ratio20>=1.836058
- changed rule/parameter: single pre-entry momentum threshold sweep
- parameter group: pre_momentum
- old value: none
- new value: ['sig5_vol_ratio20', '>=', 1.836058]
- command: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\scripts\full_loop_a_pullback_c2_high.py --max_iterations 200`
- FIT metrics: trades=1468, wins=475, losses=993, win_rate=32.36%, PF=0.3534, net=Rs -535,722, avg_win=Rs 616, avg_loss=Rs -834, SL/TGT/EOD=827/422/219, top_trade/day/symbol=0.0023/None/None
- VAL metrics: trades=996, wins=281, losses=715, win_rate=28.21%, PF=0.2823, net=Rs -424,602, avg_win=Rs 594, avg_loss=Rs -827, SL/TGT/EOD=575/237/184, top_trade/day/symbol=0.004/None/None
- full TRAIN metrics: trades=2464, wins=756, losses=1708, win_rate=30.68%, PF=0.3238, net=Rs -960,323, avg_win=Rs 608, avg_loss=Rs -832, SL/TGT/EOD=1402/659/403, top_trade/day/symbol=0.0015/None/None
- TEST metrics: not run
- keep/reject: REJECT
- failure classification: train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- next action: continue train-side search

## Iteration 133 - premom_pre10_body_sum_r<=0.139794
- changed rule/parameter: single pre-entry momentum threshold sweep
- parameter group: pre_momentum
- old value: none
- new value: ['pre10_body_sum_r', '<=', 0.139794]
- command: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\scripts\full_loop_a_pullback_c2_high.py --max_iterations 200`
- FIT metrics: trades=558, wins=184, losses=374, win_rate=32.97%, PF=0.383, net=Rs -178,770, avg_win=Rs 603, avg_loss=Rs -775, SL/TGT/EOD=272/156/130, top_trade/day/symbol=0.006/None/None
- VAL metrics: trades=303, wins=60, losses=243, win_rate=19.8%, PF=0.1763, net=Rs -147,884, avg_win=Rs 528, avg_loss=Rs -739, SL/TGT/EOD=160/41/102, top_trade/day/symbol=0.0211/None/None
- full TRAIN metrics: trades=861, wins=244, losses=617, win_rate=28.34%, PF=0.3039, net=Rs -326,654, avg_win=Rs 585, avg_loss=Rs -761, SL/TGT/EOD=432/197/232, top_trade/day/symbol=0.0047/None/None
- TEST metrics: not run
- keep/reject: REJECT
- failure classification: train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- next action: continue train-side search

## Iteration 134 - premom_pre10_close_pos<=0.676894
- changed rule/parameter: single pre-entry momentum threshold sweep
- parameter group: pre_momentum
- old value: none
- new value: ['pre10_close_pos', '<=', 0.676894]
- command: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\scripts\full_loop_a_pullback_c2_high.py --max_iterations 200`
- FIT metrics: trades=391, wins=122, losses=269, win_rate=31.2%, PF=0.3438, net=Rs -140,057, avg_win=Rs 602, avg_loss=Rs -793, SL/TGT/EOD=205/101/85, top_trade/day/symbol=0.0091/None/None
- VAL metrics: trades=461, wins=126, losses=335, win_rate=27.33%, PF=0.2786, net=Rs -191,964, avg_win=Rs 588, avg_loss=Rs -794, SL/TGT/EOD=252/107/102, top_trade/day/symbol=0.009/None/None
- full TRAIN metrics: trades=852, wins=248, losses=604, win_rate=29.11%, PF=0.3076, net=Rs -332,021, avg_win=Rs 595, avg_loss=Rs -794, SL/TGT/EOD=457/208/187, top_trade/day/symbol=0.0045/None/None
- TEST metrics: not run
- keep/reject: REJECT
- failure classification: train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- next action: continue train-side search

## Iteration 135 - premom_pre10_dir_count<=3.0
- changed rule/parameter: single pre-entry momentum threshold sweep
- parameter group: pre_momentum
- old value: none
- new value: ['pre10_dir_count', '<=', 3.0]
- command: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\scripts\full_loop_a_pullback_c2_high.py --max_iterations 200`
- FIT metrics: trades=648, wins=207, losses=441, win_rate=31.94%, PF=0.3465, net=Rs -228,966, avg_win=Rs 586, avg_loss=Rs -794, SL/TGT/EOD=342/175/131, top_trade/day/symbol=0.0055/None/None
- VAL metrics: trades=366, wins=93, losses=273, win_rate=25.41%, PF=0.2341, net=Rs -167,384, avg_win=Rs 550, avg_loss=Rs -800, SL/TGT/EOD=205/70/91, top_trade/day/symbol=0.013/None/None
- full TRAIN metrics: trades=1014, wins=300, losses=714, win_rate=29.59%, PF=0.3033, net=Rs -396,350, avg_win=Rs 575, avg_loss=Rs -797, SL/TGT/EOD=547/245/222, top_trade/day/symbol=0.0039/None/None
- TEST metrics: not run
- keep/reject: REJECT
- failure classification: train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- next action: continue train-side search

## Iteration 136 - premom_pre10_mom_r<=0.204232
- changed rule/parameter: single pre-entry momentum threshold sweep
- parameter group: pre_momentum
- old value: none
- new value: ['pre10_mom_r', '<=', 0.204232]
- command: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\scripts\full_loop_a_pullback_c2_high.py --max_iterations 200`
- FIT metrics: trades=559, wins=181, losses=378, win_rate=32.38%, PF=0.3734, net=Rs -181,706, avg_win=Rs 598, avg_loss=Rs -767, SL/TGT/EOD=270/150/139, top_trade/day/symbol=0.0062/None/None
- VAL metrics: trades=303, wins=66, losses=237, win_rate=21.78%, PF=0.2086, net=Rs -133,936, avg_win=Rs 535, avg_loss=Rs -714, SL/TGT/EOD=145/47/111, top_trade/day/symbol=0.0189/None/None
- full TRAIN metrics: trades=862, wins=247, losses=615, win_rate=28.65%, PF=0.3126, net=Rs -315,642, avg_win=Rs 581, avg_loss=Rs -747, SL/TGT/EOD=415/197/250, top_trade/day/symbol=0.0046/None/None
- TEST metrics: not run
- keep/reject: REJECT
- failure classification: train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- next action: continue train-side search

## Iteration 137 - premom_pre10_range_r<=0.556437
- changed rule/parameter: single pre-entry momentum threshold sweep
- parameter group: pre_momentum
- old value: none
- new value: ['pre10_range_r', '<=', 0.556437]
- command: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\scripts\full_loop_a_pullback_c2_high.py --max_iterations 200`
- FIT metrics: trades=558, wins=178, losses=380, win_rate=31.9%, PF=0.3636, net=Rs -179,780, avg_win=Rs 577, avg_loss=Rs -743, SL/TGT/EOD=252/141/165, top_trade/day/symbol=0.0065/None/None
- VAL metrics: trades=354, wins=87, losses=267, win_rate=24.58%, PF=0.2419, net=Rs -140,299, avg_win=Rs 515, avg_loss=Rs -693, SL/TGT/EOD=157/60/137, top_trade/day/symbol=0.0149/None/None
- full TRAIN metrics: trades=912, wins=265, losses=647, win_rate=29.06%, PF=0.3154, net=Rs -320,079, avg_win=Rs 556, avg_loss=Rs -723, SL/TGT/EOD=409/201/302, top_trade/day/symbol=0.0045/None/None
- TEST metrics: not run
- keep/reject: REJECT
- failure classification: train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- next action: continue train-side search

## Iteration 138 - premom_pre10_vol_ratio20<=1.024219
- changed rule/parameter: single pre-entry momentum threshold sweep
- parameter group: pre_momentum
- old value: none
- new value: ['pre10_vol_ratio20', '<=', 1.024219]
- command: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\scripts\full_loop_a_pullback_c2_high.py --max_iterations 200`
- FIT metrics: trades=597, wins=203, losses=394, win_rate=34.0%, PF=0.3929, net=Rs -192,067, avg_win=Rs 612, avg_loss=Rs -803, SL/TGT/EOD=314/175/108, top_trade/day/symbol=0.0054/None/None
- VAL metrics: trades=288, wins=74, losses=214, win_rate=25.69%, PF=0.252, net=Rs -122,162, avg_win=Rs 556, avg_loss=Rs -763, SL/TGT/EOD=152/57/79, top_trade/day/symbol=0.0162/None/None
- full TRAIN metrics: trades=885, wins=277, losses=608, win_rate=31.3%, PF=0.345, net=Rs -314,229, avg_win=Rs 597, avg_loss=Rs -789, SL/TGT/EOD=466/232/187, top_trade/day/symbol=0.004/None/None
- TEST metrics: not run
- keep/reject: REJECT
- failure classification: train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- next action: continue train-side search

## Iteration 139 - premom_pre1_adx<=20.623265
- changed rule/parameter: single pre-entry momentum threshold sweep
- parameter group: pre_momentum
- old value: none
- new value: ['pre1_adx', '<=', 20.623265]
- command: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\scripts\full_loop_a_pullback_c2_high.py --max_iterations 200`
- FIT metrics: trades=659, wins=222, losses=437, win_rate=33.69%, PF=0.379, net=Rs -218,102, avg_win=Rs 600, avg_loss=Rs -804, SL/TGT/EOD=339/188/132, top_trade/day/symbol=0.005/None/None
- VAL metrics: trades=411, wins=112, losses=299, win_rate=27.25%, PF=0.2638, net=Rs -170,486, avg_win=Rs 545, avg_loss=Rs -774, SL/TGT/EOD=213/82/116, top_trade/day/symbol=0.0109/None/None
- full TRAIN metrics: trades=1070, wins=334, losses=736, win_rate=31.21%, PF=0.3332, net=Rs -388,588, avg_win=Rs 581, avg_loss=Rs -792, SL/TGT/EOD=552/270/248, top_trade/day/symbol=0.0034/None/None
- TEST metrics: not run
- keep/reject: REJECT
- failure classification: train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- next action: continue train-side search

## Iteration 140 - combo_atr_pct_body_pct_sl0.7_t1.8
- changed rule/parameter: two-term structural combination plus exit rescue
- parameter group: combination
- old value: single/no mask
- new value: {'terms': [['atr_pct', '>=', 0.001937], ['body_pct', '>=', 0.620145]], 'exit': [0.7, 1.8]}
- command: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\scripts\full_loop_a_pullback_c2_high.py --max_iterations 200`
- FIT metrics: trades=1212, wins=312, losses=900, win_rate=25.74%, PF=0.4267, net=Rs -435,589, avg_win=Rs 1,039, avg_loss=Rs -844, SL/TGT/EOD=773/158/281, top_trade/day/symbol=0.0048/None/None
- VAL metrics: trades=899, wins=195, losses=704, win_rate=21.69%, PF=0.33, net=Rs -390,057, avg_win=Rs 985, avg_loss=Rs -827, SL/TGT/EOD=572/95/232, top_trade/day/symbol=0.0082/None/None
- full TRAIN metrics: trades=2111, wins=507, losses=1604, win_rate=24.02%, PF=0.3848, net=Rs -825,646, avg_win=Rs 1,019, avg_loss=Rs -837, SL/TGT/EOD=1345/253/513, top_trade/day/symbol=0.003/None/None
- TEST metrics: not run
- keep/reject: REJECT
- failure classification: train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- next action: continue train-side search

## Iteration 141 - combo_atr_pct_body_pct_sl0.85_t1.8
- changed rule/parameter: two-term structural combination plus exit rescue
- parameter group: combination
- old value: single/no mask
- new value: {'terms': [['atr_pct', '>=', 0.001937], ['body_pct', '>=', 0.620145]], 'exit': [0.85, 1.8]}
- command: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\scripts\full_loop_a_pullback_c2_high.py --max_iterations 200`
- FIT metrics: trades=1061, wins=304, losses=757, win_rate=28.65%, PF=0.476, net=Rs -367,965, avg_win=Rs 1,100, avg_loss=Rs -928, SL/TGT/EOD=594/168/299, top_trade/day/symbol=0.0047/None/None
- VAL metrics: trades=818, wins=191, losses=627, win_rate=23.35%, PF=0.3392, net=Rs -378,979, avg_win=Rs 1,019, avg_loss=Rs -915, SL/TGT/EOD=469/96/253, top_trade/day/symbol=0.0081/None/None
- full TRAIN metrics: trades=1879, wins=495, losses=1384, win_rate=26.34%, PF=0.4145, net=Rs -746,944, avg_win=Rs 1,068, avg_loss=Rs -922, SL/TGT/EOD=1063/264/552, top_trade/day/symbol=0.003/None/None
- TEST metrics: not run
- keep/reject: REJECT
- failure classification: train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- next action: continue train-side search

## Iteration 142 - combo_atr_pct_body_pct_sl0.85_t2.0
- changed rule/parameter: two-term structural combination plus exit rescue
- parameter group: combination
- old value: single/no mask
- new value: {'terms': [['atr_pct', '>=', 0.001937], ['body_pct', '>=', 0.620145]], 'exit': [0.85, 2.0]}
- command: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\scripts\full_loop_a_pullback_c2_high.py --max_iterations 200`
- FIT metrics: trades=1023, wins=286, losses=737, win_rate=27.96%, PF=0.466, net=Rs -363,413, avg_win=Rs 1,109, avg_loss=Rs -923, SL/TGT/EOD=575/128/320, top_trade/day/symbol=0.0056/None/None
- VAL metrics: trades=801, wins=183, losses=618, win_rate=22.85%, PF=0.3491, net=Rs -365,901, avg_win=Rs 1,072, avg_loss=Rs -910, SL/TGT/EOD=458/83/260, top_trade/day/symbol=0.009/None/None
- full TRAIN metrics: trades=1824, wins=469, losses=1355, win_rate=25.71%, PF=0.4131, net=Rs -729,313, avg_win=Rs 1,095, avg_loss=Rs -917, SL/TGT/EOD=1033/211/580, top_trade/day/symbol=0.0034/None/None
- TEST metrics: not run
- keep/reject: REJECT
- failure classification: train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- next action: continue train-side search

## Iteration 143 - combo_atr_pct_body_pct_sl1.0_t2.0
- changed rule/parameter: two-term structural combination plus exit rescue
- parameter group: combination
- old value: single/no mask
- new value: {'terms': [['atr_pct', '>=', 0.001937], ['body_pct', '>=', 0.620145]], 'exit': [1.0, 2.0]}
- command: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\scripts\full_loop_a_pullback_c2_high.py --max_iterations 200`
- FIT metrics: trades=943, wins=271, losses=672, win_rate=28.74%, PF=0.4532, net=Rs -362,204, avg_win=Rs 1,108, avg_loss=Rs -986, SL/TGT/EOD=467/122/354, top_trade/day/symbol=0.0059/None/None
- VAL metrics: trades=717, wins=179, losses=538, win_rate=24.97%, PF=0.3576, net=Rs -338,152, avg_win=Rs 1,052, avg_loss=Rs -978, SL/TGT/EOD=354/78/285, top_trade/day/symbol=0.0094/None/None
- full TRAIN metrics: trades=1660, wins=450, losses=1210, win_rate=27.11%, PF=0.4109, net=Rs -700,356, avg_win=Rs 1,085, avg_loss=Rs -982, SL/TGT/EOD=821/200/639, top_trade/day/symbol=0.0036/None/None
- TEST metrics: not run
- keep/reject: REJECT
- failure classification: train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- next action: continue train-side search

## Iteration 144 - combo_atr_pct_body_pct_sl1.1_t2.5
- changed rule/parameter: two-term structural combination plus exit rescue
- parameter group: combination
- old value: single/no mask
- new value: {'terms': [['atr_pct', '>=', 0.001937], ['body_pct', '>=', 0.620145]], 'exit': [1.1, 2.5]}
- command: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\scripts\full_loop_a_pullback_c2_high.py --max_iterations 200`
- FIT metrics: trades=869, wins=247, losses=622, win_rate=28.42%, PF=0.4497, net=Rs -350,776, avg_win=Rs 1,161, avg_loss=Rs -1,025, SL/TGT/EOD=400/78/391, top_trade/day/symbol=0.0079/None/None
- VAL metrics: trades=661, wins=171, losses=490, win_rate=25.87%, PF=0.3755, net=Rs -311,617, avg_win=Rs 1,096, avg_loss=Rs -1,018, SL/TGT/EOD=299/49/313, top_trade/day/symbol=0.0121/None/None
- full TRAIN metrics: trades=1530, wins=418, losses=1112, win_rate=27.32%, PF=0.4171, net=Rs -662,392, avg_win=Rs 1,134, avg_loss=Rs -1,022, SL/TGT/EOD=699/127/704, top_trade/day/symbol=0.0048/None/None
- TEST metrics: not run
- keep/reject: REJECT
- failure classification: train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- next action: continue train-side search

## Iteration 145 - combo_atr_pct_close_loc_sl0.7_t1.8
- changed rule/parameter: two-term structural combination plus exit rescue
- parameter group: combination
- old value: single/no mask
- new value: {'terms': [['atr_pct', '>=', 0.001937], ['close_loc', '>=', 0.731712]], 'exit': [0.7, 1.8]}
- command: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\scripts\full_loop_a_pullback_c2_high.py --max_iterations 200`
- FIT metrics: trades=1206, wins=321, losses=885, win_rate=26.62%, PF=0.4536, net=Rs -405,122, avg_win=Rs 1,048, avg_loss=Rs -838, SL/TGT/EOD=752/165/289, top_trade/day/symbol=0.0047/None/None
- VAL metrics: trades=892, wins=184, losses=708, win_rate=20.63%, PF=0.3141, net=Rs -398,426, avg_win=Rs 992, avg_loss=Rs -820, SL/TGT/EOD=569/91/232, top_trade/day/symbol=0.0086/None/None
- full TRAIN metrics: trades=2098, wins=505, losses=1593, win_rate=24.07%, PF=0.3923, net=Rs -803,548, avg_win=Rs 1,027, avg_loss=Rs -830, SL/TGT/EOD=1321/256/521, top_trade/day/symbol=0.003/None/None
- TEST metrics: not run
- keep/reject: REJECT
- failure classification: train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- next action: continue train-side search

## Iteration 146 - combo_atr_pct_close_loc_sl0.85_t1.8
- changed rule/parameter: two-term structural combination plus exit rescue
- parameter group: combination
- old value: single/no mask
- new value: {'terms': [['atr_pct', '>=', 0.001937], ['close_loc', '>=', 0.731712]], 'exit': [0.85, 1.8]}
- command: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\scripts\full_loop_a_pullback_c2_high.py --max_iterations 200`
- FIT metrics: trades=1070, wins=317, losses=753, win_rate=29.63%, PF=0.4885, net=Rs -355,259, avg_win=Rs 1,070, avg_loss=Rs -922, SL/TGT/EOD=584/171/315, top_trade/day/symbol=0.0046/None/None
- VAL metrics: trades=809, wins=175, losses=634, win_rate=21.63%, PF=0.3131, net=Rs -394,532, avg_win=Rs 1,028, avg_loss=Rs -906, SL/TGT/EOD=467/90/252, top_trade/day/symbol=0.0087/None/None
- full TRAIN metrics: trades=1879, wins=492, losses=1387, win_rate=26.18%, PF=0.4091, net=Rs -749,791, avg_win=Rs 1,055, avg_loss=Rs -915, SL/TGT/EOD=1051/261/567, top_trade/day/symbol=0.003/None/None
- TEST metrics: not run
- keep/reject: REJECT
- failure classification: train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- next action: continue train-side search

## Iteration 147 - combo_atr_pct_close_loc_sl0.85_t2.0
- changed rule/parameter: two-term structural combination plus exit rescue
- parameter group: combination
- old value: single/no mask
- new value: {'terms': [['atr_pct', '>=', 0.001937], ['close_loc', '>=', 0.731712]], 'exit': [0.85, 2.0]}
- command: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\scripts\full_loop_a_pullback_c2_high.py --max_iterations 200`
- FIT metrics: trades=1026, wins=293, losses=733, win_rate=28.56%, PF=0.4681, net=Rs -357,369, avg_win=Rs 1,074, avg_loss=Rs -917, SL/TGT/EOD=567/125/334, top_trade/day/symbol=0.0056/None/None
- VAL metrics: trades=794, wins=167, losses=627, win_rate=21.03%, PF=0.314, net=Rs -390,447, avg_win=Rs 1,070, avg_loss=Rs -908, SL/TGT/EOD=463/76/255, top_trade/day/symbol=0.0099/None/None
- full TRAIN metrics: trades=1820, wins=460, losses=1360, win_rate=25.27%, PF=0.3975, net=Rs -747,816, avg_win=Rs 1,072, avg_loss=Rs -913, SL/TGT/EOD=1030/201/589, top_trade/day/symbol=0.0036/None/None
- TEST metrics: not run
- keep/reject: REJECT
- failure classification: train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- next action: continue train-side search

## Iteration 148 - combo_atr_pct_close_loc_sl1.0_t2.0
- changed rule/parameter: two-term structural combination plus exit rescue
- parameter group: combination
- old value: single/no mask
- new value: {'terms': [['atr_pct', '>=', 0.001937], ['close_loc', '>=', 0.731712]], 'exit': [1.0, 2.0]}
- command: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\scripts\full_loop_a_pullback_c2_high.py --max_iterations 200`
- FIT metrics: trades=951, wins=283, losses=668, win_rate=29.76%, PF=0.4627, net=Rs -352,900, avg_win=Rs 1,074, avg_loss=Rs -983, SL/TGT/EOD=463/122/366, top_trade/day/symbol=0.0058/None/None
- VAL metrics: trades=703, wins=165, losses=538, win_rate=23.47%, PF=0.3298, net=Rs -350,474, avg_win=Rs 1,045, avg_loss=Rs -972, SL/TGT/EOD=351/71/281, top_trade/day/symbol=0.0102/None/None
- full TRAIN metrics: trades=1654, wins=448, losses=1206, win_rate=27.09%, PF=0.4038, net=Rs -703,374, avg_win=Rs 1,063, avg_loss=Rs -978, SL/TGT/EOD=814/193/647, top_trade/day/symbol=0.0037/None/None
- TEST metrics: not run
- keep/reject: REJECT
- failure classification: train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- next action: continue train-side search

## Iteration 149 - combo_atr_pct_close_loc_sl1.1_t2.5
- changed rule/parameter: two-term structural combination plus exit rescue
- parameter group: combination
- old value: single/no mask
- new value: {'terms': [['atr_pct', '>=', 0.001937], ['close_loc', '>=', 0.731712]], 'exit': [1.1, 2.5]}
- command: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\scripts\full_loop_a_pullback_c2_high.py --max_iterations 200`
- FIT metrics: trades=867, wins=255, losses=612, win_rate=29.41%, PF=0.4813, net=Rs -323,831, avg_win=Rs 1,179, avg_loss=Rs -1,020, SL/TGT/EOD=394/86/387, top_trade/day/symbol=0.0075/None/None
- VAL metrics: trades=655, wins=164, losses=491, win_rate=25.04%, PF=0.3517, net=Rs -325,536, avg_win=Rs 1,077, avg_loss=Rs -1,023, SL/TGT/EOD=304/44/307, top_trade/day/symbol=0.0128/None/None
- full TRAIN metrics: trades=1522, wins=419, losses=1103, win_rate=27.53%, PF=0.4236, net=Rs -649,367, avg_win=Rs 1,139, avg_loss=Rs -1,021, SL/TGT/EOD=698/130/694, top_trade/day/symbol=0.0047/None/None
- TEST metrics: not run
- keep/reject: REJECT
- failure classification: train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- next action: continue train-side search

## Iteration 150 - combo_atr_pct_lower_wick_pct_sl0.7_t1.8
- changed rule/parameter: two-term structural combination plus exit rescue
- parameter group: combination
- old value: single/no mask
- new value: {'terms': [['atr_pct', '>=', 0.001937], ['lower_wick_pct', '>=', 0.0]], 'exit': [0.7, 1.8]}
- command: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\scripts\full_loop_a_pullback_c2_high.py --max_iterations 200`
- FIT metrics: trades=1248, wins=323, losses=925, win_rate=25.88%, PF=0.4328, net=Rs -438,250, avg_win=Rs 1,035, avg_loss=Rs -835, SL/TGT/EOD=779/163/306, top_trade/day/symbol=0.0047/None/None
- VAL metrics: trades=938, wins=211, losses=727, win_rate=22.49%, PF=0.3544, net=Rs -390,343, avg_win=Rs 1,015, avg_loss=Rs -832, SL/TGT/EOD=596/107/235, top_trade/day/symbol=0.0073/None/None
- full TRAIN metrics: trades=2186, wins=534, losses=1652, win_rate=24.43%, PF=0.3984, net=Rs -828,593, avg_win=Rs 1,027, avg_loss=Rs -834, SL/TGT/EOD=1375/270/541, top_trade/day/symbol=0.0029/None/None
- TEST metrics: not run
- keep/reject: REJECT
- failure classification: train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- next action: continue train-side search

## Iteration 151 - combo_atr_pct_lower_wick_pct_sl0.85_t1.8
- changed rule/parameter: two-term structural combination plus exit rescue
- parameter group: combination
- old value: single/no mask
- new value: {'terms': [['atr_pct', '>=', 0.001937], ['lower_wick_pct', '>=', 0.0]], 'exit': [0.85, 1.8]}
- command: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\scripts\full_loop_a_pullback_c2_high.py --max_iterations 200`
- FIT metrics: trades=1111, wins=327, losses=784, win_rate=29.43%, PF=0.4864, net=Rs -371,536, avg_win=Rs 1,076, avg_loss=Rs -923, SL/TGT/EOD=614/176/321, top_trade/day/symbol=0.0045/None/None
- VAL metrics: trades=849, wins=202, losses=647, win_rate=23.79%, PF=0.3488, net=Rs -387,380, avg_win=Rs 1,027, avg_loss=Rs -919, SL/TGT/EOD=484/103/262, top_trade/day/symbol=0.0075/None/None
- full TRAIN metrics: trades=1960, wins=529, losses=1431, win_rate=26.99%, PF=0.4243, net=Rs -758,916, avg_win=Rs 1,057, avg_loss=Rs -921, SL/TGT/EOD=1098/279/583, top_trade/day/symbol=0.0028/None/None
- TEST metrics: not run
- keep/reject: REJECT
- failure classification: train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- next action: continue train-side search

## Iteration 152 - combo_atr_pct_lower_wick_pct_sl0.85_t2.0
- changed rule/parameter: two-term structural combination plus exit rescue
- parameter group: combination
- old value: single/no mask
- new value: {'terms': [['atr_pct', '>=', 0.001937], ['lower_wick_pct', '>=', 0.0]], 'exit': [0.85, 2.0]}
- command: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\scripts\full_loop_a_pullback_c2_high.py --max_iterations 200`
- FIT metrics: trades=1081, wins=310, losses=771, win_rate=28.68%, PF=0.4801, net=Rs -368,651, avg_win=Rs 1,098, avg_loss=Rs -920, SL/TGT/EOD=603/139/339, top_trade/day/symbol=0.0052/None/None
- VAL metrics: trades=827, wins=193, losses=634, win_rate=23.34%, PF=0.3554, net=Rs -374,490, avg_win=Rs 1,070, avg_loss=Rs -916, SL/TGT/EOD=471/88/268, top_trade/day/symbol=0.0086/None/None
- full TRAIN metrics: trades=1908, wins=503, losses=1405, win_rate=26.36%, PF=0.424, net=Rs -743,141, avg_win=Rs 1,087, avg_loss=Rs -918, SL/TGT/EOD=1074/227/607, top_trade/day/symbol=0.0032/None/None
- TEST metrics: not run
- keep/reject: REJECT
- failure classification: train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- next action: continue train-side search

## Iteration 153 - combo_atr_pct_lower_wick_pct_sl1.0_t2.0
- changed rule/parameter: two-term structural combination plus exit rescue
- parameter group: combination
- old value: single/no mask
- new value: {'terms': [['atr_pct', '>=', 0.001937], ['lower_wick_pct', '>=', 0.0]], 'exit': [1.0, 2.0]}
- command: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\scripts\full_loop_a_pullback_c2_high.py --max_iterations 200`
- FIT metrics: trades=980, wins=291, losses=689, win_rate=29.69%, PF=0.4729, net=Rs -355,444, avg_win=Rs 1,096, avg_loss=Rs -979, SL/TGT/EOD=475/128/377, top_trade/day/symbol=0.0055/None/None
- VAL metrics: trades=742, wins=184, losses=558, win_rate=24.8%, PF=0.3497, net=Rs -359,234, avg_win=Rs 1,050, avg_loss=Rs -990, SL/TGT/EOD=374/82/286, top_trade/day/symbol=0.0091/None/None
- full TRAIN metrics: trades=1722, wins=475, losses=1247, win_rate=27.58%, PF=0.4174, net=Rs -714,678, avg_win=Rs 1,078, avg_loss=Rs -984, SL/TGT/EOD=849/210/663, top_trade/day/symbol=0.0035/None/None
- TEST metrics: not run
- keep/reject: REJECT
- failure classification: train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- next action: continue train-side search

## Iteration 154 - combo_atr_pct_lower_wick_pct_sl1.1_t2.5
- changed rule/parameter: two-term structural combination plus exit rescue
- parameter group: combination
- old value: single/no mask
- new value: {'terms': [['atr_pct', '>=', 0.001937], ['lower_wick_pct', '>=', 0.0]], 'exit': [1.1, 2.5]}
- command: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\scripts\full_loop_a_pullback_c2_high.py --max_iterations 200`
- FIT metrics: trades=901, wins=265, losses=636, win_rate=29.41%, PF=0.4667, net=Rs -348,309, avg_win=Rs 1,150, avg_loss=Rs -1,027, SL/TGT/EOD=415/82/404, top_trade/day/symbol=0.0074/None/None
- VAL metrics: trades=676, wins=176, losses=500, win_rate=26.04%, PF=0.3758, net=Rs -320,490, avg_win=Rs 1,096, avg_loss=Rs -1,027, SL/TGT/EOD=309/50/317, top_trade/day/symbol=0.0117/None/None
- full TRAIN metrics: trades=1577, wins=441, losses=1136, win_rate=27.96%, PF=0.4267, net=Rs -668,799, avg_win=Rs 1,129, avg_loss=Rs -1,027, SL/TGT/EOD=724/132/721, top_trade/day/symbol=0.0046/None/None
- TEST metrics: not run
- keep/reject: REJECT
- failure classification: train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- next action: continue train-side search

## Iteration 155 - combo_atr_pct_market_abs_ret_pct_sl0.7_t1.8
- changed rule/parameter: two-term structural combination plus exit rescue
- parameter group: combination
- old value: single/no mask
- new value: {'terms': [['atr_pct', '>=', 0.001937], ['market_abs_ret_pct', '>=', 0.073994]], 'exit': [0.7, 1.8]}
- command: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\scripts\full_loop_a_pullback_c2_high.py --max_iterations 200`
- FIT metrics: trades=1178, wins=307, losses=871, win_rate=26.06%, PF=0.4341, net=Rs -413,163, avg_win=Rs 1,032, avg_loss=Rs -838, SL/TGT/EOD=737/155/286, top_trade/day/symbol=0.0049/None/None
- VAL metrics: trades=744, wins=157, losses=587, win_rate=21.1%, PF=0.3178, net=Rs -332,624, avg_win=Rs 987, avg_loss=Rs -831, SL/TGT/EOD=483/77/184, top_trade/day/symbol=0.0101/None/None
- full TRAIN metrics: trades=1922, wins=464, losses=1458, win_rate=24.14%, PF=0.3875, net=Rs -745,787, avg_win=Rs 1,017, avg_loss=Rs -835, SL/TGT/EOD=1220/232/470, top_trade/day/symbol=0.0033/None/None
- TEST metrics: not run
- keep/reject: REJECT
- failure classification: train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- next action: continue train-side search

## Iteration 156 - combo_atr_pct_market_abs_ret_pct_sl0.85_t1.8
- changed rule/parameter: two-term structural combination plus exit rescue
- parameter group: combination
- old value: single/no mask
- new value: {'terms': [['atr_pct', '>=', 0.001937], ['market_abs_ret_pct', '>=', 0.073994]], 'exit': [0.85, 1.8]}
- command: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\scripts\full_loop_a_pullback_c2_high.py --max_iterations 200`
- FIT metrics: trades=1049, wins=311, losses=738, win_rate=29.65%, PF=0.485, net=Rs -351,164, avg_win=Rs 1,063, avg_loss=Rs -924, SL/TGT/EOD=577/166/306, top_trade/day/symbol=0.0047/None/None
- VAL metrics: trades=680, wins=149, losses=531, win_rate=21.91%, PF=0.3061, net=Rs -337,568, avg_win=Rs 999, avg_loss=Rs -916, SL/TGT/EOD=394/73/213, top_trade/day/symbol=0.0105/None/None
- full TRAIN metrics: trades=1729, wins=460, losses=1269, win_rate=26.6%, PF=0.4105, net=Rs -688,732, avg_win=Rs 1,043, avg_loss=Rs -921, SL/TGT/EOD=971/239/519, top_trade/day/symbol=0.0033/None/None
- TEST metrics: not run
- keep/reject: REJECT
- failure classification: train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- next action: continue train-side search

## Iteration 157 - combo_atr_pct_market_abs_ret_pct_sl0.85_t2.0
- changed rule/parameter: two-term structural combination plus exit rescue
- parameter group: combination
- old value: single/no mask
- new value: {'terms': [['atr_pct', '>=', 0.001937], ['market_abs_ret_pct', '>=', 0.073994]], 'exit': [0.85, 2.0]}
- command: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\scripts\full_loop_a_pullback_c2_high.py --max_iterations 200`
- FIT metrics: trades=1023, wins=294, losses=729, win_rate=28.74%, PF=0.4838, net=Rs -345,606, avg_win=Rs 1,102, avg_loss=Rs -918, SL/TGT/EOD=566/134/323, top_trade/day/symbol=0.0055/None/None
- VAL metrics: trades=673, wins=144, losses=529, win_rate=21.4%, PF=0.3063, net=Rs -336,570, avg_win=Rs 1,032, avg_loss=Rs -917, SL/TGT/EOD=393/62/218, top_trade/day/symbol=0.0119/None/None
- full TRAIN metrics: trades=1696, wins=438, losses=1258, win_rate=25.83%, PF=0.4093, net=Rs -682,176, avg_win=Rs 1,079, avg_loss=Rs -918, SL/TGT/EOD=959/196/541, top_trade/day/symbol=0.0037/None/None
- TEST metrics: not run
- keep/reject: REJECT
- failure classification: train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- next action: continue train-side search

## Iteration 158 - combo_atr_pct_market_abs_ret_pct_sl1.0_t2.0
- changed rule/parameter: two-term structural combination plus exit rescue
- parameter group: combination
- old value: single/no mask
- new value: {'terms': [['atr_pct', '>=', 0.001937], ['market_abs_ret_pct', '>=', 0.073994]], 'exit': [1.0, 2.0]}
- command: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\scripts\full_loop_a_pullback_c2_high.py --max_iterations 200`
- FIT metrics: trades=930, wins=281, losses=649, win_rate=30.22%, PF=0.4962, net=Rs -320,208, avg_win=Rs 1,122, avg_loss=Rs -979, SL/TGT/EOD=447/130/353, top_trade/day/symbol=0.0056/None/None
- VAL metrics: trades=621, wins=147, losses=474, win_rate=23.67%, PF=0.3285, net=Rs -311,811, avg_win=Rs 1,038, avg_loss=Rs -980, SL/TGT/EOD=309/64/248, top_trade/day/symbol=0.0116/None/None
- full TRAIN metrics: trades=1551, wins=428, losses=1123, win_rate=27.6%, PF=0.4254, net=Rs -632,020, avg_win=Rs 1,093, avg_loss=Rs -979, SL/TGT/EOD=756/194/601, top_trade/day/symbol=0.0038/None/None
- TEST metrics: not run
- keep/reject: REJECT
- failure classification: train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- next action: continue train-side search

## Iteration 159 - combo_atr_pct_market_abs_ret_pct_sl1.1_t2.5
- changed rule/parameter: two-term structural combination plus exit rescue
- parameter group: combination
- old value: single/no mask
- new value: {'terms': [['atr_pct', '>=', 0.001937], ['market_abs_ret_pct', '>=', 0.073994]], 'exit': [1.1, 2.5]}
- command: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\scripts\full_loop_a_pullback_c2_high.py --max_iterations 200`
- FIT metrics: trades=854, wins=255, losses=599, win_rate=29.86%, PF=0.4996, net=Rs -308,781, avg_win=Rs 1,209, avg_loss=Rs -1,030, SL/TGT/EOD=392/88/374, top_trade/day/symbol=0.0074/None/None
- VAL metrics: trades=569, wins=146, losses=423, win_rate=25.66%, PF=0.3751, net=Rs -269,050, avg_win=Rs 1,106, avg_loss=Rs -1,018, SL/TGT/EOD=260/43/266, top_trade/day/symbol=0.014/None/None
- full TRAIN metrics: trades=1423, wins=401, losses=1022, win_rate=28.18%, PF=0.4484, net=Rs -577,830, avg_win=Rs 1,172, avg_loss=Rs -1,025, SL/TGT/EOD=652/131/640, top_trade/day/symbol=0.0048/None/None
- TEST metrics: not run
- keep/reject: REJECT
- failure classification: train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- next action: continue train-side search

## Iteration 160 - combo_atr_pct_market_ret_pct_sl0.7_t1.8
- changed rule/parameter: two-term structural combination plus exit rescue
- parameter group: combination
- old value: single/no mask
- new value: {'terms': [['atr_pct', '>=', 0.001937], ['market_ret_pct', '>=', -0.21279]], 'exit': [0.7, 1.8]}
- command: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\scripts\full_loop_a_pullback_c2_high.py --max_iterations 200`
- FIT metrics: trades=969, wins=233, losses=736, win_rate=24.05%, PF=0.3782, net=Rs -381,864, avg_win=Rs 997, avg_loss=Rs -834, SL/TGT/EOD=614/112/243, top_trade/day/symbol=0.0067/None/None
- VAL metrics: trades=879, wins=198, losses=681, win_rate=22.53%, PF=0.3569, net=Rs -362,609, avg_win=Rs 1,017, avg_loss=Rs -828, SL/TGT/EOD=553/101/225, top_trade/day/symbol=0.0078/None/None
- full TRAIN metrics: trades=1848, wins=431, losses=1417, win_rate=23.32%, PF=0.368, net=Rs -744,473, avg_win=Rs 1,006, avg_loss=Rs -831, SL/TGT/EOD=1167/213/468, top_trade/day/symbol=0.0036/None/None
- TEST metrics: not run
- keep/reject: REJECT
- failure classification: train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- next action: continue train-side search

## Iteration 161 - combo_atr_pct_market_ret_pct_sl0.85_t1.8
- changed rule/parameter: two-term structural combination plus exit rescue
- parameter group: combination
- old value: single/no mask
- new value: {'terms': [['atr_pct', '>=', 0.001937], ['market_ret_pct', '>=', -0.21279]], 'exit': [0.85, 1.8]}
- command: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\scripts\full_loop_a_pullback_c2_high.py --max_iterations 200`
- FIT metrics: trades=849, wins=235, losses=614, win_rate=27.68%, PF=0.4366, net=Rs -317,927, avg_win=Rs 1,048, avg_loss=Rs -919, SL/TGT/EOD=473/122/254, top_trade/day/symbol=0.0064/None/None
- VAL metrics: trades=803, wins=192, losses=611, win_rate=23.91%, PF=0.3478, net=Rs -366,375, avg_win=Rs 1,017, avg_loss=Rs -919, SL/TGT/EOD=458/96/249, top_trade/day/symbol=0.008/None/None
- full TRAIN metrics: trades=1652, wins=427, losses=1225, win_rate=25.85%, PF=0.3923, net=Rs -684,302, avg_win=Rs 1,035, avg_loss=Rs -919, SL/TGT/EOD=931/218/503, top_trade/day/symbol=0.0035/None/None
- TEST metrics: not run
- keep/reject: REJECT
- failure classification: train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- next action: continue train-side search

## Iteration 162 - combo_atr_pct_market_ret_pct_sl0.85_t2.0
- changed rule/parameter: two-term structural combination plus exit rescue
- parameter group: combination
- old value: single/no mask
- new value: {'terms': [['atr_pct', '>=', 0.001937], ['market_ret_pct', '>=', -0.21279]], 'exit': [0.85, 2.0]}
- command: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\scripts\full_loop_a_pullback_c2_high.py --max_iterations 200`
- FIT metrics: trades=830, wins=223, losses=607, win_rate=26.87%, PF=0.4212, net=Rs -323,246, avg_win=Rs 1,055, avg_loss=Rs -920, SL/TGT/EOD=469/94/267, top_trade/day/symbol=0.0075/None/None
- VAL metrics: trades=782, wins=182, losses=600, win_rate=23.27%, PF=0.3481, net=Rs -358,116, avg_win=Rs 1,051, avg_loss=Rs -916, SL/TGT/EOD=446/80/256, top_trade/day/symbol=0.0092/None/None
- full TRAIN metrics: trades=1612, wins=405, losses=1207, win_rate=25.12%, PF=0.385, net=Rs -681,362, avg_win=Rs 1,053, avg_loss=Rs -918, SL/TGT/EOD=915/174/523, top_trade/day/symbol=0.0041/None/None
- TEST metrics: not run
- keep/reject: REJECT
- failure classification: train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- next action: continue train-side search

## Iteration 163 - combo_atr_pct_market_ret_pct_sl1.0_t2.0
- changed rule/parameter: two-term structural combination plus exit rescue
- parameter group: combination
- old value: single/no mask
- new value: {'terms': [['atr_pct', '>=', 0.001937], ['market_ret_pct', '>=', -0.21279]], 'exit': [1.0, 2.0]}
- command: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\scripts\full_loop_a_pullback_c2_high.py --max_iterations 200`
- FIT metrics: trades=742, wins=203, losses=539, win_rate=27.36%, PF=0.4089, net=Rs -306,946, avg_win=Rs 1,046, avg_loss=Rs -963, SL/TGT/EOD=353/83/306, top_trade/day/symbol=0.0083/None/None
- VAL metrics: trades=694, wins=169, losses=525, win_rate=24.35%, PF=0.3383, net=Rs -341,078, avg_win=Rs 1,032, avg_loss=Rs -982, SL/TGT/EOD=344/72/278, top_trade/day/symbol=0.0101/None/None
- full TRAIN metrics: trades=1436, wins=372, losses=1064, win_rate=25.91%, PF=0.3737, net=Rs -648,025, avg_win=Rs 1,040, avg_loss=Rs -973, SL/TGT/EOD=697/155/584, top_trade/day/symbol=0.0046/None/None
- TEST metrics: not run
- keep/reject: REJECT
- failure classification: train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- next action: continue train-side search

## Iteration 164 - combo_atr_pct_market_ret_pct_sl1.1_t2.5
- changed rule/parameter: two-term structural combination plus exit rescue
- parameter group: combination
- old value: single/no mask
- new value: {'terms': [['atr_pct', '>=', 0.001937], ['market_ret_pct', '>=', -0.21279]], 'exit': [1.1, 2.5]}
- command: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\scripts\full_loop_a_pullback_c2_high.py --max_iterations 200`
- FIT metrics: trades=687, wins=187, losses=500, win_rate=27.22%, PF=0.4096, net=Rs -298,383, avg_win=Rs 1,107, avg_loss=Rs -1,011, SL/TGT/EOD=311/54/322, top_trade/day/symbol=0.0109/None/None
- VAL metrics: trades=636, wins=164, losses=472, win_rate=25.79%, PF=0.3819, net=Rs -296,171, avg_win=Rs 1,116, avg_loss=Rs -1,015, SL/TGT/EOD=284/48/304, top_trade/day/symbol=0.0124/None/None
- full TRAIN metrics: trades=1323, wins=351, losses=972, win_rate=26.53%, PF=0.3961, net=Rs -594,554, avg_win=Rs 1,111, avg_loss=Rs -1,013, SL/TGT/EOD=595/102/626, top_trade/day/symbol=0.0058/None/None
- TEST metrics: not run
- keep/reject: REJECT
- failure classification: train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- next action: continue train-side search

## Iteration 165 - combo_atr_pct_notional_sl0.7_t1.8
- changed rule/parameter: two-term structural combination plus exit rescue
- parameter group: combination
- old value: single/no mask
- new value: {'terms': [['atr_pct', '>=', 0.001937], ['notional', '>=', 99157.14]], 'exit': [0.7, 1.8]}
- command: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\scripts\full_loop_a_pullback_c2_high.py --max_iterations 200`
- FIT metrics: trades=1216, wins=303, losses=913, win_rate=24.92%, PF=0.4206, net=Rs -444,358, avg_win=Rs 1,065, avg_loss=Rs -840, SL/TGT/EOD=773/159/284, top_trade/day/symbol=0.0049/None/None
- VAL metrics: trades=923, wins=205, losses=718, win_rate=22.21%, PF=0.3514, net=Rs -388,601, avg_win=Rs 1,027, avg_loss=Rs -834, SL/TGT/EOD=592/109/222, top_trade/day/symbol=0.0074/None/None
- full TRAIN metrics: trades=2139, wins=508, losses=1631, win_rate=23.75%, PF=0.3903, net=Rs -832,960, avg_win=Rs 1,050, avg_loss=Rs -838, SL/TGT/EOD=1365/268/506, top_trade/day/symbol=0.0029/None/None
- TEST metrics: not run
- keep/reject: REJECT
- failure classification: train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- next action: continue train-side search

## Iteration 166 - combo_atr_pct_notional_sl0.85_t1.8
- changed rule/parameter: two-term structural combination plus exit rescue
- parameter group: combination
- old value: single/no mask
- new value: {'terms': [['atr_pct', '>=', 0.001937], ['notional', '>=', 99157.14]], 'exit': [0.85, 1.8]}
- command: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\scripts\full_loop_a_pullback_c2_high.py --max_iterations 200`
- FIT metrics: trades=1075, wins=303, losses=772, win_rate=28.19%, PF=0.46, net=Rs -385,358, avg_win=Rs 1,083, avg_loss=Rs -924, SL/TGT/EOD=599/164/312, top_trade/day/symbol=0.0048/None/None
- VAL metrics: trades=828, wins=196, losses=632, win_rate=23.67%, PF=0.3504, net=Rs -380,511, avg_win=Rs 1,047, avg_loss=Rs -927, SL/TGT/EOD=474/105/249, top_trade/day/symbol=0.0076/None/None
- full TRAIN metrics: trades=1903, wins=499, losses=1404, win_rate=26.22%, PF=0.4106, net=Rs -765,869, avg_win=Rs 1,069, avg_loss=Rs -925, SL/TGT/EOD=1073/269/561, top_trade/day/symbol=0.0029/None/None
- TEST metrics: not run
- keep/reject: REJECT
- failure classification: train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- next action: continue train-side search

## Iteration 167 - combo_atr_pct_notional_sl0.85_t2.0
- changed rule/parameter: two-term structural combination plus exit rescue
- parameter group: combination
- old value: single/no mask
- new value: {'terms': [['atr_pct', '>=', 0.001937], ['notional', '>=', 99157.14]], 'exit': [0.85, 2.0]}
- command: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\scripts\full_loop_a_pullback_c2_high.py --max_iterations 200`
- FIT metrics: trades=1052, wins=286, losses=766, win_rate=27.19%, PF=0.4466, net=Rs -390,933, avg_win=Rs 1,103, avg_loss=Rs -922, SL/TGT/EOD=594/130/328, top_trade/day/symbol=0.0056/None/None
- VAL metrics: trades=810, wins=188, losses=622, win_rate=23.21%, PF=0.36, net=Rs -368,444, avg_win=Rs 1,103, avg_loss=Rs -926, SL/TGT/EOD=465/91/254, top_trade/day/symbol=0.0085/None/None
- full TRAIN metrics: trades=1862, wins=474, losses=1388, win_rate=25.46%, PF=0.4078, net=Rs -759,378, avg_win=Rs 1,103, avg_loss=Rs -924, SL/TGT/EOD=1059/221/582, top_trade/day/symbol=0.0034/None/None
- TEST metrics: not run
- keep/reject: REJECT
- failure classification: train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- next action: continue train-side search

## Iteration 168 - combo_atr_pct_notional_sl1.0_t2.0
- changed rule/parameter: two-term structural combination plus exit rescue
- parameter group: combination
- old value: single/no mask
- new value: {'terms': [['atr_pct', '>=', 0.001937], ['notional', '>=', 99157.14]], 'exit': [1.0, 2.0]}
- command: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\scripts\full_loop_a_pullback_c2_high.py --max_iterations 200`
- FIT metrics: trades=968, wins=272, losses=696, win_rate=28.1%, PF=0.4399, net=Rs -386,390, avg_win=Rs 1,116, avg_loss=Rs -991, SL/TGT/EOD=481/126/361, top_trade/day/symbol=0.0058/None/None
- VAL metrics: trades=720, wins=188, losses=532, win_rate=26.11%, PF=0.3753, net=Rs -332,705, avg_win=Rs 1,063, avg_loss=Rs -1,001, SL/TGT/EOD=365/86/269, top_trade/day/symbol=0.0088/None/None
- full TRAIN metrics: trades=1688, wins=460, losses=1228, win_rate=27.25%, PF=0.4117, net=Rs -719,096, avg_win=Rs 1,094, avg_loss=Rs -995, SL/TGT/EOD=846/212/630, top_trade/day/symbol=0.0035/None/None
- TEST metrics: not run
- keep/reject: REJECT
- failure classification: train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- next action: continue train-side search

## Iteration 169 - combo_atr_pct_notional_sl1.1_t2.5
- changed rule/parameter: two-term structural combination plus exit rescue
- parameter group: combination
- old value: single/no mask
- new value: {'terms': [['atr_pct', '>=', 0.001937], ['notional', '>=', 99157.14]], 'exit': [1.1, 2.5]}
- command: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\scripts\full_loop_a_pullback_c2_high.py --max_iterations 200`
- FIT metrics: trades=898, wins=244, losses=654, win_rate=27.17%, PF=0.4343, net=Rs -381,167, avg_win=Rs 1,199, avg_loss=Rs -1,030, SL/TGT/EOD=420/82/396, top_trade/day/symbol=0.0077/None/None
- VAL metrics: trades=659, wins=178, losses=481, win_rate=27.01%, PF=0.3892, net=Rs -308,299, avg_win=Rs 1,104, avg_loss=Rs -1,049, SL/TGT/EOD=307/50/302, top_trade/day/symbol=0.0115/None/None
- full TRAIN metrics: trades=1557, wins=422, losses=1135, win_rate=27.1%, PF=0.415, net=Rs -689,466, avg_win=Rs 1,159, avg_loss=Rs -1,038, SL/TGT/EOD=727/132/698, top_trade/day/symbol=0.0046/None/None
- TEST metrics: not run
- keep/reject: REJECT
- failure classification: train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- next action: continue train-side search

## Iteration 170 - combo_atr_pct_quality_score_sl0.7_t1.8
- changed rule/parameter: two-term structural combination plus exit rescue
- parameter group: combination
- old value: single/no mask
- new value: {'terms': [['atr_pct', '>=', 0.001937], ['quality_score', '>=', 29.113851]], 'exit': [0.7, 1.8]}
- command: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\scripts\full_loop_a_pullback_c2_high.py --max_iterations 200`
- FIT metrics: trades=1218, wins=302, losses=916, win_rate=24.79%, PF=0.4062, net=Rs -458,230, avg_win=Rs 1,038, avg_loss=Rs -842, SL/TGT/EOD=781/158/279, top_trade/day/symbol=0.005/None/None
- VAL metrics: trades=895, wins=199, losses=696, win_rate=22.23%, PF=0.3576, net=Rs -372,177, avg_win=Rs 1,041, avg_loss=Rs -832, SL/TGT/EOD=573/107/215, top_trade/day/symbol=0.0076/None/None
- full TRAIN metrics: trades=2113, wins=501, losses=1612, win_rate=23.71%, PF=0.3854, net=Rs -830,407, avg_win=Rs 1,039, avg_loss=Rs -838, SL/TGT/EOD=1354/265/494, top_trade/day/symbol=0.003/None/None
- TEST metrics: not run
- keep/reject: REJECT
- failure classification: train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- next action: continue train-side search

## Iteration 171 - combo_atr_pct_quality_score_sl0.85_t1.8
- changed rule/parameter: two-term structural combination plus exit rescue
- parameter group: combination
- old value: single/no mask
- new value: {'terms': [['atr_pct', '>=', 0.001937], ['quality_score', '>=', 29.113851]], 'exit': [0.85, 1.8]}
- command: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\scripts\full_loop_a_pullback_c2_high.py --max_iterations 200`
- FIT metrics: trades=1079, wins=301, losses=778, win_rate=27.9%, PF=0.438, net=Rs -409,112, avg_win=Rs 1,059, avg_loss=Rs -936, SL/TGT/EOD=618/163/298, top_trade/day/symbol=0.0049/None/None
- VAL metrics: trades=816, wins=202, losses=614, win_rate=24.75%, PF=0.3692, net=Rs -356,982, avg_win=Rs 1,034, avg_loss=Rs -922, SL/TGT/EOD=464/106/246, top_trade/day/symbol=0.0075/None/None
- full TRAIN metrics: trades=1895, wins=503, losses=1392, win_rate=26.54%, PF=0.4079, net=Rs -766,093, avg_win=Rs 1,049, avg_loss=Rs -930, SL/TGT/EOD=1082/269/544, top_trade/day/symbol=0.003/None/None
- TEST metrics: not run
- keep/reject: REJECT
- failure classification: train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- next action: continue train-side search

## Iteration 172 - combo_atr_pct_quality_score_sl0.85_t2.0
- changed rule/parameter: two-term structural combination plus exit rescue
- parameter group: combination
- old value: single/no mask
- new value: {'terms': [['atr_pct', '>=', 0.001937], ['quality_score', '>=', 29.113851]], 'exit': [0.85, 2.0]}
- command: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\scripts\full_loop_a_pullback_c2_high.py --max_iterations 200`
- FIT metrics: trades=1053, wins=287, losses=766, win_rate=27.26%, PF=0.4365, net=Rs -402,561, avg_win=Rs 1,086, avg_loss=Rs -933, SL/TGT/EOD=608/130/315, top_trade/day/symbol=0.0057/None/None
- VAL metrics: trades=801, wins=195, losses=606, win_rate=24.34%, PF=0.3822, net=Rs -343,595, avg_win=Rs 1,090, avg_loss=Rs -918, SL/TGT/EOD=455/92/254, top_trade/day/symbol=0.0083/None/None
- full TRAIN metrics: trades=1854, wins=482, losses=1372, win_rate=26.0%, PF=0.4127, net=Rs -746,156, avg_win=Rs 1,088, avg_loss=Rs -926, SL/TGT/EOD=1063/222/569, top_trade/day/symbol=0.0034/None/None
- TEST metrics: not run
- keep/reject: REJECT
- failure classification: train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- next action: continue train-side search

## Iteration 173 - combo_atr_pct_quality_score_sl1.0_t2.0
- changed rule/parameter: two-term structural combination plus exit rescue
- parameter group: combination
- old value: single/no mask
- new value: {'terms': [['atr_pct', '>=', 0.001937], ['quality_score', '>=', 29.113851]], 'exit': [1.0, 2.0]}
- command: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\scripts\full_loop_a_pullback_c2_high.py --max_iterations 200`
- FIT metrics: trades=968, wins=270, losses=698, win_rate=27.89%, PF=0.4298, net=Rs -396,100, avg_win=Rs 1,106, avg_loss=Rs -995, SL/TGT/EOD=490/124/354, top_trade/day/symbol=0.0059/None/None
- VAL metrics: trades=714, wins=182, losses=532, win_rate=25.49%, PF=0.3703, net=Rs -331,464, avg_win=Rs 1,071, avg_loss=Rs -989, SL/TGT/EOD=357/83/274, top_trade/day/symbol=0.0091/None/None
- full TRAIN metrics: trades=1682, wins=452, losses=1230, win_rate=26.87%, PF=0.4041, net=Rs -727,564, avg_win=Rs 1,092, avg_loss=Rs -993, SL/TGT/EOD=847/207/628, top_trade/day/symbol=0.0036/None/None
- TEST metrics: not run
- keep/reject: REJECT
- failure classification: train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- next action: continue train-side search

## Iteration 174 - combo_atr_pct_quality_score_sl1.1_t2.5
- changed rule/parameter: two-term structural combination plus exit rescue
- parameter group: combination
- old value: single/no mask
- new value: {'terms': [['atr_pct', '>=', 0.001937], ['quality_score', '>=', 29.113851]], 'exit': [1.1, 2.5]}
- command: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\scripts\full_loop_a_pullback_c2_high.py --max_iterations 200`
- FIT metrics: trades=894, wins=242, losses=652, win_rate=27.07%, PF=0.4216, net=Rs -396,364, avg_win=Rs 1,194, avg_loss=Rs -1,051, SL/TGT/EOD=435/82/377, top_trade/day/symbol=0.0078/None/None
- VAL metrics: trades=658, wins=172, losses=486, win_rate=26.14%, PF=0.3881, net=Rs -306,896, avg_win=Rs 1,131, avg_loss=Rs -1,032, SL/TGT/EOD=308/52/298, top_trade/day/symbol=0.0116/None/None
- full TRAIN metrics: trades=1552, wins=414, losses=1138, win_rate=26.68%, PF=0.4074, net=Rs -703,260, avg_win=Rs 1,168, avg_loss=Rs -1,043, SL/TGT/EOD=743/134/675, top_trade/day/symbol=0.0047/None/None
- TEST metrics: not run
- keep/reject: REJECT
- failure classification: train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- next action: continue train-side search

## Iteration 175 - combo_atr_pct_rs_pct_sl0.7_t1.8
- changed rule/parameter: two-term structural combination plus exit rescue
- parameter group: combination
- old value: single/no mask
- new value: {'terms': [['atr_pct', '>=', 0.001937], ['rs_pct', '>=', -1.287037]], 'exit': [0.7, 1.8]}
- command: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\scripts\full_loop_a_pullback_c2_high.py --max_iterations 200`
- FIT metrics: trades=1199, wins=307, losses=892, win_rate=25.6%, PF=0.4287, net=Rs -423,935, avg_win=Rs 1,036, avg_loss=Rs -832, SL/TGT/EOD=744/157/298, top_trade/day/symbol=0.0049/None/None
- VAL metrics: trades=910, wins=209, losses=701, win_rate=22.97%, PF=0.3601, net=Rs -371,105, avg_win=Rs 999, avg_loss=Rs -827, SL/TGT/EOD=572/106/232, top_trade/day/symbol=0.0075/None/None
- full TRAIN metrics: trades=2109, wins=516, losses=1593, win_rate=24.47%, PF=0.3986, net=Rs -795,040, avg_win=Rs 1,021, avg_loss=Rs -830, SL/TGT/EOD=1316/263/530, top_trade/day/symbol=0.003/None/None
- TEST metrics: not run
- keep/reject: REJECT
- failure classification: train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- next action: continue train-side search

## Iteration 176 - combo_atr_pct_rs_pct_sl0.85_t1.8
- changed rule/parameter: two-term structural combination plus exit rescue
- parameter group: combination
- old value: single/no mask
- new value: {'terms': [['atr_pct', '>=', 0.001937], ['rs_pct', '>=', -1.287037]], 'exit': [0.85, 1.8]}
- command: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\scripts\full_loop_a_pullback_c2_high.py --max_iterations 200`
- FIT metrics: trades=1071, wins=311, losses=760, win_rate=29.04%, PF=0.485, net=Rs -359,152, avg_win=Rs 1,087, avg_loss=Rs -918, SL/TGT/EOD=587/170/314, top_trade/day/symbol=0.0046/None/None
- VAL metrics: trades=823, wins=205, losses=618, win_rate=24.91%, PF=0.3716, net=Rs -354,959, avg_win=Rs 1,024, avg_loss=Rs -914, SL/TGT/EOD=459/105/259, top_trade/day/symbol=0.0075/None/None
- full TRAIN metrics: trades=1894, wins=516, losses=1378, win_rate=27.24%, PF=0.4342, net=Rs -714,112, avg_win=Rs 1,062, avg_loss=Rs -916, SL/TGT/EOD=1046/275/573, top_trade/day/symbol=0.0029/None/None
- TEST metrics: not run
- keep/reject: REJECT
- failure classification: train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- next action: continue train-side search

## Iteration 177 - combo_atr_pct_rs_pct_sl0.85_t2.0
- changed rule/parameter: two-term structural combination plus exit rescue
- parameter group: combination
- old value: single/no mask
- new value: {'terms': [['atr_pct', '>=', 0.001937], ['rs_pct', '>=', -1.287037]], 'exit': [0.85, 2.0]}
- command: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\scripts\full_loop_a_pullback_c2_high.py --max_iterations 200`
- FIT metrics: trades=1041, wins=296, losses=745, win_rate=28.43%, PF=0.4783, net=Rs -356,072, avg_win=Rs 1,103, avg_loss=Rs -916, SL/TGT/EOD=578/134/329, top_trade/day/symbol=0.0054/None/None
- VAL metrics: trades=805, wins=196, losses=609, win_rate=24.35%, PF=0.3787, net=Rs -344,248, avg_win=Rs 1,070, avg_loss=Rs -910, SL/TGT/EOD=449/89/267, top_trade/day/symbol=0.0084/None/None
- full TRAIN metrics: trades=1846, wins=492, losses=1354, win_rate=26.65%, PF=0.4337, net=Rs -700,320, avg_win=Rs 1,090, avg_loss=Rs -913, SL/TGT/EOD=1027/223/596, top_trade/day/symbol=0.0033/None/None
- TEST metrics: not run
- keep/reject: REJECT
- failure classification: train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- next action: continue train-side search

## Iteration 178 - combo_atr_pct_rs_pct_sl1.0_t2.0
- changed rule/parameter: two-term structural combination plus exit rescue
- parameter group: combination
- old value: single/no mask
- new value: {'terms': [['atr_pct', '>=', 0.001937], ['rs_pct', '>=', -1.287037]], 'exit': [1.0, 2.0]}
- command: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\scripts\full_loop_a_pullback_c2_high.py --max_iterations 200`
- FIT metrics: trades=960, wins=284, losses=676, win_rate=29.58%, PF=0.4739, net=Rs -350,267, avg_win=Rs 1,111, avg_loss=Rs -985, SL/TGT/EOD=468/131/361, top_trade/day/symbol=0.0056/None/None
- VAL metrics: trades=726, wins=190, losses=536, win_rate=26.17%, PF=0.371, net=Rs -332,133, avg_win=Rs 1,031, avg_loss=Rs -985, SL/TGT/EOD=359/83/284, top_trade/day/symbol=0.009/None/None
- full TRAIN metrics: trades=1686, wins=474, losses=1212, win_rate=28.11%, PF=0.4284, net=Rs -682,401, avg_win=Rs 1,079, avg_loss=Rs -985, SL/TGT/EOD=827/214/645, top_trade/day/symbol=0.0035/None/None
- TEST metrics: not run
- keep/reject: REJECT
- failure classification: train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- next action: continue train-side search

## Iteration 179 - combo_atr_pct_rs_pct_sl1.1_t2.5
- changed rule/parameter: two-term structural combination plus exit rescue
- parameter group: combination
- old value: single/no mask
- new value: {'terms': [['atr_pct', '>=', 0.001937], ['rs_pct', '>=', -1.287037]], 'exit': [1.1, 2.5]}
- command: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\scripts\full_loop_a_pullback_c2_high.py --max_iterations 200`
- FIT metrics: trades=884, wins=259, losses=625, win_rate=29.3%, PF=0.4662, net=Rs -343,560, avg_win=Rs 1,158, avg_loss=Rs -1,030, SL/TGT/EOD=409/80/395, top_trade/day/symbol=0.0076/None/None
- VAL metrics: trades=669, wins=176, losses=493, win_rate=26.31%, PF=0.3833, net=Rs -310,775, avg_win=Rs 1,098, avg_loss=Rs -1,022, SL/TGT/EOD=304/52/313, top_trade/day/symbol=0.0117/None/None
- full TRAIN metrics: trades=1553, wins=435, losses=1118, win_rate=28.01%, PF=0.4298, net=Rs -654,336, avg_win=Rs 1,134, avg_loss=Rs -1,026, SL/TGT/EOD=713/132/708, top_trade/day/symbol=0.0046/None/None
- TEST metrics: not run
- keep/reject: REJECT
- failure classification: train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- next action: continue train-side search

## Iteration 180 - combo_atr_pct_signal_minute_sl0.7_t1.8
- changed rule/parameter: two-term structural combination plus exit rescue
- parameter group: combination
- old value: single/no mask
- new value: {'terms': [['atr_pct', '>=', 0.001937], ['signal_minute', '>=', 700.0]], 'exit': [0.7, 1.8]}
- command: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\scripts\full_loop_a_pullback_c2_high.py --max_iterations 200`
- FIT metrics: trades=1077, wins=295, losses=782, win_rate=27.39%, PF=0.4613, net=Rs -345,448, avg_win=Rs 1,003, avg_loss=Rs -820, SL/TGT/EOD=633/142/302, top_trade/day/symbol=0.0053/None/None
- VAL metrics: trades=830, wins=181, losses=649, win_rate=21.81%, PF=0.3303, net=Rs -357,833, avg_win=Rs 975, avg_loss=Rs -823, SL/TGT/EOD=523/87/220, top_trade/day/symbol=0.0089/None/None
- full TRAIN metrics: trades=1907, wins=476, losses=1431, win_rate=24.96%, PF=0.4017, net=Rs -703,281, avg_win=Rs 992, avg_loss=Rs -821, SL/TGT/EOD=1156/229/522, top_trade/day/symbol=0.0033/None/None
- TEST metrics: not run
- keep/reject: REJECT
- failure classification: train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- next action: continue train-side search

## Iteration 181 - combo_atr_pct_signal_minute_sl0.85_t1.8
- changed rule/parameter: two-term structural combination plus exit rescue
- parameter group: combination
- old value: single/no mask
- new value: {'terms': [['atr_pct', '>=', 0.001937], ['signal_minute', '>=', 700.0]], 'exit': [0.85, 1.8]}
- command: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\scripts\full_loop_a_pullback_c2_high.py --max_iterations 200`
- FIT metrics: trades=959, wins=287, losses=672, win_rate=29.93%, PF=0.4908, net=Rs -305,839, avg_win=Rs 1,027, avg_loss=Rs -894, SL/TGT/EOD=491/143/325, top_trade/day/symbol=0.0053/None/None
- VAL metrics: trades=755, wins=176, losses=579, win_rate=23.31%, PF=0.3392, net=Rs -344,909, avg_win=Rs 1,006, avg_loss=Rs -902, SL/TGT/EOD=419/89/247, top_trade/day/symbol=0.0088/None/None
- full TRAIN metrics: trades=1714, wins=463, losses=1251, win_rate=27.01%, PF=0.4203, net=Rs -650,748, avg_win=Rs 1,019, avg_loss=Rs -897, SL/TGT/EOD=910/232/572, top_trade/day/symbol=0.0033/None/None
- TEST metrics: not run
- keep/reject: REJECT
- failure classification: train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- next action: continue train-side search

## Iteration 182 - combo_atr_pct_signal_minute_sl0.85_t2.0
- changed rule/parameter: two-term structural combination plus exit rescue
- parameter group: combination
- old value: single/no mask
- new value: {'terms': [['atr_pct', '>=', 0.001937], ['signal_minute', '>=', 700.0]], 'exit': [0.85, 2.0]}
- command: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\scripts\full_loop_a_pullback_c2_high.py --max_iterations 200`
- FIT metrics: trades=927, wins=269, losses=658, win_rate=29.02%, PF=0.4797, net=Rs -305,433, avg_win=Rs 1,047, avg_loss=Rs -892, SL/TGT/EOD=482/111/334, top_trade/day/symbol=0.0063/None/None
- VAL metrics: trades=745, wins=172, losses=573, win_rate=23.09%, PF=0.3556, net=Rs -332,842, avg_win=Rs 1,068, avg_loss=Rs -901, SL/TGT/EOD=414/82/249, top_trade/day/symbol=0.0096/None/None
- full TRAIN metrics: trades=1672, wins=441, losses=1231, win_rate=26.38%, PF=0.4216, net=Rs -638,275, avg_win=Rs 1,055, avg_loss=Rs -896, SL/TGT/EOD=896/193/583, top_trade/day/symbol=0.0038/None/None
- TEST metrics: not run
- keep/reject: REJECT
- failure classification: train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- next action: continue train-side search

## Iteration 183 - combo_atr_pct_signal_minute_sl1.0_t2.0
- changed rule/parameter: two-term structural combination plus exit rescue
- parameter group: combination
- old value: single/no mask
- new value: {'terms': [['atr_pct', '>=', 0.001937], ['signal_minute', '>=', 700.0]], 'exit': [1.0, 2.0]}
- command: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\scripts\full_loop_a_pullback_c2_high.py --max_iterations 200`
- FIT metrics: trades=854, wins=262, losses=592, win_rate=30.68%, PF=0.4851, net=Rs -289,358, avg_win=Rs 1,040, avg_loss=Rs -949, SL/TGT/EOD=382/109/363, top_trade/day/symbol=0.0065/None/None
- VAL metrics: trades=680, wins=172, losses=508, win_rate=25.29%, PF=0.3534, net=Rs -320,738, avg_win=Rs 1,019, avg_loss=Rs -976, SL/TGT/EOD=333/77/270, top_trade/day/symbol=0.0101/None/None
- full TRAIN metrics: trades=1534, wins=434, losses=1100, win_rate=28.29%, PF=0.4233, net=Rs -610,096, avg_win=Rs 1,032, avg_loss=Rs -962, SL/TGT/EOD=715/186/633, top_trade/day/symbol=0.0039/None/None
- TEST metrics: not run
- keep/reject: REJECT
- failure classification: train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- next action: continue train-side search

## Iteration 184 - combo_atr_pct_signal_minute_sl1.1_t2.5
- changed rule/parameter: two-term structural combination plus exit rescue
- parameter group: combination
- old value: single/no mask
- new value: {'terms': [['atr_pct', '>=', 0.001937], ['signal_minute', '>=', 700.0]], 'exit': [1.1, 2.5]}
- command: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\scripts\full_loop_a_pullback_c2_high.py --max_iterations 200`
- FIT metrics: trades=790, wins=250, losses=540, win_rate=31.65%, PF=0.5246, net=Rs -253,463, avg_win=Rs 1,119, avg_loss=Rs -987, SL/TGT/EOD=321/75/394, top_trade/day/symbol=0.0081/None/None
- VAL metrics: trades=621, wins=154, losses=467, win_rate=24.8%, PF=0.3347, net=Rs -317,554, avg_win=Rs 1,037, avg_loss=Rs -1,022, SL/TGT/EOD=285/43/293, top_trade/day/symbol=0.0142/None/None
- full TRAIN metrics: trades=1411, wins=404, losses=1007, win_rate=28.63%, PF=0.4349, net=Rs -571,017, avg_win=Rs 1,088, avg_loss=Rs -1,003, SL/TGT/EOD=606/118/687, top_trade/day/symbol=0.0052/None/None
- TEST metrics: not run
- keep/reject: REJECT
- failure classification: train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- next action: continue train-side search

## Iteration 185 - combo_atr_pct_signal_range_pct_sl0.7_t1.8
- changed rule/parameter: two-term structural combination plus exit rescue
- parameter group: combination
- old value: single/no mask
- new value: {'terms': [['atr_pct', '>=', 0.001937], ['signal_range_pct', '>=', 0.322068]], 'exit': [0.7, 1.8]}
- command: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\scripts\full_loop_a_pullback_c2_high.py --max_iterations 200`
- FIT metrics: trades=1228, wins=316, losses=912, win_rate=25.73%, PF=0.4276, net=Rs -437,889, avg_win=Rs 1,035, avg_loss=Rs -839, SL/TGT/EOD=775/161/292, top_trade/day/symbol=0.0048/None/None
- VAL metrics: trades=943, wins=213, losses=730, win_rate=22.59%, PF=0.3499, net=Rs -397,976, avg_win=Rs 1,006, avg_loss=Rs -839, SL/TGT/EOD=606/108/229, top_trade/day/symbol=0.0073/None/None
- full TRAIN metrics: trades=2171, wins=529, losses=1642, win_rate=24.37%, PF=0.3931, net=Rs -835,865, avg_win=Rs 1,023, avg_loss=Rs -839, SL/TGT/EOD=1381/269/521, top_trade/day/symbol=0.0029/None/None
- TEST metrics: not run
- keep/reject: REJECT
- failure classification: train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- next action: continue train-side search

## Iteration 186 - combo_atr_pct_signal_range_pct_sl0.85_t1.8
- changed rule/parameter: two-term structural combination plus exit rescue
- parameter group: combination
- old value: single/no mask
- new value: {'terms': [['atr_pct', '>=', 0.001937], ['signal_range_pct', '>=', 0.322068]], 'exit': [0.85, 1.8]}
- command: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\scripts\full_loop_a_pullback_c2_high.py --max_iterations 200`
- FIT metrics: trades=1096, wins=311, losses=785, win_rate=28.38%, PF=0.4558, net=Rs -395,569, avg_win=Rs 1,065, avg_loss=Rs -926, SL/TGT/EOD=614/167/315, top_trade/day/symbol=0.0047/None/None
- VAL metrics: trades=841, wins=208, losses=633, win_rate=24.73%, PF=0.3686, net=Rs -369,229, avg_win=Rs 1,036, avg_loss=Rs -924, SL/TGT/EOD=477/108/256, top_trade/day/symbol=0.0073/None/None
- full TRAIN metrics: trades=1937, wins=519, losses=1418, win_rate=26.79%, PF=0.4169, net=Rs -764,797, avg_win=Rs 1,054, avg_loss=Rs -925, SL/TGT/EOD=1091/275/571, top_trade/day/symbol=0.0029/None/None
- TEST metrics: not run
- keep/reject: REJECT
- failure classification: train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- next action: continue train-side search

## Iteration 187 - combo_atr_pct_signal_range_pct_sl0.85_t2.0
- changed rule/parameter: two-term structural combination plus exit rescue
- parameter group: combination
- old value: single/no mask
- new value: {'terms': [['atr_pct', '>=', 0.001937], ['signal_range_pct', '>=', 0.322068]], 'exit': [0.85, 2.0]}
- command: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\scripts\full_loop_a_pullback_c2_high.py --max_iterations 200`
- FIT metrics: trades=1067, wins=297, losses=770, win_rate=27.84%, PF=0.4542, net=Rs -389,371, avg_win=Rs 1,091, avg_loss=Rs -926, SL/TGT/EOD=606/133/328, top_trade/day/symbol=0.0055/None/None
- VAL metrics: trades=821, wins=195, losses=626, win_rate=23.75%, PF=0.367, net=Rs -364,711, avg_win=Rs 1,084, avg_loss=Rs -920, SL/TGT/EOD=469/91/261, top_trade/day/symbol=0.0084/None/None
- full TRAIN metrics: trades=1888, wins=492, losses=1396, win_rate=26.06%, PF=0.4152, net=Rs -754,082, avg_win=Rs 1,088, avg_loss=Rs -924, SL/TGT/EOD=1075/224/589, top_trade/day/symbol=0.0033/None/None
- TEST metrics: not run
- keep/reject: REJECT
- failure classification: train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- next action: continue train-side search

## Iteration 188 - combo_atr_pct_signal_range_pct_sl1.0_t2.0
- changed rule/parameter: two-term structural combination plus exit rescue
- parameter group: combination
- old value: single/no mask
- new value: {'terms': [['atr_pct', '>=', 0.001937], ['signal_range_pct', '>=', 0.322068]], 'exit': [1.0, 2.0]}
- command: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\scripts\full_loop_a_pullback_c2_high.py --max_iterations 200`
- FIT metrics: trades=979, wins=287, losses=692, win_rate=29.32%, PF=0.4474, net=Rs -381,967, avg_win=Rs 1,077, avg_loss=Rs -999, SL/TGT/EOD=494/127/358, top_trade/day/symbol=0.0057/None/None
- VAL metrics: trades=740, wins=190, losses=550, win_rate=25.68%, PF=0.3577, net=Rs -353,885, avg_win=Rs 1,037, avg_loss=Rs -1,002, SL/TGT/EOD=377/83/280, top_trade/day/symbol=0.009/None/None
- full TRAIN metrics: trades=1719, wins=477, losses=1242, win_rate=27.75%, PF=0.4076, net=Rs -735,851, avg_win=Rs 1,061, avg_loss=Rs -1,000, SL/TGT/EOD=871/210/638, top_trade/day/symbol=0.0035/None/None
- TEST metrics: not run
- keep/reject: REJECT
- failure classification: train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- next action: continue train-side search

## Iteration 189 - guard_maxpos10
- changed rule/parameter: portfolio max-position guard rescue
- parameter group: guard
- old value: 20
- new value: 10
- command: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\scripts\full_loop_a_pullback_c2_high.py --max_iterations 200`
- FIT metrics: trades=919, wins=313, losses=606, win_rate=34.06%, PF=0.3857, net=Rs -313,733, avg_win=Rs 629, avg_loss=Rs -843, SL/TGT/EOD=515/286/118, top_trade/day/symbol=0.0034/None/None
- VAL metrics: trades=634, wins=195, losses=439, win_rate=30.76%, PF=0.3216, net=Rs -249,307, avg_win=Rs 606, avg_loss=Rs -837, SL/TGT/EOD=362/169/103, top_trade/day/symbol=0.0056/None/None
- full TRAIN metrics: trades=1553, wins=508, losses=1045, win_rate=32.71%, PF=0.3589, net=Rs -563,040, avg_win=Rs 620, avg_loss=Rs -840, SL/TGT/EOD=877/455/221, top_trade/day/symbol=0.0021/None/None
- TEST metrics: not run
- keep/reject: REJECT
- failure classification: train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- next action: continue train-side search

## Iteration 190 - guard_maxpos15
- changed rule/parameter: portfolio max-position guard rescue
- parameter group: guard
- old value: 20
- new value: 15
- command: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\scripts\full_loop_a_pullback_c2_high.py --max_iterations 200`
- FIT metrics: trades=1282, wins=446, losses=836, win_rate=34.79%, PF=0.3986, net=Rs -422,290, avg_win=Rs 628, avg_loss=Rs -840, SL/TGT/EOD=705/407/170, top_trade/day/symbol=0.0024/None/None
- VAL metrics: trades=866, wins=260, losses=606, win_rate=30.02%, PF=0.3157, net=Rs -342,164, avg_win=Rs 607, avg_loss=Rs -825, SL/TGT/EOD=487/226/153, top_trade/day/symbol=0.0042/None/None
- full TRAIN metrics: trades=2148, wins=706, losses=1442, win_rate=32.87%, PF=0.3641, net=Rs -764,454, avg_win=Rs 620, avg_loss=Rs -834, SL/TGT/EOD=1192/633/323, top_trade/day/symbol=0.0015/None/None
- TEST metrics: not run
- keep/reject: REJECT
- failure classification: train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- next action: continue train-side search

## Iteration 191 - guard_daily_loss_3000
- changed rule/parameter: daily loss guard rescue
- parameter group: guard
- old value: 0
- new value: 3000.0
- command: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\scripts\full_loop_a_pullback_c2_high.py --max_iterations 200`
- FIT metrics: trades=724, wins=236, losses=488, win_rate=32.6%, PF=0.3551, net=Rs -273,498, avg_win=Rs 638, avg_loss=Rs -869, SL/TGT/EOD=441/222/61, top_trade/day/symbol=0.0044/None/None
- VAL metrics: trades=506, wins=171, losses=335, win_rate=33.79%, PF=0.3617, net=Rs -177,572, avg_win=Rs 588, avg_loss=Rs -830, SL/TGT/EOD=278/144/84, top_trade/day/symbol=0.0066/None/None
- full TRAIN metrics: trades=1230, wins=407, losses=823, win_rate=33.09%, PF=0.3577, net=Rs -451,070, avg_win=Rs 617, avg_loss=Rs -853, SL/TGT/EOD=719/366/145, top_trade/day/symbol=0.0027/None/None
- TEST metrics: not run
- keep/reject: REJECT
- failure classification: train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- next action: continue train-side search

## Iteration 192 - guard_daily_loss_5000
- changed rule/parameter: daily loss guard rescue
- parameter group: guard
- old value: 0
- new value: 5000.0
- command: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\scripts\full_loop_a_pullback_c2_high.py --max_iterations 200`
- FIT metrics: trades=839, wins=276, losses=563, win_rate=32.9%, PF=0.3593, net=Rs -312,236, avg_win=Rs 634, avg_loss=Rs -866, SL/TGT/EOD=507/259/73, top_trade/day/symbol=0.0038/None/None
- VAL metrics: trades=629, wins=216, losses=413, win_rate=34.34%, PF=0.3729, net=Rs -215,031, avg_win=Rs 592, avg_loss=Rs -830, SL/TGT/EOD=340/182/107, top_trade/day/symbol=0.0052/None/None
- full TRAIN metrics: trades=1468, wins=492, losses=976, win_rate=33.51%, PF=0.3649, net=Rs -527,267, avg_win=Rs 616, avg_loss=Rs -851, SL/TGT/EOD=847/441/180, top_trade/day/symbol=0.0022/None/None
- TEST metrics: not run
- keep/reject: REJECT
- failure classification: train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- next action: continue train-side search

## Iteration 193 - guard_daily_loss_7500
- changed rule/parameter: daily loss guard rescue
- parameter group: guard
- old value: 0
- new value: 7500.0
- command: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\scripts\full_loop_a_pullback_c2_high.py --max_iterations 200`
- FIT metrics: trades=993, wins=329, losses=664, win_rate=33.13%, PF=0.3635, net=Rs -364,601, avg_win=Rs 633, avg_loss=Rs -863, SL/TGT/EOD=593/307/93, top_trade/day/symbol=0.0032/None/None
- VAL metrics: trades=729, wins=232, losses=497, win_rate=31.82%, PF=0.3313, net=Rs -276,556, avg_win=Rs 591, avg_loss=Rs -832, SL/TGT/EOD=410/195/124, top_trade/day/symbol=0.0049/None/None
- full TRAIN metrics: trades=1722, wins=561, losses=1161, win_rate=32.58%, PF=0.35, net=Rs -641,157, avg_win=Rs 615, avg_loss=Rs -850, SL/TGT/EOD=1003/502/217, top_trade/day/symbol=0.0019/None/None
- TEST metrics: not run
- keep/reject: REJECT
- failure classification: train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- next action: continue train-side search

## Iteration 194 - regime_align_0.0
- changed rule/parameter: book-level regime alignment rescue
- parameter group: regime
- old value: off
- new value: on band 0.0
- command: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\scripts\full_loop_a_pullback_c2_high.py --max_iterations 200`
- FIT metrics: trades=1048, wins=309, losses=739, win_rate=29.48%, PF=0.3056, net=Rs -433,045, avg_win=Rs 617, avg_loss=Rs -844, SL/TGT/EOD=622/276/150, top_trade/day/symbol=0.0035/None/None
- VAL metrics: trades=825, wins=222, losses=603, win_rate=26.91%, PF=0.2659, net=Rs -364,008, avg_win=Rs 594, avg_loss=Rs -822, SL/TGT/EOD=477/191/157, top_trade/day/symbol=0.0051/None/None
- full TRAIN metrics: trades=1873, wins=531, losses=1342, win_rate=28.35%, PF=0.288, net=Rs -797,054, avg_win=Rs 607, avg_loss=Rs -834, SL/TGT/EOD=1099/467/307, top_trade/day/symbol=0.0021/None/None
- TEST metrics: not run
- keep/reject: REJECT
- failure classification: train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- next action: continue train-side search

## Iteration 195 - regime_align_0.15
- changed rule/parameter: book-level regime alignment rescue
- parameter group: regime
- old value: off
- new value: on band 0.15
- command: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\scripts\full_loop_a_pullback_c2_high.py --max_iterations 200`
- FIT metrics: trades=1164, wins=356, losses=808, win_rate=30.58%, PF=0.3259, net=Rs -458,730, avg_win=Rs 623, avg_loss=Rs -842, SL/TGT/EOD=677/322/165, top_trade/day/symbol=0.003/None/None
- VAL metrics: trades=1012, wins=284, losses=728, win_rate=28.06%, PF=0.2827, net=Rs -429,896, avg_win=Rs 597, avg_loss=Rs -823, SL/TGT/EOD=583/241/188, top_trade/day/symbol=0.0039/None/None
- full TRAIN metrics: trades=2176, wins=640, losses=1536, win_rate=29.41%, PF=0.3057, net=Rs -888,626, avg_win=Rs 611, avg_loss=Rs -833, SL/TGT/EOD=1260/563/353, top_trade/day/symbol=0.0017/None/None
- TEST metrics: not run
- keep/reject: REJECT
- failure classification: train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- next action: continue train-side search

## Iteration 196 - regime_align_0.3
- changed rule/parameter: book-level regime alignment rescue
- parameter group: regime
- old value: off
- new value: on band 0.3
- command: `py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\scripts\full_loop_a_pullback_c2_high.py --max_iterations 200`
- FIT metrics: trades=1232, wins=399, losses=833, win_rate=32.39%, PF=0.3551, net=Rs -452,585, avg_win=Rs 625, avg_loss=Rs -842, SL/TGT/EOD=699/362/171, top_trade/day/symbol=0.0027/None/None
- VAL metrics: trades=1039, wins=297, losses=742, win_rate=28.59%, PF=0.2919, net=Rs -433,078, avg_win=Rs 601, avg_loss=Rs -824, SL/TGT/EOD=596/255/188, top_trade/day/symbol=0.0037/None/None
- full TRAIN metrics: trades=2271, wins=696, losses=1575, win_rate=30.65%, PF=0.3257, net=Rs -885,664, avg_win=Rs 615, avg_loss=Rs -834, SL/TGT/EOD=1295/617/359, top_trade/day/symbol=0.0016/None/None
- TEST metrics: not run
- keep/reject: REJECT
- failure classification: train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0
- next action: continue train-side search
