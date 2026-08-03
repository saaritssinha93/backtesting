# Path Rework Result - A_PULLBACK_C2_THEN_BREAK_C2_HIGH

Research-only. No final config or live/paper watch change was made.

## Status
- Pool signals evaluated: 6833
- Path validation vs canonical base resolver: {'sampled': 300, 'matched': 297, 'match_rate': 0.99, 'mismatches': [{'sid': 2269, 'ticker': 'AYE', 'mine': 'SL', 'canonical': 'TARGET'}, {'sid': 3964, 'ticker': 'HINDPETRO', 'mine': 'SL', 'canonical': 'TARGET'}, {'sid': 3842, 'ticker': 'DYCL', 'mine': 'SL', 'canonical': 'TARGET'}]}
- Iterations: 420
- Passing approval-required candidates: 0

## Top TRAIN/TEST Rows
No candidate reached TEST.

## Top Robust Search Scores
- G_market_ret_pct<=-0.095256: FIT 298/0.8243 VAL 172/0.5941 TRAIN 464/0.7551 net=Rs -61,549; verdict=REJECT
- F_mloss1: FIT 349/0.6192 VAL 243/0.5782 TRAIN 594/0.6001 net=Rs -142,326; verdict=REJECT
- A_first_sl1.4_t2.2: FIT 798/0.6765 VAL 580/0.5431 TRAIN 1384/0.6268 net=Rs -340,905; verdict=REJECT
- A_none_sl1.4_t2.2: FIT 799/0.6323 VAL 586/0.5519 TRAIN 1388/0.6092 net=Rs -355,814; verdict=REJECT
- F_mloss2: FIT 459/0.5831 VAL 353/0.5892 TRAIN 810/0.5732 net=Rs -208,833; verdict=REJECT
- A_first_sl1.4_t1.8: FIT 835/0.6818 VAL 600/0.5352 TRAIN 1435/0.6148 net=Rs -356,243; verdict=REJECT
- G_market_ret_pct<=0.03817: FIT 422/0.7121 VAL 353/0.4905 TRAIN 778/0.6246 net=Rs -167,920; verdict=REJECT
- G_market_ret_pct<=-0.561011: FIT 188/0.5074 VAL 32/0.5906 TRAIN 215/0.5896 net=Rs -56,118; verdict=REJECT
- A_none_sl1.4_t1.8: FIT 826/0.6546 VAL 612/0.5264 TRAIN 1451/0.5859 net=Rs -392,996; verdict=REJECT
- A_first_sl0.85_t2.2: FIT 968/0.6053 VAL 726/0.512 TRAIN 1685/0.585 net=Rs -418,141; verdict=REJECT
- A_none_sl0.85_t2.2: FIT 991/0.556 VAL 726/0.5391 TRAIN 1714/0.5523 net=Rs -461,255; verdict=REJECT
- A_none_sl1.0_t2.2: FIT 909/0.5999 VAL 673/0.514 TRAIN 1579/0.5756 net=Rs -412,595; verdict=REJECT
- A_first_sl0.85_t1.8: FIT 1012/0.5941 VAL 752/0.5077 TRAIN 1765/0.5761 net=Rs -438,795; verdict=REJECT
- G_market_abs_ret_pct>=0.31274: FIT 532/0.5441 VAL 236/0.5249 TRAIN 764/0.5539 net=Rs -211,547; verdict=REJECT
- A_none_sl1.4_t2.8: FIT 757/0.6268 VAL 560/0.5169 TRAIN 1312/0.5709 net=Rs -383,907; verdict=REJECT

## Controlled TRAIN PF Rows
none

## Passing Candidates
No path-rework candidate passed TRAIN PF > 1.30 and TEST PF > 1.40 with positive PnL/stability gates.
