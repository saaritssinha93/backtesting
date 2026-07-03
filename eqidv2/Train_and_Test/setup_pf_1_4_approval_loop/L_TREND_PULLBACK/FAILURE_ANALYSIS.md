# L_TREND_PULLBACK - FAILURE_ANALYSIS

## Failure Classification
- Baseline failure: TRAIN PF too low and raw pullbacks bleed through costs.
- Main recurring search failure: many candidates either stay below TRAIN PF 1.30 or enter the train band but fail dominance / thin-test confidence.
- TEST caveat: only two TEST sessions (`2026-06-22`, `2026-06-24`) are available for this setup under the strict split.

## Worst Days
- {'window': 'TRAIN', 'trade_date': '2026-06-15', 'trades': 35, 'net_pnl': -15732.24}
- {'window': 'TRAIN', 'trade_date': '2026-05-21', 'trades': 16, 'net_pnl': -10280.13}
- {'window': 'TRAIN', 'trade_date': '2026-06-10', 'trades': 10, 'net_pnl': -9287.8}
- {'window': 'TRAIN', 'trade_date': '2026-05-29', 'trades': 8, 'net_pnl': -7428.75}
- {'window': 'TRAIN', 'trade_date': '2026-06-05', 'trades': 9, 'net_pnl': -6272.25}
- {'window': 'TRAIN', 'trade_date': '2026-06-01', 'trades': 6, 'net_pnl': -5571.5}
- {'window': 'TRAIN', 'trade_date': '2026-06-04', 'trades': 6, 'net_pnl': -4982.42}
- {'window': 'TRAIN', 'trade_date': '2026-05-19', 'trades': 4, 'net_pnl': -3711.09}
- {'window': 'TRAIN', 'trade_date': '2026-05-27', 'trades': 3, 'net_pnl': -2795.3}
- {'window': 'TRAIN', 'trade_date': '2026-05-25', 'trades': 5, 'net_pnl': -2681.71}

## Worst Symbols
- {'window': 'TRAIN', 'ticker': 'J&KBANK', 'trades': 2, 'net_pnl': -1864.48}
- {'window': 'TRAIN', 'ticker': 'ANTELOPUS', 'trades': 2, 'net_pnl': -1856.22}
- {'window': 'TRAIN', 'ticker': 'NYKAA', 'trades': 2, 'net_pnl': -1440.86}
- {'window': 'TRAIN', 'ticker': 'HARSHA', 'trades': 1, 'net_pnl': -932.71}
- {'window': 'TRAIN', 'ticker': 'CANBK', 'trades': 1, 'net_pnl': -932.63}
- {'window': 'TRAIN', 'ticker': 'PARADEEP', 'trades': 1, 'net_pnl': -932.63}
- {'window': 'TRAIN', 'ticker': 'IMFA', 'trades': 1, 'net_pnl': -932.62}
- {'window': 'TRAIN', 'ticker': 'GROWW', 'trades': 1, 'net_pnl': -932.51}
- {'window': 'TRAIN', 'ticker': 'ZENTEC', 'trades': 1, 'net_pnl': -932.49}
- {'window': 'TRAIN', 'ticker': 'PNBGILTS', 'trades': 1, 'net_pnl': -932.43}

## Time Window Result
- {'window': 'TEST', 'entry_hour': 12, 'trades': 5, 'pf': 2.3384, 'net_pnl': 1246.48}
- {'window': 'TEST', 'entry_hour': 13, 'trades': 1, 'pf': 0.0, 'net_pnl': -735.45}
- {'window': 'TRAIN', 'entry_hour': 11, 'trades': 29, 'pf': 0.1523, 'net_pnl': -16061.3}
- {'window': 'TRAIN', 'entry_hour': 12, 'trades': 44, 'pf': 0.2501, 'net_pnl': -21769.11}
- {'window': 'TRAIN', 'entry_hour': 13, 'trades': 35, 'pf': 0.338, 'net_pnl': -12775.1}
- {'window': 'TRAIN', 'entry_hour': 14, 'trades': 24, 'pf': 0.0353, 'net_pnl': -18194.53}