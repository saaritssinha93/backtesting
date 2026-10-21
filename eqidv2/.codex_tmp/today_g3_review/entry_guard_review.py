"""Read-only fixed-trade sensitivity for the immutable 2026-10-15 G3 run.

This is not a pre-quota reselection or portfolio resimulation.
"""
from pathlib import Path
import hashlib
import json

import pandas as pd


ROOT = Path(__file__).resolve().parents[2]
OUT = Path(__file__).resolve().parent
RUN = Path('C:/TradingData/eqidv2/backtesting_result_v13_v10_g_3/production/runs/2026-10-15/20261015T170545010772_39928700')
G4_CONFIG = ROOT / 'configs/strategies/v13_v10_g_4_g3_mirror_guard_v9.json'


def metrics(frame):
    pnl = frame.portfolio_net_profit_rupees
    return {
        'trades': len(frame), 'wins': int((pnl > 0).sum()),
        'losses': int((pnl < 0).sum()),
        'win_rate_pct': float((pnl > 0).mean() * 100),
        'net_pnl_rupees': float(pnl.sum()),
        'stop_exits': int(frame.exit_reason.isin(['STOP', 'TIGHTENED_STOP']).sum()),
    }


def main():
    cfg = json.loads(G4_CONFIG.read_text(encoding='utf-8'))
    primary = cfg['primary_scenario']
    limits = {**cfg['g3_contract']['maximum_confirmation_extension_atr_by_setup'],
              **cfg['scenarios'][primary]}
    all_rows = pd.read_csv(RUN / 'portfolio_trades.csv')
    trades = all_rows.loc[all_rows.portfolio_executed.astype(str).str.lower().eq('true')].copy()
    trades['confirmation_extension_atr'] = (
        (trades.confirmation_close - trades.v9_1m_ema20) / trades.v9_1m_atr14
        * trades.side.map({'LONG': 1, 'SHORT': -1})
    )
    assert len(trades) == 9
    assert trades.confirmation_extension_atr.notna().all()
    trades['existing_g4_atr_limit'] = trades.setup_id.map(limits)
    trades['existing_g4_atr_reject'] = trades.existing_g4_atr_limit.notna() & (
        trades.confirmation_extension_atr > trades.existing_g4_atr_limit)
    trades['blanket_3atr_reject'] = trades.confirmation_extension_atr > 3
    columns = [
        'tradingsymbol', 'setup_id', 'side', 'signal_ts', 'confirmation_ts', 'entry_ts',
        'exit_ts', 'exit_reason', 'portfolio_net_profit_rupees', 'confirmation_close',
        'v9_1m_ema20', 'v9_1m_atr14', 'confirmation_extension_atr', 'v9_1m_volume_ratio',
        'volume_ratio', 'mfe_pct', 'mae_pct', 'existing_g4_atr_limit',
        'existing_g4_atr_reject', 'blanket_3atr_reject',
    ]
    trades[columns].to_csv(OUT / 'today_entry_guard_audit.csv', index=False)
    report = {
        'scope': 'FIXED_EXECUTED_TRADE_REMOVAL_ONLY_NO_RESELECTION_NO_REALLOCATION',
        'session_date': '2026-10-15', 'source_run': str(RUN),
        'source_portfolio_sha256': hashlib.sha256((RUN / 'portfolio_trades.csv').read_bytes()).hexdigest(),
        'g4_default_config': str(G4_CONFIG), 'g4_primary_scenario': primary,
        'g4_atr_limits': limits,
        'formula_long': '(completed_confirmation_close - EMA20) / ATR14',
        'formula_short': '(EMA20 - completed_confirmation_close) / ATR14',
        'baseline': metrics(trades),
        'existing_g4_atr_rules_applied_to_today_g3_fills': metrics(trades.loc[~trades.existing_g4_atr_reject]),
        'blanket_3atr_diagnostic': metrics(trades.loc[~trades.blanket_3atr_reject]),
        'existing_g4_rejected_trades': trades.loc[trades.existing_g4_atr_reject, columns].to_dict('records'),
        'blanket_3atr_rejected_trades': trades.loc[trades.blanket_3atr_reject, columns].to_dict('records'),
        'caveats': [
            'Baseline entry, exit, costs, sizes, and fills remain fixed; removed positions are not replaced.',
            'A real guard may select another candidate and change portfolio capacity; this is not a full G4 result.',
            'Current G4 mirrors frozen G3 V11 with 49 setups; today G3 is V13 with 51 setups.',
            'Latest published G4 run covers through 2026-10-14 only, not 2026-10-15.',
            'Blanket 3ATR is a previously studied threshold, not a new optimized threshold.',
            'Blanket 3ATR removed 175 winners/67 losers in the prior 51-session fixed-trade screen, reducing net PnL by 613110.992970 rupees.',
            'Do not deploy a today-only improvement as if it were holdout-qualified.',
        ],
    }
    # JSON-compatible missing limits in the trade diagnostics.
    report = json.loads(json.dumps(report, default=str).replace('NaN', 'null'))
    (OUT / 'entry_guard_review.json').write_text(json.dumps(report, indent=2), encoding='utf-8')
    print(json.dumps({k: report[k] for k in ['baseline', 'existing_g4_atr_rules_applied_to_today_g3_fills', 'blanket_3atr_diagnostic']}, indent=2))


if __name__ == '__main__':
    main()
