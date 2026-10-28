"""Read-only monthly attribution of frozen G8 backtest ledgers."""
from pathlib import Path
import json
import hashlib
import sys
import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))
from research.g8_exit_engine import metrics

OUT = Path(__file__).resolve().parent
RUN = Path('C:/TradingData/eqidv2/backtesting_result_v13_v10_g_8/daily_comparison/2026-10-23/20261023T125505562512_g8_baseline_vs_shock')
summary = json.loads((RUN / 'summary.json').read_text())
manifest = json.loads((RUN / 'manifest.json').read_text())
for relative in ['summary.json', 'baseline/combined_portfolio_trades.csv', 'shock/combined_portfolio_trades.csv', 'features.csv']:
    expected = manifest['files'].get(relative, manifest['files'].get(relative.replace('/', '\\')))
    assert expected == hashlib.sha256((RUN / relative).read_bytes()).hexdigest(), relative
days = summary['periods']['all']
prior = Path(summary['prior_run'])
features = pd.concat([pd.read_csv(prior / 'features.csv'), pd.read_csv(RUN / 'features.csv')], ignore_index=True)
assert not features.sid.duplicated().any()
features = features.set_index('sid')
rows, sides, regimes, feature_stats, diagnostics = [], [], [], [], []
proof = {}

for label in ('baseline', 'shock'):
    path = RUN / label / 'combined_portfolio_trades.csv'
    proof[str(path)] = hashlib.sha256(path.read_bytes()).hexdigest()
    book = pd.read_csv(path)
    assert not book.sid.duplicated().any()
    trades = book.loc[book.portfolio_executed.eq(True)].copy()
    trades['month'] = trades.day.str[:7]
    for month in sorted(trades.month.unique()):
        calendar = [d for d in days if d.startswith(month)]
        part = trades.loc[trades.month.eq(month)]
        m = metrics(book, calendar)
        m.update(model='G8' if label == 'baseline' else 'G8_shock', month=month,
                 stop_rate_pct=m['stop_exits'] / m['trades'] * 100,
                 target_rate_pct=m['target_exits'] / m['trades'] * 100,
                 losing_day_rate_pct=m['losing_days'] / len(calendar) * 100,
                 avg_win_rs=part.loc[part.portfolio_net_profit_rupees.gt(0), 'portfolio_net_profit_rupees'].mean(),
                 avg_loss_abs_rs=-part.loc[part.portfolio_net_profit_rupees.lt(0), 'portfolio_net_profit_rupees'].mean(),
                 mean_mfe_pct=part.mfe_pct.mean(), mean_mae_pct=part.mae_pct.mean(),
                 median_mfe_pct=part.mfe_pct.median(), median_mae_pct=part.mae_pct.median())
        rows.append(m)
        for side, sub in part.groupby('side'):
            p = sub.portfolio_net_profit_rupees
            sides.append(dict(model=m['model'], month=month, side=side, trades=len(sub), wins=int(p.gt(0).sum()),
                              win_rate_pct=100*p.gt(0).mean(), net_pnl_rs=p.sum(),
                              stop_rate_pct=100*sub.exit_reason.isin(['STOP','TIGHTENED_STOP','PROTECTED_STOP']).mean()))
        f = features.reindex(part.sid).copy()
        f['net_pnl_rs'] = part.portfolio_net_profit_rupees.to_numpy()
        for regime, sub in f.groupby('exit_regime'):
            regimes.append(dict(model=m['model'], month=month, regime=regime, trades=len(sub),
                                share_pct=100*len(sub)/len(f), wins=int(sub.net_pnl_rs.gt(0).sum()),
                                win_rate_pct=100*sub.net_pnl_rs.gt(0).mean(), net_pnl_rs=sub.net_pnl_rs.sum()))
        if label == 'baseline':
            for field in ('vix_level','vix_level_ratio','vix_5m_pct','vix_30m_pct','signed_nifty_slot',
                          'signed_nifty_session','confirmation_range_pct','signal_move_abs_pct','volume_log',
                          'oi_change_pct','confirmation_close_position','signal_body_ratio'):
                series = pd.to_numeric(f[field], errors='coerce')
                feature_stats.append(dict(month=month, feature=field, count=int(series.notna().sum()),
                                          mean=series.mean(), median=series.median(), q25=series.quantile(.25), q75=series.quantile(.75)))
            for group, sub in [('all', part), ('wins', part.loc[part.portfolio_net_profit_rupees.gt(0)]),
                               ('losses', part.loc[part.portfolio_net_profit_rupees.lt(0)])]:
                diagnostics.append(dict(month=month, outcome=group, trades=len(sub),
                    mfe_mean=sub.mfe_pct.mean(), mfe_median=sub.mfe_pct.median(),
                    mae_mean=sub.mae_pct.mean(), mae_median=sub.mae_pct.median(),
                    never_positive_pct=100*sub.mfe_pct.le(0).mean(), under_010_mfe_pct=100*sub.mfe_pct.lt(.1).mean(),
                    under_025_mfe_pct=100*sub.mfe_pct.lt(.25).mean(),
                    target_pct_median=sub.target_pct.median(), holding_minutes_median=sub.holding_minutes.median()))

base = pd.read_csv(RUN/'baseline/combined_portfolio_trades.csv')
base = base.loc[base.portfolio_executed.eq(True)].copy()
base['win'] = base.portfolio_net_profit_rupees.gt(0).astype(int)
daily = base.groupby('day').agg(n=('win','size'), wins=('win','sum'), pnl=('portfolio_net_profit_rupees','sum'))
rng = np.random.default_rng(20261023)
bootstrap = {}
for name, mask in [('august_september', daily.index.str[:7].isin(['2026-08','2026-09'])),
                   ('october', daily.index.str.startswith('2026-10'))]:
    cohort = daily.loc[mask]
    sampled = rng.integers(0, len(cohort), (20000, len(cohort)))
    rates = 100 * cohort.wins.to_numpy()[sampled].sum(axis=1) / cohort.n.to_numpy()[sampled].sum(axis=1)
    bootstrap[name] = dict(sessions=len(cohort), trades=int(cohort.n.sum()), wins=int(cohort.wins.sum()),
                            win_rate_pct=100*cohort.wins.sum()/cohort.n.sum(),
                            bootstrap_ci95=list(np.quantile(rates,[.025,.975])), samples=rates)
diff = bootstrap['october']['samples'] - bootstrap['august_september']['samples']
for item in bootstrap.values():
    item.pop('samples')
bootstrap['october_minus_augsep'] = dict(delta_win_rate_pp=bootstrap['october']['win_rate_pct']-bootstrap['august_september']['win_rate_pct'],
                                        daily_block_bootstrap_ci95_pp=list(np.quantile(diff,[.025,.975])))
bootstrap['limitation'] = 'Descriptive resampling of days within historical months; does not correct strategy selection or serial dependence across days, and is not causal evidence.'
(OUT/'g8_day_block_bootstrap.json').write_text(json.dumps(bootstrap, indent=2))

pd.DataFrame(rows).to_csv(OUT/'monthly_g8.csv', index=False)
pd.DataFrame(sides).to_csv(OUT/'monthly_g8_side.csv', index=False)
pd.DataFrame(regimes).to_csv(OUT/'monthly_g8_regime.csv', index=False)
pd.DataFrame(feature_stats).to_csv(OUT/'monthly_g8_entry_features.csv', index=False)
pd.DataFrame(diagnostics).to_csv(OUT/'monthly_g8_outcome_diagnostics.csv', index=False)
(OUT/'provenance.json').write_text(json.dumps(dict(run=str(RUN), manifest_sha256=hashlib.sha256((RUN/'manifest.json').read_bytes()).hexdigest(),
    days=days, source_books=proof, policy=summary['baseline_policy'], shock=summary['shock_policy'],
    caveat='Frozen retrospective history, modeled costs; no production or research pointers changed.'), indent=2))
print(pd.DataFrame(rows)[['model','month','sessions','trades','wins','win_rate_pct','net_pnl_rs','profit_factor','stop_rate_pct','target_rate_pct','losing_days','losing_day_rate_pct','mean_mfe_pct','mean_mae_pct']].round(3).to_string(index=False))
print('\nSIDES\n'+pd.DataFrame(sides).round(3).to_string(index=False))
print('\nREGIMES\n'+pd.DataFrame(regimes).round(3).to_string(index=False))
