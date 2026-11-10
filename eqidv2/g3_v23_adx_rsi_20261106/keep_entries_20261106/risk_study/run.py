"""Keep every baseline fill and change exposure using confirmation-time indicators.

Fees scale at the inherited 5 bps rate. No winning/losing trade labels participate
in sizing. This is a fractional-notional sensitivity replay, not an integer-share
or live execution simulation.
"""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

import numpy as np
import pandas as pd

from g3_v23_adx_rsi_20261106 import policy as v23
from g3_v23_adx_rsi_20261106 import run as original

PACKAGE = Path(__file__).resolve().parent
SOURCE = PACKAGE.parents[1] / 'results_through_20261104'
CUTOFF = '2026-10-27'
POLICIES = {
    'baseline_fixed_size': {'strong': 1.0, 'weak': 1.0, 'normalized': False},
    'weak_75pct': {'strong': 1.0, 'weak': .75, 'normalized': False},
    'weak_50pct': {'strong': 1.0, 'weak': .50, 'normalized': False},
    'strong_110_weak_75': {'strong': 1.10, 'weak': .75, 'normalized': False},
    'strong_125_weak_50': {'strong': 1.25, 'weak': .50, 'normalized': False},
    'train_normalized_weak_50': {'strong': 1.0, 'weak': .50, 'normalized': True},
}


def sha(path):
    return original.sha(path)


def verify_source():
    manifest = json.loads((SOURCE / 'manifest.json').read_text())
    for name, digest in manifest.items():
        path = (SOURCE / name).resolve()
        if not path.is_relative_to(SOURCE.resolve()) or sha(path) != digest:
            raise ValueError('V23 source manifest mismatch: ' + name)
    for name, digest in json.loads((SOURCE / 'provenance.json').read_text())['frozen_inputs_sha256'].items():
        if sha(SOURCE / 'frozen_inputs' / name) != digest:
            raise ValueError('V23 frozen input mismatch: ' + name)
    return sha(SOURCE / 'manifest.json')


def factors(audit, name, normalization=1.):
    if name not in POLICIES or normalization <= 0 or not np.isfinite(normalization):
        raise ValueError('Invalid sizing policy')
    # This mask uses only causally available ADX, DI, and RSI fields. It does
    # not use trade fill/outcome information and does not filter any entry.
    strong = v23.masks(audit)['adx20_di_rsi']
    ready = v23.flag(audit.adx_ready) & v23.flag(audit.rsi_ready)
    spec = POLICIES[name]
    size = np.where(ready, np.where(strong, spec['strong'], spec['weak']), 1.)
    return pd.Series(size * normalization, index=audit.index)


def normalization(audit, name):
    if not POLICIES[name]['normalized']:
        return 1.
    # Only the earlier 59 sessions' admitted ORDERS establish the constant,
    # never later observations or hindsight knowledge of which orders fill.
    train = audit.loc[audit.day.le(CUTOFF) & v23.flag(audit.all_g7_allowed)]
    if train.empty:
        raise ValueError('Missing training orders for fixed sizing normalization')
    return 1. / float(factors(train, name).mean())


def metric(frame, days):
    rows = frame.loc[frame.day.isin(days)]
    pnl = rows.adjusted_net_pnl_rs
    daily = rows.groupby('day').adjusted_net_pnl_rs.sum().reindex(days, fill_value=0.)
    equity = np.r_[0., daily.cumsum().to_numpy()]
    return dict(sessions=len(days), trades=len(rows), wins=int(pnl.gt(0).sum()), losses=int(pnl.lt(0).sum()),
        win_rate_pct=float(pnl.gt(0).mean() * 100) if len(rows) else 0.,
        net_pnl_rs=float(pnl.sum()), gross_loss_rs=float(-pnl.clip(upper=0).sum()),
        daily_close_drawdown_rs=float((np.maximum.accumulate(equity)-equity).max()),
        negative_days=int(daily.lt(0).sum()), worst_day_rs=float(daily.min()),
        mean_trade_rs=float(pnl.mean()) if len(rows) else 0.,
        median_trade_rs=float(pnl.median()) if len(rows) else 0.,
        gross_entry_notional_rs=float(rows.adjusted_entry_notional_rs.sum()),
        exposure_ratio_to_baseline=float(rows.size_factor.mean()) if len(rows) else 0.,
        variable_costs_rs=float(rows.adjusted_costs_rs.sum()))


def choose(overall):
    early = overall.loc[overall.period.eq('early_59')]
    base = early.loc[early.policy.eq('baseline_fixed_size')].iloc[0]
    eligible = early.loc[early.policy.ne('baseline_fixed_size')
        & early.net_pnl_rs.ge(base.net_pnl_rs)
        & early.gross_loss_rs.lt(base.gross_loss_rs)
        & early.daily_close_drawdown_rs.lt(base.daily_close_drawdown_rs)]
    if eligible.empty:
        return 'baseline_fixed_size', 'NO_ELIGIBLE_SIZING_CHANGE'
    chosen = eligible.sort_values(['net_pnl_rs','daily_close_drawdown_rs','policy'],
        ascending=[False,True,True]).iloc[0].policy
    return chosen, 'EARLIER_PERIOD_CANDIDATE_ONLY'


def make_book(trades, audit, name, scale):
    applied = audit[['sid']].copy()
    applied['size_factor'] = factors(audit, name, scale)
    book = trades.merge(applied, on='sid', how='left', validate='one_to_one')
    if len(book) != len(trades) or book.size_factor.isna().any() or not book.size_factor.gt(0).all():
        raise ValueError('Sizing changed trade inventory')
    book['policy'] = name
    book['baseline_costs_rs'] = 250.
    book['baseline_entry_notional_rs'] = 500000.
    book['adjusted_entry_notional_rs'] = book.baseline_entry_notional_rs * book.size_factor
    book['adjusted_costs_rs'] = book.baseline_costs_rs * book.size_factor
    book['adjusted_gross_pnl_rs'] = (book.portfolio_net_profit_rupees + book.baseline_costs_rs) * book.size_factor
    book['adjusted_net_pnl_rs'] = book.adjusted_gross_pnl_rs - book.adjusted_costs_rs
    if not np.array_equal(np.sign(book.adjusted_net_pnl_rs), np.sign(book.portfolio_net_profit_rupees)):
        raise ValueError('Positive scaling changed trade outcome signs')
    return book


def run(output):
    output = Path(output).resolve()
    if output.exists() or output == PACKAGE or not output.is_relative_to(PACKAGE):
        raise ValueError('Use a fresh output folder within risk_study')
    source_hash = verify_source()
    audit = pd.read_csv(SOURCE / 'frozen_inputs/v21_g7_adx_rsi_features.csv')
    trades = pd.read_csv(SOURCE / 'frozen_inputs/v21_g7_native_trades.csv')
    original.check_identities(audit, trades)
    if len(trades) != 771 or trades.sid.duplicated().any():
        raise ValueError('Expected 771 distinct baseline fills')
    days = sorted(audit.day.unique())
    if len(days) != 65:
        raise ValueError('Expected 65 source sessions')
    periods = {'full_65':days,'early_59':[d for d in days if d <= CUTOFF],
               'later_6':[d for d in days if d > CUTOFF]}
    overall, daily, books, constants = [], [], {}, {}
    for name in POLICIES:
        constants[name] = normalization(audit, name)
        book = make_book(trades, audit, name, constants[name])
        books[name] = book
        for label, cohort in periods.items():
            overall.append(dict(policy=name, period=label, **metric(book,cohort)))
        for day in days:
            daily.append(dict(policy=name, day=day, **metric(book,[day])))
    overall, daily = pd.DataFrame(overall), pd.DataFrame(daily)
    baseline = overall.loc[overall.policy.eq('baseline_fixed_size') & overall.period.eq('full_65')].iloc[0]
    if not (baseline.trades == 771 and baseline.losses == 247 and
            np.isclose(baseline.net_pnl_rs,1149524.04618079,rtol=0,atol=.01)):
        raise ValueError('Baseline parity failed')
    for name in POLICIES:
        full = overall.loc[overall.policy.eq(name)&overall.period.eq('full_65')].iloc[0]
        sums = daily.loc[daily.policy.eq(name)]
        for field in ('trades','wins','losses','net_pnl_rs','gross_loss_rs','gross_entry_notional_rs','variable_costs_rs'):
            if not np.isclose(full[field],sums[field].sum(),rtol=0,atol=.01):
                raise ValueError('Daily reconciliation failed: '+name+' '+field)
    selected,status = choose(overall)
    config = dict(scope='BACKTEST_ONLY', execution_authority=False, selection=status,
        selected_policy=selected, selected_from='early_59', policies=POLICIES, normalization_constants=constants,
        signal='V23 ADX14>=20, directional DI, direction-normalized RSI three-minute change >=0',
        warmup_policy='Keep baseline size before both ADX and RSI are ready; optional normalization applies globally',
        trades_preserved=771, selected_orders_unchanged=871, native_exit_paths_unchanged=True,
        fill_identity_validation='PASS', daily_reconciliation='PASS',
        preserved_native_fee_rs=250, fee_assumption='Same 5bps cost rate; fractional-notional fee scales with size',
        entry_notional_rs=500000, capital_per_entry_rs=100000, leverage=5,
        fractional_notional_only=True, integer_share_lot_rounding_not_modelled=True,
        later_six_independent_validation=False, retrospective_only=True)
    output.mkdir(parents=True)
    overall.to_csv(output/'overall.csv',index=False)
    daily.to_csv(output/'daily_results.csv',index=False)
    pd.concat(books.values(),ignore_index=True).to_csv(output/'trade_comparison.csv',index=False)
    (output/'config.json').write_text(json.dumps(config,indent=2,allow_nan=False)+'\n')
    (output/'provenance.json').write_text(json.dumps(dict(source_manifest_sha256=source_hash,
        source=str(SOURCE),code_sha256={str(Path(__file__)):sha(__file__)},
        feature_policy_sha256=sha(v23.__file__)),indent=2)+'\n')
    lines=['# V23 keep-all-entries risk-sizing study','',
        'Every variant preserves all **771 fills**, their symbols, sides, setup IDs, entry/exit times and prices. '
        'No entries or exits change. Positive size multipliers preserve all **524 winning / 247 losing trades** '
        'and **67.96% win rate**. Only rupee amounts change.','',
        'History: 65 sessions, 29 July–4 November 2026. Baseline is V21 + G7, '
        'Rs 100,000 capital at 5x exposure per trade, unlimited portfolio capital.','',
        '**Selection:** '+status+'; selected policy: `'+selected+'`. Selection uses the earlier 59 sessions only, '
        'requiring no lower net profit, lower gross loss and lower daily-close drawdown. '
        'Candidates are ranked by net profit, then drawdown.','']
    for period,title in [('full_65','All 65 sessions'),('early_59','Earlier 59 sessions'),('later_6','Later six retrospective sessions')]:
        lines += ['## '+title,'','| Policy | Trades | Net P&L (Rs) | Gross loss (Rs) | Daily-close DD (Rs) | Exposure / baseline | Losing days |',
                  '|:--|--:|--:|--:|--:|--:|--:|']
        for r in overall.loc[overall.period.eq(period)].itertuples():
            lines.append(f'| {r.policy} | {r.trades} | {r.net_pnl_rs:,.2f} | {r.gross_loss_rs:,.2f} | {r.daily_close_drawdown_rs:,.2f} | {r.exposure_ratio_to_baseline:.4f}x | {r.negative_days} |')
        lines.append('')
    lines += ['## Limits and fairness','',
        'A lower rupee loss from smaller positions is partly less risk deployed, not fewer losing trades. '
        'Increasing strong-position size can increase profit by increasing exposure; compare the exposure column. '
        'The normalized arm uses a single normalization constant computed from earlier admitted orders, '
        'not which orders eventually filled or any later results. It targets equal earlier-order notional, '
        'not exact equal filled exposure in every period.','',
        'Source native fees and P&L are retained alongside adjusted values. The sensitivity assumes linear '
        '5bps costs, unchanged fills/slippage and fractional notional. Real integer share counts, fixed fee '
        'components and size-dependent market impact can differ. No sizing change is proposed for LIVE. '
        'The later six sessions were previously observed, so are not independent forward validation.','',
        '[Day-wise results](daily_results.csv) · [Every trade and factor](trade_comparison.csv) · [Configuration](config.json)','']
    (output/'REPORT.md').write_text('\n'.join(lines),encoding='utf-8')
    (output/'manifest.json').write_text(json.dumps({p.name:sha(p) for p in output.iterdir() if p.is_file()},indent=2)+'\n')
    print(overall.loc[overall.period.eq('full_65')].to_string(index=False))
    print('SELECTION:',status,selected)
    return config


if __name__ == '__main__':
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--output',type=Path,default=PACKAGE/'results_through_20261104')
    run(p.parse_args().output)
