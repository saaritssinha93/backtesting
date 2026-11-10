"""Reproducible V23 subset backtest on hash-verified V21/G7 native fills.

Run: python -m g3_v23_adx_rsi_20261106.run
No broker access, strategy activation, or writes outside this research package.
"""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import shutil

import numpy as np
import pandas as pd

from . import policy

PACKAGE = Path(__file__).resolve().parent
ROOT = PACKAGE.parent
G7_PACKAGE = ROOT / 'g3_v21_expanded_20261104/refinement_20261104/g7_v4_confirmations_20261104'
G7_SOURCE = G7_PACKAGE / 'results_through_20261104'
ADX_SOURCE = G7_PACKAGE / 'adx_rsi_20261105/results_through_20261104'


def sha(path):
    digest = hashlib.sha256()
    with Path(path).open('rb') as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b''):
            digest.update(block)
    return digest.hexdigest()


def verify_source(folder):
    manifest = json.loads((folder / 'manifest.json').read_text(encoding='utf-8'))
    for name, expected in manifest.items():
        if Path(name).name != name or sha(folder / name) != expected:
            raise ValueError('Source artifact changed: ' + str(folder / name))
    provenance = json.loads((folder / 'provenance.json').read_text(encoding='utf-8'))
    for name, expected in provenance.get('code_sha256', {}).items():
        if sha(name) != expected:
            raise ValueError('Source builder changed: ' + name)
    return sha(folder / 'manifest.json')


def metrics(fills, selected, days):
    rows = fills.loc[fills.day.isin(days)]
    pnl = pd.to_numeric(rows.portfolio_net_profit_rupees, errors='raise')
    if not np.isfinite(pnl).all():
        raise ValueError('Invalid native trade P&L')
    daily = rows.assign(pnl=pnl).groupby('day').pnl.sum().reindex(days, fill_value=0.)
    equity = np.r_[0., daily.cumsum().to_numpy()]
    loss = float(-pnl.clip(upper=0).sum())
    return dict(sessions=len(days), selected_orders=int(selected), trades=len(rows),
                wins=int((pnl > 0).sum()), losses=int((pnl < 0).sum()),
                win_rate_pct=float((pnl > 0).mean() * 100) if len(rows) else 0.,
                net_pnl_rs=float(pnl.sum()), gross_loss_rs=loss,
                profit_factor=float(pnl.clip(lower=0).sum() / loss) if loss else None,
                average_trade_rs=float(pnl.mean()) if len(rows) else 0.,
                median_trade_rs=float(pnl.median()) if len(rows) else 0.,
                average_daily_pnl_rs=float(daily.mean()), median_daily_pnl_rs=float(daily.median()),
                daily_close_drawdown_rs=float((np.maximum.accumulate(equity) - equity).max()),
                worst_day_rs=float(daily.min()), negative_days=int((daily < 0).sum()),
                costs_rs=250. * len(rows))


def check_identities(audit, trades):
    if audit.sid.duplicated().any() or trades.sid.duplicated().any():
        raise ValueError('Duplicate order identity')
    joined = trades.merge(audit, on='sid', suffixes=('_trade', '_audit'), how='left', validate='one_to_one')
    for field in ('day', 'setup_id', 'tradingsymbol', 'side'):
        if not joined[field + '_trade'].eq(joined[field + '_audit']).all():
            raise ValueError('Native fill/admission identity mismatch: ' + field)
    if not policy.flag(joined.all_g7_allowed).all():
        raise ValueError('Native fill was not admitted by the baseline')
    if not pd.to_datetime(joined.entry_ts, utc=True).ge(pd.to_datetime(joined.confirmation_ts, utc=True)).all():
        raise ValueError('Native fill precedes confirmation')


def parity(actual, expected):
    for field in ('selected_orders', 'trades', 'wins', 'losses', 'win_rate_pct', 'net_pnl_rs',
                  'average_trade_rs', 'daily_close_drawdown_rs', 'negative_days', 'worst_day_rs'):
        if not np.isclose(float(actual[field]), float(expected[field]), rtol=0, atol=.01):
            raise ValueError('Baseline parity failed: ' + field)


def table(rows):
    lines = ['| Policy | Trades | Losses | Win rate | Net P&L (Rs) | Gross losses (Rs) | Mean/trade (Rs) | Median/trade (Rs) | Daily-close DD (Rs) |',
             '|:--|--:|--:|--:|--:|--:|--:|--:|--:|']
    for r in rows.itertuples():
        lines.append(f'| {r.policy} | {r.trades} | {r.losses} | {r.win_rate_pct:.2f}% | {r.net_pnl_rs:,.2f} | {r.gross_loss_rs:,.2f} | {r.average_trade_rs:,.2f} | {r.median_trade_rs:,.2f} | {r.daily_close_drawdown_rs:,.2f} |')
    return lines


def run(output):
    output = Path(output).resolve()
    if output.exists() or output == PACKAGE or not output.is_relative_to(PACKAGE):
        raise ValueError('Use a fresh output folder inside V23; existing results are never overwritten')
    proofs = {str(folder.relative_to(ROOT)): verify_source(folder) for folder in (G7_SOURCE, ADX_SOURCE)}
    audit = pd.read_csv(ADX_SOURCE / 'admission_audit.csv')
    trades = pd.read_csv(G7_SOURCE / 'trades.csv')
    source_overall = pd.read_csv(G7_SOURCE / 'overall.csv')
    source_daily = pd.read_csv(G7_SOURCE / 'daily.csv')
    source_days = sorted(source_daily.day.unique())
    if len(audit) != 1092 or len(trades) != 771 or audit.setup_id.nunique() != 73:
        raise ValueError('V21 + G7 frozen inventory changed')
    if len(source_days) != 65 or source_days[0] != '2026-07-29' or source_days[-1] != '2026-11-04':
        raise ValueError('Unexpected source history')
    check_identities(audit, trades)
    masks = policy.masks(audit)
    periods = dict(full_65=source_days, early_59=[d for d in source_days if d <= policy.SELECTION['training_cutoff']],
                   later_6=[d for d in source_days if d > policy.SELECTION['training_cutoff']])
    books, overall_rows, daily_rows = {}, [], []
    for name, mask in masks.items():
        ids = set(audit.loc[mask, 'sid'])
        fills = trades.loc[trades.sid.isin(ids)].copy()
        books[name] = fills
        for period, days in periods.items():
            overall_rows.append(dict(period=period, policy=name, **metrics(fills, int((mask & audit.day.isin(days)).sum()), days)))
        for day in source_days:
            daily_rows.append(dict(day=day, policy=name, **metrics(fills, int((mask & audit.day.eq(day)).sum()), [day])))
    overall, daily = pd.DataFrame(overall_rows), pd.DataFrame(daily_rows)
    baseline = overall.loc[overall.policy.eq('baseline_v21_g7') & overall.period.eq('full_65')].iloc[0]
    saved = source_overall.loc[source_overall.policy.eq('all_g7_entry_confirmations') & source_overall.period.eq('full_65')].iloc[0]
    parity(baseline, saved)
    for day in source_days:
        actual = daily.loc[daily.policy.eq('baseline_v21_g7') & daily.day.eq(day)].iloc[0]
        expected = source_daily.loc[source_daily.policy.eq('all_g7_entry_confirmations') & source_daily.day.eq(day)].iloc[0]
        parity(actual, expected)
    for name in policy.DESCRIPTIONS:
        total = overall.loc[overall.policy.eq(name) & overall.period.eq('full_65')].iloc[0]
        rows = daily.loc[daily.policy.eq(name)]
        for metric in ('trades', 'wins', 'losses', 'selected_orders', 'net_pnl_rs', 'gross_loss_rs'):
            if not np.isclose(rows[metric].sum(), total[metric], atol=.01, rtol=0):
                raise ValueError('Daily reconciliation failed: ' + name + ' ' + metric)
    chosen = policy.select_policy(overall)
    audit = audit.copy()
    for name, mask in masks.items():
        audit['admit_' + name] = mask
    audit['v23_admitted'] = masks[chosen]
    audit['v23_decision'] = np.where(~policy.flag(audit.all_g7_allowed), 'BASELINE_G7_VETO',
                                   np.where(audit.v23_admitted, 'ADMIT', 'ADX_RSI_VETO'))
    slot_rows = []
    for name in ('baseline_v21_g7', chosen):
        if name == chosen and chosen == 'baseline_v21_g7' and slot_rows:
            continue
        for setup in sorted(audit.setup_id.unique()):
            ids = audit.setup_id.eq(setup) & masks[name]
            slot_rows.append(dict(policy=name, setup_id=setup,
                **metrics(books[name].loc[books[name].setup_id.eq(setup)], int(ids.sum()), source_days)))
    decisions = trades.merge(audit[['sid', 'v23_admitted', 'v23_decision']], on='sid', validate='one_to_one')
    removed = decisions.loc[~decisions.v23_admitted]
    config = dict(version=policy.VERSION, scope='BACKTEST_ONLY', execution_authority=False,
                  baseline='V21 + all G7 entry confirmations', selected_policy=chosen,
                  selected_rule=policy.DESCRIPTIONS[chosen], selection=policy.SELECTION,
                  candidate_policies=policy.DESCRIPTIONS, configured_pairs=73,
                  signal_clock_limit='14:00', last_confirmation='14:01',
                  original_pair_cap=3, new_pair_cap=1, new_pair_5m_volume_increment=.10,
                  new_pair_target_pct=.8, new_pair_initial_stop_pct=1.25,
                  native_targets_stops_and_fills_unchanged=True, no_rank_refill=True,
                  unlimited_portfolio_capital=True, capital_per_entry_rs=100000, exposure_multiplier=5,
                  round_trip_cost_per_fill_rs=250,
                  adx_definition='WILDER14_SESSION_RESET_COMPLETED_CONFIRMATION_PREFIX',
                  rsi_definition='G7_SESSION_RESET_RSI14', warmup_policy='PASS_EXISTING_G7_DECISION',
                  missing_ready_feature_policy='ERROR',
                  periods={k: dict(first=v[0], last=v[-1], sessions=len(v)) for k,v in periods.items()},
                  baseline_parity='PASS_FULL_HISTORY_AND_EVERY_DAY',
                  retrospective_only=True, independently_unseen_sessions=0,
                  source_complete_through='2026-11-04',
                  avoided_losing_trades=int(removed.portfolio_net_profit_rupees.lt(0).sum()),
                  removed_winning_trades=int(removed.portfolio_net_profit_rupees.gt(0).sum()))
    output.mkdir(parents=True)
    frozen = output / 'frozen_inputs'
    frozen.mkdir()
    for source, destination in [(ADX_SOURCE / 'admission_audit.csv', 'v21_g7_adx_rsi_features.csv'),
                                (G7_SOURCE / 'trades.csv', 'v21_g7_native_trades.csv'),
                                (G7_SOURCE / 'config.json', 'v21_g7_config.json')]:
        shutil.copy2(source, frozen / destination)
    overall.to_csv(output / 'overall.csv', index=False)
    daily.to_csv(output / 'daily_all_policies.csv', index=False)
    daily.loc[daily.policy.isin(['baseline_v21_g7', chosen])].to_csv(output / 'daily_results.csv', index=False)
    pd.DataFrame(slot_rows).to_csv(output / 'slot_results.csv', index=False)
    audit.to_csv(output / 'admission_audit.csv', index=False)
    books[chosen].to_csv(output / 'v23_trades.csv', index=False)
    decisions.to_csv(output / 'trade_comparison.csv', index=False)
    (output / 'config.json').write_text(json.dumps(config, indent=2, allow_nan=False) + '\n')
    provenance = dict(source_manifest_sha256=proofs,
                      code_sha256={str(Path(__file__)): sha(__file__), str(Path(policy.__file__)): sha(policy.__file__)},
                      frozen_inputs_sha256={p.name: sha(p) for p in frozen.iterdir()},
                      method='Subset of immutable V21/G7 native filled orders under unlimited capital; no replacement or changed exits')
    (output / 'provenance.json').write_text(json.dumps(provenance, indent=2) + '\n')
    full = overall.loc[overall.period.eq('full_65')]
    main = full.loc[full.policy.isin(['baseline_v21_g7', chosen])]
    lines = ['# G3 V23 ADX/RSI backtesting', '',
             f"Selected research configuration: **{chosen}**. {policy.DESCRIPTIONS[chosen]}.", '',
             'Baseline: V21 + G7 confirmations; 73 configured pairs through 14:00, last confirmation 14:01. '
             'Original 34 pairs retain cap 3; 39 newer pairs retain cap 1 plus the volume gate. '
             'New pairs retain target 0.8% and initial stop 1.25%. Native exits are unchanged.', '',
             'History: **65 sealed sessions, 29 July–4 November 2026**. November 5–6 are not included. '
             'October 1 remains excluded for incomplete immutable source data. '
             'Unlimited portfolio capital; Rs 100,000 per entry with 5x exposure and Rs 250 round-trip cost per fill. '
             'This differs from LIVE sizing.', '', '## Full-history baseline comparison', '', *table(main), '',
             f"Losing trades removed: **{config['avoided_losing_trades']}**; winning trades also removed: **{config['removed_winning_trades']}**.", '',
             '## Earlier 59 sessions used for selection', '',
             *table(overall.loc[overall.period.eq('early_59') & overall.policy.isin(['baseline_v21_g7', chosen])]), '',
             '## Later six sessions: retrospective check', '',
             *table(overall.loc[overall.period.eq('later_6') & overall.policy.isin(['baseline_v21_g7', chosen])]), '',
             'All these sessions have been observed in earlier V21/G7 research. The later six are excluded from '
             'this selector but are NOT independent or prospective validation. Report both periods; do not change '
             'the chosen rule based on the later results. Daily-close drawdown is not intraday drawdown.', '',
             '## All fixed hypotheses', '', *table(full), '',
             '## Selection and data controls', '',
             'Eight ADX/RSI hypotheses plus baseline were fixed before this run. Selection uses only the first '
             '59 sessions: retain at least 50% of trades and 75% of baseline net profit, improve win rate by '
             'at least 0.5 percentage points, lower gross loss, and do not worsen daily-close drawdown. '
             'Rank eligible policies by win rate, then lower drawdown, then net profit. If none qualifies, retain baseline.', '',
             'Indicators use the completed confirmation-minute prefix. ADX14 and RSI14 reset each session; '
             'early warm-up unavailability preserves the baseline decision. Invalid ready values or future-dated '
             'features abort the run. Rejected orders do not promote another stock. Removing independent native '
             'fills is exact under the inherited unlimited-capital/no-refill convention; it is not a replay of '
             'a finite-capital account. Baseline aggregate and all 65 daily results matched the source.', '',
             '## Results', '',
             '[Day-wise comparison](daily_results.csv) · [All 73 pairs](slot_results.csv) · '
             '[V23 trades](v23_trades.csv) · [Every admission decision](admission_audit.csv) · '
             '[Removed winners and losers](trade_comparison.csv) · [Configuration](config.json) · [Provenance](provenance.json)', '',
             'No LIVE configuration, scheduler, approval, broker connection, or V21 result was changed.', '']
    (output / 'REPORT.md').write_text('\n'.join(lines), encoding='utf-8')
    (output / 'manifest.json').write_text(json.dumps({str(p.relative_to(output)): sha(p)
        for p in sorted(output.rglob('*')) if p.is_file()}, indent=2) + '\n')
    print(main.to_string(index=False))
    print('SELECTED:', chosen)
    return config


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, default=PACKAGE / 'results_through_20261104')
    args = parser.parse_args()
    run(args.output)


if __name__ == '__main__':
    main()
