"""Append the complete sealed Nov5 session to fixed V23 research policies.

Earlier results are hash-verified and retained. No threshold or selected policy
is optimized on the appended session. No broker access or live writes occur.
"""
from __future__ import annotations

import argparse
import json
import pickle
from pathlib import Path

import numpy as np
import pandas as pd

from g3_v23_adx_rsi_20261106 import policy, run as original
from g3_v23_adx_rsi_20261106.keep_entries_20261106.publish import verify_manifest
from g3_v23_adx_rsi_20261106.keep_entries_20261106.exit_study import run as exits
from g3_v23_adx_rsi_20261106.keep_entries_20261106.risk_study import run as risk
from .reporting import write_reports

PACKAGE = Path(__file__).resolve().parent
ROOT_PACKAGE = PACKAGE.parent
DAY = '2026-11-05'
ORIGINAL = ROOT_PACKAGE / 'results_through_20261104'
OLD_EXIT = ROOT_PACKAGE / 'keep_entries_20261106/exit_study/results_through_20261104'
OLD_RISK = ROOT_PACKAGE / 'keep_entries_20261106/risk_study/results_through_20261104'
OLD_CURRENT = ROOT_PACKAGE / 'keep_entries_20261106/current_results_through_20261104'
DAY_SOURCE = PACKAGE / 'source_20261105'
OUTPUT = PACKAGE / 'results_through_20261105'
IDENTITY = ['sid', 'day', 'setup_id', 'tradingsymbol', 'side', 'entry_ts']
ADDITIVE = ['selected_orders', 'trades', 'wins', 'losses', 'net_pnl_rs', 'gross_loss_rs', 'costs_rs']


def read_json(path):
    return json.loads(Path(path).read_text(encoding='utf-8'))


def assert_entries(base, candidate):
    if base.sid.duplicated().any() or candidate.sid.duplicated().any():
        raise ValueError('Duplicate entry identity')
    left, right = [frame.sort_values('sid').reset_index(drop=True) for frame in (base, candidate)]
    pd.testing.assert_frame_equal(left[IDENTITY], right[IDENTITY], check_dtype=False, check_exact=True)
    if not np.allclose(left.entry_price, right.entry_price, atol=1e-10, rtol=0):
        raise ValueError('Changed entry price beyond CSV round-trip precision')


def append_day(previous, latest):
    if not latest.day.eq(DAY).all() or previous.day.ge(DAY).any():
        raise ValueError('Append must contain only the new completed session')
    if set(previous.sid) & set(latest.sid):
        raise ValueError('New session reused a historical identity')
    joined = pd.concat([previous, latest], ignore_index=True, sort=False)
    if joined.sid.duplicated().any():
        raise ValueError('Duplicate appended identity')
    return joined


def replay_day(inputs, settings):
    frozen = inputs['trades']
    lookup = inputs['parity'].set_index('sid')
    if set(lookup.index) != set(frozen.sid):
        raise ValueError('New-session native parity inventory mismatch')
    book = frozen.copy()
    for index, row in frozen.iterrows():
        meta = lookup.loc[row.sid]
        path = inputs['paths'][int(row.sid)]
        exits.verify_path(path, int(meta.entry_index))
        result = exits.replay(path, int(meta.entry_index), float(row.entry_price), row.side == 'LONG',
                              float(meta.native_target_pct), bool(meta.weak_entry_adx_rsi), settings)
        if result['index'] > int(meta.exit_index):
            raise ValueError('Overlay delayed the native exit')
        unchanged = (result['index'] == int(meta.exit_index) and result['reason'] == row.exit_reason
                     and abs(result['price'] - row.exit_price) < 1e-8)
        pnl = float(row.portfolio_net_profit_rupees) if unchanged else (
            (1 if row.side == 'LONG' else -1) * (result['price'] / row.entry_price - 1) * 500000 - 250)
        if abs(pnl) < 1e-7:
            pnl = 0.
        book.loc[index, 'exit_ts'] = str(exits.exit_timestamp(path, result))
        book.loc[index, 'exit_price'] = result['price']
        book.loc[index, 'exit_reason'] = result['reason']
        book.loc[index, 'portfolio_net_profit_rupees'] = pnl
    assert_entries(frozen, book)
    if not settings:
        if not np.allclose(book.portfolio_net_profit_rupees, frozen.portfolio_net_profit_rupees, atol=1e-7, rtol=0):
            raise ValueError('New session baseline P&L parity failed')
        if not pd.to_datetime(book.exit_ts, utc=True).eq(pd.to_datetime(frozen.exit_ts, utc=True)).all():
            raise ValueError('New session baseline exit-time parity failed')
    return book


def summarize(books, audit, days):
    periods = {'full_66': days, 'early_59': [d for d in days if d <= risk.CUTOFF],
               'later_7': [d for d in days if d > risk.CUTOFF], 'new_day_20261105': [DAY],
               'previous_65': [d for d in days if d < DAY]}
    masks = policy.masks(audit)
    overall, daily = [], []
    for name, book in books.items():
        admitted = audit.loc[masks['adx20_di_rsi' if name == 'archived_entry_filter' else 'baseline_v21_g7']]
        for label, cohort in periods.items():
            metric = original.metrics(book, int(admitted.day.isin(cohort).sum()), cohort)
            metric['breakeven_trades'] = metric['trades'] - metric['wins'] - metric['losses']
            overall.append(dict(policy=name, period=label, **metric))
        for day in days:
            metric = original.metrics(book, int(admitted.day.eq(day).sum()), [day])
            metric['breakeven_trades'] = metric['trades'] - metric['wins'] - metric['losses']
            daily.append(dict(policy=name, day=day, **metric))
    overall, daily = pd.DataFrame(overall), pd.DataFrame(daily)
    for name in books:
        full = overall.loc[overall.policy.eq(name) & overall.period.eq('full_66')].iloc[0]
        part = daily.loc[daily.policy.eq(name)]
        for field in ADDITIVE + ['breakeven_trades']:
            if not np.isclose(full[field], part[field].sum(), atol=1e-5, rtol=0):
                raise ValueError('Daily reconciliation failed: ' + name + '/' + field)
    return overall, daily, periods


def verify_previous(overall):
    old = pd.read_csv(OLD_EXIT / 'overall.csv')
    old_filter = pd.read_csv(ORIGINAL / 'overall.csv')
    fields = ADDITIVE + ['daily_close_drawdown_rs', 'negative_days', 'win_rate_pct']
    for row in overall.loc[overall.period.eq('previous_65')].itertuples():
        source = old_filter if row.policy == 'archived_entry_filter' else old
        name = 'adx20_di_rsi' if row.policy == 'archived_entry_filter' else row.policy
        saved = source.loc[source.policy.eq(name) & source.period.eq('full_65')].iloc[0]
        for field in fields:
            if not np.isclose(getattr(row, field), saved[field], atol=1e-5, rtol=0):
                raise ValueError('Earlier65 result changed: ' + name + '/' + field)


def sizing_results(trades, audit, periods, output):
    config = read_json(OLD_RISK / 'config.json')
    if config['policies'] != risk.POLICIES:
        raise ValueError('Sizing hypotheses changed')
    books, overall, daily = [], [], []
    for name, scale in config['normalization_constants'].items():
        if not np.isclose(scale, risk.normalization(audit, name), atol=1e-12, rtol=0):
            raise ValueError('Earlier-order sizing normalization changed')
        book = risk.make_book(trades, audit, name, scale)
        assert_entries(trades, book)
        books.append(book)
        for label, cohort in periods.items():
            overall.append(dict(policy=name, period=label, **risk.metric(book, cohort)))
        for day in periods['full_66']:
            daily.append(dict(policy=name, day=day, **risk.metric(book, [day])))
    overall, daily = pd.DataFrame(overall), pd.DataFrame(daily)
    selected, status = risk.choose(overall)
    if selected != config['selected_policy']:
        raise ValueError('Appending Nov5 changed the earlier-only sizing decision')
    old = pd.read_csv(OLD_RISK / 'overall.csv')
    for row in overall.loc[overall.period.eq('previous_65')].itertuples():
        saved = old.loc[old.policy.eq(row.policy) & old.period.eq('full_65')].iloc[0]
        for field in ('trades', 'wins', 'losses', 'net_pnl_rs', 'gross_loss_rs', 'daily_close_drawdown_rs'):
            if not np.isclose(getattr(row, field), saved[field], atol=1e-5, rtol=0):
                raise ValueError('Earlier sizing result changed')
    for name in risk.POLICIES:
        full = overall.loc[overall.policy.eq(name) & overall.period.eq('full_66')].iloc[0]
        part = daily.loc[daily.policy.eq(name)]
        for field in ('trades', 'wins', 'losses', 'net_pnl_rs', 'gross_loss_rs', 'variable_costs_rs'):
            if not np.isclose(full[field], part[field].sum(), atol=1e-5, rtol=0):
                raise ValueError('Sizing daily reconciliation failed')
    overall.to_csv(output / 'sizing_overall.csv', index=False)
    daily.to_csv(output / 'sizing_daily.csv', index=False)
    pd.concat(books, ignore_index=True).to_csv(output / 'sizing_trades.csv', index=False)
    return dict(selected_sizing_policy=selected, sizing_selection=status,
                sizing_normalization_constants=config['normalization_constants'])


def run(output=OUTPUT):
    output = Path(output).resolve()
    if output.exists() or output == PACKAGE or not output.is_relative_to(PACKAGE):
        raise ValueError('Use a fresh output folder inside the Nov5 V23 extension')
    sources = [ORIGINAL, OLD_EXIT, OLD_RISK, OLD_CURRENT, DAY_SOURCE]
    for folder in sources:
        verify_manifest(folder)
        for name, expected in read_json(folder / 'provenance.json').get('code_sha256', {}).items():
            if original.sha(name) != expected:
                raise ValueError('Source builder hash changed: ' + name)
    old_config = read_json(OLD_EXIT / 'config.json')
    if old_config['candidate_policies'] != exits.CANDIDATES:
        raise ValueError('Fixed exit hypotheses changed')
    current = read_json(OLD_CURRENT / 'config.json')
    new_config = read_json(DAY_SOURCE / 'config.json')
    # This pickle is loaded only after its entire producing manifest is verified.
    inputs = pickle.loads((DAY_SOURCE / 'inputs.pkl').read_bytes())
    if inputs['days'] != [DAY]:
        raise ValueError('Expected exactly the completed Nov5 session')
    prior = pd.read_csv(ORIGINAL / 'frozen_inputs/v21_g7_native_trades.csv')
    baseline = append_day(prior, inputs['trades'])
    old_audit = pd.read_csv(ORIGINAL / 'frozen_inputs/v21_g7_adx_rsi_features.csv')
    latest_audit = pd.read_csv(DAY_SOURCE / 'admission_audit.csv')
    audit = append_day(old_audit, latest_audit)
    original.check_identities(audit, baseline)
    days = sorted([*pd.read_csv(OLD_EXIT / 'daily.csv').day.unique(), DAY])
    if len(days) != 66 or len(set(days)) != 66 or days[-2:] != ['2026-11-04', DAY]:
        raise ValueError('Completed session inventory changed')
    from g3_v18_20261102 import data
    if data.available_days(DAY) != days:
        raise ValueError('Some available completed source sessions are missing from the report')
    books = {}
    for name, settings in exits.CANDIDATES.items():
        older = pd.read_csv(OLD_EXIT / (name + '_trades.csv'))
        latest = replay_day(inputs, settings)
        books[name] = append_day(older, latest)
        assert_entries(baseline, books[name])
    old_filtered = pd.read_csv(ORIGINAL / 'v23_trades.csv')
    allowed = latest_audit.loc[policy.masks(latest_audit)['adx20_di_rsi'], 'sid']
    books['archived_entry_filter'] = append_day(old_filtered, inputs['trades'].loc[inputs['trades'].sid.isin(allowed)])
    overall, daily, periods = summarize(books, audit, days)
    verify_previous(overall)
    exit_only = overall.loc[overall.policy.isin(exits.CANDIDATES)]
    selected = exits.choose(exit_only)
    if selected != current['selected_exit_policy']:
        raise ValueError('Appending Nov5 changed the fixed earlier-only exit decision')
    output.mkdir(parents=True)
    overall.to_csv(output / 'overall.csv', index=False)
    daily.to_csv(output / 'daily.csv', index=False)
    audit.to_csv(output / 'admission_audit.csv', index=False)
    for name, book in books.items():
        book.to_csv(output / (name + '_trades.csv'), index=False)
    books[selected].to_csv(output / 'selected_trades.csv', index=False)
    inputs['parity'].to_csv(output / 'nov5_native_parity.csv', index=False)
    sizing = sizing_results(baseline, audit, periods, output)
    base_row = overall.loc[overall.policy.eq(exits.BASELINE) & overall.period.eq('full_66')].iloc[0]
    new_row = overall.loc[overall.policy.eq(exits.BASELINE) & overall.period.eq('new_day_20261105')].iloc[0]
    config = dict(version='G3-V23-KEEP-ALL-ENTRIES-THROUGH-20261105', scope='BACKTEST_ONLY',
        execution_authority=False, adx_rsi_entry_veto_enabled=False, live_changed=False,
        selected_exit_policy=selected, **sizing, fixed_exit_hypotheses=exits.CANDIDATES,
        selection_frozen_from=str(OLD_CURRENT), threshold_retuned=False, history_cutoff=DAY,
        training_cutoff=risk.CUTOFF, periods=periods, configured_pairs=73,
        original_pair_cap=3, new_pair_cap=1, new_pair_volume_increment=.10,
        signal_clock_limit='14:00', last_confirmation='14:01', execution_cutoff='15:15',
        per_trade_capital_rs=100000, per_trade_exposure_rs=500000, per_trade_cost_rs=250,
        unlimited_portfolio_capital=True, selected_orders=int(base_row.selected_orders),
        filled_trades=int(base_row.trades), new_day_selected_orders=int(new_row.selected_orders),
        new_day_filled_trades=int(new_row.trades), previous_65_parity='PASS_ALL_FIXED_POLICIES',
        same_entries_each_day='PASS_ALL_EXIT_AND_SIZING_POLICIES', retrospective_only=True,
        nov5_source_config=new_config,
        coverage_notes=['All 218 stock sources contain all 360 one-minute bars from 09:16 through the strategy exit cutoff 15:15 on 5 November.',
            '217 stocks are eligible in the sealed feature ledger. SAIL is excluded for the day because its 09:20 futures OI observation is missing.',
            'KAYNES is excluded only from 09:20 opening setups for missing opening OI evidence; later setups retain valid source data.',
            'All 73 setup/direction pairs through 14:00 are evaluated using the same sealed snapshot; all stock futures use November contracts.',
            'October 1 remains excluded from older history by the immutable source. No November 6 intraday data are included.',
            'Inherited earlier-source limits remain: some archived cohorts use after-dated membership/full-day eligibility; October 30 reconstructed opening OI does not prove contemporaneous availability.'])
    (output / 'config.json').write_text(json.dumps(config, indent=2, default=str) + '\n', encoding='utf-8')
    write_reports(output, overall, daily, config)
    validation = dict(previous_65_parity=True, all_day_trade_counts_preserved=True,
        total_native_parity=int(len(prior) + len(inputs['parity'])), new_day_native_parity=int(len(inputs['parity'])),
        daily_rows=len(daily), overall_rows=len(overall), all_sessions=days,
        source_manifests_verified=len(sources), earlier_selection_unchanged=True)
    (output / 'validation.json').write_text(json.dumps(validation, indent=2) + '\n')
    code = [Path(__file__), PACKAGE / 'reporting.py', Path(exits.__file__), Path(risk.__file__),
            Path(policy.__file__), Path(original.__file__)]
    provenance = dict(code_sha256={str(p): original.sha(p) for p in code},
        source_manifests={str(p): original.sha(p / 'manifest.json') for p in sources},
        append_only=True, previous_results_overwritten=False)
    (output / 'provenance.json').write_text(json.dumps(provenance, indent=2) + '\n')
    (output / 'manifest.json').write_text(json.dumps({p.name: original.sha(p) for p in output.iterdir() if p.is_file()}, indent=2) + '\n')
    print(overall.loc[overall.period.isin(['full_66', 'new_day_20261105']),
        ['period','policy','trades','wins','losses','net_pnl_rs','gross_loss_rs','daily_close_drawdown_rs']].to_string(index=False), flush=True)
    return config


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, default=OUTPUT)
    run(parser.parse_args().output)
