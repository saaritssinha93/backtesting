"""Evaluate seven frozen loss-management rules on all790 unchanged V23 fills."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd

from g3_v23_adx_rsi_20261106 import policy as entry_policy, run as original
from g3_v23_adx_rsi_20261106.keep_entries_20261106.exit_study import run as native
from g3_v23_adx_rsi_20261106.through_20261105.run import assert_entries
from . import engine, inputs
from .reporting import write_reports

PACKAGE = Path(__file__).resolve().parent
OUTPUT = PACKAGE / 'results_through_20261105'
BASELINE = engine.BASELINE
FIELDS = ('trades','wins','losses','selected_orders','net_pnl_rs','gross_loss_rs','costs_rs')


def choose(overall):
    early = overall.loc[overall.period.eq('early_59')]
    base = early.loc[early.policy.eq(BASELINE)].iloc[0]
    eligible = early.loc[early.policy.ne(BASELINE) & early.trades.eq(base.trades)
        & early.net_pnl_rs.ge(base.net_pnl_rs) & early.gross_loss_rs.lt(base.gross_loss_rs)
        & early.daily_close_drawdown_rs.le(base.daily_close_drawdown_rs)]
    order = ['net_pnl_rs', 'gross_loss_rs', 'daily_close_drawdown_rs', 'policy']
    ascending = [False, True, True, True]
    training = (BASELINE if eligible.empty else
                eligible.sort_values(order, ascending=ascending).iloc[0].policy)
    if training == BASELINE:
        exploratory = early.loc[early.policy.ne(BASELINE) & early.gross_loss_rs.lt(base.gross_loss_rs)]
        diagnostic = (BASELINE if exploratory.empty else
                      exploratory.sort_values(order, ascending=ascending).iloc[0].policy)
    else:
        diagnostic = training
    return training, diagnostic


def promotion(overall, candidate):
    if candidate == BASELINE:
        return False, ['No earlier59 candidate reduced losses while preserving net profit and drawdown.']
    reasons = []
    for period in ('full_66', 'later_7'):
        cohort = overall.loc[overall.period.eq(period)].set_index('policy')
        row, base = cohort.loc[candidate], cohort.loc[BASELINE]
        if row.trades != base.trades:
            reasons.append(period + ': trade count changed')
        if row.net_pnl_rs < base.net_pnl_rs:
            reasons.append(period + ': net profit below baseline')
        if row.gross_loss_rs > base.gross_loss_rs or (period == 'full_66' and row.gross_loss_rs == base.gross_loss_rs):
            reasons.append(period + ': loss reduction requirement failed')
        if row.daily_close_drawdown_rs > base.daily_close_drawdown_rs:
            reasons.append(period + ': daily-close drawdown worse than baseline')
    return not reasons, reasons


def metrics(book, selected, days):
    result = original.metrics(book, selected, days)
    result['breakeven_trades'] = result['trades'] - result['wins'] - result['losses']
    return result


def verified_baseline(data):
    trades, lookup = data['trades'], data['parity'].set_index('sid')
    features, parity = {}, []
    for row in trades.itertuples(index=False):
        meta, path = lookup.loc[row.sid], data['paths'][int(row.sid)]
        native.verify_path(path, int(meta.entry_index))
        # Features are built internally from the verified OHLC path.
        features[int(row.sid)] = engine.build_features(path)
        result = engine.replay(path, int(meta.entry_index), float(row.entry_price),
                               row.side == 'LONG', float(meta.native_target_pct), {})
        stamp = native.exit_timestamp(path, result)
        pnl = (1 if row.side == 'LONG' else -1) * (result['price'] / row.entry_price - 1) * 500000 - 250
        if (result['index'] != int(meta.exit_index) or result['reason'] != row.exit_reason
                or stamp != pd.Timestamp(row.exit_ts)
                or not np.isclose(result['price'], row.exit_price, atol=1e-7, rtol=0)
                or not np.isclose(pnl, row.portfolio_net_profit_rupees, atol=1e-5, rtol=0)):
            raise ValueError('Native baseline parity failed: ' + str(row.sid))
        parity.append(dict(sid=int(row.sid), day=row.day, native_parity=True,
                           price_delta_rs=result['price']-row.exit_price,
                           pnl_delta_rs=pnl-row.portfolio_net_profit_rupees))
    return features, pd.DataFrame(parity)


def replay_book(data, features, name, settings):
    frozen = data['trades']
    lookup = data['parity'].set_index('sid')
    book, changes = frozen.copy(), []
    for idx, row in frozen.iterrows():
        meta, path = lookup.loc[row.sid], data['paths'][int(row.sid)]
        result = engine.replay(path, int(meta.entry_index), float(row.entry_price), row.side == 'LONG',
                               float(meta.native_target_pct), settings, features[int(row.sid)])
        if result['index'] > int(meta.exit_index):
            raise ValueError('Loss rule delayed the native exit')
        stamp = native.exit_timestamp(path, result)
        unchanged = (result['index'] == int(meta.exit_index)
                     and stamp == pd.Timestamp(row.exit_ts)
                     and abs(result['price'] - row.exit_price) < 1e-8)
        pnl = float(row.portfolio_net_profit_rupees) if unchanged else (
            (1 if row.side == 'LONG' else -1) * (result['price'] / row.entry_price - 1) * 500000 - 250)
        if abs(pnl) < 1e-7:
            pnl = 0.
        book.loc[idx, 'exit_ts'] = str(stamp)
        book.loc[idx, 'exit_price'] = result['price']
        book.loc[idx, 'exit_reason'] = result['reason']
        book.loc[idx, 'portfolio_net_profit_rupees'] = pnl
        before = float(row.portfolio_net_profit_rupees)
        changes.append(dict(policy=name, sid=int(row.sid), day=row.day,
            baseline_pnl_rs=before, candidate_pnl_rs=pnl, pnl_change_rs=pnl-before,
            exit_changed=not unchanged, baseline_winner_harmed=before>0 and pnl<before-1e-7,
            baseline_loser_improved=before<0 and pnl>before+1e-7,
            winner_turned_loss=before>0 and pnl<0, loser_turned_win=before<0 and pnl>0,
            baseline_exit_ts=row.exit_ts, candidate_exit_ts=str(stamp),
            exit_reason=result['reason'], same_bar_ambiguous=result['ambiguous']))
    assert_entries(frozen, book)
    return book, pd.DataFrame(changes)


def cost_stress(books, changes, periods):
    rows = []
    for name, book in books.items():
        changed = changes.loc[changes.policy.eq(name)].set_index('sid').exit_changed
        for bps in (0, 1, 2, 5):
            test = book.copy()
            extra = test.sid.map(changed).astype(float) * 500000 * bps / 10000
            test['portfolio_net_profit_rupees'] -= extra
            for period in ('full_66', 'early_59', 'later_7'):
                metric = metrics(test, 0, periods[period])
                rows.append(dict(policy=name, period=period, incremental_slippage_bps=bps,
                    extra_slippage_rs=float(extra.loc[test.day.isin(periods[period])].sum()),
                    **{key:metric[key] for key in ('trades','wins','losses','net_pnl_rs','gross_loss_rs','daily_close_drawdown_rs')}))
    return pd.DataFrame(rows)


def run(output=OUTPUT):
    output = Path(output).resolve()
    if output.exists() or output == PACKAGE or not output.is_relative_to(PACKAGE):
        raise ValueError('Use a fresh result directory inside isolated loss_control_20261106')
    protocol = json.loads((PACKAGE / 'protocol.json').read_text())
    if set(protocol['definitions']) != set(engine.CANDIDATES) or len(engine.CANDIDATES) != 8:
        raise ValueError('Frozen hypothesis inventory changed')
    data = inputs.load()
    days, audit = data['days'], data['audit']
    counts = audit.loc[entry_policy.flag(audit.all_g7_allowed)].groupby('day').size().to_dict()
    if sum(counts.values()) != 891:
        raise ValueError('Baseline admission count changed')
    periods = dict(full_66=days, early_59=[d for d in days if d<=protocol['training_cutoff']],
        later_7=[d for d in days if d>protocol['training_cutoff']], nov05=['2026-11-05'],
        block_first22=days[:22], block_middle22=days[22:44], block_last22=days[44:])
    for month in sorted(set(day[:7] for day in days)):
        periods['month_'+month] = [d for d in days if d.startswith(month)]
    features, parity = verified_baseline(data)
    print('PASS: all790 native baseline exits reproduced; running7 fixed hypotheses.', flush=True)
    output.mkdir(parents=True)
    (output / 'predeclared_plan.json').write_text(json.dumps(dict(protocol=protocol,
        candidates=engine.CANDIDATES, protocol_sha256=original.sha(PACKAGE/'protocol.json'),
        engine_sha256=original.sha(engine.__file__)), indent=2) + '\n')
    books, changes, overall, daily = {}, [], [], []
    for name, settings in engine.CANDIDATES.items():
        book, differences = replay_book(data, features, name, settings)
        books[name] = book
        changes.append(differences)
        for period, cohort in periods.items():
            overall.append(dict(policy=name,period=period,**metrics(book,sum(counts.get(d,0) for d in cohort),cohort)))
        for day in days:
            daily.append(dict(policy=name,day=day,**metrics(book,counts.get(day,0),[day])))
        print('Completed fixed policy: ' + name, flush=True)
    overall, daily, changes = pd.DataFrame(overall), pd.DataFrame(daily), pd.concat(changes,ignore_index=True)
    for name in books:
        total = overall.loc[overall.policy.eq(name) & overall.period.eq('full_66')].iloc[0]
        parts = daily.loc[daily.policy.eq(name)]
        if len(parts)!=66:
            raise ValueError('Missing daily row')
        for field in FIELDS:
            if not np.isclose(total[field],parts[field].sum(),atol=1e-5,rtol=0):
                raise ValueError('Daily/full discrepancy: '+name+'/'+field)
    baseline = overall.loc[overall.policy.eq(BASELINE) & overall.period.eq('full_66')].iloc[0]
    if baseline.trades!=790 or not np.isclose(baseline.net_pnl_rs,1177522.0952653617,atol=1e-5,rtol=0):
        raise ValueError('Published baseline does not match previous V23')
    training, diagnostic = choose(overall)
    promoted, reasons = promotion(overall,training)
    selected = training if promoted else BASELINE
    config = dict(scope='BACKTEST_ONLY',execution_authority=False,live_changed=False,
        selected_policy=selected,selected_sizing_policy='baseline_fixed_size',training_candidate=training,
        comparison_candidate=diagnostic,promotion_pass=promoted,rejection_reasons=reasons,
        definitions=protocol['definitions'],historydays=days,history_through=days[-1],
        training_cutoff=protocol['training_cutoff'],policy_settings=engine.CANDIDATES[selected],
        selectionsummary=protocol['selection'],promotion_rules=protocol['promotion'],
        entry_veto_enabled=False,selected_orders=891,filled_trades=790,
        native_parity_count=len(parity),exposure_per_trade_rs=500000,cost_per_trade_rs=250,
        total_cost_rs=197500,unlimited_concurrent_capital=True,retrospective_only=True,
        coverage_notes=data['baseline_config']['coverage_notes'],
        statistical_limitation='Seven new fixed hypotheses on previously observed history; repeated research can overfit. No independent holdout or future profit guarantee.',
        feature_definition=protocol['feature_definition'])
    overall.to_csv(output/'overall.csv',index=False)
    daily.to_csv(output/'daily.csv',index=False)
    changes.to_csv(output/'trade_changes.csv',index=False)
    parity.to_csv(output/'baseline_parity.csv',index=False)
    stress=cost_stress(books,changes,periods)
    stress.to_csv(output/'cost_stress.csv',index=False)
    for name,book in books.items():
        book.to_csv(output/(name+'_trades.csv'),index=False)
    books[selected].to_csv(output/'selected_trades.csv',index=False)
    (output/'config.json').write_text(json.dumps(config,indent=2)+'\n')
    write_reports(output,overall,daily,changes,config)
    validation=dict(native_replay_parity=len(parity),all_policy_entries_preserved=790,
        all_policy_days=66,unchanged_cost_rs=197500,selection_period='early_59',
        promotion_periods=['full_66','later_7'],changed_entries=0)
    (output/'validation.json').write_text(json.dumps(validation,indent=2)+'\n')
    source_files=[Path(__file__),PACKAGE/'engine.py',PACKAGE/'inputs.py',PACKAGE/'reporting.py',PACKAGE/'protocol.json']
    provenance=dict(source_manifests=data['source_manifests'],code_sha256={str(p):original.sha(p) for p in source_files})
    (output/'provenance.json').write_text(json.dumps(provenance,indent=2)+'\n')
    (output/'manifest.json').write_text(json.dumps({p.name:original.sha(p) for p in output.iterdir() if p.is_file()},indent=2)+'\n')
    print(overall.loc[overall.period.isin(['full_66','early_59','later_7']),
        ['period','policy','trades','wins','losses','net_pnl_rs','gross_loss_rs','daily_close_drawdown_rs']].to_string(index=False),flush=True)
    print('TRAINING='+training+' CURRENT='+selected+' PROMOTED='+str(promoted),flush=True)
    return config


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output',type=Path,default=OUTPUT)
    run(parser.parse_args().output)
