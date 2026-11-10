"""Replay fixed native entries with five predeclared causal exit overlays.

Run from repository root with python -m
g3_v23_adx_rsi_20261106.keep_entries_20261106.exit_study.run
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import pickle

import numpy as np
import pandas as pd

from g3_v23_adx_rsi_20261106 import policy as entry_policy
from g3_v23_adx_rsi_20261106.run import (
    ROOT, G7_SOURCE, ADX_SOURCE, sha, verify_source, check_identities, metrics, table)

PACKAGE = Path(__file__).resolve().parent
MINUTE_NS = 60_000_000_000
BASELINE = 'baseline_v21_g7'
# Fixed before outcome computation. No parameter search or later-period selection.
CANDIDATES = {
    BASELINE: {},
    'weak_stop_1p0': {'weak_stop_pct': 1.0},
    'weak_loss20m_0p35': {'weak_loss_minutes': 20, 'weak_loss_pct': .35},
    'protect_mfe0p4_lock0p05': {'protect_mfe_pct': .4, 'protect_lock_pct': .05},
    'protect_mfe0p6_lock0p1': {'protect_mfe_pct': .6, 'protect_lock_pct': .1},
    'weak_stop1p0_protect0p6': {
        'weak_stop_pct': 1.0, 'protect_mfe_pct': .6, 'protect_lock_pct': .1},
}
SELECTION = {
    'training_cutoff': '2026-10-27', 'period': 'early_59',
    'all_entry_identities_and_notional_retained': True,
    'require_net_pnl_at_least_baseline': True,
    'require_strictly_lower_gross_loss': True,
    'require_strictly_lower_daily_close_drawdown': True,
    'ranking': ['net_pnl_rs DESC', 'daily_close_drawdown_rs ASC', 'policy ASC'],
    'fallback': BASELINE,
    'later_6': 'Retrospective diagnostic only; never used to select parameters or policy',
}


def replay(path, entry_index, entry, is_long, target_pct, weak, settings):
    """Same stop-first native model; observations affect the following bar only.

    Entry-bar extrema are never used to activate profit protection, because
    the entry might have occurred after that extreme. Exposure and entry fill
    remain fixed. A weak-entry loss close becomes a market exit at next open.
    """
    sign = 1 if is_long else -1
    target = entry * (1 + sign * target_pct / 100)
    entry_end = int(path['timestamp_ns'][entry_index])
    protection_return = -np.inf
    pending_market = False
    for j in range(entry_index, len(path['close'])):
        at = int(path['timestamp_ns'][j])
        native_stop = 1.0 if at - MINUTE_NS >= entry_end + 120 * MINUTE_NS else 1.25
        stop_return = -native_stop
        reason = 'TIGHTENED_STOP' if native_stop < 1.25 else 'STOP'
        if weak and j > entry_index and settings.get('weak_stop_pct', 1.25) < native_stop:
            stop_return = -settings['weak_stop_pct']
            reason = 'WEAK_ENTRY_STOP'
        if protection_return > stop_return:
            stop_return = protection_return
            reason = 'PROFIT_PROTECTION'
        stop = entry * (1 + sign * stop_return / 100)
        op, hi, lo, close = [float(path[key][j]) for key in ('open', 'high', 'low', 'close')]
        stop_open = op <= stop if is_long else op >= stop
        stop_hit = lo <= stop if is_long else hi >= stop
        target_hit = hi >= target if is_long else lo <= target
        # A scheduled exit is known before this open. Only a stop gap can
        # precede it; later intrabar extrema cannot override its open fill.
        if pending_market and j > entry_index:
            price = op
            why = reason if stop_open else 'WEAK_ENTRY_LOSS_TIME_EXIT'
            return dict(index=j, price=price, reason=why, event='OPEN', ambiguous=False)
        if stop_hit:
            open_fill = j > entry_index and stop_open
            return dict(index=j, price=op if open_fill else stop, reason=reason,
                        event='OPEN' if open_fill else 'INTRABAR',
                        ambiguous=bool(target_hit and not open_fill))
        if target_hit:
            return dict(index=j, price=target, reason='TARGET', event='INTRABAR', ambiguous=False)
        # All following decisions use only a completed bar. They activate on
        # the next loop iteration, never on the bar that established them.
        if j > entry_index and 'protect_mfe_pct' in settings:
            favorable = hi if is_long else lo
            achieved = sign * (favorable / entry - 1) * 100
            if achieved + 1e-10 >= settings['protect_mfe_pct']:
                protection_return = max(protection_return, settings['protect_lock_pct'])
        if weak and 'weak_loss_minutes' in settings:
            elapsed = (at - entry_end) / MINUTE_NS
            close_return = sign * (close / entry - 1) * 100
            if elapsed >= settings['weak_loss_minutes'] and close_return <= -settings['weak_loss_pct']:
                pending_market = True
    return dict(index=len(path['close']) - 1, price=float(path['close'][-1]),
                reason='TIME_EXIT_1515', event='CLOSE', ambiguous=False)


def exit_timestamp(path, result):
    stamp = pd.Timestamp(int(path['timestamp_ns'][result['index']]), tz='UTC').tz_convert('Asia/Kolkata')
    return stamp - pd.Timedelta(minutes=1) if result['event'] == 'OPEN' else stamp


def choose(overall):
    early = overall.loc[overall.period.eq('early_59')]
    base = early.loc[early.policy.eq(BASELINE)].iloc[0]
    eligible = early.loc[
        early.policy.ne(BASELINE) & early.trades.eq(base.trades)
        & early.net_pnl_rs.ge(base.net_pnl_rs)
        & early.gross_loss_rs.lt(base.gross_loss_rs)
        & early.daily_close_drawdown_rs.lt(base.daily_close_drawdown_rs)]
    if eligible.empty:
        return BASELINE
    return eligible.sort_values(['net_pnl_rs', 'daily_close_drawdown_rs', 'policy'],
                                 ascending=[False, True, True]).iloc[0].policy


def apply_admission_counts(overall, daily):
    """Distinguish preserved G7 order admissions from actual native fills."""
    audit = pd.read_csv(ADX_SOURCE / 'admission_audit.csv')
    counts = audit.loc[entry_policy.flag(audit.all_g7_allowed)].groupby('day').size()
    if int(counts.sum()) != 871:
        raise ValueError('Frozen G7 admitted-order inventory changed')
    for frame in (overall, daily):
        frame['breakeven_trades'] = frame.trades - frame.wins - frame.losses
    daily['selected_orders'] = daily.day.map(counts).fillna(0).astype(int)
    for period in overall.period.unique():
        days = sorted(daily.day.unique())
        if period == 'early_59':
            days = [day for day in days if day <= SELECTION['training_cutoff']]
        elif period == 'later_6':
            days = [day for day in days if day > SELECTION['training_cutoff']]
        overall.loc[overall.period.eq(period), 'selected_orders'] = int(counts.reindex(days, fill_value=0).sum())


def verify_path(path, entry_index):
    ts = path['timestamp_ns']
    if not len(ts) or not 0 <= entry_index < len(ts):
        raise ValueError('Missing path or invalid native entry index')
    if not np.all(np.diff(ts) == MINUTE_NS):
        raise ValueError('Non-contiguous execution path')
    prices = np.array([path[k] for k in ('open', 'high', 'low', 'close')])
    if not np.isfinite(prices).all() or not (prices > 0).all():
        raise ValueError('Non-finite or nonpositive execution price')


def build_inputs(output):
    from g3_v18_20261102 import data
    from g3_v21_expanded_20261104.refinement_20261104 import run as native_source
    from g3_v21_expanded_20261104.refinement_20261104.g7_v4_confirmations_20261104.latest_day import build_expanded_day
    import fno_v13_v10_g_2_backtest as native_simulator

    proofs = {'result_manifests': {str(p.relative_to(ROOT)): verify_source(p) for p in (G7_SOURCE, ADX_SOURCE)}}
    audit = pd.read_csv(ADX_SOURCE / 'admission_audit.csv')
    trades = pd.read_csv(G7_SOURCE / 'trades.csv')
    check_identities(audit, trades)
    if len(trades) != 771:
        raise ValueError('Frozen baseline must contain exactly 771 native fills')
    days = sorted(pd.read_csv(G7_SOURCE / 'daily.csv').day.unique())
    if len(days) != 65:
        raise ValueError('Expected all 65 frozen baseline sessions')
    mask = entry_policy.masks(audit)['adx20_di_rsi']
    weak = dict(zip(audit.sid, ~mask))
    expected = json.loads((ADX_SOURCE / 'provenance.json').read_text())['checkpoint_sha256']
    for name, digest in expected.items():
        if sha(native_source.CHECKPOINTS / name) != digest:
            raise ValueError('Native checkpoint hash mismatch: ' + name)
    proofs['checkpoint_sha256'] = expected
    _, native, _, _ = native_source._load_books()
    print('Verified 64 checkpoint hashes. Building sealed Nov4 native book...', flush=True)
    latest, latest_proof = build_expanded_day()
    native = pd.concat([native, latest], ignore_index=True, sort=False)
    native = native.loc[native.sid.isin(trades.sid)].copy()
    native.day = native.day.astype(str)
    if len(native) != 771 or not native.sid.is_unique or set(native.sid) != set(trades.sid):
        raise ValueError('Native book does not cover every frozen fill exactly once')
    native = native.set_index('sid', drop=False)
    proofs['latest_native_builder'] = latest_proof
    proofs['days'] = {}
    paths, parity = {}, []
    for day in days:
        _, minutes, source_proof = data.load_day(day)
        orders = native.loc[native.day.eq(day)]
        today_paths = data.execution_paths(orders, minutes)
        proofs['days'][day] = source_proof
        for frozen in trades.loc[trades.day.eq(day)].itertuples(index=False):
            row = native.loc[frozen.sid]
            for key in ('day', 'setup_id', 'tradingsymbol', 'side'):
                if str(row[key]) != str(getattr(frozen, key)):
                    raise ValueError('Native identity mismatch: ' + key)
            if pd.Timestamp(row.entry_ts) != pd.Timestamp(frozen.entry_ts) or not np.isclose(row.entry_price, frozen.entry_price, atol=1e-9, rtol=0):
                raise ValueError('Native entry fill changed')
            if not bool(row.filled):
                raise ValueError('Baseline identity is not a native fill')
            path = today_paths[int(frozen.sid)]
            entry_index = int(row.entry_path_index)
            verify_path(path, entry_index)
            args = (path, entry_index, float(frozen.entry_price), frozen.side == 'LONG', float(row.native_target_pct))
            original = native_simulator.staged_exit(*args)
            actual = replay(*args, weak=bool(weak[frozen.sid]), settings={})
            if (actual['index'], actual['reason'], actual['event']) != (original[0], original[2], original[4]) or not np.isclose(actual['price'], original[1], atol=1e-9, rtol=0):
                raise ValueError('Native engine parity failure')
            timestamp = exit_timestamp(path, actual)
            pnl = (1 if frozen.side == 'LONG' else -1) * (actual['price'] / frozen.entry_price - 1) * 500000 - 250
            if timestamp != pd.Timestamp(frozen.exit_ts) or actual['reason'] != frozen.exit_reason or not np.isclose(actual['price'], frozen.exit_price, atol=1e-7, rtol=0) or not np.isclose(pnl, frozen.portfolio_net_profit_rupees, atol=.00001, rtol=0):
                raise ValueError(f'Frozen exit parity failure for {frozen.sid}: {actual} {timestamp} {pnl}')
            parity.append(dict(sid=int(frozen.sid), day=day, native_replay_match=True,
                               exit_index=actual['index'], entry_index=entry_index,
                               native_target_pct=float(row.native_target_pct),
                               weak_entry_adx_rsi=bool(weak[frozen.sid]),
                               baseline_ambiguous=actual['ambiguous']))
            paths[int(frozen.sid)] = path
        print(f'Parity {day}: {len(parity)}/771 exact native fills', flush=True)
    parity = pd.DataFrame(parity)
    if len(parity) != len(trades):
        raise ValueError('Silent entry loss detected')
    inputs = dict(trades=trades, parity=parity, paths=paths, days=days)
    source_code = [Path(__file__), ROOT / 'fno_v13_v10_g_2_backtest.py',
                   ROOT / 'g3_v18_20261102/data.py', ROOT / 'g3_v23_adx_rsi_20261106/policy.py',
                   ROOT / 'g3_v23_adx_rsi_20261106/run.py']
    proofs['code_sha256'] = {str(p): sha(p) for p in source_code}
    (output / 'verified_inputs.pkl').write_bytes(pickle.dumps(inputs, protocol=5))
    proofs['verified_inputs_sha256'] = sha(output / 'verified_inputs.pkl')
    (output / 'provenance.json').write_text(json.dumps(proofs, indent=2, default=str), encoding='utf-8')
    parity.to_csv(output / 'baseline_parity.csv', index=False)
    return inputs


def run(output):
    output = Path(output).resolve()
    if output.exists() or not output.is_relative_to(PACKAGE) or output == PACKAGE:
        raise ValueError('Output must be a fresh directory inside isolated exit_study')
    output.mkdir(parents=True)
    # Persist hypotheses/selection before reading any outcome or running an arm.
    plan = dict(scope='BACKTEST_ONLY', execution_authority=False,
                baseline_policy=BASELINE, candidate_policies=CANDIDATES, selection=SELECTION)
    (output / 'predeclared_plan.json').write_text(json.dumps(plan, indent=2), encoding='utf-8')
    inputs = build_inputs(output)
    trades, parity, paths, days = [inputs[k] for k in ('trades', 'parity', 'paths', 'days')]
    lookup = parity.set_index('sid')
    periods = dict(full_65=days, early_59=[d for d in days if d <= SELECTION['training_cutoff']],
                   later_6=[d for d in days if d > SELECTION['training_cutoff']])
    books, overall, daily, changes = {}, [], [], []
    for name, settings in CANDIDATES.items():
        fills = trades.copy()
        for idx, frozen in trades.iterrows():
            sid = int(frozen.sid)
            meta = lookup.loc[sid]
            path = paths[sid]
            result = replay(path, int(meta.entry_index), float(frozen.entry_price), frozen.side == 'LONG',
                            float(meta.native_target_pct), bool(meta.weak_entry_adx_rsi), settings)
            if result['index'] > int(meta.exit_index):
                raise ValueError('Exit overlay delayed the native exit')
            unchanged = (result['index'] == int(meta.exit_index) and result['reason'] == frozen.exit_reason
                         and abs(result['price'] - frozen.exit_price) < 1e-8)
            pnl = float(frozen.portfolio_net_profit_rupees) if unchanged else (
                (1 if frozen.side == 'LONG' else -1) * (result['price'] / frozen.entry_price - 1) * 500000 - 250)
            if abs(pnl) < 1e-7:
                pnl = 0.0
            fills.loc[idx, 'exit_ts'] = str(exit_timestamp(path, result))
            fills.loc[idx, 'exit_price'] = result['price']
            fills.loc[idx, 'exit_reason'] = result['reason']
            fills.loc[idx, 'portfolio_net_profit_rupees'] = pnl
            changes.append(dict(policy=name, sid=sid, day=frozen.day, weak_entry_adx_rsi=bool(meta.weak_entry_adx_rsi),
                                exit_changed=not unchanged, baseline_pnl_rs=frozen.portfolio_net_profit_rupees,
                                overlay_pnl_rs=pnl, pnl_change_rs=pnl-frozen.portfolio_net_profit_rupees,
                                same_bar_stop_target_ambiguous=result['ambiguous']))
        identity = ['sid', 'day', 'setup_id', 'tradingsymbol', 'side', 'entry_ts', 'entry_price']
        pd.testing.assert_frame_equal(fills[identity], trades[identity], check_exact=True)
        if len(fills) != 771:
            raise ValueError('Candidate changed entry count')
        books[name] = fills
        for period, session_days in periods.items():
            overall.append(dict(policy=name, period=period, **metrics(fills, len(fills.loc[fills.day.isin(session_days)]), session_days)))
        for day in days:
            daily.append(dict(policy=name, day=day, **metrics(fills, int(fills.day.eq(day).sum()), [day])))
    overall, daily, changes = pd.DataFrame(overall), pd.DataFrame(daily), pd.DataFrame(changes)
    apply_admission_counts(overall, daily)
    selected = choose(overall)
    for name in CANDIDATES:
        summed = daily.loc[daily.policy.eq(name)]
        total = overall.loc[overall.policy.eq(name) & overall.period.eq('full_65')].iloc[0]
        for field in ('trades', 'wins', 'losses', 'net_pnl_rs', 'gross_loss_rs'):
            if not np.isclose(summed[field].sum(), total[field], atol=.00001, rtol=0):
                raise ValueError('Daily reconciliation failed: ' + name + ':' + field)
    overall.to_csv(output / 'overall.csv', index=False)
    daily.to_csv(output / 'daily.csv', index=False)
    changes.to_csv(output / 'all_candidate_trade_changes.csv', index=False)
    books[selected].to_csv(output / 'selected_trades.csv', index=False)
    for name, fills in books.items():
        fills.to_csv(output / (name + '_trades.csv'), index=False)
    config = dict(plan, selected_policy=selected, selected_settings=CANDIDATES[selected],
                  baseline_entry_count=771, baseline_admitted_order_count=871,
                  all_entries_exact=True, native_exit_parity_count=771,
                  full_history_breakeven_trades={r.policy: int(r.breakeven_trades)
                      for r in overall.loc[overall.period.eq('full_65')].itertuples()},
                  per_trade_capital_rs=100000, per_trade_exposure_rs=500000, per_trade_cost_rs=250,
                  total_cost_rs=192750, capital_assumption='Unlimited concurrent capital; frozen native entry fills',
                  weak_entry_definition='Original adx20_di_rsi mask fails; features available at confirmation',
                  stops_effective='Next bar after completed observation; no entry-bar extrema activate protection',
                  baseline_time_exit='15:15; original per-pair targets and staged 1.25% to 1% stop preserved',
                  selection_scope='Earlier59 only; all periods historically observed, not a prospective holdout')
    (output / 'config.json').write_text(json.dumps(config, indent=2), encoding='utf-8')
    lines = ['# V23 fixed-entry exit study', '',
             f'Selected policy: **{selected}**. Every arm retains all **771** native entry identities, entry timestamps and prices.', '',
             'All 771 exits were first reproduced exactly using the original staged minute engine and hash-verified historical inputs. No missing source or failed replay is silently dropped.', '',
             'Five fixed causal overlays were declared before outcome computation. Selection uses only the first 59 sessions: same trade count, net P&L at least baseline, strictly lower gross losses and daily-close drawdown. If no arm qualifies, baseline remains selected.', '',
             '## Full 65 sessions', ''] + table(overall.loc[overall.period.eq('full_65')])
    for period in ('early_59', 'later_6'):
        lines += ['', '## ' + period, ''] + table(overall.loc[overall.period.eq(period)])
    lines += ['', '## Execution assumptions and limitations', '',
              '- Fixed Rs100,000 capital / Rs500,000 exposure per fill; Rs250 round-trip cost per trade; Rs192,750 total costs in every arm. Capital is unlimited, so earlier exits do not cause extra entries or reranking.',
              f'- protect_mfe0p4_lock0p05 has {config["full_history_breakeven_trades"]["protect_mfe0p4_lock0p05"]} flat trades after costs. These are neither wins nor losses; win rate uses all 771 trades as its denominator.',
              '- The weak-entry flag uses completed ADX/DI/RSI available at confirmation. No trade outcome, future label, or later session selects individual trades.',
              '- A new weak-entry stop first applies after the entry bar. Protection observes extrema only after a post-entry bar completes, becoming effective next bar. A weak loss timer exits at the following open.',
              '- Original target and 15:15 close exits remain. Native intrabar stop-first handling is conservative when stop and target both touch; gaps through an active stop fill at the adverse open. A pre-scheduled market exit fills at open before subsequent intrabar extremes.',
              '- One-minute OHLC does not reveal the intrabar path or actual bid/ask fills. The native entry-bar stop assumption is retained. Protection can cut future winners, and a historical improvement does not guarantee lower losses tomorrow.',
              '- The six later sessions are retrospective diagnostics, not untouched prospective validation. October 1 remains excluded by the source history because its immutable data are incomplete.',
              '- Inherited source limitations remain: earlier archived cohorts include after-dated membership and full-day eligibility; October30 uses reconstructed source/opening OI that does not prove contemporaneous live availability.',
              '- daily.csv contains every candidate and all 65 days; selected_trades.csv keeps exact baseline entries and the selected exit outcomes. Source proofs and native replay parity are saved alongside.', '']
    (output / 'REPORT.md').write_text('\n'.join(lines), encoding='utf-8')
    manifest = {p.name: sha(p) for p in output.iterdir() if p.is_file() and p.name != 'manifest.json'}
    (output / 'manifest.json').write_text(json.dumps(manifest, indent=2), encoding='utf-8')
    print(overall.loc[overall.period.eq('full_65'), ['policy','trades','wins','losses','win_rate_pct','net_pnl_rs','gross_loss_rs','daily_close_drawdown_rs']].to_string(index=False), flush=True)
    print('SELECTED: ' + selected, flush=True)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--output', type=Path, default=PACKAGE / 'results_through_20261104')
    run(parser.parse_args().output)
