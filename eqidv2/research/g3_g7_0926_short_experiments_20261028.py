"""Honest, research-only 09:26 SHORT experiments for G3 and G7 V4 lock2.

The experiment grid and development-only ranking are enumerated in this file. Entry
threshold experiments are post-selection vetoes with no quota replacement.
All exit decisions use completed closes and become effective next minute.
"""
from __future__ import annotations

from datetime import date
import hashlib
import json
import math
from pathlib import Path

import numpy as np
import pandas as pd

from eqidv2_runtime_paths import RUNTIME_ROOT
from research import g3_0920_backtest as g3_source
from research.g3_0920_backtest import allocate as allocate_g3
from research.g3_sl_target_simulator import simulate_exit
from research.g7_entry_quality_20261026 import verify_members
from research.g7_v4_practical_research import BASE, execute as execute_g7


SETUP = '0926_SHORT'
G3_ROOT = Path('C:/TradingData/eqidv2/backtesting_result_v13_v10_g_3')
G3_RUN = G3_ROOT / 'backtests/runs/2026-10-27/20261027T172043456343_3d50ca01_current_full_history'
G3_MANIFEST_SHA256 = '90272456c0e6e8b75ac7fae8bac24be7e3dd83f635680814e49219b4e38759a6'
G7_RUN = Path('C:/TradingData/eqidv2/backtesting_result_v13_v10_g_7/v4_research/practical_research/runs/2026-10-27/20261027T185748273567_five_fixed_tests')
G7_MANIFEST_SHA256 = 'daa7440e6c2e0a8d60a6bc89841da802d59639aa1c2f43f78a371622ca43f56b'
OUT = Path(__file__).resolve().parents[1] / 'outputs/g3_g7_0926_short_iterative_20261028'
COSTS = (5., 9., 15., 30.)
STRESS_VARIANTS = ('baseline', 'protect_0p4_0p1', 'protect_0p2_0p15')
POINT_ESTIMATE_VARIANT = 'protect_0p2_0p15'

# name, protection activation, protected gross profit, entry-veto, complexity
VARIANTS = (
    ('baseline', None, None, None, 0),
    ('protect_0p6_0p1', .6, .1, None, 1),
    ('protect_0p5_0p1', .5, .1, None, 1),
    ('protect_0p4_0p1', .4, .1, None, 1),
    ('protect_0p3_0p1', .3, .1, None, 1),
    ('protect_0p2_0p1', .2, .1, None, 1),
    ('protect_0p2_0p15', .2, .15, None, 1),
    ('price_magnitude_min_0p30', None, None, 'price_magnitude_min_0p30', 1),
    ('oi_min_0p10', None, None, 'oi_min_0p10', 1),
    ('volume_min_2p50', None, None, 'volume_min_2p50', 1),
    ('body_min_0p60', None, None, 'body_min_0p60', 1),
    ('wick_max_0p40', None, None, 'wick_max_0p40', 1),
    ('nifty_open_max_minus_0p15', None, None, 'nifty_open_max_minus_0p15', 1),
    ('protect_0p4_0p1_price_min', .4, .1, 'price_magnitude_min_0p30', 2),
    ('protect_0p4_0p1_oi_min', .4, .1, 'oi_min_0p10', 2),
    ('protect_0p4_0p1_volume_min', .4, .1, 'volume_min_2p50', 2),
    ('protect_0p4_0p1_body_min', .4, .1, 'body_min_0p60', 2),
    ('protect_0p4_0p1_wick_max', .4, .1, 'wick_max_0p40', 2),
    ('protect_0p4_0p1_nifty_max', .4, .1, 'nifty_open_max_minus_0p15', 2),
)


def sha(path: Path) -> str:
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def build_g3_paths(rows: pd.DataFrame) -> dict:
    paths = {}
    for day_value, group in rows.groupby('day', sort=True):
        session = date.fromisoformat(str(day_value))
        minutes = {symbol: g3_source._path_for_symbol_day(
            symbol, session, Path(RUNTIME_ROOT))[0]
            for symbol in sorted(group.tradingsymbol.unique())}
        paths.update(g3_source.source.selected_paths(group, minutes, session))
    if set(paths) != set(rows.sid.astype(int)):
        raise ValueError('G3 path inventory differs from selected setup rows')
    g3_source.simulator.g.v9.validate_paths(rows, paths)
    return paths


def load_inputs():
    g3_dir = G3_RUN
    verify_members(g3_dir, G3_MANIFEST_SHA256)
    verify_members(G7_RUN, G7_MANIFEST_SHA256)
    g3 = pd.read_csv(g3_dir / 'portfolio_trades.csv')
    g7 = pd.read_csv(G7_RUN / 'loss_lock2/portfolio_trades.csv')
    summary = json.loads((G7_RUN / 'summary.json').read_text())
    cache = Path(summary['cache'])
    verify_members(cache, summary['cache_manifest_sha256'])
    native, _, _, g7_paths, proof = BASE.load_cache(cache)
    g3_rows = g3.loc[g3.setup_id.eq(SETUP)].copy()
    g7_rows = g7.loc[g7.setup_id.eq(SETUP)].copy()
    native_rows = native.loc[native.setup_id.eq(SETUP)]
    if (len(g3_rows) != 22 or len(g7_rows) != 22 or len(native_rows) != 22
            or set(g7_rows.sid) != set(native_rows.sid)):
        raise ValueError('Unexpected 09:26 SHORT source inventory')
    return dict(g3=g3, g7=g7, g3_rows=g3_rows, g7_rows=g7_rows,
                g3_paths=build_g3_paths(g3_rows), g7_paths=g7_paths,
                proof=proof, g3_dir=g3_dir, cache=cache)


def replay_exit(row, path, activation, floor):
    if not bool(row.filled):
        return None
    return simulate_exit(
        path, int(row.entry_path_index), float(row.entry_price), False,
        float(row.native_stop_pct), float(row.native_target_pct),
        protect_after_close_pct=activation, protected_profit_pct=floor or 0.,
        tighten_after_minutes=float(row.tighten_after_minutes),
        tightened_stop_pct=float(row.tightened_stop_pct))


def entry_allowed(row, rule):
    if rule is None:
        return True
    if rule == 'price_magnitude_min_0p30':
        return abs(float(row.price_change_pct)) >= .30
    if rule == 'oi_min_0p10':
        return float(row.oi_change_pct) >= .10
    if rule == 'volume_min_2p50':
        return float(row.volume_ratio) >= 2.50
    if rule == 'body_min_0p60':
        return float(row.body_ratio) >= .60
    if rule == 'wick_max_0p40':
        return float(row.wick_ratio) <= .40
    if rule == 'nifty_open_max_minus_0p15':
        return float(row.nifty_first_bar_return_pct) <= -.15
    raise ValueError('Unknown entry rule ' + str(rule))


def stats(book, calendar):
    cohort = book.loc[book.day.astype(str).isin(calendar)]
    done = cohort.loc[cohort.portfolio_executed.astype(bool)]
    pnl = done.portfolio_net_profit_rupees.astype(float)
    wins = int(pnl.gt(1e-8).sum())
    losses = int(pnl.lt(-1e-8).sum())
    flats = int(len(done) - wins - losses)
    return dict(selected=len(cohort), fills=int(cohort.filled.astype(bool).sum()),
                trades=len(done), wins=wins, losses=losses, flats=flats,
                win_rate_pct=100*wins/len(done) if len(done) else None,
                net_rs=float(pnl.sum()),
                losing_amount_rs=float(-pnl.loc[pnl.lt(-1e-8)].sum()))


def apply_variant(source, name, activation, floor, entry_rule, cost=5.):
    g3 = source['g3'].copy(deep=True)
    g7 = source['g7'].copy(deep=True)
    permission = pd.Series(g7.base_entry_allowed.astype(bool).to_numpy(), index=g7.sid)
    for rows, work, paths, strategy in (
            (source['g3_rows'], g3, source['g3_paths'], 'G3'),
            (source['g7_rows'], g7, source['g7_paths'], 'G7_V4_lock2')):
        for row in rows.itertuples():
            idx = work.index[work.sid.eq(row.sid)][0]
            if not entry_allowed(row, entry_rule):
                if strategy == 'G3':
                    work.at[idx, 'filled'] = False
                else:
                    permission.loc[row.sid] = False
            if activation is not None and bool(row.filled):
                result = replay_exit(row, paths[int(row.sid)], activation, floor)
                for field, value in result.items():
                    if field not in work:
                        work[field] = None
                    work.at[idx, field] = value
                work.at[idx, 'net_return_pct'] = result['gross_return_pct'] - cost/100
    if cost != 5.:
        g3['cost_pct'] = cost/100
        g3['net_return_pct'] = g3.gross_return_pct - cost/100
    result_g3 = allocate_g3(g3)
    cfg = source['proof']['base_config']
    result_g7 = execute_g7(dict(raw=g7, allowed=permission, integer=True,
                                clock=True, limit=2), cfg, name, cost=cost)
    return {'G3': result_g3, 'G7_V4_lock2': result_g7}


def parity(source):
    g3 = allocate_g3(source['g3'])
    allowed = pd.Series(source['g7'].base_entry_allowed.astype(bool).to_numpy(),
                        index=source['g7'].sid)
    g7 = execute_g7(dict(raw=source['g7'], allowed=allowed, integer=True,
                         clock=True, limit=2), source['proof']['base_config'], 'baseline')
    for label, actual, expected in (('G3', g3, source['g3']),
                                    ('G7', g7, source['g7'])):
        if (not actual.portfolio_executed.astype(str).equals(
                expected.portfolio_executed.astype(str))
                or not np.allclose(actual.portfolio_net_profit_rupees,
                                   expected.portfolio_net_profit_rupees,
                                   atol=1e-6, rtol=0)):
            raise ValueError(label + ' baseline portfolio replay differs')
    for rows, paths, activation, floor, label in (
            (source['g3_rows'], source['g3_paths'], None, None, 'G3'),
            (source['g7_rows'], source['g7_paths'], .6, .1, 'G7')):
        for row in rows.itertuples():
            if not bool(row.filled):
                continue
            result = replay_exit(row, paths[int(row.sid)], activation, floor)
            if (result['exit_reason'] != row.exit_reason
                    or not np.isclose(result['exit_price'], row.exit_price,
                                      atol=1e-6, rtol=0)
                    or not np.isclose(result['gross_return_pct'], row.gross_return_pct,
                                      atol=1e-6, rtol=0)):
                raise ValueError(label + ' minute-path exit parity failed')


def selection_audit(records, periods):
    frame = pd.DataFrame(records)
    dev = frame.loc[frame.period.eq('development')]
    base = dev.loc[dev.variant.eq('baseline')].set_index('strategy')
    audits = []
    by_name = {name: complexity for name, _, _, _, complexity in VARIANTS}
    for name in by_name:
        if name == 'baseline':
            continue
        candidate = dev.loc[dev.variant.eq(name)].set_index('strategy')
        fail = []
        for strategy in ('G3', 'G7_V4_lock2'):
            row, ref = candidate.loc[strategy], base.loc[strategy]
            minimum = max(5, math.ceil(.5*ref.slot_trades))
            if row.slot_trades < minimum:
                fail.append(strategy + ' retains fewer than half development trades')
            if row.slot_win_rate_pct <= 75:
                fail.append(strategy + ' development win rate is not above 75%')
            if row.slot_net_rs <= 0:
                fail.append(strategy + ' development slot net is nonpositive')
            if row.portfolio_net_rs < ref.portfolio_net_rs-1e-6:
                fail.append(strategy + ' development portfolio net falls')
        worst_win = min(candidate.slot_win_rate_pct)
        worst_net = min(candidate.slot_net_rs)
        portfolio_delta = sum(candidate.portfolio_net_rs-base.portfolio_net_rs)
        audits.append(dict(variant=name, eligible=not fail,
                           failure_reasons='; '.join(fail), complexity=by_name[name],
                           worst_strategy_win_rate_pct=float(worst_win),
                           worst_strategy_slot_net_rs=float(worst_net),
                           combined_portfolio_delta_rs=float(portfolio_delta)))
    eligible = [x for x in audits if x['eligible']]
    # Prefer the simpler rule; within complexity, prioritize the worst strategy's
    # win rate, then its slot net, then combined portfolio improvement.
    selected = None
    if eligible:
        selected = max(eligible, key=lambda x: (-x['complexity'],
            x['worst_strategy_win_rate_pct'], x['worst_strategy_slot_net_rs'],
            x['combined_portfolio_delta_rs'], x['variant']))['variant']
    return selected, audits


def run():
    OUT.mkdir(parents=True, exist_ok=True)
    protocol = dict(version='G3_G7_0926_SHORT_ITERATIVE_20261028_V1',
        scope='BACKTEST_ONLY', execution_authority=False, setup_id=SETUP,
        grid_status='ENUMERATED_IN_RUN_SCRIPT', externally_preregistered=False,
        variants=[dict(name=n, protection_activation_pct=a,
                       protected_gross_profit_pct=f, entry_veto=v,
                       complexity=c) for n, a, f, v, c in VARIANTS],
        entry_veto_semantics='Post-selection veto; no quota replacement',
        selection='Development only: both strategies >75% win rate, positive slot net, at least half baseline trades (minimum5), and no portfolio net reduction. Prefer lower complexity, then worst-strategy win rate and net.',
        later_periods='Reported without candidate replacement; historically inspected, not independent holdouts',
        costs_bps=list(COSTS), independent_holdout=False,
        active_configuration_changed=False)
    (OUT/'protocol.json').write_text(json.dumps(protocol, indent=2), encoding='utf-8')
    source = load_inputs()
    parity(source)
    periods = dict(source['proof']['periods'])
    periods['october'] = [d for d in periods['all'] if d.startswith('2026-10')]
    periods['later_blocks'] = periods['validation'] + periods['assessment']
    periods['post_development'] = (periods['validation'] + periods['assessment']
                                   + periods['known_oct26'] + periods['final_oct27'])
    records, books = [], {}
    for name, activation, floor, entry_rule, _ in VARIANTS:
        pair = apply_variant(source, name, activation, floor, entry_rule)
        books[name] = pair
        for strategy, book in pair.items():
            for period, calendar in periods.items():
                slot = stats(book.loc[book.setup_id.eq(SETUP)], calendar)
                portfolio = stats(book, calendar)
                records.append(dict(variant=name, strategy=strategy, period=period,
                    **{'slot_'+k:v for k,v in slot.items()},
                    **{'portfolio_'+k:v for k,v in portfolio.items()}))
        view = [r for r in records if r['variant'] == name and r['period'] == 'all']
        print(name, [(r['strategy'], r['slot_trades'], r['slot_wins'],
                      round(r['slot_net_rs'],2), round(r['portfolio_net_rs'],2))
                     for r in view], flush=True)
    selected, audit = selection_audit(records, periods)
    stresses = []
    for stress_name in STRESS_VARIANTS:
        spec = next(x for x in VARIANTS if x[0] == stress_name)
        for cost in COSTS:
            pair = books[stress_name] if cost == 5 else apply_variant(
                source, *spec[:4], cost=cost)
            for strategy, book in pair.items():
                slot = stats(book.loc[book.setup_id.eq(SETUP)], periods['all'])
                portfolio = stats(book, periods['all'])
                stresses.append(dict(variant=stress_name, strategy=strategy,
                    cost_bps=cost, **{'slot_'+k:v for k,v in slot.items()},
                    **{'portfolio_'+k:v for k,v in portfolio.items()}))
    details = []
    for name in ('baseline', POINT_ESTIMATE_VARIANT):
        for strategy, book in books[name].items():
            slot = book.loc[book.setup_id.eq(SETUP) & book.portfolio_executed.astype(bool)]
            details.extend(dict(variant=name, strategy=strategy, day=str(row.day),
                symbol=str(row.tradingsymbol), exit_reason=str(row.exit_reason),
                net_rs=float(row.portfolio_net_profit_rupees),
                gross_return_pct=float(row.gross_return_pct))
                for row in slot.itertuples())
    frame = pd.DataFrame(records)
    later_pass = False
    if selected:
        later = frame.loc[frame.variant.eq(selected) & frame.period.eq('later_blocks')]
        later_pass = bool(len(later) == 2 and
            later.slot_trades.ge(3).all() and
            later.slot_win_rate_pct.gt(75).all() and
            later.slot_net_rs.gt(0).all())
    summary = dict(selected_on_development=selected,
        full_history_above_75_exit_variants=['protect_0p2_0p1', 'protect_0p2_0p15'],
        cost_stressed_representative_variant=POINT_ESTIMATE_VARIANT,
        full_history_point_estimate_is_in_sample=True,
        later_blocks_descriptive_pass=later_pass,
        qualified_for_adoption=False, independent_holdout=False,
        reason='No independent holdout; repeated historical inspection and post-selection vetoes cannot establish a reliable live win rate above 75%.',
        source=dict(g3_run=str(source['g3_dir']),
                    g3_manifest_sha256=sha(source['g3_dir']/'manifest.json'),
                    g7_run=str(G7_RUN), g7_manifest_sha256=sha(G7_RUN/'manifest.json'),
                    cache=str(source['cache']), cache_manifest_sha256=sha(source['cache']/'manifest.json')))
    (OUT/'results.json').write_text(json.dumps(records, indent=2, allow_nan=False), encoding='utf-8')
    (OUT/'development_selection.json').write_text(json.dumps(audit, indent=2, allow_nan=False), encoding='utf-8')
    (OUT/'cost_stress.json').write_text(json.dumps(stresses, indent=2, allow_nan=False), encoding='utf-8')
    (OUT/'trade_details.json').write_text(json.dumps(details, indent=2, allow_nan=False), encoding='utf-8')
    (OUT/'summary.json').write_text(json.dumps(summary, indent=2, allow_nan=False), encoding='utf-8')
    pd.DataFrame(records).to_csv(OUT/'results.csv', index=False)
    pd.DataFrame(audit).to_csv(OUT/'development_selection.csv', index=False)
    pd.DataFrame(stresses).to_csv(OUT/'cost_stress.csv', index=False)
    detail_frame = pd.DataFrame(details)
    detail_frame.to_csv(OUT/'trade_details.csv', index=False)
    daywise = (detail_frame.assign(
        wins=detail_frame.net_rs.gt(1e-8).astype(int),
        losses=detail_frame.net_rs.lt(-1e-8).astype(int),
        flats=detail_frame.net_rs.abs().le(1e-8).astype(int))
        .groupby(['day', 'strategy', 'variant'], as_index=False)
        .agg(trades=('net_rs', 'size'), wins=('wins', 'sum'),
             losses=('losses', 'sum'), flats=('flats', 'sum'),
             net_rs=('net_rs', 'sum'))
        .sort_values(['day', 'strategy', 'variant']))
    daywise.to_csv(OUT/'daywise_comparison.csv', index=False)
    print('SELECTED', selected, 'LATER_DESCRIPTIVE_PASS', later_pass, flush=True)
    return summary


if __name__ == '__main__':
    run()
