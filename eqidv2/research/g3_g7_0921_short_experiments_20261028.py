"""Research-only, causal 09:21 SHORT exit and entry-veto experiments."""
from __future__ import annotations

import json
import hashlib
from pathlib import Path

import numpy as np
import pandas as pd

from research.g3_sl_target_simulator import simulate_exit
from research.g3_0920_backtest import allocate as allocate_g3
from research.g7_v4_practical_research import BASE, execute as execute_g7
from research.g7_entry_quality_20261026 import verify_members


G3_ROOT = Path('C:/TradingData/eqidv2/backtesting_result_v13_v10_g_3')
G7_RUN = Path('C:/TradingData/eqidv2/backtesting_result_v13_v10_g_7/v4_research/practical_research/runs/2026-10-27/20261027T185748273567_five_fixed_tests')
OUT = Path(__file__).resolve().parents[1] / 'outputs/g3_g7_0921_short_iterative_20261028'


def inputs():
    pointer = json.loads((G3_ROOT / 'latest/current_backtest.json').read_text())
    g3_dir = G3_ROOT / pointer['run_dir']
    verify_members(g3_dir, pointer['manifest_sha256'])
    verify_members(G7_RUN)
    g3 = pd.read_csv(g3_dir / 'portfolio_trades.csv')
    g7 = pd.read_csv(G7_RUN / 'loss_lock2/portfolio_trades.csv')
    summary = json.loads((G7_RUN / 'summary.json').read_text())
    native, _, _, paths, proof = BASE.load_cache(Path(summary['cache']))
    verify_members(summary['cache'], summary['cache_manifest_sha256'])
    selected = native.loc[native.setup_id.eq('0921_SHORT')].set_index(['day', 'tradingsymbol'])
    g3_rows = g3.loc[g3.setup_id.eq('0921_SHORT')].copy()
    g7_rows = g7.loc[g7.setup_id.eq('0921_SHORT')].copy()
    assert len(selected) == len(g3_rows) == len(g7_rows) == 10
    assert set(zip(g3_rows.day, g3_rows.tradingsymbol)) == set(selected.index)
    return g3, g7, g3_rows, g7_rows, selected, paths, proof


def one_exit(row, selected, paths, activation=None, floor=0.0):
    src = selected.loc[(row.day, row.tradingsymbol)]
    if not bool(row.filled):
        return None
    assert bool(src.filled) and np.isclose(row.entry_price, src.entry_price, atol=1e-6, rtol=0)
    return simulate_exit(paths[int(src.sid)], int(src.entry_path_index), float(row.entry_price),
                         False, float(row.native_stop_pct), float(row.native_target_pct),
                         protect_after_close_pct=activation, protected_profit_pct=floor,
                         tighten_after_minutes=float(row.tighten_after_minutes),
                         tightened_stop_pct=float(row.tightened_stop_pct))


def preview():
    _, _, g3_rows, g7_rows, selected, paths, _ = inputs()
    for label, rows in [('G3', g3_rows), ('G7', g7_rows)]:
        baseline = []
        for row in rows.itertuples():
            if not row.filled:
                continue
            r = one_exit(row, selected, paths, None)
            baseline.append(dict(day=row.day, symbol=row.tradingsymbol, saved=row.exit_reason,
                                 replay=r['exit_reason'], saved_gross=row.gross_return_pct,
                                 replay_gross=r['gross_return_pct'], saved_exit=row.exit_price,
                                 replay_exit=r['exit_price']))
        b = pd.DataFrame(baseline)
        print(label, 'BASELINE PARITY', b.saved.eq(b.replay).all(),
              np.allclose(b.saved_gross, b.replay_gross, atol=1e-6, rtol=0),
              np.allclose(b.saved_exit, b.replay_exit, atol=1e-6, rtol=0))
        if not (b.saved.eq(b.replay).all() and np.allclose(b.saved_gross, b.replay_gross, atol=1e-6, rtol=0)):
            print(b.to_string(index=False))
        print(label, 'EXIT LADDER')
        for activation in (0.2, 0.3, 0.4, 0.5, 0.6):
            rows_out = []
            for row in rows.itertuples():
                if not row.filled:
                    continue
                r = one_exit(row, selected, paths, activation, 0.1)
                net = r['gross_return_pct'] - float(row.cost_pct)
                rows_out.append(dict(day=row.day, symbol=row.tradingsymbol,
                                     reason=r['exit_reason'], net_pct=net))
            f = pd.DataFrame(rows_out)
            print(activation, 'wins', int(f.net_pct.gt(0).sum()), 'trades', len(f),
                  'net pct sum', round(f.net_pct.sum(), 4), 'losers', f.loc[f.net_pct.lt(0),'symbol'].tolist())


VARIANTS = [
    ('baseline', None, None, None),
    ('protect_0p6_0p1', 0.6, 0.1, None),
    ('protect_0p4_0p1', 0.4, 0.1, None),
    ('protect_0p3_0p1', 0.3, 0.1, None),
    ('protect_0p2_0p1', 0.2, 0.1, None),
    ('protect_0p2_0p15', 0.2, 0.15, None),
    ('oi_cap_0p6', None, None, 'oi_cap_0p6'),
    ('body_min_0p6', None, None, 'body_min_0p6'),
    ('nifty_below_minus_0p15', None, None, 'nifty_below_minus_0p15'),
    ('protect_0p2_0p1_plus_oi_cap', 0.2, 0.1, 'oi_cap_0p6'),
    ('protect_0p2_0p15_plus_oi_cap', 0.2, 0.15, 'oi_cap_0p6'),
]


def validate_candidate_configs():
    for name in ('g3_candidate_config.json', 'g7_v4_lock2_candidate_config.json'):
        overlay = json.loads((OUT / name).read_text(encoding='utf-8'))
        source = Path(overlay['source_config'])
        if not source.is_absolute():
            source = Path(__file__).resolve().parents[1] / source
        if (hashlib.sha256(source.read_bytes()).hexdigest() != overlay['source_config_sha256']
                or overlay['scope'] != 'BACKTEST_ONLY'
                or overlay['execution_authority'] is not False
                or overlay['adopted'] is not False
                or overlay['setup_id'] != '0921_SHORT'
                or overlay['post_selection_entry_veto'] !=
                {'oi_change_pct_max': 0.6, 'allow_quota_replacement': False}
                or overlay['slot_exit_override']['protect_after_completed_post_entry_close_pct'] != 0.2
                or overlay['slot_exit_override']['protected_gross_profit_pct'] != 0.15
                or overlay['slot_exit_override']['effective'] != 'NEXT_MINUTE_OPEN'):
            raise ValueError('Candidate overlay or source config drift: ' + name)


def veto(row, filter_name):
    if filter_name is None:
        return False
    if filter_name == 'oi_cap_0p6':
        return float(row.oi_change_pct) > 0.6
    if filter_name == 'body_min_0p6':
        return float(row.body_ratio) < 0.6
    if filter_name == 'nifty_below_minus_0p15':
        return float(row.nifty_first_bar_return_pct) > -0.15
    raise ValueError(filter_name)


def portfolio_stats(book, days):
    part = book.loc[book.day.astype(str).isin(days) & book.portfolio_executed.astype(bool)]
    pnl = part.portfolio_net_profit_rupees.astype(float)
    return dict(trades=len(part), wins=int(pnl.gt(1e-8).sum()),
                losses=int(pnl.lt(-1e-8).sum()),
                win_rate_pct=100*int(pnl.gt(1e-8).sum())/len(part) if len(part) else None,
                net_rs=float(pnl.sum()), losing_amount_rs=float(-pnl.loc[pnl.lt(0)].sum()))


def run():
    validate_candidate_configs()
    g3, g7, g3_rows, g7_rows, selected, paths, proof = inputs()
    base_g3 = allocate_g3(g3)
    if not (base_g3.portfolio_executed.astype(str).equals(g3.portfolio_executed.astype(str))
            and np.allclose(base_g3.portfolio_net_profit_rupees, g3.portfolio_net_profit_rupees, atol=1e-6, rtol=0)):
        raise ValueError('G3 baseline portfolio replay differs')
    cfg = proof['base_config']
    allowed = pd.Series(g7.base_entry_allowed.astype(bool).to_numpy(), index=g7.sid)
    base_g7 = execute_g7(dict(raw=g7, allowed=allowed, integer=True, clock=True, limit=2), cfg, 'baseline')
    if not (base_g7.portfolio_executed.astype(str).equals(g7.portfolio_executed.astype(str))
            and np.allclose(base_g7.portfolio_net_profit_rupees, g7.portfolio_net_profit_rupees, atol=1e-6, rtol=0)):
        raise ValueError('G7 baseline portfolio replay differs')
    for row in g3_rows.itertuples():
        if row.filled:
            check = one_exit(row, selected, paths)
            if (check['exit_reason'] != row.exit_reason or
                    not np.isclose(check['exit_price'], row.exit_price, atol=1e-6, rtol=0) or
                    not np.isclose(check['gross_return_pct'], row.gross_return_pct, atol=1e-6, rtol=0)):
                raise ValueError('G3 native trade/path parity failed')
    for row in g7_rows.itertuples():
        if row.filled:
            check = one_exit(row, selected, paths, 0.6, 0.1)
            if (check['exit_reason'] != row.exit_reason or
                    not np.isclose(check['exit_price'], row.exit_price, atol=1e-6, rtol=0) or
                    not np.isclose(check['gross_return_pct'], row.gross_return_pct, atol=1e-6, rtol=0)):
                raise ValueError('G7 protected trade/path parity failed')
    days = proof['periods']['all']
    periods = dict(all=days, development=proof['periods']['development'],
                   validation=proof['periods']['validation'], assessment=proof['periods']['assessment'],
                   october=[d for d in days if d.startswith('2026-10')])
    records = []
    detail = []
    stress = []
    for name, activation, floor, filter_name in VARIANTS:
        a = g3.copy(deep=True)
        b = g7.copy(deep=True)
        permission = allowed.copy()
        for source, work in ((g3_rows, a), (g7_rows, b)):
            for row in source.itertuples():
                index = work.index[work.sid.eq(row.sid)][0]
                if veto(row, filter_name):
                    if work is a:
                        work.at[index, 'filled'] = False
                    else:
                        permission.loc[row.sid] = False
                if activation is not None and bool(row.filled):
                    result = one_exit(row, selected, paths, activation, floor)
                    for field, value in result.items():
                        if field not in work:
                            work[field] = None
                        work.at[index, field] = value
                    work.at[index, 'net_return_pct'] = result['gross_return_pct'] - float(row.cost_pct)
        replay_g3 = allocate_g3(a)
        replay_g7 = execute_g7(dict(raw=b, allowed=permission, integer=True, clock=True, limit=2), cfg, name)
        if name in {'baseline', 'protect_0p2_0p1', 'protect_0p2_0p15_plus_oi_cap'}:
            for fee in (9., 15., 30.):
                a_fee = a.copy(deep=True)
                a_fee['cost_pct'] = fee / 100
                a_fee['net_return_pct'] = a_fee.gross_return_pct - fee / 100
                books = [('G3', allocate_g3(a_fee)),
                         ('G7_V4_lock2', execute_g7(
                             dict(raw=b, allowed=permission, integer=True, clock=True, limit=2),
                             cfg, name, cost=fee))]
                for label, book in books:
                    stress.append(dict(variant=name, strategy=label, cost_bps=fee,
                                       slot=portfolio_stats(book.loc[book.setup_id.eq('0921_SHORT')], days),
                                       portfolio=portfolio_stats(book, days)))
        for label, book in (('G3', replay_g3), ('G7_V4_lock2', replay_g7)):
            for period, calendar in periods.items():
                slot = book.loc[book.setup_id.eq('0921_SHORT')]
                records.append(dict(variant=name, strategy=label, period=period,
                                    slot=portfolio_stats(slot, calendar),
                                    portfolio=portfolio_stats(book, calendar)))
            slot = book.loc[book.setup_id.eq('0921_SHORT') & book.portfolio_executed.astype(bool)]
            detail.extend(dict(variant=name, strategy=label, day=str(row.day),
                               symbol=str(row.tradingsymbol), exit_reason=str(row.exit_reason),
                               net_rs=float(row.portfolio_net_profit_rupees),
                               gross_return_pct=float(row.gross_return_pct))
                          for row in slot.itertuples())
        allrows = [r for r in records if r['variant'] == name and r['period'] == 'all']
        print(name, [(r['strategy'], r['slot']['trades'], r['slot']['wins'],
                      round(r['slot']['net_rs'], 2), round(r['portfolio']['net_rs'], 2))
                     for r in allrows], flush=True)
    OUT.mkdir(parents=True, exist_ok=True)
    (OUT / 'results.json').write_text(json.dumps(records, indent=2, allow_nan=False), encoding='utf-8')
    (OUT / 'trade_details.json').write_text(json.dumps(detail, indent=2, allow_nan=False), encoding='utf-8')
    (OUT / 'cost_stress.json').write_text(json.dumps(stress, indent=2, allow_nan=False), encoding='utf-8')
    (OUT / 'protocol.json').write_text(json.dumps(dict(
        source_g3=str(G3_ROOT), source_g7=str(G7_RUN), variants=VARIANTS,
        design='Research-only post-selection veto or slot-only causal exit change; full portfolio replay',
        costs_bps=[5, 9, 15, 30], source_sessions=len(days), first_day=days[0], last_day=days[-1],
        independent_holdout=False, execution_authority=False,
        qualified_for_adoption=False,
        conclusion='No tested rule establishes a reliable above-75-percent win rate; the 8/8 result is post-hoc and cost-fragile.'), indent=2), encoding='utf-8')


if __name__ == '__main__':
    run()
