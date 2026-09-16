"""Single-exit search with chronological allocation and explicit fitting scope.

Default: fit through August 26 and report later history descriptively.
--fit-scope full: fit all dates, explicitly labelled in-sample optimization.
All history has been seen in prior research; neither is an untouched test.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd

import fno_v13_v10_a_backtest as engine

v10, r = engine.v10, engine.metrics


def grid():
    # Integer construction avoids dropping boundary ratios through rounding.
    return np.array([(s / 100, t / 100) for s in range(10, 134)
                     for t in range(15, 201) if 2 * t >= 3 * s], dtype=float)


class ReplayCache:
    """Vectorized full brackets; final candidates replay through native V10.

    No entry logic is duplicated: entry fills and priority come from V10/V6.
    Only constant-capital, unrestricted-symbol, three-position books supported.
    """
    def __init__(self, dataset, pairs):
        self.pairs = pairs
        base = dataset['v9_config']
        pc = base.portfolio_config()
        if (base.capital_per_entry_rupees != 100000 or base.leverage_factor != 5 or
                pc.portfolio_capital_rupees != 300000 or pc.max_positions != 3 or
                pc.max_positions_per_symbol is not None or pc.max_open_risk_rupees is not None or
                pc.max_gross_exposure_rupees is not None):
            raise ValueError('Accelerator supports only the frozen V10 portfolio')
        seed = v10.evaluate_orders(dataset['orders'], dataset['paths'],
                                  engine.exit_config(dict(stop_pct=1., target_pct=1.5)), base)[0]
        rows = v10.v9.v6.prepare_source_ledger(seed)
        rows = rows.loc[rows.filled].sort_values(
            ['entry_ts', 'confirmation_ts', 'setup_id', 'portfolio_priority_value', 'tradingsymbol', 'sid'],
            ascending=[True, True, True, False, True, True], kind='stable').reset_index(drop=True)
        self.rows = rows
        self.setup_names = sorted(s.setup_id for s in v10.v9.v5.profile_setups(v10.v9.v5.PROFILES['higher_frequency']))
        self.setup_index = np.array([self.setup_names.index(x) for x in rows.setup_id])
        self.entry = pd.to_datetime(rows.entry_ts, utc=True).astype('int64').to_numpy()
        self.days = pd.to_datetime(rows.day).dt.strftime('%Y-%m-%d').to_numpy()
        self.exit = np.empty((len(rows), len(pairs)), dtype=np.int64)
        self.gross = np.empty((len(rows), len(pairs)))
        stops, si = np.unique(pairs[:, 0], return_inverse=True)
        targets, ti = np.unique(pairs[:, 1], return_inverse=True)
        for i, row in enumerate(rows.itertuples()):
            path = dataset['paths'][int(row.sid)]
            start, entry = int(row.entry_path_index), float(row.entry_price)
            direction = 1 if row.side == 'LONG' else -1
            sl = entry * (1 - direction * stops / 100)
            tp = entry * (1 + direction * targets / 100)
            if direction == 1:
                stop_hits = path['low'][start:, None] <= sl
                target_hits = path['high'][start:, None] >= tp
            else:
                stop_hits = path['high'][start:, None] >= sl
                target_hits = path['low'][start:, None] <= tp
            size = len(path['close'])
            first_s = np.where(stop_hits.any(axis=0), stop_hits.argmax(axis=0) + start, size)[si]
            first_t = np.where(target_hits.any(axis=0), target_hits.argmax(axis=0) + start, size)[ti]
            index = np.minimum(np.minimum(first_s, first_t), size - 1)
            stopped = (first_s <= first_t) & (first_s < size)
            targeted = (first_t < first_s) & (first_t < size)
            stop_level = sl[si]
            opening = path['open'][index]
            gap = stopped & (index > start) & (direction * (opening - stop_level) < 0)
            stop_price = np.where(gap, opening, stop_level)
            self.gross[i] = np.where(stopped, direction * (stop_price / entry - 1) * 100,
                np.where(targeted, pairs[:, 1], direction * (path['close'][-1] / entry - 1) * 100))
            self.exit[i] = path['timestamp_ns'][index]

    def replay(self, configs):
        """Batch [configuration, setup] pair IDs; recalculate all capital clashes."""
        configs = np.atleast_2d(configs)
        n = len(configs)
        active = np.zeros((n, 3), dtype=np.int64)
        pnl = np.zeros((n, len(self.rows)))
        accepted = np.zeros((n, len(self.rows)), dtype=bool)
        batch = np.arange(n)
        for i in range(len(self.rows)):
            free = active <= self.entry[i]
            take = free.any(axis=1)
            place = free.argmax(axis=1)
            ids = configs[:, self.setup_index[i]]
            active[batch[take], place[take]] = self.exit[i, ids[take]]
            pnl[take, i] = self.gross[i, ids[take]] * 5000 - 250
            accepted[:, i] = take
        return pnl, accepted

    def assess(self, configs, scope='development'):
        pnl, accepted = self.replay(configs)
        out = {}
        for name, mask in [('TRAIN', self.days <= '2026-08-13'),
                           ('VALIDATION', (self.days > '2026-08-13') & (self.days <= '2026-08-26')),
                           ('DEV', self.days <= '2026-08-26'),
                           ('FULL', np.ones(len(self.days), dtype=bool))]:
            p, a = pnl[:, mask], accepted[:, mask]
            count = a.sum(axis=1)
            gain, loss = np.maximum(p, 0).sum(axis=1), -np.minimum(p, 0).sum(axis=1)
            out[name + '_trades'] = count
            out[name + '_win'] = (p > r.EPS).sum(axis=1) * 100 / np.maximum(count, 1)
            out[name + '_pf'] = np.divide(gain, loss, out=np.full(len(p), np.inf), where=loss > r.EPS)
            out[name + '_net'] = p.sum(axis=1)
        # Strictly use development columns for both eligibility and ranking.
        out['deficit'] = (np.maximum(65 - out['TRAIN_win'], 0) +
                          np.maximum(65 - out['VALIDATION_win'], 0) +
                          np.maximum(15 - out['TRAIN_trades'], 0) * 100 +
                          np.maximum(10 - out['VALIDATION_trades'], 0) * 100)
        if scope == 'full':
            out['deficit'] = np.maximum(65 - out['FULL_win'], 0) + np.maximum(30 - out['FULL_trades'], 0) * 100
        elif scope != 'development':
            raise ValueError('Unknown fitting scope')
        prefix = 'FULL' if scope == 'full' else 'DEV'
        out['objective_pf'], out['objective_net'] = out[prefix + '_pf'], out[prefix + '_net']
        return pd.DataFrame(out)


def ranking(frame):
    return frame.sort_values(['deficit', 'objective_pf', 'objective_net'],
                             ascending=[True, False, False], kind='stable').index.to_numpy()


def settings(cache, ids, default_id, scope='development'):
    def pair(i):
        s, t = cache.pairs[int(i)]
        return dict(stop_pct=float(s), target_pct=float(t))
    return dict(version='V13-v10-A', default=pair(default_id),
                setups={name: pair(ids[i]) for i, name in enumerate(cache.setup_names)},
                partial_exits=False, breakeven_stop=False,
                fitted_through='2026-09-11' if scope == 'full' else '2026-08-26',
                evidence='FULL_HISTORY_IN_SAMPLE_FIT' if scope == 'full' else 'DEVELOPMENT_FIT_PREVIOUSLY_SEEN_LATER_REPLAY')


def verify(cache, ids, ledger):
    pnl, accepted = cache.replay(ids)
    native = ledger.set_index('sid').loc[cache.rows.sid]
    np.testing.assert_array_equal(accepted[0], native.portfolio_executed.to_numpy())
    np.testing.assert_allclose(pnl[0], native.portfolio_net_profit_rupees, atol=1e-7, rtol=0)
    selected_ids = ids[cache.setup_index]
    np.testing.assert_allclose(cache.gross[np.arange(len(native)), selected_ids], native.gross_return_pct, atol=1e-10, rtol=0)
    np.testing.assert_array_equal(cache.exit[np.arange(len(native)), selected_ids],
                                 pd.to_datetime(native.exit_ts, utc=True).astype('int64'))
    return dict(filled_rows=len(native), allocation_equal=True, exit_times_equal=True,
                maximum_pnl_difference=float(np.max(np.abs(pnl[0] - native.portfolio_net_profit_rupees))))


def table(frame):
    return frame.to_markdown(index=False, floatfmt='.2f')


def run(scope='development'):
    output = engine.DEFAULT_OUTPUT / 'historical_fit' if scope == 'full' else engine.DEFAULT_OUTPUT
    output.mkdir(parents=True, exist_ok=True)
    starts, max_passes = (8, 8) if scope == 'full' else (3, 4)
    pairs = grid()
    protocol = dict(version='V13-v10-A', target_cap_pct=2., stop_cap_pct=1.5, minimum_reward_risk=1.5,
        grid='SL 0.10..1.33%; target 0.15..2.00%; step 0.01%; only target >= 1.5*SL',
        pairs=len(pairs), partial_exits=False, breakeven_stop=False,
        group='Existing five-minute SIGNAL time plus direction; setup IDs denote one-minute confirmation time',
        sparse_rule='Fewer than 5 development triggered trades: shared default; do not disable any setup',
        search=f'Exhaustive shared grid; {starts} diverse shared seeds; up to {max_passes} coordinate passes over supported setups; full grid per coordinate',
        objective='Require >=65% net win in TRAIN and VALIDATION, >=15/10 trades; maximize pooled development net PF, then net profit',
        fallback='If constraints infeasible, minimize win/sample deficit then maximize development PF; explicitly report failure',
        fit_end='2026-08-26', train_end='2026-08-13', later_used_for_selection=False,
        evidence='All history previously seen. Repeated slot optimization has substantial selection bias. No global optimum guarantee.',
        accounting='Same 5bps round trip, 100000 capital/trade, 5x, 300000 book, three slots, 15:15 exit, stop-first OHLC, adverse stop gaps')
    if scope == 'full':
        protocol.update(fit_end='2026-09-11', later_used_for_selection=True,
            objective='Require >=65% net win over FULL history and >=30 trades; maximize full-history PF then net profit',
            sparse_rule='Fewer than 5 full-history triggered trades: shared default; do not disable any setup',
            evidence='FULL HISTORY IN-SAMPLE OPTIMIZATION. Separate development-frozen variant failed the 65% full-period objective. This fit uses all dates including September; no independent performance claim.')
    r.dump(output / 'research_protocol.json', protocol)
    pd.DataFrame(pairs, columns=['stop_pct', 'target_pct']).to_csv(output / 'registered_grid.csv', index=False)
    print(f'[V10-A] Registered {len(pairs)} full-exit brackets.', flush=True)
    dataset = v10.load_source()
    cache = ReplayCache(dataset, pairs)
    nslots = len(cache.setup_names)
    fit_mask = np.ones(len(cache.days), dtype=bool) if scope == 'full' else cache.days <= '2026-08-26'
    counts = np.bincount(cache.setup_index[fit_mask], minlength=nslots)
    supported = np.flatnonzero(counts >= 5)
    print(f'[V10-A] {len(supported)}/{nslots} setups have >=5 {scope} fills.', flush=True)
    shared_configs = np.repeat(np.arange(len(pairs))[:, None], nslots, axis=1)
    shared = cache.assess(shared_configs, scope)
    shared[['stop_pct', 'target_pct']] = pairs
    shared.to_csv(output / 'shared_grid_results.csv', index=False)
    seeds = []
    for i in ranking(shared):
        if not seeds or all(np.max(np.abs(pairs[i] - pairs[j])) >= .15 - 1e-12 for j in seeds):
            seeds.append(int(i))
        if len(seeds) == starts:
            break
    trace, finalists, boards = [], [], []
    evaluations = len(pairs)
    for seed_no, seed in enumerate(seeds):
        current = np.full(nslots, seed, dtype=int)
        for turn in range(max_passes):
            changed = False
            order = supported if (turn + seed_no) % 2 == 0 else supported[::-1]
            for slot in order:
                candidates = np.repeat(current[None, :], len(pairs), axis=0)
                candidates[:, slot] = np.arange(len(pairs))
                scores = cache.assess(candidates, scope)
                evaluations += len(pairs)
                best = int(ranking(scores)[0])
                previous = int(current[slot])
                # Keep the incumbent on exact outcome ties to prevent pointless churn.
                new_key = (scores.at[best, 'deficit'], -scores.at[best, 'objective_pf'], -scores.at[best, 'objective_net'])
                old_key = (scores.at[previous, 'deficit'], -scores.at[previous, 'objective_pf'], -scores.at[previous, 'objective_net'])
                if new_key < old_key:
                    current[slot] = best
                    changed = True
                top = scores.loc[ranking(scores)[:10]].copy()
                top['pair_id'] = top.index
                top['seed'], top['pass'], top['setup_id'] = seed, turn, cache.setup_names[slot]
                boards.append(top)
                trace.append(dict(seed=seed, pass_number=turn, setup_id=cache.setup_names[slot],
                    old_pair_id=previous, chosen_pair_id=int(current[slot]), **scores.loc[current[slot]].to_dict()))
            summary = cache.assess(current, scope).iloc[0]
            print(f'[V10-A] Seed {seed_no+1}/{len(seeds)} pass {turn+1}: PF={summary.objective_pf:.5f}, win={summary.FULL_win if scope == "full" else summary.DEV_win:.2f}%, deficit={summary.deficit:.2f}', flush=True)
            if not changed:
                break
        finalists.append((current.copy(), seed))
    final_scores = cache.assess(np.array([x[0] for x in finalists]), scope)
    chosen = int(ranking(final_scores)[0])
    ids, default_id = finalists[chosen]
    cfg = settings(cache, ids, default_id, scope)
    # Report one-coordinate local sensitivity without selecting on these rows.
    sensitivity = []
    pair_ids = {(round(s * 100), round(t * 100)): i for i, (s, t) in enumerate(pairs)}
    for slot in supported:
        s, t = pairs[ids[slot]]
        for ds, dt in [(-1, 0), (1, 0), (0, -1), (0, 1)]:
            key = (round(s * 100) + ds, round(t * 100) + dt)
            if key not in pair_ids:
                continue
            neighbor = ids.copy()
            neighbor[slot] = pair_ids[key]
            sensitivity.append(dict(setup_id=cache.setup_names[slot], stop_pct=key[0]/100,
                target_pct=key[1]/100, **cache.assess(neighbor, scope).iloc[0].to_dict()))
    pd.DataFrame(sensitivity).to_csv(output / 'local_sensitivity.csv', index=False)
    r.dump(output / 'frozen_config.json', cfg)
    r.dump(output / 'selection_freeze.json', dict(development_metrics=final_scores.iloc[chosen].to_dict(),
        pair_evaluations=evaluations, shared_pair_count=len(pairs), seed_ids=seeds,
        objective_met=bool(final_scores.iloc[chosen].deficit <= 1e-9), later_used_for_selection=scope == 'full', fit_scope=scope))
    pd.DataFrame(trace).to_csv(output / 'search_trace.csv', index=False)
    pd.concat(boards, ignore_index=True).to_csv(output / 'coordinate_top10.csv', index=False)
    final_scores.to_csv(output / 'development_finalists.csv', index=False)
    r.dump(output / 'finalist_configs.json', [settings(cache, x, default, scope) for x, default in finalists])
    print('[V10-A] Settings frozen. Native replay and period reporting now.', flush=True)
    result = engine.evaluate(dataset, cfg)
    r.save(output / 'final/V13_V10_A', *result)
    checks = {'chosen': verify(cache, ids, result[1])}
    # Independent native replay across wide brackets validates the search accelerator.
    for i in sorted(set([0, len(pairs)//3, len(pairs)//2, len(pairs)-1, *seeds])):
        same = np.full(nslots, i, dtype=int)
        _, ledger, _ = engine.evaluate(dataset, settings(cache, same, i, scope))
        checks[str(i)] = verify(cache, same, ledger)
    baseline_cfg = v10.V10Config(**json.loads((v10.DEFAULT_OUTPUT / 'balanced/frozen_config.json').read_text()))
    baseline = v10.evaluate_orders(dataset['orders'], dataset['paths'], baseline_cfg, dataset['v9_config'])
    old = pd.read_csv(v10.DEFAULT_OUTPUT / 'balanced/final/V13_V10/portfolio_trades.csv', float_precision='round_trip')
    checks['v10_baseline_parity'] = v10.v9.assert_control_parity(old, baseline[1])
    r.save(output / 'final/V13_V10_CONTROL', *baseline)
    shared_id = int(ranking(shared)[0])
    shared_result = engine.evaluate(dataset, settings(cache, np.full(nslots, shared_id), shared_id, scope))
    r.save(output / 'final/V13_V10_A_SHARED_CONTROL', *shared_result)
    groups = r.periods(dataset['days'])
    groups.update({month: [d for d in dataset['days'] if str(d).startswith(prefix)] for month, prefix in
                   [('JULY', '2026-07'), ('AUGUST', '2026-08')]})
    metrics, daily, slot_rows = [], [], []
    comparisons = [('V10', baseline), ('V10-A', result), ('A_SHARED', shared_result)]
    if scope == 'full':
        devcfg = json.loads((engine.DEFAULT_OUTPUT / 'frozen_config.json').read_text())
        comparisons.append(('A_DEVELOPMENT_FROZEN', engine.evaluate(dataset, devcfg)))
    for name, (_, ledger, _) in comparisons:
        for period, days in groups.items():
            for cost in [5, 9]:
                metrics.append(dict(version=name, period=period, **r.metric(ledger, days, cost_bps=cost)))
        for day in dataset['days']:
            daily.append(dict(version=name, day=day, **r.metric(ledger, [day])))
    for slot, name in enumerate(cache.setup_names):
        ledger = result[1].loc[result[1].setup_id.eq(name)]
        row = dict(setup_id=name, signal_time=(pd.Timestamp('2026-01-01 ' + name[:2] + ':' + name[2:4]) - pd.Timedelta(minutes=1)).strftime('%H:%M'),
            side=name.split('_')[1], **cfg['setups'][name], reward_risk=pairs[ids[slot], 1] / pairs[ids[slot], 0],
            fit_triggered=int(counts[slot]), policy='SLOT_FIT' if counts[slot] >= 5 else 'SHARED_SPARSE_FALLBACK')
        for label in ['FULL', 'TRAIN', 'VALIDATION', 'SEPTEMBER']:
            row.update({label + '_' + key: value for key, value in r.metric(ledger, groups[label]).items()})
        slot_rows.append(row)
    metric_frame, slot_frame = pd.DataFrame(metrics), pd.DataFrame(slot_rows)
    metric_frame.to_csv(output / 'comparison_metrics.csv', index=False)
    pd.DataFrame(daily).to_csv(output / 'daily_results.csv', index=False)
    slot_frame.to_csv(output / 'slot_settings_and_results.csv', index=False)
    r.dump(output / 'verification.json', checks)
    show = metric_frame.loc[metric_frame.cost_bps.eq(5), ['version', 'period', 'trades', 'win_rate_pct', 'profit_factor', 'net_profit_rupees', 'daily_close_drawdown_rupees']]
    report = '\n\n'.join([
        '# V13-v10-A results',
        'Full exits only, fixed initial stop, no partial exits or break-even moves. Targets <=2%, stops <=1.5%, target:SL >=1.5. Existing V10 selections, next-minute entry and expiry preserved. Each setup denotes signal time plus direction; its ID uses confirmation time one minute later.',
        f'Searched {len(pairs):,} distinct brackets and {evaluations:,} portfolio configurations including repeated coordinate scans. {starts} starts and up to {max_passes} coordinate passes; best found in this search, not a proven global optimum. Fit scope: {scope}. Fitted through {cfg["fitted_through"]}. Evidence: {protocol["evidence"]}',
        'Objective: ' + protocol['objective'] + '. ' + protocol['sparse_rule'] + '. Per-slot win rates are reported; 65% is a portfolio objective, not a guarantee in every sparse slot. TRAIN/VALIDATION labels in the full-history report are date partitions within fitting data, not independent validation.',
        table(show),
        '## Exit settings and full-period slot results',
        table(slot_frame[['signal_time', 'side', 'stop_pct', 'target_pct', 'reward_risk', 'fit_triggered', 'policy', 'FULL_trades', 'FULL_win_rate_pct', 'FULL_profit_factor']]),
        '## Accounting and verification',
        'Modeled cash-price execution using futures OI selections; Rs 100,000 capital per trade, 5x exposure, Rs 300,000 portfolio and three simultaneous positions. Flat 5bps round-trip costs and 9bps stress saved. Net wins include profitable time exits. Target:SL is the configured price-distance ratio before costs; time exits and gaps change realized reward/risk. Stop-first same-bar ordering and adverse stop gaps retained. Drawdown is daily-close only.',
        'Final settings and sampled wide-grid brackets matched native V10 exit times, gross returns, portfolio acceptance and P&L. Original V10 replay matches the saved baseline. Full trade, daily, monthly, per-slot and stress ledgers are alongside this report.',
        'Reproduce both rounds: `python -B fno_v13_v10_a_research.py --fit-scope development`, then `python -B fno_v13_v10_a_research.py --fit-scope full`. Replay active full-history settings: `python -B fno_v13_v10_a_backtest.py`.'
    ])
    (output / 'V13_V10_A_DETAILED_RESULTS.md').write_text(report + '\n', encoding='utf-8')
    files = [Path(__file__), Path(engine.__file__), Path(v10.__file__), Path(r.__file__), Path(v10.v9.__file__),
             Path(v10.v9.v5.__file__), Path(v10.v9.v6.__file__)]
    r.dump(output / 'research_manifest.json', dict(complete=True,
        code_sha256={str(p.resolve()): v10.sha(p) for p in files},
        artifacts={str(p.relative_to(output)): v10.sha(p) for p in sorted(output.rglob('*'))
                   if p.is_file() and p.name != 'research_manifest.json'}))
    print(show.to_string(index=False), flush=True)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--fit-scope', choices=['development', 'full'], default='development')
    run(parser.parse_args().fit_scope)
