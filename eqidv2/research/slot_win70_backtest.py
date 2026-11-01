"""Replay the shared slot policy on every frozen selected opportunity.

The allowlist evaluates every represented slot/direction pair retrospectively
from the old executed portfolio.
Entry/exit paths are unchanged by whole-slot selection, but all capital,
symbol-admission and G7 realized-loss-lock decisions are recomputed. This is
not an independent holdout and does not place or authorize broker orders.
"""
from __future__ import annotations

import argparse
from datetime import datetime
import hashlib
import json
from pathlib import Path
import sys

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
SOURCE = Path('C:/TradingData/eqidv2/backtesting_result_v13_v10_g_7/v4_lock_backtest/runs/2026-10-28/20261028T143059782924_f86f1197_0926_short_wick020_lock2')
G7_CACHE = Path('C:/TradingData/eqidv2/backtesting_result_v13_v10_g_7/v4_research/runs/2026-10-27/20261027T165517683392_v4_extremes/inputs')
FILES = {'G3': 'g3_reference_portfolio_trades.csv', 'G7_V4_LOCK2': 'portfolio_trades.csv'}
SOURCE_HASHES = {'G3': '7fd051ac41fb3ad9956ca52f641fdc8b55bed033c6a04365c7e648dd7c3de564',
                 'G7_V4_LOCK2': '341b3735093aa31bfdfe31e71a61f7353da0136686c156caa0106606d97c5e1e'}
G7_PROVENANCE_SHA256 = '0ea754593755d258e266d93d4307065bbe696940ecb96b0b21866f4c381ab558'
COSTS = (5., 9., 15.)
VERSION = 'G3-G7-V4-LOCK2-SLOT-WIN70-ROLLOUT-V1'


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def read(path):
    return json.loads(Path(path).read_text(encoding='utf-8-sig'))


def dump(path, payload):
    Path(path).write_text(json.dumps(payload, indent=2, sort_keys=True, default=str, allow_nan=False), encoding='utf8')


def flags(values):
    from research.g8_exit_engine import _flags
    return _flags(values, 'execution flag')


def metrics(book, days):
    from research.g8_exit_engine import metrics as compute
    result = compute(book, days)
    cohort = book.loc[book.day.astype(str).str[:10].isin(days)]
    if 'slot_policy_allowed' in cohort:
        result['slot_policy_allowed_opportunities'] = int(flags(cohort.slot_policy_allowed).sum())
    return result


def apply_slot_policy(source, strategy, enabled, base_config, cost=5.):
    """Reallocate all opportunities, retaining disabled rows as explicit audit.

    Existing G7 final permissions are deliberately discarded. The base Nifty
    and entry-quality permissions are retained before a fresh two-loss lock.
    """
    if not isinstance(enabled, (set, frozenset)):
        enabled = frozenset(enabled)
    if source.sid.duplicated().any() or not source.index.is_unique:
        raise ValueError('Opportunity identities must be unique')
    work = source.copy(deep=True)
    slot_allowed = work.setup_id.isin(enabled)
    if strategy == 'G3':
        from research.g3_0920_backtest import allocate
        source_filled = flags(work.filled)
        work['slot_policy_source_filled'] = source_filled
        work['filled'] = source_filled & slot_allowed
        work['cost_pct'] = np.where(work.filled, float(cost) / 100., np.nan)
        work['net_return_pct'] = np.where(work.filled,
            pd.to_numeric(work.gross_return_pct, errors='coerce') - float(cost) / 100., np.nan)
        result = allocate(work)
    elif strategy == 'G7_V4_LOCK2':
        from research.g7_v4_practical_research import execute
        permissions = flags(work.base_entry_allowed) & slot_allowed
        work['slot_policy_source_base_entry_allowed'] = flags(work.base_entry_allowed)
        allowed = pd.Series(permissions.to_numpy(bool), index=work.sid.astype(int))
        result = execute(dict(raw=work, allowed=allowed, integer=True, clock=True, limit=2),
                         base_config, 'slot_win70_loss_lock2', cost=cost)
    else:
        raise ValueError('Unknown strategy: ' + strategy)
    # Both allocators preserve input order; enforce this instead of relying on
    # index arithmetic to mark a different opportunity as disabled.
    if not result.sid.reset_index(drop=True).equals(source.sid.reset_index(drop=True)):
        raise ValueError('Allocator changed source row order or inventory')
    for col in ('entry_price', 'exit_price', 'gross_return_pct'):
        if not np.allclose(pd.to_numeric(result[col]), pd.to_numeric(source[col]),
                           equal_nan=True, rtol=0, atol=1e-9):
            raise ValueError('Slot selection changed frozen execution outcome: ' + col)
    for col in ('entry_ts', 'exit_ts'):
        if not pd.to_datetime(result[col], utc=True).equals(pd.to_datetime(source[col], utc=True)):
            raise ValueError('Slot selection changed frozen execution clock: ' + col)
    result['slot_policy_allowed'] = slot_allowed.to_numpy(bool)
    blocked = ~result.slot_policy_allowed
    result.loc[blocked, 'portfolio_status'] = 'SLOT_WIN_RATE_BELOW_70_OR_NO_EVIDENCE'
    result.loc[blocked, 'portfolio_reject_reason'] = 'FROZEN_SLOT_ALLOWLIST_REJECTED'
    if (flags(result.portfolio_executed) & blocked).any():
        raise ValueError('Disabled slot was executed')
    result['selection_is_retrospective'] = True
    result['scope'] = 'BACKTEST_ONLY'
    result['execution_authority'] = False
    return result


def verify_control(actual, saved, strategy):
    a = actual.set_index('sid').sort_index()
    b = saved.set_index('sid').sort_index()
    if not a.index.equals(b.index):
        raise ValueError('Control opportunity inventory changed: ' + strategy)
    fields = ['portfolio_executed', 'portfolio_net_profit_rupees',
              'portfolio_gross_profit_rupees', 'portfolio_cost_rupees']
    for col in fields:
        if col == 'portfolio_executed':
            good = np.array_equal(flags(a[col]), flags(b[col]))
        else:
            good = np.allclose(pd.to_numeric(a[col]), pd.to_numeric(b[col]), rtol=0, atol=1e-6)
        if not good:
            raise ValueError('Baseline parity failed: ' + strategy + ' ' + col)
    return dict(strategy=strategy, passed=True, opportunities=len(a), checked_fields=fields)


def source_inputs():
    manifest = read(SOURCE / 'artifact_manifest.json')
    for name, digest in manifest['files'].items():
        if Path(name).name != name or sha(SOURCE / name) != digest:
            raise ValueError('Frozen source member changed: ' + name)
    cache_manifest = read(G7_CACHE / 'manifest.json')
    if (sha(G7_CACHE / 'provenance.json') != cache_manifest['files']['provenance.json']
            or sha(G7_CACHE / 'provenance.json') != G7_PROVENANCE_SHA256):
        raise ValueError('G7 source configuration changed')
    proof = read(G7_CACHE / 'provenance.json')
    days = proof['periods']['all']
    if len(days) != 59 or days[0] != '2026-07-29' or days[-1] != '2026-10-27':
        raise ValueError('Unexpected frozen calendar')
    books = {}
    for strategy, filename in FILES.items():
        if sha(SOURCE / filename) != SOURCE_HASHES[strategy]:
            raise ValueError('Pinned accepted source changed: ' + strategy)
        books[strategy] = pd.read_csv(SOURCE / filename, low_memory=False)
        if not set(books[strategy].day.astype(str).str[:10]).issubset(days):
            raise ValueError('Ledger day escaped frozen calendar')
    return books, days, proof['base_config']


def run(output_dir, *, publish_dashboard=False, config_path=None):
    import strategy_slot_selection as policy
    output_dir = Path(output_dir).resolve()
    if output_dir.exists():
        raise FileExistsError('Fresh output directory required: ' + str(output_dir))
    output_dir.mkdir(parents=True)
    snapshot = policy.policy_payload()
    initial_fingerprint = policy.policy_fingerprint()
    baseline, days, base_config = source_inputs()
    periods = dict(all=days, october=[d for d in days if d.startswith('2026-10')])
    results = []
    controls = []
    daily = []
    slot_rows = []
    changes = []
    final_books = {}
    for strategy, source in baseline.items():
        enabled = policy.enabled_setup_ids(strategy)
        for cost in COSTS:
            all_ids = frozenset(source.setup_id.unique())
            control = apply_slot_policy(source, strategy, all_ids, base_config, cost)
            if cost == 5.:
                controls.append(verify_control(control, source, strategy))
            result = apply_slot_policy(source, strategy, enabled, base_config, cost)
            result['slot_policy_fingerprint'] = policy.policy_fingerprint(strategy)
            result['source_strategy_version'] = result.strategy_version
            result['strategy_version'] = VERSION + '-' + strategy
            result['strategy_fingerprint'] = policy.policy_fingerprint(strategy)
            for period, calendar in periods.items():
                for label, book in [('baseline', control), ('selected_slots', result)]:
                    results.append(dict(strategy=strategy, cost_bps=cost, period=period,
                                        variant=label, **metrics(book, calendar)))
            if cost == 5.:
                final_books[strategy] = result
                result.to_csv(output_dir / (strategy.lower() + '_portfolio_trades.csv'), index=False)
                for day in days:
                    for label, book in [('baseline', control), ('selected_slots', result)]:
                        daily.append(dict(strategy=strategy, variant=label, day=day, **metrics(book, [day])))
                for setup_id, part in result.groupby('setup_id'):
                    slot_rows.append(dict(strategy=strategy, setup_id=setup_id,
                                          enabled=setup_id in enabled, **metrics(part, days)))
                old = source.set_index('sid')
                new = result.set_index('sid').loc[old.index]
                old_x, new_x = flags(old.portfolio_executed), flags(new.portfolio_executed)
                for sid in old.index[old_x.ne(new_x)]:
                    changes.append(dict(strategy=strategy, sid=int(sid), day=str(old.loc[sid, 'day']),
                        setup_id=str(old.loc[sid, 'setup_id']), tradingsymbol=str(old.loc[sid, 'tradingsymbol']),
                        baseline_executed=bool(old_x.loc[sid]), selected_executed=bool(new_x.loc[sid]),
                        baseline_net_pnl_rs=float(old.loc[sid, 'portfolio_net_profit_rupees']),
                        selected_net_pnl_rs=float(new.loc[sid, 'portfolio_net_profit_rupees'])))
        print(strategy, 'selected slots', len(enabled), metrics(final_books[strategy], days), flush=True)
    if policy.policy_fingerprint() != initial_fingerprint:
        raise ValueError('Slot policy changed during replay')
    table = pd.DataFrame(results)
    table.to_csv(output_dir / 'overall_comparison.csv', index=False)
    daywise = pd.DataFrame(daily)
    daywise.to_csv(output_dir / 'daywise_comparison.csv', index=False)
    daywise.loc[daywise.day.str.startswith('2026-10')].to_csv(output_dir / 'october_daywise_comparison.csv', index=False)
    # Stable legacy names remain available to callers of the G7 main entrypoint.
    final_books['G7_V4_LOCK2'].to_csv(output_dir / 'portfolio_trades.csv', index=False)
    final_books['G3'].to_csv(output_dir / 'g3_reference_portfolio_trades.csv', index=False)
    for strategy, name in [('G7_V4_LOCK2', 'daywise_results.csv'), ('G3', 'g3_reference_daywise_results.csv')]:
        daywise.loc[daywise.strategy.eq(strategy) & daywise.variant.eq('selected_slots')].to_csv(output_dir / name, index=False)
    pd.DataFrame(slot_rows).to_csv(output_dir / 'selected_slot_results.csv', index=False)
    pd.DataFrame(changes).to_csv(output_dir / 'changed_admissions.csv', index=False)
    dump(output_dir / 'baseline_reproduction.json', controls)
    dump(output_dir / 'slot_policy.json', snapshot)
    dump(output_dir / 'config.json', read(config_path) if config_path else snapshot)
    summary = dict(schema_version=VERSION, first_date=days[0], last_date=days[-1], sessions=len(days),
        days=days, scope='BACKTEST_ONLY', execution_authority=False,
        slot_policy_fingerprint=initial_fingerprint, baseline_reproduction_passed=all(x['passed'] for x in controls),
        selection_scope=snapshot.get('selection_scope', snapshot.get('selection_basis')),
        selection_is_retrospective=True, independent_holdout=False,
        costs_bps=list(COSTS), overall_comparison=table.to_dict('records'),
        portfolio_metrics=table.loc[table.strategy.eq('G7_V4_LOCK2') & table.variant.eq('selected_slots') & table.cost_bps.eq(5.)].to_dict('records'),
        baseline_portfolio_metrics=table.loc[table.strategy.eq('G7_V4_LOCK2') & table.variant.eq('baseline') & table.cost_bps.eq(5.)].to_dict('records'),
        g3_reference_portfolio_metrics=table.loc[table.strategy.eq('G3') & table.variant.eq('selected_slots') & table.cost_bps.eq(5.)].to_dict('records'),
        selection_note='Every represented slot/direction pair uses the historical >=70% cutoff; zero-trade slots are excluded. Replay results reuse the selection history and are not independently validated.',
        execution_note='Every selected opportunity retained. Frozen fills/exits reused; capital, same-symbol admission and G7 two-realized-loss lock freshly recomputed.',
        limits=['One-minute OHLC execution; modeled costs, no exact spread or queue replay.',
                'Frozen comparison ends 2026-10-27; 2026-10-28 is outside this comparison.',
                'Selection has no minimum trade count beyond at least one execution.',
                'Upstream reconstructed historical universe retains its existing availability limitations.'])
    dump(output_dir / 'summary.json', summary)
    input_paths = [SOURCE / 'artifact_manifest.json', G7_CACHE / 'manifest.json', G7_CACHE / 'provenance.json',
                   Path(__file__), Path(policy.__file__), *[SOURCE / f for f in FILES.values()]]
    if config_path:
        input_paths.append(Path(config_path))
        summary['variant_id'] = read(config_path)['variant_id']
        dump(output_dir / 'summary.json', summary)
    dump(output_dir / 'source_provenance.json', dict(input_and_code_sha256={str(p):sha(p) for p in input_paths},
         enabled_setup_ids={s: sorted(policy.enabled_setup_ids(s)) for s in FILES}))
    dump(output_dir / 'artifact_manifest.json', dict(scope='BACKTEST_ONLY', execution_authority=False,
         files={p.name:sha(p) for p in sorted(output_dir.iterdir()) if p.is_file() and p.name != 'artifact_manifest.json'}))
    if publish_dashboard:
        from dashboard_slot_selection import publish_replay
        publication = publish_replay(output_dir, books=final_books, days=days)
        print('DASHBOARD PUBLICATION', publication, flush=True)
    return dict(output_dir=str(output_dir), summary=summary, books=final_books, days=days)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output-dir', type=Path)
    parser.add_argument('--publish-dashboard', action='store_true')
    args = parser.parse_args(argv)
    out = args.output_dir or ROOT / 'outputs/slot_win70_rollout_20261028/replay' / datetime.now().strftime('%Y%m%dT%H%M%S%f')
    run(out, publish_dashboard=args.publish_dashboard)


if __name__ == '__main__':
    main()
