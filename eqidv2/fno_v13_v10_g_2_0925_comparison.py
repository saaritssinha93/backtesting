"""Hash-verified historical comparison of the opt-in G2 09:25 LONG exception.

Research only. Raw rejected observations are reconsidered; no hindsight winner
watchlist is used. Original selections retain priority. No broker connections.
"""
from __future__ import annotations

from datetime import date, datetime
from pathlib import Path

import numpy as np
import pandas as pd

import fno_v13_v10_g_2_backtest as g2
import fno_v13_v10_g_2_extend_results as ext
import fno_v13_v10_g_daily_replay as replay
import fno_oi_hybrid_data as hybrid


def snapshot_for(day, verified):
    run, result = ext._latest_successful_run(ext.DEFAULT_DAILY_ROOT, day)
    manifest_path, source_day = ext._snapshot_for_day(ext.DEFAULT_DAILY_ROOT, day, run, result)
    if str(manifest_path) not in verified:
        manifest = g2.read_json(manifest_path)
        if manifest.get('complete') is not True:
            raise ValueError('Incomplete snapshot')
        replay._verify_input_snapshot(manifest_path.parent, manifest)
        verified[str(manifest_path)] = g2.sha256(manifest_path)
    return run, result, manifest_path.parent


def rebuild_0925(day, run, snapshot):
    """Older Sep24 replay has no raw feature ledger; rebuild from sealed inputs."""
    manifest = g2.read_json(run / 'source_manifest.json')
    record = next(r for r in manifest['sources'] if r['role'] == 'DATED_UNIVERSE')
    universe_path = Path(record['path'])
    if g2.sha256(universe_path) != record['sha256']:
        raise ValueError('Dated universe changed')
    universe = pd.read_parquet(universe_path)
    coverage = pd.read_csv(run / 'coverage.csv')
    if coverage[['missing_equity_minutes', 'missing_futures_bars']].ne(0).any().any():
        raise ValueError('Older replay coverage requires explicit exclusions')
    rows = []
    problems = []
    stamp = pd.Timestamp(f'{day} 09:25', tz='Asia/Kolkata')
    for i, symbol in enumerate(coverage.symbol):
        contract = universe.loc[universe.equity_symbol.eq(symbol)].iloc[0]
        future_symbol = contract.futures_tradingsymbol
        minute = replay._load_minute(hybrid.equity_one_minute_path(symbol, snapshot / 'equity_1m'), day, problems, symbol)
        future = replay._load_future(snapshot / 'futures_5m' /
            f'{ext.common.safe_contract_stem(future_symbol)}_5minute.parquet', day, problems, symbol)
        # This exception does not consume EMA. Reconstruct the finite-lookback
        # price/volume features with ample *past-only* warmup (20 prior 5m bars),
        # avoiding unnecessary aggregation of years of unused EMA history.
        five_input = hybrid.aggregate_equity_one_minute_to_five_minute(
            minute.loc[minute.ts.le(stamp)].tail(1000))
        if len(five_input.loc[five_input.ts.lt(stamp)]) < 20:
            raise ValueError(f'Insufficient finite-lookback warmup: {symbol}')
        five = hybrid.join_equity_price_with_futures_oi(five_input, future)
        observed = five.loc[five.ts.eq(stamp)]
        if len(observed) != 1:
            raise ValueError(f'Missing exact 09:25 candle: {symbol}')
        row = observed.iloc[0].to_dict()
        # Never publish the warmup-truncated EMA as a real full-history EMA.
        for field in ('ema9','ema20','ema50'):
            row[field] = np.nan
        confirmation_ts = stamp + pd.Timedelta(minutes=1)
        confirm = minute.loc[minute.ts.eq(confirmation_ts)]
        if len(confirm) != 1:
            raise ValueError(f'Missing confirmation: {symbol}')
        c = confirm.iloc[0]
        # Daily replay explicitly promotes OHLC to float64 before ratios.
        op, high, low, close = (float(c[f]) for f in ('open','high','low','close'))
        span = high-low
        denominator = minute.volume.shift(1).rolling(20, min_periods=5).mean().loc[c.name]
        row.update(day=day, signal_ts=stamp, confirmation_ts=confirmation_ts,
                   tradingsymbol=symbol, futures_tradingsymbol=future_symbol,
                   signal_close=row['close'], hhmm_int=925,
                   body_ratio=abs(close-op)/span if span > 0 else np.nan,
                   v9_1m_upper_wick_ratio=(high-max(op,close))/span if span > 0 else np.nan,
                   v9_1m_volume_ratio=c.volume/denominator if denominator > 0 else np.nan,
                   v9_1m_feature_ts=confirmation_ts, v9_exact_confirmation_present=True,
                   confirmation_source_flagged=any(str(c.get(f, False)).lower() in ('true','1','1.0')
                       for f in ('gap_filled','opening_snapshot','provisional_stale')))
        row.update({f'confirmation_{f}': c[f] for f in ('open','high','low','close','volume')})
        rows.append(row)
        if i % 40 == 0:
            print(f'Rebuilding {day}: {i+1}/{len(coverage)} stocks', flush=True)
    if problems:
        raise ValueError(problems)
    raw = pd.DataFrame(rows)
    # Independent parity against the older published strict candidate features.
    strict = pd.read_csv(run / 'candidate_signals.csv')
    strict = strict.loc[strict.hhmm_int.eq(925)]
    fields = ['oi_change_pct','price_change_pct','volume_ratio','body_ratio','v9_1m_volume_ratio']
    check = strict.merge(raw, on='tradingsymbol', suffixes=('_old','_new'), validate='one_to_one')
    if len(check) != len(strict):
        raise ValueError('Rebuilt candidate coverage mismatch')
    for field in fields:
        if not np.allclose(check[field+'_old'], check[field+'_new'], atol=1e-8, rtol=1e-8, equal_nan=True):
            raise ValueError(f'Rebuilt feature parity failed: {field}')
    return raw


def base_paths(orders, sealed_paths, raw_all, snapshot):
    """Use sealed original paths, validate later-snapshot prices before additions."""
    paths = dict(sealed_paths)
    parity_count = 0
    for symbol, group in orders.groupby('tradingsymbol'):
        problems = []
        minute = replay._load_minute(hybrid.equity_one_minute_path(symbol, snapshot / 'equity_1m'),
                                    max(group.day), problems, symbol)
        if problems:
            raise ValueError(problems)
        for row in group.itertuples():
            confirm = pd.Timestamp(row.confirmation_ts)
            selected = minute.loc[minute.ts.gt(confirm) & minute.ts.le(replay._cutoff(row.day))]
            rebuilt = {'timestamp_ns': selected.ts.astype('int64').to_numpy(),
                       **{f: selected[f].to_numpy(float) for f in ('open','high','low','close')}}
            if int(row.sid) in sealed_paths:
                for field, values in sealed_paths[int(row.sid)].items():
                    if len(values) != len(rebuilt[field]) or not np.allclose(values, rebuilt[field], atol=1e-8, rtol=0):
                        raise ValueError(f'Sealed path parity failed: {row.day} {symbol} {field}')
                parity_count += 1
            else:
                # Validate every completed intraday five-minute OHLCV, not only entry.
                rebuilt_five = hybrid.aggregate_equity_one_minute_to_five_minute(
                    minute.loc[minute.ts.dt.date.eq(row.day)])
                reference = raw_all.loc[raw_all.tradingsymbol.eq(symbol)
                    & pd.to_datetime(raw_all.day).dt.date.eq(row.day)]
                check = reference.merge(rebuilt_five, left_on='signal_ts', right_on='ts',
                                        suffixes=('_sealed','_snapshot'), validate='one_to_one')
                if len(check) != len(reference) or len(check) < 70:
                    raise ValueError(f'New-path five-minute coverage mismatch: {symbol} {row.day}')
                for field in ('open','high','low','close','volume'):
                    if not np.allclose(check[field+'_sealed'], check[field+'_snapshot'], atol=1e-8, rtol=0):
                        raise ValueError(f'New-path price parity failed: {symbol} {row.day} {field}')
                paths[int(row.sid)] = rebuilt
    g2.g.v9.validate_paths(orders, paths)
    print(f'Base paths verified; {parity_count} original paths exactly reproduced', flush=True)
    return paths


def simulate(orders, paths, source, segment):
    orders = ext._apply_retained_g_exits(orders, source)
    g2.g.v9.validate_paths(orders, paths)
    trades = g2.simulate_staged(orders, paths, cost_bps=source['cost_bps'], max_entry_delay_minutes=10)
    for name, value in [('filled',False),('entry_ts',pd.NaT),('exit_ts',pd.NaT),
                        ('gross_return_pct',np.nan),('net_return_pct',np.nan),('cost_pct',np.nan)]:
        if name not in trades:
            trades[name] = value
    base = ext._portfolio_config(source)
    trades = g2.g.v9.v5.apply_fixed_capital_model(trades, base.capital_per_entry_rupees, base.leverage_factor)
    trades['segment'] = segment
    trades['comparison_key'] = segment+':'+trades.sid.astype(str)+':'+trades.setup_id.astype(str)
    sign = np.where(trades.side.eq('LONG'), 1., -1.)
    entry = trades.get('entry_price', pd.Series(np.nan, index=trades.index))
    trades['initial_stop_price'] = entry*(1-sign*g2.INITIAL_STOP_PCT/100)
    trades['tightened_stop_price'] = entry*(1-sign*g2.TIGHTENED_STOP_PCT/100)
    trades['target_price'] = entry*(1+sign*trades.native_target_pct/100)
    trades['tighten_after_minutes'] = g2.TIGHTEN_AFTER_MINUTES
    return trades


def metrics(ledger, days):
    result = g2.g.r.metric(ledger, days)
    executed = ledger.loc[ledger.portfolio_executed.eq(True)]
    daily = g2.build_breakdowns(ledger, days)['daily']
    result.update(gross_profit_rupees=float(executed.portfolio_gross_profit_rupees.sum()),
                  cost_rupees=float(executed.portfolio_cost_rupees.sum()),
                  positive_days=int(daily.net_pnl_rupees.gt(0).sum()), sessions=len(days))
    return result


def run(output=None, *, source_bundle=g2.DEFAULT_SOURCE_BUNDLE,
        source_g_config=g2.DEFAULT_G_CONFIG, include_extensions=False):
    output = Path(output) if output else g2.DEFAULT_OUTPUT.parent / f'run_{datetime.now():%Y%m%d_%H%M%S}_relaxed0925_comparison'
    if output.exists():
        raise FileExistsError('Fresh output directory required')
    print('Verifying sealed G2 baseline and raw observations', flush=True)
    dataset = g2.load_bundle(Path(source_bundle), Path(source_g_config))
    source = dataset['source_g']
    raw = pd.read_parquet(dataset['source'] / 'dataset/all_5m_features.parquet')
    original = dataset['orders'].copy()
    relaxed, audit = g2.apply_relaxed_0925_long(original, raw.loc[raw.hhmm_int.eq(925)])
    print(f'Base selected orders: {len(original)} -> {len(relaxed)}', flush=True)
    verified = {}
    _, _, snapshot = snapshot_for(date(2026,9,25), verified)
    paths = base_paths(relaxed, dataset['paths'], raw, snapshot)
    frames = {'original':[simulate(original, paths, source, 'base')],
              'relaxed':[simulate(relaxed, paths, source, 'base')]}
    audits = [audit]
    days = list(dataset['days'])
    evidence = []
    if include_extensions:
        for day in (*ext.COMPLETE_EXTENSION_DAYS, date(2026,10,5)):
            print(f'Checking complete daily session {day}', flush=True)
            run_path, result, snapshot = snapshot_for(day, verified)
            manifest = g2.read_json(run_path / 'source_manifest.json')
            if manifest['frozen_config_sha256'] != g2.sha256(Path(source_g_config)):
                raise ValueError('Daily source configuration drift')
            feature_path = run_path / 'feature_ledger.csv'
            if feature_path.exists():
                feature_manifest = g2.read_json(run_path / 'feature_ledger.csv.manifest.json')
                if g2.sha256(feature_path) != feature_manifest['artifact_sha256']:
                    raise ValueError('Feature ledger hash mismatch')
                raw_day = pd.read_csv(feature_path)
                if not raw_day.session_date.eq(str(day)).all():
                    raise ValueError('Feature ledger date mismatch')
            else:
                raw_day = rebuild_0925(day, run_path, snapshot)
            candidates = pd.read_csv(run_path / 'candidate_signals.csv')
            candidates['day'] = pd.to_datetime(candidates.day).dt.date
            original = g2.g.select_orders(candidates, dataset['v9_config'],
                g2.g.SelectionChange(**source['selection_change']), core_first=source['core_first'],
                morning_slots=source.get('morning_slots',False),
                two_bar_continuation=source.get('two_bar_continuation',False))
            if g2._selection_keys(original) != g2._selection_keys(pd.read_csv(run_path / 'selected_orders.csv')):
                raise ValueError('Daily original selection parity failure')
            relaxed, audit = g2.apply_relaxed_0925_long(original, raw_day)
            paths = ext._selected_paths(relaxed, day, snapshot)
            # Check the recorded G ledger to validate paths and all unchanged assumptions.
            control_orders = ext._apply_retained_g_exits(original, source)
            official = pd.read_csv(run_path / 'portfolio_trades.csv')
            if len(original):
                _, control, _ = ext._simulate(control_orders, paths, dataset['v9_config'], stop_pct=None)
                ext._assert_daily_parity(control, official, day)
            elif len(official):
                raise ValueError('Empty original selection but nonempty official ledger')
            for name, orders in [('original',original),('relaxed',relaxed)]:
                frames[name].append(simulate(orders, paths, source, str(day)))
            audits.append(audit)
            days.append(day)
            evidence.append(dict(day=str(day), source_run=str(run_path), raw_features_rebuilt=not feature_path.exists()))
    output.mkdir(parents=True)
    reports, ledgers, daily_frames = {}, {}, {}
    for name, parts in frames.items():
        trades = pd.concat([p for p in parts if len(p)], ignore_index=True, sort=False)
        trades['day'] = pd.to_datetime(trades.day).dt.date
        ledger, _ = g2.g.v9.v6.apply_portfolio_constraints(trades, dataset['v9_config'].portfolio_config())
        ledgers[name] = ledger
        reports[name] = metrics(ledger, days)
        if name == 'original' and include_extensions and len(days) == 44:
            # Independent published staged-stop control, not a target for the new arm.
            if (reports[name]['trades'] != 85 or reports[name]['wins'] != 59
                or not np.isclose(reports[name]['net_profit_rupees'], 229917.0395538971, atol=1e-6, rtol=0)):
                raise ValueError('Published original G2 baseline parity failed')
        trades.to_csv(output / f'{name}_selected_orders.csv', index=False)
        ledger.to_csv(output / f'{name}_portfolio_trades.csv', index=False)
        breakdowns = g2.build_breakdowns(ledger, days)
        for kind, table in breakdowns.items():
            table.to_csv(output / f'{name}_{kind}_results.csv', index=False)
        daily_frames[name] = breakdowns['daily'].set_index('day').add_prefix(name+'_')
    daily = daily_frames['original'].join(daily_frames['relaxed'])
    daily['net_change_rupees'] = daily.relaxed_net_pnl_rupees-daily.original_net_pnl_rupees
    daily.to_csv(output / 'daywise_comparison.csv')
    added = ledgers['relaxed'].loc[ledgers['relaxed'].relaxed_0925_added.eq(True)]
    added.to_csv(output / 'added_trades.csv', index=False)
    original_done = ledgers['original'].loc[ledgers['original'].portfolio_executed.eq(True)]
    relaxed_done = ledgers['relaxed'].loc[ledgers['relaxed'].portfolio_executed.eq(True)]
    displaced = original_done.loc[~original_done.comparison_key.isin(relaxed_done.comparison_key)]
    displaced.to_csv(output / 'displaced_original_trades.csv', index=False)
    pd.concat(audits, ignore_index=True, sort=False).to_csv(output / 'raw_0925_audit.csv', index=False)
    before = {}
    for name, ledger in ledgers.items():
        earlier = ledger.loc[ledger.day.lt(date(2026,10,5))]
        before[name] = metrics(earlier, [d for d in days if d < date(2026,10,5)])
    report = dict(parameters=g2.RELAXED_0925_LONG, staged_stop=g2.config(source)['stop_change'],
        summaries=reports, excluding_oct5=before, added_orders=len(added), displaced_original_fills=len(displaced),
        sessions=[str(d) for d in days], daily_sources=evidence, verified_snapshot_manifests=verified,
        source_bundle=str(dataset['source']), source_config_sha256=g2.sha256(Path(source_g_config)),
        script_sha256=g2.sha256(Path(__file__)), g2_script_sha256=g2.sha256(Path(g2.__file__)),
        original_G_script_sha256=g2.sha256(Path(g2.g.__file__)),
        limitations=['Thresholds chosen with knowledge of October 5 winner; retrospective, not out-of-sample validation.',
            'One 09:25 LONG order per day; all original selections reserved, no top-winner watchlist.',
            'Only recorded complete sessions included; October 1 excluded for incomplete OI coverage.',
            'Flat 5 bps round-trip cost, fixed notional exposure, no separate slippage/market-impact model.',
            'Stops evaluated on minute bars, stop-first for ambiguous bars, unchanged 15:15 research cutoff.',
            'No live strategy, configuration, scheduler or broker order changed.'])
    g2.dump_json(output / 'summary.json', ext._finite_json(report))
    print(reports, flush=True)
    print(f'RESULTS: {output}', flush=True)
    return report
