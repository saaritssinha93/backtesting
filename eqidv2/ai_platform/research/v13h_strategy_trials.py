"""Fixed, research-only V13-V10-H strategy trials on a verified G bundle.

Each trial changes one decision or execution rule. The historical source is
already known to the researcher, so results are exploratory rather than an
untouched validation. No broker or live configuration is imported here.
"""
from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from dataclasses import replace
from pathlib import Path
import json
import math

import numpy as np
import pandas as pd

from . import v13h_research as h
from .v13h_execution import ExecutionModel, daily_metrics, metrics, minute_equity, simulate_portfolio


TRIALS = (
    'extension_ema9', 'extension_vwap', 'stop_atr', 'stop_structure',
    'retest', 'failed_breakdown_exit', 'breadth_regime',
    'relative_strength', 'sector_cap', 'vix_regime',
)
RULES = {
    'extension_ema9': 'Signed entry-to-EMA9 extension <= 1.0 completed 5m ATR; no slot refill',
    'extension_vwap': 'Signed entry-to-session-VWAP extension <= 1.0 completed 5m ATR; no slot refill',
    'stop_atr': 'Stop one completed 5m ATR from actual fill; planned risk <= INR 3000',
    'stop_structure': 'Stop one tick beyond completed signal 5m candle extreme; planned risk <= INR 3000',
    'retest': 'After first trigger, completed later 1m candle touches level and rejects; enter next 1m open before original deadline',
    'failed_breakdown_exit': 'After fill, completed 1m close reclaims confirmation extreme; exit next 1m open',
    'sector_cap': 'Retain at most one G selected symbol per dated sector per setup slot; no slot refill',
    'breadth_regime': 'Short only if observed-universe advancing fraction < 0.5; long only if > 0.5',
    'relative_strength': 'Directional stock completed 5m return must exceed exact same-bar NIFTY futures return',
    'vix_regime': 'Only trade when a same-session India VIX value was available by decision time and below 20',
}


def _context_for_orders(source: dict) -> pd.DataFrame:
    bars = pd.read_parquet(source['root'] / 'dataset/all_5m_features.parquet')
    context = h.context_features(bars)
    needed = context[['signal_ts', 'tradingsymbol', 'contract_month', 'atr_14_5m',
                      'observed_universe_advance_fraction', 'high', 'low', 'available_at']].copy()
    orders = source['orders'].merge(needed, on=['signal_ts', 'tradingsymbol', 'contract_month'],
                                   how='left', validate='many_to_one')
    decision = pd.to_datetime(orders.signal_ts, utc=True)
    available = pd.to_datetime(orders.available_at, utc=True)
    if available.isna().any() or available.gt(decision).any():
        raise ValueError('Missing or future five-minute context for a selected order')
    missing = orders[['v9_5m_vwap', 'high', 'low']].isna().sum()
    if missing.any():
        raise ValueError(f'Selected order lacks required completed five-minute context: {missing.to_dict()}')
    if (orders.atr_14_5m.notna() & orders.atr_14_5m.le(0)).any():
        raise ValueError('Selected order has nonpositive ATR')
    return orders


def _attach_verified_index(orders: pd.DataFrame, source: dict) -> tuple[pd.DataFrame, str, dict]:
    manifest = source['dataset']
    frames, hashes = [], {}
    months = set(orders.contract_month.astype(str))
    for record in manifest['sources']:
        original = Path(record['path'])
        if not (original.name.startswith('NIFTY') and original.name.endswith('FUT_5minute.parquet')):
            continue
        month = original.name.removeprefix('NIFTY').removesuffix('FUT_5minute.parquet')
        if month not in months:
            continue
        snapshot = source['root'] / 'index_5m' / original.name
        path = snapshot if snapshot.is_file() else original
        if not path.is_file() or h.sha(path) != record['sha256']:
            return orders, 'NIFTY_SOURCE_HASH_MISMATCH', {}
        frame = pd.read_parquet(path, columns=['timestamp', 'close']).sort_values('timestamp')
        frame['nifty_return_pct'] = frame.close.pct_change(fill_method=None) * 100
        frame['contract_month'] = month
        frame = frame.rename(columns={'timestamp': 'signal_ts'})
        frames.append(frame[['signal_ts', 'contract_month', 'nifty_return_pct']])
        hashes[str(path.resolve())] = record['sha256']
    if not frames or set(pd.concat(frames).contract_month.astype(str)) != months:
        return orders, 'NIFTY_MONTH_COVERAGE_MISSING', {}
    series = pd.concat(frames, ignore_index=True)
    series['signal_ts'] = pd.to_datetime(series.signal_ts, utc=True)
    if series.duplicated(['signal_ts', 'contract_month']).any():
        raise ValueError('Duplicate NIFTY five-minute index bars')
    copy = orders.copy()
    copy['signal_ts'] = pd.to_datetime(copy.signal_ts, utc=True)
    copy = copy.merge(series, on=['signal_ts', 'contract_month'], how='left', validate='many_to_one')
    if copy.nifty_return_pct.isna().any():
        return orders, 'NIFTY_EXACT_BAR_COVERAGE_MISSING', {}
    return copy, 'AVAILABLE_VERIFIED', hashes


def _sector_lookup(path: Path, orders: pd.DataFrame) -> tuple[dict, str]:
    # A dated, verified map is required; a present-day static mapping can
    # silently misclassify historical symbols or restructurings.
    if not path.is_file():
        raise ValueError(f'Sector mapping unavailable: {path}')
    mapping = pd.read_csv(path, dtype=str)
    required = {'day', 'tradingsymbol', 'sector'}
    if not required.issubset(mapping):
        raise ValueError('Sector mapping needs day, tradingsymbol, sector')
    if mapping[['day', 'tradingsymbol']].duplicated().any() or mapping[list(required)].isna().any().any():
        raise ValueError('Incomplete or duplicate dated sector mapping')
    lookup = {(r.day, r.tradingsymbol): r.sector for r in mapping.itertuples()}
    missing = [(str(r.day), r.tradingsymbol) for r in orders.itertuples()
               if (str(r.day), r.tradingsymbol) not in lookup]
    if missing:
        raise ValueError(f'Dated sectors missing for {len(missing)} G selections')
    return lookup, h.sha(path)


def _attach_vix(orders: pd.DataFrame, path: Path) -> tuple[pd.DataFrame, str]:
    if not path.is_file():
        raise ValueError(f'VIX history unavailable: {path}')
    frame = pd.read_csv(path)
    required = {'observed_at', 'available_at', 'india_vix'}
    if not required.issubset(frame):
        raise ValueError('VIX CSV needs observed_at, available_at, india_vix')
    for field in ('observed_at', 'available_at'):
        if not frame[field].astype(str).str.contains(r'(?:Z|[+-]\d\d:\d\d)$', regex=True).all():
            raise ValueError(f'VIX {field} requires an explicit timezone offset')
    observed = pd.to_datetime(frame.observed_at, utc=True, errors='coerce')
    available = pd.to_datetime(frame.available_at, utc=True, errors='coerce')
    values = pd.to_numeric(frame.india_vix, errors='coerce')
    if (observed.isna().any() or available.isna().any() or
        available.lt(observed).any() or not np.isfinite(values).all() or values.le(0).any()):
        raise ValueError('VIX history has invalid values or noncausal timestamps')
    frame = pd.DataFrame(dict(available_at=available, india_vix=values))
    frame['day'] = frame.available_at.dt.tz_convert('Asia/Kolkata').dt.date.astype(str)
    if frame.duplicated(['day', 'available_at']).any():
        raise ValueError('Duplicate VIX availability timestamps')
    result = orders.copy()
    result['decision_at'] = pd.to_datetime(result.confirmation_ts, utc=True)
    selections = []
    for day, group in result.groupby(result.day.astype(str), sort=False):
        quotes = frame.loc[frame.day.eq(day)].sort_values('available_at')
        if quotes.empty:
            raise ValueError(f'No same-session VIX quote for {day}')
        matched = pd.merge_asof(group.sort_values('decision_at'),
                                quotes[['available_at', 'india_vix']],
                                left_on='decision_at', right_on='available_at',
                                direction='backward')
        if matched.india_vix.isna().any():
            raise ValueError(f'No VIX quote available at decision for {day}')
        selections.append(matched)
    result = pd.concat(selections, ignore_index=True).sort_values('sid')
    return result, h.sha(path)


def _prepare_trial(orders: pd.DataFrame, name: str, model: ExecutionModel,
                   sector_lookup: dict | None = None) -> tuple[pd.DataFrame, pd.DataFrame]:
    changed = orders.copy()
    decision = changed[['sid', 'day', 'setup_id', 'tradingsymbol', 'side']].copy()
    decision['decision'] = 'RETAINED'
    decision['extension_atr'] = np.nan
    sign = np.where(changed.side.eq('LONG'), 1., -1.)
    if name in ('extension_ema9', 'extension_vwap'):
        anchor = changed.v9_5m_ema9 if name == 'extension_ema9' else changed.v9_5m_vwap
        extension = sign * (changed.trigger - anchor) / changed.atr_14_5m
        decision.loc[~np.isfinite(extension), 'decision'] = 'NO_ATR_G_RULE_RETAINED'
        decision['extension_atr'] = extension
        decision.loc[extension.gt(1.), 'decision'] = 'FILTERED_EXTENSION'
        changed = changed.loc[~extension.gt(1.)].copy()
    elif name == 'stop_atr':
        changed['research_stop_distance'] = changed.atr_14_5m.astype(float)
        decision.loc[changed.atr_14_5m.isna(), 'decision'] = 'NO_ATR_G_RULE_RETAINED'
    elif name == 'stop_structure':
        changed['research_stop_reference'] = np.where(
            changed.side.eq('LONG'), changed.low-model.tick_size, changed.high+model.tick_size)
    elif name == 'retest':
        changed['research_entry_rule'] = 'retest'
    elif name == 'failed_breakdown_exit':
        changed['research_invalidation_reference'] = np.where(
            changed.side.eq('LONG'), changed.confirmation_low, changed.confirmation_high)
    elif name == 'sector_cap':
        if sector_lookup is None:
            raise ValueError('Sector cap requires complete dated sector mapping')
        # Retain G's original core-first selection order by rank, then SID.
        ranked = changed.sort_values(['day', 'setup_id', 'v9_rank_in_setup_day', 'sid'], kind='stable')
        keep, seen = [], set()
        for row in ranked.itertuples():
            group = (str(row.day), row.setup_id, sector_lookup[(str(row.day), row.tradingsymbol)])
            if group not in seen:
                seen.add(group)
                keep.append(int(row.sid))
        decision.loc[~decision.sid.isin(keep), 'decision'] = 'FILTERED_SECTOR_DUPLICATE'
        changed = changed.loc[changed.sid.isin(keep)].copy()
    elif name == 'breadth_regime':
        breadth = changed.observed_universe_advance_fraction
        decision.loc[breadth.isna(), 'decision'] = 'NO_BREADTH_G_RULE_RETAINED'
        rejected = breadth.notna() & np.where(changed.side.eq('LONG'), breadth.le(.5), breadth.ge(.5))
        decision.loc[rejected, 'decision'] = 'FILTERED_BREADTH_REGIME'
        changed = changed.loc[~rejected].copy()
    elif name == 'relative_strength':
        if 'nifty_return_pct' not in changed:
            raise ValueError('Relative strength requires exact verified NIFTY bars')
        edge = sign * (changed.price_change_pct - changed.nifty_return_pct)
        decision['stock_minus_nifty_signed_pct'] = edge
        rejected = edge.le(0)
        decision.loc[rejected, 'decision'] = 'FILTERED_RELATIVE_WEAKNESS'
        changed = changed.loc[~rejected].copy()
    elif name == 'vix_regime':
        if 'india_vix' not in changed or changed.india_vix.isna().any():
            raise ValueError('VIX regime requires verified decision-time history')
        decision['india_vix'] = changed.india_vix.to_numpy()
        rejected = changed.india_vix.ge(20.)
        decision.loc[rejected, 'decision'] = 'FILTERED_HIGH_VIX'
        changed = changed.loc[~rejected].copy()
    else:
        raise ValueError(f'Unknown H strategy trial: {name}')
    return changed.reset_index(drop=True), decision


def _minute_rows(ledger: pd.DataFrame, paths: dict) -> pd.DataFrame:
    records = []
    for row in ledger.loc[ledger.status.eq('EXECUTED')].itertuples():
        path = paths[int(row.sid)]
        sign = 1 if row.side == 'LONG' else -1
        for i in range(int(row.entry_index), int(row.exit_index) + 1):
            closed = i == int(row.exit_index)
            mark = float(row.exit_price if closed else path['close'][i])
            records.append(dict(day=str(row.day), sid=int(row.sid), setup_id=row.setup_id,
                                tradingsymbol=row.tradingsymbol, side=row.side,
                                minute=pd.Timestamp(int(path['timestamp_ns'][i]), tz='UTC').tz_convert('Asia/Kolkata'),
                                entry_price=float(row.entry_price), minute_open=float(path['open'][i]),
                                minute_high=float(path['high'][i]), minute_low=float(path['low'][i]),
                                minute_close=float(path['close'][i]), mark_price=mark,
                                quantity=int(row.quantity), exited=closed,
                                exit_reason=row.exit_reason if closed else '',
                                cumulative_net_rupees=(float(row.net_profit_rupees) if closed else
                                    float(row.quantity)*sign*(mark-float(row.entry_price))-float(row.cost_rupees)/2)))
    return pd.DataFrame(records)


def run(source_run: Path, output_root: Path, *, run_id: str, sector_map: Path | None = None,
        vix_data: Path | None = None,
        model: ExecutionModel | None = None) -> Path:
    model = model or ExecutionModel()
    model.validate()
    if not run_id or any(c not in 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-' for c in run_id):
        raise ValueError('Invalid immutable run ID')
    source_run, output_root = source_run.resolve(), output_root.resolve()
    if output_root == source_run or output_root.is_relative_to(source_run):
        raise ValueError('Trial outputs cannot be inside frozen source')
    out = output_root / 'strategy_trials' / run_id
    if out.exists():
        raise FileExistsError(f'Run already exists: {out}')
    source = h.load_source(source_run)
    orders = _context_for_orders(source)
    indexed_orders, index_status, index_hashes = _attach_verified_index(orders, source)
    vix_orders, vix_sha = (None, None) if vix_data is None else _attach_vix(orders, vix_data)
    paths = source['paths']
    all_days = source['days']
    sector_lookup, sector_sha = (None, None) if sector_map is None else _sector_lookup(sector_map, orders)
    permitted = [name for name in TRIALS
                 if (name != 'sector_cap' or sector_lookup is not None) and
                 (name != 'relative_strength' or index_status == 'AVAILABLE_VERIFIED') and
                 (name != 'vix_regime' or vix_orders is not None)]
    code_path = Path(__file__).resolve()
    hashes = {'strategy_trial_code': h.sha(code_path),
              'execution_code': h.sha(code_path.with_name('v13h_execution.py')),
              'source_manifest': source['hashes']['bundle_manifest.json'],
              'sector_map': sector_sha, 'vix_data': vix_sha,
              'verified_nifty_sources': index_hashes}
    out.mkdir(parents=True, exist_ok=False)
    h.write_json(out / 'experiment.json', dict(schema='V13_V10_H_FIXED_STRATEGY_TRIALS_V1',
        registered_at_utc=datetime.now(timezone.utc).isoformat(), source_through=max(all_days),
        rules={name: RULES[name] for name in permitted}, rejected_trials={
            'sector_cap': 'DATED_SECTOR_MAP_UNAVAILABLE' if sector_lookup is None else None,
            'relative_strength': index_status if index_status != 'AVAILABLE_VERIFIED' else None,
            'vix_regime': 'VERIFIED_DECISION_TIME_VIX_SERIES_UNAVAILABLE' if vix_orders is None else None},
        thresholds={'extension_max_atr': 1.0, 'stop_atr_multiple': 1.0},
        one_change_per_trial=True, no_slot_refill=True, model=model.__dict__, hashes=hashes,
        historical_evidence='EXPLORATORY_REUSED_HISTORY', execution_authority=False))
    reference_orders = orders.copy()
    reference = simulate_portfolio(reference_orders, paths, model, None)
    # Equal-risk reference is necessary for the wider/narrower stop trials.
    risk_reference = simulate_portfolio(reference_orders, paths, model, 3000.)
    controls = {'g_shared': reference, 'risk3000_reference': risk_reference}
    results = {}
    now_ist = pd.Timestamp.now(tz='Asia/Kolkata')
    # Before a session is finalized, report the preceding 14 calendar dates.
    window_end = now_ist.date() if now_ist.hour >= 16 else now_ist.date() - timedelta(days=1)
    last_two_week_start = window_end - timedelta(days=13)
    last_two_week_days = [day for day in all_days
                          if last_two_week_start.isoformat() <= day <= window_end.isoformat()]
    for name in (*controls, *permitted):
        trial_orders, decisions = ((reference_orders, None) if name in controls else
                                   _prepare_trial(indexed_orders if name == 'relative_strength' else
                                                  vix_orders if name == 'vix_regime' else orders,
                                                  name, model, sector_lookup))
        budget = 3000. if name in ('risk3000_reference', 'stop_atr', 'stop_structure') else None
        ledger = controls[name] if name in controls else simulate_portfolio(trial_orders, paths, model, budget)
        folder = out / name
        folder.mkdir()
        ledger.to_parquet(folder / 'trades.parquet', index=False)
        daily = daily_metrics(ledger, all_days)
        daily.to_csv(folder / 'daily.csv', index=False)
        marks = minute_equity(ledger, paths)
        summary = metrics(ledger, daily, marks)
        if decisions is not None:
            decisions.to_parquet(folder / 'decisions.parquet', index=False)
        recent = ledger.loc[ledger.day.astype(str).isin(last_two_week_days)].copy()
        recent.to_csv(folder / 'last_two_weeks_stock_entries.csv', index=False)
        _minute_rows(recent, paths).to_csv(folder / 'last_two_weeks_stock_minutes.csv', index=False)
        result_reference = risk_reference if budget is not None else reference
        reference_daily = daily_metrics(result_reference, all_days)
        summary['comparison_reference'] = 'risk3000_reference' if budget is not None else 'g_shared'
        summary['delta_net_rupees'] = float(summary['net_profit_rupees'] -
            result_reference.loc[result_reference.status.eq('EXECUTED'), 'net_profit_rupees'].sum())
        summary['last_two_week_included_sessions'] = last_two_week_days
        summary['last_two_week_net_rupees'] = float(daily.loc[daily.day.isin(last_two_week_days), 'net_profit_rupees'].sum())
        if name not in controls:
            stresses = {}
            for scenario, stress_model in (
                ('ADVERSE_5BPS_EACH_SIDE', replace(model,
                    entry_slippage_bps=model.entry_slippage_bps + 5,
                    exit_slippage_bps=model.exit_slippage_bps + 5)),
                ('DELAY_1M_ABSOLUTE_EXPIRY', replace(model, delay_minutes=model.delay_minutes + 1)),
            ):
                tested = simulate_portfolio(trial_orders, paths, stress_model, budget)
                control = simulate_portfolio(reference_orders, paths, stress_model, budget)
                net = lambda frame: float(frame.loc[frame.status.eq('EXECUTED'), 'net_profit_rupees'].sum())
                stresses[scenario] = dict(h_net_rupees=net(tested), reference_net_rupees=net(control),
                                          delta_rupees=net(tested)-net(control))
            summary['sensitivity'] = stresses
        results[name] = summary
        if name not in controls:
            paired = daily[['day', 'selected', 'trades', 'net_profit_rupees']].merge(
                reference_daily[['day', 'selected', 'trades', 'net_profit_rupees']], on='day',
                suffixes=('_h', '_reference'), validate='one_to_one')
            paired['delta_rupees'] = paired.net_profit_rupees_h - paired.net_profit_rupees_reference
            paired.to_csv(folder / 'paired_daily.csv', index=False)
            fields = ['sid', 'setup_id', 'day', 'tradingsymbol', 'side', 'status',
                      'entry_ts', 'entry_price', 'exit_ts', 'exit_price',
                      'exit_reason', 'quantity', 'net_profit_rupees']
            attribution = result_reference[fields].merge(
                ledger[fields], on=['sid', 'setup_id', 'day', 'tradingsymbol', 'side'],
                how='outer', suffixes=('_reference', '_h'), validate='one_to_one')
            attribution = attribution.merge(decisions[['sid', 'setup_id', 'decision']],
                                            on=['sid', 'setup_id'], how='left',
                                            validate='one_to_one')
            attribution['delta_net_rupees'] = (attribution.net_profit_rupees_h.fillna(0) -
                                               attribution.net_profit_rupees_reference.fillna(0))
            attribution.to_csv(folder / 'trade_attribution.csv', index=False)
            validation = h.paired_validation(pd.DataFrame(dict(
                day=paired.day, g_net_rupees=paired.net_profit_rupees_reference,
                h_net_rupees=paired.net_profit_rupees_h, delta_rupees=paired.delta_rupees)))
            h.write_json(folder / 'validation.json', validation)
            summary['chronological_folds'] = len(validation['folds'])
            summary['exploratory_mean_daily_delta_interval'] = (
                validation['exploratory_day_block_bootstrap_mean_delta_95pct'])
    recent_daily = pd.DataFrame({'day': last_two_week_days})
    recent_stock = reference.loc[reference.day.astype(str).isin(last_two_week_days),
        ['sid', 'setup_id', 'day', 'tradingsymbol', 'side', 'status', 'entry_ts',
         'entry_price', 'exit_ts', 'exit_price', 'net_profit_rupees']].copy()
    recent_stock = recent_stock.rename(columns={field: 'g_' + field for field in
        ('status', 'entry_ts', 'entry_price', 'exit_ts', 'exit_price', 'net_profit_rupees')})
    for name in (*controls, *permitted):
        trial_folder = out / name
        daily = pd.read_csv(trial_folder / 'daily.csv')
        recent_daily = recent_daily.merge(
            daily[['day', 'trades', 'net_profit_rupees']].rename(columns={
                'trades': f'{name}_trades', 'net_profit_rupees': f'{name}_net_rupees'}),
            on='day', how='left', validate='one_to_one')
        if name == 'g_shared':
            continue
        ledger = pd.read_parquet(trial_folder / 'trades.parquet')
        fields = ['sid', 'setup_id', 'status', 'entry_ts', 'entry_price',
                  'exit_ts', 'exit_price', 'net_profit_rupees']
        renamed = ledger[fields].rename(columns={field: f'{name}_{field}' for field in fields[2:]})
        recent_stock = recent_stock.merge(renamed, on=['sid', 'setup_id'],
                                          how='left', validate='one_to_one')
    recent_daily.to_csv(out / 'last_two_weeks_daily_comparison.csv', index=False)
    recent_stock.to_csv(out / 'last_two_weeks_stock_comparison.csv', index=False)
    # Rehash all source artifacts after simulation to detect a changing bundle.
    for name, expected in source['hashes'].items():
        if h.sha(h.safe_child(source_run, name)) != expected:
            raise ValueError(f'Frozen source changed during trials: {name}')
    if h.sha(code_path) != hashes['strategy_trial_code'] or h.sha(code_path.with_name('v13h_execution.py')) != hashes['execution_code']:
        raise ValueError('Research code changed during trial run')
    for path, expected in index_hashes.items():
        if h.sha(Path(path)) != expected:
            raise ValueError(f'Verified NIFTY source changed during trials: {path}')
    if vix_data is not None and h.sha(vix_data) != vix_sha:
        raise ValueError('VIX history changed during trials')
    h.write_json(out / 'results.json', dict(source_through=max(all_days), sessions=len(all_days),
        last_two_week_window=[last_two_week_start.isoformat(), window_end.isoformat()],
        last_two_week_included_sessions=last_two_week_days, trials=results,
        excluded_trials={'sector_cap': 'DATED_SECTOR_MAP_UNAVAILABLE' if sector_lookup is None else None,
                         'relative_strength': index_status if index_status != 'AVAILABLE_VERIFIED' else None,
                         'vix_regime': 'NO_VERIFIED_ASOF_VIX_SERIES' if vix_orders is None else None},
        promotion_eligible=False, execution_authority=False))
    report = ['# V13-V10-H fixed strategy trials', '',
              f'Source through: {max(all_days)}; included sessions: {len(all_days)}.',
              f'Last two calendar weeks: {last_two_week_start} through {window_end}; '
              f'verified source sessions: {", ".join(last_two_week_days)}.',
              'Historical comparisons are exploratory. No trial has execution authority.', '',
              '| Trial | Reference | Selected | Executed | Net INR | Delta INR | Daily drawdown INR |',
              '| --- | --- | ---: | ---: | ---: | ---: | ---: |']
    for name, item in results.items():
        report.append(f"| {name} | {item['comparison_reference']} | {item['selected_orders']} | "
                      f"{item['executed_trades']} | {item['net_profit_rupees']:,.2f} | "
                      f"{item['delta_net_rupees']:,.2f} | {item['daily_close_drawdown_rupees']:,.2f} |")
    report += ['', 'Each trial directory has `trades.parquet`, `daily.csv`, '
               '`last_two_weeks_stock_entries.csv`, `last_two_weeks_stock_minutes.csv`, '
               'and `trade_attribution.csv`.',
               'The G shared and INR 3,000 risk references are separate.',
               'Filtered slots are not refilled. Missing ATR retains the G rule and is marked in decisions.', '',
               '## Unavailable evidence', '',
               f'- Sector cap: {"available" if sector_lookup is not None else "complete dated sector map missing"}.',
               f'- Relative strength: {index_status}.',
               f'- India VIX regime: {"available" if vix_orders is not None else "verified decision-time history missing"}.',
               '- Tick size is uniform INR 0.05 and costs are the declared model assumptions.',
               '- One-minute OHLC is resolved stop first; intraminute order and actual fills are unavailable.',
               '']
    (out / 'REPORT.md').write_text('\n'.join(report), encoding='utf-8')
    artifact_hashes = {p.relative_to(out).as_posix(): h.sha(p) for p in sorted(out.rglob('*')) if p.is_file()}
    h.write_json(out / 'manifest.json', dict(state='COMPLETE', artifacts=artifact_hashes,
        source_manifest_sha256=hashes['source_manifest'], execution_authority=False))
    return out
