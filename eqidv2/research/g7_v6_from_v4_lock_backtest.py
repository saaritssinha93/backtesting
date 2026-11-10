"""Isolated G7 V6 entry-filter experiments using the frozen G7 V4 lock book.

This is a selected-opportunity ablation. The source is already OI filtered,
so it must never be used to claim results for OI removal or new entry setups.
"""
from __future__ import annotations

import argparse
from datetime import date
import hashlib
import json
from pathlib import Path
import sys

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from research import slot_win70_backtest as reference
from research.g7_v4_practical_research import execute
from research.g8_exit_engine import _flags
import strategy_slot_selection as slots

FEATURES = Path(r'C:/TradingData/eqidv2/backtesting_result_v13_v10_g_7/v4_research/practical_research/runs/2026-10-27/20261027T185748273567_five_fixed_tests/entry_features_and_decisions.csv')
OUT = ROOT / 'outputs/g7_v6_20261102/selected_opportunity_iteration'


def sha(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open('rb') as stream:
        for chunk in iter(lambda: stream.read(4 * 1024 * 1024), b''):
            digest.update(chunk)
    return digest.hexdigest()


def selected_vwap(rows: pd.DataFrame) -> pd.DataFrame:
    """Compute causal session VWAP at each five-minute signal from source minutes."""
    results = []
    # Source paths are immutable input snapshots. Each is read once for its
    # requested dates; the completed minute ending at the signal is included.
    for (source_path, source_hash), group in rows.groupby(['stock_source_path', 'stock_source_sha256']):
        path = Path(source_path)
        if sha(path) != source_hash:
            raise ValueError(f'Archived cash source hash changed: {path.name}')
        days = sorted(set(group.day.astype(str).str[:10]))
        low = pd.Timestamp(days[0], tz='Asia/Kolkata')
        high = pd.Timestamp(days[-1], tz='Asia/Kolkata') + pd.Timedelta(days=1)
        columns = ['date', 'high', 'low', 'close', 'volume', 'gap_filled', 'opening_snapshot', 'provisional_stale', 'is_synthetic']
        import pyarrow.parquet as pq
        available = pq.read_schema(path).names
        minute = pd.read_parquet(path, columns=[c for c in columns if c in available],
                                 filters=[('date', '>=', low), ('date', '<', high)])
        minute['ts'] = pd.to_datetime(minute.date, utc=True).dt.tz_convert('Asia/Kolkata')
        for row in group.itertuples():
            end = pd.Timestamp(row.signal_ts).tz_convert('Asia/Kolkata')
            start = pd.Timestamp(f'{row.day} 09:16', tz='Asia/Kolkata')
            prefix = minute.loc[minute.ts.between(start, end)].sort_values('ts')
            expected = pd.date_range(start, end, freq='min')
            valid = len(prefix) == len(expected) and prefix.ts.tolist() == list(expected)
            for flag in ('gap_filled', 'opening_snapshot', 'provisional_stale', 'is_synthetic'):
                if flag in prefix and prefix[flag].astype(str).str.strip().str.lower().isin(('true', '1', '1.0')).any():
                    valid = False
            vol = pd.to_numeric(prefix.volume, errors='coerce')
            if valid and len(prefix) and vol.ge(0).all() and vol.sum() > 0:
                typical = (prefix.high + prefix.low + prefix.close) / 3
                vwap = float((typical * vol).sum() / vol.sum())
                distance = 100 * (float(row.signal_close) / vwap - 1)
            else:
                vwap, distance = np.nan, np.nan
            results.append(dict(sid=int(row.Index), signal_vwap=vwap,
                                signal_vwap_distance_pct=distance, vwap_source_valid=bool(valid)))
        if sha(path) != source_hash:
            raise ValueError(f'Archived cash source changed during calculation: {path.name}')
    return pd.DataFrame(results).set_index('sid')


def variants(data: pd.DataFrame) -> dict[str, pd.Series]:
    is_long = data.side.eq('LONG')
    def long_gate(mask):
        return ~is_long | mask.fillna(False)
    oi = pd.to_numeric(data.oi_change_pct, errors='coerce')
    vol = pd.to_numeric(data.volume_ratio, errors='coerce')
    slope = pd.to_numeric(data.sel_signed_vwap_slope5_pct, errors='coerce')
    ready = _flags(data.sel_vwap_slope5_ready, 'VWAP slope ready')
    dist = pd.to_numeric(data.signal_vwap_distance_pct, errors='coerce')
    above = dist.gt(0)
    rising = ready & slope.gt(0)
    result = {'v4_lock_control': pd.Series(True, index=data.index)}
    for threshold in (.06, .08, .10, .20, .30, .40):
        result[f'oi_min_{threshold:.2f}'] = long_gate(oi.ge(threshold))
    for threshold in (1.05, 1.10, 1.20, 1.25, 1.30, 1.50, 2.0):
        result[f'volume_min_{threshold:.2f}'] = long_gate(vol.ge(threshold))
    result['above_session_vwap_only'] = long_gate(above)
    result['rising_vwap_slope_only'] = long_gate(rising)
    for limit in (.75, 1.25, 2.0):
        result[f'vwap_rising_maxdist_{limit:.2f}'] = long_gate(above & rising & dist.le(limit))
    for threshold in (1.2, 1.5):
        result[f'vwap_volume_{threshold:.1f}'] = long_gate(above & rising & vol.ge(threshold))
    result['vwap_rising_only'] = long_gate(above & rising)
    # OR experiments: retain a LONG when either independent confirmation
    # is present. All remain inside the frozen V4 prefiltered opportunity pool.
    oi10 = oi.ge(.10)
    vol12 = vol.ge(1.20)
    vwap = above & rising
    result['or_oi10_vol12'] = long_gate(oi10 | vol12)
    result['or_oi10_vwap'] = long_gate(oi10 | vwap)
    result['or_vol12_vwap'] = long_gate(vol12 | vwap)
    result['or_any_oi10_vol12_vwap'] = long_gate(oi10 | vol12 | vwap)
    result['two_of_oi10_vol12_vwap'] = long_gate((oi10 & vol12) | (oi10 & vwap) | (vol12 & vwap))
    result['or_oi10_above_vwap'] = long_gate(oi10 | above)
    result['or_vol12_above_vwap'] = long_gate(vol12 | above)
    result['or_oi10_vol12_then_vwap'] = long_gate((oi10 | vol12) & vwap)
    result['or_oi10_vol12_or_vwap'] = long_gate((oi10 & vol12) | vwap)
    return result


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument('--output', type=Path, default=OUT)
    args = parser.parse_args()
    out = args.output.resolve()
    out.mkdir(parents=True, exist_ok=True)
    if list(out.iterdir()):
        raise FileExistsError(f'Fresh result directory required: {out}')
    books, days, config = reference.source_inputs()
    source = books['G7_V4_LOCK2']
    enabled = slots.enabled_setup_ids('G7_V4_LOCK2')
    feature = pd.read_csv(FEATURES, low_memory=False).set_index('sid')
    # One V4 09:26 short replacement has no practical indicator row. New
    # experiments apply to longs only, so retain the existing short unchanged.
    needed_longs = set(source.loc[source.side.eq('LONG'), 'sid'].astype(int))
    if not needed_longs.issubset(set(feature.index.astype(int))):
        raise ValueError('Long feature inventory incomplete')
    native = pd.read_csv(Path(r'C:/TradingData/eqidv2/backtesting_result_v13_v10_g_7/v4_research/runs/2026-10-27/20261027T165517683392_v4_extremes/inputs/native.csv'), low_memory=False).set_index('sid')
    longs = feature.loc[sorted(needed_longs)].copy()
    for column in ('signal_ts', 'signal_close', 'side'):
        longs[column] = native.loc[longs.index, column]
    vw = selected_vwap(longs)
    data = source[['sid', 'day', 'setup_id', 'tradingsymbol', 'side', 'oi_change_pct', 'volume_ratio']].set_index('sid')
    for column in ('sel_signed_vwap_slope5_pct', 'sel_vwap_slope5_ready'):
        data[column] = feature[column]
    data = data.join(vw)
    data.loc[data.side.eq('SHORT'), 'sel_vwap_slope5_ready'] = False
    data['sel_vwap_slope5_ready'] = data.sel_vwap_slope5_ready.astype(str).str.strip().str.lower().isin(('true', '1', '1.0'))
    if data.loc[data.side.eq('LONG'), 'signal_vwap'].isna().any():
        raise ValueError('Long VWAP source is incomplete; no silent omission')
    controls = {}
    rows, decisions, books_by_name = [], [], {}
    periods = {'all': days, 'development': days[:35], 'validation': days[35:49],
               'assessment': days[49:57], 'later': days[35:]}
    for cost in (5., 15., 30.):
        expected = reference.apply_slot_policy(source, 'G7_V4_LOCK2', enabled, config, cost)
        base_allowed = _flags(source.base_entry_allowed, 'base') & source.setup_id.isin(enabled)
        masks = variants(data)
        for name, mask in masks.items():
            allowed = pd.Series((base_allowed.to_numpy(bool) & mask.to_numpy(bool)), index=source.sid.astype(int))
            book = execute(dict(raw=source.copy(deep=True), allowed=allowed,
                                integer=True, clock=True, limit=2), config, name, cost=cost)
            if not book.sid.equals(source.sid):
                raise ValueError('Opportunity inventory/order drift')
            if name == 'v4_lock_control':
                reference.verify_control(book, expected, 'G7 V4 lock control')
                controls[cost] = book
            control = controls[cost]
            for period, calendar in periods.items():
                result = reference.metrics(book, calendar)
                baseline = reference.metrics(control, calendar)
                rows.append(dict(variant=name, cost_bps=cost, period=period,
                                 net_delta_vs_v4_rs=result['net_pnl_rs'] - baseline['net_pnl_rs'],
                                 **result))
            if cost == 5.:
                books_by_name[name] = book
                decisions.append(pd.DataFrame({'sid': source.sid, 'day': source.day,
                                                'symbol': source.tradingsymbol, 'setup_id': source.setup_id,
                                                'side': source.side, 'variant': name,
                                                'feature_pass': mask.to_numpy(bool),
                                                'executed': _flags(book.portfolio_executed, 'executed').to_numpy(bool),
                                                'net_pnl_rs': book.portfolio_net_profit_rupees}))
        print('Completed cost', cost, flush=True)
    table = pd.DataFrame(rows)
    table.to_csv(out / 'all_variants.csv', index=False)
    pd.concat(decisions, ignore_index=True).to_csv(out / 'opportunity_decisions.csv', index=False)
    data.reset_index().to_csv(out / 'entry_features.csv', index=False)
    base = table.loc[table.variant.eq('v4_lock_control') & table.cost_bps.eq(5.)].set_index('period')
    development = table.loc[table.period.eq('development') & table.cost_bps.eq(5.) & ~table.variant.eq('v4_lock_control')].copy()
    development = development.sort_values(['net_pnl_rs', 'daily_close_drawdown_rs'], ascending=[False, True])
    best_alternative = str(development.iloc[0].variant)
    improvement_candidate = best_alternative if development.iloc[0].net_pnl_rs > base.loc['development', 'net_pnl_rs'] else None
    selected = table.loc[table.variant.isin(['v4_lock_control', best_alternative]) & table.cost_bps.eq(5.)]
    report = dict(scope='BACKTEST_ONLY', execution_authority=False, method='Frozen G7 V4 selected-opportunity filters only',
                  data_end=days[-1], sessions=len(days), source_opportunities=len(source),
                  existing_oi_gate_cannot_be_removed_in_this_pool=True,
                  historical_slot_selection_uses_outcomes=True, independent_holdout=False,
                  research_code_sha256=sha(Path(__file__)),
                  source_feature_sha256=sha(FEATURES),
                  development_best_alternative=best_alternative,
                  development_improvement_candidate=improvement_candidate,
                  retained_control='v4_lock_control' if improvement_candidate is None else None,
                  baseline_control_parity=True, development_winner_results=selected.to_dict('records'),
                  all_variants=sorted(set(table.variant)))
    (out / 'summary.json').write_text(json.dumps(report, indent=2, default=str), encoding='utf-8')
    print(table.loc[table.period.isin(['all', 'development', 'validation', 'assessment']) & table.cost_bps.eq(5.),
                    ['variant', 'period', 'trades', 'wins', 'losses', 'net_pnl_rs',
                     'net_delta_vs_v4_rs', 'daily_close_drawdown_rs']].to_string(index=False), flush=True)
    print('G7 V6 selected opportunity output', out, flush=True)


if __name__ == '__main__':
    main()
