"""Isolated counterfactual replay of the two G3 scanner slots affected on 2026-10-12."""
from __future__ import annotations

import hashlib
import html
import json
import math
import sys
from collections import Counter
from datetime import date
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
import fno_v13_v10_g_3_contract as contract
import fno_v13_v10_g_3_live_config as config
import fno_v13_v10_g_2_backtest as simulator

DAY = date(2026, 10, 12)
RUN = Path(json.loads(Path(__file__).with_name('latest_run.json').read_text())['run_dir'])
SAVE = ('tradingsymbol', 'side', 'signal_end', 'signal_timestamp', 'signal_close',
        'open', 'high', 'low', 'close', 'volume', 'ema9', 'ema20', 'ema50',
        'price_change_pct', 'oi', 'prev_oi', 'oi_change_pct', 'volume_ratio',
        'traded_value', 'nifty_first_bar_return_pct')


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def json_value(value):
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if hasattr(value, 'item'):
        return json_value(value.item())
    if isinstance(value, dict):
        return {str(k): json_value(v) for k, v in value.items()}
    if isinstance(value, list):
        return [json_value(v) for v in value]
    if isinstance(value, float) and not math.isfinite(value):
        return None
    if pd.isna(value):
        return None
    return value


def dump(name, obj):
    path = RUN / name
    path.write_text(json.dumps(json_value(obj), indent=2, allow_nan=False) + '\n', encoding='utf-8')
    return path


def yn(value):
    return 'YES' if bool(value) else 'NO'


def table(rows, fields):
    tr = ''.join('<tr>' + ''.join(f'<td>{html.escape(str(row.get(key) if row.get(key) is not None else ""))}</td>' for key, label in fields) + '</tr>' for row in rows)
    th = ''.join(f'<th>{html.escape(label)}</th>' for key, label in fields)
    return '<table><thead><tr>' + th + '</tr></thead><tbody>' + tr + '</tbody></table>'


def main():
    manifest = json.loads((RUN / 'recovery_manifest.json').read_text())
    assert manifest['session_date'] == DAY.isoformat()
    assert manifest['strategy_fingerprint'] == contract.strategy_fingerprint()
    assert all(sha(Path(s['path'])) == s['sha256'] == sha(Path(s['snapshot']))
               for s in manifest['original_sources'])
    assert all(s['state'] == 'FETCHED' and sha(Path(s['parquet'])) == s['parquet_sha256']
               for s in manifest['minute_sources'])
    original = json.loads((RUN / 'base_candidates.json').read_text())
    assert len(original) == 20 and len({r['tradingsymbol'] for r in original}) == 19
    assert Counter(r['signal_end'] for r in original) == {'09:25': 5, '09:30': 15}

    candidates, paths_by_symbol, notes = [], {}, []
    for sid, row in enumerate(original):
        symbol, signal = row['tradingsymbol'], pd.Timestamp(row['signal_ts'])
        end = signal + pd.Timedelta(minutes=1)
        frame = paths_by_symbol.setdefault(symbol, pd.read_parquet(RUN / 'minutes' / f'{symbol}.parquet'))
        exact = frame.loc[frame.ts.eq(end)]
        assert len(exact) == 1, (symbol, end, 'exact confirmation unavailable')
        assert str(exact.iloc[0]['source']) == 'KITE_HISTORICAL_MINUTE_RETROSPECTIVE'
        prior = frame.loc[frame.ts.lt(end)].tail(config.LIVE_CONFIRMATION_VOLUME_REQUIRED_PRIOR)
        assert len(prior) == 20 and prior.ts.is_unique and prior.volume.ge(0).all()
        raw = exact.iloc[0].to_dict()
        raw['timestamp'] = end.isoformat()
        raw['confirmation_history_complete'] = True
        annotated = config.annotate_confirmation_volume(raw, frame[['ts', 'volume']].rename(columns={'ts': 'timestamp'}))
        assert annotated['confirmation_prior_volume_count'] == 20
        assert config._number(annotated['v9_1m_volume_ratio']) >= 0
        assert annotated['confirmation_prior_volume_last_ts'] == prior.ts.iloc[-1].isoformat()
        assert annotated['confirmation_volume_policy'] == config.CONFIRMATION_VOLUME_POLICY
        candidate = {key: row.get(key) for key in SAVE}
        candidate['signal_timestamp'] = signal.isoformat()
        candidate['signal_close'] = float(row['close'])
        candidate['sid'] = sid
        candidate['tick_size'] = .05
        assert contract.base_signal_side(candidate, row['signal_end'], session_date=DAY,
                   nifty_first_bar_return_pct=row.get('nifty_first_bar_return_pct')) == row['side']
        result = config.confirmation_metrics(candidate, annotated)
        assert pd.Timestamp(result['confirmation_timestamp']) == end
        assert pd.Timestamp(result['v9_1m_feature_ts']) == end
        assert result['confirmation_prior_volume_count'] == 20
        candidates.append(result)
        notes.append(dict(symbol=symbol, slot=row['signal_end'], source_scanner_sha256=row['source_scanner_sha256'],
                          minute_parquet_sha256=next(s['parquet_sha256'] for s in manifest['minute_sources'] if s['symbol'] == symbol),
                          prior_20_start=prior.ts.iloc[0].isoformat(), prior_20_end=prior.ts.iloc[-1].isoformat(),
                          exact_confirmation=end.isoformat()))

    audit = contract.selection_audit(pd.DataFrame(candidates))
    assert len(audit) == 20 and audit.sid.is_unique
    audit_rows = []
    for row in audit.to_dict('records'):
        setup = contract.setup_for(row['signal_end'], row['side'], session_date=DAY)
        failed = []
        for name, passed in (
            ('direction_or_confirmation_volume', row['confirmed']),
            ('setup_price_change', row['price_change_pct'] >= setup.price_change_pct),
            ('setup_oi_change', row['oi_change_pct'] >= setup.oi_change_pct),
            ('setup_5m_volume', row['volume_ratio'] >= setup.volume_ratio),
            ('setup_1m_body', setup.body_ratio <= row['body_ratio'] <= 1),
            ('setup_1m_wick', 0 <= row['wick_ratio'] <= setup.max_wick_ratio),
            ('setup_liquidity', row['traded_value'] >= setup.min_traded_value),
            ('confirmation_volume', row['v9_1m_volume_ratio'] >= contract.confirmation_volume_minimum(row['side'])),
        ):
            if not passed:
                failed.append(name)
        audit_rows.append(dict(signal_slot=row['signal_end'], confirmation_end=setup.confirmation_end,
            symbol=row['tradingsymbol'], side=row['side'], signal_close=row['signal_close'],
            confirmation_open=row['confirm_open'], confirmation_high=row['confirm_high'],
            confirmation_low=row['confirm_low'], confirmation_close=row['confirm_close'],
            confirmation_volume=row['confirm_volume'], confirmation_volume_ratio=row['v9_1m_volume_ratio'],
            required_confirmation_volume_ratio=contract.confirmation_volume_minimum(row['side']),
            prior_completed_minute_count=row['confirmation_prior_volume_count'],
            confirmation_reason=row['confirmation_reason'], price_change_pct=row['price_change_pct'],
            required_price_change_pct=setup.price_change_pct, oi_change_pct=row['oi_change_pct'],
            required_oi_change_pct=setup.oi_change_pct, five_minute_volume_ratio=row['volume_ratio'],
            required_five_minute_volume_ratio=setup.volume_ratio, body_ratio=row['body_ratio'],
            required_body_ratio=setup.body_ratio, wick_ratio=row['wick_ratio'],
            maximum_wick_ratio=setup.max_wick_ratio, traded_value=row['traded_value'],
            selected=bool(row['v9_selected']), filter_pass=bool(row['v9_filter_pass']),
            decision=row['v9_decision'], failed_checks=','.join(failed), trigger=row['trigger']))
        assert (not failed) == bool(row['v9_filter_pass']), (row['tradingsymbol'], failed)
    audit_rows.sort(key=lambda x: (x['signal_slot'], x['symbol']))
    selected = audit.loc[audit.v9_selected.eq(True)].copy()
    assert set(selected.signal_end) <= {'09:25', '09:30'}
    expected_asof = pd.Timestamp(manifest['as_of_ist']).tz_convert(contract.IST)
    paths = {}
    for item in selected.itertuples(index=False):
        frame = paths_by_symbol[item.tradingsymbol]
        bars = frame.loc[frame.ts.gt(item.confirmation_ts) & frame.ts.le(expected_asof) & frame.ts.dt.date.eq(DAY)].copy()
        first = pd.Timestamp(item.confirmation_ts).tz_convert(contract.IST) + pd.Timedelta(minutes=1)
        expected = pd.date_range(first, expected_asof, freq='min')
        assert len(bars) == len(expected) and bars.ts.reset_index(drop=True).eq(pd.Series(expected)).all(), item.tradingsymbol
        assert bars[['open','high','low','close']].notna().all().all()
        paths[int(item.sid)] = {'timestamp_ns': bars.ts.astype('int64').to_numpy(),
            **{key: bars[key].to_numpy(float) for key in ('open','high','low','close')}}
    trades = simulator.simulate_staged(selected, paths, cost_bps=contract.ROUND_TRIP_COST_BPS,
                                     max_entry_delay_minutes=contract.ENTRY_EXPIRY_MINUTES) if len(selected) else selected.copy()
    trade_rows = []
    for trade in trades.to_dict('records'):
        record = dict(signal_id=trade['signal_id'], signal_slot=trade['signal_end'],
            confirmation_end=trade['configured_confirmation_end'], symbol=trade['tradingsymbol'],
            side=trade['side'], setup_id=trade['setup_id'], trigger=trade['trigger'],
            filled=bool(trade['filled']), entry_time=trade.get('entry_ts'),
            entry_price=trade.get('entry_price'), exit_time=trade.get('exit_ts'),
            exit_price=trade.get('exit_price'), exit_reason=trade.get('exit_reason'),
            target_pct=trade['native_target_pct'], initial_stop_pct=contract.INITIAL_STOP_PCT,
            gross_return_pct=trade.get('gross_return_pct'), net_return_pct=trade.get('net_return_pct'),
            round_trip_cost_bps=contract.ROUND_TRIP_COST_BPS,
            capital_per_entry_rs=contract.CAPITAL_PER_ENTRY_RS,
            assumed_exposure_rs=contract.TARGET_EXPOSURE_RS,
            modeled_net_profit_rs=(trade.get('net_return_pct') or 0) / 100 * contract.TARGET_EXPOSURE_RS
            if bool(trade['filled']) and trade.get('exit_reason') != 'TIME_EXIT_1515' else None,
            as_of_ist=expected_asof)
        if trade.get('exit_reason') == 'TIME_EXIT_1515' and expected_asof.time() < pd.Timestamp('15:15').time():
            record.update(exit_reason='OPEN_AS_OF', mark_time=record['exit_time'],
                          mark_price=record['exit_price'], exit_time=None, exit_price=None,
                          gross_return_pct=None, net_return_pct=None, modeled_net_profit_rs=None)
        trade_rows.append(record)
    slot_rows = []
    for signal in ('09:25', '09:30'):
        these = [r for r in audit_rows if r['signal_slot'] == signal]
        slot_rows.append(dict(signal_slot=signal, confirmation_end=these[0]['confirmation_end'],
            original_scanner_published_candidates=0, reconstructed_base_candidates=len(these),
            recovered_exact_confirmation_bars=len(these), filter_pass=sum(r['filter_pass'] for r in these),
            selected_orders=sum(r['selected'] for r in these),
            modeled_fills=sum(t['filled'] for t in trade_rows if t['signal_slot'] == signal)))
    assert sum(r['selected_orders'] for r in slot_rows) == len(trade_rows)
    assert all(sha(Path(s['path'])) == s['sha256'] for s in manifest['original_sources'])
    summary = dict(session_date=DAY.isoformat(), role='ISOLATED_COUNTERFACTUAL_REPLAY',
        execution_authority=False, as_of_ist=expected_asof, strategy_version=contract.STRATEGY_VERSION,
        strategy_fingerprint=contract.strategy_fingerprint(), source_kind='SAVED_5M_SCANNER_FEATURES_PLUS_RETROSPECTIVE_BROKER_1M',
        first_stage_candidates=20, unique_symbols=19, exact_minute_bars=20,
        selected_orders=len(trade_rows), modeled_fills=sum(t['filled'] for t in trade_rows),
        slots=slot_rows, trade_results=trade_rows,
        assumptions=dict(previous_observed_minutes=20, session_prior_tail='Broker historical data ends at 15:15 on prior session; no later candles were supplied',
            capital_per_entry_rs=contract.CAPITAL_PER_ENTRY_RS, leverage=contract.LEVERAGE,
            round_trip_cost_bps=contract.ROUND_TRIP_COST_BPS, entry_expiry_minutes=contract.ENTRY_EXPIRY_MINUTES,
            exit_model='G3 accepted 1.25% initial stop, 1.00% after 120 minutes, configured target; native minute simulator'),
        interpretation='Counterfactual strategy result using retrospective data. No live order or actual fill is inferred.')
    dump('candidate_audit.json', audit_rows)
    dump('trade_results.json', trade_rows)
    dump('replay_result.json', summary)
    dump('source_lineage.json', notes)
    report = '''<!doctype html><html lang="en"><head><meta charset="utf-8"><title>G3 missing morning slots — 12 October 2026</title>
<style>body{font:15px/1.5 system-ui,sans-serif;max-width:1200px;margin:32px auto;padding:0 20px;color:#172033}h1{margin-bottom:4px}p.lede{color:#40516e}table{border-collapse:collapse;width:100%;margin:14px 0 28px;font-size:13px}th,td{border:1px solid #cad3df;padding:7px 8px;text-align:left}th{background:#eaf0f8}tr:nth-child(even){background:#f7f9fc}.callout{background:#eff7ff;border-left:4px solid #2775c6;padding:12px;margin:16px 0}</style></head><body>'''
    report += '<h1>G3 09:25 and 09:30 reconstruction</h1><p class="lede">12 October 2026 · Research replay from saved scanner inputs and retrospectively retrieved one-minute candles · as of ' + html.escape(expected_asof.strftime('%H:%M IST')) + '</p>'
    report += '<div class="callout"><strong>' + str(len(trade_rows)) + ' possible selected entry; ' + str(sum(t['filled'] for t in trade_rows)) + ' modeled fill.</strong> This is a counterfactual replay. The recorded PAPER session did not place this order.</div>'
    report += '<h2>Slots</h2>' + table(slot_rows, [('signal_slot','5m signal'),('confirmation_end','1m confirmation'),('reconstructed_base_candidates','Base candidates'),('recovered_exact_confirmation_bars','Recovered 1m bars'),('filter_pass','Filter pass'),('selected_orders','Selected orders'),('modeled_fills','Modeled fills')])
    report += '<h2>Modeled entry and exit</h2>' + table(trade_rows, [('symbol','Symbol'),('signal_slot','Signal'),('trigger','Trigger'),('entry_time','Entry bar end (IST)'),('entry_price','Entry price'),('exit_time','Exit bar end (IST)'),('exit_price','Exit price'),('exit_reason','Exit reason'),('net_return_pct','Net return %'),('modeled_net_profit_rs','Modeled net ₹')])
    report += '<p>Backtest sizing: ₹100,000 capital per trade at 5× exposure (₹500,000), with 5 bps round-trip costs. Minute-bar prices and times model potential fills, not observed broker execution.</p>'
    report += '<h2>All 20 confirmation checks</h2>' + table(audit_rows, [('signal_slot','5m signal'),('symbol','Symbol'),('confirmation_close','1m close'),('confirmation_volume_ratio','1m volume ratio'),('body_ratio','Body ratio'),('wick_ratio','Wick ratio'),('confirmation_reason','Confirmation result'),('failed_checks','Failed rules'),('decision','Decision')])
    report += '<p>Volume ratio uses the 20 preceding observed completed minutes. The historical broker source returned prior-session minutes only through 15:15; this replay does not fill absent minutes.</p>'
    report += '</body></html>'
    (RUN / 'report.html').write_text(report, encoding='utf-8')
    print(json.dumps(json_value(dict(run_dir=str(RUN), slots=slot_rows, trade_results=trade_rows)), indent=2))


if __name__ == '__main__':
    main()
