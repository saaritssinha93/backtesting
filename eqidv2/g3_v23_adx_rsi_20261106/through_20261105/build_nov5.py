"""Build the complete sealed November 5 inputs without changing prior research.

V21 selection, all G7 confirmations, native execution and fixed per-entry
economics are reused unchanged. ADX/RSI are diagnostics, never entry vetoes.
"""
from __future__ import annotations

import argparse
from datetime import date
import json
from pathlib import Path
import pickle

import numpy as np
import pandas as pd

from fno_v13_v10_g_3_daily_replay import simulate
import fno_v13_v10_g_2_backtest as native_simulator
from g3_v18_20261102 import data
from g3_v21_expanded_20261104 import source, selection
from g3_v21_expanded_20261104.refinement_20261104 import unlimited
from g3_v21_expanded_20261104.refinement_20261104.strict_new_pairs_20261104 import policy as strict
from g3_v21_expanded_20261104.refinement_20261104.g7_v4_confirmations_20261104 import policy as g7
from g3_v21_expanded_20261104.refinement_20261104.g7_v4_confirmations_20261104.stock_features import build_stock_audit
from g3_v21_expanded_20261104.refinement_20261104.g7_v4_confirmations_20261104.nifty_features import build_nifty_audit
from g3_v21_expanded_20261104.refinement_20261104.g7_v4_confirmations_20261104.adx_rsi_20261105.features import build_adx_audit
from g3_v23_adx_rsi_20261106 import policy as v23
from g3_v23_adx_rsi_20261106.run import sha, check_identities, metrics
from g3_v23_adx_rsi_20261106.keep_entries_20261106.exit_study.run import replay, exit_timestamp, verify_path

PACKAGE = Path(__file__).resolve().parent
DAY = '2026-11-05'
DAY_INDEX = 65
TRADE_COLUMNS = ['day', 'setup_id', 'tradingsymbol', 'side', 'entry_ts', 'exit_ts',
                 'entry_price', 'exit_price', 'portfolio_net_profit_rupees', 'exit_reason', 'sid']
PARITY_COLUMNS = ['sid', 'day', 'native_replay_match', 'exit_index', 'entry_index',
                  'native_target_pct', 'weak_entry_adx_rsi', 'baseline_ambiguous']
ADX_COLUMNS = ['sid', 'adx14', 'plus_di14', 'minus_di14', 'adx_delta3', 'adx_ready',
               'adx_delta3_ready', 'adx_available_at', 'adx_session_bars', 'adx_stock_source_sha256']


def validate_source(raw, minutes, proof):
    if proof.get('loader_source') != 'HASH_VERIFIED_SEALED_DAILY_FEATURE_LEDGER_AND_RAW_SNAPSHOT':
        raise ValueError('Nov5 requires a complete immutable daily source')
    for key in ('run_manifest', 'source_manifest', 'snapshot_manifest'):
        if not proof.get(key) or sha(Path(proof[key])) != proof[key + '_sha256']:
            raise ValueError('Sealed source proof changed: ' + key)
    expansion = proof.get('v21_expansion', {})
    if expansion.get('missing_append_rows') != 0 or expansion.get('through_clock') != '14:00':
        raise ValueError('Not all expanded Nov5 clocks are represented')
    if len(selection.SPECS) != 73 or not raw.day.astype(str).eq(DAY).all():
        raise ValueError('Unexpected session or setup inventory')
    wanted = set(source.CLOCKS)
    for symbol, part in raw.groupby('tradingsymbol'):
        if len(part) != len(wanted) or set(part.signal_ts.dt.strftime('%H:%M')) != wanted:
            raise ValueError('Incomplete feature clock grid: ' + symbol)
    checked = data._normalize_minutes(minutes, DAY)
    if len(checked) != len(minutes) or not set(raw.tradingsymbol).issubset(set(minutes.tradingsymbol)):
        raise ValueError('Execution universe or full-session minute coverage is incomplete')
    return dict(day=DAY, complete_session=True, signal_first='09:20', signal_last='14:00',
                confirmation_last='14:01', exit_last='15:15', setup_pairs=73,
                source_feature_symbols=int(raw.tradingsymbol.nunique()),
                execution_symbols=int(minutes.tradingsymbol.nunique()),
                feature_rows=len(raw), minute_rows=len(minutes), bars_per_execution_symbol=360,
                source_eligibility=proof.get('coverage', {}),
                excluded_execution_symbols=proof.get('excluded_execution_symbols', []),
                expansion=expansion)


def load_source():
    cache = PACKAGE / 'raw_source_cache.pkl'
    proof_path = PACKAGE / 'raw_source_cache_proof.json'
    if cache.exists() and proof_path.exists():
        cache_proof = json.loads(proof_path.read_text())
        if cache_proof['sha256'] != sha(cache) or cache_proof['source_loader_sha256'] != sha(source.__file__):
            raise ValueError('Nov5 isolated source cache changed')
        raw, minutes, proof = pickle.loads(cache.read_bytes())
    else:
        raw, minutes, proof = source.load_day(DAY)
    coverage = validate_source(raw, minutes, proof)
    return raw, minutes, proof, coverage


def feature_audit(orders):
    if orders.empty:
        names = ['sid', 'day', 'setup_id', 'tradingsymbol', 'side', 'confirmation_ts',
                 'all_g7_allowed', 'rsi14', 'signed_rsi_delta3', 'rsi_ready',
                 'feature_available_at', *ADX_COLUMNS[1:]]
        audit = pd.DataFrame(columns=names)
        return audit, dict(stock=[], nifty=[], adx=[])
    stock, stock_proof = build_stock_audit(orders)
    nifty, nifty_proof = build_nifty_audit(orders)
    audit = g7.decisions(orders, stock, nifty)
    audit['day'] = audit.day.astype(str)
    audit['all_g7_allowed'] = audit.all_g7_entry_allowed.astype(bool)
    audit['pair_class'] = np.where(audit.setup_id.isin(strict.NEW_IDS), 'new39', 'original34')
    adx, adx_proof = build_adx_audit(orders)
    for key in ('day', 'setup_id', 'tradingsymbol', 'side', 'confirmation_ts'):
        left = audit.set_index('sid')[key].sort_index()
        right = adx.set_index('sid')[key].sort_index()
        if key == 'confirmation_ts':
            equal = pd.to_datetime(left, utc=True).equals(pd.to_datetime(right, utc=True))
        else:
            equal = left.astype(str).equals(right.astype(str))
        if not equal:
            raise ValueError('Indicator/source identity changed: ' + key)
    audit = audit.merge(adx[ADX_COLUMNS], on='sid', how='left', validate='one_to_one')
    masks = v23.masks(audit)
    if not masks['baseline_v21_g7'].equals(audit.all_g7_allowed):
        raise ValueError('ADX/RSI changed baseline admission')
    for name, mask in masks.items():
        audit['diagnostic_' + name] = mask
    return audit, dict(stock=stock_proof, nifty=nifty_proof, adx=adx_proof)


def native_parity(trades, book, paths, audit):
    weak = dict(zip(audit.sid, ~v23.masks(audit)['adx20_di_rsi'])) if len(audit) else {}
    rows = []
    used = {}
    native = book.set_index('sid', drop=False)
    for frozen in trades.itertuples(index=False):
        row = native.loc[frozen.sid]
        path = paths[int(frozen.sid)]
        entry_index = int(row.entry_path_index)
        verify_path(path, entry_index)
        args = (path, entry_index, float(frozen.entry_price), frozen.side == 'LONG', float(row.native_target_pct))
        original = native_simulator.staged_exit(*args)
        actual = replay(*args, weak=weak[frozen.sid], settings={})
        if (actual['index'], actual['reason'], actual['event']) != (original[0], original[2], original[4]):
            raise ValueError('Nov5 original-engine parity failed')
        timestamp = exit_timestamp(path, actual)
        pnl = (1 if frozen.side == 'LONG' else -1) * (actual['price'] / frozen.entry_price - 1) * 500000 - 250
        if (timestamp != pd.Timestamp(frozen.exit_ts) or actual['reason'] != frozen.exit_reason
                or not np.isclose(actual['price'], original[1], atol=1e-9, rtol=0)
                or not np.isclose(actual['price'], frozen.exit_price, atol=1e-7, rtol=0)
                or not np.isclose(pnl, frozen.portfolio_net_profit_rupees, atol=1e-5, rtol=0)):
            raise ValueError('Nov5 frozen-price/time/P&L parity failed: ' + str(frozen.sid))
        rows.append(dict(sid=int(frozen.sid), day=DAY, native_replay_match=True,
                         exit_index=actual['index'], entry_index=entry_index,
                         native_target_pct=float(row.native_target_pct),
                         weak_entry_adx_rsi=bool(weak[frozen.sid]),
                         baseline_ambiguous=actual['ambiguous']))
        used[int(frozen.sid)] = path
    return pd.DataFrame(rows, columns=PARITY_COLUMNS), used


def run(output=PACKAGE / 'source_20261105'):
    output = Path(output).resolve()
    if output.exists() or output == PACKAGE or not output.is_relative_to(PACKAGE):
        raise ValueError('Use a fresh isolated Nov5 source output directory')
    print('Loading and validating full sealed Nov5 source...', flush=True)
    raw, minutes, source_proof, coverage = load_source()
    raw = raw.reset_index(drop=True).copy()
    raw['sid'] = DAY_INDEX * 1_000_000 + np.arange(len(raw), dtype=np.int64)
    expanded_orders, selection_audit, _ = selection.select(raw)
    if not expanded_orders.sid.is_unique or not expanded_orders.day.astype(str).eq(DAY).all():
        raise ValueError('Expanded order identities are invalid')
    paths = data.execution_paths(expanded_orders, minutes)
    print('Replaying native expanded orders:', len(expanded_orders), flush=True)
    native, _ = simulate(expanded_orders, paths, [date.fromisoformat(DAY)])
    if set(native.sid) != set(expanded_orders.sid) or not native.sid.is_unique:
        raise ValueError('Nov5 native simulator changed selected order identities')
    orders = strict.apply(native)
    print('Building G7 and ADX/RSI audit for strict V21 orders:', len(orders), flush=True)
    audit, indicator_proofs = feature_audit(orders)
    if len(orders):
        admitted = g7.subset(orders, audit, g7.ARMS[-1])
    else:
        admitted = orders.copy()
    book = unlimited._unlimited(admitted)
    if not set(book.sid) == set(audit.loc[audit.all_g7_allowed.astype(bool), 'sid']):
        raise ValueError('G7 selected-order inventory mismatch')
    for key in TRADE_COLUMNS:
        if key not in book and book.empty:
            book[key] = pd.Series(dtype='object')
    trades = book.loc[book.portfolio_executed.eq(True), TRADE_COLUMNS].copy()
    # Keep the same serializable trade schema as the original V23 baseline.
    for key in ('entry_ts', 'exit_ts'):
        trades[key] = trades[key].astype(str)
    trades['day'] = trades.day.astype(str)
    check_identities(audit, trades)
    parity, used_paths = native_parity(trades, book, paths, audit)
    if len(parity) != len(trades) or set(parity.sid) != set(trades.sid):
        raise ValueError('Nov5 replay silently dropped a native fill')
    values = metrics(trades, len(admitted), [DAY])
    config = dict(scope='BACKTEST_ONLY', execution_authority=False, day=DAY,
                  sid_block=DAY_INDEX, full_session_included=True,
                  adx_rsi_entry_veto_enabled=False, expanded_orders=len(expanded_orders),
                  v21_strict_orders=len(orders), all_g7_admitted_orders=len(admitted),
                  native_fills=len(trades), native_parity_count=len(parity),
                  baseline_metric=values, coverage=coverage,
                  fixed_policy_fingerprints=dict(selection=selection.fingerprint(), strict=strict.fingerprint(), g7=g7.fingerprint()),
                  costs_per_fill_rs=250, exposure_per_fill_rs=500000,
                  interpretation='Full available Nov5 session, with immutable source stock eligibility exclusions disclosed; no market-data imputation.')
    output.mkdir(parents=True)
    for name, frame in [('native_trades', trades), ('admission_audit', audit),
                        ('all_g7_orders', book), ('v21_orders', orders),
                        ('expanded_orders', native), ('selection_audit', selection_audit),
                        ('baseline_parity', parity)]:
        frame.to_csv(output / (name + '.csv'), index=False)
    (output / 'inputs.pkl').write_bytes(pickle.dumps(dict(trades=trades, parity=parity, paths=used_paths, days=[DAY]), protocol=5))
    (output / 'config.json').write_text(json.dumps(config, indent=2, default=str) + '\n')
    (output / 'source_proof.json').write_text(json.dumps(source_proof, indent=2, default=str) + '\n')
    (output / 'indicator_proofs.json').write_text(json.dumps(indicator_proofs, indent=2, default=str) + '\n')
    files = [Path(__file__), Path(source.__file__), Path(selection.__file__), Path(strict.__file__),
             Path(g7.__file__), Path(unlimited.__file__), Path(simulate.__code__.co_filename),
             Path(build_stock_audit.__code__.co_filename), Path(build_nifty_audit.__code__.co_filename),
             Path(build_adx_audit.__code__.co_filename), Path(v23.__file__),
             Path(replay.__code__.co_filename), Path(data.__file__), Path(native_simulator.__file__)]
    provenance = dict(code_sha256={str(p):sha(p) for p in files},
                      input_pickle_sha256=sha(output / 'inputs.pkl'),
                      raw_cache_sha256=sha(PACKAGE / 'raw_source_cache.pkl') if (PACKAGE / 'raw_source_cache.pkl').exists() else None)
    (output / 'provenance.json').write_text(json.dumps(provenance, indent=2) + '\n')
    (output / 'manifest.json').write_text(json.dumps({p.name:sha(p) for p in output.iterdir() if p.is_file()}, indent=2) + '\n')
    print(json.dumps({key:config[key] for key in ('day','expanded_orders','v21_strict_orders','all_g7_admitted_orders','native_fills','baseline_metric')}, indent=2), flush=True)
    return config


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, default=PACKAGE / 'source_20261105')
    run(parser.parse_args().output)
