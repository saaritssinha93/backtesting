"""Verified H research bundles, attribution, causal context and paired reports.

No live configuration writes, broker APIs, optimizers, or automatic promotion.
"""
from __future__ import annotations

import hashlib
import json
import math
from dataclasses import asdict, replace
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

import fno_v13_v10_g_backtest as g
from .v13h_execution import ExecutionModel, daily_metrics, metrics, minute_equity, simulate_portfolio

SCHEMA = 'V13_V10_H_RESEARCH_V1'
G_CONFIG_SHA = 'd8bcae37d7725279f8ac6e5c4c96a44e8a43f41b5e42d413b167fd806b52a127'
DEFAULT_SOURCE = Path('C:/TradingData/eqidv2/fno_oi/strategy_research/v13_v10_g_full_history/run_20260925_cutoff_corrected_through_20260923')
DEFAULT_OUTPUT = Path('C:/TradingData/eqidv2/v13_v10_h_research')
KEYS = ['day', 'hhmm_int', 'side', 'setup_id', 'tradingsymbol']


def sha(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open('rb') as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b''):
            digest.update(block)
    return digest.hexdigest()


def canonical(value) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()).hexdigest()


def read_json(path):
    return json.loads(Path(path).read_text(encoding='utf-8'))


def write_json(path, value):
    Path(path).write_text(json.dumps(value, indent=2, sort_keys=True, allow_nan=False) + '\n', encoding='utf-8')


def safe_child(root: Path, relative: str) -> Path:
    path = (root / relative).resolve()
    if not path.is_relative_to(root.resolve()) or path == root.resolve():
        raise ValueError(f'Artifact outside source bundle: {relative}')
    return path


def _bool(values):
    return values.eq(True) | values.astype(str).str.lower().eq('true')


def load_source(root: Path) -> dict:
    """Require a checksum-inventoried G bundle, then recompute exact G control."""
    root = root.resolve()
    bundle = read_json(root / 'bundle_manifest.json')
    if bundle.get('state') != 'COMPLETE':
        raise ValueError('Source bundle must be COMPLETE')
    required = {'dataset/signals.parquet', 'dataset/paths.npz', 'dataset/dataset_manifest.json',
                'dataset/setup_audit.parquet', 'dataset/all_5m_features.parquet', 'dataset/eligibility.parquet',
                'dataset/path_quality.parquet', 'g_backtest/portfolio_trades.csv',
                'g_backtest/summary.json', 'g_backtest/run_metadata.json'}
    if not required.issubset(bundle['artifacts']):
        raise ValueError('Source manifest is missing required artifacts')
    hashes = {'bundle_manifest.json': sha(root / 'bundle_manifest.json')}
    for name, record in bundle['artifacts'].items():
        path = safe_child(root, name)
        if not path.is_file() or sha(path) != record['sha256']:
            raise ValueError(f'Source artifact checksum mismatch: {name}')
        hashes[name] = record['sha256']
    metadata = read_json(root / 'g_backtest/run_metadata.json')
    dataset = read_json(root / 'dataset/dataset_manifest.json')
    config_path = Path(metadata['frozen_g_config'])
    if metadata['frozen_g_config_sha256'] != G_CONFIG_SHA or sha(config_path) != G_CONFIG_SHA:
        raise ValueError('Retained G config does not match its pinned identity')
    settings = g.checked_settings(read_json(config_path))
    g.v9.validate_configuration()
    for name, checksum in dataset['output_sha256'].items():
        if hashes.get('dataset/' + name.replace('\\', '/')) != checksum:
            raise ValueError(f'Dataset and bundle manifests disagree: {name}')
    days = list(dataset['days'])
    if not days or days != sorted(set(days)) or max(days) > metadata['through_day']:
        raise ValueError('Invalid or post-cutoff session calendar')
    signals = pd.read_parquet(root / 'dataset/signals.parquet')
    if not set(signals.day.astype(str)).issubset(days):
        raise ValueError('Signals fall outside declared sessions')
    base = g.v9.V9Config(portfolio_capital_rupees=1_000_000., capital_per_entry_rupees=100_000.,
                         leverage_factor=5., max_positions=None, cost_bps=5.)
    selection = g.selection_audit(signals, base, g.SelectionChange(**settings['selection_change']), core_first=True)
    orders = selection.loc[selection.v9_selected].copy().reset_index(drop=True)
    saved = pd.read_csv(root / 'g_backtest/portfolio_trades.csv')
    identity = lambda frame: list(zip(frame.sid.astype(int), frame.setup_id.astype(str)))
    if identity(orders) != identity(saved):
        raise ValueError('Ordered G selection differs from the saved control')
    with np.load(root / 'dataset/paths.npz', allow_pickle=False) as archive:
        paths = {int(sid): {key: archive[f'{int(sid)}_{key}'].copy()
                           for key in ('timestamp_ns', 'open', 'high', 'low', 'close')}
                 for sid in orders.sid}
    g.v9.validate_paths(orders, paths)
    native_trades, native_ledger, summary = g.evaluate(
        dict(signals=signals, orders=orders, paths=paths, v9_config=base), settings)
    if identity(native_ledger) != identity(saved):
        raise ValueError('G ledger order changed')
    checks = {}
    for name in ('filled', 'portfolio_executed', 'exit_reason'):
        checks[name] = native_ledger[name].astype(str).str.lower().tolist() == saved[name].astype(str).str.lower().tolist()
    for name in ('entry_price', 'exit_price', 'portfolio_net_profit_rupees', 'portfolio_cost_rupees'):
        checks[name] = bool(np.allclose(pd.to_numeric(native_ledger[name]), pd.to_numeric(saved[name]),
                                        rtol=1e-10, atol=1e-7, equal_nan=True))
    for name in ('entry_ts', 'exit_ts'):
        checks[name] = pd.to_datetime(native_ledger[name], utc=True).equals(pd.to_datetime(saved[name], utc=True))
    declared = metadata['metrics']['full_history']
    checks['net_profit'] = math.isclose(summary['net_profit_rupees'], declared['net_profit_rupees'], abs_tol=.0001)
    if not all(checks.values()):
        raise ValueError(f'Exact G baseline reconciliation failed: {checks}')
    configured = orders.copy()
    for key in ('stop_pct', 'target_pct'):
        configured['native_' + key] = orders.setup_id.map({k: v[key] for k, v in settings['exit']['setups'].items()}).fillna(settings['exit']['default'][key])
    return dict(root=root, hashes=hashes, config_path=config_path, settings=settings, days=days,
                metadata=metadata, dataset=dataset, signals=signals, selection=selection,
                orders=configured, paths=paths, native_ledger=native_ledger,
                native_summary=summary, parity=checks)


def decision_audit(raw: pd.DataFrame, selection: pd.DataFrame, settings: dict) -> pd.DataFrame:
    """Retain upstream rejected rows and add G-specific gate margins/states."""
    rows = raw.copy()
    if rows.duplicated(KEYS).any():
        raise ValueError('Duplicate candidate identities')
    change = g.SelectionChange(**settings['selection_change'])
    setups = {s.setup_id: g.setup_pair(s, change)[1] for s in g.v9.v5.profile_setups(g.v9.v5.PROFILES['higher_frequency'])}
    if not set(rows.setup_id).issubset(setups):
        raise ValueError('Unknown setup in source audit')
    sign = np.where(rows.side.eq('LONG'), 1., -1.)
    specs = [('price', 'price_change_pct', 'price_change_pct', sign),
             ('oi', 'oi_change_pct', 'oi_change_pct', 1),
             ('volume_5m', 'volume_ratio', 'volume_ratio', 1),
             ('body', 'body_ratio', 'body_ratio', 1),
             ('liquidity', 'traded_value', 'min_traded_value', 1)]
    for label, column, attribute, direction in specs:
        threshold = rows.setup_id.map({k: getattr(v, attribute) for k, v in setups.items()})
        rows['h_required_' + label] = threshold
        rows['h_margin_' + label] = pd.to_numeric(rows[column], errors='coerce') * direction - threshold
    rows['h_margin_wick'] = rows.setup_id.map({k: v.max_wick_ratio for k, v in setups.items()}) - pd.to_numeric(rows.wick_ratio, errors='coerce')
    rows['h_margin_volume_1m'] = pd.to_numeric(rows.v9_1m_volume_ratio, errors='coerce') - 1.2
    rows['h_ema9_20_signed_pct'] = sign * (rows.v9_5m_ema9 - rows.v9_5m_ema20) / rows.signal_close * 100
    required = ['oi', 'prev_oi', 'signal_close', 'confirmation_open', 'confirmation_high',
                'confirmation_low', 'confirmation_close', 'v9_5m_ema9', 'v9_5m_ema20', 'v9_5m_ema50']
    margins = [c for c in rows if c.startswith('h_margin_')]
    missing = ~np.isfinite(rows[required + margins].apply(pd.to_numeric, errors='coerce')).all(axis=1)
    confirmation = pd.to_datetime(rows.confirmation_ts, utc=True, errors='coerce')
    signal = pd.to_datetime(rows.signal_ts, utc=True, errors='coerce')
    causal = confirmation.notna() & signal.notna()
    for field, cutoff in [('v9_1m_feature_ts', confirmation), ('v9_5m_feature_ts', signal),
                          ('v9_feature_available_ts', confirmation)]:
        observed = pd.to_datetime(rows[field], utc=True, errors='coerce')
        missing |= observed.isna()
        causal &= observed.le(cutoff)
    upstream = [c for c in rows if c.startswith('check_') and not c.startswith('check_setup_')]
    passed = rows[upstream].apply(_bool).all(axis=1) & rows[margins].ge(0).all(axis=1) & causal
    rows['h_state'] = np.select([missing, ~passed], ['DATA_MISSING', 'GATE_FAILED'], default='RANKED_OUT')
    rows['h_first_reason'] = np.select([missing, ~causal], ['REQUIRED_INPUT_MISSING', 'FEATURE_NOT_AVAILABLE_AT_DECISION'], default='')
    for column in upstream + margins:
        failed = ~_bool(rows[column]) if column in upstream else ~rows[column].ge(0)
        rows.loc[rows.h_first_reason.eq('') & failed, 'h_first_reason'] = column
    small = selection[KEYS + ['v9_selected', 'v9_filter_pass', 'v9_rank_in_setup_day', 'v10_g_f_core']].copy()
    rows['day'] = rows.day.astype(str)
    small['day'] = small.day.astype(str)
    rows = rows.merge(small, on=KEYS, how='left', validate='one_to_one')
    unmatched = rows.h_state.eq('RANKED_OUT') & rows.v9_filter_pass.isna()
    rows.loc[unmatched, 'h_state'] = 'DATA_MISSING'
    rows.loc[unmatched, 'h_first_reason'] = 'NO_VERIFIED_G_SELECTION_RECORD'
    chosen = rows.v9_selected.eq(True)
    if int(chosen.sum()) != int(selection.v9_selected.sum()):
        raise ValueError('Selected G candidate absent from attribution audit')
    if (chosen & ~rows.h_state.eq('RANKED_OUT')).any():
        raise ValueError('Selected G row fails the reconstructed audit')
    # Later V9 feature failures are not rank losses.
    rows.loc[rows.v9_filter_pass.eq(False) & rows.h_state.eq('RANKED_OUT'), 'h_state'] = 'GATE_FAILED'
    rows.loc[chosen, 'h_state'] = 'SELECTED'
    rows.loc[rows.h_first_reason.eq(''), 'h_first_reason'] = rows.loc[rows.h_first_reason.eq(''), 'h_state']
    return rows


def coverage_audit(rows: pd.DataFrame, eligibility: pd.DataFrame, days: list[str]) -> pd.DataFrame:
    eligibility = eligibility.copy()
    eligibility['day'] = eligibility.day.astype(str)
    records = []
    setup_ids = sorted(rows.setup_id.unique())
    for item in eligibility.to_dict('records'):
        day = item['day']
        for setup in setup_ids:
            subset = rows.loc[rows.day.eq(day) & rows.setup_id.eq(setup)]
            expected = int(item['universe_size'])
            records.append(dict(day=day, setup_id=setup, included_in_comparison=day in days,
                source_eligible=bool(item['eligible']), source_reason=str(item.get('v13_v5_eligibility_reason', item.get('reason', ''))),
                expected_universe_size=expected, observed_symbols=int(subset.tradingsymbol.nunique()),
                missing_expected_count=max(0, expected-int(subset.tradingsymbol.nunique())),
                data_missing_rows=int(subset.h_state.eq('DATA_MISSING').sum()),
                selected=int(subset.h_state.eq('SELECTED').sum()),
                dated_roster_membership='UNVERIFIED_COUNT_ONLY',
                raw_arrival_times='UNAVAILABLE_FINALIZED_HISTORY', warmup_counts='UNAVAILABLE_IN_SOURCE'))
    return pd.DataFrame(records)


def context_features(frame: pd.DataFrame) -> pd.DataFrame:
    """Backward-only observations; never supplied to G's selector or H sizing."""
    columns = ['day', 'signal_ts', 'tradingsymbol', 'contract_month', 'hhmm_int',
               'open', 'high', 'low', 'close', 'prev_close', 'volume', 'oi_change_pct',
               'v9_5m_vwap', 'v9_5m_feature_ts', 'price_change_pct']
    out = frame[columns].copy().sort_values(['tradingsymbol', 'contract_month', 'signal_ts'], kind='stable')
    if out.duplicated(['tradingsymbol', 'signal_ts']).any():
        raise ValueError('Duplicate context bar')
    stamp = pd.to_datetime(out.signal_ts, utc=True)
    features = pd.to_datetime(out.v9_5m_feature_ts, utc=True)
    if features.isna().any() or (features > stamp).any():
        raise ValueError('Noncausal five-minute feature source')
    group_keys = ['tradingsymbol', 'contract_month']
    out['_tr'] = pd.concat([out.high-out.low, (out.high-out.prev_close).abs(),
                            (out.low-out.prev_close).abs()], axis=1).max(axis=1)
    out['atr_14_5m'] = out.groupby(group_keys, sort=False)._tr.transform(lambda x: x.rolling(14, min_periods=14).mean())
    out['_return'] = np.log(out.close / out.prev_close.where(out.prev_close > 0))
    out['realized_vol_20_5m_pct'] = out.groupby(group_keys, sort=False)._return.transform(lambda x: x.rolling(20, min_periods=20).std(ddof=1)*100)
    out['vwap_extension_atr'] = (out.close-out.v9_5m_vwap) / out.atr_14_5m.where(out.atr_14_5m > 0)
    prior_volume = out.groupby(['tradingsymbol', 'hhmm_int'], sort=False).volume.transform(
        lambda x: x.shift(1).rolling(20, min_periods=5).mean())
    out['same_clock_rvol_prior20_min5'] = out.volume / prior_volume.where(prior_volume > 0)
    grouped = out.groupby(group_keys, sort=False)
    contiguous = stamp.sub(pd.to_datetime(grouped.signal_ts.shift(), utc=True)).eq(pd.Timedelta(minutes=5))
    same_day = out.day.eq(grouped.day.shift())
    out['oi_acceleration_5m'] = grouped.oi_change_pct.diff().where(contiguous & same_day)
    out['_advance'] = out.price_change_pct.gt(0).where(out.price_change_pct.notna())
    out['observed_universe_advance_fraction'] = out.groupby('signal_ts')._advance.transform('mean')
    out['observed_universe_count'] = out.groupby('signal_ts').tradingsymbol.transform('nunique')
    out['available_at'] = features
    out['provenance'] = 'HASH_VERIFIED_FINALIZED_5M_BARS_NOT_ORIGINAL_ARRIVAL_TIMES'
    for field in ('india_vix', 'sector_relative_strength', 'bid_ask_spread', 'quote_age_ms', 'correlation_exposure'):
        out[field] = np.nan
        out[field + '_status'] = 'UNAVAILABLE'
    return out.drop(columns=['_tr', '_return', '_advance'])


def paired_validation(daily: pd.DataFrame, minimum_train_days=20, fold_days=5) -> dict:
    """Whole-day chronological diagnostics. No fitting, random row splits or holdout claims."""
    days = daily.day.tolist()
    folds = []
    for start in range(minimum_train_days, len(days), fold_days):
        block = daily.iloc[start:start+fold_days]
        folds.append(dict(train_through=days[start-1], test_from=block.day.iloc[0], test_through=block.day.iloc[-1],
                          sessions=len(block), g_net_rupees=float(block.g_net_rupees.sum()),
                          h_net_rupees=float(block.h_net_rupees.sum()), delta_rupees=float(block.delta_rupees.sum())))
    delta = daily.delta_rupees.to_numpy(float)
    interval = None
    if len(delta) >= 10:
        rng = np.random.default_rng(13010)
        # Moving day blocks preserve some serial dependence; exploratory only.
        samples = []
        block_size = min(5, len(delta))
        for _ in range(2000):
            starts = rng.integers(0, len(delta)-block_size+1, math.ceil(len(delta)/block_size))
            sample = np.concatenate([delta[s:s+block_size] for s in starts])[:len(delta)]
            samples.append(float(sample.mean()))
        interval = [float(v) for v in np.quantile(samples, [.025, .975])]
    return dict(evidence='REUSED_HISTORY_CHRONOLOGICAL_DIAGNOSTIC_NOT_UNTOUCHED_TEST',
                model_fitted=False, folds=folds, paired_mean_daily_delta_rupees=float(delta.mean()),
                exploratory_day_block_bootstrap_mean_delta_95pct=interval,
                bootstrap_block_days=5, bootstrap_replicates=2000, prospective_sessions=0,
                promotion_eligible=False)


def _clean(value):
    if isinstance(value, dict):
        return {str(k): _clean(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_clean(v) for v in value]
    if isinstance(value, (float, np.floating)) and not math.isfinite(value):
        return None
    if isinstance(value, np.generic):
        return value.item()
    return value


def code_hashes():
    repo = Path(__file__).resolve().parents[2]
    return {str(p.relative_to(repo)).replace('\\', '/'): sha(p) for p in
            [repo/'fno_v13_v10_h_backtest.py', Path(__file__), Path(__file__).with_name('v13h_execution.py'),
             repo/'fno_v13_v10_g_backtest.py', repo/'fno_v13_v10_f_backtest.py', repo/'fno_v13_v10_b_backtest.py',
             repo/'fno_v13_v9_backtest.py', repo/'fno_v13_corrected_v5_backtest.py', repo/'fno_v13_v6_portfolio_backtest.py']}


def run(source_run: Path, output_root: Path, *, experiment='risk_3000', model=None,
        run_id=None, holdout_start=None, holdout_end=None) -> Path:
    if experiment not in ('control', 'risk_3000'):
        raise ValueError('Only one preregistered sizing hypothesis or the control is allowed')
    model = model or ExecutionModel()
    model.validate()
    if (model.portfolio_capital, model.entry_capital, model.leverage) != (1_000_000., 100_000., 5.):
        raise ValueError('H comparison retains G capital and leverage limits')
    source_run, output_root = source_run.resolve(), output_root.resolve()
    if output_root == source_run or output_root.is_relative_to(source_run):
        raise ValueError('H outputs cannot be inside the frozen G source bundle')
    now = datetime.now(timezone.utc)
    run_id = run_id or now.strftime('%Y%m%dT%H%M%S%fZ')
    if not run_id or any(c not in 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-' for c in run_id):
        raise ValueError('Invalid run ID')
    out = output_root / 'runs' / run_id
    if out.exists():
        raise FileExistsError(f'Refusing to overwrite {out}')
    if bool(holdout_start) != bool(holdout_end):
        raise ValueError('Both future holdout boundaries are required')
    if holdout_start:
        start, end = pd.Timestamp(holdout_start).date(), pd.Timestamp(holdout_end).date()
        today = pd.Timestamp(now).tz_convert('Asia/Kolkata').date()
        if start <= today or end < start:
            raise ValueError('Holdout must be an unobserved future interval')
    print('Verifying the frozen bundle and replaying exact G...', flush=True)
    source = load_source(source_run)
    if holdout_start and str(start) <= max(source['days']):
        raise ValueError('Holdout overlaps historical source')
    out.mkdir(parents=True, exist_ok=False)
    plan = dict(schema=SCHEMA, experiment=experiment, registered_at_utc=now.isoformat(),
                hypothesis='Predefined INR 3000 planned stop-plus-flat-cost risk versus fixed exposure; same signals and exit percentages',
                risk_budget_rupees=3000. if experiment == 'risk_3000' else None,
                trial_budget=1, execution_model=asdict(model), g_config_sha256=G_CONFIG_SHA,
                code_sha256=code_hashes(), source_manifest_sha256=source['hashes']['bundle_manifest.json'],
                holdout=dict(start=holdout_start, end=holdout_end, state='RESERVED_NOT_COLLECTED' if holdout_start else 'NOT_RESERVED'),
                primary_metric='paired_mean_daily_net_profit_after_declared_costs',
                acceptance=dict(positive_future_paired_delta_required=True, drawdown_no_worse_than_control=True,
                                minimum_prospective_sessions=20, sample_adequacy_review_required=True,
                                independent_manual_review_required=True),
                historical_evidence='EXPLORATORY_REUSED_HISTORY', execution_authority=False,
                live_configuration_changed=False)
    # Written before any H outcome is calculated; does not make reused history untouched.
    write_json(out/'experiment.json', plan)
    source['native_ledger'].to_parquet(out/'g_exact_legacy.parquet', index=False)
    raw = pd.read_parquet(source_run/'dataset/setup_audit.parquet')
    if not set(raw.day.astype(str)).issubset(source['days']):
        raise ValueError('Candidate audit contains undeclared sessions')
    print('Building complete available-candidate attribution and coverage...', flush=True)
    audit = decision_audit(raw, source['selection'], source['settings'])
    coverage = coverage_audit(audit, pd.read_parquet(source_run/'dataset/eligibility.parquet'), source['days'])
    coverage.to_parquet(out/'coverage.parquet', index=False)
    feature_source = pd.read_parquet(source_run/'dataset/all_5m_features.parquet')
    if feature_source.day.astype(str).gt(max(source['days'])).any():
        raise ValueError('Market context contains post-cutoff bars')
    context = context_features(feature_source)
    context.to_parquet(out/'market_context.parquet', index=False)
    risk = plan['risk_budget_rupees']
    scenarios = [('DECLARED_MODEL', model),
                 ('ADVERSE_5BPS_EACH_SIDE', replace(model, entry_slippage_bps=model.entry_slippage_bps+5,
                                                   exit_slippage_bps=model.exit_slippage_bps+5)),
                 ('DELAY_1M_ABSOLUTE_EXPIRY', replace(model, delay_minutes=model.delay_minutes+1))]
    scenario_metrics = {}
    for name, execution in scenarios:
        print(f'Paired execution: {name}', flush=True)
        folder = out / name.lower()
        folder.mkdir()
        ledgers, stats, dailies = {}, {}, {}
        for label, budget in [('g', None), ('h', risk)]:
            ledger = simulate_portfolio(source['orders'], source['paths'], execution, budget)
            marks = minute_equity(ledger, source['paths'])
            daily = daily_metrics(ledger, source['days'])
            ledgers[label], dailies[label] = ledger, daily
            stats[label] = metrics(ledger, daily, marks)
            ledger.to_parquet(folder/f'{label}_trades.parquet', index=False)
            marks.to_parquet(folder/f'{label}_minute_equity.parquet', index=False)
        paired = pd.DataFrame(dict(day=source['days'], g_net_rupees=dailies['g'].net_profit_rupees,
                                   h_net_rupees=dailies['h'].net_profit_rupees,
                                   g_trades=dailies['g'].trades, h_trades=dailies['h'].trades,
                                   g_cost_rupees=dailies['g'].cost_rupees, h_cost_rupees=dailies['h'].cost_rupees))
        paired['delta_rupees'] = paired.h_net_rupees - paired.g_net_rupees
        paired.to_parquet(folder/'daily_comparison.parquet', index=False)
        stats['delta_net_rupees'] = stats['h']['net_profit_rupees']-stats['g']['net_profit_rupees']
        stats['model'] = asdict(execution)
        scenario_metrics[name] = stats
        if experiment == 'control':
            pd.testing.assert_frame_equal(ledgers['g'], ledgers['h'])
        if name == 'DECLARED_MODEL':
            primary_daily = paired
            validation = paired_validation(paired)
            for label in ('g', 'h'):
                states = ledgers[label].set_index(['sid', 'setup_id']).status
                audit[label + '_execution_state'] = [states.get((int(sid), setup), 'NOT_SELECTED')
                    if pd.notna(sid) else 'NOT_SELECTED' for sid, setup in zip(audit.sid, audit.setup_id)]
            compared = ledgers['g'][['sid', 'setup_id', 'quantity', 'status', 'net_profit_rupees']].merge(
                ledgers['h'][['sid', 'setup_id', 'quantity', 'status', 'net_profit_rupees']],
                on=['sid', 'setup_id'], suffixes=('_g', '_h'), validate='one_to_one')
            compared['delta_rupees'] = compared.net_profit_rupees_h-compared.net_profit_rupees_g
            compared.to_parquet(out/'trade_attribution.parquet', index=False)
            # Keep execution-model changes separate from the sizing experiment.
            legacy = source['native_ledger'][['sid', 'setup_id', 'day', 'tradingsymbol', 'filled',
                'entry_price', 'exit_price', 'exit_reason', 'portfolio_net_profit_rupees']].copy()
            legacy['day'] = legacy.day.astype(str)
            legacy = legacy.merge(ledgers['g'][['sid', 'setup_id', 'status', 'entry_price', 'exit_price',
                                              'net_profit_rupees']], on=['sid', 'setup_id'],
                                  suffixes=('_legacy', '_shared'), validate='one_to_one')
            legacy['delta_rupees'] = legacy.net_profit_rupees-legacy.portfolio_net_profit_rupees
            legacy.to_parquet(out/'execution_model_attribution.parquet', index=False)
    audit.to_parquet(out/'decision_audit.parquet', index=False)
    write_json(out/'validation.json', validation)
    stages = {
        'G-0': 'IMPLEMENTED_EXACT_G_REPLAY_AND_IMMUTABLE_RUN',
        'G-1': 'PARTIAL_ALL_SOURCE_DATES_AND_SLOTS_AUDITED_ROSTER_ARRIVAL_AND_WARMUP_PROOF_MISSING',
        'G-2': 'IMPLEMENTED_SENSITIVITY_ENGINE_BROKER_COSTS_TICKS_DEPTH_UNVERIFIED',
        'G-3': 'IMPLEMENTED_AVAILABLE_CANDIDATE_GATES_MARGINS_RANK_AND_EXECUTION_JOIN',
        'G-4': 'PARTIAL_CAUSAL_DERIVED_CONTEXT_VIX_SECTOR_QUOTES_UNAVAILABLE',
        'G-5': 'IMPLEMENTED_ONE_FIXED_SIZING_TRIAL_NO_PARAMETER_SEARCH',
        'G-6': 'PARTIAL_CHRONOLOGICAL_DIAGNOSTICS_NO_UNTOUCHED_EVIDENCE',
        'G-7': 'BLOCKED_PAIRED_PROSPECTIVE_COLLECTOR_AND_FUTURE_SESSIONS_REQUIRED',
    }
    # Verify source integrity again before publishing a COMPLETE result.
    for name, checksum in source['hashes'].items():
        if sha(safe_child(source_run, name)) != checksum:
            raise ValueError(f'G source changed during H run: {name}')
    if sha(source['config_path']) != G_CONFIG_SHA or code_hashes() != plan['code_sha256']:
        raise ValueError('Configuration or code changed during the run')
    result = dict(schema=SCHEMA, state='COMPLETE', run_id=run_id, source_run=str(source_run),
                  source_through=max(source['days']), sessions=len(source['days']),
                  experiment_sha256=sha(out/'experiment.json'), exact_g_parity=source['parity'],
                  g_legacy_metrics=source['metadata']['metrics']['full_history'], scenarios=scenario_metrics,
                  candidate_state_counts={str(k): int(v) for k, v in audit.h_state.value_counts().items()},
                  available_context_rows=len(context), source_unchanged=True,
                  sizing_changed_orders=int(compared.quantity_g.ne(compared.quantity_h).sum()),
                  mean_daily_sizing_delta_rupees=validation['paired_mean_daily_delta_rupees'],
                  gates=dict(exact_g_replay=True, historical_positive_sizing_delta=primary_daily.delta_rupees.sum() > 0,
                             untouched_holdout=False, prospective_20_sessions=False, broker_cost_reconciliation=False,
                             independent_manual_review=False),
                  stages=stages, promotion_eligible=False, execution_authority=False,
                  live_configuration_changed=False, source_artifact_sha256=source['hashes'])
    result = _clean(result)
    write_json(out/'comparison.json', result)
    report = render_report(result, primary_daily, validation, plan)
    (out/'REPORT.md').write_text(report, encoding='utf-8')
    artifacts = {p.name if p.parent == out else p.relative_to(out).as_posix(): sha(p)
                 for p in sorted(out.rglob('*')) if p.is_file()}
    write_json(out/'manifest.json', dict(schema=SCHEMA, state='COMPLETE', artifacts=artifacts,
                                       execution_authority=False, promotion_eligible=False))
    return out


def render_report(result, daily, validation, plan):
    base = result['g_legacy_metrics']
    primary = result['scenarios']['DECLARED_MODEL']
    lines = ['# V13-V10-H versus frozen G', '',
             f"- Run: `{result['run_id']}`; {result['sessions']} included sessions through {result['source_through']}.",
             '- RESEARCH ONLY. No live changes, broker orders or automatic promotion.',
             f"- Experiment: `{plan['experiment']}`. Risk budget: {plan['risk_budget_rupees']} rupees.",
             '- G exact legacy replay reconciled per trade. Shared-model G and H use the same selections/exits.',
             '- This is reused history, not an untouched or prospective validation result.', '',
             '## Aggregate comparison', '', '| Metric | G saved/exact legacy | G shared execution model | H shared execution model |',
             '| --- | ---: | ---: | ---: |']
    for title, key, legacy in [('Net P&L INR', 'net_profit_rupees', base['net_profit_rupees']),
                              ('Executed trades', 'executed_trades', base['trades']),
                              ('Win rate %', 'win_rate_pct', base['win_rate_pct']),
                              ('Profit factor', 'profit_factor', base['profit_factor']),
                              ('Daily-close drawdown INR', 'daily_close_drawdown_rupees', base['daily_close_drawdown_rupees']),
                              ('Minute-close MTM drawdown INR', 'minute_close_mtm_drawdown_rupees', None)]:
        fmt = lambda x: 'UNAVAILABLE' if x is None else f'{x:,.2f}'
        lines.append(f'| {title} | {fmt(legacy)} | {fmt(primary["g"][key])} | {fmt(primary["h"][key])} |')
    lines += ['', f"Sizing-only H minus shared-model G: INR {primary['delta_net_rupees']:,.2f}.",
              'The difference from saved G also includes execution-model changes; it is not sizing alpha.', '',
              '## Execution sensitivities', '', '| Scenario | G net INR | H net INR | H minus G INR |', '| --- | ---: | ---: | ---: |']
    for name, item in result['scenarios'].items():
        lines.append(f"| {name} | {item['g']['net_profit_rupees']:,.2f} | {item['h']['net_profit_rupees']:,.2f} | {item['delta_net_rupees']:,.2f} |")
    lines += ['', '## Day-wise paired results', '', '| Day | G trades | H trades | G net INR | H net INR | Difference INR |',
              '| --- | ---: | ---: | ---: | ---: | ---: |']
    for row in daily.itertuples():
        lines.append(f'| {row.day} | {row.g_trades} | {row.h_trades} | {row.g_net_rupees:,.2f} | {row.h_net_rupees:,.2f} | {row.delta_rupees:,.2f} |')
    lines += ['', '## Validation and limitations', '',
              f"- Whole-day chronological diagnostic folds: {len(validation['folds'])}; no fitted prediction model.",
              f"- Exploratory block-bootstrap interval for mean daily H-G INR: {validation['exploratory_day_block_bootstrap_mean_delta_95pct']}.",
              '- No claim that 20 future sessions alone establishes statistical significance.',
              '- Uniform tick size is an assumption, not verified symbol/date tick metadata.',
              '- Float32 storage noise at valid tick boundaries is normalized before trigger comparisons.',
              '- Fees are a flat round-trip entry-notional proxy; itemized broker charges and market impact are unavailable.',
              '- Full-size fills are assumed; no depth, queue or partial-fill evidence. Both arms have identical exposure caps.',
              '- One-minute OHLC uses stop-first ambiguity; later stop gaps get adverse open fills.',
              '- Delays retain the absolute confirmation+10-minute deadline. Same-minute capital release is disallowed.',
              '- MTM is sampled at minute closes, not tick-level worst drawdown; half flat costs accrue at entry/exit.',
              '- Available-candidate coverage is not a proof of complete raw historical inputs or original arrival-time availability.',
              '- No absent VIX/sector/quote data is imputed as zero; context features cannot influence this sizing experiment.',
              '- Risk includes planned stop/flat costs; gaps and realized execution can exceed the budget.',
              '- Excluded source sessions and missing inputs remain visible in coverage.parquet.',
              '- Prospective H collection and independent approval are still required; promotion remains blocked.', '',
              '## Plan implementation status', '']
    lines.extend(f'- {key}: {value}' for key, value in result['stages'].items())
    return '\n'.join(lines)+'\n'
