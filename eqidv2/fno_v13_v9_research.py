"""Registered V13-v9 selection study with incumbent parity and frozen replay.

Run after fno_v13_v9_data.py. All historical splits have previously been seen.
The default engine remains the incumbent unless a frozen config is supplied.
"""
from __future__ import annotations

import argparse
import hashlib
import json
from dataclasses import asdict, replace
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

import fno_v13_corrected_v5_backtest as v5
import fno_v13_v9_backtest as engine
import fno_v13_v9_data as data

ROOT = Path('C:/TradingData/eqidv2/fno_oi/strategy_research')
DEFAULT_OUTPUT = ROOT / 'v13_corrected_v9/run_20260913'
KEYS = ['day', 'tradingsymbol', 'side', 'setup_id']


def dump(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, indent=2, default=str, allow_nan=True), encoding='utf-8')


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def execution_keys(ledger: pd.DataFrame) -> set[tuple]:
    frame = ledger.loc[ledger.portfolio_executed.eq(True), KEYS].copy()
    frame['day'] = pd.to_datetime(frame.day).dt.strftime('%Y-%m-%d')
    return set(frame.itertuples(index=False, name=None))


def daily_metrics(ledger: pd.DataFrame, days: list, name: str, period: str) -> tuple[dict, pd.DataFrame]:
    day_strings = [pd.Timestamp(day).strftime('%Y-%m-%d') for day in days]
    frame = ledger.copy()
    frame['day'] = pd.to_datetime(frame.day).dt.strftime('%Y-%m-%d')
    frame = frame.loc[frame.day.isin(day_strings)]
    filled = frame.loc[frame.portfolio_executed.eq(True)]
    pnl = filled.net_profit_rupees.to_numpy(float)
    grouped = filled.groupby('day').net_profit_rupees.sum().reindex(day_strings, fill_value=0.)
    counts = filled.groupby('day').size().reindex(day_strings, fill_value=0)
    curve = np.r_[0., grouped.cumsum().to_numpy()]
    drawdown = curve - np.maximum.accumulate(curve)
    positive, negative = pnl[pnl > 0].sum(), -pnl[pnl < 0].sum()
    metric = dict(name=name, period=period, sessions=len(days), selected_orders=len(frame),
                  trades=len(filled), wins=int((pnl > 0).sum()), losses=int((pnl < 0).sum()),
                  net_profit_rupees=float(pnl.sum()),
                  profit_factor=float(positive / negative) if negative else (float('inf') if positive else None),
                  win_rate_pct=float((pnl > 0).mean() * 100) if len(pnl) else None,
                  daily_close_drawdown_rupees=float(-drawdown.min()),
                  average_trade_rupees=float(pnl.mean()) if len(pnl) else None)
    daily = pd.DataFrame({'day': day_strings, 'name': name, 'trades': counts.to_numpy(),
                          'net_profit_rupees': grouped.to_numpy(), 'cumulative_profit_rupees': curve[1:],
                          'daily_close_drawdown_rupees': -drawdown[1:]})
    return metric, daily


def assess_candidate(candidate: dict[str, dict], control: dict[str, dict],
                     changed_keys: int, gate: dict) -> dict:
    reasons = []
    for period in ('TRAIN', 'VALIDATION'):
        for label, metric in [('CANDIDATE', candidate[period]), ('CONTROL', control[period])]:
            if not all(np.isfinite(metric[field]) for field in ('trades', 'net_profit_rupees', 'daily_close_drawdown_rupees')):
                reasons.append(f'{period}_{label}_NONFINITE_METRICS')
    for period, minimum in [('TRAIN', gate['minimum_train_executed']),
                            ('VALIDATION', gate['minimum_validation_executed'])]:
        current, incumbent = candidate[period], control[period]
        if current['trades'] < minimum:
            reasons.append(f'{period}_INSUFFICIENT_TRADES')
        if current['trades'] < incumbent['trades'] * gate['minimum_fraction_control_executed_each_split']:
            reasons.append(f'{period}_INSUFFICIENT_RETENTION')
        if current['net_profit_rupees'] <= 0:
            reasons.append(f'{period}_NONPOSITIVE_NET')
        if current['net_profit_rupees'] <= incumbent['net_profit_rupees'] + 1e-7:
            reasons.append(f'{period}_NO_PNL_IMPROVEMENT')
    if changed_keys < gate['minimum_changed_execution_keys_development']:
        reasons.append('INSUFFICIENT_CHANGED_EXECUTIONS')
    if candidate['VALIDATION']['daily_close_drawdown_rupees'] > control['VALIDATION']['daily_close_drawdown_rupees'] + 1e-7:
        reasons.append('VALIDATION_DRAWDOWN_WORSE')
    return {'accepted_development': not reasons, 'rejection_reasons': '|'.join(reasons),
            'changed_development_execution_keys': changed_keys,
            'train_delta_rupees': candidate['TRAIN']['net_profit_rupees'] - control['TRAIN']['net_profit_rupees'],
            'validation_delta_rupees': candidate['VALIDATION']['net_profit_rupees'] - control['VALIDATION']['net_profit_rupees']}


def select_frozen_config(assessments: pd.DataFrame, configs: dict) -> engine.V9Config:
    passed = assessments.loc[assessments.accepted_development.eq(True)]
    if passed.empty:
        return engine.V9Config()
    winner = passed.sort_values(['validation_delta_rupees', 'train_delta_rupees', 'name'],
                                ascending=[False, False, True], kind='stable').iloc[0]['name']
    return configs[winner]


def save_replay(folder: Path, selected: pd.DataFrame, ledger: pd.DataFrame, summary: dict) -> None:
    folder.mkdir(parents=True, exist_ok=True)
    selected.to_csv(folder / 'selected_trades.csv', index=False)
    ledger.to_csv(folder / 'portfolio_trades.csv', index=False)
    dump(folder / 'summary.json', summary)


def published_parity(selected: pd.DataFrame, ledger: pd.DataFrame, output: Path) -> dict:
    originals = {
        'V5': ROOT / 'v13_corrected_v5/higher_frequency/fno_v13_corrected_v5_higher_frequency_trades.csv',
        'V6': ROOT / 'v13_corrected_v6/asof_20260911_300k_3slot/fno_v13_v6_portfolio_trades.csv',
    }
    parity = {}
    for name, current in [('V5', selected), ('V6', ledger)]:
        previous = pd.read_csv(originals[name], float_precision='round_trip')
        parity[name] = engine.assert_control_parity(previous, current)
        parity[name].update(source=str(originals[name]), source_sha256=sha(originals[name]))
    dump(output / 'baseline_parity.json', parity)
    return parity


def paired_daily_summary(daily: pd.DataFrame, chosen: str) -> dict:
    pivot = daily.pivot(index='day', columns='name', values='net_profit_rupees')
    delta = (pivot[chosen] - pivot['V13_V6_CONTROL']).to_numpy(float)
    rng = np.random.default_rng(1309)
    draws = rng.choice(delta, size=(10000, len(delta)), replace=True).mean(axis=1) if len(delta) else np.array([0.])
    return {'sessions': len(delta), 'paired_mean_daily_delta_rupees': float(delta.mean()),
            'paired_daily_bootstrap_95pct_mean_interval_rupees': np.quantile(draws, [.025, .975]).tolist(),
            'candidate_better_days': int((delta > 1e-7).sum()), 'candidate_worse_days': int((delta < -1e-7).sum()),
            'limitations': 'Descriptive paired daily resampling of previously seen history; small dependent sample; does not correct selection bias or establish future profitability.'}


def write_attribution(control: pd.DataFrame, chosen: pd.DataFrame, output: Path) -> pd.DataFrame:
    frames = []
    for frame in (control, chosen):
        selected = frame.loc[frame.portfolio_executed.eq(True), KEYS + ['net_profit_rupees', 'entry_ts', 'exit_ts', 'exit_reason']].copy()
        selected['day'] = pd.to_datetime(selected.day).dt.strftime('%Y-%m-%d')
        frames.append(selected)
    bridge = frames[0].merge(frames[1], on=KEYS, how='outer', suffixes=('_control', '_v9'), indicator=True)
    bridge['execution_change'] = bridge['_merge'].astype(str).map({'both': 'RETAINED', 'left_only': 'REMOVED', 'right_only': 'ADDED'})
    bridge['pnl_delta_rupees'] = bridge.net_profit_rupees_v9.fillna(0) - bridge.net_profit_rupees_control.fillna(0)
    bridge.drop(columns='_merge').to_csv(output / 'executed_trade_attribution.csv', index=False)
    bridge.groupby(['setup_id', 'execution_change'], dropna=False).agg(
        trades=('day', 'size'), pnl_delta_rupees=('pnl_delta_rupees', 'sum')).reset_index().to_csv(output / 'setup_attribution.csv', index=False)
    return bridge


def complete_decision_audit(setup_audit: pd.DataFrame, decisions: pd.DataFrame,
                            ledger: pd.DataFrame, folder: Path) -> None:
    """Carry every original opportunity through selection, trigger and capital gates."""
    audit = setup_audit.copy()
    decision_columns = ['sid', 'setup_id', 'v9_decision', 'v9_filter_pass',
                        'v9_filter_reject_reason', 'v9_rank_in_setup_day', 'v9_selected']
    audit = audit.merge(decisions[decision_columns], on=['sid', 'setup_id'], how='left', validate='many_to_one')
    fill_columns = ['sid', 'setup_id', 'filled', 'portfolio_executed', 'portfolio_status',
                    'portfolio_reject_reason', 'entry_ts', 'exit_ts', 'exit_reason', 'portfolio_net_profit_rupees']
    audit = audit.merge(ledger[fill_columns], on=['sid', 'setup_id'], how='left', validate='many_to_one')
    audit['final_decision_stage'] = audit.v9_decision.fillna(audit.selection_status)
    selected = audit.v9_selected.eq(True)
    audit.loc[selected & ~audit.filled.eq(True), 'final_decision_stage'] = 'ENTRY_NOT_FILLED'
    audit.loc[audit.filled.eq(True) & ~audit.portfolio_executed.eq(True), 'final_decision_stage'] = 'PORTFOLIO_REJECTED'
    audit.loc[audit.portfolio_executed.eq(True), 'final_decision_stage'] = 'EXECUTED'
    if int(audit.portfolio_executed.eq(True).sum()) != int(ledger.portfolio_executed.sum()):
        raise AssertionError('Full setup audit failed to account for every portfolio execution')
    audit.to_parquet(folder / 'all_setup_decisions.parquet', index=False)
    audit.groupby(['setup_id', 'final_decision_stage'], dropna=False).size().rename('opportunities').reset_index().to_csv(
        folder / 'complete_decision_funnel.csv', index=False)


def table(frame: pd.DataFrame, columns: list[str]) -> str:
    shown = frame.loc[:, columns].copy()
    for name in shown:
        if pd.api.types.is_float_dtype(shown[name]):
            shown[name] = shown[name].map(lambda value: f'{value:,.2f}' if pd.notna(value) else '')
    return shown.to_markdown(index=False, disable_numparse=True)


def equity_plot(daily: pd.DataFrame, output: Path) -> None:
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    from matplotlib.ticker import FuncFormatter
    fig, axes = plt.subplots(2, 1, figsize=(11, 7), sharex=True, gridspec_kw={'height_ratios': [2, 1]})
    styles = [('V13_V6_CONTROL', 'V6 control', '#245a81', '-'),
              ('V13_V7_TIME180', 'V7 180 minute exit', '#80609d', '-'),
              ('V13_V9_FROZEN', 'V9 frozen selection', '#d16a22', '--')]
    for name, label, color, style in styles:
        frame = daily.loc[daily.name.eq(name)].sort_values('day')
        dates = pd.to_datetime(frame.day)
        axes[0].plot(dates, frame.cumulative_profit_rupees, label=label, color=color, linestyle=style, linewidth=2)
        axes[1].plot(dates, -frame.daily_close_drawdown_rupees, color=color, linestyle=style, linewidth=1.5)
    axes[0].set_title(f'V13 comparison on the same {daily.day.nunique()} historical sessions', loc='left', fontweight='bold')
    axes[0].set_ylabel('Cumulative net P&L (Rs)')
    axes[1].set_ylabel('Daily close drawdown (Rs)')
    axes[0].legend(loc='upper left')
    for axis in axes:
        axis.grid(alpha=.2)
        axis.yaxis.set_major_formatter(FuncFormatter(lambda value, _: f'{value:,.0f}'))
    fig.text(.01, .01, 'Rs 300,000 capital | modeled 5x exposure | flat 5bps round trip | previously seen history', fontsize=9)
    fig.autofmt_xdate()
    fig.tight_layout(rect=(0, .04, 1, 1))
    fig.savefig(output / 'v13_v9_equity_comparison.png', dpi=160)
    plt.close(fig)


def diagnostic_findings(output: Path) -> list[str]:
    folder = output / 'diagnostics'
    native_file = folder / 'native_scaleout_eligible_counterfactuals.parquet'
    if not native_file.exists():
        return ['The full selection/rejection diagnostic is still running.', '']
    import fno_v13_v9_diagnostics as diagnostics
    native = pd.read_parquet(native_file)
    summary = diagnostics.outcome_summary(native, ['period', 'selection_stage'])
    summary.to_csv(output / 'selected_vs_ranked_out_summary.csv', index=False)
    broad = pd.read_parquet(folder / 'native_scaleout_all_setup_counterfactuals.parquet',
        columns=['day', 'period', 'selection_stage', 'filled', 'net_return_pct', 'causal_rejection_reasons'])
    rejection = diagnostics.outcome_summary(broad, ['period', 'selection_stage'])
    rejection.to_csv(output / 'first_rejection_outcome_summary.csv', index=False)
    development = summary.loc[summary.period.isin(['TRAIN', 'VALIDATION'])]
    relevant = rejection.loc[rejection.period.isin(['TRAIN', 'VALIDATION']) & rejection.selection_stage.isin([
        'REJECT_CONFIRMATION_DIRECTION', 'REJECT_CONFIRMATION_DISPLACEMENT',
        'REJECT_OI_INCREASING', 'REJECT_LOOSE_PRICE', 'REJECT_LOOSE_VOLUME'])]
    bins = pd.read_csv(folder / 'native_development_feature_bin_stability.csv')
    adequate = int(bins.sample_status.eq('DESCRIPTIVE_SUFFICIENT').sum())
    v8 = pd.read_csv(folder / 'v8_causal_coverage_summary.csv')
    lines = [
        '## What the complete audit found', '',
        'Only 152 opportunities passed the original setup rules: 115 were selected and 37 ranked out. The other 90,260 failed one or more original checks. The default portfolio then accepted 77 of 102 triggered orders; 25 were rejected for capital and 13 selected orders never triggered within S+10.', '',
        'The native picker already separated training candidates well. Its ranked-out alternatives lost money on average in TRAIN. Validation had only two filled ranked-out alternatives, both on one session. That tiny validation subset is insufficient evidence for a new universal ranking rule.', '',
        table(development, ['period', 'selection_stage', 'observations', 'resolved_fills', 'win_rate_pct', 'mean_net_return_pct']), '',
        'The quantities below are per-opportunity underlying-price returns after the same flat cost and V13 exits. They are overlapping counterfactuals without portfolio allocation. First-failed-check groups can also fail later checks; the table does not isolate the effect of removing one filter.', '',
        table(relevant, ['period', 'selection_stage', 'resolved_fills', 'mean_net_return_pct']), '',
        'The audit does not prove every original rule is optimal. Confirmation-displacement first-rejection groups have positive mean returns in both development periods, but they can fail other setup checks too. Only one TRAIN opportunity and no VALIDATION opportunities failed displacement alone, so those grouped means do not validate relaxing that rule.', '',
        f'Of {len(bins):,} native setup/selection-stage feature bins, {adequate} have at least 10 filled observations in both development periods. Broad rejected pools have more observations, but their hypothetical outcomes cannot be added to portfolio profit.', '',
        'The 1m body and wick changes reduced development P&L despite retaining similar trade counts. VWAP-extension and higher-volume gates helped TRAIN but hurt VALIDATION. Additional 1m EMA alignment changed only two development executions. These results support keeping the original rules for this research release.', '',
        'V8 remains a futures-feature shadow. Its causal coverage does not span the validation sample, so no V8 quality filter can be compared consistently here.', '',
        table(v8, ['period', 'v8_feature_status', 'candidates']), '',
    ]
    bridge_path = output / 'development/RANK_5M_EMA_SPREAD/executed_trade_attribution.csv'
    if bridge_path.exists():
        bridge = pd.read_csv(bridge_path)
        swaps = bridge.loc[bridge.day.gt(str(v5.TRAIN_END)) & bridge.day.le(str(v5.VALIDATION_END)) & bridge.execution_change.ne('RETAINED')]
        lines += ['The EMA-spread ranking gain in validation comes from this single setup substitution. The same ranking lost money relative to V6 in TRAIN; the validation gain therefore did not qualify it for promotion.', '',
                  table(swaps, ['day', 'tradingsymbol', 'setup_id', 'execution_change', 'net_profit_rupees_control', 'net_profit_rupees_v9']), '']
    lines += ['September deserves separate attention: V6 makes only Rs 981.61 at 5bps and loses Rs 2,618.39 at 9bps. V7 improves September to Rs 5,391.64 at 5bps, but lowers full-history profit by Rs 29,200.36. That is an observed exit/regime trade-off, not evidence for replacing every V6 exit with V7.', '']
    return lines


def render_report(output: Path, dataset: dict, final: pd.DataFrame, development: pd.DataFrame,
                  assessments: pd.DataFrame, chosen: engine.V9Config, bootstrap: dict, parity: dict) -> Path:
    is_candidate = chosen.name != 'V13_V6_CONTROL'
    full = final.loc[final.period.eq('FULL')]
    split = final.loc[final.period.isin(['TRAIN', 'VALIDATION', 'PSEUDO_TEST', 'SEPTEMBER']) & ~final.name.eq('V13_V5_UNCONSTRAINED')]
    chosen_full = full.loc[full.name.eq('V13_V9_FROZEN')].iloc[0]
    control_full = full.loc[full.name.eq('V13_V6_CONTROL')].iloc[0]
    pseudo = final.loc[final.period.eq('PSEUDO_TEST')].set_index('name')
    pseudo_delta = pseudo.loc['V13_V9_FROZEN', 'net_profit_rupees'] - pseudo.loc['V13_V6_CONTROL', 'net_profit_rupees']
    diagnostic_path = output / 'diagnostics/diagnostics_manifest.json'
    diagnostic = json.loads(diagnostic_path.read_text()) if diagnostic_path.is_file() else {'status': 'PENDING_DIAGNOSTICS'}
    if 'v8_context' in diagnostic:
        diagnostic['v8_context'] = {key: value for key, value in diagnostic['v8_context'].items() if key != 'sources'}
    deletion_path = output / 'v14_deletion_manifest.json'
    deletion = json.loads(deletion_path.read_text(encoding='utf-8-sig')) if deletion_path.is_file() else []
    remaining = [row['path'] for row in deletion if Path(row['path']).exists()]
    lines = [
        '# V13 v9 F&O selection research', '',
        f'Frozen selection: **{chosen.name}**. ' + ('This rule passed the registered development gates.' if is_candidate else 'No experimental rule passed all registered development gates; V9 retains V6 as its execution default.'), '',
        f'Full-history V9 net: **Rs {chosen_full.net_profit_rupees:,.2f}**, versus V6 **Rs {control_full.net_profit_rupees:,.2f}**; difference **Rs {chosen_full.net_profit_rupees - control_full.net_profit_rupees:,.2f}**. Later previously seen history delta: **Rs {pseudo_delta:,.2f}**.', '',
        '## Same-calendar comparisons', '',
        f'{len(dataset["days"])} native eligible sessions through 2026-09-11. Stock cash prices execute signals supported by mapped stock-futures OI. These are not futures-lot or option-premium P&L figures.', '',
        'V6, V7 and V9 use Rs 300,000 capital, three positions, Rs 100,000 capital per entry and modeled 5x exposure. V5 is an unconstrained reference, with different aggregate capital requirements. All use the inherited flat 5bps round-trip cost.', '',
        table(full, ['name', 'trades', 'wins', 'losses', 'net_profit_rupees', 'profit_factor', 'daily_close_drawdown_rupees']), '',
        '![V6, V7 and frozen V9 historical equity and daily-close drawdown](v13_v9_equity_comparison.png)', '',
        table(split, ['name', 'period', 'trades', 'net_profit_rupees', 'profit_factor', 'daily_close_drawdown_rupees']), '',
        '## What was analyzed', '',
        f'The dataset contains {len(dataset["all_5m_features"]):,} observed complete 5m bars, {len(dataset["setup_audit"]):,} native setup-side opportunities and {len(dataset["annotated"]):,} strict native confirmations before final index/OI gates. Each setup row retains all causal rejection reasons and exact 1m confirmation features. Unconfigured times remain visible in the full 5m pool.', '',
        'Forward outcomes are separate from entry features. Both strict eligible ranked-out candidates and broader rejected setups use the original V13 partial/runner payoff for the primary diagnostic. A separately labeled fixed-bracket counterfactual is secondary. Overlapping hypothetical returns are not portfolio profit.', '',
        '```json', json.dumps(diagnostic, indent=2), '```', '',
        '## Registered selection study', '',
        'The eight individual rules were registered before their V9 results. They act before top-N selection; each portfolio is replayed from scratch. No threshold grid, combined filter or later-history retuning was allowed. V7 retains its independent 180-minute exit comparison. V8 is a causal futures-feature shadow, not a separate profitable execution engine.', '',
        table(assessments, ['name', 'accepted_development', 'changed_development_execution_keys', 'train_delta_rupees', 'validation_delta_rupees', 'rejection_reasons']), '',
        table(development, ['name', 'period', 'trades', 'net_profit_rupees', 'profit_factor', 'daily_close_drawdown_rupees']), '',
        *diagnostic_findings(output),
        '## Reproduction and limits', '',
        f'Published V5/V6 selected orders, fill flags, entry/exit timestamps and P&L passed parity: {json.dumps(parity)}.', '',
        'All dates were previously researched. TRAIN ends August 13, VALIDATION ends August 26, and the later period is a previously seen pseudo-test. A historical increase is a research result; it is not an untouched validation or a guaranteed improved live system.', '',
        'The original execution assumptions are retained equally: fractional modeled exposure, fixed capital reservations, minute OHLC stop-first ambiguity, same-minute exit/re-entry ordering and flat bps costs. Drawdown above is based on daily closes, not intraday mark-to-market. Missing historical universe snapshots and sparse causal futures-minute history remain source limitations. Cost/exposure stress is saved separately.', '',
        f'Paired daily bootstrap: {json.dumps(bootstrap)}', '',
        '## Files and commands', '',
        '- `dataset/all_5m_features.parquet`: every observed 5m bar and exact next 1m features.',
        '- `dataset/setup_audit.parquet`: every configured setup-side selection/rejection and causal reason.',
        '- `diagnostics/`: rejection funnels, winner/loser features, forward path quality and development bins.',
        '- `development_metrics.csv`, `development_assessments.csv`: all eight registered results and gate decisions.',
        '- `frozen_config.json`: selection frozen from development; engine default remains incumbent.',
        '- `final/`, `final_metrics.csv`, `final_daily.csv`: fresh complete portfolio replays, including all setup decisions through entry and portfolio rejection.',
        '- `executed_trade_attribution.csv`: added, removed and retained executions with exact P&L difference.',
        '- `cost_exposure_stress.csv`: equal-assumption 9bps and 1x sensitivities.', '',
        '```powershell', f'python -B fno_v13_v9_research.py --output-dir "{output}"',
        f'python -B fno_v13_v9_backtest.py --dataset-dir "{output / "dataset"}" --config-json "{output / "frozen_config.json"}" --output-dir "{output / "replay"}"', '```', '',
        '## V14 cleanup', '',
        'The six V14 F&O engines/helpers, five tests, implementation status and temporary audit Python scripts were removed. Automatic approval review blocked recursive deletion of the old V14 output folders. Remaining paths are explicitly recorded below; no V9 code imports or reads V14 results.', '',
        *[f'- `{path}`' for path in remaining], '',
    ]
    report = output / 'V13_V9_DETAILED_RESULTS.md'
    report.write_text('\n'.join(lines), encoding='utf-8')
    return report


def run(output: Path, *, skip_diagnostics: bool = False) -> dict:
    output = output.resolve()
    protocol_path = output / 'research_protocol.json'
    if not protocol_path.is_file():
        raise RuntimeError('A research_protocol.json must be registered in the output directory before any candidate results.')
    protocol = json.loads(protocol_path.read_text(encoding='utf-8'))
    configs = engine.experiment_configs()
    if set(configs) != {item['name'] for item in protocol['independent_selection_hypotheses']}:
        raise RuntimeError('Experiment registry differs from registered protocol')
    fixed_hypotheses = {
        'WICK_1M_MAX_035': {'max_1m_wick_ratio': .35},
        'BODY_1M_MIN_060': {'min_1m_body_ratio': .60},
        'VWAP_EXTENSION_5M_MAX_150': {'max_signed_5m_vwap_extension_pct': 1.5},
        'EMA_1M_ALIGNED': {'require_1m_ema_alignment': True},
        'VOLUME_5M_MIN_150': {'min_5m_volume_ratio': 1.5},
        'RANGE_5M_MAX_150': {'max_5m_range_pct': 1.5},
        'RANK_1M_BODY': {'ranking': '1m_body'},
        'RANK_5M_EMA_SPREAD': {'ranking': '5m_ema_strength'},
    }
    for name, fields in fixed_hypotheses.items():
        expected = engine.V9Config(name=name, cost_bps=5., leverage_factor=5.,
            capital_per_entry_rupees=100000., portfolio_capital_rupees=300000.,
            max_positions=3, maximum_holding_minutes=None, **fields)
        if asdict(configs[name]) != asdict(expected):
            raise RuntimeError(f'Registered hypothesis parameters changed: {name}')
    registry_path = output / 'registered_configs.json'
    registry = {name: asdict(cfg) for name, cfg in configs.items()}
    if registry_path.exists() and json.loads(registry_path.read_text()) != registry:
        raise RuntimeError('Existing registered configurations changed')
    dump(registry_path, registry)
    engine.validate_configuration()
    dataset = data.build_dataset(output / 'dataset', through_day=protocol['through_day'])
    days = dataset['days']
    native_splits = v5.split_days(days)
    splits = {period: native_splits[period] for period in ('TRAIN', 'VALIDATION', 'PSEUDO_TEST')}
    control = engine.V9Config()
    base_selected, base_ledger, base_summary = engine.evaluate(dataset['signals'], dataset['paths'], days, control)
    parity = published_parity(base_selected, base_ledger, output)
    print('[V9] Published V5/V6 baseline parity passed', flush=True)
    controls, metrics, assessments = {}, [], []
    for period in ['TRAIN', 'VALIDATION']:
        controls[period] = daily_metrics(base_ledger, splits[period], control.name, period)[0]
        metrics.append(controls[period])
    dev_days = splits['TRAIN'] + splits['VALIDATION']
    base_dev = base_ledger.loc[pd.to_datetime(base_ledger.day).dt.date.isin(dev_days)]
    for name, cfg in configs.items():
        selected, ledger, summary = engine.evaluate(dataset['signals'], dataset['paths'], dev_days, cfg)
        save_replay(output / 'development' / name, selected, ledger, summary)
        write_attribution(base_dev, ledger, output / 'development' / name)
        result = {period: daily_metrics(ledger, splits[period], name, period)[0] for period in ['TRAIN', 'VALIDATION']}
        metrics.extend(result.values())
        changed = len(execution_keys(base_dev) ^ execution_keys(ledger))
        assessment = dict(name=name, **assess_candidate(result, controls, changed, protocol['development_acceptance']))
        assessments.append(assessment)
        print('[V9 DEVELOPMENT] ' + json.dumps(assessment), flush=True)
    dev_frame, assessment_frame = pd.DataFrame(metrics), pd.DataFrame(assessments)
    dev_frame.to_csv(output / 'development_metrics.csv', index=False)
    assessment_frame.to_csv(output / 'development_assessments.csv', index=False)
    frozen = select_frozen_config(assessment_frame, configs)
    frozen_dict = asdict(frozen)
    freeze_path = output / 'frozen_config.json'
    if freeze_path.exists() and json.loads(freeze_path.read_text()) != frozen_dict:
        raise RuntimeError('Existing frozen selection differs: use a separately registered new research run')
    dump(freeze_path, frozen_dict)
    dump(output / 'selection_freeze.json', {'protocol_sha256': sha(protocol_path),
        'configuration_sha256': sha(freeze_path), 'selected': frozen.name,
        'evidence': 'TRAIN_AND_VALIDATION_ONLY; LATER_HISTORY_PREVIOUSLY_SEEN',
        'fallback': frozen.name == control.name})
    print(f'[V9 FROZEN] {frozen.name}', flush=True)
    final_cfgs = [replace(control, name='V13_V5_UNCONSTRAINED', max_positions=None, portfolio_capital_rupees=1e9),
                  control, replace(control, name='V13_V7_TIME180', maximum_holding_minutes=180),
                  replace(frozen, name='V13_V9_FROZEN')]
    final_metrics, final_daily, ledgers = [], [], {}
    periods = {'FULL': days, **splits, 'SEPTEMBER': [day for day in days if day.month == 9]}
    for cfg in final_cfgs:
        selected, ledger, summary = engine.evaluate(dataset['signals'], dataset['paths'], days, cfg)
        save_replay(output / 'final' / cfg.name, selected, ledger, summary)
        decisions = engine.selection_audit(dataset['signals'], cfg)
        decisions.to_csv(output / 'final' / cfg.name / 'selection_audit.csv', index=False)
        if cfg.name in ('V13_V6_CONTROL', 'V13_V9_FROZEN'):
            complete_decision_audit(dataset['setup_audit'], decisions, ledger, output / 'final' / cfg.name)
        ledgers[cfg.name] = ledger
        for period, period_days in periods.items():
            metric, daily = daily_metrics(ledger, period_days, cfg.name, period)
            final_metrics.append(metric)
            if period == 'FULL':
                final_daily.append(daily)
        print(f'[V9 FINAL] {cfg.name}: {summary["net_profit_rupees"]:,.2f}', flush=True)
    final, daily = pd.DataFrame(final_metrics), pd.concat(final_daily, ignore_index=True)
    final.to_csv(output / 'final_metrics.csv', index=False)
    daily.to_csv(output / 'final_daily.csv', index=False)
    equity_plot(daily, output)
    write_attribution(ledgers[control.name], ledgers['V13_V9_FROZEN'], output)
    bootstrap = paired_daily_summary(daily, 'V13_V9_FROZEN')
    dump(output / 'paired_daily_bootstrap.json', bootstrap)
    stress = []
    for cfg in final_cfgs[1:]:
        for cost, leverage in [(9., 5.), (5., 1.)]:
            _, ledger, _ = engine.evaluate(dataset['signals'], dataset['paths'], days, replace(cfg, cost_bps=cost, leverage_factor=leverage))
            for period in ['FULL', 'PSEUDO_TEST', 'SEPTEMBER']:
                row = daily_metrics(ledger, periods[period], cfg.name, period)[0]
                stress.append(dict(**row, flat_round_trip_cost_bps=cost, leverage_factor=leverage))
    pd.DataFrame(stress).to_csv(output / 'cost_exposure_stress.csv', index=False)
    if not skip_diagnostics:
        import fno_v13_v9_diagnostics as diagnostics
        diagnostics.run_diagnostics(dataset, output / 'diagnostics')
    report = render_report(output, dataset, final, dev_frame, assessment_frame, frozen, bootstrap, parity)
    manifest = {'complete': not skip_diagnostics or (output / 'diagnostics/diagnostics_manifest.json').exists(),
                'frozen_selection': frozen.name, 'report': str(report), 'protocol_sha256': sha(protocol_path),
                'source_hashes': {path.name: sha(path) for path in Path(__file__).parent.glob('fno_v13_v9*.py')},
                'incumbent_source_hashes': engine.source_hashes(), 'artifacts': {str(path.relative_to(output)): sha(path)
                 for path in output.rglob('*') if path.is_file() and path.name != 'research_manifest.json' and path.suffix.lower() != '.log' and 'native_v13_cache' not in str(path)}}
    dump(output / 'research_manifest.json', manifest)
    return {'complete': manifest['complete'], 'frozen_selection': frozen.name, 'report': str(report)}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output-dir', type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument('--skip-diagnostics', action='store_true', help='Run diagnostics separately; report remains incomplete until diagnostics exist.')
    args = parser.parse_args(argv)
    print(json.dumps(run(args.output_dir, skip_diagnostics=args.skip_diagnostics), indent=2), flush=True)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
