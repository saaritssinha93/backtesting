"""V13-v10-F: selection research with V10-E's strict volume filter."""
from __future__ import annotations

import argparse
import copy
import json
from dataclasses import asdict, dataclass, replace
from pathlib import Path

import numpy as np
import pandas as pd

import fno_v13_v10_e_backtest as e

b, d, v10, v9, r = e.b, e.d, e.d.v10, e.v9, e.r
DEFAULT_SOURCE = e.DEFAULT_SOURCE
DEFAULT_SOURCE_CONFIG = e.DEFAULT_SOURCE_CONFIG
DEFAULT_OUTPUT = e.DEFAULT_OUTPUT.parent.parent / 'v13_corrected_v10_f/run_20260913_volume120'
VOLUME_RATIO = 1.20


@dataclass(frozen=True)
class SelectionChange:
    oi_multiplier: float = 1.0
    price_multiplier: float = 1.0
    body_reduction: float = 0.0
    wick_increase: float = 0.0
    extra_setup_entries: int = 0
    entry_expiry_minutes: int = 10
    preserve_primary: bool = False
    additional_min_body: float = 0.0
    expansion_side: str = 'BOTH'

    def validate(self):
        for name in ('oi_multiplier', 'price_multiplier'):
            value = getattr(self, name)
            if not np.isfinite(value) or not .5 <= value <= 1:
                raise ValueError(f'{name} must be finite between 0.5 and 1')
        for name in ('body_reduction', 'wick_increase'):
            value = getattr(self, name)
            if not np.isfinite(value) or not 0 <= value <= .1:
                raise ValueError(f'{name} must be finite between 0 and 0.1')
        if self.extra_setup_entries not in (0, 1):
            raise ValueError('At most one extra order per setup')
        if self.entry_expiry_minutes not in (10, 15):
            raise ValueError('Entry expiry must be 10 or 15 minutes')
        if self.additional_min_body not in (0., .6):
            raise ValueError('Additional candidate body ratio must be 0 or 0.6')
        if self.expansion_side not in ('BOTH', 'LONG', 'SHORT'):
            raise ValueError('Invalid expansion side')


DEFAULT_CHANGE = SelectionChange(oi_multiplier=.5, expansion_side='SHORT')


def config(source_exit: dict, change: SelectionChange = DEFAULT_CHANGE) -> dict:
    change.validate()
    result = e.config(copy.deepcopy(source_exit), VOLUME_RATIO)
    result.update(
        version='V13-v10-F', source_version='V13-v10-E',
        selection_change=asdict(change), entry_expiry_minutes=change.entry_expiry_minutes,
        selection_rule='Volume ratio >=1.20 before setup gates and ranking; apply selection_change to native setup thresholds.',
        evidence='EXPLORATORY_REUSE_OF_PREVIOUSLY_REVIEWED_HISTORY_NO_UNTOUCHED_TEST',
    )
    return result


def checked_settings(settings: dict) -> dict:
    threshold = float(settings['minimum_confirmation_1m_volume_ratio'])
    if not np.isfinite(threshold) or threshold != VOLUME_RATIO:
        raise ValueError('V10-F requires E volume ratio >=1.20, with no lower-volume fallback')
    if 'fallback_confirmation_1m_volume_ratio' in settings:
        raise ValueError('Obsolete lower-volume fallback config; use the corrected V10-F config')
    change = SelectionChange(**settings['selection_change'])
    if settings['entry_expiry_minutes'] != change.entry_expiry_minutes:
        raise ValueError('Conflicting entry expiry settings')
    return config(settings['exit'], change)


def selection_audit(signals: pd.DataFrame, base: v9.V9Config,
                    change: SelectionChange = DEFAULT_CHANGE) -> pd.DataFrame:
    change.validate()
    base.validate()
    if base.ranking != 'native':
        raise ValueError('V10-F preserves native ranking')
    volume = pd.to_numeric(signals.v9_1m_volume_ratio, errors='coerce')
    available = signals.loc[np.isfinite(volume) & volume.ge(VOLUME_RATIO)].copy()
    stamp = pd.to_datetime(available.v9_1m_feature_ts, utc=True, errors='coerce')
    cutoff = pd.to_datetime(available.confirmation_ts, utc=True)
    if (stamp.isna() | stamp.gt(cutoff)).any():
        raise ValueError('Noncausal or missing confirmation volume timestamp')
    parts = []
    for original in v9.v5.profile_setups(v9.v5.PROFILES['higher_frequency']):
        applied = change if change.expansion_side in ('BOTH', original.side) else SelectionChange()
        setup = replace(
            original, oi_change_pct=original.oi_change_pct * applied.oi_multiplier,
            price_change_pct=original.price_change_pct * applied.price_multiplier,
            body_ratio=max(0., original.body_ratio - applied.body_reduction),
            max_wick_ratio=min(1., original.max_wick_ratio + applied.wick_increase),
            max_entries=original.max_entries + applied.extra_setup_entries,
        )
        rows = v9._filter_and_score(v9.v5.replay._eligible(available, setup), base)
        if rows.empty:
            continue
        picker = v9.PICKER_COLUMNS[setup.picker]
        if picker == 'abs_price_change_pct':
            rows[picker] = rows.price_change_pct.abs()
        passed = rows.loc[rows.v9_filter_pass]
        ranked = passed.sort_values(
            ['day', picker, 'traded_value', 'tradingsymbol'],
            ascending=[True, False, False, True], kind='stable',
        )
        selected = v9.v5.replay.select_setup_rows(passed, setup)
        if change.preserve_primary:
            primary = v9.v5.replay.select_setup_rows(passed, original)
            occupied_days = set(primary.day.astype(str))
            additional = passed.loc[~passed.day.astype(str).isin(occupied_days)]
            if change.additional_min_body:
                v9._validate_feature_clock(additional, replace(base, min_1m_body_ratio=change.additional_min_body))
                additional = additional.loc[additional.v9_1m_body_ratio.ge(change.additional_min_body)]
            selected = pd.concat([primary, v9.v5.replay.select_setup_rows(additional, setup)])
        rows['v9_selected'] = rows.index.isin(selected.index)
        rows['v9_rank_in_setup_day'] = ranked.groupby('day', sort=False).cumcount() + 1
        rows['v9_decision'] = np.where(rows.v9_selected, 'SELECTED', 'RANKED_OUT')
        rows['v9_configuration'] = 'V13_V10_F'
        rows = v9._setup_metadata(rows, setup)
        rows['v10_f_required_oi_pct'] = setup.oi_change_pct
        rows['v10_f_required_price_pct'] = setup.price_change_pct
        rows['v10_f_required_body_ratio'] = setup.body_ratio
        rows['v10_f_maximum_wick_ratio'] = setup.max_wick_ratio
        parts.append(rows)
    if not parts:
        return v9.selection_audit(available.iloc[:0], base)
    return pd.concat(parts, ignore_index=True).sort_values(
        ['day', 'hhmm_int', 'side', 'setup_id', 'tradingsymbol'], kind='stable'
    ).reset_index(drop=True)


def select_orders(signals, base, change: SelectionChange = DEFAULT_CHANGE):
    audit = selection_audit(signals, base, change)
    return audit.loc[audit.v9_selected].reset_index(drop=True)


def orders_dataset(base, signals, archive, orders):
    paths = {
        int(sid): {field: archive[f'{int(sid)}_{field}']
                   for field in ('timestamp_ns', 'open', 'high', 'low', 'close')}
        for sid in orders.sid
    }
    v9.validate_paths(orders, paths)
    return {**base, 'signals': signals, 'orders': orders, 'paths': paths}


def load_source(source: Path = DEFAULT_SOURCE, settings: dict | None = None):
    if settings is None:
        settings = config(json.loads(DEFAULT_SOURCE_CONFIG.read_text(encoding='utf-8')))
    checked = checked_settings(settings)
    base = v10.load_source(source)
    signals = pd.read_parquet(source / 'dataset/signals.parquet')
    orders = select_orders(signals, base['v9_config'], SelectionChange(**checked['selection_change']))
    with np.load(source / 'dataset/paths.npz', allow_pickle=False) as archive:
        return orders_dataset(base, signals, archive, orders)


def evaluate(dataset, settings):
    checked = checked_settings(settings)
    volume = pd.to_numeric(dataset['orders'].v9_1m_volume_ratio, errors='coerce')
    if not (np.isfinite(volume) & volume.ge(VOLUME_RATIO)).all():
        raise ValueError('An order violates the strict 1.20 volume filter')
    if checked['entry_expiry_minutes'] == 10:
        trades, ledger, summary = b.evaluate(dataset, checked['exit'])
    else:
        base = dataset['v9_config']
        exits = checked['exit']
        orders = dataset['orders'].copy()
        for field in ('stop_pct', 'target_pct'):
            orders[f'native_{field}'] = orders.setup_id.map(
                {key: pair[field] for key, pair in exits['setups'].items()}
            ).fillna(exits['default'][field])
        v9.validate_paths(orders, dataset['paths'])
        trades = v9.v5.simulate_native(
            orders, dataset['paths'], cost_bps=base.cost_bps,
            max_entry_delay_minutes=checked['entry_expiry_minutes'],
        )
        trades = v9.v5.apply_fixed_capital_model(
            trades, base.capital_per_entry_rupees, base.leverage_factor
        )
        ledger, summary = v9.v6.apply_portfolio_constraints(trades, base.portfolio_config())
    for frame in (trades, ledger):
        frame.rename(columns={f'v10_b_{name}': f'v10_f_{name}'
                              for name in ('stop_pct', 'target_pct', 'reward_risk')}, inplace=True)
        frame['v10_f_stop_pct'] = frame.native_stop_pct
        frame['v10_f_target_pct'] = frame.native_target_pct
        frame['v10_f_reward_risk'] = frame.native_target_pct / frame.native_stop_pct
        frame['v10_f_confirmation_volume_ratio_minimum'] = VOLUME_RATIO
    summary.update(
        version='V13-v10-F', settings=checked, evidence=checked['evidence'],
        minimum_confirmation_1m_volume_ratio=VOLUME_RATIO, partial_exits=False,
        breakeven_stop=False, square_off='15:15 Asia/Kolkata',
    )
    return trades, ledger, summary


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source-dir', type=Path, default=DEFAULT_SOURCE)
    parser.add_argument('--config-json', type=Path, default=DEFAULT_OUTPUT / 'frozen_config.json')
    parser.add_argument('--output-dir', type=Path, default=DEFAULT_OUTPUT / 'cli_replay')
    args = parser.parse_args()
    settings = json.loads(args.config_json.read_text(encoding='utf-8'))
    dataset = load_source(args.source_dir, settings)
    result = evaluate(dataset, settings)
    r.save(args.output_dir, *result)
    print(json.dumps(r.metric(result[1], dataset['days']), indent=2))


if __name__ == '__main__':
    main()
