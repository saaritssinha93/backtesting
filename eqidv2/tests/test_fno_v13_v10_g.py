"""Causal selection and immutable execution contracts for the replacement G."""
import copy
import json
from dataclasses import asdict, replace

import numpy as np
import pandas as pd
import pytest

import fno_v13_v10_g_backtest as g


def signal(sid=1, *, side='SHORT', slot=930, day='2026-09-01', **changes):
    hour, minute = divmod(slot, 100)
    stamp = pd.Timestamp(f'{day} {hour:02d}:{minute:02d}:00', tz='Asia/Kolkata')
    confirmation = stamp + pd.Timedelta(minutes=1)
    row = dict(
        sid=sid, day=day, hhmm_int=slot, side=side, tradingsymbol=f'STOCK{sid}',
        price_change_pct=-.3 if side == 'SHORT' else .7,
        oi_change_pct=.13, volume_ratio=2., body_ratio=.6, wick_ratio=.2,
        traded_value=1_000_000., v9_1m_volume_ratio=1.2,
        signal_ts=stamp, confirmation_ts=confirmation,
        v9_1m_feature_ts=confirmation,
    )
    row.update(changes)
    return row


def exits():
    return dict(
        version='V13-v10-B', default=dict(stop_pct=.82, target_pct=1.23),
        setups={'0931_SHORT': dict(stop_pct=.6, target_pct=1.8)},
        partial_exits=False, breakeven_stop=False,
    )


@pytest.fixture
def fixed_settings(monkeypatch):
    frozen = {'exit': exits()}
    monkeypatch.setattr(g, 'frozen_f_settings', lambda: copy.deepcopy(frozen))
    return g.config(frozen['exit'], g.SelectionChange())


def selected(rows, change=None, **kwargs):
    frame = rows if isinstance(rows, pd.DataFrame) else pd.DataFrame(rows)
    return g.select_orders(frame, g.v9.V9Config(), change or g.SelectionChange(), **kwargs)


def test_core_survives_better_ranked_new_candidate_without_extra_quota():
    rows = [signal(1), signal(2, oi_change_pct=.10, price_change_pct=-.8)]
    change = g.SelectionChange(oi_multiplier=.75)
    assert selected(rows, change).sid.tolist() == [1]
    assert selected(rows, change, core_first=False).sid.tolist() == [2]
    expanded = selected(rows, replace(change, extra_setup_entries=1)).set_index('sid')
    assert set(expanded.index) == {1, 2}
    assert bool(expanded.loc[1, 'v10_g_f_core'])
    assert not bool(expanded.loc[2, 'v10_g_f_core'])


def test_fills_unused_second_slot_on_an_already_occupied_setup_day():
    rows = [
        signal(1, slot=925, oi_change_pct=.06),
        signal(2, slot=925, price_change_pct=-.17, volume_ratio=5.),
        signal(3, slot=925, price_change_pct=-.16, volume_ratio=3.),
    ]
    result = selected(rows, g.SelectionChange(price_multiplier=.75))
    assert set(result.sid) == {1, 2}
    assert result.max_entries.eq(2).all()
    assert result.set_index('sid').loc[1, 'v10_g_f_core']


def test_expansion_is_limited_to_requested_side():
    rows = [signal(1, price_change_pct=-.17),
            signal(2, side='LONG', price_change_pct=.55)]
    change = g.SelectionChange(price_multiplier=.75, expansion_side='SHORT')
    assert selected(rows, change).sid.tolist() == [1]
    assert selected(rows, replace(change, expansion_side='LONG')).sid.tolist() == [2]
    assert set(selected(rows, replace(change, expansion_side='BOTH')).sid) == {1, 2}


@pytest.mark.parametrize('one_minute', [1.19999, .9, np.nan, np.inf, -np.inf])
def test_confirmation_volume_remains_a_hard_gate(one_minute):
    row = signal(v9_1m_volume_ratio=one_minute, price_change_pct=-.17)
    assert selected([row], g.SelectionChange(price_multiplier=.75, extra_setup_entries=1)).empty


def test_setup_specific_five_minute_volume_gate_is_unchanged():
    rows = [signal(1, slot=925, volume_ratio=1.49999),
            signal(2, slot=925, volume_ratio=1.5)]
    assert selected(rows, g.SelectionChange(price_multiplier=.65, body_reduction=.1)).sid.tolist() == [2]


def test_volume_filter_is_applied_before_core_and_additional_ranking():
    rows = [signal(1, price_change_pct=-.9, v9_1m_volume_ratio=1.199), signal(2)]
    result = selected(rows)
    assert result.sid.tolist() == [2]
    assert result.v10_g_f_core.all()


def test_oi_relaxation_is_relative_to_f_and_respects_frozen_source_floor():
    setups = {(s.signal_end, s.side): s for s in g.v9.v5.profile_setups(
        g.v9.v5.PROFILES['higher_frequency'])}
    change = g.SelectionChange(oi_multiplier=.5)
    core, expanded = g.setup_pair(setups['09:30', 'SHORT'], change)
    assert core.oi_change_pct == pytest.approx(.125)
    assert expanded.oi_change_pct == pytest.approx(.0625)
    core, expanded = g.setup_pair(setups['09:25', 'SHORT'], change)
    assert core.oi_change_pct == expanded.oi_change_pct == .05
    rows = [signal(1, oi_change_pct=.0625),
            signal(2, oi_change_pct=.06249, day='2026-09-02')]
    assert selected(rows, change).sid.tolist() == [1]


@pytest.mark.parametrize('field,value', [
    ('v9_1m_feature_ts', '2026-09-01 09:32:00+05:30'),
    ('v9_1m_feature_ts', None),
    ('v9_1m_feature_ts', 'invalid'),
    ('confirmation_ts', None),
])
def test_invalid_or_future_confirmation_clock_fails_closed(field, value):
    with pytest.raises(ValueError, match='Noncausal|missing'):
        selected([signal(**{field: value})])


def test_signal_id_and_caller_index_cannot_corrupt_setup_quota():
    rows = pd.DataFrame([signal(1), signal(2, oi_change_pct=.1, price_change_pct=-.8)])
    change = g.SelectionChange(oi_multiplier=.75)
    expected = selected(rows, change).sid.tolist()
    rows.index = [0, 0]
    assert selected(rows, change).sid.tolist() == expected == [1]
    rows.loc[:, 'sid'] = 1
    with pytest.raises(ValueError, match='Duplicate signal'):
        selected(rows, change)


def test_outcomes_and_input_order_do_not_change_selections():
    rows = pd.DataFrame([signal(1), signal(2, oi_change_pct=.1, price_change_pct=-.8)])
    change = g.SelectionChange(oi_multiplier=.75, extra_setup_entries=1)
    expected = selected(rows, change)[['sid', 'setup_id']]
    altered = rows.assign(net_profit_rupees=[-1e12, 1e12], mfe_pct=[0, 1000],
                          future_stop_hit=[True, False]).iloc[::-1]
    pd.testing.assert_frame_equal(selected(altered, change)[['sid', 'setup_id']], expected)


def test_no_new_entry_windows_are_created():
    # 10:05 was part of the rejected expansion, not an existing F setup.
    assert selected([signal(slot=1005)], g.SelectionChange(price_multiplier=.65,
        body_reduction=.1, extra_setup_entries=1)).empty


def test_config_copies_exits_and_retains_corrected_capital(fixed_settings):
    settings = g.checked_settings(fixed_settings)
    assert settings['portfolio_capital_rupees'] == 1_000_000
    assert settings['capital_per_entry_rupees'] == 100_000
    assert settings['leverage_factor'] == 5
    assert settings['max_positions'] is None
    assert settings['entry_expiry_minutes'] == 10
    assert not settings['partial_exits'] and not settings['breakeven_stop']
    original = exits()
    result = g.config(original, g.SelectionChange())
    result['exit']['default']['target_pct'] = 2.
    assert original == exits()


@pytest.mark.parametrize('field,value', [
    ('portfolio_capital_rupees', 300_000), ('capital_per_entry_rupees', 75_000),
    ('leverage_factor', 1), ('max_positions', 3), ('entry_expiry_minutes', 15),
    ('minimum_confirmation_1m_volume_ratio', .9), ('partial_exits', True),
    ('breakeven_stop', True), ('cost_bps', 0),
])
def test_execution_settings_cannot_be_silently_changed(fixed_settings, field, value):
    fixed_settings[field] = value
    with pytest.raises(ValueError, match='Conflicting fixed G'):
        g.checked_settings(fixed_settings)


def test_valid_but_different_exits_are_rejected(fixed_settings):
    fixed_settings['exit']['default']['target_pct'] = 1.64
    with pytest.raises(ValueError, match='frozen F exits'):
        g.checked_settings(fixed_settings)


@pytest.mark.parametrize('flag', ['false', 'true', 1, None])
def test_core_preservation_flag_requires_an_explicit_boolean(flag):
    with pytest.raises(ValueError, match='boolean'):
        g.config(exits(), g.SelectionChange(), core_first=flag)


@pytest.mark.parametrize('change', [
    {'price_multiplier': .64}, {'oi_multiplier': .49}, {'body_reduction': .11},
    {'wick_increase': np.nan}, {'extra_setup_entries': 2}, {'expansion_side': 'ANY'},
])
def test_invalid_selection_parameters_are_rejected(change):
    with pytest.raises(ValueError):
        g.SelectionChange(**change).validate()


def test_evaluation_rejects_injected_orders_and_uses_reconstructed_inputs(fixed_settings, monkeypatch):
    signals = pd.DataFrame([signal(1), signal(2, oi_change_pct=.1)])
    base = replace(g.v9.V9Config(), portfolio_capital_rupees=1_000_000., max_positions=None)
    orders = g.select_orders(signals, base)
    dataset = {'signals': signals, 'v9_config': base, 'orders': orders.copy()}
    dataset['orders'].loc[:, 'sid'] = 2
    with pytest.raises(ValueError, match='causal G selection'):
        g.evaluate(dataset, fixed_settings)
    dataset['orders'] = orders.copy()
    dataset['orders'].loc[:, 'price_change_pct'] = -9999
    observed = {}
    def fake_evaluate(rebuilt, exit_config):
        observed['price'] = rebuilt['orders'].price_change_pct.tolist()
        observed['exit'] = exit_config
        return pd.DataFrame(), pd.DataFrame(), {}
    monkeypatch.setattr(g.f.b, 'evaluate', fake_evaluate)
    g.evaluate(dataset, fixed_settings)
    assert observed['price'] == [-.3]
    assert observed['exit'] == exits()


@pytest.fixture
def frozen_source(tmp_path, monkeypatch):
    source = tmp_path / 'source'
    folder = source / 'dataset'
    folder.mkdir(parents=True)
    pd.DataFrame([signal()]).to_parquet(folder / 'signals.parquet', index=False)
    config_path = source / 'frozen_config.json'
    config_path.write_text(json.dumps(asdict(g.v9.V9Config())), encoding='utf-8')
    (source / 'research_manifest.json').write_text(json.dumps({
        'artifacts': {'frozen_config.json': g.f.v10.sha(config_path)}
    }), encoding='utf-8')
    upstream = tmp_path / 'upstream.csv'
    upstream.write_text('field\noriginal\n', encoding='utf-8')
    manifest = dict(
        sources=[dict(path=str(upstream), exists=True, sha256=g.f.v10.sha(upstream))],
        output_sha256={'signals.parquet': g.f.v10.sha(folder / 'signals.parquet')},
        days=['2026-09-01'],
    )
    (folder / 'dataset_manifest.json').write_text(json.dumps(manifest), encoding='utf-8')
    # Source-code attestation is a separate integration concern; these tests
    # isolate the exact artifact and metadata checks performed by load_source.
    monkeypatch.setattr(g.v9, 'validate_configuration', lambda: {})
    return source, upstream


def test_loader_keeps_frozen_selection_settings_and_corrects_only_capacity(frozen_source):
    source, _ = frozen_source
    result = g.load_source(source)
    expected = replace(g.v9.V9Config(), portfolio_capital_rupees=1_000_000., max_positions=None)
    assert result['v9_config'] == expected
    assert result['source_verification']['all_frozen_artifacts_verified']
    assert result['source_verification']['known_metadata_drift'] == []


def test_loader_refuses_modified_frozen_artifacts(frozen_source):
    source, _ = frozen_source
    (source / 'dataset' / 'signals.parquet').write_bytes(b'changed artifact')
    with pytest.raises(RuntimeError, match='artifact drift'):
        g.load_source(source)


def test_loader_refuses_unexplained_or_further_source_drift(frozen_source, monkeypatch):
    source, upstream = frozen_source
    upstream.write_text('field\nreviewed metadata refresh\n', encoding='utf-8')
    with pytest.raises(RuntimeError, match='Unexplained frozen source drift'):
        g.load_source(source)
    monkeypatch.setattr(g, 'KNOWN_METADATA_DRIFT', {
        str(upstream.resolve()).lower(): g.f.v10.sha(upstream)
    })
    result = g.load_source(source)
    assert len(result['source_verification']['known_metadata_drift']) == 1
    upstream.write_text('field\nunreviewed additional change\n', encoding='utf-8')
    with pytest.raises(RuntimeError, match='Unexplained frozen source drift'):
        g.load_source(source)


def test_loader_refuses_changed_frozen_v9_filter_or_cost_config(frozen_source):
    source, _ = frozen_source
    path = source / 'frozen_config.json'
    changed = json.loads(path.read_text(encoding='utf-8'))
    changed['cost_bps'] = 0
    path.write_text(json.dumps(changed), encoding='utf-8')
    with pytest.raises((ValueError, RuntimeError), match='[Cc]onfig.*drift|[Dd]rift.*config'):
        g.load_source(source)


def test_research_choice_respects_quality_and_simplicity_before_highest_pf():
    import fno_v13_v10_g_research as research
    rows = [
        dict(candidate='F_CONTROL', trades=61, trades_per_day=61/31, quality_pass=True,
             objective_pass=False, changed_families=0, distance_from_three=1., profit_factor=3.5),
        dict(candidate='SIMPLE', trades=90, trades_per_day=90/31, quality_pass=True,
             objective_pass=True, changed_families=2, distance_from_three=3/31, profit_factor=3.4),
        dict(candidate='COMPLEX_HIGH_PF', trades=93, trades_per_day=3., quality_pass=True,
             objective_pass=True, changed_families=3, distance_from_three=0., profit_factor=8.),
        dict(candidate='WEAK_QUALITY', trades=93, trades_per_day=3., quality_pass=False,
             objective_pass=False, changed_families=1, distance_from_three=0., profit_factor=3.2),
    ]
    chosen, status = research.choose(pd.DataFrame(rows))
    assert chosen == 'SIMPLE'
    assert status == 'HISTORICAL_OBJECTIVE_MET_EXPLORATORY'
    # Failure to hit frequency must remain explicit when choosing a smaller gain.
    rows[1].update(trades=80, trades_per_day=80/31, objective_pass=False)
    rows[2].update(quality_pass=False, objective_pass=False)
    chosen, status = research.choose(pd.DataFrame(rows))
    assert chosen == 'SIMPLE'
    assert status == 'QUALITY_PRESERVED_FREQUENCY_OBJECTIVE_NOT_MET'
    rows[1]['quality_pass'] = False
    assert research.choose(pd.DataFrame(rows)) == ('F_CONTROL', 'NO_QUALIFYING_IMPROVEMENT_RETAIN_F')
