from datetime import date
import copy
import json
import sys

import pandas as pd
import pytest

from research import g4_g3_mirror_backtest as mirror


def candidates():
    stamp = pd.Timestamp('2026-10-14 12:35', tz='Asia/Kolkata')
    rows=[]
    for i,symbol in enumerate(['A','B']):
        rows.append(dict(sid=i,tradingsymbol=symbol,side='SHORT',signal_ts=stamp,
            confirmation_ts=stamp+pd.Timedelta(minutes=1),v9_1m_feature_ts=stamp+pd.Timedelta(minutes=1),
            signal_close=100.,confirmation_open=100.,confirmation_high=100.1,
            confirmation_low=99.7,confirmation_close=99.8,price_change_pct=-2.,
            oi_change_pct=.5,oi=100500.,prev_oi=100000.,volume_ratio=10.,
            ema9=99.,ema20=100.,ema50=101.,body_ratio=.9,wick_ratio=.01,
            traded_value=2e6-i*1e6,v9_1m_volume_ratio=10.,
            research_extension_atr=3.01 if i==0 else 3.0))
    return mirror.donor.selection_audit(pd.DataFrame(rows))


def test_extra_guard_applies_before_ranking_and_allows_replacement():
    pool=candidates()
    assert mirror.reselect(pool,[]).tradingsymbol.tolist()==['A']
    assert mirror.reselect(pool,['1236_SHORT']).tradingsymbol.tolist()==['B']
    pool['research_extension_atr']=float('nan')
    assert mirror.reselect(pool,['1236_SHORT']).empty


def test_copy_keeps_all_rules_and_disabled_inventory_without_mutating_donor():
    original=mirror.frozen_donor.strategy_payload()
    config=mirror.configuration()
    identity=mirror.Identity(config,'guard_1235_short')
    assert len(identity.ACTIVE_SETUPS)==49
    assert len(identity.payload['disabled_setups'])==10
    assert identity.payload['exit']==original['exit']
    assert identity.payload['maximum_confirmation_extension_atr_by_setup']=={
        '1036_LONG':3.,'1036_SHORT':3.,'1236_SHORT':3.}
    assert mirror.frozen_donor.strategy_payload()==original
    assert identity.strategy_fingerprint()!=mirror.frozen_donor.strategy_fingerprint()
    assert identity.payload['source_strategy_version'] == mirror.frozen_donor.STRATEGY_VERSION
    assert identity.payload['source_strategy_fingerprint'] == mirror.DONOR_FINGERPRINT


def test_configuration_rejects_changed_stop_or_target(tmp_path):
    config=copy.deepcopy(mirror.configuration_payload())
    config['g3_contract']['active_setups'][0]['target_pct']=99
    path=tmp_path/'changed.json';path.write_text(json.dumps(config))
    with pytest.raises(ValueError,match='changed'):
        mirror.configuration(path)


def test_enrichment_ignores_future_and_rejects_changed_confirmation():
    stamp=pd.Timestamp('2026-10-14 12:36',tz='Asia/Kolkata')
    frame=pd.DataFrame(dict(ts=pd.date_range(stamp-pd.Timedelta(minutes=30),periods=32,freq='min'),
        open=100.,high=100.1,low=99.7,close=99.8))
    prices=object.__new__(mirror.Prices)
    prices.frame=lambda day,symbol:(frame,'test')
    pool=candidates()
    before=prices.enrich(pool)
    frame.loc[frame.ts.gt(stamp),['open','high','low','close']]=1e9
    after=prices.enrich(pool)
    assert before.research_extension_atr.equals(after.research_extension_atr)
    frame.loc[frame.ts.eq(stamp),'close']=1
    with pytest.raises(ValueError,match='OHLC changed'):
        prices.enrich(pool)


def test_normal_g4_cli_routes_to_mirror_through_requested_day(tmp_path,monkeypatch):
    import fno_v13_v10_g_4_backtest as cli
    calls=[]
    monkeypatch.setattr(cli,'BACKTEST_ROOT',tmp_path)
    def fake(config,output,through,candidate_pool_run=None):
        calls.append(through)
        return dict(sessions=51,last_date='2026-10-14',scenarios=[])
    monkeypatch.setattr(cli,'run_mirror',fake)
    monkeypatch.setattr(sys,'argv',['g4','--config',str(mirror.DEFAULT_CONFIG),
         '--through','2026-10-14','--output-dir',str(tmp_path/'result')])
    cli.main()
    assert calls==[date(2026,10,14)]


def test_normal_g4_cli_retains_explicit_49_setup_bounded_profile(tmp_path,monkeypatch):
    import fno_v13_v10_g_4_backtest as cli
    from research import g4_bounded_exit_backtest as bounded
    calls=[]
    monkeypatch.setattr(cli,'BACKTEST_ROOT',tmp_path)
    def fake(config,output,through):
        calls.append((config,through))
        return dict(sessions=52,first_date='2026-07-29',last_date='2026-10-15',
                    active_setups=49,comparison=[])
    monkeypatch.setattr(cli,'run_bounded',fake)
    monkeypatch.setattr(sys,'argv',['g4','--config',str(bounded.DEFAULT_CONFIG),'--through','2026-10-15',
         '--output-dir',str(tmp_path/'result')])
    cli.main()
    assert calls==[(bounded.DEFAULT_CONFIG,date(2026,10,15))]


def test_execution_path_accepts_nullable_boolean_and_rejects_flagged_bar():
    stamp=pd.Timestamp('2026-10-14 12:36',tz=mirror.production.IST)
    frame=pd.DataFrame(dict(ts=pd.date_range(stamp+pd.Timedelta(minutes=1),
        pd.Timestamp('2026-10-14 15:15',tz=mirror.production.IST),freq='min'),
        open=100.,high=101.,low=99.,close=100.))
    frame['gap_filled']=pd.Series(False,index=frame.index,dtype='boolean')
    frame.loc[0,'gap_filled']=pd.NA
    prices=object.__new__(mirror.Prices)
    prices.frame=lambda day,symbol:(frame,'test')
    orders=candidates().iloc[:1]
    assert len(prices.paths(orders)[0]['open'])==len(frame)
    frame.loc[1,'gap_filled']=True
    with pytest.raises(ValueError,match='Flagged execution minute'):
        prices.paths(orders)


@pytest.mark.parametrize('limit', [3.5, 4.0])
def test_per_setup_limit_accepts_boundary_and_reselects_below_it(limit):
    pool=candidates()
    pool['research_extension_atr']=[limit+.001,limit]
    assert mirror.reselect(pool,{'1236_SHORT':limit}).tradingsymbol.tolist()==['B']
    assert mirror.reselect(pool,{'0946_LONG':3.0}).tradingsymbol.tolist()==['A']
    pool['research_extension_atr']=float('nan')
    assert mirror.reselect(pool,{'1236_SHORT':limit}).empty


def test_v8_adds_exact_six_with_combined_limits_and_preserves_v7():
    cfg=mirror.configuration(mirror.ROOT/'configs/strategies/v13_v10_g_4_g3_mirror_guard_v8.json')
    expected={'0926_SHORT':4.,'0936_LONG':3.5,'1001_LONG':4.,
              '1136_LONG':3.5,'1146_SHORT':4.,'1301_SHORT':3.5}
    assert cfg['scenarios']['guard_new_six']==expected
    assert len(cfg['scenarios'])==14
    assert cfg['primary_scenario']=='guard_all_ten'
    identity=mirror.Identity(cfg,'guard_all_ten')
    assert identity.payload['maximum_confirmation_extension_atr_by_setup']=={
        '1036_LONG':3.,'1036_SHORT':3.,**mirror.SCENARIOS['guard_all_four'],**expected}
    old=mirror.configuration(mirror.ROOT/'configs/strategies/v13_v10_g_4_g3_mirror_guard_v7.json')
    assert len(old['scenarios'])==6
    assert mirror.Identity(old,'guard_all_four').STRATEGY_VERSION.startswith(mirror.LEGACY_VERSION)


def test_v9_relaxes_only_g4_0926_long_five_minute_volume_before_ranking():
    cfg = mirror.configuration(mirror.ROOT/'configs/strategies/v13_v10_g_4_g3_mirror_guard_v9.json')
    scenario = cfg['primary_scenario']
    assert cfg['five_minute_volume_ratio_overrides'][scenario] == {'0926_LONG': 1.2}
    identity = mirror.Identity(cfg, scenario)
    assert identity._ACTIVE_BY_ID['0926_LONG'].volume_ratio == 1.2
    assert identity.payload['core_setups'][1]['volume_ratio'] == 1.2
    assert mirror.frozen_donor._ACTIVE_BY_ID['0926_LONG'].volume_ratio == 3.0
    assert identity._ACTIVE_BY_ID['0926_SHORT'].volume_ratio == mirror.frozen_donor._ACTIVE_BY_ID['0926_SHORT'].volume_ratio

    stamp = pd.Timestamp('2026-10-14 09:25', tz='Asia/Kolkata')
    rows = []
    for sid, symbol, volume, liquidity in [(1, 'LOWER_VOLUME', 1.2, 2e6), (2, 'OLD_PASS', 3.0, 1e6)]:
        rows.append(dict(sid=sid, tradingsymbol=symbol, side='LONG', signal_ts=stamp,
            confirmation_ts=stamp+pd.Timedelta(minutes=1),
            v9_1m_feature_ts=stamp+pd.Timedelta(minutes=1), signal_close=100.,
            confirmation_open=100., confirmation_high=101.2, confirmation_low=99.9,
            confirmation_close=101., price_change_pct=.5, oi_change_pct=.5,
            oi=100500., prev_oi=100000., volume_ratio=volume,
            ema9=101., ema20=100., ema50=99., body_ratio=.8, wick_ratio=.01,
            traded_value=liquidity, v9_1m_volume_ratio=2.))
    # The current G3 donor now also uses 1.2. Compare against the frozen G4
    # source rule explicitly, then verify the G4 override's reselection.
    old_setup = mirror.frozen_donor._ACTIVE_BY_ID['0926_LONG']
    assert not mirror.production.passes_selected_filters(rows[0], old_setup,
                                                          session_date=stamp.date())
    assert mirror.production.passes_selected_filters(rows[1], old_setup,
                                                     session_date=stamp.date())
    pool = mirror.donor.selection_audit(pd.DataFrame(rows))
    pool['research_extension_atr'] = 1.0
    selected = mirror.reselect(pool, {}, {'0926_LONG': 1.2})
    assert selected.tradingsymbol.tolist() == ['LOWER_VOLUME']


def test_v10_adds_three_atr_guard_to_0926_long_on_top_of_v9_volume_rule():
    cfg = mirror.configuration(mirror.ROOT/'configs/strategies/v13_v10_g_4_g3_mirror_guard_v10.json')
    scenario = cfg['primary_scenario']
    assert cfg['five_minute_volume_ratio_overrides'][scenario] == {'0926_LONG': 1.2}
    assert cfg['scenarios'][scenario]['0926_LONG'] == 3.0
    assert mirror.Identity(cfg, scenario).payload['maximum_confirmation_extension_atr_by_setup']['0926_LONG'] == 3.0


def test_default_g4_choice_is_v9_volume_120_without_0926_atr_guard():
    cfg = mirror.configuration()
    assert cfg['variant_id'] == mirror.PREVIOUS_VERSION
    scenario = cfg['primary_scenario']
    assert cfg['five_minute_volume_ratio_overrides'][scenario] == {'0926_LONG': 1.2}
    assert '0926_LONG' not in cfg['scenarios'][scenario]


def test_new_g3_slots_cannot_enter_existing_g4_selection():
    pool = candidates()
    extra = pool.iloc[[0]].copy()
    stamp = pd.Timestamp('2026-10-15 10:10', tz='Asia/Kolkata')
    extra['side'] = 'LONG'
    extra['signal_ts'] = extra['signal_timestamp'] = stamp
    extra['confirmation_ts'] = extra['confirmation_timestamp'] = extra['v9_1m_feature_ts'] = stamp + pd.Timedelta(minutes=1)
    extra['setup_id'] = '1011_LONG'
    extra['tradingsymbol'] = 'NEW_G3_ONLY'
    extra['price_change_pct'] = .5
    extra['ema9'], extra['ema20'], extra['ema50'] = 103., 102., 101.
    pool = pd.concat([pool, extra], ignore_index=True)
    selected = mirror.reselect(pool, {})
    assert selected.tradingsymbol.tolist() == ['A']
    assert set(selected.setup_id).issubset(mirror.frozen_donor._ACTIVE_BY_ID)
    with pytest.raises(ValueError, match='frozen 49-setup'):
        mirror.reselect(pool, {'1011_LONG': 3.0})


@pytest.fixture
def frozen_baseline_refs(tmp_path, monkeypatch):
    g3, g4 = tmp_path/'g3', tmp_path/'g4'
    monkeypatch.setattr(mirror, 'G3_ROOT', g3)
    monkeypatch.setattr(mirror, 'G4_ROOT', g4)
    def package(directory, payloads):
        directory.mkdir(parents=True)
        for name, payload in payloads.items():
            (directory/name).write_text(json.dumps(payload) if isinstance(payload, dict) else payload)
        manifest = {'files': {name: mirror.sha256(directory/name) for name in payloads}}
        (directory/'manifest.json').write_text(json.dumps(manifest))
        return mirror.sha256(directory/'manifest.json')
    frozen = g3/'backtests/frozen_v11'
    summary = dict(strategy_version=mirror.frozen_donor.STRATEGY_VERSION,
        strategy_fingerprint=mirror.DONOR_FINGERPRINT, active_setup_count=49, last_date='2026-10-14')
    frozen_hash = package(frozen, {'config.json': mirror.frozen_donor.strategy_payload(),
        'summary.json': summary, 'selected_orders.csv': 'sid\n', 'portfolio_trades.csv': 'sid\n'})
    current = g3/'backtests/current_v12'
    current_hash = package(current, {'config.json': mirror.donor.strategy_payload(),
        'summary.json': dict(strategy_version=mirror.donor.STRATEGY_VERSION,
            strategy_fingerprint=mirror.donor.strategy_fingerprint(), active_setup_count=51,
            last_date='2026-10-15'), 'selected_orders.csv': 'sid\n', 'portfolio_trades.csv': 'sid\n'})
    saved = g4/'runs/saved'
    saved_hash = package(saved, {'config.json': mirror.configuration_payload(),
        'summary.json': dict(execution_authority=False, mirror_parity_verified=True,
            baseline_g3_run=str(frozen), baseline_g3_manifest_sha256=frozen_hash)})
    pointer = dict(run_dir=current.relative_to(g3).as_posix(), manifest_sha256=current_hash)
    (g3/'latest').mkdir()
    (g3/'latest/current_backtest.json').write_text(json.dumps(pointer))
    (g4/'latest').mkdir()
    (g4/'latest/g3_mirror.json').write_text(json.dumps(dict(execution_authority=False,
        run_dir=saved.relative_to(g4).as_posix(), manifest_sha256=saved_hash)))
    return tmp_path, pointer, frozen, frozen_hash


def test_new_g3_pointer_uses_verified_prior_g4_donor_without_mutation(frozen_baseline_refs):
    root, pointer, frozen, frozen_hash = frozen_baseline_refs
    before = {path.relative_to(root): path.read_bytes() for path in root.rglob('*') if path.is_file()}
    directory, summary, reference = mirror._resolve_frozen_baseline(pointer)
    assert directory == frozen
    assert reference['manifest_sha256'] == frozen_hash
    assert summary['strategy_version'] == mirror.frozen_donor.STRATEGY_VERSION
    assert summary['active_setup_count'] == 49
    assert {path.relative_to(root): path.read_bytes() for path in root.rglob('*') if path.is_file()} == before


def test_g4_cannot_silently_extend_frozen_donor_to_current_g3_date(frozen_baseline_refs):
    _, _, _, _ = frozen_baseline_refs
    output = mirror.G4_ROOT/'runs/unwritten'
    with pytest.raises(ValueError, match='frozen G4 donor historical cutoff'):
        mirror.run(mirror.DEFAULT_CONFIG, output, through=date(2026, 10, 15))
    assert not output.exists()


def test_tampered_frozen_baseline_is_rejected_before_output_creation(frozen_baseline_refs):
    _, pointer, frozen, _ = frozen_baseline_refs
    (frozen/'selected_orders.csv').write_text('sid\n999\n')
    with pytest.raises(ValueError, match='artifact changed'):
        mirror._resolve_frozen_baseline(pointer)
