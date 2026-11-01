"""Whole-slot selection must rerun capacity and realized-loss admission."""
import pandas as pd
import pytest

from research import slot_win70_backtest as replay


CONFIG = dict(capital_per_entry_rs=100_000., leverage=5., portfolio_capital_rs=1_000_000.,
              max_positions_per_symbol=1, model_cost_bps_round_trip=5.)


def order(sid, setup, entry, exit_, gross, *, symbol=None):
    stamp = lambda clock: pd.Timestamp('2026-10-01 ' + clock, tz='Asia/Kolkata')
    return dict(sid=sid, signal_id='synthetic-' + str(sid), day='2026-10-01', setup_id=setup,
        tradingsymbol=symbol or 'STOCK' + str(sid), side='LONG', filled=True, g8_baseline_filled=True,
        base_entry_allowed=True, final_entry_allowed=False, final_permission=False,
        entry_price=100., entry_path_index=0, entry_ts=stamp(entry), confirmation_ts=stamp(entry)-pd.Timedelta(minutes=1),
        exit_ts=stamp(exit_), exit_event='INTRABAR', exit_price=100.+gross,
        exit_reason='TARGET' if gross > 0 else 'STOP', gross_return_pct=gross, net_return_pct=gross-.05,
        cost_pct=.05, initial_stop_pct=1.2, picker='max_liquidity', traded_value=1_000_000.)


def test_disabled_loss_releases_later_g7_trade_and_retains_source_inventory():
    source = pd.DataFrame([order(1,'DISABLED','10:01','10:03',-1.2),
                           order(2,'KEEP','10:02','10:04',-1.2),
                           order(3,'KEEP','10:06','10:10',1.)])
    before = source.copy(deep=True)
    baseline = replay.apply_slot_policy(source,'G7_V4_LOCK2',{'KEEP','DISABLED'},CONFIG)
    selected = replay.apply_slot_policy(source,'G7_V4_LOCK2',{'KEEP'},CONFIG)
    assert baseline.portfolio_executed.tolist() == [True,True,False]
    assert selected.portfolio_executed.tolist() == [False,True,True]
    assert selected.loss_lock_ts.isna().all()
    assert selected.sid.tolist() == source.sid.tolist()
    assert selected.loc[0,'portfolio_reject_reason'] == 'FROZEN_SLOT_ALLOWLIST_REJECTED'
    pd.testing.assert_frame_equal(source,before)


def test_g7_selected_slot_filter_releases_capital_for_later_opportunity():
    source = pd.DataFrame([order(1,'DISABLED','10:01','10:20',1.),
                           order(2,'KEEP','10:02','10:10',1.)])
    config = {**CONFIG,'portfolio_capital_rs':100_000.}
    baseline = replay.apply_slot_policy(source,'G7_V4_LOCK2',{'KEEP','DISABLED'},config)
    selected = replay.apply_slot_policy(source,'G7_V4_LOCK2',{'KEEP'},config)
    assert baseline.portfolio_executed.tolist() == [True,False]
    assert selected.portfolio_executed.tolist() == [False,True]


def test_g7_retains_original_base_permissions_and_recalculates_cost_losses():
    source = pd.DataFrame([order(1,'KEEP','10:01','10:03',.1),
                           order(2,'KEEP','10:02','10:04',.1),
                           order(3,'KEEP','10:06','10:10',1.),
                           order(4,'KEEP','10:07','10:11',1.)])
    source.loc[3,'base_entry_allowed'] = False
    low = replay.apply_slot_policy(source,'G7_V4_LOCK2',{'KEEP'},CONFIG,5.)
    high = replay.apply_slot_policy(source,'G7_V4_LOCK2',{'KEEP'},CONFIG,15.)
    assert low.portfolio_executed.tolist() == [True,True,True,False]
    assert high.portfolio_executed.tolist() == [True,True,False,False]
    assert high.daily_loss_lock_blocked.iloc[2]
    assert not high.base_entry_allowed.iloc[3]


def test_g3_disabled_slot_cannot_execute_and_can_release_symbol_capacity():
    source = pd.DataFrame([order(1,'DISABLED','10:01','10:20',1.,symbol='SAME'),
                           order(2,'KEEP','10:02','10:10',1.,symbol='SAME')])
    selected = replay.apply_slot_policy(source,'G3',{'KEEP'},CONFIG)
    assert selected.portfolio_executed.tolist() == [False,True]
    assert selected.slot_policy_source_filled.tolist() == [True,True]
    assert selected.sid.tolist() == source.sid.tolist()
    assert selected.portfolio_net_profit_rupees.tolist() == pytest.approx([0.,4750.])


def test_current_g7_cli_dispatches_to_shared_slot_replay(monkeypatch,tmp_path):
    from research import g7_v4_lock_backtest as entrypoint
    called = {}
    def fake_run(out, **kwargs):
        called.update(out=out, **kwargs)
        return {'output_dir':str(out)}
    monkeypatch.setattr(replay,'run',fake_run)
    entrypoint.main(['--output-dir',str(tmp_path/'new')])
    assert called['out'] == tmp_path/'new'
    assert called['config_path'] == entrypoint.DEFAULT_CONFIG
    assert called['publish_dashboard'] is False


def test_all_slot_scope_excludes_every_below_threshold_and_zero_sample_slot():
    import strategy_slot_selection as policy
    for strategy in policy.STRATEGIES:
        rows = policy.policy_payload(strategy)['slot_statistics']
        assert len(rows) == 51
        assert policy.enabled_setup_ids(strategy) == {r['setup_id'] for r in rows
            if r['trades'] and 100*r['wins'] >= 70*r['trades']}
