"""The two requested slots share rules across saved specs and execution consumers."""
from dataclasses import asdict
from datetime import date, datetime
import hashlib
import json
from pathlib import Path

import pandas as pd

import dashboard_flow as flow
import fno_v13_v10_g_3_contract as production
import fno_v13_v10_g_3_daily_replay as replay
import fno_v13_v10_g_3_live_config as runtime
from fno_v13_v10_g_3_preflight import inspect_deployment
from research import g3_backtest_profile as profile
from research import g3_backtest_report as report
from research.archived_g3_v11 import contract as archived_v11
from research.g3_0920_backtest import _stats
from tools import g3_live_operator as operator


ROOT = Path(__file__).resolve().parents[1]
ADDED = {'0921_SHORT': ('09:20', 'SHORT', 1.2), '1011_LONG': ('10:10', 'LONG', .6)}


def test_live_paper_daily_historical_specs_share_both_requested_targets():
    runtime.validate_strategy()
    saved_production = json.loads(runtime.CONFIG_PATH.read_text())
    saved_backtest = profile.configuration()
    assert runtime.contract is production is profile.production
    assert replay.contract is profile.contract
    assert len(production.ACTIVE_SETUPS) == 61
    assert len(profile.contract.ACTIVE_SETUPS) == 51
    for setup_id, (signal, side, volume) in ADDED.items():
        live = runtime.setup_for(signal, side)
        daily = replay.contract.setup_for(signal, side)
        assert live.setup_id == daily.setup_id == setup_id
        assert live.target_pct == daily.target_pct == 1.5
        assert runtime.confirmation_volume_minimum(side, signal_end=signal) == volume
        assert replay.contract.confirmation_volume_minimum(side, signal_end=signal) == volume
        live_rules = asdict(live)
        replay_rules = asdict(daily)
        live_rules.pop('source_version')
        replay_rules.pop('source_version')
        assert live_rules == replay_rules
        for payload in (saved_production, saved_backtest):
            row = next(row for row in payload['active_setups']
                       if row['signal_end'] == signal and row['side'] == side)
            assert row['target_pct'] == 1.5
            assert payload['exit']['setups'][setup_id]['target_pct'] == 1.5


def test_preflight_registry_and_operator_plan_include_two_slots_without_mutation(tmp_path):
    deployment = ROOT / 'configs/strategies/active_profile.json'
    before = deployment.read_bytes()
    result = inspect_deployment(verify_frozen=False)
    assert result['ready'] and result['api_registry_matches']
    assert result['promoted_two_slot_rules_match']
    slots = {row['setup_id']: row for row in result['promoted_two_slots']}
    assert set(slots) == set(ADDED)
    assert all(row['target_pct'] == 1.5 and row['historical_daily_paper_live_rules_match']
               for row in slots.values())
    assert result['workers_started'] is result['broker_requests'] is result['arm_files_written'] is False
    state = json.loads(before)
    now = datetime(2026, 10, 16, 8, 30, tzinfo=runtime.IST)
    plan = operator.plan(now.date(), state, now=now, runtime_root=tmp_path)
    assert len(plan['setup_ids']) == 61
    assert {key: plan['targets_pct'][key] for key in ADDED} == dict.fromkeys(ADDED, 1.5)
    assert not plan['mutates_controls'] and not plan['starts_workers'] and not plan['submits_orders']
    assert deployment.read_bytes() == before


def test_previous_backtest_scope_keeps_its_49_setups_and_ten_disabled():
    scope = flow._g3_backtest_scope(archived_v11.STRATEGY_VERSION, archived_v11.strategy_fingerprint())
    assert scope['active_setup_count'] == 49
    assert scope['disabled_setup_count'] == 10
    assert '09:20 SHORT and 10:10 LONG' not in scope['backtest_scope_text']
    assert not profile.contract.supported_identity(archived_v11.STRATEGY_VERSION, archived_v11.strategy_fingerprint())
    assert profile.contract.supported_identity(archived_v11.STRATEGY_VERSION,
                                               archived_v11.strategy_fingerprint(), allow_legacy=True)


def test_saved_previous_production_report_preserves_59_setup_inventory(tmp_path):
    previous = json.loads((ROOT / 'configs/strategies/v13_v10_g_3_production_v9.json').read_text())
    ledger = pd.DataFrame([dict(day='2026-10-15', setup_id='0931_LONG', filled=True,
        portfolio_executed=True, portfolio_net_profit_rupees=100.,
        portfolio_gross_profit_rupees=105., portfolio_cost_rupees=5.)])
    summary = dict(days=['2026-10-15'], first_date='2026-10-15', last_date='2026-10-15', sessions=1,
        active_setup_count=59, strategy_version=previous['strategy_version'],
        strategy_fingerprint=hashlib.sha256(json.dumps(previous, sort_keys=True,
            separators=(',', ':'), allow_nan=False).encode()).hexdigest(),
        current_g3=_stats(ledger, [date(2026, 10, 15)]))
    daily = pd.DataFrame([dict(day='2026-10-15', trades=1, wins=1, losses=0, net_pnl=100.)])
    report.write(tmp_path, summary, ledger, daily)
    setups = pd.read_csv(tmp_path / 'setup_results.csv')
    assert len(setups) == 59
    assert not set(ADDED) & set(setups.setup_id)
    page = (tmp_path / 'report.html').read_text()
    assert 'G3 historical backtest: 59 setups' in page
    assert 'This saved V13-V10-G-3-PRODUCTION-V9 report' in page
    assert 'disabled from backtesting' not in page.lower()
