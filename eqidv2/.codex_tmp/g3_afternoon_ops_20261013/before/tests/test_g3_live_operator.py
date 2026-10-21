"""Operator handoff tests: disposable state only; no broker/task mutations."""
from datetime import date, datetime, timedelta
import copy
import base64
import json
import os
from pathlib import Path
import shutil
import subprocess

import pytest
from tools import g3_live_operator as op

NOW = datetime(2026, 10, 13, 8, 30, tzinfo=op.config.IST)
DAY = NOW.date()


@pytest.fixture
def state():
    value = op.read(op.PROFILE)
    value.update(deployment_stage='PAPER_VALIDATION', live_activation_authorized=False)
    value.pop('live_approval', None)
    return value


@pytest.fixture
def report():
    return dict(mode='VALIDATE_ONLY', orders_submitted=0, order_mutations_available=False,
        completed_at_ist=NOW.isoformat(), apps=[dict(app='app1', credential_files_present=True,
            state='READ_ACCESS_CONFIRMED', open_net_positions=0, nonterminal_orders=0,
            funds_positive=True, equity_segment_enabled=True,
            capabilities=dict(nse=True, mis=True, limit=True, market=True, sl=True))])


def test_preview_is_read_only_and_covers_every_setup(state, tmp_path):
    before = copy.deepcopy(state)
    root = tmp_path / 'absent'
    plan = op.plan(DAY, state, now=NOW, runtime_root=root)
    assert state == before and not root.exists()
    assert plan['active_setup_count'] == 44
    assert len(set(plan['setup_ids'])) == 44
    assert plan['targets_pct']['1006_LONG'] == .56
    assert plan['targets_pct']['1031_SHORT'] == 1.66
    assert plan['targets_pct']['1136_LONG'] == 7.15
    assert plan['targets_pct']['1231_SHORT'] == 1.85
    assert plan['last_confirmation'] == '12:31'
    assert plan['pipeline_deadline'] == '12:33'
    assert not plan['mutates_controls'] and not plan['starts_workers']


def test_tomorrow_plan_keeps_approved_midday_filters_and_all_previous_setups(state, tmp_path):
    plan = op.plan(date(2026, 10, 14), state, now=NOW, runtime_root=tmp_path)
    selected = json.loads((op.ROOT / 'configs/strategies/v13_v10_g_4_backtest_selected_midday_v5.json').read_text())
    previous = json.loads((op.ROOT / 'configs/strategies/v13_v10_g_3_production_v5.json').read_text())
    rows = {row['setup_id']: row for row in plan['decision_setups']}
    assert len(selected['setups']) == 12
    fields = {'price_change_pct': 'price_change_pct', 'oi_change_pct': 'oi_change_pct',
              'five_minute_volume_ratio_min': 'volume_ratio', 'one_minute_body_ratio_min': 'body_ratio',
              'one_minute_wick_ratio_max': 'max_wick_ratio',
              'one_minute_volume_ratio_min': 'minimum_confirmation_volume_ratio'}
    for setup in selected['setups']:
        actual = rows[setup['setup_id']]
        assert actual['target_pct'] == setup['target_pct']
        assert actual['stop_pct'] == setup['initial_stop_pct']
        assert actual['confirmation_end'] == setup['confirmation_end']
        assert actual['max_entries'] == setup['max_entries'] == 1
        assert actual['picker'] == setup['picker'] == 'max_liquidity'
        for source, target in fields.items():
            assert actual[target] == setup['thresholds'][source]
    for before in previous['active_setups']:
        setup_id = before['confirmation_end'].replace(':', '') + '_' + before['side']
        assert {key: rows[setup_id][key] for key in before if key != 'source_version'} == {
            key: value for key, value in before.items() if key != 'source_version'}
        assert rows[setup_id]['minimum_confirmation_volume_ratio'] == previous[
            'minimum_confirmation_1m_volume_ratio_by_setup'].get(setup_id, previous[
                'minimum_confirmation_1m_volume_ratio'][before['side']])
    assert len(previous['active_setups']) == 32
    assert len(rows) == 44 and len(plan['signal_to_confirmation']) == 30
    assert plan['exit_policy'] == dict(initial_stop_pct=1.25, tightened_stop_pct=1.,
        tighten_after_minutes=120, entry_expiry_minutes=10, square_off='15:15')
    assert not plan['mutates_controls'] and not plan['starts_workers'] and not plan['submits_orders']


def test_approval_requires_current_fingerprint_and_preserves_sizing(state, report, tmp_path):
    approved, arm = op.approval_payloads(DAY, state, report, now=NOW, runtime_root=tmp_path)
    assert approved['live_execution_policy'] == state['live_execution_policy']
    assert state['live_activation_authorized'] is False
    assert approved['live_approval']['paper_validation'] == 'OPERATOR_OVERRIDE_NOT_ATTESTED'
    assert approved['live_approval']['setup_ids'] == arm['setup_ids']
    op.validate_launch(approved, arm, now=NOW, runtime_root=tmp_path)
    with pytest.raises(ValueError, match='today'):
        op.validate_launch(approved, arm, now=NOW + timedelta(days=1), runtime_root=tmp_path)


@pytest.mark.parametrize('key,value', [('strategy_fingerprint','stale'),
    ('live_execution_policy_fingerprint','stale'), ('session_date','2026-10-12'),
    ('setup_ids',['1006_LONG']), ('enabled',False)])
def test_stale_or_partial_arm_rejected(state, report, tmp_path, key, value):
    approved, arm = op.approval_payloads(DAY, state, report, now=NOW, runtime_root=tmp_path)
    arm[key] = value
    with pytest.raises(ValueError):
        op.validate_launch(approved, arm, now=NOW, runtime_root=tmp_path)


@pytest.mark.parametrize('name', ['v13_v10_g_3_live','v6_live','v13_v10_g_live','control'])
def test_never_clears_existing_emergency_switch(state, tmp_path, name):
    path = tmp_path / 'fno_oi' / name / 'kill_switch.json'
    path.parent.mkdir(parents=True)
    path.write_text('{"enabled":true}')
    before = path.read_bytes()
    with pytest.raises(ValueError, match='kill switch'):
        op.plan(DAY, state, now=NOW, runtime_root=tmp_path)
    assert path.read_bytes() == before


@pytest.mark.parametrize('field,value', [('open_net_positions',1),('nonterminal_orders',1),
    ('funds_positive',False),('state','SESSION_INVALID_OR_EXPIRED')])
def test_broker_failure_or_unreconciled_account_blocks(report, field, value):
    report['apps'][0][field] = value
    with pytest.raises(ValueError):
        op.validate_broker_report(report, now=NOW)


def test_stale_broker_proof_and_changed_profile_are_rejected(state, report, tmp_path):
    with pytest.raises(ValueError, match='60 seconds'):
        op.validate_broker_report(report, now=NOW + timedelta(seconds=61))
    state['strategy_fingerprint'] = 'stale'
    with pytest.raises(ValueError, match='does not match'):
        op.approval_payloads(DAY, state, report, now=NOW, runtime_root=tmp_path)


@pytest.mark.parametrize('day', [date(2026,10,12),date(2026,10,17),date(2026,10,23)])
def test_old_weekend_or_distant_dates_rejected(state, tmp_path, day):
    with pytest.raises(ValueError):
        op.plan(day, state, now=NOW, runtime_root=tmp_path)


def test_approve_without_explicit_operator_flag_cannot_write(state, monkeypatch):
    monkeypatch.setattr(op, 'read', lambda path: state)
    monkeypatch.setattr(op, 'atomic_json', lambda *a: pytest.fail('Unexpected control mutation'))
    assert op.main(['approve']) == 2


def test_acknowledgement_is_scoped_to_validated_child_only(state, report, tmp_path, monkeypatch):
    approved, arm = op.approval_payloads(DAY, state, report, now=NOW, runtime_root=tmp_path)
    monkeypatch.setattr(op, 'read', lambda path: approved if path == op.PROFILE else arm)
    validate = op.validate_launch
    monkeypatch.setattr(op, 'validate_launch', lambda s,a: validate(s,a,now=NOW,runtime_root=tmp_path))
    calls=[]
    monkeypatch.setattr(op.subprocess, 'call', lambda *a,**kw: calls.append((a,kw)) or 0)
    monkeypatch.delenv(op.config.LIVE_ACK_ENV, raising=False)
    assert op.main(['run-approved']) == 0
    assert calls[0][1]['env'][op.config.LIVE_ACK_ENV] == op.config.LIVE_ACK
    assert op.config.LIVE_ACK_ENV not in op.os.environ
    assert 'activate_fno_v13_v10_g_3_live_kite_qty1.bat' in calls[0][0][0]


@pytest.mark.skipif(os.name != 'nt' or not shutil.which('powershell'), reason='Windows operator wrapper')
@pytest.mark.parametrize('enable_fails', [False, True])
@pytest.mark.parametrize('had_arm', [False, True])
def test_operator_task_transition_and_failure_restore_are_simulated_only(tmp_path, enable_fails, had_arm):
    bat = tmp_path / 'bat'
    bat.mkdir()
    for name in ('approve_g3_live.ps1', 'g3_task_xml_helpers.ps1'):
        shutil.copy2(op.ROOT / 'bat' / name, bat / name)
    profile, arm = tmp_path / 'profile.json', tmp_path / 'arm.json'
    profile.write_text('{"live_activation_authorized":false}')
    if had_arm:
        arm.write_text('{"enabled":false,"previous":true}')
    original_profile = profile.read_bytes()
    original_arm = arm.read_bytes() if had_arm else None
    prepared = dict(task='SIMULATED_G3_TASK', deployment_file=str(profile), arm_file=str(arm),
        scheduled_launcher=str(bat / 'run_g3_approved_live.bat'), session_date='2026-10-14', active_setup_count=44)
    (tmp_path / 'plan.json').write_text(json.dumps(prepared))
    # Every scheduler, process and Python/broker operation is replaced before
    # loading the copied wrapper. All paths are inside this disposable root.
    escaped = str(tmp_path).replace("'", "''")
    script = r'''
$ErrorActionPreference='Stop'
$TestRoot='__ROOT__'
$global:SimulatedEnabled=$false
$global:SimulatedRegistered=@()
function FakePython {
    if ($args -contains 'plan') { Get-Content -LiteralPath (Join-Path $TestRoot 'plan.json') -Raw }
    elseif ($args -contains 'approve') {
        [IO.File]::WriteAllText((Join-Path $TestRoot 'profile.json'), '{"live_activation_authorized":true}')
        [IO.File]::WriteAllText((Join-Path $TestRoot 'arm.json'), '{"enabled":true}')
    }
    $global:LASTEXITCODE=0
}
function Get-ScheduledTask { param($TaskName); [pscustomobject]@{State='Disabled'} }
function Get-CimInstance { }
function Export-ScheduledTask { param($TaskName); '<Task xmlns="http://schemas.microsoft.com/windows/2004/02/mit/task"><Triggers><CalendarTrigger><StartBoundary>2026-09-15T09:15:00</StartBoundary></CalendarTrigger></Triggers><Settings><Enabled>false</Enabled></Settings><Actions><Exec><Command>cmd.exe</Command><Arguments>old</Arguments></Exec></Actions></Task>' }
function Register-ScheduledTask { param($TaskName,$Xml,[switch]$Force); $global:SimulatedRegistered += $Xml }
function Disable-ScheduledTask { param($TaskName); $global:SimulatedEnabled=$false }
function Enable-ScheduledTask { param($TaskName); __ENABLE__ }
try {
    & (Join-Path $TestRoot 'bat/approve_g3_live.ps1') -SessionDate '2026-10-14' -Apply -Python FakePython | Out-Null
    $outcome='SUCCESS'
} catch { $outcome='RESTORED'; $simulationError=$_.Exception.Message }
[pscustomobject]@{Error=$simulationError;Outcome=$outcome;Enabled=$global:SimulatedEnabled;Registrations=$global:SimulatedRegistered.Count;FirstXml=$global:SimulatedRegistered[0];LastXml=$global:SimulatedRegistered[-1]} | ConvertTo-Json
'''.replace('__ROOT__', escaped).replace('__ENABLE__', "throw 'SIMULATED_ENABLE_FAILURE'" if enable_fails else '$global:SimulatedEnabled=$true')
    encoded = base64.b64encode(script.encode('utf-16-le')).decode()
    result = subprocess.run(['powershell','-NoProfile','-ExecutionPolicy','Bypass','-EncodedCommand',encoded],capture_output=True,text=True,timeout=25)
    assert result.returncode == 0, result.stderr
    observed = json.loads(result.stdout)
    assert observed['FirstXml'] is not None, observed
    assert 'run_g3_approved_live.bat' in observed['FirstXml']
    assert '<Enabled>false</Enabled>' in observed['FirstXml']
    assert '2026-09-15T09:15:00' in observed['FirstXml']
    if enable_fails:
        assert observed['Outcome'] == 'RESTORED' and observed['Registrations'] == 2 and not observed['Enabled']
        assert profile.read_bytes() == original_profile
        assert arm.read_bytes() == original_arm if had_arm else not arm.exists()
        assert '<Arguments>old</Arguments>' in observed['LastXml']
    else:
        assert observed['Outcome'] == 'SUCCESS' and observed['Enabled'] and observed['Registrations'] == 1
        assert json.loads(profile.read_text())['live_activation_authorized']
        assert json.loads(arm.read_text())['enabled']
