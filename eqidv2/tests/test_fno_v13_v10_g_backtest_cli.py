"""Main G backtest defaults to dated production without touching frozen runs."""
import json
from datetime import date, datetime

import pytest

import fno_v13_v10_g_backtest as g
import fno_v13_v10_g_daily_replay as replay


@pytest.fixture
def cli(monkeypatch, tmp_path):
    common = g.v9.v5.common
    now = datetime(2026, 10, 6, 16, 30, tzinfo=common.IST)
    monkeypatch.setattr(common, 'now_ist', lambda: now)
    monkeypatch.setattr(common, 'load_holidays', set)
    monkeypatch.setattr(common, 'is_trading_day', lambda day, holidays: True)
    monkeypatch.setattr(g, 'DEFAULT_PRODUCTION_OUTPUT', tmp_path / 'production')
    calls = []

    def run(day, output):
        calls.append((day, output))
        return dict(complete=True, session_date=day.isoformat(),
                    strategy_policy=g.policy.policy_for_day(day))

    monkeypatch.setattr(replay, 'replay_day', run)
    return calls, now


def test_no_arguments_use_today_and_promoted_policy(cli, tmp_path, capsys):
    calls, now = cli
    assert g.main([]) == 0
    assert calls == [(now.date(), tmp_path / 'production' / '2026-10-06' / now.strftime('%Y%m%dT%H%M%S%f'))]
    result = json.loads(capsys.readouterr().out)
    assert result['strategy_policy']['relaxed_0925_long'] is True
    assert result['strategy_policy']['staged_stop'] is True


@pytest.mark.parametrize('option', ['--session-date', '--date'])
def test_explicit_historical_date_retains_its_old_policy(cli, tmp_path, capsys, option):
    output = tmp_path / 'historical'
    assert g.main([option, '2026-10-05', '--output-dir', str(output)]) == 0
    assert cli[0] == [(date(2026, 10, 5), output)]
    assert json.loads(capsys.readouterr().out)['strategy_policy']['staged_stop'] is False


@pytest.mark.parametrize('clock,state,code', [
    (datetime(2026, 10, 5, 16, 30), 'BLOCKED_FUTURE_DATE', 2),
    (datetime(2026, 10, 6, 15, 29, 59), 'WAITING_FOR_SESSION_CLOSE', 2),
])
def test_future_or_unclosed_session_does_not_start_replay(cli, monkeypatch, capsys, clock, state, code):
    monkeypatch.setattr(g.v9.v5.common, 'now_ist', lambda: clock.replace(tzinfo=g.v9.v5.common.IST))
    assert g.main(['--session-date', '2026-10-06']) == code
    assert not cli[0]
    assert json.loads(capsys.readouterr().out)['state'] == state


def test_nontrading_day_skips_without_rolling_back(cli, monkeypatch, capsys):
    monkeypatch.setattr(g.v9.v5.common, 'is_trading_day', lambda *args: False)
    assert g.main([]) == 0
    assert not cli[0]
    result = json.loads(capsys.readouterr().out)
    assert result['state'] == 'SKIPPED_NON_TRADING_DAY'
    assert result['session_date'] == '2026-10-06'


def test_closed_session_can_run_and_incomplete_result_returns_two(cli, monkeypatch):
    monkeypatch.setattr(g.v9.v5.common, 'now_ist', lambda: cli[1].replace(hour=15, minute=30))
    monkeypatch.setattr(replay, 'replay_day', lambda *args: dict(complete=False))
    assert g.main([]) == 2


@pytest.mark.parametrize('arguments', [
    ['--source-dir', 'source'], ['--config-json', 'config.json'],
    ['--session-date', '2026-10-06', '--frozen-research'],
])
def test_ambiguous_research_options_are_rejected_before_replay(cli, arguments):
    with pytest.raises(SystemExit) as exc:
        g.main(arguments)
    assert exc.value.code == 2
    assert not cli[0]


def test_frozen_research_remains_explicit_and_reproducible(cli, monkeypatch, tmp_path):
    seen = []
    monkeypatch.setattr(g, '_run_frozen_research', lambda *args: seen.append(args) or 0)
    assert g.main(['--frozen-research']) == 0
    assert seen == [(g.DEFAULT_SOURCE, g.DEFAULT_OUTPUT / 'frozen_config.json', g.DEFAULT_OUTPUT / 'cli_replay')]
    source, config, output = (tmp_path / part for part in ('source', 'config.json', 'output'))
    assert g.main(['--frozen-research', '--source-dir', str(source), '--config-json', str(config),
                   '--output-dir', str(output)]) == 0
    assert seen[-1] == (source, config, output)
    assert not cli[0]


def test_production_does_not_overwrite_existing_results(cli, tmp_path):
    output = tmp_path / 'existing'
    output.mkdir()
    previous = output / 'daily_results.csv'
    previous.write_text('existing historical result\n', encoding='utf-8')
    with pytest.raises(SystemExit) as exc:
        g.main(['--output-dir', str(output)])
    assert exc.value.code == 2
    assert not cli[0]
    assert previous.read_text(encoding='utf-8') == 'existing historical result\n'
