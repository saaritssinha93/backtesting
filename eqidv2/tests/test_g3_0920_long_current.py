"""The 09:20 LONG history is visible only while its sealed result is intact."""
from __future__ import annotations

import csv
from datetime import date, timedelta
import json
from pathlib import Path

import dashboard_flow as flow
import pytest
from research import g3_0920_long_current as current
from research import g3_0920_long_daily as daily


def test_old_daily_sidecar_cannot_duplicate_promoted_production_setup(tmp_path, monkeypatch):
    monkeypatch.setattr(daily.production, "_read_verified_pointer", lambda *a, **kw:
        (tmp_path, dict(session_date="2026-10-12", strategy_version="V13-V10-G-3-PRODUCTION-V3")))
    with pytest.raises(ValueError, match="already includes 09:20 LONG"):
        daily._production_day(date(2026, 10, 12), tmp_path)


def _csv(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def test_dashboard_selects_only_verified_0920_long_history(tmp_path, monkeypatch):
    monkeypatch.setattr(flow, "DATA_ROOT", tmp_path)
    monkeypatch.setattr(flow, "active_profile_name", lambda: "V13_V10_G_3")
    root = tmp_path / "backtesting_result_v13_v10_g_3"
    run = root / "backtests/runs/2026-10-12/0920_long_t300"
    days = [(date(2026, 10, 12) - timedelta(days=48 - index)).isoformat()
            for index in range(49)]
    _csv(run / "daily_results.csv", [dict(day=day, variant_fills=int(index == 48),
         variant_wins=int(index == 48), variant_net_pnl_rs=100 if index == 48 else 0)
         for index, day in enumerate(days)])
    _csv(run / "variant_trades.csv", [dict(day=days[-1], tradingsymbol="AAA", side="LONG",
         filled=True, portfolio_executed=True, portfolio_net_profit_rupees=100)])
    _csv(run / "added_0920_orders.csv", [dict(day=days[-1], signal_end="09:20",
         side="LONG", target_pct=3.0)])
    (run / "config.json").write_text(json.dumps(current._config()), encoding="utf-8")
    (run / "summary.json").write_text(json.dumps(dict(variant_id=current.VARIANT_ID,
         scope="BACKTEST_ONLY", execution_authority=False, short_0920_enabled=False,
         existing_g3_trades_unchanged=True, sessions=49, period_start=days[0],
         period_end=days[-1], created_at_ist="2026-10-13T01:00:00+05:30",
         with_0920_long=dict(executed_trades=1, wins=1, net_pnl_rs=100),
         added_0920_long=dict(selected_orders=1))), encoding="utf-8")
    code = (Path(current.__file__).resolve(), Path(current.paired.__file__).resolve(),
            Path(current.__file__).with_name("g3_0920_strategy.py"), current.CONFIG_PATH)
    manifest = dict(variant_id=current.VARIANT_ID, execution_authority=False,
        code_sha256={path.name: current.paired.sha256(path) for path in code},
        files={path.name: current.paired.sha256(path) for path in run.iterdir() if path.is_file()})
    (run / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    current.publish(root, run)

    selected = flow.load_flow_data(strategy="V13-V10-G-3")
    assert selected["selected_run"]["target_pct"] == 3.0
    assert selected["selected_run"]["short_0920_enabled"] is False
    assert selected["summary"]["net_pnl"] == 100
    assert any("retrospective opening OI" in warning
               for warning in selected["warnings"])

    (run / "variant_trades.csv").write_text("tampered", encoding="utf-8")
    assert not flow._verified_g3_0920_long_backtest()


def test_daily_sidecar_is_separate_and_rejects_older_pointer(tmp_path, monkeypatch):
    monkeypatch.setattr(flow, "DATA_ROOT", tmp_path)
    monkeypatch.setattr(flow, "active_profile_name", lambda: "V13_V10_G_3")
    root = tmp_path / "backtesting_result_v13_v10_g_3"
    run = root / "backtests/daily/runs/2026-10-12/sidecar"
    _csv(run / "daily_results.csv", [dict(day="2026-10-12", variant_fills=1,
         variant_wins=0, variant_net_pnl_rs=-100)])
    _csv(run / "variant_trades.csv", [dict(day="2026-10-12", tradingsymbol="AAA",
         side="LONG", filled=True, portfolio_executed=True,
         portfolio_net_profit_rupees=-100)])
    _csv(run / "added_0920_orders.csv", [dict(day="2026-10-12", signal_end="09:20",
         side="LONG", target_pct=3.0)])
    (run / "config.json").write_text(json.dumps(current._config()), encoding="utf-8")
    (run / "summary.json").write_text(json.dumps(dict(variant_id=current.VARIANT_ID,
         scope="BACKTEST_ONLY", execution_authority=False, short_0920_enabled=False,
         existing_g3_trades_unchanged=True, sessions=1, period_start="2026-10-12",
         period_end="2026-10-12", created_at_ist="2026-10-13T01:00:00+05:30",
         with_0920_long=dict(executed_trades=1, wins=0, net_pnl_rs=-100),
         added_0920_long=dict(selected_orders=1))), encoding="utf-8")
    code = (Path(daily.__file__).resolve(), Path(current.__file__).resolve(),
            Path(daily.paired.__file__).resolve(), Path(daily.exporter.__file__).resolve(),
            Path(daily.__file__).with_name("g3_0920_strategy.py"), current.CONFIG_PATH)
    (run / "manifest.json").write_text(json.dumps(dict(variant_id=current.VARIANT_ID,
         execution_authority=False,
         code_sha256={path.name: daily.paired.sha256(path) for path in code},
         files={path.name: daily.paired.sha256(path) for path in run.iterdir() if path.is_file()})),
         encoding="utf-8")
    daily.publish(root, run)
    found = flow._verified_g3_0920_daily()
    assert len(found) == 1 and found[0][0]["role"] == "backtest_daily"
    assert flow.load_flow_data(found[0][0]["id"])["summary"]["net_pnl"] == -100
    pointer_path = root / "latest" / daily.POINTER_NAME
    pointer = json.loads(pointer_path.read_text(encoding="utf-8"))
    pointer["session_date"] = "2026-10-13"
    pointer_path.write_text(json.dumps(pointer), encoding="utf-8")
    try:
        daily.publish(root, run)
    except ValueError as exc:
        assert "newer" in str(exc)
    else:
        raise AssertionError("An older sidecar replaced the newer pointer")
