from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from tools import fno_daily_strategy_dashboard as daily


def _fixture_run(root: Path) -> Path:
    root.mkdir(parents=True)
    rows = []
    contracts = {}
    for index, strategy in enumerate(daily.EXPECTED_STRATEGIES):
        rows.append(
            {
                "session_date": "2026-09-02",
                "strategy": strategy,
                "candidates": 2,
                "fills": 1,
                "wins": 1,
                "losses": 0,
                "profit_factor": None,
                "net_return_points": 1.0 + index,
                "net_pnl_rs": 100.0 + index,
                "source_complete": True,
                "source_incomplete_symbol_sessions": 0,
                "all_input_candidates": 3 if strategy == "V12_SELECTED" else None,
            }
        )
        contracts[strategy] = [
            {
                "symbol": "TEST",
                "side": "LONG",
                "entry_time": "2026-09-02 09:31:00+05:30",
                "entry_price": 100.0,
                "exit_time": "2026-09-02 10:00:00+05:30",
                "exit_reason": "TARGET",
                "net_pnl_rs": 100.0 + index,
            }
        ]
    pd.DataFrame(rows).to_csv(root / "comparison.csv", index=False)
    (root / "trade_contracts.json").write_text(json.dumps(contracts), encoding="utf-8")
    (root / "manifest.json").write_text(
        json.dumps(
            {
                "schema_version": daily.SCHEMA_VERSION,
                "complete": True,
                "session_date": "2026-09-02",
                "source_complete": True,
                "source_incomplete_symbol_sessions": 0,
            }
        ),
        encoding="utf-8",
    )
    return root


def test_render_is_explicitly_fno_and_contains_all_five_versions(tmp_path: Path) -> None:
    report, _ = daily.render_report(_fixture_run(tmp_path / "run"))

    assert "Backtesting result v6/v8/v10/v11/v12 — FnO" in report
    for friendly_name in daily.FRIENDLY_NAMES.values():
        assert friendly_name in report


def test_publish_replaces_dashboard_report_with_fno_comparison(tmp_path: Path) -> None:
    paths = daily.publish(_fixture_run(tmp_path / "run"), tmp_path / "dashboard")

    assert paths["latest_report"].is_file()
    assert "FnO strategy comparison" in paths["latest_report"].read_text(encoding="utf-8")
    payload = json.loads(paths["combined_json"].read_text(encoding="utf-8"))
    assert payload["schema_version"] == daily.PUBLICATION_SCHEMA_VERSION
    assert [row["strategy"] for row in payload["strategies"]] == list(
        daily.EXPECTED_STRATEGIES
    )


def test_scheduled_runner_cannot_call_equity_avwap_pipeline() -> None:
    runner = (daily.BASE_DIR / "bat" / "run_backtesting_result_v11_1600.bat").read_text(
        encoding="utf-8"
    )

    assert "tools\\fno_daily_strategy_dashboard.py" in runner
    assert "backtesting_result_v11_daily.py" not in runner
    assert "avwap_5min" not in runner.lower()
