"""Current G3 result presentation follows the shared setup count."""
from pathlib import Path
import subprocess
import sys

import dashboard_flow as flow


def test_daily_launcher_imports_in_a_fresh_process():
    result = subprocess.run([sys.executable, "-m", "backtesting_result_v13_v10_g_3_daily", "--help"],
        cwd=Path(__file__).resolve().parents[1], capture_output=True, text=True, timeout=30)
    assert result.returncode == 0, result.stderr
    assert "--date" in result.stdout


def test_current_historical_copy_uses_selected_run_setup_count(tmp_path, monkeypatch):
    monkeypatch.setattr(flow, "DATA_ROOT", tmp_path)
    path = tmp_path / "daily_results.csv"
    path.write_text("day,trades,wins,losses,net_pnl\n2026-10-13,0,0,0,0\n")
    metadata = dict(strategy="V13-V10-G-3", kind="backtest", verified=True,
        run_name="midday", id="midday", period_start="2026-10-13", period_end="2026-10-13",
        sessions=1, active_setup_count=44, current_full_strategy=True,
        strategy_matches_active=True, strategy_matches_backtest=True, role="backtest_history")
    monkeypatch.setattr(flow, "_verified_g3_current_backtest", lambda: [(metadata, path)])
    for name in ("_verified_g3_runs", "_verified_g3_0920_long_backtest", "_verified_g3_0920_daily",
                 "_verified_g3_research_runs"):
        monkeypatch.setattr(flow, name, lambda: [])
    result = flow.load_flow_data(strategy="V13-V10-G-3")
    copy = next(item for item in result["warnings"] if item.startswith("Current G-3 historical backtest:"))
    assert "44 setups" in copy and "44-setup decision configuration" in copy
    assert "32-setup" not in copy
