"""Daily G6 isolation and regression against the saved historical pipeline."""
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

import fno_v13_v10_g_6_backtest as cli
from research import g6_daily_backtest as daily


def test_cli_daily_routes_to_isolated_runner(monkeypatch, capsys):
    monkeypatch.setattr("sys.argv", ["g6", "--date", "2026-10-21"])
    monkeypatch.setattr(cli, "run", lambda *a, **k: pytest.fail("Must not run full history"))
    seen = {}

    def fake(config, output, **kwargs):
        seen.update(output=output, **kwargs)
        return dict(variant_id=daily.g6.VERSION, sessions=1, first_date="2026-10-21",
                    last_date="2026-10-21", active_setups=51, comparison=[])

    monkeypatch.setattr(daily, "run", fake)
    cli.main()
    assert seen["day"] == date(2026, 10, 21)
    assert seen["direction_filter_mode"] == daily.modes.NIFTY_FILTER
    assert seen["output"].is_relative_to(daily.DAILY_ROOT)
    assert seen["output"].parent.name == "2026-10-21"


@pytest.mark.parametrize("extra", [["--through", "2026-10-21"],
    ["--direction-filter-mode", "primary_index_both_counter_skip"]])
def test_cli_daily_rejects_incompatible_options(monkeypatch, extra):
    monkeypatch.setattr("sys.argv", ["g6", "--date", "2026-10-21"] + extra)
    with pytest.raises(SystemExit) as error:
        cli.main()
    assert error.value.code == 2


def test_daily_does_not_use_unverified_source(monkeypatch):
    monkeypatch.setattr(daily, "_verified_daily", lambda *a: None)
    with pytest.raises(ValueError, match="No verified complete"):
        daily.load_source(date(2026, 10, 19))


def test_daily_requires_closed_session():
    with pytest.raises(ValueError, match="closed session"):
        daily.load_source(date(2099, 1, 1))


def test_daily_cannot_write_into_historical_runs():
    with pytest.raises(ValueError, match="daily/runs"):
        daily.run(daily.g6.DEFAULT_CONFIG, daily.g6.G6_ROOT / "runs" / "bad",
                  day=date(2026, 10, 21))


def test_daily_selection_and_replay_reproduce_october19_historical_result():
    root = daily.g6.G6_ROOT / "runs/2026-10-21/20261021T151944559417_7b43ad5e_g6_primary_index"
    if not root.exists():
        pytest.skip("Local sealed G6 integration fixture unavailable")
    assert daily.base.sha256(root / "manifest.json") == "1255501c24a98654177f7ccadec2c89b3d1b002e68fb170fdb30b3dbb29e3b08"
    manifest = daily.base.read(root / "manifest.json")
    for filename in ("summary.json", "g6_unfiltered/portfolio_trades.csv",
                     daily.g6.NIFTY_POLICY + "/portfolio_trades.csv"):
        assert daily.base.sha256(root / filename) == manifest["files"][filename]
    historical = daily.base.read(root / "summary.json")
    if historical["variant_id"] != daily.g6.VERSION:
        pytest.skip("Sealed G6 integration fixture predates the 10:11 SHORT V2 rule")
    g3_run = Path(historical["current_g3_source_run"])
    assert daily.base.sha256(g3_run / "manifest.json") == historical["current_g3_manifest_sha256"]
    source_summary = daily.base.read(g3_run / "summary.json")
    evidence = next(x for x in source_summary["daily_path_evidence"] if "2026-10-19" in x["run_dir"])
    directory = Path(evidence["run_dir"])
    source_record = next(x for x in source_summary["daily_sources"] if "2026-10-19" in x["run_dir"])
    assert daily.base.sha256(directory / "run_manifest.json") == source_record["manifest_sha256"]
    payload = daily.base.read(directory / "run_manifest.json")
    for item in payload["artifacts"].values():
        assert daily.base.sha256(directory / item["path"]) == item["sha256"]
    prices = daily.g6.Prices(dict(daily_path_evidence=[evidence]))
    config, day = daily.g6.configuration(), date(2026, 10, 19)
    orders, _, _ = daily.select_orders(day, directory, payload, prices, config)
    paths = prices.paths(orders.sort_values(["tradingsymbol", "confirmation_ts"]))
    unfiltered = daily.bounded_replay(orders, paths, day, config)
    audit, _, _ = daily.nifty.order_audit(unfiltered, prices)
    filtered = daily.filter_book(unfiltered, audit, config)
    for name, result in (("g6_unfiltered", unfiltered), (daily.g6.NIFTY_POLICY, filtered)):
        baseline = daily.read_frozen_csv(root / name / "portfolio_trades.csv")
        baseline = baseline.loc[baseline.day.eq(str(day))].set_index("signal_id").sort_index()
        actual = result.set_index("signal_id").sort_index()
        assert actual.index.tolist() == baseline.index.tolist()
        for field in ("portfolio_executed", "filled", "exit_reason"):
            pd.testing.assert_series_equal(actual[field], baseline[field], check_dtype=False)
        for field in ("entry_ts", "exit_ts"):
            pd.testing.assert_series_equal(pd.to_datetime(actual[field], utc=True),
                                          pd.to_datetime(baseline[field], utc=True))
        for field in ("entry_price", "exit_price", "portfolio_net_profit_rupees",
                      "portfolio_gross_profit_rupees", "portfolio_cost_rupees"):
            np.testing.assert_allclose(actual[field], baseline[field], rtol=0, atol=1e-6, equal_nan=True)
