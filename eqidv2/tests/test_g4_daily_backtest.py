"""Safety and parity tests for the isolated G4 daily replay."""
from datetime import date

import numpy as np
import pandas as pd
import pytest

from research import g4_daily_backtest as daily


def test_daily_cannot_write_into_cumulative_g4_runs():
    with pytest.raises(ValueError, match="daily/runs"):
        daily.run(daily.g4.DEFAULT_CONFIG, daily.g4.bounded.G4_ROOT / "runs" / "bad",
                  day=date(2026, 10, 21))


def test_g4_daily_rejects_future_session():
    with pytest.raises(ValueError, match="closed session"):
        daily.source.load_source(date(2099, 1, 1))


def test_today_g4_matches_independently_verified_g6_unfiltered_result():
    g6_root = daily.g4.bounded.G4_ROOT.parent / "backtesting_result_v13_v10_g_6"
    g6_run = g6_root / "daily/runs/2026-10-21/20261021T173211642474_659533af_g6_nifty_futures"
    if not g6_run.exists():
        pytest.skip("Sealed G6 V1 daily integration fixture unavailable")
    assert daily.g4.mirror.sha256(g6_run / "manifest.json") == "6f4d5ff77e81945322e9f459be2ed24a5252a1b52ed824c6977c04f4219ad119"
    try:
        source_dir, payload, provenance = daily.source.load_source(date(2026, 10, 21))
    except ValueError as exc:
        if "No verified complete current-profile G3 daily source" not in str(exc):
            raise
        pytest.skip("The sealed 21 October G3 daily fixture predates the current profile")
    prices = daily.g4.Prices(dict(daily_path_evidence=[{key:provenance[key] for key in
        ("snapshot_manifest", "snapshot_manifest_sha256")}]))
    config, identity = daily.g4.configuration(), daily.g4.Identity(daily.g4.configuration())
    orders, _, _ = daily.source.select_orders(date(2026, 10, 21), source_dir, payload,
                                               prices, config, identity=identity)
    orders = daily.preserve_frozen_1011_short(date(2026, 10, 21), source_dir,
                                              payload, orders, identity)
    paths = prices.paths(orders.sort_values(["tradingsymbol", "confirmation_ts"]))
    native = daily.g4.bounded.history._simulate(orders, paths, [date(2026, 10, 21)], identity)
    actual = daily.g4._bounded(native, paths, config).set_index(["day", "setup_id", "tradingsymbol"])
    expected = daily.read_frozen_csv(g6_run / "g6_unfiltered/portfolio_trades.csv").set_index(
        ["day", "setup_id", "tradingsymbol"])
    assert actual.index.tolist() == expected.index.tolist()
    for field in ("portfolio_executed", "filled", "exit_reason"):
        pd.testing.assert_series_equal(actual[field], expected[field], check_dtype=False)
    for field in ("entry_ts", "exit_ts"):
        pd.testing.assert_series_equal(pd.to_datetime(actual[field], utc=True),
                                      pd.to_datetime(expected[field], utc=True))
    for field in ("entry_price", "exit_price", "portfolio_net_profit_rupees"):
        np.testing.assert_allclose(actual[field], expected[field], rtol=0, atol=1e-6, equal_nan=True)
