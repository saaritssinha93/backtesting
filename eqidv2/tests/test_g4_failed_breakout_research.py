"""Entry, allocation and partial accounting integration for the fixed G4 test."""
import json

import numpy as np
import pandas as pd
import pytest

from research import g4_failed_breakout_research as experiment


DAY = "2026-10-16"
TZ = "Asia/Kolkata"


def stamp(clock):
    return pd.Timestamp(f"{DAY} {clock}", tz=TZ)


def path(rows, start="10:01"):
    values = np.asarray(rows, float)
    times = pd.date_range(stamp(start), periods=len(rows), freq="min")
    return {"timestamp_ns": times.asi8,
            **{name: values[:, i] for i, name in enumerate(("open", "high", "low", "close"))}}


def order(sid, confirmation="10:00", *, side="SHORT"):
    return dict(sid=sid, day=DAY, setup_id=f"{sid}_{side}", tradingsymbol=f"S{sid}",
                side=side, confirmation_ts=stamp(confirmation), trigger=100.,
                confirmation_high=100.5, confirmation_low=99.5,
                native_target_pct=1.5, native_stop_pct=1.2,
                picker="max_liquidity", traded_value=1000.)


def test_saved_fills_are_recomputed_and_unfilled_orders_cannot_trigger_exit_rule():
    rows = [order(0), order(1)]
    for row in rows:
        row.update(filled=True, entry_price=999., entry_ts=stamp("11:00"),
                   entry_path_index=99, exit_reason="SAVED_STALE_RESULT")
    original = pd.DataFrame(rows)
    original_copy = original.copy(deep=True)
    prices = {
        0: path([[100.8, 100.9, 100.5, 100.8], [100.6, 100.7, 99.9, 100.3],
                 [100.3, 100.4, 100.1, 100.2]]),
        1: path([[101, 101.2, 100.8, 101], [101, 101.4, 100.8, 101.3],
                 [101.3, 101.5, 101.1, 101.2]]),
    }
    result = experiment.replay(original, prices, "short_failed_breakout", stamp("10:03")).set_index("sid")
    filled = result.loc[0]
    assert filled.filled and filled.entry_path_index == 1
    assert filled.entry_price == 100. and filled.entry_ts == stamp("10:02")
    assert filled.position_state == "OPEN"
    unfilled = result.loc[1]
    assert not unfilled.filled and not unfilled.portfolio_executed
    assert pd.isna(unfilled.entry_ts) and np.isnan(unfilled.entry_price)
    assert unfilled.failed_breakout_audit_reason == "UNFILLED"
    assert not unfilled.failed_breakout_exit_applied
    assert unfilled.valuation_net_rs == 0
    pd.testing.assert_frame_equal(original, original_copy)


def test_early_exit_releases_real_portfolio_capital_and_admits_previously_rejected_order():
    original = pd.DataFrame([order(i) for i in range(10)] + [order(10, "10:02")])
    prices = {i: path([[100, 100.1, 99.9, 100]] * 6) for i in range(10)}
    prices[0] = path([[100, 100.1, 99.9, 100], [100, 100.7, 99.9, 100.6],
                      [100.8, 100.9, 99.9, 100.3], *[[100, 100.1, 99.9, 100]] * 3])
    prices[10] = path([[100, 100.1, 99.9, 100]] * 4, "10:03")
    cutoff = stamp("10:06")
    before = experiment.replay(original, prices, "current_g4", cutoff)
    after = experiment.replay(original, prices, "short_failed_breakout", cutoff)
    b, a = (frame.set_index("sid") for frame in (before, after))
    assert before.filled.sum() == after.filled.sum() == 11
    assert before.portfolio_executed.sum() == 10
    assert not b.loc[10].portfolio_executed
    assert b.loc[10].portfolio_reject_reason == "INSUFFICIENT_PORTFOLIO_CAPITAL"
    assert b.loc[10].portfolio_reserved_capital_before_rupees == 1_000_000
    assert a.loc[0].exit_reason == "FAILED_BREAKOUT_EXIT"
    assert a.loc[0].exit_ts == stamp("10:02")
    assert after.portfolio_executed.sum() == 11
    assert a.loc[10].portfolio_executed
    assert a.loc[10].portfolio_reserved_capital_before_rupees == 900_000
    assert a.loc[10].portfolio_open_positions_before == 9
    assert a.loc[10].entry_price == b.loc[10].entry_price == 100.
    assert a.loc[10].entry_ts == b.loc[10].entry_ts == stamp("10:03")
    paired = experiment.changes(before, after, "valuation_net_rs").set_index("sid")
    assert paired.loc[10].newly_allocated
    assert paired.newly_allocated.sum() == 1
    assert paired.delta_net_rs.sum() == pytest.approx(after.valuation_net_rs.sum() - before.valuation_net_rs.sum())
    assert a.loc[0].realized_net_rs == pytest.approx(-4_250)
    opened = after.position_state.eq("OPEN")
    assert after.loc[opened].realized_net_rs.sum() == 0
    assert after.loc[opened].portfolio_net_profit_rupees.sum() == 0
    assert after.valuation_net_rs.sum() == pytest.approx(
        after.realized_net_rs.sum() + after.open_mtm_net_after_reserved_cost_rs.sum())


def test_partial_open_positions_reserve_capital_and_keep_estimated_mtm_out_of_realized_pnl():
    original = pd.DataFrame([order(i) for i in range(11)])
    prices = {i: path([[100, 100.1, 99.9, 100], [100, 100.3, 99.9, 100.2]]) for i in range(11)}
    result = experiment.replay(original, prices, "short_failed_breakout", stamp("10:02"))
    assert result.filled.sum() == 11 and result.portfolio_executed.sum() == 10
    assert result.position_state.eq("OPEN").sum() == 10
    assert result.position_state.eq("PORTFOLIO_REJECTED").sum() == 1
    assert result.realized_net_rs.sum() == 0
    assert result.portfolio_net_profit_rupees.sum() == 0
    assert result.open_mtm_net_after_reserved_cost_rs.sum() == pytest.approx(-12_500)
    assert result.valuation_net_rs.sum() == pytest.approx(-12_500)
    assert result.exit_ts.isna().all()
    assert result.loc[result.position_state.eq("OPEN"), "allocation_release_ts"].eq(stamp("10:03")).all()


def test_partial_prices_ignore_extreme_future_bars_before_path_replay(tmp_path):
    snapshot = tmp_path / "snapshot"
    equities = snapshot / "equity_1m"
    equities.mkdir(parents=True)
    parquet = equities / "S0_stocks_indicators_1min.parquet"
    frame = pd.DataFrame(dict(
        date=pd.date_range(stamp("10:01"), periods=4, freq="min"),
        open=[100., 100., 100.1, 110.], high=[100.1, 100.2, 100.3, 120.],
        low=[99.9, 99.9, 100., 80.], close=[100., 100.1, 100.2, 110.]))
    frame.to_parquet(parquet, index=False)
    manifest_path = snapshot / "snapshot_manifest.json"
    manifest_path.write_text(json.dumps({"sources": [dict(captured=True, role="EQUITY_ONE_MINUTE",
        snapshot_relative_path="equity_1m/" + parquet.name,
        sha256=experiment.base.sha256(parquet))]}), encoding="utf-8")
    summary = dict(snapshot_manifest=str(manifest_path),
                   snapshot_manifest_sha256=experiment.base.sha256(manifest_path),
                   session_date=DAY, as_of_ist=str(stamp("10:03")))
    prices = experiment.PartialPrices(summary)
    orders = pd.DataFrame([order(0)])
    paths = prices.paths(orders)
    assert len(paths[0]["close"]) == 3
    assert paths[0]["timestamp_ns"][-1] == stamp("10:03").value
    result = experiment.replay(orders, paths, "short_failed_breakout", prices.cutoff).iloc[0]
    assert result.position_state == "OPEN" and result.exit_reason == "OPEN_AT_CUTOFF"
    assert result.mark_price == 100.2
    assert result.realized_net_rs == 0
    assert result.open_mtm_net_after_reserved_cost_rs == pytest.approx(-1_250)
    assert pd.isna(result.exit_ts)
