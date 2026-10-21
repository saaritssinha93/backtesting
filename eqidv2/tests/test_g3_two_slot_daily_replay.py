"""The new slots survive raw snapshot reconstruction and actual minute replay."""
import json

import numpy as np
import pandas as pd
import pytest

import fno_v13_replay_source as source
import fno_v13_v10_g_3_daily_replay as replay
from tests.test_fno_v13_v10_g_3_daily_replay import DAY, _minute_frame, raw


def _configure_slot(raw, clock, side):
    roots, equity, _ = raw
    prior = pd.read_parquet(equity)
    prior = prior.loc[prior.date.dt.date.ne(DAY)]
    today = _minute_frame(DAY, move=False)
    direction = 1. if side == "LONG" else -1.
    signal = pd.Timestamp(f"{DAY} {clock}", tz="Asia/Kolkata")
    confirmation = signal + pd.Timedelta(minutes=1)
    base = 100 + direction * np.arange(len(today)) * .001
    today[["open", "high", "low", "close"]] = np.column_stack(
        [base - direction * .0005, base + .01, base - .01, base])
    for offset, price in enumerate(100 + direction * np.array([1.2, 1.4, 1.6, 1.8, 2.])):
        stamp = signal - pd.Timedelta(minutes=4-offset)
        today.loc[today.date.eq(stamp), ["open", "high", "low", "close", "volume"]] = (
            [price-.2, price+.05, price-.21, price, 200.] if side == "LONG"
            else [price+.2, price+.21, price-.05, price, 200.])
    today.loc[today.date.eq(confirmation), ["open", "high", "low", "close", "volume"]] = (
        [102., 102.25, 101.99, 102.2, 100.] if side == "LONG"
        else [98.05, 98.06, 97.94, 97.95, 250.])
    today.loc[today.date.gt(confirmation), ["open", "high", "low", "close", "volume"]] = (
        [102.3, 102.4, 102.3, 102.35, 100.] if side == "LONG"
        else [97.90, 97.94, 97.85, 97.90, 100.])
    today.loc[today.date.gt(confirmation + pd.Timedelta(minutes=1)), ["open", "high", "low", "close"]] = (
        [107., 107.2, 107., 107.1] if side == "LONG" else [91., 91.2, 90.9, 91.1])
    pd.concat([prior, today], ignore_index=True).to_parquet(equity, index=False)
    # A valid same-contract first-minute OI creates a 0.1% opening rise.
    future = pd.read_parquet(roots.futures_5m / "EXAMPLE30JANFUT_5minute.parquet")
    current = future.loc[future.timestamp.eq(pd.Timestamp(f"{DAY} 09:20", tz="Asia/Kolkata")), "oi"].item()
    opening = source.opening_inputs.minute_path(roots.opening_observer, DAY, "EXAMPLE30JANFUT")
    evidence = json.loads(opening.read_text())
    evidence["candles"][0]["oi"] = current / 1.001
    opening.write_text(json.dumps(evidence))
    return roots, confirmation


@pytest.mark.parametrize("clock,side,setup_id", [
    ("09:20", "SHORT", "0921_SHORT"), ("10:10", "LONG", "1011_LONG")])
def test_new_slot_raw_inputs_select_and_fill_at_one_and_half_percent(raw, tmp_path, clock, side, setup_id):
    roots, confirmation = _configure_slot(raw, clock, side)
    directory = tmp_path / setup_id
    result = replay.replay_day(DAY, directory, roots=roots)
    assert result["complete"], result["coverage"]
    orders = pd.read_csv(directory / "selected_orders.csv")
    assert len(orders.loc[orders.setup_id.eq(setup_id)]) == 1
    ledger = pd.read_csv(directory / "portfolio_trades.csv")
    selected = ledger.loc[ledger.setup_id.eq(setup_id)]
    assert len(selected) == 1
    row = selected.iloc[0]
    assert row.native_target_pct == 1.5 and row.native_stop_pct == 1.25
    assert row.filled and row.portfolio_executed and row.exit_reason == "TARGET"
    assert row.gross_return_pct == pytest.approx(1.5)
    assert row.net_return_pct == pytest.approx(1.45)
    assert pd.Timestamp(row.confirmation_ts) == confirmation
    assert pd.Timestamp(row.entry_ts) > confirmation
    provenance = json.loads((directory / "source_manifest.json").read_text())
    assert provenance["source_kind"] == "IMMUTABLE_RAW_MARKET_SNAPSHOT"
    assert provenance["completed_trade_outputs_consumed"] is False


def test_opening_short_missing_first_minute_oi_blocks_daily_publication(raw, tmp_path):
    roots, _ = _configure_slot(raw, "09:20", "SHORT")
    source.opening_inputs.minute_path(roots.opening_observer, DAY, "EXAMPLE30JANFUT").unlink()
    directory = tmp_path / "missing-opening"
    result = replay.replay_day(DAY, directory, roots=roots)
    assert not result["complete"] and result["state"] == "BLOCKED_INCOMPLETE_DATA"
    assert "portfolio_trades" not in result["artifacts"]
    assert any(item["reason"] == "NO_VALID_0920_OPENING_INPUTS"
               for item in result["coverage"]["blocking_problems"])
    exclusions = result["coverage"]["opening_0920_exclusions"]
    assert any("0921_SHORT" in item["setup_ids"] for item in exclusions)


def test_opening_short_positive_nifty_rejects_otherwise_valid_raw_candidate(raw, tmp_path):
    roots, _ = _configure_slot(raw, "09:20", "SHORT")
    path = roots.futures_5m / "NIFTY30JANFUT_5minute.parquet"
    nifty = pd.read_parquet(path)
    mask = nifty.timestamp.eq(pd.Timestamp(f"{DAY} 09:20", tz="Asia/Kolkata"))
    nifty.loc[mask, "close"] = 100.1
    nifty.to_parquet(path, index=False)
    directory = tmp_path / "nifty-rejected"
    result = replay.replay_day(DAY, directory, roots=roots)
    assert result["complete"], result["coverage"]
    features = pd.read_csv(directory / "feature_ledger.csv")
    opening = features.loc[features.hhmm_int.eq(920)].iloc[0]
    assert opening.opening_0920_status == "VALID"
    assert opening.price_change_pct < -.13 and opening.oi_change_pct >= .05
    assert opening.nifty_first_bar_return_pct > -.05
    orders = pd.read_csv(directory / "selected_orders.csv")
    assert not orders.setup_id.eq("0921_SHORT").any()
