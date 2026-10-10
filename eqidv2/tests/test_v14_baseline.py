"""Frozen G-3 parity and the explicit OI-free selection boundary."""
from __future__ import annotations

import ast
import hashlib
import json
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from v14 import baseline

ARCHIVE = Path(r"C:/TradingData/eqidv2/fno_oi/strategy_research/v13_corrected_v10_g_3/frozen_20261008_long110_nextminute_v1")


def _candidate(symbol="CASH", sid=0, side="LONG", signal="2026-10-08 09:30", **changes):
    stamp = pd.Timestamp(signal, tz=baseline.IST)
    row = dict(sid=sid, day=stamp.date(), tradingsymbol=symbol, signal_ts=stamp,
               confirmation_ts=stamp + pd.Timedelta(minutes=1), side=side,
               hhmm_int=int(stamp.strftime("%H%M")), price_change_pct=.8 if side == "LONG" else -.8,
               volume_ratio=3., body_ratio=.8, wick_ratio=.1, traded_value=1e7,
               v9_1m_volume_ratio=1.1 if side == "LONG" else 1.2, trigger=100.)
    return {**row, **changes}


def _path(confirmation, **overrides):
    stamp = pd.Timestamp(confirmation)
    times = pd.date_range(stamp + pd.Timedelta(minutes=1), stamp.normalize() + pd.Timedelta(hours=15, minutes=15), freq="min")
    result = {"timestamp_ns": times.asi8, "open": np.full(len(times), 100.),
              "high": np.full(len(times), 100.1), "low": np.full(len(times), 99.9), "close": np.full(len(times), 100.)}
    for name, value in overrides.items():
        result[name] = np.full(len(times), value)
    return result


def test_copied_snapshot_and_exact_function_extraction():
    manifest = baseline.verify_snapshot()
    target = (baseline.SNAPSHOT / "execution.py").read_text(encoding="utf-8")
    nodes = {n.name: n for n in ast.parse(target).body if isinstance(n, (ast.FunctionDef, ast.ClassDef))}
    lines = target.splitlines(keepends=True)
    for record in manifest["extracted_functions"]:
        node = nodes[record["name"]]
        start = min([node.lineno] + [d.lineno for d in node.decorator_list]) - 1
        snippet = "".join(lines[start:node.end_lineno])
        assert hashlib.sha256(snippet.encode()).hexdigest() == record["sha256"]
    assert len(baseline.setup_rules()) == 14
    assert baseline.frozen_config()["minimum_confirmation_1m_volume_ratio"] == {"LONG": 1.1, "SHORT": 1.2}


def test_cash_only_callback_and_native_core_reservation():
    # The expanded candidate has more cash liquidity; the original core keeps
    # priority at this one-entry short setup exactly as it did in frozen G-3.
    rows = pd.DataFrame([
        _candidate("EXPANDED", 0, "SHORT", "2026-10-08 09:50", price_change_pct=-.15, traded_value=9e7),
        _candidate("CORE", 1, "SHORT", "2026-10-08 09:50", price_change_pct=-.25, traded_value=1e7),
    ])
    selected = baseline.select_orders(rows, lambda r, setup: pd.Series(True, index=r.index))
    assert selected.tradingsymbol.tolist() == ["CORE"]
    assert not any("oi" in column.lower() for column in selected.columns)
    assert baseline.select_orders(rows, lambda r, setup: r.tradingsymbol.ne("CORE")).tradingsymbol.tolist() == ["EXPANDED"]


def test_confirmation_threshold_and_exact_clock():
    rows = pd.DataFrame([_candidate()])
    assert len(baseline.select_orders(rows)) == 1
    rows["v9_1m_volume_ratio"] = 1.099
    assert baseline.select_orders(rows).empty
    rows["confirmation_ts"] += pd.Timedelta(minutes=1)
    with pytest.raises(ValueError, match="exact next-minute"):
        baseline.select_orders(rows)


def test_expiry_all_unfilled_and_same_bar_stop_first():
    orders = baseline.select_orders(pd.DataFrame([_candidate()]))
    sid = int(orders.sid.iloc[0])
    path = _path(orders.confirmation_ts.iloc[0], open=99., high=99.5, low=98.9, close=99.)
    path["high"][10] = 100.1  # First trigger after the original ten-minute expiry.
    result = baseline.simulate(orders, {sid: path})
    assert not result.filled.any()
    assert result.portfolio_net_profit_rupees.sum() == 0
    path = _path(orders.confirmation_ts.iloc[0])
    path["high"][0], path["low"][0] = 104., 98.
    result = baseline.simulate(orders, {sid: path})
    assert result.exit_reason.iloc[0] == "STOP"
    assert result.exit_price.iloc[0] == 98.75
    assert result.cost_rupees.iloc[0] == 250.
    assert result.portfolio_net_profit_rupees.iloc[0] == pytest.approx(-6500.)


def test_tightened_stop_activates_only_at_later_bar_open_and_capital_limits():
    orders = baseline.select_orders(pd.DataFrame([_candidate()]))
    path = _path(orders.confirmation_ts.iloc[0])
    path["low"][120:122] = 98.9
    result = baseline.simulate(orders, {0: path})
    assert result.exit_reason.iloc[0] == "TIGHTENED_STOP"
    assert result.exit_path_index.iloc[0] == 121
    assert result.exit_price.iloc[0] == 99.
    batch = pd.concat([orders.assign(sid=i, tradingsymbol=f"CASH{i:02}") for i in range(11)], ignore_index=True)
    result = baseline.simulate(batch, {i: path for i in range(11)})
    assert result.portfolio_executed.sum() == 10
    assert result.portfolio_reject_reason.iloc[-1] == "INSUFFICIENT_PORTFOLIO_CAPITAL"


@pytest.mark.skipif(not ARCHIVE.exists(), reason="Local sealed G-3 archive unavailable")
def test_archived_historical_g3_selections_match_with_authentic_oi_gate():
    provenance = json.loads((ARCHIVE / "source_study_provenance.json").read_text())
    signals = pd.read_parquet(Path(provenance["source_bundle"]) / "dataset/signals.parquet")
    if "signal_ts" not in signals:
        signals["signal_ts"] = pd.to_datetime(signals.confirmation_ts) - pd.Timedelta(minutes=1)
    selected = baseline.select_orders(signals, lambda rows, setup: rows.oi_change_pct.ge(setup["oi_change_pct"]))
    expected = pd.read_csv(ARCHIVE / "trades.csv")
    expected = expected.loc[expected.day.isin(signals.day.astype(str).unique())]
    columns = ["sid", "setup_id", "tradingsymbol", "side"]
    assert sorted(map(tuple, selected[columns].to_numpy())) == sorted(map(tuple, expected[columns].to_numpy()))


@pytest.mark.skipif(not ARCHIVE.exists(), reason="Local sealed G-3 archive unavailable")
def test_all_93_archived_g3_executions_match_cash_replay():
    original = pd.read_csv(ARCHIVE / "trades.csv")
    original = original.loc[original.portfolio_executed.astype(str).str.lower().eq("true")].copy()
    assert len(original) == 93
    minute = pd.read_parquet(ARCHIVE / "minute_context.parquet")
    paths = {}
    for row in original.itertuples(index=False):
        context = minute.loc[minute.day.astype(str).eq(str(row.day)) & minute.tradingsymbol.eq(row.tradingsymbol)]
        paths[int(row.sid)] = baseline.native_paths(context, row.confirmation_ts)
    columns = ["sid", "day", "tradingsymbol", "side", "setup_id", "picker", "confirmation_ts",
               "trigger", "price_change_pct", "volume_ratio", "traded_value"]
    result = baseline.simulate(original[columns].reset_index(drop=True), paths)
    original = original.reset_index(drop=True)
    for column in ("entry_price", "exit_price", "holding_minutes", "net_return_pct", "portfolio_net_profit_rupees"):
        np.testing.assert_allclose(result[column], original[column], atol=1e-8, rtol=1e-10)
    for column in ("filled", "exit_reason", "portfolio_executed"):
        assert result[column].tolist() == original[column].tolist()
    for column in ("entry_ts", "exit_ts"):
        assert pd.to_datetime(result[column], utc=True).tolist() == pd.to_datetime(original[column], utc=True).tolist()
