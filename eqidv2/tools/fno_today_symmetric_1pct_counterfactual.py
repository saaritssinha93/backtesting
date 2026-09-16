"""Today-only 1% stop / 1% target counterfactual for FnO strategies.

This research utility intentionally does not import or alter the production
strategy files.  It keeps the published selections, fills, costs and source
snapshots, replacing only the post-entry exit bracket with 1% stop / 1%
target.  It writes a standalone report under strategy_research.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd


DAY = "2026-09-03"
RUNTIME = Path(r"C:\TradingData\eqidv2\fno_oi\strategy_research")
V6_AUDIT = RUNTIME / "v6_corrected" / "fno_v6_corrected_trades.csv"
V13_AUDIT = RUNTIME / "v13_corrected_v2" / "fno_v13_corrected_v2_trades.csv"
V6_CACHE = RUNTIME / "v6_corrected" / "_cache"
DAILY_RUN = (
    RUNTIME
    / "daily_fno_comparison_v1"
    / "runs"
    / "fno_2026-09-03_20260903T162026000358+0530"
    / "strategies"
)
SNAPSHOT_1M = (
    RUNTIME
    / "daily_fno_comparison_v1"
    / "source_snapshots"
    / "2026-09-03"
    / "snapshot_20260903T162002717075+0530_7l34_95j"
    / "equity_1m"
)
OUTPUT_DIR = RUNTIME / "symmetric_1pct_exit_counterfactual"
OUTPUT_CSV = OUTPUT_DIR / "fno_2026-09-03_symmetric_1pct_trades.csv"
OUTPUT_REPORT = OUTPUT_DIR / "fno_2026-09-03_symmetric_1pct_report.md"


@dataclass(frozen=True)
class V6PathResult:
    exit_reason: str
    exit_price: float
    net_return_pct: float


def _round_down(value: float, tick: float) -> float:
    return float(np.floor((value + 1e-12) / tick) * tick)


def _round_up(value: float, tick: float) -> float:
    return float(np.ceil((value - 1e-12) / tick) * tick)


def _load_v6_paths(audit: pd.DataFrame) -> dict[int, dict[str, np.ndarray]]:
    """Return paths keyed by published audit SID.

    The corrected audit rebases SID values while concatenating the 26AUG and
    26SEP contract regimes.  A per-regime cache retains local SIDs, so match
    the selected row by its stable day/symbol/side/slot/trigger identity.
    """
    wanted = audit.copy()
    wanted["day_text"] = pd.to_datetime(wanted["day"]).dt.strftime("%Y-%m-%d")
    matches: list[tuple[int, Path, Path, pd.DataFrame]] = []
    for parquet in V6_CACHE.glob("26SEP_*.parquet"):
        npz = parquet.with_suffix(".npz")
        if not npz.exists():
            continue
        probe = pd.read_parquet(
            parquet, columns=["day", "sid", "tradingsymbol", "side", "hhmm_int", "trigger"]
        )
        probe["day_text"] = pd.to_datetime(probe["day"]).dt.strftime("%Y-%m-%d")
        merged = wanted.merge(
            probe,
            on=["day_text", "tradingsymbol", "side", "hhmm_int", "trigger"],
            how="left",
            suffixes=("_audit", "_cache"),
        )
        if merged["sid_cache"].notna().all() and not merged["sid_cache"].duplicated().any():
            matches.append((parquet.stat().st_mtime_ns, parquet, npz, merged))
    if not matches:
        raise RuntimeError("No V6 cache matches every published selected trade.")
    _, parquet, npz, mapping = max(matches, key=lambda item: item[0])
    blob = np.load(npz)
    paths: dict[int, dict[str, np.ndarray]] = {}
    for row in mapping.to_dict("records"):
        audit_sid, cache_sid = int(row["sid_audit"]), int(row["sid_cache"])
        suffix = {"high": "h", "low": "l", "close": "c"}
        paths[audit_sid] = {
            field: blob[f"{cache_sid}_{suffix[field]}"]
            for field in ("high", "low", "close")
        }
    return paths


def _simulate_v6_path(
    *, side: str, trigger: float, path: dict[str, np.ndarray], cost_bps: float,
    stop_pct: float = 1.0, target_pct: float = 1.0,
) -> V6PathResult:
    high = path["high"]
    low = path["low"]
    close = path["close"]
    long_side = side == "LONG"
    touches = np.flatnonzero(high >= trigger) if long_side else np.flatnonzero(low <= trigger)
    if not touches.size:
        raise RuntimeError(f"Published V6 fill disappeared: {side=} {trigger=}")
    entry_index = int(touches[0])
    if long_side:
        stop = trigger * (1.0 - stop_pct / 100.0)
        target = trigger * (1.0 + target_pct / 100.0)
        stop_hits = np.flatnonzero(low[entry_index:] <= stop)
        target_hits = np.flatnonzero(high[entry_index:] >= target)
    else:
        stop = trigger * (1.0 + stop_pct / 100.0)
        target = trigger * (1.0 - target_pct / 100.0)
        stop_hits = np.flatnonzero(high[entry_index:] >= stop)
        target_hits = np.flatnonzero(low[entry_index:] <= target)
    never = np.iinfo(np.int32).max
    stop_index = int(stop_hits[0]) if stop_hits.size else never
    target_index = int(target_hits[0]) if target_hits.size else never
    if stop_index == never and target_index == never:
        exit_reason, exit_price = "SQUARE_OFF_LAST_AVAILABLE_BAR", float(close[-1])
    elif stop_index <= target_index:
        exit_reason, exit_price = "STOP", float(stop)
    else:
        exit_reason, exit_price = "TARGET", float(target)
    gross = exit_price / trigger - 1.0 if long_side else 1.0 - exit_price / trigger
    return V6PathResult(exit_reason, exit_price, (gross - cost_bps / 10_000.0) * 100.0)


def _v6_rows(label: str, audit_path: Path, cost_bps: float) -> list[dict[str, object]]:
    audit = pd.read_csv(audit_path)
    day_rows = audit.loc[pd.to_datetime(audit["day"]).dt.strftime("%Y-%m-%d").eq(DAY)].copy()
    day_rows = day_rows.loc[day_rows["filled"].astype(str).str.lower().eq("true")]
    paths = _load_v6_paths(day_rows)
    rows: list[dict[str, object]] = []
    for row in day_rows.to_dict("records"):
        published = _simulate_v6_path(
            side=str(row["side"]), trigger=float(row["trigger"]),
            path=paths[int(row["sid"])], cost_bps=cost_bps,
            stop_pct=float(row["stop_pct"]), target_pct=float(row["target_pct"]),
        )
        if not np.isclose(published.net_return_pct, float(row["net_return_pct"]), atol=1e-10):
            raise AssertionError(
                f"{label} published-path reproduction failed for {row['tradingsymbol']}: "
                f"{published.net_return_pct} != {row['net_return_pct']}"
            )
        result = _simulate_v6_path(
            side=str(row["side"]), trigger=float(row["trigger"]),
            path=paths[int(row["sid"])], cost_bps=cost_bps,
        )
        rows.append({
            "strategy": label,
            "symbol": str(row["tradingsymbol"]),
            "side": str(row["side"]),
            "entry_time": str(row.get("confirmation_ts", "")),
            "entry_price": float(row["trigger"]),
            "tick_size": np.nan,
            "cost_bps": cost_bps,
            "stop_price_1pct": float(row["trigger"]) * (0.99 if row["side"] == "LONG" else 1.01),
            "target_price_1pct": float(row["trigger"]) * (1.01 if row["side"] == "LONG" else 0.99),
            "exit_price_1pct": result.exit_price,
            "exit_reason_1pct": result.exit_reason,
            "net_return_pct_1pct": result.net_return_pct,
            "quantity": np.nan,
            "net_pnl_rs_1pct": np.nan,
            "data_end": "last cached V6 forward bar",
        })
    return rows


def _normalise_ist(values: pd.Series) -> pd.Series:
    stamps = pd.to_datetime(values, errors="coerce")
    if getattr(stamps.dt, "tz", None) is None:
        return stamps.dt.tz_localize("Asia/Kolkata")
    return stamps.dt.tz_convert("Asia/Kolkata")


def _simulate_v10_v12_trade(
    row: dict[str, object], cost_bps: float,
    bracket_override: tuple[float, float] | None = None,
) -> dict[str, object]:
    symbol = str(row["symbol"])
    side = str(row["side"])
    entry = float(row["entry_price"])
    tick = float(row["tick_size"])
    quantity = int(float(row["quantity"]))
    entry_time = pd.Timestamp(row["entry_time"])
    if entry_time.tzinfo is None:
        entry_time = entry_time.tz_localize("Asia/Kolkata")
    else:
        entry_time = entry_time.tz_convert("Asia/Kolkata")
    path = SNAPSHOT_1M / f"{symbol}_stocks_indicators_1min.parquet"
    bars = pd.read_parquet(path, columns=["date", "open", "high", "low", "close"])
    bars["ts"] = _normalise_ist(bars["date"])
    bars = bars.loc[bars["ts"].dt.strftime("%Y-%m-%d").eq(DAY) & bars["ts"].ge(entry_time)].sort_values("ts")
    if bars.empty:
        raise RuntimeError(f"No snapshot bars after entry for {symbol}")

    if bracket_override is not None:
        stop, target = bracket_override
    elif side == "LONG":
        stop = _round_down(entry * 0.99, tick)
        target = _round_down(entry * 1.01, tick)
    else:
        stop = _round_up(entry * 1.01, tick)
        target = _round_up(entry * 0.99, tick)

    exit_reason = "SQUARE_OFF_LAST_AVAILABLE_BAR"
    exit_price = float(bars.iloc[-1]["close"])
    exit_time = bars.iloc[-1]["ts"]
    for item in bars.itertuples(index=False):
        opens_before_position = item.ts != entry_time
        if side == "LONG":
            if opens_before_position and float(item.open) <= stop:
                exit_reason, exit_price = "STOP_GAP", _round_down(float(item.open), tick)
            elif opens_before_position and float(item.open) >= target:
                exit_reason, exit_price = "TARGET", target
            elif float(item.low) <= stop:
                exit_reason, exit_price = "STOP", stop
            elif float(item.high) >= target:
                exit_reason, exit_price = "TARGET", target
            else:
                continue
        else:
            if opens_before_position and float(item.open) >= stop:
                exit_reason, exit_price = "STOP_GAP", _round_up(float(item.open), tick)
            elif opens_before_position and float(item.open) <= target:
                exit_reason, exit_price = "TARGET", target
            elif float(item.high) >= stop:
                exit_reason, exit_price = "STOP", stop
            elif float(item.low) <= target:
                exit_reason, exit_price = "TARGET", target
            else:
                continue
        exit_time = item.ts
        break
    gross_return = (exit_price / entry - 1.0) * 100.0 if side == "LONG" else (1.0 - exit_price / entry) * 100.0
    net_return = gross_return - cost_bps / 100.0
    pnl = (exit_price - entry) * quantity if side == "LONG" else (entry - exit_price) * quantity
    pnl -= entry * quantity * cost_bps / 10_000.0
    return {
        "symbol": symbol,
        "side": side,
        "entry_time": str(entry_time),
        "entry_price": entry,
        "tick_size": tick,
        "cost_bps": cost_bps,
        "stop_price_1pct": stop,
        "target_price_1pct": target,
        "exit_price_1pct": exit_price,
        "exit_reason_1pct": exit_reason,
        "exit_time_1pct": str(exit_time),
        "net_return_pct_1pct": net_return,
        "quantity": quantity,
        "net_pnl_rs_1pct": pnl,
        "data_end": str(bars.iloc[-1]["ts"]),
    }


def _v10_v12_rows(label: str, folder: str, cost_bps: float) -> list[dict[str, object]]:
    trades = pd.read_csv(DAILY_RUN / folder / "closed_trades.csv")
    trades = trades.loc[trades["filled"].astype(str).str.lower().eq("true")]
    rows: list[dict[str, object]] = []
    for trade in trades.to_dict("records"):
        published = _simulate_v10_v12_trade(
            trade,
            cost_bps,
            bracket_override=(float(trade["stop_price"]), float(trade["target_price"])),
        )
        expected_reason = str(trade["exit_reason"])
        expected_net = float(trade["net_return_pct"])
        if (
            published["exit_reason_1pct"] != expected_reason
            or not np.isclose(float(published["net_return_pct_1pct"]), expected_net, atol=1e-10)
        ):
            raise AssertionError(
                f"{label} published-path reproduction failed for {trade['symbol']}: "
                f"{published['exit_reason_1pct']} / {published['net_return_pct_1pct']} "
                f"!= {expected_reason} / {expected_net}"
            )
        rows.append({"strategy": label, **_simulate_v10_v12_trade(trade, cost_bps)})
    return rows


def _summary(frame: pd.DataFrame) -> pd.DataFrame:
    items: list[dict[str, object]] = []
    for strategy, trades in frame.groupby("strategy", sort=False):
        values = trades["net_return_pct_1pct"].to_numpy(float)
        gain = float(values[values > 0].sum())
        loss = float(-values[values < 0].sum())
        pnl = pd.to_numeric(trades["net_pnl_rs_1pct"], errors="coerce")
        items.append({
            "strategy": strategy,
            "cost_bps": float(trades["cost_bps"].iloc[0]),
            "fills": len(trades),
            "wins": int((values > 0).sum()),
            "losses": int((values < 0).sum()),
            "profit_factor": gain / loss if loss else (float("inf") if gain else np.nan),
            "net_return_sum_pct": float(values.sum()),
            "net_pnl_rs": float(pnl.sum()) if pnl.notna().any() else np.nan,
        })
    return pd.DataFrame(items)


def main() -> int:
    rows = []
    rows += _v6_rows("V6_CORRECTED", V6_AUDIT, 5.0)
    rows += _v10_v12_rows("V10", "v10_stage7_0935_long_max_050_gap2", 15.0)
    rows += _v10_v12_rows("V12", "v12_selected", 15.0)
    rows += _v6_rows("V13_V2", V13_AUDIT, 5.0)
    trades = pd.DataFrame(rows)
    summary = _summary(trades)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    trades.to_csv(OUTPUT_CSV, index=False)
    report = [
        f"# FnO {DAY} symmetric 1% stop / 1% target counterfactual",
        "",
        "Same published selections, entries, source snapshots and per-strategy costs; only the post-entry bracket was changed to 1% stop and 1% target.",
        "",
        "## Summary",
        "",
        summary.to_markdown(index=False, floatfmt=".6f"),
        "",
        "## Trades",
        "",
        trades[[
            "strategy", "symbol", "side", "entry_time", "entry_price", "stop_price_1pct", "target_price_1pct", "exit_reason_1pct", "exit_price_1pct", "net_return_pct_1pct", "net_pnl_rs_1pct", "data_end",
        ]].to_markdown(index=False, floatfmt=".6f"),
        "",
        "## Limits",
        "",
        "V6/V13 returns are percentage-only, as their native corrected backtests are not lot-sized. V10/V12 use the published 15 bps cost and their original quantities. V10/V12 source bars end at 15:15, so any LAST_AVAILABLE_BAR result is a 15:15 sensitivity mark rather than a verified 15:30 square-off.",
    ]
    OUTPUT_REPORT.write_text("\n".join(report), encoding="utf-8")
    print(summary.to_string(index=False))
    print(f"TRADES={OUTPUT_CSV}")
    print(f"REPORT={OUTPUT_REPORT}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
