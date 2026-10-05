"""Build an aligned daywise comparison for three V13-v10-G-2 fixed stops."""
from __future__ import annotations

from pathlib import Path
import sys

import pandas as pd


ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(Path(__file__).resolve().parent))

import fno_v13_v10_g_2_backtest as g2
import fno_v13_v10_g_2_extend_results as ext
import v13_g2_sl_sweep as sweep


OUTPUT = Path(__file__).with_name(
    "v13_v10_g2_daywise_sl_1p00_1p25_2p75_through_2026-09-30.csv"
)
STOPS = (("sl_1p00", 1.00), ("sl_1p25", 1.25), ("sl_2p75", 2.75))
MONEY_COLUMNS = (
    "gross_rupees",
    "cost_rupees",
    "net_rupees",
    "cumulative_net_rupees",
    "daily_close_drawdown_rupees",
)


def ledger_for_stop(published, segments, stop_pct: float) -> pd.DataFrame:
    trade_frames = [
        ext._simulate(orders, paths, published["base"], stop_pct=stop_pct)[0]
        for _, orders, paths in segments
    ]
    all_trades = pd.concat(trade_frames, ignore_index=True, sort=False)
    ledger, _ = g2.g.v9.v6.apply_portfolio_constraints(
        all_trades, published["base"].portfolio_config()
    )
    return ledger


def daywise(ledger: pd.DataFrame, days: list, prefix: str) -> pd.DataFrame:
    calendar = pd.Index([str(day) for day in days], name="date")
    work = ledger.copy()
    work["date"] = pd.to_datetime(work["day"]).dt.strftime("%Y-%m-%d")
    work["portfolio_executed"] = work["portfolio_executed"].eq(True)

    selected = work.groupby("date").size().reindex(calendar, fill_value=0)
    executed = work.loc[work["portfolio_executed"]].copy()
    pnl = pd.to_numeric(
        executed["portfolio_net_profit_rupees"], errors="coerce"
    ).fillna(0.0)
    executed["_win"] = pnl.gt(1e-9).astype(int)
    executed["_loss"] = pnl.lt(-1e-9).astype(int)
    grouped = executed.groupby("date").agg(
        trades=("portfolio_executed", "size"),
        wins=("_win", "sum"),
        losses=("_loss", "sum"),
        gross_rupees=("portfolio_gross_profit_rupees", "sum"),
        cost_rupees=("portfolio_cost_rupees", "sum"),
        net_rupees=("portfolio_net_profit_rupees", "sum"),
    )
    grouped = grouped.reindex(calendar, fill_value=0)
    grouped.insert(0, "selected", selected.astype(int))
    for column in ("trades", "wins", "losses"):
        grouped[column] = grouped[column].astype(int)

    grouped["cumulative_net_rupees"] = grouped["net_rupees"].cumsum()
    peaks = grouped["cumulative_net_rupees"].cummax().clip(lower=0.0)
    grouped["daily_close_drawdown_rupees"] = (
        peaks - grouped["cumulative_net_rupees"]
    )
    for column in MONEY_COLUMNS:
        # Preserve sub-paise replay precision in the interchange CSV. The
        # presentation layer can format these monetary values to two decimals.
        grouped[column] = grouped[column].astype(float).round(6)
    return grouped.add_prefix(f"{prefix}_").reset_index()


def main() -> None:
    published, segments, days = sweep.prepared_segments()
    result = pd.DataFrame({"date": [str(day) for day in days]})
    for prefix, stop_pct in STOPS:
        current = daywise(ledger_for_stop(published, segments, stop_pct), days, prefix)
        result = result.merge(current, on="date", how="left", validate="one_to_one")

    for prefix, _ in STOPS[1:]:
        result[f"{prefix}_daily_net_delta_vs_1p00_rupees"] = (
            result[f"{prefix}_net_rupees"] - result["sl_1p00_net_rupees"]
        ).round(6)
        result[f"{prefix}_cumulative_net_delta_vs_1p00_rupees"] = (
            result[f"{prefix}_cumulative_net_rupees"]
            - result["sl_1p00_cumulative_net_rupees"]
        ).round(6)

    if len(result) != 43 or result["date"].duplicated().any():
        raise RuntimeError("Expected exactly 43 unique sessions")

    expected_totals = {
        "sl_1p00": (94, 85, 57, 28, 241909.01, 21250.00, 220659.01),
        "sl_1p25": (94, 85, 59, 26, 244893.92, 21250.00, 223643.92),
        "sl_2p75": (94, 85, 60, 25, 265262.72, 21250.00, 244012.72),
    }
    for prefix, expected in expected_totals.items():
        actual = (
            int(result[f"{prefix}_selected"].sum()),
            int(result[f"{prefix}_trades"].sum()),
            int(result[f"{prefix}_wins"].sum()),
            int(result[f"{prefix}_losses"].sum()),
            round(
                float(result[f"{prefix}_cumulative_net_rupees"].iloc[-1])
                + float(result[f"{prefix}_cost_rupees"].sum()),
                2,
            ),
            round(float(result[f"{prefix}_cost_rupees"].sum()), 2),
            round(float(result[f"{prefix}_cumulative_net_rupees"].iloc[-1]), 2),
        )
        if actual != expected:
            raise RuntimeError(f"Unexpected {prefix} totals: {actual!r} != {expected!r}")

    result.to_csv(OUTPUT, index=False, float_format="%.6f")
    print(OUTPUT.resolve())
    print(result.to_string(index=False))


if __name__ == "__main__":
    main()
