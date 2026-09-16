"""Replay one V13-v5 day with the frozen V6 full-position exits."""

from pathlib import Path
import sys

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import fno_v13_corrected_v5_backtest as v13


DAY = "2026-09-07"
RESULT_ROOT = Path(r"C:\TradingData\eqidv2\fno_oi\strategy_research\v13_corrected_v5")
TRADES_PATH = (
    RESULT_ROOT
    / "higher_frequency"
    / "fno_v13_corrected_v5_higher_frequency_trades.csv"
)
OUTPUT_PATH = RESULT_ROOT / "research" / f"v13_v5_{DAY}_v6_exit_counterfactual.csv"


def replay(cutoff: str, orders: pd.DataFrame) -> pd.DataFrame:
    paths, _ = v13.materialize_raw_paths(orders, cutoff=cutoff)
    replayed = v13.simulate_native(
        orders,
        paths,
        cost_bps=5.0,
        max_entry_delay_minutes=v13.MAX_ENTRY_DELAY_MINUTES,
    )
    expected = pd.to_numeric(orders["entry_price"], errors="raise").to_numpy(float)
    observed = pd.to_numeric(replayed["entry_price"], errors="raise").to_numpy(float)
    if not np.allclose(expected, observed, rtol=0.0, atol=1e-8):
        raise AssertionError("Counterfactual did not preserve the V13-v5 entries")
    replayed = v13.apply_fixed_capital_model(replayed, 100_000.0, 5.0)
    replayed.insert(0, "counterfactual_cutoff", cutoff)
    return replayed


def main() -> None:
    source = pd.read_csv(TRADES_PATH)
    orders = source.loc[source["day"].astype(str).eq(DAY)].copy()
    if len(orders) != 7:
        raise AssertionError(f"Expected 7 V13-v5 trades for {DAY}; found {len(orders)}")

    frames = [replay("1515", orders)]
    try:
        frames.append(replay("1530", orders))
    except (RuntimeError, FileNotFoundError) as exc:
        print(f"[WARN] 15:30 comparison unavailable: {type(exc).__name__}: {exc}")

    combined = pd.concat(frames, ignore_index=True)
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    combined.to_csv(OUTPUT_PATH, index=False)

    for cutoff, group in combined.groupby("counterfactual_cutoff", sort=True):
        filled = group.loc[group["filled"].astype(bool)]
        wins = int((filled["net_profit_rupees"] > 0).sum())
        losses = int((filled["net_profit_rupees"] < 0).sum())
        print(
            f"[V6 exits @ {cutoff}] trades={len(filled)} wins={wins} losses={losses} "
            f"gross_rs={filled['pre_cost_profit_rupees'].sum():.2f} "
            f"cost_rs={filled['cost_rupees'].sum():.2f} "
            f"net_rs={filled['net_profit_rupees'].sum():.2f} "
            f"net_pct={filled['net_return_pct'].sum():.6f}"
        )
        print(
            filled[
                [
                    "tradingsymbol",
                    "side",
                    "native_stop_pct",
                    "native_target_pct",
                    "entry_price",
                    "exit_price",
                    "exit_ts",
                    "exit_reason",
                    "net_return_pct",
                    "net_profit_rupees",
                ]
            ].to_string(index=False)
        )
    print(f"[OUTPUT] {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
