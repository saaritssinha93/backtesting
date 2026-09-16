"""Create a non-official V13-v5 intraday mark-to-market snapshot."""

from __future__ import annotations

import argparse
import sys
from datetime import date
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import fno_v13_corrected_v5_backtest as v13


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--day", required=True)
    parser.add_argument("--cutoff", required=True)
    args = parser.parse_args()

    session_day = date.fromisoformat(args.day)
    signals, _, _, _, _, _, eligibility = v13.load_market(
        session_day,
        rebuild_cache=True,
        refresh_eligibility=True,
    )
    eligible = eligibility.loc[
        eligibility["day"].astype(str).eq(args.day),
        ["day", "coverage", "eligible", "v13_v5_eligibility_reason"],
    ]
    if eligible.empty or not bool(eligible.iloc[-1]["eligible"]):
        raise RuntimeError(f"Session is not V13-v5 eligible: {eligible.to_dict('records')}")

    profile = v13.PROFILES["higher_frequency"]
    orders = v13.select_orders(signals, v13.profile_setups(profile))
    orders = orders.loc[orders["day"].astype(str).eq(args.day)].copy()
    if orders.empty:
        print("[SNAPSHOT] no selected V13-v5 orders")
        return

    paths, _ = v13.materialize_raw_paths(orders, cutoff=args.cutoff)
    audit = v13.simulate_scaleout(orders, paths, profile.exit, cost_bps=5.0)
    audit = v13.apply_fixed_capital_model(audit, 100_000.0, 5.0)
    audit["snapshot_cutoff"] = args.cutoff
    audit["snapshot_status"] = audit["exit_reason"].astype(str).map(
        lambda value: "OPEN_MTM" if "TIME_EXIT" in value else "CLOSED"
    )
    audit.loc[audit["snapshot_status"].eq("OPEN_MTM"), "exit_reason"] = (
        "OPEN_MTM_AT_" + args.cutoff
    )

    output_root = (
        Path(r"C:\TradingData\eqidv2\fno_oi\strategy_research\v13_corrected_v5")
        / "daily_options"
        / args.day
    )
    output_root.mkdir(parents=True, exist_ok=True)
    output_path = output_root / f"v13_v5_intraday_snapshot_{args.cutoff}.csv"
    audit.to_csv(output_path, index=False)

    filled = audit.loc[audit["filled"].astype(bool)].copy()
    closed = filled.loc[filled["snapshot_status"].eq("CLOSED")]
    open_mtm = filled.loc[filled["snapshot_status"].eq("OPEN_MTM")]
    print(
        f"[SNAPSHOT] selected={len(audit)} filled={len(filled)} "
        f"closed={len(closed)} open={len(open_mtm)}"
    )
    print(
        f"[SNAPSHOT] realized_net_rs={closed['net_profit_rupees'].sum():.2f} "
        f"open_mtm_net_rs={open_mtm['net_profit_rupees'].sum():.2f} "
        f"combined_net_rs={filled['net_profit_rupees'].sum():.2f}"
    )
    print(
        filled[
            [
                "tradingsymbol",
                "side",
                "entry_ts",
                "entry_price",
                "exit_ts",
                "exit_price",
                "snapshot_status",
                "exit_reason",
                "net_return_pct",
                "net_profit_rupees",
            ]
        ].to_string(index=False)
    )
    print(f"[OUTPUT] {output_path}")


if __name__ == "__main__":
    main()
