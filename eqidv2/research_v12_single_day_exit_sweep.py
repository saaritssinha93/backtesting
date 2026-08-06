"""Research-only per-setup exit sweep for resolved V12 entry signals."""

from __future__ import annotations

import argparse
import os
from pathlib import Path

import numpy as np
import pandas as pd


def _values(start: float, stop: float, step: float) -> list[float]:
    count = int(round((stop - start) / step))
    return [round(start + index * step, 4) for index in range(count + 1)]


def main() -> int:
    parser = argparse.ArgumentParser(description="V12 single-day overfit exit sweep")
    parser.add_argument("--signals", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--setup-conf-module", default="final_setup_conf_v12")
    parser.add_argument("--sl-min", type=float, default=0.10)
    parser.add_argument("--sl-max", type=float, default=3.00)
    parser.add_argument("--target-min", type=float, default=0.10)
    parser.add_argument("--target-max", type=float, default=3.00)
    parser.add_argument("--step", type=float, default=0.05)
    args = parser.parse_args()

    output_text = str(args.output.resolve()).lower()
    if "experiment" not in output_text and "research" not in output_text:
        raise SystemExit("output path must be experimental/research")
    os.environ["EQIDV2_V12_FINAL_SETUP_CONF_MODULE"] = args.setup_conf_module
    import avwap_5min_ID_v12_backtesting as v12

    v12._V11_COST_MODEL = "statutory"
    v12._V11_SLIPPAGE_BPS = 0.0
    v12._activate_final_setup_conf()
    signals = pd.read_csv(args.signals)
    if signals.empty:
        raise SystemExit("signals file is empty")

    rows: list[dict[str, object]] = []
    for setup, setup_signals in signals.groupby("setup", sort=True):
        for sl_pct in _values(args.sl_min, args.sl_max, args.step):
            for target_pct in _values(args.target_min, args.target_max, args.step):
                v12.v6.SETUP_EXIT_RULES[str(setup)] = (sl_pct, target_pct)
                resolved: list[dict[str, object]] = []
                for _, source_row in setup_signals.iterrows():
                    candidate = source_row.copy()
                    side = str(candidate.get("side", "")).upper()
                    entry = float(candidate["v7_signal_entry_price"])
                    if side == "LONG":
                        stop = round(entry * (1.0 - sl_pct / 100.0), 2)
                        target = round(entry * (1.0 + target_pct / 100.0), 2)
                    else:
                        stop = round(entry * (1.0 + sl_pct / 100.0), 2)
                        target = round(entry * (1.0 - target_pct / 100.0), 2)
                    candidate["v7_signal_stop_price"] = stop
                    candidate["v7_signal_target_price"] = target
                    candidate["v7_signal_sl_pct"] = sl_pct
                    candidate["v7_signal_target_pct"] = target_pct
                    candidate["quantity"] = v12._risk_based_qty(entry, stop)
                    record = v12._resolve_v7_entry_engine_signal(
                        candidate,
                        label="v12_single_day_exit_sweep",
                        entry_fill_model="ltp_on_signal_1min_open",
                        selected_strategy_profile="final_setup_conf",
                    )
                    if record is not None:
                        resolved.append(record)
                net = float(sum(float(record["v6_net_pnl_rs"]) for record in resolved))
                gross = float(sum(float(record["v6_gross_pnl_rs"]) for record in resolved))
                costs = float(sum(float(record["v6_cost_rs"]) for record in resolved))
                outcomes = "|".join(str(record["v6_outcome"]) for record in resolved)
                exits = "|".join(
                    f"{record['ticker']}@{record['v6_exit_time_ist']}:{float(record['v6_exit_price']):.4f}"
                    for record in resolved
                )
                rows.append(
                    {
                        "setup": setup,
                        "sl_pct": sl_pct,
                        "target_pct": target_pct,
                        "trades": len(resolved),
                        "wins": int(sum(float(record["v6_net_pnl_rs"]) > 0.0 for record in resolved)),
                        "gross_pnl_rs": gross,
                        "cost_rs": costs,
                        "net_pnl_rs": net,
                        "outcomes": outcomes,
                        "exits": exits,
                    }
                )

    result = pd.DataFrame(rows).sort_values(
        ["setup", "net_pnl_rs", "wins", "target_pct", "sl_pct"],
        ascending=[True, False, False, True, True],
        kind="mergesort",
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    result.to_csv(args.output, index=False)
    best = result.groupby("setup", sort=True, as_index=False).head(1)
    print(best.to_string(index=False))
    print(f"combined_best_net_pnl_rs={best['net_pnl_rs'].sum():.4f}")
    print(f"output={args.output.resolve()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
