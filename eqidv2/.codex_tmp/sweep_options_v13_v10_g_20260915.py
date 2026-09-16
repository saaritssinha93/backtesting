from datetime import date
from pathlib import Path
import sys

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import fno_oi_common as common
import fno_v13_v10_g_options_paper as paper
from fno_v13_v10_g_options_one_lot_execution import simulate_one_lot_trade


day = date(2026, 9, 15)
master, master_path, master_sha = paper.load_exact_options_master(day)
prepared = []
for equity in paper.load_equity_entries(day, "", "PAPER"):
    mapping = paper.map_equity_entry(equity, master, master_path, master_sha)
    candles, _ = paper.option_data.load_contract_five_minutes(
        mapping["option_symbol"],
        [(common.FNO_ROOT / "raw_options_1m", 1), (common.FNO_ROOT / "raw_options_5m", 5)],
    )
    candles = candles.loc[common._to_ist(candles["timestamp"]).dt.date.eq(day)].copy()
    prepared.append((equity, mapping, candles))

rows = []
trade_rows = []
for stop_points in range(2, 51):
    for target_points in range(1, 101):
        total = 0.0
        wins = 0
        closed = 0
        outcomes = []
        for equity, mapping, candles in prepared:
            result, _ = simulate_one_lot_trade(
                {
                    "trade_id": f"OPT_{equity['signal_id']}",
                    "day": day.isoformat(),
                    "entry_ts": paper._ist(equity["_equity_entry_ts"]).ceil("5min"),
                    "lot_size": mapping["lot_size"],
                    "tick_size": mapping["tick_size"],
                    "check_previous_volume": True,
                },
                candles,
                stop_points / 100.0,
                target_points / 100.0,
                slippage_bps=paper.SLIPPAGE_BPS,
                participation=paper.PREVIOUS_BAR_PARTICIPATION,
            )
            pnl = float(result["net_pnl"]) if result["status"] == "CLOSED" else float("nan")
            if result["status"] == "CLOSED":
                total += pnl
                closed += 1
                wins += pnl > 0
            outcomes.append((equity["_equity_symbol"], result["reason"], pnl))
        rows.append({
            "stop_pct": stop_points,
            "target_pct": target_points,
            "net_pnl_rs": total,
            "closed": closed,
            "wins": wins,
            "losses": closed - wins,
        })
        for symbol, reason, pnl in outcomes:
            trade_rows.append({
                "stop_pct": stop_points,
                "target_pct": target_points,
                "equity_symbol": symbol,
                "exit_reason": reason,
                "net_pnl_rs": pnl,
            })

frame = pd.DataFrame(rows).sort_values(
    ["net_pnl_rs", "wins", "stop_pct", "target_pct"],
    ascending=[False, False, True, True],
    kind="stable",
).reset_index(drop=True)
output = common.FNO_ROOT / "v13_v10_g_options_paper" / "research" / day.isoformat()
output.mkdir(parents=True, exist_ok=True)
frame.to_csv(output / "sl_target_sweep_1pct_grid.csv", index=False)
pd.DataFrame(trade_rows).to_csv(output / "sl_target_sweep_trade_details.csv", index=False)
print(frame.head(30).to_string(index=False))
print(output)
