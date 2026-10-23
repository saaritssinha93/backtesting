"""Inspect the seven verified 23 October 2026 daily backtest books."""
from pathlib import Path
import json

import numpy as np
import pandas as pd


ROOT = Path("C:/TradingData/eqidv2")
DAY = "2026-10-23"


def package(group, fragment):
    return ROOT / f"backtesting_result_v13_v10_g_{group}" / fragment


def load():
    p3 = package(3, "production/runs/2026-10-23/20261023T171521599663_b80e6def")
    p4 = package(4, "daily/runs/2026-10-23/20261023_compare_6b96653a_g4")
    p6 = package(6, "daily/runs/2026-10-23/20261023T183148480808_985a0e9e_g6_nifty_futures")
    p72 = package(7, "daily/runs/2026-10-23/20261023T183148514804_bbcb21e1_g7_nifty_futures")
    p73 = package(7, "daily/runs/2026-10-23/20261023T183213971224_05933c99_g7_nifty_futures")
    p8 = package(8, "daily_comparison/2026-10-23/20261023T125505562512_g8_baseline_vs_shock")
    paths = {
        "G3": p3 / "portfolio_trades.csv",
        "G4": p4 / "portfolio_trades.csv",
        "G6": p6 / "g6_nifty_futures_both_counter/portfolio_trades.csv",
        "G7V2": p72 / "g7_nifty_futures_both_counter/portfolio_trades.csv",
        "G7V3": p73 / "g7_nifty_futures_both_counter/portfolio_trades.csv",
        "G8": p8 / "baseline/portfolio_trades.csv",
        "G8shock": p8 / "shock/portfolio_trades.csv",
    }
    assert json.loads((p3 / "replay_result.json").read_text())["complete"] is True
    assert json.loads((p8 / "summary.json").read_text())["through"] == DAY
    for p in (p4, p6, p72, p73):
        s = json.loads((p / "summary.json").read_text())
        assert s["complete"] is True and s["session_date"] == DAY
        assert Path(s["source_run"]).resolve() == p3.resolve()
    assert "REPLICA-V2" in json.loads((p72 / "summary.json").read_text())["variant_id"]
    assert "REPLICA-V3" in json.loads((p73 / "summary.json").read_text())["variant_id"]
    books = {name: pd.read_csv(path) for name, path in paths.items()}
    return paths, books


def main():
    paths, books = load()
    key = ["setup_id", "tradingsymbol", "side"]
    keys = {name: set(map(tuple, book[key].to_numpy())) for name, book in books.items()}
    assert len(set(map(frozenset, keys.values()))) == 1, "Different selected trade identities"
    summary = []
    all_trades = []
    for name, book in books.items():
        trades = book.loc[book.portfolio_executed.eq(True)].copy()
        net = trades.portfolio_net_profit_rupees.astype(float)
        summary.append(dict(model=name, selected_orders=len(book), trades=len(trades),
                            wins=int((net > 0).sum()), losses=int((net < 0).sum()),
                            win_rate_pct=100 * (net > 0).mean(),
                            gross_pnl_rs=trades.portfolio_gross_profit_rupees.sum(),
                            cost_rs=trades.portfolio_cost_rupees.sum(),
                            net_pnl_rs=net.sum(),
                            losing_trade_sum_rs=net.loc[net < 0].sum(),
                            stop_count=int(trades.exit_reason.astype(str).str.contains("STOP").sum())))
        all_trades.append(book.assign(model=name)[key + ["model", "portfolio_executed",
                                                       "portfolio_net_profit_rupees", "exit_reason"]])
    totals = pd.DataFrame(summary).set_index("model")
    assert np.allclose(totals.gross_pnl_rs - totals.cost_rs, totals.net_pnl_rs, atol=1e-6)
    joined = pd.concat(all_trades, ignore_index=True)
    matrix = joined.pivot(index=key, columns="model", values="portfolio_net_profit_rupees")
    exits = joined.pivot(index=key, columns="model", values="exit_reason")
    assert np.allclose(totals.loc[["G4", "G6", "G7V2", "G8"], "net_pnl_rs"],
                       totals.loc["G4", "net_pnl_rs"], atol=1e-6)
    print("SUMMARY")
    print(totals.round(2).to_string())
    print("\nTRADE NET PNL")
    print(matrix.round(2).to_string())
    print("\nV3 EXIT REASONS")
    print(exits[["G7V2", "G7V3"]].to_string())
    print("\nSOURCE PATHS")
    for name, path in paths.items():
        print(name, path)


if __name__ == "__main__":
    main()
