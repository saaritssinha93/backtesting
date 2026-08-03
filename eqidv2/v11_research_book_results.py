"""Build Book C exit-only ledgers and summarize the 2026-07-22 V11 book study."""

from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd

import avwap_5min_ID_v11_backtesting as v11
import v11_exit_policy_resolver as resolver


BASELINE = "final_setup_conf_v11_working"
BOOK_A = "final_setup_conf_v11_book_a_reduced_core"
BOOK_B = "final_setup_conf_v11_book_b_time_filtered"
C_POLICIES = {
    "final_setup_conf_v11_book_c_time90": {"max_hold_minutes": 90},
    "final_setup_conf_v11_book_c_time120": {"max_hold_minutes": 120},
    "final_setup_conf_v11_book_c_breakeven": {"breakeven_trigger_r": 1.0},
    "final_setup_conf_v11_book_c_trailing": {
        "trailing_trigger_r": 1.0,
        "trailing_distance_r": 0.5,
    },
}
BOOK_LABELS = {
    BASELINE: "Baseline working",
    BOOK_A: "Book A reduced core",
    BOOK_B: "Book B time filtered",
    "final_setup_conf_v11_book_c_time90": "Book C 90-minute",
    "final_setup_conf_v11_book_c_time120": "Book C 120-minute",
    "final_setup_conf_v11_book_c_breakeven": "Book C break-even",
    "final_setup_conf_v11_book_c_trailing": "Book C trailing",
}
WINDOWS = {
    "train": ("2026-05-21", "2026-06-19"),
    "validation": ("2026-06-22", "2026-07-07"),
    "diagnostic_holdout": ("2026-07-08", "2026-07-22"),
}


def date_dirs(root: Path, module: str) -> list[Path]:
    path = root / module
    return sorted(
        d for d in path.iterdir()
        if d.is_dir() and len(d.name) == 10 and (d / "summary.txt").exists()
    )


def load_book(root: Path, module: str) -> pd.DataFrame:
    frames = []
    for day_dir in date_dirs(root, module):
        trade_path = day_dir / "v11_ID_trades.csv"
        if not trade_path.exists() or trade_path.stat().st_size <= 2:
            continue
        df = pd.read_csv(trade_path, low_memory=False)
        if df.empty:
            continue
        df["session"] = day_dir.name
        frames.append(df)
    return pd.concat(frames, ignore_index=True, sort=False) if frames else pd.DataFrame()


def build_exit_variant(source: pd.DataFrame, policy: dict) -> pd.DataFrame:
    rows = []
    for _, row in source.iterrows():
        bars = v11._load_1m_with_open(str(row["symbol"]))
        if bars is None or bars.empty:
            raise RuntimeError(f"missing 1-minute bars for {row['symbol']} {row['session']}")
        setup = str(row["setup_name"])
        import final_setup_conf_v11_book_a_reduced_core as book_a
        exit_cfg = book_a.FINAL_SETUP_CONF[setup]["exit"]
        result = resolver.resolve(
            bars,
            str(row["side"]),
            float(row["entry_price"]),
            row["entry_time"],
            float(exit_cfg["sl_pct"]),
            float(exit_cfg["tgt_pct"]),
            policy,
        )
        if result is None:
            raise RuntimeError(f"could not resolve {row['symbol']} {row['entry_time']}")
        out = row.to_dict()
        quantity = int(float(row["quantity"]))
        entry = float(row["entry_price"])
        exit_price = float(result.exit_price)
        pnl = (exit_price - entry) * quantity
        if str(row["side"]).upper() == "SHORT":
            pnl = -pnl
        out.update({
            "exit_time": result.exit_time_ist,
            "exit_price": exit_price,
            "exit_reason": result.outcome,
            "pnl": float(pnl),
            "pnl_pct": float(result.pnl_pct_price),
        })
        rows.append(out)
    return pd.DataFrame(rows)


def write_variant(root: Path, module: str, trades: pd.DataFrame) -> None:
    for day, part in trades.groupby("session", sort=True):
        out = root / module / str(day)
        out.mkdir(parents=True, exist_ok=True)
        part.drop(columns=["session"], errors="ignore").to_csv(out / "v11_ID_trades.csv", index=False)


def stats(module: str, trades: pd.DataFrame, sessions: list[str], window: str) -> dict:
    part = trades[trades["session"].isin(sessions)].copy() if not trades.empty else trades
    pnl = pd.to_numeric(part.get("pnl", pd.Series(dtype=float)), errors="coerce").fillna(0.0)
    daily = part.groupby("session")["pnl"].sum().reindex(sessions, fill_value=0.0)
    cumulative = daily.cumsum()
    peak = cumulative.cummax().clip(lower=0.0)
    positive_days = daily[daily > 0]
    best_day = float(daily.max()) if len(daily) else 0.0
    return {
        "module": module,
        "book": BOOK_LABELS[module],
        "window": window,
        "start": sessions[0],
        "end": sessions[-1],
        "sessions": len(sessions),
        "trades": len(part),
        "trades_per_session": len(part) / len(sessions),
        "wins": int((pnl > 0).sum()),
        "losses": int((pnl < 0).sum()),
        "win_rate_pct": float((pnl > 0).mean() * 100.0) if len(pnl) else 0.0,
        "net_pnl_rs": float(pnl.sum()),
        "profit_factor": float(pnl[pnl > 0].sum() / -pnl[pnl < 0].sum()) if (pnl < 0).any() else np.inf,
        "positive_days": int((daily > 0).sum()),
        "negative_days": int((daily < 0).sum()),
        "max_drawdown_rs": float((cumulative - peak).min()) if len(daily) else 0.0,
        "best_day_share_pct": float(best_day / positive_days.sum() * 100.0) if positive_days.sum() > 0 else 0.0,
        "pnl_without_best_day_rs": float(pnl.sum() - best_day),
    }


def markdown_table(df: pd.DataFrame) -> str:
    return df.to_markdown(index=False)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", type=Path, default=Path(r"C:\TradingData\eqidv2\v11_research_books_20260722"))
    args = ap.parse_args()
    root = args.root

    books = {
        BASELINE: load_book(root, BASELINE),
        BOOK_A: load_book(root, BOOK_A),
        BOOK_B: load_book(root, BOOK_B),
    }
    source = books[BOOK_A]
    for module, policy in C_POLICIES.items():
        derived = build_exit_variant(source, policy)
        write_variant(root, module, derived)
        books[module] = derived

    all_sessions = sorted({d.name for d in date_dirs(root, BASELINE)})
    rows = []
    for window, (start, end) in WINDOWS.items():
        sessions = [day for day in all_sessions if start <= day <= end]
        for module, trades in books.items():
            rows.append(stats(module, trades, sessions, window))
    summary = pd.DataFrame(rows)
    summary.to_csv(root / "fixed_window_comparison.csv", index=False)

    setup_frames = []
    for module, trades in books.items():
        if trades.empty:
            continue
        tmp = trades.copy()
        tmp["pnl"] = pd.to_numeric(tmp["pnl"], errors="coerce").fillna(0.0)
        grouped = tmp.groupby(["side", "setup_name"], dropna=False)
        for (side, setup), part in grouped:
            p = part["pnl"]
            setup_frames.append({
                "module": module,
                "book": BOOK_LABELS[module],
                "side": side,
                "setup": setup,
                "trades": len(part),
                "win_rate_pct": (p > 0).mean() * 100.0,
                "net_pnl_rs": p.sum(),
                "profit_factor": p[p > 0].sum() / -p[p < 0].sum() if (p < 0).any() else np.inf,
            })
    setup_summary = pd.DataFrame(setup_frames)
    setup_summary.to_csv(root / "setup_comparison.csv", index=False)

    report = [
        "# V11 Research Book Fixed-Window Comparison",
        "",
        "- Candidate source: archived V7 live-parity snapshots.",
        "- P&L basis: current legacy live-parity price P&L; cost-reporting work was explicitly deferred.",
        "- The final window is diagnostic, not untouched: July data influenced Book B's time hypotheses.",
        "- Book C uses Book A's exact entries; only the exit path changes.",
        "",
    ]
    show_cols = [
        "book", "trades", "win_rate_pct", "net_pnl_rs", "profit_factor",
        "positive_days", "negative_days", "max_drawdown_rs", "pnl_without_best_day_rs",
    ]
    for window in WINDOWS:
        view = summary[summary["window"].eq(window)][show_cols].copy()
        for col in ["win_rate_pct", "net_pnl_rs", "profit_factor", "max_drawdown_rs", "pnl_without_best_day_rs"]:
            view[col] = view[col].round(2)
        report.extend([f"## {window.replace('_', ' ').title()}", "", markdown_table(view), ""])
    (root / "fixed_window_comparison.md").write_text("\n".join(report), encoding="utf-8")
    print(summary.to_string(index=False))
    print(f"wrote {root / 'fixed_window_comparison.md'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
