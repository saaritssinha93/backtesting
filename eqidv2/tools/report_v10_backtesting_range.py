from __future__ import annotations

import argparse
import math
from pathlib import Path

import pandas as pd


V10_TRADES_CSV = Path(r"C:\TradingData\eqidv2\outputs_ID_v10_5min\trades.csv")
DEFAULT_OUT_DIR = Path(r"C:\TradingData\eqidv2\v10_backtesting_reports")
IST = "Asia/Kolkata"


def _to_ts(value) -> pd.Timestamp:
    ts = pd.to_datetime(value, errors="coerce")
    if pd.isna(ts):
        return pd.NaT
    if ts.tz is None:
        return ts.tz_localize(IST)
    return ts.tz_convert(IST)


def _to_num(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce").fillna(0.0)


def _fmt_num(value, digits: int = 2) -> str:
    try:
        value = float(value)
    except Exception:
        return str(value)
    if math.isnan(value):
        return ""
    return f"{value:,.{digits}f}"


def _fmt_pct(value) -> str:
    return f"{_fmt_num(value, 2)}%"


def _pf(pnl: pd.Series) -> float:
    s = pd.to_numeric(pnl, errors="coerce").fillna(0.0)
    gains = float(s[s > 0].sum())
    losses = float(-s[s < 0].sum())
    if losses <= 0:
        return math.inf if gains > 0 else 0.0
    return gains / losses


def _metrics(df: pd.DataFrame) -> dict:
    if df.empty:
        return {
            "trades": 0,
            "wins": 0,
            "losses": 0,
            "win_rate_pct": 0.0,
            "target": 0,
            "sl": 0,
            "eod": 0,
            "profit_factor": 0.0,
            "net_pnl": 0.0,
            "gross_pnl": 0.0,
            "cost": 0.0,
            "long_trades": 0,
            "short_trades": 0,
            "long_pnl": 0.0,
            "short_pnl": 0.0,
        }
    pnl = _to_num(df["v6_net_pnl_rs"])
    side = df["side"].astype(str).str.upper()
    outcome = df["v6_outcome"].astype(str).str.upper()
    return {
        "trades": int(len(df)),
        "wins": int((pnl > 0).sum()),
        "losses": int((pnl < 0).sum()),
        "win_rate_pct": float((pnl > 0).mean() * 100.0),
        "target": int((outcome == "TARGET").sum()),
        "sl": int((outcome == "SL").sum()),
        "eod": int((outcome == "EOD").sum()),
        "profit_factor": float(_pf(pnl)),
        "net_pnl": float(pnl.sum()),
        "gross_pnl": float(_to_num(df["v6_gross_pnl_rs"]).sum()) if "v6_gross_pnl_rs" in df.columns else 0.0,
        "cost": float(_to_num(df["v6_cost_rs"]).sum()) if "v6_cost_rs" in df.columns else 0.0,
        "long_trades": int((side == "LONG").sum()),
        "short_trades": int((side == "SHORT").sum()),
        "long_pnl": float(pnl[side == "LONG"].sum()),
        "short_pnl": float(pnl[side == "SHORT"].sum()),
    }


def _markdown_table(headers: list[str], rows: list[list[object]]) -> str:
    if not rows:
        return "_No rows._"
    out = [
        "| " + " | ".join(headers) + " |",
        "| " + " | ".join(["---"] * len(headers)) + " |",
    ]
    for row in rows:
        out.append("| " + " | ".join(str(x) for x in row) + " |")
    return "\n".join(out)


def _prepare(df: pd.DataFrame, start: str, end: str) -> pd.DataFrame:
    out = df.copy()
    out["trade_date"] = out["trade_date"].astype(str).str[:10]
    out["signal_ts"] = out["signal_time_ist"].map(_to_ts)
    out["entry_ts"] = out["entry_time_v6"].map(_to_ts)
    out["exit_ts"] = out["v6_exit_time_ist"].map(_to_ts)
    out["ticker"] = out["ticker"].astype(str).str.upper()
    out["side"] = out["side"].astype(str).str.upper()
    out["setup"] = out["setup"].astype(str)
    mask = (pd.to_datetime(out["trade_date"]) >= pd.Timestamp(start)) & (
        pd.to_datetime(out["trade_date"]) <= pd.Timestamp(end)
    )
    out = out.loc[mask].copy()
    return out.sort_values(["trade_date", "entry_ts", "ticker"]).reset_index(drop=True)


def _group_rows(df: pd.DataFrame, group_cols: list[str]) -> list[list[object]]:
    rows: list[list[object]] = []
    if df.empty:
        return rows
    for keys, g in df.groupby(group_cols, sort=True):
        if not isinstance(keys, tuple):
            keys = (keys,)
        m = _metrics(g)
        label = " ".join(str(k) for k in keys)
        rows.append(
            [
                label,
                m["trades"],
                _fmt_num(m["net_pnl"]),
                _fmt_pct(m["win_rate_pct"]),
                _fmt_num(m["profit_factor"], 3),
                f"{m['target']}/{m['sl']}/{m['eod']}",
                _fmt_num(g["v6_sl_pct"].iloc[0]) if "v6_sl_pct" in g.columns else "",
                _fmt_num(g["v6_target_pct"].iloc[0]) if "v6_target_pct" in g.columns else "",
            ]
        )
    return rows


def _date_rows(df: pd.DataFrame, start: str, end: str) -> list[list[object]]:
    rows = []
    for day in pd.date_range(start=start, end=end, freq="D").strftime("%Y-%m-%d"):
        g = df.loc[df["trade_date"] == day]
        m = _metrics(g)
        first = g["entry_ts"].min()
        last = g["entry_ts"].max()
        rows.append(
            [
                day,
                m["trades"],
                _fmt_num(m["net_pnl"]),
                _fmt_pct(m["win_rate_pct"]),
                _fmt_num(m["profit_factor"], 3),
                f"{m['target']}/{m['sl']}/{m['eod']}",
                first.strftime("%H:%M") if not pd.isna(first) else "",
                last.strftime("%H:%M") if not pd.isna(last) else "",
            ]
        )
    return rows


def _time_bucket(ts: pd.Timestamp) -> str:
    if pd.isna(ts):
        return "NA"
    minutes = ts.hour * 60 + ts.minute
    if minutes < 570:
        return "<09:30"
    if minutes <= 660:
        return "09:30-11:00"
    if minutes <= 750:
        return "11:01-12:30"
    if minutes <= 900:
        return "12:31-15:00"
    return ">15:00"


def _trade_rows(df: pd.DataFrame) -> list[list[object]]:
    rows = []
    for _, r in df.sort_values(["entry_ts", "ticker"]).iterrows():
        rows.append(
            [
                r["signal_ts"].strftime("%H:%M") if not pd.isna(r["signal_ts"]) else "",
                r["entry_ts"].strftime("%H:%M") if not pd.isna(r["entry_ts"]) else "",
                str(r["ticker"]),
                str(r["side"]),
                str(r["setup"]),
                _fmt_num(r.get("entry_price_v6")),
                _fmt_num(r.get("v6_sl_pct")),
                _fmt_num(r.get("v6_target_pct")),
                str(r.get("v6_outcome", "")),
                r["exit_ts"].strftime("%H:%M") if not pd.isna(r["exit_ts"]) else "",
                _fmt_num(r.get("v6_exit_price")),
                str(int(float(r.get("v6_bars_held", 0)))) if str(r.get("v6_bars_held", "")).strip() else "",
                _fmt_num(r.get("v6_net_pnl_rs")),
                _fmt_num(r.get("ranker_score"), 3),
                str(r.get("v8_live_gate_stage", "")),
            ]
        )
    return rows


def _build_report(df: pd.DataFrame, start: str, end: str, source: Path, csv_path: Path, inputs_text: str = "") -> str:
    lines: list[str] = []
    lines.append(f"# v10 backtesting report: {start} to {end}")
    lines.append("")
    lines.append("## Method")
    lines.append("")
    if "mode=v7_live_paper_replay" in inputs_text:
        lines.append("- Source: existing v10 `trades.csv` generated by `v7_live_paper_replay` mode.")
        lines.append("- Signal/trade source: actual v7 live paper trades.")
        lines.append("- Entry model: actual v7 live paper entry time and entry price.")
        lines.append("- Exit model: v7 live stop/target from paper trade rows, resolved on 1-minute OHLC bars with 15:20 IST EOD cutoff.")
        lines.append("- PnL: actual live quantity, price-only, no modeled backtest costs.")
    else:
        lines.append("- Source: existing v10 backtest `trades.csv` filtered by `trade_date`.")
        lines.append("- Strategy flow: current v7 live-style discovery pipeline inside v10.")
        lines.append("- Entry model: next 1-minute open after the 5-minute signal.")
        lines.append("- Exit model: setup-specific 1-minute SL/target resolver with 15:20 IST EOD cutoff.")
        lines.append("- PnL: v10 net PnL field after modeled costs from the backtest output.")
    lines.append("")
    lines.append(f"- Input CSV: `{source}`")
    lines.append(f"- Filtered detailed CSV: `{csv_path}`")
    lines.append("")

    m = _metrics(df)
    lines.append("## Overall Summary")
    lines.append("")
    lines.append(
        _markdown_table(
            ["Metric", "Value"],
            [
                ["Trades", m["trades"]],
                ["Wins / Losses", f"{m['wins']} / {m['losses']}"],
                ["Win rate", _fmt_pct(m["win_rate_pct"])],
                ["Target / SL / EOD", f"{m['target']} / {m['sl']} / {m['eod']}"],
                ["Profit factor", _fmt_num(m["profit_factor"], 3)],
                ["Gross PnL Rs", _fmt_num(m["gross_pnl"])],
                ["Costs Rs", _fmt_num(m["cost"])],
                ["Net PnL Rs", _fmt_num(m["net_pnl"])],
                ["LONG trades / PnL", f"{m['long_trades']} / Rs {_fmt_num(m['long_pnl'])}"],
                ["SHORT trades / PnL", f"{m['short_trades']} / Rs {_fmt_num(m['short_pnl'])}"],
            ],
        )
    )
    lines.append("")

    lines.append("## Date Summary")
    lines.append("")
    lines.append(
        _markdown_table(
            ["Date", "Trades", "Net PnL", "Win rate", "PF", "T/SL/EOD", "First entry", "Last entry"],
            _date_rows(df, start, end),
        )
    )
    lines.append("")

    lines.append("## Entry Window Summary")
    lines.append("")
    work = df.copy()
    work["entry_window"] = work["entry_ts"].map(_time_bucket)
    lines.append(
        _markdown_table(
            ["Window", "Trades", "Net PnL", "Win rate", "PF", "T/SL/EOD", "SL%", "TGT%"],
            _group_rows(work, ["entry_window"]),
        )
    )
    lines.append("")

    lines.append("## Setup Summary")
    lines.append("")
    lines.append(
        _markdown_table(
            ["Side Setup", "Trades", "Net PnL", "Win rate", "PF", "T/SL/EOD", "SL%", "TGT%"],
            _group_rows(df, ["side", "setup"]),
        )
    )
    lines.append("")

    for day in pd.date_range(start=start, end=end, freq="D").strftime("%Y-%m-%d"):
        g = df.loc[df["trade_date"] == day].copy()
        lines.append(f"## {day}")
        lines.append("")
        if g.empty:
            lines.append("_No v10 backtesting trades._")
            lines.append("")
            continue
        dm = _metrics(g)
        lines.append(
            f"Trades: {dm['trades']} | Net PnL Rs {_fmt_num(dm['net_pnl'])} | "
            f"Win rate {_fmt_pct(dm['win_rate_pct'])} | PF {_fmt_num(dm['profit_factor'], 3)} | "
            f"T/SL/EOD {dm['target']}/{dm['sl']}/{dm['eod']}"
        )
        lines.append("")
        lines.append("### Setup Breakdown")
        lines.append("")
        lines.append(
            _markdown_table(
                ["Side Setup", "Trades", "Net PnL", "Win rate", "PF", "T/SL/EOD", "SL%", "TGT%"],
                _group_rows(g, ["side", "setup"]),
            )
        )
        lines.append("")
        lines.append("### Trades")
        lines.append("")
        lines.append(
            _markdown_table(
                [
                    "Signal",
                    "Entry",
                    "Ticker",
                    "Side",
                    "Setup",
                    "Entry Px",
                    "SL%",
                    "TGT%",
                    "Outcome",
                    "Exit",
                    "Exit Px",
                    "Bars",
                    "Net PnL",
                    "Rank",
                    "Gate stage",
                ],
                _trade_rows(g),
            )
        )
        lines.append("")
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description="Create a Markdown report for a v10 backtest date range.")
    parser.add_argument("--start", default="2026-05-21")
    parser.add_argument("--end", default="2026-05-29")
    parser.add_argument("--trades-csv", type=Path, default=V10_TRADES_CSV)
    parser.add_argument("--out-dir", type=Path, default=DEFAULT_OUT_DIR)
    parser.add_argument("--doc", type=Path, default=Path("v10_backtesting_2026-05-21 to 2026-05-29.md"))
    args = parser.parse_args()

    if not args.trades_csv.exists():
        raise SystemExit(f"Missing v10 trades CSV: {args.trades_csv}")

    args.out_dir.mkdir(parents=True, exist_ok=True)
    raw = pd.read_csv(args.trades_csv, low_memory=False)
    trades = _prepare(raw, args.start, args.end)

    range_slug = f"{args.start}_to_{args.end}"
    csv_path = args.out_dir / f"v10_backtesting_trades_{range_slug}.csv"
    trades.to_csv(csv_path, index=False)

    inputs_path = args.trades_csv.parent / "inputs.txt"
    inputs_text = inputs_path.read_text(encoding="utf-8") if inputs_path.exists() else ""
    report = _build_report(trades, args.start, args.end, args.trades_csv, csv_path, inputs_text)
    args.doc.write_text(report, encoding="utf-8")

    m = _metrics(trades)
    print(f"Wrote report: {args.doc.resolve()}")
    print(f"Wrote CSV: {csv_path}")
    print(f"Trades: {m['trades']}")
    print(f"Net PnL: {m['net_pnl']:.2f}")
    print(f"Win rate: {m['win_rate_pct']:.2f}%")
    print(f"PF: {m['profit_factor']:.3f}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
