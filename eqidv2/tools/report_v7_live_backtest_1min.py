from __future__ import annotations

import argparse
import math
import re
import sys
from functools import lru_cache
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import v17D_exit_resolver as exit_resolver


LIVE_DIR = Path(r"C:\TradingData\eqidv2\live_signals")
ONE_MIN_DIR = Path(r"C:\TradingData\eqidv2\stocks_indicators_1min_eq")
DEFAULT_OUT_DIR = Path(r"C:\TradingData\eqidv2\v7_live_1min_backtest_reports")
IST = "Asia/Kolkata"


def _to_ts(value) -> pd.Timestamp:
    ts = pd.to_datetime(value, errors="coerce")
    if pd.isna(ts):
        return pd.NaT
    if ts.tz is None:
        return ts.tz_localize(IST)
    return ts.tz_convert(IST)


def _to_float(value, default: float = 0.0) -> float:
    try:
        if value is None or str(value).strip() == "":
            return default
        out = float(value)
        if math.isnan(out):
            return default
        return out
    except Exception:
        return default


def _fmt_num(value, digits: int = 2) -> str:
    if value is None:
        return ""
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


def _metrics(df: pd.DataFrame, pnl_col: str, outcome_col: str) -> dict:
    if df.empty:
        return {
            "trades": 0,
            "wins": 0,
            "losses": 0,
            "win_rate_pct": 0.0,
            "target": 0,
            "sl": 0,
            "eod": 0,
            "net_pnl": 0.0,
            "profit_factor": 0.0,
        }
    pnl = pd.to_numeric(df[pnl_col], errors="coerce").fillna(0.0)
    outcome = df[outcome_col].astype(str).str.upper()
    return {
        "trades": int(len(df)),
        "wins": int((pnl > 0).sum()),
        "losses": int((pnl < 0).sum()),
        "win_rate_pct": float((pnl > 0).mean() * 100.0),
        "target": int((outcome == "TARGET").sum()),
        "sl": int((outcome == "SL").sum()),
        "eod": int(outcome.isin(["EOD", "EOD_CLOSE"]).sum()),
        "net_pnl": float(pnl.sum()),
        "profit_factor": float(_pf(pnl)),
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


def _read_live_trades(start: str, end: str, live_dir: Path) -> pd.DataFrame:
    start_dt = pd.Timestamp(start).date()
    end_dt = pd.Timestamp(end).date()
    rows: list[dict] = []
    pattern = re.compile(r"paper_trades_(\d{4}-\d{2}-\d{2})_id_5min_v7\.csv$", re.I)
    for path in sorted(live_dir.glob("paper_trades_*_id_5min_v7.csv")):
        m = pattern.search(path.name)
        if not m:
            continue
        day = pd.Timestamp(m.group(1)).date()
        if day < start_dt or day > end_dt:
            continue
        frame = pd.read_csv(path)
        if frame.empty or "ticker" not in frame.columns:
            continue
        for _, row in frame.iterrows():
            ticker = str(row.get("ticker", "")).strip().upper()
            if not ticker:
                continue
            entry_ts = _to_ts(row.get("entry_time"))
            signal_ts = _to_ts(row.get("signal_datetime", row.get("signal_entry_datetime_ist")))
            exit_ts = _to_ts(row.get("exit_time"))
            side = str(row.get("side", "")).strip().upper()
            setup = str(row.get("setup", "")).strip()
            entry = _to_float(row.get("entry_price"))
            stop = _to_float(row.get("stop_price"))
            target = _to_float(row.get("target_price"))
            if side == "LONG":
                sl_pct = (entry - stop) / entry * 100.0 if entry > 0 and stop > 0 else math.nan
                target_pct = (target - entry) / entry * 100.0 if entry > 0 and target > 0 else math.nan
            else:
                sl_pct = (stop - entry) / entry * 100.0 if entry > 0 and stop > 0 else math.nan
                target_pct = (entry - target) / entry * 100.0 if entry > 0 and target > 0 else math.nan
            rows.append(
                {
                    "trade_date": str(day),
                    "trade_id": row.get("trade_id", ""),
                    "signal_id": row.get("signal_id", ""),
                    "signal_time": signal_ts,
                    "entry_time": entry_ts,
                    "live_exit_time": exit_ts,
                    "ticker": ticker,
                    "side": side,
                    "setup": setup,
                    "quantity": int(_to_float(row.get("quantity"), 0.0)),
                    "entry_price": entry,
                    "stop_price": stop,
                    "target_price": target,
                    "sl_pct_from_live": sl_pct,
                    "target_pct_from_live": target_pct,
                    "live_exit_price": _to_float(row.get("exit_price")),
                    "live_outcome": str(row.get("outcome", "")).strip().upper(),
                    "live_pnl_rs": _to_float(row.get("pnl_rs")),
                    "quality_score": _to_float(row.get("quality_score"), math.nan),
                }
            )
    if not rows:
        return pd.DataFrame()
    out = pd.DataFrame(rows).sort_values(["trade_date", "entry_time", "ticker"]).reset_index(drop=True)
    return out


@lru_cache(maxsize=None)
def _load_1m(ticker: str, one_min_dir: str) -> pd.DataFrame | None:
    path = Path(one_min_dir) / f"{ticker}_stocks_indicators_1min.parquet"
    if not path.exists():
        return None
    df = pd.read_parquet(path, columns=["date", "high", "low", "close"])
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    if getattr(df["date"].dt, "tz", None) is None:
        df["date"] = df["date"].dt.tz_localize("UTC").dt.tz_convert(IST)
    else:
        df["date"] = df["date"].dt.tz_convert(IST)
    return df.dropna(subset=["date"]).sort_values("date").set_index("date")


def _mfe_mae(
    bars: pd.DataFrame | None,
    side: str,
    entry_price: float,
    entry_time: pd.Timestamp,
    exit_time: pd.Timestamp,
) -> tuple[float, float]:
    if bars is None or bars.empty or pd.isna(entry_time) or pd.isna(exit_time) or entry_price <= 0:
        return math.nan, math.nan
    sub = bars[(bars.index >= entry_time) & (bars.index <= exit_time)]
    if sub.empty:
        return math.nan, math.nan
    high = float(sub["high"].max())
    low = float(sub["low"].min())
    if side == "SHORT":
        mfe = (entry_price - low) / entry_price * 100.0
        mae = (entry_price - high) / entry_price * 100.0
    else:
        mfe = (high - entry_price) / entry_price * 100.0
        mae = (low - entry_price) / entry_price * 100.0
    return mfe, mae


def _resolve_live_trade(row: pd.Series, one_min_dir: Path) -> dict:
    bars = _load_1m(str(row["ticker"]), str(one_min_dir))
    if bars is None or bars.empty:
        return {"one_min_status": "MISSING_1MIN"}
    if row["entry_price"] <= 0 or pd.isna(row["entry_time"]):
        return {"one_min_status": "BAD_ENTRY"}
    if not math.isfinite(row["sl_pct_from_live"]) or not math.isfinite(row["target_pct_from_live"]):
        return {"one_min_status": "BAD_SL_TARGET"}
    result = exit_resolver.resolve(
        bars=bars,
        side=row["side"],
        entry_price=float(row["entry_price"]),
        entry_time_ist=row["entry_time"],
        sl_pct=float(row["sl_pct_from_live"]),
        tgt_pct=float(row["target_pct_from_live"]),
    )
    if result is None:
        return {"one_min_status": "NO_BARS_IN_WINDOW"}

    qty = int(row["quantity"])
    if row["side"] == "SHORT":
        pnl = (float(row["entry_price"]) - float(result.exit_price)) * qty
    else:
        pnl = (float(result.exit_price) - float(row["entry_price"])) * qty
    mfe, mae = _mfe_mae(bars, row["side"], float(row["entry_price"]), row["entry_time"], result.exit_time_ist)
    return {
        "one_min_status": "RESOLVED",
        "one_min_outcome": result.outcome,
        "one_min_exit_time": result.exit_time_ist,
        "one_min_exit_price": float(result.exit_price),
        "one_min_bars_held": int(result.bars_held),
        "one_min_pnl_pct_price": float(result.pnl_pct_price),
        "one_min_pnl_rs": float(pnl),
        "one_min_mfe_pct": float(mfe),
        "one_min_mae_pct": float(mae),
    }


def _add_resolution(trades: pd.DataFrame, one_min_dir: Path) -> pd.DataFrame:
    resolved = []
    for _, row in trades.iterrows():
        rec = row.to_dict()
        rec.update(_resolve_live_trade(row, one_min_dir))
        resolved.append(rec)
    out = pd.DataFrame(resolved)
    out["pnl_delta_1min_minus_live"] = (
        pd.to_numeric(out.get("one_min_pnl_rs"), errors="coerce").fillna(0.0)
        - pd.to_numeric(out["live_pnl_rs"], errors="coerce").fillna(0.0)
    )
    out["outcome_changed"] = (
        out.get("one_min_outcome", "").astype(str).str.upper().replace({"EOD": "EOD_CLOSE"})
        != out["live_outcome"].astype(str).str.upper().replace({"EOD": "EOD_CLOSE"})
    )
    return out


def _group_rows(df: pd.DataFrame, group_col: str) -> list[list[object]]:
    rows = []
    for name, g in df.groupby(group_col, sort=True):
        live = _metrics(g, "live_pnl_rs", "live_outcome")
        one = _metrics(g, "one_min_pnl_rs", "one_min_outcome")
        rows.append(
            [
                name,
                len(g),
                _fmt_num(live["net_pnl"]),
                _fmt_num(one["net_pnl"]),
                _fmt_num(one["net_pnl"] - live["net_pnl"]),
                _fmt_pct(live["win_rate_pct"]),
                _fmt_pct(one["win_rate_pct"]),
                f"{one['target']}/{one['sl']}/{one['eod']}",
            ]
        )
    return rows


def _trade_rows(df: pd.DataFrame) -> list[list[object]]:
    rows = []
    for _, r in df.sort_values(["entry_time", "ticker"]).iterrows():
        rows.append(
            [
                r["entry_time"].strftime("%H:%M:%S") if not pd.isna(r["entry_time"]) else "",
                r["ticker"],
                r["side"],
                r["setup"],
                int(r["quantity"]),
                _fmt_num(r["entry_price"]),
                _fmt_num(r["stop_price"]),
                _fmt_num(r["target_price"]),
                r["live_outcome"],
                _fmt_num(r["live_pnl_rs"]),
                str(r.get("one_min_outcome", "")),
                r["one_min_exit_time"].strftime("%H:%M") if not pd.isna(r.get("one_min_exit_time", pd.NaT)) else "",
                _fmt_num(r.get("one_min_exit_price")),
                _fmt_num(r.get("one_min_pnl_rs")),
                _fmt_num(r.get("pnl_delta_1min_minus_live")),
                _fmt_num(r.get("one_min_mfe_pct")),
                _fmt_num(r.get("one_min_mae_pct")),
            ]
        )
    return rows


def _build_report(df: pd.DataFrame, start: str, end: str, live_dir: Path, one_min_dir: Path, csv_path: Path) -> str:
    lines: list[str] = []
    lines.append(f"# v7 live 1-minute backtest: {start} to {end}")
    lines.append("")
    lines.append("## Method")
    lines.append("")
    lines.append("- Signal/trade source: actual v7 live paper trade files.")
    lines.append("- Entry: actual v7 live paper entry time, entry price, side, setup, and quantity.")
    lines.append("- Stop/target: actual v7 live paper stop and target prices, converted to per-trade percentages.")
    lines.append("- Exit: `v17D_exit_resolver` over 1-minute OHLC bars, with 15:20 IST EOD cutoff.")
    lines.append("- Same-bar SL and target collision uses resolver's pessimistic rule: SL first.")
    lines.append("- PnL: price-only using the actual v7 live quantity; no brokerage/cost deduction.")
    lines.append("")
    lines.append(f"- Live files: `{live_dir}`")
    lines.append(f"- 1-minute data: `{one_min_dir}`")
    lines.append(f"- Detailed CSV: `{csv_path}`")
    lines.append("")

    if df.empty:
        lines.append("No v7 live trades found for this range.")
        return "\n".join(lines)

    live = _metrics(df, "live_pnl_rs", "live_outcome")
    one = _metrics(df, "one_min_pnl_rs", "one_min_outcome")
    lines.append("## Overall Summary")
    lines.append("")
    lines.append(
        _markdown_table(
            ["Metric", "v7 live paper", "1-min backtest", "Delta"],
            [
                ["Trades", live["trades"], one["trades"], one["trades"] - live["trades"]],
                ["Wins", live["wins"], one["wins"], one["wins"] - live["wins"]],
                ["Losses", live["losses"], one["losses"], one["losses"] - live["losses"]],
                ["Win rate", _fmt_pct(live["win_rate_pct"]), _fmt_pct(one["win_rate_pct"]), _fmt_pct(one["win_rate_pct"] - live["win_rate_pct"])],
                ["Target/SL/EOD", f"{live['target']}/{live['sl']}/{live['eod']}", f"{one['target']}/{one['sl']}/{one['eod']}", ""],
                ["Profit factor", _fmt_num(live["profit_factor"], 3), _fmt_num(one["profit_factor"], 3), _fmt_num(one["profit_factor"] - live["profit_factor"], 3)],
                ["Net PnL Rs", _fmt_num(live["net_pnl"]), _fmt_num(one["net_pnl"]), _fmt_num(one["net_pnl"] - live["net_pnl"])],
                ["Outcome changed", "", int(df["outcome_changed"].sum()), ""],
            ],
        )
    )
    lines.append("")

    lines.append("## Date Summary")
    lines.append("")
    date_rows = []
    all_dates = pd.date_range(start=start, end=end, freq="D").strftime("%Y-%m-%d").tolist()
    for day in all_dates:
        g = df.loc[df["trade_date"] == day]
        live_d = _metrics(g, "live_pnl_rs", "live_outcome")
        one_d = _metrics(g, "one_min_pnl_rs", "one_min_outcome")
        date_rows.append(
            [
                day,
                live_d["trades"],
                _fmt_num(live_d["net_pnl"]),
                _fmt_num(one_d["net_pnl"]),
                _fmt_num(one_d["net_pnl"] - live_d["net_pnl"]),
                _fmt_pct(live_d["win_rate_pct"]),
                _fmt_pct(one_d["win_rate_pct"]),
                f"{one_d['target']}/{one_d['sl']}/{one_d['eod']}",
            ]
        )
    lines.append(_markdown_table(["Date", "Trades", "Live PnL", "1-min PnL", "Delta", "Live win", "1-min win", "1-min T/SL/EOD"], date_rows))
    lines.append("")

    lines.append("## Setup Summary")
    lines.append("")
    lines.append(
        _markdown_table(
            ["Setup", "Trades", "Live PnL", "1-min PnL", "Delta", "Live win", "1-min win", "1-min T/SL/EOD"],
            _group_rows(df, "setup"),
        )
    )
    lines.append("")

    for day in all_dates:
        g = df.loc[df["trade_date"] == day].copy()
        lines.append(f"## {day}")
        lines.append("")
        if g.empty:
            lines.append("_No v7 live trades._")
            lines.append("")
            continue
        live_d = _metrics(g, "live_pnl_rs", "live_outcome")
        one_d = _metrics(g, "one_min_pnl_rs", "one_min_outcome")
        lines.append(
            f"Trades: {len(g)} | live PnL Rs {_fmt_num(live_d['net_pnl'])} | "
            f"1-min PnL Rs {_fmt_num(one_d['net_pnl'])} | "
            f"delta Rs {_fmt_num(one_d['net_pnl'] - live_d['net_pnl'])} | "
            f"1-min T/SL/EOD {one_d['target']}/{one_d['sl']}/{one_d['eod']}"
        )
        lines.append("")
        lines.append("### Setup Breakdown")
        lines.append("")
        lines.append(
            _markdown_table(
                ["Setup", "Trades", "Live PnL", "1-min PnL", "Delta", "Live win", "1-min win", "1-min T/SL/EOD"],
                _group_rows(g, "setup"),
            )
        )
        lines.append("")
        lines.append("### Trades")
        lines.append("")
        lines.append(
            _markdown_table(
                [
                    "Entry",
                    "Ticker",
                    "Side",
                    "Setup",
                    "Qty",
                    "Entry Px",
                    "SL",
                    "TGT",
                    "Live Out",
                    "Live PnL",
                    "1m Out",
                    "1m Exit",
                    "1m Exit Px",
                    "1m PnL",
                    "Delta",
                    "MFE%",
                    "MAE%",
                ],
                _trade_rows(g),
            )
        )
        lines.append("")
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description="Backtest actual v7 live paper trades through 1-minute OHLC exits.")
    parser.add_argument("--start", default="2026-05-21")
    parser.add_argument("--end", default="2026-05-29")
    parser.add_argument("--live-dir", type=Path, default=LIVE_DIR)
    parser.add_argument("--one-min-dir", type=Path, default=ONE_MIN_DIR)
    parser.add_argument("--out-dir", type=Path, default=DEFAULT_OUT_DIR)
    parser.add_argument("--doc", type=Path, default=Path("v7_live_2026-05-21 to 2026-05-29.md"))
    args = parser.parse_args()

    args.out_dir.mkdir(parents=True, exist_ok=True)
    trades = _read_live_trades(args.start, args.end, args.live_dir)
    resolved = _add_resolution(trades, args.one_min_dir) if not trades.empty else trades

    range_slug = f"{args.start}_to_{args.end}"
    csv_path = args.out_dir / f"v7_live_1min_backtest_trades_{range_slug}.csv"
    resolved.to_csv(csv_path, index=False)

    report = _build_report(resolved, args.start, args.end, args.live_dir, args.one_min_dir, csv_path)
    args.doc.write_text(report, encoding="utf-8")

    print(f"Wrote report: {args.doc.resolve()}")
    print(f"Wrote CSV: {csv_path}")
    if not resolved.empty:
        live = _metrics(resolved, "live_pnl_rs", "live_outcome")
        one = _metrics(resolved, "one_min_pnl_rs", "one_min_outcome")
        print(f"Trades: {len(resolved)}")
        print(f"Live PnL: {live['net_pnl']:.2f}")
        print(f"1-min PnL: {one['net_pnl']:.2f}")
        print(f"Delta: {one['net_pnl'] - live['net_pnl']:.2f}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
