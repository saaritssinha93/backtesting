#!/usr/bin/env python3
"""
Validate V11 lab books across fixed live-parity windows.

Research-only. Runs modules through avwap_5min_ID_v11_backtesting.py in
live_parity mode using archived V7 candidate JSON snapshots, then writes
module/window/setup/side/time reports.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import math
import os
import re
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Iterable

import numpy as np
import pandas as pd

from eqidv2_runtime_paths import runtime_dir
from v11_lab_shadow_monitor import LIVE_PARITY_ENV


BASE_DIR = Path(__file__).resolve().parent
DEFAULT_JSON_DIR = runtime_dir("signal_discovery_v7_5mins_ID") / "json"
DEFAULT_OUT_ROOT = runtime_dir("backtesting_result_v11") / "v11_lab_books_confabc_20260710"
DATE_RE = re.compile(r"candidate_tickers_(\d{8})_\d{4}\.json$")
DEFAULT_SKIP_DATES = {"2026-05-28"}


def _date_from_token(token: str) -> str:
    return f"{token[:4]}-{token[4:6]}-{token[6:8]}"


def discover_dates(json_dir: Path, date_from: str, date_to: str, skip_dates: set[str]) -> list[str]:
    counts: dict[str, int] = {}
    for path in json_dir.glob("candidate_tickers_*.json"):
        match = DATE_RE.match(path.name)
        if not match:
            continue
        day = _date_from_token(match.group(1))
        if day < date_from or day > date_to or day in skip_dates:
            continue
        counts[day] = counts.get(day, 0) + 1
    return sorted(day for day, count in counts.items() if count >= 10)


def _read_csv(path: Path) -> pd.DataFrame:
    if not path.exists() or path.stat().st_size <= 2:
        return pd.DataFrame()
    try:
        return pd.read_csv(path, low_memory=False)
    except pd.errors.EmptyDataError:
        return pd.DataFrame()


def _run_one(module: str, day: str, out_root: Path, json_dir: Path, force: bool) -> tuple[bool, str, Path]:
    out_dir = out_root / module / day
    done_marker = out_dir / "summary.txt"
    if done_marker.exists() and not force:
        return True, "cached", out_dir
    out_dir.mkdir(parents=True, exist_ok=True)
    env = os.environ.copy()
    env.update(LIVE_PARITY_ENV)
    env["EQIDV2_V11_FINAL_SETUP_CONF_MODULE"] = module
    cmd = [
        sys.executable, "-u", str(BASE_DIR / "avwap_5min_ID_v11_backtesting.py"),
        "--out", str(out_dir),
        "--selected_strategy_profile", "final_setup_conf",
        "--mode", "live_parity",
        "--live_date", day,
        "--live_candidate_json_dir", str(json_dir),
    ]
    proc = subprocess.run(cmd, cwd=str(BASE_DIR), env=env, text=True, capture_output=True, timeout=1800)
    (out_dir / "validator_run.log").write_text(
        f"module={module}\nday={day}\ncmd={' '.join(cmd)}\nreturncode={proc.returncode}\n\nSTDOUT\n{proc.stdout}\n\nSTDERR\n{proc.stderr}\n",
        encoding="utf-8",
        errors="replace",
    )
    return proc.returncode == 0, f"exit={proc.returncode}", out_dir


def _load_trades(module: str, dates: list[str], out_root: Path) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for day in dates:
        path = out_root / module / day / "v11_ID_trades.csv"
        df = _read_csv(path)
        if df.empty:
            continue
        df["module"] = module
        df["date"] = df.get("date", day).astype(str)
        df["pnl"] = pd.to_numeric(df.get("pnl", 0.0), errors="coerce").fillna(0.0)
        frames.append(df)
    if not frames:
        return pd.DataFrame(columns=["module", "date", "symbol", "side", "setup_name", "entry_time", "exit_reason", "pnl"])
    return pd.concat(frames, ignore_index=True, sort=False)


def _daily_for_module(trades: pd.DataFrame, module: str, dates: list[str]) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    mod = trades[trades["module"].eq(module)] if not trades.empty else trades
    for day in dates:
        part = mod[mod["date"].astype(str).eq(day)] if not mod.empty else mod
        pnl = float(part["pnl"].sum()) if not part.empty else 0.0
        rows.append({
            "module": module,
            "date": day,
            "trades": int(len(part)),
            "wins": int((part["pnl"] > 0).sum()) if not part.empty else 0,
            "losses": int((part["pnl"] < 0).sum()) if not part.empty else 0,
            "pnl_rs": round(pnl, 2),
        })
    out = pd.DataFrame(rows)
    out["cum_pnl_rs"] = out["pnl_rs"].cumsum()
    out["peak_pnl_rs"] = out["cum_pnl_rs"].cummax()
    out["drawdown_rs"] = out["cum_pnl_rs"] - out["peak_pnl_rs"]
    return out


def _profit_factor(pnl: pd.Series) -> float:
    gross_profit = float(pnl[pnl > 0].sum())
    gross_loss = float(-pnl[pnl < 0].sum())
    if gross_loss > 0:
        return gross_profit / gross_loss
    return float("inf") if gross_profit > 0 else 0.0


def _summary(module: str, trades: pd.DataFrame, daily: pd.DataFrame, dates: list[str], window: str) -> dict[str, Any]:
    pnl = trades["pnl"] if not trades.empty else pd.Series(dtype=float)
    gross_profit = float(pnl[pnl > 0].sum()) if not pnl.empty else 0.0
    gross_loss = float(-pnl[pnl < 0].sum()) if not pnl.empty else 0.0
    best_day = float(daily["pnl_rs"].max()) if not daily.empty else 0.0
    pos_sum = float(daily.loc[daily["pnl_rs"] > 0, "pnl_rs"].sum()) if not daily.empty else 0.0
    return {
        "module": module,
        "window": window,
        "start": dates[0] if dates else "",
        "end": dates[-1] if dates else "",
        "days": int(len(dates)),
        "trade_days": int((daily["trades"] > 0).sum()) if not daily.empty else 0,
        "positive_days": int((daily["pnl_rs"] > 0).sum()) if not daily.empty else 0,
        "negative_days": int((daily["pnl_rs"] < 0).sum()) if not daily.empty else 0,
        "flat_days": int((daily["pnl_rs"] == 0).sum()) if not daily.empty else 0,
        "trades": int(len(trades)),
        "trades_per_day": round(len(trades) / len(dates), 2) if dates else 0.0,
        "wins": int((pnl > 0).sum()) if not pnl.empty else 0,
        "losses": int((pnl < 0).sum()) if not pnl.empty else 0,
        "win_rate_pct": round(float((pnl > 0).mean() * 100.0), 2) if len(pnl) else 0.0,
        "net_pnl_rs": round(float(pnl.sum()) if not pnl.empty else 0.0, 2),
        "gross_profit_rs": round(gross_profit, 2),
        "gross_loss_rs": round(gross_loss, 2),
        "profit_factor": round(_profit_factor(pnl), 3),
        "avg_pnl_per_day_rs": round((float(pnl.sum()) if not pnl.empty else 0.0) / len(dates), 2) if dates else 0.0,
        "max_losing_day_rs": round(float(daily["pnl_rs"].min()) if not daily.empty else 0.0, 2),
        "max_daily_drawdown_rs": round(float(daily["drawdown_rs"].min()) if "drawdown_rs" in daily else 0.0, 2),
        "best_day_share_pct": round(100.0 * best_day / pos_sum, 2) if best_day > 0 and pos_sum > 0 else 0.0,
        "pnl_without_best_day_rs": round((float(pnl.sum()) if not pnl.empty else 0.0) - best_day, 2),
    }


def _group_summary(trades: pd.DataFrame, group_cols: list[str]) -> pd.DataFrame:
    if trades.empty:
        return pd.DataFrame()
    rows: list[dict[str, Any]] = []
    for keys, part in trades.groupby(group_cols, dropna=False):
        if not isinstance(keys, tuple):
            keys = (keys,)
        pnl = part["pnl"]
        row = {col: val for col, val in zip(group_cols, keys)}
        row.update({
            "trades": int(len(part)),
            "wins": int((pnl > 0).sum()),
            "losses": int((pnl < 0).sum()),
            "win_rate_pct": round(float((pnl > 0).mean() * 100.0), 2),
            "net_pnl_rs": round(float(pnl.sum()), 2),
            "profit_factor": round(_profit_factor(pnl), 3),
            "avg_pnl_rs": round(float(pnl.mean()), 2),
        })
        rows.append(row)
    return pd.DataFrame(rows).sort_values("net_pnl_rs", ascending=False).reset_index(drop=True)


def _add_time_bucket(trades: pd.DataFrame) -> pd.DataFrame:
    if trades.empty:
        return trades
    out = trades.copy()
    ts = pd.to_datetime(out.get("entry_time", ""), errors="coerce")
    mins = ts.dt.hour * 60 + ts.dt.minute
    bins = [0, 600, 690, 780, 870, 2000]
    labels = ["pre_10", "10_00_11_30", "11_30_13_00", "13_00_14_30", "after_14_30"]
    out["time_bucket"] = pd.cut(mins, bins=bins, labels=labels, right=True).astype(str)
    return out


def _window_dates(dates: list[str]) -> dict[str, list[str]]:
    return {
        "all_available": dates,
        "last_40": dates[-40:],
        "last_20": dates[-20:],
        "last_10": dates[-10:],
        "last_5": dates[-5:],
    }


def _md_table(df: pd.DataFrame, max_rows: int = 80) -> str:
    if df.empty:
        return "_No rows._"
    return df.head(max_rows).to_markdown(index=False)


def validate(
    modules: list[str],
    dates: list[str],
    out_root: Path,
    json_dir: Path,
    force: bool,
    workers: int = 1,
) -> dict[str, Any]:
    out_root.mkdir(parents=True, exist_ok=True)
    run_rows: list[dict[str, Any]] = []
    tasks = [(module, day) for module in modules for day in dates]
    workers = max(1, int(workers))

    def run_task(module: str, day: str) -> dict[str, Any]:
        ok, detail, out_dir = _run_one(module, day, out_root, json_dir, force)
        return {"module": module, "date": day, "ok": ok, "detail": detail, "out_dir": str(out_dir)}

    with ThreadPoolExecutor(max_workers=workers) as pool:
        future_map = {pool.submit(run_task, module, day): (module, day) for module, day in tasks}
        completed = 0
        for future in as_completed(future_map):
            module, day = future_map[future]
            row = future.result()
            run_rows.append(row)
            completed += 1
            print(f"[{completed}/{len(tasks)}] {module} {day} {row['detail']}", flush=True)
            if not row["ok"]:
                raise RuntimeError(f"{module} {day} failed: {row['detail']}")
    run_rows.sort(key=lambda row: (row["module"], row["date"]))
    run_log = pd.DataFrame(run_rows)
    run_log.to_csv(out_root / "run_log.csv", index=False)

    all_trades = pd.concat([_load_trades(module, dates, out_root) for module in modules], ignore_index=True, sort=False)
    all_trades = _add_time_bucket(all_trades)
    all_trades.to_csv(out_root / "all_trades.csv", index=False)

    daily_all = pd.concat([_daily_for_module(all_trades, module, dates) for module in modules], ignore_index=True, sort=False)
    daily_all.to_csv(out_root / "daily_all.csv", index=False)

    summary_rows: list[dict[str, Any]] = []
    for window, w_dates in _window_dates(dates).items():
        for module in modules:
            mod_trades = all_trades[
                all_trades["module"].eq(module) & all_trades["date"].astype(str).isin(w_dates)
            ].copy() if not all_trades.empty else pd.DataFrame()
            mod_daily = _daily_for_module(all_trades, module, w_dates)
            summary_rows.append(_summary(module, mod_trades, mod_daily, w_dates, window))
    summary = pd.DataFrame(summary_rows)
    summary.to_csv(out_root / "window_summary.csv", index=False)

    by_setup = _group_summary(all_trades, ["module", "side", "setup_name"])
    by_side = _group_summary(all_trades, ["module", "side"])
    by_time = _group_summary(all_trades, ["module", "time_bucket"])
    by_setup.to_csv(out_root / "by_setup_all.csv", index=False)
    by_side.to_csv(out_root / "by_side_all.csv", index=False)
    by_time.to_csv(out_root / "by_time_all.csv", index=False)

    report = [
        "# V11 Lab Book Validation",
        "",
        f"- Dates: `{dates[0]}`..`{dates[-1]}` ({len(dates)} usable live-snapshot sessions)",
        f"- Modules: `{', '.join(modules)}`",
        "- Mode: V11 live-parity, next 1-minute open entry model.",
        "- P&L note: v11 live-parity output is price P&L from the backtester; use shadow/paper comparison before promotion.",
        "",
        "## Window Summary",
        _md_table(summary.sort_values(["window", "net_pnl_rs"], ascending=[True, False]), 200),
        "",
        "## Setup Summary - All Available",
        _md_table(by_setup, 120),
        "",
        "## Side Summary - All Available",
        _md_table(by_side, 40),
        "",
        "## Time Window Summary - All Available",
        _md_table(by_time, 80),
        "",
        "## Guardrail",
        "No V7 live/paper code or production config is modified by this validator.",
        "",
    ]
    (out_root / "validation_report.md").write_text("\n".join(report), encoding="utf-8", errors="replace")
    payload = {
        "out_root": str(out_root),
        "dates": dates,
        "modules": modules,
        "summary": summary.replace([np.inf, -np.inf], np.nan).to_dict(orient="records"),
    }
    (out_root / "validation_summary.json").write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return payload


def main(argv: Iterable[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--modules", nargs="+", default=[
        "final_setup_conf_v11_conf_a",
        "final_setup_conf_v11_conf_b",
        "final_setup_conf_v11_conf_c",
        "final_setup_conf_v11_conf_d",
    ])
    ap.add_argument("--live-candidate-json-dir", default=str(DEFAULT_JSON_DIR))
    ap.add_argument("--out-root", default=str(DEFAULT_OUT_ROOT))
    ap.add_argument("--date-from", default="2026-05-21")
    ap.add_argument("--date-to", default=dt.date.today().isoformat())
    ap.add_argument("--skip-date", action="append", default=sorted(DEFAULT_SKIP_DATES))
    ap.add_argument("--force", action="store_true")
    ap.add_argument("--workers", type=int, default=1, help="Parallel isolated module/date runs")
    args = ap.parse_args(list(argv) if argv is not None else None)
    json_dir = Path(args.live_candidate_json_dir)
    skip_dates = set(args.skip_date or [])
    dates = discover_dates(json_dir, str(args.date_from), str(args.date_to), skip_dates)
    if not dates:
        raise SystemExit("No candidate JSON dates found for validation window.")
    validate(
        [str(m) for m in args.modules], dates, Path(args.out_root), json_dir,
        bool(args.force), workers=int(args.workers),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
