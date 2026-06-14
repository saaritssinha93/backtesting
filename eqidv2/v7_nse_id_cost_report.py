"""
Daily V7 NSE ID cost report.

Reads the V7 ID 5-minute paper-trade CSV, applies the NSE intraday equity cost
model, and writes dashboard-ready Markdown/JSON/CSV artifacts.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from eqidv2_runtime_paths import LIVE_SIGNALS_DIR, RUNTIME_STATUS_DIR, runtime_dir
from nse_intraday_costs import CostConfig, intraday_equity_costs


SESSION_NAME = "V7 NSE ID Cost"
SESSION_SLUG = "v7_nse_id_cost"
SESSION_ROOT = runtime_dir(SESSION_SLUG)
LATEST_DIR = SESSION_ROOT / "latest"
REPORTS_DIR = SESSION_ROOT / "reports"
HEARTBEAT_DIR = SESSION_ROOT / "heartbeat"

for _path in (SESSION_ROOT, LATEST_DIR, REPORTS_DIR, HEARTBEAT_DIR, RUNTIME_STATUS_DIR):
    _path.mkdir(parents=True, exist_ok=True)


def _now_ist() -> pd.Timestamp:
    return pd.Timestamp.now(tz="Asia/Kolkata")


def _fmt_ts(value: Any) -> str:
    ts = pd.to_datetime(value, errors="coerce")
    if pd.isna(ts):
        return ""
    if ts.tzinfo is None:
        ts = ts.tz_localize("Asia/Kolkata")
    else:
        ts = ts.tz_convert("Asia/Kolkata")
    offset = ts.strftime("%z")
    return f"{ts.strftime('%Y-%m-%d %H:%M:%S')}{offset[:3]}:{offset[3:]}"


def _fmt_num(value: Any, digits: int = 2) -> str:
    try:
        x = float(value)
    except Exception:
        return ""
    if not np.isfinite(x):
        return ""
    return f"{x:.{digits}f}"


def _read_csv(path: Path) -> pd.DataFrame:
    if not path.exists() or path.stat().st_size <= 2:
        return pd.DataFrame()
    try:
        return pd.read_csv(path)
    except Exception:
        return _read_csv_lenient(path)


def _read_csv_lenient(path: Path) -> pd.DataFrame:
    """Read mixed-schema paper-trade CSVs without silently losing the day.

    On 2026-06-10 the paper executor appended rows with the newly added
    ``initial_stop_price`` field to a file that still had the older header.
    That makes strict CSV parsers reject the file even though the trade rows
    are usable. Normalize that specific compatible schema drift here.
    """
    try:
        with path.open("r", newline="", encoding="utf-8-sig") as fh:
            reader = csv.reader(fh)
            header = next(reader, [])
            if not header:
                return pd.DataFrame()
            rows = list(reader)
    except Exception:
        return pd.DataFrame()

    effective_header = list(header)
    insert_initial_stop_at: int | None = None
    if "initial_stop_price" not in effective_header and "target_price" in effective_header:
        insert_initial_stop_at = effective_header.index("target_price")
        effective_header = (
            effective_header[:insert_initial_stop_at]
            + ["initial_stop_price"]
            + effective_header[insert_initial_stop_at:]
        )

    normalized_rows: list[list[str]] = []
    for row in rows:
        work = list(row)
        if insert_initial_stop_at is not None and len(work) == len(header):
            work = work[:insert_initial_stop_at] + [""] + work[insert_initial_stop_at:]
        if len(work) < len(effective_header):
            work = work + [""] * (len(effective_header) - len(work))
        elif len(work) > len(effective_header):
            work = work[: len(effective_header)]
        normalized_rows.append(work)

    return pd.DataFrame(normalized_rows, columns=effective_header)


def _write_text_atomic(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f"{path.name}.tmp.{os.getpid()}")
    tmp.write_text(text, encoding="utf-8")
    tmp.replace(path)


def _write_json_atomic(path: Path, payload: dict[str, Any]) -> None:
    _write_text_atomic(path, json.dumps(payload, indent=2, sort_keys=True, default=str))


def _write_status(state: str, **extra: Any) -> None:
    payload = {
        "session": SESSION_NAME,
        "session_slug": SESSION_SLUG,
        "status": state,
        "pid": os.getpid(),
        "updated_at_ist": _fmt_ts(_now_ist()),
        **extra,
    }
    text = "\n".join(f"{k}={v}" for k, v in payload.items()) + "\n"
    _write_text_atomic(RUNTIME_STATUS_DIR / f"{SESSION_SLUG}.status", text)
    _write_text_atomic(RUNTIME_STATUS_DIR / f"{SESSION_SLUG}.heartbeat", text)
    _write_json_atomic(HEARTBEAT_DIR / f"{SESSION_SLUG}.status.json", payload)
    _write_json_atomic(HEARTBEAT_DIR / f"{SESSION_SLUG}.heartbeat.json", payload)


def _safe_float(value: Any, default: float = np.nan) -> float:
    try:
        x = float(value)
    except Exception:
        return default
    return x if np.isfinite(x) else default


def _profit_factor(values: pd.Series) -> float:
    pnl = pd.to_numeric(values, errors="coerce").fillna(0.0)
    gross_profit = float(pnl[pnl > 0].sum())
    gross_loss = float(-pnl[pnl < 0].sum())
    if gross_loss <= 0:
        return np.inf if gross_profit > 0 else np.nan
    return gross_profit / gross_loss


def _cost_paper_trades(df: pd.DataFrame, cfg: CostConfig) -> pd.DataFrame:
    if df.empty:
        return df
    out = df.copy()
    for col in (
        "v7_nse_id_gross_pnl_rs",
        "v7_nse_id_brokerage_rs",
        "v7_nse_id_stt_rs",
        "v7_nse_id_exch_txn_rs",
        "v7_nse_id_sebi_rs",
        "v7_nse_id_ipft_rs",
        "v7_nse_id_stamp_rs",
        "v7_nse_id_gst_rs",
        "v7_nse_id_total_cost_rs",
        "v7_nse_id_net_pnl_rs",
        "v7_nse_id_cost_bps_of_turnover",
        "v7_nse_id_cost_pct_of_entry",
    ):
        out[col] = np.nan
    out["v7_nse_id_cost_error"] = ""

    outcome = out.get("outcome", pd.Series("", index=out.index)).fillna("").astype(str).str.upper()
    entry = pd.to_numeric(out.get("entry_price", pd.Series(dtype=float)), errors="coerce")
    exit_ = pd.to_numeric(out.get("exit_price", pd.Series(dtype=float)), errors="coerce")
    qty = pd.to_numeric(out.get("quantity", pd.Series(dtype=float)), errors="coerce")
    side = out.get("side", pd.Series("", index=out.index)).fillna("").astype(str).str.upper().str.strip()
    eligible = (
        ~outcome.str.startswith("ENTRY_SKIPPED")
        & entry.gt(0)
        & exit_.gt(0)
        & qty.gt(0)
        & side.isin(["LONG", "SHORT"])
    )
    for idx in out.index[eligible]:
        try:
            b = intraday_equity_costs(float(entry.loc[idx]), float(exit_.loc[idx]), float(qty.loc[idx]), str(side.loc[idx]), cfg)
        except Exception as exc:
            out.at[idx, "v7_nse_id_cost_error"] = f"{type(exc).__name__}: {exc}"
            continue
        out.at[idx, "v7_nse_id_gross_pnl_rs"] = b.gross_pnl
        out.at[idx, "v7_nse_id_brokerage_rs"] = b.brokerage
        out.at[idx, "v7_nse_id_stt_rs"] = b.stt
        out.at[idx, "v7_nse_id_exch_txn_rs"] = b.exch_txn
        out.at[idx, "v7_nse_id_sebi_rs"] = b.sebi
        out.at[idx, "v7_nse_id_ipft_rs"] = b.ipft
        out.at[idx, "v7_nse_id_stamp_rs"] = b.stamp
        out.at[idx, "v7_nse_id_gst_rs"] = b.gst
        out.at[idx, "v7_nse_id_total_cost_rs"] = b.total_cost
        out.at[idx, "v7_nse_id_net_pnl_rs"] = b.net_pnl
        out.at[idx, "v7_nse_id_cost_bps_of_turnover"] = b.cost_bps_of_turnover
        out.at[idx, "v7_nse_id_cost_pct_of_entry"] = b.cost_pct_of_entry
    return out


def _per_setup_cost_stats(costed: pd.DataFrame) -> pd.DataFrame:
    """Cost breakdown by setup — shows which setups have highest cost drag."""
    if costed.empty:
        return pd.DataFrame()
    costed_mask = pd.to_numeric(costed.get("v7_nse_id_total_cost_rs", pd.Series(dtype=float)), errors="coerce").notna()
    traded = costed.loc[costed_mask].copy()
    if traded.empty:
        return pd.DataFrame()
    traded["setup"] = traded.get("setup", pd.Series("", index=traded.index)).fillna("UNKNOWN").astype(str)
    traded["_gross"] = pd.to_numeric(traded.get("v7_nse_id_gross_pnl_rs"), errors="coerce").fillna(0.0)
    traded["_net"] = pd.to_numeric(traded.get("v7_nse_id_net_pnl_rs"), errors="coerce").fillna(0.0)
    traded["_cost"] = pd.to_numeric(traded.get("v7_nse_id_total_cost_rs"), errors="coerce").fillna(0.0)
    traded["_cost_bps"] = pd.to_numeric(traded.get("v7_nse_id_cost_bps_of_turnover"), errors="coerce")
    rows: list[dict[str, Any]] = []
    for setup, grp in traded.groupby("setup"):
        gross_pf = _profit_factor(grp["_gross"])
        net_pf = _profit_factor(grp["_net"])
        rows.append(
            {
                "setup": setup,
                "trades": int(len(grp)),
                "gross_pnl_rs": round(float(grp["_gross"].sum()), 2),
                "total_cost_rs": round(float(grp["_cost"].sum()), 2),
                "net_pnl_rs": round(float(grp["_net"].sum()), 2),
                "gross_pf": _fmt_num(gross_pf, 3) if np.isfinite(gross_pf) else "inf",
                "net_pf": _fmt_num(net_pf, 3) if np.isfinite(net_pf) else "inf",
                "avg_cost_bps": _fmt_num(grp["_cost_bps"].mean(), 2),
                "cost_pct_of_gross": _fmt_num(100.0 * float(grp["_cost"].sum()) / max(abs(float(grp["_gross"].sum())), 0.01), 2),
            }
        )
    return pd.DataFrame(rows).sort_values("trades", ascending=False).reset_index(drop=True)


def _cost_by_side(costed: pd.DataFrame) -> pd.DataFrame:
    """LONG vs SHORT cost comparison — SHORT incurs STT on both legs."""
    if costed.empty:
        return pd.DataFrame()
    costed_mask = pd.to_numeric(costed.get("v7_nse_id_total_cost_rs", pd.Series(dtype=float)), errors="coerce").notna()
    traded = costed.loc[costed_mask].copy()
    if traded.empty:
        return pd.DataFrame()
    traded["_side"] = traded.get("side", pd.Series("", index=traded.index)).fillna("").astype(str).str.upper()
    traded["_net"] = pd.to_numeric(traded.get("v7_nse_id_net_pnl_rs"), errors="coerce").fillna(0.0)
    traded["_cost"] = pd.to_numeric(traded.get("v7_nse_id_total_cost_rs"), errors="coerce").fillna(0.0)
    traded["_stt"] = pd.to_numeric(traded.get("v7_nse_id_stt_rs"), errors="coerce").fillna(0.0)
    traded["_cost_bps"] = pd.to_numeric(traded.get("v7_nse_id_cost_bps_of_turnover"), errors="coerce")
    rows: list[dict[str, Any]] = []
    for side in ("LONG", "SHORT"):
        grp = traded.loc[traded["_side"].eq(side)]
        if grp.empty:
            continue
        rows.append(
            {
                "side": side,
                "trades": int(len(grp)),
                "total_cost_rs": round(float(grp["_cost"].sum()), 2),
                "stt_rs": round(float(grp["_stt"].sum()), 2),
                "stt_pct_of_cost": _fmt_num(100.0 * float(grp["_stt"].sum()) / max(float(grp["_cost"].sum()), 0.01), 1),
                "avg_cost_bps": _fmt_num(grp["_cost_bps"].mean(), 2),
                "net_pnl_rs": round(float(grp["_net"].sum()), 2),
                "net_pf": _fmt_num(_profit_factor(grp["_net"]), 3),
            }
        )
    return pd.DataFrame(rows)


def _breakeven_analysis(costed: pd.DataFrame) -> dict[str, Any]:
    """Compute the minimum gross PF needed to achieve net_pf >= 1.0 and >= 1.2."""
    if costed.empty:
        return {}
    costed_mask = pd.to_numeric(costed.get("v7_nse_id_total_cost_rs", pd.Series(dtype=float)), errors="coerce").notna()
    traded = costed.loc[costed_mask].copy()
    if traded.empty:
        return {}
    gross = pd.to_numeric(traded.get("v7_nse_id_gross_pnl_rs"), errors="coerce").fillna(0.0)
    net = pd.to_numeric(traded.get("v7_nse_id_net_pnl_rs"), errors="coerce").fillna(0.0)
    cost = pd.to_numeric(traded.get("v7_nse_id_total_cost_rs"), errors="coerce").fillna(0.0)
    gross_wins = float(gross[gross > 0].sum())
    gross_losses = float(-gross[gross < 0].sum())
    net_wins = float(net[net > 0].sum())
    net_losses = float(-net[net < 0].sum())
    total_cost = float(cost.sum())
    avg_cost_bps = _safe_float(pd.to_numeric(traded.get("v7_nse_id_cost_bps_of_turnover"), errors="coerce").mean())
    # Required gross PF: if net_pf = gross_wins / (gross_losses + total_cost) = 1.0
    # then gross_wins = gross_losses + total_cost → gross_pf = (gross_losses + total_cost) / gross_losses if gross_losses > 0
    req_gross_pf_breakeven = (gross_losses + total_cost) / gross_losses if gross_losses > 0 else np.nan
    req_gross_pf_1_2 = 1.2 * (gross_losses + total_cost / 1.2) / gross_losses if gross_losses > 0 else np.nan
    return {
        "total_cost_rs": round(total_cost, 2),
        "avg_cost_bps": round(avg_cost_bps, 2) if np.isfinite(avg_cost_bps) else None,
        "gross_pf_actual": round(_profit_factor(gross), 3) if np.isfinite(_profit_factor(gross)) else None,
        "net_pf_actual": round(_profit_factor(net), 3) if np.isfinite(_profit_factor(net)) else None,
        "min_gross_pf_for_breakeven": round(req_gross_pf_breakeven, 3) if np.isfinite(req_gross_pf_breakeven) else None,
        "min_gross_pf_for_net_pf_1_2": round(req_gross_pf_1_2, 3) if np.isfinite(req_gross_pf_1_2) else None,
    }


def _load_rolling_cost_summaries(reports_dir: Path, n_days: int = 20) -> pd.DataFrame:
    """Aggregate last N daily JSON summaries to show rolling cost trend."""
    if not reports_dir.exists():
        return pd.DataFrame()
    jsons = sorted(reports_dir.glob("v7_nse_id_cost_*.json"), key=lambda p: p.name, reverse=True)[:n_days]
    rows: list[dict[str, Any]] = []
    for path in reversed(jsons):
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            rows.append(
                {
                    "day": data.get("day", ""),
                    "costed_trades": data.get("costed_trades", 0),
                    "gross_pnl_rs": data.get("gross_pnl_rs", 0),
                    "total_cost_rs": data.get("total_cost_rs", 0),
                    "net_pnl_rs": data.get("net_pnl_rs", 0),
                    "gross_pf": data.get("gross_profit_factor", ""),
                    "net_pf": data.get("net_profit_factor", ""),
                    "avg_cost_bps": data.get("avg_cost_bps_of_turnover", ""),
                }
            )
        except Exception:
            continue
    return pd.DataFrame(rows)


def _summarize(day: str, source_csv: Path, costed: pd.DataFrame, cfg: CostConfig) -> dict[str, Any]:
    costed_mask = pd.to_numeric(costed.get("v7_nse_id_total_cost_rs", pd.Series(dtype=float)), errors="coerce").notna()
    traded = costed.loc[costed_mask].copy() if not costed.empty else pd.DataFrame()
    gross = pd.to_numeric(traded.get("v7_nse_id_gross_pnl_rs", pd.Series(dtype=float)), errors="coerce").fillna(0.0)
    net = pd.to_numeric(traded.get("v7_nse_id_net_pnl_rs", pd.Series(dtype=float)), errors="coerce").fillna(0.0)
    total_cost = pd.to_numeric(traded.get("v7_nse_id_total_cost_rs", pd.Series(dtype=float)), errors="coerce").fillna(0.0)
    outcome = traded.get("outcome", pd.Series(dtype=str)).fillna("").astype(str).str.upper()
    summary = {
        "session": SESSION_NAME,
        "day": day,
        "generated_at_ist": _fmt_ts(_now_ist()),
        "source_csv": str(source_csv),
        "rates_as_of": cfg.rates_as_of,
        "paper_rows": int(len(costed)),
        "costed_trades": int(len(traded)),
        "targets": int(outcome.eq("TARGET").sum()) if not traded.empty else 0,
        "sl": int(outcome.eq("SL").sum()) if not traded.empty else 0,
        "gross_pnl_rs": round(float(gross.sum()), 4),
        "total_cost_rs": round(float(total_cost.sum()), 4),
        "net_pnl_rs": round(float(net.sum()), 4),
        "gross_profit_factor": round(float(_profit_factor(gross)), 4) if np.isfinite(_profit_factor(gross)) else str(_profit_factor(gross)),
        "net_profit_factor": round(float(_profit_factor(net)), 4) if np.isfinite(_profit_factor(net)) else str(_profit_factor(net)),
        "avg_cost_bps_of_turnover": round(float(pd.to_numeric(traded.get("v7_nse_id_cost_bps_of_turnover", pd.Series(dtype=float)), errors="coerce").mean()), 4) if not traded.empty else np.nan,
        "avg_cost_pct_of_entry": round(float(pd.to_numeric(traded.get("v7_nse_id_cost_pct_of_entry", pd.Series(dtype=float)), errors="coerce").mean()), 4) if not traded.empty else np.nan,
    }
    for col, name in (
        ("v7_nse_id_brokerage_rs", "brokerage_rs"),
        ("v7_nse_id_stt_rs", "stt_rs"),
        ("v7_nse_id_exch_txn_rs", "exch_txn_rs"),
        ("v7_nse_id_sebi_rs", "sebi_rs"),
        ("v7_nse_id_ipft_rs", "ipft_rs"),
        ("v7_nse_id_stamp_rs", "stamp_rs"),
        ("v7_nse_id_gst_rs", "gst_rs"),
    ):
        summary[name] = round(float(pd.to_numeric(traded.get(col, pd.Series(dtype=float)), errors="coerce").fillna(0.0).sum()), 4) if not traded.empty else 0.0
    return summary


def _render_markdown(
    summary: dict[str, Any],
    costed: pd.DataFrame,
    setup_stats: pd.DataFrame | None = None,
    side_stats: pd.DataFrame | None = None,
    breakeven: dict[str, Any] | None = None,
    rolling: pd.DataFrame | None = None,
) -> str:
    lines = [
        f"# V7 NSE ID Cost - {summary.get('day', '')}",
        "",
        f"- Generated: {summary.get('generated_at_ist', '')}",
        f"- Source: `{summary.get('source_csv', '')}`",
        f"- Rates as of: `{summary.get('rates_as_of', '')}`",
        "",
        "## Summary",
        "",
        "| metric | value |",
        "|---|---:|",
        f"| paper rows | {summary.get('paper_rows', 0)} |",
        f"| costed trades | {summary.get('costed_trades', 0)} |",
        f"| target / SL | {summary.get('targets', 0)} / {summary.get('sl', 0)} |",
        f"| gross P&L | Rs {_fmt_num(summary.get('gross_pnl_rs'), 2)} |",
        f"| total NSE ID cost | Rs {_fmt_num(summary.get('total_cost_rs'), 2)} |",
        f"| **net P&L** | **Rs {_fmt_num(summary.get('net_pnl_rs'), 2)}** |",
        f"| gross PF / net PF | {_fmt_num(summary.get('gross_profit_factor'), 3)} / {_fmt_num(summary.get('net_profit_factor'), 3)} |",
        f"| avg cost | {_fmt_num(summary.get('avg_cost_bps_of_turnover'), 3)} bps turnover / {_fmt_num(summary.get('avg_cost_pct_of_entry'), 4)}% entry notional |",
        "",
        "## Cost Components",
        "",
        "| component | Rs |",
        "|---|---:|",
        f"| brokerage | {_fmt_num(summary.get('brokerage_rs'), 2)} |",
        f"| STT | {_fmt_num(summary.get('stt_rs'), 2)} |",
        f"| exchange txn | {_fmt_num(summary.get('exch_txn_rs'), 2)} |",
        f"| SEBI | {_fmt_num(summary.get('sebi_rs'), 2)} |",
        f"| IPFT | {_fmt_num(summary.get('ipft_rs'), 2)} |",
        f"| stamp | {_fmt_num(summary.get('stamp_rs'), 2)} |",
        f"| GST | {_fmt_num(summary.get('gst_rs'), 2)} |",
        "",
        "## Trades",
        "",
    ]
    if costed.empty:
        lines.append("No paper trades found.")
        return "\n".join(lines) + "\n"

    view = costed.copy()
    if "entry_time" in view.columns:
        view["_entry_ts"] = pd.to_datetime(view["entry_time"], errors="coerce")
        view = view.sort_values("_entry_ts")
    view = view.tail(40)
    lines.append("| ticker | side | setup | outcome | gross | cost | net | cost bps |")
    lines.append("|---|---|---|---|---:|---:|---:|---:|")
    for _, row in view.iterrows():
        gross = row.get("v7_nse_id_gross_pnl_rs")
        if pd.isna(gross):
            gross = row.get("pnl_rs")
        lines.append(
            f"| {row.get('ticker', '')} | {row.get('side', '')} | {row.get('setup', '')} | "
            f"{row.get('outcome', '')} | {_fmt_num(gross, 2)} | {_fmt_num(row.get('v7_nse_id_total_cost_rs'), 2)} | "
            f"{_fmt_num(row.get('v7_nse_id_net_pnl_rs'), 2)} | {_fmt_num(row.get('v7_nse_id_cost_bps_of_turnover'), 3)} |"
        )

    if setup_stats is not None and not setup_stats.empty:
        lines.extend(["", "## Per-Setup Cost Breakdown", ""])
        lines.append("`cost_pct_of_gross` = total cost / |gross P&L| × 100. High % = cost-heavy setup.")
        lines.append("")
        cols = [c for c in setup_stats.columns]
        lines.append("| " + " | ".join(cols) + " |")
        lines.append("| " + " | ".join("---" for _ in cols) + " |")
        for _, row in setup_stats.iterrows():
            lines.append("| " + " | ".join(str(row.get(c, "")).replace("|", "\\|") for c in cols) + " |")

    if side_stats is not None and not side_stats.empty:
        lines.extend(["", "## LONG vs SHORT Cost Comparison", ""])
        lines.append("SHORT has STT on both entry and exit legs — expect higher cost bps than LONG.")
        lines.append("")
        cols = [c for c in side_stats.columns]
        lines.append("| " + " | ".join(cols) + " |")
        lines.append("| " + " | ".join("---" for _ in cols) + " |")
        for _, row in side_stats.iterrows():
            lines.append("| " + " | ".join(str(row.get(c, "")).replace("|", "\\|") for c in cols) + " |")

    if breakeven:
        lines.extend(["", "## Breakeven Analysis", ""])
        lines.extend(
            [
                f"- Actual gross PF: **{breakeven.get('gross_pf_actual', '')}**",
                f"- Actual net PF: **{breakeven.get('net_pf_actual', '')}**",
                f"- Total cost today: Rs {breakeven.get('total_cost_rs', '')}",
                f"- Avg cost: {breakeven.get('avg_cost_bps', '')} bps of turnover",
                f"- Min gross PF to break even (net PF ≥ 1.0): **{breakeven.get('min_gross_pf_for_breakeven', '')}**",
                f"- Min gross PF for net PF ≥ 1.2: **{breakeven.get('min_gross_pf_for_net_pf_1_2', '')}**",
            ]
        )

    if rolling is not None and not rolling.empty:
        lines.extend(["", "## Rolling Cost Trend (Last 20 Days)", ""])
        cols = [c for c in rolling.columns]
        lines.append("| " + " | ".join(cols) + " |")
        lines.append("| " + " | ".join("---" for _ in cols) + " |")
        for _, row in rolling.iterrows():
            lines.append("| " + " | ".join(str(row.get(c, "")).replace("|", "\\|") for c in cols) + " |")

    return "\n".join(lines) + "\n"


def run(day: str) -> tuple[Path, Path, Path]:
    _write_status("RUNNING", phase="START", day=day)
    cfg = CostConfig()
    source_csv = LIVE_SIGNALS_DIR / f"paper_trades_{day}_id_5min_v7.csv"
    paper = _read_csv(source_csv)
    costed = _cost_paper_trades(paper, cfg)
    summary = _summarize(day, source_csv, costed, cfg)

    latest_csv = LATEST_DIR / "latest_v7_nse_id_cost.csv"
    latest_json = LATEST_DIR / "latest_v7_nse_id_cost.json"
    latest_md = LATEST_DIR / "latest_v7_nse_id_cost.md"
    dated_csv = REPORTS_DIR / f"v7_nse_id_cost_{day}.csv"
    dated_json = REPORTS_DIR / f"v7_nse_id_cost_{day}.json"
    dated_md = REPORTS_DIR / f"v7_nse_id_cost_{day}.md"

    setup_stats = _per_setup_cost_stats(costed)
    side_stats = _cost_by_side(costed)
    breakeven = _breakeven_analysis(costed)
    rolling = _load_rolling_cost_summaries(REPORTS_DIR, n_days=20)

    costed.to_csv(latest_csv, index=False)
    costed.to_csv(dated_csv, index=False)
    _write_json_atomic(latest_json, summary)
    _write_json_atomic(dated_json, summary)
    md = _render_markdown(summary, costed, setup_stats, side_stats, breakeven, rolling)
    _write_text_atomic(latest_md, md)
    _write_text_atomic(dated_md, md)
    _write_status(
        "DONE",
        phase="WROTE_REPORT",
        day=day,
        costed_trades=summary.get("costed_trades", 0),
        gross_pnl_rs=summary.get("gross_pnl_rs", 0),
        total_cost_rs=summary.get("total_cost_rs", 0),
        net_pnl_rs=summary.get("net_pnl_rs", 0),
        report=str(latest_md),
        csv=str(latest_csv),
    )
    return latest_md, latest_json, latest_csv


def _parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description=SESSION_NAME)
    ap.add_argument("--date", default="", help="YYYY-MM-DD; defaults to today IST")
    return ap.parse_args()


def main() -> int:
    args = _parse_args()
    day = str(args.date or _now_ist().strftime("%Y-%m-%d"))
    try:
        md, js, csv = run(day)
    except Exception as exc:
        _write_status("ERROR", phase="FAILED", day=day, error=f"{type(exc).__name__}: {exc}")
        print(f"[v7_nse_id_cost] ERROR {type(exc).__name__}: {exc}", flush=True)
        return 1
    print(f"[v7_nse_id_cost] wrote {md} | {js} | {csv}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
