from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from eqidv2_runtime_paths import LIVE_SIGNALS_DIR, runtime_dir


BASE_DIR = Path(__file__).resolve().parent
PYTHON_EXE = Path(sys.executable)
OUT_ROOT = runtime_dir("backtesting_result_v11")
LATEST_DIR = OUT_ROOT / "latest"
REPORTS_DIR = OUT_ROOT / "reports"
V11_SCRIPT = BASE_DIR / "avwap_5min_ID_v11_backtesting.py"
LIVE_CANDIDATE_JSON_DIR = runtime_dir("signal_discovery_v7_5mins_ID", "json")
BACKTEST_PNL_COL = "v6_net_pnl_rs"

for _p in (OUT_ROOT, LATEST_DIR, REPORTS_DIR):
    _p.mkdir(parents=True, exist_ok=True)


def _read_csv(path: Path) -> pd.DataFrame:
    if not path.exists() or path.stat().st_size <= 2:
        return pd.DataFrame()
    try:
        return pd.read_csv(path, low_memory=False)
    except Exception:
        return pd.DataFrame()


def _safe_float(value: Any, default: float = np.nan) -> float:
    try:
        return float(value)
    except Exception:
        return float(default)


def _profit_factor(pnl: pd.Series) -> float:
    values = pd.to_numeric(pnl, errors="coerce").fillna(0.0)
    gp = float(values[values > 0].sum())
    gl = float(-values[values < 0].sum())
    if gl <= 0:
        return np.inf if gp > 0 else np.nan
    return gp / gl


def _fmt_num(value: Any, digits: int = 2) -> str:
    val = _safe_float(value)
    if np.isinf(val):
        return "inf"
    if not np.isfinite(val):
        return "NA"
    return f"{val:,.{digits}f}"


def _fmt_pf(value: Any) -> str:
    return _fmt_num(value, 2)


def _paper_path(day: str) -> Path:
    return LIVE_SIGNALS_DIR / f"paper_trades_{day}_id_5min_v7.csv"


def _signal_key(row: pd.Series, *, source: str) -> str:
    ticker = str(row.get("ticker", "")).upper().strip()
    side = str(row.get("side", "")).upper().strip()
    setup = str(row.get("setup", "")).strip()
    if source == "v11":
        ts = row.get("signal_time_v8", row.get("signal_time_ist", ""))
    else:
        ts = row.get("signal_entry_datetime_ist", row.get("signal_datetime", ""))
    parsed = pd.to_datetime(ts, errors="coerce")
    if pd.notna(parsed):
        if parsed.tzinfo is None:
            parsed = parsed.tz_localize("Asia/Kolkata")
        else:
            parsed = parsed.tz_convert("Asia/Kolkata")
        ts_text = parsed.strftime("%Y-%m-%d %H:%M:%S%z")
    else:
        ts_text = str(ts)
    return f"{ticker}|{side}|{setup}|{ts_text}"


def _signal_components(row: pd.Series, *, source: str) -> dict[str, Any]:
    if source == "v11":
        ts = row.get("signal_time_v8", row.get("signal_time_ist", ""))
    else:
        ts = row.get("signal_entry_datetime_ist", row.get("signal_datetime", ""))
    parsed = pd.to_datetime(ts, errors="coerce")
    if pd.notna(parsed):
        parsed = pd.Timestamp(parsed)
        if parsed.tzinfo is None:
            parsed = parsed.tz_localize("Asia/Kolkata")
        else:
            parsed = parsed.tz_convert("Asia/Kolkata")
    return {
        "_cmp_ticker": str(row.get("ticker", "")).upper().strip(),
        "_cmp_side": str(row.get("side", "")).upper().strip(),
        "_cmp_setup": str(row.get("setup", "")).strip(),
        "_cmp_ts": parsed,
    }


def _add_compare_columns(df: pd.DataFrame, *, source: str) -> pd.DataFrame:
    if df.empty:
        return df
    out = df.copy()
    comps = [_signal_components(row, source=source) for _, row in out.iterrows()]
    for col in ("_cmp_ticker", "_cmp_side", "_cmp_setup", "_cmp_ts"):
        out[col] = [item[col] for item in comps]
    out["_compare_key"] = [_signal_key(row, source=source) for _, row in out.iterrows()]
    return out


def _nearest_match_rows(
    left: pd.DataFrame,
    right: pd.DataFrame,
    *,
    left_label: str,
    right_label: str,
    left_pnl_col: str,
    right_pnl_col: str,
) -> pd.DataFrame:
    columns = [
        f"{left_label}_compare_key",
        "ticker",
        "side",
        "setup",
        f"{left_label}_signal_time",
        f"{left_label}_pnl_rs",
        f"nearest_{right_label}_compare_key",
        f"nearest_{right_label}_signal_time",
        f"nearest_{right_label}_pnl_rs",
        "nearest_delta_sec",
        "within_5min",
        "within_15min",
    ]
    if left.empty:
        return pd.DataFrame(columns=columns)
    if right.empty:
        rows = []
        for _, row in left.iterrows():
            rows.append(
                {
                    f"{left_label}_compare_key": row.get("_compare_key", ""),
                    "ticker": row.get("_cmp_ticker", row.get("ticker", "")),
                    "side": row.get("_cmp_side", row.get("side", "")),
                    "setup": row.get("_cmp_setup", row.get("setup", "")),
                    f"{left_label}_signal_time": row.get("_cmp_ts", ""),
                    f"{left_label}_pnl_rs": _safe_float(row.get(left_pnl_col, 0.0), 0.0),
                    f"nearest_{right_label}_compare_key": "",
                    f"nearest_{right_label}_signal_time": "",
                    f"nearest_{right_label}_pnl_rs": np.nan,
                    "nearest_delta_sec": np.nan,
                    "within_5min": False,
                    "within_15min": False,
                }
            )
        return pd.DataFrame(rows, columns=columns)

    rows: list[dict[str, Any]] = []
    right_work = right.copy()
    for _, row in left.iterrows():
        mask = (
            right_work["_cmp_ticker"].astype(str).eq(str(row.get("_cmp_ticker", "")))
            & right_work["_cmp_side"].astype(str).eq(str(row.get("_cmp_side", "")))
            & right_work["_cmp_setup"].astype(str).eq(str(row.get("_cmp_setup", "")))
        )
        candidates = right_work.loc[mask].copy()
        left_ts = row.get("_cmp_ts", pd.NaT)
        nearest = pd.Series(dtype=object)
        delta = np.nan
        if pd.notna(left_ts) and not candidates.empty:
            candidates["_delta_sec"] = [
                abs((ts - left_ts).total_seconds()) if pd.notna(ts) else np.nan
                for ts in candidates["_cmp_ts"]
            ]
            candidates = candidates.sort_values(["_delta_sec", "_compare_key"], ascending=[True, True])
            nearest = candidates.iloc[0]
            delta = _safe_float(nearest.get("_delta_sec"), np.nan)
        rows.append(
            {
                f"{left_label}_compare_key": row.get("_compare_key", ""),
                "ticker": row.get("_cmp_ticker", row.get("ticker", "")),
                "side": row.get("_cmp_side", row.get("side", "")),
                "setup": row.get("_cmp_setup", row.get("setup", "")),
                f"{left_label}_signal_time": left_ts,
                f"{left_label}_pnl_rs": _safe_float(row.get(left_pnl_col, 0.0), 0.0),
                f"nearest_{right_label}_compare_key": nearest.get("_compare_key", "") if not nearest.empty else "",
                f"nearest_{right_label}_signal_time": nearest.get("_cmp_ts", "") if not nearest.empty else "",
                f"nearest_{right_label}_pnl_rs": _safe_float(nearest.get(right_pnl_col, np.nan), np.nan) if not nearest.empty else np.nan,
                "nearest_delta_sec": delta,
                "within_5min": bool(np.isfinite(delta) and delta <= 300),
                "within_15min": bool(np.isfinite(delta) and delta <= 900),
            }
        )
    return pd.DataFrame(rows, columns=columns)


def _summary(df: pd.DataFrame, pnl_col: str) -> dict[str, Any]:
    if df.empty or pnl_col not in df.columns:
        return {"trades": 0, "wins": 0, "losses": 0, "net": 0.0, "pf": np.nan}
    pnl = pd.to_numeric(df[pnl_col], errors="coerce").fillna(0.0)
    return {
        "trades": int(len(df)),
        "wins": int((pnl > 0).sum()),
        "losses": int((pnl < 0).sum()),
        "net": float(pnl.sum()),
        "pf": _profit_factor(pnl),
    }


def _run_v11_live_parity(day: str, out_dir: Path) -> tuple[int, str]:
    cmd = [
        str(PYTHON_EXE),
        "-u",
        str(V11_SCRIPT),
        "--mode",
        "live_parity",
        "--live_date",
        day,
        "--live_candidate_json_dir",
        str(LIVE_CANDIDATE_JSON_DIR),
        "--out",
        str(out_dir),
    ]
    result = subprocess.run(cmd, cwd=str(BASE_DIR), capture_output=True, text=True)
    return int(result.returncode), (result.stdout or "") + (result.stderr or "")


def build_report(day: str, *, run_v11: bool = True) -> int:
    out_dir = OUT_ROOT / day
    out_dir.mkdir(parents=True, exist_ok=True)
    log_text = ""
    rc = 0
    if run_v11:
        rc, log_text = _run_v11_live_parity(day, out_dir)
        (out_dir / "v11_live_parity_run.log").write_text(log_text, encoding="utf-8")

    v11 = _read_csv(out_dir / "trades.csv")
    paper = _read_csv(_paper_path(day))
    v11_summary = _summary(v11, BACKTEST_PNL_COL)
    paper_summary = _summary(paper, "pnl_rs")

    v11 = _add_compare_columns(v11, source="v11")
    paper = _add_compare_columns(paper, source="paper")

    v11_keys = set(v11.get("_compare_key", pd.Series(dtype=str)).dropna().astype(str))
    paper_keys = set(paper.get("_compare_key", pd.Series(dtype=str)).dropna().astype(str))
    only_v11 = v11.loc[v11.get("_compare_key", pd.Series(dtype=str)).isin(sorted(v11_keys - paper_keys))].copy() if not v11.empty else pd.DataFrame()
    only_paper = paper.loc[paper.get("_compare_key", pd.Series(dtype=str)).isin(sorted(paper_keys - v11_keys))].copy() if not paper.empty else pd.DataFrame()
    matched = v11.loc[v11.get("_compare_key", pd.Series(dtype=str)).isin(sorted(v11_keys & paper_keys))].copy() if not v11.empty else pd.DataFrame()
    matched_detail = (
        v11.merge(paper, on="_compare_key", suffixes=("_v11", "_paper"))
        if not v11.empty and not paper.empty and (v11_keys & paper_keys)
        else pd.DataFrame()
    )
    if not matched_detail.empty:
        matched_detail["v11_pnl_rs"] = pd.to_numeric(matched_detail.get(BACKTEST_PNL_COL), errors="coerce").fillna(0.0)
        matched_detail["paper_pnl_rs"] = pd.to_numeric(matched_detail.get("pnl_rs"), errors="coerce").fillna(0.0)
        matched_detail["pnl_gap_v11_minus_paper_rs"] = matched_detail["v11_pnl_rs"] - matched_detail["paper_pnl_rs"]
        matched_detail["v11_entry_price"] = pd.to_numeric(matched_detail.get("entry_price_v6"), errors="coerce")
        matched_detail["paper_entry_price"] = pd.to_numeric(matched_detail.get("entry_price"), errors="coerce")
        matched_detail["entry_price_gap_v11_minus_paper"] = matched_detail["v11_entry_price"] - matched_detail["paper_entry_price"]
    paper_nearest_v11 = _nearest_match_rows(
        only_paper,
        v11,
        left_label="paper",
        right_label="v11",
        left_pnl_col="pnl_rs",
        right_pnl_col=BACKTEST_PNL_COL,
    )
    v11_nearest_paper = _nearest_match_rows(
        only_v11,
        paper,
        left_label="v11",
        right_label="paper",
        left_pnl_col=BACKTEST_PNL_COL,
        right_pnl_col="pnl_rs",
    )
    near_paper_5m = int(paper_nearest_v11.get("within_5min", pd.Series(dtype=bool)).sum()) if not paper_nearest_v11.empty else 0
    near_paper_15m = int(paper_nearest_v11.get("within_15min", pd.Series(dtype=bool)).sum()) if not paper_nearest_v11.empty else 0
    near_v11_5m = int(v11_nearest_paper.get("within_5min", pd.Series(dtype=bool)).sum()) if not v11_nearest_paper.empty else 0
    near_v11_15m = int(v11_nearest_paper.get("within_15min", pd.Series(dtype=bool)).sum()) if not v11_nearest_paper.empty else 0
    matched_pnl_gap = (
        float(matched_detail["pnl_gap_v11_minus_paper_rs"].sum())
        if not matched_detail.empty and "pnl_gap_v11_minus_paper_rs" in matched_detail.columns
        else 0.0
    )

    by_setup_lines: list[str] = []
    if not v11.empty and BACKTEST_PNL_COL in v11.columns:
        tmp = v11.copy()
        tmp[BACKTEST_PNL_COL] = pd.to_numeric(tmp[BACKTEST_PNL_COL], errors="coerce").fillna(0.0)
        for (side, setup), group in tmp.groupby(["side", "setup"], dropna=False):
            s = _summary(group, BACKTEST_PNL_COL)
            by_setup_lines.append(
                f"| {side} | {setup} | {s['trades']} | {s['wins']} | Rs {_fmt_num(s['net'])} | {_fmt_pf(s['pf'])} |"
            )

    discrepancy_reasons = [
        "v11 live_parity replays signal-discovery candidates through the v11 live-parity gate; paper TRUE executes final live signal CSVs.",
        "paper TRUE may reject via PF gate, risk/capacity, late detection, or entry retry logic.",
        "v11 uses first 1-minute open at/after signal and model exits; paper TRUE uses live executor timing and LTP/entry handling.",
        "If same ticker has multiple candidates, v11 live_parity dedupes one ticker per day to match live entry guard.",
    ]

    fix_suggestions = [
        "Do not edit avwap_5min_ID_v11_backtesting.py automatically. Review this report first.",
        "If v11-only trades are valid paper skips, add the paper skip reason into the comparison report, not the backtest logic.",
        "If paper-only trades are missing from v11, check candidate snapshot loading and live_date filtering in live_parity mode.",
        "Use paper_only_nearest_v11.csv and v11_only_nearest_paper.csv to separate timestamp drift from true candidate mismatch.",
        "Use matched_v11_vs_paper_detail.csv for exact-match entry/PnL drift before changing v11 execution assumptions.",
        "If PnL differs on matched trades, compare entry_time/entry_price and decide whether v11 should model paper TRUE entry behavior more closely.",
        "If setup totals differ consistently, check accepted_rules/gate parity before changing strategy rules.",
    ]

    lines = [
        f"# Backtesting Result v11 - {day}",
        "",
        "Report-only mode. No changes are made to `avwap_5min_ID_v11_backtesting.py`.",
        "",
        "## Run Status",
        "",
        f"- v11 live_parity exit code: {rc}",
        f"- v11 output dir: {out_dir}",
        f"- paper TRUE file: {_paper_path(day)}",
        "",
        "## Result Comparison",
        "",
        "| source | trades | wins | losses | net pnl | PF |",
        "|---|---:|---:|---:|---:|---:|",
        f"| v11 live_parity day-only | {v11_summary['trades']} | {v11_summary['wins']} | {v11_summary['losses']} | Rs {_fmt_num(v11_summary['net'])} | {_fmt_pf(v11_summary['pf'])} |",
        f"| V7 paper TRUE live | {paper_summary['trades']} | {paper_summary['wins']} | {paper_summary['losses']} | Rs {_fmt_num(paper_summary['net'])} | {_fmt_pf(paper_summary['pf'])} |",
        "",
        "## Discrepancy Counts",
        "",
        f"- Matched signal keys: {len(v11_keys & paper_keys)}",
        f"- v11-only trades: {len(v11_keys - paper_keys)}",
        f"- paper-only trades: {len(paper_keys - v11_keys)}",
        f"- Paper-only rows with nearest v11 same ticker/side/setup within 5 minutes: {near_paper_5m}",
        f"- Paper-only rows with nearest v11 same ticker/side/setup within 15 minutes: {near_paper_15m}",
        f"- v11-only rows with nearest paper same ticker/side/setup within 5 minutes: {near_v11_5m}",
        f"- v11-only rows with nearest paper same ticker/side/setup within 15 minutes: {near_v11_15m}",
        f"- Exact-match PnL gap, v11 minus paper: Rs {_fmt_num(matched_pnl_gap)}",
        "",
        "## V11 By Setup",
        "",
    ]
    if by_setup_lines:
        lines.extend(["| side | setup | trades | wins | net pnl | PF |", "|---|---|---:|---:|---:|---:|"])
        lines.extend(by_setup_lines)
    else:
        lines.append("No v11 setup rows.")
    lines.extend(["", "## Likely Discrepancy Causes", ""])
    lines.extend([f"- {x}" for x in discrepancy_reasons])
    lines.extend(["", "## Suggested Fix Direction For avwap_5min_ID_v11_backtesting.py", ""])
    lines.extend([f"- {x}" for x in fix_suggestions])

    if rc != 0:
        lines.extend(["", "## v11 Run Log Tail", ""])
        tail = "\n".join(log_text.splitlines()[-40:])
        lines.append("```text")
        lines.append(tail)
        lines.append("```")

    report = "\n".join(lines) + "\n"
    report_path = REPORTS_DIR / f"backtesting_result_v11_{day}.md"
    report_path.write_text(report, encoding="utf-8")
    (LATEST_DIR / "latest_backtesting_result_v11.md").write_text(report, encoding="utf-8")
    (LATEST_DIR / "latest_backtesting_result_v11.json").write_text(
        json.dumps(
            {
                "day": day,
                "exit_code": rc,
                "v11_summary": v11_summary,
                "paper_summary": paper_summary,
                "matched": len(v11_keys & paper_keys),
                "v11_only": len(v11_keys - paper_keys),
                "paper_only": len(paper_keys - v11_keys),
                "paper_only_nearest_v11_within_5min": near_paper_5m,
                "paper_only_nearest_v11_within_15min": near_paper_15m,
                "v11_only_nearest_paper_within_5min": near_v11_5m,
                "v11_only_nearest_paper_within_15min": near_v11_15m,
                "matched_pnl_gap_v11_minus_paper_rs": matched_pnl_gap,
                "report": str(report_path),
                "out_dir": str(out_dir),
                "backtesting_script": str(V11_SCRIPT),
            },
            indent=2,
            sort_keys=True,
            default=str,
        ),
        encoding="utf-8",
    )
    only_v11.to_csv(out_dir / "v11_only_vs_paper.csv", index=False)
    only_paper.to_csv(out_dir / "paper_only_vs_v11.csv", index=False)
    matched.to_csv(out_dir / "matched_v11_vs_paper.csv", index=False)
    matched_detail.to_csv(out_dir / "matched_v11_vs_paper_detail.csv", index=False)
    paper_nearest_v11.to_csv(out_dir / "paper_only_nearest_v11.csv", index=False)
    v11_nearest_paper.to_csv(out_dir / "v11_only_nearest_paper.csv", index=False)
    print(report)
    return rc


def _default_day() -> str:
    return pd.Timestamp.now(tz="Asia/Kolkata").strftime("%Y-%m-%d")


def main() -> int:
    ap = argparse.ArgumentParser(description="Run day-only v11 live parity and compare with V7 paper TRUE.")
    ap.add_argument("--date", default=_default_day())
    ap.add_argument("--no-run-v11", action="store_true")
    args = ap.parse_args()
    return build_report(str(args.date), run_v11=not args.no_run_v11)


if __name__ == "__main__":
    raise SystemExit(main())
