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
OUT_ROOT = runtime_dir("backtesting_result_v7_v8")
LATEST_DIR = OUT_ROOT / "latest"
REPORTS_DIR = OUT_ROOT / "reports"
V8_SCRIPT = BASE_DIR / "avwap_5min_ID_v8_backtesting.py"
LIVE_CANDIDATE_JSON_DIR = runtime_dir("signal_discovery_v7_5mins_ID", "json")

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
    if source == "v8":
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


def _run_v8_live_parity(day: str, out_dir: Path) -> tuple[int, str]:
    cmd = [
        str(PYTHON_EXE),
        "-u",
        str(V8_SCRIPT),
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


def build_report(day: str, *, run_v8: bool = True) -> int:
    out_dir = OUT_ROOT / day
    out_dir.mkdir(parents=True, exist_ok=True)
    log_text = ""
    rc = 0
    if run_v8:
        rc, log_text = _run_v8_live_parity(day, out_dir)
        (out_dir / "v8_live_parity_run.log").write_text(log_text, encoding="utf-8")

    v8 = _read_csv(out_dir / "trades.csv")
    paper = _read_csv(_paper_path(day))
    v8_summary = _summary(v8, "v6_net_pnl_rs")
    paper_summary = _summary(paper, "pnl_rs")

    if not v8.empty:
        v8["_compare_key"] = [_signal_key(row, source="v8") for _, row in v8.iterrows()]
    if not paper.empty:
        paper["_compare_key"] = [_signal_key(row, source="paper") for _, row in paper.iterrows()]

    v8_keys = set(v8.get("_compare_key", pd.Series(dtype=str)).dropna().astype(str))
    paper_keys = set(paper.get("_compare_key", pd.Series(dtype=str)).dropna().astype(str))
    only_v8 = v8.loc[v8.get("_compare_key", pd.Series(dtype=str)).isin(sorted(v8_keys - paper_keys))].copy() if not v8.empty else pd.DataFrame()
    only_paper = paper.loc[paper.get("_compare_key", pd.Series(dtype=str)).isin(sorted(paper_keys - v8_keys))].copy() if not paper.empty else pd.DataFrame()
    matched = v8.loc[v8.get("_compare_key", pd.Series(dtype=str)).isin(sorted(v8_keys & paper_keys))].copy() if not v8.empty else pd.DataFrame()

    by_setup_lines: list[str] = []
    if not v8.empty:
        tmp = v8.copy()
        tmp["v6_net_pnl_rs"] = pd.to_numeric(tmp["v6_net_pnl_rs"], errors="coerce").fillna(0.0)
        for (side, setup), group in tmp.groupby(["side", "setup"], dropna=False):
            s = _summary(group, "v6_net_pnl_rs")
            by_setup_lines.append(
                f"| {side} | {setup} | {s['trades']} | {s['wins']} | Rs {_fmt_num(s['net'])} | {_fmt_pf(s['pf'])} |"
            )

    discrepancy_reasons = [
        "v8 live_parity replays signal-discovery candidates and v8 gate; paper TRUE executes final live signal CSVs.",
        "paper TRUE may reject via PF gate, risk/capacity, late detection, or entry retry logic.",
        "v8 uses first 1-minute open at/after signal and model exits; paper TRUE uses live executor timing and LTP/entry handling.",
        "If same ticker has multiple candidates, v8 live_parity dedupes one ticker per day to match live entry guard.",
    ]

    fix_suggestions = [
        "Do not edit avwap_5min_ID_v8_backtesting.py automatically. Review this report first.",
        "If v8-only trades are valid paper skips, add the paper skip reason into the comparison report, not the backtest logic.",
        "If paper-only trades are missing from v8, check candidate snapshot loading and live_date filtering in live_parity mode.",
        "If PnL differs on matched trades, compare entry_time/entry_price and decide whether v8 should model paper TRUE entry behavior more closely.",
        "If setup totals differ consistently, check accepted_rules/gate parity before changing strategy rules.",
    ]

    lines = [
        f"# Backtesting Result v7/v8 - {day}",
        "",
        "Report-only mode. No changes are made to `avwap_5min_ID_v8_backtesting.py`.",
        "",
        "## Run Status",
        "",
        f"- v8 live_parity exit code: {rc}",
        f"- v8 output dir: {out_dir}",
        f"- paper TRUE file: {_paper_path(day)}",
        "",
        "## Result Comparison",
        "",
        "| source | trades | wins | losses | net pnl | PF |",
        "|---|---:|---:|---:|---:|---:|",
        f"| v8 live_parity day-only | {v8_summary['trades']} | {v8_summary['wins']} | {v8_summary['losses']} | Rs {_fmt_num(v8_summary['net'])} | {_fmt_pf(v8_summary['pf'])} |",
        f"| V7 paper TRUE live | {paper_summary['trades']} | {paper_summary['wins']} | {paper_summary['losses']} | Rs {_fmt_num(paper_summary['net'])} | {_fmt_pf(paper_summary['pf'])} |",
        "",
        "## Discrepancy Counts",
        "",
        f"- Matched signal keys: {len(v8_keys & paper_keys)}",
        f"- v8-only trades: {len(v8_keys - paper_keys)}",
        f"- paper-only trades: {len(paper_keys - v8_keys)}",
        "",
        "## V8 By Setup",
        "",
    ]
    if by_setup_lines:
        lines.extend(["| side | setup | trades | wins | net pnl | PF |", "|---|---|---:|---:|---:|---:|"])
        lines.extend(by_setup_lines)
    else:
        lines.append("No v8 setup rows.")
    lines.extend(["", "## Likely Discrepancy Causes", ""])
    lines.extend([f"- {x}" for x in discrepancy_reasons])
    lines.extend(["", "## Suggested Fix Direction For avwap_5min_ID_v8_backtesting.py", ""])
    lines.extend([f"- {x}" for x in fix_suggestions])

    if rc != 0:
        lines.extend(["", "## v8 Run Log Tail", ""])
        tail = "\n".join(log_text.splitlines()[-40:])
        lines.append("```text")
        lines.append(tail)
        lines.append("```")

    report = "\n".join(lines) + "\n"
    report_path = REPORTS_DIR / f"backtesting_result_v7_v8_{day}.md"
    report_path.write_text(report, encoding="utf-8")
    (LATEST_DIR / "latest_backtesting_result_v7_v8.md").write_text(report, encoding="utf-8")
    (LATEST_DIR / "latest_backtesting_result_v7_v8.json").write_text(
        json.dumps(
            {
                "day": day,
                "exit_code": rc,
                "v8_summary": v8_summary,
                "paper_summary": paper_summary,
                "matched": len(v8_keys & paper_keys),
                "v8_only": len(v8_keys - paper_keys),
                "paper_only": len(paper_keys - v8_keys),
                "report": str(report_path),
                "out_dir": str(out_dir),
            },
            indent=2,
            sort_keys=True,
            default=str,
        ),
        encoding="utf-8",
    )
    only_v8.to_csv(out_dir / "v8_only_vs_paper.csv", index=False)
    only_paper.to_csv(out_dir / "paper_only_vs_v8.csv", index=False)
    matched.to_csv(out_dir / "matched_v8_vs_paper.csv", index=False)
    print(report)
    return rc


def _default_day() -> str:
    return pd.Timestamp.now(tz="Asia/Kolkata").strftime("%Y-%m-%d")


def main() -> int:
    ap = argparse.ArgumentParser(description="Run day-only v8 live parity and compare with V7 paper TRUE.")
    ap.add_argument("--date", default=_default_day())
    ap.add_argument("--no-run-v8", action="store_true")
    args = ap.parse_args()
    return build_report(str(args.date), run_v8=not args.no_run_v8)


if __name__ == "__main__":
    raise SystemExit(main())
