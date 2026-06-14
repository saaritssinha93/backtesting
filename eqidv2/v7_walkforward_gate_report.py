"""
Daily V7 walk-forward gate report.

This is intentionally a report-only harness around walkforward_gate.py. It
reads resolved V11 backtest trades, maps them to the gate input contract, and
writes dashboard artifacts. It does not write the production accepted_rules.csv.
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from eqidv2_runtime_paths import RUNTIME_STATUS_DIR, runtime_dir
from nse_intraday_costs import CostConfig
from walkforward_gate import WalkForwardConfig, net_pnl_vectorized, run_gate


SESSION_NAME = "V7 Walkforward Gate"
SESSION_SLUG = "v7_walkforward_gate"
SESSION_ROOT = runtime_dir(SESSION_SLUG)
LATEST_DIR = SESSION_ROOT / "latest"
REPORTS_DIR = SESSION_ROOT / "reports"
HEARTBEAT_DIR = SESSION_ROOT / "heartbeat"
BACKTEST_ROOT = runtime_dir("backtesting_result_v11")

PRODUCTION_ACCEPTED_RULES_PATH = Path(
    os.getenv(
        "EQIDV2_V7_ACCEPTED_RULES_PATH",
        r"C:\TradingData\eqidv2\outputs_ID_v8_5min_research_restore\accepted_rules.csv",
    )
)

DECISION_COLUMNS = [
    "setup",
    "n_oos",
    "win_rate",
    "gross_pf_oos",
    "net_pf_oos",
    "net_expectancy_oos",
    "total_net_oos",
    "n_folds",
    "fold_consistency",
    "p_value",
    "net_pf_is",
    "oos_is_ratio",
    "overfit_flag",
    "fitted_rule",
    "fdr_significant",
    "decision",
]

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
        return "inf" if x > 0 else ""
    return f"{x:.{digits}f}"


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


def _profit_factor(values: pd.Series | np.ndarray) -> float:
    pnl = pd.to_numeric(pd.Series(values), errors="coerce").fillna(0.0).to_numpy(float)
    gains = pnl[pnl > 0].sum()
    losses = -pnl[pnl < 0].sum()
    if losses <= 0:
        return float("inf") if gains > 0 else 0.0
    return float(gains / losses)


def _read_trade_csv(path: Path) -> pd.DataFrame:
    if not path.exists() or path.stat().st_size <= 2:
        return pd.DataFrame()
    try:
        return pd.read_csv(path)
    except Exception:
        return pd.DataFrame()


def _parse_day(value: str) -> pd.Timestamp:
    parsed = pd.to_datetime(value, errors="coerce")
    if pd.isna(parsed):
        raise ValueError(f"invalid date: {value!r}")
    return parsed.normalize()


def _source_trade_paths(as_of_day: str, lookback_days: int | None) -> list[Path]:
    as_of = _parse_day(as_of_day)
    start = as_of - pd.Timedelta(days=lookback_days - 1) if lookback_days else None
    paths: list[Path] = []
    for day_dir in sorted(BACKTEST_ROOT.iterdir() if BACKTEST_ROOT.exists() else [], key=lambda p: p.name):
        if not day_dir.is_dir():
            continue
        day = pd.to_datetime(day_dir.name, errors="coerce")
        if pd.isna(day):
            continue
        day = day.normalize()
        if day > as_of:
            continue
        if start is not None and day < start:
            continue
        trades_csv = day_dir / "trades.csv"
        if trades_csv.exists():
            paths.append(trades_csv)
    return paths


def load_backtest_trades(as_of_day: str, lookback_days: int | None) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for path in _source_trade_paths(as_of_day, lookback_days):
        df = _read_trade_csv(path)
        if df.empty:
            continue
        df["_source_day"] = path.parent.name
        df["_source_csv"] = str(path)
        frames.append(df)
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def prepare_gate_input(raw: pd.DataFrame) -> tuple[pd.DataFrame, list[str]]:
    required = {
        "trade_date",
        "setup",
        "side",
        "entry_price_v6",
        "v6_exit_price",
        "notional_exposure_rs",
    }
    missing = sorted(required - set(raw.columns))
    if missing:
        return pd.DataFrame(), missing

    out = raw.copy()
    out["date"] = pd.to_datetime(out["trade_date"], errors="coerce").dt.normalize()
    out["setup"] = out["setup"].fillna("").astype(str).str.strip()
    out["side"] = out["side"].fillna("").astype(str).str.upper().str.strip()
    out["entry_price"] = pd.to_numeric(out["entry_price_v6"], errors="coerce")
    out["exit_price"] = pd.to_numeric(out["v6_exit_price"], errors="coerce")
    out["_notional"] = pd.to_numeric(out["notional_exposure_rs"], errors="coerce")
    out["qty"] = out["_notional"] / out["entry_price"]
    out = out.loc[
        out["date"].notna()
        & out["setup"].ne("")
        & out["side"].isin(["LONG", "SHORT"])
        & out["entry_price"].gt(0)
        & out["exit_price"].gt(0)
        & out["qty"].gt(0)
    ].copy()
    return out.reset_index(drop=True), []


def descriptive_setup_stats(gate_df: pd.DataFrame, cfg: CostConfig) -> pd.DataFrame:
    columns = [
        "setup",
        "trades",
        "days",
        "win_rate",
        "net_pf",
        "net_pnl_rs",
        "avg_net_pnl_rs",
    ]
    if gate_df.empty:
        return pd.DataFrame(columns=columns)

    out = gate_df[["setup", "date", "entry_price", "exit_price", "qty", "side"]].copy()
    out["net_pnl_rs"] = net_pnl_vectorized(
        out["entry_price"].to_numpy(float),
        out["exit_price"].to_numpy(float),
        out["qty"].to_numpy(float),
        out["side"].to_numpy(),
        cfg,
    )

    rows: list[dict[str, Any]] = []
    for setup, g in out.groupby("setup"):
        net = pd.to_numeric(g["net_pnl_rs"], errors="coerce").fillna(0.0)
        rows.append(
            {
                "setup": setup,
                "trades": int(len(g)),
                "days": int(pd.to_datetime(g["date"], errors="coerce").dt.normalize().nunique()),
                "win_rate": round(float((net > 0).mean()), 4) if len(net) else 0.0,
                "net_pf": round(_profit_factor(net), 4) if np.isfinite(_profit_factor(net)) else "inf",
                "net_pnl_rs": round(float(net.sum()), 4),
                "avg_net_pnl_rs": round(float(net.mean()), 4) if len(net) else 0.0,
            }
        )
    stats = pd.DataFrame(rows)
    return stats.sort_values(["net_pnl_rs", "trades"], ascending=[False, False]).reset_index(drop=True)


def _safe_float(value: Any, default: float = np.nan) -> float:
    try:
        x = float(value)
    except Exception:
        return default
    return x if np.isfinite(x) else default


def _binding_constraint(
    net_pf: float,
    fold_cons: float,
    p_val: float,
    n_oos: float,
    overfit: str,
    fdr_sig: str,
    wf: WalkForwardConfig,
) -> str:
    failed: list[str] = []
    if not np.isfinite(n_oos) or n_oos < wf.min_oos_trades:
        failed.append(f"n_oos<{wf.min_oos_trades}(got {int(n_oos) if np.isfinite(n_oos) else '?'})")
    if not np.isfinite(net_pf) or net_pf < wf.min_net_pf:
        failed.append(f"net_pf<{wf.min_net_pf:.2f}(got {net_pf:.3f})" if np.isfinite(net_pf) else f"net_pf<{wf.min_net_pf:.2f}(got ?)")
    if not np.isfinite(fold_cons) or fold_cons < wf.min_fold_consistency:
        failed.append(f"fold_cons<{wf.min_fold_consistency:.2f}(got {fold_cons:.3f})" if np.isfinite(fold_cons) else f"fold_cons<{wf.min_fold_consistency:.2f}(got ?)")
    if str(overfit).upper() in {"TRUE", "1", "YES"}:
        failed.append("overfit=TRUE")
    if str(fdr_sig).upper() not in {"TRUE", "1", "YES"}:
        failed.append("fdr_not_sig")
    return " + ".join(failed[:3]) if failed else "ALL_PASS"


def _gate_threshold_gap(decisions: pd.DataFrame, wf: WalkForwardConfig) -> pd.DataFrame:
    """Per-setup table showing exact values vs gate thresholds — identifies borderline cases."""
    if decisions.empty:
        return pd.DataFrame()
    rows: list[dict[str, Any]] = []
    for _, row in decisions.iterrows():
        net_pf = _safe_float(row.get("net_pf_oos"))
        fold_cons = _safe_float(row.get("fold_consistency"))
        p_val = _safe_float(row.get("p_value"))
        n_oos = _safe_float(row.get("n_oos"))
        overfit = str(row.get("overfit_flag", ""))
        fdr_sig = str(row.get("fdr_significant", ""))
        rows.append(
            {
                "setup": str(row.get("setup", "")),
                "decision": str(row.get("decision", "")),
                "n_oos": int(n_oos) if np.isfinite(n_oos) else "",
                "oos_gap": (int(n_oos) - wf.min_oos_trades) if np.isfinite(n_oos) else "",
                "net_pf_oos": _fmt_num(net_pf, 3),
                "pf_gap": _fmt_num(net_pf - wf.min_net_pf, 3),
                "fold_cons": _fmt_num(fold_cons, 3),
                "cons_gap": _fmt_num(fold_cons - wf.min_fold_consistency, 3),
                "p_value": _fmt_num(p_val, 4),
                "fdr_sig": fdr_sig,
                "overfit": overfit,
                "binding_constraint": _binding_constraint(
                    net_pf, fold_cons, p_val, n_oos, overfit, fdr_sig, wf
                ),
            }
        )
    return pd.DataFrame(rows)


def _rolling_net_pf_by_setup(
    gate_df: pd.DataFrame, cfg: CostConfig, windows: list[int]
) -> pd.DataFrame:
    """Net PF per setup across rolling day-count windows for stability tracking."""
    if gate_df.empty:
        return pd.DataFrame()
    df = gate_df.copy()
    df["_net_pnl"] = net_pnl_vectorized(
        df["entry_price"].to_numpy(float),
        df["exit_price"].to_numpy(float),
        df["qty"].to_numpy(float),
        df["side"].to_numpy(),
        cfg,
    )
    dates = sorted(
        pd.to_datetime(df["date"], errors="coerce").dt.normalize().dropna().unique()
    )
    rows: list[dict[str, Any]] = []
    for setup, grp in df.groupby("setup"):
        row: dict[str, Any] = {"setup": setup, "all_trades": int(len(grp))}
        for w in windows:
            key_pf = f"net_pf_{w}d"
            key_n = f"n_{w}d"
            if len(dates) < w:
                row[key_pf] = "insuf"
                row[key_n] = ""
            else:
                cutoff = dates[-w]
                recent = grp.loc[
                    pd.to_datetime(grp["date"], errors="coerce").dt.normalize() >= cutoff
                ]
                if recent.empty:
                    row[key_pf] = "0T"
                    row[key_n] = 0
                else:
                    pf = _profit_factor(recent["_net_pnl"])
                    row[key_pf] = _fmt_num(pf, 3) if np.isfinite(pf) else "inf"
                    row[key_n] = int(len(recent))
        rows.append(row)
    return pd.DataFrame(rows).sort_values("all_trades", ascending=False).reset_index(drop=True)


def _decision_counts(report: pd.DataFrame) -> dict[str, int]:
    if report.empty or "decision" not in report.columns:
        return {"PROMOTE": 0, "PROBATION": 0, "REJECT": 0}
    counts = report["decision"].fillna("").astype(str).value_counts().to_dict()
    return {key: int(counts.get(key, 0)) for key in ("PROMOTE", "PROBATION", "REJECT")}


def _markdown_table(df: pd.DataFrame) -> list[str]:
    if df.empty:
        return []
    cols = [str(c) for c in df.columns]
    lines = [
        "| " + " | ".join(cols) + " |",
        "| " + " | ".join("---" for _ in cols) + " |",
    ]
    for _, row in df.iterrows():
        vals = []
        for col in df.columns:
            value = row.get(col, "")
            if pd.isna(value):
                value = ""
            vals.append(str(value).replace("|", "\\|"))
        lines.append("| " + " | ".join(vals) + " |")
    return lines


def _render_markdown(
    summary: dict[str, Any],
    decisions: pd.DataFrame,
    descriptive: pd.DataFrame,
    threshold_gap: pd.DataFrame | None = None,
    rolling_pf: pd.DataFrame | None = None,
) -> str:
    lines = [
        f"# V7 Walkforward Gate - {summary.get('day', '')}",
        "",
        "**Report-only shadow gate. This job does not write production accepted_rules.csv.**",
        "",
        f"- Generated: {summary.get('generated_at_ist', '')}",
        f"- Source root: `{summary.get('source_root', '')}`",
        f"- Production accepted rules: `{summary.get('production_accepted_rules_path', '')}`",
        f"- Candidate output: `{summary.get('candidate_accepted_rules_path', '')}`",
        "",
        "## Summary",
        "",
        "| metric | value |",
        "|---|---:|",
        f"| gate status | {summary.get('gate_status', '')} |",
        f"| raw backtest rows | {summary.get('raw_rows', 0)} |",
        f"| mapped gate rows | {summary.get('gate_rows', 0)} |",
        f"| source trading days | {summary.get('source_days', 0)} |",
        f"| setups | {summary.get('setups', 0)} |",
        f"| strict train / embargo / test | {summary.get('train_days', 0)} / {summary.get('embargo_days', 0)} / {summary.get('test_days', 0)} days |",
        f"| first fold needs | {summary.get('first_fold_required_days', 0)} trading days |",
        f"| full fold needs | {summary.get('full_fold_required_days', 0)} trading days |",
        f"| min OOS trades / setup | {summary.get('min_oos_trades', 0)} |",
        f"| decisions PROMOTE / PROBATION / REJECT | {summary.get('promote_count', 0)} / {summary.get('probation_count', 0)} / {summary.get('reject_count', 0)} |",
        "",
    ]

    reason = str(summary.get("gate_reason", "") or "")
    if reason:
        lines.extend(["## Gate Note", "", reason, ""])

    lines.extend(
        [
            "## Decision Report",
            "",
        ]
    )
    if decisions.empty:
        lines.append("No strict walk-forward decisions were produced.")
    else:
        view_cols = [
            "setup",
            "decision",
            "n_oos",
            "net_pf_oos",
            "net_expectancy_oos",
            "fold_consistency",
            "p_value",
            "fdr_significant",
            "overfit_flag",
        ]
        view = decisions[[c for c in view_cols if c in decisions.columns]].copy()
        lines.extend(_markdown_table(view))

    lines.extend(["", "## Descriptive Setup Sample", ""])
    if descriptive.empty:
        lines.append("No valid mapped trades found for descriptive statistics.")
    else:
        view = descriptive.head(25).copy()
        lines.extend(_markdown_table(view))

    if threshold_gap is not None and not threshold_gap.empty:
        lines.extend(["", "## Gate Threshold Analysis", ""])
        lines.append(
            "Gap columns show `value − threshold`; positive = passed with margin, "
            "negative = failed. `binding_constraint` names the worst-failing criteria."
        )
        lines.append("")
        lines.extend(_markdown_table(threshold_gap))

    if rolling_pf is not None and not rolling_pf.empty:
        lines.extend(["", "## Rolling Net PF by Setup", ""])
        lines.append(
            "Net PF computed over the most recent N trading days in the backtest data. "
            "`insuf` = fewer days available than the window. "
            "Use this to spot setups with degrading or improving recent performance."
        )
        lines.append("")
        lines.extend(_markdown_table(rolling_pf))

    lines.extend(
        [
            "",
            "## Interpretation",
            "",
            "- PROMOTE means the setup cleared strict out-of-sample, net-of-cost, FDR-adjusted checks.",
            "- PROBATION means the setup cleared base performance bars but did not clear all statistical/overfit checks.",
            "- REJECT means insufficient or weak out-of-sample evidence under the strict gate.",
            "- Until this report has enough source days, the descriptive sample is useful for monitoring only.",
            "- `pf_gap` and `cons_gap` in the threshold table are negative when a setup failed that criterion.",
        ]
    )
    return "\n".join(lines) + "\n"


def run(day: str, wf: WalkForwardConfig, lookback_days: int | None) -> tuple[Path, Path, Path]:
    _write_status("RUNNING", phase="START", day=day)
    cfg = CostConfig()
    raw = load_backtest_trades(day, lookback_days)
    gate_df, missing = prepare_gate_input(raw) if not raw.empty else (pd.DataFrame(), [])
    source_days = int(pd.to_datetime(gate_df["date"], errors="coerce").dt.normalize().nunique()) if not gate_df.empty else 0
    first_fold_required_days = wf.train_days + wf.embargo_days + 1
    full_fold_required_days = wf.train_days + wf.embargo_days + wf.test_days
    descriptive = descriptive_setup_stats(gate_df, cfg)

    if missing:
        gate_status = "INPUT_CONTRACT_MISSING"
        gate_reason = f"Missing required source columns: {', '.join(missing)}."
        decisions = pd.DataFrame(columns=DECISION_COLUMNS)
    elif raw.empty:
        gate_status = "NO_SOURCE_TRADES"
        gate_reason = f"No trades.csv files found under {BACKTEST_ROOT} through {day}."
        decisions = pd.DataFrame(columns=DECISION_COLUMNS)
    elif gate_df.empty:
        gate_status = "NO_VALID_GATE_ROWS"
        gate_reason = "Source trades were present, but none mapped to valid side/entry/exit/qty rows."
        decisions = pd.DataFrame(columns=DECISION_COLUMNS)
    elif source_days < first_fold_required_days:
        gate_status = "INSUFFICIENT_HISTORY"
        gate_reason = (
            f"Strict gate needs at least {first_fold_required_days} trading days for the first "
            f"purged walk-forward fold and {full_fold_required_days} for a full {wf.test_days}-day "
            f"test fold. Current mapped history has {source_days} trading days."
        )
        decisions = pd.DataFrame(columns=DECISION_COLUMNS)
    else:
        gate_status = "STRICT_GATE_COMPLETE"
        gate_reason = "Strict report-only walk-forward gate completed. Production rules were not modified."
        decisions = run_gate(gate_df, wf, cfg)
        for col in DECISION_COLUMNS:
            if col not in decisions.columns:
                decisions[col] = np.nan
        decisions = decisions[DECISION_COLUMNS]

    counts = _decision_counts(decisions)
    candidate = decisions.loc[decisions.get("decision", pd.Series(dtype=str)).eq("PROMOTE")].copy()

    latest_decisions_csv = LATEST_DIR / "latest_v7_walkforward_gate.csv"
    latest_candidate_csv = LATEST_DIR / "accepted_rules_candidate.csv"
    latest_input_csv = LATEST_DIR / "latest_v7_walkforward_gate_input.csv"
    latest_descriptive_csv = LATEST_DIR / "latest_v7_walkforward_gate_descriptive.csv"
    latest_json = LATEST_DIR / "latest_v7_walkforward_gate.json"
    latest_md = LATEST_DIR / "latest_v7_walkforward_gate.md"

    dated_decisions_csv = REPORTS_DIR / f"v7_walkforward_gate_{day}.csv"
    dated_candidate_csv = REPORTS_DIR / f"accepted_rules_candidate_{day}.csv"
    dated_input_csv = REPORTS_DIR / f"v7_walkforward_gate_input_{day}.csv"
    dated_descriptive_csv = REPORTS_DIR / f"v7_walkforward_gate_descriptive_{day}.csv"
    dated_json = REPORTS_DIR / f"v7_walkforward_gate_{day}.json"
    dated_md = REPORTS_DIR / f"v7_walkforward_gate_{day}.md"

    summary = {
        "session": SESSION_NAME,
        "session_slug": SESSION_SLUG,
        "day": day,
        "generated_at_ist": _fmt_ts(_now_ist()),
        "report_only": True,
        "production_rules_modified": False,
        "gate_status": gate_status,
        "gate_reason": gate_reason,
        "source_root": str(BACKTEST_ROOT),
        "source_trade_files": [str(p) for p in _source_trade_paths(day, lookback_days)],
        "production_accepted_rules_path": str(PRODUCTION_ACCEPTED_RULES_PATH),
        "candidate_accepted_rules_path": str(latest_candidate_csv),
        "raw_rows": int(len(raw)),
        "gate_rows": int(len(gate_df)),
        "source_days": source_days,
        "setups": int(gate_df["setup"].nunique()) if not gate_df.empty else 0,
        "train_days": wf.train_days,
        "test_days": wf.test_days,
        "embargo_days": wf.embargo_days,
        "step_days": wf.step(),
        "first_fold_required_days": first_fold_required_days,
        "full_fold_required_days": full_fold_required_days,
        "min_oos_trades": wf.min_oos_trades,
        "min_net_pf": wf.min_net_pf,
        "min_fold_consistency": wf.min_fold_consistency,
        "fdr_alpha": wf.fdr_alpha,
        "n_bootstrap": wf.n_bootstrap,
        "promote_count": counts["PROMOTE"],
        "probation_count": counts["PROBATION"],
        "reject_count": counts["REJECT"],
        "decision_rows": int(len(decisions)),
        "candidate_rows": int(len(candidate)),
    }

    threshold_gap = _gate_threshold_gap(decisions, wf) if gate_status == "STRICT_GATE_COMPLETE" else pd.DataFrame()
    rolling_pf = _rolling_net_pf_by_setup(gate_df, cfg, [20, 40, 60]) if not gate_df.empty else pd.DataFrame()

    latest_threshold_csv = LATEST_DIR / "latest_v7_walkforward_gate_thresholds.csv"
    latest_rolling_csv = LATEST_DIR / "latest_v7_walkforward_gate_rolling_pf.csv"
    dated_threshold_csv = REPORTS_DIR / f"v7_walkforward_gate_thresholds_{day}.csv"
    dated_rolling_csv = REPORTS_DIR / f"v7_walkforward_gate_rolling_pf_{day}.csv"

    decisions.to_csv(latest_decisions_csv, index=False)
    decisions.to_csv(dated_decisions_csv, index=False)
    candidate.to_csv(latest_candidate_csv, index=False)
    candidate.to_csv(dated_candidate_csv, index=False)
    gate_df.to_csv(latest_input_csv, index=False)
    gate_df.to_csv(dated_input_csv, index=False)
    descriptive.to_csv(latest_descriptive_csv, index=False)
    descriptive.to_csv(dated_descriptive_csv, index=False)
    threshold_gap.to_csv(latest_threshold_csv, index=False)
    threshold_gap.to_csv(dated_threshold_csv, index=False)
    rolling_pf.to_csv(latest_rolling_csv, index=False)
    rolling_pf.to_csv(dated_rolling_csv, index=False)
    _write_json_atomic(latest_json, summary)
    _write_json_atomic(dated_json, summary)

    md = _render_markdown(summary, decisions, descriptive, threshold_gap, rolling_pf)
    _write_text_atomic(latest_md, md)
    _write_text_atomic(dated_md, md)

    _write_status(
        "DONE",
        phase=gate_status,
        day=day,
        source_days=source_days,
        gate_rows=len(gate_df),
        promotes=counts["PROMOTE"],
        probation=counts["PROBATION"],
        rejects=counts["REJECT"],
        report=str(latest_md),
        csv=str(latest_decisions_csv),
    )
    return latest_md, latest_json, latest_decisions_csv


def _parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description=SESSION_NAME)
    ap.add_argument("--date", default="", help="YYYY-MM-DD; defaults to today IST")
    ap.add_argument("--lookback-days", type=int, default=0, help="Calendar lookback; 0 means all available history")
    ap.add_argument("--train-days", type=int, default=60)
    ap.add_argument("--test-days", type=int, default=20)
    ap.add_argument("--embargo-days", type=int, default=1)
    ap.add_argument("--step-days", type=int, default=0, help="Defaults to test-days when 0")
    ap.add_argument("--min-oos-trades", type=int, default=40)
    ap.add_argument("--min-net-pf", type=float, default=1.20)
    ap.add_argument("--min-fold-consistency", type=float, default=0.55)
    ap.add_argument("--fdr-alpha", type=float, default=0.10)
    ap.add_argument("--bootstrap", type=int, default=5000)
    return ap.parse_args()


def main() -> int:
    args = _parse_args()
    day = str(args.date or _now_ist().strftime("%Y-%m-%d"))
    wf = WalkForwardConfig(
        train_days=args.train_days,
        test_days=args.test_days,
        embargo_days=args.embargo_days,
        step_days=args.step_days or None,
        min_oos_trades=args.min_oos_trades,
        min_net_pf=args.min_net_pf,
        min_fold_consistency=args.min_fold_consistency,
        fdr_alpha=args.fdr_alpha,
        n_bootstrap=args.bootstrap,
    )
    lookback_days = args.lookback_days if args.lookback_days > 0 else None
    try:
        md, js, csv = run(day, wf, lookback_days)
    except Exception as exc:
        _write_status("ERROR", phase="FAILED", day=day, error=f"{type(exc).__name__}: {exc}")
        print(f"[v7_walkforward_gate] ERROR {type(exc).__name__}: {exc}", flush=True)
        return 1
    print(f"[v7_walkforward_gate] wrote {md} | {js} | {csv}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
