from __future__ import annotations

import argparse
from datetime import datetime
from pathlib import Path
from typing import Iterable

import numpy as np
import pandas as pd


SUMMARY_PREFIX = "daily_trade_summary_"


def _latest_input_csv(outputs_dir: Path) -> Path:
    files = [
        p
        for p in outputs_dir.glob("*.csv")
        if p.is_file() and not p.name.startswith(SUMMARY_PREFIX)
    ]
    if not files:
        raise FileNotFoundError(f"No CSV files found in: {outputs_dir}")

    preferred = [p for p in files if "longshort_trades_ALL_DAYS" in p.name]
    if not preferred:
        preferred = [p for p in files if "trades_ALL_DAYS" in p.name]
    pool = preferred or files

    return max(pool, key=lambda p: p.stat().st_mtime)


def _to_numeric(df: pd.DataFrame, col: str, default: float = 0.0) -> pd.Series:
    if col not in df.columns:
        return pd.Series(default, index=df.index, dtype="float64")
    return pd.to_numeric(df[col], errors="coerce").fillna(default)


def _safe_pct(num: float, den: float) -> float:
    if den == 0 or pd.isna(den):
        return np.nan
    return (num / den) * 100.0


def _append_overall_rows(summary: pd.DataFrame) -> pd.DataFrame:
    """
    Append two rows:
    - TOTAL   : sum for numeric columns (with ratio fields recomputed from totals)
    - AVERAGE : mean for numeric columns
    """
    if summary.empty:
        return summary

    numeric_cols = [c for c in summary.columns if c != "trade_date"]

    total_row = {"trade_date": "TOTAL"}
    avg_row = {"trade_date": "AVERAGE"}

    for c in numeric_cols:
        s = pd.to_numeric(summary[c], errors="coerce")
        total_row[c] = float(s.sum(skipna=True))
        avg_row[c] = float(s.mean(skipna=True))

    # Recompute key ratio columns in TOTAL row from total numerators/denominators.
    inv_no_lev = total_row.get("invested_without_leverage_rs", np.nan)
    inv_with_lev = total_row.get("invested_with_leverage_rs", np.nan)
    net_pnl = total_row.get("net_pnl_rs", np.nan)
    gross_pnl = total_row.get("gross_pnl_rs", np.nan)

    total_row["avg_effective_leverage"] = (
        (inv_with_lev / inv_no_lev) if pd.notna(inv_no_lev) and inv_no_lev != 0 else np.nan
    )
    total_row["net_pnl_pct_on_invested_without_lev"] = _safe_pct(net_pnl, inv_no_lev)
    total_row["net_pnl_pct_on_invested_with_lev"] = _safe_pct(net_pnl, inv_with_lev)
    total_row["gross_pnl_pct_on_invested_without_lev"] = _safe_pct(gross_pnl, inv_no_lev)
    total_row["gross_pnl_pct_on_invested_with_lev"] = _safe_pct(gross_pnl, inv_with_lev)

    out = pd.concat([summary, pd.DataFrame([total_row, avg_row])], ignore_index=True)
    return out


def _ensure_trade_date(df: pd.DataFrame) -> pd.Series:
    if "trade_date" in df.columns:
        td = pd.to_datetime(df["trade_date"], errors="coerce")
        return td.dt.strftime("%Y-%m-%d")

    fallback_cols: Iterable[str] = ("signal_time_ist", "entry_time_ist", "exit_time_ist")
    for col in fallback_cols:
        if col not in df.columns:
            continue
        ts = pd.to_datetime(df[col], errors="coerce")
        if ts.notna().any():
            return ts.dt.strftime("%Y-%m-%d")

    raise ValueError(
        "Could not infer trade date. Expected one of: "
        "trade_date, signal_time_ist, entry_time_ist, exit_time_ist"
    )


def _prepare_required_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["trade_date"] = _ensure_trade_date(out)

    out["position_size_rs"] = _to_numeric(out, "position_size_rs", default=np.nan)
    out["leverage"] = _to_numeric(out, "leverage", default=np.nan)
    out["notional_exposure_rs"] = _to_numeric(out, "notional_exposure_rs", default=np.nan)

    # Fill missing notional/capital/leverage values if any.
    missing_notional = out["notional_exposure_rs"].isna()
    out.loc[missing_notional, "notional_exposure_rs"] = (
        out.loc[missing_notional, "position_size_rs"] * out.loc[missing_notional, "leverage"]
    )

    missing_position = out["position_size_rs"].isna()
    den = out.loc[missing_position, "leverage"].replace(0, np.nan)
    out.loc[missing_position, "position_size_rs"] = (
        out.loc[missing_position, "notional_exposure_rs"] / den
    )

    out["position_size_rs"] = out["position_size_rs"].fillna(0.0)
    out["leverage"] = out["leverage"].fillna(1.0)
    out["notional_exposure_rs"] = out["notional_exposure_rs"].fillna(
        out["position_size_rs"] * out["leverage"]
    )

    out["pnl_rs"] = _to_numeric(out, "pnl_rs", default=np.nan)
    out["pnl_rs_gross"] = _to_numeric(out, "pnl_rs_gross", default=np.nan)

    # Fallback to pct-based reconstruction if rupee P&L columns are absent.
    if out["pnl_rs"].isna().all():
        pnl_pct_price = _to_numeric(out, "pnl_pct_price", default=np.nan)
        if pnl_pct_price.isna().all():
            pnl_pct_price = _to_numeric(out, "pnl_pct", default=0.0)
        out["pnl_rs"] = (pnl_pct_price / 100.0) * out["notional_exposure_rs"]
    out["pnl_rs"] = out["pnl_rs"].fillna(0.0)

    if out["pnl_rs_gross"].isna().all():
        pnl_pct_gross_price = _to_numeric(out, "pnl_pct_gross_price", default=np.nan)
        if pnl_pct_gross_price.isna().all():
            pnl_pct_gross_price = _to_numeric(out, "pnl_pct_gross", default=0.0)
        out["pnl_rs_gross"] = (pnl_pct_gross_price / 100.0) * out["notional_exposure_rs"]
    out["pnl_rs_gross"] = out["pnl_rs_gross"].fillna(0.0)

    return out


def summarize_file(input_csv: Path) -> pd.DataFrame:
    df = pd.read_csv(input_csv)
    if df.empty:
        return pd.DataFrame()

    d = _prepare_required_columns(df)
    d = d.dropna(subset=["trade_date"]).copy()
    if d.empty:
        return pd.DataFrame()

    rows = []
    for trade_date, g in d.groupby("trade_date", sort=True):
        net_pnl = float(g["pnl_rs"].sum())
        gross_pnl = float(g["pnl_rs_gross"].sum())
        invested_no_lev = float(g["position_size_rs"].sum())
        invested_with_lev = float(g["notional_exposure_rs"].sum())

        net_profit = float(g.loc[g["pnl_rs"] > 0, "pnl_rs"].sum())
        net_loss = float(g.loc[g["pnl_rs"] < 0, "pnl_rs"].sum())
        gross_profit = float(g.loc[g["pnl_rs_gross"] > 0, "pnl_rs_gross"].sum())
        gross_loss = float(g.loc[g["pnl_rs_gross"] < 0, "pnl_rs_gross"].sum())

        rows.append(
            {
                "trade_date": trade_date,
                "entries_count": int(len(g)),
                "net_profit_rs": net_profit,
                "net_loss_rs": net_loss,
                "net_pnl_rs": net_pnl,
                "gross_profit_rs": gross_profit,
                "gross_loss_rs": gross_loss,
                "gross_pnl_rs": gross_pnl,
                "cost_impact_rs_gross_minus_net": gross_pnl - net_pnl,
                "invested_without_leverage_rs": invested_no_lev,
                "invested_with_leverage_rs": invested_with_lev,
                "avg_effective_leverage": invested_with_lev / invested_no_lev if invested_no_lev else np.nan,
                "net_pnl_pct_on_invested_without_lev": _safe_pct(net_pnl, invested_no_lev),
                "net_pnl_pct_on_invested_with_lev": _safe_pct(net_pnl, invested_with_lev),
                "gross_pnl_pct_on_invested_without_lev": _safe_pct(gross_pnl, invested_no_lev),
                "gross_pnl_pct_on_invested_with_lev": _safe_pct(gross_pnl, invested_with_lev),
            }
        )

    summary = pd.DataFrame(rows).sort_values("trade_date").reset_index(drop=True)
    summary = _append_overall_rows(summary)

    rupee_cols = [c for c in summary.columns if c.endswith("_rs")]
    pct_cols = [c for c in summary.columns if c.endswith("_lev")]

    for c in rupee_cols:
        summary[c] = pd.to_numeric(summary[c], errors="coerce").round(2)
    summary["entries_count"] = pd.to_numeric(summary["entries_count"], errors="coerce").round(2)
    summary["avg_effective_leverage"] = pd.to_numeric(
        summary["avg_effective_leverage"], errors="coerce"
    ).round(4)
    for c in pct_cols:
        summary[c] = pd.to_numeric(summary[c], errors="coerce").round(4)

    return summary


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Read the latest trades CSV from outputs/ and create a date-wise summary CSV "
            "with entries, gross/net P&L, invested capital/notional, and P&L percentages."
        )
    )
    parser.add_argument(
        "--outputs-dir",
        type=Path,
        default=Path(__file__).resolve().parent / "outputs",
        help="Directory containing backtest output CSV files (default: ./outputs)",
    )
    parser.add_argument(
        "--input-csv",
        type=Path,
        default=None,
        help="Optional explicit input CSV path. If omitted, latest CSV from outputs-dir is used.",
    )
    parser.add_argument(
        "--output-csv",
        type=Path,
        default=None,
        help="Optional explicit output summary CSV path.",
    )
    args = parser.parse_args()

    outputs_dir = args.outputs_dir.resolve()
    outputs_dir.mkdir(parents=True, exist_ok=True)

    input_csv = args.input_csv.resolve() if args.input_csv else _latest_input_csv(outputs_dir)
    summary = summarize_file(input_csv)

    if summary.empty:
        raise RuntimeError(f"No rows to summarize from: {input_csv}")

    if args.output_csv:
        out_csv = args.output_csv.resolve()
    else:
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        out_csv = outputs_dir / f"{SUMMARY_PREFIX}{input_csv.stem}_{ts}.csv"

    summary.to_csv(out_csv, index=False)

    print(f"[INPUT ] {input_csv}")
    print(f"[OUTPUT] {out_csv}")
    print(f"[ROWS  ] {len(summary)} date rows")


if __name__ == "__main__":
    main()
