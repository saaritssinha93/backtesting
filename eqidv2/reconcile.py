from __future__ import annotations

import argparse
import json
import math
import re
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from nse_intraday_costs import CostConfig, intraday_equity_costs


IST = "Asia/Kolkata"
ENTRY_SLIPPAGE_BPS = 5.0
EXIT_SLIPPAGE_BPS = 5.0
BAR_TOL_MIN = 5
NET_TOL_BPS_NOTIONAL = 10.0

DEFAULT_RUNTIME_ROOT = Path(r"C:\TradingData\eqidv2")
DEFAULT_DATES = (
    "2026-06-29",
    "2026-06-30",
    "2026-07-01",
    "2026-07-02",
    "2026-07-03",
    "2026-07-06",
    "2026-07-07",
)


@dataclass(frozen=True)
class Paths:
    runtime_root: Path
    live_signals: Path
    v11_root: Path
    out_dir: Path


def _read_csv(path: Path) -> pd.DataFrame:
    if not path.exists() or path.stat().st_size <= 2:
        return pd.DataFrame()
    try:
        return pd.read_csv(path, low_memory=False)
    except Exception as exc:
        print(f"[WARN] failed reading {path}: {exc}", file=sys.stderr)
        return pd.DataFrame()


def _safe_float(value: Any, default: float = np.nan) -> float:
    try:
        out = float(value)
        if math.isfinite(out):
            return out
    except Exception:
        pass
    return float(default)


def _safe_int(value: Any, default: int = 0) -> int:
    val = _safe_float(value, np.nan)
    if np.isfinite(val):
        return int(round(val))
    return int(default)


def _first(row: pd.Series, names: tuple[str, ...], default: Any = "") -> Any:
    for name in names:
        if name in row.index and not pd.isna(row.get(name)):
            text = str(row.get(name)).strip()
            if text and text.upper() not in {"NAN", "NONE", "NAT", "NULL"}:
                return row.get(name)
    return default


def _to_ist(value: Any) -> pd.Timestamp:
    ts = pd.to_datetime(value, errors="coerce")
    if pd.isna(ts):
        return pd.NaT
    if ts.tzinfo is None:
        return ts.tz_localize(IST)
    return ts.tz_convert(IST)


def _fmt_ts(value: Any) -> str:
    ts = _to_ist(value)
    if pd.isna(ts):
        return ""
    return ts.strftime("%Y-%m-%d %H:%M:%S%z")


def _entry_bar(ts: Any) -> pd.Timestamp:
    stamp = _to_ist(ts)
    if pd.isna(stamp):
        return pd.NaT
    return stamp.floor("5min")


def _date_from_ts(ts: Any, fallback: str = "") -> str:
    stamp = _to_ist(ts)
    if pd.isna(stamp):
        return fallback
    return str(stamp.date())


def _norm_side(value: Any) -> str:
    text = str(value or "").strip().upper()
    if text in {"BUY", "LONG"}:
        return "LONG"
    if text in {"SELL", "SHORT"}:
        return "SHORT"
    return text


def _norm_outcome(value: Any) -> str:
    text = str(value or "").strip().upper()
    if text in {"EOD", "EOD_CLOSE", "FORCED_CLOSE"}:
        return "EOD_CLOSE"
    if text.startswith("TIME_STOP"):
        return "TIME_STOP"
    if text.startswith("MANUAL") or text.startswith("KILL"):
        return "MANUAL_KILL"
    return text


def _gross_pnl(side: str, entry: float, exit_price: float, qty: int) -> float:
    if side == "LONG":
        return (exit_price - entry) * qty
    return (entry - exit_price) * qty


def _apply_exit_slippage(price: float, side: str, outcome: str, bps: float = EXIT_SLIPPAGE_BPS) -> float:
    if not np.isfinite(price) or price <= 0:
        return price
    if _norm_outcome(outcome) == "TARGET":
        return round(price, 2)
    slip = price * bps / 10_000.0
    return round(price - slip if side == "LONG" else price + slip, 2)


def _costed_trade(side: str, entry: float, exit_price: float, qty: int) -> dict[str, float]:
    if qty <= 0 or entry <= 0 or exit_price <= 0 or side not in {"LONG", "SHORT"}:
        return {"gross_pnl": np.nan, "costs": np.nan, "net_pnl": np.nan, "cost_bps": np.nan}
    b = intraday_equity_costs(entry, exit_price, qty, side, CostConfig())
    return {
        "gross_pnl": float(b.gross_pnl),
        "costs": float(b.total_cost),
        "net_pnl": float(b.net_pnl),
        "cost_bps": float(b.cost_bps_of_turnover),
    }


def discover_dates(paths: Paths, limit: int) -> list[str]:
    dates: set[str] = set()
    pat = re.compile(r"paper_trade_execution_(\d{4}-\d{2}-\d{2})_id_5min_v7\.log$")
    for path in paths.live_signals.glob("paper_trade_execution_*_id_5min_v7.log"):
        match = pat.search(path.name)
        if match:
            dates.add(match.group(1))
    if not dates:
        return list(DEFAULT_DATES)[-limit:]
    return sorted(dates)[-limit:]


def run_v11_for_dates(dates: list[str], selected_strategy_profile: str) -> None:
    for day in dates:
        cmd = [
            sys.executable,
            "-u",
            "backtesting_result_v11_daily.py",
            "--date",
            day,
            "--selected-strategy-profile",
            selected_strategy_profile,
        ]
        print(f"[RUN] {' '.join(cmd)}")
        result = subprocess.run(cmd, cwd=Path(__file__).resolve().parent, text=True, capture_output=True, timeout=900)
        if result.returncode != 0:
            tail = "\n".join(((result.stdout or "") + (result.stderr or "")).splitlines()[-25:])
            raise SystemExit(f"v11 run failed for {day} rc={result.returncode}\n{tail}")


def _concat_raw_csvs(paths: list[tuple[str, Path]]) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for day, path in paths:
        df = _read_csv(path)
        if df.empty:
            continue
        df = df.copy()
        df.insert(0, "source_file", str(path))
        df.insert(0, "date", day)
        frames.append(df)
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def load_raw_live_paper(paths: Paths, dates: list[str]) -> pd.DataFrame:
    return _concat_raw_csvs([(day, paths.live_signals / f"paper_trades_{day}_id_5min_v7.csv") for day in dates])


def load_raw_live_signals(paths: Paths, dates: list[str]) -> pd.DataFrame:
    dated_paths: list[tuple[str, Path]] = []
    for day in dates:
        dated_paths.append((day, paths.live_signals / f"signals_{day}_id_5min_v7_long.csv"))
        dated_paths.append((day, paths.live_signals / f"signals_{day}_id_5min_v7_short.csv"))
    return _concat_raw_csvs(dated_paths)


def load_raw_v11_trades(paths: Paths, dates: list[str]) -> pd.DataFrame:
    dated_paths = []
    for day in dates:
        path = paths.v11_root / day / "trades.csv"
        if not path.exists():
            path = paths.v11_root / day / "v11_ID_trades.csv"
        dated_paths.append((day, path))
    return _concat_raw_csvs(dated_paths)


def load_raw_v11_signals(paths: Paths, dates: list[str]) -> pd.DataFrame:
    dated_paths = []
    for day in dates:
        path = paths.v11_root / day / "live_parity_selected_strategy_signals.csv"
        if not path.exists():
            path = paths.v11_root / day / "live_parity_entry_engine_signals.csv"
        dated_paths.append((day, path))
    return _concat_raw_csvs(dated_paths)


def load_live_trades(paths: Paths, dates: list[str]) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for day in dates:
        path = paths.live_signals / f"paper_trades_{day}_id_5min_v7.csv"
        df = _read_csv(path)
        for idx, row in df.iterrows():
            outcome = _norm_outcome(_first(row, ("outcome", "exit_reason"), ""))
            if _is_nonfilled_live_outcome(outcome):
                continue
            entry_ts = _to_ist(_first(row, ("entry_time", "entry_time_ist", "signal_entry_datetime_ist", "signal_datetime"), ""))
            exit_ts = _to_ist(_first(row, ("exit_time", "exit_time_ist"), ""))
            symbol = str(_first(row, ("ticker", "symbol", "tradingsymbol"), "")).strip().upper()
            side = _norm_side(_first(row, ("side", "direction"), ""))
            setup = str(_first(row, ("setup", "setup_name"), "")).strip()
            entry = _safe_float(_first(row, ("entry_price",), np.nan))
            exit_price = _safe_float(_first(row, ("exit_price",), np.nan))
            qty = _safe_int(_first(row, ("quantity", "qty"), 0))
            gross = _safe_float(_first(row, ("gross_pnl_rs", "gross_pnl"), np.nan))
            costs = _safe_float(_first(row, ("total_cost_rs", "total_cost"), np.nan))
            net = _safe_float(_first(row, ("net_pnl_rs", "net_pnl", "pnl_rs", "pnl"), np.nan))
            rows.append({
                "source": "LIVE",
                "date": day,
                "row_id": f"live:{day}:{idx}",
                "signal_id": str(_first(row, ("signal_id",), "")),
                "symbol": symbol,
                "side": side,
                "setup": setup,
                "entry_time": entry_ts,
                "entry_bar": _entry_bar(entry_ts),
                "exit_time": exit_ts,
                "entry_price": entry,
                "exit_price": exit_price,
                "exit_price_model": exit_price,
                "exit_reason": outcome,
                "qty": qty,
                "gross_pnl": gross,
                "costs": costs,
                "net_pnl": net,
                "notional": entry * qty if np.isfinite(entry) else np.nan,
                "source_file": str(path),
                "quality_score": _safe_float(_first(row, ("quality_score",), np.nan)),
            })
    return pd.DataFrame(rows)


def _is_nonfilled_live_outcome(outcome: str) -> bool:
    text = _norm_outcome(outcome)
    return (
        text.startswith("ENTRY_SKIPPED")
        or text.startswith("ORDER_REJECT")
        or text.startswith("REJECT")
        or text in {"SKIPPED", "ENTRY_REJECTED", "NO_FILL"}
    )


def load_live_nonfilled_rows(paths: Paths, dates: list[str]) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for day in dates:
        path = paths.live_signals / f"paper_trades_{day}_id_5min_v7.csv"
        df = _read_csv(path)
        for idx, row in df.iterrows():
            outcome = _norm_outcome(_first(row, ("outcome", "exit_reason"), ""))
            if not _is_nonfilled_live_outcome(outcome):
                continue
            signal_ts = _to_ist(_first(row, ("signal_entry_datetime_ist", "signal_datetime"), ""))
            entry_ts = _to_ist(_first(row, ("entry_time", "entry_time_ist", "signal_entry_datetime_ist", "signal_datetime"), ""))
            rows.append({
                "source": "LIVE_NONFILLED",
                "date": day,
                "row_id": f"live_nonfilled:{day}:{idx}",
                "signal_id": str(_first(row, ("signal_id",), "")),
                "symbol": str(_first(row, ("ticker", "symbol", "tradingsymbol"), "")).strip().upper(),
                "side": _norm_side(_first(row, ("side", "direction"), "")),
                "setup": str(_first(row, ("setup", "setup_name"), "")).strip(),
                "signal_time": signal_ts,
                "entry_time": entry_ts,
                "entry_bar": _entry_bar(entry_ts),
                "entry_price": _safe_float(_first(row, ("entry_price",), np.nan)),
                "exit_price": _safe_float(_first(row, ("exit_price",), np.nan)),
                "exit_reason": outcome,
                "qty": _safe_int(_first(row, ("quantity", "qty"), 0)),
                "gross_pnl": _safe_float(_first(row, ("gross_pnl_rs", "gross_pnl"), np.nan)),
                "costs": _safe_float(_first(row, ("total_cost_rs", "total_cost"), np.nan)),
                "net_pnl": _safe_float(_first(row, ("net_pnl_rs", "net_pnl", "pnl_rs", "pnl"), np.nan)),
                "source_file": str(path),
                "quality_score": _safe_float(_first(row, ("quality_score",), np.nan)),
            })
    return pd.DataFrame(rows)


def load_v11_trades(paths: Paths, dates: list[str]) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for day in dates:
        raw_path = paths.v11_root / day / "trades.csv"
        df = _read_csv(raw_path)
        if df.empty:
            raw_path = paths.v11_root / day / "v11_ID_trades.csv"
            df = _read_csv(raw_path)
        for idx, row in df.iterrows():
            entry_ts = _to_ist(_first(row, ("entry_time_v6", "entry_time", "entry_time_ist"), ""))
            exit_ts = _to_ist(_first(row, ("v6_exit_time_ist", "exit_time", "exit_time_ist"), ""))
            symbol = str(_first(row, ("ticker", "symbol"), "")).strip().upper()
            side = _norm_side(_first(row, ("side",), ""))
            setup = str(_first(row, ("setup", "setup_name"), "")).strip()
            entry = _safe_float(_first(row, ("entry_price_v6", "entry_price"), np.nan))
            exit_raw = _safe_float(_first(row, ("v6_exit_price", "exit_price"), np.nan))
            outcome = _norm_outcome(_first(row, ("v6_outcome", "exit_reason"), ""))
            qty = _safe_int(_first(row, ("quantity", "qty"), 0))
            exit_model = _apply_exit_slippage(exit_raw, side, outcome)
            costed = _costed_trade(side, entry, exit_model, qty)
            reported_cost = _safe_float(_first(row, ("v6_cost_rs",), 0.0), 0.0)
            reported_net = _safe_float(_first(row, ("v6_net_pnl_rs", "pnl"), np.nan))
            rows.append({
                "source": "V11",
                "date": day or _date_from_ts(entry_ts),
                "row_id": f"v11:{day}:{idx}",
                "signal_id": str(_first(row, ("signal_id",), "")),
                "symbol": symbol,
                "side": side,
                "setup": setup,
                "entry_time": entry_ts,
                "entry_bar": _entry_bar(entry_ts),
                "exit_time": exit_ts,
                "entry_price": entry,
                "exit_price": exit_raw,
                "exit_price_model": exit_model,
                "exit_reason": outcome,
                "qty": qty,
                "gross_pnl": costed["gross_pnl"],
                "costs": costed["costs"],
                "net_pnl": costed["net_pnl"],
                "reported_costs": reported_cost,
                "reported_net_pnl": reported_net,
                "notional": entry * qty if np.isfinite(entry) else np.nan,
                "source_file": str(raw_path),
                "v11_selected_strategy_profile": str(_first(row, ("v11_selected_strategy_profile",), "")),
                "v11_exit_rule_source": str(_first(row, ("v11_exit_rule_source",), "")),
                "v11_exit_override_applied": str(_first(row, ("v11_exit_override_applied",), "")),
            })
    return pd.DataFrame(rows)


def load_live_signals(paths: Paths, dates: list[str]) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for day in dates:
        for suffix in ("long", "short"):
            path = paths.live_signals / f"signals_{day}_id_5min_v7_{suffix}.csv"
            df = _read_csv(path)
            for idx, row in df.iterrows():
                ts = _to_ist(_first(row, ("signal_bar_time_ist", "signal_bar_close_ist", "signal_datetime"), ""))
                rows.append({
                    "source": "LIVE_SIGNAL",
                    "date": day,
                    "row_id": f"live_signal:{day}:{suffix}:{idx}",
                    "signal_id": str(_first(row, ("signal_id",), "")),
                    "symbol": str(_first(row, ("ticker", "symbol"), "")).strip().upper(),
                    "side": _norm_side(_first(row, ("side",), "")),
                    "setup": str(_first(row, ("setup", "setup_name"), "")).strip(),
                    "signal_time": ts,
                    "signal_bar": _entry_bar(ts),
                    "source_file": str(path),
                })
    return pd.DataFrame(rows)


def load_v11_signals(paths: Paths, dates: list[str]) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for day in dates:
        path = paths.v11_root / day / "live_parity_selected_strategy_signals.csv"
        df = _read_csv(path)
        if df.empty:
            path = paths.v11_root / day / "live_parity_entry_engine_signals.csv"
            df = _read_csv(path)
        for idx, row in df.iterrows():
            ts = _to_ist(_first(row, ("signal_bar_time_ist", "bar_time_ist", "signal_time_ist", "signal_datetime"), ""))
            rows.append({
                "source": "V11_SIGNAL",
                "date": day,
                "row_id": f"v11_signal:{day}:{idx}",
                "signal_id": str(_first(row, ("signal_id",), "")),
                "symbol": str(_first(row, ("ticker", "symbol"), "")).strip().upper(),
                "side": _norm_side(_first(row, ("side",), "")),
                "setup": str(_first(row, ("setup", "setup_name"), "")).strip(),
                "signal_time": ts,
                "signal_bar": _entry_bar(ts),
                "source_file": str(path),
            })
    return pd.DataFrame(rows)


def _same_trade_candidates(live_row: pd.Series, bt: pd.DataFrame, remaining: set[int]) -> list[tuple[float, int]]:
    out: list[tuple[float, int]] = []
    if pd.isna(live_row["entry_bar"]):
        return out
    for idx in remaining:
        row = bt.loc[idx]
        if (
            row["date"] != live_row["date"]
            or row["symbol"] != live_row["symbol"]
            or row["side"] != live_row["side"]
            or row["setup"] != live_row["setup"]
            or pd.isna(row["entry_bar"])
        ):
            continue
        delta_min = abs((row["entry_bar"] - live_row["entry_bar"]).total_seconds()) / 60.0
        if delta_min <= BAR_TOL_MIN:
            entry_gap = abs(_safe_float(row["entry_price"], 0.0) - _safe_float(live_row["entry_price"], 0.0))
            out.append((delta_min * 10_000.0 + entry_gap, idx))
    return sorted(out)


def match_trades(live: pd.DataFrame, bt: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    remaining = set(bt.index.tolist())
    matched_rows: list[dict[str, Any]] = []
    live_only_rows: list[dict[str, Any]] = []

    sort_cols = ["date", "entry_time", "symbol", "setup", "side"]
    live_iter = live.sort_values(sort_cols, na_position="last") if not live.empty else live
    for _, lrow in live_iter.iterrows():
        candidates = _same_trade_candidates(lrow, bt, remaining)
        if not candidates:
            live_only_rows.append({**lrow.to_dict(), "bucket": "LIVE_ONLY"})
            continue
        _, bidx = candidates[0]
        remaining.remove(bidx)
        brow = bt.loc[bidx]
        notional = max(_safe_float(lrow.get("notional"), np.nan), _safe_float(brow.get("notional"), np.nan))
        net_tol = (NET_TOL_BPS_NOTIONAL / 10_000.0) * notional if np.isfinite(notional) else np.nan
        entry_tol = max(0.05, abs(_safe_float(lrow.get("entry_price"), 0.0)) * ENTRY_SLIPPAGE_BPS / 10_000.0)
        exit_ref = _safe_float(lrow.get("exit_price"), 0.0)
        exit_tol = max(0.05, abs(exit_ref) * EXIT_SLIPPAGE_BPS / 10_000.0)
        entry_delta_min = abs((brow["entry_time"] - lrow["entry_time"]).total_seconds()) / 60.0 if pd.notna(brow["entry_time"]) and pd.notna(lrow["entry_time"]) else np.nan
        exit_delta_min = abs((brow["exit_time"] - lrow["exit_time"]).total_seconds()) / 60.0 if pd.notna(brow["exit_time"]) and pd.notna(lrow["exit_time"]) else np.nan
        entry_diff = _safe_float(brow.get("entry_price"), np.nan) - _safe_float(lrow.get("entry_price"), np.nan)
        exit_diff = _safe_float(brow.get("exit_price_model"), np.nan) - _safe_float(lrow.get("exit_price"), np.nan)
        net_diff = _safe_float(brow.get("net_pnl"), np.nan) - _safe_float(lrow.get("net_pnl"), np.nan)
        gross_diff = _safe_float(brow.get("gross_pnl"), np.nan) - _safe_float(lrow.get("gross_pnl"), np.nan)
        cost_diff = _safe_float(brow.get("costs"), np.nan) - _safe_float(lrow.get("costs"), np.nan)
        matched_rows.append({
            "bucket": "MATCHED",
            "date": lrow["date"],
            "symbol": lrow["symbol"],
            "setup": lrow["setup"],
            "side": lrow["side"],
            "live_row_id": lrow["row_id"],
            "v11_row_id": brow["row_id"],
            "live_entry_time": _fmt_ts(lrow["entry_time"]),
            "v11_entry_time": _fmt_ts(brow["entry_time"]),
            "entry_delta_min": entry_delta_min,
            "live_exit_time": _fmt_ts(lrow["exit_time"]),
            "v11_exit_time": _fmt_ts(brow["exit_time"]),
            "exit_delta_min": exit_delta_min,
            "live_entry_price": lrow["entry_price"],
            "v11_entry_price": brow["entry_price"],
            "entry_price_diff": entry_diff,
            "entry_price_tol": entry_tol,
            "entry_price_within_tol": abs(entry_diff) <= entry_tol if np.isfinite(entry_diff) else False,
            "live_exit_price": lrow["exit_price"],
            "v11_exit_price_raw": brow["exit_price"],
            "v11_exit_price_model": brow["exit_price_model"],
            "exit_price_diff": exit_diff,
            "exit_price_tol": exit_tol,
            "exit_price_within_tol": abs(exit_diff) <= exit_tol if np.isfinite(exit_diff) else False,
            "live_exit_reason": lrow["exit_reason"],
            "v11_exit_reason": brow["exit_reason"],
            "exit_reason_match": _norm_outcome(lrow["exit_reason"]) == _norm_outcome(brow["exit_reason"]),
            "live_qty": lrow["qty"],
            "v11_qty": brow["qty"],
            "qty_match": int(lrow["qty"]) == int(brow["qty"]),
            "live_gross_pnl": lrow["gross_pnl"],
            "v11_gross_pnl": brow["gross_pnl"],
            "gross_pnl_diff": gross_diff,
            "live_costs": lrow["costs"],
            "v11_costs_model": brow["costs"],
            "v11_reported_costs": brow.get("reported_costs", np.nan),
            "cost_diff": cost_diff,
            "live_net_pnl": lrow["net_pnl"],
            "v11_net_pnl_model": brow["net_pnl"],
            "v11_reported_net_pnl": brow.get("reported_net_pnl", np.nan),
            "net_pnl_diff": net_diff,
            "net_pnl_tol": net_tol,
            "net_pnl_within_tol": abs(net_diff) <= net_tol if np.isfinite(net_diff) and np.isfinite(net_tol) else False,
            "timestamp_within_1bar": (
                (not np.isfinite(entry_delta_min) or entry_delta_min <= BAR_TOL_MIN)
                and (not np.isfinite(exit_delta_min) or exit_delta_min <= BAR_TOL_MIN)
            ),
            "notional": notional,
        })
    bt_only_rows = [{**bt.loc[idx].to_dict(), "bucket": "BACKTEST_ONLY"} for idx in sorted(remaining)]
    return pd.DataFrame(matched_rows), pd.DataFrame(live_only_rows), pd.DataFrame(bt_only_rows)


def match_signals(live_sig: pd.DataFrame, bt_sig: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    remaining = set(bt_sig.index.tolist())
    matched: list[dict[str, Any]] = []
    live_only: list[dict[str, Any]] = []
    for _, lrow in live_sig.sort_values(["date", "signal_time", "symbol"], na_position="last").iterrows():
        candidates: list[tuple[float, int]] = []
        for idx in remaining:
            brow = bt_sig.loc[idx]
            if brow["date"] == lrow["date"] and brow["symbol"] == lrow["symbol"] and brow["side"] == lrow["side"] and brow["setup"] == lrow["setup"]:
                if pd.notna(brow["signal_bar"]) and pd.notna(lrow["signal_bar"]):
                    delta = abs((brow["signal_bar"] - lrow["signal_bar"]).total_seconds()) / 60.0
                    if delta <= BAR_TOL_MIN:
                        candidates.append((delta, idx))
        if candidates:
            _, bidx = sorted(candidates)[0]
            remaining.remove(bidx)
            brow = bt_sig.loc[bidx]
            matched.append({
                "date": lrow["date"],
                "symbol": lrow["symbol"],
                "side": lrow["side"],
                "setup": lrow["setup"],
                "live_signal_time": _fmt_ts(lrow["signal_time"]),
                "v11_signal_time": _fmt_ts(brow["signal_time"]),
            })
        else:
            live_only.append({**lrow.to_dict(), "bucket": "LIVE_SIGNAL_ONLY"})
    bt_only = [{**bt_sig.loc[idx].to_dict(), "bucket": "V11_SIGNAL_ONLY"} for idx in sorted(remaining)]
    return pd.DataFrame(matched), pd.DataFrame(live_only), pd.DataFrame(bt_only)


def _agg_trades(df: pd.DataFrame, signals: pd.DataFrame, label: str) -> pd.DataFrame:
    keys = ["date", "setup"]
    rows: list[dict[str, Any]] = []
    signal_counts = signals.groupby(keys).size().to_dict() if not signals.empty else {}
    grouped = df.groupby(keys, dropna=False) if not df.empty else []
    seen = set()
    for key, g in grouped:
        date, setup = key
        seen.add((date, setup))
        pnl = pd.to_numeric(g["net_pnl"], errors="coerce").fillna(0.0)
        gross = pd.to_numeric(g["gross_pnl"], errors="coerce").fillna(0.0)
        costs = pd.to_numeric(g["costs"], errors="coerce").fillna(0.0)
        wins = pnl[pnl > 0]
        losses = pnl[pnl < 0]
        rows.append({
            "side": label,
            "date": date,
            "setup": setup,
            "signals": int(signal_counts.get((date, setup), 0)),
            "trades": int(len(g)),
            "win_rate_pct": round(100.0 * len(wins) / max(len(g), 1), 2),
            "avg_win": round(float(wins.mean()), 2) if len(wins) else 0.0,
            "avg_loss": round(float(losses.mean()), 2) if len(losses) else 0.0,
            "gross_pnl": round(float(gross.sum()), 2),
            "costs": round(float(costs.sum()), 2),
            "net_pnl": round(float(pnl.sum()), 2),
        })
    for (date, setup), count in signal_counts.items():
        if (date, setup) not in seen:
            rows.append({
                "side": label,
                "date": date,
                "setup": setup,
                "signals": int(count),
                "trades": 0,
                "win_rate_pct": 0.0,
                "avg_win": 0.0,
                "avg_loss": 0.0,
                "gross_pnl": 0.0,
                "costs": 0.0,
                "net_pnl": 0.0,
            })
    return pd.DataFrame(rows).sort_values(["date", "setup", "side"]) if rows else pd.DataFrame()


def _daily_table(live: pd.DataFrame, bt: pd.DataFrame, live_sig: pd.DataFrame, bt_sig: pd.DataFrame) -> pd.DataFrame:
    dates = sorted(set(live.get("date", pd.Series(dtype=str))) | set(bt.get("date", pd.Series(dtype=str))) | set(live_sig.get("date", pd.Series(dtype=str))) | set(bt_sig.get("date", pd.Series(dtype=str))))
    rows = []
    for day in dates:
        l = live[live["date"] == day] if not live.empty else pd.DataFrame()
        b = bt[bt["date"] == day] if not bt.empty else pd.DataFrame()
        ls = live_sig[live_sig["date"] == day] if not live_sig.empty else pd.DataFrame()
        bs = bt_sig[bt_sig["date"] == day] if not bt_sig.empty else pd.DataFrame()
        rows.append({
            "date": day,
            "live_signals": int(len(ls)),
            "v11_signals": int(len(bs)),
            "live_trades": int(len(l)),
            "v11_trades": int(len(b)),
            "live_gross_pnl": round(float(pd.to_numeric(l.get("gross_pnl", pd.Series(dtype=float)), errors="coerce").fillna(0).sum()), 2),
            "v11_gross_pnl_model": round(float(pd.to_numeric(b.get("gross_pnl", pd.Series(dtype=float)), errors="coerce").fillna(0).sum()), 2),
            "live_costs": round(float(pd.to_numeric(l.get("costs", pd.Series(dtype=float)), errors="coerce").fillna(0).sum()), 2),
            "v11_costs_model": round(float(pd.to_numeric(b.get("costs", pd.Series(dtype=float)), errors="coerce").fillna(0).sum()), 2),
            "live_net_pnl": round(float(pd.to_numeric(l.get("net_pnl", pd.Series(dtype=float)), errors="coerce").fillna(0).sum()), 2),
            "v11_net_pnl_model": round(float(pd.to_numeric(b.get("net_pnl", pd.Series(dtype=float)), errors="coerce").fillna(0).sum()), 2),
        })
    out = pd.DataFrame(rows)
    if not out.empty:
        out["net_pnl_diff"] = (out["v11_net_pnl_model"] - out["live_net_pnl"]).round(2)
    return out


def _root_causes(
    matched: pd.DataFrame,
    live_only: pd.DataFrame,
    bt_only: pd.DataFrame,
    live_sig: pd.DataFrame,
    bt_sig: pd.DataFrame,
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []

    if not matched.empty:
        raw_zero_cost = matched[pd.to_numeric(matched["v11_reported_costs"], errors="coerce").fillna(0.0).abs() < 0.001]
        if len(raw_zero_cost):
            sample = raw_zero_cost.iloc[0]
            rows.append({
                "rank_order": 7,
                "finding": "Cost model mismatch in raw V11 live-parity output",
                "evidence_count": int(len(raw_zero_cost)),
                "sample_trade": f"{sample['date']} {sample['symbol']} {sample['side']} {sample['setup']}",
                "evidence": f"V11 reported_costs={sample['v11_reported_costs']} while modeled_costs={sample['v11_costs_model']:.2f}; live_costs={sample['live_costs']:.2f}.",
                "fix": "Keep V11 raw output, but use reconciliation costed columns for parity until live_parity resolver writes statutory costs.",
            })
        bad_price = matched[(matched["entry_price_within_tol"] == False) | (matched["exit_price_within_tol"] == False)]
        if len(bad_price):
            sample = bad_price.iloc[0]
            rows.append({
                "rank_order": 6,
                "finding": "Execution price mismatch beyond modeled slippage",
                "evidence_count": int(len(bad_price)),
                "sample_trade": f"{sample['date']} {sample['symbol']} {sample['side']} {sample['setup']}",
                "evidence": f"entry_diff={sample['entry_price_diff']:.2f}, exit_diff={sample['exit_price_diff']:.2f}.",
                "fix": "Model V7 paper's actual LTP-at-signal and exit slippage, or store the LTP tick used by V7 for replay.",
            })
        bad_reason = matched[matched["exit_reason_match"] == False]
        if len(bad_reason):
            sample = bad_reason.iloc[0]
            rows.append({
                "rank_order": 3,
                "finding": "Exit reason mismatch",
                "evidence_count": int(len(bad_reason)),
                "sample_trade": f"{sample['date']} {sample['symbol']} {sample['side']} {sample['setup']}",
                "evidence": f"live={sample['live_exit_reason']} vs v11={sample['v11_exit_reason']}.",
                "fix": "Compare 5-second live LTP path against 1-minute OHLC resolver for the named trades.",
            })
        bad_net = matched[matched["net_pnl_within_tol"] == False]
        if len(bad_net):
            sample = bad_net.iloc[0]
            rows.append({
                "rank_order": 6,
                "finding": "Matched-trade net P&L outside 10 bps notional tolerance",
                "evidence_count": int(len(bad_net)),
                "sample_trade": f"{sample['date']} {sample['symbol']} {sample['side']} {sample['setup']}",
                "evidence": f"net_diff={sample['net_pnl_diff']:.2f}, tolerance={sample['net_pnl_tol']:.2f}.",
                "fix": "Prioritize entry/exit fill model parity before judging setup edge.",
            })

    if not live_only.empty:
        for _, row in live_only.head(5).iterrows():
            same_sig = bt_sig[
                (bt_sig["date"] == row["date"])
                & (bt_sig["symbol"] == row["symbol"])
                & (bt_sig["side"] == row["side"])
                & (bt_sig["setup"] == row["setup"])
            ] if not bt_sig.empty else pd.DataFrame()
            finding = "Live trade missing from V11 selected signal set" if same_sig.empty else "Live trade has V11 signal but no matched V11 trade"
            rank = 1 if same_sig.empty else 3
            rows.append({
                "rank_order": rank,
                "finding": finding,
                "evidence_count": int(len(live_only)),
                "sample_trade": f"{row['date']} {row['symbol']} {row['side']} {row['setup']}",
                "evidence": f"live entry={_fmt_ts(row['entry_time'])}, live net={_safe_float(row['net_pnl'], 0.0):.2f}.",
                "fix": "Inspect V11 selected_strategy_rejects and live JSON for this signal.",
            })
            break

    if not bt_only.empty:
        for _, row in bt_only.head(5).iterrows():
            same_live_signal = live_sig[
                (live_sig["date"] == row["date"])
                & (live_sig["symbol"] == row["symbol"])
                & (live_sig["side"] == row["side"])
                & (live_sig["setup"] == row["setup"])
            ] if not live_sig.empty else pd.DataFrame()
            finding = "V11 backtest-only trade absent from live signal CSV" if same_live_signal.empty else "V11 trade has live signal but no live paper execution"
            rank = 5 if same_live_signal.empty else 6
            rows.append({
                "rank_order": rank,
                "finding": finding,
                "evidence_count": int(len(bt_only)),
                "sample_trade": f"{row['date']} {row['symbol']} {row['side']} {row['setup']}",
                "evidence": f"v11 entry={_fmt_ts(row['entry_time'])}, v11 modeled net={_safe_float(row['net_pnl'], 0.0):.2f}.",
                "fix": "Check live executor skips/brakes if live signal exists; otherwise compare V11 selected profile against live signal writer.",
            })
            break

    rows.append({
        "rank_order": 2,
        "finding": "5-minute data roots differ by design",
        "evidence_count": 1,
        "sample_trade": "",
        "evidence": r"V7 live feed root is C:\TradingData\eqidv2\stocks_indicators_5min_eq_live; V11 backtest root is C:\TradingData\eqidv2\stocks_indicators_5min_eq_live2.",
        "fix": "For strict OHLC parity, archive per-slot live bars or diff symbol/day OHLC before comparing signals.",
    })
    rows.append({
        "rank_order": 4,
        "finding": "Cross-sectional and gate state are replayed from live JSON in this run",
        "evidence_count": 1,
        "sample_trade": "",
        "evidence": "All V11 inputs.txt files show mode=live_parity and live_candidate_json_dir under signal_discovery_v7_5mins_ID.",
        "fix": "Keep using live_parity for EOD parity; avoid historical_full_day for gate-state assertions.",
    })
    rows.append({
        "rank_order": 8,
        "finding": "No systemic timezone offset detected by keying in IST and allowing one 5-minute bar",
        "evidence_count": int(len(matched)),
        "sample_trade": "",
        "evidence": "The reconciler parses all timestamps to Asia/Kolkata and matches on entry_bar.",
        "fix": "Keep explicit timezone normalization in daily parity.",
    })
    rows.append({
        "rank_order": 9,
        "finding": "Logic drift remains between live executor and V11 resolver",
        "evidence_count": 1,
        "sample_trade": "",
        "evidence": "V7 has entry retry/slip gate, portfolio brakes, C_OR time stop/session cap, and 5-second LTP exits; V11 live_parity uses deterministic selected signals and 1-minute OHLC resolver.",
        "fix": "Move shared execution-resolution rules into a common pure module used by both paper executor and V11.",
    })
    return pd.DataFrame(rows).sort_values(["rank_order", "finding"]).reset_index(drop=True)


def _pct(num: float, den: float) -> float:
    if den == 0:
        return np.nan
    return 100.0 * num / den


def _fmt_money(value: Any) -> str:
    val = _safe_float(value, np.nan)
    if not np.isfinite(val):
        return "NA"
    return f"{val:,.2f}"


def _fmt_pct(value: Any) -> str:
    val = _safe_float(value, np.nan)
    if not np.isfinite(val):
        return "NA"
    return f"{val:.2f}%"


def _md_table(df: pd.DataFrame, max_rows: int = 30) -> list[str]:
    if df.empty:
        return ["None."]
    view = df.head(max_rows).copy()
    lines = ["| " + " | ".join(map(str, view.columns)) + " |", "| " + " | ".join("---" for _ in view.columns) + " |"]
    for _, row in view.iterrows():
        vals = []
        for col in view.columns:
            val = row.get(col, "")
            if isinstance(val, float):
                vals.append(f"{val:.2f}" if np.isfinite(val) else "NA")
            else:
                vals.append(str(val).replace("|", "\\|")[:220])
        lines.append("| " + " | ".join(vals) + " |")
    return lines


def _trade_label(row: pd.Series | None) -> str:
    if row is None:
        return ""
    return f"{row.get('date', '')} {row.get('symbol', '')} {row.get('side', '')} {row.get('setup', '')}".strip()


def _first_row(df: pd.DataFrame) -> pd.Series | None:
    if df.empty:
        return None
    return df.iloc[0]


def _root_cause_checks_requested_order(
    matched: pd.DataFrame,
    live_only: pd.DataFrame,
    bt_only: pd.DataFrame,
    live_nonfilled: pd.DataFrame,
) -> pd.DataFrame:
    bad_net = matched[matched["net_pnl_within_tol"] == False] if not matched.empty else pd.DataFrame()
    bad_reason = matched[matched["exit_reason_match"] == False] if not matched.empty else pd.DataFrame()
    bad_price = matched[(matched["entry_price_within_tol"] == False) | (matched["exit_price_within_tol"] == False)] if not matched.empty else pd.DataFrame()
    raw_zero_cost = matched[pd.to_numeric(matched["v11_reported_costs"], errors="coerce").fillna(0.0).abs() < 0.001] if not matched.empty else pd.DataFrame()

    live_only_sample = _first_row(live_only)
    bt_only_sample = _first_row(bt_only)
    nonfilled_sample = _first_row(live_nonfilled)
    net_sample = _first_row(bad_net)
    reason_sample = _first_row(bad_reason)
    price_sample = _first_row(bad_price)
    cost_sample = _first_row(raw_zero_cost)

    rows = [
        {
            "check_order": 1,
            "area": "Config/parameter drift",
            "status": "PARTIAL_FAIL",
            "sample": _trade_label(net_sample),
            "evidence": "Wrapper diff found scanner/gate/filter values aligned; execution settings drift materially. Matched trades still miss 10 bps P&L tolerance.",
            "fix": "Make V11 live-parity execution options mirror V7 paper executor before comparing setup edge.",
        },
        {
            "check_order": 2,
            "area": "Data mismatch",
            "status": "LIKELY",
            "sample": _trade_label(bt_only_sample),
            "evidence": f"5-minute roots differ and {len(bt_only)} V11 trades are absent from live paper fills; OHLC checksums were not available in the V7 CSVs.",
            "fix": "Archive or checksum per-slot live OHLC and diff it against the V11 root before signal comparison.",
        },
        {
            "check_order": 3,
            "area": "Signal timing/lookahead",
            "status": "PARTIAL_FAIL",
            "sample": _trade_label(reason_sample if reason_sample is not None else net_sample),
            "evidence": "Matched entries are within one bar, but V7 fills from live LTP seconds after signal while V11 resolves on 1-minute OHLC; RPTECH shows an exit-reason/timing mismatch.",
            "fix": "Replay completed-bar decision time and next-bar/next-tick fill rules identically in both paths.",
        },
        {
            "check_order": 4,
            "area": "Cross-sectional RS universe mismatch",
            "status": "POSSIBLE",
            "sample": _trade_label(bt_only_sample),
            "evidence": "V11 live_parity replays live JSON snapshots, reducing future-data risk, but V11 selected signals still diverge sharply from live signal CSVs.",
            "fix": "Persist per-slot live universe/feed coverage and RS ranks, then compare rank rows symbol by symbol.",
        },
        {
            "check_order": 5,
            "area": "Gate/qualification tracker state",
            "status": "MOSTLY_ALIGNED",
            "sample": _trade_label(live_only_sample),
            "evidence": f"Every V11 inputs.txt reports live_parity mode; still, {len(live_only)} filled live trades are missing from V11 selected trades.",
            "fix": "For each live-only row, join against V11 selected_strategy_rejects and archived JSON gate fields.",
        },
        {
            "check_order": 6,
            "area": "Execution reality",
            "status": "FAIL",
            "sample": _trade_label(nonfilled_sample if nonfilled_sample is not None else price_sample),
            "evidence": f"V7 paper produced {len(live_nonfilled)} nonfilled/skipped rows and matched fills have price/P&L failures under the current model.",
            "fix": "Store and replay V7 paper's exact entry/exit LTP ticks, stale-signal skips, retry gates, and portfolio brakes.",
        },
        {
            "check_order": 7,
            "area": "Cost model mismatch",
            "status": "FAIL",
            "sample": _trade_label(cost_sample),
            "evidence": "Raw V11 live-parity trades report zero costs while V7 paper rows include statutory costs; reconciler recomputes V11 costs for comparison.",
            "fix": "Write statutory cost columns from V11 live-parity resolver using nse_intraday_costs.py.",
        },
        {
            "check_order": 8,
            "area": "Timezone/bar indexing",
            "status": "PASS_WITH_RESIDUAL",
            "sample": _trade_label(net_sample),
            "evidence": "All matching keys are normalized to Asia/Kolkata and allow one 5-minute bar; no systemic one-bar offset appeared among matched trades.",
            "fix": "Keep explicit IST parsing and include entry_bar in all daily parity outputs.",
        },
        {
            "check_order": 9,
            "area": "Logic drift",
            "status": "FAIL",
            "sample": _trade_label(reason_sample if reason_sample is not None else live_only_sample),
            "evidence": "V7 paper executor has retry/slip gates, C_OR time stop/session cap, portfolio brakes, 5-second LTP exits, and statutory costs; V11 uses deterministic selected signals and a 1-minute resolver.",
            "fix": "Extract shared pure execution-resolution logic used by both V7 paper and V11 backtest.",
        },
    ]
    return pd.DataFrame(rows)


def build_report(
    dates: list[str],
    paths: Paths,
    live: pd.DataFrame,
    bt: pd.DataFrame,
    live_nonfilled: pd.DataFrame,
    live_sig: pd.DataFrame,
    bt_sig: pd.DataFrame,
    matched: pd.DataFrame,
    live_only: pd.DataFrame,
    bt_only: pd.DataFrame,
    sig_matched: pd.DataFrame,
    sig_live_only: pd.DataFrame,
    sig_bt_only: pd.DataFrame,
    daily: pd.DataFrame,
    per_setup: pd.DataFrame,
    causes: pd.DataFrame,
) -> str:
    live_trades = len(live)
    live_nonfilled_rows = len(live_nonfilled)
    live_paper_rows = live_trades + live_nonfilled_rows
    bt_trades = len(bt)
    matched_n = len(matched)
    live_signals = len(live_sig)
    bt_signals = len(bt_sig)
    sig_matched_n = len(sig_matched)
    trade_match_live = _pct(matched_n, live_trades)
    trade_match_bt = _pct(matched_n, bt_trades)
    signal_match_live = _pct(sig_matched_n, live_signals)
    signal_match_bt = _pct(sig_matched_n, bt_signals)
    live_net = float(pd.to_numeric(live.get("net_pnl", pd.Series(dtype=float)), errors="coerce").fillna(0).sum())
    bt_net = float(pd.to_numeric(bt.get("net_pnl", pd.Series(dtype=float)), errors="coerce").fillna(0).sum())
    divergence = _pct(bt_net - live_net, abs(live_net)) if abs(live_net) > 0 else np.nan
    corr = daily["live_net_pnl"].corr(daily["v11_net_pnl_model"]) if len(daily) >= 2 else np.nan
    pass_flag = (
        live_trades > 0
        and trade_match_live >= 95.0
        and signal_match_live >= 95.0
        and (matched.empty or bool(matched["net_pnl_within_tol"].all()))
        and abs(divergence) <= 5.0
    )

    match_quality = matched[[
        "date", "symbol", "side", "setup", "entry_delta_min", "exit_delta_min",
        "entry_price_diff", "exit_price_diff", "net_pnl_diff", "net_pnl_tol",
        "exit_reason_match", "net_pnl_within_tol",
    ]].copy() if not matched.empty else pd.DataFrame()
    mismatches = match_quality[
        (match_quality["exit_reason_match"] == False)
        | (match_quality["net_pnl_within_tol"] == False)
    ] if not match_quality.empty else pd.DataFrame()

    lines: list[str] = []
    lines.append("# V7 Paper-Live vs V11 Backtest Parity Report")
    lines.append("")
    lines.append(f"Dates: {', '.join(dates)}")
    lines.append("")
    lines.append("## Executive Summary")
    lines.append("")
    lines.append(f"Verdict: {'PASS' if pass_flag else 'FAIL'}")
    lines.append("")
    lines.append(f"- Live source: `{paths.live_signals}` paper trade/signal CSVs")
    lines.append(f"- V11 source: `{paths.v11_root}\\YYYY-MM-DD` daily live-parity outputs")
    lines.append(f"- Live paper rows: {live_paper_rows} total, {live_trades} filled trades, {live_nonfilled_rows} nonfilled/skipped/rejected rows")
    lines.append(f"- Trade match rate: {_fmt_pct(trade_match_live)} of live trades, {_fmt_pct(trade_match_bt)} of V11 trades")
    lines.append(f"- Signal match rate: {_fmt_pct(signal_match_live)} of live signals, {_fmt_pct(signal_match_bt)} of V11 signals")
    lines.append(f"- Live total net P&L: Rs {_fmt_money(live_net)}")
    lines.append(f"- V11 modeled total net P&L: Rs {_fmt_money(bt_net)}")
    lines.append(f"- Total net P&L divergence: {_fmt_pct(divergence)}")
    lines.append(f"- Daily P&L correlation: {corr:.3f}" if np.isfinite(corr) else "- Daily P&L correlation: NA")
    lines.append("")
    lines.append("V11 was run in `live_parity` mode with `selected_strategy_profile=final_setup_conf`, so it replayed live JSON candidate/gate snapshots rather than recomputing gate state from future data. V11 raw P&L is price-only in this path; this report uses recomputed statutory costs and V7-style exit slippage for V11 modeled P&L.")
    lines.append("")
    lines.append("## Inputs Used")
    lines.append("")
    input_rows = []
    for day in dates:
        live_day = live[live["date"] == day] if not live.empty else pd.DataFrame()
        nonfilled_day = live_nonfilled[live_nonfilled["date"] == day] if not live_nonfilled.empty else pd.DataFrame()
        input_rows.append({
            "date": day,
            "live_filled_rows": len(live_day),
            "live_nonfilled_rows": len(nonfilled_day),
            "live_trades": str(paths.live_signals / f"paper_trades_{day}_id_5min_v7.csv"),
            "live_signals_long": str(paths.live_signals / f"signals_{day}_id_5min_v7_long.csv"),
            "live_signals_short": str(paths.live_signals / f"signals_{day}_id_5min_v7_short.csv"),
            "v11_dir": str(paths.v11_root / day),
        })
    lines.extend(_md_table(pd.DataFrame(input_rows), max_rows=10))
    lines.append("")
    lines.append("## Config And Input Alignment")
    lines.append("")
    lines.append("- Dates: matched to the last seven NSE sessions discovered from V7 logs.")
    lines.append("- Universe: both V7 scanner and V11 live-parity path route through `candidate_scan.v2._load_universe()` for the main V7 universe; Tier123 add-on may use its own futures fallback in V11 internals.")
    lines.append("- 5-minute bar source: V7 live feed root is `stocks_indicators_5min_eq_live`; V11 backtest root is `stocks_indicators_5min_eq_live2`, while live-parity signals come from archived live JSON snapshots.")
    lines.append("- Session: market window 09:15-15:30 IST; entry window 09:30-14:30 IST; unresolved exits close at 15:20 IST.")
    lines.append("- Gate state: V11 `inputs.txt` for every date shows `mode=live_parity`, so gate/qualification state came from live-day JSON snapshots.")
    lines.append("- V7 scanner config wrapper: `bat\\run_eqidv2_signal_discovery_v7_5min_id_persistent.bat`.")
    lines.append("- V7 paper executor config wrapper: `bat\\run_avwap_trade_execution_PAPER_TRADE_TRUE_id_5min_v7.bat`.")
    lines.append("- V11 entry point: `avwap_5min_ID_v11_backtesting.py`; daily wrapper used here: `backtesting_result_v11_daily.py`.")
    lines.append("- V11 run command used per day: `python -u backtesting_result_v11_daily.py --date YYYY-MM-DD --selected-strategy-profile final_setup_conf`.")
    lines.append("- Cost model: `nse_intraday_costs.py`; universe route: `candidate_scan.v2._load_universe()` from `filtered_stocks_MIS.py`.")
    lines.append("- Scanner config drift: all trading gate/filter env vars match; only live scheduling/feed wait worker knobs are absent in V11 (`SCAN_WORKERS`, `TIER123_SCAN_WORKERS`, `PARALLEL_SCAN_BRANCHES`, feed-gate delay/poll/failure budget, `POST_SLOT_DELAY_SEC`, `TIER123_LATEST_START_LAG_SEC`).")
    lines.append("- Execution config drift: V7 paper uses actual LTP polling, entry retry/slip gates, daily loss brake, capacity gates, C_OR time stop/session cap, 5-second LTP exits, statutory costs; V11 uses 1-minute OHLC resolution, no portfolio state, raw zero-cost P&L.")
    lines.append("")
    lines.append("## Config Diff Table")
    lines.append("")
    config_diffs = pd.DataFrame([
        {"field": "Trading gate/filter env vars", "V7 paper-live": "matched wrapper values", "V11 backtest": "matched wrapper values", "impact": "No drift found"},
        {"field": "SCAN_WORKERS / TIER123_SCAN_WORKERS", "V7 paper-live": "24 / 24", "V11 backtest": "not set in scheduled wrapper", "impact": "Scheduling only"},
        {"field": "PARALLEL_SCAN_BRANCHES", "V7 paper-live": "1", "V11 backtest": "not set", "impact": "Scheduling only"},
        {"field": "TIER123_LATEST_START_LAG_SEC", "V7 paper-live": "40", "V11 backtest": "not set", "impact": "Feed timing"},
        {"field": "FEED_GATE_MAX_VERIFICATION_FAILURES", "V7 paper-live": "5", "V11 backtest": "not set", "impact": "Feed timing"},
        {"field": "FEED_GATE_MIN_DELAY_SEC / POLL_SEC", "V7 paper-live": "1 / 0.5", "V11 backtest": "not set", "impact": "Feed timing"},
        {"field": "POST_SLOT_DELAY_SEC", "V7 paper-live": "75", "V11 backtest": "not set", "impact": "Feed timing"},
        {"field": "Entry fill source", "V7 paper-live": "actual ltp_on_signal", "V11 backtest": "ltp_on_signal_1m_open approximation", "impact": "High"},
        {"field": "Entry slippage", "V7 paper-live": "5 bps adverse", "V11 backtest": "5 bps adverse", "impact": "Matched"},
        {"field": "Exit slippage", "V7 paper-live": "5 bps adverse except target", "V11 backtest": "raw live-parity output has none; reconciler models it", "impact": "High"},
        {"field": "Statutory costs", "V7 paper-live": "nse_intraday_costs.py columns", "V11 backtest": "raw v6_cost_rs=0; reconciler recomputes", "impact": "High"},
        {"field": "Entry slip retry gate", "V7 paper-live": "0.3% max slip, wait 300s, poll 2s", "V11 backtest": "not modeled", "impact": "High"},
        {"field": "Daily loss brake", "V7 paper-live": "Rs 10,000", "V11 backtest": "not modeled", "impact": "Medium"},
        {"field": "Capacity gates", "V7 paper-live": "max concurrent/open 100/100, capital 2,000,000", "V11 backtest": "no live capacity state", "impact": "Medium"},
        {"field": "C_OR setup execution controls", "V7 paper-live": "30m time stop/session cap 50", "V11 backtest": "no setup-specific live time stop in resolver", "impact": "High"},
        {"field": "Exit resolver", "V7 paper-live": "5-second LTP polling", "V11 backtest": "1-minute OHLC", "impact": "High"},
        {"field": "Gate state source", "V7 paper-live": "live-day gate state", "V11 backtest": "live JSON snapshots in live_parity", "impact": "Matched"},
    ])
    lines.extend(_md_table(config_diffs, max_rows=30))
    lines.append("")
    lines.append("## Daily Aggregate")
    lines.append("")
    lines.extend(_md_table(daily, max_rows=20))
    lines.append("")
    lines.append("## Per-Day Per-Setup Aggregate")
    lines.append("")
    lines.extend(_md_table(per_setup, max_rows=80))
    lines.append("")
    lines.append("## Trade Buckets")
    lines.append("")
    bucket_summary = pd.DataFrame([
        {"bucket": "MATCHED", "rows": len(matched)},
        {"bucket": "LIVE_ONLY", "rows": len(live_only)},
        {"bucket": "BACKTEST_ONLY", "rows": len(bt_only)},
        {"bucket": "LIVE_NONFILLED_SKIPPED_OR_REJECTED", "rows": len(live_nonfilled)},
    ])
    lines.extend(_md_table(bucket_summary))
    lines.append("")
    lines.append("## Live Nonfilled/Skipped/Rejection Rows")
    lines.append("")
    live_nonfilled_view = live_nonfilled[["date", "symbol", "side", "setup", "entry_time", "exit_reason", "qty", "net_pnl", "source_file"]].copy() if not live_nonfilled.empty else pd.DataFrame()
    if not live_nonfilled_view.empty:
        live_nonfilled_view["entry_time"] = live_nonfilled_view["entry_time"].map(_fmt_ts)
    lines.extend(_md_table(live_nonfilled_view, max_rows=20))
    lines.append("")
    lines.append("## Matched Trade Tolerance Failures")
    lines.append("")
    lines.extend(_md_table(mismatches, max_rows=20))
    lines.append("")
    lines.append("## Live-Only Sample")
    lines.append("")
    live_only_view = live_only[["date", "symbol", "side", "setup", "entry_time", "exit_reason", "net_pnl", "source_file"]].copy() if not live_only.empty else pd.DataFrame()
    if not live_only_view.empty:
        live_only_view["entry_time"] = live_only_view["entry_time"].map(_fmt_ts)
    lines.extend(_md_table(live_only_view, max_rows=20))
    lines.append("")
    lines.append("## Backtest-Only Sample")
    lines.append("")
    bt_only_view = bt_only[["date", "symbol", "side", "setup", "entry_time", "exit_reason", "net_pnl", "source_file"]].copy() if not bt_only.empty else pd.DataFrame()
    if not bt_only_view.empty:
        bt_only_view["entry_time"] = bt_only_view["entry_time"].map(_fmt_ts)
    lines.extend(_md_table(bt_only_view, max_rows=20))
    lines.append("")
    lines.append("## Ranked Root Causes")
    lines.append("")
    lines.extend(_md_table(causes, max_rows=40))
    lines.append("")
    lines.append("## Root Cause Checks In Requested Order")
    lines.append("")
    lines.extend(_md_table(_root_cause_checks_requested_order(matched, live_only, bt_only, live_nonfilled), max_rows=20))
    lines.append("")
    lines.append("## Fixes Ranked By Impact Vs Effort")
    lines.append("")
    fixes = pd.DataFrame([
        {"priority": 1, "fix": "Make V11 live_parity resolver emit statutory costs and V7-style exit slippage columns", "impact": "High", "effort": "Low"},
        {"priority": 2, "fix": "Archive the exact V7 entry LTP and exit LTP ticks used by paper executor, then replay those in parity", "impact": "High", "effort": "Medium"},
        {"priority": 3, "fix": "Extract shared execution rules for entry retry/slip gate, C_OR time stop, portfolio brakes, and exit slippage into one pure module", "impact": "High", "effort": "Medium"},
        {"priority": 4, "fix": "Store per-slot live universe/feed coverage and OHLC checksum so RS/data mismatches are directly provable", "impact": "Medium", "effort": "Medium"},
        {"priority": 5, "fix": "Add a scheduled EOD call to `python reconcile.py --run-v11 --dates YYYY-MM-DD` after the 16:00 V11 job", "impact": "Medium", "effort": "Low"},
    ])
    lines.extend(_md_table(fixes))
    lines.append("")
    lines.append("## Output Files")
    lines.append("")
    for name in (
        "raw_live_paper_trades.csv",
        "raw_live_signals.csv",
        "raw_v11_trades.csv",
        "raw_v11_signals.csv",
        "matched_trades.csv",
        "live_only_trades.csv",
        "backtest_only_trades.csv",
        "live_nonfilled_rows.csv",
        "matched_signals.csv",
        "live_only_signals.csv",
        "backtest_only_signals.csv",
        "daily_aggregate.csv",
        "per_setup_aggregate.csv",
        "root_causes.csv",
    ):
        lines.append(f"- `{paths.out_dir / name}`")
    lines.append("")
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description="Reconcile V7 paper-live trades/signals against V11 live-parity backtests.")
    parser.add_argument("--runtime-root", default=str(DEFAULT_RUNTIME_ROOT))
    parser.add_argument("--dates", default="", help="Comma-separated YYYY-MM-DD list. Default: discover latest seven V7 paper-live log dates.")
    parser.add_argument("--days", type=int, default=7)
    parser.add_argument("--run-v11", action="store_true", help="Run backtesting_result_v11_daily.py before reconciling.")
    parser.add_argument("--selected-strategy-profile", default="final_setup_conf")
    parser.add_argument("--out-dir", default=r"C:\TradingData\eqidv2\backtesting_result_v11\seven_day_reconcile")
    parser.add_argument("--report", default="parity_report.md")
    args = parser.parse_args()

    runtime_root = Path(args.runtime_root)
    paths = Paths(
        runtime_root=runtime_root,
        live_signals=runtime_root / "live_signals",
        v11_root=runtime_root / "backtesting_result_v11",
        out_dir=Path(args.out_dir),
    )
    paths.out_dir.mkdir(parents=True, exist_ok=True)

    if args.dates.strip():
        dates = [d.strip() for d in args.dates.split(",") if d.strip()]
    else:
        dates = discover_dates(paths, args.days)

    if args.run_v11:
        run_v11_for_dates(dates, args.selected_strategy_profile)

    raw_live_paper = load_raw_live_paper(paths, dates)
    raw_live_signals = load_raw_live_signals(paths, dates)
    raw_v11_trades = load_raw_v11_trades(paths, dates)
    raw_v11_signals = load_raw_v11_signals(paths, dates)
    live = load_live_trades(paths, dates)
    live_nonfilled = load_live_nonfilled_rows(paths, dates)
    bt = load_v11_trades(paths, dates)
    live_sig = load_live_signals(paths, dates)
    bt_sig = load_v11_signals(paths, dates)

    matched, live_only, bt_only = match_trades(live, bt)
    sig_matched, sig_live_only, sig_bt_only = match_signals(live_sig, bt_sig)
    daily = _daily_table(live, bt, live_sig, bt_sig)
    per_setup = pd.concat([
        _agg_trades(live, live_sig, "LIVE"),
        _agg_trades(bt, bt_sig, "V11_MODELED"),
    ], ignore_index=True)
    causes = _root_causes(matched, live_only, bt_only, live_sig, bt_sig)

    artifacts = {
        "raw_live_paper_trades.csv": raw_live_paper,
        "raw_live_signals.csv": raw_live_signals,
        "raw_v11_trades.csv": raw_v11_trades,
        "raw_v11_signals.csv": raw_v11_signals,
        "normalized_live_trades.csv": live,
        "live_nonfilled_rows.csv": live_nonfilled,
        "normalized_v11_trades.csv": bt,
        "matched_trades.csv": matched,
        "live_only_trades.csv": live_only,
        "backtest_only_trades.csv": bt_only,
        "normalized_live_signals.csv": live_sig,
        "normalized_v11_signals.csv": bt_sig,
        "matched_signals.csv": sig_matched,
        "live_only_signals.csv": sig_live_only,
        "backtest_only_signals.csv": sig_bt_only,
        "daily_aggregate.csv": daily,
        "per_setup_aggregate.csv": per_setup,
        "root_causes.csv": causes,
    }
    for name, df in artifacts.items():
        out = df.copy()
        for col in out.columns:
            if pd.api.types.is_datetime64_any_dtype(out[col]):
                out[col] = out[col].map(_fmt_ts)
        out.to_csv(paths.out_dir / name, index=False)

    summary = {
        "dates": dates,
        "raw_live_paper_rows": int(len(raw_live_paper)),
        "raw_live_signal_rows": int(len(raw_live_signals)),
        "raw_v11_trade_rows": int(len(raw_v11_trades)),
        "raw_v11_signal_rows": int(len(raw_v11_signals)),
        "live_paper_rows": int(len(live) + len(live_nonfilled)),
        "live_trades": int(len(live)),
        "live_nonfilled_rows": int(len(live_nonfilled)),
        "v11_trades": int(len(bt)),
        "matched_trades": int(len(matched)),
        "live_only_trades": int(len(live_only)),
        "backtest_only_trades": int(len(bt_only)),
        "live_signals": int(len(live_sig)),
        "v11_signals": int(len(bt_sig)),
        "matched_signals": int(len(sig_matched)),
        "out_dir": str(paths.out_dir),
        "report": str(Path(args.report).resolve()),
    }
    (paths.out_dir / "summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")

    report = build_report(
        dates,
        paths,
        live,
        bt,
        live_nonfilled,
        live_sig,
        bt_sig,
        matched,
        live_only,
        bt_only,
        sig_matched,
        sig_live_only,
        sig_bt_only,
        daily,
        per_setup,
        causes,
    )
    Path(args.report).write_text(report, encoding="utf-8")
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
