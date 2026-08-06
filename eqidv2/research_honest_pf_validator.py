"""Fail-closed robustness validator for completed intraday backtests.

This utility evaluates an existing ``trades.csv``.  It does not search,
optimise, change a strategy configuration, or write any live trading file.
Even when every statistical gate passes, production approval is deliberately
hard-coded to ``False``.

The qualification contract is intentionally demanding:

* at least 300 trades, 60 evaluated sessions, and 40 active trading days;
* net profit factor (PF) at least 1.60;
* each chronological session half has net PF at least 1.10 and positive P&L;
* the one-sided 95% day-cluster bootstrap lower PF bound is at least 1.20;
* PF remains at least 1.20 after removing the five best trades; and
* PF remains at least 1.20 with costs stressed by 25%.

Required trade columns are ``net_pnl_rs``, ``gross_pnl_rs``, ``cost_rs`` and
a date-like column (normally ``trade_date``).  The stressed result is computed
exactly as ``gross_pnl_rs - 1.25 * cost_rs``.
"""

from __future__ import annotations

import argparse
import json
import math
from pathlib import Path
from typing import Any, Iterable, Sequence

import numpy as np
import pandas as pd


SCHEMA_VERSION = "eqidv2_honest_pf_validation_v1"
PRODUCTION_APPROVED = False
PROMOTION_ACTION = "NONE_RESEARCH_ONLY"

MIN_TRADES = 300
MIN_SESSIONS = 60
MIN_ACTIVE_DAYS = 40
MIN_NET_PF = 1.60
MIN_HALF_NET_PF = 1.10
MIN_BOOTSTRAP_LOWER_PF = 1.20
MIN_TOP5_REMOVED_PF = 1.20
MIN_STRESSED_PF = 1.20

BOOTSTRAP_SEED = 20260805
BOOTSTRAP_DRAWS = 100_000
BOOTSTRAP_LOWER_QUANTILE = 0.05
BOOTSTRAP_CHUNK_SIZE = 2_048

DATE_COLUMN_CANDIDATES = (
    "trade_date",
    "date",
    "session_date",
    "session",
    "entry_time",
    "entry_ts",
    "timestamp",
    "datetime",
    "slot_ist",
)
INCLUSION_COLUMN_CANDIDATES = ("included", "eligible", "valid")


def profit_factor(values: Iterable[float]) -> float:
    """Return gross positive P&L divided by absolute gross negative P&L."""

    pnl = np.asarray(list(values), dtype=float)
    pnl = pnl[np.isfinite(pnl)]
    gains = float(pnl[pnl > 0.0].sum())
    losses = float(-pnl[pnl < 0.0].sum())
    if losses == 0.0:
        return math.inf if gains > 0.0 else 0.0
    return gains / losses


def _normalise_dates(values: Iterable[Any], label: str) -> pd.Series:
    parsed = pd.to_datetime(pd.Series(values), errors="coerce", utc=True)
    if parsed.isna().any():
        bad = int(parsed.isna().sum())
        raise ValueError(f"{label} contains {bad} unparseable date value(s)")
    # UTC parsing safely handles a mixture of naive and timezone-aware input.
    # For date-only/session values, normalising after removing timezone keeps
    # the stated calendar date.  Timestamp columns should normally use IST;
    # convert those before taking the date.
    raw = pd.Series(values).astype("string")
    has_clock = raw.str.contains(r"[T ]\d{1,2}:\d{2}", regex=True, na=False)
    if bool(has_clock.any()):
        parsed = parsed.dt.tz_convert("Asia/Kolkata")
    return parsed.dt.tz_localize(None).dt.normalize()


def _detect_date_column(frame: pd.DataFrame, requested: str | None, label: str) -> str:
    if requested:
        if requested not in frame.columns:
            raise ValueError(f"{label} has no requested date column {requested!r}")
        return requested
    for column in DATE_COLUMN_CANDIDATES:
        if column in frame.columns:
            return column
    raise ValueError(
        f"{label} has no recognised date column; tried {DATE_COLUMN_CANDIDATES}"
    )


def _boolean_inclusion_mask(values: pd.Series, column: str) -> pd.Series:
    """Parse an audit inclusion field strictly; ambiguous values are errors."""

    normalised = values.astype("string").str.strip().str.lower()
    mapping = {
        "true": True,
        "false": False,
        "1": True,
        "0": False,
        "1.0": True,
        "0.0": False,
        "yes": True,
        "no": False,
        "y": True,
        "n": False,
    }
    parsed = normalised.map(mapping)
    ambiguous = parsed.isna()
    if bool(ambiguous.any()):
        examples = values.loc[ambiguous].astype("string").drop_duplicates().head(5)
        rendered = ", ".join(repr(value) for value in examples.tolist())
        raise ValueError(
            f"session/audit inclusion column {column!r} contains ambiguous "
            f"value(s): {rendered}"
        )
    return parsed.astype(bool)


def prepare_trades(
    trades: pd.DataFrame,
    trade_date_column: str | None = None,
) -> pd.DataFrame:
    """Validate the strict input contract and attach ``_session_date``."""

    required = {"net_pnl_rs", "gross_pnl_rs", "cost_rs"}
    missing = sorted(required - set(trades.columns))
    if missing:
        raise ValueError(f"trades CSV missing required column(s): {missing}")
    date_column = _detect_date_column(trades, trade_date_column, "trades CSV")
    out = trades.copy()
    out["_session_date"] = _normalise_dates(out[date_column], date_column)
    for column in sorted(required):
        out[column] = pd.to_numeric(out[column], errors="coerce")
        if out[column].isna().any() or not np.isfinite(out[column]).all():
            raise ValueError(f"trades CSV column {column!r} contains non-finite values")
    if (out["cost_rs"] < 0.0).any():
        raise ValueError("trades CSV cost_rs must be non-negative")
    return out


def filter_trades_to_window(
    trades: pd.DataFrame,
    start_date: str | None,
    end_date: str | None,
) -> pd.DataFrame:
    """Inclusively restrict prepared trades to an explicit evaluation window."""

    if bool(start_date) != bool(end_date):
        raise ValueError("--start-date and --end-date must be supplied together")
    if not start_date:
        return trades.copy()
    start = pd.Timestamp(start_date).tz_localize(None).normalize()
    end = pd.Timestamp(end_date).tz_localize(None).normalize()
    if end < start:
        raise ValueError("--end-date precedes --start-date")
    dates = trades["_session_date"]
    return trades.loc[dates.between(start, end, inclusive="both")].copy()


def _read_session_frame(path: Path) -> pd.DataFrame:
    suffix = path.suffix.lower()
    if suffix == ".csv":
        return pd.read_csv(path)
    if suffix == ".json":
        payload = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(payload, list):
            return pd.DataFrame(payload)
        if isinstance(payload, dict):
            for key in ("sessions", "trading_sessions", "dates", "days", "daily"):
                value = payload.get(key)
                if isinstance(value, list):
                    if value and not isinstance(value[0], dict):
                        return pd.DataFrame({"date": value})
                    return pd.DataFrame(value)
        raise ValueError(
            f"{path} does not contain explicit session dates; use its slot/daily audit CSV"
        )
    raise ValueError(f"unsupported session file type {suffix!r}; use CSV or JSON")


def load_sessions(
    *,
    trades: pd.DataFrame,
    sessions_path: Path | None = None,
    session_date_column: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
) -> tuple[list[pd.Timestamp], str, list[str]]:
    """Load the evaluation calendar and disclose any calendar approximation."""

    warnings: list[str] = []
    if bool(start_date) != bool(end_date):
        raise ValueError("--start-date and --end-date must be supplied together")

    if sessions_path is not None:
        frame = _read_session_frame(sessions_path)
        lower_columns = {str(column).strip().lower(): column for column in frame.columns}
        inclusion_columns = [
            lower_columns[name]
            for name in INCLUSION_COLUMN_CANDIDATES
            if name in lower_columns
        ]
        if inclusion_columns:
            masks = [
                _boolean_inclusion_mask(frame[column], str(column))
                for column in inclusion_columns
            ]
            keep = masks[0].copy()
            for mask in masks[1:]:
                keep &= mask
            frame = frame.loc[keep].copy()
        column = _detect_date_column(frame, session_date_column, "session/audit file")
        values = _normalise_dates(frame[column], column)
        source = f"explicit_session_file:{sessions_path}"
        if inclusion_columns:
            source += ";inclusion_columns=" + ",".join(map(str, inclusion_columns))
    elif start_date and end_date:
        start = pd.Timestamp(start_date).normalize()
        end = pd.Timestamp(end_date).normalize()
        if end < start:
            raise ValueError("--end-date precedes --start-date")
        values = pd.Series(pd.bdate_range(start, end))
        source = "weekday_calendar_from_start_end"
        warnings.append(
            "Sessions were generated as Monday-Friday dates; exchange holidays were not verified."
        )
    else:
        values = trades["_session_date"].copy()
        source = "trade_dates_only_conservative"
        warnings.append(
            "No session audit/range was supplied; session count includes active trade dates only."
        )

    sessions = sorted(set(pd.Series(values).dropna().tolist()))
    if start_date and end_date and sessions_path is not None:
        start = pd.Timestamp(start_date).normalize()
        end = pd.Timestamp(end_date).normalize()
        sessions = [day for day in sessions if start <= day <= end]
    if not sessions:
        raise ValueError("evaluation session calendar is empty")

    trade_days = set(trades["_session_date"].tolist())
    outside = sorted(trade_days - set(sessions))
    if outside:
        preview = ", ".join(str(value.date()) for value in outside[:5])
        raise ValueError(f"trade dates fall outside the evaluation sessions: {preview}")
    return sessions, source, warnings


def split_sessions(
    sessions: Sequence[pd.Timestamp],
) -> tuple[list[pd.Timestamp], list[pd.Timestamp]]:
    ordered = sorted(set(sessions))
    midpoint = len(ordered) // 2
    return ordered[:midpoint], ordered[midpoint:]


def _pnl_metrics(pnl: np.ndarray) -> dict[str, Any]:
    values = np.asarray(pnl, dtype=float)
    return {
        "trades": int(len(values)),
        "net_pnl_rs": float(values.sum()),
        "profit_factor": float(profit_factor(values)),
        "wins": int((values > 0.0).sum()),
        "losses": int((values < 0.0).sum()),
        "flat": int((values == 0.0).sum()),
    }


def day_cluster_bootstrap_pf(
    trades: pd.DataFrame,
    sessions: Sequence[pd.Timestamp],
    *,
    draws: int = BOOTSTRAP_DRAWS,
    seed: int = BOOTSTRAP_SEED,
) -> dict[str, Any]:
    """Bootstrap whole sessions and return the PF sampling distribution.

    Positive and negative trade P&L are aggregated separately per session, so
    sampling a day keeps every trade from that day together.  Zero-trade
    sessions stay in the calendar and are sampled as zero clusters.
    """

    if draws <= 0:
        raise ValueError("bootstrap draws must be positive")
    calendar = pd.DatetimeIndex(sorted(set(sessions)))
    grouped = trades.groupby("_session_date")["net_pnl_rs"]
    positive = grouped.apply(lambda values: float(values[values > 0.0].sum()))
    negative = grouped.apply(lambda values: float(-values[values < 0.0].sum()))
    day_gains = positive.reindex(calendar, fill_value=0.0).to_numpy(dtype=float)
    day_losses = negative.reindex(calendar, fill_value=0.0).to_numpy(dtype=float)

    rng = np.random.default_rng(seed)
    sampled_pf = np.empty(draws, dtype=float)
    offset = 0
    while offset < draws:
        size = min(BOOTSTRAP_CHUNK_SIZE, draws - offset)
        indexes = rng.integers(0, len(calendar), size=(size, len(calendar)))
        gains = day_gains[indexes].sum(axis=1)
        losses = day_losses[indexes].sum(axis=1)
        ratios = np.divide(
            gains,
            losses,
            out=np.full(size, np.inf, dtype=float),
            where=losses > 0.0,
        )
        ratios[(losses == 0.0) & (gains == 0.0)] = 0.0
        sampled_pf[offset : offset + size] = ratios
        offset += size

    return {
        "method": "nonparametric_session_cluster_resampling",
        "seed": int(seed),
        "draws": int(draws),
        "one_sided_confidence": 0.95,
        "lower_profit_factor": float(
            np.quantile(sampled_pf, BOOTSTRAP_LOWER_QUANTILE)
        ),
        "median_profit_factor": float(np.quantile(sampled_pf, 0.50)),
        "upper_profit_factor": float(np.quantile(sampled_pf, 0.95)),
    }


def evaluate(
    trades: pd.DataFrame,
    sessions: Sequence[pd.Timestamp],
    *,
    session_source: str,
    warnings: Sequence[str] = (),
    bootstrap_draws: int = BOOTSTRAP_DRAWS,
) -> dict[str, Any]:
    """Apply the immutable qualification gates to validated trade data."""

    sessions = sorted(set(pd.Timestamp(day).normalize() for day in sessions))
    pnl = trades["net_pnl_rs"].to_numpy(dtype=float)
    overall = _pnl_metrics(pnl)
    active_days = int(trades["_session_date"].nunique())

    first_sessions, second_sessions = split_sessions(sessions)

    def half_result(label: str, half_sessions: Sequence[pd.Timestamp]) -> dict[str, Any]:
        half = trades.loc[trades["_session_date"].isin(half_sessions)]
        result = _pnl_metrics(half["net_pnl_rs"].to_numpy(dtype=float))
        result.update(
            {
                "label": label,
                "sessions": int(len(half_sessions)),
                "start_date": str(half_sessions[0].date()) if half_sessions else None,
                "end_date": str(half_sessions[-1].date()) if half_sessions else None,
            }
        )
        return result

    first = half_result("first_chronological_half", first_sessions)
    second = half_result("second_chronological_half", second_sessions)

    bootstrap = day_cluster_bootstrap_pf(
        trades, sessions, draws=bootstrap_draws, seed=BOOTSTRAP_SEED
    )

    profitable = trades.index[trades["net_pnl_rs"] > 0.0]
    top_indexes = (
        trades.loc[profitable, "net_pnl_rs"].nlargest(min(5, len(profitable))).index
    )
    without_top5 = trades.drop(index=top_indexes)
    top5_metrics = _pnl_metrics(without_top5["net_pnl_rs"].to_numpy(dtype=float))
    top5_metrics["removed_trades"] = int(len(top_indexes))
    top5_metrics["removed_net_pnl_rs"] = float(
        trades.loc[top_indexes, "net_pnl_rs"].sum()
    )

    stressed_pnl = (
        trades["gross_pnl_rs"].to_numpy(dtype=float)
        - 1.25 * trades["cost_rs"].to_numpy(dtype=float)
    )
    stressed = _pnl_metrics(stressed_pnl)
    stressed["formula"] = "gross_pnl_rs - 1.25 * cost_rs"

    checks = {
        "at_least_300_trades": overall["trades"] >= MIN_TRADES,
        "at_least_60_sessions": len(sessions) >= MIN_SESSIONS,
        "at_least_40_active_days": active_days >= MIN_ACTIVE_DAYS,
        "net_pf_at_least_1p60": overall["profit_factor"] >= MIN_NET_PF,
        "first_half_net_pf_at_least_1p10": first["profit_factor"]
        >= MIN_HALF_NET_PF,
        "first_half_net_pnl_positive": first["net_pnl_rs"] > 0.0,
        "second_half_net_pf_at_least_1p10": second["profit_factor"]
        >= MIN_HALF_NET_PF,
        "second_half_net_pnl_positive": second["net_pnl_rs"] > 0.0,
        "bootstrap_95pct_lower_pf_at_least_1p20": bootstrap[
            "lower_profit_factor"
        ]
        >= MIN_BOOTSTRAP_LOWER_PF,
        "top5_removed_pf_at_least_1p20": top5_metrics["profit_factor"]
        >= MIN_TOP5_REMOVED_PF,
        "cost_plus_25pct_stressed_pf_at_least_1p20": stressed["profit_factor"]
        >= MIN_STRESSED_PF,
    }
    qualification_pass = bool(all(checks.values()))

    return {
        "schema_version": SCHEMA_VERSION,
        "qualification_pass": qualification_pass,
        "production_approved": PRODUCTION_APPROVED,
        "promotion_action": PROMOTION_ACTION,
        "decision": "RESEARCH_QUALIFIED" if qualification_pass else "REJECT",
        "thresholds": {
            "minimum_trades": MIN_TRADES,
            "minimum_sessions": MIN_SESSIONS,
            "minimum_active_days": MIN_ACTIVE_DAYS,
            "minimum_net_profit_factor": MIN_NET_PF,
            "minimum_each_half_net_profit_factor": MIN_HALF_NET_PF,
            "each_half_net_pnl_must_be_positive": True,
            "minimum_bootstrap_95pct_lower_profit_factor": MIN_BOOTSTRAP_LOWER_PF,
            "minimum_top5_removed_profit_factor": MIN_TOP5_REMOVED_PF,
            "minimum_cost_plus_25pct_stressed_profit_factor": MIN_STRESSED_PF,
        },
        "sample": {
            "sessions": int(len(sessions)),
            "active_days": active_days,
            "start_date": str(sessions[0].date()),
            "end_date": str(sessions[-1].date()),
            "session_source": session_source,
        },
        "overall": overall,
        "first_chronological_half": first,
        "second_chronological_half": second,
        "day_cluster_bootstrap": bootstrap,
        "top5_profitable_trades_removed": top5_metrics,
        "cost_plus_25pct_stress": stressed,
        "checks": checks,
        "failed_checks": [name for name, passed in checks.items() if not passed],
        "warnings": list(warnings),
    }


def _display_number(value: Any) -> str:
    if isinstance(value, (float, np.floating)):
        if math.isinf(float(value)):
            return "Infinity" if value > 0 else "-Infinity"
        if math.isnan(float(value)):
            return "null"
        return f"{float(value):.6f}"
    return str(value)


def render_markdown(result: dict[str, Any]) -> str:
    """Create a compact human-readable companion to the machine report."""

    overall = result["overall"]
    sample = result["sample"]
    first = result["first_chronological_half"]
    second = result["second_chronological_half"]
    bootstrap = result["day_cluster_bootstrap"]
    top5 = result["top5_profitable_trades_removed"]
    stressed = result["cost_plus_25pct_stress"]
    decision = result["decision"]
    lines = [
        "# Honest PF validation",
        "",
        f"**Decision:** {decision}",
        "",
        "Production approval is always **false**; this report is research-only.",
        "",
        "| Measure | Result | Required |",
        "|---|---:|---:|",
        f"| Trades | {overall['trades']} | >= {MIN_TRADES} |",
        f"| Sessions | {sample['sessions']} | >= {MIN_SESSIONS} |",
        f"| Active days | {sample['active_days']} | >= {MIN_ACTIVE_DAYS} |",
        f"| Net PF | {_display_number(overall['profit_factor'])} | >= {MIN_NET_PF:.2f} |",
        f"| First-half PF / net P&L | {_display_number(first['profit_factor'])} / Rs {_display_number(first['net_pnl_rs'])} | >= {MIN_HALF_NET_PF:.2f} / > 0 |",
        f"| Second-half PF / net P&L | {_display_number(second['profit_factor'])} / Rs {_display_number(second['net_pnl_rs'])} | >= {MIN_HALF_NET_PF:.2f} / > 0 |",
        f"| Day-bootstrap 95% lower PF | {_display_number(bootstrap['lower_profit_factor'])} | >= {MIN_BOOTSTRAP_LOWER_PF:.2f} |",
        f"| PF after best 5 removed | {_display_number(top5['profit_factor'])} | >= {MIN_TOP5_REMOVED_PF:.2f} |",
        f"| PF with costs +25% | {_display_number(stressed['profit_factor'])} | >= {MIN_STRESSED_PF:.2f} |",
        "",
        f"Failed checks: {', '.join(result['failed_checks']) or 'none'}",
        "",
        f"Session source: `{sample['session_source']}`",
    ]
    if result["warnings"]:
        lines.extend(["", "Warnings:"])
        lines.extend(f"- {warning}" for warning in result["warnings"])
    return "\n".join(lines) + "\n"


def _json_safe(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_safe(item) for item in value]
    if isinstance(value, (np.bool_, bool)):
        return bool(value)
    if isinstance(value, (np.integer, int)):
        return int(value)
    if isinstance(value, (np.floating, float)):
        number = float(value)
        if math.isinf(number):
            return "Infinity" if number > 0 else "-Infinity"
        if math.isnan(number):
            return None
        return number
    return value


def write_reports(
    result: dict[str, Any], json_path: Path, markdown_path: Path
) -> None:
    json_path.parent.mkdir(parents=True, exist_ok=True)
    markdown_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.write_text(
        json.dumps(_json_safe(result), indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    markdown_path.write_text(render_markdown(result), encoding="utf-8")


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--trades", type=Path, required=True)
    parser.add_argument(
        "--sessions-csv",
        "--session-audit",
        "--daily-summary",
        dest="sessions_path",
        type=Path,
        help="CSV/JSON containing explicit evaluated session dates",
    )
    parser.add_argument("--trade-date-column")
    parser.add_argument("--session-date-column")
    parser.add_argument("--start-date")
    parser.add_argument("--end-date")
    parser.add_argument("--out-json", type=Path)
    parser.add_argument("--out-markdown", "--out-md", dest="out_markdown", type=Path)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    trades = prepare_trades(pd.read_csv(args.trades), args.trade_date_column)
    # Filter outcomes before loading/filtering the calendar.  This lets one
    # combined trades file be evaluated over a preregistered subwindow while
    # retaining the fail-closed check that every retained trade belongs to an
    # explicit evaluation session.
    trades = filter_trades_to_window(trades, args.start_date, args.end_date)
    sessions, source, warnings = load_sessions(
        trades=trades,
        sessions_path=args.sessions_path,
        session_date_column=args.session_date_column,
        start_date=args.start_date,
        end_date=args.end_date,
    )
    result = evaluate(trades, sessions, session_source=source, warnings=warnings)
    json_path = args.out_json or args.trades.with_name(
        f"{args.trades.stem}_honest_pf_validation.json"
    )
    markdown_path = args.out_markdown or args.trades.with_name(
        f"{args.trades.stem}_honest_pf_validation.md"
    )
    write_reports(result, json_path, markdown_path)
    print(render_markdown(result), end="")
    print(f"JSON: {json_path}")
    print(f"Markdown: {markdown_path}")
    return 0 if result["qualification_pass"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
