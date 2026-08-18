from __future__ import annotations

import argparse
import sys
from datetime import date, datetime, time as dtime
from typing import Any

import pandas as pd

import fno_oi_common as common
import fno_oi_fetch_5min as fetcher


SESSION = "fno_oi_eod_qc"


def inspect_contract(contract: pd.Series, session_date: date) -> dict[str, Any]:
    symbol = str(contract["tradingsymbol"])
    path = common.raw_contract_path(symbol)
    expected = common.expected_slot_ends(session_date)
    result: dict[str, Any] = {
        "underlying": str(contract["underlying"]),
        "tradingsymbol": symbol,
        "instrument_token": int(contract["instrument_token"]),
        "expiry": pd.Timestamp(contract["expiry"]).strftime("%Y-%m-%d"),
        "days_to_expiry": int((pd.Timestamp(contract["expiry"]).date() - session_date).days),
        "is_index_future": bool(contract.get("is_index_future", False)),
        "expected_candles": int(len(expected)),
        "observed_candles": 0,
        "valid_candles": 0,
        "missing_candles": int(len(expected)),
        "duplicate_candles": 0,
        "invalid_ohlc": 0,
        "missing_or_negative_oi": 0,
        "coverage_ratio": 0.0,
        "first_timestamp": "",
        "last_timestamp": "",
        "state": "MISSING_FILE",
        "path": str(path),
    }
    if not path.exists():
        return result
    try:
        frame = pd.read_parquet(path)
    except Exception as exc:
        result["state"] = "UNREADABLE"
        result["error"] = f"{type(exc).__name__}: {exc}"
        return result
    if frame.empty or "timestamp" not in frame.columns:
        result["state"] = "EMPTY"
        return result

    timestamps = pd.to_datetime(frame["timestamp"], errors="coerce")
    if getattr(timestamps.dt, "tz", None) is None:
        timestamps = timestamps.dt.tz_localize(common.IST)
    else:
        timestamps = timestamps.dt.tz_convert(common.IST)
    frame = frame.assign(timestamp=timestamps).dropna(subset=["timestamp"])
    frame = frame.loc[frame["timestamp"].dt.date.eq(session_date)].copy()
    if frame.empty:
        result["state"] = "NO_SESSION_ROWS"
        return result

    result["duplicate_candles"] = int(frame["timestamp"].duplicated().sum())
    frame = frame.drop_duplicates("timestamp", keep="last").sort_values("timestamp")
    observed = pd.DatetimeIndex(frame["timestamp"])
    expected_set = set(expected)
    observed_expected = observed[observed.isin(expected)]
    missing = expected.difference(observed_expected)
    numeric = frame[["open", "high", "low", "close", "volume", "oi"]].apply(
        pd.to_numeric, errors="coerce"
    )
    valid_ohlc = (
        numeric[["open", "high", "low", "close"]].notna().all(axis=1)
        & numeric["high"].ge(numeric[["open", "close"]].max(axis=1))
        & numeric["low"].le(numeric[["open", "close"]].min(axis=1))
        & numeric["high"].ge(numeric["low"])
        & numeric["close"].gt(0)
    )
    valid_oi = numeric["oi"].notna() & numeric["oi"].ge(0)
    valid_volume = numeric["volume"].notna() & numeric["volume"].ge(0)
    valid = valid_ohlc & valid_oi & valid_volume & frame["timestamp"].isin(expected_set)
    result.update(
        {
            "observed_candles": int(len(observed_expected)),
            "valid_candles": int(valid.sum()),
            "missing_candles": int(len(missing)),
            "invalid_ohlc": int((~valid_ohlc).sum()),
            "missing_or_negative_oi": int((~valid_oi).sum()),
            "coverage_ratio": float(valid.sum() / len(expected)) if len(expected) else 0.0,
            "first_timestamp": frame["timestamp"].min().isoformat(),
            "last_timestamp": frame["timestamp"].max().isoformat(),
        }
    )
    result["state"] = (
        "COMPLETE"
        if result["missing_candles"] == 0
        and result["invalid_ohlc"] == 0
        and result["missing_or_negative_oi"] == 0
        and result["duplicate_candles"] == 0
        else "PARTIAL"
    )
    return result


def inspect_universe(universe: pd.DataFrame, session_date: date) -> pd.DataFrame:
    rows = [inspect_contract(contract, session_date) for _, contract in universe.iterrows()]
    return pd.DataFrame(rows).sort_values(["state", "coverage_ratio", "tradingsymbol"]).reset_index(drop=True)


def _repair_universe(
    universe: pd.DataFrame,
    qc: pd.DataFrame,
    args: argparse.Namespace,
) -> list[dict[str, Any]]:
    needs_repair = set(qc.loc[qc["state"].ne("COMPLETE"), "tradingsymbol"].astype(str))
    repair_universe = universe.loc[universe["tradingsymbol"].astype(str).isin(needs_repair)].copy()
    if repair_universe.empty:
        return []
    runtimes = fetcher._build_app_runtimes(args)
    session_date = date.fromisoformat(args.session_date) if args.session_date else common.now_ist().date()
    start = datetime.combine(session_date, dtime(9, 15), tzinfo=common.IST)
    end = datetime.combine(session_date, dtime(15, 30), tzinfo=common.IST)
    print(f"[REPAIR] Refetching {len(repair_universe)} incomplete contracts.", flush=True)
    return fetcher.fetch_contracts(
        repair_universe,
        runtimes,
        start,
        end,
        slot_end=None,
        max_retries=args.max_retries,
        phase="EOD_REPAIR",
    )


def render_report(
    session_date: date,
    qc: pd.DataFrame,
    repair_outcomes: list[dict[str, Any]],
) -> str:
    complete = int(qc["state"].eq("COMPLETE").sum())
    expected = int(len(qc))
    mean_coverage = float(qc["coverage_ratio"].mean()) if expected else 0.0
    total_missing = int(qc["missing_candles"].sum()) if expected else 0
    lines = [
        "# FnO OI End-of-Day Quality",
        "",
        f"Session date: {session_date.isoformat()}",
        f"Published: {common.now_ist().isoformat(timespec='seconds')}",
        f"Contracts complete: {complete}/{expected}",
        f"Mean valid-candle coverage: {mean_coverage:.1%}",
        f"Missing contract-candles: {total_missing}",
        f"Repair requests completed: {len(repair_outcomes)}",
        "No missing OI values are synthesized.",
        "",
        "Lowest coverage contracts:",
        "",
        "Contract | Expiry | Valid/Expected | Coverage | Missing | OI errors | State",
        "--- | --- | ---: | ---: | ---: | ---: | ---",
    ]
    for row in qc.nsmallest(min(30, len(qc)), "coverage_ratio").itertuples(index=False):
        lines.append(
            f"{row.tradingsymbol} | {row.expiry} | {row.valid_candles}/{row.expected_candles} | "
            f"{row.coverage_ratio:.1%} | {row.missing_candles} | "
            f"{row.missing_or_negative_oi} | {row.state}"
        )
    expiring = qc.loc[qc["days_to_expiry"].le(2)].sort_values("days_to_expiry")
    lines.extend(["", "Contracts near expiry:", "", "Contract | Expiry | Days | Coverage", "--- | --- | ---: | ---:"])
    for row in expiring.itertuples(index=False):
        lines.append(
            f"{row.tradingsymbol} | {row.expiry} | {row.days_to_expiry} | {row.coverage_ratio:.1%}"
        )
    if expiring.empty:
        lines.append("None | - | - | -")
    return "\n".join(lines) + "\n"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Repair and audit the permanent FnO 5-minute OHLCV+OI contract archive."
    )
    parser.add_argument("--session-date", default="")
    parser.add_argument("--no-repair", action="store_true")
    parser.add_argument("--min-contract-coverage", type=float, default=0.80)
    parser.add_argument("--request-interval-sec", type=float, default=0.36)
    parser.add_argument("--timeout-sec", type=float, default=8.0)
    parser.add_argument("--max-retries", type=int, default=3)
    parser.add_argument("--max-apps", type=int, default=8)
    parser.add_argument("--allow-non-trading-day", action="store_true")
    return parser


def run(args: argparse.Namespace) -> int:
    session_date = (
        date.fromisoformat(args.session_date) if args.session_date else common.now_ist().date()
    )
    args.session_date = session_date.isoformat()
    if (
        not args.allow_non_trading_day
        and not common.is_trading_day(session_date, common.load_holidays())
    ):
        common.publish_status(
            SESSION,
            "SKIPPED_NON_TRADING_DAY",
            session_date_ist=session_date.isoformat(),
        )
        return 0
    universe = common.load_near_month_universe(expected_date=session_date)
    common.publish_status(
        SESSION,
        "RUNNING",
        phase="INITIAL_QC",
        session_date_ist=session_date.isoformat(),
        contracts_expected=len(universe),
    )
    qc = inspect_universe(universe, session_date)
    repair_outcomes: list[dict[str, Any]] = []
    if not args.no_repair and qc["state"].ne("COMPLETE").any():
        common.publish_status(
            SESSION,
            "RUNNING",
            phase="REPAIR",
            session_date_ist=session_date.isoformat(),
            incomplete_contracts=int(qc["state"].ne("COMPLETE").sum()),
        )
        repair_outcomes = _repair_universe(universe, qc, args)
        qc = inspect_universe(universe, session_date)

    dated_csv = common.QC_DIR / f"fno_oi_eod_qc_{session_date.isoformat()}.csv"
    dated_parquet = common.QC_DIR / f"fno_oi_eod_qc_{session_date.isoformat()}.parquet"
    latest_csv = common.LATEST_DIR / "latest_fno_oi_eod_qc.csv"
    latest_report = common.LATEST_DIR / "latest_fno_oi_eod_qc.md"
    common.atomic_write_csv(qc, dated_csv)
    common.atomic_write_parquet(qc, dated_parquet)
    common.atomic_write_csv(qc, latest_csv)
    common.atomic_write_text(latest_report, render_report(session_date, qc, repair_outcomes))

    complete = int(qc["state"].eq("COMPLETE").sum())
    contracts_at_threshold = int(
        qc["coverage_ratio"].ge(float(args.min_contract_coverage)).sum()
    )
    repair_failed = sum(item["state"] == "FAILED" for item in repair_outcomes)
    overall_ok = repair_failed == 0 and contracts_at_threshold == len(qc)
    state = "SUCCESS" if overall_ok else "PARTIAL"
    common.publish_status(
        SESSION,
        state,
        phase="DONE",
        session_date_ist=session_date.isoformat(),
        contracts_expected=len(qc),
        contracts_complete=complete,
        contracts_at_coverage_threshold=contracts_at_threshold,
        repair_failed=repair_failed,
        output=latest_report,
    )
    print(
        f"[EOD][{state}] complete={complete}/{len(qc)} "
        f"coverage_ok={contracts_at_threshold}/{len(qc)} repair_failed={repair_failed}",
        flush=True,
    )
    return 0


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        return run(args)
    except KeyboardInterrupt:
        common.publish_status(SESSION, "STOPPED", heartbeat_state="STOPPED", phase="INTERRUPTED")
        return 0
    except Exception as exc:
        common.publish_status(
            SESSION,
            "FAILED",
            heartbeat_state="CRASHED",
            phase="FAILED",
            error=f"{type(exc).__name__}: {exc}",
        )
        print(f"[FATAL] {type(exc).__name__}: {exc}", file=sys.stderr, flush=True)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
