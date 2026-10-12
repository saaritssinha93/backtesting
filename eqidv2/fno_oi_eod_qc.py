from __future__ import annotations

import argparse
import math
import sys
from datetime import date, datetime, time as dtime
from typing import Any

import pandas as pd
import numpy as np

import fno_oi_common as common


SESSION = "fno_oi_eod_qc"
NUMERIC_COLUMNS = ("open", "high", "low", "close", "volume", "oi")
QUALITY_FLAGS = (
    "gap_filled", "is_gap_filled", "opening_snapshot", "provisional_stale",
    "is_synthetic", "synthetic", "forward_filled", "is_filled",
    "interpolated", "is_interpolated",
    "synthetic_repair", "repaired_synthetic",
)


def _timestamps(values: pd.Series) -> pd.Series:
    parsed = pd.to_datetime(values, errors="coerce", format="mixed")
    return parsed.dt.tz_localize(common.IST) if parsed.dt.tz is None else parsed.dt.tz_convert(common.IST)


def _bad_flag(values: pd.Series) -> pd.Series:
    """Optional legacy null flags mean undeclared; unknown declarations fail."""
    return values.notna() & ~values.astype(str).str.strip().str.lower().isin({"0", "0.0", "false", "no", "off", ""})


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
        "invalid_timestamps": 0,
        "unexpected_timestamps": 0,
        "invalid_ohlc": 0,
        "missing_or_negative_oi": 0,
        "invalid_volume": 0,
        "invalid_quality": 0,
        "invalid_candle_start": 0,
        "invalid_identity": 0,
        "missing_valid_candles": int(len(expected)),
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
    if frame.empty:
        result["state"] = "EMPTY"
        return result
    missing_columns = sorted(({"timestamp"} | set(NUMERIC_COLUMNS)) - set(frame.columns))
    if missing_columns:
        result.update(state="INVALID_SCHEMA", missing_columns=missing_columns)
        return result
    try:
        timestamps = _timestamps(frame["timestamp"])
    except (ValueError, TypeError, AttributeError, OverflowError):
        result.update(state="INVALID_TIMESTAMP_SCHEMA", invalid_timestamps=len(frame))
        return result
    # Unparseable dates cannot safely be assigned to another session. Surface
    # archive corruption rather than silently discarding potentially missing bars.
    result["invalid_timestamps"] = int(timestamps.isna().sum())
    frame = frame.assign(timestamp=timestamps)
    frame = frame.loc[frame["timestamp"].dt.date.eq(session_date)].copy()
    if frame.empty:
        result["state"] = "INVALID_TIMESTAMPS" if result["invalid_timestamps"] else "NO_SESSION_ROWS"
        return result

    result["duplicate_candles"] = int(frame["timestamp"].duplicated().sum())
    frame = frame.sort_values("timestamp").reset_index(drop=True)
    duplicated = frame["timestamp"].duplicated(keep=False)
    observed = pd.DatetimeIndex(frame["timestamp"].unique())
    expected_set = set(expected)
    observed_expected = observed[observed.isin(expected)]
    missing = expected.difference(observed_expected)
    numeric = frame[list(NUMERIC_COLUMNS)].apply(
        pd.to_numeric, errors="coerce"
    )
    finite = pd.DataFrame(np.isfinite(numeric.to_numpy(dtype=float)), index=numeric.index, columns=numeric.columns)
    valid_ohlc = (
        finite[["open", "high", "low", "close"]].all(axis=1)
        & numeric[["open", "high", "low", "close"]].gt(0).all(axis=1)
        & numeric["high"].ge(numeric[["open", "close"]].max(axis=1))
        & numeric["low"].le(numeric[["open", "close"]].min(axis=1))
        & numeric["high"].ge(numeric["low"])
    )
    valid_oi = finite["oi"] & numeric["oi"].ge(0)
    valid_volume = finite["volume"] & numeric["volume"].ge(0)
    valid_quality = pd.Series(True, index=frame.index)
    if "quality_state" in frame:
        valid_quality &= frame["quality_state"].eq("VALID")
    for flag in QUALITY_FLAGS:
        if flag in frame:
            valid_quality &= ~_bad_flag(frame[flag])
    if "source" in frame:
        valid_quality &= ~frame["source"].fillna("").astype(str).str.upper().str.contains(
            "SYNTHETIC|GAP.?FILL|FORWARD.?FILL|CARRY.?FORWARD|INTERPOLAT|PROXY", regex=True
        )
    valid_start = pd.Series(True, index=frame.index)
    if "candle_start" in frame:
        try:
            valid_start &= frame["timestamp"].sub(_timestamps(frame["candle_start"])).eq(pd.Timedelta(minutes=5))
        except (ValueError, TypeError, AttributeError, OverflowError):
            valid_start[:] = False
    valid_identity = pd.Series(True, index=frame.index)
    for column in ("tradingsymbol", "underlying"):
        if column in frame:
            valid_identity &= frame[column].eq(str(contract[column]))
    if "instrument_token" in frame:
        valid_identity &= pd.to_numeric(frame["instrument_token"], errors="coerce").eq(int(contract["instrument_token"]))
    if "expiry" in frame:
        valid_identity &= pd.to_datetime(frame["expiry"], errors="coerce").dt.date.eq(pd.Timestamp(contract["expiry"]).date())
    on_clock = frame["timestamp"].isin(expected_set)
    valid = valid_ohlc & valid_oi & valid_volume & valid_quality & valid_start & valid_identity & on_clock & ~duplicated
    valid_timestamps = pd.DatetimeIndex(frame.loc[valid, "timestamp"])
    result.update(
        {
            "observed_candles": int(len(observed_expected)),
            "valid_candles": int(valid.sum()),
            "missing_candles": int(len(missing)),
            "missing_valid_candles": int(len(expected.difference(valid_timestamps))),
            "unexpected_timestamps": int((~on_clock).sum()),
            "invalid_ohlc": int((~valid_ohlc).sum()),
            "missing_or_negative_oi": int((~valid_oi).sum()),
            "invalid_volume": int((~valid_volume).sum()),
            "invalid_quality": int((~valid_quality).sum()),
            "invalid_candle_start": int((~valid_start).sum()),
            "invalid_identity": int((~valid_identity).sum()),
            "coverage_ratio": float(valid.sum() / len(expected)) if len(expected) else 0.0,
            "first_timestamp": frame["timestamp"].min().isoformat(),
            "last_timestamp": frame["timestamp"].max().isoformat(),
        }
    )
    result["state"] = (
        "COMPLETE"
        if result["valid_candles"] == result["expected_candles"] == 75
        and all(result[key] == 0 for key in ("missing_candles", "invalid_timestamps", "unexpected_timestamps",
            "invalid_ohlc", "missing_or_negative_oi", "invalid_volume", "invalid_quality", "invalid_candle_start",
            "invalid_identity", "duplicate_candles"))
        else "PARTIAL"
    )
    return result


def inspect_universe(universe: pd.DataFrame, session_date: date) -> pd.DataFrame:
    if universe.empty:
        raise ValueError("Cannot certify an empty FnO universe")
    rows = [inspect_contract(contract, session_date) for _, contract in universe.iterrows()]
    return pd.DataFrame(rows).sort_values(["state", "coverage_ratio", "tradingsymbol"]).reset_index(drop=True)


def load_session_universe(session_date: date) -> pd.DataFrame:
    """Use immutable date membership; today's latest is not a historical index."""
    path = common.UNIVERSE_DIR / f"near_month_{session_date.isoformat()}.parquet"
    if not path.is_file():
        raise FileNotFoundError(f"Dated near-month universe is unavailable: {path}")
    frame = pd.read_parquet(path)
    required = {"underlying", "tradingsymbol", "instrument_token", "expiry", "master_date"}
    if frame.empty or not required.issubset(frame.columns):
        raise ValueError("Dated near-month universe is empty or missing required fields")
    master_dates = pd.to_datetime(frame["master_date"], errors="coerce")
    expiries = pd.to_datetime(frame["expiry"], errors="coerce")
    tokens = pd.to_numeric(frame["instrument_token"], errors="coerce")
    if (not master_dates.notna().all() or not master_dates.dt.date.eq(session_date).all()
            or not expiries.notna().all() or not expiries.dt.date.ge(session_date).all()
            or not tokens.map(lambda value: pd.notna(value) and math.isfinite(value) and value > 0 and float(value).is_integer()).all()
            or any(frame[column].isna().any() or frame[column].astype(str).str.strip().eq("").any()
                   or frame[column].duplicated().any() for column in ("underlying", "tradingsymbol"))
            or tokens.duplicated().any()):
        raise ValueError("Dated near-month universe has invalid dates, expiry, identity or duplicate contracts")
    frame = frame.copy()
    frame["expiry"], frame["master_date"] = expiries, master_dates
    return frame.sort_values("underlying").reset_index(drop=True)


def _publish(session_date: date, state: str, **metadata: Any) -> None:
    """Historical audit progress is scoped and never replaces live health."""
    metadata = {"session_date_ist": session_date.isoformat(), **metadata}
    if session_date == common.now_ist().date():
        common.publish_status(SESSION, state, **metadata)
    else:
        common.atomic_write_json(
            common.QC_DIR / f"fno_oi_eod_qc_{session_date.isoformat()}_status.json",
            {"session": SESSION, "status": state, "scope": "DATED_AUDIT",
             "ts": common.now_ist().isoformat(timespec="seconds"), **metadata},
        )


def _repair_universe(
    universe: pd.DataFrame,
    qc: pd.DataFrame,
    args: argparse.Namespace,
) -> list[dict[str, Any]]:
    # A --no-repair audit does not import the broker fetcher or discover apps.
    import fno_oi_fetch_5min as fetcher
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
        "Success requires every contract to contain all 75 exact, unique, valid five-minute bars (09:20-15:30 IST).",
        "Coverage thresholds are diagnostic only; incomplete or invalid archives fail QC.",
        "No missing OI values are synthesized.",
        "",
        "Lowest coverage contracts:",
        "",
        "Contract | Expiry | Valid/Expected | Coverage | Missing | OI errors | Volume errors | Quality errors | State",
        "--- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---",
    ]
    for row in qc.nsmallest(min(30, len(qc)), "coverage_ratio").itertuples(index=False):
        lines.append(
            f"{row.tradingsymbol} | {row.expiry} | {row.valid_candles}/{row.expected_candles} | "
            f"{row.coverage_ratio:.1%} | {row.missing_candles} | "
            f"{row.missing_or_negative_oi} | {row.invalid_volume} | {row.invalid_quality} | {row.state}"
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
    parser.add_argument("--min-contract-coverage", type=float, default=1.0,
                        help="Diagnostic coverage counter (default: 1.0); success always requires every contract COMPLETE.")
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
    if session_date > common.now_ist().date():
        raise ValueError("Cannot perform end-of-day QC for a future session")
    threshold = float(args.min_contract_coverage)
    if not math.isfinite(threshold) or not 0 < threshold <= 1:
        raise ValueError("min-contract-coverage must be finite and between 0 and 1")
    if (
        not args.allow_non_trading_day
        and not common.is_trading_day(session_date, common.load_holidays())
    ):
        _publish(session_date, "SKIPPED_NON_TRADING_DAY")
        return 0
    universe = load_session_universe(session_date)
    _publish(
        session_date, "RUNNING",
        phase="INITIAL_QC",
        session_date_ist=session_date.isoformat(),
        contracts_expected=len(universe),
    )
    qc = inspect_universe(universe, session_date)
    repair_outcomes: list[dict[str, Any]] = []
    if not args.no_repair and qc["state"].ne("COMPLETE").any():
        _publish(
            session_date, "RUNNING",
            phase="REPAIR",
            session_date_ist=session_date.isoformat(),
            incomplete_contracts=int(qc["state"].ne("COMPLETE").sum()),
        )
        repair_outcomes = _repair_universe(universe, qc, args)
        qc = inspect_universe(universe, session_date)

    dated_csv = common.QC_DIR / f"fno_oi_eod_qc_{session_date.isoformat()}.csv"
    dated_parquet = common.QC_DIR / f"fno_oi_eod_qc_{session_date.isoformat()}.parquet"
    dated_report = common.QC_DIR / f"fno_oi_eod_qc_{session_date.isoformat()}.md"
    latest_csv = common.LATEST_DIR / "latest_fno_oi_eod_qc.csv"
    latest_report = common.LATEST_DIR / "latest_fno_oi_eod_qc.md"
    common.atomic_write_csv(qc, dated_csv)
    common.atomic_write_parquet(qc, dated_parquet)
    report = render_report(session_date, qc, repair_outcomes)
    common.atomic_write_text(dated_report, report)
    current_session = session_date == common.now_ist().date()
    if current_session:
        common.atomic_write_csv(qc, latest_csv)
        common.atomic_write_text(latest_report, report)

    complete = int(qc["state"].eq("COMPLETE").sum())
    contracts_at_threshold = int(
        qc["coverage_ratio"].ge(threshold).sum()
    )
    repair_failed = sum(item.get("state") in {"FAILED", "INVALID_DATA"} for item in repair_outcomes)
    overall_ok = len(qc) > 0 and repair_failed == 0 and complete == len(qc)
    state = "SUCCESS" if overall_ok else "PARTIAL"
    _publish(
        session_date, state,
        phase="DONE",
        session_date_ist=session_date.isoformat(),
        contracts_expected=len(qc),
        contracts_complete=complete,
        contracts_at_coverage_threshold=contracts_at_threshold,
        coverage_threshold=threshold,
        required_valid_candles=75,
        strict_complete_required=True,
        repair_failed=repair_failed,
        output=latest_report if current_session else dated_report,
    )
    print(
        f"[EOD][{state}] complete={complete}/{len(qc)} "
        f"coverage_ok={contracts_at_threshold}/{len(qc)} repair_failed={repair_failed}",
        flush=True,
    )
    return 0 if overall_ok else 1


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        return run(args)
    except KeyboardInterrupt:
        session_date = date.fromisoformat(args.session_date) if args.session_date else common.now_ist().date()
        _publish(session_date, "STOPPED", heartbeat_state="STOPPED", phase="INTERRUPTED")
        return 130
    except Exception as exc:
        try:
            session_date = date.fromisoformat(args.session_date) if args.session_date else common.now_ist().date()
            _publish(session_date, "FAILED", heartbeat_state="CRASHED", phase="FAILED",
                     error=f"{type(exc).__name__}: {exc}")
        except (ValueError, OSError):
            # A malformed date or failed status write must still return failure;
            # it does not justify replacing a different session's live status.
            pass
        print(f"[FATAL] {type(exc).__name__}: {exc}", file=sys.stderr, flush=True)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
