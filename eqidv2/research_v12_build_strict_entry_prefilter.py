"""Derive entry-safe hourly K300 snapshots from a combined prefilter CSV.

The V12 hourly entry schedule can consume lists stamped 09:20 through 14:20.
For a session to survive this research-only sanitizer, every one of those six
actionable snapshots must contain exactly 300 normalized unique tickers, the
complete rank set 1..300, bars completed exactly at the slot, and zero
staleness.  One bad actionable slot excludes the entire session.

The 15:20 snapshot cannot activate an intraday entry under the membership
schedule.  It is therefore audited as exact/degraded/absent but never causes a
session exclusion and is never emitted to the derived entry CSV.

This utility does not rebuild rankings and does not touch production files.
It only validates, filters, normalizes, and records hashes/provenance.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Sequence

import pandas as pd


IST = "Asia/Kolkata"
BUDGET = 300
ACTIONABLE_TIMES = ("09:20", "10:20", "11:20", "12:20", "13:20", "14:20")
AUDIT_ONLY_TIME = "15:20"
REQUIRED_COLUMNS = (
    "slot_ist",
    "ticker",
    "selection_rank",
    "date",
    "staleness_seconds",
)
DEFAULT_DERIVED_NAME = "hourly_candidates_strict_entry.csv"


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _json_value(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_value(item) for item in value]
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if pd.isna(value):
        return None
    if hasattr(value, "item"):
        return value.item()
    return value


def _timestamp_ist(value: object) -> pd.Timestamp:
    try:
        stamp = pd.Timestamp(value)
    except Exception:
        return pd.NaT
    if pd.isna(stamp):
        return pd.NaT
    try:
        return stamp.tz_localize(IST) if stamp.tzinfo is None else stamp.tz_convert(IST)
    except Exception:
        return pd.NaT


def _normalise_timestamps(values: pd.Series) -> pd.Series:
    return values.map(_timestamp_ist)


def _expected_slot(session_date: str, clock: str) -> pd.Timestamp:
    return pd.Timestamp(f"{session_date} {clock}", tz=IST)


def _session_strings(values: Iterable[object]) -> list[str]:
    dates: set[str] = set()
    for value in values:
        try:
            stamp = pd.Timestamp(value)
        except Exception:
            continue
        if pd.isna(stamp):
            continue
        if stamp.tzinfo is not None:
            stamp = stamp.tz_convert(IST).tz_localize(None)
        dates.add(stamp.strftime("%Y-%m-%d"))
    return sorted(dates)


def load_session_calendar(path: Path) -> list[str]:
    """Read expected session dates from a CSV or one-date-per-line text file."""

    if path.suffix.lower() == ".csv":
        frame = pd.read_csv(path)
        for column in ("trade_date", "session_date", "date"):
            if column in frame.columns:
                return _session_strings(frame[column])
        if len(frame.columns) == 1:
            return _session_strings(frame.iloc[:, 0])
        raise ValueError(
            "session calendar CSV needs trade_date, session_date, date, or one column"
        )
    values = [line.strip() for line in path.read_text(encoding="utf-8").splitlines()]
    return _session_strings(value for value in values if value)


@dataclass(frozen=True)
class DerivationResult:
    derived: pd.DataFrame
    session_audit: pd.DataFrame
    slot_audit: pd.DataFrame
    excluded_sessions: pd.DataFrame
    global_audit: dict[str, Any]


def _validate_slot(group: pd.DataFrame, expected: pd.Timestamp) -> list[str]:
    """Return deterministic failure reasons for one expected K300 snapshot."""

    if group.empty:
        return ["missing_slot"]

    reasons: list[str] = []
    if len(group) != BUDGET:
        reasons.append(f"row_count_{len(group)}_expected_{BUDGET}")

    tickers = group["_ticker_normalized"]
    valid_tickers = tickers.ne("") & tickers.notna()
    if not valid_tickers.all():
        reasons.append("blank_or_invalid_ticker")
    unique_tickers = int(tickers.loc[valid_tickers].nunique())
    if unique_tickers != BUDGET:
        reasons.append(f"unique_tickers_{unique_tickers}_expected_{BUDGET}")

    ranks = pd.to_numeric(group["selection_rank"], errors="coerce")
    integral = ranks.notna() & ranks.mod(1).eq(0)
    rank_set = set(ranks.loc[integral].astype(int).tolist())
    if not integral.all() or rank_set != set(range(1, BUDGET + 1)):
        reasons.append("rank_set_not_exact_1_through_300")
    if int(ranks.nunique(dropna=True)) != BUDGET:
        reasons.append("ranks_not_unique_300")

    if not group["_slot_ts"].eq(expected).all():
        reasons.append("slot_timestamp_not_exact_expected_slot")
    if not group["_bar_ts"].eq(expected).all():
        reasons.append("selected_bar_not_completed_exactly_at_slot")

    staleness = pd.to_numeric(group["staleness_seconds"], errors="coerce")
    if not staleness.notna().all() or not staleness.eq(0.0).all():
        reasons.append("staleness_not_exactly_zero")
    return reasons


def derive_strict_entry_snapshots(
    frame: pd.DataFrame,
    expected_sessions: Iterable[str] | None = None,
) -> DerivationResult:
    """Validate full sessions and return only exact actionable snapshots."""

    missing_columns = sorted(set(REQUIRED_COLUMNS) - set(frame.columns))
    if missing_columns:
        raise ValueError("missing required columns: " + ",".join(missing_columns))

    work = frame.copy()
    work["_input_row"] = range(len(work))
    work["_slot_ts"] = _normalise_timestamps(work["slot_ist"])
    work["_bar_ts"] = _normalise_timestamps(work["date"])
    work["_ticker_normalized"] = (
        work["ticker"].astype("string").fillna("").str.upper().str.strip()
    )
    valid_slot = work["_slot_ts"].notna()
    work["_session_date"] = work["_slot_ts"].map(
        lambda value: value.strftime("%Y-%m-%d") if pd.notna(value) else None
    )

    observed_sessions = sorted(work.loc[valid_slot, "_session_date"].dropna().unique())
    if expected_sessions is None:
        sessions = observed_sessions
        session_source = "observed_valid_slot_dates"
    else:
        sessions = _session_strings(expected_sessions)
        session_source = "explicit_session_calendar"
    if not sessions:
        raise ValueError("no normal sessions available for validation")

    slot_rows: list[dict[str, Any]] = []
    session_rows: list[dict[str, Any]] = []
    included_frames: list[pd.DataFrame] = []

    for session_date in sessions:
        day = work.loc[work["_session_date"].eq(session_date)].copy()
        day_reasons: list[str] = []
        for clock in ACTIONABLE_TIMES:
            expected = _expected_slot(session_date, clock)
            group = day.loc[day["_slot_ts"].eq(expected)].copy()
            reasons = _validate_slot(group, expected)
            passed = not reasons
            if not passed:
                day_reasons.extend(f"{clock}:{reason}" for reason in reasons)
            slot_rows.append(
                {
                    "session_date": session_date,
                    "slot_time": clock,
                    "actionable": True,
                    "rows": int(len(group)),
                    "status": "exact" if passed else "failed",
                    "reasons": ";".join(reasons),
                }
            )

        final_expected = _expected_slot(session_date, AUDIT_ONLY_TIME)
        final_group = day.loc[day["_slot_ts"].eq(final_expected)].copy()
        final_reasons = _validate_slot(final_group, final_expected)
        if final_group.empty:
            final_status = "absent_permitted"
        elif final_reasons:
            final_status = "degraded_permitted"
        else:
            final_status = "exact_permitted"
        slot_rows.append(
            {
                "session_date": session_date,
                "slot_time": AUDIT_ONLY_TIME,
                "actionable": False,
                "rows": int(len(final_group)),
                "status": final_status,
                "reasons": ";".join(final_reasons),
            }
        )

        included = not day_reasons
        if included:
            actionable_expected = {
                _expected_slot(session_date, clock) for clock in ACTIONABLE_TIMES
            }
            included_frames.append(day.loc[day["_slot_ts"].isin(actionable_expected)].copy())
        session_rows.append(
            {
                "session_date": session_date,
                "included": included,
                "actionable_status": "exact" if included else "excluded",
                "actionable_failure_reasons": ";".join(day_reasons),
                "audit_15_20_status": final_status,
                "audit_15_20_reasons": ";".join(final_reasons),
            }
        )

    derived = (
        pd.concat(included_frames, ignore_index=True)
        if included_frames
        else work.iloc[0:0].copy()
    )
    if not derived.empty:
        derived = derived.sort_values(
            ["_slot_ts", "selection_rank", "_ticker_normalized", "_input_row"],
            kind="mergesort",
        ).copy()
        derived["slot_ist"] = derived["_slot_ts"].map(lambda value: value.isoformat())
        derived["date"] = derived["_bar_ts"].map(lambda value: value.isoformat())
        derived["ticker"] = derived["_ticker_normalized"]
        derived["selection_rank"] = pd.to_numeric(
            derived["selection_rank"], errors="raise"
        ).astype(int)
        derived["staleness_seconds"] = 0.0
    derived = derived.drop(
        columns=[
            "_input_row",
            "_slot_ts",
            "_bar_ts",
            "_ticker_normalized",
            "_session_date",
        ],
        errors="ignore",
    )
    derived = derived.loc[:, frame.columns]

    session_audit = pd.DataFrame(session_rows)
    slot_audit = pd.DataFrame(slot_rows)
    excluded = session_audit.loc[~session_audit["included"]].copy()
    unexpected_sessions = sorted(set(observed_sessions) - set(sessions))
    expected_clock_set = set(ACTIONABLE_TIMES) | {AUDIT_ONLY_TIME}
    unexpected_slot_rows = work.loc[
        valid_slot
        & work["_session_date"].isin(sessions)
        & ~work["_slot_ts"].map(
            lambda value: value.strftime("%H:%M") if pd.notna(value) else ""
        ).isin(expected_clock_set)
    ]
    global_audit = {
        "session_source": session_source,
        "observed_session_dates": observed_sessions,
        "expected_session_dates": sessions,
        "unexpected_observed_session_dates": unexpected_sessions,
        "input_rows": int(len(work)),
        "invalid_slot_timestamp_rows": int(work["_slot_ts"].isna().sum()),
        "invalid_bar_timestamp_rows": int(work["_bar_ts"].isna().sum()),
        "unexpected_slot_time_rows_on_expected_sessions": int(len(unexpected_slot_rows)),
        "derived_rows": int(len(derived)),
    }
    return DerivationResult(
        derived=derived,
        session_audit=session_audit,
        slot_audit=slot_audit,
        excluded_sessions=excluded,
        global_audit=global_audit,
    )


def _ensure_output_targets(
    input_path: Path,
    output_dir: Path,
    derived_name: str,
    overwrite: bool,
) -> dict[str, Path]:
    targets = {
        "derived": output_dir / derived_name,
        "session_audit": output_dir / "session_audit.csv",
        "slot_audit": output_dir / "slot_audit.csv",
        "excluded_sessions": output_dir / "excluded_sessions.csv",
        "contract": output_dir / "strict_entry_contract.json",
    }
    if targets["derived"].resolve() == input_path.resolve():
        raise ValueError("derived CSV must not overwrite the input CSV")
    existing = [str(path) for path in targets.values() if path.exists()]
    if existing and not overwrite:
        raise FileExistsError(
            "output artifacts already exist; pass --overwrite: " + ", ".join(existing)
        )
    return targets


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Derive session-complete actionable K300 hourly entry snapshots"
    )
    parser.add_argument("--input", type=Path, required=True, help="combined hourly prefilter CSV")
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument(
        "--session-calendar",
        type=Path,
        help="optional CSV/text list of normal sessions, including entirely absent days",
    )
    parser.add_argument("--derived-name", default=DEFAULT_DERIVED_NAME)
    parser.add_argument("--overwrite", action="store_true")
    return parser.parse_args(argv)


def run(args: argparse.Namespace) -> dict[str, Any]:
    input_path = args.input.resolve()
    if not input_path.is_file():
        raise FileNotFoundError(f"input CSV not found: {input_path}")
    if args.session_calendar is not None and not args.session_calendar.is_file():
        raise FileNotFoundError(f"session calendar not found: {args.session_calendar}")
    if not str(args.derived_name).lower().endswith(".csv"):
        raise ValueError("--derived-name must end in .csv")

    args.output_dir.mkdir(parents=True, exist_ok=True)
    targets = _ensure_output_targets(
        input_path, args.output_dir, args.derived_name, args.overwrite
    )
    frame = pd.read_csv(input_path)
    expected_sessions = (
        load_session_calendar(args.session_calendar)
        if args.session_calendar is not None
        else None
    )
    result = derive_strict_entry_snapshots(frame, expected_sessions)

    result.derived.to_csv(targets["derived"], index=False)
    result.session_audit.to_csv(targets["session_audit"], index=False)
    result.slot_audit.to_csv(targets["slot_audit"], index=False)
    result.excluded_sessions.to_csv(targets["excluded_sessions"], index=False)

    included = result.session_audit.loc[result.session_audit["included"]]
    final_counts = result.session_audit["audit_15_20_status"].value_counts().to_dict()
    expected_rows = int(len(included) * len(ACTIONABLE_TIMES) * BUDGET)
    contract: dict[str, Any] = {
        "schema_version": 1,
        "research_only": True,
        "production_approved": False,
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "entry_contract": {
            "timezone": IST,
            "budget": BUDGET,
            "actionable_slots": list(ACTIONABLE_TIMES),
            "audit_only_slot": AUDIT_ONLY_TIME,
            "required_actionable_properties": [
                "exactly_300_rows",
                "300_unique_normalized_tickers",
                "rank_set_exactly_1_through_300",
                "selected_bar_completed_exactly_at_slot",
                "staleness_seconds_exactly_zero",
            ],
            "session_policy": "exclude_entire_day_if_any_actionable_slot_fails",
            "audit_only_policy": "15:20_exact_degraded_or_absent_never_excludes_and_is_not_emitted",
        },
        "input": {
            "path": str(input_path),
            "bytes": input_path.stat().st_size,
            "sha256": _sha256(input_path),
            "rows": int(len(frame)),
        },
        "session_calendar": (
            {
                "path": str(args.session_calendar.resolve()),
                "sha256": _sha256(args.session_calendar),
            }
            if args.session_calendar is not None
            else None
        ),
        "audit": {
            **result.global_audit,
            "sessions_considered": int(len(result.session_audit)),
            "sessions_included": int(result.session_audit["included"].sum()),
            "sessions_excluded": int((~result.session_audit["included"]).sum()),
            "excluded_session_reasons": {
                str(row.session_date): str(row.actionable_failure_reasons)
                for row in result.excluded_sessions.itertuples()
            },
            "audit_15_20_status_counts": final_counts,
        },
        "derived": {
            "path": str(targets["derived"].resolve()),
            "bytes": targets["derived"].stat().st_size,
            "sha256": _sha256(targets["derived"]),
            "rows": int(len(result.derived)),
            "expected_rows_from_included_sessions": expected_rows,
            "all_rows_are_actionable": True,
            "contains_15_20_rows": False,
        },
        "audit_artifacts": {
            key: {
                "path": str(path.resolve()),
                "sha256": _sha256(path),
            }
            for key, path in targets.items()
            if key not in {"derived", "contract"}
        },
        "provenance": {
            "utility": str(Path(__file__).resolve()),
            "utility_sha256": _sha256(Path(__file__)),
        },
    }
    if len(result.derived) != expected_rows:
        raise RuntimeError(
            f"derived row reconciliation failed: {len(result.derived)} != {expected_rows}"
        )
    targets["contract"].write_text(
        json.dumps(_json_value(contract), indent=2), encoding="utf-8"
    )
    return contract


def main(argv: Sequence[str] | None = None) -> int:
    contract = run(parse_args(argv))
    print(json.dumps(_json_value(contract), indent=2), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
