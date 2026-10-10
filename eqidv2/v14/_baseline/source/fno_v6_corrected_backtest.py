"""V6 CORRECTED - point-in-time near-month futures OI.

The promoted V6 backtest
(``fno_oi_ema_confirm_0925_0930_0935_0940_0945_v6.py``) pins ONE static
universe snapshot, ``near_month_2026-08-11.parquet``, and replays it over the
whole history.  Every contract in that snapshot is 26AUG, so sessions from
2026-05-27 onward were scored against 26AUG open interest even on dates when
26AUG was a back-month nobody traded:

    month     median OI    median 1m volume    26AUG's real role
    2026-05       8,475                   0    3rd month out, untraded
    2026-06      60,088                   0    2nd month out, untraded
    2026-07     867,600                 112    next month, thin
    2026-08  13,512,662               1,850    front month, real

``oi_change_pct`` over that stretch measures a contract ageing into
front-month, not order flow.

This module replays the identical V6 setup book against the contract that was
actually the near month on each session:

  * ROLL POLICY - the near month for session ``d`` is the stored contract with
    the smallest expiry ``>= d``.  This matches the roll the live collector
    itself performs (its own snapshots switch 26AUG -> 26SEP on 2026-08-26,
    the session after the 2026-08-25 expiry).
  * ELIGIBILITY - a session is replayed only when that contract's 5-minute
    bars are actually stored for it.  Sessions whose true near month was never
    captured are dropped, never substituted.  No day is scored against an
    expired or a not-yet-front contract.

Nothing else changes: the setup book, cost model, confirmation policy, entry
and exit paths are imported from the promoted V6 module, so any difference in
the result is attributable to contract selection alone.
"""

from __future__ import annotations

import argparse
import time
from dataclasses import asdict
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

import fno_oi_common as common
import fno_oi_backtest_provenance as provenance
import fno_oi_ema_confirm_sweep as sw
import fno_oi_hybrid_data as hybrid
import fno_v5_hybrid_backtest as replay
import fno_oi_ema_confirm_0925_0930_0935_0940_0945_v6 as v6


def _install_pandas_compatible_ist() -> str:
    """Make ``common.IST`` usable by this environment's pandas.

    ``common.IST`` is a ``zoneinfo.ZoneInfo``. pandas 1.3.4 predates zoneinfo
    support: it misreads ZoneInfo as a fixed-offset tz and calls
    ``tz.utcoffset(None).total_seconds()``, but ``ZoneInfo.utcoffset(None)``
    returns ``None``, so every rebuild path dies with
    ``AttributeError: 'NoneType' object has no attribute 'total_seconds'``.
    The promoted V6 run only avoids this because it reads a signal cache that
    was built elsewhere; any genuine rebuild is broken on this interpreter.

    pytz's Asia/Kolkata is the same zone (UTC+05:30, no DST in India, ever),
    and pandas 1.3.4 handles it natively. Swapping the object in is behaviour
    preserving - it changes only the tzinfo implementation, never an instant.
    """
    if not str(getattr(common.IST, "__class__", "")).endswith("ZoneInfo'>"):
        return "unchanged"
    try:
        import pytz
    except ImportError:
        return "unavailable"
    try:
        # The exact sequence that fails under ZoneInfo on pandas 1.3.4.
        probe = pd.to_datetime(pd.Series(["2026-08-11 09:25:00"]))
        probe.dt.tz_localize(common.IST).dt.date
        return "not needed"
    except AttributeError:
        common.IST = pytz.timezone("Asia/Kolkata")
        return "applied"


IST_COMPAT = _install_pandas_compatible_ist()

STRATEGY_VERSION = "FNO_V6_CORRECTED_ROLLING_NEAR_MONTH"
OBJECTIVE = v6.OBJECTIVE
CONFIG_SOURCE = "V6_BEST_NET_SETUPS_WITH_POINT_IN_TIME_NEAR_MONTH_OI"
ROLL_POLICY = "NEAREST_UNEXPIRED_STORED_CONTRACT"

# The promoted V6 book, imported so it can never drift from this replay.
ACTIVE_SETUPS = v6.ACTIVE_SETUPS
FAULTY_UNIVERSE_PATH = v6.BACKTEST_UNIVERSE_PATH

RESULT_DIR = common.FNO_ROOT / "strategy_research" / "v6_corrected"
CACHE_DIR = RESULT_DIR / "_cache"
DAILY_OUTPUT_PATH = RESULT_DIR / "fno_v6_corrected_daily.csv"
AUDIT_OUTPUT_PATH = RESULT_DIR / "fno_v6_corrected_trades.csv"
SETUPS_OUTPUT_PATH = RESULT_DIR / "fno_v6_corrected_setups.csv"
ELIGIBILITY_PATH = RESULT_DIR / "fno_v6_corrected_session_eligibility.csv"
COMPARISON_PATH = RESULT_DIR / "fno_v6_corrected_vs_faulty.csv"
REPORT_PATH = RESULT_DIR / "fno_v6_corrected_report.md"
PROVENANCE_PATH = RESULT_DIR / "fno_v6_corrected_provenance.json"


# --------------------------------------------------------------------------
# roll calendar
# --------------------------------------------------------------------------
_MONTH_CODES = (
    "JAN", "FEB", "MAR", "APR", "MAY", "JUN",
    "JUL", "AUG", "SEP", "OCT", "NOV", "DEC",
)


def last_tuesday(year: int, month: int) -> date:
    """NSE monthly stock-future expiry: the last Tuesday of the month."""
    first_next = date(year + (month == 12), (month % 12) + 1, 1)
    last_day = first_next - timedelta(days=1)
    return last_day - timedelta(days=(last_day.weekday() - 1) % 7)


def observed_expiries() -> dict[str, date]:
    """Contract month -> expiry, read from every stored instrument master."""
    calendar: dict[str, date] = {}
    sources = [common.UNIVERSE_DIR / "contract_registry.parquet"]
    sources += sorted(common.MASTER_DIR.glob("instrument_master_*.parquet"))
    for path in sources:
        if not path.exists():
            continue
        try:
            frame = pd.read_parquet(path, columns=["tradingsymbol", "expiry"])
        except Exception:
            continue
        expiry = pd.to_datetime(frame["expiry"], errors="coerce")
        symbols = frame["tradingsymbol"].astype(str).str.upper()
        month = symbols.str.extract(r"(\d{2}[A-Z]{3})FUT", expand=False)
        keep = month.notna() & expiry.notna()
        for code, exp in zip(month[keep], expiry[keep].dt.date):
            calendar.setdefault(str(code), exp)
    return calendar


def build_expiry_calendar(
    first_session: date, last_session: date
) -> tuple[dict[str, date], dict[str, str]]:
    """Full monthly expiry series spanning the data, not just stored contracts.

    Months that were never captured must still appear, otherwise a May session
    would resolve its "nearest unexpired contract" to 26AUG - the very
    substitution this module exists to prevent. Observed expiries always win;
    the rest are derived from the last-Tuesday rule, which reproduces every
    observed Tuesday expiry exactly.
    """
    observed = observed_expiries()
    calendar: dict[str, date] = dict(observed)
    origin: dict[str, str] = {code: "OBSERVED" for code in observed}

    cursor = date(first_session.year, first_session.month, 1)
    horizon = max([last_session] + list(observed.values()))
    while cursor <= horizon:
        code = f"{cursor.year % 100:02d}{_MONTH_CODES[cursor.month - 1]}"
        if code not in calendar:
            calendar[code] = last_tuesday(cursor.year, cursor.month)
            origin[code] = "DERIVED_LAST_TUESDAY"
        cursor = date(cursor.year + (cursor.month == 12), (cursor.month % 12) + 1, 1)

    ordered = dict(sorted(calendar.items(), key=lambda kv: kv[1]))
    return ordered, origin


def near_month_for(session: date, calendar: dict[str, date]) -> str | None:
    """Nearest contract whose expiry has not passed on ``session``."""
    live = [(exp, code) for code, exp in calendar.items() if exp >= session]
    return min(live)[1] if live else None


# --------------------------------------------------------------------------
# stored-contract availability
# --------------------------------------------------------------------------
def regime_universe_paths() -> dict[str, Path]:
    """Latest dated snapshot that holds exactly one contract month."""
    by_month: dict[str, Path] = {}
    for path in sorted(common.UNIVERSE_DIR.glob("near_month_*.parquet")):
        try:
            frame = pd.read_parquet(path, columns=["tradingsymbol"])
        except Exception:
            continue
        months = set(
            frame["tradingsymbol"]
            .astype(str)
            .str.upper()
            .str.extract(r"(\d{2}[A-Z]{3})FUT", expand=False)
            .dropna()
        )
        if len(months) == 1:
            by_month[months.pop()] = path
    return by_month


def contract_sessions(universe: pd.DataFrame) -> dict[date, int]:
    """Session -> how many of this universe's futures have stored 5m bars."""
    counts: dict[date, int] = {}
    for symbol in universe["futures_tradingsymbol"].astype(str):
        path = common.raw_contract_path(symbol)
        if not path.exists():
            continue
        try:
            stamps = pd.read_parquet(path, columns=["timestamp"])["timestamp"]
        except Exception:
            continue
        # ``common.IST`` is a ZoneInfo; this pandas build cannot take ``.dt.date``
        # off a ZoneInfo-tz series, so convert through the equivalent key.
        days = (
            pd.to_datetime(stamps, utc=True)
            .dt.tz_convert("Asia/Kolkata")
            .dt.date.dropna()
            .unique()
        )
        for day in days:
            counts[day] = counts.get(day, 0) + 1
    return counts


def build_eligibility(
    regimes: dict[str, Path],
    *,
    min_coverage: float,
):
    """One row per candidate session with its roll decision and verdict."""
    universes: dict[str, pd.DataFrame] = {}
    coverage: dict[str, dict[date, int]] = {}
    for month, path in regimes.items():
        mapped, _ = provenance.load_backtest_universe(
            universe_path=path, contract_month_contains=month
        )
        universes[month] = mapped
        coverage[month] = contract_sessions(mapped)

    candidates: set[date] = set()
    for counts in coverage.values():
        candidates |= set(counts)
    if not candidates:
        raise RuntimeError("No stored futures sessions found.")

    # The calendar must span the data, including months never captured.
    calendar, origin = build_expiry_calendar(min(candidates), max(candidates))

    rows: list[dict[str, Any]] = []
    for session in sorted(candidates):
        month = near_month_for(session, calendar)
        if month is None:
            rows.append(
                {
                    "day": session,
                    "required_contract": "",
                    "expiry": "",
                    "universe_size": 0,
                    "contracts_with_data": 0,
                    "coverage": 0.0,
                    "eligible": False,
                    "reason": "NO_EXPIRY_CALENDAR",
                }
            )
            continue
        size = int(len(universes.get(month, [])))
        have = int(coverage.get(month, {}).get(session, 0))
        ratio = (have / size) if size else 0.0
        if month not in regimes:
            reason = "CONTRACT_NEVER_CAPTURED"
        elif have == 0:
            reason = "NEAR_MONTH_NOT_STORED_FOR_SESSION"
        elif ratio < min_coverage:
            reason = "INSUFFICIENT_CONTRACT_COVERAGE"
        else:
            reason = "OK"
        rows.append(
            {
                "day": session,
                "required_contract": month,
                "expiry": calendar.get(month, ""),
                "universe_size": size,
                "contracts_with_data": have,
                "coverage": round(ratio, 4),
                "eligible": reason == "OK",
                "reason": reason,
                "expiry_source": origin.get(month, ""),
            }
        )
    return pd.DataFrame(rows), calendar, origin


# --------------------------------------------------------------------------
# signal construction
# --------------------------------------------------------------------------
def _cache_key(month: str, days: list[date], square_off: str, bars: int) -> str:
    payload = {
        "month": month,
        "days": [str(d) for d in sorted(days)],
        "square_off": str(square_off),
        "max_forward_bars": int(bars),
        "roll_policy": ROLL_POLICY,
        "confirmation_policy": sw.CONFIRMATION_POLICY_V6_STRICT,
        "data_contract": hybrid.DATA_CONTRACT_VERSION,
    }
    return common.canonical_json_sha256(payload)[:16]


_FIELD_BY_SUFFIX = {"h": "high", "l": "low", "c": "close"}


def _load_cached(stem: Path):
    sig, npz = stem.with_suffix(".parquet"), stem.with_suffix(".npz")
    if not (sig.exists() and npz.exists()):
        return None
    signals = pd.read_parquet(sig)
    signals["day"] = pd.to_datetime(signals["day"]).dt.date
    blob = np.load(npz)
    paths: dict[int, dict[str, np.ndarray]] = {}
    for key in blob.files:
        sid_text, suffix = key.rsplit("_", 1)
        paths.setdefault(int(sid_text), {})[_FIELD_BY_SUFFIX[suffix]] = blob[key]
    return signals, paths


def _store_cached(stem: Path, signals: pd.DataFrame, paths: dict) -> None:
    stem.parent.mkdir(parents=True, exist_ok=True)
    common.atomic_write_parquet(signals, stem.with_suffix(".parquet"))
    flat: dict[str, np.ndarray] = {}
    for sid, path in paths.items():
        flat[f"{sid}_h"] = path["high"]
        flat[f"{sid}_l"] = path["low"]
        flat[f"{sid}_c"] = path["close"]
    np.savez_compressed(stem.with_suffix(".npz"), **flat)


def build_regime_signals(
    month: str,
    universe_path: Path,
    days: list[date],
    *,
    square_off: str,
    max_forward_bars: int,
    rebuild: bool,
    label: str = "",
):
    """Signals for one contract regime, restricted to its own sessions."""
    mapped, record = provenance.load_backtest_universe(
        universe_path=universe_path,
        contract_month_contains=month,
    )
    tag = label or month
    stem = CACHE_DIR / f"{tag}_{_cache_key(tag, days, square_off, max_forward_bars)}"
    cached = None if rebuild else _load_cached(stem)
    if cached is None:
        print(
            f"[BUILD] {tag}: {len(mapped)} contracts over {len(days)} sessions",
            flush=True,
        )
        signals, paths = sw.build_signal_table(
            set(days),
            square_off=square_off,
            max_forward_bars=max_forward_bars,
            mapped_universe=mapped,
            confirmation_policy=sw.CONFIRMATION_POLICY_V6_STRICT,
        )
        _store_cached(stem, signals, paths)
    else:
        print(f"[CACHE] {tag}: reusing {stem.name}", flush=True)
        signals, paths = cached
    signals = signals.copy()
    signals["day"] = pd.to_datetime(signals["day"]).dt.date
    signals = signals.loc[signals["day"].isin(set(days))].copy()
    signals["contract_month"] = month
    record = dict(record)
    record["contract_month"] = month
    record["sessions"] = len(days)
    return signals, paths, record


def concat_regimes(parts):
    """Merge regimes, re-basing ``sid`` so path keys never collide."""
    frames: list[pd.DataFrame] = []
    merged: dict[int, dict[str, np.ndarray]] = {}
    offset = 0
    for signals, paths in parts:
        if signals.empty and not paths:
            continue
        if not signals.empty:
            block = signals.copy()
            block["sid"] = block["sid"].astype(int) + offset
            frames.append(block)
        for sid, path in paths.items():
            merged[int(sid) + offset] = path
        offset += (max(paths) + 1) if paths else 0
    if not frames:
        return pd.DataFrame(), merged
    out = pd.concat(frames, ignore_index=True)
    return out.sort_values(["day", "sid"]).reset_index(drop=True), merged


def run_book(signals: pd.DataFrame, paths: dict, days: list[date], *, cost_bps: float,
             split_day: date):
    """Replay the V6 setup book and return (audit, daily, stats)."""
    audit = replay.replay_setups(
        signals, paths, cost_bps=cost_bps, setups=ACTIVE_SETUPS
    )
    if audit.empty:
        empty_daily = replay.build_daily_curve(
            pd.DataFrame(columns=["day", "side", "filled", "net_return_pct"]),
            days,
            split_day=split_day,
        )
        return audit, empty_daily, replay.summary_stats(empty_daily, audit)
    audit = audit.copy()
    audit["objective"] = OBJECTIVE
    audit["strategy_version"] = STRATEGY_VERSION
    daily = replay.build_daily_curve(audit, days, split_day=split_day)
    daily["objective"] = OBJECTIVE
    daily["strategy_version"] = STRATEGY_VERSION
    return audit, daily, replay.summary_stats(daily, audit)


# --------------------------------------------------------------------------
# reporting
# --------------------------------------------------------------------------
def _fmt(value: Any) -> str:
    if isinstance(value, float):
        if not np.isfinite(value):
            return "inf"
        return f"{value:,.6f}"
    return f"{value:,}" if isinstance(value, int) else str(value)


def render_report(
    eligibility: pd.DataFrame,
    daily: pd.DataFrame,
    audit: pd.DataFrame,
    setups: pd.DataFrame,
    stats: dict[str, Any],
    *,
    regimes: list[dict[str, Any]],
    cost_bps: float,
    split_day: date,
    parity: dict[str, Any] | None,
    faulty: dict[str, Any] | None,
) -> str:
    ok = eligibility.loc[eligibility["eligible"]]
    dropped = eligibility.loc[~eligibility["eligible"]]
    lines: list[str] = []
    lines.append("# FnO V6 CORRECTED - point-in-time near-month OI")
    lines.append("")
    lines.append(f"- strategy_version: `{STRATEGY_VERSION}`")
    lines.append(f"- roll_policy: `{ROLL_POLICY}`")
    lines.append(f"- setup_book: imported from `{v6.STRATEGY_VERSION}` ({len(ACTIVE_SETUPS)} setups)")
    lines.append(f"- cost_bps: {cost_bps}")
    lines.append(f"- split_day: {split_day}")
    lines.append("")

    lines.append("## Session eligibility")
    lines.append("")
    lines.append(f"- candidate sessions: **{len(eligibility)}**")
    lines.append(f"- replayed (true near month stored): **{len(ok)}**")
    lines.append(f"- dropped: **{len(dropped)}**")
    lines.append("")
    if not dropped.empty:
        lines.append("| reason | sessions | first | last |")
        lines.append("|---|---:|---|---|")
        for reason, grp in dropped.groupby("reason"):
            lines.append(
                f"| {reason} | {len(grp)} | {grp['day'].min()} | {grp['day'].max()} |"
            )
        lines.append("")
    lines.append("### Contract regimes replayed")
    lines.append("")
    lines.append("| contract | expiry | universe | sessions | first | last |")
    lines.append("|---|---|---:|---:|---|---|")
    for regime in regimes:
        lines.append(
            f"| {regime['contract_month']} | {regime['expiry']} | "
            f"{regime['universe_size']} | {regime['sessions']} | "
            f"{regime['first_day']} | {regime['last_day']} |"
        )
    lines.append("")

    lines.append("## Corrected result")
    lines.append("")
    lines.append("| metric | value |")
    lines.append("|---|---:|")
    for key, value in stats.items():
        lines.append(f"| {key} | {_fmt(value)} |")
    lines.append("")

    if parity is not None:
        lines.append("## Pipeline parity check")
        lines.append("")
        lines.append(
            "On sessions where the corrected roll selects the SAME contract the "
            "faulty run pinned (26AUG), the two must agree exactly. Any "
            "difference here would mean this module changed something other "
            "than contract selection."
        )
        lines.append("")
        lines.append("| check | value |")
        lines.append("|---|---:|")
        for key, value in parity.items():
            lines.append(f"| {key} | {_fmt(value)} |")
        lines.append("")

    if faulty is not None:
        lines.append("## Corrected vs faulty")
        lines.append("")
        lines.append("| metric | faulty (static 26AUG) | corrected (rolling) |")
        lines.append("|---|---:|---:|")
        for key in stats:
            if key in faulty:
                lines.append(f"| {key} | {_fmt(faulty[key])} | {_fmt(stats[key])} |")
        lines.append("")

    if not setups.empty:
        lines.append("## Per-setup")
        lines.append("")
        lines.append("| " + " | ".join(str(c) for c in setups.columns) + " |")
        lines.append("|" + "---|" * len(setups.columns))
        for row in setups.to_dict("records"):
            lines.append("| " + " | ".join(_fmt(row[c]) for c in setups.columns) + " |")
        lines.append("")

    lines.append("## Daily curve")
    lines.append("")
    cols = [c for c in ("day", "selections", "fills", "portfolio_net_return_pct") if c in daily.columns]
    lines.append("| " + " | ".join(cols) + " |")
    lines.append("|" + "---|" * len(cols))
    for row in daily.to_dict("records"):
        lines.append("| " + " | ".join(_fmt(row[c]) for c in cols) + " |")
    lines.append("")
    return "\n".join(lines)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--split-day", default="2026-08-14")
    parser.add_argument("--cost-bps", type=float, default=5.0)
    parser.add_argument("--square-off", default="1530")
    parser.add_argument("--max-forward-bars", type=int, default=400)
    parser.add_argument(
        "--min-contract-coverage",
        type=float,
        default=0.80,
        help="fraction of the point-in-time universe that must have stored "
        "near-month bars for a session to be replayed",
    )
    parser.add_argument("--rebuild-cache", action="store_true")
    parser.add_argument(
        "--compare-faulty",
        action="store_true",
        help="also replay the static 26AUG universe over the same eligible "
        "sessions and diff the two",
    )
    parser.add_argument(
        "--eligibility-only",
        action="store_true",
        help="print the roll calendar and session verdicts, then stop",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    started = time.monotonic()
    RESULT_DIR.mkdir(parents=True, exist_ok=True)

    regimes = regime_universe_paths()
    print(
        "[ROLL] stored contract regimes: "
        + ", ".join(f"{m} <- {p.name}" for m, p in sorted(regimes.items())),
        flush=True,
    )

    eligibility, calendar, origin = build_eligibility(
        regimes, min_coverage=args.min_contract_coverage
    )
    print("[ROLL] expiry calendar:", flush=True)
    for code, expiry in calendar.items():
        stored = "stored" if code in regimes else "NOT STORED"
        print(
            f"         {code}  expiry {expiry} ({expiry.strftime('%a')})  "
            f"{origin.get(code, '')}  {stored}",
            flush=True,
        )
    common.atomic_write_csv(eligibility, ELIGIBILITY_PATH)
    ok = eligibility.loc[eligibility["eligible"]]
    print(
        f"[ELIGIBLE] {len(ok)} of {len(eligibility)} candidate sessions "
        f"(min coverage {args.min_contract_coverage:.0%})",
        flush=True,
    )
    for reason, grp in eligibility.loc[~eligibility["eligible"]].groupby("reason"):
        print(
            f"           dropped {len(grp):>3}  {reason}  "
            f"{grp['day'].min()} .. {grp['day'].max()}",
            flush=True,
        )
    if ok.empty:
        raise RuntimeError("V6 corrected has no eligible sessions to replay.")
    if args.eligibility_only:
        print(f"[WROTE] {ELIGIBILITY_PATH}", flush=True)
        return 0

    days_by_month: dict[str, list[date]] = {}
    for row in ok.to_dict("records"):
        days_by_month.setdefault(str(row["required_contract"]), []).append(row["day"])

    parts: list[tuple[pd.DataFrame, dict]] = []
    regime_records: list[dict[str, Any]] = []
    for month in sorted(days_by_month, key=lambda m: calendar[m]):
        days = sorted(days_by_month[month])
        signals, paths, record = build_regime_signals(
            month,
            regimes[month],
            days,
            square_off=args.square_off,
            max_forward_bars=args.max_forward_bars,
            rebuild=args.rebuild_cache,
        )
        parts.append((signals, paths))
        regime_records.append(
            {
                "contract_month": month,
                "expiry": calendar[month],
                "universe_size": int(
                    ok.loc[ok["required_contract"] == month, "universe_size"].max()
                ),
                "sessions": len(days),
                "first_day": days[0],
                "last_day": days[-1],
                "universe_record": record,
            }
        )

    signals, paths = concat_regimes(parts)
    if signals.empty:
        raise RuntimeError("V6 corrected produced no candidate signals.")
    days = sorted(set(signals["day"]))
    print(f"[DATA] {len(signals):,} candidate signals over {len(days)} sessions", flush=True)

    # A session can clear the futures gate and still not replay - most often
    # because the equity 1m backtest store is written end-of-day and has no
    # bars for today yet. Losing it silently is the same class of defect this
    # module exists to remove, so reconcile and say so.
    unreplayed = sorted(set(ok["day"]) - set(days))
    if unreplayed:
        equity_root = hybrid.DEFAULT_BACKTEST_EQUITY_1M_DIR
        for session in unreplayed:
            eligibility.loc[eligibility["day"] == session, "eligible"] = False
            eligibility.loc[eligibility["day"] == session, "reason"] = (
                "NEAR_MONTH_OK_BUT_NO_EQUITY_BARS_OR_CANDIDATES"
            )
            print(
                f"[WARN] {session}: near month present but no replayable "
                f"signals; equity 1m store ({equity_root.name}) is the usual "
                "cause on the current session",
                flush=True,
            )
        common.atomic_write_csv(eligibility, ELIGIBILITY_PATH)
        ok = eligibility.loc[eligibility["eligible"]]

    split_day = pd.Timestamp(args.split_day).date()
    audit, daily, stats = run_book(
        signals, paths, days, cost_bps=args.cost_bps, split_day=split_day
    )
    if audit.empty:
        raise RuntimeError("V6 corrected selected no orders.")
    setups = v6.build_setup_summary(audit)

    parity: dict[str, Any] | None = None
    faulty_stats: dict[str, Any] | None = None
    if args.compare_faulty:
        faulty_month = "26AUG"
        faulty_days = sorted(set(ok["day"]))
        f_signals, f_paths, _ = build_regime_signals(
            faulty_month,
            FAULTY_UNIVERSE_PATH,
            faulty_days,
            square_off=args.square_off,
            max_forward_bars=args.max_forward_bars,
            rebuild=args.rebuild_cache,
            label="FAULTY_STATIC_26AUG",
        )
        f_days = sorted(set(f_signals["day"])) if not f_signals.empty else []
        f_audit, f_daily, faulty_stats = run_book(
            f_signals, f_paths, f_days, cost_bps=args.cost_bps, split_day=split_day
        )

        shared = sorted(set(days_by_month.get(faulty_month, [])))
        left = daily.loc[daily["day"].isin(shared)].set_index("day")
        right = f_daily.loc[f_daily["day"].isin(shared)].set_index("day")
        common_cols = [
            c
            for c in ("selections", "fills", "portfolio_net_return_pct")
            if c in left.columns and c in right.columns
        ]
        aligned = left[common_cols].join(right[common_cols], how="inner", rsuffix="_faulty")
        mismatches = 0
        for col in common_cols:
            mismatches += int(
                (~np.isclose(
                    aligned[col].to_numpy(float),
                    aligned[f"{col}_faulty"].to_numpy(float),
                    rtol=0.0,
                    atol=1e-9,
                )).sum()
            )
        parity = {
            "shared_26AUG_sessions": len(shared),
            "compared_sessions": int(len(aligned)),
            "cell_mismatches": mismatches,
            "identical": mismatches == 0,
        }

        merged = daily[["day", "selections", "fills", "portfolio_net_return_pct"]].merge(
            f_daily[["day", "selections", "fills", "portfolio_net_return_pct"]],
            on="day",
            how="outer",
            suffixes=("_corrected", "_faulty"),
        ).sort_values("day")
        merged["contract_used"] = merged["day"].map(
            {d: m for m, ds in days_by_month.items() for d in ds}
        )
        common.atomic_write_csv(merged, COMPARISON_PATH)

    common.atomic_write_csv(daily, DAILY_OUTPUT_PATH)
    common.atomic_write_csv(audit, AUDIT_OUTPUT_PATH)
    common.atomic_write_csv(setups, SETUPS_OUTPUT_PATH)
    common.atomic_write_text(
        REPORT_PATH,
        render_report(
            eligibility,
            daily,
            audit,
            setups,
            stats,
            regimes=regime_records,
            cost_bps=args.cost_bps,
            split_day=split_day,
            parity=parity,
            faulty=faulty_stats,
        ),
    )
    common.atomic_write_json(
        PROVENANCE_PATH,
        {
            "strategy_version": STRATEGY_VERSION,
            "objective": OBJECTIVE,
            "config_source": CONFIG_SOURCE,
            "roll_policy": ROLL_POLICY,
            "generated_at_ist": common.now_ist().isoformat(timespec="seconds"),
            "data_contract": hybrid.DATA_CONTRACT_VERSION,
            "confirmation_policy": sw.CONFIRMATION_POLICY_V6_STRICT,
            "expiry_calendar": {k: str(v) for k, v in calendar.items()},
            "regimes": [
                {k: (str(v) if isinstance(v, date) else v) for k, v in r.items()}
                for r in regime_records
            ],
            "parameters": {
                "split_day": str(args.split_day),
                "cost_bps": float(args.cost_bps),
                "square_off": str(args.square_off),
                "max_forward_bars": int(args.max_forward_bars),
                "min_contract_coverage": float(args.min_contract_coverage),
            },
            "sessions_replayed": len(days),
            "sessions_dropped": int((~eligibility["eligible"]).sum()),
            "active_setups": [asdict(setup) for setup in ACTIVE_SETUPS],
            "stats": {k: (float(v) if isinstance(v, (int, float)) else v)
                      for k, v in stats.items()},
            "parity_vs_faulty": parity,
            "faulty_stats": faulty_stats,
        },
    )

    print("", flush=True)
    print(f"[RESULT] sessions={stats['sessions']} orders={stats['orders']} "
          f"fills={stats['fills']}", flush=True)
    for key in ("trade_pf", "day_pf", "net_pct"):
        if key in stats:
            print(f"         {key}={_fmt(stats[key])}", flush=True)
    if parity is not None:
        print(f"[PARITY] shared 26AUG sessions identical to faulty run: "
              f"{parity['identical']} ({parity['cell_mismatches']} mismatched cells)",
              flush=True)
    print(f"[WROTE] {REPORT_PATH}", flush=True)
    print(f"[DONE] {time.monotonic() - started:.1f}s", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
