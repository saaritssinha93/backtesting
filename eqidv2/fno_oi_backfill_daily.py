"""Backfill long-history daily continuous futures OHLCV + OI for backtesting.

Kite serves ``continuous=True`` only on the ``day`` interval (every intraday
interval rejects it with "invalid interval for continuous data"). That makes the
daily continuous series the only way to get multi-year futures OI depth without
recovering expired instrument tokens.

Measured platform limits (probed 2026-08-10, verified across RELIANCE, TCS,
SBIN, NIFTY, BANKNIFTY):

* ``day`` + ``continuous=True`` + ``oi=True`` is accepted.
* A single request may span at most 2000 days.
* Price/volume reaches back to ~2011, but **open interest is zero before
  2019-01-22** on every underlying probed -- that date is a platform-wide
  cutoff, not a per-symbol listing artefact. Underlyings added to F&O later
  start at their own inclusion date (e.g. 360ONE from 2025-06-27).

Output layout (additive; nothing here is read by the live 5-minute pipeline)::

    <runtime>/fno_oi/daily_continuous/<UNDERLYING>_daily.parquet
    <runtime>/fno_oi/daily_continuous/_panel/fno_oi_daily_panel.parquet
    <runtime>/fno_oi/latest/latest_fno_oi_backfill_daily.md
"""

from __future__ import annotations

import argparse
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import date, datetime, time as dtime, timedelta
from pathlib import Path
from typing import Any, Iterable

import numpy as np
import pandas as pd

import fno_oi_common as common


SESSION = "fno_oi_backfill_daily"

DAILY_DIR = common.FNO_ROOT / "daily_continuous"
PANEL_DIR = DAILY_DIR / "_panel"
PANEL_PATH = PANEL_DIR / "fno_oi_daily_panel.parquet"
REPORT_PATH = common.LATEST_DIR / "latest_fno_oi_backfill_daily.md"

# Kite hard limit for a single ``day``-interval request.
MAX_REQUEST_DAYS = 2000
# Earliest date on which Kite returns non-zero OI (probed, platform-wide).
OI_FLOOR = date(2019, 1, 22)

MARKET_OPEN = dtime(9, 15)
MARKET_CLOSE = dtime(15, 30)

DAILY_DATA_VERSION = "fno_oi_daily_continuous_v1"

DAILY_COLUMNS = (
    "date",
    "underlying",
    "anchor_tradingsymbol",
    "instrument_token",
    "is_index_future",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "oi",
    "price_change",
    "price_change_pct",
    "oi_change",
    "oi_change_pct",
    "oi_signal",
    "roll_window",
    "fetch_timestamp",
    "source",
    "data_version",
)


@dataclass
class AppRuntime:
    app_name: str
    client: Any
    pace_seconds: float
    _last_call_at: float = 0.0
    _pace_lock: threading.Lock = field(default_factory=threading.Lock)

    def pace(self) -> None:
        with self._pace_lock:
            wait = self.pace_seconds - (time.monotonic() - self._last_call_at)
            if wait > 0:
                time.sleep(wait)
            self._last_call_at = time.monotonic()


def _build_app_runtimes(args: argparse.Namespace) -> list[AppRuntime]:
    credentials = common.discover_kite_credentials(max_apps=args.max_apps)
    runtimes: list[AppRuntime] = []
    failures: list[str] = []
    for credential in credentials:
        try:
            client = common.make_kite_client(credential, timeout_sec=args.timeout_sec)
            runtimes.append(
                AppRuntime(
                    app_name=credential.app_name,
                    client=client,
                    pace_seconds=max(0.34, float(args.request_interval_sec)),
                )
            )
        except Exception as exc:
            failures.append(f"{credential.app_name}:{type(exc).__name__}:{exc}")
    if not runtimes:
        raise RuntimeError(f"No usable Kite clients. Failures: {failures}")
    if failures:
        print(f"[AUTH][WARN] unusable apps: {failures}", flush=True)
    return runtimes


def market_is_open(moment: datetime, holidays: set[date]) -> bool:
    current = moment.astimezone(common.IST)
    if not common.is_trading_day(current.date(), holidays):
        return False
    return MARKET_OPEN <= current.time() <= MARKET_CLOSE


def detect_roll_bars(
    days: pd.Series,
    oi_change_pct: pd.Series,
    *,
    jump_pct: float = 150.0,
    month_end_days: int = 12,
    month_start_days: int = 4,
    widen_bars: int = 0,
) -> pd.Series:
    """Flag bars where the continuous series stitches to a new front month.

    A continuous series is stitched by Kite across contract rollovers, so OI
    steps discontinuously at expiry -- the new front month carries its own book.
    Any OI-change feature computed across that boundary is an artefact, so it is
    marked rather than silently emitting a fake signal.

    Detection is **data-driven rather than calendar-driven** on purpose. NSE
    moved monthly F&O expiry from the last Thursday to the last Tuesday (the
    2026-08 contract expires 2026-08-25, a Tuesday), so any hardcoded weekday
    rule is wrong on one side of the change and silently mislabels every roll.
    The stitch itself is unmistakable in the data: OI typically moves by an
    order of magnitude. Requiring the jump to land near a month boundary keeps
    ordinary volatility from tripping it. Both ends of the boundary matter --
    when the last Tuesday is the final day of the month (2026-03-31,
    2026-06-30), the stitch appears on the 1st of the *next* month, so a
    month-end-only guard silently misses those rolls.

    ``widen_bars`` additionally flags N bars either side of each detected
    stitch, for excluding expiry-week roll decay -- OI bleeding out of the front
    month as positions migrate. Those bars are genuine but roll-driven rather
    than directional.
    """

    stamps = pd.to_datetime(days)
    magnitude = oi_change_pct.abs()
    days_in_month = stamps.dt.daysinmonth
    near_month_end = (days_in_month - stamps.dt.day) <= max(0, int(month_end_days))
    near_month_start = stamps.dt.day <= max(0, int(month_start_days))
    flagged = (magnitude >= float(jump_pct)) & (near_month_end | near_month_start)
    flagged = flagged.fillna(False).astype(bool)

    widen = max(0, int(widen_bars))
    if widen:
        widened = flagged.copy()
        for shift in range(1, widen + 1):
            widened |= flagged.shift(shift, fill_value=False)
            widened |= flagged.shift(-shift, fill_value=False)
        flagged = widened
    return pd.Series(flagged.to_numpy(), index=days.index, dtype=bool)


def classify_oi_signal(price_change: pd.Series, oi_change: pd.Series) -> pd.Series:
    """Standard four-quadrant futures OI read."""

    price_up = price_change > 0
    price_down = price_change < 0
    oi_up = oi_change > 0
    oi_down = oi_change < 0
    return pd.Series(
        np.select(
            [
                price_up & oi_up,
                price_down & oi_up,
                price_up & oi_down,
                price_down & oi_down,
            ],
            [
                "LONG_BUILDUP",
                "SHORT_BUILDUP",
                "SHORT_COVERING",
                "LONG_UNWINDING",
            ],
            default="NEUTRAL",
        ),
        index=price_change.index,
        dtype="object",
    )


def normalize_daily_records(
    records: Iterable[Any] | pd.DataFrame,
    contract: pd.Series,
    *,
    fetch_timestamp: datetime | None = None,
    roll_jump_pct: float = 150.0,
    roll_widen_bars: int = 0,
) -> pd.DataFrame:
    # Accept either Kite's list-of-dicts or an already-concatenated frame;
    # ``list(df)`` would otherwise yield column names rather than rows.
    if isinstance(records, pd.DataFrame):
        frame = records.copy()
    else:
        frame = pd.DataFrame(list(records))
    if frame.empty or "date" not in frame.columns:
        return pd.DataFrame(columns=list(DAILY_COLUMNS))

    for column in ("open", "high", "low", "close", "volume", "oi"):
        if column not in frame.columns:
            frame[column] = np.nan
        frame[column] = pd.to_numeric(frame[column], errors="coerce")

    frame["date"] = pd.to_datetime(frame["date"], errors="coerce", utc=True)
    frame["date"] = frame["date"].dt.tz_convert(common.IST).dt.tz_localize(None).dt.normalize()
    frame = frame.loc[frame["date"].notna()].copy()
    if frame.empty:
        return pd.DataFrame(columns=list(DAILY_COLUMNS))

    frame["underlying"] = str(contract.get("underlying", "")).strip().upper()
    frame["anchor_tradingsymbol"] = str(contract.get("tradingsymbol", "")).strip().upper()
    frame["instrument_token"] = int(contract.get("instrument_token"))
    frame["is_index_future"] = bool(contract.get("is_index_future", False))

    frame = (
        frame.drop_duplicates(subset=["date"], keep="last")
        .sort_values("date", kind="stable")
        .reset_index(drop=True)
    )

    frame["price_change"] = frame["close"].diff()
    frame["price_change_pct"] = frame["close"].pct_change() * 100.0
    frame["oi_change"] = frame["oi"].diff()
    previous_oi = frame["oi"].shift(1)
    frame["oi_change_pct"] = pd.Series(
        np.where(previous_oi.gt(0), frame["oi_change"] / previous_oi * 100.0, np.nan),
        index=frame.index,
    )
    frame["oi_signal"] = classify_oi_signal(frame["price_change"], frame["oi_change"])
    frame["roll_window"] = detect_roll_bars(
        frame["date"],
        frame["oi_change_pct"],
        jump_pct=roll_jump_pct,
        widen_bars=roll_widen_bars,
    )
    # Cross-expiry deltas are stitching artefacts, not real book changes.
    frame.loc[frame["roll_window"], ["oi_change", "oi_change_pct"]] = np.nan
    frame.loc[frame["roll_window"], "oi_signal"] = "ROLL"

    frame["fetch_timestamp"] = pd.Timestamp(fetch_timestamp or common.now_ist())
    frame["source"] = "kite_historical_continuous"
    frame["data_version"] = DAILY_DATA_VERSION
    return frame.loc[:, list(DAILY_COLUMNS)]


def daily_path(underlying: str) -> Path:
    return DAILY_DIR / f"{common.safe_contract_stem(underlying)}_daily.parquet"


def _historical_daily(
    runtime: AppRuntime,
    token: int,
    from_dt: date,
    to_dt: date,
    *,
    max_retries: int,
) -> list[dict[str, Any]]:
    last_error: Exception | None = None
    for attempt in range(1, max(1, int(max_retries)) + 1):
        try:
            runtime.pace()
            return runtime.client.historical_data(
                int(token),
                from_dt,
                to_dt,
                "day",
                continuous=True,
                oi=True,
            )
        except Exception as exc:
            last_error = exc
            message = str(exc).lower()
            if attempt >= max_retries:
                break
            if "429" in message or "too many requests" in message or "rate limit" in message:
                delay = max(2.0, 2.0**attempt)
            else:
                delay = min(8.0, 0.75 * (2 ** (attempt - 1)))
            time.sleep(delay)
    assert last_error is not None
    raise last_error


def plan_windows(start: date, end: date, *, max_days: int = MAX_REQUEST_DAYS) -> list[tuple[date, date]]:
    """Split [start, end] into chunks Kite will accept."""

    if start > end:
        return []
    windows: list[tuple[date, date]] = []
    cursor = start
    while cursor <= end:
        stop = min(end, cursor + timedelta(days=max_days - 1))
        windows.append((cursor, stop))
        cursor = stop + timedelta(days=1)
    return windows


def resume_start(underlying: str, floor: date, *, full_refresh: bool) -> date:
    path = daily_path(underlying)
    if full_refresh or not path.exists():
        return floor
    try:
        existing = pd.read_parquet(path, columns=["date"])
    except Exception:
        return floor
    if existing.empty:
        return floor
    last = pd.to_datetime(existing["date"], errors="coerce").max()
    if pd.isna(last):
        return floor
    # Re-pull the final stored day so a partially written session is corrected.
    return max(floor, last.date())


def backfill_one(
    runtime: AppRuntime,
    contract: pd.Series,
    *,
    floor: date,
    end: date,
    full_refresh: bool,
    max_retries: int,
    roll_jump_pct: float = 150.0,
    roll_widen_bars: int = 0,
) -> dict[str, Any]:
    underlying = str(contract["underlying"])
    started = time.monotonic()
    try:
        start = resume_start(underlying, floor, full_refresh=full_refresh)
        windows = plan_windows(start, end)
        if not windows:
            return {
                "underlying": underlying,
                "app": runtime.app_name,
                "state": "UP_TO_DATE",
                "rows": 0,
                "requests": 0,
                "elapsed_sec": time.monotonic() - started,
                "error": "",
            }
        collected: list[pd.DataFrame] = []
        for window_start, window_stop in windows:
            records = _historical_daily(
                runtime,
                int(contract["instrument_token"]),
                window_start,
                window_stop,
                max_retries=max_retries,
            )
            if records:
                collected.append(pd.DataFrame(records))
        if not collected:
            return {
                "underlying": underlying,
                "app": runtime.app_name,
                "state": "NO_DATA",
                "rows": 0,
                "requests": len(windows),
                "elapsed_sec": time.monotonic() - started,
                "error": "",
            }

        merged = pd.concat(collected, ignore_index=True, sort=False)
        rows = normalize_daily_records(
            merged,
            contract,
            roll_jump_pct=roll_jump_pct,
            roll_widen_bars=roll_widen_bars,
        )
        if rows.empty:
            return {
                "underlying": underlying,
                "app": runtime.app_name,
                "state": "NO_DATA",
                "rows": 0,
                "requests": len(windows),
                "elapsed_sec": time.monotonic() - started,
                "error": "",
            }

        path = daily_path(underlying)
        if path.exists() and not full_refresh:
            try:
                previous = pd.read_parquet(path)
                rows = pd.concat([previous, rows], ignore_index=True, sort=False)
            except Exception:
                pass
        rows["date"] = pd.to_datetime(rows["date"], errors="coerce")
        rows = (
            rows.drop_duplicates(subset=["date"], keep="last")
            .sort_values("date", kind="stable")
            .reset_index(drop=True)
        )
        rows = rows.loc[:, [c for c in DAILY_COLUMNS if c in rows.columns]]
        common.atomic_write_parquet(rows, path)

        oi_rows = int((pd.to_numeric(rows["oi"], errors="coerce") > 0).sum())
        return {
            "underlying": underlying,
            "app": runtime.app_name,
            "state": "WRITTEN",
            "rows": int(len(rows)),
            "oi_rows": oi_rows,
            "first_date": str(rows["date"].min())[:10],
            "last_date": str(rows["date"].max())[:10],
            "requests": len(windows),
            "elapsed_sec": time.monotonic() - started,
            "error": "",
        }
    except Exception as exc:
        return {
            "underlying": underlying,
            "app": runtime.app_name,
            "state": "FAILED",
            "rows": 0,
            "requests": 0,
            "elapsed_sec": time.monotonic() - started,
            "error": f"{type(exc).__name__}: {exc}",
        }


def _partition_rows(universe: pd.DataFrame, count: int) -> list[list[pd.Series]]:
    partitions: list[list[pd.Series]] = [[] for _ in range(max(1, count))]
    for index, (_, row) in enumerate(universe.sort_values("underlying").iterrows()):
        partitions[index % len(partitions)].append(row)
    return partitions


def backfill_all(
    universe: pd.DataFrame,
    runtimes: list[AppRuntime],
    *,
    floor: date,
    end: date,
    full_refresh: bool,
    max_retries: int,
    roll_jump_pct: float = 150.0,
    roll_widen_bars: int = 0,
) -> list[dict[str, Any]]:
    partitions = _partition_rows(universe, len(runtimes))
    total = int(len(universe))
    done = 0
    done_lock = threading.Lock()

    def _run_partition(runtime: AppRuntime, rows: Iterable[pd.Series]) -> list[dict[str, Any]]:
        nonlocal done
        results: list[dict[str, Any]] = []
        for contract in rows:
            outcome = backfill_one(
                runtime,
                contract,
                floor=floor,
                end=end,
                full_refresh=full_refresh,
                max_retries=max_retries,
                roll_jump_pct=roll_jump_pct,
                roll_widen_bars=roll_widen_bars,
            )
            results.append(outcome)
            with done_lock:
                done += 1
                position = done
            common.publish_heartbeat(
                SESSION,
                "RUNNING",
                app=runtime.app_name,
                underlying=contract["underlying"],
                progress=f"{position}/{total}",
            )
            if position % 25 == 0 or position == total:
                print(f"[PROGRESS] {position}/{total}", flush=True)
        return results

    outcomes: list[dict[str, Any]] = []
    with ThreadPoolExecutor(
        max_workers=len(runtimes), thread_name_prefix="fno-oi-daily"
    ) as pool:
        futures = [
            pool.submit(_run_partition, runtime, partition)
            for runtime, partition in zip(runtimes, partitions)
            if partition
        ]
        for future in as_completed(futures):
            outcomes.extend(future.result())
    return sorted(outcomes, key=lambda item: str(item["underlying"]))


def rebuild_panel() -> dict[str, Any]:
    files = sorted(DAILY_DIR.glob("*_daily.parquet"))
    if not files:
        return {"underlyings": 0, "rows": 0}
    frames = []
    for path in files:
        try:
            frames.append(pd.read_parquet(path))
        except Exception as exc:
            print(f"[PANEL][WARN] skipped {path.name}: {exc}", flush=True)
    if not frames:
        return {"underlyings": 0, "rows": 0}
    panel = pd.concat(frames, ignore_index=True, sort=False)
    panel["date"] = pd.to_datetime(panel["date"], errors="coerce")
    panel = (
        panel.drop_duplicates(subset=["underlying", "date"], keep="last")
        .sort_values(["underlying", "date"], kind="stable")
        .reset_index(drop=True)
    )
    common.atomic_write_parquet(panel, PANEL_PATH)
    return {
        "underlyings": int(panel["underlying"].nunique()),
        "rows": int(len(panel)),
        "first_date": str(panel["date"].min())[:10],
        "last_date": str(panel["date"].max())[:10],
        "path": str(PANEL_PATH),
    }


def render_report(
    outcomes: list[dict[str, Any]],
    panel: dict[str, Any],
    *,
    floor: date,
    end: date,
    duration_sec: float,
) -> str:
    states: dict[str, int] = {}
    for outcome in outcomes:
        states[outcome["state"]] = states.get(outcome["state"], 0) + 1
    written = [o for o in outcomes if o["state"] == "WRITTEN"]
    failed = [o for o in outcomes if o["state"] == "FAILED"]
    total_rows = sum(int(o.get("rows", 0)) for o in written)
    requests = sum(int(o.get("requests", 0)) for o in outcomes)

    lines = [
        "# FnO Daily Continuous OI Backfill",
        "",
        f"- Generated: {common.now_ist().isoformat(timespec='seconds')}",
        f"- Requested range: {floor.isoformat()} -> {end.isoformat()}",
        f"- Underlyings processed: {len(outcomes)}",
        f"- Kite requests: {requests}",
        f"- Rows stored: {total_rows:,}",
        f"- Duration: {duration_sec:.1f}s",
        "",
        "## Outcome",
        "",
        "| State | Count |",
        "| --- | ---: |",
    ]
    for state in sorted(states):
        lines.append(f"| {state} | {states[state]} |")

    lines += [
        "",
        "## Panel",
        "",
        f"- Underlyings: {panel.get('underlyings', 0)}",
        f"- Rows: {panel.get('rows', 0):,}",
        f"- Range: {panel.get('first_date', 'n/a')} -> {panel.get('last_date', 'n/a')}",
        f"- Path: `{panel.get('path', 'n/a')}`",
        "",
    ]

    if written:
        earliest = sorted(written, key=lambda o: str(o.get("first_date", "9999")))[:10]
        lines += [
            "## Deepest histories",
            "",
            "| Underlying | First | Last | Rows | OI rows |",
            "| --- | --- | --- | ---: | ---: |",
        ]
        for outcome in earliest:
            lines.append(
                f"| {outcome['underlying']} | {outcome.get('first_date', '')} | "
                f"{outcome.get('last_date', '')} | {outcome.get('rows', 0):,} | "
                f"{outcome.get('oi_rows', 0):,} |"
            )
        lines.append("")

    if failed:
        lines += ["## Failures", "", "| Underlying | Error |", "| --- | --- |"]
        for outcome in failed[:20]:
            lines.append(f"| {outcome['underlying']} | {outcome['error'][:120]} |")
        lines.append("")

    return "\n".join(lines)


def load_universe() -> pd.DataFrame:
    path = common.UNIVERSE_DIR / "latest_near_month.parquet"
    if not path.exists():
        raise FileNotFoundError(
            f"Near-month universe missing: {path}. Run fno_oi_universe.py first."
        )
    frame = pd.read_parquet(path)
    if frame.empty:
        raise ValueError(f"Near-month universe is empty: {path}")
    return frame.sort_values("underlying").reset_index(drop=True)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--from-date",
        default=OI_FLOOR.isoformat(),
        help=f"Start date (default {OI_FLOOR.isoformat()}, the OI availability floor).",
    )
    parser.add_argument("--to-date", default="", help="End date (default: today).")
    parser.add_argument(
        "--underlyings",
        default="",
        help="Comma-separated subset, e.g. RELIANCE,TCS. Default: whole near-month universe.",
    )
    parser.add_argument("--limit", type=int, default=0, help="Process only the first N underlyings.")
    parser.add_argument(
        "--full-refresh",
        action="store_true",
        help="Ignore stored history and re-pull the whole range.",
    )
    parser.add_argument(
        "--allow-market-hours",
        action="store_true",
        help="Permit running between 09:15 and 15:30. Off by default so the "
        "backfill cannot contend with the live 5-minute feed.",
    )
    parser.add_argument(
        "--roll-jump-pct",
        type=float,
        default=150.0,
        help="Absolute OI change%% near month end that marks a rollover stitch "
        "(default 150). Detection is data-driven because NSE moved monthly "
        "expiry from the last Thursday to the last Tuesday.",
    )
    parser.add_argument(
        "--roll-widen-bars",
        type=int,
        default=0,
        help="Also void N bars either side of each detected stitch, to exclude "
        "expiry-week roll decay (default 0 = stitch only).",
    )
    parser.add_argument("--panel-only", action="store_true", help="Rebuild the panel, fetch nothing.")
    parser.add_argument("--dry-run", action="store_true", help="Print the plan and exit.")
    parser.add_argument("--max-apps", type=int, default=8)
    parser.add_argument("--request-interval-sec", type=float, default=0.36)
    parser.add_argument("--timeout-sec", type=float, default=15.0)
    parser.add_argument("--max-retries", type=int, default=3)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    started = time.monotonic()
    DAILY_DIR.mkdir(parents=True, exist_ok=True)
    PANEL_DIR.mkdir(parents=True, exist_ok=True)

    if args.panel_only:
        panel = rebuild_panel()
        print(f"[PANEL] {panel}", flush=True)
        return 0

    floor = pd.Timestamp(args.from_date).date()
    if floor < OI_FLOOR:
        print(
            f"[PLAN][WARN] {floor} precedes the measured OI floor {OI_FLOOR}; "
            "bars before it carry price/volume but oi=0.",
            flush=True,
        )
    end = pd.Timestamp(args.to_date).date() if args.to_date else common.now_ist().date()

    universe = load_universe()
    if args.underlyings:
        wanted = {s.strip().upper() for s in args.underlyings.split(",") if s.strip()}
        universe = universe.loc[universe["underlying"].isin(wanted)].reset_index(drop=True)
        missing = wanted - set(universe["underlying"])
        if missing:
            print(f"[PLAN][WARN] not in universe: {sorted(missing)}", flush=True)
    if args.limit > 0:
        universe = universe.head(args.limit).reset_index(drop=True)
    if universe.empty:
        print("[PLAN] Nothing to do.", flush=True)
        return 0

    windows = plan_windows(floor, end)
    print(
        f"[PLAN] {len(universe)} underlyings | {floor} -> {end} | "
        f"{len(windows)} window(s)/underlying | ~{len(universe) * len(windows)} requests",
        flush=True,
    )
    if args.dry_run:
        for start_day, stop_day in windows:
            print(f"  window {start_day} -> {stop_day}", flush=True)
        return 0

    holidays = common.load_holidays()
    if market_is_open(common.now_ist(), holidays) and not args.allow_market_hours:
        print(
            "[GUARD] Market is open. This backfill shares the Kite rate limit with "
            "the live 5-minute FnO feed, so it is blocked by default. Re-run after "
            "15:30 IST, or pass --allow-market-hours to override.",
            flush=True,
        )
        common.publish_status(SESSION, "SKIPPED_MARKET_HOURS")
        return 0

    common.publish_status(
        SESSION,
        "RUNNING",
        underlyings=int(len(universe)),
        from_date=floor.isoformat(),
        to_date=end.isoformat(),
    )
    try:
        runtimes = _build_app_runtimes(args)
        outcomes = backfill_all(
            universe,
            runtimes,
            floor=floor,
            end=end,
            full_refresh=args.full_refresh,
            max_retries=args.max_retries,
            roll_jump_pct=args.roll_jump_pct,
            roll_widen_bars=args.roll_widen_bars,
        )
        panel = rebuild_panel()
        duration = time.monotonic() - started
        report = render_report(outcomes, panel, floor=floor, end=end, duration_sec=duration)
        common.atomic_write_text(REPORT_PATH, report)

        failed = sum(1 for o in outcomes if o["state"] == "FAILED")
        written = sum(1 for o in outcomes if o["state"] == "WRITTEN")
        common.publish_status(
            SESSION,
            "FAILED" if failed and not written else "SUCCESS",
            underlyings=int(len(outcomes)),
            written=written,
            failed=failed,
            panel_rows=int(panel.get("rows", 0)),
            duration_sec=round(duration, 2),
        )
        print(
            f"[DONE] written={written} failed={failed} "
            f"panel_rows={panel.get('rows', 0):,} in {duration:.1f}s",
            flush=True,
        )
        print(f"[REPORT] {REPORT_PATH}", flush=True)
        return 1 if failed and not written else 0
    except Exception as exc:
        common.publish_status(SESSION, "FAILED", error=f"{type(exc).__name__}: {exc}")
        print(f"[FATAL] {type(exc).__name__}: {exc}", file=sys.stderr, flush=True)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
