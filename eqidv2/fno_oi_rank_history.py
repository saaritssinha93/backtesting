"""Rebuild the live FnO OI rankings across the whole backfilled 5-minute store.

The live ranker (fno_oi_feature_ranker.py) computes one cross-section per
5-minute slot and keeps only that slot. This script replays the identical logic
over every slot in ``raw_contracts_5m/`` so the same gainers / losers / activity
leaderboards exist historically for backtesting.

Parity is structural, not reimplemented: ``build_contract_features`` and the
leaderboard helpers are imported from the live ranker, and the ranking maths
here reproduces ``rank_feature_snapshot`` exactly -- the only change is that
per-slot ``.loc[valid].rank()`` calls become ``groupby(...).rank()`` over all
slots at once, which is the same computation vectorised.

Two things differ from live by necessity, both recorded on every row:

**Cohort.** Live ranks one universe: the ~213 near-month contracts. The store
now holds three contract months, and ranking a far month against a front month
compares books with very different liquidity. ``--cohort month`` (the default)
therefore ranks within each contract month.

**Front-month coverage.** A contract only becomes the front month the day after
the previous month's contract expires. Derived from the store itself -- OCT was
introduced 2026-07-29, so JUL expired 2026-07-28 and AUG became the front month
on 2026-07-29. Before that date AUG was a next/far month while JUN and JUL held
the front, and those contracts are unreachable (expired instrument tokens).
Rows carry ``is_front_month`` so a backtest can restrict itself to the window
where the cross-section is genuinely live-equivalent.
"""

from __future__ import annotations

import argparse
import re
import sys
import time
from datetime import date, datetime
from pathlib import Path
from typing import Any, Iterable

import numpy as np
import pandas as pd

import fno_oi_common as common
from fno_oi_feature_ranker import (
    _leaderboards,
    _render_table,
    build_contract_features,
)


SESSION = "fno_oi_rank_history"

RANK_HISTORY_DIR = common.FNO_ROOT / "rank_history"
REPORT_PATH = common.LATEST_DIR / "latest_fno_oi_rank_history.md"

RANK_HISTORY_VERSION = "fno_oi_rank_history_v1"

# Mirrors bat/run_fno_oi_feature_ranker.bat so historical rows classify exactly
# as live rows do.
LIVE_FEATURE_OPTIONS: dict[str, Any] = {
    "min_price_move_pct": 0.10,
    "min_oi_move_pct": 0.25,
    "min_oi": 0.0,
    "min_volume_5m": 0.0,
    "min_day_volume": 0.0,
    "min_traded_value": 0.0,
    "min_active_bars": 0,
}
LIVE_TOP_N = 20

IDENTITY_COLUMNS = (
    "timestamp",
    "session_date",
    "underlying",
    "tradingsymbol",
    "instrument_token",
    "expiry",
    "contract_month",
)

# Carried through untouched so live-layout output is schema-identical to what
# fno_oi_feature_ranker.py writes.
PASSTHROUGH_COLUMNS = (
    "candle_start",
    "exchange_token",
    "days_to_expiry",
    "lot_size",
    "tick_size",
    "is_index_future",
    "prev_close",
    "price_change_15m",
    "price_change_30m",
    "price_change_60m",
    "fetch_timestamp",
    "source",
    "data_version",
    "feature_version",
)

FEATURE_COLUMNS = (
    "open",
    "high",
    "low",
    "close",
    "volume",
    "oi",
    "quality_state",
    "oi_change_5m",
    "oi_change_pct_5m",
    "oi_change_15m",
    "oi_change_pct_15m",
    "oi_change_30m",
    "oi_change_pct_30m",
    "oi_change_60m",
    "oi_change_pct_60m",
    "price_change_5m",
    "price_change_pct_5m",
    "price_change_pct_15m",
    "price_change_pct_30m",
    "price_change_pct_60m",
    "prev_day_close",
    "prev_day_close_oi",
    "session_open",
    "session_open_oi",
    "price_change_pct_day",
    "oi_change_day",
    "oi_change_pct_day",
    "price_change_pct_from_open",
    "oi_change_from_open",
    "oi_change_pct_from_open",
    "volume_15m",
    "volume_30m",
    "day_volume",
    "active_bars_session",
    "rolling_avg_volume_20",
    "volume_ratio",
    "volume_change_pct_5m",
    "volume_velocity",
    "volume_acceleration",
    "traded_value_5m",
    "oi_velocity",
    "oi_acceleration",
    "oi_zscore_20",
    "oi_reset_suspect",
    "classification",
    "classification_threshold_pass",
    "long_buildup",
    "short_buildup",
    "short_covering",
    "long_unwinding",
    "eligible_for_rank",
)

RANK_COLUMNS = (
    "oi_rank_5m",
    "oi_percentile_5m",
    "oi_rank_15m",
    "oi_percentile_15m",
    "oi_rank_30m",
    "oi_percentile_30m",
    "oi_rank_60m",
    "oi_percentile_60m",
    "oi_rank_day",
    "oi_percentile_day",
    "oi_rank_from_open",
    "oi_percentile_from_open",
    "oi_activity_percentile_5m",
    "volume_percentile",
    "price_move_percentile_5m",
    "oi_acceleration_percentile",
    "volume_acceleration_percentile",
    "oi_zscore_percentile",
    "volume_rank",
    "activity_score",
    "oi_rank_change_5m",
    "oi_rank_change_15m",
)

CONTEXT_COLUMNS = (
    "contract_rank_on_date",
    "is_front_month",
    "cohort",
    "cohort_size",
    "rank_history_version",
)


def contract_month_of(path: Path) -> str:
    match = re.search(r"(\d{2}[A-Z]{3})FUT", path.name.upper())
    return match.group(1) if match else "UNKNOWN"


def discover_contract_files(months: str, underlyings: str) -> list[Path]:
    files = sorted(p for p in (common.RAW_CONTRACT_DIR).glob("*_5minute.parquet"))
    if months:
        wanted_months = {m.strip().upper() for m in months.split(",") if m.strip()}
        files = [p for p in files if contract_month_of(p) in wanted_months]
    if underlyings:
        wanted = {u.strip().upper() for u in underlyings.split(",") if u.strip()}
        files = [p for p in files if any(p.name.upper().startswith(u) for u in wanted)]
    return files


def load_features(
    files: Iterable[Path],
    *,
    feature_options: dict[str, Any],
    progress_every: int = 50,
) -> pd.DataFrame:
    """Build live-identical features for every contract, then stack them."""

    frames: list[pd.DataFrame] = []
    files = list(files)
    total = len(files)
    keep = [c for c in IDENTITY_COLUMNS + FEATURE_COLUMNS + PASSTHROUGH_COLUMNS]
    for index, path in enumerate(files, start=1):
        try:
            raw = pd.read_parquet(path)
            features = build_contract_features(raw, **feature_options)
        except Exception as exc:
            print(f"[FEATURES][WARN] {path.name}: {type(exc).__name__}: {exc}", flush=True)
            continue
        present = [c for c in keep if c in features.columns]
        frames.append(features.loc[:, present].copy())
        if index % progress_every == 0 or index == total:
            print(f"[FEATURES] {index}/{total}", flush=True)
            common.publish_heartbeat(SESSION, "RUNNING", phase="features",
                                     progress=f"{index}/{total}")
    if not frames:
        return pd.DataFrame()
    panel = pd.concat(frames, ignore_index=True, sort=False)
    panel["timestamp"] = common._to_ist(panel["timestamp"])
    return panel


def annotate_front_month(panel: pd.DataFrame) -> pd.DataFrame:
    """Mark, per calendar date, which contract month was the front month.

    Contract *i* becomes the front month the day after contract *i-1* expires.
    The earliest contract in the store has no predecessor here, so its start is
    taken from the introduction date of contract *i+2*: a new far month is
    listed exactly when the old front month expires.
    """

    out = panel.copy()
    out["expiry"] = pd.to_datetime(out["expiry"], errors="coerce")
    months = (
        out.groupby("contract_month", as_index=False)
        .agg(expiry=("expiry", "max"), introduced=("session_date", "min"))
        .sort_values("expiry", kind="stable")
        .reset_index(drop=True)
    )

    starts: dict[str, date] = {}
    for i, row in months.iterrows():
        if i == 0:
            if len(months) >= 3:
                start = months.loc[2, "introduced"]
            else:
                start = row["introduced"]
        else:
            start = (months.loc[i - 1, "expiry"] + pd.Timedelta(days=1)).date()
        starts[row["contract_month"]] = start

    ends = {r["contract_month"]: r["expiry"].date() for _, r in months.iterrows()}
    order = {r["contract_month"]: i for i, r in months.iterrows()}

    session = pd.to_datetime(out["session_date"]).dt.date
    start_series = out["contract_month"].map(starts)
    end_series = out["contract_month"].map(ends)
    out["is_front_month"] = (session >= start_series) & (session <= end_series)

    # 1 = front, 2 = next, 3 = far, relative to each date.
    base = out["contract_month"].map(order).astype("float64")
    front_at_date = (
        out.loc[out["is_front_month"], ["session_date", "contract_month"]]
        .drop_duplicates("session_date")
        .set_index("session_date")["contract_month"]
        .map(order)
    )
    front_base = pd.to_datetime(out["session_date"]).dt.date.map(front_at_date)
    out["contract_rank_on_date"] = (base - front_base + 1).astype("Int64")
    return out


def _masked(frame: pd.DataFrame, column: str, eligible: pd.Series) -> pd.Series:
    """Values for eligible rows only; others NaN so ranking skips them.

    Live ranks the eligible subset via ``.loc[valid, col].rank()``. Ranking the
    full column with ineligible rows set to NaN is the same computation --
    pandas excludes NaN from ranking and returns NaN for those positions.
    """

    values = pd.to_numeric(frame[column], errors="coerce")
    return values.where(eligible)


def rank_history(panel: pd.DataFrame, *, cohort: str) -> pd.DataFrame:
    """Vectorised equivalent of rank_feature_snapshot over every slot."""

    if panel.empty:
        return panel

    ranked = panel.copy()
    if cohort == "month":
        keys = ["timestamp", "contract_month"]
    elif cohort == "front":
        ranked = ranked.loc[ranked["is_front_month"]].copy()
        keys = ["timestamp"]
    else:
        keys = ["timestamp"]
    ranked["cohort"] = cohort

    eligible = ranked["eligible_for_rank"].fillna(False).astype(bool)
    groups = ranked.groupby(keys, sort=False)

    def rank_desc(column: str) -> pd.Series:
        masked = _masked(ranked, column, eligible)
        return masked.groupby([ranked[k] for k in keys], sort=False).rank(
            method="min", ascending=False
        )

    def pct_asc(column: str, *, absolute: bool = False) -> pd.Series:
        masked = _masked(ranked, column, eligible)
        if absolute:
            masked = masked.abs()
        return masked.groupby([ranked[k] for k in keys], sort=False).rank(
            method="average", ascending=True, pct=True
        ).mul(100.0)

    for horizon in (5, 15, 30, 60):
        source = f"oi_change_pct_{horizon}m"
        if source not in ranked.columns:
            continue
        ranked[f"oi_rank_{horizon}m"] = rank_desc(source)
        ranked[f"oi_percentile_{horizon}m"] = pct_asc(source)

    for suffix, source in (("day", "oi_change_pct_day"),
                           ("from_open", "oi_change_pct_from_open")):
        if source not in ranked.columns:
            continue
        ranked[f"oi_rank_{suffix}"] = rank_desc(source)
        ranked[f"oi_percentile_{suffix}"] = pct_asc(source)

    ranked["oi_activity_percentile_5m"] = pct_asc("oi_change_pct_5m", absolute=True)
    ranked["volume_percentile"] = pct_asc("volume_ratio")
    ranked["price_move_percentile_5m"] = pct_asc("price_change_pct_5m", absolute=True)
    ranked["oi_acceleration_percentile"] = pct_asc("oi_acceleration", absolute=True)
    ranked["volume_acceleration_percentile"] = pct_asc("volume_acceleration", absolute=True)
    ranked["oi_zscore_percentile"] = pct_asc("oi_zscore_20", absolute=True)
    ranked["volume_rank"] = rank_desc("volume_ratio")

    score_columns = [
        "oi_activity_percentile_5m",
        "volume_percentile",
        "price_move_percentile_5m",
        "oi_acceleration_percentile",
    ]
    ranked["activity_score"] = ranked[score_columns].mean(axis=1, skipna=True)

    ranked["cohort_size"] = groups["tradingsymbol"].transform("size").astype("Int64")

    # Live reads the previous slot's stored ranks off disk; here the same
    # lookup is a self-merge on (contract, timestamp - N minutes).
    lookup = ranked.loc[:, ["tradingsymbol", "timestamp", "oi_rank_5m"]].copy()
    for minutes, output_name in ((5, "oi_rank_change_5m"), (15, "oi_rank_change_15m")):
        prior = lookup.rename(columns={"oi_rank_5m": "_prior"}).copy()
        prior["timestamp"] = prior["timestamp"] + pd.Timedelta(minutes=minutes)
        prior = prior.drop_duplicates(subset=["tradingsymbol", "timestamp"], keep="last")
        ranked = ranked.merge(prior, on=["tradingsymbol", "timestamp"], how="left")
        ranked[output_name] = ranked["_prior"] - ranked["oi_rank_5m"]
        ranked = ranked.drop(columns=["_prior"])

    ranked["rank_history_version"] = RANK_HISTORY_VERSION
    return ranked.sort_values(
        ["timestamp", "contract_month", "eligible_for_rank", "oi_rank_5m", "tradingsymbol"],
        ascending=[True, True, False, True, True],
        na_position="last",
    ).reset_index(drop=True)


def build_leaderboards(ranked: pd.DataFrame, top_n: int) -> pd.DataFrame:
    """Top-N gainers / losers / activity per slot, using the live selection."""

    boards: list[pd.DataFrame] = []
    group_keys = ["timestamp", "contract_month"] if "contract_month" in ranked.columns else ["timestamp"]
    for keys, chunk in ranked.groupby(group_keys, sort=True):
        gainers, losers, activity = _leaderboards(chunk, top_n)
        for name, frame in (("gainers", gainers), ("losers", losers), ("activity", activity)):
            if frame.empty:
                continue
            piece = frame.copy()
            piece["board"] = name
            piece["board_rank"] = range(1, len(piece) + 1)
            boards.append(piece)
    if not boards:
        return pd.DataFrame()
    return pd.concat(boards, ignore_index=True, sort=False)


def persist(ranked: pd.DataFrame, leaderboards: pd.DataFrame) -> dict[str, Any]:
    RANK_HISTORY_DIR.mkdir(parents=True, exist_ok=True)
    written_days = 0
    rows_written = 0
    ranked = ranked.copy()
    ranked["session_date"] = pd.to_datetime(ranked["session_date"]).dt.date
    if not leaderboards.empty:
        leaderboards = leaderboards.copy()
        leaderboards["session_date"] = pd.to_datetime(leaderboards["session_date"]).dt.date

    for day, chunk in ranked.groupby("session_date", sort=True):
        day_dir = RANK_HISTORY_DIR / day.isoformat()
        day_dir.mkdir(parents=True, exist_ok=True)
        common.atomic_write_parquet(
            chunk.reset_index(drop=True), day_dir / f"rankings_{day.isoformat()}.parquet"
        )
        if not leaderboards.empty:
            board_chunk = leaderboards.loc[leaderboards["session_date"].eq(day)]
            if not board_chunk.empty:
                common.atomic_write_parquet(
                    board_chunk.reset_index(drop=True),
                    day_dir / f"leaderboard_{day.isoformat()}.parquet",
                )
        written_days += 1
        rows_written += int(len(chunk))
    return {"days": written_days, "rows": rows_written}


def _live_schema(sample_dir: Path, pattern: str) -> list[str] | None:
    """Column order of an existing live file, so backfilled slots match it."""

    for path in sorted(sample_dir.glob(pattern)):
        try:
            return list(pd.read_parquet(path).columns)
        except Exception:
            continue
    return None


def emit_live_layout(
    ranked: pd.DataFrame,
    *,
    top_n: int,
    overwrite: bool,
) -> dict[str, Any]:
    """Write per-slot files in the exact layout fno_oi_feature_ranker.py uses.

    Only front-month rows are emitted: live ranks the near-month universe, so a
    slot filled from a next/far month would not be a faithful replacement.
    Existing files are left alone unless ``overwrite`` is set -- genuine live
    output is the source of truth and this only fills the gaps around it.
    """

    front = ranked.loc[ranked["is_front_month"].fillna(False)].copy()
    if front.empty:
        return {"written": 0, "skipped": 0, "days": 0, "reason": "no front-month rows"}

    front["session_date"] = pd.to_datetime(front["session_date"]).dt.date
    written = 0
    skipped = 0
    days: set[date] = set()

    for day, day_rows in front.groupby("session_date", sort=True):
        rank_dir = common.RANKING_DIR / day.isoformat()
        snap_dir = common.FEATURE_SNAPSHOT_DIR / day.isoformat()
        rank_dir.mkdir(parents=True, exist_ok=True)
        snap_dir.mkdir(parents=True, exist_ok=True)

        rank_schema = _live_schema(rank_dir, "fno_oi_rankings_*.parquet")
        snap_schema = _live_schema(snap_dir, "fno_oi_features_*.parquet")

        for slot, chunk in day_rows.groupby("timestamp", sort=True):
            hhmm = pd.Timestamp(slot).strftime("%H%M")
            ranking_path = rank_dir / f"fno_oi_rankings_{hhmm}.parquet"
            if ranking_path.exists() and not overwrite:
                skipped += 1
                continue

            chunk = chunk.sort_values(
                ["eligible_for_rank", "oi_rank_5m", "tradingsymbol"],
                ascending=[False, True, True],
                na_position="last",
            ).reset_index(drop=True)

            ranking_out = (
                chunk.reindex(columns=rank_schema) if rank_schema else chunk
            )
            common.atomic_write_parquet(ranking_out, ranking_path)

            gainers, losers, activity = _leaderboards(chunk, top_n)
            for name, frame in (
                ("oi_gainers", gainers),
                ("oi_losers", losers),
                ("oi_activity", activity),
            ):
                out = frame.reindex(columns=rank_schema) if rank_schema else frame
                common.atomic_write_csv(out, rank_dir / f"{name}_{hhmm}.csv")

            snapshot = chunk.reindex(columns=snap_schema) if snap_schema else chunk
            common.atomic_write_parquet(
                snapshot, snap_dir / f"fno_oi_features_{hhmm}.parquet"
            )
            written += 1
            days.add(day)

    return {"written": written, "skipped": skipped, "days": len(days)}


def render_report(
    ranked: pd.DataFrame,
    leaderboards: pd.DataFrame,
    stats: dict[str, Any],
    *,
    cohort: str,
    top_n: int,
    duration_sec: float,
) -> str:
    days = sorted(pd.to_datetime(ranked["session_date"]).dt.date.unique())
    front = ranked.loc[ranked["is_front_month"]]
    front_days = sorted(pd.to_datetime(front["session_date"]).dt.date.unique()) if not front.empty else []
    eligible = int(ranked["eligible_for_rank"].fillna(False).sum())

    lines = [
        "# FnO OI Rank History",
        "",
        f"- Generated: {common.now_ist().isoformat(timespec='seconds')}",
        f"- Cohort: {cohort}",
        f"- Top-N per board: {top_n}",
        f"- Contracts: {ranked['tradingsymbol'].nunique()}",
        f"- Slots: {ranked['timestamp'].nunique():,}",
        f"- Rows: {len(ranked):,} ({eligible:,} eligible for rank)",
        f"- Trading days: {len(days)} ({days[0]} -> {days[-1]})" if days else "- Trading days: 0",
        f"- Duration: {duration_sec:.1f}s",
        "",
        "## Front-month coverage",
        "",
        "A cross-section only matches live where the ranked contract was the "
        "front month. Earlier rows rank a next/far month, because the true "
        "front months of that period have expired and their instrument tokens "
        "are unreachable.",
        "",
    ]
    if front_days:
        lines += [
            f"- Live-equivalent window: **{front_days[0]} -> {front_days[-1]}** "
            f"({len(front_days)} trading days, {len(front):,} rows)",
            f"- Non-front rows: {len(ranked) - len(front):,} "
            "(usable as a far-month cross-section, not as a live replica)",
            "",
        ]
    else:
        lines += ["- No front-month rows in range.", ""]

    by_month = (
        ranked.groupby("contract_month")
        .agg(rows=("tradingsymbol", "size"),
             contracts=("tradingsymbol", "nunique"),
             eligible=("eligible_for_rank", "sum"))
        .reset_index()
    )
    lines += ["## Rows by contract month", "",
              "| Month | Contracts | Rows | Eligible |", "| --- | ---: | ---: | ---: |"]
    for _, row in by_month.iterrows():
        lines.append(
            f"| {row['contract_month']} | {int(row['contracts'])} | "
            f"{int(row['rows']):,} | {int(row['eligible']):,} |"
        )

    classes = ranked.loc[ranked["eligible_for_rank"].fillna(False), "classification"]
    if not classes.empty:
        lines += ["", "## Classification mix (eligible rows)", "",
                  "| Classification | Rows | Share |", "| --- | ---: | ---: |"]
        counts = classes.value_counts()
        for name, count in counts.items():
            lines.append(f"| {name} | {int(count):,} | {count / len(classes):.1%} |")

    if not leaderboards.empty:
        lines += ["", "## Leaderboard rows", "", "| Board | Rows |", "| --- | ---: |"]
        for name, count in leaderboards["board"].value_counts().items():
            lines.append(f"| {name} | {int(count):,} |")

        latest_slot = leaderboards["timestamp"].max()
        sample = leaderboards.loc[
            leaderboards["timestamp"].eq(latest_slot) & leaderboards["board"].eq("gainers")
        ].head(10)
        if not sample.empty:
            lines += ["", f"## Sample: OI gainers at {latest_slot}", ""]
            lines += _render_table(sample)

    live_stats = stats.get("live_layout")
    if live_stats:
        lines += [
            "", "## Live-layout emission", "",
            f"- Slots written into rankings/<date>/: {live_stats.get('written', 0)}",
            f"- Existing live slots left untouched: {live_stats.get('skipped', 0)}",
            f"- Days touched: {live_stats.get('days', 0)}",
        ]

    lines += ["", "## Output", "",
              f"- Days written: {stats.get('days', 0)}",
              f"- Rows written: {stats.get('rows', 0):,}",
              f"- Path: `{RANK_HISTORY_DIR}\\<YYYY-MM-DD>\\rankings_<YYYY-MM-DD>.parquet`",
              f"- Boards: `{RANK_HISTORY_DIR}\\<YYYY-MM-DD>\\leaderboard_<YYYY-MM-DD>.parquet`",
              ""]
    return "\n".join(lines)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--cohort",
        choices=("month", "all", "front"),
        default="month",
        help="Cross-section to rank within. 'month' (default) ranks each "
        "contract month separately; 'front' keeps only front-month rows for "
        "strict live parity; 'all' pools every contract per slot.",
    )
    parser.add_argument("--months", default="", help="Comma-separated contract months, e.g. 26AUG,26SEP.")
    parser.add_argument("--underlyings", default="", help="Comma-separated underlyings.")
    parser.add_argument("--from-date", default="", help="Keep slots on/after this date.")
    parser.add_argument("--to-date", default="", help="Keep slots on/before this date.")
    parser.add_argument("--top-n", type=int, default=LIVE_TOP_N)
    parser.add_argument("--limit", type=int, default=0, help="Process only the first N contracts.")
    parser.add_argument("--no-leaderboards", action="store_true", help="Skip leaderboard extraction.")
    parser.add_argument(
        "--emit-live-layout",
        action="store_true",
        help="Also write per-slot files into rankings/<date>/ and "
        "feature_snapshots/<date>/ in the live ranker's exact layout, filling "
        "slots the live pipeline missed. Front-month rows only.",
    )
    parser.add_argument(
        "--overwrite-live",
        action="store_true",
        help="With --emit-live-layout, replace existing live files instead of "
        "skipping them. Off by default: genuine live output is the source of truth.",
    )
    parser.add_argument("--dry-run", action="store_true", help="Print the plan and exit.")
    parser.add_argument("--min-price-move-pct", type=float, default=LIVE_FEATURE_OPTIONS["min_price_move_pct"])
    parser.add_argument("--min-oi-move-pct", type=float, default=LIVE_FEATURE_OPTIONS["min_oi_move_pct"])
    parser.add_argument("--min-oi", type=float, default=LIVE_FEATURE_OPTIONS["min_oi"])
    parser.add_argument("--min-volume-5m", type=float, default=LIVE_FEATURE_OPTIONS["min_volume_5m"])
    parser.add_argument("--min-day-volume", type=float, default=LIVE_FEATURE_OPTIONS["min_day_volume"])
    parser.add_argument("--min-traded-value", type=float, default=LIVE_FEATURE_OPTIONS["min_traded_value"])
    parser.add_argument("--min-active-bars", type=int, default=LIVE_FEATURE_OPTIONS["min_active_bars"])
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    started = time.monotonic()

    files = discover_contract_files(args.months, args.underlyings)
    if args.limit > 0:
        files = files[: args.limit]
    if not files:
        print("[PLAN] No contract files matched.", flush=True)
        return 0

    print(f"[PLAN] {len(files)} contracts | cohort={args.cohort} | top_n={args.top_n}", flush=True)
    if args.dry_run:
        for path in files[:10]:
            print(f"   {path.name}", flush=True)
        if len(files) > 10:
            print(f"   ... +{len(files) - 10} more", flush=True)
        return 0

    feature_options = {
        "min_price_move_pct": args.min_price_move_pct,
        "min_oi_move_pct": args.min_oi_move_pct,
        "min_oi": args.min_oi,
        "min_volume_5m": args.min_volume_5m,
        "min_day_volume": args.min_day_volume,
        "min_traded_value": args.min_traded_value,
        "min_active_bars": args.min_active_bars,
    }

    common.publish_status(SESSION, "RUNNING", contracts=len(files), cohort=args.cohort)
    try:
        panel = load_features(files, feature_options=feature_options)
        if panel.empty:
            print("[DONE] No features built.", flush=True)
            common.publish_status(SESSION, "FAILED", error="no features")
            return 1

        if args.from_date:
            floor = pd.Timestamp(args.from_date).date()
            panel = panel.loc[pd.to_datetime(panel["session_date"]).dt.date >= floor]
        if args.to_date:
            ceil = pd.Timestamp(args.to_date).date()
            panel = panel.loc[pd.to_datetime(panel["session_date"]).dt.date <= ceil]
        if panel.empty:
            print("[DONE] Date filter removed every row.", flush=True)
            return 0

        print(f"[FEATURES] {len(panel):,} rows across {panel['timestamp'].nunique():,} slots", flush=True)
        panel = annotate_front_month(panel)

        print("[RANK] ranking cross-sections...", flush=True)
        common.publish_heartbeat(SESSION, "RUNNING", phase="rank")
        ranked = rank_history(panel, cohort=args.cohort)

        leaderboards = pd.DataFrame()
        if not args.no_leaderboards:
            print("[RANK] extracting leaderboards...", flush=True)
            common.publish_heartbeat(SESSION, "RUNNING", phase="leaderboards")
            leaderboards = build_leaderboards(ranked, args.top_n)

        print("[WRITE] persisting per-day partitions...", flush=True)
        stats = persist(ranked, leaderboards)

        if args.emit_live_layout:
            print("[WRITE] emitting live-layout per-slot files...", flush=True)
            common.publish_heartbeat(SESSION, "RUNNING", phase="live_layout")
            live_stats = emit_live_layout(
                ranked, top_n=args.top_n, overwrite=args.overwrite_live
            )
            stats["live_layout"] = live_stats
            print(
                f"[LIVE-LAYOUT] wrote {live_stats.get('written', 0)} slots, "
                f"skipped {live_stats.get('skipped', 0)} existing, "
                f"across {live_stats.get('days', 0)} day(s)",
                flush=True,
            )

        duration = time.monotonic() - started
        report = render_report(
            ranked, leaderboards, stats,
            cohort=args.cohort, top_n=args.top_n, duration_sec=duration,
        )
        common.atomic_write_text(REPORT_PATH, report)
        common.publish_status(
            SESSION,
            "SUCCESS",
            contracts=int(ranked["tradingsymbol"].nunique()),
            slots=int(ranked["timestamp"].nunique()),
            rows=int(len(ranked)),
            days=int(stats.get("days", 0)),
            duration_sec=round(duration, 2),
        )
        print(
            f"[DONE] {len(ranked):,} ranked rows | {ranked['timestamp'].nunique():,} slots | "
            f"{stats.get('days', 0)} days in {duration:.1f}s",
            flush=True,
        )
        print(f"[REPORT] {REPORT_PATH}", flush=True)
        return 0
    except Exception as exc:
        common.publish_status(SESSION, "FAILED", error=f"{type(exc).__name__}: {exc}")
        print(f"[FATAL] {type(exc).__name__}: {exc}", file=sys.stderr, flush=True)
        raise


if __name__ == "__main__":
    raise SystemExit(main())
