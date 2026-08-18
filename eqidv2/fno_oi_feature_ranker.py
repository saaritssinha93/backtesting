from __future__ import annotations

import argparse
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, time as dtime, timedelta
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

import fno_oi_common as common


SESSION = "fno_oi_feature_ranker"
FIRST_SLOT = dtime(9, 20)
LAST_SLOT = dtime(15, 30)


def _to_ist(values: pd.Series) -> pd.Series:
    return common._to_ist(values)


def _exact_lag(values: pd.Series, timestamps: pd.Series, minutes: int) -> pd.Series:
    lookup = pd.Series(values.to_numpy(), index=pd.DatetimeIndex(timestamps))
    lookup = lookup.loc[~lookup.index.duplicated(keep="last")]
    targets = pd.DatetimeIndex(timestamps) - pd.Timedelta(minutes=minutes)
    return pd.Series(lookup.reindex(targets).to_numpy(), index=values.index, dtype="float64")


def _pct_change(current: pd.Series, baseline: pd.Series) -> pd.Series:
    current_num = pd.to_numeric(current, errors="coerce")
    baseline_num = pd.to_numeric(baseline, errors="coerce")
    return current_num.div(baseline_num.where(baseline_num.gt(0))).sub(1.0).mul(100.0)


def build_contract_features(
    raw: pd.DataFrame,
    *,
    min_price_move_pct: float = 0.10,
    min_oi_move_pct: float = 0.25,
    min_oi: float = 0.0,
    min_volume_5m: float = 0.0,
    min_day_volume: float = 0.0,
    min_traded_value: float = 0.0,
    min_active_bars: int = 0,
) -> pd.DataFrame:
    required = {
        "timestamp",
        "underlying",
        "tradingsymbol",
        "instrument_token",
        "expiry",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "oi",
        "quality_state",
    }
    missing = required - set(raw.columns)
    if missing:
        raise ValueError(f"Raw FnO contract data missing columns: {sorted(missing)}")
    out = raw.copy()
    out["timestamp"] = _to_ist(out["timestamp"])
    out = (
        out.dropna(subset=["timestamp"])
        .drop_duplicates(subset=["tradingsymbol", "timestamp"], keep="last")
        .sort_values("timestamp", kind="stable")
        .reset_index(drop=True)
    )
    for column in ("open", "high", "low", "close", "volume", "oi"):
        out[column] = pd.to_numeric(out[column], errors="coerce")
    out["expiry"] = pd.to_datetime(out["expiry"], errors="coerce").dt.normalize()
    if "contract_month" not in out.columns:
        out["contract_month"] = out["expiry"].dt.strftime("%Y-%m")
    out["session_date"] = out["timestamp"].dt.date

    for horizon in (5, 15, 30, 60):
        prior_oi = _exact_lag(out["oi"], out["timestamp"], horizon)
        prior_close = _exact_lag(out["close"], out["timestamp"], horizon)
        out[f"oi_change_{horizon}m"] = out["oi"] - prior_oi
        out[f"oi_change_pct_{horizon}m"] = _pct_change(out["oi"], prior_oi)
        out[f"price_change_{horizon}m"] = out["close"] - prior_close
        out[f"price_change_pct_{horizon}m"] = _pct_change(out["close"], prior_close)

    sessions = (
        out.groupby("session_date", sort=True, observed=True)
        .agg(day_close=("close", "last"), day_close_oi=("oi", "last"))
        .reset_index()
    )
    sessions["prev_day_close"] = sessions["day_close"].shift(1)
    sessions["prev_day_close_oi"] = sessions["day_close_oi"].shift(1)
    prev_close_map = sessions.set_index("session_date")["prev_day_close"]
    prev_oi_map = sessions.set_index("session_date")["prev_day_close_oi"]
    out["prev_day_close"] = out["session_date"].map(prev_close_map)
    out["prev_close"] = out["prev_day_close"]
    out["prev_day_close_oi"] = out["session_date"].map(prev_oi_map)
    out["session_open"] = out.groupby("session_date", sort=False)["open"].transform("first")
    out["session_open_oi"] = out.groupby("session_date", sort=False)["oi"].transform("first")
    out["price_change_pct_day"] = _pct_change(out["close"], out["prev_day_close"])
    out["oi_change_day"] = out["oi"] - out["prev_day_close_oi"]
    out["oi_change_pct_day"] = _pct_change(out["oi"], out["prev_day_close_oi"])
    out["price_change_pct_from_open"] = _pct_change(out["close"], out["session_open"])
    out["oi_change_from_open"] = out["oi"] - out["session_open_oi"]
    out["oi_change_pct_from_open"] = _pct_change(out["oi"], out["session_open_oi"])

    grouped_volume = out.groupby("session_date", sort=False)["volume"]
    out["volume_15m"] = grouped_volume.rolling(3, min_periods=3).sum().reset_index(level=0, drop=True)
    out["volume_30m"] = grouped_volume.rolling(6, min_periods=6).sum().reset_index(level=0, drop=True)
    out["day_volume"] = grouped_volume.cumsum()
    out["active_bars_session"] = out.groupby("session_date", sort=False).cumcount() + 1
    prior_volume_mean = out["volume"].shift(1).rolling(20, min_periods=5).mean()
    out["rolling_avg_volume_20"] = prior_volume_mean
    out["volume_ratio"] = out["volume"].div(prior_volume_mean.where(prior_volume_mean.gt(0)))
    prior_volume = _exact_lag(out["volume"], out["timestamp"], 5)
    out["volume_change_pct_5m"] = _pct_change(out["volume"], prior_volume)
    out["volume_velocity"] = out["volume_change_pct_5m"]
    out["volume_acceleration"] = out["volume_velocity"] - _exact_lag(
        out["volume_velocity"], out["timestamp"], 5
    )
    out["traded_value_5m"] = out["close"] * out["volume"]

    out["oi_velocity"] = out["oi_change_pct_5m"]
    prior_velocity = _exact_lag(out["oi_velocity"], out["timestamp"], 5)
    out["oi_acceleration"] = out["oi_velocity"] - prior_velocity
    prior_oi_mean = out["oi_change_pct_5m"].shift(1).rolling(20, min_periods=5).mean()
    prior_oi_std = out["oi_change_pct_5m"].shift(1).rolling(20, min_periods=5).std(ddof=0)
    out["oi_zscore_20"] = (out["oi_change_pct_5m"] - prior_oi_mean).div(
        prior_oi_std.where(prior_oi_std.gt(0))
    )
    out["oi_reset_suspect"] = out["oi_change_pct_5m"].le(-80.0)

    price_up = out["price_change_pct_5m"].gt(0)
    price_down = out["price_change_pct_5m"].lt(0)
    oi_up = out["oi_change_pct_5m"].gt(0)
    oi_down = out["oi_change_pct_5m"].lt(0)
    material_move = (
        out["price_change_pct_5m"].abs().ge(max(0.0, float(min_price_move_pct)))
        & out["oi_change_pct_5m"].abs().ge(max(0.0, float(min_oi_move_pct)))
    )
    out["classification_threshold_pass"] = material_move
    out["classification"] = np.select(
        [
            material_move & price_up & oi_up,
            material_move & price_down & oi_up,
            material_move & price_up & oi_down,
            material_move & price_down & oi_down,
        ],
        ["LONG_BUILDUP", "SHORT_BUILDUP", "SHORT_COVERING", "LONG_UNWINDING"],
        default="NEUTRAL",
    )
    out["long_buildup"] = out["classification"].eq("LONG_BUILDUP")
    out["short_buildup"] = out["classification"].eq("SHORT_BUILDUP")
    out["short_covering"] = out["classification"].eq("SHORT_COVERING")
    out["long_unwinding"] = out["classification"].eq("LONG_UNWINDING")
    out["eligible_for_rank"] = (
        out["quality_state"].eq("VALID")
        & out["oi"].gt(0)
        & out["oi_change_pct_5m"].notna()
        & out["price_change_pct_5m"].notna()
        & out["oi"].ge(max(0.0, float(min_oi)))
        & out["volume"].ge(max(0.0, float(min_volume_5m)))
        & out["day_volume"].ge(max(0.0, float(min_day_volume)))
        & out["traded_value_5m"].ge(max(0.0, float(min_traded_value)))
        & out["active_bars_session"].ge(max(0, int(min_active_bars)))
    )
    out["feature_version"] = common.FEATURE_DATA_VERSION
    return out


def _append_feature_row(path: Path, row: pd.DataFrame) -> None:
    existing = pd.read_parquet(path) if path.exists() else pd.DataFrame(columns=row.columns)
    combined = (
        row.copy()
        if existing.empty
        else pd.concat([existing, row], ignore_index=True, sort=False)
    )
    combined["timestamp"] = _to_ist(combined["timestamp"])
    combined = (
        combined.drop_duplicates(subset=["tradingsymbol", "timestamp"], keep="last")
        .sort_values("timestamp", kind="stable")
        .reset_index(drop=True)
    )
    common.atomic_write_parquet(combined, path)


def _contract_feature_for_slot(
    contract: pd.Series,
    slot_end: datetime,
    feature_options: dict[str, Any],
) -> tuple[dict[str, Any] | None, str]:
    symbol = str(contract["tradingsymbol"])
    path = common.raw_contract_path(symbol)
    if not path.exists():
        return None, "raw_file_missing"
    try:
        raw = pd.read_parquet(path)
        features = build_contract_features(raw, **feature_options)
        target = pd.Timestamp(slot_end)
        current = features.loc[features["timestamp"].eq(target)].tail(1)
        if current.empty:
            return None, "slot_missing"
        _append_feature_row(common.feature_contract_path(symbol), current)
        return current.iloc[0].to_dict(), "ok"
    except Exception as exc:
        return None, f"{type(exc).__name__}: {exc}"


def build_feature_snapshot(
    universe: pd.DataFrame,
    slot_end: datetime,
    *,
    workers: int = 8,
    feature_options: dict[str, Any] | None = None,
) -> tuple[pd.DataFrame, list[dict[str, str]]]:
    rows: list[dict[str, Any]] = []
    failures: list[dict[str, str]] = []
    max_workers = max(1, min(int(workers), len(universe), 32))
    with ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="fno-feature") as pool:
        futures = {
            pool.submit(
                _contract_feature_for_slot,
                contract,
                slot_end,
                dict(feature_options or {}),
            ): str(contract["tradingsymbol"])
            for _, contract in universe.iterrows()
        }
        for future in as_completed(futures):
            symbol = futures[future]
            row, state = future.result()
            if row is None:
                failures.append({"tradingsymbol": symbol, "reason": state})
            else:
                rows.append(row)
    snapshot = pd.DataFrame(rows)
    if not snapshot.empty:
        snapshot["timestamp"] = _to_ist(snapshot["timestamp"])
        snapshot = snapshot.sort_values("tradingsymbol").reset_index(drop=True)
    return snapshot, failures


def _rank_percentile(values: pd.Series) -> pd.Series:
    numeric = pd.to_numeric(values, errors="coerce")
    return numeric.rank(method="average", ascending=True, pct=True).mul(100.0)


def _load_prior_ranks(slot_end: datetime, minutes: int) -> pd.DataFrame:
    prior = slot_end - timedelta(minutes=minutes)
    path = ranking_snapshot_path(prior)
    if not path.exists():
        return pd.DataFrame(columns=["tradingsymbol", "oi_rank_5m"])
    try:
        return pd.read_parquet(path, columns=["tradingsymbol", "oi_rank_5m"])
    except Exception:
        return pd.DataFrame(columns=["tradingsymbol", "oi_rank_5m"])


def rank_feature_snapshot(snapshot: pd.DataFrame, slot_end: datetime) -> pd.DataFrame:
    if snapshot.empty:
        return snapshot.copy()
    ranked = snapshot.copy()
    eligible = ranked["eligible_for_rank"].fillna(False).astype(bool)
    for horizon in (5, 15, 30, 60):
        source = f"oi_change_pct_{horizon}m"
        if source not in ranked.columns:
            continue
        valid = eligible & pd.to_numeric(ranked[source], errors="coerce").notna()
        ranked.loc[valid, f"oi_rank_{horizon}m"] = ranked.loc[valid, source].rank(
            method="min", ascending=False
        )
        ranked.loc[valid, f"oi_percentile_{horizon}m"] = _rank_percentile(
            ranked.loc[valid, source]
        )
    for suffix, source in (
        ("day", "oi_change_pct_day"),
        ("from_open", "oi_change_pct_from_open"),
    ):
        valid = eligible & pd.to_numeric(ranked[source], errors="coerce").notna()
        ranked.loc[valid, f"oi_rank_{suffix}"] = ranked.loc[valid, source].rank(
            method="min", ascending=False
        )
        ranked.loc[valid, f"oi_percentile_{suffix}"] = _rank_percentile(
            ranked.loc[valid, source]
        )

    ranked.loc[eligible, "oi_activity_percentile_5m"] = _rank_percentile(
        ranked.loc[eligible, "oi_change_pct_5m"].abs()
    )
    ranked.loc[eligible, "volume_percentile"] = _rank_percentile(
        ranked.loc[eligible, "volume_ratio"]
    )
    ranked.loc[eligible, "price_move_percentile_5m"] = _rank_percentile(
        ranked.loc[eligible, "price_change_pct_5m"].abs()
    )
    ranked.loc[eligible, "oi_acceleration_percentile"] = _rank_percentile(
        ranked.loc[eligible, "oi_acceleration"].abs()
    )
    ranked.loc[eligible, "volume_acceleration_percentile"] = _rank_percentile(
        ranked.loc[eligible, "volume_acceleration"].abs()
    )
    ranked.loc[eligible, "oi_zscore_percentile"] = _rank_percentile(
        ranked.loc[eligible, "oi_zscore_20"].abs()
    )
    ranked.loc[eligible, "volume_rank"] = ranked.loc[eligible, "volume_ratio"].rank(
        method="min", ascending=False
    )
    score_columns = [
        "oi_activity_percentile_5m",
        "volume_percentile",
        "price_move_percentile_5m",
        "oi_acceleration_percentile",
    ]
    ranked["activity_score"] = ranked[score_columns].mean(axis=1, skipna=True)

    for minutes, output_name in ((5, "oi_rank_change_5m"), (15, "oi_rank_change_15m")):
        prior = _load_prior_ranks(slot_end, minutes).rename(
            columns={"oi_rank_5m": f"_prior_rank_{minutes}m"}
        )
        ranked = ranked.merge(prior, on="tradingsymbol", how="left", validate="one_to_one")
        ranked[output_name] = ranked[f"_prior_rank_{minutes}m"] - ranked["oi_rank_5m"]
        ranked = ranked.drop(columns=[f"_prior_rank_{minutes}m"])
    return ranked.sort_values(
        ["eligible_for_rank", "oi_rank_5m", "tradingsymbol"],
        ascending=[False, True, True],
        na_position="last",
    ).reset_index(drop=True)


def ranking_snapshot_path(slot_end: datetime) -> Path:
    day_dir = common.RANKING_DIR / slot_end.date().isoformat()
    return day_dir / f"fno_oi_rankings_{slot_end.strftime('%H%M')}.parquet"


def feature_snapshot_path(slot_end: datetime) -> Path:
    day_dir = common.FEATURE_SNAPSHOT_DIR / slot_end.date().isoformat()
    return day_dir / f"fno_oi_features_{slot_end.strftime('%H%M')}.parquet"


def _leaderboards(ranked: pd.DataFrame, top_n: int) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    eligible = ranked.loc[ranked["eligible_for_rank"].fillna(False)].copy()
    gainers = eligible.loc[eligible["oi_change_pct_5m"].gt(0)].nlargest(top_n, "oi_change_pct_5m")
    losers = eligible.loc[eligible["oi_change_pct_5m"].lt(0)].nsmallest(top_n, "oi_change_pct_5m")
    activity = eligible.nlargest(top_n, "activity_score")
    return gainers, losers, activity


def _fmt_number(value: Any, decimals: int = 2) -> str:
    number = pd.to_numeric(value, errors="coerce")
    return "n/a" if pd.isna(number) else f"{float(number):.{decimals}f}"


def _render_table(rows: pd.DataFrame) -> list[str]:
    lines = [
        "Rank | Underlying | Contract | Price 5m % | OI 5m % | Volume ratio | Class",
        "---: | --- | --- | ---: | ---: | ---: | ---",
    ]
    for index, row in enumerate(rows.itertuples(index=False), start=1):
        lines.append(
            f"{index} | {row.underlying} | {row.tradingsymbol} | "
            f"{_fmt_number(row.price_change_pct_5m)} | {_fmt_number(row.oi_change_pct_5m)} | "
            f"{_fmt_number(row.volume_ratio)} | {row.classification}"
        )
    if rows.empty:
        lines.append("- | No eligible contracts | - | - | - | - | -")
    return lines


def render_leaderboard_report(
    slot_end: datetime,
    fetch_marker: dict[str, Any],
    ranked: pd.DataFrame,
    gainers: pd.DataFrame,
    losers: pd.DataFrame,
    activity: pd.DataFrame,
    failures: list[dict[str, str]],
) -> str:
    eligible = int(ranked["eligible_for_rank"].fillna(False).sum()) if not ranked.empty else 0
    lines = [
        "# FnO OI Gainers and Losers",
        "",
        f"Completed slot: {slot_end.isoformat()}",
        f"Fetch coverage: {float(fetch_marker.get('coverage_ratio', 0.0)):.1%}",
        f"Contracts fetched: {fetch_marker.get('contracts_written', 0)}/{fetch_marker.get('contracts_expected', 0)}",
        f"Contracts eligible for ranking: {eligible}",
        f"Feature failures/missing slots: {len(failures)}",
        "Timestamp convention: completed candle end time.",
        "",
        "## OI Gainers",
        "",
        *_render_table(gainers),
        "",
        "## OI Losers",
        "",
        *_render_table(losers),
        "",
        "## Highest Activity",
        "",
        *_render_table(activity),
    ]
    return "\n".join(lines) + "\n"


def persist_rankings(
    slot_end: datetime,
    fetch_marker: dict[str, Any],
    features: pd.DataFrame,
    ranked: pd.DataFrame,
    failures: list[dict[str, str]],
    *,
    top_n: int,
) -> dict[str, Any]:
    common.atomic_write_parquet(features, feature_snapshot_path(slot_end))
    ranking_path = ranking_snapshot_path(slot_end)
    common.atomic_write_parquet(ranked, ranking_path)
    gainers, losers, activity = _leaderboards(ranked, top_n)
    day_dir = common.RANKING_DIR / slot_end.date().isoformat()
    common.atomic_write_csv(gainers, day_dir / f"oi_gainers_{slot_end.strftime('%H%M')}.csv")
    common.atomic_write_csv(losers, day_dir / f"oi_losers_{slot_end.strftime('%H%M')}.csv")
    common.atomic_write_csv(activity, day_dir / f"oi_activity_{slot_end.strftime('%H%M')}.csv")
    common.atomic_write_csv(ranked, common.LATEST_DIR / "latest_fno_oi_rankings.csv")
    common.atomic_write_csv(activity, common.LATEST_DIR / "latest_fno_oi_candidates_shadow.csv")
    report = render_leaderboard_report(
        slot_end,
        fetch_marker,
        ranked,
        gainers,
        losers,
        activity,
        failures,
    )
    report_path = common.LATEST_DIR / "latest_fno_oi_leaderboard.md"
    common.atomic_write_text(report_path, report)
    eligible = int(ranked["eligible_for_rank"].fillna(False).sum()) if not ranked.empty else 0
    return {
        "ranking_path": str(ranking_path),
        "report_path": str(report_path),
        "eligible_contracts": eligible,
        "gainers": int(len(gainers)),
        "losers": int(len(losers)),
        "activity_candidates": int(len(activity)),
    }


def process_slot(
    marker_path: Path,
    universe: pd.DataFrame,
    args: argparse.Namespace,
) -> dict[str, Any]:
    started = time.monotonic()
    fetch_marker = common.read_json(marker_path)
    slot_end = pd.Timestamp(fetch_marker["slot_ist"])
    if slot_end.tzinfo is None:
        slot_end = slot_end.tz_localize(common.IST)
    else:
        slot_end = slot_end.tz_convert(common.IST)
    slot_dt = slot_end.to_pydatetime()
    if str(fetch_marker.get("source", "")).lower() != "final":
        raise ValueError(f"Fetch marker is not final: {marker_path.name}")
    if not bool(fetch_marker.get("complete")):
        raise ValueError(
            f"Fetch marker is incomplete: coverage={fetch_marker.get('coverage_ratio')} "
            f"failed={fetch_marker.get('failed_count')}"
        )

    common.publish_status(
        SESSION,
        "RUNNING",
        phase="CALCULATE",
        slot=slot_dt.isoformat(),
        fetch_marker=marker_path.name,
    )
    features, failures = build_feature_snapshot(
        universe,
        slot_dt,
        workers=args.workers,
        feature_options={
            "min_price_move_pct": args.min_price_move_pct,
            "min_oi_move_pct": args.min_oi_move_pct,
            "min_oi": args.min_oi,
            "min_volume_5m": args.min_volume_5m,
            "min_day_volume": args.min_day_volume,
            "min_traded_value": args.min_traded_value,
            "min_active_bars": args.min_active_bars,
        },
    )
    if features.empty:
        raise RuntimeError(f"No contract features were available for {slot_dt.isoformat()}.")
    ranked = rank_feature_snapshot(features, slot_dt)
    outputs = persist_rankings(
        slot_dt,
        fetch_marker,
        features,
        ranked,
        failures,
        top_n=args.top_n,
    )
    complete = outputs["eligible_contracts"] >= math_coverage_floor(
        int(fetch_marker.get("contracts_expected", len(universe))),
        float(args.min_rank_coverage),
    )
    state = "SUCCESS" if complete else "PARTIAL"
    marker = {
        "schema_version": "fno_oi_ranking_slot_v1",
        "source": "final",
        "state": state,
        "complete": complete,
        "slot_ist": slot_dt.isoformat(),
        "published_at_ist": common.now_ist().isoformat(timespec="seconds"),
        "contracts_expected": int(fetch_marker.get("contracts_expected", len(universe))),
        "features_available": int(len(features)),
        "eligible_contracts": outputs["eligible_contracts"],
        "feature_failure_count": len(failures),
        "duration_sec": time.monotonic() - started,
        **outputs,
    }
    common.atomic_write_json(common.ranking_slot_path(slot_dt), marker)
    common.publish_status(
        SESSION,
        state,
        heartbeat_state="RUNNING",
        phase="SLOT_DONE",
        slot=slot_dt.isoformat(),
        eligible_contracts=outputs["eligible_contracts"],
        feature_failure_count=len(failures),
        output=outputs["report_path"],
    )
    print(
        f"[RANK][{state}] slot={slot_dt.strftime('%H:%M')} "
        f"features={len(features)} eligible={outputs['eligible_contracts']} "
        f"failures={len(failures)} duration={marker['duration_sec']:.1f}s",
        flush=True,
    )
    return marker


def math_coverage_floor(expected: int, ratio: float) -> int:
    return int(np.ceil(max(0, expected) * max(0.0, min(1.0, ratio))))


def _eligible_fetch_markers(day: date) -> list[Path]:
    eligible: list[Path] = []
    for path in sorted(common.FETCH_SLOT_DIR.glob(f"slot_{day.strftime('%Y%m%d')}_*.json")):
        try:
            payload = common.read_json(path)
        except Exception:
            continue
        if str(payload.get("source", "")).lower() == "final" and bool(payload.get("complete")):
            eligible.append(path)
    return eligible


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Calculate FnO OI features and timestamp-aligned cross-sectional rankings."
    )
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--slot", default="")
    parser.add_argument("--session-date", default="")
    parser.add_argument("--poll-sec", type=float, default=1.0)
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--top-n", type=int, default=20)
    parser.add_argument("--min-rank-coverage", type=float, default=0.70)
    parser.add_argument("--min-price-move-pct", type=float, default=0.10)
    parser.add_argument("--min-oi-move-pct", type=float, default=0.25)
    parser.add_argument("--min-oi", type=float, default=0.0)
    parser.add_argument("--min-volume-5m", type=float, default=0.0)
    parser.add_argument("--min-day-volume", type=float, default=0.0)
    parser.add_argument("--min-traded-value", type=float, default=0.0)
    parser.add_argument("--min-active-bars", type=int, default=0)
    parser.add_argument("--catch-up", action="store_true")
    parser.add_argument("--allow-non-trading-day", action="store_true")
    return parser


def run_session(args: argparse.Namespace) -> int:
    current = common.now_ist()
    session_date = (
        date.fromisoformat(args.session_date) if args.session_date else current.date()
    )
    if not 0 < float(args.min_rank_coverage) <= 1:
        raise ValueError("--min-rank-coverage must be in (0, 1].")
    for name in (
        "min_price_move_pct",
        "min_oi_move_pct",
        "min_oi",
        "min_volume_5m",
        "min_day_volume",
        "min_traded_value",
        "min_active_bars",
    ):
        if float(getattr(args, name)) < 0:
            raise ValueError(f"--{name.replace('_', '-')} must be non-negative.")
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
    processed = {
        path.stem.rsplit("_", 1)[-1]
        for path in common.RANKING_SLOT_DIR.glob(f"slot_{session_date.strftime('%Y%m%d')}_*.json")
    }
    common.publish_status(
        SESSION,
        "RUNNING",
        phase="START",
        session_date_ist=session_date.isoformat(),
        contracts_expected=len(universe),
        processed_slots=len(processed),
    )

    if args.once:
        if args.slot:
            slot = pd.Timestamp(args.slot)
            if slot.tzinfo is None:
                slot = slot.tz_localize(common.IST)
            else:
                slot = slot.tz_convert(common.IST)
            marker = common.fetch_slot_path(slot)
        else:
            markers = _eligible_fetch_markers(session_date)
            if not markers:
                raise RuntimeError("No complete FnO fetch marker is available.")
            marker = markers[-1]
        process_slot(marker, universe, args)
        return 0

    end_deadline = datetime.combine(session_date, LAST_SLOT, tzinfo=common.IST) + timedelta(minutes=5)
    ignored_stale: set[str] = set()
    while True:
        current = common.now_ist()
        if current.date() != session_date or current >= end_deadline:
            common.publish_status(
                SESSION,
                "DONE",
                phase="END_TIME",
                session_date_ist=session_date.isoformat(),
                processed_slots=len(processed),
            )
            return 0
        markers = [
            path
            for path in _eligible_fetch_markers(session_date)
            if path.stem.rsplit("_", 1)[-1] not in processed
            and path.stem.rsplit("_", 1)[-1] not in ignored_stale
        ]
        if markers and not args.catch_up and len(markers) > 1:
            for stale in markers[:-1]:
                ignored_stale.add(stale.stem.rsplit("_", 1)[-1])
                print(f"[RANK][SKIP_STALE] {stale.name}", flush=True)
            markers = markers[-1:]
        if not markers:
            common.publish_heartbeat(
                SESSION,
                "WAITING",
                phase="WAIT_FETCH_MARKER",
                session_date_ist=session_date.isoformat(),
                processed_slots=len(processed),
            )
            time.sleep(max(0.2, min(float(args.poll_sec), 5.0)))
            continue
        for marker in markers:
            slot_key = marker.stem.rsplit("_", 1)[-1]
            try:
                process_slot(marker, universe, args)
                processed.add(slot_key)
            except Exception as exc:
                common.publish_status(
                    SESSION,
                    "FAILED",
                    heartbeat_state="RUNNING",
                    phase="SLOT_FAILED",
                    marker=marker.name,
                    error=f"{type(exc).__name__}: {exc}",
                )
                print(
                    f"[RANK][ERROR] marker={marker.name} {type(exc).__name__}: {exc}",
                    file=sys.stderr,
                    flush=True,
                )
                time.sleep(max(1.0, float(args.poll_sec)))


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        return run_session(args)
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
