from __future__ import annotations

import itertools
import json
import math
import os
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import asdict, dataclass
from pathlib import Path

import numpy as np
import pandas as pd

import avwap_5min_ID_v11_backtesting as v11
import research_v11_market_relative_response as common


OUT_DIR = Path(r"C:\TradingData\eqidv2\outputs_ID_v11_late_day_intraday_momentum")

LONG_SETUP = "LD_OPENING_IMPULSE_RETENTION_LONG"
SHORT_SETUP = "LD_OPENING_IMPULSE_RETENTION_SHORT"
SETUPS = (LONG_SETUP, SHORT_SETUP)

EXIT_RULES = (
    (0.60, 0.80),
    (0.70, 1.00),
    (0.80, 1.20),
    (0.90, 1.50),
)

WORKERS = max(1, int(os.getenv("EQIDV2_LDIM_WORKERS", "6")))
RANDOM_SEED = 4051

_WORKER_MARKET_CONTEXT: dict[str, dict[str, float]] = {}


@dataclass(frozen=True)
class FilterProfile:
    name: str
    min_opening_abs_ret_pct: float
    min_opening_abs_rs_pct: float
    min_current_abs_rs_pct: float
    min_retention_ratio: float
    min_vol_ratio: float
    max_consolidation_width_atr: float
    max_abs_vwap_dist_atr: float


FILTER_PROFILES = (
    FilterProfile("broad", 0.25, 0.15, 0.10, 0.40, 0.80, 2.50, 3.00),
    FilterProfile("balanced", 0.50, 0.30, 0.30, 0.60, 1.00, 2.00, 2.50),
    FilterProfile("strict", 0.80, 0.50, 0.50, 0.75, 1.20, 1.50, 2.00),
    FilterProfile("elite", 1.20, 0.80, 0.80, 0.90, 1.50, 1.25, 1.50),
)

TIME_WINDOWS = (
    ("early_close", 780, 840),
    ("late", 810, 870),
    ("closing", 840, 890),
    ("full", 780, 890),
)


def _load_opening_market_context() -> dict[str, dict[str, float]]:
    market = None
    # NIFTYBEES first for live parity + volume-bearing VWAP regime (true NIFTY 50
    # index has zero volume and must not become the regime source).
    for ticker in ("NIFTYBEES", "NIFTY", "NIFTY50", "NIFTY_50"):
        market = common._read_5m(ticker)
        if market is not None and not market.empty:
            break
    if market is None or market.empty:
        raise RuntimeError("No usable NIFTY 5-minute data found")

    context: dict[str, dict[str, float]] = {}
    for day, group in market.groupby("date_only", sort=True):
        g = group.reset_index(drop=True)
        if len(g) < 7:
            continue
        day_open = float(g["open"].iloc[0])
        opening_close = float(g["close"].iloc[5])
        opening_ret = (opening_close / day_open - 1.0) * 100.0 if day_open > 0 else 0.0
        context[str(day)] = {
            "opening_ret_pct": float(opening_ret),
            "day_open": float(day_open),
        }
    return context


def _candidate(
    ticker: str,
    setup: str,
    side: str,
    row: pd.Series,
    *,
    score: float,
    opening_ret_pct: float,
    opening_market_ret_pct: float,
    current_ret_pct: float,
    current_market_ret_pct: float,
    retention_ratio: float,
    consolidation_width_atr: float,
    pullback_depth_atr: float,
    ema50_aligned: bool,
    reason: str,
) -> dict:
    ts = common._normalise_ts(row["date"])
    opening_rs = opening_ret_pct - opening_market_ret_pct
    current_rs = current_ret_pct - current_market_ret_pct
    return {
        "candidate_id": f"{ticker}|{side}|{setup}|{ts.isoformat()}",
        "scan_session": "v11_late_day_intraday_momentum",
        "selection_mode": "late_day_intraday_momentum_probe",
        "candidate_family": "late_day_intraday_momentum",
        "scan_slot_ist": v11._fmt_ist(ts),
        "signal_time_ist": v11._fmt_ist(ts),
        "bar_time_ist": v11._fmt_ist(ts),
        "ticker": ticker,
        "side": side,
        "setup": setup,
        "signal_open": float(row["open"]),
        "signal_high": float(row["high"]),
        "signal_low": float(row["low"]),
        "signal_close": float(row["close"]),
        "signal_volume": float(row.get("volume", 0.0)),
        "quality_score": float(score),
        "ranker_score": float(score),
        "score": float(score),
        "opening_ret_pct": float(opening_ret_pct),
        "opening_market_ret_pct": float(opening_market_ret_pct),
        "opening_rs_pct": float(opening_rs),
        "stock_day_ret_pct": float(current_ret_pct),
        "market_ret_pct": float(current_market_ret_pct),
        "rs_pct": float(current_rs),
        "retention_ratio": float(retention_ratio),
        "consolidation_width_atr": float(consolidation_width_atr),
        "pullback_depth_atr": float(pullback_depth_atr),
        "ema50_aligned": bool(ema50_aligned),
        "vol_ratio": float(row.get("vol_ratio", np.nan)),
        "atr_pct": float(row.get("atr_pct", np.nan)),
        "body_pct": float(row.get("body_pct", np.nan)),
        "close_loc": float(row.get("close_loc", np.nan)),
        "vwap_dist_atr": float(row.get("vwap_dist_atr", np.nan)),
        "signal_minute": int(ts.hour * 60 + ts.minute),
        "reason": reason,
        "status": "LATE_DAY_INTRADAY_MOMENTUM_RAW",
        "created_at_ist": pd.Timestamp.now(tz="Asia/Kolkata").isoformat(),
    }


def _scan_ticker(ticker: str, market_context: dict[str, dict[str, float]]) -> list[dict]:
    df = common._read_5m(ticker)
    if df is None or df.empty:
        return []

    rows: list[dict] = []
    for day, group in df.groupby("date_only", sort=True):
        market = market_context.get(str(day))
        if not market:
            continue
        g = group.reset_index(drop=True)
        if len(g) < 45:
            continue
        day_open = float(g["open"].iloc[0])
        opening_close = float(g["close"].iloc[5])
        opening_ret = (opening_close / day_open - 1.0) * 100.0 if day_open > 0 else 0.0
        opening_market_ret = float(market["opening_ret_pct"])
        opening_rs = opening_ret - opening_market_ret

        for i in range(42, len(g) - 1):
            row = g.iloc[i]
            ts = common._normalise_ts(row["date"])
            minute = ts.hour * 60 + ts.minute
            if minute < 780 or minute > 890:
                continue

            close = float(row["close"])
            open_px = float(row["open"])
            high = float(row["high"])
            low = float(row["low"])
            atr = float(row.get("ATR", np.nan))
            vwap = float(row.get("VWAP", np.nan))
            ema20 = float(row.get("EMA_20", np.nan))
            ema50 = float(row.get("EMA_50", np.nan))
            vol_ratio = float(row.get("vol_ratio", np.nan))
            close_loc = float(row.get("close_loc", np.nan))
            vwap_dist = float(row.get("vwap_dist_atr", np.nan))
            current_range = float(row.get("range", np.nan))
            day_value = float(row.get("day_value_so_far_rs", 0.0))
            if (
                close < 25
                or day_value < 30_000_000
                or not np.isfinite(atr)
                or atr <= 0
                or not np.isfinite(vwap)
                or not np.isfinite(ema20)
                or not np.isfinite(vol_ratio)
                or vol_ratio < 0.80
                or not np.isfinite(close_loc)
                or not np.isfinite(vwap_dist)
                or not np.isfinite(current_range)
                or current_range > 2.50 * atr
            ):
                continue

            market_day_open = float(market["day_open"])
            market_bar = common._WORKER_MARKET_CONTEXT.get(str(day), {}).get(ts)
            if not market_bar:
                continue
            current_market_ret = float(market_bar["market_ret_pct"])
            current_ret = (close / day_open - 1.0) * 100.0 if day_open > 0 else 0.0
            current_rs = current_ret - current_market_ret
            prior4 = g.iloc[i - 4 : i]
            prior_high = float(prior4["high"].max())
            prior_low = float(prior4["low"].min())
            width_atr = (prior_high - prior_low) / atr
            day_high_before = float(g["high"].iloc[:i].max())
            day_low_before = float(g["low"].iloc[:i].min())
            long_pullback_depth = (day_high_before - close) / atr
            short_pullback_depth = (close - day_low_before) / atr
            long_retention = current_ret / opening_ret if opening_ret > 0 else np.nan
            short_retention = current_ret / opening_ret if opening_ret < 0 else np.nan
            long_ema50_aligned = bool(np.isfinite(ema50) and close > ema20 >= ema50)
            short_ema50_aligned = bool(np.isfinite(ema50) and close < ema20 <= ema50)

            broad_long = (
                opening_ret >= 0.25
                and opening_rs >= 0.15
                and current_ret > 0
                and current_rs >= 0.10
                and np.isfinite(long_retention)
                and long_retention >= 0.40
                and close > prior_high
                and close > open_px
                and close_loc >= 0.55
                and close > vwap
                and close > ema20
                and 0 <= vwap_dist <= 3.00
                and width_atr <= 2.50
                and long_pullback_depth <= 4.0
            )
            if broad_long:
                score = (
                    30.0
                    + 8.0 * opening_ret
                    + 8.0 * opening_rs
                    + 6.0 * max(current_rs, 0.0)
                    + 8.0 * min(max(long_retention, 0.0), 2.0)
                    + 5.0 * max(vol_ratio - 0.8, 0.0)
                    - 2.0 * width_atr
                    - 1.5 * long_pullback_depth
                    + (5.0 if long_ema50_aligned else 0.0)
                )
                rows.append(
                    _candidate(
                        ticker,
                        LONG_SETUP,
                        "LONG",
                        row,
                        score=score,
                        opening_ret_pct=opening_ret,
                        opening_market_ret_pct=opening_market_ret,
                        current_ret_pct=current_ret,
                        current_market_ret_pct=current_market_ret,
                        retention_ratio=long_retention,
                        consolidation_width_atr=width_atr,
                        pullback_depth_atr=long_pullback_depth,
                        ema50_aligned=long_ema50_aligned,
                        reason="opening_relative_strength_retained_then_late_range_break",
                    )
                )

            broad_short = (
                opening_ret <= -0.25
                and opening_rs <= -0.15
                and current_ret < 0
                and current_rs <= -0.10
                and np.isfinite(short_retention)
                and short_retention >= 0.40
                and close < prior_low
                and close < open_px
                and close_loc <= 0.45
                and close < vwap
                and close < ema20
                and -3.00 <= vwap_dist <= 0
                and width_atr <= 2.50
                and short_pullback_depth <= 4.0
            )
            if broad_short:
                score = (
                    30.0
                    + 8.0 * -opening_ret
                    + 8.0 * -opening_rs
                    + 6.0 * max(-current_rs, 0.0)
                    + 8.0 * min(max(short_retention, 0.0), 2.0)
                    + 5.0 * max(vol_ratio - 0.8, 0.0)
                    - 2.0 * width_atr
                    - 1.5 * short_pullback_depth
                    + (5.0 if short_ema50_aligned else 0.0)
                )
                rows.append(
                    _candidate(
                        ticker,
                        SHORT_SETUP,
                        "SHORT",
                        row,
                        score=score,
                        opening_ret_pct=opening_ret,
                        opening_market_ret_pct=opening_market_ret,
                        current_ret_pct=current_ret,
                        current_market_ret_pct=current_market_ret,
                        retention_ratio=short_retention,
                        consolidation_width_atr=width_atr,
                        pullback_depth_atr=short_pullback_depth,
                        ema50_aligned=short_ema50_aligned,
                        reason="opening_relative_weakness_retained_then_late_range_break",
                    )
                )
    return rows


def _init_worker(
    opening_market_context: dict[str, dict[str, float]],
    bar_market_context: dict[str, dict[pd.Timestamp, dict[str, float | str]]],
) -> None:
    global _WORKER_MARKET_CONTEXT
    _WORKER_MARKET_CONTEXT = bar_market_context
    common._WORKER_MARKET_CONTEXT = bar_market_context
    common._LDIM_OPENING_MARKET_CONTEXT = opening_market_context


def _scan_job(ticker: str) -> tuple[str, list[dict], str | None, float]:
    started = time.time()
    try:
        opening_context = getattr(common, "_LDIM_OPENING_MARKET_CONTEXT", {})
        return ticker, _scan_ticker(ticker, opening_context), None, time.time() - started
    except Exception as exc:
        return ticker, [], repr(exc), time.time() - started


def _scan_all() -> pd.DataFrame:
    opening_context = _load_opening_market_context()
    bar_context = common._load_market_context()
    universe = common._load_universe()
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    pd.DataFrame({"ticker": universe}).to_csv(OUT_DIR / "late_day_momentum_universe.csv", index=False)
    print(f"[scan] universe={len(universe):,} workers={min(WORKERS, len(universe))}", flush=True)

    rows: list[dict] = []
    errors: list[dict] = []
    started = time.time()
    workers = min(WORKERS, max(1, len(universe)))
    with ProcessPoolExecutor(
        max_workers=workers,
        initializer=_init_worker,
        initargs=(opening_context, bar_context),
    ) as pool:
        futures = {pool.submit(_scan_job, ticker): ticker for ticker in universe}
        for i, future in enumerate(as_completed(futures), 1):
            ticker, ticker_rows, error, seconds = future.result()
            rows.extend(ticker_rows)
            if error:
                errors.append({"ticker": ticker, "error": error})
            if i % 10 == 0 or i == len(universe) or error:
                print(
                    f"  [scan {i:3d}/{len(universe)}] {ticker} rows={len(rows):,} "
                    f"last_sec={seconds:.1f} elapsed={time.time() - started:.1f}s",
                    flush=True,
                )

    pd.DataFrame(errors).to_csv(OUT_DIR / "late_day_momentum_scan_errors.csv", index=False)
    raw = pd.DataFrame(rows)
    if raw.empty:
        return raw
    raw["_day"] = pd.to_datetime(raw["signal_time_ist"], errors="coerce").dt.strftime("%Y-%m-%d")
    return (
        raw.sort_values(["setup", "ticker", "_day", "quality_score"], ascending=[True, True, True, False])
        .drop_duplicates(["setup", "ticker", "_day"], keep="first")
        .drop(columns="_day")
        .reset_index(drop=True)
    )


def _resolve_all(candidates: pd.DataFrame) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for setup in SETUPS:
        setup_candidates = candidates.loc[candidates["setup"].astype(str).eq(setup)].copy()
        for sl_pct, target_pct in EXIT_RULES:
            print(
                f"[resolve] {setup} raw={len(setup_candidates):,} exit={sl_pct:.2f}/{target_pct:.2f}",
                flush=True,
            )
            v11.v6.SETUP_EXIT_RULES[setup] = (sl_pct, target_pct)
            signals, _, rejects = v11._build_v7_entry_engine_signals(setup_candidates)
            if not rejects.empty:
                rejects.to_csv(
                    OUT_DIR / f"rejects_{setup}_{sl_pct:.2f}_{target_pct:.2f}.csv",
                    index=False,
                )
            if signals.empty:
                continue
            trades = v11._resolve_v7_entry_engine_signals(
                signals,
                label=f"late_day_momentum_{setup}_{sl_pct:.2f}_{target_pct:.2f}",
                entry_fill_model="ltp_on_signal_1m_open",
                selected_strategy_profile="none",
            )
            if trades.empty:
                continue
            trades["exit_key"] = f"{sl_pct:.2f}/{target_pct:.2f}"
            trades["probe_sl_pct"] = sl_pct
            trades["probe_target_pct"] = target_pct
            trades["date"] = pd.to_datetime(trades["trade_date"], errors="coerce")
            trades["pnl"] = pd.to_numeric(trades["v6_net_pnl_rs"], errors="coerce").fillna(0.0)
            frames.append(trades)
    return pd.concat(frames, ignore_index=True, sort=False) if frames else pd.DataFrame()


def _profit_factor(pnl: pd.Series) -> float:
    values = pd.to_numeric(pnl, errors="coerce").fillna(0.0)
    gains = float(values[values > 0].sum())
    losses = float(-values[values < 0].sum())
    if losses <= 0:
        return math.inf if gains > 0 else 0.0
    return gains / losses


def _metrics(frame: pd.DataFrame) -> dict[str, float | int]:
    if frame.empty:
        return {
            "trades": 0,
            "days": 0,
            "pnl": 0.0,
            "pf": 0.0,
            "win_pct": 0.0,
            "avg_trade": 0.0,
            "positive_month_pct": 0.0,
        }
    pnl = pd.to_numeric(frame["pnl"], errors="coerce").fillna(0.0)
    monthly = frame.assign(_pnl=pnl).groupby(frame["date"].dt.to_period("M"))["_pnl"].sum()
    return {
        "trades": int(len(frame)),
        "days": int(frame["date"].dt.date.nunique()),
        "pnl": float(pnl.sum()),
        "pf": float(_profit_factor(pnl)),
        "win_pct": float((pnl > 0).mean() * 100.0),
        "avg_trade": float(pnl.mean()),
        "positive_month_pct": float((monthly > 0).mean() * 100.0) if len(monthly) else 0.0,
    }


def _split(frame: pd.DataFrame, split: str) -> pd.DataFrame:
    if split == "train":
        return frame.loc[frame["date"] <= common.TRAIN_END]
    if split == "valid":
        return frame.loc[
            (frame["date"] >= common.VALID_START) & (frame["date"] <= common.VALID_END)
        ]
    if split == "holdout":
        return frame.loc[
            (frame["date"] >= common.HOLDOUT_START) & (frame["date"] <= common.END_DATE)
        ]
    raise ValueError(split)


def _bootstrap(frame: pd.DataFrame, n_bootstrap: int = 5000) -> dict[str, float]:
    if frame.empty:
        return {"bootstrap_p_lte_zero": 1.0, "daily_pnl_ci_low": 0.0, "daily_pnl_ci_high": 0.0}
    daily = frame.groupby(frame["date"].dt.normalize())["pnl"].sum().to_numpy(dtype=float)
    if len(daily) < 5:
        return {"bootstrap_p_lte_zero": 1.0, "daily_pnl_ci_low": 0.0, "daily_pnl_ci_high": 0.0}
    rng = np.random.default_rng(RANDOM_SEED)
    samples = daily[rng.integers(0, len(daily), size=(n_bootstrap, len(daily)))].mean(axis=1)
    return {
        "bootstrap_p_lte_zero": float((samples <= 0).mean()),
        "daily_pnl_ci_low": float(np.quantile(samples, 0.025)),
        "daily_pnl_ci_high": float(np.quantile(samples, 0.975)),
    }


def _filter_mask(
    frame: pd.DataFrame,
    side: str,
    profile: FilterProfile,
    start_minute: int,
    end_minute: int,
    require_ema50: bool,
) -> pd.Series:
    opening_ret = pd.to_numeric(frame["opening_ret_pct"], errors="coerce")
    opening_rs = pd.to_numeric(frame["opening_rs_pct"], errors="coerce")
    current_rs = pd.to_numeric(frame["rs_pct"], errors="coerce")
    retention = pd.to_numeric(frame["retention_ratio"], errors="coerce")
    vol_ratio = pd.to_numeric(frame["vol_ratio"], errors="coerce")
    width = pd.to_numeric(frame["consolidation_width_atr"], errors="coerce")
    vwap_dist = pd.to_numeric(frame["vwap_dist_atr"], errors="coerce")
    minute = pd.to_numeric(frame["signal_minute"], errors="coerce")
    ema50 = frame["ema50_aligned"].astype(str).str.lower().isin({"true", "1"})

    common_mask = (
        minute.between(start_minute, end_minute)
        & (retention >= profile.min_retention_ratio)
        & (vol_ratio >= profile.min_vol_ratio)
        & (width <= profile.max_consolidation_width_atr)
        & (vwap_dist.abs() <= profile.max_abs_vwap_dist_atr)
    )
    if require_ema50:
        common_mask &= ema50
    if side == "LONG":
        return (
            common_mask
            & (opening_ret >= profile.min_opening_abs_ret_pct)
            & (opening_rs >= profile.min_opening_abs_rs_pct)
            & (current_rs >= profile.min_current_abs_rs_pct)
        )
    return (
        common_mask
        & (opening_ret <= -profile.min_opening_abs_ret_pct)
        & (opening_rs <= -profile.min_opening_abs_rs_pct)
        & (current_rs <= -profile.min_current_abs_rs_pct)
    )


def _configuration_rows(trades: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, pd.Series]]:
    rows: list[dict] = []
    masks: dict[str, pd.Series] = {}
    for setup, side in ((LONG_SETUP, "LONG"), (SHORT_SETUP, "SHORT")):
        for profile, (window_name, start_minute, end_minute), require_ema50, exit_rule in itertools.product(
            FILTER_PROFILES,
            TIME_WINDOWS,
            (False, True),
            EXIT_RULES,
        ):
            exit_key = f"{exit_rule[0]:.2f}/{exit_rule[1]:.2f}"
            mask = (
                trades["setup"].astype(str).eq(setup)
                & trades["exit_key"].astype(str).eq(exit_key)
                & _filter_mask(
                    trades,
                    side,
                    profile,
                    start_minute,
                    end_minute,
                    require_ema50,
                )
            )
            config_id = (
                f"{setup}|{profile.name}|{window_name}|ema50={int(require_ema50)}|exit={exit_key}"
            )
            masks[config_id] = mask
            row: dict[str, object] = {
                "config_id": config_id,
                "setup": setup,
                "side": side,
                "profile": profile.name,
                "time_window": window_name,
                "start_minute": start_minute,
                "end_minute": end_minute,
                "require_ema50": require_ema50,
                "exit_key": exit_key,
                "sl_pct": exit_rule[0],
                "target_pct": exit_rule[1],
                **asdict(profile),
            }
            for split in ("train", "valid", "holdout"):
                for key, value in _metrics(_split(trades.loc[mask].copy(), split)).items():
                    row[f"{split}_{key}"] = value
            rows.append(row)
    return pd.DataFrame(rows), masks


def _select(
    configurations: pd.DataFrame,
    masks: dict[str, pd.Series],
    trades: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    selected: list[dict] = []
    chosen_frames: list[pd.DataFrame] = []
    for setup in SETUPS:
        train_pool = configurations.loc[
            configurations["setup"].eq(setup)
            & (configurations["train_trades"] >= 35)
            & (configurations["train_pf"] >= 1.25)
            & (configurations["train_pnl"] > 0)
            & (configurations["train_positive_month_pct"] >= 55.0)
        ].copy()
        if train_pool.empty:
            selected.append({"setup": setup, "decision": "REJECT_NO_TRAIN_EDGE"})
            continue
        train_pool["train_score"] = (
            np.minimum(train_pool["train_pf"], 4.0) * 100.0
            + np.log1p(train_pool["train_trades"]) * 12.0
            + train_pool["train_positive_month_pct"]
            + np.maximum(train_pool["train_avg_trade"], 0.0) / 10.0
        )
        shortlist = train_pool.sort_values(
            ["train_score", "train_trades"], ascending=[False, False]
        ).head(10)
        valid = shortlist.loc[
            (shortlist["valid_trades"] >= 12)
            & (shortlist["valid_pf"] >= 1.15)
            & (shortlist["valid_pnl"] > 0)
            & (shortlist["valid_positive_month_pct"] >= 50.0)
        ].copy()
        if valid.empty:
            row = shortlist.iloc[0].to_dict()
            row["decision"] = "REJECT_VALIDATION"
            selected.append(row)
            continue
        valid["validation_score"] = (
            np.minimum(valid["valid_pf"], 4.0) * 100.0
            + np.log1p(valid["valid_trades"]) * 12.0
            + valid["valid_positive_month_pct"]
            + np.maximum(valid["valid_avg_trade"], 0.0) / 10.0
        )
        row = valid.sort_values(
            ["validation_score", "valid_trades"], ascending=[False, False]
        ).iloc[0].to_dict()
        chosen = trades.loc[masks[str(row["config_id"])]].copy()
        bootstrap = _bootstrap(_split(chosen, "holdout"))
        row.update(bootstrap)
        passes = (
            int(row["holdout_trades"]) >= 20
            and float(row["holdout_pf"]) >= 1.20
            and float(row["holdout_pnl"]) > 0
            and float(bootstrap["bootstrap_p_lte_zero"]) <= 0.15
        )
        row["decision"] = "PROMISING_PROBATION" if passes else "REJECT_HOLDOUT"
        selected.append(row)
        chosen["selected_config_id"] = row["config_id"]
        chosen_frames.append(chosen)
    return (
        pd.DataFrame(selected),
        pd.concat(chosen_frames, ignore_index=True, sort=False) if chosen_frames else pd.DataFrame(),
    )


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    candidates_path = OUT_DIR / "late_day_momentum_raw_candidates.csv"
    trades_path = OUT_DIR / "late_day_momentum_resolved_trades.csv"

    if candidates_path.exists():
        candidates = pd.read_csv(candidates_path)
        print(f"[load] cached candidates={len(candidates):,}", flush=True)
    else:
        candidates = _scan_all()
        candidates.to_csv(candidates_path, index=False)
        print(f"[scan] wrote candidates={len(candidates):,}", flush=True)
    if candidates.empty:
        raise SystemExit("No late-day momentum candidates generated")
    candidates.groupby(["setup", "side"]).size().reset_index(name="raw_candidates").to_csv(
        OUT_DIR / "late_day_momentum_raw_counts.csv", index=False
    )

    if trades_path.exists():
        trades = pd.read_csv(trades_path)
        trades["date"] = pd.to_datetime(trades["date"], errors="coerce")
        print(f"[load] cached resolved trades={len(trades):,}", flush=True)
    else:
        trades = _resolve_all(candidates)
        trades.to_csv(trades_path, index=False)
        print(f"[resolve] wrote trades={len(trades):,}", flush=True)
    if trades.empty:
        raise SystemExit("No late-day momentum trades resolved")

    configurations, masks = _configuration_rows(trades)
    configurations.to_csv(OUT_DIR / "late_day_momentum_configurations.csv", index=False)
    selected, selected_trades = _select(configurations, masks, trades)
    selected.to_csv(OUT_DIR / "late_day_momentum_selected.csv", index=False)
    selected_trades.to_csv(OUT_DIR / "late_day_momentum_selected_trades.csv", index=False)

    metadata = {
        "research_hypothesis": (
            "opening half-hour stock return and relative strength persist into a "
            "late-session range break when direction is retained above/below VWAP"
        ),
        "start_date": str(common.START_DATE.date()),
        "train_end": str(common.TRAIN_END.date()),
        "validation": [str(common.VALID_START.date()), str(common.VALID_END.date())],
        "holdout": [str(common.HOLDOUT_START.date()), str(common.END_DATE.date())],
        "filter_profiles": [asdict(profile) for profile in FILTER_PROFILES],
        "time_windows": TIME_WINDOWS,
        "exit_rules": EXIT_RULES,
        "candidate_count": int(len(candidates)),
        "resolved_trade_rows": int(len(trades)),
    }
    (OUT_DIR / "late_day_momentum_run_metadata.json").write_text(
        json.dumps(metadata, indent=2),
        encoding="ascii",
    )

    print("\n[selected]", flush=True)
    print(
        selected[
            [
                col
                for col in (
                    "setup",
                    "decision",
                    "profile",
                    "time_window",
                    "require_ema50",
                    "exit_key",
                    "train_trades",
                    "train_pf",
                    "valid_trades",
                    "valid_pf",
                    "holdout_trades",
                    "holdout_pf",
                    "holdout_pnl",
                    "bootstrap_p_lte_zero",
                )
                if col in selected.columns
            ]
        ].to_string(index=False),
        flush=True,
    )


if __name__ == "__main__":
    main()
