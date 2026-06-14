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

import avwap_5min_ID_v2_backtesting as v2
import avwap_5min_ID_v11_backtesting as v11


DATA_ROOT = Path(r"C:\TradingData\eqidv2\stocks_indicators_5min_eq_live2")
OUT_DIR = Path(r"C:\TradingData\eqidv2\outputs_ID_v11_market_relative_response")

START_DATE = pd.Timestamp("2025-06-02")
TRAIN_END = pd.Timestamp("2026-01-31")
VALID_START = pd.Timestamp("2026-02-01")
VALID_END = pd.Timestamp("2026-03-31")
HOLDOUT_START = pd.Timestamp("2026-04-01")
END_DATE = pd.Timestamp(os.getenv("EQIDV2_MRR_END_DATE", "2026-06-11"))

LONG_SETUP = "X_MARKET_PULLBACK_RESILIENCE_LONG"
SHORT_SETUP = "X_MARKET_BOUNCE_FAILURE_SHORT"
SETUPS = (LONG_SETUP, SHORT_SETUP)

EXIT_RULES = (
    (0.60, 0.90),
    (0.70, 1.00),
    (0.75, 1.20),
    (0.80, 1.50),
)

WORKERS = max(1, int(os.getenv("EQIDV2_MRR_WORKERS", "4")))
UNIVERSE_MODE = os.getenv("EQIDV2_MRR_UNIVERSE", "futures").strip().lower()
RANDOM_SEED = 1127

_WORKER_MARKET_CONTEXT: dict[str, dict[pd.Timestamp, dict[str, float | str]]] = {}


@dataclass(frozen=True)
class FilterProfile:
    name: str
    min_abs_rs_pct: float
    min_response_divergence_pct: float
    min_adverse_market_move_pct: float
    max_stock_giveback_pct: float
    min_vol_ratio: float
    max_compression_width_atr: float


FILTER_PROFILES = (
    FilterProfile("broad", 0.15, 0.18, 0.08, 0.12, 1.00, 2.25),
    FilterProfile("balanced", 0.40, 0.30, 0.12, 0.03, 1.15, 1.75),
    FilterProfile("strict", 0.70, 0.45, 0.16, 0.00, 1.30, 1.50),
    FilterProfile("elite", 1.00, 0.65, 0.20, -0.05, 1.50, 1.25),
)

TIME_WINDOWS = (
    ("morning", 600, 720),
    ("morning_plus", 600, 780),
    ("midday", 630, 840),
    ("full", 600, 870),
)


def _normalise_ts(value) -> pd.Timestamp:
    ts = pd.to_datetime(value, errors="coerce")
    if pd.isna(ts):
        return pd.NaT
    if ts.tz is None:
        return ts.tz_localize("Asia/Kolkata")
    return ts.tz_convert("Asia/Kolkata")


def _prepare_5m(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["date"] = pd.to_datetime(out["date"], errors="coerce")
    if getattr(out["date"].dt, "tz", None) is None:
        out["date"] = out["date"].dt.tz_localize("Asia/Kolkata")
    else:
        out["date"] = out["date"].dt.tz_convert("Asia/Kolkata")
    out = out.dropna(subset=["date"]).sort_values("date").drop_duplicates("date", keep="last")
    out["date_only"] = out["date"].dt.strftime("%Y-%m-%d")
    out = out.loc[
        (out["date"].dt.date >= START_DATE.date())
        & (out["date"].dt.date <= END_DATE.date())
    ].copy()
    if out.empty:
        return out

    if (
        "ATR" not in out.columns
        or out["ATR"].isna().all()
        or "VWAP" not in out.columns
        or out["VWAP"].isna().all()
    ):
        out = v2._prepare_5m(out)

    out["Volume_SMA20"] = out.groupby("date_only")["volume"].transform(
        lambda s: s.shift(1).rolling(v2.VWAP_LOOKBACK, min_periods=8).mean()
    )
    out["traded_value_rs"] = out["close"] * out["volume"]
    out["day_value_so_far_rs"] = out.groupby("date_only")["traded_value_rs"].cumsum()
    out["range"] = out["high"] - out["low"]
    nonzero_range = out["range"].replace(0, np.nan)
    out["body_pct"] = (out["close"] - out["open"]).abs() / nonzero_range
    out["close_loc"] = (out["close"] - out["low"]) / nonzero_range
    out["upper_wick_pct"] = (
        out["high"] - out[["open", "close"]].max(axis=1)
    ) / nonzero_range
    out["lower_wick_pct"] = (
        out[["open", "close"]].min(axis=1) - out["low"]
    ) / nonzero_range
    out["vol_ratio"] = out["volume"] / out["Volume_SMA20"].replace(0, np.nan)
    out["atr_pct"] = out["ATR"] / out["close"].replace(0, np.nan)
    out["vwap_dist_atr"] = (out["close"] - out["VWAP"]) / out["ATR"].replace(0, np.nan)
    return out.reset_index(drop=True)


def _read_5m(ticker: str) -> pd.DataFrame | None:
    path = DATA_ROOT / f"{ticker}_stocks_indicators_5min.parquet"
    if not path.exists():
        return None
    frame = pd.read_parquet(path)
    if frame.empty or "date" not in frame.columns:
        return None
    prepared = _prepare_5m(frame)
    return prepared if not prepared.empty else None


def _load_universe() -> list[str]:
    if UNIVERSE_MODE in {"futures", "fo", "nfo"}:
        from filtered_stocks_NSE_futures_only import selected_stocks

        raw = selected_stocks
    elif UNIVERSE_MODE == "all":
        raw = v2._load_universe()
    else:
        path = Path("configs/universe_fo.csv")
        frame = pd.read_csv(path)
        col = "ticker" if "ticker" in frame.columns else frame.columns[0]
        raw = frame[col].dropna().tolist()

    return sorted(
        {
            str(value).upper().replace(".NS", "").strip()
            for value in raw
            if str(value).strip()
            and not str(value).upper().startswith("NIFTY")
            and not str(value).upper().endswith("BEES")
            and (DATA_ROOT / f"{str(value).upper().replace('.NS', '').strip()}_stocks_indicators_5min.parquet").exists()
        }
    )


def _load_market_context() -> dict[str, dict[pd.Timestamp, dict[str, float | str]]]:
    market = None
    # NIFTYBEES first to stay in parity with live (v2._load_market_context is
    # NIFTYBEES-first) and to keep a volume-bearing series for VWAP regime. The
    # true NIFTY 50 index (zero volume) must never become the regime source.
    for ticker in ("NIFTYBEES", "NIFTY", "NIFTY50", "NIFTY_50"):
        market = _read_5m(ticker)
        if market is not None and not market.empty:
            break
    if market is None or market.empty:
        raise RuntimeError("No usable NIFTY 5-minute market context found")

    context: dict[str, dict[pd.Timestamp, dict[str, float | str]]] = {}
    for day, group in market.groupby("date_only", sort=True):
        g = group.reset_index(drop=True)
        day_open = float(g["open"].iloc[0])
        records: dict[pd.Timestamp, dict[str, float | str]] = {}
        for i, row in g.iterrows():
            ts = _normalise_ts(row["date"])
            close = float(row["close"])
            day_ret = (close / day_open - 1.0) * 100.0 if day_open > 0 else 0.0
            pre3 = np.nan
            pre5 = np.nan
            if i >= 4:
                pre3 = (float(g["close"].iloc[i - 1]) / float(g["close"].iloc[i - 4]) - 1.0) * 100.0
            if i >= 6:
                pre5 = (float(g["close"].iloc[i - 1]) / float(g["close"].iloc[i - 6]) - 1.0) * 100.0
            vwap = float(row.get("VWAP", np.nan))
            adx = float(row.get("ADX", np.nan))
            if np.isfinite(vwap) and close > vwap and day_ret >= 0.20:
                regime = "BULL"
            elif np.isfinite(vwap) and close < vwap and day_ret <= -0.20:
                regime = "BEAR"
            elif np.isfinite(adx) and adx >= 25:
                regime = "TREND"
            else:
                regime = "NEUTRAL"
            records[ts] = {
                "market_ret_pct": float(day_ret),
                "market_pre3_ret_pct": float(pre3),
                "market_pre5_ret_pct": float(pre5),
                "regime": regime,
            }
        context[str(day)] = records
    return context


def _candidate(
    ticker: str,
    setup: str,
    side: str,
    row: pd.Series,
    *,
    score: float,
    market: dict[str, float | str],
    stock_day_ret_pct: float,
    stock_pre3_ret_pct: float,
    stock_pre5_ret_pct: float,
    response_divergence_pct: float,
    compression_width_atr: float,
    ema20_slope_atr: float,
    trend_aligned: bool,
    reason: str,
) -> dict:
    ts = _normalise_ts(row["date"])
    market_ret = float(market["market_ret_pct"])
    return {
        "candidate_id": f"{ticker}|{side}|{setup}|{ts.isoformat()}",
        "scan_session": "v11_market_relative_response",
        "selection_mode": "market_relative_response_probe",
        "candidate_family": "market_relative_response",
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
        "stock_day_ret_pct": float(stock_day_ret_pct),
        "market_ret_pct": market_ret,
        "rs_pct": float(stock_day_ret_pct - market_ret),
        "stock_pre3_ret_pct": float(stock_pre3_ret_pct),
        "market_pre3_ret_pct": float(market["market_pre3_ret_pct"]),
        "stock_pre5_ret_pct": float(stock_pre5_ret_pct),
        "market_pre5_ret_pct": float(market["market_pre5_ret_pct"]),
        "response_divergence_pct": float(response_divergence_pct),
        "compression_width_atr": float(compression_width_atr),
        "ema20_slope_atr": float(ema20_slope_atr),
        "trend_aligned": bool(trend_aligned),
        "regime": str(market["regime"]),
        "vol_ratio": float(row.get("vol_ratio", np.nan)),
        "atr_pct": float(row.get("atr_pct", np.nan)),
        "body_pct": float(row.get("body_pct", np.nan)),
        "close_loc": float(row.get("close_loc", np.nan)),
        "vwap_dist_atr": float(row.get("vwap_dist_atr", np.nan)),
        "signal_minute": int(ts.hour * 60 + ts.minute),
        "reason": reason,
        "status": "MARKET_RELATIVE_RESPONSE_RAW",
        "created_at_ist": pd.Timestamp.now(tz="Asia/Kolkata").isoformat(),
    }


def _scan_ticker(ticker: str, market_context: dict[str, dict[pd.Timestamp, dict[str, float | str]]]) -> list[dict]:
    df = _read_5m(ticker)
    if df is None or df.empty:
        return []

    rows: list[dict] = []
    for day, group in df.groupby("date_only", sort=True):
        market_day = market_context.get(str(day), {})
        if not market_day:
            continue
        g = group.reset_index(drop=True)
        if len(g) < 16:
            continue
        day_open = float(g["open"].iloc[0])

        for i in range(6, len(g) - 1):
            row = g.iloc[i]
            ts = _normalise_ts(row["date"])
            minute = ts.hour * 60 + ts.minute
            if minute < 600 or minute > 870:
                continue
            market = market_day.get(ts)
            if not market:
                continue
            market_pre3 = float(market["market_pre3_ret_pct"])
            market_pre5 = float(market["market_pre5_ret_pct"])
            if not np.isfinite(market_pre3) or not np.isfinite(market_pre5):
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
                or day_value < 20_000_000
                or not np.isfinite(atr)
                or atr <= 0
                or not np.isfinite(vwap)
                or not np.isfinite(vol_ratio)
                or vol_ratio < 1.0
                or not np.isfinite(close_loc)
                or not np.isfinite(current_range)
                or current_range > 2.25 * atr
            ):
                continue

            prior3 = g.iloc[i - 3 : i]
            prior5 = g.iloc[i - 5 : i]
            stock_pre3 = (float(g["close"].iloc[i - 1]) / float(g["close"].iloc[i - 4]) - 1.0) * 100.0
            stock_pre5 = (float(g["close"].iloc[i - 1]) / float(g["close"].iloc[i - 6]) - 1.0) * 100.0
            stock_day_ret = (close / day_open - 1.0) * 100.0 if day_open > 0 else 0.0
            market_day_ret = float(market["market_ret_pct"])
            rs_pct = stock_day_ret - market_day_ret
            response_divergence = stock_pre3 - market_pre3
            width_atr = float(prior3["high"].max() - prior3["low"].min()) / atr
            ema20_slope_atr = (
                (ema20 - float(g["EMA_20"].iloc[i - 3])) / atr
                if np.isfinite(ema20) and np.isfinite(float(g["EMA_20"].iloc[i - 3]))
                else np.nan
            )
            prior_high = float(prior3["high"].max())
            prior_low = float(prior3["low"].min())
            prior5_high = float(prior5["high"].max())
            prior5_low = float(prior5["low"].min())

            long_trend = bool(
                np.isfinite(ema20)
                and np.isfinite(ema50)
                and close > ema20 >= ema50
                and np.isfinite(ema20_slope_atr)
                and ema20_slope_atr >= 0
            )
            short_trend = bool(
                np.isfinite(ema20)
                and np.isfinite(ema50)
                and close < ema20 <= ema50
                and np.isfinite(ema20_slope_atr)
                and ema20_slope_atr <= 0
            )

            broad_long = (
                market_pre3 <= -0.08
                and stock_pre3 >= -0.12
                and response_divergence >= 0.18
                and rs_pct >= 0.15
                and close > prior_high
                and high > prior5_high
                and close > open_px
                and close_loc >= 0.55
                and close > vwap
                and low >= vwap - 0.50 * atr
                and 0 <= vwap_dist <= 2.50
                and width_atr <= 2.25
            )
            if broad_long:
                score = (
                    35.0
                    + 18.0 * response_divergence
                    + 7.0 * max(rs_pct, 0.0)
                    + 6.0 * min(max(vol_ratio - 1.0, 0.0), 3.0)
                    + 8.0 * max(close_loc - 0.50, 0.0)
                    - 3.0 * width_atr
                    + (5.0 if long_trend else 0.0)
                )
                rows.append(
                    _candidate(
                        ticker,
                        LONG_SETUP,
                        "LONG",
                        row,
                        score=score,
                        market=market,
                        stock_day_ret_pct=stock_day_ret,
                        stock_pre3_ret_pct=stock_pre3,
                        stock_pre5_ret_pct=stock_pre5,
                        response_divergence_pct=response_divergence,
                        compression_width_atr=width_atr,
                        ema20_slope_atr=ema20_slope_atr,
                        trend_aligned=long_trend,
                        reason="stock_held_during_index_pullback_then_broke_prior_range",
                    )
                )

            broad_short = (
                market_pre3 >= 0.08
                and stock_pre3 <= 0.12
                and response_divergence <= -0.18
                and rs_pct <= -0.15
                and close < prior_low
                and low < prior5_low
                and close < open_px
                and close_loc <= 0.45
                and close < vwap
                and high <= vwap + 0.50 * atr
                and -2.50 <= vwap_dist <= 0
                and width_atr <= 2.25
            )
            if broad_short:
                score = (
                    35.0
                    + 18.0 * -response_divergence
                    + 7.0 * max(-rs_pct, 0.0)
                    + 6.0 * min(max(vol_ratio - 1.0, 0.0), 3.0)
                    + 8.0 * max(0.50 - close_loc, 0.0)
                    - 3.0 * width_atr
                    + (5.0 if short_trend else 0.0)
                )
                rows.append(
                    _candidate(
                        ticker,
                        SHORT_SETUP,
                        "SHORT",
                        row,
                        score=score,
                        market=market,
                        stock_day_ret_pct=stock_day_ret,
                        stock_pre3_ret_pct=stock_pre3,
                        stock_pre5_ret_pct=stock_pre5,
                        response_divergence_pct=response_divergence,
                        compression_width_atr=width_atr,
                        ema20_slope_atr=ema20_slope_atr,
                        trend_aligned=short_trend,
                        reason="stock_failed_during_index_bounce_then_broke_prior_range",
                    )
                )
    return rows


def _init_worker(market_context: dict[str, dict[pd.Timestamp, dict[str, float | str]]]) -> None:
    global _WORKER_MARKET_CONTEXT
    _WORKER_MARKET_CONTEXT = market_context


def _scan_job(ticker: str) -> tuple[str, list[dict], str | None, float]:
    started = time.time()
    try:
        return ticker, _scan_ticker(ticker, _WORKER_MARKET_CONTEXT), None, time.time() - started
    except Exception as exc:
        return ticker, [], repr(exc), time.time() - started


def _scan_all() -> pd.DataFrame:
    market_context = _load_market_context()
    universe = _load_universe()
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    pd.DataFrame({"ticker": universe, "universe_mode": UNIVERSE_MODE}).to_csv(
        OUT_DIR / "market_relative_response_universe.csv", index=False
    )
    print(f"[scan] universe={len(universe):,} workers={min(WORKERS, len(universe))}", flush=True)

    rows: list[dict] = []
    errors: list[dict] = []
    started = time.time()
    workers = min(WORKERS, max(1, len(universe)))
    if workers == 1:
        _init_worker(market_context)
        iterator = ((_scan_job(ticker), ticker) for ticker in universe)
        for i, (result, _) in enumerate(iterator, 1):
            ticker, ticker_rows, error, seconds = result
            rows.extend(ticker_rows)
            if error:
                errors.append({"ticker": ticker, "error": error})
            if i % 10 == 0 or i == len(universe):
                print(
                    f"  [scan {i:3d}/{len(universe)}] {ticker} rows={len(rows):,} "
                    f"last_sec={seconds:.1f} elapsed={time.time() - started:.1f}s",
                    flush=True,
                )
    else:
        with ProcessPoolExecutor(
            max_workers=workers,
            initializer=_init_worker,
            initargs=(market_context,),
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

    pd.DataFrame(errors).to_csv(OUT_DIR / "market_relative_response_scan_errors.csv", index=False)
    raw = pd.DataFrame(rows)
    if raw.empty:
        return raw
    raw["_day"] = pd.to_datetime(raw["signal_time_ist"], errors="coerce").dt.strftime("%Y-%m-%d")
    raw = (
        raw.sort_values(["setup", "ticker", "_day", "quality_score"], ascending=[True, True, True, False])
        .drop_duplicates(["setup", "ticker", "_day"], keep="first")
        .drop(columns="_day")
        .reset_index(drop=True)
    )
    return raw


def _resolve_all(candidates: pd.DataFrame) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for setup in SETUPS:
        setup_candidates = candidates.loc[candidates["setup"].astype(str).eq(setup)].copy()
        if setup_candidates.empty:
            continue
        for sl_pct, target_pct in EXIT_RULES:
            print(
                f"[resolve] {setup} raw={len(setup_candidates):,} exit={sl_pct:.2f}/{target_pct:.2f}",
                flush=True,
            )
            v11.v6.SETUP_EXIT_RULES[setup] = (sl_pct, target_pct)
            signals, _, rejects = v11._build_v7_entry_engine_signals(setup_candidates)
            if not rejects.empty:
                rejects.assign(
                    setup=setup,
                    sl_pct=sl_pct,
                    target_pct=target_pct,
                ).to_csv(
                    OUT_DIR / f"rejects_{setup}_{sl_pct:.2f}_{target_pct:.2f}.csv",
                    index=False,
                )
            if signals.empty:
                continue
            trades = v11._resolve_v7_entry_engine_signals(
                signals,
                label=f"market_relative_response_{setup}_{sl_pct:.2f}_{target_pct:.2f}",
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


def _period_metrics(frame: pd.DataFrame) -> dict[str, float | int]:
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


def _day_bootstrap(frame: pd.DataFrame, n_bootstrap: int = 5000) -> dict[str, float]:
    if frame.empty:
        return {"bootstrap_p_lte_zero": 1.0, "daily_pnl_ci_low": 0.0, "daily_pnl_ci_high": 0.0}
    daily = frame.groupby(frame["date"].dt.normalize())["pnl"].sum().to_numpy(dtype=float)
    if len(daily) < 5:
        return {"bootstrap_p_lte_zero": 1.0, "daily_pnl_ci_low": 0.0, "daily_pnl_ci_high": 0.0}
    rng = np.random.default_rng(RANDOM_SEED)
    indices = rng.integers(0, len(daily), size=(n_bootstrap, len(daily)))
    means = daily[indices].mean(axis=1)
    return {
        "bootstrap_p_lte_zero": float((means <= 0).mean()),
        "daily_pnl_ci_low": float(np.quantile(means, 0.025)),
        "daily_pnl_ci_high": float(np.quantile(means, 0.975)),
    }


def _profile_mask(
    frame: pd.DataFrame,
    side: str,
    profile: FilterProfile,
    start_minute: int,
    end_minute: int,
    require_trend: bool,
) -> pd.Series:
    rs_pct = pd.to_numeric(frame["rs_pct"], errors="coerce")
    response = pd.to_numeric(frame["response_divergence_pct"], errors="coerce")
    market_pre3 = pd.to_numeric(frame["market_pre3_ret_pct"], errors="coerce")
    stock_pre3 = pd.to_numeric(frame["stock_pre3_ret_pct"], errors="coerce")
    vol_ratio = pd.to_numeric(frame["vol_ratio"], errors="coerce")
    width = pd.to_numeric(frame["compression_width_atr"], errors="coerce")
    signal_minute = pd.to_numeric(frame["signal_minute"], errors="coerce")
    trend = frame["trend_aligned"].astype(str).str.lower().isin({"true", "1"})

    common = (
        signal_minute.between(start_minute, end_minute)
        & (vol_ratio >= profile.min_vol_ratio)
        & (width <= profile.max_compression_width_atr)
    )
    if require_trend:
        common &= trend

    if side == "LONG":
        return (
            common
            & (rs_pct >= profile.min_abs_rs_pct)
            & (response >= profile.min_response_divergence_pct)
            & (market_pre3 <= -profile.min_adverse_market_move_pct)
            & (stock_pre3 >= -profile.max_stock_giveback_pct)
        )
    return (
        common
        & (rs_pct <= -profile.min_abs_rs_pct)
        & (response <= -profile.min_response_divergence_pct)
        & (market_pre3 >= profile.min_adverse_market_move_pct)
        & (stock_pre3 <= profile.max_stock_giveback_pct)
    )


def _split(frame: pd.DataFrame, split: str) -> pd.DataFrame:
    if split == "train":
        return frame.loc[frame["date"] <= TRAIN_END]
    if split == "valid":
        return frame.loc[(frame["date"] >= VALID_START) & (frame["date"] <= VALID_END)]
    if split == "holdout":
        return frame.loc[(frame["date"] >= HOLDOUT_START) & (frame["date"] <= END_DATE)]
    raise ValueError(split)


def _configuration_rows(trades: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, pd.Series]]:
    rows: list[dict] = []
    masks: dict[str, pd.Series] = {}
    for setup, side in ((LONG_SETUP, "LONG"), (SHORT_SETUP, "SHORT")):
        setup_frame = trades.loc[trades["setup"].astype(str).eq(setup)]
        for profile, (window_name, start_minute, end_minute), require_trend, exit_rule in itertools.product(
            FILTER_PROFILES,
            TIME_WINDOWS,
            (False, True),
            EXIT_RULES,
        ):
            exit_key = f"{exit_rule[0]:.2f}/{exit_rule[1]:.2f}"
            base_mask = trades.index.isin(setup_frame.index) & trades["exit_key"].astype(str).eq(exit_key)
            filter_mask = _profile_mask(
                trades,
                side,
                profile,
                start_minute,
                end_minute,
                require_trend,
            )
            mask = pd.Series(base_mask, index=trades.index) & filter_mask
            config_id = (
                f"{setup}|{profile.name}|{window_name}|trend={int(require_trend)}|exit={exit_key}"
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
                "require_trend": require_trend,
                "exit_key": exit_key,
                "sl_pct": exit_rule[0],
                "target_pct": exit_rule[1],
                **asdict(profile),
            }
            for split in ("train", "valid", "holdout"):
                metrics = _period_metrics(_split(trades.loc[mask].copy(), split))
                for key, value in metrics.items():
                    row[f"{split}_{key}"] = value
            rows.append(row)
    return pd.DataFrame(rows), masks


def _select_configurations(
    configurations: pd.DataFrame,
    masks: dict[str, pd.Series],
    trades: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    selected_rows: list[dict] = []
    selected_trades: list[pd.DataFrame] = []

    for setup in SETUPS:
        pool = configurations.loc[
            configurations["setup"].eq(setup)
            & (configurations["train_trades"] >= 30)
            & (configurations["train_pf"] >= 1.25)
            & (configurations["train_pnl"] > 0)
            & (configurations["train_positive_month_pct"] >= 55.0)
        ].copy()
        if pool.empty:
            selected_rows.append({"setup": setup, "decision": "REJECT_NO_TRAIN_EDGE"})
            continue

        pool["train_score"] = (
            np.minimum(pool["train_pf"], 4.0) * 100.0
            + np.log1p(pool["train_trades"]) * 12.0
            + pool["train_positive_month_pct"]
            + np.maximum(pool["train_avg_trade"], 0.0) / 10.0
        )
        train_shortlist = pool.sort_values(
            ["train_score", "train_trades"], ascending=[False, False]
        ).head(10)
        valid_pass = train_shortlist.loc[
            (train_shortlist["valid_trades"] >= 10)
            & (train_shortlist["valid_pf"] >= 1.15)
            & (train_shortlist["valid_pnl"] > 0)
            & (train_shortlist["valid_positive_month_pct"] >= 50.0)
        ].copy()
        if valid_pass.empty:
            best = train_shortlist.iloc[0].to_dict()
            best["decision"] = "REJECT_VALIDATION"
            selected_rows.append(best)
            continue

        valid_pass["validation_score"] = (
            np.minimum(valid_pass["valid_pf"], 4.0) * 100.0
            + np.log1p(valid_pass["valid_trades"]) * 12.0
            + valid_pass["valid_positive_month_pct"]
            + np.maximum(valid_pass["valid_avg_trade"], 0.0) / 10.0
        )
        best = valid_pass.sort_values(
            ["validation_score", "valid_trades"], ascending=[False, False]
        ).iloc[0].to_dict()
        config_id = str(best["config_id"])
        chosen = trades.loc[masks[config_id]].copy()
        holdout = _split(chosen, "holdout")
        bootstrap = _day_bootstrap(holdout)
        best.update(bootstrap)
        holdout_pass = (
            int(best["holdout_trades"]) >= 20
            and float(best["holdout_pf"]) >= 1.20
            and float(best["holdout_pnl"]) > 0
            and float(bootstrap["bootstrap_p_lte_zero"]) <= 0.15
        )
        best["decision"] = "PROMISING_PROBATION" if holdout_pass else "REJECT_HOLDOUT"
        selected_rows.append(best)
        chosen["selected_config_id"] = config_id
        selected_trades.append(chosen)

    return (
        pd.DataFrame(selected_rows),
        pd.concat(selected_trades, ignore_index=True, sort=False) if selected_trades else pd.DataFrame(),
    )


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    raw_path = OUT_DIR / "market_relative_response_raw_candidates.csv"
    trades_path = OUT_DIR / "market_relative_response_resolved_trades.csv"

    if raw_path.exists():
        candidates = pd.read_csv(raw_path)
        print(f"[load] cached candidates={len(candidates):,}", flush=True)
    else:
        candidates = _scan_all()
        candidates.to_csv(raw_path, index=False)
        print(f"[scan] wrote {raw_path} rows={len(candidates):,}", flush=True)
    if candidates.empty:
        raise SystemExit("No market-relative-response candidates generated")

    candidates.groupby(["setup", "side"]).size().reset_index(name="raw_candidates").to_csv(
        OUT_DIR / "market_relative_response_raw_counts.csv", index=False
    )

    if trades_path.exists():
        trades = pd.read_csv(trades_path)
        trades["date"] = pd.to_datetime(trades["date"], errors="coerce")
        print(f"[load] cached resolved trades={len(trades):,}", flush=True)
    else:
        trades = _resolve_all(candidates)
        trades.to_csv(trades_path, index=False)
        print(f"[resolve] wrote {trades_path} rows={len(trades):,}", flush=True)
    if trades.empty:
        raise SystemExit("No market-relative-response trades resolved")

    configurations, masks = _configuration_rows(trades)
    configurations.to_csv(OUT_DIR / "market_relative_response_configurations.csv", index=False)
    selected, selected_trades = _select_configurations(configurations, masks, trades)
    selected.to_csv(OUT_DIR / "market_relative_response_selected.csv", index=False)
    selected_trades.to_csv(OUT_DIR / "market_relative_response_selected_trades.csv", index=False)

    metadata = {
        "data_root": str(DATA_ROOT),
        "start_date": str(START_DATE.date()),
        "train_end": str(TRAIN_END.date()),
        "validation": [str(VALID_START.date()), str(VALID_END.date())],
        "holdout": [str(HOLDOUT_START.date()), str(END_DATE.date())],
        "universe_mode": UNIVERSE_MODE,
        "filter_profiles": [asdict(profile) for profile in FILTER_PROFILES],
        "time_windows": TIME_WINDOWS,
        "exit_rules": EXIT_RULES,
        "candidate_count": int(len(candidates)),
        "resolved_trade_rows": int(len(trades)),
    }
    (OUT_DIR / "market_relative_response_run_metadata.json").write_text(
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
                    "require_trend",
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
