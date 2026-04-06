from __future__ import annotations

import argparse
import io
import json
import math
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from typing import Dict, Iterable, List, Tuple

import numpy as np
import pandas as pd

import avwap_combined_runner_v16_5min as base


ROOT = Path(__file__).resolve().parent
OUT_DIR = ROOT / "outputs_v16_5min_sltarget_search"
ROUNDTRIP_COST_PCT_FALLBACK = 0.16


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Search v16_5min short/long SL+target settings")
    p.add_argument("--validate-top", type=int, default=4, help="Exact-validate top N combined candidates")
    p.add_argument("--top-k-side", type=int, default=8, help="Number of top short/long candidates used to form pair grid")
    p.add_argument("--skip-exact", action="store_true", help="Skip exact full-run validation")
    return p.parse_args()


def _latest_v16_csv() -> Path:
    files = sorted(
        base.runtime_dir("outputs_v16_5min").glob("avwap_longshort_trades_v16_5min_ALL_DAYS_*.csv"),
        key=lambda p: p.stat().st_mtime,
    )
    if not files:
        raise FileNotFoundError("No v16_5min trade CSV found in outputs_v16_5min")
    return files[-1]


def _profit_factor(pnl: pd.Series) -> float:
    pos = float(pnl[pnl > 0].sum())
    neg = float(-pnl[pnl < 0].sum())
    if neg <= 0:
        return math.inf if pos > 0 else 0.0
    return pos / neg


def _max_drawdown(pnl: pd.Series) -> float:
    if pnl.empty:
        return 0.0
    equity = pnl.cumsum()
    dd = equity.cummax() - equity
    return float(dd.max())


def _calc_metrics(df: pd.DataFrame) -> Dict[str, float]:
    if df.empty:
        return {
            "trades": 0,
            "trade_days": 0,
            "trades_per_day": 0.0,
            "sum_pnl_pct": 0.0,
            "avg_pnl_pct": 0.0,
            "trade_win_pct": 0.0,
            "day_win_pct": 0.0,
            "profit_factor": 0.0,
            "max_drawdown_pct": 0.0,
            "sl_rate_pct": 0.0,
            "target_rate_pct": 0.0,
            "eod_rate_pct": 0.0,
            "sum_pnl_rs": 0.0,
        }
    work = df.sort_values(["entry_time_ist", "ticker"], kind="mergesort").copy()
    pnl = work["pnl_pct"].astype(float)
    day_pnl = work.groupby("trade_date", sort=True)["pnl_pct"].sum()
    outcome = work["outcome"].astype(str).str.upper()
    return {
        "trades": int(len(work)),
        "trade_days": int(work["trade_date"].nunique()),
        "trades_per_day": round(float(len(work) / max(work["trade_date"].nunique(), 1)), 2),
        "sum_pnl_pct": round(float(pnl.sum()), 4),
        "avg_pnl_pct": round(float(pnl.mean()), 4),
        "trade_win_pct": round(float((pnl > 0).mean() * 100.0), 2),
        "day_win_pct": round(float((day_pnl > 0).mean() * 100.0), 2),
        "profit_factor": round(float(_profit_factor(pnl)), 3),
        "max_drawdown_pct": round(float(_max_drawdown(pnl)), 4),
        "sl_rate_pct": round(float((outcome == "SL").mean() * 100.0), 2),
        "target_rate_pct": round(float((outcome == "TARGET").mean() * 100.0), 2),
        "eod_rate_pct": round(float((outcome == "EOD").mean() * 100.0), 2),
        "sum_pnl_rs": round(float(work["pnl_rs"].astype(float).sum()), 2),
    }


def _score_candidates(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    metrics = [
        ("sum_pnl_pct", False, 0.35),
        ("profit_factor", False, 0.30),
        ("day_win_pct", False, 0.20),
        ("trade_win_pct", False, 0.10),
        ("max_drawdown_pct", True, 0.05),
    ]
    out = df.copy()
    score = np.zeros(len(out), dtype=float)
    for col, invert, weight in metrics:
        s = out[col].astype(float)
        lo, hi = float(s.min()), float(s.max())
        if hi == lo:
            norm = np.full(len(out), 100.0, dtype=float)
        else:
            if invert:
                norm = (hi - s.to_numpy()) / (hi - lo) * 100.0
            else:
                norm = (s.to_numpy() - lo) / (hi - lo) * 100.0
        out[f"norm_{col}"] = np.round(norm, 2)
        score += norm * weight
    out["score"] = np.round(score, 2)
    return out.sort_values(["score", "sum_pnl_pct", "profit_factor", "day_win_pct"], ascending=[False, False, False, False])


def _load_bar_cache(dir_path: Path, tickers: Iterable[str], suffixes: List[str]) -> Dict[str, pd.DataFrame]:
    cache: Dict[str, pd.DataFrame] = {}
    for ticker in sorted(set(tickers)):
        df = pd.DataFrame()
        for suffix in suffixes:
            path = dir_path / f"{ticker}{suffix}"
            if path.exists():
                df = base.read_5m_parquet(str(path))
                break
        if not df.empty and "datetime" in df.columns:
            df = df.sort_values("datetime").reset_index(drop=True)
        cache[ticker] = df
    return cache


def _load_1m_cache(tickers: Iterable[str]) -> Dict[str, pd.DataFrame]:
    return _load_bar_cache(
        base._resolve_5min_dir(),
        tickers,
        [
            "_stocks_indicators_1min.parquet",
            ".parquet",
            "_1min.parquet",
            "_5min.parquet",
            "_stocks_indicators_5min.parquet",
        ],
    )


def _load_5m_cache(tickers: Iterable[str]) -> Dict[str, pd.DataFrame]:
    return _load_bar_cache(
        base._resolve_15m_dir(),
        tickers,
        [
            "_stocks_indicators_5min.parquet",
            ".parquet",
            "_5min.parquet",
            "_stocks_indicators_1min.parquet",
            "_1min.parquet",
        ],
    )


def _build_trade_paths(
    trades: pd.DataFrame,
    cache_1m: Dict[str, pd.DataFrame],
    cache_5m: Dict[str, pd.DataFrame],
) -> List[dict]:
    rows: List[dict] = []
    for row in trades.itertuples(index=False):
        ticker = str(row.ticker)
        entry_time = pd.Timestamp(row.entry_time_ist)
        trade_day = entry_time.normalize()
        bars = pd.DataFrame()

        df_1m = cache_1m.get(ticker)
        if df_1m is not None and not df_1m.empty:
            mask = (df_1m["datetime"] > entry_time) & (df_1m["datetime"].dt.normalize() == trade_day)
            bars = df_1m.loc[mask, ["datetime", "high", "low", "close"]]

        if bars.empty:
            df_5m = cache_5m.get(ticker)
            if df_5m is not None and not df_5m.empty:
                mask = (df_5m["datetime"] > entry_time) & (df_5m["datetime"].dt.normalize() == trade_day)
                bars = df_5m.loc[mask, ["datetime", "high", "low", "close"]]
                if bars.empty:
                    same_day = df_5m.loc[df_5m["datetime"].dt.normalize() == trade_day, ["datetime", "high", "low", "close"]]
                    bars = same_day.tail(1)

        if bars.empty:
            continue
        cost_pct = np.nan
        if hasattr(row, "pnl_pct_gross_price") and hasattr(row, "pnl_pct_price"):
            cost_pct = float(getattr(row, "pnl_pct_gross_price", np.nan)) - float(getattr(row, "pnl_pct_price", np.nan))
        if not np.isfinite(cost_pct) and hasattr(row, "pnl_pct_gross") and hasattr(row, "pnl_pct"):
            lev = float(getattr(row, "leverage", np.nan))
            gross = float(getattr(row, "pnl_pct_gross", np.nan))
            net = float(getattr(row, "pnl_pct", np.nan))
            if np.isfinite(lev) and lev > 0 and np.isfinite(gross) and np.isfinite(net):
                cost_pct = (gross - net) / lev
        if not np.isfinite(cost_pct):
            cost_pct = ROUNDTRIP_COST_PCT_FALLBACK
        leverage = float(getattr(row, "leverage", np.nan))
        if not np.isfinite(leverage) or leverage <= 0:
            leverage = 5.0
        rows.append(
            {
                "ticker": ticker,
                "trade_date": pd.Timestamp(row.trade_date).date(),
                "entry_time_ist": entry_time,
                "side": str(row.side).upper(),
                "entry_price": float(row.entry_price),
                "position_size_rs": float(getattr(row, "position_size_rs", 50000.0) or 50000.0),
                "leverage": leverage,
                "quality_score": float(getattr(row, "quality_score", np.nan)),
                "cost_pct": float(cost_pct),
                "high": bars["high"].to_numpy(dtype=float, copy=True),
                "low": bars["low"].to_numpy(dtype=float, copy=True),
                "close": bars["close"].to_numpy(dtype=float, copy=True),
                "times": bars["datetime"].to_numpy(copy=True),
            }
        )
    return rows


def _simulate_trade(path: dict, stop_pct: float, target_pct: float) -> dict:
    entry = float(path["entry_price"])
    hi = path["high"]
    lo = path["low"]
    close = path["close"]
    times = path["times"]
    side = path["side"]

    if side == "SHORT":
        stop_price = entry * (1.0 + stop_pct)
        target_price = entry * (1.0 - target_pct)
        stop_hits = np.flatnonzero(hi >= stop_price)
        tgt_hits = np.flatnonzero(lo <= target_price)
    else:
        stop_price = entry * (1.0 - stop_pct)
        target_price = entry * (1.0 + target_pct)
        stop_hits = np.flatnonzero(lo <= stop_price)
        tgt_hits = np.flatnonzero(hi >= target_price)

    stop_idx = int(stop_hits[0]) if len(stop_hits) else None
    tgt_idx = int(tgt_hits[0]) if len(tgt_hits) else None

    if stop_idx is not None and (tgt_idx is None or stop_idx <= tgt_idx):
        exit_idx = stop_idx
        exit_price = stop_price
        outcome = "SL"
    elif tgt_idx is not None:
        exit_idx = tgt_idx
        exit_price = target_price
        outcome = "TARGET"
    else:
        exit_idx = len(close) - 1
        exit_price = float(close[-1])
        outcome = "EOD"

    if side == "SHORT":
        raw_price_pct = (entry - exit_price) / entry * 100.0 if entry else 0.0
    else:
        raw_price_pct = (exit_price - entry) / entry * 100.0 if entry else 0.0
    net_price_pct = raw_price_pct - float(path["cost_pct"])
    leverage = float(path["leverage"])
    net_pct = net_price_pct * leverage
    gross_pct = raw_price_pct * leverage
    pnl_rs = float(path["position_size_rs"]) * net_pct / 100.0

    return {
        "ticker": path["ticker"],
        "trade_date": path["trade_date"],
        "entry_time_ist": path["entry_time_ist"],
        "side": side,
        "exit_time_ist": pd.Timestamp(times[exit_idx]),
        "exit_price": float(exit_price),
        "outcome": outcome,
        "pnl_pct": float(net_pct),
        "pnl_pct_gross": float(gross_pct),
        "pnl_rs": float(pnl_rs),
        "position_size_rs": float(path["position_size_rs"]),
        "leverage": leverage,
        "quality_score": float(path["quality_score"]),
    }


def _evaluate_side(paths: List[dict], stop_pct: float, target_pct: float) -> Tuple[Dict[str, float], pd.DataFrame]:
    trades = pd.DataFrame(_simulate_trade(path, stop_pct, target_pct) for path in paths)
    metrics = _calc_metrics(trades)
    metrics.update(
        {
            "stop_pct": round(stop_pct, 6),
            "target_pct": round(target_pct, 6),
            "rr": round(target_pct / stop_pct, 3) if stop_pct > 0 else 0.0,
        }
    )
    return metrics, trades


def _grid_values(start: float, stop: float, step: float) -> List[float]:
    vals = []
    cur = start
    while cur <= stop + (step / 10.0):
        vals.append(round(cur, 6))
        cur += step
    return vals


def _run_side_grid(paths: List[dict], sl_values: List[float], tgt_values: List[float]) -> Tuple[pd.DataFrame, Dict[Tuple[float, float], pd.DataFrame]]:
    rows = []
    detail: Dict[Tuple[float, float], pd.DataFrame] = {}
    for sl in sl_values:
        for tgt in tgt_values:
            metrics, trades = _evaluate_side(paths, sl, tgt)
            rows.append(metrics)
            detail[(sl, tgt)] = trades
    scored = _score_candidates(pd.DataFrame(rows))
    return scored, detail


def _refine_grid(scored: pd.DataFrame, side: str) -> Tuple[List[float], List[float]]:
    seeds = pd.concat(
        [
            scored.head(5),
            scored.nlargest(3, "sum_pnl_pct"),
            scored.nlargest(3, "profit_factor"),
            scored.nlargest(3, "day_win_pct"),
        ],
        ignore_index=True,
    ).drop_duplicates(subset=["stop_pct", "target_pct"])
    sl_vals = set()
    tgt_vals = set()
    if side == "SHORT":
        sl_step = 0.0002
        tgt_step = 0.00025
        sl_lo, sl_hi = 0.0050, 0.0090
        tgt_lo, tgt_hi = 0.0065, 0.0130
    else:
        sl_step = 0.0002
        tgt_step = 0.00025
        sl_lo, sl_hi = 0.0045, 0.0092
        tgt_lo, tgt_hi = 0.0065, 0.0130
    for row in seeds.itertuples(index=False):
        for delta in (-2, -1, 0, 1, 2):
            sl_vals.add(round(min(sl_hi, max(sl_lo, row.stop_pct + (delta * sl_step))), 6))
            tgt_vals.add(round(min(tgt_hi, max(tgt_lo, row.target_pct + (delta * tgt_step))), 6))
    return sorted(sl_vals), sorted(tgt_vals)


def _combine_pair(short_df: pd.DataFrame, long_df: pd.DataFrame, s_cfg: Tuple[float, float], l_cfg: Tuple[float, float]) -> Dict[str, float]:
    combined = pd.concat([short_df, long_df], ignore_index=True)
    metrics = _calc_metrics(combined)
    metrics.update(
        {
            "s_stop_pct": round(s_cfg[0], 6),
            "s_target_pct": round(s_cfg[1], 6),
            "l_stop_pct": round(l_cfg[0], 6),
            "l_target_pct": round(l_cfg[1], 6),
            "s_rr": round(s_cfg[1] / s_cfg[0], 3) if s_cfg[0] > 0 else 0.0,
            "l_rr": round(l_cfg[1] / l_cfg[0], 3) if l_cfg[0] > 0 else 0.0,
        }
    )
    return metrics


def _score_pairs(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    out = df.copy()
    specs = [
        ("sum_pnl_pct", False, 0.30),
        ("profit_factor", False, 0.25),
        ("day_win_pct", False, 0.20),
        ("trade_win_pct", False, 0.10),
        ("max_drawdown_pct", True, 0.10),
        ("trades_per_day", False, 0.05),
    ]
    score = np.zeros(len(out), dtype=float)
    for col, invert, weight in specs:
        s = out[col].astype(float)
        lo, hi = float(s.min()), float(s.max())
        if hi == lo:
            norm = np.full(len(out), 100.0, dtype=float)
        else:
            if invert:
                norm = (hi - s.to_numpy()) / (hi - lo) * 100.0
            else:
                norm = (s.to_numpy() - lo) / (hi - lo) * 100.0
        out[f"norm_{col}"] = np.round(norm, 2)
        score += norm * weight
    out["score"] = np.round(score, 2)
    return out.sort_values(["score", "sum_pnl_pct", "profit_factor", "day_win_pct"], ascending=[False, False, False, False])


def _configure_exact_runner(
    s_sl: float,
    s_tgt: float,
    l_sl: float,
    l_tgt: float,
    out_dir: Path,
):
    short_cfg = base.default_short_config(reports_dir=out_dir)
    long_cfg = base.default_long_config_v9(reports_dir=out_dir)

    dir_15m = base._resolve_15m_dir()
    short_cfg.dir_15m = str(dir_15m)
    long_cfg.dir_15m = str(dir_15m)
    short_cfg.end_15m = "_stocks_indicators_5min.parquet"
    long_cfg.end_15m = "_stocks_indicators_5min.parquet"
    short_cfg.market_regime_tickers = tuple(base.NIFTY_CONTEXT_TICKERS)
    long_cfg.market_regime_tickers = tuple(base.NIFTY_CONTEXT_TICKERS)

    short_cfg, long_cfg = base.apply_live_parity_profile(short_cfg, long_cfg)
    short_cfg.stop_pct = float(s_sl)
    short_cfg.target_pct = float(s_tgt)
    long_cfg.stop_pct = float(l_sl)
    long_cfg.target_pct = float(l_tgt)

    vix_map = base._load_india_vix(ROOT)
    short_cfg.vix_scale_enabled = base.VIX_SCALE_ENABLED
    long_cfg.vix_scale_enabled = base.VIX_SCALE_ENABLED
    if base.VIX_SCALE_ENABLED and vix_map:
        short_cfg.vix_daily = vix_map
        short_cfg.vix_baseline = base.VIX_BASELINE
        short_cfg.vix_scale_min = base.VIX_SCALE_MIN
        short_cfg.vix_scale_max = base.VIX_SCALE_MAX
        short_cfg.vix_scale_target = base.VIX_SCALE_TARGET
        short_cfg.vix_scale_sl = base.VIX_SCALE_SL
        long_cfg.vix_daily = vix_map
        long_cfg.vix_baseline = base.VIX_BASELINE
        long_cfg.vix_scale_min = base.VIX_SCALE_MIN
        long_cfg.vix_scale_max = base.VIX_SCALE_MAX
        long_cfg.vix_scale_target = base.VIX_SCALE_TARGET
        long_cfg.vix_scale_sl = base.VIX_SCALE_SL

    regime_map, regime_source = base.build_market_regime_map(short_cfg)
    if regime_map:
        short_cfg.market_regime_map = regime_map
        long_cfg.market_regime_map = regime_map
        short_cfg.enable_market_regime_filter = True
        long_cfg.enable_market_regime_filter = True
    else:
        regime_source = ""
        short_cfg.enable_market_regime_filter = False
        long_cfg.enable_market_regime_filter = False

    return short_cfg, long_cfg, regime_source


def _run_exact_pair(
    s_sl: float,
    s_tgt: float,
    l_sl: float,
    l_tgt: float,
    label: str,
) -> Dict[str, object]:
    run_dir = OUT_DIR / label
    run_dir.mkdir(parents=True, exist_ok=True)
    short_cfg, long_cfg, regime_source = _configure_exact_runner(
        s_sl,
        s_tgt,
        l_sl,
        l_tgt,
        run_dir,
    )
    dir_1m = base._resolve_5min_dir()

    buffer = io.StringIO()
    with redirect_stdout(buffer), redirect_stderr(buffer):
        short_df = base._run_side_parallel("SHORT", short_cfg, base.MAX_WORKERS)
        long_df = base._run_side_parallel("LONG", long_cfg, base.MAX_WORKERS)

        if base.NIFTY_CONTEXT_ENABLED:
            mode_map, nifty_ret_map, context_src, context_counts = base._build_nifty_intraday_context(short_cfg)
            if mode_map:
                short_df, long_df = base._apply_nifty_intraday_context(short_df, long_df, short_cfg, mode_map, nifty_ret_map)
            else:
                context_src = ""
                context_counts = {}
        else:
            context_src = ""
            context_counts = {}

        short_df, long_df = base._replace_current_day_with_live_parity(short_df, long_df)
        short_df, long_df = base._apply_v16_post_scan_filters(short_df, long_df)

        suffix = ".parquet"
        if dir_1m.is_dir():
            sample_files = list(dir_1m.glob("*"))[:5]
            for sf in sample_files:
                if sf.suffix:
                    suffix = sf.suffix
                    break
        if not short_df.empty:
            short_df = base._resolve_exits_5min(
                short_df,
                dir_1m,
                suffix,
                short_cfg.parquet_engine,
                eod_exit_time=base.V15_EOD_EXIT_TIME,
            )
            short_df = base._add_notional_pnl(base._sort_trades_for_output(short_df))
        if not long_df.empty:
            long_df = base._resolve_exits_5min(
                long_df,
                dir_1m,
                suffix,
                long_cfg.parquet_engine,
                eod_exit_time=base.V15_EOD_EXIT_TIME,
            )
            long_df = base._add_notional_pnl(base._sort_trades_for_output(long_df))

    combined = pd.concat([short_df, long_df], ignore_index=True)
    combined = base._add_notional_pnl(base._sort_trades_for_output(combined))
    short_df = combined[combined["side"].astype(str).str.upper().eq("SHORT")].copy()
    long_df = combined[combined["side"].astype(str).str.upper().eq("LONG")].copy()

    csv_path = run_dir / f"exact_{label}.csv"
    combined.to_csv(csv_path, index=False)
    (run_dir / f"exact_{label}.log").write_text(buffer.getvalue(), encoding="utf-8")

    return {
        "label": label,
        "csv": str(csv_path),
        "log": str(run_dir / f"exact_{label}.log"),
        "short": _calc_metrics(short_df),
        "long": _calc_metrics(long_df),
        "combined": _calc_metrics(combined),
        "params": {
            "short_stop_pct": round(s_sl, 6),
            "short_target_pct": round(s_tgt, 6),
            "long_stop_pct": round(l_sl, 6),
            "long_target_pct": round(l_tgt, 6),
            "regime_source": regime_source,
        },
    }


def main() -> None:
    args = _parse_args()
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    trades_csv = _latest_v16_csv()
    trades = pd.read_csv(trades_csv)
    trades["trade_date"] = pd.to_datetime(trades["trade_date"]).dt.date
    trades["entry_time_ist"] = pd.to_datetime(trades["entry_time_ist"])

    tickers = trades["ticker"].astype(str).unique()
    cache_1m = _load_1m_cache(tickers)
    cache_5m = _load_5m_cache(tickers)
    short_paths = _build_trade_paths(
        trades[trades["side"].astype(str).str.upper().eq("SHORT")].copy(),
        cache_1m,
        cache_5m,
    )
    long_paths = _build_trade_paths(
        trades[trades["side"].astype(str).str.upper().eq("LONG")].copy(),
        cache_1m,
        cache_5m,
    )

    short_sl_broad = _grid_values(0.0055, 0.0090, 0.0005)
    short_tgt_broad = _grid_values(0.0065, 0.0125, 0.0005)
    long_sl_broad = _grid_values(0.0045, 0.0090, 0.0005)
    long_tgt_broad = _grid_values(0.0065, 0.0125, 0.0005)

    short_broad, short_detail = _run_side_grid(short_paths, short_sl_broad, short_tgt_broad)
    long_broad, long_detail = _run_side_grid(long_paths, long_sl_broad, long_tgt_broad)

    short_sl_refine, short_tgt_refine = _refine_grid(short_broad, "SHORT")
    long_sl_refine, long_tgt_refine = _refine_grid(long_broad, "LONG")

    short_refine, short_detail_refine = _run_side_grid(short_paths, short_sl_refine, short_tgt_refine)
    long_refine, long_detail_refine = _run_side_grid(long_paths, long_sl_refine, long_tgt_refine)

    short_all = pd.concat([short_broad, short_refine], ignore_index=True).drop_duplicates(subset=["stop_pct", "target_pct"])
    long_all = pd.concat([long_broad, long_refine], ignore_index=True).drop_duplicates(subset=["stop_pct", "target_pct"])
    short_all = _score_candidates(short_all)
    long_all = _score_candidates(long_all)

    short_detail.update(short_detail_refine)
    long_detail.update(long_detail_refine)

    short_all.to_csv(OUT_DIR / "short_candidates.csv", index=False)
    long_all.to_csv(OUT_DIR / "long_candidates.csv", index=False)

    top_short = short_all.head(int(args.top_k_side))
    top_long = long_all.head(int(args.top_k_side))
    pair_rows = []
    for s_row in top_short.itertuples(index=False):
        s_cfg = (float(s_row.stop_pct), float(s_row.target_pct))
        s_df = short_detail[s_cfg]
        for l_row in top_long.itertuples(index=False):
            l_cfg = (float(l_row.stop_pct), float(l_row.target_pct))
            l_df = long_detail[l_cfg]
            pair_rows.append(_combine_pair(s_df, l_df, s_cfg, l_cfg))
    pair_df = _score_pairs(pd.DataFrame(pair_rows).drop_duplicates(subset=["s_stop_pct", "s_target_pct", "l_stop_pct", "l_target_pct"]))
    pair_df.to_csv(OUT_DIR / "pair_candidates_approx.csv", index=False)

    exact_results = []
    if not args.skip_exact and args.validate_top > 0:
        for idx, row in enumerate(pair_df.head(int(args.validate_top)).itertuples(index=False), 1):
            label = (
                f"rank{idx}_sSL{int(round(row.s_stop_pct * 10000)):04d}_sTGT{int(round(row.s_target_pct * 10000)):04d}_"
                f"lSL{int(round(row.l_stop_pct * 10000)):04d}_lTGT{int(round(row.l_target_pct * 10000)):04d}"
            )
            exact_results.append(
                _run_exact_pair(
                    s_sl=float(row.s_stop_pct),
                    s_tgt=float(row.s_target_pct),
                    l_sl=float(row.l_stop_pct),
                    l_tgt=float(row.l_target_pct),
                    label=label,
                )
            )
        with (OUT_DIR / "exact_validation.json").open("w", encoding="utf-8") as fh:
            json.dump(exact_results, fh, indent=2)

    summary = {
        "source_v16_5min_csv": str(trades_csv),
        "baseline": {
            "short": _calc_metrics(trades[trades["side"].astype(str).str.upper().eq("SHORT")].copy()),
            "long": _calc_metrics(trades[trades["side"].astype(str).str.upper().eq("LONG")].copy()),
            "combined": _calc_metrics(trades.copy()),
        },
        "top_short_approx": short_all.head(10).to_dict(orient="records"),
        "top_long_approx": long_all.head(10).to_dict(orient="records"),
        "top_pairs_approx": pair_df.head(10).to_dict(orient="records"),
        "exact_validation": exact_results,
    }
    with (OUT_DIR / "summary.json").open("w", encoding="utf-8") as fh:
        json.dump(summary, fh, indent=2)

    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
