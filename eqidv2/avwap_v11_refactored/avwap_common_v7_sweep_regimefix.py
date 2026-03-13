# -*- coding: utf-8 -*-
"""
Runner-level regime fix wrapper around avwap_common_v7_sweep.

Keeps shared v7 sweep code unchanged while exposing a safer market-regime
builder for combined runners that would otherwise pick an all-neutral index
feed.
"""

from __future__ import annotations

from avwap_v11_refactored.avwap_common_v7_sweep import *  # noqa: F401,F403
from avwap_v11_refactored.avwap_common_v7_sweep import (
    StrategyConfig,
    _ts_to_key,
    compute_day_avwap,
    in_session,
    prepare_indicators,
    read_15m_parquet,
)


def build_market_regime_map(cfg: StrategyConfig) -> Tuple[Dict[str, int], str]:
    """
    Build regime map from the first usable market index parquet.

    Some index feeds provide zero volume for all bars. That makes AVWAP
    undefined and yields an all-neutral regime map, which would otherwise
    block every trade when `market_regime_allow_neutral=False`.
    """
    for ticker_found in cfg.market_regime_tickers:
        try:
            p = Path(cfg.dir_15m) / f"{ticker_found}{cfg.end_15m}"
            if not p.exists():
                continue

            df_idx = read_15m_parquet(str(p), cfg.parquet_engine)
            if df_idx.empty:
                continue
            df_idx = df_idx[df_idx["date"].apply(lambda ts: in_session(ts, cfg))].copy()
            if df_idx.empty:
                continue
            df_idx = prepare_indicators(df_idx.sort_values("date").reset_index(drop=True), cfg)

            per_day: List[pd.DataFrame] = []
            for _, g in df_idx.groupby("day", sort=True):
                g2 = g.copy().reset_index(drop=True)
                g2["AVWAP"] = compute_day_avwap(g2)
                per_day.append(g2)
            if not per_day:
                continue

            idx = pd.concat(per_day, ignore_index=True)
            close = pd.to_numeric(idx["close"], errors="coerce")
            ema20 = pd.to_numeric(idx["EMA20"], errors="coerce")
            avwap = pd.to_numeric(idx["AVWAP"], errors="coerce")
            rsi = pd.to_numeric(idx["RSI15"], errors="coerce")

            long_bias = (close > ema20) & (close > avwap) & (rsi > 50.0)
            short_bias = (close < ema20) & (close < avwap) & (rsi < 50.0)
            bias = np.where(long_bias, 1, np.where(short_bias, -1, 0))
            if not np.any(bias != 0):
                continue

            out: Dict[str, int] = {}
            for ts, b in zip(idx["date"], bias):
                if pd.isna(ts):
                    continue
                out[_ts_to_key(ts)] = int(b)
            if out:
                return out, ticker_found
        except Exception:
            continue
    return {}, ""
