"""Causal morning momentum features for the frozen V14 candidate pool.

Every market reading is from the exact completed Nifty-futures candle at the
candidate's signal clock.  Missing candles and nonadjacent stock bars produce
NaN features with false readiness rather than a forward-filled direction.
This module adds features only; it does not select trades or inspect outcomes.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from research import g4_nifty_direction_backtest as nifty
from research import g14_oi_day_or_permissions as sources


IST = "Asia/Kolkata"
CLOCKS = frozenset(("09:20", "09:25", "09:30", "09:35", "09:40", "09:45"))
REQUIRED = frozenset((
    "sid", "day", "signal_ts", "tradingsymbol", "side", "contract_month",
    "open", "close", "prev_close", "price_change_pct", "source_1m_count",
    "v18_stock_from_open_pct", "v18_stock_ready",
))
OUTPUT_NUMERIC = (
    "mom_nifty_session_pct", "mom_nifty_bar_pct", "mom_stock_bar_pct",
    "mom_stock_prev_bar_pct", "mom_stock_from_open_pct",
    "mom_nifty_session_signed_pct", "mom_nifty_bar_signed_pct",
    "mom_stock_bar_signed_pct", "mom_stock_prev_bar_signed_pct",
    "mom_stock_relative_bar_signed_pct", "mom_stock_relative_open_signed_pct",
)


def _stamps(values: pd.Series) -> pd.Series:
    if any(pd.Timestamp(value).tzinfo is None for value in values.dropna()):
        raise ValueError("V14 momentum signal timestamps must be timezone-aware")
    return pd.to_datetime(values, errors="raise", utc=True).dt.tz_convert(IST)


def _num(frame: pd.DataFrame, name: str) -> pd.Series:
    return pd.to_numeric(frame[name], errors="coerce")


def _true(values: pd.Series) -> pd.Series:
    return values.astype(str).str.lower().isin(("true", "1", "1.0"))


def enrich_day(candidates: pd.DataFrame, day: str) -> pd.DataFrame:
    """Return the same candidate rows/order with SID-keyed causal features.

    Readiness is separate for current Nifty, stock impulse, prior stock bar,
    and stock-versus-Nifty session-open relative strength. The V18 stock-open
    value is accepted only when its completed-signal value matches the value
    recomputed from that stock's exact 09:20 opening candle. At 09:20 there is
    deliberately no prior completed stock five-minute bar in this session.
    """
    out = candidates.copy(deep=True)
    for field in OUTPUT_NUMERIC:
        out[field] = np.nan
    for field in ("mom_nifty_ready", "mom_stock_bar_ready", "mom_stock_prev_bar_ready",
                  "mom_relative_bar_ready", "mom_relative_open_ready"):
        out[field] = False
    out["mom_nifty_status"] = "UNAVAILABLE"
    out["mom_nifty_contract"] = ""
    out["mom_nifty_source_sha256"] = ""
    out["mom_nifty_bar_ts"] = ""
    out["mom_stock_open_source"] = "UNAVAILABLE"
    out["mom_feature_ts"] = pd.NaT
    if out.empty:
        return out
    missing = REQUIRED - set(out)
    if missing:
        raise ValueError(f"V14 momentum candidate fields missing: {sorted(missing)}")
    if out.sid.isna().any() or out.sid.duplicated().any():
        raise ValueError("V14 momentum requires unique source SID")
    stamp = _stamps(out.signal_ts)
    if (out.day.astype(str).str[:10] != day).any() or not stamp.dt.strftime("%Y-%m-%d").eq(day).all():
        raise ValueError("V14 momentum candidates must belong to exactly the requested day")
    if out.assign(_signal=stamp).duplicated(["tradingsymbol", "_signal"]).any():
        raise ValueError("Duplicate stock signal clock in V14 momentum source")
    out["mom_feature_ts"] = stamp
    clock = stamp.dt.strftime("%H:%M")
    exact_clock = (clock.isin(CLOCKS) & stamp.eq(stamp.dt.floor("min"))
                   & stamp.eq(stamp.dt.floor("5min")))
    sign = pd.Series(np.where(out.side.eq("LONG"), 1.,
                              np.where(out.side.eq("SHORT"), -1., np.nan)), index=out.index)
    price = _num(out, "price_change_pct")
    raw_open, raw_close, prev_close = (_num(out, field) for field in ("open", "close", "prev_close"))
    count5 = _num(out, "source_1m_count").eq(5)
    opening = clock.eq("09:20")
    basis = pd.Series(np.where(opening,
                               100 * (raw_close / raw_open - 1),
                               100 * (raw_close / prev_close - 1)), index=out.index)
    basis_match = np.isclose(price.to_numpy(float), basis.to_numpy(float),
                             rtol=0, atol=1e-4, equal_nan=False)
    direction_match = ((price.gt(0) & sign.eq(1)) | (price.lt(0) & sign.eq(-1)))
    valid_stock = (exact_clock & count5 & direction_match & np.isfinite(price)
                   & np.isfinite(raw_open) & raw_open.gt(0)
                   & np.isfinite(raw_close) & raw_close.gt(0)
                   & basis_match)
    # `opening_0920_status` also covers futures OI evidence. A genuine cash
    # opening bar can anchor stock-from-open momentum even when that OI proof
    # is absent; V14's independent entry integrity gate handles OI eligibility.
    if "price_change_basis" in out:
        valid_stock &= ~opening | out.price_change_basis.eq("09:15_CASH_OPEN_TO_09:20_CASH_CLOSE")
    out["mom_stock_bar_ready"] = valid_stock.fillna(False).astype(bool)
    out.loc[valid_stock, "mom_stock_bar_pct"] = price.loc[valid_stock]
    out.loc[valid_stock, "mom_stock_bar_signed_pct"] = (sign * price).loc[valid_stock]

    # The exact 09:20 cash open provides a timestamp-verified stock session
    # reference. Do not accept a V18 return from an absent opening bar.
    opening_rows = out.loc[opening & valid_stock, ["tradingsymbol"]].copy()
    opening_rows["_opening_cash"] = raw_open.loc[opening & valid_stock]
    opening_cash = out.tradingsymbol.map(
        opening_rows.set_index("tradingsymbol")._opening_cash)
    stock_open_expected = 100 * (raw_close / opening_cash - 1)
    stock_open_source = _num(out, "v18_stock_from_open_pct")
    opening_evidence = valid_stock & np.isfinite(opening_cash) & opening_cash.gt(0)
    supplied = out.v18_stock_ready.notna() | stock_open_source.notna()
    v18_verified = (opening_evidence & _true(out.v18_stock_ready)
                    & np.isfinite(stock_open_source)
                    & np.isclose(stock_open_source.to_numpy(float),
                                 stock_open_expected.to_numpy(float),
                                 rtol=0, atol=1e-4, equal_nan=False))
    # The recovered 09:40/09:45 candidate rows have exact completed cash
    # OHLC but no V18 enrichment. Their current close and exact valid 09:20
    # cash open suffice for a same-session return; do not override an explicit
    # V18 invalid/mismatched value with this fallback.
    raw_reconstructed = opening_evidence & ~supplied
    stock_open_valid = v18_verified | raw_reconstructed
    stock_open_value = pd.Series(np.where(v18_verified, stock_open_source,
                                           np.where(raw_reconstructed, stock_open_expected, np.nan)),
                                 index=out.index)
    out.loc[stock_open_valid, "mom_stock_from_open_pct"] = stock_open_value.loc[stock_open_valid]
    out.loc[v18_verified, "mom_stock_open_source"] = "V18_VERIFIED"
    out.loc[raw_reconstructed, "mom_stock_open_source"] = "RAW_OPEN_CLOSE_RECONSTRUCTED"

    # Exact adjacent stock clocks only; a stale prior row is never a recent
    # impulse. Check the source's previous close against that prior completed
    # close as an additional identity guard.
    work = pd.DataFrame({
        "_pos": np.arange(len(out)), "symbol": out.tradingsymbol.to_numpy(),
        "stamp": stamp.to_numpy(), "close": raw_close.to_numpy(float),
        "prev_close": prev_close.to_numpy(float), "price": price.to_numpy(float),
        "valid": valid_stock.to_numpy(bool),
    }).sort_values(["symbol", "stamp"], kind="stable")
    group = work.groupby("symbol", sort=False)
    previous_stamp = group.stamp.shift()
    previous_close = group.close.shift()
    previous_price = group.price.shift()
    previous_valid = group.valid.shift().eq(True)
    adjacent = (work.stamp - previous_stamp).eq(pd.Timedelta(minutes=5))
    close_match = np.isclose(work.prev_close.to_numpy(float),
                             previous_close.to_numpy(float), rtol=1e-5,
                             atol=1e-4, equal_nan=False)
    prior_ready_sorted = (work.valid & previous_valid & adjacent & close_match)
    prior_ready = np.zeros(len(out), dtype=bool)
    prior_price = np.full(len(out), np.nan)
    prior_ready[work._pos.to_numpy(int)] = prior_ready_sorted.to_numpy(bool)
    prior_price[work._pos.to_numpy(int)] = previous_price.to_numpy(float)
    out["mom_stock_prev_bar_ready"] = prior_ready
    out.loc[prior_ready, "mom_stock_prev_bar_pct"] = prior_price[prior_ready]
    out.loc[prior_ready, "mom_stock_prev_bar_signed_pct"] = (
        sign.to_numpy(float)[prior_ready] * prior_price[prior_ready])

    # `_nifty_day` verifies the retained source hash and near-month contract.
    # `direction_at` uses the exact completed candle; its finite slot/session
    # values remain valid if only the UNUSED previous close is missing.
    market, proof = sources._nifty_day(day)
    expected_contract = out.contract_month.map(
        lambda value: f"NIFTY{nifty.normalize_month(value)}FUT" if pd.notna(value) else "")
    contract_match = expected_contract.eq(proof["nifty_contract"])
    by_stamp: dict[pd.Timestamp, dict] = {}
    for moment in stamp.loc[exact_clock].unique():
        at = pd.Timestamp(moment)
        direction = nifty.direction_at(market, at)
        bar_time = pd.Timestamp(direction["nifty_bar_ts"])
        if bar_time.tzinfo is None or bar_time.tz_convert(IST) > at:
            raise ValueError("V14 momentum Nifty source uses a future/naive bar")
        by_stamp[at] = direction
    directions = [by_stamp.get(moment, {}) for moment in stamp]
    market_bar = pd.Series([item.get("slot_candle", np.nan) for item in directions], index=out.index, dtype=float)
    market_session = pd.Series([item.get("session_open", np.nan) for item in directions], index=out.index, dtype=float)
    out["mom_nifty_status"] = [item.get("status", "UNAVAILABLE") for item in directions]
    out["mom_nifty_contract"] = proof["nifty_contract"]
    out["mom_nifty_source_sha256"] = proof["nifty_snapshot_sha256"]
    out["mom_nifty_bar_ts"] = [item.get("nifty_bar_ts", "") for item in directions]
    market_ready = (exact_clock & contract_match & np.isfinite(market_bar)
                    & np.isfinite(market_session) & sign.notna())
    out["mom_nifty_ready"] = market_ready.fillna(False).astype(bool)
    out.loc[market_ready, "mom_nifty_bar_pct"] = market_bar.loc[market_ready]
    out.loc[market_ready, "mom_nifty_session_pct"] = market_session.loc[market_ready]
    out.loc[market_ready, "mom_nifty_bar_signed_pct"] = (sign * market_bar).loc[market_ready]
    out.loc[market_ready, "mom_nifty_session_signed_pct"] = (sign * market_session).loc[market_ready]
    relative_bar = market_ready & valid_stock
    relative_open = market_ready & stock_open_valid
    out["mom_relative_bar_ready"] = relative_bar.fillna(False).astype(bool)
    out["mom_relative_open_ready"] = relative_open.fillna(False).astype(bool)
    out.loc[relative_bar, "mom_stock_relative_bar_signed_pct"] = (
        sign * (price - market_bar)).loc[relative_bar]
    out.loc[relative_open, "mom_stock_relative_open_signed_pct"] = (
        sign * (stock_open_value - market_session)).loc[relative_open]
    return out


__all__ = ["enrich_day", "OUTPUT_NUMERIC"]
