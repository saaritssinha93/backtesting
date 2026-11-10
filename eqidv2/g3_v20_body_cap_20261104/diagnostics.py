"""Causal G3 decision explanations for hindsight-ranked daily movers."""
from __future__ import annotations

import math

import pandas as pd

from research import g3_slot_win70_contract as contract
import fno_v13_v10_g_3_contract as production


def _n(row, name: str) -> float:
    try:
        value = float(row.get(name))
        return value if math.isfinite(value) else math.nan
    except (TypeError, ValueError):
        return math.nan


def _true(value) -> bool:
    return str(value).lower() in ("true", "1", "1.0")


def top_two(minutes: pd.DataFrame, day: str) -> pd.DataFrame:
    part = minutes.sort_values(["tradingsymbol", "ts"])
    grouped = part.groupby("tradingsymbol", sort=True)
    frame = grouped.agg(open=("open", "first"), high=("high", "max"),
                        low=("low", "min"), close=("close", "last"),
                        count=("ts", "size")).reset_index()
    if not frame["count"].eq(360).all():
        raise ValueError(f"Incomplete day {day}")
    highs = part.loc[grouped.high.idxmax(), ["tradingsymbol", "ts"]].set_index("tradingsymbol").ts
    lows = part.loc[grouped.low.idxmin(), ["tradingsymbol", "ts"]].set_index("tradingsymbol").ts
    frame["high_time"] = frame.tradingsymbol.map(highs)
    frame["low_time"] = frame.tradingsymbol.map(lows)
    frame["up_pct"] = (frame.high / frame.open - 1) * 100
    frame["down_pct"] = (frame.low / frame.open - 1) * 100
    selected = []
    for side, column, limit, ascending, price_column, clock_column in (
            ("LONG", "up_pct", 3, False, "high", "high_time"),
            ("SHORT", "down_pct", -3, True, "low", "low_time")):
        eligible = frame.loc[frame[column].ge(limit) if side == "LONG" else frame[column].le(limit)]
        eligible = eligible.sort_values([column, "tradingsymbol"], ascending=[ascending, True]).head(2)
        for rank, row in enumerate(eligible.itertuples(), 1):
            stock_bars = part.loc[part.tradingsymbol.eq(row.tradingsymbol)]
            crossing = stock_bars.loc[stock_bars.high.ge(row.open * 1.03) if side == "LONG"
                                      else stock_bars.low.le(row.open * .97)]
            if crossing.empty:
                raise ValueError(f"Missing 3% crossing for {day} {row.tradingsymbol} {side}")
            selected.append(dict(day=day, side=side, rank=rank,
                                 tradingsymbol=row.tradingsymbol, move_pct=getattr(row, column),
                                 open_price=row.open, extreme_price=getattr(row, price_column),
                                 extreme_time=getattr(row, clock_column),
                                 first_3pct_time=crossing.iloc[0].ts,
                                 close_pct=(row.close / row.open - 1) * 100))
    return pd.DataFrame(selected)


def _strict_failures(row: dict, side: str) -> list[str]:
    direction = 1 if side == "LONG" else -1
    failures = []
    stack = row.get("v9_5m_ema_bull" if direction > 0 else "v9_5m_ema_bear")
    if not _true(stack):
        failures.append("EMA stack")
    oi, prev, change = (_n(row, key) for key in ("oi", "prev_oi", "oi_change_pct"))
    if not (prev > 0 and oi > prev and .05 <= change <= 1):
        failures.append("OI rise/range")
    if not _n(row, "volume_ratio") >= .8:
        failures.append("5m volume")
    if not direction * _n(row, "price_change_pct") >= .10:
        failures.append("directional price")
    o, h, low, c, signal = (_n(row, key) for key in (
        "confirmation_open", "confirmation_high", "confirmation_low", "confirmation_close", "signal_close"))
    if not (_true(row.get("v9_exact_confirmation_present")) and h > low and
            direction * (c - o) > 0 and direction * (c - signal) > 0):
        failures.append("exact 1m continuation")
    if side == "SHORT" and pd.Timestamp(row["signal_ts"]).strftime("%H:%M") == "09:25":
        if not _n(row, "nifty_first_bar_return_pct") <= -.05:
            failures.append("09:25 Nifty short gate")
    return failures


def _post_failures(row: dict, side: str, body_cap: float | None = None) -> list[str]:
    spec = contract._ACTIVE_BY_ID[row["setup_id"]]
    direction = 1 if side == "LONG" else -1
    failures = []
    if not direction * _n(row, "price_change_pct") >= spec.price_change_pct:
        failures.append(f"slot price <{spec.price_change_pct:.2f}%")
    if not _n(row, "oi_change_pct") >= spec.oi_change_pct:
        failures.append(f"slot OI <{spec.oi_change_pct:.2f}%")
    if not _n(row, "volume_ratio") >= spec.volume_ratio:
        failures.append(f"slot 5m volume <{spec.volume_ratio:.2f}")
    body = _n(row, "body_ratio")
    body_minimum = min(spec.body_ratio, body_cap) if body_cap is not None else spec.body_ratio
    if not body_minimum <= body <= 1:
        failures.append(f"1m body {body:.2f} <{body_minimum:.2f}" if math.isfinite(body)
                        else "1m body missing/invalid")
    wick = _n(row, "wick_ratio")
    if not 0 <= wick <= spec.max_wick_ratio:
        failures.append(f"1m wick >{spec.max_wick_ratio:.2f}")
    wick_min = contract.MIN_DIRECTIONAL_WICK_RATIO_BY_SETUP.get(spec.setup_id, 0)
    if wick_min and not wick >= wick_min:
        failures.append(f"1m directional wick <{wick_min:.2f}")
    if not _n(row, "traded_value") >= spec.min_traded_value:
        failures.append("slot traded value")
    vol_min = production.confirmation_volume_minimum(side, setup_id=spec.setup_id)
    if not _n(row, "v9_1m_volume_ratio") >= vol_min:
        failures.append(f"1m volume <{vol_min:.2f}")
    if spec.setup_id in production.MAX_CONFIRMATION_EXTENSION_ATR_BY_SETUP:
        extension = production.extension_atr(row.get("confirm_close"), row.get("v9_1m_ema20"),
                                             row.get("v9_1m_atr14"), side)
        if not math.isfinite(extension) or extension > production.MAX_CONFIRMATION_EXTENSION_ATR_BY_SETUP[spec.setup_id]:
            failures.append("EMA20 extension")
    if row.get("opening_0920_status") != "VALID" and spec.signal_end == "09:20":
        failures.append("opening proof")
    return failures


def explain_miss(raw: pd.DataFrame, audit: pd.DataFrame, symbol: str, side: str,
                 strict: pd.DataFrame | None = None, body_cap: float | None = None) -> str:
    matched = audit.loc[audit.tradingsymbol.eq(symbol) & audit.side.eq(side)]
    if not matched.empty:
        candidates = []
        for row in matched.to_dict("records"):
            failures = _post_failures(row, side, body_cap)
            time = pd.Timestamp(row["signal_ts"]).strftime("%H:%M")
            candidates.append((len(failures), time, row["setup_id"], failures))
        _, time, setup, failures = min(candidates)
        return f"{setup} ({time}): " + (", ".join(failures) if failures else "postfilter rejection")
    source = raw.loc[raw.tradingsymbol.eq(symbol)]
    if source.empty:
        return "No G3 feature rows: source stock excluded from signal cohort"
    if strict is not None and not strict.empty:
        native = strict.loc[strict.tradingsymbol.eq(symbol) & strict.side.eq(side)]
        if not native.empty:
            clocks = sorted(set(pd.to_datetime(native.signal_ts).dt.strftime("%H:%M")))
            if all(contract.setup_for(clock, side) is None for clock in clocks):
                return "Native signal only in disabled G3 slot(s): " + ", ".join(clocks)
    candidates = []
    for row in source.to_dict("records"):
        time = pd.Timestamp(row["signal_ts"]).strftime("%H:%M")
        if contract.setup_for(time, side) is None:
            continue
        failures = _strict_failures(row, side)
        candidates.append((len(failures), time, failures))
    if not candidates:
        return "No enabled G3 slot for this side in feature ledger"
    _, time, failures = min(candidates)
    return f"No native signal; closest {time}: " + (", ".join(failures) if failures else "source gate")


def status(book: pd.DataFrame, symbol: str, side: str) -> tuple[str, float, str]:
    matched = book.loc[book.tradingsymbol.eq(symbol) & book.side.eq(side)]
    if matched.empty:
        return "NOT_SELECTED", 0., ""
    executed = matched.loc[matched.portfolio_executed.astype(str).str.lower().eq("true")]
    if not executed.empty:
        return "EXECUTED", float(executed.portfolio_net_profit_rupees.sum()), ""
    reasons = sorted(set(matched.portfolio_reject_reason.dropna().astype(str)))
    return "SELECTED_ONLY", 0., ", ".join(reasons) if reasons else "order did not execute"
