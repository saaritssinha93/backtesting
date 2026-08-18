"""Parameter sweep for the 5m EMA/OI + 1m confirmation setup.

Sweeps the seven thresholds that define the setup and reports profit factor per
combination. It evaluates every 5-minute slot in the session, not one slot, so a
single day yields a usable number of signals.

**Read the stability columns before believing any PF.** A grid this size will
always surface a high-PF corner on a small sample; that is arithmetic, not edge.
Two guards are reported alongside every result and neither is optional:

* ``top2_share`` -- fraction of gross profit contributed by the two best trades.
  Above ~0.6 the "edge" is a couple of lucky fills.
* ``days_profitable`` / ``n_days`` -- when the sweep runs over many sessions, a
  configuration that only works on one or two days is a day-concentration
  artefact. This repository has a documented history of exactly that failure.

The intended use is ``--days today`` and ``--days all`` side by side: the first
answers "what fit today", the second answers "did it mean anything".

Price, volume, indicators, confirmation and entry paths come from the NSE cash
equity. Only OI and OI percentage change come from its mapped NFO future. Both
cash datasets are candle-end labelled.
"""

from __future__ import annotations

import argparse
import itertools
import sys
import time
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Iterable

import numpy as np
import pandas as pd

import fno_oi_common as common
import fno_oi_ema_confirm_backtest as bt
import fno_oi_hybrid_data as hybrid


SESSION = "fno_oi_ema_confirm_sweep"

RANK_HISTORY_DIR = common.FNO_ROOT / "rank_history"
RESULT_DIR = common.FNO_ROOT / "strategy_research"
REPORT_PATH = common.LATEST_DIR / "latest_fno_oi_ema_confirm_sweep.md"

# Loosest thresholds used to build the candidate superset. Every swept
# combination is a subset of this, so signals are computed once.
LOOSE = dict(price_change_pct=0.10, oi_change_pct=0.05, volume_ratio=0.8)

GRID = {
    "price_change_pct": [0.10, 0.20, 0.30, 0.50],
    "oi_change_pct": [0.10, 0.25, 0.50, 0.75],
    "volume_ratio": [1.0, 1.5, 2.0, 3.0],
    "body_ratio": [0.40, 0.50, 0.60],
    "max_wick_ratio": [0.30, 0.40, 0.50],
}
BRACKETS = {
    "stop_pct": [0.3, 0.5, 0.75, 1.0],
    "target_pct": [0.3, 0.5, 0.75, 1.0, 1.5, 2.0],
}

FIRST_SIGNAL_SLOT = "0925"   # 0920 has no 5-minute predecessor to diff against
LAST_SIGNAL_SLOT = "1500"

CONFIRMATION_POLICY_V6_STRICT = "v6_strict"
CONFIRMATION_POLICY_V7_BREAKOUT = "v7_high_low_breakout"
CONFIRMATION_POLICIES = frozenset(
    {CONFIRMATION_POLICY_V6_STRICT, CONFIRMATION_POLICY_V7_BREAKOUT}
)


def _confirmation_candle_passes(
    policy: str,
    *,
    long_side: bool,
    candle_open: float,
    candle_close: float,
    signal_close: float,
) -> bool:
    """Return whether an exact, positive-range 1m candle may set a trigger.

    Range and timestamp completeness are checked by :func:`build_signal_table`.
    The V7 policy deliberately ignores candle colour, body/wick morphology and
    displacement from the 5m signal close; direction is confirmed only when a
    later candle trades through the recorded high/low trigger.
    """

    if policy == CONFIRMATION_POLICY_V7_BREAKOUT:
        return True
    if policy == CONFIRMATION_POLICY_V6_STRICT:
        if long_side:
            return candle_close > candle_open and candle_close > signal_close
        return candle_close < candle_open and candle_close < signal_close
    raise ValueError(
        f"unsupported confirmation_policy {policy!r}; "
        f"expected one of {sorted(CONFIRMATION_POLICIES)}"
    )


def _valid_v7_confirmation_candle(
    *,
    candle_open: float,
    candle_high: float,
    candle_low: float,
    candle_close: float,
    candle_volume: float,
    source_flagged: bool,
) -> bool:
    """Fail closed on malformed or explicitly non-real confirmation rows."""

    ohlcv = np.asarray(
        [candle_open, candle_high, candle_low, candle_close, candle_volume],
        dtype=float,
    )
    if source_flagged or not np.isfinite(ohlcv).all():
        return False
    return bool(
        candle_low > 0
        and candle_high > candle_low
        and candle_high >= max(candle_open, candle_close)
        and candle_low <= min(candle_open, candle_close)
        and candle_volume >= 0
    )


def load_five_minute_history(
    symbol: str,
    *,
    root: Path | None = None,
) -> pd.DataFrame:
    """Load futures bars from the live root or an explicit frozen root."""

    if root is None:
        return bt.load_five_minute(symbol)
    path = Path(root) / f"{common.safe_contract_stem(symbol)}_5minute.parquet"
    if not path.exists():
        return pd.DataFrame()
    frame = pd.read_parquet(
        path,
        columns=["timestamp", "open", "high", "low", "close", "volume", "oi"],
    )
    frame["ts"] = pd.to_datetime(frame["timestamp"], utc=True).dt.tz_convert(
        common.IST
    )
    return frame.sort_values("ts").reset_index(drop=True)


def load_minute_history(
    symbol: str,
    *,
    root: Path | None = None,
) -> pd.DataFrame:
    if root is None:
        return hybrid.load_equity_one_minute(symbol)
    return hybrid.load_equity_one_minute(symbol, root=Path(root))


def _resolve_equity_symbol(symbol: str, *, root: Path | None) -> str:
    if root is None:
        return hybrid.resolve_backtest_equity_symbol(symbol)
    return hybrid.resolve_backtest_equity_symbol(symbol, root=Path(root))


def build_signal_table(
    days: set[date] | None,
    *,
    square_off: str,
    max_forward_bars: int,
    mapped_universe: pd.DataFrame | None = None,
    confirmation_policy: str = CONFIRMATION_POLICY_V6_STRICT,
    futures_5m_root: Path | None = None,
    equity_1m_root: Path | None = None,
) -> tuple[pd.DataFrame, dict[int, dict[str, np.ndarray]]]:
    """Every candidate signal at the loosest thresholds, with its forward path.

    ``confirmation_policy`` defaults to the historical V6 directional-candle
    gate. V7 callers can select ``CONFIRMATION_POLICY_V7_BREAKOUT`` to let any
    exact, finite, positive-range 1m candle establish the directional high/low
    stop-entry trigger. Explicit source roots let a cache build consume an
    immutable snapshot; omitting them retains the historical live-root loaders.
    """

    if confirmation_policy not in CONFIRMATION_POLICIES:
        raise ValueError(
            f"unsupported confirmation_policy {confirmation_policy!r}; "
            f"expected one of {sorted(CONFIRMATION_POLICIES)}"
        )

    if mapped_universe is None:
        # Research callers that do not choose a universe explicitly still
        # resolve the latest pointer once to its canonical dated file.
        import fno_oi_backtest_provenance as provenance

        universe, _ = provenance.load_backtest_universe()
    else:
        universe = mapped_universe.copy()

    records: list[dict[str, Any]] = []
    paths: dict[int, dict[str, np.ndarray]] = {}
    sid = 0
    total = len(universe)

    for count, contract in enumerate(universe.to_dict("records"), start=1):
        futures_symbol = str(contract["futures_tradingsymbol"])
        equity_symbol = _resolve_equity_symbol(
            str(contract["equity_symbol"]), root=equity_1m_root
        )
        futures_five = load_five_minute_history(
            futures_symbol, root=futures_5m_root
        )
        minute = load_minute_history(equity_symbol, root=equity_1m_root)
        equity_five = hybrid.aggregate_equity_one_minute_to_five_minute(minute)
        if futures_five.empty or equity_five.empty or minute.empty:
            continue
        five = hybrid.join_equity_price_with_futures_oi(equity_five, futures_five)
        if five.empty:
            continue
        five["day"] = five["ts"].dt.date
        five["hhmm"] = five["ts"].dt.strftime("%H%M")
        if days is not None:
            five = five.loc[five["day"].isin(days)]
        five = five.loc[five["hhmm"].between(FIRST_SIGNAL_SLOT, LAST_SIGNAL_SLOT)]
        if five.empty:
            continue

        bull = five["ema9"].gt(five["ema20"]) & five["ema20"].gt(five["ema50"])
        bear = five["ema9"].lt(five["ema20"]) & five["ema20"].lt(five["ema50"])
        oi_ok = five["oi"].gt(five["prev_oi"]) & five["oi_change_pct"].ge(LOOSE["oi_change_pct"])
        vol_ok = five["volume_ratio"].ge(LOOSE["volume_ratio"])
        long_hit = bull & oi_ok & vol_ok & five["price_change_pct"].ge(LOOSE["price_change_pct"])
        short_hit = bear & oi_ok & vol_ok & five["price_change_pct"].le(-LOOSE["price_change_pct"])
        hits = five.loc[long_hit | short_hit].copy()
        if hits.empty:
            continue
        hits["side"] = np.where(long_hit.loc[hits.index], "LONG", "SHORT")

        # Epoch nanoseconds: searchsorted cannot compare tz-aware Timestamps.
        minute_ts = minute["ts"].astype("int64").to_numpy()
        m_open = minute["open"].to_numpy(float)
        m_high = minute["high"].to_numpy(float)
        m_low = minute["low"].to_numpy(float)
        m_close = minute["close"].to_numpy(float)
        m_hhmm = minute["ts"].dt.strftime("%H%M").to_numpy()
        if confirmation_policy == CONFIRMATION_POLICY_V7_BREAKOUT:
            m_volume = minute["volume"].to_numpy(float)
            m_source_real = np.ones(len(minute), dtype=bool)
            for column in ("gap_filled", "opening_snapshot", "provisional_stale"):
                if column not in minute.columns:
                    continue
                values = minute[column]
                flagged = (
                    pd.to_numeric(values, errors="coerce").fillna(0).ne(0)
                    | values.astype(str)
                    .str.strip()
                    .str.lower()
                    .isin({"true", "yes", "on"})
                )
                m_source_real &= ~flagged.to_numpy(bool)
        else:
            # Keep the historical V6 input path independent of optional V7-only
            # source-lineage fields.
            m_volume = None
            m_source_real = None

        for _, sig in hits.iterrows():
            # Equity 1-minute files are end-labelled. A 09:25 signal therefore
            # confirms on the candle ending 09:26; entry can begin at 09:27.
            want = (pd.Timestamp(sig["ts"]) + pd.Timedelta(minutes=1)).value
            idx = int(np.searchsorted(minute_ts, want))
            if idx >= len(minute_ts) or minute_ts[idx] != want:
                continue
            if confirmation_policy == CONFIRMATION_POLICY_V7_BREAKOUT and not (
                _valid_v7_confirmation_candle(
                    candle_open=m_open[idx],
                    candle_high=m_high[idx],
                    candle_low=m_low[idx],
                    candle_close=m_close[idx],
                    candle_volume=m_volume[idx],
                    source_flagged=not m_source_real[idx],
                )
            ):
                continue
            rng = m_high[idx] - m_low[idx]
            if rng <= 0:
                continue
            body = abs(m_close[idx] - m_open[idx])
            upper = m_high[idx] - max(m_open[idx], m_close[idx])
            lower = min(m_open[idx], m_close[idx]) - m_low[idx]
            long_side = sig["side"] == "LONG"

            if not _confirmation_candle_passes(
                confirmation_policy,
                long_side=long_side,
                candle_open=m_open[idx],
                candle_close=m_close[idx],
                signal_close=float(sig["close"]),
            ):
                continue

            stop_idx = idx + 1
            end_idx = min(stop_idx + max_forward_bars, len(minute_ts))
            fwd = slice(stop_idx, end_idx)
            keep = m_hhmm[fwd] <= square_off
            if not keep.any():
                continue
            paths[sid] = {
                "high": m_high[fwd][keep],
                "low": m_low[fwd][keep],
                "close": m_close[fwd][keep],
            }
            records.append(
                {
                    "sid": sid,
                    "day": sig["day"],
                    "hhmm": sig["hhmm"],
                    "tradingsymbol": equity_symbol,
                    "instrument_token": int(contract["equity_instrument_token"]),
                    "exchange": "NSE",
                    "futures_tradingsymbol": futures_symbol,
                    "futures_instrument_token": int(contract["futures_instrument_token"]),
                    "data_contract": hybrid.DATA_CONTRACT_VERSION,
                    "price_source": hybrid.BACKTEST_EQUITY_5M_CONSTRUCTION,
                    "oi_source": "NFO_FUTURE",
                    "oi": float(sig["oi"]),
                    "prev_oi": float(sig["prev_oi"]),
                    "side": sig["side"],
                    "price_change_pct": float(sig["price_change_pct"]),
                    "oi_change_pct": float(sig["oi_change_pct"]),
                    "volume_ratio": float(sig["volume_ratio"]),
                    "body_ratio": float(body / rng),
                    "wick_ratio": float((upper if long_side else lower) / rng),
                    "trigger": float(m_high[idx] if long_side else m_low[idx]),
                    "traded_value": float(sig["traded_value"]),
                    "hhmm_int": int(sig["hhmm"]),
                }
            )
            sid += 1
        if count % 50 == 0:
            print(f"[SIGNALS] {count}/{total} contracts | {len(records)} candidates", flush=True)

    return pd.DataFrame(records), paths


def simulate_bracket(
    signals: pd.DataFrame,
    paths: dict[int, dict[str, np.ndarray]],
    *,
    stop_pct: float,
    target_pct: float,
    cost_bps: float,
) -> np.ndarray:
    """Net return % per signal for one (stop, target) pair. NaN = never filled."""

    out = np.full(len(signals), np.nan)
    cost = cost_bps / 10000.0
    for i, (sid, side, trigger) in enumerate(
        zip(signals["sid"].to_numpy(), signals["side"].to_numpy(), signals["trigger"].to_numpy())
    ):
        path = paths.get(int(sid))
        if path is None:
            continue
        high, low, close = path["high"], path["low"], path["close"]
        if high.size == 0:
            continue
        long_side = side == "LONG"

        # Stop-entry fill: first forward bar that trades through the trigger.
        touched = np.flatnonzero(high >= trigger) if long_side else np.flatnonzero(low <= trigger)
        if touched.size == 0:
            continue
        e = int(touched[0])

        if long_side:
            stop = trigger * (1 - stop_pct / 100.0)
            target = trigger * (1 + target_pct / 100.0)
            hit_stop = np.flatnonzero(low[e:] <= stop)
            hit_target = np.flatnonzero(high[e:] >= target)
        else:
            stop = trigger * (1 + stop_pct / 100.0)
            target = trigger * (1 - target_pct / 100.0)
            hit_stop = np.flatnonzero(high[e:] >= stop)
            hit_target = np.flatnonzero(low[e:] <= target)

        s_i = hit_stop[0] if hit_stop.size else np.iinfo(np.int32).max
        t_i = hit_target[0] if hit_target.size else np.iinfo(np.int32).max
        if s_i == t_i == np.iinfo(np.int32).max:
            exit_price = close[-1]                      # square-off
        elif s_i <= t_i:                                # pessimistic on ties
            exit_price = stop
        else:
            exit_price = target
        gross = (exit_price / trigger - 1.0) if long_side else (1.0 - exit_price / trigger)
        out[i] = (gross - cost) * 100.0
    return out


def build_market_context() -> pd.DataFrame:
    """Per (day, slot) market state, for regime-conditioning signals.

    ``breadth`` is the share of near-month contracts trading above the previous
    day's close at that slot -- a direct read of how one-sided the tape is. On
    2026-07-08, the session that carried 40% of the unfiltered result, breadth
    was 0.061 against a median of 0.506.
    """

    frames = []
    for day_dir in sorted(p for p in RANK_HISTORY_DIR.iterdir() if p.is_dir()):
        path = day_dir / f"rankings_{day_dir.name}.parquet"
        if path.exists():
            frames.append(
                pd.read_parquet(
                    path,
                    columns=["timestamp", "tradingsymbol", "contract_month",
                             "price_change_pct_day"],
                )
            )
    if not frames:
        return pd.DataFrame()
    panel = pd.concat(frames, ignore_index=True)
    panel = panel.loc[panel["contract_month"].eq("2026-08")]
    stamps = pd.to_datetime(panel["timestamp"], utc=True).dt.tz_convert(common.IST)
    panel["day"] = stamps.dt.date
    panel["hhmm_int"] = stamps.dt.strftime("%H%M").astype(int)

    # Only contracts with a real prior close count. On a contract's first
    # session price_change_pct_day is NaN throughout, and NaN > 0 is False --
    # which would otherwise read as breadth 0.0, the most bearish value in the
    # sample, rather than "unknown".
    known = panel["price_change_pct_day"].notna()
    panel = panel.assign(_up=panel["price_change_pct_day"].gt(0), _known=known)
    grouped = panel.groupby(["day", "hhmm_int"], sort=True)
    context = grouped.agg(
        _up_count=("_up", "sum"), universe=("_known", "sum")
    ).reset_index()
    context["breadth"] = context["_up_count"] / context["universe"].where(context["universe"].gt(0))
    context = context.drop(columns=["_up_count"])

    nifty = panel.loc[panel["tradingsymbol"].eq("NIFTY26AUGFUT"),
                      ["day", "hhmm_int", "price_change_pct_day"]]
    nifty = nifty.rename(columns={"price_change_pct_day": "nifty_ret_day"})
    context = context.merge(nifty, on=["day", "hhmm_int"], how="left")
    # Session 1 of a contract has no previous close, so breadth is meaningless.
    context.loc[context["universe"].lt(20), ["breadth", "nifty_ret_day"]] = np.nan
    return context


def simulate_managed(
    signals: pd.DataFrame,
    paths: dict[int, dict[str, np.ndarray]],
    *,
    stop_pct: float,
    target_pct: float,
    breakeven_pct: float,
    trail_pct: float,
    cost_bps: float,
) -> np.ndarray:
    """Bracket simulation with optional breakeven and trailing stops.

    Within a bar the stop is evaluated **before** that bar's extreme is folded
    into the running peak. Otherwise the same high that tightens a trailing stop
    could also be used to dodge it, which is look-ahead.

    ``breakeven_pct`` moves the stop to entry once favourable excursion reaches
    it. ``trail_pct`` trails the stop that far behind the peak, active once the
    trade has moved at least that much. Zero disables either.
    """

    out = np.full(len(signals), np.nan)
    cost = cost_bps / 10000.0
    for i, (sid, side, trigger) in enumerate(
        zip(signals["sid"].to_numpy(), signals["side"].to_numpy(), signals["trigger"].to_numpy())
    ):
        path = paths.get(int(sid))
        if path is None:
            continue
        high, low, close = path["high"], path["low"], path["close"]
        if high.size == 0:
            continue
        long_side = side == "LONG"
        touched = np.flatnonzero(high >= trigger) if long_side else np.flatnonzero(low <= trigger)
        if touched.size == 0:
            continue
        e = int(touched[0])

        if long_side:
            stop = trigger * (1 - stop_pct / 100.0)
            target = trigger * (1 + target_pct / 100.0)
        else:
            stop = trigger * (1 + stop_pct / 100.0)
            target = trigger * (1 - target_pct / 100.0)

        peak = trigger
        exit_price = float(close[-1])
        for j in range(e, high.size):
            if long_side:
                if low[j] <= stop:
                    exit_price = stop
                    break
                if high[j] >= target:
                    exit_price = target
                    break
                peak = max(peak, float(high[j]))
                mfe = (peak / trigger - 1.0) * 100.0
                if breakeven_pct > 0 and mfe >= breakeven_pct:
                    stop = max(stop, trigger)
                if trail_pct > 0 and mfe >= trail_pct:
                    stop = max(stop, peak * (1 - trail_pct / 100.0))
            else:
                if high[j] >= stop:
                    exit_price = stop
                    break
                if low[j] <= target:
                    exit_price = target
                    break
                peak = min(peak, float(low[j]))
                mfe = (1.0 - peak / trigger) * 100.0
                if breakeven_pct > 0 and mfe >= breakeven_pct:
                    stop = min(stop, trigger)
                if trail_pct > 0 and mfe >= trail_pct:
                    stop = min(stop, peak * (1 + trail_pct / 100.0))
        gross = (exit_price / trigger - 1.0) if long_side else (1.0 - exit_price / trigger)
        out[i] = (gross - cost) * 100.0
    return out


def score(net: np.ndarray, days: np.ndarray) -> dict[str, Any]:
    mask = ~np.isnan(net)
    net = net[mask]
    days = days[mask]
    if net.size == 0:
        return {"trades": 0}
    profit = net[net > 0].sum()
    loss = -net[net < 0].sum()
    order = np.argsort(net)[::-1]
    top2 = net[order[:2]].sum()
    by_day: dict[Any, float] = {}
    for d, r in zip(days, net):
        by_day[d] = by_day.get(d, 0.0) + r
    day_values = np.array(list(by_day.values()))
    return {
        "trades": int(net.size),
        "win_rate": round(float((net > 0).mean()), 4),
        "net_sum": round(float(net.sum()), 3),
        "net_mean": round(float(net.mean()), 4),
        "pf": round(float(profit / loss), 3) if loss > 0 else None,
        "top2_share": round(float(top2 / profit), 3) if profit > 0 else None,
        "n_days": int(day_values.size),
        "days_profitable": int((day_values > 0).sum()),
        "day_win_rate": round(float((day_values > 0).mean()), 3),
    }


def sweep(
    signals: pd.DataFrame,
    paths: dict[int, dict[str, np.ndarray]],
    *,
    cost_bps: float,
    min_trades: int,
    sides: Iterable[str],
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    combos = list(itertools.product(*GRID.values()))
    brackets = list(itertools.product(*BRACKETS.values()))
    print(f"[SWEEP] {len(combos)} filter combos x {len(brackets)} brackets x "
          f"{len(list(sides))} side(s)", flush=True)

    for stop_pct, target_pct in brackets:
        net_all = simulate_bracket(
            signals, paths, stop_pct=stop_pct, target_pct=target_pct, cost_bps=cost_bps
        )
        for side in sides:
            side_mask = (signals["side"] == side).to_numpy() if side != "BOTH" else np.ones(len(signals), bool)
            for price_c, oi_c, vol_c, body_c, wick_c in combos:
                mask = (
                    side_mask
                    & (signals["oi_change_pct"] >= oi_c).to_numpy()
                    & (signals["volume_ratio"] >= vol_c).to_numpy()
                    & (signals["body_ratio"] >= body_c).to_numpy()
                    & (signals["wick_ratio"] <= wick_c).to_numpy()
                )
                pc = signals["price_change_pct"].to_numpy()
                if side == "SHORT":
                    mask &= pc <= -price_c
                elif side == "LONG":
                    mask &= pc >= price_c
                else:
                    mask &= np.abs(pc) >= price_c
                if mask.sum() < min_trades:
                    continue
                stats = score(net_all[mask], signals["day"].to_numpy()[mask])
                if stats.get("trades", 0) < min_trades:
                    continue
                stats.update(
                    side=side, price_change_pct=price_c, oi_change_pct=oi_c,
                    volume_ratio=vol_c, body_ratio=body_c, max_wick_ratio=wick_c,
                    stop_pct=stop_pct, target_pct=target_pct,
                )
                rows.append(stats)
        print(f"[SWEEP] stop {stop_pct} / target {target_pct} done "
              f"({len(rows)} rows so far)", flush=True)
    return pd.DataFrame(rows)


def render_report(today: pd.DataFrame, allday: pd.DataFrame, meta: dict[str, Any]) -> str:
    lines = [
        "# 5m EMA/OI + 1m Confirmation -- Parameter Sweep",
        "",
        f"- Generated: {common.now_ist().isoformat(timespec='seconds')}",
        f"- Signal slots scanned: {FIRST_SIGNAL_SLOT} to {LAST_SIGNAL_SLOT} (whole session)",
        f"- Cost: {meta['cost_bps']} bps round trip | min trades per combo: {meta['min_trades']}",
        f"- Candidate signals: today {meta['n_today']:,} | 52-day {meta['n_all']:,}",
        "",
        "> A grid this size **always** produces a high-PF corner on a small "
        "sample. Read `top2_share` and `day_win_rate` before anything else.",
        "",
    ]

    def table(df: pd.DataFrame, title: str, n: int = 15) -> list[str]:
        if df.empty:
            return [f"## {title}", "", "No combination met the minimum trade count.", ""]
        top = df.sort_values("pf", ascending=False).head(n)
        out = [
            f"## {title}", "",
            "| PF | Trades | Win | Net % | Side | price | OI | vol | body | wick | stop | target | top2 | day-win |",
            "| ---: | ---: | ---: | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
        ]
        for _, r in top.iterrows():
            out.append(
                f"| {r['pf']} | {r['trades']} | {r['win_rate']:.0%} | {r['net_sum']:+.2f} | "
                f"{r['side']} | {r['price_change_pct']} | {r['oi_change_pct']} | "
                f"{r['volume_ratio']} | {r['body_ratio']} | {r['max_wick_ratio']} | "
                f"{r['stop_pct']} | {r['target_pct']} | {r['top2_share']} | {r['day_win_rate']} |"
            )
        out.append("")
        return out

    lines += table(today, "Best by PF -- TODAY only")
    lines += table(allday, "Best by PF -- all 52 days")

    if not today.empty and not allday.empty:
        key = ["side", "price_change_pct", "oi_change_pct", "volume_ratio",
               "body_ratio", "max_wick_ratio", "stop_pct", "target_pct"]
        merged = today.sort_values("pf", ascending=False).head(20).merge(
            allday, on=key, suffixes=("_today", "_52d")
        )
        lines += [
            "## Do today's best parameters hold over 52 days?",
            "",
            "| Side | price | OI | vol | body | wick | stop | target | PF today | PF 52d | Trades 52d | day-win 52d |",
            "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
        ]
        for _, r in merged.iterrows():
            lines.append(
                f"| {r['side']} | {r['price_change_pct']} | {r['oi_change_pct']} | "
                f"{r['volume_ratio']} | {r['body_ratio']} | {r['max_wick_ratio']} | "
                f"{r['stop_pct']} | {r['target_pct']} | {r['pf_today']} | "
                f"**{r['pf_52d']}** | {r['trades_52d']} | {r['day_win_rate_52d']} |"
            )
        lines.append("")
    return "\n".join(lines) + "\n"


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--date", default="2026-08-10", help="The 'today' session.")
    parser.add_argument("--cost-bps", type=float, default=5.0)
    parser.add_argument("--min-trades", type=int, default=5)
    parser.add_argument("--min-trades-all", type=int, default=30)
    parser.add_argument("--square-off", default="1530")
    parser.add_argument("--max-forward-bars", type=int, default=400)
    parser.add_argument("--skip-all-days", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    started = time.monotonic()
    RESULT_DIR.mkdir(parents=True, exist_ok=True)
    today = pd.Timestamp(args.date).date()
    common.publish_status(SESSION, "RUNNING", day=today.isoformat())

    print("[BUILD] signal superset over all 52 days...", flush=True)
    signals, paths = build_signal_table(
        None, square_off=args.square_off, max_forward_bars=args.max_forward_bars
    )
    if signals.empty:
        print("[DONE] no candidate signals", flush=True)
        return 0
    print(f"[BUILD] {len(signals):,} candidate signals "
          f"({signals['day'].nunique()} days, {signals.side.value_counts().to_dict()})", flush=True)

    today_mask = signals["day"].eq(today).to_numpy()
    sig_today = signals.loc[today_mask].reset_index(drop=True)
    print(f"[BUILD] today: {len(sig_today):,} candidates", flush=True)

    sides = ["LONG", "SHORT", "BOTH"]
    print("\n[SWEEP] TODAY")
    res_today = sweep(sig_today, paths, cost_bps=args.cost_bps,
                      min_trades=args.min_trades, sides=sides)
    res_all = pd.DataFrame()
    if not args.skip_all_days:
        print("\n[SWEEP] ALL 52 DAYS")
        res_all = sweep(signals, paths, cost_bps=args.cost_bps,
                        min_trades=args.min_trades_all, sides=sides)

    if not res_today.empty:
        common.atomic_write_csv(res_today, RESULT_DIR / "ema_confirm_sweep_today.csv")
    if not res_all.empty:
        common.atomic_write_csv(res_all, RESULT_DIR / "ema_confirm_sweep_52d.csv")

    meta = {"cost_bps": args.cost_bps, "min_trades": args.min_trades,
            "n_today": len(sig_today), "n_all": len(signals)}
    report = render_report(res_today, res_all, meta)
    common.atomic_write_text(REPORT_PATH, report)
    print(report, flush=True)
    duration = time.monotonic() - started
    common.publish_status(SESSION, "SUCCESS", combos_today=len(res_today),
                          combos_all=len(res_all), duration_sec=round(duration, 1))
    print(f"[DONE] {duration:.0f}s | {REPORT_PATH}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
