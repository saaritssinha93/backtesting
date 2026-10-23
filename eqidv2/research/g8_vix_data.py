"""Read-only India VIX input audit and causal intraday features for G-8 research.

Kite timestamps mark candle START. A five-minute close becomes observable only
at timestamp + five minutes. No daily close or incomplete bar is used in an
earlier decision. This module never modifies existing strategy data or orders.
"""
from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
INPUT_ROOT = Path(r"C:\TradingData\eqidv2\backtesting_result_v13_v10_g_8\inputs")
DEFAULT_5M = INPUT_ROOT / "india_vix_5minute_2026-07-27_to_2026-10-22.csv"
DEFAULT_15M = INPUT_ROOT / "india_vix_15minute_2026-07-27_to_2026-10-22.csv"
IST = "Asia/Kolkata"


def _timestamp(value):
    value = pd.Timestamp(value)
    return value.tz_localize(IST) if value.tzinfo is None else value.tz_convert(IST)


def load_bars(path=DEFAULT_5M, minutes=5):
    """Validate raw Kite CSV and add explicit candle-availability timestamps."""
    frame = pd.read_csv(path)
    required = {"date", "open", "high", "low", "close", "volume"}
    if not required.issubset(frame):
        raise ValueError(f"Missing VIX fields: {sorted(required - set(frame))}")
    frame["date"] = pd.to_datetime(frame["date"], utc=True).dt.tz_convert(IST)
    if frame["date"].isna().any() or frame["date"].duplicated().any():
        raise ValueError("Missing/duplicate VIX timestamps")
    frame = frame.sort_values("date").reset_index(drop=True)
    prices = frame[["open", "high", "low", "close"]].apply(pd.to_numeric)
    if not np.isfinite(prices).all().all() or (prices <= 0).any().any():
        raise ValueError("Non-positive/non-finite VIX OHLC")
    if ((prices.high < prices[["open", "close", "low"]].max(axis=1)) |
            (prices.low > prices[["open", "close", "high"]].min(axis=1))).any():
        raise ValueError("Invalid VIX OHLC ordering")
    frame["day"] = frame.date.dt.strftime("%Y-%m-%d")
    frame["available_at"] = frame.date + pd.Timedelta(minutes=minutes)
    return frame


class VixData:
    """As-of lookup; call ``features(decision_at, slot_start)`` in IST.

    Returns None if no completed current-session bar or the freshest completed
    bar is more than five minutes stale. Percentage values are percentage
    points: 1.0 means a one-percent increase, not a decimal return of 1.0.
    """

    def __init__(self, path=DEFAULT_5M):
        self.path = Path(path)
        self.frame = load_bars(path)
        self.days = {day: group.reset_index(drop=True)
                     for day, group in self.frame.groupby("day", sort=True)}
        self.previous_close = {}
        prior = None
        for day, group in self.days.items():
            self.previous_close[day] = prior
            prior = float(group.iloc[-1].close)

    def features(self, decision_at, slot_start=None):
        decision = _timestamp(decision_at)
        day = decision.strftime("%Y-%m-%d")
        frame = self.days.get(day)
        if frame is None or frame.empty:
            return None
        available = frame[frame.available_at <= decision]
        if available.empty:
            return None
        current = available.iloc[-1]
        age = (decision - current.available_at).total_seconds()
        if age > 300:
            return None
        opening = frame.iloc[0]
        # An absent opening bar cannot establish the actual session move.
        if opening.date.strftime("%H:%M") != "09:15":
            return None

        def anchor(at):
            if at < opening.date or at > decision:
                return None
            prior = frame[frame.available_at <= at]
            return float(prior.iloc[-1].close) if not prior.empty else float(opening.open)

        close = float(current.close)

        def pct(base):
            return None if base is None else (close / base - 1.0) * 100.0

        slot = _timestamp(slot_start) if slot_start is not None else opening.date
        if slot.strftime("%Y-%m-%d") != day or slot > decision:
            raise ValueError("VIX slot start must be in the decision session and at/before decision")
        return {
            "vix_level": close,
            "vix_session_pct": pct(float(opening.open)),
            "vix_slot_pct": pct(anchor(slot)),
            "vix_15m_pct": pct(anchor(decision - pd.Timedelta(minutes=15))),
            "vix_30m_pct": pct(anchor(decision - pd.Timedelta(minutes=30))),
            "vix_previous_close_pct": pct(self.previous_close[day]),
            "vix_bar_start": current.date.isoformat(),
            "vix_available_at": current.available_at.isoformat(),
            "vix_decision_at": decision.isoformat(),
            "vix_age_seconds": age,
        }


def write_audit(path_5m=DEFAULT_5M, path_15m=DEFAULT_15M):
    """Write provenance and coverage only within G-8's isolated input folder."""
    five = load_bars(path_5m, 5)
    fifteen = load_bars(path_15m, 15)
    audit = []
    for day, frame in five.groupby("day", sort=True):
        expected = pd.date_range(f"{day} 09:15", f"{day} 15:25", freq="5min", tz=IST)
        actual = pd.DatetimeIndex(frame.date)
        audit.append({"day": day, "bars_5m": len(frame), "expected_bars_5m": 75,
                      "missing_5m": len(expected.difference(actual)),
                      "unexpected_5m": len(actual.difference(expected)),
                      "bars_15m": int((fifteen.day == day).sum()),
                      "session_open": float(frame.iloc[0].open),
                      "session_close": float(frame.iloc[-1].close)})
    coverage = pd.DataFrame(audit)
    if (coverage.missing_5m != 0).any() or (coverage.unexpected_5m != 0).any():
        raise ValueError("VIX contains incomplete or unexpected five-minute sessions")
    # Independently fetched interval data must agree on common OHLC aggregates.
    grouped = five.set_index("date").resample("15min", origin="start_day").agg(
        {"open": "first", "high": "max", "low": "min", "close": "last"}).dropna()
    comparison = fifteen.set_index("date")[["open", "high", "low", "close"]]
    common = grouped.index.intersection(comparison.index)
    max_difference = float((grouped.loc[common] - comparison.loc[common]).abs().max().max())
    if len(common) != len(fifteen) or max_difference > 0.000001:
        raise ValueError("VIX 5m aggregates disagree with independent 15m retrieval")
    coverage_path = INPUT_ROOT / "india_vix_coverage.csv"
    coverage.to_csv(coverage_path, index=False)
    provenance = {
        "schema": "g8_india_vix_inputs_v1",
        "audited_at_utc": datetime.now(timezone.utc).isoformat(),
        "source": "Zerodha Kite Connect historical_data (read-only)",
        "instrument_token": 264969,
        "exchange": "NSE",
        "tradingsymbol": "INDIA VIX",
        "request_from": "2026-07-27 09:15:00+05:30",
        "request_to": "2026-10-22 15:30:00+05:30",
        "source_fetcher": "kite_vix_fetch_5m_15m.py",
        "timezone": IST,
        "timestamp_semantics": "Candle start; close/high/low observable only at start + interval",
        "features": "Current-session completed 5m closes only; previous-session close is lagged",
        "sessions": len(coverage),
        "first_session": coverage.iloc[0].day,
        "last_session": coverage.iloc[-1].day,
        "rows_5m": len(five), "rows_15m": len(fifteen),
        "all_sessions_complete": True,
        "max_5m_to_15m_ohlc_difference": max_difference,
        "files": {str(Path(path).name): hashlib.sha256(Path(path).read_bytes()).hexdigest()
                  for path in (path_5m, path_15m, coverage_path)},
        "execution_authority": False,
    }
    (INPUT_ROOT / "india_vix_provenance.json").write_text(
        json.dumps(provenance, indent=2) + "\n", encoding="utf-8")
    return provenance


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--audit", action="store_true", help="Validate and record isolated VIX inputs")
    args = parser.parse_args()
    if args.audit:
        print(json.dumps(write_audit(), indent=2))
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
