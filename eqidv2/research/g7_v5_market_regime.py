"""Historical, completed-bar Nifty and F&O breadth gate for G7 V5 research."""
from __future__ import annotations

import hashlib
import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd

from research.g7_v5_candidate_audit import load_day


SPOT = Path("C:/TradingData/eqidv2/stocks_indicators_5min_eq_live/NIFTY50_INDEX_stocks_indicators_5min.parquet")
DATED_UNIVERSE = Path("C:/TradingData/eqidv2/fno_oi/universe")


def universe_count(day: str, observed: int) -> tuple[int, str]:
    dated = DATED_UNIVERSE / f"near_month_{day}.parquet"
    if dated.is_file():
        universe = pd.read_parquet(dated)
        symbol_field = "equity_symbol" if "equity_symbol" in universe else "underlying"
        symbols = universe.loc[universe.is_index_future.eq(False), symbol_field].dropna().nunique()
        if symbols < observed:
            raise ValueError(f"Dated F&O universe smaller than observed candidate pool: {day}")
        return int(symbols), str(dated)
    # Earlier source-cache dates use the documented archived monthly cohort.
    # Its observed stock count is complete for that retrospective cohort, but
    # the cohort itself is not point-in-time and is disclosed in the proof.
    return observed, "ARCHIVED_AFTER_DATED_COHORT"


def market_rows(calendar: list[str]) -> tuple[pd.DataFrame, dict]:
    if not SPOT.is_file():
        raise FileNotFoundError(SPOT)
    original = SPOT.read_bytes()
    digest = hashlib.sha256(original).hexdigest()
    index = pd.read_parquet(SPOT)
    index["ts"] = pd.to_datetime(index.date, utc=True).dt.tz_convert("Asia/Kolkata")
    index = index.sort_values("ts").drop_duplicates("ts", keep="last")
    for flag in ("opening_snapshot", "gap_filled", "provisional_stale", "is_synthetic"):
        if flag in index:
            index = index.loc[~index[flag].fillna(False).astype(bool)]
    index = index.loc[index.ts.dt.strftime("%H:%M").between("09:20", "15:30")].copy()
    index["day"] = index.ts.dt.strftime("%Y-%m-%d")
    index["ema20"] = index.close.ewm(span=20, adjust=False, min_periods=100).mean()
    if not set(calendar).issubset(set(index.day)):
        raise ValueError("True Nifty spot series misses a frozen V4 session")
    all_days = sorted(index.day.unique())
    previous_close = {}
    for day in calendar:
        earlier = [d for d in all_days if d < day]
        if not earlier:
            raise ValueError(f"No prior Nifty spot session: {day}")
        prior = index.loc[index.day.eq(earlier[-1]) & index.ts.dt.strftime("%H:%M").eq("15:30")]
        if len(prior) != 1:
            raise ValueError(f"No exact prior 15:30 spot bar: {day}")
        previous_close[day] = float(prior.close.iloc[0])
    results = []
    sources = []
    for day in calendar:
        raw, proof = load_day(day)
        sources.append(dict(day=day, **proof))
        f = raw.loc[raw.hhmm_int.between(920, 1330)].copy()
        f["signal_ts"] = pd.to_datetime(f.signal_ts, utc=True).dt.tz_convert("Asia/Kolkata")
        opening = f.loc[f.hhmm_int.eq(920), ["tradingsymbol", "open"]].drop_duplicates("tradingsymbol")
        opens = opening.set_index("tradingsymbol").open
        observed = int(f.tradingsymbol.nunique())
        universe, universe_source = universe_count(day, observed)
        spot = index.loc[index.day.eq(day)].set_index(index.loc[index.day.eq(day)].ts.dt.strftime("%H:%M"))
        first = spot.loc["09:20"]
        if isinstance(first, pd.DataFrame):
            raise ValueError(f"Duplicated opening spot bar: {day}")
        gap = 100 * (float(first.open) / previous_close[day] - 1)
        provisional = []
        for clock, stock in f.groupby(f.signal_ts.dt.strftime("%H:%M"), sort=True):
            if clock not in spot.index:
                continue
            candle = spot.loc[clock]
            stock = stock.loc[stock.tradingsymbol.isin(opens.index)].copy()
            stock["opening_price"] = stock.tradingsymbol.map(opens)
            valid = (pd.to_numeric(stock.close, errors="coerce").gt(0)
                     & pd.to_numeric(stock.prev_close, errors="coerce").gt(0)
                     & pd.to_numeric(stock.opening_price, errors="coerce").gt(0)
                     & pd.to_numeric(stock.source_1m_count, errors="coerce").eq(5))
            stock = stock.loc[valid]
            close = float(candle.close)
            last15 = np.nan
            earlier_clock = (pd.Timestamp(f"{day} {clock}", tz="Asia/Kolkata")
                             - pd.Timedelta(minutes=15)).strftime("%H:%M")
            if earlier_clock in spot.index:
                last15 = 100 * (close / float(spot.loc[earlier_clock].close) - 1)
            above_prior = 100 * float(stock.close.gt(stock.prev_close).mean()) if len(stock) else np.nan
            above_open = 100 * float(stock.close.gt(stock.opening_price).mean()) if len(stock) else np.nan
            coverage = 100 * len(stock) / universe if universe else 0
            from_open = 100 * (close / float(first.open) - 1)
            day_gain = 100 * (close / previous_close[day] - 1)
            strength = ((gap >= .50 and from_open >= .15) or day_gain >= 1. or
                        (np.isfinite(last15) and last15 >= .25))
            qualifying = bool(strength and np.isfinite(last15) and last15 >= .10 and
                              from_open > 0 and close > float(candle.ema20) and
                              above_prior >= 65 and above_open >= 60 and coverage >= 95)
            provisional.append(dict(day=day, clock=clock, signal_ts=str(candle.ts),
                nifty_gap_pct=gap, nifty_day_gain_pct=day_gain,
                nifty_from_open_pct=from_open, nifty_last15_pct=last15,
                nifty_above_ema20=bool(close > float(candle.ema20)),
                breadth_above_prior_pct=above_prior, breadth_above_open_pct=above_open,
                breadth_stocks=len(stock), universe_stocks=universe,
                observed_source_stocks=observed, universe_source=universe_source,
                coverage_pct=coverage,
                first_check_pass=qualifying))
        for pos, row in enumerate(provisional):
            row["strong_market"] = bool(pos and row["first_check_pass"] and
                                        provisional[pos-1]["first_check_pass"] and
                                        row["clock"] >= "09:40")
            results.append(row)
    frame = pd.DataFrame(results)
    if frame.duplicated(["day", "clock"]).any():
        raise ValueError("Market state has duplicate decision clock")
    return frame, dict(nifty_spot_path=str(SPOT), nifty_spot_sha256=digest,
                       stock_source_proofs=sources, first_possible_strong_clock="09:40",
                       historical_source_note="First eight early sessions use the archived after-dated stock universe")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--calendar", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.output.exists():
        raise FileExistsError(args.output)
    calendar = json.loads(args.calendar.read_text(encoding="utf-8"))
    days = [row["day"] for row in calendar["checks"]]
    market, proof = market_rows(days)
    args.output.mkdir(parents=True)
    market.to_csv(args.output / "market_regime.csv", index=False)
    (args.output / "source_proof.json").write_text(json.dumps(proof, indent=2), encoding="utf-8")
    for clock in ("09:30", "09:40", "09:50", "10:45"):
        part = market.loc[market.clock.eq(clock)]
        if len(part):
            print(clock, "sessions", len(part), "strong", int(part.strong_market.sum()),
                  "last15>=.10", int(part.nifty_last15_pct.ge(.10).sum()),
                  "breadth65", int(part.breadth_above_prior_pct.ge(65).sum()),
                  "above_open60", int(part.breadth_above_open_pct.ge(60).sum()),
                  "coverage95", int(part.coverage_pct.ge(95).sum()), flush=True)


if __name__ == "__main__":
    main()
