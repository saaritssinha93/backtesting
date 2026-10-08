"""Rebuild September 24's complete signal pool from sealed replay inputs.

The September 24 daily run predates the raw feature ledger. Its September 25
physical snapshot retains the earlier minute history. This helper verifies
that snapshot and reconstructs the nine native G signal slots for every stock.
It only reads source files and does not modify either strategy baseline.
"""
from __future__ import annotations

import sys
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import fno_oi_common as common
import fno_oi_hybrid_data as hybrid
import fno_v13_v10_g_2_backtest as g2
import fno_v13_v10_g_2_extend_results as ext
import fno_v13_v10_g_daily_replay as replay


DAY = date(2026, 9, 24)


def _flagged(frame: pd.DataFrame) -> pd.Series:
    flagged = pd.Series(False, index=frame.index)
    for name in ("gap_filled", "opening_snapshot", "provisional_stale"):
        if name in frame:
            value = frame[name]
            flagged |= (pd.to_numeric(value, errors="coerce").fillna(0).ne(0)
                        | value.astype(str).str.strip().str.lower().isin(("true", "yes", "on")))
    return flagged


def _one_symbol(minute: pd.DataFrame, future: pd.DataFrame, contract: pd.Series,
                signal_times: pd.DatetimeIndex) -> pd.DataFrame:
    five = hybrid.aggregate_equity_one_minute_to_five_minute(minute)
    five = hybrid.join_equity_price_with_futures_oi(five, future)
    five = five.loc[five.ts.isin(signal_times)].copy()
    if len(five) != len(signal_times) or five.ts.duplicated().any():
        raise ValueError(f"Incomplete five-minute signal slots: {contract.name}")
    five["day"] = DAY
    five["signal_ts"] = five.ts
    five["confirmation_ts"] = five.ts + pd.Timedelta(minutes=1)
    five["hhmm"] = five.ts.dt.strftime("%H%M")
    five["hhmm_int"] = five.hhmm.astype(int)
    five["signal_close"] = five.close
    five["tradingsymbol"] = str(contract.name)
    five["futures_tradingsymbol"] = str(contract.futures_tradingsymbol)
    five["contract_month"] = str(contract.contract_month)
    five["instrument_token"] = int(contract.equity_instrument_token)
    five["futures_instrument_token"] = int(contract.futures_instrument_token)
    five["exchange"] = "NSE"
    five["price_source"] = hybrid.BACKTEST_EQUITY_5M_CONSTRUCTION
    five["oi_source"] = "NFO_FUTURE"
    five["data_contract"] = hybrid.DATA_CONTRACT_VERSION
    for span in (9, 20, 50):
        five[f"v9_5m_ema{span}"] = five[f"ema{span}"]
    five["v9_5m_ema_bull"] = (five.ema9.gt(five.ema20) & five.ema20.gt(five.ema50)).astype("boolean")
    five["v9_5m_ema_bear"] = (five.ema9.lt(five.ema20) & five.ema20.lt(five.ema50)).astype("boolean")
    five["v9_5m_feature_ts"] = five.signal_ts

    volume = pd.to_numeric(minute.volume, errors="coerce")
    denominator = volume.shift(1).rolling(20, min_periods=5).mean()
    confirmation = minute[["ts", "open", "high", "low", "close", "volume"]].copy()
    confirmation["v9_1m_volume_ratio"] = volume.div(denominator.where(denominator.gt(0)))
    confirmation["confirmation_source_flagged"] = _flagged(minute)
    confirmation = confirmation.loc[confirmation.ts.isin(five.confirmation_ts)].copy()
    confirmation["v9_1m_feature_ts"] = confirmation.ts
    op, hi, lo, cl = (confirmation[name].astype(float)
                      for name in ("open", "high", "low", "close"))
    candle_range = (hi - lo).where(hi.gt(lo))
    confirmation["body_ratio"] = (cl - op).abs().div(candle_range)
    confirmation["v9_1m_upper_wick_ratio"] = (hi - pd.concat([op, cl], axis=1).max(axis=1)).div(candle_range)
    confirmation["v9_1m_lower_wick_ratio"] = (pd.concat([op, cl], axis=1).min(axis=1) - lo).div(candle_range)
    confirmation = confirmation.rename(columns={
        "ts": "confirmation_ts",
        **{name: f"confirmation_{name}" for name in ("open", "high", "low", "close", "volume")},
    })
    five = five.merge(confirmation, on="confirmation_ts", how="left", validate="one_to_one")
    five["v9_exact_confirmation_present"] = five.v9_1m_feature_ts.notna()
    five["v9_feature_available_ts"] = five.v9_1m_feature_ts
    return five.drop(columns=["date", "ts"], errors="ignore")


def load_sep24_raw_pool() -> tuple[pd.DataFrame, dict]:
    """Return verified raw signal rows and source/parity evidence for 2026-09-24."""
    run, result = ext._latest_successful_run(ext.DEFAULT_DAILY_ROOT, DAY)
    manifest_path, source_day = ext._snapshot_for_day(ext.DEFAULT_DAILY_ROOT, DAY, run, result)
    snapshot = manifest_path.parent
    manifest = g2.read_json(manifest_path)
    if manifest.get("complete") is not True:
        raise ValueError("Incomplete September 25 physical snapshot")
    replay._verify_input_snapshot(snapshot, manifest)
    source_manifest = g2.read_json(run / "source_manifest.json")
    if source_manifest.get("frozen_config_sha256") != g2.sha256(g2.DEFAULT_G_CONFIG):
        raise ValueError("September 24 source configuration differs from frozen G")
    if source_manifest.get("source_fingerprint") != result.get("source_fingerprint"):
        raise ValueError("September 24 source fingerprint differs from recorded replay")
    record = next(item for item in source_manifest["sources"]
                  if item["role"] == "DATED_UNIVERSE")
    universe_path = Path(record["path"])
    if g2.sha256(universe_path) != record["sha256"]:
        raise ValueError("September 24 dated universe changed")
    universe = pd.read_parquet(universe_path)
    stocks = universe.loc[~universe.is_index_future.fillna(False).astype(bool)].copy()
    contracts = stocks.set_index("equity_symbol", verify_integrity=True)
    coverage = pd.read_csv(run / "coverage.csv")
    if coverage[["missing_equity_minutes", "missing_futures_bars"]].ne(0).any().any():
        raise ValueError("September 24 coverage has missing required inputs")
    if set(coverage.symbol) != set(contracts.index):
        raise ValueError("September 24 coverage and dated universe differ")

    nifty = universe.loc[universe.underlying.astype(str).str.upper().eq("NIFTY")]
    if len(nifty) != 1:
        raise ValueError("Ambiguous September 24 NIFTY context")
    nifty_path = snapshot / "futures_5m" / (common.safe_contract_stem(
        str(nifty.iloc[0].tradingsymbol)) + "_5minute.parquet")
    problems: list = []
    nifty_future = replay._load_future(nifty_path, DAY, problems, "NIFTY")
    nifty_return = replay.config.nifty_context_from_bars(nifty_future, DAY)
    if problems or not np.isfinite(nifty_return):
        raise ValueError(f"Invalid September 24 NIFTY context: {problems}")

    signal_times, _, _ = replay._slot_times(DAY)
    frames = []
    for symbol in coverage.symbol:
        contract = contracts.loc[symbol]
        future_symbol = str(contract.futures_tradingsymbol)
        minute_path = hybrid.equity_one_minute_path(symbol, snapshot / "equity_1m")
        future_path = snapshot / "futures_5m" / (common.safe_contract_stem(future_symbol) + "_5minute.parquet")
        minute = replay._load_minute(minute_path, DAY, problems, symbol)
        future = replay._load_future(future_path, DAY, problems, symbol)
        replay._coverage(minute, future, DAY, symbol, problems)
        if problems:
            raise ValueError(f"September 24 coverage failure for {symbol}: {problems}")
        frames.append(_one_symbol(minute, future, contract, signal_times))
    raw = pd.concat(frames, ignore_index=True, sort=False)
    raw["nifty_first_bar_return_pct"] = float(nifty_return)
    raw["sid"] = np.arange(24_000_000, 24_000_000 + len(raw), dtype=np.int64)
    strict = replay._strict_signals(raw, float(nifty_return))
    official = pd.read_csv(run / "candidate_signals.csv", float_precision="round_trip")
    old = official.copy()
    old["signal_ts"] = pd.to_datetime(old.signal_ts, utc=True).dt.tz_convert("Asia/Kolkata")
    old["confirmation_ts"] = pd.to_datetime(old.confirmation_ts, utc=True).dt.tz_convert("Asia/Kolkata")
    keys = ["tradingsymbol", "signal_ts", "confirmation_ts", "side"]
    if (strict[keys].duplicated().any() or old[keys].duplicated().any()
            or len(strict) != len(old)):
        raise ValueError("September 24 strict candidate count or keys differ")
    comparison = old.merge(strict, on=keys, how="outer", suffixes=("_official", "_rebuilt"),
                           indicator=True, validate="one_to_one")
    if not comparison._merge.eq("both").all():
        raise ValueError("September 24 strict candidate identity parity failed")
    numeric = ("oi_change_pct", "price_change_pct", "volume_ratio", "body_ratio",
               "v9_1m_volume_ratio", "confirmation_open", "confirmation_high",
               "confirmation_low", "confirmation_close", "confirmation_volume", "trigger")
    for name in numeric:
        a = pd.to_numeric(comparison[name + "_official"], errors="coerce")
        b = pd.to_numeric(comparison[name + "_rebuilt"], errors="coerce")
        if not np.allclose(a, b, atol=1e-8, rtol=0, equal_nan=True):
            raise ValueError(f"September 24 strict candidate {name} parity failed")
    evidence = dict(day=DAY.isoformat(), source_run=str(run), snapshot=str(snapshot),
                    snapshot_source_day=source_day, snapshot_manifest_sha256=g2.sha256(manifest_path),
                    dated_universe=str(universe_path), dated_universe_sha256=record["sha256"],
                    stock_count=len(coverage), raw_rows=len(raw), strict_parity_rows=len(comparison),
                    nifty_first_bar_return_pct=float(nifty_return))
    return raw, evidence


def rebuild_sep24(day: date, run: Path, snapshot: Path) -> pd.DataFrame:
    """Runner adapter: verify supplied source identities and return raw rows."""
    if day != DAY:
        raise ValueError(f"This helper only reconstructs {DAY}")
    expected_run, result = ext._latest_successful_run(ext.DEFAULT_DAILY_ROOT, DAY)
    manifest_path, _ = ext._snapshot_for_day(ext.DEFAULT_DAILY_ROOT, DAY, expected_run, result)
    if Path(run).resolve() != expected_run.resolve():
        raise ValueError("September 24 runner passed a different daily run")
    if Path(snapshot).resolve() != manifest_path.parent.resolve():
        raise ValueError("September 24 runner passed a different physical snapshot")
    raw, _ = load_sep24_raw_pool()
    return raw


if __name__ == "__main__":
    _, proof = load_sep24_raw_pool()
    print(proof)
