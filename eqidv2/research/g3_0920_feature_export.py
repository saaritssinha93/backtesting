"""Isolated, read-only G-3 09:20 research features for a dated stock universe.

This does not add a trading setup. In particular, the opening OI comparison is
09:16-to-09:20 (four minutes), and historical files acquired after the signal
are retrospective evidence, not proof that a live 09:20 decision could use them.
"""
from __future__ import annotations

import argparse
from collections import Counter
from datetime import date, datetime, timedelta
import json
import math
import os
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq

import dashboard_flow_opening_data as opening
import fno_oi_backtest_provenance as provenance
import fno_oi_common as common
import fno_oi_hybrid_data as hybrid
from fno_v13_v10_g_3_opening_oi import bootstrap_0920_oi_change
from eqidv2_runtime_paths import RUNTIME_ROOT


def _stamp(value):
    if value is None or pd.isna(value):
        return None
    try:
        stamp = pd.Timestamp(value)
        return stamp.tz_localize(common.IST) if stamp.tzinfo is None else stamp.tz_convert(common.IST)
    except (TypeError, ValueError):
        return None


def _number(value, *, positive=False):
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) and (number > 0 if positive else number >= 0) else None


def _signed(value):
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _value(record, field):
    return _number(record.get(field)) if record is not None else None


def _is_flagged(record):
    for key in ("gap_filled", "is_gap_filled", "opening_snapshot", "provisional_stale", "is_synthetic",
                "synthetic", "synthetic_repair", "forward_filled", "is_filled", "interpolated"):
        value = record.get(key)
        if value is not None and not pd.isna(value) and str(value).strip().lower() not in {"", "0", "0.0", "false", "no", "none", "nan"}:
            return True
    quality = record.get("quality_state")
    if quality is not None and not pd.isna(quality) and str(quality).strip().upper() not in {"", "VALID"}:
        return True
    source = str(record.get("source", "")).upper()
    return any(word in source for word in ("SYNTHETIC", "GAP_FILL", "GAP-FILL", "FORWARD_FILL",
                                           "FORWARD-FILL", "INTERPOLAT", "CARRY_FORWARD", "CARRY FORWARD", "PROXY"))


def _valid_ohlcv(record):
    if record is None or _is_flagged(record):
        return False
    numbers = {key: _number(record.get(key), positive=key != "volume")
               for key in ("open", "high", "low", "close", "volume")}
    return (all(value is not None for value in numbers.values())
            and numbers["high"] >= max(numbers["open"], numbers["close"])
            and numbers["low"] <= min(numbers["open"], numbers["close"]))


def _read_cash(path):
    if not path.is_file():
        return pd.DataFrame()
    names = set(pq.ParquetFile(path).schema.names)
    required = {"date", "open", "high", "low", "close", "volume"}
    if not required <= names:
        raise ValueError(f"Missing cash-minute columns: {sorted(required - names)}")
    optional = {"fetched_at", "source", "quality_state", "gap_filled", "is_gap_filled", "opening_snapshot",
                "provisional_stale", "is_synthetic", "synthetic", "synthetic_repair",
                "forward_filled", "is_filled", "interpolated"}
    frame = pd.read_parquet(path, columns=sorted(required | (optional & names)))
    frame["ts"] = hybrid._to_ist(frame["date"])
    frame = frame.loc[frame.ts.notna()].sort_values("ts", kind="stable")
    # Never silently choose between two different observations of one minute.
    duplicate = frame.ts.duplicated(keep=False)
    frame = frame.loc[~duplicate].copy()
    valid = pd.Series(True, index=frame.index)
    for key in ("gap_filled", "is_gap_filled", "opening_snapshot", "provisional_stale", "is_synthetic",
                "synthetic", "synthetic_repair", "forward_filled", "is_filled", "interpolated"):
        if key in frame:
            valid &= frame[key].astype(str).str.strip().str.lower().isin(
                {"", "0", "0.0", "false", "no", "none", "nan", "<na>", "nat"})
    if "quality_state" in frame:
        valid &= frame.quality_state.fillna("").astype(str).str.strip().str.upper().isin(
            {"", "VALID", "NONE", "NAN"})
    if "source" in frame:
        valid &= ~frame.source.astype(str).str.upper().str.contains(
            "SYNTHETIC|GAP.?FILL|FORWARD.?FILL|INTERPOLAT|CARRY.?FORWARD|PROXY", regex=True, na=False)
    for key in ("open", "high", "low", "close", "volume"):
        frame[key] = pd.to_numeric(frame[key], errors="coerce")
        valid &= frame[key].notna() & frame[key].map(math.isfinite)
        valid &= frame[key].ge(0 if key == "volume" else 0.000001)
    valid &= frame.high.ge(frame[["open", "close"]].max(axis=1))
    valid &= frame.low.le(frame[["open", "close"]].min(axis=1))
    return frame.loc[valid].reset_index(drop=True)


def _cash_features(minute, day):
    boundary = pd.Timestamp(datetime.combine(day, datetime.min.time()).replace(hour=9, minute=20,
                                                                                 tzinfo=common.IST))
    confirmation_ts = boundary + pd.Timedelta(minutes=1)
    result = {"cash_0920_status": "MISSING_OR_FLAGGED_CASH_FIRST_BAR", "confirmation_0921_status": "MISSING_OR_FLAGGED_0921_MINUTE"}
    if minute.empty:
        return result
    prior = minute.loc[minute.ts.le(confirmation_ts)].copy()
    five = hybrid.aggregate_equity_one_minute_to_five_minute(prior)
    if not five.empty:
        featured = hybrid.add_equity_five_minute_features(five)
        selected = featured.loc[featured.ts.eq(boundary)]
        if len(selected) == 1:
            bar = selected.iloc[0]
            opened, closed = float(bar["open"]), float(bar["close"])
            if opened > 0 and _valid_ohlcv(bar.to_dict()):
                first_minutes = minute.loc[minute.ts.between(boundary - pd.Timedelta(minutes=4), boundary)]
                fetched = [_stamp(value) for value in first_minutes.get("fetched_at", pd.Series(dtype=object))]
                latest_fetch = max(fetched) if len(fetched) == 5 and all(value is not None for value in fetched) else None
                fetched_after_each_end = (latest_fetch is not None and all(
                    stamp >= minute_end for stamp, minute_end in zip(fetched, first_minutes.ts)))
                candle_range = float(bar["high"] - bar["low"])
                result.update(cash_0920_status="VALID_COMPLETED_FIRST_BAR",
                              cash_open=opened, cash_high=float(bar["high"]), cash_low=float(bar["low"]),
                              cash_close=closed, cash_volume=float(bar["volume"]),
                              price_first_5m_pct=(closed / opened - 1) * 100,
                              price_first_5m_basis="09:15_CASH_OPEN_TO_09:20_CASH_CLOSE",
                              previous_completed_5m_close=_number(bar.get("prev_close"), positive=True),
                              price_vs_previous_completed_5m_pct=_signed(bar.get("price_change_pct")),
                              price_vs_verified_pdc_pct=None,
                              price_vs_verified_pdc_status="NOT_EVALUATED_BY_G3_SOURCE",
                              opening_gap_vs_verified_pdc_pct=None,
                              ema9=_number(bar.get("ema9"), positive=True),
                              ema20=_number(bar.get("ema20"), positive=True),
                              ema50=_number(bar.get("ema50"), positive=True),
                              volume_ratio=_number(bar.get("volume_ratio")),
                              traded_value=closed * float(bar["volume"]),
                              signal_body_ratio=abs(closed - opened) / candle_range if candle_range > 0 else None,
                              signal_upper_wick_ratio=(float(bar["high"]) - max(opened, closed)) / candle_range if candle_range > 0 else None,
                              signal_lower_wick_ratio=(min(opened, closed) - float(bar["low"])) / candle_range if candle_range > 0 else None,
                              cash_first_bar_latest_fetched_at=latest_fetch.isoformat() if latest_fetch is not None else None,
                              cash_first_bar_received_by_0921=fetched_after_each_end and latest_fetch <= confirmation_ts)
    selected = minute.loc[minute.ts.eq(confirmation_ts)]
    if len(selected) == 1 and _valid_ohlcv(selected.iloc[0].to_dict()):
        bar = selected.iloc[0]
        opened, closed = float(bar.open), float(bar.close)
        candle_range = float(bar.high - bar.low)
        volume = pd.to_numeric(minute.volume, errors="coerce")
        denominator = volume.shift(1).rolling(20, min_periods=5).mean()
        prior_mean = denominator.loc[selected.index[0]]
        fetched = _stamp(bar.get("fetched_at"))
        result.update(confirmation_0921_status="VALID_REAL_MINUTE",
                      confirmation_asof_0920=False,
                      confirmation_open=opened, confirmation_high=float(bar.high),
                      confirmation_low=float(bar.low), confirmation_close=closed,
                      confirmation_volume=float(bar.volume),
                      confirmation_volume_ratio=float(bar.volume / prior_mean) if pd.notna(prior_mean) and prior_mean > 0 else None,
                      v9_1m_volume_ratio=float(bar.volume / prior_mean) if pd.notna(prior_mean) and prior_mean > 0 else None,
                      confirmation_body_ratio=abs(closed - opened) / candle_range if candle_range > 0 else None,
                      confirmation_upper_wick_ratio=(float(bar.high) - max(opened, closed)) / candle_range if candle_range > 0 else None,
                      confirmation_lower_wick_ratio=(min(opened, closed) - float(bar.low)) / candle_range if candle_range > 0 else None,
                      confirmation_fetched_at=fetched.isoformat() if fetched is not None else None,
                      confirmation_received_by_0922=fetched is not None and confirmation_ts <= fetched <= confirmation_ts + pd.Timedelta(minutes=1))
    return result


def _futures_0920(path, day, expected_contract, expected_expiry):
    if not path.is_file():
        return None, "MISSING_FUTURES_5M_FILE"
    frame = pd.read_parquet(path)
    if "timestamp" not in frame or "oi" not in frame:
        return None, "MISSING_FUTURES_TIMESTAMP_OR_OI"
    frame["ts"] = hybrid._to_ist(frame.timestamp)
    boundary = pd.Timestamp(datetime.combine(day, datetime.min.time()).replace(hour=9, minute=20,
                                                                                 tzinfo=common.IST))
    selected = frame.loc[frame.ts.eq(boundary)]
    if len(selected) != 1:
        return None, "MISSING_OR_DUPLICATE_FUTURES_0920"
    record = selected.iloc[0].to_dict()
    if str(record.get("tradingsymbol", "")) != expected_contract:
        return None, "FUTURES_0920_DATED_CONTRACT_MISMATCH"
    observed_expiry = pd.to_datetime(record.get("expiry"), errors="coerce")
    if pd.isna(observed_expiry) or observed_expiry.date() != pd.Timestamp(expected_expiry).date():
        return None, "FUTURES_0920_EXPIRY_MISMATCH"
    if _is_flagged(record) or _number(record.get("oi"), positive=True) is None:
        return None, "INVALID_OR_FLAGGED_FUTURES_0920_OI"
    start = _stamp(record.get("candle_start"))
    if start is not None and start != boundary - pd.Timedelta(minutes=5):
        return None, "FUTURES_0920_NOT_COMPLETED_5M_CANDLE"
    if "source_1m_count" in record and pd.notna(record["source_1m_count"]) and _number(record["source_1m_count"]) != 5:
        return None, "FUTURES_0920_INCOMPLETE_CANDLE"
    fetched = _stamp(record.get("fetch_timestamp"))
    return {"oi": float(record["oi"]), "oi_source": "NEAR_MONTH_FUTURES_5M",
            "source": record.get("source"), "quality_state": record.get("quality_state", "VALID"),
            "fetch_timestamp": fetched.isoformat() if fetched is not None else None}, "VALID_COMPLETED_FUTURES_0920"


def _nifty_context(full, root, day):
    rows = full.loc[full.underlying.astype(str).str.upper().eq("NIFTY")]
    if len(rows) != 1:
        return {"nifty_context_status": "MISSING_OR_AMBIGUOUS_DATED_NIFTY"}
    contract = str(rows.iloc[0].tradingsymbol)
    path = root / "fno_oi" / "raw_contracts_5m" / f"{common.safe_contract_stem(contract)}_5minute.parquet"
    if not path.is_file():
        return {"nifty_context_status": "MISSING_NIFTY_FUTURES_5M_FILE"}
    try:
        frame = pd.read_parquet(path)
        frame["ts"] = hybrid._to_ist(frame.timestamp)
        boundary = pd.Timestamp(datetime.combine(day, datetime.min.time()).replace(hour=9, minute=20,
                                                                                     tzinfo=common.IST))
        selected = frame.loc[frame.ts.eq(boundary)]
        if len(selected) != 1 or not _valid_ohlcv(selected.iloc[0].to_dict()):
            return {"nifty_context_status": "MISSING_OR_INVALID_NIFTY_FIRST_BAR"}
        record = selected.iloc[0]
        if str(record.get("tradingsymbol", "")) != contract:
            return {"nifty_context_status": "NIFTY_DATED_CONTRACT_MISMATCH"}
        observed_expiry = pd.to_datetime(record.get("expiry"), errors="coerce")
        if pd.isna(observed_expiry) or observed_expiry.date() != pd.Timestamp(rows.iloc[0].expiry).date():
            return {"nifty_context_status": "NIFTY_EXPIRY_MISMATCH"}
        start = _stamp(record.get("candle_start"))
        if start is not None and start != boundary - pd.Timedelta(minutes=5):
            return {"nifty_context_status": "NIFTY_FIRST_BAR_TIME_INVALID"}
        fetched = _stamp(record.get("fetch_timestamp"))
        return {"nifty_context_status": "VALID_COMPLETED_NIFTY_FIRST_BAR",
                "nifty_first_bar_return_pct": (float(record.close) / float(record.open) - 1) * 100,
                "nifty_first_bar_contract": contract,
                "nifty_first_bar_source": str(record.get("source", "")),
                "nifty_first_bar_fetch_timestamp": fetched.isoformat() if fetched is not None else None,
                "nifty_received_by_0921": fetched is not None and boundary <= fetched <= boundary + pd.Timedelta(minutes=1)}
    except (OSError, ValueError, KeyError, TypeError):
        return {"nifty_context_status": "NIFTY_FIRST_BAR_SOURCE_ERROR"}


def build_day(day: date, *, data_root: Path | str = RUNTIME_ROOT):
    """Return one research row per stock in the exact dated near-month universe."""
    if type(day) is not date:
        raise TypeError("day must be datetime.date")
    root = Path(data_root)
    universe_path = root / "fno_oi" / "universe" / f"near_month_{day}.parquet"
    if not universe_path.is_file():
        raise FileNotFoundError(f"Dated universe is missing under the requested data root: {universe_path}")
    full = pd.read_parquet(universe_path)
    stocks = full.loc[~full.is_index_future.fillna(False).astype(bool)]
    expiry = pd.to_datetime(stocks.expiry, errors="coerce").dropna().unique()
    if len(expiry) != 1:
        raise ValueError("Dated stock universe requires one near-month expiry")
    month = pd.Timestamp(expiry[0]).strftime("%y%b").upper()
    mapped, universe_proof = provenance.load_backtest_universe(
        universe_path=universe_path, universe_date=day,
        contract_month_contains=month, require_persisted_mapping=True)
    nifty_context = _nifty_context(full, root, day)
    rows = []
    boundary = datetime.combine(day, datetime.min.time()).replace(hour=9, minute=20, tzinfo=common.IST)
    for item in mapped.to_dict("records"):
        symbol = hybrid.resolve_backtest_equity_symbol(str(item["equity_symbol"]), root=root / "stocks_indicators_1min_eq")
        contract = str(item["futures_tradingsymbol"])
        row = {"session_date": day.isoformat(), "symbol": symbol, "contract": contract,
               "signal_end": "09:20", "confirmation_end": "09:21",
               "research_only": True, "active_g3_setup": False,
               "oi_change_pct": None, "oi_change_interval_minutes": None,
               "oi_change_source": None, "oi_baseline_acquired_by_0920": False,
               "oi_bar_received_by_0921": False, "signal_inputs_received_by_0921": False,
               "complete_decision_evidence_by_0922": False,
               "confirmation_asof_0920": False}
        row.update(nifty_context)
        reasons = []
        if nifty_context["nifty_context_status"] != "VALID_COMPLETED_NIFTY_FIRST_BAR":
            reasons.append(nifty_context["nifty_context_status"])
        cash_path = hybrid.equity_one_minute_path(symbol, root / "stocks_indicators_1min_eq")
        try:
            minute = _read_cash(cash_path)
            row.update(_cash_features(minute, day))
        except (OSError, ValueError, KeyError, TypeError) as exc:
            row.update(cash_0920_status="CASH_SOURCE_ERROR", confirmation_0921_status="CASH_SOURCE_ERROR",
                       cash_source_error=f"{type(exc).__name__}: {exc}")
        if row["cash_0920_status"] != "VALID_COMPLETED_FIRST_BAR":
            reasons.append(row["cash_0920_status"])
        if row["confirmation_0921_status"] != "VALID_REAL_MINUTE":
            reasons.append(row["confirmation_0921_status"])
        future_path = root / "fno_oi" / "raw_contracts_5m" / f"{common.safe_contract_stem(contract)}_5minute.parquet"
        try:
            future, future_status = _futures_0920(future_path, day, contract, item["expiry"])
        except (OSError, ValueError, KeyError, TypeError) as exc:
            future, future_status = None, f"FUTURES_SOURCE_ERROR_{type(exc).__name__}"
        row["futures_0920_status"] = future_status
        if future is not None:
            row["futures_0920_oi"] = future["oi"]
            row["oi_research_0920"] = future["oi"]
            row["futures_0920_fetch_timestamp"] = future["fetch_timestamp"]
            fetched = _stamp(future["fetch_timestamp"])
            row["oi_bar_received_by_0921"] = fetched is not None and boundary <= fetched <= boundary + timedelta(minutes=1)
        else:
            reasons.append(future_status)
        try:
            first = opening._first_minute(root, day.isoformat(), symbol, contract)
        except (OSError, ValueError, KeyError, TypeError):
            first = None
        if first is None:
            row["opening_0916_status"] = "MISSING_OR_INVALID_FIRST_MINUTE_OI"
            reasons.append(row["opening_0916_status"])
        else:
            row["opening_0916_oi"] = _number(first.get("oi"), positive=True)
            row["prev_oi_research_0916"] = row["opening_0916_oi"]
            row["opening_0916_status"] = ("VALID_FIRST_MINUTE_EVIDENCE" if row["opening_0916_oi"] is not None
                                          else "INVALID_ZERO_FIRST_MINUTE_OI")
            if row["opening_0916_oi"] is None:
                reasons.append(row["opening_0916_status"])
            row["opening_0916_acquired_at"] = first.get("acquired_at")
            acquired = _stamp(first.get("acquired_at"))
            row["oi_baseline_acquired_by_0920"] = acquired is not None and boundary - timedelta(minutes=4) <= acquired <= boundary
        if first is not None and future is not None:
            feature = bootstrap_0920_oi_change(
                first, {**future, "contract": contract}, boundary=boundary)
            if feature is not None:
                row.update(oi_change_pct=feature["oi_change_pct"],
                           oi_change_interval_minutes=feature["oi_change_interval_minutes"],
                           oi_change_source=feature["oi_change_source"],
                           oi_change_from=feature["oi_change_from"], oi_change_to=feature["oi_change_to"],
                           oi_change_availability=feature["availability"])
            elif row["opening_0916_status"] == "VALID_FIRST_MINUTE_EVIDENCE":
                reasons.append("INVALID_SAME_CONTRACT_0916_TO_0920_OI")
        required_indicators = ("price_first_5m_pct", "ema9", "ema20", "ema50", "volume_ratio",
                               "traded_value", "signal_body_ratio", "signal_upper_wick_ratio",
                               "signal_lower_wick_ratio", "v9_1m_volume_ratio",
                               "confirmation_body_ratio", "confirmation_upper_wick_ratio",
                               "confirmation_lower_wick_ratio", "nifty_first_bar_return_pct")
        if any(_signed(row.get(key)) is None for key in required_indicators):
            reasons.append("MISSING_G3_RESEARCH_INDICATORS")
        row["signal_inputs_received_by_0921"] = bool(row.get("cash_first_bar_received_by_0921")
                                                      and row["oi_baseline_acquired_by_0920"]
                                                      and row["oi_bar_received_by_0921"]
                                                      and row.get("nifty_received_by_0921")
                                                      and row["oi_change_pct"] is not None)
        row["complete_decision_evidence_by_0922"] = bool(row["signal_inputs_received_by_0921"]
                                                          and row.get("confirmation_received_by_0922")
                                                          and row["confirmation_0921_status"] == "VALID_REAL_MINUTE")
        row["missing_reasons"] = ";".join(dict.fromkeys(reasons))
        row["research_feature_status"] = "COMPLETE" if not reasons else "PARTIAL"
        rows.append(row)
    frame = pd.DataFrame(rows)
    coverage = {"schema": "g3_0920_research_features_v1", "session_date": day.isoformat(),
                "universe": universe_proof, "stock_rows": len(frame),
                "cash_first_bars": int(frame.cash_0920_status.eq("VALID_COMPLETED_FIRST_BAR").sum()),
                "real_0921_confirmations": int(frame.confirmation_0921_status.eq("VALID_REAL_MINUTE").sum()),
                "valid_0916_oi_changes": int(frame.oi_change_pct.notna().sum()),
                "nifty_first_bar_status": nifty_context["nifty_context_status"],
                "signal_inputs_received_by_0921": int(frame.signal_inputs_received_by_0921.sum()),
                "complete_decision_evidence_by_0922": int(frame.complete_decision_evidence_by_0922.sum()),
                "missing_reason_counts": dict(Counter(reason for value in frame.missing_reasons for reason in value.split(";") if reason)),
                "semantics": {"price_first_5m_pct": "Completed 09:15-09:20 cash candle close/open - 1",
                              "price_vs_previous_completed_5m_pct": "G3 native previous completed cash 5m bar, usually prior session at the open; not first-bar movement or verified PDC",
                              "oi_change_pct": "Valid same dated futures contract 09:16 minute OI to completed 09:20 OI; four minutes, retrospective unless acquisition proved timely",
                              "confirmation_0921": "Real next minute, unavailable at 09:20; its source receipt is recorded separately"},
                "trading_logic_changed": False}
    return frame, coverage


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--date", required=True, type=date.fromisoformat)
    parser.add_argument("--data-root", type=Path, default=RUNTIME_ROOT)
    parser.add_argument("--output-root", type=Path, default=Path("outputs/g3_0920_research"))
    args = parser.parse_args(argv)
    frame, coverage = build_day(args.date, data_root=args.data_root)
    target = args.output_root / args.date.isoformat()
    target.mkdir(parents=True, exist_ok=True)
    parquet = target / "features.parquet"
    temporary = target / ".features.parquet.part"
    frame.to_parquet(temporary, index=False)
    os.replace(temporary, parquet)
    csv = target / "features.csv"
    temporary_csv = target / ".features.csv.part"
    frame.to_csv(temporary_csv, index=False)
    os.replace(temporary_csv, csv)
    report = target / "coverage.json"
    temporary_json = target / ".coverage.json.part"
    temporary_json.write_text(json.dumps(coverage, indent=2, default=str) + "\n", encoding="utf-8")
    os.replace(temporary_json, report)
    print(json.dumps({"features": str(parquet.resolve()), "features_csv": str(csv.resolve()),
                      "coverage": str(report.resolve()),
                      "stock_rows": len(frame), "valid_0916_oi_changes": coverage["valid_0916_oi_changes"]}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
