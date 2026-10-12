"""Batch, read-only 09:20 G-3 research features for dated trading sessions.

Cash history is read and its causal rolling features are computed once per
symbol. Every output row retains the dated futures contract and the source
receipt times; historical recovery is never asserted to have been live data.
No strategy setup, order, or published G-3 result is modified.
"""
from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta
import hashlib
import json
import os
from pathlib import Path
from typing import Iterable

import pandas as pd

import fno_oi_backtest_provenance as provenance
import fno_oi_common as common
import fno_oi_hybrid_data as hybrid
from dashboard_flow_opening_data import _first_minute
from eqidv2_runtime_paths import RUNTIME_ROOT
from fno_v13_v10_g_3_opening_oi import bootstrap_0920_oi_change
from nse_market_calendar import market_closed_reason
from research import g3_0920_feature_export as single


_SEALED_RUN = Path("fno_oi/strategy_research/v13_v10_g_full_history") / "run_20260925_cutoff_corrected_through_20260923"
_MONTHLY_SOURCES = {"26AUG": date(2026, 8, 21), "26SEP": date(2026, 9, 24)}


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _mapping_tuples(rows) -> list[tuple[str, str, int, int]]:
    result = []
    for row in rows:
        result.append((str(row["equity_symbol"]), str(row["futures_tradingsymbol"]),
                       int(row["equity_instrument_token"]), int(row["futures_instrument_token"])))
    if (len(result) != len(set(result)) or
            len({entry[0] for entry in result}) != len(result) or
            len({entry[1] for entry in result}) != len(result) or
            any(not equity or not future or cash_token <= 0 or future_token <= 0
                for equity, future, cash_token, future_token in result)):
        raise ValueError("Sealed 09:20 mapping contains a duplicate or invalid stock/futures token")
    return sorted(result)


def _mapping_digest(tuples: list[tuple[str, str, int, int]]) -> str:
    return hashlib.sha256(json.dumps(tuples, separators=(",", ":")).encode("utf-8")).hexdigest()


def _sealed_material(root: Path) -> dict:
    """Validate the archived research bundle before consulting its mappings."""
    bundle = root / _SEALED_RUN
    dataset = bundle / "dataset"
    bundle_path = bundle / "bundle_manifest.json"
    sealed = json.loads(bundle_path.read_text(encoding="utf-8"))
    if sealed.get("state") != "COMPLETE" or sealed.get("execution_authority") is not False:
        raise ValueError("Sealed monthly mapping bundle lacks completed research-only state")
    names = ("dataset/dataset_manifest.json", "dataset/all_5m_features.parquet",
             "dataset/source_session_eligibility.csv", "dataset/source_manifest.csv")
    for name in names:
        artifact = sealed.get("artifacts", {}).get(name)
        path = bundle / name
        if (not isinstance(artifact, dict) or not path.is_file() or
                path.stat().st_size != artifact.get("bytes") or _sha256(path) != artifact.get("sha256")):
            raise ValueError(f"Sealed monthly mapping artifact hash/size mismatch: {name}")
    manifest = json.loads((dataset / "dataset_manifest.json").read_text(encoding="utf-8"))
    for name in ("all_5m_features.parquet", "source_session_eligibility.csv", "source_manifest.csv"):
        if manifest.get("output_sha256", {}).get(name) != sealed["artifacts"][f"dataset/{name}"]["sha256"]:
            raise ValueError(f"Sealed monthly dataset manifest disagrees on {name}")
    eligibility = pd.read_csv(dataset / "source_session_eligibility.csv", dtype=str, keep_default_na=False)
    required = {"day", "required_contract", "expiry", "universe_size", "contracts_with_data", "eligible"}
    if not required <= set(eligibility.columns):
        raise ValueError("Sealed monthly source eligibility lacks required columns")
    feature_columns = ["day", "hhmm_int", "tradingsymbol", "futures_tradingsymbol",
                       "contract_month", "instrument_token", "futures_instrument_token", "exchange"]
    first_bars = pd.read_parquet(dataset / "all_5m_features.parquet", columns=feature_columns,
                                 filters=[("hhmm_int", "==", 920)])
    if not set(feature_columns) <= set(first_bars.columns):
        raise ValueError("Sealed monthly first-bar mapping columns are missing")
    first_bars["day"] = first_bars.day.astype(str)
    sources = {str(Path(item["path"]).resolve()).casefold(): item["sha256"]
               for item in manifest.get("sources", []) if isinstance(item, dict) and item.get("exists") is True
               and item.get("path") and item.get("sha256")}
    source_manifest = pd.read_csv(dataset / "source_manifest.csv", dtype=str, keep_default_na=False)
    return {"bundle": bundle, "bundle_manifest": sealed, "dataset_manifest": manifest,
            "eligibility": eligibility, "first_bars": first_bars, "sources": sources,
            "source_manifest": source_manifest, "bundle_manifest_sha256": _sha256(bundle_path)}


def load_sealed_mapping(day: date, data_root: Path | str = RUNTIME_ROOT,
                        *, _material: dict | None = None) -> tuple[list[dict], dict, dict]:
    """Return a research-only monthly mapping, with source and date proof.

    This is retrospective monthly evidence, never a contemporaneous dated
    universe or a production trading mapping. Absent/contradictory days fail.
    """
    if type(day) is not date:
        raise TypeError("day must be datetime.date")
    root = Path(data_root)
    material = _material if _material is not None else _sealed_material(root)
    manifest = material["dataset_manifest"]
    session = day.isoformat()
    if session not in manifest.get("days", []):
        raise ValueError(f"Day is absent from the sealed monthly research dataset: {session}")
    eligible = material["eligibility"].loc[material["eligibility"].day.eq(session)]
    if len(eligible) != 1 or eligible.iloc[0]["eligible"].lower() != "true":
        raise ValueError(f"Sealed monthly data is not source-eligible for {session}")
    record = eligible.iloc[0]
    month = record["required_contract"]
    monthly_date = _MONTHLY_SOURCES.get(month)
    if monthly_date is None or pd.Timestamp(record["expiry"]).date() != pd.Timestamp(manifest["calendar"][month]).date():
        raise ValueError(f"Sealed monthly expiry/contract is unrecognized for {session}")
    path = root / "fno_oi" / "universe" / f"near_month_{monthly_date}.parquet"
    expected_hash = material["sources"].get(str(path.resolve()).casefold())
    if not expected_hash or not path.is_file() or _sha256(path) != expected_hash:
        raise ValueError(f"Sealed monthly universe source has changed or is absent: {path}")
    mapped, monthly_proof = provenance.load_backtest_universe(
        universe_path=path, universe_date=monthly_date,
        contract_month_contains=month, require_persisted_mapping=True,
        expected_file_sha256=expected_hash)
    monthly_rows = mapped.to_dict("records")
    monthly_tuples = _mapping_tuples(monthly_rows)
    if (len(monthly_tuples) != int(record["universe_size"]) or
            len(monthly_tuples) != int(record["contracts_with_data"])):
        raise ValueError(f"Sealed monthly eligibility count disagrees for {session}")
    first = material["first_bars"].loc[material["first_bars"].day.eq(session)]
    if len(first) != len(monthly_tuples) or not first.contract_month.astype(str).eq(month).all():
        raise ValueError(f"Sealed monthly first-bar contract coverage disagrees for {session}")
    sealed_rows = [{"equity_symbol": item["tradingsymbol"],
                    "futures_tradingsymbol": item["futures_tradingsymbol"],
                    "equity_instrument_token": item["instrument_token"],
                    "futures_instrument_token": item["futures_instrument_token"]}
                   for item in first.to_dict("records")]
    sealed_tuples = _mapping_tuples(sealed_rows)
    if sealed_tuples != monthly_tuples:
        raise ValueError(f"Sealed first-bar mappings conflict with hashed monthly universe: {session}")
    # A legacy dated file may omit cash-token fields. Its saved futures identity
    # must still agree with the sealed monthly source before using fallback.
    dated_path = root / "fno_oi" / "universe" / f"near_month_{session}.parquet"
    incomplete_dated_hash = None
    if dated_path.is_file():
        dated = pd.read_parquet(dated_path)
        if {"equity_symbol", "equity_instrument_token", "futures_tradingsymbol", "futures_instrument_token"} <= set(dated.columns):
            raise ValueError(f"Validly mapped dated universe must take precedence over sealed fallback: {session}")
        needed = {"tradingsymbol", "instrument_token", "is_index_future"}
        if not needed <= set(dated.columns):
            raise ValueError(f"Legacy dated universe cannot be checked for conflict: {session}")
        stock = dated.loc[~dated.is_index_future.fillna(False).astype(bool)]
        observed = sorted((str(r.tradingsymbol), int(r.instrument_token)) for r in stock.itertuples())
        expected = sorted((str(r["futures_tradingsymbol"]), int(r["futures_instrument_token"]))
                          for r in monthly_rows)
        if observed != expected:
            raise ValueError(f"Legacy dated universe conflicts with sealed monthly futures: {session}")
        incomplete_dated_hash = _sha256(dated_path)
    full = pd.read_parquet(path)
    nifty_rows = full.loc[full.underlying.astype(str).str.upper().eq("NIFTY")]
    if len(nifty_rows) != 1 or pd.Timestamp(nifty_rows.iloc[0].expiry).date() != pd.Timestamp(record["expiry"]).date():
        raise ValueError(f"Sealed monthly NIFTY identity is missing or conflicting: {session}")
    nifty = single._nifty_context(full, root, day)
    nifty_contract = str(nifty_rows.iloc[0].tradingsymbol)
    nifty_path = root / "fno_oi" / "raw_contracts_5m" / f"{common.safe_contract_stem(nifty_contract)}_5minute.parquet"
    if not nifty_path.is_file():
        raise ValueError(f"Dated NIFTY context file is missing: {nifty_path}")
    proof = {**monthly_proof, "mapping_source": "SEALED_MONTHLY_DATASET",
             "session_date": session, "monthly_universe_date": monthly_date.isoformat(),
             "monthly_universe_file_sha256": expected_hash,
             "bundle_manifest_sha256": material["bundle_manifest_sha256"],
             "dataset_manifest_sha256": material["bundle_manifest"]["artifacts"]["dataset/dataset_manifest.json"]["sha256"],
             "all_5m_features_sha256": material["bundle_manifest"]["artifacts"]["dataset/all_5m_features.parquet"]["sha256"],
             "source_session_eligibility_sha256": material["bundle_manifest"]["artifacts"]["dataset/source_session_eligibility.csv"]["sha256"],
             "sealed_session_mapping_sha256": _mapping_digest(sealed_tuples),
             "sealed_session_mapping_count": len(sealed_tuples),
             "incomplete_dated_universe_sha256": incomplete_dated_hash,
             "nifty_contract": nifty_contract,
             "nifty_expiry": pd.Timestamp(nifty_rows.iloc[0].expiry).date().isoformat(),
             "nifty_source_sha256": _sha256(nifty_path),
             "mapping_availability": "RETROSPECTIVE_MONTHLY_RESEARCH_ONLY"}
    return monthly_rows, proof, nifty


def _boundary(day: date, minute: int = 20) -> pd.Timestamp:
    return pd.Timestamp(datetime(day.year, day.month, day.day, 9, minute, tzinfo=common.IST))


def _cash_index(path: Path) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Load once; use the single-day export's strict source validation."""
    minute = single._read_cash(path)
    if minute.empty:
        return minute.set_index(pd.DatetimeIndex([], tz=common.IST)), pd.DataFrame()
    minute = minute.sort_values("ts").reset_index(drop=True)
    volume = pd.to_numeric(minute.volume, errors="coerce")
    minute["prior_20_1m_volume_mean"] = volume.shift(1).rolling(20, min_periods=5).mean()
    five = hybrid.aggregate_equity_one_minute_to_five_minute(minute)
    featured = hybrid.add_equity_five_minute_features(five) if not five.empty else five
    return minute.set_index("ts", drop=False), featured.set_index("ts", drop=False) if not featured.empty else featured


def _cash_row(minute: pd.DataFrame, five: pd.DataFrame, day: date) -> dict:
    """The single-day arithmetic, selecting only evidence through 09:21."""
    boundary, confirmation_ts = _boundary(day), _boundary(day, 21)
    out = {"cash_0920_status": "MISSING_OR_FLAGGED_CASH_FIRST_BAR",
           "confirmation_0921_status": "MISSING_OR_FLAGGED_0921_MINUTE"}
    if minute.empty:
        return out
    if not five.empty and boundary in five.index:
        bar = five.loc[boundary]
        if isinstance(bar, pd.Series) and single._valid_ohlcv(bar.to_dict()):
            opened, closed = float(bar.open), float(bar.close)
            first = minute.loc[boundary - pd.Timedelta(minutes=4):boundary]
            fetched = [single._stamp(value) for value in first.get("fetched_at", pd.Series(dtype=object))]
            latest = max(fetched) if len(first) == 5 and len(fetched) == 5 and all(stamp is not None for stamp in fetched) else None
            received = latest is not None and all(stamp >= ts for stamp, ts in zip(fetched, first.index))
            spread = float(bar.high - bar.low)
            out.update(cash_0920_status="VALID_COMPLETED_FIRST_BAR",
                       cash_open=opened, cash_high=float(bar.high), cash_low=float(bar.low),
                       cash_close=closed, cash_volume=float(bar.volume),
                       price_first_5m_pct=(closed / opened - 1) * 100,
                       price_first_5m_basis="09:15_CASH_OPEN_TO_09:20_CASH_CLOSE",
                       previous_completed_5m_close=single._number(bar.get("prev_close"), positive=True),
                       price_vs_previous_completed_5m_pct=single._signed(bar.get("price_change_pct")),
                       price_vs_verified_pdc_pct=None,
                       price_vs_verified_pdc_status="NOT_EVALUATED_BY_G3_SOURCE",
                       opening_gap_vs_verified_pdc_pct=None,
                       ema9=single._number(bar.get("ema9"), positive=True),
                       ema20=single._number(bar.get("ema20"), positive=True),
                       ema50=single._number(bar.get("ema50"), positive=True),
                       volume_ratio=single._number(bar.get("volume_ratio")),
                       traded_value=closed * float(bar.volume),
                       signal_body_ratio=abs(closed - opened) / spread if spread > 0 else None,
                       signal_upper_wick_ratio=(float(bar.high) - max(opened, closed)) / spread if spread > 0 else None,
                       signal_lower_wick_ratio=(min(opened, closed) - float(bar.low)) / spread if spread > 0 else None,
                       cash_first_bar_latest_fetched_at=latest.isoformat() if latest is not None else None,
                       cash_first_bar_received_by_0921=received and latest <= confirmation_ts)
    if confirmation_ts in minute.index:
        bar = minute.loc[confirmation_ts]
        if isinstance(bar, pd.Series) and single._valid_ohlcv(bar.to_dict()):
            opened, closed = float(bar.open), float(bar.close)
            spread = float(bar.high - bar.low)
            prior_mean = bar.get("prior_20_1m_volume_mean")
            fetched = single._stamp(bar.get("fetched_at"))
            ratio = (float(bar.volume / prior_mean) if pd.notna(prior_mean) and prior_mean > 0 else None)
            out.update(confirmation_0921_status="VALID_REAL_MINUTE",
                       confirmation_asof_0920=False,
                       confirmation_open=opened, confirmation_high=float(bar.high),
                       confirmation_low=float(bar.low), confirmation_close=closed,
                       confirmation_volume=float(bar.volume),
                       confirmation_volume_ratio=ratio, v9_1m_volume_ratio=ratio,
                       confirmation_body_ratio=abs(closed - opened) / spread if spread > 0 else None,
                       confirmation_upper_wick_ratio=(float(bar.high) - max(opened, closed)) / spread if spread > 0 else None,
                       confirmation_lower_wick_ratio=(min(opened, closed) - float(bar.low)) / spread if spread > 0 else None,
                       confirmation_fetched_at=fetched.isoformat() if fetched is not None else None,
                       confirmation_received_by_0922=fetched is not None and confirmation_ts <= fetched <= confirmation_ts + pd.Timedelta(minutes=1))
    return out


def _future_index(path: Path) -> tuple[pd.DataFrame | None, str | None]:
    if not path.is_file():
        return None, "MISSING_FUTURES_5M_FILE"
    try:
        frame = pd.read_parquet(path)
        if "timestamp" not in frame or "oi" not in frame:
            return None, "MISSING_FUTURES_TIMESTAMP_OR_OI"
        frame["ts"] = hybrid._to_ist(frame.timestamp)
        return frame.set_index("ts", drop=False).sort_index(), None
    except (OSError, ValueError, KeyError, TypeError):
        return None, "FUTURES_SOURCE_ERROR"


def _future_row(index: pd.DataFrame | None, initial_error: str | None, day: date,
                contract: str, expiry) -> tuple[dict | None, str]:
    if initial_error:
        return None, initial_error
    assert index is not None
    boundary = _boundary(day)
    if boundary not in index.index:
        return None, "MISSING_OR_DUPLICATE_FUTURES_0920"
    selected = index.loc[[boundary]]
    if len(selected) != 1:
        return None, "MISSING_OR_DUPLICATE_FUTURES_0920"
    record = selected.iloc[0].to_dict()
    if str(record.get("tradingsymbol", "")) != contract:
        return None, "FUTURES_0920_DATED_CONTRACT_MISMATCH"
    observed_expiry = pd.to_datetime(record.get("expiry"), errors="coerce")
    if pd.isna(observed_expiry) or observed_expiry.date() != pd.Timestamp(expiry).date():
        return None, "FUTURES_0920_EXPIRY_MISMATCH"
    if single._is_flagged(record) or single._number(record.get("oi"), positive=True) is None:
        return None, "INVALID_OR_FLAGGED_FUTURES_0920_OI"
    start = single._stamp(record.get("candle_start"))
    if start is not None and start != boundary - pd.Timedelta(minutes=5):
        return None, "FUTURES_0920_NOT_COMPLETED_5M_CANDLE"
    if "source_1m_count" in record and pd.notna(record["source_1m_count"]) and single._number(record["source_1m_count"]) != 5:
        return None, "FUTURES_0920_INCOMPLETE_CANDLE"
    fetched = single._stamp(record.get("fetch_timestamp"))
    return {"oi": float(record["oi"]), "oi_source": "NEAR_MONTH_FUTURES_5M",
            "source": record.get("source"), "quality_state": record.get("quality_state", "VALID"),
            "fetch_timestamp": fetched.isoformat() if fetched is not None else None}, "VALID_COMPLETED_FUTURES_0920"


def _dated_context(root: Path, day: date, *, allow_sealed_fallback: bool = False,
                   sealed_material: dict | None = None) -> tuple[list[dict], dict, dict]:
    path = root / "fno_oi" / "universe" / f"near_month_{day}.parquet"
    if not path.is_file():
        if allow_sealed_fallback:
            return load_sealed_mapping(day, root, _material=sealed_material)
        raise FileNotFoundError(f"Dated universe is missing under the requested data root: {path}")
    full = pd.read_parquet(path)
    required_mapping = {"equity_symbol", "equity_instrument_token",
                        "futures_tradingsymbol", "futures_instrument_token"}
    if not required_mapping <= set(full.columns):
        if allow_sealed_fallback:
            return load_sealed_mapping(day, root, _material=sealed_material)
        raise ValueError(f"Dated universe lacks persisted stock/futures mapping: {path}")
    stocks = full.loc[~full.is_index_future.fillna(False).astype(bool)]
    expiry = pd.to_datetime(stocks.expiry, errors="coerce").dropna().unique()
    if len(expiry) != 1:
        raise ValueError(f"Dated stock universe requires one near-month expiry: {day}")
    month = pd.Timestamp(expiry[0]).strftime("%y%b").upper()
    mapped, proof = provenance.load_backtest_universe(
        universe_path=path, universe_date=day,
        contract_month_contains=month, require_persisted_mapping=True)
    nifty = single._nifty_context(full, root, day)
    return mapped.to_dict("records"), proof, nifty


def build_days(days: Iterable[date], *, data_root: Path | str = RUNTIME_ROOT,
               skip_closed: bool = False,
               allow_sealed_fallback: bool = False) -> tuple[pd.DataFrame, dict]:
    """Build the same first-slot feature semantics across selected sessions.

    A selected trading day with missing mappings or sources is never skipped.
    A calendar-closed date is rejected unless ``skip_closed=True``, in which
    case the omission and exchange-calendar reason appear in coverage.
    """
    selected = list(days)
    if not selected or any(type(day) is not date for day in selected) or len(set(selected)) != len(selected):
        raise ValueError("Supply one or more distinct datetime.date sessions")
    selected.sort()
    closed = [{"day": day.isoformat(), "reason": market_closed_reason(day)} for day in selected if market_closed_reason(day)]
    if closed and not skip_closed:
        raise ValueError(f"Selected closed sessions require explicit skip_closed=True: {closed}")
    actual = [day for day in selected if not market_closed_reason(day)]
    if not actual:
        raise ValueError("No trading sessions remain after explicitly skipping closed dates")
    root = Path(data_root)
    sealed_material = _sealed_material(root) if allow_sealed_fallback else None
    sessions = {}
    schedule: dict[str, list[tuple[date, dict]]] = defaultdict(list)
    for day in actual:
        mapped, proof, nifty = _dated_context(root, day, allow_sealed_fallback=allow_sealed_fallback,
                                              sealed_material=sealed_material)
        sessions[day] = {"proof": proof, "nifty": nifty, "universe": len(mapped)}
        symbols = set()
        contracts = set()
        for item in mapped:
            symbol = hybrid.resolve_backtest_equity_symbol(
                str(item["equity_symbol"]), root=root / "stocks_indicators_1min_eq")
            contract = str(item["futures_tradingsymbol"])
            if symbol in symbols or contract in contracts:
                raise ValueError(f"Duplicated dated stock or contract mapping: {day} {symbol} {contract}")
            symbols.add(symbol)
            contracts.add(contract)
            schedule[symbol].append((day, item))
    rows = []
    for symbol, dated in sorted(schedule.items()):
        cash_path = hybrid.equity_one_minute_path(symbol, root / "stocks_indicators_1min_eq")
        try:
            minute, five = _cash_index(cash_path)
            cash_error = None
        except (OSError, ValueError, KeyError, TypeError) as exc:
            minute = five = pd.DataFrame()
            cash_error = f"{type(exc).__name__}: {exc}"
        futures_cache: dict[str, tuple[pd.DataFrame | None, str | None]] = {}
        for day, item in dated:
            contract = str(item["futures_tradingsymbol"])
            boundary = _boundary(day)
            nifty = sessions[day]["nifty"]
            row = {"session_date": day.isoformat(), "symbol": symbol, "contract": contract,
                   "signal_end": "09:20", "confirmation_end": "09:21",
                   "research_only": True, "active_g3_setup": False,
                   "oi_change_pct": None, "oi_change_interval_minutes": None,
                   "oi_change_source": None, "oi_baseline_acquired_by_0920": False,
                   "oi_bar_received_by_0921": False, "signal_inputs_received_by_0921": False,
                   "complete_decision_evidence_by_0922": False,
                   "confirmation_asof_0920": False, **nifty}
            reasons = []
            if nifty["nifty_context_status"] != "VALID_COMPLETED_NIFTY_FIRST_BAR":
                reasons.append(nifty["nifty_context_status"])
            if cash_error is None:
                row.update(_cash_row(minute, five, day))
            else:
                row.update(cash_0920_status="CASH_SOURCE_ERROR",
                           confirmation_0921_status="CASH_SOURCE_ERROR", cash_source_error=cash_error)
            if row["cash_0920_status"] != "VALID_COMPLETED_FIRST_BAR":
                reasons.append(row["cash_0920_status"])
            if row["confirmation_0921_status"] != "VALID_REAL_MINUTE":
                reasons.append(row["confirmation_0921_status"])
            if contract not in futures_cache:
                path = root / "fno_oi" / "raw_contracts_5m" / f"{common.safe_contract_stem(contract)}_5minute.parquet"
                futures_cache[contract] = _future_index(path)
            future, status = _future_row(*futures_cache[contract], day, contract, item["expiry"])
            row["futures_0920_status"] = status
            if future is None:
                reasons.append(status)
            else:
                row.update(futures_0920_oi=future["oi"], oi_research_0920=future["oi"],
                           futures_0920_fetch_timestamp=future["fetch_timestamp"])
                fetched = single._stamp(future["fetch_timestamp"])
                row["oi_bar_received_by_0921"] = fetched is not None and boundary <= fetched <= boundary + timedelta(minutes=1)
            try:
                first = _first_minute(root, day.isoformat(), symbol, contract)
            except (OSError, ValueError, KeyError, TypeError):
                first = None
            if first is None:
                row["opening_0916_status"] = "MISSING_OR_INVALID_FIRST_MINUTE_OI"
                reasons.append(row["opening_0916_status"])
            else:
                row["opening_0916_oi"] = single._number(first.get("oi"), positive=True)
                row["prev_oi_research_0916"] = row["opening_0916_oi"]
                row["opening_0916_status"] = ("VALID_FIRST_MINUTE_EVIDENCE" if row["opening_0916_oi"] is not None
                                              else "INVALID_ZERO_FIRST_MINUTE_OI")
                if row["opening_0916_oi"] is None:
                    reasons.append(row["opening_0916_status"])
                row["opening_0916_acquired_at"] = first.get("acquired_at")
                acquired = single._stamp(first.get("acquired_at"))
                row["oi_baseline_acquired_by_0920"] = acquired is not None and boundary - timedelta(minutes=4) <= acquired <= boundary
            if first is not None and future is not None:
                feature = bootstrap_0920_oi_change(first, {**future, "contract": contract}, boundary=boundary)
                if feature is not None:
                    row.update(oi_change_pct=feature["oi_change_pct"],
                               oi_change_interval_minutes=feature["oi_change_interval_minutes"],
                               oi_change_source=feature["oi_change_source"],
                               oi_change_from=feature["oi_change_from"], oi_change_to=feature["oi_change_to"],
                               oi_change_availability=feature["availability"])
                elif row["opening_0916_status"] == "VALID_FIRST_MINUTE_EVIDENCE":
                    reasons.append("INVALID_SAME_CONTRACT_0916_TO_0920_OI")
            required = ("price_first_5m_pct", "ema9", "ema20", "ema50", "volume_ratio",
                        "traded_value", "signal_body_ratio", "signal_upper_wick_ratio",
                        "signal_lower_wick_ratio", "v9_1m_volume_ratio",
                        "confirmation_body_ratio", "confirmation_upper_wick_ratio",
                        "confirmation_lower_wick_ratio", "nifty_first_bar_return_pct")
            if any(single._signed(row.get(key)) is None for key in required):
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
    frame = pd.DataFrame(rows).sort_values(["session_date", "symbol"], kind="stable").reset_index(drop=True)
    per_day = []
    for day in actual:
        part = frame.loc[frame.session_date.eq(day.isoformat())]
        session = sessions[day]
        if len(part) != session["universe"]:
            raise AssertionError(f"Dated stock row count mismatch: {day}")
        per_day.append({"session_date": day.isoformat(), "universe": session["proof"],
                        "stock_rows": len(part),
                        "cash_first_bars": int(part.cash_0920_status.eq("VALID_COMPLETED_FIRST_BAR").sum()),
                        "real_0921_confirmations": int(part.confirmation_0921_status.eq("VALID_REAL_MINUTE").sum()),
                        "valid_0916_oi_changes": int(part.oi_change_pct.notna().sum()),
                        "nifty_first_bar_status": session["nifty"]["nifty_context_status"],
                        "signal_inputs_received_by_0921": int(part.signal_inputs_received_by_0921.sum()),
                        "complete_decision_evidence_by_0922": int(part.complete_decision_evidence_by_0922.sum()),
                        "missing_reason_counts": dict(Counter(reason for value in part.missing_reasons for reason in value.split(";") if reason))})
    coverage = {"schema": "g3_0920_research_history_v1", "first_day": actual[0].isoformat(),
                "last_day": actual[-1].isoformat(), "requested_days": [day.isoformat() for day in selected],
                "included_days": [day.isoformat() for day in actual], "skipped_closed_days": closed,
                "trading_sessions": len(actual), "stock_rows": len(frame),
                "cash_first_bars": sum(part["cash_first_bars"] for part in per_day),
                "real_0921_confirmations": sum(part["real_0921_confirmations"] for part in per_day),
                "valid_0916_oi_changes": sum(part["valid_0916_oi_changes"] for part in per_day),
                "signal_inputs_received_by_0921": sum(part["signal_inputs_received_by_0921"] for part in per_day),
                "complete_decision_evidence_by_0922": sum(part["complete_decision_evidence_by_0922"] for part in per_day),
                "per_day": per_day, "trading_logic_changed": False,
                "allow_sealed_fallback": allow_sealed_fallback,
                "semantics": {"cash": "Completed 09:15-09:20 first bar from five genuine cash minutes; full-history EMA/volume warmup is causal",
                              "oi": "Same dated contract 09:16 minute to 09:20 completed bar over four minutes; post-market acquisition is retrospective",
                              "confirmation": "Separate 09:21 completed cash minute, never available as of 09:20",
                              "receipt_flags": "True only for evidence timestamped received after its candle end and before the stated cutoff"}}
    return frame, coverage


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start", type=date.fromisoformat, required=True)
    parser.add_argument("--through", type=date.fromisoformat, required=True)
    parser.add_argument("--data-root", type=Path, default=RUNTIME_ROOT)
    parser.add_argument("--output-root", type=Path, default=Path("outputs/g3_0920_research_history"))
    parser.add_argument("--skip-closed", action="store_true", help="Explicitly omit exchange-closed dates and list them in coverage")
    parser.add_argument("--skip-missing-universe", action="store_true",
                        help="Explicitly omit trading dates without verified mapping evidence and list them in coverage")
    parser.add_argument("--allow-sealed-fallback", action="store_true",
                        help="Opt in to retrospective sealed monthly mapping for dates without persisted dated mappings")
    args = parser.parse_args(argv)
    if args.start > args.through:
        parser.error("--start must not be after --through")
    universe_root = args.data_root / "fno_oi" / "universe"
    all_dates = [args.start + timedelta(days=n) for n in range((args.through - args.start).days + 1)]
    missing = []
    material = None
    for day in all_dates:
        if market_closed_reason(day):
            continue
        dated_path = universe_root / f"near_month_{day}.parquet"
        dated_mapped = False
        if dated_path.is_file():
            required = {"equity_symbol", "equity_instrument_token",
                        "futures_tradingsymbol", "futures_instrument_token"}
            dated_mapped = required <= set(pd.read_parquet(dated_path).columns)
        if dated_mapped:
            continue
        if args.allow_sealed_fallback:
            if material is None:
                material = _sealed_material(args.data_root)
            eligible = material["eligibility"].loc[material["eligibility"].day.eq(day.isoformat())]
            if (day.isoformat() in material["dataset_manifest"].get("days", []) and
                    len(eligible) == 1 and eligible.iloc[0]["eligible"].lower() == "true"):
                # An eligible source with a conflicting or corrupted mapping is
                # an error, not a day to silently omit under the skip flag.
                load_sealed_mapping(day, args.data_root, _material=material)
                continue
        missing.append(day)
    if missing and not args.skip_missing_universe:
        parser.error(f"Trading dates lack verified stock/futures mappings; pass --skip-missing-universe to disclose and omit: {missing}")
    dates = [day for day in all_dates if day not in missing]
    target = args.output_root / f"{args.start.isoformat()}_through_{args.through.isoformat()}"
    if target.exists():
        raise FileExistsError(f"Research export directory already exists: {target}")
    frame, coverage = build_days(dates, data_root=args.data_root, skip_closed=args.skip_closed,
                                 allow_sealed_fallback=args.allow_sealed_fallback)
    coverage["requested_calendar_start"] = args.start.isoformat()
    coverage["requested_calendar_through"] = args.through.isoformat()
    coverage["skipped_missing_universe_days"] = [day.isoformat() for day in missing]
    coverage["data_root"] = str(args.data_root.resolve())
    args.output_root.mkdir(parents=True, exist_ok=True)
    # A research comparison must not silently replace evidence from an
    # earlier extraction. Use a fresh output root for each run.
    target.mkdir(exist_ok=False)
    for name, writer in (("features.parquet", lambda path: frame.to_parquet(path, index=False)),
                         ("features.csv", lambda path: frame.to_csv(path, index=False)),
                         ("coverage.json", lambda path: path.write_text(json.dumps(coverage, indent=2, default=str) + "\n", encoding="utf-8"))):
        destination = target / name
        temporary = target / f".{name}.part"
        writer(temporary)
        os.replace(temporary, destination)
    artifacts = {}
    for name in ("features.parquet", "features.csv", "coverage.json"):
        digest = hashlib.sha256()
        with (target / name).open("rb") as source:
            for chunk in iter(lambda: source.read(1024 * 1024), b""):
                digest.update(chunk)
        artifacts[name] = digest.hexdigest()
    manifest = {"schema": "g3_0920_research_history_run_v1",
                "created_at": datetime.now(common.IST).isoformat(),
                "trading_logic_changed": False, "immutable_output": True,
                "requested_calendar_start": args.start.isoformat(),
                "requested_calendar_through": args.through.isoformat(),
                "artifacts_sha256": artifacts}
    (target / "run_manifest.json").write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"features": str((target / "features.parquet").resolve()),
                      "coverage": str((target / "coverage.json").resolve()),
                      "sessions": coverage["trading_sessions"], "stock_rows": len(frame),
                      "valid_0916_oi_changes": coverage["valid_0916_oi_changes"]}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
