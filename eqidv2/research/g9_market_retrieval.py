"""Bounded read-only retrieval of missing, dated Nifty futures bars for G-9.

Writes only a new G-9 research directory; never overwrites trading archives.
Provider responses must match every overlapping archived OHLC bar before any
missing observation is offered as a supplement. No order endpoint is used.
"""
from __future__ import annotations

import argparse
import contextlib
from datetime import datetime, timezone
import hashlib
import io
import json
from pathlib import Path

import numpy as np
import pandas as pd

from research.g9_market_features import IST, RAW_DIR, _prepare


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def retrieve(output_dir):
    output = Path(output_dir).resolve()
    allowed_root = Path('C:/TradingData/eqidv2/backtesting_result_v13_v10_g_9/inputs/retrieved').resolve()
    if not output.is_relative_to(allowed_root) or output == allowed_root:
        raise ValueError('Retrieval output must be a new child directory of G9 inputs/retrieved')
    if output.exists():
        raise ValueError("Fresh retrieval directory required")
    output.mkdir(parents=True)
    record = {"scope": "BACKTEST_ONLY", "execution_authority": False,
        "source": "Kite historical_data read-only", "retrieved_at_utc": datetime.now(timezone.utc).isoformat(),
        "requests": [], "max_historical_requests": 2,
        "acceptance": "all available overlap OHLC agrees <=1e-6; missing observations only; matching dated token/expiry metadata"}
    try:
        # Helper prints account identity on successful authentication. Suppress
        # stdout/stderr; do not print credentials or provider exception strings.
        with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            from kite_vix_fetch_5m_15m import setup_kite_session
            kite = setup_kite_session()
    except Exception as exc:
        record["session_status"] = "unavailable"
        record["session_error_type"] = type(exc).__name__
        (output / "retrieval_provenance.json").write_text(json.dumps(record, indent=2), encoding="utf-8")
        return record
    record["session_status"] = "authenticated"
    specs = [("NIFTY26AUGFUT", "2026-08-20", "2026-08-25"),
             ("NIFTY26SEPFUT", "2026-09-17", "2026-09-18")]
    for symbol, first, last in specs:
        path = RAW_DIR / (symbol + "_5minute.parquet")
        digest = sha(path)
        original = pd.read_parquet(path)
        if sha(path) != digest:
            raise ValueError("Source changed during retrieval audit")
        tokens = original.instrument_token.unique()
        expiries = original.expiry.unique()
        if len(tokens) != 1 or len(expiries) != 1 or not original.tradingsymbol.eq(symbol).all():
            raise ValueError("Ambiguous historical instrument identity")
        token = int(tokens[0])
        request = {"symbol": symbol, "instrument_token": token,
            "expiry": pd.Timestamp(expiries[0]).strftime("%Y-%m-%d"),
            "from": first + " 09:15", "to": last + " 15:30", "interval": "5minute",
            "source_archive": str(path), "source_archive_sha256": digest}
        record["requests"].append(request)
        try:
            with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
                rows = kite.historical_data(instrument_token=token,
                    from_date=pd.Timestamp(first + " 09:15", tz=IST).to_pydatetime(),
                    to_date=pd.Timestamp(last + " 15:30", tz=IST).to_pydatetime(),
                    interval="5minute", continuous=False, oi=True)
            raw = pd.DataFrame(rows)
            request["response_rows"] = len(raw)
            if raw.empty:
                request["status"] = "no_data_returned"
                continue
            raw_path = output / (symbol + "_provider_response.csv")
            raw.to_csv(raw_path, index=False)
            request["response_sha256"] = sha(raw_path)
            fetched, duplicates = _prepare(raw, "date", True)
            if not fetched.bar_valid.all() or duplicates:
                request["status"] = "rejected_invalid_or_duplicate_response"
                continue
            expected_start = pd.Timestamp(first + " 09:20", tz=IST)
            expected_end = pd.Timestamp(last + " 15:30", tz=IST)
            if not fetched.available_at.between(expected_start, expected_end).all():
                request["status"] = "rejected_outside_requested_window"
                continue
            archive, _ = _prepare(original, "timestamp")
            old = archive.set_index("available_at")
            new = fetched.set_index("available_at")
            overlap = old.index.intersection(new.index)
            request["overlap_bars"] = len(overlap)
            if len(overlap) < 50:
                request["status"] = "rejected_insufficient_identity_overlap"
                continue
            delta = float(np.abs(old.loc[overlap, ["open", "high", "low", "close"]].to_numpy(float) -
                                 new.loc[overlap, ["open", "high", "low", "close"]].to_numpy(float)).max())
            request["max_overlap_ohlc_difference"] = delta
            if not np.isfinite(delta) or delta > 1e-6:
                request["status"] = "rejected_conflicting_archive_ohlc"
                continue
            missing = fetched.loc[~fetched.available_at.isin(old.index)].copy()
            request["missing_bars_recovered"] = len(missing)
            if missing.empty:
                request["status"] = "verified_but_no_missing_bars_recovered"
                continue
            missing["timestamp"] = missing.available_at
            missing["candle_start"] = missing.available_at - pd.Timedelta(minutes=5)
            missing["underlying"] = "NIFTY"
            missing["tradingsymbol"] = symbol
            missing["expiry"] = pd.Timestamp(expiries[0])
            missing["instrument_token"] = token
            missing["quality_state"] = "VALID"
            missing["fetch_timestamp"] = pd.Timestamp.now(tz=IST)
            missing["source"] = "kite_historical_readonly_g9_retrieval"
            supplement = output / (symbol + "_verified_missing_bars.parquet")
            missing.to_parquet(supplement, index=False)
            request["supplement_path"] = str(supplement)
            request["supplement_sha256"] = sha(supplement)
            request["status"] = "verified_missing_bars_recovered"
        except Exception as exc:
            request["status"] = "provider_or_validation_error"
            request["error_type"] = type(exc).__name__
    record["files"] = {p.name: sha(p) for p in output.iterdir() if p.is_file()}
    (output / "retrieval_provenance.json").write_text(json.dumps(record, indent=2), encoding="utf-8")
    return record


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    proof = retrieve(args.output)
    print(json.dumps({"output": str(args.output), "session_status": proof["session_status"],
        "requests": [{k: r[k] for k in ("symbol", "status", "response_rows", "overlap_bars", "missing_bars_recovered") if k in r}
                     for r in proof["requests"]]}, indent=2))


if __name__ == "__main__":
    main()
