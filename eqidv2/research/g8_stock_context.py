"""Causal stock/Nifty context from G8's exact archived G7 V2 price bytes.

This module never changes original source packages.  Stock and Nifty returns
share the same completed five-minute endpoints and opening anchors.  ATR uses
14 completed current-session minutes strictly before confirmation; unavailable
history remains missing, with an explicit reason rather than a later-bar fill.
"""
from __future__ import annotations

from collections import Counter
import json
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

from research.g4_failed_breakout_research import SealedPrices
from research.g4_vwap_rsi_features import _check_flags
from research import g4_g3_mirror_backtest as base
from research import g8_sources

IST = "Asia/Kolkata"
CACHE = g8_sources.G8_ROOT / "inputs/stock_context_v1"
SCHEMA = "g8_causal_stock_context_v1"
OHLC = ("open", "high", "low", "close")


def _dump(path, value):
    Path(path).write_text(json.dumps(value, indent=2, default=str, allow_nan=False) + "\n", encoding="utf-8")


def _valid_window(window, expected):
    if not np.array_equal(pd.DatetimeIndex(window.ts).as_unit("ns").asi8,
                          expected.as_unit("ns").asi8):
        return "MISSING_OR_DUPLICATE_MINUTE"
    values = window[list(OHLC)].to_numpy(float)
    if not np.isfinite(values).all() or (values <= 0).any():
        return "INVALID_OHLC"
    if ((values[:, 1] < values.max(axis=1)).any()
            or (values[:, 2] > values.min(axis=1)).any()):
        return "INVALID_OHLC_ORDER"
    try:
        _check_flags(window)
    except ValueError:
        return "FLAGGED_INPUT"
    return "OK"


def stock_context(raw, row, nifty):
    """Derive one order's features without looking after its confirmation."""
    stamp = pd.Timestamp(row["confirmation_ts"])
    if stamp.tzinfo is None or stamp != stamp.floor("min"):
        raise ValueError("Timezone-aware completed-minute confirmation required")
    stamp = stamp.tz_convert(IST)
    if stamp.strftime("%Y-%m-%d") != str(row["day"])[:10]:
        raise ValueError("Confirmation escaped its trading day")
    if row["side"] not in ("LONG", "SHORT"):
        raise ValueError("Unknown trade side")
    sign = 1. if row["side"] == "LONG" else -1.
    current = raw.loc[raw.ts.eq(stamp)]
    if len(current) != 1 or any(not np.isclose(float(current.iloc[0][field]),
            float(row["confirmation_" + field]), atol=.001, rtol=0) for field in OHLC):
        raise ValueError("Archived confirmation OHLC differs from selected order")
    if _valid_window(current, pd.DatetimeIndex([stamp])) != "OK":
        raise ValueError("Invalid archived confirmation input")
    end = stamp.floor("5min")
    opening = stamp.normalize() + pd.Timedelta(hours=9, minutes=15)
    if end < opening + pd.Timedelta(minutes=5):
        raise ValueError("No completed five-minute stock/Nifty interval")
    if pd.Timestamp(nifty["nifty_bar_ts"]).tz_convert(IST) != end:
        raise ValueError("Nifty and stock feature endpoints differ")
    result = dict(sid=int(row["sid"]), stock_context_at=stamp.isoformat(),
        stock_context_bar_end=end.isoformat(), stock_context_available_at=stamp.isoformat(),
        stock_relative5_pct=np.nan, stock_relative_session_pct=np.nan,
        stock5_return_pct=np.nan, stock_session_return_pct=np.nan,
        stock_prior14_atr=np.nan, confirmation_range_atr=np.nan,
        stock_atr_prior_bars=0, stock_atr_last_bar="")

    for label, start, market_return in (
            ("5", end - pd.Timedelta(minutes=4), float(nifty["slot_candle"])),
            ("session", opening + pd.Timedelta(minutes=1), float(nifty["session_open"]))):
        window = raw.loc[raw.ts.ge(start) & raw.ts.le(end)].sort_values("ts", kind="stable")
        expected = pd.date_range(start, end, freq="min")
        status = _valid_window(window, expected)
        if not np.isfinite(market_return):
            status = "INVALID_NIFTY_RETURN"
        feature_name = "stock_relative5" if label == "5" else "stock_relative_session"
        result[feature_name + "_status"] = status
        if status == "OK":
            stock_return = (float(window.iloc[-1].close) / float(window.iloc[0].open) - 1.) * 100.
            result["stock" + label + "_return_pct" if label == "5" else "stock_session_return_pct"] = stock_return
            result[feature_name + "_pct"] = sign * (stock_return - market_return)

    # Use the same session in every archive, so later snapshots do not confer
    # extra previous-session history on one order or one source package.
    prior = raw.loc[raw.ts.gt(opening) & raw.ts.lt(stamp)].sort_values("ts", kind="stable")
    result["stock_atr_prior_bars"] = min(14, len(prior))
    if len(prior) < 14:
        result["stock_atr_status"] = "INSUFFICIENT_14_PRIOR_SESSION_MINUTES"
    else:
        window = prior.iloc[-14:]
        expected = pd.date_range(stamp - pd.Timedelta(minutes=14), stamp - pd.Timedelta(minutes=1), freq="min")
        status = _valid_window(window, expected)
        if len(prior) >= 15 and status == "OK":
            status = _valid_window(prior.iloc[-15:], pd.date_range(
                stamp-pd.Timedelta(minutes=15), stamp-pd.Timedelta(minutes=1), freq="min"))
        if status == "OK":
            previous = float(prior.iloc[-15].close) if len(prior) >= 15 else float(window.iloc[0].open)
            previous_closes = np.r_[previous, window.close.to_numpy(float)[:-1]]
            high, low = window.high.to_numpy(float), window.low.to_numpy(float)
            atr = float(np.maximum.reduce([high-low, abs(high-previous_closes), abs(low-previous_closes)]).mean())
            if atr <= 0 or not np.isfinite(atr):
                status = "INVALID_PRIOR_ATR"
            else:
                result["stock_prior14_atr"] = atr
                result["confirmation_range_atr"] = (float(row["confirmation_high"])-float(row["confirmation_low"]))/atr
                result["stock_atr_last_bar"] = window.iloc[-1].ts.isoformat()
        result["stock_atr_status"] = status
    return result


def _read_daily(path, expected):
    path = Path(path)
    if base.sha256(path) != expected:
        raise ValueError("Archived daily stock source changed")
    available = set(pq.ParquetFile(path).schema_arrow.names)
    fields = ["date", *OHLC] + [f for f in ("gap_filled", "opening_snapshot", "provisional_stale") if f in available]
    frame = pd.read_parquet(path, columns=fields)
    frame["ts"] = base.source.hybrid._to_ist(frame.date).dt.as_unit("ns")
    if base.sha256(path) != expected or frame.ts.isna().any():
        raise ValueError("Archived daily stock source changed while reading")
    return frame.sort_values("ts", kind="stable").reset_index(drop=True)


def _validate_cache(cache, source):
    manifest = json.loads((cache / "manifest.json").read_text(encoding="utf-8"))
    if (manifest["schema_version"] != SCHEMA or manifest["source_manifest_sha256"] != source["manifest_sha256"]
            or manifest["code_sha256"] != base.sha256(Path(__file__))):
        raise ValueError("Stock-context cache identity or implementation changed")
    for name, digest in manifest["files"].items():
        path = (cache / name).resolve()
        if not path.is_relative_to(cache.resolve()) or base.sha256(path) != digest:
            raise ValueError("Stock-context cache member changed")
    proofs = json.loads((cache / "source_proof.json").read_text(encoding="utf-8"))
    for path, digest in proofs["files"].items():
        if base.sha256(Path(path)) != digest:
            raise ValueError("Stock-context immutable source changed")
    features = pd.read_csv(cache / "features.csv").set_index("sid", drop=False)
    if not features.index.is_unique or set(features.index) != set(source["orders"].sid):
        raise ValueError("Stock-context order inventory differs")
    return features


def load_stock_context(source):
    """Return a SID-indexed frame, using only exact original archived inputs."""
    cache = CACHE.resolve()
    if (cache / "manifest.json").is_file():
        return _validate_cache(cache, source)
    if cache.exists() and any(p.name not in {"features.csv", "source_proof.json", "coverage.json"}
                              for p in cache.iterdir()):
        raise ValueError("Unexpected files in interrupted stock-context cache")
    records, proof = [], {}
    orders = source["orders"]
    audit = source["audit"].set_index("sid")
    historical_source, daily_source = source["provenance"]["sources"]
    history = Path(historical_source["source_run"])
    daily = Path(daily_source["source_run"])
    for path, expected in ((history / "manifest.json", historical_source["manifest_sha256"]),
                           (daily / "manifest.json", daily_source["manifest_sha256"])):
        if base.sha256(path) != expected:
            raise ValueError("Original stock-context source package changed")
        proof[str(path)] = expected
    history_manifest = json.loads((history / "manifest.json").read_text(encoding="utf-8"))
    if base.sha256(history / "summary.json") != history_manifest["files"]["summary.json"]:
        raise ValueError("Original historical summary changed")
    summary = json.loads((history / "summary.json").read_text(encoding="utf-8"))
    source_g3 = Path(summary["current_g3_source_run"])
    if base.sha256(source_g3 / "manifest.json") != summary["current_g3_manifest_sha256"]:
        raise ValueError("Original G3 history manifest changed")
    source_manifest = json.loads((source_g3 / "manifest.json").read_text(encoding="utf-8"))
    if base.sha256(source_g3 / "summary.json") != source_manifest["files"]["summary.json"]:
        raise ValueError("Original G3 source summary changed")
    original_summary = json.loads((source_g3 / "summary.json").read_text(encoding="utf-8"))
    prices = SealedPrices(original_summary, summary["price_sources"])
    daily_paths = {Path(path).name: (path, p["sha256"]) for path, p in daily_source["price_sources"].items()}
    daily_cache = {}
    manifests = {p: json.loads((p / "manifest.json").read_text(encoding="utf-8")) for p in (history, daily)}
    for count, row in enumerate(orders.sort_values(["tradingsymbol", "day", "confirmation_ts"]).to_dict("records"), 1):
        package = history if row["g8_source_package"] == history.name else daily
        if package == daily and row["g8_source_package"] != daily.name:
            raise ValueError("Unknown stock-context source package")
        if package == history:
            raw, key = prices.frame(pd.Timestamp(row["day"]).date(), row["tradingsymbol"])
            resolved = prices.resolution[key]
            path, digest = resolved["resolved_path"], resolved["sha256"]
        else:
            path, digest = daily_paths[row["tradingsymbol"] + "_stocks_indicators_1min.parquet"]
            if path not in daily_cache:
                daily_cache[path] = _read_daily(path, digest)
            raw = daily_cache[path]
        proof[str(path)] = digest
        relative = "nifty_inputs/" + str(row["day"])[:10] + ".parquet"
        nifty_path = package / relative
        if str(nifty_path) not in proof:
            digest = manifests[package]["files"][relative]
            if base.sha256(nifty_path) != digest:
                raise ValueError("Archived Nifty feature source changed")
            proof[str(nifty_path)] = digest
        result = stock_context(raw, row, audit.loc[int(row["sid"])] )
        result.update(stock_source_path=str(path), stock_source_sha256=proof[str(path)],
                      nifty_source_path=str(nifty_path), nifty_source_sha256=proof[str(nifty_path)])
        records.append(result)
        if count % 150 == 0:
            print(f"G8 exact stock context: {count}/{len(orders)}", flush=True)
    frame = pd.DataFrame(records).sort_values("sid").set_index("sid", drop=False)
    if not frame.index.is_unique or set(frame.index) != set(orders.sid):
        raise ValueError("Stock-context source inventory differs")
    cache.mkdir(parents=True, exist_ok=True)
    frame.to_csv(cache / "features.csv", index=False)
    _dump(cache / "source_proof.json", dict(files=proof, exact_original_bytes=True,
        opening_anchor="09:15 opening via first minute ending09:16", slot_anchor="matching completed5m OHLC",
        atr="last14 completed current-session1m bars before confirmation; no cross-session fill"))
    _dump(cache / "coverage.json", dict(orders=len(frame), status={column: dict(Counter(frame[column]))
        for column in ("stock_relative5_status", "stock_relative_session_status", "stock_atr_status")}))
    _dump(cache / "manifest.json", dict(schema_version=SCHEMA, source_manifest_sha256=source["manifest_sha256"],
        code_sha256=base.sha256(Path(__file__)), files={p.name: base.sha256(p) for p in cache.iterdir() if p.is_file()}))
    return _validate_cache(cache, source)


if __name__ == "__main__":
    data = load_stock_context(g8_sources.load_sources())
    print(data[["stock_relative5_status", "stock_relative_session_status", "stock_atr_status"]].apply(pd.Series.value_counts).to_string())
