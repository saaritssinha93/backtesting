"""Read-only 09:50 LONG counterfactual for October 8, 9 and 12."""
from __future__ import annotations

import hashlib
import json
import sys
from dataclasses import asdict, replace
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
import fno_v13_v10_g_3_contract as contract

OUT = Path(r"C:\TradingData\eqidv2\fno_oi\strategy_research\v13_v10_g_3_0950_long_20261012")
G = Path(r"C:\TradingData\eqidv2\backtesting_result_v13_v10_g\runs")
LIVE = Path(r"C:\TradingData\eqidv2\fno_oi\v13_v10_g_3_live")
RUNS = {
    "2026-10-08": G / "2026-10-08/20261008T164156506767",
    "2026-10-09": G / "2026-10-09/20261009T162013087486",
}


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def check(row: dict, setup: contract.SetupSpec) -> dict:
    side = contract.base_signal_side(row, "09:50")
    checks = {
        "base_long": side == "LONG",
        "confirmation_direction": contract._confirmation_valid(contract._candidate(row), "LONG"),
        "setup_5m_price": float(row["price_change_pct"]) >= setup.price_change_pct,
        "setup_oi": float(row["oi_change_pct"]) >= setup.oi_change_pct,
        "setup_5m_volume": float(row["volume_ratio"]) >= setup.volume_ratio,
        "confirmation_body": float(row["body_ratio"]) >= setup.body_ratio,
        "confirmation_wick": float(row["wick_ratio"]) <= setup.max_wick_ratio,
        "confirmation_volume": float(row["v9_1m_volume_ratio"]) >= contract.confirmation_volume_minimum("LONG"),
    }
    passed = contract.passes_selected_filters(row, setup)
    if passed != all(checks.values()):
        raise AssertionError((row["tradingsymbol"], checks, passed))
    return dict(symbol=row["tradingsymbol"], passed=passed,
                failed=[name for name, good in checks.items() if not good],
                price_change_pct=float(row["price_change_pct"]),
                oi_change_pct=float(row["oi_change_pct"]),
                five_minute_volume_ratio=float(row["volume_ratio"]),
                confirmation_body_ratio=float(row["body_ratio"]),
                confirmation_wick_ratio=float(row["wick_ratio"]),
                confirmation_volume_ratio=float(row["v9_1m_volume_ratio"]),
                confirmation_high=float(row.get("confirm_high", row.get("confirmation_high"))))


def historical(day: str, setup: contract.SetupSpec) -> dict:
    run = RUNS[day]
    manifest = json.loads((run / "feature_ledger.csv.manifest.json").read_text())
    if sha(run / "feature_ledger.csv") != manifest["artifact_sha256"]:
        raise RuntimeError(f"Feature ledger hash mismatch {day}")
    signals = pd.read_csv(run / "candidate_signals.csv", float_precision="round_trip")
    raw = pd.read_csv(run / "feature_ledger.csv", float_precision="round_trip")
    base = raw.loc[raw.signal_end.eq("09:50") & raw.base_long_pass.astype(str).str.lower().eq("true")]
    strict = signals.loc[signals.hhmm_int.eq(950) & signals.side.eq("LONG")]
    records = []
    for row in strict.to_dict("records"):
        row["signal_timestamp"] = row["signal_ts"]
        row["confirmation_timestamp"] = row["confirmation_ts"]
        row["confirm_open"] = row["confirmation_open"]
        row["confirm_high"] = row["confirmation_high"]
        row["confirm_low"] = row["confirmation_low"]
        row["confirm_close"] = row["confirmation_close"]
        row["ema9"] = row.get("ema9", row.get("v9_5m_ema9"))
        row["ema20"] = row.get("ema20", row.get("v9_5m_ema20"))
        row["ema50"] = row.get("ema50", row.get("v9_5m_ema50"))
        records.append(check(row, setup))
    if any(x["passed"] for x in records):
        raise RuntimeError("Historical day has qualifying candidate; execution replay required")
    return dict(date=day, source="verified_after_close_G_feature_ledger_and_strict_pool",
                five_minute_base_long_count=len(base), strict_1m_long_count=len(strict),
                selected_count=0, possible_entries=0, candidates=records,
                sources={"feature_ledger": str(run / "feature_ledger.csv"),
                         "feature_ledger_sha256": sha(run / "feature_ledger.csv"),
                         "candidate_signals": str(run / "candidate_signals.csv"),
                         "candidate_signals_sha256": sha(run / "candidate_signals.csv")})


def today(setup: contract.SetupSpec) -> dict:
    day = "2026-10-12"
    scanner_path = LIVE / "scanner_5m" / day / "slot_0950.json"
    confirmation_path = LIVE / "confirmation_1m" / day / "slot_0951.json"
    scanner = json.loads(scanner_path.read_text())
    confirmation = json.loads(confirmation_path.read_text())
    if scanner["strategy_fingerprint"] != confirmation["strategy_fingerprint"]:
        raise RuntimeError("Live source strategy fingerprints differ")
    rows = {r["tradingsymbol"]: r for r in confirmation["feature_evaluations"]}
    symbols = [r["tradingsymbol"] for r in scanner["candidates"] if r["side"] == "LONG"]
    if len(symbols) != 5 or set(symbols) != set(rows):
        raise RuntimeError("Live 09:50 candidate/confirmation mismatch")
    records = []
    for symbol in symbols:
        row = rows[symbol].copy()
        row.update(side="LONG", signal_timestamp=row["signal_ts"],
                   confirmation_timestamp=row["confirmation_ts"],
                   v9_1m_feature_ts=row["confirmation_ts"],
                   confirm_open=row["confirmation_open"], confirm_high=row["confirmation_high"],
                   confirm_low=row["confirmation_low"], confirm_close=row["confirmation_close"])
        records.append(check(row, setup))
    if any(x["passed"] for x in records):
        raise RuntimeError("Today has qualifying candidate; trigger replay required")
    return dict(date=day, source="saved_live_scanner_and_confirmation_packets",
                five_minute_base_long_count=len(symbols), confirmation_rows=len(rows),
                selected_count=0, possible_entries=0, candidates=records,
                sources={"scanner": str(scanner_path), "scanner_sha256": sha(scanner_path),
                         "confirmation": str(confirmation_path),
                         "confirmation_sha256": sha(confirmation_path)})


def main() -> None:
    donor = contract._ACTIVE_BY_ID["0956_LONG"]
    setup = replace(donor, signal_end="09:50", confirmation_end="09:51")
    result = dict(rule="temporary 09:50 LONG cloned from 09:55 LONG",
                  setup=asdict(setup), days=[historical(day, setup) for day in RUNS])
    result["days"].append(today(setup))
    target = OUT / "recent_0950_long_20261008_09_12.json"
    target.write_text(json.dumps(result, indent=2, allow_nan=False) + "\n", encoding="utf-8")
    for day in result["days"]:
        print(day["date"], "base", day["five_minute_base_long_count"],
              "strict", day.get("strict_1m_long_count", day.get("confirmation_rows")),
              "pass", day["selected_count"])
        for item in day["candidates"]:
            print(" ", item["symbol"], item["failed"])
    print(target)


if __name__ == "__main__":
    main()
