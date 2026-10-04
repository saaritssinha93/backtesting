from __future__ import annotations

import json
from datetime import datetime, timezone

import pandas as pd
import pytest

from ai_platform.observability.data_quality import (
    AppendOnlyObservationLedger,
    canonical_frame_sha256,
    evaluate_ohlcv,
    hash_input_window,
)
from ai_platform.observability.feature_ledger import (
    build_v13_v10_g_feature_ledger,
    evaluate_v13_v10_g_base_row,
    read_feature_ledger,
    write_feature_ledger,
)


def _bars() -> pd.DataFrame:
    stamps = pd.date_range("2026-09-21 09:16", periods=3, freq="min", tz="Asia/Kolkata")
    return pd.DataFrame(
        {
            "ts": stamps,
            "open": [100.0, 101.0, 102.0],
            "high": [102.0, 103.0, 104.0],
            "low": [99.0, 100.0, 101.0],
            "close": [101.0, 102.0, 103.0],
            "volume": [10, 11, 12],
            "oi": [1000, 1005, 1010],
        }
    )


def _passing_long() -> dict:
    signal = pd.Timestamp("2026-09-21 09:30", tz="Asia/Kolkata")
    return {
        "day": signal.date(),
        "signal_ts": signal,
        "confirmation_ts": signal + pd.Timedelta(minutes=1),
        "v9_1m_feature_ts": signal + pd.Timedelta(minutes=1),
        "signal_end": "09:30",
        "confirmation_end": "09:31",
        "tradingsymbol": "TEST",
        "futures_tradingsymbol": "TEST26SEPFUT",
        "open": 99.5,
        "high": 100.5,
        "low": 99.0,
        "close": 100.0,
        "volume": 20_000,
        "signal_close": 100.0,
        "prev_close": 99.2,
        "ema9": 101.0,
        "ema20": 100.0,
        "ema50": 99.0,
        "price_change_pct": 0.8,
        "oi": 1005.0,
        "prev_oi": 1000.0,
        "oi_change_pct": 0.5,
        "volume_ratio": 2.0,
        "traded_value": 2_000_000.0,
        "confirmation_open": 100.0,
        "confirmation_high": 102.0,
        "confirmation_low": 99.5,
        "confirmation_close": 101.5,
        "confirmation_volume": 10_000,
        "body_ratio": 0.6,
        "v9_1m_upper_wick_ratio": 0.2,
        "v9_1m_lower_wick_ratio": 0.2,
        "v9_1m_volume_ratio": 1.5,
        "v9_exact_confirmation_present": True,
    }


def test_frame_and_input_window_fingerprints_are_deterministic() -> None:
    bars = _bars()
    reversed_bars = bars.iloc[::-1].reset_index(drop=True)

    assert canonical_frame_sha256(bars) != canonical_frame_sha256(reversed_bars)
    assert canonical_frame_sha256(bars, sort_by=["ts"]) == canonical_frame_sha256(
        reversed_bars, sort_by=["ts"]
    )

    fingerprint = hash_input_window(
        bars,
        end=bars.ts.iloc[1],
        columns=["ts", "close", "volume"],
    )
    assert fingerprint.row_count == 2
    assert fingerprint.last_timestamp == bars.ts.iloc[1].isoformat()
    assert len(fingerprint.sha256) == 64


def test_quality_report_distinguishes_zero_oi_and_blocking_gaps() -> None:
    bars = _bars()
    bars.loc[1, "oi"] = 0
    warning = evaluate_ohlcv(
        bars,
        expected_timestamps=bars.ts,
        source="NFO_FUTURE",
        symbol="TEST26SEPFUT",
    )
    assert warning.status == "WARN"
    assert warning.usable is True
    assert warning.zero_oi_count == 1

    blocked = evaluate_ohlcv(
        bars.iloc[:2],
        expected_timestamps=bars.ts,
        source="NFO_FUTURE",
        symbol="TEST26SEPFUT",
    )
    assert blocked.status == "BLOCKED"
    assert blocked.missing_timestamp_count == 1
    assert blocked.missing_timestamps == (bars.ts.iloc[2].isoformat(),)


def test_append_only_observation_ledger_detects_tampering(tmp_path) -> None:
    ledger = AppendOnlyObservationLedger(tmp_path / "observations")
    path = ledger.append(
        "raw_equity_1m",
        {"symbol": "TEST", "rows": 3, "sha256": "a" * 64},
        observed_at=datetime(2026, 9, 21, 4, 0, tzinfo=timezone.utc),
        identity={"run_id": "run-1", "slot": "09:20"},
    )
    verified = ledger.verify()
    assert verified.valid is True
    assert verified.record_count == 1

    record = json.loads(path.read_text(encoding="utf-8"))
    record["payload"]["rows"] = 4
    path.write_text(json.dumps(record), encoding="utf-8")
    failed = ledger.verify()
    assert failed.valid is False
    assert failed.invalid_count == 1
    assert "digest mismatch" in failed.errors[0]


def test_v13_g_feature_ledger_explains_pass_and_rejection() -> None:
    passing = _passing_long()
    rejected = {**passing, "tradingsymbol": "ZEROOI", "oi": 0.0}
    result = build_v13_v10_g_feature_ledger(
        pd.DataFrame([passing, rejected]), nifty_return=-0.10
    )

    assert len(result) == 2
    accepted = result.loc[result.tradingsymbol.eq("TEST")].iloc[0]
    assert accepted.base_side == "LONG"
    assert bool(accepted.strict_signal_pass) is True
    assert bool(accepted.setup_filter_pass) is True
    assert accepted.first_failed_gate == ""
    assert len(accepted.feature_values_sha256) == 64
    assert len(accepted.ledger_row_sha256) == 64

    zero_oi = result.loc[result.tradingsymbol.eq("ZEROOI")].iloc[0]
    assert zero_oi.base_side == ""
    assert bool(zero_oi.gate_oi_pair_positive) is False
    assert "gate_oi_pair_positive" in json.loads(zero_oi.failed_gates)


def test_v13_g_nifty_gate_is_visible_for_0925_short() -> None:
    row = _passing_long()
    signal = pd.Timestamp("2026-09-21 09:25", tz="Asia/Kolkata")
    row.update(
        signal_ts=signal,
        confirmation_ts=signal + pd.Timedelta(minutes=1),
        v9_1m_feature_ts=signal + pd.Timedelta(minutes=1),
        signal_end="09:25",
        confirmation_end="09:26",
        ema9=99.0,
        ema20=100.0,
        ema50=101.0,
        price_change_pct=-0.5,
        confirmation_open=100.0,
        confirmation_high=100.5,
        confirmation_low=98.0,
        confirmation_close=98.5,
    )
    rejected = evaluate_v13_v10_g_base_row(row, nifty_return=0.20)
    assert rejected["gate_nifty_0925_short"] is False
    assert rejected["base_short_pass"] is False


def test_feature_ledger_artifact_is_verifiable(tmp_path) -> None:
    frame = build_v13_v10_g_feature_ledger(
        pd.DataFrame([_passing_long()]), nifty_return=-0.10
    )
    path = tmp_path / "feature_ledger.csv"
    manifest = write_feature_ledger(frame, path)

    loaded = read_feature_ledger(path)
    assert len(loaded) == 1
    assert manifest["row_count"] == 1
    assert manifest["artifact_sha256"]

    with path.open("ab") as stream:
        stream.write(b"tampered\n")
    with pytest.raises(ValueError, match="artifact digest mismatch"):
        read_feature_ledger(path)
