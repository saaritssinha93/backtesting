from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from research.g3_trade_monitor_oi_data import join_decision_features, _stem


CONTRACT = "ABC26AUGFUT"


def archive(tmp_path, *, ends=None, oi=None, received=None):
    ends = ends or ["2026-08-10 09:20", "2026-08-10 09:25", "2026-08-10 09:30"]
    stamps = pd.to_datetime(ends).tz_localize("Asia/Kolkata")
    frame = pd.DataFrame(dict(timestamp=stamps, candle_start=stamps-pd.Timedelta(minutes=5),
        oi=oi or [100, 100.5, 101], close=[100, 99.9, 99.8][:len(ends)],
        tradingsymbol=CONTRACT, underlying="ABC", instrument_token=123,
        expiry=pd.Timestamp("2026-08-25"), quality_state="VALID",
        fetch_timestamp=(pd.to_datetime(received, format="mixed").tz_localize("Asia/Kolkata") if received
            else stamps+pd.Timedelta(seconds=30))))
    path = tmp_path / "raw_contracts_5m" / f"{_stem(CONTRACT)}_5minute.parquet"
    path.parent.mkdir(exist_ok=True)
    frame.to_parquet(path)
    return frame, path


def features(times, **extra):
    return pd.DataFrame(dict(day="2026-08-10", tradingsymbol="ABC", futures_tradingsymbol=CONTRACT,
        futures_instrument_token=123, decision_at_ist=[f"2026-08-10T{t}:00+05:30" for t in times], **extra))


def test_lag_blocks_future_bar_and_order_is_retained(tmp_path):
    archive(tmp_path)
    result, coverage, provenance = join_decision_features(features(["09:31", "09:30", "09:36", "09:37"], sid=[3,2,1,0]), tmp_path)
    assert result.sid.tolist() == [3,2,1,0]
    assert result.oi_valid.tolist() == [True, False, True, False]
    assert result.iloc[0].oi_change_10m_pct == pytest.approx(1)
    assert result.iloc[0].futures_return_10m_pct == pytest.approx(-.2)
    assert result.iloc[3].oi_reason == "STALE_OI_BAR"
    assert not result.point_in_time_proven.any()
    assert coverage.decision_rows.sum() == 4
    assert provenance["availability_label"] == "ASSUMED_AVAILABLE_REUSED_HISTORY"
    assert provenance["sources"][0]["sha256_before"] == provenance["sources"][0]["sha256_after"]


def test_missing_midpoint_never_uses_positional_lag(tmp_path):
    frame, path = archive(tmp_path)
    frame.iloc[[0,2]].to_parquet(path)
    result, _, _ = join_decision_features(features(["09:31"]), tmp_path)
    assert not result.iloc[0].oi_valid
    assert result.iloc[0].oi_reason == "MISSING_MIDPOINT_BAR"


@pytest.mark.parametrize("bad_oi", [0, -1, np.nan, np.inf])
def test_invalid_endpoint_or_midpoint_oi_fail_closed(tmp_path, bad_oi):
    archive(tmp_path, oi=[100,bad_oi,101])
    result, _, _ = join_decision_features(features(["09:31"]), tmp_path)
    assert not result.iloc[0].oi_valid
    assert pd.isna(result.iloc[0].oi_change_10m_pct)


def test_retrospective_receipt_cannot_be_used_earlier(tmp_path):
    archive(tmp_path, received=["2026-08-11 09:20", "2026-08-11 09:25", "2026-08-11 09:30"])
    result, _, provenance = join_decision_features(features(["09:31"]), tmp_path, "received_time")
    assert not result.oi_valid.any()
    assert not result.point_in_time_proven.any()
    assert provenance["availability_label"] == "RETAINED_REVISION_RECEIPT_CONSTRAINED"


def test_all_three_receipts_must_precede_decision(tmp_path):
    archive(tmp_path, received=["2026-08-10 09:33", "2026-08-10 09:25:30", "2026-08-10 09:30:30"])
    result, _, _ = join_decision_features(features(["09:31", "09:33"]), tmp_path, "received_time")
    assert result.oi_valid.tolist() == [False, True]
    assert result.point_in_time_proven.tolist() == [False, True]


@pytest.mark.parametrize("mutate,reason", [
    (lambda f:f.assign(futures_instrument_token=999), "TRADE_TOKEN_MISMATCH"),
    (lambda f:f.assign(tradingsymbol="OTHER"), "TRADE_UNDERLYING_MISMATCH"),
])
def test_trade_identity_is_checked(tmp_path, mutate, reason):
    archive(tmp_path)
    result, _, _ = join_decision_features(mutate(features(["09:31"])), tmp_path)
    assert not result.iloc[0].oi_valid
    assert result.iloc[0].oi_reason == reason


def test_contract_does_not_roll_into_other_expiry(tmp_path):
    archive(tmp_path)
    result, _, _ = join_decision_features(features(["09:31"]).assign(futures_tradingsymbol="ABC26SEPFUT"), tmp_path)
    assert result.iloc[0].oi_reason == "MISSING_CONTRACT_ARCHIVE"


def test_missing_contract_uses_only_dated_universe(tmp_path):
    archive(tmp_path)
    universe = tmp_path / "universe"
    universe.mkdir()
    pd.DataFrame(dict(underlying=["ABC"], tradingsymbol=[CONTRACT])).to_parquet(universe / "near_month_2026-08-10.parquet")
    result, _, provenance = join_decision_features(features(["09:31"]).drop(columns=["futures_tradingsymbol"]), tmp_path)
    assert result.iloc[0].oi_valid
    assert result.iloc[0].oi_identity_source == "DATED_NEAR_MONTH_UNIVERSE"
    assert len(provenance["sources"]) == 2


def test_missing_archive_keeps_cohort(tmp_path):
    result, coverage, _ = join_decision_features(features(["09:31", "09:32"]), tmp_path)
    assert len(result) == coverage.decision_rows.sum() == 2
    assert not result.oi_valid.any()


def test_duplicate_timestamp_is_rejected(tmp_path):
    frame, path = archive(tmp_path)
    pd.concat([frame, frame.iloc[[0]]], ignore_index=True).to_parquet(path)
    result, _, _ = join_decision_features(features(["09:31"]), tmp_path)
    assert result.iloc[0].oi_reason == "DUPLICATE_BAR"


def test_current_bad_bar_does_not_fall_back_to_older_valid_bar(tmp_path):
    frame, path = archive(tmp_path)
    new = frame.iloc[[-1]].copy()
    new.timestamp += pd.Timedelta(minutes=5)
    new.fetch_timestamp += pd.Timedelta(minutes=5)
    new.oi = 0
    pd.concat([frame, new], ignore_index=True).to_parquet(path)
    result, _, _ = join_decision_features(features(["09:36"]), tmp_path)
    assert not result.iloc[0].oi_valid
    assert result.iloc[0].oi_current_bar_end.minute == 35


def test_empty_features(tmp_path):
    result, coverage, provenance = join_decision_features(features([]), tmp_path)
    assert result.empty and coverage.empty
    assert provenance["valid_decision_rows"] == 0


def test_previous_session_cannot_supply_missing_endpoints(tmp_path):
    archive(tmp_path, ends=["2026-08-07 15:20", "2026-08-07 15:25", "2026-08-10 09:30"])
    result, _, _ = join_decision_features(features(["09:31"]), tmp_path)
    assert not result.iloc[0].oi_valid
    assert result.iloc[0].oi_reason == "MISSING_MIDPOINT_BAR"


def test_unrelated_sessions_are_not_used(tmp_path):
    archive(tmp_path, ends=["2026-08-07 09:20", "2026-08-07 09:25", "2026-08-07 09:30"])
    result, _, _ = join_decision_features(features(["09:31"]), tmp_path)
    assert not result.iloc[0].oi_valid
    assert result.iloc[0].oi_reason == "NO_AVAILABLE_SESSION_BAR"


def test_input_prices_and_oi_are_preserved_for_reproducibility(tmp_path):
    archive(tmp_path)
    result, _, _ = join_decision_features(features(["09:31"]), tmp_path)
    row = result.iloc[0]
    assert [row.oi_current_value, row.oi_midpoint_value, row.oi_reference_value] == [101, 100.5, 100]
    assert row.oi_current_futures_close == 99.8
    assert row.oi_reference_futures_close == 100
    assert len(row.oi_source_sha256) == 64


def test_expired_contract_is_not_carried_forward(tmp_path):
    frame, path = archive(tmp_path)
    frame.expiry = pd.Timestamp("2026-08-07")
    frame.to_parquet(path)
    result, _, _ = join_decision_features(features(["09:31"]), tmp_path)
    assert result.iloc[0].oi_reason == "EXPIRED_CONTRACT"


def test_hash_change_while_reading_stops_research(tmp_path, monkeypatch):
    archive(tmp_path)
    import research.g3_trade_monitor_oi_data as data
    hashes = iter(["before", "after"])
    monkeypatch.setattr(data, "_sha", lambda path:next(hashes))
    with pytest.raises(RuntimeError, match="changed while reading"):
        data.join_decision_features(features(["09:31"]), tmp_path)


def test_conflicting_explicit_contracts_fail_closed(tmp_path):
    archive(tmp_path)
    result, _, _ = join_decision_features(features(["09:31"]).assign(opening_oi_contract="ABC26SEPFUT"), tmp_path)
    assert not result.iloc[0].oi_valid
    assert result.iloc[0].oi_reason == "CONFLICTING_CONTRACT_IDENTITY"
