"""Prevent source mutation, mixed strategy versions and silently lost candidates."""
import json

import numpy as np
import pandas as pd
import pytest

from research import g9_dual_market_sources as sources


def _manifest(directory, files):
    path = directory / "manifest.json"
    path.write_text(json.dumps({"files": files}), encoding="utf-8")
    return sources.sha(path)


def test_source_package_detects_changed_member(tmp_path):
    member = tmp_path / "orders.csv"
    member.write_text("original", encoding="utf-8")
    expected = _manifest(tmp_path, {member.name: sources.sha(member)})
    member.write_text("tampered", encoding="utf-8")
    with pytest.raises(ValueError, match="source member changed"):
        sources._verify_package(tmp_path, expected)


def test_source_manifest_cannot_escape_package(tmp_path):
    directory = tmp_path / "package"
    directory.mkdir()
    outside = tmp_path / "outside.csv"
    outside.write_text("outside", encoding="utf-8")
    expected = _manifest(directory, {"../outside.csv": sources.sha(outside)})
    with pytest.raises(ValueError, match="source member changed"):
        sources._verify_package(directory, expected)


def test_missing_cache_never_invokes_legacy_builder(tmp_path, monkeypatch):
    monkeypatch.setattr(sources, "CACHE", tmp_path / "absent")
    def fail_if_called(**kwargs):
        pytest.fail("Missing cache must fail before the legacy source loader")
    monkeypatch.setattr(sources.g8_sources, "load_sources", fail_if_called)
    with pytest.raises(ValueError, match="existing G7 source package is missing"):
        sources.load_sources()
    assert not (tmp_path / "absent").exists()


def test_later_date_cannot_silently_reuse_old_history():
    with pytest.raises(ValueError, match="ends at 2026-10-23"):
        sources.load_sources(through="2026-10-26")


def test_minute_path_cannot_include_confirmation_bar():
    row = dict(sid=1, day="2026-10-23", setup_id="1011_LONG", tradingsymbol="EXAMPLE",
               side="LONG", confirmation_ts="2026-10-23 10:11:00+05:30")
    frames = {name: pd.DataFrame([row]) for name in ("orders", "unfiltered", "baseline", "audit")}
    path = {"timestamp_ns": np.array([pd.Timestamp(row["confirmation_ts"]).value]),
            **{field: np.array([100.]) for field in ("open", "high", "low", "close")}}
    with pytest.raises(ValueError, match="Noncausal"):
        sources._validate_inventory(frames, {1: path}, [row["day"]])


@pytest.fixture
def futures_candles(tmp_path, monkeypatch):
    monkeypatch.setattr(sources, "DATA_ROOT", tmp_path)
    directory = tmp_path / "fno_oi/raw_contracts_5m"
    directory.mkdir(parents=True)
    path = directory / "EXAMPLE26OCTFUT_5minute.parquet"
    timestamps = pd.date_range("2026-10-23 09:55:00", periods=3, freq="5min", tz="Asia/Kolkata")
    frame = pd.DataFrame(dict(timestamp=timestamps, candle_start=timestamps - pd.Timedelta(minutes=5),
                             tradingsymbol="EXAMPLE26OCTFUT", close=[100., 102., 900.],
                             oi=[1000., 1100., 9000.], quality_state="VALID", is_synthetic=False,
                             gap_filled=False))
    orders = pd.DataFrame([dict(sid=9, signal_ts="2026-10-23 10:00:00+05:30",
                               futures_tradingsymbol="EXAMPLE26OCTFUT", oi=1100., prev_oi=1000.)])
    frame.to_parquet(path, index=False)
    return frame, orders, path


def test_futures_context_uses_exact_past_candles_and_matches_oi(futures_candles):
    frame, orders, path = futures_candles
    result, provenance = sources.futures_context(orders)
    assert bool(result.loc[9, "futures_context_available"])
    assert result.loc[9, "futures_ret_5m"] == pytest.approx(.02)
    assert result.loc[9, "futures_ret_5m_pct"] == pytest.approx(2.)
    assert result.loc[9, "futures_oi_change_pct"] == pytest.approx(10.)
    assert result.loc[9, "futures_source_sha256"] == sources.sha(path)
    assert not provenance["real_time_receive_time_verified"]
    assert not provenance["oi_freshness_verified"]
    # A later bar cannot alter the feature or be used as a fallback.
    frame.loc[2, "close"] = 1.
    frame.loc[2, "oi"] = 1.
    frame.to_parquet(path, index=False)
    later_changed, _ = sources.futures_context(orders)
    assert later_changed.loc[9, "futures_ret_5m"] == result.loc[9, "futures_ret_5m"]


def test_futures_context_does_not_forward_fill_missing_previous_bar(futures_candles):
    frame, orders, path = futures_candles
    frame.iloc[1:].to_parquet(path, index=False)
    result, _ = sources.futures_context(orders)
    assert not result.loc[9, "futures_context_available"]
    assert result.loc[9, "futures_context_reason"] == "MISSING_EXACT_PREVIOUS_BAR"


def test_futures_context_rejects_mismatched_candidate_oi(futures_candles):
    _, orders, _ = futures_candles
    result, _ = sources.futures_context(orders.assign(prev_oi=999.))
    assert not result.loc[9, "futures_context_available"]
    assert result.loc[9, "futures_context_reason"] == "CANDIDATE_OI_MISMATCH"


@pytest.mark.parametrize("column,value,reason", [
    ("is_synthetic", True, "SYNTHETIC_GAP_OR_STALE_CANDLE"),
    ("gap_filled", True, "SYNTHETIC_GAP_OR_STALE_CANDLE"),
    ("quality_state", "STALE", "INVALID_CANDLE_QUALITY"),
    ("tradingsymbol", "OTHER26OCTFUT", "CONTRACT_IDENTITY_MISMATCH"),
])
def test_futures_context_rejects_unreliable_bars(futures_candles, column, value, reason):
    frame, orders, path = futures_candles
    frame.loc[0, column] = value
    frame.to_parquet(path, index=False)
    result, _ = sources.futures_context(orders)
    assert not result.loc[9, "futures_context_available"]
    assert result.loc[9, "futures_context_reason"] == reason


@pytest.mark.skipif(not (sources.CACHE / "manifest.json").is_file(), reason="Pinned local trading data unavailable")
def test_real_g7_v2_sources_reproduce_all_candidates_and_october23():
    result = sources.load_sources()
    assert result["config"]["variant_id"] == "V13-V10-G-7-G6-REPLICA-V2"
    assert len(result["orders"]) == len(result["paths"]) == 582
    assert result["orders"].sid.is_unique
    assert result["days"][0] == "2026-07-29"
    assert result["days"][-1] == "2026-10-23"
    assert len(result["days"]) == 57
    assert result["baseline_metrics"]["trades"] == 437
    assert result["baseline_metrics"]["net_pnl_rs"] == pytest.approx(1069645.367666155, abs=1e-6)
    assert result["unfiltered"].filled.sum() == 514
    assert set(result["orders"].loc[result["orders"].day.eq("2026-10-23"), "sid"]) == set(range(572, 583))
    assert not result["capability_audit"].true_ofi_available.any()
    assert not result["capability_audit"].true_cvd_available.any()
    assert result["provenance"]["baseline_allocation_reproduced"] is True
