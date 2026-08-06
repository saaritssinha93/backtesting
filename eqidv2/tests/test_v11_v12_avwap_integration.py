from __future__ import annotations

from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

import avwap_5min_ID_v11_backtesting as v11
import avwap_5min_ID_v7_candidate_scan as v7_scan
import eqidv2_late_bb10_compression as late_bb10
import avwap_5min_ID_v12_backtesting as v12


BACKTEST_MODULES = (v11, v12)


def _bars_with_ineligible_rows() -> pd.DataFrame:
    timestamps = pd.to_datetime(
        [
            "2026-08-05 09:15:00+05:30",
            "2026-08-05 09:20:00+05:30",
            "2026-08-05 09:25:00+05:30",
            "2026-08-05 09:30:00+05:30",
            "2026-08-05 09:35:00+05:30",
        ]
    )
    close = [500.0, 100.0, 999.0, 104.0, 888.0]
    frame = pd.DataFrame(
        {
            "date": timestamps,
            "open": close,
            "high": close,
            "low": close,
            "close": close,
            "volume": [1_000.0, 10.0, 5_000.0, 30.0, 4_000.0],
            "ATR": [2.0] * 5,
            "VWAP": [-1.0] * 5,
            "AVWAP": [-2.0] * 5,
            "opening_snapshot": [1, 0, 0, 0, 0],
            "gap_filled": [0, 0, 1, 0, 0],
            "source_1m_count": [5, 5, 5, 5, 4],
        }
    )
    frame["date_only"] = frame["date"].dt.date
    return frame


@pytest.mark.parametrize("module", BACKTEST_MODULES)
def test_tier123_uses_shared_causal_avwap_contract(module) -> None:
    prepared = module._tier123_prepare_5m(_bars_with_ineligible_rows())

    assert np.isnan(prepared.loc[0, "AVWAP"])
    assert prepared.loc[1, "AVWAP"] == pytest.approx(100.0)
    assert np.isnan(prepared.loc[2, "AVWAP"])
    assert prepared.loc[3, "AVWAP"] == pytest.approx(103.0)
    assert np.isnan(prepared.loc[4, "AVWAP"])
    assert np.isnan(prepared.loc[2, "avwap_dist_atr"])

    # Session VWAP remains a distinct feature and intentionally includes the
    # ordinary session-VWAP input population.
    assert prepared.loc[3, "VWAP"] != pytest.approx(prepared.loc[3, "AVWAP"])
    assert "vwap_dist_atr" in prepared
    assert "avwap_dist_atr" in prepared


@pytest.mark.parametrize("module", BACKTEST_MODULES)
def test_avwap_setup_profile_gate_uses_avwap_distance(module) -> None:
    signals = pd.DataFrame(
        {
            "setup": ["B_AVWAP_RECLAIM_REVERSAL", "B_AVWAP_RECLAIM_REVERSAL"],
            "signal_time_ist": ["2026-08-05 11:00:00+05:30"] * 2,
            "vwap_dist_atr": [10.0, 10.0],
            "avwap_dist_atr": [0.7, 0.2],
        }
    )

    selected = module._selected_strategy_mask(
        signals, "production_core_ab_max_pnl_low_valid"
    )
    assert selected.tolist() == [True, False]

    missing_avwap = signals.drop(columns="avwap_dist_atr").iloc[:1]
    assert not module._selected_strategy_mask(
        missing_avwap, "production_core_ab_max_pnl_low_valid"
    ).iloc[0]


@pytest.mark.parametrize("module", BACKTEST_MODULES)
def test_legacy_conf_term_and_top_n_are_routed_by_setup_name(module, monkeypatch) -> None:
    conf = SimpleNamespace(
        FINAL_SETUP_CONF={
            "DOC5D_AVWAP_RECLAIM_LONG": {
                "mask_terms": [["vwap_dist_atr", ">=", 0.0]],
                "entry_guards": {"top_n": 2},
            }
        }
    )
    monkeypatch.setattr(module, "_load_final_setup_conf_module", lambda: conf)
    signals = pd.DataFrame(
        {
            "ticker": ["A", "B", "C"],
            "setup": ["DOC5D_AVWAP_RECLAIM_LONG"] * 3,
            "signal_time_ist": ["2026-08-05 11:00:00+05:30"] * 3,
            # Reversed rankings prove which feature the Top-N actually used.
            "vwap_dist_atr": [1.0, 2.0, 3.0],
            "avwap_dist_atr": [3.0, 2.0, 1.0],
        }
    )

    selected = module._final_setup_conf_mask(signals)
    assert signals.loc[selected, "ticker"].tolist() == ["A", "B"]
    assert module._setup_feature_column("E_VWAP_BAND_FADE", "vwap_dist_atr") == "vwap_dist_atr"


def test_v12_window_loader_requests_avwap_eligibility_flags() -> None:
    assert {"AVWAP", "opening_snapshot", "gap_filled", "source_1m_count"}.issubset(
        v12._V12_HISTORICAL_5M_COLUMNS
    )


def test_v7_v11_v12_share_the_late_bb10_detector_module() -> None:
    assert v7_scan.late_bb10 is late_bb10
    assert v11.candidate_scan.late_bb10 is late_bb10
    assert v12.candidate_scan.late_bb10 is late_bb10
