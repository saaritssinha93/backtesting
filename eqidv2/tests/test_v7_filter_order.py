"""
Phase 0 characterization tests: filter ordering in signal discovery scanner.

Confirms that after the fix (merge V11 BEFORE apply_research_live_filters),
V11 LONG candidates are rejected when SHORT_FOCUS is enabled, and
V11 candidates pass through anti-chase rules — i.e., they are not exempt
from global policy gates.

Never writes to C:\\TradingData.
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pandas as pd
import pytest

# Import the filter functions directly from the scanner module.
# We test the functions in isolation so no live data is touched.
import eqidv2_signal_discovery_v7_5min_id_persistent as scanner


def _make_candidate(ticker="RELIANCE", side="LONG", setup="C_OR_BREAKOUT",
                    close_loc=0.70, vwap_dist_atr=1.0, rs_pct=0.5,
                    candidate_source="V11") -> pd.DataFrame:
    return pd.DataFrame([{
        "ticker": ticker,
        "side": side,
        "setup": setup,
        "close_loc": close_loc,
        "vwap_dist_atr": vwap_dist_atr,
        "rs_pct": rs_pct,
        "vol_ratio": 2.0,
        "atr_pct": 0.004,
        "body_pct": 0.60,
        "quality_score": 80.0,
        "ranker_score": 0.75,
        "market_ret_pct": 0.1,
        "signal_time_ist": "2026-06-07 10:00:00+05:30",
        "candidate_source": candidate_source,
        "candidate_family": "V7_STANDARD",
        "selection_mode": "v8_setup_compatible",
    }])


# ---------------------------------------------------------------------------
# short-focus rejects LONG candidates regardless of source
# ---------------------------------------------------------------------------

def test_short_focus_rejects_v11_long_candidate(monkeypatch):
    monkeypatch.setattr(scanner, "SHORT_FOCUS_ENABLE", True)
    monkeypatch.setattr(scanner, "SHORT_FOCUS_ALLOWED_SIDES", {"SHORT"})
    monkeypatch.setattr(scanner, "RESEARCH_LIVE_FILTER_ENABLE", True)

    df = _make_candidate(side="LONG", candidate_source="V11")
    gated, rejected, stats = scanner.apply_research_live_filters(df, "2026-06-07")

    assert len(gated) == 0, "V11 LONG must be rejected when SHORT_FOCUS=True"
    assert len(rejected) == 1
    assert "SHORT_FOCUS" in str(rejected.iloc[0]["research_live_filter_reason"])


def test_short_focus_passes_v11_short_candidate(monkeypatch):
    monkeypatch.setattr(scanner, "SHORT_FOCUS_ENABLE", True)
    monkeypatch.setattr(scanner, "SHORT_FOCUS_ALLOWED_SIDES", {"SHORT"})
    monkeypatch.setattr(scanner, "RESEARCH_LIVE_FILTER_ENABLE", True)

    df = _make_candidate(side="SHORT", setup="S_BB_SQUEEZE_SHORT", candidate_source="V11")
    gated, rejected, stats = scanner.apply_research_live_filters(df, "2026-06-07")

    assert len(gated) == 1, "V11 SHORT must pass when SHORT_FOCUS=True"


# ---------------------------------------------------------------------------
# anti-chase rejects overextended LONG regardless of source
# ---------------------------------------------------------------------------

def test_anti_chase_rejects_v11_long_overextended(monkeypatch):
    monkeypatch.setattr(scanner, "SHORT_FOCUS_ENABLE", False)
    monkeypatch.setattr(scanner, "RESEARCH_LIVE_FILTER_ENABLE", True)
    monkeypatch.setattr(scanner, "LONG_ANTI_CHASE_CLOSE_LOC_GT", 0.97)
    monkeypatch.setattr(scanner, "LONG_ANTI_CHASE_VWAP_DIST_ATR_GT", 3.50)

    df = _make_candidate(side="LONG", close_loc=0.98, vwap_dist_atr=4.0,
                         candidate_source="V11")
    gated, rejected, stats = scanner.apply_research_live_filters(df, "2026-06-07")

    assert len(gated) == 0, "Anti-chase must block overextended V11 LONG"


def test_anti_chase_passes_normal_long(monkeypatch):
    monkeypatch.setattr(scanner, "SHORT_FOCUS_ENABLE", False)
    monkeypatch.setattr(scanner, "RESEARCH_LIVE_FILTER_ENABLE", True)
    monkeypatch.setattr(scanner, "LONG_ANTI_CHASE_CLOSE_LOC_GT", 0.97)
    monkeypatch.setattr(scanner, "LONG_ANTI_CHASE_VWAP_DIST_ATR_GT", 3.50)

    df = _make_candidate(side="LONG", close_loc=0.75, vwap_dist_atr=1.0,
                         candidate_source="V11")
    gated, rejected, stats = scanner.apply_research_live_filters(df, "2026-06-07")

    assert len(gated) == 1, "Normal V11 LONG must pass anti-chase filter"


# ---------------------------------------------------------------------------
# L_TREND_PULLBACK probation
# ---------------------------------------------------------------------------

def test_l_trend_pullback_probation_blocked(monkeypatch):
    monkeypatch.setattr(scanner, "SHORT_FOCUS_ENABLE", False)
    monkeypatch.setattr(scanner, "RESEARCH_LIVE_FILTER_ENABLE", True)
    monkeypatch.setattr(scanner, "L_TREND_PULLBACK_PROBATION_BLOCK", True)

    df = _make_candidate(side="LONG", setup="L_TREND_PULLBACK", candidate_source="V11")
    gated, rejected, stats = scanner.apply_research_live_filters(df, "2026-06-07")

    assert len(gated) == 0, "L_TREND_PULLBACK must be blocked under probation even for V11"
