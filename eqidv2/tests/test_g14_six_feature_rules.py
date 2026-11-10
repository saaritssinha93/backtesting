"""Focused contract tests for the frozen V14 six-field selector."""

from __future__ import annotations

import pandas as pd
import json

from research.g14_six_feature_rules import V14_CONFIG_PATH, evaluate_variants


def _candidate(
    sid: int,
    symbol: str,
    *,
    side: str = "LONG",
    day: str = "2026-09-10",
    slot: str = "0925",
    gap: float = 0.30,
    pdc: float = 0.45,
    move: float = 0.20,
    delta: float = 0.10,
    day_oi: float = 0.20,
) -> dict:
    return {
        "sid": sid,
        "day": day,
        "tradingsymbol": symbol,
        "slot": slot,
        "side": side,
        "signal_ts": f"{day} {slot[:2]}:{slot[2:]}:00+05:30",
        "open_gap_pct": gap,
        "pdc_pct": pdc,
        "price_5m_pct": move,
        "oi": 100000.0,
        "oi_delta_5m_pct": delta,
        "oi_day_pct": day_oi,
        "integrity_ok": True,
        "integrity_reason": "ok",
        # This field deliberately must never affect the selection.
        "future_pnl": -999999.0,
    }


def test_long_short_mirror_with_same_oi_sign() -> None:
    rows = [
        _candidate(1, "LONGSYM", side="LONG"),
        _candidate(2, "SHORTSYM", side="SHORT", gap=-0.30, pdc=-0.45, move=-0.20),
    ]
    selected = evaluate_variants(pd.DataFrame(rows))
    for variant_id, frame in selected.items():
        assert set(frame["sid"]) == {1, 2}, variant_id


def test_or_requires_a_complete_price_path() -> None:
    rows = [
        _candidate(1, "CONT", day="2026-09-10", gap=0.30, pdc=0.30, move=0.12),
        _candidate(2, "FLAT", day="2026-09-11", gap=0.00, pdc=0.30, move=0.12),
        _candidate(3, "REV", day="2026-09-14", gap=-0.30, pdc=0.10, move=0.12),
        _candidate(4, "WEAK", day="2026-09-15", gap=-0.30, pdc=0.04, move=0.12),
    ]
    selected, audit = evaluate_variants(pd.DataFrame(rows), return_audit=True)
    assert set(selected["C_three_paths_OR"]["sid"]) == {1, 2, 3}
    weak_reason = audit["decisions"].loc[
        lambda d: d["variant_id"].eq("C_three_paths_OR") & d["sid"].eq(4), "decision_reason"
    ].iat[0]
    assert weak_reason == "excluded_rule"


def test_strict_opening_oi_day_zero_exception() -> None:
    rows = [
        _candidate(1, "OPEN", slot="0920", day_oi=0.0, delta=0.10),
        _candidate(2, "LATER", slot="0925", day_oi=0.0, delta=0.10),
    ]
    selected = evaluate_variants(pd.DataFrame(rows))
    assert set(selected["D_strict_OR"]["sid"]) == {1}


def test_quota_tie_uses_symbol_and_ignores_future_pnl() -> None:
    rows = [
        _candidate(1, "ZZZ"),
        _candidate(2, "AAA"),
    ]
    rows[0]["future_pnl"] = 1000000.0
    rows[1]["future_pnl"] = -1000000.0
    selected, audit = evaluate_variants(pd.DataFrame(rows), return_audit=True)
    assert selected["C_three_paths_OR"]["tradingsymbol"].tolist() == ["AAA"]
    assert "rule_score" in selected["C_three_paths_OR"]
    reasons = audit["decisions"].loc[
        lambda d: d["variant_id"].eq("C_three_paths_OR")
    ].set_index("tradingsymbol")["decision_reason"].to_dict()
    assert reasons == {"ZZZ": "excluded_quota", "AAA": "selected"}


def test_integrity_precedes_rule_and_missing_feature_fails_closed() -> None:
    rows = [_candidate(1, "NOOI"), _candidate(2, "NAN")]
    rows[0]["integrity_ok"] = False
    rows[0]["integrity_reason"] = "contract_mismatch"
    rows[1]["pdc_pct"] = float("nan")
    _, audit = evaluate_variants(pd.DataFrame(rows), return_audit=True)
    reasons = audit["decisions"].loc[
        lambda d: d["variant_id"].eq("A_direction_only")
    ].set_index("sid")["decision_reason"].to_dict()
    assert reasons == {1: "excluded_integrity:contract_mismatch", 2: "excluded_missing_feature"}


def test_v2_setups_have_no_legacy_selection_indicators() -> None:
    config = json.loads(V14_CONFIG_PATH.read_text(encoding="utf-8"))
    allowed = {
        "setup_id", "signal_end", "confirmation_end", "side", "max_entries", "stop_pct", "target_pct"
    }
    assert len(config["active_setups"]) == 12
    assert all(set(setup) == allowed for setup in config["active_setups"])
