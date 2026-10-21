"""New G3 slots reselect complete feature pools and retain their own paths."""
from datetime import date, timedelta

import pandas as pd
import pytest

from research import g3_two_slot_backtest as new_slots
from research import g3_target_cutoff_backtest as history


DAY = date(2026, 8, 3)


def _opening(symbol="FIRST", day=DAY, **changes):
    row = dict.fromkeys(new_slots.REQUIRED_COLUMNS)
    row.update(session_date=str(day), symbol=symbol, contract=f"{symbol}26AUGFUT",
        research_feature_status="COMPLETE", cash_0920_status="VALID_COMPLETED_FIRST_BAR",
        confirmation_0921_status="VALID_REAL_MINUTE", futures_0920_status="VALID_COMPLETED_FUTURES_0920",
        opening_0916_status="VALID_FIRST_MINUTE_EVIDENCE", price_first_5m_pct=-1., oi_change_pct=.1,
        oi_change_source="FIRST_MINUTE_0916_TO_COMPLETED_0920_FUTURES_4M", oi_change_interval_minutes=4,
        oi_change_availability="RETROSPECTIVE_RESEARCH", futures_0920_oi=100.1, opening_0916_oi=100.,
        cash_close=99., ema9=99., ema20=100., ema50=101., volume_ratio=2., traded_value=10_000.,
        confirmation_open=99., confirmation_high=99.1, confirmation_low=98.4, confirmation_close=98.5,
        confirmation_body_ratio=.7, confirmation_upper_wick_ratio=.15, confirmation_lower_wick_ratio=.15,
        v9_1m_volume_ratio=1.3, nifty_context_status="VALID_COMPLETED_NIFTY_FIRST_BAR",
        nifty_first_bar_return_pct=-.1, signal_end="09:20")
    row.update(changes)
    return row


def _late(symbol="LATE", day=DAY, sid=0, **changes):
    signal = pd.Timestamp(f"{day} 10:10", tz="Asia/Kolkata")
    confirmation = signal + pd.Timedelta(minutes=1)
    row = dict(sid=sid, day=day, tradingsymbol=symbol, side="LONG", signal_end="10:10",
        signal_ts=signal, confirmation_ts=confirmation, v9_1m_feature_ts=confirmation,
        v9_5m_feature_ts=signal, v9_feature_available_ts=confirmation,
        signal_close=100., price_change_pct=.2, oi_change_pct=.1, oi=100.1, prev_oi=100.,
        ema9=100., ema20=99., ema50=98., volume_ratio=2., body_ratio=.7, wick_ratio=.2,
        traded_value=100_000., confirmation_open=100., confirmation_high=100.7,
        confirmation_low=99.9, confirmation_close=100.6, v9_1m_volume_ratio=.8,
        v9_exact_confirmation_present=True, source_1m_count=5)
    row.update(changes)
    return row


def test_opening_source_failure_stays_excluded_and_nifty_is_a_strategy_gate():
    features = pd.DataFrame([
        _opening("VALID"),
        _opening("MISSING", opening_0916_status="MISSING", research_feature_status="PARTIAL"),
        _opening("NIFTY", nifty_first_bar_return_pct=.2),
    ])
    pool = new_slots.opening_candidates(features, [DAY])
    audit = new_slots.production.selection_audit(pool).set_index("tradingsymbol")
    assert audit.loc["VALID", "v9_selected"]
    assert not audit.loc["MISSING", "v9_filter_pass"]
    assert not audit.loc["NIFTY", "v9_filter_pass"]
    assert audit.loc["MISSING", "opening_0920_status"] == "INVALID_SOURCE"
    assert audit.loc["NIFTY", "opening_0920_status"] == "VALID"
    coverage = new_slots._opening_coverage(pool, [DAY])
    assert coverage["excluded_stock_rows"] == 1
    assert coverage["per_day"][0]["source_valid_stock_rows"] == 2


def test_opening_universe_requires_all_days_and_no_duplicate_stock():
    with pytest.raises(ValueError, match="every historical session"):
        new_slots.opening_candidates(pd.DataFrame([_opening()]), [DAY, DAY + timedelta(days=1)])
    with pytest.raises(ValueError, match="Duplicate stock"):
        new_slots.opening_candidates(pd.DataFrame([_opening(), _opening()]), [DAY])


def test_reselection_uses_full_pool_ranking_and_separate_path_ids(monkeypatch, tmp_path):
    days = [DAY + timedelta(days=n) for n in range(50)]
    historical_days, daily_day = days[:49], days[-1]
    features = pd.DataFrame([_opening(day=day) for day in historical_days] + [
        _opening("RANK1", volume_ratio=5.), _opening("RANK2", volume_ratio=4.),
        _opening("MISSING", opening_0916_status="MISSING", research_feature_status="PARTIAL", volume_ratio=9.),
    ])
    late = pd.DataFrame([_late("LOW", sid=1, traded_value=100.), _late("HIGH", sid=2)])
    monkeypatch.setattr(new_slots, "load_late_source", lambda **kwargs:
        (late, {1: {"marker": "LOW"}, 2: {"marker": "HIGH"}}, historical_days, {}, {}))
    monkeypatch.setattr(new_slots.opening_inputs, "load_feature_evidence", lambda path: (features, {}, {}))
    monkeypatch.setattr(new_slots.opening_inputs, "validate_feature_universe", lambda *args: {})
    def opening_paths(orders, root):
        assert orders.native_target_pct.eq(1.5).all()
        return {int(row.sid): {"marker": row.tradingsymbol} for row in orders.itertuples()}, []
    monkeypatch.setattr(new_slots.opening_inputs, "new_execution_paths", opening_paths)
    daily_file = tmp_path / "candidates.csv"
    # Same local SID as a historical candidate must still get the daily path.
    pd.DataFrame([_late("DAILY", day=daily_day, sid=2)]).to_csv(daily_file, index=False)
    payload = dict(session_date=str(daily_day), artifacts={"candidate_signals":
        dict(path=daily_file.name, sha256=new_slots.sha256(daily_file))})
    monkeypatch.setattr(history, "_daily_paths", lambda directory, payload, orders:
        ({int(row.sid): {"marker": "DAILY"} for row in orders.itertuples()}, {}))
    original = pd.DataFrame([dict(sid=2, setup_id="0926_LONG")])
    orders, paths, proof, audit = new_slots.reselect(original, days, [(tmp_path, payload, pd.DataFrame())])
    selected_first = orders.loc[orders.day.eq(DAY)]
    assert set(selected_first.tradingsymbol) == {"RANK1", "RANK2", "HIGH"}
    assert not orders.sid.duplicated().any()
    assert not set(orders.sid) & set(original.sid)
    assert set(paths) == set(orders.sid)
    assert all(paths[int(row.sid)]["marker"] == row.tradingsymbol for row in orders.itertuples())
    assert proof["selected_by_setup"] == {"0921_SHORT": 50, "1011_LONG": 2}
    assert proof["opening_coverage"]["excluded_stock_rows"] == 1
    assert not proof["outcomes_used_for_selection"]
    assert audit.loc[audit.tradingsymbol.eq("LOW"), "v9_decision"].item() == "RANKED_OUT"


def test_package_rejects_omitted_new_slot_orders(tmp_path):
    audit = pd.DataFrame([dict(day=str(DAY), setup_id="1011_LONG", tradingsymbol="LATE",
                              sid=98_000_000, v9_selected=True)])
    audit.to_csv(tmp_path / "enabled_two_slots_selection.csv", index=False)
    proof = dict(setup_ids=sorted(new_slots.SETUP_IDS), outcomes_used_for_selection=False,
                 candidate_rows=1, selected_orders=1, selected_by_setup={"0921_SHORT": 0, "1011_LONG": 1})
    actual = audit.assign(native_target_pct=1.5)
    history._verify_two_slot_audit(tmp_path, {"enabled_two_slots": proof}, actual)
    with pytest.raises(ValueError, match="selections differ"):
        history._verify_two_slot_audit(tmp_path, {"enabled_two_slots": proof}, actual.iloc[:0])
    with pytest.raises(ValueError, match="1.50 percent"):
        history._verify_two_slot_audit(tmp_path, {"enabled_two_slots": proof}, actual.assign(native_target_pct=.75))
