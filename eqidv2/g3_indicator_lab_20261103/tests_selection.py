"""Independent control parity and guard/selection behavior, no production writes."""
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from g3_indicator_lab_20261103 import selection as s


def candidate(symbol="EXAMPLE", clock="09:30", sid=1, **changes):
    signal = pd.Timestamp("2026-10-23 " + clock, tz="Asia/Kolkata")
    confirm = signal + pd.Timedelta(minutes=1)
    row = dict(sid=sid, tradingsymbol=symbol, signal_ts=signal, confirmation_ts=confirm,
        day=signal.date(), hhmm_int=int(clock.replace(":", "")),
        signal_close=100., ema9=99., ema20=100., ema50=101.,
        v9_5m_ema_bull=False, v9_5m_ema_bear=True,
        price_change_pct=-.6, oi_change_pct=.6, oi=1006., prev_oi=1000., volume_ratio=2.,
        confirmation_open=100., confirmation_high=100.1, confirmation_low=99., confirmation_close=99.2,
        body_ratio=.7, v9_1m_upper_wick_ratio=.1, v9_1m_lower_wick_ratio=.2,
        traded_value=1_000_000., v9_1m_volume_ratio=2.,
        v9_1m_feature_ts=confirm, v9_5m_feature_ts=signal, v9_feature_available_ts=confirm,
        v9_exact_confirmation_present=True, source_1m_count=5, nifty_first_bar_return_pct=-.05,
        opening_0920_status="VALID", price_change_basis="09:15_CASH_OPEN_TO_09:20_CASH_CLOSE",
        oi_change_source="FIRST_MINUTE_0916_TO_COMPLETED_0920_FUTURES_4M")
    row.update(changes)
    return row


def picked(rows, variant="control", **kwargs):
    return s.select(s.prepare(pd.DataFrame(rows)), variant, **kwargs)


def test_independent_native_control_and_no_mutation():
    raw = pd.DataFrame([candidate("A", sid=11), candidate("B", sid=22,price_change_pct=-.5)])
    original = raw.copy(deep=True)
    fingerprint = s.production.strategy_fingerprint()
    actual, audit = s.select(s.prepare(raw))
    native = s.native_control(raw)
    assert set(actual.sid) == set(native.sid) == {11}
    assert set(audit.sid) == {11,22}
    pd.testing.assert_frame_equal(raw, original)
    assert s.production.strategy_fingerprint() == fingerprint


@pytest.mark.parametrize("variant,changes", [
    ("no_ema", dict(ema9=102.,ema20=101.,ema50=100.,v9_5m_ema_bull=True,v9_5m_ema_bear=False)),
    ("no_oi_lower", dict(oi=998.,oi_change_pct=-.2)),
    ("no_oi_upper", dict(oi=1020.,oi_change_pct=2.)),
    ("no_vol5", dict(volume_ratio=.1)),
    ("no_vol1", dict(v9_1m_volume_ratio=.1)),
    ("no_body", dict(body_ratio=.1)),
    ("no_min_wick", dict(clock="09:25",v9_1m_lower_wick_ratio=.1)),
    ("no_max_wick", dict(clock="10:35",body_ratio=.3,v9_1m_lower_wick_ratio=.7,
                          v9_1m_ema20=99.5,v9_1m_atr14=.5)),
])
def test_individual_gate_removal_admits_only_its_rejected_candidate(variant,changes):
    row = candidate(**changes)
    assert picked([row])[0].empty
    orders,audit = picked([row],variant)
    assert orders.sid.tolist() == [1]
    removed = variant[3:]
    assert not audit["gate_"+removed].iloc[0]
    remaining = [g for g in s.GATES if g != removed]
    assert audit[["gate_"+g for g in remaining]].all(axis=1).iloc[0]


@pytest.mark.parametrize("changes", [
    {"oi":np.nan}, {"prev_oi":0.}, {"oi":-1.}, {"volume_ratio":np.nan},
    {"v9_1m_volume_ratio":np.nan}, {"confirmation_close":100.1},
    {"source_1m_count":4}, {"confirmation_source_flagged":True},
    {"is_synthetic":True}, {"confirmation_high":98.},
    {"v9_1m_feature_ts":pd.Timestamp("2026-10-23 09:32",tz="Asia/Kolkata")},
    {"v9_5m_feature_ts":pd.Timestamp("2026-10-23 09:35",tz="Asia/Kolkata")},
])
def test_score_or_and_gate_removal_never_bypass_integrity(changes):
    prepared=s.prepare(pd.DataFrame([candidate(**changes)]))
    orders,audit=s.select(prepared,"custom_or",removed_gates=s.GATES,extra_mask=[True],ranking_score=[100.])
    assert orders.empty
    assert audit.first_failed_gate.iloc[0]=="integrity"


def test_opening_evidence_and_nifty_restriction_remain_mandatory():
    assert picked([candidate(clock="09:20",opening_0920_status="MISSING")],
                  "all51_slots",removed_gates=s.GATES)[0].empty
    assert picked([candidate(clock="09:25",nifty_first_bar_return_pct=.2)],
                  "no_oi_lower",removed_gates=s.GATES)[0].empty


def test_core_priority_changes_only_with_no_core():
    rows=[candidate("CORE_A","09:35",10,price_change_pct=-.6,traded_value=100.),
          candidate("CORE_B","09:35",11,price_change_pct=-.55,traded_value=90.),
          candidate("RELAXED","09:35",12,price_change_pct=-.4,traded_value=1000.)]
    orders,audit=picked(rows)
    assert set(orders.tradingsymbol)=={"CORE_A","CORE_B"}
    assert orders.v10_g_f_core.all()
    assert audit.set_index("tradingsymbol").loc["RELAXED","v9_filter_pass"]
    relaxed,_=picked(rows,"no_core")
    assert set(relaxed.tradingsymbol)=={"RELAXED","CORE_A"}


def test_filter_before_ranking_replaces_rejected_leader():
    rows=[candidate("LEADER",sid=1,price_change_pct=-.8),candidate("REPLACEMENT",sid=2)]
    assert picked(rows)[0].tradingsymbol.tolist()==["LEADER"]
    orders,audit=picked(rows,"extra_gate",extra_mask=[False,True])
    assert orders.tradingsymbol.tolist()==["REPLACEMENT"]
    assert audit.set_index("tradingsymbol").loc["LEADER","first_failed_gate"]=="extra"


def test_score_ranking_same_eligibility_and_deterministic_ties():
    rows=[candidate("A",sid=1,price_change_pct=-.8),candidate("B",sid=2)]
    orders,audit=picked(rows,"weighted",ranking_score=[.2,.9])
    assert orders.tradingsymbol.tolist()==["B"]
    assert audit.v9_filter_pass.all()
    assert picked(rows,"weighted",ranking_score=[1.,1.])[0].tradingsymbol.tolist()==["A"]


def test_slot_allowlist_arm_restores_previous51_only():
    row=candidate(clock="09:30",price_change_pct=.8,ema9=102.,ema20=101.,ema50=100.,
        v9_5m_ema_bull=True,v9_5m_ema_bear=False,confirmation_close=100.8,confirmation_high=101.,
        confirmation_low=99.9)
    assert picked([row])[0].empty
    orders,audit=picked([row],"all51_slots")
    assert orders.setup_id.tolist()==["0931_LONG"]
    assert not audit.slot_enabled.iloc[0]
    # A discarded tiny-target production setup is outside this experiment.
    excluded=candidate(clock="12:35",price_change_pct=.5)
    assert "1236_LONG" not in s.prepare(pd.DataFrame([excluded])).setup_id.tolist()


def test_duplicate_and_index_misalignment_rejected():
    raw=pd.DataFrame([candidate()])
    with pytest.raises(ValueError,match="globally unique"):
        s.prepare(pd.concat([raw,raw]))
    prepared=s.prepare(raw)
    with pytest.raises(ValueError,match="index"):
        s.select(prepared,extra_mask=pd.Series([True],index=[5]))
    with pytest.raises(ValueError,match="immutable"):
        s.select(prepared,removed_gates=("integrity",))
    assert s.select(s.prepare(raw.iloc[:0]))[0].empty


@pytest.mark.parametrize("day",["2026-08-03","2026-10-23"])
def test_full_prefilter_historical_control_parity(day):
    path=Path(__file__).resolve().parents[1]/f"outputs/g3_early_entry_20261023/full_history_source/cache/{day}/features.parquet"
    if not path.is_file():
        pytest.skip("Local historical source unavailable")
    raw=pd.read_parquet(path)
    raw["sid"]=np.arange(len(raw),dtype=np.int64)
    orders,_=s.select(s.prepare(raw))
    native=s.native_control(raw)
    columns=["sid","setup_id","tradingsymbol","v10_g_f_core","stop_pct","target_pct"]
    pd.testing.assert_frame_equal(orders[columns].sort_values("sid").reset_index(drop=True),
                                 native[columns].sort_values("sid").reset_index(drop=True),
                                 check_dtype=False)
