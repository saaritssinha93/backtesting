"""Portfolio irrelevance proofs retain native fills, tie order and capital clocks."""
import numpy as np
import pandas as pd
import pytest
import json
from pathlib import Path

from v14 import baseline
from v14.path_evidence import assess
from v14.portfolio_evidence import EVIDENCE_VERSION, resolve_portfolio


def _orders(count=11, day="2026-08-03"):
    return pd.DataFrame([dict(sid=i, day=day, tradingsymbol=f"CASH{i:02d}", side="LONG",
        confirmation_ts=pd.Timestamp(f"{day} 09:26", tz=baseline.IST), setup_id="0926_LONG",
        trigger=100., picker="max_liquidity", traded_value=1e8-i*1e5, volume_ratio=2.,
        price_change_pct=1., native_target_pct=.97, native_stop_pct=1.25)
        for i in range(count)])


def _path(row):
    stamp = pd.Timestamp(row.confirmation_ts)
    times = pd.date_range(stamp+pd.Timedelta(minutes=1), stamp.normalize()+pd.Timedelta(hours=15, minutes=15), freq="min")
    result = dict(timestamp_ns=times.as_unit("ns").asi8,
                  open=np.full(len(times),100.), high=np.full(len(times),100.1),
                  low=np.full(len(times),99.9), close=np.full(len(times),100.))
    result["high"][20] = 102.
    return result


def _inputs(orders, unknown=(), paths=None, prefix=5):
    full = {int(row.sid): _path(row) for row in orders.itertuples()} if paths is None else paths
    evidence = {int(row.sid): assess(row, {k:v[:prefix] for k,v in full[row.sid].items()}
                                    if row.sid in unknown else full[row.sid])
                for row in orders.itertuples()}
    known_ids = [sid for sid, proof in evidence.items() if proof["resolved"]]
    ledger = baseline.simulate(orders.loc[orders.sid.isin(known_ids)].copy(), full)
    return full, evidence, ledger


def _resolve(orders, evidence, ledger, **kwargs):
    return resolve_portfolio(orders, evidence, ledger, days=sorted(orders.day.unique()), **kwargs)


def _parity(actual, expected):
    actual, expected = actual.set_index("sid").sort_index(), expected.set_index("sid").sort_index()
    fields = [c for c in expected if c.startswith("portfolio_")]
    pd.testing.assert_frame_equal(actual[fields], expected[fields], check_dtype=False,
                                  rtol=1e-10, atol=1e-8)


def test_all_resolved_matches_every_native_portfolio_column_and_keeps_inputs():
    orders = _orders(15)
    full, evidence, known = _inputs(orders)
    original_orders, original_known = orders.copy(deep=True), known.copy(deep=True)
    result = _resolve(orders, evidence, known)
    assert result["resolved_days"] == ["2026-08-03"]
    assert result["ledger"].source_outcome_known.all()
    assert result["ledger"].portfolio_evidence_policy.eq(EVIDENCE_VERSION).all()
    _parity(result["ledger"], baseline.simulate(orders, full))
    pd.testing.assert_frame_equal(orders, original_orders)
    pd.testing.assert_frame_equal(known, original_known)


def test_unknown_exit_rejected_at_full_book_has_exact_portfolio_zero_without_fake_exit():
    orders = _orders(12)
    full, evidence, known = _inputs(orders, unknown={10,11})
    assert all(not evidence[sid]["resolved"] for sid in (10,11))
    result = _resolve(orders, evidence, known)
    assert result["resolved_days"] == ["2026-08-03"]
    assert result["rejected_unknown"].sid.tolist() == [10,11]
    assert result["rejected_unknown"].reserved_capital_rupees.eq(1e6).all()
    _parity(result["ledger"], baseline.simulate(orders, full))
    rejected = result["ledger"].set_index("sid").loc[[10,11]]
    assert rejected.filled.all() and not rejected.portfolio_executed.any()
    assert rejected.exit_ts.isna().all() and rejected.exit_price.isna().all()
    assert rejected.net_profit_rupees.isna().all()
    assert rejected.portfolio_net_profit_rupees.eq(0).all()
    # Later hypothetical outcomes of rejected orders never change portfolio P&L.
    for sid in (10,11):
        full[sid]["high"][:] = 100.1
        full[sid]["low"][40] = 98.
    _parity(result["ledger"], baseline.simulate(orders, full))


def test_admitted_unknown_blocks_whole_day_and_never_reserves_a_guessed_duration():
    first, second = _orders(11), _orders(1,"2026-08-04").assign(sid=99)
    orders = pd.concat([first,second],ignore_index=True)
    _, evidence, known = _inputs(orders, unknown={0})
    result = _resolve(orders,evidence,known)
    assert result["resolved_days"] == ["2026-08-04"]
    assert result["ledger"].sid.tolist() == [99]
    assert result["rejected_unknown"].empty
    assert result["blocked_days"].reason.tolist() == ["ADMISSIBLE_ORDER_HAS_UNKNOWN_EXIT"]
    assert result["blocked_days"].sid.tolist() == [0]


@pytest.mark.parametrize("field,value",[("traded_value",2e8),("tradingsymbol","AAA")])
def test_native_priority_and_symbol_tie_can_make_unknown_admissible(field,value):
    orders = _orders(11)
    if field=="tradingsymbol":
        orders["traded_value"] = 1e8
    orders.loc[10,field] = value
    _,evidence,known = _inputs(orders,unknown={10})
    result = _resolve(orders,evidence,known)
    assert result["resolved_days"] == []
    assert result["blocked_days"].sid.tolist() == [10]
    assert result["ledger"].empty


def test_setup_key_precedes_picker_priority_exactly_as_native():
    orders = _orders(11)
    orders.loc[10,"setup_id"] = "0926_SHORT"
    orders.loc[10,"side"] = "SHORT"
    orders.loc[10,"traded_value"] = 1e12
    full,evidence,known = _inputs(orders,unknown={10})
    result = _resolve(orders,evidence,known)
    assert result["rejected_unknown"].sid.tolist()==[10]
    _parity(result["ledger"],baseline.simulate(orders,full))


@pytest.mark.parametrize("unknown_entry_offset,expected_days",[(4,["2026-08-03"]),(5,[])])
def test_gap_open_exit_releases_at_exact_clock_before_equal_timestamp_candidate(unknown_entry_offset,expected_days):
    orders = _orders(11)
    paths = {row.sid:_path(row) for row in orders.itertuples()}
    # A stop at the 09:33 candle OPEN releases capital at09:32.
    paths[0]["open"][6]=98.
    paths[0]["low"][6]=97.9
    for field,value in (("open",99.5),("high",99.6),("low",99.4),("close",99.5)):
        paths[10][field][:unknown_entry_offset]=value
    _,evidence,known = _inputs(orders,unknown={10},paths=paths,prefix=8)
    assert pd.Timestamp(evidence[0]["evidence"]["outcome"]["resolution_ts"]).strftime("%H:%M")=="09:32"
    result = _resolve(orders,evidence,known)
    assert result["resolved_days"]==expected_days
    if expected_days:
        _parity(result["ledger"],baseline.simulate(orders,paths))
    else:
        assert result["blocked_days"].reason.iloc[0]=="ADMISSIBLE_ORDER_HAS_UNKNOWN_EXIT"


def test_unknown_entry_does_not_get_assumed_unfilled_even_with_ten_positions():
    orders = _orders(11)
    full = {row.sid:_path(row) for row in orders.itertuples()}
    for field,value in (("open",99.5),("high",99.6),("low",99.4),("close",99.5)):
        full[10][field][:6]=value
    _,evidence,known=_inputs(orders,unknown={10},paths=full)
    assert evidence[10]["reason"]=="ENTRY_WINDOW_INCOMPLETE"
    result=_resolve(orders,evidence,known)
    assert not result["resolved_days"]
    assert result["blocked_days"].reason.iloc[0]=="UNKNOWN_ENTRY_OR_UNTRUSTWORTHY_PROOF"


def test_ambiguous_prefix_entry_is_not_treated_as_proven():
    orders = _orders(11)
    _,evidence,known=_inputs(orders,unknown={10})
    evidence[10]["reason"]="AMBIGUOUS_REQUIRED_TIMESTAMP"
    result=_resolve(orders,evidence,known)
    assert not result["resolved_days"]


def test_flat_persisted_evidence_supported_and_rejected_proof_is_bound_to_source():
    orders = _orders(11)
    _,evidence,known=_inputs(orders,unknown={10})
    flat={str(sid):dict(sid=sid,day="2026-08-03",symbol=f"CASH{sid:02d}",
        resolved=p["resolved"],reason=p["reason"],**p["evidence"]) for sid,p in evidence.items()}
    result=_resolve(orders,flat,known)
    assert result["rejected_unknown"].source_prefix_sha256.iloc[0]==flat["10"]["prefix_sha256"]
    flat["10"]["symbol"]="WRONG"
    with pytest.raises(ValueError,match="symbol differs"):
        _resolve(orders,flat,known)


def test_zero_trade_valid_day_unavailable_day_and_empty_input_keep_calendar():
    orders=_orders(1)
    full={0:_path(orders.iloc[0])}
    full[0]["high"][:10]=99.9
    full[0]["open"][:10]=99.8
    full[0]["low"][:10]=99.7
    full[0]["close"][:10]=99.8
    _,evidence,known=_inputs(orders,paths=full)
    result=resolve_portfolio(orders,evidence,known,days=["2026-08-03","2026-08-04","2026-08-05"],
                             unavailable_days=["2026-08-05"])
    assert result["resolved_days"]==["2026-08-03","2026-08-04"]
    assert not result["ledger"].filled.any()
    empty=resolve_portfolio(orders.iloc[:0],{},pd.DataFrame(),days=["2026-08-03"])
    assert empty["resolved_days"]==["2026-08-03"]
    assert empty["ledger"].empty


@pytest.mark.parametrize("mutation,pattern",[
    (lambda e,k:e[10]["evidence"]["entry"].update(ts="2026-08-03 09:40+05:30"),"outside native expiry"),
    (lambda e,k:e[10]["evidence"].update(continuous_valid_minutes=0),"contiguous proof"),
])
def test_invalid_unknown_entry_cannot_be_rejected_by_assumption(mutation,pattern):
    orders=_orders(11)
    _,evidence,known=_inputs(orders,unknown={10})
    mutation(evidence,known)
    with pytest.raises(ValueError,match=pattern):
        _resolve(orders,evidence,known)


def test_cost_override_and_known_ledger_drift_rejected():
    orders=_orders(11)
    full,evidence,_=_inputs(orders,unknown={10})
    known=baseline.simulate(orders.iloc[:10],full,cost_bps=10)
    with pytest.raises(ValueError,match="capital, stop, or cost"):
        _resolve(orders,evidence,known)
    result=_resolve(orders,evidence,known,cost_bps=10)
    _parity(result["ledger"],baseline.simulate(orders,full,cost_bps=10))
    changed=known.copy()
    changed.loc[0,"exit_price"]=50.
    with pytest.raises(ValueError,match="execution differs"):
        _resolve(orders,evidence,changed,cost_bps=10)
    changed=known.copy()
    changed.loc[0,["net_profit_rupees","pre_cost_profit_rupees"]]+=1000.
    with pytest.raises(ValueError,match="P&L differs from proven"):
        _resolve(orders,evidence,changed,cost_bps=10)


def test_missing_resolved_ledger_and_duplicate_orders_fail_closed():
    orders=_orders(11)
    _,evidence,known=_inputs(orders,unknown={10})
    with pytest.raises(ValueError,match="missing from native known ledger"):
        _resolve(orders,evidence,known.iloc[1:])
    with pytest.raises(ValueError,match="unique integers"):
        _resolve(pd.concat([orders,orders.iloc[:1]],ignore_index=True),evidence,known)


ARCHIVE=Path("C:/TradingData/eqidv2/v14/runs/20261010_entry_quality_uncapped_v1")


@pytest.mark.skipif(not (ARCHIVE/"summary.json").exists(),reason="Local completed uncapped study unavailable")
def test_all_six_actual_study_ledgers_retain_every_native_portfolio_field():
    evidence=json.loads((ARCHIVE/"path_evidence.json").read_text(encoding="utf-8"))
    coverage=json.loads((ARCHIVE/"evaluation_coverage.json").read_text(encoding="utf-8"))
    checked=0
    for name,days in coverage["own_days"].items():
        orders=pd.read_csv(ARCHIVE/f"selected_{name}.csv")
        orders=orders.loc[orders.day.isin(days)].copy()
        known=pd.read_csv(ARCHIVE/f"trades_{name}.csv")
        result=resolve_portfolio(orders,evidence,known,days=days)
        assert result["resolved_days"]==days
        # CSV interprets native empty rejection strings as NA on readback.
        known["portfolio_reject_reason"]=known.portfolio_reject_reason.fillna("")
        _parity(result["ledger"],known)
        checked+=len(known)
    assert checked>2000
