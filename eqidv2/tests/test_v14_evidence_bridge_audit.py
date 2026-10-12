"""Independent proof-to-execution checks, including direction binding."""
import numpy as np
import pandas as pd
import pytest

from v14 import baseline, evidence_execution, interval_evidence


def scenario():
    confirmation=pd.Timestamp("2026-08-03 09:31",tz=baseline.IST)
    times=pd.date_range(confirmation+pd.Timedelta(minutes=1),confirmation.normalize()+pd.Timedelta(hours=15,minutes=15),freq="min")
    orders=pd.DataFrame([dict(sid=1,day="2026-08-03",tradingsymbol="SYNTHETIC",side="LONG",trigger=100.,
        confirmation_ts=confirmation,setup_id="0931_LONG",picker="max_liquidity",traded_value=2e7,
        volume_ratio=3.,abs_price_change_pct=.8)])
    complete=dict(timestamp_ns=times.as_unit("ns").asi8,open=np.full(len(times),100.),
        high=np.full(len(times),100.1),low=np.full(len(times),99.9),close=np.full(len(times),100.))
    complete["close"][-1]=100.5
    complete["high"][-1]=100.6
    keep=times!=pd.Timestamp("2026-08-03 09:42",tz=baseline.IST)
    sparse={key:value[keep] for key,value in complete.items()}
    five=pd.DataFrame([dict(ts=pd.Timestamp("2026-08-03 09:45",tz=baseline.IST),open=100.,high=101.75,low=99.9,
        close=100.,volume=100.,symbol="SYNTHETIC",source=interval_evidence.NATIVE_SOURCE,
        independent_interval=True,candle_minutes=5,source_hash="c"*64,price_scale_compatible=True)])
    proof=interval_evidence.assess(orders.iloc[0],sparse,five)
    assert proof["resolved"] and proof["evidence"]["outcome"]["exit_reason"]=="TIME_EXIT_1515"
    return orders,complete,sparse,five,proof


def test_evidence_bridge_replays_exact_finances_and_marks_unknown_excursions():
    orders,complete,_,_,proof=scenario()
    expected=baseline.simulate(orders,{1:complete})
    actual=evidence_execution.simulate_proven(orders,{1:proof})
    for field in ("portfolio_executed","entry_price","exit_price","entry_ts","exit_ts","exit_reason",
                  "portfolio_net_profit_rupees","portfolio_gross_profit_rupees","portfolio_cost_rupees"):
        pd.testing.assert_series_equal(actual[field],expected[field],check_names=False)
    assert pd.isna(actual.iloc[0].mfe_pct) and pd.isna(actual.iloc[0].mae_pct)
    assert actual.iloc[0].mfe_mae_exact==False


@pytest.mark.parametrize("field,value",[("side","SHORT"),("tradingsymbol","OTHER")])
def test_interval_proof_cannot_be_reused_for_another_selected_order_identity(field,value):
    orders,_,sparse,five,proof=scenario()
    changed=orders.copy()
    changed[field]=value
    if field=="side":
        # The LONG gap range permits a SHORT stop, despite matching observed final prices/time.
        reassessed=interval_evidence.assess(changed.iloc[0],sparse,five)
        assert not reassessed["resolved"]
        assert reassessed["reason"]=="5M_ENVELOPE_PERMITS_UNKNOWN_TOUCH"
    with pytest.raises(ValueError,match="proof"):
        evidence_execution.simulate_proven(changed,{1:proof})


def test_interval_proof_changed_real_path_cannot_be_executed():
    orders,_,_,_,proof=scenario()
    proof["path"]["close"][-1]+=0.01
    with pytest.raises(ValueError,match="path changed"):
        evidence_execution.simulate_proven(orders,{1:proof})


@pytest.mark.parametrize("cost",[5.,10.])
def test_bridge_preserves_native_capital_rejections_and_explicit_cost_override(cost):
    template,complete,sparse,five,_=scenario()
    records,proofs,paths=[],{},{}
    for sid in range(13):
        row=template.iloc[0].to_dict()
        row.update(sid=sid,tradingsymbol=f"SYNTHETIC{sid:02}")
        records.append(row)
        proofs[sid]=interval_evidence.assess(row,sparse,five.assign(symbol=row["tradingsymbol"]))
        assert proofs[sid]["resolved"]
        paths[sid]=complete
    orders=pd.DataFrame(records)
    expected=baseline.simulate(orders,paths,cost_bps=cost)
    actual=evidence_execution.simulate_proven(orders,proofs,cost_bps=cost)
    assert actual.portfolio_executed.sum()==10
    assert actual.portfolio_reject_reason.eq("INSUFFICIENT_PORTFOLIO_CAPITAL").sum()==3
    for field in ("portfolio_executed","portfolio_reject_reason","portfolio_net_profit_rupees",
                  "portfolio_gross_profit_rupees","portfolio_cost_rupees","portfolio_trade_exposure_rupees"):
        pd.testing.assert_series_equal(actual[field],expected[field],check_names=False)
