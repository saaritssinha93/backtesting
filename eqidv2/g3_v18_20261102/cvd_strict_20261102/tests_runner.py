"""Strict audit, attribution and empty-variant safeguards."""
import pandas as pd
import pytest

from . import run as r
from . import selection as s
from .tests_selection import row


def test_output_isolation_and_frozen_source_rejection(tmp_path):
    with pytest.raises(ValueError,match="NEW child"):
        r.fresh_output(r.PACKAGE.parent/"cvd_advanced_20261102"/"new_result")
    with pytest.raises(FileExistsError):
        r.fresh_output(r.PACKAGE/"run.py")
    with pytest.raises(ValueError,match="reuse"):
        r.verified_inputs(tmp_path/"features.parquet",tmp_path/"configuration.json")


def test_native_source_and_gate_ranking_verification_and_replacement_trace():
    result=s.select_all(pd.DataFrame([row("A",1,adv_acceleration=.20),row("B",2,traded_value=900.)]))
    original=result["base_control"][1]
    changed=result["strict_gate"][1]
    assert r.verify_audit(original,changed,"strict_gate")
    detail=r.decision_groups(original,changed,"strict_gate")[0]
    assert detail["replacements"]==1 and detail["deletions"]==0
    bad=changed.copy()
    bad.loc[0,"traded_value"]+=1
    with pytest.raises(ValueError,match="original input"):
        r.verify_audit(original,bad,"strict_gate")
    bad=changed.copy()
    bad["v9_selected"]=True
    with pytest.raises(ValueError,match="rejected"):
        r.verify_audit(original,bad,"strict_gate")


def test_empty_variant_cannot_qualify_and_reference_is_matched_control():
    records=[dict(variant=v,period="october",cost_bps=5,trades=100,net_pnl_rs=1000.,daily_close_drawdown_rs=100.) for v in s.VARIANTS]
    overall=pd.DataFrame(records)
    overall.loc[overall.variant.eq("base_control"),"net_pnl_rs"]=999999.
    overall.loc[overall.variant.eq("strict_gate"),"net_pnl_rs"]=1001.
    overall.loc[overall.variant.eq("strict_rvol_gate"),["trades","net_pnl_rs","daily_close_drawdown_rs"]]=[0,0.,0.]
    groups=pd.DataFrame([dict(variant=v,day="2026-10-23",selected_changed=True) for v in s.MATCHED_CONTROLS])
    result={x["variant"]:x for x in r.qualification(overall,groups)}
    assert result["strict_gate"]["descriptive_screen_pass"]
    assert result["strict_gate"]["reference_variant"]=="cvd_ready_control"
    assert not result["strict_rvol_gate"]["descriptive_screen_pass"]


def test_removal_analysis_separates_warmup_and_threshold_and_gross_net():
    # Arithmetic fixtures only; never published as market performance.
    base=dict(day="2026-10-23",setup_id="S",signal_ts="2026-10-23 09:50:00+05:30",filled=True,portfolio_executed=True,portfolio_cost_rupees=50.)
    prior=pd.DataFrame([dict(base,sid=1,tradingsymbol="A",portfolio_net_profit_rupees=500.,portfolio_gross_profit_rupees=550.),
                        dict(base,sid=2,tradingsymbol="B",portfolio_net_profit_rupees=-300.,portfolio_gross_profit_rupees=-250.)])
    book=pd.DataFrame([dict(base,sid=3,tradingsymbol="C",portfolio_net_profit_rupees=700.,portfolio_gross_profit_rupees=750.)])
    audit=pd.DataFrame(dict(sid=[1,2,3],strict_gate_reason=["BEFORE_0945_OR_INVALID_SIGNAL","PRESSURE_BELOW_010","PASS"]))
    detail,proof=r.attribution(prior,book,"strict_gate","base_control",audit)
    assert proof["reconciled"] and proof["gross_net_cost_reconciled"]
    assert detail.pnl_delta_rs.sum()==500.
    assert detail.gross_pnl_delta_rs.sum()==450.
    summary=r.trade_change_analysis(detail).iloc[0]
    assert summary.removed_early_warmup_trades==1
    assert summary.removed_winning_trades==1 and summary.removed_losing_trades==1
    assert summary.added_fills==1 and summary.executed_trade_fraction==.5


def test_empty_rvol_reference_and_variant_remain_explicit_zero_comparison():
    columns=["sid","day","setup_id","signal_ts","tradingsymbol","filled","portfolio_executed",
             "portfolio_net_profit_rupees","portfolio_gross_profit_rupees","portfolio_cost_rupees"]
    empty=pd.DataFrame(columns=columns)
    audit=pd.DataFrame(columns=["sid","strict_gate_reason"])
    detail,proof=r.attribution(empty,empty,"strict_rvol_gate","rvol_ready_control",audit)
    assert detail.empty and "sid" in detail
    assert proof["reconciled"] and proof["headline_pnl_delta_rs"]==0.
    analysis=r.trade_change_analysis(detail,pairs=[("strict_rvol_gate","rvol_ready_control")])
    assert len(analysis)==3
    assert analysis.trades.eq(0).all() and analysis.reference_trades.eq(0).all()
    assert analysis.status.eq("NO_EXECUTED_TRADES").all()
    assert analysis.executed_trade_fraction.isna().all()


def test_native_empty_ledger_keeps_zero_day_cost_accounting():
    from datetime import date
    from fno_v13_v10_g_3_daily_replay import simulate
    from research.g3_0920_backtest import allocate
    from g3_v18_20261102 import run as common
    empty_orders=s.select(pd.DataFrame([row()]).iloc[:0],"strict_rvol_gate")[0]
    book,_=simulate(empty_orders,{},[date(2026,10,23)])
    overall,daily,monthly=[],[],[]
    for cost in r.COSTS:
        stressed=book if cost==5 else r.checks.stressed_book(book,cost,allocate)
        metrics=common.metrics(stressed,["2026-10-23"])
        overall.append(dict(variant="strict_rvol_gate",period="all",cost_bps=cost,**metrics))
        daily.append(dict(variant="strict_rvol_gate",day="2026-10-23",cost_bps=cost,**metrics))
        monthly.append(dict(variant="strict_rvol_gate",month="2026-10",cost_bps=cost,**metrics))
    assert r.checks.verify_accounting({"strict_rvol_gate":book},pd.DataFrame(overall),pd.DataFrame(daily),pd.DataFrame(monthly),["2026-10-23"])
