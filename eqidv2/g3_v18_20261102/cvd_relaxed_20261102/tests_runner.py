"""Focused risk accounting, native preservation and future-outcome safeguards."""
import pandas as pd
import pytest

from . import run as r
from . import selection as s
from g3_v18_20261102.cvd_strict_20261102.tests_selection import row


def book():
    # Arithmetic-only fixture, never exported as empirical backtest performance.
    return pd.DataFrame([dict(day="2026-08-28",portfolio_executed=True,filled=True,portfolio_net_profit_rupees=100.),
        dict(day="2026-08-28",portfolio_executed=True,filled=True,portfolio_net_profit_rupees=-40.),
        dict(day="2026-08-31",portfolio_executed=True,filled=True,portfolio_net_profit_rupees=-20.),
        dict(day="2026-08-31",portfolio_executed=False,filled=True,portfolio_net_profit_rupees=-999.)])


def test_loss_day_and_trade_losses_are_distinct_and_zero_days_retained():
    result=r.metrics(book(),["2026-08-27","2026-08-28","2026-08-31"])
    assert result["net_pnl_rs"]==40.
    assert result["loss_day_pnl_abs_rs"]==20.
    assert result["loss_trade_pnl_abs_rs"]==60.
    assert result["losing_days"]==1 and result["winning_days"]==1
    assert result["zero_pnl_days"]==1 and result["zero_trade_days"]==1
    assert result["trades"]==3 and result["losses"]==2
    assert result["worst_day_rs"]==-20.


def test_empty_metrics_and_empty_attribution_remain_zero_calendar():
    empty=book().iloc[:0]
    result=r.metrics(empty,["2026-08-31"])
    assert result["zero_trade_days"]==1 and result["losing_days"]==0
    assert result["loss_day_pnl_abs_rs"]==0. and result["loss_trade_pnl_abs_rs"]==0.
    empty["sid"]=pd.Series(dtype=int)
    detail,proof=r.attribution(empty,empty,"either_soft","available_control",pd.DataFrame(columns=["sid","relaxed_gate_reason"]))
    assert detail.empty and proof["reconciled"]


def test_output_isolation_and_overwrite_refusal():
    with pytest.raises(ValueError,match="NEW child"):
        r.fresh_output(r.PACKAGE.parent/"cvd_strict_20261102"/"new_output")
    with pytest.raises(FileExistsError):
        r.fresh_output(r.PACKAGE/"run.py")


def test_native_reranking_and_source_metrics_preserved():
    raw=pd.DataFrame([row("LEADER",1,adv_recent15_pressure=.3,adv_acceleration=.2),row("NEXT",2,traded_value=900.)])
    result=s.select_all(raw)
    original,changed=result["base_control"][1],result["relaxed_and"][1]
    assert r.verify_audit(original,changed,"relaxed_and")
    assert result["relaxed_and"][0].tradingsymbol.tolist()==["NEXT"]
    trace=r.decision_groups(original,changed,"relaxed_and")[0]
    assert trace["replacements"]==1
    bad=changed.copy();bad.loc[0,"traded_value"]+=1
    with pytest.raises(ValueError,match="original input"):
        r.verify_audit(original,bad,"relaxed_and")


def test_opening_native_selection_preserved_without_cvd_evidence():
    from g3_v18_20261102.cvd_advanced_20261102.tests_selection import row as early_row
    raw=pd.DataFrame([early_row("EARLY",1,clock="09:30",adv_ready=False)])
    result=s.select_all(raw)
    for variant in s.VARIANTS[1:]:
        assert r.verify_audit(result["base_control"][1],result[variant][1],variant)
        assert len(result[variant][0])==1


def test_calibration_lock_rejects_future_outcomes_before_calling_selector():
    called=[]
    chooser=lambda metrics,changed:called.append(True)
    with pytest.raises(ValueError,match="future"):
        r.calibration_lock({"base_control":book()},["2026-08-28","2026-09-01"],[],"2026-08-31","2026-09","2026-09-01",lambda x:x,chooser)
    future=book();future.loc[0,"day"]="2026-09-01"
    with pytest.raises(ValueError,match="future"):
        r.calibration_lock({"base_control":future},["2026-08-28","2026-08-31"],[],"2026-08-31","2026-09","2026-09-01",lambda x:x,chooser)
    assert not called


def test_empty_training_lock_is_explicit_fallback_and_boundary_is_recorded():
    observed=[]
    def chooser(frame,changed):
        observed.append((frame.empty,changed.copy()))
        return dict(selected_variant="available_control",reference_variant="available_control",candidates=[])
    lock,frame=r.calibration_lock({},[],[],"2026-08-31","2026-09","2026-09-01",lambda x:x,chooser)
    assert frame.empty and observed[0][0]
    assert lock["training_sessions"]==0 and lock["future_outcomes_used_for_policy"] is False
    assert lock["future_variant_replay_computed_before_lock"] is False
    assert lock["baseline_reference_loaded_for_parity"] is True
    assert lock["training_dates"]==[] and lock["persisted_before_future_day"] is True
    assert lock["before_loading_day"]=="2026-09-01"
    assert lock["selected_variant"]=="available_control"


def test_trade_retention_has_explicit_base_and_matched_references():
    frame=pd.DataFrame([dict(variant="base_control",cost_bps=5,period="all",trades=100),
        dict(variant="available_control",cost_bps=5,period="all",trades=80),
        dict(variant="relaxed_and",cost_bps=5,period="all",trades=60)])
    out=r.add_retention(frame,["cost_bps","period"]).set_index("variant")
    assert out.loc["relaxed_and","trade_retention_vs_base"]==.6
    assert out.loc["relaxed_and","trade_retention_vs_matched"]==.75


def test_daily_loss_metric_reconciliation_rejects_conflated_amounts():
    days=["2026-08-27","2026-08-28","2026-08-31"]
    overall=pd.DataFrame([dict(variant="base_control",cost_bps=5,period="all",**r.metrics(book(),days))])
    daily=pd.DataFrame([dict(variant="base_control",cost_bps=5,day=day,**r.metrics(book(),[day])) for day in days])
    assert r.verify_daily_risk_metrics(overall,daily,{"all":days})["passed"]
    overall.loc[0,"loss_day_pnl_abs_rs"]=60.
    with pytest.raises(ValueError,match="loss metric"):
        r.verify_daily_risk_metrics(overall,daily,{"all":days})
