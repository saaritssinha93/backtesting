"""Accepted G3 contract: causal boundaries, native ranking and frozen parity."""
from __future__ import annotations

import copy
import json
import os
import subprocess
import sys
from dataclasses import asdict
from datetime import date
from pathlib import Path

import pandas as pd
import pytest

import fno_live_profile as profiles
import fno_v13_v10_g_3_contract as contract
import fno_v13_v10_g_3_live_config as config

ROOT = Path(__file__).resolve().parents[1]
RESEARCH = Path("C:/TradingData/eqidv2/fno_oi/strategy_research")
FROZEN = RESEARCH / "v13_corrected_v10_g_3/frozen_20261008_long110_nextminute_v1"


def candidate(symbol="A", *, side="LONG", signal="09:30", **changes):
    stamp = pd.Timestamp(f"2026-10-09 {signal}", tz="Asia/Kolkata")
    row = dict(tradingsymbol=symbol, side=side, signal_timestamp=stamp.isoformat(),
        confirmation_timestamp=(stamp+pd.Timedelta(minutes=1)).isoformat(),
        v9_1m_feature_ts=(stamp+pd.Timedelta(minutes=1)).isoformat(),
        signal_close=100., price_change_pct=.7 if side == "LONG" else -.7,
        oi_change_pct=.6, oi=100600., prev_oi=100000., volume_ratio=3.,
        ema9=101. if side == "LONG" else 99., ema20=100., ema50=99. if side == "LONG" else 101.,
        body_ratio=.65, wick_ratio=.2, traded_value=1e6, v9_1m_volume_ratio=1.1 if side == "LONG" else 1.2,
        confirmed=True, nifty_first_bar_return_pct=-.05)
    row.update(changes)
    return row


def test_normalized_effective_book_and_frozen_metadata_are_distinct():
    config.validate_strategy()
    assert config.strategy_fingerprint() == contract.config_fingerprint()
    assert len(config.ACTIVE_SETUPS) == 14
    assert config.setup_for("09:50", "LONG") is None
    assert config.setup_for("09:55", "SHORT") is None
    assert config.setup_for("10:00", "SHORT") is None
    assert config.setup_for("09:55", "LONG").target_pct == .9
    assert config.setup_for("09:25", "LONG").stop_pct == 1.25
    assert config.setup_for("09:25", "LONG", session_date=date(2026, 10, 9)).volume_ratio == 3.
    assert config.setup_for("09:25", "LONG", session_date=date(2026, 10, 9)).body_ratio == .6
    if not FROZEN.exists():
        pytest.skip("Accepted external evidence archive unavailable")
    frozen = contract.verify_frozen_config(FROZEN / "frozen_config.json")
    donor = frozen["setup_rules"][0]["core"]
    assert donor["stop_pct"] != config.CORE_SETUPS[0].stop_pct
    assert frozen["execution_authority"] is False


@pytest.mark.parametrize("setup", config.ACTIVE_SETUPS, ids=lambda s:s.setup_id)
def test_all_setup_boundary_thresholds_and_quotas(setup):
    side = setup.side
    row = candidate(side=side, signal=setup.signal_end,
        price_change_pct=setup.price_change_pct * (1 if side == "LONG" else -1),
        oi_change_pct=setup.oi_change_pct, volume_ratio=setup.volume_ratio,
        body_ratio=setup.body_ratio, wick_ratio=setup.max_wick_ratio)
    assert config.passes_selected_filters(row, setup)
    for field in ("oi_change_pct", "volume_ratio", "body_ratio", "v9_1m_volume_ratio"):
        assert not config.passes_selected_filters({**row,field:row[field]-1e-9},setup), field
    assert not config.passes_selected_filters({**row,"wick_ratio":setup.max_wick_ratio+1e-9},setup)
    changed_price = row["price_change_pct"] - (1e-9 if side == "LONG" else -1e-9)
    assert not config.passes_selected_filters({**row,"price_change_pct":changed_price},setup)


@pytest.mark.parametrize("ratio", [1.099999999,None,float("nan"),float("inf"),-1])
def test_long_confirmation_volume_rejects_before_ranking(ratio):
    rows = [candidate("BIG",traded_value=1e10,v9_1m_volume_ratio=ratio),candidate("VALID")]
    assert [r["tradingsymbol"] for r in config.rank_candidates(rows,config.setup_for("09:30","LONG"))] == ["VALID"]


def test_new_long_pool_reranks_while_short_threshold_stays_120():
    setup=config.setup_for("09:30","LONG")
    old=candidate("OLD",price_change_pct=.7,v9_1m_volume_ratio=1.2)
    added=candidate("NEW",price_change_pct=.8,v9_1m_volume_ratio=1.1)
    result=config.rank_candidates([old,added],setup)
    assert [r["tradingsymbol"] for r in result] == ["NEW"]
    assert result[0]["v9_1m_volume_ratio"] == 1.1
    short=candidate(side="SHORT",v9_1m_volume_ratio=1.19999999)
    assert config.rank_candidates([short],config.setup_for("09:30","SHORT")) == []


def test_short_core_precedes_better_ranked_expansion_and_ties_are_stable():
    setup=config.setup_for("09:35","SHORT")
    core=candidate("CORE",side="SHORT",signal="09:35",price_change_pct=-.5,traded_value=1e5)
    a=candidate("A",side="SHORT",signal="09:35",price_change_pct=-.4,traded_value=1e8)
    b={**a,"tradingsymbol":"B"}
    for rows in ([b,a,core],[core,a,b],[a,core,b]):
        selected=config.rank_candidates(rows,setup)
        assert [r["tradingsymbol"] for r in selected] == ["CORE","A"]
        assert [r["v10_g_f_core"] for r in selected] == [True,False]


@pytest.mark.parametrize("field,value", [
    ("ema9",100.),("oi_change_pct",1.0000001),("prev_oi",100601.),
    ("body_ratio",float("nan")),("confirmed",False),("gap_filled",True),
    ("opening_snapshot",True),("provisional_stale","true"),("confirmation_source_flagged",True),
    ("source_1m_count",4),("v9_exact_confirmation_present",False),
    ("signal_timestamp","2026-10-09T09:30:01+05:30"),
    ("confirmation_timestamp","2026-10-09T09:32:00+05:30"),
    ("v9_1m_feature_ts","2026-10-09T09:30:00+05:30"),
    ("v9_1m_feature_ts","2026-10-09T09:32:00+05:30"),
    ("v9_5m_feature_ts","2026-10-09T09:31:00+05:30"),
    ("v9_feature_available_ts","2026-10-09T09:32:00+05:30")])
def test_invalid_noncausal_or_flagged_candidates_fail_closed(field,value):
    assert config.rank_candidates([candidate(**{field:value})],config.setup_for("09:30","LONG")) == []


def test_strict_0925_never_inherits_dated_g_relaxation_and_nifty_is_short_only():
    setup=config.setup_for("09:25","LONG",session_date=date(2026,10,9))
    good=candidate(signal="09:25")
    assert config.passes_selected_filters(good,setup)
    assert not config.passes_selected_filters({**good,"ema9":98.,"volume_ratio":1.75,"body_ratio":.54},setup)
    short=candidate(side="SHORT",signal="09:25")
    assert config.passes_selected_filters(short,config.setup_for("09:25","SHORT"))
    assert not config.passes_selected_filters({**short,"nifty_first_bar_return_pct":-.049999},config.setup_for("09:25","SHORT"))
    assert config.policy.policy_for_day(date(2026,7,29))["staged_stop"] is True
    assert config.policy.policy_for_day(date(2026,10,9))["relaxed_0925_long"] is False


def test_confirmation_metrics_use_side_threshold_clock_and_flags():
    row=candidate()
    bar=dict(timestamp=row["confirmation_timestamp"],open=100.,high=101.,low=99.9,close=100.8,
             volume=110.,v9_1m_volume_ratio=1.1,v9_1m_feature_ts=row["v9_1m_feature_ts"])
    assert config.confirmation_metrics(row,bar)["confirmed"]
    assert config.confirmation_metrics(row,{**bar,"gap_filled":True})["confirmed"] is False
    assert config.confirmation_metrics(row,{**bar,"v9_1m_volume_ratio":1.099999})["confirmed"] is False
    assert config.confirmation_metrics(row,{**bar,"timestamp":"2026-10-09T09:32:00+05:30"})["confirmed"] is False


def test_causal_volume_warmup_and_separate_runtime_readiness():
    stamps=pd.date_range("2026-10-08T15:01:00+05:30",periods=15,freq="min").append(
        pd.date_range("2026-10-09T09:16:00+05:30",periods=20,freq="min"))
    history=pd.DataFrame(dict(ts=stamps,volume=100.))
    bar=dict(timestamp="2026-10-09T09:26:00+05:30",volume=110.)
    result=config.annotate_confirmation_volume(bar,history)
    assert result["v9_1m_volume_ratio"] == 1.1
    assert result["confirmation_prior_volume_count"] == 20
    history.loc[history.ts.ge(pd.Timestamp(bar["timestamp"])),"volume"]=1e20
    assert result == config.annotate_confirmation_volume(bar,history)
    assert config.annotate_confirmation_volume(bar,history.iloc[15:19])["v9_1m_volume_ratio"] is None
    assert config.LIVE_CONFIRMATION_VOLUME_REQUIRED_PRIOR == 20
    assert config.CONFIRMATION_VOLUME_MIN_PERIODS == 5


def test_allocation_identity_and_default_authority_are_isolated():
    assert config.size_position(250.,1,live=True).quantity == 200
    assert config.size_position(250.,1,live=False).quantity == 2000
    assert config.size_position(250.,25,live=True).quantity == 0
    assert not config.ARMED and not config.EXECUTION_AUTHORITY
    assert config.CONTROL_ROOT_NAME != "v6_live"
    assert config.PORTFOLIO_CAPITAL_RS == 1e6 and config.MAX_POSITIONS is None
    assert config.entry_expiry(date(2026,10,9),"09:26").strftime("%H:%M") == "09:36"
    identity=lambda version:config.canonical_signal_id(version,date(2026,10,9),"09:30","09:31","LONG","ABC")
    assert identity(config.STRATEGY_VERSION) != identity("FNO_V13_V10_G_RETAINED_20260914")


def test_dataframe_selection_and_live_rank_are_identical_and_ignore_outcomes():
    rows=[candidate("B"),candidate("A")]
    for i,row in enumerate(rows):
        row.update(sid=i,net_pnl=1e20*i,max_future_adverse_pct=1e20*i,label_outcome="FUTURE")
    selected=contract.select_orders(pd.DataFrame(rows))
    assert selected.tradingsymbol.tolist() == ["A"]
    assert selected.native_stop_pct.tolist() == [1.25]
    assert selected.native_target_pct.tolist() == [2.]
    with pytest.raises(ValueError,match="Duplicate"):
        contract.select_orders(pd.DataFrame([rows[0],rows[0]]))
    changed=copy.deepcopy(contract.strategy_payload())
    changed["minimum_confirmation_1m_volume_ratio"]["LONG"]=1.2
    with pytest.raises(ValueError,match="configuration"):
        contract.validate_contract(changed)


def test_profile_routing_preserves_g_and_legacy(monkeypatch):
    seen=[]
    monkeypatch.setattr(profiles.importlib,"import_module",lambda name:seen.append(name) or name)
    monkeypatch.setenv(profiles.PROFILE_ENV,profiles.G3_PROFILE)
    assert profiles.config_for_generation("v6") == "fno_v13_v10_g_3_live_config"
    assert profiles.config_for_generation("v5") == "fno_v5_live_config"
    monkeypatch.setenv(profiles.PROFILE_ENV,profiles.G_PROFILE)
    assert profiles.config_for_generation("v6") == "fno_v13_v10_g_live_config"
    assert profiles.is_g3_config(config) and profiles.is_modern_config(config) and not profiles.is_g_config(config)


def test_import_does_not_create_runtime_root_or_require_frozen_files(tmp_path):
    runtime=tmp_path/"absent_runtime"
    env={**os.environ,"EQIDV2_RUNTIME_ROOT":str(runtime),"PYTHONPATH":str(ROOT)}
    result=subprocess.run([sys.executable,"-c","import fno_v13_v10_g_3_live_config as c; c.validate_strategy()"],
                          cwd=ROOT,env=env,capture_output=True,text=True)
    assert result.returncode == 0,result.stderr
    assert not runtime.exists()


def test_sealed_full_candidate_selection_matches_historical_g3_oracle(tmp_path):
    source=RESEARCH/"v13_v10_g_full_history/run_20260925_cutoff_corrected_through_20260923/dataset/signals.parquet"
    if not source.exists():
        pytest.skip("External sealed full candidate universe unavailable")
    # Legacy imports create output directories: isolate them in a subprocess.
    # The oracle uses the old research shim solely as an independent comparator.
    script=r'''
import json
from pathlib import Path
import pandas as pd
import fno_v13_v10_g_backtest as g
import fno_v13_v10_g_3_contract as c
root=Path("C:/TradingData/eqidv2/fno_oi/strategy_research")
x=pd.read_parquet(root/"v13_v10_g_full_history/run_20260925_cutoff_corrected_through_20260923/dataset/signals.parquet")
settings=json.loads((root/"v13_corrected_v10_g/run_20260914_opportunity_expansion/frozen_config.json").read_text())
work=x.copy()
work.loc[work.side.eq("LONG"),"v9_1m_volume_ratio"]*=1.2/1.1
old=g.select_orders(work,g.v9.V9Config(),g.SelectionChange(**settings["selection_change"]),core_first=True)
new=c.select_orders(x)
keys=lambda z:set(zip(z.sid.astype(int),z.setup_id))
assert keys(old)==keys(new),(keys(old)-keys(new),keys(new)-keys(old))
assert len(old)==len(new)==88
observed=x.set_index("sid").v9_1m_volume_ratio
assert all(row.v9_1m_volume_ratio==observed.loc[row.sid] for row in new.itertuples())
print(json.dumps({"universe_rows":len(x),"selected":len(new),"parity":True}))
'''
    env={**os.environ,"EQIDV2_RUNTIME_ROOT":str(tmp_path/"runtime"),"PYTHONPATH":str(ROOT)}
    result=subprocess.run([sys.executable,"-c",script],cwd=ROOT,env=env,capture_output=True,text=True)
    assert result.returncode == 0,result.stdout+result.stderr
    assert json.loads(result.stdout)["universe_rows"] == 5622
