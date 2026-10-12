"""Fixed-selection replay, whole-date evidence, cost stress and archive integrity."""
import json
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from v14 import baseline, coverage_study as study, selection
from v14.path_evidence import assess


class InlineWorkers:
    def __init__(self, **kwargs): pass
    def __enter__(self): return self
    def __exit__(self, *args): return False
    def map(self, function, tasks): return map(function,tasks)


def _path(row):
    start=pd.Timestamp(row.confirmation_ts)
    ts=pd.date_range(start+pd.Timedelta(minutes=1),start.normalize()+pd.Timedelta(hours=15,minutes=15),freq="min")
    out=dict(timestamp_ns=ts.as_unit("ns").asi8,open=np.full(len(ts),100.),high=np.full(len(ts),100.1),
             low=np.full(len(ts),99.9),close=np.full(len(ts),100.))
    out["high"][20]=102.
    return out


def _json(path,value):
    path.parent.mkdir(parents=True,exist_ok=True)
    path.write_text(json.dumps(value),encoding="utf-8")


def _fixture(tmp_path):
    root,source,repair,output=(tmp_path/name for name in ("root","source","repair","output"))
    source.mkdir();repair.mkdir();(root/"data/1m").mkdir(parents=True)
    days=["2026-08-03","2026-08-04","2026-08-05","2026-08-06","2026-08-07"]
    rows=[]
    for d,day in enumerate(days[:4]):
        for i in range(11):
            stamp=pd.Timestamp(day+" 09:25",tz=baseline.IST)
            rows.append(dict(sid=d*100+i,day=day,tradingsymbol=f"CASH{i:02d}",signal_ts=stamp,
                confirmation_ts=stamp+pd.Timedelta(minutes=1),side="LONG",setup_id="0926_LONG",trigger=100.,
                picker="max_liquidity",traded_value=1e8-i*1e5,volume_ratio=2.,price_change_pct=1.,
                slot_selection_policy=selection.UNLIMITED_SELECTION,native_target_pct=.97,native_stop_pct=1.25))
    control=pd.DataFrame(rows)
    focus=control.loc[control.sid.mod(100).lt(10)].copy()
    selected={"no_oi_control":control,"cumulative_rvol_2":focus}
    control.to_parquet(source/"feature_ledger.parquet",index=False)
    for name,frame in selected.items():
        frame.to_csv(source/f"selected_{name}.csv",index=False)
        old=frame.loc[frame.day.isin(days[:2])].copy()
        baseline.simulate(old,{r.sid:_path(r) for r in old.itertuples()}).to_csv(source/f"trades_{name}.csv",index=False)
    _json(source/"protocol.json",dict(protocol=dict(variants=[dict(name=n) for n in selected],slot_selection_policy=selection.UNLIMITED_SELECTION)))
    _json(source/"selected_profile.json",dict(selected_variant=dict(name="cumulative_rvol_2"),selection=dict(qualified=False),
        cost_bps=5,slot_selection_policy=selection.UNLIMITED_SELECTION))
    _json(source/"evaluation_coverage.json",dict(requested_days=days,evaluated_days=days[:2],
        missing_index_days=days[4:],insufficient_cash_coverage_days=[]))
    _json(source/"path_evidence.json",{})
    (source/"unknown_outcomes.csv").write_text("sid,day,symbol\n",encoding="utf-8")
    (source/"nifty_context.csv").write_text("day,nifty_first_bar_return_pct\n",encoding="utf-8")
    (source/"session_data_coverage.csv").write_text("day,presence_fraction\n",encoding="utf-8")
    _json(root/"data/universe_manifest.json",dict(snapshot=True))
    files=[]
    for symbol in control.tradingsymbol.unique():
        raw=root/"data/1m"/f"{symbol}.parquet"
        raw.write_bytes(b"canonical")
        files.append(dict(symbol=symbol,sha256=study.sha256(raw)))
    manifest=dict(study_sessions=days,files=files,universe_sha256=study.sha256(root/"data/universe_manifest.json"))
    _json(root/"data/manifest.json",manifest)
    (source/"input_manifest.json").write_bytes((root/"data/manifest.json").read_bytes())
    (source/"input_universe_manifest.json").write_bytes((root/"data/universe_manifest.json").read_bytes())
    _json(source/"feature_manifest.json",dict(feature_sha256=study.sha256(source/"feature_ledger.parquet"),
        data_manifest_sha256=study.sha256(root/"data/manifest.json"),nifty_context_sha256=study.sha256(source/"nifty_context.csv"),
        session_coverage_sha256=study.sha256(source/"session_data_coverage.csv")))
    _json(repair/"inventory.json",dict(canonical_root=str(root),source_run=str(source),pairs=[]))
    for name in ("fetch_summary.json","fetch_5m_summary.json"):
        _json(repair/name,dict(status="COMPLETE",errors=0,canonical_hashes_unchanged=True,recovered_missing_minutes=0))
    _json(root/"selected_profile.json",dict(immutable=True))
    return root,source,repair,output,selected


def test_runner_keeps_orders_profile_old_parity_and_reports_only_exact_dates(tmp_path,monkeypatch):
    root,source,repair,output,selected=_fixture(tmp_path)
    untouched={p:study.sha256(p) for p in source.iterdir() if p.is_file()}
    default_hash=study.sha256(root/"selected_profile.json")
    monkeypatch.setattr(study,"ProcessPoolExecutor",InlineWorkers)
    def worker(task):
        rows=task[4];proofs={}
        for row in rows.itertuples():
            path=_path(row)
            if row.sid in {210,300}:
                path={k:v[:5] for k,v in path.items()}
            proofs[row.sid]=assess(row,path)
        return dict(proofs=proofs,interval_attempts=[],repair_verification=[])
    monkeypatch.setattr(study,"_symbol_worker",worker)
    def known(orders,proofs):
        use=orders.loc[orders.sid.map(lambda sid:proofs[sid]["resolved"])].copy()
        return baseline.simulate(use,{sid:p["path"] for sid,p in proofs.items() if p["resolved"]},allow_observed_prefix=True)
    monkeypatch.setattr(study,"_known_trades",known)
    summary=study.run(root,source,repair,output)
    assert summary["common_evaluated_sessions"]==3
    assert summary["fixed_profile_variant"]=="cumulative_rvol_2"
    assert summary["old_date_parity"]=="PASS"
    assert not summary["default_profile_promoted"] and not summary["profile_retuned"]
    result=pd.read_csv(output/"trades_no_oi_control.csv")
    assert result.sid.eq(210).any() and not result.day.eq("2026-08-06").any()
    rejected=result.set_index("sid").loc[210]
    assert rejected.filled and not rejected.portfolio_executed
    assert pd.isna(rejected.exit_price) and rejected.portfolio_net_profit_rupees==0
    comparison=pd.read_csv(output/"variant_comparison.csv")
    rows=comparison.loc[comparison.variant.eq("no_oi_control") & comparison.scope.eq("common_all_dates") & comparison.split.eq("ALL")].set_index("total_bps")
    assert rows.loc[5.,"trades"]==30
    assert rows.loc[10.,"net_pnl"]==pytest.approx(rows.loc[5.,"net_pnl"]-30*250)
    assert rows.loc[25.,"net_pnl"]==pytest.approx(rows.loc[5.,"net_pnl"]-30*1000)
    assert study.sha256(root/"selected_profile.json")==default_hash
    assert all(study.sha256(path)==expected for path,expected in untouched.items())
    assert (output/"report.html").exists()
    with pytest.raises(FileExistsError,match="immutable"):
        study.run(root,source,repair,output)


def test_preregister_binds_selection_and_source_drift_and_canonical_verification(tmp_path):
    root,source,repair,output,selected=_fixture(tmp_path)
    output.mkdir()
    inputs=study.verify_inputs(root,source,repair)
    study.preregister(root,source,repair,output,selected,inputs)
    assert study.preregister(root,source,repair,output,selected,inputs)["primary_roundtrip_bps"]==5
    (source/"selected_no_oi_control.csv").write_text("changed",encoding="utf-8")
    with pytest.raises(RuntimeError,match="protocol or inputs changed"):
        study.preregister(root,source,repair,output,selected,inputs)
    (root/"data/1m/CASH00.parquet").write_bytes(b"changed")
    with pytest.raises(RuntimeError,match="Canonical cash data changed"):
        study.verify_inputs(root,source,repair)


def test_shared_selected_signal_cannot_change_its_setup_between_variants(tmp_path):
    _,source,_,_,selected=_fixture(tmp_path)
    assert len(study.load_fixed_selections(source))==2
    changed=selected["cumulative_rvol_2"].copy()
    changed.loc[changed.index[0],"setup_id"]="0931_LONG"
    changed.to_csv(source/"selected_cumulative_rvol_2.csv",index=False)
    with pytest.raises(ValueError,match="Conflicting shared selected-order field.*setup_id"):
        study.load_fixed_selections(source)


def test_worker_retains_strict_entry_when_independent_interval_proof_fails(tmp_path,monkeypatch):
    root,source,repair,_,selected=_fixture(tmp_path)
    rows=selected["no_oi_control"].iloc[[0]].copy()
    path=_path(next(rows.itertuples()))
    path={k:np.delete(v,5) for k,v in path.items()}
    minute=pd.DataFrame(dict(ts=pd.to_datetime(path["timestamp_ns"],utc=True).tz_convert(baseline.IST),
        **{k:path[k] for k in ("open","high","low","close")},volume=1))
    monkeypatch.setattr(study.data,"load_symbol_1m",lambda *a,**k:minute)
    monkeypatch.setattr(study.gap_repair,"load_verified_repair",lambda *a,**k:(minute.iloc[:0],pd.DataFrame(),
                       dict(symbol="CASH00",day="2026-08-03",price_scale_compatible=False,minute_additions=0)))
    def failed(row,path,envelopes):
        result=assess(row,path)
        result["reason"]="5M_ENVELOPE_PERMITS_UNKNOWN_TOUCH"
        return result
    monkeypatch.setattr(study.interval_evidence,"assess",failed)
    task=(str(root),str(source),str(repair),"CASH00",rows,study.sha256(root/"data/1m/CASH00.parquet"),{"2026-08-03"})
    result=study._symbol_worker(task)
    assert result["proofs"][0]["reason"]=="POSITION_OPEN_AT_FIRST_INTERRUPTION"
    assert result["proofs"][0]["evidence"]["entry"]["price"]==100.
    assert result["interval_attempts"][0]["interval_reason"]=="5M_ENVELOPE_PERMITS_UNKNOWN_TOUCH"
    (root/"data/1m/CASH00.parquet").write_bytes(b"changed")
    with pytest.raises(RuntimeError,match="changed before proof"):
        study._symbol_worker(task)
