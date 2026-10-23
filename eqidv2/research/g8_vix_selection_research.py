"""Bounded, chronological G8 VIX entry-selection experiments through 2026-10-22.

V2 selection research preserves original exits. Every feature is computed before
entry. Previously inspected history is reused: all evidence remains retrospective.
"""
from __future__ import annotations

import argparse
from copy import deepcopy
from datetime import datetime, timezone
from itertools import combinations
import json
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler

from research.g8_exit_engine import ENGINE, allocate_variant, metrics
from research.g8_nifty_vix_backtest import OUTPUT_ROOT, CUTOFF, sha, dump, features as original_features
from research.g8_sources import load_sources
from research.g8_vix_data import DEFAULT_5M, VixData, IST

ROOT = Path(__file__).resolve().parents[1]
VERSION = "V13-V10-G-8-VIX-SELECTION-V2"
BASELINE = "baseline_nifty"


def rule_specs():
    specs = [dict(id=BASELINE, family="baseline", mode="baseline")]
    for window in (5, 15, 30):
        for threshold in (.5, 1., 2.):
            for mode in ("direction", "persistent_direction", "adverse_alignment", "rising_alignment"):
                specs.append(dict(id=f"{mode}_{window}m_{threshold}", family=mode,
                                  mode=mode, window=window, threshold=threshold))
    for threshold in (1., 2., 4.):
        for mode in ("overnight_direction", "overnight_alignment"):
            specs.append(dict(id=f"{mode}_{threshold}", family="overnight", mode=mode, threshold=threshold))
    for level in (1.05, 1.10, 1.20):
        specs.append(dict(id=f"high_level_alignment_{level}", family="level", mode="level", level=level))
    for threshold in (.5, 1.):
        specs.append(dict(id=f"accelerating_fear_{threshold}", family="acceleration", mode="acceleration", threshold=threshold))
    for threshold in (1., 2., 3.):
        specs.append(dict(id=f"whipsaw_alignment_{threshold}", family="whipsaw", mode="whipsaw", threshold=threshold))
    for threshold in (.5, 1.):
        for close_position in (.65, .8):
            specs.append(dict(id=f"vix_candle_quality_{threshold}_{close_position}", family="confirmation",
                mode="confirmation", threshold=threshold, close_position=close_position))
    # Explicit replication references from V1, included before looking at new results.
    specs += [dict(id="previous_g8_selected", family="reference", mode="previous_selected"),
              dict(id="previous_historical_best", family="reference", mode="previous_best")]
    return specs


def build_features(source, vix):
    """Only saved confirmation inputs and completed VIX bars enter this frame."""
    original = source["unfiltered"]
    f = original_features(original, source["audit"], source["config"], vix)
    order = original.set_index("sid")
    records = []
    for row in f.itertuples(index=False):
        stamp = pd.Timestamp(row.confirmation_ts).tz_convert(IST)
        session = vix.days[row.day]
        completed = session.loc[session.available_at.le(stamp)]
        if completed.empty:
            raise ValueError("VIX unavailable before decision")
        close = float(completed.iloc[-1].close)
        day_open = float(session.iloc[0].open)
        prior_days = [day for day in vix.days if day < row.day][-20:]
        lagged_closes = [float(vix.days[day].iloc[-1].close) for day in prior_days]
        level_reference = float(np.median(lagged_closes)) if len(lagged_closes) >= 10 else np.nan
        r = order.loc[row.sid]
        sign = 1 if row.side == "LONG" else -1
        op, hi, lo, cl = [float(r["confirmation_"+field]) for field in ("open", "high", "low", "close")]
        if not np.isfinite([op, hi, lo, cl]).all() or lo <= 0 or hi < max(op, cl, lo) or lo > min(op, cl):
            raise ValueError("Invalid saved confirmation OHLC")
        span = hi-lo
        pos = ((cl-lo)/span if sign == 1 else (hi-cl)/span) if span else .5
        recent5 = vix.features(stamp-pd.Timedelta(minutes=5))
        v5 = (close/recent5["vix_level"]-1)*100 if recent5 else np.nan
        recent30 = completed.loc[completed.available_at.gt(stamp-pd.Timedelta(minutes=30))]
        range30 = (float(recent30.high.max())-float(recent30.low.min()))/close*100
        prev = float(row.vix_previous_close_pct)
        session_move = float(row.vix_session_pct)
        v15 = float(row.vix_15m_pct) if pd.notna(row.vix_15m_pct) else session_move
        v30 = float(row.vix_30m_pct) if pd.notna(row.vix_30m_pct) else session_move
        v5 = v5 if np.isfinite(v5) else session_move
        clock = stamp.hour*60+stamp.minute
        records.append(dict(sid=row.sid, vix_5m_pct=v5, vix_15_pct=v15, vix_30_pct=v30,
            vix_acceleration_pct=v5-v15/3., vix_range30_pct=range30,
            vix_level_ratio=close/level_reference if np.isfinite(level_reference) else np.nan,
            vix_level_ready=len(lagged_closes)>=10, vix_level_prior_sessions=len(lagged_closes),
            vix_level_last_reference_day=prior_days[-1] if prior_days else "",
            vix_open_gap_pct=(day_open/vix.previous_close[row.day]-1)*100,
            side_sign=sign, signed_vix_session=sign*session_move,
            signed_vix_recent=sign*v15, signed_vix_previous_close=sign*prev,
            signed_nifty_slot=sign*row.nifty_slot_pct, signed_nifty_session=sign*row.nifty_session_pct,
            confirmation_close_position=pos, confirmation_body_direction=sign*(cl/op-1)*100,
            confirmation_range_pct=span/cl*100, signal_move_abs_pct=abs(float(r.price_change_pct)),
            volume_log=float(np.log1p(min(float(r.volume_ratio),100.))),
            oi_change_pct=float(r.oi_change_pct), signal_body_ratio=float(r.body_ratio),
            time_fraction=(clock-(9*60+15))/375., feature_available_at=stamp.isoformat()))
    result = f.join(pd.DataFrame(records).set_index("sid"), rsuffix="_extra")
    if result.index.duplicated().any() or not result.index.equals(f.index):
        raise ValueError("Feature SID mismatch")
    return result


def allowed_for(f, spec):
    base = f.nifty_base_allowed.astype(bool)
    mode = spec["mode"]
    if mode == "baseline":
        return base.copy()
    if mode == "combined":
        a, b = allowed_for(f,spec["first"]), allowed_for(f,spec["second"])
        return a & b if spec["join"] == "both_filters" else a | b
    threshold = spec.get("threshold", 1.)
    recent = f[f"vix_{spec.get('window',15)}{'m' if spec.get('window',15)==5 else ''}_pct"]
    hostile = f.side_sign*recent >= threshold
    aligned = f.nifty_both_aligned.astype(bool)
    if mode == "direction":
        return base & ~hostile
    if mode == "persistent_direction":
        return base & ~(hostile & (f.side_sign*f.vix_session_pct >= threshold))
    if mode == "adverse_alignment":
        return base & (~hostile | aligned)
    if mode == "rising_alignment":
        return base & (recent.lt(threshold) | aligned)
    if mode == "overnight_direction":
        return base & (f.side_sign*f.vix_previous_close_pct < threshold)
    if mode == "overnight_alignment":
        return base & ((f.side_sign*f.vix_previous_close_pct < threshold) | aligned)
    if mode == "level":
        stress = f.vix_level_ready & f.vix_level_ratio.ge(spec["level"])
        return base & (~stress | aligned)
    if mode == "acceleration":
        stress = f.vix_acceleration_pct.ge(threshold) & f.vix_5m_pct.ge(threshold)
        return base & (~stress | aligned)
    if mode == "whipsaw":
        return base & (f.vix_range30_pct.lt(threshold) | aligned)
    if mode == "confirmation":
        strong = f.confirmation_close_position.ge(spec["close_position"]) & f.confirmation_body_direction.gt(0)
        return base & (~hostile | strong)
    if mode == "previous_selected":
        stress = f.vix_session_pct.ge(1.) & f.vix_15_pct.ge(1.)
        return base & (~stress | aligned)
    if mode == "previous_best":
        return base & ~((f.side_sign*f.vix_session_pct >= 2.) & (f.side_sign*f.vix_15_pct >= 2.))
    raise ValueError("Unknown rule: "+mode)


VIX_FEATURES = ["vix_level_ratio", "vix_level_ready", "vix_session_pct", "vix_previous_close_pct",
    "vix_5m_pct", "vix_15_pct", "vix_30_pct", "vix_acceleration_pct", "vix_range30_pct",
    "side_sign", "signed_vix_session", "signed_vix_recent", "signed_vix_previous_close"]
CONTEXT_FEATURES = ["side_sign", "signed_nifty_slot", "signed_nifty_session", "nifty_both_aligned",
    "time_fraction", "confirmation_close_position", "confirmation_body_direction", "confirmation_range_pct",
    "signal_move_abs_pct", "volume_log", "oi_change_pct", "signal_body_ratio"]


def model_specs():
    return [dict(id=f"logistic_{inputs}_C{c}_p{p}", family="model_"+inputs, mode="model", inputs=inputs, c=c, probability=p)
            for inputs in ("vix", "context", "vix_context") for c in (.1,1.) for p in (.5,.6,.7,.8)]


def model_masks(f, original, train_days):
    """Fit preprocessing/labels only on training sessions; reuse the fitted model."""
    out, parameters = {}, []
    source = original.set_index("sid").loc[f.index]
    fit = f.day.isin(train_days) & source.filled & f.nifty_base_allowed
    y = source.loc[fit,"net_return_pct"].gt(0).to_numpy()
    if len(y) < 80 or min(np.bincount(y.astype(int),minlength=2)) < 10:
        raise ValueError("Insufficient chronological data for a binary model")
    group_size = f.loc[fit].groupby("day").sid.transform("count").to_numpy()
    weight = 1./group_size
    weight /= weight.mean()
    for inputs in ("vix", "context", "vix_context"):
        columns = list(dict.fromkeys((VIX_FEATURES if inputs != "context" else []) +
                                    (CONTEXT_FEATURES if inputs != "vix" else [])))
        matrix = f[columns].astype(float)
        for c in (.1,1.):
            pipeline = make_pipeline(SimpleImputer(strategy="median"), StandardScaler(),
                LogisticRegression(C=c,max_iter=2000,random_state=20261022))
            pipeline.fit(matrix.loc[fit],y,logisticregression__sample_weight=weight)
            proba = pd.Series(pipeline.predict_proba(matrix)[:,1],index=f.index)
            for p in (.5,.6,.7,.8):
                name = f"logistic_{inputs}_C{c}_p{p}"
                out[name] = f.nifty_base_allowed & proba.ge(p)
            parameters.append(dict(inputs=inputs,c=c,columns=columns,
                train_from=min(train_days),train_through=max(train_days),training_trades=int(fit.sum()),
                training_wins=int(y.sum()),coefficient=pipeline[-1].coef_[0].tolist(),
                intercept=float(pipeline[-1].intercept_[0]),
                imputer_median=pipeline[0].statistics_.tolist(),
                scaler_mean=pipeline[1].mean_.tolist(),scaler_scale=pipeline[1].scale_.tolist()))
    return out, parameters


class FastSelection:
    """Fixed-size selection allocator, checked against the full engine per policy.

    Exits and position sizes are unchanged in this experiment. Only the entry
    allow-mask varies; capital can release at a modeled exit timestamp.
    """
    def __init__(self, original, config):
        self.original = original.reset_index(drop=True).copy()
        self.config = config
        if any(config.get(k) is not None for k in ("max_positions_per_symbol","max_gross_exposure_rs","max_open_risk_rs")):
            raise ValueError("Fast selection supports the inherited fixed-capital limits only")
        sized = ENGINE.v5.apply_fixed_capital_model(self.original,config["capital_per_entry_rs"],config["leverage"])
        raw = ENGINE.v6.prepare_source_ledger(sized)
        raw["_confirmation_sort"] = raw.confirmation_ts.fillna(raw.entry_ts)
        raw["_setup_sort"] = raw.setup_id.fillna("").astype(str)
        ordered = raw.loc[raw.filled].sort_values(
            ["entry_ts","_confirmation_sort","_setup_sort","portfolio_priority_value","tradingsymbol","sid"],
            ascending=[True,True,True,False,True,True],kind="stable")
        self.candidates = [(int(i),int(pd.Timestamp(r.entry_ts).value),int(pd.Timestamp(r.exit_ts).value))
                           for i,r in ordered.iterrows()]
        self.days = sorted(raw.day.unique())
        self.day_index = pd.Index(self.days).get_indexer(raw.day)
        self.gross = pd.to_numeric(raw.gross_return_pct).fillna(0).to_numpy()*config["capital_per_entry_rs"]*config["leverage"]/100.
        self.sid = pd.Index(raw.sid)
        self.max_positions = config.get("max_positions",10)
        self.capital_positions = int(np.floor((config["portfolio_capital_rs"]+1e-8)/config["capital_per_entry_rs"]))

    def execution(self, allowed):
        if not allowed.index.is_unique or set(allowed.index) != set(self.sid):
            raise ValueError("Missing selection decision")
        permit = allowed.reindex(self.sid).to_numpy(bool)
        accepted = np.zeros(len(permit),dtype=bool)
        active = []
        limit = min(self.max_positions,self.capital_positions)
        for i,entry,exit_ in self.candidates:
            if not permit[i]:
                continue
            active = [v for v in active if v > entry]
            if len(active) < limit:
                accepted[i] = True
                active.append(exit_)
        return accepted

    def stats(self, execution, days, cost=5.):
        cohort = np.isin(self.original.day.to_numpy(),days)
        mask = execution & cohort
        net = self.gross - cost/10000.*self.config["capital_per_entry_rs"]*self.config["leverage"]
        values = net[mask]
        daily_all = np.bincount(self.day_index[mask],weights=values,minlength=len(self.days))
        daily = daily_all[pd.Index(self.days).get_indexer(days)]
        curve = np.r_[0.,daily.cumsum()]
        return dict(trades=int(mask.sum()),wins=int((values>1e-8).sum()),losses=int((values < -1e-8).sum()),
            win_rate_pct=float(100.*(values>1e-8).mean()) if len(values) else 0.,
            net_pnl_rs=float(values.sum()),gross_pnl_rs=float(self.gross[mask].sum()),
            cost_rs=float(mask.sum()*cost/10000.*self.config["capital_per_entry_rs"]*self.config["leverage"]),
            losing_days=int((daily < -1e-8).sum()),profitable_days=int((daily > 1e-8).sum()),
            daily_close_drawdown_rs=float((np.maximum.accumulate(curve)-curve).max()),
            worst_day_rs=float(daily.min()),negative_trade_sum_abs_rs=float(-values[values<0].sum()),
            losing_day_sum_abs_rs=float(-daily[daily<0].sum()))

    def full_book(self, allowed, policy, cost=5.):
        book = allocate_variant(self.original,allowed,self.config,policy,cost_bps=cost)
        expected = self.execution(allowed)
        if not np.array_equal(book.portfolio_executed.to_numpy(bool),expected):
            raise ValueError("Fast allocation differs from full engine: "+policy)
        a,b = self.stats(expected,self.days,cost),metrics(book,self.days)
        for key,value in a.items():
            if not np.isclose(value,b[key],atol=1e-6,rtol=0):
                raise ValueError("Fast metrics differ from full engine: "+key)
        return book


def evaluate(masks, fast, periods):
    executions = {name:fast.execution(mask) for name,mask in masks.items()}
    records = []
    b = executions[BASELINE]
    for name,execution in executions.items():
        for period,days in periods.items():
            part = fast.original.day.isin(days).to_numpy()
            changes = (execution != b) & part
            records.append(dict(policy=name,period=period,changed_executions=int(changes.sum()),
                changed_days=int(fast.original.loc[changes,"day"].nunique()),**fast.stats(execution,days)))
    return pd.DataFrame(records),executions


def training_eligible(row, baseline):
    return bool(row["net_pnl_rs"] > baseline["net_pnl_rs"]+1e-6 and
        row["win_rate_pct"] >= baseline["win_rate_pct"]+.25 and
        row["trades"] >= .70*baseline["trades"] and row["changed_executions"] >= 5 and row["changed_days"] >= 3 and
        row["negative_trade_sum_abs_rs"] <= baseline["negative_trade_sum_abs_rs"] and
        row["losing_days"] <= baseline["losing_days"] and
        row["daily_close_drawdown_rs"] <= max(1.,1.10*baseline["daily_close_drawdown_rs"]))


def train_choice(table):
    rows = table.loc[table.period.eq("development")].set_index("policy",drop=False)
    # Earlier full-history winners are comparison references, never new promotion candidates.
    rows = rows.loc[~rows.index.isin(["previous_g8_selected","previous_historical_best"])]
    baseline = rows.loc[BASELINE]
    eligible = rows.loc[[training_eligible(row,baseline) for row in rows.to_dict("records")]]
    if eligible.empty:
        return BASELINE
    return str(eligible.reset_index(drop=True).sort_values(["net_pnl_rs","win_rate_pct","policy"],
        ascending=[False,False,True]).iloc[0].policy)


def validation_eligible(candidate, baseline):
    return bool(candidate["changed_executions"] >= 3 and candidate["changed_days"] >= 2 and
        candidate["net_pnl_rs"] > baseline["net_pnl_rs"]+1e-6 and
        candidate["win_rate_pct"] > baseline["win_rate_pct"] and
        candidate["trades"] >= .7*baseline["trades"] and candidate["losing_days"] <= baseline["losing_days"] and
        candidate["daily_close_drawdown_rs"] <= max(1.,1.1*baseline["daily_close_drawdown_rs"]))


def pair_specs(specs, table):
    """At most four training-selected rule families; at most twelve pairs."""
    training = table.loc[table.period.eq("development")].set_index("policy")
    base = training.loc[BASELINE]
    family_best = {}
    for spec in specs:
        if spec["family"] in ("baseline","reference"):
            continue
        row = training.loc[spec["id"]]
        if not training_eligible(row,base):
            continue
        key = (row.net_pnl_rs,row.win_rate_pct,spec["id"])
        old = family_best.get(spec["family"])
        if old is None or key > old[0]:
            family_best[spec["family"]] = (key,spec)
    best = [v[1] for v in sorted(family_best.values(),key=lambda v:v[0],reverse=True)[:4]]
    result = []
    for a,b in combinations(best,2):
        for join in ("both_filters","consensus_veto"):
            result.append(dict(id=f"{join}__{a['id']}__{b['id']}",family="combined",mode="combined",join=join,first=a,second=b))
    return result


def fit_round(f, original, fast, train_days, validation_days, test_days, include_models=True):
    periods = dict(development=train_days,validation=validation_days,final_test=test_days,all=fast.days)
    atomic = rule_specs()
    masks = {spec["id"]:allowed_for(f,spec) for spec in atomic}
    rules_table,_ = evaluate(masks,fast,periods)
    pairs = pair_specs(atomic,rules_table)
    masks.update({spec["id"]:allowed_for(f,spec) for spec in pairs})
    models = []
    if include_models:
        mm,models = model_masks(f,original,train_days)
        masks.update(mm)
    table,executions = evaluate(masks,fast,periods)
    chosen = train_choice(table)
    validation = table.loc[table.period.eq("validation")].set_index("policy")
    passes = chosen != BASELINE and validation_eligible(validation.loc[chosen],validation.loc[BASELINE])
    selected = chosen if passes else BASELINE
    return dict(masks=masks,executions=executions,table=table,chosen=chosen,selected=selected,
                validation_passed=passes,pairs=pairs,models=models)


def write_report(out, summary, comparison, daily, slots, excluded, top):
    def table(frame):
        return frame.to_html(index=False,float_format=lambda x:f"{x:,.2f}",escape=True)
    columns = ["label","period","trades","win_rate_pct","net_pnl_rs","losing_days","daily_close_drawdown_rs"]
    selected = summary["selected_policy"]
    text = f"""<!doctype html><html><head><meta charset="utf-8"><title>G8 VIX selection V2</title>
    <style>body{{font:15px system-ui;margin:32px;color:#203448}}table{{border-collapse:collapse;font-size:13px}}td,th{{padding:8px;border:1px solid #d7e1eb;text-align:right}}th{{background:#edf4fc}}.scroll{{overflow:auto}}.note{{background:#fff2ce;padding:16px}}h1,h2{{color:#173f69}}</style></head><body>
    <h1>G8: improving entry selection with VIX</h1><p>56 eligible sessions, 29 July–22 October 2026. G7 V2 entries and original exits.
    {summary['candidate_count']} candidates in the fixed chronological round: {len(rule_specs())} single rules,
    {summary['pair_count']} combinations and {len(model_specs())} regularized model/threshold choices.</p>
    <div class="note">All history in this report has been examined previously. These are retrospective internal checks,
    including the final period. They are not independent validation. No parameter search can establish definite profits.</div>
    <h2>Decision</h2><p>Training-selected: <b>{summary['development_selected']}</b>.
    Validation with actual activity passed: <b>{summary['validation_passed']}</b>. Research default: <b>{selected}</b>.
    A new rule must improve BOTH profit and win rate, retain at least 70% of trades, control drawdown/losses,
    and change at least 3 validation executions across 2 days. No final-period outcome chooses the policy.</p>
    <div class="scroll">{table(comparison[columns])}</div>
    <h2>What was tested</h2><p>VIX changes over 5, 15 and 30 minutes; persistent session-plus-recent moves;
    change versus previous close; VIX level against the median of up to 20 strictly prior session closes;
    accelerating fear; intraday whipsaw range; and confirmation candle strength during adverse VIX.
    All gates retain the approved Nifty-futures rejection when both Nifty directions oppose the trade.</p>
    <p>Rules are shared across all 51 slot/side setups. Up to four eligible rule families are chosen on training data only;
    pairs either apply both filters or reject only when both filters veto. Small logistic models compare VIX alone,
    stock/Nifty context alone, and the combined features. Their preprocessing and coefficients use training sessions only.
    No stock ID or individual slot lookup is fitted. Saved VWAP indicators with gaps/implausible values were excluded.</p>
    <p>Rule changes below 15/30 minutes of available history fall back to the completed session move and are flagged.
    The lagged level filter is inactive until at least 10 prior VIX sessions exist. No closing information from the decision day is used.</p>
    <h2>Iterations and evaluation</h2><p>Round 1 screens single rules on the first 35 sessions (through 18 September).
    Round 2 builds training-qualified pairs. Round 3 compares regularized models and gates the selected candidate on the next
    10 sessions (21 September–6 October). The last 11 sessions (7–22 October) are reported without retuning.
    Three additional expanding windows train and validate using only earlier days, then trade the next 7 sessions.
    Pair discovery, imputation, scaling and model fitting are repeated entirely within each earlier window.</p>
    <p>Full-period results for a supervised model include its training sessions. The later-period and rolling figures are
    the relevant model robustness checks. Historical maximum-profit rows below are descriptive and cannot select the default.</p>
    <h2>Highest full-history results — exploratory</h2><div class="scroll">{table(top)}</div>
    <h2>Day-wise results</h2><div class="scroll">{table(daily)}</div>
    <h2>All slot/side results</h2><div class="scroll">{table(slots)}</div>
    <h2>Baseline trades removed by the selected rule</h2><div class="scroll">{table(excluded)}</div>
    <h2>Execution and risk assumptions</h2><p>Original 1.20% SL; tighten to 1.00% after 120 minutes;
    original slot targets 0.60–2.00%; 15:15 square-off. Fixed INR 100,000 capital per entry, 5x exposure, INR 1,000,000
    portfolio, at most 10 positions. Reallocation handles capital released by exits. Model quantities are fractional,
    costs are round-trip 5bps with 10/15bps sensitivity, and drawdown is measured at daily close.
    Original stop-first ambiguous-bar and gap assumptions remain. Sealed baseline prices and source coverage limitations remain,
    including missing 1 October inputs and opening-OI gaps. Live and paper settings are unchanged.</p>
    <p><a href="summary.json">Summary and provenance</a> | <a href="parameter_trials.csv">Every trial</a> |
    <a href="daywise_results.csv">Day-wise CSV</a> | <a href="slot_side_results.csv">Slot/side CSV</a> |
    <a href="cost_sensitivity.csv">Costs</a> | <a href="rolling_choices.csv">Rolling decisions</a> |
    <a href="feature_decisions.csv">Causal features and selections</a> | <a href="selected_config.json">Selected config</a></p>
    </body></html>"""
    (out/"report.html").write_text(text,encoding="utf-8")


def run(through=CUTOFF,output=None):
    if through != CUTOFF:
        raise ValueError("Pinned data cutoff is "+CUTOFF)
    output = Path(output or OUTPUT_ROOT/"selection_research"/CUTOFF/
        (datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%f")+"_vix_selection_v2"))
    output.mkdir(parents=True,exist_ok=False)
    code = {str(path.relative_to(ROOT)):sha(path) for path in
        [Path(__file__),ROOT/"research/g8_exit_engine.py",ROOT/"research/g8_sources.py",ROOT/"research/g8_vix_data.py"]}
    dump(output/"predeclared_plan.json",dict(version=VERSION,cutoff=CUTOFF,rules=rule_specs(),models=model_specs(),
        pair_budget=12,pair_discovery="four best eligible families on training only",independent_holdout=False,
        training_guard="positive net improvement, win rate +0.25pp, 70% trades, >=5 changed executions on >=3 days, no more loss sum/losing days, drawdown <=110%",
        validation_guard="strictly positive net and win rate improvements, 70% trades, >=3 changed executions on >=2 days, no extra losing days, drawdown <=110%"))
    source = load_sources()
    provenance = json.loads((DEFAULT_5M.parent/"india_vix_provenance.json").read_text(encoding="utf-8"))
    if sha(DEFAULT_5M) != provenance["files"][DEFAULT_5M.name]:
        raise ValueError("VIX provenance changed")
    frozen = output/"india_vix_5minute.csv"
    frozen.write_bytes(DEFAULT_5M.read_bytes())
    if sha(frozen) != provenance["files"][DEFAULT_5M.name]:
        raise ValueError("VIX changed while freezing")
    f = build_features(source,VixData(frozen))
    f.to_csv(output/"features.csv",index=False)
    config = deepcopy(source["config"])
    config.update(strategy="V13-V10-G-8",variant_id=VERSION,scope="BACKTEST_ONLY",execution_authority=False)
    fast = FastSelection(source["unfiltered"],config)
    days = sorted(source["days"])
    print(f"Round 1/2/3: {len(rule_specs())} rules plus up to 12 pairs and {len(model_specs())} model thresholds",flush=True)
    fitted = fit_round(f,source["unfiltered"],fast,days[:35],days[35:45],days[45:])
    trial = fitted["table"]
    trial.to_csv(output/"parameter_trials.csv",index=False)
    dump(output/"training_discovered_pairs.json",fitted["pairs"])
    dump(output/"fitted_models.json",fitted["models"])
    selection = fitted["selected"]
    all_rows = trial.loc[trial.period.eq("all")].set_index("policy")
    # Full-history maxima are evidence summaries only, never promotion inputs.
    best_rule = str(all_rows.loc[[s["id"] for s in rule_specs()+fitted["pairs"]]].sort_values(
        ["net_pnl_rs","win_rate_pct"],ascending=False).index[0])
    b = all_rows.loc[BASELINE]
    eligible = all_rows.loc[(all_rows.net_pnl_rs > b.net_pnl_rs) & (all_rows.win_rate_pct > b.win_rate_pct)]
    best_both = str(eligible.sort_values(["net_pnl_rs","win_rate_pct"],ascending=False).index[0]) if len(eligible) else BASELINE
    labels = dict(baseline=BASELINE,previous_g8="previous_g8_selected",development_candidate=fitted["chosen"],
        selected=selection,historical_best_rule=best_rule,historical_best_both=best_both)
    comparisons,books = [],{}
    periods=dict(development=days[:35],validation=days[35:45],final_test=days[45:],all=days)
    for label,name in labels.items():
        print("Full-engine parity:",label,name,flush=True)
        book = fast.full_book(fitted["masks"][name],name)
        books[label] = book
        folder = output/label
        folder.mkdir()
        book.to_csv(folder/"portfolio_trades.csv",index=False)
        for period,calendar in periods.items():
            comparisons.append(dict(label=label,policy=name,period=period,**metrics(book,calendar)))
    # Compare original source baseline per SID, rather than accepting matching totals.
    expected = source["baseline"].set_index("sid").sort_index()
    actual = books["baseline"].set_index("sid").sort_index()
    for column in ("portfolio_executed","portfolio_net_profit_rupees"):
        if not np.allclose(actual[column].astype(float),expected[column].astype(float),atol=1e-6,rtol=0):
            raise ValueError("Original baseline differs per SID")
    rolling,choices = [],[]
    for start in (35,42,49):
        print(f"Rolling round: train through {days[start-8]}, validate through {days[start-1]}, test {days[start]}..{days[start+6]}",flush=True)
        r = fit_round(f,source["unfiltered"],fast,days[:start-7],days[start-7:start],days[start:start+7])
        name = r["selected"]
        book = fast.full_book(r["masks"][name],name)
        rolling.append(book.loc[book.day.isin(days[start:start+7])].copy())
        choices.append(dict(train_through=days[start-8],validation_from=days[start-7],validation_through=days[start-1],
            test_from=days[start],test_through=days[start+6],candidate=r["chosen"],
            validation_passed=r["validation_passed"],selected_policy=name,
            candidate_count=len(r["masks"])))
        r["table"].to_csv(output/f"rolling_trials_{start}.csv",index=False)
        dump(output/f"rolling_fit_{start}.json",dict(pairs=r["pairs"],models=r["models"]))
    rolling_book = pd.concat(rolling,ignore_index=True)
    rolling_book.to_csv(output/"rolling_trades.csv",index=False)
    pd.DataFrame(choices).to_csv(output/"rolling_choices.csv",index=False)
    for label,book in (("rolling_selected",rolling_book),("rolling_matched_baseline",books["baseline"])):
        comparisons.append(dict(label=label,policy="varies" if label=="rolling_selected" else BASELINE,
            period="rolling_last21",**metrics(book,days[35:])))
    comparison = pd.DataFrame(comparisons)
    comparison.to_csv(output/"overall_comparison.csv",index=False)
    daily,slot,cost = [],[],[]
    for label,book in books.items():
        for day in days:
            daily.append(dict(label=label,day=day,**metrics(book,[day])))
        for setup in config["active_setups"]:
            slot.append(dict(label=label,setup_id=setup["setup_id"],slot=setup["confirmation_end"],side=setup["side"],
                **metrics(book.loc[book.setup_id.eq(setup["setup_id"])],days)))
        for side in ("LONG","SHORT"):
            slot.append(dict(label=label,setup_id="ALL_"+side,slot="ALL",side=side,
                **metrics(book.loc[book.side.eq(side)],days)))
        name = labels[label]
        for bps in (5.,10.,15.):
            execution=fitted["executions"][name]
            for period,calendar in (("all",days),("final_test",days[45:])):
                cost.append(dict(label=label,cost_bps=bps,period=period,**fast.stats(execution,calendar,bps)))
    daily = pd.DataFrame(daily)
    daily.to_csv(output/"daywise_all_policies.csv",index=False)
    pivot=daily.pivot(index="day",columns="label",values="net_pnl_rs").reset_index()
    for label in labels:
        if label != "baseline":
            pivot[label+"_delta_rs"]=pivot[label]-pivot.baseline
    pivot.to_csv(output/"daywise_results.csv",index=False)
    slot=pd.DataFrame(slot)
    slot.to_csv(output/"slot_side_results.csv",index=False)
    pd.DataFrame(cost).to_csv(output/"cost_sensitivity.csv",index=False)
    decisions=f.copy()
    for label,name in labels.items():
        decisions[label+"_allowed"]=fitted["masks"][name]
    decisions.to_csv(output/"feature_decisions.csv",index=False)
    orig=books["baseline"].set_index("sid")
    chosen=books["selected"].set_index("sid")
    excluded=orig.loc[orig.portfolio_executed & ~chosen.portfolio_executed,
        ["day","setup_id","tradingsymbol","side","portfolio_net_profit_rupees"]]
    excluded.to_csv(output/"selected_avoided_trades.csv")
    historical_excluded=orig.loc[orig.portfolio_executed & ~books["historical_best_rule"].set_index("sid").portfolio_executed,
        ["day","setup_id","tradingsymbol","side","portfolio_net_profit_rupees"]]
    historical_excluded.to_csv(output/"historical_best_avoided_trades.csv")
    specs={s["id"]:s for s in rule_specs()+fitted["pairs"]+model_specs()}
    config["g8_selection"] = dict(policy=selection,spec=specs[selection],pre_entry_only=True,entries_unchanged=True,
        exits_unchanged=True,cutoff=CUTOFF,validation_passed=fitted["validation_passed"],
        independent_holdout=False,rollback=BASELINE)
    dump(output/"selected_config.json",config)
    final=comparison.loc[comparison.period.eq("final_test")].set_index("label")
    all_cmp=comparison.loc[comparison.period.eq("all")].set_index("label")
    selected_delta=float(all_cmp.loc["selected","net_pnl_rs"]-all_cmp.loc["baseline","net_pnl_rs"])
    final_delta=float(final.loc["selected","net_pnl_rs"]-final.loc["baseline","net_pnl_rs"])
    summary=dict(version=VERSION,through=CUTOFF,first_day=days[0],sessions=len(days),candidate_count=len(fitted["masks"]),
        pair_count=len(fitted["pairs"]),development_selected=fitted["chosen"],selected_policy=selection,
        validation_passed=fitted["validation_passed"],historical_best_rule=best_rule,historical_best_both=best_both,
        selected_overall_delta_rs=selected_delta,selected_final_delta_rs=final_delta,
        selected_metrics=metrics(books["selected"],days),baseline_metrics=metrics(books["baseline"],days),
        source_cache_manifest_sha256=source["manifest_sha256"],vix_sha256=sha(frozen),
        source_provenance=source["provenance"],independent_holdout=False,execution_authority=False,
        caveats=["All dates have already been inspected; repeated searches compound selection bias.",
            "Training-only pair discovery and model fitting limit direct leakage but do not make old history unseen.",
            "All supervised full-period totals include in-sample fitting sessions; inspect later and rolling periods.",
            "51 slots share rules; no outcome-based stock blacklist or slot deletion.",
            "Saved VWAP and sparse derived indicators were deliberately excluded because of data quality gaps.",
            "Only filtering frozen ranked orders was tested; no replacement-stock ranking or new entry times."])
    dump(output/"summary.json",summary)
    top=all_rows.reset_index().sort_values(["net_pnl_rs","win_rate_pct"],ascending=False).head(15)
    top[["policy","trades","win_rate_pct","net_pnl_rs","losing_days","daily_close_drawdown_rs"]].to_csv(output/"historical_top15.csv",index=False)
    write_report(output,summary,comparison,pivot,slot.loc[slot.label.eq("selected")],excluded.reset_index(),
                 top[["policy","trades","win_rate_pct","net_pnl_rs","losing_days"]])
    if any(sha(ROOT/path)!=digest for path,digest in code.items()):
        raise ValueError("Code changed during execution")
    dump(output/"manifest.json",dict(code=code,files={str(p.relative_to(output)).replace("\\","/"):sha(p)
        for p in output.rglob("*") if p.is_file()}))
    verify(output)
    dump(OUTPUT_ROOT/"latest_selection_research.json",dict(run_dir=str(output),manifest_sha256=sha(output/"manifest.json"),
        selected_policy=selection,execution_authority=False))
    print(json.dumps({k:summary[k] for k in ("candidate_count","development_selected","selected_policy","validation_passed",
        "historical_best_rule","historical_best_both","selected_overall_delta_rs","selected_final_delta_rs")},indent=2))
    print(output)
    return output


def verify(output):
    output=Path(output).resolve()
    manifest=json.loads((output/"manifest.json").read_text(encoding="utf-8"))
    for name,digest in manifest["files"].items():
        path=(output/name).resolve()
        if not path.is_relative_to(output) or sha(path)!=digest:
            raise ValueError("Changed result file: "+name)
    for name,digest in manifest["code"].items():
        path=(ROOT/name).resolve()
        if not path.is_relative_to(ROOT) or sha(path)!=digest:
            raise ValueError("Changed implementation: "+name)
    f=pd.read_csv(output/"features.csv")
    if not (pd.to_datetime(f.vix_available_at,utc=True)<=pd.to_datetime(f.confirmation_ts,utc=True)).all():
        raise ValueError("Future VIX close")
    ready=f.vix_level_ready
    if not (f.loc[ready,"vix_level_last_reference_day"] < f.loc[ready,"day"]).all():
        raise ValueError("Future level normalization")
    comparison=pd.read_csv(output/"overall_comparison.csv")
    for label in comparison.loc[comparison.period.eq("all"),"label"]:
        book=pd.read_csv(output/label/"portfolio_trades.csv")
        days=sorted(f.day.unique())
        m=metrics(book,days)
        row=comparison.loc[comparison.label.eq(label)&comparison.period.eq("all")].iloc[0]
        for k in ("net_pnl_rs","trades","wins","losses","cost_rs","gross_pnl_rs"):
            if not np.isclose(m[k],row[k],atol=1e-6,rtol=0):
                raise ValueError("Comparison reconciliation: "+label+"/"+k)
    return True


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--through",default=CUTOFF)
    parser.add_argument("--output")
    parser.add_argument("--verify")
    args=parser.parse_args()
    print(verify(args.verify)) if args.verify else run(args.through,args.output)


if __name__=="__main__":
    main()
