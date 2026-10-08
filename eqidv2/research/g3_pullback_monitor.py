"""Causal, observational post-entry pullback study on the frozen G-3 book.

Rules and endpoints are in g3_pullback_protocol_v1.json. No parameter fitting
or changes to the frozen trades are performed. This is historical research.
"""
from __future__ import annotations

import argparse
import hashlib
import html
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from research.g3_freeze import DEFAULT_FROZEN, load_frozen
from research.g3_pullback_metrics import summarize, summarize_phases

IST = "Asia/Kolkata"
PROTOCOL = Path(__file__).with_name("g3_pullback_protocol_v1.json")
SPECS = {5: (.30, (.20, .40)), 30: (.50, (.40, .75))}


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def features(minute: pd.DataFrame) -> pd.DataFrame:
    """Every value in a row depends only on that row and earlier candles."""
    m = minute.copy().sort_values("ts").reset_index(drop=True)
    if m.ts.duplicated().any():
        raise ValueError("Duplicate context minute")
    previous_close = m.close.shift(1)
    tr = pd.concat([m.high-m.low, (m.high-previous_close).abs(),
                    (m.low-previous_close).abs()], axis=1).max(axis=1)
    m["atr14"] = tr.rolling(14, min_periods=14).mean()
    m["sma5"] = m.close.rolling(5, min_periods=5).mean()
    m["sma13"] = m.close.rolling(13, min_periods=13).mean()
    m["prior3_low"] = m.low.shift(1).rolling(3, min_periods=3).min()
    m["prior3_high"] = m.high.shift(1).rolling(3, min_periods=3).max()
    denom = m.volume.shift(1).rolling(20, min_periods=20).mean()
    m["volume_ratio20"] = m.volume / denom.where(denom.gt(0))
    m["momentum3"] = m.close-m.close.shift(3)
    m["momentum5"] = m.close-m.close.shift(5)
    return m


def warning_features(row, sign: int, favorable_close: float) -> dict:
    candle_range = float(row.high-row.low)
    atr = float(row.atr14)
    required = [row.close, row.open, row.sma5, row.sma13, row.prior3_low,
                row.prior3_high, row.momentum3, row.momentum5, atr]
    ready = bool(np.isfinite(required).all() and atr > 0 and candle_range >= 0)
    if not ready:
        return dict(feature_ready=False, fast_warning=False, slow_warning=False,
                    baseline_warning=False)
    adverse_body = -sign*float(row.close-row.open)/candle_range if candle_range > 0 else 0.
    momentum3 = sign*float(row.momentum3)/atr
    momentum5 = sign*float(row.momentum5)/atr
    micro_break = bool(row.close < row.prior3_low if sign == 1 else row.close > row.prior3_high)
    trend_damage = bool(sign*(row.close-row.sma5) < 0 and sign*(row.sma5-row.sma13) < 0)
    giveback = sign*(favorable_close-float(row.close))/atr
    volume = float(row.volume_ratio20)
    fast = momentum3 <= -.5 and (micro_break or (adverse_body >= .5 and np.isfinite(volume) and volume >= 1.2))
    slow = trend_damage and momentum5 <= -.5 and giveback >= 1.
    return dict(feature_ready=True, fast_warning=bool(fast), slow_warning=bool(slow),
                baseline_warning=bool(sign*(row.close-row.open) < 0),
                adverse_body_fraction=adverse_body, momentum3_atr=momentum3,
                momentum5_atr=momentum5, micro_break=micro_break,
                trend_damage=trend_damage, giveback_atr=giveback,
                volume_ratio20=volume, atr14=atr, sma5=float(row.sma5), sma13=float(row.sma13))


def label_outcome(minute: pd.DataFrame, decision_ts: pd.Timestamp,
                  decision_close: float, sign: int, horizon: int, threshold: float,
                  exit_ts: pd.Timestamp, exit_bar_end: pd.Timestamp,
                  exit_price: float, exit_event: str = "INTRABAR") -> dict:
    """Use known temporal order; unresolved intrabar extremes stay unknown."""
    horizon_end = decision_ts + pd.Timedelta(minutes=horizon)
    end = min(horizon_end, exit_ts)
    bar_allowed = minute.ts.le(exit_bar_end) if exit_event == "CLOSE" else minute.ts.lt(exit_bar_end)
    future = minute.loc[minute.ts.gt(decision_ts) & minute.ts.le(end)
                        & bar_allowed]
    extreme = future.low if sign == 1 else future.high
    adverse = -sign*(extreme/decision_close-1)*100
    hit = future.loc[adverse.ge(threshold-1e-10)]
    event_ts = hit.ts.iloc[0] if len(hit) else pd.NaT
    event_source = "COMPLETED_BAR" if len(hit) else ""
    terminal_adverse = -sign*(exit_price/decision_close-1)*100
    exit_in_window = decision_ts < exit_ts <= horizon_end
    threshold_hit = lambda value: value >= threshold-1e-10
    exit_bar = minute.loc[minute.ts.eq(exit_bar_end)]
    opening_adverse = np.nan
    if exit_in_window and exit_event == "INTRABAR":
        if len(exit_bar) != 1:
            raise ValueError("Missing/duplicate intrabar exit candle")
        opening_ts = exit_bar_end-pd.Timedelta(minutes=1)
        opening_adverse = -sign*(float(exit_bar.open.iloc[0])/decision_close-1)*100
        if opening_ts >= decision_ts and threshold_hit(opening_adverse) and (pd.isna(event_ts) or opening_ts < event_ts):
            event_ts, event_source = opening_ts, "EXIT_CANDLE_OPEN"
    if exit_in_window and threshold_hit(terminal_adverse) and (pd.isna(event_ts) or exit_ts < event_ts):
        event_ts, event_source = exit_ts, "ACTUAL_EXIT_FILL"
    event = bool(pd.notna(event_ts))
    unknown = False
    if not event and exit_in_window and exit_event == "INTRABAR":
        adverse_extreme = float(exit_bar.low.iloc[0] if sign == 1 else exit_bar.high.iloc[0])
        unknown = bool(threshold_hit(-sign*(adverse_extreme/decision_close-1)*100))
    censored = bool(exit_ts < horizon_end and not event)
    recovered = False
    if event:
        after = future.loc[future.ts.gt(event_ts)]
        recovered = bool((sign*(after.close/decision_close-1)).ge(0).any())
        if event_ts < exit_ts <= horizon_end and sign*(exit_price/decision_close-1) >= 0:
            recovered = True
    all_adverse = [float(adverse.max())] if len(adverse) else []
    if exit_in_window:
        all_adverse.append(float(terminal_adverse))
    if np.isfinite(opening_adverse):
        all_adverse.append(float(opening_adverse))
    return dict(event=event, event_ts=event_ts, event_source=event_source,
        lead_minutes=float((event_ts-decision_ts).total_seconds()/60) if event else np.nan,
        observed_exposure_minutes=float((end-decision_ts).total_seconds()/60),
        max_future_adverse_pct=max([0., *all_adverse]),
        early_exit=bool(exit_ts < horizon_end), censored=censored,
        outcome_available=not unknown, unknown_reason="UNKNOWN_INTRABAR_ORDER" if unknown else "",
        full_horizon_eligible=not censored and not unknown, recovered_by_later_close=recovered)


def make_predictions(trades: pd.DataFrame, minutes: pd.DataFrame, days: list[str]):
    dense, grids, trade_rows = [], [], []
    context = {}
    for (day, symbol), group in minutes.groupby(["day", "tradingsymbol"], sort=False):
        context[(str(day), str(symbol))] = features(group)
    earlier = set(days[:30])
    for trade_no, tr in enumerate(trades.itertuples(index=False)):
        day, symbol = str(tr.day), str(tr.tradingsymbol)
        trade_id = f"{day}|{tr.setup_id}|{symbol}|{tr.sid}"
        m = context[(day, symbol)]
        entry_ts = pd.Timestamp(tr.entry_ts)
        exit_ts = pd.Timestamp(tr.exit_ts)
        exit_bar_end = pd.Timestamp(tr.exit_bar_end_ts)
        side = str(tr.side)
        sign = 1 if side == "LONG" else -1
        observed = m.loc[m.ts.gt(entry_ts) & m.ts.lt(exit_ts)]
        favorable = float(tr.entry_price)
        first = None
        decision_count = 0
        for row in observed.itertuples(index=False):
            favorable = max(favorable, float(row.close)) if sign == 1 else min(favorable, float(row.close))
            feat = warning_features(row, sign, favorable)
            if not feat["feature_ready"]:
                continue
            stamp = pd.Timestamp(row.ts)
            if first is None:
                first = stamp
            age = float((stamp-entry_ts).total_seconds()/60)
            phase = "ENTRY_1_5" if age <= 5 else "EARLY_6_30" if age <= 30 else "LATE_31_PLUS"
            elapsed = int((stamp-first).total_seconds()/60)
            base = dict(trade_id=trade_id, day=day, tradingsymbol=symbol,
                setup_id=tr.setup_id, side=side, sid=int(tr.sid), decision_ts=stamp,
                decision_close=float(row.close), trade_age_minutes=age, phase=phase,
                split="EARLIER30" if day in earlier else "LATER16", **feat)
            for horizon, (threshold, sensitivities) in SPECS.items():
                flag = feat["fast_warning"] if horizon == 5 else feat["slow_warning"]
                info = dict(base, horizon_minutes=horizon, threshold_pct=threshold, warning=flag)
                outcome = label_outcome(m, stamp, float(row.close), sign, horizon, threshold,
                                       exit_ts, exit_bar_end, float(tr.exit_price), str(tr.exit_event))
                dense.append(dict(info, **outcome))
                if elapsed % horizon == 0:
                    grids.append(dict(info, **outcome))
                    for alternative in sensitivities:
                        other = label_outcome(m, stamp, float(row.close), sign, horizon, alternative,
                                              exit_ts, exit_bar_end, float(tr.exit_price), str(tr.exit_event))
                        grids.append(dict(info, threshold_pct=alternative, **other))
            decision_count += 1
        trade_rows.append(dict(trade_id=trade_id, day=day, tradingsymbol=symbol,
            side=side, setup_id=tr.setup_id, entry_ts=entry_ts, exit_ts=exit_ts,
            entry_price=float(tr.entry_price), exit_price=float(tr.exit_price),
            exit_reason=tr.exit_reason, net_pnl=float(tr.portfolio_net_profit_rupees),
            first_monitor_ts=first, monitoring_minutes=decision_count,
            monitor_available=bool(decision_count),
            unavailable_reason="" if decision_count else "EXIT_BEFORE_FIRST_COMPLETE_POST_ENTRY_OBSERVATION"))
    return pd.DataFrame(dense), pd.DataFrame(grids), pd.DataFrame(trade_rows)


def alert_episodes(dense: pd.DataFrame) -> pd.DataFrame:
    parts = []
    for (_, horizon), group in dense.groupby(["trade_id", "horizon_minutes"], sort=False):
        next_allowed = None
        for row in group.sort_values("decision_ts").itertuples(index=False):
            if row.warning and (next_allowed is None or row.decision_ts >= next_allowed):
                parts.append(row._asdict())
                next_allowed = row.decision_ts + pd.Timedelta(minutes=int(horizon))
    return pd.DataFrame(parts, columns=dense.columns)


def alert_summary(alerts: pd.DataFrame, first_only: bool = False) -> pd.DataFrame:
    work = alerts.sort_values("decision_ts")
    if first_only:
        work = work.drop_duplicates(["trade_id", "horizon_minutes"])
    records = []
    for split in ("ALL", "EARLIER30", "LATER16"):
        for side in ("BOTH", "LONG", "SHORT"):
            for horizon in SPECS:
                x = work.loc[work.horizon_minutes.eq(horizon)]
                if split != "ALL":
                    x = x.loc[x.split.eq(split)]
                if side != "BOTH":
                    x = x.loc[x.side.eq(side)]
                total = len(x)
                resolved = x.loc[x.outcome_available]
                n, hits = len(resolved), int(resolved.event.sum())
                unknown = total-n
                records.append(dict(split=split, side=side, horizon_minutes=horizon,
                    alerts=total, resolved_alerts=n, unknown_alerts=unknown,
                    successful_alerts=hits, false_alerts=n-hits,
                    precision=hits/n if n else np.nan, warned_trades=x.trade_id.nunique(),
                    precision_identification_lower=hits/total if total else np.nan,
                    precision_identification_upper=(hits+unknown)/total if total else np.nan,
                    warned_days=x.day.nunique(), censored=int(x.censored.sum()),
                    median_lead_minutes=x.loc[x.event, "lead_minutes"].median(),
                    recovered_events=int((x.event & x.recovered_by_later_close).sum())))
    return pd.DataFrame(records)


def _pct(value):
    return "—" if pd.isna(value) else f"{100*float(value):.1f}%"


def write_report(out: Path, frozen: dict, metric: pd.DataFrame, alert_stats: pd.DataFrame,
                 trade_detail: pd.DataFrame, dense: pd.DataFrame, grid: pd.DataFrame):
    primary = metric.loc[metric.analysis.eq("PRIMARY") & metric.phase.eq("ALL") &
        ((metric.horizon_minutes.eq(5) & metric.threshold_pct.eq(.3)) |
         (metric.horizon_minutes.eq(30) & metric.threshold_pct.eq(.5)))].copy()
    display = []
    for r in primary.loc[primary.split.isin(["ALL", "LATER16"]) & primary.side.ne("BOTH")].itertuples():
        display.append({"Period":r.split,"Side":r.side,"Forecast":f"{r.horizon_minutes}m / {r.threshold_pct:.2f}%",
            "Warnings":int(r.tp+r.fp),"Correct":int(r.tp),"Success (precision)":_pct(r.precision),
            "Day-cluster 95% CI":f"{_pct(r.precision_bootstrap_low)}–{_pct(r.precision_bootstrap_high)}",
            "Recall":_pct(r.recall),"False-positive rate":_pct(r.fpr),
            "Event prevalence":_pct(r.prevalence),"Candle baseline precision":_pct(r.baseline_precision)})
    tables = pd.DataFrame(display)
    lines = ["# Frozen G-3: post-entry adverse pullback monitor", "",
        f"The frozen entry strategy is exact-next-minute LONG confirmation at 1.10× volume; SHORT remains 1.20×. The historical book contains {len(trade_detail)} executed trades on {len(frozen['days'])} available sessions through {max(frozen['days'])}. Net P&L is ₹{trade_detail.net_pnl.sum():,.2f}. Monitoring does not change these trades.", "",
        "A warning predicts a NEW adverse movement from its completed-candle closing price: 0.30% within five minutes (FAST), or 0.50% within thirty minutes (SLOW). LONG adverse means falling prices; SHORT adverse means rising prices. Both forecasts are evaluated throughout each open trade. At the frozen ₹500,000 exposure these moves correspond to ₹1,500 and ₹2,500 gross price movement, not extra booked losses or an exit recommendation.", "",
        "Success rate means correct warnings divided by warnings (precision). Recall measures the fraction of observed events warned about. The false-positive rate uses non-event windows as its denominator. These are distinct from trading win rate.", "",
        "The main table uses fixed, non-overlapping windows starting at the first completed minute strictly after recorded entry, repeated every five or thirty minutes. Dense every-minute alerts with a matching cooldown are reported separately. No warning can anticipate a movement before the first observation; fast exits can leave no monitoring opportunity.", "",
        "| Period | Side | Forecast | Warnings | Correct | Success | Day-cluster 95% CI | Recall | FPR | Event rate | Candle benchmark |",
        "|---|---|---|---:|---:|---:|---|---:|---:|---:|---:|"]
    for x in display:
        lines.append("| " + " | ".join(str(v) for v in x.values()) + " |")
    lines += ["", "## Rules fixed before examining monitor outcomes", "",
        "FAST: direction-normalized three-minute momentum is at most −0.5 ATR; additionally, the close breaks the prior three-candle adverse extreme, or an adverse candle body occupies at least half its range with volume at least 1.20× its previous twenty-minute mean.", "",
        "SLOW: the close and SMA5/SMA13 ordering oppose the trade, five-minute directional momentum is at most −0.5 ATR, and price has retreated at least one ATR from the best completed close observed since monitoring began (including entry price). ATR is the simple mean of fourteen completed true ranges. Both rules use the same thresholds for LONG and SHORT.", "",
        "## Validation and interpretation", "",
        "The first thirty sessions and last sixteen sessions are reported separately. No monitor threshold was optimized on either segment. Because this strategy history was already used for G-3 research, the later segment is chronological validation, not a fresh prospective test.", "",
        "Primary outcomes end at the earlier of the forecast horizon or the frozen trade exit. An exit with a resolved absence of an event is a non-event for this exposure question. The FULL_HORIZON tables instead censor early exits without an event. The executed exit price and known exit-candle open can establish an event; a closing time exit allows the whole candle. If only an intrabar exit candle's adverse extreme crosses the threshold, the sequence is unknown: that row stays in coverage and is excluded from resolved success/failure counts. Identification bounds show precision if all unresolved warnings fail or succeed. Entry-bar extrema never become future forecast outcomes.", "",
        "The 95% bootstrap intervals resample whole trading dates, including dates without trades. They remain exploratory pointwise intervals across several endpoints. Candle-benchmark and prevalence comparisons help identify whether warnings add information beyond a common adverse candle or the underlying event frequency.", "",
        "A correct pullback warning can occur in a profitable trade or before a recovery. This study does not establish that exiting, tightening a stop, or reversing at a warning improves net P&L. Threshold sensitivity tables reuse the same warning rules; they are not a parameter-selection exercise.", "",
        f"Monitoring was available for {int(trade_detail.monitor_available.sum())}/{len(trade_detail)} trades. There are {dense.shape[0]//2:,} observed trade-minutes, {len(grid):,} grid rows including sensitivity endpoints, and {len(alert_stats)} grouped alert summaries. October 1 is excluded because the source replay is incomplete.", "",
        "Files: metrics.csv (primary and censored full-horizon metrics), sensitivity.csv, phase_metrics.csv, predictions_1m.csv, evaluation_windows.csv, alert_episodes.csv, alert_success.csv, first_alert_success.csv, trade_report.csv, daywise_results.csv, protocol.json, validation.json, provenance.json."]
    (out/"report.md").write_text("\n".join(lines)+"\n", encoding="utf-8")
    body = "\n".join(f"<p>{html.escape(p)}</p>" for p in lines if p and not p.startswith("|"))
    links = " ".join(f'<a href="{p.name}">{p.name}</a>' for p in out.glob("*.csv"))
    page = ('<!doctype html><html lang="en"><meta charset="utf-8"><title>G-3 pullback monitor report</title>'
        '<style>body{font:16px system-ui;max-width:1500px;margin:36px auto;padding:0 24px;color:#17212b}table{border-collapse:collapse;font-size:14px;display:block;overflow:auto}th,td{padding:8px;border:1px solid #ccd4df;text-align:right}th{background:#eaf1f8}p{max-width:1100px;line-height:1.55}a{display:inline-block;margin:5px}</style>'
        '<h1>Frozen G-3 pullback monitor</h1>'+tables.to_html(index=False,escape=True)+body+
        '<h2>Every trade</h2>'+trade_detail.to_html(index=False,escape=True)+'<h2>Downloads</h2>'+links+'</html>')
    (out/"report.html").write_text(page, encoding="utf-8")


def run(frozen_path: Path, out: Path, bootstrap_reps: int = 2000, print_outcomes: bool = False):
    if out.exists() and any(out.iterdir()):
        raise FileExistsError("Research output must be new or empty")
    out.mkdir(parents=True, exist_ok=True)
    protocol = json.loads(PROTOCOL.read_text(encoding="utf-8"))
    (out/"protocol.json").write_text(json.dumps(protocol,indent=2), encoding="utf-8")
    print("Verifying frozen G-3 inputs", flush=True)
    frozen = load_frozen(frozen_path)
    all_trades = frozen["trades"].copy()
    trades = all_trades.loc[all_trades.portfolio_executed.eq(True)].copy()
    trades["day"] = pd.to_datetime(trades.day).dt.strftime("%Y-%m-%d")
    for col in ("entry_ts", "exit_ts", "exit_bar_end_ts"):
        trades[col] = pd.to_datetime(trades[col], utc=True).dt.tz_convert(IST)
    minutes = frozen["minutes"].copy()
    minutes["day"] = pd.to_datetime(minutes.day).dt.strftime("%Y-%m-%d")
    minutes["ts"] = pd.to_datetime(minutes.ts, utc=True).dt.tz_convert(IST)
    days = sorted(str(d) for d in frozen["days"])
    if len(days) != 46 or len(trades) != 93:
        raise ValueError("This predeclared study expects frozen G-3's46 sessions/93 executed trades")
    print("Building causal observations and future labels", flush=True)
    dense, grid, trade_detail = make_predictions(trades, minutes, days)
    alerts = alert_episodes(dense)
    print(f"{len(dense)} primary-horizon minute rows; {len(grid)} evaluation windows; {len(alerts)} alerts", flush=True)
    for h in SPECS:
        alert_group = alerts.loc[alerts.horizon_minutes.eq(h)]
        counts = alert_group.groupby("trade_id").agg(
            alerts=("warning","size"), correct=("event","sum"), first=("decision_ts","min"))
        for field in ("alerts", "correct", "first"):
            trade_detail[f"{h}m_{field}"] = trade_detail.trade_id.map(counts[field])
    print("Computing day-cluster uncertainty", flush=True)
    metric = summarize(grid, days, bootstrap_reps=bootstrap_reps, seed=20261008)
    phase_metric = summarize_phases(grid, days)
    primary_mask = (metric.horizon_minutes.eq(5)&metric.threshold_pct.eq(.3)) | (metric.horizon_minutes.eq(30)&metric.threshold_pct.eq(.5))
    alert_stats = alert_summary(alerts)
    day_rows = []
    for day in days:
        for side in ("BOTH", "LONG", "SHORT"):
            for h, (threshold, _) in SPECS.items():
                x = grid.loc[grid.day.eq(day)&grid.horizon_minutes.eq(h)&grid.threshold_pct.eq(threshold)]
                if side != "BOTH":
                    x = x.loc[x.side.eq(side)]
                source_count = len(x)
                unknown_count = int((~x.outcome_available).sum())
                x = x.loc[x.outcome_available]
                day_rows.append(dict(day=day,side=side,horizon_minutes=h,source_observations=source_count,
                    unknown_observations=unknown_count,observations=len(x),
                    warnings=int(x.warning.sum()),events=int(x.event.sum()),
                    tp=int((x.warning&x.event).sum()),fp=int((x.warning&~x.event).sum()),
                    fn=int((~x.warning&x.event).sum()),tn=int((~x.warning&~x.event).sum())))
    frames = {"predictions_1m.csv":dense,"evaluation_windows.csv":grid,"alert_episodes.csv":alerts,
        "metrics.csv":metric.loc[primary_mask],"sensitivity.csv":metric,
        "phase_metrics.csv":phase_metric,"alert_success.csv":alert_stats,
        "first_alert_success.csv":alert_summary(alerts,True),"trade_report.csv":trade_detail,
        "daywise_results.csv":pd.DataFrame(day_rows)}
    for name, frame in frames.items():
        frame.to_csv(out/name,index=False)
    validation = dict(status="PASS",executed_trades=len(trades),selected_orders=len(all_trades),
        net_pnl_unchanged=float(trades.portfolio_net_profit_rupees.sum()),
        days=len(days),protocol_sha256=sha(PROTOCOL),monitor_source_sha256=sha(Path(__file__)),
        warnings_use_completed_bars=True,outcomes_start_after_decision=True,
        intrabar_unordered_extrema_not_labeled=True,baseline_trade_actions_changed=False,
        tuning_performed=False,bootstrap_reps=bootstrap_reps,
        monitor_available_trades=int(trade_detail.monitor_available.sum()))
    (out/"validation.json").write_text(json.dumps(validation,indent=2),encoding="utf-8")
    provenance = dict(frozen_path=str(frozen_path),frozen_manifest=frozen["manifest"],
        protocol_file=str(PROTOCOL),days=days,split_earlier30=days[:30],split_later16=days[30:],
        primary_thresholds={"5m":.3,"30m":.5},mode="HISTORICAL_OBSERVER_NO_TRADE_ACTIONS")
    (out/"provenance.json").write_text(json.dumps(provenance,indent=2,default=str),encoding="utf-8")
    write_report(out,frozen,metric,alert_stats,trade_detail,dense,grid)
    artifact_hashes = {p.name:sha(p) for p in out.iterdir() if p.is_file()}
    (out/"artifact_hashes.json").write_text(json.dumps(artifact_hashes,indent=2),encoding="utf-8")
    if print_outcomes:
        print(metric.loc[primary_mask & metric.analysis.eq("PRIMARY") & metric.side.ne("BOTH"),
            ["split","side","horizon_minutes","observations","tp","fp","fn","tn","precision","recall","prevalence","baseline_precision"]].to_string(index=False),flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--frozen-dir",type=Path,default=DEFAULT_FROZEN)
    parser.add_argument("--output-dir",type=Path,required=True)
    parser.add_argument("--bootstrap-reps",type=int,default=2000)
    parser.add_argument("--print-outcomes",action="store_true")
    args = parser.parse_args()
    run(args.frozen_dir,args.output_dir,args.bootstrap_reps,args.print_outcomes)
