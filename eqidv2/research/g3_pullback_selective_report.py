"""Standalone HTML presentation for the observer-only selective pullback study.

All selection, forecasting and evaluation belong to the caller. This module
only presents supplied evidence; it never infers a trading action or changes
the frozen G-3 ledger.
"""
from __future__ import annotations

import html
import json
from pathlib import Path
from urllib.parse import quote

import pandas as pd


RATE_COLUMNS = {
    "grid_precision", "grid_recall", "no_warning_event_rate", "npv",
    "alert_precision", "alert_precision_low", "alert_precision_high",
    "first_alert_precision", "precision", "recall", "event_rate",
    "observed_event_rate", "precision_lower", "precision_upper",
    "one_sided_lower", "lower_bound", "npv_low", "npv_high",
    "grid_prevalence", "full_horizon_no_warning_event_rate",
    "precision_identification_lower", "exact_independent_lower",
}
LABELS = {
    "split": "Period", "policy": "Policy", "side": "Side",
    "horizon_minutes": "Horizon (min)", "model_stage": "Model stage",
    "grid_rows": "Grid windows", "resolved_grid_rows": "Resolved windows",
    "grid_events": "Event windows", "grid_warnings": "Warned windows",
    "grid_tp": "Correct windows", "grid_fp": "False windows",
    "grid_fn": "Missed event windows", "grid_tn": "Quiet unalerted windows",
    "grid_precision": "Grid precision", "grid_recall": "Grid recall",
    "no_warning_event_rate": "Event rate without warning",
    "npv": "No-warning negative predictive value",
    "alert_count": "Alerts", "resolved_alerts": "Resolved alerts",
    "successful_alerts": "Correct alerts", "false_alerts": "False alerts",
    "alert_precision": "Alert precision", "warned_trades": "Warned trades",
    "warned_days": "Warned dates", "median_lead_minutes": "Median lead (min)",
    "alert_precision_low": "95% cluster CI: lower",
    "alert_precision_high": "95% cluster CI: upper",
    "first_alert_count": "First alerts", "first_alert_precision": "First-alert precision",
    "tradingsymbol": "Symbol", "trade_id": "Trade", "day": "Date",
    "monitor_available": "Monitoring available", "net_pnl": "Frozen net P&L (INR)",
    "no_qualifying_watch": "No qualifying watch", "no_high_confidence_signal": "No high-confidence signal",
    "monitoring_rows": "Observed minutes", "insufficient_evidence_rows": "Insufficient-evidence minutes",
    "high_confidence_alerts": "High-confidence alerts", "research_watch_alerts": "Research-watch alerts",
    "availability": "Monitoring availability", "missed_grid_events": "Events missed by watch",
    "full_horizon_no_warning_rows": "Full-horizon eligible unalerted windows",
    "full_horizon_no_warning_event_rate": "Event rate in eligible unalerted windows",
    "grid_prevalence": "Grid event prevalence", "score_band": "Raw score band",
    "mean_raw_score": "Mean raw score", "observed_event_rate": "Observed event frequency",
}


def _escape(value) -> str:
    return html.escape(str(value), quote=True)


def _missing(value) -> bool:
    if value is None:
        return True
    try:
        return bool(pd.isna(value))
    except (TypeError, ValueError):
        return False


def _cell(value, column: str) -> str:
    if _missing(value):
        return "N/A"
    if isinstance(value, (dict, list, tuple)):
        return _escape(json.dumps(value, ensure_ascii=False, default=str))
    if column in RATE_COLUMNS:
        try:
            return f"{100 * float(value):.2f}%"
        except (TypeError, ValueError):
            return _escape(value)
    if isinstance(value, bool):
        return "Yes" if value else "No"
    if isinstance(value, float):
        if "pnl" in column or "rupees" in column:
            return f"{value:,.2f}"
        return f"{value:,.0f}" if value.is_integer() else f"{value:,.3f}"
    return _escape(value)


def _table(frame: pd.DataFrame, columns=None, *, searchable=False,
           empty="No observations are available for this view.") -> str:
    if frame is None or frame.empty:
        return f'<p class="empty">{_escape(empty)}</p>'
    chosen = [c for c in (columns or list(frame.columns)) if c in frame.columns]
    if not chosen:
        chosen = list(frame.columns)
    search = ('<label class="search">Filter these rows '
              '<input type="search" placeholder="Date, symbol, policy, or status" '
              'aria-label="Filter table rows"></label>') if searchable else ""
    header = "".join(f'<th scope="col">{_escape(LABELS.get(c, c.replace("_", " ").capitalize()))}</th>'
                     for c in chosen)
    rows = []
    for record in frame.loc[:, chosen].itertuples(index=False, name=None):
        rows.append("<tr>" + "".join(f"<td>{_cell(v, c)}</td>" for c, v in zip(chosen, record)) + "</tr>")
    return (f'<div class="table-block">{search}<div class="table-wrap" tabindex="0">'
            f'<table><thead><tr>{header}</tr></thead><tbody>{"".join(rows)}</tbody></table>'
            f'</div><p class="row-count">{len(frame):,} rows</p></div>')


def _number(value, *, currency=False) -> str:
    if _missing(value):
        return "N/A"
    if isinstance(value, (list, tuple)):
        value = len(value)
    try:
        return f"₹{float(value):,.2f}" if currency else f"{int(value):,}"
    except (TypeError, ValueError):
        return str(value)


def _subset(frame: pd.DataFrame, **criteria) -> pd.DataFrame:
    result = frame.copy()
    for key, value in criteria.items():
        if key in result:
            result = result.loc[result[key].eq(value)]
    return result


def _json_block(value) -> str:
    return '<pre>' + _escape(json.dumps(value, indent=2, ensure_ascii=False, default=str)) + '</pre>'


def write_report(out: Path, summary: dict, metrics: pd.DataFrame,
                 selection: dict, daywise: pd.DataFrame, trades: pd.DataFrame,
                 bands: pd.DataFrame) -> Path:
    """Write ``report.html`` using supplied results, without altering inputs."""
    out = Path(out)
    out.mkdir(parents=True, exist_ok=True)
    metrics = metrics.copy() if metrics is not None else pd.DataFrame()
    daywise = daywise.copy() if daywise is not None else pd.DataFrame()
    trades = trades.copy() if trades is not None else pd.DataFrame()
    bands = bands.copy() if bands is not None else pd.DataFrame()

    later = _subset(metrics, split="LATER16", model_stage="ALL")
    if later.empty and "split" in metrics:
        later = metrics.loc[metrics.split.astype(str).str.upper().isin(["EVALUATION", "EVAL16", "HELD16"])]
        if "model_stage" in later and later.model_stage.eq("ALL").any():
            later = later.loc[later.model_stage.eq("ALL")]
    if "side" in later and later.side.eq("BOTH").any():
        combined = later.loc[later.side.eq("BOTH")]
        by_side = later.loc[later.side.ne("BOTH")]
    else:
        combined, by_side = later, pd.DataFrame()
    high = _subset(combined, policy="HIGH_CONFIDENCE") if "policy" in combined else pd.DataFrame()
    high_count = pd.to_numeric(high.get("alert_count", pd.Series(dtype=float)), errors="coerce").sum()
    if not high.empty and high_count == 0:
        verdict = "No high-confidence pullback warning qualified in the later evaluation sessions."
    elif not high.empty:
        verdict = "Selective pullback warnings were measured on reused historical evaluation sessions."
    else:
        verdict = "Selective pullback research: inspect warning evidence and no-signal coverage separately."

    parts = [
        '<header><div class="eyebrow">FROZEN G-3 · OBSERVER-ONLY RESEARCH</div>',
        '<h1>Pullback prediction, with an explicit option to abstain</h1>',
        f'<p class="lede">{_escape(verdict)}</p>',
        f'<p class="meta">Version: {_escape(summary.get("version", "N/A"))} '
        f'· Status: {_escape(summary.get("status", "N/A"))}</p></header>',
        '<main><section class="cards" aria-label="Study overview">',
    ]
    for title, value, detail in (
        ("Available sessions", _number(summary.get("sessions")), "Recorded archive; not all calendar dates"),
        ("Frozen executed trades", _number(summary.get("trades")), "Entries, stops, targets and costs preserved"),
        ("Monitorable trades", _number(summary.get("monitored_trades")), "Complete post-entry observations required"),
        ("Frozen net P&L", _number(summary.get("frozen_net_pnl"), currency=True), "Historical ledger; not monitor-generated profit"),
    ):
        parts.append(f'<div class="card"><span>{_escape(title)}</span><strong>{_escape(value)}</strong>'
                     f'<small>{_escape(detail)}</small></div>')
    parts.extend([
        '</section><aside class="notice"><strong>No warning is not an all-clear.</strong> '
        'It means the selected rule did not find enough evidence to warn. A pullback can still occur. '
        'Missing observations and an ineligible profit stage require abstention. Historical scores are '
        'exploratory model scores, not calibrated probabilities.</aside>',
        '<nav aria-label="Report contents"><a href="#comparison">Evaluation</a>'
        '<a href="#coverage">No-signal coverage</a><a href="#stages">Stage support</a>'
        '<a href="#daily">Daywise results</a><a href="#protocol">Interpretation</a>'
        '<a href="#artifacts">Download evidence</a></nav>',
        '<section id="comparison"><h2>Later-session comparison</h2>',
        '<p>The original monitor is the control. <strong>RESEARCH_WATCH</strong> reports an exploratory '
        'candidate. <strong>HIGH_CONFIDENCE</strong> applies the stronger evidence gate. '
        'The later 16 sessions have already been inspected in previous research; they are reused '
        'historical evaluation, not untouched prospective validation.</p>',
    ])
    alert_columns = ["policy", "side", "horizon_minutes", "alert_count", "resolved_alerts",
                     "successful_alerts", "false_alerts", "alert_precision", "alert_precision_low",
                     "alert_precision_high", "warned_trades", "warned_days", "median_lead_minutes",
                     "first_alert_count", "first_alert_precision"]
    parts.append(_table(combined, alert_columns, empty="No later-session aggregate metrics were supplied."))
    if not by_side.empty:
        parts.append('<h3>Separate LONG and SHORT results</h3>')
        parts.append('<p>Side-specific performance is visible here because a pooled result can conceal '
                     'failure on one direction. Small denominators do not establish reliable improvement.</p>')
        parts.append(_table(by_side, alert_columns))
    matched = _subset(metrics, split="LATER16", model_stage="ENTRY_RISK", side="BOTH")
    if not matched.empty:
        parts.append('<h3>Comparable entry-risk scope: trade age up to 30 minutes</h3>')
        parts.append('<p>Compare the candidate with the original policy inside the same entry-risk '
                     'stage. A comparison with the original all-age policy also reflects abstention '
                     'later in trades. Higher historical precision alone is not a general improvement: '
                     'coverage, missed events, side-specific failures and future validation still matter.</p>')
        parts.append(_table(matched, ["policy", "horizon_minutes", "alert_count", "resolved_alerts",
                                       "successful_alerts", "false_alerts", "alert_precision", "warned_trades",
                                       "warned_days", "grid_events", "grid_tp", "grid_recall"]))
    parts.extend([
        '<p class="caption">Correct alerts predict a new adverse movement from the completed warning '
        'close: at least 0.30% within 5 minutes or 0.50% within 30 minutes. LONG adverse means a fall; '
        'SHORT adverse means a rise. Repeated alerts are correlated. The supplied 95% intervals '
        'resample trading dates; a degenerate or narrow interval with little support is not proof '
        'of certainty. No alerts means precision is undefined, not 100%.</p>',
        '<h3>What happened when there was no warning?</h3>',
    ])
    parts.append(_table(combined, ["policy", "side", "horizon_minutes", "grid_rows", "resolved_grid_rows",
                                  "grid_events", "grid_warnings", "grid_tp", "grid_fp", "grid_fn", "grid_tn",
                                  "grid_precision", "grid_recall", "no_warning_event_rate", "npv",
                                  "full_horizon_no_warning_rows", "full_horizon_no_warning_event_rate"]))
    parts.extend([
        '<p class="caption">These fixed-grid windows provide the recall and no-warning denominators. '
        'Do not combine grid recall with alert precision as if they came from one confusion matrix. '
        'Negative predictive value is the fraction of resolved unalerted windows without the defined '
        'event; a high value can arise simply because events are rare. The full-horizon sensitivity '
        'retains observed events or windows that remained open through expiry, excluding early-exit '
        'non-events. Its selected denominator is different from the primary exposure question.</p>',
    ])
    alerts_path = out / "selective_alerts.csv"
    if alerts_path.is_file():
        alerts = pd.read_csv(alerts_path)
        period_column = "day_split" if "day_split" in alerts else "split"
        if period_column in alerts:
            alerts = alerts.loc[alerts[period_column].eq("LATER16")].copy()
        if not alerts.empty:
            for source, display in (("decision_ts", "Warning time (IST)"),
                                     ("event_ts", "Event bar time (IST)")):
                if source in alerts:
                    stamps = pd.to_datetime(alerts[source], utc=True, errors="coerce")
                    alerts[display] = stamps.dt.tz_convert("Asia/Kolkata").dt.strftime("%Y-%m-%d %H:%M")
            if "event" in alerts and "outcome_available" in alerts:
                alerts["Observed outcome"] = ["Unknown" if not available else "New pullback observed" if event else "No threshold event"
                    for available, event in zip(alerts.outcome_available, alerts.event)]
            parts.append('<h3>Each selective alert in the later sessions</h3>')
            parts.append('<p>Times below are in IST. Lead time is measured from the warning close to '
                         'the first qualifying event bar; an intrabar threshold crossing can occur '
                         'up to one minute before that bar timestamp. False alerts have no event lead. '
                         'A raw model score is not a validated probability.</p>')
            parts.append(_table(alerts, ["day", "tradingsymbol", "side", "model_stage", "horizon_minutes",
                                          "Warning time (IST)", "trade_age_minutes", "decision_close",
                                          "risk_score", "Observed outcome", "Event bar time (IST)",
                                          "lead_minutes", "max_future_adverse_pct", "signal_state"], searchable=True))
    parts.extend([
        '</section>',
        '<section id="coverage"><h2>Every trade: warnings, no signal and unavailable monitoring</h2>',
        '<p>All supplied trade records are shown so a trade with no qualifying warning remains visible. '
        'Zero warnings means <strong>no qualifying signal</strong>. It does not mean no future pullback. '
        'An unavailable observation or no established profit stage is a separate reason to abstain.</p>',
    ])
    trade_first = ["day", "tradingsymbol", "side", "horizon_minutes", "availability",
                   "monitoring_rows", "no_qualifying_watch", "no_high_confidence_signal",
                   "research_watch_alerts", "high_confidence_alerts", "insufficient_evidence_rows",
                   "grid_events", "missed_grid_events", "first_watch_ts", "trade_id", "policy",
                   "status", "prediction_status", "monitor_available", "monitoring_minutes",
                   "unavailable_reason", "no_signal_reason", "reason", "net_pnl"]
    trade_columns = [c for c in trade_first if c in trades] + [c for c in trades if c not in trade_first]
    if "no_qualifying_watch" in trades:
        no_watch = trades.loc[trades.no_qualifying_watch.eq(True)]
        parts.append('<h3>Every trade and horizon with no qualifying research watch</h3>')
        parts.append(_table(no_watch, trade_columns, searchable=True,
                            empty="Every supplied trade/horizon has a qualifying research watch."))
        parts.append('<details><summary>All trade and horizon records, including issued warnings</summary>')
        parts.append(_table(trades, trade_columns, searchable=True))
        parts.append('</details>')
    else:
        parts.append(_table(trades, trade_columns, searchable=True))
    parts.append('</section><section id="stages"><h2>Support by model stage</h2>')
    parts.append('<p>Entry risk and later profit giveback answer different questions. Later profit-giveback '
                 'decisions require favorable progress already visible at decision time. Ineligible stages '
                 'abstain; they must not silently appear as validated low risk.</p>')
    staged = metrics.loc[metrics.model_stage.ne("ALL")] if "model_stage" in metrics else metrics
    stage_columns = ["split", "policy", "side", "horizon_minutes", "model_stage", "grid_rows",
                     "resolved_grid_rows", "grid_events", "alert_count", "resolved_alerts",
                     "successful_alerts", "alert_precision", "warned_trades", "warned_days",
                     "grid_recall", "no_warning_event_rate"]
    parts.append(_table(staged, stage_columns, searchable=True))
    parts.extend([
        '<h3>Score bands and observed frequencies</h3>',
        '<p>Raw scores are not calibrated probabilities. A score of 0.95 does not establish a 95% chance '
        'of a pullback. Read each band with its number of observations, dates, trades and observed events. '
        'Empty or sparse bands provide little evidence.</p>',
    ])
    parts.append(_table(bands, searchable=True))
    parts.extend([
        '</section><section id="daily"><h2>All session-level counts</h2>',
        '<p>Zero-alert and zero-trade sessions should remain in the exported daily calendar. Filter by '
        'date, policy, horizon or status to inspect the available evidence.</p>',
    ])
    parts.append(_table(daywise, searchable=True))
    parts.extend([
        '</section><section id="protocol"><h2>What the evidence can establish</h2>',
        '<h3>A strict warning target is a gate, not a guarantee</h3>',
        '<p>The high-confidence research target is at least <strong>95% observed precision</strong> '
        'with a <strong>90% one-sided lower confidence bound</strong> for independent outcomes. '
        'With no failures, at least 29 independent successes are needed to exceed that 90% lower bound '
        'at 95% confidence. Repeated minute warnings and several trades on one date are correlated; '
        '29 correlated alerts do not constitute 29 independent successes. Passing a historical gate '
        'does not promise 99% or 100% future accuracy.</p>',
        '<h3>Labels describe exposure while the frozen trade remains open</h3>',
        '<p>The endpoint is capped at the earlier of the forecast horizon and the actual frozen exit. '
        'An early exit without the threshold event is a non-event for this exposure question; it is '
        'not evidence of safety for the unused remainder of the horizon. Unknown intrabar ordering '
        'is excluded from resolved outcomes. A movement already completed when the decision is made '
        'does not count as a successful future prediction.</p>',
        '<h3>Why trade age was added</h3>',
        '<p>In the existing 46-session history, the original first post-entry 5-minute anchor had '
        '<strong>31 events in 92 windows (33.70%)</strong>. For trade ages above 30 minutes, the original '
        '5-minute grid had <strong>150 events in 3,272 windows (4.58%)</strong>. These are descriptive '
        'findings from already inspected history, not independent evidence that a newly selected model '
        'will predict well. The original 30-minute grid has no ages 6–30 anchor: its first anchors '
        'are at trade ages 1 and 31 minutes.</p>',
        '<h3>Chronological development and remaining validation</h3>',
        '<p>Training and selection use earlier dates; evaluation follows in time. Entire trading dates '
        'must remain together. The current archive has already informed strategy and monitor research. '
        'A fixed candidate needs new, prospectively collected observations before claims of reliable '
        'future precision. The study does not test an exit strategy or establish profit from acting '
        'on a warning.</p>',
    ])
    date_rows = []
    for key, title in (("train_days", "Training"), ("check_days", "Selection check"),
                       ("evaluation_days", "Historical evaluation")):
        days = summary.get(key, []) or []
        date_rows.append({"Period": title, "Sessions": len(days), "Dates": ", ".join(map(str, days))})
    parts.append(_table(pd.DataFrame(date_rows)))
    parts.append('<details><summary>Frozen candidate selection and evidence gates</summary>')
    parts.append(_json_block(selection))
    parts.append('</details><details><summary>Validation and source provenance</summary>')
    parts.append(_json_block({"input_hashes_unchanged": summary.get("input_hashes_unchanged"),
                              "validation": summary.get("validation", {}),
                              "sources": summary.get("sources", {})}))
    parts.append('</details>')
    sources = summary.get("sources", [])
    if isinstance(sources, list):
        links = []
        for source in sources:
            if isinstance(source, str) and source.startswith("https://"):
                title = ("Probability calibration: scikit-learn documentation" if "scikit-learn.org" in source
                         else "Binomial confidence limits: NIST handbook" if "nist.gov" in source
                         else source)
                links.append(f'<a href="{_escape(source)}">{_escape(title)}</a>')
        if links:
            parts.append('<p class="caption">Method references for score interpretation and confidence bounds: '
                         + "; ".join(links) + '.</p>')
    parts.append('</section><section id="artifacts"><h2>Download the evidence</h2>')
    artifacts = sorted(p for p in out.iterdir() if p.is_file() and p.suffix.lower() in {".csv", ".json"})
    if artifacts:
        parts.append('<ul class="artifacts">')
        for path in artifacts:
            parts.append(f'<li><a href="{quote(path.name)}">{_escape(path.name)}</a>'
                         f'<span>{path.stat().st_size / 1024:,.1f} KB</span></li>')
        parts.append('</ul>')
    else:
        parts.append('<p class="empty">No CSV or JSON artifacts were present when this report was written.</p>')
    parts.append('</section></main><footer>Frozen G-3 · Historical research · No entry, stop, target or cost changes</footer>')

    style = """
    :root{color-scheme:light;--ink:#142636;--muted:#5c6c78;--line:#dce4e9;--navy:#123d56;--paper:#fff}
    *{box-sizing:border-box}body{margin:0;background:#f3f6f8;color:var(--ink);font:15px/1.6 system-ui,-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif}
    header{background:linear-gradient(120deg,#0c2638,#184f68);color:white;padding:48px max(24px,calc((100vw - 1400px)/2)) 40px}
    header h1{font-size:clamp(28px,3vw,42px);line-height:1.18;max-width:970px;margin:10px 0 18px;font-weight:720;letter-spacing:-.025em}
    .eyebrow{font-size:12px;letter-spacing:.12em;color:#b4d6e8;font-weight:700}.lede{font-size:20px;max-width:1020px;margin:0 0 12px}.meta{font-size:12px;color:#c6dce7;overflow-wrap:anywhere}
    main{max-width:1450px;padding:28px 24px 50px;margin:auto}.cards{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:14px;margin:0 0 24px;background:none;border:0;padding:0;box-shadow:none}
    .card{background:var(--paper);padding:19px 20px;border:1px solid var(--line);border-radius:10px}.card span{display:block;color:var(--muted);font-size:12px;text-transform:uppercase;letter-spacing:.035em}.card strong{display:block;font-size:26px;margin:5px 0}.card small{display:block;color:var(--muted);font-size:12px;line-height:1.5}
    .notice{background:#fff6df;border:1px solid #e9cc81;border-left:5px solid #ba7e11;border-radius:6px;padding:18px 20px;margin-bottom:20px;max-width:none}.notice strong{color:#725011}
    nav{display:flex;flex-wrap:wrap;gap:8px;margin:22px 0}nav a{padding:7px 12px;background:#e7eff4;border-radius:6px;text-decoration:none;font-size:13px;font-weight:650}
    section{background:var(--paper);border:1px solid var(--line);border-radius:10px;margin:0 0 22px;padding:25px;box-shadow:0 2px 4px #122b3e03;scroll-margin-top:18px}
    h2{font-size:24px;line-height:1.3;margin:0 0 14px;letter-spacing:-.02em}h3{font-size:18px;margin:28px 0 10px}p{max-width:1150px;margin:10px 0 16px}a{color:#136590}a:hover{text-decoration:underline}.caption{font-size:13px;color:var(--muted);margin-top:13px}
    .table-wrap{overflow:auto;border:1px solid var(--line);border-radius:6px;max-height:640px}table{border-collapse:collapse;width:100%;font-size:12px;line-height:1.45}th{position:sticky;top:0;background:#eaf0f4;color:#203e52;font-size:11px;font-weight:700;text-align:left;min-width:86px;max-width:190px;z-index:1}th,td{padding:10px 12px;border-bottom:1px solid #e6ecf0;vertical-align:top}td{font-variant-numeric:tabular-nums;max-width:380px;overflow-wrap:anywhere}tbody tr:nth-child(even){background:#f8fafb}tbody tr:hover{background:#edf5fa}tbody tr:last-child td{border-bottom:0}
    .row-count{font-size:11px;color:var(--muted);margin:6px 0 14px}.search{display:flex;align-items:center;gap:12px;font-size:12px;color:var(--muted);margin:12px 0}input{border:1px solid #bdcdd8;border-radius:5px;padding:9px 11px;width:min(380px,70%);font:inherit;background:white;color:var(--ink)}input:focus{outline:2px solid #8fb8d0;outline-offset:2px}
    details{border:1px solid var(--line);border-radius:6px;margin:16px 0;padding:12px 14px}summary{cursor:pointer;font-weight:650;color:var(--navy)}details .table-block{margin-top:15px}pre{font:12px/1.6 ui-monospace,Consolas,monospace;overflow:auto;max-height:620px;background:#f6f8fa;padding:16px;border-radius:5px;white-space:pre-wrap;overflow-wrap:anywhere}.empty{color:var(--muted);padding:16px;border:1px dashed #bfccd6;border-radius:6px;background:#f7f9fb}
    .artifacts{list-style:none;padding:0;display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:8px}.artifacts li{border:1px solid var(--line);border-radius:5px;padding:10px 12px;display:flex;justify-content:space-between;gap:12px;font-size:13px}.artifacts a{overflow-wrap:anywhere}.artifacts span{color:var(--muted);font-size:11px;white-space:nowrap}footer{padding:22px;text-align:center;color:var(--muted);font-size:12px;border-top:1px solid var(--line)}
    @media(max-width:900px){.cards{grid-template-columns:repeat(2,minmax(0,1fr))}.artifacts{grid-template-columns:1fr}section{padding:20px}}
    @media(max-width:520px){main{padding:18px 12px}.cards{gap:8px}.card{padding:13px}.card strong{font-size:21px}header{padding:30px 18px}section{padding:17px}.search{display:block}input{display:block;width:100%;margin-top:6px}}
    @media print{body{background:white;font-size:10pt}header{background:white;color:#142636;padding:10px 0}.eyebrow,.meta{color:#526373}.lede{font-size:13pt}main{padding:0}nav,.search{display:none}section{break-inside:auto;border:0;padding:12px 0}.cards{grid-template-columns:repeat(4,minmax(0,1fr))}.table-wrap{max-height:none;overflow:visible}table{font-size:8pt}th{position:static}th,td{padding:5px}details{display:block}footer{font-size:8pt}.artifacts{display:block}}
    """
    script = """
    document.querySelectorAll('.table-block').forEach(function(block){
      var input=block.querySelector('input'); if(!input)return;
      var rows=Array.from(block.querySelectorAll('tbody tr'));
      input.addEventListener('input',function(){var q=input.value.trim().toLowerCase();var n=0;
        rows.forEach(function(row){var show=row.textContent.toLowerCase().includes(q);row.hidden=!show;if(show)n++;});
        block.querySelector('.row-count').textContent=n.toLocaleString()+' of '+rows.length.toLocaleString()+' rows';
      });
    });
    """
    document = ('<!doctype html><html lang="en"><head><meta charset="utf-8">'
                '<meta name="viewport" content="width=device-width,initial-scale=1">'
                '<title>G-3 selective pullback research</title><style>' + style + '</style></head><body>'
                + "\n".join(parts) + '<script>' + script + '</script></body></html>')
    path = out / "report.html"
    path.write_text(document, encoding="utf-8")
    return path
