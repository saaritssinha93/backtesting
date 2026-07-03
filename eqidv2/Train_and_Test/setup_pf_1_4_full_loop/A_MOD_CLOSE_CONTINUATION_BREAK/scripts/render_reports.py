r"""render_reports.py — render campaign JSON artifacts into the required markdown reports.

Reads (from the working dir):
  baseline_result.json, failure_study.json, sweeps.json, trials.csv, combos.json,
  confirmations.json, rescue.json (optional), iteration_records.json,
  pools/pool_full/_manifest.json

Writes:
  BASELINE_RESULT.md, PARAMETER_SWEEP_SUMMARY.md, ITERATION_LOG.md,
  FAILURE_ANALYSIS.md, CANDIDATE_CONFIGS.md, APPROVAL_REQUIRED_FINAL_RECOMMENDATION.md,
  candidates/<SETUP>_candidate_<NNN>.json

Research-only; never touches final_setup_conf.py.
"""
from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import pandas as pd

HERE = Path(__file__).resolve()
WORK = HERE.parents[1]
SETUP = "A_MOD_CLOSE_CONTINUATION_BREAK"
SIDE = "LONG"
TODAY = date.today().isoformat()

HEAD = f"_Generated {TODAY}. Research-only; NO live trades; NO final_setup_conf.py edits._"


def _load(name, default=None):
    p = WORK / name
    if not p.exists():
        return default
    return json.loads(p.read_text(encoding="utf-8"))


def mline(m):
    if not m:
        return "(not run)"
    return (f"n={m['n']} PF={m['net_pf']} net=Rs{m['net_pnl']:,.0f} win%={m.get('win_rate')} "
            f"avgW=Rs{m.get('avg_win', 0):,.0f} avgL=Rs{m.get('avg_loss', 0):,.0f} "
            f"maxDD=Rs{m.get('max_dd', 0):,.0f} SL/TGT/EOD={m.get('sl_cnt')}/{m.get('tgt_cnt')}/{m.get('eod_cnt')} "
            f"tpd={m.get('trades_per_day')} domT/D/S={m.get('trade_dom_gross')}/{m.get('day_dom')}/{m.get('sym_dom')} "
            f"dbp={m.get('day_block_p')}")


def mtable(m):
    if not m:
        return "(not run)"
    rows = [
        ("trades", m.get("n")), ("net PF", m.get("net_pf")),
        ("net PnL", f"Rs{m.get('net_pnl', 0):,.0f}"),
        ("win rate", f"{m.get('win_rate')}%"),
        ("wins / losses", f"{m.get('wins')} / {m.get('losses')}"),
        ("avg win / avg loss", f"Rs{m.get('avg_win', 0):,.0f} / Rs{m.get('avg_loss', 0):,.0f}"),
        ("gross profit / loss", f"Rs{m.get('gross_profit', 0):,.0f} / Rs{m.get('gross_loss', 0):,.0f}"),
        ("max drawdown", f"Rs{m.get('max_dd', 0):,.0f}"),
        ("SL / TGT / EOD exits", f"{m.get('sl_cnt')} / {m.get('tgt_cnt')} / {m.get('eod_cnt')}"),
        ("target-fill rate", f"{m.get('target_rate')}%"),
        ("trades/day", m.get("trades_per_day")),
        ("days / symbols", f"{m.get('n_days')} / {m.get('n_syms')}"),
        ("top-trade gross share", m.get("trade_dom_gross")),
        ("top-day net share", m.get("day_dom")),
        ("top-symbol net share", m.get("sym_dom")),
        ("day-block p", m.get("day_block_p")),
        ("top day", m.get("top_day")), ("top symbol", m.get("top_sym")),
    ]
    return "| metric | value |\n|---|---|\n" + "\n".join(f"| {k} | {v} |" for k, v in rows)


def render_baseline():
    b = _load("baseline_result.json")
    man = _load("pools/pool_full/_manifest.json", {})
    if not b:
        return
    w = b["windows"]
    lines = [f"# {SETUP} ({SIDE}) — BASELINE_RESULT", "", HEAD, "",
             "## Current rules (config of record)", "",
             "- **Config source:** NOT in final_setup_conf.py — unpromoted catalog setup; "
             "baseline = raw detector + production exit, plus the live-overlay OR-gate variant.",
             "- **Detector (5-min):** moderate-impulse bar, long structure, above session VWAP, "
             "`close_loc >= 0.75`, `close > prev_bar_high`, `rs_pct > 0.00`, `vol_ratio >= 1.4` "
             "(avwap_5min_ID_v2_backtesting.py:704-711; catalog min qs 6.8).",
             "- **Structural note:** the shared candidate scan keeps ONE candidate per (ticker, bar) "
             "by quality score with alphabetical tie-break, so A_MOD_BREAK_C1_HIGH shadows this setup "
             "outside BEAR regime — 96.8% of this setup's rows are BEAR-regime days "
             "(it is effectively a bear-day continuation LONG).",
             "- **Pre-momentum:** none. **Filters:** live overlay OR-gate "
             "(`signal_range_pct >= 2.2` OR `notional <= Rs100k`). **Guards:** none.",
             "- **SL/Target:** 0.70% / 1.50% (v6.SETUP_EXIT_RULES). "
             "**Exit:** first-touch SL/TARGET on 1-min bars else EOD 15:20 IST.",
             "- **Costs:** statutory NSE intraday + 15 bps/leg adverse slippage; "
             "entry = next 1-min open after the 5-min signal.", "",
             "## Sessions (exact)", "",
             f"- **TRAIN** {w['TRAIN'][0]}..{w['TRAIN'][1]} ({w['TRAIN'][2]} sessions) — requested 2026-03-01..2026-05-30",
             f"- **FIT** {w['FIT'][0]}..{w['FIT'][1]} ({w['FIT'][2]} sessions, first 60% of TRAIN)",
             f"- **VAL** {w['VAL'][0]}..{w['VAL'][1]} ({w['VAL'][2]} sessions, last 40% of TRAIN)",
             f"- **TEST** {w['TEST'][0]}..{w['TEST'][1]} ({w['TEST'][2]} sessions) — requested 2026-06-01..2026-07-02; "
             "2026-07-02 excluded (1-min exit data truncated ~09:30), 2026-06-26 has no 5-min data.",
             f"- Pool: {man.get('rows_final', '?')} raw rows over {man.get('n_sessions', '?')} sessions "
             f"({man.get('first_session')}..{man.get('last_session')}).", ""]
    for name, r in b["results"].items():
        lines += [f"## {name}", "",
                  f"cfg: SL {r['cfg']['sl']} / Tgt {r['cfg']['tgt']}, mask={r['cfg']['mask_terms']}, "
                  f"premom={r['cfg']['premom_terms']}, guard={r['cfg']['guard']}, "
                  f"or_gate={r.get('or_gate', False)}", ""]
        for wname in ("FIT", "VAL", "TRAIN", "TEST"):
            m = r["metrics"].get(wname)
            lines += [f"### {name} — {wname}", "", mtable(m), ""]
    diag = b.get("diagnosis")
    if diag:
        lines += ["## Initial diagnosis", ""] + [f"- {d}" for d in diag]
    (WORK / "BASELINE_RESULT.md").write_text("\n".join(lines), encoding="utf-8")
    print("wrote BASELINE_RESULT.md")


def render_sweeps():
    s = _load("sweeps.json")
    if not s:
        return
    lines = [f"# {SETUP} ({SIDE}) — PARAMETER_SWEEP_SUMMARY", "", HEAD, "",
             "One-knob-at-a-time sweeps on FIT/VAL from the baseline hypothesis "
             "(raw detector, SL 0.70 / Tgt 1.50). `score` = band objective "
             "`reward(min(PF_fit,PF_val)) − 0.80·|PF_fit−PF_val|` tenting at PF 1.70. "
             "`keep-for-combos` requires FIT & VAL PF >= 1.05 with >= 6 trades in each.", ""]
    titles = {"exits": "Exit grid (SL x Target)", "mask": "Indicator / price-action masks (single term)",
              "regime": "Regime (categorical)", "premom": "Pre-momentum gates (single term)",
              "guards": "Guards (time / top-N / portfolio)", "overlay": "Overlay variants"}
    for grp, rows in s.items():
        if not rows:
            continue
        lines += [f"## {titles.get(grp, grp)}", "",
                  "| knob value | FIT n/PF | VAL n/PF | score | decision |",
                  "|---|---|---|---|---|"]
        for r in sorted(rows, key=lambda x: -x["score"]):
            lines.append(f"| {r['label']} | {r['fit_n']}/{r['fit_pf']} | {r['val_n']}/{r['val_pf']} "
                         f"| {r['score']} | {r['decision']} |")
        keeps = [r for r in rows if r["decision"].startswith("keep")]
        best = max(rows, key=lambda x: x["score"]) if rows else None
        lines += ["", f"- stable values: {len(keeps)}/{len(rows)}; best: `{best['label']}` "
                      f"(FIT {best['fit_n']}/{best['fit_pf']}, VAL {best['val_n']}/{best['val_pf']})", ""]
    lines += ["## Overfit-risk notes", "",
              "- Values where FIT PF is high but VAL collapses are rejected by the gap penalty — "
              "see rows with high FIT PF and `reject` decision.",
              "- Thresholds are TRAIN quantiles only (q10..q90); no TEST information used.",
              "- market_ret_pct / notional / signal_minute masks were excluded by design "
              "(documented overfit vectors)."]
    (WORK / "PARAMETER_SWEEP_SUMMARY.md").write_text("\n".join(lines), encoding="utf-8")
    print("wrote PARAMETER_SWEEP_SUMMARY.md")


def render_iterlog():
    recs = _load("iteration_records.json", [])
    v2 = _load("iteration_records_v2.json", [])
    for r in v2:
        r = dict(r)
        r["iter"] = len(recs) + 1
        r["group"] = "v2:" + str(r["group"])
        recs.append(r)
    lines = [f"# {SETUP} ({SIDE}) — ITERATION_LOG", "", HEAD, "",
             f"Total logged iterations: **{len(recs)}** (each = one hypothesis scored on FIT/VAL; "
             "TRAIN/TEST columns filled only when that iteration was confirmed). "
             "Full trial-by-trial search records: `trials.csv`.", "",
             "Command for every iteration (same process, staged):",
             "```", "py -3.12 Train_and_Test\\setup_pf_1_4_full_loop\\A_MOD_CLOSE_CONTINUATION_BREAK\\scripts\\campaign_amccb.py --stages 2,3,4,5,6 --trials 500 --seed 7", "```", "",
             "| # | group | change | FIT n/PF | VAL n/PF | TRAIN | TEST | decision | failure class / next |",
             "|---|---|---|---|---|---|---|---|---|"]
    for r in recs:
        fit = f"{r['fit']['n']}/{r['fit']['pf']}" if r.get("fit") else "-"
        val = f"{r['val']['n']}/{r['val']['pf']}" if r.get("val") else "-"
        tr = (f"n={r['train']['n']} PF={r['train']['net_pf']} net=Rs{r['train']['net_pnl']:,.0f}"
              if r.get("train") else "-")
        te = (f"n={r['test']['n']} PF={r['test']['net_pf']} net=Rs{r['test']['net_pnl']:,.0f}"
              if r.get("test") else "-")
        ch = str(r["change"]).replace("|", "\\|")
        fc = (str(r.get("failure_class") or "") + " / " + str(r.get("next_action") or "")).replace("|", "\\|")
        lines.append(f"| {r['iter']} | {r['group']} | {ch} | {fit} | {val} | {tr} | {te} "
                     f"| {r['decision']} | {fc} |")
    (WORK / "ITERATION_LOG.md").write_text("\n".join(lines), encoding="utf-8")
    print(f"wrote ITERATION_LOG.md ({len(recs)} iterations)")


def render_failures():
    f = _load("failure_study.json")
    if not f:
        return
    conf = _load("confirmations.json", []) or []
    lines = [f"# {SETUP} ({SIDE}) — FAILURE_ANALYSIS", "", HEAD, "",
             "## Baseline book (TRAIN, raw detector @ SL0.70/T1.50, 15 bps/leg)", "",
             mline(f.get("train_metrics")), "",
             f"- winners {f.get('n_winners')} vs losers {f.get('n_losers')}", "",
             "## By outcome", "", f"- {f.get('by_outcome')}",
             f"- avg bars held by outcome: {f.get('avg_bars_held_by_outcome')}", "",
             "## By hour bucket", ""]
    for k, v in (f.get("by_hour") or {}).items():
        lines.append(f"- {k}: n={v['n']} net=Rs{v['net']:,.0f} PF={v['pf']}")
    lines += ["", "## By regime", ""]
    for k, v in (f.get("by_regime") or {}).items():
        lines.append(f"- {k}: n={v['n']} net=Rs{v['net']:,.0f} PF={v['pf']}")
    lines += ["", "## Winner vs loser feature medians (signal features)", "",
              "| feature | winners | losers |", "|---|---|---|"]
    wm, lm = f.get("winner_medians", {}), f.get("loser_medians", {})
    for k in wm:
        if wm.get(k) is None and lm.get(k) is None:
            continue
        lines.append(f"| {k} | {wm.get(k)} | {lm.get(k)} |")
    lines += ["", "## Worst days", ""]
    lines += [f"- {k}: Rs{v:,.0f}" for k, v in (f.get("worst_days") or {}).items()]
    lines += ["", "## Worst symbols", ""]
    lines += [f"- {k}: Rs{v:,.0f}" for k, v in (f.get("worst_symbols") or {}).items()]
    lines += ["", "## Worst trades", "", "| date | ticker | outcome | bars | net Rs |", "|---|---|---|---|---|"]
    for r in f.get("worst_trades", []):
        lines.append(f"| {r['trade_date']} | {r['ticker']} | {r['outcome']} | {r['bars_held']} "
                     f"| {r['net_pnl_rs']:,.0f} |")
    rej = [c for c in conf if c.get("verdict") and c["verdict"] != "CANDIDATE"]
    if rej:
        lines += ["", "## Why rejected candidates failed (confirmation stage)", ""]
        for c in rej:
            cfg = c["cfg"]
            m = ";".join(f"{a}{o}{b}" for a, o, b in cfg["mask_terms"]) or "-"
            p = ";".join(f"{a}{o}{b}" for a, o, b in cfg["premom_terms"]) or "-"
            lines.append(f"- SL{cfg['sl']}/T{cfg['tgt']} mask[{m}] pm[{p}] guard={cfg['guard']}: "
                         f"**{c['verdict']}** (TRAIN {mline(c.get('train'))} | TEST {mline(c.get('test'))})")
    (WORK / "FAILURE_ANALYSIS.md").write_text("\n".join(lines), encoding="utf-8")
    print("wrote FAILURE_ANALYSIS.md")


def render_candidates():
    conf = (_load("confirmations.json", []) or []) + (_load("confirmations_v2.json", []) or [])
    cands = [c for c in conf if c.get("verdict") == "CANDIDATE"]
    (WORK / "candidates").mkdir(exist_ok=True)
    lines = [f"# {SETUP} ({SIDE}) — CANDIDATE_CONFIGS", "", HEAD, ""]
    if not cands:
        lines += ["**No candidate cleared the full gate** "
                  "(TRAIN PF in [1.30,1.80], TEST PF > 1.40, positive net PnL both windows, "
                  "meaningful trades, domination caps, FIT/VAL coherence, robustness).",
                  "", "See APPROVAL_REQUIRED_FINAL_RECOMMENDATION.md for the closest near-misses."]
    for i, c in enumerate(cands, 1):
        cfg = c["cfg"]
        block = {
            "side": SIDE,
            "exit": {"sl_pct": cfg["sl"], "tgt_pct": cfg["tgt"]},
            "mask_terms": [list(t) for t in cfg["mask_terms"]],
            "pre_momentum_terms": [list(t) for t in cfg["premom_terms"]],
            "entry_guards": cfg["guard"] or {},
            "max_positions": cfg["max_positions"],
            "daily_loss_rs": cfg["daily_loss_rs"],
        }
        cand_file = WORK / "candidates" / f"{SETUP}_candidate_{i:03d}.json"
        cand_file.write_text(json.dumps({
            "setup": SETUP, "side": SIDE, "verdict": "APPROVAL_REQUIRED",
            "config": block, "train_15bps": c["train"], "test_15bps": c["test"],
            "checks": c.get("checks"), "robustness": c.get("robustness"),
        }, indent=2, default=str), encoding="utf-8")
        lines += [f"## Candidate {i:03d}", "", "```json", json.dumps(block, indent=2), "```", "",
                  f"- TRAIN @15bps: {mline(c['train'])}",
                  f"- TEST  @15bps: {mline(c['test'])}",
                  f"- checks: {c.get('checks')}",
                  f"- robustness: {c.get('robustness')}",
                  f"- file: `candidates/{cand_file.name}`",
                  "- Recommendation: **APPROVAL REQUIRED** (do not auto-promote).", ""]
    (WORK / "CANDIDATE_CONFIGS.md").write_text("\n".join(lines), encoding="utf-8")
    print(f"wrote CANDIDATE_CONFIGS.md ({len(cands)} candidates)")
    return cands


def render_v2():
    hyp = _load("hypotheses_v2.json", [])
    sw = _load("sweeps_v2.json", [])
    combos = _load("combos_v2.json", [])
    conf2 = _load("confirmations_v2.json", [])
    if not (hyp or sw or combos):
        return
    lines = [f"# {SETUP} ({SIDE}) — V2 EXPANDED-FEATURE CAMPAIGN REPORT", "", HEAD, "",
             "Pool: `pools/pool_enriched` — every signal row enriched with ~41 causal indicator/"
             "price-action/day-context features recomputed from OHLCV (uniform TRAIN/TEST coverage; "
             "stored MACD/BB/CCI/MFI/OBV/VWAP columns were 0% populated in June and NOT used) plus "
             "the 8 pre-momentum engine features as `x_pm_*` columns.", "",
             "## Structural hypothesis packs (all explainable rule sets)", "",
             "| hypothesis | exit | FIT n/PF | VAL n/PF | score | decision |", "|---|---|---|---|---|---|"]
    for r in sorted(hyp, key=lambda x: -x["score"]):
        lines.append(f"| {r['name']} | SL{r['sl']}/T{r['tgt']} | {r['fit_n']}/{r['fit_pf']} "
                     f"| {r['val_n']}/{r['val_pf']} | {r['score']} | {r['decision']} |")
    keeps = [r for r in sw if r["decision"] == "keep"]
    lines += ["", f"## Single-term sweeps over the expanded space: {len(keeps)} keeps / {len(sw)} tested", "",
              "Top 15 by band score:", "",
              "| term | FIT n/PF | VAL n/PF | score |", "|---|---|---|---|"]
    for r in sorted(sw, key=lambda x: -x["score"])[:15]:
        lines.append(f"| {r['label']} | {r['fit_n']}/{r['fit_pf']} | {r['val_n']}/{r['val_pf']} "
                     f"| {r['score']} |")
    lines += ["", "## Top TPE combinations (3,000 trials)", "",
              "| score | FIT n/PF | VAL n/PF | config |", "|---|---|---|---|"]
    for r in combos[:12]:
        cfg = r["cfg"]
        m = ";".join(f"{a}{o}{b}" for a, o, b in cfg["mask_terms"]) or "-"
        lines.append(f"| {round(r['score'],4)} | {r['fit_n']}/{round(r['fit_pf'],3)} "
                     f"| {r['val_n']}/{round(r['val_pf'],3)} | SL{cfg['sl']}/T{cfg['tgt']} [{m}] "
                     f"g={cfg['guard']} |")
    lines += ["", "## Confirmations (full TRAIN; TEST scored once if in band)", ""]
    for c in conf2:
        cfg = c["cfg"]
        m = ";".join(f"{a}{o}{b}" for a, o, b in cfg["mask_terms"]) or "-"
        t = c["train"]
        line = (f"- SL{cfg['sl']}/T{cfg['tgt']} [{m}] g={cfg['guard']}: TRAIN n={t['n']} "
                f"PF={t['net_pf']} net=Rs{t['net_pnl']:,.0f}")
        if c.get("test"):
            te = c["test"]
            line += f" | TEST n={te['n']} PF={te['net_pf']} net=Rs{te['net_pnl']:,.0f}"
        lines += [line, f"  - verdict: **{c['verdict']}**"]
    (WORK / "V2_EXPANDED_FEATURE_REPORT.md").write_text("\n".join(lines), encoding="utf-8")
    print("wrote V2_EXPANDED_FEATURE_REPORT.md")


def render_final(cands):
    conf = (_load("confirmations.json", []) or []) + (_load("confirmations_v2.json", []) or [])
    b = _load("baseline_result.json", {})
    lines = [f"# {SETUP} ({SIDE}) — APPROVAL_REQUIRED / FINAL RECOMMENDATION", "", HEAD, ""]
    if cands:
        best = max(cands, key=lambda c: (c["test"]["net_pf"], c["train"]["net_pf"]))
        cfg = best["cfg"]
        block = {
            "side": SIDE,
            "exit": {"sl_pct": cfg["sl"], "tgt_pct": cfg["tgt"]},
            "mask_terms": [list(t) for t in cfg["mask_terms"]],
            "pre_momentum_terms": [list(t) for t in cfg["premom_terms"]],
            "entry_guards": cfg["guard"] or {},
            "max_positions": cfg["max_positions"],
            "daily_loss_rs": cfg["daily_loss_rs"],
        }
        lines += ["## Approval recommendation: **YES — APPROVAL REQUIRED**", "",
                  "## Best candidate config (proposed)", "", "```json",
                  json.dumps(block, indent=2), "```", "",
                  f"- TRAIN @15bps: {mline(best['train'])}",
                  f"- TEST  @15bps: {mline(best['test'])}", "",
                  "## File that would need approval before edit", "",
                  "- `final_setup_conf.py` (repo root) — add `" + SETUP + "` under `FINAL_SETUP_CONF` "
                  "with the JSON block above (and mirror in Train_and_Test/final_setup_conf.py if desired).", "",
                  "> **DO NOT MOVE TO FINAL CONFIG UNTIL USER APPROVES**", ""]
    else:
        lines += ["## Approval recommendation: **NO**", "",
                  "No configuration met TRAIN PF in [1.30, 1.80] AND TEST PF > 1.40 with positive "
                  "PnL, meaningful trades, domination caps and robustness.", "",
                  "> **DO NOT MOVE TO FINAL CONFIG UNTIL USER APPROVES** (nothing to promote)", "",
                  "## Evidence summary (why NO)", "",
                  "- Baseline: TRAIN PF 0.315 (n=1,887, -Rs846k) / TEST PF 0.252 (n=613, -Rs316k); "
                  "FIT/VAL/TRAIN/TEST uniformly negative, so the loss is structural, not one bad month.",
                  "- 443 logged iterations: 49 exit combos, 198 single indicator/price-action masks, "
                  "144 pre-momentum gates, 17 guards, 3 regime slices, 46 Optuna TPE combinations, "
                  "and a full rescue loop (guard-only, single-term, simplified + 250-trial TPE round).",
                  "- Best single knob: FIT/VAL PF 0.45 (sig5_vol_ratio20>=4.8). Best combination: "
                  "FIT 0.56 / VAL 0.59 (SL0.85/T2.0, wick_skew>=0.042 & rs_pct<=3.02, pre3_range_r>=0.63, "
                  "min_slot 09:45). Best rescue config: PF ~0.51.",
                  "- v1: ZERO configs reached the TRAIN band floor of 1.30 -> the 10-run TEST budget was "
                  "never spent; TEST remained completely untouched by the v1 search (no test-fitting).",
                  "- Failure study: winner/loser feature medians are nearly identical, and losers have "
                  "HIGHER rs_pct and quality_score than winners — signal strength is anti-predictive; "
                  "every hour bucket and every regime slice with n>=20 is negative.", "",
                  "### v2 expanded-feature campaign (1,230 more iterations)", "",
                  "- Pool re-enriched with ~41 causal indicator/price-action/day-context features "
                  "computed from OHLCV (RSI+slope, ADX+slope, MACD, Bollinger, Keltner, Stochastic, "
                  "Williams %R, CCI, MFI, OBV, ROC 3/6/12, EMA20/50/200 structure+slopes, session "
                  "VWAP, day high/range/position, bar index, opening range, prev-day H/L/C, gap, "
                  "candle/volume structure) + the 8 pre-momentum features as searchable columns.",
                  "- 18 hand-written STRUCTURAL HYPOTHESIS packs (trend-alignment, fresh-at-day-high, "
                  "PDH break, squeeze-expansion, not-exhausted, MACD turn, volume+MFI thrust, OBV "
                  "accumulation, Keltner breakout, gap-up continuation, OR breakout, RSI momentum "
                  "zone, low-vol name, premom confirm, time windows) x 2 exit anchors: ALL 36 reject "
                  "(best PF ~0.5). There are ZERO signals in the first trading hour at all.",
                  "- 1,116 single-term sweeps over 59 features: 0 keeps; best single term "
                  "x_range_vs_avg20<=0.91 (quiet-bar breakouts) FIT 0.55 / VAL 0.60.",
                  "- 3,000 Optuna TPE trials (up to 3 AND-terms + slot guards + exits): exactly ONE "
                  "config reached the TRAIN band — sig5_adx_calc<=22.35 (weak 5-min trend) in the "
                  "10:00-11:00 window, top-3/slot, SL0.7/T2.0: TRAIN n=20 PF 1.56 (+Rs4,459), "
                  "identical through the true pre-momentum path. Scored ONCE on TEST: n=3, PF 0.31, "
                  "-Rs1,278, single trade = 100% of gross -> thin-pocket overfit, rejected by the gate. "
                  "Its relaxed neighbor (<=28.03) already drops to TRAIN PF 1.13 (knife-edge).", "",
                  "## Closest confirmations (full TRAIN, none in band)", ""]
        near = sorted([c for c in conf if c.get("train")],
                      key=lambda c: -c["train"]["net_pf"])[:3]
        for c in near:
            cfg = c["cfg"]
            m = ";".join(f"{a}{o}{b}" for a, o, b in cfg["mask_terms"]) or "-"
            p = ";".join(f"{a}{o}{b}" for a, o, b in cfg["premom_terms"]) or "-"
            lines += [f"- SL{cfg['sl']}/T{cfg['tgt']} mask[{m}] pm[{p}] guard={cfg['guard']}: "
                      f"verdict {c['verdict']}",
                      f"  - TRAIN {mline(c.get('train'))}",
                      f"  - TEST  {mline(c.get('test'))}"]
    lines += ["", "## Rerun commands", "", "```",
              "cd <repo root>",
              "py -3.12 avwap_5min_ID_v11_backtesting.py --mode historical_all_available "
              "--start_date 2026-06-25 --end_date 2026-07-02 --workers 8 "
              "--out Train_and_Test\\setup_pf_1_4_full_loop\\A_MOD_CLOSE_CONTINUATION_BREAK\\pools\\_tail_raw_gen",
              "py -3.12 Train_and_Test\\setup_pf_1_4_full_loop\\A_MOD_CLOSE_CONTINUATION_BREAK\\scripts\\build_pool_amccb.py",
              "py -3.12 Train_and_Test\\setup_pf_1_4_full_loop\\A_MOD_CLOSE_CONTINUATION_BREAK\\scripts\\eval_baseline.py",
              "py -3.12 Train_and_Test\\setup_pf_1_4_full_loop\\A_MOD_CLOSE_CONTINUATION_BREAK\\scripts\\campaign_amccb.py --stages 2,3,4,5,6 --trials 500 --seed 7",
              "py -3.12 Train_and_Test\\setup_pf_1_4_full_loop\\A_MOD_CLOSE_CONTINUATION_BREAK\\scripts\\render_reports.py",
              "```", "",
              "## Remaining risks", "",
              "- This setup exists as the same-bar-collapse residual of A_MOD_BREAK_C1_HIGH: "
              "96.8% of its signals occur in BEAR regime (bear-day continuation LONG). Regime shift "
              "changes its firing rate structurally.",
              "- June TEST is thin on several days; 2026-06-26 missing (no 5-min data), 2026-07-02 "
              "excluded (1-min truncation).",
              "- Screening-basis pool (raw candidates): live gate parity must be confirmed on the v11 "
              "conf backtest before any live watch.",
              "- 15 bps/leg slippage assumed; illiquid small-caps may be worse."]
    (WORK / "APPROVAL_REQUIRED_FINAL_RECOMMENDATION.md").write_text("\n".join(lines), encoding="utf-8")
    print("wrote APPROVAL_REQUIRED_FINAL_RECOMMENDATION.md")


def main():
    render_baseline()
    render_sweeps()
    render_v2()
    render_iterlog()
    render_failures()
    cands = render_candidates()
    render_final(cands)


if __name__ == "__main__":
    main()
