r"""write_reports.py — generate the campaign markdown deliverables for
B_AVWAP_RECLAIM_REVERSAL from the artifacts produced by recreate_pool.py + run_full_loop.py.

RESEARCH-ONLY. Writes ONLY inside Train_and_Test/setup_pf_1_4_full_loop/B_AVWAP_RECLAIM_REVERSAL/.

  POOL_RECREATION_REPORT.md   <- pools/pool_manifest.json + sessions_coverage.csv
  PARAMETER_INVENTORY.md      <- pool columns + conf rules + supported knobs
  BASELINE_RESULT.md          <- run_summary.json baseline metrics
  PARAMETER_SWEEP_SUMMARY.md  <- sweeps.csv
  ITERATION_LOG.md            <- iteration_log.csv + trials.csv
  FAILURE_ANALYSIS.md         <- baseline/finalist trade details + run_summary
  CANDIDATE_CONFIGS.md        <- passing results (+ candidates/*.json)
  APPROVAL_REQUIRED_FINAL_RECOMMENDATION.md

Run from repo root:
  py -3.12 Train_and_Test\setup_pf_1_4_full_loop\B_AVWAP_RECLAIM_REVERSAL\scripts\write_reports.py
"""
from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import pandas as pd

_HERE = Path(__file__).resolve().parent
WORK = _HERE.parent
POOLS = WORK / "pools"
SETUP = "B_AVWAP_RECLAIM_REVERSAL"
SIDE = "LONG"
TODAY = date.today().isoformat()
HDR = f"_Generated {TODAY}. Research-only; NO live trades; NO final_setup_conf.py edits._"
WARNING = "> **DO NOT MOVE TO FINAL CONFIG UNTIL USER APPROVES**"


def _m(mm):
    if not mm:
        return "(not run)"
    return (f"n={mm['n']} PF={mm['net_pf']} net=Rs{mm['net_pnl']:,.0f} win%={mm['win_rate']} "
            f"avgW=Rs{mm['avg_win']:,.0f} avgL=Rs{mm['avg_loss']:,.0f} maxDD=Rs{mm['max_dd']:,.0f} "
            f"SL/TGT/EOD={mm['sl_cnt']}/{mm['tgt_cnt']}/{mm['eod_cnt']} tgt%={mm['target_rate']} "
            f"tpd={mm['trades_per_day']} tradeDom={mm['trade_dom_gross']} dayDom={mm['day_dom']} "
            f"symDom={mm['sym_dom']} dbp={mm['day_block_p']}")


def _mtable(m):
    ratio = round(abs(m["avg_win"] / m["avg_loss"]), 2) if m.get("avg_loss") else "n/a"
    return (f"| trades | {m['n']} |\n| net PF | {m['net_pf']} |\n| net PnL | Rs{m['net_pnl']:,.0f} |\n"
            f"| win rate | {m['win_rate']}% |\n| wins / losses | {m['wins']} / {m['losses']} |\n"
            f"| avg win / avg loss | Rs{m['avg_win']:,.0f} / Rs{m['avg_loss']:,.0f} |\n"
            f"| avgW/avgL ratio | {ratio} |\n"
            f"| gross profit / loss | Rs{m['gross_profit']:,.0f} / Rs{m['gross_loss']:,.0f} |\n"
            f"| max drawdown | Rs{m['max_dd']:,.0f} |\n"
            f"| SL / TGT / EOD exits | {m['sl_cnt']} / {m['tgt_cnt']} / {m['eod_cnt']} |\n"
            f"| target-fill rate | {m['target_rate']}% |\n"
            f"| trades/day | {m['trades_per_day']} |\n| days / symbols | {m['n_days']} / {m['n_syms']} |\n"
            f"| top-trade gross share | {m['trade_dom_gross']} |\n| top-day net share | {m['day_dom']} |\n"
            f"| top-symbol net share | {m['sym_dom']} |\n| day-block p | {m['day_block_p']} |\n"
            f"| top day | {m['top_day']} |\n| top symbol | {m['top_sym']} |")


def main() -> int:
    manifest = json.loads((POOLS / "pool_manifest.json").read_text(encoding="utf-8"))
    summary = json.loads((WORK / "run_summary.json").read_text(encoding="utf-8"))
    cov = pd.read_csv(POOLS / "sessions_coverage.csv")
    sweeps = pd.read_csv(WORK / "sweeps.csv") if (WORK / "sweeps.csv").exists() else pd.DataFrame()
    iters = pd.read_csv(WORK / "iteration_log.csv")
    trials = pd.read_csv(WORK / "trials.csv") if (WORK / "trials.csv").exists() else pd.DataFrame()
    win = summary["windows"]

    # ---------------- POOL_RECREATION_REPORT ------------------------------------
    tr_cov = cov[cov["window"] == "TRAIN"]
    te_cov = cov[cov["window"] == "TEST"]
    pr = [f"# {SETUP} ({SIDE}) — POOL_RECREATION_REPORT", "", HDR, "",
          "## Raw data sources used", "",
          "- **5-minute signal generation:** production clean-pool scanner "
          "(`avwap_5min_ID_v11_backtesting.py --mode historical_all_available`, ab-gate enabled so "
          "A_*/B_* probation setups appear in the raw scan) on data root "
          "`C:\\TradingData\\eqidv2\\stocks_indicators_5min_eq_live2`.",
          "- **1-minute exit realism:** `C:\\TradingData\\eqidv2\\stocks_indicators_1min_eq` "
          "(+ live raw 1-min fallback merge inside `v11._load_1m_with_open`), exits resolved to 15:20 IST.",
          "- **Cost model:** statutory NSE intraday costs (`nse_intraday_costs`) + 15 bps/leg adverse "
          "slippage on entry AND exit (repo default for this book).",
          "- Harvested RAW-candidate segments (cross-source determinism verified on shared dates "
          "by the A_MOD campaigns — the shared scanner emits identical row sets per setup):", ""]
    for k, v in manifest["rows_per_source_prededup"].items():
        pr.append(f"  - `{k}`: {v} rows")
    pr += ["", "## Requested vs actual windows", "",
           f"- requested TRAIN: `{manifest['requested']['TRAIN'][0]} .. {manifest['requested']['TRAIN'][1]}`",
           f"- actual TRAIN: `{manifest['actual']['TRAIN'][0]} .. {manifest['actual']['TRAIN'][1]}` "
           f"(**{manifest['actual']['n_train_sessions']} completed sessions**)",
           f"- requested TEST: `{manifest['requested']['TEST'][0]} .. {manifest['requested']['TEST'][1]}`",
           f"- actual TEST: `{manifest['actual']['TEST'][0]} .. {manifest['actual']['TEST'][1]}` "
           f"(**{manifest['actual']['n_test_sessions']} completed sessions**)",
           "",
           "- 2026-07-02 (today) is EXCLUDED: the 5-min feed is complete but the EOD 1-min sync has not "
           "run, so SL/target exits cannot be simulated realistically for it yet.",
           "- 2026-05-30 / 2026-05-31 are Sat/Sun; last May session is 2026-05-29.",
           f"- weekdays inside the window with NO session data (exchange holiday or no-data): "
           f"`{', '.join(manifest['missing_weekdays_in_window']) or 'none'}`",
           "",
           "## Pool contents", "",
           f"- rows (pre-dedupe basis, cross-source deduped): **{manifest['rows_total']}**",
           f"- symbols: **{manifest['n_symbols']}**",
           f"- TRAIN rows: {int(tr_cov['rows'].sum())} across {len(tr_cov)} sessions "
           f"(median {int(tr_cov['rows'].median())}/session)",
           f"- TEST rows: {int(te_cov['rows'].sum())} across {len(te_cov)} sessions "
           f"(median {int(te_cov['rows'].median())}/session)",
           f"- per-setup pool file: `{manifest['out_csv']}`",
           "", "## Session coverage (per session)", "",
           "| session | window | raw rows | tickers |", "|---|---|---|---|"]
    for _, r in cov.iterrows():
        pr.append(f"| {r['session']} | {r['window']} | {r['rows']} | {r['tickers']} |")
    pr += ["", "## Data quality notes", "",
           "- The pool is RAW candidates (pre-gate) from the production detector — the campaign tunes "
           "the same object the v11/live conf gate would consume.",
           "- Weekdays with no session in any root (holiday/no-data) are listed above.",
           "- Entry attachment drops rows with no next-1-min bar within 3 minutes of the 5-min signal "
           "(same rule as production).",
           f"- Rerun: `py -3.12 Train_and_Test\\setup_pf_1_4_full_loop\\{SETUP}\\scripts\\recreate_pool.py` "
           "(fresh-scan segment: see pools/_fresh_scan.log for the exact scanner command).",
           ]
    (WORK / "POOL_RECREATION_REPORT.md").write_text("\n".join(pr), encoding="utf-8")

    # ---------------- PARAMETER_INVENTORY ---------------------------------------
    bc = summary["baseline_cfg"]
    inv = [f"# {SETUP} ({SIDE}) — PARAMETER_INVENTORY", "", HDR, "",
           "## 1. Current setup rules (config source: " + summary["baseline_src"] + ")", "",
           f"- **setup / side:** {SETUP} / {SIDE}",
           "- **entry trigger (detection, read-only):** a below-VWAP stock reclaims the session VWAP on a strong up-bar in a non-BEAR regime (min quality 6.0): `close>open`, `close_loc>=0.60`, `prev_close<prev_VWAP`, `close>VWAP`, `rs_pct>-0.10`, `vol_ratio>=1.4`, `regime!=BEAR` — reason tag `reclaim_session_vwap_from_below` (candidate_scan.v2._scan_day)",
           f"- **indicator rules (mask):** `{bc['mask_terms']}`",
           "- **non-indicator rules:** entry = next 1-min open after the 5-min signal (max 3-min delay), "
           "SHORT fill with adverse slippage; one trade per ticker per day after family dedupe.",
           f"- **pre-momentum rules (gate, ALL required, missing->block):** `{bc['pre_momentum_terms']}`",
           f"- **filters:** mask terms above (vol_ratio conviction filter).",
           f"- **guards:** `{bc['entry_guards'] or '{}'}`",
           f"- **SL / target:** {bc['exit']['sl_pct']}% / {bc['exit']['tgt_pct']}%",
           "- **exit logic:** first-touch SL/TARGET on 1-min OHLC, else EOD forced exit 15:20 IST.",
           "- **time windows:** none in baseline (scanner emits 09:15..15:00 slots).",
           f"- **portfolio limits:** max_positions {bc.get('max_positions') or 20}, "
           f"daily_loss_rs {bc.get('daily_loss_rs') or 0.0} (0 = off).",
           "", "## 2. Available columns/features in the recreated pool", "",
           "Populated for this setup's raw scanner rows (wide-schema columns the scanner does not "
           "emit for it are empty and are pruned from the search automatically):", "",
           "- **price/OHLC:** signal_open, signal_high, signal_low, signal_close",
           "- **volume:** signal_volume, vol_ratio (bar vs 20-bar avg)",
           "- **VWAP:** vwap_dist_atr (distance from session VWAP in ATRs)",
           "- **volatility:** atr_pct",
           "- **candle structure (derived at load):** body_pct, close_loc, signal_range_pct, "
           "upper_wick_pct, lower_wick_pct, wick_skew_pct",
           "- **relative strength / market:** rs_pct, market_ret_pct, market_abs_ret_pct (banned as "
           "overfit vector), regime (BEAR/NEUTRAL/TREND)",
           "- **scanner quality:** quality_score (ranker_score ~99% empty -> excluded)",
           "- **time/session:** signal_time_ist, signal_minute, scan_slot_ist, _day, _slot",
           "- **symbol:** ticker",
           "- **pre-momentum (computed 1-min/5-min at eval):** pre_entry_momentum_score, sig5_adx_calc, "
           "sig5_rsi_dir, sig5_vol_ratio20, pre1_adx, pre3_range_r, pre5_mom_r, pre3_close_pos",
           "- **NOT available for this setup's raw rows:** EMA/SMA columns, RSI/MACD columns, BB/Keltner, "
           "MFI/OBV/CCI/Stoch/W%R/ROC/Supertrend, pressure_ratio, breakout-geometry columns "
           "(breakout_strength_atr, orh/pdh/prev20 distances) — all empty in the raw scanner schema; "
           "indicator structure enters via the pre-momentum features + quality_score instead.",
           "", "## 3. Supported pipeline knobs (all exercised in this campaign)", "",
           "| knob | supported | search range |", "|---|---|---|",
           "| mask_terms (<=2 numeric + optional regime categorical) | yes | feats above x q0.1..0.9 x >=/<= |",
           "| pre_momentum_terms (<=2) | yes | 8 premom feats x q0.1..0.9 x >=/<= |",
           "| min_slot | yes | 09:30,09:45,10:00,10:30,11:00,12:00 |",
           "| max_slot | yes | 11:30,12:00,12:30,13:00,14:00,14:30 |",
           "| top_n (per slot, by vwap_dist_atr) | yes | 0-3 |",
           "| max_positions | yes | 10, 20 |",
           "| daily_loss_rs kill-switch | yes | 0 (off), 2000, 4000 |",
           "| SL % | yes | 0.50,0.70,0.85,1.00,1.10,1.20,1.50 |",
           "| target % | yes | 0.60,0.80,1.00,1.25,1.50,2.00,2.50 |",
           "| EOD forced exit | fixed 15:20 IST | (production convention) |",
           "| trailing SL / break-even / time-exit | NOT supported by repo resolver | not searched |",
           "| regime_align overlay | supported (book-level) | regime mask term searched instead |",
           "| max trades/day / per symbol | via family dedupe (1/ticker/day) + top_n + max_positions | — |",
           "", "## 4. Why these ranges are realistic", "",
           "- Thresholds come from TRAIN-only quantiles (q0.1..q0.9) — no hand-picked magic numbers, "
           "no TEST leakage.",
           "- Exit grid spans tight-scalp (0.5/0.6) to wide-runner (1.5/2.5), covering all four "
           "SL-x-target quadrants around the production 1.10/1.00.",
           "- max 2 mask + 2 premom terms + 1 categorical keeps configs explainable and audit-able "
           "(the historical overfit failures in this repo all came from >=3-term 6-decimal gates).",
           "- market_ret_pct / signal_minute / notional are EXCLUDED as mask features (documented "
           "dominant overfit vectors in setup_train_test.py); time-of-day is expressed via the "
           "coarse min_slot/max_slot guards instead."]
    (WORK / "PARAMETER_INVENTORY.md").write_text("\n".join(inv), encoding="utf-8")

    # ---------------- BASELINE_RESULT -------------------------------------------
    mB = summary["baseline_metrics"]
    bl = [f"# {SETUP} ({SIDE}) — BASELINE_RESULT", "", HDR, "",
          f"- **Config source:** {summary['baseline_src']}",
          f"- **Baseline exit:** SL {bc['exit']['sl_pct']}% / Tgt {bc['exit']['tgt_pct']}%",
          f"- **Baseline mask_terms:** `{bc['mask_terms']}`",
          f"- **Baseline pre_momentum_terms:** `{bc['pre_momentum_terms']}`",
          f"- **Baseline entry_guards:** `{bc['entry_guards'] or '{}'}`",
          "- **Exit model:** resolve on 1-min OHLC to 15:20 IST (TARGET/SL/EOD); statutory NSE costs; "
          "entry = next 1-min open after the 5-min signal + 15 bps/leg slippage.", "",
          "## Sessions (exact)", "",
          f"- **FIT** {win['FIT']} ({len(win['FIT_sessions'])} sessions — first 60% of TRAIN)",
          f"- **VAL** {win['VAL']} ({len(win['VAL_sessions'])} sessions — last 40% of TRAIN)",
          f"- **TRAIN** {win['TRAIN']} ({win['n_train_sessions']} sessions)",
          f"- **TEST** {win['TEST']} ({win['n_test_sessions']} sessions): "
          f"{', '.join(win['TEST_sessions'])}", ""]
    for lbl in ("FIT", "VAL", "TRAIN", "TEST"):
        bl += [f"## Baseline {lbl} metrics (@15 bps/leg, statutory costs)", "",
               "| metric | value |", "|---|---|", _mtable(mB[lbl]), ""]
    tr, te = mB["TRAIN"], mB["TEST"]
    bl += ["## Initial diagnosis", "",
           f"- Baseline TRAIN PF {tr['net_pf']} (n={tr['n']}) / TEST PF {te['net_pf']} (n={te['n']}) "
           f"vs goal TRAIN [1.30,1.80] / TEST >1.40.",
           f"- Baseline FIT PF {mB['FIT']['net_pf']} vs VAL PF {mB['VAL']['net_pf']} — "
           + ("stable halves." if abs(mB['FIT']['net_pf'] - mB['VAL']['net_pf']) < 0.5 else
              "large FIT/VAL gap — the conf thresholds do not generalise inside TRAIN itself."),
           f"- Exit mix TRAIN SL/TGT/EOD = {tr['sl_cnt']}/{tr['tgt_cnt']}/{tr['eod_cnt']}; "
           f"avgW/avgL = Rs{tr['avg_win']:,.0f}/Rs{tr['avg_loss']:,.0f}.",
           "- See FAILURE_ANALYSIS.md for loser classification."]
    (WORK / "BASELINE_RESULT.md").write_text("\n".join(bl), encoding="utf-8")

    # ---------------- PARAMETER_SWEEP_SUMMARY ------------------------------------
    ps = [f"# {SETUP} ({SIDE}) — PARAMETER_SWEEP_SUMMARY", "", HDR, "",
          f"Stage-3 one-knob-at-a-time sweeps from the baseline config, scored on FIT+VAL with the "
          f"band objective (tent at PF {1.80}, gap penalty 0.80). "
          f"Baseline FIT/VAL band score is the reference; `improve` = higher score.", ""]
    if not sweeps.empty:
        ps += [f"Total sweeps: **{len(sweeps)}** | improve: {int((sweeps['vs_baseline']=='improve').sum())} "
               f"| worse: {int((sweeps['vs_baseline']=='worse').sum())}", ""]
        for grp, g in sweeps.groupby("group"):
            ps += [f"## {grp}", "",
                   "| knob | old | new | FIT n/PF | VAL n/PF | score | vs baseline |",
                   "|---|---|---|---|---|---|---|"]
            g = g.sort_values("score", ascending=False)
            for _, r in g.iterrows():
                ps.append(f"| {r['knob']} | {r['old']} | {r['new']} | {r['fit_n']}/{r['fit_pf']} | "
                          f"{r['val_n']}/{r['val_pf']} | {r['score']} | {r['vs_baseline']} |")
            ps.append("")
        ps += ["## Best stable knobs (score-improving, FIT and VAL both alive)", ""]
        good = sweeps[(sweeps["vs_baseline"] == "improve")].sort_values("score", ascending=False).head(15)
        for _, r in good.iterrows():
            ps.append(f"- **{r['group']} / {r['knob']}** -> {r['new']} "
                      f"(FIT {r['fit_n']}/{r['fit_pf']}, VAL {r['val_n']}/{r['val_pf']}, score {r['score']})")
        ps += ["", "## Overfit-risk notes", "",
               "- Any knob whose FIT PF explodes while VAL PF collapses is a knife-edge; the band "
               "objective already penalises the gap, and stage-5 adds neighborhood + dropout checks.",
               "- Sweeps that push PF far above 1.80 are treated as overshoot, not success."]
    else:
        ps.append("_sweeps skipped_")
    (WORK / "PARAMETER_SWEEP_SUMMARY.md").write_text("\n".join(ps), encoding="utf-8")

    # ---------------- ITERATION_LOG ----------------------------------------------
    il = [f"# {SETUP} ({SIDE}) — ITERATION_LOG", "", HDR, "",
          f"Optimizer: {summary['optimizer']}. Protocol: search ONLY on FIT/VAL "
          f"(band objective, tent at PF 1.80, gap penalty); confirm on full TRAIN; TEST scored ONCE per "
          f"finalist whose TRAIN lands in [1.30,1.80]; TEST evaluations budget-capped "
          f"({summary['n_test_evals']} used).", "",
          f"- Stage 1 baseline: 1 iteration",
          f"- Stage 3 single-knob sweeps: {summary['n_sweeps']} iterations (see PARAMETER_SWEEP_SUMMARY.md)",
          f"- Stage 4 combination search: {summary['n_trials']} trials "
          f"({summary['n_unique_configs']} unique configs; full list in trials.csv)",
          f"- Stage 5/6 finalist + rescue confirmations: "
          f"{len(iters[iters['stage'].str.startswith(('5', '6'))])} iterations", "",
          "## Full per-iteration log (baseline, sweeps, finalists, rescues)", "",
          "Complete row-level log: `iteration_log.csv` (every iteration: stage, group, change, old/new, "
          "FIT/VAL/TRAIN/TEST metrics, exit counts, keep/reject + why + next action). "
          "Key iterations below.", "",
          "| # | stage | group | change | old -> new | SL/Tgt | FIT n/PF | VAL n/PF | TRAIN n/PF | TEST n/PF | keep | why |",
          "|---|---|---|---|---|---|---|---|---|---|---|---|"]

    def _fmt(v):
        return "-" if v is None or (isinstance(v, float) and pd.isna(v)) else v
    show = iters[(iters["stage"] != "3-sweep") | (iters["keep"] == "improve")]
    for _, r in show.iterrows():
        il.append(f"| {r['iter']} | {r['stage']} | {r['group']} | {r['change']} | "
                  f"{_fmt(r['old'])} -> {_fmt(r['new'])} | {r['sl']}/{r['tgt']} | "
                  f"{_fmt(r['fit_n'])}/{_fmt(r['fit_pf'])} | {_fmt(r['val_n'])}/{_fmt(r['val_pf'])} | "
                  f"{_fmt(r['train_n'])}/{_fmt(r['train_pf'])} | {_fmt(r['test_n'])}/{_fmt(r['test_pf'])} | "
                  f"{r['keep']} | {str(r['why'])[:90]} |")
    if not trials.empty:
        il += ["", "## Top 40 stage-4 trials (by FIT/VAL band score)", "",
               "| # | SL | Tgt | mask | premom | guard | FIT n/PF | VAL n/PF | score |",
               "|---|----|----|------|--------|-------|----------|----------|-------|"]
        for i, (_, r) in enumerate(trials.head(40).iterrows(), 1):
            il.append(f"| {i} | {r['sl']} | {r['tgt']} | {r['mask']} | {r['premom']} | {r['guard']} | "
                      f"{r['fit_n']}/{r['fit_pf']} | {r['val_n']}/{r['val_pf']} | {r['score']} |")
    (WORK / "ITERATION_LOG.md").write_text("\n".join(il), encoding="utf-8")

    # ---------------- FAILURE_ANALYSIS -------------------------------------------
    fa = [f"# {SETUP} ({SIDE}) — FAILURE_ANALYSIS", "", HDR, ""]
    bt = WORK / "baseline_trades_train.csv"
    if bt.exists():
        det = pd.read_csv(bt)
        det["hh"] = pd.to_datetime(det["signal_time"]).dt.hour
        losers = det[det["net_pnl_rs"] < 0]
        fa += ["## Baseline TRAIN book — loser classification", "",
               f"- trades {len(det)}, losers {len(losers)} "
               f"({round(len(losers)/max(1,len(det))*100,1)}%)",
               f"- outcome mix: {det['outcome'].value_counts().to_dict()}",
               f"- loser outcome mix: {losers['outcome'].value_counts().to_dict()}",
               f"- avg bars held (win/lose): "
               f"{round(det[det['net_pnl_rs']>0]['bars_held'].mean(),1)} / {round(losers['bars_held'].mean(),1)}",
               "", "### Net PnL by signal hour (baseline TRAIN)", "",
               "| hour | n | net Rs | PF proxy |", "|---|---|---|---|"]
        for hh, g in det.groupby("hh"):
            gp = g[g["net_pnl_rs"] > 0]["net_pnl_rs"].sum()
            gl = -g[g["net_pnl_rs"] < 0]["net_pnl_rs"].sum()
            fa.append(f"| {hh}:00 | {len(g)} | {g['net_pnl_rs'].sum():,.0f} | "
                      f"{round(gp/gl,2) if gl>0 else 'inf'} |")
        wd = det.groupby("trade_date")["net_pnl_rs"].sum().sort_values()
        ws = det.groupby("ticker")["net_pnl_rs"].sum().sort_values()
        fa += ["", "### Worst days (baseline TRAIN)", ""] + \
              [f"- {d}: Rs{v:,.0f}" for d, v in wd.head(5).items()] + \
              ["", "### Worst symbols (baseline TRAIN)", ""] + \
              [f"- {s}: Rs{v:,.0f}" for s, v in ws.head(5).items()] + \
              ["", "### Worst trades (baseline TRAIN)", "",
               "| date | ticker | outcome | bars | net Rs |", "|---|---|---|---|---|"]
        for _, r in det.nsmallest(8, "net_pnl_rs").iterrows():
            fa.append(f"| {r['trade_date']} | {r['ticker']} | {r['outcome']} | {r['bars_held']} | "
                      f"{r['net_pnl_rs']:,.0f} |")
    fa += ["", "## Why rejected candidates failed (from the loop)", ""]
    for r in summary["results"]:
        if not r.get("passed"):
            tagline = "; ".join(r.get("hard_reasons", [])) or "-"
            trm = r.get("train")
            tem = r.get("test")
            fa.append(f"- **{r.get('tag', 'finalist #%s' % r['id'])}** — TRAIN "
                      f"{'PF %s n=%s' % (trm['net_pf'], trm['n']) if trm else '-'}"
                      + (f", TEST PF {tem['net_pf']} n={tem['n']}" if tem else "")
                      + f" -> {tagline}")
    fa += ["", "## Structural notes", "",
           "- Pre-momentum issues, indicator weakness, filter/guard weakness and volume/volatility/trend "
           "issues are quantified knob-by-knob in PARAMETER_SWEEP_SUMMARY.md (every knob's relaxed/medium/"
           "strict variants with FIT/VAL outcomes).",
           "- Fake-breakdown avoidance shows up in the wick/close_loc sweeps; time-of-day weakness in the "
           "min_slot/max_slot sweeps; exhaustion in the pre5_mom_r/pre3_range_r premom sweeps."]
    (WORK / "FAILURE_ANALYSIS.md").write_text("\n".join(fa), encoding="utf-8")

    # ---------------- CANDIDATE_CONFIGS + candidates/*.json ----------------------
    passing = [r for r in summary["results"] if r.get("passed")]
    (WORK / "candidates").mkdir(exist_ok=True)
    if passing:
        cc = [f"# {SETUP} ({SIDE}) — CANDIDATE_CONFIGS (passed all gates)", "", HDR, ""]
        for i, r in enumerate(passing, 1):
            cid = f"{SETUP}_candidate_{i:03d}"
            cc += [f"## Candidate {i:03d}", "", "```json", json.dumps(r["cfg"], indent=2), "```", "",
                   f"- TRAIN: {_m(r['train'])}", f"- TEST:  {_m(r['test'])}",
                   f"- robustness: neighbor={r['robust']['neighbor_pass']} dropout={r['robust']['dropout_pass']}",
                   f"- domination: TRAIN trade/day/sym = {r['train']['trade_dom_gross']}/"
                   f"{r['train']['day_dom']}/{r['train']['sym_dom']} | TEST = "
                   f"{r['test']['trade_dom_gross']}/{r['test']['day_dom']}/{r['test']['sym_dom']} "
                   f"(caps 0.35/0.40/0.40)",
                   f"- warnings: {'; '.join(r.get('warnings', [])) or 'none'}",
                   "- Recommendation: **APPROVAL REQUIRED** (do not auto-promote).", ""]
            (WORK / "candidates" / f"{cid}.json").write_text(
                json.dumps({"setup": SETUP, "side": SIDE, "verdict": "APPROVAL_REQUIRED",
                            "config": r["cfg"], "train": r["train"], "test": r["test"],
                            "robust": r["robust"], "warnings": r.get("warnings", []),
                            "windows": {"TRAIN": win["TRAIN"], "TEST": win["TEST"]}},
                           indent=2, default=str), encoding="utf-8")
        (WORK / "CANDIDATE_CONFIGS.md").write_text("\n".join(cc), encoding="utf-8")
    else:
        (WORK / "CANDIDATE_CONFIGS.md").write_text(
            f"# {SETUP} ({SIDE}) — CANDIDATE_CONFIGS\n\n{HDR}\n\n"
            f"**No candidate cleared the full gate** (TRAIN PF in [1.30,1.80], TEST PF > 1.40, positive "
            f"net both windows, meaningful trades, domination caps 0.35/0.40/0.40, day-block p <= 0.10, "
            f"neighborhood + dropout robustness).\n\nSee ITERATION_LOG.md and FAILURE_ANALYSIS.md for "
            f"why every attempt failed.\n", encoding="utf-8")

    # ---------------- APPROVAL_REQUIRED_FINAL_RECOMMENDATION ---------------------
    rec_yes = bool(passing)
    bestr = passing[0] if passing else None
    ar = [f"# {SETUP} ({SIDE}) — APPROVAL_REQUIRED / FINAL RECOMMENDATION", "", HDR, "",
          f"## Approval recommendation: **{'YES — APPROVAL REQUIRED' if rec_yes else 'NO'}**", ""]
    if bestr:
        ar += ["## Best candidate (proposed, NOT promoted)", "",
               "```json", json.dumps(bestr["cfg"], indent=2), "```", "",
               f"- TRAIN: {_m(bestr['train'])}",
               f"- TEST:  {_m(bestr['test'])}", "",
               "## File that would need approval before edit", "",
               "- `final_setup_conf.py` (repo root) — replace the `" + SETUP + "` entry "
               "(exit + mask_terms + pre_momentum_terms + entry_guards) with the block above; then mirror "
               "to `Train_and_Test/final_setup_conf.py` per repo convention.", "",
               WARNING, ""]
    else:
        ar += ["## No promotion proposed", "",
               "- No config cleared the robust TRAIN+TEST gate on the recreated Mar-Jul pool. "
               "The existing conf entry stays as-is; nothing is edited.", "", WARNING, ""]
    ar += ["## Rerun commands", "", "```",
           f"# pool recreation (fresh-scan segment already on disk; see pools/_fresh_scan.log)",
           f"py -3.12 Train_and_Test\\setup_pf_1_4_full_loop\\{SETUP}\\scripts\\recreate_pool.py",
           f"# baseline + sweeps + search + finalists + rescue",
           f"py -3.12 Train_and_Test\\setup_pf_1_4_full_loop\\{SETUP}\\scripts\\run_full_loop.py "
           f"--trials 500 --time_budget_min 60 --seed 7",
           f"# reports",
           f"py -3.12 Train_and_Test\\setup_pf_1_4_full_loop\\{SETUP}\\scripts\\write_reports.py",
           "```", "",
           "## Remaining risks", "",
           f"- TEST = {win['n_test_sessions']} June/July sessions; June was a poor month for many "
           "of this book's setups — a single-month OOS is still one market regime.",
           "- RAW-pool basis: live fires this setup through the conf gate, but v8/research-layer "
           "differences remain a live-parity risk (watch live/paper before sizing).",
           "- Domination caps used: trade<=35% gross, day<=40% net, symbol<=40% net.",
           "- No trailing/break-even exits: resolver supports fixed SL/TGT + EOD only."]
    (WORK / "APPROVAL_REQUIRED_FINAL_RECOMMENDATION.md").write_text("\n".join(ar), encoding="utf-8")

    print(f"[reports] wrote 8 campaign reports under {WORK}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
