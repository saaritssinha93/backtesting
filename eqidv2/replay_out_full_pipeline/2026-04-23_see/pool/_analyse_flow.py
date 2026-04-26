import json, csv, collections

# -------- 1. Lifecycle events per signal_id --------
by_sid = collections.defaultdict(dict)
with open("pool_lifecycle_2026-04-23_v16_5min.jsonl") as f:
    for line in f:
        d = json.loads(line)
        by_sid[d["signal_id"]][d["event"]] = d

# -------- 2. DE CSVs (indexed by signal_id) --------
def load_csv(p):
    with open(p, newline="") as f:
        return list(csv.DictReader(f))
long_sigs  = load_csv("signals_2026-04-23_v16_5min_long.csv")
short_sigs = load_csv("signals_2026-04-23_v16_5min_short.csv")
detected   = load_csv("detected_signals_2026-04-23_v16_5min.csv")
de_rows    = {r["signal_id"]: r for r in long_sigs + short_sigs}
det_rows   = {r["signal_id"]: r for r in detected}

# -------- 3. Slot timeline --------
with open("../replay_slot_timeline.csv") as f:
    slot_rows = list(csv.DictReader(f))

# -------- 4. Per-signal summary --------
def _hhmm(s):
    if not s: return ""
    try:
        if "T" in s:
            return s.split("T")[-1].split("+")[0][:8]
        return s.split(" ")[1].split("+")[0][:8]
    except Exception:
        return s[:16]

def _entry_plus(entry_iso, secs):
    # Return HH:MM:SS for entry_slot + secs (live-expected PF/DE fire time).
    if not entry_iso:
        return ""
    try:
        ts = pd.Timestamp(entry_iso) if False else None
    except Exception:
        return ""
    # Parse without pandas — entry_iso is 'YYYY-MM-DD HH:MM:SS+TZ' or ISO.
    import datetime as _dt
    s = entry_iso.replace("T", " ")
    # Strip TZ suffix
    for tz in ("+05:30", "+0530"):
        if s.endswith(tz):
            s = s[: -len(tz)]
            break
    try:
        dt = _dt.datetime.strptime(s.strip(), "%Y-%m-%d %H:%M:%S")
    except Exception:
        return ""
    dt = dt + _dt.timedelta(seconds=secs)
    return dt.strftime("%H:%M:%S")

summary = []
for sid, evs in by_sid.items():
    w  = evs.get("WRITTEN", {})
    pf = evs.get("PF_VERIFIED", {})
    de = evs.get("DE_PASSED", {})
    dr = evs.get("DROPPED", {})
    desig = de_rows.get(sid, {})
    det   = det_rows.get(sid, {})
    outcome = "DE_PASSED" if de else ("DROPPED" if dr else "?")
    entry_iso = w.get("signal_entry_datetime_ist", "")
    summary.append({
        "ticker": w.get("ticker",""),
        "side":   w.get("side",""),
        "setup":  w.get("setup",""),
        "trig_slot":   _hhmm(w.get("trigger_bar_iso","")),
        "entry_slot":  _hhmm(entry_iso),
        "see_written":   _hhmm(w.get("ts","")),
        "pf_rep":        _hhmm(pf.get("ts","")) if pf else "",
        "de_rep":        _hhmm(de.get("ts","")) if de else "",
        "pf_live":       _entry_plus(entry_iso, 2) if pf else "",
        "de_live":       _entry_plus(entry_iso, 4) if de else "",
        "dropped":       _hhmm(dr.get("ts","")) if dr else "",
        "drop_reason": dr.get("reason","") if dr else "",
        "entry_px":  desig.get("entry_price","")  or det.get("entry_price",""),
        "stop_px":   desig.get("stop_price","")   or det.get("stop_price",""),
        "target_px": desig.get("target_price","") or det.get("target_price",""),
        "lag_sig_bar":    det.get("lag_from_signal_bar_sec",""),
        "lag_entry_slot": det.get("lag_from_entry_slot_sec",""),
        "outcome": outcome,
    })

summary.sort(key=lambda r: (0 if r["outcome"]=="DE_PASSED" else 1, r["entry_slot"], r["ticker"]))

out = []
push = out.append

push("="*170)
push("V16 5min SEE -> PF -> DE  Full Pipeline Replay  --  Per-Signal Flow")
push("Date: 2026-04-23   Slots: 76  (09:15 -> 15:30)   Universe: 1044   Engine: SEE (early-emit)")
push("="*170)
push("Column key (per strategy_v2.txt §1 — T = trigger-bar CLOSE, Y = entry-bar OPEN = T+lag*5min):")
push("  Trig       = trigger_bar_iso (= T, bar CLOSE wall-clock; trigger bar spans [T-5min, T])")
push("  Entry      = signal_entry_datetime_ist (= Y, entry bar OPEN wall-clock = source_slot)")
push("  SEE->Pool  = wall ts when SEE wrote the candidate (T + 45s)")
push("  PF_rep     = REPLAY ts when PF_VERIFIED event fired. Replay driver fires PF at")
push("               trigger_slot+47s (SAME slot as SEE), NOT at entry_slot+2s. This is a")
push("               replay artifact — LIVE fires PF at Y+2s.")
push("  DE_rep     = REPLAY ts when DE_PASSED event fired. Same caveat: DE fires at")
push("               trigger_slot+49s in replay, NOT at Y+4s as in LIVE.")
push("  PF_live*   = LIVE-expected PF fire time (= Y+2s = entry_slot+2s). Derived, not observed.")
push("  DE_live*   = LIVE-expected DE fire time (= Y+4s = entry_slot+4s). Derived, not observed.")
push("  Drop       = wall ts when DE dropped it (reason e.g. expired_window_parity)")
push("  Lag        = lag_from_signal_bar_sec from detected_signals CSV (DE fire minus signal-bar close)")
push("")

hdr = (f"{'Ticker':<14} {'Side':<6} {'Setup':<36} {'Trig':<9} {'Entry':<9} "
       f"{'SEE->Pool':<10} {'PF_rep':<10} {'DE_rep':<10} {'PF_live*':<10} {'DE_live*':<10} {'Drop':<10} "
       f"{'Reason':<22} {'Entry$':>9} {'Stop$':>9} {'Target$':>9} {'Lag(s)':>7}")
push(hdr)
push("-"*170)

cur = None
for r in summary:
    if r["outcome"] != cur:
        push("")
        push(f"===== OUTCOME = {r['outcome']} =====")
        cur = r["outcome"]
    push(f"{r['ticker']:<14} {r['side']:<6} {r['setup']:<36} "
         f"{r['trig_slot']:<9} {r['entry_slot']:<9} "
         f"{r['see_written']:<10} {r['pf_rep']:<10} {r['de_rep']:<10} "
         f"{r['pf_live']:<10} {r['de_live']:<10} {r['dropped']:<10} "
         f"{r['drop_reason']:<22} "
         f"{r['entry_px']:>9} {r['stop_px']:>9} {r['target_px']:>9} {r['lag_sig_bar']:>7}")

# Per-ticker roll-up
push("")
push("="*150)
push("PER-TICKER ROLL-UP")
push("="*150)
by_tic = collections.defaultdict(list)
for r in summary:
    by_tic[r["ticker"]].append(r)
push(f"{'Ticker':<14} {'Pass':>5} {'Drop':>5}  Setups (pass)                                              Setups (drop)")
for tic in sorted(by_tic):
    rs = by_tic[tic]
    p = [x for x in rs if x["outcome"]=="DE_PASSED"]
    d = [x for x in rs if x["outcome"]=="DROPPED"]
    pset = ", ".join(f"{x['side'][0]}/{x['setup']}" for x in p) or "-"
    dset = ", ".join(f"{x['side'][0]}/{x['setup']}" for x in d) or "-"
    push(f"{tic:<14} {len(p):>5} {len(d):>5}  {pset:<56}  {dset}")

# Setup / Side / Hour
push("")
push("="*150)
push("DISTRIBUTION -- SETUP x OUTCOME")
push("="*150)
by_setup = collections.defaultdict(lambda: {"pass":0,"drop":0})
for r in summary:
    by_setup[r["setup"]]["pass" if r["outcome"]=="DE_PASSED" else "drop"] += 1
push(f"{'Setup':<40} {'Pass':>6} {'Drop':>6}  {'Total':>6}  PassRate")
for s in sorted(by_setup, key=lambda k: -(by_setup[k]["pass"]+by_setup[k]["drop"])):
    t = by_setup[s]["pass"]+by_setup[s]["drop"]
    rate = f"{100*by_setup[s]['pass']/t:.1f}%" if t else "n/a"
    push(f"{s:<40} {by_setup[s]['pass']:>6} {by_setup[s]['drop']:>6}  {t:>6}  {rate}")

push("")
push("DISTRIBUTION -- SIDE x OUTCOME")
by_side = collections.defaultdict(lambda: {"pass":0,"drop":0})
for r in summary:
    by_side[r["side"]]["pass" if r["outcome"]=="DE_PASSED" else "drop"] += 1
for s in by_side:
    push(f"  {s:<6} pass={by_side[s]['pass']:>3} drop={by_side[s]['drop']:>3}")

push("")
push("DISTRIBUTION -- ENTRY-SLOT HOUR x OUTCOME")
by_hr = collections.defaultdict(lambda: {"pass":0,"drop":0})
for r in summary:
    hr = r["entry_slot"][:2]
    by_hr[hr]["pass" if r["outcome"]=="DE_PASSED" else "drop"] += 1
for h in sorted(by_hr):
    push(f"  {h}:xx  pass={by_hr[h]['pass']:>3} drop={by_hr[h]['drop']:>3}")

# Lag stats
push("")
push("="*150)
push("DETECTION LAG STATS (DE-passed signals)")
push("="*150)
lag_s = [int(r["lag_sig_bar"]) for r in summary if r["outcome"]=="DE_PASSED" and r["lag_sig_bar"]]
lag_e = [int(r["lag_entry_slot"]) for r in summary if r["outcome"]=="DE_PASSED" and r["lag_entry_slot"]]
def stat(xs):
    if not xs: return "n/a"
    xs = sorted(xs)
    return f"n={len(xs)} min={xs[0]} p50={xs[len(xs)//2]} mean={sum(xs)//len(xs)} max={xs[-1]}"
push(f"lag_from_signal_bar_sec : {stat(lag_s)}")
push(f"lag_from_entry_slot_sec : {stat(lag_e)}   (negative = DE fired BEFORE entry-bar open, i.e. early)")

# Slot timeline (non-zero)
push("")
push("="*150)
push("SLOT TIMELINE (non-zero activity only)")
push("="*150)
push(f"{'Slot':<6} {'SEE_S':>5} {'SEE_L':>5} {'Add':>4} {'Pool':>5} {'PF_pend':>8} {'PF_rdy':>7} {'Mrk':<5} {'DE':<12} {'L+':>3} {'S+':>3} {'elap':>6}")
for r in slot_rows:
    nz = any(int(r[k])>0 for k in ("raw_short","raw_long","pool_total_after","de_long_csv_rows","de_short_csv_rows"))
    if not nz: continue
    push(f"{r['slot']:<6} {r['raw_short']:>5} {r['raw_long']:>5} {r['pool_added']:>4} "
         f"{r['pool_total_after']:>5} {r['pf_pending_tickers']:>8} {r['pf_ready_tickers']:>7} "
         f"{r['pf_marker_written']:<5} {r['de_cycle_result']:<12} {r['de_long_csv_rows']:>3} {r['de_short_csv_rows']:>3} {r['elapsed_sec']:>6}")

text = "\n".join(out)
with open("per_ticker_flow.txt","w",encoding="utf-8") as f:
    f.write(text)
print(f"Wrote per_ticker_flow.txt  ({len(text.splitlines())} lines, {len(text)} bytes)")
