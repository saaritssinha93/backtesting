"""
OR Close Position Filter test for LONG trades.
Loads Run13 trades CSV, looks up OR bar for each LONG trade,
applies thresholds and reports metric impact.

Filter: reject LONG if (OR_close - OR_low) / (OR_high - OR_low) < threshold
Also tests entry-bar volume exhaustion filter.
"""
import pandas as pd
import numpy as np
from pathlib import Path

OUT = open(Path(__file__).parent / "_or_close_filter_out.txt", "w", encoding="utf-8")
def p(*a, **k): k["file"]=OUT; import builtins; builtins.print(*a,**k); OUT.flush()

TRADES_CSV = Path(r"C:\TradingData\eqidv2\outputs_v16_5min\avwap_longshort_trades_v16_5min_ALL_DAYS_20260406_170217.csv")
DATA_5M    = Path(r"C:\TradingData\eqidv2\stocks_indicators_5min_eq")
MARKET_DAYS = 69

def pf(s):
    pos = float(s[s>0].sum()); neg = float(-s[s<0].sum())
    return pos/neg if neg>0 else (999 if pos>0 else 0)

def dd(s):
    if s.empty: return 0.0
    eq = s.cumsum(); return float((eq.cummax()-eq).max())

def metrics(df, label=""):
    if df.empty:
        return {"label":label,"n":0,"tpd":0,"pf":0,"pnl":0,"win":0,"dwin":0,"dd":0}
    pnl  = df["pnl_pct"].astype(float)
    dpnl = df.groupby("trade_date")["pnl_pct"].sum()
    n    = len(df)
    return {
        "label"  : label,
        "n"      : n,
        "tpd"    : round(n / MARKET_DAYS, 2),
        "pf"     : round(pf(pnl), 3),
        "pnl"    : round(pnl.sum(), 2),
        "win"    : round((pnl > 0).mean() * 100, 1),
        "dwin"   : round((dpnl > 0).mean() * 100, 1),
        "dd"     : round(dd(pnl), 2),
        "tgt_pct": round((df["outcome"].str.upper()=="TARGET").mean()*100, 1),
        "sl_pct" : round((df["outcome"].str.upper()=="SL").mean()*100, 1),
    }

def fmt_row(m):
    return (f"  {m['label']:<35s} N={m['n']:>4d}  TPD={m['tpd']:.2f}  PF={m['pf']:.3f}  "
            f"PnL={m['pnl']:>7.1f}%  Win={m['win']:.1f}%  DayWin={m['dwin']:.1f}%  "
            f"MaxDD={m['dd']:.2f}%  TGT={m.get('tgt_pct',0):.1f}%  SL={m.get('sl_pct',0):.1f}%")

# ── Load trades ──────────────────────────────────────────────────────────────
p("Loading Run 13 trades...")
all_trades = pd.read_csv(TRADES_CSV)
shorts  = all_trades[all_trades["side"].str.upper() == "SHORT"].copy()
longs   = all_trades[all_trades["side"].str.upper() == "LONG"].copy()
p(f"Total={len(all_trades)}  SHORT={len(shorts)}  LONG={len(longs)}")

# ── Compute OR bar metrics for every LONG trade ───────────────────────────
p("\nComputing OR bar data for all LONG trades...")

_5m_cache = {}

def get_5m(ticker):
    if ticker not in _5m_cache:
        f = DATA_5M / f"{ticker}_stocks_indicators_5min.parquet"
        if f.exists():
            df = pd.read_parquet(f)
            df["date"] = pd.to_datetime(df["date"])
            _5m_cache[ticker] = df
        else:
            _5m_cache[ticker] = pd.DataFrame()
    return _5m_cache[ticker]

def or_bar_data(ticker, trade_date_str):
    """Return dict with OR bar metrics for given ticker+date."""
    df = get_5m(ticker)
    if df.empty:
        return None
    day = df[df["date"].dt.strftime("%Y-%m-%d") == trade_date_str]
    if day.empty:
        return None
    or_bar = day.iloc[0]
    o, h, l, c, v = float(or_bar["open"]), float(or_bar["high"]), float(or_bar["low"]), float(or_bar["close"]), float(or_bar["volume"])
    rng = h - l
    close_pos = (c - l) / rng if rng > 0 else 0.5
    wick_pct  = (h - c) / rng if rng > 0 else 0.0   # upper wick as fraction of range
    return {
        "or_open": o, "or_high": h, "or_low": l, "or_close": c, "or_vol": v,
        "or_range": rng, "or_close_pos": close_pos, "or_wick_pct": wick_pct,
        "or_range_pct": rng / o * 100,
        "entry_above_or_high_pct": None,  # filled below
    }

# ── Compute entry_bar volume for exhaustion filter ────────────────────────
def entry_bar_vol_ratio(ticker, trade_date_str, entry_px):
    df = get_5m(ticker)
    if df.empty:
        return None, None
    day = df[df["date"].dt.strftime("%Y-%m-%d") == trade_date_str].reset_index(drop=True)
    if day.empty:
        return None, None
    avg_vol = day["volume"].mean()
    hits = day[day["high"] >= entry_px * 0.999]
    if hits.empty:
        return None, None
    entry_bar = hits.iloc[0]
    entry_idx = hits.index[0]
    entry_vol = float(entry_bar["volume"])
    ratio = entry_vol / avg_vol if avg_vol > 0 else 0
    return ratio, int(entry_idx)

# Pre-compute all OR data for LONG trades
p("Pre-computing OR bar metrics for all LONG trades (loading parquet files)...")
longs = longs.copy()
or_close_pos_list   = []
or_wick_pct_list    = []
or_range_pct_list   = []
entry_above_or_list = []
entry_vol_ratio_list= []
bars_from_open_list = []

total = len(longs)
for i, (_, row) in enumerate(longs.iterrows()):
    if i % 50 == 0:
        p(f"  {i}/{total}...")
    ticker = str(row["ticker"])
    date_s = str(row["trade_date"])
    entry  = float(row["entry_price"])

    ob = or_bar_data(ticker, date_s)
    if ob is None:
        or_close_pos_list.append(np.nan)
        or_wick_pct_list.append(np.nan)
        or_range_pct_list.append(np.nan)
        entry_above_or_list.append(np.nan)
    else:
        or_close_pos_list.append(ob["or_close_pos"])
        or_wick_pct_list.append(ob["or_wick_pct"])
        or_range_pct_list.append(ob["or_range_pct"])
        above = (entry - ob["or_high"]) / ob["or_high"] * 100
        entry_above_or_list.append(above)

    vr, bidx = entry_bar_vol_ratio(ticker, date_s, entry)
    entry_vol_ratio_list.append(vr if vr is not None else np.nan)
    bars_from_open_list.append(bidx if bidx is not None else np.nan)

longs["or_close_pos"]      = or_close_pos_list
longs["or_wick_pct"]       = or_wick_pct_list
longs["or_range_pct"]      = or_range_pct_list
longs["entry_above_or_pct"]= entry_above_or_list
longs["entry_vol_ratio"]   = entry_vol_ratio_list
longs["bars_from_open"]    = bars_from_open_list

p(f"\nOR data computed. Trades with OR data: {longs['or_close_pos'].notna().sum()}/{len(longs)}")

# ── Distribution analysis ─────────────────────────────────────────────────
p("\n" + "="*90)
p("OR CLOSE POSITION DISTRIBUTION (all 326 LONG trades)")
p("="*90)
valid = longs[longs["or_close_pos"].notna()]
for thresh in [0.10, 0.20, 0.25, 0.30, 0.35, 0.40, 0.50]:
    below = (valid["or_close_pos"] < thresh).sum()
    p(f"  OR close pos < {thresh:.2f}: {below:>4d} trades ({below/len(valid)*100:.1f}%)  "
      f"  wins={( (valid[valid['or_close_pos']<thresh]['pnl_pct'].astype(float)>0).mean()*100 ):.1f}%  "
      f"  vs all-LONG win={(valid['pnl_pct'].astype(float)>0).mean()*100:.1f}%")

p("\n--- OR close pos by outcome ---")
for outcome in ["TARGET","SL","EOD"]:
    sub = valid[valid["outcome"].str.upper()==outcome]
    if not sub.empty:
        p(f"  {outcome}: avg OR close pos = {sub['or_close_pos'].mean():.3f}  "
          f"  median = {sub['or_close_pos'].median():.3f}  "
          f"  <0.25: {(sub['or_close_pos']<0.25).sum()}/{len(sub)} ({(sub['or_close_pos']<0.25).mean()*100:.1f}%)")

# ── Filter 1: OR Close Position thresholds ───────────────────────────────
p("\n" + "="*90)
p("FILTER 1: OR CLOSE POSITION < threshold -> REJECT LONG")
p("="*90)

baseline_long = metrics(longs, "Run13 LONG baseline")
baseline_comb = metrics(pd.concat([shorts, longs]), "Run13 COMBINED baseline")
p("\nBASELINE:")
p(fmt_row(baseline_long))
p(fmt_row(baseline_comb))

p("\nFILTER RESULTS (LONG only — SHORT unchanged):")
results = []
for thresh in [0.10, 0.15, 0.20, 0.25, 0.30, 0.35, 0.40]:
    kept  = longs[(longs["or_close_pos"].isna()) | (longs["or_close_pos"] >= thresh)]
    killed= longs[longs["or_close_pos"] < thresh]
    m_kept = metrics(kept, f"OR close >= {thresh:.2f}")
    comb = pd.concat([shorts, kept])
    m_comb = metrics(comb, f"COMBINED OR close >= {thresh:.2f}")
    killed_wins  = int((killed["pnl_pct"].astype(float) > 0).sum())
    killed_losses= int((killed["pnl_pct"].astype(float) < 0).sum())
    p(f"\n  Threshold {thresh:.2f} | filtered out {len(killed)} LONG trades "
      f"(wins killed={killed_wins}, losses avoided={killed_losses}):")
    p("  LONG: " + fmt_row(m_kept))
    p("  COMB: " + fmt_row(m_comb))
    results.append({"thresh":thresh, "killed":len(killed), "wins_killed":killed_wins,
                    "losses_avoided":killed_losses, **{k+"_long":v for k,v in m_kept.items()},
                    **{k+"_comb":v for k,v in m_comb.items()}})

# ── Filter 2: Entry volume exhaustion ────────────────────────────────────
p("\n" + "="*90)
p("FILTER 2: ENTRY BAR VOLUME > Nx avg AND bars_from_open >= 3 -> REJECT LONG")
p("="*90)

for vol_mult in [3.0, 4.0, 5.0]:
    cond = (
        (longs["entry_vol_ratio"] > vol_mult) &
        (longs["bars_from_open"] >= 3) &
        longs["entry_vol_ratio"].notna()
    )
    killed = longs[cond]
    kept   = longs[~cond]
    mk = metrics(kept, f"Vol < {vol_mult}x (bars>=3)")
    mc = metrics(pd.concat([shorts, kept]), f"COMB Vol < {vol_mult}x")
    kw = int((killed["pnl_pct"].astype(float)>0).sum())
    kl = int((killed["pnl_pct"].astype(float)<0).sum())
    p(f"\n  Vol > {vol_mult}x filter | filtered {len(killed)} trades (wins={kw}, losses={kl}):")
    p("  LONG: " + fmt_row(mk))
    p("  COMB: " + fmt_row(mc))

# ── Filter 1+2 Combined ───────────────────────────────────────────────────
p("\n" + "="*90)
p("FILTER 1 + FILTER 2 COMBINED (best of each)")
p("="*90)

for cp_thresh, vol_mult in [(0.25, 4.0), (0.25, 5.0), (0.30, 4.0)]:
    cond_or  = (longs["or_close_pos"].notna()) & (longs["or_close_pos"] < cp_thresh)
    cond_vol = (longs["entry_vol_ratio"] > vol_mult) & (longs["bars_from_open"] >= 3) & longs["entry_vol_ratio"].notna()
    killed   = longs[cond_or | cond_vol]
    kept     = longs[~(cond_or | cond_vol)]
    mk = metrics(kept, f"OR>={cp_thresh:.2f} + Vol<{vol_mult}x")
    mc = metrics(pd.concat([shorts, kept]), f"COMB OR>={cp_thresh:.2f}+Vol<{vol_mult}x")
    kw = int((killed["pnl_pct"].astype(float)>0).sum())
    kl = int((killed["pnl_pct"].astype(float)<0).sum())
    p(f"\n  OR close >= {cp_thresh:.2f} AND vol <= {vol_mult}x | filtered {len(killed)} (wins={kw}, losses={kl}):")
    p("  LONG: " + fmt_row(mk))
    p("  COMB: " + fmt_row(mc))

# ── Show today's trades by OR close pos ──────────────────────────────────
p("\n" + "="*90)
p("TODAY 2026-04-06 — OR CLOSE POSITION FOR ALL LONG TRADES")
p("="*90)
today_long = longs[longs["trade_date"]=="2026-04-06"][
    ["ticker","pnl_pct","outcome","or_close_pos","or_wick_pct","entry_above_or_pct","entry_vol_ratio","bars_from_open","quality_score"]
].sort_values("or_close_pos")
p(today_long.to_string(index=False))

# ── Top worst OR close pos trades overall ────────────────────────────────
p("\n" + "="*90)
p("WORST 20 OR CLOSE POSITION LONG TRADES (across all 69 days)")
p("="*90)
worst = valid.nsmallest(20, "or_close_pos")[
    ["trade_date","ticker","pnl_pct","outcome","or_close_pos","or_wick_pct","or_range_pct","quality_score"]
]
p(worst.to_string(index=False))
p(f"\nWin rate among worst 20 OR close pos: {(worst['pnl_pct'].astype(float)>0).mean()*100:.1f}%")

p("\n[DONE]")
OUT.close()
print("Output written to _or_close_filter_out.txt")
