"""
Deep study of 3 statistical-variance losses: GESHIP, BAJAJELEC, NFL
Loads 5-min and 1-min data, all indicators, for entry context analysis.
"""
import pandas as pd
import numpy as np
from pathlib import Path

OUT = open(Path(__file__).parent / "_deep3_loss_study_out.txt", "w", encoding="utf-8")

def p(*a, **k):
    k["file"] = OUT
    import builtins; builtins.print(*a, **k)
    OUT.flush()

DATA_5M  = Path(r"C:\TradingData\eqidv2\stocks_indicators_5min_eq")
DATA_1M  = Path(r"C:\TradingData\eqidv2\stocks_indicators_1min_eq")
DATE     = "2026-04-06"

LOSSES = {
    "GESHIP":   {"side":"LONG", "entry":1494.8989, "exit":1485.4837, "sl_pct":0.0075, "qs":7.382, "setup":"A_MOD_BREAK_C1_HIGH"},
    "BAJAJELEC":{"side":"LONG", "entry":352.8206,  "exit":350.5984,  "sl_pct":0.0075, "qs":7.447, "setup":"A_MOD_BREAK_C1_HIGH"},
    "NFL":      {"side":"LONG", "entry":70.20,     "exit":69.7579,   "sl_pct":0.0075, "qs":5.969, "setup":"A_MOD_BREAK_C1_HIGH"},
}

def load(path, ticker, tf):
    f = path / f"{ticker}_stocks_indicators_{tf}.parquet"
    if not f.exists():
        return pd.DataFrame()
    df = pd.read_parquet(f)
    df["date"] = pd.to_datetime(df["date"])
    return df

def day(df, d=DATE):
    if df.empty: return df
    return df[df["date"].dt.strftime("%Y-%m-%d") == d].reset_index(drop=True)

def prev_day(df, d=DATE):
    """Get the previous trading day's data."""
    if df.empty: return df
    tz = df["date"].dt.tz
    cutoff = pd.Timestamp(d, tz=tz) if tz else pd.Timestamp(d)
    norm = df["date"].dt.normalize()
    prev_dates = norm[norm < cutoff].unique()
    if len(prev_dates) == 0: return df.head(0)
    last_prev = prev_dates[-1]
    return df[norm == last_prev].reset_index(drop=True)

def show_bar(df, label, entry_px, side, n_before=5, n_after=10):
    """Find entry bar and show context."""
    if df.empty:
        p(f"  [{label}] no data"); return None, -1

    if side == "LONG":
        hits = df[df["high"] >= entry_px * 0.999]
    else:
        hits = df[df["low"] <= entry_px * 1.001]

    if hits.empty:
        p(f"  [{label}] entry bar not found"); return None, -1

    idx = hits.index[0]
    start = max(0, idx - n_before)
    end   = min(len(df), idx + n_after + 1)
    return df.iloc[start:end], idx

def fmt(df):
    """Format dataframe for printing — pick best available columns."""
    priority = [
        "date","open","high","low","close","volume",
        "rsi","adx","atr","ema9","ema21","ema50","ema200",
        "vwap","avwap",
        "bb_upper","bb_lower","bb_mid",
        "macd","macd_signal","macd_hist",
        "cci","mfi","obv",
        "quality_score",
    ]
    cols = [c for c in priority if c in df.columns]
    return df[cols]

def analyse(ticker, meta):
    SL_PCT  = meta["sl_pct"]
    TGT_PCT = 0.0080
    entry   = meta["entry"]
    sl_abs  = entry * (1 - SL_PCT)
    tgt_abs = entry * (1 + TGT_PCT)
    side    = meta["side"]

    p(f"\n{'='*100}")
    p(f"TICKER: {ticker}  |  SIDE: {side}  |  SETUP: {meta['setup']}  |  QS: {meta['qs']:.3f}")
    p(f"Entry: {entry:.4f}  |  SL: {sl_abs:.4f} (-{SL_PCT*100:.2f}%)  |  TGT: {tgt_abs:.4f} (+{TGT_PCT*100:.2f}%)  |  Exit: {meta['exit']:.4f}")
    p(f"{'='*100}")

    df5  = load(DATA_5M, ticker, "5min")
    df1  = load(DATA_1M, ticker, "1min")
    day5 = day(df5)
    day1 = day(df1)
    prev5= prev_day(df5)

    # ── Available columns ──────────────────────────────────────────────
    p(f"\n[COLUMNS in 5-min parquet]: {list(df5.columns) if not df5.empty else 'N/A'}")

    # ── Previous day context ───────────────────────────────────────────
    if not prev5.empty:
        p(f"\n--- PREVIOUS DAY (last 6 bars, 5-min) ---")
        p(fmt(prev5.tail(6)).to_string(index=False))
        prev_close = float(prev5["close"].iloc[-1])
        prev_high  = float(prev5["high"].max())
        prev_low   = float(prev5["low"].min())
        p(f"  Prev-day close={prev_close:.2f}  high={prev_high:.2f}  low={prev_low:.2f}")
        p(f"  Today open vs prev close: gap = {(day5.iloc[0]['open'] - prev_close)/prev_close*100:.2f}% " if not day5.empty else "")

    # ── OR bar analysis ───────────────────────────────────────────────
    if not day5.empty:
        or_bar = day5.iloc[0]
        or_open  = float(or_bar["open"])
        or_high  = float(or_bar["high"])
        or_low   = float(or_bar["low"])
        or_close = float(or_bar["close"])
        or_vol   = float(or_bar["volume"])
        or_range = or_high - or_low
        or_close_pos = (or_close - or_low) / or_range if or_range > 0 else 0.5
        p(f"\n--- OR BAR (09:15-09:20, 5-min) ---")
        p(f"  open={or_open:.2f}  high={or_high:.2f}  low={or_low:.2f}  close={or_close:.2f}  vol={or_vol:.0f}")
        p(f"  OR range = {or_range:.2f} ({or_range/or_open*100:.2f}%)")
        p(f"  OR close position = {or_close_pos:.2f}  (0=bottom, 1=top)  <<< key: <0.3 = bearish OR")
        p(f"  Entry vs OR-high = {entry:.2f} vs {or_high:.2f}  = {(entry-or_high)/or_high*100:+.2f}% above OR-high")
        p(f"  Gap from OR-low to entry = {(entry-or_low)/or_low*100:+.2f}%")

    # ── Full day 5-min with all indicators ────────────────────────────
    p(f"\n--- FULL DAY 5-MIN (all indicators) ---")
    if not day5.empty:
        p(fmt(day5).to_string(index=False))

    # ── Entry bar context (5-min, 5 bars before + 10 after) ───────────
    p(f"\n--- ENTRY CONTEXT 5-MIN (5 before + entry + 10 after) ---")
    ctx5, entry_idx5 = show_bar(day5, "5min", entry, side, n_before=5, n_after=10)
    if ctx5 is not None:
        p(fmt(ctx5).to_string(index=False))

        entry_bar5 = day5.iloc[entry_idx5]
        p(f"\n  ENTRY BAR ({entry_bar5['date']}):")
        for col in ["rsi","adx","atr","ema9","ema21","ema50","ema200","vwap","avwap","bb_upper","bb_lower","macd","macd_signal","cci","mfi","quality_score"]:
            if col in entry_bar5.index and pd.notna(entry_bar5[col]):
                p(f"    {col:20s} = {entry_bar5[col]:.4f}")

        # Price run-up to entry
        if entry_idx5 >= 1:
            open_price = float(day5.iloc[0]["open"])
            p(f"\n  PRICE RUN-UP TO ENTRY:")
            p(f"    Day open:  {open_price:.2f}")
            p(f"    Entry:     {entry:.2f}")
            p(f"    Run-up:    {(entry-open_price)/open_price*100:+.2f}%  ({entry_idx5} bars from open)")
            p(f"    Bars from open to entry: {entry_idx5}")
            if entry_idx5 >= 3:
                run3 = (entry - float(day5.iloc[max(0,entry_idx5-3)]["close"])) / float(day5.iloc[max(0,entry_idx5-3)]["close"]) * 100
                p(f"    Run in last 3 bars:    {run3:+.2f}%")

    # ── 1-min entry context ───────────────────────────────────────────
    p(f"\n--- ENTRY CONTEXT 1-MIN (8 before + entry + 15 after) ---")
    if not day1.empty:
        ctx1, entry_idx1 = show_bar(day1, "1min", entry, side, n_before=8, n_after=15)
        if ctx1 is not None:
            p(fmt(ctx1).to_string(index=False))

            entry_bar1 = day1.iloc[entry_idx1]
            p(f"\n  ENTRY BAR 1-MIN ({entry_bar1['date']}):")
            for col in ["rsi","adx","atr","vwap","avwap","bb_upper","bb_lower","macd","cci","quality_score"]:
                if col in entry_bar1.index and pd.notna(entry_bar1[col]):
                    p(f"    {col:20s} = {entry_bar1[col]:.4f}")
    else:
        p("  [No 1-min data available]")

    # ── Momentum & volume analysis ────────────────────────────────────
    p(f"\n--- MOMENTUM & VOLUME ANALYSIS ---")
    if not day5.empty and entry_idx5 >= 0:
        # Volume profile: avg volume vs entry bar volume
        avg_vol = day5["volume"].mean()
        entry_vol = float(day5.iloc[entry_idx5]["volume"])
        p(f"  Day avg volume/bar: {avg_vol:.0f}")
        p(f"  Entry bar volume:   {entry_vol:.0f}  ({entry_vol/avg_vol:.2f}x avg)")

        # Consecutive up bars before entry
        up_bars = 0
        for i in range(entry_idx5-1, max(-1, entry_idx5-6), -1):
            if i >= 0 and day5.iloc[i]["close"] > day5.iloc[i]["open"]:
                up_bars += 1
            else:
                break
        p(f"  Consecutive bullish bars before entry: {up_bars}")

        # RSI at entry if available
        if "rsi" in day5.columns and pd.notna(day5.iloc[entry_idx5].get("rsi")):
            rsi_val = float(day5.iloc[entry_idx5]["rsi"])
            p(f"  RSI at entry: {rsi_val:.1f}  {'<<< OVERBOUGHT' if rsi_val > 65 else '<<< NORMAL' if rsi_val > 45 else '<<< WEAK'}")

        # ADX at entry
        if "adx" in day5.columns and pd.notna(day5.iloc[entry_idx5].get("adx")):
            adx_val = float(day5.iloc[entry_idx5]["adx"])
            p(f"  ADX at entry: {adx_val:.1f}  {'<<< STRONG TREND' if adx_val > 25 else '<<< WEAK TREND'}")

        # ATR context
        if "atr" in day5.columns and pd.notna(day5.iloc[entry_idx5].get("atr")):
            atr_val = float(day5.iloc[entry_idx5]["atr"])
            p(f"  ATR at entry: {atr_val:.4f}  ({atr_val/entry*100:.2f}% of price)")
            p(f"  SL as ATR multiple: {(entry-sl_abs)/atr_val:.2f}x ATR")

    # ── Post-entry what actually happened ─────────────────────────────
    p(f"\n--- WHAT HAPPENED AFTER ENTRY (5-min) ---")
    if ctx5 is not None:
        local_idx = entry_idx5 - max(0, entry_idx5 - 5)  # position within ctx5
        post = fmt(day5.iloc[entry_idx5:min(entry_idx5+15, len(day5))])
        p(post.to_string(index=False))
        # Find when SL was hit
        post_raw = day5.iloc[entry_idx5:]
        sl_hit = post_raw[post_raw["low"] <= sl_abs]
        tgt_hit = post_raw[post_raw["high"] >= tgt_abs]
        if not sl_hit.empty:
            p(f"\n  SL ({sl_abs:.2f}) first breached at: {sl_hit.iloc[0]['date']}  (bar {sl_hit.index[0]-entry_idx5} after entry)")
        if not tgt_hit.empty:
            p(f"  TGT ({tgt_abs:.2f}) first breached at: {tgt_hit.iloc[0]['date']}  (bar {tgt_hit.index[0]-entry_idx5} after entry)")
        else:
            p(f"  TGT ({tgt_abs:.2f}) was NEVER hit on this day")

    p(f"\n{'─'*100}")

for ticker, meta in LOSSES.items():
    analyse(ticker, meta)

p("\n\n" + "="*100)
p("CROSS-TICKER PATTERNS SUMMARY")
p("="*100)
p("""
Pattern analysis across GESHIP, BAJAJELEC, NFL:
(see individual sections above for full data)
""")

OUT.close()
print(f"Output written to _deep3_loss_study_out.txt")
