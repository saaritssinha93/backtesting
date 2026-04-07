"""
Deep loss analysis for 2026-04-06 losing trades.
Loads 5-min parquet for each loser, shows indicators at entry time,
and compares them against today's winners for filter impact.
"""
import pandas as pd
import numpy as np
import sys
from pathlib import Path

OUT_FILE = Path(__file__).parent / "_deep_loss_analysis_out.txt"
_fh = open(OUT_FILE, "w", encoding="utf-8")

def print(*args, **kwargs):
    kwargs.setdefault("file", _fh)
    import builtins
    builtins.print(*args, **kwargs)
    _fh.flush()

TRADE_CSV   = r"C:\TradingData\eqidv2\outputs_v16_5min\avwap_longshort_trades_v16_5min_ALL_DAYS_20260406_170217.csv"
DATA_5MIN   = Path(r"C:\TradingData\eqidv2\stocks_indicators_5min_eq")
DATA_1MIN   = Path(r"C:\TradingData\eqidv2\stocks_indicators_1min_eq")
DATE        = "2026-04-06"

LOSERS  = ["HITECH", "TCPLPACK", "GESHIP", "BAJAJELEC", "NFL"]
# For impact study, include today's winners too
WINNERS = ["GAEL","MVGJL","THANGAMAYL","POWERINDIA","ZYDUSWELL",
           "AJMERA","APTECHT","ARKADE","DBOL","EXICOM",
           "SAMHI","MHRIL","MUNJALAU","PNBHOUSING","JINDALSAW"]

def load_5m(ticker):
    p = DATA_5MIN / f"{ticker}_stocks_indicators_5min.parquet"
    if not p.exists():
        return None
    df = pd.read_parquet(p)
    df["date"] = pd.to_datetime(df["date"])
    return df

def load_1m(ticker):
    p = DATA_1MIN / f"{ticker}_stocks_indicators_1min.parquet"
    if not p.exists():
        return None
    df = pd.read_parquet(p)
    df["date"] = pd.to_datetime(df["date"])
    return df

def get_day(df, date_str):
    if df is None or df.empty:
        return pd.DataFrame()
    mask = df["date"].dt.strftime("%Y-%m-%d") == date_str
    return df[mask].reset_index(drop=True)

def show_cols(df, cols):
    available = [c for c in cols if c in df.columns]
    return df[available]

KEY_COLS = ["date","open","high","low","close","volume",
            "rsi","atr","adx","ema9","ema21","ema50",
            "vwap","bb_upper","bb_lower","macd","macd_signal",
            "quality_score"] if True else []

# ── Load trades ──────────────────────────────────────────────────────────────
trades = pd.read_csv(TRADE_CSV)
today  = trades[trades["trade_date"] == DATE].copy()
losses = today[today["pnl_pct"] < 0].set_index("ticker")
wins   = today[today["pnl_pct"] > 0].set_index("ticker")

print("=" * 90)
print(f"DEEP LOSS ANALYSIS — {DATE}")
print("=" * 90)

for ticker in LOSERS:
    row = losses.loc[ticker] if ticker in losses.index else None
    if row is None:
        print(f"\n{ticker}: not found in losses"); continue

    print(f"\n{'─'*90}")
    print(f"TICKER: {ticker}  |  SIDE: {row['side']}  |  SETUP: {row['setup']}  |  IMPULSE: {row['impulse_type']}")
    print(f"  Entry: {row['entry_price']:.4f}  Exit: {row['exit_price']:.4f}  Outcome: {row['outcome']}  PnL: {row['pnl_pct']:.3f}%")
    print(f"  QS: {row['quality_score']:.4f}  ExitCase: {row['exit_resolution_case']}")

    df5 = load_5m(ticker)
    day5 = get_day(df5, DATE)

    if day5.empty:
        print(f"  [!] No 5-min data found for {ticker}"); continue

    # Find entry bar (closest bar to entry_price)
    entry_px = float(row["entry_price"])
    side     = str(row["side"]).upper()

    # Show all bars of the day
    avail = [c for c in ["date","open","high","low","close","volume","rsi","adx","atr",
                          "ema9","ema21","ema50","vwap","quality_score"] if c in day5.columns]
    print(f"\n  5-MIN BARS FOR {DATE}:")
    pd.set_option("display.float_format", "{:.2f}".format)
    pd.set_option("display.max_columns", 20)
    pd.set_option("display.width", 200)
    print(day5[avail].to_string(index=False))

    # Identify the signal / entry bar
    if side == "LONG":
        # entry bar is where close crossed above some level near entry_price
        # find bar where high >= entry_price (first breach)
        hit = day5[day5["high"] >= entry_px * 0.999]
    else:
        hit = day5[day5["low"] <= entry_px * 1.001]

    if not hit.empty:
        entry_bar = hit.iloc[0]
        entry_idx = hit.index[0]
        print(f"\n  ENTRY BAR (approx): {entry_bar['date']}")

        # Show 3 bars before entry for context
        pre_start = max(0, entry_idx - 3)
        pre_bars = day5.iloc[pre_start:entry_idx+1][avail]
        print(f"  PRE-ENTRY CONTEXT (last 3 bars + entry bar):")
        print(pre_bars.to_string(index=False))

        # Key indicator values at entry
        print(f"\n  KEY INDICATORS AT ENTRY:")
        for col in ["rsi","adx","atr","ema9","ema21","ema50","vwap","quality_score"]:
            if col in entry_bar.index:
                print(f"    {col:20s}: {entry_bar[col]:.4f}")

        # Post-entry bars (what happened after)
        post_bars = day5.iloc[entry_idx:min(entry_idx+8, len(day5))][avail]
        print(f"\n  POST-ENTRY BARS (8 bars after entry):")
        print(post_bars.to_string(index=False))
    else:
        print(f"  [!] Could not identify entry bar from 5-min data")

    # OR-high context (first bar)
    first_bar = day5.iloc[0] if not day5.empty else None
    if first_bar is not None:
        or_high = first_bar["high"]
        or_low  = first_bar["low"]
        print(f"\n  OR (first 5-min bar): open={first_bar['open']:.2f}  high={or_high:.2f}  low={or_low:.2f}  close={first_bar['close']:.2f}")
        if side == "LONG":
            print(f"  Entry vs OR-high: entry={entry_px:.2f}  OR-high={or_high:.2f}  diff={entry_px-or_high:.2f} ({(entry_px-or_high)/or_high*100:.2f}%)")
        else:
            print(f"  Entry vs OR-low:  entry={entry_px:.2f}  OR-low={or_low:.2f}   diff={or_low-entry_px:.2f} ({(or_low-entry_px)/or_low*100:.2f}%)")


# ── Filter impact study ───────────────────────────────────────────────────────
print(f"\n\n{'='*90}")
print("FILTER IMPACT STUDY — what filter could avoid losses? and what winners would it kill?")
print("="*90)

def get_entry_bar_indicators(ticker, side, entry_px, date_str):
    """Return indicator dict at the approximate entry bar."""
    df5 = load_5m(ticker)
    day5 = get_day(df5, date_str)
    if day5.empty:
        return {}
    if side == "LONG":
        hit = day5[day5["high"] >= entry_px * 0.999]
    else:
        hit = day5[day5["low"] <= entry_px * 1.001]
    if hit.empty:
        return {}
    bar = hit.iloc[0]
    return {c: bar[c] for c in ["rsi","adx","atr","ema9","ema21","ema50","vwap","quality_score"]
            if c in bar.index}

print("\n--- INDICATORS AT ENTRY: Losses vs Wins ---")
rows = []
for ticker in LOSERS + WINNERS:
    t = losses if ticker in losses.index else wins
    if ticker not in t.index:
        continue
    row = t.loc[ticker]
    ind = get_entry_bar_indicators(ticker, str(row["side"]).upper(), float(row["entry_price"]), DATE)
    ind["ticker"]  = ticker
    ind["group"]   = "LOSS" if ticker in LOSERS else "WIN"
    ind["side"]    = row["side"]
    ind["qs_trade"]= float(row["quality_score"])
    ind["pnl"]     = float(row["pnl_pct"])
    rows.append(ind)

impact = pd.DataFrame(rows)
cols_show = [c for c in ["ticker","group","side","qs_trade","pnl","rsi","adx","quality_score","ema9","ema21","ema50","vwap"] if c in impact.columns]
print(impact[cols_show].sort_values(["group","pnl"]).to_string(index=False))

# Threshold analysis
print(f"\n--- QS THRESHOLD ANALYSIS ---")
for qs_thresh in [5.5, 6.0, 6.5, 7.0]:
    if "qs_trade" not in impact.columns:
        break
    kept  = impact[impact["qs_trade"] >= qs_thresh]
    lost  = impact[impact["qs_trade"] < qs_thresh]
    losses_avoided = len(lost[lost["group"]=="LOSS"])
    winners_killed = len(lost[lost["group"]=="WIN"])
    losses_kept    = len(kept[kept["group"]=="LOSS"])
    winners_kept   = len(kept[kept["group"]=="WIN"])
    print(f"  QS >= {qs_thresh:.1f}:  avoid {losses_avoided}/5 losses | kill {winners_killed}/15 winners | keep {winners_kept}W + {losses_kept}L")

print(f"\n--- RSI ANALYSIS AT ENTRY (LONG trades only) ---")
long_trades = impact[impact["side"].str.upper()=="LONG"]
if "rsi" in long_trades.columns:
    print(long_trades[["ticker","group","rsi","qs_trade","pnl"]].sort_values("rsi").to_string(index=False))

print(f"\n--- EMA TREND AT ENTRY (is close > ema9 > ema21?) ---")
df5_rows = []
for ticker in LOSERS + WINNERS:
    df5 = load_5m(ticker)
    day5 = get_day(df5, DATE)
    if day5.empty:
        continue
    t = losses if ticker in losses.index else wins
    if ticker not in t.index:
        continue
    row = t.loc[ticker]
    entry_px = float(row["entry_price"])
    side = str(row["side"]).upper()
    if side == "LONG":
        hit = day5[day5["high"] >= entry_px * 0.999]
    else:
        hit = day5[day5["low"] <= entry_px * 1.001]
    if hit.empty:
        continue
    bar = hit.iloc[0]
    e9  = bar.get("ema9", np.nan)
    e21 = bar.get("ema21", np.nan)
    e50 = bar.get("ema50", np.nan)
    cls = bar["close"]
    trend_ok = (cls > e9 > e21) if side == "LONG" else (cls < e9 < e21)
    df5_rows.append({
        "ticker": ticker,
        "group": "LOSS" if ticker in LOSERS else "WIN",
        "side": side,
        "close": cls,
        "ema9": e9,
        "ema21": e21,
        "ema50": e50,
        "trend_aligned": trend_ok,
        "qs": float(row["quality_score"]),
        "pnl": float(row["pnl_pct"])
    })

trend_df = pd.DataFrame(df5_rows)
if not trend_df.empty:
    print(trend_df[["ticker","group","side","close","ema9","ema21","trend_aligned","qs","pnl"]].sort_values(["group","pnl"]).to_string(index=False))

    # How many losses have misaligned trend?
    loss_misaligned  = trend_df[(trend_df["group"]=="LOSS") & (~trend_df["trend_aligned"])]
    win_misaligned   = trend_df[(trend_df["group"]=="WIN")  & (~trend_df["trend_aligned"])]
    print(f"\n  Losses with misaligned trend: {len(loss_misaligned)}/5 -> {list(loss_misaligned['ticker'])}")
    print(f"  Wins  with misaligned trend:  {len(win_misaligned)}/15 -> {list(win_misaligned['ticker'])}")

print("\n[DONE]")
_fh.close()
