import pandas as pd

CSV = r"C:\TradingData\eqidv2\outputs_v16_5min\avwap_longshort_trades_v16_5min_ALL_DAYS_20260406_170217.csv"
df = pd.read_csv(CSV)
today = df[df["trade_date"] == "2026-04-06"].copy()

losses = today[today["pnl_pct"] < 0].sort_values("pnl_pct")
wins = today[today["pnl_pct"] > 0]

cols = ["ticker","side","setup","impulse_type","quality_score","entry_price","exit_price","outcome","pnl_pct","exit_resolution_case"]

print("=" * 80)
print(f"TODAY 2026-04-06 | Total={len(today)} | Wins={len(wins)} | Losses={len(losses)}")
print("=" * 80)

print("\n=== LOSSES ===")
print(losses[cols].to_string(index=False))

print("\n=== WINS ===")
print(wins[cols].to_string(index=False))

print("\n=== LOSS BREAKDOWN ===")
print(f"By side:    {losses.groupby('side').size().to_dict()}")
print(f"By setup:   {losses.groupby('setup').size().to_dict()}")
print(f"By impulse: {losses.groupby('impulse_type').size().to_dict()}")
print(f"Avg QS (losses): {losses['quality_score'].mean():.3f}")
print(f"Avg QS (wins):   {wins['quality_score'].mean():.3f}")
print(f"Avg pnl (losses): {losses['pnl_pct'].mean():.4f}%")
print(f"Avg pnl (wins):   {wins['pnl_pct'].mean():.4f}%")
