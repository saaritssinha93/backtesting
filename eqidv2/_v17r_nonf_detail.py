import glob
import pandas as pd

od = r"c:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2\outputs_v17D_backtest\v17r_nonf_run"
f = glob.glob(od + r"\step5_costed_*.csv")[0]
df = pd.read_csv(f)


def pf(s):
    s = pd.to_numeric(s, errors="coerce").dropna()
    w, l = float(s[s > 0].sum()), float(-s[s < 0].sum())
    return float("inf") if l <= 0 and w > 0 else (0.0 if l <= 0 else w / l)


print("=== v17r_nonf ALL-ADV (pre universe filter), realistic per-row costs ===")
print(f"n={len(df)}  PF={pf(df['pnl_pct_eff_net']):.3f}  "
      f"sum_rs={pd.to_numeric(df['pnl_rs_eff_net']).sum():+,.0f}")
print()
print("Per-setup x ADV bucket:")
for (setup, bucket), g in df.groupby(["setup", "adv_bucket"]):
    rs = pd.to_numeric(g["pnl_rs_eff_net"]).sum()
    win = (g["outcome"] == "TARGET").mean() * 100
    print(f"  {setup:<32s} {bucket:<10s} n={len(g):<4d} "
          f"PF={pf(g['pnl_pct_eff_net']):.3f} win%={win:5.1f} Rs={rs:+,.0f}")
print()
for setup in df["setup"].unique():
    sub = df[df["setup"] == setup]
    print(f"{setup} outcome mix: {sub['outcome'].value_counts().to_dict()}")
