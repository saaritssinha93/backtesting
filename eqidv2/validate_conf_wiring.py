"""Import-level validation of the final_setup_conf wiring in avwap_5min_ID_v11_backtesting.
No universe scan / heavy I/O — safe to run during market hours."""
import pandas as pd
import avwap_5min_ID_v11_backtesting as v11
import final_setup_conf as fc
import avwap_5min_ID_v6_backtesting as v6
import avwap_5min_ID_v7_candidate_scan as cs

conf = fc.FINAL_SETUP_CONF
print("conf setups:", len(conf))
assert "final_setup_conf" in v11.SELECTED_STRATEGY_PROFILE_CHOICES
assert v11._normalise_selected_strategy_profile("final_setup_conf") == "final_setup_conf"

# activate
v11._activate_final_setup_conf()
# 1. exits overridden
for k, cfg in conf.items():
    ex = cfg["exit"]
    assert v6.SETUP_EXIT_RULES[k] == (ex["sl_pct"], ex["tgt_pct"]), (k, v6.SETUP_EXIT_RULES[k], ex)
# 2. premom gates = only conf setups with premom
exp_pm = {k for k, c in conf.items() if c.get("pre_momentum_terms")}
assert set(v11.PRE_ENTRY_MOMENTUM_SETUP_GATES) == exp_pm, (set(v11.PRE_ENTRY_MOMENTUM_SETUP_GATES), exp_pm)
for k in exp_pm:
    assert list(v11.PRE_ENTRY_MOMENTUM_SETUP_GATES[k]) == [tuple(t) for t in conf[k]["pre_momentum_terms"]]
# 3. allowed setups restricted to the 9
assert set(cs.ALLOWED_SETUPS) == set(conf) and cs.FILTER_TO_V8_EXIT_SETUPS is True
print("OK: exits + premom gates + allowed-setups overridden correctly")
print("  premom-gated:", sorted(exp_pm))

# 4. mask test on synthetic candidates (one PASS + one FAIL per masked setup, plus a non-conf setup)
rows = [
    # B_AVWAP mask vwap_dist_atr<=1.0
    {"setup": "B_AVWAP_RECLAIM_REVERSAL", "vwap_dist_atr": 0.5, "exp": True},
    {"setup": "B_AVWAP_RECLAIM_REVERSAL", "vwap_dist_atr": 2.0, "exp": False},
    # B_HUGE_C1 mask regime!=BULL
    {"setup": "B_HUGE_C1_CLOSE_RECLAIM_BREAK", "regime": "NEUTRAL", "exp": True},
    {"setup": "B_HUGE_C1_CLOSE_RECLAIM_BREAK", "regime": "BULL", "exp": False},
    # E_VWAP_LOSE mask vol_ratio in [1.8,3.2] + guard min_slot 09:45
    {"setup": "E_VWAP_LOSE_EARLY_SHORT", "vol_ratio": 2.5, "t": "10:00", "exp": True},
    {"setup": "E_VWAP_LOSE_EARLY_SHORT", "vol_ratio": 5.0, "t": "10:00", "exp": False},   # vol out of band
    {"setup": "E_VWAP_LOSE_EARLY_SHORT", "vol_ratio": 2.5, "t": "09:30", "exp": False},   # before 09:45 guard
    # T_SHORT mask vol_ratio<=1.33
    {"setup": "T_TREND_DAY_EMA_STAIR_SHORT", "vol_ratio": 1.0, "exp": True},
    {"setup": "T_TREND_DAY_EMA_STAIR_SHORT", "vol_ratio": 2.0, "exp": False},
    # L_PRESSURE mask quality_score<=25
    {"setup": "L_PRESSURE_BURST_VWAP", "quality_score": 20.0, "exp": True},
    {"setup": "L_PRESSURE_BURST_VWAP", "quality_score": 50.0, "exp": False},
    # no-mask conf setups -> pass
    {"setup": "A_PULLBACK_C2_THEN_BREAK_C2_LOW", "exp": True},
    {"setup": "G_HIGHER_HIGH_BREAK", "exp": True},
    {"setup": "L_DOUBLE_BOTTOM_VWAP", "exp": True},
    {"setup": "D_EMA20_REJECTION", "exp": True},
    # non-conf setup -> excluded
    {"setup": "C_OR_BREAKOUT", "exp": False},
]
df = pd.DataFrame(rows)
df["signal_time_ist"] = df.get("t", pd.Series(["10:00"] * len(df))).fillna("10:00").map(lambda s: f"2026-03-02 {s}:00+05:30")
for c in ["vwap_dist_atr", "vol_ratio", "quality_score"]:
    if c not in df:
        df[c] = float("nan")
if "regime" not in df:
    df["regime"] = "NEUTRAL"
df["regime"] = df["regime"].fillna("NEUTRAL")
for c in ["signal_open", "signal_high", "signal_low", "signal_close", "market_ret_pct", "rs_pct", "atr_pct", "body_pct", "ranker_score"]:
    df[c] = 1.0

mask = v11._final_setup_conf_mask(df).reset_index(drop=True)
df = df.reset_index(drop=True)
ok = True
for i in range(len(df)):
    got = bool(mask.iloc[i]); exp = bool(df.loc[i, "exp"])
    flag = "OK " if got == exp else "FAIL"
    if got != exp:
        ok = False
    extra = df.loc[i, "vwap_dist_atr"] if df.loc[i, "setup"].startswith("B_AVWAP") else (
        df.loc[i, "regime"] if df.loc[i, "setup"].startswith("B_HUGE") else (
            df.loc[i, "vol_ratio"] if df.loc[i, "setup"] in ("E_VWAP_LOSE_EARLY_SHORT", "T_TREND_DAY_EMA_STAIR_SHORT") else (
                df.loc[i, "quality_score"] if df.loc[i, "setup"].startswith("L_PRESSURE") else "")))
    print(f"  [{flag}] {df.loc[i,'setup']:<34} {str(extra):>8}  t={df.loc[i,'signal_time_ist'][-14:-9]}  got={got} exp={exp}")
print("\nMASK TEST:", "ALL PASS" if ok else "*** FAILURES ***")
assert ok, "mask test failed"
print("\n=== final_setup_conf wiring VALIDATED (import-level). Heavy backtest run deferred to post-close. ===")
