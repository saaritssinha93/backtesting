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
# 3. allowed setups restricted to the current conf book
assert set(cs.ALLOWED_SETUPS) == set(conf) and cs.FILTER_TO_V8_EXIT_SETUPS is True
print("OK: exits + premom gates + allowed-setups overridden correctly")
print("  premom-gated:", sorted(exp_pm))

# 4. mask test on synthetic candidates (one PASS + one FAIL per masked setup, plus a non-conf setup)
rows = [
    # A_PULLBACK mask quality_score>=123.7606; ADX is a pre-momentum gate tested above.
    {"setup": "A_PULLBACK_C2_THEN_BREAK_C2_LOW", "quality_score": 130.0, "exp": True},
    {"setup": "A_PULLBACK_C2_THEN_BREAK_C2_LOW", "quality_score": 120.0, "exp": False},
    # A_MOD mask vol_ratio>=1.955814
    {"setup": "A_MOD_BREAK_C1_LOW", "vol_ratio": 2.2, "exp": True},
    {"setup": "A_MOD_BREAK_C1_LOW", "vol_ratio": 1.5, "exp": False},
    # G_LOWER mask vol_ratio>=4.129044 AND quality_score>=76.444124
    {"setup": "G_LOWER_LOW_BREAK", "vol_ratio": 4.5, "quality_score": 80.0, "exp": True},
    {"setup": "G_LOWER_LOW_BREAK", "vol_ratio": 3.0, "quality_score": 80.0, "exp": False},
    {"setup": "G_LOWER_LOW_BREAK", "vol_ratio": 4.5, "quality_score": 70.0, "exp": False},
    # no-mask active conf setups -> pass
    {"setup": "B_HUGE_RED_FAILED_BOUNCE", "exp": True},
    {"setup": "C_OR_BREAKDOWN", "exp": True},
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
    extra = (
        f"vol={df.loc[i, 'vol_ratio']} q={df.loc[i, 'quality_score']}"
        if df.loc[i, "setup"] in ("A_MOD_BREAK_C1_LOW", "G_LOWER_LOW_BREAK")
        else (df.loc[i, "quality_score"] if df.loc[i, "setup"] == "A_PULLBACK_C2_THEN_BREAK_C2_LOW" else "")
    )
    print(f"  [{flag}] {df.loc[i,'setup']:<34} {str(extra):>8}  t={df.loc[i,'signal_time_ist'][-14:-9]}  got={got} exp={exp}")
print("\nMASK TEST:", "ALL PASS" if ok else "*** FAILURES ***")
assert ok, "mask test failed"
print("\n=== final_setup_conf wiring VALIDATED (import-level). Heavy backtest run deferred to post-close. ===")
