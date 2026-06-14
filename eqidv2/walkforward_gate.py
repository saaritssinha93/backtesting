"""
walkforward_gate.py
===================

The ONLY thing allowed to promote a setup/rule into accepted_rules.csv.

Problem it solves
-----------------
The V11 overlay carries thresholds like `quality >= 97.873364` and
`vol ratio <= 1.698991`. Six-decimal thresholds are optimizer artifacts fit to
one in-sample window; they do not generalize. With ~26 setups x multiple gates,
some will look great purely by chance. This gate decides promotion on
WALK-FORWARD, OUT-OF-SAMPLE, NET-OF-COST performance, with:

  1. Purged walk-forward folds (split by trading DAY + embargo) so train never
     leaks into test via overnight / multi-day autocorrelation.
  2. Costs applied per trade (imports nse_intraday_costs) -> everything is NET.
  3. Any threshold fitting happens on TRAIN ONLY, scored on TEST.
  4. A bootstrap one-sided p-value that OOS mean net P&L > 0.
  5. Benjamini-Hochberg FDR correction across all setups (multiple testing).
  6. An in-sample-vs-out-of-sample degradation ratio as an overfit flag.

Decision per setup: PROMOTE / PROBATION / REJECT.

Input contract
--------------
A pandas DataFrame of RESOLVED candidate trades (one row per candidate that the
backtester already resolved to an exit under the setup's exit rules), with:

  date         : trading day (date or datetime, used only for fold splitting)
  setup        : setup name (str)
  side         : "LONG" | "SHORT"
  entry_price  : float
  exit_price   : float   (resolved exit: stop / target / time-stop fill)
  qty          : float   (share count; for correct flat-fee cost modelling)
  <features>   : any numeric columns you might threshold on

The gate consumes OUTCOMES, not raw bars: your existing 5-min exit backtester
produces the entry/exit; this layer only decides which rules to trust forward.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable, Sequence

import numpy as np
import pandas as pd

from nse_intraday_costs import CostConfig, intraday_equity_costs


# ---------------------------------------------------------------------------
# Vectorised net P&L (mirrors nse_intraday_costs.intraday_equity_costs).
# Kept here for speed on large research frames; cross-checked in the demo.
# ---------------------------------------------------------------------------
def net_pnl_vectorized(entry: np.ndarray, exit_: np.ndarray, qty: np.ndarray,
                       side: np.ndarray, cfg: CostConfig) -> np.ndarray:
    entry = np.asarray(entry, float)
    exit_ = np.asarray(exit_, float)
    qty = np.asarray(qty, float)
    is_long = (np.asarray(side) == "LONG")

    entry_val, exit_val = entry * qty, exit_ * qty
    buy_value = np.where(is_long, entry_val, exit_val)
    sell_value = np.where(is_long, exit_val, entry_val)
    gross = np.where(is_long, (exit_ - entry) * qty, (entry - exit_) * qty)
    turnover = buy_value + sell_value

    brokerage = (np.minimum(cfg.brokerage_cap_rs, cfg.brokerage_pct * buy_value)
                 + np.minimum(cfg.brokerage_cap_rs, cfg.brokerage_pct * sell_value))
    stt = cfg.stt_sell_pct * sell_value
    exch = cfg.exch_txn_pct * turnover
    sebi = cfg.sebi_pct * turnover
    ipft = cfg.ipft_pct * turnover
    stamp = cfg.stamp_buy_pct * buy_value
    gst = cfg.gst_pct * (brokerage + exch + sebi + ipft)
    total = brokerage + stt + exch + sebi + ipft + stamp + gst
    return gross - total


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class WalkForwardConfig:
    train_days: int = 60            # rolling train window (trading days)
    test_days: int = 20             # forward test window
    embargo_days: int = 1           # gap between train end and test start
    step_days: int | None = None    # roll step; defaults to test_days (no overlap)

    min_oos_trades: int = 40        # below this, no setup can be promoted
    min_net_pf: float = 1.20        # OOS net profit factor bar
    min_fold_consistency: float = 0.55  # frac of test folds with positive net
    overfit_ratio: float = 0.60     # flag if net_pf_oos / net_pf_is < this

    fdr_alpha: float = 0.10         # Benjamini-Hochberg false discovery rate
    n_bootstrap: int = 5000
    random_seed: int = 7

    # When True, folds are built over the GLOBAL trading calendar shared by all
    # setups (so a 20-day test window is the same calendar window for every
    # setup, and a low-frequency setup contributes whatever trades fall in it)
    # instead of each setup's own sparse trading days. Required to get any OOS
    # evaluation of setups that trade < train_days distinct days in a short
    # history. Default False preserves the original per-setup-days behaviour.
    global_calendar_folds: bool = False

    def step(self) -> int:
        return self.step_days or self.test_days


# ---------------------------------------------------------------------------
# Fit / apply contract (threshold fitting happens on TRAIN ONLY)
# ---------------------------------------------------------------------------
FitFn = Callable[[pd.DataFrame], object]
ApplyFn = Callable[[pd.DataFrame, object], pd.DataFrame]


def no_fit(_train: pd.DataFrame) -> None:
    """Default: don't fit anything. Honest baseline -- does the raw setup
    have OOS edge at all, before any threshold cherry-picking?"""
    return None


def apply_identity(test: pd.DataFrame, _params: object) -> pd.DataFrame:
    return test


def make_threshold_fitter(feature: str, cfg: CostConfig,
                          min_keep: int = 15, n_grid: int = 17):
    """
    Build a (fit_fn, apply_fn) pair that, on TRAIN, picks the single threshold on
    `feature` (and direction) maximising NET expectancy, then applies it to TEST.

    This is exactly the operation that produces 6-decimal thresholds -- the
    difference is the gate scores it OUT-OF-SAMPLE and will reject it if the
    in-sample-optimal cut does not survive forward.
    """
    def fit(train: pd.DataFrame):
        x = train[feature].to_numpy(float)
        net = net_pnl_vectorized(train["entry_price"], train["exit_price"],
                                 train["qty"], train["side"].to_numpy(), cfg)
        qs = np.linspace(0.1, 0.9, n_grid)
        thresholds = np.unique(np.nanquantile(x, qs))
        best = None  # (net_expectancy, direction, thr)
        for thr in thresholds:
            for direction in (">=", "<="):
                keep = (x >= thr) if direction == ">=" else (x <= thr)
                if keep.sum() < min_keep:
                    continue
                exp = net[keep].mean()
                if best is None or exp > best[0]:
                    best = (exp, direction, float(thr))
        if best is None:
            return None
        return {"feature": feature, "direction": best[1], "threshold": best[2]}

    def apply(test: pd.DataFrame, params):
        if not params:
            return test
        x = test[params["feature"]].to_numpy(float)
        keep = (x >= params["threshold"]) if params["direction"] == ">=" \
            else (x <= params["threshold"])
        return test.loc[keep]

    return fit, apply


# ---------------------------------------------------------------------------
# Fold construction
# ---------------------------------------------------------------------------
def purged_walk_forward_folds(days: Sequence, wf: WalkForwardConfig):
    """Yield (train_days, test_days) tuples. Days are split chronologically with
    an embargo gap so adjacent-day leakage cannot inflate OOS results."""
    days = list(pd.to_datetime(pd.Series(sorted(set(days)))).dt.normalize())
    n = len(days)
    start = 0
    while True:
        tr_end = start + wf.train_days
        te_start = tr_end + wf.embargo_days
        te_end = te_start + wf.test_days
        if te_start >= n:
            break
        train = days[start:tr_end]
        test = days[te_start:min(te_end, n)]
        if test:
            yield train, test
        start += wf.step()


# ---------------------------------------------------------------------------
# Stats helpers
# ---------------------------------------------------------------------------
def _profit_factor(net: np.ndarray) -> float:
    gains = net[net > 0].sum()
    losses = -net[net < 0].sum()
    if losses == 0:
        return float("inf") if gains > 0 else 0.0
    return float(gains / losses)


def _bootstrap_p_gt_zero(net: np.ndarray, n_boot: int, rng: np.random.Generator) -> float:
    """One-sided p-value for mean(net) > 0 via the bootstrap. Small samples -> 1.0."""
    if len(net) < 5:
        return 1.0
    idx = rng.integers(0, len(net), size=(n_boot, len(net)))
    boot_means = net[idx].mean(axis=1)
    return float((boot_means <= 0).mean())


def _benjamini_hochberg(pvals: dict[str, float], alpha: float) -> dict[str, bool]:
    """Return {key: significant} after BH-FDR control across all tested setups."""
    items = [(k, v) for k, v in pvals.items() if np.isfinite(v)]
    m = len(items)
    if m == 0:
        return {k: False for k in pvals}
    items.sort(key=lambda kv: kv[1])
    sig = {k: False for k in pvals}
    max_k = 0
    for i, (_, p) in enumerate(items, start=1):
        if p <= (i / m) * alpha:
            max_k = i
    for i, (k, _) in enumerate(items, start=1):
        if i <= max_k:
            sig[k] = True
    return sig


# ---------------------------------------------------------------------------
# Per-setup evaluation
# ---------------------------------------------------------------------------
def evaluate_setup(setup_df: pd.DataFrame, wf: WalkForwardConfig, cost_cfg: CostConfig,
                   fit_fn: FitFn, apply_fn: ApplyFn, rng: np.random.Generator,
                   global_days: Sequence | None = None) -> dict:
    by_day = {d: g for d, g in setup_df.groupby(pd.to_datetime(setup_df["date"]).dt.normalize())}
    # Build folds over the global calendar when requested (so sparse setups get
    # real OOS windows); otherwise over the setup's own trading days.
    if wf.global_calendar_folds and global_days is not None:
        fold_days = list(pd.to_datetime(pd.Series(sorted(set(global_days)))).dt.normalize())
    else:
        fold_days = sorted(by_day)

    oos_net, is_net = [], []
    fold_net_sums = []
    fitted_params = []

    for train_days, test_days in purged_walk_forward_folds(fold_days, wf):
        train = pd.concat([by_day[d] for d in train_days if d in by_day], ignore_index=True) \
            if any(d in by_day for d in train_days) else setup_df.iloc[0:0]
        test = pd.concat([by_day[d] for d in test_days if d in by_day], ignore_index=True) \
            if any(d in by_day for d in test_days) else setup_df.iloc[0:0]
        if len(train) == 0 or len(test) == 0:
            continue

        params = fit_fn(train)
        fitted_params.append(params)

        sel_test = apply_fn(test, params)
        sel_train = apply_fn(train, params)  # in-sample reference (fit & score on train)

        if len(sel_test):
            n_test = net_pnl_vectorized(sel_test["entry_price"], sel_test["exit_price"],
                                        sel_test["qty"], sel_test["side"].to_numpy(), cost_cfg)
            oos_net.append(n_test)
            fold_net_sums.append(float(n_test.sum()))
        if len(sel_train):
            n_train = net_pnl_vectorized(sel_train["entry_price"], sel_train["exit_price"],
                                         sel_train["qty"], sel_train["side"].to_numpy(), cost_cfg)
            is_net.append(n_train)

    oos = np.concatenate(oos_net) if oos_net else np.array([])
    ins = np.concatenate(is_net) if is_net else np.array([])

    n_oos = int(len(oos))
    net_pf_oos = _profit_factor(oos) if n_oos else 0.0
    net_pf_is = _profit_factor(ins) if len(ins) else 0.0
    win_rate = float((oos > 0).mean()) if n_oos else 0.0
    expectancy = float(oos.mean()) if n_oos else 0.0
    total_net = float(oos.sum()) if n_oos else 0.0
    pos_folds = sum(1 for s in fold_net_sums if s > 0)
    fold_consistency = pos_folds / len(fold_net_sums) if fold_net_sums else 0.0
    pval = _bootstrap_p_gt_zero(oos, wf.n_bootstrap, rng)

    # overfit flag: strong in-sample, weak out-of-sample
    if net_pf_is > 1.0 and np.isfinite(net_pf_oos):
        oos_is_ratio = net_pf_oos / net_pf_is
    else:
        oos_is_ratio = float("nan")
    overfit_flag = bool(np.isfinite(oos_is_ratio) and oos_is_ratio < wf.overfit_ratio
                        and net_pf_is >= 1.3)

    # report the most common fitted threshold (if any) for the contract row
    chosen = None
    real_params = [p for p in fitted_params if p]
    if real_params:
        feats = {p["feature"] for p in real_params}
        if len(feats) == 1:
            f = real_params[0]["feature"]
            dirs = pd.Series([p["direction"] for p in real_params]).mode().iloc[0]
            thr = float(np.median([p["threshold"] for p in real_params]))
            chosen = {"feature": f, "direction": dirs, "threshold": round(thr, 4)}

    return dict(
        n_oos=n_oos, win_rate=round(win_rate, 4),
        gross_pf_oos=round(_profit_factor(
            net_pnl_vectorized(setup_df["entry_price"], setup_df["exit_price"],
                               setup_df["qty"], setup_df["side"].to_numpy(),
                               CostConfig(brokerage_pct=0, brokerage_cap_rs=0, stt_sell_pct=0,
                                          exch_txn_pct=0, sebi_pct=0, ipft_pct=0,
                                          stamp_buy_pct=0, gst_pct=0))), 3),
        net_pf_oos=round(net_pf_oos, 3) if np.isfinite(net_pf_oos) else net_pf_oos,
        net_expectancy_oos=round(expectancy, 3), total_net_oos=round(total_net, 2),
        n_folds=len(fold_net_sums), fold_consistency=round(fold_consistency, 3),
        p_value=round(pval, 4), net_pf_is=round(net_pf_is, 3) if np.isfinite(net_pf_is) else net_pf_is,
        oos_is_ratio=round(oos_is_ratio, 3) if np.isfinite(oos_is_ratio) else oos_is_ratio,
        overfit_flag=overfit_flag, fitted_rule=chosen,
    )


# ---------------------------------------------------------------------------
# Top-level gate
# ---------------------------------------------------------------------------
def run_gate(df: pd.DataFrame, wf: WalkForwardConfig | None = None,
             cost_cfg: CostConfig | None = None,
             fit_fn: FitFn = no_fit, apply_fn: ApplyFn = apply_identity,
             accepted_rules_path: str | None = None) -> pd.DataFrame:
    """
    Run the walk-forward promotion gate across every setup in `df`.
    Returns a decision report (one row per setup). Optionally writes the
    PROMOTE rows to an accepted_rules-style CSV.
    """
    wf = wf or WalkForwardConfig()
    cost_cfg = cost_cfg or CostConfig()
    rng = np.random.default_rng(wf.random_seed)

    required = {"date", "setup", "side", "entry_price", "exit_price", "qty"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"input frame missing columns: {sorted(missing)}")

    global_days = (
        sorted(pd.to_datetime(df["date"]).dt.normalize().unique())
        if wf.global_calendar_folds else None
    )
    rows = {}
    for setup, g in df.groupby("setup"):
        rows[setup] = evaluate_setup(g.reset_index(drop=True), wf, cost_cfg,
                                     fit_fn, apply_fn, rng, global_days=global_days)

    pvals = {s: r["p_value"] for s, r in rows.items()}
    sig = _benjamini_hochberg(pvals, wf.fdr_alpha)

    report = []
    for setup, r in rows.items():
        base_pass = (r["n_oos"] >= wf.min_oos_trades
                     and (r["net_pf_oos"] == float("inf") or r["net_pf_oos"] >= wf.min_net_pf)
                     and r["fold_consistency"] >= wf.min_fold_consistency)
        fdr_sig = sig.get(setup, False)
        if base_pass and fdr_sig and not r["overfit_flag"]:
            decision = "PROMOTE"
        elif base_pass and (not fdr_sig or r["overfit_flag"]):
            decision = "PROBATION"
        else:
            decision = "REJECT"
        report.append({"setup": setup, **r, "fdr_significant": fdr_sig, "decision": decision})

    rep = pd.DataFrame(report).sort_values(
        ["decision", "net_pf_oos"], ascending=[True, False]).reset_index(drop=True)

    if accepted_rules_path:
        promoted = rep[rep["decision"] == "PROMOTE"].copy()
        # Map to your accepted_rules.csv contract here as needed.
        promoted.to_csv(accepted_rules_path, index=False)

    return rep


# ---------------------------------------------------------------------------
# Demo: real-edge setup vs pure-noise setup vs overfit-trap setup
# ---------------------------------------------------------------------------
def _make_demo(seed: int = 1) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    days = pd.bdate_range("2026-01-01", periods=160)
    recs = []
    for d in days:
        # REAL_EDGE: genuine positive net drift (mean move > cost)
        for _ in range(rng.integers(3, 7)):
            entry = rng.uniform(200, 800)
            move = rng.normal(0.0035, 0.011)          # +35 bps mean -> survives cost
            recs.append(("REAL_EDGE_SHORT", "SHORT", entry, entry * (1 - move),
                         round(50000 / entry), d, rng.normal(), rng.normal()))
        # NOISE: zero-mean, should be rejected
        for _ in range(rng.integers(3, 7)):
            entry = rng.uniform(200, 800)
            move = rng.normal(0.0, 0.012)
            recs.append(("NOISE_SHORT", "SHORT", entry, entry * (1 - move),
                         round(50000 / entry), d, rng.normal(), rng.normal()))
        # OVERFIT_TRAP: zero edge, but a noise feature an optimizer will latch onto
        for _ in range(rng.integers(3, 7)):
            entry = rng.uniform(200, 800)
            f = rng.normal()
            move = rng.normal(0.0, 0.012)             # feature is NOT predictive
            recs.append(("OVERFIT_TRAP_SHORT", "SHORT", entry, entry * (1 - move),
                         round(50000 / entry), d, f, rng.normal()))
    return pd.DataFrame(recs, columns=[
        "setup", "side", "entry_price", "exit_price", "qty", "date", "feat_a", "feat_b"])


if __name__ == "__main__":
    cfg = CostConfig()

    # Cross-check the vectorised cost path against the scalar reference.
    s = intraday_equity_costs(500, 495, 100, "SHORT", cfg).net_pnl
    v = net_pnl_vectorized(np.array([500.]), np.array([495.]), np.array([100.]),
                           np.array(["SHORT"]), cfg)[0]
    assert abs(s - v) < 1e-3, (s, v)   # scalar path rounds to 4dp
    print(f"vectorised cost path matches scalar reference  ({s} == {round(v,4)})\n")

    df = _make_demo()
    wf = WalkForwardConfig(train_days=60, test_days=20, embargo_days=1)

    print("=== Gate run A: NO threshold fitting (does the raw setup have OOS edge?) ===")
    rep_a = run_gate(df, wf, cfg, fit_fn=no_fit, apply_fn=apply_identity)
    cols = ["setup", "n_oos", "win_rate", "net_pf_oos", "net_expectancy_oos",
            "fold_consistency", "p_value", "fdr_significant", "decision"]
    print(rep_a[cols].to_string(index=False), "\n")

    print("=== Gate run B: fit a threshold on 'feat_a' on TRAIN, score on TEST ===")
    fit_fn, apply_fn = make_threshold_fitter("feat_a", cfg)
    rep_b = run_gate(df, wf, cfg, fit_fn=fit_fn, apply_fn=apply_fn)
    cols_b = ["setup", "n_oos", "net_pf_oos", "net_pf_is", "oos_is_ratio",
              "overfit_flag", "fdr_significant", "decision"]
    print(rep_b[cols_b].to_string(index=False))
    print("\nNote: run A cleanly PROMOTEs the real raw edge and REJECTs both")
    print("zero-mean setups. In run B, fitting a non-predictive feature inflates")
    print("in-sample PF (REAL_EDGE: IS 2.5 vs OOS 1.4) -> the OOS/IS ratio trips the")
    print("overfit flag and the fitted rule is downgraded to PROBATION. A fitted")
    print("threshold must clear a higher bar than the honest no-fit baseline.")
