"""v17D Step 3.5 — volatility-targeted position sizing.

Replaces fixed Rs-notional × multiplier sizing with **per-trade-Rs-risk
constant** sizing:

    qty = target_risk_rs / (entry_price × SL%)

This means a 0.75% SL on a Rs 500 stock and a 1.50% SL on a Rs 200 stock
both risk the same Rs amount when stopped out — equalizing risk exposure
across trades regardless of underlying volatility.

Combined with the v17C size_mult tier, the final position is:

    notional_rs = qty × entry_price × size_mult

Public API:
    qty_for_risk(entry_price, sl_pct, target_risk_rs) -> int
    size_position(entry_price, sl_pct, target_risk_rs, size_mult,
                   max_notional_rs=None) -> dict
        Returns {qty, notional_rs, risk_rs, mult_applied}
    risk_normalize_existing_csv(df, target_risk_rs) -> df
        Re-resize trades in an existing CSV; returns df with new
        position_size_rs and recomputed pnl_rs.
"""
from __future__ import annotations

import pandas as pd


def qty_for_risk(
    entry_price: float, sl_pct: float, target_risk_rs: float,
) -> int:
    """Shares such that an SL hit equals exactly target_risk_rs."""
    if entry_price <= 0 or sl_pct <= 0 or target_risk_rs <= 0:
        return 0
    risk_per_share_rs = entry_price * sl_pct / 100.0
    if risk_per_share_rs <= 0:
        return 0
    return int(target_risk_rs / risk_per_share_rs)


def size_position(
    entry_price: float,
    sl_pct: float,
    target_risk_rs: float,
    size_mult: float = 1.0,
    *,
    max_notional_rs: float | None = None,
) -> dict:
    """Compute final qty and notional with vol-targeting + tier multiplier.

    Returns dict with: qty, notional_rs, risk_rs, mult_applied, capped_by_max
    """
    base_qty = qty_for_risk(entry_price, sl_pct, target_risk_rs)
    qty = int(base_qty * size_mult)
    notional = qty * entry_price
    capped = False
    if max_notional_rs is not None and notional > max_notional_rs:
        qty = int(max_notional_rs / entry_price)
        notional = qty * entry_price
        capped = True
    risk = qty * entry_price * sl_pct / 100.0
    return {
        "qty": qty,
        "notional_rs": notional,
        "risk_rs": risk,
        "mult_applied": size_mult,
        "capped_by_max": capped,
    }


def risk_normalize_existing_csv(
    df: pd.DataFrame, target_risk_rs: float = 150.0,
) -> pd.DataFrame:
    """Re-size each row in a v17D trades CSV to constant per-trade Rs risk.

    Recomputes:
        position_size_rs = qty × entry_price
        pnl_rs           = pnl_pct × position_size_rs / 100

    Requires columns: entry_price, sl_pct, pnl_pct.
    Preserves all other columns.
    """
    out = df.copy()
    qtys, notionals, risks, pnl_rs_new = [], [], [], []
    for _, r in out.iterrows():
        ep = float(r.get("entry_price", 0) or 0)
        sl = float(r.get("sl_pct", 0) or 0)
        size_mult = float(r.get("size_mult", 1.0) or 1.0)
        sized = size_position(ep, sl, target_risk_rs, size_mult)
        qtys.append(sized["qty"])
        notionals.append(sized["notional_rs"])
        risks.append(sized["risk_rs"])
        pnl_pct = float(r.get("pnl_pct", 0) or 0)
        pnl_rs_new.append(pnl_pct * sized["notional_rs"] / 100.0)
    out["qty"] = qtys
    out["position_size_rs"] = notionals
    out["risk_rs"] = risks
    out["pnl_rs"] = pnl_rs_new
    return out


if __name__ == "__main__":
    # Compare fixed-notional vs vol-targeted sizing for two stocks
    target_risk = 150.0   # Rs 150 per trade

    # Stock A: Rs 500, 1% SL  -> per-share risk Rs 5
    sized_a = size_position(500.0, 1.0, target_risk, size_mult=1.0)
    print(f"Stock A (Rs500, 1%SL):  qty={sized_a['qty']:>3d}  "
          f"notional=Rs{sized_a['notional_rs']:>7,.0f}  risk=Rs{sized_a['risk_rs']:>5.0f}")

    # Stock B: Rs 100, 2% SL  -> per-share risk Rs 2
    sized_b = size_position(100.0, 2.0, target_risk, size_mult=1.0)
    print(f"Stock B (Rs100, 2%SL):  qty={sized_b['qty']:>3d}  "
          f"notional=Rs{sized_b['notional_rs']:>7,.0f}  risk=Rs{sized_b['risk_rs']:>5.0f}")

    # Stock C: high-vol, 3% SL -> qty smaller, same risk
    sized_c = size_position(500.0, 3.0, target_risk, size_mult=1.30)
    print(f"Stock C (Rs500, 3%SL, 1.3x mult):  qty={sized_c['qty']:>3d}  "
          f"notional=Rs{sized_c['notional_rs']:>7,.0f}  risk=Rs{sized_c['risk_rs']:>5.0f}")

    assert abs(sized_a["risk_rs"] - target_risk) < 5  # within rounding
    assert abs(sized_b["risk_rs"] - target_risk) < 5
    print("\nVol-targeted sizing equalizes per-trade risk across stocks.")
