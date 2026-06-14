"""
nse_intraday_costs.py
=====================

Statutory + brokerage cost model for NSE intraday EQUITY trades (Zerodha-style
discount broker), as of 2026. Single responsibility: turn a resolved trade
(entry, exit, qty, side) into a rupee cost breakdown and a net P&L.

Why this exists
---------------
The V7 paper executor fills at `ltp_on_signal` and reports a GROSS P&L. For a
strategy whose targets are 0.6%-1.5%, costs are 10-25% of the gross edge per
trade, and several setups go negative once costs are applied. Nothing about the
edge can be trusted until the headline number is NET.

Rate sources / dates
--------------------
All rates are configurable in `CostConfig` and dated. Reconcile against a live
Zerodha contract note quarterly; SEBI "true-to-label" circulars and Union
Budgets change these. Defaults below reflect the 2026 NSE cash/intraday regime:

  - Brokerage:        min(Rs 20, 0.03% of turnover) per executed order (per leg)
  - STT:              0.025% on the SELL-side turnover ONLY (asymmetric)
  - Exch txn (NSE):   0.00297% of turnover (both legs)
  - SEBI turnover:    Rs 10 per crore  = 0.0001% (both legs)
  - NSE IPFT:         Rs 10 per crore  = 0.0001% (both legs)
  - Stamp duty:       0.003% on the BUY-side turnover ONLY (intraday/non-delivery)
  - GST:              18% on (brokerage + exch txn + SEBI + IPFT). NOT on STT/stamp.
  - DP charges:       N/A for intraday (only on delivery sells).

Asymmetry that matters for a short-focused book
-----------------------------------------------
STT is on the SELL leg, stamp duty on the BUY leg. For a LONG the sell is the
exit; for a SHORT the sell is the ENTRY. The model handles this per `side`, so a
short and a long of identical size do NOT incur identical cost timing/profile.
"""

from __future__ import annotations

from dataclasses import dataclass, asdict
from typing import Literal

Side = Literal["LONG", "SHORT"]


@dataclass(frozen=True)
class CostConfig:
    """All rates as plain decimals (0.0003 == 0.03%). Edit + re-date here."""
    rates_as_of: str = "2026-06"                 # provenance stamp
    brokerage_pct: float = 0.0003                # 0.03% per leg
    brokerage_cap_rs: float = 20.0               # min(cap, pct*turnover) per leg
    stt_sell_pct: float = 0.00025                # 0.025% sell side only
    exch_txn_pct: float = 0.0000297              # NSE cash 0.00297% both legs
    sebi_pct: float = 0.000001                   # Rs 10 / crore
    ipft_pct: float = 0.000001                   # NSE IPFT Rs 10 / crore
    stamp_buy_pct: float = 0.00003               # 0.003% buy side only (intraday)
    gst_pct: float = 0.18                        # on brokerage + exch + sebi + ipft


@dataclass(frozen=True)
class CostBreakdown:
    side: Side
    gross_pnl: float
    brokerage: float
    stt: float
    exch_txn: float
    sebi: float
    ipft: float
    stamp: float
    gst: float
    total_cost: float
    net_pnl: float
    cost_bps_of_turnover: float   # round-trip cost as bps of (buy+sell) notional
    cost_pct_of_entry: float      # round-trip cost as % of ENTRY notional (single side)

    def as_dict(self) -> dict:
        return asdict(self)


def _brokerage_one_leg(turnover: float, cfg: CostConfig) -> float:
    return min(cfg.brokerage_cap_rs, cfg.brokerage_pct * turnover)


def intraday_equity_costs(
    entry_price: float,
    exit_price: float,
    qty: float,
    side: Side,
    cfg: CostConfig | None = None,
) -> CostBreakdown:
    """
    Compute the full cost breakdown and net P&L for one resolved intraday trade.

    `qty` is share count (not notional). For a LONG, buy@entry / sell@exit;
    for a SHORT, sell@entry / buy@exit. Costs are charged on turnover regardless
    of whether the trade won or lost.
    """
    cfg = cfg or CostConfig()
    if side not in ("LONG", "SHORT"):
        raise ValueError(f"side must be LONG or SHORT, got {side!r}")
    if qty <= 0:
        raise ValueError("qty must be positive")

    entry_val = entry_price * qty
    exit_val = exit_price * qty

    if side == "LONG":
        buy_value, sell_value = entry_val, exit_val
        gross_pnl = (exit_price - entry_price) * qty
    else:  # SHORT
        buy_value, sell_value = exit_val, entry_val
        gross_pnl = (entry_price - exit_price) * qty

    turnover = buy_value + sell_value

    brokerage = _brokerage_one_leg(buy_value, cfg) + _brokerage_one_leg(sell_value, cfg)
    stt = cfg.stt_sell_pct * sell_value
    exch_txn = cfg.exch_txn_pct * turnover
    sebi = cfg.sebi_pct * turnover
    ipft = cfg.ipft_pct * turnover
    stamp = cfg.stamp_buy_pct * buy_value
    gst = cfg.gst_pct * (brokerage + exch_txn + sebi + ipft)

    total_cost = brokerage + stt + exch_txn + sebi + ipft + stamp + gst
    net_pnl = gross_pnl - total_cost
    cost_bps = (total_cost / turnover) * 1e4 if turnover else 0.0
    entry_notional = entry_price * qty
    cost_pct_entry = (total_cost / entry_notional) * 100 if entry_notional else 0.0

    return CostBreakdown(
        side=side, gross_pnl=round(gross_pnl, 4),
        brokerage=round(brokerage, 4), stt=round(stt, 4), exch_txn=round(exch_txn, 4),
        sebi=round(sebi, 4), ipft=round(ipft, 4), stamp=round(stamp, 4), gst=round(gst, 4),
        total_cost=round(total_cost, 4), net_pnl=round(net_pnl, 4),
        cost_bps_of_turnover=round(cost_bps, 3), cost_pct_of_entry=round(cost_pct_entry, 4),
    )


def net_pnl(entry_price: float, exit_price: float, qty: float, side: Side,
            cfg: CostConfig | None = None) -> float:
    """Convenience: just the net rupee P&L."""
    return intraday_equity_costs(entry_price, exit_price, qty, side, cfg).net_pnl


def breakeven_win_rate(sl_pct: float, target_pct: float,
                       roundtrip_cost_pct: float = 0.0) -> float:
    """
    Breakeven win rate for a fixed SL%/Target% setup, optionally net of a
    round-trip cost expressed as % of notional.

        p* = (SL + cost) / (SL + Target)

    Pass sl/target/cost as decimals OR all as percent points consistently.
    """
    return (sl_pct + roundtrip_cost_pct) / (sl_pct + target_pct)


# ---------------------------------------------------------------------------
# Pandas helper for batch-costing a trades dataframe (e.g. paper_trades_*.csv)
# ---------------------------------------------------------------------------
def cost_trades_frame(df, cfg: CostConfig | None = None,
                      entry_col="entry_price", exit_col="exit_price",
                      qty_col="qty", side_col="side"):
    """
    Add cost columns to a trades DataFrame in place-ish (returns a new frame).
    Expects columns: entry_price, exit_price, qty, side  (rename via kwargs).
    Adds: gross_pnl, total_cost, net_pnl, cost_bps_of_turnover.
    """
    import pandas as pd  # local import keeps the core module dependency-free

    cfg = cfg or CostConfig()
    recs = []
    for _, r in df.iterrows():
        b = intraday_equity_costs(
            float(r[entry_col]), float(r[exit_col]), float(r[qty_col]),
            str(r[side_col]).upper(), cfg,
        )
        recs.append({
            "gross_pnl": b.gross_pnl, "total_cost": b.total_cost,
            "net_pnl": b.net_pnl, "cost_bps_of_turnover": b.cost_bps_of_turnover,
        })
    out = df.copy()
    cost_df = pd.DataFrame(recs, index=df.index)
    for c in cost_df.columns:
        out[c] = cost_df[c]
    return out


# ---------------------------------------------------------------------------
# Demo / self-check
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    cfg = CostConfig()
    print(f"Cost rates as of {cfg.rates_as_of}\n")

    # A realistic V7-style trade: ~Rs 50k notional (Rs 10k margin x ~5x MIS),
    # short setup hitting a 1.0% target on a Rs 500 stock.
    entry, target_pct = 500.0, 0.01
    qty = round(50_000 / entry)               # 100 shares -> ~Rs 50k notional
    exit_win = entry * (1 - target_pct)       # short wins as price falls
    b = intraday_equity_costs(entry, exit_win, qty, "SHORT", cfg)
    print("SHORT, Rs ~50k notional, +1.0% target hit:")
    for k, v in b.as_dict().items():
        print(f"  {k:>24}: {v}")
    print(f"  -> costs ate {b.total_cost / b.gross_pnl * 100:.1f}% of gross profit\n")

    # Show how the cost drag re-rates the tight-target setups.
    # Cost must be expressed as % of ENTRY notional to match SL%/Target% units.
    cost_pct = b.cost_pct_of_entry
    print("Breakeven win rate (gross vs net @ a representative cost):")
    print(f"  (using round-trip cost = {cost_pct:.3f}% of entry notional)\n")
    setups = [
        ("E_VWAP_BAND_FADE", 0.70, 0.60),
        ("L_BB_SQUEEZE_LONG", 0.75, 0.75),
        ("A_PULLBACK_C2_LOW", 0.85, 1.00),
        ("Fallback 0.75/1.00", 0.75, 1.00),
        ("A_MOD_BREAK_C1_LOW", 0.70, 1.50),
    ]
    print(f"  {'setup':<22}{'gross BE%':>10}{'net BE%':>10}")
    for name, sl, tgt in setups:
        g = breakeven_win_rate(sl, tgt) * 100
        n = breakeven_win_rate(sl, tgt, cost_pct) * 100
        print(f"  {name:<22}{g:>9.1f}%{n:>9.1f}%")
