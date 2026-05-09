"""v17D cost model — realistic Indian intraday equity round-trip costs.

Replaces the ad-hoc constants in v17C noNF:
    _E3_TGT_COSTS_PCT = 0.16%   (slip + comm both legs)
    _E3_SL_COSTS_PCT  = 0.19%   (+ extra stop slip)

Cost components (per leg unless noted):
    - Brokerage           min(Rs 20, 0.03% of turnover)     [Zerodha intraday]
    - STT                 0.025% on SELL leg only           [intraday equity]
    - NSE txn charges     0.00345% per leg
    - SEBI charges        0.0001% per leg
    - GST                 18% on (brokerage + NSE + SEBI)
    - Stamp duty          0.003% on BUY leg only            [Maharashtra; applies to NSE]
    - Slippage            ADV-bucketed; both legs (extra on stop)

ADV slippage assumptions (calibrated against typical NSE 5-min intraday fills):
    top100      0.05% per leg
    mid         0.10% per leg
    long_tail   0.20% per leg

Stop-out adds an extra 0.03% slippage on the stop leg (orders fill worse on
adverse moves).

Public API:
    round_trip_pct(entry_price, exit_price, side, adv_bucket, exit_kind)
        -> dict with breakdown and total_pct
    costs_pct_for_v17C(adv_bucket, exit_kind="TARGET") -> float
        drop-in replacement for v17C's _E3_TGT_COSTS_PCT / _E3_SL_COSTS_PCT
    slippage_pct_per_leg(adv_bucket, is_stop_leg=False) -> float
    adv_bucket_for(adv_rs_cr: float) -> str

Doctest:
    >>> round(costs_pct_for_v17C("mid", "TARGET"), 3)
    0.307
    >>> round(costs_pct_for_v17C("mid", "SL"), 3)
    0.337
    >>> adv_bucket_for(800.0)
    'top100'
    >>> adv_bucket_for(150.0)
    'mid'
    >>> adv_bucket_for(20.0)
    'long_tail'
"""
from __future__ import annotations

from dataclasses import dataclass

BROKERAGE_PCT = 0.0003          # 0.03%
BROKERAGE_CAP_RS = 20.0
STT_SELL_PCT = 0.00025          # 0.025% on sell leg
NSE_TXN_PCT = 0.0000345         # 0.00345% per leg
SEBI_PCT = 0.000001             # 0.0001% per leg
GST_PCT = 0.18                  # 18% on (brokerage + nse + sebi)
STAMP_DUTY_BUY_PCT = 0.00003    # 0.003% on buy leg

SLIPPAGE_PER_LEG = {
    "top100": 0.0005,           # 0.05%
    "mid": 0.0010,              # 0.10%
    "long_tail": 0.0020,        # 0.20%
}
EXTRA_STOP_SLIPPAGE = 0.0003    # 0.03% on adverse fill

ADV_TOP100_FLOOR_RS_CR = 500.0  # >= Rs 500 cr ADV
ADV_MID_FLOOR_RS_CR = 50.0      # >= Rs 50 cr ADV


def adv_bucket_for(adv_rs_cr: float) -> str:
    """Map a stock's 20-day ADV (in Rs crore) to a slippage bucket."""
    if adv_rs_cr >= ADV_TOP100_FLOOR_RS_CR:
        return "top100"
    if adv_rs_cr >= ADV_MID_FLOOR_RS_CR:
        return "mid"
    return "long_tail"


def slippage_pct_per_leg(adv_bucket: str, is_stop_leg: bool = False) -> float:
    """Per-leg slippage as a fraction of price (e.g. 0.0005 = 0.05%)."""
    base = SLIPPAGE_PER_LEG.get(adv_bucket, SLIPPAGE_PER_LEG["long_tail"])
    return base + (EXTRA_STOP_SLIPPAGE if is_stop_leg else 0.0)


@dataclass(frozen=True)
class CostBreakdown:
    brokerage_pct: float
    stt_pct: float
    nse_txn_pct: float
    sebi_pct: float
    gst_pct: float
    stamp_duty_pct: float
    slippage_pct: float
    total_pct: float

    def as_dict(self) -> dict:
        return {
            "brokerage_pct": self.brokerage_pct,
            "stt_pct": self.stt_pct,
            "nse_txn_pct": self.nse_txn_pct,
            "sebi_pct": self.sebi_pct,
            "gst_pct": self.gst_pct,
            "stamp_duty_pct": self.stamp_duty_pct,
            "slippage_pct": self.slippage_pct,
            "total_pct": self.total_pct,
        }


def _brokerage_pct_for_leg(turnover_rs: float) -> float:
    """Brokerage as a fraction of turnover, capped at Rs 20 per leg."""
    if turnover_rs <= 0:
        return 0.0
    raw = BROKERAGE_PCT
    capped_rs = min(raw * turnover_rs, BROKERAGE_CAP_RS)
    return capped_rs / turnover_rs


def round_trip_pct(
    entry_price: float,
    qty: int,
    side: str,
    adv_bucket: str,
    exit_kind: str = "TARGET",
) -> CostBreakdown:
    """Full round-trip cost as a percentage of entry price.

    Args:
        entry_price: per-share entry price in Rs.
        qty: number of shares (used only for brokerage cap calculation).
        side: "LONG" or "SHORT".
        adv_bucket: "top100" / "mid" / "long_tail".
        exit_kind: "TARGET" / "SL" / "EOD". SL uses extra slippage on stop leg.

    Returns:
        CostBreakdown with each component as percent (e.g. 0.16 means 0.16%)
        and total_pct as the sum.
    """
    side = side.upper()
    exit_kind = exit_kind.upper()
    if side not in ("LONG", "SHORT"):
        raise ValueError(f"side must be LONG or SHORT, got {side!r}")
    if exit_kind not in ("TARGET", "SL", "EOD"):
        raise ValueError(f"exit_kind must be TARGET/SL/EOD, got {exit_kind!r}")

    # Two legs of equal turnover (approx; exit price differs slightly).
    leg_turnover_rs = entry_price * max(int(qty), 1)
    brok_per_leg = _brokerage_pct_for_leg(leg_turnover_rs)

    brokerage_pct = brok_per_leg * 2 * 100
    stt_pct = STT_SELL_PCT * 100
    nse_pct = NSE_TXN_PCT * 2 * 100
    sebi_pct = SEBI_PCT * 2 * 100
    stamp_pct = STAMP_DUTY_BUY_PCT * 100
    gstable = brok_per_leg * 2 + NSE_TXN_PCT * 2 + SEBI_PCT * 2
    gst_pct = gstable * GST_PCT * 100

    slip_entry = slippage_pct_per_leg(adv_bucket, is_stop_leg=False)
    slip_exit = slippage_pct_per_leg(adv_bucket, is_stop_leg=(exit_kind == "SL"))
    slippage_pct = (slip_entry + slip_exit) * 100

    total_pct = (
        brokerage_pct + stt_pct + nse_pct + sebi_pct
        + gst_pct + stamp_pct + slippage_pct
    )
    return CostBreakdown(
        brokerage_pct=brokerage_pct,
        stt_pct=stt_pct,
        nse_txn_pct=nse_pct,
        sebi_pct=sebi_pct,
        gst_pct=gst_pct,
        stamp_duty_pct=stamp_pct,
        slippage_pct=slippage_pct,
        total_pct=total_pct,
    )


def costs_pct_for_v17C(adv_bucket: str = "mid", exit_kind: str = "TARGET") -> float:
    """Drop-in replacement for v17C's _E3_TGT_COSTS_PCT / _E3_SL_COSTS_PCT.

    Uses a representative entry_price=Rs 500 and qty=40 (Rs 20K notional)
    to derive the brokerage component. For Cand-E sized positions this is
    within 1bp of the per-trade actual.
    """
    bd = round_trip_pct(
        entry_price=500.0, qty=40, side="LONG",
        adv_bucket=adv_bucket, exit_kind=exit_kind,
    )
    return bd.total_pct


if __name__ == "__main__":
    import doctest
    doctest.testmod(verbose=True)
    print()
    print("Per-bucket round-trip costs (entry=Rs 500, qty=40):")
    for bucket in ("top100", "mid", "long_tail"):
        for exit_kind in ("TARGET", "SL", "EOD"):
            pct = costs_pct_for_v17C(bucket, exit_kind)
            print(f"  {bucket:10s} {exit_kind:7s}  {pct:.3f}%")
