"""v17D pre-entry check — single composed gate before every signal goes live.

Plumbs together universe filter + kill-switch + logging so the runner can
call ONE function before placing an order.

Usage in a runner (Phase 4+):

    from eqidv2 import v17D_pre_entry_check as gate
    decision = gate.check(
        ticker=row["ticker"],
        last_close=row["last_close"],
        adv_rs_cr=row["adv_rs_cr"],
        fo_set=fo_set,
        intraday_pnl_pct=session_pnl,
        weekly_pnl_pct=rolling_5d_pnl,
        signal=row.to_dict(),
    )
    if not decision.allow:
        continue                # decision was already logged
    place_order(row)

Backtest mode: pass `log=False` to skip writing JSON streams, useful when
running large backtests where every signal would balloon log size.
"""
from __future__ import annotations

from dataclasses import dataclass

from eqidv2 import v17D_kill_switch as ks
from eqidv2 import v17D_logging as logger
from eqidv2 import v17D_universe_filter as uf


@dataclass(frozen=True)
class GateDecision:
    allow: bool
    block_reason: str = ""    # "" if allow=True
    detail: str = ""

    def __bool__(self) -> bool:
        return self.allow


def check(
    ticker: str,
    last_close: float | None,
    adv_rs_cr: float | None,
    fo_set: set | None,
    intraday_pnl_pct: float = 0.0,
    weekly_pnl_pct: float = 0.0,
    circuit_band_pct: float | None = None,
    signal: dict | None = None,
    log: bool = True,
) -> GateDecision:
    """Run the composed pre-entry gate. Logs the decision if log=True."""
    sig = signal or {}

    kill = ks.check_before_entry(
        intraday_pnl_pct=intraday_pnl_pct,
        weekly_pnl_pct=weekly_pnl_pct,
    )
    if kill.halted:
        decision = GateDecision(False, f"KILL_SWITCH:{kill.reason}", kill.detail)
        if log:
            logger.log_event("decisions", {
                **sig, "ticker": ticker, "decision": "DROP",
                "reason": decision.block_reason, "detail": decision.detail,
            })
        return decision

    elig = uf.is_eligible(
        ticker=ticker, last_close=last_close, adv_rs_cr=adv_rs_cr,
        fo_set=fo_set, circuit_band_pct=circuit_band_pct,
    )
    if not elig.eligible:
        decision = GateDecision(
            False, f"UNIVERSE:{elig.reason}", elig.detail,
        )
        if log:
            logger.log_event("decisions", {
                **sig, "ticker": ticker, "decision": "DROP",
                "reason": decision.block_reason, "detail": decision.detail,
            })
        return decision

    decision = GateDecision(True, "", "")
    if log:
        logger.log_event("decisions", {
            **sig, "ticker": ticker, "decision": "KEEP",
            "reason": "passed_pre_entry_gate",
        })
    return decision


if __name__ == "__main__":
    fo = {"RELIANCE"}
    print(check("RELIANCE", 2500.0, 1500.0, fo, log=False))
    print(check("PENNY", 25.0, 100.0, fo, log=False))
    print(check("RELIANCE", 2500.0, 1500.0, fo,
                intraday_pnl_pct=-3.0, log=False))
