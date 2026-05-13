"""v17D universe filter — pre-scan eligibility gate.

Filters out stocks that should not be traded regardless of signal quality:
    - illiquid (low ADV → poor fills, wide spreads)
    - too cheap (penny stocks have circuit-limit and quote-stuffing risk)
    - too expensive (capital-inefficient at retail size)
    - non-F&O (proxy for sufficient institutional liquidity)
    - circuit-limit-restricted (5% / 10% / 20% bands)

Public API:
    is_eligible(ticker, last_close, adv_rs_cr, fo_set=None,
                circuit_band_pct=None) -> EligibilityResult
    load_fo_list(path=None) -> set[str]
    filter_universe(stocks_df) -> stocks_df  (vectorized; pandas DataFrame in)

A stock missing F&O membership data is treated as INELIGIBLE (fail-safe).
A stock missing ADV is treated as INELIGIBLE.
A stock missing price is treated as INELIGIBLE.

Doctest:
    >>> r = is_eligible("RELIANCE", 2500.0, 1500.0, fo_set={"RELIANCE"})
    >>> r.eligible
    True
    >>> is_eligible("PENNY", 25.0, 100.0, fo_set={"PENNY"}).eligible
    False
    >>> is_eligible("ILLIQ", 500.0, 10.0, fo_set={"ILLIQ"}).eligible
    False
    >>> is_eligible("NON_FO", 500.0, 200.0, fo_set=set()).eligible
    False
    >>> is_eligible("CIRCUIT", 500.0, 200.0, fo_set={"CIRCUIT"}, circuit_band_pct=10.0).eligible
    False
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

PRICE_FLOOR_RS = 50.0
PRICE_CAP_RS = 5000.0
ADV_FLOOR_RS_CR = 50.0
DEFAULT_FO_LIST_PATH = Path(__file__).parent / "configs" / "fo_list.json"


@dataclass(frozen=True)
class EligibilityResult:
    eligible: bool
    reason: str           # one of: "ELIGIBLE", "PRICE_FLOOR", "PRICE_CAP",
                          # "ADV_FLOOR", "NOT_FO", "CIRCUIT_LIMITED",
                          # "MISSING_DATA"
    detail: str = ""

    def __bool__(self) -> bool:
        return self.eligible


def is_eligible(
    ticker: str,
    last_close: float | None,
    adv_rs_cr: float | None,
    fo_set: set | None = None,
    circuit_band_pct: float | None = None,
) -> EligibilityResult:
    """Return whether a ticker passes the v17D universe filter.

    Args:
        ticker: NSE symbol (e.g. "RELIANCE").
        last_close: yesterday's close in Rs. None → ineligible.
        adv_rs_cr: 20-day average daily volume in Rs crore. None → ineligible.
        fo_set: set of F&O-eligible tickers. None → no F&O check (TEST ONLY).
        circuit_band_pct: today's circuit band (5/10/20). None → no circuit check.
    """
    if last_close is None:
        return EligibilityResult(False, "MISSING_DATA", "last_close is None")
    if adv_rs_cr is None:
        return EligibilityResult(False, "MISSING_DATA", "adv_rs_cr is None")

    if last_close < PRICE_FLOOR_RS:
        return EligibilityResult(
            False, "PRICE_FLOOR",
            f"price Rs {last_close:.2f} < floor Rs {PRICE_FLOOR_RS:.0f}",
        )
    if last_close > PRICE_CAP_RS:
        return EligibilityResult(
            False, "PRICE_CAP",
            f"price Rs {last_close:.2f} > cap Rs {PRICE_CAP_RS:.0f}",
        )

    if adv_rs_cr < ADV_FLOOR_RS_CR:
        return EligibilityResult(
            False, "ADV_FLOOR",
            f"ADV Rs {adv_rs_cr:.1f} cr < floor Rs {ADV_FLOOR_RS_CR:.0f} cr",
        )

    if fo_set is not None and ticker not in fo_set:
        return EligibilityResult(False, "NOT_FO", f"{ticker} not in F&O list")

    if circuit_band_pct is not None and circuit_band_pct <= 10.0:
        return EligibilityResult(
            False, "CIRCUIT_LIMITED",
            f"circuit band {circuit_band_pct:.0f}% restricts intraday range",
        )

    return EligibilityResult(True, "ELIGIBLE")


def load_fo_list(path: str | Path | None = None) -> set:
    """Load NSE F&O symbol list from JSON file. Returns set of symbols."""
    p = Path(path) if path else DEFAULT_FO_LIST_PATH
    if not p.exists():
        return set()
    with open(p, "r", encoding="utf-8") as f:
        data = json.load(f)
    if isinstance(data, dict) and "symbols" in data:
        return set(data["symbols"])
    if isinstance(data, list):
        return set(data)
    raise ValueError(f"unexpected F&O list format in {p}")


def filter_universe(stocks_df, fo_set=None):
    """Vectorized filter for a pandas DataFrame.

    Expected columns: ticker, last_close, adv_rs_cr (and optionally
    circuit_band_pct). Returns the filtered DataFrame plus a 'reason'
    column indicating why each row was kept or dropped.
    """
    import pandas as pd  # local import; module usable without pandas

    out = stocks_df.copy()
    out["eligibility_reason"] = "UNCHECKED"
    out["eligible"] = False

    for idx, row in out.iterrows():
        result = is_eligible(
            ticker=row.get("ticker", ""),
            last_close=row.get("last_close"),
            adv_rs_cr=row.get("adv_rs_cr"),
            fo_set=fo_set,
            circuit_band_pct=row.get("circuit_band_pct"),
        )
        out.at[idx, "eligibility_reason"] = result.reason
        out.at[idx, "eligible"] = result.eligible

    return out


if __name__ == "__main__":
    import doctest
    doctest.testmod(verbose=True)
