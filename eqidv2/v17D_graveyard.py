"""v17D Step 4.8 — setup graveyard auto-append.

Helper for the rolling drop log at eqidv2/SETUP_GRAVEYARD.md.
Each call appends one row to the markdown table; safe to call from any
runner code that auto-disables a setup (G6 kill-PF trip, drift trip,
manual disable).

Drop-reason taxonomy (mirrors roadmap):
    OVERFIT       IS PF good, OOS PF collapses
    COSTS         PF degrades < 1.40 under realistic costs
    NO_EDGE       Raw PF < 1.30, no filter raises it
    CORRELATION   Jaccard >= 0.5 with existing setup
    LIVE_DEGRADE  Live PF < 1.40 over 60-day pilot
    DRIFT         KS test fails on >=2 features for 3 consecutive days
    LOW_FREQ      n < 30 in 60-day window (statistically unreliable)
    OPERATIONAL   Detection too brittle, false-positive rate high

Public API:
    append_drop(setup_id, phase, reason, n, pf, win_pct, oos_pf=None,
                notes="", revival_condition="", path=None)
    list_graveyard(path=None) -> list of dicts
"""
from __future__ import annotations

from datetime import date as date_type
from pathlib import Path

DEFAULT_PATH = Path(__file__).parent / "SETUP_GRAVEYARD.md"

VALID_REASONS = (
    "OVERFIT", "COSTS", "NO_EDGE", "CORRELATION",
    "LIVE_DEGRADE", "DRIFT", "LOW_FREQ", "OPERATIONAL",
)

HEADER = """# Setup Graveyard

Drop log for v17D setups. Auto-appended by `v17D_graveyard.append_drop()`
and reviewed quarterly per Phase 4.8 of the roadmap.

| Setup ID | Date dropped | Phase | Reason | n | PF | Win % | OOS PF | Revival condition | Notes |
|---|---|---|---|---|---|---|---|---|---|
"""


def _ensure_file(path: Path) -> None:
    if not path.exists():
        path.write_text(HEADER, encoding="utf-8")


def append_drop(
    setup_id: str,
    phase: str,
    reason: str,
    n: int,
    pf: float,
    win_pct: float = float("nan"),
    oos_pf: float | None = None,
    notes: str = "",
    revival_condition: str = "",
    path: str | Path | None = None,
    drop_date: date_type | None = None,
) -> None:
    if reason not in VALID_REASONS:
        raise ValueError(
            f"reason must be one of {VALID_REASONS}, got {reason!r}"
        )
    p = Path(path) if path else DEFAULT_PATH
    _ensure_file(p)
    d = drop_date or date_type.today()
    row = (
        f"| {setup_id} | {d.isoformat()} | {phase} | {reason} | "
        f"{int(n)} | {pf:.2f} | "
        f"{('—' if win_pct != win_pct else f'{win_pct:.1f}%')} | "
        f"{('—' if oos_pf is None else f'{oos_pf:.2f}')} | "
        f"{revival_condition or '—'} | "
        f"{notes or '—'} |\n"
    )
    with open(p, "a", encoding="utf-8") as f:
        f.write(row)


def list_graveyard(path: str | Path | None = None) -> list:
    """Return list of {setup_id, drop_date, phase, reason, ...} dicts."""
    p = Path(path) if path else DEFAULT_PATH
    if not p.exists():
        return []
    out = []
    for line in p.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line.startswith("|") or line.startswith("| Setup ID") or line.startswith("|---"):
            continue
        cells = [c.strip() for c in line.strip("|").split("|")]
        if len(cells) < 10:
            continue
        out.append({
            "setup_id": cells[0],
            "drop_date": cells[1],
            "phase": cells[2],
            "reason": cells[3],
            "n": cells[4],
            "pf": cells[5],
            "win_pct": cells[6],
            "oos_pf": cells[7],
            "revival_condition": cells[8],
            "notes": cells[9],
        })
    return out


if __name__ == "__main__":
    # Smoke test
    import tempfile
    tmp = Path(tempfile.mkdtemp()) / "test_graveyard.md"
    append_drop(
        "TC_3_HH_HL_MOMENTUM", "Phase 0.2", "OVERFIT",
        n=45, pf=1.62, win_pct=58.0, oos_pf=0.95,
        notes="Threshold perturbation collapsed at -10%",
        revival_condition="Re-test after Q3 regime shift",
        path=tmp,
    )
    append_drop(
        "MR_4_THREE_BAR_DRIVE", "Phase 0.5", "CORRELATION",
        n=320, pf=1.45, oos_pf=1.30,
        notes="Jaccard 0.62 with VO_1_CLIMAX_REVERSAL",
        path=tmp,
    )
    rows = list_graveyard(tmp)
    print(f"Graveyard rows: {len(rows)}")
    for r in rows:
        print(f"  {r['setup_id']:<32s}  {r['drop_date']}  {r['reason']}")
    print(tmp.read_text())
