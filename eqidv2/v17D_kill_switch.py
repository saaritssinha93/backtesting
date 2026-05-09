"""v17D kill-switch infrastructure — capital protection circuit breakers.

Five independent halt conditions, checked before every new entry:

    1. Env var V17D_KILL_SWITCH=1            (manual immediate halt)
    2. File ~/v17D_kill exists                (manual halt, persists across runs)
    3. Daily DD: intraday PnL <= -2.5% capital
    4. Weekly DD: 5-day rolling PnL <= -5% capital, requires manual re-arm
    5. Per-setup G6 rolling-PF kill (handled in v17C governors, see notes)

Daily DD halts new entries but leaves open positions managed by their SLs.
Weekly DD halts the next trading session entirely; user must `touch ~/v17D_rearm`
to resume.

Public API:
    check_before_entry(intraday_pnl_pct, weekly_pnl_pct, capital_rs=None)
        -> KillStatus  (also tested as bool)
    arm_weekly_kill(reason: str)
    disarm_weekly_kill_if_armed() -> bool   (checks for ~/v17D_rearm file)
    is_armed() -> bool
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

ENV_KILL_VAR = "V17D_KILL_SWITCH"
KILL_FILE = Path.home() / "v17D_kill"
WEEKLY_KILL_FILE = Path.home() / "v17D_weekly_kill"
REARM_FILE = Path.home() / "v17D_rearm"

DAILY_DD_KILL_PCT = -2.5         # halt if intraday PnL% <= -2.5%
WEEKLY_DD_KILL_PCT = -5.0        # arm weekly kill if 5-day rolling <= -5%


@dataclass(frozen=True)
class KillStatus:
    halted: bool
    reason: str         # ENV_VAR / FILE / DAILY_DD / WEEKLY_DD / OK
    detail: str = ""

    def __bool__(self) -> bool:
        return self.halted


def _env_kill_active() -> bool:
    val = os.environ.get(ENV_KILL_VAR, "").strip().lower()
    return val in ("1", "true", "yes", "on")


def _file_kill_active() -> bool:
    return KILL_FILE.exists()


def _weekly_kill_active() -> bool:
    """Weekly kill is set by arm_weekly_kill() and cleared by re-arm."""
    if not WEEKLY_KILL_FILE.exists():
        return False
    if REARM_FILE.exists():
        try:
            WEEKLY_KILL_FILE.unlink()
            REARM_FILE.unlink()
        except OSError:
            return True
        return False
    return True


def check_before_entry(
    intraday_pnl_pct: float = 0.0,
    weekly_pnl_pct: float = 0.0,
    capital_rs: float | None = None,
) -> KillStatus:
    """Check all kill conditions. Return KillStatus(halted=True) if any fires."""
    if _env_kill_active():
        return KillStatus(True, "ENV_VAR", f"{ENV_KILL_VAR}=1 in environment")

    if _file_kill_active():
        return KillStatus(True, "FILE", f"{KILL_FILE} exists")

    if _weekly_kill_active():
        return KillStatus(
            True, "WEEKLY_DD",
            f"weekly DD kill armed (file: {WEEKLY_KILL_FILE}); "
            f"touch {REARM_FILE} to resume",
        )

    if intraday_pnl_pct <= DAILY_DD_KILL_PCT:
        return KillStatus(
            True, "DAILY_DD",
            f"intraday PnL {intraday_pnl_pct:.2f}% <= "
            f"floor {DAILY_DD_KILL_PCT:.2f}%",
        )

    if weekly_pnl_pct <= WEEKLY_DD_KILL_PCT:
        arm_weekly_kill(
            f"5-day rolling PnL {weekly_pnl_pct:.2f}% <= "
            f"floor {WEEKLY_DD_KILL_PCT:.2f}%"
        )
        return KillStatus(
            True, "WEEKLY_DD",
            f"weekly PnL {weekly_pnl_pct:.2f}% <= floor; "
            f"weekly kill armed (touch {REARM_FILE} to resume)",
        )

    return KillStatus(False, "OK")


def arm_weekly_kill(reason: str) -> None:
    """Create the weekly-kill marker file; halts trading until re-armed."""
    ts = datetime.utcnow().isoformat()
    WEEKLY_KILL_FILE.write_text(
        f"{ts} | reason: {reason}\n"
        f"To resume: touch {REARM_FILE}\n",
        encoding="utf-8",
    )


def disarm_weekly_kill_if_armed() -> bool:
    """Manually clear weekly kill if user has placed the re-arm file.
    Returns True if a kill was cleared, False otherwise."""
    if WEEKLY_KILL_FILE.exists() and REARM_FILE.exists():
        try:
            WEEKLY_KILL_FILE.unlink()
            REARM_FILE.unlink()
            return True
        except OSError:
            return False
    return False


def is_armed() -> bool:
    """Return True if any kill condition is currently active (env/file/weekly)."""
    return _env_kill_active() or _file_kill_active() or _weekly_kill_active()


if __name__ == "__main__":
    print("v17D kill-switch status:")
    print(f"  env var ({ENV_KILL_VAR}): {'SET' if _env_kill_active() else 'unset'}")
    print(f"  file ({KILL_FILE}): {'EXISTS' if _file_kill_active() else 'absent'}")
    print(f"  weekly: {'ARMED' if _weekly_kill_active() else 'inactive'}")
    print()
    status = check_before_entry(intraday_pnl_pct=0.0, weekly_pnl_pct=0.0)
    print(f"  check_before_entry(): halted={status.halted} reason={status.reason}")
    if status.detail:
        print(f"    detail: {status.detail}")
