"""Shared G/G3 entry authority. Exits and reconciliation never use this gate."""
from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path

from fno_strategy_registry import deployment_state


def entry_authority(profile: str) -> tuple[bool, str]:
    if profile not in {"V13_V10_G", "V13_V10_G_3"}:
        return True, "LEGACY_PROFILE"
    try:
        state = deployment_state()
    except (OSError, ValueError, TypeError):
        return False, "DEPLOYMENT_RECORD_INVALID"
    if state.get("profile") != profile:
        return False, "INACTIVE_STRATEGY_ENTRY_BLOCKED"
    if profile == "V13_V10_G_3" and not (
        state.get("deployment_stage") == "LIVE_APPROVED"
        and state.get("live_activation_authorized") is True
    ):
        return False, "G3_LIVE_ACTIVATION_NOT_AUTHORIZED"
    return True, "ACTIVE_STRATEGY_OWNER"


@contextmanager
def entry_owner_lock(root: Path, profile: str):
    # A single account entry lock serializes every G/G3 new-order mutation.
    # Read authority again while holding it; a stale arm alone is insufficient.
    from fno_v13_v10_g_paper import paper_portfolio_lock
    with paper_portfolio_lock(Path(root) / "entry_owner", timeout_sec=5.):
        allowed, reason = entry_authority(profile)
        if not allowed:
            raise RuntimeError(reason)
        yield
