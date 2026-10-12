"""Read-only active-strategy routing. Selecting a profile never arms trading.

Historical tools keep their explicit G/G2/G3 profiles. This registry is only for
the operational dashboard, current consumers and approved deployment routing.
"""
from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
from pathlib import Path

from fno_v13_v10_g_3_contract import STRATEGY_VERSION as G3_STRATEGY_VERSION

WORKSPACE = Path(__file__).resolve().parent
ACTIVE_PROFILE_PATH = WORKSPACE / "configs" / "strategies" / "active_profile.json"


@dataclass(frozen=True)
class StrategyProfile:
    name: str
    slug: str
    label: str
    version: str

    @property
    def live_dir(self) -> str:
        return self.slug + "_live"

    @property
    def backtest_dir(self) -> str:
        return "backtesting_result_" + self.slug

    @property
    def options_dir(self) -> str:
        return self.slug + "_options_paper"

    def reference(self, value: str) -> str:
        """Map an active G path/task/report value; never rewrite saved evidence."""
        if self.name == "V13_V10_G":
            return value
        # A scheduled time suffix such as _1620 is not a strategy version.
        # Preserve already numbered versions (_2, _3, ...), not clock suffixes.
        value = re.sub(r"v13_v10_g(?!_[0-9]{1,2}(?:\b|_))", self.slug, value)
        return re.sub(r"V13_V10_G(?!_[0-9]{1,2}(?:\b|_))", self.name, value)

    def display(self, value: str) -> str:
        if self.name == "V13_V10_G":
            return value
        return re.sub(r"V13[- ]V10[- ]G(?!-[0-9])", self.label, value, flags=re.I)


PROFILES = {
    "V13_V10_G": StrategyProfile("V13_V10_G", "v13_v10_g", "V13-V10-G", "FNO_V13_V10_G_RETAINED_20260914"),
    "V13_V10_G_3": StrategyProfile("V13_V10_G_3", "v13_v10_g_3", "V13-V10-G-3", G3_STRATEGY_VERSION),
}


def deployment_state(path: Path | None = None) -> dict:
    path = path or ACTIVE_PROFILE_PATH
    if not path.exists():
        return {"profile": "V13_V10_G", "deployment_stage": "LEGACY", "live_activation_authorized": False}
    value = json.loads(path.read_text(encoding="utf-8-sig"))
    if not isinstance(value, dict) or value.get("profile") not in PROFILES:
        raise ValueError("Invalid active FnO strategy deployment record")
    return value


def active_profile_name() -> str:
    name = os.getenv("FNO_ACTIVE_STRATEGY_PROFILE", "").strip() or deployment_state()["profile"]
    if name not in PROFILES:
        raise ValueError(f"Unsupported active strategy profile: {name!r}")
    return name


def active_profile() -> StrategyProfile:
    return PROFILES[active_profile_name()]


def expected_fingerprint(profile: str | None = None) -> str:
    name = profile or active_profile_name()
    if name == "V13_V10_G_3":
        from fno_v13_v10_g_3_contract import config_fingerprint
        return config_fingerprint()
    # Historical G fingerprints remain the responsibility of its explicit
    # registry/record. Do not import a live engine to render a dashboard.
    return ""


def g3_live_activation_authorized() -> bool:
    state = deployment_state()
    return (state.get("profile") == "V13_V10_G_3"
            and state.get("deployment_stage") == "LIVE_APPROVED"
            and state.get("live_activation_authorized") is True)


__all__ = ["StrategyProfile", "PROFILES", "active_profile", "active_profile_name", "deployment_state", "expected_fingerprint", "g3_live_activation_authorized"]
