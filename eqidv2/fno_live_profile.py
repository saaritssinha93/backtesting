"""Select the strategy behind stable FnO runtime transport generations."""
from __future__ import annotations

import importlib
import os

G_PROFILE = 'V13_V10_G'
G3_PROFILE = 'V13_V10_G_3'
G7_V4_LOCK_PROFILE = 'V13_V10_G_7_V4_LOCK'
PROFILE_ENV = 'FNO_V6_STRATEGY_PROFILE'


def config_for_generation(generation: str):
    generation = str(generation).strip().lower()
    if generation not in ('v5', 'v6'):
        raise ValueError(f'Unsupported FnO transport generation: {generation}')
    profile = os.getenv(PROFILE_ENV, '').strip().upper()
    if generation == 'v6' and profile:
        if profile not in (G_PROFILE, G3_PROFILE, G7_V4_LOCK_PROFILE):
            raise ValueError(f'Unsupported FnO V6 strategy profile: {profile}')
        module = {
            G_PROFILE: 'fno_v13_v10_g_live_config',
            G3_PROFILE: 'fno_v13_v10_g_3_live_config',
            G7_V4_LOCK_PROFILE: 'fno_v13_v10_g_7_v4_lock_live_config',
        }[profile]
        return importlib.import_module(module)
    return importlib.import_module(f'fno_{generation}_live_config')


def is_g_config(config) -> bool:
    return getattr(config, 'STRATEGY_PROFILE', '') == G_PROFILE


def is_g3_config(config) -> bool:
    return getattr(config, 'STRATEGY_PROFILE', '') == G3_PROFILE


def is_modern_config(config) -> bool:
    """Profiles requiring exact-minute readiness, dated identity and paper locks."""
    return is_g_config(config) or is_g3_config(config)
