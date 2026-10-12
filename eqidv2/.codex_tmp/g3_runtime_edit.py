from pathlib import Path

def edit(name, replacements):
    path = Path(name)
    text = path.read_text(encoding='utf-8')
    for old, new in replacements:
        if old not in text:
            raise ValueError(f'{name}: missing replacement {old[:100]}')
        text = text.replace(old, new)
    path.write_text(text, encoding='utf-8', newline='')

edit('fno_v5_live.py', [
    ('config_for_generation, is_g_config', 'config_for_generation, is_modern_config, is_g3_config'),
    ('is_g_config(config)', 'is_modern_config(config)'),
    ('return canonical_signal_id(', 'return getattr(config, "canonical_signal_id", canonical_signal_id)('),
    ('not g_policy.enabled_for_session(session_date)', 'not getattr(config, "policy", g_policy).enabled_for_session(session_date)'),
    ('g_policy.INITIAL_STOP_PCT', 'getattr(config, "policy", g_policy).INITIAL_STOP_PCT'),
    ('g_policy.TIGHTENED_STOP_PCT', 'getattr(config, "policy", g_policy).TIGHTENED_STOP_PCT'),
    ('g_policy.TIGHTEN_AFTER_MINUTES', 'getattr(config, "policy", g_policy).TIGHTEN_AFTER_MINUTES'),
    ('expire_pending(state, now)', 'expire_pending(state, now, strategy_config=config)'),
    ('enforce_fill_capacity(previous, proposed, root)', 'enforce_fill_capacity(previous, proposed, root, strategy_config=config)'),
    ('def _live_arm_state(session_date: date) -> tuple[bool, str]:\n', '''def _live_arm_state(session_date: date) -> tuple[bool, str]:
    from fno_live_entry_guard import entry_authority
    allowed, reason = entry_authority(getattr(config, "STRATEGY_PROFILE", ""))
    if not allowed:
        return False, reason
    if is_g3_config(config):
        for root_name in ("v6_live", "v13_v10_g_live", "control"):
            if bool(_read_json(common.FNO_ROOT / root_name / "kill_switch.json").get("enabled")):
                return False, "GLOBAL_KILL_SWITCH_ENABLED"
'''),
    ('    try:\n        return _broker_place(client, **payload)\n    except BrokerMutationUncertain as exc:', '''    try:
        if is_modern_config(config):
            from fno_live_entry_guard import entry_owner_lock
            with entry_owner_lock(common.FNO_ROOT / "control", config.STRATEGY_PROFILE):
                # The lock protects the authority check and broker submission.
                return _broker_place(client, **payload)
        return _broker_place(client, **payload)
    except BrokerMutationUncertain as exc:'''),
])

edit('fno_equity_fetch_1min.py', [
    ('config_for_generation, is_g_config', 'config_for_generation, is_modern_config'),
    ('is_g_config(config)', 'is_modern_config(config)'),
    ('def _g_completed_volume_history(records: list[dict[str, Any]], expected_end: datetime) -> pd.DataFrame:\n    import fno_v13_v10_g_live_config as g_config', '''def _g_completed_volume_history(records: list[dict[str, Any]], expected_end: datetime, strategy_config=None) -> pd.DataFrame:
    if strategy_config is None:
        import fno_v13_v10_g_live_config as strategy_config
    g_config = strategy_config'''),
    ('g_profile = candidate.get("strategy_profile") == "V13_V10_G"', 'g_profile = candidate.get("strategy_profile") in {"V13_V10_G", "V13_V10_G_3"}'),
    ('            import fno_v13_v10_g_live_config as g_config', '''            from importlib import import_module
            g_config = import_module({"V13_V10_G": "fno_v13_v10_g_live_config",
                                      "V13_V10_G_3": "fno_v13_v10_g_3_live_config"}[candidate["strategy_profile"]])'''),
    ('_g_completed_volume_history(records, expected_end)', '_g_completed_volume_history(records, expected_end, g_config)'),
    ('_g_completed_volume_history(list(earlier) + list(records), expected_end)', '_g_completed_volume_history(list(earlier) + list(records), expected_end, g_config)'),
])

edit('fno_v6_live_kite_session.py', [
    ('config_for_generation, is_g_config', 'config_for_generation, is_modern_config, is_g3_config'),
    ('"fno_v13_v10_g_live_kite_qty1" if is_g_config(config) else "fno_v6_live_kite_qty1"', 'f"{getattr(config, \'SESSION_PREFIX\', \'fno_v6\')}_live_kite_qty1"'),
    ('"v13_v10_g" if _is_g_profile() else "v6"', 'getattr(config, "SESSION_PREFIX", "fno_v6").removeprefix("fno_")'),
    ('return is_g_config(config)', 'return is_modern_config(config)'),
    ('return "V13-V10-G" if _is_g_profile() else "V6"', 'return getattr(config, "DISPLAY_LABEL", "V6")'),
    ('"fno_v13_v10_g_live.py" if _is_g_profile() else "fno_v6_live.py"', 'f"{getattr(config, \'SESSION_PREFIX\', \'fno_v6\')}_live.py"'),
    ('env["FNO_V6_STRATEGY_PROFILE"] = "V13_V10_G"', 'env["FNO_V6_STRATEGY_PROFILE"] = config.STRATEGY_PROFILE'),
    ('"fno_v13_v10_g" if _is_g_profile() else "fno_v6"', 'getattr(config, "SESSION_PREFIX", "fno_v6")'),
    ('    kill_enabled = bool(kill.get("enabled"))\n    if not acknowledgement_valid:', '''    kill_enabled = bool(kill.get("enabled"))
    from fno_live_entry_guard import entry_authority
    owner_allowed, owner_reason = entry_authority(getattr(config, "STRATEGY_PROFILE", ""))
    if is_g3_config(config):
        kill_enabled = kill_enabled or any(bool(_read_json(common.FNO_ROOT / root / "kill_switch.json").get("enabled"))
            for root in ("v6_live", "v13_v10_g_live", "control"))
    if not owner_allowed:
        reason = owner_reason
    elif not acknowledgement_valid:'''),
    ('    """Persist explicit authorization for this strategy and session date."""\n', '''    """Persist explicit authorization for the legacy strategy only."""
    if is_g3_config(config):
        raise ValueError("G3 auto-arm is disabled; separate dated activation approval is required")
'''),
    ('    if getattr(args, "auto_arm", False):\n        arm_path', '''    if is_g3_config(config):
        from fno_strategy_registry import g3_live_activation_authorized
        if not getattr(args, "activate_live", False) or not g3_live_activation_authorized():
            raise ValueError("G3 LIVE workers require --activate-live and separate LIVE_APPROVED deployment authorization")
    if getattr(args, "auto_arm", False):
        arm_path'''),
    ('    parser.add_argument("--once", action="store_true")', '    parser.add_argument("--once", action="store_true")\n    parser.add_argument("--activate-live", action="store_true", help="Start G3 workers only after separate LIVE_APPROVED authorization; never arms.")'),
])
