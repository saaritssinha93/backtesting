"""Predeclared entry experiments for the G4 setup review; no outcome access.

Raw candidate quality and immutable market gates are validated once. Numerical
setup thresholds are then applied before ranking, so rejected top-ranked stocks
can be replaced and relaxed filters can admit previously rejected candidates.
"""
from __future__ import annotations

import copy
from dataclasses import asdict
import json
import math

import pandas as pd

import fno_v13_v10_g_3_contract as prod
from research import g4_complete_inventory_backtest as complete


VERSION = "G4-SETUP-ENTRY-ENGINE-V2"
MULTIPLIERS = (.75, 1.25, 1.5)
RATIO_DELTAS = (-.2, -.1, .1, .2)
FLOORS = {"price_change_pct": prod.BASE_PRICE_CHANGE_PCT,
          "oi_change_pct": prod.BASE_OI_CHANGE_PCT,
          "volume_ratio": prod.BASE_VOLUME_RATIO}
NUMERIC_FIELDS = ("price_change_pct", "oi_change_pct", "volume_ratio",
                  "body_ratio", "wick_ratio", "traded_value", "v9_1m_volume_ratio")


def _setup_id(spec: dict) -> str:
    return spec["confirmation_end"].replace(":", "") + "_" + spec["side"]


def _specs(config):
    identity = complete.Identity(config)
    active = {name: asdict(spec) for name, spec in identity._ACTIVE_BY_ID.items()}
    core = {_setup_id(spec): dict(spec) for spec in identity.payload["core_setups"]}
    for name, minimum in config.get("five_minute_volume_ratio_overrides", {}).items():
        active[name]["volume_ratio"] = float(minimum)
        core[name]["volume_ratio"] = float(minimum)
    return active, core


def _number(value):
    try:
        return float(value)
    except (ValueError, TypeError):
        return math.nan


def _signature(item):
    return json.dumps({key: item[key] for key in
                       ("active", "core", "confirmation_volume", "extension_max")},
                      sort_keys=True, allow_nan=False)


def configurations(config: dict, effective_guards: dict) -> dict[str, list[dict]]:
    """Return a deterministic JSON-safe grid, without reading any outcomes.

    The supplied guards describe actual baseline selection. In particular the
    historical 09:25 LONG override did not apply the guard declared in V13 JSON.
    The caller pins that discrepancy in the research provenance.
    """
    active, core = _specs(config)
    guards = {**prod.MAX_CONFIRMATION_EXTENSION_ATR_BY_SETUP, **effective_guards}
    result = {}
    for setup_id in active:
        side = active[setup_id]["side"]
        baseline = dict(name="baseline", setup_id=setup_id,
            active=active[setup_id], core=core[setup_id],
            confirmation_volume=float(config["minimum_confirmation_1m_volume_ratio_by_setup"].get(
                setup_id, prod.MIN_CONFIRMATION_VOLUME_BY_SIDE[side])),
            extension_max=guards.get(setup_id), complexity=0, changes={})
        candidates = [baseline]

        def add(name, transforms):
            variant = copy.deepcopy(baseline)
            variant["name"] = name
            for field, operation, amount in transforms:
                if field in ("confirmation_volume", "extension_max"):
                    value = variant[field]
                    variant[field] = round(value * amount if operation == "factor" else value + amount, 10)
                else:
                    for scope in ("active", "core"):
                        value = variant[scope][field]
                        value = value * amount if operation == "factor" else value + amount
                        if field in FLOORS:
                            value = max(FLOORS[field], value)
                        elif field in ("body_ratio", "max_wick_ratio"):
                            value = min(1., max(0., value))
                        variant[scope][field] = round(value, 10)
                variant["changes"][field] = {"operation": operation, "value": amount}
            variant["complexity"] = len(transforms)
            candidates.append(variant)

        for field in ("price_change_pct", "oi_change_pct", "volume_ratio", "confirmation_volume"):
            for factor in MULTIPLIERS:
                add(f"{field}_x{factor:g}", [(field, "factor", factor)])
        for field in ("body_ratio", "max_wick_ratio"):
            for delta in RATIO_DELTAS:
                add(f"{field}_{delta:+g}", [(field, "delta", delta)])
        if baseline["extension_max"] is not None:
            for factor in (.75, 1.25):
                add(f"extension_max_x{factor:g}", [("extension_max", "factor", factor)])
        for label, factor in (("loose", .75), ("strict", 1.25)):
            add("price_volume_" + label, [(field, "factor", factor)
                for field in ("price_change_pct", "volume_ratio")])
            add("oi_confirmation_" + label, [(field, "factor", factor)
                for field in ("oi_change_pct", "confirmation_volume")])
            delta = -.1 if label == "loose" else .1
            add("body_wick_" + label, [("body_ratio", "delta", delta),
                                     ("max_wick_ratio", "delta", -delta)])
        unique = {}
        for item in candidates:
            unique.setdefault(_signature(item), item)
        result[setup_id] = list(unique.values())
    return result


def _source_valid(row: dict, spec: dict) -> bool:
    if str(row.get("side", "")).upper() != spec["side"]:
        return False
    try:
        signal = prod._stamp(row.get("signal_timestamp"))
        confirmation = prod._stamp(row.get("confirmation_timestamp"))
        feature = prod._stamp(row.get("v9_1m_feature_ts"))
        if (signal.strftime("%H:%M") != spec["signal_end"] or signal != signal.floor("min")
                or confirmation != signal + pd.Timedelta(minutes=1) or feature != confirmation
                or signal.date() != pd.Timestamp(row["day"]).date()):
            return False
        if "v9_5m_feature_ts" in row and prod._stamp(row["v9_5m_feature_ts"]) > signal:
            return False
        if "v9_feature_available_ts" in row and prod._stamp(row["v9_feature_available_ts"]) > confirmation:
            return False
    except (TypeError, ValueError, KeyError):
        return False
    if row.get("signal_end", spec["signal_end"]) != spec["signal_end"]:
        return False
    if not prod._confirmation_valid(row, spec["side"]):
        return False
    if prod.base_signal_side(row, spec["signal_end"], session_date=signal.date()) != spec["side"]:
        return False
    return all(math.isfinite(_number(row.get(field))) for field in NUMERIC_FIELDS)


def _passes(row: dict, spec: dict, confirmation_volume: float, extension_max) -> bool:
    sign = 1. if spec["side"] == "LONG" else -1.
    extension = _number(row.get("research_extension_atr"))
    return (sign * _number(row["price_change_pct"]) >= spec["price_change_pct"]
            and _number(row["oi_change_pct"]) >= spec["oi_change_pct"]
            and _number(row["volume_ratio"]) >= spec["volume_ratio"]
            and spec["body_ratio"] <= _number(row["body_ratio"]) <= 1.
            and 0. <= _number(row["wick_ratio"]) <= spec["max_wick_ratio"]
            and _number(row["traded_value"]) >= spec["min_traded_value"]
            and _number(row["v9_1m_volume_ratio"]) >= confirmation_volume
            and (extension_max is None or math.isfinite(extension) and extension <= extension_max))


class PreparedSelection:
    """Validated, outcome-independent candidate groups shared across variants."""

    def __init__(self, pool: pd.DataFrame, config: dict, effective_guards: dict):
        self.config = copy.deepcopy(config)
        self.active, self.core = _specs(config)
        self.variants = configurations(config, effective_guards)
        self.pools = {}
        self.columns = list(pool.columns)
        if pool.sid.duplicated().any():
            raise ValueError("Duplicate candidate SID")
        if pool.duplicated(["day", "setup_id", "tradingsymbol"]).any():
            raise ValueError("Duplicate candidate in setup/session/symbol")
        self.valid_counts = {name: 0 for name in self.active}
        for record in pool.to_dict("records"):
            row = prod._candidate(record)
            setup_id = row.get("setup_id")
            if setup_id not in self.active:
                continue
            if not _source_valid(row, self.active[setup_id]):
                continue
            row["day"] = str(row["day"])[:10]
            self.pools.setdefault((setup_id, row["day"]), []).append(row)
            self.valid_counts[setup_id] += 1

    def select(self, setup_id: str, variant: dict) -> pd.DataFrame:
        if setup_id not in self.active or variant.get("setup_id") != setup_id:
            raise ValueError("Variant/setup mismatch")
        active, core = variant["active"], variant["core"]
        for scope, baseline in ((active, self.active[setup_id]), (core, self.core[setup_id])):
            for field in ("signal_end", "confirmation_end", "side", "picker", "max_entries"):
                if scope[field] != baseline[field]:
                    raise ValueError("Variant modified immutable selection mechanics")
        spec = prod.SetupSpec(**active)
        selected = []
        for (name, day), pool in sorted(self.pools.items()):
            if name != setup_id:
                continue
            eligible = sorted((row for row in pool if _passes(row, active,
                variant["confirmation_volume"], variant["extension_max"])),
                key=lambda row: prod._rank_key(row, spec))
            if setup_id == "0926_LONG":
                chosen = eligible[:active["max_entries"]]
                core_ids = {row["sid"] for row in chosen}
            else:
                core_chosen = [row for row in eligible if _passes(row, core,
                    variant["confirmation_volume"], variant["extension_max"])][:core["max_entries"]]
                core_ids = {row["sid"] for row in core_chosen}
                chosen = core_chosen + [row for row in eligible if row["sid"] not in core_ids][
                    :active["max_entries"] - len(core_chosen)]
            ranks = {row["sid"]: number for number, row in enumerate(eligible, 1)}
            for original in chosen:
                row = dict(original)
                row.update(v9_selected=True, v9_filter_pass=True,
                    v9_rank_in_setup_day=ranks[row["sid"]], v9_decision="SELECTED",
                    v10_g_f_core=row["sid"] in core_ids, native_stop_pct=active["stop_pct"],
                    native_target_pct=active["target_pct"], picker=active["picker"],
                    max_entries=active["max_entries"], configured_confirmation_end=active["confirmation_end"],
                    entry_variant=variant["name"], strategy_version=self.config["variant_id"],
                    signal_id=prod.canonical_signal_id(self.config["variant_id"], pd.Timestamp(day).date(),
                        active["signal_end"], active["confirmation_end"], active["side"], row["tradingsymbol"]))
                selected.append(row)
        if not selected:
            return pd.DataFrame(columns=list(dict.fromkeys(self.columns + ["v9_selected", "v9_filter_pass",
                "v9_rank_in_setup_day", "v10_g_f_core", "entry_variant", "native_stop_pct", "native_target_pct"])))
        return pd.DataFrame(selected).sort_values(["day", "setup_id", "tradingsymbol"], kind="stable").reset_index(drop=True)
