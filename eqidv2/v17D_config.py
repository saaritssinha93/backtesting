"""v17D config loader with schema validation.

Loads `eqidv2/configs/v17D.yaml` into a typed Config object. Fails fast
with a clear error if required fields are missing or malformed.

Public API:
    load_config(path: str | Path | None = None) -> Config
    Config (dataclass): all v17D tunables grouped by section
    SetupConfig, FilterStep (dataclasses): per-setup detail

Usage:
    from eqidv2.v17D_config import load_config
    cfg = load_config()
    print(cfg.universe.adv_floor_rs_cr)
    for setup_id, setup_cfg in cfg.setups.items():
        if setup_cfg.enabled:
            print(setup_id, setup_cfg.size_mult)
"""
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

DEFAULT_CONFIG_PATH = Path(__file__).parent / "configs" / "v17D.yaml"


@dataclass(frozen=True)
class UniverseConfig:
    price_floor_rs: float
    price_cap_rs: float
    adv_floor_rs_cr: float
    fo_list_path: str


@dataclass(frozen=True)
class CostsConfig:
    brokerage_pct: float
    brokerage_cap_rs: float
    stt_sell_pct: float
    nse_txn_pct: float
    sebi_pct: float
    gst_pct: float
    stamp_duty_buy_pct: float
    slippage_per_leg: dict     # {bucket: pct}
    extra_stop_slippage: float
    adv_top100_floor_rs_cr: float
    adv_mid_floor_rs_cr: float


@dataclass(frozen=True)
class KillSwitchConfig:
    daily_dd_kill_pct: float
    weekly_dd_kill_pct: float


@dataclass(frozen=True)
class LoggingConfig:
    log_dir: str
    rotate_daily: bool
    streams: list


@dataclass(frozen=True)
class SetupConfig:
    enabled: bool
    size_mult: float
    max_daily_trades: int | None
    sl_pct: float
    tgt_pct: float
    notes: str = ""


@dataclass(frozen=True)
class FilterStep:
    feature: str
    op: str            # ">=" or "<="
    value: float


@dataclass(frozen=True)
class GovernorsConfig:
    long_time_window: tuple
    short_time_window: tuple
    max_long_per_day: int
    max_short_per_day: int
    daily_loss_kill_streak: int
    setup_cool_losses: int
    setup_cool_window: int
    max_concurrent_per_sector_per_side: int
    max_long_per_sector_per_day: int
    net_beta_throttle_threshold: int


@dataclass(frozen=True)
class ParetoSearchConfig:
    count_floor_per_day: float
    worst_day_floor: int
    min_oos_pf: float
    tier1_baseline_pf: float
    max_setups_per_family: int


@dataclass(frozen=True)
class LibrarySizingConfig:
    default_size_mult: float
    default_max_daily_trades: int
    ship_threshold_pf: float
    ship_threshold_oos_pf: float
    ship_threshold_n_min: int


@dataclass(frozen=True)
class TradingConfig:
    leverage: float
    default_position_size_rs: int
    eod_cutoff_ist: str


@dataclass(frozen=True)
class Config:
    version: str
    universe: UniverseConfig
    costs: CostsConfig
    kill_switch: KillSwitchConfig
    logging: LoggingConfig
    setups: dict             # {setup_id: SetupConfig}
    filter_chains: dict      # {(side, setup_id): [FilterStep, ...]}
    governors: GovernorsConfig
    pareto_search: ParetoSearchConfig
    library_initial_sizing: LibrarySizingConfig
    trading: TradingConfig


def _require(d: dict, key: str, section: str) -> Any:
    if key not in d:
        raise SystemExit(f"v17D config: missing {section}.{key}")
    return d[key]


def _build_setups(raw: dict) -> dict:
    out = {}
    for setup_id, sec in raw.items():
        out[setup_id] = SetupConfig(
            enabled=bool(_require(sec, "enabled", f"setups.{setup_id}")),
            size_mult=float(_require(sec, "size_mult", f"setups.{setup_id}")),
            max_daily_trades=sec.get("max_daily_trades"),
            sl_pct=float(_require(sec, "sl_pct", f"setups.{setup_id}")),
            tgt_pct=float(_require(sec, "tgt_pct", f"setups.{setup_id}")),
            notes=sec.get("notes", ""),
        )
        # R:R invariant
        if out[setup_id].tgt_pct < out[setup_id].sl_pct:
            raise SystemExit(
                f"v17D config: {setup_id} has TGT < SL "
                f"({out[setup_id].tgt_pct} < {out[setup_id].sl_pct})"
            )
    return out


def _build_chains(raw: dict) -> dict:
    out = {}
    for side, side_block in raw.items():
        if side not in ("LONG", "SHORT"):
            raise SystemExit(f"v17D config: bad side {side!r} in filter_chains")
        for setup_id, steps in side_block.items():
            chain = []
            for step in steps:
                op = step["op"]
                if op not in (">=", "<="):
                    raise SystemExit(f"bad op {op!r} in filter_chains.{side}.{setup_id}")
                chain.append(FilterStep(
                    feature=str(step["feature"]),
                    op=op,
                    value=float(step["value"]),
                ))
            out[(side, setup_id)] = chain
    return out


def load_config(path: str | Path | None = None) -> Config:
    try:
        import yaml
    except ImportError:
        raise SystemExit(
            "v17D config requires PyYAML; install with: pip install pyyaml"
        )
    p = Path(path) if path else DEFAULT_CONFIG_PATH
    if not p.exists():
        raise SystemExit(f"v17D config not found: {p}")
    with open(p, "r", encoding="utf-8") as f:
        raw = yaml.safe_load(f)

    return Config(
        version=str(raw.get("version", "unknown")),
        universe=UniverseConfig(**raw["universe"]),
        costs=CostsConfig(**raw["costs"]),
        kill_switch=KillSwitchConfig(**raw["kill_switch"]),
        logging=LoggingConfig(**raw["logging"]),
        setups=_build_setups(raw["setups"]),
        filter_chains=_build_chains(raw["filter_chains"]),
        governors=GovernorsConfig(
            long_time_window=tuple(raw["governors"]["long_time_window"]),
            short_time_window=tuple(raw["governors"]["short_time_window"]),
            max_long_per_day=int(raw["governors"]["max_long_per_day"]),
            max_short_per_day=int(raw["governors"]["max_short_per_day"]),
            daily_loss_kill_streak=int(raw["governors"]["daily_loss_kill_streak"]),
            setup_cool_losses=int(raw["governors"]["setup_cool_losses"]),
            setup_cool_window=int(raw["governors"]["setup_cool_window"]),
            max_concurrent_per_sector_per_side=int(raw["governors"]["max_concurrent_per_sector_per_side"]),
            max_long_per_sector_per_day=int(raw["governors"]["max_long_per_sector_per_day"]),
            net_beta_throttle_threshold=int(raw["governors"]["net_beta_throttle_threshold"]),
        ),
        pareto_search=ParetoSearchConfig(**raw["pareto_search"]),
        library_initial_sizing=LibrarySizingConfig(**raw["library_initial_sizing"]),
        trading=TradingConfig(**raw["trading"]),
    )


if __name__ == "__main__":
    cfg = load_config()
    print(f"v17D config loaded: version={cfg.version}")
    print(f"  universe: price [{cfg.universe.price_floor_rs}, {cfg.universe.price_cap_rs}], "
          f"ADV >= Rs {cfg.universe.adv_floor_rs_cr} cr")
    print(f"  costs: brokerage {cfg.costs.brokerage_pct*100:.3f}%, slippage {cfg.costs.slippage_per_leg}")
    print(f"  kill: daily {cfg.kill_switch.daily_dd_kill_pct}%, weekly {cfg.kill_switch.weekly_dd_kill_pct}%")
    print(f"  setups: {len(cfg.setups)} ({sum(1 for s in cfg.setups.values() if s.enabled)} enabled)")
    print(f"  filter chains: {len(cfg.filter_chains)} (side, setup) pairs")
    print(f"  trading: leverage {cfg.trading.leverage}x, size Rs {cfg.trading.default_position_size_rs:,}")
