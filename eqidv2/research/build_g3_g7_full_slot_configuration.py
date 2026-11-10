"""Render a dated, read-only Markdown inventory of G3/G7 backtest rules."""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

from research import g3_backtest_profile as g3_profile
from research import g7_g6_replica_backtest as g7
import fno_v13_v10_g_3_contract as production
import strategy_slot_selection as slots


ROOT = Path(__file__).resolve().parents[1]
G3_CONFIG = ROOT / "configs/strategies/v13_v10_g_3_backtest_slot_win70_v17.json"
G7_CONFIG = ROOT / "configs/strategies/v13_v10_g_7_g6_replica_v3.json"
G7_V4_CONFIG = ROOT / "configs/strategies/v13_v10_g_7_v4_low_efficiency_backtest.json"
G7_LOCK_CONFIG = ROOT / "configs/strategies/v13_v10_g_7_v4_lock_backtest.json"
SLOT_CONFIG = ROOT / "configs/strategies/slot_win70_20261028.json"


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def number(value) -> str:
    return f"{float(value):.3f}".rstrip("0").rstrip(".") if float(value) else "0"


def clock(row: dict) -> str:
    return f"{row['signal_end']}→{row['confirmation_end']}"


def fmt_rule(row: dict, *, g7_mode: bool, enabled: set[str],
             confirm: dict, extension: dict, min_wick: dict,
             disabled: dict[str, str], slot_filters: dict,
             preferred_core: dict[str, dict]) -> str:
    ident = row.get("setup_id") or row["confirmation_end"].replace(":", "") + "_" + row["side"]
    side = row["side"]
    extra = []
    core = preferred_core.get(ident)
    if core and core["price_change_pct"] != row["price_change_pct"]:
        extra.append(f"preferred core move≥{number(core['price_change_pct'])}%")
    if ident in extension:
        extra.append(f"1m extension≤{number(extension[ident])} ATR")
    if ident in min_wick:
        extra.append(f"lower wick≥{number(min_wick[ident])}")
    if row["signal_end"] in {"09:20", "09:25"} and side == "SHORT":
        extra.append("opening Nifty≤−0.05%")
    if row["signal_end"] == "09:20":
        extra.append("verified opening OI")
    if ident in slot_filters:
        setting = slot_filters[ident]
        if setting["field"] == "traded_value":
            extra.append(f"postrank value≥₹{int(setting['minimum']):,}")
        else:
            extra.append(f"postrank OI≥{number(setting['minimum'])}%")
    if ident not in enabled:
        extra.append(disabled.get(ident, "frozen slot allowlist rejects"))
    state = "ON" if ident in enabled else "OFF"
    vol1 = confirm.get(ident, 1.1 if side == "LONG" else 1.2)
    fields = [ident, clock(row), side, state,
              number(row["price_change_pct"]), number(row["oi_change_pct"]),
              number(row["volume_ratio"]), number(row["min_traded_value"]),
              number(vol1), number(row["body_ratio"]),
              number(row["max_wick_ratio"]),
              f"{row['max_entries']} / {row['picker']}", number(row["stop_pct"]),
              number(row["target_pct"]), "; ".join(extra) if extra else "—"]
    return "| " + " | ".join(fields) + " |"


HEADER = "| ID | 5m→1m IST | Side | State | 5m move % | 5m OI min % | 5m vol × | Value min ₹ | 1m vol × | 1m body min | 1m wick max | Quota / rank | SL % | Target % | Extra guard / reason |"
DIVIDER = "|" + "---|" * 15


def table(rows: list[dict], *, g7_mode: bool, enabled: set[str],
          confirm: dict, extension: dict, min_wick: dict,
          disabled: dict[str, str], slot_filters: dict,
          preferred_core: dict[str, dict]) -> str:
    ordered = sorted(rows, key=lambda item: (item["signal_end"], item["side"]))
    if len({item.get("setup_id") or item["confirmation_end"].replace(":", "")+"_"+item["side"]
            for item in ordered}) != len(ordered):
        raise ValueError("Duplicate setup/direction in configuration")
    return "\n".join([HEADER, DIVIDER, *(fmt_rule(row, g7_mode=g7_mode,
                         enabled=enabled, confirm=confirm, extension=extension,
                         min_wick=min_wick, disabled=disabled,
                         slot_filters=slot_filters,
                         preferred_core=preferred_core) for row in ordered)])


def run(output: Path) -> dict:
    g3 = g3_profile.configuration()
    g7_current = g7.configuration()
    v4 = json.loads(G7_V4_CONFIG.read_text(encoding="utf-8"))
    lock = json.loads(G7_LOCK_CONFIG.read_text(encoding="utf-8"))
    g3_enabled = slots.enabled_setup_ids("G3")
    g7_enabled = slots.enabled_setup_ids("G7_V4_LOCK2")
    if (len(g3["active_setups"]) != 34 or len(g3_enabled) != 34
            or len(production.ACTIVE_SETUPS) != 61 or len(g7_current["active_setups"]) != 51
            or len(g7_enabled) != 42):
        raise ValueError("Expected pinned G3/G7 setup counts changed")
    if set(item["confirmation_end"].replace(":", "")+"_"+item["side"]
           for item in g3["active_setups"]) != g3_enabled:
        raise ValueError("G3 enabled table differs from active contract")
    if lock["selected_variant"] != "wick_ge_0p20" or lock["daily_loss_lock"]["maximum_realized_net_losing_exits"] != 2:
        raise ValueError("G7 V4 lock policy changed")
    if v4["entry_filter"]["policy"] != "low_efficiency" or v4["exit_policy"]["protect_after_close_pct"] != .6:
        raise ValueError("G7 V4 protection policy changed")
    g3_disabled = {item["confirmation_end"].replace(":", "")+"_"+item["side"]: item.get("reason", "disabled")
                   for item in g3["disabled_setups"]}
    g3_rows = [dict(item.__dict__, setup_id=item.setup_id) for item in production.ACTIVE_SETUPS]
    g3_core = {item["confirmation_end"].replace(":", "")+"_"+item["side"]: item
               for item in g3["core_setups"]}
    g3_table = table(g3_rows, g7_mode=False, enabled=g3_enabled,
                     confirm=g3["minimum_confirmation_1m_volume_ratio_by_setup"],
                     extension=g3["maximum_confirmation_extension_atr_by_setup"],
                     min_wick=g3["minimum_directional_wick_ratio_by_setup"],
                     disabled=g3_disabled, slot_filters={}, preferred_core=g3_core)
    g7_table = table(g7_current["active_setups"], g7_mode=True, enabled=g7_enabled,
                     confirm=g7_current["minimum_confirmation_1m_volume_ratio_by_setup"],
                     extension=g7_current["maximum_confirmation_extension_atr_by_setup"],
                     min_wick={"0926_SHORT": .2}, disabled={},
                     slot_filters=g7_current["slot_entry_filters"], preferred_core={})
    source_paths = [G3_CONFIG, G7_CONFIG, G7_V4_CONFIG, G7_LOCK_CONFIG, SLOT_CONFIG,
                    ROOT / "fno_v13_v10_g_3_contract.py",
                    ROOT / "research/g3_slot_win70_contract.py",
                    ROOT / "research/g7_g6_replica_backtest.py",
                    ROOT / "g3_v18_20261102/selection.py"]
    sources = "\n".join(f"- `{p.relative_to(ROOT).as_posix()}` — SHA-256 `{sha(p)}`" for p in source_paths)
    document = f"""# Full slot configuration: G‑3 V17, G‑7 V4 Lock, and G‑3 V18 research

**Snapshot:** 3 November 2026. **Scope:** backtesting configuration. The open `START_HERE.html` describes G‑3 V18, which is a separate research version. Its `control` arm uses the G‑3 V17 rules below. G‑7 V4 Lock is included because it was the other strategy in the preceding comparison. This file does not change PAPER, LIVE, or a current backtest pointer.

For the assessment of indicator necessity, AND/OR combinations, weighted ranking and news, see [the indicator review](INDICATOR_REVIEW_2026-11-03.md). Its proposed experiments are not active settings.

## How to read the slot tables

Each row is one **signal slot and direction**. A five-minute bar ending at the first clock is checked, then the next one-minute confirmation ending at the second clock. `ON` means the slot can be selected; `OFF` lists its donor configuration for audit **but cannot place a backtest entry**. Both tables show configured minimum directional 5m price movement: LONG requires at least the shown positive percentage; SHORT requires a move at least that large downward. OI is a positive percentage increase. `5m vol ×` and `1m vol ×` are ratios to their respective historical volume reference; zero in a minimum column disables that particular threshold. `Value min ₹` records the configured traded-value threshold; the G‑7 12:26 SHORT threshold is applied **after ranking** as shown in Extra. Body and wick values are fractions of the confirmation candle's range. The wick maximum is the **upper wick for LONG** and **lower wick for SHORT**. `Quota / rank` is the maximum candidates per setup per day, chosen by the displayed rank key; `max_move` ranks by absolute directional move. All percentages are percentages, not decimal fractions.

### Shared integrity and timing checks

- Five-minute signal features must be complete and available at the signal bar close; one-minute features must be available at the exact next confirmation close. No later candle may contribute to selection. A five-minute source bar requires five valid one-minute components. Gaps, synthetic/opening snapshots, and stale confirmation inputs fail the relevant data check.
- The base directional gate requires EMA9 > EMA20 > EMA50 for LONG or EMA9 < EMA20 < EMA50 for SHORT, an absolute price change of at least 0.10%, increasing futures OI, OI change between **0.05% and 1.00%**, and five-minute relative volume at least **0.80×**. Per-row thresholds can be stricter. An eligible one-minute confirmation must close above both its open and signal close for LONG, or below both for SHORT.
- The 09:20 signal uses **09:15 cash open → 09:20 cash close** and a verified 09:16→09:20 near-month futures OI comparison; ordinary prior-bar OI is not substituted. For 09:20/09:25 SHORT signals, the dated near-month Nifty futures 09:20 first-bar return must be **≤−0.05%**.
- The confirmation-volume reference is the previous **20 observed completed one-minute bars**, excluding the current minute, with a five-observation research minimum. Live warmup rules may require 20 prior observations; this table is the backtest contract. A signal is ranked only after its entry filters pass; the core-first quota may prioritize core-rule candidates before relaxed candidates.
- G‑3's explicit global switches are `strict_0925_long=true`, `morning_slots=false`, and `two_bar_continuation=false`. Its opening-data rule excludes a stock from 09:20 if that stock's required OI baseline is missing or late; if all opening inputs are missing, it blocks the 09:20 cycle. These are configuration switches, not extra per-row thresholds.
- Entry is triggered at the confirmation high for LONG or confirmation low for SHORT, from the next minute, with the original **10-minute** trigger expiry. The historical backtest uses one-minute OHLC and conservative stop-first ordering if stop and target are both touched in one bar. Runtime's 90-second activation grace is a separate clock, not a five-minute wait or a change to this historical entry window.

## G‑3 V17 backtest — all 61 configured rows, 34 enabled

The table includes all 61 production donor rows so disabled slot/direction pairs are explicit. `ON` rows are the effective V17 backtest slots. An `OFF` row's stop and target are donor values only. G‑3 V17 disabled slots below the frozen historical net-win-rate cutoff or without executed evidence, and retained three explicitly restored low-target setups. That allowlist was chosen on past outcomes through 27 October; it is retrospective, not an independent forward validation.

**Core-first preference:** the move column gives the minimum expanded eligibility threshold. Candidates satisfying the preferred core rules are considered first for the setup quota. Five SHORT setups have a stricter core move threshold, shown in Extra: 09:26, 09:31, 09:51 and 11:21 prefer **0.20%** downward movement, and 09:36 prefers **0.50%**. A candidate clearing only the lower table threshold can therefore lose its place to a core candidate; clearing the minimum alone does not guarantee selection.

{g3_table}

**G‑3 exits and capital.** Full-position target is the row target; initial SL is **1.25%**, tightened to **1.00%** after **120 minutes**. No partial exit or breakeven stop is configured. Positions square off at the **15:15** completed minute. The modeled portfolio has **₹10,00,000** capital, **₹1,00,000** allocated per entry with **5×** exposure (₹5,00,000 target exposure), and **5 bps round-trip** modeled cost. `max_positions` is unset; allocation still observes available portfolio capital. The 10:35 LONG/SHORT confirmation has the additional directional close-to-EMA20 extension limit of **3 ATR**. The 09:26 SHORT confirmation needs at least **0.20 lower-wick ratio** before ranking.

## G‑7 V4 Lock backtest — all 51 configured rows, 42 enabled

`OFF` rows are excluded by the fixed G‑7 slot allowlist. The base G‑7 configuration supplies the per-row entry thresholds and full targets; V4 Lock adds the guards below. `postrank` in the Extra column means the ranked order is vetoed if it misses the stronger threshold; the quota is **not** filled by a replacement in that particular rule. The 09:26 SHORT lower-wick rule is different: it runs **before** max-volume ranking and allows the next eligible candidate to replace a rejected one.

{g7_table}

**G‑7 direction and entry guards.** The selected direction policy uses dated near-month **Nifty futures**. It rejects a trade only if **both** the exact latest completed five-minute candle's close-versus-open direction and that close-versus-the-09:20-bar's-open direction oppose the trade. This uses completed five-minute evidence, not the confirmation minute's latest quote. Missing or invalid required Nifty evidence fails the run. A separate V4 veto rejects an entry only when all three conditions hold at confirmation: a directional VWAP z-score **>2.5** *or* a prior same-clock historical **95th-percentile extreme**; 10-change price efficiency **<0.30**; and RSI14 weakening over **three completed minutes**. With fewer than **30 valid session one-minute bars**, this V4 indicator veto preserves the entry. The 09:26 SHORT also requires lower wick **≥0.20** and confirmation extension **≤4 ATR**. Other per-slot extension limits are in the table.

**G‑7 effective V4 exits and capital.** V4 Lock replays a **single full-position target** at the row percentage. Its initial SL is **1.20%**, tightened to **1.00%** after **120 minutes**. A completed post-entry one-minute close reaching **+0.60% gross** arms a stop protecting **+0.10% gross**, executable from the **next** one-minute open; the active stop never loosens and a gap can fill worse. This V4 replay supersedes the earlier G‑7 V3 half-at-half-target exit plan. At 15:15, remaining positions square off. Capital is **₹10,00,000**, with **₹1,00,000** per entry, **5×** target exposure, **integer cash-share sizing**, one active position per stock, a conservative entry clock, and **5 bps round-trip** modeled cost. After the **second realized net losing exit** in a day, new admissions are blocked; existing positions continue to be managed. The fixed slot allowlist is applied before fresh portfolio allocation and loss-lock replay.

## G‑3 V18 research overlays (not the active V17 contract)

The V18 `control` retains every V17 slot, quota, entry and exit setting above. Each other V18 arm applies its extra rule **before** ranking so another eligible stock may replace a rejected one. These are separate research arms; none is promoted to LIVE or PAPER.

| V18 arm | Additional or removed rule |
|---|---|
| `control` | Exact G‑3 V17 selection and execution. |
| `market_only` | Directional breadth from session open **≥50%**, with dated-universe coverage **≥90%** and valid exact-clock data. |
| `strength_only` | Signed stock return since open minus Nifty spot return since open **≥0** in the trade direction. |
| `vwap_pressure` | Signed five-minute approximate VWAP distance **≥0**, VWAP slope **≥0**, and six-bar candle/volume pressure **≥0**. |
| `advanced` | All three preceding added groups together. |
| `simplified` | Removes EMA9/20/50 stacking and the minimum 1m body / maximum 1m wick thresholds, including the 09:26 SHORT lower-wick minimum; keeps price, OI, volume, directional confirmation, timing, ranking, quotas, stops and targets. |
| `simplified_advanced` | `simplified` plus all three added groups. |

V18 uses only completed signal-bar market/stock features for its new filters. Missing required features fail closed; there is no first-bar VWAP-slope substitute. Its VWAP is a typical-price approximation and pressure is a candle/volume proxy, not measured aggressor flow. The V18 results in `START_HERE.html` end on **30 October 2026** and did **not** qualify any changed arm for promotion under its predeclared October check.

## Versioned source files

{sources}

The tables were generated from these checked-in contracts. For exact implementation details, inspect the source files and the saved versioned configuration JSON rather than assuming an `OFF` row is tradable.
"""
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(document, encoding="utf-8")
    return dict(output=str(output), g3_configured=len(g3_rows), g3_enabled=len(g3_enabled),
                g7_configured=len(g7_current["active_setups"]), g7_enabled=len(g7_enabled),
                lines=document.count("\n")+1)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(run(args.output), indent=2))


if __name__ == "__main__":
    main()
