"""Train-first 09:50 LONG threshold search and chronological sensitivity audit."""
from __future__ import annotations

import hashlib
import itertools
import json
from datetime import date
from pathlib import Path

import pandas as pd

import strict_1pct_study as study


ROOT = study.OUT / "joint_0950_5m_1m"
OUT = ROOT / "joint_search.json"
GRID = ROOT / "joint_grid.csv"
PROVENANCE = ROOT / "candidate_outcomes_provenance.json"
POOL = ROOT / "candidate_outcomes.parquet"

# Prespecified coarse, interpretable levels. Base gates remain fixed.
PRICE = (.10, .20, .30, .40, .50)
OI = (.05, .10, .15, .20, .25)
FIVE_VOLUME = (.80, 1.00, 1.30, 1.50, 2.00)
BODY = (.25, .30, .35, .40, .50, .60)
WICK = (.40, .50, .55, .60)
MINUTE_VOLUME = (.60, .80, 1.00, 1.30, 1.50)


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    provenance = json.loads(PROVENANCE.read_text())
    if sha(POOL) != provenance["outcome_sha256"]:
        raise RuntimeError("Candidate outcome pool changed")
    _, _, accepted = study.previous.load_signals()
    historical_days = [date.fromisoformat(value) for value in accepted["days"]]
    train_days = set(historical_days[:30])
    later_days = set(historical_days[30:])
    recent_days = set(study.recent_replay.DAYS)
    rows = pd.read_parquet(POOL).to_dict("records")
    for row in rows:
        row["day"] = date.fromisoformat(row["day"])
    rows.sort(key=lambda row: (row["day"], -float(row["traded_value"]), row["tradingsymbol"]))
    if len(rows) != 84 or len({int(row["sid"]) for row in rows}) != len(rows):
        raise RuntimeError("Unexpected candidate count or duplicate IDs")

    def evaluate(config):
        price, oi, vol5, body, wick, vol1 = config
        selected = []
        taken = set()
        for row in rows:
            if row["day"] in taken:
                continue
            if (row["price_change_pct"] >= price and row["oi_change_pct"] >= oi and
                row["volume_ratio"] >= vol5 and row["body_ratio"] >= body and
                row["wick_ratio"] <= wick and row["v9_1m_volume_ratio"] >= vol1):
                selected.append(row)
                taken.add(row["day"])
        def metric(group):
            chosen = [row for row in selected if row["day"] in group]
            fills = [row for row in chosen if row["filled"]]
            wins = sum(row["net_profit_rupees"] > 0 for row in fills)
            losses = sum(row["net_profit_rupees"] < 0 for row in fills)
            return dict(selected=len(chosen),fills=len(fills),wins=wins,losses=losses,
                win_rate_pct=100*wins/len(fills) if fills else None,
                net_pnl_rs=float(sum(row["net_profit_rupees"] for row in fills)))
        return dict(config=config,first30=metric(train_days),later16=metric(later_days),
            recent3=metric(recent_days),validation19=metric(later_days|recent_days),
            all49=metric(train_days|later_days|recent_days),
            selected_ids=tuple(int(row["sid"]) for row in selected))

    def train_eligible(item):
        metric = item["first30"]
        return metric["fills"] >= 5 and metric["win_rate_pct"] is not None and metric["win_rate_pct"] >= 70

    def train_rank(item):
        metric = item["first30"]
        return (metric["fills"], metric["net_pnl_rs"], metric["win_rate_pct"],
            item["config"][0],item["config"][1],item["config"][2],
            item["config"][3],-item["config"][4],item["config"][5])

    base_1m = (.50,.50,1.30)
    five_stage = [evaluate((p,o,v,*base_1m)) for p,o,v in
                  itertools.product(PRICE,OI,FIVE_VOLUME)]
    five_qualified = [item for item in five_stage if train_eligible(item)]
    if not five_qualified:
        raise RuntimeError("No five-minute configuration passed the training criterion")
    stage1 = max(five_qualified,key=train_rank)
    fixed5 = stage1["config"][:3]
    one_stage = [evaluate((*fixed5,b,w,v)) for b,w,v in
                 itertools.product(BODY,WICK,MINUTE_VOLUME)]
    one_qualified = [item for item in one_stage if train_eligible(item)]
    if not one_qualified:
        raise RuntimeError("No one-minute configuration passed training after five-minute selection")
    stage2 = max(one_qualified,key=train_rank)

    all_items = [evaluate(config) for config in itertools.product(
        PRICE,OI,FIVE_VOLUME,BODY,WICK,MINUTE_VOLUME)]
    unique_sets = len({item["selected_ids"] for item in all_items})
    apparent = [item for item in all_items if item["all49"]["fills"] > 4 and
        item["all49"]["win_rate_pct"] is not None and item["all49"]["win_rate_pct"] >= 70]
    apparent_best = max(apparent,key=lambda item: (
        item["all49"]["fills"],item["all49"]["net_pnl_rs"],
        item["validation19"]["fills"],*item["config"])) if apparent else None
    consistent_apparent = [item for item in all_items if train_eligible(item) and
        item["all49"]["win_rate_pct"] is not None and item["all49"]["win_rate_pct"] >= 70]
    consistent_apparent_best = max(consistent_apparent,key=lambda item: (
        item["all49"]["fills"],item["all49"]["net_pnl_rs"],
        item["validation19"]["fills"],item["config"][0],item["config"][1],
        item["config"][2],item["config"][3],-item["config"][4],
        item["config"][5])) if consistent_apparent else None
    robust = [item for item in all_items if train_eligible(item) and
        item["validation19"]["fills"] >= 3 and
        item["validation19"]["win_rate_pct"] >= 70]
    # Complete grid is saved for audit; do not tune the train-selected preset on validation.
    summary_rows = []
    for item in all_items:
        row = {name:value for name,value in zip(
            ("price_min","oi_min","five_volume_min","body_min","wick_max","minute_volume_min"),item["config"])}
        for bucket in ("first30","later16","recent3","validation19","all49"):
            row.update({f"{bucket}_{key}":value for key,value in item[bucket].items()})
        summary_rows.append(row)
    pd.DataFrame(summary_rows).to_csv(GRID,index=False)
    def record(item):
        if item is None:
            return None
        chosen = [row for row in rows if int(row["sid"]) in item["selected_ids"]]
        return dict(config=dict(zip(("five_minute_price_min","five_minute_oi_min",
            "five_minute_volume_min","one_minute_body_min","one_minute_wick_max",
            "one_minute_volume_min"),item["config"])),
            first30=item["first30"],later16=item["later16"],recent3=item["recent3"],
            validation19=item["validation19"],all49=item["all49"],
            selected=[dict(date=str(row["day"]),symbol=row["tradingsymbol"],
                filled=bool(row["filled"]),exit_reason=row["exit_reason"],
                net_pnl_rs=float(row["net_profit_rupees"])) for row in chosen])
    report=dict(study="Joint 5-minute and 1-minute 09:50 LONG threshold search",
        baseline="No original G3 09:50 LONG; original other slots unchanged",
        fixed_policies=dict(one_entry_per_day=True,picker="max_liquidity",
            exact_next_minute_and_direction_required=True,
            data_quality_flags_rejected=True,
            exit={"initial_stop_pct":1.25,"tightened_stop_pct":1.,
                "tighten_after_minutes":120,"target_pct":1.,"cost_bps":5.}),
        train_sessions=30,later_historical_sessions=16,recent_sessions=3,
        search_levels=dict(price=PRICE,oi=OI,five_volume=FIVE_VOLUME,
            body=BODY,wick=WICK,minute_volume=MINUTE_VOLUME),
        stage1_five_minute_train_selected=record(stage1),
        stage2_one_minute_train_selected=record(stage2),
        grid_configs=len(all_items),unique_order_sets=unique_sets,
        grid_apparent_70pct_count=len(apparent),
        grid_train_and_all49_70pct_count=len(consistent_apparent),
        grid_train_70pct_at_least5fills_and_validation_70pct_at_least3fills_count=len(robust),
        grid_best_apparent=record(apparent_best),
        grid_best_train_and_all49_apparent=record(consistent_apparent_best),
        outcome_pool=str(POOL),outcome_pool_sha256=sha(POOL),
        full_grid_csv=str(GRID),full_grid_sha256=sha(GRID),
        assessment="The chronological check is not untouched external validation because earlier research inspected some later outcomes. Grid best is outcome-selected; do not treat its observed win rate as predictive.",
        production_modified=False)
    OUT.write_text(json.dumps(report,indent=2,allow_nan=False)+"\n",encoding="utf-8")
    for label,item in (("stage1",stage1),("stage2",stage2),("grid_best",apparent_best),
                       ("grid_train_and_all49",consistent_apparent_best)):
        print(label,item["config"] if item else None,
            item["first30"] if item else None,
            item["validation19"] if item else None,
            item["all49"] if item else None,flush=True)
    print("grid",len(all_items),"unique sets",unique_sets,"apparent >=70",len(apparent),
        "chronological numeric pass",len(robust),flush=True)
    print(OUT,flush=True)


if __name__ == "__main__":
    main()
