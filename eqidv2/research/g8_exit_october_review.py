"""October slice of the already tested G8 exit rules; no parameter selection changes."""
import json
from pathlib import Path

import numpy as np
import pandas as pd

from research import g8_regime_exit_research as study


def run():
    core = study.core
    pointer = json.loads((core.OUTPUT_ROOT / "latest_exit_regime_research.json").read_text())
    parent = Path(pointer["run_dir"])
    study.verify(parent)
    source = core.load_sources()
    days = [d for d in source["days"] if d.startswith("2026-10-")]
    source["unfiltered"] = source["unfiltered"].loc[source["unfiltered"].day.isin(days)].copy()
    f = pd.read_csv(parent / "features.csv").set_index("sid", drop=False)
    f = f.loc[f.day.isin(days)].copy()
    allowed = core.allowed_for(f, dict(mode="persistent_direction", window=30, threshold=2.))
    out = parent / "october_review"
    out.mkdir(exist_ok=False)
    core.dump(out / "scope.json", dict(days=days, sessions=len(days),
        rule="Re-evaluate all 27 previously declared policies on October only; no new thresholds",
        independent_holdout=False, promotion=False, missing_session="2026-10-01: sealed inputs unavailable"))
    rows, daily = [], []
    previous = pd.read_csv(parent / "parameter_trials.csv")
    whole = previous.loc[previous.period.eq("all")].set_index("policy")
    baseline = pd.read_csv(parent / "baseline/portfolio_trades.csv")
    check = study.metrics(baseline, days)
    for spec in study.specifications():
        name = spec["id"]
        raw = study.replay(source, study.settings(f, spec))
        book = study.allocate_variant(raw, allowed, source["config"], name)
        stats = study.metrics(book, days)
        if name == study.BASE:
            for key in ("trades", "wins", "losses", "net_pnl_rs"):
                assert np.isclose(stats[key], check[key], atol=1e-6, rtol=0), key
        rows.append(dict(policy=name, **stats,
            full_history_profit_rs=whole.loc[name, "net_pnl_rs"],
            full_history_win_rate_pct=whole.loc[name, "win_rate_pct"],
            full_history_losing_days=whole.loc[name, "losing_days"],
            full_history_drawdown_rs=whole.loc[name, "daily_close_drawdown_rs"]))
        for day in days:
            daily.append(dict(policy=name, day=day, **study.metrics(book, [day])))
    table = pd.DataFrame(rows)
    b = table.set_index("policy").loc[study.BASE]
    table["october_net_change_rs"] = table.net_pnl_rs - b.net_pnl_rs
    table["october_loss_sum_change_rs"] = table.negative_trade_sum_abs_rs - b.negative_trade_sum_abs_rs
    table.sort_values("net_pnl_rs", ascending=False).to_csv(out / "october_comparison.csv", index=False)
    daily = pd.DataFrame(daily)
    daily.to_csv(out / "october_daily_long.csv", index=False)
    daily.pivot(index="day", columns="policy", values="net_pnl_rs").to_csv(out / "october_daywise.csv")
    core.dump(out / "manifest.json", dict(parent_manifest_sha256=core.sha(parent / "manifest.json"),
        code={str(Path(__file__).relative_to(core.ROOT)): core.sha(Path(__file__))},
        files={p.name:core.sha(p) for p in out.iterdir() if p.is_file()}))
    print("OUTPUT", out)
    print(table.sort_values("net_pnl_rs", ascending=False)[["policy", "trades", "win_rate_pct", "net_pnl_rs", "losing_days", "daily_close_drawdown_rs", "full_history_profit_rs"]].to_string(index=False))


if __name__ == "__main__":
    run()
