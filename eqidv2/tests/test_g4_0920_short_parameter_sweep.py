import pandas as pd

from research import g4_0920_short_parameter_sweep as sweep


def _row(policy, *, win=75., net=1., loss=90., portfolio_win=80., baseline_win=80.,
         portfolio_retention=90., setup_retention=90.):
    return dict(period="development35", policy=policy, setup_win_rate_pct=win,
        win_rate_pct=portfolio_win, baseline_win_rate_pct=baseline_win,
        original_winner_profit_retention_pct=portfolio_retention,
        setup_original_winner_profit_retention_pct=setup_retention,
        loss_amount_rs=loss, baseline_loss_amount_rs=100., net_delta_rs=net)


def test_target_only_break_even_includes_costs():
    tiny = next(v for v in sweep.VARIANTS if v.name == "target_011")
    assert round(tiny.reward_after_cost_rs, 8) == 300.
    assert round(tiny.loss_after_cost_rs, 8) == 6250.
    assert round(tiny.target_break_even_win_rate_pct, 6) == 95.419847


def test_selection_requires_all_economic_guards():
    table = pd.DataFrame([_row("baseline", net=0., loss=100.), _row("good")])
    assert sweep.select_on_development(table)[0] == "good"
    for field, value in (("setup_win_rate_pct", 74.99),
                         ("win_rate_pct", 79.99),
                         ("original_winner_profit_retention_pct", 89.99),
                         ("setup_original_winner_profit_retention_pct", 89.99),
                         ("loss_amount_rs", 100.), ("net_delta_rs", 0.)):
        changed = table.copy()
        changed.loc[changed.policy.eq("good"), field] = value
        assert sweep.select_on_development(changed)[0] == "baseline"


def test_later_rows_never_affect_development_selection():
    early = pd.DataFrame([_row("baseline", net=0., loss=100.), _row("good")])
    future = pd.DataFrame([dict(_row("future_winner"), period="later17", net_delta_rs=1e9)])
    assert sweep.select_on_development(pd.concat([early, future], ignore_index=True))[0] == "good"


def test_declared_grid_is_bounded_and_has_one_baseline():
    assert len(sweep.VARIANTS) == 12
    assert [v.name for v in sweep.VARIANTS].count("baseline") == 1
    assert len({v.name for v in sweep.VARIANTS}) == len(sweep.VARIANTS)
    assert all(0 < v.target_pct <= 2 and 0 < v.stop_pct <= 1.2 for v in sweep.VARIANTS)
