# PROPOSED FIX — 2026-06-22  [APPLIED 2026-06-22 — config edited, awaiting process restart]

> **STATUS: APPLIED to `final_setup_conf.py`** via an appended reversible
> `_LIVE_DEMOTION_2026_06_22` transform. Validated: FINAL_SETUP_CONF 16→12; the 4
> setups moved to RESEARCH_WATCH_CONF (enabled=False). Backup:
> `final_setup_conf.py.bak_20260622_demote`.
> **Takes effect on next process start** — the running scanner/entry-engine hold the
> old 16-setup conf in memory (the bootstrap imports the cached module, no live reload).
> The 3 `run_conf_paper_*.bat` tasks relaunch at 09:00, so it auto-applies next session
> (2026-06-23). For sooner, restart those 3 tasks **after market close** (not mid-session).


Stop the live conf paper bleed (conf-era 06-16..06-22: −Rs 29,053, PF 0.25). Evidence:
`Train_and_Test/ANALYSIS_2026-06-22.md` §B/§C and `live_paper_holdout.py`.

This is a **config-only** change to the single source of truth `final_setup_conf.py`
(repo root — cannot move; live stack imports it in place). It is **reversible** and needs
**no code/wiring/overlay change** (the 06-16 non-conf trades were a one-day bootstrap
artifact; 06-17→today the live book is already conf-only).

## The change: demote 4 overfit, live-losing setups
Move these 4 entries OUT of `FINAL_SETUP_CONF` and INTO `RESEARCH_WATCH_CONF`
(add `"enabled": False` + a `live_demotion` note). The bootstrap trades everything in
`FINAL_SETUP_CONF`, so this removes them from the live book immediately on next scanner cycle.

| setup | live (06-16..22) | why demote |
|---|---|---|
| P_PDH_BREAK_RETEST_LONG | −14,497 / 40t / PF 0.25 | gate enforced but doesn't generalize; 0.5/0.6 scalp at ~13 trades/day = death by cost |
| L_RS_LEADER_VWAP_HOLD | −6,619 / 13t / PF 0.15 | mask-only (no premom gate); 8% win live vs prov test 3.82 |
| V_RECLAIM_PULLBACK_LONG | −1,937 / 3t / PF 0.00 | 0% win live vs prov test 5.28 |
| E_ORB_RETEST_HOLD_LONG | −1,442 / 5t / PF 0.01 | 20% win live vs prov test 2.50 |

**Impact:** removes ~−Rs 24k of the −Rs 29k. Book drops from 16 → 12 active setups.

## Kept (for now)
- `E_VWAP_LOSE_EARLY_SHORT` (−241, PF 0.92 — ~breakeven live; watch, don't cut yet).
- The 11 other conf setups (thin/no live trades in the window → insufficient live evidence
  to cut; revisit after P1 regen + band re-tune).

## Suggested `RESEARCH_WATCH_CONF` note per demoted setup
```python
"provenance": {
    ...existing...,
    "live_demotion": {
        "demoted_on": "2026-06-22",
        "reason": "live conf paper 06-16..06-22 net loser; backtest test-PF did not generalize",
        "live": {"net_rs": <from table>, "trades": <n>, "pf": <pf>, "win_pct": <w>},
        "re_validation_trigger": "band-objective re-tune on regenerated June pool must show "
                                 "test PF>=1.3 + day_block_p<0.10 before re-promotion",
    },
},
"enabled": False,
```

## Apply procedure (after sign-off)
1. Back up `final_setup_conf.py`.
2. Move the 4 dict blocks into `RESEARCH_WATCH_CONF`, add `enabled: False` + `live_demotion`.
3. `py -3.12 -c "import final_setup_conf as fc; print(len(fc.FINAL_SETUP_CONF))"` → expect 12.
4. No restart needed if the scanner re-reads conf each cycle; otherwise restart the 3
   `run_conf_paper_*.bat` tasks **after market close**.
5. Confirm next session: `py -3.12 Train_and_Test/live_paper_holdout.py` shows the 4 gone.

## Reversal
Move the blocks back into `FINAL_SETUP_CONF` (or set `enabled` back) — fully reversible.
