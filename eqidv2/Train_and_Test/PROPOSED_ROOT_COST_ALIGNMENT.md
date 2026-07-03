# Root cost/sizing alignment (v11 ↔ tuner ↔ live)  [✅ APPLIED 2026-06-23, default-OFF]

> **Status: APPLIED** to `avwap_5min_ID_v11_backtesting.py` after sign-off, via the
> **non-breaking, default-OFF** mechanism in §2 (module globals `_V11_COST_MODEL`/`_V11_SLIPPAGE_BPS`,
> set only by `main()`). Validated: v11 parses; **on import the defaults are `flat_bps`/0 bps, so the
> live stack — which imports this module — is byte-identical and unaffected**; `--cost_model`/
> `--slippage_bps` register; statutory math correct. **Remaining:** the post-close full-backtest
> reconciliation run (§5) — not yet executed (heavy job, run after 15:30 IST, ≤8 workers).
> The exact change applied is below (implemented via module globals rather than threading
> through every signature — lower-risk, no caller touched).

## 1. Why — three stacked inconsistencies between the tuner and the v11 backtest

The Train_and_Test tuner and the v11 backtester resolve the *same* trades to *different* P&L,
which is why their "net PF" numbers aren't comparable (and why neither matches live). Three
separate mismatches, all in the resolution/cost path:

| Dimension | v11 backtest (`_resolve_trade_1m_entry`) | Tuner (`setup_train_test`) | Live (V7) |
|---|---|---|---|
| **Notional / sizing** | `gross = pct × v6.EFFECTIVE_NOTIONAL` = **Rs 50,000** (10k margin × 5) | `qty = V7_SIGNAL_NOTIONAL_RS/price`, `gross = (exit−entry)×qty` ≈ **Rs 100,000** (20k × 5) | **Rs 100,000** (`V7_SIGNAL_NOTIONAL_RS`) |
| **Cost model** | flat `v6._net_pnl_rs`: `cost = 50k × (16 bps + 3 bps on SL)/1e4` ≈ **Rs 80–95/trade** | per-trade **statutory** (`nse_intraday_costs`) + **15 bps/leg spread** | statutory at fill time |
| **Slippage** | **none** (raw 1-min open; the 5 bps `V7_PAPER_SLIPPAGE_PCT` is used only in the live-parity/signals path, not here) | 15 bps/leg on **both** entry & exit | real bid-ask at fill |

**Net effect:** v11 gross ≈ ½ the tuner's gross (50k vs 100k), and v11 cost is a flat ~Rs 80
while the tuner's is proportional + spread. So the two disagree on rupee P&L *and* on net PF
(cost as a share of gross differs), and **v11 is the optimistic one** (bigger relative gross,
smaller relative cost, no slippage). The live PF-0.25 reality sits below both.

> The 2× **notional** mismatch is the most surprising and the most consequential — it is a
> pure sizing inconsistency that has nothing to do with cost philosophy and should be fixed
> regardless of which cost model wins.

## 2. What — non-breaking, default-OFF additions to the v11 backtester

Add a cost-model + slippage option to `avwap_5min_ID_v11_backtesting.py` that, when enabled,
makes v11 resolve P&L **exactly the way the tuner (and live) does**: live notional, statutory
costs, per-leg spread. Default stays the current flat-bps/50k path, so nothing changes unless
asked.

### 2a. CLI (near the existing `--cost_bps`, line ~4378)
```python
ap.add_argument("--cost_model", choices=["flat_bps", "statutory"], default="flat_bps",
                help="flat_bps = legacy v6 flat cost on EFFECTIVE_NOTIONAL (default, unchanged); "
                     "statutory = per-trade NSE costs on the LIVE notional (matches the tuner + live)")
ap.add_argument("--slippage_bps", type=float, default=0.0,
                help="adverse per-leg slippage/half-spread (bps) on entry+exit, statutory mode only "
                     "(set 15 to match the tuner's default)")
```
Thread `args.cost_model` / `args.slippage_bps` through `_resolve_trades(..., cost_bps, label)`
→ `_resolve_trade_1m_entry(row, cost_bps, cost_model, slippage_bps)` (add the two params with
defaults `"flat_bps"`/`0.0` so every other caller is unaffected).

### 2b. The resolution block (`_resolve_trade_1m_entry`, the `v6._net_pnl_rs` call ~line 584)
```python
# --- BEFORE ---
net, gross, cost = v6._net_pnl_rs(res.pnl_pct_price, res.outcome, cost_bps)

# --- AFTER (default branch identical to before) ---
if cost_model == "statutory":
    import nse_intraday_costs as _nse
    side = str(row["side"]).upper()
    qty = max(1, int(V7_SIGNAL_NOTIONAL_RS / entry_px))      # LIVE notional, like the tuner
    s = slippage_bps / 1e4
    fill   = entry_px * (1 + s) if side == "LONG" else entry_px * (1 - s)
    exit_p = res.exit_price * (1 - s) if side == "LONG" else res.exit_price * (1 + s)
    net  = _nse.net_pnl(fill, exit_p, qty, side)             # statutory, per-trade
    gross = (exit_p - fill) * qty if side == "LONG" else (fill - exit_p) * qty
    cost = gross - net
else:                                                        # flat_bps (legacy, default)
    net, gross, cost = v6._net_pnl_rs(res.pnl_pct_price, res.outcome, cost_bps)
```
(`entry_px` is already in scope from `_first_1m_entry`; `V7_SIGNAL_NOTIONAL_RS` is a module
constant.) Everything downstream — the `v6_gross_pnl_rs / v6_cost_rs / v6_net_pnl_rs` record
fields — is unchanged in shape.

## 3. Equivalence guarantee

| Run | Produces |
|---|---|
| `--cost_model flat_bps` (default) | **byte-identical** to today |
| `--cost_model statutory --slippage_bps 15` | matches the tuner's default (`COST_MODEL=statutory`, `SLIPPAGE_BPS=15`) modulo integer-qty rounding |
| tuner `--cost_model flat_bps --slippage_bps 0` | ≈ matches v11's legacy path (the tuner already supports this, batch-3) |

So after this, **both engines can be run in either mode** and reconciled head-to-head; the
recommended production mode is `statutory` + 15 bps on **both**, on the **100k live notional**.

## 4. The notional decision (the part that needs your call)

Standardize the P&L notional. Two options:

- **(Recommended) Live basis — Rs 100k everywhere.** v11 statutory mode already uses
  `V7_SIGNAL_NOTIONAL_RS`; this matches the tuner and what live actually trades. The legacy
  flat-bps path keeps 50k (so old numbers are reproducible), but the *production* comparison is
  all-100k.
- **Legacy basis — Rs 50k everywhere.** Would instead require lowering the tuner +
  `V7_SIGNAL_NOTIONAL_RS` to 50k — but that contradicts the live executor's actual 20k×5 sizing,
  so this is **not** recommended.

## 5. Validation plan (post-close, ≤8 workers)

1. v11 `--cost_model flat_bps` on a fixed range → confirm **identical** to a pre-change run (diff the trades CSV).
2. v11 `--cost_model statutory --slippage_bps 15` on the same range.
3. Tuner `train_test_conf.py` on the same setups/window in (a) statutory+15 and (b) flat_bps+0.
4. Reconcile: v11-statutory book net ≈ tuner-statutory book net (within qty-rounding); v11-flat ≈ tuner-flat. The tuner's per-run **cost reconciliation** line (batch-3) already prints both.

## 6. Risk / rollback

- **Risk:** low. Default path untouched; the new branch is opt-in and self-contained.
- **Blast radius:** only `avwap_5min_ID_v11_backtesting.py` (+ 1-line import of `nse_intraday_costs`,
  already a repo module). No change to `final_setup_conf.py`, the live scanner, or the overlay.
- **Rollback:** delete the two CLI args + the `if cost_model == "statutory"` branch.

## 7. Sign-off

This file is the reviewable diff. **Nothing has been applied.** On your OK I will:
1. apply the patch to `avwap_5min_ID_v11_backtesting.py`,
2. run the §5 validation (after close, ≤8 workers),
3. record the reconciliation in PROJECT_OVERVIEW §11 and close the G4 "open" item.
