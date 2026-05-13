# v17D Runbook

Operational reference for running v17D in production. Keep this beside the
keyboard. Updated end-of-day after every incident.

---

## Quick reference

| Action | Command / step |
|---|---|
| Start morning session | See **Morning startup** below |
| Halt all trading immediately | `touch ~/v17D_kill` |
| Halt via env (current process) | `export V17D_KILL_SWITCH=1` |
| Resume after weekly DD kill | `touch ~/v17D_rearm` |
| End-of-day reconciliation | `python -m eqidv2.v17D_reconcile` |
| View today's logs | `ls ~/v17D_logs/*$(date +%Y%m%d)*` |
| Tail decisions log live | `tail -f ~/v17D_logs/decisions_$(date +%Y%m%d).jsonl` |
| Check kill-switch state | `python -m eqidv2.v17D_kill_switch` |
| Reload config (no restart) | `kill -HUP <pid>` (if SIGHUP handler installed) |

---

## Morning startup (08:30 IST)

Run in order. Each step gates the next.

1. **Power / network check** — laptop charged > 50%, ethernet preferred over wifi.
2. **Check broker portal** — log in, verify margin, no pending orders from yesterday.
3. **Refresh F&O list** if Monday or first day of month:
   ```bash
   # Download from NSE; replace eqidv2/configs/fo_list.json
   ```
4. **Pre-market sanity check:**
   ```bash
   python -m eqidv2.v17D_pre_flight
   # Expected: all green ticks
   # If any FAIL: investigate before starting
   ```
5. **Check yesterday's reconciliation report:**
   ```bash
   cat ~/v17D_logs/daily_summary_$(date -d yesterday +%Y%m%d).txt
   # Verify: PnL match, no broker mismatches, no missed exits
   ```
6. **Verify kill-switch state:**
   ```bash
   python -m eqidv2.v17D_kill_switch
   # Expected: all clear, no manual halt active
   ```
7. **Earnings check** — for each ticker on watchlist, mark restricted in config if earnings today.
8. **Start the runner:**
   ```bash
   python -m eqidv2.v17D_runner --config eqidv2/configs/v17D.yaml
   # Logs to ~/v17D_logs/
   ```
9. **Tail decisions log** in another terminal to monitor first 30 min.

---

## During the session

**Monitor:**
- Open positions count vs daily caps (15 LONG / 10 SHORT)
- Per-position PnL (early SL hits = recheck SL placement)
- Governor drop counts (G1 time, G2 cap, G3 loss-streak, G4 cool, etc.)
- Sector concentration (max 4 per sector per side)

**Manual interventions (rare):**

| Situation | Action |
|---|---|
| Strategy firing too many trades on one stock | Add ticker to today's restricted list, restart runner |
| Broker connection drops | Runner should reconnect automatically; if 2+ min, manual intervention |
| Suspicious fill (slippage > 0.5%) | Note in journal; if persistent, raise slip floor in cost model |
| Fat-finger order (qty wildly off) | Cancel via broker app, then `touch ~/v17D_kill` to halt |
| Position size unexpectedly large | Halt, check vol-targeted sizing logic |

---

## Daily DD kill (intraday <= -2.5%)

**Automatic:** runner halts new entries, holds open positions to their SLs / EOD.

**Manual response:**
1. Note time, P&L at halt, last 5 trades in journal.
2. Wait. Do NOT override the kill mid-session.
3. After 15:30 close, run reconciliation as usual.
4. Review which setup(s) drove the drawdown.

---

## Weekly DD kill (5-day rolling <= -5%)

**Automatic:** weekly kill marker file created; next session won't start until manual re-arm.

**Manual response:**
1. Halt session immediately (kill-switch).
2. Off-day analysis:
   - Per-setup PF over the losing week vs prior weeks
   - Drift detection report (Step 4.5) for any features that shifted
   - Sector concentration in losing trades
3. Decide: resume at full size / resume at 0.50x / pause for retune / drop a setup.
4. Document decision in journal.
5. Re-arm:
   ```bash
   touch ~/v17D_rearm
   # Verify kill cleared:
   python -m eqidv2.v17D_kill_switch
   ```

---

## End-of-day (15:30+ IST)

1. **Verify all positions closed:**
   ```bash
   python -m eqidv2.v17D_position_check
   # Should show zero open positions
   ```
   Any straggler → close manually via broker.
2. **Stop the runner:**
   ```bash
   kill -TERM <runner_pid>   # graceful
   ```
3. **Reconcile:**
   ```bash
   python -m eqidv2.v17D_reconcile --date today
   # Output: ~/v17D_logs/reconciliation_YYYYMMDD.txt
   ```
   Mismatches > Rs 100 → investigate before next session.
4. **Drift check:**
   ```bash
   python -m eqidv2.v17D_drift_check --date today
   # KS test on each feature vs in-sample distribution
   # Flags appear in dashboard
   ```
5. **Daily journal entry** (5 min):
   - Total PnL, trades count, win/loss
   - Anomalies, surprises, setups that misbehaved
   - Market regime observation
   - Anything to investigate tomorrow

---

## Position stuck open at EOD

**Definition:** runner reports a position open after 15:25 IST.

1. **Don't panic.** Verify via broker app first (sometimes the runner's view is stale).
2. **If genuinely open:**
   - Manually close at market via broker.
   - Update internal ledger via `python -m eqidv2.v17D_manual_exit --ticker XYZ --price NN.NN`.
3. **Investigate cause:**
   - Order rejection? (insufficient margin, circuit limit)
   - Broker disconnect at exit time?
   - SL/TGT not placed? (modify-order failure)
4. **Document in journal.** If recurring, add automated alert.

---

## Manual override procedures

**Halt one ticker:**
1. Edit `eqidv2/configs/v17D.yaml`, add ticker to `universe.restricted_today` (TODO: add this field).
2. SIGHUP the runner OR restart.

**Halt one setup:**
1. Edit `eqidv2/configs/v17D.yaml`, set `setups.<SETUP_ID>.enabled: false`.
2. SIGHUP / restart.

**Reduce size globally:**
1. Edit `trading.default_position_size_rs` to lower value.
2. SIGHUP / restart.
3. Note in journal *why*.

---

## Escalation contacts

| Issue | Contact | Method |
|---|---|---|
| Broker tech issue (orders failing) | Broker support | Phone + email |
| Data feed down | Data vendor | Email |
| Strategy logic concern | (your CA / advisor) | Phone |
| Tax / compliance question | CA | Email |
| Regulatory event (SEBI circular) | CA | Phone |

---

## Rare emergency procedures

**Power loss during session:**
1. As soon as power returns, log in to broker app, **check open positions first**.
2. If positions still open with no risk control: place manual stop-loss orders.
3. Then start runner with extra caution; verify ledger reconciles with broker.
4. Document gap in journal.

**Internet loss during session:**
1. Phone-tether immediately (have plan ready).
2. If outage > 5 min, halt: positions are at risk without monitoring.
3. Manual square-off if uncertain.

**Wrong config deployed:**
1. Halt immediately.
2. `git checkout HEAD~1 eqidv2/configs/v17D.yaml`
3. Restart with last-known-good config.
4. Investigate diff before re-deploying.

---

## Pilot ramp schedule (Phase 5)

| Day | Size mult | Action |
|---|---|---|
| 1 | 0.10x | Monitor every trade, journal each |
| 2 | 0.10x | Same |
| 3 | 0.10x | Same; review at EOD whether to ramp |
| 4 | 0.30x | Reduce monitoring frequency to per-hour |
| 5 | 0.30x | |
| ... | | |
| 11 | 0.50x | Compare PF + slippage to paper |
| 14 | 0.50x | Final review for full-size approval |
| 15+ | 1.00x | Full size if all gates passed |

**Halt the ramp** if any of:
- Live PF < 60% of paper PF over the last 5 days
- Any production incident (kill-switch trip, reconciliation mismatch, missed exit)
- Personal: stress level too high for sober decisions

---

## Checklist for first live trading day

- [ ] Phase 0–4 gates all passed; date-tracked in roadmap
- [ ] Pilot ramp schedule reviewed
- [ ] Capital allocated and ring-fenced (don't co-mingle with savings)
- [ ] On-call contacts saved in phone
- [ ] CA briefed on intraday PnL reporting
- [ ] Backup broker credentials accessible
- [ ] Power backup plan tested (UPS, mobile hotspot)
- [ ] Daily journal template open
- [ ] Tea / coffee / snacks (this is a discipline exercise)
