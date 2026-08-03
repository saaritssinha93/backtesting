# V7 / V11 parity test — 29 July 2026

## Before 09:00 IST

1. Run `bat\run_v7_v11_parity_preflight.bat`.
2. Continue only when the final state is `READY`. Pre-market runtime-manifest
   warnings are expected because the workers have not started yet.
3. Confirm the three scheduled tasks are enabled for 09:00:
   scanner, 1-minute entry engine, and V7 paper executor.

## After launch

At or after 09:02, run:

```bat
bat\run_v7_v11_parity_preflight.bat --require-running
```

All checks must pass. In particular, each fresh runtime manifest must resolve:

- `final_setup_conf_v11_working`
- the frozen 11-setup active book
- its matching `run_conf_paper_*.bat` launcher

## During the session

For every due 5-minute slot:

1. The scanner must publish the final candidate JSON before its completion
   marker.
2. The completion marker hash must match that exact JSON.
3. The entry engine must consume only that exact slot—never `latest` or a
   previous slot.
4. Candidate rows must contain frozen signal ADX, RSI, volume ratio, bar-close
   time, and decision-ready time.
5. Entry decisions must report feature version
   `shared_immutable_slot_features_v1`.
6. No entry may precede `decision_ready_at_ist`.

Latency target (measured from the intended T+1 entry instant, not from the
earlier candidate publication):

- intended-entry to signal-written: at most 10 seconds
- operational goal: at most 5 seconds

## After 15:30

Run the normal V11 daily report for `2026-07-29`. Accept parity only if:

- candidate IDs and candidate replacement decisions match;
- setup, side, entry selection, stop, target, and quantity match;
- no V11-only backdated entries exist;
- every V7-only or V11-only row has a named funnel rejection or operational
  cause;
- strategy P&L and operational misses are reported separately;
- remaining P&L differences are explained only by actual fill price and costs.

Do not change setup thresholds during this test day. Preserve the frozen
manifests, slot candidate JSONs, completion markers, immutable 1-minute slot
files, funnels, signals, and paper trades until the comparison is complete.
