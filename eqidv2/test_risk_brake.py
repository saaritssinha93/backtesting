"""P0-18 acceptance test for the MTM-aware risk brake.

Acceptance (rev-2 P0-18): a book of open losers totalling -Rs10k MTM with ZERO
realized losses trips the brake within one evaluation; the event shows an
MTM-trigger; the throttle and per-setup caps are observable in reject reasons.
"""
import eqidv2_risk_brake as rb


def main() -> int:
    cfg = rb.BrakeConfig(daily_loss_rs=10000, per_trade_loss_rs=5000, mtm_enabled=True,
                         max_entries_per_day=60, per_setup_concurrency_cap=3, flatten_on_breach=False)
    ok = True

    # 1) MTM-only breach: zero realized, -Rs10k open MTM -> trips with daily_mtm
    positions = {
        "a": {"ticker": "AAA", "side": "LONG", "entry_price": 100.0, "quantity": 200},   # -Rs4000 @95
        "b": {"ticker": "BBB", "side": "SHORT", "entry_price": 50.0, "quantity": 200},    # -Rs6000 @80? compute
    }
    last = {"AAA": 80.0, "BBB": 80.0}  # AAA: (80-100)*200=-4000 ; BBB short: (50-80)*200=-6000 => -10000
    mtm = rb.open_mtm_rs(positions, last)
    d = rb.evaluate_daily_brake(realized_rs=0.0, open_mtm_rs=mtm, cfg=cfg)
    cond = d.tripped and d.trigger == "daily_mtm"
    print(f"[{'PASS' if cond else 'FAIL'}] MTM-only breach trips (mtm=Rs{mtm:,.0f}, trigger={d.trigger}) :: {d.reason}")
    ok &= cond

    # 2) legacy realized-only brake would NOT have tripped (realized=0)
    legacy = rb.evaluate_daily_brake(realized_rs=0.0, open_mtm_rs=0.0, cfg=cfg)  # mtm ignored
    cond = not legacy.tripped
    print(f"[{'PASS' if cond else 'FAIL'}] realized-only (mtm=0) does NOT trip — the gap P0-18 closes")
    ok &= cond

    # 3) realized breach (net still below limit) -> daily_realized trigger
    d = rb.evaluate_daily_brake(realized_rs=-10500.0, open_mtm_rs=0.0, cfg=cfg)
    cond = d.tripped and d.trigger == "daily_realized"
    print(f"[{'PASS' if cond else 'FAIL'}] realized breach trips with daily_realized trigger")
    ok &= cond

    # 3b) positive open MTM can offset a realized loss below the limit (net-aware)
    d = rb.evaluate_daily_brake(realized_rs=-10500.0, open_mtm_rs=2000.0, cfg=cfg)
    cond = not d.tripped  # net -8500 is above -10000
    print(f"[{'PASS' if cond else 'FAIL'}] net-aware: +MTM offsets realized (net -8500 does not trip)")
    ok &= cond

    # 4) entry throttle observable
    allowed, reason = rb.entry_allowed(0.0, 0.0, entries_today=60, setup="X", setup_open_count=0, cfg=cfg)
    cond = (not allowed) and "THROTTLE" in reason
    print(f"[{'PASS' if cond else 'FAIL'}] per-day throttle blocks + is greppable :: {reason}")
    ok &= cond

    # 5) per-setup concurrency cap observable
    allowed, reason = rb.entry_allowed(0.0, 0.0, entries_today=0, setup="C_OR_BREAKDOWN", setup_open_count=3, cfg=cfg)
    cond = (not allowed) and "SETUP_CONCURRENCY" in reason
    print(f"[{'PASS' if cond else 'FAIL'}] per-setup concurrency cap blocks + is greppable :: {reason}")
    ok &= cond

    # 6) healthy book is allowed
    allowed, reason = rb.entry_allowed(500.0, -1000.0, entries_today=5, setup="X", setup_open_count=0, cfg=cfg)
    cond = allowed and reason == ""
    print(f"[{'PASS' if cond else 'FAIL'}] healthy book allows entry")
    ok &= cond

    # 7) flatten flag-gated: OFF -> no flatten even when tripped
    d_off = rb.evaluate_daily_brake(0.0, -20000.0, rb.BrakeConfig(flatten_on_breach=False))
    d_on = rb.evaluate_daily_brake(0.0, -20000.0, rb.BrakeConfig(flatten_on_breach=True))
    cond = d_off.tripped and not d_off.flatten and d_on.tripped and d_on.flatten
    print(f"[{'PASS' if cond else 'FAIL'}] flatten is flag-gated (off={d_off.flatten}, on={d_on.flatten})")
    ok &= cond

    print("RESULT:", "PASS" if ok else "FAIL")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
