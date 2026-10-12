# Kite order readiness: validation only

Run `bat\run_kite_order_readiness_test.bat` to check configured Kite apps without submitting an order. With no arguments it checks all apps and pauses before closing the window. The launcher preserves the tool's exit code.

The tool uses existing credentials. It does not authenticate again, refresh tokens, change strategy configuration, arm LIVE trading, or start workers. The G3 LIVE approval, acknowledgement, dated arm and quantity gates remain unchanged.

## Commands

From the repository root:

```bat
bat\run_kite_order_readiness_test.bat --app all
bat\run_kite_order_readiness_test.bat --app app1
bat\run_kite_order_readiness_test.bat --app app1 --symbol SBIN --side BUY --quantity 1 --order-type MARKET --product MIS --exchange NSE
```

The last command requests a margin calculation only. Its symbol and quantity are preview inputs, not an instruction to trade. There is no `--confirm-live` option.

| Option | Meaning |
| --- | --- |
| `--app` | `all` or `app1` through `app8`; default `all`. |
| `--symbol`, `--side` | Supply both to request a margin preview; side is `BUY` or `SELL`. Omit both for read checks only. |
| `--quantity` | Positive integer; defaults to `1`. Applies only to the preview. |
| `--order-type` | `LIMIT`, `MARKET`, `SL`, or `SL-M`. |
| `--price` | Required for `LIMIT` and `SL`. |
| `--trigger-price` | Required for `SL` and `SL-M`. |
| `--product` | `MIS` or `CNC`. |
| `--exchange` | `NSE` only; this tool covers equity. |

## What it checks

The fixed read requests check broker profile, equity margins, positions and orders. An optional `POST /margins/orders` calculates margins; order placement uses a separate endpoint. See the official [user and funds API](https://kite.trade/docs/connect/v3/user/), [margin calculation API](https://kite.trade/docs/connect/v3/margins/) and [orders API](https://kite.trade/docs/connect/v3/orders/).

Successful reads or a margin preview do **not** establish static-IP authorization, order-write permission, broker acceptance, a future fill, or LIVE strategy readiness. No order-placement, modification or cancellation request is used to test those permissions.

Credentials come from `api_key.txt` / `access_token.txt` for app1 and `api_keyN.txt` / `access_tokenN.txt` for apps 2–8. Do not pass credentials on the command line. Local `auth_v2_state.json` dates are metadata, not proof that a token remains valid. Kite documents next-day 6 AM expiry and possible earlier invalidation; an authenticated read checks validity at the time of the request. See [authentication response attributes](https://kite.trade/docs/connect/v3/user/#response-attributes).

## Results

Sanitized local reports are written to `logs/kite_order_readiness/{timestamp}.json` and `logs/kite_order_readiness/latest.json`. They should contain check results, not credentials, user IDs or raw account responses.

| Exit code | Meaning |
| --- | --- |
| `0` | At least one selected app passed the full read-access checks. This does not mean every configured app passed or that orders are authorized. |
| `2` | No selected app passed the full read-access checks. Inspect the per-app sanitized results. |
| `3` | Setup or report-writing failure. |

Use this dedicated tool instead of `kite_testing.py`, which also contains live order-submission paths and prints broader account information.

An operator who explicitly wants to submit a single order can use the separate [manual LIMIT order tool](MANUAL_KITE_ORDER_TEST.md). This readiness validator has no submission option and its endpoint allowlist remains unchanged.

## Recorded validation — 12 October 2026

The validation ran from 08:47:06 to 08:47:26 IST using `python tools/test_kite_order_readiness.py --app all`. All eight configured apps passed profile, equity-margin, order-list and position-list reads. Each reported the equity segment enabled and a positive available live balance. No existing daily orders or open net positions were reported at that time. Apps can share an account; these counts and balances must not be added across apps.

The published regular-session schedule was before opening. The persisted G3 deployment remained `PAPER_VALIDATION` with LIVE authorization false. No symbol-specific margin calculation was requested, and actual order acceptance/static-IP order permission were not tested. Zero order mutations, token refreshes, strategy activations or scheduler changes were performed.

Sanitized evidence: `logs/kite_order_readiness/20261012T084726592812_52389210.json`. This is a point-in-time check; rerun the launcher for current results.

The regular-session calendar is isolated in `configs/markets/nse_equity_calendar_2026.json`, based on the official [NSE holidays](https://www.nseindia.com/resources/exchange-communication-holidays?apiversion=v2) and [market timings](https://www.nseindia.com/static/market-data/market-timings). The tool labels missing-year calendars, naive timestamps and special-session hours as unverified. Calendar hours do not establish live exchange availability and must be rechecked after exchange circular changes.
