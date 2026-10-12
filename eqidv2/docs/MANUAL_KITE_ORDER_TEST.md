# Manual single LIMIT order

`tools/manual_kite_order_test.py` is a separate, operator-run tool. It previews by default. **With `--submit` and the exact interactive confirmation, it can submit one real order.** The [readiness validator](KITE_ORDER_READINESS_TEST.md) remains read-only.

This tool supports one explicitly selected app (`app1`–`app8`), one NSE symbol, BUY or SELL, a positive integer quantity, MIS or CNC, and a LIMIT price with at most two decimal places. Order variety is regular and validity is DAY. It does not choose a security, quantity or price for you, place protective exits, or activate the G3 strategy.

Replace every uppercase placeholder below with your own choice; these are templates, not trading recommendations. First preview:

```text
bat\run_manual_kite_order_test.bat --app APP --symbol SYMBOL --side SIDE --quantity QUANTITY --price LIMIT_PRICE --product PRODUCT
```

For a manual submission, use an interactive terminal and add a new intent label (8–64 letters, digits, hyphens or underscores):

```text
bat\run_manual_kite_order_test.bat --app APP --symbol SYMBOL --side SIDE --quantity QUANTITY --price LIMIT_PRICE --product PRODUCT --submit --intent-id UNIQUE_INTENT_ID
```

The tool displays the exact app, order and margin estimate. It requires sufficient reported balance for that estimate and asks you to type an exact confirmation line. There is no `--yes`, automatic app fallback or automatic broker retry. Redirected/noninteractive input cannot confirm a submission.

Before its single `POST /orders/regular`, the tool exclusively claims `logs/manual_kite_order_test/intents/UNIQUE_INTENT_ID/` and atomically writes `intent.json` with a unique broker tag. A completed attempt writes `result.json`. An existing intent directory blocks reuse even if a crash left it empty. Preserve these files and do not delete an intent to retry it.

A temporary Windows file lock permits up to 175 milliseconds of local atomic-rename retries. This never repeats the intent claim or broker request; persistent write failure remains explicit.

| Result | Meaning |
| --- | --- |
| `PREVIEW_ONLY` | No order submission was attempted. |
| `BROKER_ACKNOWLEDGED` | A broker order ID was received; check its status and fills in Kite. |
| `BROKER_REQUEST_REJECTED` | The broker returned an explicit request error; no retry is performed. |
| `UNKNOWN_ACCEPTANCE` | A submission was attempted but its result is unclear. Review Kite using the tag, symbol and quantity. Do not retry or create a replacement intent until you have resolved the result. |
| `INTENT_ALREADY_EXISTS` | That intent is permanently consumed locally; review its files and Kite. |

Exit `0` means preview completion or receipt of an order acknowledgement; it does not mean a fill. Exit `2` means blocked, cancelled, rejected or uncertain. Exit `3` means setup or local persistence failure. If result persistence fails **after submission**, review Kite before further action.

Existing credentials are loaded only for the selected app; tokens are never refreshed. Output omits credential values and raw account responses. It retains the operator's order details, unique tag and any returned order ID needed for manual reconciliation. G3 deployment records, arm/kill controls, scheduler tasks and workers are untouched.

The margin preview uses the separate [Kite margin-calculation API](https://kite.trade/docs/connect/v3/margins/). Kite documents order placement separately and states that an order ID does not establish exchange execution; see the [official orders documentation](https://kite.trade/docs/connect/v3/orders/). Read access or a margin estimate cannot prove static-IP order-write permission.
