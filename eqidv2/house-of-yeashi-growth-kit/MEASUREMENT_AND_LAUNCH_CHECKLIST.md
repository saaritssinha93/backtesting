# Measurement and Launch Checklist

No campaign should optimize to clicks or page views when the commercial objective is sales. Purchase value must be accurate first.

## 1. Economics worksheet

Complete for Mystery Jar, everyday jewellery, hampers, and ₹999+ bundles:

- Selling price.
- Average discount.
- Product cost.
- Packaging cost.
- Payment fee.
- Shipping paid by the store.
- Expected failed-delivery/RTO cost.
- Replacement/refund allowance.
- Contribution before ads.
- Desired contribution after ads.
- Maximum CAC.
- Target ROAS.

Formulas:

`Contribution before ads = net revenue − variable fulfilment costs`

`Maximum CAC = new-customer AOV × (pre-ad contribution % − desired post-ad contribution %)`

`Target ROAS = AOV ÷ maximum CAC`

## 2. Google stack

In Shopify, connect the Google & YouTube channel to:

- Google Merchant Center.
- Google Analytics 4.
- Google Ads.

Shopify setup guide: https://help.shopify.com/en/manual/online-sales-channels/marketplaces/google/getting-setup/connect

Then:

- Verify `https://www.houseofyeashi.com/` in Search Console.
- Submit `/sitemap.xml`.
- Enable Merchant Center free listings.
- Confirm India shipping settings and return policy.
- Check product approvals, brand, product type/category, identifiers, images, price, and availability.
- Link Google Ads and GA4.
- Set Purchase as the primary Google Ads conversion.
- Enable enhanced conversions through the supported Shopify/Google integration where available.

Google explains that enhanced conversions use hashed first-party data to improve measurement: https://support.google.com/google-ads/answer/9888656

### GA4 events to verify

- `view_item`
- `add_to_cart`
- `begin_checkout`
- `purchase`

For `purchase`, verify:

- One event per transaction.
- Correct transaction ID.
- Correct INR currency.
- Correct order value and discount treatment.
- Correct item IDs matching the feed.
- No duplicate browser/server/Shopify events.

## 3. Meta stack

The public storefront loads Meta Pixel ID `1527660765499372`. Confirm that it belongs to the correct Business Manager and catalogue.

In Shopify Facebook & Instagram settings:

- Connect the correct Business Manager, Page, Instagram professional account, ad account, Pixel/dataset, and catalogue.
- Choose the data-sharing level appropriate for the business’s consent/privacy obligations.
- If using Maximum, Shopify says the store uses Meta Pixel plus Conversions API and current Meta advertising technology: https://help.shopify.com/en/manual/promoting-marketing/analyze-marketing/meta-data-sharing
- Keep the privacy policy and consent handling consistent with the selected level.

Validate in Meta Events Manager:

- `ViewContent`
- `AddToCart`
- `InitiateCheckout`
- `Purchase`

For each event, verify:

- Correct product/content IDs matching the catalogue.
- Correct INR value.
- Pixel/CAPI deduplication using matching event IDs.
- Event Match Quality diagnostics.
- Domain verification and prioritized Purchase event where applicable.

## 4. UTM standard

Use lower-case, stable names:

```text
utm_source=google|facebook|instagram
utm_medium=cpc|paid_social|organic_social|creator
utm_campaign=<channel>_<objective>_<offer>_<yyyymm>
utm_content=<format>_<angle>_<version>
utm_term=<keyword-or-audience-label>
```

Examples:

```text
?utm_source=instagram&utm_medium=paid_social&utm_campaign=meta_sales_mystery_jar_202608&utm_content=reel_unboxing_v1
?utm_source=google&utm_medium=cpc&utm_campaign=search_mystery_jar_202608&utm_content=rsa_v1&utm_term=mystery_jewellery_jar
```

Never place personal data in UTMs.

## 5. Test-order procedure

1. Open a fresh private browser session.
2. Use a dedicated QA UTM link.
3. View an in-stock Mystery Jar variant.
4. Add to cart.
5. Begin checkout.
6. Complete one real low-risk order using an approved payment method.
7. Confirm the order in Shopify.
8. Confirm one Purchase with the same INR value and transaction ID in GA4/Google Ads.
9. Confirm one deduplicated Purchase with matching content IDs/value in Meta.
10. Confirm Merchant Center price, stock, shipping, and return information match the landing page.
11. Refund/cancel the test only through the store’s normal process and document how analytics handles it.

## 6. Launch gates

### Go

- Phone, Terms, shipping, claims, and gift-page placeholder fixed.
- Purchase events correct in Shopify, GA4/Google Ads, and Meta.
- No duplicate purchases.
- Intended feed at least 95% approved.
- Zero price, stock, shipping, and landing-page mismatches.
- Maximum CAC and target ROAS signed off from real costs.
- Creative uses real merchandise and verified claims.
- Landing page is usable on mobile and checkout works.

### No-go

- Unknown contribution margin.
- Contradictory shipping rule.
- Pixel/GA4 Purchase missing or duplicated.
- Ads optimize to ViewContent/AddToCart because Purchase is broken.
- Unverified waterproof/material/review/scarcity claim.
- Feed disapproval for misrepresentation or inconsistent policy.
- Trademark-sensitive product advertising without authenticity/authorization evidence.

## 7. Weekly reporting table

Report by channel, campaign, offer, and creative:

- Spend.
- Impressions and reach.
- Frequency.
- Link clicks and landing-page views.
- Add-to-cart, checkout, purchase.
- Shopify orders and net revenue.
- New customers.
- AOV.
- CAC.
- Contribution after ads.
- ROAS and blended MER.
- Refund/replacement/failed-delivery rate.
- Product-feed disapprovals or stock mismatch.

Use platform attribution for optimization, but use Shopify orders and contribution as the commercial source of truth.

## 8. Access needed for execution

Do not paste passwords, OTPs, card details, or API keys into chat. Use each platform’s role/invite system.

To build drafts and validate setup, provide access to:

- Shopify collaborator/staff role with Products, Marketing, Apps/Channels, Themes, and Analytics as needed.
- Google Search Console property.
- Merchant Center admin/standard access as appropriate.
- Google Ads standard/admin access as appropriate.
- GA4 property access.
- Meta Business Manager access to Page, Instagram, ad account, Pixel/dataset, and catalogue.

Creating drafts can be done without spend. Publishing campaigns or increasing budgets should require your explicit final approval of campaign, audience, destination, daily/lifetime budget, schedule, and payment account.

## 9. Inputs still required from the owner

- 30-day media budget ceiling.
- Target geography: India-wide or selected cities/states.
- Average order value and new-customer AOV.
- Product cost and contribution margin by the main offer.
- Current monthly sessions, conversion rate, and order count.
- COD/prepaid mix and failed-delivery/RTO rate, if COD is offered.
- Top-margin and consistently stocked products.
- Documentary support for material, plating, anti-tarnish, and water claims.
- Whether the business meets Google Business Profile’s in-person eligibility.
