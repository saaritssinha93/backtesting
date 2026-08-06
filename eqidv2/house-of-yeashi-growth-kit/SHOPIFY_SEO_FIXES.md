# Shopify, SEO, Trust, and Feed Fixes

Audit date: 5 August 2026

## Priority 0 — complete before paid launch

### 1. Make business information consistent

- Correct the privacy-policy phone from `+91 93102322798` to the verified store number.
- Keep the same business name, support email, phone, and mailing address in Contact, privacy, footer, checkout, Merchant Center, Meta, and Google Ads.
- Add a Terms of Service policy and footer link.

### 2. Decide and publish one shipping rule

Current conflict:

- Homepage: “free shipping on every jar.”
- Shipping policy: ₹100 below ₹999; free prepaid shipping above ₹999.

If jars truly ship free, add that explicit exception to the policy, Shopify shipping profile/discount, checkout, and Merchant Center. If not, remove the homepage claim and do not use it in ads.

### 3. Use a verified claims register

Create a simple product metafield/table with:

- Base material.
- Plating and thickness, if documented.
- Nickel/lead status, if tested.
- Anti-tarnish status and evidence.
- Water resistance level and care limits.
- Warranty/guarantee, if any.
- Supplier/testing document link.

Render only verified claims on product pages and feeds. Do not globally print “waterproof” while some product care copy says to avoid water.

### 4. Repair the gift landing page

- Remove “Next, add collections to display in the grid.”
- Give the gift-box landing page and Mystery Jar product distinct search intent.
- Link every tier directly to the appropriate product/variant.
- Add a visible comparison table, contents-vary disclosure, delivery summary, and policy summary.

### 5. Correct product data

Audit all products against the physical item and supplier record. Known examples found in the audit include inaccurate or duplicated descriptions, missing metadata, inconsistent brands, and trademark-sensitive names.

For every variant, complete:

- Unique title and human-written description.
- Product type and Google product category.
- House of Yeashi as brand where accurate.
- SKU and MPN; GTIN only when legitimately assigned.
- Material, colour, gender/age group where applicable.
- Correct price, sale price, stock, shipping weight, and image.
- Claim-safe care instructions.

## Priority 1 — highest SEO impact

### Homepage

Current title/H1 are only “House of Yeashi.” Google recommends descriptive, concise titles and a clear primary heading: https://developers.google.com/search/docs/appearance/title-link

Suggested fields:

- SEO title: `Anti-Tarnish Jewellery Online India | House of Yeashi`
- Meta description: `Shop trend-led anti-tarnish necklaces, earrings, rings, bracelets and personalised Mystery Jars from House of Yeashi. Delivery across India.`
- H1: `Anti-Tarnish Jewellery for Everyday Wear`
- Hero copy: `Trend-led pieces and personalised Mystery Jars, curated for your style.`
- Primary CTA: `Shop Mystery Jars`
- Secondary CTA: `Shop Bestsellers`

Add visible brand copy explaining what “Yeashi” means, who curates the products, the business location, and why customers should trust the materials/claims.

### Collections

All audited collection templates use a paragraph-like title rather than an H1; key collections also lack meta descriptions and category copy.

For every collection, add:

1. One visible `<h1>`.
2. 100–250 words of genuinely useful category copy.
3. Unique SEO title and meta description.
4. Links to related collections and relevant guides.
5. A unique social image.

Starter mapping:

| Collection | SEO title | H1 |
|---|---|---|
| Earrings | Anti-Tarnish Earrings for Women | Anti-Tarnish Earrings for Everyday Style |
| Necklace | Dainty Anti-Tarnish Necklaces Online | Everyday Necklaces for Women |
| Rings | Trendy Adjustable Rings for Women | Rings for Everyday Stacking |
| Bracelets | Anti-Tarnish Bracelets for Women | Everyday Bracelets and Bangles |
| Under ₹500 | Jewellery Under ₹500 | Trendy Jewellery Under ₹500 |
| Gift/Jars | Personalised Jewellery Mystery Jars | Jewellery Mystery Jars Curated for Your Vibe |

Do not repeat the same paragraph across collections. Include only accurate material, price, shipping, and care statements.

### Product metadata

The theme often uses the beginning of the full product body as the meta description, creating long, poorly spaced snippets.

Use this pattern:

- SEO title: `<Product name> – <primary benefit/material> | House of Yeashi`
- Meta description: `Shop <product name> by House of Yeashi. <accurate material/benefit>. ₹<price>. See delivery, care and product details.`

For Mystery Jars:

- SEO title: `Jewellery Mystery Jars From ₹599 | House of Yeashi`
- Meta description: `Choose a Mystery Jar from ₹599 and share your preferred colours or vibe. See each tier, included piece count, delivery and policy details.`

### Brand-entity reinforcement

Google initially suggests the spelling “House of Yashi.” Strengthen the exact brand entity:

- Use “House of Yeashi” identically in title, H1, logo alt, footer, About, Organization schema, social bios, Merchant Center, and directory profiles.
- Add an About page explaining the name and founders.
- Add `sameAs` links for Instagram, Facebook, and Threads in Organization schema.
- Request a consistent Facebook vanity URL.
- Use Google Business Profile only if the business makes in-person customer contact; online-only businesses are ineligible: https://support.google.com/business/answer/7039811

## Priority 2 — technical and rich results

### Hero and image weight

The homepage uses three large PNG hero assets of about 2.14–2.32 MB each, repeated across carousel elements, without effective responsive source handling. This is a likely mobile LCP problem.

Actions:

- Export responsive AVIF/WebP variants.
- Load only the first hero slide eagerly.
- Lazy-load later slides.
- Add `srcset` and `sizes`.
- Put headline and CTA in HTML rather than baking important text into images.
- Add meaningful alt text to content images; keep decorative images empty-alt.
- Replace very large source uploads when a smaller original is sufficient.

### Structured data

Product pages already emit ProductGroup/Product/Offer data with INR price and availability. Improve it rather than duplicating conflicting apps.

- Organization: add logo, `sameAs`, contact point, and verified business details.
- Product: consistent brand, SKU/MPN/GTIN, image, availability, and variant mapping.
- Add `BreadcrumbList`.
- Add shipping and return-policy properties only when they exactly match the visible policy and Merchant Center.
- Add review/rating markup only for genuine reviews visibly displayed on the page.

Google recommends combining product structured data with Merchant Center data for maximum eligibility: https://developers.google.com/search/docs/appearance/structured-data/product

Example Organization shape—replace placeholders with verified values:

```json
{
  "@context": "https://schema.org",
  "@type": "Organization",
  "name": "House of Yeashi",
  "url": "https://www.houseofyeashi.com/",
  "logo": "https://www.houseofyeashi.com/path-to-square-logo.png",
  "email": "hoy@houseofyeashi.com",
  "telephone": "+91-9310232278",
  "sameAs": [
    "https://www.instagram.com/houseofyeashi/",
    "https://www.facebook.com/profile.php?id=61583087292419",
    "https://www.threads.com/@houseofyeashi"
  ]
}
```

### Social metadata

The homepage requests a large social card but lacks a usable `og:image`/`twitter:image` in the audit.

- Add a 1200×630 homepage share image.
- Add page-specific social images for Mystery Jars, gifts, and top collections.
- Ensure every `og:image` uses HTTPS.
- Use concise OG titles/descriptions that match visible page claims.

### App/script audit

Key templates expose many script resources. After measurement:

- Remove unused Shopify apps and app embeds.
- Disable unused theme modules.
- Avoid loading carousel/product logic on templates that do not use it.
- Re-test mobile performance and conversion after every removal.

## Priority 3 — content that compounds reach

The blog sitemap contains no articles. Start with high-intent, product-linked guides:

1. `Best Anti-Tarnish Jewellery Under ₹500 in India`
2. `Mystery Jewellery Jar: What You Get at Every Price`
3. `Anti-Tarnish vs Water-Resistant vs Waterproof Jewellery`
4. `How to Choose a Necklace for Every Neckline`
5. `Jewellery Gifts for Sisters Under ₹1,500`
6. `How to Care for Gold-Plated Everyday Jewellery`
7. `Three Jewellery Stacks Under ₹999`
8. `How House of Yeashi Curates a Mystery Jar From Your Vibe`

Each article should use real photos, link to relevant products/collections, answer the query early, and avoid unsupported claims.

## Search Console and Merchant Center

- Verify the `https://www.houseofyeashi.com/` property.
- Submit `https://www.houseofyeashi.com/sitemap.xml` and monitor errors. Google notes that submitting it enables crawl/error monitoring even if it was already discovered: https://support.google.com/webmasters/answer/7451001
- Inspect and request indexing for the homepage, Mystery Jar, best collections, and new articles after substantive updates.
- Connect Shopify Google & YouTube to Merchant Center, GA4, and Google Ads. Shopify documents that the channel syncs store/product information: https://help.shopify.com/en/manual/online-sales-channels/marketplaces/google/getting-setup/connect
- Maintain at least 95% approved intended products, with zero price, stock, shipping, or landing-page mismatches before scaling ads.

## Recommended implementation order

1. Phone, Terms, shipping, claims, gift placeholder.
2. Product copy and feed fields.
3. Hero performance.
4. Collection H1/copy and homepage positioning.
5. Social images and schema.
6. About, FAQ, care, and content program.
