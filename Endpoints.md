# Endpoints and API Calls

This document lists all the external API endpoints and calls used by the scraper tool, separated by website domain.

## Wolt

*   **Venue Content API:** `https://consumer-api.wolt.com/consumer-api/v3/web/venue-content/slug/{venue_slug}`
*   **Consumer Assortment API:** `https://consumer-api.wolt.com/consumer-api/consumer-assortment/v1/venues/slug/{venue_slug}/assortment`
*   **Category API:** `https://consumer-api.wolt.com/consumer-api/consumer-assortment/v1/venues/slug/{venue_slug}/assortment/categories/slug/{category_slug}`

## Uber Eats

*   **Get Store API:** `https://www.ubereats.com/_p/api/getStoreV1`
*   **Get Catalog Presentation API:** `https://www.ubereats.com/_p/api/getCatalogPresentationV2`

## Glovo

*   **Store Menu API:** `https://api.glovoapp.com/v3/stores/{store_id}/addresses/{address_id}/node/store_menu`
*   **Partial Content API:** `https://api.glovoapp.com/v4/stores/{store_id}/addresses/{address_id}/content/partial`

## Tazz

*   No direct API calls are made. The website is scraped using Puppeteer to extract the menu data from the HTML.

## Foody

*   **Catalog API:** `https://apinew.foody.com.cy/v3/shops/catalog?shop_id={shop_id}`

## Foodora

*   **Vendor API:** `https://<country_code>.fd-api.com/api/v5/vendors/{vendor_id}`
    *   The `<country_code>` is determined from the URL (e.g., `se` for Sweden).

## Pyszne.pl

*   **Items API:** `https://globalmenucdn.eu-central-1.production.jet-external.com/{restaurant_slug}_{country_code}_items.json`
*   **Item Details API:** `https://globalmenucdn.eu-central-1.production.jet-external.com/{restaurant_slug}_{country_code}_itemDetails.json`
*   **Menu API:** `https://www.pyszne.{country_code}/api/restaurants/{restaurant_slug}/menu`

## Google Cloud Translate

*   **Translate API:** `https://translation.googleapis.com/language/translate/v2`

## Airtable

*   **Airtable API:** `https://api.airtable.com/v0/{baseId}/{tableName}`
