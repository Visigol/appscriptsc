# Endpoints and API Calls

This document lists all the external API endpoints and calls used by the scraper tool, separated by website domain.

## Wolt

*   **Consumer Assortment API:** `https://consumer-api.wolt.com/consumer-api/consumer-assortment/v1/venues/slug/{venue_slug}/assortment`

## Uber Eats

*   The scraper fetches the main page HTML and parses an embedded JSON data block (`__PRELOADED_STATE__`) to extract the menu data. No direct API calls are made.

## Glovo

*   The scraper first fetches the main page HTML to extract the `storeId` and `addressId` from an embedded JSON data block.
*   **Content API:** `https://api.glovoapp.com/v3/stores/{store_id}/addresses/{address_id}/content`

## Tazz

*   No direct API calls are made. The website is scraped by fetching the HTML and parsing it with regular expressions.

## Foody

*   **Catalog API:** `https://apinew.foody.com.cy/v3/shops/catalog?shop_id={shop_id}`

## Foodora

*   **Vendor API:** `https://<country_code>.fd-api.com/api/v5/vendors/{vendor_id}`
    *   The `<country_code>` is determined from the URL (e.g., `se` for Sweden).

## Pyszne.pl

*   **Items API:** `https://globalmenucdn.eu-central-1.production.jet-external.com/{restaurant_slug}_{country_code}_items.json`
*   **Menu API:** `https://www.pyszne.{country_code}/api/restaurants/{restaurant_slug}/menu`

## Google Cloud Translate

*   **Translate API:** `https://translation.googleapis.com/language/translate/v2`

## Airtable

*   **Airtable API:** `https://api.airtable.com/v0/{baseId}/{tableName}`
