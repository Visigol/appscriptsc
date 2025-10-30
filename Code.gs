/**
 * Main backend logic for the Google Apps Script scraper.
 * Handles web requests, manages job status in a Google Sheet,
 * and orchestrates the scraping process asynchronously.
 */

// The name of the Google Sheet used for job tracking.
const STATUS_SHEET_NAME = 'ScrapeStatus';

/**
 * Handles HTTP GET requests.
 * - If a 'jobId' is provided, it returns the status of that job.
 * - Otherwise, it serves the main HTML frontend.
 * Note: When called from google.script.run, this will return the HTML content as a string.
 * When called as a web app, it will serve the HTML page.
 */
function doGet(e) {
  if (e.parameter.jobId) {
    // When accessed as a web app URL with a jobId, return JSON
    return createJsonResponse(getJobStatus(e.parameter.jobId));
  }
  // Serve the frontend HTML
  return HtmlService.createHtmlOutputFromFile('index').setXFrameOptionsMode(HtmlService.XFrameOptionsMode.ALLOWALL);
}

/**
 * Handles HTTP POST requests to initiate a new scrape job.
 * This function is called from the frontend via `google.script.run`.
 * It returns a plain JavaScript object for the success handler.
 */
function doPost(e) {
  try {
    // When called via google.script.run, the argument is the payload itself, not the full request object.
    const postData = typeof e.postData === 'string' ? JSON.parse(e.postData.contents) : e;
    const url = postData.url;

    if (!url) {
      // For google.script.run, we throw an error for the onFailure handler
      throw new Error('URL is required');
    }

    const jobId = Utilities.getUuid();
    const sheet = getOrCreateStatusSheet();
    sheet.appendRow([jobId, 'queued', url, new Date().toISOString(), '']);

    // Create a one-time trigger to run the scraper function asynchronously.
    ScriptApp.newTrigger('runQueuedScrape')
      .timeBased()
      .after(1000) // 1 second delay
      .create();

    // Return a plain JS object for the withSuccessHandler callback
    return { message: 'Scrape queued', jobId: jobId };
  } catch (error) {
     // Log the error for debugging and re-throw it for the onFailure handler.
    console.error('doPost Error: ' + error.stack);
    throw new Error('Failed to process request: ' + error.message);
  }
}

/**
 * Retrieves the status and result of a specific job.
 * Called from the frontend via `google.script.run`.
 * Returns a plain JavaScript object for the success handler.
 */
function getJobStatus(jobId) {
  if (!jobId) {
     throw new Error('No jobId provided');
  }
  try {
    const sheet = getOrCreateStatusSheet();
    const data = sheet.getDataRange().getValues();
    const headers = data[0];
    const rowIndex = data.findIndex(row => row[0] === jobId);

    if (rowIndex === -1) {
       return { status: 'error', error: 'Job not found' }; // Return object, not an error, as the poll expects a status
    }

    const jobData = {};
    headers.forEach((key, index) => {
      jobData[key] = data[rowIndex][index];
    });

    // If there's a result, try to parse it.
    if (jobData.result) {
        try {
            jobData.result = JSON.parse(jobData.result);
        } catch (e) {
            // Ignore if parsing fails, leave it as a string.
        }
    }

    return jobData;
  } catch (error) {
    console.error('getJobStatus Error: ' + error.stack);
    // Let the frontend know the poll failed.
    throw new Error('Failed to get job status: ' + error.message);
  }
}


/**
 * A trigger-invoked function that finds and runs the oldest 'queued' job.
 * It deletes its own trigger after execution to ensure it only runs once.
 */
function runQueuedScrape(e) {
  // Delete the trigger that invoked this function.
  if (e && e.triggerUid) {
    const allTriggers = ScriptApp.getProjectTriggers();
    for (const trigger of allTriggers) {
      if (trigger.getUniqueId() === e.triggerUid) {
        ScriptApp.deleteTrigger(trigger);
        break;
      }
    }
  }

  // Find the first job with 'queued' status.
  const sheet = getOrCreateStatusSheet();
  const data = sheet.getDataRange().getValues();
  const headers = data[0];
  const statusColIndex = headers.indexOf('status');
  const jobRowIndex = data.findIndex(row => row[statusColIndex] === 'queued');

  if (jobRowIndex !== -1) {
    const jobIdColIndex = headers.indexOf('jobId');
    const urlColIndex = headers.indexOf('url');
    const jobId = data[jobRowIndex][jobIdColIndex];
    const url = data[jobRowIndex][urlColIndex];

    // Run the main scraping logic for the found job.
    main(jobId, url);
  }
}


/**
 * The main scraping orchestrator.
 * It updates the job status and calls the appropriate scraper function based on the URL.
 */
function main(jobId, url) {
  try {
    updateJobStatus(jobId, 'scraping');
    let result;

    // --- Scraper Routing ---
    if (url.includes('wolt.com')) {
      result = scrapeWolt(url);
    } else if (url.includes('ubereats.com')) {
      result = scrapeUberEats(url);
    } else if (url.includes('glovoapp.com')) {
      result = scrapeGlovo(url);
    } else if (url.includes('foodora.')) {
      result = scrapeFoodora(url);
    } else if (url.includes('pyszne.pl')) {
      result = scrapePyszne(url);
    } else if (url.includes('foody.com')) {
      result = scrapeFoody(url);
    } else if (url.includes('tazz.ro')) {
      result = scrapeTazz(url);
    } else {
      throw new Error('This website is not supported yet.');
    }

    updateJobStatus(jobId, 'completed', result);
  } catch (error) {
    // Ensure the error object is serializable.
    const errorMessage = error.message || 'An unknown error occurred.';
    updateJobStatus(jobId, 'error', { error: errorMessage });
  }
}

/**
 * Updates the status and result of a job in the Google Sheet.
 */
function updateJobStatus(jobId, status, resultData) {
  try {
    const sheet = getOrCreateStatusSheet();
    const data = sheet.getDataRange().getValues();
    const headers = data[0];
    const rowIndex = data.findIndex(row => row[0] === jobId);

    if (rowIndex > -1) {
      const statusColIndex = headers.indexOf('status');
      if (statusColIndex !== -1) {
        sheet.getRange(rowIndex + 1, statusColIndex + 1).setValue(status);
      }

      if (resultData) {
        const resultColIndex = headers.indexOf('result');
        if (resultColIndex !== -1) {
          const dataString = JSON.stringify(resultData);
          sheet.getRange(rowIndex + 1, resultColIndex + 1).setValue(dataString);
        }
      }
    }
  } catch (error) {
    // Log error to the console for debugging.
    console.error(`Failed to update job status for ${jobId}: ${error.message}`);
  }
}

/**
 * Scrapes menu data from a Wolt restaurant page.
 */
function scrapeWolt(url) {
  const venueSlugMatch = url.match(/\/(?:restaurant|venue)\/([^/]+)/);
  if (!venueSlugMatch) {
    throw new Error('Invalid Wolt URL: Could not extract venue slug.');
  }
  const venue = venueSlugMatch[1];

  const assortmentUrl = `https://consumer-api.wolt.com/consumer-api/consumer-assortment/v1/venues/slug/${venue}/assortment`;
  const response = UrlFetchApp.fetch(assortmentUrl, {
    headers: {
      'accept': 'application/json, text/plain, */*',
      'accept-language': 'en-US,en;q=0.9',
      'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36'
    },
    muteHttpExceptions: false
  });

  const data = JSON.parse(response.getContentText());
  const dishes = [];
  const categories = data.categories || [];
  const items = data.items || [];
  const categoryMap = new Map();

  for (const category of categories) {
    for (const itemId of (category.item_ids || [])) {
      categoryMap.set(itemId, category.name);
    }
  }

  for (const item of items) {
    dishes.push({
      category: categoryMap.get(item.id) || 'Unknown Category',
      dishName: item.name,
      price: (item.price / 100).toFixed(2),
      description: item.description,
      image: item.images && item.images.length > 0 ? item.images[0].url : ''
    });
  }

  // Store results in a new sheet named after the restaurant.
  const spreadsheet = SpreadsheetApp.getActiveSpreadsheet();
  let resultSheet = spreadsheet.getSheetByName(venue);
  if (resultSheet) {
    resultSheet.clear(); // Clear old data
  } else {
    resultSheet = spreadsheet.insertSheet(venue);
  }

  resultSheet.appendRow(['Category', 'Dish Name', 'Price', 'Description', 'Image']);
  dishes.forEach(dish => {
    resultSheet.appendRow([dish.category, dish.dishName, dish.price, dish.description, dish.image]);
  });

  return {
    message: `Scraping complete! Results are in the '${venue}' sheet.`,
    counters: {
      dishes: dishes.length,
      categories: categories.length
    }
  };
}

function scrapeTazz(url) {
  const restaurantName = url.split('/').pop() || 'Tazz_Restaurant';
  const response = UrlFetchApp.fetch(url, { muteHttpExceptions: false });
  const htmlContent = response.getContentText();

  // This is a simplified parser. A more robust solution would use a proper HTML parsing library.
  // Apps Script does not have a built-in library for this, so we are using regex.
  const dishes = [];
  const categoryRegex = /<h2[^>]*class="widget-title"[^>]*>([^<]+)<\/h2>/g;
  const productRegex = /<div[^>]*class="restaurant-product-card"[^>]*>([\s\S]*?)<\/div>/g;
  const titleRegex = /<div[^>]*class="title-container"[^>]*>([^<]+)<\/div>/;
  const priceRegex = /<div[^>]*class="price-container[^"]*"[^>]*>([^<]+)<\/div>/;
  const descriptionRegex = /<div[^>]*class="description-container"[^>]*>([^<]+)<\/div>/;
  const imageRegex = /<img[^>]*src="([^"]+)"/;

  let match;
  let lastCategory = 'Unknown Category';
  while ((match = productRegex.exec(htmlContent)) !== null) {
    const productHtml = match[1];
    const categoryMatch = categoryRegex.exec(htmlContent); // This might not be accurate
    if(categoryMatch) lastCategory = categoryMatch[1].trim();

    const titleMatch = productHtml.match(titleRegex);
    const priceMatch = productHtml.match(priceRegex);
    const descriptionMatch = productHtml.match(descriptionRegex);
    const imageMatch = productHtml.match(imageRegex);

    if (titleMatch) {
      dishes.push({
        category: lastCategory,
        dishName: titleMatch[1].trim(),
        price: priceMatch ? priceMatch[1].trim().replace(/[^0-9.,]/g, '') : 'N/A',
        description: descriptionMatch ? descriptionMatch[1].trim() : '',
        image: imageMatch ? imageMatch[1] : ''
      });
    }
  }

  const spreadsheet = SpreadsheetApp.getActiveSpreadsheet();
  let resultSheet = spreadsheet.getSheetByName(restaurantName);
  if (resultSheet) {
    resultSheet.clear();
  } else {
    resultSheet = spreadsheet.insertSheet(restaurantName);
  }

  resultSheet.appendRow(['Category', 'Dish Name', 'Price', 'Description', 'Image']);
  dishes.forEach(dish => {
    resultSheet.appendRow([dish.category, dish.dishName, dish.price, dish.description, dish.image]);
  });

  return {
    message: `Scraping complete! Results are in the '${restaurantName}' sheet.`,
    counters: {
      dishes: dishes.length,
      categories: [...new Set(dishes.map(d => d.category))].length
    }
  };
}

function scrapeFoody(url) {
  let shopId;
  const shopIdMatch = url.match(/shop_id=(\d+)/);
  if (!shopIdMatch || !shopIdMatch[1]) {
    // Fallback for URLs that have the shop ID in the path
    const pathMatch = url.match(/\/restaurant\/.*\/(\d+)/);
    if (!pathMatch || !pathMatch[1]) {
      throw new Error('Invalid Foody URL: Could not extract shop_id.');
    }
    shopId = pathMatch[1];
  } else {
    shopId = shopIdMatch[1];
  }
  const restaurantName = `Foody_${shopId}`;

  const apiUrl = `https://apinew.foody.com.cy/v3/shops/catalog?shop_id=${shopId}`;
  const options = {
    headers: {
      'accept': 'application/json'
    },
    muteHttpExceptions: false
  };

  const response = UrlFetchApp.fetch(apiUrl, options);
  const data = JSON.parse(response.getContentText());

  const dishes = [];
  const sections = data.data?.menu?.categories || [];
  let categoryCount = sections.length;

  for (const section of sections) {
    const categoryName = section.name || 'Unknown Category';
    const items = section.items || [];
    for (const item of items) {
      dishes.push({
        category: categoryName,
        dishName: item.name,
        price: item.price ? (item.price).toFixed(2) : 'N/A',
        description: item.description || '',
        image: item.images?.menu || ''
      });
    }
  }

  const spreadsheet = SpreadsheetApp.getActiveSpreadsheet();
  let resultSheet = spreadsheet.getSheetByName(restaurantName);
  if (resultSheet) {
    resultSheet.clear();
  } else {
    resultSheet = spreadsheet.insertSheet(restaurantName);
  }

  resultSheet.appendRow(['Category', 'Dish Name', 'Price', 'Description', 'Image']);
  dishes.forEach(dish => {
    resultSheet.appendRow([dish.category, dish.dishName, dish.price, dish.description, dish.image]);
  });

  return {
    message: `Scraping complete! Results are in the '${restaurantName}' sheet.`,
    counters: {
      dishes: dishes.length,
      categories: categoryCount
    }
  };
}

function scrapePyszne(url) {
  const restaurantSlugMatch = url.match(/\/menu\/([^/]+)/);
  if (!restaurantSlugMatch || !restaurantSlugMatch[1]) {
    throw new Error('Invalid Pyszne.pl URL: Could not extract restaurant slug.');
  }
  const restaurantSlug = restaurantSlugMatch[1];
  const restaurantName = restaurantSlug;

  let countryCode = 'pl';
  if (url.includes('pyszne.pt')) {
    countryCode = 'pt';
  }

  const itemsApiUrl = `https://globalmenucdn.eu-central-1.production.jet-external.com/${restaurantSlug}_${countryCode}_items.json`;
  const menuApiUrl = `https://www.pyszne.${countryCode}/api/restaurants/${restaurantSlug}/menu`;

  const options = {
    headers: {
      'Accept': 'application/json, text/plain, */*',
      'X-Jet-Application': 'OneWeb'
    },
    muteHttpExceptions: false
  };

  const itemsResponse = UrlFetchApp.fetch(itemsApiUrl, options);
  const itemsData = JSON.parse(itemsResponse.getContentText());
  const menuResponse = UrlFetchApp.fetch(menuApiUrl, options);
  const menuData = JSON.parse(menuResponse.getContentText());

  const dishes = [];
  const items = itemsData.Items || [];
  const menuGroups = menuData.categories || [];
  const categoryMap = new Map();
  let categoryCount = 0;

  for (const menuGroup of menuGroups) {
    categoryMap.set(menuGroup.id, menuGroup.name);
    categoryCount++;
  }

  for (const item of items) {
    const variation = item.Variations && item.Variations.length > 0 ? item.Variations[0] : {};
    const menuGroupId = variation.MenuGroupIds && variation.MenuGroupIds.length > 0 ? variation.MenuGroupIds[0] : null;
    dishes.push({
      category: menuGroupId ? categoryMap.get(menuGroupId) : 'Unknown Category',
      dishName: item.Name,
      price: variation.BasePrice ? variation.BasePrice.toFixed(2) : 'N/A',
      description: item.Description || '',
      image: item.ImageSources && item.ImageSources.length > 0 ? item.ImageSources[0].Path.replace('{transformations}', 'f_auto,w_512') : ''
    });
  }

  const spreadsheet = SpreadsheetApp.getActiveSpreadsheet();
  let resultSheet = spreadsheet.getSheetByName(restaurantName);
  if (resultSheet) {
    resultSheet.clear();
  } else {
    resultSheet = spreadsheet.insertSheet(restaurantName);
  }

  resultSheet.appendRow(['Category', 'Dish Name', 'Price', 'Description', 'Image']);
  dishes.forEach(dish => {
    resultSheet.appendRow([dish.category, dish.dishName, dish.price, dish.description, dish.image]);
  });

  return {
    message: `Scraping complete! Results are in the '${restaurantName}' sheet.`,
    counters: {
      dishes: dishes.length,
      categories: categoryCount
    }
  };
}

function scrapeFoodora(url) {
  const vendorIdMatch = url.match(/\/restaurant\/([^/]+)/);
  if (!vendorIdMatch || !vendorIdMatch[1]) {
    throw new Error('Invalid Foodora URL: Could not extract vendor ID.');
  }
  const vendorId = vendorIdMatch[1];
  const restaurantName = vendorId;

  let countryCode = 'se'; // Default to Sweden
  const urlMatch = url.match(/foodora\.([a-z]{2})/i);
  if (urlMatch && urlMatch[1]) {
    countryCode = urlMatch[1].toLowerCase();
  }

  const apiBaseMap = {
    'se': 'se.fd-api.com',
    'no': 'no.fd-api.com',
    'hu': 'hu.fd-api.com',
    'at': 'at.fd-api.com',
    'cz': 'cz.fd-api.com',
  };
  const apiBase = apiBaseMap[countryCode] || 'op.fd-api.com';

  const apiUrl = `https://${apiBase}/api/v5/vendors/${vendorId}?include=menus`;
  const options = {
    headers: {
      'accept': 'application/json, text/plain, */*',
      'x-fp-api-key': 'volo'
    },
    muteHttpExceptions: false
  };

  const response = UrlFetchApp.fetch(apiUrl, options);
  const data = JSON.parse(response.getContentText());

  const dishes = [];
  const sections = data.data?.menus?.[0]?.menu_categories || [];
  let categoryCount = sections.length;

  for (const section of sections) {
    const categoryName = section.name || 'Unknown Category';
    const items = section.products || [];
    for (const item of items) {
      dishes.push({
        category: categoryName,
        dishName: item.name,
        price: item.product_variations?.[0]?.price ? (item.product_variations[0].price / 100).toFixed(2) : 'N/A',
        description: item.description || '',
        image: item.file_path || ''
      });
    }
  }

  const spreadsheet = SpreadsheetApp.getActiveSpreadsheet();
  let resultSheet = spreadsheet.getSheetByName(restaurantName);
  if (resultSheet) {
    resultSheet.clear();
  } else {
    resultSheet = spreadsheet.insertSheet(restaurantName);
  }

  resultSheet.appendRow(['Category', 'Dish Name', 'Price', 'Description', 'Image']);
  dishes.forEach(dish => {
    resultSheet.appendRow([dish.category, dish.dishName, dish.price, dish.description, dish.image]);
  });

  return {
    message: `Scraping complete! Results are in the '${restaurantName}' sheet.`,
    counters: {
      dishes: dishes.length,
      categories: categoryCount
    }
  };
}


function scrapeUberEats(url) {
  const restaurantName = (url.split('/store/')[1] || 'UberEats_Restaurant').split('/')[0];

  // Fetch the page HTML to find the embedded JSON data
  const pageResponse = UrlFetchApp.fetch(url, { muteHttpExceptions: true });
  const pageContent = pageResponse.getContentText();

  // Look for the __PRELOADED_STATE__ JSON blob.
  const jsonMatch = pageContent.match(/<script>window.__PRELOADED_STATE__ = ([\s\S]*?);<\/script>/);
  if (!jsonMatch || !jsonMatch[1]) {
    throw new Error('Invalid Uber Eats page: Could not find __PRELOADED_STATE__ JSON.');
  }

  const pageData = JSON.parse(jsonMatch[1]);

  // The exact path to the menu data can change, so this might need updating in the future.
  const stores = pageData?.stores || {};
  let catalogSectionsMap;

  // Find the first store object that has a catalogSectionsMap
  for (const key in stores) {
    if (stores[key]?.catalogSectionsMap) {
      catalogSectionsMap = stores[key].catalogSectionsMap;
      break;
    }
  }

  if (!catalogSectionsMap) {
     throw new Error('Could not find catalogSectionsMap in the page data.');
  }

  const dishes = [];
  let categoryCount = 0;

  for (const sectionId in catalogSectionsMap) {
    const sectionArray = catalogSectionsMap[sectionId] || [];
    categoryCount++;
    for (const section of sectionArray) {
      const std = section?.payload?.standardItemsPayload;
      if (!std) continue;
      const categoryName = std.title ? std.title.text : 'Unknown Category';
      const catalogItems = std.catalogItems || [];
      for (const item of catalogItems) {
        dishes.push({
          category: categoryName,
          dishName: item.title,
          price: item.priceTagline ? item.priceTagline.text : 'N/A',
          description: item.itemDescription || '',
          image: item.imageUrl || ''
        });
      }
    }
  }

  const spreadsheet = SpreadsheetApp.getActiveSpreadsheet();
  let resultSheet = spreadsheet.getSheetByName(restaurantName);
  if (resultSheet) {
    resultSheet.clear();
  } else {
    resultSheet = spreadsheet.insertSheet(restaurantName);
  }

  resultSheet.appendRow(['Category', 'Dish Name', 'Price', 'Description', 'Image']);
  dishes.forEach(dish => {
    resultSheet.appendRow([dish.category, dish.dishName, dish.price, dish.description, dish.image]);
  });

  return {
    message: `Scraping complete! Results are in the '${restaurantName}' sheet.`,
    counters: {
      dishes: dishes.length,
      categories: categoryCount
    }
  };
}

function scrapeGlovo(url) {
  const restaurantName = (url.split('/').filter(Boolean).pop() || 'Glovo_Restaurant');

  // First, fetch the page HTML to find the storeId and addressId
  const pageResponse = UrlFetchApp.fetch(url, { muteHttpExceptions: true });
  const pageContent = pageResponse.getContentText();

  // Look for a JSON blob containing page metadata. This is more reliable than loose regex.
  const jsonMatch = pageContent.match(/<script type="application\/json" data-compid="page-meta-data">([\s\S]*?)<\/script>/);
  if (!jsonMatch || !jsonMatch[1]) {
    throw new Error('Invalid Glovo page: Could not find page metadata JSON.');
  }

  const pageData = JSON.parse(jsonMatch[1]);
  const storeId = pageData?.seo?.analytics?.storeId;
  const addressId = pageData?.address?.id;

  if (!storeId || !addressId) {
     throw new Error('Invalid Glovo URL: Could not extract store or address ID from page data.');
  }

  const apiUrl = `https://api.glovoapp.com/v3/stores/${storeId}/addresses/${addressId}/content`;

  const options = {
    headers: {
      'accept': 'application/json',
      'glovo-api-version': '14',
      'glovo-app-platform': 'web',
      'glovo-app-type': 'customer',
      'glovo-language-code': 'en' // Defaulting to English
    },
    muteHttpExceptions: false
  };

  const response = UrlFetchApp.fetch(apiUrl, options);
  const data = JSON.parse(response.getContentText());

  const dishes = [];
  const body = data?.data?.body || [];
  let categoryCount = 0;

  for (const section of body) {
    if (section?.type !== 'LIST') continue;
    categoryCount++;
    const categoryName = section?.data?.title || 'Unknown Category';

    for (const el of (section?.data?.elements || [])) {
      if (el?.type !== 'PRODUCT_ROW') continue;
      const p = el.data || {};
      dishes.push({
        category: categoryName,
        dishName: p.name,
        price: p.priceInfo ? (p.priceInfo.amount / 100).toFixed(2) : 'N/A',
        description: p.description || '',
        image: p.imageUrl || ''
      });
    }
  }

  const spreadsheet = SpreadsheetApp.getActiveSpreadsheet();
  let resultSheet = spreadsheet.getSheetByName(restaurantName);
  if (resultSheet) {
    resultSheet.clear();
  } else {
    resultSheet = spreadsheet.insertSheet(restaurantName);
  }

  resultSheet.appendRow(['Category', 'Dish Name', 'Price', 'Description', 'Image']);
  dishes.forEach(dish => {
    resultSheet.appendRow([dish.category, dish.dishName, dish.price, dish.description, dish.image]);
  });

  return {
    message: `Scraping complete! Results are in the '${restaurantName}' sheet.`,
    counters: {
      dishes: dishes.length,
      categories: categoryCount
    }
  };
}


// --- UTILITY FUNCTIONS ---

/**
 * Gets the status sheet, creating it with headers if it doesn't exist.
 */
function getOrCreateStatusSheet() {
  const spreadsheet = SpreadsheetApp.getActiveSpreadsheet();
  let sheet = spreadsheet.getSheetByName(STATUS_SHEET_NAME);
  if (!sheet) {
    sheet = spreadsheet.insertSheet(STATUS_SHEET_NAME);
    // Define the headers, including the 'result' column.
    const headers = ['jobId', 'status', 'url', 'createdAt', 'result'];
    sheet.getRange(1, 1, 1, headers.length).setValues([headers]);
  }
  return sheet;
}

/**
 * Creates a JSON ContentService output for serving web app responses.
 */
function createJsonResponse(obj) {
  return ContentService.createTextOutput(JSON.stringify(obj))
    .setMimeType(ContentService.MimeType.JSON);
}