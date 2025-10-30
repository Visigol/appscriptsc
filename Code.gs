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
 */
function doGet(e) {
  if (e.parameter.jobId) {
    return getJobStatus(e.parameter.jobId);
  }
  return HtmlService.createHtmlOutputFromFile('index').setXFrameOptionsMode(HtmlService.XFrameOptionsMode.ALLOWALL);
}

/**
 * Handles HTTP POST requests to initiate a new scrape job.
 * It creates a job entry, stores it in the status sheet, and
 * creates a trigger to run the scrape asynchronously.
 */
function doPost(e) {
  try {
    const postData = JSON.parse(e.postData.contents);
    const url = postData.url;
    if (!url) {
      return createJsonResponse({ error: 'URL is required' });
    }

    const jobId = Utilities.getUuid();
    const sheet = getOrCreateStatusSheet();
    // Append a new row for the job with an empty result field.
    sheet.appendRow([jobId, 'queued', url, new Date().toISOString(), '']);

    // Create a one-time trigger to run the scraper function asynchronously.
    ScriptApp.newTrigger('runQueuedScrape')
      .timeBased()
      .after(1000) // 1 second delay
      .create();

    return createJsonResponse({ message: 'Scrape queued', jobId: jobId });
  } catch (error) {
    return createJsonResponse({ error: 'Failed to process request: ' + error.message });
  }
}

/**
 * Retrieves the status and result of a specific job from the Google Sheet.
 */
function getJobStatus(jobId) {
  if (!jobId) {
    return createJsonResponse({ status: 'error', error: 'No jobId provided' });
  }
  try {
    const sheet = getOrCreateStatusSheet();
    const data = sheet.getDataRange().getValues();
    const headers = data[0];
    const rowIndex = data.findIndex(row => row[0] === jobId);

    if (rowIndex === -1) {
      return createJsonResponse({ status: 'error', error: 'Job not found' });
    }

    const jobData = {};
    headers.forEach((key, index) => {
      jobData[key] = data[rowIndex][index];
    });

    return createJsonResponse(jobData);
  } catch (error) {
    return createJsonResponse({ status: 'error', error: 'Failed to get job status: ' + error.message });
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
    // This is where you would add logic to call different scraper functions
    // based on the URL domain.
    if (url.includes('wolt.com')) {
      result = scrapeWolt(url);
    }
    // TODO: Add else-if blocks for other scrapers like Uber Eats, Glovo, etc.
    // else if (url.includes('ubereats.com')) {
    //   result = scrapeUberEats(url);
    // }
    else {
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
 * Creates a JSON ContentService output.
 */
function createJsonResponse(obj) {
  return ContentService.createTextOutput(JSON.stringify(obj))
    .setMimeType(ContentService.MimeType.JSON);
}