import requests
import time
import random
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime, timezone
from bs4 import BeautifulSoup
import http.client
import bimatquocgia
import pytz
import json
from concurrent.futures import ThreadPoolExecutor, as_completed

# === CONFIG ===
SHEET_URL = bimatquocgia.SHEET_URL
GOOGLE_SERVICE_ACCOUNT_FILE = bimatquocgia.SERVICE_ACCOUNT
SCRAPINGANT_API_KEY = bimatquocgia.SCRAPINGANT
OXYLABS_ACCOUNT = bimatquocgia.OXYLABS_ACCOUNT
OXYLABS_PASSWORD = bimatquocgia.OXYLABS_PASSWORD
HASDATA_API_KEY = bimatquocgia.HASDATA
WebScrapingDotAi_API_KEY = bimatquocgia.WEBSCRAPINGDOTAI
SCRAPINGROBOT_API_KEY = bimatquocgia.SCRAPINGROBOT
WebScrapingApiDtCm_API_KEY = bimatquocgia.WEBSCRAPINGAPIDOTCOM
TIMEZONE = bimatquocgia.TIMEZONE
AMAZON_ZIP_CODE = bimatquocgia.ZIP_CODE
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36",
]

# === GOOGLE SHEETS SETUP ===
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
creds = Credentials.from_service_account_file(GOOGLE_SERVICE_ACCOUNT_FILE, scopes=SCOPES)
gc = gspread.authorize(creds)

# Open sheet
sh = gc.open_by_url(SHEET_URL)
url_sheet = sh.sheet1        # assumes URLs in Sheet1 col A

# Try to open Logs sheet, create if missing
try:
    log_sheet = sh.worksheet("Logs")
except gspread.exceptions.WorksheetNotFound:
    print("ℹ️ 'Logs' sheet not found, creating it...")
    log_sheet = sh.add_worksheet(title="Logs", rows=1000, cols=3)
    # Add headers
    log_sheet.append_row(["Timestamp", "URL", "Price"], value_input_option="RAW")

# === SCRAPERS ===
alternateAPI = -1
badAPI = [True, True, True, True, True]
errorAPI = [15, 15, 5, 5, 15]

def fatalError(status, no_retry, indexAPI):
    global badAPI
    global errorAPI
    for i in no_retry:
        if status == i:
            print("! Fatal Error for", indexAPI, "with", status)
            badAPI[indexAPI] = False
            errorAPI[indexAPI] = 0
            break

def get_soup(target):
    global alternateAPI
    global badAPI
    global errorAPI
    run = True
    while 1:
        alternateAPI += 1
        if sum(errorAPI) == 0:
            if(badAPI[0]):
                errorAPI[0] = 15
            if(badAPI[1]):
                errorAPI[1] = 15
            if(badAPI[2]):
                errorAPI[2] = 5
            if badAPI[3] == 0:
                errorAPI[3] = 5
            if badAPI[4] == 0:
                errorAPI[4] = 15
        if alternateAPI == 0 and errorAPI[0]:
            return get_soup_scrapingant(target)
        if alternateAPI == 1 and errorAPI[1]:
            return get_soup_scrapingrobot(target)
        if alternateAPI == 2 and errorAPI[2]:
            return get_soup_hasdata(target)
        if alternateAPI == 3 and errorAPI[3]:
            return get_soup_WebScrapingDotAi(target)
        if alternateAPI == 4:
            alternateAPI = -1
            if errorAPI[4]:
                return get_soup_WebScrapingApiDtCm(target)

def get_soup_scrapingant(target):
    # Get html using ScrapingAnt
    conn = http.client.HTTPSConnection("api.scrapingant.com")
    conn.request("GET", "/v2/general?url=" + target + "&x-api-key=" + SCRAPINGANT_API_KEY + "&return_page_source=true")

    res = conn.getresponse()
    data = res.read()
    html = data.decode("utf-8")

    fatalError(res.status, no_retry = [400, 403, 500], indexAPI = 0)

    print("# Scraping Ant:       ", end = '')

    return BeautifulSoup(html, "html.parser") # Translate to soup

def get_soup_hasdata(target):
    conn = http.client.HTTPSConnection("api.hasdata.com")

    payload = "{\"url\":\"" + target + "\",\"proxyType\":\"datacenter\",\"blockResources\":true,\"blockAds\":false,\"blockUrls\":[],\"jsScenario\":[],\"screenshot\":false,\"jsRendering\":false,\"extractEmails\":false,\"includeOnlyTags\":[],\"excludeTags\":[],\"outputFormat\":[\"html\"]}"

    headers = {
        'x-api-key': HASDATA_API_KEY,
        'Content-Type': "application/json"
    }

    conn.request("POST", "/scrape/web", payload, headers)

    res = conn.getresponse()
    data = res.read()
    html = data.decode("utf-8")

    fatalError(res.status, no_retry = [401, 403, 500], indexAPI = 2)
    print("# HasData:            ", end = '')
    return BeautifulSoup(html, "html.parser")

def get_soup_WebScrapingApiDtCm(target):
    conn = http.client.HTTPSConnection("api.webscrapingapi.com")
    conn.request("GET", "/v2?api_key=pItvmglV1oldQDWM38h9AfIYSoAEy0qf&timeout=20000&url=" + target)

    res = conn.getresponse()
    data = res.read()
    html = data.decode("utf-8")

    fatalError(res.status, no_retry = [401, 403, 500, 503], indexAPI = 4)
    print("# WebScrapingApiDtCm: ", end = '')
    return BeautifulSoup(html, "html.parser")

def get_soup_WebScrapingDotAi(target):
    params = {
        "api_key": WebScrapingDotAi_API_KEY,
        "url": target,
        "timeout": "20000",
        "js": "false",
    }
    response = requests.get('https://api.webscraping.ai/html', params=params)
    html = response.text

    fatalError(response.status_code, no_retry = [400, 402, 403], indexAPI = 3)
    print("# WebScraping Dot Ai: ", end = '')
    return BeautifulSoup(html, "html.parser")

def get_soup_scrapingrobot(target):
    url = "https://api.scrapingrobot.com/?token=" + SCRAPINGROBOT_API_KEY + "&url=" + target
    response = requests.get(url)
    data = response.json()
    print("# Scraping Robot:     ", end = '')
    if 'result' in data:
        html = data['result']
        return BeautifulSoup(html, "html.parser")
    elif 'error' in data:
        fatalError(
            data['error'],
            no_retry = [
                "Token query parameter not found",
                "Invalid client token",
                "You do not have enough credits",
                "Request-body is not a valid JSON",
                "Internal server error"
            ],
            indexAPI = 1)
    return BeautifulSoup("<html></html>", "html.parser")

def get_wayfair(target):
    soup = get_soup(target)
    price_tag = soup.find(attrs={"data-name-id": "PriceDisplay"})
    if price_tag:
        return float(price_tag.get_text(strip=True)[1:])
    return None

def get_amazon(target, location = AMAZON_ZIP_CODE):
    try:
        # Extract ASIN
        asin_position = target.find("/dp/")
        if asin_position == -1:
            return -1
        asin = target[asin_position + 4 : asin_position + 14]
        if "/" in asin:
            return -1

        # Build payload
        payload = {
            'source': 'amazon_pricing',
            'domain': 'com',
            'geo_location': location,
            'query': asin,
            'parse': True,
        }

        # Call API (add timeout to avoid hanging)
        response = requests.post(
            'https://realtime.oxylabs.io/v1/queries',
            auth=(OXYLABS_ACCOUNT, OXYLABS_PASSWORD),
            json=payload,
            timeout=15,
        )

        response.raise_for_status()  # raise if HTTP error
        data = response.json()

        # Extract price safely
        return (
                    data.get("results", [{}])[0]
                    .get("content", {})
                    .get("pricing", [{}])[0]
                    .get("price")
                )

    except Exception:
        return get_amazon_backup(target)

def get_amazon_backup(target):
    soup = get_soup(target)
    price_tag = soup.find(attrs={"id": "corePriceDisplay_desktop_feature_div"})
    if price_tag:
        price_tag_splitted = price_tag.get_text(strip=True).split()
        price_tag_splitted_second_time = price_tag_splitted[0][1:].split("$")
        return float(price_tag_splitted_second_time[0].replace(",", ""))
    return None

def fetch_price(target):
    retries = 6
    while retries:
        retries -= 1

        # Check platform
        price = None
        if target[:24] == "https://www.wayfair.com/":
            price = get_wayfair(target)
        elif target[:19] == "https://www.amazon.":
            price = get_amazon(target)
        else:
            print("Platform not supported!")
            return None

        # Check result
        if price == -1:
            print("URL Error!")
            return None
        if price:
            print(f"SUCCESS | Retries left: {retries} | {target} -> {price}")
            time.sleep(1)
            return price

        badAPI[alternateAPI] -= 1
        # If failed, retry
        print(f"FAILED  | Retries left: {retries} | {target}")
        time.sleep(3)
    return None

# === MAIN ===
if __name__ == "__main__":
    now = datetime.now(pytz.timezone(TIMEZONE))
    timestamp = now.strftime("%Y-%m-%d %H:%M")
    print(f"Running job at {timestamp}")

    urls = url_sheet.col_values(1)
    print("Found", len(urls)-1, "URLs configured.")  # DEBUG

    urls = urls[1:]  # skip header row
    if not urls:
        print("⚠️ No URLs found! Check your sheet.")
        exit()

    results = []

    with ThreadPoolExecutor(max_workers=2) as executor:
        future_to_url = {}
        for url in urls:
            future = executor.submit(fetch_price, url)
            future_to_url[future] = url
            time.sleep(1)

        for future in as_completed(future_to_url):
            url = future_to_url[future]
            try:
                price = future.result()
            except Exception as e:
                price = f"Error: {e}"
            results.append([timestamp, url, price])

    for i in range(5):
        try:
            log_sheet.append_rows(results, value_input_option="USER_ENTERED")
            print("✅ Logged successfully")
            break
        except Exception as e:
            print("❌ Logging failed:", e)
