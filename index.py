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
# from amzpy import AmazonScraper
import json

# === CONFIG ===
SHEET_URL = bimatquocgia.SHEET_URL
GOOGLE_SERVICE_ACCOUNT_FILE = bimatquocgia.SERVICE_ACCOUNT
SCRAPINGANT_API_KEY = bimatquocgia.SCRAPINGANT
OXYLABS_ACCOUNT = bimatquocgia.OXYLABS_ACCOUNT
OXYLABS_PASSWORD = bimatquocgia.OXYLABS_PASSWORD
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
def get_soup_scrapingant(target):
    # Get html using ScrapingAnt
    conn = http.client.HTTPSConnection("api.scrapingant.com")
    conn.request("GET", "/v2/general?url=" + target + "&x-api-key=" + SCRAPINGANT_API_KEY + "&proxy_country=US&return_page_source=true")

    res = conn.getresponse()
    data = res.read()
    html = data.decode("utf-8")

    return BeautifulSoup(html, "html.parser") # Translate to soup

def get_wayfair(target):
    soup = get_soup_scrapingant(target)
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
    soup = get_soup_scrapingant(target)
    price_tag = soup.find(attrs={"id": "corePriceDisplay_desktop_feature_div"})
    if price_tag:
        price_tag_splitted = price_tag.get_text(strip=True).split()
        price_tag_splitted_second_time = price_tag_splitted[0][1:].split("$")
        return float(price_tag_splitted_second_time[0].replace(",", ""))
    return None

def fetch_price(target):
    retries = 5
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
            return price
        # If failed, retry
        print("Extracting failed, retries remaining", retries)
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
    for url in urls:
        price = fetch_price(url)
        print(f"{url} -> {price}")
        results.append([timestamp, url, price if price is not None else "N/A"])
        time.sleep(1)

    try:
        log_sheet.append_rows(results, value_input_option="USER_ENTERED")
        print("✅ Logged successfully")
    except Exception as e:
        print("❌ Logging failed:", e)
