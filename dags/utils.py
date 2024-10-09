import os
import re
import time
import boto3
import pandas as pd
from bs4 import BeautifulSoup
from urllib.parse import urlparse, parse_qs
from selenium import webdriver
from fake_useragent import UserAgent
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.wait import WebDriverWait


# Constants
HOME = os.getcwd()
SAVE_PROGRESS = 20
HOME_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(HOME_DIR, "data", "url")
SCRAPED_DATA_DIR = os.path.join(HOME_DIR, "data", "scraped")
SCRAPED_FILE_PATH = os.path.join(SCRAPED_DATA_DIR, "scraped_data.xlsx")
FNM = "best_ingress_urls"
URL_FNM = os.path.join(
    SCRAPED_DATA_DIR.replace("scraped", "url"),
    f"{FNM.replace('urls', 'new_urls')}.xlsx",
)
MAX_THREADS = 10  # Adjust based on your system and network capacity
AIRPORTS_URL = "https://raw.githubusercontent.com/jpatokal/openflights/master/data/airports.dat"

# S3 configuration
S3_BUCKET_NAME = 'bluexpress'
S3_DRIVERS = "drivers"
S3_INPUT_FOLDER = 'flight-data/input'
S3_OUTPUT_FOLDER = 'flight-data/output'

# Create an S3 client
s3 = boto3.client('s3')
chromedriver = s3.get_object(S3_BUCKET_NAME)
custom_spinner = ['smooth', 'classic', 'classic2', 'brackets', 'blocks',
                  'bubbles', 'solid', 'checks', 'circles', 'squares',
                  'halloween', 'filling', 'notes', 'ruler', 'ruler2',
                  'fish', 'scuba']


def save_file(data_df, filename, index=False):
    if os.path.exists(filename):
        existing_data_df = pd.read_excel(filename)
        updated_data_df = pd.concat([existing_data_df, data_df], ignore_index=True)
        updated_data_df.to_excel(filename, index=index)
        print(f"File saved!: {filename}")
    else:
        directory = os.path.dirname(filename)
        os.makedirs(directory, exist_ok=True)
        data_df.to_excel(filename, index=index)
        print(f"File saved!: {filename}")



def load_product_page(url):
    ua = UserAgent()
    user_agent = ua.random

    options = webdriver.ChromeOptions()
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--no-sandbox")
    options.add_argument('--headless')
    options.add_argument(f'user-agent={user_agent}')
    options.add_argument("--incognito")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option("useAutomationExtension", False)

    with webdriver.Chrome(options=options, service=Service(executable_path=f"{S3_BUCKET_NAME}/{S3_DRIVERS}/chromedriver")) as driver:
        try:
            WebDriverWait(driver, timeout=10)
            driver.get(url)

            for _ in range(4):
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(2)

            driver.maximize_window()
            time.sleep(1)

            page_source = driver.page_source
            soup = BeautifulSoup(page_source, "html.parser")

        except Exception as e:
            print(f"An error occurred while using driver for: {url}: {e}")

    return soup


def separate_flights(flight_info):
    # Initialize an empty list to store the separated flight information
    flights = []
    # Temporary list to accumulate details for each flight
    current_flight = []

    for info in flight_info:
        if info == '':
            # If an empty string is encountered, append the accumulated flight details to the flights list
            if current_flight:
                flights.append(current_flight)
                current_flight = []
        else:
            # Otherwise, add the information to the current flight details
            current_flight.append(info)
    
    # Don't forget to add the last flight details if the list did not end with an empty string
    if current_flight:
        flights.append(current_flight)

    return flights


def extract_suffix_from_query(url):
    """
    Extracts the numeric suffix from the `pageOrigin` query parameter in the URL.
    Assumes that the suffix is the last part of the `pageOrigin` parameter value.
    """
    query = urlparse(url).query
    params = parse_qs(query)
    page_origin = params.get('pageOrigin', [''])[0]
    
    # Split `pageOrigin` by '.' and get the last part
    suffix_parts = page_origin.split('.')[-1]
    
    return suffix_parts


def filter_first_links(urls):
    """
    Filters out only the first link for each flight based on the suffix extracted from `pageOrigin`.
    """
    seen_suffixes = set()
    first_links = []
    
    for url in urls:
        suffix = extract_suffix_from_query(url)
        if suffix and suffix not in seen_suffixes:
            seen_suffixes.add(suffix)
            first_links.append(url)
    
    return first_links


def get_flight_details(bsp):
    flight_info = bsp.find_all("div", class_="bottom")
    res = [carrier.get_text(strip=True) for carrier in flight_info]
    flights = separate_flights(res)
    split_flights = []
    for flight in flights:
        airline, departure, stopovers, duration, arrival = flight
        split_flights.append({
            'Airline': airline,
            'Departure': departure,
            'Stopovers': stopovers,
            'Duration': duration,
            'Arrival': arrival
        })
    return split_flights


def get_flight_link(bsp):
    flight_ = bsp.find_all("a", class_="booking-link")
    flight_url = [flt_url.get("href") for flt_url in flight_]
    flight_link = filter_first_links(flight_url)
    return flight_link


def get_flight_price(bsp):
    flight_cost = bsp.find_all("div", class_="Base-Results-HorizonResult Flights-Results-FlightResultItem responsive phoenix-rising phoenix-rising")
    price = [cost.get("aria-label") for cost in flight_cost]
    prices = [re.search(r'\$(\d{1,3}(?:,\d{3})*)', result).group(1) for result in price]
    return prices