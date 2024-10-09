"""
Extract daily flight urls
"""

import os
import time
import utils
import logging
import numpy as np
import pandas as pd
from io import BytesIO
from typing import Dict
from concurrent.futures import ThreadPoolExecutor, as_completed
from alive_progress import alive_bar
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook



# Configure logging
logging.basicConfig(
    format='%(asctime)s - %(levelname)s - %(message)s', level=logging.INFO
)


#def load_airport_data(url=utils.AIRPORTS_URL):
#    """Loads airport data from a given URL.
#
#    Args:
#        url (str, optional): URL to load airport data from. Defaults to AIRPORTS_URL.
#
#    Returns:
#        pandas.DataFrame or None: DataFrame containing airport data or None if loading fails.
#    """
#    try:
#        airport_df = pd.read_csv(
#            url,
#            header=None,
#            names=[
#                "airportID", "name", "city", "country", "iata", "icao",
#                "latitude", "longitude", "altitude", "timezone", "dst",
#                "tz_database_timezone", "type", "source"
#            ]
#        )
#        logging.info("Airport data loaded successfully.")
#        return airport_df
#    except Exception as e:
#        logging.error(f"Error loading airport data from {url}: {e}")
#        return None
    

def load_airport_data(url=utils.AIRPORTS_URL):
    """Loads airport data from a given URL or S3."""
    try:
        if url.startswith('s3://'):
            # Load from S3
            s3_path = url.replace('s3://', '').split('/', 1)
            obj = utils.s3.get_object(Bucket=s3_path[0], Key=s3_path[1])
            airport_df = pd.read_csv(obj['Body'], header=None, names=[
                "airportID", "name", "city", "country", "iata", "icao",
                "latitude", "longitude", "altitude", "timezone", "dst",
                "tz_database_timezone", "type", "source"
            ])
        else:
            # Load from URL
            airport_df = pd.read_csv(url, header=None, names=[
                "airportID", "name", "city", "country", "iata", "icao",
                "latitude", "longitude", "altitude", "timezone", "dst",
                "tz_database_timezone", "type", "source"
            ])
        logging.info("Airport data loaded successfully.")
        return airport_df
    except Exception as e:
        logging.error(f"Error loading airport data from {url}: {e}")
        return None



def clean_airport_data(df):
    """Cleans the airport DataFrame by:
        - Replacing "\\N" with NaN in 'iata' column
        - Dropping rows with NaN 'iata' values
        - Removing airbases and airstrips

    Args:
        df (pandas.DataFrame): DataFrame containing airport data.

    Returns:
        pandas.DataFrame or None: Cleaned DataFrame or None if input is None.
    """
    if df is not None:
        df["iata"] = df["iata"].replace("\\N", np.nan)
        df.dropna(subset=["iata"], inplace=True)

        # Remove airbases and airstrips more efficiently
        df = df[~df["name"].str.endswith(("Base", "Airstrip"))]

        logging.info("Data cleaning completed.")
        return df
    else:
        logging.warning("Data cleaning skipped due to missing DataFrame.")
        return None


def generate_urls(airport_df, base_url, start_date, num_days, trip_type):
    """Generates flight URLs based on airport data, base URL, date range, and trip type.

    Args:
        airport_df (pandas.DataFrame): DataFrame containing airport data with 'iata' column.
        base_url (str): Base URL string with placeholders for IATA code and date.
        start_date (datetime): Starting date for URL generation.
        num_days (int): Number of consecutive days to generate URLs for.
        trip_type (str): Type of trip, used as a key in the output dictionary (e.g. "ingress_flight_url").

    Returns:
        pandas.DataFrame: DataFrame containing generated URLs and airport information.
    """
    all_urls = []
    for date_offset in range(num_days):
        date_str = (start_date + timedelta(days=date_offset)).strftime("%Y-%m-%d")
        for _, row in airport_df.iterrows():
            url = base_url.format(row["iata"], date_str)
            all_urls.append({
                trip_type: url,
                "name": row["name"],
                "city": row["city"],
                "country": row["country"]
            })
    logging.info(f"Generated {len(all_urls)} {trip_type} URLs for {num_days} days.")
    return pd.DataFrame(all_urls)


def save_to_excel(df, filename):
    """Saves a DataFrame to an Excel file.

    Args:
        df (pandas.DataFrame): DataFrame to save.
        filename (str): Name of the Excel file.
    """
    if not os.path.exists(utils.DATA_DIR):
        os.makedirs(utils.DATA_DIR, exist_ok=True)
    filepath = os.path.join(utils.DATA_DIR, filename)
    try:
        df.to_excel(filepath, index=False)
        logging.info(f"Data saved to {filepath}")
    except Exception as e:
        logging.error(f"Error saving data to {filepath}: {e}")


def save_to_s3(df, filename, folder):
    """Saves a DataFrame to an S3 bucket."""
    try:
        csv_buffer = df.to_csv(index=False)
        utils.s3.put_object(Bucket=utils.S3_BUCKET_NAME, Key=f'{folder}/{filename}', Body=csv_buffer.encode())
        logging.info(f"Data saved to s3://{utils.S3_BUCKET_NAME}/{folder}/{filename}")
    except Exception as e:
        logging.error(f"Error saving data to s3://{utils.S3_BUCKET_NAME}/{folder}/{filename}: {e}")


def load_excel_from_s3(bucket_name: str, file_key: str) -> pd.DataFrame:
    """
    Load an Excel file from an S3 bucket into a Pandas DataFrame.

    Parameters:
    - bucket_name (str): The name of the S3 bucket.
    - file_key (str): The path to the Excel file within the bucket.

    Returns:
    - pd.DataFrame: DataFrame containing the contents of the Excel file.
    
    Raises:
    - ValueError: If the file cannot be read as an Excel file.
    - Exception: For any other unexpected errors.
    """
    try:

        # Fetch the file from S3
        logging.info(f"Fetching the file '{file_key}' from bucket '{bucket_name}'")
        response = utils.s3.get_object(Bucket=bucket_name, Key=file_key)

        # Read the Excel file into a DataFrame
        logging.info("Reading the Excel file into a DataFrame")
        df = pd.read_excel(BytesIO(response['Body'].read()), engine='openpyxl')

        logging.info("Successfully loaded the Excel file into a DataFrame")
        return df

    except ValueError as ve:
        logging.error("ValueError encountered: %s", ve)
        raise ValueError("Could not read the file as an Excel file. Please check the file format.") from ve
    except Exception as e:
        logging.exception("An error occurred while loading the Excel file")
        raise e
    

def load_data_from_s3(bucket_name: str, key: str, **kwargs):
    """
    Loads data from an S3 bucket.

    Args:
        bucket_name (str): Name of the S3 bucket.
        key (str): Key of the object in the bucket.

    Returns:
        data (object): Loaded data.
    """
    s3_hook = S3Hook()
    file_content = s3_hook.read_key(key=key, bucket_name=bucket_name)
    return file_content


def save_data_url_files(data_df: pd.DataFrame, url_df: pd.DataFrame) -> None:
    """Saves scraped data and URL data to disk."""
    logging.info("Saving file on disk...")
    utils.save_file(data_df, utils.SCRAPED_FILE_PATH, index=False)
    url_df.to_excel(utils.URL_FNM, index=False)


def scrape_flight_data(row: pd.Series) -> Dict:
    """Scrapes flight data for a given URL."""
    url = row["ingress_flight_url"]
    try:
        soup = utils.load_product_page(url)
        time.sleep(2)
        if soup:  # Check if soup is not None
            return {
                "country": row["country"],
                "city": row["city"],
                "flight_details": utils.get_flight_details(soup),
                "flight_price": utils.get_flight_price(soup),
                "flight_url": utils.get_flight_link(soup),
            }
        else:
            logging.error(f"Failed to load page: {url}")
            return {"error": f"Failed to load page: {url}"}
    except Exception as e:
        logging.error(f"An error occurred while scraping - {url}: {e}")
        return {"error": str(e)}  # Return error information


def flight_urls_extraction():
    """Main function to execute the URL generation and saving process."""
    # Base URLs
    base_urls = {
        "best": {
            "ingress": "https://booking.kayak.com/flights/{0}-NBO/{1}?sort=bestflight_a",
            "egress": "https://booking.kayak.com/flights/NBO-{0}/{1}?sort=bestflight_a"
        },
        "cheap": {
            "ingress": "https://booking.kayak.com/flights/{0}-NBO/{1}?sort=price_a",
            "egress": "https://booking.kayak.com/flights/NBO-{0}/{1}?sort=price_a"
        },
        "quick": {
            "ingress": "https://booking.kayak.com/flights/{0}-NBO/{1}?sort=duration_a",
            "egress": "https://booking.kayak.com/flights/NBO-{0}/{1}?sort=duration_a"
        }
    }

    # Customizable parameters
    start_date = datetime.now()
    num_days = 7

    # Load and clean data
    airport_df = load_airport_data()
    airport_df = clean_airport_data(airport_df)

    if airport_df is not None:
        for sort_type, urls in base_urls.items():
            for trip_type in ["ingress", "egress"]:
                # Generate URLs
                urls_df = generate_urls(
                    airport_df,
                    urls[trip_type],
                    start_date,
                    num_days,
                    f"{trip_type}_flight_url"
                )
                # Save to Excel
                save_to_excel(urls_df, f"{sort_type}_{trip_type}_urls.xlsx")
                save_to_s3(urls_df, f"{sort_type}_{trip_type}_urls.xlsx", f"{utils.S3_INPUT_FOLDER}/urls")
    else:
        logging.error("URL generation skipped due to missing DataFrame.")


def flight_details_extraction():
    """Main function to execute the scraping process."""
    # Load URL data
    #url_filepath = os.path.join(utils.DATA_DIR, f"{utils.FNM}.xlsx")
    url_filename = f"{utils.FNM}.xlsx"
    url_s3_path = f"s3://{utils.S3_BUCKET_NAME}/{utils.S3_INPUT_FOLDER}/urls/{url_filename}"
    #if not os.path.exists(url_s3_path):
    #    logging.error(f"URL file not found: {url_s3_path}")
    #    return  # Exit early if the file doesn't exist

    #url_df = pd.read_excel(BytesIO(url_s3_path['Body'].read()), engine="openpyxl")
    url_df = load_excel_from_s3(utils.S3_BUCKET_NAME, url_s3_path)

    url_df.drop_duplicates(subset=["ingress_flight_url"], inplace=True)
    url_df["extracted"] = url_df.get("extracted", "no")

    # Initialize DataFrame with preallocated size
    data_df = pd.DataFrame(
        columns=[
            "country",
            "city",
            "flight_details",
            "flight_price",
            "flight_url",
            "error",  # Add error column
        ],
        dtype=object,
    )

    with alive_bar(total=len(url_df), bar="bubbles") as bar:
        with ThreadPoolExecutor(max_workers=utils.MAX_THREADS) as executor:
            # Submit tasks to the thread pool and keep track of their order
            futures = [executor.submit(scrape_flight_data, row) for _, row in url_df.iterrows() if row["extracted"] != "yes"]

            # Process completed tasks in the order they were submitted
            for index, future in enumerate(futures):
                try:
                    flight_data = future.result()
                except Exception as e:
                    logging.error(f"An error occurred in thread: {e}")
                    flight_data = {"error": str(e)}

                data_df.loc[index] = flight_data  # Assuming index corresponds to data_df
                url_df.at[index, "extracted"] = "yes" if "error" not in flight_data else "error"

                bar()

                if (index + 1) % utils.SAVE_PROGRESS == 0:
                    logging.info(
                        f"Processed {index + 1} URLs, saving progress to disk."
                    )
                    save_data_url_files(data_df, url_df)
                    save_to_s3(data_df, utils.SCRAPED_FILE_PATH, f"{utils.S3_OUTPUT_FOLDER}/scraped")
                    save_to_s3(url_df, f"{utils.FNM}.xlsx", f"{utils.S3_OUTPUT_FOLDER}/urls")

    # Final save after processing all URLs
    save_data_url_files(data_df, url_df)
    save_to_s3(data_df, utils.SCRAPED_FILE_PATH, f"{utils.S3_OUTPUT_FOLDER}")

with DAG(
    dag_id="flight_urls",
    default_args={"owner": "airflow"},
    schedule_interval="@daily",
    description="Extract flight urls",
    start_date=datetime.today(),
    tags=["extract"],
    catchup=False
) as dag:
    extract_urls_task = PythonOperator(
        task_id="extract_urls_task",
        python_callable=flight_urls_extraction
    )
    extract_details_task = PythonOperator(
        task_id="extract_details_task",
        python_callable=flight_details_extraction
    )

    extract_urls_task >> extract_details_task

