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

def load_airport_data(url: str) -> pd.DataFrame:
    """Loads airport data from a given URL or S3 path.

    Args:
        url (str): URL or S3 path to load airport data from. 

    Returns:
        pandas.DataFrame: DataFrame containing airport data.
        
    Raises:
        Exception: If unable to load data from the provided URL or S3 path.
    """
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
        raise  # Re-raise the exception to halt execution


def clean_airport_data(df: pd.DataFrame) -> pd.DataFrame:
    """Cleans the airport DataFrame by:
        - Replacing "\\N" with NaN in 'iata' column
        - Dropping rows with NaN 'iata' values
        - Removing airbases and airstrips

    Args:
        df (pandas.DataFrame): DataFrame containing airport data.

    Returns:
        pandas.DataFrame: Cleaned DataFrame.
    """
    df["iata"] = df["iata"].replace("\\N", np.nan)
    df.dropna(subset=["iata"], inplace=True)
    df = df[~df["name"].str.contains("Base|Airstrip", case=False, na=False)]
    logging.info("Data cleaning completed.")
    return df


def generate_urls(airport_df: pd.DataFrame, base_url: str, start_date: datetime, num_days: int, trip_type: str) -> pd.DataFrame:
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


#def save_to_s3(df: pd.DataFrame, filename: str, folder: str) -> None:
#    """Saves a DataFrame to an S3 bucket.
#
#    Args:
#        df (pandas.DataFrame): DataFrame to save.
#        filename (str): Name of the file to save in S3.
#        folder (str): Folder path in S3 bucket to save the file.
#    """
#    try:
#        csv_buffer = df.to_csv(index=False)
#        utils.s3.put_object(Bucket=utils.S3_BUCKET_NAME, Key=f'{folder}/{filename}', Body=csv_buffer.encode())
#        logging.info(f"Data saved to s3://{utils.S3_BUCKET_NAME}/{folder}/{filename}")
#    except Exception as e:
#        logging.error(f"Error saving data to s3://{utils.S3_BUCKET_NAME}/{folder}/{filename}: {e}")
#        raise  # Re-raise to trigger retry or failure handling in Airflow


def save_to_s3(df: pd.DataFrame, filename: str, folder: str) -> None:
    """Saves a DataFrame to an S3 bucket as an Excel file."""
    try:
        # Create a BytesIO object to store the Excel file in memory
        excel_buffer = BytesIO()
        df.to_excel(excel_buffer, index=False)
        excel_buffer.seek(0)  # Reset the buffer pointer to the beginning

        utils.s3.put_object(Bucket=utils.S3_BUCKET_NAME, Key=f'{folder}/{filename}', Body=excel_buffer.read())
        logging.info(f"Data saved to s3://{utils.S3_BUCKET_NAME}/{folder}/{filename}")
    except Exception as e:
        logging.error(f"Error saving data to s3://{utils.S3_BUCKET_NAME}/{folder}/{filename}: {e}")
        raise 


def load_excel_from_s3(bucket_name: str, file_key: str) -> pd.DataFrame:
    """
    Load an Excel file from an S3 bucket into a Pandas DataFrame.

    Parameters:
    - bucket_name (str): The name of the S3 bucket.
    - file_key (str): The path to the Excel file within the bucket.

    Returns:
    - pd.DataFrame: DataFrame containing the contents of the Excel file.
    
    Raises:
    - Exception: For any errors during file loading.
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

    except Exception as e:
        logging.exception("An error occurred while loading the Excel file")
        raise  # Re-raise to handle in Airflow


def scrape_flight_data(row: pd.Series) -> Dict:
    """Scrapes flight data for a given URL."""
    url = row["ingress_flight_url"]
    max_retries = 3
    retry_delay = 5
    for attempt in range(1, max_retries + 1):
        try:
            soup = utils.load_product_page(url)
            time.sleep(2)
            if soup:
                return {
                    "country": row["country"],
                    "city": row["city"],
                    "flight_details": utils.get_flight_details(soup),
                    "flight_price": utils.get_flight_price(soup),
                    "flight_url": utils.get_flight_link(soup),
                }
            else:
                logging.warning(f"Failed to load page: {url}, attempt {attempt}/{max_retries}")
        except Exception as e:
            logging.warning(f"An error occurred while scraping - {url}, attempt {attempt}/{max_retries}: {e}")
            
        if attempt < max_retries:
            logging.info(f"Retrying in {retry_delay} seconds...")
            time.sleep(retry_delay)
    
    logging.error(f"Max retries reached for URL: {url}")
    return {"error": f"Failed to load page after {max_retries} attempts"}


def flight_urls_extraction(**kwargs):
    """Extracts flight URLs and saves them to S3."""
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
    airport_df = load_airport_data(kwargs["airport_data_url"])  # Pass URL through Airflow variables
    airport_df = clean_airport_data(airport_df)

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
            # Save to S3
            save_to_s3(urls_df, f"{sort_type}_{trip_type}_urls.xlsx", f"{utils.S3_INPUT_FOLDER}/urls")


def flight_details_extraction(**kwargs):
    """Extracts flight details and saves them to S3."""
    url_filename = f"{utils.FNM}.xlsx"
    url_s3_path = f"{utils.S3_INPUT_FOLDER}/urls/{url_filename}"

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
            "error",
        ],
        dtype=object,
    )

    total_urls = len(url_df)
    processed_urls = 0

    with alive_bar(total=total_urls, bar="bubbles") as bar:
        with ThreadPoolExecutor(max_workers=utils.MAX_THREADS) as executor:
            futures = [executor.submit(scrape_flight_data, row) for _, row in url_df.iterrows() if row["extracted"] != "yes"]

            for index, future in enumerate(as_completed(futures)):
                try:
                    flight_data = future.result()
                except Exception as e:
                    logging.error(f"An error occurred in thread: {e}")
                    flight_data = {"error": str(e)}

                data_df.loc[index] = flight_data
                url_df.at[index, "extracted"] = "yes" if "error" not in flight_data else "error"

                processed_urls += 1
                bar()

                if processed_urls % utils.SAVE_PROGRESS == 0 or processed_urls == total_urls:
                    logging.info(f"Processed {processed_urls}/{total_urls} URLs, saving progress to S3.")
                    save_to_s3(data_df, "scraped_data.xlsx", f"{utils.S3_OUTPUT_FOLDER}/scraped")
                    save_to_s3(url_df, f"{utils.FNM}.xlsx", f"{utils.S3_OUTPUT_FOLDER}/urls")


with DAG(
    dag_id="flight_info",
    default_args={"owner": "airflow"},
    schedule_interval="@daily",
    description="Extract flight urls and details",
    start_date=datetime.today(),
    tags=["extract"],
    catchup=False
) as dag:
    extract_urls_task = PythonOperator(
        task_id="extract_urls_task",
        python_callable=flight_urls_extraction,
        op_kwargs={"airport_data_url": "https://raw.githubusercontent.com/jpatokal/openflights/master/data/airports.dat"}  # Example: Pass airport data URL here
    )
    extract_details_task = PythonOperator(
        task_id="extract_details_task",
        python_callable=flight_details_extraction
    )

    extract_urls_task >> extract_details_task

