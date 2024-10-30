from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from requests.exceptions import RequestException
import requests
import logging
import pandas as pd
from urllib.parse import urlencode
from pyarrow.lib import ArrowIOError

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=4, max=10),
    retry=retry_if_exception_type(RequestException)
)
def make_api_request(url):
    """
    Make an API request with retry logic.

    Args:
        url (str): The URL to make the request to

    Returns:
        dict: JSON response from the API

    Raises:
        RequestException: If the API request fails after all retry attempts
    """
    
    try:
        response = requests.get(url)
        response.raise_for_status()
        logger.debug(f"Successful API request to: {url}")  # Added debug level logging for successful requests
        return response.json()
    except RequestException as e:
        # what else can be implemented here?
        logger.error(f"API request failed: {str(e)}", exc_info=True) # Included exc_info=True when logging exceptions to capture stack traces
        raise


def create_params(start_date, end_date, limit, offset):
    """
    Creates API request parameters for fetching taxi ride data.

    Args:
        start_date (str): Start date for the data range
        end_date (str): End date for the data range
        limit (int): Maximum number of records to retrieve per request
        offset (int): Number of records to skip

    Returns:
        dict: Dictionary containing API query parameters
    """

    return {
        '$select': 'tpep_pickup_datetime, tpep_dropoff_datetime', # duplications?
        '$where': f"tpep_pickup_datetime >= '{start_date}' AND tpep_pickup_datetime <= '{end_date}'",
        '$limit': str(limit),
        '$offset': str(offset),
        '$order': 'tpep_pickup_datetime' # might not need to order
    }


def process_response(data, all_data):
    """
    Processes API response data and adds it to the collection.

    Args:
        data (list): New data received from the API
        all_data (list): Existing collection of all data

    Returns:
        bool: False if no data received, True if data was successfully processed
    """

    if not data:
        return False
    all_data.extend(data)
    logger.info(f"Fetched {len(data)} records. Total records: {len(all_data)}")
    return True


def fetch_all_data(start_date, end_date, limit, base_url):
    """
    Retrieves all taxi ride data for a given date range using pagination.

    Args:
        start_date (str): Start date for the data range
        end_date (str): End date for the data range
        limit (int): Maximum number of records per API request
        base_url (str): Base URL for the API endpoint

    Returns:
        list: Collection of all retrieved taxi ride data
    
    Raises:
        RequestException: If API requests consistently fail
        Exception: For other unexpected errors during data fetching
    """

    all_data = []
    offset = 0

    while True:
        params = create_params(start_date, end_date, limit, offset)
        url = f"{base_url}?{urlencode(params)}"

        try:
            data = make_api_request(url)
            if not process_response(data, all_data):
                logger.info("No more data to fetch")  # Added info log for normal completion
                break
            offset += len(data)
        except Exception as e:
            logger.error(f"Error fetching data: {str(e)}", exc_info=True)  # Added exc_info
            break

    return all_data


def calculate_trip_length(df):
    """
    Calculates trip duration in minutes for each taxi ride.

    Args:
        df (pandas.DataFrame): DataFrame containing pickup and dropoff timestamps

    Returns:
        pandas.DataFrame: DataFrame with added 'trip_length' column in minutes
    """

    df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
    df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])
    # get length of each trip 
    df['trip_length'] = (df['tpep_dropoff_datetime'] - df['tpep_pickup_datetime']).dt.total_seconds() / 60
    return df


def aggregate_daily_trips(df):
    """
    Aggregates trip data by date and calculates daily total trip times.

    Args:
        df (pandas.DataFrame): DataFrame containing trip data

    Returns:
        pandas.DataFrame: Daily summary with total trip times in hours
    """

    daily_summary = df.groupby(df['tpep_pickup_datetime'].dt.date)['trip_length'].sum().reset_index()
    daily_summary['daily_trip_time (in hours)'] = (daily_summary['trip_length'] / 60).round(1)
    daily_summary.drop('trip_length', axis=1, inplace=True)
    daily_summary['tpep_pickup_datetime'] = pd.to_datetime(daily_summary['tpep_pickup_datetime'])
    return daily_summary    


def calculate_rolling_average(df, window):
    """
    Calculates rolling average of daily trip times.

    Args:
        df (pandas.DataFrame): DataFrame containing daily trip summaries
        window (int): Size of the rolling window in days

    Returns:
        pandas.DataFrame: DataFrame with added rolling average column
    """

    df['rolling_average'] = df['daily_trip_time (in hours)'].rolling(window=window, min_periods=1).mean().round(1)
    return df    


def process_taxi_data(df, choice): # single responsibility might be important
    """
    Processes taxi data and calculates statistics based on user choice.

    Args:
        df (pandas.DataFrame): Raw taxi ride data
        choice (int): Processing option (1 for mean calculation, 2 for rolling average)

    Returns:
        tuple: (trip_lengths DataFrame, daily_summary DataFrame, mean_calculation)
    """

    trip_lengths = calculate_trip_length(df)
    daily_summary = aggregate_daily_trips(trip_lengths)
    # this is not preffered (with choices)
    if choice == 1:
        mean_calculation = daily_summary['daily_trip_time (in hours)'].mean()
    if choice == 2:
        mean_calculation = calculate_rolling_average(daily_summary, 45)
    return trip_lengths, daily_summary, mean_calculation


def read_parquet_file(file_path):
    """
    Reads a Parquet file into a pandas DataFrame.

    Args:
        file_path (str): Path to the Parquet file

    Returns:
        pandas.DataFrame: DataFrame containing the Parquet file data

    Raises:
        FileNotFoundError: If the specified file doesn't exist
        ArrowIOError: If there's an error reading the Parquet file
        Exception: For other unexpected errors
    """

    try:
        df = pd.read_parquet(file_path, engine='pyarrow')
        return df
    except FileNotFoundError:
        logger.error(f"File not found: {file_path}")
        raise
    except ArrowIOError as e:
        logger.error(f"Error reading parquet file: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error reading file: {str(e)}")
        raise