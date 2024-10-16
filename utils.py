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
    """Make an API request with retry logic."""
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.json()
    except RequestException as e:
        logger.error(f"API request failed: {str(e)}")
        raise

def create_params(start_date, end_date, limit, offset):
    """Create parameters for API request. To keep the data stored in small size in the production setup, 
    retrieved records are constrained with only two attributes we need for the tasks."""
    return {
        '$select': 'tpep_pickup_datetime, tpep_dropoff_datetime',
        '$where': f"tpep_pickup_datetime >= '{start_date}' AND tpep_pickup_datetime <= '{end_date}'",
        '$limit': str(limit),
        '$offset': str(offset),
        '$order': 'tpep_pickup_datetime'
    }

def process_response(data, all_data):
    if not data:
        return False
    all_data.extend(data)
    logger.info(f"Fetched {len(data)} records. Total records: {len(all_data)}")
    return True

def fetch_all_data(start_date, end_date, limit, base_url):
    all_data = []
    offset = 0

    while True:
        params = create_params(start_date, end_date, limit, offset)
        url = f"{base_url}?{urlencode(params)}"

        try:
            data = make_api_request(url)
            if not process_response(data, all_data):
                break
            offset += len(data)
        except Exception as e:
            logger.error(f"Error fetching data: {str(e)}")
            break

    return all_data

def calculate_trip_length(df):
    df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
    df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])
    # get length of each trip 
    df['trip_length'] = (df['tpep_dropoff_datetime'] - df['tpep_pickup_datetime']).dt.total_seconds() / 60
    return df

def aggregate_daily_trips(df):
    daily_summary = df.groupby(df['tpep_pickup_datetime'].dt.date)['trip_length'].sum().reset_index()
    daily_summary['daily_trip_time (in hours)'] = (daily_summary['trip_length'] / 60).round(1)
    daily_summary.drop('trip_length', axis=1, inplace=True)
    daily_summary['tpep_pickup_datetime'] = pd.to_datetime(daily_summary['tpep_pickup_datetime'])
    return daily_summary    

def calculate_rolling_average(df, window):
    df['rolling_average'] = df['daily_trip_time (in hours)'].rolling(window=window, min_periods=1).mean().round(1)
    return df    

def process_taxi_data(df, choice):
    """Process taxi data and calculate trip lengths."""
    trip_lengths = calculate_trip_length(df)
    daily_summary = aggregate_daily_trips(trip_lengths)
    if choice == 1:
        mean_calculation = daily_summary['daily_trip_time (in hours)'].mean()
    if choice == 2:
        mean_calculation = calculate_rolling_average(daily_summary, 45)
    return trip_lengths, daily_summary, mean_calculation

def read_parquet_file(file_path):
    """Read a Parquet file and return a DataFrame."""
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

#def save_to_parquet(df, file_path):
#    """Save DataFrame to Parquet file."""
#    try:
#        df.to_parquet(file_path, engine='pyarrow')
#        logger.info(f"DataFrame saved as parquet file at: {file_path}")
#    except Exception as e:
#        logger.error(f"Error saving to parquet file: {str(e)}")
#        raise
