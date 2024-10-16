import logging
import yaml
import os
import pandas as pd
from utils import fetch_all_data, process_taxi_data, read_parquet_file

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    logger.info('Start of the script')

    logger.info('Loading the configuration from YAML file')
    with open('config.yaml', 'r') as f:
        config = yaml.safe_load(f)

    logger.info('Getting the BASE URL and date range from configuration')
    BASE_URL = os.getenv('BASE_URL', config['api']['base_url'])
    LIMIT = int(os.getenv('LIMIT', config['api']['limit']))
    START_DATE = config['date_ranges']['start_date_2']
    END_DATE = config['date_ranges']['end_date_2']

    logger.info(f"BASE_URL: {BASE_URL}, LIMIT: {LIMIT}, START_DATE: {START_DATE}, END_DATE: {END_DATE}")
    result = fetch_all_data(START_DATE, END_DATE, LIMIT, BASE_URL)
    logger.info(f"Total records fetched: {len(result)}")

    logger.info('Reading the locally stored data.')
    file_path = r'./data/taxi_trips.parquet'
    trips = read_parquet_file(file_path)

    logger.info('Ingesting the result records to the data retrieved from local storage.')
    new_trips = pd.DataFrame(result)
    new_trips['tpep_pickup_datetime'] = pd.to_datetime(new_trips['tpep_pickup_datetime'])
    new_trips['tpep_dropoff_datetime'] = pd.to_datetime(new_trips['tpep_dropoff_datetime'])
    trips = pd.concat([trips, new_trips], ignore_index=True)
    logger.info(f"Total records: {len(trips)}")

    logger.info('Saving the updated data to local disk.')
    file_path = r'./data/taxi_trips.parquet'
    trips.to_parquet(file_path, engine='pyarrow')
    logger.info(f"DataFrame saved as parquet file at: {file_path}")

    logger.info('Calculating the 45 day rolling average trip lengths with 1 step size.')
    mean_calculation = process_taxi_data(trips, 2)[2]
    logger.info(mean_calculation)

    logger.info('End of the script')

if __name__ == "__main__":
    main()