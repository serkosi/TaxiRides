import logging
import yaml
import os
import pandas as pd
from utils import fetch_all_data, process_taxi_data

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
    START_DATE = config['date_ranges']['start_date_1']
    END_DATE = config['date_ranges']['end_date_1']

    logger.info(f"BASE_URL: {BASE_URL}, LIMIT: {LIMIT}, START_DATE: {START_DATE}, END_DATE: {END_DATE}")
    result = fetch_all_data(START_DATE, END_DATE, LIMIT, BASE_URL)
    logger.info(f"Total records fetched: {len(result)}")

    logger.info('Converting the result to a DataFrame in datetime format.')
    trips = pd.DataFrame(result)
    trips['tpep_pickup_datetime'] = pd.to_datetime(trips['tpep_pickup_datetime'])
    trips['tpep_dropoff_datetime'] = pd.to_datetime(trips['tpep_dropoff_datetime'])

    logger.info('Saving the data to local disk.')
    file_path = r'./data/taxi_trips.parquet'
    trips.to_parquet(file_path, engine='pyarrow')
    logger.info(f"DataFrame saved as parquet file at: {file_path}")

    logger.info('Calculating daily trip lengths and the average trip length of all yellow taxis for a month.')
    aggregated_trips = process_taxi_data(trips, 1)[1]
    logger.info(aggregated_trips)
    mean_calculation = process_taxi_data(trips, 1)[2]
    logger.info(f"Average trip length of all yellow taxis for a month: {mean_calculation:.1f} hours")

    logger.info('End of the script')

if __name__ == "__main__":
    main()