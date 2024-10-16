import unittest
import pandas as pd
#import numpy as np
#from pandas.testing import assert_frame_equal
from utils import process_taxi_data, read_parquet_file

class TestTask2(unittest.TestCase):

    def test_read_parquet_file(self):
        file_path = './data/taxi_trips.parquet'
        df = read_parquet_file(file_path)
        self.assertIsInstance(df, pd.DataFrame)
        self.assertGreater(len(df), 0)
        self.assertIn('tpep_pickup_datetime', df.columns)
        self.assertIn('tpep_dropoff_datetime', df.columns)

    def test_process_taxi_data(self):
        df = pd.DataFrame({
            'tpep_pickup_datetime': pd.date_range(start='2023-01-01', periods=100, freq='h'),
            'tpep_dropoff_datetime': pd.date_range(start='2023-01-01 01:00:00', periods=100, freq='h')
        })
        
        result = process_taxi_data(df, choice=2)
        trip_lengths, daily_summary, *_ = result

        self.assertIsInstance(trip_lengths, pd.DataFrame)
        self.assertIsInstance(daily_summary, pd.DataFrame)

        self.assertEqual(list(daily_summary.columns), ['tpep_pickup_datetime', 'daily_trip_time (in hours)', 'rolling_average'])
        self.assertEqual(len(daily_summary), 5)  # 5 days in the sample data

        self.assertEqual(daily_summary['tpep_pickup_datetime'].dt.date.tolist(), 
                         pd.date_range(start='2023-01-01', periods=5).date.tolist())
        
        expected_daily_trip_time = [24.0, 24.0, 24.0, 24.0, 4.0]
        self.assertListEqual(daily_summary['daily_trip_time (in hours)'].tolist(), expected_daily_trip_time)

        # Check if rolling average is calculated correctly
        self.assertAlmostEqual(daily_summary['rolling_average'].iloc[-1], 20.0, places=1)

    def test_process_taxi_data_empty(self):
        empty_df = pd.DataFrame(columns=['tpep_pickup_datetime', 'tpep_dropoff_datetime'])
        result = process_taxi_data(empty_df, choice=2)
        trip_lengths, daily_summary, *_ = result
        self.assertTrue(trip_lengths.empty)
        self.assertTrue(daily_summary.empty)

    def test_process_taxi_data_single_day(self):
        df = pd.DataFrame({
            'tpep_pickup_datetime': pd.date_range(start='2023-01-01', periods=24, freq='h'),
            'tpep_dropoff_datetime': pd.date_range(start='2023-01-01 01:00:00', periods=24, freq='h')
        })
        result = process_taxi_data(df, choice=2)
        trip_lengths, daily_summary, *_ = result
        self.assertEqual(len(daily_summary), 1)
        self.assertAlmostEqual(daily_summary['daily_trip_time (in hours)'].iloc[0], 24.0)
        self.assertAlmostEqual(daily_summary['rolling_average'].iloc[0], 24.0)

if __name__ == '__main__':
    unittest.main()
