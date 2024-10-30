import unittest
from unittest.mock import patch, MagicMock
from utils import make_api_request, create_params, process_response, fetch_all_data, calculate_trip_length, aggregate_daily_trips, calculate_rolling_average, process_taxi_data
import pandas as pd
from pandas.testing import assert_frame_equal

class TestTask1(unittest.TestCase):

    @patch('utils.requests.get')
    def test_make_api_request(self, mock_get):
        mock_response = MagicMock()
        mock_response.json.return_value = {'data': 'test'}
        mock_get.return_value = mock_response
        
        result = make_api_request('http://test.com')
        self.assertEqual(result, {'data': 'test'})

    def test_create_params(self):
        params = create_params('2023-01-01', '2023-01-31', 5000, 0)
        self.assertIn('$select', params)
        self.assertIn('$where', params)
        self.assertIn('$limit', params)

    def test_process_response(self):
        all_data = []
        data = [{'id': 1}, {'id': 2}]
        result = process_response(data, all_data)
        self.assertTrue(result)
        self.assertEqual(len(all_data), 2)

    @patch('utils.make_api_request')
    @patch('utils.process_response')
    def test_fetch_all_data(self, mock_process_response, mock_make_api_request):
        # Set up the mock to return different results on successive calls
        mock_make_api_request.side_effect = [
            [{'id': 1}, {'id': 2}],  # First call
            [{'id': 3}, {'id': 4}],  # Second call
            [{'id': 5}],             # Third call
            []                       # Fourth call - empty list to end the loop
        ]
        
        # Mock process_response to actually add data to all_data
        def mock_process(data, all_data):
            all_data.extend(data)
            return len(data) > 0

        mock_process_response.side_effect = mock_process
        
        result = fetch_all_data('2023-01-01', '2023-01-31', 50000, 'http://test.com')
        
        self.assertEqual(len(result), 5)
        self.assertEqual(mock_make_api_request.call_count, 4)
        self.assertEqual(mock_process_response.call_count, 4)

    def test_calculate_trip_length(self):
        df = pd.DataFrame({
            'tpep_pickup_datetime': ['2023-01-01 12:00:00', '2023-01-01 13:00:00'],
            'tpep_dropoff_datetime': ['2023-01-01 12:30:00', '2023-01-01 14:00:00']
        })
        result = calculate_trip_length(df)
        expected = pd.DataFrame({
            'tpep_pickup_datetime': pd.to_datetime(['2023-01-01 12:00:00', '2023-01-01 13:00:00']),
            'tpep_dropoff_datetime': pd.to_datetime(['2023-01-01 12:30:00', '2023-01-01 14:00:00']),
            'trip_length': [30.0, 60.0]
        })
        assert_frame_equal(result, expected)
    
    def test_aggregate_daily_trips(self):
        df = pd.DataFrame({
            'tpep_pickup_datetime': pd.to_datetime(['2023-01-01 12:00:00', '2023-01-01 13:00:00', '2023-01-02 12:00:00']),
            'trip_length': [30.0, 60.0, 45.0]
        })
        result = aggregate_daily_trips(df)
        expected = pd.DataFrame({
            'tpep_pickup_datetime': pd.to_datetime(['2023-01-01', '2023-01-02']),
            'daily_trip_time (in hours)': [1.5, 0.8]
        })
        assert_frame_equal(result, expected)
    
    def test_calculate_rolling_average(self):
        df = pd.DataFrame({
            'tpep_pickup_datetime': pd.date_range(start='2023-01-01', periods=5),
            'daily_trip_time (in hours)': [1.0, 2.0, 3.0, 4.0, 5.0]
        })
        result = calculate_rolling_average(df, 3)
        expected = pd.DataFrame({
            'tpep_pickup_datetime': pd.date_range(start='2023-01-01', periods=5),
            'daily_trip_time (in hours)': [1.0, 2.0, 3.0, 4.0, 5.0],
            'rolling_average': [1.0, 1.5, 2.0, 3.0, 4.0]
        })
        assert_frame_equal(result, expected)

    # The test function below was revised.
    # Since the test data (df) contained only trips from a single day, the rolling average was identical to the daily trip time.
    # That didn't actually test the "rolling" aspect of the average calculation. Therefore, test data was expanded to cover multiple
    # days and it was ensured that the rolling average calculation is working as expected over these multiple days.
    def test_process_taxi_data(self):
        df = pd.DataFrame({
            'tpep_pickup_datetime': pd.to_datetime([
                '2023-01-01 12:00:00', '2023-01-01 13:00:00',
                '2023-01-02 12:00:00', '2023-01-02 14:00:00',
                '2023-01-03 10:00:00', '2023-01-03 11:00:00',
                '2023-01-04 09:00:00', '2023-01-04 10:30:00',
                '2023-01-05 11:00:00', '2023-01-05 13:00:00'
                ]),
            'tpep_dropoff_datetime': pd.to_datetime([
                '2023-01-01 12:30:00', '2023-01-01 14:00:00',
                '2023-01-02 13:00:00', '2023-01-02 16:00:00',
                '2023-01-03 11:00:00', '2023-01-03 13:00:00',
                '2023-01-04 10:00:00', '2023-01-04 12:00:00',
                '2023-01-05 12:00:00', '2023-01-05 15:00:00'
            ])
        })

        # Test mean calculation (for choice-1. The choice structure should be eliminated though.)
        _, _, result_mean = process_taxi_data(df, 1)
        self.assertAlmostEqual(result_mean, 2.6) # Mean of daily trip times: (1.5 + 3.0 + 3.0 + 2.5 + 3.0) / 5 = 2.6

        # Test rolling average calculation (choice-2. The choice structure should be eliminated though.)
        _, daily_summary, result_rolling = process_taxi_data(df, 2)
    
        # The rolling average is calculated with a window of 45 days as per the function
        # However, since we only have 5 days of data, all values will be the same as the cumulative average up to that day
        expected_rolling = pd.DataFrame({
            'tpep_pickup_datetime': pd.to_datetime(['2023-01-01', '2023-01-02', '2023-01-03', '2023-01-04', '2023-01-05']),
            'daily_trip_time (in hours)': [1.5, 3.0, 3.0, 2.5, 3.0],
            'rolling_average': [1.5, 2.2, 2.5, 2.5, 2.6] # rolling averages are intentionally rounded to 1 decimal place 
            # to match the original function output requirements
        })

        pd.testing.assert_frame_equal(result_rolling, expected_rolling)

        # Verify daily_summary
        expected_daily = pd.DataFrame({
            'tpep_pickup_datetime': pd.to_datetime(['2023-01-01', '2023-01-02', '2023-01-03', '2023-01-04', '2023-01-05']),
            'daily_trip_time (in hours)': [1.5, 3.0, 3.0, 2.5, 3.0]
        })
        pd.testing.assert_frame_equal(daily_summary[expected_daily.columns], expected_daily)

if __name__ == '__main__':
    unittest.main()
