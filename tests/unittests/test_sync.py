import datetime
import unittest
from tap_linkedin_ads.sync import get_next_url
from tap_linkedin_ads.sync import shift_sync_window
from tap_linkedin_ads.sync import split_into_chunks


class TestSyncUtils(unittest.TestCase):
    def test_split_into_chunks(self):
        MAX_CHUNK_LENGTH = 17
        fields = list(range(65))

        actual = split_into_chunks(fields, MAX_CHUNK_LENGTH)

        expected = [
            [ 0,  1,  2,  3,  4,  5,  6,  7,  8,  9, 10, 11, 12, 13, 14, 15, 16],
            [17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33],
            [34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50],
            [51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64]
        ]

        self.assertEqual(expected, list(actual))

    def test_get_next_url(self):
        data = {
            'paging': {
                'links': []
            }
        }

        links = [{'rel': 'next', 'href': '/foo'},]

        expected_1 = None
        actual_1 = get_next_url(data)

        self.assertEqual(expected_1, actual_1)

        data['paging']['links'] = links
        expected_2 = 'https://api.linkedin.com/foo'
        actual_2 = get_next_url(data)

        self.assertEqual(expected_2, actual_2)

    def test_shift_sync_window_non_boundary(self):
        expected_start_date = datetime.date(year=2020, month=10, day=1)
        expected_end_date = datetime.date(year=2020, month=10, day=31)
        expected_params = {
            'dateRange.start.year': expected_start_date.year,
            'dateRange.start.month': expected_start_date.month,
            'dateRange.start.day': expected_start_date.day,
            'dateRange.end.year': expected_end_date.year,
            'dateRange.end.month': expected_end_date.month,
            'dateRange.end.day': expected_end_date.day,
        }

        params = {
            'dateRange.end.year': 2020,
            'dateRange.end.month': 10,
            'dateRange.end.day': 1,
        }
        today = datetime.date(year=2020, month=11, day=10)

        actual_start_date, actual_end_date, actual_params = shift_sync_window(params, today)

        self.assertEqual(expected_start_date, actual_start_date)
        self.assertEqual(expected_end_date, actual_end_date)
        self.assertEqual(expected_params, actual_params)

    def test_shift_sync_window_boundary(self):
        expected_start_date = datetime.date(year=2020, month=10, day=1)
        expected_end_date = datetime.date(year=2020, month=10, day=15)
        expected_params = {
            'dateRange.start.year': expected_start_date.year,
            'dateRange.start.month': expected_start_date.month,
            'dateRange.start.day': expected_start_date.day,
            'dateRange.end.year': expected_end_date.year,
            'dateRange.end.month': expected_end_date.month,
            'dateRange.end.day': expected_end_date.day,
        }

        params = {
            'dateRange.end.year': 2020,
            'dateRange.end.month': 10,
            'dateRange.end.day': 1,
        }
        today = datetime.date(year=2020, month=10, day=15)

        actual_start_date, actual_end_date, actual_params = shift_sync_window(params, today)

        self.assertEqual(expected_start_date, actual_start_date)
        self.assertEqual(expected_end_date, actual_end_date)
        self.assertEqual(expected_params, actual_params)

