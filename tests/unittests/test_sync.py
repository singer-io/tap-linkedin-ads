import unittest
from tap_linkedin_ads.sync import split_into_chunks


class TestChunking(unittest.TestCase):
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
