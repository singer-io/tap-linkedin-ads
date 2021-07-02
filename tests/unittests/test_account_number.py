from logging import NullHandler
from unittest import mock
import tap_linkedin_ads
import unittest

def mocked_discover():
    class Catalog():
        def __init__(self):
            pass
        def to_dict(self):
            return {"streams":[]}

    return Catalog()

@mock.patch("tap_linkedin_ads.discover", side_effect=mocked_discover)
class TestAccountNumber(unittest.TestCase):

    def test_valid_account_numbers(self, mocked_discover):
        '''
        If account number is valid then discover will be called
        '''
        config = {"accounts": "503491473, 503498742"}
        tap_linkedin_ads.do_discover(config)
        self.assertEqual(mocked_discover.call_count, 1)

    def test_invalid_account_numbers(self, mocked_discover):
        '''
        If account number is invalid then ValueError is raised.
        '''
        config = {"accounts": "503491473, sSsQS503498742"}
        try:
            tap_linkedin_ads.do_discover(config)
        except ValueError as e:
            self.assertEqual(str(e), 'The accounts provided in the configuration are not valid numbers.')

        self.assertEqual(mocked_discover.call_count, 0)

    def test_empty_account_numbers(self, mocked_discover):
        '''
        If account number is invalid then ValueError is raised.
        '''
        config = {"accounts": ''}
        tap_linkedin_ads.do_discover(config)
        self.assertEqual(mocked_discover.call_count, 1)
