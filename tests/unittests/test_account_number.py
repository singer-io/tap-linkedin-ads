from logging import NullHandler
from unittest import mock
import tap_linkedin_ads.client as _client
import tap_linkedin_ads
import unittest
import requests

def mocked_discover():
    class Catalog():
        def __init__(self):
            pass
        def to_dict(self):
            return {"streams":[]}

    return Catalog()

class Mockresponse:
    def __init__(self, status_code, json, raise_error, headers=None):
        self.status_code = status_code
        self.raise_error = raise_error
        self.text = json
        self.headers = headers

    def raise_for_status(self):
        if not self.raise_error:
            return self.status_code

        raise requests.HTTPError("Sample message")

    def json(self):
        return self.text

def get_response(status_code, json={}, raise_error=False):
    return Mockresponse(status_code, json, raise_error)

@mock.patch("tap_linkedin_ads.discover", side_effect=mocked_discover)
@mock.patch("tap_linkedin_ads.client.LinkedinClient.check_accounts")
class TestAccountNumber(unittest.TestCase):
    '''
        Verify provided account numbers are valid numbers or not.
    '''

    def test_valid_account_numbers(self, mocked_client, mocked_discover):
        '''
        If account number is valid then discover will be called
        '''
        config = {"accounts": "503491473, 503498742"}
        client = _client.LinkedinClient('')
        tap_linkedin_ads.do_discover(client, config)
        self.assertEqual(mocked_discover.call_count, 1)
        self.assertEqual(mocked_client.call_count, 1)

    def test_invalid_account_numbers(self, mocked_client, mocked_discover):
        '''
        If account number is invalid then ValueError is raised.
        '''
        config = {"accounts": "503491473, sSsQS503498742"}
        client = _client.LinkedinClient('')
        try:
            tap_linkedin_ads.do_discover(client, config)
        except ValueError as e:
            self.assertEqual(str(e), "The account '{}' provided in the configuration is having non-numeric value.".format("sSsQS503498742"))

        self.assertEqual(mocked_discover.call_count, 0)
        self.assertEqual(mocked_client.call_count, 0)

    def test_empty_account_numbers(self, mocked_client, mocked_discover):
        '''
        If account number is invalid then ValueError is raised.
        '''
        config = {"accounts": ''}
        client = _client.LinkedinClient('')
        tap_linkedin_ads.do_discover(client, config)
        self.assertEqual(mocked_discover.call_count, 1)
        self.assertEqual(mocked_client.call_count, 1)

@mock.patch("tap_linkedin_ads.discover", side_effect=mocked_discover)
@mock.patch("requests.Session.get")
class TestValidLinkedInAccount(unittest.TestCase):
    '''
        Verify provided account numbers are valid account number or not as per LinkedInAds
    '''

    def test_valid_linkedIn_accounts(self, mocked_request, mocked_discover):
        '''
        If account number is valid then discover will be called
        '''
        mocked_request.return_value = get_response(200, raise_error = True)
        config = {"accounts": "1111, 2222"}
        client = _client.LinkedinClient('')
        tap_linkedin_ads.do_discover(client, config)
        self.assertEquals(mocked_discover.call_count, 1)

    def test_invalid_linkedIn_accounts(self, mocked_request, mocked_discover):
        '''
        If account number is valid then discover will be called
        '''
        mocked_request.return_value = get_response(404, raise_error = True)
        config = {"accounts": "1111, 2222"}
        client = _client.LinkedinClient('')
        try:
            tap_linkedin_ads.do_discover(client, config)
        except Exception as e:
            expected_invalid_accounts = ["1111", "2222"]
            self.assertEqual(str(e), "These account are invalid LinkedIn Ads accounts from provided accounts:{}".format(expected_invalid_accounts))

        self.assertEqual(mocked_discover.call_count, 0)
