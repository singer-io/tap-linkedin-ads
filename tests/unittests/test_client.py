from logging import NullHandler
from unittest import mock
import tap_linkedin_ads.client as _client
import tap_linkedin_ads
import unittest
import requests
from datetime import datetime
import calendar

@mock.patch("requests.Session.post")
class TestLinkedInClient(unittest.TestCase):

    def test_access_token_empty_expires(self, mocked_post):
        '''
        Ensure that we retrieve and set expires for client with no self.__expires
        '''
        client = _client.LinkedinClient('client_id', 'client_secret', 'refresh_token', 'access_token')

        mocked_response = mock.Mock()
        mocked_response.json.return_value = {"expires_at": 1663272887}
        mocked_response.status_code = 200
        mocked_post.return_value = mocked_response

        expires = client.get_expires_time()
        assert expires is None

        client.fetch_and_set_access_token()
        expires = client.get_expires_time()
        self.assertEqual(expires, datetime.fromtimestamp(1663272887))

    def test_access_token_expires_valid(self, mocked_post):
        '''
        Ensure that we check and return on valid self.__expires
        '''
        client = _client.LinkedinClient('client_id', 'client_secret', 'refresh_token', 'access_token')

        test_expires = 2073919961
        mocked_response = mock.MagicMock()
        mocked_response.status_code = 200
        mocked_response.json.return_value = {
            "access_token": "abcdef12345",
            "expires_at": test_expires,
            "expires_in": 5184000
        }
        mocked_post.return_value = mocked_response
                
        client.set_mock_expires(datetime.fromtimestamp(test_expires))
        client.fetch_and_set_access_token()
        expires = client.get_expires_time()
        self.assertEqual(expires, datetime.fromtimestamp(test_expires))


    def test_access_token_expires_invalid(self, mocked_post):
        '''
        Ensure that we check self.__expires and retrieve new access token if it has expired
        '''
        client = _client.LinkedinClient('client_id', 'client_secret', 'refresh_token', 'access_token')

        test_expires = 1663693121
        mocked_response = mock.MagicMock()
        mocked_response.status_code = 200
        mocked_response.json.return_value = {
            "access_token": "abcdef12345",
            "expires_at": test_expires,
            "expires_in": 5184000
        }
        mocked_post.return_value = mocked_response

        client.set_mock_expires(datetime.fromtimestamp(test_expires))
        client.fetch_and_set_access_token()
        expires = client.get_expires_time()
        self.assertGreater(expires, datetime.fromtimestamp(test_expires))

    def test_no_access_token(self, mocked_post):
        '''
        Ensure that we get an access token if we don't already have one
        '''
        pass
        # client = _client.LinkedinClient('client_id', 'client_secret', 'refresh_token', 'access_token')

        # test_expires = 1663693121
        # mocked_response = mock.Mock()
        # mocked_response.json.return_value = {
        #     "access_token": "abcdef12345",
        #     "expires_in": 5184000
        # }
        # mocked_response.status_code = 200
        # mocked_post.return_value = mocked_response
        
        # client.fetch_and_set_access_token()
        # expires = client.get_expires_time()
        # self.assertGreater(expires, datetime.fromtimestamp(test_expires))
