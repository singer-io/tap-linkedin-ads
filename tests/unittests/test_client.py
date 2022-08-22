import unittest
import requests
from tap_linkedin_ads.client import LinkedinClient
from unittest import mock
import json

def get_response(status_code, json_resp={}):
    """
    Returns mock response
    """
    response = requests.Response()
    response.status_code = status_code
    response._content = json.dumps(json_resp).encode()
    return response

class TestClient(unittest.TestCase):
    """Test `request` function of client class."""

    @mock.patch('requests.Session.request', return_value=get_response(200))
    @mock.patch('tap_linkedin_ads.client.LinkedinClient.fetch_and_set_access_token')
    def test_request(self, mock_fetch_and_set_access_token, mock_request):
        """
        Test that the `request` method works properly for linkedin-ads API.
        """
        client = LinkedinClient('client_id', 'client_secret', 'refresh_token', 'access_token', user_agent='windows')
        client.request('GET', None, 'accounts', endpoint='https://api.linkedin.com/')

        self.assertEqual(mock_request.call_count, 1)
