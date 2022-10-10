from unittest import mock
import tap_linkedin_ads.client as client
from tap_linkedin_ads.client import REQUEST_TIMEOUT
import unittest
import requests
from parameterized import parameterized


class TestTimeoutValue(unittest.TestCase):
    """
        Verify the value of timeout is set as expected
    """
    
    @parameterized.expand([
        (100, 100.0),
        (100.0, 100.0),
        ("100", 100.0),
        ("100.0", 100.0),
        ("", REQUEST_TIMEOUT),
        (0.0, REQUEST_TIMEOUT),
        ("0.0", REQUEST_TIMEOUT),
    ])
    def test_timeout_values(self, test_value, expected_value):
        """
        Test different values of `request_timeout` such as,
            - Passing integer, float, string-integer, string-float sets the float value of requests timeout
            - Passing null_string, 0, or zero(in string) sets the default request-timeout value
        """
        config = {"client_id": "test_client_id",
                  "client_secret": "test_client_secret",
                  "refresh_token": "test_refresh_token",
                  "access_token": "test_access_token",
                  "user_agent": "test_user_agent",
                  "request_timeout": test_value}

        # initialize 'LinkedinClient'
        cl = client.LinkedinClient(client_id=config['client_id'],
                                   client_secret=config['client_secret'],
                                   refresh_token=config['refresh_token'],
                                   access_token=config['access_token'],
                                   config_path='config_path',
                                   user_agent=config['user_agent'],
                                   request_timeout=config.get('request_timeout'))

        # verify that the timeout value is the same as the expected value
        self.assertEquals(expected_value, cl.request_timeout)

    def test_timeout_value_not_passed_in_config(self):
        """
        Test if no value of request_timeout is passed in the config,
        The default value is set in the client. 
        """
        config = {"client_id": "test_client_id",
                  "client_secret": "test_client_secret",
                  "refresh_token": "test_refresh_token",
                  "access_token": "test_access_token",
                  "user_agent": "test_user_agent"}

        # initialize 'LinkedinClient'
        cl = client.LinkedinClient(client_id=config['client_id'],
                                   client_secret=config['client_secret'],
                                   refresh_token=config['refresh_token'],
                                   access_token=config['access_token'],
                                   config_path='config_path',
                                   user_agent=config['user_agent'],
                                   request_timeout=config.get('request_timeout'))

        # verify that timeout value is default as request timeout is not passed in config
        self.assertEquals(300, cl.request_timeout)


@mock.patch("time.sleep")
@mock.patch("requests.Session.request")
class TestTimeoutBackoff(unittest.TestCase):
    """
        Verify that we backoff for 5 times for the 'Timeout' error
    """
    config = {"client_id": "test_client_id",
              "client_secret": "test_client_secret",
              "refresh_token": "test_refresh_token",
              "access_token": "test_access_token",
              "user_agent": "test_user_agent",
              "accounts": "acc1"}

    # initialize 'LinkedinClient'
    client = client.LinkedinClient(client_id=config['client_id'],
                                   client_secret=config['client_secret'],
                                   refresh_token=config['refresh_token'],
                                   access_token=config['access_token'],
                                   config_path='config_path',
                                   user_agent=config['user_agent'],
                                   request_timeout=config.get('request_timeout'))

    def test_timeout_error__check_access_token(self, mocked_request, mocked_sleep):
        """
        Test for `check_access_token` will backoff 5 times on Timeout.
        """

        # mock request and raise the 'Timeout' error
        mocked_request.side_effect = requests.Timeout

        with self.assertRaises(requests.Timeout):
            # function call
            with client.LinkedinClient(client_id=self.config['client_id'],
                                       client_secret=self.config['client_secret'],
                                       refresh_token=self.config['refresh_token'],
                                       access_token=self.config['access_token'],
                                       config_path='config_path',
                                       user_agent=self.config['user_agent'],
                                       request_timeout=self.config.get('request_timeout')) as cl:
                pass

        # verify that we backoff for 5 times
        self.assertEquals(mocked_request.call_count, 5)

    def test_timeout_error__check_accounts(self, mocked_request, mocked_sleep):
        """
        Test `check_accounts` will backoff 5 times on Timeout. 
        """

        # mock request and raise the 'Timeout' error
        mocked_request.side_effect = requests.Timeout

        with self.assertRaises(requests.Timeout):
            # function call
            self.client.check_accounts(self.config)

        # verify that we backoff for 5 times
        self.assertEquals(mocked_request.call_count, 5)

    def test_timeout_error__request(self, mocked_request, mocked_sleep):
        """
        Test that `requests` will backoff 5 times on Timeout.
        """

        # mock request and raise the 'Timeout' error
        mocked_request.side_effect = requests.Timeout

        with self.assertRaises(requests.Timeout):
            # function call
            self.client.request('GET')

        # verify that we backoff for 5 times
        self.assertEquals(mocked_request.call_count, 5)


@mock.patch("time.sleep")
@mock.patch("requests.Session.request")
class TestConnectionErrorBackoff(unittest.TestCase):
    """
        Verify that we backoff for 5 times for the 'ConnectionError'
    """

    def test_connection_error__check_access_token(self, mocked_request, mocked_sleep):
        """
        Test for `check_access_token` will backoff 5 times on ConnectionError.
        """

        # mock request and raise the 'ConnectionError'
        mocked_request.side_effect = requests.ConnectionError

        config = {"client_id": "test_client_id",
                  "client_secret": "test_client_secret",
                  "refresh_token": "test_refresh_token",
                  "access_token": "test_access_token",
                  "user_agent": "test_user_agent",
                  "user_agent": "test_user_agent"}

        # initialize 'LinkedinClient'
        with self.assertRaises(requests.ConnectionError):
            with client.LinkedinClient(client_id=config['client_id'],
                                       client_secret=config['client_secret'],
                                       refresh_token=config['refresh_token'],
                                       access_token=config['access_token'],
                                        config_path='config_path',
                                       user_agent=config['user_agent'],
                                       request_timeout=config.get('request_timeout')) as cl:
                pass

        # verify that we backoff for 5 times
        self.assertEquals(mocked_request.call_count, 5)

    def test_connection_error__check_accounts(self, mocked_request, mocked_sleep):
        """
        Test for `check_accounts` will backoff 5 times on ConnectionError.
        """

        # mock request and raise the 'ConnectionError'
        mocked_request.side_effect = requests.ConnectionError

        config = {"client_id": "test_client_id",
                  "client_secret": "test_client_secret",
                  "refresh_token": "test_refresh_token",
                  "access_token": "test_access_token",
                  "user_agent": "test_user_agent",
                  "accounts": "1, 2"}

        # initialize 'LinkedinClient'
        cl = client.LinkedinClient(client_id=config['client_id'],
                                   client_secret=config['client_secret'],
                                   refresh_token=config['refresh_token'],
                                   access_token=config['access_token'],
                                   config_path='config_path',
                                   user_agent=config['user_agent'],
                                   request_timeout=config.get('request_timeout'))

        with self.assertRaises(requests.ConnectionError):
            # function call
            cl.check_accounts(config)

        # verify that we backoff for 5 times
        self.assertEquals(mocked_request.call_count, 5)
