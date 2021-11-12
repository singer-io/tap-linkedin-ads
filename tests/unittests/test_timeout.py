from unittest import mock
import tap_linkedin_ads.client as client
import unittest
import requests

class TestTimeoutValue(unittest.TestCase):
    """
        Verify the value of timeout is set as expected
    """

    def test_timeout_value_not_passed_in_config(self):
        config = {
            "access_token": "test_access_token",
            "user_agent": "test_user_agent"
        }

        # initialize 'LinkedinClient'
        cl = client.LinkedinClient(access_token=config['access_token'],
                                   user_agent=config['user_agent'],
                                   timeout_from_config=config.get('request_timeout'))

        # verify that timeout value is default as request timeout is not passed in config
        self.assertEquals(300, cl.request_timeout)

    def test_timeout_int_value_passed_in_config(self):
        config = {
            "access_token": "test_access_token",
            "user_agent": "test_user_agent",
            "request_timeout": 100
        }

        # initialize 'LinkedinClient'
        cl = client.LinkedinClient(access_token=config['access_token'],
                                   user_agent=config['user_agent'],
                                   timeout_from_config=config.get('request_timeout'))

        # verify that timeout value is same as the value passed in the config
        self.assertEquals(100.0, cl.request_timeout)

    def test_timeout_string_value_passed_in_config(self):
        config = {
            "access_token": "test_access_token",
            "user_agent": "test_user_agent",
            "request_timeout": "100"
        }

        # initialize 'LinkedinClient'
        cl = client.LinkedinClient(access_token=config['access_token'],
                                   user_agent=config['user_agent'],
                                   timeout_from_config=config.get('request_timeout'))

        # verify that timeout value is same as the value passed in the config
        self.assertEquals(100.0, cl.request_timeout)

    def test_timeout_empty_value_passed_in_config(self):
        config = {
            "access_token": "test_access_token",
            "user_agent": "test_user_agent",
            "request_timeout": ""
        }

        # initialize 'LinkedinClient'
        cl = client.LinkedinClient(access_token=config['access_token'],
                                   user_agent=config['user_agent'],
                                   timeout_from_config=config.get('request_timeout'))

        # verify that timeout value is default as request timeout is empty in the config
        self.assertEquals(300, cl.request_timeout)

    def test_timeout_0_value_passed_in_config(self):
        config = {
            "access_token": "test_access_token",
            "user_agent": "test_user_agent",
            "request_timeout": 0.0
        }

        # initialize 'LinkedinClient'
        cl = client.LinkedinClient(access_token=config['access_token'],
                                   user_agent=config['user_agent'],
                                   timeout_from_config=config.get('request_timeout'))

        # verify that timeout value is default as request timeout is zero in the config
        self.assertEquals(300, cl.request_timeout)

    def test_timeout_string_0_value_passed_in_config(self):
        config = {
            "access_token": "test_access_token",
            "user_agent": "test_user_agent",
            "request_timeout": "0.0"
        }

        # initialize 'LinkedinClient'
        cl = client.LinkedinClient(access_token=config['access_token'],
                                   user_agent=config['user_agent'],
                                   timeout_from_config=config.get('request_timeout'))

        # verify that timeout value is default as request timeout is zero in the config
        self.assertEquals(300, cl.request_timeout)

@mock.patch("time.sleep")
class TestTimeoutBackoff(unittest.TestCase):
    """
        Verify that we backoff for 5 times for the 'Timeout' error
    """

    @mock.patch("requests.Session.get")
    def test_timeout_error__check_access_token(self, mocked_request, mocked_sleep):

        # mock request and raise the 'Timeout' error
        mocked_request.side_effect = requests.Timeout

        config = {
            "access_token": "test_access_token",
            "user_agent": "test_user_agent"
        }

        # initialize 'LinkedinClient'
        cl = client.LinkedinClient(access_token=config['access_token'],
                                   user_agent=config['user_agent'],
                                   timeout_from_config=config.get('request_timeout'))

        try:
            # function call
            cl.check_access_token()
        except requests.Timeout:
            pass

        # verify that we backoff for 5 times
        self.assertEquals(mocked_request.call_count, 5)

    @mock.patch("requests.Session.get")
    def test_timeout_error__check_accounts(self, mocked_request, mocked_sleep):

        # mock request and raise the 'Timeout' error
        mocked_request.side_effect = requests.Timeout

        config = {
            "access_token": "test_access_token",
            "user_agent": "test_user_agent",
            "accounts": "1, 2"
        }

        # initialize 'LinkedinClient'
        cl = client.LinkedinClient(access_token=config['access_token'],
                                   user_agent=config['user_agent'],
                                   timeout_from_config=config.get('request_timeout'))

        try:
            # function call
            cl.check_accounts(config)
        except requests.Timeout:
            pass

        # verify that we backoff for 5 times
        self.assertEquals(mocked_request.call_count, 5)

    @mock.patch("tap_linkedin_ads.client.LinkedinClient.check_access_token")
    @mock.patch("requests.Session.request")
    def test_timeout_error__request(self, mocked_request, mocked_check_access_token, mocked_sleep):

        # mock request and raise the 'Timeout' error
        mocked_request.side_effect = requests.Timeout

        config = {
            "access_token": "test_access_token",
            "user_agent": "test_user_agent",
            "accounts": "1, 2"
        }

        # initialize 'LinkedinClient'
        cl = client.LinkedinClient(access_token=config['access_token'],
                                   user_agent=config['user_agent'],
                                   timeout_from_config=config.get('request_timeout'))

        try:
            # function call
            cl.request('GET')
        except requests.Timeout:
            pass

        # verify that we backoff for 5 times
        self.assertEquals(mocked_request.call_count, 5)
