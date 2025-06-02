from unittest import mock
import tap_linkedin_ads.client as _client
import tap_linkedin_ads
import unittest
import requests
from datetime import datetime, timedelta
import calendar

@mock.patch("tap_linkedin_ads.client.LinkedinClient.write_access_token_to_config")
@mock.patch("requests.Session.post")
class TestLinkedInClient(unittest.TestCase):

    # def test_access_token_empty_expires(self, mocked_post, mock_write_token):
    #     '''
    #     Ensure that we retrieve and set expires for client with no self.__expires
    #     '''
    #     client = _client.LinkedinClient('client_id', 'client_secret', 'refresh_token', 'access_token', 'config_path')

    #     future_time = int(datetime.utcnow().timestamp()) + 88400
    #     mocked_response = mock.Mock()
    #     mocked_response.json.return_value = {
    #         "expires_at": future_time,
    #         "created_at": 1716560216
    #     }
        
    #     mocked_response.status_code = 200
    #     mocked_post.return_value = mocked_response

    #     expires = client.get_expires_time_for_test()
    #     assert expires is None

    #     client.fetch_and_set_access_token()
    #     expires = client.get_expires_time_for_test()

    #     self.assertEqual(expires, datetime.fromtimestamp(future_time))

    # def test_access_token_expires_valid(self, mocked_post, mock_write_token):
    #     '''
    #     Ensure that we check and return on valid self.__expires
    #     '''
    #     client = _client.LinkedinClient('client_id', 'client_secret', 'refresh_token', 'access_token', 'config_path')

    #     future_time = int(datetime.utcnow().timestamp()) + 88400
    #     mocked_response = mock.MagicMock()
    #     mocked_response.status_code = 200
    #     mocked_response.json.return_value = {
    #         "access_token": "abcdef12345",
    #         "expires_at": future_time,
    #         "created_at": 1716560216
    #     }
    #     mocked_post.return_value = mocked_response

    #     client.set_mock_expires_for_test(datetime.fromtimestamp(future_time))
    #     client.fetch_and_set_access_token()
    #     expires = client.get_expires_time_for_test()
    #     self.assertEqual(expires, datetime.fromtimestamp(future_time))


    # def test_access_token_expires_invalid(self, mocked_post, mock_write_token):
    #     '''
    #     Ensure that we check self.__expires and retrieve new access token if it has expired
    #     '''
    #     client = _client.LinkedinClient('client_id', 'client_secret', 'refresh_token', 'access_token', 'config_path')

    #     old_time = int(datetime.utcnow().timestamp()) - 100
    #     mocked_response = mock.MagicMock()
    #     mocked_response.status_code = 200
    #     mocked_response.json.return_value = {
    #         "access_token": "abcdef12345",
    #         "expires_in": 5184000,
    #         "created_at": 1716560216
    #     }
    #     mocked_post.return_value = mocked_response

    #     client.set_mock_expires_for_test(datetime.fromtimestamp(old_time))
    #     client.fetch_and_set_access_token()
    #     new_expires = client.get_expires_time_for_test()
    #     self.assertGreater(new_expires, datetime.fromtimestamp(old_time))

    # def test_no_access_token(self, mocked_post, mock_write_token):
    #     '''
    #     Ensure that we get an access token if we don't already have one
    #     '''
    #     client = _client.LinkedinClient('client_id', 'client_secret', 'refresh_token', 'config_path', None)

    #     expires = client.get_expires_time_for_test()
    #     assert expires is None

    #     old_time = int(datetime.utcnow().timestamp()) - 100
    #     mocked_token_check_response = mock.Mock()
    #     mocked_token_check_response.json.return_value = {
    #         "access_token": "abcdef12345",
    #         "expires_at": old_time,
    #         "created_at": 1716560216
    #     }
    #     mocked_token_check_response.status_code = 200

    #     mocked_refresh_token_response = mock.Mock()
    #     mocked_refresh_token_response.json.return_value = {
    #         "access_token": "abcdef12345",
    #         "expires_in": 5184000,
    #         "created_at": 1716560216
    #     }
    #     mocked_refresh_token_response.status_code = 200
    #     mocked_post.side_effect = [mocked_token_check_response, mocked_refresh_token_response]

    #     client.fetch_and_set_access_token()
    #     expires = client.get_expires_time_for_test()
    #     self.assertGreater(expires, datetime.fromtimestamp(old_time))

    # def test_no_refresh_token(self, mocked_post, mock_write_token):
    #     '''
    #     Ensure that we use the existing access token if we don't have a refresh token
    #     '''
    #     expected_access_token = 'access_token'
    #     client = _client.LinkedinClient(None, None, None, 'access_token', 'config_path')

    #     client.fetch_and_set_access_token()
    #     actual = client.access_token
    #     self.assertEqual(expected_access_token, actual)

    @mock.patch("tap_linkedin_ads.client.LOGGER")
    def test_refresh_token_expires_within_month(self, mock_logger, mocked_post, mock_write_token):
        '''
        Ensure that we log a warning if the refresh token expires within a month
        '''
        client = _client.LinkedinClient('client_id', 'client_secret', 'refresh_token', 'access_token', 'config_path')

        mocked_response = mock.Mock()
        mocked_response.json.return_value = {
            "expires_at": datetime.utcnow().timestamp(),
            "created_at": (datetime.utcnow() - timedelta(days=334)).timestamp()
        }
        
        mocked_response.status_code = 200
        mocked_post.return_value = mocked_response

        client.get_token_expires()
        mock_logger.warning.assert_called_with(
            "The refresh token is going to expire soon. Please re-authenticate your connection to generate a new token and resume extraction."
        )

    @mock.patch("tap_linkedin_ads.client.LOGGER")
    def test_refresh_token_empty_expires_after_2_month(self, mock_logger, mocked_post, mock_write_token):
        '''
        Ensure that we do not log a warning if the refresh token expires after 2 months
        '''
        client = _client.LinkedinClient('client_id', 'client_secret', 'refresh_token', 'access_token', 'config_path')

        mocked_response = mock.Mock()
        mocked_response.json.return_value = {
            "expires_at": datetime.utcnow().timestamp(),
            "created_at": (datetime.utcnow() - timedelta(days=304)).timestamp()
        }

        mocked_response.status_code = 200
        mocked_post.return_value = mocked_response

        client.get_token_expires()
        self.assertEqual(mock_logger.warning.call_count, 0)
