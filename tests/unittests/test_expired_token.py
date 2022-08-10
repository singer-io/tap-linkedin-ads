import unittest
from unittest import mock
from parameterized import parameterized
import  tap_linkedin_ads.client as client
import requests
import json


def get_response(status_code, json_resp={}):
    """
    Returns mock response
    """
    response = requests.Response()
    response.status_code = status_code
    response._content = json.dumps(json_resp).encode()
    return response

@mock.patch("time.time", return_value = 1659355200)
@mock.patch("requests.Session.post")
class TestIsTokenExpire(unittest.TestCase):
    """
    Test `is_token_expired` method of the client in various conditions
    """

    @parameterized.expand([
        (1662033600, False),    # If the token has not expired
        (1646136000, True),     # Expired token
        (1659441600, True),     # About to expire (In 2 days)
    ])
    def test_is_token_expired(self, mock_request, mock_time, mock_expires, expected_value):
        """
        Test method for the given condition:
            - If the token is expired, the method returns true
            - If the token is not expired, the method returns false
            - If the token will expired in 2 days, method returns false
        """

        mock_request.return_value = get_response(200,{"expires_at": mock_expires})
        _client = client.LinkedinClient("CLIENT_ID","CLIENT_SECRET","REFRESH_TOKEN","ACCESS_TOKEN")
        retuen_value = _client.is_token_expired()

        # Verify returned boolean value is expected
        self.assertEqual(retuen_value, expected_value)

    @parameterized.expand([
        (400, client.LinkedInBadRequestError, "The request is missing or has a bad parameter."),
        (401, client.LinkedInUnauthorizedError, "Invalid authorization credentials."),
        (403, client.LinkedInForbiddenError, "User does not have permission to access the resource."),
        (404, client.LinkedInNotFoundError, "The resource you have specified cannot be found. Either the accounts provided are invalid or you do not have access to the Ad Account."),
        (405, client.LinkedInMethodNotAllowedError, "The provided HTTP method is not supported by the URL."),
        (411, client.LinkedInLengthRequiredError, "The server refuses to accept the request without a defined Content-Length header."),
        (500, client.LinkedInInternalServiceError, "An error has occurred at LinkedIn's end."),
        (504, client.LinkedInGatewayTimeoutError, "A gateway timeout occurred. There is a problem at LinkedIn's end."),
    ])
    @mock.patch("tap_linkedin_ads.client.LinkedinClient.fetch_and_set_access_token")
    @mock.patch("time.sleep")
    def test_custom_error_message(self, error_code, error, message, mock_sleep, mocked_access_token, mock_request, mock_time):
        """
        Test that exception is raised with the custom error message.
        """
        mock_request.return_value = get_response(error_code)
        expected_message = "HTTP-error-code: {}, Error: {}".format(error_code, message)
        linkedIn_client = client.LinkedinClient('client_id', 'client_secret', 'refresh_token', 'access_token')
        with self.assertRaises(error) as e:
            linkedIn_client.is_token_expired()

        # Verify that the exception message was expected
        self.assertEquals(str(e.exception), expected_message)

    @parameterized.expand([
        (requests.exceptions.Timeout,),
        (requests.exceptions.ConnectionError,),
    ])
    @mock.patch("tap_linkedin_ads.client.LinkedinClient.fetch_and_set_access_token")
    @mock.patch("time.sleep")
    def test_backoff_on_timeout_and_connection_error(self, error, mock_sleep, mocked_access_token, mock_request, mock_time):
        """
        Test that function back off 5 times for Timeout and connection error.
        """
        mock_request.side_effect = error
        # expected_message = "HTTP-error-code: {}, Error: {}".format(error_code, message)
        linkedIn_client = client.LinkedinClient('client_id', 'client_secret', 'refresh_token', 'access_token')
        with self.assertRaises(error) as e:
            linkedIn_client.is_token_expired()

        # Verify that function backoff expected times
        self.assertEquals(mock_request.call_count, 5)
