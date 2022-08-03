from unittest import mock
import tap_linkedin_ads.client as client
import unittest
import requests
import time
import json
from parameterized import parameterized

def get_response(status_code, json_resp={}):
    """
    Returns mock response
    """
    response = requests.Response()
    response.status_code = status_code
    response._content = json.dumps(json_resp).encode()
    return response

def get_400_response(*args, **kwargs):
    """Return response for 400 error code."""
    json_resp = {"message": "Invalid params for account.",
                "status": 400,
                "code": "BAD_REQUEST"}
    return get_response(400, json_resp)

def get_401_response(*args, **kwargs):
    """Return response for 401 error code."""
    json_resp = {"message": "The authorization has expired, please re-authorize.",
                "status": 401,
                "code": "UNAUTHORIZED"}
    return get_response(401, json_resp)

def get_403_response(*args, **kwargs):
    """Return response for 403 error code."""
    json_resp = {"message": "You do not have permission to access this resource.",
                "status": 403,
                "code": "FORBIDDEN"}
    return get_response(403, json_resp)

def get_404_response(*args, **kwargs):
    """Return response for 404 error code."""
    json_resp = {"message": "Not Found.",
                "status": 404,
                "code": "NOT_FOUND"}
    return get_response(404, json_resp)

def get_405_response(*args, **kwargs):
    """Return response for 405 error code."""
    json_resp = {"message": "The URL doesn't support this HTTP method.",
                "status": 405,
                "code": "METHOD_NOT_ALLOWED"}
    return get_response(405, json_resp)

def get_411_response(*args, **kwargs):
    """Return response for 411 error code."""
    json_resp = {"message": "Please add a defined Content-Length header.",
                "status": 411,
                "code": "LENGTH_REQUIRED"}
    return get_response(411, json_resp)

def get_429_response(*args, **kwargs):
    """Return response for 429 error code."""
    json_resp = {"message": "APT ratelimit exceeded, retry after some time.",
                "status": 429,
                "code": "RATELIMIT_EXCEEDED"}
    return get_response(429, json_resp)

def get_500_response(*args, **kwargs):
    """Return response for 500 error code."""
    json_resp = {"message": "Internal error, please retry after some time.",
                "status": 500,
                "code": "INTERNAL_ERROR"}
    return get_response(500, json_resp)

def get_504_response(*args, **kwargs):
    """Return response for 504 error code."""
    json_resp = {"message": "Gateway timed out, please retry after some time.",
                "status": 504,
                "code": "GATEWAY_TIMEOUT"}
    return get_response(504, json_resp)

def get_503_response(*args, **kwargs):
    """Return response for 503 unknown error code."""
    return get_response(503)


class TestBackoffHandling(unittest.TestCase):
    """
    Test that functions are backing off expected times/till expected time.
    """

    @parameterized.expand([
        (get_500_response, client.LinkedInInternalServiceError),
        (get_504_response, client.LinkedInGatewayTimeoutError),
        (get_503_response, client.Server5xxError),
        (requests.exceptions.Timeout, requests.exceptions.Timeout),
        (requests.exceptions.ConnectionError, requests.exceptions.ConnectionError),
    ])
    @mock.patch("time.sleep")
    @mock.patch("requests.Session.post")
    def test_fetch_and_set_token_backoff(self, mock_response, error, mock_requests, mock_sleep):
        """
        Test `fetch_and_set_access_token` will backoff 5 times for 5xx, Timeout, ConnectionError.
        """
        mock_requests.side_effect = mock_response
        with self.assertRaises(error) as e:
            with client.LinkedinClient("","","refresh_token","") as _client:
                pass

        # Verify that `session.post` was called 5 times
        self.assertEqual(mock_requests.call_count, 5)

    @parameterized.expand([
        (get_500_response, client.LinkedInInternalServiceError),
        (get_504_response, client.LinkedInGatewayTimeoutError),
        (get_503_response, client.Server5xxError),
        (requests.exceptions.Timeout, requests.exceptions.Timeout),
        (requests.exceptions.ConnectionError, requests.exceptions.ConnectionError),
    ])
    @mock.patch("time.sleep")
    @mock.patch("requests.Session.get")
    def test_check_accounts_backoff(self, mock_response, error, mock_requests, mock_sleep):
        """
        Test `check_accounts` will backoff 5 times for 5xx, Timeout, ConnectionError.
        """
        config = {"accounts": "acc1"}
        mock_requests.side_effect = mock_response
        linkedIn_client = client.LinkedinClient('client_id', 'client_secret', 'refresh_token', 'access_token')
        with self.assertRaises(error) as e:
            linkedIn_client.check_accounts(config)

        # Verify that `session.get` was called 5 times
        self.assertEqual(mock_requests.call_count, 5)

    @parameterized.expand([
        (get_500_response, client.LinkedInInternalServiceError),
        (get_504_response, client.LinkedInGatewayTimeoutError),
        (get_503_response, client.Server5xxError),
        (get_429_response, client.Server429Error),
        (requests.exceptions.ConnectionError, requests.exceptions.ConnectionError),
    ])
    @mock.patch("requests.Session.request")
    def test_requests_backoff(self, mock_response, error, mock_requests):
        """
        Test `request` method will backoff for 600 seconds for 5xx, 429 and ConnectionError.
        """
        mock_requests.side_effect = mock_response
        linkedIn_client = client.LinkedinClient('client_id', 'client_secret', 'refresh_token', 'access_token')
        start_time = time.time()
        with self.assertRaises(error) as e:
            linkedIn_client.request("GET")
        end_time = time.time()

        # Verify that `session.request` was called till 600 seconds
        self.assertGreaterEqual(end_time-start_time, 600)

    @mock.patch("time.sleep")
    @mock.patch("requests.Session.request")
    def test_requests_timeout_backoff(self, mock_requests, mock_sleep):
        """
        Test `request` method will backoff 5 times Timeout error.
        """
        mock_requests.side_effect = requests.exceptions.Timeout
        linkedIn_client = client.LinkedinClient('client_id', 'client_secret', 'refresh_token', 'access_token')
        with self.assertRaises(requests.exceptions.Timeout) as e:
            linkedIn_client.request("GET")

        # Verify that `session.request` was called 5 times
        self.assertEqual(mock_requests.call_count, 5)

@mock.patch("requests.Session.request")
@mock.patch("tap_linkedin_ads.client.LinkedinClient.fetch_and_set_access_token")
class TestExceptionHandling(unittest.TestCase):
    """
    Test exception handling for `request` method.
    """

    @parameterized.expand([
        (400, get_response(400), client.LinkedInBadRequestError, "The request is missing or has a bad parameter."),
        (401, get_response(401), client.LinkedInUnauthorizedError, "Invalid authorization credentials."),
        (403, get_response(403), client.LinkedInForbiddenError, "User does not have permission to access the resource."),
        (404, get_response(404), client.LinkedInNotFoundError, "The resource you have specified cannot be found. Either the accounts provided are invalid or you do not have access to the Ad Account."),
        (405, get_response(405), client.LinkedInMethodNotAllowedError, "The provided HTTP method is not supported by the URL."),
        (411, get_response(411), client.LinkedInLengthRequiredError, "The server refuses to accept the request without a defined Content-Length header."),
    ])
    def test_custom_error_message(self, mocked_access_token, mocked_request, error_code, mock_response, error, message):
        """
        Test that exception is raised with the custom error message.
        """
        mocked_request.return_value = mock_response
        expected_message = "HTTP-error-code: {}, Error: {}".format(error_code, message)
        linkedIn_client = client.LinkedinClient('client_id', 'client_secret', 'refresh_token', 'access_token')
        with self.assertRaises(error) as e:
            linkedIn_client.request("GET")

        # Verify that the exception message was expected
        self.assertEquals(str(e.exception), expected_message)

    @parameterized.expand([
        (400, get_400_response(), client.LinkedInBadRequestError,  "Invalid params for account."),
        (401, get_401_response(), client.LinkedInUnauthorizedError, "The authorization has expired, please re-authorize."),
        (403, get_403_response(), client.LinkedInForbiddenError, "You do not have permission to access this resource."),
        (404, get_404_response(), client.LinkedInNotFoundError, "The resource you have specified cannot be found. Either the accounts provided are invalid or you do not have access to the Ad Account."),
        (405, get_405_response(), client.LinkedInMethodNotAllowedError, "The URL doesn't support this HTTP method."),
        (411, get_411_response(), client.LinkedInLengthRequiredError, "Please add a defined Content-Length header."),
    ])
    def test_response_error_message(self, mocked_access_token, mocked_request, error_code, mock_response, error, message):
        """
        Test that exception is raised with the response error message.
        """
        mocked_request.return_value = mock_response
        expected_message = "HTTP-error-code: {}, Error: {}".format(error_code, message)
        linkedIn_client = client.LinkedinClient('client_id', 'client_secret', 'refresh_token', 'access_token')
        with self.assertRaises(error) as e:
            linkedIn_client.request("GET")

        # Verify that the exception message was expected
        self.assertEquals(str(e.exception), expected_message)

    @mock.patch("tap_linkedin_ads.client.LOGGER.error")
    def test_401_error_expired_access_token(self, mocked_logger, mocked_access_token, mocked_request):
        """
        Test for 401 error LOGGER is written with an expected message.
        """
        response_json = {"message": "Expired access token, please re-authenticate.",
                            "status": 401,
                            "code": "UNAUTHORIZED"}
        mocked_request.return_value = get_response(401, response_json)
        linkedIn_client = client.LinkedinClient('client_id', 'client_secret', 'refresh_token', 'access_token')
        with self.assertRaises(client.LinkedInUnauthorizedError) as e:
            linkedIn_client.request("POST")

        # Verify logger called with expected message
        mocked_logger.assert_called_with("Your access_token has expired as per LinkedIn’s security policy. Please re-authenticate your connection to generate a new token and resume extraction.")

        # Verify that the exception message was expected
        self.assertEquals(str(e.exception), "HTTP-error-code: 401, Error: {}".format(response_json.get('message')))

@mock.patch("time.sleep")
@mock.patch("requests.Session.post")
class TestAccessToken(unittest.TestCase):
    """
    Test exception handling for `fetch_and_set_access_token` method.
    """

    @parameterized.expand([
        (400, get_response(400), client.LinkedInBadRequestError, "The request is missing or has a bad parameter."),
        (401, get_response(401), client.LinkedInUnauthorizedError, "Invalid authorization credentials."),
        (403, get_response(403), client.LinkedInForbiddenError, "User does not have permission to access the resource."),
        (404, get_response(404), client.LinkedInNotFoundError, "The resource you have specified cannot be found. Either the accounts provided are invalid or you do not have access to the Ad Account."),
        (404, get_404_response(), client.LinkedInNotFoundError, "The resource you have specified cannot be found. Either the accounts provided are invalid or you do not have access to the Ad Account."),
        (405, get_response(405), client.LinkedInMethodNotAllowedError, "The provided HTTP method is not supported by the URL."),
        (411, get_response(411), client.LinkedInLengthRequiredError, "The server refuses to accept the request without a defined Content-Length header."),
        (429, get_response(429), client.LinkedInRateLimitExceeededError, "API rate limit exceeded, please retry after some time."),
        (500, get_response(500), client.LinkedInInternalServiceError, "An error has occurred at LinkedIn's end."),
        (504, get_response(504), client.LinkedInGatewayTimeoutError, "A gateway timeout occurred. There is a problem at LinkedIn's end."),
        (503, get_503_response(), client.Server5xxError, "Unknown Error"),   # Test case for unknown 5xx error
    ])
    def test_custom_error_message(self, mock_request, mock_sleep, error_code, mock_response, error, message):
        """
        Test that exception is raised with the custom error message.
        """
        mock_request.return_value = mock_response
        expected_message = "HTTP-error-code: {}, Error: {}".format(error_code, message)
        linkedIn_client = client.LinkedinClient('client_id', 'client_secret', 'refresh_token', 'access_token')
        with self.assertRaises(error) as e:
            linkedIn_client.fetch_and_set_access_token()

        # Verify that the exception message was expected
        self.assertEquals(str(e.exception), expected_message)

    @parameterized.expand([
        (400, get_400_response(), client.LinkedInBadRequestError,  "Invalid params for account."),
        (401, get_401_response(), client.LinkedInUnauthorizedError, "The authorization has expired, please re-authorize."),
        (403, get_403_response(), client.LinkedInForbiddenError, "You do not have permission to access this resource."),
        (405, get_405_response(), client.LinkedInMethodNotAllowedError, "The URL doesn't support this HTTP method."),
        (411, get_411_response(), client.LinkedInLengthRequiredError, "Please add a defined Content-Length header."),
        (429, get_429_response(), client.LinkedInRateLimitExceeededError, "APT ratelimit exceeded, retry after some time."),
        (500, get_500_response(), client.LinkedInInternalServiceError, "Internal error, please retry after some time."),
        (504, get_504_response(), client.LinkedInGatewayTimeoutError, "Gateway timed out, please retry after some time."),
    ])
    def test_resopnse_error_message(self, mock_request, mock_sleep, error_code, mock_response, error, message):
        """
        Test that exception is raised with the response error message.
        """
        mock_request.return_value = mock_response
        expected_message = "HTTP-error-code: {}, Error: {}".format(error_code, message)
        linkedIn_client = client.LinkedinClient('client_id', 'client_secret', 'refresh_token', 'access_token')
        with self.assertRaises(error) as e:
            linkedIn_client.fetch_and_set_access_token()

        # Verify that the exception message was expected
        self.assertEquals(str(e.exception), expected_message)

    @mock.patch("tap_linkedin_ads.client.LOGGER.error")
    def test_401_error_expired_access_token(self, mock_logger, mock_request, mock_sleep):
        """
        Test for 401 error LOGGER is written with an expected message.
        """
        response_json = {"message": "Expired access token , please re-authenticate.",
                            "status": 401,
                            "code": "UNAUTHORIZED"}
        mock_request.return_value = get_response(401, response_json)
        linkedIn_client = client.LinkedinClient('client_id', 'client_secret', 'refresh_token', 'access_token')
        with self.assertRaises(client.LinkedInUnauthorizedError) as e:
            linkedIn_client.fetch_and_set_access_token()

        # Verify logger called with expected message
        mock_logger.assert_called_with("Your access_token has expired as per LinkedIn’s security policy. Please re-authenticate your connection to generate a new token and resume extraction.")

        # Verify that the exception message was expected
        self.assertEquals(str(e.exception), "HTTP-error-code: 401, Error: {}".format(response_json.get('message')))
