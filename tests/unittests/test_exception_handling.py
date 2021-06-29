
from unittest import mock
import tap_linkedin_ads.client as client
import unittest
import requests

class Mockresponse:
    def __init__(self, status_code, json, raise_error, text=None, content=None):
        self.status_code = status_code
        self.raise_error = raise_error
        self.text = json
        self.content = content if content is not None else "linkedIn ads"

    def raise_for_status(self):
        if not self.raise_error:
            return self.status_code

        raise requests.HTTPError("Sample message")

    def json(self):
        return self.text

def get_response(status_code, json={}, raise_error=False, content=None):
    return Mockresponse(status_code, json, raise_error, content=content)

@mock.patch("requests.Session.request")
@mock.patch("tap_linkedin_ads.client.LinkedinClient.check_access_token")
class TestExceptionHandling(unittest.TestCase):

    def test_400_error_custom_message(self, mocked_access_token, mocked_request):
        mocked_request.return_value = get_response(400, raise_error = True)
        linkedIn_client = client.LinkedinClient("", "")
        try:
            linkedIn_client.request("/abc")
        except client.LinkedInBadRequestError as e:
            self.assertEquals(str(e), "HTTP-error-code: 400, Error: The request is missing or has bad parameters.")

    def test_401_error_custom_message(self, mocked_access_token, mocked_request):
        mocked_request.return_value = get_response(401, raise_error = True)
        linkedIn_client = client.LinkedinClient("", "")
        try:
            linkedIn_client.request("/abc")
        except client.LinkedInUnauthorizedError as e:
            self.assertEquals(str(e), "HTTP-error-code: 401, Error: Invalid authorization credentials.")

    def test_402_error_custom_message(self, mocked_access_token, mocked_request):
        mocked_request.return_value = get_response(402, raise_error = True)
        linkedIn_client = client.LinkedinClient("", "")
        try:
            linkedIn_client.request("/abc")
        except client.LinkedInPaymentRequiredError as e:
            self.assertEquals(str(e), "HTTP-error-code: 402, Error: Payment is required to complete the operation.")

    def test_403_error_custom_message(self, mocked_access_token, mocked_request):
        mocked_request.return_value = get_response(403, raise_error = True)
        linkedIn_client = client.LinkedinClient("", "")
        try:
            linkedIn_client.request("/abc")
        except client.LinkedInForbiddenError as e:
            self.assertEquals(str(e), "HTTP-error-code: 403, Error: User does not have permission to access the resource.")

    def test_404_error_custom_message(self, mocked_access_token, mocked_request):
        mocked_request.return_value = get_response(404, raise_error = True)
        linkedIn_client = client.LinkedinClient("", "")
        try:
            linkedIn_client.request("/abc")
        except client.LinkedInNotFoundError as e:
            self.assertEquals(str(e), "HTTP-error-code: 404, Error: The requested resource does not exist.")

    def test_409_error_custom_message(self, mocked_access_token, mocked_request):
        mocked_request.return_value = get_response(409, raise_error = True)
        linkedIn_client = client.LinkedinClient("", "")
        try:
            linkedIn_client.request("/abc")
        except client.LinkedInConflictError as e:
            self.assertEquals(str(e), "HTTP-error-code: 409, Error: The API request cannot be completed because the requested operation would conflict with an existing item.")

    def test_500_error_custom_message(self, mocked_access_token, mocked_request):
        mocked_request.return_value = get_response(406, raise_error = True)
        linkedIn_client = client.LinkedinClient("", "")
        try:
            linkedIn_client.request("/abc")
        except client.LinkedInInternalServiceError as e:
            self.assertEquals(str(e), "HTTP-error-code: 500, Error: The request failed due to an internal error.")

    def test_400_error_response_message(self, mocked_access_token, mocked_request):
        response_json = {"message": "Response message for status code 400"}
        mocked_request.return_value = get_response(400, response_json, raise_error = True)
        linkedIn_client = client.LinkedinClient("", "")
        try:
            linkedIn_client.request("/abc")
        except client.LinkedInBadRequestError as e:
            self.assertEquals(str(e), "HTTP-error-code: 400, Error: {}".format(response_json.get('message')))

    def test_401_error_response_message(self, mocked_access_token, mocked_request):
        response_json = {"message": "Response message for status code 401"}
        mocked_request.return_value = get_response(401, response_json, raise_error = True)
        linkedIn_client = client.LinkedinClient("", "")
        try:
            linkedIn_client.request("/abc")
        except client.LinkedInUnauthorizedError as e:
            self.assertEquals(str(e), "HTTP-error-code: 401, Error: {}".format(response_json.get('message')))
    
    def test_402_error_response_message(self, mocked_access_token, mocked_request):
        response_json = {"message": "Response message for status code 402"}
        mocked_request.return_value = get_response(402, response_json, raise_error = True)
        linkedIn_client = client.LinkedinClient("", "")
        try:
            linkedIn_client.request("/abc")
        except client.LinkedInPaymentRequiredError as e:
            self.assertEquals(str(e), "HTTP-error-code: 402, Error: {}".format(response_json.get('message')))

    def test_403_error_response_message(self, mocked_access_token, mocked_request):
        response_json = {"message": "Response message for status code 403"}
        mocked_request.return_value = get_response(403, response_json, raise_error = True)
        linkedIn_client = client.LinkedinClient("", "")
        try:
            linkedIn_client.request("/abc")
        except client.LinkedInForbiddenError as e:
            self.assertEquals(str(e), "HTTP-error-code: 403, Error: {}".format(response_json.get('message')))

    def test_404_error_response_message(self, mocked_access_token, mocked_request):
        response_json = {"message": "Response message for status code 404"}
        mocked_request.return_value = get_response(404, response_json, raise_error = True)
        linkedIn_client = client.LinkedinClient("", "")
        try:
            linkedIn_client.request("/abc")
        except client.LinkedInNotFoundError as e:
            self.assertEquals(str(e), "HTTP-error-code: 404, Error: {}".format(response_json.get('message')))

    def test_409_error_response_message(self, mocked_access_token, mocked_request):
        response_json = {"message": "Response message for status code 409"}
        mocked_request.return_value = get_response(409, response_json, raise_error = True)
        linkedIn_client = client.LinkedinClient("", "")
        try:
            linkedIn_client.request("/abc")
        except client.LinkedInConflictError as e:
            self.assertEquals(str(e), "HTTP-error-code: 409, Error: {}".format(response_json.get('message')))

    def test_500_error_response_message(self, mocked_access_token, mocked_request):
        response_json = {"message": "Response message for status code 500"}
        mocked_request.return_value = get_response(500, response_json, raise_error = True)
        linkedIn_client = client.LinkedinClient("", "")
        try:
            linkedIn_client.request("/abc")
        except client.LinkedInInternalServiceError as e:
            self.assertEquals(str(e), "HTTP-error-code: 500, Error: {}".format(response_json.get('message')))

    def test_error_with_empty_response(self, mocked_access_token, mocked_request):
        mocked_request.return_value = get_response(400, raise_error = True, content='')
        linkedIn_client = client.LinkedinClient("", "")
        with self.assertRaises(client.LinkedInError):
            linkedIn_client.request("/abc")

    @mock.patch("tap_linkedin_ads.client.LOGGER.error")
    def test_401_error_expired_access_token(self, mocked_logger, mocked_access_token, mocked_request):
        response_json = {"message": "Expired access token"}
        mocked_request.return_value = get_response(401, response_json, raise_error = True)
        linkedIn_client = client.LinkedinClient("", "")
        try:
            linkedIn_client.request("/abc")
        except client.LinkedInUnauthorizedError as e:
            mocked_logger.assert_called_with("Your access_token has expired as per LinkedIn’s security \
            policy. \n Please re-authenticate your connection to generate a new token \
            and resume extraction.")
            self.assertEquals(str(e), "HTTP-error-code: 401, Error: {}".format(response_json.get('message')))

@mock.patch("requests.Session.get")
class TestAccessToken(unittest.TestCase):

    def test_400_error_custom_message(self, mocked_request):
        mocked_request.return_value = get_response(400, raise_error = True)
        linkedIn_client = client.LinkedinClient("", "")
        try:
            linkedIn_client.check_access_token()
        except client.LinkedInBadRequestError as e:
            self.assertEquals(str(e), "HTTP-error-code: 400, Error: The request is missing or has bad parameters.")

    def test_401_error_custom_messsage(self, mocked_request):
        mocked_request.return_value = get_response(401, raise_error = True)
        linkedIn_client = client.LinkedinClient("", "")
        try:
            linkedIn_client.check_access_token()
        except client.LinkedInUnauthorizedError as e:
            self.assertEquals(str(e), "HTTP-error-code: 401, Error: Invalid authorization credentials.")

    def test_402_error_custom_message(self, mocked_request):
        mocked_request.return_value = get_response(402, raise_error = True)
        linkedIn_client = client.LinkedinClient("", "")
        try:
            linkedIn_client.check_access_token()
        except client.LinkedInPaymentRequiredError as e:
            self.assertEquals(str(e), "HTTP-error-code: 402, Error: Payment is required to complete the operation.")

    def test_403_error_custom_message(self, mocked_request):
        mocked_request.return_value = get_response(403, raise_error = True)
        linkedIn_client = client.LinkedinClient("", "")
        try:
            linkedIn_client.check_access_token()
        except client.LinkedInForbiddenError as e:
            self.assertEquals(str(e), "HTTP-error-code: 403, Error: User does not have permission to access the resource.")

    def test_404_error_custom_message(self, mocked_request):
        mocked_request.return_value = get_response(404, raise_error = True)
        linkedIn_client = client.LinkedinClient("", "")
        try:
            linkedIn_client.check_access_token()
        except client.LinkedInNotFoundError as e:
            self.assertEquals(str(e), "HTTP-error-code: 404, Error: The requested resource does not exist.")

    def test_409_error_custom_message(self, mocked_request):
        mocked_request.return_value = get_response(409, raise_error = True)
        linkedIn_client = client.LinkedinClient("", "")
        try:
            linkedIn_client.check_access_token()
        except client.LinkedInConflictError as e:
            self.assertEquals(str(e), "HTTP-error-code: 409, Error: The API request cannot be completed because the requested operation would conflict with an existing item.")

    def test_500_error_custom_message(self, mocked_request):
        mocked_request.return_value = get_response(406, raise_error = True)
        linkedIn_client = client.LinkedinClient("", "")
        try:
            linkedIn_client.check_access_token()
        except client.LinkedInInternalServiceError as e:
            self.assertEquals(str(e), "HTTP-error-code: 500, Error: The request failed due to an internal error.")

    def test_400_error_response_message(self, mocked_request):
        response_json = {"message": "Response message for status code 400"}
        mocked_request.return_value = get_response(400, response_json, raise_error = True)
        linkedIn_client = client.LinkedinClient("", "")
        try:
            linkedIn_client.check_access_token()
        except client.LinkedInBadRequestError as e:
            self.assertEquals(str(e), "HTTP-error-code: 400, Error: {}".format(response_json.get('message')))

    def test_401_error_response_message(self, mocked_request):
        response_json = {"message": "Response message for status code 401"}
        mocked_request.return_value = get_response(401, response_json, raise_error = True)
        linkedIn_client = client.LinkedinClient("", "")
        try:
            linkedIn_client.check_access_token()
        except client.LinkedInUnauthorizedError as e:
            self.assertEquals(str(e), "HTTP-error-code: 401, Error: {}".format(response_json.get('message')))
    
    def test_402_error_response_message(self, mocked_request):
        response_json = {"message": "Response message for status code 402"}
        mocked_request.return_value = get_response(402, response_json, raise_error = True)
        linkedIn_client = client.LinkedinClient("", "")
        try:
            linkedIn_client.check_access_token()
        except client.LinkedInPaymentRequiredError as e:
            self.assertEquals(str(e), "HTTP-error-code: 402, Error: {}".format(response_json.get('message')))

    def test_403_error_response_message(self, mocked_request):
        response_json = {"message": "Response message for status code 403"}
        mocked_request.return_value = get_response(403, response_json, raise_error = True)
        linkedIn_client = client.LinkedinClient("", "")
        try:
            linkedIn_client.check_access_token()
        except client.LinkedInForbiddenError as e:
            self.assertEquals(str(e), "HTTP-error-code: 403, Error: {}".format(response_json.get('message')))

    def test_404_error_response_message(self, mocked_request):
        response_json = {"message": "Response message for status code 404"}
        mocked_request.return_value = get_response(404, response_json, raise_error = True)
        linkedIn_client = client.LinkedinClient("", "")
        try:
            linkedIn_client.check_access_token()
        except client.LinkedInNotFoundError as e:
            self.assertEquals(str(e), "HTTP-error-code: 404, Error: {}".format(response_json.get('message')))

    def test_409_error_response_message(self, mocked_request):
        response_json = {"message": "Response message for status code 409"}
        mocked_request.return_value = get_response(409, response_json, raise_error = True)
        linkedIn_client = client.LinkedinClient("", "")
        try:
            linkedIn_client.check_access_token()
        except client.LinkedInConflictError as e:
            self.assertEquals(str(e), "HTTP-error-code: 409, Error: {}".format(response_json.get('message')))

    def test_500_error_response_message(self, mocked_request):
        response_json = {"message": "Response message for status code 500"}
        mocked_request.return_value = get_response(500, response_json, raise_error = True)
        linkedIn_client = client.LinkedinClient("", "")
        try:
            linkedIn_client.check_access_token()
        except client.LinkedInInternalServiceError as e:
            self.assertEquals(str(e), "HTTP-error-code: 500, Error: {}".format(response_json.get('message')))

    def test_error_with_empty_response(self, mocked_request):
        mocked_request.return_value = get_response(400, raise_error = True, content='')
        linkedIn_client = client.LinkedinClient("", "")
        with self.assertRaises(client.LinkedInError):
            linkedIn_client.check_access_token()
    
    @mock.patch("tap_linkedin_ads.client.LOGGER.error")
    def test_401_error_expired_access_token(self, mocked_logger, mocked_request):
        response_json = {"message": "Expired access token"}
        mocked_request.return_value = get_response(401, response_json, raise_error = True)
        linkedIn_client = client.LinkedinClient("", "")
        try:
            linkedIn_client.check_access_token()
        except client.LinkedInUnauthorizedError as e:
            mocked_logger.assert_called_with("Your access_token has expired as per LinkedIn’s security \
            policy. \n Please re-authenticate your connection to generate a new token \
            and resume extraction.")
            self.assertEquals(str(e), "HTTP-error-code: 401, Error: {}".format(response_json.get('message')))
