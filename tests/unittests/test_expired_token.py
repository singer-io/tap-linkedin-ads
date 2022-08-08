import unittest
from unittest import mock
from parameterized import parameterized
from tap_linkedin_ads.client import LinkedinClient


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

        mock_request.return_value.json.return_value = {"expires_at": mock_expires}
        client = LinkedinClient("CLIENT_ID","CLIENT_SECRET","REFRESH_TOKEN","ACCESS_TOKEN")
        retuen_value = client.is_token_expired()

        # Verify returned boolean value is expected
        self.assertEqual(retuen_value, expected_value)
