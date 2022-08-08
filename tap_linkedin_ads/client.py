from datetime import datetime, timedelta
import backoff
import requests
import time

from singer import metrics
import singer

LOGGER = singer.get_logger()
BASE_URL = 'https://api.linkedin.com/rest'
LINKEDIN_TOKEN_URI = 'https://www.linkedin.com/oauth/v2/accessToken'


# set default timeout of 300 seconds
REQUEST_TIMEOUT = 300

class LinkedInError(Exception):
    pass

class Server5xxError(LinkedInError):
    pass


class Server429Error(LinkedInError):
    pass


class LinkedInBadRequestError(LinkedInError):
    pass


class LinkedInUnauthorizedError(LinkedInError):
    pass


class LinkedInMethodNotAllowedError(LinkedInError):
    pass


class LinkedInNotFoundError(LinkedInError):
    pass

class LinkedInForbiddenError(LinkedInError):
    pass

class LinkedInLengthRequiredError(LinkedInError):
    pass

class LinkedInRateLimitExceeededError(Server429Error):
    pass

class LinkedInInternalServiceError(Server5xxError):
    pass

class LinkedInGatewayTimeoutError(Server5xxError):
    pass


ERROR_CODE_EXCEPTION_MAPPING = {
    400: {
        "raise_exception": LinkedInBadRequestError,
        "message": "The request is missing or has a bad parameter."
    },
    401: {
        "raise_exception": LinkedInUnauthorizedError,
        "message": "Invalid authorization credentials."
    },
    403: {
        "raise_exception": LinkedInForbiddenError,
        "message": "User does not have permission to access the resource."
    },
    404: {
        "raise_exception": LinkedInNotFoundError,
        "message": "The resource you have specified cannot be found. Either the accounts provided are invalid or you do not have access to the Ad Account."
    },
    405: {
        "raise_exception": LinkedInMethodNotAllowedError,
        "message": "The provided HTTP method is not supported by the URL."
    },
    411: {
        "raise_exception": LinkedInLengthRequiredError,
        "message": "The server refuses to accept the request without a defined Content-Length header."
    },
    429: {
        "raise_exception": LinkedInRateLimitExceeededError,
        "message": "API rate limit exceeded, please retry after some time."
    },
    500: {
        "raise_exception": LinkedInInternalServiceError,
        "message": "An error has occurred at LinkedIn's end."
    },
    504: {
        "raise_exception": LinkedInGatewayTimeoutError,
        "message": "A gateway timeout occurred. There is a problem at LinkedIn's end."
    }
}

def raise_for_error(response):
    error_code = response.status_code
    try:
        response_json = response.json()
    except Exception:
        response_json = {}

    if error_code == 404:
        # 404 returns "Not Found" so getting custom message
        error_description = ERROR_CODE_EXCEPTION_MAPPING.get(error_code).get("message")
    else:
        # get message from the reponse if present or get custom message if not present
        error_description = response_json.get("errorDetails", response_json.get("message", ERROR_CODE_EXCEPTION_MAPPING.get(error_code, {}).get("message", "Unknown Error")))

    if response.status_code == 401 and 'Expired access token' in error_description:
        LOGGER.error("Your access_token has expired as per LinkedIn’s security policy. Please re-authenticate your connection to generate a new token and resume extraction.")

    message = "HTTP-error-code: {}, Error: {}".format(
                error_code, error_description)

    exc = ERROR_CODE_EXCEPTION_MAPPING.get(error_code, {}).get("raise_exception", LinkedInError)
    raise exc(message) from None

class LinkedinClient: # pylint: disable=too-many-instance-attributes
    def __init__(self, # pylint: disable=too-many-arguments
                 client_id,
                 client_secret,
                 refresh_token,
                 access_token,
                 request_timeout=REQUEST_TIMEOUT,
                 user_agent=None):
        self.__client_id = client_id
        self.__client_secret = client_secret
        self.__refresh_token = refresh_token
        self.__user_agent = user_agent
        self.__access_token = access_token
        self.__expires = None
        self.__session = requests.Session()
        self.__base_url = None
        # if request_timeout is other than 0,"0" or "" then use request_timeout
        if request_timeout and float(request_timeout):
            request_timeout = float(request_timeout)
        else: # If value is 0,"0" or "" then set default to 300 seconds.
            request_timeout = REQUEST_TIMEOUT
        self.request_timeout = request_timeout


    @property
    def access_token(self):
        return self.__access_token

    # during 'Timeout' error there is also possibility of 'ConnectionError',
    # hence added backoff for 'ConnectionError' too.
    # as 'check_access_token' is also called in 'request' hence added backoff here
    # instead of 'check_access_token' to avoid backoff 25 times
    @backoff.on_exception(backoff.expo,
                          (requests.exceptions.ConnectionError, requests.exceptions.Timeout),
                          max_tries=5,
                          factor=2)
    def __enter__(self):
        self.fetch_and_set_access_token()
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        self.__session.close()

    def is_token_expired(self):
        """
        Function to check if the access token is expired.
        """
        current_time = time.time()
        headers = {
            "Content-Type": "application/x-www-form-urlencoded"
        }
        payload='client_id={}&client_secret={}&token={}'.format(self.__client_id,self.__client_secret, self.__access_token)
        response = self.__session.post("https://www.linkedin.com/oauth/v2/introspectToken", headers=headers, data=payload)

        # Substracting 2days(172800 seconds) before expiration date for the possibility of sync runs for long time
        return response.json()['expires_at'] - 172800 < current_time

    @backoff.on_exception(backoff.expo,
                          Server5xxError,
                          max_tries=5,
                          factor=2)
    def fetch_and_set_access_token(self):
        """
        This method generates a new access token if the refresh token is provided.

        Note: Linkedin-ads access token expires in 60 days, whereas the refresh token expires in 365 days.
        """
        # If the refresh token is not provided then we are assuming that it is an old connection
        # and client has provided the valid access_token already
        # Checking if the token is expired, It will be refreshed
        if not self.__refresh_token or not self.is_token_expired():
            return

        headers = {}
        if self.__user_agent:
            headers['User-Agent'] = self.__user_agent

        response = self.__session.post(
            url=LINKEDIN_TOKEN_URI,
            headers=headers,
            data={
                'grant_type': 'refresh_token',
                'client_id': self.__client_id,
                'client_secret': self.__client_secret,
                'refresh_token': self.__refresh_token,
            },
            timeout=self.request_timeout)

        if response.status_code != 200:
            raise_for_error(response)

        data = response.json()
        self.__access_token = data['access_token']
        self.__expires = datetime.utcnow() + timedelta(seconds=data['expires_in'])
        LOGGER.info('Authorized, token expires = %s', format(self.__expires))

        # Waiting 30 seconds after generating a new token
        # as it works after several seconds.
        time.sleep(30)

    # during 'Timeout' error there is also possibility of 'ConnectionError',
    # hence added backoff for 'ConnectionError' too.
    @backoff.on_exception(backoff.expo,
                          (Server5xxError, requests.exceptions.ConnectionError, requests.exceptions.Timeout),
                          max_tries=5,
                          factor=2)
    def check_accounts(self, config):
        headers = {}
        if self.__user_agent:
            headers['User-Agent'] = self.__user_agent
        headers['Authorization'] = 'Bearer {}'.format(self.__access_token)
        headers['Accept'] = 'application/json'
        headers['LinkedIn-Version'] = "202207"

        if config.get('accounts'):
            account_list = config['accounts'].replace(" ", "").split(",")
            invalid_account = []
            for account in account_list:
                response = self.__session.get(
                    url='https://api.linkedin.com/rest/adAccountUsers?q=accounts&count=1&start=0&accounts=urn:li:sponsoredAccount:{}'.format(account),
                    headers=headers,
                    timeout=self.request_timeout)

                # Account users API will return 400 if account is not in number format.
                # Account users API will return 404 if provided account is valid number but invalid LinkedIn Ads account
                if response.status_code in [400, 404]:
                    invalid_account.append(account)
                elif response.status_code != 200:
                    raise_for_error(response)
            if invalid_account:
                error_message = 'Invalid Linked Ads accounts provided during the configuration:{}'.format(invalid_account)
                raise Exception(error_message) from None

    @backoff.on_exception(
        backoff.expo,
        (Server5xxError, requests.exceptions.ConnectionError, Server429Error),
        # Choosing a max time of 10 minutes since documentation for the
        # [ads reporting api](https://docs.microsoft.com/en-us/linkedin/marketing/integrations/ads-reporting/ads-reporting#data-throttling) says
        # "Data limit for all queries over a 5 min interval: 45 million metric values(where metric value is the value for a metric specified in the fields parameter)."
        max_time=600, # seconds
        jitter=backoff.full_jitter,
    )
    # backoff for 'Timeout' error
    @backoff.on_exception(
        backoff.expo,
        requests.exceptions.Timeout,
        max_tries=5,
        factor=2
    )
    def request(self, method, url=None, path=None, **kwargs):

        if not url and self.__base_url is None:
            self.__base_url = 'https://api.linkedin.com/rest'

        if not url and path:
            url = '{}/{}'.format(self.__base_url, path)

        if 'endpoint' in kwargs:
            endpoint = kwargs['endpoint']
            del kwargs['endpoint']
        else:
            endpoint = None

        if 'headers' not in kwargs:
            kwargs['headers'] = {}
        kwargs['headers']['Authorization'] = 'Bearer {}'.format(self.__access_token)
        kwargs['headers']['Accept'] = 'application/json'
        kwargs['headers']['LinkedIn-Version'] = "202207"
        kwargs['headers']['Cache-Control'] = "no-cache"

        if self.__user_agent:
            kwargs['headers']['User-Agent'] = self.__user_agent

        if method == 'POST':
            kwargs['headers']['Content-Type'] = 'application/json'

        with metrics.http_request_timer(endpoint) as timer:
            response = self.__session.request(method, url, timeout=self.request_timeout, **kwargs)
            timer.tags[metrics.Tag.http_status_code] = response.status_code

        if response.status_code != 200:
            raise_for_error(response)
        return response.json()

    def get(self, url=None, path=None, **kwargs):
        return self.request('GET', url=url, path=path, **kwargs)

    def post(self, url=None, path=None, **kwargs):
        return self.request('POST', url=url, path=path, **kwargs)
