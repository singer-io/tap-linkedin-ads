import backoff
import requests

from singer import metrics
import singer

LOGGER = singer.get_logger()

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

class LinkedinClient:
    def __init__(self,
                 access_token,
                 user_agent=None,
                 timeout_from_config=None):
        self.__access_token = access_token
        self.__user_agent = user_agent
        self.__session = requests.Session()
        self.__base_url = None
        self.__verified = False

        # if the 'timeout_from_config' value is 0, "0", "" or not passed then set default value of 300 seconds.
        if timeout_from_config and float(timeout_from_config):
            # update the request timeout for the requests
            self.request_timeout = float(timeout_from_config)
        else:
            # set the default timeout of 300 seconds
            self.request_timeout = REQUEST_TIMEOUT

    # during 'Timeout' error there is also possibility of 'ConnectionError',
    # hence added backoff for 'ConnectionError' too.
    # as 'check_access_token' is also called in 'request' hence added backoff here
    # instead of 'check_access_token' to avoid backoff 25 times
    @backoff.on_exception(backoff.expo,
                          (requests.exceptions.ConnectionError, requests.exceptions.Timeout),
                          max_tries=5,
                          factor=2)
    def __enter__(self):
        self.__verified = self.check_access_token()
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        self.__session.close()

    @backoff.on_exception(backoff.expo,
                          Server5xxError,
                          max_tries=5,
                          factor=2)
    def check_access_token(self): #pylint: disable=inconsistent-return-statements
        if self.__access_token is None:
            raise Exception('Error: Missing access_token.')
        headers = {}
        if self.__user_agent:
            headers['User-Agent'] = self.__user_agent
        headers['Authorization'] = 'Bearer {}'.format(self.__access_token)
        headers['Accept'] = 'application/json'
        response = self.__session.get(
            # Simple endpoint that returns 1 Account record (to check API/token access):
            url='https://api.linkedin.com/v2/adAccountsV2?q=search&start=0&count=1',
            headers=headers,
            timeout=self.request_timeout)
        if response.status_code != 200:
            LOGGER.error('Error status_code = %s', response.status_code)
            raise_for_error(response)
        else:
            resp = response.json()
            if 'elements' in resp: #pylint: disable=simplifiable-if-statement
                return True
            else:
                return False

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

        if config.get('accounts'):
            account_list = config['accounts'].replace(" ", "").split(",")
            invalid_account = []
            for account in account_list:
                response = self.__session.get(
                    url='https://api.linkedin.com/v2/adAccountUsersV2?q=accounts&count=1&start=0&accounts=urn:li:sponsoredAccount:{}'.format(account),
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
        if not self.__verified:
            self.__verified = self.check_access_token()

        if not url and self.__base_url is None:
            self.__base_url = 'https://api.linkedin.com/v2'

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
