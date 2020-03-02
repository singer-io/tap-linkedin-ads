import backoff
import requests

from singer import metrics
import singer

LOGGER = singer.get_logger()


class Server5xxError(Exception):
    pass


class Server429Error(Exception):
    pass


class LinkedInError(Exception):
    pass


class LinkedInBadRequestError(LinkedInError):
    pass


class LinkedInUnauthorizedError(LinkedInError):
    pass


class LinkedInPaymentRequiredError(LinkedInError):
    pass


class LinkedInNotFoundError(LinkedInError):
    pass


class LinkedInConflictError(LinkedInError):
    pass


class LinkedInForbiddenError(LinkedInError):
    pass


class LinkedInInternalServiceError(LinkedInError):
    pass


ERROR_CODE_EXCEPTION_MAPPING = {
    400: LinkedInBadRequestError,
    401: LinkedInUnauthorizedError,
    402: LinkedInPaymentRequiredError,
    403: LinkedInForbiddenError,
    404: LinkedInNotFoundError,
    409: LinkedInForbiddenError,
    500: LinkedInInternalServiceError}


def get_exception_for_error_code(error_code):
    return ERROR_CODE_EXCEPTION_MAPPING.get(error_code, LinkedInError)

def raise_for_error(response):
    try:
        response.raise_for_status()
    except (requests.HTTPError, requests.ConnectionError) as error:
        try:
            content_length = len(response.content)
            if content_length == 0:
                # There is nothing we can do here since LinkedIn has neither sent
                # us a 2xx response nor a response content.
                return
            response = response.json()
            if ('error' in response) or ('errorCode' in response):
                message = '%s: %s' % (response.get('error', str(error)),
                                      response.get('message', 'Unknown Error'))
                error_code = response.get('status')
                ex = get_exception_for_error_code(error_code)
                if response.status_code == 401 and 'Expired access token' in message:
                    LOGGER.error("Your access_token has expired as per LinkedIn’s security \
                        policy. \n Please re-authenticate your connection to generate a new token \
                        and resume extraction.")
                raise ex(message)
            else:
                raise LinkedInError(error)
        except (ValueError, TypeError):
            raise LinkedInError(error)


class LinkedinClient:
    def __init__(
        self,
        client_id,
        client_secret,
        refresh_token,
        user_agent=None
    ):
        self.__client_id = client_id
        self.__client_secret = client_secret
        self.__refresh_token = refresh_token
        self.__user_agent = user_agent
        self.__session = requests.Session()
        self.__base_url = None

    def __enter__(self):
        self.__access_token = self.get_access_token()
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        self.__session.close()

    @backoff.on_exception(backoff.expo,
                          Server5xxError,
                          max_tries=5,
                          factor=2)
    def get_access_token(self):
        """ Get a fresh access token using the refresh token provided in the config file
        """
        url = 'https://www.linkedin.com/oauth/v2/accessToken'
        response = self.__session.post(url, data={
            'grant_type': 'refresh_token',
            'client_id': self.__client_id,
            'client_secret': self.__client_secret,
            'refresh_token': self.__refresh_token,
        })
        if response.status_code != 200:
            LOGGER.error('Error status_code = %s', response.status_code)
            raise_for_error(response)
        return response.json()['access_token']

    @backoff.on_exception(backoff.expo,
                          (Server5xxError, requests.exceptions.ConnectionError, Server429Error),
                          max_tries=5,
                          factor=2)
    def request(self, method, url=None, path=None, **kwargs):
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
            response = self.__session.request(method, url, **kwargs)
            timer.tags[metrics.Tag.http_status_code] = response.status_code

        if response.status_code >= 500:
            raise Server5xxError()

        if response.status_code != 200:
            raise_for_error(response)

        return response.json()

    def get(self, url=None, path=None, **kwargs):
        return self.request('GET', url=url, path=path, **kwargs)

    def post(self, url=None, path=None, **kwargs):
        return self.request('POST', url=url, path=path, **kwargs)
