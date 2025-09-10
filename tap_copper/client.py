from typing import Any, Dict, Mapping, Optional, Tuple

import backoff
import time
import requests
from requests import session
from requests.exceptions import Timeout, ConnectionError, ChunkedEncodingError
from singer import get_logger, metrics

from tap_copper.exceptions import ERROR_CODE_EXCEPTION_MAPPING, copperError, copperBackoffError

LOGGER = get_logger()
REQUEST_TIMEOUT = 300


def raise_for_error(response: requests.Response) -> None:
    """Raises the associated response exception. Takes in a response object,
    checks the status code, and throws the associated exception based on the
    status code.

    :param resp: requests.Response object
    """
    try:
        response_json = response.json()
    except Exception:  # pylint: disable=broad-exception-caught
        response_json = {}

    if response.status_code not in [200, 201, 204]:
        if response_json.get("error"):
            message = (
                f"HTTP-error-code: {response.status_code}, "
                f"Error: {response_json.get('error')}"
            )
        else:
            error_message = ERROR_CODE_EXCEPTION_MAPPING.get(
                response.status_code, {}
            ).get("message", "Unknown Error")
            message = (
                f"HTTP-error-code: {response.status_code}, "
                f"Error: {response_json.get('message', error_message)}"
            )
        exc = ERROR_CODE_EXCEPTION_MAPPING.get(
            response.status_code, {}
        ).get("raise_exception", copperError)
        raise exc(message, response) from None


def _wait_if_retry_after(details) -> None:
    """Backoff handler: sleep if exception has retry_after attribute."""
    exc = details["exception"]
    if hasattr(exc, "retry_after") and exc.retry_after is not None:
        time.sleep(exc.retry_after)


class Client:
    """
    A Wrapper class.
    ~~~
    Performs:
     - Authentication
     - Response parsing
     - HTTP Error handling and retry
    """

    def __init__(self, config: Mapping[str, Any]) -> None:
        self.config = config
        self._session = session()
        self.base_url = "https://api.copper.com/developer_api/v1/"
        config_request_timeout = config.get("request_timeout")
        self.request_timeout = (
            float(config_request_timeout) if config_request_timeout else REQUEST_TIMEOUT
        )

    def __enter__(self):
        self.check_api_credentials()
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        self._session.close()

    def check_api_credentials(self) -> None:
        """Validate presence of Copper credentials."""
        if not (self.config.get("api_key") or self.config.get("access_token")):
            raise copperError("Missing Copper credential: 'api_key' or 'access_token'.")
        if not (self.config.get("user_email") or self.config.get("email")):
            raise copperError("Missing Copper credential: 'user_email' or 'email'.")

    def authenticate(self, headers: Dict, params: Dict) -> Tuple[Dict, Dict]:
        """Attach Copper authentication headers."""
        headers = dict(headers or {})
        params = dict(params or {})

        api_key = self.config.get("api_key") or self.config.get("access_token")
        user_email = self.config.get("user_email") or self.config.get("email")

        if api_key:
            headers["X-PW-AccessToken"] = api_key
        if user_email:
            headers["X-PW-UserEmail"] = user_email

        headers.setdefault("X-PW-Application", "developer_api")
        headers.setdefault("Accept", "application/json")
        headers.setdefault("Content-Type", "application/json")

        return headers, params

    def make_request(  # pylint: disable=too-many-arguments, too-many-positional-arguments
        self,
        method: str,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, Any]] = None,
        body: Optional[Dict[str, Any]] = None,
        path: Optional[str] = None,
    ) -> Any:
        """
        Sends an HTTP request to the specified API endpoint.
        """
        params = params or {}
        headers = headers or {}
        body = body or {}

        if not endpoint:
            endpoint = f"{self.base_url}/{str(path).lstrip('/')}" if path else self.base_url

        headers, params = self.authenticate(headers, params)

        return self.__make_request(
            method.upper(),
            endpoint,
            headers=headers,
            params=params,
            json=body,
            timeout=self.request_timeout,
        )

    @backoff.on_exception(
        wait_gen=lambda: backoff.expo(factor=2),
        on_backoff=_wait_if_retry_after,
        exception=(
            ConnectionResetError,
            ConnectionError,  # pylint: disable=redefined-builtin
            ChunkedEncodingError,
            Timeout,
            #copperRateLimitError,
            copperBackoffError,
        ),
        max_tries=5,
    )
    def __make_request(
        self, method: str, endpoint: str, **kwargs
    ) -> Optional[Mapping[Any, Any]]:
        """Performs HTTP Operations."""
        method = method.upper()
        with metrics.http_request_timer(endpoint):
            if method in ("GET", "POST"):
                if method == "GET":
                    kwargs.pop("data", None)
                response = self._session.request(method, endpoint, **kwargs)
                raise_for_error(response)
            else:
                raise ValueError(f"Unsupported method: {method}")

        return response.json()
