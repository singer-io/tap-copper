"""HTTP client for Copper API with auth, retries, metrics, and error handling."""

from typing import Any, Dict, Mapping, Optional, Tuple
import json

import backoff
import requests
from requests import session
from requests.exceptions import (
    Timeout,
    ConnectionError as RequestsConnectionError,
    ChunkedEncodingError,
)
from singer import get_logger, metrics

from tap_copper.exceptions import (
    ERROR_CODE_EXCEPTION_MAPPING,
    copperError,
    copperBackoffError,
)

LOGGER = get_logger()
REQUEST_TIMEOUT = 300


def raise_for_error(response: requests.Response) -> None:
    """
    Raise a domain-specific exception for non-2xx responses.

    Tries to parse JSON error payload; falls back to status mapping.
    """
    try:
        response_json = response.json()
    except (ValueError, json.JSONDecodeError) as exc:
        # Endpoints sometimes return non-JSON bodies (HTML, text, empty).
        LOGGER.warning("Failed to parse response JSON: %s", exc)
        response_json = {}

    if response.status_code in (200, 201, 204):
        return

    # Prefer explicit "error" key, else "message", else mapping default
    payload_msg = response_json.get("error") or response_json.get("message")
    mapped_msg = ERROR_CODE_EXCEPTION_MAPPING.get(response.status_code, {}).get(
        "message", "Unknown Error"
    )
    message = f"HTTP {response.status_code}: {payload_msg or mapped_msg}"

    exc_class = ERROR_CODE_EXCEPTION_MAPPING.get(response.status_code, {}).get(
        "raise_exception", copperError
    )
    LOGGER.error("Raising exception for status %s: %s", response.status_code, message)
    raise exc_class(message, response) from None


class Client:
    """
    HTTP Client wrapper that handles:
      - Authentication (Copper headers)
      - Response parsing and metrics
      - HTTP error handling + retry
    """

    def __init__(self, config: Mapping[str, Any]) -> None:
        self.config: Dict[str, Any] = dict(config or {})
        self._session = session()

        # Normalize exactly one trailing slash
        base = self.config.get("base_url") or "https://api.copper.com/developer_api/v1"
        self.base_url = base.rstrip("/")

        # Request timeout
        config_request_timeout = self.config.get("request_timeout")
        self.request_timeout = (
            float(config_request_timeout) if config_request_timeout else REQUEST_TIMEOUT
        )

    def __enter__(self):
        self.check_api_credentials()
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        self._session.close()

    def check_api_credentials(self) -> None:
        """Soft validation of required config keys."""
        if not (self.config.get("api_key") or self.config.get("access_token")):
            LOGGER.warning("Copper api_key/access_token not found in config.")
        if not (self.config.get("user_email") or self.config.get("email")):
            LOGGER.warning("Copper user_email/email not found in config.")

    @staticmethod
    def _strip_empty_keys(d: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        """Drop empty-string keys to avoid bad headers/params."""
        if not isinstance(d, dict):
            return {}
        return {k: v for k, v in d.items() if isinstance(k, str) and k.strip() != ""}

    def authenticate(self, headers: Dict, params: Dict) -> Tuple[Dict, Dict]:
        """Attach Copper auth headers and sane defaults."""
        headers = dict(self._strip_empty_keys(headers))
        params = dict(self._strip_empty_keys(params))

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

    def get(self, endpoint: str, params: Dict, headers: Dict, path: str = None) -> Any:
        """Perform a GET request."""
        try:
            return self.make_request(
                method="GET",
                endpoint=endpoint,
                params=params,
                headers=headers,
                path=path,
            )
        except Exception as exc:  # pylint: disable=broad-except
            LOGGER.exception("Failed GET request to %s: %s", endpoint or path, exc)
            raise

    def post(  # pylint: disable=too-many-arguments,too-many-positional-arguments
        self,
        endpoint: str,
        params: Dict,
        headers: Dict,
        body: Dict,
        path: str = None,
    ) -> Any:
        """Perform a POST request."""
        try:
            return self.make_request(
                method="POST",
                endpoint=endpoint,
                params=params,
                headers=headers,
                body=body,
                path=path,
            )
        except Exception as exc:  # pylint: disable=broad-except
            LOGGER.exception("Failed POST request to %s: %s", endpoint or path, exc)
            raise

    def make_request(  # pylint: disable=too-many-arguments,too-many-positional-arguments
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
        Builds URL from base_url + path if endpoint not given.
        """
        params = params or {}
        headers = headers or {}
        body = body or {}

        # Build URL robustly if endpoint not provided
        if not endpoint:
            endpoint = self.base_url if not path else f"{self.base_url}/{str(path).lstrip('/')}"

        # Inject auth + strip empty keys
        headers, params = self.authenticate(headers, params)

        return self.__make_request(
            method,
            endpoint,
            headers=headers,
            params=params,
            json=body,  # dropped for GET inside __make_request
            timeout=self.request_timeout,
        )

    @backoff.on_exception(
        wait_gen=backoff.expo,
        exception=(
            ConnectionResetError,
            RequestsConnectionError,
            ChunkedEncodingError,
            Timeout,
            copperBackoffError,
        ),
        max_tries=5,
        factor=2,
    )
    def __make_request(
        self,
        method: str,
        endpoint: str,
        **kwargs,
    ) -> Optional[Mapping[str, Any]]:
        """Low-level HTTP request/response handler with metrics + error raising."""
        method = (method or "").upper()
        try:
            with metrics.http_request_timer(endpoint):
                if method == "GET":
                    kwargs.pop("json", None)  # do not send body on GET
                elif method != "POST":
                    raise ValueError(f"Unsupported method: {method}")

                response = self._session.request(method, endpoint, **kwargs)
                raise_for_error(response)
                return response.json()
        except Exception as exc:  # pylint: disable=broad-except
            LOGGER.exception("%s request to %s failed: %s", method, endpoint, exc)
            raise
