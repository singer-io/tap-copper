"""HTTP client for Copper API with auth, retries, metrics, and error handling."""

from typing import Any, Dict, Mapping, Optional, Tuple
from dataclasses import dataclass
import json
import time

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
    CopperError,
    CopperBackoffError,
)

LOGGER = get_logger()
REQUEST_TIMEOUT = 300


@dataclass
class _RequestOptions:
    """Internal holder to avoid too-many-arguments on make_request."""
    endpoint: Optional[str] = None
    params: Optional[Dict[str, Any]] = None
    headers: Optional[Dict[str, Any]] = None
    body: Optional[Dict[str, Any]] = None
    path: Optional[str] = None


def wait_if_retry_after(details):
    """Backoff handler: if exception has retry_after, sleep exactly that long."""
    exc = details.get("exception")
    retry_after = getattr(exc, "retry_after", None)
    if retry_after is not None:
        LOGGER.warning("Respecting Retry-After: sleeping %s seconds", retry_after)
        time.sleep(retry_after)


def raise_for_error(response: requests.Response) -> None:
    """
    Raise domain-specific exception for non-2xx responses.
    Extracts error messages from Copper payloads when possible.
    """
    try:
        response_json = response.json()
    except (ValueError, json.JSONDecodeError):
        response_json = {}

    status = response.status_code
    if status in (200, 201, 204):
        return

    payload_msg = (
        response_json.get("error")
        or response_json.get("message")
        or (
            ", ".join(response_json.get("errors", []))
            if isinstance(response_json.get("errors"), list)
            else None
        )
        or response.text.strip()
    )

    mapped = ERROR_CODE_EXCEPTION_MAPPING.get(status, {})
    default_msg = mapped.get("message", "Unknown Error")
    message = f"HTTP {status}: {payload_msg or default_msg}"

    exc_class = mapped.get("raise_exception", CopperError)
    LOGGER.error("Raising exception for %s: %s", status, message)
    raise exc_class(message, response) from None


class Client:
    """
    HTTP Client wrapper that handles:
      - Authentication headers
      - Metrics and error logging
      - Retry/backoff for network and 5xx/429 errors
    """

    def __init__(self, config: Mapping[str, Any]) -> None:
        self.config: Dict[str, Any] = dict(config or {})
        self._session = session()
        base = self.config.get("base_url") or "https://api.copper.com/developer_api/v1"
        self.base_url = base.rstrip("/")
        timeout_val = self.config.get("request_timeout")
        self.request_timeout = float(timeout_val) if timeout_val else REQUEST_TIMEOUT

    def __enter__(self):
        self.check_api_credentials()
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        self._session.close()

    def check_api_credentials(self) -> None:
        """Warn if required credentials are missing."""
        if not (self.config.get("api_key") or self.config.get("access_token")):
            LOGGER.warning("Copper api_key/access_token not found in config.")
        if not (self.config.get("user_email") or self.config.get("email")):
            LOGGER.warning("Copper user_email/email not found in config.")

    @staticmethod
    def _strip_empty_keys(d: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        """Drop empty-string keys to avoid bad headers/params."""
        if not isinstance(d, dict):
            return {}
        return {k: v for k, v in d.items() if isinstance(k, str) and k.strip()}

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

    def get(self, endpoint: str, params: Dict, headers: Dict,
            path: str = None) -> Any:
        """Perform a GET request."""
        opts = _RequestOptions(
            endpoint=endpoint,
            params=params,
            headers=headers,
            path=path,
        )
        return self.make_request("GET", opts)

    # pylint: disable=too-many-arguments, too-many-positional-arguments
    def post(self, endpoint: str, params: Dict, headers: Dict,
             body: Dict, path: str = None) -> Any:
        """Perform a POST request."""
        opts = _RequestOptions(
            endpoint=endpoint,
            params=params,
            headers=headers,
            body=body,
            path=path,
        )
        return self.make_request("POST", opts)

    def make_request(self, method: str, opts: _RequestOptions) -> Any:
        """
        Sends an HTTP request to the specified API endpoint.
        Builds URL from base_url + path if endpoint not given.
        """
        params = opts.params or {}
        headers = opts.headers or {}
        body = opts.body or {}
        endpoint = opts.endpoint

        if not endpoint:
            endpoint = (
                self.base_url
                if not opts.path
                else f"{self.base_url}/{str(opts.path).lstrip('/')}"
            )

        headers, params = self.authenticate(headers, params)

        return self.__make_request(
            method,
            endpoint,
            headers=headers,
            params=params,
            json=body,
            timeout=self.request_timeout,
        )

    @backoff.on_exception(
        wait_gen=backoff.expo,
        on_backoff=wait_if_retry_after,
        exception=(
            ConnectionResetError,
            RequestsConnectionError,
            ChunkedEncodingError,
            Timeout,
            CopperBackoffError,
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
        with metrics.http_request_timer(endpoint):
            if method == "GET":
                kwargs.pop("json", None)
            elif method != "POST":
                raise ValueError(f"Unsupported method: {method}")

            response = self._session.request(method, endpoint, **kwargs)
            raise_for_error(response)

            if response.status_code == 204:
                return None
            try:
                return response.json()
            except ValueError:
                return None
