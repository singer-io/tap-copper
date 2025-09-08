# tap_copper/client.py
"""HTTP client for Copper API with auth, retries, metrics, and error handling."""

from dataclasses import dataclass
from typing import Any, Dict, Mapping, Optional, Tuple

import json
import time

import backoff
import requests
from requests import Session
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


def wait_if_retry_after(details) -> None:
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
        self._session: Session = Session()
        base = self.config.get("base_url") or "https://api.copper.com/developer_api/v1"
        self.base_url = str(base).rstrip("/")
        timeout_val = self.config.get("request_timeout")
        self.request_timeout = float(timeout_val) if timeout_val else REQUEST_TIMEOUT

    def __enter__(self):
        self._check_api_credentials()
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        self._session.close()

    def _check_api_credentials(self) -> None:
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

    def authenticate(self, headers: Optional[Dict], params: Optional[Dict]) -> Tuple[Dict, Dict]:
        """Attach Copper auth headers and sane defaults."""
        headers = dict(self._strip_empty_keys(headers or {}))
        params = dict(self._strip_empty_keys(params or {}))
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

    # Public helpers expected by tests (keyword args allowed)
    def get(self, endpoint: str, *, params: Optional[Dict] = None, headers: Optional[Dict] = None, path: Optional[str] = None) -> Any:
        """Perform a GET request."""
        opts = _RequestOptions(endpoint=endpoint, params=params or {}, headers=headers or {}, path=path)
        return self._make_request("GET", opts)

    def post(self, endpoint: str, *, params: Optional[Dict] = None, headers: Optional[Dict] = None, body: Optional[Dict] = None, path: Optional[str] = None) -> Any:
        """Perform a POST request."""
        opts = _RequestOptions(endpoint=endpoint, params=params or {}, headers=headers or {}, body=body or {}, path=path)
        return self._make_request("POST", opts)

    # Singer-tap convenience wrapper used by streams
    def make_request(self, method: str, endpoint: Optional[str] = None, params: Optional[Dict] = None, headers: Optional[Dict] = None, body: Optional[Dict] = None, path: Optional[str] = None) -> Any:
        """Unified request entry point used by streams (supports GET/POST)."""
        opts = _RequestOptions(endpoint=endpoint, params=params, headers=headers, body=body, path=path)
        return self._make_request(method, opts)

    def _resolve_endpoint(self, endpoint: Optional[str], path: Optional[str]) -> str:
        if endpoint:
            return endpoint
        if path:
            return f"{self.base_url}/{str(path).lstrip('/')}"
        return self.base_url

    @backoff.on_exception(
        wait_gen=backoff.expo,  # factor=2 default is fine here
        on_backoff=wait_if_retry_after,
        exception=(
            RequestsConnectionError,           # requests.exceptions.ConnectionError
            Timeout,
            ChunkedEncodingError,
            CopperBackoffError,                # our mapped 429/5xx classes
        ),
        max_tries=5,
    )
    def __request(self, method: str, endpoint: str, **kwargs) -> Optional[Mapping[str, Any]]:
        """Low-level HTTP request/response handler with metrics + error raising."""
        with metrics.http_request_timer(endpoint):
            response = self._session.request(method, endpoint, **kwargs)
            raise_for_error(response)

            if response.status_code == 204:
                return None
            try:
                return response.json()
            except ValueError:
                return None

    def _make_request(self, method: str, opts: _RequestOptions) -> Any:
        """
        Sends an HTTP request to the specified API endpoint.
        Builds URL from base_url + path if endpoint not given.
        """
        params = opts.params or {}
        headers = opts.headers or {}
        body = opts.body or {}
        endpoint = self._resolve_endpoint(opts.endpoint, opts.path)

        headers, params = self.authenticate(headers, params)

        # requests will encode JSON properly via 'json' kw for POST; for GET we omit body
        if method.upper() == "GET":
            return self.__request("GET", endpoint, headers=headers, params=params, timeout=self.request_timeout)
        if method.upper() == "POST":
            return self.__request("POST", endpoint, headers=headers, params=params, json=body, timeout=self.request_timeout)
        raise ValueError(f"Unsupported method: {method}")
