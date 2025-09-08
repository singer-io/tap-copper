"""HTTP client (testing variant) for Copper API with auth, retries, and error handling."""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any, Dict, Mapping, Optional

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
DEFAULT_BASE_URL = "https://api.copper.com/developer_api/v1"


@dataclass
class RequestOptions:
    """Container for optional request arguments to keep signatures short."""
    endpoint: Optional[str] = None
    params: Optional[Dict[str, Any]] = None
    headers: Optional[Dict[str, Any]] = None
    body: Optional[Dict[str, Any]] = None
    path: Optional[str] = None


def wait_if_retry_after(details: Dict[str, Any]) -> None:
    """Respect a Retry-After value provided by an exception during backoff."""
    exc = details.get("exception")
    retry_after = getattr(exc, "retry_after", None)
    if retry_after:
        LOGGER.warning("Retrying after %s second(s) due to rate limiting.", retry_after)
        time.sleep(retry_after)


def raise_for_error(response: requests.Response) -> None:
    """Raise a domain-specific exception for non-2xx responses."""
    try:
        payload = response.json()
    except Exception:  # pylint: disable=broad-except
        payload = {}

    if response.status_code in (200, 201, 204):
        return

    errors = payload.get("errors")
    errors_joined = ", ".join(errors) if isinstance(errors, list) else None
    message = (
        payload.get("error")
        or payload.get("message")
        or errors_joined
        or response.text.strip()
        or "Unknown Error"
    )

    exc_cls = ERROR_CODE_EXCEPTION_MAPPING.get(
        response.status_code, {}
    ).get("raise_exception", CopperError)

    raise exc_cls(f"HTTP {response.status_code}: {message}", response) from None


class Client:
    """HTTP client wrapper for Copper API (testing variant)."""

    def __init__(self, config: Mapping[str, Any]) -> None:
        self.config: Dict[str, Any] = dict(config or {})
        self._session = session()
        self.base_url = (self.config.get("base_url") or DEFAULT_BASE_URL).rstrip("/")
        timeout_val = self.config.get("request_timeout")
        self.request_timeout = float(timeout_val) if timeout_val else REQUEST_TIMEOUT

    def __enter__(self) -> "Client":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:  # noqa: D401
        self._session.close()

    def authenticate(
        self,
        headers: Optional[Dict[str, Any]],
        params: Optional[Dict[str, Any]],
    ) -> tuple[Dict[str, Any], Dict[str, Any]]:
        """Attach Copper authentication headers to request headers."""
        hdrs: Dict[str, Any] = dict(headers or {})
        prms: Dict[str, Any] = dict(params or {})

        api_key = self.config.get("api_key") or self.config.get("access_token")
        user_email = self.config.get("user_email") or self.config.get("email")

        if api_key:
            hdrs["X-PW-AccessToken"] = api_key
        if user_email:
            hdrs["X-PW-UserEmail"] = user_email

        hdrs.setdefault("X-PW-Application", "developer_api")
        hdrs.setdefault("Accept", "application/json")
        hdrs.setdefault("Content-Type", "application/json")

        return hdrs, prms

    def get(self, opts: RequestOptions) -> Any:
        """Perform a GET request."""
        return self.make_request("GET", opts)

    def post(self, opts: RequestOptions) -> Any:
        """Perform a POST request."""
        return self.make_request("POST", opts)

    def make_request(self, method: str, opts: RequestOptions) -> Any:
        """Send an HTTP request with retries and error handling."""
        if opts.endpoint:
            url = opts.endpoint
        else:
            suffix = str(opts.path).lstrip("/") if opts.path else ""
            url = f"{self.base_url}/{suffix}" if suffix else self.base_url

        auth_headers, auth_params = self.authenticate(
            opts.headers or {}, opts.params or {}
        )

        return self._do_request(
            method.upper(),
            url,
            headers=auth_headers,
            params=auth_params,
            json=opts.body or {},
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
    def _do_request(self, method: str, url: str, **kwargs) -> Optional[Mapping[str, Any]]:
        """Low-level HTTP request/response handler with metrics and error raising."""
        with metrics.http_request_timer(url):
            response = self._session.request(method, url, **kwargs)
            raise_for_error(response)

        if response.status_code == 204:
            return None

        try:
            return response.json()
        except ValueError:
            return None
