"""HTTP client for Copper API with auth, retries, metrics, and error handling."""

from dataclasses import dataclass
from typing import Any, Dict, Mapping, Optional, Tuple
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
    CopperBackoffError,
    CopperError,
    CopperRateLimitError,
)

LOGGER = get_logger()
REQUEST_TIMEOUT = 300


@dataclass
class _RequestOptions:
    """Internal container for request params."""

    endpoint: Optional[str] = None
    params: Optional[Dict[str, Any]] = None
    headers: Optional[Dict[str, Any]] = None
    body: Optional[Dict[str, Any]] = None
    path: Optional[str] = None


def _wait_if_retry_after(details) -> None:
    """If the exception carries a retry_after, sleep exactly that long."""
    exc = details.get("exception")
    retry_after = getattr(exc, "retry_after", None)
    if retry_after is not None:
        LOGGER.warning("Retry-After honored: sleeping %s second(s)", retry_after)
        time.sleep(retry_after)


def _raise_for_error(response: requests.Response) -> None:
    """Map non-2xx responses to domain exceptions with a helpful message."""
    try:
        payload = response.json()
    except (ValueError, json.JSONDecodeError):
        payload = {}

    status = response.status_code
    if status in (200, 201, 204):
        return

    msg = (
        payload.get("error")
        or payload.get("message")
        or (
            ", ".join(payload.get("errors", []))
            if isinstance(payload.get("errors"), list)
            else None
        )
        or response.text.strip()
        or "Unknown Error"
    )

    mapping = ERROR_CODE_EXCEPTION_MAPPING.get(status, {})
    exc_cls = mapping.get("raise_exception", CopperError)
    default_msg = mapping.get("message", "Unknown Error")
    full_msg = f"HTTP {status}: {msg or default_msg}"
    raise exc_cls(full_msg, response) from None


class Client:
    """Copper HTTP client: auth, retries, metrics, and error handling."""

    def __init__(self, config: Mapping[str, Any]) -> None:
        self.config: Dict[str, Any] = dict(config or {})
        self._session = session()
        base = self.config.get("base_url") or "https://api.copper.com/developer_api/v1"
        self.base_url = base.rstrip("/")
        timeout_val = self.config.get("request_timeout")
        self.request_timeout = float(timeout_val) if timeout_val else REQUEST_TIMEOUT

    def __enter__(self):
        self._validate_api_credentials()
        return self

    def __exit__(self, exc_type, exc, tb):
        self._session.close()

    # ---------- configuration / auth ----------

    def _validate_api_credentials(self) -> None:
        """Raise if required credentials are not present."""
        if not (self.config.get("api_key") or self.config.get("access_token")):
            LOGGER.error("Missing Copper API credentials: api_key or access_token.")
            raise CopperError(
                "Missing required Copper API credentials: api_key or access_token."
            )
        if not (self.config.get("user_email") or self.config.get("email")):
            LOGGER.error("Missing Copper user identity: user_email or email.")
            raise CopperError(
                "Missing required Copper user email: user_email or email."
            )

    @staticmethod
    def _strip_empty_keys(dct: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        """Drop empty-string keys to avoid bad headers/params."""
        if not isinstance(dct, dict):
            return {}
        return {k: v for k, v in dct.items() if isinstance(k, str) and k.strip()}

    def _authenticate(self, headers: Dict, params: Dict) -> Tuple[Dict, Dict]:
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

    # ---------- public API ----------

    def make_request(self, method: str, opts: _RequestOptions) -> Any:
        """Builds and issues an HTTP request."""
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

        headers, params = self._authenticate(headers, params)

        return self.__request(
            method,
            endpoint,
            headers=headers,
            params=params,
            json=body,
            timeout=self.request_timeout,
        )

    # Public helpers expected by tests (keyword args allowed)
    def get(
        self,
        endpoint: str,
        *,
        params: Optional[Dict] = None,
        headers: Optional[Dict] = None,
        path: Optional[str] = None,
    ) -> Any:
        """Thin GET wrapper used by tests/streams."""
        opts = _RequestOptions(endpoint=endpoint, params=params, headers=headers, path=path)
        return self.make_request("GET", opts)

    def post(   # pylint: disable=too-many-arguments
        self,
        endpoint: str,
        *,
        params: Optional[Dict] = None,
        headers: Optional[Dict] = None,
        body: Optional[Dict] = None,
        path: Optional[str] = None,
    ) -> Any:
        """Thin POST wrapper used by tests/streams."""
        opts = _RequestOptions(
            endpoint=endpoint, params=params, headers=headers, body=body, path=path
        )
        return self.make_request("POST", opts)

    # ---------- low-level HTTP ----------

    @backoff.on_exception(
        wait_gen=lambda: backoff.expo(factor=2),
        on_backoff=_wait_if_retry_after,
        exception=(
            ConnectionResetError,
            RequestsConnectionError,
            ChunkedEncodingError,
            Timeout,
            CopperBackoffError,  # e.g. 429/5xx mapped errors
            CopperRateLimitError,
        ),
        max_tries=5,
    )
    def __request(
        self,
        method: str,
        endpoint: str,
        **kwargs: Any,
    ) -> Optional[Mapping[str, Any]]:
        """Perform the HTTP request with metrics and error mapping."""
        method = (method or "").upper()
        with metrics.http_request_timer(endpoint):
            if method == "GET":
                # requests ignores 'json' for GET; remove to be explicit
                kwargs.pop("json", None)
            elif method != "POST":
                raise ValueError(f"Unsupported method: {method}")

            response = self._session.request(method, endpoint, **kwargs)
            _raise_for_error(response)

            if response.status_code == 204:
                return None

            try:
                return response.json()
            except ValueError:
                return None
