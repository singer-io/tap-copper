"""Custom Copper API exception classes mapping to HTTP error codes."""

from __future__ import annotations


class CopperError(Exception):
    """Generic HTTP error for Copper."""

    def __init__(self, message: str | None = None, response=None) -> None:
        msg = message or "An error occurred with the Copper API."
        super().__init__(msg)
        self.message = msg
        self.response = response


class CopperBackoffError(CopperError):
    """Base class for errors that should trigger backoff/retry."""


class CopperBadRequestError(CopperError):
    """400 Bad Request."""


class CopperUnauthorizedError(CopperError):
    """401 Unauthorized."""


class CopperForbiddenError(CopperError):
    """403 Forbidden."""


class CopperNotFoundError(CopperError):
    """404 Not Found."""


class CopperConflictError(CopperError):
    """409 Conflict."""


class CopperUnprocessableEntityError(CopperBackoffError):
    """422 Unprocessable Entity."""


class CopperRateLimitError(CopperBackoffError):
    """429 Too Many Requests / Rate Limited.

    Parses the 'Retry-After' header (seconds) if present and exposes it as
    `retry_after` for backoff handlers.
    """

    def __init__(self, message: str | None = None, response=None) -> None:
        self.response = response

        retry_after = None
        if response is not None and hasattr(response, "headers"):
            raw_retry = response.headers.get("Retry-After")
            if raw_retry:
                try:
                    retry_after = int(raw_retry)
                except (TypeError, ValueError):
                    retry_after = None

        self.retry_after = retry_after
        base_msg = message or "Rate limit hit"
        retry_info = (
            f"(Retry after {self.retry_after} seconds.)"
            if self.retry_after is not None
            else "(Retry after unknown delay.)"
        )
        full_message = f"{base_msg} {retry_info}"
        super().__init__(full_message, response=response)


class CopperInternalServerError(CopperBackoffError):
    """500 Internal Server Error."""


class CopperNotImplementedError(CopperBackoffError):
    """501 Not Implemented."""


class CopperBadGatewayError(CopperBackoffError):
    """502 Bad Gateway."""


class CopperServiceUnavailableError(CopperBackoffError):
    """503 Service Unavailable."""


class CopperGatewayTimeout(CopperBackoffError):
    """504 Gateway Timeout."""


ERROR_CODE_EXCEPTION_MAPPING = {
    400: {
        "raise_exception": CopperBadRequestError,
        "message": "A validation exception has occurred.",
    },
    401: {
        "raise_exception": CopperUnauthorizedError,
        "message": (
            "The access token provided is expired, revoked, malformed or invalid "
            "for other reasons."
        ),
    },
    403: {
        "raise_exception": CopperForbiddenError,
        "message": "You are missing the following required scopes: read",
    },
    404: {
        "raise_exception": CopperNotFoundError,
        "message": "The resource you have specified cannot be found.",
    },
    409: {
        "raise_exception": CopperConflictError,
        "message": (
            "The API request cannot be completed because the requested operation "
            "would conflict with an existing item."
        ),
    },
    422: {
        "raise_exception": CopperUnprocessableEntityError,
        "message": "The request content itself is not processable by the server.",
    },
    429: {
        "raise_exception": CopperRateLimitError,
        "message": (
            "The API rate limit for your organisation/application pairing has been "
            "exceeded."
        ),
    },
    500: {
        "raise_exception": CopperInternalServerError,
        "message": (
            "The server encountered an unexpected condition which prevented it from "
            "fulfilling the request."
        ),
    },
    501: {
        "raise_exception": CopperNotImplementedError,
        "message": (
            "The server does not support the functionality required to fulfill the "
            "request."
        ),
    },
    502: {
        "raise_exception": CopperBadGatewayError,
        "message": "Server received an invalid response.",
    },
    503: {
        "raise_exception": CopperServiceUnavailableError,
        "message": "API service is currently unavailable.",
    },
    504: {
        "raise_exception": CopperGatewayTimeout,
        "message": "API request timed out while waiting for a response.",
    },
}
