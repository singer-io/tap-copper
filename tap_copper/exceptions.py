"""Exception classes for handling Copper API errors."""


class CopperError(Exception):
    """Class representing a generic HTTP error."""

    def __init__(self, message=None, response=None):
        super().__init__(message)
        self.message = message
        self.response = response


class CopperBackoffError(CopperError):
    """Class representing backoff error handling."""
    pass


class CopperBadRequestError(CopperError):
    """Class representing 400 status code."""
    pass


class CopperUnauthorizedError(CopperError):
    """Class representing 401 status code."""
    pass


class CopperForbiddenError(CopperError):
    """Class representing 403 status code."""
    pass


class CopperNotFoundError(CopperError):
    """Class representing 404 status code."""
    pass


class CopperConflictError(CopperError):
    """Class representing 409 status code."""
    pass


class CopperUnprocessableEntityError(CopperBackoffError):
    """Class representing 422 status code."""
    pass


class CopperRateLimitError(CopperBackoffError):
    """Class representing 429 status code."""

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
    """Class representing 500 status code."""
    pass


class CopperNotImplementedError(CopperBackoffError):
    """Class representing 501 status code."""
    pass


class CopperBadGatewayError(CopperBackoffError):
    """Class representing 502 status code."""
    pass


class CopperServiceUnavailableError(CopperBackoffError):
    """Class representing 503 status code."""
    pass


class CopperGatewayTimeout(CopperBackoffError):
    """Class representing 504 status code."""
    pass


ERROR_CODE_EXCEPTION_MAPPING = {
    400: {
        "raise_exception": CopperBadRequestError,
        "message": "A validation exception has occurred."
    },
    401: {
        "raise_exception": CopperUnauthorizedError,
        "message": (
            "The access token provided is expired, revoked, malformed "
            "or invalid for other reasons."
        )
    },
    403: {
        "raise_exception": CopperForbiddenError,
        "message": "You are missing the following required scopes: read"
    },
    404: {
        "raise_exception": CopperNotFoundError,
        "message": "The resource you have specified cannot be found."
    },
    409: {
        "raise_exception": CopperConflictError,
        "message": (
            "The API request cannot be completed because the requested operation "
            "would conflict with an existing item."
        )
    },
    422: {
        "raise_exception": CopperUnprocessableEntityError,
        "message": "The request content itself is not processable by the server."
    },
    429: {
        "raise_exception": CopperRateLimitError,
        "message": (
            "The API rate limit for your organisation/application pairing "
            "has been exceeded."
        )
    },
    500: {
        "raise_exception": CopperInternalServerError,
        "message": (
            "The server encountered an unexpected condition which prevented "
            "it from fulfilling the request."
        )
    },
    501: {
        "raise_exception": CopperNotImplementedError,
        "message": (
            "The server does not support the functionality required to fulfill the request."
        )
    },
    502: {
        "raise_exception": CopperBadGatewayError,
        "message": "Server received an invalid response."
    },
    503: {
        "raise_exception": CopperServiceUnavailableError,
        "message": "API service is currently unavailable.",
    },
    504: {
        "raise_exception": CopperGatewayTimeout,
        "message": "API request timed out while waiting for a response.",
    }
}
