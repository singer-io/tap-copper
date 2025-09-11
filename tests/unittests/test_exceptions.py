import pytest

from tap_copper.exceptions import (
    CopperError,
    CopperBackoffError,
    CopperRateLimitError,
)


class _Resp:
    """Minimal response double exposing .headers and .status_code."""
    def __init__(self, status_code=429, headers=None, payload=None):
        self.status_code = status_code
        self.headers = headers or {}
        self._payload = payload if payload is not None else {}

    def json(self):
        """Return the JSON payload for compatibility with callers."""
        return self._payload


def test_rate_limit_error_parses_integer_retry_after_and_formats_message():
    """Validate that Retry-After is parsed as int and included in the message."""
    resp = _Resp(headers={"Retry-After": "3"})
    err = CopperRateLimitError("Too many requests", response=resp)
    assert isinstance(err, CopperBackoffError)
    assert isinstance(err, CopperError)
    assert err.retry_after == 3
    assert "Retry after 3 seconds" in str(err)


def test_rate_limit_error_handles_non_integer_retry_after_with_unknown_text():
    """Ensure non-integer Retry-After results in retry_after=None and 'unknown delay' text."""
    resp = _Resp(headers={"Retry-After": "abc"})
    err = CopperRateLimitError("Rate limited", response=resp)
    assert err.retry_after is None
    assert "unknown delay" in str(err)


def test_rate_limit_error_handles_missing_retry_after_header():
    """Ensure missing Retry-After header yields retry_after=None and 'unknown delay' text."""
    resp = _Resp(headers={})
    err = CopperRateLimitError("Rate limited", response=resp)
    assert err.retry_after is None
    assert "unknown delay" in str(err)


def test_rate_limit_error_uses_default_message_when_none_provided():
    """Verify that a default base message is used when no message argument is provided."""
    resp = _Resp(headers={"Retry-After": "5"})
    err = CopperRateLimitError(response=resp)
    assert "Rate limit hit" in str(err)
    assert "Retry after 5 seconds" in str(err)


def test_rate_limit_error_with_none_response_defaults_to_unknown_delay():
    """Confirm that a None response produces retry_after=None and 'unknown delay' text."""
    err = CopperRateLimitError("Limited", response=None)
    assert err.retry_after is None
    assert "unknown delay" in str(err)


def test_raise_for_error_integration_populates_retry_after_and_message():
    """Integration: raise_for_error(429) should raise CopperRateLimitError with parsed Retry-After."""
    from tap_copper.client import raise_for_error  # local import to avoid circulars in other tests

    resp = _Resp(
        status_code=429,
        headers={"Retry-After": "7"},
        payload={"message": "Too many requests"},
    )
    with pytest.raises(CopperRateLimitError) as excinfo:
        raise_for_error(resp)

    err = excinfo.value
    assert isinstance(err, CopperRateLimitError)
    assert err.retry_after == 7
    assert "Too many requests" in str(err)
    assert "Retry after 7 seconds" in str(err)
