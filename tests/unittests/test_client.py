import json
from unittest.mock import patch
import pytest
import requests

from tap_copper.client import Client, raise_for_error
from tap_copper.exceptions import (
    ERROR_CODE_EXCEPTION_MAPPING,
    CopperError,
    CopperBadRequestError,
    CopperRateLimitError,
    CopperInternalServerError,
    CopperServiceUnavailableError,
)

# ---------- Test helpers ----------

_NON_JSON = object()

class MockResponse:
    """Lightweight fake of requests.Response for raise_for_error tests."""
    def __init__(self, status_code=200, payload=None, headers=None):
        self.status_code = status_code
        self._payload = payload if payload is not None else {}
        self.headers = headers or {}
        # emulate .text used by raise_for_error
        if payload is _NON_JSON:
            self.text = "not-json"
        else:
            try:
                self.text = json.dumps(self._payload)
            except Exception:
                self.text = ""

    def json(self):
        if self._payload is _NON_JSON:
            raise json.JSONDecodeError("bad", "doc", 0)
        return self._payload


def make_response(code, payload=None, headers=None):
    return MockResponse(status_code=code, payload=payload, headers=headers)


# ---------- raise_for_error tests ----------

@pytest.mark.parametrize(
    "status,payload",
    [
        (200, {"ok": True}),
        (201, {"ok": True}),
        (204, {}),
    ],
    ids=["200-ok", "201-created", "204-no-content"],
)
def test_raise_for_error_success(status, payload):
    """Successful status codes should not raise."""
    response = make_response(status, payload)
    raise_for_error(response)  # should not raise


@pytest.mark.parametrize(
    "status,payload,expected_exc,why",
    [
        (400, {"message": "Bad request happened"}, CopperBadRequestError, "mapped 400 with message key"),
        (500, _NON_JSON, CopperInternalServerError, "server error with non-JSON body"),
        (429, {}, CopperRateLimitError, "rate limited; mapping picks RateLimit error"),
        (418, {"message": "I’m a teapot"}, CopperError, "unknown status uses default CopperError"),
    ],
    ids=[
        "400-bad-request-message",
        "500-internal-server-error-nonjson",
        "429-rate-limit",
        "418-unknown-default",
    ],
)
def test_raise_for_error_mapping(status, payload, expected_exc, why):
    """Error statuses should map to correct exception types with sensible messages."""
    response = make_response(status, payload)
    with pytest.raises(expected_exc) as excinfo:
        raise_for_error(response)
    assert str(status) in str(excinfo.value)


# ---------- Client backoff/retry tests ----------

@pytest.fixture(autouse=True)
def no_backoff_sleep(monkeypatch):
    """Avoid real sleeps from backoff by returning zeros."""
    def zero_expo(*args, **kwargs):
        return iter([0, 0, 0, 0, 0, 0, 0])
    monkeypatch.setattr("tap_copper.client.backoff.expo", zero_expo)
    yield

@pytest.fixture
def client_cfg():
    return {
        "api_key": "dummy-key",
        "user_email": "dummy@example.com",
        "base_url": "https://api.copper.com/developer_api/v1",
    }

@pytest.mark.parametrize(
    "exc_cls",
    [
        requests.exceptions.ConnectionError,
        requests.exceptions.Timeout,
        requests.exceptions.ChunkedEncodingError,
    ],
    ids=["connection-error", "timeout", "chunked-encoding"],
)
@patch("requests.Session.request")
def test_client_backoff_on_transient_errors(mock_request, exc_cls, client_cfg):
    """
    Client should retry on transient network errors covered by the backoff decorator
    and ultimately raise after max_tries.
    """
    mock_request.side_effect = exc_cls
    with Client(client_cfg) as client:
        with pytest.raises(exc_cls):
            client.get("https://example.test", params={}, headers={})
    assert mock_request.call_count == 5  # max_tries


@patch("requests.Session.request")
def test_client_backoff_on_mapped_server_errors(mock_request, client_cfg):
    """
    503 mapped to CopperServiceUnavailableError (a backoff class) -> retry then raise.
    """
    mock_request.return_value = make_response(503, {"message": "Service unavailable"})
    with Client(client_cfg) as client:
        with pytest.raises(CopperServiceUnavailableError):
            client.get("https://example.test", params={}, headers={})
    assert mock_request.call_count == 5


@patch("requests.Session.request")
def test_client_backoff_on_rate_limit(mock_request, client_cfg):
    """
    429 should raise CopperRateLimitError (a backoff class) -> retry then raise.
    """
    mock_request.return_value = make_response(
        429, {"message": "Too many requests"}, headers={"Retry-After": "3"}
    )
    with Client(client_cfg) as client:
        with pytest.raises(CopperRateLimitError):
            client.get("https://example.test", params={}, headers={})
    assert mock_request.call_count == 5


@patch("requests.Session.request")
def test_client_successful_request_get(mock_request, client_cfg):
    """Happy-path GET request returns parsed JSON and sets auth headers."""
    payload = {"result": ["ok"]}
    mock_request.return_value = make_response(200, payload)

    with Client(client_cfg) as client:
        res = client.get("https://example.test", params={"a": 1}, headers={"X-Custom": "v"})
        assert res == payload

    # Verify request called with populated headers/params
    called_args, called_kwargs = mock_request.call_args
    assert called_args[0] == "GET"
    assert called_args[1] == "https://example.test"
    hdrs = called_kwargs["headers"]
    assert hdrs["X-PW-AccessToken"] == client_cfg["api_key"]
    assert hdrs["X-PW-UserEmail"] == client_cfg["user_email"]
    assert called_kwargs["params"] == {"a": 1}


@patch("requests.Session.request")
def test_client_successful_request_post(mock_request, client_cfg):
    """Happy-path POST request sends JSON body and returns parsed JSON."""
    payload = {"ok": True}
    mock_request.return_value = make_response(200, payload)

    with Client(client_cfg) as client:
        res = client.post("https://example.test", params={}, headers={}, body={"q": "x"})
        assert res == payload

    _, called_kwargs = mock_request.call_args
    # Our client passes JSON via 'json' kwarg (not 'data')
    assert "json" in called_kwargs and called_kwargs["json"] == {"q": "x"}
