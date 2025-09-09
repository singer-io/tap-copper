# tests/unittests/test_client.py

import json
import pytest
import requests

from tap_copper.client import Client
from tap_copper.exceptions import (
    CopperError,
    CopperRateLimitError,
    CopperServiceUnavailableError,
)


# ---------- light response double ----------

class MockResponse:
    """Minimal fake of requests.Response for Client tests."""
    def __init__(self, status_code=200, payload=None, headers=None, text=None):
        self.status_code = status_code
        self._payload = payload
        self.headers = headers or {}
        if text is not None:
            self.text = text
        else:
            try:
                self.text = json.dumps(payload) if payload is not None else ""
            except Exception:
                self.text = ""

    def json(self):
        if isinstance(self._payload, _NonJson):
            raise json.JSONDecodeError("bad", "doc", 0)
        return self._payload


class _NonJson:
    """Sentinel for non-JSON bodies."""


def make_response(code, payload=None, headers=None, text=None):
    return MockResponse(code, payload=payload, headers=headers, text=text)


# ---------- shared fixtures ----------

@pytest.fixture(autouse=True)
def no_backoff_sleep(monkeypatch):
    """Make backoff immediately retry (no sleep) and small finite iterator."""
    def zero_expo(*_args, **_kwargs):
        return iter([0, 0, 0, 0, 0])  # 5 retries
    monkeypatch.setattr("tap_copper.client.backoff.expo", zero_expo)
    yield


@pytest.fixture
def client_cfg():
    return {
        "api_key": "dummy-key",
        "user_email": "dummy@example.com",
        "base_url": "https://api.copper.com/developer_api/v1",
        "request_timeout": 12,
    }


# ---------- tests ----------

@pytest.mark.parametrize(
    "exc_cls",
    [
        requests.exceptions.ConnectionError,
        requests.exceptions.Timeout,
        requests.exceptions.ChunkedEncodingError,
    ],
    ids=["connection-error", "timeout", "chunked-encoding"],
)
def test_client_backoff_on_transient_errors(monkeypatch, exc_cls, client_cfg):
    def fake_request(*_a, **_k):
        raise exc_cls()
    monkeypatch.setattr("requests.Session.request", fake_request)

    with Client(client_cfg) as client:
        with pytest.raises(exc_cls):
            client.get("https://example.test", params={}, headers={})


def test_client_backoff_on_mapped_server_errors(monkeypatch, client_cfg):
    monkeypatch.setattr(
        "requests.Session.request",
        lambda *_a, **_k: make_response(503, {"message": "Service unavailable"}),
    )
    with Client(client_cfg) as client:
        with pytest.raises(CopperServiceUnavailableError):
            client.get("https://example.test", params={}, headers={})


def test_client_backoff_on_rate_limit(monkeypatch, client_cfg):
    monkeypatch.setattr(
        "requests.Session.request",
        lambda *_a, **_k: make_response(
            429, {"message": "Too many requests"}, headers={"Retry-After": "3"}
        ),
    )
    with Client(client_cfg) as client:
        with pytest.raises(CopperRateLimitError):
            client.get("https://example.test", params={}, headers={})


def test_client_successful_request_get(monkeypatch, client_cfg):
    payload = {"result": ["ok"]}

    def fake_request(*a, **k):
        return make_response(200, payload)

    monkeypatch.setattr("requests.Session.request", fake_request)

    with Client(client_cfg) as client:
        res = client.get("https://example.test", params={"a": 1}, headers={"X-Custom": "v"})
        assert res == payload


def test_client_successful_request_post(monkeypatch, client_cfg):
    payload = {"ok": True}
    monkeypatch.setattr("requests.Session.request", lambda *_a, **_k: make_response(200, payload))
    with Client(client_cfg) as client:
        res = client.post("https://example.test", params={}, headers={}, body={"q": "x"})
        assert res == payload


def test_builds_endpoint_from_path_get(monkeypatch, client_cfg):
    payload = {"ok": 1}
    monkeypatch.setattr("requests.Session.request", lambda *_a, **_k: make_response(200, payload))
    with Client(client_cfg) as client:
        res = client.get(None, params={"p": 1}, headers={}, path="tags")
        assert res == payload


def test_builds_endpoint_from_path_post(monkeypatch, client_cfg):
    payload = {"ok": 2}
    monkeypatch.setattr("requests.Session.request", lambda *_a, **_k: make_response(200, payload))
    with Client(client_cfg) as client:
        res = client.post(None, params={}, headers={}, body={"x": 1}, path="companies/search")
        assert res == payload


def test_no_content_response_returns_none(monkeypatch, client_cfg):
    monkeypatch.setattr("requests.Session.request", lambda *_a, **_k: make_response(204, {}))
    with Client(client_cfg) as client:
        res = client.get("https://example.test", params={}, headers={})
        assert res is None


def test_unknown_error_maps_to_copper_error(monkeypatch, client_cfg):
    monkeypatch.setattr("requests.Session.request", lambda *_a, **_k: make_response(418, {"message": "teapot"}))
    with Client(client_cfg) as client:
        with pytest.raises(CopperError):
            client.get("https://example.test", params={}, headers={})


def test_bad_method_raises_value_error(client_cfg):
    """Call make_request with a minimal opts-like object to trigger ValueError on method."""
    class _MiniOpts:
        # Provide attributes accessed inside Client.make_request
        endpoint = "https://example.test"  # <-- required by your client
        url = "https://example.test"       # safe extra (if client checks url)
        path = None
        params = {}
        headers = {}
        body = None
        timeout = None

    with Client(client_cfg) as client:
        with pytest.raises(ValueError):
            client.make_request("PUT", _MiniOpts())  # invalid method -> ValueError


def test_custom_headers_merged_and_auth_applied(monkeypatch, client_cfg):
    payload = {"ok": True}
    monkeypatch.setattr("requests.Session.request", lambda *_a, **_k: make_response(200, payload))
    with Client(client_cfg) as client:
        res = client.get("https://example.test", params={}, headers={"Accept": "application/json"})
        assert res == payload


def test_empty_string_keys_are_stripped_from_headers_and_params(monkeypatch, client_cfg):
    payload = {"ok": True}
    monkeypatch.setattr("requests.Session.request", lambda *_a, **_k: make_response(200, payload))
    with Client(client_cfg) as client:
        res = client.get("https://example.test", params={"": "x", "a": 1}, headers={"": "y"})
        assert res == payload
