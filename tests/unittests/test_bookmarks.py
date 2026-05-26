# tests/unittests/test_bookmarks.py

"""Unit tests for bookmark read/write functionality."""

from unittest.mock import patch, MagicMock
import pytest
from singer import metadata

from tap_copper.streams.abstracts import IncrementalStream


class ConcreteIncremental(IncrementalStream):
    """Concrete implementation of IncrementalStream for testing."""
    tap_stream_id = "test_stream"
    replication_method = "INCREMENTAL"
    replication_keys = ["date_modified"]
    key_properties = ["id"]
    path = "test/search"
    http_method = "POST"


@pytest.fixture
def mock_client():
    """Client stub."""
    client = MagicMock()
    client.config = {
        "api_key": "test_key",
        "user_email": "test@example.com",
        "start_date": "2024-01-01T00:00:00Z",
    }
    client.base_url = "https://api.copper.com/developer_api/v1"
    return client


@pytest.fixture
def mock_catalog():
    """Catalog stub with schema + valid metadata list."""
    class Cat:
        class _Schema:
            @staticmethod
            def to_dict():
                return {
                    "properties": {
                        "id": {"type": ["integer", "null"]},
                        "date_modified": {"type": ["null", "string"], "format": "date-time"},
                    }
                }
        schema = _Schema()
        metadata = metadata.new()
    return Cat()


class TestGetBookmark:
    """Tests for IncrementalStream.get_bookmark."""

    def test_returns_existing_bookmark(self, mock_client, mock_catalog):
        """Should return existing bookmark value from state."""
        stream = ConcreteIncremental(client=mock_client, catalog=mock_catalog)
        state = {"bookmarks": {"test_stream": {"date_modified": "2024-06-01T00:00:00Z"}}}

        result = stream.get_bookmark(state=state, stream="test_stream")
        assert result == "2024-06-01T00:00:00Z"

    def test_falls_back_to_start_date(self, mock_client, mock_catalog):
        """Should fall back to start_date when bookmark doesn't exist."""
        stream = ConcreteIncremental(client=mock_client, catalog=mock_catalog)
        state = {"bookmarks": {}}

        result = stream.get_bookmark(state=state, stream="test_stream")
        assert result == "2024-01-01T00:00:00Z"

    def test_custom_key(self, mock_client, mock_catalog):
        """Should use custom key when provided."""
        stream = ConcreteIncremental(client=mock_client, catalog=mock_catalog)
        state = {"bookmarks": {"test_stream": {"custom_key": "2024-03-01T00:00:00Z"}}}

        result = stream.get_bookmark(state=state, stream="test_stream", key="custom_key")
        assert result == "2024-03-01T00:00:00Z"


class TestWriteBookmark:
    """Tests for IncrementalStream.write_bookmark."""

    def test_advances_bookmark(self, mock_client, mock_catalog):
        """Should advance the bookmark when new value is greater."""
        stream = ConcreteIncremental(client=mock_client, catalog=mock_catalog)
        state = {"bookmarks": {"test_stream": {"date_modified": "2024-01-01T00:00:00Z"}}}

        result_state = stream.write_bookmark(
            state=state,
            stream="test_stream",
            value="2024-06-01T00:00:00Z",
        )
        assert result_state["bookmarks"]["test_stream"]["date_modified"] == "2024-06-01T00:00:00Z"

    def test_does_not_regress_bookmark(self, mock_client, mock_catalog):
        """Should not regress the bookmark when new value is less."""
        stream = ConcreteIncremental(client=mock_client, catalog=mock_catalog)
        state = {"bookmarks": {"test_stream": {"date_modified": "2024-06-01T00:00:00Z"}}}

        result_state = stream.write_bookmark(
            state=state,
            stream="test_stream",
            value="2024-01-01T00:00:00Z",
        )
        assert result_state["bookmarks"]["test_stream"]["date_modified"] == "2024-06-01T00:00:00Z"

    def test_writes_new_bookmark(self, mock_client, mock_catalog):
        """Should write a new bookmark when none exists."""
        stream = ConcreteIncremental(client=mock_client, catalog=mock_catalog)
        state = {"bookmarks": {}}

        result_state = stream.write_bookmark(
            state=state,
            stream="test_stream",
            value="2024-03-15T12:00:00Z",
        )
        assert result_state["bookmarks"]["test_stream"]["date_modified"] == "2024-03-15T12:00:00Z"
