# tests/unittests/test_sync.py

"""Unit tests for sync orchestration."""

from unittest.mock import patch, MagicMock
import pytest
from singer import metadata

from tap_copper.streams.abstracts import IncrementalStream, FullTableStream
from tap_copper.exceptions import CopperForbiddenError, CopperNotFoundError, CopperUnauthorizedError


class ConcreteIncremental(IncrementalStream):
    """Concrete implementation of IncrementalStream for testing."""
    tap_stream_id = "test_incremental"
    replication_method = "INCREMENTAL"
    replication_keys = ["date_modified"]
    key_properties = ["id"]
    path = "test/search"
    http_method = "POST"


class ConcreteFullTable(FullTableStream):
    """Concrete implementation of FullTableStream for testing."""
    tap_stream_id = "test_full_table"
    replication_method = "FULL_TABLE"
    replication_keys = []
    key_properties = ["id"]
    path = "test/list"
    http_method = "GET"


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


@pytest.fixture
def mock_full_table_catalog():
    """Catalog stub for full table streams."""
    class Cat:
        class _Schema:
            @staticmethod
            def to_dict():
                return {
                    "properties": {
                        "id": {"type": ["integer", "null"]},
                        "name": {"type": ["null", "string"]},
                    }
                }
        schema = _Schema()
        metadata = metadata.new()
    return Cat()


@pytest.fixture
def passthrough_transformer():
    """Transformer stub that returns record unchanged."""
    class T:
        @staticmethod
        def transform(rec, *_args, **_kwargs):
            return rec
    return T()


class TestCheckAccess:
    """Tests for BaseStream.check_access."""

    def test_check_access_returns_true_on_success(self, mock_client, mock_catalog):
        """check_access returns True when API call succeeds."""
        mock_client.make_request.return_value = []
        stream = ConcreteIncremental(client=mock_client, catalog=mock_catalog)

        assert stream.check_access() is True

    def test_check_access_returns_false_on_forbidden(self, mock_client, mock_catalog):
        """check_access returns False when API returns 403."""
        mock_client.make_request.side_effect = CopperForbiddenError("Forbidden")
        stream = ConcreteIncremental(client=mock_client, catalog=mock_catalog)

        assert stream.check_access() is False

    def test_check_access_raises_on_unauthorized(self, mock_client, mock_catalog):
        """check_access propagates 401 instead of excluding the stream."""
        mock_client.make_request.side_effect = CopperUnauthorizedError("Unauthorized")
        stream = ConcreteIncremental(client=mock_client, catalog=mock_catalog)

        with pytest.raises(CopperUnauthorizedError):
            stream.check_access()

    def test_check_access_returns_false_on_not_found(self, mock_client, mock_catalog):
        """check_access returns False when API endpoint returns 404."""
        mock_client.make_request.side_effect = CopperNotFoundError("Not Found")
        stream = ConcreteIncremental(client=mock_client, catalog=mock_catalog)

        assert stream.check_access() is False

    def test_check_access_logs_warning_message_on_inaccessible_stream(self, mock_client, mock_catalog):
        """check_access logs stream-specific warning when access probing fails."""
        exc = CopperForbiddenError("Forbidden")
        mock_client.make_request.side_effect = exc
        stream = ConcreteIncremental(client=mock_client, catalog=mock_catalog)

        with patch("tap_copper.streams.abstracts.LOGGER.warning") as mock_warning:
            assert stream.check_access() is False

        mock_warning.assert_called_once_with(
            "Unauthorized Stream: %s, excluding from catalog. HTTP-Error-Message:'%s'",
            stream.tap_stream_id,
            str(exc),
        )

class TestIncrementalSync:
    """Tests for IncrementalStream.sync."""

    @patch("tap_copper.streams.abstracts.write_record")
    def test_incremental_sync_filters_by_bookmark(
        self, mock_write_record, mock_client, mock_catalog, passthrough_transformer
    ):
        """Records older than bookmark should be skipped."""
        stream = ConcreteIncremental(client=mock_client, catalog=mock_catalog)
        stream.metadata = {(): {"selected": True}}

        records = [
            {"id": 1, "date_modified": "2024-01-15T00:00:00Z"},
            {"id": 2, "date_modified": "2023-12-01T00:00:00Z"},  # older than bookmark
            {"id": 3, "date_modified": "2024-02-01T00:00:00Z"},
        ]
        state = {"bookmarks": {"test_incremental": {"date_modified": "2024-01-01T00:00:00Z"}}}

        with patch.object(stream, "get_records", return_value=iter(records)):
            stream.sync(state=state, transformer=passthrough_transformer)

        # Record 2 is before the bookmark, should not be written
        written_ids = [c[0][1]["id"] for c in mock_write_record.call_args_list]
        assert 1 in written_ids
        assert 3 in written_ids
        assert 2 not in written_ids

    @patch("tap_copper.streams.abstracts.write_record")
    def test_bookmark_advances_after_sync(
        self, mock_write_record, mock_client, mock_catalog, passthrough_transformer
    ):
        """Bookmark should advance to the max date_modified seen."""
        stream = ConcreteIncremental(client=mock_client, catalog=mock_catalog)
        stream.metadata = {(): {"selected": True}}

        records = [
            {"id": 1, "date_modified": "2024-03-01T00:00:00Z"},
            {"id": 2, "date_modified": "2024-05-01T00:00:00Z"},
        ]
        state = {"bookmarks": {"test_incremental": {"date_modified": "2024-01-01T00:00:00Z"}}}

        with patch.object(stream, "get_records", return_value=iter(records)):
            stream.sync(state=state, transformer=passthrough_transformer)

        assert state["bookmarks"]["test_incremental"]["date_modified"] == "2024-05-01T00:00:00Z"


class TestFullTableSync:
    """Tests for FullTableStream.sync."""

    @patch("tap_copper.streams.abstracts.write_record")
    def test_full_table_sync_writes_all_records(
        self, mock_write_record, mock_client, mock_full_table_catalog, passthrough_transformer
    ):
        """Full table sync should write all records regardless of dates."""
        stream = ConcreteFullTable(client=mock_client, catalog=mock_full_table_catalog)
        stream.metadata = {(): {"selected": True}}

        records = [
            {"id": 1, "name": "Record A"},
            {"id": 2, "name": "Record B"},
            {"id": 3, "name": "Record C"},
        ]

        with patch.object(stream, "get_records", return_value=iter(records)):
            stream.sync(state={}, transformer=passthrough_transformer)

        assert mock_write_record.call_count == 3
