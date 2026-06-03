# tests/unittests/test_discovery.py

"""Unit tests for discovery with access checks."""

from unittest.mock import patch, MagicMock
import pytest

from tap_copper.discover import discover, _apply_access_checks, _prune_inaccessible_children
from tap_copper.exceptions import CopperForbiddenError


def _make_stream_cls(parent_value, check_access_result=True):
    """Create a mock stream class with a proper 'parent' class attribute."""
    class MockStreamCls:
        parent = parent_value

        def __init__(self, client=None, **kwargs):
            self.client = client

        def check_access(self):
            return check_access_result

    return MockStreamCls


@pytest.fixture
def mock_client():
    """Client stub for discovery tests."""
    client = MagicMock()
    client.config = {
        "api_key": "test_key",
        "user_email": "test@example.com",
        "start_date": "2024-01-01T00:00:00Z",
    }
    client.base_url = "https://api.copper.com/developer_api/v1"
    return client


class TestApplyAccessChecks:
    """Tests for _apply_access_checks."""

    def test_all_streams_accessible(self, mock_client):
        """When all streams are accessible, nothing is removed."""
        schemas = {"companies": {}, "people": {}, "leads": {}}
        field_metadata = {"companies": [], "people": [], "leads": []}

        mock_streams = {
            "companies": _make_stream_cls("", True),
            "people": _make_stream_cls("", True),
            "leads": _make_stream_cls("", True),
        }

        with patch("tap_copper.discover.STREAMS", mock_streams):
            _apply_access_checks(mock_client, schemas, field_metadata)

        assert "companies" in schemas
        assert "people" in schemas
        assert "leads" in schemas

    def test_partial_access_excludes_forbidden_streams(self, mock_client):
        """When some streams return 403, they are excluded from catalog."""
        schemas = {"companies": {}, "people": {}, "leads": {}}
        field_metadata = {"companies": [], "people": [], "leads": []}

        mock_streams = {
            "companies": _make_stream_cls("", False),
            "people": _make_stream_cls("", True),
            "leads": _make_stream_cls("", True),
        }

        with patch("tap_copper.discover.STREAMS", mock_streams):
            _apply_access_checks(mock_client, schemas, field_metadata)

        assert "companies" not in schemas
        assert "companies" not in field_metadata
        assert "people" in schemas
        assert "leads" in schemas

    def test_all_streams_forbidden_raises_error(self, mock_client):
        """When ALL parent streams are inaccessible, raise CopperForbiddenError."""
        schemas = {"companies": {}, "people": {}}
        field_metadata = {"companies": [], "people": []}

        mock_streams = {
            "companies": _make_stream_cls("", False),
            "people": _make_stream_cls("", False),
        }

        with patch("tap_copper.discover.STREAMS", mock_streams):
            with pytest.raises(CopperForbiddenError) as exc_info:
                _apply_access_checks(mock_client, schemas, field_metadata)

        assert "do not have 'read' access to any" in str(exc_info.value)

    def test_child_streams_not_probed(self, mock_client):
        """Child streams should not be probed directly; access governed by parent."""
        schemas = {"companies": {}, "child_stream": {}}
        field_metadata = {"companies": [], "child_stream": []}

        mock_streams = {
            "companies": _make_stream_cls("", True),
            "child_stream": _make_stream_cls("companies", True),
        }

        with patch("tap_copper.discover.STREAMS", mock_streams):
            _apply_access_checks(mock_client, schemas, field_metadata)

        # Both should remain (parent accessible, child not probed)
        assert "companies" in schemas
        assert "child_stream" in schemas


class TestPruneInaccessibleChildren:
    """Tests for _prune_inaccessible_children."""

    def test_children_removed_when_parent_excluded(self):
        """Child streams are removed when their parent is not in schemas."""
        schemas = {"child_stream": {}}
        field_metadata = {"child_stream": []}

        mock_streams = {
            "child_stream": _make_stream_cls("companies", True),
        }

        with patch("tap_copper.discover.STREAMS", mock_streams):
            _prune_inaccessible_children(schemas, field_metadata)

        assert "child_stream" not in schemas
        assert "child_stream" not in field_metadata

    def test_children_kept_when_parent_present(self):
        """Child streams are kept when their parent is in schemas."""
        schemas = {"companies": {}, "child_stream": {}}
        field_metadata = {"companies": [], "child_stream": []}

        mock_streams = {
            "companies": _make_stream_cls("", True),
            "child_stream": _make_stream_cls("companies", True),
        }

        with patch("tap_copper.discover.STREAMS", mock_streams):
            _prune_inaccessible_children(schemas, field_metadata)

        assert "child_stream" in schemas


class TestDiscover:
    """Tests for discover() function."""

    @patch("tap_copper.discover.get_schemas")
    @patch("tap_copper.discover._apply_access_checks")
    def test_discover_returns_catalog(self, mock_access_checks, mock_get_schemas, mock_client):
        """discover() should return a Catalog with entries for accessible streams."""
        mock_get_schemas.return_value = (
            {"companies": {"type": "object", "properties": {"id": {"type": "integer"}}}},
            {"companies": [{"breadcrumb": (), "metadata": {"table-key-properties": ["id"]}}]},
        )
        mock_access_checks.return_value = None

        catalog = discover(client=mock_client)

        assert len(catalog.streams) == 1
        assert catalog.streams[0].stream == "companies"
        mock_access_checks.assert_called_once()

    @patch("tap_copper.discover.get_schemas")
    def test_discover_raises_when_all_forbidden(self, mock_get_schemas, mock_client):
        """discover() should raise when _apply_access_checks raises."""
        mock_get_schemas.return_value = (
            {"companies": {"type": "object", "properties": {}}},
            {"companies": []},
        )

        with patch(
            "tap_copper.discover._apply_access_checks",
            side_effect=CopperForbiddenError("No access"),
        ):
            with pytest.raises(CopperForbiddenError):
                discover(client=mock_client)
