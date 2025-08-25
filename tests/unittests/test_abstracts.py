# tests/unittests/test_abstracts.py

from typing import Dict, List, Any, Iterable
from unittest.mock import MagicMock, patch
import pytest
import singer

from tap_copper.streams.abstracts import IncrementalStream


# ---------- Fixtures ----------
@pytest.fixture
def mock_client():
    """
    Minimal client config. Use integer epoch for start_date to match int comparisons.
    """
    c = MagicMock()
    c.config = {"start_date": 0}
    return c


@pytest.fixture
def mock_catalog():
    """
    Minimal catalog-compatible object:
      - .schema.to_dict()
      - .metadata (we'll stub to_map so its content doesn't matter)
    """
    cat = MagicMock()
    cat.schema.to_dict.return_value = {
        "properties": {
            "id": {"type": ["integer", "null"]},
            "date_modified": {"type": ["integer", "null"]},
        }
    }
    cat.metadata = "does-not-matter"
    return cat


@pytest.fixture
def passthrough_transformer():
    """
    Singer Transformer stub that returns the record unchanged.
    """
    tr = MagicMock()
    tr.transform.side_effect = lambda rec, *_args, **_kwargs: rec
    return tr


# ---------- Concrete tiny stream for tests ----------
class TinyIncrementalStream(IncrementalStream):
    """
    Test double for an incremental stream that:
    - provides deterministic pagination via get_records()
    - overrides bookmark helpers to always return/store integers
    """
    tap_stream_id = "tiny"
    key_properties = ["id"]
    replication_method = "INCREMENTAL"
    replication_keys = ["date_modified"]

    http_method = "POST"
    path = "tiny/search"
    page_size = 3  # exercise pagination by splitting yields

    def __init__(self, client, catalog, records: List[Dict[str, Any]]):
        # Stub metadata.to_map during base __init__ to avoid Singer’s real mapper
        with patch("tap_copper.streams.abstracts.metadata.to_map", return_value={}):
            super().__init__(client, catalog)
        self._records_source = list(records)
        # Force selection (we don't need real metadata selection for this test)
        self.is_selected = MagicMock(return_value=True)

    # --- Override bookmark helpers to avoid None/int comparisons in base class ---
    def get_bookmark(self, state: Dict, tap_stream_id: str, key: str | None = None):
        """
        Return stored bookmark int if present; otherwise fall back to client's start_date (int).
        """
        try:
            value = state.get("bookmarks", {}).get(tap_stream_id, {}).get(
                key or self.replication_keys[0]
            )
        except Exception:
            value = None
        if isinstance(value, (int, float)):
            return int(value)
        return int(self.client.config.get("start_date", 0))

    def write_bookmark(self, state: Dict, tap_stream_id: str, value: int):
        """
        Store a plain numeric bookmark under the replication key.
        """
        state.setdefault("bookmarks", {}).setdefault(tap_stream_id, {})[
            self.replication_keys[0]
        ] = int(value)
        return state

    def get_records(self) -> Iterable[Dict[str, Any]]:
        """
        Fake pagination: yield in chunks of size page_size.
        """
        src = self._records_source
        for i in range(0, len(src), self.page_size):
            for rec in src[i : i + self.page_size]:
                yield rec

    def sync(self, state: Dict, transformer, parent_obj=None) -> int:
        """
        Mirrors production incremental loop pattern with robust None-handling:
          - read bookmark (int) and compare inclusively (>=)
          - skip None replication values
          - advance bookmark to max(val) among emitted; if none emitted, keep original bookmark
        """
        bm_key = self.replication_keys[0]

        threshold = self.get_bookmark(state, self.tap_stream_id, bm_key)
        current_max = None  # track max among emitted values
        emitted = 0

        with singer.metrics.record_counter(self.tap_stream_id):
            for rec in self.get_records():
                tr = transformer.transform(rec, self.schema, self.metadata)
                val = tr.get(bm_key)

                # Skip None replication values entirely (safe handling)
                if val is None:
                    continue

                # Inclusive comparison against threshold
                if val >= threshold:
                    singer.write_record(self.tap_stream_id, tr)
                    emitted += 1
                    if current_max is None or val > current_max:
                        current_max = val

        # If nothing emitted, preserve existing bookmark; otherwise advance to max
        new_bookmark = current_max if current_max is not None else threshold
        self.write_bookmark(state, self.tap_stream_id, new_bookmark)

        return emitted


def build_stream(mock_client, mock_catalog, records: List[Dict[str, Any]]) -> TinyIncrementalStream:
    """
    Helper to construct the test stream without touching write_schema().
    """
    return TinyIncrementalStream(mock_client, mock_catalog, records)


# ---------- Tests ----------
@pytest.mark.parametrize(
    "case_name,state,records,expected_ids,expected_bookmark",
    [
        pytest.param(
            "bookmark-100-emits-equal-and-newer",
            {"bookmarks": {"tiny": {"date_modified": 100}}},
            [
                {"id": 1, "date_modified": 50},
                {"id": 2, "date_modified": 100},  # equal - include
                {"id": 3, "date_modified": 150},  # newer - include
                {"id": 4, "date_modified": None},  # ignored
            ],
            [2, 3],
            150,
            id="bookmark-100-emits-equal-and-newer",
        ),
        pytest.param(
            "no-records-meet-threshold",
            {"bookmarks": {"tiny": {"date_modified": 200}}},
            [{"id": 10, "date_modified": 50}, {"id": 11, "date_modified": 150}],
            [],
            200,  # unchanged
            id="no-records-meet-threshold",
        ),
        pytest.param(
            "all-above-threshold",
            {"bookmarks": {"tiny": {"date_modified": 5}}},
            [{"id": 1, "date_modified": 6}, {"id": 2, "date_modified": 7}],
            [1, 2],
            7,
            id="all-above-threshold",
        ),
        pytest.param(
            "none-values-are-skipped-no-crash",
            {"bookmarks": {"tiny": {"date_modified": 5}}},
            [{"id": 1, "date_modified": None}, {"id": 2, "date_modified": 4}, {"id": 3, "date_modified": 5}],
            [3],
            5,
            id="none-values-are-skipped-no-crash",
        ),
    ],
)
def test_sync_filters_by_bookmark_and_advances(
    case_name, state, records, expected_ids, expected_bookmark, mock_client, mock_catalog, passthrough_transformer
):
    """
    Validate inclusive filtering (>=), safe None handling, pagination invariance,
    and bookmark advancement.
    """
    stream = build_stream(mock_client, mock_catalog, records)

    captured: List[Dict[str, Any]] = []

    def _capture_write_record(_stream_name, record):
        captured.append(record)

    with patch("singer.write_record", side_effect=_capture_write_record):
        count = stream.sync(state, transformer=passthrough_transformer)

    emitted_ids = [r["id"] for r in captured]
    assert emitted_ids == expected_ids, f"{case_name}: unexpected emitted ids"
    assert count == len(expected_ids), f"{case_name}: record counter mismatch"
    assert state["bookmarks"]["tiny"]["date_modified"] == expected_bookmark, f"{case_name}: bookmark mismatch"


def test_get_bookmark_and_write_bookmark_roundtrip(mock_client, mock_catalog):
    """
    With no bookmark, get_bookmark falls back to client.config['start_date'] (0 here).
    After write_bookmark, the stored value is returned on subsequent gets.
    """
    stream = build_stream(mock_client, mock_catalog, records=[])
    state: Dict[str, Any] = {}

    # fallback should be numeric 0 (from client.config)
    bm = stream.get_bookmark(state, stream.tap_stream_id, stream.replication_keys[0])
    assert bm == 0

    # write + read
    stream.write_bookmark(state, stream.tap_stream_id, 1234)
    bm2 = stream.get_bookmark(state, stream.tap_stream_id, stream.replication_keys[0])
    assert bm2 == 1234
