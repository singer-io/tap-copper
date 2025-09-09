# tests/unittests/test_abstracts.py

from __future__ import annotations
from typing import Dict, List, Any, Iterable
import pytest
import singer
from singer import metadata

from tap_copper.streams.abstracts import IncrementalStream


# ---------- Fixtures ----------
@pytest.fixture
def mock_client():
    """Tiny client stub with integer start_date."""
    class C:
        config = {"start_date": 0}
    return C()


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
                        "date_modified": {"type": ["integer", "null"]},
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


# ---------- Concrete tiny stream for tests ----------
class TinyIncrementalStream(IncrementalStream):
    tap_stream_id = "tiny"
    key_properties = ["id"]
    replication_method = "INCREMENTAL"
    replication_keys = ["date_modified"]

    http_method = "POST"
    path = "tiny/search"
    page_size = 3  # to exercise pagination

    def __init__(self, client, catalog, records: List[Dict[str, Any]]):
        super().__init__(client, catalog)
        self._records_source = list(records)
        self.is_selected = lambda: True  # force selection

    # Bookmark helpers -> always numeric
    def get_bookmark(self, state: Dict, tap_stream_id: str, key: str | None = None):
        value = state.get("bookmarks", {}).get(tap_stream_id, {}).get(
            key or self.replication_keys[0]
        )
        if isinstance(value, (int, float)):
            return int(value)
        return int(self.client.config.get("start_date", 0))

    def write_bookmark(self, state: Dict, tap_stream_id: str, value: int):
        state.setdefault("bookmarks", {}).setdefault(tap_stream_id, {})[
            self.replication_keys[0]
        ] = int(value)
        return state

    def get_records(self) -> Iterable[Dict[str, Any]]:
        src = self._records_source
        for i in range(0, len(src), self.page_size):
            for rec in src[i : i + self.page_size]:
                yield rec

    def sync(self, state: Dict, transformer, parent_obj=None) -> int:
        bm_key = self.replication_keys[0]
        threshold = self.get_bookmark(state, self.tap_stream_id, bm_key)
        current_max = None
        emitted = 0

        with singer.metrics.record_counter(self.tap_stream_id):
            for rec in self.get_records():
                tr = transformer.transform(rec, self.schema, self.metadata)
                val = tr.get(bm_key)

                if val is None:
                    continue

                if val >= threshold:
                    singer.write_record(self.tap_stream_id, tr)
                    emitted += 1
                    if current_max is None or val > current_max:
                        current_max = val

        new_bookmark = current_max if current_max is not None else threshold
        self.write_bookmark(state, self.tap_stream_id, new_bookmark)
        return emitted


def build_stream(mock_client, mock_catalog, records: List[Dict[str, Any]]) -> TinyIncrementalStream:
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
                {"id": 2, "date_modified": 100},
                {"id": 3, "date_modified": 150},
                {"id": 4, "date_modified": None},
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
            200,
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
    case_name, state, records, expected_ids, expected_bookmark, mock_client, mock_catalog, passthrough_transformer, monkeypatch
):
    stream = build_stream(mock_client, mock_catalog, records)

    captured: List[Dict[str, Any]] = []

    def _capture_write_record(_stream_name, record):
        captured.append(record)

    monkeypatch.setattr("singer.write_record", _capture_write_record)

    count = stream.sync(state, transformer=passthrough_transformer)

    emitted_ids = [r["id"] for r in captured]
    assert emitted_ids == expected_ids, f"{case_name}: unexpected emitted ids"
    assert count == len(expected_ids), f"{case_name}: record counter mismatch"
    assert state["bookmarks"]["tiny"]["date_modified"] == expected_bookmark, f"{case_name}: bookmark mismatch"


def test_get_bookmark_and_write_bookmark_roundtrip(mock_client, mock_catalog):
    stream = build_stream(mock_client, mock_catalog, records=[])
    state: Dict[str, Any] = {}

    bm = stream.get_bookmark(state, stream.tap_stream_id, stream.replication_keys[0])
    assert bm == 0

    stream.write_bookmark(state, stream.tap_stream_id, 1234)
    bm2 = stream.get_bookmark(state, stream.tap_stream_id, stream.replication_keys[0])
    assert bm2 == 1234
