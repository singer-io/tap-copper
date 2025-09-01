from tap_copper.streams.abstracts import IncrementalStream
from singer import metrics, write_record

class Projects(IncrementalStream):
    tap_stream_id = "projects"
    key_properties = ["id"]
    replication_method = "INCREMENTAL"
    replication_keys = ["date_modified"]

    http_method = "POST"
    path = "projects/search"
    data_key = None
    page_size = 200

    def sync(self, state, transformer, parent_obj=None):
        bm = self.get_bookmark(state, self.tap_stream_id)
        current_max = bm
        self.params.clear()
        self.data_payload.clear()
        self.update_data_payload(
            page_size=self.page_size,
            page_number=1,
            sort_by="date_modified",
            sort_direction="asc",
            minimum_modified_date=bm
        )
        self.url_endpoint = self.get_url_endpoint(parent_obj)
        with metrics.record_counter(self.tap_stream_id) as counter:
            for rec in self.get_records():
                tr = transformer.transform(rec, self.schema, self.metadata)
                val = tr.get(self.replication_keys[0])
                if val is not None and val >= bm:
                    if self.is_selected():
                        write_record(self.tap_stream_id, tr)
                        counter.increment()
                    if val > current_max:
                        current_max = val
            state = self.write_bookmark(state, self.tap_stream_id, value=current_max)
            return counter.value

    def get_records(self):
        while True:
            resp = self.client.make_request(
                self.http_method, self.url_endpoint, self.params, self.headers, body=self.data_payload, path=self.path
            )
            items = resp if isinstance(resp, list) else []
            for it in items:
                yield it
            if len(items) < self.page_size:
                break
            self.update_data_payload(page_number=int(self.data_payload.get("page_number", 1)) + 1)
