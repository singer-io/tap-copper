from tap_copper.streams.abstracts import ChildBaseStream
from singer import metrics, write_record

class Opportunities(ChildBaseStream):
    tap_stream_id = "opportunities"
    key_properties = ["id"]
    replication_method = "INCREMENTAL"
    replication_keys = ["date_modified"]

    http_method = "POST"
    path = "opportunities/search"   # filter via body: company_ids=[...]
    data_key = None                 # top-level array
    page_size = 200

    def get_url_endpoint(self, parent_obj=None):
        return f"{self.client.base_url}/{self.path}"

    def sync(self, state, transformer, parent_obj=None):
        bm = self.get_bookmark(state, self.tap_stream_id)
        current_max = bm

        body = {
            "page_size": self.page_size,
            "page_number": 1,
            "sort_by": "date_modified",
            "sort_direction": "asc",
            "minimum_modified_date": bm,
        }
        # >>> Keep consistent with People: array form
        if parent_obj and "id" in parent_obj:
            body["company_ids"] = [parent_obj["id"]]

        self.params.clear()
        self.data_payload.clear()
        self.update_data_payload(**body)
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
        self.params.pop("", None)
        while True:
            resp = self.client.make_request(
                self.http_method,
                self.get_url_endpoint(),
                self.params,
                self.headers,
                body=self.data_payload,
                path=self.path,
            )
            items = resp if isinstance(resp, list) else []
            for it in items:
                yield it

            if len(items) < self.page_size:
                break

            next_page = int(self.data_payload.get("page_number", 1)) + 1
            self.update_data_payload(page_number=next_page)
