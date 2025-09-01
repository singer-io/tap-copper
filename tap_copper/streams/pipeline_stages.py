from tap_copper.streams.abstracts import FullTableStream

class PipelineStages(FullTableStream):
    tap_stream_id = "pipeline_stages"
    key_properties = ["id"]
    replication_method = "FULL_TABLE"

    http_method = "POST"
    path = "pipeline_stages/search"
    data_key = None
    page_size = 200

    def get_url_endpoint(self, parent_obj=None):
        # Plain search endpoint (no {} formatting)
        return f"{self.client.base_url}/{self.path}"

    def sync(self, state, transformer, parent_obj=None):
        # Build body; if called as a child, include the parent pipeline id
        self.params.clear()
        self.data_payload.clear()
        body = {
            "page_size": self.page_size,
            "page_number": 1,
        }
        if parent_obj and "id" in parent_obj:
            body["pipeline_id"] = parent_obj["id"]
        self.update_data_payload(**body)
        self.url_endpoint = self.get_url_endpoint(parent_obj)

        from singer import metrics, write_record
        with metrics.record_counter(self.tap_stream_id) as counter:
            for rec in self.get_records():
                tr = transformer.transform(rec, self.schema, self.metadata)
                if self.is_selected():
                    write_record(self.tap_stream_id, tr)
                    counter.increment()
            return counter.value

    def get_records(self):
        while True:
            resp = self.client.make_request(
                self.http_method,
                self.url_endpoint,
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
