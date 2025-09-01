from tap_copper.streams.abstracts import FullTableStream

class Users(FullTableStream):
    tap_stream_id = "users"
    key_properties = ["id"]
    replication_method = "FULL_TABLE"

    http_method = "POST"
    path = "users/search"
    data_key = None
    page_size = 200

    def get_records(self):
        self.params.clear()
        self.data_payload.clear()
        self.update_data_payload(page_size=self.page_size, page_number=1)

        while True:
            resp = self.client.make_request(
                self.http_method, self.get_url_endpoint(), self.params, self.headers, body=self.data_payload, path=self.path
            )
            items = resp if isinstance(resp, list) else []
            for it in items:
                yield it

            if len(items) < self.page_size:
                break
            self.update_data_payload(page_number=int(self.data_payload.get("page_number", 1)) + 1)
