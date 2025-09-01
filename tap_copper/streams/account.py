from tap_copper.streams.abstracts import FullTableStream

class Account(FullTableStream):
    tap_stream_id = "account"
    key_properties = ["id"]
    replication_method = "FULL_TABLE"

    http_method = "GET"
    path = "account"
    data_key = None

    def get_records(self):
        resp = self.client.make_request(
            self.http_method, self.get_url_endpoint(), self.params, self.headers, body=self.data_payload, path=self.path
        )
        if isinstance(resp, dict):
            yield resp
