from tap_copper.streams.abstracts import FullTableStream

class LossReasons(FullTableStream):
    tap_stream_id = "loss_reasons"
    key_properties = ["id"]
    replication_method = "FULL_TABLE"

    http_method = "GET"
    path = "loss_reasons"
    data_key = "loss_reasons"
