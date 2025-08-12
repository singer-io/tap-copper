from tap_copper.streams.abstracts import FullTableStream

class LossReasons(FullTableStream):
    tap_stream_id = "loss_reasons"
    key_properties = ["id"]
    replication_method = "FULL_TABLE"
    data_key = "root"
    path = "loss_reasons"
