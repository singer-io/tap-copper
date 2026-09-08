from tap_copper.streams.abstracts import FullTableStream


class PipelineStages(FullTableStream):
    tap_stream_id = "pipeline_stages"
    key_properties = ["id"]
    replication_method = "FULL_TABLE"

    http_method = "GET"
    path = "pipeline_stages"
    data_key = None
