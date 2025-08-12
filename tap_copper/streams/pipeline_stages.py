from tap_copper.streams.abstracts import FullTableStream

class PipelineStages(FullTableStream):
    tap_stream_id = "pipeline_stages"
    key_properties = ["id"]
    replication_method = "FULL_TABLE"
    data_key = "root"
    path = "pipeline_stages"
    path = "pipelines"
