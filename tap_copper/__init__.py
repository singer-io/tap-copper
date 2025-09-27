import json
import sys
import argparse

import singer
from singer.catalog import Catalog

from tap_copper.discover import discover as discover_with_config
from tap_copper.client import Client
from tap_copper.sync import sync

LOGGER = singer.get_logger()


def _load_json_file(path: str):
    with open(path, "r", encoding="utf-8") as fh:
        return json.load(fh)


def parse_args(argv=None):
    parser = argparse.ArgumentParser(prog="tap-copper")
    parser.add_argument("--config", help="Config file", required=False)
    parser.add_argument("--discover", action="store_true", help="Run discovery")
    parser.add_argument("--catalog", help="Catalog file", required=False)
    parser.add_argument("--state", help="State file", required=False)
    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)

    config = {}
    if args.config:
        config = _load_json_file(args.config)

    if args.discover:
        LOGGER.info("DISCOVERY STARTED")
        catalog = discover_with_config(config=config)
        catalog.dump()
        LOGGER.info("DISCOVERY FINISHED")
        return 0

    if not args.catalog:
        LOGGER.error("Catalog is required for sync. Use --catalog <file>.")
        return 1

    with open(args.catalog, "r", encoding="utf-8") as fh:
        catalog = Catalog.from_dict(json.load(fh))

    state = {}
    if args.state:
        state = _load_json_file(args.state)

    client = Client(config)
    sync(client=client, config=config, catalog=catalog, state=state)

    return 0


if __name__ == "__main__":
    sys.exit(main())
