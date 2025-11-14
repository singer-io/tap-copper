import json
import sys
from typing import Dict

import singer

from tap_copper.client import Client
from tap_copper.discover import discover
from tap_copper.sync import sync

LOGGER = singer.get_logger()

REQUIRED_CONFIG_KEYS = ['api_key', 'user_email', 'start_date']


def do_discover(config: Dict = None):
    """
    Discover and emit the catalog to stdout
    """
    LOGGER.info("Starting discover")
    catalog = discover(config=config)
    json.dump(catalog.to_dict(), sys.stdout, indent=2)
    LOGGER.info("Finished discover")


def main():

    parsed_args = singer.utils.parse_args(REQUIRED_CONFIG_KEYS)
    state = {}
    if parsed_args.state:
        state = parsed_args.state

    with Client(parsed_args.config) as client:
        if parsed_args.discover:
            do_discover(config=parsed_args.config)
        elif parsed_args.catalog:
            sync(
                client=client,
                config=parsed_args.config,
                catalog=parsed_args.catalog,
                state=state)


if __name__ == "__main__":
    main()
