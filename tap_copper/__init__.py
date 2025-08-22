"""
Main entry point for the tap-copper Singer tap.

Handles discovery (--discover) and sync (with a provided catalog).
"""

import json
import sys
from typing import Any, Dict

import singer
from tap_copper.client import Client
from tap_copper.discover import discover
from tap_copper.sync import sync

LOGGER = singer.get_logger()

REQUIRED_CONFIG_KEYS = ["api_key", "user_email"]


def do_discover() -> None:
    """Discover and emit the catalog to stdout."""
    LOGGER.info("Starting discover")
    catalog = discover()
    json.dump(catalog.to_dict(), sys.stdout, indent=2)
    sys.stdout.write("\n")
    LOGGER.info("Finished discover")


@singer.utils.handle_top_exception(LOGGER)
def main() -> None:
    """Parse command-line arguments and run discovery or sync mode."""
    parsed_args = singer.utils.parse_args(REQUIRED_CONFIG_KEYS)
    state: Dict[str, Any] = parsed_args.state or {}

    with Client(parsed_args.config) as client:
        if parsed_args.discover:
            do_discover()
            return

        if parsed_args.catalog:
            sync(
                client=client,
                config=parsed_args.config,
                catalog=parsed_args.catalog,
                state=state,
            )
            return

        raise SystemExit("No mode specified: use --discover or provide a catalog for sync.")


if __name__ == "__main__":
    main()
