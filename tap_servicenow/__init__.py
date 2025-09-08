import sys
import json
import singer
from tap_servicenow.client import Client
from tap_servicenow.discover import discover
from tap_servicenow.sync import sync

LOGGER = singer.get_logger()

REQUIRED_CONFIG_KEYS = ['instance', 'user', 'password']


def do_discover(client):
    """
    Discover and emit the catalog to stdout.
    """
    LOGGER.info("Starting discover")
    catalog = discover(client)
    json.dump(catalog.to_dict(), sys.stdout, indent=2)
    LOGGER.info("Finished discover")


@singer.utils.handle_top_exception(LOGGER)
def main():
    """
    Run the tap
    """
    parsed_args = singer.utils.parse_args(REQUIRED_CONFIG_KEYS)
    state = parsed_args.state or {}

    with Client(parsed_args.config) as client:
        if parsed_args.discover:
            do_discover(client)
        elif parsed_args.catalog:
            sync(
                client=client,
                config=parsed_args.config,
                catalog=parsed_args.catalog,
                state=state
            )


if __name__ == "__main__":
    main()
