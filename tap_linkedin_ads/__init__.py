#!/usr/bin/env python3

import sys
import json
import argparse
import singer
from singer import metadata, utils
from tap_linkedin_ads.client import LinkedinClient, REQUEST_TIMEOUT
from tap_linkedin_ads.discover import discover as _discover
from tap_linkedin_ads.sync import sync as _sync


LOGGER = singer.get_logger()
REQUEST_TIMEOUT = 300

REQUIRED_CONFIG_KEYS = [
    'access_token',
    'user_agent'
]


def do_discover(client):
    LOGGER.info('Starting discover')
    client.check_accounts(client.get_config())
    catalog = _discover()
    json.dump(catalog.to_dict(), sys.stdout, indent=2)
    LOGGER.info('Finished discover')


@singer.utils.handle_top_exception(LOGGER)
def main():
    parsed_args = singer.utils.parse_args(REQUIRED_CONFIG_KEYS)

    with LinkedinClient(parsed_args.config.get('client_id', None),
                        parsed_args.config.get('client_secret', None),
                        parsed_args.config.get('refresh_token', None),
                        parsed_args.config.get('access_token'),
                        parsed_args.config_path,
                        REQUEST_TIMEOUT,
                        parsed_args.config['user_agent']
                        ) as client:

        state = {}
        if parsed_args.state:
            state = parsed_args.state
        if parsed_args.discover:
            do_discover(client)
        elif parsed_args.catalog:
            _sync(client=client,
                  catalog=parsed_args.catalog,
                  state=state)


if __name__ == '__main__':
    main()
