#!/usr/bin/env python3

import sys
import json
import singer
from tap_linkedin_ads.client import LinkedinClient
from tap_linkedin_ads.discover import discover
from tap_linkedin_ads.sync import sync

LOGGER = singer.get_logger()

REQUIRED_CONFIG_KEYS = [
    'start_date',
    'user_agent',
    'accounts',
    'client_id',
    'client_secret',
    'refresh_token',
]

def do_discover():

    LOGGER.info('Starting discover')
    catalog = discover()
    json.dump(catalog.to_dict(), sys.stdout, indent=2)
    LOGGER.info('Finished discover')


@singer.utils.handle_top_exception(LOGGER)
def main():

    parsed_args = singer.utils.parse_args(REQUIRED_CONFIG_KEYS)

    with LinkedinClient(
        client_id=parsed_args.config['client_id'],
        client_secret=parsed_args.config['client_secret'],
        refresh_token=parsed_args.config['refresh_token'],
        user_agent=parsed_args.config['user_agent'],
    ) as client:
        state = {}
        if parsed_args.state:
            state = parsed_args.state

        if parsed_args.discover:
            do_discover()
        elif parsed_args.catalog:
            sync(client=client,
                 config=parsed_args.config,
                 catalog=parsed_args.catalog,
                 state=state)

if __name__ == '__main__':
    main()
