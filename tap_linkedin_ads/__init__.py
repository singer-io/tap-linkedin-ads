#!/usr/bin/env python3

import sys
import json
import argparse
import singer
from singer import metadata, utils
from tap_linkedin_ads.client import LinkedinClient
from tap_linkedin_ads.discover import discover
from tap_linkedin_ads.sync import sync

LOGGER = singer.get_logger()

REQUIRED_CONFIG_KEYS = [
    'start_date',
    'user_agent',
    'access_token',
    'accounts'
]

def do_discover():

    LOGGER.info('Starting discover')
    catalog = discover()
    json.dump(catalog.to_dict(), sys.stdout, indent=2)
    LOGGER.info('Finished discover')


@singer.utils.handle_top_exception(LOGGER)
def main():

    parsed_args = singer.utils.parse_args(REQUIRED_CONFIG_KEYS)

    with LinkedinClient(parsed_args.config['user_agent'],
                        parsed_args.config['access_token']) as client:
        state = {}
        if parsed_args.state:
            state = parsed_args.state

        if parsed_args.discover:
            do_discover()
        elif parsed_args.catalog:
            sync(client=client,
                 catalog=parsed_args.catalog,
                 state=state,
                 start_date=parsed_args.config['start_date'])

if __name__ == '__main__':
    main()
