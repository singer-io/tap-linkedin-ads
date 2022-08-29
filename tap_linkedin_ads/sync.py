import singer
from tap_linkedin_ads.streams import STREAMS, write_bookmark

LOGGER = singer.get_logger()

LOOKBACK_WINDOW = 7
DATE_WINDOW_SIZE = 30 # days
PAGE_SIZE = 100

def update_currently_syncing(state, stream_name):
    """
    Currently syncing sets the stream currently being delivered in the state.
    If the integration is interrupted, this state property is used to identify the
    starting point to continue from.

    Reference: https://github.com/singer-io/singer-python/blob/master/singer/bookmarks.py#L41-L46
    """
    if (stream_name is None) and ('currently_syncing' in state):
        # Remove the existing currently_syncing stream from the state for the complete sync
        del state['currently_syncing']
    else:
        # Set currently_syncing stream
        singer.set_currently_syncing(state, stream_name)
    singer.write_state(state)

def get_streams_to_sync(selected_streams):
    """
    Get lists of streams to call the sync method.
    For children, ensure that dependent parent_stream is included even if it is not selected.
    """
    streams_to_sync = []

    # Loop thru all selected streams
    for stream_name in selected_streams:
        stream_obj = STREAMS[stream_name]
        # If the stream has a parent_stream, then it is a child stream
        parent_stream = hasattr(stream_obj, 'parent') and stream_obj.parent

        # Append selected parent streams
        if not parent_stream:
            streams_to_sync.append(stream_name)
        else:
            # Append un-selected parent streams of selected children
            if parent_stream not in selected_streams and parent_stream not in streams_to_sync:
                streams_to_sync.append(parent_stream)

    return streams_to_sync

def get_page_size(config):
    """
    Get page size from config.
    Return the default value if an empty string is given and raise an exception if an invalid value is given.
    """
    page_size = config.get('page_size', PAGE_SIZE)
    if page_size == "":
        return PAGE_SIZE
    try:
        if isinstance(page_size, float):
            raise Exception

        page_size = int(page_size)
        if page_size <= 0:
            # Raise an exception if negative page_size is given in the config.
            raise Exception
        return page_size
    except Exception:
        raise Exception("The entered page size ({}) is invalid".format(page_size))

def sync(client, config, catalog, state):
    """
    sync selected streams.
    """
    start_date = config['start_date']
    page_size = get_page_size(config)

    if config.get('date_window_size'):
        LOGGER.info('Using non-standard date window size of %s', config.get('date_window_size'))
        date_window_size = int(config.get('date_window_size'))
    else:
        date_window_size = DATE_WINDOW_SIZE
        LOGGER.info('Using standard date window size of %s', DATE_WINDOW_SIZE)

    # Get ALL selected streams from catalog
    selected_streams = []
    for stream in catalog.get_selected_streams(state):
        selected_streams.append(stream.stream)
    LOGGER.info('selected_streams: %s', selected_streams)

    if not selected_streams:
        return

    # last_stream = Previous currently synced stream, if the load was interrupted
    last_stream = singer.get_currently_syncing(state)
    LOGGER.info('last/currently syncing stream: %s', last_stream)

    # Get the list of streams(to sync stream itself or its child stream) for which
    # sync method needs to be called
    stream_to_sync = get_streams_to_sync(selected_streams)

    # Loop through all `stream_to_sync` streams
    for stream_name in stream_to_sync:
        stream_obj = STREAMS[stream_name]()

        # Add appropriate account_filter query parameters based on account_filter type
        account_filter = stream_obj.account_filter
        if config.get("accounts") and account_filter is not None:
            account_list = config['accounts'].replace(" ", "").split(",")
            params = stream_obj.params
            for idx, account in enumerate(account_list):
                if account_filter == 'search_id_values_param':
                    params['search.id.values[{}]'.format(idx)] = int(account)
                elif account_filter == 'search_account_values_param':
                    params['search.account.values[{}]'.format(idx)] = \
                        'urn:li:sponsoredAccount:{}'.format(account)
                elif account_filter == 'accounts_param':
                    params['accounts[{}]'.format(idx)] = \
                        'urn:li:sponsoredAccount:{}'.format(account)

            # Update params of specific stream
            stream_obj.params = params

        LOGGER.info('START Syncing: %s', stream_name)
        update_currently_syncing(state, stream_name)

        # Write schema for parent streams
        if stream_name in selected_streams:
            stream_obj.write_schema(catalog)

        total_records, max_bookmark_value = stream_obj.sync_endpoint(
            client=client, catalog=catalog,
            state=state,
            page_size=page_size,
            start_date=start_date,
            selected_streams=selected_streams,
            date_window_size=date_window_size)

        # Write parent stream's bookmarks
        if stream_obj.replication_keys and stream_name in selected_streams:
            write_bookmark(state, max_bookmark_value, stream_name)

        update_currently_syncing(state, None)
        LOGGER.info('Synced: %s, total_records: %s', stream_name, total_records)
        LOGGER.info('FINISHED Syncing: %s', stream_name)
