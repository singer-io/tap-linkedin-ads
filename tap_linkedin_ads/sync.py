import singer
from tap_linkedin_ads.streams import STREAMS, write_bookmark
from datetime import datetime, timedelta

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

def get_date_windows(start_date, end_date, granularity):
    """
    Generate date windows based on granularity
    """
    start = datetime.strptime(start_date, '%Y-%m-%d')
    end = datetime.strptime(end_date, '%Y-%m-%d')
    windows = []
    
    if granularity == 'DAILY':
        current = start
        while current <= end:
            windows.append({
                'start': current.strftime('%Y-%m-%d'),
                'end': current.strftime('%Y-%m-%d')
            })
            current += timedelta(days=1)
    elif granularity == 'MONTHLY':
        current = start.replace(day=1)
        while current <= end:
            month_end = (current.replace(day=1) + timedelta(days=32)).replace(day=1) - timedelta(days=1)
            windows.append({
                'start': current.strftime('%Y-%m-%d'),
                'end': min(month_end, end).strftime('%Y-%m-%d')
            })
            current = (current.replace(day=1) + timedelta(days=32)).replace(day=1)
    elif granularity == 'YEARLY':
        current = start.replace(month=1, day=1)
        while current <= end:
            year_end = current.replace(month=12, day=31)
            windows.append({
                'start': current.strftime('%Y-%m-%d'),
                'end': min(year_end, end).strftime('%Y-%m-%d')
            })
            current = current.replace(year=current.year + 1)
            
    return windows

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
            if len(account_list) > 0:
                params = stream_obj.params
                if account_filter == 'search_id_values_param':
                    # Convert account IDs to URN format
                    urn_list = ["urn%3Ali%3AsponsoredAccount%3A{}".format(account_id) for account_id in account_list]
                    # Create the query parameter string
                    param_value = "(id:(values:List({})))".format(','.join(urn_list))
                    params['search'] = param_value
                elif account_filter == 'accounts_param':
                    for idx, account in enumerate(account_list):
                        params['accounts[{}]'.format(idx)] = \
                            'urn:li:sponsoredAccount:{}'.format(account)
                # Update params of specific stream
                stream_obj.params = params

        LOGGER.info('START Syncing: %s', stream_name)
        update_currently_syncing(state, stream_name)

        # Write schema for parent streams
        if stream_name in selected_streams:
            stream_obj.write_schema(catalog)

        # Get time granularity from config
        time_granularity = config.get('time_granularity', 'MONTHLY').upper()
        if time_granularity not in VALID_TIME_GRANULARITIES:
            raise Exception(f"Invalid time_granularity: {time_granularity}. Must be one of {VALID_TIME_GRANULARITIES}")

        # Generate date windows based on granularity
        date_windows = get_date_windows(start_date, datetime.now().strftime('%Y-%m-%d'), time_granularity)
        LOGGER.info('Generated %s date windows for granularity %s', len(date_windows), time_granularity)

        total_records, max_bookmark_value = stream_obj.sync_endpoint(
            client=client, catalog=catalog,
            state=state, page_size=page_size,
            start_date=start_date,
            selected_streams=selected_streams,
            date_window_size=date_window_size,
            account_list=account_list)

        # Write parent stream's bookmarks
        if stream_obj.replication_keys and stream_name in selected_streams:
            write_bookmark(state, max_bookmark_value, stream_name)

        update_currently_syncing(state, None)
        LOGGER.info('Synced: %s, total_records: %s', stream_name, total_records)
        LOGGER.info('FINISHED Syncing: %s', stream_name)
