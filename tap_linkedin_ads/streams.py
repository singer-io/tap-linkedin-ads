import urllib.parse
import copy
import datetime
from datetime import timedelta
import singer
from singer import metrics, metadata, utils
from singer import Transformer, should_sync_field, UNIX_MILLISECONDS_INTEGER_DATETIME_PARSING
from singer.utils import strptime_to_utc, strftime
from tap_linkedin_ads.transform import transform_json, snake_case_to_camel_case

LOGGER = singer.get_logger()

# Below fields are a list of foreign keys(primary key of a parent) and replication keys that API can not accept in the parameters.
# We will skip these fields while passing selected fields in the API parameters.
FIELDS_UNAVAILABLE_FOR_AD_ANALYTICS = {
    'campaign',
    'campaignId',
    'startAt',
    'endAt',
    'creative',
    'creativeId',
}

def write_bookmark(state, value, stream_name):
    """
    Write the bookmark in the state corresponding to the stream.
    """
    if 'bookmarks' not in state:
        state['bookmarks'] = {}
    state['bookmarks'][stream_name] = value
    LOGGER.info('Write state for stream: %s, value: %s', stream_name, value)
    singer.write_state(state)

def selected_fields(catalog_for_stream):
    """
    Get all selected fields of given streams
    """
    mdata = metadata.to_map(catalog_for_stream.metadata)
    fields = catalog_for_stream.schema.properties.keys()

    selected_fields_list = []
    # Loop through all fields of the given stream
    for field in fields:
        field_metadata = mdata.get(('properties', field))
        if should_sync_field(field_metadata.get('inclusion'), field_metadata.get('selected')):
            selected_fields_list.append(field)

    return selected_fields_list

def split_into_chunks(fields, chunk_length):
    """
    Return list of chunk_length fields for total fields.
    Example:

    Args: fields = [1, 2, 3, 4, 5], chunk_length = 2
    Return: [[1, 2], [3, 4], [5]]
    """
    return (fields[x:x+chunk_length] for x in range(0, len(fields), chunk_length))

def sync_analytics_endpoint(client, stream_name, path, query_string):
    """
    Call API for analytics endpoint and return all pages of records.
    """
    page = 1
    next_url = 'https://api.linkedin.com/rest/{}?{}'.format(path, query_string)

    # Loop until the last page
    while next_url:
        LOGGER.info('URL for %s: %s', stream_name, next_url)

        data = client.get(url=next_url, endpoint=stream_name)
        yield data
        # Fetch next page
        next_url = get_next_url(data)

        LOGGER.info('%s: Synced page %s', stream_name, page)
        page = page + 1

def get_next_url(data):
    """
    Prepare and return the URL to fetch the next page of records.
    """
    next_url = None
    links = data.get('paging', {}).get('links', [])
    for link in links:
        rel = link.get('rel')
        if rel == 'next':
            href = link.get('href')
            if href:
                # url must be kept encoded for the creatives endpoint.
                # Ref - https://learn.microsoft.com/en-us/linkedin/marketing/integrations/ads/account-structure/create-and-manage-creatives?view=li-lms-2023-01&tabs=http#sample-request-3
                if "rest/creatives" in href:
                    return 'https://api.linkedin.com{}'.format(href)
                # Prepare next page URL
                next_url = 'https://api.linkedin.com{}'.format(urllib.parse.unquote(href))
    return next_url

def shift_sync_window(params, today, date_window_size, forced_window_size=None):
    """
    Move ahead date window by date_window_size and update params with the new date window.
    """
    current_end = datetime.date(
        year=params['dateRange.end.year'],
        month=params['dateRange.end.month'],
        day=params['dateRange.end.day'],
    )

    new_end = min(today, current_end + timedelta(days=(forced_window_size if forced_window_size else date_window_size)))

    new_params = {**params,
                  'dateRange.start.day': current_end.day,
                  'dateRange.start.month': current_end.month,
                  'dateRange.start.year': current_end.year,

                  'dateRange.end.day': new_end.day,
                  'dateRange.end.month': new_end.month,
                  'dateRange.end.year': new_end.year,}
    return current_end, new_end, new_params

def merge_responses(data):
    """
    Prepare map with key as primary key and value as the record itself for analytics streams.
    The primary key is a combination of pivotValue and start date fields value.
    Update existing records with the same primary key value.
    """
    full_records = {}
    # Loop through each page of data
    for page in data:
        # Loop through each record of the page
        for element in page:
            temp_start = element['dateRange']['start']
            temp_pivotValue = element['pivotValue']
            string_start = '{}-{}-{}'.format(temp_start['year'], temp_start['month'], temp_start['day'])
            primary_key = (temp_pivotValue, string_start)
            if primary_key in full_records:
                # Update existing record with same primary key
                full_records[primary_key].update(element)
            else:
                full_records[primary_key] = element
    return full_records

class LinkedInAds:
    """
    A base class representing tap-linkedin-ads streams
    properties:

        tap_stream_id        : stream name for the endpoint
        replicaiton_method   : replication method of given streams. Possible values: FULL_TABLE, INCREMENTAL
        replicaion_keys      : Replications keys for an incremental stream
        key_properties       : Primary keys for a given stream
        path                 : API endpoint relative path, when added to the base URL, creates the full path
        account_filter       : Method for Account filtering. Each uses a different query pattern/parameter:
            search_id_values_param, search_account_values_param, accounts_param
        params               : Query, sort, and other endpoint specific parameters
        data_key             : JSON element containing the records for the endpoint
        bookmark_query_field : Typically a date-time field is used for filtering the query
        bookmark_field       : Replication key field, typically a date-time, used for filtering the results
            and setting the state
        foreign_key          : Primary key of the Parent stream.
        children             : A collection of child endpoints (where the endpoint path includes the parent id)
        parent               : On each of the children, name of the parent stream

    """
    tap_stream_id = None
    replicaiton_method = None
    replication_keys = None
    key_properties = []
    foreign_key = None
    account_filter = None
    path = None
    parent = None
    data_key = None
    children = []
    count = None
    params = {}
    headers = {}
    def write_schema(self, catalog):
        """
        Write the schema for the selected stream.
        """
        stream = catalog.get_stream(self.tap_stream_id)
        schema = stream.schema.to_dict()
        try:
            singer.write_schema(self.tap_stream_id, schema, stream.key_properties)
        except OSError as err:
            LOGGER.info('OS Error writing schema for: %s', self.tap_stream_id)
            raise err

    def write_record(self, record, time_extracted):
        """
        Write the record for the selected stream.
        """
        try:
            singer.write_record(self.tap_stream_id, record, time_extracted=time_extracted)
        except OSError as err:
            LOGGER.info('OS Error writing record for: %s', self.tap_stream_id)
            LOGGER.info('record: %s', record)
            raise err

    def get_bookmark(self, state, default):
        """
        Return bookmark value if available in the state otherwise return start date
        """
        if (state is None) or ('bookmarks' not in state):
            return default
        return (
            state
            .get('bookmarks', {})
            .get(self.tap_stream_id, default)
        )

    # pylint: disable=too-many-arguments,too-many-locals
    def process_records(self,
                        catalog,
                        records,
                        time_extracted,
                        bookmark_field=None,
                        max_bookmark_value=None,
                        last_datetime=None,
                        parent_id=None):
        """
        Transform and write a record if the replication key value is greater than the last bookmark.
        Update maximum bookmark value to write in the state.
        """
        stream = catalog.get_stream(self.tap_stream_id)
        schema = stream.schema.to_dict()
        stream_metadata = metadata.to_map(stream.metadata)
        with metrics.record_counter(self.tap_stream_id) as counter:
            for record in records:
                # If child object, add parent_id to record
                if parent_id and self.parent:
                    record[self.parent + '_id'] = parent_id

                # Transform record for Singer.io
                with Transformer(integer_datetime_fmt=UNIX_MILLISECONDS_INTEGER_DATETIME_PARSING) \
                    as transformer:
                    transformed_record = transformer.transform(
                        record,
                        schema,
                        stream_metadata)
                    # Check replication key value if it is available in the record
                    if bookmark_field and (bookmark_field in transformed_record):
                        # Reset max_bookmark_value to new value if higher
                        if max_bookmark_value is None or strptime_to_utc(transformed_record[bookmark_field]) > strptime_to_utc(max_bookmark_value):
                            max_bookmark_value = transformed_record[bookmark_field]

                        last_dttm = strptime_to_utc(last_datetime)
                        bookmark_dttm = strptime_to_utc(transformed_record[bookmark_field])
                        # Keep only records whose bookmark is after the last_datetime
                        if bookmark_dttm >= last_dttm:
                            self.write_record(transformed_record, time_extracted=time_extracted)
                            counter.increment()
                    else:
                        # Write record if replication key is not available in the record
                        self.write_record(transformed_record, time_extracted=time_extracted)
                        counter.increment()

            return max_bookmark_value, counter.value

    # pylint: disable=too-many-branches,too-many-statements,too-many-arguments,too-many-locals
    def sync_endpoint(self,
                      client,
                      catalog,
                      state,
                      page_size,
                      start_date,
                      selected_streams,
                      date_window_size,
                      parent_id=None):
        """
        Sync a specific parent or child endpoint.
        """
        # Get the latest bookmark for the stream and set the last_datetime
        last_datetime = self.get_bookmark(state, start_date)
        max_bookmark_value = last_datetime
        LOGGER.info('%s: bookmark last_datetime = %s', self.tap_stream_id, max_bookmark_value)

        bookmark_field = next(iter(self.replication_keys))
        # Initialize child_max_bookmarks
        child_max_bookmarks = {}
        children = self.children
        # Loop through all children
        for child_stream_name in children:

            if child_stream_name in selected_streams:
                child_obj = STREAMS[child_stream_name]()
                # Write schema for each child stream
                child_obj.write_schema(catalog)
                child_bookmark_field = child_obj.replication_keys
                if child_bookmark_field:
                    child_last_datetime = child_obj.get_bookmark(state, start_date)
                    # Add the last bookmark of child stream in the `child_max_bookmarks` map
                    child_max_bookmarks[child_stream_name] = child_last_datetime

        # Pagination reference:
        # https://docs.microsoft.com/en-us/linkedin/shared/api-guide/concepts/pagination?context=linkedin/marketing/context
        # Each page has a "start" (offset value) and a "count" (batch size, number of records)
        # Increase the "start" by the "count" for each batch.
        # Continue until the "start" exceeds the total_records.
        start = 0 # Starting offset value for each batch API call
        total_records = 0
        page = 1

        endpoint_params = {
            'start': start,
            'count': page_size,
            **self.params # adds in endpoint specific, sort, filter params
        }

        querystring = '&'.join(['%s=%s' % (key, value) for (key, value) in endpoint_params.items()])
        next_url = 'https://api.linkedin.com/rest/{}?{}'.format(self.path, querystring)

        while next_url: #pylint: disable=too-many-nested-blocks
            LOGGER.info('URL for %s: %s', self.tap_stream_id, next_url)

            # Get data, API request
            data = client.get(
                url=next_url,
                endpoint=self.tap_stream_id,
                headers=self.headers)
            # time_extracted: datetime when the data was extracted from the API
            time_extracted = utils.now()

            # Transform data with transform_json from transform.py
            #  This function converts unix datetimes, de-nests audit fields,
            #  tranforms URNs to IDs, tranforms/abstracts variably named fields,
            #  converts camelCase to snake_case for fieldname keys.
            # For the Linkedin Ads API, 'elements' is always the root data_key for records.
            # The data_key identifies the collection of records below the <root> element
            transformed_data = [] # initialize the record list
            if self.data_key in data:
                transformed_data = transform_json(data, self.tap_stream_id)[self.data_key]
            if not transformed_data or transformed_data is None:
                LOGGER.info('No transformed_data')
                break # No data results

            pre_singer_transformed_data = copy.deepcopy(transformed_data)
            if self.tap_stream_id in selected_streams:
                # Process records and gets the max_bookmark_value and record_count for the set of records
                max_bookmark_value, record_count = self.process_records(
                    catalog=catalog,
                    records=transformed_data,
                    time_extracted=time_extracted,
                    bookmark_field=bookmark_field,
                    max_bookmark_value=max_bookmark_value,
                    last_datetime=last_datetime,
                    parent_id=parent_id)
                LOGGER.info('%s, records processed: %s', self.tap_stream_id, record_count)
                total_records = total_records + record_count

            # Loop thru parent batch records for each children objects
            for child_stream_name in children:
                if child_stream_name in selected_streams:
                    # For each parent record
                    child_obj = STREAMS[child_stream_name]()

                    for record in pre_singer_transformed_data:

                        parent_id = record.get(child_obj.foreign_key)

                        child_stream_params = child_obj.params
                        # Add children filter params based on parent IDs
                        if self.tap_stream_id == 'accounts':
                            account = 'urn:li:sponsoredAccount:{}'.format(parent_id)
                            owner_id = record.get('reference_organization_id', None)
                            owner = 'urn:li:organization:{}'.format(owner_id)
                            if child_stream_name == 'video_ads' and owner_id is not None:
                                child_stream_params['account'] = account
                                child_stream_params['owner'] = owner
                            else:
                                LOGGER.warning("Skipping video_ads call for %s account as reference_organization_id is not found.", account)
                                continue
                        elif self.tap_stream_id == 'campaigns':
                            campaign = 'urn:li:sponsoredCampaign:{}'.format(parent_id)
                            if child_stream_name == 'creatives':
                                # The value of the campaigns in the query params should be passed in the encoded format.
                                # Ref - https://learn.microsoft.com/en-us/linkedin/marketing/integrations/ads/account-structure/create-and-manage-creatives?view=li-lms-2023-01&tabs=http#sample-request-3
                                child_stream_params['campaigns'] = 'List(urn%3Ali%3AsponsoredCampaign%3A{})'.format(parent_id)
                            elif child_stream_name in ('ad_analytics_by_campaign', 'ad_analytics_by_creative'):
                                child_stream_params['campaigns[0]'] = campaign

                        # Update params for the child stream
                        child_obj.params = child_stream_params
                        LOGGER.info('Syncing: %s, parent_stream: %s, parent_id: %s',
                                    child_stream_name,
                                    self.tap_stream_id,
                                    parent_id)

                        # Call sync method for the child stream
                        if child_stream_name in {'ad_analytics_by_campaign', 'ad_analytics_by_creative'}:
                            child_total_records, child_batch_bookmark_value = child_obj.sync_ad_analytics(
                                client=client,
                                catalog=catalog,
                                last_datetime=child_obj.get_bookmark(state, start_date),
                                date_window_size=date_window_size,
                                parent_id=parent_id)
                        else:
                            child_total_records, child_batch_bookmark_value = child_obj.sync_endpoint(
                                client=client,
                                catalog=catalog,
                                state=state,
                                page_size=page_size,
                                start_date=start_date,
                                selected_streams=selected_streams,
                                date_window_size=date_window_size,
                                parent_id=parent_id)

                        child_batch_bookmark_dttm = strptime_to_utc(child_batch_bookmark_value)
                        child_max_bookmark = child_max_bookmarks.get(child_stream_name)
                        child_max_bookmark_dttm = strptime_to_utc(child_max_bookmark)
                        if child_batch_bookmark_dttm > child_max_bookmark_dttm:
                            # Update bookmark for child stream.
                            child_max_bookmarks[child_stream_name] = strftime(child_batch_bookmark_dttm)

                        LOGGER.info('Synced: %s, parent_id: %s, total_records: %s',
                                    child_stream_name,
                                    parent_id,
                                    child_total_records)
                        LOGGER.info('FINISHED Syncing: %s', child_stream_name)

            # Pagination: Get next_url
            next_url = get_next_url(data)

            if self.tap_stream_id in selected_streams:
                LOGGER.info('%s: Synced page %s, this page: %s. Total records processed: %s',
                            self.tap_stream_id,
                            page,
                            record_count,
                            total_records)
            page = page + 1

        # Write child stream's bookmarks
        for key, val in list(child_max_bookmarks.items()):
            write_bookmark(state, val, key)

        return total_records, max_bookmark_value

    # pylint: disable=too-many-branches,too-many-statements,unused-argument
    def sync_ad_analytics(self, client, catalog, last_datetime, date_window_size, parent_id=None):
        """
        Sync method for ad_analytics_by_campaign, ad_analytics_by_creative
        """
        # LinkedIn has a max of 20 fields per request. We cap the chunks at 17
        # to make sure there's always room for us to append `dateRange`,
        # `pivot`, and `pivotValue`
        MAX_CHUNK_LENGTH = 17

        bookmark_field = next(iter(self.replication_keys))

        max_bookmark_value = last_datetime
        last_datetime_dt = strptime_to_utc(last_datetime) - timedelta(days=7)

        # Prepare date window for API call
        window_start_date = last_datetime_dt.date()
        window_end_date = window_start_date + timedelta(days=date_window_size)
        today = datetime.date.today()

        # Reset end_date of date window if it is greater than today
        window_end_date = min(window_end_date, today)

        # Override the default start and end dates
        static_params = {**self.params,
                         'dateRange.start.day': window_start_date.day,
                         'dateRange.start.month': window_start_date.month,
                         'dateRange.start.year': window_start_date.year,
                         'dateRange.end.day': window_end_date.day,
                         'dateRange.end.month': window_end_date.month,
                         'dateRange.end.year': window_end_date.year,}

        # Here, valid_selected_fields is a list of fields that the user has selected.
        # API accepts these fields in the parameter and returns its value in the response.
        valid_selected_fields = [snake_case_to_camel_case(field)
                                 for field in selected_fields(catalog.get_stream(self.tap_stream_id))
                                 if snake_case_to_camel_case(field) not in FIELDS_UNAVAILABLE_FOR_AD_ANALYTICS]

        # When testing the API, if the fields in `field` all return `0` then
        # the API returns its empty response.

        # However, the API distinguishes between a day with non-null values
        # (even if this means the values are all `0`) and a day with null
        # values. We found that requesting these fields gives you the days with
        # non-null values
        first_chunk = [['dateRange', 'pivot', 'pivotValue']]

        chunks = first_chunk + list(split_into_chunks(valid_selected_fields, MAX_CHUNK_LENGTH))

        # We have to append these fields in order to ensure we get them back
        # so that we can create the composite primary key for the record and
        # to merge the multiple responses based on this primary key
        for chunk in chunks:
            for field in ['dateRange', 'pivot', 'pivotValue']:
                if field not in chunk:
                    chunk.append(field)

        ############### PAGINATION (for these 2 streams) ###############
        # The Tap requests LinkedIn with one Campaign ID at one time.
        # 1 Campaign permits 100 Ads
        # Considering, 1 Ad is active and the existing behavior of the tap uses 30 Day window size
        #       and timeGranularity = DAILY(Results grouped by day) we get 30 records in one API response
        # Considering the maximum permitted size of Ads are created, "3000" records will be returned in an API response.
        # If “count=100” and records=100 in the API are the same then the next URL will be returned and if we hit that URL, 400 error code will be returned.
        # This case is unreachable because here “count” is 10000 and at maximum, only 3000 records will be returned in an API response.

        total_records = 0
        while window_end_date <= today:
            responses = []
            for chunk in chunks:
                static_params['fields'] = ','.join(chunk)
                params = {"start": 0,
                          **static_params}
                query_string = '&'.join(['%s=%s' % (key, value) for (key, value) in params.items()])
                LOGGER.info('Syncing %s from %s to %s', parent_id, window_start_date, window_end_date)
                for page in sync_analytics_endpoint(client, self.tap_stream_id, self.path, query_string):
                    if page.get(self.data_key):
                        responses.append(page.get(self.data_key))
            raw_records = merge_responses(responses)
            time_extracted = utils.now()

            # While we broke the ad_analytics streams out from
            # `sync_endpoint()`, we want to process them the same. And
            # transform_json() expects a dictionary with a key equal to
            # `data_key` and its value is the response from the API

            # Note that `transform_json()` returns the same structure we pass
            # in. `sync_endpoint()` grabs `data_key` from the return value, so
            # we mirror that here
            transformed_data = transform_json({self.data_key: list(raw_records.values())},
                                              self.tap_stream_id)[self.data_key]
            if not transformed_data:
                LOGGER.info('No transformed_data')
            else:
                max_bookmark_value, record_count = self.process_records(
                    catalog=catalog,
                    records=transformed_data,
                    time_extracted=time_extracted,
                    bookmark_field=bookmark_field,
                    max_bookmark_value=last_datetime,
                    last_datetime=strftime(last_datetime_dt),
                    parent_id=parent_id)
                LOGGER.info('%s, records processed: %s', self.tap_stream_id, record_count)
                LOGGER.info('%s: max_bookmark: %s', self.tap_stream_id, max_bookmark_value)
                total_records += record_count

            window_start_date, window_end_date, static_params = shift_sync_window(static_params, today, date_window_size)

            if window_start_date == window_end_date:
                break

        return total_records, max_bookmark_value

class Accounts(LinkedInAds):
    """
    https://docs.microsoft.com/en-us/linkedin/marketing/integrations/ads/account-structure/create-and-manage-accounts#search-for-accounts
    """
    tap_stream_id = "accounts"
    replication_method = "INCREMENTAL"
    replication_keys = ["last_modified_time"]
    key_properties = ["id"]
    account_filter = "search_id_values_param"
    path = "adAccounts"
    data_key = "elements"
    children = ["video_ads"]
    params = {
        "q": "search",
        "sort.field": "ID",
        "sort.order": "ASCENDING"
    }

class VideoAds(LinkedInAds):
    """
    https://docs.microsoft.com/en-us/linkedin/marketing/integrations/ads/advertising-targeting/create-and-manage-video#finders
    """
    tap_stream_id = "video_ads"
    replication_keys = ["last_modified_time"]
    replication_method = "INCREMENTAL"
    key_properties = ["content_reference"]
    foreign_key = "id"
    path = "adDirectSponsoredContents"
    data_key = "elements"
    parent = "accounts"
    params = {
        "q": "account"
    }

class AccountUsers(LinkedInAds):
    """
    https://docs.microsoft.com/en-us/linkedin/marketing/integrations/ads/account-structure/create-and-manage-account-users#find-ad-account-users-by-accounts
    """
    tap_stream_id = "account_users"
    replication_keys = ["last_modified_time"]
    replication_method = "INCREMENTAL"
    key_properties = ["account_id", "user_person_id"]
    account_filter = "accounts_param"
    path = "adAccountUsers"
    data_key = "elements"
    params = {
        "q": "accounts"
    }

class CampaignGroups(LinkedInAds):
    """
    https://docs.microsoft.com/en-us/linkedin/marketing/integrations/ads/account-structure/create-and-manage-campaign-groups#search-for-campaign-groups
    """
    tap_stream_id = "campaign_groups"
    replication_method = "INCREMENTAL"
    replication_keys = ["last_modified_time"]
    key_properties = ["id"]
    account_filter = "search_account_values_param"
    path = "adCampaignGroups"
    data_key = "elements"
    params = {
        "q": "search",
        "sort.field": "ID",
        "sort.order": "ASCENDING"
    }

class Campaigns(LinkedInAds):
    """
    https://docs.microsoft.com/en-us/linkedin/marketing/integrations/ads/account-structure/create-and-manage-campaigns#search-for-campaigns
    """
    tap_stream_id = "campaigns"
    replication_method = "INCREMENTAL"
    replication_keys = ["last_modified_time"]
    key_properties = ["id"]
    account_filter = "search_account_values_param"
    path = "adCampaigns"
    data_key = "elements"
    children = ["ad_analytics_by_campaign", "creatives", "ad_analytics_by_creative"]
    params = {
        "q": "search",
        "sort.field": "ID",
        "sort.order": "ASCENDING"
    }

class Creatives(LinkedInAds):
    """
    https://learn.microsoft.com/en-us/linkedin/marketing/integrations/ads/account-structure/create-and-manage-creatives?view=li-lms-2023-01&tabs=http#search-for-creatives
    """
    tap_stream_id = "creatives"
    replication_method = "INCREMENTAL"
    replication_keys = ["last_modified_at"]
    key_properties = ["id"]
    path = "creatives"
    foreign_key = "id"
    data_key = "elements"
    parent = "campaigns"
    # The value of the campaigns in the query params should be passed in the encoded format.
    # Ref - https://learn.microsoft.com/en-us/linkedin/marketing/integrations/ads/account-structure/create-and-manage-creatives?view=li-lms-2023-01&tabs=http#sample-request-3
    params = {
        "q": "criteria",
        "campaigns": "List(urn%3Ali%3AsponsoredCampaign%3A{})",
        "sortOrder": "ASCENDING"
    }
    # Requires this specific headers for creatives endpoint.
    # Ref - https://learn.microsoft.com/en-us/linkedin/marketing/integrations/ads/account-structure/create-and-manage-creatives?view=li-lms-2023-01&tabs=http#search-for-creatives
    headers = {'X-Restli-Protocol-Version': "2.0.0",
               "X-RestLi-Method": "FINDER"}

class AdAnalyticsByCampaign(LinkedInAds):
    """
    https://docs.microsoft.com/en-us/linkedin/marketing/integrations/ads-reporting/ads-reporting#analytics-finder
    """
    tap_stream_id = "ad_analytics_by_campaign"
    replication_method = "INCREMENTAL"
    replication_keys = ["end_at"]
    key_properties = ["campaign_id", "start_at"]
    account_filter = "accounts_param"
    path = "adAnalytics"
    foreign_key = "id"
    data_key = "elements"
    parent = "campaigns"
    params = {
        "q": "analytics",
        "pivot": "CAMPAIGN",
        "timeGranularity": "DAILY",
        "count": 10000
    }

class AdAnalyticsByCreative(LinkedInAds):
    """
    https://docs.microsoft.com/en-us/linkedin/marketing/integrations/ads-reporting/ads-reporting#analytics-finder
    """
    tap_stream_id = "ad_analytics_by_creative"
    replication_method = "INCREMENTAL"
    replication_keys = ["end_at"]
    key_properties = ["creative_id", "start_at"]
    account_filter = "accounts_param"
    path = "adAnalytics"
    foreign_key = "id"
    data_key = "elements"
    parent = "campaigns"
    params = {
        "q": "analytics",
        "pivot": "CREATIVE",
        "timeGranularity": "DAILY",
        "count": 10000
    }

# Dictionary of the stream classes
STREAMS = {
    "accounts": Accounts,
    "video_ads": VideoAds,
    "account_users": AccountUsers,
    "campaign_groups": CampaignGroups,
    "campaigns": Campaigns,
    "creatives": Creatives,
    "ad_analytics_by_campaign": AdAnalyticsByCampaign,
    "ad_analytics_by_creative": AdAnalyticsByCreative
}
