import datetime
from unittest import mock
from singer import utils
from parameterized import parameterized
import unittest
from singer.schema import Schema
from singer.catalog import Catalog, CatalogEntry
from tap_linkedin_ads.streams import split_into_chunks, get_next_url, shift_sync_window, merge_responses, sync_analytics_endpoint, STREAMS, LinkedInAds
import tap_linkedin_ads.client as _client
from tap_linkedin_ads.client import LinkedinClient

MAX_CHUNK_LENGTH = 17
ACCOUNT_OBJ = STREAMS['accounts']()
VIDEO_ADS_OBJ = STREAMS['video_ads']()
CAMPAIGN_OBJ = STREAMS['campaigns']()
AD_ANALYTICS_BY_CAMPAIGN = STREAMS['ad_analytics_by_campaign']()

# Mock catalog
CATALOG = Catalog(streams=[
    CatalogEntry(
        stream='accounts',
        tap_stream_id='accounts',
        schema=Schema(
            properties={
                'id': Schema(type='integer'),
                'name': Schema(type='string')}),
        metadata=[
            {'metadata': {'inclusion': 'automatic'},'breadcrumb': ['properties','id']},
            {'metadata': {'inclusion': 'automatic'},'breadcrumb': ['properties','last_modified_time']},
            {'metadata': {'inclusion': 'available','selected': True},'breadcrumb': ['properties','name']}
    ]),
    CatalogEntry(
        stream='video_ads',
        tap_stream_id='video_ads',
        schema=Schema(
            properties={
                'id': Schema(type='integer'),
                'name': Schema(type='string')}),
        metadata=[
            {'metadata': {'inclusion': 'automatic'},'breadcrumb': ['properties','id']},
            {'metadata': {'inclusion': 'automatic'},'breadcrumb': ['properties','last_modified_time']},
            {'metadata': {'inclusion': 'available','selected': True},'breadcrumb': ['properties','name']}
    ]),
    CatalogEntry(
        stream='campaigns',
        tap_stream_id='campaigns',
        schema=Schema(
            properties={
                'id': Schema(type='integer'),
                'name': Schema(type='string')}),
        metadata=[
            {'metadata': {'inclusion': 'automatic'},'breadcrumb': ['properties','id']},
            {'metadata': {'inclusion': 'automatic'},'breadcrumb': ['properties','last_modified_time']},
            {'metadata': {'inclusion': 'available','selected': True},'breadcrumb': ['properties','name']}
    ]),
    CatalogEntry(
        stream='ad_analytics_by_campaign',
        tap_stream_id='ad_analytics_by_campaign',
        schema=Schema(
            properties={
                'id': Schema(type='integer'),
                'name': Schema(type='string')}),
        metadata=[
            {'metadata': {'inclusion': 'automatic'},'breadcrumb': ['properties','id']},
            {'metadata': {'inclusion': 'automatic'},'breadcrumb': ['properties','end_at']},
            {'metadata': {'inclusion': 'available','selected': True},'breadcrumb': ['properties','name']}
    ])])

class TestStreamsUtils(unittest.TestCase):
    """
    Test all utility functions of streams module
    """
    
    def test_split_into_chunks(self):
        """
        Test that `test_split_into_chunks` split 65 fields into 4 chunk of MAX_CHUNK_LENGTH
        """
        fields = list(range(65))
        actual = split_into_chunks(fields, MAX_CHUNK_LENGTH)
        expected = [
            [ 0,  1,  2,  3,  4,  5,  6,  7,  8,  9, 10, 11, 12, 13, 14, 15, 16],
            [17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33],
            [34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50],
            [51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64]
        ]

        # Verify that `test_split_into_chunks` return 4 list of 17 fields for total 65 fields.
        self.assertEqual(expected, list(actual))

    def test_split_into_chunks_2(self):
        """
        Test that `test_split_into_chunks` split 34 fields into 2 chunk of MAX_CHUNK_LENGTH
        """
        fields = list(range(34))
        actual = split_into_chunks(fields, MAX_CHUNK_LENGTH)
        expected = [
            [ 0,  1,  2,  3,  4,  5,  6,  7,  8,  9, 10, 11, 12, 13, 14, 15, 16],
            [17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33],
        ]

        # Verify that `test_split_into_chunks` return 2 list of 17 fields for total 34 fields.
        self.assertEqual(expected, list(actual))

    def test_split_into_chunks_for_less_fields_than_max_length(self):
        """
        Test that `test_split_into_chunks` split 15 fields into single chunk
        """
        fields = list(range(15))
        actual = split_into_chunks(fields, MAX_CHUNK_LENGTH)
        expected = [
            [ 0,  1,  2,  3,  4,  5,  6,  7,  8,  9, 10, 11, 12, 13, 14]
        ]

        # Verify that `test_split_into_chunks` return single list of 15 fields.
        self.assertEqual(expected, list(actual))
    @parameterized.expand([
        ['test_single_page', [None], 1],
        ['test_multiple_page', ["next_url", None], 2]
    ])
    @mock.patch('tap_linkedin_ads.client.LinkedinClient.request', return_value=[])
    @mock.patch('tap_linkedin_ads.streams.get_next_url')
    def test_sync_analytics_endpoint(self, name, next_url, expected_call_count, mock_next_url, mock_get):
        """
        Test that sync_analytics_endpoint function works properly for single page as well as multiple pages.
        """
        mock_next_url.side_effect = next_url
        client = _client.LinkedinClient('client_id', 'client_secret', 'refresh_token', 'access_token', 'config_path')     
        data = list(sync_analytics_endpoint(client, "stream", "path", "query=query"))
        
        # Verify that get method of client is called expected times.
        self.assertEqual(expected_call_count, mock_get.call_count)
        
        
    @parameterized.expand([
        ["test_single_page", [], None],
        ["test_multiple_page", [{'rel': 'next', 'href': '/foo'}], 'https://api.linkedin.com/foo']
    ])
    def test_get_next_url(self, name, links, expected_url):
        """
        Test that get_next_url return link of next page in case of 'href' 
        """
        data = {
            'paging': {
                'links': links
            }
        }

        actual_url = get_next_url(data)

        # Verify the next page url
        self.assertEqual(expected_url, actual_url)

    @parameterized.expand([
        ['test_shift_sync_window_non_boundary', 11, 10],
        ['test_shift_sync_window_boundary', 10, 31]
    ])
    def test_shift_sync_window(self, name, today_month, today_date):
        """
        Test that `shift_sync_window` function move date window properly.
        """
        expected_start_date = datetime.date(year=2020, month=10, day=1)
        expected_end_date = datetime.date(year=2020, month=10, day=31)
        expected_params = {
            'dateRange.start.year': expected_start_date.year,
            'dateRange.start.month': expected_start_date.month,
            'dateRange.start.day': expected_start_date.day,
            'dateRange.end.year': expected_end_date.year,
            'dateRange.end.month': expected_end_date.month,
            'dateRange.end.day': expected_end_date.day,
        }
        
        params = {
            'dateRange.end.year': 2020,
            'dateRange.end.month': 10,
            'dateRange.end.day': 1,
        }
        
        today = datetime.date(year=2020, month=today_month, day=today_date)

        actual_start_date, actual_end_date, actual_params = shift_sync_window(params, today, 30)

        # Verify start date, end date and params of the resultant date window
        self.assertEqual(expected_start_date, actual_start_date)
        self.assertEqual(expected_end_date, actual_end_date)
        self.assertEqual(expected_params, actual_params)

    def test_merge_responses_no_overlap(self):
        """
        Test merge_response function with records of unique date range value.
        """
        expected_output = {
            ('urn:li:sponsoredCampaign:123456789', '2020-10-1') : {'dateRange': {'start': {'year': 2020, 'month': 10, 'day': 1}},
                           'a': 1, 'pivotValue': 'urn:li:sponsoredCampaign:123456789'},
            ('urn:li:sponsoredCampaign:123456789', '2020-10-2') : {'dateRange': {'start': {'year': 2020, 'month': 10, 'day': 2}},
                           'b': 2, 'pivotValue': 'urn:li:sponsoredCampaign:123456789'},
            ('urn:li:sponsoredCampaign:123456789', '2020-10-3') : {'dateRange': {'start': {'year': 2020, 'month': 10, 'day': 3}},
                           'c': 3, 'pivotValue': 'urn:li:sponsoredCampaign:123456789'},
            ('urn:li:sponsoredCampaign:123456789', '2020-10-4') : {'dateRange': {'start': {'year': 2020, 'month': 10, 'day': 4}},
                           'd': 4, 'pivotValue': 'urn:li:sponsoredCampaign:123456789'},
            ('urn:li:sponsoredCampaign:123456789', '2020-10-5') : {'dateRange': {'start': {'year': 2020, 'month': 10, 'day': 5}},
                           'e': 5, 'pivotValue': 'urn:li:sponsoredCampaign:123456789'},
            ('urn:li:sponsoredCampaign:123456789', '2020-10-6') : {'dateRange': {'start': {'year': 2020, 'month': 10, 'day': 6}},
                           'f': 6, 'pivotValue': 'urn:li:sponsoredCampaign:123456789'},
            }

        data = [
            [{'dateRange': {'start': {'year': 2020, 'month': 10, 'day': 1}},
              'a': 1, 'pivotValue': 'urn:li:sponsoredCampaign:123456789'},
             {'dateRange': {'start': {'year': 2020, 'month': 10, 'day': 2}},
              'b': 2, 'pivotValue': 'urn:li:sponsoredCampaign:123456789'},
             {'dateRange': {'start': {'year': 2020, 'month': 10, 'day': 3}},
              'c': 3, 'pivotValue': 'urn:li:sponsoredCampaign:123456789'},],
            [{'dateRange': {'start': {'year': 2020, 'month': 10, 'day': 4}},
              'd': 4, 'pivotValue': 'urn:li:sponsoredCampaign:123456789'},
             {'dateRange': {'start': {'year': 2020, 'month': 10, 'day': 5}},
              'e': 5, 'pivotValue': 'urn:li:sponsoredCampaign:123456789'},
             {'dateRange': {'start': {'year': 2020, 'month': 10, 'day': 6}},
              'f': 6, 'pivotValue': 'urn:li:sponsoredCampaign:123456789'},],
        ]

        actual_output = merge_responses(data)

        self.assertEqual(expected_output, actual_output)

    def test_merge_responses_with_overlap(self):
        """
        Test merge_responses function with records of same date range value
        """
        data = [
            [{'dateRange': {'start': {'year': 2020, 'month': 10, 'day': 1}},
              'a': 1, 'pivotValue': 'urn:li:sponsoredCampaign:123456789'},
             {'dateRange': {'start': {'year': 2020, 'month': 10, 'day': 1}},
              'b': 7, 'pivotValue': 'urn:li:sponsoredCampaign:123456789'},
             {'dateRange': {'start': {'year': 2020, 'month': 10, 'day': 2}},
              'b': 2, 'pivotValue': 'urn:li:sponsoredCampaign:123456789'},
             {'dateRange': {'start': {'year': 2020, 'month': 10, 'day': 3}},
              'c': 3, 'pivotValue': 'urn:li:sponsoredCampaign:123456789'},],
            [{'dateRange': {'start': {'year': 2020, 'month': 10, 'day': 4}},
              'd': 4, 'pivotValue': 'urn:li:sponsoredCampaign:123456789'},
             {'dateRange': {'start': {'year': 2020, 'month': 10, 'day': 5}},
              'e': 5, 'pivotValue': 'urn:li:sponsoredCampaign:123456789'},
             {'dateRange': {'start': {'year': 2020, 'month': 10, 'day': 6}},
              'f': 6, 'pivotValue': 'urn:li:sponsoredCampaign:123456789'},],
        ]

        expected_output = {
            ('urn:li:sponsoredCampaign:123456789', '2020-10-1') : {'dateRange': {'start': {'year': 2020, 'month': 10, 'day': 1}},
                           'a': 1, 'b': 7, 'pivotValue': 'urn:li:sponsoredCampaign:123456789'},
            ('urn:li:sponsoredCampaign:123456789', '2020-10-2') : {'dateRange': {'start': {'year': 2020, 'month': 10, 'day': 2}},
                           'b': 2, 'pivotValue': 'urn:li:sponsoredCampaign:123456789'},
            ('urn:li:sponsoredCampaign:123456789', '2020-10-3') : {'dateRange': {'start': {'year': 2020, 'month': 10, 'day': 3}},
                           'c': 3, 'pivotValue': 'urn:li:sponsoredCampaign:123456789'},
            ('urn:li:sponsoredCampaign:123456789', '2020-10-4') : {'dateRange': {'start': {'year': 2020, 'month': 10, 'day': 4}},
                           'd': 4, 'pivotValue': 'urn:li:sponsoredCampaign:123456789'},
            ('urn:li:sponsoredCampaign:123456789', '2020-10-5') : {'dateRange': {'start': {'year': 2020, 'month': 10, 'day': 5}},
                           'e': 5, 'pivotValue': 'urn:li:sponsoredCampaign:123456789'},
            ('urn:li:sponsoredCampaign:123456789', '2020-10-6') : {'dateRange': {'start': {'year': 2020, 'month': 10, 'day': 6}},
                           'f': 6, 'pivotValue': 'urn:li:sponsoredCampaign:123456789'},
            }

        actual_output = merge_responses(data)

        # Verify that merge_responses function merge records by primary with same date range value.
        self.assertEqual(expected_output, actual_output)

class TestLinkedInAds(unittest.TestCase):
    """
    Test LinkedInAds class's functionality.
    """

    @parameterized.expand([
        ['test_none_state', None, "default"],
        ['test_empty_state', {}, "default"],
        ['test_empty_bookmark', {'bookmarks': {'a': 'bookmark_a'}}, "default"],
        ['test_non_empty_bookmark', {'bookmarks': {'accounts': 'bookmark_accounts'}}, "bookmark_accounts"]
    ])
    def test_get_bookmark(self, name, state, expected_output):
        """
        Test get_bookmark function for the following scenrios,
        Case 1: Return default value if state is None
        Case 2: Return default value if `bookmarks` key is not found in the state
        Case 3: Return default value if stream_name is not found in the bookmarks
        Cas 4: Return actual bookmark value if it is found in the state
        """
        
        actual_output = ACCOUNT_OBJ.get_bookmark(state, "default")
        
        self.assertEqual(expected_output, actual_output)
        
    @parameterized.expand([
        ['test_zero_records', ACCOUNT_OBJ, [], "last_modified_time", "2021-07-20T08:50:30.169000Z", 0],
        ['test_zero_latest_records', ACCOUNT_OBJ, [{'id': 1, 'last_modified_time': '2021-06-26T08:50:30.169000Z'}], "last_modified_time", "2021-07-20T08:50:30.169000Z", 0],
        ['test_new_record', ACCOUNT_OBJ, [{'id': 1, 'last_modified_time': '2021-06-26T08:50:30.169000Z'}, {'id': 2, 'last_modified_time': '2021-07-24T08:50:30.169000Z'}], "last_modified_time", "2021-07-24T08:50:30.169000Z", 1],
        ['test_no_replication_key', ACCOUNT_OBJ, [{'id': 1, 'last_modified_time': '2021-06-26T08:50:30.169000Z'}], None, "2021-07-20T08:50:30.169000Z", 1],
        ['test_child_record', VIDEO_ADS_OBJ, [{'id': 1, 'last_modified_time': '2021-08-26T08:50:30.169000Z'}], "last_modified_time", '2021-08-26T08:50:30.169000Z', 1]
    ])
    def test_process_records(self, name, stream_obj, records, replication_key, expected_max_bookmark, expected_record_count):
        """
        Test that process_records function calculate maximum bookmark properly and return it with total no of records.
        """
        max_bookmark_value = last_datetime = "2021-07-20T08:50:30.169000Z"
        actual_max_bookmark, actual_record_count = stream_obj.process_records(CATALOG, records, utils.now(), replication_key, max_bookmark_value, last_datetime)
        
        # Verify maximum bookmark and total records.
        self.assertEqual(expected_max_bookmark, actual_max_bookmark)
        self.assertEqual(expected_record_count, actual_record_count)

    @parameterized.expand([
        ['test_only_parent_selcted_stream', ['accounts'], ACCOUNT_OBJ,
         [{'paging': {'start': 0, 'count': 100, 'links': [], 'total': 1},'elements': [{'changeAuditStamps': {'created': {'time': 1564585620000}, 'lastModified': {'time': 1564585620000}}, 'id': 1}]}],
         0, 1
        ],
        ['test_only_child_selected_stream', ['video_ads'], ACCOUNT_OBJ,
         [{'paging': {'start': 0, 'count': 100, 'links': [], 'total': 1},'elements': [{'changeAuditStamps': {'created': {'time': 1564585620000}, 'lastModified': {'time': 1564585620000}}, 'id': 1, 'reference_organization_id': 1}]},
          {'paging': {'start': 0, 'count': 100, 'links': [], 'total': 0},'elements': []}],
         1, 0
        ],
        ['test_parent_child_selected_stream', ['ad_analytics_by_campaign', 'campaigns'], CAMPAIGN_OBJ,
         [{'paging': {'start': 0, 'count': 100, 'links': [], 'total': 1},'elements': [{'changeAuditStamps': {'created': {'time': 1564585620000}, 'lastModified': {'time': 1564585620000}}, 'id': 1, 'reference_organization_id': 1}]},
          {'paging': {'start': 0, 'count': 100, 'links': [], 'total': 0},'elements': []}],
         1, 1
        ],
        ['test_only_parent_selcted_stream', ['campaigns'], CAMPAIGN_OBJ,
         [{'paging': {'start': 0, 'count': 100, 'links': [], 'total': 1},'elements': [{'changeAuditStamps': {'created': {'time': 1564585620000}, 'lastModified': {'time': 1564585620000}}, 'id': 1}]}],
         0, 1
        ]   
    ])
    @mock.patch("tap_linkedin_ads.streams.LinkedInAds.sync_ad_analytics", return_value=(1, "2019-07-31T15:07:00.000000Z"))
    @mock.patch("tap_linkedin_ads.streams.LinkedInAds.get_bookmark", return_value = "2019-07-31T15:07:00.000000Z")
    @mock.patch("tap_linkedin_ads.client.LinkedinClient.request")
    @mock.patch("tap_linkedin_ads.streams.LinkedInAds.process_records")
    @mock.patch("tap_linkedin_ads.streams.LinkedInAds.write_schema")
    def test_sync_endpoint(self, name, selected_streams, stream_obj, mock_response, expected_write_schema_count, mock_record_count,
                           mock_write_schema,mock_process_records,mock_client,mock_get_bookmark, mock_sync_ad_analytics):
        """
        Test sync_endpoint function for parent and child streams.
        """
        client = LinkedinClient('client_id', 'client_secret', 'refresh_token', 'access_token', 'config_path')
        state={}
        start_date='2019-06-01T00:00:00Z'
        page_size = 100
        date_window_size = 7

        mock_client.side_effect = mock_response
        mock_process_records.return_value = "2019-07-31T15:07:00.000000Z",1
        actual_total_record, actual_max_bookmark = stream_obj.sync_endpoint(client, CATALOG, state, page_size, start_date, selected_streams, date_window_size)
        
        # Verify total no of records
        self.assertEqual(actual_total_record, mock_record_count)
        # Verify maximum bookmark
        self.assertEqual(actual_max_bookmark, "2019-07-31T15:07:00.000000Z")
        # Verify total no of write_schema function call. sync_endpoint calls write_schema single time for each child.
        self.assertEqual(mock_write_schema.call_count, expected_write_schema_count)

    @mock.patch("tap_linkedin_ads.sync.LOGGER.warning")
    @mock.patch("tap_linkedin_ads.streams.LinkedInAds.get_bookmark", return_value = "2019-07-31T15:07:00.000000Z")
    @mock.patch("tap_linkedin_ads.client.LinkedinClient.request")
    @mock.patch("tap_linkedin_ads.streams.LinkedInAds.process_records")
    @mock.patch("tap_linkedin_ads.streams.LinkedInAds.write_schema")
    def test_sync_endpoint_for_reference_organization_id_is_None(self, mock_write_schema,mock_process_records,mock_client,mock_get_bookmark,
                                                                 mock_warning):
        """
        Verify that tap skips API call for video_ads stream if owner_id in the parent's record is None.
        """
        client = LinkedinClient('client_id', 'client_secret', 'refresh_token', 'access_token', 'config_path')
        state={'currently_syncing': 'accounts'}
        start_date='2019-06-01T00:00:00Z'
        page_size = 100
        date_window_size = 7
        selected_streams = ['accounts', 'video_ads']

        mock_client.side_effect = [{'paging': {'start': 0, 'count': 100, 'links': [], 'total': 1},'elements': [{'changeAuditStamps': {'created': {'time': 1564585620000}, 'lastModified': {'time': 1564585620000}}, 'id': 1}]}]
        mock_process_records.return_value = "2019-07-31T15:07:00.000000Z",1
        ACCOUNT_OBJ.sync_endpoint(client, CATALOG, state, page_size, start_date, selected_streams, date_window_size)
        
        mock_warning.assert_called_with('Skipping video_ads call for %s account as reference_organization_id is not found.', 'urn:li:sponsoredAccount:1')


    @parameterized.expand([
        ['test_no_record', 0, '2022-08-01T00:00:00Z', {'elements': []}],
        ['test_multiple_record', 1, '2022-08-01T00:00:00Z', {'elements': [{'id': 1}]}]
    ])
    @mock.patch("tap_linkedin_ads.streams.LinkedInAds.process_records")
    @mock.patch("tap_linkedin_ads.streams.shift_sync_window", return_value=('', '', ''))
    @mock.patch("tap_linkedin_ads.streams.transform_json")
    @mock.patch("tap_linkedin_ads.streams.sync_analytics_endpoint")
    @mock.patch("tap_linkedin_ads.streams.merge_responses")
    def test_sync_ad_analytics(self, name, expected_record_count, expected_max_bookmark, mock_tranform_data, 
                               mock_merge_response, mock_endpoint, mock_transform, mock_shift_windows, mock_process_record):
        """
        Test that `sync_ad_analytics` function work properly for zero records as well as multiple records.
        """

        client = LinkedinClient('client_id', 'client_secret', 'refresh_token', 'access_token', 'config_path')
        bookmark='2022-08-01T00:00:00Z'
        date_window_size = 7

        mock_transform.return_value = mock_tranform_data
        mock_process_record.return_value = (expected_max_bookmark, expected_record_count)
        actual_record_count, actual_max_bookmark =  AD_ANALYTICS_BY_CAMPAIGN.sync_ad_analytics(client, CATALOG, bookmark, date_window_size)
        
        # Verify maximum bookmark
        self.assertEqual(actual_max_bookmark, expected_max_bookmark)
        # Verify total no of records
        self.assertEqual(actual_record_count, expected_record_count)

    @mock.patch('singer.write_schema', side_effect=OSError('error'))
    @mock.patch('tap_linkedin_ads.streams.LOGGER.info')
    def test_write_schema(self, mock_logger, mock_write_schema):
        """
        Test that tap handle OSError while writing the schema.
        """
        with self.assertRaises(OSError) as e:
            ACCOUNT_OBJ.write_schema(CATALOG)

        mock_logger.assert_called_with('OS Error writing schema for: %s', 'accounts')

    @mock.patch('singer.write_record', side_effect=OSError('error'))
    @mock.patch('tap_linkedin_ads.streams.LOGGER.info')
    def test_write_record(self, mock_logger, mock_write_record):
        """
        Test that tap handle OSError while writing the record.
        """
        with self.assertRaises(OSError) as e:
            ACCOUNT_OBJ.write_record([], '')
        
        mock_logger.assert_called_with('record: %s', [])
