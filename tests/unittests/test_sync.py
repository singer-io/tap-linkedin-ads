import unittest
from unittest import mock
from parameterized import parameterized
from singer.schema import Schema
from singer.catalog import Catalog, CatalogEntry
from tap_linkedin_ads.sync import get_page_size, get_streams_to_sync, update_currently_syncing, sync
from tap_linkedin_ads.client import LinkedinClient

DEFAULT_PAGE_SIZE = 100
CATALOG = Catalog(streams=[
    CatalogEntry(
        stream='accounts',
        tap_stream_id='accounts',
        key_properties='id',
        schema=Schema(
            properties={
                'id': Schema(type='integer'),
                'name': Schema(type='string')}),
        metadata=[
            {"breadcrumb": [], "metadata": {'selected': True}},
            {'metadata': {'inclusion': 'automatic'},'breadcrumb': ['properties','id']},
            {'metadata': {'inclusion': 'automatic'},'breadcrumb': ['properties','last_modified_time']},
            {'metadata': {'inclusion': 'available','selected': True},'breadcrumb': ['properties','name']}
    ]),
    CatalogEntry(
        stream='video_ads',
        tap_stream_id='video_ads',
        key_properties='id',
        schema=Schema(
            properties={
                'id': Schema(type='integer'),
                'name': Schema(type='string')}),
        metadata=[
            {"breadcrumb": [], "metadata": {'selected': True}},
            {'metadata': {'inclusion': 'automatic'},'breadcrumb': ['properties','id']},
            {'metadata': {'inclusion': 'automatic'},'breadcrumb': ['properties','last_modified_time']},
            {'metadata': {'inclusion': 'available','selected': True},'breadcrumb': ['properties','name']}
    ]),
    CatalogEntry(
        stream='account_users',
        tap_stream_id='account_users',
        key_properties='id',
        schema=Schema(
            properties={
                'id': Schema(type='integer')}),
        metadata=[
            {"breadcrumb": [], "metadata": {'selected': True}},
    ]),
    CatalogEntry(
        stream='campaigns',
        tap_stream_id='campaigns',
        key_properties='id',
        schema=Schema(
            properties={
                'id': Schema(type='integer'),
                'name': Schema(type='string')}),
        metadata=[
            {"breadcrumb": [], "metadata": {'selected': True}},
            {'metadata': {'inclusion': 'automatic'},'breadcrumb': ['properties','id']},
            {'metadata': {'inclusion': 'automatic'},'breadcrumb': ['properties','last_modified_time']},
            {'metadata': {'inclusion': 'available','selected': True},'breadcrumb': ['properties','name']}
    ]),
    CatalogEntry(
        stream='ad_analytics_by_campaign',
        key_properties='id',
        tap_stream_id='ad_analytics_by_campaign',
        schema=Schema(
            properties={
                'id': Schema(type='integer'),
                'name': Schema(type='string')}),
        metadata=[
            {"breadcrumb": [], "metadata": {'selected': True}},
            {'metadata': {'inclusion': 'automatic'},'breadcrumb': ['properties','id']},
            {'metadata': {'inclusion': 'automatic'},'breadcrumb': ['properties','end_at']},
            {'metadata': {'inclusion': 'available','selected': True},'breadcrumb': ['properties','name']}
    ])])

class TestSyncUtils(unittest.TestCase):
    """
    Test utility functions of sync module.
    """
    @parameterized.expand([
        ['test_float_value', 100.05],
        ['test_zero_int_value', 0],
        ['test_invalid_string', '*&^a'],
        ['test_zero_float_value', 0.0],
        ['test_negative_value', -10],
        ['test_nagative_float_value', -10.04]
    ])
    def test_invalid_page_size(self, name, page_size):
        """
        Test all invalid page value of config parameter `page_size`.
        """
        error_message = 'The entered page size ({}) is invalid'
        with self.assertRaises(Exception) as err:
            get_page_size({'page_size': page_size})

        # Verify that tap raises error with porper message for invalid `page_size` value
        self.assertEqual(str(err.exception), error_message.format(page_size))

    @parameterized.expand([
        ['test_valid_integer', 300, 300],
        ['test_empty_string', "", DEFAULT_PAGE_SIZE],
        ['test_valid_string', "300", 300],
    ])
    def test_valid_page_size(self, name, config_page_size, expected_page_size):
        """
        Test all valid page value of config parameter `page_size`.
        """
        actual_page_size = get_page_size({'page_size': config_page_size})
        
        # Verify page_size value. 
        # If `page_size` param is not available in the config then `get_page_size` should return default value
        self.assertEqual(actual_page_size, expected_page_size)

    @parameterized.expand([
        ['test_only_parent_selected', ['campaigns'], ['campaigns']],
        ['test_only_single_child_selected', ['ad_analytics_by_campaign'], ['campaigns']],
        ['test_multiple_child_selected', ['accounts', 'ad_analytics_by_campaign', 'ad_analytics_by_creative'], ['accounts', 'campaigns']],
        ['test_parent_child_both_selected', ['ad_analytics_by_creative', 'campaigns'], ['campaigns']]
    ])
    def test_get_streams_to_sync(self, name, selected_streams, expected_parent_streams):
        """
        Test that get_streams_to_sync function return valid list of stream names for which 
        sync_endpoints method require to call.
        """
        actual_parent_streams = get_streams_to_sync(selected_streams)
        
        self.assertEqual(expected_parent_streams, actual_parent_streams)
    
    @parameterized.expand([
        ['test_reset_existing_currently_syncing', {'currently_syncing': 'a'}, None, 0],
        ['test_new_set_currently_syncing', {}, 'a', 1]
    ])
    @mock.patch('singer.set_currently_syncing')
    def test_update_currently_syncing(self, name, current_state, stream, expected_call_count, mock_singer_currently_syncing):
        """
        Test that update_currently_syncing function reset currently_syncing for complete sync 
        and set currently_syncing for interrupted sync.
        """
        update_currently_syncing(current_state, stream)
        self.assertEqual(expected_call_count, mock_singer_currently_syncing.call_count)

class TestSync(unittest.TestCase):
    
    @parameterized.expand([
        ['test_sync_without_datewindow', {'start_date': '2019-06-01T00:00:00Z', 'accounts': '12345'}, 30],
        ['test_sync_with_datewindow', {'start_date': '2019-06-01T00:00:00Z', 'date_window_size': 7, 'accounts': '1245'}, 7]
    ])
    @mock.patch('tap_linkedin_ads.streams.LinkedInAds.sync_endpoint', return_value=(1, '2020-06-01T00:00:00Z'))
    def test_sync(self, name, config, expected_date_window, mock_sync_endpoint):
        """
        Test sync function
        """
        client = LinkedinClient('client_id', 'client_secret', 'refresh_token', 'access_token', 'config_path')
        state = {} 

        sync(client, config, CATALOG, state)
        mock_sync_endpoint.assert_called_with(client=client,
                                              catalog=CATALOG, 
                                              state=state, 
                                              page_size=100, 
                                              start_date="2019-06-01T00:00:00Z", 
                                              selected_streams=['accounts', 'video_ads', 'account_users', 'campaigns', 'ad_analytics_by_campaign'], 
                                              date_window_size=expected_date_window)