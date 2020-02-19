"""
Test tap combined
"""

import unittest
import os
from tap_tester import menagerie
import tap_tester.runner as runner
import tap_tester.connections as connections
from tap_tester.scenario import SCENARIOS


class LinkedinAdsSyncTest(unittest.TestCase):

    ALL_STREAMS = {
        'accounts',
        'video_ads',
        'account_users',
        'campaign_groups',
        'campaigns',
        'ad_analytics_by_campaign',
        'creatives',
        'ad_analytics_by_creative'
    }

    def name(self):
        return "tap_linkedin_ads_combined_test"

    def tap_name(self):
        """The name of the tap"""
        return "tap-linkedin-ads"

    def get_type(self):
        """the expected url route ending"""
        return "platform.linkedin-ads"

    def expected_check_streams(self):
        return self.ALL_STREAMS

    def expected_sync_streams(self):
        return self.ALL_STREAMS

    def expected_pks(self):
        return {
            'accounts': {"id"},
            'video_ads': {"content_reference"},
            'account_users': {"account_id", "user_person_id"},
            'campaign_groups': {"id"},
            'campaigns': {"id"},
            'ad_analytics_by_campaign': {"campaign_id", "start_at"},
            'creatives': {"id"},
            'ad_analytics_by_creative': {"creative_id", "start_at"}
        }

    def expected_replication_keys(self):
        return {'account_users': ['last_modified_time'],
                'accounts': ['last_modified_time'],
                'ad_analytics_by_campaign': ['end_at'],
                'ad_analytics_by_creative': ['end_at'],
                'campaign_groups': ['last_modified_time'],
                'campaigns': ['last_modified_time'],
                'creatives': ['last_modified_time'],
                'video_ads': ['last_modified_time']}

    def get_properties(self):
        """Configuration properties required for the tap."""
        return {'start_date': '2018-08-21T00:00:00Z',
                'accounts': os.getenv('TAP_LINKEDIN_ADS_ACCOUNTS')}

    def get_credentials(self):
        """Authentication information for the test account"""
        return {'access_token': os.getenv('TAP_LINKEDIN_ADS_ACCESS_TOKEN')}

    def setUp(self):
        missing_envs = [x for x in [os.getenv('TAP_LINKEDIN_ADS_ACCESS_TOKEN'),
                                    os.getenv('TAP_LINKEDIN_ADS_ACCOUNTS')]
                        if x == None]
        if len(missing_envs) != 0:
            raise Exception("set TAP_LINKEDIN_ADS_ACCESS_TOKEN and TAP_LINKEDIN_ADS_ACCOUNTS")

    def max_bookmarks_by_stream(self, sync_records):
        max_bookmarks = {}
        for stream, batch in sync_records.items():
            upsert_messages = [m for m in batch.get('messages') if m['action'] == 'upsert']

            stream_bookmark_key = self.expected_replication_keys().get(stream) or set()

            # There shouldn't be a compound replication key
            assert not stream_bookmark_key or len(stream_bookmark_key) == 1

            if not stream_bookmark_key:
                continue

            stream_bookmark_key = stream_bookmark_key.pop()

            bk_values = [message["data"].get(stream_bookmark_key) for message in upsert_messages]

            max_bookmarks[stream] = {stream_bookmark_key: max(bk_values)}

        return max_bookmarks

    def min_bookmarks_by_stream(self, sync_records):
        min_bookmarks = {}
        for stream, batch in sync_records.items():
            upsert_messages = [m for m in batch.get('messages') if m['action'] == 'upsert']

            stream_bookmark_key = self.expected_replication_keys().get(stream) or set()

            # There shouldn't be a compound replication key
            assert not stream_bookmark_key or len(stream_bookmark_key) == 1

            if not stream_bookmark_key:
                continue

            stream_bookmark_key = stream_bookmark_key.pop()

            bk_values = [message["data"].get(stream_bookmark_key) for message in upsert_messages]

            min_bookmarks[stream] = {stream_bookmark_key: min(bk_values)}

        return min_bookmarks

    def run_sync_and_get_record_count(self, conn_id):
        sync_job_name = runner.run_sync_mode(self, conn_id)
        exit_status = menagerie.get_exit_status(conn_id, sync_job_name)
        menagerie.verify_sync_exit_status(self, exit_status, sync_job_name)

        return runner.examine_target_output_file(self,
                                                 conn_id,
                                                 self.expected_sync_streams(),
                                                 self.expected_pks())

    def test_run(self):

        conn_id = connections.ensure_connection(self, payload_hook=None)

        # Run the tap in check mode
        check_job_name = runner.run_check_mode(self, conn_id)

        # Verify the check's exit status
        exit_status = menagerie.get_exit_status(conn_id, check_job_name)
        menagerie.verify_check_exit_status(self, exit_status, check_job_name)

        # Verify that there are catalogs found
        catalog_entries = menagerie.get_catalogs(conn_id)

        # Select all streams and all fields
        for entry in catalog_entries:

            schema = menagerie.select_catalog(conn_id, entry)

            catalog_entry = {
                'key_properties': entry.get('key_properties'),
                'schema' : schema,
                'tap_stream_id': entry.get('tap_stream_id'),
                'replication_method': entry.get('replication_method'),
                'replication_key': entry.get('replication_key')
            }

            connections.select_catalog_and_fields_via_metadata(conn_id, catalog_entry, schema)

        # found_catalogs = menagerie.get_catalogs(conn_id)
        self.assertGreater(len(catalog_entries),
                           0,
                           msg="unable to locate schemas for connection {}".format(conn_id))

        set_of_discovered_streams = {entry['tap_stream_id'] for entry in catalog_entries}
        self.assertTrue(
            self.expected_check_streams().issubset(set_of_discovered_streams),
            msg="Expected check streams are not a subset of discovered streams"
        )

        menagerie.set_state(conn_id, {})

        # Verify that tap and target exit codes are 0
        first_record_count = self.run_sync_and_get_record_count(conn_id)

        # verify that we only sync selected streams
        self.assertEqual(set(first_record_count.keys()), self.expected_sync_streams())

        first_state = menagerie.get_state(conn_id)

        first_sync_records = runner.get_records_from_target_output()
        first_max_bookmarks = self.max_bookmarks_by_stream(first_sync_records)
        first_min_bookmarks = self.min_bookmarks_by_stream(first_sync_records)

        # Run second sync
        second_record_count = self.run_sync_and_get_record_count(conn_id)
        second_state = menagerie.get_state(conn_id)

        second_sync_records = runner.get_records_from_target_output()
        second_max_bookmarks = self.max_bookmarks_by_stream(second_sync_records)
        second_min_bookmarks = self.min_bookmarks_by_stream(second_sync_records)

        for stream in self.expected_sync_streams():
            # Verify first sync returns more data or same amount of data
            self.assertGreaterEqual(
                first_record_count.get(stream, 0),
                second_record_count.get(stream, 0),
                msg="Second sync didn't always return less records for stream {}".format(stream)
            )

            self.assertGreaterEqual(
                second_state['bookmarks'][stream],
                first_state['bookmarks'][stream]
            )


SCENARIOS.add(LinkedinAdsSyncTest)
