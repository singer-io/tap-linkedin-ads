from tap_tester.scenario import SCENARIOS
import tap_tester.connections as connections
import tap_tester.menagerie as menagerie
import tap_tester.runner as runner
import os
from datetime import datetime, time, date, timezone, timedelta
import unittest
from functools import reduce
from singer import utils
from singer import metadata


class HelpScoutBase(unittest.TestCase):

    def name(self):
        return "tap_tester_helpscout_base"

    def setUp(self):
        missing_envs = [x for x in [os.getenv('TAP_HELPSCOUT_CLIENT_ID'),
                                os.getenv('TAP_HELPSCOUT_CLIENT_SECRET'),
                                os.getenv('TAP_HELPSCOUT_START_DATE'),
                                os.getenv('TAP_HELPSCOUT_REFRESH_TOKEN'),
                                os.getenv('TAP_HELPSCOUT_USER_AGENT'),] if x == None]
        if len(missing_envs) != 0:
            #pylint: disable=line-too-long
            raise Exception("set TAP_HELPSCOUT_CLIENT_ID, TAP_HELPSCOUT_CLIENT_SECRET, \
                TAP_HELPSCOUT_START_DATE, TAP_HELPSCOUT_REFRESH_TOKEN, TAP_HELPSCOUT_USER_AGENT")

        self.conn_id = connections.ensure_connection(self)


    def tap_name(self):
        return "tap-helpscout"


    def get_type(self):
        return "platform.helpscout"


    def get_credentials(self):
        return {'client_secret': os.getenv('TAP_HELPSCOUT_CLIENT_SECRET'), 
                'refresh_token': os.getenv('TAP_HELPSCOUT_REFRESH_TOKEN'),
                'access_token': os.getenv('TAP_HELPSCOUT_ACCESS_TOKEN')}


    def get_properties(self):
        return {'start_date': os.getenv('TAP_HELPSCOUT_START_DATE'),
                'client_id': os.getenv('TAP_HELPSCOUT_CLIENT_ID'),
                'redirect_uri': os.getenv('TAP_HELPSCOUT_USER_AGENT')}



    def expected_sync_streams(self):
        return {
            'conversations',
            'conversation_threads',
            'customers',
            'mailboxes',
            'mailbox_fields',
            'mailbox_folders',
            'users',
            'workflows'
        }


    def expected_replication_method(self):
        return {
            'conversations': 'INCREMENTAL',
            'conversation_threads': 'INCREMENTAL',
            'customers': 'INCREMENTAL',
            'mailboxes': 'INCREMENTAL',
            'mailbox_fields': 'FULL_TABLE',
            'mailbox_folders': 'INCREMENTAL',
            'users': 'INCREMENTAL',
            'workflows': 'INCREMENTAL'
        }


    def expected_pks(self):
        return {
            'conversations': {'id'},
            'conversation_threads': {'id'},
            'customers': {'id'},
            'mailboxes': {'id'},
            'mailbox_fields': {'id'},
            'mailbox_folders': {'id'},
            'users': {'id'},
            'workflows': {'id'}
        }


    def expected_rks(self):
        return {
            'conversations': {'user_updated_at'},
            'conversation_threads': {'updated_at'},
            'customers': {'updated_at'},
            'mailboxes': {'updated_at'},
            'mailbox_fields': {},
            'mailbox_folders': {'updated_at'},
            'users': {'updated_at'},
            'workspaces': {'modified_at'}
        }


    def run_sync(self, conn_id):
        """
        Run a sync job and make sure it exited properly.
        Return a dictionary with keys of streams synced
        and values of records synced for each stream
        """
        # Run a sync job using orchestrator
        sync_job_name = runner.run_sync_mode(self, conn_id)

        # Verify tap and target exit codes
        exit_status = menagerie.get_exit_status(conn_id, sync_job_name)
        menagerie.verify_sync_exit_status(self, exit_status, sync_job_name)

        # Verify actual rows were synced
        sync_record_count = runner.examine_target_output_file(
            self, conn_id, self.expected_sync_streams(), self.expected_pks())
        return sync_record_count


    def min_bookmarks_by_stream(self, sync_records):
        """Return the minimum value for the replication key for each stream"""
        min_bookmarks = {}
        for stream, batch in sync_records.items():

            upsert_messages = [m for m in batch.get('messages') if m['action'] == 'upsert']
            stream_bookmark_key = self.expected_rks().get(stream, set())
            assert len(stream_bookmark_key) == 1  # There shouldn't be a compound replication key
            (stream_bookmark_key, ) = stream_bookmark_key

            bk_values = [message["data"].get(stream_bookmark_key) for message in upsert_messages]
            min_bookmarks[stream] = {stream_bookmark_key: None}
            for bk_value in bk_values:
                if bk_value is None:
                    continue

                if min_bookmarks[stream][stream_bookmark_key] is None:
                    min_bookmarks[stream][stream_bookmark_key] = bk_value

                if bk_value < min_bookmarks[stream][stream_bookmark_key]:
                    min_bookmarks[stream][stream_bookmark_key] = bk_value
        return min_bookmarks


    def max_bookmarks_by_stream(self, sync_records):
        """
        Return the maximum value for the replication key for each stream
        which is the bookmark expected value.
        Comparisons are based on the class of the bookmark value. Dates will be
        string compared which works for ISO date-time strings
        """
        max_bookmarks = {}
        for stream, batch in sync_records.items():

            upsert_messages = [m for m in batch.get('messages') if m['action'] == 'upsert']
            stream_bookmark_key = self.expected_rks().get(stream, set())

            assert len(stream_bookmark_key) == 1  # There shouldn't be a compound replication key
            stream_bookmark_key = stream_bookmark_key.pop()

            bk_values = [message["data"].get(stream_bookmark_key) for message in upsert_messages]
            max_bookmarks[stream] = {stream_bookmark_key: None}
            for bk_value in bk_values:
                if bk_value is None:
                    continue

                if max_bookmarks[stream][stream_bookmark_key] is None:
                    max_bookmarks[stream][stream_bookmark_key] = bk_value

                if bk_value > max_bookmarks[stream][stream_bookmark_key]:
                    max_bookmarks[stream][stream_bookmark_key] = bk_value
        return max_bookmarks


    def test_run(self):
        # Connect to stitch service.
        runner.run_check_job_and_check_status(self)    

        # Get and check streams.
        self.found_catalogs = menagerie.get_catalogs(self.conn_id)
        self.assertEqual(len(self.found_catalogs), 5, msg="unable to locate schemas for connection {}".format(self.conn_id))

        # Match streams.
        our_catalogs = [c for c in self.found_catalogs if c.get('tap_stream_id') in self.expected_sync_streams()]
        for c in our_catalogs:
            c_annotated = menagerie.get_annotated_schema(self.conn_id, c['stream_id'])
            c_metadata = metadata.to_map(c_annotated['metadata'])
            connections.select_catalog_and_fields_via_metadata(self.conn_id, c, c_annotated, [], [])

        # Clear state before our run
        menagerie.set_state(self.conn_id, {})

        # Run a sync job using orchestrator, verify tap and target exit codes
        # and verify actual rows were synced.
        first_sync_record_count = self.run_sync(self.conn_id)

        replicated_row_count =  reduce(lambda accum, c : accum + c, first_sync_record_count.values())
        self.assertGreater(replicated_row_count, 0, msg="failed to replicate any data: {}".format(first_sync_record_count))
        print("total replicated row count: {}".format(replicated_row_count))

        # Get incremental vs. non-incremental streams.
        non_incremental_streams = {key for key, value in self.expected_replication_method().items() if value != 'INCREMENTAL'}
        incremental_streams = {key for key, value in self.expected_replication_method().items() if value == 'INCREMENTAL'}

        # Get bookmark and state data for first sync, excluding full table streams.
        first_sync_state = menagerie.get_state(self.conn_id)
        first_sync_records = runner.get_records_from_target_output()

        for v in non_incremental_streams:
            first_sync_records.pop(v, None)

        first_max_bookmarks = self.max_bookmarks_by_stream(first_sync_records)
        first_min_bookmarks = self.min_bookmarks_by_stream(first_sync_records)

        # Run a second sync job using orchestrator.
        second_sync_record_count = self.run_sync(self.conn_id)
        
        # Get data about rows synced, excluding full table streams.
        second_sync_records = runner.get_records_from_target_output()

        for v in non_incremental_streams:
            second_sync_records.pop(v, None)

        second_min_bookmarks = self.min_bookmarks_by_stream(second_sync_records)

        for stream in incremental_streams:
            # get bookmark values from state and target data
            stream_bookmark_key = self.expected_rks().get(stream, set())
            assert len(stream_bookmark_key) == 1  # There shouldn't be a compound replication key
            stream_bookmark_key = stream_bookmark_key.pop()

            state_value = first_sync_state.get("bookmarks", {}).get(
                stream, {None: None}).get(stream_bookmark_key)
            target_value = first_max_bookmarks.get(
                stream, {None: None}).get(stream_bookmark_key)
            target_min_value = first_min_bookmarks.get(
                stream, {None: None}).get(stream_bookmark_key)

            if target_value:
                # Convert everything to datetime.
                state_value = utils.strptime_with_tz(state_value)
                target_value = utils.strptime_with_tz(target_value)
                target_min_value = utils.strptime_with_tz(target_min_value)

                # verify that there is data with different bookmark values - setup necessary
                self.assertTrue(target_value >= target_min_value, msg="Data isn't set up to be able to test bookmarks")

                # verify state agrees with target data after 1st sync
                self.assertEqual(state_value, target_value, msg="The bookmark value isn't correct based on target data")

                # verify that you get less data the 2nd time around
                self.assertGreater(
                    first_sync_record_count.get(stream, 0),
                    second_sync_record_count.get(stream, 0),
                    msg="second sync didn't have less records, bookmark usage not verified")

                if len(second_sync_records) > 0 and len(second_min_bookmarks) > 0:
                    # verify all data from 2nd sync >= 1st bookmark
                    target_value = second_min_bookmarks.get(stream, {None: None}).get(stream_bookmark_key)
                    target_value = utils.strptime_with_tz(target_value)
                    # verify that the minimum bookmark sent to the target for the second sync
                    # is greater than or equal to the bookmark from the first sync
                    self.assertTrue(target_value >= state_value)

SCENARIOS.add(HelpScoutBase)