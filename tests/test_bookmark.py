import tap_tester.connections as connections
import tap_tester.runner as runner
import tap_tester.menagerie as menagerie
from datetime import timedelta
from datetime import datetime as dt

from base import TestLinkedinAdsBase

class BookmarkTest(TestLinkedinAdsBase):
    """Test tap sets a bookmark and respects it for the next sync of a stream"""

    def name(self):
        return "tap_tester_linkedin_ads_bookmark_test"

    def timedelta_formatted(self, dtime, days, str_format):
        """
        Move ahead dateTime by given days.
        """
        date_stripped = dt.strptime(dtime, str_format)
        return_date = date_stripped + timedelta(days=days)
        return dt.strftime(return_date, str_format)

    def test_run(self):
        """
        • Verify for each incremental stream you can do a sync that records bookmarks, and that the format matches expectations.
        • Verify that the bookmark is the maximum value sent to the target for the replication key.
        • Verify that a second sync respects the bookmark
            - All data of the second sync is >= the bookmark from the first sync
            - The number of records in the 2nd sync is less than the first (This assumes that
              new data added to the stream is done at a rate slow enough that you haven't
              doubled the amount of data from the start date to the first sync between
              the first sync and second sync run in this test)
        Verify that for the FULL_TABLE stream,
            • Verify that a bookmark doesn't exist for full table streams.
            • All data replicated in sync 1 is replicated again in sync 2.

        PREREQUISITE
        For EACH stream that is incrementally replicated, there are multiple rows of data with
            different values for the replication key
        """

        streams_to_test = self.expected_streams()

        expected_replication_keys = self.expected_replication_keys()
        expected_replication_methods = self.expected_replication_method()

        ##########################################################################
        # First Sync
        ##########################################################################
        conn_id = connections.ensure_connection(self)

        # Run in check mode
        found_catalogs = self.run_and_verify_check_mode(conn_id)

        # Table and field selection
        catalog_entries = [catalog for catalog in found_catalogs
                           if catalog.get('tap_stream_id') in streams_to_test]

        self.perform_and_verify_table_and_field_selection(
            conn_id, catalog_entries)

        # Run a first sync job using orchestrator
        first_sync_record_count = self.run_and_verify_sync(conn_id)
        first_sync_records = runner.get_records_from_target_output()
        first_sync_bookmarks = menagerie.get_state(conn_id)

        ##########################################################################
        # Update State Between Syncs
        ##########################################################################

        simulated_states = self.calculated_states_by_stream(
            first_sync_bookmarks)
        new_states = {'bookmarks': simulated_states}
        menagerie.set_state(conn_id, new_states)

        ##########################################################################
        # Second Sync
        ##########################################################################

        second_sync_record_count = self.run_and_verify_sync(conn_id)
        second_sync_records = runner.get_records_from_target_output()
        second_sync_bookmarks = menagerie.get_state(conn_id)

        ##########################################################################
        # Test By Stream
        ##########################################################################


        for stream in streams_to_test:
            with self.subTest(stream=stream):

                # Expected values
                expected_replication_method = expected_replication_methods[stream]

                # Collect information for assertions from syncs 1 & 2 based on expected values
                first_sync_count = first_sync_record_count.get(stream, 0)
                second_sync_count = second_sync_record_count.get(stream, 0)
                first_sync_messages = [record.get('data') for record in
                                        first_sync_records.get(
                                            stream, {}).get('messages', [])
                                        if record.get('action') == 'upsert']
                second_sync_messages = [record.get('data') for record in
                                        second_sync_records.get(
                                            stream, {}).get('messages', [])
                                        if record.get('action') == 'upsert']
                first_bookmark_value = first_sync_bookmarks.get('bookmarks', {stream: None}).get(stream)
                second_bookmark_value = second_sync_bookmarks.get('bookmarks', {stream: None}).get(stream)


                if expected_replication_method == self.INCREMENTAL:

                    # Collect information specific to incremental streams from syncs 1 & 2
                    replication_key = next(
                        iter(expected_replication_keys[stream]))

                    simulated_bookmark_value = new_states['bookmarks'][stream]
                    if stream in ['ad_analytics_by_campaign', 'ad_analytics_by_creative']:
                        # These 2 streams follow look_back_window of 7 days. So, records of the past 7 days from
                        # the bookmark would also be available in the next sync.
                        simulated_bookmark_value = self.timedelta_formatted(simulated_bookmark_value, days=-7, str_format="%Y-%m-%dT%H:%M:%S.%fZ")

                    # Verify the first sync sets a bookmark of the expected form
                    self.assertIsNotNone(first_bookmark_value)

                    # Verify the second sync sets a bookmark of the expected form
                    self.assertIsNotNone(second_bookmark_value)

                    # Verify the second sync bookmark is Equal to the first sync bookmark
                    # assumes no changes to data during test
                    self.assertEqual(second_bookmark_value, first_bookmark_value)

                    for record in first_sync_messages:

                        # Verify the first sync bookmark value is the max replication key value for a given stream
                        replication_key_value = record.get(replication_key)

                        self.assertLessEqual(
                            replication_key_value, first_bookmark_value,
                            msg="First sync bookmark was set incorrectly, a record with a greater replication-key value was synced."
                        )

                    for record in second_sync_messages:
                        # Verify the second sync replication key value is Greater or Equal to the simulated bookmark
                        replication_key_value = record.get(replication_key)

                        self.assertGreaterEqual(replication_key_value, simulated_bookmark_value,
                                                msg="Second sync records do not respect the previous bookmark.")

                        # Verify the second sync bookmark value is the max replication key value for a given stream
                        self.assertLessEqual(
                            replication_key_value, second_bookmark_value,
                            msg="Second sync bookmark was set incorrectly, a record with a greater replication-key value was synced."
                        )

                elif expected_replication_method == self.FULL_TABLE:

                    # Verify the syncs do not set a bookmark for full table streams
                    self.assertIsNone(first_bookmark_value)
                    self.assertIsNone(second_bookmark_value)

                    # Verify the number of records in the second sync is the same as the first
                    self.assertEqual(second_sync_count, first_sync_count)

                else:
                    raise NotImplementedError(
                        "INVALID EXPECTATIONS\t\tSTREAM: {} REPLICATION_METHOD: {}".format(
                            stream, expected_replication_method)
                    )

                # Verify at least 1 record was replicated in the second sync
                self.assertGreater(
                    second_sync_count, 0, msg="We are not fully testing bookmarking for {}".format(stream))
