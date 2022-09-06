from tap_tester import connections, runner, LOGGER

from base import TestLinkedinAdsBase

class LinkedinAdsStartDateTest(TestLinkedinAdsBase):
    """
    Ensure both expected streams respect the start date. Run tap in check mode, 
    run 1st sync with start date = 2018-01-01T00:00:00Z,
    run check mode and 2nd sync on a new connection with start date = 2021-08-07T00:00:00Z/2019-08-01T00:00:00Z.
    """

    start_date_1 = ""
    start_date_2 = ""

    @staticmethod
    def name():
        return "tap_tester_linkedin_ads_start_date_test"

    def test_run(self):

        streams_to_test = {"account_users"}
        self.run_start_date(streams_to_test, "2021-08-07T00:00:00Z")

        self.run_start_date(self.expected_streams() - streams_to_test, "2019-08-01T00:00:00Z")

    def run_start_date(self, expected_streams, start_date_2):
        """
        Instantiate start_date according to the desired data set and run the test.

        • Verify the total number of records replicated in sync1 is greater than sync2
        • Verify bookmark key values are greater than or equal to the start_date of sync1 and sync2
        • Verify the number of records replicated in sync 1 is greater than the number of records replicated in sync 2 for the stream
        • Verify the records replicated in sync 2 were also replicated in sync 1
        """

        self.start_date_1 = '2018-01-01T00:00:00Z'
        self.start_date_2 = start_date_2

        start_date_1_epoch = self.dt_to_ts(self.start_date_1, self.START_DATE_FORMAT)
        start_date_2_epoch = self.dt_to_ts(self.start_date_2, self.START_DATE_FORMAT)

        # Set start date 1
        self.START_DATE = self.start_date_1

        expected_replication_methods = self.expected_replication_method()

        ##########################################################################
        ### First Sync
        ##########################################################################

        # Instantiate connection
        conn_id_1 = connections.ensure_connection(self, original_properties=False)

        # Run check mode
        found_catalogs_1 = self.run_and_verify_check_mode(conn_id_1)

        # Table and field selection
        test_catalogs_1_all_fields = [catalog for catalog in found_catalogs_1
                                      if catalog.get('stream_name') in expected_streams]
        self.perform_and_verify_table_and_field_selection(conn_id_1, test_catalogs_1_all_fields, select_all_fields=True)

        # Run initial sync
        record_count_by_stream_1 = self.run_and_verify_sync(conn_id_1)
        synced_records_1 = runner.get_records_from_target_output()

        ##########################################################################
        ### Update START DATE Between Syncs
        ##########################################################################

        LOGGER.info("REPLICATION START DATE CHANGE: {} ===>>> {} ".format(self.START_DATE, self.start_date_2))
        # Set start date 2
        self.START_DATE = self.start_date_2

        ##########################################################################
        ### Second Sync
        ##########################################################################

        # Create a new connection with the new start_date
        conn_id_2 = connections.ensure_connection(self, original_properties=False)

        # Run check mode
        found_catalogs_2 = self.run_and_verify_check_mode(conn_id_2)

        # Table and field selection
        test_catalogs_2_all_fields = [catalog for catalog in found_catalogs_2
                                      if catalog.get('stream_name') in expected_streams]
        self.perform_and_verify_table_and_field_selection(conn_id_2, test_catalogs_2_all_fields, select_all_fields=True)

        # Run sync
        record_count_by_stream_2 = self.run_and_verify_sync(conn_id_2)
        synced_records_2 = runner.get_records_from_target_output()

        for stream in expected_streams:

            # Skipping these fields as there is not enough data available
            if stream in ["accounts"]:
                continue


            with self.subTest(stream=stream):

                # Expected values
                expected_primary_keys = self.expected_primary_keys()[stream]
                expected_start_date_keys = self.expected_start_date_keys()[stream]
                expected_replication_method = expected_replication_methods[stream]

                # Collect information for assertions from syncs 1 & 2 base on expected values
                record_count_sync_1 = record_count_by_stream_1.get(stream, 0)
                record_count_sync_2 = record_count_by_stream_2.get(stream, 0)
                primary_keys_list_1 = [tuple(message.get('data').get(expected_pk) for expected_pk in expected_primary_keys)
                                       for message in synced_records_1.get(stream).get('messages')
                                       if message.get('action') == 'upsert']
                primary_keys_list_2 = [tuple(message.get('data').get(expected_pk) for expected_pk in expected_primary_keys)
                                       for message in synced_records_2.get(stream).get('messages')
                                       if message.get('action') == 'upsert']

                primary_keys_sync_1 = set(primary_keys_list_1)
                primary_keys_sync_2 = set(primary_keys_list_2)

                # Expected bookmark key is one element in set so directly access it
                start_date_keys_list_1 = [message.get('data').get(next(iter(expected_start_date_keys))) for message in synced_records_1.get(stream).get('messages')
                                        if message.get('action') == 'upsert']
                start_date_keys_list_2 = [message.get('data').get(next(iter(expected_start_date_keys))) for message in synced_records_2.get(stream).get('messages')
                                        if message.get('action') == 'upsert']

                start_date_key_sync_1 = set(start_date_keys_list_1)
                start_date_key_sync_2 = set(start_date_keys_list_2)

                # Verify that sync 2 has at least one record synced
                self.assertGreater(record_count_by_stream_2.get(stream, 0), 0)

                if expected_replication_method == self.INCREMENTAL:

                    # Verify bookmark key values are greater than or equal to start_date of sync 1
                    for start_date_key_value in start_date_key_sync_1:
                        self.assertGreaterEqual(self.dt_to_ts(start_date_key_value, self.RECORD_REPLICATION_KEY_FORMAT), start_date_1_epoch)

                    # Verify bookmark key values are greater than or equal to start_date of sync 2
                    for start_date_key_value in start_date_key_sync_2:
                        self.assertGreaterEqual(self.dt_to_ts(start_date_key_value, self.RECORD_REPLICATION_KEY_FORMAT), start_date_2_epoch)

                if self.expected_metadata()[stream][self.OBEYS_START_DATE]:

                    # Verify the number of records replicated in sync 1 is greater than the number
                    # of records replicated in sync 2 for stream
                    self.assertGreaterEqual(record_count_sync_1, record_count_sync_2)

                    # Verify the records replicated in sync 2 were also replicated in sync 1
                    self.assertTrue(primary_keys_sync_2.issubset(primary_keys_sync_1))

                else:

                    # Verify that the 2nd sync with a later start_date replicates the same number of
                    # records as the 1st sync.
                    self.assertEqual(record_count_sync_2, record_count_sync_1)

                    # Verify by primary key the same records are replicated in the 1st and 2nd syncs
                    self.assertSetEqual(primary_keys_sync_1, primary_keys_sync_2)
