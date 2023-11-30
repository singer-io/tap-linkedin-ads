from  tap_tester import connections, runner
from base import TestLinkedinAdsBase

class AutomaticFieldsTest(TestLinkedinAdsBase):
    """
    Ensure running the tap with all streams selected and all fields deselected results in the replication of just the
    primary keys and replication keys (automatic fields).
    """

    def name(self):
        return "tap_tester_linedin_ads_automatic_fields_test"

    def test_run(self):
        """
        • Verify we can deselect all fields except when inclusion=automatic, which is handled by base.py methods
        • Verify that only the automatic fields are sent to the target.
        • Verify that all replicated records have unique primary key values.
        """

        streams_to_test = self.expected_streams()
        # Skip `ad_analytics_by_campaign` and `ad_analytics_by_creative` from the test because we pass only selected fields
        # in the API param of these streams and that's why in this test we get 0 records.
        # So, if we select at least one available field then API returns a record otherwise it returns 0 records.
        streams_to_test = streams_to_test - {'ad_analytics_by_campaign', 'ad_analytics_by_creative'}

        conn_id = connections.ensure_connection(self)

        found_catalogs = self.run_and_verify_check_mode(conn_id)

        # Table and field selection
        test_catalogs_automatic_fields = [catalog for catalog in found_catalogs
                                          if catalog.get('tap_stream_id') in streams_to_test]

        # Select all streams and no fields within streams
        self.perform_and_verify_table_and_field_selection(
            conn_id, test_catalogs_automatic_fields, select_all_fields=False)

        record_count_by_stream = self.run_and_verify_sync(conn_id)
        synced_records = runner.get_records_from_target_output()

        for stream in streams_to_test:
            with self.subTest(stream=stream):

                # Expected values
                expected_keys = self.expected_automatic_fields().get(stream)
                expected_primary_keys = self.expected_primary_keys()[stream]

                # Collect actual values
                data = synced_records.get(stream, {})
                record_messages_keys = [set(row['data'].keys())
                                        for row in data.get('messages', [])]
                primary_keys_list = [tuple(message.get('data', {}).get(expected_pk) for expected_pk in expected_primary_keys)
                                     for message in data.get('messages', [])
                                     if message.get('action') == 'upsert']
                unique_primary_keys_list = set(primary_keys_list)

                # Verify that you get some records for each stream
                self.assertGreater(
                    record_count_by_stream.get(stream, -1), 0,
                    msg="The number of records is not over the stream min limit")

                # Verify that only the automatic fields are sent to the target
                for actual_keys in record_messages_keys:
                    self.assertSetEqual(expected_keys, actual_keys)

                #Verify that all replicated records have unique primary key values.
                self.assertEqual(len(primary_keys_list),
                                 len(unique_primary_keys_list),
                                 msg="Replicated record does not have unique primary key values.")
