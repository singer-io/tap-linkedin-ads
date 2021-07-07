from tap_tester import connections
from base import TestLinkedinAdsBase

class LinkedinAdsSyncTest(TestLinkedinAdsBase):

    @staticmethod
    def name():
        return "tap_tester_linkedin_ads_sync_test"

    def test_run(self):
        """
        Testing that sync creates the appropriate catalog with valid metadata.
        Verify that all fields and all streams have selected set to True in the metadata
        """
        conn_id = connections.ensure_connection(self)

        found_catalogs = self.run_and_verify_check_mode(conn_id)

        self.perform_and_verify_table_and_field_selection(conn_id,found_catalogs)

        record_count_by_stream = self.run_and_verify_sync(conn_id)

        # check if all streams have collected records
        for stream in self.expected_streams():
            self.assertGreater(record_count_by_stream.get(stream, 0), 0)