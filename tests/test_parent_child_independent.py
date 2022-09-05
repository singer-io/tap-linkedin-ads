from tap_tester import connections
from base import TestLinkedinAdsBase

class LinkedinAdsParentChildIndependentTest(TestLinkedinAdsBase):

    def name(self):
        return "tap_tester_linedin_ads_parent_child_test"

    def test_run(self):
        """
        Testing that tap is working fine if only child streams are selected
        • Verify that if only child streams are selected then only child streams are replicated.
        """

        child_streams = {"video_ads", "creatives", "ad_analytics_by_campaign", "ad_analytics_by_creative"}

        # Instantiate connection
        conn_id = connections.ensure_connection(self)

        # Run check mode
        found_catalogs = self.run_and_verify_check_mode(conn_id)

        # Table and field selection
        test_catalogs = [catalog for catalog in found_catalogs
                         if catalog.get('stream_name') in child_streams]

        self.perform_and_verify_table_and_field_selection(conn_id, test_catalogs)

        # Run initial sync
        record_count_by_stream = self.run_and_verify_sync(conn_id)

        # Verify no unexpected streams were replicated
        synced_stream_names = set(record_count_by_stream.keys())
        self.assertSetEqual(child_streams, synced_stream_names)
