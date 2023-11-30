from math import ceil
from tap_tester import runner, connections

from base import TestLinkedinAdsBase

class LinkedinAdsPaginationTest(TestLinkedinAdsBase):
    """
    Ensure tap can replicate multiple pages of data for streams that use pagination.
    """

    @staticmethod
    def name():
        return "tap_tester_linkedin_ads_pagination_test"

    def test_run(self):
        page_size = 1
        conn_id = connections.ensure_connection(self)

        # "ad_analytics_by_creative" and "ad_analytics_by_campaign" does not support pagination
        # Documentation: https://docs.microsoft.com/en-us/linkedin/marketing/integrations/ads-reporting/ads-reporting?tabs=http
        expected_streams = self.expected_streams() - set({"ad_analytics_by_campaign", "ad_analytics_by_creative"})
        found_catalogs = self.run_and_verify_check_mode(conn_id)

        # Table and field selection
        test_catalogs = [catalog for catalog in found_catalogs
                                      if catalog.get('stream_name') in expected_streams]

        self.perform_and_verify_table_and_field_selection(conn_id, test_catalogs)

        self.run_and_verify_sync(conn_id)

        synced_records = runner.get_records_from_target_output()

        for stream in expected_streams:
            with self.subTest(stream=stream):
                # Expected values
                expected_primary_keys = self.expected_primary_keys()

                # Collect information for assertions from sync based on expected values
                primary_keys_list = [(message.get('data').get(expected_pk) for expected_pk in expected_primary_keys)
                                       for message in synced_records.get(stream).get('messages')
                                       if message.get('action') == 'upsert']

                # Chunk the replicated records (just primary keys) into expected pages
                pages = []
                page_count = ceil(len(primary_keys_list) / page_size)
                for page_index in range(page_count):
                    page_start = page_index * page_size
                    page_end = (page_index + 1) * page_size
                    pages.append(set(primary_keys_list[page_start:page_end]))

                # Verify by primary keys that data is unique for each page
                for current_index, current_page in enumerate(pages):
                    with self.subTest(current_page_primary_keys=current_page):

                        for other_index, other_page in enumerate(pages):
                            if current_index == other_index:
                                continue  # don't compare the page to itself

                            self.assertTrue(current_page.isdisjoint(other_page),
                                            msg=f'other_page_primary_keys={other_page}')
