from tap_tester import runner, connections, menagerie

from base import TestLinkedinAdsBase

# As we can't find the below fields in the docs and also
# it won't be generated by Linkedin APIs now so expected.
KNOWN_MISSING_FIELDS = {
    "account_users": {
        "campaign_contact"
    },
    "creatives": {
        "reference_share_id",
    },
    "campaigns": {
        "associated_entity_person_id",
        "targeting",
        "version_tag",
    },
    "campaign_groups": {
        "allowed_campaign_types"
    },
    "video_ads": {
        "content_reference_share_id",
        "content_reference_ucg_post_id",
        "change_audit_stamps"
    },
    "accounts": {
        "total_budget_ends_at",
        "total_budget",
        "reference_person_id",
    },
    "ad_analytics_by_creative": {
        "average_daily_reach_metrics",
        "average_previous_seven_day_reach_metrics",
        "average_previous_thirty_day_reach_metrics",
        #BUG: TDL-22692
        "approximate_unique_impressions",
        #BUG: TDL-25618
        'job_applications',
        'viral_post_view_job_applications',
        'post_click_job_applications',
        'external_website_post_view_conversions',
        'viral_external_website_conversions',
        'viral_post_view_registrations',
        'talent_leads',
        'viral_registrations',
        'post_view_job_applications',
        'viral_external_website_post_click_conversions',
        'post_click_registrations',
        'job_apply_clicks',
        'conversion_value_in_local_currency',
        'registrations',
        'post_view_registrations',
        'viral_post_view_job_apply_clicks',
        'viral_external_website_post_view_conversions',
        'viral_post_click_job_apply_clicks',
        'viral_job_apply_clicks',
        'viral_job_applications',
        'viral_post_click_job_applications',
        'viral_post_click_registrations',
        'external_website_post_click_conversions',
        'post_view_job_apply_clicks',
        'post_click_job_apply_clicks'
    },
    "ad_analytics_by_campaign": {
        "average_daily_reach_metrics"
        "average_previous_seven_day_reach_metrics",
        "average_previous_thirty_day_reach_metrics",
        #BUG: TDL-22692
        "approximate_unique_impressions",
        #BUG: TDL-25618 
        'job_applications',
        'viral_post_view_job_applications',
        'post_click_job_applications',
        'external_website_post_view_conversions',
        'viral_external_website_conversions',
        'viral_post_view_registrations',
        'talent_leads',
        'viral_registrations',
        'post_view_job_applications',
        'viral_external_website_post_click_conversions',
        'post_click_registrations',
        'job_apply_clicks',
        'conversion_value_in_local_currency',
        'registrations',
        'post_view_registrations',
        'viral_post_view_job_apply_clicks',
        'viral_external_website_post_view_conversions',
        'viral_post_click_job_apply_clicks',
        'viral_job_apply_clicks',
        'viral_job_applications',
        'viral_post_click_job_applications',
        'viral_post_click_registrations',
        'external_website_post_click_conversions',
        'post_view_job_apply_clicks',
        'post_click_job_apply_clicks'
    },
}

class AllFields(TestLinkedinAdsBase):
    """Test that with all fields selected for a stream automatic and available fields are  replicated"""

    @staticmethod
    def name():
        return "tap_tester_linkedin_ads_all_fields"

    def test_run(self):
        """
        Ensure running the tap with all streams and fields selected results in the
        Replication of all fields.
        - Verify no unexpected streams were replicated
        - Verify that more than just the automatic fields are replicated for each stream.
        """

        expected_streams = self.expected_streams() - {"account_users"}

        # Instantiate connection
        conn_id = connections.ensure_connection(self)

        # Run check mode
        found_catalogs = self.run_and_verify_check_mode(conn_id)

        # Table and field selection
        test_catalogs_all_fields = [catalog for catalog in found_catalogs
                                    if catalog.get('stream_name') in expected_streams]
        self.perform_and_verify_table_and_field_selection(
            conn_id, test_catalogs_all_fields, select_all_fields=True,
        )

        # Grab metadata after performing table-and-field selection to set expectations
        stream_to_all_catalog_fields = dict() # used for asserting all fields are replicated
        for catalog in test_catalogs_all_fields:
            stream_id, stream_name = catalog['stream_id'], catalog['stream_name']
            catalog_entry = menagerie.get_annotated_schema(conn_id, stream_id)
            fields_from_field_level_md = [md_entry['breadcrumb'][1]
                                          for md_entry in catalog_entry['metadata']
                                          if md_entry['breadcrumb'] != []]
            stream_to_all_catalog_fields[stream_name] = set(fields_from_field_level_md)

        # Run initial sync
        record_count_by_stream = self.run_and_verify_sync(conn_id)
        synced_records = runner.get_records_from_target_output()

        # Verify no unexpected streams were replicated
        synced_stream_names = set(synced_records.keys())
        self.assertSetEqual(expected_streams, synced_stream_names)

        for stream in expected_streams:
            with self.subTest(stream=stream):

                # Expected values
                expected_automatic_keys = self.expected_automatic_fields().get(stream)

                # Get all expected keys
                expected_all_keys = stream_to_all_catalog_fields[stream]

                # Collect actual values
                messages = synced_records.get(stream)

                actual_all_keys = set()
                for message in messages['messages']:
                    if message['action'] == 'upsert':
                        actual_all_keys.update(set(message['data'].keys()))

                # Verify that you get some records for each stream
                self.assertGreater(record_count_by_stream.get(stream, -1), 0)

                # Verify all fields for a stream were replicated
                self.assertGreater(len(expected_all_keys), len(expected_automatic_keys))
                self.assertTrue(expected_automatic_keys.issubset(expected_all_keys), msg=f'{expected_automatic_keys-expected_all_keys} is not in "expected_all_keys"')

                self.assertSetEqual(expected_all_keys.difference(KNOWN_MISSING_FIELDS.get(stream, set())), actual_all_keys)
