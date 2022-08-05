
        return "tap_tester_linedin_ads_discovery_test"

    def test_run(self):
        """
        Testing that discovery creates the appropriate catalog with valid metadata.

        • Verify number of actual streams discovered matches expected
        • Verify the stream names discovered were what we expect
        • Verify stream names follow the naming convention
          streams should only have lowercase alphas and underscore
        • Verify there are no duplicate/conflicting metadata entries.
        • verify there is only 1 top level breadcrumb
        • Verify replication key(s) match expectations.
        • verify primary key(s)
        • Verify the actual replication method matches our expected replication method.
        • verify that primary keys are given the inclusion of automatic.
        • verify that all fields have the inclusion of available metadata.
        """
        streams_to_test = self.expected_streams()

        conn_id = connections.ensure_connection(self)

        found_catalogs = self.run_and_verify_check_mode(conn_id)
        # Verify stream names follow the naming convention
        # streams should only have lowercase alphas and underscores
        found_catalog_names = {c['tap_stream_id'] for c in found_catalogs}
        self.assertTrue(all([re.fullmatch(r"[a-z_]+",  name) for name in found_catalog_names]),
                        msg="One or more streams don't follow standard naming")

        for stream in streams_to_test:
            with self.subTest(stream=stream):

                # Verify ensure the catalog is found for a given stream
                catalog = next(iter([catalog for catalog in found_catalogs
                                     if catalog["stream_name"] == stream]))
                self.assertIsNotNone(catalog)

                # collecting expected values
                expected_primary_keys = self.expected_primary_keys()[stream]
                expected_replication_keys = self.expected_replication_keys()[stream]
                expected_replication_method = self.expected_replication_method()[stream]

                # add primary keys and replication keys in automatically replicated keys to check
                expected_automatic_fields = expected_primary_keys | expected_replication_keys

                # add "date_range", "pivot", "pivot_value" as automatic
                # for "ad_analytics_by_campaign" and "ad_analytics_by_creative"
                if stream in ["ad_analytics_by_campaign", "ad_analytics_by_creative"]:
                    expected_automatic_fields |= set(['pivot', 'date_range', 'pivot_value'])

                # collecting actual values...
                schema_and_metadata = menagerie.get_annotated_schema(conn_id, catalog['stream_id'])
                metadata = schema_and_metadata["metadata"]
                stream_properties = [item for item in metadata if item.get("breadcrumb") == []]
                actual_primary_keys = set(
                    stream_properties[0].get(
                        "metadata", {self.PRIMARY_KEYS: []}).get(self.PRIMARY_KEYS, [])
                )
                actual_replication_keys = set(
                    stream_properties[0].get(
                        "metadata", {self.REPLICATION_KEYS: []}).get(self.REPLICATION_KEYS, [])
                )
                actual_replication_method = stream_properties[0].get(
                    "metadata", {self.REPLICATION_METHOD: None}).get(self.REPLICATION_METHOD)
                actual_automatic_fields = set(
                    item.get("breadcrumb", ["properties", None])[1] for item in metadata
                    if item.get("metadata").get("inclusion") == "automatic"
                )

                actual_fields = []
                for md_entry in metadata:
                    if md_entry['breadcrumb'] != []:
                        actual_fields.append(md_entry['breadcrumb'][1])

                ##########################################################################
                ### metadata assertions
                ##########################################################################

                # verify there is only 1 top-level breadcrumb in metadata
                self.assertTrue(len(stream_properties) == 1,
                                msg="There is NOT only one top level breadcrumb for {}".format(stream) + \
                                "\nstream_properties | {}".format(stream_properties))

                # verify there is no duplicate metadata entries
                self.assertEqual(len(actual_fields), len(set(actual_fields)), msg = "duplicates in the fields retrieved")

                # verify replication key(s) match expectations
                self.assertEqual(expected_replication_keys, actual_replication_keys,
                                    msg="expected replication key {} but actual is {}".format(
                                        expected_replication_keys, actual_replication_keys))

                # verify primary key(s) match expectations
                self.assertSetEqual(
                    expected_primary_keys, actual_primary_keys,
                )

                # verify the replication method matches our expectations
                self.assertEqual(expected_replication_method, actual_replication_method,
                                    msg="The actual replication method {} doesn't match the expected {}".format(
                                        actual_replication_method, expected_replication_method))

                # verify that if there is a replication key we are doing INCREMENTAL otherwise FULL
                if expected_replication_keys:
                    self.assertEqual(self.INCREMENTAL, actual_replication_method)
                else:
                    self.assertEqual(self.FULL_TABLE, actual_replication_method)

                # verify that primary keys
                # are given the inclusion of automatic in metadata.
                self.assertSetEqual(expected_automatic_fields, actual_automatic_fields)

                # verify that all other fields have the inclusion of available
                # This assumes there are no unsupported fields for SaaS sources
                self.assertTrue(
                    all({item.get("metadata").get("inclusion") == "available"
                         for item in metadata
                         if item.get("breadcrumb", []) != []
                         and item.get("breadcrumb", ["properties", None])[1]
                         not in actual_automatic_fields}),
                    msg="Not all non key properties are set to available in metadata")
