from decimal import Decimal
import unittest
from unittest import mock
from parameterized import parameterized
from tap_linkedin_ads.transform import (convert, snake_case_to_camel_case, convert_array, convert_json,
                                        transform_accounts, transform_analytics, transform_json,
                                        transform_campaigns, transform_creatives, transform_audit_fields,
                                        transform_urn, transform_data, string_to_decimal)


class TestConvertCamelcaseToSnakeCase(unittest.TestCase):
    """
    Test `convert` function to convert camel case text to snake case
    """

    @parameterized.expand([
        ("registration", "registration"),
        ("viralVideoViews", "viral_video_views"),
    ])
    def test_convert(self, name, expected_value):
        """
        Test that if the text is in camel-case, convert it into snake-case,
        otherwise, return as it is.
        """
        converted_name = convert(name)

        # Verify expected name is returned
        self.assertEqual(converted_name, expected_value)


class TestConvertSnakecaseToCamelcase(unittest.TestCase):
    """
    Test `snake_case_to_camel_case` function.
    """
    @parameterized.expand([
        ("registration", "registration"),
        ("viral_video_views", "viralVideoViews"),
        ("","")
    ])
    def test_convert(self, name, expected_value):
        """
        Test that if the text is in snake-case, convert it into camel-case,
        otherwise, return as it is.
        """
        converted_name = snake_case_to_camel_case(name)

        # Verify expected name is returned
        self.assertEqual(converted_name, expected_value)


@mock.patch("tap_linkedin_ads.transform.convert_json", return_value = {'dummy_data': 'data_1'})
class TestConvertArray(unittest.TestCase):
    """
    Test `convert_array` function to transform array.
    """

    test_array_1 = ["1","2","3"]
    test_array_2 = [[1,2,3]]
    test_array_3 = [{"dummyData": "data_1"}]

    @parameterized.expand([
        (test_array_1, test_array_1),
        (test_array_2, test_array_2),
        (test_array_3, [{'dummy_data': 'data_1'}]),
    ])
    def test_convert_array(self, mock_convert, test_array, expected_array):
        """
        Test handling of dictionary, array, and other types of items in function.
        """
        transformed_array = convert_array(test_array)

        # Verify returned array is expected
        self.assertEqual(transformed_array, expected_array)


@mock.patch("tap_linkedin_ads.transform.convert_array", return_value = [1, 2, 3])
class TestConvertJson(unittest.TestCase):
    """
    Test `convert_json` function.
    """

    test_dict_1 = {"dictField1": "value1", "dictField2": "value2"}
    exp_dict_1 = {"dict_field1": "value1", "dict_field2": "value2"}
    test_dict_2 = {"dictField1": [1, 2, 3], "dictField2": [1, 2, 3]}
    exp_dict_2 = {"dict_field1": [1, 2, 3], "dict_field2": [1, 2, 3]}
    test_dict_3 = {"dictField1": {}, "dictField2": {}}
    exp_dict_3 = {"dict_field1": {}, "dict_field2": {}}

    @parameterized.expand([
        (test_dict_1, exp_dict_1),
        (test_dict_2, exp_dict_2),
        (test_dict_3, exp_dict_3),
    ])
    def test_convert_json(self, mock_convert_array, test_dict, expected_dict):
        """
        Test that each key is converted to camel-case, and
        handling of dictionary, array, and other types of value.
        """
        transformed_dict = convert_json(test_dict)

        # Verify returned dict is expected
        self.assertEqual(transformed_dict, expected_dict)

    @mock.patch("tap_linkedin_ads.transform.LOGGER.error")
    @mock.patch("tap_linkedin_ads.transform.convert")
    def test_error_handling(self, mock_convert, mock_logger, mock_convert_array):
        """
        Test handling of `TypeError`.
        """
        mock_convert.side_effect = TypeError

        with self.assertRaises(TypeError):
            convert_json({"error-key": ""})

        # Verify logger is called with expected args
        mock_logger.assert_called_with("Error key = %s", "error-key")


class TestStringToDecimal(unittest.TestCase):
    """
    Test `string_to_decimal` function.
    """

    @parameterized.expand([
        ("10.111", Decimal("10.111")),
        ("abc", None),
    ])
    def test_correct_value(self, value, expected_value):
        """
        Test that the function returns a Decimal value for correct input,
        and None for incorrect value.
        """
        return_value = string_to_decimal(value)

        # Verify return value is expected
        self.assertEqual(return_value, expected_value)


class TestTransformAccounts(unittest.TestCase):
    """
    Test `transform_accounts` function.
    """
    
    @parameterized.expand([
        ({"total_budget": "10.1"}, {"total_budget": Decimal('10.1')}),
        ({"dummy_field": "10.1"}, {"dummy_field": "10.1"}),
    ])
    def test_transform(self, test_dict, expected_dict):
        """
        Test that `currency_fields` are converted to Decimal object.
        """

        transformed_dict = transform_accounts(test_dict)

        # Verify returned dict is expected
        self.assertEqual(transformed_dict, expected_dict)


class TestTransformAnalytics(unittest.TestCase):
    """
    Test `transform_analytics` function.
    """

    test_dict_1 = {
        "conversion_value_in_local_currency": "0",
        "cost_in_local_currency": "24.9200000000006",
        "cost_in_usd": "24.9200000000006",
        "pivot": "CREATIVE",
        "pivot_value": "urn:li:sponsoredCreative:84316234",
        "date_range": {
            "start": {"year":2020, "month": 10, "day": 11},
            "end": {"year":2010, "month": 5, "day": 15},
        },
    }
    exp_dict_1 = {
        **test_dict_1,
        "conversion_value_in_local_currency": Decimal("0"),
        "cost_in_local_currency": Decimal("24.9200000000006"),
        "cost_in_usd": Decimal("24.9200000000006"),
        "creative": "urn:li:sponsoredCreative:84316234",
        "start_at": "2020-10-11T00:00:00.000000Z",
        "end_at": "2010-05-16T00:00:00Z",
    }

    @parameterized.expand([
        (test_dict_1, exp_dict_1),
    ])
    def test_transform_analytics(self, test_dict, expected_dict):
        """
        Test dictionary is transformed as expected.
        """
        transformed_dict = transform_analytics(test_dict)

        # Verify returned dict is expected
        self.assertEqual(transformed_dict, expected_dict)


class TestTransformCampaign(unittest.TestCase):
    """
    Test `transform_campaigns` function.
    """
    test_dict_1 = {"daily_budget": {"amount": "25"}, "targeting": {}}
    exp_dict_1 = {"daily_budget": {"amount": Decimal("25")}, "targeting": {}}

    test_dict_2 = {"unit_cost": {"amount": "0.01"}, "targeting_criteria": {}}
    exp_dict_2 = {"unit_cost": {"amount": Decimal("0.01")}, "targeting_criteria": {}}

    test_dict_3 = {
        "unit_cost": {"amount": "0.01"},
        "targeting_criteria": {},
        "targeting": {
            "excluded_targeting_facets": {
                "employers": ["urn:li:organization:1035", "urn:li:organization:0000"],
                "employers2": [{"1":1}],
                "dict_field": {"1":1,"2":2}
            },
            "included_targeting_facets": {
                "employers": [ "urn:li:organization:1035", "urn:li:organization:0000"],
                "employers2": [{"1":1}],
                "dict_field": {"1":1,"2":2}
            }
        },
    }
    exp_dict_3 = {
        "unit_cost": {"amount": Decimal("0.01")},
        "targeting_criteria": {"exclude": []},
        "targeting": {
            "excluded_targeting_facets": [
                {"type": "employers", "values": ["urn:li:organization:1035", "urn:li:organization:0000"]},
                {"type": "employers2", "values": ["{'1': 1}"]},
                {"type": "dict_field", "values": ["{'1': 1, '2': 2}"]},
            ],
            "included_targeting_facets": [
                {"type": "employers", "values": ["urn:li:organization:1035", "urn:li:organization:0000"]},
                {"type": "employers2", "values": ["{'1': 1}"]},
                {"type": "dict_field", "values": ["{'1': 1, '2': 2}"]},
            ]
        }
    }

    test_dict_4 = {
        "unit_cost": {"amount": "0.01"},
        "targeting_criteria": {
            "exclude": {
                "or": {
                    "urn:li:locations": ["urn:li:geo:102095887"],
                    "urn:li:locations2": [{"dummy_data": "None"}],
                    "dict_field": {"1": 1},
                },
            },
            "include": {
                "and": [
                    {"or": {"urn:li:industries": ["urn:li:industry:9"]}},
                    {"or": {"urn:li:industries3": [{"dummy_data": "None"}]}},
                    {"or": {"urn:li:industries2": {"1": 1}}},
                ]
            }
        },
        "targeting": {
            "excluded_targeting_facets": {},
            "included_targeting_facets": {},
        },
    }
    exp_dict_4 = {
        "unit_cost": {"amount": Decimal("0.01")},
        "targeting_criteria": {
            "include": {
                "and": [
                    {"type": "urn:li:industries", "values": ["urn:li:industry:9"]},
                    {"type": "urn:li:industries3", "values": ["{'dummy_data': 'None'}"]},
                    {"type": "urn:li:industries2", "values": ["{'1': 1}"]},
                ]
            },
            "exclude": [
                {"type": "urn:li:locations", "values": ["urn:li:geo:102095887"]},
                {"type": "urn:li:locations2", "values": ["{'dummy_data': 'None'}"]},
                {"type": "dict_field", "values": ["{'1': 1}"]},
            ]
        },
        "targeting": {
            "excluded_targeting_facets": [],
            "included_targeting_facets": [],
        }
    }

    @parameterized.expand([
        (test_dict_1, exp_dict_1),
        (test_dict_2, exp_dict_2),
        (test_dict_3, exp_dict_3),
        (test_dict_4, exp_dict_4),
    ])
    def test_transform_campaigns(self, test_dict, expected_dict):
        """
        Test that all fields are transformed as expected.
        """
        transformed_dict = transform_campaigns(test_dict)

        # Verify returned dict is expected
        self.assertEqual(transformed_dict, expected_dict)


class TestTransformCreatives(unittest.TestCase):
    """
    Test `transform_creatives` function.
    """
    test_dict_1 = {
        "variables": {
            "data": {
                "com.linkedin.ads": {"1":1,"2":2}
            }
        }
    }
    exp_dict_1 = {
        'variables': {
            'type': 'com.linkedin.ads',
            'values': [
                {'key': '1', 'value': '1'},
                {'key': '2', 'value': '2'}
            ]
        }
    }
    test_dict_2 = {
        "refrence": ""
    }

    @parameterized.expand([
        (test_dict_1, exp_dict_1),
        (test_dict_2, {**test_dict_2}),
    ])
    def test_transform_creatives(self, test_dict_1, expected_dict):
        """
        Test that fields are converted as expeted.
        """
        transformed_dict = transform_creatives(test_dict_1)

        # Verify returned dict is expected
        self.assertEqual(transformed_dict, expected_dict)


class TestTransformAuditFields(unittest.TestCase):
    """
    Test `transform_audit_fields` function.
    """

    test_dict_1 = {"reference": "urn:li:organization:20111635"}

    test_dict_2 = {
        "change_audit_stamps": {
            "created": {"time": 1563562455000},
            "last_modified": {"time": 1626169039381}
        }
    }
    added_fields_2 = {
        "created_time": 1563562455000,
        "last_modified_time": 1626169039381,
    }

    @parameterized.expand([
        (test_dict_1, {**test_dict_1}),
        (test_dict_2, {**test_dict_2, **added_fields_2}),
    ])
    def test_transform_audit_fields(self, test_dict, expected_dict):
        """
        Test that time fields are added to first level.
        """

        transformed_dict = transform_audit_fields(test_dict)

        # Verify returned dict is expected
        self.assertEqual(transformed_dict, expected_dict)


class TestTransformUrn(unittest.TestCase):
    """
    Test `transform_urn` funcrion.
    """

    test_dict_1 = {"reference": "urn:li:organization:10000000"}
    added_fields_1 = {"reference_organization_id": 10000000}

    test_dict_2 = {"account": "urn:li:sponsoredAccount:10000000"} #with sponsered
    added_fields_2 = {"account_id": 10000000}

    test_dict_3 = {"pivot_value": "urn:li:sponsoredCreative:84316234"}

    @parameterized.expand([
        (test_dict_1, {**test_dict_1, **added_fields_1}),
        (test_dict_2, {**test_dict_2, **added_fields_2}),
        ({**test_dict_3}, {**test_dict_3}),
    ])
    def test_transform_urn(self, test_dict, expected_dict):
        """
        Test that expected fields are added.
        """
        transformed_dict = transform_urn(test_dict)

        # Verify returned dict is expected
        self.assertEqual(transformed_dict, expected_dict)


@mock.patch("tap_linkedin_ads.transform.transform_urn")
@mock.patch("tap_linkedin_ads.transform.transform_audit_fields")
class TestTransformData(unittest.TestCase):
    """
    Test `transform_data` function that it calls other transform function respective to stream_name
    """

    test_dict = {"type": "BUSINESS", "id": 503491473}

    @mock.patch("tap_linkedin_ads.transform.transform_accounts")
    def test_accounts_stream(self, mock_transform_accounts, mock_audit_fields, mock_transform_urn):
        """
        Test for `accounts` stream `transform_accounts` is called.
        """
        mock_audit_fields.return_value = self.test_dict
        transformed_dict = transform_data({"elements": [self.test_dict]*3}, "accounts")

        # Verify transform function called for each element
        self.assertEqual(mock_transform_accounts.call_count, 3)
        self.assertEqual(transformed_dict, {"elements": [self.test_dict]*3})

    @mock.patch("tap_linkedin_ads.transform.transform_campaigns")
    def test_campaigns_stream(self, mock_transform_campaigns, mock_audit_fields, mock_transform_urn):
        """
        Test for `campaigns` stream `transform_campaigns` is called.
        """
        mock_audit_fields.return_value = self.test_dict
        transformed_dict = transform_data({"elements": [self.test_dict]*4}, "campaigns")

        # Verify transform function called for each element
        self.assertEqual(mock_transform_campaigns.call_count, 4)
        self.assertEqual(transformed_dict, {"elements": [self.test_dict]*4})

    @mock.patch("tap_linkedin_ads.transform.transform_analytics")
    def test_analytics_stream(self, mock_transform_analytics, mock_audit_fields, mock_transform_urn):
        """
        Test for any analytics stream `transform_analytics` is called.
        """
        mock_audit_fields.return_value = self.test_dict
        transformed_dict = transform_data({"elements": [self.test_dict]*4}, "ad_analytics_by_creatives")

        # Verify transform function called for each element
        self.assertEqual(mock_transform_analytics.call_count, 4)
        self.assertEqual(transformed_dict, {"elements": [self.test_dict]*4})

    @mock.patch("tap_linkedin_ads.transform.transform_creatives")
    def test_creatives_stream(self, mock_transform_creatives, mock_audit_fields, mock_transform_urn):
        """
        Test for `creatives` stream `transform_creatives` is called.
        """
        mock_audit_fields.return_value = self.test_dict
        transformed_dict = transform_data({"elements": [self.test_dict]*4}, "creatives")

        # Verify transform function called for each element
        self.assertEqual(mock_transform_creatives.call_count, 4)
        self.assertEqual(transformed_dict, {"elements": [self.test_dict]*4})

    @mock.patch("tap_linkedin_ads.transform.transform_analytics")
    @mock.patch("tap_linkedin_ads.transform.transform_campaigns")
    @mock.patch("tap_linkedin_ads.transform.transform_accounts")
    @mock.patch("tap_linkedin_ads.transform.transform_creatives")
    def test_other_streams(self, transform_creatives, transform_accounts, transform_campaigns, transform_analytics,  mock_audit_fields, mock_transform_urn):
        """
        Test for any other streams transformed dictionary is returned.
        """
        mock_audit_fields.return_value = self.test_dict
        transformed_dict = transform_data({"elements": [self.test_dict]*4}, "video_ads")

        # Verify any transform function specific to a stream was not called
        self.assertFalse(transform_creatives.called)
        self.assertFalse(transform_accounts.called)
        self.assertFalse(transform_campaigns.called)
        self.assertFalse(transform_analytics.called)
        self.assertEqual(transformed_dict, {"elements": [self.test_dict]*4})


class TestTransformJson(unittest.TestCase):
    """
    Test `transform_json` function.
    """
    @mock.patch("tap_linkedin_ads.transform.LOGGER.info")
    @mock.patch("tap_linkedin_ads.transform.convert_json")
    @mock.patch("tap_linkedin_ads.transform.transform_data")
    def test_transform_json(self, mock_transform_data, mock_convert_json, mock_logger):
        """
        Test the code flow of `transform_json` function.
        """
        test_dict = {
            "elements": []
        }
        mock_transform_data.return_value = test_dict
        returned_dict = transform_json(test_dict, "accounts")

        mock_convert_json.assert_called_with(test_dict)

        # Verify logger called with expected arguments
        self.assertTrue(mock_logger.called)

        # Verify expected dictionary was returned
        self.assertEqual(returned_dict, test_dict)

