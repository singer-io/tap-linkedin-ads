import unittest
from unittest import mock
from singer.catalog import Catalog
from tap_linkedin_ads.client import LinkedInForbiddenError
from tap_linkedin_ads.discover import discover, _apply_access_checks, _prune_inaccessible_children
from tap_linkedin_ads.schema import STREAMS, get_schemas


class TestDiscover(unittest.TestCase):
    """Test `discover` function."""

    def _make_client(self, accounts="123456"):
        client = mock.MagicMock()
        client.config = {"accounts": accounts}
        return client

    def test_discover_returns_catalog(self):
        """discover() returns a Catalog when all streams are accessible."""
        client = self._make_client()
        with mock.patch(
            "tap_linkedin_ads.discover._apply_access_checks"
        ) as mock_access:
            mock_access.return_value = None
            result = discover(client)
        self.assertIsInstance(result, Catalog)

    def test_discover_catalog_contains_all_streams(self):
        """discover() catalog contains all streams when all are accessible."""
        client = self._make_client()
        with mock.patch(
            "tap_linkedin_ads.discover._apply_access_checks"
        ) as mock_access:
            mock_access.return_value = None
            result = discover(client)
        stream_ids = [s.tap_stream_id for s in result.streams]
        for stream_name in STREAMS:
            self.assertIn(stream_name, stream_ids)

    def test_discover_calls_access_checks(self):
        """discover() calls _apply_access_checks with the client."""
        client = self._make_client()
        with mock.patch(
            "tap_linkedin_ads.discover._apply_access_checks"
        ) as mock_access:
            mock_access.return_value = None
            discover(client)
        mock_access.assert_called_once()
        call_args = mock_access.call_args[0]
        self.assertIs(call_args[0], client)


class TestApplyAccessChecks(unittest.TestCase):
    """Test _apply_access_checks function."""

    def _make_client(self, accounts="123456"):
        client = mock.MagicMock()
        client.config = {"accounts": accounts}
        return client

    def _build_schemas_and_metadata(self):
        return get_schemas()

    def test_all_streams_accessible_no_removal(self):
        """No streams removed when all check_access() return True."""
        client = self._make_client()
        schemas, field_metadata = self._build_schemas_and_metadata()
        original_count = len(schemas)

        with mock.patch.object(
            __import__(
                "tap_linkedin_ads.streams", fromlist=["LinkedInAds"]
            ).LinkedInAds,
            "check_access",
            return_value=True,
        ):
            _apply_access_checks(client, schemas, field_metadata)

        self.assertEqual(len(schemas), original_count)

    def test_inaccessible_parent_stream_removed(self):
        """An inaccessible parent stream is removed from schemas."""
        client = self._make_client()
        schemas, field_metadata = self._build_schemas_and_metadata()

        def _check_access(self, client):
            if self.tap_stream_id == "accounts":
                return False
            return True

        with mock.patch(
            "tap_linkedin_ads.streams.LinkedInAds.check_access",
            new=_check_access,
        ):
            _apply_access_checks(client, schemas, field_metadata)

        self.assertNotIn("accounts", schemas)
        self.assertNotIn("accounts", field_metadata)

    def test_inaccessible_parent_removes_child_streams(self):
        """Child streams are removed when their parent is inaccessible."""
        client = self._make_client()
        schemas, field_metadata = self._build_schemas_and_metadata()

        def _check_access(self, client):
            if self.tap_stream_id == "accounts":
                return False
            return True

        with mock.patch(
            "tap_linkedin_ads.streams.LinkedInAds.check_access",
            new=_check_access,
        ):
            _apply_access_checks(client, schemas, field_metadata)

        # video_ads is a child of accounts
        self.assertNotIn("accounts", schemas)
        self.assertNotIn("video_ads", schemas)

    def test_inaccessible_campaigns_removes_child_streams(self):
        """Child streams of campaigns are removed when campaigns is inaccessible."""
        client = self._make_client()
        schemas, field_metadata = self._build_schemas_and_metadata()

        def _check_access(self, client):
            if self.tap_stream_id == "campaigns":
                return False
            return True

        with mock.patch(
            "tap_linkedin_ads.streams.LinkedInAds.check_access",
            new=_check_access,
        ):
            _apply_access_checks(client, schemas, field_metadata)

        self.assertNotIn("campaigns", schemas)
        self.assertNotIn("creatives", schemas)
        self.assertNotIn("ad_analytics_by_campaign", schemas)
        self.assertNotIn("ad_analytics_by_creative", schemas)

    def test_all_parent_streams_inaccessible_raises(self):
        """Raises LinkedInForbiddenError when ALL parent streams are inaccessible."""
        client = self._make_client()
        schemas, field_metadata = self._build_schemas_and_metadata()

        with mock.patch(
            "tap_linkedin_ads.streams.LinkedInAds.check_access",
            return_value=False,
        ):
            with self.assertRaises(LinkedInForbiddenError):
                _apply_access_checks(client, schemas, field_metadata)

    def test_partial_inaccessibility_logs_warning(self):
        """Warning is logged when some (but not all) parent streams are inaccessible."""
        client = self._make_client()
        schemas, field_metadata = self._build_schemas_and_metadata()

        def _check_access(self, client):
            if self.tap_stream_id == "account_users":
                return False
            return True

        with mock.patch(
            "tap_linkedin_ads.streams.LinkedInAds.check_access",
            new=_check_access,
        ):
            with self.assertLogs("root", level="WARNING") as log:
                _apply_access_checks(client, schemas, field_metadata)

        self.assertTrue(
            any("account_users" in msg for msg in log.output),
            "Expected warning mentioning 'account_users'",
        )


class TestPruneInaccessibleChildren(unittest.TestCase):
    """Test _prune_inaccessible_children function."""

    def _build_schemas_and_metadata(self):
        return get_schemas()

    def test_child_removed_when_parent_absent(self):
        """video_ads is removed when accounts is not in schemas."""
        schemas, field_metadata = self._build_schemas_and_metadata()
        schemas.pop("accounts")
        field_metadata.pop("accounts")

        _prune_inaccessible_children(schemas, field_metadata)

        self.assertNotIn("video_ads", schemas)
        self.assertNotIn("video_ads", field_metadata)

    def test_child_kept_when_parent_present(self):
        """video_ads is kept when accounts is present in schemas."""
        schemas, field_metadata = self._build_schemas_and_metadata()

        _prune_inaccessible_children(schemas, field_metadata)

        self.assertIn("video_ads", schemas)

    def test_campaigns_children_removed_when_campaigns_absent(self):
        """creatives, ad_analytics_by_campaign, ad_analytics_by_creative removed when campaigns absent."""
        schemas, field_metadata = self._build_schemas_and_metadata()
        schemas.pop("campaigns")
        field_metadata.pop("campaigns")

        _prune_inaccessible_children(schemas, field_metadata)

        self.assertNotIn("creatives", schemas)
        self.assertNotIn("ad_analytics_by_campaign", schemas)
        self.assertNotIn("ad_analytics_by_creative", schemas)

    def test_no_removal_when_all_parents_present(self):
        """No streams removed when all parent streams are present."""
        schemas, field_metadata = self._build_schemas_and_metadata()
        original_count = len(schemas)

        _prune_inaccessible_children(schemas, field_metadata)

        self.assertEqual(len(schemas), original_count)


class TestCheckAccess(unittest.TestCase):
    """Test LinkedInAds.check_access() method."""

    def _make_stream(self, stream_name, accounts="123456"):
        from tap_linkedin_ads.streams import STREAMS
        self.client = mock.MagicMock()
        self.client.config = {"accounts": accounts}
        return STREAMS[stream_name]()

    def test_child_stream_always_accessible(self):
        """Child streams (with parent) make a real API probe; return True on success."""
        stream = self._make_stream("video_ads")
        self.client.get.return_value = {"elements": []}
        result = stream.check_access(self.client)
        self.assertTrue(result)
        self.client.get.assert_called_once()

    def test_child_creatives_always_accessible(self):
        """creatives (child of campaigns) makes a real API probe; returns True on success."""
        stream = self._make_stream("creatives")
        self.client.get.return_value = {"elements": []}
        result = stream.check_access(self.client)
        self.assertTrue(result)
        self.client.get.assert_called_once()

    def test_accounts_accessible_returns_true(self):
        """check_access returns True when accounts endpoint responds successfully."""
        stream = self._make_stream("accounts")
        self.client.get.return_value = {"elements": []}
        result = stream.check_access(self.client)
        self.assertTrue(result)
        self.client.get.assert_called_once()

    def test_accounts_forbidden_returns_false(self):
        """check_access returns False when accounts endpoint returns 403."""
        stream = self._make_stream("accounts")
        self.client.get.side_effect = LinkedInForbiddenError("403")
        result = stream.check_access(self.client)
        self.assertFalse(result)

    def test_account_users_accessible_returns_true(self):
        """check_access returns True when account_users endpoint responds successfully."""
        stream = self._make_stream("account_users")
        self.client.get.return_value = {"elements": []}
        result = stream.check_access(self.client)
        self.assertTrue(result)

    def test_account_users_forbidden_returns_false(self):
        """check_access returns False when account_users returns 403."""
        stream = self._make_stream("account_users")
        self.client.get.side_effect = LinkedInForbiddenError("403")
        result = stream.check_access(self.client)
        self.assertFalse(result)

    def test_campaign_groups_accessible_returns_true(self):
        """check_access returns True when campaign_groups endpoint responds successfully."""
        stream = self._make_stream("campaign_groups")
        self.client.get.return_value = {"elements": []}
        result = stream.check_access(self.client)
        self.assertTrue(result)

    def test_campaigns_accessible_returns_true(self):
        """check_access returns True when campaigns endpoint responds successfully."""
        stream = self._make_stream("campaigns")
        self.client.get.return_value = {"elements": []}
        result = stream.check_access(self.client)
        self.assertTrue(result)

    def test_check_access_uses_correct_url_for_accounts(self):
        """check_access passes an adAccounts URL for the accounts stream."""
        stream = self._make_stream("accounts")
        self.client.get.return_value = {}
        stream.check_access(self.client)
        call_kwargs = self.client.get.call_args
        url = call_kwargs[1].get("url") or call_kwargs[0][0]
        self.assertIn("adAccounts", url)

    def test_check_access_uses_account_id_in_url_for_campaign_groups(self):
        """check_access includes account ID in URL for campaign_groups."""
        stream = self._make_stream("campaign_groups", accounts="987654")
        self.client.get.return_value = {}
        stream.check_access(self.client)
        call_kwargs = self.client.get.call_args
        url = call_kwargs[1].get("url") or call_kwargs[0][0]
        self.assertIn("987654", url)
        self.assertIn("adCampaignGroups", url)

