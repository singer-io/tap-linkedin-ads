import unittest
from unittest import mock
from tap_linkedin_ads.discover import discover
from singer.catalog import Catalog


class TestDiscover(unittest.TestCase):
    """Test `discover` function."""
    def test_discover(self):
        
        return_catalog = discover()
        
        self.assertIsInstance(return_catalog, Catalog)
