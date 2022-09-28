import unittest
from unittest import mock
from tap_linkedin_ads import main
from singer.catalog import Catalog


class MockArgs:
    """Mock args object class"""

    def __init__(self, config = None, config_path = None, catalog = None, state = {}, discover = False) -> None:
        self.config = config 
        self.catalog = catalog
        self.state = state
        self.config_path = None
        self.discover = discover

@mock.patch("singer.utils.parse_args")
@mock.patch("tap_linkedin_ads._discover")
@mock.patch("tap_linkedin_ads._sync")
class TestMainWorkflow(unittest.TestCase):
    """
    Test main function for discover mode.
    """

    mock_config = {"start_date": "", "access_token": "", "user_agent": ""}
    mock_catalog = Catalog.from_dict({"streams": [{"stream": "landings", "schema": {}, "metadata": {}}]})

    def test_discover_with_config(self, mock_sync, mock_discover, mock_args):
        """
        Test `_discover` function is called for discover mode.
        """
        mock_discover.return_value.to_dict.return_value = dict()
        mock_args.return_value = MockArgs(discover = True, config = self.mock_config)
        main()

        self.assertTrue(mock_discover.called)
        self.assertFalse(mock_sync.called)

    def test_sync_with_catalog(self, mock_sync, mock_discover, mock_args):
        """
        Test sync mode with catalog given in args.
        """

        mock_args.return_value = MockArgs(config=self.mock_config, catalog=self.mock_catalog)
        main()

        # Verify `_sync` is called with expected arguments
        mock_sync.assert_called_with(client=mock.ANY,
                                     config=self.mock_config,
                                     catalog=self.mock_catalog,
                                     state={})

        # verify `_discover` function is not called
        self.assertFalse(mock_discover.called)

    def test_sync_without_catalog(self, mock_sync, mock_discover, mock_args):
        """
        Test sync mode without catalog given in args.
        """

        mock_args.return_value = MockArgs(config=self.mock_config)
        main()

        # Verify `_sync` is not called
        self.assertFalse(mock_sync.called)

    def test_sync_with_state(self, mock_sync, mock_discover, mock_args):
        """
        Test sync mode with the state given in args.
        """
        mock_state = {"bookmarks": {"projects": ""}}
        mock_args.return_value = MockArgs(config=self.mock_config, catalog=self.mock_catalog, state=mock_state)
        main()

        # Verify `_sync` is called with expected arguments
        mock_sync.assert_called_with(client=mock.ANY,
                                     config=self.mock_config,
                                     state=mock_state,
                                     catalog=self.mock_catalog)
