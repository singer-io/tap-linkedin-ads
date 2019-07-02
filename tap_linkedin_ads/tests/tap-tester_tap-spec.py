import os

class TapSpec():
    """ Base class to specify tap-specific configuration. """

    REPLICATION_KEYS = "valid-replication-keys"
    PRIMARY_KEYS = "table-key-properties"
    FOREIGN_KEYS = "table-foreign-key-properties"
    REPLICATION_METHOD = "forced-replication-method"
    API_LIMIT = "max-row-limit"
    INCREMENTAL = "INCREMENTAL"
    FULL = "FULL_TABLE"
    START_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

    CONFIGURATION_ENVIRONMENT = {
        "properties": {
            "user_agent": "TAP_HELPSCOUT_USER_AGENT",
            "start_date": "TAP_HELPSCOUT_START_DATE"
        },
        "credentials": {
            "client_secret": "TAP_HELPSCOUT_CLIENT_SECRET",
            "refresh_token": "TAP_HELPSCOUT_REFRESH_TOKEN",
            "client_id": "TAP_HELPSCOUT_CLIENT_ID"
        }
    }

    DEFAULT_START_DATE = '2019-01-01 00:00:00'

    @staticmethod
    def tap_name():
        """The name of the tap"""
        return "helpscout"

    @staticmethod
    def get_type():
        """the expected url route ending"""
        return "platform.helpscout"

    def get_properties(self, original: bool = True):
        """Configuration properties required for the tap."""
        properties_env = self.CONFIGURATION_ENVIRONMENT['properties']
        return_value = {k: os.getenv(v) for k, v in properties_env.items()}
        return_value['start_date'] = self.DEFAULT_START_DATE

        if original:
            return return_value

        # This test needs the new connections start date to be larger than the default
        assert self.start_date > return_value["start_date"]

        return_value["start_date"] = self.start_date
        return return_value

    def get_credentials(self):
        """Authentication information for the test account"""
        credentials_env = self.CONFIGURATION_ENVIRONMENT['credentials']
        return {k: os.getenv(v) for k, v in credentials_env.items()}

    def expected_metadata(self):
        """The expected streams and metadata about the streams"""

        default = {
                self.REPLICATION_KEYS: {"updated_at"},
                self.PRIMARY_KEYS: {"id"},
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.API_LIMIT: 250}

        meta = default.copy()
        meta.update({self.FOREIGN_KEYS: {"owner_id", "owner_resource"}})

        return {
            "orders": default,
            "metafields": meta,
            "transactions": {
                self.REPLICATION_KEYS: {"created_at"},
                self.PRIMARY_KEYS: {"id"},
                self.FOREIGN_KEYS: {"order_id"},
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.API_LIMIT: 250}
        }