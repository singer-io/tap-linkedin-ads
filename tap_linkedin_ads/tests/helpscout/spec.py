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
            "start_date": "TAP_HELPSCOUT_START_DATE",
            "user_agent": "TAP_HELPSCOUT_USER_AGENT"
        },
        "credentials": {
            "refresh_token": "TAP_HELPSCOUT_REFRESH_TOKEN",
            "client_id": "TAP_HELPSCOUT_CLIENT_ID",
            "client_secret": "TAP_HELPSCOUT_CLIENT_SECRET"
        }
    }

    DEFAULT_START_DATE = '2018-01-01 00:00:00'

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

        id_pk = {
            self.PRIMARY_KEYS: {"id"},
        }

        return {
            "conversations": id_pk,
            "conversation_threads": id_pk,
            "customers": id_pk,
            "mailboxes": id_pk,
            "mailbox_fields": id_pk,
            "mailbox_folders": id_pk,
            "users": id_pk,
            "workflows": id_pk
        }