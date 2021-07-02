from singer.catalog import Catalog, CatalogEntry, Schema
from tap_linkedin_ads.schema import get_schemas, STREAMS

def discover():
    schemas, field_metadata = get_schemas()
    catalog = Catalog([])

    for stream_name, schema_dict in schemas.items():
        schema = Schema.from_dict(schema_dict)
        mdata = field_metadata[stream_name]

        catalog.streams.append(CatalogEntry(
            stream=stream_name,
            tap_stream_id=stream_name,
            key_properties=STREAMS[stream_name]['key_properties'],
            schema=schema,
            metadata=mdata
        ))

    return catalog

def check_accounts_list(config):
    if config.get('accounts'):
        account_list = config['accounts'].replace(" ", "").split(",")
        for account in account_list:
            try:
                int(account)
            except ValueError as e:
                message = "The account '{}' provided in the configuration is having non-numeric value.".format(account)
                raise ValueError(message) from None
