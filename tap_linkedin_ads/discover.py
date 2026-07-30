import singer
from singer.catalog import Catalog, CatalogEntry, Schema
from tap_linkedin_ads.schema import get_schemas, STREAMS
from tap_linkedin_ads.client import LinkedInForbiddenError

LOGGER = singer.get_logger()


def _prune_inaccessible_children(schemas: dict, field_metadata: dict) -> None:
    """
    Remove child streams from the catalog whose parent stream was excluded.
    Mutates schemas and field_metadata in place.
    """
    for name, stream_cls in list(STREAMS.items()):
        if name in schemas and stream_cls.parent and stream_cls.parent not in schemas:
            LOGGER.warning(
                "Stream '%s' excluded from catalog because its parent stream '%s' is not accessible.",
                name, stream_cls.parent,
            )
            schemas.pop(name, None)
            field_metadata.pop(name, None)


def _apply_access_checks(client, schemas: dict, field_metadata: dict) -> None:
    """
    Probe each stream for read access and remove inaccessible streams
    (and their children) from schemas and field_metadata in place.

    Two-pass strategy:
      Pass 1 — parent streams: check access, fetch first real record ID.
      Pass 2 — child streams: probe using the real parent ID.
        - Parent excluded → child pruned before pass 2.
        - Parent has no records → child included with a warning, no probe.

    Raises LinkedInForbiddenError if no streams remain accessible.
    """
    inaccessible_streams = []

    # Pass 1: check parent streams and collect first real IDs
    parent_first_ids = {}  # stream_name -> first record ID (str) or None
    for stream_name, stream_cls in list(STREAMS.items()):
        if stream_name not in schemas:
            continue
        if stream_cls.parent:
            continue  # handled in pass 2
        stream_obj = stream_cls()
        if stream_obj.check_access(client):
            parent_first_ids[stream_name] = stream_obj.get_first_id(client)
            if parent_first_ids[stream_name] is None:
                LOGGER.info(
                    "Stream '%s' is accessible but returned no records; child streams "
                    "will be included without an API access probe.",
                    stream_name,
                )
        else:
            inaccessible_streams.append(stream_name)
            schemas.pop(stream_name, None)
            field_metadata.pop(stream_name, None)

    # Prune children whose parent was just excluded before the child pass.
    _prune_inaccessible_children(schemas, field_metadata)

    # Pass 2: check child streams using real parent IDs
    for stream_name, stream_cls in list(STREAMS.items()):
        if stream_name not in schemas:
            continue
        if not stream_cls.parent:
            continue  # already handled
        real_parent_id = parent_first_ids.get(stream_cls.parent)
        stream_obj = stream_cls()
        if not stream_obj.check_access(client, parent_id=real_parent_id):
            inaccessible_streams.append(stream_name)
            schemas.pop(stream_name, None)
            field_metadata.pop(stream_name, None)

    if not schemas:
        raise LinkedInForbiddenError(
            "HTTP-error-code: 403, Error: The credentials do not have "
            "'read' access to any supported streams."
        )
    if inaccessible_streams:
        LOGGER.warning(
            "No 'read' access to stream(s): %s. Excluded from catalog.",
            ", ".join(inaccessible_streams),
        )


def discover(client) -> Catalog:
    """
    Run the discovery mode, prepare the catalog file and return the catalog.
    Access to each stream is verified using the provided client and streams
    the credentials cannot read are excluded from the returned catalog.
    """
    schemas, field_metadata = get_schemas()
    _apply_access_checks(client, schemas, field_metadata)
    catalog = Catalog([])

    for stream_name, schema_dict in schemas.items():
        schema = Schema.from_dict(schema_dict)
        mdata = field_metadata[stream_name]

        catalog.streams.append(CatalogEntry(
            stream=stream_name,
            tap_stream_id=stream_name,
            key_properties=STREAMS[stream_name].key_properties,
            schema=schema,
            metadata=mdata
        ))

    return catalog
