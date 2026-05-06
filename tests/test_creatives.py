import json
from io import StringIO
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs
from unittest import mock
from unittest.mock import patch, MagicMock

import responses
from singer.catalog import Catalog

from tap_linkedin_ads import main


def _read_json_fixture(name: str) -> dict[str, Any]:
    path = Path(__file__).parent / "fixtures" / name
    return json.loads(path.read_text())


def _build_catalog(raw_catalog: dict[str, Any], stream_name: str) -> Catalog:
    for stream in raw_catalog["streams"]:
        if stream["stream"] != stream_name:
            continue

        metadata = [mt for mt in stream["metadata"] if mt["breadcrumb"] == []][0]
        metadata["metadata"]["selected"] = True

    return Catalog.from_dict(raw_catalog)


@responses.activate
@patch("singer.utils.parse_args")
def test_creatives_sync_from_scratch(patched_parse_args: MagicMock) -> None:
    config = _read_json_fixture("config.json")
    raw_catalog = _read_json_fixture("catalog.json")

    patched_parse_args.return_value = MagicMock(
        config=config,
        catalog=_build_catalog(raw_catalog, "creatives"),
        state={},
        discover=False,
    )

    responses.post(
        "https://api.linkedin.com/rest/adAccounts/12345/adCampaigns",
        json={
            "elements": [{"id": i} for i in range(1, 31)],
            "metadata": {},
        },
    )
    responses.post(
        "https://api.linkedin.com/rest/adAccounts/12345/creatives",
        json={
            "elements": [
                {
                    "id": "c-1",
                    "name": "Creative A",
                    "lastModifiedAt": "2026-01-10T00:00:00Z",
                },
                {
                    "id": "c-2",
                    "name": "Creative B",
                    "lastModifiedAt": "2026-01-11T00:00:00Z",
                },
            ],
            "metadata": {},
        },
    )
    responses.post(
        "https://api.linkedin.com/rest/adAccounts/12345/creatives",
        json={
            "elements": [
                {
                    "id": "c-3",
                    "name": "Creative C",
                    "lastModifiedAt": "2026-01-10T14:00:00Z",
                },
            ],
            "metadata": {},
        },
    )

    with mock.patch("sys.stdout", new=StringIO()) as fake_stdout:
        main()

    assert len(responses.calls) == 3

    parsed_qs_1 = parse_qs(responses.calls[1].request.body)
    assert parsed_qs_1["q"] == ["criteria"]
    assert parsed_qs_1["sortOrder"] == ["ASCENDING"]
    assert parsed_qs_1["campaigns"] == [
        f'List({",".join([f"urn:li:sponsoredCampaign:{i}" for i in range(1, 21)])})'
    ]

    parsed_qs_2 = parse_qs(responses.calls[2].request.body)
    assert parsed_qs_2["q"] == ["criteria"]
    assert parsed_qs_2["sortOrder"] == ["ASCENDING"]
    assert parsed_qs_2["campaigns"] == [
        f'List({",".join([f"urn:li:sponsoredCampaign:{i}" for i in range(21, 31)])})'
    ]

    tap_messages = [json.loads(line) for line in fake_stdout.getvalue().splitlines()]

    records = [m["record"] for m in tap_messages if m["type"] == "RECORD"]
    assert len(records) == 3
    assert [r["id"] for r in records] == ["c-1", "c-2", "c-3"]

    state_messages = [m for m in tap_messages if m["type"] == "STATE"]
    assert len(state_messages) == 3  # start and finish sync + actual state at the end
    assert state_messages[-1]["value"] == {
        "bookmarks": {"creatives": "2026-01-11T00:00:00.000000Z"}
    }


@responses.activate
@patch("singer.utils.parse_args")
def test_creatives_sync_with_state(patched_parse_args: MagicMock) -> None:
    config = _read_json_fixture("config.json")
    raw_catalog = _read_json_fixture("catalog.json")
    state = {"bookmarks": {"creatives": "2026-01-11T00:00:00.000000Z"}}

    patched_parse_args.return_value = MagicMock(
        config=config,
        catalog=_build_catalog(raw_catalog, "creatives"),
        state=state,
        discover=False,
    )

    responses.post(
        "https://api.linkedin.com/rest/adAccounts/12345/adCampaigns",
        json={
            "elements": [{"id": i} for i in range(1, 11)],
            "metadata": {},
        },
    )
    responses.post(
        "https://api.linkedin.com/rest/adAccounts/12345/creatives",
        json={
            "elements": [
                {
                    "id": "c-1",
                    "name": "Creative A",
                    "lastModifiedAt": "2026-01-10T00:00:00Z",
                },
                {
                    "id": "c-2",
                    "name": "Creative B",
                    "lastModifiedAt": "2026-03-10T00:00:00Z",
                },
            ],
            "metadata": {},
        },
    )

    with mock.patch("sys.stdout", new=StringIO()) as fake_stdout:
        main()

    assert len(responses.calls) == 2

    parsed_qs = parse_qs(responses.calls[1].request.body)
    assert parsed_qs["q"] == ["criteria"]
    assert parsed_qs["sortOrder"] == ["ASCENDING"]
    assert parsed_qs["campaigns"] == [
        f'List({",".join([f"urn:li:sponsoredCampaign:{i}" for i in range(1, 11)])})'
    ]

    tap_messages = [json.loads(line) for line in fake_stdout.getvalue().splitlines()]

    records = [m["record"] for m in tap_messages if m["type"] == "RECORD"]
    assert len(records) == 1  # one record is filtered
    assert [r["id"] for r in records] == ["c-2"]

    state_messages = [m for m in tap_messages if m["type"] == "STATE"]
    assert len(state_messages) == 3  # start and finish sync + actual state at the end
    assert state_messages[-1]["value"] == {
        "bookmarks": {"creatives": "2026-03-10T00:00:00.000000Z"}
    }

