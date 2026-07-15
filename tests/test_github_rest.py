from unittest.mock import MagicMock

import pytest

from scripts.common.github_rest import GitHubRestClient


def _client(response):
    requester = MagicMock()
    requester.requestJsonAndCheck.return_value = response
    return GitHubRestClient(MagicMock(_requester=requester)), requester


def test_artifact_page_uses_versioned_endpoint_contract():
    client, requester = _client((
        {"content-type": "application/json"},
        {
            "total_count": 1,
            "artifacts": [{
                "id": 42,
                "name": "test-results",
                "size_in_bytes": 123,
                "expired": False,
                "archive_download_url": "https://api.github.com/example",
            }],
        },
    ))

    page = client.list_run_artifacts_page("valkey-io/valkey", 77, page=2)

    assert page.total_count == 1
    assert page.artifacts[0].artifact_id == 42
    assert page.artifacts[0].name == "test-results"
    requester.requestJsonAndCheck.assert_called_once_with(
        "GET",
        "/repos/valkey-io/valkey/actions/runs/77/artifacts?per_page=100&page=2",
    )


@pytest.mark.parametrize(
    ("payload", "message"),
    [
        ([], "not an object"),
        ({"artifacts": []}, "total_count"),
        ({"total_count": True, "artifacts": []}, "total_count"),
        ({"total_count": 1, "artifacts": "bad"}, "artifacts list"),
        (
            {"total_count": 1, "artifacts": [{
                "id": True,
                "name": "result",
                "size_in_bytes": 1,
                "expired": False,
            }]},
            "invalid id",
        ),
        (
            {"total_count": 1, "artifacts": [{
                "id": 1,
                "name": "",
                "size_in_bytes": 1,
                "expired": False,
            }]},
            "invalid name",
        ),
        (
            {"total_count": 1, "artifacts": [{
                "id": 1,
                "name": "result",
                "size_in_bytes": -1,
                "expired": False,
            }]},
            "size_in_bytes",
        ),
        (
            {"total_count": 1, "artifacts": [{
                "id": 1,
                "name": "result",
                "size_in_bytes": 1,
                "expired": 0,
            }]},
            "expired flag",
        ),
    ],
)
def test_artifact_page_rejects_schema_violations(payload, message):
    client, _ = _client(({}, payload))
    with pytest.raises(RuntimeError, match=message):
        client.list_run_artifacts_page("org/repo", 1, page=1)


def test_artifact_page_rejects_invalid_requester_response():
    client, _ = _client({"total_count": 0, "artifacts": []})
    with pytest.raises(RuntimeError, match="invalid response"):
        client.list_run_artifacts_page("org/repo", 1, page=1)


@pytest.mark.parametrize(
    ("repository", "run_id", "page"),
    [
        ("org/repo/extra", 1, 1),
        ("org/repo", 0, 1),
        ("org/repo", 1, 0),
    ],
)
def test_artifact_page_rejects_invalid_endpoint_inputs(repository, run_id, page):
    client, requester = _client(({}, {"total_count": 0, "artifacts": []}))
    with pytest.raises(ValueError):
        client.list_run_artifacts_page(repository, run_id, page=page)
    requester.requestJsonAndCheck.assert_not_called()
