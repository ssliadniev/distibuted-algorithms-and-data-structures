import pytest
from pydantic import ValidationError

from raft_node.config import Settings


def valid_settings(**overrides: object) -> Settings:
    values: dict[str, object] = {
        "node_id": "node1",
        "cluster_members": {
            "node1": "http://node1:8000/",
            "node2": "http://node2:8000",
            "node3": "http://node3:8000",
        }
    }
    values.update(overrides)

    return Settings(**values)


def test_settings_expose_members_peers_and_seconds() -> None:
    settings = valid_settings()

    assert settings.member_ids == ("node1", "node2", "node3")
    assert settings.peer_urls == {
        "node2": "http://node2:8000",
        "node3": "http://node3:8000"
    }
    assert settings.heartbeat_interval == 0.1
    assert settings.election_timeout_range == (0.45, 0.9)
    assert settings.http_request_timeout == 0.25
    assert settings.client_commit_timeout == 3.0


@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        ({"node_id": ""}, "node_id cannot be empty"),
        ({"node_id": "missing"}, "node_id must exist"),
        ({"cluster_members": {"node1": "not-a-url"}}, "invalid HTTP URL"),
        ({"heartbeat_interval_ms": 450}, "heartbeat interval"),
        ({"election_timeout_min_ms": 900}, "minimum election timeout")
    ]
)
def test_settings_reject_invalid_configuration(
    overrides: dict[str, object],
    message: str
) -> None:
    with pytest.raises(ValidationError, match=message):
        valid_settings(**overrides)
