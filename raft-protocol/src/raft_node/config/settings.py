from functools import cached_property
from typing import Self
from urllib.parse import urlsplit

from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="RAFT_", env_file=".env", case_sensitive=False, extra="ignore", frozen=True
    )

    node_id: str = Field(description="The unique identifier for this Raft node.")
    bind_host: str = Field(
        default="0.0.0.0", description="The hostname or IP address to bind the HTTP server to."
    )
    bind_port: int = Field(
        default=8000, ge=1, le=65535, description="The port to bind the HTTP server to."
    )
    cluster_members: dict[str, str] = Field(
        description="Mapping of all cluster node IDs to their HTTP peer URLs."
    )
    log_level: str = Field(default="INFO", description="Application logging level.")
    heartbeat_interval_ms: int = Field(
        default=100, gt=0, description="Interval between leader heartbeats in milliseconds."
    )
    election_timeout_min_ms: int = Field(
        default=450,
        gt=0,
        description="Minimum bound for the randomized election timeout in milliseconds."
    )
    election_timeout_max_ms: int = Field(
        default=900,
        gt=0,
        description="Maximum bound for the randomized election timeout in milliseconds."
    )
    http_request_timeout_ms: int = Field(
        default=250, gt=0, description="Timeout for peer-to-peer HTTP RPCs in milliseconds."
    )
    client_commit_timeout_ms: int = Field(
        default=3000,
        gt=0,
        description="Maximum time to wait for a client command to commit in milliseconds."
    )

    @model_validator(mode="after")
    def validate_cluster_and_timing(self) -> Self:
        if not self.node_id:
            raise ValueError("node_id cannot be empty")
        if self.node_id not in self.cluster_members:
            raise ValueError("node_id must exist in cluster_members")
        if any(not member_id for member_id in self.cluster_members):
            raise ValueError("cluster member IDs cannot be empty")

        for member_id, raw_url in self.cluster_members.items():
            parsed = urlsplit(raw_url)
            if parsed.scheme not in {"http", "https"} or not parsed.netloc:
                raise ValueError(f"cluster member {member_id!r} has an invalid HTTP URL")

        if self.heartbeat_interval_ms >= self.election_timeout_min_ms:
            raise ValueError("heartbeat interval must be shorter than the minimum election timeout")
        if self.election_timeout_min_ms >= self.election_timeout_max_ms:
            raise ValueError("minimum election timeout must be shorter than the maximum")

        return self

    @cached_property
    def member_ids(self) -> tuple[str, ...]:
        return tuple(self.cluster_members)

    @cached_property
    def peer_urls(self) -> dict[str, str]:
        """
        Return a mapping of peer node IDs to their normalized HTTP URLs, excluding this node.
        """

        return {
            member_id: url.rstrip("/")
            for member_id, url in self.cluster_members.items()
            if member_id != self.node_id
        }

    @property
    def heartbeat_interval(self) -> float:
        return self.heartbeat_interval_ms / 1000.0

    @property
    def election_timeout_range(self) -> tuple[float, float]:
        return self.election_timeout_min_ms / 1000.0, self.election_timeout_max_ms / 1000.0

    @property
    def http_request_timeout(self) -> float:
        return self.http_request_timeout_ms / 1000.0

    @property
    def client_commit_timeout(self) -> float:
        return self.client_commit_timeout_ms / 1000.0
