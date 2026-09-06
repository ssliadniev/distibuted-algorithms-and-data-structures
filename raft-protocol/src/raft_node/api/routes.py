from typing import cast

from fastapi import APIRouter, Depends, HTTPException, Request, status

from raft_node.api.schemas import (CommandRequestBody, CommandResponseBody,
                                   HealthBody, LogRequestBody, LogResponseBody,
                                   NodeStatusBody, StatusLogEntry,
                                   VoteRequestBody, VoteResponseBody)
from raft_node.core import NotLeaderError
from raft_node.runtime import CommitTimeoutError, NodeStoppedError, RaftNode

api_router = APIRouter()


def get_raft_node(request: Request) -> RaftNode:
    """
    Inject the Raft node instance from the application state.

    Args:
        request: The incoming FastAPI request.

    Returns:
        The running RaftNode instance attached to the app state.
    """

    return cast(RaftNode, request.app.state.raft_node)


@api_router.get("/health", response_model=HealthBody)
async def health(node: RaftNode = Depends(get_raft_node)) -> HealthBody:
    """
    Check the liveliness of the HTTP server.
    """

    return HealthBody(node_id=node.core.state.node_id, status="ok")


@api_router.get("/", response_model=NodeStatusBody)
async def get_status(node: RaftNode = Depends(get_raft_node)) -> NodeStatusBody:
    """
    Retrieve the current in-memory Raft log and consensus state.
    """

    snapshot = await node.snapshot()

    return NodeStatusBody(
        node_id=snapshot.node_id,
        members=list(snapshot.members),
        role=snapshot.role.value,
        current_term=snapshot.current_term,
        voted_for=snapshot.voted_for,
        leader_id=snapshot.leader_id,
        commit_length=snapshot.commit_length,
        log=[
            StatusLogEntry(
                index=index,
                term=entry.term,
                command=entry.command,
                committed=index < snapshot.commit_length,
            )
            for index, entry in enumerate(snapshot.log)
        ],
    )


@api_router.post("/", response_model=CommandResponseBody, status_code=status.HTTP_201_CREATED)
async def submit_command(body: CommandRequestBody, node: RaftNode = Depends(get_raft_node)) -> CommandResponseBody:
    """
    Submit a client command to the Raft cluster.

    Raises:
        HTTPException (409): If the node is not the current leader.
        HTTPException (504): If the command fails to commit within the deadline.
        HTTPException (503): If the node runtime has stopped.
    """

    try:
        result = await node.submit_command(body.command)
    except NotLeaderError as error:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={"error": "not_a_leader", "leader_id": error.leader_id},
        ) from error
    except CommitTimeoutError as error:
        raise HTTPException(
            status_code=status.HTTP_504_GATEWAY_TIMEOUT,
            detail={"error": "commit_timeout", "index": error.index, "term": error.term},
        ) from error
    except NodeStoppedError as error:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={"error": "node_stopped"},
        ) from error

    return CommandResponseBody(
        index=result.index,
        term=result.entry.term,
        command=result.entry.command,
        committed=True,
    )


@api_router.post("/raft/vote", response_model=VoteResponseBody)
async def request_vote(body: VoteRequestBody, node: RaftNode = Depends(get_raft_node)) -> VoteResponseBody:
    """
    Handle an incoming RequestVote RPC from a candidate.
    """

    response = await node.receive_vote_request(body.to_domain())
    return VoteResponseBody.from_domain(response)


@api_router.post("/raft/log", response_model=LogResponseBody)
async def append_entries(body: LogRequestBody,node: RaftNode = Depends(get_raft_node)) -> LogResponseBody:
    """
    Handle an incoming AppendEntries RPC from the leader.
    """

    response = await node.receive_log_request(body.to_domain())
    return LogResponseBody.from_domain(response)
