import grpc

from dfs.proto import raft_pb2, raft_pb2_grpc
from dfs.transport import channel_to, serve


class _EchoRaft(raft_pb2_grpc.RaftServicer):
    def RequestVote(
        self,
        request: raft_pb2.RequestVoteRequest,
        context: grpc.ServicerContext,
    ) -> raft_pb2.RequestVoteResponse:
        return raft_pb2.RequestVoteResponse(term=request.term, vote_granted=False)

    def AppendEntries(
        self,
        request: raft_pb2.AppendEntriesRequest,
        context: grpc.ServicerContext,
    ) -> raft_pb2.AppendEntriesResponse:
        return raft_pb2.AppendEntriesResponse(term=request.term, success=False, conflict_index=0)


def test_requestvote_roundtrips_over_grpc() -> None:
    server, address = serve(_EchoRaft())
    try:
        with channel_to(address) as channel:
            stub = raft_pb2_grpc.RaftStub(channel)
            reply = stub.RequestVote(raft_pb2.RequestVoteRequest(term=7))
        assert reply.term == 7
        assert reply.vote_granted is False
    finally:
        server.stop(None)
