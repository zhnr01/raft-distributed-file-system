from concurrent import futures

import grpc

from dfs.proto import raft_pb2_grpc

DEFAULT_ADDRESS = "127.0.0.1:0"
DEFAULT_MAX_WORKERS = 4


def serve(
    servicer: raft_pb2_grpc.RaftServicer,
    address: str = DEFAULT_ADDRESS,
    max_workers: int = DEFAULT_MAX_WORKERS,
) -> tuple[grpc.Server, str]:
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=max_workers))
    raft_pb2_grpc.add_RaftServicer_to_server(servicer, server)
    port = server.add_insecure_port(address)
    server.start()
    host = address.rsplit(":", 1)[0]
    return server, f"{host}:{port}"


def channel_to(address: str) -> grpc.Channel:
    return grpc.insecure_channel(address)
