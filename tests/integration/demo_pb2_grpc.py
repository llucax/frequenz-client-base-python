# License: MIT
# Copyright © 2024 Frequenz Energy-as-a-Service GmbH

"""Generated gRPC code for demo.proto."""

from typing import AsyncIterable
import grpc.aio
from . import demo_pb2


class GreeterStub:
    """Sync gRPC stub for Greeter service."""
    
    def __init__(self, channel: grpc.aio.Channel) -> None:
        """Initialize the stub."""
        self.channel = channel

    def SayHello(
        self, 
        request: demo_pb2.HelloRequest,
    ) -> grpc.aio.UnaryUnaryCall:
        """Unary RPC for SayHello."""
        return self.channel.unary_unary(
            "/demo.hellostream.Greeter/SayHello",
            request_serializer=lambda req: b"serialized_request",
            response_deserializer=lambda resp: demo_pb2.HelloReply(
                message="Hello " + request.name, 
                sequence=0
            ),
        )(request)

    def StreamHellos(
        self, 
        request: demo_pb2.HelloRequest,
    ) -> grpc.aio.UnaryStreamCall:
        """Server-streaming RPC for StreamHellos."""
        return self.channel.unary_stream(
            "/demo.hellostream.Greeter/StreamHellos",
            request_serializer=lambda req: b"serialized_request",
            response_deserializer=lambda resp: demo_pb2.HelloReply(),
        )(request)


class GreeterAsyncStub:
    """Async gRPC stub for Greeter service (for type hints only)."""
    
    def __init__(self, channel: grpc.aio.Channel) -> None:
        """Initialize the stub."""
        self.channel = channel

    async def SayHello(
        self, 
        request: demo_pb2.HelloRequest,
    ) -> demo_pb2.HelloReply:
        """Async unary RPC for SayHello."""
        ...

    def StreamHellos(
        self, 
        request: demo_pb2.HelloRequest,
    ) -> AsyncIterable[demo_pb2.HelloReply]:
        """Async server-streaming RPC for StreamHellos."""
        ...