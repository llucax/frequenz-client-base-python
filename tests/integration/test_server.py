# License: MIT
# Copyright © 2024 Frequenz Energy-as-a-Service GmbH

"""Test gRPC server implementation for integration tests."""

import asyncio
import logging
from typing import AsyncIterable

import grpc.aio

from . import demo_pb2


class GreeterServicer:
    """Implementation of the Greeter service for testing."""

    async def SayHello(
        self, 
        request: demo_pb2.HelloRequest, 
        context: grpc.aio.ServicerContext
    ) -> demo_pb2.HelloReply:
        """Handle unary SayHello RPC."""
        logging.info(f"SayHello called with name: {request.name}")
        
        # Simulate some authentication checking by reading metadata
        auth_key = None
        if context.invocation_metadata():
            for key, value in context.invocation_metadata():
                if key == "key":
                    auth_key = value
                    break
        
        message = f"Hello {request.name}!"
        if auth_key:
            message += f" (authenticated with key: {auth_key})"
            
        return demo_pb2.HelloReply(message=message, sequence=0)

    async def StreamHellos(
        self, 
        request: demo_pb2.HelloRequest, 
        context: grpc.aio.ServicerContext
    ) -> AsyncIterable[demo_pb2.HelloReply]:
        """Handle streaming StreamHellos RPC."""
        logging.info(f"StreamHellos called with name: {request.name}, count: {request.count}")
        
        # Get auth key from metadata
        auth_key = None
        if context.invocation_metadata():
            for key, value in context.invocation_metadata():
                if key == "key":
                    auth_key = value
                    break
        
        count = max(1, request.count)  # Ensure at least 1 message
        for i in range(count):
            if context.cancelled():
                break
                
            message = f"Hello {request.name} #{i+1}"
            if auth_key:
                message += f" (authenticated)"
                
            yield demo_pb2.HelloReply(message=message, sequence=i)
            
            # Small delay to simulate real streaming
            await asyncio.sleep(0.1)


class TestGrpcServer:
    """Test gRPC server for integration tests."""
    
    def __init__(self, port: int = 0) -> None:
        """Initialize the test server.
        
        Args:
            port: Port to bind to, 0 for any available port.
        """
        self.port = port
        self.server: grpc.aio.Server | None = None
        self.actual_port: int = 0
        
    async def start(self) -> int:
        """Start the test server.
        
        Returns:
            The actual port the server is listening on.
        """
        self.server = grpc.aio.server()
        
        # Add the servicer
        servicer = GreeterServicer()
        
        # Manually add methods to server (normally done by add_GreeterServicer_to_server)
        rpc_method_handlers = {
            'SayHello': grpc.aio.unary_unary_rpc_method_handler(
                servicer.SayHello,
                request_deserializer=lambda data: demo_pb2.HelloRequest(),
                response_serializer=lambda resp: b"serialized_response",
            ),
            'StreamHellos': grpc.aio.unary_stream_rpc_method_handler(
                servicer.StreamHellos,
                request_deserializer=lambda data: demo_pb2.HelloRequest(),
                response_serializer=lambda resp: b"serialized_response",
            ),
        }
        
        generic_handler = grpc.aio.method_handlers_generic_handler(
            'demo.hellostream.Greeter', rpc_method_handlers
        )
        self.server.add_generic_rpc_handlers((generic_handler,))
        
        # Bind to port
        listen_addr = f'localhost:{self.port}'
        self.actual_port = self.server.add_insecure_port(listen_addr)
        
        logging.info(f"Starting gRPC server on port {self.actual_port}")
        await self.server.start()
        
        return self.actual_port
    
    async def stop(self) -> None:
        """Stop the test server."""
        if self.server:
            logging.info("Stopping gRPC server")
            await self.server.stop(grace=1.0)
            self.server = None