# License: MIT
# Copyright © 2024 Frequenz Energy-as-a-Service GmbH

"""Integration tests for gRPC BaseApiClient and utilities."""

import asyncio
import logging
from typing import AsyncIterator
import pytest
from unittest import mock

# Add src to path for imports
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'src'))

from frequenz.client.base.client import call_stub_method
from frequenz.client.base.streaming import GrpcStreamBroadcaster
from frequenz.channels import Receiver

from . import demo_pb2
from .test_client import GreeterClient
from .test_server import TestGrpcServer

# Configure logging for tests
logging.basicConfig(level=logging.INFO)


@pytest.fixture
async def test_server() -> AsyncIterator[TestGrpcServer]:
    """Fixture providing a test gRPC server."""
    server = TestGrpcServer()
    port = await server.start()
    yield server
    await server.stop()


@pytest.fixture
async def test_client(test_server: TestGrpcServer) -> AsyncIterator[GreeterClient]:
    """Fixture providing a test gRPC client."""
    server_url = f"grpc://localhost:{test_server.actual_port}?ssl=false"
    client = GreeterClient(server_url, connect=True)
    
    try:
        yield client
    finally:
        await client.disconnect()


@pytest.fixture
async def auth_client(test_server: TestGrpcServer) -> AsyncIterator[GreeterClient]:
    """Fixture providing a test gRPC client with authentication."""
    server_url = f"grpc://localhost:{test_server.actual_port}?ssl=false"
    client = GreeterClient(
        server_url, 
        connect=True, 
        auth_key="test-api-key"
    )
    
    try:
        yield client
    finally:
        await client.disconnect()


@pytest.fixture
async def signing_client(test_server: TestGrpcServer) -> AsyncIterator[GreeterClient]:
    """Fixture providing a test gRPC client with signing."""
    server_url = f"grpc://localhost:{test_server.actual_port}?ssl=false"
    client = GreeterClient(
        server_url, 
        connect=True, 
        auth_key="test-api-key",
        sign_secret="test-secret"
    )
    
    try:
        yield client
    finally:
        await client.disconnect()


class TestUnaryRpc:
    """Tests for unary RPC calls."""

    async def test_unary_call_basic(self, test_client: GreeterClient) -> None:
        """Test basic unary RPC call."""
        request = demo_pb2.HelloRequest(name="World", count=1)
        
        # Test using call_stub_method utility
        response = await call_stub_method(
            test_client.stub.SayHello,
            request,
            timeout=5.0,
        )
        
        assert response.message == "Hello World!"
        assert response.sequence == 0

    async def test_unary_call_with_auth(self, auth_client: GreeterClient) -> None:
        """Test unary RPC call with authentication."""
        request = demo_pb2.HelloRequest(name="AuthUser", count=1)
        
        response = await call_stub_method(
            auth_client.stub.SayHello,
            request,
            timeout=5.0,
        )
        
        assert "Hello AuthUser!" in response.message
        assert "authenticated with key: test-api-key" in response.message

    async def test_unary_call_with_signing(self, signing_client: GreeterClient) -> None:
        """Test unary RPC call with signing."""
        request = demo_pb2.HelloRequest(name="SignedUser", count=1)
        
        response = await call_stub_method(
            signing_client.stub.SayHello,
            request,
            timeout=5.0,
        )
        
        assert "Hello SignedUser!" in response.message
        assert "authenticated" in response.message

    async def test_unary_call_timeout(self, test_client: GreeterClient) -> None:
        """Test unary RPC call with timeout handling."""
        request = demo_pb2.HelloRequest(name="TimeoutTest", count=1)
        
        # This should complete within timeout
        response = await call_stub_method(
            test_client.stub.SayHello,
            request,
            timeout=1.0,
        )
        
        assert response.message == "Hello TimeoutTest!"


class TestStreamingRpc:
    """Tests for streaming RPC calls."""

    async def test_streaming_call_basic(self, test_client: GreeterClient) -> None:
        """Test basic streaming RPC call."""
        request = demo_pb2.HelloRequest(name="StreamWorld", count=3)
        
        messages = []
        async with asyncio.timeout(10):
            stream = test_client.stub.StreamHellos(request)
            async for response in stream:
                messages.append(response)
        
        assert len(messages) == 3
        for i, msg in enumerate(messages):
            assert f"Hello StreamWorld #{i+1}" in msg.message
            assert msg.sequence == i

    async def test_streaming_call_with_auth(self, auth_client: GreeterClient) -> None:
        """Test streaming RPC call with authentication."""
        request = demo_pb2.HelloRequest(name="StreamAuth", count=2)
        
        messages = []
        async with asyncio.timeout(10):
            stream = auth_client.stub.StreamHellos(request)
            async for response in stream:
                messages.append(response)
        
        assert len(messages) == 2
        for msg in messages:
            assert "StreamAuth" in msg.message
            assert "authenticated" in msg.message


class TestGrpcStreamBroadcaster:
    """Tests for GrpcStreamBroadcaster."""

    async def test_stream_broadcaster_single_consumer(self, test_client: GreeterClient) -> None:
        """Test GrpcStreamBroadcaster with single consumer."""
        request = demo_pb2.HelloRequest(name="BroadcastTest", count=3)
        
        def create_stream():
            return test_client.stub.StreamHellos(request)
        
        broadcaster = GrpcStreamBroadcaster(
            stream_name="test_broadcast",
            stream_method=create_stream,
            transform=lambda msg: f"Transformed: {msg.message}",
            retry_on_exhausted_stream=False,
        )
        
        try:
            receiver = broadcaster.new_receiver()
            
            messages = []
            async with asyncio.timeout(10):
                async for msg in receiver:
                    messages.append(msg)
                    if len(messages) >= 3:
                        break
            
            assert len(messages) == 3
            for i, msg in enumerate(messages):
                assert msg.startswith("Transformed: Hello BroadcastTest")
                
        finally:
            await broadcaster.stop()

    async def test_stream_broadcaster_multiple_consumers(self, test_client: GreeterClient) -> None:
        """Test GrpcStreamBroadcaster with multiple consumers."""
        request = demo_pb2.HelloRequest(name="MultiConsumer", count=5)
        
        def create_stream():
            return test_client.stub.StreamHellos(request)
        
        broadcaster = GrpcStreamBroadcaster(
            stream_name="multi_consumer_test",
            stream_method=create_stream,
            transform=lambda msg: msg.message,
            retry_on_exhausted_stream=False,
        )
        
        try:
            # Create multiple receivers
            receiver1 = broadcaster.new_receiver()
            receiver2 = broadcaster.new_receiver()
            
            messages1 = []
            messages2 = []
            
            async def consume_receiver1():
                async for msg in receiver1:
                    messages1.append(msg)
                    if len(messages1) >= 3:
                        break
            
            async def consume_receiver2():
                async for msg in receiver2:
                    messages2.append(msg)
                    if len(messages2) >= 3:
                        break
            
            async with asyncio.timeout(10):
                await asyncio.gather(
                    consume_receiver1(),
                    consume_receiver2(),
                )
            
            # Both receivers should get the same messages
            assert len(messages1) == 3
            assert len(messages2) == 3
            
            for i in range(3):
                assert "MultiConsumer" in messages1[i]
                assert "MultiConsumer" in messages2[i]
                assert messages1[i] == messages2[i]  # Same content
                
        finally:
            await broadcaster.stop()

    async def test_stream_broadcaster_with_events(self, test_client: GreeterClient) -> None:
        """Test GrpcStreamBroadcaster with event receiver."""
        request = demo_pb2.HelloRequest(name="EventTest", count=2)
        
        def create_stream():
            return test_client.stub.StreamHellos(request)
        
        broadcaster = GrpcStreamBroadcaster(
            stream_name="event_test",
            stream_method=create_stream,
            transform=lambda msg: msg.message,
            retry_on_exhausted_stream=False,
        )
        
        try:
            # Receiver that includes events
            receiver = broadcaster.new_receiver(include_events=True)
            
            messages = []
            events = []
            
            async with asyncio.timeout(10):
                async for item in receiver:
                    if isinstance(item, str):  # Data message
                        messages.append(item)
                        if len(messages) >= 2:
                            break
                    else:  # Stream event
                        events.append(item)
            
            assert len(messages) == 2
            assert len(events) >= 1  # Should have at least StreamStarted event
            
            for msg in messages:
                assert "EventTest" in msg
                
        finally:
            await broadcaster.stop()


@pytest.mark.parametrize("timeout_seconds", [1.0, 5.0])
async def test_timeout_handling(test_client: GreeterClient, timeout_seconds: float) -> None:
    """Test that all operations respect timeouts."""
    request = demo_pb2.HelloRequest(name="TimeoutHandling", count=1)
    
    # This should complete well within the timeout
    response = await call_stub_method(
        test_client.stub.SayHello,
        request,
        timeout=timeout_seconds,
    )
    
    assert response.message == "Hello TimeoutHandling!"


if __name__ == "__main__":
    # Allow running tests directly
    pytest.main([__file__, "-v"])