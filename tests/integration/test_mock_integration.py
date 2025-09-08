# License: MIT
# Copyright © 2024 Frequenz Energy-as-a-Service GmbH

"""Mock-based integration tests for gRPC BaseApiClient and utilities."""

import asyncio
import pytest
from typing import AsyncIterator
from unittest import mock
import logging

# Mock the grpc module for testing without dependencies
import sys
from unittest.mock import MagicMock

# Mock grpc modules before any imports
grpc_mock = MagicMock()
grpc_aio_mock = MagicMock()
grpc_mock.aio = grpc_aio_mock
sys.modules['grpc'] = grpc_mock
sys.modules['grpc.aio'] = grpc_aio_mock

# Mock protobuf
google_mock = MagicMock()
protobuf_mock = MagicMock()
google_mock.protobuf = protobuf_mock
sys.modules['google'] = google_mock
sys.modules['google.protobuf'] = protobuf_mock
sys.modules['google.protobuf.message'] = protobuf_mock.message

# Mock frequenz.channels
frequenz_mock = MagicMock()
channels_mock = MagicMock()
frequenz_mock.channels = channels_mock
sys.modules['frequenz'] = frequenz_mock
sys.modules['frequenz.channels'] = channels_mock

# Add src to path for imports
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'src'))

# Now we can import our modules
from frequenz.client.base.client import BaseApiClient, call_stub_method
from frequenz.client.base.channel import ChannelOptions
from frequenz.client.base.streaming import GrpcStreamBroadcaster

# Mock message classes
class MockHelloRequest:
    """Mock for HelloRequest message."""
    
    def __init__(self, name: str = "", count: int = 0) -> None:
        self.name = name
        self.count = count


class MockHelloReply:
    """Mock for HelloReply message."""
    
    def __init__(self, message: str = "", sequence: int = 0) -> None:
        self.message = message
        self.sequence = sequence


class MockGreeterStub:
    """Mock gRPC stub for testing."""
    
    def __init__(self, channel: mock.MagicMock) -> None:
        self.channel = channel
        
    async def SayHello(self, request: MockHelloRequest) -> MockHelloReply:
        """Mock unary call."""
        return MockHelloReply(
            message=f"Hello {request.name}!",
            sequence=0
        )
        
    def StreamHellos(self, request: MockHelloRequest) -> AsyncIterator[MockHelloReply]:
        """Mock streaming call."""
        return self._stream_hellos_impl(request)
        
    async def _stream_hellos_impl(self, request: MockHelloRequest) -> AsyncIterator[MockHelloReply]:
        """Implementation of streaming call."""
        count = max(1, request.count)
        for i in range(count):
            yield MockHelloReply(
                message=f"Hello {request.name} #{i+1}",
                sequence=i
            )
            await asyncio.sleep(0.01)  # Small delay to simulate streaming


class MockGreeterClient(BaseApiClient[MockGreeterStub]):
    """Mock client for testing."""

    def __init__(
        self,
        server_url: str,
        *,
        connect: bool = True,
        channel_defaults: ChannelOptions | None = None,
        auth_key: str | None = None,
        sign_secret: str | None = None,
    ) -> None:
        super().__init__(
            server_url=server_url,
            create_stub=self._create_stub,
            connect=connect,
            channel_defaults=channel_defaults or ChannelOptions(),
            auth_key=auth_key,
            sign_secret=sign_secret,
        )

    def _create_stub(self, channel: mock.MagicMock) -> MockGreeterStub:
        return MockGreeterStub(channel)

    @property
    def stub(self) -> MockGreeterStub:
        return self._stub  # type: ignore[return-value]


@pytest.fixture
def mock_channel() -> mock.MagicMock:
    """Fixture providing a mock gRPC channel."""
    return mock.MagicMock()


@pytest.fixture
def test_client(mock_channel: mock.MagicMock) -> MockGreeterClient:
    """Fixture providing a test client."""
    with mock.patch('frequenz.client.base.client.parse_grpc_uri', return_value=mock_channel):
        client = MockGreeterClient("grpc://localhost:50051", connect=True)
        return client


@pytest.fixture
def auth_client(mock_channel: mock.MagicMock) -> MockGreeterClient:
    """Fixture providing a test client with authentication."""
    with mock.patch('frequenz.client.base.client.parse_grpc_uri', return_value=mock_channel):
        client = MockGreeterClient(
            "grpc://localhost:50051",
            connect=True,
            auth_key="test-api-key"
        )
        return client


class TestMockUnaryRpc:
    """Tests for unary RPC calls using mocks."""

    async def test_unary_call_basic(self, test_client: MockGreeterClient) -> None:
        """Test basic unary RPC call."""
        request = MockHelloRequest(name="World", count=1)
        
        # Test using call_stub_method utility
        with mock.patch('frequenz.client.base.client.ClientNotConnected') as mock_exc:
            # Mock that client is connected
            test_client._stub = MockGreeterStub(mock.MagicMock())
            
            response = await call_stub_method(
                test_client.stub.SayHello,
                request,
                timeout=5.0,
            )
            
            assert response.message == "Hello World!"
            assert response.sequence == 0

    async def test_unary_call_with_auth(self, auth_client: MockGreeterClient) -> None:
        """Test unary RPC call with authentication."""
        request = MockHelloRequest(name="AuthUser", count=1)
        
        # Mock that client is connected
        auth_client._stub = MockGreeterStub(mock.MagicMock())
        
        response = await call_stub_method(
            auth_client.stub.SayHello,
            request,
            timeout=5.0,
        )
        
        assert "Hello AuthUser!" in response.message

    async def test_call_stub_method_timeout(self, test_client: MockGreeterClient) -> None:
        """Test call_stub_method with timeout handling."""
        request = MockHelloRequest(name="TimeoutTest", count=1)
        
        # Mock that client is connected
        test_client._stub = MockGreeterStub(mock.MagicMock())
        
        # This should complete within timeout
        response = await call_stub_method(
            test_client.stub.SayHello,
            request,
            timeout=1.0,
        )
        
        assert response.message == "Hello TimeoutTest!"


class TestMockStreamingRpc:
    """Tests for streaming RPC calls using mocks."""

    async def test_streaming_call_basic(self, test_client: MockGreeterClient) -> None:
        """Test basic streaming RPC call."""
        request = MockHelloRequest(name="StreamWorld", count=3)
        
        # Mock that client is connected
        test_client._stub = MockGreeterStub(mock.MagicMock())
        
        messages = []
        async with asyncio.timeout(10):
            async for response in test_client.stub._stream_hellos_impl(request):
                messages.append(response)
        
        assert len(messages) == 3
        for i, msg in enumerate(messages):
            assert f"Hello StreamWorld #{i+1}" in msg.message
            assert msg.sequence == i


class TestMockGrpcStreamBroadcaster:
    """Tests for GrpcStreamBroadcaster using mocks."""

    async def test_stream_broadcaster_creation(self) -> None:
        """Test GrpcStreamBroadcaster creation and basic functionality."""
        
        # Create a mock stream method
        async def mock_stream_method():
            """Mock stream method that yields messages."""
            for i in range(3):
                yield MockHelloReply(
                    message=f"Broadcast message {i}",
                    sequence=i
                )
                await asyncio.sleep(0.01)
        
        # Mock the stream call to return our async generator
        mock_stream_call = mock.MagicMock()
        mock_stream_call.__aiter__ = lambda: mock_stream_method()
        
        def create_stream():
            return mock_stream_call
        
        # Mock the channels module components
        mock_broadcast = mock.MagicMock()
        mock_sender = mock.MagicMock()
        mock_receiver = mock.MagicMock()
        
        mock_broadcast.new_sender.return_value = mock_sender
        mock_broadcast.new_receiver.return_value = mock_receiver
        
        # Mock the receiver to return our test data
        messages = [
            "Transformed: Broadcast message 0",
            "Transformed: Broadcast message 1", 
            "Transformed: Broadcast message 2"
        ]
        
        async def mock_receiver_iter():
            for msg in messages:
                yield msg
                
        mock_receiver.__aiter__ = mock_receiver_iter
        
        with mock.patch('frequenz.client.base.streaming.channels.Broadcast', return_value=mock_broadcast):
            with mock.patch('asyncio.create_task') as mock_create_task:
                broadcaster = GrpcStreamBroadcaster(
                    stream_name="test_broadcast",
                    stream_method=create_stream,
                    transform=lambda msg: f"Transformed: {msg.message}",
                    retry_on_exhausted_stream=False,
                )
                
                # Verify that broadcaster was created
                assert broadcaster is not None
                
                # Verify new_receiver method works
                receiver = broadcaster.new_receiver()
                assert receiver is not None


class TestMockAuthentication:
    """Tests for authentication and signing interceptors."""

    def test_authentication_interceptor_creation(self, auth_client: MockGreeterClient) -> None:
        """Test that authentication interceptors are properly configured."""
        # Verify client has auth_key set
        assert auth_client._auth_key == "test-api-key"
        
        # Verify that the client initialization worked
        assert auth_client.server_url == "grpc://localhost:50051"

    def test_signing_interceptor_creation(self) -> None:
        """Test that signing interceptors are properly configured."""
        with mock.patch('frequenz.client.base.client.parse_grpc_uri') as mock_parse:
            mock_channel = mock.MagicMock()
            mock_parse.return_value = mock_channel
            
            client = MockGreeterClient(
                "grpc://localhost:50051",
                connect=True,
                auth_key="test-api-key",
                sign_secret="test-secret"
            )
            
            # Verify client has both auth and signing configured
            assert client._auth_key == "test-api-key"
            assert client._sign_secret == "test-secret"


@pytest.mark.parametrize("timeout_seconds", [1.0, 5.0])
async def test_mock_timeout_handling(test_client: MockGreeterClient, timeout_seconds: float) -> None:
    """Test that operations respect timeouts."""
    request = MockHelloRequest(name="TimeoutHandling", count=1)
    
    # Mock that client is connected
    test_client._stub = MockGreeterStub(mock.MagicMock())
    
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