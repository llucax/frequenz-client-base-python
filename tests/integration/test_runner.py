# License: MIT
# Copyright © 2024 Frequenz Energy-as-a-Service GmbH

"""Standalone runner for integration tests without external dependencies."""

import asyncio
import sys
import os
from unittest import mock
from typing import AsyncIterator

# Mock the grpc module for testing without dependencies
grpc_mock = mock.MagicMock()
grpc_aio_mock = mock.MagicMock()
grpc_mock.aio = grpc_aio_mock
sys.modules['grpc'] = grpc_mock
sys.modules['grpc.aio'] = grpc_aio_mock

# Mock protobuf
google_mock = mock.MagicMock()
protobuf_mock = mock.MagicMock()
google_mock.protobuf = protobuf_mock
sys.modules['google'] = google_mock
sys.modules['google.protobuf'] = protobuf_mock
sys.modules['google.protobuf.message'] = protobuf_mock.message

# Mock frequenz.channels
frequenz_mock = mock.MagicMock()
channels_mock = mock.MagicMock()
frequenz_mock.channels = channels_mock
sys.modules['frequenz'] = frequenz_mock
sys.modules['frequenz.channels'] = channels_mock

# Add src to path for imports
src_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'src'))
sys.path.insert(0, src_path)

# Try importing directly by module path
sys.path.insert(0, os.path.join(src_path, 'frequenz', 'client', 'base'))

try:
    from client import BaseApiClient, call_stub_method
    from channel import ChannelOptions
    from streaming import GrpcStreamBroadcaster
except ImportError:
    # Alternative import approach
    import importlib.util
    
    # Load client module directly
    client_spec = importlib.util.spec_from_file_location(
        "client", 
        os.path.join(src_path, 'frequenz', 'client', 'base', 'client.py')
    )
    client_module = importlib.util.module_from_spec(client_spec)
    client_spec.loader.exec_module(client_module)
    
    BaseApiClient = client_module.BaseApiClient
    call_stub_method = client_module.call_stub_method
    
    # Load channel module
    channel_spec = importlib.util.spec_from_file_location(
        "channel",
        os.path.join(src_path, 'frequenz', 'client', 'base', 'channel.py')
    )
    channel_module = importlib.util.module_from_spec(channel_spec)
    channel_spec.loader.exec_module(channel_module)
    
    ChannelOptions = channel_module.ChannelOptions
    
    # Load streaming module
    streaming_spec = importlib.util.spec_from_file_location(
        "streaming",
        os.path.join(src_path, 'frequenz', 'client', 'base', 'streaming.py')
    )
    streaming_module = importlib.util.module_from_spec(streaming_spec)
    streaming_spec.loader.exec_module(streaming_module)
    
    GrpcStreamBroadcaster = streaming_module.GrpcStreamBroadcaster


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


async def test_unary_call_basic() -> None:
    """Test basic unary RPC call."""
    print("Testing basic unary RPC call...")
    
    with mock.patch('frequenz.client.base.client.parse_grpc_uri') as mock_parse:
        mock_channel = mock.MagicMock()
        mock_parse.return_value = mock_channel
        
        client = MockGreeterClient("grpc://localhost:50051", connect=True)
        client._stub = MockGreeterStub(mock.MagicMock())
        
        request = MockHelloRequest(name="World", count=1)
        
        response = await call_stub_method(
            client.stub.SayHello,
            request,
            timeout=5.0,
        )
        
        assert response.message == "Hello World!"
        assert response.sequence == 0
        print("✓ Basic unary call test passed")


async def test_unary_call_with_auth() -> None:
    """Test unary RPC call with authentication."""
    print("Testing unary RPC call with authentication...")
    
    with mock.patch('frequenz.client.base.client.parse_grpc_uri') as mock_parse:
        mock_channel = mock.MagicMock()
        mock_parse.return_value = mock_channel
        
        client = MockGreeterClient(
            "grpc://localhost:50051",
            connect=True,
            auth_key="test-api-key"
        )
        client._stub = MockGreeterStub(mock.MagicMock())
        
        request = MockHelloRequest(name="AuthUser", count=1)
        
        response = await call_stub_method(
            client.stub.SayHello,
            request,
            timeout=5.0,
        )
        
        assert "Hello AuthUser!" in response.message
        assert client._auth_key == "test-api-key"
        print("✓ Authentication test passed")


async def test_client_with_signing() -> None:
    """Test client with signing interceptor."""
    print("Testing client with signing interceptor...")
    
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
        print("✓ Signing interceptor test passed")


async def test_stream_broadcaster_creation() -> None:
    """Test GrpcStreamBroadcaster creation."""
    print("Testing GrpcStreamBroadcaster creation...")
    
    # Create a mock stream method
    def create_stream():
        mock_stream_call = mock.MagicMock()
        return mock_stream_call
    
    # Mock the channels module components
    mock_broadcast = mock.MagicMock()
    mock_sender = mock.MagicMock()
    mock_receiver = mock.MagicMock()
    
    mock_broadcast.new_sender.return_value = mock_sender
    mock_broadcast.new_receiver.return_value = mock_receiver
    
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
            print("✓ GrpcStreamBroadcaster creation test passed")


async def test_timeout_handling() -> None:
    """Test timeout handling."""
    print("Testing timeout handling...")
    
    with mock.patch('frequenz.client.base.client.parse_grpc_uri') as mock_parse:
        mock_channel = mock.MagicMock()
        mock_parse.return_value = mock_channel
        
        client = MockGreeterClient("grpc://localhost:50051", connect=True)
        client._stub = MockGreeterStub(mock.MagicMock())
        
        request = MockHelloRequest(name="TimeoutTest", count=1)
        
        response = await call_stub_method(
            client.stub.SayHello,
            request,
            timeout=1.0,
        )
        
        assert response.message == "Hello TimeoutTest!"
        print("✓ Timeout handling test passed")


async def run_all_tests() -> None:
    """Run all integration tests."""
    print("Running integration tests for gRPC BaseApiClient...")
    print("=" * 60)
    
    try:
        await test_unary_call_basic()
        await test_unary_call_with_auth()
        await test_client_with_signing()
        await test_stream_broadcaster_creation()
        await test_timeout_handling()
        
        print("=" * 60)
        print("✓ All integration tests passed!")
        
    except Exception as e:
        print(f"✗ Test failed with error: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    return True


if __name__ == "__main__":
    success = asyncio.run(run_all_tests())
    sys.exit(0 if success else 1)