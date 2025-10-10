# License: MIT
# Copyright © 2024 Frequenz Energy-as-a-Service GmbH

"""Integration tests for gRPC BaseApiClient and utilities."""

import asyncio
import logging
import subprocess
import sys
from collections.abc import AsyncIterator
from pathlib import Path

import pytest

from frequenz.client.base.client import call_stub_method
from frequenz.client.base.streaming import GrpcStreamBroadcaster

from . import service_pb2
from .client import GreeterClient
from .server import GrpcServer


@pytest.fixture(scope="session", autouse=True)
def build_protos() -> None:
    """Compile protobuf python files for integration tests.

    Runs:
        python -m grpc_tools.protoc -I. --python_out=. --mypy_grpc_out=. --grpc_python_out=. --mypy_out=. tests/integration/service.proto
    from the repository root so generated files are placed under tests/integration.
    """
    project_root = Path(__file__).resolve().parents[2]

    cmd = [
        sys.executable,
        "-m",
        "grpc_tools.protoc",
        "-I.",
        "--python_out=.",
        "--mypy_grpc_out=.",
        "--grpc_python_out=.",
        "--mypy_out=.",
        "tests/integration/service.proto",
    ]

    try:
        subprocess.run(cmd, cwd=str(project_root), check=True)
    except subprocess.CalledProcessError as exc:
        pytest.exit(f"Failed to build protobuf files: {exc}")


@pytest.fixture
def auth_key() -> str | None:
    """Get the default auth key."""
    return None


@pytest.fixture
def sign_secret() -> str | None:
    """Get the default signing secret."""
    return None


@pytest.fixture
async def server() -> AsyncIterator[GrpcServer]:
    """Fixture providing a test gRPC server."""
    async with GrpcServer() as server:
        yield server


@pytest.fixture
async def client(
    server: GrpcServer, auth_key: str | None, sign_secret: str | None
) -> AsyncIterator[GreeterClient]:
    """Fixture providing a test gRPC client."""
    print(f"Creating client fixture, {sign_secret=} {auth_key=}")
    async with GreeterClient(
        f"grpc://localhost:{server.port}",
        connect=True,
        auth_key=auth_key,
        sign_secret=sign_secret,
    ) as client:
        yield client


class TestUnaryRpc:
    """Tests for unary RPC calls."""

    @pytest.mark.parametrize(
        "auth_key", [None, "test-api-key"], ids=["no-auth", "with-auth"]
    )
    @pytest.mark.parametrize(
        "sign_secret", [None, "signing-secret"], ids=["no-sign", "with-sign"]
    )
    async def test_unary_call_basic(
        self, client: GreeterClient, auth_key: str | None, sign_secret: str | None
    ) -> None:
        """Test basic unary RPC call."""
        response = await call_stub_method(
            client, lambda: client.stub.SayHello(service_pb2.HelloRequest(name="World"))
        )

        assert response.message == "Hello World!"
        assert response.sequence == 0
        if auth_key:
            assert "(authenticated with key: test-api-key)" in response.message
        if sign_secret:
            assert "(signed)" in response.message

    async def test_unary_call_timeout(self, client: GreeterClient) -> None:
        """Test unary RPC call with timeout handling."""
        request = service_pb2.HelloRequest(name="TimeoutTest", count=1)

        # This should complete within timeout
        response = await call_stub_method(client.stub.SayHello, request)

        assert response.message == "Hello TimeoutTest!"


class TestStreamingRpc:
    """Tests for streaming RPC calls."""

    async def test_streaming_call_basic(self, client: GreeterClient) -> None:
        """Test basic streaming RPC call."""
        request = service_pb2.HelloRequest(name="StreamWorld", count=3)

        messages = []
        async with asyncio.timeout(10):
            stream = client.stub.StreamHellos(request)
            async for response in stream:
                messages.append(response)

        assert len(messages) == 3
        for i, msg in enumerate(messages):
            assert f"Hello StreamWorld #{i+1}" in msg.message
            assert msg.sequence == i

    async def test_streaming_call_with_auth(self, auth_client: GreeterClient) -> None:
        """Test streaming RPC call with authentication."""
        request = service_pb2.HelloRequest(name="StreamAuth", count=2)

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

    async def test_stream_broadcaster_single_consumer(
        self, client: GreeterClient
    ) -> None:
        """Test GrpcStreamBroadcaster with single consumer."""
        request = service_pb2.HelloRequest(name="BroadcastTest", count=3)

        def create_stream():
            return client.stub.StreamHellos(request)

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

    async def test_stream_broadcaster_multiple_consumers(
        self, client: GreeterClient
    ) -> None:
        """Test GrpcStreamBroadcaster with multiple consumers."""
        request = service_pb2.HelloRequest(name="MultiConsumer", count=5)

        def create_stream():
            return client.stub.StreamHellos(request)

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

    async def test_stream_broadcaster_with_events(self, client: GreeterClient) -> None:
        """Test GrpcStreamBroadcaster with event receiver."""
        request = service_pb2.HelloRequest(name="EventTest", count=2)

        def create_stream():
            return client.stub.StreamHellos(request)

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
async def test_timeout_handling(client: GreeterClient, timeout_seconds: float) -> None:
    """Test that all operations respect timeouts."""
    request = service_pb2.HelloRequest(name="TimeoutHandling", count=1)

    # This should complete well within the timeout
    response = await call_stub_method(
        client.stub.SayHello,
        request,
        timeout=timeout_seconds,
    )

    assert response.message == "Hello TimeoutHandling!"


if __name__ == "__main__":
    # Allow running tests directly
    pytest.main([__file__, "-v"])
