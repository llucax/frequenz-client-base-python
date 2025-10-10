# License: MIT
# Copyright © 2024 Frequenz Energy-as-a-Service GmbH

"""Test gRPC server implementation for integration tests."""

from __future__ import annotations

import asyncio
import base64
import hashlib
import hmac
import logging
from collections.abc import AsyncIterator
from typing import Any, Mapping, Self

import grpc.aio

from . import service_pb2, service_pb2_grpc

_logger = logging.getLogger(__name__)


class GreeterServicer(service_pb2_grpc.GreeterServicer):
    """Implementation of the Greeter service for testing."""

    def __init__(
        self, signing_secrets: Mapping[str, str] | None = None
    ) -> None:  # key -> secret
        """Create the servicer.

        Args:
            signing_secrets: Optional mapping of API key to secret to enable
                HMAC based request signature verification.
        """
        self._signing_secrets = dict(signing_secrets or {})

    # --- internal helpers -------------------------------------------------
    def _verify_signature(
        self, context: grpc.aio.ServicerContext[Any, Any], rpc: str
    ) -> bool:
        """Verify request signature if present.

        Returns True if signature is valid or not provided, False if invalid.
        """
        if not self._signing_secrets:
            return True  # Signing not enabled on server.

        # Metadata values may be bytes (grpcio types); cast to str for uniformity.
        md: dict[str, str] = {}
        for k, v in context.invocation_metadata() or []:  # type: ignore[assignment]
            if isinstance(v, bytes):  # defensive
                try:
                    md[k] = v.decode()
                except Exception:  # noqa: BLE001 - best effort
                    continue
            else:
                md[k] = str(v)

        sig = md.get("sig")
        key = md.get("key")
        ts = md.get("ts")
        nonce = md.get("nonce")
        if not sig:
            return True  # No signature provided; allow (so we can test unsigned requests too).
        if not (key and ts and nonce):
            return False
        secret = self._signing_secrets.get(key)
        if not secret:
            return False

        # Reproduce algorithm: HMAC_SHA256(secret, f"{key}:{ts}:{nonce}:{rpc}") then
        # base64url encode without padding.
        msg = f"{key}:{ts}:{nonce}:{rpc}".encode()
        digest = hmac.new(secret.encode(), msg, hashlib.sha256).digest()
        expected = base64.urlsafe_b64encode(digest).decode().rstrip("=")
        return hmac.compare_digest(expected, sig)

    async def SayHello(  # pylint: disable=invalid-name
        self,
        request: service_pb2.HelloRequest,
        context: grpc.aio.ServicerContext[Any, Any],
    ) -> service_pb2.HelloReply:
        """Handle unary SayHello RPC."""
        _logger.info("SayHello called with name: %s", request.name)

        auth_key = None
        if metadata := context.invocation_metadata():
            for key, value in metadata:
                if key == "key":
                    auth_key = value
                    break

        rpc_name = "SayHello"
        if not self._verify_signature(context, rpc_name):
            await context.abort(grpc.StatusCode.UNAUTHENTICATED, "Invalid signature")

        message = f"Hello {request.name}!"
        if auth_key:
            message += f" (authenticated with key: {auth_key!r})"
        if self._signing_secrets and "sig" in {
            k for k, _ in (context.invocation_metadata() or [])
        }:
            message += " (signed)"
        return service_pb2.HelloReply(message=message, sequence=0)

    async def StreamHellos(  # pylint: disable=invalid-name
        self,
        request: service_pb2.HelloRequest,
        context: grpc.aio.ServicerContext[Any, Any],
    ) -> AsyncIterator[service_pb2.HelloReply]:
        """Handle streaming StreamHellos RPC."""
        _logger.info(
            "StreamHellos called with name: %s, count: %s",
            request.name,
            request.count,
        )

        # Get auth key from metadata
        auth_key = None
        if metadata := context.invocation_metadata():
            for key, value in metadata:
                if key == "key":
                    auth_key = value
                    break

        rpc_name = "StreamHellos"
        if not self._verify_signature(context, rpc_name):
            await context.abort(grpc.StatusCode.UNAUTHENTICATED, "Invalid signature")

        count = max(1, request.count)  # Ensure at least 1 message
        for i in range(count):
            if context.cancelled():
                break

            message = f"Hello {request.name} #{i}"
            if auth_key:
                message += " (authenticated)"
            if self._signing_secrets and "sig" in {
                k for k, _ in (context.invocation_metadata() or [])
            }:
                message += " (signed)"

            yield service_pb2.HelloReply(message=message, sequence=i)

            # Small delay to simulate real streaming
            await asyncio.sleep(0.1)


class GrpcServer:
    """Test gRPC server for integration tests."""

    def __init__(
        self, port: int = 0, signing_secrets: Mapping[str, str] | None = None
    ) -> None:
        """Initialize the test server.

        Args:
            port: Port to bind to, 0 for any available port.
            signing_secrets: Optional mapping of API key to secret. If provided,
                request signatures (when present) will be verified.
        """
        self.server: grpc.aio.Server | None = None
        self.request_port: int = port
        self._port: int | None = None
        self._signing_secrets = dict(signing_secrets or {})

    @property
    def port(self) -> int:
        """Get the actual listening port.

        Returns:
            The bound port number.
        """
        assert self._port is not None
        return self._port

    async def start(self) -> None:
        """Start the test server."""
        assert not self.server
        _logger.info("Starting gRPC server with requested port %s", self.request_port)

        servicer = GreeterServicer(self._signing_secrets)
        self.server = grpc.aio.server()
        self._port = self.server.add_insecure_port(f"localhost:{self.request_port}")
        service_pb2_grpc.add_GreeterServicer_to_server(servicer, self.server)
        await self.server.start()
        _logger.info("Started gRPC server on localhost:%s", self._port)

    async def wait_for_termination(self) -> None:
        """Wait for the server to terminate."""
        assert self.server
        await self.server.wait_for_termination()

    async def stop(self) -> None:
        """Stop the test server."""
        if self.server:
            _logger.info("Stopping gRPC server")
            await self.server.stop(grace=1.0)
            self.server = None

    async def __aenter__(self) -> Self:
        """Start the server when entering an async context and return self.

        Returns:
            A tuple of self and the actual port the server is listening on.
        """
        await self.start()
        return self

    async def __aexit__(
        self,
        _exc_type: type[BaseException] | None,
        _exc_val: BaseException | None,
        _exc_tb: Any | None,
    ) -> bool | None:
        """Stop the server when exiting an async context."""
        await self.stop()
        return None


async def _run() -> None:
    """Run the gRPC server indefinitely."""
    logging.basicConfig(level=logging.INFO)
    async with GrpcServer() as server:
        await server.wait_for_termination()


if __name__ == "__main__":
    asyncio.run(_run())
