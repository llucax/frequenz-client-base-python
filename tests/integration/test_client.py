# License: MIT
# Copyright © 2024 Frequenz Energy-as-a-Service GmbH

"""Test client implementation for integration tests."""

from typing import TYPE_CHECKING

import grpc.aio

# We need to add the src path to import the base client
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'src'))

from frequenz.client.base.client import BaseApiClient
from frequenz.client.base.channel import ChannelOptions

from . import demo_pb2_grpc

if TYPE_CHECKING:
    # Use async stub for proper type hints
    _GreeterStub = demo_pb2_grpc.GreeterAsyncStub
else:
    # Use sync stub for runtime
    _GreeterStub = demo_pb2_grpc.GreeterStub


class GreeterClient(BaseApiClient[_GreeterStub]):
    """Test client for the Greeter service."""

    def __init__(
        self,
        server_url: str,
        *,
        connect: bool = True,
        channel_defaults: ChannelOptions | None = None,
        auth_key: str | None = None,
        sign_secret: str | None = None,
    ) -> None:
        """Initialize the client.
        
        Args:
            server_url: gRPC server URL.
            connect: Whether to connect immediately.
            channel_defaults: Default channel options.
            auth_key: API key for authentication.
            sign_secret: Secret for signing requests.
        """
        super().__init__(
            server_url=server_url,
            create_stub=self._create_stub,
            connect=connect,
            channel_defaults=channel_defaults or ChannelOptions(),
            auth_key=auth_key,
            sign_secret=sign_secret,
        )

    def _create_stub(self, channel: grpc.aio.Channel) -> _GreeterStub:
        """Create the gRPC stub.
        
        Args:
            channel: gRPC channel.
            
        Returns:
            The gRPC stub.
        """
        return demo_pb2_grpc.GreeterStub(channel)  # type: ignore[return-value]

    @property
    def stub(self) -> _GreeterStub:
        """Get the gRPC stub with proper typing.
        
        Returns:
            The gRPC stub.
        """
        return self._stub  # type: ignore[return-value]