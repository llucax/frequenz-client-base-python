# License: MIT
# Copyright © 2023 Frequenz Energy-as-a-Service GmbH

"""Implementation of the grpc streaming helper."""

import asyncio
import logging
from collections.abc import AsyncIterator, Callable, Mapping
from typing import Generic, TypeVar, overload

from typing_extensions import override

from frequenz import channels

from . import retry
from ._grpchacks import GrpcioError, GrpclibError

_logger = logging.getLogger(__name__)


StubOutT = TypeVar("StubOutT")
"""The type of the response from a gRPC stub method."""

TransformOutT_co = TypeVar("TransformOutT_co", covariant=True)
"""The type of the transformed response from a gRPC stub method."""

BroadcasterMapKeyT = TypeVar("BroadcasterMapKeyT")
"""The type of the key used to map broadcasters to their keys."""


class GrpcStreamBroadcaster(Generic[StubOutT, TransformOutT_co]):
    """Helper class to handle grpc streaming methods."""

    def __init__(
        self,
        stream_name: str,
        stream_method: Callable[[], AsyncIterator[StubOutT]],
        transform: Callable[[StubOutT], TransformOutT_co],
        retry_strategy: retry.Strategy | None = None,
    ):
        """Initialize the streaming helper.

        Args:
            stream_name: A name to identify the stream in the logs.
            stream_method: A function that returns the grpc stream. This function is
                called everytime the connection is lost and we want to retry.
            transform: A function to transform the input type to the output type.
            retry_strategy: The retry strategy to use, when the connection is lost. Defaults
                to retries every 3 seconds, with a jitter of 1 second, indefinitely.
        """
        self._stream_name = stream_name
        self._stream_method = stream_method
        self._transform = transform
        self._retry_strategy = (
            retry.LinearBackoff() if retry_strategy is None else retry_strategy.copy()
        )

        self._channel: channels.Broadcast[TransformOutT_co] = channels.Broadcast(
            name=f"GrpcStreamBroadcaster-{stream_name}"
        )
        self._task = asyncio.create_task(self._run())

    def new_receiver(self, maxsize: int = 50) -> channels.Receiver[TransformOutT_co]:
        """Create a new receiver for the stream.

        Args:
            maxsize: The maximum number of messages to buffer.

        Returns:
            A new receiver.
        """
        return self._channel.new_receiver(limit=maxsize)

    async def stop(self) -> None:
        """Stop the streaming helper."""
        if self._task.done():
            return
        self._task.cancel()
        try:
            await self._task
        except asyncio.CancelledError:
            pass
        await self._channel.close()

    async def _run(self) -> None:
        """Run the streaming helper."""
        sender = self._channel.new_sender()

        while True:
            error: Exception | None = None
            _logger.info("%s: starting to stream", self._stream_name)
            try:
                call = self._stream_method()
                async for msg in call:
                    await sender.send(self._transform(msg))
            except (GrpcioError, GrpclibError) as err:
                error = err
            error_str = f"Error: {error}" if error else "Stream exhausted"
            interval = self._retry_strategy.next_interval()
            if interval is None:
                _logger.error(
                    "%s: connection ended, retry limit exceeded (%s), giving up. %s.",
                    self._stream_name,
                    self._retry_strategy.get_progress(),
                    error_str,
                )
                await self._channel.close()
                break
            _logger.warning(
                "%s: connection ended, retrying %s in %0.3f seconds. %s.",
                self._stream_name,
                self._retry_strategy.get_progress(),
                interval,
                error_str,
            )
            await asyncio.sleep(interval)


class GrpcStreamBroadcasterMap(
    Mapping[BroadcasterMapKeyT, GrpcStreamBroadcaster[StubOutT, TransformOutT_co]]
):

    def __init__(
        self,
        broadcasters: (
            dict[BroadcasterMapKeyT, GrpcStreamBroadcaster[StubOutT, TransformOutT_co]]
            | None
        ) = None,
        *,
        channel_name_prefix: str | None = None,
    ) -> None:
        if broadcasters is None:
            broadcasters = {}
        self._broadcasters: dict[
            BroadcasterMapKeyT, GrpcStreamBroadcaster[StubOutT, TransformOutT_co]
        ] = broadcasters
        self._channel_name_prefix = channel_name_prefix

    @property
    def channel_name_prefix(self) -> str | None:
        return self._channel_name_prefix

    @override
    def __getitem__(
        self, key: BroadcasterMapKeyT
    ) -> GrpcStreamBroadcaster[StubOutT, TransformOutT_co]:
        broadcaster = self._broadcasters.get(key)
        if broadcaster is None:
            broadcaster = GrpcStreamBroadcaster(
                f"{self._channel_name_prefix or ''}{key}",
                stream_method,
                transform,
                retry_strategy=retry_strategy,
            )
            broadcasters[key] = broadcaster
        return broadcaster.new_receiver(maxsize=buffer_size)
            return self._broadcasters[key]
