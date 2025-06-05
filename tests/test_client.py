# License: MIT
# Copyright © 2024 Frequenz Energy-as-a-Service GmbH

"""Tests for the BaseApiClient class."""

from collections.abc import Callable
from unittest import mock

import grpc.aio
import pytest
import pytest_mock

from frequenz.client.base.channel import ChannelOptions, SslOptions
from frequenz.client.base.client import (
    BaseApiClient,
    StubT,
    call_stub_method,
)
from frequenz.client.base.exception import ClientNotConnected, UnknownError


def _auto_connect_name(auto_connect: bool) -> str:
    return f"{auto_connect=}"


_DEFAULT_SERVER_URL = "grpc://localhost"


@pytest.fixture
def stub() -> mock.MagicMock:
    """Return a mock stub."""
    return mock.MagicMock(name="stub")


@pytest.fixture
def create_stub(stub: mock.MagicMock) -> mock.MagicMock:
    """Return a mock create_stub function."""
    return mock.MagicMock(name="create_stub", return_value=stub)


@pytest.fixture
def channel() -> mock.MagicMock:
    """Return a mock channel."""
    return mock.MagicMock(name="channel", spec=grpc.aio.Channel)


@pytest.fixture
def parse_grpc_uri(
    mocker: pytest_mock.MockFixture, channel: mock.MagicMock
) -> mock.MagicMock:
    """Return a mock parse_grpc_uri function."""
    return mocker.patch(
        "frequenz.client.base.client.parse_grpc_uri",
        name="parse_grpc_uri",
        return_value=channel,
    )


class TestBaseApiClient:
    """Test class for BaseApiClient."""

    _stub: mock.MagicMock
    _create_stub: mock.MagicMock
    _channel: mock.MagicMock
    _parse_grpc_uri: mock.MagicMock

    def _assert_is_disconnected(self, client: BaseApiClient[StubT]) -> None:
        """Assert that the client is disconnected."""
        assert not client.is_connected

        with pytest.raises(ClientNotConnected, match=r"") as exc_info:
            _ = client.channel
        exc = exc_info.value
        assert exc.server_url == _DEFAULT_SERVER_URL
        assert exc.operation == "channel"

        with pytest.raises(ClientNotConnected, match=r"") as exc_info:
            _ = client.channel
        exc = exc_info.value
        assert exc.server_url == _DEFAULT_SERVER_URL
        assert exc.operation == "channel"

    @pytest.fixture(autouse=True)
    def attach_fixtures_to_self(
        self,
        stub: mock.MagicMock,
        create_stub: mock.MagicMock,
        channel: mock.MagicMock,
        parse_grpc_uri: mock.MagicMock,
    ) -> None:
        """Attach fixtures to self for use in tests."""
        self._stub = stub
        self._create_stub = create_stub
        self._channel = channel
        self._parse_grpc_uri = parse_grpc_uri

    @pytest.mark.parametrize("auto_connect", [True, False], ids=_auto_connect_name)
    async def test_init(self, auto_connect: bool) -> None:
        """Test initializing the BaseApiClient."""
        client = BaseApiClient(
            _DEFAULT_SERVER_URL, self._create_stub, connect=auto_connect
        )

        assert client.server_url == _DEFAULT_SERVER_URL
        if auto_connect:
            self._parse_grpc_uri.assert_called_once_with(
                client.server_url,
                [],
                defaults=ChannelOptions(),
            )
            assert client.channel is self._channel
            assert client._stub is self._stub  # pylint: disable=protected-access
            assert client.is_connected
            self._create_stub.assert_called_once_with(self._channel)
            await client.disconnect()
        else:
            self._assert_is_disconnected(client)
            self._parse_grpc_uri.assert_not_called()
            self._create_stub.assert_not_called()

    async def test_init_with_channel_defaults(self) -> None:
        """Test initializing the BaseApiClient with channel defaults."""
        channel_defaults = ChannelOptions(ssl=SslOptions(enabled=False))
        async with BaseApiClient(
            _DEFAULT_SERVER_URL, self._create_stub, channel_defaults=channel_defaults
        ) as client:
            assert client.server_url == _DEFAULT_SERVER_URL
            self._parse_grpc_uri.assert_called_once_with(
                client.server_url,
                [],
                defaults=channel_defaults,
            )
            assert client.channel is self._channel
            assert client._stub is self._stub  # pylint: disable=protected-access
            assert client.is_connected
            self._create_stub.assert_called_once_with(self._channel)

    @pytest.mark.parametrize(
        "new_server_url", [None, _DEFAULT_SERVER_URL, "grpc://localhost:50051"]
    )
    @pytest.mark.parametrize("auto_connect", [True, False], ids=_auto_connect_name)
    async def test_connect(
        self,
        new_server_url: str | None,
        auto_connect: bool,
    ) -> None:
        """Test connecting the BaseApiClient."""
        client = BaseApiClient(
            _DEFAULT_SERVER_URL, self._create_stub, connect=auto_connect
        )
        # We want to check only what happens when we call connect, so we reset the mocks
        # that were called during initialization
        self._parse_grpc_uri.reset_mock()
        self._create_stub.reset_mock()

        client.connect(new_server_url)

        assert client.channel is self._channel
        assert client._stub is self._stub  # pylint: disable=protected-access
        assert client.is_connected

        same_url = new_server_url is None or new_server_url == _DEFAULT_SERVER_URL

        if same_url:
            assert client.server_url == _DEFAULT_SERVER_URL
        else:
            assert client.server_url == new_server_url

        # If we were previously connected and the URL didn't change, the client should not
        # reconnect
        if auto_connect and same_url:
            self._parse_grpc_uri.assert_not_called()
            self._create_stub.assert_not_called()
        else:
            self._parse_grpc_uri.assert_called_once_with(
                client.server_url,
                [],
                defaults=ChannelOptions(),
            )
            self._create_stub.assert_called_once_with(self._channel)

        if auto_connect:
            await client.disconnect()

    async def test_disconnect(self) -> None:
        """Test disconnecting the BaseApiClient."""
        client = BaseApiClient(_DEFAULT_SERVER_URL, self._create_stub, connect=True)

        await client.disconnect()

        self._channel.__aexit__.assert_called_once_with(None, None, None)
        assert client.server_url == _DEFAULT_SERVER_URL
        self._assert_is_disconnected(client)

    @pytest.mark.parametrize("auto_connect", [True, False], ids=_auto_connect_name)
    async def test_async_context_manager(self, auto_connect: bool) -> None:
        """Test using the BaseApiClient as an async context manager."""
        client = BaseApiClient(
            _DEFAULT_SERVER_URL, self._create_stub, connect=auto_connect
        )
        # We want to check only what happens when we enter the context manager, so we reset
        # the self that were called during initialization
        self._parse_grpc_uri.reset_mock()
        self._create_stub.reset_mock()

        async with client:
            assert client.channel is self._channel
            assert client._stub is self._stub  # pylint: disable=protected-access
            assert client.is_connected
            self._channel.__aexit__.assert_not_called()
            # If we were previously connected, the client should not reconnect when entering
            # the context manager
            if auto_connect:
                self._parse_grpc_uri.assert_not_called()
                self._create_stub.assert_not_called()
            else:
                self._parse_grpc_uri.assert_called_once_with(
                    client.server_url,
                    [],
                    defaults=ChannelOptions(),
                )
                self._create_stub.assert_called_once_with(self._channel)

        self._channel.__aexit__.assert_called_once_with(None, None, None)
        assert client.server_url == _DEFAULT_SERVER_URL
        self._assert_is_disconnected(client)

    async def test_create_interceptors(self) -> None:
        """Test that the client constructor creates the interceptors as expected."""
        async with BaseApiClient(
            _DEFAULT_SERVER_URL,
            self._create_stub,
            connect=True,
            auth_key="hunter2",
            sign_secret="password1245",
        ):
            self._parse_grpc_uri.assert_called_once_with(
                _DEFAULT_SERVER_URL,
                [mock.ANY, mock.ANY, mock.ANY, mock.ANY],
                defaults=ChannelOptions(),
            )
            args, _ = self._parse_grpc_uri.call_args
            interceptors = args[1]
            for interceptor in interceptors:
                assert isinstance(interceptor, grpc.aio.ClientInterceptor)


def _transform_name(transform: bool) -> str:
    return "transform" if transform else "no_transform"


@pytest.fixture
def mock_transform() -> mock.MagicMock:
    """Return a mock transform function."""
    return mock.MagicMock(name="transform", spec=Callable[[int], int], return_value=2)


@pytest.mark.parametrize(
    "method_name",
    [None, "method"],
)
@pytest.mark.parametrize(
    "transform",
    [True, False],
    ids=_transform_name,
)
async def test_call_stub_method_not_connected(
    method_name: str, transform: bool, mock_transform: mock.MagicMock | None
) -> None:
    """Test calling a stub method when the client is not connected."""
    mock_client = mock.MagicMock(name="client", spec=BaseApiClient)
    mock_client.is_connected = False
    mock_client.server_url = "server_url"
    mock_stub_method = mock.AsyncMock(name="stub_method")
    if not transform:
        mock_transform = None

    with pytest.raises(ClientNotConnected) as exc_info:
        _ = await call_stub_method(
            mock_client,
            mock_stub_method,
            transform=mock_transform,
            method_name=method_name,
        )
    mock_stub_method.assert_not_called()
    assert exc_info.value.server_url == "server_url"
    assert exc_info.value.operation == (
        method_name or "test_call_stub_method_not_connected"
    )
    if mock_transform:
        mock_transform.assert_not_called()


@pytest.mark.parametrize(
    "method_name",
    [None, "method"],
)
@pytest.mark.parametrize(
    "transform",
    [True, False],
    ids=_transform_name,
)
async def test_call_stub_method_exception(
    method_name: str,
    transform: bool,
    mock_transform: mock.MagicMock | None,
) -> None:
    """Test calling a stub method that raises an exception."""
    mock_client = mock.MagicMock(name="client", spec=BaseApiClient)
    mock_client.is_connected = True
    mock_client.server_url = "server_url"
    exception = grpc.aio.AioRpcError(
        grpc.StatusCode.UNKNOWN,
        mock.MagicMock(name="initial_metadata"),
        mock.MagicMock(name="trailing_metadata"),
        "details",
        "debug_error_string",
    )
    mock_stub_method = mock.MagicMock(name="stub_method", side_effect=exception)
    if not transform:
        mock_transform = None

    with pytest.raises(UnknownError) as exc_info:
        _ = await call_stub_method(
            mock_client,
            mock_stub_method,
            transform=mock_transform,
            method_name=method_name,
        )
    mock_stub_method.assert_called_once_with()
    assert exc_info.value.server_url == "server_url"
    assert exc_info.value.operation == (
        method_name or "test_call_stub_method_exception"
    )
    assert exc_info.value.__cause__ is exception
    assert exc_info.value.grpc_error is exception
    if mock_transform:
        mock_transform.assert_not_called()


@pytest.mark.parametrize(
    "method_name",
    [None, "method"],
)
@pytest.mark.parametrize(
    "transform",
    [True, False],
    ids=_transform_name,
)
async def test_call_stub_method_success(
    method_name: str,
    transform: bool,
    mock_transform: mock.MagicMock | None,
) -> None:
    """Test calling a stub method that succeeds."""
    mock_client = mock.MagicMock(name="client", spec=BaseApiClient)
    mock_client.is_connected = True
    mock_client.server_url = "server_url"
    mock_stub_method = mock.AsyncMock(name="stub_method", return_value=1)
    if not transform:
        mock_transform = None

    response = await call_stub_method(
        mock_client,
        mock_stub_method,
        transform=mock_transform,
        method_name=method_name,
    )

    mock_stub_method.assert_called_once_with()
    assert response == (2 if transform else 1)
    if mock_transform:
        mock_transform.assert_called_once_with(1)
