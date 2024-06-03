# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""Implementations for retry strategies."""

import random
from abc import ABC, abstractmethod
from collections.abc import Iterator
from copy import deepcopy
from typing import Self

from typing_extensions import override

DEFAULT_RETRY_INTERVAL = 3.0
"""Default retry interval, in seconds."""

DEFAULT_RETRY_JITTER = 1.0
"""Default retry jitter, in seconds."""


class Strategy(ABC):
    """Interface for implementing retry strategies."""

    _limit: int | None
    _count: int

    @abstractmethod
    def next_interval(self) -> float | None:
        """Return the time to wait before the next retry.

        Returns `None` if the retry limit has been reached, and no more retries
        are possible.

        Returns:
            Time until next retry when below retry limit, and None otherwise.
        """

    def get_progress(self) -> str:
        """Return a string denoting the retry progress.

        Returns:
            String denoting retry progress in the form "(count/limit)"
        """
        if self._limit is None:
            return f"({self._count}/∞)"

        return f"({self._count}/{self._limit})"

    def reset(self) -> None:
        """Reset the retry counter.

        To be called as soon as a connection is successful.
        """
        self._count = 0

    def copy(self) -> Self:
        """Create a new instance of `self`.

        Returns:
            A deepcopy of `self`.
        """
        ret = deepcopy(self)
        ret.reset()
        return ret

    def __iter__(self) -> Iterator[float]:
        """Return an iterator over the retry intervals.

        Yields:
            Next retry interval in seconds.
        """
        while True:
            interval = self.next_interval()
            if interval is None:
                break
            yield interval


class LinearBackoff(Strategy):
    """Provides methods for calculating the interval between retries."""

    def __init__(
        self,
        interval: float = DEFAULT_RETRY_INTERVAL,
        jitter: float = DEFAULT_RETRY_JITTER,
        limit: int | None = None,
    ) -> None:
        """Create a `LinearBackoff` instance.

        Args:
            interval: time to wait for before the next retry, in seconds.
            jitter: a jitter to add to the retry interval.
            limit: max number of retries before giving up.  `None` means no
                limit, and `0` means no retry.
        """
        self._interval = interval
        self._jitter = jitter
        self._limit = limit

        self._count = 0

    @override
    def next_interval(self) -> float | None:
        """Return the time to wait before the next retry.

        Returns `None` if the retry limit has been reached, and no more retries
        are possible.

        Returns:
            Time until next retry when below retry limit, and None otherwise.
        """
        if self._limit is not None and self._count >= self._limit:
            return None
        self._count += 1
        return self._interval + random.uniform(0.0, self._jitter)


class ExponentialBackoff(Strategy):
    """Provides methods for calculating the exponential interval between retries."""

    DEFAULT_INTERVAL = DEFAULT_RETRY_INTERVAL
    """Default retry interval, in seconds."""

    DEFAULT_MAX_INTERVAL = 60.0
    """Default maximum retry interval, in seconds."""

    DEFAULT_MULTIPLIER = 2.0
    """Default multiplier for exponential increment."""

    # pylint: disable=too-many-arguments
    def __init__(
        self,
        initial_interval: float = DEFAULT_INTERVAL,
        max_interval: float = DEFAULT_MAX_INTERVAL,
        multiplier: float = DEFAULT_MULTIPLIER,
        jitter: float = DEFAULT_RETRY_JITTER,
        limit: int | None = None,
    ) -> None:
        """Create a `ExponentialBackoff` instance.

        Args:
            initial_interval: time to wait for before the first retry, in
                seconds.
            max_interval: maximum interval, in seconds.
            multiplier: exponential increment for interval.
            jitter: a jitter to add to the retry interval.
            limit: max number of retries before giving up.  `None` means no
                limit, and `0` means no retry.
        """
        self._initial = initial_interval
        self._max = max_interval
        self._multiplier = multiplier
        self._jitter = jitter
        self._limit = limit

        self._count = 0

    @override
    def next_interval(self) -> float | None:
        """Return the time to wait before the next retry.

        Returns `None` if the retry limit has been reached, and no more retries
        are possible.

        Returns:
            Time until next retry when below retry limit, and None otherwise.
        """
        if self._limit is not None and self._count >= self._limit:
            return None
        self._count += 1
        exp_backoff_interval = self._initial * self._multiplier ** (self._count - 1)
        return min(exp_backoff_interval + random.uniform(0.0, self._jitter), self._max)


class StrategyMap(Strategy):
    """A mapping of exception types to retry strategies.

    This class is a subclass of `dict` that provides a more convenient way to create a
    mapping of exception types to retry strategies.

    The

    The order in the map is important, as `isinstance()` is used to find the first matching
    exception type, so exeptions higher in the hierarchy should be last, otherwise the
    subclass will never be matched. For example, if you have a retry strategy for
    """

    def __init__(self, exception_map: dict[type[Exception], Strategy]) -> None:
        """Create an instance.

        Args:
            exception_map: The mapping of exception types to retry strategies.
        """
        super().__init__()
        self.exception_map: dict[type[Exception], Strategy] = exception_map
        self.current_exception: type[Exception] | None = None
        self.validate()

    def validate(self) -> None:
        """Validate the mapping.

        This method checks that the exception types are ordered correctly. If an
        exception type is a subclass of another exception type, it should be placed
        after the parent exception type in the mapping.

        Raises:
            ValueError: If the mapping is invalid.
        """
        for exception_type in self.exception_map:
            for other_exception_type in self.exception_map:
                if exception_type is other_exception_type:
                    continue
                if issubclass(exception_type, other_exception_type):
                    raise ValueError(
                        f"{exception_type} is a subclass of {other_exception_type}, "
                        "but it is placed before it in the mapping. "
                        "Exception types should be ordered from the most specific to the "
                        "most general."
                    )

    @override
    def get_progress(self) -> str:
        """Return a string denoting the retry progress.

        Returns:
            String denoting retry progress in the form "(count/limit)"
        """
        if self._limit is None:
            return f"({self._count}/∞)"

        return ", ".join(
            f"{exception_type.__name__}:{strategy.get_progress()}"
            for exception_type, strategy in self.exception_map.items()
        )

    @override
    def reset(self) -> None:
        """Reset the retry counter.

        To be called as soon as a connection is successful.
        """
        for strategy in self.exception_map.values():
            strategy.reset()

    @override
    def next_interval(self) -> float | None:
        """Return the time to wait before the next retry.

        Returns `None` if the retry limit has been reached, and no more retries
        are possible.

        Returns:
            Time until next retry when below retry limit, and None otherwise.

        Raises:
            ValueError: If
                [`current_exception`][frequenz.client.base.retry.StrategyMap.current_exception]
                is not set.
        """
        if not self.current_exception:
            raise ValueError("No exception type set. Assign current_exception first.")
        for exception in reversed(self.exception_map):
            if isinstance(self.current_exception, exception):
                return self.exception_map[exception].next_interval()
        return None
