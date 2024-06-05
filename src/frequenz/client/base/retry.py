# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""Implementations for retry strategies."""

import random
from abc import ABC, abstractmethod
from copy import deepcopy
from typing import Self

from typing_extensions import override

DEFAULT_RETRY_INTERVAL = 3.0
"""Default retry interval, in seconds."""

DEFAULT_RETRY_JITTER = 1.0
"""Default retry jitter, in seconds."""


class Strategy(ABC):
    """Interface for implementing retry strategies."""

    def __init__(self, *, limit: int | None = None) -> None:
        """Create an instance.

        Args:
            limit: The maximum number of retries before giving up. `None` means no
                limit, and `0` means no retry.
        """
        self.limit = limit  # Assign via property to enforce validation
        self._count = 0
        self._last_interval: float | None = None

    @property
    def limit(self) -> int | None:
        """The maximum number of retries before giving up.

        `None` means no limit and `0` means no retry.

        If set to a negative value, a `ValueError` is raised.
        """
        return self._limit

    @limit.setter
    def limit(self, value: int | None) -> None:
        if value is not None and value < 0:
            raise ValueError(f"limit must be non-negative, got {value}")
        self._limit = value

    @property
    def count(self) -> int:
        """The number of retries attempted so far."""
        return self._count

    @property
    def _is_exhausted(self) -> bool:
        """Whether the retry limit has been reached."""
        return self._limit is not None and self._count >= self._limit

    @abstractmethod
    def _calculate_next_wait(self) -> float:
        """Calculate the time to wait before the next retry.

        This method doesn't have into account the retry limit, it is mainly intended as
        a helper for the `next_interval()` method and the method that should be
        implemented by subclasses.

        Returns:
            The time to wait before the next retry, in seconds.
        """

    def next_interval(self) -> float | None:
        """Return the time to wait before the next retry.

        Returns `None` if the retry limit has been reached, and no more retries
        are possible.

        Returns:
            The time until next retry in seconds when below retry limit, and `None` if
                retrying is
                [exhausted][frequenz.client.base.retry.Strategy.is_exhausted].
        """
        if self._is_exhausted:
            self._last_interval = None
            return None
        self._count += 1
        self._last_interval = self._calculate_next_wait()
        return self._last_interval

    def __str__(self) -> str:
        """Return a string denoting the retry progress."""
        progress = (
            f"({self._count}/∞)"
            if self._limit is None
            else f"{self._count}/{self._limit}"
        )
        if self._is_exhausted and self._last_interval is None:
            return f"retry limit reached ({progress})"

        time_info = (
            "" if self._last_interval is None else f" in {self._last_interval} seconds"
        )
        return f"retrying ({progress}){time_info}"

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


class LinearBackoff(Strategy):
    """Provides methods for calculating the interval between retries."""

    def __init__(
        self,
        interval: float = DEFAULT_RETRY_INTERVAL,
        jitter: float = DEFAULT_RETRY_JITTER,
        *,
        limit: int | None = None,
    ) -> None:
        """Create a `LinearBackoff` instance.

        Args:
            interval: time to wait for before the next retry, in seconds.
            jitter: a jitter to add to the retry interval.
            limit: max number of retries before giving up.  `None` means no
                limit, and `0` means no retry.
        """
        super().__init__(limit=limit)
        self._interval = interval
        self._jitter = jitter

    @override
    def _calculate_next_wait(self) -> float:
        """Calculate the time to wait before the next retry.

        This method doesn't have into account the retry limit, it is mainly intended as
        a helper for the `next_interval()` method and the method that should be
        implemented by subclasses.

        Returns:
            The time to wait before the next retry, in seconds.
        """
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
        *,
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
        super().__init__(limit=limit)
        self._initial = initial_interval
        self._max = max_interval
        self._multiplier = multiplier
        self._jitter = jitter

    @override
    def _calculate_next_wait(self) -> float:
        """Calculate the time to wait before the next retry.

        This method doesn't have into account the retry limit, it is mainly intended as
        a helper for the `next_interval()` method and the method that should be
        implemented by subclasses.

        Returns:
            The time to wait before the next retry, in seconds.
        """
        exp_backoff_interval = self._initial * self._multiplier ** (self._count - 1)
        return min(exp_backoff_interval + random.uniform(0.0, self._jitter), self._max)
