# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""Retry strategies for handling transient errors.

Retry strategies are used to calculate the time to wait before the next retry. They
usually have a limit on the number of retries before giving up.

Normally, retry strategies are used in combination with a loop that retries an
operation until it succeeds or the retry limit is reached. The loop should call the
[`next_interval()`][frequenz.client.base.retry.Strategy.next_interval] method to get
the time to wait before the next retry. If the method returns `None`, the retry limit
has been reached, and no more retries are possible.

Example:
    ```python
    import logging
    import asyncio

    def operation_that_may_fail():
        ...

    strategy: Strategy = LinearBackoff()
    while True:
        try:
            operation_that_may_fail()
        except RuntimeError as error:
            interval = strategy.next_interval()
            if interval is None:
                logging.error("Failed, %s, bailing out. Error: %s", strategy, error)
                break
            logging.warning("Failed, %s. Error: %s", strategy, error)
            await asyncio.sleep(interval)
    ```
"""

import random
from abc import ABC, abstractmethod
from copy import deepcopy
from typing import Self

from typing_extensions import override


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

        To be called as soon as an operation successful if you want to reuse this
        strategy.
        """
        self._count = 0

    def copy(self, reset_copy: bool = False) -> Self:
        """Return a copy of this strategy, optionally resetting the copy's state.

        Args:
            reset_copy: Whether to reset the state of the copy. If `True`, the
                [`reset()`][frequenz.client.base.retry.Strategy.reset] method is called
                on the copy.

        Returns:
            A copy of this strategy, possibly with its state reset.
        """
        ret = deepcopy(self)
        if reset_copy:
            ret.reset()
        return ret


class IntervalWithJitterBasedStrategy(Strategy, ABC):
    """Base class for strategies that use retries based on an interval plus a jitter."""

    DEFAULT_INTERVAL = 3.0
    """Default retry interval, in seconds."""

    DEFAULT_JITTER = 1.0
    """Default retry jitter, in seconds."""

    def __init__(
        self,
        *,
        interval: float = DEFAULT_INTERVAL,
        jitter: float = DEFAULT_JITTER,
        limit: int | None = None,
    ) -> None:
        """Create an instance.

        Args:
            interval: The minimum amount of time to wait for before the next retry, in
                seconds. It should be a positive number.
            jitter: The jitter to add to the retry interval, in seconds. It should be a
                positive number including zero.
            limit: The maximum number of retries before giving up. `None` means no
                limit, and `0` means no retry.

        Raises:
            ValueError: If `interval` or `jitter` are not a positive number.
        """
        super().__init__(limit=limit)
        if interval <= 0.0:
            raise ValueError(f"interval must be a positive number, got {interval}")
        self._interval = interval
        self.jitter = jitter  # Assign via property to enforce validation

    @property
    def interval(self) -> float:
        """The base amount of time to wait for before the next retry, in seconds."""
        return self._interval

    @property
    def jitter(self) -> float:
        """The jitter to add to the retry interval, in seconds.

        Must be a positive number.
        """
        return self._jitter

    @jitter.setter
    def jitter(self, value: float) -> None:
        if value < 0.0:
            raise ValueError(f"jitter must be a positive number, got {value}")
        self._jitter = value

    def apply_jitter(self, value: float) -> float:
        """Return the value with some added uniformly."""
        # Tricky comparison with 0.0, but in this case it should be fine, it should not
        # be negative because it was validated and if it is not exactly zero, it should
        # be OK to pass to `uniform()` and get a value even if it is the smaller value
        # representable by a float.
        if self._jitter == 0.0:
            return value
        return value + random.uniform(0.0, self._jitter)


class LinearBackoff(IntervalWithJitterBasedStrategy):
    """A retry strategy that retries as a linear function of the retry count."""

    def __init__(
        self,
        interval: float = IntervalWithJitterBasedStrategy.DEFAULT_INTERVAL,
        jitter: float = IntervalWithJitterBasedStrategy.DEFAULT_JITTER,
        limit: int | None = None,
    ) -> None:
        """Create a `LinearBackoff` instance.

        Args:
            interval: The minimum amount of time to wait for before the next retry, in
                seconds. It should be a positive number.
            jitter: The jitter to add to the retry interval, in seconds. It should be a
                positive number including zero.
            limit: The maximum number of retries before giving up. `None` means no
                limit, and `0` means no retry.

        Raises:
            ValueError: If `interval` or `jitter` are not a positive number.
        """
        super().__init__(limit=limit, interval=interval, jitter=jitter)

    @override
    def _calculate_next_wait(self) -> float:
        """Calculate the time to wait before the next retry.

        This method doesn't have into account the retry limit, it is mainly intended as
        a helper for the `next_interval()` method and the method that should be
        implemented by subclasses.

        Returns:
            The time to wait before the next retry, in seconds.
        """
        return self.apply_jitter(self._interval)


class ExponentialBackoff(IntervalWithJitterBasedStrategy):
    """A retry strategy that retries as an exponential function of the retry count."""

    DEFAULT_MAX_INTERVAL = 60.0
    """Default maximum retry interval, in seconds."""

    DEFAULT_MULTIPLIER = 2.0
    """Default multiplier for exponential increment."""

    # pylint: disable=too-many-arguments
    def __init__(
        self,
        interval: float = IntervalWithJitterBasedStrategy.DEFAULT_INTERVAL,
        jitter: float = IntervalWithJitterBasedStrategy.DEFAULT_JITTER,
        max_interval: float = DEFAULT_MAX_INTERVAL,
        multiplier: float = DEFAULT_MULTIPLIER,
        limit: int | None = None,
    ) -> None:
        """Create a `ExponentialBackoff` instance.

        Args:
            interval: The minimum amount of time to wait for before the next retry, in
                seconds. It should be a positive number.
            jitter: The jitter to add to the retry interval, in seconds. It should be a
                positive number including zero.
            max_interval: The maximum amount of time to wait for before the next retry,
                in seconds. It should be a positive number.
            multiplier: The multiplier for the exponential increment. It should be a
                positive number greater than one.
            limit: The maximum number of retries before giving up. `None` means no
                limit, and `0` means no retry.

        Raises:
            ValueError: If `interval` or `jitter` are not a positive number.
        """
        super().__init__(limit=limit, interval=interval, jitter=jitter)
        self._max_interval = max_interval
        self._multiplier = multiplier

    @override
    def _calculate_next_wait(self) -> float:
        """Calculate the time to wait before the next retry.

        This method doesn't have into account the retry limit, it is mainly intended as
        a helper for the `next_interval()` method and the method that should be
        implemented by subclasses.

        Returns:
            The time to wait before the next retry, in seconds.
        """
        exp_backoff_interval = self._interval * self._multiplier ** (self._count - 1)
        return min(self.apply_jitter(exp_backoff_interval), self._max_interval)
