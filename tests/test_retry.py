# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""Tests for retry strategies."""

# pylint: disable=chained-comparison

import pytest
from typing_extensions import override

from frequenz.client.base import retry


class _FakeRetryStrategy(retry.Strategy):
    def __init__(self, *, limit: int | None) -> None:
        super().__init__(limit=limit)

    @override
    def _calculate_next_wait(self) -> float:
        return 1.0


class TestStrategy:
    """Tests for the base retry strategy."""

    def test_wrong_limit(self) -> None:
        """Test creating a retry strategy with a wrong limit."""
        with pytest.raises(ValueError, match="limit must be non-negative, got -1"):
            _ = _FakeRetryStrategy(limit=-1)

    def test_no_limit(self) -> None:
        """Test a retry strategy without a limit."""
        strategy = _FakeRetryStrategy(limit=None)

        for _ in range(100):
            assert strategy.next_interval() == 1.0

        strategy.reset()
        assert strategy.count == 0

    def test_with_limit(self) -> None:
        """Test a retry strategy with a limit."""
        strategy = _FakeRetryStrategy(limit=3)

        for _ in range(3):
            assert strategy.next_interval() == 1.0
        assert strategy.next_interval() is None

        strategy.reset()
        assert strategy.count == 0

    def test_with_limit_0(self) -> None:
        """Test a retry strategy with a limit."""
        strategy = _FakeRetryStrategy(limit=0)

        assert strategy.next_interval() is None

        strategy.reset()
        assert strategy.count == 0

    def test_copy(self) -> None:
        """Test copying a retry strategy."""
        strategy = _FakeRetryStrategy(limit=2)

        copy1 = strategy.copy(reset_copy=True)
        assert copy1 is not strategy
        assert copy1.limit == strategy.limit
        assert copy1.count == 0
        assert copy1.next_interval() == 1.0
        assert copy1.next_interval() == 1.0
        assert copy1.next_interval() is None

        copy2 = copy1.copy(reset_copy=True)
        assert copy2 is not copy1
        assert copy2 is not strategy
        assert copy2.limit == strategy.limit
        assert copy2.count == 0
        assert copy1.next_interval() is None
        assert copy2.next_interval() == 1.0
        assert copy2.next_interval() == 1.0
        assert copy2.next_interval() is None

        copy3 = copy1.copy()
        assert copy3 is not copy2
        assert copy3 is not copy1
        assert copy3 is not strategy
        assert copy3.limit == strategy.limit
        assert copy3.count == 2
        assert copy1.next_interval() is None
        assert copy2.next_interval() is None
        assert copy3.next_interval() is None

    def test_str(self) -> None:
        """Test the string representation of a retry strategy."""
        strategy = _FakeRetryStrategy(limit=1)
        assert str(strategy) == "retrying (0/1)"
        _ = strategy.next_interval()
        assert str(strategy) == "retrying (1/1) in 1.0 seconds"
        _ = strategy.next_interval()
        assert str(strategy) == "retry limit reached (1/1)"


class TestLinearBackoff:
    """Tests for the linear backoff retry strategy."""

    def test_no_limit(self) -> None:
        """Test base case."""
        interval = 3
        jitter = 0
        limit = None
        strategy = retry.LinearBackoff(interval=interval, jitter=jitter, limit=limit)

        for _ in range(10):
            assert strategy.next_interval() == interval

    def test_with_limit(self) -> None:
        """Test limit works."""
        interval = 3
        jitter = 0
        limit = 5
        strategy: retry.Strategy = retry.LinearBackoff(
            interval=interval, jitter=jitter, limit=limit
        )

        for _ in range(limit):
            assert strategy.next_interval() == interval
        assert strategy.next_interval() is None

        strategy.reset()
        for _ in range(limit - 1):
            assert strategy.next_interval() == interval
        strategy.reset()
        for _ in range(limit):
            assert strategy.next_interval() == interval
        assert strategy.next_interval() is None

    def test_with_jitter_no_limit(self) -> None:
        """Test with jitter but no limit."""
        interval = 3
        jitter = 1
        limit = None
        strategy: retry.Strategy = retry.LinearBackoff(
            interval=interval, jitter=jitter, limit=limit
        )

        prev = 0.0
        for _ in range(5):
            next_val = strategy.next_interval()
            assert next_val is not None
            assert next_val > interval and next_val < (interval + jitter)
            assert next_val != prev
            prev = next_val

    def test_with_jitter_with_limit(self) -> None:
        """Test with jitter and limit."""
        interval = 3
        jitter = 1
        limit = 2
        strategy: retry.Strategy = retry.LinearBackoff(
            interval=interval, jitter=jitter, limit=limit
        )

        prev = 0.0
        for _ in range(2):
            next_val = strategy.next_interval()
            assert next_val is not None
            assert next_val > interval and next_val < (interval + jitter)
            assert next_val != prev
            prev = next_val
        assert strategy.next_interval() is None

        strategy.reset()
        next_val = strategy.next_interval()
        assert next_val is not None
        assert next_val > interval and next_val < (interval + jitter)
        assert next_val != prev

    def test_deep_copy(self) -> None:
        """Test if deep copies are really deep copies."""
        strategy = retry.LinearBackoff(interval=1.0, jitter=0.0, limit=2)

        copy1 = strategy.copy(reset_copy=True)
        assert copy1.next_interval() == 1.0
        assert copy1.next_interval() == 1.0
        assert copy1.next_interval() is None

        copy2 = copy1.copy(reset_copy=True)
        assert copy1.next_interval() is None
        assert copy2.next_interval() == 1.0
        assert copy2.next_interval() == 1.0
        assert copy2.next_interval() is None


class TestExponentialBackoff:
    """Tests for the exponential backoff retry strategy."""

    def test_no_limit(self) -> None:
        """Test base case."""
        strategy = retry.ExponentialBackoff(
            interval=3.0, max_interval=30.0, multiplier=2, jitter=0.0
        )

        assert strategy.next_interval() == 3.0
        assert strategy.next_interval() == 6.0
        assert strategy.next_interval() == 12.0
        assert strategy.next_interval() == 24.0
        assert strategy.next_interval() == 30.0
        assert strategy.next_interval() == 30.0

    def test_with_limit(self) -> None:
        """Test limit works."""
        strategy = retry.ExponentialBackoff(interval=3.0, jitter=0.0, limit=3)

        assert strategy.next_interval() == 3.0
        assert strategy.next_interval() == 6.0
        assert strategy.next_interval() == 12.0
        assert strategy.next_interval() is None

    def test_deep_copy(self) -> None:
        """Test if deep copies are really deep copies."""
        strategy = retry.ExponentialBackoff(
            interval=3.0, max_interval=30.0, multiplier=2, jitter=0.0, limit=2
        )

        copy1 = strategy.copy(reset_copy=True)
        assert copy1.next_interval() == 3.0
        assert copy1.next_interval() == 6.0
        assert copy1.next_interval() is None

        copy2 = copy1.copy(reset_copy=True)
        assert copy1.next_interval() is None
        assert copy2.next_interval() == 3.0
        assert copy2.next_interval() == 6.0
        assert copy2.next_interval() is None

    def test_with_jitter_no_limit(self) -> None:
        """Test with jitter but no limit."""
        initial_interval = 3
        max_interval = 100
        jitter = 1
        multiplier = 2
        limit = None
        strategy: retry.Strategy = retry.ExponentialBackoff(
            interval=initial_interval,
            max_interval=max_interval,
            multiplier=multiplier,
            jitter=jitter,
            limit=limit,
        )

        prev = 0.0
        for count in range(5):
            next_val = strategy.next_interval()
            exp_backoff_interval = initial_interval * multiplier**count
            assert next_val is not None
            assert initial_interval <= next_val <= max_interval
            assert next_val >= min(exp_backoff_interval, max_interval)
            assert next_val <= min(exp_backoff_interval + jitter, max_interval)
            assert next_val != prev
            prev = next_val

    def test_with_jitter_with_limit(self) -> None:
        """Test with jitter and limit."""
        initial_interval = 3
        max_interval = 100
        jitter = 1
        multiplier = 2
        limit = 2
        strategy: retry.Strategy = retry.ExponentialBackoff(
            interval=initial_interval,
            max_interval=max_interval,
            multiplier=multiplier,
            jitter=jitter,
            limit=limit,
        )

        prev = 0.0
        for count in range(2):
            next_val = strategy.next_interval()
            exp_backoff_interval = initial_interval * multiplier**count
            assert next_val is not None
            assert initial_interval <= next_val <= max_interval
            assert next_val >= min(exp_backoff_interval, max_interval)
            assert next_val <= min(exp_backoff_interval + jitter, max_interval)
            assert next_val != prev
            prev = next_val
        assert strategy.next_interval() is None

        strategy.reset()
        next_val = strategy.next_interval()
        count = 0
        exp_backoff_interval = initial_interval * multiplier**count
        assert next_val is not None
        assert initial_interval <= next_val <= max_interval
        assert next_val >= min(exp_backoff_interval, max_interval)
        assert next_val <= min(exp_backoff_interval + jitter, max_interval)
        assert next_val != prev
