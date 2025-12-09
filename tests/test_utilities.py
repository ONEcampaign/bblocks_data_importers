"""Tests for the utilities module."""

from unittest.mock import Mock

import pandas as pd
import pytest
from diskcache import Cache

from bblocks.data_importers import utilities


def test_convert_dtypes_with_default_backend():
    """Test conversion with the default backend 'pyarrow'."""
    df = pd.DataFrame({"col1": [1, 2, 3], "col2": [1.0, 2.5, 3.3]})
    result = utilities.convert_dtypes(df)

    assert result.dtypes["col1"].name == "int64[pyarrow]"
    assert result.dtypes["col2"].name == "double[pyarrow]"


def test_convert_dtypes_with_numpy_nullable_backend():
    """Test conversion with 'numpy_nullable' backend."""
    df = pd.DataFrame({"col1": [1, 2, 3], "col2": [1.0, 2.5, 3.3]})
    result = utilities.convert_dtypes(df, backend="numpy_nullable")

    assert result.dtypes["col1"].name == "Int64"
    assert result.dtypes["col2"].name == "Float64"


def test_convert_dtypes_invalid_backend():
    """Test that ValueError is raised when an unsupported backend is provided."""
    df = pd.DataFrame({"col1": [1, 2, 3]})
    with pytest.raises(ValueError, match="Unsupported backend 'invalid_backend'"):
        utilities.convert_dtypes(df, backend="invalid_backend")


def test_disk_memoize_returns_cached_value_without_running_function():
    """Ensure cached values short-circuit the wrapped function."""
    cache = Mock(spec=Cache)
    cache.get.return_value = "cached-result"

    @utilities.disk_memoize(cache=cache)
    def expensive_function(x):
        raise AssertionError("Function should not execute when value is cached")

    assert expensive_function(1) == "cached-result"
    cache.get.assert_called_once()
    cache.set.assert_not_called()


def test_disk_memoize_caches_and_reuses_results(tmp_path):
    """Verify results are cached for identical args/kwargs."""
    with Cache(directory=tmp_path / "memoize-cache") as cache:
        call_counter = {"count": 0}

        @utilities.disk_memoize(cache=cache, expire=10)
        def add(x, y=0):
            call_counter["count"] += 1
            return x + y

        assert add(1, y=2) == 3
        assert call_counter["count"] == 1

        assert add(1, y=2) == 3
        assert call_counter["count"] == 1  # result reused from cache

        cache_key = ("add", (1,), (("y", 2),))
        assert cache.get(cache_key) == 3


def test_disk_memoize_passes_expire_to_cache_set():
    """Ensure expire param is forwarded to cache.set."""
    cache = Mock(spec=Cache)
    cache.get.return_value = None

    @utilities.disk_memoize(cache=cache, expire=123)
    def multiply(x):
        return x * 2

    assert multiply(5) == 10
    cache.get.assert_called_once_with(("multiply", (5,), ()))
    cache.set.assert_called_once_with(("multiply", (5,), ()), 10, expire=123)
