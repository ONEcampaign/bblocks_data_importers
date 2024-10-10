"""Tests for the utilities module."""

import pytest
import pandas as pd

from bblocks_data_importers import utilities


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
