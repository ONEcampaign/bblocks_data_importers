import pandas as pd
from typing import Literal


def convert_dtypes(df: pd.DataFrame, backend: Literal["pyarrow", "numpy_nullable"] = "pyarrow") -> pd.DataFrame:
    """Converts the DataFrame to the specified backend dtypes

    Args:
        df: The DataFrame to convert
        backend: The backend to use for the conversion. Default is "pyarrow"

    Returns:
        A DataFrame with the pyarrow dtypes
    """

    supported_backends = {"pyarrow", "numpy_nullable"}

    # Check if the backend is valid
    if backend not in supported_backends:
        raise ValueError(f"Unsupported backend '{backend}'. Supported backends are {supported_backends}.")

    # Convert dtypes using the specified backend
    return df.convert_dtypes(dtype_backend=backend)


